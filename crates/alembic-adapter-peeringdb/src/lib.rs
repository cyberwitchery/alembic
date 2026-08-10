//! peeringdb adapter for alembic.
//!
//! uses the peeringdb-rs crate to fetch data from PeeringDB.
//! set the `PEERINGDB_API_KEY` environment variable to authenticate.

use alembic_core::{JsonMap, Schema, TypeName};
use alembic_engine::{build_key_from_schema, BackendId, ObservedObject, ObservedState, Observer};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// blocking fetch plus conversion for one supported type.
type Loader = fn(&TypeName, &alembic_core::TypeSchema) -> Result<Vec<ObservedObject>>;

/// every type this adapter observes, paired with the fetch behind it. the
/// supported set and the dispatch read these same rows, so neither can drift.
const LOADERS: &[(&str, Loader)] = &[
    ("peeringdb.ix", |tn, ts| {
        fetch(peeringdb_rs::load_peeringdb_ix, tn, ts)
    }),
    ("peeringdb.net", |tn, ts| {
        fetch(peeringdb_rs::load_peeringdb_net, tn, ts)
    }),
    ("peeringdb.org", |tn, ts| {
        fetch(peeringdb_rs::load_peeringdb_org, tn, ts)
    }),
    ("peeringdb.netixlan", |tn, ts| {
        fetch(peeringdb_rs::load_peeringdb_netixlan, tn, ts)
    }),
];

fn loader_for(type_name: &TypeName) -> Option<Loader> {
    LOADERS
        .iter()
        .find(|(name, _)| *name == type_name.as_str())
        .map(|(_, load)| *load)
}

/// fields only a record of this type carries. every field of every peeringdb
/// struct but `id` is optional and none of them reject an unknown key, so a
/// payload from the wrong endpoint deserializes cleanly and reaches the ir with
/// every type-specific field null. a record holding none of these is not the
/// type it claims, whatever it deserialized into.
fn witness_fields(type_name: &TypeName) -> &'static [&'static str] {
    match type_name.as_str() {
        // an org record carries `id`, `name`, `notes`, `created`, `updated` and
        // `status` too, so only the netixlan-only fields tell the two apart.
        "peeringdb.netixlan" => &[
            "net_id",
            "ix_id",
            "ixlan_id",
            "asn",
            "ipaddr4",
            "ipaddr6",
            "speed",
            "is_rs_peer",
        ],
        _ => &[],
    }
}

fn fetch<T: Serialize + HasId>(
    load: fn() -> Result<Vec<T>>,
    type_name: &TypeName,
    type_schema: &alembic_core::TypeSchema,
) -> Result<Vec<ObservedObject>> {
    let short = type_name.as_str().trim_start_matches("peeringdb.");
    let data = load().map_err(|e| anyhow!("failed to load {} data: {}", short, e))?;
    to_observed_objects(type_name, type_schema, data)
}

pub struct PeeringDBAdapter;

impl PeeringDBAdapter {
    /// create a new PeeringDB adapter.
    ///
    /// authentication is handled via the `PEERINGDB_API_KEY` environment variable.
    pub fn new() -> Self {
        Self
    }
}

impl Default for PeeringDBAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Observer for PeeringDBAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        _state: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        let requested: BTreeSet<TypeName> = if types.is_empty() {
            // empty means every schema-declared supported type; skip types the schema omits.
            LOADERS
                .iter()
                .map(|(name, _)| TypeName::new(*name))
                .filter(|tn| schema.types.contains_key(tn.as_str()))
                .collect()
        } else {
            types.iter().cloned().collect()
        };

        let mut state = ObservedState::default();

        for type_name in requested {
            let load =
                loader_for(&type_name).ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?
                .clone();

            let tn = type_name.clone();
            let objects = tokio::task::spawn_blocking(move || load(&tn, &type_schema)).await??;

            for object in objects {
                state.insert(object)?;
            }
        }

        Ok(state)
    }
}

/// trait for PeeringDB objects that have an id field.
trait HasId {
    fn id(&self) -> u32;
}

impl HasId for peeringdb_rs::PeeringdbIx {
    fn id(&self) -> u32 {
        self.id
    }
}

impl HasId for peeringdb_rs::PeeringdbNet {
    fn id(&self) -> u32 {
        self.id
    }
}

impl HasId for peeringdb_rs::PeeringdbOrg {
    fn id(&self) -> u32 {
        self.id
    }
}

impl HasId for peeringdb_rs::PeeringdbNetixlan {
    fn id(&self) -> u32 {
        self.id
    }
}

fn to_observed_objects<T: Serialize + HasId>(
    type_name: &TypeName,
    type_schema: &alembic_core::TypeSchema,
    items: Vec<T>,
) -> Result<Vec<ObservedObject>> {
    let mut objects = Vec::new();
    let witnesses = witness_fields(type_name);

    for item in items {
        let id = item.id();
        let value = serde_json::to_value(&item)?;
        let attrs: JsonMap = match value {
            serde_json::Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>().into(),
            _ => return Err(anyhow!("expected object from serialization")),
        };

        if !witnesses.is_empty()
            && !witnesses
                .iter()
                .any(|field| attrs.get(*field).is_some_and(|value| !value.is_null()))
        {
            return Err(anyhow!(
                "peeringdb answered {type_name} with a record (id {id}) carrying none of {}, \
                 so the payload is not {type_name} data",
                witnesses.join(", ")
            ));
        }

        let key = build_key_from_schema(type_schema, &attrs)?;

        objects.push(ObservedObject {
            type_name: type_name.clone(),
            key,
            attrs,
            backend_id: Some(BackendId::Int(id as u64)),
        });
    }

    Ok(objects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{FieldSchema, FieldType, TypeSchema};

    fn ix_schema() -> TypeSchema {
        TypeSchema {
            key: BTreeMap::from([(
                "name".to_string(),
                FieldSchema {
                    r#type: FieldType::String,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                },
            )]),
            fields: BTreeMap::new(),
        }
    }

    #[test]
    fn build_key_extracts_fields() {
        let schema = ix_schema();
        let attrs: JsonMap = BTreeMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("name".to_string(), serde_json::json!("DE-CIX Frankfurt")),
        ])
        .into();
        let key = build_key_from_schema(&schema, &attrs).unwrap();
        assert_eq!(
            key.get("name"),
            Some(&serde_json::json!("DE-CIX Frankfurt"))
        );
    }

    #[test]
    fn build_key_errors_on_missing_field() {
        let schema = ix_schema();
        let attrs: JsonMap = BTreeMap::from([("id".to_string(), serde_json::json!(1))]).into();
        let err = build_key_from_schema(&schema, &attrs).unwrap_err();
        assert!(err.to_string().contains("missing key field name"));
    }

    fn sample_ix(id: u32, name: &str) -> peeringdb_rs::PeeringdbIx {
        peeringdb_rs::PeeringdbIx {
            id,
            org_id_id: None,
            name: Some(name.to_string()),
            aka: None,
            name_long: None,
            city: None,
            country: None,
            region_continent: None,
            media: None,
            notes: None,
            proto_unicast: None,
            proto_multicast: None,
            proto_ipv6: None,
            website: None,
            url_stats: None,
            status_dashboard: None,
            tech_email: None,
            tech_phone: None,
            policy_email: None,
            policy_phone: None,
            sales_email: None,
            sales_phone: None,
            ixf_net_count: None,
            ixf_last_import: None,
            ixf_import_request: None,
            ixf_import_request_status: None,
            net_count: None,
            fac_count: None,
            service_level: None,
            terms: None,
            created: None,
            updated: None,
            status: None,
        }
    }

    #[test]
    fn to_observed_objects_maps_id_key_and_attrs() {
        let type_name = TypeName::new("peeringdb.ix");
        let schema = ix_schema();
        let objects =
            to_observed_objects(&type_name, &schema, vec![sample_ix(42, "DE-CIX Frankfurt")])
                .unwrap();

        assert_eq!(objects.len(), 1);
        let object = &objects[0];
        assert_eq!(object.type_name, type_name);
        assert_eq!(object.backend_id, Some(BackendId::Int(42)));
        assert_eq!(
            object.key.get("name"),
            Some(&serde_json::json!("DE-CIX Frankfurt"))
        );
        assert_eq!(
            object.attrs.get("name"),
            Some(&serde_json::json!("DE-CIX Frankfurt"))
        );
    }

    #[tokio::test]
    async fn observe_errors_on_missing_schema() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let state = alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let err = adapter
            .read(&schema, &[TypeName::new("peeringdb.ix")], &state)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing schema"));
    }

    #[tokio::test]
    async fn observe_empty_types_skips_undeclared_types() {
        // empty types + a schema that declares none of them must skip (not error);
        // the explicit-type path still errors (observe_errors_on_missing_schema).
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let state = alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert!(observed.by_key.is_empty());
    }

    #[tokio::test]
    async fn observe_errors_on_unsupported_type() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::from([("peeringdb.fac".to_string(), ix_schema())]),
        };
        let state_store =
            alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let err = adapter
            .read(&schema, &[TypeName::new("peeringdb.fac")], &state_store)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("unsupported type peeringdb.fac"));
    }

    #[tokio::test]
    async fn observe_empty_types_skips_unsupported_types() {
        // only an explicit request errors: a schema may legitimately declare types
        // this backend does not own, and empty types still skips those.
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::from([("peeringdb.fac".to_string(), ix_schema())]),
        };
        let state_store =
            alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let observed = adapter.read(&schema, &[], &state_store).await.unwrap();

        assert!(observed.by_key.is_empty());
    }

    /// a record of another type deserialized as a netixlan: the fields the two
    /// share carry values, every netixlan-only field is null. an org record has
    /// exactly this shape, since `id`, `name`, `notes`, `created`, `updated` and
    /// `status` are common to both.
    fn org_record_as_netixlan(id: u32, name: &str) -> peeringdb_rs::PeeringdbNetixlan {
        peeringdb_rs::PeeringdbNetixlan {
            id,
            net_id: None,
            ix_id: None,
            name: Some(name.to_string()),
            ixlan_id: None,
            notes: None,
            speed: None,
            asn: None,
            ipaddr4: None,
            ipaddr6: None,
            is_rs_peer: None,
            bfd_support: None,
            operational: None,
            created: None,
            updated: None,
            status: Some("ok".to_string()),
        }
    }

    #[test]
    fn a_record_carrying_no_netixlan_field_is_refused() {
        let err = to_observed_objects(
            &TypeName::new("peeringdb.netixlan"),
            &ix_schema(),
            vec![org_record_as_netixlan(2, "Equinix, Inc.")],
        )
        .expect_err("an org record must not be observed as a netixlan");

        let message = format!("{err:#}");
        assert!(message.contains("net_id"), "{message}");
        assert!(message.contains("peeringdb.netixlan"), "{message}");
    }

    #[test]
    fn one_netixlan_field_is_enough_to_accept_a_record() {
        // the guard asks whether the record is the type it claims, not whether
        // it is fully populated: a netixlan may legitimately omit most fields.
        let mut sparse = org_record_as_netixlan(7, "leaf");
        sparse.asn = Some(64512);

        let objects = to_observed_objects(
            &TypeName::new("peeringdb.netixlan"),
            &ix_schema(),
            vec![sparse],
        )
        .expect("a record carrying a netixlan field is a netixlan");
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].backend_id, Some(BackendId::Int(7)));
    }

    #[test]
    fn a_type_with_no_witness_fields_is_unguarded() {
        // ix, net and org read their own endpoints, so nothing distinguishes a
        // sparse record from a wrong one and the guard must not invent a rule.
        assert!(witness_fields(&TypeName::new("peeringdb.ix")).is_empty());
        let objects = to_observed_objects(
            &TypeName::new("peeringdb.ix"),
            &ix_schema(),
            vec![sample_ix(1, "x")],
        )
        .expect("an ix record is not witness-checked");
        assert_eq!(objects.len(), 1);
    }

    #[test]
    fn loaders_cover_the_documented_types() {
        let supported: Vec<&str> = LOADERS.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            supported,
            vec![
                "peeringdb.ix",
                "peeringdb.net",
                "peeringdb.org",
                "peeringdb.netixlan"
            ]
        );
        for name in supported {
            assert!(loader_for(&TypeName::new(name)).is_some());
        }
        assert!(loader_for(&TypeName::new("peeringdb.fac")).is_none());
    }
}
