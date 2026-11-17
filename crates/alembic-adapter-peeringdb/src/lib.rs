//! peeringdb adapter for alembic.
//!
//! uses the peeringdb-rs crate to fetch data from PeeringDB.
//! set the `PEERINGDB_API_KEY` environment variable to authenticate.

use alembic_adapter_sdk::BackendId;
use alembic_core::{JsonMap, Schema, TypeName};
use alembic_engine::{build_key_from_schema, ObservedObject, ObservedState, Observer};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// blocking fetch plus conversion for one supported type.
/// `Some(ids)` narrows the fetch, `None` reads the whole table.
type Loader =
    fn(&TypeName, &alembic_core::TypeSchema, Option<&[u64]>) -> Result<Vec<ObservedObject>>;

/// a peeringdb-rs `load_peeringdb_*_filtered` function.
type FilteredLoad<T> = fn(&[(&str, &str)]) -> Result<Vec<T>>;

/// every type this adapter observes, paired with the fetch behind it. the
/// supported set and the dispatch read these same rows, so neither can drift.
const LOADERS: &[(&str, Loader)] = &[
    ("peeringdb.ix", |tn, ts, ids| {
        fetch(peeringdb_rs::load_peeringdb_ix_filtered, tn, ts, ids)
    }),
    ("peeringdb.net", |tn, ts, ids| {
        fetch(peeringdb_rs::load_peeringdb_net_filtered, tn, ts, ids)
    }),
    ("peeringdb.org", |tn, ts, ids| {
        fetch(peeringdb_rs::load_peeringdb_org_filtered, tn, ts, ids)
    }),
    ("peeringdb.netixlan", |tn, ts, ids| {
        fetch(peeringdb_rs::load_peeringdb_netixlan_filtered, tn, ts, ids)
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
    load: FilteredLoad<T>,
    type_name: &TypeName,
    type_schema: &alembic_core::TypeSchema,
    ids: Option<&[u64]>,
) -> Result<Vec<ObservedObject>> {
    let short = type_name.as_str().trim_start_matches("peeringdb.");
    let data = match ids {
        Some(ids) => load_by_ids(load, ids),
        None => load(&[]),
    }
    .map_err(|e| anyhow!("failed to load {} data: {}", short, e))?;
    to_observed_objects(type_name, type_schema, data)
}

/// fetch by `id__in`. 500 ids per request, which keeps the query string short
/// and the number of requests low.
fn load_by_ids<T>(load: FilteredLoad<T>, ids: &[u64]) -> Result<Vec<T>> {
    const CHUNK: usize = 500;
    let mut data = Vec::new();
    for chunk in ids.chunks(CHUNK) {
        let joined = chunk
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        data.extend(load(&[("id__in", &joined)])?);
    }
    Ok(data)
}

/// the peeringdb ids bound in state for one type. an empty vec means the type
/// has no bindings and can be skipped. `None` means at least one binding is not
/// a peeringdb integer id, so the caller must fall back to the full table.
fn bound_int_ids(
    state_store: &alembic_engine::StateStore,
    type_name: &TypeName,
) -> Option<Vec<u64>> {
    let Some(bound) = state_store.backend_ids().get(type_name) else {
        return Some(Vec::new());
    };
    bound
        .keys()
        .map(|id| match id {
            BackendId::Int(id) => Some(*id),
            BackendId::String(_) => None,
        })
        .collect()
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
        state_store: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        self.read_listing(schema, types, state_store, false).await
    }

    async fn read_bound(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        self.read_listing(schema, types, state_store, true).await
    }
}

impl PeeringDBAdapter {
    async fn read_listing(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
        bound_only: bool,
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
            let bound = bound_only
                .then(|| bound_int_ids(state_store, &type_name))
                .flatten();
            if bound.as_ref().is_some_and(|ids| ids.is_empty()) {
                continue;
            }

            let tn = type_name.clone();
            let objects =
                tokio::task::spawn_blocking(move || load(&tn, &type_schema, bound.as_deref()))
                    .await??;

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

        let key = build_key_from_schema(type_schema, &attrs)
            .with_context(|| format!("build key for {type_name}"))?;

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
    use alembic_adapter_sdk::StateData;
    use alembic_core::{FieldSchema, FieldType, TypeSchema, Uid};

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
        let state = alembic_engine::StateStore::new(None, StateData::default());
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
        let state = alembic_engine::StateStore::new(None, StateData::default());
        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert!(observed.is_empty());
    }

    #[tokio::test]
    async fn observe_errors_on_unsupported_type() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::from([("peeringdb.fac".to_string(), ix_schema())]),
        };
        let state_store = alembic_engine::StateStore::new(None, StateData::default());
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
        let state_store = alembic_engine::StateStore::new(None, StateData::default());
        let observed = adapter.read(&schema, &[], &state_store).await.unwrap();

        assert!(observed.is_empty());
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

    fn fake_ix_filtered(params: &[(&str, &str)]) -> Result<Vec<peeringdb_rs::PeeringdbIx>> {
        let [("id__in", ids)] = params else {
            panic!("expected one id__in filter, got {params:?}");
        };
        let ids: Vec<u32> = ids.split(',').map(|id| id.parse().unwrap()).collect();
        assert!(ids.len() <= 500, "chunk of {} ids", ids.len());
        Ok(ids.into_iter().map(|id| sample_ix(id, "x")).collect())
    }

    #[test]
    fn load_by_ids_chunks_the_id_filter() {
        let ids: Vec<u64> = (1..=1200).collect();
        let loaded = load_by_ids(fake_ix_filtered, &ids).unwrap();
        let loaded: Vec<u64> = loaded.iter().map(|ix| u64::from(ix.id)).collect();
        assert_eq!(loaded, ids);
    }

    #[test]
    fn bound_int_ids_separates_unbound_from_unaddressable() {
        let mut state = alembic_engine::StateStore::new(None, StateData::default());
        let ix = TypeName::new("peeringdb.ix");
        let net = TypeName::new("peeringdb.net");
        assert_eq!(bound_int_ids(&state, &ix), Some(Vec::new()));

        state.set_backend_id(
            ix.clone(),
            Uid::parse_str("00000000-0000-0000-0000-0000000000a1").unwrap(),
            BackendId::Int(26),
        );
        assert_eq!(bound_int_ids(&state, &ix), Some(vec![26]));

        state.set_backend_id(
            net.clone(),
            Uid::parse_str("00000000-0000-0000-0000-0000000000a2").unwrap(),
            BackendId::String("abc".to_string()),
        );
        assert_eq!(bound_int_ids(&state, &net), None);
    }

    #[tokio::test]
    async fn a_bound_read_skips_a_type_with_no_state_bindings() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::from([("peeringdb.netixlan".to_string(), ix_schema())]),
        };
        let state = alembic_engine::StateStore::new(None, StateData::default());
        let observed = adapter
            .read_bound(&schema, &[TypeName::new("peeringdb.netixlan")], &state)
            .await
            .unwrap();
        assert!(observed.is_empty());
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
