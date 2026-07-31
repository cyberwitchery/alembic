//! import of canonical inventory from backend state.

use crate::adapter_ops::{
    backend_id_from_value, build_key_from_schema, normalize_attrs_refs, StateMappings,
};
use crate::state::{StateData, StateStore};
use crate::types::{BackendId, ObservedObject, Observer};
use alembic_core::{
    key_string, uid_v5, FieldType, Inventory, JsonMap, Key, Object, Schema, TypeName, TypeSchema,
};
use anyhow::Result;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub struct ImportReport {
    pub inventory: Inventory,
}

/// observe a backend into a canonical inventory.
///
/// import derives canonical uids, so it reads against an empty state store: that
/// leaves every state-first ref resolver on its canonical fallback, and objects
/// and their refs land in the same uid space. the fallback needs key material an
/// adapter may not have (generic refs are bare ids; a netbox brief can omit a key
/// field), so identity is bootstrapped from the observation itself first.
pub async fn import_inventory(
    adapter: &(dyn Observer + '_),
    schema: &Schema,
    types: &[TypeName],
) -> Result<ImportReport> {
    let stateless = StateStore::new(None, StateData::default());
    let observed = adapter.read(schema, types, &stateless).await?;

    let objects: Vec<ObservedObject> = observed.by_key.into_values().collect();
    let mappings = bootstrap_mappings(schema, &objects);

    let mut inventory_objects = Vec::new();
    let mut warned: BTreeSet<(String, String)> = BTreeSet::new();
    for object in objects {
        let (key, mut attrs) = materialize(schema, &object, &mappings);
        project_attrs(schema, &object.type_name, &mut attrs, &mut warned);
        inventory_objects.push(Object {
            uid: uid_v5(object.type_name.as_str(), &key_string(&key)),
            type_name: object.type_name,
            key,
            attrs,
            source: None,
        });
    }
    // sort on the final key: normalizing a ref-typed key field moves it.
    inventory_objects
        .sort_by_cached_key(|o| (o.type_name.as_str().to_string(), key_string(&o.key)));

    Ok(ImportReport {
        inventory: Inventory {
            schema: schema.clone(),
            objects: inventory_objects,
        },
    })
}

/// bootstrap a `backend id -> canonical uid` index out of the observation, so
/// refs the adapter left as backend ids can be rewritten into the uid space the
/// imported objects live in. mirrors the phase 2 the infrahub adapter runs
/// locally for its own read.
///
/// an object's uid derives from its key, and a key field may itself be a ref, so
/// resolve to a fixpoint: each round settles the objects whose key refs are
/// already known, seeding the next round, until nothing new resolves. objects in
/// a reference cycle never settle and keep their backend ids.
fn bootstrap_mappings(schema: &Schema, objects: &[ObservedObject]) -> StateMappings {
    let mut observed_ids: BTreeMap<&str, BTreeSet<BackendId>> = BTreeMap::new();
    for object in objects {
        if let Some(backend_id) = &object.backend_id {
            observed_ids
                .entry(object.type_name.as_str())
                .or_default()
                .insert(backend_id.clone());
        }
    }

    // an object with no backend id seeds nothing, and one whose type the schema
    // omits keeps the adapter's key untouched (the flat / custom-schema tier).
    let mut pending: Vec<_> = objects
        .iter()
        .filter_map(|object| {
            let backend_id = object.backend_id.as_ref()?;
            let type_schema = schema.types.get(object.type_name.as_str())?;
            Some((object, backend_id, type_schema))
        })
        .collect();

    let mut mappings = StateMappings::default();
    loop {
        let before = pending.len();
        pending.retain(|(object, backend_id, type_schema)| {
            if !key_refs_settled(type_schema, &object.attrs, &mappings, &observed_ids) {
                return true;
            }
            let attrs = normalize_attrs_refs(&object.attrs, type_schema, &mappings);
            let key = derive_key(type_schema, &attrs, &object.key);
            mappings.insert(
                object.type_name.as_str(),
                (*backend_id).clone(),
                uid_v5(object.type_name.as_str(), &key_string(&key)),
            );
            false
        });
        if pending.len() == before {
            break;
        }
    }
    mappings
}

/// whether every reference-typed *key* field of `attrs` is settled, so the
/// object's own uid can be derived without baking a backend id into it. only a
/// ref naming an observed object that is not indexed yet blocks: one the adapter
/// already resolved canonically, or one pointing outside the observation, will
/// never resolve and so imposes no constraint.
fn key_refs_settled(
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    mappings: &StateMappings,
    observed_ids: &BTreeMap<&str, BTreeSet<BackendId>>,
) -> bool {
    for (field, field_schema) in &type_schema.key {
        let target = match &field_schema.r#type {
            FieldType::Ref { target } | FieldType::ListRef { target } => target,
            _ => continue,
        };
        let Some(value) = attrs.get(field) else {
            continue;
        };
        let items = match value {
            Value::Array(items) => items.as_slice(),
            other => std::slice::from_ref(other),
        };
        for item in items {
            let Some(backend_id) = backend_id_from_value(item) else {
                continue;
            };
            if mappings.uid_for(target, &backend_id).is_none()
                && observed_ids
                    .get(target.as_str())
                    .is_some_and(|ids| ids.contains(&backend_id))
            {
                return false;
            }
        }
    }
    true
}

/// materialize one observed object against the full index: refs normalized, key
/// re-derived from the normalized attrs since a ref-typed key field may have
/// moved. a type absent from the schema passes through untouched, matching
/// `project_attrs`.
fn materialize(
    schema: &Schema,
    object: &ObservedObject,
    mappings: &StateMappings,
) -> (Key, JsonMap) {
    let Some(type_schema) = schema.types.get(object.type_name.as_str()) else {
        return (object.key.clone(), object.attrs.clone());
    };
    let attrs = normalize_attrs_refs(&object.attrs, type_schema, mappings);
    let key = derive_key(type_schema, &attrs, &object.key);
    (key, attrs)
}

/// the key `attrs` implies, falling back to the key the adapter observed when
/// the attrs do not carry every key field. every built-in adapter builds its key
/// from attrs so the two agree, but the external protocol carries `key` and
/// `attrs` independently and an adapter need not echo one into the other.
fn derive_key(type_schema: &TypeSchema, attrs: &JsonMap, observed: &Key) -> Key {
    build_key_from_schema(type_schema, attrs).unwrap_or_else(|_| observed.clone())
}

/// project observed attrs onto the schema by dropping any attr key that is not
/// declared in the type's `fields`.
///
/// backends return server-computed fields (e.g. `dcim.cable.last_updated`) that
/// are not in the schema and could never be managed. left in place they make the
/// imported inventory fail `validate_inventory` with `ExtraAttrField`, so we
/// mirror that check here (validation.rs: `type_schema.fields.contains_key`) and
/// drop the offending keys, warning once per key for the import. types absent
/// from the schema are left untouched, matching validation's early return for
/// unknown types (this preserves the flat / custom-schema tier). key fields are
/// never touched; they validate separately against `type_schema.key`.
fn project_attrs(
    schema: &Schema,
    type_name: &TypeName,
    attrs: &mut JsonMap,
    warned: &mut BTreeSet<(String, String)>,
) {
    let Some(type_schema) = schema.types.get(type_name.as_str()) else {
        return;
    };

    attrs.retain(|field, _| {
        let declared = type_schema.fields.contains_key(field);
        if !declared && warned.insert((type_name.as_str().to_string(), field.clone())) {
            tracing::warn!(
                "import: dropping undeclared attr {}.{}; server-computed field is not in the schema and cannot be managed",
                type_name.as_str(),
                field
            );
        }
        declared
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BackendId, ObservedState};
    use crate::Observer;
    use alembic_core::{
        key_string, FieldSchema, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema,
    };
    use async_trait::async_trait;
    use futures::executor::block_on;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    // callsite interest is global in tracing, and an import running with no
    // subscriber caches `never` for project_attrs' warning. serialize the imports
    // so the log-capturing test can rebuild that cache under its own subscriber.
    static IMPORT_LOCK: Mutex<()> = Mutex::new(());

    fn import_unlocked(adapter: &dyn Observer, schema: &Schema) -> ImportReport {
        block_on(import_inventory(adapter, schema, &[])).unwrap()
    }

    fn run_import(adapter: &dyn Observer, schema: &Schema) -> ImportReport {
        let _guard = IMPORT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        import_unlocked(adapter, schema)
    }

    struct MockAdapter {
        observed: ObservedState,
    }

    #[async_trait]
    impl Observer for MockAdapter {
        async fn read(
            &self,
            _schema: &Schema,
            _types: &[TypeName],
            _state: &crate::state::StateStore,
        ) -> anyhow::Result<ObservedState> {
            Ok(self.observed.clone())
        }
    }

    type Mappings = BTreeMap<TypeName, BTreeMap<alembic_core::Uid, BackendId>>;

    /// records the state store it was read with, so a test can assert import
    /// observes statelessly.
    struct RecordingAdapter {
        observed: ObservedState,
        seen: Arc<Mutex<Option<Mappings>>>,
    }

    #[async_trait]
    impl Observer for RecordingAdapter {
        async fn read(
            &self,
            _schema: &Schema,
            _types: &[TypeName],
            state: &crate::state::StateStore,
        ) -> anyhow::Result<ObservedState> {
            *self.seen.lock().unwrap() = Some(state.all_mappings().clone());
            Ok(self.observed.clone())
        }
    }

    fn observed_state() -> Result<ObservedState> {
        let mut state = ObservedState::default();
        state.insert(crate::ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({
                "name": "FRA1",
                "slug": "fra1",
                "status": "active"
            })),
            backend_id: Some(BackendId::Int(1)),
        })?;
        Ok(state)
    }

    fn key_str(raw: &str) -> Key {
        let mut map = BTreeMap::new();
        for segment in raw.split('/') {
            let (field, value) = segment
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid key segment: {segment}"));
            map.insert(
                field.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
        Key::from(map)
    }

    fn attrs_map(value: serde_json::Value) -> JsonMap {
        let serde_json::Value::Object(map) = value else {
            panic!("attrs must be a json object");
        };
        map.into_iter().collect::<BTreeMap<_, _>>().into()
    }

    fn schema_for_observed(state: &ObservedState) -> Schema {
        let mut types: BTreeMap<String, TypeSchema> = BTreeMap::new();
        for object in state.by_key.values() {
            let entry = types
                .entry(object.type_name.as_str().to_string())
                .or_insert_with(|| TypeSchema {
                    key: BTreeMap::new(),
                    fields: BTreeMap::new(),
                });
            for field in object.key.keys() {
                entry.key.entry(field.clone()).or_insert(FieldSchema {
                    r#type: FieldType::Json,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                });
            }
            for field in object.attrs.keys() {
                entry.fields.entry(field.clone()).or_insert(FieldSchema {
                    r#type: FieldType::Json,
                    required: false,
                    nullable: true,
                    description: None,
                    format: None,
                    pattern: None,
                });
            }
        }
        Schema { types }
    }

    #[test]
    fn import_inventory_uses_stable_uid() {
        let adapter = MockAdapter {
            observed: observed_state().unwrap(),
        };
        let schema = schema_for_observed(&adapter.observed);
        let report = run_import(&adapter, &schema);
        assert_eq!(report.inventory.objects.len(), 1);
        let object = &report.inventory.objects[0];
        let key = key_str("site=fra1");
        assert_eq!(object.key, key);
        assert_eq!(object.uid, uid_v5("dcim.site", &key_string(&key)));
    }

    #[test]
    fn import_observes_with_an_empty_state() {
        // the guard: state-first ref resolvers must fall back to canonical uids,
        // or the objects (always canonical) and their refs land in different spaces.
        let seen = Arc::new(Mutex::new(None));
        let adapter = RecordingAdapter {
            observed: observed_state().unwrap(),
            seen: Arc::clone(&seen),
        };
        let schema = schema_for_observed(&adapter.observed);
        run_import(&adapter, &schema);

        let mappings = seen.lock().unwrap().clone().expect("adapter was read");
        assert!(
            mappings.is_empty(),
            "import must observe with no state mappings: {mappings:?}"
        );
    }

    fn field_schema(required: bool, nullable: bool) -> FieldSchema {
        FieldSchema {
            r#type: FieldType::Json,
            required,
            nullable,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn type_schema(key_fields: &[&str], attr_fields: &[&str]) -> TypeSchema {
        let mut key = BTreeMap::new();
        for field in key_fields {
            key.insert((*field).to_string(), field_schema(true, false));
        }
        let mut fields = BTreeMap::new();
        for field in attr_fields {
            fields.insert((*field).to_string(), field_schema(false, true));
        }
        TypeSchema { key, fields }
    }

    /// build a schema declaring exactly the given key/attr fields per type.
    fn schema_of(entries: &[(&str, &[&str], &[&str])]) -> Schema {
        let mut types = BTreeMap::new();
        for (name, key_fields, attr_fields) in entries {
            types.insert((*name).to_string(), type_schema(key_fields, attr_fields));
        }
        Schema { types }
    }

    fn observed_of(items: &[(&str, &str, serde_json::Value)]) -> ObservedState {
        let mut state = ObservedState::default();
        for (index, (type_name, key, attrs)) in items.iter().enumerate() {
            state
                .insert(crate::ObservedObject {
                    type_name: TypeName::new(*type_name),
                    key: key_str(key),
                    attrs: attrs_map(attrs.clone()),
                    backend_id: Some(BackendId::Int((index + 1) as u64)),
                })
                .unwrap();
        }
        state
    }

    fn import(observed: ObservedState, schema: &Schema) -> ImportReport {
        run_import(&MockAdapter { observed }, schema)
    }

    fn typed_field(field_type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type: field_type,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn ref_field(target: &str) -> FieldSchema {
        typed_field(FieldType::Ref {
            target: target.to_string(),
        })
    }

    /// a type declaring exactly the given key and attr field schemas.
    fn typed_type(key: &[(&str, FieldSchema)], fields: &[(&str, FieldSchema)]) -> TypeSchema {
        let owned = |entries: &[(&str, FieldSchema)]| {
            entries
                .iter()
                .map(|(name, schema)| ((*name).to_string(), schema.clone()))
                .collect()
        };
        TypeSchema {
            key: owned(key),
            fields: owned(fields),
        }
    }

    fn observed_object(
        type_name: &str,
        key: &str,
        attrs: serde_json::Value,
        backend_id: u64,
    ) -> crate::ObservedObject {
        crate::ObservedObject {
            type_name: TypeName::new(type_name),
            key: key_str(key),
            attrs: attrs_map(attrs),
            backend_id: Some(BackendId::Int(backend_id)),
        }
    }

    fn object_of<'a>(report: &'a ImportReport, type_name: &str) -> &'a Object {
        report
            .inventory
            .objects
            .iter()
            .find(|object| object.type_name.as_str() == type_name)
            .unwrap_or_else(|| panic!("no {type_name} in the imported inventory"))
    }

    #[test]
    fn import_derives_a_key_ref_uid_from_the_resolved_target() {
        // an interface keyed on (device, name) whose device arrives as a bare
        // backend id: the interface uid must derive from the device's canonical
        // uid, in whichever order the fixpoint meets the objects. the type names
        // flip so the interface sorts before its target on the second pass. the
        // cable refs the interface, so an index seeded from a key that still
        // held a backend id shows up as a dangling ref.
        let cases = [("a.device", "b.interface"), ("b.device", "a.interface")];
        for (device_type, interface_type) in cases {
            let string_field = typed_field(FieldType::String);
            let schema = Schema {
                types: BTreeMap::from([
                    (
                        device_type.to_string(),
                        typed_type(&[("name", string_field.clone())], &[]),
                    ),
                    (
                        interface_type.to_string(),
                        typed_type(
                            &[
                                ("device", ref_field(device_type)),
                                ("name", string_field.clone()),
                            ],
                            &[],
                        ),
                    ),
                    (
                        "c.cable".to_string(),
                        typed_type(
                            &[("name", string_field.clone())],
                            &[("interface", ref_field(interface_type))],
                        ),
                    ),
                ]),
            };

            let mut observed = ObservedState::default();
            observed
                .insert(observed_object(
                    device_type,
                    "name=leaf01",
                    json!({ "name": "leaf01" }),
                    1,
                ))
                .unwrap();
            observed
                .insert(observed_object(
                    interface_type,
                    "device=1/name=eth0",
                    json!({ "device": 1, "name": "eth0" }),
                    2,
                ))
                .unwrap();
            observed
                .insert(observed_object(
                    "c.cable",
                    "name=c1",
                    json!({ "name": "c1", "interface": 2 }),
                    3,
                ))
                .unwrap();

            let report = import(observed, &schema);
            let device = object_of(&report, device_type);
            let interface = object_of(&report, interface_type);

            assert_eq!(
                interface.key.get("device").and_then(Value::as_str),
                Some(device.uid.to_string().as_str()),
                "{interface_type}.device resolves to the device uid"
            );
            assert_eq!(
                interface.uid,
                uid_v5(interface_type, &key_string(&interface.key)),
                "{interface_type} uid derives from the resolved key"
            );
            assert_eq!(
                object_of(&report, "c.cable")
                    .attrs
                    .get("interface")
                    .and_then(Value::as_str),
                Some(interface.uid.to_string().as_str()),
                "the index holds the interface's resolved uid, not one minted early"
            );
            let validation = alembic_core::validate_inventory(&report.inventory);
            assert!(
                validation.errors.is_empty(),
                "imported inventory must validate: {:?}",
                validation.errors
            );
        }
    }

    #[test]
    fn import_leaves_a_key_ref_cycle_on_backend_ids() {
        // two objects keyed on each other: neither uid can be derived, so the
        // fixpoint settles nothing and both keep their backend ids rather than
        // looping forever.
        let schema = Schema {
            types: BTreeMap::from([
                (
                    "a.node".to_string(),
                    typed_type(&[("peer", ref_field("b.node"))], &[]),
                ),
                (
                    "b.node".to_string(),
                    typed_type(&[("peer", ref_field("a.node"))], &[]),
                ),
            ]),
        };

        let mut observed = ObservedState::default();
        observed
            .insert(observed_object("a.node", "peer=2", json!({ "peer": 2 }), 1))
            .unwrap();
        observed
            .insert(observed_object("b.node", "peer=1", json!({ "peer": 1 }), 2))
            .unwrap();

        let report = import(observed, &schema);
        assert_eq!(report.inventory.objects.len(), 2);
        assert_eq!(
            object_of(&report, "a.node").key.get("peer"),
            Some(&json!(2))
        );
        assert_eq!(
            object_of(&report, "b.node").key.get("peer"),
            Some(&json!(1))
        );
    }

    #[test]
    fn import_drops_undeclared_attrs() {
        // `last_updated` is server-computed and not in the schema; `label` is declared.
        let observed = observed_of(&[(
            "dcim.cable",
            "cable=c1",
            json!({ "label": "uplink", "last_updated": "2026-06-09T00:00:00Z" }),
        )]);
        let schema = schema_of(&[("dcim.cable", &["cable"], &["label"])]);
        let report = import(observed, &schema);

        let object = &report.inventory.objects[0];
        assert!(object.attrs.contains_key("label"), "declared attr is kept");
        assert!(
            !object.attrs.contains_key("last_updated"),
            "undeclared attr is dropped"
        );
        // key fields are never projected away.
        assert_eq!(object.key, key_str("cable=c1"));
    }

    #[test]
    fn import_keeps_attrs_for_unknown_type() {
        // the observed type is absent from the schema, so attrs pass through untouched
        // (validation short-circuits for unknown types, preserving the flat-schema tier).
        let observed = observed_of(&[(
            "custom.thing",
            "id=x1",
            json!({ "anything": "goes", "count": 7 }),
        )]);
        let schema = schema_of(&[("dcim.cable", &["cable"], &["label"])]);
        let report = import(observed, &schema);

        let object = &report.inventory.objects[0];
        assert!(object.attrs.contains_key("anything"));
        assert!(object.attrs.contains_key("count"));
        assert_eq!(object.attrs.len(), 2);
    }

    #[test]
    fn import_inventory_passes_validation() {
        // red-green: without projection the imported inventory carries `last_updated`,
        // which fails validate_inventory with ExtraAttrField.
        let observed = observed_of(&[(
            "dcim.cable",
            "cable=c1",
            json!({ "label": "uplink", "last_updated": "2026-06-09T00:00:00Z" }),
        )]);
        let schema = schema_of(&[("dcim.cable", &["cable"], &["label"])]);
        let report = import(observed, &schema);

        let validation = alembic_core::validate_inventory(&report.inventory);
        assert!(
            !validation
                .errors
                .iter()
                .any(|error| matches!(error, alembic_core::ValidationError::ExtraAttrField { .. })),
            "import must not produce ExtraAttrField errors: {:?}",
            validation.errors
        );
    }

    #[test]
    fn import_projects_each_type_independently() {
        let observed = observed_of(&[
            (
                "dcim.cable",
                "cable=c1",
                json!({ "label": "uplink", "last_updated": "t" }),
            ),
            (
                "dcim.site",
                "site=fra1",
                json!({ "name": "FRA1", "created": "t" }),
            ),
        ]);
        let schema = schema_of(&[
            ("dcim.cable", &["cable"], &["label"]),
            ("dcim.site", &["site"], &["name"]),
        ]);
        let report = import(observed, &schema);

        let cable = report
            .inventory
            .objects
            .iter()
            .find(|object| object.type_name.as_str() == "dcim.cable")
            .expect("cable imported");
        assert!(cable.attrs.contains_key("label"));
        assert!(!cable.attrs.contains_key("last_updated"));

        let site = report
            .inventory
            .objects
            .iter()
            .find(|object| object.type_name.as_str() == "dcim.site")
            .expect("site imported");
        assert!(site.attrs.contains_key("name"));
        assert!(!site.attrs.contains_key("created"));
    }

    #[test]
    fn import_warns_once_per_undeclared_attr_across_objects() {
        let observed = observed_of(&[
            (
                "dcim.cable",
                "cable=c1",
                json!({ "label": "uplink", "last_updated": "t" }),
            ),
            (
                "dcim.cable",
                "cable=c2",
                json!({ "label": "downlink", "last_updated": "t" }),
            ),
            (
                "dcim.site",
                "site=s1",
                json!({ "name": "hq", "last_updated": "t" }),
            ),
        ]);
        let schema = schema_of(&[
            ("dcim.cable", &["cable"], &["label"]),
            ("dcim.site", &["site"], &["name"]),
        ]);

        let _guard = IMPORT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (report, logged) =
            crate::test_log::capture(|| import_unlocked(&MockAdapter { observed }, &schema));

        assert_eq!(report.inventory.objects.len(), 3);
        assert_eq!(
            logged
                .matches("dropping undeclared attr dcim.cable.last_updated")
                .count(),
            1,
            "the same undeclared attr warns once for the whole import, not once per object"
        );
        assert_eq!(
            logged
                .matches("dropping undeclared attr dcim.site.last_updated")
                .count(),
            1,
            "each undeclared (type, field) warns once for the whole import, across types"
        );
    }
}
