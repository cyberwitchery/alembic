//! import of canonical inventory from backend state.

use crate::adapter_ops::{
    backend_id_from_value, build_key_from_schema, normalize_attrs_refs, StateMappings,
};
use crate::state::{StateData, StateStore};
use crate::types::{BackendId, ObservedObject, Observer};
use alembic_core::{
    key_string, uid_v5, FieldType, Inventory, JsonMap, Key, Object, Schema, TypeName, TypeSchema,
    Uid,
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

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
///
/// the inventory is validated before it is returned, as `compile_map` validates
/// what it builds: every consumer of an imported file validates on load, so one
/// that does not validate has no use.
pub async fn import_inventory(
    adapter: &(dyn Observer + '_),
    schema: &Schema,
    types: &[TypeName],
) -> Result<ImportReport> {
    let stateless = StateStore::new(None, StateData::default());
    let observed = adapter.read(schema, types, &stateless).await?;

    let objects: Vec<ObservedObject> = observed.by_key.into_values().collect();
    let observed_ids = observed_backend_ids(&objects);
    let mappings = bootstrap_mappings(schema, &objects, &observed_ids);

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

    let inventory = Inventory {
        schema: schema.clone(),
        objects: inventory_objects,
    };
    // both name a cause validation can only report as a symptom, and import
    // still holds the observation and the types it asked for. imported objects
    // carry no source, so `..._with_sources` would add nothing.
    undeclared_types_to_result(&inventory, schema, types)?;
    unresolved_refs_to_result(unresolved_refs(&inventory, &mappings, &observed_ids))?;
    crate::report_to_result(crate::validate(&inventory))?;

    Ok(ImportReport { inventory })
}

/// the backend ids the observation carries, per type.
fn observed_backend_ids(objects: &[ObservedObject]) -> BTreeMap<String, BTreeSet<BackendId>> {
    let mut ids: BTreeMap<String, BTreeSet<BackendId>> = BTreeMap::new();
    for object in objects {
        if let Some(backend_id) = &object.backend_id {
            ids.entry(object.type_name.as_str().to_string())
                .or_default()
                .insert(backend_id.clone());
        }
    }
    ids
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
fn bootstrap_mappings(
    schema: &Schema,
    objects: &[ObservedObject],
    observed_ids: &BTreeMap<String, BTreeSet<BackendId>>,
) -> StateMappings {
    // an object with no backend id seeds nothing, and one whose type the schema
    // omits is refused later as an undeclared type, so it seeds nothing either.
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
            if !key_refs_settled(type_schema, &object.attrs, &mappings, observed_ids) {
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
    observed_ids: &BTreeMap<String, BTreeSet<BackendId>>,
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
/// `project_attrs`; import then refuses it as an undeclared type.
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

/// a ref that came out of normalization still holding a backend id, with why the
/// id could not be rewritten. `field` is labelled as validation labels it
/// (`<type>.<field>`, key fields under `key.`), so both name the same place.
#[derive(Debug)]
struct UnresolvedRef {
    field: String,
    target: String,
    value: Value,
    cause: UnresolvedCause,
}

#[derive(Debug, Clone, Copy)]
enum UnresolvedCause {
    /// the target is not in the observation at all.
    Unobserved,
    /// the target was observed but never mapped to a uid. this tests absence
    /// from the index, not a cycle; an undeclared type is the other way to be
    /// absent and is refused first, which leaves key refs that never settle.
    Unmapped,
    /// the target has a uid and this copy of the ref kept the backend id anyway:
    /// only `attrs` is normalized, so a key field `derive_key` fell back to the
    /// observed key for is whatever the adapter sent.
    NotRewritten(Uid),
}

impl fmt::Display for UnresolvedRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {} {}: ", self.field, self.target, self.value)?;
        match &self.cause {
            UnresolvedCause::Unobserved => write!(
                f,
                "no {} with that backend id was observed, so there is no uid to point at; import reads only the types the schema in `-f` declares",
                self.target
            ),
            UnresolvedCause::Unmapped => write!(
                f,
                "the {} it names was observed but never mapped to a uid: its own key refs never settle, so it is keyed on a reference cycle",
                self.target
            ),
            UnresolvedCause::NotRewritten(uid) => write!(
                f,
                "the {} it names is {}, but the adapter reported this key field only in `key` and not in `attrs`, so it was left as read",
                self.target, uid
            ),
        }
    }
}

/// an observed type the schema does not declare. validation can only call it
/// `unknown type`, which reads as a gap in the user's own `-f`; import knows the
/// list it asked for, so it says the type came back from the adapter unasked.
/// refusing it here is also what leaves `UnresolvedCause::Unmapped` a cycle.
fn undeclared_types_to_result(
    inventory: &Inventory,
    schema: &Schema,
    types: &[TypeName],
) -> Result<()> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for object in &inventory.objects {
        let name = object.type_name.as_str();
        if !schema.types.contains_key(name) {
            *counts.entry(name).or_default() += 1;
        }
    }
    if counts.is_empty() {
        return Ok(());
    }
    // an empty request means every declared type to the built-in adapters; external ones decide.
    let requested: Vec<&str> = if types.is_empty() {
        schema.types.keys().map(String::as_str).collect()
    } else {
        types.iter().map(TypeName::as_str).collect()
    };
    let mut message = format!(
        "import failed: {} observed type(s) are not declared by the schema, so the inventory would not validate:\n",
        counts.len()
    );
    for (name, count) in counts {
        message.push_str(&format!("- {name}: {count} object(s)\n"));
    }
    // with nothing declared the cause is the `-f`, so naming the list would point away from it.
    if requested.is_empty() {
        message.push_str(
            "the schema in `-f` declares no types, so import asked for nothing and every observed type is undeclared",
        );
    } else {
        message.push_str(&format!(
            "import asked for {}; these came back unasked, so they are the adapter's, not a gap in the schema in `-f`",
            requested.join(", ")
        ));
    }
    Err(anyhow!(message))
}

fn unresolved_refs_to_result(unresolved: Vec<UnresolvedRef>) -> Result<()> {
    if unresolved.is_empty() {
        return Ok(());
    }
    let mut message = format!(
        "import failed: {} reference(s) could not be resolved to an imported object, so the inventory would not validate:\n",
        unresolved.len()
    );
    for entry in unresolved {
        message.push_str(&format!("- {entry}\n"));
    }
    Err(anyhow!(message))
}

/// every ref in the built inventory that is still a backend id. walks the same
/// fields `normalize_attrs_refs` rewrote, so a leaf it reached and left behind is
/// a leaf reported here.
fn unresolved_refs(
    inventory: &Inventory,
    mappings: &StateMappings,
    observed_ids: &BTreeMap<String, BTreeSet<BackendId>>,
) -> Vec<UnresolvedRef> {
    let mut found = Vec::new();
    for object in &inventory.objects {
        let Some(type_schema) = inventory.schema.types.get(object.type_name.as_str()) else {
            continue;
        };
        crate::refs::visit_ref_leaves(
            type_schema,
            &object.key,
            &object.attrs,
            &mut |field, target, value| {
                if let Some(entry) = classify_ref(
                    &object.type_name,
                    &field,
                    target,
                    value,
                    mappings,
                    observed_ids,
                ) {
                    found.push(entry);
                }
            },
        );
    }
    found
}

fn classify_ref(
    type_name: &TypeName,
    field: &str,
    target: &str,
    value: &Value,
    mappings: &StateMappings,
    observed_ids: &BTreeMap<String, BTreeSet<BackendId>>,
) -> Option<UnresolvedRef> {
    // a resolved ref, or a value that is not a ref shape at all: nothing to say
    // about the observation, and validation names the value.
    let backend_id = crate::refs::unrewritten_backend_id(value)?;
    let cause = match mappings.uid_for(target, &backend_id) {
        Some(uid) => UnresolvedCause::NotRewritten(uid),
        None if observed_ids
            .get(target)
            .is_some_and(|ids| ids.contains(&backend_id)) =>
        {
            UnresolvedCause::Unmapped
        }
        None => UnresolvedCause::Unobserved,
    };
    Some(UnresolvedRef {
        field: format!("{type_name}.{field}"),
        target: target.to_string(),
        value: value.clone(),
        cause,
    })
}

/// project observed attrs onto the schema by dropping any attr key that is not
/// declared in the type's `fields`.
///
/// backends return server-computed fields (e.g. `dcim.cable.last_updated`) that
/// are not in the schema and could never be managed. left in place they make the
/// imported inventory fail `validate_inventory` with `ExtraAttrField`, so we
/// mirror that check here (validation.rs: `type_schema.fields.contains_key`) and
/// drop the offending keys, warning once per key for the import. types absent
/// from the schema are left untouched, since import refuses the whole object as
/// an undeclared type and projecting its attrs would say nothing more. key fields
/// are never touched; they validate separately against `type_schema.key`.
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

    fn import_err(observed: ObservedState, schema: &Schema) -> String {
        let _guard = IMPORT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        block_on(import_inventory(&MockAdapter { observed }, schema, &[]))
            .expect_err("import must refuse an inventory that does not validate")
            .to_string()
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
    fn import_refuses_a_key_ref_cycle_by_name() {
        // two objects keyed on each other: neither uid can be derived, so the
        // fixpoint settles nothing rather than looping forever, and both keep
        // their backend ids. that inventory cannot validate, so import says which
        // refs are stuck and why instead of writing it.
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

        let error = import_err(observed, &schema);
        assert!(
            error.contains("2 reference(s) could not be resolved"),
            "{error}"
        );
        for (field, target, id) in [
            ("a.node.key.peer", "b.node", "2"),
            ("b.node.key.peer", "a.node", "1"),
        ] {
            assert!(
                error.contains(&format!(
                    "{field} -> {target} {id}: the {target} it names was observed but never mapped to a uid"
                )),
                "{error}"
            );
        }
    }

    #[test]
    fn import_refuses_a_ref_to_an_object_outside_the_observation() {
        // the common case: a plain attr ref whose target the observation never
        // returned. it keeps its backend id, which validation reports as
        // `expected uuid, got number`; import knows it is a missing object.
        let string_field = typed_field(FieldType::String);
        let schema = Schema {
            types: BTreeMap::from([
                (
                    "b.interface".to_string(),
                    typed_type(&[("name", string_field.clone())], &[]),
                ),
                (
                    "c.cable".to_string(),
                    typed_type(
                        &[("name", string_field)],
                        &[("interface", ref_field("b.interface"))],
                    ),
                ),
            ]),
        };

        let mut observed = ObservedState::default();
        observed
            .insert(observed_object(
                "c.cable",
                "name=c1",
                json!({ "name": "c1", "interface": 99 }),
                1,
            ))
            .unwrap();

        let error = import_err(observed, &schema);
        assert!(
            error.contains(
                "c.cable.interface -> b.interface 99: no b.interface with that backend id was observed"
            ),
            "{error}"
        );
    }

    #[test]
    fn import_reports_every_unresolved_ref_at_once() {
        // one message per import, not one failure per ref: a stale inventory
        // usually breaks in several places and the operator wants the whole list.
        let string_field = typed_field(FieldType::String);
        let schema = Schema {
            types: BTreeMap::from([
                (
                    "b.interface".to_string(),
                    typed_type(&[("name", string_field.clone())], &[]),
                ),
                (
                    "c.cable".to_string(),
                    typed_type(
                        &[("name", string_field)],
                        &[("interface", ref_field("b.interface"))],
                    ),
                ),
            ]),
        };

        let observed = observed_of(&[
            (
                "c.cable",
                "name=c1",
                json!({ "name": "c1", "interface": 98 }),
            ),
            (
                "c.cable",
                "name=c2",
                json!({ "name": "c2", "interface": 99 }),
            ),
        ]);

        let error = import_err(observed, &schema);
        assert!(
            error.contains("2 reference(s) could not be resolved"),
            "{error}"
        );
        assert!(error.contains("-> b.interface 98"), "{error}");
        assert!(error.contains("-> b.interface 99"), "{error}");
    }

    /// import one `c.cable` whose `interface` field carries the given type and
    /// value, against a schema declaring a `b.interface` nothing observes.
    fn unresolved_in(field_type: FieldType, value: serde_json::Value) -> String {
        let string_field = typed_field(FieldType::String);
        let schema = Schema {
            types: BTreeMap::from([
                (
                    "b.interface".to_string(),
                    typed_type(&[("name", string_field.clone())], &[]),
                ),
                (
                    "c.cable".to_string(),
                    typed_type(
                        &[("name", string_field)],
                        &[("interface", typed_field(field_type))],
                    ),
                ),
            ]),
        };
        let observed = observed_of(&[(
            "c.cable",
            "name=c1",
            json!({ "name": "c1", "interface": value }),
        )]);
        import_err(observed, &schema)
    }

    fn interface_ref() -> FieldType {
        FieldType::Ref {
            target: "b.interface".to_string(),
        }
    }

    #[test]
    fn import_reports_every_unresolved_ref_of_a_list_ref() {
        // the shared walk has an arm per ref-bearing field type and the other tests
        // only drive the plain `Ref`. a list of refs reports every element, not
        // just the first.
        let error = unresolved_in(
            FieldType::ListRef {
                target: "b.interface".to_string(),
            },
            json!([98, 99]),
        );
        assert!(
            error.contains("2 reference(s) could not be resolved"),
            "{error}"
        );
        assert!(
            error.contains("c.cable.interface -> b.interface 98"),
            "{error}"
        );
        assert!(
            error.contains("c.cable.interface -> b.interface 99"),
            "{error}"
        );
    }

    #[test]
    fn import_reports_an_unresolved_ref_nested_in_a_list() {
        // `List` recurses on its item type, so a ref reached through it is
        // labelled with the field that holds the list.
        let error = unresolved_in(
            FieldType::List {
                item: Box::new(interface_ref()),
            },
            json!([99]),
        );
        assert!(
            error.contains("c.cable.interface -> b.interface 99"),
            "{error}"
        );
    }

    #[test]
    fn import_reports_an_unresolved_ref_nested_in_a_map() {
        // `Map` recurses on its value type; keys hold no refs.
        let error = unresolved_in(
            FieldType::Map {
                value: Box::new(interface_ref()),
            },
            json!({ "primary": 99 }),
        );
        assert!(
            error.contains("c.cable.interface -> b.interface 99"),
            "{error}"
        );
    }

    #[test]
    fn import_refuses_a_key_ref_the_adapter_reported_only_in_the_key() {
        // the external protocol carries `key` and `attrs` independently, and only
        // `attrs` is normalized, so a ref-typed key field absent from `attrs`
        // reaches the inventory as read even though its target has a uid.
        let schema = Schema {
            types: BTreeMap::from([
                (
                    "a.device".to_string(),
                    typed_type(&[("name", typed_field(FieldType::String))], &[]),
                ),
                (
                    "b.iface".to_string(),
                    typed_type(
                        &[
                            ("device", ref_field("a.device")),
                            ("name", typed_field(FieldType::String)),
                        ],
                        &[],
                    ),
                ),
            ]),
        };

        let mut observed = ObservedState::default();
        observed
            .insert(observed_object(
                "a.device",
                "name=leaf01",
                json!({ "name": "leaf01" }),
                1,
            ))
            .unwrap();
        observed
            .insert(crate::ObservedObject {
                type_name: TypeName::new("b.iface"),
                key: Key::from(BTreeMap::from([
                    ("device".to_string(), json!(1)),
                    ("name".to_string(), json!("eth0")),
                ])),
                attrs: attrs_map(json!({ "name": "eth0" })),
                backend_id: Some(BackendId::Int(2)),
            })
            .unwrap();

        let device_uid = uid_v5("a.device", &key_string(&key_str("name=leaf01")));
        let error = import_err(observed, &schema);
        assert!(
            error.contains(&format!(
                "b.iface.key.device -> a.device 1: the a.device it names is {device_uid}"
            )),
            "{error}"
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
    fn import_refuses_a_type_absent_from_the_schema() {
        // an observed type the schema does not declare would load nowhere, and
        // `validate` can only call it `unknown type`, which reads as the schema
        // in `-f` missing an entry. import asks for the schema's types and
        // nothing else, so it names the list it asked for instead.
        let observed = observed_of(&[(
            "custom.thing",
            "id=x1",
            json!({ "anything": "goes", "count": 7 }),
        )]);
        let schema = schema_of(&[("dcim.cable", &["cable"], &["label"])]);

        let error = import_err(observed, &schema);
        assert!(error.contains("- custom.thing: 1 object(s)"), "{error}");
        assert!(error.contains("import asked for dcim.cable"), "{error}");
    }

    #[test]
    fn import_names_the_empty_schema_rather_than_an_empty_list() {
        // the cli asks for `schema.types.keys()`, so an empty schema empties both
        // branches of the request. naming the list would render as nothing and
        // point away from the `-f`, which is the cause in this one case.
        let observed = observed_of(&[("custom.thing", "id=x1", json!({ "anything": "goes" }))]);
        let schema = schema_of(&[]);

        let error = import_err(observed, &schema);
        assert!(error.contains("- custom.thing: 1 object(s)"), "{error}");
        assert!(
            error.contains("the schema in `-f` declares no types, so import asked for nothing"),
            "{error}"
        );
        assert!(!error.contains("asked for ;"), "{error}");
    }

    #[test]
    fn import_refuses_a_ref_to_an_undeclared_type_as_undeclared_not_as_a_cycle() {
        // red-green for the classifier: a ref to an object whose type the schema
        // omits looks observed-but-unmapped, and used to be reported as a cycle
        // that does not exist. the undeclared type is the real cause.
        let string_field = typed_field(FieldType::String);
        let schema = Schema {
            types: BTreeMap::from([(
                "c.cable".to_string(),
                typed_type(
                    &[("name", string_field)],
                    &[("interface", ref_field("b.interface"))],
                ),
            )]),
        };

        let observed = observed_of(&[
            (
                "c.cable",
                "name=c1",
                json!({ "name": "c1", "interface": 2 }),
            ),
            ("b.interface", "name=eth0", json!({ "name": "eth0" })),
        ]);

        let error = import_err(observed, &schema);
        assert!(error.contains("- b.interface: 1 object(s)"), "{error}");
        assert!(!error.contains("reference cycle"), "{error}");
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
            validation.errors.is_empty(),
            "an imported inventory must validate: {:?}",
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
