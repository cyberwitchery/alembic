//! import of canonical inventory from backend state.

use crate::state::StateStore;
use crate::types::Observer;
use alembic_core::{key_string, uid_v5, Inventory, JsonMap, Object, Schema, TypeName};
use anyhow::Result;
use std::collections::BTreeSet;

#[derive(Debug)]
pub struct ImportReport {
    pub inventory: Inventory,
}

pub async fn import_inventory(
    adapter: &(dyn Observer + '_),
    schema: &Schema,
    types: &[TypeName],
    state: &StateStore,
) -> Result<ImportReport> {
    let observed = adapter.read(schema, types, state).await?;

    let mut objects: Vec<_> = observed.by_key.into_values().collect();
    objects.sort_by_cached_key(|o| (o.type_name.as_str().to_string(), key_string(&o.key)));

    let mut inventory_objects = Vec::new();
    let mut warned: BTreeSet<(String, String)> = BTreeSet::new();
    for mut object in objects {
        project_attrs(schema, &object.type_name, &mut object.attrs, &mut warned);
        let uid = uid_v5(object.type_name.as_str(), &key_string(&object.key));
        inventory_objects.push(Object {
            uid,
            type_name: object.type_name,
            key: object.key,
            attrs: object.attrs,
            source: None,
        });
    }

    Ok(ImportReport {
        inventory: Inventory {
            schema: schema.clone(),
            objects: inventory_objects,
        },
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
        let state = crate::state::StateStore::new(None, crate::state::StateData::default());
        let report = block_on(import_inventory(&adapter, &schema, &[], &state)).unwrap();
        assert_eq!(report.inventory.objects.len(), 1);
        let object = &report.inventory.objects[0];
        let key = key_str("site=fra1");
        assert_eq!(object.key, key);
        assert_eq!(object.uid, uid_v5("dcim.site", &key_string(&key)));
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
        let adapter = MockAdapter { observed };
        let state = crate::state::StateStore::new(None, crate::state::StateData::default());
        block_on(import_inventory(&adapter, schema, &[], &state)).unwrap()
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

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl LogBuffer {
        fn logged(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
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

        let buffer = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .finish();
        let report = tracing::subscriber::with_default(subscriber, || import(observed, &schema));

        assert_eq!(report.inventory.objects.len(), 3);
        assert_eq!(
            buffer
                .logged()
                .matches("dropping undeclared attr dcim.cable.last_updated")
                .count(),
            1,
            "the same undeclared attr warns once for the whole import, not once per object"
        );
        assert_eq!(
            buffer
                .logged()
                .matches("dropping undeclared attr dcim.site.last_updated")
                .count(),
            1,
            "each undeclared (type, field) warns once for the whole import, across types"
        );
    }
}
