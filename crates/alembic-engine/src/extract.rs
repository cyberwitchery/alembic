//! import of canonical inventory from backend state.

use crate::state::StateStore;
use crate::types::Adapter;
use alembic_core::{key_string, uid_v5, Inventory, Object, Schema, TypeName};
use anyhow::Result;

#[derive(Debug)]
pub struct ImportReport {
    pub inventory: Inventory,
}

pub async fn import_inventory(
    adapter: &(dyn Adapter + '_),
    schema: &Schema,
    types: &[TypeName],
    state: &StateStore,
) -> Result<ImportReport> {
    let observed = adapter.read(schema, types, state).await?;

    let mut objects: Vec<_> = observed.by_key.values().cloned().collect();
    objects.sort_by(|a, b| {
        (a.type_name.as_str().to_string(), key_string(&a.key))
            .cmp(&(b.type_name.as_str().to_string(), key_string(&b.key)))
    });

    let mut inventory_objects = Vec::new();
    for object in objects {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BackendId, ObservedState};
    use crate::Adapter;
    use alembic_core::{
        key_string, FieldSchema, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema,
    };
    use async_trait::async_trait;
    use futures::executor::block_on;
    use serde_json::json;
    use std::collections::BTreeMap;

    struct MockAdapter {
        observed: ObservedState,
    }

    #[async_trait]
    impl Adapter for MockAdapter {
        async fn read(
            &self,
            _schema: &Schema,
            _types: &[TypeName],
            _state: &crate::state::StateStore,
        ) -> anyhow::Result<ObservedState> {
            Ok(self.observed.clone())
        }

        async fn write(
            &self,
            _schema: &Schema,
            _ops: &[crate::Op],
            _state: &crate::state::StateStore,
        ) -> anyhow::Result<crate::ApplyReport> {
            unimplemented!("not used in import tests")
        }
    }

    fn observed_state() -> ObservedState {
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
        });
        state
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
            observed: observed_state(),
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
}
