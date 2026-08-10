use anyhow::{anyhow, Result};
use netbox::{Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::mapping::ExistingCustomField;
use super::registry::ObjectTypeRegistry;

#[derive(Debug, Clone)]
pub(super) struct CustomObjectType {
    pub(super) id: u64,
    pub(super) name: String,
    pub(super) object_type_name: Option<String>,
    pub(super) table_model_name: Option<String>,
    pub(super) description: Option<String>,
}

impl CustomObjectType {
    pub(super) fn object_type_parts(&self) -> Option<(String, String)> {
        self.object_type_name
            .as_deref()
            .and_then(|name| name.split_once('.'))
            .map(|(app, model)| (app.to_string(), model.to_string()))
    }
}

#[derive(Debug, Clone)]
pub(super) struct CustomObjectField {
    pub(super) id: u64,
    pub(super) custom_object_type: u64,
    pub(super) name: String,
    pub(super) current: ExistingCustomField,
}

/// an existing custom field, reduced to what a provision needs: its backend id
/// and the properties convergence compares.
#[derive(Debug, Clone)]
pub(super) struct CustomFieldDef {
    /// `None` for a field netbox listed without one: it can be detected, not patched.
    pub(super) id: Option<u64>,
    pub(super) current: ExistingCustomField,
}

pub(super) struct NetBoxClient {
    client: Client,
}

impl NetBoxClient {
    pub(super) fn new(url: &str, token: &str) -> Result<Self> {
        let config = ClientConfig::new(url, token).with_http_client_builder(|builder| {
            // avoid macOS SystemConfiguration proxy panics in CLI runs.
            builder.no_proxy()
        });
        let client = Client::new(config)?;
        Ok(Self { client })
    }

    pub(super) async fn list_all<T>(
        &self,
        resource: &netbox::Resource<T>,
        query: Option<QueryBuilder>,
    ) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let base_query = query.unwrap_or_default();
        let mut results = Vec::new();
        let mut offset = 0usize;
        let limit = 200usize;

        loop {
            let page = resource
                .list(Some(base_query.clone().limit(limit).offset(offset)))
                .await?;
            let page_count = page.results.len();
            results.extend(page.results);
            if results.len() >= page.count || page_count == 0 {
                break;
            }
            offset += limit;
        }

        Ok(results)
    }

    pub(super) async fn fetch_custom_fields(&self) -> Result<BTreeMap<String, BTreeSet<String>>> {
        Ok(self
            .fetch_custom_field_defs()
            .await?
            .into_iter()
            .map(|(object_type, fields)| (object_type, fields.into_keys().collect()))
            .collect())
    }

    /// the same read as `fetch_custom_fields`, keeping each field's definition so
    /// provisioning can converge one that already exists.
    pub(super) async fn fetch_custom_field_defs(
        &self,
    ) -> Result<BTreeMap<String, BTreeMap<String, CustomFieldDef>>> {
        let fields = self
            .list_all(&self.client.extras().custom_fields(), None)
            .await?;
        let mut by_type: BTreeMap<String, BTreeMap<String, CustomFieldDef>> = BTreeMap::new();
        for field in fields {
            let def = CustomFieldDef {
                id: field.id.map(|id| id as u64),
                current: ExistingCustomField {
                    required: field.required.unwrap_or(false),
                    description: field.description.clone().unwrap_or_default(),
                    validation_regex: field.validation_regex.clone().unwrap_or_default(),
                },
            };
            for object_type in field.object_types {
                by_type
                    .entry(object_type)
                    .or_default()
                    .insert(field.name.clone(), def.clone());
            }
        }
        Ok(by_type)
    }

    pub(super) async fn fetch_tags(&self) -> Result<BTreeSet<String>> {
        let tags = self.list_all(&self.client.extras().tags(), None).await?;
        Ok(tags.into_iter().map(|t| t.name).collect())
    }

    pub(super) async fn fetch_object_types(&self) -> Result<ObjectTypeRegistry> {
        let types = self
            .list_all(&self.client.core().object_types(), None)
            .await?;
        ObjectTypeRegistry::from_object_types(types)
    }

    pub(super) async fn fetch_custom_object_types(&self) -> Result<Option<Vec<CustomObjectType>>> {
        let resource: netbox::Resource<Value> = self
            .client
            .resource("plugins/custom-objects/custom-object-types/");
        let items = match self.list_all(&resource, None).await {
            Ok(items) => items,
            Err(err) if is_404_anyhow(&err) => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut types = Vec::new();
        for item in items {
            types.push(parse_custom_object_type(item)?);
        }
        Ok(Some(types))
    }

    pub(super) async fn fetch_custom_object_type_fields(
        &self,
    ) -> Result<Option<Vec<CustomObjectField>>> {
        let resource: netbox::Resource<Value> = self
            .client
            .resource("plugins/custom-objects/custom-object-type-fields/");
        let items = match self.list_all(&resource, None).await {
            Ok(items) => items,
            Err(err) if is_404_anyhow(&err) => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut fields = Vec::new();
        for item in items {
            fields.push(parse_custom_object_field(item)?);
        }
        Ok(Some(fields))
    }
}

impl std::ops::Deref for NetBoxClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

pub(super) fn is_404_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<netbox::Error>()
        .is_some_and(|e| matches!(e, netbox::Error::ApiError { status: 404, .. }))
}

pub(super) fn parse_custom_object_type(value: Value) -> Result<CustomObjectType> {
    let Value::Object(map) = value else {
        return Err(anyhow!("expected object for custom object type"));
    };
    let id = map
        .get("id")
        .and_then(as_u64)
        .ok_or_else(|| anyhow!("custom object type missing id"))?;
    let name = map
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("custom object type missing name"))?
        .to_string();
    let object_type_name = map
        .get("object_type_name")
        .and_then(Value::as_str)
        .map(|value| value.to_string());
    let table_model_name = map
        .get("table_model_name")
        .and_then(Value::as_str)
        .map(|value| value.to_string());
    let description = map
        .get("description")
        .and_then(Value::as_str)
        .map(|value| value.to_string());
    Ok(CustomObjectType {
        id,
        name,
        object_type_name,
        table_model_name,
        description,
    })
}

fn parse_custom_object_field(value: Value) -> Result<CustomObjectField> {
    let Value::Object(map) = value else {
        return Err(anyhow!("expected object for custom object field"));
    };
    let id = map
        .get("id")
        .and_then(as_u64)
        .ok_or_else(|| anyhow!("custom object field missing id"))?;
    let custom_object_type = map
        .get("custom_object_type")
        .and_then(parse_custom_object_type_id)
        .ok_or_else(|| anyhow!("custom object field missing custom_object_type"))?;
    let name = map
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("custom object field missing name"))?
        .to_string();
    Ok(CustomObjectField {
        id,
        custom_object_type,
        name,
        current: ExistingCustomField {
            required: map
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            description: string_or_default(map.get("description")),
            validation_regex: string_or_default(map.get("validation_regex")),
        },
    })
}

fn string_or_default(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn parse_custom_object_type_id(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(raw) => raw.parse().ok(),
        Value::Object(map) => map.get("id").and_then(as_u64),
        _ => None,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(raw) => raw.parse().ok(),
        _ => None,
    }
}
