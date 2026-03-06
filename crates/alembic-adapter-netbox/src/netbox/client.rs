use anyhow::{anyhow, Result};
use netbox::{Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::registry::ObjectTypeRegistry;

#[derive(Debug, Clone)]
pub(super) struct CustomObjectType {
    pub(super) id: u64,
    pub(super) name: String,
    pub(super) object_type_name: Option<String>,
    pub(super) table_model_name: Option<String>,
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
    pub(super) custom_object_type: u64,
    pub(super) name: String,
}

pub(super) struct NetBoxClient {
    client: Client,
}

impl NetBoxClient {
    pub(super) fn new(url: &str, token: &str) -> Result<Self> {
        let config = ClientConfig::new(url, token);
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
        let fields = self.client.extras().custom_fields().list(None).await?;
        let mut by_type: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for field in fields.results {
            for object_type in field.object_types {
                by_type
                    .entry(object_type)
                    .or_default()
                    .insert(field.name.clone());
            }
        }
        Ok(by_type)
    }

    pub(super) async fn fetch_tags(&self) -> Result<BTreeSet<String>> {
        let mut tags = BTreeSet::new();
        let mut offset = 0usize;
        let limit = 200usize;
        loop {
            let page = self
                .client
                .extras()
                .tags()
                .list(Some(QueryBuilder::default().limit(limit).offset(offset)))
                .await?;
            let page_count = page.results.len();
            for tag in page.results {
                tags.insert(tag.name);
            }
            if tags.len() >= page.count || page_count == 0 {
                break;
            }
            offset += limit;
        }
        Ok(tags)
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
            Err(err) if is_404_error(&err) => return Ok(None),
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
            Err(err) if is_404_error(&err) => return Ok(None),
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

fn is_404_error(err: &anyhow::Error) -> bool {
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
    Ok(CustomObjectType {
        id,
        name,
        object_type_name,
        table_model_name,
    })
}

fn parse_custom_object_field(value: Value) -> Result<CustomObjectField> {
    let Value::Object(map) = value else {
        return Err(anyhow!("expected object for custom object field"));
    };
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
        custom_object_type,
        name,
    })
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
