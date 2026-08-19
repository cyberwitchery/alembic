use anyhow::Result;
use nautobot::{Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use super::mapping::{slugify, ExistingCustomField};
use super::registry::ObjectTypeRegistry;

/// an existing custom field, reduced to what a provision needs: its backend id
/// and the properties convergence compares.
#[derive(Debug, Clone)]
pub(super) struct CustomFieldDef {
    /// `None` for a field nautobot listed without one: it can be detected, not patched.
    pub(super) id: Option<String>,
    pub(super) current: ExistingCustomField,
}

pub(super) struct NautobotClient {
    client: Client,
    /// cache of leaked endpoint paths so each distinct endpoint is leaked at
    /// most once for the process lifetime (`nautobot::Resource` needs `&'static str`).
    interned: Mutex<HashMap<String, &'static str>>,
}

impl NautobotClient {
    pub(super) fn new(url: &str, token: &str) -> Result<Self> {
        let config = ClientConfig::new(url, token);
        let client = Client::new(config)?;
        Ok(Self {
            client,
            interned: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn resource<T: DeserializeOwned>(&self, endpoint: String) -> nautobot::Resource<T> {
        nautobot::Resource::new(self.client.clone(), self.intern(endpoint))
    }

    /// return a `&'static str` for `endpoint`, leaking it at most once per
    /// distinct value. `resource()` is called per object type and on every
    /// per-op path; leaking on every call would re-leak the same endpoint
    /// string repeatedly, an unbounded leak over a long-running reconcile loop.
    fn intern(&self, endpoint: String) -> &'static str {
        let mut interned = self
            .interned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        interned
            .entry(endpoint)
            .or_insert_with_key(|key| Box::leak(key.clone().into_boxed_str()))
    }

    pub(super) async fn list_all<T>(
        &self,
        resource: &nautobot::Resource<T>,
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
            .map(|(content_type, fields)| (content_type, fields.into_keys().collect()))
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
            let key = field.key.clone().unwrap_or_else(|| slugify(&field.label));
            let def = CustomFieldDef {
                id: field.id.map(|id| id.to_string()),
                current: ExistingCustomField {
                    required: field.required.unwrap_or(false),
                    description: field.description.clone().unwrap_or_default(),
                    validation_regex: field.validation_regex.clone().unwrap_or_default(),
                },
            };
            for content_type in field.content_types {
                by_type
                    .entry(content_type)
                    .or_default()
                    .insert(key.clone(), def.clone());
            }
        }
        Ok(by_type)
    }

    /// the choices each `select`/`multi-select` custom field currently offers,
    /// keyed by backend field id. read untyped for the reason the create posts
    /// untyped: the generated `CustomFieldChoice` mis-decodes its nested
    /// `custom_field`.
    pub(super) async fn fetch_custom_field_choices(
        &self,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let resource: nautobot::Resource<Value> =
            self.resource("extras/custom-field-choices/".to_string());
        let mut by_field: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for choice in self.list_all(&resource, None).await? {
            // a read nests `custom_field`; a create posts it as a bare id.
            let field = choice.get("custom_field");
            let id = field.and_then(Value::as_str).or_else(|| {
                field
                    .and_then(|field| field.get("id"))
                    .and_then(Value::as_str)
            });
            let (Some(id), Some(value)) = (id, choice.get("value").and_then(Value::as_str)) else {
                continue;
            };
            by_field
                .entry(id.to_string())
                .or_default()
                .insert(value.to_string());
        }
        Ok(by_field)
    }

    pub(super) async fn fetch_tags(&self) -> Result<BTreeSet<String>> {
        let tags = self.list_all(&self.client.extras().tags(), None).await?;
        Ok(tags.into_iter().map(|t| t.name).collect())
    }

    pub(super) async fn fetch_object_types(&self) -> Result<ObjectTypeRegistry> {
        let types = self
            .list_all(&self.client.extras().content_types(), None)
            .await?;
        ObjectTypeRegistry::from_content_types(types)
    }
}

impl std::ops::Deref for NautobotClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_reuses_one_leak_per_distinct_endpoint() {
        let client = NautobotClient::new("https://example.com", "token").unwrap();

        // the same endpoint string interns to the same &'static str, so a second
        // resource() for the same path reuses the leak instead of re-leaking it.
        let first = client.intern("dcim/devices/".to_string());
        let second = client.intern("dcim/devices/".to_string());
        assert!(
            std::ptr::eq(first.as_ptr(), second.as_ptr()),
            "repeated endpoint should return the same interned pointer"
        );

        // distinct endpoints still get their own (single) leak.
        let other = client.intern("ipam/prefixes/".to_string());
        assert!(
            !std::ptr::eq(first.as_ptr(), other.as_ptr()),
            "distinct endpoints should intern to distinct pointers"
        );
    }
}
