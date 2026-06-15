use anyhow::Result;
use nautobot::{Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use super::mapping::slugify;
use super::registry::ObjectTypeRegistry;

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
        let fields = self
            .list_all(&self.client.extras().custom_fields(), None)
            .await?;
        let mut by_type: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for field in fields {
            let key = field.key.clone().unwrap_or_else(|| slugify(&field.label));
            for content_type in field.content_types {
                by_type.entry(content_type).or_default().insert(key.clone());
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
