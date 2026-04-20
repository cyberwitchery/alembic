use anyhow::Result;
use nautobot::{Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

use super::mapping::slugify;
use super::registry::ObjectTypeRegistry;

pub(super) struct NautobotClient {
    client: Client,
}

impl NautobotClient {
    pub(super) fn new(url: &str, token: &str) -> Result<Self> {
        let config = ClientConfig::new(url, token);
        let client = Client::new(config)?;
        Ok(Self { client })
    }

    pub(super) fn resource<T: DeserializeOwned>(&self, endpoint: String) -> nautobot::Resource<T> {
        let path: &'static str = Box::leak(endpoint.into_boxed_str());
        nautobot::Resource::new(self.client.clone(), path)
    }

    pub(super) async fn list_all<T>(&self, resource: &nautobot::Resource<T>, query: Option<QueryBuilder>) -> Result<Vec<T>>
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
