use alembic_engine::BackendCapabilities;
use anyhow::Result;
use netbox::{Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

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

    pub(super) async fn fetch_capabilities(&self) -> Result<BackendCapabilities> {
        let fields = self.client.extras().custom_fields().list(None).await?;
        let mut by_kind: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for field in fields.results {
            for object_type in field.object_types {
                by_kind
                    .entry(object_type)
                    .or_default()
                    .insert(field.name.clone());
            }
        }
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
        Ok(BackendCapabilities {
            custom_fields_by_kind: by_kind,
            tags,
        })
    }
}

impl std::ops::Deref for NetBoxClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
