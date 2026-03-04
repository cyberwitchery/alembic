//! nautobot adapter implementation.

mod client;
mod mapping;
mod ops;
mod registry;
mod state;

use alembic_engine::MissingCustomField;
use anyhow::Result;
use nautobot::models::CustomFieldTypeChoices;
use std::collections::BTreeSet;
use std::sync::Arc;

use client::NautobotClient;
use mapping::*;

/// nautobot adapter that maps ir objects to nautobot api calls.
pub struct NautobotAdapter {
    client: Arc<NautobotClient>,
}

impl NautobotAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str) -> Result<Self> {
        let client = Arc::new(NautobotClient::new(url, token)?);
        Ok(Self { client })
    }

    pub async fn create_custom_fields(&self, missing: &[MissingCustomField]) -> Result<()> {
        let grouped = group_custom_fields(missing);
        for (field, entry) in grouped {
            let field_type = map_field_type(&entry.field_type);

            let type_value = match field_type {
                CustomFieldTypeChoices::Text => "text",
                CustomFieldTypeChoices::Integer => "integer",
                CustomFieldTypeChoices::Boolean => "boolean",
                CustomFieldTypeChoices::Json => "json",
                _ => "text",
            };

            let request = serde_json::json!({
                "content_types": entry.object_types,
                "type": type_value,
                "name": field,
                "label": field,
                "required": false
            });

            let _ = self
                .client
                .extras()
                .custom_fields()
                .create(&request)
                .await?;
        }
        Ok(())
    }

    pub async fn create_tags(&self, tags: &[String]) -> Result<()> {
        let unique: BTreeSet<String> = tags.iter().cloned().collect();
        for tag in unique {
            let request = serde_json::json!({
                "name": tag,
                "slug": slugify(&tag),
                "content_types": []
            });
            let _ = self.client.extras().tags().create(&request).await?;
        }
        Ok(())
    }
}

fn map_field_type(name: &str) -> CustomFieldTypeChoices {
    match name {
        "text" => CustomFieldTypeChoices::Text,
        "integer" => CustomFieldTypeChoices::Integer,
        "boolean" => CustomFieldTypeChoices::Boolean,
        "json" => CustomFieldTypeChoices::Json,
        _ => CustomFieldTypeChoices::Text,
    }
}
