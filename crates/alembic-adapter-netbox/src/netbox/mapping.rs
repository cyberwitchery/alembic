use alembic_core::Attrs;
use alembic_engine::{MissingCustomField, Op};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(super) fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in input.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            out.push(lower);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

pub(super) fn build_tag_inputs(tags: &[String]) -> Vec<netbox::models::NestedTag> {
    tags.iter()
        .map(|tag| netbox::models::NestedTag::new(tag.clone(), slugify(tag)))
        .collect()
}

pub(super) fn map_custom_fields_patch(fields: &BTreeMap<String, Value>) -> HashMap<String, Value> {
    fields
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub(super) fn map_custom_fields(
    fields: Option<HashMap<String, Value>>,
) -> Option<BTreeMap<String, Value>> {
    fields.map(|map| map.into_iter().collect())
}

pub(super) fn map_tags(tags: Option<Vec<netbox::models::NestedTag>>) -> Option<Vec<String>> {
    tags.map(|items| {
        let mut tags: Vec<String> = items.into_iter().map(|tag| tag.name).collect();
        tags.sort();
        tags
    })
}

#[derive(Default)]
pub(super) struct CustomFieldProposal {
    pub(super) object_types: BTreeSet<String>,
    pub(super) field_type: String,
}

pub(super) fn group_custom_fields(
    missing: &[MissingCustomField],
) -> BTreeMap<String, CustomFieldProposal> {
    let mut grouped: BTreeMap<String, CustomFieldProposal> = BTreeMap::new();
    for entry in missing {
        let proposal = grouped.entry(entry.field.clone()).or_default();
        proposal.object_types.insert(entry.kind.clone());
        let entry_type = custom_field_type(&entry.sample);
        proposal.field_type = merge_field_type(&proposal.field_type, entry_type);
    }
    grouped
}

pub(super) fn custom_field_type(value: &Value) -> String {
    match value {
        Value::String(_) => "text".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                "integer".to_string()
            } else {
                "decimal".to_string()
            }
        }
        Value::Array(_) | Value::Object(_) => "json".to_string(),
        Value::Null => "text".to_string(),
    }
}

pub(super) fn merge_field_type(current: &str, incoming: String) -> String {
    if current.is_empty() {
        return incoming;
    }
    if current == "json" || incoming == "json" {
        return "json".to_string();
    }
    if current == "text" || incoming == "text" {
        return "text".to_string();
    }
    if current == "decimal" || incoming == "decimal" {
        return "decimal".to_string();
    }
    if current == "boolean" || incoming == "boolean" {
        return "boolean".to_string();
    }
    "integer".to_string()
}

pub(super) fn should_skip_op(op: &Op) -> bool {
    match op {
        Op::Create { kind, desired, .. } => {
            kind.is_custom() || matches!(desired.base.attrs, Attrs::Generic(_))
        }
        Op::Update { kind, desired, .. } => {
            kind.is_custom() || matches!(desired.base.attrs, Attrs::Generic(_))
        }
        Op::Delete { kind, .. } => kind.is_custom(),
    }
}

/// map string status to netbox site status enum.
pub(super) fn site_status_from_str(
    value: &str,
) -> Result<netbox::models::writable_site_request::Status> {
    match value {
        "planned" => Ok(netbox::models::writable_site_request::Status::Planned),
        "staging" => Ok(netbox::models::writable_site_request::Status::Staging),
        "active" => Ok(netbox::models::writable_site_request::Status::Active),
        "decommissioning" => Ok(netbox::models::writable_site_request::Status::Decommissioning),
        "retired" => Ok(netbox::models::writable_site_request::Status::Retired),
        _ => Err(anyhow!("unknown site status: {value}")),
    }
}

/// map string status to netbox site status enum for patch requests.
pub(super) fn patched_site_status_from_str(
    value: &str,
) -> Result<netbox::models::patched_writable_site_request::Status> {
    match value {
        "planned" => Ok(netbox::models::patched_writable_site_request::Status::Planned),
        "staging" => Ok(netbox::models::patched_writable_site_request::Status::Staging),
        "active" => Ok(netbox::models::patched_writable_site_request::Status::Active),
        "decommissioning" => {
            Ok(netbox::models::patched_writable_site_request::Status::Decommissioning)
        }
        "retired" => Ok(netbox::models::patched_writable_site_request::Status::Retired),
        _ => Err(anyhow!("unknown site status: {value}")),
    }
}

/// map netbox location status enum to string.
pub(super) fn status_value_to_str(value: netbox::models::location_status::Value) -> &'static str {
    match value {
        netbox::models::location_status::Value::Planned => "planned",
        netbox::models::location_status::Value::Staging => "staging",
        netbox::models::location_status::Value::Active => "active",
        netbox::models::location_status::Value::Decommissioning => "decommissioning",
        netbox::models::location_status::Value::Retired => "retired",
    }
}

/// map netbox device status enum to string.
pub(super) fn device_status_to_str(value: netbox::models::device_status::Value) -> &'static str {
    match value {
        netbox::models::device_status::Value::Offline => "offline",
        netbox::models::device_status::Value::Active => "active",
        netbox::models::device_status::Value::Planned => "planned",
        netbox::models::device_status::Value::Staged => "staged",
        netbox::models::device_status::Value::Failed => "failed",
        netbox::models::device_status::Value::Inventory => "inventory",
        netbox::models::device_status::Value::Decommissioning => "decommissioning",
    }
}

/// map interface type strings to netbox create enum (subset for mvp).
pub(super) fn interface_type_from_str(
    value: Option<&str>,
) -> Result<netbox::models::writable_interface_request::RHashType> {
    match value.unwrap_or("1000base-t") {
        "1000base-t" => Ok(netbox::models::writable_interface_request::RHashType::Variant1000baseT),
        "virtual" => Ok(netbox::models::writable_interface_request::RHashType::Virtual),
        "bridge" => Ok(netbox::models::writable_interface_request::RHashType::Bridge),
        "lag" => Ok(netbox::models::writable_interface_request::RHashType::Lag),
        other => Err(anyhow!("unsupported interface type: {other}")),
    }
}

/// map interface type strings to netbox patch enum (subset for mvp).
pub(super) fn patched_interface_type_from_str(
    value: Option<&str>,
) -> Result<netbox::models::patched_writable_interface_request::RHashType> {
    match value.unwrap_or("1000base-t") {
        "1000base-t" => {
            Ok(netbox::models::patched_writable_interface_request::RHashType::Variant1000baseT)
        }
        "virtual" => Ok(netbox::models::patched_writable_interface_request::RHashType::Virtual),
        "bridge" => Ok(netbox::models::patched_writable_interface_request::RHashType::Bridge),
        "lag" => Ok(netbox::models::patched_writable_interface_request::RHashType::Lag),
        other => Err(anyhow!("unsupported interface type: {other}")),
    }
}
