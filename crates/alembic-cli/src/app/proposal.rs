use super::diag::warn;
use super::Backend;
use alembic_engine::{
    build_plan_with_projection, missing_custom_fields, missing_tags, plan, project_and_observe,
    Adapter, Plan, ProjectionSpec, StateStore,
};
use anyhow::Result;

pub(super) async fn build_plan_with_proposal(
    adapter: &dyn Adapter,
    inventory: &alembic_core::Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    projection: Option<&ProjectionSpec>,
    projection_strict: bool,
    backend: Backend,
) -> Result<Plan> {
    let Some(spec) = projection else {
        return build_plan_with_projection(
            adapter,
            inventory,
            state,
            allow_delete,
            None,
            projection_strict,
        )
        .await;
    };
    let (projected, mut observed) =
        project_and_observe(adapter, inventory, state, Some(spec)).await?;
    let missing = missing_custom_fields(spec, &inventory.objects, &observed.capabilities)?;
    if !missing.is_empty() {
        warn("proposal", "projection proposal: missing custom fields");
        for entry in &missing {
            warn(
                "proposal",
                &format!(
                    "- rule {} (type {}, attr {}) -> field {}",
                    entry.rule, entry.type_name, entry.attr_key, entry.field
                ),
            );
        }
        let prompt = format!("create missing custom fields in {:?}? [y/N] ", backend);
        if confirm(&prompt)? {
            adapter.create_custom_fields(&missing).await?;
            for entry in &missing {
                observed
                    .capabilities
                    .custom_fields_by_type
                    .entry(entry.type_name.clone())
                    .or_default()
                    .insert(entry.field.clone());
            }
        }
    }
    let missing_tags = missing_tags(spec, &inventory.objects, &observed.capabilities)?;
    if !missing_tags.is_empty() {
        warn("proposal", "projection proposal: missing tags");
        for entry in &missing_tags {
            warn(
                "proposal",
                &format!(
                    "- rule {} (type {}, attr {}) -> tag {}",
                    entry.rule, entry.type_name, entry.attr_key, entry.tag
                ),
            );
        }
        let prompt = format!("create missing tags in {:?}? [y/N] ", backend);
        if confirm(&prompt)? {
            let tags: Vec<String> = missing_tags.iter().map(|entry| entry.tag.clone()).collect();
            adapter.create_tags(&tags).await?;
            for tag in tags {
                observed.capabilities.tags.insert(tag);
            }
        }
    }
    if projection_strict {
        alembic_engine::validate_projection_strict(
            spec,
            &inventory.objects,
            &observed.capabilities,
        )?;
    }
    Ok(plan(
        &projected,
        &observed,
        state,
        &inventory.schema,
        allow_delete,
    ))
}

pub(super) fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{self, Write};
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim().to_lowercase().as_str(), "y" | "yes"))
}
