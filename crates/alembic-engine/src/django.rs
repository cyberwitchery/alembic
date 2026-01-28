//! django app generation from alembic ir.

use alembic_core::{FieldFormat, FieldType, Inventory, Schema, TypeName, TypeSchema};
use anyhow::Result;
use std::fs;
use std::path::Path;

const GENERATED_MODELS: &str = "generated_models.py";
const GENERATED_ADMIN: &str = "generated_admin.py";
const GENERATED_SERIALIZERS: &str = "generated_serializers.py";
const GENERATED_VIEWS: &str = "generated_views.py";
const GENERATED_URLS: &str = "generated_urls.py";
const USER_MODELS: &str = "models.py";
const USER_ADMIN: &str = "admin.py";
const USER_SERIALIZERS: &str = "serializers.py";
const USER_VIEWS: &str = "views.py";
const USER_URLS: &str = "urls.py";
const USER_EXTENSIONS: &str = "extensions.py";

const MODELS_TEMPLATE: &str = include_str!("../templates/models.py.tpl");
const ADMIN_TEMPLATE: &str = include_str!("../templates/admin.py.tpl");
const SERIALIZERS_TEMPLATE: &str = include_str!("../templates/serializers.py.tpl");
const VIEWS_TEMPLATE: &str = include_str!("../templates/views.py.tpl");
const URLS_TEMPLATE: &str = include_str!("../templates/urls.py.tpl");
const ADMIN_SEARCH_FIELDS: &[&str] = &["key", "uid"];

#[derive(Debug)]
struct ModelSpec {
    class_name: String,
    fields: Vec<FieldSpec>,
    key_fields: Vec<String>,
    has_validators: bool,
}

#[derive(Debug, Clone)]
struct FieldSpec {
    name: String,
    field_type: DjangoFieldType,
    required: bool,
    nullable: bool,
    choices: Option<Vec<String>>,
    validators: Vec<String>,
}

#[derive(Debug, Clone)]
enum DjangoFieldType {
    Char,
    Text,
    Integer,
    Float,
    Boolean,
    Uuid,
    Date,
    DateTime,
    Time,
    Json,
    Slug,
    IpAddress,
    ForeignKey { target: String },
    ManyToMany { target: String },
}

#[derive(Debug, Clone, Copy)]
pub struct DjangoEmitOptions {
    pub emit_admin: bool,
}

impl Default for DjangoEmitOptions {
    fn default() -> Self {
        Self { emit_admin: true }
    }
}

pub fn emit_django_app(
    app_dir: &Path,
    inventory: &Inventory,
    options: DjangoEmitOptions,
) -> Result<()> {
    fs::create_dir_all(app_dir)?;
    let app_name = app_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("alembic_app");

    let types = schema_types(&inventory.schema);
    let models: Vec<ModelSpec> = types
        .into_iter()
        .map(|(name, schema)| model_spec_from_schema(&name, &schema))
        .collect();

    let rendered = render_files(&models, app_name, options.emit_admin);
    fs::write(app_dir.join(GENERATED_MODELS), rendered.models)?;
    if let Some(admin) = rendered.admin {
        fs::write(app_dir.join(GENERATED_ADMIN), admin)?;
    }
    fs::write(app_dir.join(GENERATED_SERIALIZERS), rendered.serializers)?;
    fs::write(app_dir.join(GENERATED_VIEWS), rendered.views)?;
    fs::write(app_dir.join(GENERATED_URLS), rendered.urls)?;

    write_user_file(
        app_dir.join(USER_MODELS),
        user_models_stub(),
        &[default_models_stub()],
    )?;
    if options.emit_admin {
        write_user_file(
            app_dir.join(USER_ADMIN),
            user_admin_stub(),
            &[default_admin_stub()],
        )?;
    }
    write_user_file(app_dir.join(USER_SERIALIZERS), user_serializers_stub(), &[])?;
    write_user_file(
        app_dir.join(USER_VIEWS),
        user_views_stub(),
        &[default_views_stub()],
    )?;
    write_user_file(app_dir.join(USER_URLS), user_urls_stub(), &[])?;
    write_if_missing(app_dir.join(USER_EXTENSIONS), user_extensions_stub())?;

    Ok(())
}

fn schema_types(schema: &Schema) -> Vec<(TypeName, TypeSchema)> {
    let mut types: Vec<(String, TypeSchema)> = schema
        .types
        .iter()
        .map(|(name, schema)| (name.clone(), schema.clone()))
        .collect();
    types.sort_by(|a, b| a.0.cmp(&b.0));
    types
        .into_iter()
        .map(|(name, schema)| (TypeName::new(name), schema))
        .collect()
}

fn model_spec_from_schema(type_name: &TypeName, schema: &TypeSchema) -> ModelSpec {
    let class_name = class_name_for_type(type_name.as_str());
    let mut fields = Vec::new();
    let mut key_fields = Vec::new();
    let mut has_validators = false;

    for (field, field_schema) in schema.key.iter() {
        let spec = field_spec_from_schema(field, field_schema, true);
        if !spec.validators.is_empty() {
            has_validators = true;
        }
        key_fields.push(field.to_string());
        fields.push(spec);
    }

    for (field, field_schema) in schema.fields.iter() {
        if schema.key.contains_key(field) {
            continue;
        }
        let spec = field_spec_from_schema(field, field_schema, false);
        if !spec.validators.is_empty() {
            has_validators = true;
        }
        fields.push(spec);
    }

    ModelSpec {
        class_name,
        fields,
        key_fields,
        has_validators,
    }
}

struct DjangoFiles {
    models: String,
    admin: Option<String>,
    serializers: String,
    views: String,
    urls: String,
}

fn render_files(models: &[ModelSpec], app_name: &str, emit_admin: bool) -> DjangoFiles {
    let model_names: Vec<String> = models.iter().map(|m| m.class_name.clone()).collect();
    let model_import = import_line("from .generated_models import ", &model_names);
    let serializer_names: Vec<String> = model_names
        .iter()
        .map(|name| format!("{name}Serializer"))
        .collect();
    let serializer_import = import_line("from .generated_serializers import ", &serializer_names);
    let view_names: Vec<String> = model_names
        .iter()
        .map(|name| format!("{name}ViewSet"))
        .collect();
    let view_import = import_line("from .generated_views import ", &view_names);

    let models_block = render_models_block(models);
    let validators_import = if models.iter().any(|m| m.has_validators) {
        "from django.core.validators import RegexValidator"
    } else {
        ""
    };
    let admins_block = if emit_admin {
        render_admins_block(models)
    } else {
        String::new()
    };
    let serializers_block = render_serializers_block(models);
    let views_block = render_views_block(models);
    let routes_block = render_routes_block(models);

    let models = render_template(
        MODELS_TEMPLATE,
        &[
            ("validators_import", validators_import.to_string()),
            ("models", models_block),
        ],
    );
    let admin = if emit_admin {
        Some(render_template(
            ADMIN_TEMPLATE,
            &[
                ("model_import", model_import.clone()),
                ("admins", admins_block),
            ],
        ))
    } else {
        None
    };
    let serializers = render_template(
        SERIALIZERS_TEMPLATE,
        &[
            ("model_import", model_import.clone()),
            ("serializers", serializers_block),
        ],
    );
    let views = render_template(
        VIEWS_TEMPLATE,
        &[
            ("model_import", model_import),
            ("serializer_import", serializer_import),
            ("views", views_block),
        ],
    );
    let urls = render_template(
        URLS_TEMPLATE,
        &[
            ("view_import", view_import),
            ("routes", routes_block),
            ("app_name", app_name.to_string()),
        ],
    );

    DjangoFiles {
        models,
        admin,
        serializers,
        views,
        urls,
    }
}

fn render_field(field: &FieldSpec) -> String {
    let mut args = Vec::new();
    if let Some(choices) = &field.choices {
        let choice_items = choices
            .iter()
            .map(|value| format!("(\"{value}\", \"{value}\")"))
            .collect::<Vec<_>>()
            .join(", ");
        args.push(format!("choices=[{choice_items}]"));
    }
    if !field.validators.is_empty() {
        let validators = field.validators.join(", ");
        args.push(format!("validators=[{validators}]"));
    }
    if !field.required {
        args.push("blank=True".to_string());
    }
    if field.nullable {
        args.push("null=True".to_string());
    }
    if matches!(field.field_type, DjangoFieldType::IpAddress)
        && args.iter().any(|arg| arg == "blank=True")
        && !args.iter().any(|arg| arg == "null=True")
    {
        args.push("null=True".to_string());
    }

    let args_str = args.join(", ");

    match &field.field_type {
        DjangoFieldType::Char => {
            if args_str.is_empty() {
                format!("{} = models.CharField(max_length=255)", field.name)
            } else {
                format!(
                    "{} = models.CharField(max_length=255, {})",
                    field.name, args_str
                )
            }
        }
        DjangoFieldType::Text => format!("{} = models.TextField({})", field.name, args_str),
        DjangoFieldType::Integer => format!("{} = models.IntegerField({})", field.name, args_str),
        DjangoFieldType::Float => format!("{} = models.FloatField({})", field.name, args_str),
        DjangoFieldType::Boolean => format!("{} = models.BooleanField({})", field.name, args_str),
        DjangoFieldType::Uuid => format!("{} = models.UUIDField({})", field.name, args_str),
        DjangoFieldType::Date => format!("{} = models.DateField({})", field.name, args_str),
        DjangoFieldType::DateTime => format!("{} = models.DateTimeField({})", field.name, args_str),
        DjangoFieldType::Time => format!("{} = models.TimeField({})", field.name, args_str),
        DjangoFieldType::Json => format!("{} = models.JSONField({})", field.name, args_str),
        DjangoFieldType::Slug => format!("{} = models.SlugField({})", field.name, args_str),
        DjangoFieldType::IpAddress => {
            format!(
                "{} = models.GenericIPAddressField({})",
                field.name, args_str
            )
        }
        DjangoFieldType::ForeignKey { target } => {
            let mut fk_args = vec![
                format!("\"{}\"", target),
                "on_delete=models.PROTECT".to_string(),
            ];
            fk_args.extend(args);
            format!("{} = models.ForeignKey({})", field.name, fk_args.join(", "))
        }
        DjangoFieldType::ManyToMany { target } => {
            let mut m2m_args = vec![format!("\"{}\"", target)];
            m2m_args.extend(args.into_iter().filter(|arg| arg != "null=True"));
            format!(
                "{} = models.ManyToManyField({})",
                field.name,
                m2m_args.join(", ")
            )
        }
    }
}

fn render_models_block(models: &[ModelSpec]) -> String {
    models
        .iter()
        .map(render_model_block)
        .collect::<Vec<String>>()
        .join("\n")
}

fn render_model_block(model: &ModelSpec) -> String {
    let mut fields = Vec::with_capacity(model.fields.len() + 3);
    fields.push("uid = models.UUIDField(primary_key=True, editable=False)".to_string());
    fields.push("key = models.TextField()".to_string());
    fields.push("attrs = models.JSONField(default=dict, blank=True)".to_string());
    for field in &model.fields {
        fields.push(render_field(field));
    }

    let mut lines = Vec::new();
    lines.push(format!("class {}(models.Model):", model.class_name));
    lines.push(format!("    {}", fields.join("\n    ")));

    if !model.key_fields.is_empty() {
        let unique_fields = model
            .key_fields
            .iter()
            .map(|field| format!("\"{field}\""))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push("".to_string());
        lines.push("    class Meta:".to_string());
        lines.push(format!(
            "        constraints = [models.UniqueConstraint(fields=[{unique_fields}], name=\"{}_key\")]",
            model.class_name.to_lowercase()
        ));
    }

    lines.join("\n") + "\n"
}

fn render_admins_block(models: &[ModelSpec]) -> String {
    models
        .iter()
        .map(render_admin_block)
        .collect::<Vec<String>>()
        .join("\n")
}

fn render_admin_block(model: &ModelSpec) -> String {
    let list_display = admin_list_display(model);
    let list_filter = admin_list_filter(model);
    let mut lines = vec![
        format!(
            "@admin.register({})\nclass {}Admin(admin.ModelAdmin):",
            model.class_name, model.class_name
        ),
        format!("    list_display = [{}]", join_quoted(&list_display)),
        format!("    search_fields = [{}]", join_quoted(ADMIN_SEARCH_FIELDS)),
    ];
    if !list_filter.is_empty() {
        lines.push(format!("    list_filter = [{}]", join_quoted(&list_filter)));
    }
    lines.join("\n")
}

fn render_serializers_block(models: &[ModelSpec]) -> String {
    models
        .iter()
        .map(render_serializer_block)
        .collect::<Vec<String>>()
        .join("\n")
}

fn render_serializer_block(model: &ModelSpec) -> String {
    let fields = serializer_fields(model);
    format!(
        "class {}Serializer(serializers.ModelSerializer):\n    class Meta:\n        model = {}\n        fields = [{}]\n",
        model.class_name,
        model.class_name,
        join_quoted(&fields)
    )
}

fn render_views_block(models: &[ModelSpec]) -> String {
    models
        .iter()
        .map(render_view_block)
        .collect::<Vec<String>>()
        .join("\n")
}

fn render_view_block(model: &ModelSpec) -> String {
    let list_display = admin_list_display(model);
    let search_fields = admin_search_fields_for_model(model);
    format!(
        "class {}ViewSet(viewsets.ModelViewSet):\n    queryset = {}.objects.all()\n    serializer_class = {}Serializer\n    filterset_fields = [{}]\n    search_fields = [{}]\n    ordering_fields = [{}]\n",
        model.class_name,
        model.class_name,
        model.class_name,
        join_quoted(&list_display),
        join_quoted(&search_fields),
        join_quoted(&list_display)
    )
}

fn render_routes_block(models: &[ModelSpec]) -> String {
    models
        .iter()
        .map(|model| {
            let endpoint = pluralize(model.class_name.to_lowercase().as_str());
            format!(
                "router.register(\"{endpoint}\", {}ViewSet)",
                model.class_name
            )
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn field_name(field: &FieldSpec) -> &str {
    field.name.as_str()
}

fn join_quoted(fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| format!("\"{field}\""))
        .collect::<Vec<String>>()
        .join(", ")
}

fn import_line<T: AsRef<str>>(prefix: &str, names: &[T]) -> String {
    if names.is_empty() {
        String::new()
    } else {
        format!(
            "{prefix}{}",
            names
                .iter()
                .map(|name| name.as_ref())
                .collect::<Vec<&str>>()
                .join(", ")
        )
    }
}

fn admin_list_display(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["key", "uid"];
    for field in &model.fields {
        fields.push(field_name(field));
    }
    fields
}

fn admin_search_fields_for_model(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["key"];
    for field in &model.fields {
        match field.field_type {
            DjangoFieldType::Char
            | DjangoFieldType::Text
            | DjangoFieldType::Slug
            | DjangoFieldType::Uuid
            | DjangoFieldType::IpAddress => fields.push(field_name(field)),
            _ => {}
        }
    }
    fields
}

fn admin_list_filter(model: &ModelSpec) -> Vec<&str> {
    let mut fields = Vec::new();
    for field in &model.fields {
        match field.field_type {
            DjangoFieldType::Boolean => fields.push(field_name(field)),
            _ => {
                if field.name == "status" {
                    fields.push(field_name(field));
                }
            }
        }
    }
    fields
}

fn serializer_fields(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["uid", "key", "attrs"];
    for field in &model.fields {
        fields.push(field_name(field));
    }
    fields
}

fn class_name_for_type(type_name: &str) -> String {
    type_name
        .split('.')
        .map(|segment| {
            segment
                .split('_')
                .filter(|s| !s.is_empty())
                .map(|part| {
                    let mut chars = part.chars();
                    match chars.next() {
                        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                        None => String::new(),
                    }
                })
                .collect::<String>()
        })
        .collect::<String>()
}

fn pluralize(name: &str) -> String {
    if name.ends_with('s') {
        format!("{name}es")
    } else if let Some(stripped) = name.strip_suffix('y') {
        format!("{}ies", stripped)
    } else {
        format!("{name}s")
    }
}

fn field_spec_from_schema(
    name: &str,
    schema: &alembic_core::FieldSchema,
    is_key: bool,
) -> FieldSpec {
    let mut validators = Vec::new();
    let mut choices = None;

    if let Some(format) = &schema.format {
        validators.push(format_validator(format));
    }
    if let Some(pattern) = &schema.pattern {
        validators.push(format!("RegexValidator(r\"{pattern}\")"));
    }

    let field_type = match &schema.r#type {
        FieldType::String => DjangoFieldType::Char,
        FieldType::Text => DjangoFieldType::Text,
        FieldType::Int => DjangoFieldType::Integer,
        FieldType::Float => DjangoFieldType::Float,
        FieldType::Bool => DjangoFieldType::Boolean,
        FieldType::Uuid => DjangoFieldType::Uuid,
        FieldType::Date => DjangoFieldType::Date,
        FieldType::Datetime => DjangoFieldType::DateTime,
        FieldType::Time => DjangoFieldType::Time,
        FieldType::Json => DjangoFieldType::Json,
        FieldType::IpAddress => DjangoFieldType::IpAddress,
        FieldType::Cidr | FieldType::Prefix | FieldType::Mac => {
            validators.push(format_validator(&format_for_field_type(&schema.r#type)));
            DjangoFieldType::Char
        }
        FieldType::Slug => DjangoFieldType::Slug,
        FieldType::Enum { values } => {
            choices = Some(values.clone());
            DjangoFieldType::Char
        }
        FieldType::List { .. } | FieldType::Map { .. } => DjangoFieldType::Json,
        FieldType::Ref { target } => DjangoFieldType::ForeignKey {
            target: class_name_for_type(target),
        },
        FieldType::ListRef { target } => DjangoFieldType::ManyToMany {
            target: class_name_for_type(target),
        },
    };

    let required = schema.required || is_key;
    let nullable = schema.nullable && !is_key;

    FieldSpec {
        name: name.to_string(),
        field_type,
        required,
        nullable,
        choices,
        validators,
    }
}

fn format_for_field_type(field_type: &FieldType) -> FieldFormat {
    match field_type {
        FieldType::IpAddress => FieldFormat::IpAddress,
        FieldType::Cidr => FieldFormat::Cidr,
        FieldType::Prefix => FieldFormat::Prefix,
        FieldType::Mac => FieldFormat::Mac,
        FieldType::Uuid => FieldFormat::Uuid,
        FieldType::Slug => FieldFormat::Slug,
        _ => FieldFormat::Slug,
    }
}

fn format_validator(format: &FieldFormat) -> String {
    match format {
        FieldFormat::Slug => "RegexValidator(r\"^[a-z0-9]+(?:[a-z0-9_-]*[a-z0-9])?$\")".to_string(),
        FieldFormat::IpAddress => {
            "RegexValidator(r\"^([0-9]{1,3}\\.){3}[0-9]{1,3}$|^[0-9a-fA-F:]+$\")".to_string()
        }
        FieldFormat::Cidr | FieldFormat::Prefix => {
            "RegexValidator(r\"^[0-9a-fA-F:\\./]+$\")".to_string()
        }
        FieldFormat::Mac => {
            "RegexValidator(r\"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$\")".to_string()
        }
        FieldFormat::Uuid => {
            "RegexValidator(r\"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$\")".to_string()
        }
    }
}

fn write_if_missing(path: impl AsRef<Path>, contents: &str) -> Result<()> {
    let path = path.as_ref();
    if path.exists() {
        return Ok(());
    }
    fs::write(path, contents)?;
    Ok(())
}

fn render_template(template: &str, vars: &[(&str, String)]) -> String {
    let mut output = template.to_string();
    for (key, value) in vars {
        let token = format!("{{{{{key}}}}}");
        output = output.replace(&token, value);
    }
    output
}

fn write_user_file(path: impl AsRef<Path>, contents: &str, defaults: &[&str]) -> Result<()> {
    let path = path.as_ref();
    if path.exists() {
        let existing = fs::read_to_string(path)?;
        let normalized = existing.trim().replace("\r\n", "\n");
        let is_default = defaults
            .iter()
            .any(|candidate| candidate.trim().replace("\r\n", "\n") == normalized);
        if !is_default {
            return Ok(());
        }
    }
    fs::write(path, contents)?;
    Ok(())
}

fn user_models_stub() -> &'static str {
    "from .generated_models import *  # noqa: F401,F403\nfrom .extensions import *  # noqa: F401,F403\n"
}

fn user_admin_stub() -> &'static str {
    "from .generated_admin import *  # noqa: F401,F403\nfrom .extensions import *  # noqa: F401,F403\n"
}

fn user_serializers_stub() -> &'static str {
    "from .generated_serializers import *  # noqa: F401,F403\nfrom .extensions import *  # noqa: F401,F403\n"
}

fn user_views_stub() -> &'static str {
    "from .generated_views import *  # noqa: F401,F403\nfrom .extensions import *  # noqa: F401,F403\n"
}

fn user_urls_stub() -> &'static str {
    "from .generated_urls import *  # noqa: F401,F403\n"
}

fn user_extensions_stub() -> &'static str {
    "# User extension hooks live here.\n"
}

fn default_models_stub() -> &'static str {
    "from django.db import models\n\n# Create your models here.\n"
}

fn default_admin_stub() -> &'static str {
    "from django.contrib import admin\n\n# Register your models here.\n"
}

fn default_views_stub() -> &'static str {
    "from django.shortcuts import render\n\n# Create your views here.\n"
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{
        FieldSchema, FieldType, Inventory, JsonMap, Object, Schema, TypeName, TypeSchema,
    };
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn attrs_map(pairs: Vec<(&str, Value)>) -> JsonMap {
        JsonMap::from(
            pairs
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    fn key_str(raw: &str) -> alembic_core::Key {
        let mut map = BTreeMap::new();
        for segment in raw.split('/') {
            let (field, value) = segment
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid key segment: {segment}"));
            map.insert(field.to_string(), Value::String(value.to_string()));
        }
        alembic_core::Key::from(map)
    }

    fn obj(uid: u128, type_name: &str, key: &str, attrs: JsonMap) -> Object {
        Object::new(
            Uuid::from_u128(uid),
            TypeName::new(type_name),
            key_str(key),
            attrs,
        )
        .unwrap()
    }

    fn test_schema() -> Schema {
        let mut types = BTreeMap::new();
        types.insert(
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([(
                    "slug".to_string(),
                    FieldSchema {
                        r#type: FieldType::Slug,
                        required: true,
                        nullable: false,
                        description: None,
                        format: None,
                        pattern: None,
                    },
                )]),
                fields: BTreeMap::from([
                    (
                        "name".to_string(),
                        FieldSchema {
                            r#type: FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                    (
                        "slug".to_string(),
                        FieldSchema {
                            r#type: FieldType::Slug,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                ]),
            },
        );
        types.insert(
            "dcim.device".to_string(),
            TypeSchema {
                key: BTreeMap::from([(
                    "name".to_string(),
                    FieldSchema {
                        r#type: FieldType::Slug,
                        required: true,
                        nullable: false,
                        description: None,
                        format: None,
                        pattern: None,
                    },
                )]),
                fields: BTreeMap::from([
                    (
                        "name".to_string(),
                        FieldSchema {
                            r#type: FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                    (
                        "site".to_string(),
                        FieldSchema {
                            r#type: FieldType::Ref {
                                target: "dcim.site".to_string(),
                            },
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                    (
                        "role".to_string(),
                        FieldSchema {
                            r#type: FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                    (
                        "device_type".to_string(),
                        FieldSchema {
                            r#type: FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                ]),
            },
        );
        types.insert(
            "dcim.interface".to_string(),
            TypeSchema {
                key: BTreeMap::from([(
                    "name".to_string(),
                    FieldSchema {
                        r#type: FieldType::Slug,
                        required: true,
                        nullable: false,
                        description: None,
                        format: None,
                        pattern: None,
                    },
                )]),
                fields: BTreeMap::from([
                    (
                        "name".to_string(),
                        FieldSchema {
                            r#type: FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                    (
                        "device".to_string(),
                        FieldSchema {
                            r#type: FieldType::Ref {
                                target: "dcim.device".to_string(),
                            },
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    ),
                ]),
            },
        );
        Schema { types }
    }

    fn sample_inventory() -> Inventory {
        let objects = vec![
            obj(
                1,
                "dcim.device",
                "name=leaf01",
                attrs_map(vec![
                    ("name", json!("leaf01")),
                    ("site", json!(Uuid::from_u128(2).to_string())),
                    ("role", json!("leaf")),
                    ("device_type", json!("leaf-switch")),
                ]),
            ),
            obj(
                2,
                "dcim.site",
                "slug=fra1",
                attrs_map(vec![("name", json!("FRA1")), ("slug", json!("fra1"))]),
            ),
            obj(
                3,
                "dcim.interface",
                "name=eth0",
                attrs_map(vec![
                    ("name", json!("eth0")),
                    ("device", json!(Uuid::from_u128(1).to_string())),
                ]),
            ),
        ];
        Inventory {
            schema: test_schema(),
            objects,
        }
    }

    #[test]
    fn emit_django_app_writes_files_and_stubs() {
        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions::default(),
        )
        .unwrap();

        assert!(dir.path().join(GENERATED_MODELS).exists());
        assert!(dir.path().join(GENERATED_ADMIN).exists());
        assert!(dir.path().join(GENERATED_SERIALIZERS).exists());
        assert!(dir.path().join(GENERATED_VIEWS).exists());
        assert!(dir.path().join(GENERATED_URLS).exists());
        assert!(dir.path().join(USER_MODELS).exists());
        assert!(dir.path().join(USER_ADMIN).exists());
        assert!(dir.path().join(USER_SERIALIZERS).exists());
        assert!(dir.path().join(USER_VIEWS).exists());
        assert!(dir.path().join(USER_URLS).exists());
        assert!(dir.path().join(USER_EXTENSIONS).exists());

        let models = fs::read_to_string(dir.path().join(GENERATED_MODELS)).unwrap();
        assert!(models.contains("class DcimSite"));
        assert!(models.contains("site = models.ForeignKey(\"DcimSite\""));
        assert!(models.contains("device = models.ForeignKey(\"DcimDevice\""));
        assert!(models.contains("uid = models.UUIDField"));
        assert!(models.contains("attrs = models.JSONField"));
    }

    #[test]
    fn emit_django_app_does_not_overwrite_user_files() {
        let dir = tempdir().unwrap();
        let models_path = dir.path().join(USER_MODELS);
        let admin_path = dir.path().join(USER_ADMIN);
        fs::write(&models_path, "# user models\n").unwrap();
        fs::write(&admin_path, "# user admin\n").unwrap();

        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions::default(),
        )
        .unwrap();

        assert_eq!(fs::read_to_string(models_path).unwrap(), "# user models\n");
        assert_eq!(fs::read_to_string(admin_path).unwrap(), "# user admin\n");
    }

    #[test]
    fn emit_django_app_overwrites_default_skeleton() {
        let dir = tempdir().unwrap();
        let models_path = dir.path().join(USER_MODELS);
        let admin_path = dir.path().join(USER_ADMIN);
        let views_path = dir.path().join(USER_VIEWS);
        fs::write(&models_path, default_models_stub()).unwrap();
        fs::write(&admin_path, default_admin_stub()).unwrap();
        fs::write(&views_path, default_views_stub()).unwrap();

        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions::default(),
        )
        .unwrap();

        let models = fs::read_to_string(models_path).unwrap();
        let admin = fs::read_to_string(admin_path).unwrap();
        let views = fs::read_to_string(views_path).unwrap();
        assert!(models.contains("generated_models"));
        assert!(admin.contains("generated_admin"));
        assert!(views.contains("generated_views"));
    }

    #[test]
    fn generated_admin_includes_defaults() {
        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions::default(),
        )
        .unwrap();
        let admin = fs::read_to_string(dir.path().join(GENERATED_ADMIN)).unwrap();

        assert!(admin.contains("class DcimDeviceAdmin"));
        assert!(admin.contains(
            "list_display = [\"key\", \"uid\", \"name\", \"device_type\", \"role\", \"site\"]"
        ));
        assert!(admin.contains("search_fields = [\"key\", \"uid\"]"));
        assert!(admin.contains("class DcimInterfaceAdmin"));
    }

    #[test]
    fn generated_api_files_include_models() {
        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions::default(),
        )
        .unwrap();
        let serializers = fs::read_to_string(dir.path().join(GENERATED_SERIALIZERS)).unwrap();
        let views = fs::read_to_string(dir.path().join(GENERATED_VIEWS)).unwrap();
        let urls = fs::read_to_string(dir.path().join(GENERATED_URLS)).unwrap();

        assert!(serializers.contains("class DcimDeviceSerializer"));
        assert!(views.contains("class DcimDeviceViewSet"));
        assert!(urls.contains("router.register(\"dcimdevices\""));
        assert!(urls.contains("schema_view"));
    }

    #[test]
    fn generated_models_are_deterministic_by_kind() {
        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions::default(),
        )
        .unwrap();

        let models = fs::read_to_string(dir.path().join(GENERATED_MODELS)).unwrap();
        let device_pos = models.find("class DcimDevice").unwrap();
        let interface_pos = models.find("class DcimInterface").unwrap();
        let site_pos = models.find("class DcimSite").unwrap();
        assert!(device_pos < interface_pos);
        assert!(interface_pos < site_pos);
    }
}
