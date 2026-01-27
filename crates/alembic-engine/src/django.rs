//! django app generation from alembic ir.

use alembic_core::{key_string, Inventory, TypeName};
use anyhow::Result;
use std::collections::BTreeSet;
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
    class_name: &'static str,
    fields: Vec<FieldSpec>,
}

#[derive(Debug)]
enum FieldSpec {
    Text {
        name: &'static str,
        optional: bool,
    },
    Bool {
        name: &'static str,
        optional: bool,
    },
    ForeignKey {
        name: &'static str,
        target: &'static str,
        optional: bool,
    },
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

    let types = inventory_types(inventory);
    let models: Vec<ModelSpec> = types.iter().filter_map(model_spec).collect();

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

fn inventory_types(inventory: &Inventory) -> Vec<TypeName> {
    let mut objects = inventory.objects.clone();
    objects.sort_by(|a, b| {
        (a.type_name.as_str().to_string(), key_string(&a.key))
            .cmp(&(b.type_name.as_str().to_string(), key_string(&b.key)))
    });

    let mut seen = BTreeSet::new();
    let mut types = Vec::new();
    for object in objects {
        if seen.insert(object.type_name.as_str().to_string()) {
            types.push(object.type_name);
        }
    }
    types
}

fn model_spec(type_name: &TypeName) -> Option<ModelSpec> {
    match type_name.as_str() {
        "dcim.site" => Some(ModelSpec {
            class_name: "Site",
            fields: vec![
                FieldSpec::Text {
                    name: "name",
                    optional: false,
                },
                FieldSpec::Text {
                    name: "slug",
                    optional: false,
                },
                FieldSpec::Text {
                    name: "status",
                    optional: true,
                },
                FieldSpec::Text {
                    name: "description",
                    optional: true,
                },
            ],
        }),
        "dcim.device" => Some(ModelSpec {
            class_name: "Device",
            fields: vec![
                FieldSpec::Text {
                    name: "name",
                    optional: false,
                },
                FieldSpec::ForeignKey {
                    name: "site",
                    target: "Site",
                    optional: false,
                },
                FieldSpec::Text {
                    name: "role",
                    optional: false,
                },
                FieldSpec::Text {
                    name: "device_type",
                    optional: false,
                },
                FieldSpec::Text {
                    name: "status",
                    optional: true,
                },
            ],
        }),
        "dcim.interface" => Some(ModelSpec {
            class_name: "Interface",
            fields: vec![
                FieldSpec::Text {
                    name: "name",
                    optional: false,
                },
                FieldSpec::ForeignKey {
                    name: "device",
                    target: "Device",
                    optional: false,
                },
                FieldSpec::Text {
                    name: "if_type",
                    optional: true,
                },
                FieldSpec::Bool {
                    name: "enabled",
                    optional: true,
                },
                FieldSpec::Text {
                    name: "description",
                    optional: true,
                },
            ],
        }),
        "ipam.prefix" => Some(ModelSpec {
            class_name: "Prefix",
            fields: vec![
                FieldSpec::Text {
                    name: "prefix",
                    optional: false,
                },
                FieldSpec::ForeignKey {
                    name: "site",
                    target: "Site",
                    optional: true,
                },
                FieldSpec::Text {
                    name: "description",
                    optional: true,
                },
            ],
        }),
        "ipam.ip_address" => Some(ModelSpec {
            class_name: "IpAddress",
            fields: vec![
                FieldSpec::Text {
                    name: "address",
                    optional: false,
                },
                FieldSpec::ForeignKey {
                    name: "assigned_interface",
                    target: "Interface",
                    optional: true,
                },
                FieldSpec::Text {
                    name: "description",
                    optional: true,
                },
            ],
        }),
        _ => None,
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
    let model_names: Vec<&str> = models.iter().map(|m| m.class_name).collect();
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
    let admins_block = if emit_admin {
        render_admins_block(models)
    } else {
        String::new()
    };
    let serializers_block = render_serializers_block(models);
    let views_block = render_views_block(models);
    let routes_block = render_routes_block(models);

    let models = render_template(MODELS_TEMPLATE, &[("models", models_block)]);
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
    match field {
        FieldSpec::Text { name, optional } => {
            format!("{} = models.TextField({})", name, nullable(*optional))
        }
        FieldSpec::Bool { name, optional } => {
            format!("{} = models.BooleanField({})", name, nullable(*optional))
        }
        FieldSpec::ForeignKey {
            name,
            target,
            optional,
        } => {
            let mut args = vec![
                format!("\"{}\"", target),
                "on_delete=models.PROTECT".to_string(),
            ];
            if *optional {
                args.push("null=True".to_string());
                args.push("blank=True".to_string());
            }
            format!("{} = models.ForeignKey({})", name, args.join(", "))
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

    format!(
        "class {}(models.Model):\n    {}\n",
        model.class_name,
        fields.join("\n    ")
    )
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
            let name = model.class_name;
            let endpoint = format!("{}s", name.to_lowercase());
            format!("router.register(\"{endpoint}\", {name}ViewSet)")
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn field_name(field: &FieldSpec) -> &'static str {
    match field {
        FieldSpec::Text { name, .. } => name,
        FieldSpec::Bool { name, .. } => name,
        FieldSpec::ForeignKey { name, .. } => name,
    }
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

fn admin_list_display(model: &ModelSpec) -> Vec<&'static str> {
    let mut fields = vec!["key", "uid"];
    fields.extend(model.fields.iter().map(field_name));
    fields
}

fn admin_search_fields_for_model(model: &ModelSpec) -> Vec<&'static str> {
    let mut fields = vec!["key"];
    for field in &model.fields {
        if matches!(field, FieldSpec::Text { .. }) {
            fields.push(field_name(field));
        }
    }
    fields
}

fn admin_list_filter(model: &ModelSpec) -> Vec<&'static str> {
    let mut fields = Vec::new();
    for field in &model.fields {
        match field {
            FieldSpec::Bool { name, .. } => fields.push(*name),
            FieldSpec::Text { name, .. } if *name == "status" => fields.push(*name),
            _ => {}
        }
    }
    fields
}

fn serializer_fields(model: &ModelSpec) -> Vec<&'static str> {
    let mut fields = vec!["uid", "key", "attrs"];
    fields.extend(model.fields.iter().map(field_name));
    fields
}

fn nullable(optional: bool) -> String {
    if optional {
        "null=True, blank=True".to_string()
    } else {
        String::new()
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

    fn schema_for(objects: &[Object]) -> Schema {
        let mut types: BTreeMap<String, TypeSchema> = BTreeMap::new();
        for object in objects {
            let entry = types
                .entry(object.type_name.as_str().to_string())
                .or_insert_with(|| TypeSchema {
                    key: BTreeMap::new(),
                    fields: BTreeMap::new(),
                });
            for field in object.key.keys() {
                entry.key.entry(field.clone()).or_insert(FieldSchema {
                    r#type: FieldType::Json,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                });
            }
            for field in object.attrs.keys() {
                entry.fields.entry(field.clone()).or_insert(FieldSchema {
                    r#type: FieldType::Json,
                    required: false,
                    nullable: true,
                    description: None,
                    format: None,
                    pattern: None,
                });
            }
        }
        Schema { types }
    }

    fn sample_inventory() -> Inventory {
        let objects = vec![
            obj(
                1,
                "dcim.device",
                "device=leaf01",
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
                "site=fra1",
                attrs_map(vec![("name", json!("FRA1")), ("slug", json!("fra1"))]),
            ),
            obj(
                3,
                "dcim.interface",
                "interface=eth0",
                attrs_map(vec![
                    ("name", json!("eth0")),
                    ("device", json!(Uuid::from_u128(1).to_string())),
                ]),
            ),
        ];
        Inventory {
            schema: schema_for(&objects),
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
        assert!(models.contains("class Site"));
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

        assert!(admin.contains("class DeviceAdmin"));
        assert!(admin.contains("list_display = [\"key\", \"uid\", \"name\", \"site\", \"role\", \"device_type\", \"status\"]"));
        assert!(admin.contains("search_fields = [\"key\", \"uid\"]"));
        assert!(admin.contains("list_filter = [\"status\"]"));
        assert!(admin.contains("class InterfaceAdmin"));
        assert!(admin.contains("list_filter = [\"enabled\"]"));
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

        assert!(serializers.contains("class DeviceSerializer"));
        assert!(views.contains("class DeviceViewSet"));
        assert!(urls.contains("router.register(\"devices\""));
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
        let device_pos = models.find("class Device").unwrap();
        let site_pos = models.find("class Site").unwrap();
        assert!(device_pos < site_pos);
    }
}
