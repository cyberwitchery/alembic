//! django app generation from alembic ir.

use alembic_core::{Inventory, Kind};
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

    let kinds = inventory_kinds(inventory);
    let models: Vec<ModelSpec> = kinds.iter().filter_map(model_spec).collect();

    let models_content = render_models(&models);
    fs::write(app_dir.join(GENERATED_MODELS), models_content)?;

    if options.emit_admin {
        let admin_content = render_admin(&models);
        fs::write(app_dir.join(GENERATED_ADMIN), admin_content)?;
    }

    let serializers_content = render_serializers(&models);
    fs::write(app_dir.join(GENERATED_SERIALIZERS), serializers_content)?;

    let views_content = render_views(&models);
    fs::write(app_dir.join(GENERATED_VIEWS), views_content)?;

    let urls_content = render_urls(&models, app_name);
    fs::write(app_dir.join(GENERATED_URLS), urls_content)?;

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

fn inventory_kinds(inventory: &Inventory) -> Vec<Kind> {
    let mut objects = inventory.objects.clone();
    objects.sort_by(|a, b| {
        (a.kind.as_string(), a.key.clone()).cmp(&(b.kind.as_string(), b.key.clone()))
    });

    let mut seen = BTreeSet::new();
    let mut kinds = Vec::new();
    for object in objects {
        if seen.insert(object.kind.as_string()) {
            kinds.push(object.kind);
        }
    }
    kinds
}

fn model_spec(kind: &Kind) -> Option<ModelSpec> {
    match kind {
        Kind::DcimSite => Some(ModelSpec {
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
        Kind::DcimDevice => Some(ModelSpec {
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
        Kind::DcimInterface => Some(ModelSpec {
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
        Kind::IpamPrefix => Some(ModelSpec {
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
        Kind::IpamIpAddress => Some(ModelSpec {
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
        Kind::Custom(_) => None,
    }
}

fn render_models(models: &[ModelSpec]) -> String {
    let mut rendered = String::new();
    for model in models {
        rendered.push_str(&format!("class {}(models.Model):\n", model.class_name));
        rendered.push_str("    uid = models.UUIDField(primary_key=True, editable=False)\n");
        rendered.push_str("    key = models.TextField()\n");
        rendered.push_str("    x = models.JSONField(default=dict, blank=True)\n");
        for field in &model.fields {
            rendered.push_str(&format!("    {}\n", render_field(field)));
        }
        rendered.push('\n');
    }
    render_template(MODELS_TEMPLATE, &[("models", rendered)])
}

fn render_admin(models: &[ModelSpec]) -> String {
    let names: Vec<&str> = models.iter().map(|m| m.class_name).collect();
    let model_import = if names.is_empty() {
        String::new()
    } else {
        format!("from .generated_models import {}", names.join(", "))
    };
    let mut admins = String::new();
    for model in models {
        let list_display = admin_list_display(model);
        let search_fields = admin_search_fields();
        let list_filter = admin_list_filter(model);

        admins.push_str(&format!(
            "@admin.register({})\nclass {}Admin(admin.ModelAdmin):\n",
            model.class_name, model.class_name
        ));
        admins.push_str(&format!(
            "    list_display = [{}]\n",
            list_display
                .iter()
                .map(|field| format!("\"{field}\""))
                .collect::<Vec<String>>()
                .join(", ")
        ));
        admins.push_str(&format!(
            "    search_fields = [{}]\n",
            search_fields
                .iter()
                .map(|field| format!("\"{field}\""))
                .collect::<Vec<String>>()
                .join(", ")
        ));
        if !list_filter.is_empty() {
            admins.push_str(&format!(
                "    list_filter = [{}]\n",
                list_filter
                    .iter()
                    .map(|field| format!("\"{field}\""))
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }
        admins.push('\n');
    }
    render_template(
        ADMIN_TEMPLATE,
        &[("model_import", model_import), ("admins", admins)],
    )
}

fn render_serializers(models: &[ModelSpec]) -> String {
    let names: Vec<&str> = models.iter().map(|m| m.class_name).collect();
    let model_import = if names.is_empty() {
        String::new()
    } else {
        format!("from .generated_models import {}", names.join(", "))
    };
    let mut serializers = String::new();
    for model in models {
        let fields = serializer_fields(model);
        serializers.push_str(&format!(
            "class {}Serializer(serializers.ModelSerializer):\n",
            model.class_name
        ));
        serializers.push_str("    class Meta:\n");
        serializers.push_str(&format!("        model = {}\n", model.class_name));
        serializers.push_str(&format!(
            "        fields = [{}]\n\n",
            fields
                .iter()
                .map(|field| format!("\"{field}\""))
                .collect::<Vec<String>>()
                .join(", ")
        ));
    }
    render_template(
        SERIALIZERS_TEMPLATE,
        &[("model_import", model_import), ("serializers", serializers)],
    )
}

fn render_views(models: &[ModelSpec]) -> String {
    let names: Vec<&str> = models.iter().map(|m| m.class_name).collect();
    let model_import = if names.is_empty() {
        String::new()
    } else {
        format!("from .generated_models import {}", names.join(", "))
    };
    let serializer_import = if names.is_empty() {
        String::new()
    } else {
        format!(
            "from .generated_serializers import {}",
            names
                .iter()
                .map(|name| format!("{name}Serializer"))
                .collect::<Vec<String>>()
                .join(", ")
        )
    };
    let mut views = String::new();
    for model in models {
        let filterset_fields = admin_list_display(model);
        let search_fields = admin_search_fields_for_model(model);
        let ordering_fields = admin_list_display(model);
        views.push_str(&format!(
            "class {}ViewSet(viewsets.ModelViewSet):\n",
            model.class_name
        ));
        views.push_str(&format!(
            "    queryset = {}.objects.all()\n",
            model.class_name
        ));
        views.push_str(&format!(
            "    serializer_class = {}Serializer\n",
            model.class_name
        ));
        views.push_str(&format!(
            "    filterset_fields = [{}]\n",
            filterset_fields
                .iter()
                .map(|field| format!("\"{field}\""))
                .collect::<Vec<String>>()
                .join(", ")
        ));
        views.push_str(&format!(
            "    search_fields = [{}]\n",
            search_fields
                .iter()
                .map(|field| format!("\"{field}\""))
                .collect::<Vec<String>>()
                .join(", ")
        ));
        views.push_str(&format!(
            "    ordering_fields = [{}]\n\n",
            ordering_fields
                .iter()
                .map(|field| format!("\"{field}\""))
                .collect::<Vec<String>>()
                .join(", ")
        ));
    }
    render_template(
        VIEWS_TEMPLATE,
        &[
            ("model_import", model_import),
            ("serializer_import", serializer_import),
            ("views", views),
        ],
    )
}

fn render_urls(models: &[ModelSpec], app_name: &str) -> String {
    let view_import = if models.is_empty() {
        String::new()
    } else {
        format!(
            "from .generated_views import {}",
            models
                .iter()
                .map(|model| format!("{}ViewSet", model.class_name))
                .collect::<Vec<String>>()
                .join(", ")
        )
    };
    let mut routes = String::new();
    for model in models {
        let name = model.class_name;
        let endpoint = format!("{}s", name.to_lowercase());
        routes.push_str(&format!("router.register(\"{endpoint}\", {name}ViewSet)\n"));
    }
    render_template(
        URLS_TEMPLATE,
        &[
            ("view_import", view_import),
            ("routes", routes),
            ("app_name", app_name.to_string()),
        ],
    )
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

fn field_name(field: &FieldSpec) -> &'static str {
    match field {
        FieldSpec::Text { name, .. } => name,
        FieldSpec::Bool { name, .. } => name,
        FieldSpec::ForeignKey { name, .. } => name,
    }
}

fn admin_list_display(model: &ModelSpec) -> Vec<&'static str> {
    let mut fields = vec!["key", "uid"];
    fields.extend(model.fields.iter().map(field_name));
    fields
}

fn admin_search_fields() -> Vec<&'static str> {
    vec!["key", "uid"]
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
    let mut fields = vec!["uid", "key", "x"];
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
    use alembic_core::{Attrs, DeviceAttrs, InterfaceAttrs, Inventory, Object, SiteAttrs};
    use tempfile::tempdir;
    use uuid::Uuid;

    fn sample_inventory() -> Inventory {
        Inventory {
            objects: vec![
                Object::new(
                    Uuid::from_u128(1),
                    "device=leaf01".to_string(),
                    Attrs::Device(DeviceAttrs {
                        name: "leaf01".to_string(),
                        site: Uuid::from_u128(2),
                        role: "leaf".to_string(),
                        device_type: "leaf-switch".to_string(),
                        status: None,
                    }),
                ),
                Object::new(
                    Uuid::from_u128(2),
                    "site=fra1".to_string(),
                    Attrs::Site(SiteAttrs {
                        name: "FRA1".to_string(),
                        slug: "fra1".to_string(),
                        status: None,
                        description: None,
                    }),
                ),
                Object::new(
                    Uuid::from_u128(3),
                    "interface=eth0".to_string(),
                    Attrs::Interface(InterfaceAttrs {
                        name: "eth0".to_string(),
                        device: Uuid::from_u128(1),
                        if_type: None,
                        enabled: None,
                        description: None,
                    }),
                ),
            ],
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
        assert!(models.contains("x = models.JSONField"));
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
