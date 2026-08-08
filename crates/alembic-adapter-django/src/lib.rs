//! django app generation from alembic ir.

use crate::emit::{CommandRunner, DjangoConfig};
use alembic_core::{key_string, FieldFormat, FieldType, Inventory, Object, Schema, TypeSchema};
use alembic_engine::{pluralize, AppliedOp, ApplyReport, Emitter, Op, StateStore};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub mod emit;

pub struct DjangoAdapter {
    config: DjangoConfig,
}

impl DjangoAdapter {
    pub fn new(config: DjangoConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Emitter for DjangoAdapter {
    async fn write(&self, schema: &Schema, ops: &[Op], _state: &StateStore) -> Result<ApplyReport> {
        let mut inventory = Inventory {
            schema: schema.clone(),
            objects: Vec::new(),
        };
        let mut apply_report = ApplyReport::default();

        for op in ops {
            match op {
                Op::Create {
                    uid,
                    type_name,
                    desired,
                } => {
                    inventory.objects.push(Object {
                        uid: *uid,
                        type_name: type_name.clone(),
                        key: desired.key.clone(),
                        attrs: desired.attrs.clone(),
                        source: desired.source.clone(),
                    });
                    apply_report.applied.push(AppliedOp {
                        uid: *uid,
                        type_name: type_name.clone(),
                        backend_id: None,
                    })
                }
                Op::Update { .. } => {
                    return Err(anyhow!("unsupported operation, cannot update object"))
                }
                Op::Delete { .. } => {
                    return Err(anyhow!("unsupported operation, cannot delete object"))
                }
            }
        }

        let runner = CommandRunner::new();
        emit::run_emit(&runner, &inventory, &self.config)?;
        Ok(apply_report)
    }
}

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
const FIXTURES_DIR: &str = "fixtures";
/// the objects land in one fixture, loaded by label (`manage.py loaddata alembic`).
pub const FIXTURE_LABEL: &str = "alembic";
const FIXTURE_FILE: &str = "alembic.json";

const MODELS_TEMPLATE: &str = include_str!("../templates/models.py.tpl");
const ADMIN_TEMPLATE: &str = include_str!("../templates/admin.py.tpl");
const SERIALIZERS_TEMPLATE: &str = include_str!("../templates/serializers.py.tpl");
const VIEWS_TEMPLATE: &str = include_str!("../templates/views.py.tpl");
const URLS_TEMPLATE: &str = include_str!("../templates/urls.py.tpl");

/// two blank lines between top-level definitions, as pep8 wants them.
const BLOCK_SEPARATOR: &str = "\n\n\n";

/// a list is a plain `JSONField`, so a declared element type has no native slot:
/// this carries it as a member check instead. the leading underscore keeps the
/// name clear of the model classes, which never start with one.
const MEMBER_VALIDATOR_CLASS: &str = r#"@deconstructible
class _ListMembers:
    """checks every member of a list against its declared element type."""

    def __init__(self, choices=None, regex=None):
        self.choices = choices
        self.regex = regex

    def __call__(self, value):
        if not isinstance(value, (list, tuple)):
            raise ValidationError(
                "expected a list, got %(got)s", params={"got": type(value).__name__}
            )
        for member in value:
            if self.choices is not None and member not in self.choices:
                raise ValidationError(
                    "%(member)s is not one of %(choices)s",
                    params={"member": member, "choices": ", ".join(self.choices)},
                )
            if self.regex is not None and not (
                isinstance(member, str) and re.search(self.regex, member)
            ):
                raise ValidationError(
                    "%(member)s does not match %(regex)s",
                    params={"member": member, "regex": self.regex},
                )"#;

#[derive(Debug)]
struct ModelSpec {
    type_name: String,
    class_name: String,
    fields: Vec<FieldSpec>,
    key_fields: Vec<String>,
}

impl ModelSpec {
    fn relation_fields(&self) -> impl Iterator<Item = &FieldSpec> {
        self.fields
            .iter()
            .filter(|field| matches!(field.field_type, DjangoFieldType::ForeignKey { .. }))
    }

    fn many_to_many_fields(&self) -> impl Iterator<Item = &FieldSpec> {
        self.fields
            .iter()
            .filter(|field| matches!(field.field_type, DjangoFieldType::ManyToMany { .. }))
    }
}

#[derive(Debug, Clone)]
struct FieldSpec {
    name: String,
    field_type: DjangoFieldType,
    required: bool,
    nullable: bool,
    choices: Option<Vec<String>>,
    validators: Vec<String>,
    /// a list's declared element check, rendered alongside `validators`.
    member_validator: Option<String>,
    help_text: Option<String>,
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
    /// `default=list` for a list-shaped field, `default=dict` otherwise.
    Json {
        list: bool,
    },
    Slug,
    IpAddress,
    ForeignKey {
        target: String,
    },
    ManyToMany {
        target: String,
    },
}

impl DjangoFieldType {
    /// text columns hold "" for absent, so they stay NOT NULL; everything else
    /// needs `null=True` or an optional value has nowhere to go.
    fn is_textual(&self) -> bool {
        matches!(
            self,
            DjangoFieldType::Char | DjangoFieldType::Text | DjangoFieldType::Slug
        )
    }

    fn is_json(&self) -> bool {
        matches!(self, DjangoFieldType::Json { .. })
    }

    fn is_many_to_many(&self) -> bool {
        matches!(self, DjangoFieldType::ManyToMany { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DjangoEmitOptions {
    pub emit_admin: bool,
    /// whether `django_filters` is importable in the target interpreter. without
    /// it there is no filter backend, so per-field filtering is not advertised.
    pub filter_backend: bool,
    /// whether `drf_spectacular` is importable in the target interpreter. it
    /// serves the openapi schema and the docs page; without it neither is routed.
    pub schema_view: bool,
}

impl Default for DjangoEmitOptions {
    fn default() -> Self {
        Self {
            emit_admin: true,
            filter_backend: false,
            schema_view: false,
        }
    }
}

// python hard keywords; soft keywords (match, case, type, _) are legal
// identifiers and stay allowed.
const PYTHON_KEYWORDS: &[&str] = &[
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
    "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
    "with", "yield",
];

// field names the emitter assigns on every generated model; a schema field of
// the same name would silently override them (the `uid` case drops the uuid
// primary key without any error from `manage.py check`).
const RESERVED_FIELD_NAMES: &[&str] = &["uid", "key", "attrs"];

// names django or python already give a meaning to on a model instance; a field
// of the same name shadows the manager, the pk alias, or a model method.
const DJANGO_RESERVED_FIELD_NAMES: &[&str] =
    &["pk", "id", "objects", "save", "delete", "clean", "_state"];

fn is_python_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return false;
    }
    !PYTHON_KEYWORDS.contains(&name)
}

// field and type names are interpolated into python source verbatim, so a name
// that is not a usable python identifier must fail the emit instead of
// producing a broken or silently wrong app. all violations are collected into
// one error.
fn validate_schema_names(schema: &Schema) -> Result<()> {
    let mut problems = Vec::new();
    for (type_name, type_schema) in &schema.types {
        let class_name = class_name_for_type(type_name);
        if !is_python_identifier(&class_name) {
            problems.push(format!(
                "type '{type_name}' renders to class name '{class_name}', which is not a valid python identifier"
            ));
        }
        let field_names = type_schema.key.keys().chain(
            type_schema
                .fields
                .keys()
                .filter(|field| !type_schema.key.contains_key(*field)),
        );
        for field in field_names {
            if RESERVED_FIELD_NAMES.contains(&field.as_str()) {
                problems.push(format!(
                    "field '{field}' of type '{type_name}' is reserved: it would shadow the generated model attribute of the same name"
                ));
            } else if DJANGO_RESERVED_FIELD_NAMES.contains(&field.as_str()) {
                problems.push(format!(
                    "field '{field}' of type '{type_name}' is reserved: django already gives that name a meaning on a model"
                ));
            } else if !is_python_identifier(field) {
                problems.push(format!(
                    "field '{field}' of type '{type_name}' is not a valid python identifier"
                ));
            } else if field.ends_with('_') {
                // fields.E001/E002: django rejects both outright.
                problems.push(format!(
                    "field '{field}' of type '{type_name}' ends with an underscore, which django rejects"
                ));
            } else if field.contains("__") {
                problems.push(format!(
                    "field '{field}' of type '{type_name}' contains a double underscore, which django rejects"
                ));
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "cannot emit django app, invalid names in schema:\n  {}",
            problems.join("\n  ")
        ))
    }
}

pub fn emit_django_app(
    app_dir: &Path,
    inventory: &Inventory,
    options: DjangoEmitOptions,
) -> Result<()> {
    validate_schema_names(&inventory.schema)?;
    fs::create_dir_all(app_dir)?;
    let app_name = app_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("alembic_app");

    let models = build_models(&inventory.schema)?;

    let rendered = render_files(&models, &options);
    fs::write(app_dir.join(GENERATED_MODELS), rendered.models)?;
    if let Some(admin) = rendered.admin {
        fs::write(app_dir.join(GENERATED_ADMIN), admin)?;
    }
    fs::write(app_dir.join(GENERATED_SERIALIZERS), rendered.serializers)?;
    fs::write(app_dir.join(GENERATED_VIEWS), rendered.views)?;
    fs::write(app_dir.join(GENERATED_URLS), rendered.urls)?;

    let fixtures_dir = app_dir.join(FIXTURES_DIR);
    fs::create_dir_all(&fixtures_dir)?;
    let entries = fixture_entries(app_name, &models, &inventory.objects)?;
    fs::write(
        fixtures_dir.join(FIXTURE_FILE),
        format!("{}\n", serde_json::to_string_pretty(&entries)?),
    )?;

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

/// turn the schema into model specs, rejecting anything that cannot become a
/// valid django app: colliding model names, dangling relation targets, field
/// names python or the ir envelope already own.
fn build_models(schema: &Schema) -> Result<Vec<ModelSpec>> {
    let mut by_class: BTreeMap<String, String> = BTreeMap::new();
    for type_name in schema.types.keys() {
        let class_name = class_name_for_type(type_name);
        if class_name.is_empty() {
            bail!("type '{type_name}' does not map to a usable django model name");
        }
        if let Some(previous) = by_class.insert(class_name.clone(), type_name.clone()) {
            bail!(
                "types '{previous}' and '{type_name}' both map to the django model \
                 '{class_name}'; rename one of them"
            );
        }
    }

    for (type_name, type_schema) in schema.types.iter() {
        for (field, field_schema) in type_schema.key.iter().chain(type_schema.fields.iter()) {
            let target = match &field_schema.r#type {
                FieldType::Ref { target } | FieldType::ListRef { target } => target,
                _ => continue,
            };
            if !schema.types.contains_key(target) {
                bail!(
                    "{type_name}.{field} references unknown type '{target}'; \
                     the django backend can only relate to types in the same model"
                );
            }
        }
    }

    let mut models = Vec::with_capacity(schema.types.len());
    let mut endpoints: BTreeMap<String, String> = BTreeMap::new();
    for (type_name, type_schema) in schema.types.iter() {
        let model = model_spec_from_schema(type_name, type_schema);
        if let Some(previous) = endpoints.insert(endpoint_for(&model), model.type_name.clone()) {
            bail!(
                "types '{previous}' and '{}' both map to the api route '{}'; rename one of them",
                model.type_name,
                endpoint_for(&model)
            );
        }
        models.push(model);
    }
    Ok(models)
}

fn model_spec_from_schema(type_name: &str, schema: &TypeSchema) -> ModelSpec {
    let class_name = class_name_for_type(type_name);
    let mut fields = Vec::new();
    let mut key_fields = Vec::new();

    for (field, field_schema) in schema.key.iter() {
        key_fields.push(field.to_string());
        fields.push(field_spec_from_schema(field, field_schema, true));
    }

    for (field, field_schema) in schema.fields.iter() {
        if schema.key.contains_key(field) {
            continue;
        }
        fields.push(field_spec_from_schema(field, field_schema, false));
    }

    ModelSpec {
        type_name: type_name.to_string(),
        class_name,
        fields,
        key_fields,
    }
}

struct DjangoFiles {
    models: String,
    admin: Option<String>,
    serializers: String,
    views: String,
    urls: String,
}

fn render_files(models: &[ModelSpec], options: &DjangoEmitOptions) -> DjangoFiles {
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

    let has_field = |predicate: fn(&FieldSpec) -> bool| {
        models
            .iter()
            .any(|model| model.fields.iter().any(&predicate))
    };
    let member_validators = has_field(|field| field.member_validator.is_some());

    let mut model_imports = Vec::new();
    if member_validators {
        model_imports.push("import re".to_string());
    }
    model_imports.push("import uuid".to_string());
    model_imports.push(String::new());
    model_imports.push("from django.db import models".to_string());
    if has_field(|field| !field.validators.is_empty()) {
        model_imports.push("from django.core.validators import RegexValidator".to_string());
    }
    if member_validators {
        model_imports.push("from django.core.exceptions import ValidationError".to_string());
        model_imports.push("from django.utils.deconstruct import deconstructible".to_string());
    }

    let mut model_blocks = render_blocks(models, render_model_block);
    if member_validators {
        model_blocks = format!("{MEMBER_VALIDATOR_CLASS}{BLOCK_SEPARATOR}{model_blocks}");
    }
    let models_file = render_template(
        MODELS_TEMPLATE,
        &[
            ("imports", model_imports.join("\n")),
            ("models", model_blocks),
        ],
    );
    let admin = if options.emit_admin {
        Some(render_template(
            ADMIN_TEMPLATE,
            &[
                ("model_import", model_import.clone()),
                ("admins", render_blocks(models, render_admin_block)),
            ],
        ))
    } else {
        None
    };
    let serializers = render_template(
        SERIALIZERS_TEMPLATE,
        &[
            ("model_import", model_import.clone()),
            (
                "serializers",
                render_blocks(models, render_serializer_block),
            ),
        ],
    );
    let views = render_template(
        VIEWS_TEMPLATE,
        &[
            ("model_import", model_import),
            ("serializer_import", serializer_import),
            (
                "views",
                render_blocks(models, |model| render_view_block(model, options)),
            ),
        ],
    );
    // drf's own schema view is the legacy coreapi surface: it needs three
    // optional packages and django-filter no longer implements the hook it
    // calls, so it is served by drf-spectacular or not at all.
    let (schema_import, schema_routes) = if options.schema_view {
        (
            "from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView\n"
                .to_string(),
            concat!(
                "    path(\"schema/\", SpectacularAPIView.as_view(), name=\"schema\"),\n",
                "    path(\"docs/\", SpectacularSwaggerView.as_view(url_name=\"schema\"), name=\"docs\"),\n",
            )
            .to_string(),
        )
    } else {
        (String::new(), String::new())
    };
    let urls = render_template(
        URLS_TEMPLATE,
        &[
            ("schema_import", schema_import),
            ("view_import", view_import),
            ("routes", render_routes_block(models)),
            ("schema_routes", schema_routes),
        ],
    );

    DjangoFiles {
        models: models_file,
        admin,
        serializers,
        views,
        urls,
    }
}

fn render_blocks(models: &[ModelSpec], render: impl Fn(&ModelSpec) -> String) -> String {
    models
        .iter()
        .map(render)
        .collect::<Vec<String>>()
        .join(BLOCK_SEPARATOR)
}

fn render_field(model: &ModelSpec, field: &FieldSpec) -> String {
    let mut args = Vec::new();
    if let Some(choices) = &field.choices {
        let choice_items = choices
            .iter()
            .map(|value| format!("({}, {})", py_str(value), py_str(value)))
            .collect::<Vec<_>>()
            .join(", ");
        args.push(format!("choices=[{choice_items}]"));
    }
    let validators: Vec<&str> = field
        .validators
        .iter()
        .chain(field.member_validator.iter())
        .map(String::as_str)
        .collect();
    if !validators.is_empty() {
        args.push(format!("validators=[{}]", validators.join(", ")));
    }
    if let Some(help_text) = &field.help_text {
        args.push(format!("help_text={}", py_str(help_text)));
    }

    let optional = !field.required;
    if optional {
        args.push("blank=True".to_string());
    }
    // `blank=True` is a form-level flag only: without `null=True` the column is
    // still NOT NULL and an absent value cannot be saved at all.
    let nullable = field.nullable || (optional && !field.field_type.is_textual());
    if nullable && !field.field_type.is_json() && !field.field_type.is_many_to_many() {
        args.push("null=True".to_string());
    }

    let mut leading = Vec::new();
    let mut trailing = Vec::new();
    let django_type = match &field.field_type {
        DjangoFieldType::Char => {
            leading.push("max_length=255".to_string());
            "CharField"
        }
        DjangoFieldType::Text => "TextField",
        DjangoFieldType::Integer => "IntegerField",
        DjangoFieldType::Float => "FloatField",
        DjangoFieldType::Boolean => "BooleanField",
        DjangoFieldType::Uuid => "UUIDField",
        DjangoFieldType::Date => "DateField",
        DjangoFieldType::DateTime => "DateTimeField",
        DjangoFieldType::Time => "TimeField",
        DjangoFieldType::Json { list } => {
            if optional {
                trailing.push(format!("default={}", if *list { "list" } else { "dict" }));
            }
            "JSONField"
        }
        DjangoFieldType::Slug => "SlugField",
        DjangoFieldType::IpAddress => "GenericIPAddressField",
        DjangoFieldType::ForeignKey { target } => {
            leading.push(py_str(target));
            leading.push("on_delete=models.PROTECT".to_string());
            leading.push(related_name_arg(model, field));
            "ForeignKey"
        }
        DjangoFieldType::ManyToMany { target } => {
            leading.push(py_str(target));
            leading.push(related_name_arg(model, field));
            "ManyToManyField"
        }
    };

    leading.extend(args);
    leading.extend(trailing);
    format!(
        "{} = models.{}({})",
        field.name,
        django_type,
        leading.join(", ")
    )
}

/// a reverse accessor per relation: unique by construction (model names are
/// unique, field names are unique within a model), so two relations to the same
/// target cannot clash (fields.E304) while navigation stays available.
fn related_name_arg(model: &ModelSpec, field: &FieldSpec) -> String {
    format!(
        "related_name={}",
        py_str(&format!(
            "{}_{}",
            model.class_name.to_lowercase(),
            field.name
        ))
    )
}

fn render_model_block(model: &ModelSpec) -> String {
    let mut fields = Vec::with_capacity(model.fields.len() + 3);
    fields.push(
        "uid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)".to_string(),
    );
    fields.push("key = models.TextField()".to_string());
    fields.push("attrs = models.JSONField(default=dict, blank=True)".to_string());
    for field in &model.fields {
        fields.push(render_field(model, field));
    }

    let mut lines = vec![
        format!("class {}(models.Model):", model.class_name),
        format!("    {}", fields.join("\n    ")),
        String::new(),
        "    class Meta:".to_string(),
        // pagination over an unordered queryset is not stable; the key is unique
        // per type, so it is the natural order.
        "        ordering = [\"key\"]".to_string(),
        format!("        verbose_name = {}", py_str(&model.type_name)),
        format!(
            "        verbose_name_plural = {}",
            py_str(&pluralize(&model.type_name))
        ),
    ];

    if !model.key_fields.is_empty() {
        let unique_fields = model
            .key_fields
            .iter()
            .map(|field| py_str(field))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "        constraints = [models.UniqueConstraint(fields=[{unique_fields}], name=\"{}_key\")]",
            model.class_name.to_lowercase()
        ));
    }

    lines.push(String::new());
    lines.push("    def __str__(self):".to_string());
    // the key column holds the ir key as json, which reads as
    // `{"name":"leaf01"}` wherever django prints an object. the key *values*
    // are what a person recognises, and a ref key field renders through the
    // target's own __str__.
    if model.key_fields.is_empty() {
        lines.push("        return self.key".to_string());
    } else {
        let parts = model
            .key_fields
            .iter()
            .map(|field| format!("{{self.{field}}}"))
            .collect::<Vec<_>>()
            .join(" / ");
        lines.push(format!("        return f\"{parts}\""));
    }

    lines.join("\n")
}

fn render_admin_block(model: &ModelSpec) -> String {
    let list_display = admin_list_display(model);
    let list_filter = admin_list_filter(model);
    let related: Vec<&str> = model
        .relation_fields()
        .map(|field| field.name.as_str())
        .collect();
    let mut lines = vec![
        format!(
            "@admin.register({})\nclass {}Admin(admin.ModelAdmin):",
            model.class_name, model.class_name
        ),
        format!("    list_display = [{}]", join_quoted(&list_display)),
        format!(
            "    search_fields = [{}]",
            join_quoted(&text_search_fields(model))
        ),
    ];
    if !list_filter.is_empty() {
        lines.push(format!("    list_filter = [{}]", join_quoted(&list_filter)));
    }
    if !related.is_empty() {
        // a foreign key in list_display is one query per row without this.
        lines.push(format!(
            "    list_select_related = [{}]",
            join_quoted(&related)
        ));
    }
    lines.join("\n")
}

fn render_serializer_block(model: &ModelSpec) -> String {
    let fields = serializer_fields(model);
    format!(
        "class {}Serializer(serializers.ModelSerializer):\n    class Meta:\n        model = {}\n        fields = [{}]",
        model.class_name,
        model.class_name,
        join_quoted(&fields)
    )
}

fn render_view_block(model: &ModelSpec, options: &DjangoEmitOptions) -> String {
    let prefetch: Vec<&str> = model
        .many_to_many_fields()
        .map(|field| field.name.as_str())
        .collect();
    let queryset = if prefetch.is_empty() {
        format!("{}.objects.all()", model.class_name)
    } else {
        format!(
            "{}.objects.all().prefetch_related({})",
            model.class_name,
            join_quoted(&prefetch)
        )
    };

    let mut lines = vec![
        format!("class {}ViewSet(viewsets.ModelViewSet):", model.class_name),
        format!("    queryset = {queryset}"),
        format!("    serializer_class = {}Serializer", model.class_name),
    ];
    if options.filter_backend {
        lines.push(format!(
            "    filterset_fields = [{}]",
            join_quoted(&filterset_fields(model))
        ));
    }
    lines.push(format!(
        "    search_fields = [{}]",
        join_quoted(&text_search_fields(model))
    ));
    lines.push(format!(
        "    ordering_fields = [{}]",
        join_quoted(&ordering_fields(model))
    ));
    lines.push("    ordering = [\"key\"]".to_string());
    lines.join("\n")
}

fn endpoint_for(model: &ModelSpec) -> String {
    pluralize(model.class_name.to_lowercase().as_str())
}

fn render_routes_block(models: &[ModelSpec]) -> String {
    models
        .iter()
        .map(|model| {
            format!(
                "router.register(\"{}\", {}ViewSet)",
                endpoint_for(model),
                model.class_name
            )
        })
        .collect::<Vec<String>>()
        .join("\n")
}

/// a python string literal for an arbitrary value: everything here ends up in
/// generated source, so a stray quote or backslash must not break the file.
fn py_str(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn join_quoted(fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| py_str(field))
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
    // the object's own __str__ leads: the `key` column is the ir key as json,
    // which is not what anyone scans a changelist for. it stays searchable.
    let mut fields = vec!["__str__"];
    for field in &model.fields {
        // ManyToManyField is invalid in a Django admin list_display (admin.E109),
        // and json/text blobs make the changelist unreadable.
        if field.field_type.is_many_to_many()
            || field.field_type.is_json()
            || matches!(field.field_type, DjangoFieldType::Text)
        {
            continue;
        }
        fields.push(field.name.as_str());
    }
    fields.push("uid");
    fields
}

/// icontains is what both the admin and drf's SearchFilter emit, and postgres
/// has no such operator for uuid, inet, or json columns.
fn text_search_fields(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["key"];
    for field in &model.fields {
        if field.field_type.is_textual() {
            fields.push(field.name.as_str());
        }
    }
    fields
}

fn admin_list_filter(model: &ModelSpec) -> Vec<&str> {
    model
        .fields
        .iter()
        .filter(|field| {
            matches!(field.field_type, DjangoFieldType::Boolean) || field.choices.is_some()
        })
        .map(|field| field.name.as_str())
        .collect()
}

/// django-filter has no filter for json columns and cannot resolve a text blob
/// to a sensible lookup, so those stay out of the filterset.
fn filterset_fields(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["key", "uid"];
    for field in &model.fields {
        if field.field_type.is_json() || matches!(field.field_type, DjangoFieldType::Text) {
            continue;
        }
        fields.push(field.name.as_str());
    }
    fields
}

fn ordering_fields(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["key", "uid"];
    for field in &model.fields {
        if field.field_type.is_json() || field.field_type.is_many_to_many() {
            continue;
        }
        fields.push(field.name.as_str());
    }
    fields
}

fn serializer_fields(model: &ModelSpec) -> Vec<&str> {
    let mut fields = vec!["uid", "key", "attrs"];
    for field in &model.fields {
        fields.push(field.name.as_str());
    }
    fields
}

/// the ir objects as a django fixture: the uid is the primary key, so relations
/// carry over as-is and `loaddata` is idempotent across runs.
fn fixture_entries(app_name: &str, models: &[ModelSpec], objects: &[Object]) -> Result<Vec<Value>> {
    let by_type: BTreeMap<&str, &ModelSpec> = models
        .iter()
        .map(|model| (model.type_name.as_str(), model))
        .collect();

    let mut entries: Vec<(String, String, Value)> = Vec::with_capacity(objects.len());
    for object in objects {
        let type_name = object.type_name.as_str();
        let model = by_type.get(type_name).ok_or_else(|| {
            anyhow!(
                "object {} has type '{type_name}' which is not in the schema",
                object.uid
            )
        })?;

        let mut fields = Map::new();
        for spec in &model.fields {
            let value = object
                .attrs
                .get(&spec.name)
                .or_else(|| object.key.get(&spec.name));
            match value {
                Some(Value::Null) | None => {}
                Some(value) => {
                    fields.insert(spec.name.clone(), value.clone());
                }
            }
        }
        // whatever the schema does not model stays in the envelope blob, so
        // nothing in the inventory is dropped on the way into the app.
        let leftovers: Map<String, Value> = object
            .attrs
            .iter()
            .filter(|(name, _)| !model.fields.iter().any(|spec| &spec.name == *name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        fields.insert("key".to_string(), Value::String(key_string(&object.key)));
        fields.insert("attrs".to_string(), Value::Object(leftovers));

        let model_label = format!("{app_name}.{}", model.class_name.to_lowercase());
        let pk = object.uid.to_string();
        entries.push((
            model_label.clone(),
            pk.clone(),
            json!({"model": model_label, "pk": pk, "fields": Value::Object(fields)}),
        ));
    }

    entries.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    Ok(entries.into_iter().map(|(_, _, entry)| entry).collect())
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

fn field_spec_from_schema(
    name: &str,
    schema: &alembic_core::FieldSchema,
    is_key: bool,
) -> FieldSpec {
    let mut validators = Vec::new();
    let mut choices = None;
    let mut member_validator = None;

    if let Some(format) = &schema.format {
        validators.push(format_validator(format));
    }
    if let Some(pattern) = &schema.pattern {
        validators.push(format!("RegexValidator({})", py_str(pattern)));
    }
    // an author-written constraint beats a type-derived one, as in the engine's
    // `validation_regex_for_schema`. a declared format and pattern both apply
    // though: `validators` is a list, where a custom field carries one regex.
    let declared = schema.format.is_some() || schema.pattern.is_some();

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
        FieldType::Json => DjangoFieldType::Json { list: false },
        FieldType::IpAddress => DjangoFieldType::IpAddress,
        FieldType::Cidr | FieldType::Prefix | FieldType::Mac => {
            if !declared {
                validators.extend(
                    alembic_core::format_for_field_type(&schema.r#type)
                        .as_ref()
                        .map(format_validator),
                );
            }
            DjangoFieldType::Char
        }
        FieldType::Slug => DjangoFieldType::Slug,
        FieldType::Enum { values } => {
            choices = Some(values.clone());
            DjangoFieldType::Char
        }
        FieldType::List { item } => {
            member_validator = member_check(item).as_ref().map(render_member_check);
            DjangoFieldType::Json { list: true }
        }
        FieldType::Map { .. } => DjangoFieldType::Json { list: false },
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
        member_validator,
        help_text: schema.description.clone(),
    }
}

/// the element constraints a list column can carry. an enum's members and a
/// format-typed item's regex are exactly what core checks each entry against;
/// every other element type has no django check that is not an approximation,
/// so it gets none.
enum MemberCheck {
    Choices(Vec<String>),
    Regex(&'static str),
}

fn member_check(item: &FieldType) -> Option<MemberCheck> {
    match item {
        FieldType::Enum { values } => Some(MemberCheck::Choices(values.clone())),
        _ => alembic_core::format_for_field_type(item)
            .map(|format| MemberCheck::Regex(alembic_core::format_regex(&format))),
    }
}

fn render_member_check(check: &MemberCheck) -> String {
    match check {
        MemberCheck::Choices(values) => format!(
            "_ListMembers(choices=[{}])",
            values
                .iter()
                .map(|value| py_str(value))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        MemberCheck::Regex(pattern) => format!("_ListMembers(regex={})", py_str(pattern)),
    }
}

fn format_validator(format: &FieldFormat) -> String {
    format!(
        "RegexValidator({})",
        py_str(alembic_core::format_regex(format))
    )
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
    use alembic_core::{FieldSchema, JsonMap, TypeName};
    use serde_json::json;
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

    fn field(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn optional(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            required: false,
            ..field(r#type)
        }
    }

    fn type_schema(key: Vec<(&str, FieldSchema)>, fields: Vec<(&str, FieldSchema)>) -> TypeSchema {
        TypeSchema {
            key: key
                .into_iter()
                .map(|(name, schema)| (name.to_string(), schema))
                .collect(),
            fields: fields
                .into_iter()
                .map(|(name, schema)| (name.to_string(), schema))
                .collect(),
        }
    }

    fn schema_of(types: Vec<(&str, TypeSchema)>) -> Schema {
        Schema {
            types: types
                .into_iter()
                .map(|(name, schema)| (name.to_string(), schema))
                .collect(),
        }
    }

    fn test_schema() -> Schema {
        schema_of(vec![
            (
                "dcim.site",
                type_schema(
                    vec![("slug", field(FieldType::Slug))],
                    vec![
                        ("name", field(FieldType::String)),
                        ("slug", field(FieldType::Slug)),
                    ],
                ),
            ),
            (
                "dcim.device",
                type_schema(
                    vec![("name", field(FieldType::Slug))],
                    vec![
                        ("name", field(FieldType::String)),
                        (
                            "site",
                            field(FieldType::Ref {
                                target: "dcim.site".to_string(),
                            }),
                        ),
                        ("role", field(FieldType::String)),
                        ("device_type", field(FieldType::String)),
                    ],
                ),
            ),
            (
                "dcim.interface",
                type_schema(
                    vec![("name", field(FieldType::Slug))],
                    vec![
                        ("name", field(FieldType::String)),
                        (
                            "device",
                            field(FieldType::Ref {
                                target: "dcim.device".to_string(),
                            }),
                        ),
                    ],
                ),
            ),
        ])
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
                    ("unmodelled", json!("kept in attrs")),
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

    fn emit_to_temp(inventory: &Inventory) -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        emit_django_app(dir.path(), inventory, DjangoEmitOptions::default()).unwrap();
        dir
    }

    fn generated(dir: &tempfile::TempDir, name: &str) -> String {
        fs::read_to_string(dir.path().join(name)).unwrap()
    }

    #[test]
    fn emit_django_app_writes_files_and_stubs() {
        let dir = emit_to_temp(&sample_inventory());

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
        assert!(dir.path().join(FIXTURES_DIR).join(FIXTURE_FILE).exists());

        let models = generated(&dir, GENERATED_MODELS);
        assert!(models.contains("class DcimSite"));
        assert!(models.contains(
            "site = models.ForeignKey(\"DcimSite\", on_delete=models.PROTECT, related_name=\"dcimdevice_site\")"
        ));
        assert!(models.contains(
            "device = models.ForeignKey(\"DcimDevice\", on_delete=models.PROTECT, related_name=\"dciminterface_device\")"
        ));
        assert!(models.contains("attrs = models.JSONField"));
    }

    #[test]
    fn uid_defaults_so_the_api_can_create_objects() {
        // the pk is not editable, so drf marks it read-only: without a default,
        // every POST to the generated api dies on a NOT NULL uid.
        let models = generated(&emit_to_temp(&sample_inventory()), GENERATED_MODELS);
        assert!(models.contains("import uuid"));
        assert!(models.contains(
            "uid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)"
        ));
    }

    #[test]
    fn optional_non_text_fields_are_nullable() {
        // `blank=True` is a form-level flag: without `null=True` the column stays
        // NOT NULL and an absent value cannot be saved at all.
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.site",
                type_schema(
                    vec![("slug", field(FieldType::Slug))],
                    vec![
                        ("active", optional(FieldType::Bool)),
                        ("created", optional(FieldType::Datetime)),
                        ("mgmt_ip", optional(FieldType::IpAddress)),
                        ("name", optional(FieldType::String)),
                        ("meta", optional(FieldType::Json)),
                        (
                            "tags",
                            optional(FieldType::List {
                                item: Box::new(FieldType::String),
                            }),
                        ),
                    ],
                ),
            )]),
            objects: vec![],
        };
        let models = generated(&emit_to_temp(&inventory), GENERATED_MODELS);

        assert!(models.contains("active = models.BooleanField(blank=True, null=True)"));
        assert!(models.contains("created = models.DateTimeField(blank=True, null=True)"));
        assert!(models.contains("mgmt_ip = models.GenericIPAddressField(blank=True, null=True)"));
        // text columns hold "" for absent, so they stay NOT NULL.
        assert!(models.contains("name = models.CharField(max_length=255, blank=True)"));
        // json columns get a default instead of null.
        assert!(models.contains("meta = models.JSONField(blank=True, default=dict)"));
        assert!(models.contains("tags = models.JSONField(blank=True, default=list)"));
    }

    #[test]
    fn many_to_many_is_never_nullable() {
        // django rejects null=True on a ManyToManyField (fields.W340).
        let inventory = Inventory {
            schema: schema_of(vec![
                (
                    "dcim.tag",
                    type_schema(vec![("name", field(FieldType::Slug))], vec![]),
                ),
                (
                    "dcim.device",
                    type_schema(
                        vec![("name", field(FieldType::Slug))],
                        vec![(
                            "tags",
                            optional(FieldType::ListRef {
                                target: "dcim.tag".to_string(),
                            }),
                        )],
                    ),
                ),
            ]),
            objects: vec![],
        };
        let models = generated(&emit_to_temp(&inventory), GENERATED_MODELS);
        assert!(models.contains(
            "tags = models.ManyToManyField(\"DcimTag\", related_name=\"dcimdevice_tags\", blank=True)"
        ));
    }

    fn list_field_models(item: FieldType) -> String {
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.interface",
                type_schema(
                    vec![("name", field(FieldType::Slug))],
                    vec![(
                        "members",
                        field(FieldType::List {
                            item: Box::new(item),
                        }),
                    )],
                ),
            )]),
            objects: vec![],
        };
        generated(&emit_to_temp(&inventory), GENERATED_MODELS)
    }

    #[test]
    fn list_of_enum_carries_its_declared_members() {
        let models = list_field_models(FieldType::Enum {
            values: vec!["access".to_string(), "trunk".to_string()],
        });
        assert!(
            models.contains(
                "members = models.JSONField(validators=[_ListMembers(choices=[\"access\", \"trunk\"])])"
            ),
            "{models}"
        );
        assert!(models.contains("class _ListMembers:"), "{models}");
        assert!(models.contains("import re"), "{models}");
        assert!(
            models.contains("from django.utils.deconstruct import deconstructible"),
            "{models}"
        );
        // the member check is not a RegexValidator, so nothing imports one.
        assert!(!models.contains("import RegexValidator"), "{models}");
    }

    #[test]
    fn list_of_format_typed_items_carries_the_format_regex() {
        let models = list_field_models(FieldType::Mac);
        assert!(
            models.contains(&format!(
                "members = models.JSONField(validators=[_ListMembers(regex={})])",
                py_str(alembic_core::format_regex(&FieldFormat::Mac))
            )),
            "{models}"
        );
    }

    #[test]
    fn list_of_plain_strings_carries_no_member_check() {
        // a string element has no declared constraint to carry, so the column
        // stays a bare JSONField and the helper is not emitted.
        let models = list_field_models(FieldType::String);
        assert!(models.contains("members = models.JSONField()"), "{models}");
        assert!(!models.contains("_ListMembers"), "{models}");
        assert!(!models.contains("import re"), "{models}");
    }

    #[test]
    fn map_values_carry_no_member_check() {
        // maps stay plain json, as they do on the nautobot backend.
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.interface",
                type_schema(
                    vec![("name", field(FieldType::Slug))],
                    vec![(
                        "labels",
                        field(FieldType::Map {
                            value: Box::new(FieldType::Enum {
                                values: vec!["access".to_string()],
                            }),
                        }),
                    )],
                ),
            )]),
            objects: vec![],
        };
        let models = generated(&emit_to_temp(&inventory), GENERATED_MODELS);
        assert!(models.contains("labels = models.JSONField()"), "{models}");
        assert!(!models.contains("_ListMembers"), "{models}");
    }

    /// what core's own checker makes of `[member]` in a declared `list` field.
    fn core_accepts_member(item: &FieldType, member: &Value) -> bool {
        core_accepts(
            field(FieldType::List {
                item: Box::new(item.clone()),
            }),
            json!([member]),
        )
    }

    fn core_accepts(schema: FieldSchema, value: Value) -> bool {
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.interface",
                type_schema(
                    vec![("name", field(FieldType::Slug))],
                    vec![("members", schema)],
                ),
            )]),
            objects: vec![obj(
                1,
                "dcim.interface",
                "name=eth0",
                attrs_map(vec![("members", value)]),
            )],
        };
        alembic_core::validate_inventory(&inventory)
            .errors
            .is_empty()
    }

    /// what the emitted member check makes of the same value. a regex check runs
    /// through core's own `pattern` machinery, the engine django's `re.search`
    /// stands in for, and rejects a non-string as the generated `_ListMembers`
    /// does.
    fn check_accepts_member(check: &Option<MemberCheck>, member: &Value) -> bool {
        match check {
            None => true,
            Some(MemberCheck::Choices(values)) => member
                .as_str()
                .is_some_and(|raw| values.iter().any(|value| value == raw)),
            Some(MemberCheck::Regex(pattern)) => {
                let mut schema = field(FieldType::String);
                schema.pattern = Some((*pattern).to_string());
                core_accepts(schema, member.clone())
            }
        }
    }

    #[test]
    fn member_check_never_rejects_what_core_accepts() {
        let items = vec![
            FieldType::Enum {
                values: vec!["access".to_string(), "trunk".to_string()],
            },
            FieldType::Mac,
            FieldType::Cidr,
            FieldType::Prefix,
            FieldType::Uuid,
            FieldType::Slug,
            FieldType::IpAddress,
            FieldType::String,
            FieldType::Int,
            FieldType::Bool,
            FieldType::Date,
            FieldType::Json,
        ];
        let members = vec![
            json!("access"),
            json!("trunk"),
            json!("ACCESS"),
            json!("bogus"),
            json!(""),
            json!("aa:bb:cc:dd:ee:ff"),
            json!("AA-BB-CC-DD-EE-FF"),
            json!("aabbccddeeff"),
            json!("10.0.0.0/24"),
            json!("2001:db8::/32"),
            json!("::ffff:192.168.0.1"),
            json!("192.168.0.1"),
            json!("f47ac10b-58cc-4372-a567-0e02b2c3d479"),
            json!("f47ac10b58cc4372a5670e02b2c3d479"),
            json!("{f47ac10b-58cc-4372-a567-0e02b2c3d479}"),
            json!("urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479"),
            json!("fra1"),
            json!("fra-1_a"),
            json!("Fra1"),
            json!("with space"),
            json!("with\nnewline"),
            json!("ünïcödé"),
            json!("2024-01-01"),
            json!("2024-01-01T00:00:00Z"),
            json!(7),
            json!(1.5),
            json!(true),
            json!(null),
            json!(["nested"]),
            json!({"nested": "object"}),
        ];

        for item in &items {
            let check = member_check(item);
            for member in &members {
                if core_accepts_member(item, member) {
                    assert!(
                        check_accepts_member(&check, member),
                        "core accepts {member} as a {item:?} member, the generated check rejects it"
                    );
                }
            }
        }
    }

    #[test]
    fn member_check_rejects_an_undeclared_member() {
        // the other half of the invariant: a check that accepted everything
        // would satisfy `member_check_never_rejects_what_core_accepts` too.
        let enum_check = member_check(&FieldType::Enum {
            values: vec!["access".to_string(), "trunk".to_string()],
        });
        assert!(!check_accepts_member(&enum_check, &json!("bogus")));
        assert!(check_accepts_member(&enum_check, &json!("access")));

        let mac_check = member_check(&FieldType::Mac);
        assert!(!check_accepts_member(&mac_check, &json!("not-a-mac")));
        assert!(check_accepts_member(
            &mac_check,
            &json!("aa:bb:cc:dd:ee:ff")
        ));
    }

    #[test]
    fn string_values_are_escaped_into_python() {
        // a stray quote in an enum value or pattern must not break the
        // generated python.
        let mut status = field(FieldType::Enum {
            values: vec!["active".to_string(), "retired \"old\"".to_string()],
        });
        status.pattern = Some("^\\d+\"$".to_string());
        status.description = Some("a \"quoted\" description".to_string());
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.site",
                type_schema(
                    vec![("slug", field(FieldType::Slug))],
                    vec![("status", status)],
                ),
            )]),
            objects: vec![],
        };
        let models = generated(&emit_to_temp(&inventory), GENERATED_MODELS);

        assert!(
            models.contains(r#"("retired \"old\"", "retired \"old\"")"#),
            "{models}"
        );
        assert!(models.contains(r#"RegexValidator("^\\d+\"$")"#), "{models}");
        assert!(
            models.contains(r#"help_text="a \"quoted\" description""#),
            "{models}"
        );
    }

    fn models_for_field(schema: FieldSchema) -> String {
        let inventory = Inventory {
            schema: schema_of(vec![(
                "ipam.address",
                type_schema(
                    vec![("slug", field(FieldType::Slug))],
                    vec![("value", schema)],
                ),
            )]),
            objects: vec![],
        };
        generated(&emit_to_temp(&inventory), GENERATED_MODELS)
    }

    const CIDR_REGEX: &str = r#"RegexValidator("^[0-9a-fA-F:\\./]+$")"#;

    #[test]
    fn a_format_typed_field_gets_the_derived_validator() {
        let models = models_for_field(field(FieldType::Cidr));

        assert!(
            models.contains(&format!("validators=[{CIDR_REGEX}]")),
            "{models}"
        );
    }

    #[test]
    fn a_declared_format_replaces_the_derived_one() {
        let mut value = field(FieldType::Cidr);
        value.format = Some(FieldFormat::Cidr);
        let models = models_for_field(value);

        assert_eq!(models.matches(CIDR_REGEX).count(), 1, "{models}");
    }

    #[test]
    fn a_declared_pattern_replaces_the_derived_one() {
        let mut value = field(FieldType::Mac);
        value.pattern = Some("^00:".to_string());
        let models = models_for_field(value);

        assert!(
            models.contains(r#"validators=[RegexValidator("^00:")]"#),
            "{models}"
        );
    }

    #[test]
    fn a_declared_format_and_pattern_both_apply() {
        let mut value = field(FieldType::Mac);
        value.format = Some(FieldFormat::Mac);
        value.pattern = Some("^00:".to_string());
        let models = models_for_field(value);

        assert!(
            models.contains(
                r#"validators=[RegexValidator("^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"), RegexValidator("^00:")]"#
            ),
            "{models}"
        );
    }

    #[test]
    fn field_descriptions_become_help_text() {
        let mut name = field(FieldType::String);
        name.description = Some("human readable name".to_string());
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.site",
                type_schema(vec![("slug", field(FieldType::Slug))], vec![("name", name)]),
            )]),
            objects: vec![],
        };
        let models = generated(&emit_to_temp(&inventory), GENERATED_MODELS);
        assert!(
            models.contains("help_text=\"human readable name\""),
            "{models}"
        );
    }

    #[test]
    fn unusable_field_names_are_rejected() {
        for name in ["class", "key", "attrs", "uid", "pk", "trailing_", "do__ble"] {
            let inventory = Inventory {
                schema: schema_of(vec![(
                    "dcim.site",
                    type_schema(
                        vec![("slug", field(FieldType::Slug))],
                        vec![(name, field(FieldType::String))],
                    ),
                )]),
                objects: vec![],
            };
            let dir = tempdir().unwrap();
            let result = emit_django_app(dir.path(), &inventory, DjangoEmitOptions::default());
            assert!(
                result.is_err(),
                "expected '{name}' to be rejected instead of emitting broken python"
            );
        }
    }

    #[test]
    fn colliding_model_names_are_rejected() {
        // both types render as `class DcimSite`, which would silently drop one.
        let inventory = Inventory {
            schema: schema_of(vec![
                (
                    "dcim.site",
                    type_schema(vec![("slug", field(FieldType::Slug))], vec![]),
                ),
                (
                    "dcim_site",
                    type_schema(vec![("slug", field(FieldType::Slug))], vec![]),
                ),
            ]),
            objects: vec![],
        };
        let dir = tempdir().unwrap();
        let err = emit_django_app(dir.path(), &inventory, DjangoEmitOptions::default())
            .expect_err("colliding model names must fail");
        assert!(err.to_string().contains("DcimSite"), "{err}");
    }

    #[test]
    fn dangling_relation_targets_are_rejected() {
        let inventory = Inventory {
            schema: schema_of(vec![(
                "dcim.device",
                type_schema(
                    vec![("name", field(FieldType::Slug))],
                    vec![(
                        "site",
                        field(FieldType::Ref {
                            target: "dcim.site".to_string(),
                        }),
                    )],
                ),
            )]),
            objects: vec![],
        };
        let dir = tempdir().unwrap();
        let err = emit_django_app(dir.path(), &inventory, DjangoEmitOptions::default())
            .expect_err("a relation to a type outside the model must fail");
        assert!(err.to_string().contains("dcim.site"), "{err}");
    }

    #[test]
    fn objects_are_emitted_as_a_fixture() {
        let dir = emit_to_temp(&sample_inventory());
        let fixture: Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join(FIXTURES_DIR).join(FIXTURE_FILE)).unwrap(),
        )
        .unwrap();
        let entries = fixture.as_array().expect("a fixture list");
        assert_eq!(entries.len(), 3);

        let device = entries
            .iter()
            .find(|entry| entry["pk"] == json!(Uuid::from_u128(1).to_string()))
            .expect("the device is in the fixture");
        let app_name = dir.path().file_name().unwrap().to_str().unwrap();
        assert_eq!(device["model"], json!(format!("{app_name}.dcimdevice")));
        assert_eq!(device["fields"]["name"], json!("leaf01"));
        // a relation carries over as the target uid, which is that model's pk.
        assert_eq!(
            device["fields"]["site"],
            json!(Uuid::from_u128(2).to_string())
        );
        assert_eq!(device["fields"]["key"], json!("{\"name\":\"leaf01\"}"));
        // what the schema does not model stays in the envelope blob.
        assert_eq!(
            device["fields"]["attrs"],
            json!({"unmodelled": "kept in attrs"})
        );
    }

    #[test]
    fn admin_list_display_excludes_many_to_many_fields() {
        // a ManyToManyField in admin list_display trips admin.E109 under `manage.py check`,
        // so a list_ref field must not leak into it. it must still exist as a model field.
        let inventory = Inventory {
            schema: schema_of(vec![
                (
                    "dcim.tag",
                    type_schema(vec![("name", field(FieldType::Slug))], vec![]),
                ),
                (
                    "dcim.device",
                    type_schema(
                        vec![("name", field(FieldType::Slug))],
                        vec![
                            ("name", field(FieldType::String)),
                            (
                                "tags",
                                optional(FieldType::ListRef {
                                    target: "dcim.tag".to_string(),
                                }),
                            ),
                        ],
                    ),
                ),
            ]),
            objects: vec![],
        };
        let dir = emit_to_temp(&inventory);

        // the field is still generated as a real M2M relation on the model...
        assert!(generated(&dir, GENERATED_MODELS).contains("tags = models.ManyToManyField("));

        // ...but it must not appear in the admin list_display (its only path into admin.py).
        let admin = generated(&dir, GENERATED_ADMIN);
        assert!(admin.contains("list_display"));
        assert!(!admin.contains("\"tags\""));
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
        let admin = generated(&emit_to_temp(&sample_inventory()), GENERATED_ADMIN);

        assert!(admin.contains("class DcimDeviceAdmin"));
        assert!(admin.contains(
            "list_display = [\"__str__\", \"name\", \"device_type\", \"role\", \"site\", \"uid\"]"
        ));
        // icontains has no postgres operator for uuid, so only text columns are searched.
        assert!(admin.contains("search_fields = [\"key\", \"name\", \"device_type\", \"role\"]"));
        assert!(admin.contains("list_select_related = [\"site\"]"));
        assert!(admin.contains("class DcimInterfaceAdmin"));
    }

    #[test]
    fn str_renders_the_key_values_not_the_json_key() {
        // `key` holds the ir key as json, so a __str__ returning it reads as
        // `{"name":"leaf01"}` in the admin and the browsable api.
        let models = generated(&emit_to_temp(&sample_inventory()), GENERATED_MODELS);
        assert!(
            models.contains("        return f\"{self.name}\""),
            "{models}"
        );
        // a composite key renders every component, refs through their own __str__.
        let inventory = Inventory {
            schema: schema_of(vec![
                (
                    "dcim.device",
                    type_schema(vec![("name", field(FieldType::Slug))], vec![]),
                ),
                (
                    "dcim.interface",
                    type_schema(
                        vec![
                            (
                                "device",
                                field(FieldType::Ref {
                                    target: "dcim.device".to_string(),
                                }),
                            ),
                            ("name", field(FieldType::Slug)),
                        ],
                        vec![],
                    ),
                ),
            ]),
            objects: vec![],
        };
        let models = generated(&emit_to_temp(&inventory), GENERATED_MODELS);
        assert!(
            models.contains("        return f\"{self.device} / {self.name}\""),
            "{models}"
        );
    }

    #[test]
    fn generated_api_files_include_models() {
        let dir = emit_to_temp(&sample_inventory());
        let serializers = generated(&dir, GENERATED_SERIALIZERS);
        let views = generated(&dir, GENERATED_VIEWS);
        let urls = generated(&dir, GENERATED_URLS);

        assert!(serializers.contains("class DcimDeviceSerializer"));
        assert!(views.contains("class DcimDeviceViewSet"));
        assert!(views.contains("ordering = [\"key\"]"));
        assert!(urls.contains("router.register(\"dcimdevices\""));
        // without drf-spectacular there is nothing to serve a schema with.
        assert!(!urls.contains("schema"), "{urls}");
    }

    #[test]
    fn filterset_fields_are_only_emitted_with_a_filter_backend() {
        // without django-filter installed there is no backend to honour them, and
        // a filter that silently returns everything is worse than none.
        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions {
                emit_admin: true,
                filter_backend: false,
                ..DjangoEmitOptions::default()
            },
        )
        .unwrap();
        assert!(!generated(&dir, GENERATED_VIEWS).contains("filterset_fields"));

        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions {
                emit_admin: true,
                filter_backend: true,
                ..DjangoEmitOptions::default()
            },
        )
        .unwrap();
        let views = generated(&dir, GENERATED_VIEWS);
        assert!(
            views.contains("filterset_fields = [\"key\", \"uid\", \"name\""),
            "{views}"
        );
    }

    #[test]
    fn schema_route_is_wired_when_drf_can_serve_it() {
        let dir = tempdir().unwrap();
        emit_django_app(
            dir.path(),
            &sample_inventory(),
            DjangoEmitOptions {
                schema_view: true,
                ..DjangoEmitOptions::default()
            },
        )
        .unwrap();
        let urls = generated(&dir, GENERATED_URLS);
        assert!(urls.contains("from drf_spectacular.views import"), "{urls}");
        assert!(urls.contains("SpectacularAPIView.as_view()"), "{urls}");
        assert!(urls.contains("SpectacularSwaggerView.as_view("), "{urls}");
    }

    #[test]
    fn generated_models_are_deterministic_by_kind() {
        let models = generated(&emit_to_temp(&sample_inventory()), GENERATED_MODELS);
        let device_pos = models.find("class DcimDevice").unwrap();
        let interface_pos = models.find("class DcimInterface").unwrap();
        let site_pos = models.find("class DcimSite").unwrap();
        assert!(device_pos < interface_pos);
        assert!(interface_pos < site_pos);
    }

    fn plain_field(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type,
            required: false,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn inventory_with_field(field_name: &str) -> Inventory {
        let mut types = BTreeMap::new();
        types.insert(
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([("name".to_string(), plain_field(FieldType::Slug))]),
                fields: BTreeMap::from([(field_name.to_string(), plain_field(FieldType::String))]),
            },
        );
        Inventory {
            schema: Schema { types },
            objects: vec![],
        }
    }

    fn emit_error(inventory: &Inventory) -> String {
        let dir = tempdir().unwrap();
        let result = emit_django_app(dir.path(), inventory, DjangoEmitOptions::default());
        result
            .expect_err("expected emit to reject the schema")
            .to_string()
    }

    #[test]
    fn rejects_field_named_uid() {
        // the silent one: a schema field `uid` would override the generated
        // `uid = models.UUIDField(primary_key=True)`, dropping the uuid primary key
        // without any error from `manage.py check`.
        let err = emit_error(&inventory_with_field("uid"));
        assert!(
            err.contains("uid"),
            "error should name the field, got: {err}"
        );
        assert!(
            err.contains("reserved"),
            "error should say reserved, got: {err}"
        );
    }

    #[test]
    fn rejects_field_named_key() {
        let err = emit_error(&inventory_with_field("key"));
        assert!(
            err.contains("'key'"),
            "error should name the field, got: {err}"
        );
        assert!(
            err.contains("reserved"),
            "error should say reserved, got: {err}"
        );
    }

    #[test]
    fn rejects_field_named_attrs() {
        let err = emit_error(&inventory_with_field("attrs"));
        assert!(
            err.contains("'attrs'"),
            "error should name the field, got: {err}"
        );
        assert!(
            err.contains("reserved"),
            "error should say reserved, got: {err}"
        );
    }

    #[test]
    fn rejects_python_keyword_field_name() {
        let err = emit_error(&inventory_with_field("from"));
        assert!(
            err.contains("'from'"),
            "error should name the field, got: {err}"
        );
    }

    #[test]
    fn rejects_leading_digit_field_name() {
        let err = emit_error(&inventory_with_field("10g_port"));
        assert!(
            err.contains("'10g_port'"),
            "error should name the field, got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_key_field_name() {
        // key fields are rendered as model fields too, so the identifier
        // check must cover them.
        let mut types = BTreeMap::new();
        types.insert(
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([("class".to_string(), plain_field(FieldType::Slug))]),
                fields: BTreeMap::new(),
            },
        );
        let inventory = Inventory {
            schema: Schema { types },
            objects: vec![],
        };
        let err = emit_error(&inventory);
        assert!(
            err.contains("'class'"),
            "error should name the key field, got: {err}"
        );
    }

    #[test]
    fn rejects_type_name_that_renders_to_invalid_class_name() {
        // class_name_for_type("10g.port") yields "10gPort", which cannot be a
        // python class name.
        let mut types = BTreeMap::new();
        types.insert(
            "10g.port".to_string(),
            TypeSchema {
                key: BTreeMap::from([("name".to_string(), plain_field(FieldType::Slug))]),
                fields: BTreeMap::new(),
            },
        );
        let inventory = Inventory {
            schema: Schema { types },
            objects: vec![],
        };
        let err = emit_error(&inventory);
        assert!(
            err.contains("10g.port"),
            "error should name the type, got: {err}"
        );
        assert!(
            err.contains("10gPort"),
            "error should show the class name, got: {err}"
        );
    }

    #[test]
    fn reports_all_offending_names_at_once() {
        let mut types = BTreeMap::new();
        types.insert(
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([("name".to_string(), plain_field(FieldType::Slug))]),
                fields: BTreeMap::from([
                    ("uid".to_string(), plain_field(FieldType::String)),
                    ("from".to_string(), plain_field(FieldType::String)),
                    ("2fa".to_string(), plain_field(FieldType::String)),
                ]),
            },
        );
        let inventory = Inventory {
            schema: Schema { types },
            objects: vec![],
        };
        let err = emit_error(&inventory);
        for name in ["'uid'", "'from'", "'2fa'"] {
            assert!(err.contains(name), "error should list {name}, got: {err}");
        }
    }

    #[test]
    fn accepts_soft_keyword_field_names() {
        // `match` and `type` are soft keywords, legal as python identifiers.
        let dir = tempdir().unwrap();
        let mut types = BTreeMap::new();
        types.insert(
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([("name".to_string(), plain_field(FieldType::Slug))]),
                fields: BTreeMap::from([
                    ("match".to_string(), plain_field(FieldType::String)),
                    ("type".to_string(), plain_field(FieldType::String)),
                ]),
            },
        );
        let inventory = Inventory {
            schema: Schema { types },
            objects: vec![],
        };
        emit_django_app(dir.path(), &inventory, DjangoEmitOptions::default()).unwrap();
    }

    #[test]
    fn render_routes_block_pluralizes_endpoints() {
        // `prefix` -> `prefixes` (not `prefixs`); vowel + y `gateway` ->
        // `gateways` (not `gatewaies`).
        let models = vec![
            ModelSpec {
                type_name: "ipam.prefix".to_string(),
                class_name: "Prefix".to_string(),
                fields: Vec::new(),
                key_fields: Vec::new(),
            },
            ModelSpec {
                type_name: "ipam.gateway".to_string(),
                class_name: "Gateway".to_string(),
                fields: Vec::new(),
                key_fields: Vec::new(),
            },
        ];
        let routes = render_routes_block(&models);
        assert!(routes.contains("router.register(\"prefixes\", PrefixViewSet)"));
        assert!(routes.contains("router.register(\"gateways\", GatewayViewSet)"));
    }
}
