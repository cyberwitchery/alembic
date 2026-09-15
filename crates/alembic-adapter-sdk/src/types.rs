use alembic_core::{Key, Object, TypeName, Uid};
use serde::{Deserialize, Serialize};
use std::fmt;

/// generic backend identifier (integer or string/uuid).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackendId {
    Int(u64),
    String(String),
}

impl fmt::Display for BackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendId::Int(id) => write!(f, "{}", id),
            BackendId::String(id) => write!(f, "{}", id),
        }
    }
}

impl From<u64> for BackendId {
    fn from(id: u64) -> Self {
        BackendId::Int(id)
    }
}

impl From<String> for BackendId {
    fn from(id: String) -> Self {
        BackendId::String(id)
    }
}

/// plan operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    /// create a new backend object.
    Create {
        uid: Uid,
        type_name: TypeName,
        desired: Object,
    },
    /// update an existing backend object.
    Update {
        uid: Uid,
        type_name: TypeName,
        desired: Object,
        changes: Vec<FieldChange>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_id: Option<BackendId>,
    },
    /// delete a backend object.
    Delete {
        uid: Uid,
        type_name: TypeName,
        key: Key,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_id: Option<BackendId>,
    },
}

impl Op {
    /// returns the ir uid for this operation.
    pub fn uid(&self) -> Uid {
        match self {
            Op::Create { uid, .. } => *uid,
            Op::Update { uid, .. } => *uid,
            Op::Delete { uid, .. } => *uid,
        }
    }

    /// returns the type name for this operation.
    pub fn type_name(&self) -> &TypeName {
        match self {
            Op::Create { type_name, .. } => type_name,
            Op::Update { type_name, .. } => type_name,
            Op::Delete { type_name, .. } => type_name,
        }
    }

    pub fn hashed(&self) -> u64 {
        stable_json_hash(self)
    }
}

/// hash a value's json serialization via the same v5 uuid mechanism ir
/// identity is built on. journal identity (file names, per-op hashes) is
/// persisted to disk and compared across runs, so it must not depend on
/// `DefaultHasher`, whose algorithm is not stable across rust releases.
pub fn stable_json_hash<T: Serialize>(value: &T) -> u64 {
    // serializing engine types cannot fail: plain structs and enums whose only
    // maps are string-keyed.
    let bytes = serde_json::to_vec(value).expect("engine value serializes to json");
    uuid::Uuid::new_v5(&alembic_core::ALEMBIC_UID_NAMESPACE, &bytes)
        .as_u64_pair()
        .0
}

/// field-level change for an update op.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
#[serde(deny_unknown_fields)]
pub struct FieldChange {
    /// field name within attrs.
    pub field: String,
    /// previous value from observed state.
    pub from: serde_json::Value,
    /// desired value from the ir.
    pub to: serde_json::Value,
}

/// result for a single applied operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedOp {
    /// ir uid for the operation.
    pub uid: Uid,
    /// type for the operation.
    pub type_name: TypeName,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// backend id returned by the adapter, if any.
    pub backend_id: Option<BackendId>,
}

/// aggregated apply report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyReport {
    /// list of operations applied by the adapter.
    #[serde(default)]
    pub applied: Vec<AppliedOp>,
    /// operations an interrupted run applied, recovered from its journal on resume,
    /// with the backend id each one returned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resumed: Vec<AppliedOp>,
    /// number of previously applied operations, only set when apply is accompanied by a journal
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previously_applied_count: Option<usize>,
    /// provisioning report, merged from both passes: `ensure_schema` and `write`.
    #[serde(default)]
    pub provision: ProvisionReport,
}

/// report of what an apply provisioned, across `ensure_schema` and `write`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionReport {
    /// custom fields created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_fields: Vec<String>,
    /// custom fields the backend already had, converged onto their declared
    /// properties.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated_fields: Vec<String>,
    /// tags created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_tags: Vec<String>,
    /// custom object types created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_object_types: Vec<String>,
    /// custom object fields created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_object_fields: Vec<String>,
    /// custom object fields the backend already had, converged onto their declared
    /// properties. its own category because creates and deletes are split
    /// native/object here too.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated_object_fields: Vec<String>,
    /// object types deprecated on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deprecated_object_types: Vec<String>,
    /// object fields deprecated on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deprecated_object_fields: Vec<String>,
    /// object types deleted on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_object_types: Vec<String>,
    /// object fields deleted on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_object_fields: Vec<String>,
}

impl ProvisionReport {
    /// fold another report in. one apply provisions in two passes -- `ensure_schema`
    /// fills the schema categories, `write` fills the tags it creates from the ops --
    /// so both have to reach the same report.
    pub fn merge(&mut self, other: ProvisionReport) {
        // every reader of these categories destructures without `..`: a category
        // added later has to answer in each rather than be dropped in silence.
        let ProvisionReport {
            created_fields,
            updated_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = other;
        self.created_fields.extend(created_fields);
        self.updated_fields.extend(updated_fields);
        self.created_tags.extend(created_tags);
        self.created_object_types.extend(created_object_types);
        self.created_object_fields.extend(created_object_fields);
        self.updated_object_fields.extend(updated_object_fields);
        self.deprecated_object_types.extend(deprecated_object_types);
        self.deprecated_object_fields
            .extend(deprecated_object_fields);
        self.deleted_object_types.extend(deleted_object_types);
        self.deleted_object_fields.extend(deleted_object_fields);
    }

    /// the changes a run made to schema it did not create, labelled for the
    /// operator. a create is new, so its count is the whole story; everything
    /// else reaches into state that was already there, so name what it wrote.
    pub fn named_changes(&self, tense: Tense) -> Vec<(&'static str, &str)> {
        // destructured without `..`, like the folds above: a category added later
        // has to answer whether it names what it touched.
        let ProvisionReport {
            created_fields: _,
            updated_fields,
            created_tags: _,
            created_object_types: _,
            created_object_fields: _,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = self;
        let (updated, deprecated, deleted) = match tense {
            Tense::Past => ("updated", "deprecated", "deleted"),
            Tense::Would => ("would update", "would deprecate", "would delete"),
        };
        [
            (updated, updated_fields),
            (updated, updated_object_fields),
            (deprecated, deprecated_object_types),
            (deprecated, deprecated_object_fields),
            (deleted, deleted_object_types),
            (deleted, deleted_object_fields),
        ]
        .into_iter()
        .flat_map(|(label, names)| names.iter().map(move |name| (label, name.as_str())))
        .collect()
    }

    pub fn is_empty(&self) -> bool {
        let ProvisionReport {
            created_fields,
            updated_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = self;
        created_fields.is_empty()
            && updated_fields.is_empty()
            && created_tags.is_empty()
            && created_object_types.is_empty()
            && created_object_fields.is_empty()
            && updated_object_fields.is_empty()
            && deprecated_object_types.is_empty()
            && deprecated_object_fields.is_empty()
            && deleted_object_types.is_empty()
            && deleted_object_fields.is_empty()
    }
}

impl fmt::Display for ProvisionReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(f, "no schema changes");
        }

        let ProvisionReport {
            created_fields,
            updated_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = self;

        let mut first = true;
        let sections: &[(&str, &[String])] = &[
            ("fields created", created_fields),
            ("fields updated", updated_fields),
            ("tags created", created_tags),
            ("object types created", created_object_types),
            ("object fields created", created_object_fields),
            ("object fields updated", updated_object_fields),
            ("object types deprecated", deprecated_object_types),
            ("object fields deprecated", deprecated_object_fields),
            ("object types deleted", deleted_object_types),
            ("object fields deleted", deleted_object_fields),
        ];

        for (label, items) in sections {
            if items.is_empty() {
                continue;
            }
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{} {label}", items.len())?;
            first = false;
        }

        Ok(())
    }
}

/// how `named_changes` words a change: what a run did, or what a preview says
/// it would do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tense {
    Past,
    Would,
}
