//! validation utilities for the ir.

use crate::ir::{Attrs, Inventory, Kind, Object, Uid};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// validation errors emitted during graph validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("duplicate uid: {0}")]
    DuplicateUid(Uid),
    #[error("duplicate key: {0}")]
    DuplicateKey(String),
    #[error("missing reference {field} -> {target}")]
    MissingReference { field: &'static str, target: Uid },
    #[error("kind mismatch for {field}: expected {expected}, got {actual}")]
    KindMismatch {
        field: &'static str,
        expected: Kind,
        actual: Kind,
    },
}

/// aggregated validation report.
#[derive(Debug, Default, Clone)]
pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
}

impl ValidationReport {
    /// return true when no errors are present.
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

/// validate uniqueness and reference integrity for the given inventory.
pub fn validate_inventory(inventory: &Inventory) -> ValidationReport {
    let mut report = ValidationReport::default();
    let mut seen_uids = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    let mut uid_to_kind = BTreeMap::new();

    for object in &inventory.objects {
        if !seen_uids.insert(object.uid) {
            report
                .errors
                .push(ValidationError::DuplicateUid(object.uid));
        }
        if !seen_keys.insert(object.key.clone()) {
            report
                .errors
                .push(ValidationError::DuplicateKey(object.key.clone()));
        }
        uid_to_kind.insert(object.uid, object.kind);
    }

    for object in &inventory.objects {
        validate_object_refs(object, &uid_to_kind, &mut report);
    }

    report
}

/// validate references for a single object against the uid->kind map.
fn validate_object_refs(
    object: &Object,
    uid_to_kind: &BTreeMap<Uid, Kind>,
    report: &mut ValidationReport,
) {
    match &object.attrs {
        Attrs::Device(attrs) => {
            check_ref(
                "device.site",
                attrs.site,
                Kind::DcimSite,
                uid_to_kind,
                report,
            );
        }
        Attrs::Interface(attrs) => {
            check_ref(
                "interface.device",
                attrs.device,
                Kind::DcimDevice,
                uid_to_kind,
                report,
            );
        }
        Attrs::IpAddress(attrs) => {
            if let Some(target) = attrs.assigned_interface {
                check_ref(
                    "ip_address.assigned_interface",
                    target,
                    Kind::DcimInterface,
                    uid_to_kind,
                    report,
                );
            }
        }
        Attrs::Prefix(attrs) => {
            if let Some(target) = attrs.site {
                check_ref("prefix.site", target, Kind::DcimSite, uid_to_kind, report);
            }
        }
        Attrs::Site(_) => {}
    }
}

/// validate that a uid exists and matches the expected kind.
fn check_ref(
    field: &'static str,
    target: Uid,
    expected: Kind,
    uid_to_kind: &BTreeMap<Uid, Kind>,
    report: &mut ValidationReport,
) {
    match uid_to_kind.get(&target) {
        None => report
            .errors
            .push(ValidationError::MissingReference { field, target }),
        Some(actual) if *actual != expected => report.errors.push(ValidationError::KindMismatch {
            field,
            expected,
            actual: *actual,
        }),
        _ => {}
    }
}
