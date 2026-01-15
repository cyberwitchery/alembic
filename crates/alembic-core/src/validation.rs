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
    #[error("missing field: {0}")]
    MissingField(&'static str),
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
        if object.key.trim().is_empty() {
            report.errors.push(ValidationError::MissingField("key"));
        }
        if object.kind.is_empty() {
            report.errors.push(ValidationError::MissingField("kind"));
        }
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
        uid_to_kind.insert(object.uid, object.kind.clone());
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
        Attrs::Site(_) | Attrs::Generic(_) => {}
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
            actual: actual.clone(),
        }),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Attrs, DeviceAttrs, InterfaceAttrs, Object, SiteAttrs};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    #[test]
    fn detects_duplicate_keys() {
        let objects = vec![
            Object::new(
                uid(1),
                "site=fra1".to_string(),
                Attrs::Site(SiteAttrs {
                    name: "FRA1".to_string(),
                    slug: "fra1".to_string(),
                    status: None,
                    description: None,
                }),
            ),
            Object::new(
                uid(2),
                "site=fra1".to_string(),
                Attrs::Site(SiteAttrs {
                    name: "FRA1-dup".to_string(),
                    slug: "fra1-dup".to_string(),
                    status: None,
                    description: None,
                }),
            ),
        ];
        let report = validate_inventory(&Inventory { objects });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::DuplicateKey(_))));
    }

    #[test]
    fn detects_missing_key() {
        let objects = vec![Object::new(
            uid(30),
            "".to_string(),
            Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: None,
                description: None,
            }),
        )];
        let report = validate_inventory(&Inventory { objects });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingField("key"))));
    }

    #[test]
    fn detects_missing_kind() {
        let object = Object::new_generic(
            uid(31),
            Kind::Custom("".to_string()),
            "custom=empty".to_string(),
            BTreeMap::new(),
        );
        let report = validate_inventory(&Inventory {
            objects: vec![object],
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingField("kind"))));
    }

    #[test]
    fn detects_kind_mismatch() {
        let site_uid = uid(10);
        let wrong_uid = uid(11);
        let objects = vec![
            Object::new(
                site_uid,
                "site=fra1".to_string(),
                Attrs::Site(SiteAttrs {
                    name: "FRA1".to_string(),
                    slug: "fra1".to_string(),
                    status: None,
                    description: None,
                }),
            ),
            Object::new(
                wrong_uid,
                "device=leaf01".to_string(),
                Attrs::Device(DeviceAttrs {
                    name: "leaf01".to_string(),
                    site: site_uid,
                    role: "leaf".to_string(),
                    device_type: "leaf-switch".to_string(),
                    status: None,
                }),
            ),
            Object::new(
                uid(12),
                "device=leaf01/interface=eth0".to_string(),
                Attrs::Interface(InterfaceAttrs {
                    name: "eth0".to_string(),
                    device: site_uid,
                    if_type: None,
                    enabled: None,
                    description: None,
                }),
            ),
        ];
        let report = validate_inventory(&Inventory { objects });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::KindMismatch { .. })));
    }
}
