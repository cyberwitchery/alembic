//! `alembic version`: what this binary is and what it can do, for a caller
//! that checks for a capability instead of pinning a version range.

use alembic_adapter_sdk::external::EXTERNAL_PROTOCOL_VERSION;
use anyhow::Result;
use serde::Serialize;

/// capabilities a caller may test for. a name is added when a release adds
/// something worth checking, and never removed while the capability stays;
/// `docs/cli.md` lists what each one means.
const FEATURES: &[&str] = &[
    "apply-stdin",
    "exit-codes",
    "map-objects",
    "map-starlark",
    "plan-effects",
];

/// where state, and with it the apply journal, can be kept (`ALEMBIC_STATE_BACKEND`).
const STATE_BACKENDS: &[&str] = &["file", "postgres"];

#[derive(Debug, Serialize)]
struct VersionInfo {
    version: &'static str,
    backends: &'static [&'static str],
    external_protocol: u8,
    state_backends: &'static [&'static str],
    features: &'static [&'static str],
}

fn info() -> VersionInfo {
    VersionInfo {
        version: env!("CARGO_PKG_VERSION"),
        backends: alembic_adapter_registry::supported_backends(),
        external_protocol: EXTERNAL_PROTOCOL_VERSION,
        state_backends: STATE_BACKENDS,
        features: FEATURES,
    }
}

pub(super) fn print(json: bool) -> Result<()> {
    let info = info();
    if json {
        println!("{}", serde_json::to_string_pretty(&info)?);
    } else {
        println!("alembic {}", info.version);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_names_the_version_backends_and_protocol() {
        let value = serde_json::to_value(info()).unwrap();
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(value["external_protocol"], 1);
        let backends = value["backends"].as_array().unwrap();
        assert!(backends.contains(&"netbox".into()), "{backends:?}");
        assert!(backends.contains(&"external".into()), "{backends:?}");
        assert_eq!(
            value["state_backends"],
            serde_json::json!(["file", "postgres"])
        );
    }

    #[test]
    fn features_are_sorted_and_unique() {
        // a caller diffs this list across releases; keep it stable to read
        let mut sorted = FEATURES.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted, FEATURES);
    }
}
