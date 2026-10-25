//! telling alembic's documents apart, so a file passed where another kind was
//! expected is named for what it is instead of failing as a bare parse error.

use anyhow::anyhow;
use serde_json::Value as JsonValue;

/// the documents the cli reads or writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentKind {
    Inventory,
    Plan,
    MapSpec,
    DriftReport,
    ValidationReport,
    ApplyReport,
    State,
}

impl DocumentKind {
    fn noun(self) -> &'static str {
        match self {
            DocumentKind::Inventory => "an inventory",
            DocumentKind::Plan => "a plan",
            DocumentKind::MapSpec => "a map spec",
            DocumentKind::DriftReport => "a drift report",
            DocumentKind::ValidationReport => "a validation report",
            DocumentKind::ApplyReport => "an apply report",
            DocumentKind::State => "a state file",
        }
    }

    /// where this kind of document comes from or goes, for the hint.
    fn usage(self) -> &'static str {
        match self {
            DocumentKind::Inventory => "pass it to `-f` of validate, plan, import or map",
            DocumentKind::Plan => "pass it to `apply -p`",
            DocumentKind::MapSpec => "pass it to `map --spec`",
            DocumentKind::DriftReport => "`plan --report -o` writes it, no command reads it",
            DocumentKind::ValidationReport => "`validate -o` writes it, no command reads it",
            DocumentKind::ApplyReport => "`apply -o` writes it, no command reads it",
            DocumentKind::State => "alembic keeps it under `.alembic/`, no command takes it",
        }
    }
}

/// top-level keys only one kind of document carries, most specific first: a
/// map spec and an inventory both have `objects`, a plan and an inventory both
/// have `schema`.
const MARKERS: &[(&str, DocumentKind)] = &[
    ("ops", DocumentKind::Plan),
    ("rules", DocumentKind::MapSpec),
    ("transforms", DocumentKind::MapSpec),
    ("include", DocumentKind::Inventory),
    ("imports", DocumentKind::Inventory),
    ("scope", DocumentKind::Inventory),
    ("objects", DocumentKind::Inventory),
    ("changed", DocumentKind::DriftReport),
    ("missing", DocumentKind::DriftReport),
    ("extra", DocumentKind::DriftReport),
    ("applied", DocumentKind::ApplyReport),
    ("resumed", DocumentKind::ApplyReport),
    ("errors", DocumentKind::ValidationReport),
    ("mappings", DocumentKind::State),
];

/// the kind `raw` looks like, with the key that gave it away. json or yaml.
fn sniff(raw: &str) -> Option<(DocumentKind, &'static str)> {
    let doc: JsonValue = serde_json::from_str(raw)
        .ok()
        .or_else(|| crate::yaml::from_str(raw).ok())?;
    let map = doc.as_object()?;
    MARKERS
        .iter()
        .find(|(key, _)| map.contains_key(*key))
        .map(|&(key, kind)| (kind, key))
}

/// `err`, a failure to parse `raw` as `expected`, with a hint in front when
/// `raw` is another kind of document. the parse error stays underneath.
pub fn explain_parse_error(expected: DocumentKind, raw: &str, err: anyhow::Error) -> anyhow::Error {
    match sniff(raw) {
        Some((found, key)) if found != expected => err.context(anyhow!(
            "expected {}, but this looks like {} (it has a top-level `{key}`); {}",
            expected.noun(),
            found.noun(),
            found.usage()
        )),
        _ => err,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniff_names_each_document_by_its_marker() {
        for (raw, kind) in [
            ("schema: {types: {}}\nops: []\n", DocumentKind::Plan),
            ("schema: {types: {}}\nrules: []\n", DocumentKind::MapSpec),
            (
                "schema: {types: {}}\nobjects: []\n",
                DocumentKind::Inventory,
            ),
            ("include: [a.yaml]\n", DocumentKind::Inventory),
            (
                r#"{"changed": [], "missing": [], "extra": []}"#,
                DocumentKind::DriftReport,
            ),
            (r#"{"applied": []}"#, DocumentKind::ApplyReport),
            (r#"{"errors": []}"#, DocumentKind::ValidationReport),
            (r#"{"backend": null, "mappings": {}}"#, DocumentKind::State),
        ] {
            assert_eq!(sniff(raw).map(|(kind, _)| kind), Some(kind), "{raw}");
        }
        for raw in ["", "- a\n- b\n", "not: alembic\n", "{not json"] {
            assert_eq!(sniff(raw), None, "{raw}");
        }
    }

    #[test]
    fn explain_parse_error_leaves_a_document_of_the_expected_kind_alone() {
        let err = explain_parse_error(
            DocumentKind::Plan,
            "schema: {types: {}}\nops: [1]\n",
            anyhow!("invalid op"),
        );
        assert_eq!(format!("{err:#}"), "invalid op");
    }

    #[test]
    fn explain_parse_error_names_the_document_it_got() {
        let err = explain_parse_error(
            DocumentKind::Inventory,
            "schema: {types: {}}\nops: []\n",
            anyhow!("unknown field `ops`"),
        );
        assert_eq!(
            format!("{err:#}"),
            "expected an inventory, but this looks like a plan (it has a top-level `ops`); \
             pass it to `apply -p`: unknown field `ops`"
        );
    }
}
