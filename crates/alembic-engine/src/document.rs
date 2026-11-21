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

/// `raw` as json or yaml.
fn parse(raw: &str) -> Option<JsonValue> {
    serde_json::from_str(raw)
        .ok()
        .or_else(|| crate::yaml::from_str(raw).ok())
}

/// the kind `doc` looks like, with the key that gave it away.
fn sniff(doc: &JsonValue) -> Option<(DocumentKind, &'static str)> {
    let map = doc.as_object()?;
    MARKERS
        .iter()
        .find(|(key, _)| map.contains_key(*key))
        .map(|&(key, kind)| (kind, key))
}

/// the first type in `doc`'s schema whose `key` is a map, the form an earlier
/// alembic wrote.
fn map_form_key(doc: &JsonValue) -> Option<&str> {
    doc.get("schema")?
        .get("types")?
        .as_object()?
        .iter()
        .find(|(_, block)| block.get("key").is_some_and(JsonValue::is_object))
        .map(|(name, _)| name.as_str())
}

/// `err`, a failure to parse `raw` as `expected`, with a hint in front when
/// `raw` is another kind of document; the parse error stays underneath. a plan
/// with a map-form key was written by an earlier alembic, and gets an error that
/// says to plan again instead.
pub fn explain_parse_error(expected: DocumentKind, raw: &str, err: anyhow::Error) -> anyhow::Error {
    let Some(doc) = parse(raw) else {
        return err;
    };
    match sniff(&doc) {
        Some((found, key)) if found != expected => err.context(anyhow!(
            "expected {}, but this looks like {} (it has a top-level `{key}`); {}",
            expected.noun(),
            found.noun(),
            found.usage()
        )),
        Some((DocumentKind::Plan, _)) => match map_form_key(&doc) {
            Some(type_name) => anyhow!(
                "this plan was written by an earlier alembic (the key of {type_name} is a map); \
                 make it again with `alembic plan`"
            ),
            None => err,
        },
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
            let found = parse(raw).as_ref().and_then(sniff).map(|(kind, _)| kind);
            assert_eq!(found, Some(kind), "{raw}");
        }
        for raw in ["", "- a\n- b\n", "not: alembic\n", "{not json"] {
            assert_eq!(parse(raw).as_ref().and_then(sniff), None, "{raw}");
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
    fn explain_parse_error_says_to_plan_again_for_a_plan_with_a_map_form_key() {
        let raw = "schema:\n  types:\n    dcim.site:\n      key: {slug: {type: slug}}\nops: []\n";
        let err = explain_parse_error(
            DocumentKind::Plan,
            raw,
            anyhow!("key is a list of field names, not a map"),
        );
        assert_eq!(
            format!("{err:#}"),
            "this plan was written by an earlier alembic (the key of dcim.site is a map); \
             make it again with `alembic plan`"
        );
        // an inventory keeps the parse error, which says how to write the list.
        let raw = raw.replace("ops: []", "objects: []");
        let err = explain_parse_error(
            DocumentKind::Inventory,
            &raw,
            anyhow!("key is a list of field names, not a map"),
        );
        assert_eq!(
            format!("{err:#}"),
            "key is a list of field names, not a map"
        );
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
