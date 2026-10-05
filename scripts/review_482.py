from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement target, found {count}")
    file.write_text(text.replace(old, new, 1))


types = "crates/alembic-adapter-sdk/src/types.rs"
replace_once(
    types,
    """        .flat_map(|(label, names)| names.iter().map(move |name| (label, name.as_str())))
        .collect()
    }

    pub fn is_empty(&self) -> bool {
""",
    """        .flat_map(|(label, names)| names.iter().map(move |name| (label, name.as_str())))
        .collect()
    }

    /// summarize schema changes for an operator, using past tense for changes a
    /// run made and prospective wording for a read-only preview.
    pub fn summary(&self, tense: Tense) -> String {
        if self.is_empty() {
            return \"no schema changes\".to_string();
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
        let (created, updated, deprecated, deleted) = match tense {
            Tense::Past => (\"created\", \"updated\", \"deprecated\", \"deleted\"),
            Tense::Would => (
                \"would be created\",
                \"would be updated\",
                \"would be deprecated\",
                \"would be deleted\",
            ),
        };

        [
            (\"fields\", created, created_fields),
            (\"fields\", updated, updated_fields),
            (\"tags\", created, created_tags),
            (\"object types\", created, created_object_types),
            (\"object fields\", created, created_object_fields),
            (\"object fields\", updated, updated_object_fields),
            (\"object types\", deprecated, deprecated_object_types),
            (\"object fields\", deprecated, deprecated_object_fields),
            (\"object types\", deleted, deleted_object_types),
            (\"object fields\", deleted, deleted_object_fields),
        ]
        .into_iter()
        .filter(|(_, _, items)| !items.is_empty())
        .map(|(kind, verb, items)| format!(\"{} {kind} {verb}\", items.len()))
        .collect::<Vec<_>>()
        .join(\", \")
    }

    pub fn is_empty(&self) -> bool {
""",
)

replace_once(
    types,
    """impl fmt::Display for ProvisionReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(f, \"no schema changes\");
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
            (\"fields created\", created_fields),
            (\"fields updated\", updated_fields),
            (\"tags created\", created_tags),
            (\"object types created\", created_object_types),
            (\"object fields created\", created_object_fields),
            (\"object fields updated\", updated_object_fields),
            (\"object types deprecated\", deprecated_object_types),
            (\"object fields deprecated\", deprecated_object_fields),
            (\"object types deleted\", deleted_object_types),
            (\"object fields deleted\", deleted_object_fields),
        ];

        for (label, items) in sections {
            if items.is_empty() {
                continue;
            }
            if !first {
                write!(f, \", \")?;
            }
            write!(f, \"{} {label}\", items.len())?;
            first = false;
        }

        Ok(())
    }
}
""",
    """impl fmt::Display for ProvisionReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.summary(Tense::Past))
    }
}
""",
)

replace_once(
    types,
    """    #[test]
    fn op_helpers() {
""",
    """    #[test]
    fn provision_report_summary_honors_tense() {
        let report = ProvisionReport {
            created_fields: vec![\"site.tier\".to_string()],
            updated_fields: vec![\"site.owner\".to_string()],
            created_tags: vec![\"managed\".to_string()],
            created_object_types: vec![\"dcim.widget\".to_string()],
            created_object_fields: vec![\"dcim.widget.size\".to_string()],
            updated_object_fields: vec![\"dcim.widget.color\".to_string()],
            deprecated_object_types: vec![\"dcim.gadget\".to_string()],
            deprecated_object_fields: vec![\"dcim.gadget.color\".to_string()],
            deleted_object_types: vec![\"dcim.relic\".to_string()],
            deleted_object_fields: vec![\"dcim.relic.age\".to_string()],
        };

        assert_eq!(
            report.summary(Tense::Past),
            \"1 fields created, 1 fields updated, 1 tags created, 1 object types created, 1 object fields created, 1 object fields updated, 1 object types deprecated, 1 object fields deprecated, 1 object types deleted, 1 object fields deleted\"
        );
        assert_eq!(
            report.summary(Tense::Would),
            \"1 fields would be created, 1 fields would be updated, 1 tags would be created, 1 object types would be created, 1 object fields would be created, 1 object fields would be updated, 1 object types would be deprecated, 1 object fields would be deprecated, 1 object types would be deleted, 1 object fields would be deleted\"
        );
        assert_eq!(format!(\"{report}\"), report.summary(Tense::Past));

        let empty = ProvisionReport::default();
        assert_eq!(empty.summary(Tense::Past), \"no schema changes\");
        assert_eq!(empty.summary(Tense::Would), \"no schema changes\");
        assert_eq!(empty.to_string(), \"no schema changes\");
    }

    #[test]
    fn op_helpers() {
""",
)

app = "crates/alembic-cli/src/app/mod.rs"
replace_once(
    app,
    "use alembic_adapter_sdk::{ApplyReport, Op, ProvisionReport, StateData, Tense};\n",
    "use alembic_adapter_sdk::{ApplyReport, Op, StateData, Tense};\n",
)
replace_once(
    app,
    """/// format the count summary for a read-only schema preview. the report's
/// `Display` is intentionally past-tense because it also backs real provisioning
/// output; previews need to say what would happen instead of what already did.
fn schema_preview_summary(report: &ProvisionReport) -> String {
    if report.is_empty() {
        return \"no schema changes\".to_string();
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
    } = report;

    [
        (\"fields would be created\", created_fields),
        (\"fields would be updated\", updated_fields),
        (\"tags would be created\", created_tags),
        (\"object types would be created\", created_object_types),
        (\"object fields would be created\", created_object_fields),
        (\"object fields would be updated\", updated_object_fields),
        (\"object types would be deprecated\", deprecated_object_types),
        (\"object fields would be deprecated\", deprecated_object_fields),
        (\"object types would be deleted\", deleted_object_types),
        (\"object fields would be deleted\", deleted_object_fields),
    ]
    .into_iter()
    .filter(|(_, items)| !items.is_empty())
    .map(|(label, items)| format!(\"{} {label}\", items.len()))
    .collect::<Vec<_>>()
    .join(\", \")
}

""",
    "",
)
replace_once(
    app,
    '                            eprintln!("schema preview: {}", schema_preview_summary(&report));\n',
    '                            eprintln!("schema preview: {}", report.summary(Tense::Would));\n',
)

changelog = "CHANGELOG.md"
replace_once(
    changelog,
    "## Unreleased\n\n",
    "## Unreleased\n\n- cli: schema previews name created fields, tags, object types and object fields, and their count summary uses prospective wording (`would be created`, etc.) instead of claiming the read-only preview already made those changes (#478)\n",
)
