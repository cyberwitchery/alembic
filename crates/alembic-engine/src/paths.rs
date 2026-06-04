use anyhow::{anyhow, Result};
use serde_yaml::Value as YamlValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PredicateOp {
    Eq,
    Ne,
    Exists,
    NotExists,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Predicate {
    field: String,
    op: PredicateOp,
    value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectorToken {
    Key(String),
    Index(usize),
    Wildcard,
    Predicate(Predicate),
}

#[derive(Debug, Clone)]
pub(crate) enum PathToken {
    Key(String),
    Index(usize),
}

#[derive(Debug)]
pub(crate) struct RelativePath {
    up: usize,
    selectors: Vec<SelectorToken>,
}

pub(crate) fn parse_selector_path(path: &str) -> Result<Vec<SelectorToken>> {
    if !path.starts_with('/') {
        return Err(anyhow!("select path must start with '/'"));
    }
    let mut tokens = Vec::new();
    for segment in split_path_segments(path.trim_start_matches('/')) {
        if segment.is_empty() {
            continue;
        }
        tokens.extend(parse_selector_segment(segment)?);
    }
    Ok(tokens)
}

/// Split a path on `/`, treating any `/` inside `[...]` as literal so predicate
/// values may contain slashes (e.g. the CIDR in `[prefix=10.0.0.0/24]`).
fn split_path_segments(path: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (idx, ch) in path.char_indices() {
        match ch {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            '/' if depth == 0 => {
                segments.push(&path[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    segments.push(&path[start..]);
    segments
}

/// Parse a single segment into its base selector (if any) followed by its
/// trailing predicates, e.g. `devices[role=leaf][vendor=cisco]`.
fn parse_selector_segment(segment: &str) -> Result<Vec<SelectorToken>> {
    let bracket = segment.find('[');
    let base = match bracket {
        Some(idx) => &segment[..idx],
        None => segment,
    };
    let mut tokens = Vec::new();
    if !base.is_empty() {
        tokens.push(parse_base_selector(base));
    }
    if let Some(idx) = bracket {
        for predicate in parse_predicates(&segment[idx..])? {
            tokens.push(SelectorToken::Predicate(predicate));
        }
    }
    if tokens.is_empty() {
        return Err(anyhow!("empty path segment"));
    }
    Ok(tokens)
}

fn parse_base_selector(base: &str) -> SelectorToken {
    if base == "*" {
        SelectorToken::Wildcard
    } else if let Ok(index) = base.parse::<usize>() {
        SelectorToken::Index(index)
    } else {
        SelectorToken::Key(base.to_string())
    }
}

/// Parse one or more `[field op value]` predicates from the start of `rest`.
fn parse_predicates(mut rest: &str) -> Result<Vec<Predicate>> {
    let mut predicates = Vec::new();
    while !rest.is_empty() {
        if !rest.starts_with('[') {
            return Err(anyhow!("unexpected text after predicate: '{rest}'"));
        }
        let Some(end) = rest.find(']') else {
            return Err(anyhow!("unterminated '[' in path segment"));
        };
        predicates.push(parse_predicate(&rest[1..end])?);
        rest = &rest[end + 1..];
    }
    Ok(predicates)
}

/// Parse the inside of a predicate. With an `=`, it is a value predicate
/// (`field=value`, or `field!=value` when `!` immediately precedes the `=`) and
/// the value is the literal remainder up to the closing `]`. With no `=`, it is
/// an existence predicate: a bare `field` is `Exists`, and a leading `!`
/// (`!field`) is `NotExists`. An empty field name is an error in every form.
fn parse_predicate(inner: &str) -> Result<Predicate> {
    let Some(eq) = inner.find('=') else {
        // No `=`: an existence predicate. A leading `!` negates it.
        let (op, field) = match inner.strip_prefix('!') {
            Some(rest) => (PredicateOp::NotExists, rest.trim()),
            None => (PredicateOp::Exists, inner.trim()),
        };
        if field.is_empty() {
            return Err(anyhow!("predicate '{inner}' has an empty field name"));
        }
        return Ok(Predicate {
            field: field.to_string(),
            op,
            value: String::new(),
        });
    };
    let (op, field_end) = if eq > 0 && inner.as_bytes()[eq - 1] == b'!' {
        (PredicateOp::Ne, eq - 1)
    } else {
        (PredicateOp::Eq, eq)
    };
    let field = inner[..field_end].trim();
    if field.is_empty() {
        return Err(anyhow!("predicate '{inner}' has an empty field name"));
    }
    Ok(Predicate {
        field: field.to_string(),
        op,
        value: inner[eq + 1..].to_string(),
    })
}

pub(crate) fn parse_relative_path(path: &str) -> Result<RelativePath> {
    let mut rest = path.trim();
    let mut up = 0;
    while rest.starts_with('^') {
        up += 1;
        rest = &rest[1..];
        if rest.starts_with('.') {
            rest = &rest[1..];
        }
    }
    if rest.starts_with('.') {
        rest = &rest[1..];
    }
    if rest.starts_with('/') {
        rest = &rest[1..];
    }
    let mut selectors = Vec::new();
    for segment in split_path_segments(rest) {
        if segment.is_empty() {
            continue;
        }
        selectors.extend(parse_selector_segment(segment)?);
    }
    Ok(RelativePath { up, selectors })
}

pub(crate) fn extract_values<'a>(
    raw: &'a YamlValue,
    path: &[PathToken],
    rel: &RelativePath,
) -> Result<Vec<&'a YamlValue>> {
    let base_path = ancestor_path(raw, path, rel.up)?;
    let Some(base_value) = value_at_path(raw, &base_path) else {
        return Ok(Vec::new());
    };
    let mut results = Vec::new();
    select_values(base_value, &rel.selectors, &mut results);
    Ok(results)
}

fn ancestor_path(raw: &YamlValue, path: &[PathToken], up: usize) -> Result<Vec<PathToken>> {
    let mut current: Vec<PathToken> = path.to_vec();
    for _ in 0..up {
        if current.is_empty() {
            return Err(anyhow!("relative path escapes above root"));
        }
        current.pop();
        while let Some(value) = value_at_path(raw, &current) {
            if matches!(value, YamlValue::Sequence(_)) {
                if current.is_empty() {
                    break;
                }
                current.pop();
            } else {
                break;
            }
        }
    }
    Ok(current)
}

fn value_at_path<'a>(value: &'a YamlValue, path: &[PathToken]) -> Option<&'a YamlValue> {
    let mut current = value;
    for token in path {
        match token {
            PathToken::Key(key) => {
                let YamlValue::Mapping(map) = current else {
                    return None;
                };
                current = map.get(YamlValue::String(key.clone()))?;
            }
            PathToken::Index(index) => {
                let YamlValue::Sequence(items) = current else {
                    return None;
                };
                current = items.get(*index)?;
            }
        }
    }
    Some(current)
}

fn select_values<'a>(
    value: &'a YamlValue,
    selectors: &[SelectorToken],
    results: &mut Vec<&'a YamlValue>,
) {
    if selectors.is_empty() {
        results.push(value);
        return;
    }
    match selectors[0].clone() {
        SelectorToken::Key(key) => {
            if let YamlValue::Mapping(map) = value {
                if let Some(value) = map.get(YamlValue::String(key)) {
                    select_values(value, &selectors[1..], results);
                }
            }
        }
        SelectorToken::Index(index) => {
            if let YamlValue::Sequence(items) = value {
                if let Some(value) = items.get(index) {
                    select_values(value, &selectors[1..], results);
                }
            }
        }
        SelectorToken::Wildcard => match value {
            YamlValue::Sequence(items) => {
                for value in items {
                    select_values(value, &selectors[1..], results);
                }
            }
            YamlValue::Mapping(map) => {
                for (key, value) in map {
                    if key.as_str().is_none() {
                        continue;
                    }
                    select_values(value, &selectors[1..], results);
                }
            }
            _ => {}
        },
        SelectorToken::Predicate(predicate) => match value {
            YamlValue::Sequence(items) => {
                for item in items {
                    if node_satisfies(item, &predicate) {
                        select_values(item, &selectors[1..], results);
                    }
                }
            }
            YamlValue::Mapping(_) if node_satisfies(value, &predicate) => {
                select_values(value, &selectors[1..], results);
            }
            _ => {}
        },
    }
}

/// A node satisfies a predicate only when it is a mapping. `field=value` and
/// `field!=value` compare the field's scalar rendering (see `render_scalar`), so
/// a missing or non-scalar field never satisfies them. `[field]` (`Exists`) holds
/// when `field` is present and non-null, for ANY value type -- sequences and
/// mappings included, since this tests presence rather than a scalar value.
/// `[!field]` (`NotExists`) is the complement: it holds when `field` is absent or
/// null.
fn node_satisfies(value: &YamlValue, predicate: &Predicate) -> bool {
    let YamlValue::Mapping(map) = value else {
        return false;
    };
    let field = map.get(YamlValue::String(predicate.field.clone()));
    match predicate.op {
        PredicateOp::Exists => matches!(field, Some(value) if !value.is_null()),
        PredicateOp::NotExists => match field {
            Some(value) => value.is_null(),
            None => true,
        },
        PredicateOp::Eq => {
            matches!(field.and_then(render_scalar), Some(rendered) if rendered == predicate.value)
        }
        PredicateOp::Ne => {
            matches!(field.and_then(render_scalar), Some(rendered) if rendered != predicate.value)
        }
    }
}

/// Render a scalar yaml value to the text a predicate compares against. Strings
/// pass through; numbers and bools use their natural form (`42`, `true`). Null,
/// sequences, mappings, and tagged values are not scalars and never match.
fn render_scalar(value: &YamlValue) -> Option<String> {
    match value {
        YamlValue::String(text) => Some(text.clone()),
        YamlValue::Number(number) => Some(number.to_string()),
        YamlValue::Bool(boolean) => Some(boolean.to_string()),
        YamlValue::Null | YamlValue::Sequence(_) | YamlValue::Mapping(_) | YamlValue::Tagged(_) => {
            None
        }
    }
}

pub(crate) fn select_paths(
    value: &YamlValue,
    selectors: &[SelectorToken],
    current_path: &mut Vec<PathToken>,
    results: &mut Vec<Vec<PathToken>>,
) {
    if selectors.is_empty() {
        results.push(current_path.clone());
        return;
    }

    match selectors[0].clone() {
        SelectorToken::Key(key) => {
            if let YamlValue::Mapping(map) = value {
                if let Some(value) = map.get(YamlValue::String(key.clone())) {
                    current_path.push(PathToken::Key(key));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
        }
        SelectorToken::Index(index) => {
            if let YamlValue::Sequence(items) = value {
                if let Some(value) = items.get(index) {
                    current_path.push(PathToken::Index(index));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
        }
        SelectorToken::Wildcard => match value {
            YamlValue::Sequence(items) => {
                for (index, value) in items.iter().enumerate() {
                    current_path.push(PathToken::Index(index));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
            YamlValue::Mapping(map) => {
                for (key, value) in map {
                    let Some(key) = key.as_str() else {
                        continue;
                    };
                    current_path.push(PathToken::Key(key.to_string()));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
            _ => {}
        },
        SelectorToken::Predicate(predicate) => match value {
            YamlValue::Sequence(items) => {
                for (index, item) in items.iter().enumerate() {
                    if node_satisfies(item, &predicate) {
                        current_path.push(PathToken::Index(index));
                        select_paths(item, &selectors[1..], current_path, results);
                        current_path.pop();
                    }
                }
            }
            YamlValue::Mapping(_) if node_satisfies(value, &predicate) => {
                select_paths(value, &selectors[1..], current_path, results);
            }
            _ => {}
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_relative_path_tracks_parent_hops() {
        let rel = parse_relative_path("^^.slug").unwrap();
        assert_eq!(rel.up, 2);
        assert_eq!(rel.selectors.len(), 1);
    }

    fn parse_yaml(input: &str) -> YamlValue {
        serde_yaml::from_str(input).unwrap()
    }

    #[test]
    fn wildcard_selector_returns_all_nodes() {
        let raw = parse_yaml(
            r#"
sites:
  - slug: a
    devices:
      - name: d1
      - name: d2
  - slug: b
    devices:
      - name: d3
"#,
        );
        let selectors = parse_selector_path("/sites/*/devices/*").unwrap();
        let mut selected = Vec::new();
        select_paths(&raw, &selectors, &mut Vec::new(), &mut selected);
        assert_eq!(selected.len(), 3);
    }

    /// Resolve a selector path through BOTH traversals and assert they agree,
    /// returning the matched values. Used by every predicate test below so the
    /// "select_values and select_paths behave identically" requirement is
    /// checked on each case rather than asserted once.
    fn select_agreeing<'a>(raw: &'a YamlValue, path: &str) -> Vec<&'a YamlValue> {
        let selectors = parse_selector_path(path).unwrap();
        let mut direct = Vec::new();
        select_values(raw, &selectors, &mut direct);
        let mut paths = Vec::new();
        select_paths(raw, &selectors, &mut Vec::new(), &mut paths);
        let via_paths: Vec<&YamlValue> = paths
            .iter()
            .map(|path| value_at_path(raw, path).unwrap())
            .collect();
        assert_eq!(direct, via_paths, "traversals disagree for `{path}`");
        direct
    }

    fn names(values: &[&YamlValue]) -> Vec<String> {
        values
            .iter()
            .map(|value| {
                value
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    fn predicate(field: &str, op: PredicateOp, value: &str) -> SelectorToken {
        SelectorToken::Predicate(Predicate {
            field: field.to_string(),
            op,
            value: value.to_string(),
        })
    }

    #[test]
    fn parses_predicate_forms() {
        assert_eq!(
            parse_selector_path("/devices[role=leaf]").unwrap(),
            vec![
                SelectorToken::Key("devices".to_string()),
                predicate("role", PredicateOp::Eq, "leaf"),
            ],
        );
        assert_eq!(
            parse_selector_path("/*[role=leaf]").unwrap(),
            vec![
                SelectorToken::Wildcard,
                predicate("role", PredicateOp::Eq, "leaf")
            ],
        );
        assert_eq!(
            parse_selector_path("/[role=leaf]").unwrap(),
            vec![predicate("role", PredicateOp::Eq, "leaf")],
        );
        assert_eq!(
            parse_selector_path("/devices[a=x][b=y]").unwrap(),
            vec![
                SelectorToken::Key("devices".to_string()),
                predicate("a", PredicateOp::Eq, "x"),
                predicate("b", PredicateOp::Eq, "y"),
            ],
        );
        assert_eq!(
            parse_selector_path("/[role!=leaf]").unwrap(),
            vec![predicate("role", PredicateOp::Ne, "leaf")],
        );
        assert_eq!(
            parse_selector_path("/devices[primary_ip]").unwrap(),
            vec![
                SelectorToken::Key("devices".to_string()),
                predicate("primary_ip", PredicateOp::Exists, ""),
            ],
        );
        assert_eq!(
            parse_selector_path("/devices[!primary_ip]").unwrap(),
            vec![
                SelectorToken::Key("devices".to_string()),
                predicate("primary_ip", PredicateOp::NotExists, ""),
            ],
        );
    }

    #[test]
    fn rejects_malformed_predicates() {
        let unterminated = parse_selector_path("/devices[role=leaf").unwrap_err();
        assert!(unterminated.to_string().contains("unterminated"));

        let empty_field = parse_selector_path("/devices[=leaf]").unwrap_err();
        assert!(empty_field.to_string().contains("empty field name"));

        // `[role]` is now a valid existence predicate, but a predicate with no
        // field name (`[]` or `[!]`) is still malformed.
        let empty_exists = parse_selector_path("/devices[]").unwrap_err();
        assert!(empty_exists.to_string().contains("empty field name"));

        let empty_not_exists = parse_selector_path("/devices[!]").unwrap_err();
        assert!(empty_not_exists.to_string().contains("empty field name"));
    }

    #[test]
    fn predicate_filters_sequence_elements() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    role: leaf
  - name: b
    role: spine
  - name: c
    role: leaf
"#,
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[role=leaf]")),
            vec!["a", "c"]
        );
    }

    #[test]
    fn predicate_guards_mapping_node() {
        let raw = parse_yaml(
            r#"
device:
  name: a
  role: leaf
"#,
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/device[role=leaf]")),
            vec!["a"]
        );
        assert!(select_agreeing(&raw, "/device[role=spine]").is_empty());
    }

    #[test]
    fn chained_predicates_are_anded() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    role: leaf
    vendor: cisco
  - name: b
    role: leaf
    vendor: juniper
  - name: c
    role: spine
    vendor: cisco
"#,
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[role=leaf][vendor=cisco]")),
            vec!["a"],
        );
    }

    #[test]
    fn not_equals_filters_sequence() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    role: leaf
  - name: b
    role: spine
"#,
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[role!=leaf]")),
            vec!["b"]
        );
    }

    #[test]
    fn missing_field_never_matches() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    role: leaf
  - name: b
"#,
        );
        // `=` skips the element that lacks the field.
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[role=leaf]")),
            vec!["a"]
        );
        // `!=` requires the field to be present, so the field-less element is excluded too.
        assert!(select_agreeing(&raw, "/devices[role!=leaf]").is_empty());
    }

    #[test]
    fn non_scalar_field_never_matches() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    role:
      - leaf
  - name: b
    role:
      kind: leaf
"#,
        );
        assert!(select_agreeing(&raw, "/devices[role=leaf]").is_empty());
        assert!(select_agreeing(&raw, "/devices[role!=leaf]").is_empty());
    }

    #[test]
    fn existence_predicate_keeps_present_non_null_fields() {
        let raw = parse_yaml(
            r#"
devices:
  - name: scalar
    primary_ip: 10.0.0.1
  - name: absent
  - name: null_value
    primary_ip: null
  - name: empty_list
    primary_ip: []
  - name: empty_map
    primary_ip: {}
  - name: empty_string
    primary_ip: ""
  - name: false_value
    primary_ip: false
  - name: zero
    primary_ip: 0
"#,
        );
        // `[field]` keeps every element whose `primary_ip` is present and
        // non-null, whatever the value's type: scalars, sequences, mappings,
        // empty strings, `false`, and `0` all count -- only absent or null fail.
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[primary_ip]")),
            vec![
                "scalar",
                "empty_list",
                "empty_map",
                "empty_string",
                "false_value",
                "zero",
            ],
        );
        // `[!field]` is the exact complement: absent OR null.
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[!primary_ip]")),
            vec!["absent", "null_value"],
        );
    }

    #[test]
    fn existence_predicate_guards_mapping_node() {
        let raw = parse_yaml(
            r#"
device:
  name: a
  primary_ip: 10.0.0.1
"#,
        );
        // `[field]` keeps the mapping when the field is present; `[!field]` does not.
        assert_eq!(
            names(&select_agreeing(&raw, "/device[primary_ip]")),
            vec!["a"]
        );
        assert!(select_agreeing(&raw, "/device[!primary_ip]").is_empty());

        // When the mapping lacks the field, the guards swap.
        let without = parse_yaml(
            r#"
device:
  name: a
"#,
        );
        assert!(select_agreeing(&without, "/device[primary_ip]").is_empty());
        assert_eq!(
            names(&select_agreeing(&without, "/device[!primary_ip]")),
            vec!["a"]
        );
    }

    #[test]
    fn existence_predicate_chains_with_value_predicates() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    role: leaf
    primary_ip: 10.0.0.1
  - name: b
    role: leaf
  - name: c
    role: spine
    primary_ip: 10.0.0.2
"#,
        );
        // role=leaf AND primary_ip present -> only a.
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[role=leaf][primary_ip]")),
            vec!["a"],
        );
        // role=leaf AND primary_ip absent -> only b.
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[role=leaf][!primary_ip]")),
            vec!["b"],
        );
    }

    #[test]
    fn number_and_bool_values_match_natural_text() {
        let raw = parse_yaml(
            r#"
devices:
  - name: a
    asn: 65001
    enabled: true
  - name: b
    asn: 65002
    enabled: false
"#,
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[asn=65001]")),
            vec!["a"]
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[enabled=true]")),
            vec!["a"]
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/devices[enabled=false]")),
            vec!["b"]
        );
    }

    #[test]
    fn predicate_value_may_contain_slash() {
        let raw = parse_yaml(
            r#"
prefixes:
  - name: a
    prefix: 10.0.0.0/24
  - name: b
    prefix: 10.0.1.0/24
"#,
        );
        // The '/' inside [...] must not start a new segment.
        let selectors = parse_selector_path("/prefixes/*[prefix=10.0.0.0/24]").unwrap();
        assert_eq!(selectors.len(), 3);
        assert_eq!(
            names(&select_agreeing(&raw, "/prefixes/*[prefix=10.0.0.0/24]")),
            vec!["a"],
        );
    }

    #[test]
    fn wildcard_then_predicate_combo() {
        let raw = parse_yaml(
            r#"
sites:
  - slug: s1
    devices:
      - name: a
        role: leaf
      - name: b
        role: spine
  - slug: s2
    devices:
      - name: c
        role: leaf
"#,
        );
        assert_eq!(
            names(&select_agreeing(&raw, "/sites/*/devices/*[role=leaf]")),
            vec!["a", "c"],
        );
    }

    #[test]
    fn relative_path_splitting_is_bracket_aware() {
        let rel = parse_relative_path(".prefixes/*[prefix=10.0.0.0/24]").unwrap();
        // Key(prefixes), Wildcard, Predicate -- not split on the CIDR '/'.
        assert_eq!(rel.selectors.len(), 3);
        assert_eq!(rel.up, 0);
    }
}
