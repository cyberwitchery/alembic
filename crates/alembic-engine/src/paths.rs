use anyhow::{anyhow, Result};
use serde_yaml::Value as YamlValue;

#[derive(Debug, Clone)]
pub(crate) enum SelectorToken {
    Key(String),
    Index(usize),
    Wildcard,
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
    for segment in path.trim_start_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }
        tokens.push(parse_selector_segment(segment)?);
    }
    Ok(tokens)
}

fn parse_selector_segment(segment: &str) -> Result<SelectorToken> {
    if segment == "*" {
        return Ok(SelectorToken::Wildcard);
    }
    if let Ok(index) = segment.parse::<usize>() {
        return Ok(SelectorToken::Index(index));
    }
    Ok(SelectorToken::Key(segment.to_string()))
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
    let selectors = if rest.is_empty() {
        Vec::new()
    } else {
        rest.split('/')
            .filter(|s| !s.is_empty())
            .map(parse_selector_segment)
            .collect::<Result<Vec<_>>>()?
    };
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
}
