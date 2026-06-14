//! field predicates for `map` selectors, e.g. the `[attrs.role=leaf]` tail of a
//! `match` pattern. (the raw-yaml path traversal that used to live here went away
//! with the raw->ir mapping engine; only the predicate parser remains, consumed
//! by `transform`.)

use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PredicateOp {
    Eq,
    Ne,
    Exists,
    NotExists,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Predicate {
    pub(crate) field: String,
    pub(crate) op: PredicateOp,
    pub(crate) value: String,
}

/// parse one or more `[field op value]` predicates from the start of `rest`
/// (e.g. `[role=leaf][vendor=cisco]`). Chained predicates are ANDed by the caller.
pub(crate) fn parse_predicates(mut rest: &str) -> Result<Vec<Predicate>> {
    let mut predicates = Vec::new();
    while !rest.is_empty() {
        if !rest.starts_with('[') {
            return Err(anyhow!("unexpected text after predicate: '{rest}'"));
        }
        let Some(end) = rest.find(']') else {
            return Err(anyhow!("unterminated '[' in selector"));
        };
        predicates.push(parse_predicate(&rest[1..end])?);
        rest = &rest[end + 1..];
    }
    Ok(predicates)
}

/// parse the inside of a predicate. With an `=`, it is a value predicate
/// (`field=value`, or `field!=value` when `!` immediately precedes the `=`) and
/// the value is the literal remainder up to the closing `]`. With no `=`, it is
/// an existence predicate: a bare `field` is `Exists`, and a leading `!`
/// (`!field`) is `NotExists`. An empty field name is an error in every form.
fn parse_predicate(inner: &str) -> Result<Predicate> {
    let Some(eq) = inner.find('=') else {
        // no `=`: an existence predicate. A leading `!` negates it.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn predicate(field: &str, op: PredicateOp, value: &str) -> Predicate {
        Predicate {
            field: field.to_string(),
            op,
            value: value.to_string(),
        }
    }

    #[test]
    fn parses_predicate_forms() {
        assert_eq!(
            parse_predicates("[role=leaf]").unwrap(),
            vec![predicate("role", PredicateOp::Eq, "leaf")],
        );
        assert_eq!(
            parse_predicates("[a=x][b=y]").unwrap(),
            vec![
                predicate("a", PredicateOp::Eq, "x"),
                predicate("b", PredicateOp::Eq, "y"),
            ],
        );
        assert_eq!(
            parse_predicates("[role!=leaf]").unwrap(),
            vec![predicate("role", PredicateOp::Ne, "leaf")],
        );
        assert_eq!(
            parse_predicates("[primary_ip]").unwrap(),
            vec![predicate("primary_ip", PredicateOp::Exists, "")],
        );
        assert_eq!(
            parse_predicates("[!primary_ip]").unwrap(),
            vec![predicate("primary_ip", PredicateOp::NotExists, "")],
        );
        // values may contain '/' (e.g. a cidr)
        assert_eq!(
            parse_predicates("[prefix=10.0.0.0/24]").unwrap(),
            vec![predicate("prefix", PredicateOp::Eq, "10.0.0.0/24")],
        );
    }

    #[test]
    fn rejects_malformed_predicates() {
        assert!(parse_predicates("[role=leaf")
            .unwrap_err()
            .to_string()
            .contains("unterminated"));
        assert!(parse_predicates("[=leaf]")
            .unwrap_err()
            .to_string()
            .contains("empty field name"));
        assert!(parse_predicates("[]")
            .unwrap_err()
            .to_string()
            .contains("empty field name"));
        assert!(parse_predicates("[!]")
            .unwrap_err()
            .to_string()
            .contains("empty field name"));
    }
}
