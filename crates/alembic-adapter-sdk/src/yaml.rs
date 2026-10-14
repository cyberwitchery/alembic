//! yaml in and out. every yaml file alembic reads or writes goes through here,
//! so one set of parsing rules holds for inventories, map specs, configs, plans
//! and adapter files. it lives in the sdk so an adapter gets the same rules
//! without linking the engine.

use serde::de::DeserializeOwned;
use serde::Serialize;

/// a yaml read or write failure. it carries a line and column but never the
/// source, and names no yaml library, so swapping one is not a breaking change.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(Box<dyn std::error::Error + Send + Sync>);

impl Error {
    fn new(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(source.into())
    }
}

/// parse a yaml document. only `true`/`false` are booleans, so `no` or `on`
/// stay strings, a duplicate key is an error, and errors carry a line and
/// column but never echo the source (a config can hold a token).
pub fn from_str<T: DeserializeOwned>(raw: &str) -> Result<T, Error> {
    let options = serde_saphyr::options! {
        strict_booleans: true,
        with_snippet: false,
    };
    // the multi-document reader rejects content after the first node that
    // `from_str` would silently drop, such as a dedent after an indented root.
    let mut documents =
        serde_saphyr::from_multiple_with_options(raw, options.clone()).map_err(Error::new)?;
    match documents.len() {
        0 => serde_saphyr::from_str_with_options(raw, options).map_err(Error::new),
        1 => Ok(documents.remove(0)),
        n => Err(Error::new(format!("expected one yaml document, found {n}"))),
    }
}

/// serialize a value as a yaml document.
pub fn to_string<T: Serialize>(value: &T) -> Result<String, Error> {
    serde_saphyr::to_string(value).map_err(Error::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn yaml_1_1_booleans_stay_strings() {
        let value: Value = from_str("a: no\nb: on\nc: y\nd: true\ne: False\n").unwrap();
        assert_eq!(
            value,
            json!({"a": "no", "b": "on", "c": "y", "d": true, "e": false})
        );
    }

    #[test]
    fn a_duplicate_key_is_an_error() {
        let err = from_str::<Value>("a: 1\na: 2\n").unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn content_after_the_first_node_is_an_error() {
        assert!(from_str::<Value>("  x: 1\ny: 2\n").is_err());
        assert!(from_str::<Value>("a: 1\n---\nb: 2\n").is_err());
    }

    #[test]
    fn an_empty_document_is_null() {
        assert_eq!(from_str::<Value>("# nothing\n").unwrap(), Value::Null);
    }

    #[test]
    fn errors_do_not_echo_the_source() {
        let err = from_str::<Vec<String>>("token: hunter2\n").unwrap_err();
        assert!(!format!("{err:#}").contains("hunter2"), "{err:#}");
    }

    #[test]
    fn strings_that_read_as_other_scalars_round_trip() {
        let value = json!(["no", "017", "1.0", "null", "~", "true", "", "a: b"]);
        let back: Value = from_str(&to_string(&value).unwrap()).unwrap();
        assert_eq!(back, value);
    }
}
