//! rest-api endpoint path normalization.

/// normalize a rest-api endpoint to its canonical `app/resource/` path, dropping
/// a trailing object-id segment when `is_object_id` matches it. netbox and
/// nautobot share this and differ only in `is_object_id` (integer vs uuid pks).
pub fn normalize_endpoint(endpoint: &str, is_object_id: impl Fn(&str) -> bool) -> Option<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut path = trimmed;
    if let Some(idx) = trimmed.find("/api/") {
        path = &trimmed[idx + 5..];
    }
    let path = path.trim_start_matches('/');
    let path = path.strip_prefix("api/").unwrap_or(path);
    let trimmed = path.trim_end_matches('/');
    let mut segments: Vec<&str> = trimmed.split('/').collect();
    if let Some(last) = segments.last().copied() {
        if !last.is_empty() && is_object_id(last) {
            segments.pop();
        }
    }
    if segments.is_empty() {
        return None;
    }
    let mut normalized = segments.join("/");
    normalized.push('/');
    Some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_digits(s: &str) -> bool {
        s.chars().all(|c| c.is_ascii_digit())
    }

    #[test]
    fn strips_scheme_host_and_api_prefix() {
        assert_eq!(
            normalize_endpoint("https://netbox.example.com/api/dcim/sites/", is_digits),
            Some("dcim/sites/".to_string())
        );
        assert_eq!(
            normalize_endpoint("http://localhost/api/dcim/devices/", is_digits),
            Some("dcim/devices/".to_string())
        );
        assert_eq!(
            normalize_endpoint("/api/ipam/prefixes/", is_digits),
            Some("ipam/prefixes/".to_string())
        );
        assert_eq!(
            normalize_endpoint("api/dcim/devices/", is_digits),
            Some("dcim/devices/".to_string())
        );
    }

    #[test]
    fn adds_trailing_slash_and_trims_whitespace() {
        assert_eq!(
            normalize_endpoint("dcim/devices", is_digits),
            Some("dcim/devices/".to_string())
        );
        assert_eq!(
            normalize_endpoint("  dcim/devices/  ", is_digits),
            Some("dcim/devices/".to_string())
        );
    }

    #[test]
    fn drops_trailing_id_only_when_predicate_matches() {
        // same input: stripped when the predicate matches the trailing segment,
        // kept when it does not.
        assert_eq!(
            normalize_endpoint("dcim/sites/5/", is_digits),
            Some("dcim/sites/".to_string())
        );
        assert_eq!(
            normalize_endpoint("dcim/sites/5/", |_| false),
            Some("dcim/sites/5/".to_string())
        );
    }

    #[test]
    fn empty_or_id_only_inputs_return_none() {
        assert_eq!(normalize_endpoint("", is_digits), None);
        assert_eq!(normalize_endpoint("   ", is_digits), None);
        // a single segment that is itself an id leaves nothing behind.
        assert_eq!(normalize_endpoint("/api/5/", is_digits), None);
    }
}
