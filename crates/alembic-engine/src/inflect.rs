//! string inflection for rest-api endpoint segments.

/// pluralize a django/drf endpoint segment, the way django's url router does.
/// shared by the netbox, nautobot, and django adapters.
pub fn pluralize(value: &str) -> String {
    if value.ends_with("address") {
        return format!("{value}es");
    }
    if let Some(stripped) = value.strip_suffix('y') {
        // consonant + y -> ies (city -> cities); vowel + y -> +s (bay -> bays)
        if !stripped.ends_with(['a', 'e', 'i', 'o', 'u']) {
            return format!("{stripped}ies");
        }
    }
    if value.ends_with('s')
        || value.ends_with('x')
        || value.ends_with('z')
        || value.ends_with("ch")
        || value.ends_with("sh")
    {
        return format!("{value}es");
    }
    format!("{value}s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pluralize_covers_every_branch() {
        // x/z/ch/sh -> +es (the cases django's stale copy got wrong)
        assert_eq!(pluralize("prefix"), "prefixes");
        assert_eq!(pluralize("box"), "boxes");
        assert_eq!(pluralize("buzz"), "buzzes");
        assert_eq!(pluralize("branch"), "branches");
        assert_eq!(pluralize("dish"), "dishes");
        // consonant + y -> ies
        assert_eq!(pluralize("city"), "cities");
        // vowel + y -> +s (django's stale copy got this wrong too)
        assert_eq!(pluralize("gateway"), "gateways");
        assert_eq!(pluralize("bay"), "bays");
        // address -> addresses, special-cased ahead of the +es rule
        assert_eq!(pluralize("address"), "addresses");
        assert_eq!(pluralize("ip-address"), "ip-addresses");
        // already ends in s -> +es
        assert_eq!(pluralize("status"), "statuses");
        // default -> +s
        assert_eq!(pluralize("device"), "devices");
        assert_eq!(pluralize("device-bay"), "device-bays");
    }
}
