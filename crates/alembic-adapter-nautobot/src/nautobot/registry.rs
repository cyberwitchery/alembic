use alembic_core::TypeName;
use alembic_engine::{normalize_endpoint, pluralize};
use anyhow::{anyhow, Result};
use nautobot::models::ContentType;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub(super) struct ObjectTypeInfo {
    pub(super) type_name: TypeName,
    pub(super) endpoint: String,
    pub(super) features: BTreeSet<String>,
}

/// the routes nautobot's api root reports, per app label, each keyed by
/// [`route_key`] so a content type can be matched against a real route.
pub(super) type RouteIndex = BTreeMap<String, BTreeMap<String, String>>;

/// a route with its separators dropped, so `location-types` and the
/// `locationtypes` a model pluralizes to compare equal.
pub(super) fn route_key(value: &str) -> String {
    value
        .chars()
        .filter(|c| *c != '-' && *c != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

/// pick the app's real route for a content type. morphology only proposes
/// candidates here; the route itself is always one nautobot reported.
fn match_route(routes: &BTreeMap<String, String>, model: &str, derived: &str) -> Option<String> {
    [pluralize(model), pluralize(derived), model.to_string()]
        .iter()
        .find_map(|candidate| routes.get(&route_key(candidate)).cloned())
}

#[derive(Debug, Clone, Default)]
pub(super) struct ObjectTypeRegistry {
    by_type: BTreeMap<String, ObjectTypeInfo>,
    by_endpoint: BTreeMap<String, String>,
}

impl ObjectTypeRegistry {
    pub(super) fn from_content_types(types: Vec<ContentType>, routes: &RouteIndex) -> Result<Self> {
        let mut registry = ObjectTypeRegistry::default();
        for content_type in types {
            let app_label = content_type.app_label;
            let model = content_type.model;

            let endpoint_type = format!("{}.{}", app_label, model);
            let endpoint_name = if let Some(display) = content_type.display {
                if let Some((_, name)) = display.split_once('|') {
                    name.trim().replace(' ', "-")
                } else {
                    model.clone()
                }
            } else {
                model.clone()
            };
            // an unlisted app, or a content type no route matches, keeps the
            // derived spelling.
            let route = routes
                .get(&app_label)
                .and_then(|routes| match_route(routes, &model, &endpoint_name))
                .unwrap_or_else(|| pluralize(&endpoint_name));
            let endpoint = format!("{}/{}/", app_label, route);

            let features: BTreeSet<String> = ["custom-fields", "tags", "local-context"]
                .iter()
                .map(|s| s.to_string())
                .collect();

            let info = ObjectTypeInfo {
                type_name: TypeName::new(endpoint_type.clone()),
                endpoint: endpoint.clone(),
                features,
            };

            registry.by_endpoint.insert(endpoint, endpoint_type.clone());
            registry.by_type.insert(endpoint_type, info);
        }

        if registry.by_type.is_empty() {
            return Err(anyhow!("nautobot returned no content types"));
        }

        Ok(registry)
    }

    pub(super) fn info_for(&self, type_name: &TypeName) -> Option<ObjectTypeInfo> {
        self.by_type.get(type_name.as_str()).cloned()
    }

    pub(super) fn type_names(&self) -> Vec<TypeName> {
        self.by_type
            .values()
            .map(|info| info.type_name.clone())
            .collect()
    }

    pub(super) fn type_name_for_endpoint(&self, endpoint: &str) -> Option<&str> {
        let normalized = normalize_endpoint(endpoint, |s| {
            s.chars().all(|c| c.is_ascii_digit()) || uuid::Uuid::parse_str(s).is_ok()
        })?;
        self.by_endpoint.get(&normalized).map(|name| name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautobot::models::ContentType;

    fn content_type(app_label: &str, model: &str, display: Option<&str>) -> ContentType {
        ContentType {
            app_label: app_label.to_string(),
            model: model.to_string(),
            display: display.map(str::to_string),
            ..Default::default()
        }
    }

    fn routes(app_label: &str, routes: &[&str]) -> RouteIndex {
        RouteIndex::from([(
            app_label.to_string(),
            routes
                .iter()
                .map(|route| (route_key(route), route.to_string()))
                .collect(),
        )])
    }

    #[test]
    fn registry_from_content_types() {
        let types = vec![
            content_type("dcim", "device", Some("dcim | device")),
            content_type("dcim", "locationtype", Some("dcim | location type")),
            content_type("dcim", "devicebay", Some("dcim | device bay")),
        ];

        let registry = ObjectTypeRegistry::from_content_types(
            types,
            &routes("dcim", &["devices", "location-types", "device-bays"]),
        )
        .unwrap();
        let device = registry.info_for(&TypeName::new("dcim.device")).unwrap();
        assert_eq!(device.endpoint, "dcim/devices/");

        let loc_type = registry
            .info_for(&TypeName::new("dcim.locationtype"))
            .unwrap();
        assert_eq!(loc_type.endpoint, "dcim/location-types/");

        let device_bay = registry.info_for(&TypeName::new("dcim.devicebay")).unwrap();
        assert_eq!(device_bay.endpoint, "dcim/device-bays/");
    }

    /// an unspaced model carries no word boundary to pluralize, so only a real
    /// route spells it. both faces: the read path and the url a ref resolves by.
    #[test]
    fn an_unspaced_model_takes_the_real_route() {
        let registry = ObjectTypeRegistry::from_content_types(
            vec![content_type("dcim", "locationtype", None)],
            &routes("dcim", &["devices", "location-types"]),
        )
        .unwrap();

        let info = registry
            .info_for(&TypeName::new("dcim.locationtype"))
            .unwrap();
        assert_eq!(info.endpoint, "dcim/location-types/");
        assert_eq!(
            registry.type_name_for_endpoint(
                "https://nautobot.example.com/api/dcim/location-types/11111111-1111-1111-1111-111111111111/"
            ),
            Some("dcim.locationtype")
        );
    }

    #[test]
    fn route_key_ignores_separators_and_case() {
        assert_eq!(route_key("location-types"), route_key("locationtypes"));
        // nautobot spells acronyms in caps in `display`.
        assert_eq!(route_key("VLAN-groups"), route_key("vlangroups"));
    }

    /// display is a human string; the model is the identifier, so it goes first.
    #[test]
    fn the_model_is_matched_ahead_of_the_display_name() {
        let dcim = routes("dcim", &["location-types", "sites"]);
        let dcim = dcim.get("dcim").unwrap();
        assert_eq!(
            match_route(dcim, "locationtype", "site"),
            Some("location-types".to_string())
        );
    }

    /// nautobot registers `users.objectpermission` under its verbose name, so
    /// only the display string reaches that route.
    #[test]
    fn a_route_named_for_the_verbose_name_matches_through_display() {
        let users = routes("users", &["permissions", "tokens"]);
        let users = users.get("users").unwrap();
        assert_eq!(
            match_route(users, "objectpermission", "permission"),
            Some("permissions".to_string())
        );
        assert_eq!(
            match_route(users, "objectpermission", "objectpermission"),
            None
        );
    }

    #[test]
    fn an_already_singular_route_matches_the_bare_model() {
        let registry = ObjectTypeRegistry::from_content_types(
            vec![content_type("ipam", "ipaddresstointerface", None)],
            &routes("ipam", &["ip-addresses", "ip-address-to-interface"]),
        )
        .unwrap();

        let info = registry
            .info_for(&TypeName::new("ipam.ipaddresstointerface"))
            .unwrap();
        assert_eq!(info.endpoint, "ipam/ip-address-to-interface/");
    }

    #[test]
    fn a_type_no_route_matches_keeps_the_derived_endpoint() {
        let registry = ObjectTypeRegistry::from_content_types(
            vec![
                // an app the api root never listed.
                content_type("auth", "group", Some("auth | group")),
                // an app it listed, holding no route for this type.
                content_type("dcim", "site", Some("dcim | site")),
            ],
            &routes("dcim", &["devices", "locations"]),
        )
        .unwrap();

        let group = registry.info_for(&TypeName::new("auth.group")).unwrap();
        assert_eq!(group.endpoint, "auth/groups/");
        let site = registry.info_for(&TypeName::new("dcim.site")).unwrap();
        assert_eq!(site.endpoint, "dcim/sites/");
    }

    #[test]
    fn an_empty_route_index_derives_every_endpoint() {
        let registry = ObjectTypeRegistry::from_content_types(
            vec![
                content_type("dcim", "locationtype", None),
                content_type("ipam", "prefix", Some("ipam | prefix")),
            ],
            &RouteIndex::new(),
        )
        .unwrap();

        assert_eq!(
            registry
                .info_for(&TypeName::new("dcim.locationtype"))
                .unwrap()
                .endpoint,
            "dcim/locationtypes/"
        );
        assert_eq!(
            registry
                .info_for(&TypeName::new("ipam.prefix"))
                .unwrap()
                .endpoint,
            "ipam/prefixes/"
        );
    }
}
