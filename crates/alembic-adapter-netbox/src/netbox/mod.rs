//! netbox adapter implementation.

mod client;
mod mapping;
mod ops;
mod state;

use alembic_engine::{MissingCustomField, StateStore};
use anyhow::{anyhow, Result};
use std::collections::BTreeSet;
use std::sync::MutexGuard;

#[cfg(test)]
use alembic_engine::Adapter;
#[cfg(test)]
use std::collections::BTreeMap;

use client::NetBoxClient;
use mapping::*;

/// netbox adapter that maps ir objects to netbox api calls.
pub struct NetBoxAdapter {
    client: NetBoxClient,
    state: std::sync::Mutex<StateStore>,
}

impl NetBoxAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str, state: StateStore) -> Result<Self> {
        let client = NetBoxClient::new(url, token)?;
        Ok(Self {
            client,
            state: std::sync::Mutex::new(state),
        })
    }

    pub async fn create_custom_fields(&self, missing: &[MissingCustomField]) -> Result<()> {
        let grouped = group_custom_fields(missing);
        for (field, entry) in grouped {
            let request = netbox::extras::CreateCustomFieldRequest {
                object_types: entry.object_types.into_iter().collect(),
                r#type: entry.field_type,
                name: field.clone(),
                related_object_type: None,
                label: Some(field),
                group_name: None,
                description: None,
                required: None,
                unique: None,
                search_weight: None,
                filter_logic: None,
                ui_visible: None,
                ui_editable: None,
                is_cloneable: None,
                default: None,
                related_object_filter: None,
                weight: None,
                validation_minimum: None,
                validation_maximum: None,
                validation_regex: None,
                choice_set: None,
                comments: None,
            };
            let _ = self
                .client
                .extras()
                .custom_fields()
                .create(&request)
                .await?;
        }
        Ok(())
    }

    pub async fn create_tags(&self, tags: &[String]) -> Result<()> {
        let unique: BTreeSet<String> = tags.iter().cloned().collect();
        for tag in unique {
            let request = netbox::extras::CreateTagRequest {
                name: tag.clone(),
                slug: slugify(&tag),
                color: None,
                description: None,
                weight: None,
                object_types: None,
            };
            let _ = self.client.extras().tags().create(&request).await?;
        }
        Ok(())
    }

    fn state_guard(&self) -> Result<MutexGuard<'_, StateStore>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("state lock poisoned"))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn site_status_mapping() {
        let status = site_status_from_str("active").unwrap();
        assert!(matches!(
            status,
            netbox::models::writable_site_request::Status::Active
        ));
    }

    #[test]
    fn patched_site_status_mapping() {
        let status = patched_site_status_from_str("retired").unwrap();
        assert!(matches!(
            status,
            netbox::models::patched_writable_site_request::Status::Retired
        ));
    }

    #[test]
    fn interface_type_mapping() {
        let value = interface_type_from_str(Some("1000base-t")).unwrap();
        assert!(matches!(
            value,
            netbox::models::writable_interface_request::RHashType::Variant1000baseT
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{
        Attrs, DeviceAttrs, InterfaceAttrs, IpAddressAttrs, Kind, PrefixAttrs, SiteAttrs, Uid,
    };
    use alembic_engine::{Op, ProjectedObject, ProjectionData};
    use httpmock::Method::{DELETE, GET, PATCH, POST};
    use httpmock::{Mock, MockServer};
    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    fn projected(base: alembic_core::Object) -> ProjectedObject {
        ProjectedObject {
            base,
            projection: ProjectionData::default(),
        }
    }

    fn page(results: serde_json::Value) -> serde_json::Value {
        json!({
            "count": results.as_array().map(|a| a.len()).unwrap_or(0),
            "next": null,
            "previous": null,
            "results": results
        })
    }

    fn device_payload(id: i32, name: &str, site_id: i32) -> serde_json::Value {
        json!({
            "id": id,
            "name": name,
            "device_type": {
                "manufacturer": { "name": "acme", "slug": "acme" },
                "model": "leaf-switch",
                "slug": "leaf-switch"
            },
            "role": { "name": "leaf", "slug": "leaf" },
            "site": { "id": site_id, "name": "FRA1", "slug": "fra1" }
        })
    }

    fn interface_payload(id: i32, device_id: i32) -> serde_json::Value {
        json!({
            "id": id,
            "device": { "id": device_id, "name": "leaf01" },
            "name": "eth0",
            "type": { "value": "1000base-t" },
            "enabled": true
        })
    }

    fn site_payload(id: i32) -> serde_json::Value {
        json!({
            "id": id,
            "name": "FRA1",
            "slug": "fra1",
            "status": { "value": "active" }
        })
    }

    fn prefix_payload(id: i32) -> serde_json::Value {
        json!({
            "id": id,
            "prefix": "10.0.0.0/24",
            "description": "subnet",
            "scope_type": "dcim.site",
            "scope_id": 1
        })
    }

    fn ip_payload(id: i32, interface_id: i64) -> serde_json::Value {
        json!({
            "id": id,
            "address": "10.0.0.10/24",
            "assigned_object_type": "dcim.interface",
            "assigned_object_id": interface_id,
            "description": "leaf01 eth0"
        })
    }

    fn state_with_mappings(path: &std::path::Path) -> StateStore {
        let mut store = StateStore::load(path).unwrap();
        store.set_backend_id(Kind::DcimSite, uid(1), 1);
        store.set_backend_id(Kind::DcimDevice, uid(2), 2);
        store.set_backend_id(Kind::DcimInterface, uid(3), 3);
        store
    }

    fn mock_list<'a>(
        server: &'a MockServer,
        path: &'a str,
        payload: serde_json::Value,
    ) -> Mock<'a> {
        server.mock(|when, then| {
            when.method(GET)
                .path(path)
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(page(payload));
        })
    }

    #[tokio::test]
    async fn observe_maps_state_and_attrs() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = state_with_mappings(&dir.path().join("state.json"));
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _sites = mock_list(&server, "/api/dcim/sites/", json!([site_payload(1)]));
        let _devices = mock_list(
            &server,
            "/api/dcim/devices/",
            json!([device_payload(2, "leaf01", 1)]),
        );
        let _interfaces = mock_list(
            &server,
            "/api/dcim/interfaces/",
            json!([interface_payload(3, 2)]),
        );
        let _prefixes = mock_list(&server, "/api/ipam/prefixes/", json!([prefix_payload(4)]));
        let _ips = mock_list(
            &server,
            "/api/ipam/ip-addresses/",
            json!([ip_payload(5, 3)]),
        );
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _tags = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200)
                .json_body(page(json!([{"id": 1, "name": "fabric", "slug": "fabric"}])));
        });

        let observed = adapter
            .observe(&[
                Kind::DcimSite,
                Kind::DcimDevice,
                Kind::DcimInterface,
                Kind::IpamPrefix,
                Kind::IpamIpAddress,
            ])
            .await
            .unwrap();

        let site = observed.by_key.get("site=fra1").unwrap();
        assert!(matches!(site.attrs, Attrs::Site(_)));

        let device = observed.by_key.get("site=fra1/device=leaf01").unwrap();
        match &device.attrs {
            Attrs::Device(attrs) => {
                assert_eq!(attrs.name, "leaf01");
                assert_eq!(attrs.site, uid(1));
            }
            _ => panic!("expected device attrs"),
        }

        let interface = observed.by_key.get("device=leaf01/interface=eth0").unwrap();
        match &interface.attrs {
            Attrs::Interface(attrs) => {
                assert_eq!(attrs.device, uid(2));
                assert_eq!(attrs.if_type.as_deref(), Some("1000base-t"));
            }
            _ => panic!("expected interface attrs"),
        }

        let prefix = observed.by_key.get("prefix=10.0.0.0/24").unwrap();
        match &prefix.attrs {
            Attrs::Prefix(attrs) => {
                assert_eq!(attrs.site, Some(uid(1)));
            }
            _ => panic!("expected prefix attrs"),
        }

        let ip = observed.by_key.get("ip=10.0.0.10/24").unwrap();
        match &ip.attrs {
            Attrs::IpAddress(attrs) => {
                assert_eq!(attrs.assigned_interface, Some(uid(3)));
            }
            _ => panic!("expected ip attrs"),
        }
        assert!(observed.capabilities.tags.contains("fabric"));
    }

    #[tokio::test]
    async fn apply_create_update_delete_flow() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _role = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/device-roles/")
                .query_param("name", "leaf");
            then.status(200)
                .json_body(page(json!([{"id": 10, "name": "leaf", "slug": "leaf"}])));
        });
        let _dtype = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/device-types/")
                .query_param("model", "leaf-switch");
            then.status(200).json_body(page(json!([{
                "id": 20,
                "manufacturer": { "name": "acme", "slug": "acme" },
                "model": "leaf-switch",
                "slug": "leaf-switch"
            }])));
        });
        let _device_get = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/devices/2/");
            then.status(200).json_body(device_payload(2, "leaf01", 1));
        });
        let _site_create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201).json_body(site_payload(1));
        });
        let _device_create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/devices/");
            then.status(201).json_body(device_payload(2, "leaf01", 1));
        });
        let _interface_create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/interfaces/");
            then.status(201).json_body(interface_payload(3, 2));
        });
        let _prefix_create = server.mock(|when, then| {
            when.method(POST).path("/api/ipam/prefixes/");
            then.status(201).json_body(prefix_payload(4));
        });
        let _ip_create = server.mock(|when, then| {
            when.method(POST).path("/api/ipam/ip-addresses/");
            then.status(201).json_body(ip_payload(5, 3));
        });

        let ops = vec![
            Op::Create {
                uid: uid(1),
                kind: Kind::DcimSite,
                desired: projected(
                    alembic_core::Object::new(
                        uid(1),
                        "site=fra1".to_string(),
                        Attrs::Site(SiteAttrs {
                            name: "FRA1".to_string(),
                            slug: "fra1".to_string(),
                            status: Some("active".to_string()),
                            description: None,
                        }),
                    )
                    .unwrap(),
                ),
            },
            Op::Create {
                uid: uid(2),
                kind: Kind::DcimDevice,
                desired: projected(
                    alembic_core::Object::new(
                        uid(2),
                        "site=fra1/device=leaf01".to_string(),
                        Attrs::Device(DeviceAttrs {
                            name: "leaf01".to_string(),
                            site: uid(1),
                            role: "leaf".to_string(),
                            device_type: "leaf-switch".to_string(),
                            status: Some("active".to_string()),
                        }),
                    )
                    .unwrap(),
                ),
            },
            Op::Create {
                uid: uid(3),
                kind: Kind::DcimInterface,
                desired: projected(
                    alembic_core::Object::new(
                        uid(3),
                        "device=leaf01/interface=eth0".to_string(),
                        Attrs::Interface(InterfaceAttrs {
                            name: "eth0".to_string(),
                            device: uid(2),
                            if_type: Some("1000base-t".to_string()),
                            enabled: Some(true),
                            description: None,
                        }),
                    )
                    .unwrap(),
                ),
            },
            Op::Create {
                uid: uid(4),
                kind: Kind::IpamPrefix,
                desired: projected(
                    alembic_core::Object::new(
                        uid(4),
                        "prefix=10.0.0.0/24".to_string(),
                        Attrs::Prefix(PrefixAttrs {
                            prefix: "10.0.0.0/24".to_string(),
                            site: Some(uid(1)),
                            description: Some("subnet".to_string()),
                        }),
                    )
                    .unwrap(),
                ),
            },
            Op::Create {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                desired: projected(
                    alembic_core::Object::new(
                        uid(5),
                        "ip=10.0.0.10/24".to_string(),
                        Attrs::IpAddress(IpAddressAttrs {
                            address: "10.0.0.10/24".to_string(),
                            assigned_interface: Some(uid(3)),
                            description: Some("leaf01 eth0".to_string()),
                        }),
                    )
                    .unwrap(),
                ),
            },
        ];

        let report = adapter.apply(&ops).await.unwrap();
        assert_eq!(report.applied.len(), ops.len());
    }

    #[tokio::test]
    async fn create_tags_posts_unique_names() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _tags = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(201)
                .json_body(json!({"id": 1, "name": "fabric", "slug": "fabric"}));
        });

        adapter
            .create_tags(&["fabric".to_string(), "fabric".to_string()])
            .await
            .unwrap();
    }

    #[test]
    fn slugify_normalizes_value() {
        assert_eq!(slugify("EVPN Fabric"), "evpn-fabric");
        assert_eq!(slugify("edge--core"), "edge-core");
    }

    #[test]
    fn build_tag_inputs_uses_slugify() {
        let tags = vec!["EVPN Fabric".to_string()];
        let inputs = build_tag_inputs(&tags);
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "EVPN Fabric");
        assert_eq!(inputs[0].slug, "evpn-fabric");
    }

    #[test]
    fn map_custom_fields_patch_clones_values() {
        let mut fields = BTreeMap::new();
        fields.insert("fabric".to_string(), json!("fra1"));
        let mapped = map_custom_fields_patch(&fields);
        assert_eq!(mapped.get("fabric"), Some(&json!("fra1")));
    }

    #[tokio::test]
    async fn apply_update_with_lookups() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
        state.set_backend_id(Kind::DcimSite, uid(1), 1);
        state.set_backend_id(Kind::DcimDevice, uid(2), 2);
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _site_lookup = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/sites/")
                .query_param("slug", "fra1");
            then.status(200).json_body(page(json!([site_payload(1)])));
        });
        let _device_lookup = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/devices/")
                .query_param("name", "leaf01");
            then.status(200)
                .json_body(page(json!([device_payload(2, "leaf01", 1)])));
        });
        let _interface_lookup = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/interfaces/")
                .query_param("device_id", "2")
                .query_param("name", "eth0");
            then.status(200)
                .json_body(page(json!([interface_payload(3, 2)])));
        });
        let _prefix_lookup = server.mock(|when, then| {
            when.method(GET)
                .path("/api/ipam/prefixes/")
                .query_param("prefix", "10.0.0.0/24");
            then.status(200).json_body(page(json!([prefix_payload(4)])));
        });
        let _ip_lookup = server.mock(|when, then| {
            when.method(GET)
                .path("/api/ipam/ip-addresses/")
                .query_param("address", "10.0.0.10/24");
            then.status(200).json_body(page(json!([ip_payload(5, 3)])));
        });
        let _role = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/device-roles/")
                .query_param("name", "leaf");
            then.status(200)
                .json_body(page(json!([{"id": 10, "name": "leaf", "slug": "leaf"}])));
        });
        let _dtype = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/device-types/")
                .query_param("model", "leaf-switch");
            then.status(200).json_body(page(json!([{
                "id": 20,
                "manufacturer": { "name": "acme", "slug": "acme" },
                "model": "leaf-switch",
                "slug": "leaf-switch"
            }])));
        });
        let _device_get = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/devices/2/");
            then.status(200).json_body(device_payload(2, "leaf01", 1));
        });
        let _site_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/dcim/sites/");
            then.status(200).json_body(json!([site_payload(1)]));
        });
        let _device_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/dcim/devices/");
            then.status(200)
                .json_body(json!([device_payload(2, "leaf01", 1)]));
        });
        let _interface_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/dcim/interfaces/");
            then.status(200).json_body(json!([interface_payload(3, 2)]));
        });
        let _prefix_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/ipam/prefixes/");
            then.status(200).json_body(json!([prefix_payload(4)]));
        });
        let _ip_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/ipam/ip-addresses/");
            then.status(200).json_body(json!([ip_payload(5, 3)]));
        });
        let _ip_delete = server.mock(|when, then| {
            when.method(DELETE).path("/api/ipam/ip-addresses/");
            then.status(204);
        });

        let ops = vec![
            Op::Update {
                uid: uid(1),
                kind: Kind::DcimSite,
                desired: projected(
                    alembic_core::Object::new(
                        uid(1),
                        "site=fra1".to_string(),
                        Attrs::Site(SiteAttrs {
                            name: "FRA1".to_string(),
                            slug: "fra1".to_string(),
                            status: Some("active".to_string()),
                            description: None,
                        }),
                    )
                    .unwrap(),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(2),
                kind: Kind::DcimDevice,
                desired: projected(
                    alembic_core::Object::new(
                        uid(2),
                        "site=fra1/device=leaf01".to_string(),
                        Attrs::Device(DeviceAttrs {
                            name: "leaf01".to_string(),
                            site: uid(1),
                            role: "leaf".to_string(),
                            device_type: "leaf-switch".to_string(),
                            status: Some("active".to_string()),
                        }),
                    )
                    .unwrap(),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(3),
                kind: Kind::DcimInterface,
                desired: projected(
                    alembic_core::Object::new(
                        uid(3),
                        "device=leaf01/interface=eth0".to_string(),
                        Attrs::Interface(InterfaceAttrs {
                            name: "eth0".to_string(),
                            device: uid(2),
                            if_type: Some("1000base-t".to_string()),
                            enabled: Some(true),
                            description: None,
                        }),
                    )
                    .unwrap(),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(4),
                kind: Kind::IpamPrefix,
                desired: projected(
                    alembic_core::Object::new(
                        uid(4),
                        "prefix=10.0.0.0/24".to_string(),
                        Attrs::Prefix(PrefixAttrs {
                            prefix: "10.0.0.0/24".to_string(),
                            site: Some(uid(1)),
                            description: Some("subnet".to_string()),
                        }),
                    )
                    .unwrap(),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                desired: projected(
                    alembic_core::Object::new(
                        uid(5),
                        "ip=10.0.0.10/24".to_string(),
                        Attrs::IpAddress(IpAddressAttrs {
                            address: "10.0.0.10/24".to_string(),
                            assigned_interface: Some(uid(3)),
                            description: Some("leaf01 eth0".to_string()),
                        }),
                    )
                    .unwrap(),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Delete {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                key: "ip=10.0.0.10/24".to_string(),
                backend_id: Some(5),
            },
        ];

        let report = adapter.apply(&ops).await.unwrap();
        assert_eq!(report.applied.len(), ops.len());
    }
}
