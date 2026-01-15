//! netbox adapter implementation.

use alembic_core::{
    Attrs, DeviceAttrs, InterfaceAttrs, IpAddressAttrs, Kind, PrefixAttrs, SiteAttrs, Uid,
};
use alembic_engine::{
    Adapter, AppliedOp, ApplyReport, ObservedObject, ObservedState, Op, StateStore,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use netbox::{Client, ClientConfig, QueryBuilder};
use std::collections::{BTreeMap, BTreeSet};

/// netbox adapter that maps ir objects to netbox api calls.
pub struct NetBoxAdapter {
    client: Client,
    state: StateStore,
}

impl NetBoxAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str, state: StateStore) -> Result<Self> {
        let config = ClientConfig::new(url, token);
        let client = Client::new(config)?;
        Ok(Self { client, state })
    }
}

#[async_trait]
impl Adapter for NetBoxAdapter {
    async fn observe(&self, kinds: &[Kind]) -> Result<ObservedState> {
        let mut state = ObservedState::default();
        let unique: BTreeSet<Kind> = kinds.iter().copied().collect();
        let mut site_id_to_uid = BTreeMap::new();
        let mut device_id_to_uid = BTreeMap::new();
        let mut interface_id_to_uid = BTreeMap::new();

        for (_kind, mapping) in self.state.all_mappings() {
            match _kind {
                Kind::DcimSite => {
                    for (uid, backend_id) in mapping {
                        site_id_to_uid.insert(*backend_id, *uid);
                    }
                }
                Kind::DcimDevice => {
                    for (uid, backend_id) in mapping {
                        device_id_to_uid.insert(*backend_id, *uid);
                    }
                }
                Kind::DcimInterface => {
                    for (uid, backend_id) in mapping {
                        interface_id_to_uid.insert(*backend_id, *uid);
                    }
                }
                _ => {}
            }
        }

        for kind in unique {
            match kind {
                Kind::DcimSite => {
                    let sites = self
                        .client
                        .dcim()
                        .sites()
                        .paginate(None)?
                        .collect_all()
                        .await?;
                    for site in sites {
                        let backend_id = site.id.map(|id| id as u64);
                        let key = format!("site={}", site.slug);
                        let attrs = Attrs::Site(SiteAttrs {
                            name: site.name,
                            slug: site.slug,
                            status: site
                                .status
                                .and_then(|status| status.value.map(status_value_to_str))
                                .map(|s| s.to_string()),
                            description: site.description,
                        });
                        state.insert(ObservedObject {
                            kind,
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::DcimDevice => {
                    let devices = self
                        .client
                        .dcim()
                        .devices()
                        .paginate(None)?
                        .collect_all()
                        .await?;
                    for device in devices {
                        let backend_id = device.id.map(|id| id as u64);
                        let name = device
                            .name
                            .flatten()
                            .unwrap_or_else(|| "unknown".to_string());
                        let site_slug = device.site.slug;
                        let key = format!("site={}/device={}", site_slug, name);
                        let site_uid = device
                            .site
                            .id
                            .map(|id| id as u64)
                            .and_then(|id| site_id_to_uid.get(&id).copied())
                            .unwrap_or_else(Uid::nil);
                        let attrs = Attrs::Device(DeviceAttrs {
                            name,
                            site: site_uid,
                            role: device.role.name,
                            device_type: device.device_type.model,
                            status: device
                                .status
                                .and_then(|status| status.value.map(device_status_to_str))
                                .map(|s| s.to_string()),
                        });
                        state.insert(ObservedObject {
                            kind,
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::DcimInterface => {
                    let interfaces = self
                        .client
                        .dcim()
                        .interfaces()
                        .paginate(None)?
                        .collect_all()
                        .await?;
                    for interface in interfaces {
                        let backend_id = interface.id.map(|id| id as u64);
                        let device_name = interface
                            .device
                            .name
                            .flatten()
                            .unwrap_or_else(|| "unknown".to_string());
                        let key = format!("device={}/interface={}", device_name, interface.name);
                        let if_type = interface
                            .r#type
                            .value
                            .and_then(|value| serde_json::to_value(value).ok())
                            .and_then(|value| value.as_str().map(|s| s.to_string()));
                        let device_uid = interface
                            .device
                            .id
                            .map(|id| id as u64)
                            .and_then(|id| device_id_to_uid.get(&id).copied())
                            .unwrap_or_else(Uid::nil);
                        let attrs = Attrs::Interface(InterfaceAttrs {
                            name: interface.name,
                            device: device_uid,
                            if_type,
                            enabled: interface.enabled,
                            description: interface.description,
                        });
                        state.insert(ObservedObject {
                            kind,
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::IpamPrefix => {
                    let prefixes = self
                        .client
                        .ipam()
                        .prefixes()
                        .paginate(None)?
                        .collect_all()
                        .await?;
                    for prefix in prefixes {
                        let backend_id = prefix.id.map(|id| id as u64);
                        let key = format!("prefix={}", prefix.prefix);
                        let attrs = Attrs::Prefix(PrefixAttrs {
                            prefix: prefix.prefix,
                            site: None,
                            description: prefix.description,
                        });
                        state.insert(ObservedObject {
                            kind,
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::IpamIpAddress => {
                    let ips = self
                        .client
                        .ipam()
                        .ip_addresses()
                        .paginate(None)?
                        .collect_all()
                        .await?;
                    for ip in ips {
                        let backend_id = ip.id.map(|id| id as u64);
                        let key = format!("ip={}", ip.address);
                        let assigned_interface = match (
                            ip.assigned_object_type.clone().flatten(),
                            ip.assigned_object_id.flatten(),
                        ) {
                            (Some(kind), Some(id)) if kind == "dcim.interface" => {
                                interface_id_to_uid.get(&(id as u64)).copied()
                            }
                            _ => None,
                        };
                        let attrs = Attrs::IpAddress(IpAddressAttrs {
                            address: ip.address,
                            assigned_interface,
                            description: ip.description,
                        });
                        state.insert(ObservedObject {
                            kind,
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
            }
        }

        Ok(state)
    }

    async fn apply(&self, ops: &[Op]) -> Result<ApplyReport> {
        let mut applied = Vec::new();
        let mut resolved = BTreeMap::new();

        for mapping in self.state.all_mappings().values() {
            for (uid, backend_id) in mapping {
                resolved.insert(*uid, *backend_id);
            }
        }

        for op in ops {
            match op {
                Op::Create { uid, kind, desired } => {
                    let backend_id = self.create_object(*kind, desired, &mut resolved).await?;
                    resolved.insert(*uid, backend_id);
                    applied.push(AppliedOp {
                        uid: *uid,
                        kind: *kind,
                        backend_id: Some(backend_id),
                    });
                }
                Op::Update {
                    uid,
                    kind,
                    desired,
                    backend_id,
                    ..
                } => {
                    let id = self
                        .resolve_backend_id(
                            *kind,
                            *uid,
                            backend_id.unwrap_or(0),
                            desired,
                            &resolved,
                        )
                        .await?;
                    self.update_object(*kind, id, desired, &resolved).await?;
                    applied.push(AppliedOp {
                        uid: *uid,
                        kind: *kind,
                        backend_id: Some(id),
                    });
                }
                Op::Delete {
                    uid,
                    kind,
                    key,
                    backend_id,
                } => {
                    let id = if let Some(id) = backend_id {
                        *id
                    } else if let Some(id) = resolved.get(uid).copied() {
                        id
                    } else {
                        return Err(anyhow!("missing backend id for delete: {}", key));
                    };
                    self.delete_object(*kind, id).await?;
                    applied.push(AppliedOp {
                        uid: *uid,
                        kind: *kind,
                        backend_id: None,
                    });
                }
            }
        }

        Ok(ApplyReport { applied })
    }
}

impl NetBoxAdapter {
    /// create a backend object from the desired ir.
    async fn create_object(
        &self,
        kind: Kind,
        desired: &alembic_core::Object,
        resolved: &mut BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        match &desired.attrs {
            Attrs::Site(attrs) => self.create_site(attrs).await,
            Attrs::Device(attrs) => self.create_device(attrs, resolved).await,
            Attrs::Interface(attrs) => self.create_interface(attrs, resolved).await,
            Attrs::Prefix(attrs) => self.create_prefix(attrs, resolved).await,
            Attrs::IpAddress(attrs) => self.create_ip_address(attrs, resolved).await,
        }
        .with_context(|| format!("create {}", kind.as_str()))
    }

    /// update a backend object to match the desired ir.
    async fn update_object(
        &self,
        kind: Kind,
        backend_id: u64,
        desired: &alembic_core::Object,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<()> {
        match &desired.attrs {
            Attrs::Site(attrs) => self.update_site(backend_id, attrs).await,
            Attrs::Device(attrs) => self.update_device(backend_id, attrs, resolved).await,
            Attrs::Interface(attrs) => self.update_interface(backend_id, attrs, resolved).await,
            Attrs::Prefix(attrs) => self.update_prefix(backend_id, attrs, resolved).await,
            Attrs::IpAddress(attrs) => self.update_ip_address(backend_id, attrs, resolved).await,
        }
        .with_context(|| format!("update {}", kind.as_str()))
    }

    /// delete a backend object by id.
    async fn delete_object(&self, kind: Kind, backend_id: u64) -> Result<()> {
        match kind {
            Kind::DcimSite => {
                self.client.dcim().sites().delete(backend_id).await?;
            }
            Kind::DcimDevice => {
                self.client.dcim().devices().delete(backend_id).await?;
            }
            Kind::DcimInterface => {
                self.client.dcim().interfaces().delete(backend_id).await?;
            }
            Kind::IpamPrefix => {
                self.client.ipam().prefixes().delete(backend_id).await?;
            }
            Kind::IpamIpAddress => {
                self.client.ipam().ip_addresses().delete(backend_id).await?;
            }
        }
        Ok(())
    }

    /// resolve a backend id via state mapping or key-based lookup.
    async fn resolve_backend_id(
        &self,
        kind: Kind,
        uid: Uid,
        backend_id: u64,
        desired: &alembic_core::Object,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        if backend_id != 0 {
            return Ok(backend_id);
        }
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }

        match &desired.attrs {
            Attrs::Site(attrs) => self.lookup_site_id(attrs).await,
            Attrs::Device(attrs) => self.lookup_device_id(attrs).await,
            Attrs::Interface(attrs) => self.lookup_interface_id(attrs, resolved).await,
            Attrs::Prefix(attrs) => self.lookup_prefix_id(attrs).await,
            Attrs::IpAddress(attrs) => self.lookup_ip_address_id(attrs).await,
        }
        .with_context(|| format!("resolve backend id for {}", kind.as_str()))
    }

    /// create a site and return its backend id.
    async fn create_site(&self, attrs: &SiteAttrs) -> Result<u64> {
        let mut request =
            netbox::models::WritableSiteRequest::new(attrs.name.clone(), attrs.slug.clone());
        if let Some(status) = &attrs.status {
            request.status = Some(site_status_from_str(status)?);
        }
        request.description = attrs.description.clone();
        let site = self.client.dcim().sites().create(&request).await?;
        site.id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("site create returned no id"))
    }

    /// update a site by backend id.
    async fn update_site(&self, backend_id: u64, attrs: &SiteAttrs) -> Result<()> {
        let mut request = netbox::models::PatchedWritableSiteRequest::new();
        request.name = Some(attrs.name.clone());
        request.slug = Some(attrs.slug.clone());
        if let Some(status) = &attrs.status {
            request.status = Some(patched_site_status_from_str(status)?);
        }
        request.description = attrs.description.clone();
        self.client
            .dcim()
            .sites()
            .patch(backend_id, &request)
            .await?;
        Ok(())
    }

    /// create a device and return its backend id.
    async fn create_device(
        &self,
        attrs: &DeviceAttrs,
        resolved: &mut BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        let site_id = self.resolve_site_id(attrs.site, resolved).await?;
        let role_id = self.lookup_device_role_id(&attrs.role).await?;
        let device_type_id = self.lookup_device_type_id(&attrs.device_type).await?;

        let request = netbox::dcim::CreateDeviceRequest {
            name: attrs.name.clone(),
            device_type: device_type_id as i32,
            role: role_id as i32,
            site: site_id as i32,
            status: attrs.status.clone(),
            serial: None,
            asset_tag: None,
            tags: None,
        };

        let device = self.client.dcim().devices().create(&request).await?;
        device
            .id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("device create returned no id"))
    }

    /// update a device by backend id.
    async fn update_device(
        &self,
        backend_id: u64,
        attrs: &DeviceAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<()> {
        let site_id = self.resolve_site_id(attrs.site, resolved).await?;
        let role_id = self.lookup_device_role_id(&attrs.role).await?;
        let device_type_id = self.lookup_device_type_id(&attrs.device_type).await?;

        let request = netbox::dcim::UpdateDeviceRequest {
            name: Some(attrs.name.clone()),
            device_type: Some(device_type_id as i32),
            role: Some(role_id as i32),
            site: Some(site_id as i32),
            status: attrs.status.clone(),
            serial: None,
            asset_tag: None,
        };

        self.client
            .dcim()
            .devices()
            .patch(backend_id, &request)
            .await?;
        Ok(())
    }

    /// create an interface and return its backend id.
    async fn create_interface(
        &self,
        attrs: &InterfaceAttrs,
        resolved: &mut BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        let device_id = self.resolve_device_id(attrs.device, resolved).await?;
        let interface_type = interface_type_from_str(attrs.if_type.as_deref())?;
        let device_name = self.device_name_by_id(device_id).await?;

        let mut device_ref = netbox::models::BriefInterfaceRequestDevice::new();
        device_ref.name = Some(Some(device_name));

        let request = netbox::models::WritableInterfaceRequest {
            device: Box::new(device_ref),
            name: attrs.name.clone(),
            r#type: interface_type,
            enabled: attrs.enabled,
            description: attrs.description.clone(),
            ..Default::default()
        };

        let interface = self.client.dcim().interfaces().create(&request).await?;
        interface
            .id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("interface create returned no id"))
    }

    /// update an interface by backend id.
    async fn update_interface(
        &self,
        backend_id: u64,
        attrs: &InterfaceAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<()> {
        let interface_type = patched_interface_type_from_str(attrs.if_type.as_deref())?;
        let mut request = netbox::models::PatchedWritableInterfaceRequest::new();
        request.name = Some(attrs.name.clone());
        request.r#type = Some(interface_type);
        request.enabled = attrs.enabled;
        request.description = attrs.description.clone();

        if let Some(device_id) = resolved.get(&attrs.device) {
            let mut device_ref = netbox::models::BriefInterfaceRequestDevice::new();
            device_ref.name = Some(Some(self.device_name_by_id(*device_id).await?));
            request.device = Some(Box::new(device_ref));
        }

        self.client
            .dcim()
            .interfaces()
            .patch(backend_id, &request)
            .await?;
        Ok(())
    }

    /// create a prefix and return its backend id.
    async fn create_prefix(
        &self,
        attrs: &PrefixAttrs,
        resolved: &mut BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        let site_id = match attrs.site {
            Some(site_uid) => Some(self.resolve_site_id(site_uid, resolved).await?),
            None => None,
        };

        let request = netbox::ipam::CreatePrefixRequest {
            prefix: attrs.prefix.clone(),
            site: site_id.map(|id| id as i32),
            vrf: None,
            tenant: None,
            vlan: None,
            status: None,
            role: None,
            is_pool: None,
            description: attrs.description.clone(),
            tags: None,
        };

        let prefix = self.client.ipam().prefixes().create(&request).await?;
        prefix
            .id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("prefix create returned no id"))
    }

    /// update a prefix by backend id.
    async fn update_prefix(
        &self,
        backend_id: u64,
        attrs: &PrefixAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<()> {
        let site_id = match attrs.site {
            Some(site_uid) => Some(self.resolve_site_id(site_uid, resolved).await?),
            None => None,
        };

        let request = netbox::ipam::UpdatePrefixRequest {
            prefix: Some(attrs.prefix.clone()),
            site: site_id.map(|id| id as i32),
            status: None,
            description: attrs.description.clone(),
        };

        self.client
            .ipam()
            .prefixes()
            .patch(backend_id, &request)
            .await?;
        Ok(())
    }

    /// create an ip address and return its backend id.
    async fn create_ip_address(
        &self,
        attrs: &IpAddressAttrs,
        resolved: &mut BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        let (assigned_type, assigned_id) = match attrs.assigned_interface {
            Some(interface_uid) => {
                let id = self.resolve_interface_id(interface_uid, resolved).await?;
                (Some("dcim.interface".to_string()), Some(id as i32))
            }
            None => (None, None),
        };

        let request = netbox::ipam::CreateIpAddressRequest {
            address: attrs.address.clone(),
            vrf: None,
            tenant: None,
            status: None,
            role: None,
            assigned_object_type: assigned_type,
            assigned_object_id: assigned_id,
            dns_name: None,
            description: attrs.description.clone(),
            tags: None,
        };

        let ip = self.client.ipam().ip_addresses().create(&request).await?;
        ip.id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("ip address create returned no id"))
    }

    /// update an ip address by backend id.
    async fn update_ip_address(
        &self,
        backend_id: u64,
        attrs: &IpAddressAttrs,
        _resolved: &BTreeMap<Uid, u64>,
    ) -> Result<()> {
        let request = netbox::ipam::UpdateIpAddressRequest {
            address: Some(attrs.address.clone()),
            status: None,
            dns_name: None,
            description: attrs.description.clone(),
        };

        self.client
            .ipam()
            .ip_addresses()
            .patch(backend_id, &request)
            .await?;
        Ok(())
    }

    /// resolve site id from state mapping.
    async fn resolve_site_id(&self, uid: Uid, resolved: &BTreeMap<Uid, u64>) -> Result<u64> {
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }
        Err(anyhow!("missing site backend id for uid {uid}"))
    }

    /// resolve device id from state mapping.
    async fn resolve_device_id(&self, uid: Uid, resolved: &BTreeMap<Uid, u64>) -> Result<u64> {
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }
        Err(anyhow!("missing device backend id for uid {uid}"))
    }

    /// resolve interface id from state mapping.
    async fn resolve_interface_id(&self, uid: Uid, resolved: &BTreeMap<Uid, u64>) -> Result<u64> {
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }
        Err(anyhow!("missing interface backend id for uid {uid}"))
    }

    /// lookup a site by slug.
    async fn lookup_site_id(&self, attrs: &SiteAttrs) -> Result<u64> {
        let query = QueryBuilder::new().filter("slug", &attrs.slug);
        let page = self.client.dcim().sites().list(Some(query)).await?;
        let site = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("site not found: {}", attrs.slug))?;
        site.id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("site lookup missing id"))
    }

    /// lookup a device by name.
    async fn lookup_device_id(&self, attrs: &DeviceAttrs) -> Result<u64> {
        let query = QueryBuilder::new().filter("name", &attrs.name);
        let page = self.client.dcim().devices().list(Some(query)).await?;
        let device = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("device not found: {}", attrs.name))?;
        device
            .id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("device lookup missing id"))
    }

    /// lookup an interface by device id + name.
    async fn lookup_interface_id(
        &self,
        attrs: &InterfaceAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        if let Some(device_id) = resolved.get(&attrs.device) {
            let query = QueryBuilder::new()
                .filter("device_id", device_id.to_string())
                .filter("name", &attrs.name);
            let page = self.client.dcim().interfaces().list(Some(query)).await?;
            let interface = page
                .results
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("interface not found: {}", attrs.name))?;
            return interface
                .id
                .map(|id| id as u64)
                .ok_or_else(|| anyhow!("interface lookup missing id"));
        }
        Err(anyhow!("missing device id for interface lookup"))
    }

    /// lookup a prefix by prefix string.
    async fn lookup_prefix_id(&self, attrs: &PrefixAttrs) -> Result<u64> {
        let query = QueryBuilder::new().filter("prefix", &attrs.prefix);
        let page = self.client.ipam().prefixes().list(Some(query)).await?;
        let prefix = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("prefix not found: {}", attrs.prefix))?;
        prefix
            .id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("prefix lookup missing id"))
    }

    /// lookup an ip address by cidr string.
    async fn lookup_ip_address_id(&self, attrs: &IpAddressAttrs) -> Result<u64> {
        let query = QueryBuilder::new().filter("address", &attrs.address);
        let page = self.client.ipam().ip_addresses().list(Some(query)).await?;
        let ip = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("ip address not found: {}", attrs.address))?;
        ip.id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("ip address lookup missing id"))
    }

    /// lookup a device role by name.
    async fn lookup_device_role_id(&self, role: &str) -> Result<u64> {
        let query = QueryBuilder::new().filter("name", role);
        let page = self.client.dcim().device_roles().list(Some(query)).await?;
        let role = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("device role not found: {}", role))?;
        role.id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("device role lookup missing id"))
    }

    /// lookup a device type by model.
    async fn lookup_device_type_id(&self, device_type: &str) -> Result<u64> {
        let query = QueryBuilder::new().filter("model", device_type);
        let page = self.client.dcim().device_types().list(Some(query)).await?;
        let device_type = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("device type not found: {}", device_type))?;
        device_type
            .id
            .map(|id| id as u64)
            .ok_or_else(|| anyhow!("device type lookup missing id"))
    }

    /// resolve a device name from id for interface requests.
    async fn device_name_by_id(&self, device_id: u64) -> Result<String> {
        let device = self.client.dcim().devices().get(device_id).await?;
        Ok(device
            .name
            .flatten()
            .unwrap_or_else(|| device_id.to_string()))
    }
}

/// map string status to netbox site status enum.
fn site_status_from_str(value: &str) -> Result<netbox::models::writable_site_request::Status> {
    match value {
        "planned" => Ok(netbox::models::writable_site_request::Status::Planned),
        "staging" => Ok(netbox::models::writable_site_request::Status::Staging),
        "active" => Ok(netbox::models::writable_site_request::Status::Active),
        "decommissioning" => Ok(netbox::models::writable_site_request::Status::Decommissioning),
        "retired" => Ok(netbox::models::writable_site_request::Status::Retired),
        _ => Err(anyhow!("unknown site status: {value}")),
    }
}

/// map string status to netbox site status enum for patch requests.
fn patched_site_status_from_str(
    value: &str,
) -> Result<netbox::models::patched_writable_site_request::Status> {
    match value {
        "planned" => Ok(netbox::models::patched_writable_site_request::Status::Planned),
        "staging" => Ok(netbox::models::patched_writable_site_request::Status::Staging),
        "active" => Ok(netbox::models::patched_writable_site_request::Status::Active),
        "decommissioning" => {
            Ok(netbox::models::patched_writable_site_request::Status::Decommissioning)
        }
        "retired" => Ok(netbox::models::patched_writable_site_request::Status::Retired),
        _ => Err(anyhow!("unknown site status: {value}")),
    }
}

/// map netbox location status enum to string.
fn status_value_to_str(value: netbox::models::location_status::Value) -> &'static str {
    match value {
        netbox::models::location_status::Value::Planned => "planned",
        netbox::models::location_status::Value::Staging => "staging",
        netbox::models::location_status::Value::Active => "active",
        netbox::models::location_status::Value::Decommissioning => "decommissioning",
        netbox::models::location_status::Value::Retired => "retired",
    }
}

/// map netbox device status enum to string.
fn device_status_to_str(value: netbox::models::device_status::Value) -> &'static str {
    match value {
        netbox::models::device_status::Value::Offline => "offline",
        netbox::models::device_status::Value::Active => "active",
        netbox::models::device_status::Value::Planned => "planned",
        netbox::models::device_status::Value::Staged => "staged",
        netbox::models::device_status::Value::Failed => "failed",
        netbox::models::device_status::Value::Inventory => "inventory",
        netbox::models::device_status::Value::Decommissioning => "decommissioning",
    }
}

/// map interface type strings to netbox create enum (subset for mvp).
fn interface_type_from_str(
    value: Option<&str>,
) -> Result<netbox::models::writable_interface_request::RHashType> {
    match value.unwrap_or("1000base-t") {
        "1000base-t" => Ok(netbox::models::writable_interface_request::RHashType::Variant1000baseT),
        "virtual" => Ok(netbox::models::writable_interface_request::RHashType::Virtual),
        "bridge" => Ok(netbox::models::writable_interface_request::RHashType::Bridge),
        "lag" => Ok(netbox::models::writable_interface_request::RHashType::Lag),
        other => Err(anyhow!("unsupported interface type: {other}")),
    }
}

/// map interface type strings to netbox patch enum (subset for mvp).
fn patched_interface_type_from_str(
    value: Option<&str>,
) -> Result<netbox::models::patched_writable_interface_request::RHashType> {
    match value.unwrap_or("1000base-t") {
        "1000base-t" => {
            Ok(netbox::models::patched_writable_interface_request::RHashType::Variant1000baseT)
        }
        "virtual" => Ok(netbox::models::patched_writable_interface_request::RHashType::Virtual),
        "bridge" => Ok(netbox::models::patched_writable_interface_request::RHashType::Bridge),
        "lag" => Ok(netbox::models::patched_writable_interface_request::RHashType::Lag),
        other => Err(anyhow!("unsupported interface type: {other}")),
    }
}
