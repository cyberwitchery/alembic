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
use serde::de::DeserializeOwned;
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

    async fn list_all<T>(
        &self,
        resource: &netbox::Resource<T>,
        query: Option<QueryBuilder>,
    ) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let base_query = query.unwrap_or_default();
        let mut results = Vec::new();
        let mut offset = 0usize;
        let limit = 200usize;

        loop {
            let page = resource
                .list(Some(base_query.clone().limit(limit).offset(offset)))
                .await?;
            let page_count = page.results.len();
            results.extend(page.results);
            if results.len() >= page.count || page_count == 0 {
                break;
            }
            offset += limit;
        }

        Ok(results)
    }
}

#[async_trait]
impl Adapter for NetBoxAdapter {
    async fn observe(&self, kinds: &[Kind]) -> Result<ObservedState> {
        let mut state = ObservedState::default();
        let unique: BTreeSet<Kind> = kinds.iter().cloned().collect();
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
            if kind.is_custom() {
                continue;
            }
            match kind {
                Kind::DcimSite => {
                    let sites = self.list_all(&self.client.dcim().sites(), None).await?;
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
                            kind: kind.clone(),
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::DcimDevice => {
                    let devices = self.list_all(&self.client.dcim().devices(), None).await?;
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
                            kind: kind.clone(),
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::DcimInterface => {
                    let interfaces = self
                        .list_all(&self.client.dcim().interfaces(), None)
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
                            kind: kind.clone(),
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::IpamPrefix => {
                    let prefixes = self.list_all(&self.client.ipam().prefixes(), None).await?;
                    for prefix in prefixes {
                        let backend_id = prefix.id.map(|id| id as u64);
                        let key = format!("prefix={}", prefix.prefix);
                        let attrs = Attrs::Prefix(PrefixAttrs {
                            prefix: prefix.prefix,
                            site: None,
                            description: prefix.description,
                        });
                        state.insert(ObservedObject {
                            kind: kind.clone(),
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::IpamIpAddress => {
                    let ips = self
                        .list_all(&self.client.ipam().ip_addresses(), None)
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
                            kind: kind.clone(),
                            key,
                            attrs,
                            backend_id,
                        });
                    }
                }
                Kind::Custom(_) => {}
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
            if should_skip_op(op) {
                eprintln!("skipping generic op: {op:?}");
                continue;
            }
            match op {
                Op::Update {
                    uid,
                    backend_id: Some(id),
                    ..
                }
                | Op::Delete {
                    uid,
                    backend_id: Some(id),
                    ..
                } => {
                    resolved.insert(*uid, *id);
                }
                _ => {}
            }
        }

        for op in ops {
            if should_skip_op(op) {
                eprintln!("skipping generic op: {op:?}");
                continue;
            }
            match op {
                Op::Create { uid, kind, desired } => {
                    let backend_id = self
                        .create_object(kind.clone(), desired, &mut resolved)
                        .await?;
                    resolved.insert(*uid, backend_id);
                    applied.push(AppliedOp {
                        uid: *uid,
                        kind: kind.clone(),
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
                            kind.clone(),
                            *uid,
                            backend_id.unwrap_or(0),
                            desired,
                            &resolved,
                        )
                        .await?;
                    self.update_object(kind.clone(), id, desired, &resolved)
                        .await?;
                    applied.push(AppliedOp {
                        uid: *uid,
                        kind: kind.clone(),
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
                    self.delete_object(kind.clone(), id).await?;
                    applied.push(AppliedOp {
                        uid: *uid,
                        kind: kind.clone(),
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
            Attrs::Generic(_) => return Err(anyhow!("generic attrs are not supported")),
        }
        .with_context(|| format!("create {}", kind))
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
            Attrs::Generic(_) => return Err(anyhow!("generic attrs are not supported")),
        }
        .with_context(|| format!("update {}", kind))
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
            Kind::Custom(_) => return Err(anyhow!("generic kinds are not supported")),
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
            Attrs::Generic(_) => return Err(anyhow!("generic attrs are not supported")),
        }
        .with_context(|| format!("resolve backend id for {}", kind))
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

fn should_skip_op(op: &Op) -> bool {
    match op {
        Op::Create { kind, desired, .. } => {
            kind.is_custom() || matches!(desired.attrs, Attrs::Generic(_))
        }
        Op::Update { kind, desired, .. } => {
            kind.is_custom() || matches!(desired.attrs, Attrs::Generic(_))
        }
        Op::Delete { kind, .. } => kind.is_custom(),
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
    use alembic_engine::Op;
    use httpmock::Method::{DELETE, GET, PATCH, POST};
    use httpmock::{Mock, MockServer};
    use serde_json::json;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
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
            "description": "subnet"
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

        let ip = observed.by_key.get("ip=10.0.0.10/24").unwrap();
        match &ip.attrs {
            Attrs::IpAddress(attrs) => {
                assert_eq!(attrs.assigned_interface, Some(uid(3)));
            }
            _ => panic!("expected ip attrs"),
        }
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
                desired: alembic_core::Object::new(
                    uid(1),
                    "site=fra1".to_string(),
                    Attrs::Site(SiteAttrs {
                        name: "FRA1".to_string(),
                        slug: "fra1".to_string(),
                        status: Some("active".to_string()),
                        description: None,
                    }),
                ),
            },
            Op::Create {
                uid: uid(2),
                kind: Kind::DcimDevice,
                desired: alembic_core::Object::new(
                    uid(2),
                    "site=fra1/device=leaf01".to_string(),
                    Attrs::Device(DeviceAttrs {
                        name: "leaf01".to_string(),
                        site: uid(1),
                        role: "leaf".to_string(),
                        device_type: "leaf-switch".to_string(),
                        status: Some("active".to_string()),
                    }),
                ),
            },
            Op::Create {
                uid: uid(3),
                kind: Kind::DcimInterface,
                desired: alembic_core::Object::new(
                    uid(3),
                    "device=leaf01/interface=eth0".to_string(),
                    Attrs::Interface(InterfaceAttrs {
                        name: "eth0".to_string(),
                        device: uid(2),
                        if_type: Some("1000base-t".to_string()),
                        enabled: Some(true),
                        description: None,
                    }),
                ),
            },
            Op::Create {
                uid: uid(4),
                kind: Kind::IpamPrefix,
                desired: alembic_core::Object::new(
                    uid(4),
                    "prefix=10.0.0.0/24".to_string(),
                    Attrs::Prefix(PrefixAttrs {
                        prefix: "10.0.0.0/24".to_string(),
                        site: Some(uid(1)),
                        description: Some("subnet".to_string()),
                    }),
                ),
            },
            Op::Create {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                desired: alembic_core::Object::new(
                    uid(5),
                    "ip=10.0.0.10/24".to_string(),
                    Attrs::IpAddress(IpAddressAttrs {
                        address: "10.0.0.10/24".to_string(),
                        assigned_interface: Some(uid(3)),
                        description: Some("leaf01 eth0".to_string()),
                    }),
                ),
            },
        ];

        let report = adapter.apply(&ops).await.unwrap();
        assert_eq!(report.applied.len(), ops.len());
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
            when.method(PATCH).path("/api/dcim/sites/1/");
            then.status(200).json_body(site_payload(1));
        });
        let _device_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/dcim/devices/2/");
            then.status(200).json_body(device_payload(2, "leaf01", 1));
        });
        let _interface_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/dcim/interfaces/3/");
            then.status(200).json_body(interface_payload(3, 2));
        });
        let _prefix_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/ipam/prefixes/4/");
            then.status(200).json_body(prefix_payload(4));
        });
        let _ip_patch = server.mock(|when, then| {
            when.method(PATCH).path("/api/ipam/ip-addresses/5/");
            then.status(200).json_body(ip_payload(5, 3));
        });
        let _ip_delete = server.mock(|when, then| {
            when.method(DELETE).path("/api/ipam/ip-addresses/5/");
            then.status(204);
        });

        let ops = vec![
            Op::Update {
                uid: uid(1),
                kind: Kind::DcimSite,
                desired: alembic_core::Object::new(
                    uid(1),
                    "site=fra1".to_string(),
                    Attrs::Site(SiteAttrs {
                        name: "FRA1".to_string(),
                        slug: "fra1".to_string(),
                        status: Some("active".to_string()),
                        description: None,
                    }),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(2),
                kind: Kind::DcimDevice,
                desired: alembic_core::Object::new(
                    uid(2),
                    "site=fra1/device=leaf01".to_string(),
                    Attrs::Device(DeviceAttrs {
                        name: "leaf01".to_string(),
                        site: uid(1),
                        role: "leaf".to_string(),
                        device_type: "leaf-switch".to_string(),
                        status: Some("active".to_string()),
                    }),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(3),
                kind: Kind::DcimInterface,
                desired: alembic_core::Object::new(
                    uid(3),
                    "device=leaf01/interface=eth0".to_string(),
                    Attrs::Interface(InterfaceAttrs {
                        name: "eth0".to_string(),
                        device: uid(2),
                        if_type: Some("1000base-t".to_string()),
                        enabled: Some(true),
                        description: None,
                    }),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(4),
                kind: Kind::IpamPrefix,
                desired: alembic_core::Object::new(
                    uid(4),
                    "prefix=10.0.0.0/24".to_string(),
                    Attrs::Prefix(PrefixAttrs {
                        prefix: "10.0.0.0/24".to_string(),
                        site: Some(uid(1)),
                        description: Some("subnet".to_string()),
                    }),
                ),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                desired: alembic_core::Object::new(
                    uid(5),
                    "ip=10.0.0.10/24".to_string(),
                    Attrs::IpAddress(IpAddressAttrs {
                        address: "10.0.0.10/24".to_string(),
                        assigned_interface: Some(uid(3)),
                        description: Some("leaf01 eth0".to_string()),
                    }),
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
