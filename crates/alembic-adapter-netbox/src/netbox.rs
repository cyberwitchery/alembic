//! netbox adapter implementation.

use alembic_core::{
    Attrs, DeviceAttrs, InterfaceAttrs, IpAddressAttrs, Kind, PrefixAttrs, SiteAttrs, Uid,
};
use alembic_engine::{
    Adapter, AppliedOp, ApplyReport, BackendCapabilities, MissingCustomField, ObservedObject,
    ObservedState, Op, ProjectedObject, ProjectionData, StateStore,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use netbox::{BulkDelete, BulkUpdate, Client, ClientConfig, QueryBuilder};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// netbox adapter that maps ir objects to netbox api calls.
pub struct NetBoxAdapter {
    client: Client,
    state: std::sync::Mutex<StateStore>,
}

struct PendingUpdate {
    uid: Uid,
    backend_id: u64,
    desired: ProjectedObject,
}

struct PendingDelete {
    uid: Uid,
    backend_id: u64,
}

enum PendingOp {
    Update {
        kind: Kind,
        items: Vec<PendingUpdate>,
    },
    Delete {
        kind: Kind,
        items: Vec<PendingDelete>,
    },
}

impl NetBoxAdapter {
    async fn flush_pending(
        &self,
        pending: &mut Option<PendingOp>,
        resolved: &BTreeMap<Uid, u64>,
        applied: &mut Vec<AppliedOp>,
    ) -> Result<()> {
        let Some(batch) = pending.take() else {
            return Ok(());
        };

        match batch {
            PendingOp::Update { kind, items } => {
                self.bulk_update_objects(&kind, &items, resolved).await?;
                for item in items {
                    self.apply_projection_patch(&kind, item.backend_id, &item.desired.projection)
                        .await?;
                    applied.push(AppliedOp {
                        uid: item.uid,
                        kind: kind.clone(),
                        backend_id: Some(item.backend_id),
                    });
                }
            }
            PendingOp::Delete { kind, items } => {
                self.bulk_delete_objects(&kind, &items).await?;
                for item in items {
                    applied.push(AppliedOp {
                        uid: item.uid,
                        kind: kind.clone(),
                        backend_id: None,
                    });
                }
            }
        }

        Ok(())
    }

    async fn bulk_update_objects(
        &self,
        kind: &Kind,
        items: &[PendingUpdate],
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<()> {
        match kind {
            Kind::DcimSite => {
                let mut batch = Vec::with_capacity(items.len());
                for item in items {
                    let attrs = match &item.desired.base.attrs {
                        Attrs::Site(attrs) => attrs,
                        _ => return Err(anyhow!("expected site attrs for bulk update")),
                    };
                    let request = self.build_site_patch_request(attrs)?;
                    batch.push(BulkUpdate::new(item.backend_id, request));
                }
                self.client.dcim().sites().bulk_patch(&batch).await?;
            }
            Kind::DcimDevice => {
                let mut batch = Vec::with_capacity(items.len());
                for item in items {
                    let attrs = match &item.desired.base.attrs {
                        Attrs::Device(attrs) => attrs,
                        _ => return Err(anyhow!("expected device attrs for bulk update")),
                    };
                    let request = self.build_device_update_request(attrs, resolved).await?;
                    batch.push(BulkUpdate::new(item.backend_id, request));
                }
                self.client.dcim().devices().bulk_patch(&batch).await?;
            }
            Kind::DcimInterface => {
                let mut batch = Vec::with_capacity(items.len());
                for item in items {
                    let attrs = match &item.desired.base.attrs {
                        Attrs::Interface(attrs) => attrs,
                        _ => return Err(anyhow!("expected interface attrs for bulk update")),
                    };
                    let request = self.build_interface_patch_request(attrs, resolved).await?;
                    batch.push(BulkUpdate::new(item.backend_id, request));
                }
                self.client.dcim().interfaces().bulk_patch(&batch).await?;
            }
            Kind::IpamPrefix => {
                let mut batch = Vec::with_capacity(items.len());
                for item in items {
                    let attrs = match &item.desired.base.attrs {
                        Attrs::Prefix(attrs) => attrs,
                        _ => return Err(anyhow!("expected prefix attrs for bulk update")),
                    };
                    let request = self.build_prefix_update_request(attrs, resolved).await?;
                    batch.push(BulkUpdate::new(item.backend_id, request));
                }
                self.client.ipam().prefixes().bulk_patch(&batch).await?;
            }
            Kind::IpamIpAddress => {
                let mut batch = Vec::with_capacity(items.len());
                for item in items {
                    let attrs = match &item.desired.base.attrs {
                        Attrs::IpAddress(attrs) => attrs,
                        _ => return Err(anyhow!("expected ip address attrs for bulk update")),
                    };
                    let request = self.build_ip_address_update_request(attrs)?;
                    batch.push(BulkUpdate::new(item.backend_id, request));
                }
                self.client.ipam().ip_addresses().bulk_patch(&batch).await?;
            }
            Kind::Custom(_) => return Err(anyhow!("generic kinds are not supported")),
        }

        Ok(())
    }

    async fn bulk_delete_objects(&self, kind: &Kind, items: &[PendingDelete]) -> Result<()> {
        let batch: Vec<BulkDelete> = items
            .iter()
            .map(|item| BulkDelete::new(item.backend_id))
            .collect();
        match kind {
            Kind::DcimSite => self.client.dcim().sites().bulk_delete(&batch).await?,
            Kind::DcimDevice => self.client.dcim().devices().bulk_delete(&batch).await?,
            Kind::DcimInterface => self.client.dcim().interfaces().bulk_delete(&batch).await?,
            Kind::IpamPrefix => self.client.ipam().prefixes().bulk_delete(&batch).await?,
            Kind::IpamIpAddress => {
                self.client
                    .ipam()
                    .ip_addresses()
                    .bulk_delete(&batch)
                    .await?
            }
            Kind::Custom(_) => return Err(anyhow!("generic kinds are not supported")),
        }

        Ok(())
    }

    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str, state: StateStore) -> Result<Self> {
        let config = ClientConfig::new(url, token);
        let client = Client::new(config)?;
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

        let mappings = {
            let state_guard = self.state.lock().unwrap();
            state_guard.all_mappings().clone()
        };
        for (_kind, mapping) in mappings {
            match _kind {
                Kind::DcimSite => {
                    for (uid, backend_id) in mapping {
                        site_id_to_uid.insert(backend_id, uid);
                    }
                }
                Kind::DcimDevice => {
                    for (uid, backend_id) in mapping {
                        device_id_to_uid.insert(backend_id, uid);
                    }
                }
                Kind::DcimInterface => {
                    for (uid, backend_id) in mapping {
                        interface_id_to_uid.insert(backend_id, uid);
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
                        let projection = ProjectionData {
                            custom_fields: map_custom_fields(site.custom_fields),
                            tags: map_tags(site.tags),
                            local_context: None,
                        };
                        state.insert(ObservedObject {
                            kind: kind.clone(),
                            key,
                            attrs,
                            projection,
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
                        let projection = ProjectionData {
                            custom_fields: map_custom_fields(device.custom_fields),
                            tags: map_tags(device.tags),
                            local_context: device.local_context_data.flatten(),
                        };
                        state.insert(ObservedObject {
                            kind: kind.clone(),
                            key,
                            attrs,
                            projection,
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
                        let projection = ProjectionData {
                            custom_fields: map_custom_fields(interface.custom_fields),
                            tags: map_tags(interface.tags),
                            local_context: None,
                        };
                        state.insert(ObservedObject {
                            kind: kind.clone(),
                            key,
                            attrs,
                            projection,
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
                        let projection = ProjectionData {
                            custom_fields: map_custom_fields(prefix.custom_fields),
                            tags: map_tags(prefix.tags),
                            local_context: None,
                        };
                        state.insert(ObservedObject {
                            kind: kind.clone(),
                            key,
                            attrs,
                            projection,
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
                        let projection = ProjectionData {
                            custom_fields: map_custom_fields(ip.custom_fields),
                            tags: map_tags(ip.tags),
                            local_context: None,
                        };
                        state.insert(ObservedObject {
                            kind: kind.clone(),
                            key,
                            attrs,
                            projection,
                            backend_id,
                        });
                    }
                }
                Kind::Custom(_) => {}
            }
        }

        state.capabilities = fetch_capabilities(&self.client).await?;

        Ok(state)
    }

    async fn apply(&self, ops: &[Op]) -> Result<ApplyReport> {
        let mut applied = Vec::new();
        let mut resolved = BTreeMap::new();
        let mut pending: Option<PendingOp> = None;

        let mappings = {
            let state_guard = self.state.lock().unwrap();
            state_guard.all_mappings().clone()
        };
        for mapping in mappings.values() {
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
                    self.flush_pending(&mut pending, &resolved, &mut applied)
                        .await?;
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
                            &desired.base,
                            &resolved,
                        )
                        .await?;
                    match &mut pending {
                        Some(PendingOp::Update {
                            kind: pending_kind,
                            items,
                        }) if pending_kind == kind => {
                            items.push(PendingUpdate {
                                uid: *uid,
                                backend_id: id,
                                desired: desired.clone(),
                            });
                        }
                        _ => {
                            self.flush_pending(&mut pending, &resolved, &mut applied)
                                .await?;
                            pending = Some(PendingOp::Update {
                                kind: kind.clone(),
                                items: vec![PendingUpdate {
                                    uid: *uid,
                                    backend_id: id,
                                    desired: desired.clone(),
                                }],
                            });
                        }
                    }
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
                    match &mut pending {
                        Some(PendingOp::Delete {
                            kind: pending_kind,
                            items,
                        }) if pending_kind == kind => {
                            items.push(PendingDelete {
                                uid: *uid,
                                backend_id: id,
                            });
                        }
                        _ => {
                            self.flush_pending(&mut pending, &resolved, &mut applied)
                                .await?;
                            pending = Some(PendingOp::Delete {
                                kind: kind.clone(),
                                items: vec![PendingDelete {
                                    uid: *uid,
                                    backend_id: id,
                                }],
                            });
                        }
                    }
                }
            }
        }

        self.flush_pending(&mut pending, &resolved, &mut applied)
            .await?;

        Ok(ApplyReport { applied })
    }

    fn update_state(&self, state: &StateStore) {
        let mut guard = self.state.lock().unwrap();
        *guard = state.clone();
    }
}

impl NetBoxAdapter {
    /// create a backend object from the desired ir.
    async fn create_object(
        &self,
        kind: Kind,
        desired: &ProjectedObject,
        resolved: &mut BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        let backend_id = match &desired.base.attrs {
            Attrs::Site(attrs) => self.create_site(attrs).await,
            Attrs::Device(attrs) => self.create_device(attrs, resolved).await,
            Attrs::Interface(attrs) => self.create_interface(attrs, resolved).await,
            Attrs::Prefix(attrs) => self.create_prefix(attrs, resolved).await,
            Attrs::IpAddress(attrs) => self.create_ip_address(attrs, resolved).await,
            Attrs::Generic(_) => return Err(anyhow!("generic attrs are not supported")),
        }
        .with_context(|| format!("create {}", kind))?;
        self.apply_projection_patch(&kind, backend_id, &desired.projection)
            .await?;
        Ok(backend_id)
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

    fn build_site_patch_request(
        &self,
        attrs: &SiteAttrs,
    ) -> Result<netbox::models::PatchedWritableSiteRequest> {
        let mut request = netbox::models::PatchedWritableSiteRequest::new();
        request.name = Some(attrs.name.clone());
        request.slug = Some(attrs.slug.clone());
        if let Some(status) = &attrs.status {
            request.status = Some(patched_site_status_from_str(status)?);
        }
        request.description = attrs.description.clone();
        Ok(request)
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

    async fn build_device_update_request(
        &self,
        attrs: &DeviceAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<netbox::dcim::UpdateDeviceRequest> {
        let site_id = self.resolve_site_id(attrs.site, resolved).await?;
        let role_id = self.lookup_device_role_id(&attrs.role).await?;
        let device_type_id = self.lookup_device_type_id(&attrs.device_type).await?;

        Ok(netbox::dcim::UpdateDeviceRequest {
            name: Some(attrs.name.clone()),
            device_type: Some(device_type_id as i32),
            role: Some(role_id as i32),
            site: Some(site_id as i32),
            status: attrs.status.clone(),
            serial: None,
            asset_tag: None,
        })
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

    async fn build_interface_patch_request(
        &self,
        attrs: &InterfaceAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<netbox::models::PatchedWritableInterfaceRequest> {
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

        Ok(request)
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

    async fn build_prefix_update_request(
        &self,
        attrs: &PrefixAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<netbox::ipam::UpdatePrefixRequest> {
        let site_id = match attrs.site {
            Some(site_uid) => Some(self.resolve_site_id(site_uid, resolved).await?),
            None => None,
        };

        Ok(netbox::ipam::UpdatePrefixRequest {
            prefix: Some(attrs.prefix.clone()),
            site: site_id.map(|id| id as i32),
            status: None,
            description: attrs.description.clone(),
        })
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

    fn build_ip_address_update_request(
        &self,
        attrs: &IpAddressAttrs,
    ) -> Result<netbox::ipam::UpdateIpAddressRequest> {
        Ok(netbox::ipam::UpdateIpAddressRequest {
            address: Some(attrs.address.clone()),
            status: None,
            dns_name: None,
            description: attrs.description.clone(),
        })
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

    async fn apply_projection_patch(
        &self,
        kind: &Kind,
        backend_id: u64,
        projection: &ProjectionData,
    ) -> Result<()> {
        if projection.custom_fields.is_none()
            && projection.tags.is_none()
            && projection.local_context.is_none()
        {
            return Ok(());
        }
        if projection.local_context.is_some() && !matches!(kind, Kind::DcimDevice) {
            return Err(anyhow!(
                "local context projection is only supported for dcim.device"
            ));
        }

        let tags = projection
            .tags
            .as_ref()
            .map(|items| build_tag_inputs(items));
        let custom_fields = projection
            .custom_fields
            .as_ref()
            .map(map_custom_fields_patch);

        match kind {
            Kind::DcimSite => {
                let request = netbox::dcim::PatchSiteFieldsRequest {
                    custom_fields,
                    tags,
                };
                self.client
                    .dcim()
                    .sites()
                    .patch(backend_id, &request)
                    .await?;
            }
            Kind::DcimDevice => {
                let request = netbox::dcim::PatchDeviceFieldsRequest {
                    custom_fields,
                    tags,
                    local_context_data: projection.local_context.clone(),
                };
                self.client
                    .dcim()
                    .devices()
                    .patch(backend_id, &request)
                    .await?;
            }
            Kind::DcimInterface => {
                let request = netbox::dcim::PatchInterfaceFieldsRequest {
                    custom_fields,
                    tags,
                };
                self.client
                    .dcim()
                    .interfaces()
                    .patch(backend_id, &request)
                    .await?;
            }
            Kind::IpamPrefix => {
                let request = netbox::ipam::PatchPrefixFieldsRequest {
                    custom_fields,
                    tags,
                };
                self.client
                    .ipam()
                    .prefixes()
                    .patch(backend_id, &request)
                    .await?;
            }
            Kind::IpamIpAddress => {
                let request = netbox::ipam::PatchIpAddressFieldsRequest {
                    custom_fields,
                    tags,
                };
                self.client
                    .ipam()
                    .ip_addresses()
                    .patch(backend_id, &request)
                    .await?;
            }
            Kind::Custom(_) => {}
        }

        Ok(())
    }
}

fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in input.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            out.push(lower);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

fn build_tag_inputs(tags: &[String]) -> Vec<netbox::models::NestedTag> {
    tags.iter()
        .map(|tag| netbox::models::NestedTag::new(tag.clone(), slugify(tag)))
        .collect()
}

fn map_custom_fields_patch(fields: &BTreeMap<String, Value>) -> HashMap<String, Value> {
    fields
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn map_custom_fields(fields: Option<HashMap<String, Value>>) -> Option<BTreeMap<String, Value>> {
    fields.map(|map| map.into_iter().collect())
}

fn map_tags(tags: Option<Vec<netbox::models::NestedTag>>) -> Option<Vec<String>> {
    tags.map(|items| {
        let mut tags: Vec<String> = items.into_iter().map(|tag| tag.name).collect();
        tags.sort();
        tags
    })
}

async fn fetch_capabilities(client: &Client) -> Result<BackendCapabilities> {
    let fields = client.extras().custom_fields().list(None).await?;
    let mut by_kind: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for field in fields.results {
        for object_type in field.object_types {
            by_kind
                .entry(object_type)
                .or_default()
                .insert(field.name.clone());
        }
    }
    let mut tags = BTreeSet::new();
    let mut offset = 0usize;
    let limit = 200usize;
    loop {
        let page = client
            .extras()
            .tags()
            .list(Some(QueryBuilder::default().limit(limit).offset(offset)))
            .await?;
        let page_count = page.results.len();
        for tag in page.results {
            tags.insert(tag.name);
        }
        if tags.len() >= page.count || page_count == 0 {
            break;
        }
        offset += limit;
    }
    Ok(BackendCapabilities {
        custom_fields_by_kind: by_kind,
        tags,
    })
}

#[derive(Default)]
struct CustomFieldProposal {
    object_types: BTreeSet<String>,
    field_type: String,
}

fn group_custom_fields(missing: &[MissingCustomField]) -> BTreeMap<String, CustomFieldProposal> {
    let mut grouped: BTreeMap<String, CustomFieldProposal> = BTreeMap::new();
    for entry in missing {
        let proposal = grouped.entry(entry.field.clone()).or_default();
        proposal.object_types.insert(entry.kind.clone());
        let entry_type = custom_field_type(&entry.sample);
        proposal.field_type = merge_field_type(&proposal.field_type, entry_type);
    }
    grouped
}

fn custom_field_type(value: &Value) -> String {
    match value {
        Value::String(_) => "text".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                "integer".to_string()
            } else {
                "decimal".to_string()
            }
        }
        Value::Array(_) | Value::Object(_) => "json".to_string(),
        Value::Null => "text".to_string(),
    }
}

fn merge_field_type(current: &str, incoming: String) -> String {
    if current.is_empty() {
        return incoming;
    }
    if current == "json" || incoming == "json" {
        return "json".to_string();
    }
    if current == "text" || incoming == "text" {
        return "text".to_string();
    }
    if current == "decimal" || incoming == "decimal" {
        return "decimal".to_string();
    }
    if current == "boolean" || incoming == "boolean" {
        return "boolean".to_string();
    }
    "integer".to_string()
}

fn should_skip_op(op: &Op) -> bool {
    match op {
        Op::Create { kind, desired, .. } => {
            kind.is_custom() || matches!(desired.base.attrs, Attrs::Generic(_))
        }
        Op::Update { kind, desired, .. } => {
            kind.is_custom() || matches!(desired.base.attrs, Attrs::Generic(_))
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
                desired: projected(alembic_core::Object::new(
                    uid(1),
                    "site=fra1".to_string(),
                    Attrs::Site(SiteAttrs {
                        name: "FRA1".to_string(),
                        slug: "fra1".to_string(),
                        status: Some("active".to_string()),
                        description: None,
                    }),
                )),
            },
            Op::Create {
                uid: uid(2),
                kind: Kind::DcimDevice,
                desired: projected(alembic_core::Object::new(
                    uid(2),
                    "site=fra1/device=leaf01".to_string(),
                    Attrs::Device(DeviceAttrs {
                        name: "leaf01".to_string(),
                        site: uid(1),
                        role: "leaf".to_string(),
                        device_type: "leaf-switch".to_string(),
                        status: Some("active".to_string()),
                    }),
                )),
            },
            Op::Create {
                uid: uid(3),
                kind: Kind::DcimInterface,
                desired: projected(alembic_core::Object::new(
                    uid(3),
                    "device=leaf01/interface=eth0".to_string(),
                    Attrs::Interface(InterfaceAttrs {
                        name: "eth0".to_string(),
                        device: uid(2),
                        if_type: Some("1000base-t".to_string()),
                        enabled: Some(true),
                        description: None,
                    }),
                )),
            },
            Op::Create {
                uid: uid(4),
                kind: Kind::IpamPrefix,
                desired: projected(alembic_core::Object::new(
                    uid(4),
                    "prefix=10.0.0.0/24".to_string(),
                    Attrs::Prefix(PrefixAttrs {
                        prefix: "10.0.0.0/24".to_string(),
                        site: Some(uid(1)),
                        description: Some("subnet".to_string()),
                    }),
                )),
            },
            Op::Create {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                desired: projected(alembic_core::Object::new(
                    uid(5),
                    "ip=10.0.0.10/24".to_string(),
                    Attrs::IpAddress(IpAddressAttrs {
                        address: "10.0.0.10/24".to_string(),
                        assigned_interface: Some(uid(3)),
                        description: Some("leaf01 eth0".to_string()),
                    }),
                )),
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
                desired: projected(alembic_core::Object::new(
                    uid(1),
                    "site=fra1".to_string(),
                    Attrs::Site(SiteAttrs {
                        name: "FRA1".to_string(),
                        slug: "fra1".to_string(),
                        status: Some("active".to_string()),
                        description: None,
                    }),
                )),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(2),
                kind: Kind::DcimDevice,
                desired: projected(alembic_core::Object::new(
                    uid(2),
                    "site=fra1/device=leaf01".to_string(),
                    Attrs::Device(DeviceAttrs {
                        name: "leaf01".to_string(),
                        site: uid(1),
                        role: "leaf".to_string(),
                        device_type: "leaf-switch".to_string(),
                        status: Some("active".to_string()),
                    }),
                )),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(3),
                kind: Kind::DcimInterface,
                desired: projected(alembic_core::Object::new(
                    uid(3),
                    "device=leaf01/interface=eth0".to_string(),
                    Attrs::Interface(InterfaceAttrs {
                        name: "eth0".to_string(),
                        device: uid(2),
                        if_type: Some("1000base-t".to_string()),
                        enabled: Some(true),
                        description: None,
                    }),
                )),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(4),
                kind: Kind::IpamPrefix,
                desired: projected(alembic_core::Object::new(
                    uid(4),
                    "prefix=10.0.0.0/24".to_string(),
                    Attrs::Prefix(PrefixAttrs {
                        prefix: "10.0.0.0/24".to_string(),
                        site: Some(uid(1)),
                        description: Some("subnet".to_string()),
                    }),
                )),
                changes: vec![],
                backend_id: None,
            },
            Op::Update {
                uid: uid(5),
                kind: Kind::IpamIpAddress,
                desired: projected(alembic_core::Object::new(
                    uid(5),
                    "ip=10.0.0.10/24".to_string(),
                    Attrs::IpAddress(IpAddressAttrs {
                        address: "10.0.0.10/24".to_string(),
                        assigned_interface: Some(uid(3)),
                        description: Some("leaf01 eth0".to_string()),
                    }),
                )),
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
