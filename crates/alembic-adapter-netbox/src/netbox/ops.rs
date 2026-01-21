use super::mapping::{
    build_tag_inputs, device_status_to_str, interface_type_from_str, map_custom_fields,
    map_custom_fields_patch, map_tags, patched_interface_type_from_str,
    patched_site_status_from_str, should_skip_op, site_status_from_str, status_value_to_str,
};
use super::state::{resolved_from_state, state_mappings};
use super::NetBoxAdapter;
use alembic_core::{
    Attrs, DeviceAttrs, InterfaceAttrs, IpAddressAttrs, Kind, PrefixAttrs, SiteAttrs, Uid,
};
use alembic_engine::{
    Adapter, AppliedOp, ApplyReport, ObservedObject, ObservedState, Op, ProjectedObject,
    ProjectionData, StateStore,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use netbox::{BulkDelete, BulkUpdate, QueryBuilder};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize)]
struct IpAddressPatchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dns_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assigned_object_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assigned_object_id: Option<i32>,
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

#[async_trait]
impl Adapter for NetBoxAdapter {
    async fn observe(&self, kinds: &[Kind]) -> Result<ObservedState> {
        let mut state = ObservedState::default();
        let unique: BTreeSet<Kind> = kinds.iter().cloned().collect();

        let mappings = {
            let state_guard = self.state_guard()?;
            state_mappings(&state_guard)
        };
        let site_id_to_uid = mappings.site_id_to_uid;
        let device_id_to_uid = mappings.device_id_to_uid;
        let interface_id_to_uid = mappings.interface_id_to_uid;

        for kind in unique {
            if kind.is_custom() {
                continue;
            }
            match kind {
                Kind::DcimSite => {
                    let sites = self
                        .client
                        .list_all(&self.client.dcim().sites(), None)
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
                    let devices = self
                        .client
                        .list_all(&self.client.dcim().devices(), None)
                        .await?;
                    for device in devices {
                        let backend_id = device.id.map(|id| id as u64);
                        let name = device
                            .name
                            .flatten()
                            .or_else(|| device.id.map(|id| id.to_string()))
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
                        .client
                        .list_all(&self.client.dcim().interfaces(), None)
                        .await?;
                    for interface in interfaces {
                        let backend_id = interface.id.map(|id| id as u64);
                        let device_name = interface
                            .device
                            .name
                            .flatten()
                            .or_else(|| interface.device.id.map(|id| id.to_string()))
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
                    let prefixes = self
                        .client
                        .list_all(&self.client.ipam().prefixes(), None)
                        .await?;
                    for prefix in prefixes {
                        let backend_id = prefix.id.map(|id| id as u64);
                        let site_uid = match (
                            prefix.scope_type.as_ref().and_then(|scope| scope.as_ref()),
                            prefix.scope_id.flatten(),
                        ) {
                            (Some(scope), Some(id)) if scope == "dcim.site" => {
                                site_id_to_uid.get(&(id as u64)).copied()
                            }
                            _ => None,
                        };
                        let key = format!("prefix={}", prefix.prefix);
                        let attrs = Attrs::Prefix(PrefixAttrs {
                            prefix: prefix.prefix,
                            site: site_uid,
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
                        .client
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

        state.capabilities = self.client.fetch_capabilities().await?;

        Ok(state)
    }

    async fn apply(&self, ops: &[Op]) -> Result<ApplyReport> {
        let mut applied = Vec::new();
        let mut resolved = {
            let state_guard = self.state_guard()?;
            resolved_from_state(&state_guard)
        };
        let mut pending: Option<PendingOp> = None;

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
                    resolved.insert(*uid, id);
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
        match self.state_guard() {
            Ok(mut guard) => {
                *guard = state.clone();
            }
            Err(err) => {
                eprintln!("warning: {err}");
            }
        }
    }
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
                    let request = self
                        .build_ip_address_update_request(attrs, resolved)
                        .await?;
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

    async fn build_ip_address_update_request(
        &self,
        attrs: &IpAddressAttrs,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<IpAddressPatchRequest> {
        let (assigned_type, assigned_id) = match attrs.assigned_interface {
            Some(interface_uid) => {
                let id = self.resolve_interface_id(interface_uid, resolved).await?;
                (Some("dcim.interface".to_string()), Some(id as i32))
            }
            None => (None, None),
        };
        Ok(IpAddressPatchRequest {
            address: Some(attrs.address.clone()),
            status: None,
            dns_name: None,
            description: attrs.description.clone(),
            assigned_object_type: assigned_type,
            assigned_object_id: assigned_id,
        })
    }

    async fn resolve_site_id(&self, uid: Uid, resolved: &BTreeMap<Uid, u64>) -> Result<u64> {
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }
        Err(anyhow!("missing site backend id for uid {uid}"))
    }

    async fn resolve_device_id(&self, uid: Uid, resolved: &BTreeMap<Uid, u64>) -> Result<u64> {
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }
        Err(anyhow!("missing device backend id for uid {uid}"))
    }

    async fn resolve_interface_id(&self, uid: Uid, resolved: &BTreeMap<Uid, u64>) -> Result<u64> {
        if let Some(id) = resolved.get(&uid) {
            return Ok(*id);
        }
        Err(anyhow!("missing interface backend id for uid {uid}"))
    }

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
