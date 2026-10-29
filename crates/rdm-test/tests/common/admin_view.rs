//! A node-admin's view, served over Node RPC from a fixed set of nodes: the entry the probe's cells
//! give `rdm-rpc-probe` (`--admin-birth`). It answers `fabric.get` and `node.get [mesh: m]` on op
//! `0x23` (Q285: `Get { Fabrics }`, `Get { Nodes }`, each one data frame) from that set, as an admin
//! answers both from its own view, and records every read it decodes. It
//! serves no HTTP: the probe reads nothing but Node RPC.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MeshNode, NodeId, PathName};
use rdm_transport::{ServedBirth, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::filter::Entity;
use rdm_core::read::{Read, ReadReply, ReadRequest};
use rdm_core::views::{FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// The stub admin's own `path.name`.
pub const ADMIN: &str = "mesh1.admin.9";

/// One node of a served view: `endpoint`, `addr` and `incarnation` are `None` for a node the admin holds
/// and that is not born yet.
pub fn node(name: &str, node_id: &NodeId, endpoint: Option<String>, addr: Option<SocketAddr>, incarnation: Option<IncarnationId>) -> NodeView {
    let name: PathName = name.parse().unwrap_or_else(|e| panic!("{name} is no path.name: {e:?}"));
    NodeView { cert: None, executable: None, storage_persistence: None, topology_storage_provider: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
        kind: name.kind,
        mesh: name.mesh.clone(),
        name,
        node_id: node_id.clone(),
        endpoint_id: endpoint,
        incarnation_id: incarnation,
        deployment_id: None,
        provider: Some(ProviderKind::Process),
        data_dir: None,
        status: NodeStatus::Ready,
        is_primary: false,
        is_fabric_primary: false,
        admin_api_base: None,
        transport_addr: addr,
        listeners: Vec::new(),
        declared: None,
        stopped: false,
        load: None,
        gossip: None,
        tags: Default::default(),
        config: Default::default(),
        data_dir_lost: None,
        data_dir_unreadable: None,
    }
}

/// The served admin: its birth (what `--admin-birth` names), the reads it decoded, in order.
pub struct AdminView {
    pub birth: MeshNode,
    pub seen: Arc<Mutex<Vec<ReadRequest>>>,
    router: Router,
}

impl AdminView {
    /// The `--admin-birth` argument: the birth as the JSON an admin prints on its birth line.
    pub fn birth_arg(&self) -> String {
        serde_json::to_string(&self.birth).expect("a birth serializes")
    }

    /// Close the stub's endpoint.
    pub async fn shutdown(self) {
        let _ = self.router.shutdown().await;
    }
}

/// Serve `nodes` (and the stub admin itself, in mesh1) as an admin's view, on a fresh endpoint.
pub async fn serve(nodes: Vec<NodeView>) -> AdminView {
    let key = iroh::SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let mut me = node(ADMIN, &node_id, Some(key.public().to_string()), Some(addr), Some(incarnation.clone()));
    me.is_primary = true;
    me.is_fabric_primary = true;
    let mut held = vec![me];
    held.extend(nodes);
    held.sort_by(|a, b| a.name.cmp(&b.name));
    let mut meshes: Vec<MeshView> = Vec::new();
    for n in &held {
        match meshes.iter_mut().find(|m| m.name == n.mesh) {
            Some(m) => m.nodes.push(n.name.clone()),
            None => meshes.push(MeshView { renewals: None, id: None, name: n.mesh.clone(), status: ScopeStatus::Ready, primary_admin: Some(ADMIN.parse().unwrap()), admin_api_base: None, nodes: vec![n.name.clone()] }),
        }
    }
    let fabric = FabricView { next_expiry: None, topology_storage_provider: None, roots: None, ca: None, previous_topology_storage_providers: Vec::new(), id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process, fabric_primary: Some(ADMIN.parse().unwrap()), admin_api_base: None, meshes, build_id: None };
    let seen: Arc<Mutex<Vec<ReadRequest>>> = Arc::default();
    let s = seen.clone();
    let server = ServerBuilder::new()
        .serve_stream::<Read, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: ReadRequest, sink| {
            let (s, fabric, held) = (s.clone(), fabric.clone(), held.clone());
            async move {
                s.lock().unwrap().push(req.clone());
                let (read, frame) = match req {
                    ReadRequest::Get { entity: Entity::Fabrics, filter } if filter.pairs.is_empty() => ("fabric.get", ReadReply::Fabrics { fabrics: vec![fabric] }),
                    ReadRequest::Get { entity: Entity::Nodes, filter } => ("node.get", ReadReply::Nodes { nodes: held.into_iter().filter(|n| filter.matches(n)).collect() }),
                    other => return Ok(ReadReply::NotReady { reason: format!("the stub admin {ADMIN} answers fabric.get and node.get only, not {}", other.op()) }),
                };
                let rows = frame.rows() as u32;
                let Ok(mut sink) = sink.started(ReadReply::Started { read: read.into() }).await else {
                    return Ok(ReadReply::NotReady { reason: "the caller left".into() });
                };
                if sink.data(frame).await.is_err() {
                    return Ok(ReadReply::NotReady { reason: "the caller left".into() });
                }
                Ok(ReadReply::End { rows, frames: 1 })
            }
        })
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let birth = MeshNode { node_id, name: ADMIN.parse().unwrap(), endpoint_id: EndpointId(key.public().to_string()), transport_addr: addr, incarnation, supersedes: None, runtime: None };
    AdminView { birth, seen, router }
}

/// [`serve`] for a cell with no runtime of its own: the runtime serving the stub is returned with it,
/// and the cell holds it for as long as the probe runs.
pub fn serve_on_own_runtime(nodes: Vec<NodeView>) -> (tokio::runtime::Runtime, AdminView) {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let admin = rt.block_on(serve(nodes));
    (rt, admin)
}
