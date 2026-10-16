//! A node-admin's view, served over Node RPC from a fixed set of nodes: the entry the probe's cells
//! give `rafka-rpc-probe` (`--admin-birth`). It answers `fabric.get` (Status `GetFabric`) and
//! `node.get` by mesh (Status `GetNodes { Mesh }`) from that set, as an admin answers both from its
//! own view (`rafka-node-admin-core` views.rs), and records every Status request it decodes. It
//! serves no HTTP: the probe reads nothing but Node RPC.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MeshNode, NodeId, PathName};
use rdm_transport::{ServedBirth, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::status::{NodeSelect, Status, StatusReply, StatusRequest};
use rdm_core::views::{FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// The stub admin's own `path.name`.
pub const ADMIN: &str = "mesh1.admin.9";

/// One node of a served view: `endpoint`, `addr` and `incarnation` are `None` for a node the admin holds
/// and that is not born yet.
pub fn node(name: &str, node_id: &NodeId, endpoint: Option<String>, addr: Option<SocketAddr>, incarnation: Option<IncarnationId>) -> NodeView {
    let name: PathName = name.parse().unwrap_or_else(|e| panic!("{name} is no path.name: {e:?}"));
    NodeView {
        kind: name.kind,
        mesh: name.mesh.clone(),
        name,
        node_id: node_id.clone(),
        endpoint_id: endpoint,
        incarnation_id: incarnation,
        deployment_id: None,
        provider: Some(ProviderKind::Process),
        data_dir: None,
        status: NodeStatus::ReadyForTraffic,
        is_primary: false,
        is_fabric_primary: false,
        admin_api_base: None,
        transport_addr: addr,
        listeners: Vec::new(),
        declared: None,
        parked: false,
        load: None,
        gossip: None,
        tags: Default::default(),
        config: Default::default(),
        held_running: None,
        data_dir_lost: None,
        data_dir_unreadable: None,
        os_stopped: None,
    }
}

/// The served admin: its birth (what `--admin-birth` names), the Status requests it decoded, in order.
pub struct AdminView {
    pub birth: MeshNode,
    pub seen: Arc<Mutex<Vec<StatusRequest>>>,
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
            None => meshes.push(MeshView { id: None, name: n.mesh.clone(), status: ScopeStatus::ReadyForTraffic, primary_admin: Some(ADMIN.parse().unwrap()), admin_api_base: None, nodes: vec![n.name.clone()] }),
        }
    }
    let fabric = FabricView { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process, fabric_primary: Some(ADMIN.parse().unwrap()), admin_api_base: None, meshes, build_id: None, held_running: None };
    let seen: Arc<Mutex<Vec<StatusRequest>>> = Arc::default();
    let s = seen.clone();
    let server = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let (s, fabric, held) = (s.clone(), fabric.clone(), held.clone());
            async move {
                s.lock().unwrap().push(req.clone());
                Ok(match req {
                    StatusRequest::GetFabric => StatusReply::Fabric { fabric },
                    StatusRequest::GetNodes { select: NodeSelect::Mesh(mesh) } => StatusReply::Nodes { nodes: held.into_iter().filter(|n| n.mesh == mesh).collect() },
                    other => StatusReply::NotReady { reason: format!("the stub admin {ADMIN} answers fabric.get and node.get by mesh only, not {}", other.op()) },
                })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
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
