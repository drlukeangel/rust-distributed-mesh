//! What a peer reports of the mesh, read through the `node` objects from outside it.
//!
//! A scenario that must know that every other node heard a lifecycle fact asks the node: its held view
//! of the mesh (`node.get` through `node.topology.get`), never a span count. A peer that never reports the
//! fact is named, with the fact and the last thing it reported.

use crate::{HeldView, NodeRpc, NodeStatus, NodeView};
use rdm_core::{MeshNode, NodeId, PathName};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The line a node-admin prints on its stdout once it has started: `RDM_NODE_BIRTH=` and, as JSON, the
/// birth its Node RPC door serves under (node id, `path.name`, endpoint, address, incarnation). The
/// process that started the admin reaches it by this line alone ([`Peers::learn_birth`]); the door
/// answers NotReady until it opens.
pub const BIRTH_LINE: &str = "RDM_NODE_BIRTH=";

/// The birth a line a node-admin printed names: `None` for any other line, the decode refusal named for
/// a birth line that does not decode.
pub fn birth_of_line(line: &str) -> Option<Result<MeshNode, String>> {
    let json = line.strip_prefix(BIRTH_LINE)?;
    Some(serde_json::from_str(json).map_err(|e| format!("{line}: not a birth: {e}")))
}

/// The birth a `node.get` view holds for its node when it holds the whole birth (endpoint, address and
/// incarnation): how a process that read the view reaches that node as it reaches one whose birth line
/// it read. `None` for a node not born yet.
pub fn birth_of_view(n: &NodeView) -> Option<MeshNode> {
    Some(MeshNode {
        node_id: n.node_id.clone(),
        name: n.name.clone(),
        endpoint_id: rdm_core::EndpointId(n.endpoint_id.clone().filter(|e| !e.is_empty())?),
        transport_addr: n.transport_addr?,
        incarnation: n.incarnation_id.clone()?,
        supersedes: None,
        runtime: None,
    })
}

/// A client over a table of the mesh's nodes, taken from `node.get`.
pub struct Peers {
    rpc: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    /// The birth each `path.name` resolves to now: a name that moves to another node id retires the
    /// id it held, so a call by path reaches the current birth and never a departed one.
    held: Mutex<HashMap<PathName, NodeId>>,
    _endpoint: iroh::Endpoint,
}

impl Peers {
    /// A client that reaches every node of `views`.
    pub async fn of(views: &[NodeView]) -> Self {
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the test endpoint binds");
        let resolver = Arc::new(StaticResolver::new());
        let me = Self { rpc: Arc::new(NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm")), resolver, held: Mutex::new(HashMap::new()), _endpoint: endpoint };
        me.learn(views);
        me
    }

    /// Make every node of `views` reachable.
    pub fn learn(&self, views: &[NodeView]) {
        for n in views {
            self.resolver.insert(ResolvedNode {
                node_id: n.node_id.clone(),
                name: n.name.clone(),
                endpoint_id: n.endpoint_id.as_deref().expect("a ready node carries its endpoint").parse().expect("an iroh key"),
                incarnation: n.incarnation_id.clone().expect("a ready node carries its incarnation"),
                transport_addr: n.transport_addr.expect("a ready node carries its address"),
            });
        }
    }

    /// The Build carrier whose calls start at the fabric-primary `fabric_primary`, when the caller
    /// is told where the seat is: a stream that ended `SeatMoved` names the admin that holds it now
    /// (node-rpc-envelope.md TakeSeats: "the caller resubmits the same `build_id` to N"). Its birth
    /// is read from `entry`'s node list over Node RPC and made reachable here; a list that holds
    /// none is named.
    pub async fn carrier_following(&self, entry: &PathName, fabric_primary: &PathName) -> Result<crate::BuildCarrier, crate::CallEnd> {
        let held = crate::Nodes::new(self.carrier_at(entry.clone())).get(&crate::NodeSelector::Mesh(fabric_primary.mesh.clone())).await?;
        let birth = held.iter().find(|n| n.name == *fabric_primary).ok_or_else(|| crate::CallEnd::Indeterminate { code: rdm_core::outcome::Code::NotFound, reason: format!("the node list of {entry} holds no {fabric_primary}, the fabric-primary the caller was sent to") })?;
        self.learn_known(std::slice::from_ref(birth));
        Ok(self.carrier_at(fabric_primary.clone()))
    }
    /// Make reachable every node of `views` that carries its whole birth (endpoint, incarnation and
    /// address) and is not deleted, retiring the node id a `path.name` held before: a view read
    /// while a node is pending names a node that cannot be dialled yet, and it is learned by the
    /// read that finds it ready.
    pub fn learn_known(&self, views: &[NodeView]) {
        for n in views {
            if n.status == NodeStatus::Deleted {
                continue;
            }
            let (Some(endpoint), Some(incarnation), Some(addr)) = (n.endpoint_id.as_deref().and_then(|e| e.parse().ok()), n.incarnation_id.clone(), n.transport_addr) else { continue };
            self.hold(ResolvedNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: endpoint, incarnation, transport_addr: addr });
        }
    }

    /// Make reachable the birth a node-admin printed at start ([`BIRTH_LINE`]), retiring the node id
    /// its `path.name` held before; a birth whose endpoint is no Iroh key is refused by name.
    pub fn learn_birth(&self, birth: &MeshNode) -> Result<(), String> {
        let endpoint_id = birth.endpoint_id.0.parse().map_err(|e| format!("the birth of {} ({}) names the endpoint {:?}, no Iroh key: {e}", birth.name, birth.node_id, birth.endpoint_id.0))?;
        self.hold(ResolvedNode { node_id: birth.node_id.clone(), name: birth.name.clone(), endpoint_id, incarnation: birth.incarnation.clone(), transport_addr: birth.transport_addr });
        Ok(())
    }

    /// Resolve `node` by its id and its `path.name`; a name that moves to another node id retires
    /// the id it held.
    fn hold(&self, node: ResolvedNode) {
        if let Some(before) = self.held.lock().unwrap().insert(node.name.clone(), node.node_id.clone()) {
            if before != node.node_id {
                self.resolver.remove_gone(&before);
            }
        }
        self.resolver.insert(node);
    }

    /// A carrier whose calls start at the node-admin `admin`: its reads answer from that admin's
    /// view, and a Build call it does not own follows the redirect to the fabric-primary.
    pub fn carrier_at(&self, admin: PathName) -> crate::BuildCarrier {
        crate::BuildCarrier::new(self.rpc.clone(), admin)
    }

    /// Await the typed fact `fact` of `peer`'s held view of `mesh`; a peer that never reports it is named
    /// with the fact and the last thing it reported.
    pub async fn fact(&self, peer: &NodeView, mesh: &str, fact: &str, holds: impl Fn(&HeldView) -> bool) {
        let target = NodeTarget::ExactNode(peer.node_id.clone());
        let mut last = String::from("nothing");
        for _ in 0..300 {
            match NodeRpc::new(&self.rpc).held_view(&target, mesh, &CallOptions::default()).await {
                Ok(view) if holds(&view) => return,
                Ok(view) => last = format!("{} members, overlays {:?}", view.members.len(), view.in_flight.iter().map(|o| o.operation.clone()).collect::<Vec<_>>()),
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{} never reported: {fact}; its last report was {last}", peer.name);
    }
}
