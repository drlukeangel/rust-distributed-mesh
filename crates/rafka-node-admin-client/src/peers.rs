//! What a peer reports of the mesh, read through the `node` objects from outside it.
//!
//! A scenario that must know that every other node heard a lifecycle fact asks the node: its held view
//! of the mesh (`node.get` through `node.topology.get`), never a span count. A peer that never reports the
//! fact is named, with the fact and the last thing it reported.

use crate::{HeldView, NodeRpc, NodeView};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use std::sync::Arc;
use std::time::Duration;

/// A client over a table of the mesh's nodes, taken from `node.get`.
pub struct Peers {
    rpc: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    _endpoint: iroh::Endpoint,
}

impl Peers {
    /// A client that reaches every node of `views`.
    pub async fn of(views: &[NodeView]) -> Self {
        let endpoint = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the test endpoint binds");
        let resolver = Arc::new(StaticResolver::new());
        let me = Self { rpc: Arc::new(NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm")), resolver, _endpoint: endpoint };
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

    /// The node objects over this client: Build calls start at the fabric-primary `admin` names.
    pub async fn nodes(&self, admin: crate::NodeAdminClient) -> Result<crate::Nodes, crate::CallEnd> {
        let fp = admin.fabric().await.map_err(crate::CallEnd::from)?.fabric_primary.ok_or_else(|| crate::CallEnd::Indeterminate { reason: "the admin's fabric names no fabric-primary".into() })?;
        Ok(crate::Nodes::new(admin, crate::BuildCarrier::new(self.rpc.clone(), fp)))
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
