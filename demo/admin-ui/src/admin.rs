//! The node-admin this UI reads and drives, over Node RPC: RDM serves no HTTP API
//! (rdm-crate-and-test-layout.md §2), so the UI enters at the birth a node-admin printed at start
//! (`RDM_NODE_ADMIN_BIRTH`, the JSON after `RDM_NODE_BIRTH=`) and calls the typed objects there.

use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_admin_client::{
    birth_of_view, Accepted, BuildCarrier, BuildId, BuildReceipts, BuildSpec, Builds, CallEnd, Change, ConnectionsView, ExactBirth, FabricDesired, FabricView, Fabrics,
    MeshDesired, MeshNode, NodeSelector, NodeSpec, NodeView, Nodes, Peers,
};
use std::sync::Arc;

/// The node-admin the UI enters at, and the peers it reaches over Node RPC.
pub struct Admin {
    peers: Arc<Peers>,
    entry: PathName,
}

impl Admin {
    /// The admin `RDM_NODE_ADMIN_BIRTH` names, `None` when it is unset; a birth that does not decode
    /// is refused by name.
    pub async fn from_env() -> Result<Option<Admin>, String> {
        let Some(json) = std::env::var("RDM_NODE_ADMIN_BIRTH").ok().filter(|b| !b.trim().is_empty()) else { return Ok(None) };
        let birth: MeshNode = serde_json::from_str(&json).map_err(|e| format!("RDM_NODE_ADMIN_BIRTH {json:?}: not a birth: {e}"))?;
        Ok(Some(Admin::at(&birth).await?))
    }

    /// The admin of `birth`.
    pub async fn at(birth: &MeshNode) -> Result<Admin, String> {
        let peers = Arc::new(Peers::of(&[]).await);
        peers.learn_birth(birth)?;
        Ok(Admin { peers, entry: birth.name.clone() })
    }

    fn carrier(&self) -> BuildCarrier {
        self.peers.carrier_at(self.entry.clone())
    }

    /// `fabric.get` at the entry admin.
    pub async fn fabric(&self) -> Result<FabricView, CallEnd> {
        Fabrics::new(self.carrier()).get().await
    }

    /// `node.get` of every node of every mesh the entry admin's view holds.
    pub async fn nodes(&self) -> Result<Vec<NodeView>, CallEnd> {
        let nodes = Nodes::new(self.carrier());
        let mut out = Vec::new();
        for m in self.fabric().await?.meshes {
            out.extend(nodes.get(&NodeSelector::Mesh(m.name.clone())).await?);
        }
        for n in &out {
            if let Some(birth) = birth_of_view(n) {
                let _ = self.peers.learn_birth(&birth);
            }
        }
        Ok(out)
    }

    /// `node.connections.get` of `n`'s exact birth.
    pub async fn connections(&self, n: &NodeView) -> Option<Result<ConnectionsView, CallEnd>> {
        let incarnation = n.incarnation_id.clone()?;
        let birth = ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation };
        Some(Nodes::new(self.carrier()).connections_get(&birth).await)
    }

    /// `build.get` of `id`: its receipts.
    pub async fn build(&self, id: &BuildId) -> Result<BuildReceipts, CallEnd> {
        Builds::new(self.carrier()).get(id).await
    }

    /// node.create of a `kind` node in `mesh`.
    pub async fn spawn(&self, mesh: &str, kind: NodeKind) -> Result<Accepted, CallEnd> {
        Ok(Nodes::new(self.carrier()).create(&NodeSpec { mesh: mesh.to_string(), kind }).await?.accepted().clone())
    }

    /// node.restart of `node`.
    pub async fn restart(&self, node: &PathName) -> Result<Accepted, CallEnd> {
        Ok(Nodes::new(self.carrier()).restart(node).await?.accepted().clone())
    }

    /// node.delete of `node`.
    pub async fn remove(&self, node: &PathName) -> Result<Accepted, CallEnd> {
        Ok(Nodes::new(self.carrier()).delete(node).await?.accepted().clone())
    }

    /// build.create reconciling the fabric to `desired`.
    pub async fn build_fabric(&self, desired: FabricDesired) -> Result<Accepted, CallEnd> {
        let s = Builds::new(self.carrier()).create(&BuildSpec::Fabric(desired)).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create of a new mesh with `desired`'s counts.
    pub async fn create_mesh(&self, desired: MeshDesired) -> Result<Accepted, CallEnd> {
        let s = Builds::new(self.carrier()).create(&BuildSpec::CreateMesh(desired)).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create removing the mesh `mesh`.
    pub async fn remove_mesh(&self, mesh: &str) -> Result<Accepted, CallEnd> {
        let s = self.carrier().create(Change::RemoveMesh { mesh: mesh.to_string() }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }
}
