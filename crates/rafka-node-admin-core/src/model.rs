//! Fabric → Mesh → Node model (i143 PRD §1, §13; ownership §4, §5.1).
//!
//! Every identity is opaque and compared by equality only. Incarnation ids
//! are never ordered (PRD §22).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque id local to node-admin: one runtime realised by a provider.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeploymentId(pub String);

impl DeploymentId {
    /// A fresh deployment id.
    pub fn mint() -> Self {
        Self(hex::encode(rand::random::<[u8; 16]>()))
    }
}

impl fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// Identity, path and port types are the Mesh EF's (rafka-mesh-entity);
// node-admin uses them, it does not redefine them.
pub use rafka_mesh_entity::path::is_valid_mesh_name;
pub use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName, PathNameError};

/// Node lifecycle status as published on views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work.
    Draining,
    /// Leaving: it has announced its departure.
    Leaving,
    /// Unheard past the staleness floor (`RDM_STALENESS_MS`, 30 s): this node's own pruner marks
    /// the silent peer in place and keeps it; its next digest flips it back (fabric-node-lifecycle.md
    /// §7.3, i77 PRD row 18). Never death, and never a reason to restart or delete it.
    PendingReconnect,
    /// Commanded silence: its mesh executor is restarting this exact birth (`NodeRestarting`,
    /// fabric-node-lifecycle.md) and owns bringing it back. Held through its `Leaving` and
    /// silence, exempt from the staleness marks, never routable; the later birth's own digest
    /// clears it.
    Restarting,
    /// True offline: half a floor after the mark the mesh primary's one QUIC connect found no path,
    /// on two rounds at least one staleness floor apart. Observer inferred only; never sent, never a
    /// reason to restart or delete the node.
    Dead,
}

impl NodeStatus {
    /// A node that may hold or contend for a cohort primary.
    pub fn is_live(self) -> bool {
        matches!(self, Self::Pending | Self::ReadyForTraffic | Self::Draining)
    }
}

/// Mesh and Fabric lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work.
    Draining,
    /// Retired: it has been taken out of service.
    Retired,
    /// The fabric primary has decided a peer mesh is reborn (`investigate`).
    Degraded,
}

/// Deployment provider kind, fabric policy (PRD §1.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Each node is an OS process.
    Process,
    /// Each node is a container.
    Container,
}

/// One node as the fabric control projection knows it (`NodeView`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's kind.
    pub kind: NodeKind,
    /// The name of the node's mesh.
    pub mesh: String,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's fabric endpoint id, once known.
    pub endpoint_id: Option<EndpointId>,
    /// The incarnation of the node's current birth, once launched.
    pub incarnation_id: Option<IncarnationId>,
    /// The deployment that runs the node, once created.
    pub deployment_id: Option<DeploymentId>,
    /// The provider that runs the node.
    pub provider: Option<ProviderKind>,
    /// The node's data directory.
    pub data_dir: Option<String>,
    /// The node's lifecycle state.
    pub status: NodeStatus,
    /// Whether the node holds its cohort's primary seat.
    pub is_primary: bool,
    /// Whether the node holds the fabric-primary seat.
    pub is_fabric_primary: bool,
    /// The control API base a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    /// The one address of the birth's Iroh endpoint; `None` until it is born.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// Non-Iroh listeners the birth binds, by name.
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// May application routing select this node: held, not departed, and under no open
    /// lifecycle overlay (`NodeDeleting`). The resolver never reads it.
    #[serde(default = "routable_default")]
    pub routable: bool,
    /// The lifecycle state this birth declared to its authority and the authority applied
    /// (`status_rpc`); `None` until it declares. Not `status`: that is what membership hears.
    #[serde(default)]
    pub declared: Option<String>,
}

fn routable_default() -> bool {
    true
}

impl Node {
    /// A newly allocated node: identity only, nothing realised yet.
    pub fn allocated(name: PathName) -> Self {
        Self {
            kind: name.kind,
            mesh: name.mesh.clone(),
            name,
            node_id: NodeId::mint(),
            endpoint_id: None,
            incarnation_id: None,
            deployment_id: None,
            provider: None,
            data_dir: None,
            status: NodeStatus::Pending,
            is_primary: false,
            is_fabric_primary: false,
            admin_api_base: None,
            transport_addr: None,
            listeners: Vec::new(),
            routable: true,
            declared: None,
        }
    }
}

/// A mesh as the fabric record holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mesh {
    /// `None` until a member of the mesh has said which id it carries.
    pub id: Option<MeshId>,
    /// The mesh's name.
    pub name: String,
    /// The mesh's lifecycle state.
    pub status: ScopeStatus,
}

/// The fabric record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fabric {
    /// The logical Fabric's identity, kept for its lifetime; the name is its label.
    pub id: FabricId,
    /// The fabric's name.
    pub name: String,
    /// The fabric's lifecycle state.
    pub status: ScopeStatus,
    /// The provider that runs the fabric's nodes.
    pub provider: ProviderKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_name_round_trips() {
        for s in ["mesh1.rpc.2", "mesh1.admin.1", "m-2.rpc.17"] {
            let p: PathName = s.parse().unwrap();
            assert_eq!(p.to_string(), s);
            let json = serde_json::to_string(&p).unwrap();
            assert_eq!(serde_json::from_str::<PathName>(&json).unwrap(), p);
        }
        let p: PathName = "mesh1.rpc.2".parse().unwrap();
        assert_eq!(p, PathName::new("mesh1", NodeKind::RpcNode, 2));
    }

    #[test]
    fn path_name_refuses_by_name() {
        assert_eq!("mesh1.rpc".parse::<PathName>(), Err(PathNameError::Shape("mesh1.rpc".into())));
        assert_eq!("Mesh1.rpc.1".parse::<PathName>(), Err(PathNameError::MeshName("Mesh1".into())));
        assert_eq!("mesh1.registry.1".parse::<PathName>(), Err(PathNameError::Kind("registry".into())));
        assert_eq!("mesh1.rpc.0".parse::<PathName>(), Err(PathNameError::Ordinal("0".into())));
        assert_eq!("mesh1.rpc.01".parse::<PathName>(), Err(PathNameError::Ordinal("01".into())));
    }

    #[test]
    fn minted_ids_are_distinct_and_opaque() {
        let a = IncarnationId::mint();
        let b = IncarnationId::mint();
        assert_ne!(a, b);
        assert_eq!(a.0.len(), 32);
    }

    #[test]
    fn view_spelling_matches_the_design_contract() {
        assert_eq!(serde_json::to_value(NodeStatus::ReadyForTraffic).unwrap(), "ready-for-traffic");
        assert_eq!(serde_json::to_value(NodeKind::RpcNode).unwrap(), "rpc_node");
        assert_eq!(serde_json::to_value(NodeKind::NodeAdmin).unwrap(), "node_admin");
        let n = Node::allocated("mesh1.rpc.2".parse().unwrap());
        let v = serde_json::to_value(&n).unwrap();
        assert_eq!(v["name"], "mesh1.rpc.2");
        assert_eq!(v["kind"], "rpc_node");
        assert_eq!(v["mesh"], "mesh1");
        assert_eq!(v["status"], "pending");
    }

    #[test]
    fn only_pending_ready_and_draining_are_live() {
        assert!(NodeStatus::ReadyForTraffic.is_live());
        assert!(NodeStatus::Draining.is_live());
        assert!(!NodeStatus::Leaving.is_live());
        assert!(!NodeStatus::PendingReconnect.is_live());
    }
}
