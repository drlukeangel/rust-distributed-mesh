//! Fabric → Mesh → Node model (i143 PRD §1, §13; ownership §4, §5.1).
//!
//! Every identity is opaque and compared by equality only. Freshness tokens
//! and incarnation ids are never ordered (PRD §22).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque id local to node-admin: one runtime realised by a provider.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeploymentId(pub String);

impl DeploymentId {
    pub fn mint() -> Self {
        Self(hex::encode(rand::random::<[u8; 16]>()))
    }
}

impl fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// Identity, path and endpoint-slot types are the Mesh EF's (rafka-mesh-entity);
// node-admin uses them, it does not redefine them.
pub use rafka_mesh_entity::path::is_valid_mesh_name;
pub use rafka_mesh_entity::{EndpointSlot, FabricId, FreshnessToken, IncarnationId, MeshId, NodeId, TransportId, NodeKind, PathName, PathNameError, SlotPolicy};

/// Node lifecycle status as published on views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    Pending,
    ReadyForTraffic,
    Draining,
    Leaving,
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
    Pending,
    ReadyForTraffic,
    Draining,
    Retired,
}

/// Deployment provider kind, fabric policy (PRD §1.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Process,
    Container,
}

/// One node as the fabric control projection knows it (`NodeView`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub name: PathName,
    pub kind: NodeKind,
    pub mesh: String,
    pub node_id: NodeId,
    pub transport_id: Option<TransportId>,
    pub incarnation_id: Option<IncarnationId>,
    pub deployment_id: Option<DeploymentId>,
    pub provider: Option<ProviderKind>,
    pub data_dir: Option<String>,
    pub status: NodeStatus,
    pub is_primary: bool,
    pub is_fabric_primary: bool,
    /// The control API base a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    /// The one address of the birth's Iroh endpoint; `None` until it is born.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// The logical Node RPC slots the birth serves, under their tokens.
    pub endpoints: Vec<EndpointSlot>,
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
            transport_id: None,
            incarnation_id: None,
            deployment_id: None,
            provider: None,
            data_dir: None,
            status: NodeStatus::Pending,
            is_primary: false,
            is_fabric_primary: false,
            admin_api_base: None,
            transport_addr: None,
            endpoints: Vec::new(),
            listeners: Vec::new(),
            routable: true,
            declared: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mesh {
    /// `None` until a member of the mesh has said which id it carries.
    pub id: Option<MeshId>,
    pub name: String,
    pub status: ScopeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fabric {
    /// The logical Fabric's identity, kept for its lifetime; the name is its label.
    pub id: FabricId,
    pub name: String,
    pub status: ScopeStatus,
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
        assert_eq!("mesh1.broker.1".parse::<PathName>(), Err(PathNameError::Kind("broker".into())));
        assert_eq!("mesh1.rpc.0".parse::<PathName>(), Err(PathNameError::Ordinal("0".into())));
        assert_eq!("mesh1.rpc.01".parse::<PathName>(), Err(PathNameError::Ordinal("01".into())));
    }

    #[test]
    fn minted_ids_are_distinct_and_opaque() {
        let a = IncarnationId::mint();
        let b = IncarnationId::mint();
        assert_ne!(a, b);
        assert_eq!(a.0.len(), 32);
        assert_ne!(FreshnessToken::mint(), FreshnessToken::mint());
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
        assert!(!NodeStatus::Dead.is_live());
    }
}
