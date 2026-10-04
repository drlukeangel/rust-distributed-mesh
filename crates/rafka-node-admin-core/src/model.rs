//! Fabric → Mesh → Node model (i143 PRD §1, §13; ownership §4, §5.1).
//!
//! Every identity is opaque and compared by equality only. Freshness tokens
//! and incarnation ids are never ordered (PRD §22).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

macro_rules! opaque_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            /// Mint a fresh id: 128 random bits, lower-case hex.
            pub fn mint() -> Self {
                Self(hex::encode(rand::random::<[u8; 16]>()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

opaque_id!(
    /// Logical node identity, minted once at `AllocateIdentity`; kept across restarts.
    NodeId
);
opaque_id!(
    /// One process birth of a logical node (`RuntimeIncarnationId`).
    IncarnationId
);
opaque_id!(
    /// The node's authenticated transport (Iroh) identity.
    FabricId
);
opaque_id!(
    /// One runtime realised by a deployment provider.
    DeploymentId
);
opaque_id!(
    /// Opaque per-slot freshness token, minted with each endpoint assignment.
    FreshnessToken
);
opaque_id!(
    /// A mesh's minted identity. Recovery keeps it; replacement mints a new one.
    MeshId
);

/// The two generic node kinds of the Mesh product proof estate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    NodeAdmin,
    RpcNode,
}

impl NodeKind {
    /// The `path.name` kind segment.
    pub fn segment(self) -> &'static str {
        match self {
            Self::NodeAdmin => "admin",
            Self::RpcNode => "rpc",
        }
    }

    pub fn from_segment(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Self::NodeAdmin),
            "rpc" => Some(Self::RpcNode),
            _ => None,
        }
    }
}

/// A stable topology slot: `<mesh>.<admin|rpc>.<ordinal>`, ordinal from 1.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathName {
    pub mesh: String,
    pub kind: NodeKind,
    pub ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathNameError {
    Shape(String),
    MeshName(String),
    Kind(String),
    Ordinal(String),
}

impl fmt::Display for PathNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape(s) => write!(f, "`{s}` is not <mesh>.<admin|rpc>.<ordinal>"),
            Self::MeshName(s) => write!(f, "mesh name `{s}` must match [a-z0-9][a-z0-9-]{{0,63}}"),
            Self::Kind(s) => write!(f, "node kind `{s}` is not admin or rpc"),
            Self::Ordinal(s) => write!(f, "ordinal `{s}` is not a number >= 1"),
        }
    }
}

/// `[a-z0-9][a-z0-9-]{0,63}`.
pub fn is_valid_mesh_name(m: &str) -> bool {
    let b = m.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

impl PathName {
    pub fn new(mesh: impl Into<String>, kind: NodeKind, ordinal: u32) -> Self {
        Self { mesh: mesh.into(), kind, ordinal }
    }
}

impl FromStr for PathName {
    type Err = PathNameError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        let [mesh, kind, ord] = parts[..] else { return Err(PathNameError::Shape(s.into())) };
        if !is_valid_mesh_name(mesh) {
            return Err(PathNameError::MeshName(mesh.into()));
        }
        let kind = NodeKind::from_segment(kind).ok_or_else(|| PathNameError::Kind(kind.into()))?;
        let ordinal = ord
            .parse::<u32>()
            .ok()
            .filter(|n| *n >= 1 && !ord.starts_with('0'))
            .ok_or_else(|| PathNameError::Ordinal(ord.into()))?;
        Ok(Self { mesh: mesh.into(), kind, ordinal })
    }
}

impl fmt::Display for PathName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.mesh, self.kind.segment(), self.ordinal)
    }
}

impl Serialize for PathName {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PathName {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

/// Per-slot restart policy (`docs/i143/design.md` §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotPolicy {
    /// A new port and token on every process birth.
    Fresh,
    /// A restart keeps the advertised port and token; a replacement does not.
    Stable,
}

/// One advertised endpoint slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointSlot {
    pub slot: String,
    pub addr: SocketAddr,
    pub freshness: FreshnessToken,
}

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
    pub fabric_id: Option<FabricId>,
    pub incarnation_id: Option<IncarnationId>,
    pub deployment_id: Option<DeploymentId>,
    pub provider: Option<ProviderKind>,
    pub data_dir: Option<String>,
    pub status: NodeStatus,
    pub is_primary: bool,
    pub is_fabric_primary: bool,
    /// The control API base a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    pub endpoints: Vec<EndpointSlot>,
}

impl Node {
    /// A newly allocated node: identity only, nothing realised yet.
    pub fn allocated(name: PathName) -> Self {
        Self {
            kind: name.kind,
            mesh: name.mesh.clone(),
            name,
            node_id: NodeId::mint(),
            fabric_id: None,
            incarnation_id: None,
            deployment_id: None,
            provider: None,
            data_dir: None,
            status: NodeStatus::Pending,
            is_primary: false,
            is_fabric_primary: false,
            admin_api_base: None,
            endpoints: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mesh {
    pub id: MeshId,
    pub name: String,
    pub status: ScopeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fabric {
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
