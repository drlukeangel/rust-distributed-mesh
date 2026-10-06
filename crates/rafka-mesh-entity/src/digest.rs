//! What a node publishes about itself on the fabric's membership topic.

use crate::endpoint::EndpointSet;
use crate::ids::{FabricId, IncarnationId, NodeId, TransportId};
use crate::path::PathName;
use crate::runtime::RuntimeFact;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One process birth of a logical node, as it publishes itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshNode {
    pub node_id: NodeId,
    pub name: PathName,
    pub transport_id: TransportId,
    pub incarnation: IncarnationId,
    /// The incarnation this birth replaces; `None` for the first birth.
    pub supersedes: Option<IncarnationId>,
    pub endpoints: EndpointSet,
    /// This birth's exact runtime (`runtime`): immutable for the incarnation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeFact>,
}

/// How long a process holds a departure after it accepted it (its own local
/// age, never another machine's clock). It covers the membership repair
/// horizon: a process cut off for less than this still learns the departure
/// once it rejoins.
pub const DEPARTED_RETENTION: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// A member's self-reported status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemberStatus {
    Pending,
    ReadyForTraffic,
    Draining,
    Leaving,
}

/// One membership digest: the node's current birth plus its status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDigest {
    /// The logical Fabric this member belongs to.
    pub fabric_id: FabricId,
    pub node: MeshNode,
    pub status: MemberStatus,
    /// The control API a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    pub emitted_unix_ms: u64,
    /// Where the birth keeps its data (current operational metadata a
    /// successor needs to manage it; not part of its runtime identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
    /// Kind-specific facts (e.g. the mesh id), string-keyed.
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

impl MeshDigest {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("digest serializes")
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointSlot;

    #[test]
    fn a_birth_round_trips_as_json_for_gossip() {
        let a = MeshNode {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            transport_id: TransportId("k".into()),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            endpoints: EndpointSet(vec![EndpointSlot::assign("rpc", "127.0.0.1:7000".parse().unwrap())]),
            runtime: None,
        };
        let back: MeshNode = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(back, a);
    }
}
