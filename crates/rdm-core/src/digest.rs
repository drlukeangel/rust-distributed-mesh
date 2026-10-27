//! What a node publishes about itself on the fabric's topology topic.

use crate::ids::{EndpointId, FabricId, IncarnationId, NodeId, MeshId};
use crate::path::PathName;
use crate::runtime::RuntimeFact;
use serde::{Deserialize, Serialize};

/// One process birth of a logical node, as it publishes itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshNode {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// The id of this birth.
    pub incarnation: IncarnationId,
    /// The incarnation this birth replaces; `None` for the first birth.
    pub supersedes: Option<IncarnationId>,
    /// This birth's exact runtime (`runtime`): immutable for the incarnation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeFact>,
}

/// A member's status: set by its mesh-primary alone (states.md "Who sets each state") and carried to
/// peers in that primary's member projection, never on the member's own heartbeat (gossip.md §3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemberStatus {
    /// The node has not yet declared itself ready.
    Pending,
    /// The node takes traffic.
    ReadyForTraffic,
    /// The node takes no new work: its connections are being deleted.
    #[serde(rename = "connections-deleting")]
    ConnectionsDeleting,
    /// The node has announced its departure.
    Leaving,
}

/// The liveness a birth self-publishes on its heartbeat (node-identity-and-state.md §4,
/// `NodeState`): what it measures of itself, never a status. Postcard-positional: a variant is only
/// ever appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeState {
    /// Until the birth has published once and is self-ready.
    #[default]
    Joining,
    /// The default once it is self-ready.
    Alive,
    /// Over its CPU or RAM budget.
    Overloaded,
    /// It reaches a broker only through a peer or node-admin.
    Degraded,
}

impl NodeState {
    /// The liveness a birth publishes from its own word about itself: `Joining` while it is not yet
    /// self-ready (`Pending`, before its node.started at this start), `Alive` once it is. RDM measures
    /// no budget breach and no peer-only routing, so it publishes neither `Overloaded` nor `Degraded`.
    pub fn self_published(own: MemberStatus) -> Self {
        match own {
            MemberStatus::Pending => Self::Joining,
            MemberStatus::ReadyForTraffic | MemberStatus::ConnectionsDeleting | MemberStatus::Leaving => Self::Alive,
        }
    }
}

/// One topology digest: the node's current birth, its liveness and the status held for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDigest {
    /// The logical Fabric this member belongs to.
    pub fabric_id: FabricId,
    /// The birth the digest describes.
    pub node: MeshNode,
    /// The member's status as the holder holds it: its mesh-primary's (gossip.md §3.1 `MeshTopology`),
    /// or, for the holder's own birth, its own word. A heartbeat carries none
    /// ([`crate::wire::WireDigest`]).
    pub status: MemberStatus,
    /// The liveness the birth self-published on its last heartbeat (node-identity-and-state.md §4).
    #[serde(default)]
    pub node_state: NodeState,
    /// The control API a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    /// Orders ONE birth's own heartbeats: sender-local, per incarnation, from 1 (gossip.md §3.1).
    /// A receiver takes a digest of the held birth only when this is higher than the held one's;
    /// a Rafka-time stamp never decides heartbeat order.
    pub digest_seq: u64,
    /// When the digest was emitted, in Rafka-time: evidence only, never liveness, order or `Gone`.
    pub emitted_at_rafka_ms: u64,
    /// Where the birth keeps its data (current operational metadata a
    /// successor needs to manage it; not part of its runtime identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
    /// The mesh this member belongs to, by id; a node-admin's own digest carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh_id: Option<MeshId>,
    /// While `ConnectionsDeleting`: the work still in flight at this birth; the connections delete's own evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_flight: Option<u64>,
    /// The process's CPU and RAM at the moment the digest was published (the publishing node
    /// samples its own process); `None` when the publisher sampled nothing. Load, like
    /// `in_flight`: it never moves the topology version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load: Option<NodeLoad>,
    /// How this birth's mesh channel is doing: evidence for operators, like `load`; never
    /// liveness, order, election, tickle, or the topology version. A field added to the struct
    /// moves the postcard wire format again and gets the same review.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gossip: Option<GossipStats>,
    /// Hash of this birth's joined app types; the list is read with node.get.
    #[serde(default)]
    pub held_hash: [u8; 32],
    /// The revision at which this birth is its node type's primary in its mesh (`is_primary`), when it
    /// is (gossip.md "A primary change on gossip"): on its own digest, the election's result it
    /// installed; on a member of its mesh-primary's `MeshTopology` projection, the primary recorded on the
    /// node's row. A node that hears it installs a member fact, never a primary by advertisement: the
    /// election decides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_primary: Option<u64>,
    /// The revision at which this birth is the fabric-primary (`is_fabric_primary`), when it is, as
    /// [`Self::is_primary`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_fabric_primary: Option<u64>,
}

/// One birth's view of its own mesh channel, sampled when it publishes its digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GossipStats {
    /// Members whose digest this birth holds as heard (its view's current digests).
    pub heard: u32,
    /// The active gossip neighbours on its mesh channel (NeighborUp minus NeighborDown), never
    /// the seeded peer list.
    pub neighbours: u32,
    /// Topology frames this birth broadcast on its mesh channel since it started.
    pub frames_sent: u64,
    /// Topology frames this birth decoded from its mesh channel since it started.
    pub frames_received: u64,
}

/// One process's CPU and RAM, as its digest carries them: what is used and the ceiling it runs
/// under (the host's cores and memory unless the process was given a budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeLoad {
    /// CPU in use, in thousandths of one core (2400 = 2.4 cores' worth of work).
    pub cpu_used_millicores: u32,
    /// The CPU ceiling, in thousandths of one core.
    pub cpu_budget_millicores: u32,
    /// Resident memory in use, in bytes.
    pub ram_used_bytes: u64,
    /// The memory ceiling, in bytes.
    pub ram_budget_bytes: u64,
}

impl MeshDigest {
    /// The digest as JSON bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("digest serializes")
    }

    /// The digest a JSON byte string holds, `None` when it is not one.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_birth_round_trips_as_json_for_gossip() {
        let a = MeshNode {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:7000".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        };
        let back: MeshNode = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(back, a);
    }
}

