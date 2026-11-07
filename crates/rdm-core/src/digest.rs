//! What a node publishes about itself on its mesh's topology channel, and what a view holds of each
//! node: its birth, its status and the primary roles it holds (gossip.md §3.1 `Digest`).

use crate::ids::{EndpointId, FabricId, IncarnationId, NodeId};
use crate::model::NodeStatus;
use crate::role::PrimaryRole;
use serde::{Deserialize, Serialize};

/// One process birth of a logical node: its identity, its endpoint and its lineage, as it publishes
/// them with its first gossip into its mesh (gossip.md §3.1 `Digest`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshNode {
    /// The owning mesh, independent of path spelling.
    pub mesh: String,
    /// The node's type, independent of path spelling.
    pub r#type: String,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The node's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// The id of this birth.
    pub incarnation: IncarnationId,
    /// The incarnation this birth replaces; `None` for the first birth.
    pub supersedes: Option<IncarnationId>,
}

impl MeshNode {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> crate::PathName { crate::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// One primary role a node holds now, with the revision of its record (Q392): the node-type primary
/// of its type in its mesh (`mesh-primary` for a node-admin) or the fabric-primary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoleRevision {
    /// The role.
    pub role: PrimaryRole,
    /// The revision of the role's record.
    pub revision: u64,
}

impl RoleRevision {
    /// The roles a birth of `r#type` holds: its node-type primary role at `primary`, and the
    /// fabric-primary at `fabric_primary`, each when held.
    pub fn of_type(r#type: &str, primary: Option<u64>, fabric_primary: Option<u64>) -> Vec<Self> {
        let mut roles: Vec<Self> = [(PrimaryRole::of_type(r#type), primary), (PrimaryRole::FabricPrimary, fabric_primary)]
            .into_iter()
            .filter_map(|(role, revision)| revision.map(|revision| Self { role, revision }))
            .collect();
        roles.sort_by(|a, b| a.role.cmp(&b.role));
        roles
    }
}

/// One node as a view holds it: its birth, its status and the primary roles it holds, ordered by its
/// own Digest's sequence. The author, sequence and time of a Digest are its envelope's (gossip.md
/// §3.4): `fabric_id`, `node.node_id` and `node.incarnation` are its `source`, `digest_seq` its
/// `sequence` and `emitted_at_rafka_ms` its `time`. Management metadata (runtime, data dir, control
/// API, mesh id) is never held here: it is on the node's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDigest {
    /// The logical Fabric this node belongs to.
    pub fabric_id: FabricId,
    /// The birth the digest describes.
    pub node: MeshNode,
    /// The node's status, as the node writes it on its own record (Q356) and publishes it in its
    /// Digest: one status, carried by everything that carries the node (gossip.md §3.1 `Digest`).
    pub status: NodeStatus,
    /// The primary roles this birth holds now, each with its revision (Q392, gossip.md "A primary
    /// change on gossip"): on its own Digest, the election's results it installed; on a node entry of
    /// its mesh-primary's aggregate, the primaries recorded on the node's row. A node that hears one
    /// installs a primary record, never a primary by advertisement: the election decides.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary: Vec<RoleRevision>,
    /// Orders ONE birth's own Digests: sender-local, per incarnation, from 1 (gossip.md §3.1): the
    /// envelope's `sequence`. A receiver takes a Digest of the held birth only when this is higher than
    /// the held one's; a Rafka-time stamp never decides heartbeat order.
    pub digest_seq: u64,
    /// When the Digest was emitted, in Rafka-time (the envelope's `time`): evidence only, never
    /// liveness, order or `Gone`.
    pub emitted_at_rafka_ms: u64,
}

/// One birth's view of its own mesh channel, sampled in memory: a diagnostic `node.get` serves for
/// the node itself (node-identity-and-state.md §2.5). Never gossiped and never written to a record,
/// storage or durable history (Q394).
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

/// One process's CPU and RAM, sampled in memory by the process itself: what is used and the ceiling
/// it runs under (the host's cores and memory unless the process was given a budget). Never gossiped
/// (Q394); `node.get` serves the node's own sample.
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

    /// The revision at which this birth holds `role`, when it holds it.
    pub fn role_revision(&self, role: &PrimaryRole) -> Option<u64> {
        self.primary.iter().find(|r| &r.role == role).map(|r| r.revision)
    }

    /// The revision at which this birth is its node type's primary in its mesh (`is_primary`), when
    /// it is.
    pub fn is_primary(&self) -> Option<u64> {
        self.role_revision(&PrimaryRole::of_type(&self.node.r#type))
    }

    /// The revision at which this birth is the fabric-primary (`is_fabric_primary`), when it is.
    pub fn is_fabric_primary(&self) -> Option<u64> {
        self.role_revision(&PrimaryRole::FabricPrimary)
    }

    /// Hold `role` at `revision`, or not at all (`None`): the one entry of the role in [`Self::primary`].
    pub fn set_role(&mut self, role: PrimaryRole, revision: Option<u64>) {
        self.primary.retain(|r| r.role != role);
        if let Some(revision) = revision {
            self.primary.push(RoleRevision { role, revision });
            self.primary.sort_by(|a, b| a.role.cmp(&b.role));
        }
    }

    /// Hold this birth's node-type primary role at `revision`, or not at all.
    pub fn set_primary(&mut self, revision: Option<u64>) {
        let role = PrimaryRole::of_type(&self.node.r#type);
        self.set_role(role, revision);
    }

    /// Hold the fabric-primary role at `revision`, or not at all.
    pub fn set_fabric_primary(&mut self, revision: Option<u64>) {
        self.set_role(PrimaryRole::FabricPrimary, revision);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_birth_round_trips_as_json_for_gossip() {
        let a = MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
            node_id: NodeId::mint(),
            path: "mesh1.node".into(), name: "1".into(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:7000".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        };
        let back: MeshNode = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(back, a);
    }

    /// CONTRACT (Q392): a digest holds each primary role once, at the revision last set; the node-type
    /// role of a node-admin is `mesh-primary`. Must NOT happen: two entries of one role.
    #[test]
    fn a_digest_holds_each_primary_role_once_at_its_revision() {
        let mut d = MeshDigest {
            fabric_id: FabricId::parse("1123456789ab").unwrap(),
            node: MeshNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node_id: NodeId::mint(), path: "mesh1.node-admin".into(), name: "1".into(), endpoint_id: EndpointId("k".into()), transport_addr: "127.0.0.1:7000".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None },
            status: NodeStatus::Ready, primary: Vec::new(), digest_seq: 1, emitted_at_rafka_ms: 1,
        };
        d.set_primary(Some(3));
        d.set_primary(Some(4));
        d.set_fabric_primary(Some(2));
        assert_eq!(d.primary, vec![RoleRevision { role: PrimaryRole::MeshPrimary, revision: 4 }, RoleRevision { role: PrimaryRole::FabricPrimary, revision: 2 }]);
        assert_eq!((d.is_primary(), d.is_fabric_primary()), (Some(4), Some(2)));
        d.set_primary(None);
        assert_eq!((d.is_primary(), d.is_fabric_primary()), (None, Some(2)));
    }
}
