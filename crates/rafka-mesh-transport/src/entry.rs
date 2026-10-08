//! What a node-admin answers a joining node with.
//!
//! A node takes its identity at launch; everything it needs to know about the fabric it takes
//! from the node-admin that deployed it, in the reply to its `JoinNode` call (Node RPC op
//! `0x1D`, `rafka-node-rpc-contract::join`). The answer is what that admin holds now: its
//! topology projection (fabric, meshes, nodes) and the membership digests it is projected from.
//! The node records those digests as heard, so its first view is its admin's, and only then
//! marks itself ready. There is no join mode: a member the answer names but the node never hears
//! again is inferred dead by silence, like any other.

use rafka_mesh_entity::digest::MeshDigest;
use serde::{Deserialize, Serialize};

/// What node-admin holds now.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntryAnswer {
    /// The answering admin's path name.
    pub served_by: String,
    /// The admin's topology projection (`fabric`, `meshes`, `nodes`), as the
    /// admin serialises it; this crate does not interpret it.
    pub topology: serde_json::Value,
    /// The membership digests of every member the admin hears.
    pub members: Vec<MeshDigest>,
    /// The admin's current fabric control state (its desired topology), as
    /// the admin serialises it; this crate does not interpret it. Null from
    /// an admin that holds none.
    #[serde(default)]
    pub control: serde_json::Value,
    /// The statuses the admin holds (`MeshStatus` / `FabricStatus` frames, original publisher and
    /// instant kept): the top-up a node that missed the status frames takes. Never inside `members`.
    #[serde(default)]
    pub statuses: Vec<crate::membership::Frame>,
    /// The cross-Mesh projection the answering node holds of every source Mesh, loads omitted,
    /// with each source's publisher and `topology_version`: the baseline a node's top-up installs
    /// atomically before it resumes that source's deltas (gossip.md §3.3). While the answerer is
    /// its Mesh's primary these are the versions it last published into its Mesh, so the deltas it
    /// sends next continue from them.
    #[serde(default)]
    pub sources: Vec<crate::snapshot::SourceSnapshot>,
}
