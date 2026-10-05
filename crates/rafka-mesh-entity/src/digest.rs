//! What a node publishes about itself on the fabric's membership topic.

use crate::ids::FabricId;
use crate::membership::MeshNode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
