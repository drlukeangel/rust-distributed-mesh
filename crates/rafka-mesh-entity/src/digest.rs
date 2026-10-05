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
    /// Kind-specific facts (e.g. election claims), string-keyed.
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// The `extra` key carrying a member's election claim: the unix ms at which
/// this birth became ready for traffic. Set once per birth, never moved.
pub const READY_SINCE: &str = "ready_since_ms";

impl MeshDigest {
    /// This birth's election claim (`READY_SINCE`); `None` when it makes none
    /// or the value is not a number.
    pub fn ready_since(&self) -> Option<u64> {
        self.extra.get(READY_SINCE).and_then(|v| v.parse().ok())
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("digest serializes")
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}
