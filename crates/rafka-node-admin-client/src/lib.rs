//! The typed objects a caller uses (`node`, `mesh`, `fabric` and `build`), and the node-admin
//! control API's client.
//!
//! The objects speak Node RPC: workflows go to the fabric-primary's Build family (op `0x20`), and
//! the reads (`node.get`, `mesh.get`, `fabric.get`, `node.connections.get`) are Status calls (op
//! `0x1B`) answered by the node asked, whose answers are the views of
//! `rafka_node_rpc_contract::views`. [`NodeAdminClient`] is the control API's client: every
//! topology change it makes is a Build request answered with the Build's id, a refusal keeps
//! node-admin's named reason (`{"error", "detail"}`) and HTTP status, and its reads decode into the
//! same views (`rafka-node-admin-core`'s `client_contract` test pins that every route does).
#![deny(missing_docs)]

mod build_stream;
mod calls;
mod names;
mod objects;
mod peers;
mod workflow;

pub use build_stream::{BuildCarrier, BuildFrame, BuildReceipts, BuildStream, Change, MeshCounts, StartDisposition};
pub use calls::{CallEnd, ExactBirth, HeldView, NodeRpc, StartContext, StopContext};
pub use names::{BuildOp, FabricOp, MeshOp, NodeEvent, NodeOp, NodeStep};
pub use objects::{
    await_fact, BuildPreset, BuildSpec, Builds, Fabrics, MeshSelector, Meshes, NameHeld, NodeSelector, NodeSpec, Nodes, TagFilter, Traffic,
};
pub use peers::{birth_of_line, birth_of_view, Peers, BIRTH_LINE};
/// A node's birth, as a node-admin prints it at start ([`BIRTH_LINE`]).
pub use rafka_mesh_entity::MeshNode;
pub use workflow::{fold, fold_receipts, fold_steps, Folded, Frame, Resume, WorkflowKind, WorkflowStream};


use rafka_mesh_entity::NodeKind;
/// What the reads answer: the views travel on op `0x1B` and the control API answers the same shapes.
pub use rafka_node_rpc_contract::views::{ConnectionView, ConnectionsView, DataDirLost, DataDirUnreadable, FabricView, HeldRunning, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
/// The executable-binding contract an operator hands node-admin (`RDM_EXECUTABLE_BINDINGS`).
pub use rafka_mesh_entity::binding;
pub use rafka_mesh_entity::NodeKind as LaunchKind;
/// A node's `path.name`, as the typed objects name nodes.
pub use rafka_mesh_entity::PathName;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A Build's id as node-admin returns it: the one Crockford id type (R-BI5), refused by name when
/// it is anything else.
pub use rafka_mesh_entity::BuildId;

/// One mesh's desired counts (`POST /api/meshes`, and inside a fabric Build).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDesired {
    /// The mesh's name.
    pub name: String,
    /// The number of node-admins.
    pub node_admin: u32,
    /// The number of rpc nodes.
    #[serde(default)]
    pub rpc_node: u32,
    /// The number of brokers.
    #[serde(default)]
    pub broker: u32,
    /// The number of gateways.
    #[serde(default)]
    pub gateway: u32,
    /// The number of compute nodes.
    #[serde(default)]
    pub compute: u32,
}

impl MeshDesired {
    /// A mesh named `name` with `counts` (`rafka_mesh_entity::NodeKind`, count); every other kind at 0.
    pub fn of(name: impl Into<String>, counts: impl IntoIterator<Item = (NodeKind, u32)>) -> Self {
        let mut d = MeshDesired { name: name.into(), node_admin: 0, rpc_node: 0, broker: 0, gateway: 0, compute: 0 };
        for (k, n) in counts {
            match k {
                NodeKind::NodeAdmin => d.node_admin = n,
                NodeKind::RpcNode => d.rpc_node = n,
                NodeKind::Broker => d.broker = n,
                NodeKind::Gateway => d.gateway = n,
                NodeKind::Compute => d.compute = n,
            }
        }
        d
    }
}

/// The whole fabric's desired meshes (`POST /api/build`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricDesired {
    /// The fabric's name.
    pub fabric: String,
    /// The fabric's meshes with their desired counts.
    pub meshes: Vec<MeshDesired>,
}

/// Where a Build is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// Accepted and not yet executed.
    Pending,
    /// An attempt is executing.
    Running,
    /// Every step is complete.
    Complete,
    /// The current attempt failed.
    Failed,
}

/// One step receipt of a Build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepView {
    /// The attempt the receipt belongs to.
    pub attempt: u32,
    /// The operation the step belongs to.
    pub operation: String,
    /// The step.
    pub step: String,
    /// `"complete"` or `{"failed": {"reason"}}`.
    pub outcome: serde_json::Value,
}

/// The folded Build view (`GET /api/builds?id=`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildView {
    /// The Build's id.
    pub build_id: BuildId,
    /// The complete accepted topology this Build realizes: per mesh, its node `path.name`s.
    pub topology: serde_json::Value,
    /// The change that produced it, tagged by `kind` (`add_node`, `remove_node`, ...): history.
    #[serde(default)]
    pub submitted_change: Option<serde_json::Value>,
    /// When the Build was submitted, in milliseconds since the Unix epoch.
    pub submitted_at_ms: u64,
    /// Where the Build is in its life.
    pub state: BuildState,
    /// The current attempt, counting from 1.
    pub attempt: u32,
    /// The executor running the current attempt, when one is.
    pub executor: Option<String>,
    /// The step receipts of the Build.
    pub steps: Vec<StepView>,
    /// The reason the last attempt failed, when one did.
    pub last_failure: Option<String>,
    /// Why the current attempt exists (`requested`, `proven-drift`, `restart`, `recipe`, ...).
    pub reason: String,
    /// The fenced action the current attempt carries, if any.
    #[serde(default)]
    pub action: Option<serde_json::Value>,
}

/// Why a call did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Node-admin refused, with its status and named reason.
    /// Node-admin refused the call.
    Refused {
        /// The HTTP status of the refusal.
        status: u16,
        /// The named reason.
        error: String,
        /// The detail node-admin gave with the reason.
        detail: String,
    },
    /// Node-admin could not be reached or answered something undecodable.
    /// The call did not complete.
    Transport {
        /// The URL that was called.
        url: String,
        /// Why the call failed.
        reason: String,
        /// Whether the request was written to the connection. `false` only when the
        /// connection was never made, so the request provably reached no handler.
        request_written: bool,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { status, error, detail } => write!(f, "node-admin refused ({status} {error}): {detail}"),
            Self::Transport { url, reason, .. } => write!(f, "node-admin at {url}: {reason}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// The 202 of a request that opens an attempt: the Build and the attempt this request opened
/// (attempt 1 for a Build it accepted). Wait for exactly this attempt, never for the Build alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accepted {
    /// The Build the request opened or joined.
    pub build_id: BuildId,
    /// The attempt the request opened.
    pub attempt: u32,
}

#[cfg(test)]
mod tests {
    use super::BuildId;

    /// R-BI5: a Build id the client reads is a Crockford id; a stored `bld-<24 hex>` value is refused by name.
    #[test]
    fn a_stored_bld_hex_build_id_is_refused_by_name() {
        let old = "bld-0123456789abcdef01234567";
        let e = serde_json::from_str::<BuildId>(&format!("\"{old}\"")).unwrap_err().to_string();
        assert!(e.contains("build id") && e.contains(old), "{e}");
    }
}
