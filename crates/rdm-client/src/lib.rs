//! The typed objects a caller uses (`node`, `mesh`, `fabric` and `build`), and the node-admin
//! control API's client.
#![deny(missing_docs)]

mod build_stream;
mod calls;
mod objects;
mod peers;
mod workflow;

pub use build_stream::{follow_with, BuildCarrier, BuildFrame, BuildReceipts, BuildStream, Change, Followed, MeshCounts, StartDisposition};
pub use calls::{CallEnd, ExactBirth, HeldView, NodeRpc, StartContext, StopContext};
pub use rdm_core::names::{BuildOp, FabricOp, MeshOp, NodeEvent, NodeOp, NodeStep};
pub use objects::{
    await_fact, BuildPreset, BuildSpec, Builds, Fabrics, MeshSelector, Meshes, NameHeld, NodeSelector, NodeSpec, Nodes, TagFilter, Traffic,
};
pub use peers::{birth_of_line, birth_of_view, Peers, BIRTH_LINE};
/// A node's birth, as a node-admin prints it at start ([`BIRTH_LINE`]).
pub use rdm_core::MeshNode;
pub use workflow::{fold, fold_receipts, fold_steps, Folded, Frame, Resume, WorkflowKind, WorkflowStream};

/// What the reads answer: the views travel on op `0x1B` and the control API answers the same shapes.
pub use rdm_core::views::{ConnectionView, ConnectionsView, DataDirLost, DataDirUnreadable, FabricView, HeldRunning, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
/// The executable-binding contract an operator hands node-admin (`RDM_EXECUTABLE_BINDINGS`).
pub use rdm_core::binding;
pub use rdm_core::NodeKind as LaunchKind;
/// A node's `path.name`, as the typed objects name nodes.
pub use rdm_core::PathName;
/// A Build's id as node-admin returns it: the one Crockford id type (R-BI5), refused by name when
/// it is anything else.
pub use rdm_core::BuildId;
pub use rdm_core::dto::{Accepted, BuildState, BuildView, ClientError, FabricDesired, MeshDesired, StepView};
