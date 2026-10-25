//! The typed objects a caller uses (`node`, `mesh`, `fabric` and `build`), and the node-admin
//! control API's client.
#![deny(missing_docs)]

mod build_stream;
mod calls;
mod objects;
mod peers;
mod workflow;

pub use build_stream::{follow_with, BuildCarrier, BuildFrame, BuildReceipts, BuildStream, Change, Followed, Reattached, StartDisposition};
pub use calls::{CallEnd, ExactBirth, HeldView, NodeRpc, StartContext, StopContext};
pub use rdm_core::names::{BuildOp, FabricOp, MeshOp, NodeEvent, NodeOp, NodeStep};
pub use objects::{
    await_fact, Builds, Fabrics, MeshSelector, Meshes, NameHeld, NodeSelector, Nodes, TagFilter,
};
pub use peers::{birth_of_line, birth_of_view, client_bind_for, Peers, BIRTH_LINE};
/// A node's birth, as a node-admin prints it at start ([`BIRTH_LINE`]).
pub use rdm_core::MeshNode;
pub use workflow::{fold, fold_receipts, fold_steps, Folded, Frame, Resume, WorkflowKind, WorkflowStream};

/// What the reads answer: the views travel on op `0x1B` and the control API answers the same shapes.
pub use rdm_core::views::{ConnectionView, ConnectionsView, DataDirLost, DataDirUnreadable, FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
/// Executable bindings: what each created path's config names (build-deployment.md), and a
/// consumer's per-kind set a caller expands into per-path entries before the typed call.
pub use rdm_core::binding;
pub use rdm_core::NodeKind as LaunchKind;
/// A node's `path.name`, as the typed objects name nodes.
pub use rdm_core::PathName;
/// A Build's id as node-admin returns it: the one Crockford id type (R-BI5), refused by name when
/// it is anything else.
pub use rdm_core::BuildId;
pub use rdm_core::dto::{Accepted, BuildState, BuildView, ClientError, FabricDesired, MeshDesired, StepView};
