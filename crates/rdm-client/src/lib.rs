//! The typed objects a caller uses (`node`, `mesh`, `fabric` and `build`), and the node-admin
//! control API's client.
//!
//! # Where it sits
//!
//! On `rdm-core` and `rdm-transport`, beside the fabric: a process outside it (a tool, a test, an
//! application's control plane) reaches a node-admin by its birth and reads and changes the fabric
//! through the typed objects ([`Nodes`], [`Meshes`], [`Fabrics`], [`BuildCarrier`]).
//!
//! # Example
//!
//! A node-admin prints its birth on its stdout ([`BIRTH_LINE`]); a caller enters there and reads the
//! ready nodes of one mesh:
//!
//! ```no_run
//! use rdm_client::{birth_of_line, Filter, NodeView, Nodes, Peers};
//!
//! async fn ready_nodes(admin_stdout_line: &str) -> Result<Vec<NodeView>, String> {
//!     let birth = birth_of_line(admin_stdout_line).ok_or("not a birth line")??;
//!     let peers = Peers::at(&birth).await?;
//!     let nodes = Nodes::new(peers.carrier_at(birth.name.clone()));
//!     nodes.get(&Filter::new().eq("mesh", ["mesh1"]).eq("status", ["ready"])).await.map_err(|e| e.to_string())
//! }
//! ```
//!
//! # Guide
//!
//! [Query](../../guide/architecture/query.html), [Build](../../guide/architecture/build.html) and the
//! [lifecycle index](../../guide/architecture/fabric-mesh-node-lifecycles.html), in the docs site
//! `scripts/docs-site.sh` builds.
#![deny(missing_docs)]

mod build_stream;
mod calls;
mod objects;
mod peers;
mod workflow;

/// The Build family client, stream and reattachment types ([`BuildCarrier`], [`BuildStream`], [`follow_with`]).
pub use build_stream::{follow_with, Build, BuildCarrier, BuildFrame, BuildStream, Change, Followed, Reattached, StartDisposition};
/// Single-hop Node RPC calls to an exact birth and their typed call endings ([`NodeRpc`], [`ExactBirth`], [`CallEnd`]).
pub use calls::{CallEnd, ExactBirth, HeldView, NodeRpc, StartContext, StopContext};
/// The canonical operation, step and event names of the fabric, mesh, node and Build lifecycles.
pub use rdm_core::names::{BuildOp, FabricOp, MeshOp, NodeEvent, NodeOp, NodeStep};
/// The typed `node`, `mesh`, `fabric`, `connections` and `build` objects.
pub use objects::{
    await_fact, Builds, Connections, Fabrics, Meshes, NameHeld, Nodes,
};
/// The caller's peer-location table and birth-line entry helpers ([`Peers`], [`BIRTH_LINE`]).
pub use peers::{birth_of_line, birth_of_view, client_bind_for, Peers, BIRTH_LINE};
/// A node's birth, as a node-admin prints it at start ([`BIRTH_LINE`]).
pub use rdm_core::MeshNode;
/// A node workflow's reply stream and receipt fold ([`WorkflowStream`], [`WorkflowKind`], [`fold`]).
pub use workflow::{fold, fold_receipts, fold_steps, Folded, Frame, Resume, WorkflowKind, WorkflowStream};

/// What the reads answer: the views travel on op `0x1B` and the control API answers the same shapes.
/// A read's filter (Q167): `(property, values)` pairs, AND across pairs, any-of within.
pub use rdm_core::filter::{Entity, Filter};
/// The fabric, mesh, node, connection and atlas views that the query ops return.
pub use rdm_core::views::{AtlasNodeView, AtlasView, ConnectionView, ConnectionsView, DataDirLost, DataDirUnreadable, FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
/// Executable bindings: what each created path's config names (build-deployment.md), and a
/// consumer's per-kind set a caller expands into per-path entries before the typed call.
pub use rdm_core::binding;
/// The node kind a `path.name` carries (`admin`, `gateway`, `broker`, `compute`).
pub use rdm_core::NodeKind as LaunchKind;
/// A node's `path.name`, as the typed objects name nodes.
pub use rdm_core::PathName;
/// A Build's id as node-admin returns it: the one Crockford id type (R-BI5), refused by name when
/// it is anything else.
pub use rdm_core::BuildId;
/// The Build and topology request and view shapes shared with the control API.
pub use rdm_core::dto::{Accepted, BuildState, BuildView, ClientError, FabricDesired, MeshDesired, StepView};
