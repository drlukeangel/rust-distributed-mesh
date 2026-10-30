//! RDM core: ids and identity, the Fabric → Mesh → Node model, the topology view and `RuntimeFact` types,
//! the Node RPC protocol contract (framing, outcomes, reserved codes, body schemas, views, DTOs),
//! and canonical operation/span names. Depends on no transport and no runtime.
//!
//! # Where it sits
//!
//! `rdm-core` is the bottom of the stack: `rdm-transport` carries its types on iroh, `rdm-node` is the
//! node runtime built on both, `rdm-admin` is node-admin built on `rdm-node`, and `rdm-client` is the
//! typed client a caller outside the fabric uses. `rdm-test` is the test kit an application's cells use.
//!
//! An application's own Node RPC op is a [`protocol::NodeProtocol`]: its opcode, its request and reply
//! enums and their frame ceilings, with [`protocol_variants!`] listing every variant (see its example).
//! A read of the fabric names its rows by property with a [`filter::Filter`].
//!
//! # Example
//!
//! ```
//! use rdm_core::filter::{Entity, Filter};
//! use rdm_core::{NodeKind, PathName};
//!
//! // A node's path.name: its mesh, its kind's segment and its ordinal.
//! let name: PathName = "mesh1.rpc.2".parse().expect("a path.name");
//! assert_eq!((name.mesh.as_str(), name.kind), ("mesh1", NodeKind::RpcNode));
//!
//! // `node.get` of mesh1's ready rpc nodes: AND across pairs, any-of within a pair.
//! let filter = Filter::new().eq("mesh", ["mesh1"]).eq("kind", ["rpc"]).eq("status", ["ready"]);
//! assert_eq!(filter.refusal(Entity::Nodes), None);
//! ```
//!
//! # Guide
//!
//! The architecture pages are the contract this crate's types carry: the
//! [Node RPC envelope](../../guide/architecture/node-rpc-envelope.html) and the
//! [lifecycle index](../../guide/architecture/fabric-mesh-node-lifecycles.html), in the docs site
//! `scripts/docs-site.sh` builds.
#![deny(missing_docs)]

pub mod binding;
pub mod build;
pub mod build_claim;
pub mod cadence;
pub mod catalog;
pub mod certs;
pub mod codes;
pub mod connections;
pub mod custody;
pub mod context;
pub mod digest;
pub mod dispatch;
pub mod delivery;
pub mod dto;
pub mod election;
pub mod filter;
pub mod forward;
pub mod framing;
pub mod handover;
pub mod ids;
pub mod join;
pub mod launch;
pub mod lifecycle;
pub mod meta;
pub mod model;
pub mod names;
pub mod node_command;
pub mod outcome;
pub mod path;
pub mod ping;
pub mod protocol;
pub mod publisher;
pub mod read;
pub mod reconnect;
pub mod reply;
pub mod role;
pub mod runtime;
pub mod status;
pub mod streaming;
pub mod topology;
pub mod views;
pub mod wire;

/// The diagnostic file's name in the node's log directory.
pub const TRANSPORT_DIAG_FILE: &str = "transport-diag.log";

/// The target of the node's own diagnostic lines (dial, accept, bind): routed to the transport-diag sink only.
pub const TRANSPORT_DIAG_TARGET: &str = "transport_diag";

pub use codes::ResetCode;
pub use connections::{
    CarrierPolicy, ConnectionEnd, ConnectionIndex, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery,
    EffectiveRoute, NodeConnection, RouteResolution,
};
pub use digest::{GossipStats, MemberStatus, MeshDigest, MeshNode, NodeLoad, NodeState};
pub use dto::{Accepted, BuildState, BuildView, ClientError, FabricDesired, MeshDesired, StepView};
#[doc(hidden)]
pub use ids::fixture_crockford60;
pub use ids::{BuildId, BuildIdLog, ChangeId, ConnectionId, DeploymentId, EndpointId, FabricId, IdError, IncarnationId, MeshId, NodeId, CROCKFORD60_LEN, ID_FORMAT};
pub use lifecycle::{LifecycleOp, StateHook, DELETED_RETENTION};
pub use names::{BuildOp, FabricOp, MeshOp, NodeEvent, NodeOp, NodeStep};
pub use outcome::{Committed, IndeterminateReason, MalformedKind, NotSentReason, ReplyKind, RequestFinished, RpcOutcome};
pub use outcome::PreCommit;
pub use path::{NodeKind, PathName, PathNameError};
pub use protocol::{DecodeFailure, NodeProtocol};
pub use publisher::PublisherId;
pub use role::{Primary, PrimaryRole};
pub use runtime::{RuntimeFact, RuntimeFactError, RuntimeLocator, RuntimeProvider};

/// App type declaration bounds and held-set identity.
pub mod app_types;
