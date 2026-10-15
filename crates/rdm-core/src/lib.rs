//! RDM core: ids and identity, the Fabric → Mesh → Node model, membership and `RuntimeFact` types,
//! the Node RPC protocol contract (framing, outcomes, reserved codes, body schemas, views, DTOs),
//! and canonical operation/span names. Depends on no transport and no runtime.
#![deny(missing_docs)]

pub mod binding;
pub mod build;
pub mod build_claim;
pub mod build_facts;
pub mod cadence;
pub mod catalog;
pub mod certs;
pub mod codes;
pub mod connections;
pub mod context;
pub mod digest;
pub mod dispatch;
pub mod dto;
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
pub mod outcome;
pub mod path;
pub mod ping;
pub mod protocol;
pub mod publisher;
pub mod reconnect;
pub mod runtime;
pub mod seat;
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
    CarrierChoice, CarrierPolicy, ConnectionEnd, ConnectionIndex, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery,
    EffectiveRoute, NodeConnection, RouteResolution,
};
pub use digest::{check_config, ConfigOverCap, GossipStats, MemberStatus, MeshDigest, MeshNode, NodeLoad, NodeState, MAX_CONFIG_BYTES, MAX_CONFIG_KEYS};
pub use dto::{Accepted, BuildState, BuildView, ClientError, FabricDesired, MeshDesired, StepView};
#[doc(hidden)]
pub use ids::fixture_crockford60;
pub use ids::{BuildId, BuildIdLog, ConnectionId, DeploymentId, EndpointId, FabricId, IdError, IncarnationId, MeshId, NodeId, CROCKFORD60_LEN, ID_FORMAT};
pub use lifecycle::{LifecycleOp, RoundHook, DEPARTED_RETENTION};
pub use names::{BuildOp, FabricOp, MeshOp, NodeEvent, NodeOp, NodeStep};
pub use outcome::{Committed, IndeterminateReason, MalformedKind, NotSentReason, ReplyKind, RequestFinished, RpcOutcome};
pub use outcome::PreCommit;
pub use path::{NodeKind, PathName, PathNameError};
pub use protocol::{DecodeFailure, NodeProtocol};
pub use publisher::PublisherId;
pub use runtime::{RuntimeFact, RuntimeFactError, RuntimeLocator, RuntimeProvider};
pub use seat::{Seat, SeatHolder};
