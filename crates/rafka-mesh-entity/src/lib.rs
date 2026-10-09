//! The generic Mesh entity framework (i143 PRD §19 e4; ownership §5.1).
//!
//! Eventually-convergent operational mesh facts only: exact-node identity
//! (logical node id + process incarnation) and membership. Incarnations are
//! opaque and compared by equality/supersession, never ordered. No Application
//! EF, no product state.
#![deny(missing_docs)]


pub mod binding;
pub mod connections;
pub mod digest;
pub mod launch;
pub mod meta;
pub mod ids;
pub mod lifecycle;
pub mod path;
pub mod publisher;
pub mod reconnect;
pub mod runtime;
pub mod seat;
pub mod wire;

pub use connections::{
    CarrierChoice, CarrierPolicy, ConnectionEnd, ConnectionIndex, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery,
    EffectiveRoute, NodeConnection, RouteResolution,
};
pub use digest::{MemberStatus, MeshDigest, MeshNode, NodeLoad};
pub use lifecycle::{LifecycleOp, DEPARTED_RETENTION};
pub use ids::{EndpointId, FabricId, IdError, IncarnationId, MeshId, NodeId, ID_FORMAT};
pub use publisher::PublisherId;
pub use path::{NodeKind, PathName, PathNameError};
pub use seat::{Seat, SeatHolder};
pub use runtime::{RuntimeFact, RuntimeFactError, RuntimeLocator, RuntimeProvider};
