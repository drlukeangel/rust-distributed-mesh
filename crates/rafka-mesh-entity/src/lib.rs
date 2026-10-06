//! The generic Mesh entity framework (i143 PRD §19 e4; ownership §5.1).
//!
//! Eventually-convergent operational mesh facts only: exact-node identity
//! (logical node id + process incarnation), membership, and endpoint-slot
//! freshness. Incarnations and freshness tokens are opaque and compared by
//! equality/supersession, never ordered. No Application EF, no product state.

pub mod connections;
pub mod digest;
pub mod endpoint;
pub mod launch;
pub mod ids;
pub mod lifecycle;
pub mod path;
pub mod reconnect;
pub mod runtime;

pub use connections::{
    CarrierChoice, CarrierPolicy, ConnectionEnd, ConnectionIndex, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery,
    EffectiveRoute, NodeConnection, RouteResolution,
};
pub use digest::{MemberStatus, MeshDigest, MeshNode};
pub use lifecycle::{LifecycleOp, DEPARTED_RETENTION};
pub use endpoint::{EndpointSet, EndpointSlot, SlotPolicy};
pub use ids::{FabricId, FreshnessToken, IdError, IncarnationId, MeshId, NodeId, TransportId, ID_FORMAT};
pub use path::{NodeKind, PathName, PathNameError};
pub use runtime::{RuntimeFact, RuntimeFactError, RuntimeLocator, RuntimeProvider};
