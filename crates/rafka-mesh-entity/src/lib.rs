//! The generic Mesh entity framework (i143 PRD §19 e4; ownership §5.1).
//!
//! Eventually-convergent operational mesh facts only: exact-node identity
//! (logical node id + process incarnation), membership, and endpoint-slot
//! freshness. Incarnations and freshness tokens are opaque and compared by
//! equality/supersession, never ordered. No Application EF, no product state.

pub mod digest;
pub mod endpoint;
pub mod launch;
pub mod ids;
pub mod membership;
pub mod path;

pub use digest::{MemberStatus, MeshDigest, READY_SINCE};
pub use endpoint::{EndpointSet, EndpointSlot, SlotPolicy};
pub use ids::{FabricId, FreshnessToken, IdError, IncarnationId, MeshId, NodeId, TransportId, ID_FORMAT};
pub use membership::{Change, Freshness, MeshNode, Membership, MembershipRefusal, Resolution};
pub use path::{NodeKind, PathName, PathNameError};
