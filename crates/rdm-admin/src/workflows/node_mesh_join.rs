//! Node mesh join primitive (`docs/architecture/lifecycles/node-mesh-join.md`).
//!
//! Reports to the accepting authority with `JoinNode` on op `0x1D` and enters the mesh
//! channel (and backbone for node-admins).

pub use crate::join::{call_join, call_join_to, Deployed, JoinDoor, JoinFailure, JoinSlot, Joins, Mismatch, Standing};
