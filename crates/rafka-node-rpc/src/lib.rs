//! Generic Node RPC runtime (i143 PRD §1.18, §13; node-rpc.md; ownership
//! amendment §6–§9).
//!
//! One QUIC bi-stream is one invocation on the [`ALPN`] protocol. The server
//! dispatches only a complete, cleanly finished request; the client commits
//! only after the complete request was written and its direction finished,
//! resetting an unfinished request with `499 FRAME_NOT_SENT`. Node RPC never
//! chooses a target, never retries and never reroutes.

pub mod admission;
pub mod client;
pub mod endpoint;
pub mod pool;
pub mod resolve;
pub mod server;

/// The Node RPC ALPN.
pub const ALPN: &[u8] = b"rafka-node-rpc/1";

pub use client::{Budget, CallEvidence, CallOptions, Decode, NodeRpcClient};
pub use pool::{Failpoint, PoolKey};
pub use resolve::{NodeResolver, NodeTarget, ResolvedNode, StaticResolver};
pub use server::{HandlerFault, NodeRpcServer, PeerContext, ServerBuilder, ServerStats};
