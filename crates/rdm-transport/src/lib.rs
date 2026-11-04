//! RDM transport substrate: the Node RPC runtime (client, server, pool, admission), the endpoint,
//! the clock, stream wire framing, and the OTLP span and log exporter over HTTP. Gossip is
//! `rdm-gossip`, built on this crate.
//!
//! # Where it sits
//!
//! Above `rdm-core`, whose types it carries, and below `rdm-gossip`, `rdm-node` and `rdm-admin`,
//! which build a node's process on it. A node serves its ops through a [`ServerBuilder`] and calls a peer's through a
//! [`NodeRpcClient`]; every call ends in one [`rdm_core::RpcOutcome`].
//!
//! # Example
//!
//! A call of the core `ping` op on an exact node, every outcome handled:
//!
//! ```no_run
//! use rdm_core::ping::{Ping, PingReply, PingRequest};
//! use rdm_core::{NodeId, RpcOutcome};
//! use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
//!
//! async fn ping(client: &NodeRpcClient, node: NodeId) -> Option<Vec<u8>> {
//!     let request = PingRequest::Ping { payload: b"hello".to_vec() };
//!     let (outcome, _evidence) = client.call::<Ping>(&NodeTarget::ExactNode(node), &request, &CallOptions::default()).await;
//!     match outcome {
//!         RpcOutcome::Reply(replied) => match replied.into_value() {
//!             PingReply::Pong { payload } => Some(payload),
//!             // A named refusal: NotReady, Busy, ConnectionsDeleting, ...
//!             _ => None,
//!         },
//!         // Never dispatched (NotSent, Unserved, RejectedStale), or Indeterminate: never replayed blindly.
//!         _ => None,
//!     }
//! }
//! ```
//!
//! # Guide
//!
//! [Node RPC runtime](../../guide/architecture/node-rpc.html),
//! and [Node RPC envelope](../../guide/architecture/node-rpc-envelope.html), in the docs site
//! `scripts/docs-site.sh` builds.
#![deny(missing_docs)]

// the pieces rdm-gossip shares with the Node RPC runtime
pub mod chunking;
pub mod clock;
pub mod iroh_obs;
pub mod load;
/// A unit cell's own span capture: this crate's cells, and `rdm-gossip`'s through its `testkit-gossip`
/// dev-dependency feature.
#[cfg(any(test, feature = "testkit-gossip"))]
pub mod test_span_capture;
pub mod wire;

// node-rpc modules & re-exports
pub mod admission;
pub mod client;
pub mod connections;
pub mod delivery;
pub mod endpoint;
pub mod forward;
pub mod pre_node_ready;
pub mod live;
pub mod pool;
pub mod resolve;
pub mod route;
pub mod server;
pub mod stream;

/// The Node RPC ALPN.
pub const ALPN: &[u8] = b"rafka-node-rpc/1";

pub use client::{AcceptTrigger, CallEvidence, CallEnd, CallEndObserver, CallEndsSlot, CallFailedReason, CallOptions, CarriedIdentity, ConnectionObserver, Decode, NodeRpcClient, Send, SendEnd, RoutePolicy, TimeLimit, TRANSPORT_LIMIT};
pub use delivery::{Acceptance, Delivery, DeliverySeams, DeliveryStats, Pacing};
pub use forward::{CarriedReplies, CarrierConnections};
pub use pre_node_ready::{CancelToken, PreNodeReadyGate, BirthCancel};
pub use live::{Applied, LiveNodeResolver, Refusal, DELETED_RETENTION};
pub use pool::{ClosedConnection, Failpoint, PoolKey};
pub use resolve::{NodeResolver, NodeTarget, ResolvedNode, StaticResolver};
pub use route::{ConnectedCall, RouteChoice, RouteLeg};
pub use server::{HandlerFault, LateReply, NodeRpcServer, PeerContext, ReplyWithhold, ServedBirth, ServerBuilder, ServerStats};

// mesh-ops modules & re-exports
pub mod framer;
pub mod ops;
pub use framer::{decode as framer_decode, encode as framer_encode, FramerError, TAG_LEGACY_FRAME};
pub use ops::{InternalMeshFrame, TraceContext, TracedFrame};

// telemetry modules & re-exports
pub mod export;
pub mod logs;
mod log_sink;
mod transport_diag;
pub mod watchdog;
mod pipeline;
mod evidence_capture;
pub mod telemetry;
pub use telemetry::*;
