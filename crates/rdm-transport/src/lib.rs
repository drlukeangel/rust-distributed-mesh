//! RDM transport substrate: gossip on iroh (topology digest view, frames, topology snapshots),
//! the Node RPC runtime (client, server, pool, admission), stream wire framing, and the OTLP span
//! and log exporter over HTTP.
#![deny(missing_docs)]

// gossip & topology modules
pub mod chunking;
pub mod envelope;
pub mod gossip;
pub(crate) mod forward_path;
pub mod clock;
pub(crate) mod discipline;
pub mod iroh_obs;
pub mod load;
pub mod topology;
pub(crate) mod receive_order;
pub mod snapshot;
#[cfg(test)]
mod test_span_capture;
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

pub use client::{AcceptTrigger, Budget, CallEvidence, CallOptions, CarriedIdentity, ConnectionObserver, Decode, NodeRpcClient, RoutePolicy};
pub use delivery::{Acceptance, Delivery, DeliverySeams, DeliveryStats, Pacing};
pub use forward::{CarriedReplies, CarrierEdges};
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
