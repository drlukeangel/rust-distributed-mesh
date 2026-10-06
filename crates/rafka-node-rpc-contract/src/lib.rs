//! The Node RPC protocol contract (i143 PRD §1.18, §13; node-rpc.md §8–§12,
//! §24–§28; ownership amendment §6–§7, §12).
//!
//! Pure types and codecs: framing, the reserved reset/stop namespace, the
//! local outcome algebra and the protocol trait. It depends on no transport;
//! `rafka-node-rpc` executes this contract over Iroh.

pub mod catalog;
pub mod codes;
pub mod dispatch;
pub mod echo;
pub mod forward;
pub mod framing;
pub mod outcome;
pub mod protocol;
pub mod streaming;

pub use codes::ResetCode;
pub use outcome::{Committed, IndeterminateReason, NotSentReason, PreCommit, ReplyKind, MalformedKind, RequestFinished, RpcOutcome};
pub use protocol::{DecodeFailure, NodeProtocol};
