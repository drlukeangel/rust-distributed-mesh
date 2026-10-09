//! The Build-facts read on op `0x1F` (node-rpc-envelope.md "Build facts, op `0x1F`, rdm").
//!
//! Direction: peer read, a node-admin to a node-admin. The call is read-only: it mutates nothing at
//! the target and the target decides nothing. The reply is a server stream of the Build facts the
//! target holds locally for one Build, as one consistent snapshot of its own log:
//!
//! ```text
//! Started  Facts*  End { complete }                      a stream
//! NotReady | UnknownBuild                                one refusal, nothing started
//! ```
//!
//! A `Facts` chunk carries `facts`: one postcard frame of the Build topic's message
//! (`rafka-node-admin-core` `fabric_builds::BuildMessage`, the Build facts in their wire shape),
//! packed by the one method the topic's catch-up uses. The facts are types of a crate this contract
//! does not depend on, so they travel as that frame, never as JSON.
//!
//! A caller counts the read as hydration only when `End.complete` is true, every chunk
//! `0..chunk_count` arrived, and the facts decoded equal `End.facts`. A stream that ends without
//! its `End`, an `End` with `complete = false` and a chunk count that disagrees are none of them
//! hydration: the same request may be made again from the beginning, and facts absorbed twice are
//! absorbed once. `complete = false` is the responder saying its own holdings of the Build are not
//! whole (it is not Ready itself, or a fact had no frame to travel in). Nodes that are not
//! node-admins do not serve the op: the framework answers them `421 UNSERVED_OP`.
//!
//! The family is not forwardable: it is a stream.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use serde::{Deserialize, Serialize};

pub struct BuildFacts;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildFactsRequest {
    /// Every fact the target holds for `build_id`.
    FetchBuildFacts { build_id: String },
}

impl BuildFactsRequest {
    pub fn op(&self) -> &'static str {
        match self {
            Self::FetchBuildFacts { .. } => "fetch-build-facts",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildFactsReply {
    /// One chunk of the snapshot: `facts` is one postcard frame of the Build topic's message.
    Facts { build_id: String, chunk_index: u32, chunk_count: u32, facts: Vec<u8> },
    /// The stream is over. `facts` counts the facts sent in `chunks` chunks. `complete` is the
    /// responder's statement that these are all of its holdings of the Build and that they are whole.
    End { build_id: String, facts: u32, chunks: u32, complete: bool },
    /// The target cannot answer yet; `reason` names what it waits for.
    NotReady { reason: String },
    /// The target holds no fact of `build_id`.
    UnknownBuild { build_id: String },
    PeerUnresolved { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    /// The stream begins: sent before the first chunk of an accepted read.
    Started,
}

impl BuildFactsReply {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Facts { .. } => "facts",
            Self::End { .. } => "end",
            Self::NotReady { .. } => "not-ready",
            Self::UnknownBuild { .. } => "unknown-build",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::Draining { .. } => "draining",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::Started => "started",
        }
    }
}

impl NodeProtocol for BuildFacts {
    const OP: u8 = 0x1F;
    const NAME: &'static str = "build-facts";
    /// A Build id.
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    /// One chunk: the Build topic's message bound (`rafka-mesh-transport` chunking) plus the frame's own fields.
    const MAX_REPLY_FRAME_BYTES: usize = 8 * 1024;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 10;

    type Request = BuildFactsRequest;
    type Reply = BuildFactsReply;

    fn classify_reply(reply: &BuildFactsReply) -> ReplyKind {
        match reply {
            BuildFactsReply::Facts { .. } | BuildFactsReply::End { .. } | BuildFactsReply::Started => ReplyKind::Success,
            BuildFactsReply::UnknownBuild { .. } => ReplyKind::ProtocolRefusal,
            BuildFactsReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildFactsReply::NotReady { .. } => ReplyKind::NotReady,
            BuildFactsReply::Busy { .. } => ReplyKind::Busy,
            BuildFactsReply::Draining { .. } => ReplyKind::Draining,
            BuildFactsReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildFactsReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> BuildFactsReply {
        BuildFactsReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> BuildFactsReply {
        BuildFactsReply::NotReady { reason }
    }
    fn busy(reason: String) -> BuildFactsReply {
        BuildFactsReply::Busy { reason }
    }
    fn draining(reason: String) -> BuildFactsReply {
        BuildFactsReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> BuildFactsReply {
        BuildFactsReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> BuildFactsReply {
        BuildFactsReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for BuildFacts {
    fn frame_kind(frame: &BuildFactsReply) -> FrameKind {
        match frame {
            BuildFactsReply::Started => FrameKind::Started,
            BuildFactsReply::Facts { .. } => FrameKind::Data,
            BuildFactsReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(BuildFacts::classify_reply(other)),
        }
    }
}
