//! The Build-facts read on op `0x1F` (node-rpc-envelope.md "Build facts, op `0x1F`, rdm").
//!
//! Direction: peer read, a node-admin to a node-admin. The call is read-only: it mutates nothing at
//! the target and the target decides nothing. The reply is a server stream of the Build facts the
//! target holds locally for one Build, as one consistent snapshot of its own log:
//!
//! ```text
//! Started  Facts*  End { complete }                      a stream
//! NotReady | UnknownBuild | NoCurrentBuild               one refusal, nothing started
//! ```
//!
//! `FetchBuildFacts` names the Build. `FetchCurrentBuild` names none: the target streams the facts of
//! its current accepted Build (its builds store read last in, first out, R-BI3), whose id `End`
//! carries, or answers `NoCurrentBuild` when it holds none. `FetchBuildSection` names the Build and a
//! mesh: the Build's facts with its acceptance as that mesh's section only (Q284). The acceptance never
//! travels whole: the first two carry it as every mesh's section.
//!
//! A `Facts` chunk carries `facts`: one postcard frame of the Build topic's message
//! (`rafka-node-admin-core` `fabric_builds::BuildMessage`, the Build facts in their wire shape),
//! packed by the one method the topic's catch-up uses. The facts are types of a crate this contract
//! does not depend on, so they travel as that frame, never as JSON.
//!
//! A caller counts the read as the fetched Build only when `End.complete` is true, every chunk
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

/// The BuildFacts protocol: a stream of the facts a node holds for one Build.
pub struct BuildFacts;

/// A BuildFacts call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildFactsRequest {
    /// Every fact the target holds for `build_id`.
    ///
    /// Position 0 on the wire.
    FetchBuildFacts {
        /// The Build whose facts are fetched.
        build_id: String,
    },
    /// Every fact the target holds for its current accepted Build, whose id the stream's `End`
    /// carries.
    ///
    /// Position 1 on the wire.
    FetchCurrentBuild,
    /// One mesh's section of a Build (Q284): the facts the target holds for the Build, its acceptance
    /// as `mesh`'s section only. What an admin starting, or a mesh-primary taking its role, reads of
    /// the Build; only the fabric-primary holds the acceptance whole.
    ///
    /// Position 2 on the wire.
    FetchBuildSection {
        /// The Build whose section is fetched; `None` the target's current accepted Build, whose id the
        /// stream's `End` carries (`NoCurrentBuild` when it holds none).
        build_id: Option<String>,
        /// The mesh whose section is fetched.
        mesh: String,
    },
}

impl BuildFactsRequest {
    /// The request's operation name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::FetchBuildFacts { .. } => "fetch-build-facts",
            Self::FetchCurrentBuild => "fetch-current-build",
            Self::FetchBuildSection { .. } => "fetch-build-section",
        }
    }
}

/// One frame of a BuildFacts read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildFactsReply {
    /// One chunk of the snapshot: `facts` is one postcard frame of the Build topic's message.
    ///
    /// Position 0 on the wire.
    Facts {
        /// The Build the chunk belongs to.
        build_id: String,
        /// This chunk's index.
        chunk_index: u32,
        /// The number of chunks.
        chunk_count: u32,
        /// One postcard frame of the Build topic's message.
        facts: Vec<u8>,
    },
    /// The stream is over. `facts` counts the facts sent in `chunks` chunks. `complete` is the
    /// responder's statement that these are all of its holdings of the Build and that they are whole.
    ///
    /// Position 1 on the wire.
    End {
        /// The Build read.
        build_id: String,
        /// The number of facts sent.
        facts: u32,
        /// The number of chunks sent.
        chunks: u32,
        /// Whether the responder holds no other fact of the Build and these are whole.
        complete: bool,
    },
    /// The target cannot answer yet; `reason` names what it waits for.
    /// The target is not ready to serve.
    ///
    /// Position 2 on the wire.
    NotReady {
        /// What the target waits for.
        reason: String,
    },
    /// The target holds no fact of `build_id`.
    ///
    /// Position 3 on the wire.
    UnknownBuild {
        /// The Build requested.
        build_id: String,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 4 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The target is at its admission bound.
    ///
    /// Position 5 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The target is connections-deleting and takes no new work.
    ///
    /// Position 6 on the wire.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 7 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 8 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// The stream begins: sent before the first chunk of an accepted read.
    ///
    /// Position 9 on the wire.
    Started,
    /// `FetchCurrentBuild` found no current Build: no Build in the target's builds store is in
    /// progress or completed.
    ///
    /// Position 10 on the wire.
    NoCurrentBuild {
        /// Why the target holds none.
        reason: String,
    },
}

impl BuildFactsReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Facts { .. } => "facts",
            Self::End { .. } => "end",
            Self::NotReady { .. } => "not-ready",
            Self::UnknownBuild { .. } => "unknown-build",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::Started => "started",
            Self::NoCurrentBuild { .. } => "no-current-build",
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
    const REQUEST_VARIANTS: u32 = 3;
    const REPLY_VARIANTS: u32 = 11;

    type Request = BuildFactsRequest;
    type Reply = BuildFactsReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            BuildFactsReply::Facts { .. } => "facts",
            BuildFactsReply::End { .. } => "end",
            BuildFactsReply::NotReady { .. } => "not-ready",
            BuildFactsReply::UnknownBuild { .. } => "unknown-build",
            BuildFactsReply::PeerUnresolved { .. } => "peer-unresolved",
            BuildFactsReply::Busy { .. } => "busy",
            BuildFactsReply::ConnectionsDeleting { .. } => "connections-deleting",
            BuildFactsReply::Malformed { .. } => "malformed",
            BuildFactsReply::Unauthorized { .. } => "unauthorized",
            BuildFactsReply::Started => "started",
            BuildFactsReply::NoCurrentBuild { .. } => "no-current-build",
        }
    }

    fn classify_reply(reply: &BuildFactsReply) -> ReplyKind {
        match reply {
            BuildFactsReply::Facts { .. } => ReplyKind::Success,
            BuildFactsReply::End { .. } => ReplyKind::Success,
            BuildFactsReply::NotReady { .. } => ReplyKind::NotReady,
            BuildFactsReply::UnknownBuild { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
            BuildFactsReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildFactsReply::Busy { .. } => ReplyKind::Busy,
            BuildFactsReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            BuildFactsReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildFactsReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            BuildFactsReply::Started  => ReplyKind::Success,
            BuildFactsReply::NoCurrentBuild { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
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
    fn connections_deleting(reason: String) -> BuildFactsReply {
        BuildFactsReply::ConnectionsDeleting { reason }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outcome::Code;

    /// CONTRACT: docs/architecture/node-rpc-envelope.md:1259 "Direction: peer read between node-admins. RDM owns this server stream; it is not forwardable. A non-node-admin answers 421 UNSERVED_OP. Request ceiling 1024 bytes; reply ceiling 8 KiB. There is no cursor. REQUEST_VARIANTS is 3; REPLY_VARIANTS is 11. Positions are fixed in the shown order (FetchBuildFacts=0, FetchCurrentBuild=1, FetchBuildSection=2; Facts=0 through NoCurrentBuild=10), the two last appended, none reordered."
    #[test]
    fn build_facts_protocol_wire_positions_and_stream_classifications_match_contract() {
        assert_eq!(BuildFacts::OP, 0x1F);
        assert_eq!(BuildFacts::NAME, "build-facts");
        assert!(!BuildFacts::FORWARDABLE);
        assert_eq!(BuildFacts::MAX_REQUEST_FRAME_BYTES, 1024);
        assert_eq!(BuildFacts::MAX_REPLY_FRAME_BYTES, 8 * 1024);
        assert_eq!(BuildFacts::REQUEST_VARIANTS, 3);
        assert_eq!(BuildFacts::REPLY_VARIANTS, 11);

        let r0 = BuildFactsRequest::FetchBuildFacts {
            build_id: "cq2tyb03krac".into(),
        };
        let r1 = BuildFactsRequest::FetchCurrentBuild;
        assert_eq!(r0.op(), "fetch-build-facts");
        assert_eq!(r1.op(), "fetch-current-build");

        let b0 = BuildFacts::encode_request(&r0).unwrap();
        let b1 = BuildFacts::encode_request(&r1).unwrap();
        assert_eq!(b0[0], 0);
        assert_eq!(b1[0], 1);
        assert_eq!(BuildFacts::decode_request(&b0).unwrap(), r0);
        assert_eq!(BuildFacts::decode_request(&b1).unwrap(), r1);
        let r2 = BuildFactsRequest::FetchBuildSection { build_id: Some("cq2tyb03krac".into()), mesh: "mesh1".into() };
        assert_eq!(r2.op(), "fetch-build-section");
        let b2 = BuildFacts::encode_request(&r2).unwrap();
        assert_eq!(b2[0], 2);
        assert_eq!(BuildFacts::decode_request(&b2).unwrap(), r2);

        let replies = [
            (
                0u8,
                BuildFactsReply::Facts {
                    build_id: "cq2tyb03krac".into(),
                    chunk_index: 0,
                    chunk_count: 1,
                    facts: vec![1, 2],
                },
                "facts",
                FrameKind::Data,
            ),
            (
                1,
                BuildFactsReply::End {
                    build_id: "cq2tyb03krac".into(),
                    facts: 1,
                    chunks: 1,
                    complete: true,
                },
                "end",
                FrameKind::Terminal,
            ),
            (
                2,
                BuildFactsReply::NotReady {
                    reason: "booting".into(),
                },
                "not-ready",
                FrameKind::Refusal(ReplyKind::NotReady),
            ),
            (
                3,
                BuildFactsReply::UnknownBuild {
                    build_id: "cq2tyb03krac".into(),
                },
                "unknown-build",
                FrameKind::Refusal(ReplyKind::new(Code::NotFound)),
            ),
            (
                4,
                BuildFactsReply::PeerUnresolved {
                    reason: "missing".into(),
                },
                "peer-unresolved",
                FrameKind::Refusal(ReplyKind::PeerUnresolved),
            ),
            (
                5,
                BuildFactsReply::Busy {
                    reason: "full".into(),
                },
                "busy",
                FrameKind::Refusal(ReplyKind::Busy),
            ),
            (
                6,
                BuildFactsReply::ConnectionsDeleting {
                    reason: "closing".into(),
                },
                "connections-deleting",
                FrameKind::Refusal(ReplyKind::ConnectionsDeleting),
            ),
            (
                7,
                BuildFactsReply::Malformed {
                    kind: MalformedKind::Corrupt,
                },
                "malformed",
                FrameKind::Refusal(ReplyKind::Malformed(MalformedKind::Corrupt)),
            ),
            (
                8,
                BuildFactsReply::Unauthorized {
                    reason: "denied".into(),
                },
                "unauthorized",
                FrameKind::Refusal(ReplyKind::Unauthorized),
            ),
            (9, BuildFactsReply::Started, "started", FrameKind::Started),
            (
                10,
                BuildFactsReply::NoCurrentBuild {
                    reason: "empty".into(),
                },
                "no-current-build",
                FrameKind::Refusal(ReplyKind::new(Code::NotFound)),
            ),
        ];

        for (pos, reply, name, expected_kind) in replies {
            assert_eq!(reply.name(), name);
            assert_eq!(BuildFacts::reply_detail(&reply), name);
            assert_eq!(BuildFacts::frame_kind(&reply), expected_kind);
            let encoded = BuildFacts::encode_reply(&reply).unwrap();
            assert_eq!(encoded[0], pos);
            assert_eq!(BuildFacts::decode_reply(&encoded).unwrap(), reply);
        }
    }
}
