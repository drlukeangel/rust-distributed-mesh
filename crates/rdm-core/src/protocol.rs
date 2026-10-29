//! The protocol family trait (node-rpc.md §8, §25–§26).
//!
//! Node RPC is a runtime, not a wire protocol: every family owns one stable
//! op and its codec. Requests and replies are top-level enums whose variants
//! are append-only; a discriminant past the declared variant count is
//! `UnknownVariant`, anything else undecodable is `Corrupt`.
//!
//! # The ops
//!
//! Every Node RPC op RDM declares, by op code. Each request and reply body is an enum whose variants
//! are in declaration order, which is their postcard position: each variant's documentation names its
//! position, and the fields of a variant or a carried struct travel in declaration order. The
//! testkit's own ops (`0x40`, `0x70`–`0x75`) are listed on `rdm_test`'s front page.
//!
//! | op | name | protocol | request | reply |
//! |---|---|---|---|---|
//! | `0x01` | `ping` | [`Ping`](crate::ping::Ping) | [`PingRequest`](crate::ping::PingRequest) | [`PingReply`](crate::ping::PingReply) |
//! | `0x1A` | `forward` | [`Forward`](crate::forward::Forward) | [`ForwardRequest`](crate::forward::ForwardRequest) | [`ForwardReply`](crate::forward::ForwardReply) |
//! | `0x1B` | `status` | [`Status`](crate::status::Status) | [`StatusRequest`](crate::status::StatusRequest) | [`StatusReply`](crate::status::StatusReply) |
//! | `0x1C` | `build-claim` | [`BuildClaim`](crate::build_claim::BuildClaim) | [`BuildClaimRequest`](crate::build_claim::BuildClaimRequest) | [`BuildClaimReply`](crate::build_claim::BuildClaimReply) |
//! | `0x1D` | `join` | [`Join`](crate::join::Join) | [`JoinRequest`](crate::join::JoinRequest) | [`JoinReply`](crate::join::JoinReply) |
//! | `0x1E` | `topology` | [`Topology`](crate::topology::Topology) | [`TopologyRequest`](crate::topology::TopologyRequest) | [`TopologyReply`](crate::topology::TopologyReply) |
//! | `0x1F` | `build-facts` | [`BuildFacts`](crate::build_facts::BuildFacts) | [`BuildFactsRequest`](crate::build_facts::BuildFactsRequest) | [`BuildFactsReply`](crate::build_facts::BuildFactsReply) |
//! | `0x20` | `build` | [`Build`](crate::build::Build) | [`BuildRequest`](crate::build::BuildRequest) | [`BuildReply`](crate::build::BuildReply) |
//! | `0x21` | `node-primary-update` | [`NodePrimaryUpdate`](crate::handover::NodePrimaryUpdate) | [`NodePrimaryUpdateRequest`](crate::handover::NodePrimaryUpdateRequest) | [`NodePrimaryUpdateReply`](crate::handover::NodePrimaryUpdateReply) |
//! | `0x22` | `node-command` | [`NodeCommand`](crate::node_command::NodeCommand) | [`NodeCommandRequest`](crate::node_command::NodeCommandRequest) | [`NodeCommandReply`](crate::node_command::NodeCommandReply) |
//! | `0x23` | `read` | [`Read`](crate::read::Read) | [`ReadRequest`](crate::read::ReadRequest) | [`ReadReply`](crate::read::ReadReply) |

use crate::framing::{decode_varint, Varint};
use crate::outcome::{MalformedKind, ReplyKind};
use serde::{de::DeserializeOwned, Serialize};

/// Why a frame did not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeFailure {
    /// The frame names a variant this build does not know.
    UnknownVariant,
    /// The bytes do not decode.
    Corrupt,
}

impl From<DecodeFailure> for MalformedKind {
    fn from(d: DecodeFailure) -> Self {
        match d {
            DecodeFailure::UnknownVariant => MalformedKind::UnknownVariant,
            DecodeFailure::Corrupt => MalformedKind::Corrupt,
        }
    }
}

/// Why a value did not encode, with the encoder's reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeFailure(pub String);

/// Decode a postcard top-level enum that has `variants` variants.
pub(crate) fn decode_enum<T: DeserializeOwned>(bytes: &[u8], variants: u32) -> Result<T, DecodeFailure> {
    match decode_varint(bytes) {
        Varint::Complete { value, .. } if value >= u64::from(variants) => Err(DecodeFailure::UnknownVariant),
        Varint::Complete { .. } => match postcard::from_bytes::<T>(bytes) {
            Ok(v) => Ok(v),
            Err(_) => Err(DecodeFailure::Corrupt),
        },
        _ => Err(DecodeFailure::Corrupt),
    }
}

/// Encode `v` as postcard.
pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, EncodeFailure> {
    postcard::to_allocvec(v).map_err(|e| EncodeFailure(e.to_string()))
}

/// One protocol family.
pub trait NodeProtocol: 'static + Send + Sync {
    /// The protocol's op tag.
    const OP: u8;
    /// The protocol's catalog name.
    const NAME: &'static str;
    /// The largest request frame, in bytes.
    const MAX_REQUEST_FRAME_BYTES: usize;
    /// The largest reply frame, in bytes.
    const MAX_REPLY_FRAME_BYTES: usize;
    /// Carried execution through a peer is opt-in (node-rpc.md §36).
    const FORWARDABLE: bool = false;
    /// Served while the node is connections-deleting: a connections-deleting node refuses every new
    /// call except the lifecycle control its authority sends it (the status family), which must
    /// still reach it.
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = false;
    /// Number of `Request` / `Reply` enum variants this build knows.
    const REQUEST_VARIANTS: u32;
    /// Number of reply enum variants this build knows.
    const REPLY_VARIANTS: u32;

    /// The request type.
    type Request: Serialize + DeserializeOwned + Send + Sync;
    /// The reply type.
    type Reply: Serialize + DeserializeOwned + Send + Sync;

    /// How a reply is classified.
    fn classify_reply(reply: &Self::Reply) -> ReplyKind;

    /// The typed result or refusal name recorded beside its standard code.
    fn reply_detail(reply: &Self::Reply) -> &'static str;

    /// Whether `req` only reads (node-rpc.md §43): its acceptance needs no new write, so the runtime
    /// acknowledges it as it is dispatched. A request that changes anything is acknowledged once its
    /// handler has made the accepted work recoverable, or by its first response frame.
    fn read_only(_req: &Self::Request) -> bool {
        false
    }

    /// The reply for an unresolvable peer.
    fn peer_unresolved(reason: String) -> Self::Reply;
    /// The reply for a node that is not ready.
    fn not_ready(reason: String) -> Self::Reply;
    /// The reply for a node at its admission bound.
    fn busy(reason: String) -> Self::Reply;
    /// The reply for a connections-deleting node.
    fn connections_deleting(reason: String) -> Self::Reply;
    /// The reply for a malformed request frame.
    fn malformed(kind: MalformedKind) -> Self::Reply;
    /// The reply for an unauthorized caller.
    fn unauthorized(reason: String) -> Self::Reply;

    /// Encode a request.
    fn encode_request(req: &Self::Request) -> Result<Vec<u8>, EncodeFailure> {
        encode(req)
    }
    /// Decode a request.
    fn decode_request(bytes: &[u8]) -> Result<Self::Request, DecodeFailure> {
        decode_enum(bytes, Self::REQUEST_VARIANTS)
    }
    /// Encode a reply.
    fn encode_reply(reply: &Self::Reply) -> Result<Vec<u8>, EncodeFailure> {
        encode(reply)
    }
    /// Decode a reply.
    fn decode_reply(bytes: &[u8]) -> Result<Self::Reply, DecodeFailure> {
        decode_enum(bytes, Self::REPLY_VARIANTS)
    }
}

/// Declare a protocol's variants inside its `impl NodeProtocol` block: the request enum's variants
/// and the reply enum's, each list in its enum's declaration order (its postcard position), with
/// `@reserved` where the reply enum declares the six reserved variants (Node RPC envelope,
/// Section 2), in this order: `PeerUnresolved { reason: String }`, `NotReady { reason: String }`,
/// `Busy { reason: String }`, `ConnectionsDeleting { reason: String }`,
/// `Malformed { kind: MalformedKind }`, `Unauthorized { reason: String }`.
///
/// It writes `REQUEST_VARIANTS` and `REPLY_VARIANTS` (each list's length, `@reserved` six),
/// `classify_reply` and `reply_detail` (each application variant's own code and name, written
/// `Variant = "name" => code`; each reserved variant's standard code and name) and the six reserved
/// constructors. The enums stay the application's own, declared beside it with every variant
/// written out: the helper adds no variant and moves none. Its matches carry no wildcard arm, so a
/// variant declared and not listed, or listed and not declared, does not compile.
///
/// ```
/// use rdm_core::outcome::{Code, MalformedKind, ReplyKind};
/// use rdm_core::protocol::NodeProtocol;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// pub enum ShuffleRequest { Record { record: Vec<u8> } }
///
/// #[derive(Serialize, Deserialize)]
/// pub enum ShuffleReply {
///     Recorded { offset: u64 },
///     Stale { current: u64 },
///     PeerUnresolved { reason: String },
///     NotReady { reason: String },
///     Busy { reason: String },
///     ConnectionsDeleting { reason: String },
///     Malformed { kind: MalformedKind },
///     Unauthorized { reason: String },
/// }
///
/// pub struct CgShuffleRecord;
///
/// impl NodeProtocol for CgShuffleRecord {
///     const OP: u8 = 0x25;
///     const NAME: &'static str = "cg-shuffle-record";
///     const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
///     const MAX_REPLY_FRAME_BYTES: usize = 4 * 1024;
///     type Request = ShuffleRequest;
///     type Reply = ShuffleReply;
///
///     rdm_core::protocol_variants! {
///         request ShuffleRequest { Record }
///         reply ShuffleReply {
///             Recorded = "recorded" => ReplyKind::Success,
///             Stale = "stale" => ReplyKind::new(Code::FailedPrecondition),
///             @reserved,
///         }
///     }
/// }
///
/// assert_eq!((CgShuffleRecord::REQUEST_VARIANTS, CgShuffleRecord::REPLY_VARIANTS), (1, 8));
/// ```
///
/// A reply variant declared and not listed (`Stale`) does not compile:
///
/// ```compile_fail
/// use rdm_core::outcome::{MalformedKind, ReplyKind};
/// use rdm_core::protocol::NodeProtocol;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// pub enum ShuffleRequest { Record { record: Vec<u8> } }
///
/// #[derive(Serialize, Deserialize)]
/// pub enum ShuffleReply {
///     Recorded { offset: u64 },
///     Stale { current: u64 },
///     PeerUnresolved { reason: String },
///     NotReady { reason: String },
///     Busy { reason: String },
///     ConnectionsDeleting { reason: String },
///     Malformed { kind: MalformedKind },
///     Unauthorized { reason: String },
/// }
///
/// pub struct CgShuffleRecord;
///
/// impl NodeProtocol for CgShuffleRecord {
///     const OP: u8 = 0x25;
///     const NAME: &'static str = "cg-shuffle-record";
///     const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
///     const MAX_REPLY_FRAME_BYTES: usize = 4 * 1024;
///     type Request = ShuffleRequest;
///     type Reply = ShuffleReply;
///
///     rdm_core::protocol_variants! {
///         request ShuffleRequest { Record }
///         reply ShuffleReply {
///             Recorded = "recorded" => ReplyKind::Success,
///             @reserved,
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! protocol_variants {
    (
        request $req:ident { $($r:ident),* $(,)? }
        reply $rep:ident {
            $( $a:ident = $a_name:literal => $a_code:expr , )*
            @reserved
            $( , $b:ident = $b_name:literal => $b_code:expr )* $(,)?
        }
    ) => {
        const REQUEST_VARIANTS: u32 = {
            // Every request variant is listed: a declared variant the list misses does not compile.
            #[allow(dead_code)]
            fn every_request_variant_listed(request: &$req) {
                match request {
                    $( $req::$r { .. } => {} )*
                }
            }
            <[&str]>::len(&[$(stringify!($r)),*]) as u32
        };
        const REPLY_VARIANTS: u32 = <[&str]>::len(&[
            $(stringify!($a),)*
            "PeerUnresolved", "NotReady", "Busy", "ConnectionsDeleting", "Malformed", "Unauthorized"
            $(, stringify!($b))*
        ]) as u32;

        fn classify_reply(reply: &Self::Reply) -> $crate::outcome::ReplyKind {
            match reply {
                $( $rep::$a { .. } => $a_code, )*
                $rep::PeerUnresolved { .. } => $crate::outcome::ReplyKind::PeerUnresolved,
                $rep::NotReady { .. } => $crate::outcome::ReplyKind::NotReady,
                $rep::Busy { .. } => $crate::outcome::ReplyKind::Busy,
                $rep::ConnectionsDeleting { .. } => $crate::outcome::ReplyKind::ConnectionsDeleting,
                $rep::Malformed { kind } => $crate::outcome::ReplyKind::Malformed(*kind),
                $rep::Unauthorized { .. } => $crate::outcome::ReplyKind::Unauthorized,
                $( $rep::$b { .. } => $b_code, )*
            }
        }

        fn reply_detail(reply: &Self::Reply) -> &'static str {
            match reply {
                $( $rep::$a { .. } => $a_name, )*
                $rep::PeerUnresolved { .. } => "peer-unresolved",
                $rep::NotReady { .. } => "not-ready",
                $rep::Busy { .. } => "busy",
                $rep::ConnectionsDeleting { .. } => "connections-deleting",
                $rep::Malformed { .. } => "malformed",
                $rep::Unauthorized { .. } => "unauthorized",
                $( $rep::$b { .. } => $b_name, )*
            }
        }

        fn peer_unresolved(reason: ::std::string::String) -> Self::Reply {
            $rep::PeerUnresolved { reason }
        }
        fn not_ready(reason: ::std::string::String) -> Self::Reply {
            $rep::NotReady { reason }
        }
        fn busy(reason: ::std::string::String) -> Self::Reply {
            $rep::Busy { reason }
        }
        fn connections_deleting(reason: ::std::string::String) -> Self::Reply {
            $rep::ConnectionsDeleting { reason }
        }
        fn malformed(kind: $crate::outcome::MalformedKind) -> Self::Reply {
            $rep::Malformed { kind }
        }
        fn unauthorized(reason: ::std::string::String) -> Self::Reply {
            $rep::Unauthorized { reason }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outcome::Code;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum ShapeRequest {
        Put { v: u32 },
        Get,
    }

    /// Application variants before and after the six, one of each variant form.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum ShapeReply {
        Done,
        Refused { why: String },
        PeerUnresolved { reason: String },
        NotReady { reason: String },
        Busy { reason: String },
        ConnectionsDeleting { reason: String },
        Malformed { kind: MalformedKind },
        Unauthorized { reason: String },
        Late(u32),
    }

    struct Shape;

    impl NodeProtocol for Shape {
        const OP: u8 = 0x7E;
        const NAME: &'static str = "shape";
        const MAX_REQUEST_FRAME_BYTES: usize = 64;
        const MAX_REPLY_FRAME_BYTES: usize = 64;
        type Request = ShapeRequest;
        type Reply = ShapeReply;

        crate::protocol_variants! {
            request ShapeRequest { Put, Get }
            reply ShapeReply {
                Done = "done" => ReplyKind::Success,
                Refused = "refused" => ReplyKind::new(Code::FailedPrecondition),
                @reserved,
                Late = "late" => ReplyKind::Success,
            }
        }
    }

    /// A stamped reply encodes at `position`, decodes back, and carries its name and code.
    fn stamped(reply: ShapeReply, position: u8, detail: &str, kind: ReplyKind) {
        let bytes = Shape::encode_reply(&reply).expect("a reply encodes");
        assert_eq!(bytes[0], position, "{detail} sits at its position in the reply enum");
        assert_eq!(Shape::decode_reply(&bytes).as_ref(), Ok(&reply), "{detail} decodes to itself");
        assert_eq!(Shape::reply_detail(&reply), detail);
        assert_eq!(Shape::classify_reply(&reply), kind, "{detail}'s code");
    }

    /// CONTRACT: the helper's `peer_unresolved` builds the first reserved variant, at the position
    /// `@reserved` holds, named `peer-unresolved`, classified NotFound.
    #[test]
    fn stamped_peer_unresolved_holds_its_position_and_code() {
        stamped(Shape::peer_unresolved("r".into()), 2, "peer-unresolved", ReplyKind::PeerUnresolved);
        assert_eq!(ReplyKind::PeerUnresolved.code(), Code::NotFound);
    }

    /// CONTRACT: `not_ready` builds the second reserved variant, named `not-ready`, Unavailable.
    #[test]
    fn stamped_not_ready_holds_its_position_and_code() {
        stamped(Shape::not_ready("r".into()), 3, "not-ready", ReplyKind::NotReady);
        assert_eq!(ReplyKind::NotReady.code(), Code::Unavailable);
    }

    /// CONTRACT: `busy` builds the third reserved variant, named `busy`, Unavailable.
    #[test]
    fn stamped_busy_holds_its_position_and_code() {
        stamped(Shape::busy("r".into()), 4, "busy", ReplyKind::Busy);
        assert_eq!(ReplyKind::Busy.code(), Code::Unavailable);
    }

    /// CONTRACT: `connections_deleting` builds the fourth reserved variant, named
    /// `connections-deleting`, Unavailable.
    #[test]
    fn stamped_connections_deleting_holds_its_position_and_code() {
        stamped(Shape::connections_deleting("r".into()), 5, "connections-deleting", ReplyKind::ConnectionsDeleting);
        assert_eq!(ReplyKind::ConnectionsDeleting.code(), Code::Unavailable);
    }

    /// CONTRACT: `malformed` builds the fifth reserved variant, named `malformed`, its code the
    /// malformed kind's own: every kind keeps it.
    #[test]
    fn stamped_malformed_holds_its_position_and_its_kind_code() {
        for (kind, code) in [(MalformedKind::TooLarge, Code::ResourceExhausted), (MalformedKind::TypesOverCap, Code::ResourceExhausted), (MalformedKind::UnknownVariant, Code::InvalidArgument), (MalformedKind::Corrupt, Code::DataLoss)] {
            stamped(Shape::malformed(kind), 6, "malformed", ReplyKind::Malformed(kind));
            assert_eq!(ReplyKind::Malformed(kind).code(), code, "{kind:?}");
        }
    }

    /// CONTRACT: `unauthorized` builds the sixth reserved variant, named `unauthorized`,
    /// PermissionDenied: the codec position only (no mesh receiver sends it).
    #[test]
    fn stamped_unauthorized_holds_its_position_and_code() {
        stamped(Shape::unauthorized("r".into()), 7, "unauthorized", ReplyKind::Unauthorized);
        assert_eq!(ReplyKind::Unauthorized.code(), Code::PermissionDenied);
    }

    /// CONTRACT: the helper counts every variant it lists (`@reserved` six) and keeps each
    /// application variant's own name and code on either side of the six, unit and tuple forms too;
    /// the counts bound decoding.
    #[test]
    fn helper_counts_each_list_and_keeps_the_application_variants() {
        assert_eq!((Shape::REQUEST_VARIANTS, Shape::REPLY_VARIANTS), (2, 9));
        for (reply, position, detail, kind) in [
            (ShapeReply::Done, 0u8, "done", ReplyKind::Success),
            (ShapeReply::Refused { why: "w".into() }, 1, "refused", ReplyKind::new(Code::FailedPrecondition)),
            (ShapeReply::Late(4), 8, "late", ReplyKind::Success),
        ] {
            stamped(reply, position, detail, kind);
        }
        assert_eq!(Shape::decode_reply(&[9]).err(), Some(DecodeFailure::UnknownVariant));
        assert_eq!(Shape::decode_request(&[2]).err(), Some(DecodeFailure::UnknownVariant));
        assert_eq!(Shape::decode_request(&Shape::encode_request(&ShapeRequest::Get).unwrap()), Ok(ShapeRequest::Get));
        assert_eq!(Shape::decode_request(&Shape::encode_request(&ShapeRequest::Put { v: 1 }).unwrap()), Ok(ShapeRequest::Put { v: 1 }));
    }
}
