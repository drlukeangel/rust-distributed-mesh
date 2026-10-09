//! The protocol family trait (node-rpc.md §8, §25–§26).
//!
//! Node RPC is a runtime, not a wire protocol: every family owns one stable
//! op and its codec. Requests and replies are top-level enums whose variants
//! are append-only; a discriminant past the declared variant count is
//! `UnknownVariant`, anything else undecodable is `Corrupt`.

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
    /// Served while the node drains: a draining node refuses every new call except the
    /// lifecycle control its authority sends it (the status family), which must still reach it.
    const SERVED_WHILE_DRAINING: bool = false;
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

    /// The reply for an unresolvable peer.
    fn peer_unresolved(reason: String) -> Self::Reply;
    /// The reply for a node that is not ready.
    fn not_ready(reason: String) -> Self::Reply;
    /// The reply for a node at its admission bound.
    fn busy(reason: String) -> Self::Reply;
    /// The reply for a draining node.
    fn draining(reason: String) -> Self::Reply;
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
