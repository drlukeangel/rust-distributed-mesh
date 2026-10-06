//! The protocol family trait (node-rpc.md §8, §25–§26).
//!
//! Node RPC is a runtime, not a wire protocol: every family owns one stable
//! tag and its codec. Requests and replies are top-level enums whose variants
//! are append-only; a discriminant past the declared variant count is
//! `UnknownVariant`, anything else undecodable is `Corrupt`.

use crate::framing::{decode_varint, Varint};
use crate::outcome::{MalformedKind, ReplyKind};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeFailure {
    UnknownVariant,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeFailure(pub String);

/// Decode a postcard top-level enum that has `variants` variants.
pub fn decode_enum<T: DeserializeOwned>(bytes: &[u8], variants: u32) -> Result<T, DecodeFailure> {
    match decode_varint(bytes) {
        Varint::Complete { value, .. } if value >= u64::from(variants) => Err(DecodeFailure::UnknownVariant),
        Varint::Complete { .. } => match postcard::from_bytes::<T>(bytes) {
            Ok(v) => Ok(v),
            Err(_) => Err(DecodeFailure::Corrupt),
        },
        _ => Err(DecodeFailure::Corrupt),
    }
}

pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, EncodeFailure> {
    postcard::to_allocvec(v).map_err(|e| EncodeFailure(e.to_string()))
}

/// One protocol family.
pub trait NodeProtocol: 'static + Send + Sync {
    const TAG: u8;
    const NAME: &'static str;
    const MAX_REQUEST_FRAME_BYTES: usize;
    const MAX_REPLY_FRAME_BYTES: usize;
    /// Carried execution through a peer is opt-in (node-rpc.md §36).
    const FORWARDABLE: bool = false;
    /// Number of `Request` / `Reply` enum variants this build knows.
    const REQUEST_VARIANTS: u32;
    const REPLY_VARIANTS: u32;

    type Request: Serialize + DeserializeOwned + Send + Sync;
    type Reply: Serialize + DeserializeOwned + Send + Sync;

    fn classify_reply(reply: &Self::Reply) -> ReplyKind;

    fn peer_unresolved(reason: String) -> Self::Reply;
    fn not_ready(reason: String) -> Self::Reply;
    fn busy(reason: String) -> Self::Reply;
    fn draining(reason: String) -> Self::Reply;
    fn malformed(kind: MalformedKind) -> Self::Reply;
    fn unauthorized(reason: String) -> Self::Reply;

    fn encode_request(req: &Self::Request) -> Result<Vec<u8>, EncodeFailure> {
        encode(req)
    }
    fn decode_request(bytes: &[u8]) -> Result<Self::Request, DecodeFailure> {
        decode_enum(bytes, Self::REQUEST_VARIANTS)
    }
    fn encode_reply(reply: &Self::Reply) -> Result<Vec<u8>, EncodeFailure> {
        encode(reply)
    }
    fn decode_reply(bytes: &[u8]) -> Result<Self::Reply, DecodeFailure> {
        decode_enum(bytes, Self::REPLY_VARIANTS)
    }
}
