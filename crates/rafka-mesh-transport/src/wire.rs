//! The one codec every gossip frame goes through: postcard, positional, no field names, no
//! self-description. A membership [`Frame`](crate::membership::Frame) and a Build-topic message
//! (`rafka-node-admin-core` `fabric_builds`) are encoded here and nowhere else, so what a
//! sender measured to fit one gossip message ([`crate::chunking`]) is exactly what travels.
//!
//! Postcard decodes by position. A type that reaches this codec therefore carries no
//! `skip_serializing_if`, no internally tagged enum and no `serde_json::Value`: those shift
//! fields or need a self-describing format. A type that is JSON elsewhere (REST, a journal, a
//! file) keeps its JSON shape and is mirrored by a wire type beside the codec's caller.

use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

/// Why a value did not encode, or a frame did not decode. Names what was being coded and why it
/// failed; a decode failure also names the bytes it was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError(String);

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WireError {}

impl WireError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

/// `value` as one postcard frame.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, WireError> {
    postcard::to_allocvec(value).map_err(|e| WireError(format!("postcard encode: {e}")))
}

/// One frame of type `T` from `bytes`. A frame that decodes with bytes left over is not this
/// type: it is refused by name, never read as its prefix.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireError> {
    let (value, rest) = postcard::take_from_bytes::<T>(bytes).map_err(|e| WireError(format!("postcard decode of {} bytes as {}: {e}", bytes.len(), std::any::type_name::<T>())))?;
    if rest.is_empty() {
        Ok(value)
    } else {
        Err(WireError(format!("postcard decode of {} bytes as {}: {} bytes left after the frame", bytes.len(), std::any::type_name::<T>(), rest.len())))
    }
}

// A membership digest travels as the shared `WireDigest` (`rafka_mesh_entity::wire`): the same
// shape in a gossip frame and in the join.
pub use rafka_mesh_entity::wire::{digest, digests, WireDigest};
