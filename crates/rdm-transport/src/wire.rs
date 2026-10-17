//! The shared postcard codec. Gossip uses the CloudEvents binary binding in `envelope`: literal
//! standard metadata names and separately selected opaque data, with no numeric kind on the wire.
//! Build-topic and directed RPC data retain their positional postcard schemas. Every caller
//! measures its complete encoded message; this codec rejects a valid prefix with trailing bytes.
//!
//! Positional data contains no `skip_serializing_if`, internally tagged enums or JSON values.
//! Types used by JSON surfaces keep a separate wire shape where required.

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
    /// An error naming `reason`.
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

// A membership digest travels as the shared `WireDigest` (`rdm_core::wire`): the same
// shape in a heartbeat and in the join; a mesh-primary's member projection carries each member as a
// `ProjectionEntry` (its digest, then its status).
pub use rdm_core::wire::{digest, entries, ProjectionEntry, WireDigest};

/// Consume one postcard value from a selected data schema, retaining its remaining fields.
pub(crate) fn take<T: DeserializeOwned>(bytes: &mut &[u8]) -> Result<T, WireError> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(|e| WireError::new(format!("postcard field: {e}")))?;
    *bytes = rest;
    Ok(value)
}
