//! Stream-level wire framing per the locked architecture spec:
//!
//! ```text
//! stream = tag(u8) length(unsigned-varint) payload(postcard) [EOF]
//! ```
//!
//! Single-use streams: sender encodes one frame, calls `finish()`; receiver
//! reads tag + length + payload + drops the stream. No second frame on the
//! same stream — keeps stream-level QoS predictable (one tag → one handler).
//!
//! Postcard everywhere per golden principle #11 (internal data read once).
//! LEB128 varint for length prefix matches postcard's internal encoding so
//! the entire wire stream is format-homogeneous (no fixed u32 boundary that
//! would waste 3 bytes on every small payload).
//!
//! ## Tag namespace (see `docs/architecture/mesh.md`)
//!
//! - `0x00`        — reserved / sentinel
//! - `0x01..=0x0F` — control plane (pointer pulls, auth pushes, gossip fetches)
//! - `0x10..=0x7F` — data plane (`0x10` = legacy Ping/Pong/Hello, `0x11+` heavy compute)
//! - `0x80..=0xFF` — extensions / vendor

use serde::{de::DeserializeOwned, Serialize};

/// Tag for the existing Ping/Pong/Hello substrate so it slots in behind the
/// new framing contract without rewriting handlers.
pub const TAG_LEGACY_FRAME: u8 = 0x10;

/// Errors decoding a framed stream payload.
#[derive(Debug, thiserror::Error)]
pub enum FramerError {
    #[error("input too short — need at least 1 byte for tag")]
    TooShortForTag,
    #[error("malformed varint length prefix: {0}")]
    BadLength(unsigned_varint::decode::Error),
    #[error("declared length {declared} exceeds available {available}")]
    Truncated { declared: usize, available: usize },
    #[error("postcard decode failed: {0}")]
    Postcard(#[from] postcard::Error),
}

/// Encode `(tag, &value)` to the wire form: `[tag] [varint(len)] [postcard(value)]`.
/// Returned `Vec<u8>` is the complete framed stream payload.
pub fn encode<T: Serialize>(tag: u8, value: &T) -> Vec<u8> {
    let payload = postcard::to_allocvec(value).expect("postcard encode is infallible");
    let mut len_buf = unsigned_varint::encode::usize_buffer();
    let len_bytes = unsigned_varint::encode::usize(payload.len(), &mut len_buf);
    let mut out = Vec::with_capacity(1 + len_bytes.len() + payload.len());
    out.push(tag);
    out.extend_from_slice(len_bytes);
    out.extend_from_slice(&payload);
    out
}

/// Decode a complete framed stream payload to `(tag, value, n_bytes_consumed)`.
/// `n_bytes_consumed` lets callers verify they read exactly the expected window
/// (EOF discipline — the framer doesn't permit dangling bytes after the payload).
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<(u8, T, usize), FramerError> {
    if bytes.is_empty() {
        return Err(FramerError::TooShortForTag);
    }
    let tag = bytes[0];
    let (length, rest) =
        unsigned_varint::decode::usize(&bytes[1..]).map_err(FramerError::BadLength)?;
    if rest.len() < length {
        return Err(FramerError::Truncated {
            declared: length,
            available: rest.len(),
        });
    }
    let payload = &rest[..length];
    let value: T = postcard::from_bytes(payload)?;
    let consumed = 1 + (bytes.len() - 1 - rest.len()) + length;
    Ok((tag, value, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InternalMeshFrame;
    use proptest::prelude::*;

    fn arb_tag() -> impl Strategy<Value = u8> {
        // Whole 0..=255 range — verify every tag value round-trips, not just the
        // reserved-range members. Drop-on-unknown happens at the demuxer layer,
        // not the framer.
        0u8..=255
    }

    fn arb_frame() -> impl Strategy<Value = InternalMeshFrame> {
        prop_oneof![
            any::<u64>().prop_map(|org_id| InternalMeshFrame::Ping { org_id }),
            any::<u64>().prop_map(|org_id| InternalMeshFrame::Pong { org_id }),
            ("[a-zA-Z0-9_-]{0,32}", "[a-zA-Z0-9_-]{0,32}")
                .prop_map(|(mesh_id, node_type)| InternalMeshFrame::Hello { mesh_id, node_type }),
        ]
    }

    proptest! {
        /// Encode → decode round-trips byte-for-byte for any (tag, frame) pair.
        #[test]
        fn round_trip(tag in arb_tag(), frame in arb_frame()) {
            let encoded = encode(tag, &frame);
            let (got_tag, got_frame, consumed) =
                decode::<InternalMeshFrame>(&encoded).expect("round-trip decode");
            prop_assert_eq!(tag, got_tag);
            prop_assert_eq!(consumed, encoded.len(), "must consume the entire frame");
            // Frame equality via debug repr since the enum doesn't derive PartialEq.
            prop_assert_eq!(format!("{frame:?}"), format!("{got_frame:?}"));
        }

        /// Decoder must report `Truncated` on any prefix shorter than the full frame.
        /// Catches off-by-one bugs in the length-check branch.
        #[test]
        fn truncation_detected(tag in arb_tag(), frame in arb_frame()) {
            let encoded = encode(tag, &frame);
            // Only meaningful for frames whose payload is > 0 bytes.
            if encoded.len() <= 2 { return Ok(()); }
            // Lop off the final byte of the payload.
            let truncated = &encoded[..encoded.len() - 1];
            let err = decode::<InternalMeshFrame>(truncated);
            prop_assert!(matches!(err, Err(FramerError::Truncated { .. })),
                "expected Truncated, got {err:?}");
        }
    }

    #[test]
    fn empty_input_rejected() {
        let err = decode::<InternalMeshFrame>(&[]);
        assert!(matches!(err, Err(FramerError::TooShortForTag)));
    }

    #[test]
    fn tag_only_no_length_rejected() {
        // 1 byte = tag only; varint decode of empty slice must fail.
        let err = decode::<InternalMeshFrame>(&[0x10]);
        assert!(matches!(err, Err(FramerError::BadLength(_))));
    }

    #[test]
    fn legacy_tag_constant() {
        assert_eq!(TAG_LEGACY_FRAME, 0x10);
    }
}
