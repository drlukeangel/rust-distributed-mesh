//! Canonical length-first framing (node-rpc.md §10; ownership amendment §7).
//!
//! ```text
//! request direction:   protocol_tag: u8 | header_len: varint | header | request_len: varint | payload | FIN
//! unary reply:         reply_len: varint | payload | FIN
//! server streaming:    (frame_len: varint | frame)* | FIN
//! ```
//!
//! The receiver reads the tag first (it names the protocol and therefore the
//! request ceiling), then the [`RequestHeader`] — the [`RequestTarget`] fence the
//! caller addressed, checked against the receiver's current birth and slot tokens
//! before anything else, and the [`CallContext`] that correlates the call — then
//! the declared length, and refuses an oversize request before allocating or
//! reading its body.

use crate::context::CallContext;
use serde::{Deserialize, Serialize};

/// The exact target a request addresses: the logical node and birth, and the
/// slot under the freshness token the caller holds. One process serves every
/// slot from its one endpoint, so the socket a request arrives on names
/// nothing: the request does, and the receiver refuses any part of it that is
/// not current with `425 STALE_SLOT` before the length or the body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestTarget {
    pub node_id: String,
    pub incarnation: String,
    pub slot: String,
    pub freshness: String,
}

/// The request envelope: the fence, then the observability context that never
/// participates in protocol semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestHeader {
    pub target: RequestTarget,
    pub context: CallContext,
}

impl RequestHeader {
    pub fn fence(target: RequestTarget) -> Self {
        Self { target, context: CallContext::default() }
    }
}

/// The largest encoded [`RequestHeader`] a receiver reads: the fence and a context at
/// its bounds (`MAX_TRACESTATE_BYTES` + `MAX_BAGGAGE_BYTES` + the short parts) fit.
pub const MAX_HEADER_BYTES: usize = 12 * 1024;

/// An unsigned LEB128 varint is at most 10 bytes for a `u64`.
pub const MAX_VARINT_LEN: usize = 10;

pub fn encode_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Varint {
    /// `value` read in `len` bytes.
    Complete { value: u64, len: usize },
    /// The buffer ends inside the varint.
    NeedMore,
    /// More than 10 bytes, or bits past 64.
    Overflow,
}

pub fn decode_varint(b: &[u8]) -> Varint {
    let mut value: u64 = 0;
    for (i, &byte) in b.iter().enumerate().take(MAX_VARINT_LEN) {
        let bits = u64::from(byte & 0x7f);
        if i == MAX_VARINT_LEN - 1 && bits > 1 {
            return Varint::Overflow;
        }
        value |= bits << (7 * i);
        if byte & 0x80 == 0 {
            return Varint::Complete { value, len: i + 1 };
        }
    }
    if b.len() >= MAX_VARINT_LEN {
        Varint::Overflow
    } else {
        Varint::NeedMore
    }
}

/// `tag | varint(header_len) | header | varint(len) | payload`.
pub fn encode_request(tag: u8, header: &RequestHeader, payload: &[u8]) -> Vec<u8> {
    let header = postcard::to_allocvec(header).expect("a RequestHeader always encodes");
    let mut out = Vec::with_capacity(1 + 2 * MAX_VARINT_LEN + header.len() + payload.len());
    out.push(tag);
    encode_varint(header.len() as u64, &mut out);
    out.extend_from_slice(&header);
    encode_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}

/// `varint(len) | payload` — a unary reply or one streaming frame.
pub fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAX_VARINT_LEN + payload.len());
    encode_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}

/// What the receiver knows after reading the head of a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestHead {
    /// Not enough bytes yet.
    NeedMore,
    /// The tag has no catalog entry: reset `421 UNSERVED_TAG`, no dispatch.
    /// Decided on the tag byte alone, before the length is read.
    Unserved { tag: u8 },
    /// A served tag declaring more than its protocol ceiling: typed
    /// `Malformed(TooLarge)`, body never read.
    TooLarge { tag: u8, declared: u64, max: usize },
    /// The length prefix is not a valid varint: `424 PROTOCOL_VIOLATION`.
    BadLength { tag: u8 },
    /// The header is over [`MAX_HEADER_BYTES`] or does not decode: `424 PROTOCOL_VIOLATION`.
    BadTarget { tag: u8 },
    /// The header is read and the length is not yet: the receiver checks the fence now.
    Targeted { tag: u8, header: RequestHeader },
    /// A served tag within its ceiling; the body is `payload_len` bytes after `head_len`.
    Ready { tag: u8, header: RequestHeader, payload_len: usize, head_len: usize },
}

/// Read the request head. `ceiling(tag)` is the protocol's
/// `MAX_REQUEST_FRAME_BYTES`, or `None` when the tag is not served.
pub fn parse_request_head(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> RequestHead {
    match parse_request_target(buf, ceiling) {
        Ok(t) => parse_request_length(buf, t),
        Err(head) => head,
    }
}

/// The tag and header of a request, read; the length is next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRead {
    pub tag: u8,
    pub header: RequestHeader,
    /// Where the length prefix starts.
    pub at: usize,
    /// The protocol's request ceiling.
    pub max: usize,
}

/// Stage one of the head: the tag, then the header. `Err` is `NeedMore`,
/// `Unserved` or `BadTarget`.
pub fn parse_request_target(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> Result<TargetRead, RequestHead> {
    let Some(&tag) = buf.first() else { return Err(RequestHead::NeedMore) };
    let Some(max) = ceiling(tag) else { return Err(RequestHead::Unserved { tag }) };
    match decode_varint(&buf[1..]) {
        Varint::NeedMore => Err(RequestHead::NeedMore),
        Varint::Overflow => Err(RequestHead::BadTarget { tag }),
        Varint::Complete { value, .. } if value > MAX_HEADER_BYTES as u64 => Err(RequestHead::BadTarget { tag }),
        Varint::Complete { value, len } => {
            let end = 1 + len + value as usize;
            if buf.len() < end {
                return Err(RequestHead::NeedMore);
            }
            match postcard::from_bytes::<RequestHeader>(&buf[1 + len..end]) {
                Ok(header) => Ok(TargetRead { tag, header, at: end, max }),
                Err(_) => Err(RequestHead::BadTarget { tag }),
            }
        }
    }
}

/// Stage two of the head: the declared length after the header.
pub fn parse_request_length(buf: &[u8], t: TargetRead) -> RequestHead {
    let TargetRead { tag, header, at, max } = t;
    match decode_varint(&buf[at..]) {
        Varint::NeedMore => RequestHead::Targeted { tag, header },
        Varint::Overflow => RequestHead::BadLength { tag },
        Varint::Complete { value, .. } if value > max as u64 => RequestHead::TooLarge { tag, declared: value, max },
        Varint::Complete { value, len } => RequestHead::Ready { tag, header, payload_len: value as usize, head_len: at + len },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// The buffer ends before the frame does (EOF inside a frame).
    Truncated,
    /// Declared length above the ceiling.
    TooLarge { declared: u64, max: usize },
    /// The length prefix is not a valid varint.
    BadLength,
    /// Bytes after the one frame a unary direction may carry.
    TrailingBytes(usize),
}

/// Decode one length-prefixed frame from the front of `buf`.
pub fn decode_frame(buf: &[u8], max: usize) -> Result<(&[u8], usize), FrameError> {
    match decode_varint(buf) {
        Varint::NeedMore => Err(FrameError::Truncated),
        Varint::Overflow => Err(FrameError::BadLength),
        Varint::Complete { value, .. } if value > max as u64 => Err(FrameError::TooLarge { declared: value, max }),
        Varint::Complete { value, len } => {
            let end = len + value as usize;
            if buf.len() < end {
                return Err(FrameError::Truncated);
            }
            Ok((&buf[len..end], end))
        }
    }
}

/// Decode a unary direction that must hold exactly one frame.
pub fn decode_single_frame(buf: &[u8], max: usize) -> Result<&[u8], FrameError> {
    let (payload, used) = decode_frame(buf, max)?;
    match buf.len() - used {
        0 => Ok(payload),
        extra => Err(FrameError::TrailingBytes(extra)),
    }
}

/// Split a complete request direction into `(tag, header, payload)`.
pub fn decode_request(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> Result<(u8, RequestHeader, &[u8]), RequestHead> {
    match parse_request_head(buf, ceiling) {
        RequestHead::Ready { tag, header, payload_len, head_len } if buf.len() == head_len + payload_len => {
            Ok((tag, header, &buf[head_len..]))
        }
        RequestHead::Ready { .. } | RequestHead::Targeted { .. } => Err(RequestHead::NeedMore),
        other => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_at_every_width() {
        for v in [0u64, 1, 127, 128, 300, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            let mut b = Vec::new();
            encode_varint(v, &mut b);
            assert_eq!(decode_varint(&b), Varint::Complete { value: v, len: b.len() });
            assert_eq!(decode_varint(&b[..b.len() - 1]), if b.len() == 1 { Varint::NeedMore } else { Varint::NeedMore });
        }
        assert_eq!(decode_varint(&[0xff; 11]), Varint::Overflow);
        assert_eq!(decode_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]), Varint::Overflow);
    }

    fn t() -> RequestHeader {
        RequestHeader::fence(RequestTarget { node_id: "n1".into(), incarnation: "i1".into(), slot: "rpc-0".into(), freshness: "f1".into() })
    }

    #[test]
    fn request_round_trips() {
        let frame = encode_request(0x11, &t(), b"hello");
        assert_eq!(decode_request(&frame, |_| Some(64)), Ok((0x11, t(), &b"hello"[..])));
        assert_eq!(frame[0], 0x11, "the tag is the first byte");
    }

    #[test]
    fn the_header_is_read_before_the_length_and_bounded() {
        let frame = encode_request(0x11, &t(), b"hello");
        let target_end = 2 + frame[1] as usize;
        assert_eq!(parse_request_head(&frame[..target_end], |_| Some(64)), RequestHead::Targeted { tag: 0x11, header: t() });
        assert_eq!(parse_request_head(&frame[..target_end - 1], |_| Some(64)), RequestHead::NeedMore);
        // A context at its bounds fits the header ceiling; one byte past the ceiling is a violation.
        let full = RequestHeader {
            context: CallContext {
                caller_system: Some("a".repeat(crate::context::MAX_CALLER_SYSTEM_BYTES)),
                traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
                tracestate: Some("k=".to_string() + &"v".repeat(crate::context::MAX_TRACESTATE_BYTES - 2)),
                baggage: Some("k=".to_string() + &"v".repeat(crate::context::MAX_BAGGAGE_BYTES - 2)),
            },
            ..t()
        };
        let frame = encode_request(0x11, &full, b"hello");
        assert!(matches!(decode_request(&frame, |_| Some(64)), Ok((0x11, h, _)) if h == full), "a context at its bounds rides the header");
        let mut big = vec![0x11];
        encode_varint(MAX_HEADER_BYTES as u64 + 1, &mut big);
        assert_eq!(parse_request_head(&big, |_| Some(64)), RequestHead::BadTarget { tag: 0x11 });
        let garbage = [0x11, 0x02, 0xff, 0xff];
        assert_eq!(parse_request_head(&garbage, |_| Some(64)), RequestHead::BadTarget { tag: 0x11 });
    }

    #[test]
    fn unserved_is_decided_on_the_tag_byte_alone() {
        assert_eq!(parse_request_head(&[0x42], |t| (t == 0x11).then_some(64)), RequestHead::Unserved { tag: 0x42 });
    }

    #[test]
    fn oversize_is_refused_from_the_declared_length_before_the_body() {
        let mut head = encode_request(0x11, &t(), b"");
        head.pop(); // the zero length
        encode_varint(1 << 30, &mut head); // a 1 GiB declaration, no body at all
        assert_eq!(parse_request_head(&head, |_| Some(1024)), RequestHead::TooLarge { tag: 0x11, declared: 1 << 30, max: 1024 });
    }

    #[test]
    fn partial_heads_need_more_and_bad_lengths_are_violations() {
        assert_eq!(parse_request_head(&[], |_| Some(1)), RequestHead::NeedMore);
        assert_eq!(parse_request_head(&[0x11], |_| Some(1)), RequestHead::NeedMore);
        assert_eq!(parse_request_head(&[0x11, 0x80], |_| Some(1)), RequestHead::NeedMore);
        let mut bad = encode_request(0x11, &t(), b"");
        bad.pop();
        bad.extend([0xff; 11]);
        assert_eq!(parse_request_head(&bad, |_| Some(1)), RequestHead::BadLength { tag: 0x11 });
        let frame = encode_request(0x11, &t(), b"hello");
        assert_eq!(decode_request(&frame[..frame.len() - 1], |_| Some(64)), Err(RequestHead::NeedMore));
    }

    #[test]
    fn reply_frames_round_trip_and_name_their_failures() {
        let f = encode_frame(b"reply");
        assert_eq!(decode_single_frame(&f, 64), Ok(&b"reply"[..]));
        assert_eq!(decode_single_frame(&f[..3], 64), Err(FrameError::Truncated));
        assert_eq!(decode_single_frame(&f, 2), Err(FrameError::TooLarge { declared: 5, max: 2 }));
        let mut two = f.clone();
        two.extend(encode_frame(b"x"));
        assert_eq!(decode_single_frame(&two, 64), Err(FrameError::TrailingBytes(2)));
        let (first, used) = decode_frame(&two, 64).unwrap();
        assert_eq!((first, decode_frame(&two[used..], 64).unwrap().0), (&b"reply"[..], &b"x"[..]));
    }
}
