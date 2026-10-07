//! Canonical length-first framing (node-rpc.md §10; ownership amendment §7).
//!
//! ```text
//! request direction:   fence_len: varint | fence | context_len: varint | context | request_len: varint | payload | FIN
//! unary reply:         reply_len: varint | payload | FIN
//! server streaming:    (frame_len: varint | frame)* | FIN
//! ```
//!
//! The receiver reads the [`Fence`] first and alone: the op (which names the
//! protocol and therefore the request ceiling) and the node the caller resolved,
//! each checked against what this process is and serves before the context, the
//! length or the body exist to it. Then the
//! [`CallContext`] that correlates the call, then the declared length; an
//! oversize request is refused before its body is allocated or read.

use crate::context::CallContext;
use serde::{Deserialize, Serialize};

/// The fence: what the receiver checks a request against before anything else.
/// It is core, the same two checks for every protocol: is the addressed node me
/// (`target_node_id`, the minted id; a mismatch is `425 STALE_TARGET`, the
/// caller reached a replacement or misrouted), is the op served (`op`, the
/// protocol's ledger code; unserved is `421`). One process has one endpoint, so
/// the socket a request arrives on names nothing: the fence does. It says
/// nothing about the process birth: which birth a caller is talking to is the
/// resolver's and the pool's knowledge, never the wire's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    pub target_node_id: String,
    pub op: u8,
}

/// The request envelope as the receiver holds it after the head: the fence it
/// passed, then the observability context that never participates in protocol
/// semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestHeader {
    pub fence: Fence,
    pub context: CallContext,
}

impl RequestHeader {
    pub fn fence(fence: Fence) -> Self {
        Self { fence, context: CallContext::default() }
    }
}

/// The largest encoded [`Fence`] a receiver reads before deciding.
pub const MAX_FENCE_BYTES: usize = 256;

/// The largest encoded [`CallContext`] a receiver reads: a context at its bounds
/// (`MAX_TRACESTATE_BYTES` + `MAX_BAGGAGE_BYTES` + the short parts) fits.
pub const MAX_CONTEXT_BYTES: usize = 12 * 1024;

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

/// `varint(fence_len) | fence | varint(context_len) | context | varint(len) | payload`.
pub fn encode_request(header: &RequestHeader, payload: &[u8]) -> Vec<u8> {
    let fence = postcard::to_allocvec(&header.fence).expect("a Fence always encodes");
    let context = postcard::to_allocvec(&header.context).expect("a CallContext always encodes");
    let mut out = Vec::with_capacity(3 * MAX_VARINT_LEN + fence.len() + context.len() + payload.len());
    encode_varint(fence.len() as u64, &mut out);
    out.extend_from_slice(&fence);
    encode_varint(context.len() as u64, &mut out);
    out.extend_from_slice(&context);
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
    /// The fence's op has no catalog entry: reset `421 UNSERVED_OP`, no dispatch.
    /// Decided on the fence alone, before the context or the length is read.
    Unserved { op: u8 },
    /// A served op declaring more than its protocol ceiling: typed
    /// `Malformed(TooLarge)`, body never read.
    TooLarge { op: u8, declared: u64, max: usize },
    /// The length prefix is not a valid varint: `424 PROTOCOL_VIOLATION`.
    BadLength { op: u8 },
    /// The fence names a node this process is not: reset `425 STALE_TARGET`, no dispatch.
    /// Decided on the fence alone, before the context or the length is read.
    Stale { op: u8, fence: Fence },
    /// The fence or the context is over its bound or does not decode: `424 PROTOCOL_VIOLATION`.
    /// `op` is `None` when the fence itself could not be read.
    BadTarget { op: Option<u8> },
    /// The fence and context are read and the length is not yet.
    Targeted { op: u8, header: RequestHeader },
    /// A served op within its ceiling; the body is `payload_len` bytes after `head_len`.
    Ready { op: u8, header: RequestHeader, payload_len: usize, head_len: usize },
}

/// Read the request head. `ceiling(op)` is the protocol's
/// `MAX_REQUEST_FRAME_BYTES`, or `None` when the op is not served.
pub fn parse_request_head(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> RequestHead {
    match parse_request_target(buf, ceiling, |_| true) {
        Ok(t) => parse_request_length(buf, t),
        Err(head) => head,
    }
}

/// The fence and context of a request, read; the length is next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRead {
    pub op: u8,
    pub header: RequestHeader,
    /// Where the length prefix starts.
    pub at: usize,
    /// The protocol's request ceiling.
    pub max: usize,
}

/// Stage one of the head: the fence, then the context. `Err` is `NeedMore`,
/// `Unserved`, `Stale` or `BadTarget`. The fence is decoded, its op looked up and its
/// target checked against `current` before a byte of the context is read: a `425` owes
/// nothing to section 1, so it can carry nothing from it.
pub fn parse_request_target(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>, current: impl Fn(&Fence) -> bool) -> Result<TargetRead, RequestHead> {
    let (fence, at): (Fence, usize) = match decode_section(buf, 0, MAX_FENCE_BYTES) {
        Section::NeedMore => return Err(RequestHead::NeedMore),
        Section::Bad => return Err(RequestHead::BadTarget { op: None }),
        Section::Read(bytes, at) => match postcard::from_bytes::<Fence>(bytes) {
            Ok(f) => (f, at),
            Err(_) => return Err(RequestHead::BadTarget { op: None }),
        },
    };
    let op = fence.op;
    let Some(max) = ceiling(op) else { return Err(RequestHead::Unserved { op }) };
    if !current(&fence) {
        return Err(RequestHead::Stale { op, fence });
    }
    match decode_section(buf, at, MAX_CONTEXT_BYTES) {
        Section::NeedMore => Err(RequestHead::NeedMore),
        Section::Bad => Err(RequestHead::BadTarget { op: Some(op) }),
        Section::Read(bytes, at) => match postcard::from_bytes::<CallContext>(bytes) {
            Ok(context) => Ok(TargetRead { op, header: RequestHeader { fence, context }, at, max }),
            Err(_) => Err(RequestHead::BadTarget { op: Some(op) }),
        },
    }
}

/// The fence alone, when that is all a reader needs (a carrier naming the inner
/// target, a test): `None` until the fence is complete or when it does not decode.
pub fn peek_fence(buf: &[u8]) -> Option<Fence> {
    match decode_section(buf, 0, MAX_FENCE_BYTES) {
        Section::Read(bytes, _) => postcard::from_bytes(bytes).ok(),
        _ => None,
    }
}

enum Section<'a> {
    NeedMore,
    Bad,
    /// The section's bytes and where the next section starts.
    Read(&'a [u8], usize),
}

/// One `varint(len) | bytes` section starting at `from`, bounded by `max`.
fn decode_section(buf: &[u8], from: usize, max: usize) -> Section<'_> {
    if buf.len() < from {
        return Section::NeedMore;
    }
    match decode_varint(&buf[from..]) {
        Varint::NeedMore => Section::NeedMore,
        Varint::Overflow => Section::Bad,
        Varint::Complete { value, .. } if value > max as u64 => Section::Bad,
        Varint::Complete { value, len } => {
            let start = from + len;
            let end = start + value as usize;
            if buf.len() < end {
                return Section::NeedMore;
            }
            Section::Read(&buf[start..end], end)
        }
    }
}

/// Stage two of the head: the declared length after the header.
pub fn parse_request_length(buf: &[u8], t: TargetRead) -> RequestHead {
    let TargetRead { op, header, at, max } = t;
    match decode_varint(&buf[at..]) {
        Varint::NeedMore => RequestHead::Targeted { op, header },
        Varint::Overflow => RequestHead::BadLength { op },
        Varint::Complete { value, .. } if value > max as u64 => RequestHead::TooLarge { op, declared: value, max },
        Varint::Complete { value, len } => RequestHead::Ready { op, header, payload_len: value as usize, head_len: at + len },
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

/// Split a complete request direction into `(op, header, payload)`.
pub fn decode_request(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> Result<(u8, RequestHeader, &[u8]), RequestHead> {
    match parse_request_head(buf, ceiling) {
        RequestHead::Ready { op, header, payload_len, head_len } if buf.len() == head_len + payload_len => {
            Ok((op, header, &buf[head_len..]))
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
        RequestHeader::fence(Fence { target_node_id: "n1".into(), op: 0x01 })
    }

    #[test]
    fn request_round_trips() {
        let frame = encode_request(&t(), b"hello");
        assert_eq!(decode_request(&frame, |_| Some(64)), Ok((0x01, t(), &b"hello"[..])));
        assert_eq!(peek_fence(&frame), Some(t().fence), "the fence is the first section");
    }

    #[test]
    fn the_header_is_read_before_the_length_and_bounded() {
        let frame = encode_request(&t(), b"hello");
        let fence_end = 1 + frame[0] as usize;
        let context_end = fence_end + 1 + frame[fence_end] as usize;
        assert_eq!(parse_request_head(&frame[..context_end], |_| Some(64)), RequestHead::Targeted { op: 0x01, header: t() });
        assert_eq!(parse_request_head(&frame[..context_end - 1], |_| Some(64)), RequestHead::NeedMore);
        assert_eq!(parse_request_head(&frame[..fence_end], |t| (t != 0x01).then_some(64)), RequestHead::Unserved { op: 0x01 }, "an unserved op is decided on the fence, before the context");
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
        let frame = encode_request(&full, b"hello");
        assert!(matches!(decode_request(&frame, |_| Some(64)), Ok((0x01, h, _)) if h == full), "a context at its bounds rides the header");
        // An oversize fence is refused before its op is known.
        let mut big = Vec::new();
        encode_varint(MAX_FENCE_BYTES as u64 + 1, &mut big);
        assert_eq!(parse_request_head(&big, |_| Some(64)), RequestHead::BadTarget { op: None });
        let garbage = [0x02, 0xff, 0xff];
        assert_eq!(parse_request_head(&garbage, |_| Some(64)), RequestHead::BadTarget { op: None });
        // An oversize context is refused with the op the fence named.
        let mut frame = encode_request(&t(), b"");
        let fence_end = frame.len() - 2; // `0x00` context, `0x00` payload
        frame.truncate(fence_end);
        encode_varint(MAX_CONTEXT_BYTES as u64 + 1, &mut frame);
        assert_eq!(parse_request_head(&frame, |_| Some(64)), RequestHead::BadTarget { op: Some(0x01) });
    }

    #[test]
    fn unserved_is_decided_on_the_fence_alone() {
        let unserved = RequestHeader::fence(Fence { target_node_id: "n1".into(), op: 0x42 });
        let frame = encode_request(&unserved, b"");
        let fence_end = frame.len() - 2;
        assert_eq!(parse_request_head(&frame[..fence_end], |t| (t == 0x01).then_some(64)), RequestHead::Unserved { op: 0x42 }, "decided before a byte of context arrives");
    }

    #[test]
    fn oversize_is_refused_from_the_declared_length_before_the_body() {
        let mut head = encode_request(&t(), b"");
        head.pop(); // the zero length
        encode_varint(1 << 30, &mut head); // a 1 GiB declaration, no body at all
        assert_eq!(parse_request_head(&head, |_| Some(1024)), RequestHead::TooLarge { op: 0x01, declared: 1 << 30, max: 1024 });
    }

    #[test]
    fn partial_heads_need_more_and_bad_lengths_are_violations() {
        assert_eq!(parse_request_head(&[], |_| Some(1)), RequestHead::NeedMore);
        let whole = encode_request(&t(), b"");
        for cut in 1..whole.len() - 1 {
            assert_eq!(parse_request_head(&whole[..cut], |_| Some(1)), RequestHead::NeedMore, "a prefix of {cut} bytes needs more");
        }
        let mut bad = encode_request(&t(), b"");
        bad.pop();
        bad.extend([0xff; 11]);
        assert_eq!(parse_request_head(&bad, |_| Some(1)), RequestHead::BadLength { op: 0x01 });
        let frame = encode_request(&t(), b"hello");
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
