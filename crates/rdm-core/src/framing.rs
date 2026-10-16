//! Canonical length-first framing (node-rpc.md §10; ownership amendment §7; node-rpc-envelope.md
//! "Transport frames").
//!
//! ```text
//! request frame:   header_len: varint | Request  | target_len: varint | target | context_len: varint | context | request_len: varint | payload
//! response frame:  header_len: varint | Response | reply_len: varint | reply
//! ack frame:       header_len: varint | Ack
//! ```
//!
//! Every frame, in either direction, begins with its [`TransportHeader`]: the delivery identity the
//! runtime consumes before any protocol byte is read (node-rpc.md §43). A stream's FIN ends that
//! stream, never an invocation.
//!
//! The receiver reads the [`Target`] first and alone: the op (which names the
//! protocol and therefore the request ceiling) is checked against what this
//! process serves before the context, the length or the body exist to it; the
//! node id the caller resolved is never a reason to refuse (R-UC1a). Then the
//! [`CallContext`] that correlates the call, then the declared length; an
//! oversize request is refused before its body is allocated or read.

use crate::context::CallContext;
use crate::ids::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// A sending node's exact birth, `(node.id, incarnation)`: the first half of every delivery identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Birth {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The incarnation of the birth.
    pub incarnation: IncarnationId,
}

impl std::fmt::Display for Birth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.node_id.as_str(), self.incarnation.as_str())
    }
}

/// One frame's delivery identity, `(sender, delivery_seq)`: minted by its sender, the same on every
/// resend, never reused by the sending birth. A request's identity is also its `invocation_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeliveryId {
    /// The birth that minted it.
    pub sender: Birth,
    /// The sender's sequence; the first is 1.
    pub delivery_seq: u64,
}

impl std::fmt::Display for DeliveryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.sender, self.delivery_seq)
    }
}

/// The transport header every frame begins with (node-rpc-envelope.md "Transport frames"). Its
/// postcard discriminant is a position: `Request` 0, `Response` 1, `Ack` 2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportHeader {
    /// A request frame: sections 0-2 follow.
    Request {
        /// The sending birth.
        sender: Birth,
        /// The frame's sequence; the request's `invocation_id` is `(sender, delivery_seq)`.
        delivery_seq: u64,
        /// The lowest `delivery_seq` the sender still holds unacked toward this receiver: it never
        /// resends anything below it.
        retire_below: u64,
    },
    /// A response frame: `reply_len | reply` follows.
    Response {
        /// The responding birth.
        sender: Birth,
        /// The frame's own sequence.
        delivery_seq: u64,
        /// As in `Request`.
        retire_below: u64,
        /// The request this frame answers.
        invocation_id: DeliveryId,
        /// The frame's position in the invocation's response: `0` for a unary reply; `Started` is
        /// `0`, each `Data` the next, `Terminal` the last.
        seq: u32,
    },
    /// The acknowledgement of one frame, naming that frame's identity; nothing follows.
    Ack {
        /// The acknowledged frame's sender.
        sender: Birth,
        /// The acknowledged frame's sequence.
        delivery_seq: u64,
    },
}

impl TransportHeader {
    /// The identity of the frame this header begins, or for an `Ack` the identity it acknowledges.
    pub fn delivery_id(&self) -> DeliveryId {
        match self {
            Self::Request { sender, delivery_seq, .. } | Self::Response { sender, delivery_seq, .. } | Self::Ack { sender, delivery_seq } => {
                DeliveryId { sender: sender.clone(), delivery_seq: *delivery_seq }
            }
        }
    }

    /// The header's kind, as spans name it.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Request { .. } => "request",
            Self::Response { .. } => "response",
            Self::Ack { .. } => "ack",
        }
    }

    /// The `Ack` of the frame this header begins.
    pub fn ack(&self) -> TransportHeader {
        let DeliveryId { sender, delivery_seq } = self.delivery_id();
        Self::Ack { sender, delivery_seq }
    }
}

/// The largest encoded [`TransportHeader`] a reader reads; its declared length is checked against
/// this before a byte of it is read. The largest a conforming sender writes is 88 (`Response`).
pub const MAX_HEADER_BYTES: usize = 128;

/// `varint(header_len) | header`: the header section every frame begins with.
pub fn encode_header(h: &TransportHeader, out: &mut Vec<u8>) {
    let bytes = postcard::to_allocvec(h).expect("a TransportHeader always encodes");
    encode_varint(bytes.len() as u64, out);
    out.extend_from_slice(&bytes);
}

/// A request frame: its header, then sections 0-2.
pub fn encode_request_frame(h: &TransportHeader, header: &RequestHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_header(h, &mut out);
    out.extend(encode_request(header, payload));
    out
}

/// A response frame: its header, then `reply_len | reply`.
pub fn encode_response_frame(h: &TransportHeader, reply: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_header(h, &mut out);
    out.extend(encode_frame(reply));
    out
}

/// An ack frame: the header alone.
pub fn encode_ack_frame(h: &TransportHeader) -> Vec<u8> {
    let mut out = Vec::new();
    encode_header(h, &mut out);
    out
}

/// What reading a header section at the front of a buffer found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderRead {
    /// The buffer ends inside the section.
    NeedMore,
    /// `424 PROTOCOL_VIOLATION`: over its bound, not a varint, or not a header of a known kind.
    Bad(&'static str),
    /// The header and the bytes its section took.
    Read(TransportHeader, usize),
}

/// Read the header section at the front of `buf`.
pub fn decode_header(buf: &[u8]) -> HeaderRead {
    match decode_section(buf, 0, MAX_HEADER_BYTES) {
        Section::NeedMore => HeaderRead::NeedMore,
        Section::Bad => HeaderRead::Bad("transport header over its 128-byte bound or not a varint length"),
        Section::Read(bytes, at) => match postcard::take_from_bytes::<TransportHeader>(bytes) {
            Ok((h, rest)) if rest.is_empty() => HeaderRead::Read(h, at),
            Ok(_) => HeaderRead::Bad("bytes past the transport header inside its section"),
            Err(_) => HeaderRead::Bad("transport header does not decode, or names an unknown kind"),
        },
    }
}

/// The target: what the receiver reads first, alone. It is core, the same for every protocol: the
/// op is checked against what this process serves (`op`, the protocol's ledger code; unserved is
/// `421`), and `node_id` names the node the caller resolved (empty for a call to an endpoint
/// the caller holds no record of). No receiver refuses a call for the node id it names (R-UC1a):
/// the node id is the caller's record of whom it called. It says nothing about the process birth:
/// which birth a caller is talking to is the resolver's and the pool's knowledge, never the wire's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    /// The node id the request is for.
    pub node_id: String,
    /// The op the request is for.
    pub op: u8,
}

/// The request envelope as the receiver holds it after the head: the target it
/// passed, then the observability context that never participates in protocol
/// semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestHeader {
    /// The target naming the node and op.
    pub target: Target,
    /// The observability context the request carries.
    pub context: CallContext,
}

impl RequestHeader {
    /// A header with the target and an empty context.
    pub fn target(target: Target) -> Self {
        Self { target, context: CallContext::default() }
    }
}

/// The largest encoded [`Target`] a receiver reads before deciding.
pub(crate) const MAX_TARGET_BYTES: usize = 256;

/// The largest encoded [`CallContext`] a receiver reads: a context at its bounds
/// (`MAX_TRACESTATE_BYTES` + `MAX_BAGGAGE_BYTES` + the short parts) fits.
pub(crate) const MAX_CONTEXT_BYTES: usize = 12 * 1024;

/// An unsigned LEB128 varint is at most 10 bytes for a `u64`.
pub const MAX_VARINT_LEN: usize = 10;

/// Append `v` to `out` as an unsigned LEB128 varint.
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

/// The result of reading a varint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Varint {
    /// `value` read in `len` bytes.
    Complete {
        /// The value read.
        value: u64,
        /// The bytes it took.
        len: usize,
    },
    /// The buffer ends inside the varint.
    NeedMore,
    /// More than 10 bytes, or bits past 64.
    Overflow,
}

/// Read a varint from the start of `b`.
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

/// `varint(target_len) | target | varint(context_len) | context | varint(len) | payload`.
pub fn encode_request(header: &RequestHeader, payload: &[u8]) -> Vec<u8> {
    let target = postcard::to_allocvec(&header.target).expect("a Target always encodes");
    let context = postcard::to_allocvec(&header.context).expect("a CallContext always encodes");
    let mut out = Vec::with_capacity(3 * MAX_VARINT_LEN + target.len() + context.len() + payload.len());
    encode_varint(target.len() as u64, &mut out);
    out.extend_from_slice(&target);
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
    /// The target's op has no catalog entry: reset `421 UNSERVED_OP`, no dispatch.
    /// Decided on the target alone, before the context or the length is read.
    Unserved {
        /// The op that is not served.
        op: u8,
    },
    /// A served op declaring more than its protocol ceiling: typed
    /// `Malformed(TooLarge)`, body never read.
    TooLarge {
        /// The op.
        op: u8,
        /// The length the request declared.
        declared: u64,
        /// The protocol's request ceiling.
        max: usize,
    },
    /// The length prefix is not a valid varint: `424 PROTOCOL_VIOLATION`.
    BadLength {
        /// The op.
        op: u8,
    },
    /// The target or the context is over its bound or does not decode: `424 PROTOCOL_VIOLATION`.
    /// `op` is `None` when the target itself could not be read.
    BadTarget {
        /// The op, when the target could be read.
        op: Option<u8>,
    },
    /// The target and context are read and the length is not yet.
    Targeted {
        /// The op.
        op: u8,
        /// The target and context read.
        header: RequestHeader,
    },
    /// A served op within its ceiling; the body is `payload_len` bytes after `head_len`.
    Ready {
        /// The op.
        op: u8,
        /// The target and context.
        header: RequestHeader,
        /// The body length in bytes.
        payload_len: usize,
        /// The bytes the head took.
        head_len: usize,
    },
}

/// Read the request head. `ceiling(op)` is the protocol's
/// `MAX_REQUEST_FRAME_BYTES`, or `None` when the op is not served.
pub(crate) fn parse_request_head(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> RequestHead {
    match parse_request_target(buf, ceiling) {
        Ok(t) => parse_request_length(buf, t),
        Err(head) => head,
    }
}

/// The target and context of a request, read; the length is next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetRead {
    pub op: u8,
    pub header: RequestHeader,
    /// Where the length prefix starts.
    pub at: usize,
    /// The protocol's request ceiling.
    pub max: usize,
}

/// Stage one of the head: the target, then the context. `Err` is `NeedMore`, `Unserved` or
/// `BadTarget`. The target is decoded and its op looked up before a byte of the context is read: a
/// `421` owes nothing to section 1, so it can carry nothing from it.
pub(crate) fn parse_request_target(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> Result<TargetRead, RequestHead> {
    let (target, at): (Target, usize) = match decode_section(buf, 0, MAX_TARGET_BYTES) {
        Section::NeedMore => return Err(RequestHead::NeedMore),
        Section::Bad => return Err(RequestHead::BadTarget { op: None }),
        Section::Read(bytes, at) => match postcard::from_bytes::<Target>(bytes) {
            Ok(f) => (f, at),
            Err(_) => return Err(RequestHead::BadTarget { op: None }),
        },
    };
    let op = target.op;
    let Some(max) = ceiling(op) else { return Err(RequestHead::Unserved { op }) };
    match decode_section(buf, at, MAX_CONTEXT_BYTES) {
        Section::NeedMore => Err(RequestHead::NeedMore),
        Section::Bad => Err(RequestHead::BadTarget { op: Some(op) }),
        Section::Read(bytes, at) => match postcard::from_bytes::<CallContext>(bytes) {
            Ok(context) => Ok(TargetRead { op, header: RequestHeader { target, context }, at, max }),
            Err(_) => Err(RequestHead::BadTarget { op: Some(op) }),
        },
    }
}

/// The target alone, when that is all a reader needs (a carrier naming the inner
/// target, a test): `None` until the target is complete or when it does not decode.
#[cfg(test)]
pub(crate) fn peek_target(buf: &[u8]) -> Option<Target> {
    match decode_section(buf, 0, MAX_TARGET_BYTES) {
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
pub(crate) fn parse_request_length(buf: &[u8], t: TargetRead) -> RequestHead {
    let TargetRead { op, header, at, max } = t;
    match decode_varint(&buf[at..]) {
        Varint::NeedMore => RequestHead::Targeted { op, header },
        Varint::Overflow => RequestHead::BadLength { op },
        Varint::Complete { value, .. } if value > max as u64 => RequestHead::TooLarge { op, declared: value, max },
        Varint::Complete { value, len } => RequestHead::Ready { op, header, payload_len: value as usize, head_len: at + len },
    }
}

/// Why a frame was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// The buffer ends before the frame does (EOF inside a frame).
    Truncated,
    /// Declared length above the ceiling.
    TooLarge {
        /// The length the frame declared.
        declared: u64,
        /// The ceiling.
        max: usize,
    },
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
        RequestHeader::target(Target { node_id: "n1".into(), op: 0x01 })
    }

    #[test]
    fn request_round_trips() {
        let frame = encode_request(&t(), b"hello");
        assert_eq!(decode_request(&frame, |_| Some(64)), Ok((0x01, t(), &b"hello"[..])));
        assert_eq!(peek_target(&frame), Some(t().target), "the target is the first section");
    }

    #[test]
    fn the_header_is_read_before_the_length_and_bounded() {
        let frame = encode_request(&t(), b"hello");
        let target_end = 1 + frame[0] as usize;
        let context_end = target_end + 1 + frame[target_end] as usize;
        assert_eq!(parse_request_head(&frame[..context_end], |_| Some(64)), RequestHead::Targeted { op: 0x01, header: t() });
        assert_eq!(parse_request_head(&frame[..context_end - 1], |_| Some(64)), RequestHead::NeedMore);
        assert_eq!(parse_request_head(&frame[..target_end], |t| (t != 0x01).then_some(64)), RequestHead::Unserved { op: 0x01 }, "an unserved op is decided on the target, before the context");
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
        // An oversize target is refused before its op is known.
        let mut big = Vec::new();
        encode_varint(MAX_TARGET_BYTES as u64 + 1, &mut big);
        assert_eq!(parse_request_head(&big, |_| Some(64)), RequestHead::BadTarget { op: None });
        let garbage = [0x02, 0xff, 0xff];
        assert_eq!(parse_request_head(&garbage, |_| Some(64)), RequestHead::BadTarget { op: None });
        // An oversize context is refused with the op the target named.
        let mut frame = encode_request(&t(), b"");
        let target_end = frame.len() - 2; // `0x00` context, `0x00` payload
        frame.truncate(target_end);
        encode_varint(MAX_CONTEXT_BYTES as u64 + 1, &mut frame);
        assert_eq!(parse_request_head(&frame, |_| Some(64)), RequestHead::BadTarget { op: Some(0x01) });
    }

    #[test]
    fn unserved_is_decided_on_the_target_alone() {
        let unserved = RequestHeader::target(Target { node_id: "n1".into(), op: 0x42 });
        let frame = encode_request(&unserved, b"");
        let target_end = frame.len() - 2;
        assert_eq!(parse_request_head(&frame[..target_end], |t| (t == 0x01).then_some(64)), RequestHead::Unserved { op: 0x42 }, "decided before a byte of context arrives");
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

    fn birth() -> Birth {
        Birth { node_id: NodeId::parse("abcdefghjkmn").unwrap(), incarnation: IncarnationId::parse("pqrstvwxyz01").unwrap() }
    }

    /// A birth's 26 bytes: each id is its postcard string, `0c` and its twelve characters.
    const BIRTH: &str = "0c61626364656667686a6b6d6e0c7071727374767778797a3031";

    fn section(h: &TransportHeader) -> String {
        let mut out = Vec::new();
        encode_header(h, &mut out);
        hex::encode(out)
    }

    #[test]
    fn the_three_transport_headers_are_frozen_byte_for_byte() {
        let request = TransportHeader::Request { sender: birth(), delivery_seq: 1, retire_below: 1 };
        assert_eq!(section(&request), format!("1d00{BIRTH}0101"), "Request: kind 0, the birth, delivery_seq, retire_below");
        let response = TransportHeader::Response {
            sender: birth(),
            delivery_seq: 2,
            retire_below: 1,
            invocation_id: DeliveryId { sender: birth(), delivery_seq: 1 },
            seq: 0,
        };
        assert_eq!(section(&response), format!("3901{BIRTH}0201{BIRTH}0100"), "Response: kind 1, its identity, retire_below, invocation_id, seq");
        let ack = TransportHeader::Ack { sender: birth(), delivery_seq: 1 };
        assert_eq!(section(&ack), format!("1c02{BIRTH}01"), "Ack: kind 2, the acknowledged frame's identity");
        assert_eq!(request.ack(), ack, "a frame's ack names its identity");
        for h in [request, response, ack] {
            let mut b = Vec::new();
            encode_header(&h, &mut b);
            assert_eq!(decode_header(&b), HeaderRead::Read(h, b.len()));
        }
    }

    #[test]
    fn a_frame_is_its_header_section_then_its_protocol_sections() {
        let request = TransportHeader::Request { sender: birth(), delivery_seq: 1, retire_below: 1 };
        let frame = encode_request_frame(&request, &t(), b"hi");
        assert_eq!(hex::encode(&frame), format!("1d00{BIRTH}0101" ) + "04026e3101" + "0400000000" + "026869");
        let HeaderRead::Read(h, at) = decode_header(&frame) else { panic!("the header reads") };
        assert_eq!(h, request);
        assert_eq!(decode_request(&frame[at..], |_| Some(64)), Ok((0x01, t(), &b"hi"[..])), "sections 0-2 follow the header unchanged");
        let response = TransportHeader::Response {
            sender: birth(),
            delivery_seq: 2,
            retire_below: 1,
            invocation_id: DeliveryId { sender: birth(), delivery_seq: 1 },
            seq: 0,
        };
        let frame = encode_response_frame(&response, b"ok");
        let HeaderRead::Read(_, at) = decode_header(&frame) else { panic!("the header reads") };
        assert_eq!(decode_single_frame(&frame[at..], 64), Ok(&b"ok"[..]));
        let ack = encode_ack_frame(&TransportHeader::Ack { sender: birth(), delivery_seq: 1 });
        assert_eq!(hex::encode(&ack), format!("1c02{BIRTH}01"), "nothing follows an ack's header");
    }

    #[test]
    fn transport_headers_encode_within_their_bound() {
        let request = TransportHeader::Request { sender: birth(), delivery_seq: u64::MAX, retire_below: u64::MAX };
        let response = TransportHeader::Response {
            sender: birth(),
            delivery_seq: u64::MAX,
            retire_below: u64::MAX,
            invocation_id: DeliveryId { sender: birth(), delivery_seq: u64::MAX },
            seq: u32::MAX,
        };
        let ack = TransportHeader::Ack { sender: birth(), delivery_seq: u64::MAX };
        let len = |h: &TransportHeader| postcard::to_allocvec(h).unwrap().len();
        assert_eq!((len(&request), len(&response), len(&ack)), (47, 88, 37), "the largest of each kind");
        assert!(len(&response) <= MAX_HEADER_BYTES);
    }

    #[test]
    fn a_header_over_its_bound_or_of_an_unknown_kind_is_a_violation() {
        let mut over = Vec::new();
        encode_varint(MAX_HEADER_BYTES as u64 + 1, &mut over);
        assert!(matches!(decode_header(&over), HeaderRead::Bad(_)), "refused from the declared length, before a byte of it is read");
        let unknown = hex::decode(format!("1c03{BIRTH}01")).unwrap();
        assert!(matches!(decode_header(&unknown), HeaderRead::Bad(_)), "kind 3 does not exist");
        let trailing = hex::decode(format!("1d02{BIRTH}0100")).unwrap();
        assert!(matches!(decode_header(&trailing), HeaderRead::Bad(_)), "bytes past the header inside its section");
        let a_request_without_a_header = encode_request(&t(), b"hi");
        assert!(matches!(decode_header(&a_request_without_a_header), HeaderRead::Bad(_)), "a protocol section where the header belongs");
        let whole = encode_ack_frame(&TransportHeader::Ack { sender: birth(), delivery_seq: 1 });
        for cut in 0..whole.len() {
            assert_eq!(decode_header(&whole[..cut]), HeaderRead::NeedMore, "a prefix of {cut} bytes needs more");
        }
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
