//! Canonical length-first framing (node-rpc.md §10; ownership amendment §7).
//!
//! ```text
//! request direction:   protocol_tag: u8 | request_len: varint | payload | FIN
//! unary reply:         reply_len: varint | payload | FIN
//! server streaming:    (frame_len: varint | frame)* | FIN
//! ```
//!
//! The receiver reads the tag first (it names the protocol and therefore the
//! request ceiling), then the declared length, and refuses an oversize request
//! before allocating or reading its body.

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

/// `tag | varint(len) | payload`.
pub fn encode_request(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + MAX_VARINT_LEN + payload.len());
    out.push(tag);
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// A served tag within its ceiling; the body is `payload_len` bytes after `head_len`.
    Ready { tag: u8, payload_len: usize, head_len: usize },
}

/// Read the request head. `ceiling(tag)` is the protocol's
/// `MAX_REQUEST_FRAME_BYTES`, or `None` when the tag is not served.
pub fn parse_request_head(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> RequestHead {
    let Some(&tag) = buf.first() else { return RequestHead::NeedMore };
    let Some(max) = ceiling(tag) else { return RequestHead::Unserved { tag } };
    match decode_varint(&buf[1..]) {
        Varint::NeedMore => RequestHead::NeedMore,
        Varint::Overflow => RequestHead::BadLength { tag },
        Varint::Complete { value, len } if value > max as u64 => {
            let _ = len;
            RequestHead::TooLarge { tag, declared: value, max }
        }
        Varint::Complete { value, len } => RequestHead::Ready { tag, payload_len: value as usize, head_len: 1 + len },
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

/// Split a complete request direction into `(tag, payload)`.
pub fn decode_request(buf: &[u8], ceiling: impl Fn(u8) -> Option<usize>) -> Result<(u8, &[u8]), RequestHead> {
    match parse_request_head(buf, ceiling) {
        RequestHead::Ready { tag, payload_len, head_len } if buf.len() == head_len + payload_len => {
            Ok((tag, &buf[head_len..]))
        }
        RequestHead::Ready { .. } => Err(RequestHead::NeedMore),
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

    #[test]
    fn request_round_trips() {
        let frame = encode_request(0x11, b"hello");
        assert_eq!(decode_request(&frame, |_| Some(64)), Ok((0x11, &b"hello"[..])));
        assert_eq!(frame[0], 0x11, "the tag is the first byte");
    }

    #[test]
    fn unserved_is_decided_on_the_tag_byte_alone() {
        assert_eq!(parse_request_head(&[0x42], |t| (t == 0x11).then_some(64)), RequestHead::Unserved { tag: 0x42 });
    }

    #[test]
    fn oversize_is_refused_from_the_declared_length_before_the_body() {
        let mut head = vec![0x11];
        encode_varint(1 << 30, &mut head); // a 1 GiB declaration, no body at all
        assert_eq!(parse_request_head(&head, |_| Some(1024)), RequestHead::TooLarge { tag: 0x11, declared: 1 << 30, max: 1024 });
    }

    #[test]
    fn partial_heads_need_more_and_bad_lengths_are_violations() {
        assert_eq!(parse_request_head(&[], |_| Some(1)), RequestHead::NeedMore);
        assert_eq!(parse_request_head(&[0x11], |_| Some(1)), RequestHead::NeedMore);
        assert_eq!(parse_request_head(&[0x11, 0x80], |_| Some(1)), RequestHead::NeedMore);
        let mut bad = vec![0x11];
        bad.extend([0xff; 11]);
        assert_eq!(parse_request_head(&bad, |_| Some(1)), RequestHead::BadLength { tag: 0x11 });
        let frame = encode_request(0x11, b"hello");
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
