//! The local outcome algebra (node-rpc.md §24, §40; ownership amendment §6).
//!
//! `RpcOutcome = Reply | NotSent | Unserved | Indeterminate`. Typed domain
//! refusals are classified *inside* `Reply`, never a fifth outcome.
//!
//! Every outcome is constructible only from its proving condition. A call is a
//! [`PreCommit`] until the complete request has been written and the request
//! direction finished cleanly ([`RequestFinished`]); then it is a
//! [`Committed`]. Only a `PreCommit` can end `NotSent`; only a `Committed`
//! can end `Reply`, `Unserved` or `Indeterminate`. The payload structs have
//! private fields, so no caller can fabricate an outcome:
//!
//! ```compile_fail
//! use rafka_node_rpc_contract::outcome::{NotSent, NotSentReason};
//! let forged = NotSent { reason: NotSentReason::Deadline };
//! ```
//!
//! ```compile_fail
//! use rafka_node_rpc_contract::outcome::Committed;
//! let skipped_the_commit_cut = Committed { tag: 0x11 };
//! ```

use crate::codes::ResetCode;
use crate::protocol::{DecodeFailure, NodeProtocol};

/// Shared reply semantics; the bytes stay protocol-owned (node-rpc.md §25).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplyKind {
    Success,
    PeerUnresolved,
    NotReady,
    Busy,
    Draining,
    Malformed(MalformedKind),
    Unauthorized,
    ProtocolRefusal,
    Unclassified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MalformedKind {
    /// Declared length over the protocol ceiling; body never read.
    TooLarge,
    /// A known tag carrying an operation variant this build does not know.
    UnknownVariant,
    /// Undecodable bytes.
    Corrupt,
}

/// Why a call ended before its commit cut. The request provably never
/// reached a protocol handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotSentReason {
    /// The resolver answered `Unknown`, `Gone` or `Unavailable`.
    Resolve(ResolveFailure),
    /// Dial / open_bi / slot failure before the first write.
    Connection(String),
    /// No streaming permit before the deadline.
    StreamBudget,
    /// The exact endpoint slot/freshness target was superseded before commit.
    Superseded { slot: String },
    /// The caller deadline expired before commit.
    Deadline,
    /// The sender reset an unfinished request with `499 FRAME_NOT_SENT`.
    FrameNotSent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveFailure {
    Unknown,
    Gone,
    Unavailable,
}

/// Why a committed call has no stronger proof than "may have executed".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndeterminateReason {
    /// The reply never arrived (stream/connection lost).
    ReplyLost(String),
    /// The reply budget expired after commit.
    ReplyDeadline,
    /// The stream was reset with a code other than `421`.
    Reset(u64),
    /// A reply variant this caller does not know (dispatch happened).
    UnsupportedReplyVariant,
    /// Reply bytes that do not decode.
    CorruptReply,
    /// A framing/order violation on the reply direction.
    ProtocolViolation(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replied<R> {
    value: R,
    class: ReplyKind,
}

impl<R> Replied<R> {
    pub fn value(&self) -> &R {
        &self.value
    }
    pub fn class(&self) -> ReplyKind {
        self.class
    }
    pub fn into_value(self) -> R {
        self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotSent {
    reason: NotSentReason,
}

impl NotSent {
    pub fn reason(&self) -> &NotSentReason {
        &self.reason
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unserved {
    tag: u8,
}

impl Unserved {
    pub fn tag(&self) -> u8 {
        self.tag
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indeterminate {
    reason: IndeterminateReason,
}

impl Indeterminate {
    pub fn reason(&self) -> &IndeterminateReason {
        &self.reason
    }
}

/// The result of one Node RPC call (one attempt).
#[must_use = "an RpcOutcome carries certainty; dropping it loses NotSent vs Indeterminate"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcOutcome<R> {
    /// A valid typed reply; domain refusals are classified inside.
    Reply(Replied<R>),
    /// The request never reached dispatch.
    NotSent(NotSent),
    /// The receiver's `421 UNSERVED_TAG` proves the tag was never dispatched.
    Unserved(Unserved),
    /// Committed, and no stronger proof exists. Never silently replayed.
    Indeterminate(Indeterminate),
}

impl<R> RpcOutcome<R> {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Reply(_) => "Reply",
            Self::NotSent(_) => "NotSent",
            Self::Unserved(_) => "Unserved",
            Self::Indeterminate(_) => "Indeterminate",
        }
    }

    /// True only when the request provably never reached a protocol handler,
    /// so a new attempt cannot double-apply it. `Indeterminate` is never safe.
    pub fn proves_not_dispatched(&self) -> bool {
        matches!(self, Self::NotSent(_) | Self::Unserved(_))
    }

    pub fn reply(&self) -> Option<&Replied<R>> {
        match self {
            Self::Reply(r) => Some(r),
            _ => None,
        }
    }
}

/// A call that has not crossed its commit cut.
#[derive(Debug)]
pub struct PreCommit {
    tag: u8,
}

/// Proof that the complete request frame was written and the request send
/// direction finished cleanly — the commit cut (PRD §1.18).
#[derive(Debug)]
pub struct RequestFinished {
    _private: (),
}

impl RequestFinished {
    /// Issued only when every byte of the frame was written and `finish()`
    /// succeeded. An unfinished send gets no proof and stays `PreCommit`.
    pub fn after_clean_finish(bytes_written: usize, frame_len: usize, finish_ok: bool) -> Option<Self> {
        (finish_ok && bytes_written == frame_len).then_some(Self { _private: () })
    }
}

/// A call past its commit cut.
#[derive(Debug)]
pub struct Committed {
    tag: u8,
}

impl PreCommit {
    pub fn begin(tag: u8) -> Self {
        Self { tag }
    }

    pub fn tag(&self) -> u8 {
        self.tag
    }

    /// End before the cut.
    pub fn not_sent<R>(self, reason: NotSentReason) -> RpcOutcome<R> {
        RpcOutcome::NotSent(NotSent { reason })
    }

    /// The request was cut before it finished: the stream is reset with
    /// `499 FRAME_NOT_SENT`, the receiver never dispatches, the call is `NotSent`.
    pub fn cut_before_finish<R>(self) -> (ResetCode, RpcOutcome<R>) {
        (ResetCode::FrameNotSent, RpcOutcome::NotSent(NotSent { reason: NotSentReason::FrameNotSent }))
    }

    /// Cross the cut. From here no outcome may be `NotSent`.
    pub fn commit(self, _proof: RequestFinished) -> Committed {
        Committed { tag: self.tag }
    }
}

impl Committed {
    pub fn tag(&self) -> u8 {
        self.tag
    }

    /// Decode the reply direction with protocol `P`'s codec and classifier.
    pub fn reply<P: NodeProtocol>(self, reply_payload: &[u8]) -> RpcOutcome<P::Reply> {
        match P::decode_reply(reply_payload) {
            Ok(value) => {
                let class = P::classify_reply(&value);
                RpcOutcome::Reply(Replied { value, class })
            }
            Err(DecodeFailure::UnknownVariant) => self.indeterminate(IndeterminateReason::UnsupportedReplyVariant),
            Err(DecodeFailure::Corrupt) => self.indeterminate(IndeterminateReason::CorruptReply),
        }
    }

    /// The stream was reset with `code` after commit. Only `421` proves
    /// non-dispatch; every other code leaves the call `Indeterminate`.
    pub fn reset<R>(self, code: u64) -> RpcOutcome<R> {
        match ResetCode::from_code(code) {
            Some(ResetCode::UnservedTag) => RpcOutcome::Unserved(Unserved { tag: self.tag }),
            _ => self.indeterminate(IndeterminateReason::Reset(code)),
        }
    }

    /// No valid reply and no reset: lost, timed out, or violated framing.
    pub fn indeterminate<R>(self, reason: IndeterminateReason) -> RpcOutcome<R> {
        RpcOutcome::Indeterminate(Indeterminate { reason })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::echo::{Echo, EchoReply, EchoRequest};
    use crate::protocol::NodeProtocol;

    fn committed() -> Committed {
        PreCommit::begin(Echo::TAG).commit(RequestFinished::after_clean_finish(10, 10, true).unwrap())
    }

    #[test]
    fn the_commit_proof_needs_the_whole_frame_and_a_clean_finish() {
        assert!(RequestFinished::after_clean_finish(10, 10, true).is_some());
        assert!(RequestFinished::after_clean_finish(9, 10, true).is_none(), "partial write is not committed");
        assert!(RequestFinished::after_clean_finish(10, 10, false).is_none(), "failed finish is not committed");
    }

    #[test]
    fn not_sent_comes_only_from_pre_commit() {
        let o: RpcOutcome<EchoReply> = PreCommit::begin(0x11).not_sent(NotSentReason::Deadline);
        assert_eq!(o.name(), "NotSent");
        assert!(o.proves_not_dispatched());
        let (code, cut): (_, RpcOutcome<EchoReply>) = PreCommit::begin(0x11).cut_before_finish();
        assert_eq!(code.code(), 499);
        assert!(matches!(&cut, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent));
    }

    #[test]
    fn unserved_comes_only_from_a_421_after_commit() {
        let o: RpcOutcome<EchoReply> = committed().reset(421);
        assert!(matches!(&o, RpcOutcome::Unserved(u) if u.tag() == 0x11));
        assert!(o.proves_not_dispatched());
        for code in [422u64, 423, 424, 499, 0, 501] {
            let o: RpcOutcome<EchoReply> = committed().reset(code);
            assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::Reset(code)), "{code}");
            assert!(!o.proves_not_dispatched());
        }
    }

    #[test]
    fn reply_comes_from_a_decoded_reply_with_its_class() {
        let bytes = Echo::encode_reply(&EchoReply::Echoed { payload: b"x".to_vec() }).unwrap();
        let o = committed().reply::<Echo>(&bytes);
        let r = o.reply().expect("Reply");
        assert_eq!(r.class(), ReplyKind::Success);
        let refusal = Echo::encode_reply(&Echo::busy("full".into())).unwrap();
        assert_eq!(committed().reply::<Echo>(&refusal).reply().unwrap().class(), ReplyKind::Busy, "a refusal is a classified Reply");
    }

    #[test]
    fn an_unknown_or_corrupt_reply_after_dispatch_is_indeterminate() {
        let mut unknown = Vec::new();
        crate::framing::encode_varint(999, &mut unknown);
        let o = committed().reply::<Echo>(&unknown);
        assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::UnsupportedReplyVariant));
        let o = committed().reply::<Echo>(&[0, 0xff, 0xff]);
        assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::CorruptReply));
        assert!(!o.proves_not_dispatched(), "Indeterminate is never safe to replay");
        let _ = EchoRequest::Echo { traceparent: None, payload: vec![] };
    }
}
