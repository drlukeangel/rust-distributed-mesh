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
    /// A carrier proved the inner call never committed at the final target.
    Carried(String),
    /// The protocol is not forwardable, so it never travels through a carrier.
    NotForwardable { tag: u8 },
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
    /// The inner call committed at the final target; the carrier could not learn its outcome.
    Carried(String),
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

    /// The server stopped the request direction (`422 REQUEST_STOP`) before
    /// this caller finished it: an early typed refusal (TooLarge, Busy,
    /// Draining, ...). The request never had its FIN, so it was never
    /// dispatched; a valid typed reply is that refusal (node-rpc.md §27).
    pub fn stopped_before_finish(self) -> EarlyRefusal {
        EarlyRefusal { tag: self.tag }
    }

    /// The receiver answered the unfinished request with `421 UNSERVED_TAG`:
    /// proof that its tag was never dispatched.
    pub fn unserved_before_finish<R>(self) -> RpcOutcome<R> {
        RpcOutcome::Unserved(Unserved { tag: self.tag })
    }
}

/// A request the server refused before the caller could finish it.
#[derive(Debug)]
pub struct EarlyRefusal {
    tag: u8,
}

impl EarlyRefusal {
    pub fn tag(&self) -> u8 {
        self.tag
    }

    /// A valid typed reply is the refusal (`Reply`); without one the request
    /// is still provably undispatched (`NotSent`).
    /// The early refusal's payload undecoded, for a carrier handing it across verbatim.
    pub fn relayed(self, reply_payload: Option<&[u8]>) -> RpcOutcome<Vec<u8>> {
        match reply_payload {
            Some(b) => RpcOutcome::Reply(Replied { value: b.to_vec(), class: ReplyKind::Unclassified }),
            None => RpcOutcome::NotSent(NotSent { reason: NotSentReason::FrameNotSent }),
        }
    }

    pub fn reply<P: NodeProtocol>(self, reply_payload: Option<&[u8]>) -> RpcOutcome<P::Reply> {
        match reply_payload.map(P::decode_reply) {
            Some(Ok(value)) => {
                let class = P::classify_reply(&value);
                RpcOutcome::Reply(Replied { value, class })
            }
            _ => RpcOutcome::NotSent(NotSent { reason: NotSentReason::FrameNotSent }),
        }
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

    /// The reply payload undecoded, for a carrier handing an inner reply across verbatim.
    pub fn relayed(self, reply_payload: &[u8]) -> RpcOutcome<Vec<u8>> {
        RpcOutcome::Reply(Replied { value: reply_payload.to_vec(), class: ReplyKind::Unclassified })
    }
}

/// The outcome of a carried call of protocol `P`, from the outcome of its outer forward call to
/// the carrier (node-rpc.md §36.1). Certainty composes: the outer call proven not dispatched, or
/// the carrier proving the inner call never committed, is `NotSent`; an outer or inner call that
/// committed with its outcome unknown is `Indeterminate`; only the target's own reply is a reply.
pub fn carried<P: NodeProtocol>(outer: RpcOutcome<crate::forward::ForwardReply>) -> RpcOutcome<P::Reply> {
    use crate::forward::{Forward, ForwardReply};
    let reply = match outer {
        RpcOutcome::Reply(r) => r.into_value(),
        RpcOutcome::NotSent(n) => return RpcOutcome::NotSent(n),
        RpcOutcome::Unserved(_) => return RpcOutcome::Unserved(Unserved { tag: Forward::TAG }),
        RpcOutcome::Indeterminate(i) => return RpcOutcome::Indeterminate(i),
    };
    let not_sent = |reason: NotSentReason| RpcOutcome::NotSent(NotSent { reason });
    let indeterminate = |reason: IndeterminateReason| RpcOutcome::Indeterminate(Indeterminate { reason });
    match reply {
        ForwardReply::Relayed { inner } => match P::decode_reply(&inner) {
            Ok(value) => {
                let class = P::classify_reply(&value);
                RpcOutcome::Reply(Replied { value, class })
            }
            Err(DecodeFailure::UnknownVariant) => indeterminate(IndeterminateReason::UnsupportedReplyVariant),
            Err(DecodeFailure::Corrupt) => indeterminate(IndeterminateReason::CorruptReply),
        },
        ForwardReply::InnerNotSent { reason } => not_sent(NotSentReason::Carried(reason)),
        ForwardReply::InnerUnserved { tag } => RpcOutcome::Unserved(Unserved { tag }),
        ForwardReply::InnerIndeterminate { reason } => indeterminate(IndeterminateReason::Carried(reason)),
        ForwardReply::NotForwardable { tag } => not_sent(NotSentReason::NotForwardable { tag }),
        // The carrier refused the forward itself, before any inner call.
        ForwardReply::PeerUnresolved { reason }
        | ForwardReply::NotReady { reason }
        | ForwardReply::Busy { reason }
        | ForwardReply::Draining { reason }
        | ForwardReply::Unauthorized { reason } => not_sent(NotSentReason::Carried(reason)),
        ForwardReply::Malformed { kind } => {
            not_sent(NotSentReason::Carried(format!("the carrier refused the forward as malformed: {kind:?}")))
        }
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
    fn an_early_refusal_is_its_typed_reply_or_not_sent() {
        let bytes = Echo::encode_reply(&Echo::malformed(MalformedKind::TooLarge)).unwrap();
        let o = PreCommit::begin(0x11).stopped_before_finish().reply::<Echo>(Some(&bytes));
        assert_eq!(o.reply().unwrap().class(), ReplyKind::Malformed(MalformedKind::TooLarge));
        let o = PreCommit::begin(0x11).stopped_before_finish().reply::<Echo>(None);
        assert!(matches!(&o, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent));
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
