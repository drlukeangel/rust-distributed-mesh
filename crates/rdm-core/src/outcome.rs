//! The local outcome algebra (node-rpc.md §24, §40; ownership amendment §6).
//!
//! `RpcOutcome = Reply | NotSent | Unserved | RejectedStale | Indeterminate`.
//! Typed domain refusals are classified *inside* `Reply`, never an outcome of
//! their own.
//!
//! Every outcome is constructible only from its proving condition. A call is a
//! [`PreCommit`] until the complete request has been written and the request
//! direction finished cleanly ([`RequestFinished`]); then it is a
//! [`Committed`]. Only a `PreCommit` can end `NotSent`; only a `Committed`
//! can end `Reply`, `Unserved` or `Indeterminate`. The payload structs have
//! private fields, so no caller can fabricate an outcome:
//!
//! ```compile_fail
//! use crate::outcome::{NotSent, NotSentReason};
//! let forged = NotSent { reason: NotSentReason::Deadline };
//! ```
//!
//! ```compile_fail
//! use crate::outcome::Committed;
//! let skipped_the_commit_cut = Committed { op: 0x01 };
//! ```

use crate::codes::ResetCode;
use crate::protocol::{DecodeFailure, NodeProtocol};

/// The standard gRPC outcome codes. Derived from typed outcomes; failed Build receipts retain the numeric code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "u8", into = "u8")]
#[repr(u8)]
pub enum Code {
    /// Standard code 0: `OK`.
    ///
    /// Position 0 on the wire.
    Ok = 0,
    /// Standard code 1: `CANCELLED`.
    ///
    /// Position 1 on the wire.
    Cancelled = 1,
    /// Standard code 2: `UNKNOWN`.
    ///
    /// Position 2 on the wire.
    Unknown = 2,
    /// Standard code 3: `INVALID_ARGUMENT`.
    ///
    /// Position 3 on the wire.
    InvalidArgument = 3,
    /// Standard code 4: `DEADLINE_EXCEEDED`.
    ///
    /// Position 4 on the wire.
    DeadlineExceeded = 4,
    /// Standard code 5: `NOT_FOUND`.
    ///
    /// Position 5 on the wire.
    NotFound = 5,
    /// Standard code 6: `ALREADY_EXISTS`.
    ///
    /// Position 6 on the wire.
    AlreadyExists = 6,
    /// Standard code 7: `PERMISSION_DENIED`.
    ///
    /// Position 7 on the wire.
    PermissionDenied = 7,
    /// Standard code 8: `RESOURCE_EXHAUSTED`.
    ///
    /// Position 8 on the wire.
    ResourceExhausted = 8,
    /// Standard code 9: `FAILED_PRECONDITION`.
    ///
    /// Position 9 on the wire.
    FailedPrecondition = 9,
    /// Standard code 10: `ABORTED`.
    ///
    /// Position 10 on the wire.
    Aborted = 10,
    /// Standard code 11: `OUT_OF_RANGE`.
    ///
    /// Position 11 on the wire.
    OutOfRange = 11,
    /// Standard code 12: `UNIMPLEMENTED`.
    ///
    /// Position 12 on the wire.
    Unimplemented = 12,
    /// Standard code 13: `INTERNAL`.
    ///
    /// Position 13 on the wire.
    Internal = 13,
    /// Standard code 14: `UNAVAILABLE`.
    ///
    /// Position 14 on the wire.
    Unavailable = 14,
    /// Standard code 15: `DATA_LOSS`.
    ///
    /// Position 15 on the wire.
    DataLoss = 15,
    /// Standard code 16: `UNAUTHENTICATED`.
    ///
    /// Position 16 on the wire.
    Unauthenticated = 16,
}

impl Code {
    /// All 17 standard codes, in numeric order.
    pub const ALL: [Self; 17] = [Self::Ok, Self::Cancelled, Self::Unknown, Self::InvalidArgument, Self::DeadlineExceeded, Self::NotFound, Self::AlreadyExists, Self::PermissionDenied, Self::ResourceExhausted, Self::FailedPrecondition, Self::Aborted, Self::OutOfRange, Self::Unimplemented, Self::Internal, Self::Unavailable, Self::DataLoss, Self::Unauthenticated];
    /// Standard numeric value.
    pub const fn number(self) -> u8 { self as u8 }
    /// Standard spelling.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Cancelled => "CANCELLED",
            Self::Unknown => "UNKNOWN",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Aborted => "ABORTED",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::Unimplemented => "UNIMPLEMENTED",
            Self::Internal => "INTERNAL",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::Unauthenticated => "UNAUTHENTICATED",
        }
    }
    /// Decode a standard numeric value; no extra top-level code exists.
    pub fn from_number(n: u8) -> Option<Self> { Self::ALL.into_iter().find(|c| c.number() == n) }
}

impl From<Code> for u8 {
    fn from(code: Code) -> Self { code.number() }
}
impl TryFrom<u8> for Code {
    type Error = &'static str;
    fn try_from(n: u8) -> Result<Self, Self::Error> {
        Self::from_number(n).ok_or("unknown standard outcome code")
    }
}
/// Classification of a failed-state receipt written before codes were retained.
pub fn legacy_failed_code() -> Code { Code::FailedPrecondition }

/// A view over the standard code. RDM's typed detail is the protocol reply in `Replied<R>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplyKind { code: Code }

#[allow(non_upper_case_globals, non_snake_case)]
impl ReplyKind {
    /// View a derived standard code; no parallel classification namespace exists.
    pub const fn new(code: Code) -> Self { Self { code } }
    /// The standard code.
    pub const fn code(self) -> Code { self.code }
    /// Applied, read result or completion.
    pub const Success: Self = Self::new(Code::Ok);
    /// The required subject is not known.
    pub const PeerUnresolved: Self = Self::new(Code::NotFound);
    /// The receiver is not ready.
    pub const NotReady: Self = Self::new(Code::Unavailable);
    /// The receiver is busy.
    pub const Busy: Self = Self::new(Code::Unavailable);
    /// The receiver takes no new work.
    pub const ConnectionsDeleting: Self = Self::new(Code::Unavailable);
    /// Reserved codec meaning only; never produced by a mesh handler.
    pub const Unauthorized: Self = Self::new(Code::PermissionDenied);
    /// The malformed kind retains its own standard code.
    pub const fn Malformed(kind: MalformedKind) -> Self {
        Self::new(match kind {
            MalformedKind::TooLarge | MalformedKind::TypesOverCap => Code::ResourceExhausted,
            MalformedKind::UnknownVariant => Code::InvalidArgument,
            MalformedKind::Corrupt => Code::DataLoss,
        })
    }
}

/// The named result of one item in a served set edit. Lists stay in their existing wire positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementDetail {
    /// The item was applied.
    Applied,
    /// The tag was already carried.
    AlreadyCarried,
    /// The tag was not carried.
    NotCarried,
    /// The named connection is not an open entry (unknown or already closed in today's reply).
    NotOpen,
}

/// A borrowed element and its derived code; never an added wire field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElementOutcome<'a, T: ?Sized> {
    /// The requested element.
    pub element: &'a T,
    /// Its standard code.
    pub code: Code,
    /// Its named typed result.
    pub detail: ElementDetail,
}

/// The code view of the existing named-connection lists. A not-open id has no current open entry;
/// the existing list deliberately contains both unknown ids and entries that were already closed.
pub fn connection_outcomes<'a>(closed: &'a [crate::ConnectionId], not_open: &'a [crate::ConnectionId]) -> Vec<ElementOutcome<'a, crate::ConnectionId>> {
    closed.iter().map(|id| ElementOutcome { element: id, code: Code::Ok, detail: ElementDetail::Applied })
        .chain(not_open.iter().map(|id| ElementOutcome { element: id, code: Code::NotFound, detail: ElementDetail::NotOpen })).collect()
}

/// How a request frame was malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MalformedKind {
    /// Declared length over the protocol ceiling; body never read.
    ///
    /// Position 0 on the wire.
    TooLarge,
    /// A known op carrying an operation variant this build does not know.
    ///
    /// Position 1 on the wire.
    UnknownVariant,
    /// Undecodable bytes.
    ///
    /// Position 2 on the wire.
    Corrupt,
    /// App types exceed 256 entries or 8 KiB encoded.
    ///
    /// Position 3 on the wire.
    TypesOverCap,
}

/// Why a call ended before its commit cut. The request provably never
/// reached a protocol handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotSentReason {
    /// The resolver answered `Unknown`, `Gone` or `Unavailable`.
    Resolve(ResolveFailure),
    /// Dial / open_bi / port failure before the first write.
    Connection(String),
    /// No streaming permit before the deadline.
    StreamBudget,
    /// The caller deadline expired before commit.
    Deadline,
    /// The sender reset an unfinished request with `499 FRAME_NOT_SENT`.
    FrameNotSent,
    /// A carrier proved the inner call never committed at the final target.
    Carried(String),
    /// The carrier's own Direct connection to the final target is not Active, so it made no inner call
    /// that arrived; the string is the carrier's own account of that connection.
    ConnectionLost(String),
    /// The origin's remaining budget did not exceed the carrier's reply reserve, so the carrier
    /// made no inner call. Both are whole milliseconds.
    CarrierNoBudget {
        /// The origin's remaining budget.
        remaining_ms: u64,
        /// The carrier's reply reserve.
        reserve_ms: u64,
    },
    /// The protocol is not forwardable, so it never travels through a carrier.
    NotForwardable {
        /// The op that is not forwardable.
        op: u8,
    },
    /// Connections chose no route to the exact target: no leg was started.
    NoActiveRoute,
    /// The source admits no new connection: its `node.connection.delete { All }` closed its
    /// connections, and neither has its name apply resolved nor has it started since (proposal
    /// node-properties-own-commands.md §2.1, §2.4, the admission block). No connection opened, or the one a dial already had was closed
    /// before it was pooled; the string names the source and why it admits none.
    AdmissionBlocked(String),
}

impl NotSentReason {
    /// The local proving condition's code; an expired permit wait is an owner's bound too.
    pub fn code(&self) -> Code {
        match self {
            Self::Resolve(ResolveFailure::Unknown) => Code::NotFound,
            Self::Deadline | Self::StreamBudget => Code::DeadlineExceeded,
            Self::NotForwardable { .. } => Code::FailedPrecondition,
            _ => Code::Unavailable,
        }
    }
}

/// Why a resolver gave no node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResolveFailure {
    /// No node by that target was ever known.
    ///
    /// Position 0 on the wire.
    Unknown,
    /// The node deleted and does not return.
    ///
    /// Position 1 on the wire.
    Gone,
    /// The node is known and cannot be reached now.
    ///
    /// Position 2 on the wire.
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
    /// The receiving birth's exited verdict or accepted removal ended the owed request after its
    /// commit (node-rpc.md §43): named with what the resolver holds instead.
    ReceiverGone(String),
}

impl IndeterminateReason {
    /// The typed delivery cause name.
    pub fn detail_name(&self) -> &'static str { match self {
                Self::ReplyLost(_) => "reply-lost",
                Self::ReplyDeadline => "reply-deadline",
                Self::Reset(n) => match ResetCode::from_code(*n) {
                    Some(ResetCode::InternalRpcFailure) => "handler-fault",
                    Some(ResetCode::ProtocolViolation) => "protocol-violation",
                    _ => "reset",
                },
                Self::UnsupportedReplyVariant => "unsupported-reply-variant",
                Self::CorruptReply => "corrupt-reply",
                Self::ProtocolViolation(_) => "protocol-violation",
                Self::ReceiverGone(_) => "receiver-gone",
            } }

    /// Standard code derived from the typed cause, never the explanation.
    pub fn code(&self) -> Code {
        match self {
            Self::ReceiverGone(_) => Code::Unavailable,
            Self::ReplyDeadline => Code::DeadlineExceeded,
            Self::ReplyLost(_) => Code::Unknown,
            Self::Reset(n) if [ResetCode::InternalRpcFailure, ResetCode::ProtocolViolation].iter().any(|code| *n == u64::from(code.code())) => Code::Internal,
            Self::Reset(_) => Code::Unknown,
            Self::UnsupportedReplyVariant => Code::InvalidArgument,
            Self::CorruptReply => Code::DataLoss,
            Self::ProtocolViolation(_) => Code::Internal,
        }
    }
}

/// A decoded reply and how its protocol classified it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replied<R> {
    value: R,
    class: ReplyKind,
    detail_name: &'static str,
}

impl<R> Replied<R> {
    /// The decoded reply.
    pub fn value(&self) -> &R {
        &self.value
    }
    /// The typed named RDM result/refusal beside the code.
    pub fn detail(&self) -> &R { &self.value }
    /// The protocol’s typed result name, captured at decode.
    pub fn detail_name(&self) -> &'static str { self.detail_name }
    /// How the protocol classified the reply.
    pub fn class(&self) -> ReplyKind {
        self.class
    }
    /// Take the decoded reply.
    pub fn into_value(self) -> R {
        self.value
    }
}

/// A call that ended before its commit cut: the request never reached a protocol handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotSent {
    reason: NotSentReason,
}

impl NotSent {
    /// The code derived from the typed pre-commit reason.
    pub fn code(&self) -> Code { self.reason.code() }

    /// Why the call ended before the cut.
    pub fn reason(&self) -> &NotSentReason {
        &self.reason
    }
}

/// A call the receiver answered `421 UNSERVED_OP`: the op was never dispatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unserved {
    op: u8,
}

impl Unserved {
    /// The op the receiver does not serve.
    pub fn op(&self) -> u8 {
        self.op
    }
}

/// The caller's own stale target: its resolver stopped naming the birth the call was dialled to
/// before the request went. No receiver decides it: nothing inside the mesh refuses a call for the
/// node id it names (R-UC1a).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedStale {
    target_node_id: String,
}

impl RejectedStale {
    /// The node the caller addressed, whose birth its resolver no longer names.
    pub fn target_node_id(&self) -> &str {
        &self.target_node_id
    }
}

/// A committed call with no stronger proof than that it may have executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indeterminate {
    reason: IndeterminateReason,
}

impl Indeterminate {
    /// Standard code derived from the typed cause; certainty remains Indeterminate.
    pub fn code(&self) -> Code { self.reason.code() }

    /// Why no stronger proof exists.
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
    /// The receiver's `421 UNSERVED_OP` proves the op was never dispatched.
    Unserved(Unserved),
    /// The caller's resolver stopped naming the birth the call was dialled to, before the request
    /// went: never dispatched. It is not `NotSent`: the target itself is stale.
    RejectedStale(RejectedStale),
    /// Committed, and no stronger proof exists. Never silently replayed.
    Indeterminate(Indeterminate),
}

impl<R> RpcOutcome<R> {
    /// The typed result/refusal or delivery-reason name, independent of explanation text.
    pub fn detail_name(&self) -> &'static str {
        match self {
            Self::Reply(r) => r.detail_name(),
            Self::Unserved(_) => "unserved-op",
            Self::RejectedStale(_) => "stale-target",
            Self::NotSent(n) => match n.reason() {
                NotSentReason::Resolve(ResolveFailure::Unknown) => "peer-unresolved",
                NotSentReason::Resolve(ResolveFailure::Gone) => "receiver-gone",
                NotSentReason::Resolve(ResolveFailure::Unavailable) => "unavailable",
                NotSentReason::Connection(_) => "connection",
                NotSentReason::StreamBudget => "stream-budget",
                NotSentReason::Deadline => "deadline",
                NotSentReason::FrameNotSent => "frame-not-sent",
                NotSentReason::Carried(_) => "carried",
                NotSentReason::ConnectionLost(_) => "connection-lost",
                NotSentReason::CarrierNoBudget { .. } => "carrier-no-budget",
                NotSentReason::NotForwardable { .. } => "not-forwardable",
                NotSentReason::NoActiveRoute => "no-active-route",
                NotSentReason::AdmissionBlocked(_) => "admission-blocked",
            },
            Self::Indeterminate(i) => i.reason().detail_name(),
        }
    }

    /// The code derived from the reply or the local typed proving condition.
    pub fn code(&self) -> Code {
        match self {
            Self::Reply(r) => r.class.code(),
            Self::NotSent(n) => n.code(),
            Self::Indeterminate(i) => i.code(),
            Self::Unserved(_) => Code::Unimplemented,
            Self::RejectedStale(_) => Code::Aborted,
        }
    }

    /// The outcome's variant name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Reply(_) => "Reply",
            Self::NotSent(_) => "NotSent",
            Self::Unserved(_) => "Unserved",
            Self::RejectedStale(_) => "RejectedStale",
            Self::Indeterminate(_) => "Indeterminate",
        }
    }

    /// True only when the request provably never reached a protocol handler,
    /// so a new attempt cannot double-apply it. `Indeterminate` is never safe.
    pub fn proves_not_dispatched(&self) -> bool {
        matches!(self, Self::NotSent(_) | Self::Unserved(_) | Self::RejectedStale(_))
    }

    /// The reply, when the outcome is a `Reply`.
    pub fn reply(&self) -> Option<&Replied<R>> {
        match self {
            Self::Reply(r) => Some(r),
            _ => None,
        }
    }
}

impl RpcOutcome<()> {
    /// An outcome that carries no reply, typed for any reply: `None` for a `Reply`.
    pub fn retyped<R>(self) -> Option<RpcOutcome<R>> {
        match self {
            Self::Reply(_) => None,
            Self::NotSent(n) => Some(RpcOutcome::NotSent(n)),
            Self::Unserved(u) => Some(RpcOutcome::Unserved(u)),
            Self::RejectedStale(s) => Some(RpcOutcome::RejectedStale(s)),
            Self::Indeterminate(i) => Some(RpcOutcome::Indeterminate(i)),
        }
    }
}

/// A call that has not crossed its commit cut.
#[derive(Debug)]
pub struct PreCommit {
    op: u8,
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

    /// Issued only when every byte of the request frame was written. The frame is self-delimited,
    /// so its receiver dispatches it once it is complete; the request direction stays open for the
    /// caller's acks of the response (node-rpc-envelope.md "Transport frames").
    pub fn after_complete_write(bytes_written: usize, frame_len: usize) -> Option<Self> {
        (bytes_written == frame_len).then_some(Self { _private: () })
    }
}

/// A call past its commit cut.
#[derive(Debug)]
pub struct Committed {
    op: u8,
}

impl PreCommit {
    /// A call for `op` that has not crossed its commit cut.
    pub fn begin(op: u8) -> Self {
        Self { op }
    }

    /// The op of the call.
    pub fn op(&self) -> u8 {
        self.op
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
        Committed { op: self.op }
    }

    /// The server stopped the request direction (`422 REQUEST_STOP`) before
    /// this caller finished it: an early typed refusal (TooLarge, Busy,
    /// ConnectionsDeleting, ...). The request never had its FIN, so it was never
    /// dispatched; a valid typed reply is that refusal (node-rpc.md §27).
    pub fn stopped_before_finish(self) -> EarlyRefusal {
        EarlyRefusal { op: self.op }
    }

    /// The receiver answered the unfinished request with `421 UNSERVED_OP`:
    /// proof that its op was never dispatched.
    pub fn unserved_before_finish<R>(self) -> RpcOutcome<R> {
        RpcOutcome::Unserved(Unserved { op: self.op })
    }

    /// The call's target is stale: the caller found the node it addressed superseded before or during the dial.
    /// Definitive no-dispatch, never `NotSent`.
    pub fn stale_before_finish<R>(self, target: &crate::framing::Target) -> RpcOutcome<R> {
        RpcOutcome::RejectedStale(RejectedStale { target_node_id: target.node_id.clone() })
    }
}

/// A request the server refused before the caller could finish it.
#[derive(Debug)]
pub struct EarlyRefusal {
    op: u8,
}

impl EarlyRefusal {
    /// The op of the refused request.
    pub fn op(&self) -> u8 {
        self.op
    }

    /// The early refusal's payload undecoded, for a carrier handing it across verbatim.
    pub fn forwarded(self, reply_payload: Option<&[u8]>) -> RpcOutcome<crate::forward::ForwardReply> {
        match reply_payload {
            Some(b) => forwarded_reply(b),
            None => RpcOutcome::NotSent(NotSent { reason: NotSentReason::FrameNotSent }),
        }
    }

    /// A valid typed reply is the refusal (`Reply`); without one the request
    /// is still provably undispatched (`NotSent`).
    pub fn reply<P: NodeProtocol>(self, reply_payload: Option<&[u8]>) -> RpcOutcome<P::Reply> {
        match reply_payload.map(P::decode_reply) {
            Some(Ok(value)) => {
                let class = P::classify_reply(&value);
                let detail_name = P::reply_detail(&value);
                RpcOutcome::Reply(Replied { value, class, detail_name })
            }
            _ => RpcOutcome::NotSent(NotSent { reason: NotSentReason::FrameNotSent }),
        }
    }
}

impl Committed {
    /// The op of the committed call.
    pub fn op(&self) -> u8 {
        self.op
    }

    /// Decode the reply direction with protocol `P`'s codec and classifier.
    pub fn reply<P: NodeProtocol>(self, reply_payload: &[u8]) -> RpcOutcome<P::Reply> {
        match P::decode_reply(reply_payload) {
            Ok(value) => {
                let class = P::classify_reply(&value);
                let detail_name = P::reply_detail(&value);
                RpcOutcome::Reply(Replied { value, class, detail_name })
            }
            Err(DecodeFailure::UnknownVariant) => self.indeterminate(IndeterminateReason::UnsupportedReplyVariant),
            Err(DecodeFailure::Corrupt) => self.indeterminate(IndeterminateReason::CorruptReply),
        }
    }

    /// The stream was reset with `code` after commit. Only `421` proves non-dispatch; every other
    /// code, `425` included (no receiver sends it, R-UC1a), leaves the call `Indeterminate`.
    pub fn reset<R>(self, code: u64) -> RpcOutcome<R> {
        match ResetCode::from_code(code) {
            Some(ResetCode::UnservedOp) => RpcOutcome::Unserved(Unserved { op: self.op }),
            _ => self.indeterminate(IndeterminateReason::Reset(code)),
        }
    }

    /// No valid reply and no reset: lost, timed out, or violated framing.
    pub fn indeterminate<R>(self, reason: IndeterminateReason) -> RpcOutcome<R> {
        RpcOutcome::Indeterminate(Indeterminate { reason })
    }

    /// The reply payload undecoded, for a carrier handing an inner reply across verbatim.
    pub fn forwarded(self, reply_payload: &[u8]) -> RpcOutcome<crate::forward::ForwardReply> {
        forwarded_reply(reply_payload)
    }
}

// The carrier classifies its own Forward reply, while the origin decodes the inner protocol.
fn forwarded_reply(payload: &[u8]) -> RpcOutcome<crate::forward::ForwardReply> {
    let value = crate::forward::ForwardReply::Forwarded { inner: payload.to_vec() };
    let class = crate::forward::Forward::classify_reply(&value);
    let detail_name = crate::forward::Forward::reply_detail(&value);
    RpcOutcome::Reply(Replied { value, class, detail_name })
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
        RpcOutcome::Unserved(_) => return RpcOutcome::Unserved(Unserved { op: Forward::OP }),
        RpcOutcome::RejectedStale(s) => return RpcOutcome::RejectedStale(s),
        RpcOutcome::Indeterminate(i) => return RpcOutcome::Indeterminate(i),
    };
    let not_sent = |reason: NotSentReason| RpcOutcome::NotSent(NotSent { reason });
    let indeterminate = |reason: IndeterminateReason| RpcOutcome::Indeterminate(Indeterminate { reason });
    match reply {
        ForwardReply::Forwarded { inner } => match P::decode_reply(&inner) {
            Ok(value) => {
                let class = P::classify_reply(&value);
                let detail_name = P::reply_detail(&value);
                RpcOutcome::Reply(Replied { value, class, detail_name })
            }
            Err(DecodeFailure::UnknownVariant) => indeterminate(IndeterminateReason::UnsupportedReplyVariant),
            Err(DecodeFailure::Corrupt) => indeterminate(IndeterminateReason::CorruptReply),
        },
        ForwardReply::InnerNotSent { cause, reason } => not_sent(cause.into_reason(reason)),
        ForwardReply::InnerUnserved { op } => RpcOutcome::Unserved(Unserved { op }),
        ForwardReply::InnerRejectedStale { target_node_id } => RpcOutcome::RejectedStale(RejectedStale { target_node_id: target_node_id.into() }),
        ForwardReply::InnerIndeterminate { cause, reason } => indeterminate(cause.into_reason(reason)),
        ForwardReply::ConnectionLost { reason } => not_sent(NotSentReason::ConnectionLost(reason)),
        ForwardReply::NotForwardable { op } => not_sent(NotSentReason::NotForwardable { op }),
        ForwardReply::NoInnerBudget { remaining_ms, reserve_ms } => not_sent(NotSentReason::CarrierNoBudget { remaining_ms, reserve_ms }),
        // The carrier refused the forward itself, before any inner call.
        ForwardReply::PeerUnresolved { reason }
        | ForwardReply::NotReady { reason }
        | ForwardReply::Busy { reason }
        | ForwardReply::ConnectionsDeleting { reason }
        | ForwardReply::Unauthorized { reason } => not_sent(NotSentReason::Carried(reason)),
        ForwardReply::Malformed { kind } => {
            not_sent(NotSentReason::Carried(format!("the carrier refused the forward as malformed: {kind:?}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ping::{Ping, PingReply, PingRequest};
    use crate::protocol::NodeProtocol;

    fn committed() -> Committed {
        PreCommit::begin(Ping::OP).commit(RequestFinished::after_clean_finish(10, 10, true).unwrap())
    }

    #[test]
    fn the_commit_proof_needs_the_whole_frame_and_a_clean_finish() {
        assert!(RequestFinished::after_clean_finish(10, 10, true).is_some());
        assert!(RequestFinished::after_clean_finish(9, 10, true).is_none(), "partial write is not committed");
        assert!(RequestFinished::after_clean_finish(10, 10, false).is_none(), "failed finish is not committed");
    }

    #[test]
    fn not_sent_comes_only_from_pre_commit() {
        let o: RpcOutcome<PingReply> = PreCommit::begin(0x01).not_sent(NotSentReason::Deadline);
        assert_eq!(o.name(), "NotSent");
        assert!(o.proves_not_dispatched());
        let (code, cut): (_, RpcOutcome<PingReply>) = PreCommit::begin(0x01).cut_before_finish();
        assert_eq!(code.code(), 499);
        assert!(matches!(&cut, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent));
    }

    #[test]
    fn an_early_refusal_is_its_typed_reply_or_not_sent() {
        let bytes = Ping::encode_reply(&Ping::malformed(MalformedKind::TooLarge)).unwrap();
        let o = PreCommit::begin(0x01).stopped_before_finish().reply::<Ping>(Some(&bytes));
        assert_eq!(o.reply().unwrap().class(), ReplyKind::Malformed(MalformedKind::TooLarge));
        let o = PreCommit::begin(0x01).stopped_before_finish().reply::<Ping>(None);
        assert!(matches!(&o, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent));
    }

    #[test]
    fn unserved_comes_only_from_a_421_after_commit() {
        let o: RpcOutcome<PingReply> = committed().reset(421);
        assert!(matches!(&o, RpcOutcome::Unserved(u) if u.op() == 0x01));
        assert!(o.proves_not_dispatched());
        // 425 included: no receiver sends it (R-UC1a), so it proves nothing.
        for code in [422u64, 423, 424, 425, 499, 0, 501] {
            let o: RpcOutcome<PingReply> = committed().reset(code);
            assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::Reset(code)), "{code}");
            assert!(!o.proves_not_dispatched());
        }
    }

    #[test]
    fn reply_comes_from_a_decoded_reply_with_its_class() {
        let bytes = Ping::encode_reply(&PingReply::Pong { payload: b"x".to_vec() }).unwrap();
        let o = committed().reply::<Ping>(&bytes);
        let r = o.reply().expect("Reply");
        assert_eq!(r.class(), ReplyKind::Success);
        let refusal = Ping::encode_reply(&Ping::busy("full".into())).unwrap();
        assert_eq!(committed().reply::<Ping>(&refusal).reply().unwrap().class(), ReplyKind::Busy, "a refusal is a classified Reply");
    }

    #[test]
    fn an_unknown_or_corrupt_reply_after_dispatch_is_indeterminate() {
        let mut unknown = Vec::new();
        crate::framing::encode_varint(999, &mut unknown);
        let o = committed().reply::<Ping>(&unknown);
        assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::UnsupportedReplyVariant));
        let o = committed().reply::<Ping>(&[0, 0xff, 0xff]);
        assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::CorruptReply));
        assert!(!o.proves_not_dispatched(), "Indeterminate is never safe to replay");
        let _ = PingRequest::Ping { payload: vec![] };
    }
}
