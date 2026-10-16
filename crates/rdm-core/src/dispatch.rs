//! Dispatch and certainty rules (PRD §13; ownership amendment §6–§7, §11;
//! node-rpc.md §16, §26, §40).
//!
//! [`RequestAssembly`] is the server's state machine for one request frame, after its transport
//! header. A request reaches its protocol handler only after the target was read and its
//! op checked against what this process serves (no receiver refuses a call for
//! the node id it names, R-UC1a),
//! the length decoded and the complete payload read within the protocol ceiling: the frame is
//! complete, and the bytes after it are the stream's next frames (the caller's acks). A request
//! the sender reset or ended before its frame was complete never dispatches. The client's side of the same table is
//! [`crate::outcome`]; [`replay_verdict`] is the rule that no mutating
//! `Indeterminate` is silently replayed.

use crate::catalog::SealedCatalog;
use crate::codes::ResetCode;
use crate::framing::{parse_request_length, parse_request_target, RequestHead, RequestHeader};
use crate::outcome::{MalformedKind, RpcOutcome};

/// What the server does with a request direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAction {
    /// Keep reading.
    Continue,
    /// Reset the stream with `421 UNSERVED_OP`; no protocol dispatch.
    ResetUnserved {
        /// The op that is not served.
        op: u8,
    },
    /// Reply the protocol's typed `Malformed(kind)` on the send half, then
    /// stop the receive half with `422 REQUEST_STOP`. The handler never runs.
    RefuseMalformed {
        /// The op.
        op: u8,
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// Reset with `424 PROTOCOL_VIOLATION`; the handler never runs.
    ResetViolation {
        /// The op, when the target could be read.
        op: Option<u8>,
        /// Which rule the request broke.
        reason: &'static str,
    },
    /// The sender reset the unfinished request (`499`) or the direction ended
    /// without a complete frame: drop it, never dispatch.
    Drop {
        /// Why the request is dropped.
        reason: &'static str,
    },
    /// The complete request frame: decode and dispatch.
    Dispatch {
        /// The op.
        op: u8,
        /// The target and context.
        header: RequestHeader,
        /// The request body.
        payload: Vec<u8>,
    },
}

/// The request direction, fed by the transport.
pub struct RequestAssembly<'c> {
    catalog: &'c SealedCatalog,
    buf: Vec<u8>,
    header: Option<RequestHeader>,
    head: Option<(u8, usize, usize)>,
    done: bool,
    rest: Vec<u8>,
}

impl std::fmt::Debug for RequestAssembly<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestAssembly").field("header", &self.header).field("head", &self.head).field("done", &self.done).finish()
    }
}

impl<'c> RequestAssembly<'c> {
    /// The request direction of a server serving what `catalog` seals.
    pub fn new(catalog: &'c SealedCatalog) -> Self {
        Self { catalog, buf: Vec::new(), header: None, head: None, done: false, rest: Vec::new() }
    }

    /// The op, once the head has been read and the op is served.
    pub fn head_op(&self) -> Option<u8> {
        self.head.map(|(t, _, _)| t)
    }

    /// Bytes arrived. Head decisions (unserved op, oversize declaration) are made as soon as the
    /// head is readable — before the body.
    pub fn push(&mut self, bytes: &[u8]) -> ServerAction {
        if self.done {
            return ServerAction::Continue;
        }
        self.buf.extend_from_slice(bytes);
        if self.head.is_none() {
            let read = match parse_request_target(&self.buf, |t| self.catalog.request_ceiling(t)) {
                Ok(read) => read,
                Err(RequestHead::Unserved { op }) => return self.end(ServerAction::ResetUnserved { op }),
                Err(RequestHead::BadTarget { op }) => return self.end(ServerAction::ResetViolation { op, reason: "bad request target" }),
                Err(_) => return ServerAction::Continue,
            };
            if self.header.is_none() {
                self.header = Some(read.header.clone());
            }
            match parse_request_length(&self.buf, read) {
                RequestHead::Targeted { .. } | RequestHead::NeedMore => return ServerAction::Continue,
                RequestHead::TooLarge { op, .. } => {
                    return self.end(ServerAction::RefuseMalformed { op, kind: MalformedKind::TooLarge })
                }
                RequestHead::BadLength { op } => {
                    return self.end(ServerAction::ResetViolation { op: Some(op), reason: "bad length prefix" })
                }
                RequestHead::Ready { op, payload_len, head_len, .. } => self.head = Some((op, payload_len, head_len)),
                RequestHead::Unserved { .. } | RequestHead::BadTarget { .. } => unreachable!("decided in stage one"),
            }
        }
        let (op, len, head) = self.head.expect("set above");
        if self.buf.len() < head + len {
            return ServerAction::Continue;
        }
        // The frame is complete: what follows it is the stream's next frames.
        self.rest = self.buf.split_off(head + len);
        let payload = self.buf.split_off(head);
        let header = self.header.clone().expect("a head is set only after its header");
        self.end(ServerAction::Dispatch { op, header, payload })
    }

    /// The bytes that arrived after the dispatched request frame: the start of the stream's next
    /// frames.
    pub fn take_rest(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.rest)
    }

    /// The sender finished the request direction (FIN) before the request frame was complete.
    pub fn finish(&mut self) -> ServerAction {
        if self.done {
            return ServerAction::Continue;
        }
        match self.head {
            Some((op, _, _)) => self.end(ServerAction::ResetViolation { op: Some(op), reason: "FIN before the declared length" }),
            None if self.buf.is_empty() => self.end(ServerAction::Drop { reason: "empty request direction" }),
            None => self.end(ServerAction::ResetViolation { op: self.buf.first().copied(), reason: "FIN inside the request head" }),
        }
    }

    /// The sender reset the request direction with `code`.
    pub fn reset(&mut self, code: u64) -> ServerAction {
        if self.done {
            return ServerAction::Continue;
        }
        let reason = if ResetCode::from_code(code) == Some(ResetCode::FrameNotSent) {
            "sender reset an unfinished request (499 FRAME_NOT_SENT)"
        } else {
            "sender reset the request direction"
        };
        self.end(ServerAction::Drop { reason })
    }

    fn end(&mut self, a: ServerAction) -> ServerAction {
        self.done = true;
        a
    }
}

/// Whether the caller's state machine is allowed to issue the same mutation
/// again as a new attempt. Node RPC itself never retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVerdict {
    /// The call produced a reply: there is nothing to replay.
    Answered,
    /// Provably never dispatched: a new attempt cannot double-apply.
    SafeToRetry,
    /// May have executed. A mutation must be reconciled by the domain
    /// (read back, target, idempotency key) — never silently re-sent.
    MustNotReplay,
    /// A read may be repeated.
    ReadMayRepeat,
}

/// Whether `outcome` of a call that `mutating` may be sent again as a new attempt.
pub fn replay_verdict<R>(outcome: &RpcOutcome<R>, mutating: bool) -> ReplayVerdict {
    match outcome {
        RpcOutcome::Reply(_) => ReplayVerdict::Answered,
        RpcOutcome::NotSent(_) | RpcOutcome::Unserved(_) | RpcOutcome::RejectedStale(_) => ReplayVerdict::SafeToRetry,
        RpcOutcome::Indeterminate(_) if mutating => ReplayVerdict::MustNotReplay,
        RpcOutcome::Indeterminate(_) => ReplayVerdict::ReadMayRepeat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogBuilder, CatalogEntry, Shape, OpOwner};
    use crate::ping::{Ping, PingReply, PingRequest};
    use crate::framing::{encode_request, encode_varint, Target};
    use crate::outcome::{IndeterminateReason, NotSentReason, PreCommit, ReplyKind, RequestFinished};
    use crate::protocol::{DecodeFailure, NodeProtocol};

    fn catalog() -> SealedCatalog {
        CatalogBuilder::new().serve(CatalogEntry::canonical::<Ping>(OpOwner::Core, Shape::Unary)).seal().unwrap()
    }

    fn a_request() -> RequestHeader {
        RequestHeader::target(Target { node_id: "n1".into(), op: 0x01 })
    }

    fn assembly(c: &SealedCatalog) -> RequestAssembly<'_> {
        RequestAssembly::new(c)
    }

    fn echo_frame(payload: &[u8]) -> Vec<u8> {
        let req = PingRequest::Ping { payload: payload.to_vec() };
        encode_request(&a_request(), &Ping::encode_request(&req).unwrap())
    }

    // ---- PRD §13 dispatch table ----

    #[test]
    fn known_tag_and_known_opcode_dispatch_to_the_handler_once_the_frame_is_complete() {
        let c = catalog();
        let mut a = assembly(&c);
        let f = echo_frame(b"ping");
        assert_eq!(a.push(&f[..3]), ServerAction::Continue);
        match a.push(&f[3..]) {
            ServerAction::Dispatch { op, header: t, payload } => {
                assert_eq!(op, 0x01);
                assert_eq!(t, a_request(), "the dispatched request carries the target it named");
                assert!(matches!(Ping::decode_request(&payload), Ok(PingRequest::Ping { .. })));
            }
            other => panic!("expected dispatch, got {other:?}"),
        }
    }

    #[test]
    fn known_tag_with_unknown_opcode_is_malformed_unknown_variant() {
        let mut body = Vec::new();
        encode_varint(7, &mut body); // op variant 7 does not exist
        let f = encode_request(&a_request(), &body);
        let c = catalog();
        let mut a = assembly(&c);
        let ServerAction::Dispatch { payload, .. } = a.push(&f) else { panic!() };
        // Decode is the dispatcher's first step; its failure is the typed reply.
        assert_eq!(Ping::decode_request(&payload), Err(DecodeFailure::UnknownVariant));
        let reply = Ping::malformed(DecodeFailure::UnknownVariant.into());
        assert_eq!(Ping::classify_reply(&reply), ReplyKind::Malformed(MalformedKind::UnknownVariant));
    }

    #[test]
    fn unknown_tag_is_421_before_any_body() {
        let c = catalog();
        let mut a = assembly(&c);
        let f = encode_request(&RequestHeader::target(Target { node_id: "n1".into(), op: 0x42 }), b"body");
        let target_end = f.len() - 1 - 1 - 4; // before the context, the length and the body
        assert_eq!(a.push(&f[..target_end]), ServerAction::ResetUnserved { op: 0x42 }, "decided on the target alone");
        assert_eq!(a.push(&f[target_end..]), ServerAction::Continue, "nothing after the decision");
    }

    #[test]
    fn known_oversize_is_typed_too_large_after_the_tag_read() {
        let c = catalog();
        let mut a = assembly(&c);
        let mut head = encode_request(&a_request(), b"");
        head.pop();
        encode_varint(Ping::MAX_REQUEST_FRAME_BYTES as u64 + 1, &mut head);
        assert_eq!(a.push(&head), ServerAction::RefuseMalformed { op: 0x01, kind: MalformedKind::TooLarge });
    }

    /// CONTRACT (R-UC1a, Luke 2026-10-12: "no receiver refuses a Node RPC call for the node id it
    /// names"): a request whose target names another node, or no node (a call to the endpoint of a
    /// call that reached the caller), is read, length-checked and dispatched to its served op like
    /// any other, the target carried as it came. Must NOT happen: a `425` reset, or any decision on the
    /// target's node id.
    #[test]
    fn a_target_naming_another_node_or_none_is_dispatched_never_refused() {
        let c = catalog();
        let req = Ping::encode_request(&PingRequest::Ping { payload: b"x".to_vec() }).unwrap();
        for named in [Target { node_id: "n2".into(), op: 0x01 }, Target { node_id: "".into(), op: 0x01 }] {
            let mut a = assembly(&c);
            match a.push(&encode_request(&RequestHeader::target(named.clone()), &req)) {
                ServerAction::Dispatch { op, header, .. } => {
                    assert_eq!(op, 0x01);
                    assert_eq!(header.target, named, "the dispatched request carries the target as it came");
                }
                other => panic!("a target naming {:?} is dispatched, never refused: {other:?}", named.node_id),
            }
        }
        // An oversize declaration is refused on its size alone, whatever node the target names.
        let mut a = assembly(&c);
        let mut f = encode_request(&RequestHeader::target(Target { node_id: "n2".into(), op: 0x01 }), b"");
        f.pop();
        encode_varint(Ping::MAX_REQUEST_FRAME_BYTES as u64 + 1, &mut f);
        assert_eq!(a.push(&f), ServerAction::RefuseMalformed { op: 0x01, kind: MalformedKind::TooLarge });
    }

    #[test]
    fn a_non_forwardable_family_is_marked_in_the_catalog() {
        assert!(!catalog().lookup(Ping::OP).unwrap().forwardable, "carriers refuse it by type (e6.s4)");
    }

    // ---- complete-send certainty table ----

    #[test]
    fn before_request_finish_a_499_reset_is_never_dispatched_and_is_not_sent() {
        let c = catalog();
        let mut a = assembly(&c);
        let f = echo_frame(b"partial");
        a.push(&f[..f.len() - 2]);
        assert!(matches!(a.reset(499), ServerAction::Drop { reason } if reason.contains("499")));
        assert_eq!(a.finish(), ServerAction::Continue, "a dropped request never dispatches later");
        assert_eq!(a.push(&f[f.len() - 2..]), ServerAction::Continue, "nor when its last bytes arrive after the reset");
        let (code, outcome): (_, RpcOutcome<PingReply>) = PreCommit::begin(0x01).cut_before_finish();
        assert_eq!(code, ResetCode::FrameNotSent);
        assert!(matches!(&outcome, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent));
    }

    #[test]
    fn a_fin_before_the_declared_length_is_a_violation_and_bytes_past_it_are_the_next_frames() {
        let c = catalog();
        let f = echo_frame(b"abc");
        let mut a = assembly(&c);
        a.push(&f[..f.len() - 1]);
        assert!(matches!(a.finish(), ServerAction::ResetViolation { .. }));
        let mut b = assembly(&c);
        let mut long = f.clone();
        long.extend([7, 7]);
        assert!(matches!(b.push(&long), ServerAction::Dispatch { .. }), "the request frame is complete");
        assert_eq!(b.take_rest(), vec![7, 7], "what follows it is held for the stream's next frames");
    }

    fn committed() -> crate::outcome::Committed {
        PreCommit::begin(0x01).commit(RequestFinished::after_clean_finish(1, 1, true).unwrap())
    }

    #[test]
    fn complete_request_and_valid_reply_is_reply() {
        let bytes = Ping::encode_reply(&PingReply::Pong { payload: vec![1] }).unwrap();
        assert_eq!(committed().reply::<Ping>(&bytes).name(), "Reply");
    }

    #[test]
    fn complete_request_and_unserved_proof_is_unserved() {
        assert_eq!(committed().reset::<PingReply>(421).name(), "Unserved");
    }

    /// CONTRACT (R-UC1a): `RejectedStale` is the caller's own verdict, its resolver no longer naming
    /// the birth it dialled, before the request went: never dispatched, safe to retry. A `425` after
    /// commit proves nothing (no receiver sends it) and is `Indeterminate`.
    #[test]
    fn a_stale_target_is_the_callers_verdict_before_the_request_and_a_425_after_commit_proves_nothing() {
        let early: RpcOutcome<PingReply> = PreCommit::begin(0x01).stale_before_finish(&a_request().target);
        assert!(matches!(&early, RpcOutcome::RejectedStale(s) if s.target_node_id() == "n1"), "{early:?}");
        assert!(early.proves_not_dispatched());
        assert_eq!(replay_verdict(&early, true), ReplayVerdict::SafeToRetry);
        let late = committed().reset::<PingReply>(425);
        assert!(matches!(&late, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::Reset(425)), "{late:?}");
    }

    #[test]
    fn complete_request_and_reply_loss_or_unknown_reply_version_is_indeterminate() {
        let lost: RpcOutcome<PingReply> = committed().indeterminate(IndeterminateReason::ReplyLost("stream reset".into()));
        assert_eq!(lost.name(), "Indeterminate");
        let mut unknown = Vec::new();
        encode_varint(42, &mut unknown);
        assert!(matches!(
            committed().reply::<Ping>(&unknown),
            RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::UnsupportedReplyVariant
        ));
    }

    #[test]
    fn no_mutating_indeterminate_is_silently_replayed() {
        let ind: RpcOutcome<PingReply> = committed().indeterminate(IndeterminateReason::ReplyDeadline);
        assert_eq!(replay_verdict(&ind, true), ReplayVerdict::MustNotReplay);
        assert_eq!(replay_verdict(&ind, false), ReplayVerdict::ReadMayRepeat);
        let ns: RpcOutcome<PingReply> = PreCommit::begin(0x01).not_sent(NotSentReason::Deadline);
        assert_eq!(replay_verdict(&ns, true), ReplayVerdict::SafeToRetry);
        assert_eq!(replay_verdict(&committed().reset::<PingReply>(421), true), ReplayVerdict::SafeToRetry);
        let bytes = Ping::encode_reply(&Ping::busy("x".into())).unwrap();
        assert_eq!(replay_verdict(&committed().reply::<Ping>(&bytes), true), ReplayVerdict::Answered);
    }
}
