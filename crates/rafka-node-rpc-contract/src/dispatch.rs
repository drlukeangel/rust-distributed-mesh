//! Dispatch and certainty rules (PRD §13; ownership amendment §6–§7, §11;
//! node-rpc.md §16, §26, §40).
//!
//! [`RequestAssembly`] is the server's request-direction state machine. A
//! request reaches its protocol handler only after the tag was read, the
//! length decoded, the complete payload read within the protocol ceiling, the
//! request direction finished cleanly and the payload decoded. An unfinished
//! or reset request never dispatches. The client's side of the same table is
//! [`crate::outcome`]; [`replay_verdict`] is the rule that no mutating
//! `Indeterminate` is silently replayed.

use crate::catalog::SealedCatalog;
use crate::codes::ResetCode;
use crate::framing::{parse_request_head, RequestHead};
use crate::outcome::{MalformedKind, RpcOutcome};

/// What the server does with a request direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAction {
    /// Keep reading.
    Continue,
    /// Reset the stream with `421 UNSERVED_TAG`; no protocol dispatch.
    ResetUnserved { tag: u8 },
    /// Reply the protocol's typed `Malformed(kind)` on the send half, then
    /// stop the receive half with `422 REQUEST_STOP`. The handler never runs.
    RefuseMalformed { tag: u8, kind: MalformedKind },
    /// Reset with `424 PROTOCOL_VIOLATION`; the handler never runs.
    ResetViolation { tag: Option<u8>, reason: &'static str },
    /// The sender reset the unfinished request (`499`) or the direction ended
    /// without a complete frame: drop it, never dispatch.
    Drop { reason: &'static str },
    /// The complete, cleanly finished request: decode and dispatch.
    Dispatch { tag: u8, payload: Vec<u8> },
}

/// The request direction, fed by the transport.
#[derive(Debug)]
pub struct RequestAssembly<'c> {
    catalog: &'c SealedCatalog,
    buf: Vec<u8>,
    head: Option<(u8, usize, usize)>,
    done: bool,
}

impl<'c> RequestAssembly<'c> {
    pub fn new(catalog: &'c SealedCatalog) -> Self {
        Self { catalog, buf: Vec::new(), head: None, done: false }
    }

    /// Bytes arrived. Head decisions (unserved tag, oversize declaration) are
    /// made as soon as the head is readable — before the body.
    pub fn push(&mut self, bytes: &[u8]) -> ServerAction {
        if self.done {
            return ServerAction::Continue;
        }
        self.buf.extend_from_slice(bytes);
        if self.head.is_none() {
            match parse_request_head(&self.buf, |t| self.catalog.request_ceiling(t)) {
                RequestHead::NeedMore => return ServerAction::Continue,
                RequestHead::Unserved { tag } => return self.end(ServerAction::ResetUnserved { tag }),
                RequestHead::TooLarge { tag, .. } => {
                    return self.end(ServerAction::RefuseMalformed { tag, kind: MalformedKind::TooLarge })
                }
                RequestHead::BadLength { tag } => {
                    return self.end(ServerAction::ResetViolation { tag: Some(tag), reason: "bad length prefix" })
                }
                RequestHead::Ready { tag, payload_len, head_len } => self.head = Some((tag, payload_len, head_len)),
            }
        }
        let (tag, len, head) = self.head.expect("set above");
        if self.buf.len() > head + len {
            return self.end(ServerAction::ResetViolation { tag: Some(tag), reason: "bytes past the declared request length" });
        }
        ServerAction::Continue
    }

    /// The sender finished the request direction (FIN).
    pub fn finish(&mut self) -> ServerAction {
        if self.done {
            return ServerAction::Continue;
        }
        match self.head {
            Some((tag, len, head)) if self.buf.len() == head + len => {
                let payload = self.buf.split_off(head);
                self.end(ServerAction::Dispatch { tag, payload })
            }
            Some((tag, _, _)) => self.end(ServerAction::ResetViolation { tag: Some(tag), reason: "FIN before the declared length" }),
            None if self.buf.is_empty() => self.end(ServerAction::Drop { reason: "empty request direction" }),
            None => self.end(ServerAction::ResetViolation { tag: self.buf.first().copied(), reason: "FIN inside the request head" }),
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
    /// (read back, fence, idempotency key) — never silently re-sent.
    MustNotReplay,
    /// A read may be repeated.
    ReadMayRepeat,
}

pub fn replay_verdict<R>(outcome: &RpcOutcome<R>, mutating: bool) -> ReplayVerdict {
    match outcome {
        RpcOutcome::Reply(_) => ReplayVerdict::Answered,
        RpcOutcome::NotSent(_) | RpcOutcome::Unserved(_) => ReplayVerdict::SafeToRetry,
        RpcOutcome::Indeterminate(_) if mutating => ReplayVerdict::MustNotReplay,
        RpcOutcome::Indeterminate(_) => ReplayVerdict::ReadMayRepeat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogBuilder, CatalogEntry, Shape, TagOwner};
    use crate::echo::{Echo, EchoReply, EchoRequest};
    use crate::framing::{encode_request, encode_varint};
    use crate::outcome::{IndeterminateReason, NotSentReason, PreCommit, ReplyKind, RequestFinished};
    use crate::protocol::{DecodeFailure, NodeProtocol};

    fn catalog() -> SealedCatalog {
        CatalogBuilder::new().serve(CatalogEntry::canonical::<Echo>(TagOwner::Core, Shape::Unary)).seal().unwrap()
    }

    fn echo_frame(payload: &[u8]) -> Vec<u8> {
        let req = EchoRequest::Echo { traceparent: None, payload: payload.to_vec() };
        encode_request(Echo::TAG, &Echo::encode_request(&req).unwrap())
    }

    // ---- PRD §13 dispatch table ----

    #[test]
    fn known_tag_and_known_opcode_dispatch_to_the_handler_after_a_clean_finish() {
        let c = catalog();
        let mut a = RequestAssembly::new(&c);
        let f = echo_frame(b"ping");
        assert_eq!(a.push(&f[..3]), ServerAction::Continue);
        assert_eq!(a.push(&f[3..]), ServerAction::Continue, "a complete frame waits for FIN");
        match a.finish() {
            ServerAction::Dispatch { tag, payload } => {
                assert_eq!(tag, 0x11);
                assert!(matches!(Echo::decode_request(&payload), Ok(EchoRequest::Echo { .. })));
            }
            other => panic!("expected dispatch, got {other:?}"),
        }
    }

    #[test]
    fn known_tag_with_unknown_opcode_is_malformed_unknown_variant() {
        let mut body = Vec::new();
        encode_varint(7, &mut body); // op variant 7 does not exist
        let f = encode_request(Echo::TAG, &body);
        let c = catalog();
        let mut a = RequestAssembly::new(&c);
        a.push(&f);
        let ServerAction::Dispatch { payload, .. } = a.finish() else { panic!() };
        // Decode is the dispatcher's first step; its failure is the typed reply.
        assert_eq!(Echo::decode_request(&payload), Err(DecodeFailure::UnknownVariant));
        let reply = Echo::malformed(DecodeFailure::UnknownVariant.into());
        assert_eq!(Echo::classify_reply(&reply), ReplyKind::Malformed(MalformedKind::UnknownVariant));
    }

    #[test]
    fn unknown_tag_is_421_before_any_body() {
        let c = catalog();
        let mut a = RequestAssembly::new(&c);
        assert_eq!(a.push(&[0x42]), ServerAction::ResetUnserved { tag: 0x42 });
        assert_eq!(a.push(&[1, 2, 3]), ServerAction::Continue, "nothing after the decision");
    }

    #[test]
    fn known_oversize_is_typed_too_large_after_the_tag_read() {
        let c = catalog();
        let mut a = RequestAssembly::new(&c);
        let mut head = vec![Echo::TAG];
        encode_varint(Echo::MAX_REQUEST_FRAME_BYTES as u64 + 1, &mut head);
        assert_eq!(a.push(&head), ServerAction::RefuseMalformed { tag: 0x11, kind: MalformedKind::TooLarge });
    }

    #[test]
    fn a_non_forwardable_family_is_marked_in_the_catalog() {
        assert!(!catalog().lookup(Echo::TAG).unwrap().forwardable, "carriers refuse it by type (e6.s4)");
    }

    // ---- complete-send certainty table ----

    #[test]
    fn before_request_finish_a_499_reset_is_never_dispatched_and_is_not_sent() {
        let c = catalog();
        let mut a = RequestAssembly::new(&c);
        let f = echo_frame(b"partial");
        a.push(&f[..f.len() - 2]);
        assert!(matches!(a.reset(499), ServerAction::Drop { reason } if reason.contains("499")));
        assert_eq!(a.finish(), ServerAction::Continue, "a dropped request never dispatches later");
        // Even a complete frame that was reset instead of finished is never dispatched.
        let mut b = RequestAssembly::new(&c);
        b.push(&f);
        assert!(matches!(b.reset(499), ServerAction::Drop { .. }));
        let (code, outcome): (_, RpcOutcome<EchoReply>) = PreCommit::begin(0x11).cut_before_finish();
        assert_eq!(code, ResetCode::FrameNotSent);
        assert!(matches!(&outcome, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent));
    }

    #[test]
    fn a_fin_before_the_declared_length_or_bytes_past_it_is_a_violation_not_a_dispatch() {
        let c = catalog();
        let f = echo_frame(b"abc");
        let mut a = RequestAssembly::new(&c);
        a.push(&f[..f.len() - 1]);
        assert!(matches!(a.finish(), ServerAction::ResetViolation { .. }));
        let mut b = RequestAssembly::new(&c);
        let mut long = f.clone();
        long.push(0);
        assert!(matches!(b.push(&long), ServerAction::ResetViolation { .. }));
    }

    fn committed() -> crate::outcome::Committed {
        PreCommit::begin(0x11).commit(RequestFinished::after_clean_finish(1, 1, true).unwrap())
    }

    #[test]
    fn complete_request_and_valid_reply_is_reply() {
        let bytes = Echo::encode_reply(&EchoReply::Echoed { payload: vec![1] }).unwrap();
        assert_eq!(committed().reply::<Echo>(&bytes).name(), "Reply");
    }

    #[test]
    fn complete_request_and_unserved_proof_is_unserved() {
        assert_eq!(committed().reset::<EchoReply>(421).name(), "Unserved");
    }

    #[test]
    fn complete_request_and_reply_loss_or_unknown_reply_version_is_indeterminate() {
        let lost: RpcOutcome<EchoReply> = committed().indeterminate(IndeterminateReason::ReplyLost("stream reset".into()));
        assert_eq!(lost.name(), "Indeterminate");
        let mut unknown = Vec::new();
        encode_varint(42, &mut unknown);
        assert!(matches!(
            committed().reply::<Echo>(&unknown),
            RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::UnsupportedReplyVariant
        ));
    }

    #[test]
    fn no_mutating_indeterminate_is_silently_replayed() {
        let ind: RpcOutcome<EchoReply> = committed().indeterminate(IndeterminateReason::ReplyDeadline);
        assert_eq!(replay_verdict(&ind, true), ReplayVerdict::MustNotReplay);
        assert_eq!(replay_verdict(&ind, false), ReplayVerdict::ReadMayRepeat);
        let ns: RpcOutcome<EchoReply> = PreCommit::begin(0x11).not_sent(NotSentReason::Deadline);
        assert_eq!(replay_verdict(&ns, true), ReplayVerdict::SafeToRetry);
        assert_eq!(replay_verdict(&committed().reset::<EchoReply>(421), true), ReplayVerdict::SafeToRetry);
        let bytes = Echo::encode_reply(&Echo::busy("x".into())).unwrap();
        assert_eq!(replay_verdict(&committed().reply::<Echo>(&bytes), true), ReplayVerdict::Answered);
    }
}
