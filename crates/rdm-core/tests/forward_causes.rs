//! Q40: the cause crosses Forward as typed data; explanation text never decides a code.
use rdm_core::forward::{Forward, ForwardReply, IndeterminateCause, NotSentCause};
use rdm_core::outcome::{carried, Code, PreCommit, RequestFinished, ResolveFailure, RpcOutcome};
use rdm_core::ping::Ping;
use rdm_core::protocol::{DecodeFailure, NodeProtocol};

fn committed() -> rdm_core::outcome::Committed {
    PreCommit::begin(Forward::OP).commit(RequestFinished::after_complete_write(1, 1).unwrap())
}
fn bytes(hex: &str) -> Vec<u8> {
    hex.split_whitespace().map(|v| u8::from_str_radix(v, 16).unwrap()).collect()
}
fn inner(cause: IndeterminateCause) -> RpcOutcome<rdm_core::ping::PingReply> {
    let original = cause.into_reason("same".into());
    let typed = IndeterminateCause::from_reason(&original);
    assert_eq!(typed, cause);
    let reply = ForwardReply::InnerIndeterminate { reason: "same".into(), cause: typed };
    carried::<Ping>(committed().reply::<Forward>(&Forward::encode_reply(&reply).unwrap()))
}
fn check_inner(cause: IndeterminateCause, code: Code) {
    let out = inner(cause);
    match out {
        RpcOutcome::Indeterminate(i) => {
            assert_eq!(i.code(), code);
            assert_eq!(IndeterminateCause::from_reason(i.reason()), cause);
        }
        other => panic!("the inner call committed: {other:?}"),
    }
}
#[test]
fn receiver_gone_crosses_a_carrier_as_unavailable() {
    check_inner(IndeterminateCause::ReceiverGone, Code::Unavailable);
}
#[test]
fn reply_deadline_crosses_a_carrier_as_deadline_exceeded() {
    check_inner(IndeterminateCause::ReplyDeadline, Code::DeadlineExceeded);
}
#[test]
fn reply_lost_crosses_a_carrier_as_unknown() {
    check_inner(IndeterminateCause::ReplyLost, Code::Unknown);
}
#[test]
fn every_indeterminate_cause_has_a_hand_written_golden_and_rebuilds_its_typed_origin() {
    use IndeterminateCause::*;
    for (cause, code, suffix) in [
        (ReceiverGone, Code::Unavailable, "00"),
        (ReplyDeadline, Code::DeadlineExceeded, "01"),
        (ReplyLost, Code::Unknown, "02"),
        (Reset(423), Code::Internal, "03 a7 03"),
        (Reset(424), Code::Internal, "03 a8 03"),
        (Reset(501), Code::Unknown, "03 f5 03"),
        (UnsupportedReplyVariant, Code::InvalidArgument, "04"),
        (CorruptReply, Code::DataLoss, "05"),
        (ProtocolViolation, Code::Internal, "06"),
    ] {
        let reply = ForwardReply::InnerIndeterminate { reason: "same".into(), cause };
        let mut expected = bytes("04 04 73 61 6d 65");
        expected.extend(bytes(suffix));
        assert_eq!(Forward::encode_reply(&reply).unwrap(), expected);
        assert_eq!(Forward::decode_reply(&expected).unwrap(), reply);
        assert_eq!(reply.code(), code);
        check_inner(cause, code);
    }
    assert_eq!(Forward::decode_reply(&bytes("04 04 73 61 6d 65")), Err(DecodeFailure::Corrupt), "the old frame has no cause; there is no dual decoder");
}
#[test]
fn every_not_sent_cause_has_a_hand_written_golden_and_rebuilds_its_fields() {
    use NotSentCause::*;
    for (cause, code, suffix) in [
        (Resolve(ResolveFailure::Unknown), Code::NotFound, "00 00"),
        (Resolve(ResolveFailure::Gone), Code::Unavailable, "00 01"),
        (Resolve(ResolveFailure::Unavailable), Code::Unavailable, "00 02"),
        (Connection, Code::Unavailable, "01"),
        (StreamBudget, Code::DeadlineExceeded, "02"),
        (Deadline, Code::DeadlineExceeded, "03"),
        (FrameNotSent, Code::Unavailable, "04"),
        (Carried, Code::Unavailable, "05"),
        (ConnectionLost, Code::Unavailable, "06"),
        (CarrierNoBudget { remaining_ms: 20, reserve_ms: 100 }, Code::Unavailable, "07 14 64"),
        (NotForwardable { op: 0x1B }, Code::FailedPrecondition, "08 1b"),
        (NoActiveRoute, Code::Unavailable, "09"),
        (AdmissionBlocked, Code::Unavailable, "0a"),
    ] {
        let reply = ForwardReply::InnerNotSent { reason: "same".into(), cause };
        let mut expected = bytes("01 04 73 61 6d 65");
        expected.extend(bytes(suffix));
        assert_eq!(Forward::encode_reply(&reply).unwrap(), expected);
        assert_eq!(Forward::decode_reply(&expected).unwrap(), reply);
        assert_eq!(reply.code(), code);
        match carried::<Ping>(committed().reply::<Forward>(&expected)) {
            RpcOutcome::NotSent(n) => {
                assert_eq!(n.code(), code);
                assert_eq!(NotSentCause::from_reason(n.reason()), cause);
            }
            other => panic!("the inner call never committed: {other:?}"),
        }
    }
    assert_eq!(Forward::decode_reply(&bytes("01 04 73 61 6d 65")), Err(DecodeFailure::Corrupt));
}
#[test]
fn the_carriers_own_refusals_keep_their_codes() {
    assert_eq!(ForwardReply::ConnectionLost { reason: "same".into() }.code(), Code::Unavailable);
    assert_eq!(ForwardReply::NotForwardable { op: 1 }.code(), Code::FailedPrecondition);
    assert_eq!(ForwardReply::InnerNotSent { reason: "same".into(), cause: NotSentCause::Connection }.code(), Code::Unavailable);
}
#[test]
fn the_code_namespace_is_exactly_the_seventeen_standard_codes() {
    let names = ["OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED"];
    for (n, code) in Code::ALL.into_iter().enumerate() {
        assert_eq!(code.number(), n as u8);
        assert_eq!(code.name(), names[n]);
        assert_eq!(Code::from_number(n as u8), Some(code));
    }
    assert_eq!(Code::ALL.len(), names.len());
    for n in 17..=255 { assert_eq!(Code::from_number(n), None); }
}
