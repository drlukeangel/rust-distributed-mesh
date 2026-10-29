//! Standard code semantics, with the ruled failed-receipt fields retaining their captured code.
use rdm_core::outcome::Code;
use rdm_core::protocol::NodeProtocol;
use rdm_core::status::{Status, StatusReply};

#[test]
fn the_code_namespace_is_exactly_the_seventeen_standard_numbers_and_names() {
    let names = ["OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED"];
    assert_eq!(Code::ALL.len(), 17);
    for (n, code) in Code::ALL.into_iter().enumerate() {
        assert_eq!(rdm_core::protocol::encode(&code).unwrap(), vec![n as u8]);
        assert_eq!(code.number(), n as u8);
        assert_eq!(code.name(), names[n]);
        assert_eq!(Code::from_number(n as u8), Some(code));
    }
    for n in 17..=255 {
        assert_eq!(Code::from_number(n), None);
        assert!(postcard::from_bytes::<Code>(&[n]).is_err());
    }
}

#[test]
fn repeated_application_is_already_exists_and_an_unhandled_request_is_unimplemented() {
    assert_eq!(Status::classify_reply(&StatusReply::AlreadyApplied).code(), Code::AlreadyExists);
    assert_eq!(Status::classify_reply(&StatusReply::RequestNotHandled { request: "probe".into() }).code(), Code::Unimplemented);
}

/// Independent expected codes by frozen reply position; malformed's typed kind refines its row.
pub(crate) fn check<P: NodeProtocol>(reply: &P::Reply) {
    let wire = P::encode_reply(reply).unwrap();
    let expected: &[u8] = match P::OP {
        0x01 => &[0,5,14,14,14,15,7],
        0x1A => &[0,14,12,10,2,9,5,14,14,14,15,7,14,14],
        0x1B => &[0,6,0,0,10,10,10,9,9,9,5,14,14,14,15,7,9,0,0,9,0,0,9,9,9,0,0,9,6,9,9,0,3,0,0,0,0,5,8,10,9,9,9,3,9,0,0,3,14,0,0,12,0,0,3,0,0,3,0,9],
        0x1C => &[0,10,9,9,5,14,14,14,15,7,14],
        0x1D => &[0,9,9,5,14,14,14,15,7,9,9],
        0x1E => &[0,0,0,14,5,5,14,14,15,7,0,0,0,0,0],
        0x1F => &[0,0,14,5,5,14,14,15,7,0,5],
        0x20 => &[0,0,0,0,0,9,0,0,0,9,9,10,5,9,14,10,9,9,5,14,14,14,15,7,0,9,0,0,0,3],
        0x21 => &[0,10,9,9,10,14,15,5,14,14,7,9,6],
        0x22 => &[0,0,0,3,10,14,5,14,14,15,7,0,0,0,0,13,9,0,10,0,0,0,14],
        op => panic!("no expected-code fixture for {op:#x}"),
    };
    assert_eq!(expected.len(), P::REPLY_VARIANTS as usize);
    let malformed = P::encode_reply(&P::malformed(rdm_core::outcome::MalformedKind::Corrupt)).unwrap()[0];
    let n = if wire[0] == malformed { match wire[1] { 0 | 3 => 8, 1 => 3, 2 => 15, other => panic!("unknown malformed kind {other}") } } else { expected[wire[0] as usize] };
    let n = match P::OP {
        0x1B => match rdm_core::status::Status::decode_reply(&wire).unwrap() {
            StatusReply::RejectedNotPrimary { why: rdm_core::status::NotPrimary::SubjectUnknown { .. } } => 5,
            StatusReply::RejectedNotPrimary { why: rdm_core::status::NotPrimary::SenderNotSubject { .. } } => 7,
            StatusReply::Stopped { receipt: rdm_core::status::ConnectionsReceipt::Deadline { .. } }
            | StatusReply::ConnectionsDeleted { receipt: Some(rdm_core::status::ConnectionsReceipt::Deadline { .. }), .. } => 4,
            _ => n,
        },
        0x20 => match rdm_core::build::Build::decode_reply(&wire).unwrap() {
            rdm_core::build::BuildReply::Started { disposition: rdm_core::build::Disposition::AlreadyApplied, .. } => 6,
            rdm_core::build::BuildReply::Failed { code, .. } => code.number(),
            _ => n,
        },
        0x21 => match rdm_core::handover::NodePrimaryUpdate::decode_reply(&wire).unwrap() {
            rdm_core::handover::NodePrimaryUpdateReply::Failed { code, .. } => code.number(),
            _ => n,
        },
        0x22 => match rdm_core::node_command::NodeCommand::decode_reply(&wire).unwrap() {
            rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { receipt: Some(rdm_core::status::ConnectionsReceipt::Deadline { .. }), .. } | rdm_core::node_command::NodeCommandReply::StopConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt::Deadline { .. } } => 4,
            rdm_core::node_command::NodeCommandReply::Failed { code, .. } => code.number(),
            _ => n,
        },
        _ => n,
    };
    let before = P::classify_reply(reply);
    let decoded = P::decode_reply(&wire).unwrap();
    assert_eq!(before, P::classify_reply(&decoded));
    assert_eq!(before.code(), Code::from_number(n).unwrap(), "{} reply position {}", P::NAME, wire[0]);
}

#[test]
fn every_reply_variant_of_ping_classifies_to_one_code() {
    use rdm_core::ping::*;
    use rdm_core::outcome::MalformedKind;

    let fixtures = [
            Ping::peer_unresolved("p".into()),
            Ping::not_ready("n".into()),
            Ping::busy("b".into()),
            Ping::connections_deleting("d".into()),
            Ping::malformed(MalformedKind::TooLarge),
            Ping::malformed(MalformedKind::UnknownVariant),
            Ping::malformed(MalformedKind::Corrupt),
            Ping::unauthorized("u".into()),
            PingReply::Pong { payload: vec![1, 2] },
        ];
    let mut variants = std::collections::BTreeSet::new();
    for r in fixtures { variants.insert(Ping::encode_reply(&r).unwrap()[0]); check::<Ping>(&r); }
    assert_eq!(variants.len(), Ping::REPLY_VARIANTS as usize);
    check_malformed::<Ping>();
}

#[test]
fn every_reply_variant_of_join_classifies_to_one_code() {
    use rdm_core::{NodeId, IncarnationId};
    use rdm_core::join::*;
    use rdm_core::outcome::MalformedKind;

    let fixtures = [
            JoinReply::Joined { answer: vec![1, 2, 3] },
            JoinReply::JoinMismatch { field: "incarnation".into(), deployed: "a".into(), reported: "b".into() },
            JoinReply::NotPrimary { primary: Some("mesh1.admin.1".into()) },
            JoinReply::DeploymentAbandoned { build_id: "3qcfk1kq47m9".into(), attempt: 2, node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() },
            JoinReply::CertRefused { name: "signer-down".into(), detail: "no key".into() },
            Join::peer_unresolved("p".into()),
            Join::not_ready("n".into()),
            Join::busy("b".into()),
            Join::connections_deleting("d".into()),
            Join::malformed(MalformedKind::Corrupt),
            Join::unauthorized("u".into()),
        ];
    let mut variants = std::collections::BTreeSet::new();
    for r in fixtures { variants.insert(Join::encode_reply(&r).unwrap()[0]); check::<Join>(&r); }
    assert_eq!(variants.len(), Join::REPLY_VARIANTS as usize);
    check_malformed::<Join>();
}

#[test]
fn every_reply_variant_of_build_claim_classifies_to_one_code() {
    use rdm_core::context::CallContext;
    use rdm_core::build_claim::*;
    use rdm_core::outcome::MalformedKind;

    let fixtures = [
            BuildClaimReply::Won { context: CallContext { caller_system: Some("rdm".into()), traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()), tracestate: None, baggage: None } },
            BuildClaimReply::Lost { holder: "mesh1.admin.1".into() },
            BuildClaimReply::NotOpen { next: Some(7) },
            BuildClaimReply::NotClaimant { mesh: Some("mesh2".into()), fabric_primary: Some("mesh1.admin.1".into()), mesh_primary: Some("mesh2.admin.1".into()) },
            BuildClaim::peer_unresolved("p".into()),
            BuildClaim::not_ready("n".into()),
            BuildClaim::busy("b".into()),
            BuildClaim::connections_deleting("d".into()),
            BuildClaim::malformed(MalformedKind::Corrupt),
            BuildClaim::unauthorized("u".into()),
            BuildClaimReply::BuildInProgress { current_build_id: "b7swg3jj790z".into(), mesh: "mesh2".into(), held_by: Some("mesh1.admin.1".into()) },
        ];
    let mut variants = std::collections::BTreeSet::new();
    for r in fixtures { variants.insert(BuildClaim::encode_reply(&r).unwrap()[0]); check::<BuildClaim>(&r); }
    assert_eq!(variants.len(), BuildClaim::REPLY_VARIANTS as usize);
    check_malformed::<BuildClaim>();
}

#[test]
fn deadlines_keep_the_commit_cut_and_receiver_gone_is_unavailable() {
    use rdm_core::outcome::{PreCommit, RequestFinished, NotSentReason, IndeterminateReason, RpcOutcome};
    let before: RpcOutcome<()> = PreCommit::begin(1).not_sent(NotSentReason::Deadline);
    assert_eq!(before.code(), Code::DeadlineExceeded);
    assert!(before.proves_not_dispatched());
    let committed = || PreCommit::begin(1).commit(RequestFinished::after_complete_write(1, 1).unwrap());
    for (reason, code) in [(IndeterminateReason::ReplyDeadline, Code::DeadlineExceeded), (IndeterminateReason::ReplyLost("lost".into()), Code::Unknown), (IndeterminateReason::ReceiverGone("exited verdict".into()), Code::Unavailable)] {
        let out: RpcOutcome<()> = committed().indeterminate(reason);
        assert_eq!(out.code(), code);
        assert!(!out.proves_not_dispatched());
    }
    let unserved: RpcOutcome<()> = committed().reset(421);
    assert_eq!(unserved.code(), Code::Unimplemented);
    let fault: RpcOutcome<()> = committed().reset(423);
    assert_eq!(fault.code(), Code::Internal);
}

#[test]
fn tags_have_per_element_codes_and_the_served_call_is_ok() {
    use rdm_core::{NodeId, IncarnationId};
    use rdm_core::outcome::ElementDetail;
    let create = StatusReply::TagsCreated { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), already_carried: ["old".to_string()].into() };
    assert_eq!(Status::classify_reply(&create).code(), Code::Ok);
    assert_eq!(create.tag_outcome("old").unwrap().code, Code::AlreadyExists);
    assert_eq!(create.tag_outcome("old").unwrap().detail, ElementDetail::AlreadyCarried);
    assert_eq!(create.tag_outcome("new").unwrap().code, Code::Ok);
    let delete = StatusReply::TagsDeleted { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), not_carried: ["absent".to_string()].into() };
    assert_eq!(Status::classify_reply(&delete).code(), Code::Ok);
    assert_eq!(delete.tag_outcome("absent").unwrap().code, Code::NotFound);
    assert_eq!(delete.tag_outcome("old").unwrap().code, Code::Ok);
}

pub(crate) fn check_malformed<P: NodeProtocol>() {
    use rdm_core::outcome::MalformedKind;
    for (kind, code) in [(MalformedKind::TooLarge, Code::ResourceExhausted), (MalformedKind::UnknownVariant, Code::InvalidArgument), (MalformedKind::Corrupt, Code::DataLoss), (MalformedKind::TypesOverCap, Code::ResourceExhausted)] {
        let value = P::malformed(kind);
        let decoded = P::decode_reply(&P::encode_reply(&value).unwrap()).unwrap();
        assert_eq!(P::classify_reply(&decoded).code(), code);
    }
}

#[test]
fn the_standard_code_view_keeps_the_protocols_exact_typed_detail() {
    use rdm_core::outcome::{PreCommit, RequestFinished};
    let committed = || PreCommit::begin(Status::OP).commit(RequestFinished::after_complete_write(1, 1).unwrap());
    for (value, code) in [(StatusReply::Applied, Code::Ok), (StatusReply::AlreadyApplied, Code::AlreadyExists), (StatusReply::NameHeld { name: "mesh1.rpc.1".into(), by: rdm_core::NodeId::mint() }, Code::AlreadyExists), (StatusReply::RequestNotHandled { request: "node.get".into() }, Code::Unimplemented)] {
        let bytes = Status::encode_reply(&value).unwrap();
        let out = committed().reply::<Status>(&bytes);
        assert_eq!(out.code(), code);
        assert_eq!(out.reply().unwrap().detail(), &value);
    }
}

#[test]
fn served_named_connections_have_per_element_codes_in_both_protocols() {
    use rdm_core::ConnectionId;
    use rdm_core::node_command::{NodeCommand, NodeCommandReply};
    let closed = ConnectionId::parse("0123456789ab").unwrap();
    let absent = ConnectionId::parse("1123456789ab").unwrap();
    let status = StatusReply::ConnectionsDeleted { closed: vec![closed.clone()], not_open: vec![absent.clone()], held: vec![], receipt: None };
    let command = NodeCommandReply::ConnectionsDeleted { closed: vec![closed], not_open: vec![absent], held: vec![], receipt: None };
    assert_eq!(Status::classify_reply(&status).code(), Code::Ok);
    assert_eq!(NodeCommand::classify_reply(&command).code(), Code::Ok);
    assert_eq!(status.connection_outcomes(), command.connection_outcomes());
    let outcomes = status.connection_outcomes();
    assert_eq!(outcomes[0].code, Code::Ok);
    assert_eq!(outcomes[1].code, Code::NotFound);
}

#[test]
fn resolver_unknown_is_not_found_and_gone_is_unavailable_with_its_typed_detail() {
    use rdm_core::outcome::{NotSentReason, PreCommit, ResolveFailure, RpcOutcome};
    for (reason, code) in [(ResolveFailure::Unknown, Code::NotFound), (ResolveFailure::Gone, Code::Unavailable), (ResolveFailure::Unavailable, Code::Unavailable)] {
        let out: RpcOutcome<()> = PreCommit::begin(1).not_sent(NotSentReason::Resolve(reason));
        assert_eq!(out.code(), code);
        assert!(out.proves_not_dispatched());
        match out {
            RpcOutcome::NotSent(n) => assert_eq!(n.reason(), &NotSentReason::Resolve(reason)),
            other => panic!("resolver refusal before dispatch: {other:?}"),
        }
    }
}

#[test]
fn lost_reply_is_unknown_but_handler_fault_and_protocol_violation_are_internal() {
    use rdm_core::outcome::{IndeterminateReason, PreCommit, RequestFinished, RpcOutcome};
    let committed = || PreCommit::begin(1).commit(RequestFinished::after_complete_write(1, 1).unwrap());
    let lost: RpcOutcome<()> = committed().indeterminate(IndeterminateReason::ReplyLost("streaming EOF after Started before Terminal".into()));
    assert_eq!(lost.code(), Code::Unknown);
    let violation: RpcOutcome<()> = committed().indeterminate(IndeterminateReason::ProtocolViolation("Data before Started".into()));
    assert_eq!(violation.code(), Code::Internal);
    for (reset, code) in [(423, Code::Internal), (424, Code::Internal), (501, Code::Unknown)] {
        let out: RpcOutcome<()> = committed().reset(reset);
        assert_eq!(out.code(), code);
        assert!(matches!(out, RpcOutcome::Indeterminate(_)));
    }
}
