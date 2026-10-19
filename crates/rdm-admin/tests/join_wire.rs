//! The join's answer travels as one postcard frame: a join answer survives
//! `answer_to_wire` / `answer_from_wire` unchanged. It carries the control state and the statuses;
//! the topology is read with `GetTopology` (op `0x1E`), never in the join.

use rdm_core::FabricId;
use rdm_admin::fabric_storage::FabricIdentity;
use rdm_admin::model::ProviderKind;
use rdm_admin::wire::{answer_from_wire, answer_to_wire, BuildFloor, JoinAnswer, JoinControl};

fn answer() -> JoinAnswer {
    JoinAnswer {
        served_by: "mesh1.admin.1".into(),
        control: JoinControl { cert_policy: None,
            provider: ProviderKind::Process,
            fabric: Some(FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }),
            build: Some(BuildFloor { build_id: rdm_admin::build::BuildId::mint(), attempt: 2 }),
            rafka_time_ms: 1_700_000_000_123,
            member_cert: vec![0xca, 0xfe],
            name: Some("mesh1.rpc.1.old".parse().unwrap()),
            config: Some([("tier".to_string(), "gold".to_string())].into()),
        },
        statuses: vec![],
    }
}

// @feature: node-lifecycle
#[test]
fn a_join_request_and_its_answer_round_trip_through_postcard() {
    let a = answer();
    let bytes = answer_to_wire(&a).expect("the answer has a wire shape");
    assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err(), "the frame is not JSON");
    let back = answer_from_wire(&bytes).expect("and decodes");
    assert_eq!(back.served_by, a.served_by);
    assert_eq!(back.control.provider, a.control.provider);
    assert_eq!(back.control.fabric, a.control.fabric);
    assert_eq!(back.control.rafka_time_ms, 1_700_000_000_123);
    assert_eq!(back.control.member_cert, vec![0xca, 0xfe], "the member cert travels as opaque bytes");
    assert_eq!((back.control.name, back.control.config), (a.control.name, a.control.config), "the row's name and config the joiner installs");
    assert_eq!(back.control.build.map(|b| (b.build_id, b.attempt)), a.control.build.map(|b| (b.build_id, b.attempt)));
}

// @feature: node-lifecycle
#[test]
fn a_truncated_answer_is_refused_by_name_not_decoded_partially() {
    let bytes = answer_to_wire(&answer()).unwrap();
    let e = answer_from_wire(&bytes[..bytes.len() - 1]).unwrap_err();
    assert!(!e.is_empty());
}

// @feature: node-lifecycle
#[test]
fn the_answers_member_cert_name_config_and_cert_policy_are_the_control_frames_last_fields_before_the_statuses() {
    let mut a = answer();
    a.control.rafka_time_ms = 300;
    a.control.member_cert = vec![0xca, 0xfe];
    a.control.name = None;
    a.control.config = Some([("k".to_string(), "v".to_string())].into());
    let bytes = answer_to_wire(&a).unwrap();
    // postcard varint 300 = ac 02, the cert's length 02 and its bytes ca fe, no name = 00, the
    // config Some = 01 with one entry 01 ("k" = 01 6b, "v" = 01 76), no cert policy = 00 (i145 e3),
    // then the empty statuses = 00.
    assert_eq!(&bytes[bytes.len() - 14..], &[0xac, 0x02, 0x02, 0xca, 0xfe, 0x00, 0x01, 0x01, 0x01, 0x6b, 0x01, 0x76, 0x00, 0x00], "{bytes:02x?}");
    // An authority configured with no certs sends an empty cert, and no row hands no config.
    a.control.member_cert = vec![];
    a.control.config = None;
    let bytes = answer_to_wire(&a).unwrap();
    assert_eq!(&bytes[bytes.len() - 7..], &[0xac, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00], "{bytes:02x?}");
    // The fabric's cert policy rides last: member TTL 12 = 0c, issuing 2592000 = 80 9a 9e 01, CA
    // 31536000 = 80 e7 84 0f, renew point 670 = 9e 05.
    a.control.cert_policy = Some(rdm_core::certs::CertPolicy { member_ttl_secs: 12, ..Default::default() });
    let bytes = answer_to_wire(&a).unwrap();
    assert_eq!(&bytes[bytes.len() - 13..], &[0x01, 0x0c, 0x80, 0x9a, 0x9e, 0x01, 0x80, 0xe7, 0x84, 0x0f, 0x9e, 0x05, 0x00], "{bytes:02x?}");
    assert_eq!(answer_from_wire(&bytes).unwrap().control.cert_policy, a.control.cert_policy, "and returns");
}

/// An entry answer carries the retained envelope, including its original author and R10 floor.
#[test]
fn joined_statuses_keep_the_authored_context_and_committed_revision() {
    let mut answer = answer();
    let author = crate::gossip_wire::author();
    let frame = rdm_transport::membership::Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: Some("mesh2.admin.1".into()), changed_at_rafka_ms: 1000 }.authored(&author, 2, 1000);
    answer.statuses.push(frame.clone());
    let restored = answer_from_wire(&answer_to_wire(&answer).unwrap()).unwrap();
    assert_eq!(restored.statuses[0].encode(), frame.encode());
    assert_eq!(restored.statuses[0].context().unwrap().source, author);
    assert_eq!(restored.statuses[0].context().unwrap().sequence.as_deref(), Some("00000000000000000002.00000000000000001000"));
}
