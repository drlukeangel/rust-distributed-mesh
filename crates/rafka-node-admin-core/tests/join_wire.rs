//! The join's answer travels as one postcard frame: a join answer survives
//! `answer_to_wire` / `answer_from_wire` unchanged. It carries the control state and the statuses;
//! the topology is read with `GetTopology` (op `0x1E`), never in the join.

use rafka_mesh_entity::FabricId;
use rafka_node_admin_core::fabric_storage::{FabricRecord, FabricShutdown};
use rafka_node_admin_core::model::ProviderKind;
use rafka_node_admin_core::wire::{answer_from_wire, answer_to_wire, BuildFloor, JoinAnswer, JoinControl};

fn answer() -> JoinAnswer {
    JoinAnswer {
        served_by: "mesh1.admin.1".into(),
        control: JoinControl {
            provider: ProviderKind::Process,
            fabric: Some(FabricRecord { fabric_id: FabricId::mint(), name: "fabric1".into(), build_id: None }),
            shutdown: Some(FabricShutdown { initiated_by: "mesh1.admin.1".into(), initiated_by_node_id: "n".into(), initiated_at_ms: 4 }),
            build: Some(BuildFloor { build_id: rafka_node_admin_core::build::BuildId::mint(), attempt: 2 }),
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
    assert_eq!(back.control.shutdown, a.control.shutdown);
    assert_eq!(back.control.build.map(|b| (b.build_id, b.attempt)), a.control.build.map(|b| (b.build_id, b.attempt)));
}

// @feature: node-lifecycle
#[test]
fn a_truncated_answer_is_refused_by_name_not_decoded_partially() {
    let bytes = answer_to_wire(&answer()).unwrap();
    let e = answer_from_wire(&bytes[..bytes.len() - 1]).unwrap_err();
    assert!(!e.is_empty());
}
