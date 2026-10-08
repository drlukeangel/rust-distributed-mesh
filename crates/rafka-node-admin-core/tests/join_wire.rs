//! The join's answer travels as one postcard frame: an entry answer survives
//! `answer_to_wire` / `answer_from_wire` unchanged, and a part with no wire shape is refused by
//! name rather than sent as something else.

use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId};
use rafka_mesh_transport::entry::EntryAnswer;
use rafka_node_admin_core::model::{Fabric, Mesh, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use rafka_node_admin_core::wire::{answer_from_wire, answer_to_wire};

fn digest() -> MeshDigest {
    MeshDigest {
        fabric_id: FabricId::mint(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:34567".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: 3,
        emitted_at_rafka_ms: 9,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
    }
}

fn answer() -> EntryAnswer {
    let topology = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: None, name: "mesh1".into(), status: ScopeStatus::Pending }],
        nodes: vec![],
    };
    EntryAnswer {
        served_by: "mesh1.admin.1".into(),
        topology: serde_json::to_value(&topology).unwrap(),
        members: vec![digest()],
        control: serde_json::json!({ "fabric": null, "shutdown": null, "build": null }),
        statuses: vec![],
        sources: vec![],
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
    assert_eq!(back.topology, a.topology);
    assert_eq!(back.members, a.members);
    assert_eq!(back.control, a.control);
}

// @feature: node-lifecycle
#[test]
fn an_answer_whose_topology_is_not_a_topology_is_refused_by_name() {
    let mut a = answer();
    a.topology = serde_json::json!({ "not": "a topology" });
    let e = answer_to_wire(&a).unwrap_err();
    assert!(e.contains("topology"), "{e}");
}
