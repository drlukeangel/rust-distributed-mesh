use rafka_mesh_entity::lifecycle::*;
use rafka_mesh_entity::digest::*;
use rafka_mesh_transport::membership::*;
use rafka_node_rpc_contract::status::*;
use rafka_node_admin_core::accepted::*;
use rafka_node_admin_core::build_state::*;
use rafka_mesh_entity::ids::*;
use rafka_mesh_entity::path::PathName;
use rafka_node_admin_core::build::BuildId;
use serde_json::to_string_pretty;
use std::str::FromStr;

fn main() {
    let build_id = BuildId("bld_1a2b3c4d".into());
    let path_1 = PathName::from_str("mesh1.rpc.1").unwrap();
    let inc = IncarnationId("0000000000000008".into());
    let node_id = NodeId::mint(); // Crockford
    let mesh_id = MeshId::mint();
    let fabric_id = FabricId::mint();
    let endpoint_id = EndpointId("k".into());
    
    // 1. AttemptOpened with Restart
    let opened_restart = AttemptOpened {
        build_id: build_id.clone(),
        attempt: 1,
        reason: AttemptReason::Requested,
        action: Some(AttemptAction::Restart { path: path_1.clone(), from_incarnation: inc.clone() }),
        opened_by: "mesh1.admin.1".into(),
        opened_at_ms: 1718290000000,
    };
    println!("=== AttemptOpened (Restart) ===");
    println!("{}", to_string_pretty(&opened_restart).unwrap());

    // 2. AttemptOpened with Replace
    let opened_replace = AttemptOpened {
        build_id: build_id.clone(),
        attempt: 1,
        reason: AttemptReason::ProvenDrift,
        action: Some(AttemptAction::Replace { path: path_1.clone(), from_incarnation: inc.clone() }),
        opened_by: "mesh1.admin.1".into(),
        opened_at_ms: 1718290000000,
    };
    println!("=== AttemptOpened (Replace) ===");
    println!("{}", to_string_pretty(&opened_replace).unwrap());

    // 3. BuildAttemptClaim
    let claim = BuildAttemptClaim {
        build_id: build_id.clone(),
        attempt: 1,
        executor: "mesh1.admin.1".into(),
    };
    println!("=== BuildAttemptClaim ===");
    println!("{}", to_string_pretty(&claim).unwrap());

    // 4. LifecycleOp
    let op = LifecycleOp {
        build_id: build_id.0.clone(),
        attempt: 1,
        operation: "restart-node:mesh1.rpc.1".into(),
        node_id: node_id.clone(),
        incarnation: inc.clone(),
        name: path_1.clone(),
        event_at_rafka_ms: 1718290000050,
    };
    println!("=== LifecycleOp ===");
    println!("{}", to_string_pretty(&op).unwrap());

    // 5. Frame::NodeRestarting
    let frame_restarting = Frame::NodeRestarting { op: op.clone(), forwarded_by: None };
    println!("=== Frame::NodeRestarting ===");
    println!("{}", to_string_pretty(&frame_restarting).unwrap());

    // 6. Frame::NodeDeleting
    let mut op_del = op.clone();
    op_del.operation = "retire-node:mesh1.rpc.1".into();
    let frame_deleting = Frame::NodeDeleting { op: op_del.clone(), forwarded_by: None };
    println!("=== Frame::NodeDeleting ===");
    println!("{}", to_string_pretty(&frame_deleting).unwrap());

    // 7. Frame::Digest (MeshDigest)
    let mesh_node = MeshNode {
        node_id: node_id.clone(),
        name: path_1.clone(),
        endpoint_id: endpoint_id.clone(),
        transport_addr: "127.0.0.1:7000".parse().unwrap(),
        incarnation: inc.clone(),
        supersedes: None,
        runtime: None,
    };
    let digest = MeshDigest {
        fabric_id: fabric_id.clone(),
        node: mesh_node.clone(),
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        emitted_at_rafka_ms: 1718290000000,
        digest_seq: 1,
        data_dir: None,
        mesh_id: Some(mesh_id.clone()),
        in_flight: None,
        extra: std::collections::BTreeMap::new(),
        load: None,
        gossip: None,
    };
    let frame_digest = Frame::Digest { digest: digest.clone() };
    println!("=== Frame::Digest ===");
    println!("{}", to_string_pretty(&frame_digest).unwrap());

    // 8. StatusRequest::ApplyNodeState
    let req_apply_draining = StatusRequest::ApplyNodeState {
        node_id: node_id.clone(),
        incarnation: inc.clone(),
        state: NodeState::Draining,
    };
    println!("=== StatusRequest::ApplyNodeState ===");
    println!("{}", to_string_pretty(&req_apply_draining).unwrap());

    // 9. StatusReply::NodeDrainingApplied
    let reply_draining = StatusReply::NodeDrainingApplied { in_flight: 3 };
    println!("=== StatusReply::NodeDrainingApplied ===");
    println!("{}", to_string_pretty(&reply_draining).unwrap());

    // 10. Frame::Members
    let frame_members = Frame::Members {
        mesh: "mesh1".into(),
        publisher: rafka_mesh_transport::snapshot::PublisherId { node: "mesh1.admin.1".into(), incarnation: inc.clone() },
        forwarded_by: None,
        topology_version: 1,
        published_at_rafka_ms: 1718290000000,
        snapshot_id: 1,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![digest.clone()],
        in_flight: vec![op.clone()],
        departed: vec![],
    };
    println!("=== Frame::Members ===");
    println!("{}", to_string_pretty(&frame_members).unwrap());
    
    // Write random ids to file to help python script replace them uniformly
    println!("=== IDs ===");
    println!("NODE_ID={}", node_id);
    println!("MESH_ID={}", mesh_id);
    println!("FABRIC_ID={}", fabric_id);
}
