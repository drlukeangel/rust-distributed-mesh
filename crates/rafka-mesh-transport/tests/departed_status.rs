//! i143.e12.s41 follow-up (rafka-v2 #3068), and i143.e12.s53 (R-RX1): a mesh status is taken for what
//! it carries, whoever its publisher is.
//!
//! The design (docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md R-RX1: no receiver
//! refuses an input because of who sent it; fabric-authority.md §7: "A successor mesh primary
//! reads/hydrates the Mesh lifecycle state that already exists and adopts that exact state locally";
//! R-ML2: the admin a mesh investigation creates is a node-admin joining a live mesh, which as successor
//! primary adopts the mesh's held state). The held status orders a status by its `changed_at_rafka_ms`.
//!
//! The defect this cell pins (traces bec3d0cbaa59317b18f954c1d27fdb73, d2427d34ca338d823e76781c4c052624;
//! fresh pending 647061808568a00bf7a6d8f8ef75a55c): the new mesh2.admin.1 held its predecessor's
//! node.delete departure before its entry answer, was not in its own book before its first heartbeat,
//! refused its mesh's status by name, and published a fresh `pending`.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{learn_addresses, Frame, Membership, MeshStatus};
use serde_json::Value;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn digest(name: &str, node_id: &NodeId, incarnation: &IncarnationId, port: u16) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: incarnation.clone(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(),
    }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

/// Wait, bounded, for `what` to hold at the receiver: gossip delivers on its own time.
async fn until(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !holds() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// CONTRACT (#3068; R-RX1; fabric-authority.md §7; R-ML2): through real gossip on mesh2's channel.
/// mesh2.admin.2 holds mesh2's status as the old primary X (mesh2.admin.1) published it, then executes
/// X's node.delete and publishes `NodeDeleted`; the node-admin joining the live mesh at X's path.name
/// (a new birth Y) and the member mesh2.rpc.1 each hold X departed. Y's entry answer (mesh2.admin.2's
/// held statuses, the original publisher kept) gives Y its mesh's status before Y's first heartbeat: Y
/// takes it and holds X's exact fact to adopt. The same answer at mesh2.rpc.1, where no live birth holds
/// the name, is taken too: a status is never refused for its publisher. Once Y's heartbeat reaches the
/// member, the same answer again is a repeat of the change held and is not taken again. Must NOT
/// happen: a status refused for its publisher's name (`via-departed-birth` with `frame=mesh-status`).
#[test]
fn a_node_admin_joining_at_a_departed_birth_name_takes_its_mesh_status_from_the_entry_answer_before_its_first_heartbeat() {
    let dir = crate::evidence_dir();
    let capture = rafka_mesh_telemetry::EvidenceCapture::start(&dir, "rafka-mesh-transport-s41-departed-status").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (p_node, m_node, y_node) = (node().await, node().await, node().await);
        let p_addr = addr_of(&p_node.ep);
        learn_addresses(&m_node.ep, &[p_addr.clone()]).unwrap();
        learn_addresses(&y_node.ep, &[p_addr.clone()]).unwrap();
        let mesh_id = MeshId::mint();
        let clock = rafka_mesh_transport::clock::os_clock;
        let p = Membership::join(&p_node.gossip, &p_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.admin.2", clock(), vec![]).await.unwrap();
        let m = Membership::join(&m_node.gossip, &m_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.rpc.1", clock(), vec![p_addr.clone()]).await.unwrap();
        let y = Membership::join(&y_node.gossip, &y_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.admin.1", clock(), vec![p_addr]).await.unwrap();
        m.wait_for_neighbour().await;
        y.wait_for_neighbour().await;

        // mesh2's status as X published it, held by mesh2.admin.2.
        let fact = Frame::MeshStatus { mesh: "mesh2".into(), status: "ready-for-traffic".into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 };
        p.learn_statuses(&[fact]);
        let held = Some(MeshStatus { status: "ready-for-traffic".into(), publisher: "mesh2.admin.1".into(), changed_at_rafka_ms: 5 });
        assert_eq!(p.mesh_status("mesh2"), held, "mesh2.admin.2 holds X's status");

        // X's node.delete: its executor publishes NodeDeleted on mesh2's channel.
        let (x, x_inc) = (NodeId::mint(), IncarnationId::mint());
        let departure = LifecycleOp { build_id: "dnd82kmawp3h".into(), attempt: 1, operation: "retire-node:mesh2.admin.1".into(), node_id: x.clone(), incarnation: x_inc.clone(), name: "mesh2.admin.1".parse().unwrap(), event_at_rafka_ms: 7 };
        p.publish_lifecycle(&Frame::NodeDeleted { op: departure.clone(), forwarded_by: None }).await.unwrap();
        until("Y and the member to hold X departed", || y.book.is_departed(x.as_str()) && m.book.is_departed(x.as_str())).await;
        assert!(y.book.all().iter().all(|d| d.node.name.to_string() != "mesh2.admin.1"), "Y has not published: no live birth holds the name in Y's book");

        // The entry answer: mesh2.admin.2's held statuses, the original publisher kept.
        let answer = p.status_frames("mesh2.admin.2");
        assert!(matches!(answer.as_slice(), [Frame::MeshStatus { publisher, forwarded_by: Some(by), .. }] if publisher == "mesh2.admin.1" && by == "mesh2.admin.2"), "{answer:?}");
        y.learn_statuses(&answer);
        assert_eq!(y.mesh_status("mesh2"), held, "Y takes its mesh's status before its first heartbeat: X's departure never refuses the name Y holds");
        m.learn_statuses(&answer);
        assert_eq!(m.mesh_status("mesh2"), held, "the member takes it too, though no live birth holds the name there");

        // Y's first heartbeat reaches the member: the name resolves to Y's live birth.
        let (y_id, y_inc) = (NodeId::mint(), IncarnationId::mint());
        y.publish(&digest("mesh2.admin.1", &y_id, &y_inc, 41_021)).await.unwrap();
        until("the member to hold Y's birth", || m.book.get(y_id.as_str()).is_some()).await;
        m.learn_statuses(&answer);
        assert_eq!(m.mesh_status("mesh2"), held, "the repeat leaves the held status as it is");

        for n in [&p_node, &m_node, &y_node] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let deleted: Vec<&Value> = named(&spans, "rdm.mesh.membership.remove.via-node-deleted").into_iter().filter(|s| attr(s, "node_id") == x.as_str()).collect();
        let deleted_at_y = deleted.iter().find(|s| attr(s, "holder") == "mesh2.admin.1").unwrap_or_else(|| panic!("Y applied X's departure: {deleted:#?}"));
        let taken: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-mesh-status").into_iter().filter(|s| attr(s, "mesh") == "mesh2" && attr(s, "via") == "entry-answer").collect();
        let taken_at_y = taken.iter().find(|s| attr(s, "node") == "mesh2.admin.1").unwrap_or_else(|| panic!("Y opened its status span: {taken:#?}"));
        assert_eq!((attr(taken_at_y, "publisher"), attr(taken_at_y, "forwarded_by"), attr(taken_at_y, "changed_at_rafka_ms"), attr(taken_at_y, "taken")), ("mesh2.admin.1".to_string(), "mesh2.admin.2".to_string(), "5".to_string(), "true".to_string()));
        assert!(deleted_at_y["start_unix_nano"].as_u64() < taken_at_y["start_unix_nano"].as_u64(), "the departure was held at Y before the status");
        let refused: Vec<&Value> = named(&spans, "rdm.mesh.membership.reject.via-departed-birth").into_iter().filter(|s| attr(s, "frame") == "mesh-status").collect();
        assert!(refused.is_empty(), "no status is refused for its publisher's name: {refused:#?}");
        let heard_at_m: Vec<(String, String)> = taken.iter().filter(|s| attr(s, "node") == "mesh2.rpc.1").map(|s| (attr(s, "publisher"), attr(s, "taken"))).collect();
        assert_eq!(heard_at_m, vec![("mesh2.admin.1".to_string(), "true".to_string()), ("mesh2.admin.1".to_string(), "false".to_string())], "the member takes it at the first answer, and the repeat is not taken again");
        let taken_at_m = taken.iter().find(|s| attr(s, "node") == "mesh2.rpc.1").expect("the member's first status span");
        let mut lines = String::new();
        for (what, s) in [("deleted-at-y", *deleted_at_y), ("taken-at-y", *taken_at_y), ("taken-at-member", *taken_at_m)] {
            lines.push_str(&format!("{what} {} {} holder={} node={} via={} publisher={} taken={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "holder"), attr(s, "node"), attr(s, "via"), attr(s, "publisher"), attr(s, "taken")));
        }
        std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
    });
}
