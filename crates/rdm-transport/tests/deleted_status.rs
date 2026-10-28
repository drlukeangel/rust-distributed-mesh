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
//! node.delete departure before its entry answer, was not in its own view before its first heartbeat,
//! refused its mesh's status by name, and published a fresh `pending`.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{learn_addresses, Frame, TopologyView, MeshStatus};
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
    MeshDigest { held_hash: Default::default(),
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
        is_primary: None, is_fabric_primary: None
    }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
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
/// (a new birth Y) and the member mesh2.rpc.1 each hold X deleted. Y's entry answer (mesh2.admin.2's
/// held statuses, the original publisher kept) gives Y its mesh's status before Y's first heartbeat: Y
/// takes it and holds X's exact fact to adopt. The same answer at mesh2.rpc.1, where no live birth holds
/// the name, is taken too: a status is never refused for its publisher. Once Y's heartbeat reaches the
/// member, the same answer again is a repeat of the change held and is not taken again. Must NOT
/// happen: a status refused for its publisher's name (`via-deleted-birth` with `frame=mesh-status`).
#[test]
fn a_node_admin_joining_at_a_deleted_birth_name_takes_its_mesh_status_from_the_entry_answer_before_its_first_heartbeat() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-s41-deleted-status").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (p_node, m_node, y_node) = (node().await, node().await, node().await);
        let p_addr = addr_of(&p_node.ep);
        learn_addresses(&m_node.ep, &[p_addr.clone()]).unwrap();
        learn_addresses(&y_node.ep, &[p_addr.clone()]).unwrap();
        let mesh_id = MeshId::mint();
        let clock = rdm_transport::clock::os_clock;
        let p = TopologyView::join(&p_node.gossip, &p_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.admin.2", clock(), vec![], &[]).await.unwrap();
        let m = TopologyView::join(&m_node.gossip, &m_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.rpc.1", clock(), vec![p_addr.clone()], &[]).await.unwrap();
        let y = TopologyView::join(&y_node.gossip, &y_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.admin.1", clock(), vec![p_addr], &[]).await.unwrap();
        p.set_author(&rdm_core::PublisherId { node: "mesh2.admin.2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() });
        let (x, x_inc) = (NodeId::mint(), IncarnationId::mint());
        m.wait_for_neighbour().await;
        y.wait_for_neighbour().await;

        // mesh2's status as X published it, held by mesh2.admin.2.
        let fact = Frame::MeshStatus { mesh: "mesh2".into(), status: "ready".into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 }.authored(&rdm_transport::envelope::Birth { fabric: fabric(), node: x.clone(), incarnation: x_inc.clone() }, 1, 5);
        p.learn_statuses(&[fact]);
        let held = Some(MeshStatus { status: "ready".into(), publisher: "mesh2.admin.1".into(), changed_at_rafka_ms: 5 });
        assert_eq!(p.mesh_status("mesh2"), held, "mesh2.admin.2 holds X's status");

        // X's node.delete: its executor publishes NodeDeleted on mesh2's channel.
        let departure = LifecycleOp { build_id: "dnd82kmawp3h".into(), attempt: 1, operation: "node.delete:mesh2.admin.1".into(), node_id: x.clone(), incarnation: x_inc.clone(), name: "mesh2.admin.1".parse().unwrap(), event_at_rafka_ms: 7, traceparent: None, tracestate: None };
        p.publish_lifecycle(&Frame::NodeDeleted { op: departure.clone(), forwarded_by: None }).await.unwrap();
        until("Y and the member to hold X deleted", || y.digest_view.is_deleted(x.as_str()) && m.digest_view.is_deleted(x.as_str())).await;
        assert!(y.digest_view.all().iter().all(|d| d.node.name.to_string() != "mesh2.admin.1"), "Y has not published: no live birth holds the name in Y's view");

        // The entry answer: mesh2.admin.2's held statuses, the original publisher kept.
        let answer = p.status_frames("mesh2.admin.2");
        assert!(matches!(answer.as_slice(), [frame] if matches!(frame.data(), Frame::MeshStatus { publisher, forwarded_by: Some(by), .. } if publisher == "mesh2.admin.1" && by == "mesh2.admin.2")), "{answer:?}");
        y.learn_statuses(&answer);
        assert_eq!(y.mesh_status("mesh2"), held, "Y takes its mesh's status before its first heartbeat: X's departure never refuses the name Y holds");
        m.learn_statuses(&answer);
        assert_eq!(m.mesh_status("mesh2"), held, "the member takes it too, though no live birth holds the name there");

        // Y's first heartbeat reaches the member: the name resolves to Y's live birth.
        let (y_id, y_inc) = (NodeId::mint(), IncarnationId::mint());
        y.publish(&digest("mesh2.admin.1", &y_id, &y_inc, 41_021)).await.unwrap();
        until("the member to hold Y's birth", || m.digest_view.get(y_id.as_str()).is_some()).await;
        m.learn_statuses(&answer);
        assert_eq!(m.mesh_status("mesh2"), held, "the repeat leaves the held status as it is");

        for n in [&p_node, &m_node, &y_node] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let deleted: Vec<&Value> = named(&spans, "rdm.mesh.topology.remove.via-node-deleted").into_iter().filter(|s| attr(s, "node_id") == x.as_str()).collect();
        let deleted_at_y = deleted.iter().find(|s| attr(s, "observer") == "mesh2.admin.1").unwrap_or_else(|| panic!("Y applied X's departure: {deleted:#?}"));
        let taken: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-mesh-status").into_iter().filter(|s| attr(s, "mesh") == "mesh2" && attr(s, "via") == "entry-answer").collect();
        let taken_at_y = taken.iter().find(|s| attr(s, "node") == "mesh2.admin.1").unwrap_or_else(|| panic!("Y opened its status span: {taken:#?}"));
        assert_eq!((attr(taken_at_y, "publisher"), attr(taken_at_y, "forwarded_by"), attr(taken_at_y, "changed_at_rafka_ms"), attr(taken_at_y, "taken")), ("mesh2.admin.1".to_string(), "mesh2.admin.2".to_string(), "5".to_string(), "true".to_string()));
        assert!(deleted_at_y["start_unix_nano"].as_u64() < taken_at_y["start_unix_nano"].as_u64(), "the departure was held at Y before the status");
        let refused: Vec<&Value> = named(&spans, "rdm.mesh.topology.reject.via-deleted-birth").into_iter().filter(|s| attr(s, "frame") == "mesh-status").collect();
        assert!(refused.is_empty(), "no status is refused for its publisher's name: {refused:#?}");
        let heard_at_m: Vec<(String, String)> = taken.iter().filter(|s| attr(s, "node") == "mesh2.rpc.1").map(|s| (attr(s, "publisher"), attr(s, "taken"))).collect();
        assert_eq!(heard_at_m, vec![("mesh2.admin.1".to_string(), "true".to_string()), ("mesh2.admin.1".to_string(), "false".to_string())], "the member takes it at the first answer, and the repeat is not taken again");
        let taken_at_m = taken.iter().find(|s| attr(s, "node") == "mesh2.rpc.1").expect("the member's first status span");
        let mut lines = String::new();
        for (what, s) in [("deleted-at-y", *deleted_at_y), ("taken-at-y", *taken_at_y), ("taken-at-member", *taken_at_m)] {
            lines.push_str(&format!("{what} {} {} observer={} node={} via={} publisher={} taken={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "observer"), attr(s, "node"), attr(s, "via"), attr(s, "publisher"), attr(s, "taken")));
        }
        std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
    });
}

/// The trace id of a W3C `traceparent`.
fn trace_id_of(traceparent: Option<&str>) -> Option<String> {
    traceparent.and_then(|tp| tp.split('-').nth(1)).map(str::to_string)
}

/// CONTRACT (telemetry.md §3, the deletion operation; Q231): through real gossip on mesh2's channel,
/// the executor mesh2.admin.2 publishes X's `NodeDeleted` inside the delete's span, its caller's
/// context carried in the envelope (gossip.md §3.4). The member mesh2.rpc.1's `DigestView::deleted()`
/// hands the application the operation with that context, and so does the executor's own view: an
/// application span made from the `LifecycleOp` is the delete's child and shares its trace, its
/// `tracestate` as it was. Before Q231 the operation carried none and Rafka's repair span was a root
/// of its own (e855b1df1a47d686aa2e13c23d5ff0d5; the delete's trace e30533ffcb6aab5cd4090817b7383144).
#[test]
fn an_application_span_made_from_a_deleted_operation_shares_the_deletes_trace() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-deleted-operation-trace").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (p_node, m_node) = (node().await, node().await);
        let p_addr = addr_of(&p_node.ep);
        learn_addresses(&m_node.ep, &[p_addr.clone()]).unwrap();
        let mesh_id = MeshId::mint();
        let clock = rdm_transport::clock::os_clock;
        let p = TopologyView::join(&p_node.gossip, &p_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.admin.2", clock(), vec![], &[]).await.unwrap();
        let m = TopologyView::join(&m_node.gossip, &m_node.ep, &fabric(), "mesh2", &mesh_id, "mesh2.rpc.1", clock(), vec![p_addr], &[]).await.unwrap();
        p.set_author(&rdm_core::PublisherId { node: "mesh2.admin.2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() });
        m.wait_for_neighbour().await;

        // The delete's span: the Build step's, its caller's tracestate carried in.
        let (x, x_inc) = (NodeId::mint(), IncarnationId::mint());
        let request = tracing::info_span!("app.node.delete.request");
        let delete = tracing::info_span!("app.node.delete", node = "mesh2.admin.1");
        rdm_transport::set_remote_parent(&delete, &rdm_transport::traceparent_of(&request).expect("the capture's layer gives the request a context"), Some("rafka=delete-1"));
        let deleted_in = rdm_transport::traceparent_of(&delete);
        let departure = LifecycleOp { build_id: "dnd82kmawp3h".into(), attempt: 1, operation: "node.delete:mesh2.admin.1".into(), node_id: x.clone(), incarnation: x_inc.clone(), name: "mesh2.admin.1".parse().unwrap(), event_at_rafka_ms: 7, traceparent: None, tracestate: None };
        tracing::Instrument::instrument(p.publish_lifecycle(&Frame::NodeDeleted { op: departure.clone(), forwarded_by: None }), delete.clone()).await.unwrap();
        until("the member to hold X deleted", || m.digest_view.is_deleted(x.as_str())).await;

        let mut lines = String::new();
        for (at, view) in [("member", &m), ("executor", &p)] {
            let op = view.digest_view.deleted().into_iter().find(|o| o.node_id == x).unwrap_or_else(|| panic!("the {at} hands out X's deletion"));
            assert_eq!(op, departure, "the {at}'s operation is the one deleted");
            let repair = tracing::info_span!("app.node.repair", at);
            if let Some(tp) = op.traceparent.as_deref() {
                rdm_transport::set_remote_parent(&repair, tp, op.tracestate.as_deref());
            }
            repair.in_scope(|| tracing::info!("the application reacts to the deletion"));
            let repaired_in = rdm_transport::traceparent_of(&repair);
            assert_eq!(trace_id_of(repaired_in.as_deref()), trace_id_of(deleted_in.as_deref()), "the application's span made from the {at}'s LifecycleOp shares the delete's trace: the operation carried {:?}", op.traceparent);
            assert_eq!(op.tracestate.as_deref(), Some("rafka=delete-1"), "the delete's tracestate reaches the {at}'s operation as it was");
            lines.push_str(&format!("{at} {}\n", trace_id_of(deleted_in.as_deref()).unwrap_or_default()));
        }

        for n in [&p_node, &m_node] {
            n.ep.close().await;
        }
        std::fs::write(dir.join("deleted-operation-trace-ids.txt"), lines).unwrap();
    });
}
