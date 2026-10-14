//! i143.e12.s52 (rafka-v2 #3082), R-HB1 part 1: a heartbeat is liveness only and never ends a stop.
//!
//! The design (docs/architecture/gossip.md §3.1, "Node-stopped lifecycle overlay";
//! lifecycles/node-start.md step 2; lifecycles/states.md "Who sets each state"): after
//! `node.stopped`, a heartbeat (`Digest`) of that birth is not recognised and is refused by name;
//! only the mesh-primary's `node.joined` (node.start) or node.delete ends the stop. Own-mesh members
//! take a member's status from the mesh-primary's versioned member projection, and only from it;
//! liveness, endpoints and the runtime fact come from the Digest.
//!
//! The defect these cells pin (gate b05c303b, trace 0164315174b5f3d478b34af76d519e94): the
//! mesh-primary held mesh1.admin.2's heartbeat 13 when the stop installed; heartbeat 14
//! (`ReadyForTraffic`, the same incarnation, sent before the stop) arrived after it and ended the
//! stop as `rdm.mesh.membership.update.via-node-started`.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{learn_addresses, Backbone, DigestBook, Frame, Membership, StatusEvent};
use rafka_mesh_transport::snapshot::{chunks_of, Chunk, PublisherId};
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

/// One birth's heartbeat as its node publishes it: `seq` is its `digest_seq`.
fn digest(name: &str, node_id: &NodeId, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, status: MemberStatus, seq: u64, port: u16) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: incarnation.clone(),
            supersedes,
            runtime: None,
        },
        status,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: seq,
        emitted_at_rafka_ms: seq,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(),
    }
}

fn stop_op(d: &MeshDigest) -> LifecycleOp {
    LifecycleOp {
        build_id: "build-s52".into(),
        attempt: 1,
        operation: format!("stop-node:{}", d.node.name),
        node_id: d.node.node_id.clone(),
        incarnation: d.node.incarnation.clone(),
        name: d.node.name.clone(),
        event_at_rafka_ms: 7,
    }
}

fn hold_mesh_ready(book: &DigestBook) {
    let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 1 };
    assert!(book.statuses.take(&f, &fabric()));
}

/// The mesh-primary `p` (publishing on `bb`) publishes `NodeStopped` for `stop` from the stop's
/// matching stopped reply, on its own mesh channel.
async fn publish_stopped(_p: &Membership, bb: &Backbone, stop: &LifecycleOp) {
    bb.publish_status_event(StatusEvent::Stopped, stop).await;
}

/// `m` holds `holder` as its mesh's seat record, as its entry read named it.
fn learn_primary_seat(m: &Membership, mesh: &str, node_id: &NodeId, incarnation: &IncarnationId) {
    m.learn_seats(&[(Seat::MeshPrimary, SeatHolder { mesh: mesh.into(), node_id: node_id.clone(), incarnation: incarnation.clone(), epoch: 1 }, false)], "entry");
}

/// CONTRACT (gossip.md "Node-stopped lifecycle overlay"; node-start.md step 2): the mesh-primary's
/// book holds mesh1.admin.2 at heartbeat 13, ready; `NodeStopped` drops the birth and holds the stop
/// overlay. Heartbeat 14 of the same incarnation (ready, sent before the stop, delivered after it)
/// and heartbeat 15 (pending, the shape of its rejoin) are each refused by name, naming the birth
/// and its `digest_seq`; the stop stands: the birth is not held, the overlay is held, it is not
/// routable, and no `via-node-started` is emitted. A digest of a later incarnation of the same
/// NodeId ends the stop under the ordinary rules. Must NOT happen: a heartbeat of the stopped birth
/// taken, whatever its status or sequence.
#[test]
fn a_heartbeat_of_a_stopped_birth_after_node_stopped_is_refused_by_name_and_the_stop_stands() {
    let dir = crate::evidence_dir();
    let capture = rafka_mesh_telemetry::EvidenceCapture::start(&dir, "rafka-mesh-transport-s52").expect("the cell's evidence file can be created");
    capture.run(2, async {
        let book = DigestBook::default();
        hold_mesh_ready(&book);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let at = |status, seq| digest("mesh1.admin.2", &id, &inc, None, status, seq, 41_002);
        assert!(book.record(at(MemberStatus::ReadyForTraffic, 13)), "heartbeat 13 is held");
        assert!(book.routable(id.as_str()));
        let stop = stop_op(&at(MemberStatus::ReadyForTraffic, 13));
        assert!(book.stopped(stop.clone()), "NodeStopped installs");
        assert!(book.get(id.as_str()).is_none(), "the stopped birth is dropped from the book");

        assert!(!book.record(at(MemberStatus::ReadyForTraffic, 14)), "heartbeat 14, sent before the stop and delivered after it, is refused");
        assert!(!book.record(at(MemberStatus::Pending, 15)), "a pending heartbeat of the stopped birth is refused: only node.joined ends the stop");
        assert!(book.get(id.as_str()).is_none(), "no heartbeat brought the stopped birth back");
        assert_eq!(book.in_flight(), vec![stop.clone()], "the stop overlay stands");
        assert!(!book.routable(id.as_str()), "the stopped birth stays unroutable");

        // A later incarnation of the same NodeId is another birth: it ends the stop by the ordinary rules.
        let next = IncarnationId::mint();
        assert!(book.record(digest("mesh1.admin.2", &id, &next, Some(inc.clone()), MemberStatus::Pending, 1, 41_003)), "a later incarnation is taken");
        assert!(book.in_flight().is_empty(), "a later incarnation ends the stop of the older birth");

        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let refused: Vec<&Value> = named(&spans, "rdm.mesh.membership.reject.via-stopped-birth").into_iter().filter(|s| attr(s, "node_id") == id.as_str()).collect();
        assert_eq!(refused.iter().map(|s| attr(s, "digest_seq")).collect::<Vec<_>>(), vec!["14".to_string(), "15".to_string()], "each refused heartbeat is named with its digest_seq: {refused:#?}");
        assert!(refused.iter().all(|s| attr(s, "incarnation_id") == inc.as_str() && attr(s, "held_digest_seq") == "13"), "{refused:#?}");
        let started: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-node-started").into_iter().filter(|s| attr(s, "incarnation_id") == inc.as_str()).collect();
        assert!(started.is_empty(), "no heartbeat of the stopped birth ended its stop: {started:#?}");
        std::fs::write(dir.join("trace-ids.txt"), refused.iter().map(|s| format!("{} {} digest_seq={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "digest_seq"))).collect::<String>()).unwrap();
    });
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

/// The chunks of `mesh`'s snapshot in `topology` as a topology read delivers them.
fn read_chunks(topology: &[rafka_mesh_transport::snapshot::SourceSnapshot], mesh: &str) -> Vec<Chunk> {
    let s = topology.iter().find(|s| s.mesh == mesh).expect("the read holds the mesh");
    chunks_of(&s.full(), |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
        mesh: s.mesh.clone(),
        publisher: s.publisher.clone(),
        forwarded_by: None,
        topology_version: s.topology_version,
        published_at_rafka_ms: 1,
        snapshot_id: 900,
        chunk_index,
        chunk_count,
        digests,
        in_flight,
        departed,
    })
    .into_iter()
    .map(|f| match f {
        Frame::Members { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed, .. } => Chunk { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed },
        _ => unreachable!("chunks_of shapes Members"),
    })
    .collect()
}

/// CONTRACT (gossip.md §3.1 `Members` / `MembersDelta`: "Own-mesh members receive authoritative
/// statuses through the primary's versioned member projection ... For an own-mesh birth, liveness,
/// endpoints with freshness and the runtime fact come from its Digest and status from the
/// projection; NodeStopped, NodeDeleting and NodeRestarting stay immediate events"): through real
/// gossip, mesh1.rpc.2 (an own-mesh member) holds mesh1.rpc.1 stopped by the mesh-primary's
/// `NodeStopped`. Every heartbeat of the stopped birth it hears (a late ready one, a pending one) is
/// refused by name, and so is a projection entry of the birth from a version that never carried the
/// stop (a read built before the stop, at the version held). The member learns the stop ended only
/// from the mesh-primary's projection: once the primary's projection moves, in order, from the
/// version carrying the stop to the one listing the birth after `node.joined`. Its status is then the
/// projection's `Pending` whatever the birth's next heartbeat says, and `ReadyForTraffic` only when
/// the projection says so. Where the stop ends, the member holds the birth again from the latest
/// heartbeat of it it refused, with the projection's status and that heartbeat's liveness, so the birth
/// never vanishes from its view. Must NOT happen: a heartbeat, or a projection entry from before the
/// stop, ending the stop or moving the status; the birth missing from the view once its stop ended.
#[test]
fn an_own_mesh_member_holding_node_stopped_learns_the_stop_ended_only_from_the_mesh_primarys_projection() {
    let dir = crate::evidence_dir();
    let capture = rafka_mesh_telemetry::EvidenceCapture::start(&dir, "rafka-mesh-transport-s52").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (p_node, m_node) = (node().await, node().await);
        let p_addr = addr_of(&p_node.ep);
        learn_addresses(&m_node.ep, &[p_addr.clone()]).unwrap();
        let mesh_id = MeshId::mint();
        let clock = rafka_mesh_transport::clock::os_clock;
        let p = Membership::join(&p_node.gossip, &p_node.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![]).await.unwrap();
        let m = Membership::join(&m_node.gossip, &m_node.ep, &fabric(), "mesh1", &mesh_id, "mesh1.rpc.2", clock(), vec![p_addr]).await.unwrap();
        let p_birth = IncarnationId::mint();
        let bb = Backbone::join(&p_node.gossip, &p_node.ep, &p, "mesh1", "mesh1.admin.1", p_birth.clone(), vec![]).await.unwrap();
        bb.set_mesh_primary(true);
        m.wait_for_neighbour().await;
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: p_birth.clone() };
        let round = || async { bb.publish(&p, bb.own_members(&p)).await };
        let version_at_m = || m.held_source_version("mesh1").filter(|(pb, _)| *pb == publisher).map(|(_, v)| v);

        // The mesh-primary's own digest, heard by the member directly.
        let p_digest = digest("mesh1.admin.1", &NodeId::mint(), &p_birth, None, MemberStatus::ReadyForTraffic, 0, 41_001);
        p.publish(&p_digest).await.unwrap();
        learn_primary_seat(&m, "mesh1", &p_digest.node.node_id, &p_birth);
        // mesh1.rpc.1: its heartbeats as the primary and the member hear them.
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let b = |status, seq| digest("mesh1.rpc.1", &id, &inc, None, status, seq, 41_011);
        assert!(p.book.record(b(MemberStatus::ReadyForTraffic, 1)));
        assert!(p.book.started(&id, &inc), "the primary sets it ready-for-traffic from its node.started");
        m.book.record(b(MemberStatus::ReadyForTraffic, 1));

        // v1: the primary's projection lists mesh1.rpc.1 ready.
        round().await;
        until("the member to hold the primary's first projection", || version_at_m().is_some()).await;
        let v1 = version_at_m().unwrap();
        // What the primary serves a topology read now: built before the stop.
        let before_stop = p.held_topology(&p.book.get(p_digest.node.node_id.as_str()).expect("the primary holds itself").0);

        // The stop: the primary publishes NodeStopped from the stopped reply; the member takes the event.
        let stop = stop_op(&b(MemberStatus::ReadyForTraffic, 1));
        publish_stopped(&p, &bb, &stop).await;
        until("the member to take NodeStopped", || m.book.in_flight().contains(&stop)).await;
        assert!(m.book.get(id.as_str()).is_none(), "the member dropped the stopped birth");

        // A read built before the stop, delivered after it, at the version the member holds: it lists the
        // stopped birth from a projection that never carried the stop. Refused by name; the stop stands.
        for c in read_chunks(&before_stop, "mesh1") {
            let _ = m.take_read_chunk(c, "mesh1");
        }
        assert!(m.book.get(id.as_str()).is_none() && m.book.in_flight().contains(&stop), "a projection entry from before the stop does not end it");

        // v2: the primary's projection carries the stop and no longer lists the birth.
        round().await;
        until("the member to hold the projection carrying the stop", || version_at_m().is_some_and(|v| v > v1)).await;
        let v2 = version_at_m().unwrap();

        // The stopped birth's heartbeats reach the member: a late ready one (sent before the stop) and a
        // pending one (the shape of its rejoin). Each is refused by name; the stop stands.
        assert!(!m.book.record(b(MemberStatus::ReadyForTraffic, 2)), "a late ready heartbeat of the stopped birth is refused");
        assert!(!m.book.record(b(MemberStatus::Pending, 3)), "a pending heartbeat of the stopped birth is refused");
        // When the member heard heartbeat 3: the birth's own liveness word, whatever ends the stop later.
        let heard_3 = std::time::Instant::now();
        assert!(m.book.get(id.as_str()).is_none(), "no heartbeat brought the stopped birth back");
        assert!(m.book.in_flight().contains(&stop), "only the mesh-primary's projection ends the stop");

        // node.joined at the mesh-primary (its start workflow took `Started`): its own book ends the stop
        // and holds the birth Pending; v3, its next projection, lists the birth again.
        let start = LifecycleOp { operation: "start-node:mesh1.rpc.1".into(), ..stop.clone() };
        assert!(p.book.joined(&start), "the mesh-primary ends the stop at node.joined");
        assert_eq!(p.book.get(id.as_str()).map(|(d, _)| d.status), Some(MemberStatus::Pending), "the primary holds the birth Pending");
        round().await;
        until("the member to end the stop from the primary's projection", || !m.book.in_flight().contains(&stop)).await;
        let v3 = version_at_m().unwrap();
        assert!(v3 > v2, "the stop ended at a newer version than the one carrying it");
        // The birth never vanishes from the member's view: it is held again from the latest heartbeat of
        // it the member refused (3), with the projection's status, and with that heartbeat's liveness: the
        // projection gives the status, never liveness.
        let since_3 = heard_3.elapsed();
        let (again, silent) = m.book.get(id.as_str()).expect("the member holds the birth again once its projection ends the stop");
        assert_eq!((again.node.incarnation.clone(), again.digest_seq, again.status), (inc.clone(), 3, MemberStatus::Pending), "held from the latest refused heartbeat, with the projection's status");
        assert!(silent >= since_3, "its liveness is heartbeat 3's, never refreshed by the projection: silent {silent:?}, heard {since_3:?} ago");
        assert!(m.book.record(b(MemberStatus::ReadyForTraffic, 4)), "the birth's heartbeat is taken once the stop is over");
        assert_eq!(m.book.get(id.as_str()).map(|(d, _)| d.status), Some(MemberStatus::Pending), "its status is the projection's Pending, not the heartbeat's ReadyForTraffic");

        // node.started: the primary takes the birth's report (its DeclareNodeState(ReadyForTraffic)); its
        // heartbeat says nothing of it. Its next projection says ready.
        assert!(p.book.record(b(MemberStatus::ReadyForTraffic, 4)));
        assert_eq!(p.book.get(id.as_str()).map(|(d, _)| d.status), Some(MemberStatus::Pending), "the birth's heartbeat moves no status at the primary");
        assert!(p.book.started(&id, &inc), "node.started of the birth");
        round().await;
        until("the member to take ready-for-traffic from the projection", || m.book.get(id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::ReadyForTraffic)).await;
        let v4 = version_at_m().unwrap();

        drop(bb);
        for n in [&p_node, &m_node] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let refused: Vec<&Value> = named(&spans, "rdm.mesh.membership.reject.via-stopped-birth").into_iter().filter(|s| attr(s, "node_id") == id.as_str()).collect();
        assert!(refused.iter().any(|s| attr(s, "door") == "heartbeat" && attr(s, "digest_seq") == "2"), "the late heartbeat is refused by name: {refused:#?}");
        assert!(refused.iter().any(|s| attr(s, "door") == "projection" && attr(s, "held_version") == v1.to_string() && attr(s, "topology_version") == v1.to_string()), "the projection entry from before the stop is refused naming both versions: {refused:#?}");
        let ended: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-stop-ended-in-projection").into_iter().filter(|s| attr(s, "node_id") == id.as_str()).collect();
        assert_eq!(ended.len(), 1, "the member ended the stop once, at the ordered move: {ended:#?}");
        assert_eq!((attr(ended[0], "node"), attr(ended[0], "held_version"), attr(ended[0], "topology_version"), attr(ended[0], "status")), ("mesh1.rpc.2".to_string(), v2.to_string(), v3.to_string(), "Pending".to_string()));
        let joined: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-node-joined").into_iter().filter(|s| attr(s, "node_id") == id.as_str()).collect();
        assert_eq!((joined.len(), joined.first().map(|s| attr(s, "node"))), (1, Some("mesh1.admin.1".to_string())), "the mesh-primary took node.joined once: {joined:#?}");
        let delta: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-forwarded-delta").into_iter().filter(|s| attr(s, "node") == "mesh1.admin.1" && attr(s, "mesh") == "mesh1" && attr(s, "topology_version") == v3.to_string()).collect();
        assert_eq!(delta.len(), 1, "the primary published the projection that clears the stop on its own channel: {delta:#?}");
        assert_eq!((attr(delta[0], "base_version"), attr(delta[0], "changed"), attr(delta[0], "in_flight")), (v2.to_string(), "1".to_string(), "0".to_string()), "the clear travels as the birth's own changed entry");
        let statuses: Vec<String> = named(&spans, "rdm.mesh.membership.update.via-projected-status").into_iter().filter(|s| attr(s, "member_id") == id.as_str() && attr(s, "node") == "mesh1.rpc.2").map(|s| format!("{}@{}", attr(s, "status"), attr(s, "topology_version"))).collect();
        assert_eq!(statuses, vec![format!("ReadyForTraffic@{v1}"), format!("Pending@{v3}"), format!("ReadyForTraffic@{v4}")], "the member's statuses of the birth come from the projection alone");
        let mut lines = String::new();
        for (what, s) in refused.iter().map(|s| ("refused", *s)).chain(ended.iter().map(|s| ("ended", *s))).chain(joined.iter().map(|s| ("joined", *s))).chain(delta.iter().map(|s| ("delta", *s))) {
            lines.push_str(&format!("{what} {} {} door={} held_version={} topology_version={} digest_seq={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "door"), attr(s, "held_version"), attr(s, "topology_version"), attr(s, "digest_seq")));
        }
        std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
    });
}

/// CONTRACT (node-status-heartbeat-connections.md R-HB1 "The version field": "Each status fact carries
/// its subject's `topology_version`"; §A.2: "A status fact carries the `topology_version` at which its
/// subject changed ... Receivers compare per subject"; §C "missed event repaired by top-up"): through
/// real gossip, mesh1.rpc.2 (an own-mesh member holding its mesh-primary's seat) holds the primary's
/// projection v1 listing mesh1.rpc.1 ready and takes the primary's `NodeStopped` event. No projection
/// carrying the stop ever reaches it: the primary takes node.joined of the birth before its next
/// round, and the member is parked through that round. Its top-up (a topology read of the primary)
/// installs the projection listing the birth `Pending`, at a version past the one the stop changed the
/// subject at, of the same publisher: the stop is over, and the member holds the birth again with
/// the projection's status. Must NOT happen: the stop standing forever at a member that never applied
/// a projection carrying it (it holds the stop only from the event, which carried no version, so no
/// projection could be ordered past it).
#[test]
fn a_member_that_never_applied_a_projection_carrying_a_stop_ends_it_from_a_top_up_after_node_joined() {
    let dir = crate::evidence_dir();
    let capture = rafka_mesh_telemetry::EvidenceCapture::start(&dir, "rafka-mesh-transport-s52").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (p_node, m_node) = (node().await, node().await);
        let p_addr = addr_of(&p_node.ep);
        learn_addresses(&m_node.ep, &[p_addr.clone()]).unwrap();
        let mesh_id = MeshId::mint();
        let clock = rafka_mesh_transport::clock::os_clock;
        let p = Membership::join(&p_node.gossip, &p_node.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![]).await.unwrap();
        let m = Membership::join(&m_node.gossip, &m_node.ep, &fabric(), "mesh1", &mesh_id, "mesh1.rpc.2", clock(), vec![p_addr]).await.unwrap();
        let p_birth = IncarnationId::mint();
        let bb = Backbone::join(&p_node.gossip, &p_node.ep, &p, "mesh1", "mesh1.admin.1", p_birth.clone(), vec![]).await.unwrap();
        bb.set_mesh_primary(true);
        m.wait_for_neighbour().await;
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: p_birth.clone() };
        let round = || async { bb.publish(&p, bb.own_members(&p)).await };
        let version_at_m = || m.held_source_version("mesh1").filter(|(pb, _)| *pb == publisher).map(|(_, v)| v);

        let p_digest = digest("mesh1.admin.1", &NodeId::mint(), &p_birth, None, MemberStatus::ReadyForTraffic, 0, 41_021);
        p.publish(&p_digest).await.unwrap();
        learn_primary_seat(&m, "mesh1", &p_digest.node.node_id, &p_birth);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let b = |status, seq| digest("mesh1.rpc.1", &id, &inc, None, status, seq, 41_031);
        assert!(p.book.record(b(MemberStatus::ReadyForTraffic, 1)));
        assert!(p.book.started(&id, &inc), "the primary sets it ready-for-traffic from its node.started");
        m.book.record(b(MemberStatus::ReadyForTraffic, 1));

        // v1: the primary's projection lists mesh1.rpc.1 ready.
        round().await;
        until("the member to hold the primary's first projection", || version_at_m().is_some()).await;
        let v1 = version_at_m().unwrap();

        // The stop: the member takes the primary's NodeStopped event, and nothing else of it.
        let stop = stop_op(&b(MemberStatus::ReadyForTraffic, 1));
        publish_stopped(&p, &bb, &stop).await;
        until("the member to take NodeStopped", || m.book.in_flight().contains(&stop)).await;
        assert_eq!(version_at_m(), Some(v1), "no projection carrying the stop reached the member");

        // node.joined at the mesh-primary before its next round; the member is parked through the round,
        // so the projection listing the birth again reaches it only through its top-up.
        let start = LifecycleOp { operation: "start-node:mesh1.rpc.1".into(), ..stop.clone() };
        assert!(p.book.joined(&start), "the mesh-primary ends the stop at node.joined");
        m.park().await;
        round().await;
        let served = p.held_topology(&p.book.get(p_digest.node.node_id.as_str()).expect("the primary holds itself").0);
        let read_version = served.iter().find(|s| s.mesh == "mesh1").map(|s| s.topology_version).expect("the primary serves its own mesh");
        assert!(read_version > v1, "the primary's round after node.joined moved its projection");
        for c in read_chunks(&served, "mesh1") {
            let _ = m.take_read_chunk(c, "mesh1");
        }
        assert_eq!(version_at_m(), Some(read_version), "the top-up installed the primary's projection");
        assert!(!m.book.in_flight().contains(&stop), "the top-up's projection, past the version the stop changed the birth at, ends the stop");
        assert_eq!(m.book.get(id.as_str()).map(|(d, _)| (d.node.incarnation.clone(), d.status)), Some((inc.clone(), MemberStatus::Pending)), "the member holds the birth again with the projection's status");
        m.unpark().await;

        drop(bb);
        for n in [&p_node, &m_node] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let stopped: Vec<&Value> = named(&spans, "rdm.mesh.membership.remove.via-node-stopped").into_iter().filter(|s| attr(s, "node_id") == id.as_str() && attr(s, "holder") == "mesh1.rpc.2").collect();
        assert_eq!(stopped.len(), 1, "the member took the event once: {stopped:#?}");
        let stop_version = attr(stopped[0], "topology_version");
        assert_eq!(attr(stopped[0], "publisher"), publisher.to_string(), "the event names its publisher: {stopped:#?}");
        let ended: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-stop-ended-in-projection").into_iter().filter(|s| attr(s, "node_id") == id.as_str()).collect();
        assert_eq!(ended.len(), 1, "the member ended the stop once: {ended:#?}");
        assert_eq!(
            (attr(ended[0], "node"), attr(ended[0], "channel"), attr(ended[0], "stop_carried_at"), attr(ended[0], "topology_version"), attr(ended[0], "status")),
            ("mesh1.rpc.2".to_string(), "topology-read".to_string(), stop_version.clone(), read_version.to_string(), "Pending".to_string()),
            "ended by the top-up, ordered past the version the event carried"
        );
        assert!(stop_version.parse::<u64>().unwrap() > v1 && read_version > stop_version.parse::<u64>().unwrap(), "v1 {v1} < stop {stop_version} < read {read_version}");
        let lines: String = stopped.iter().map(|s| ("stopped", *s)).chain(ended.iter().map(|s| ("ended", *s))).map(|(what, s)| format!("{what} {} {} publisher={} topology_version={} stop_carried_at={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "publisher"), attr(s, "topology_version"), attr(s, "stop_carried_at"))).collect();
        std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
    });
}
