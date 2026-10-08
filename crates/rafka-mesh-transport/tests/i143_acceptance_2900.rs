//! i143.e6.s12 acceptance (rafka-v2 #2900, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2900-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.
//!
//! CONTRACT: a receiving admin installs a chunked `Members` snapshot only once it holds EVERY
//! chunk of it, and only then advances the held source version; chunks interleaved, duplicated
//! or missed never produce a partial install, a version move or a delta. The source Mesh's
//! `topology_version` bumps on a material change of the normalized projection (a birth, an
//! endpoint, a routing status, a runtime fact, an overlay or a departure) and never on
//! `digest_seq`, a Rafka-time stamp, a load or an unchanged republish. The forwarding primary
//! derives the delta from the two fulls it holds, so one change is one delta from the version it
//! last published into its Mesh, a missed intermediate version is one delta not a replay, a
//! heartbeat-only Mesh produces no delta, and a seat change publishes a full first. A delta
//! applies only at exactly its base; a copy already applied is a no-op; any other desynchronizes
//! that source alone. The same holds through real gossip between three endpoints: the source
//! primary's backbone aggregate, the forwarding primary, and an ordinary member of its Mesh.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{backbone_topic, learn_addresses, Backbone, Frame, Membership, MAX_FRAME};
use rafka_mesh_transport::snapshot::{chunks_of, Chunk, Delta, Forward, Forwarder, Full, Gap, Moved, PublisherId, SnapshotReceiver, SourceSnapshot, SourceVersion, Taken};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const CELL: &str = "members_receiver_commits_complete_snapshot_before_advancing_version";

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2900/unit").join(CELL),
    }
}

fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        let mine = format!(".{}-", std::process::id());
        if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl") && n.to_string_lossy().contains(&mine)) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
}

fn named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

fn attr(sp: &Value, k: &str) -> String {
    sp["attributes"][k].as_str().unwrap_or_default().to_string()
}

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

/// One member of `mesh` at `ordinal`; the NodeId is minted once per ordinal by the caller.
fn member(mesh: &str, ordinal: u32, node_id: &NodeId, birth: &IncarnationId, seq: u64) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: format!("{mesh}.rpc.{ordinal}").parse().unwrap(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
            incarnation: birth.clone(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: seq,
        emitted_at_rafka_ms: 1_000 + seq,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
    }
}

struct Mesh {
    mesh: String,
    ids: Vec<(NodeId, IncarnationId)>,
}

impl Mesh {
    fn new(mesh: &str, n: u32) -> Self {
        Self { mesh: mesh.into(), ids: (0..n).map(|_| (NodeId::mint(), IncarnationId::mint())).collect() }
    }
    fn digests(&self, seq: u64) -> Vec<MeshDigest> {
        self.ids.iter().enumerate().map(|(k, (id, b))| member(&self.mesh, k as u32 + 1, id, b, seq)).collect()
    }
}

fn op(d: &MeshDigest, operation: &str) -> LifecycleOp {
    LifecycleOp {
        build_id: "b1".into(),
        attempt: 1,
        operation: operation.into(),
        node_id: d.node.node_id.clone(),
        incarnation: d.node.incarnation.clone(),
        name: d.node.name.clone(),
        event_at_rafka_ms: 7,
    }
}

fn publisher(node: &str) -> PublisherId {
    PublisherId { node: node.into(), incarnation: IncarnationId::mint() }
}

/// The chunks of `full` as the source primary's backbone `Members`.
fn members_chunks(full: &Full, p: &PublisherId, version: u64, snapshot_id: u64, forwarded_by: Option<&str>) -> Vec<Frame> {
    chunks_of(full, |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
        mesh: "mesh2".into(),
        publisher: p.clone(),
        forwarded_by: forwarded_by.map(String::from),
        topology_version: version,
        published_at_rafka_ms: 1,
        snapshot_id,
        chunk_index,
        chunk_count,
        digests,
        in_flight,
        departed,
    })
}

fn chunk_of(f: &Frame) -> Chunk {
    match f.clone() {
        Frame::Members { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed, .. } => {
            Chunk { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed }
        }
        other => panic!("not a Members chunk: {other:?}"),
    }
}

fn delta_of(f: &Frame) -> (String, PublisherId, u64, u64, Delta) {
    match f.clone() {
        Frame::MembersDelta { mesh, source_publisher, base_version, topology_version, changed, removed, in_flight, departed, .. } => {
            (mesh, source_publisher, base_version, topology_version, Delta { changed, removed, in_flight, departed })
        }
        other => panic!("not a MembersDelta: {other:?}"),
    }
}

fn installed(t: Taken) -> rafka_mesh_transport::snapshot::Install {
    match t {
        Taken::Installed(i) => *i,
        other => panic!("expected an installed snapshot: {other:?}"),
    }
}

async fn endpoint() -> iroh::Endpoint {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap()
}

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let ep = endpoint().await;
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

async fn wait<T, Fut: std::future::Future<Output = Option<T>>>(what: &str, mut probe: impl FnMut() -> Fut) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    loop {
        if let Some(v) = probe().await {
            return v;
        }
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn members_receiver_commits_complete_snapshot_before_advancing_version() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-acceptance");
    let mut result = serde_json::Map::new();

    // ---- 1. topology_version: a material change bumps it, nothing else does ----------------
    let mesh2 = Mesh::new("mesh2", 3);
    let base = Full::new(mesh2.digests(1), vec![], vec![]);
    let mut v = SourceVersion::default();
    let mut bumps = Vec::new();
    bumps.push(("first projection", v.observe(&base)));
    let mut heartbeat = mesh2.digests(900);
    heartbeat.iter_mut().for_each(|d| d.emitted_at_rafka_ms = 5_000_000);
    bumps.push(("heartbeats: digest_seq and emitted_at_rafka_ms alone", v.observe(&Full::new(heartbeat.clone(), vec![], vec![]))));
    heartbeat.iter_mut().for_each(|d| d.in_flight = Some(17));
    bumps.push(("loads alone", v.observe(&Full::new(heartbeat.clone(), vec![], vec![]))));
    bumps.push(("an unchanged republish", v.observe(&base)));
    let mut draining = mesh2.digests(2);
    draining[1].status = MemberStatus::Draining;
    bumps.push(("a status routing reads", v.observe(&Full::new(draining.clone(), vec![], vec![]))));
    let mut moved = draining.clone();
    moved[2].node.transport_addr = "127.0.0.1:49999".parse().unwrap();
    bumps.push(("an endpoint moved", v.observe(&Full::new(moved.clone(), vec![], vec![]))));
    let mut reborn = moved.clone();
    let next_birth = IncarnationId::mint();
    reborn[0].node.supersedes = Some(reborn[0].node.incarnation.clone());
    reborn[0].node.incarnation = next_birth;
    bumps.push(("a birth's incarnation changed", v.observe(&Full::new(reborn.clone(), vec![], vec![]))));
    let deleting = op(&reborn[1], "retire-node:mesh2.rpc.2");
    bumps.push(("an in_flight overlay added", v.observe(&Full::new(reborn.clone(), vec![deleting.clone()], vec![]))));
    bumps.push(("an in_flight overlay cleared with the departure", v.observe(&Full::new(vec![reborn[0].clone(), reborn[2].clone()], vec![], vec![deleting.clone()]))));
    bumps.push(("a departed identity expired", v.observe(&Full::new(vec![reborn[0].clone(), reborn[2].clone()], vec![], vec![]))));
    bumps.push(("a birth added", v.observe(&Full::new(vec![reborn[0].clone(), reborn[2].clone(), member("mesh2", 9, &NodeId::mint(), &IncarnationId::mint(), 1)], vec![], vec![]))));
    let versions: Vec<u64> = bumps.iter().map(|(_, n)| *n).collect();
    assert_eq!(versions, vec![1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8], "only material changes bump: {bumps:?}");
    result.insert("topology_version_by_observation".into(), json!(bumps.iter().map(|(w, n)| json!({ "observation": w, "topology_version": n })).collect::<Vec<_>>()));

    // ---- 2. a snapshot is chunked under one snapshot_id, every chunk fits one message ------
    let big = Mesh::new("mesh2", 60);
    let mut with_overlays: Vec<LifecycleOp> = Vec::new();
    for d in big.digests(3).iter().take(12) {
        with_overlays.push(op(d, "retire-node:mesh2.rpc.1"));
    }
    let big_full = Full::new(big.digests(3), with_overlays.clone(), with_overlays.iter().take(5).cloned().collect());
    let p = publisher("mesh2.admin.1");
    let chunks = members_chunks(&big_full, &p, 5, 9, None);
    assert!(chunks.len() > 2, "sixty members with overlays do not fit one message: {} chunks", chunks.len());
    for c in &chunks {
        assert!(c.encode().len() <= MAX_FRAME, "a chunk fits one gossip message");
    }
    let empty = members_chunks(&Full::default(), &p, 1, 1, None);
    assert_eq!(empty.len(), 1, "an empty Mesh is one empty chunk, a complete snapshot");
    result.insert("chunks_of_sixty_members".into(), json!({ "chunks": chunks.len(), "largest_chunk_bytes": chunks.iter().map(|c| c.encode().len()).max(), "max_frame": MAX_FRAME }));

    // ---- 3. the receiver: no partial install, no version move, no delta from a partial ----
    let mut rx = SnapshotReceiver::default();
    let held_before = rx.held_version("mesh2");
    // Missed and duplicated chunks: 0 and the LAST, then 0 again.
    let last = chunks.len() - 1;
    assert!(matches!(rx.take_chunk(chunk_of(&chunks[0])), Taken::Waiting { held: 1, .. }));
    assert!(matches!(rx.take_chunk(chunk_of(&chunks[last])), Taken::Waiting { held: 2, .. }));
    assert!(matches!(rx.take_chunk(chunk_of(&chunks[0])), Taken::Waiting { held: 2, .. }), "a duplicate chunk adds nothing");
    assert_eq!(rx.held_version("mesh2"), held_before, "a partial snapshot moves no version");
    assert!(rx.held_full("mesh2").is_none(), "a partial snapshot installs nothing");
    // A delta derived from the incomplete one is not applied either.
    let a_delta = Delta { changed: vec![big.digests(4)[0].clone()], ..Default::default() };
    assert!(matches!(rx.take_delta("mesh2", &p, 4, 5, &a_delta), Moved::Desynced { gap: Gap::NoBaseline, .. }), "no delta is derived from an incomplete snapshot");
    // The chunks between arrive: the snapshot completes with the last of them, and not before.
    for c in &chunks[1..last - 1] {
        assert!(matches!(rx.take_chunk(chunk_of(c)), Taken::Waiting { .. }));
        assert_eq!(rx.held_version("mesh2"), held_before, "still nothing installed, no version moved");
    }
    // The delta above desynchronized the source; the complete snapshot resumes it.
    let i = installed(rx.take_chunk(chunk_of(&chunks[last - 1])));
    assert_eq!((i.topology_version, i.full.member_count(), i.resumed), (5, 60, true));
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 5)));
    assert!(i.full.same_topology(&big_full), "the installed projection is exactly the published one");
    assert!(rx.desynced().is_empty());

    // Interleaved snapshots of one source: the newer one wins, the stale partial never installs.
    let newer = members_chunks(&Full::new(big.digests(5), vec![], vec![]), &p, 7, 11, None);
    let older = members_chunks(&Full::new(big.digests(4), vec![], vec![]), &p, 6, 10, None);
    assert!(older.len() > 1 && newer.len() > 1);
    assert!(matches!(rx.take_chunk(chunk_of(&older[0])), Taken::Waiting { .. }));
    assert!(matches!(rx.take_chunk(chunk_of(&newer[0])), Taken::Waiting { .. }), "a newer snapshot drops the stale partial");
    let stale = rx.take_chunk(chunk_of(&older[1]));
    assert!(matches!(stale, Taken::Refused(rafka_mesh_transport::snapshot::Refusal::OlderThanPending { .. })), "a chunk of the dropped snapshot is refused by name: {stale:?}");
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 5)), "neither partial moved the held version");
    let mut last_install = None;
    for c in &newer[1..] {
        if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) {
            last_install = Some(*i);
        }
    }
    let i = last_install.expect("the newer snapshot completes");
    assert_eq!((i.topology_version, i.snapshot_id), (7, 11));
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 7)));

    // A snapshot older than the version held from the same publisher is refused; a republish of
    // the same version (a new snapshot id) only refreshes.
    let old_full = members_chunks(&Full::new(big.digests(1), vec![], vec![]), &p, 6, 12, None);
    let mut refused = None;
    for c in &old_full {
        if let Taken::Refused(r) = rx.take_chunk(chunk_of(c)) {
            refused = Some(r);
        }
    }
    assert!(matches!(refused, Some(rafka_mesh_transport::snapshot::Refusal::OlderThanHeld { held_version: 7, offered_version: 6 })), "{refused:?}");
    let again = members_chunks(&Full::new(big.digests(6), vec![], vec![]), &p, 7, 13, None);
    let mut refreshed = None;
    for c in &again {
        if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) {
            refreshed = Some(*i);
        }
    }
    assert!(refreshed.expect("the republish completes").refreshed, "the same version again only hears the members again");
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 7)));

    // Malformed chunks are refused by name.
    let mut bad = chunk_of(&chunks[0]);
    bad.chunk_index = bad.chunk_count;
    assert!(matches!(rx.take_chunk(bad), Taken::Refused(rafka_mesh_transport::snapshot::Refusal::MalformedChunk { .. })));

    // ---- 4. deltas: exactly the base, a copy is a no-op, anything else desynchronizes -------
    let f7 = rx.held_full("mesh2").unwrap().clone();
    let mut changed_member = big.digests(7);
    changed_member[3].status = MemberStatus::Draining;
    let f8 = Full::new(changed_member.clone(), vec![], vec![]);
    let d78 = f7.delta_to(&f8);
    assert_eq!((d78.changed.len(), d78.removed.len(), d78.in_flight.len(), d78.departed.len()), (1, 0, 0, 0), "one remote change is one changed member");
    assert_eq!(d78.changed[0].node.node_id, changed_member[3].node.node_id);
    assert!(d78.changed.iter().all(|d| d.in_flight.is_none()), "loads are omitted");
    assert!(matches!(rx.take_delta("mesh2", &p, 7, 8, &d78), Moved::Applied { topology_version: 8, .. }));
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 8)));
    assert!(matches!(rx.take_delta("mesh2", &p, 7, 8, &d78), Moved::Duplicate { held_version: 8 }), "a delta delivered twice is not a gap");
    let gap = rx.take_delta("mesh2", &p, 9, 10, &d78);
    assert!(matches!(gap, Moved::Desynced { gap: Gap::Version { held: 8, base: 9 }, .. }), "{gap:?}");
    assert_eq!(rx.desynced().len(), 1);
    assert!(matches!(rx.take_delta("mesh2", &p, 8, 9, &d78), Moved::Desynced { gap: Gap::AlreadyDesynced, .. }), "a desynchronized source applies no delta");
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 8)), "the held projection stands");
    // Another source is untouched.
    let p3 = publisher("mesh3.admin.1");
    assert!(matches!(rx.take_delta("mesh3", &p3, 1, 2, &d78), Moved::Desynced { gap: Gap::NoBaseline, .. }));
    // The top-up: the primary's baseline is what it last PUBLISHED, which may lag what it holds.
    let published_baseline = SourceSnapshot { mesh: "mesh2".into(), publisher: p.clone(), topology_version: 9, digests: f8.digests(), in_flight: vec![], departed: vec![] };
    let r = installed(rx.install_baseline(&published_baseline));
    assert!(r.resumed);
    assert_eq!(rx.held_version("mesh2"), Some((p.clone(), 9)));
    let mut f10 = changed_member.clone();
    f10[0].status = MemberStatus::Pending;
    let d9_11 = Full::new(f8.digests(), vec![], vec![]).delta_to(&Full::new(f10, vec![], vec![]));
    assert!(matches!(rx.take_delta("mesh2", &p, 9, 11, &d9_11), Moved::Applied { topology_version: 11, .. }), "the primary's next delta continues from the baseline");
    assert!(matches!(rx.take_delta("mesh2", &p, 8, 9, &d78), Moved::Duplicate { .. }), "the delta the top-up already covered is a no-op, not another gap");
    // A new source primary is a new epoch: its delta before its full is a gap, its full resumes.
    let successor = publisher("mesh2.admin.2");
    assert!(matches!(rx.take_delta("mesh2", &successor, 1, 2, &d78), Moved::Desynced { gap: Gap::OtherEpoch { .. }, .. }));
    let first_of_successor = members_chunks(&f8, &successor, 1, 1, None);
    let mut epoch = None;
    for c in &first_of_successor {
        if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) {
            epoch = Some(*i);
        }
    }
    let epoch = epoch.expect("the successor's full installs");
    assert!(epoch.new_epoch && epoch.resumed);
    assert_eq!(rx.held_version("mesh2"), Some((successor.clone(), 1)), "its versions start again, never compared with the previous birth's");

    // A chunk lost for good: the incomplete snapshot is never applied, and the top-up from the
    // Mesh's own primary completes the picture; the straggler chunks that follow install nothing.
    let lossy = members_chunks(&Full::new(big.digests(8), vec![], vec![]), &successor, 8, 14, None);
    assert!(lossy.len() > 2);
    for (k, c) in lossy.iter().enumerate() {
        if k != 1 {
            assert!(matches!(rx.take_chunk(chunk_of(c)), Taken::Waiting { .. }), "chunk {k} of a snapshot missing chunk 1 installs nothing");
        }
    }
    assert_eq!(rx.held_version("mesh2"), Some((successor.clone(), 1)), "the held version stands while a chunk is missing");
    let top_up = SourceSnapshot { mesh: "mesh2".into(), publisher: successor.clone(), topology_version: 8, digests: big.digests(8), in_flight: vec![], departed: vec![] };
    let r = installed(rx.install_baseline(&top_up));
    assert_eq!(r.topology_version, 8);
    assert_eq!(rx.held_version("mesh2"), Some((successor.clone(), 8)), "the top-up completed what the lost chunk left incomplete");
    let late = rx.take_chunk(chunk_of(&lossy[1]));
    assert!(matches!(late, Taken::Waiting { .. } | Taken::Refused(_)), "the straggler installs nothing: {late:?}");
    assert_eq!(rx.held_version("mesh2"), Some((successor.clone(), 8)));
    // A newer snapshot supersedes an incomplete one.
    let lossy2 = members_chunks(&Full::new(big.digests(9), vec![], vec![]), &successor, 9, 15, None);
    assert!(matches!(rx.take_chunk(chunk_of(&lossy2[0])), Taken::Waiting { .. }));
    let newest = members_chunks(&Full::new(big.digests(10), vec![], vec![]), &successor, 10, 16, None);
    let mut done = None;
    for c in &newest {
        if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) {
            done = Some(*i);
        }
    }
    assert_eq!(done.expect("the newer snapshot completes").topology_version, 10);
    let straggler = rx.take_chunk(chunk_of(&lossy2[1]));
    assert!(matches!(straggler, Taken::Refused(_)), "a chunk of the superseded snapshot is refused: {straggler:?}");
    assert_eq!(rx.held_version("mesh2"), Some((successor.clone(), 10)));

    // ---- 5. the forwarding primary: stored fulls determine the delta ------------------------
    let src = publisher("mesh2.admin.1");
    let f41 = Full::new(mesh2.digests(10), vec![], vec![]);
    let mut d43 = mesh2.digests(900);
    d43[1].status = MemberStatus::Draining;
    d43[2].in_flight = Some(3);
    let f43 = Full::new(d43, vec![], vec![]);
    let mut fwd = Forwarder::default();
    let first = fwd.source("mesh1.admin.1", "mesh2", &src, 41, &f41, 1);
    let Forward::Full(first_frames) = first else { panic!("the first publication of a source is a full") };
    let carried_loads = first_frames.iter().any(|f| matches!(f, Frame::Members { digests, .. } if digests.iter().any(|d| d.in_flight.is_some())));
    assert!(!carried_loads, "a forwarded full omits loads");
    // The forwarder never held 42; it holds 43. One delta from what it last published.
    let Forward::Delta(delta) = fwd.source("mesh1.admin.1", "mesh2", &src, 43, &f43, 2) else { panic!("a moved source is a delta") };
    let (mesh, source, base, to, parts) = delta_of(&delta);
    assert_eq!((mesh.as_str(), base, to), ("mesh2", 41, 43), "base_version is what the forwarder last published, not the version before");
    assert_eq!(source, src);
    assert_eq!((parts.changed.len(), parts.removed.len()), (1, 0), "the one member whose routing status moved; a load alone is not a change");
    assert!(parts.changed[0].in_flight.is_none());
    let mut rebuilt = f41.without_loads();
    rebuilt.apply(&parts);
    assert!(rebuilt.same_topology(&f43), "the delta carries exactly the difference between the two fulls");
    // A heartbeat-only Mesh produces no delta; an older or equal version produces none.
    let mut beating = mesh2.digests(5_000);
    beating[1].status = MemberStatus::Draining;
    beating.iter_mut().for_each(|d| d.in_flight = Some(99));
    assert!(matches!(fwd.source("mesh1.admin.1", "mesh2", &src, 43, &Full::new(beating, vec![], vec![]), 3), Forward::Nothing(_)));
    assert!(matches!(fwd.source("mesh1.admin.1", "mesh2", &src, 40, &f41, 3), Forward::Nothing(_)));
    // A version bump that changes nothing a Mesh member holds sends nothing and keeps the base.
    assert!(matches!(fwd.source("mesh1.admin.1", "mesh2", &src, 44, &f43, 3), Forward::Nothing(_)));
    let mut d45 = f43.digests();
    d45[0].status = MemberStatus::Pending;
    let Forward::Delta(next) = fwd.source("mesh1.admin.1", "mesh2", &src, 45, &Full::new(d45, vec![], vec![]), 4) else { panic!("a further move is a delta") };
    let (_, _, base, to, _) = delta_of(&next);
    assert_eq!((base, to), (43, 45), "the base stays the last version published into the Mesh");
    // A seat change publishes a full before any delta; a new source epoch does too.
    fwd.reset();
    assert!(matches!(fwd.source("mesh1.admin.2", "mesh2", &src, 45, &f43, 5), Forward::Full(_)), "taking the seat: a full first");
    assert!(matches!(fwd.source("mesh1.admin.2", "mesh2", &publisher("mesh2.admin.2"), 1, &f41, 6), Forward::Full(_)), "a new source primary is a new epoch: a full first");
    // A delta too large for one message is the full.
    let huge = Mesh::new("mesh2", 80);
    let Forward::Full(_) = fwd.source("mesh1.admin.2", "mesh2", &src, 46, &Full::new(huge.digests(1), vec![], vec![]), 7) else { panic!("a different publisher: full") };
    let mut churned = huge.digests(2);
    churned.iter_mut().for_each(|d| d.status = MemberStatus::Draining);
    assert!(matches!(fwd.source("mesh1.admin.2", "mesh2", &src, 47, &Full::new(churned, vec![], vec![]), 8), Forward::Full(_)), "a delta that does not fit one message is the full");
    result.insert(
        "forwarder".into(),
        json!({ "delta_base": base, "delta_to": to, "delta_changed": parts.changed.len(), "first_publication": "full", "missed_intermediate_versions": "one delta 41 -> 43" }),
    );

    // ---- 6. through real gossip: source primary -> forwarding primary -> ordinary member -----
    let (src_node, fwd_node, member_node) = (node().await, node().await, node().await);
    let (src_addr, fwd_addr) = (addr_of(&src_node.ep), addr_of(&fwd_node.ep));
    // One side dials: the forwarding primary joins the backbone through the source primary, and
    // the ordinary member joins its Mesh channel through the forwarding primary.
    learn_addresses(&fwd_node.ep, &[src_addr.clone()]).unwrap();
    learn_addresses(&member_node.ep, &[fwd_addr.clone()]).unwrap();
    let fab = fabric();
    let (mesh1_id, mesh2_id) = (MeshId::mint(), MeshId::mint());
    let src_m = Membership::join(&src_node.gossip, &src_node.ep, &fab, "mesh2", &mesh2_id, "mesh2.admin.1", rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let fwd_m = Membership::join(&fwd_node.gossip, &fwd_node.ep, &fab, "mesh1", &mesh1_id, "mesh1.admin.1", rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let mem_m = Membership::join(&member_node.gossip, &member_node.ep, &fab, "mesh1", &mesh1_id, "mesh1.rpc.1", rafka_mesh_transport::clock::os_clock(), vec![fwd_addr.clone()]).await.unwrap();
    let src_birth = IncarnationId::mint();
    let src_bb = Backbone::join(&src_node.gossip, &src_node.ep, &src_m, "mesh2", "mesh2.admin.1", src_birth.clone(), vec![]).await.unwrap();
    let fwd_bb = Backbone::join(&fwd_node.gossip, &fwd_node.ep, &fwd_m, "mesh1", "mesh1.admin.1", IncarnationId::mint(), vec![src_addr.clone()]).await.unwrap();
    let _ = backbone_topic(&fab);
    src_bb.set_mesh_primary(true);
    fwd_bb.set_mesh_primary(true);
    let remote = Mesh::new("mesh2", 40);
    // Publish rounds (the admin's hierarchy loop does this every gossip interval) until the
    // forwarding primary holds the snapshot, then until the ordinary member does.
    let held_by_fwd = {
        let mut seq = 1;
        wait("the forwarding primary to install the source snapshot", || {
            seq += 1;
            let (bb, m, members, fbb) = (src_bb.clone(), src_m.clone(), remote.digests(seq), fwd_bb.clone());
            async move {
                bb.publish(&m, members).await;
                fbb.held_source("mesh2")
            }
        })
        .await
    };
    assert_eq!(held_by_fwd.0.node, "mesh2.admin.1");
    assert_eq!(held_by_fwd.0.incarnation, src_birth, "the publisher is the exact birth");
    let v1 = held_by_fwd.1;
    assert_eq!(v1, 1, "one projection, one version, however many rounds");
    let at_member = {
        let mut n = 0;
        wait("the ordinary member to install the forwarded full", || {
            n += 1;
            let (fbb, mm) = (fwd_bb.clone(), mem_m.clone());
            async move {
                if n % 5 == 0 {
                    fbb.forward_fulls("every").await;
                }
                mm.held_source_version("mesh2")
            }
        })
        .await
    };
    assert_eq!(at_member, (held_by_fwd.0.clone(), v1));
    // Heartbeats alone: many more rounds, digest_seq and load moving, no version, no delta.
    for round in 0..12u64 {
        let mut hb = remote.digests(100 + round);
        hb.iter_mut().for_each(|d| {
            d.emitted_at_rafka_ms = 9_000_000 + round;
            d.in_flight = Some(round);
        });
        src_bb.publish(&src_m, hb).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    assert_eq!(fwd_bb.held_source("mesh2").unwrap().1, v1, "a heartbeat-only Mesh does not bump topology_version");
    // One remote change: one version bump, one delta, applied by the ordinary member.
    let mut changed = remote.digests(500);
    changed[7].status = MemberStatus::Draining;
    let (changed_name, changed_id) = (changed[7].node.name.to_string(), changed[7].node.node_id.to_string());
    src_bb.publish(&src_m, changed.clone()).await;
    let v2 = wait("the ordinary member to apply the delta", || {
        let mm = mem_m.clone();
        async move { mm.held_source_version("mesh2").filter(|(_, v)| *v > v1).map(|(_, v)| v) }
    })
    .await;
    assert_eq!(v2, v1 + 1);
    assert!(mem_m.desynced_sources().is_empty(), "the chain held: no gap, no top-up");
    // The member's book holds the changed member as the delta stated it.
    let seen = mem_m.book.get(changed_id.as_str()).expect("the ordinary member holds the remote member").0;
    assert_eq!(seen.status, MemberStatus::Draining, "the delta's change is applied to the member's projection");
    assert!(seen.in_flight.is_none(), "the ordinary member holds no load of a remote member");

    drop(src_bb);
    drop(fwd_bb);
    for n in [&src_node, &fwd_node, &member_node] {
        n.ep.close().await;
    }
    drop(telemetry);
    let spans = collect_spans(&dir);
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let bumps_seen = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|s| attr(s, "node") == "mesh2.admin.1").count();
    assert_eq!(bumps_seen, 2, "the real publisher bumped twice: the first projection and the one change (12 heartbeat rounds bumped nothing)");
    // The source primary forwards its own Mesh as a source too; the remote change is the forwarding primary's delta.
    let deltas_sent: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-forwarded-delta").into_iter().filter(|s| attr(s, "node") == "mesh1.admin.1").collect();
    assert_eq!(deltas_sent.len(), 1, "one remote change produced one delta: {deltas_sent:?}");
    assert_eq!((attr(deltas_sent[0], "base_version"), attr(deltas_sent[0], "topology_version"), attr(deltas_sent[0], "changed")), (v1.to_string(), v2.to_string(), "1".to_string()));
    let applied = named(&spans, "rdm.mesh.membership.update.via-delta").into_iter().filter(|s| attr(s, "node") == "mesh1.rpc.1").collect::<Vec<_>>();
    assert_eq!(applied.len(), 1, "the ordinary member applied it once");
    let installs = named(&spans, "rdm.mesh.membership.update.via-snapshot-installed");
    let backbone_installs = installs.iter().filter(|s| attr(s, "channel") == "backbone" && attr(s, "node") == "mesh1.admin.1").count();
    assert!(backbone_installs >= 1, "the forwarding primary installed complete backbone snapshots");
    let chunked_install = installs.iter().find(|s| attr(s, "channel") == "backbone" && attr(s, "members") == "40").map(|s| (attr(s, "snapshot_id"), attr(s, "topology_version")));
    assert!(chunked_install.is_some(), "a 40-member snapshot installed whole");
    result.insert(
        "real_gossip".into(),
        json!({
            "publisher": held_by_fwd.0.to_string(),
            "first_version": v1,
            "version_after_one_change": v2,
            "topology_version_spans": bumps_seen,
            "heartbeat_rounds_without_a_bump": 12,
            "deltas_forwarded": deltas_sent.len(),
            "delta": { "changed_member": changed_name, "base_version": attr(deltas_sent[0], "base_version"), "topology_version": attr(deltas_sent[0], "topology_version"), "trace_id": deltas_sent[0]["trace_id"], "span_id": deltas_sent[0]["span_id"] },
            "member_applied_spans": applied.iter().map(|s| json!({ "trace_id": s["trace_id"], "span_id": s["span_id"] })).collect::<Vec<_>>(),
            "member_loads_held": false,
        }),
    );
    result.insert("cell".into(), json!(CELL));
    result.insert("spans_read".into(), json!(spans.len()));
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&Value::Object(result)).unwrap()).unwrap();
}
