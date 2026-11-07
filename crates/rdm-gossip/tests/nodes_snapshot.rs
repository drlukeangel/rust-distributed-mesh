//! i143.e6.s12 acceptance (rafka-v2 #2900, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2900-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span the cell's
//! own threads emitted, captured by its `EvidenceCapture`) there.
//!
//! CONTRACT: a receiving admin installs a chunked `TopologyFull` snapshot only once it holds EVERY
//! chunk of it, and only then advances the held source version; chunks interleaved, duplicated
//! or missed never produce a partial install, a version move or a delta. The source Mesh's
//! `topology_version` bumps on a material change of the normalized projection (a birth, an
//! endpoint, a routing status, a primary role, an overlay or a departure) and never on
//! `digest_seq`, a Rafka-time stamp or an unchanged republish. The forwarding primary
//! derives the delta from the two fulls it holds, so one change is one delta from the version it
//! last published into its Mesh, a missed intermediate version is one delta not a replay, a
//! heartbeat-only Mesh produces no delta, and a role change publishes a full first. A delta
//! applies only at exactly its base; a copy already applied is a no-op; any other desynchronizes
//! that source alone. The same holds through real gossip between three endpoints: the source
//! primary's backbone mesh topology, the forwarding primary, and an ordinary member of its Mesh.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::frame::{Frame, MAX_FRAME};
use rdm_gossip::topology::TopologyView;
use rdm_gossip::snapshot::{chunks_of, Chunk, Delta, DeltaChunk, Forward, Forwarder, Full, Gap, Moved, PublisherId, SnapshotReceiver, SourceSnapshot, SourceVersion, Taken};
use serde_json::{json, Value};
use std::time::Duration;

/// Members in the receiver cell's snapshot: enough to span more than two gossip messages.
const BIG: u32 = 150;

const CELL: &str = "members_receiver_commits_complete_snapshot_before_advancing_version";

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
        node: MeshNode { mesh: mesh.to_string(), r#type: "node".into(),
            node_id: node_id.clone(),
            path: format!("{mesh}.node"), name: (ordinal.to_string()).to_string(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
            incarnation: birth.clone(),
            supersedes: None,
        },
        status: NodeStatus::Ready,
        digest_seq: seq,
        emitted_at_rafka_ms: 1_000 + seq,
        primary: Vec::new(),
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

fn publisher(node: &str) -> PublisherId {
    PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }
}

/// The chunks of `full` as `p`'s backbone aggregate of mesh2 at `version`, published at Rafka-time `at`.
fn topology_full_chunks(full: &Full, p: &PublisherId, version: u64, at: u64) -> Vec<Frame> {
    let author = rdm_gossip::envelope::Birth::publisher(&fabric(), p);
    chunks_of(full, |nodes, chunk_index, chunk_count| Frame::aggregate("mesh2", full.mesh_id(), &author, version, at, chunk_index, chunk_count, nodes))
}

/// The chunk an aggregate frame is, as its receiver takes it: its author, version and time from its
/// envelope.
fn chunk_of(f: &Frame) -> Chunk {
    let context = f.context().expect("an aggregate is authored");
    match f.data() {
        Frame::TopologyFull { mesh, mesh_id, chunk_index, chunk_count, nodes } => Chunk {
            mesh: mesh.clone(), mesh_id: mesh_id.clone(), author: Some(context.source.clone()), topology_version: context.version().expect("an aggregate's version"), time_ms: context.time_ms,
            chunk_index: *chunk_index, chunk_count: *chunk_count, digests: nodes.iter().map(|n| n.digest(&context.source.fabric, mesh)).collect(), runtimes: Vec::new(),
        },
        other => panic!("not a TopologyFull chunk: {other:?}"),
    }
}

/// `m`'s nodes at heartbeat `seq`, the first twelve under a removal and the first five of those
/// deleted: each said by the node's status (Q390).
fn with_removals(m: &Mesh, seq: u64) -> Full {
    let mut nodes = m.digests(seq);
    for (k, d) in nodes.iter_mut().take(12).enumerate() {
        d.status = if k < 5 { NodeStatus::Deleted } else { NodeStatus::Deleting };
    }
    Full::new(nodes, vec![], vec![])
}

/// The birth `p` names, of this cell's fabric: a differential's author.
fn author_of(p: &PublisherId) -> rdm_gossip::envelope::Birth {
    rdm_gossip::envelope::Birth { fabric: fabric(), node: p.node_id.clone(), incarnation: p.incarnation.clone() }
}

/// The differential `frames` make, read off the wire: its mesh, its author, its base and version,
/// and what its chunks carry, joined.
fn delta_of(frames: &[Frame]) -> (String, rdm_gossip::envelope::Birth, u64, u64, Delta) {
    let chunks: Vec<DeltaChunk> = frames.iter().map(|f| DeltaChunk::of_frame(&Frame::decode(&f.encode()).expect("the chunk decodes")).expect("a TopologyDelta chunk")).collect();
    let first = chunks.first().expect("a differential has a chunk").clone();
    assert!(chunks.iter().all(|c| (c.base_version, c.version, c.chunk_count) == (first.base_version, first.version, first.chunk_count)), "the chunks are of one differential");
    (first.mesh, first.author, first.base_version, first.version, Delta::joined(chunks.into_iter().map(|c| c.delta)))
}

/// A top-up baseline as a topology read delivers it: the chunks of one snapshot.
fn read_baseline(rx: &mut SnapshotReceiver, s: &SourceSnapshot) -> Taken {
    let mut last = None;
    let p = PublisherId { incarnation: IncarnationId::mint(), node_id: NodeId::mint() };
    for f in topology_full_chunks(&s.full(), &p, s.topology_version, 0) {
        // A read names no author.
        last = Some(rx.take_read_chunk(Chunk { author: None, ..chunk_of(&f) }));
    }
    last.expect("a snapshot has at least one chunk")
}

fn installed(t: Taken) -> rdm_gossip::snapshot::Install {
    match t {
        Taken::Installed(i) => *i,
        other => panic!("expected an installed snapshot: {other:?}"),
    }
}

async fn endpoint() -> iroh::Endpoint {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap()
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

#[test]
fn members_receiver_commits_complete_snapshot_before_advancing_version() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-acceptance").expect("the cell's evidence file can be created");
    capture.run(4, async {
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
        bumps.push(("an unchanged republish", v.observe(&base)));
        let mut connections_deleting = mesh2.digests(2);
        connections_deleting[1].status = NodeStatus::ConnectionsDeleting;
        bumps.push(("a status routing reads", v.observe(&Full::new(connections_deleting.clone(), vec![], vec![]))));
        let mut moved = connections_deleting.clone();
        moved[2].node.transport_addr = "127.0.0.1:49999".parse().unwrap();
        bumps.push(("an endpoint moved", v.observe(&Full::new(moved.clone(), vec![], vec![]))));
        let mut re_born = moved.clone();
        let next_birth = IncarnationId::mint();
        re_born[0].node.supersedes = Some(re_born[0].node.incarnation.clone());
        re_born[0].node.incarnation = next_birth;
        bumps.push(("a birth's incarnation changed", v.observe(&Full::new(re_born.clone(), vec![], vec![]))));
        // An open removal and a departure are the node's status (Q390).
        let mut deleting = re_born.clone();
        deleting[1].status = NodeStatus::Deleting;
        bumps.push(("a node listed deleting", v.observe(&Full::new(deleting.clone(), vec![], vec![]))));
        let mut listed_deleted = deleting.clone();
        listed_deleted[1].status = NodeStatus::Deleted;
        bumps.push(("the node listed deleted", v.observe(&Full::new(listed_deleted, vec![], vec![]))));
        bumps.push(("the deleted node dropped after the retention", v.observe(&Full::new(vec![re_born[0].clone(), re_born[2].clone()], vec![], vec![]))));
        bumps.push(("a birth added", v.observe(&Full::new(vec![re_born[0].clone(), re_born[2].clone(), member("mesh2", 9, &NodeId::mint(), &IncarnationId::mint(), 1)], vec![], vec![]))));
        let versions: Vec<u64> = bumps.iter().map(|(_, n)| *n).collect();
        assert_eq!(versions, vec![1, 1, 1, 2, 3, 4, 5, 6, 7, 8], "only material changes bump: {bumps:?}");
        result.insert("topology_version_by_observation".into(), json!(bumps.iter().map(|(w, n)| json!({ "observation": w, "topology_version": n })).collect::<Vec<_>>()));

        // ---- 2. an aggregate is chunked under its (mesh, version, author), every chunk fits one message
        // A hundred nodes, some under a removal or deleted: the number of chunks postcard needs is recorded as evidence.
        let hundred = Mesh::new("mesh2", 100);
        let hundred_full = with_removals(&hundred, 3);
        let p = publisher("mesh2.node-admin.1");
        let hundred_chunks = topology_full_chunks(&hundred_full, &p, 5, 9);
        assert!(hundred_chunks.len() > 1, "a hundred nodes, some under a removal or deleted, do not fit one message: {} chunks", hundred_chunks.len());
        for c in &hundred_chunks {
            assert!(c.encode().len() <= MAX_FRAME, "a chunk fits one gossip message");
        }
        let empty = topology_full_chunks(&Full::default(), &p, 1, 1);
        assert_eq!(empty.len(), 1, "an empty Mesh is one empty chunk, a complete snapshot");
        result.insert("chunks_of_a_hundred_nodes".into(), json!({ "chunks": hundred_chunks.len(), "largest_chunk_bytes": hundred_chunks.iter().map(|c| c.encode().len()).max(), "max_frame": MAX_FRAME }));

        // The receiver below needs a snapshot that spans more than two chunks.
        let big = Mesh::new("mesh2", BIG);
        let big_full = with_removals(&big, 3);
        let chunks = topology_full_chunks(&big_full, &p, 5, 9);
        assert!(chunks.len() > 2, "{BIG} members with overlays span more than two messages: {} chunks", chunks.len());
        for c in &chunks {
            assert!(c.encode().len() <= MAX_FRAME, "a chunk fits one gossip message");
        }
        result.insert("chunks_of_receiver_snapshot".into(), json!({ "members": BIG, "chunks": chunks.len(), "largest_chunk_bytes": chunks.iter().map(|c| c.encode().len()).max(), "max_frame": MAX_FRAME }));

        // ---- 3. the receiver: no partial install, no version move, no delta from a partial ----
        let mut rx = SnapshotReceiver::default();
        let held_before = rx.held_version("mesh2");
        // Missed and duplicated chunks: 0 and the LAST, then 0 again.
        let last = chunks.len() - 1;
        assert!(matches!(rx.take_chunk(chunk_of(&chunks[0])), Taken::Waiting { held: 1, .. }));
        assert!(matches!(rx.take_chunk(chunk_of(&chunks[last])), Taken::Waiting { held: 2, .. }));
        assert!(matches!(rx.take_chunk(chunk_of(&chunks[0])), Taken::Refused(rdm_gossip::snapshot::Refusal::Duplicate)), "a duplicate chunk is ignored by name and adds nothing");
        assert_eq!(rx.held_version("mesh2"), held_before, "a partial snapshot moves no version");
        assert!(rx.held_full("mesh2").is_none(), "a partial snapshot installs nothing");
        // A delta derived from the incomplete one is not applied either.
        let a_delta = Delta { added: vec![big.digests(4)[0].clone()], ..Default::default() };
        assert!(matches!(rx.take_delta("mesh2", &author_of(&p), 4, 5, &a_delta), Moved::Desynced { gap: Gap::NoBaseline, .. }), "no delta is derived from an incomplete snapshot");
        // The chunks between arrive: the snapshot completes with the last of them, and not before.
        for (offset, c) in chunks[1..last - 1].iter().enumerate() {
            assert!(matches!(rx.take_chunk(chunk_of(c)), Taken::Waiting { held, .. } if held == offset as u32 + 3), "the duplicate retained exactly the original two chunks");
            assert_eq!(rx.held_version("mesh2"), held_before, "still nothing installed, no version moved");
        }
        // The delta above desynchronized the source; the complete snapshot resumes it.
        let i = installed(rx.take_chunk(chunk_of(&chunks[last - 1])));
        assert_eq!((i.topology_version, i.full.member_count(), i.resumed), (5, BIG as usize, true));
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 5)));
        assert!(i.full.same_topology(&big_full), "the installed projection is exactly the published one");
        assert!(rx.desynced().is_empty());

        // Interleaved snapshots of one source: the newer one wins, the stale partial never installs.
        let newer = topology_full_chunks(&Full::new(big.digests(5), vec![], vec![]), &p, 7, 11);
        let older = topology_full_chunks(&Full::new(big.digests(4), vec![], vec![]), &p, 6, 10);
        assert!(older.len() > 1 && newer.len() > 1);
        assert!(matches!(rx.take_chunk(chunk_of(&older[0])), Taken::Waiting { .. }));
        assert!(matches!(rx.take_chunk(chunk_of(&newer[0])), Taken::Waiting { .. }), "a newer snapshot drops the stale partial");
        let stale = rx.take_chunk(chunk_of(&older[1]));
        assert!(matches!(stale, Taken::Refused(rdm_gossip::snapshot::Refusal::OlderThanPending { .. })), "a chunk of the dropped snapshot is refused by name: {stale:?}");
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 5)), "neither partial moved the held version");
        let mut last_install = None;
        for c in &newer[1..] {
            if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) {
                last_install = Some(*i);
            }
        }
        let i = last_install.expect("the newer snapshot completes");
        assert_eq!((i.topology_version, i.time_ms), (7, 11));
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 7)));

        // A new snapshot publication at the held material version is fresh hearing evidence.
        // Its heartbeat sequence may advance without a topology change; only its exact repeat is
        // a duplicate. An equal version with different material content remains inconsistent.
        let again = topology_full_chunks(&Full::new(big.digests(6), vec![], vec![]), &p, 7, 13);
        let mut refreshed = None;
        for c in &again {
            if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) { refreshed = Some(*i); }
        }
        assert!(refreshed.expect("the new publication completes").refreshed);
        for c in &again {
            assert!(matches!(rx.take_chunk(chunk_of(c)), Taken::Refused(rdm_gossip::snapshot::Refusal::Duplicate)), "each installed publication chunk is ignored on its exact repeat");
        }
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 7)));

        // Malformed chunks are refused by name.
        let mut bad = chunk_of(&chunks[0]);
        bad.chunk_index = bad.chunk_count;
        assert!(matches!(rx.take_chunk(bad), Taken::Refused(rdm_gossip::snapshot::Refusal::MalformedChunk { .. })));

        // ---- 4. deltas: exactly the base, a copy is a no-op, anything else desynchronizes -------
        let f7 = rx.held_full("mesh2").unwrap().clone();
        let mut changed_member = big.digests(7);
        changed_member[3].status = NodeStatus::ConnectionsDeleting;
        let f8 = Full::new(changed_member.clone(), vec![], vec![]);
        let d78 = f7.delta_to(&f8);
        assert_eq!((d78.changed.len(), d78.removed.len(), d78.in_flight.len(), d78.deleted.len()), (1, 0, 0, 0), "one remote change is one changed member");
        assert_eq!(d78.changed[0].node_id, changed_member[3].node.node_id);
        assert!(matches!(rx.take_delta("mesh2", &author_of(&p), 7, 8, &d78), Moved::Applied { topology_version: 8, .. }));
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 8)));
        assert!(matches!(rx.take_delta("mesh2", &author_of(&p), 7, 8, &d78), Moved::Duplicate { held_version: 8 }), "a delta delivered twice is not a gap");
        let gap = rx.take_delta("mesh2", &author_of(&p), 9, 10, &d78);
        assert!(matches!(gap, Moved::Desynced { gap: Gap::Version { held: 8, base: 9 }, .. }), "{gap:?}");
        assert_eq!(rx.desynced().len(), 1);
        assert!(matches!(rx.take_delta("mesh2", &author_of(&p), 8, 9, &d78), Moved::Desynced { gap: Gap::AlreadyDesynced, .. }), "a desynchronized source applies no delta");
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 8)), "the held projection stands");
        // Another source is untouched.
        let p3 = publisher("mesh3.node-admin.1");
        assert!(matches!(rx.take_delta("mesh3", &author_of(&p3), 1, 2, &d78), Moved::Desynced { gap: Gap::NoBaseline, .. }));
        // The top-up: the primary's baseline is what it last PUBLISHED, which may lag what it holds.
        let published_baseline = SourceSnapshot { mesh: "mesh2".into(), mesh_id: None, topology_version: 9, digests: f8.digests() };
        let r = installed(read_baseline(&mut rx, &published_baseline));
        assert!(r.resumed);
        assert_eq!(rx.held_version("mesh2"), Some((Some(p.clone()), 9)));
        let mut f10 = changed_member.clone();
        f10[0].status = NodeStatus::Pending;
        let d9_11 = Full::new(f8.digests(), vec![], vec![]).delta_to(&Full::new(f10.clone(), vec![], vec![]));
        assert!(matches!(rx.take_delta("mesh2", &author_of(&p), 9, 11, &d9_11), Moved::Applied { topology_version: 11, .. }), "the primary's next delta continues from the baseline");
        // A new mesh-primary of mesh2 continues the mesh's version (Q389, Q391): its delta at the held
        // base applies, whoever sent the one before, and a full of it below the held version is refused.
        let successor = publisher("mesh2.node-admin.2");
        let mut f12 = f10.clone();
        f12[0].status = NodeStatus::Ready;
        let d11_12 = Full::new(f10.clone(), vec![], vec![]).delta_to(&Full::new(f12, vec![], vec![]));
        assert!(matches!(rx.take_delta("mesh2", &author_of(&successor), 11, 12, &d11_12), Moved::Applied { topology_version: 12, .. }), "the mesh's version, whichever mesh-primary sent it");
        assert_eq!(rx.held_version("mesh2"), Some((Some(successor.clone()), 12)));
        for c in &topology_full_chunks(&f8, &successor, 1, 1) {
            let refused = rx.take_chunk(chunk_of(c));
            assert!(matches!(refused, Taken::Refused(rdm_gossip::snapshot::Refusal::OlderThanHeld { held_version: 12, offered_version: 1 })), "a version below the held one is older, whoever sent it: {refused:?}");
        }

        // A chunk lost for good: the incomplete snapshot is never applied, and the top-up from the
        // Mesh's own primary completes the picture; the straggler chunks that follow install nothing.
        let lossy = topology_full_chunks(&Full::new(big.digests(8), vec![], vec![]), &successor, 18, 14);
        assert!(lossy.len() > 2);
        for (k, c) in lossy.iter().enumerate() {
            if k != 1 {
                assert!(matches!(rx.take_chunk(chunk_of(c)), Taken::Waiting { .. }), "chunk {k} of a snapshot missing chunk 1 installs nothing");
            }
        }
        assert_eq!(rx.held_version("mesh2"), Some((Some(successor.clone()), 12)), "the held version stands while a chunk is missing");
        let top_up = SourceSnapshot { mesh: "mesh2".into(), mesh_id: None, topology_version: 18, digests: big.digests(8) };
        let r = installed(read_baseline(&mut rx, &top_up));
        assert_eq!(r.topology_version, 18);
        assert_eq!(rx.held_version("mesh2"), Some((Some(successor.clone()), 18)), "the top-up completed what the lost chunk left incomplete");
        let late = rx.take_chunk(chunk_of(&lossy[1]));
        assert!(matches!(late, Taken::Waiting { .. } | Taken::Refused(_)), "the straggler installs nothing: {late:?}");
        assert_eq!(rx.held_version("mesh2"), Some((Some(successor.clone()), 18)));
        // A newer snapshot supersedes an incomplete one.
        let lossy2 = topology_full_chunks(&Full::new(big.digests(9), vec![], vec![]), &successor, 19, 15);
        assert!(matches!(rx.take_chunk(chunk_of(&lossy2[0])), Taken::Waiting { .. }));
        let newest = topology_full_chunks(&Full::new(big.digests(10), vec![], vec![]), &successor, 20, 16);
        let mut done = None;
        for c in &newest {
            if let Taken::Installed(i) = rx.take_chunk(chunk_of(c)) {
                done = Some(*i);
            }
        }
        assert_eq!(done.expect("the newer snapshot completes").topology_version, 20);
        let straggler = rx.take_chunk(chunk_of(&lossy2[1]));
        assert!(matches!(straggler, Taken::Refused(rdm_gossip::snapshot::Refusal::OlderThanHeld { held_version: 20, offered_version: 19 })), "the superseded chunk is refused against the actual installed floor, never retained: {straggler:?}");
        assert_eq!(rx.held_version("mesh2"), Some((Some(successor.clone()), 20)));

        // ---- 5. the forwarding primary: stored fulls determine the delta ------------------------
        let f41 = Full::new(mesh2.digests(10), vec![], vec![]);
        let mut d43 = mesh2.digests(900);
        d43[1].status = NodeStatus::ConnectionsDeleting;
        let f43 = Full::new(d43, vec![], vec![]);
        let mut fwd = Forwarder::default();
        let forwarder = author_of(&publisher("mesh1.node-admin.1"));
        fwd.set_author(forwarder.clone());
        let first = fwd.source("mesh1.node-admin.1", "mesh2", 41, &f41, 1);
        let Forward::Delta(first_frame) = first else { panic!("the first publication of a peer mesh is a differential") };
        let (_, _, first_base, first_to, first_parts) = delta_of(&first_frame);
        assert_eq!((first_base, first_to, first_parts.added.len()), (0, 41, f41.member_count()), "from 0: every node of the peer mesh is an addition");
        // The forwarder never held 42; it holds 43. One delta from what it last published.
        let Forward::Delta(delta) = fwd.source("mesh1.node-admin.1", "mesh2", 43, &f43, 2) else { panic!("a moved source is a delta") };
        let (mesh, source, base, to, parts) = delta_of(&delta);
        assert_eq!((mesh.as_str(), base, to), ("mesh2", 41, 43), "base_version is what the forwarder last published, not the version before");
        assert_eq!(source, forwarder, "the forwarder is the differential's source");
        assert_eq!((parts.added.len(), parts.changed.len(), parts.removed.len()), (0, 1, 0), "the one member whose routing status moved, its status alone; a heartbeat alone is not a change");
        let mut rebuilt = f41.clone();
        rebuilt.apply(&parts).expect("the differential applies");
        assert!(rebuilt.same_topology(&f43), "the delta carries exactly the difference between the two fulls");
        // A heartbeat-only Mesh produces no delta; an older or equal version produces none.
        let mut beating = mesh2.digests(5_000);
        beating[1].status = NodeStatus::ConnectionsDeleting;
        assert!(matches!(fwd.source("mesh1.node-admin.1", "mesh2", 43, &Full::new(beating, vec![], vec![]), 3), Forward::Nothing(_)));
        assert!(matches!(fwd.source("mesh1.node-admin.1", "mesh2", 40, &f41, 3), Forward::Nothing(_)));
        // A version bump that changes nothing a Mesh member holds sends nothing and keeps the base.
        assert!(matches!(fwd.source("mesh1.node-admin.1", "mesh2", 44, &f43, 3), Forward::Nothing(_)));
        let mut d45 = f43.digests();
        d45[0].status = NodeStatus::Pending;
        let f45 = Full::new(d45, vec![], vec![]);
        let Forward::Delta(next) = fwd.source("mesh1.node-admin.1", "mesh2", 45, &f45, 4) else { panic!("a further move is a delta") };
        let (_, _, base, to, _) = delta_of(&next);
        assert_eq!((base, to), (43, 45), "the base stays the last version published into the Mesh");
        // A forwarder taking the role starts from what its Mesh holds (v45, from the forwarder before
        // it); the next move of mesh2 is the differential from there, whichever of mesh2's primaries
        // sent it (Q389). Nothing full is sent.
        fwd.reset();
        fwd.start_from(&[("mesh2".to_string(), 45, f45.clone())]);
        assert!(matches!(fwd.source("mesh1.node-admin.2", "mesh2", 45, &f45, 5), Forward::Nothing(_)), "the version the Mesh holds is not sent again");
        let mut d46 = f45.digests();
        d46[0].status = NodeStatus::Ready;
        let f46 = Full::new(d46, vec![], vec![]);
        let Forward::Delta(after_take) = fwd.source("mesh1.node-admin.2", "mesh2", 46, &f46, 6) else { panic!("taking the role: a differential from what the Mesh holds") };
        let (_, _, take_base, take_to, _) = delta_of(&after_take);
        assert_eq!((take_base, take_to), (45, 46));
        // A differential that does not fit one message is sent in chunks (gossip.md §Size), each within
        // one message, and the next one is from its version.
        let huge = Mesh::new("mesh2", 200);
        let Forward::Delta(chunked) = fwd.source("mesh1.node-admin.2", "mesh2", 47, &Full::new(huge.digests(1), vec![], vec![]), 7) else { panic!("200 additions go in, in chunks") };
        assert!(chunked.len() > 1 && chunked.iter().all(|c| c.encode().len() <= MAX_FRAME), "200 additions do not fit one message: {} chunk(s), each within one", chunked.len());
        let (_, _, chunked_base, chunked_to, chunked_parts) = delta_of(&chunked);
        assert_eq!((chunked_base, chunked_to, chunked_parts.added.len()), (46, 47, 200), "every addition, across the chunks");
        let mut churned = huge.digests(2);
        churned.iter_mut().for_each(|d| d.status = NodeStatus::ConnectionsDeleting);
        let Forward::Delta(statuses) = fwd.source("mesh1.node-admin.2", "mesh2", 48, &Full::new(churned.clone(), vec![], vec![]), 8) else { panic!("200 status changes go in") };
        let (_, _, statuses_base, statuses_to, statuses_parts) = delta_of(&statuses);
        assert_eq!((statuses_base, statuses_to, statuses_parts.changed.len(), statuses_parts.added.len()), (47, 48, 200, 0), "a status change carries the birth and its status only");
        churned[0].status = NodeStatus::Ready;
        let Forward::Delta(small) = fwd.source("mesh1.node-admin.2", "mesh2", 49, &Full::new(churned, vec![], vec![]), 9) else { panic!("a move that fits is sent") };
        let (_, _, small_base, small_to, _) = delta_of(&small);
        assert_eq!((small.len(), small_base, small_to), (1, 48, 49), "one status change is one chunk, from the version before");
        result.insert(
            "forwarder".into(),
            json!({ "delta_base": base, "delta_to": to, "delta_changed": parts.changed.len(), "first_publication": "differential from 0", "missed_intermediate_versions": "one delta 41 -> 43", "role_take": "differential 45 -> 46" }),
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
        let src_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh2", &mesh2_id, "mesh2.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let src_m_channels = rdm_gossip::channel::Channels::new(&src_node.gossip, &src_node.ep, &src_m).unwrap();
        src_m_channels.join(rdm_gossip::channel::ChannelId::mesh(src_m.fabric_id(), src_m.mesh_id()), vec![]).await.unwrap();
        let fwd_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1_id, "mesh1.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let fwd_m_channels = rdm_gossip::channel::Channels::new(&fwd_node.gossip, &fwd_node.ep, &fwd_m).unwrap();
        fwd_m_channels.join(rdm_gossip::channel::ChannelId::mesh(fwd_m.fabric_id(), fwd_m.mesh_id()), vec![]).await.unwrap();
        let mem_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1_id, "mesh1.node.1", rdm_transport::clock::os_clock(), Default::default());
        let mem_m_channels = rdm_gossip::channel::Channels::new(&member_node.gossip, &member_node.ep, &mem_m).unwrap();
        mem_m_channels.join(rdm_gossip::channel::ChannelId::mesh(mem_m.fabric_id(), mem_m.mesh_id()), vec![fwd_addr.clone()]).await.unwrap();
        let src_birth = IncarnationId::mint();
        src_m_channels.set_publisher(&rdm_core::PublisherId { incarnation: src_birth.clone(), node_id: rdm_core::NodeId::mint() });
        src_m_channels.join(rdm_gossip::channel::ChannelId::backbone(src_m.fabric_id()), vec![]).await.unwrap();
        let src_bb = src_m_channels.backbone().expect("the backbone is joined");
        let fwd_publisher = rdm_core::PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        fwd_m_channels.set_publisher(&fwd_publisher);
        fwd_m_channels.join(rdm_gossip::channel::ChannelId::backbone(fwd_m.fabric_id()), vec![src_addr.clone()]).await.unwrap();
        let fwd_bb = fwd_m_channels.backbone().expect("the backbone is joined");
        let _ = rdm_gossip::channel::ChannelId::backbone(&fab).topic();
        // The forwarder sends each differential once (a node that misses one tops up at the next):
        // the member is its neighbour on mesh1's channel before mesh2 is published.
        fwd_m_channels.wait_for_neighbour().await;
        mem_m_channels.wait_for_neighbour().await;
        src_bb.set_mesh_primary(true);
        fwd_bb.set_mesh_primary(true);
        // Eight nodes: the first differential of mesh2 (every node an addition) fits one message. A peer
        // mesh whose first differential does not is not sent and is read instead; chunked differentials
        // are step 4 of #338.
        let remote = Mesh::new("mesh2", 8);
        // Publish (the admin's hierarchy loop does this every gossip interval) until the
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
        assert_eq!(held_by_fwd.0.as_ref().map(|p| p.incarnation.clone()), Some(src_birth), "the author is the exact birth");
        let v1 = held_by_fwd.1;
        assert_eq!(v1, 1, "one projection, one version, however many publications");
        let at_member = {
            wait("the ordinary member to apply the first differential of mesh2", || {
                let mm = mem_m.clone();
                async move { mm.held_source_version("mesh2") }
            })
            .await
        };
        assert_eq!(at_member, (fwd_publisher.clone(), v1), "the member holds mesh2 at the version its primary holds, sent by its primary: the differential's source (Q389)");
        // Heartbeats alone: many more of them, digest_seq and emitted_at moving, no version, no delta.
        for beat in 0..12u64 {
            let mut hb = remote.digests(100 + beat);
            hb.iter_mut().for_each(|d| {
                d.emitted_at_rafka_ms = 9_000_000 + beat;
            });
            src_bb.publish(&src_m, hb).await;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        assert_eq!(fwd_bb.held_source("mesh2").unwrap().1, v1, "a heartbeat-only Mesh does not bump topology_version");
        // One remote change: one version bump, one delta, applied by the ordinary member.
        let mut changed = remote.digests(500);
        changed[7].status = NodeStatus::ConnectionsDeleting;
        let (changed_name, changed_id) = (changed[7].node.name.clone(), changed[7].node.node_id.to_string());
        src_bb.publish(&src_m, changed.clone()).await;
        let v2 = wait("the ordinary member to apply the delta", || {
            let mm = mem_m.clone();
            async move { mm.held_source_version("mesh2").filter(|(_, v)| *v > v1).map(|(_, v)| v) }
        })
        .await;
        assert_eq!(v2, v1 + 1);
        assert!(mem_m.desynced_sources().is_empty(), "the chain held: no gap, no top-up");
        // The member's view holds the changed member as the delta stated it.
        let seen = mem_m.digest_view.get(changed_id.as_str()).expect("the ordinary member holds the remote member").0;
        assert_eq!(seen.status, NodeStatus::ConnectionsDeleting, "the delta's change is applied to the member's projection");

        drop(src_bb);
        drop(fwd_bb);
        for n in [&src_node, &fwd_node, &member_node] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        let bumps_seen = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|s| attr(s, "node") == "mesh2.node-admin.1").count();
        assert_eq!(bumps_seen, 2, "the real publisher bumped twice: the first projection and the one change (12 heartbeats bumped nothing)");
        // The source primary forwards its own Mesh as a source too; the remote change is the forwarding primary's delta.
        let deltas_sent: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-forwarded-delta").into_iter().filter(|s| attr(s, "node") == "mesh1.node-admin.1").collect();
        assert_eq!(deltas_sent.len(), 2, "the first differential of mesh2 and one for the remote change: {deltas_sent:?}");
        assert_eq!((attr(deltas_sent[0], "base_version"), attr(deltas_sent[0], "topology_version")), ("0".to_string(), v1.to_string()), "the first is from 0");
        assert_eq!((attr(deltas_sent[1], "base_version"), attr(deltas_sent[1], "topology_version"), attr(deltas_sent[1], "changed")), (v1.to_string(), v2.to_string(), "1".to_string()));
        let applied = named(&spans, "rdm.mesh.topology.update.via-delta").into_iter().filter(|s| attr(s, "node") == "mesh1.node.1").collect::<Vec<_>>();
        assert_eq!(applied.len(), 2, "the ordinary member applied each once");
        let installs = named(&spans, "rdm.mesh.topology.update.via-snapshot-installed");
        let backbone_installs = installs.iter().filter(|s| attr(s, "channel") == "backbone" && attr(s, "node") == "mesh1.node-admin.1").count();
        assert!(backbone_installs >= 1, "the forwarding primary installed complete backbone snapshots");
        let chunked_install = installs.iter().find(|s| attr(s, "channel") == "backbone" && attr(s, "members") == "8").map(|s| (attr(s, "time_ms"), attr(s, "topology_version")));
        assert!(chunked_install.is_some(), "an 8-member snapshot installed whole");
        result.insert(
            "real_gossip".into(),
            json!({
                "publisher": held_by_fwd.0.as_ref().map(ToString::to_string),
                "first_version": v1,
                "version_after_one_change": v2,
                "topology_version_spans": bumps_seen,
                "heartbeat_intervals_without_a_bump": 12,
                "deltas_forwarded": deltas_sent.len(),
                "delta": { "changed_member": changed_name, "base_version": attr(deltas_sent[1], "base_version"), "topology_version": attr(deltas_sent[1], "topology_version"), "trace_id": deltas_sent[1]["trace_id"], "span_id": deltas_sent[1]["span_id"] },
                "member_applied_spans": applied.iter().map(|s| json!({ "trace_id": s["trace_id"], "span_id": s["span_id"] })).collect::<Vec<_>>(),
            }),
        );
        result.insert("cell".into(), json!(CELL));
        result.insert("spans_read".into(), json!(spans.len()));
        std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&Value::Object(result)).unwrap()).unwrap();
    });
}

/// CONTRACT (gossip.md §3.3: "The forwarder diffs the fulls it holds"; "A forwarder that becomes the
/// primary publishes a full first"; §5: the joiner "reads complete source snapshots"): a node-admin
/// that installed a peer Mesh's complete snapshot from its topology read (the fabric-primary's answer,
/// before any backbone publication of that Mesh reached it) holds that source as a full, and on taking
/// its Mesh's primary role puts it into its own Mesh: an ordinary member of the Mesh installs the peer
/// source with no backbone publication of it. Must NOT happen: the read-installed source left out of
/// the primary's first full, so the members hold a peer Mesh's primary records but none of its births
/// until that Mesh's next backbone publication (lane 4's post-land-gate at 2109cdf0c,
/// primary_spent_call_report: the member's view named no fabric-primary to report to).
#[test]
fn a_primary_puts_a_peer_source_it_installed_from_a_topology_read_into_its_mesh() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let (fwd_node, member_node) = (node().await, node().await);
        let fwd_addr = addr_of(&fwd_node.ep);
        learn_addresses(&member_node.ep, &[fwd_addr.clone()]).unwrap();
        let fab = fabric();
        let mesh1_id = MeshId::mint();
        let fwd_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1_id, "mesh1.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let fwd_m_channels = rdm_gossip::channel::Channels::new(&fwd_node.gossip, &fwd_node.ep, &fwd_m).unwrap();
        fwd_m_channels.join(rdm_gossip::channel::ChannelId::mesh(&fab, &mesh1_id), vec![]).await.unwrap();
        let mem_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1_id, "mesh1.node.1", rdm_transport::clock::os_clock(), Default::default());
        let mem_m_channels = rdm_gossip::channel::Channels::new(&member_node.gossip, &member_node.ep, &mem_m).unwrap();
        mem_m_channels.join(rdm_gossip::channel::ChannelId::mesh(&fab, &mesh1_id), vec![fwd_addr.clone()]).await.unwrap();
        let fwd_publisher = rdm_core::PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        fwd_m_channels.set_publisher(&fwd_publisher);
        fwd_m_channels.join(rdm_gossip::channel::ChannelId::backbone(&fab), vec![]).await.unwrap();
        let fwd_bb = fwd_m_channels.backbone().expect("the backbone is joined");
        // The peer Mesh's complete snapshot, as the fabric-primary's topology read answers it: no
        // backbone publication of mesh2 ever reaches this node-admin.
        let src = publisher("mesh2.node-admin.1");
        let remote = Mesh::new("mesh2", 5);
        let full = Full::new(remote.digests(1), vec![], vec![]);
        let mut taken = None;
        for f in topology_full_chunks(&full, &src, 3, 0) {
            taken = Some(fwd_m_channels.take_read_chunk(Chunk { author: None, ..chunk_of(&f) }, "mesh2"));
        }
        assert!(matches!(taken, Some(Taken::Installed(_))), "the read installs the peer source: {taken:?}");
        // The node-admin takes its Mesh's primary role and publishes its own Mesh.
        fwd_bb.set_mesh_primary(true);
        let own = Mesh::new("mesh1", 2);
        let at_member = {
            let mut n = 0u64;
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            loop {
                n += 1;
                fwd_bb.publish(&fwd_m, own.digests(n)).await;
                if let Some(held) = mem_m.held_source_version("mesh2") {
                    break held;
                }
                assert!(std::time::Instant::now() < deadline, "the ordinary member never installed the peer source its primary read: its primary put only its own Mesh into it");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
        assert_eq!(at_member, (Some(fwd_publisher), 3), "the member holds the peer source at the version the primary read it, sent by its primary: the differential's source (Q389)");
        drop(fwd_bb);
    });
}
