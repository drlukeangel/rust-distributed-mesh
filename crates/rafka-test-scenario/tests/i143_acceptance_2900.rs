//! i143.e6.s12 acceptance (rafka-v2 #2900, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2900-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it) at the test cadence (staleness
//! 3 s, gossip 500 ms).
//!
//! The estate: mesh1 (one node-admin, one rpc node) and mesh2 (one node-admin, two rpc nodes),
//! settled through a Build. An observer joins the backbone and mesh1's channel and decodes the
//! real frames.
//!
//! CONTRACT: one change in mesh2 (an rpc node held, then released) is one `MembersDelta` on mesh1's
//! channel, from the version mesh1's primary last published, and the ordinary member of mesh1
//! applies it at exactly that base. A member cut off from every peer while mesh2 changes hears
//! the stripped full mesh1's primary replays when it is heard again, and holds the version the
//! source reached. A delta whose base the member does not hold (the frame a lost delta leaves
//! behind, broadcast by the observer on mesh1's channel) desynchronizes that source alone: the
//! member pulls the current fabric topology from mesh1's own primary by a topology read (Node RPC op 0x1E),
//! installs it atomically at the version that primary last published into mesh1, and the next real
//! delta applies at that baseline. mesh2's primary is never asked: no topology read of mesh2 answers
//! the member. The frames the member hears omit loads while the backbone aggregate carries them.

use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use rafka_mesh_entity::{FabricId, MemberStatus, MeshDigest, MeshId};
use rafka_mesh_transport::membership::{backbone_topic, learn_addresses, mesh_topic, Frame};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CELL: &str = "mesh_member_recovers_delta_gap_from_own_primary";
const MEMBER: &str = "mesh1.rpc.1";
const HELD: &str = "mesh2.rpc.2";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2900".into(),
        subfeature: "topology-version".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2900/process").join(CELL),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn num(sp: &Value, k: &str) -> u64 {
    attr(sp, k).parse().unwrap_or(0)
}

fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn os_now_ms() -> u64 {
    now_ns() / 1_000_000
}

/// A frame the observer decoded.
#[derive(Clone)]
struct Seen {
    heard_at_ns: u64,
    channel: &'static str,
    bytes: usize,
    frame: Frame,
}

/// A member of the backbone and mesh1's channel: it records every frame it decodes and can
/// broadcast one frame on mesh1's channel.
struct Observer {
    endpoint: Endpoint,
    _router: iroh::protocol::Router,
    seen: Arc<Mutex<Vec<Seen>>>,
    mesh_sender: GossipSender,
}

impl Observer {
    async fn join(fabric: &FabricId, mesh_id: &MeshId, seeds: Vec<EndpointAddr>) -> Self {
        let transport = iroh::endpoint::QuicTransportConfig::builder().keep_alive_interval(Duration::from_secs(1)).max_idle_timeout(Some(Duration::from_secs(3).try_into().unwrap())).build();
        let endpoint = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        learn_addresses(&endpoint, &seeds).unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let router = iroh::protocol::Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let peers: Vec<iroh::EndpointId> = seeds.iter().map(|a| a.id).collect();
        let mut mesh_sender = None;
        for (channel, topic) in [("backbone", backbone_topic(fabric)), ("mesh1", mesh_topic(fabric, mesh_id))] {
            let (sender, mut receiver) = gossip.subscribe(topic, peers.clone()).await.unwrap().split();
            if channel == "mesh1" {
                mesh_sender = Some(sender.clone());
            }
            let seen = seen.clone();
            tokio::spawn(async move {
                let _keep = sender;
                while let Some(ev) = receiver.next().await {
                    if let Ok(Event::Received(m)) = ev {
                        if let Ok(frame) = Frame::decode(&Bytes::copy_from_slice(&m.content)) {
                            seen.lock().unwrap().push(Seen { heard_at_ns: now_ns(), channel, bytes: m.content.len(), frame });
                        }
                    }
                }
            });
        }
        Self { endpoint, _router: router, seen, mesh_sender: mesh_sender.unwrap() }
    }

    fn all(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The deltas for `mesh` heard on mesh1's channel, in arrival order: (heard, base, to, changed, removed).
    fn deltas_of(&self, mesh: &str) -> Vec<(u64, u64, u64, usize, usize)> {
        self.all()
            .into_iter()
            .filter(|x| x.channel == "mesh1")
            .filter_map(|x| match &x.frame {
                Frame::MembersDelta { mesh: m, base_version, topology_version, changed, removed, .. } if m == mesh => Some((x.heard_at_ns, *base_version, *topology_version, changed.len(), removed.len())),
                _ => None,
            })
            .collect()
    }

    /// The forwarded full chunks for `mesh` on mesh1's channel.
    fn forwarded_fulls_of(&self, mesh: &str) -> Vec<Seen> {
        self.all().into_iter().filter(|x| x.channel == "mesh1" && matches!(&x.frame, Frame::Members { mesh: m, forwarded_by: Some(_), .. } if m == mesh)).collect()
    }

    async fn broadcast(&self, f: &Frame) {
        self.mesh_sender.broadcast(Bytes::from(f.encode())).await.unwrap();
    }
}

fn seeds_of(nodes: &[Value]) -> Vec<EndpointAddr> {
    nodes
        .iter()
        .filter(|n| n["kind"] == "node_admin" && n["mesh"] == "mesh1")
        .filter_map(|n| {
            let key = s(&n["endpoint_id"]).parse::<iroh::PublicKey>().ok()?;
            let addr = s(&n["transport_addr"]).parse::<std::net::SocketAddr>().ok()?;
            Some(EndpointAddr::new(key).with_ip_addr(addr))
        })
        .collect()
}

/// SIGCONT on drop: a held node is never left stopped.
struct Held {
    pid: u64,
    held: bool,
}

impl Held {
    fn signal(&mut self, sig: &str, held: bool) {
        let ok = std::process::Command::new("kill").args([sig, &self.pid.to_string()]).status().unwrap().success();
        assert!(ok, "kill {sig} {}", self.pid);
        self.held = held;
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if self.held {
            let _ = std::process::Command::new("kill").args(["-CONT", &self.pid.to_string()]).status();
        }
    }
}

fn spans_of<'a>(spans: &'a [Value], name: &str, node: &str, since: u64) -> Vec<&'a Value> {
    named(spans, name).into_iter().filter(|sp| attr(sp, "node") == node && at(sp) >= since).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_member_recovers_delta_gap_from_own_primary() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [
            {"name": "mesh1", "node_admin": 1, "rpc_node": 1},
            {"name": "mesh2", "node_admin": 1, "rpc_node": 2},
        ]}))
        .await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let nodes = estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 1, 2)], Duration::from_secs(60)).await;
    let (_, fabric_view) = estate.get("/api/fabric").await;
    let fabric = FabricId::parse(&s(&fabric_view["id"])).expect("the fabric id");
    let mesh1_id = MeshId::parse(&s(&fabric_view["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh1").expect("mesh1")["id"])).expect("the mesh1 id");

    let observer = Observer::join(&fabric, &mesh1_id, seeds_of(&nodes)).await;
    let mut hold = Held { pid: estate.pid_of(HELD).await, held: false };

    // Joined: the observer is a new neighbour of mesh1's primary, which puts the stripped full of
    // every source it has published into the Mesh.
    wait_for("the observer hears mesh2's forwarded full on mesh1's channel and the backbone aggregate", Duration::from_secs(60), || async {
        let backbone = observer.all().iter().any(|x| x.channel == "backbone" && matches!(&x.frame, Frame::Members { mesh, .. } if mesh == "mesh2"));
        (!observer.forwarded_fulls_of("mesh2").is_empty() && backbone).then_some(())
    })
    .await;
    // The first publication of a source into the Mesh is a full: no delta of mesh2 precedes it.
    let join_full = observer.forwarded_fulls_of("mesh2");
    let first_full_at = join_full.iter().map(|x| x.heard_at_ns).min().unwrap();
    assert!(observer.deltas_of("mesh2").iter().all(|d| d.0 > first_full_at), "a delta of mesh2 never precedes its full inside the Mesh");
    // A forwarded full omits loads; the backbone aggregate carries them.
    let loads_forwarded = join_full.iter().any(|x| matches!(&x.frame, Frame::Members { digests, .. } if digests.iter().any(|d| d.in_flight.is_some())));
    let loads_on_backbone = observer.all().iter().any(|x| x.channel == "backbone" && matches!(&x.frame, Frame::Members { mesh, digests, .. } if mesh == "mesh2" && digests.iter().any(|d| d.in_flight.is_some())));
    assert!(!loads_forwarded, "the forwarded cross-Mesh projection carries no loads");
    assert!(loads_on_backbone, "the backbone aggregate carries loads");
    let publisher = join_full
        .iter()
        .find_map(|x| match &x.frame {
            Frame::Members { publisher, .. } => Some(publisher.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(publisher.node, "mesh2.admin.1", "the version belongs to the source Mesh's primary");
    let stripped_full_bytes: usize = {
        // The chunks of the newest forwarded full of mesh2 (one snapshot).
        let newest = join_full.iter().filter_map(|x| match &x.frame { Frame::Members { snapshot_id, .. } => Some(*snapshot_id), _ => None }).max().unwrap();
        join_full.iter().filter(|x| matches!(&x.frame, Frame::Members { snapshot_id, .. } if *snapshot_id == newest)).map(|x| x.bytes).sum()
    };
    let backbone_full_bytes: usize = {
        let all = observer.all();
        let newest = all.iter().filter_map(|x| match &x.frame { Frame::Members { mesh, snapshot_id, forwarded_by: None, publisher: p, .. } if mesh == "mesh2" && *p == publisher && x.channel == "backbone" => Some(*snapshot_id), _ => None }).max().unwrap();
        all.iter().filter(|x| matches!(&x.frame, Frame::Members { mesh, snapshot_id, forwarded_by: None, .. } if mesh == "mesh2" && *snapshot_id == newest && x.channel == "backbone")).map(|x| x.bytes).sum()
    };
    // The member holds mesh2 before anything changes.
    let member_installed: Vec<Value> = wait_for("the member holds mesh2's projection", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let v: Vec<Value> = spans_of(&spans, "rdm.mesh.membership.update.via-snapshot-installed", MEMBER, 0).into_iter().filter(|sp| attr(sp, "mesh") == "mesh2").cloned().collect();
        (!v.is_empty()).then_some(v)
    })
    .await;
    let baseline_version = member_installed.iter().map(|sp| num(sp, "topology_version")).max().unwrap();

    // ---- healthy control: one change in mesh2, one delta, applied by the member at its base
    let deltas_before = observer.deltas_of("mesh2").len();
    let t_change_a = now_ns();
    hold.signal("-STOP", true);
    let delta_a = wait_for("the observer hears the delta for the held rpc node", Duration::from_secs(30), || async { observer.deltas_of("mesh2").get(deltas_before).copied() }).await;
    // Stay a while longer: nothing else changes, so nothing else is sent.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let after_a = observer.deltas_of("mesh2");
    assert_eq!(after_a.len(), deltas_before + 1, "one remote change is one delta: {after_a:?}");
    assert_eq!((delta_a.3, delta_a.4), (0, 1), "the held rpc node left the projection: one removal and nothing else: {delta_a:?}");
    assert_eq!(delta_a.1, baseline_version, "base_version is the version mesh1's primary last published into mesh1");
    assert_eq!(delta_a.2, delta_a.1 + 1, "one move, one version");
    let applied_a = wait_for("the member applies it at exactly its base", Duration::from_secs(30), || async {
        let spans = estate.spans();
        spans_of(&spans, "rdm.mesh.membership.update.via-delta", MEMBER, t_change_a).into_iter().find(|sp| attr(sp, "mesh") == "mesh2" && num(sp, "topology_version") == delta_a.2).cloned()
    })
    .await;
    assert_eq!(num(&applied_a, "base_version"), delta_a.1);
    let version_after_a = delta_a.2;

    // ---- the gap: a delta whose base the member does not hold
    // The newest version the primary has published into mesh1 for mesh2, from the real frames.
    let latest = observer.deltas_of("mesh2").iter().map(|d| d.2).max().unwrap();
    assert_eq!(latest, version_after_a);
    let real_digest: MeshDigest = observer
        .forwarded_fulls_of("mesh2")
        .iter()
        .find_map(|x| match &x.frame {
            Frame::Members { digests, .. } => digests.iter().find(|d| d.node.name.to_string() == "mesh2.rpc.1").cloned(),
            _ => None,
        })
        .expect("mesh2.rpc.1 is in the forwarded full");
    let mut forged = real_digest.clone();
    forged.status = MemberStatus::Draining;
    let gap_frame = Frame::MembersDelta {
        mesh: "mesh2".into(),
        source_publisher: publisher.clone(),
        base_version: latest + 1,
        topology_version: latest + 2,
        published_at_rafka_ms: os_now_ms(),
        changed: vec![forged],
        removed: vec![],
        in_flight: vec![],
        departed: vec![],
    };
    let t_gap = now_ns();
    observer.broadcast(&gap_frame).await;
    let gap = wait_for("the member names the version gap", Duration::from_secs(30), || async {
        let spans = estate.spans();
        spans_of(&spans, "rdm.mesh.membership.update.via-version-gap", MEMBER, t_gap).first().map(|sp| (*sp).clone())
    })
    .await;
    assert_eq!(attr(&gap, "mesh"), "mesh2", "only that source desynchronizes");
    assert_eq!(attr(&gap, "gap"), "version-gap");
    assert_eq!((num(&gap, "held_version"), num(&gap, "base_version")), (latest, latest + 1), "{gap}");
    let topped = wait_for("the member tops up from its own primary", Duration::from_secs(30), || async {
        let spans = estate.spans();
        spans_of(&spans, "rdm.mesh.entry.update.via-top-up", MEMBER, t_gap).into_iter().find(|sp| attr(sp, "installed").contains("mesh2@")).cloned()
    })
    .await;
    assert_eq!(attr(&topped, "mesh_primary"), "mesh1.admin.1", "the member's own mesh-primary");
    assert_eq!(attr(&topped, "served_by"), "mesh1.admin.1");
    assert!(attr(&topped, "meshes").split(',').all(|m| m == "mesh2"), "only the desynchronized source is topped up: {topped}");
    assert!(attr(&topped, "still_desynced").is_empty(), "the top-up resumed the source: {topped}");
    let installed_version: u64 = attr(&topped, "installed").split(',').find_map(|e| e.strip_prefix("mesh2@")).unwrap().parse().unwrap();
    assert_eq!(installed_version, latest, "the baseline is the version the primary last published into mesh1");

    // ---- resumed: the next real change is a delta that applies at the baseline
    let t_resume = now_ns();
    hold.signal("-CONT", false);
    let delta_c = wait_for("the observer hears the next delta", Duration::from_secs(30), || async { observer.deltas_of("mesh2").into_iter().find(|d| d.0 > t_resume) }).await;
    assert_eq!(delta_c.1, latest, "the next delta continues from the baseline the member holds");
    let applied_c = wait_for("the member applies the next delta at the baseline", Duration::from_secs(30), || async {
        let spans = estate.spans();
        spans_of(&spans, "rdm.mesh.membership.update.via-delta", MEMBER, t_resume).into_iter().find(|sp| attr(sp, "mesh") == "mesh2" && num(sp, "topology_version") == delta_c.2).cloned()
    })
    .await;
    assert_eq!(num(&applied_c, "base_version"), latest);

    // ---- a member cut off from every other process hears the full when it is heard again
    let ports_of = |names: &[&str]| udp_ports(&nodes, &names.iter().map(|n| n.to_string()).collect::<Vec<_>>());
    let mut others = ports_of(&["mesh1.admin.1", "mesh2.admin.1", "mesh2.rpc.1", HELD]);
    others.push(observer.endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port());
    let cut = Partition::start(&ports_of(&[MEMBER]), &others).unwrap_or_else(|why| panic!("RDM_REQUIRE_NETFAULT: this host cannot drop the member's traffic: {why}"));
    let cut_at = now_ns();
    hold.signal("-STOP", true);
    let delta_e = wait_for("the observer hears the delta for the held rpc node while the member is cut", Duration::from_secs(30), || async { observer.deltas_of("mesh2").into_iter().find(|d| d.0 > cut_at) }).await;
    assert_eq!(delta_e.1, delta_c.2, "the delta follows the one the member applied");
    // Past the idle timeout, so the member's connections are gone, not delayed.
    tokio::time::sleep(Duration::from_secs(5)).await;
    drop(cut);
    let healed_at = now_ns();
    let full_after_heal = wait_for("the member holds the version the source reached while it was cut", Duration::from_secs(60), || async {
        let spans = estate.spans();
        spans_of(&spans, "rdm.mesh.membership.update.via-snapshot-installed", MEMBER, healed_at).into_iter().find(|sp| attr(sp, "mesh") == "mesh2" && num(sp, "topology_version") >= delta_e.2).cloned()
    })
    .await;
    assert_eq!(attr(&full_after_heal, "new_epoch"), "false", "the same source publisher: the member resumes inside its epoch");
    assert_eq!(attr(&full_after_heal, "channel"), "mesh-channel");
    let version_now = num(&full_after_heal, "topology_version");
    // Heard again after the cut, the member needed no top-up and saw no gap: mesh1's primary said
    // what it holds when the member came up as its neighbour.
    let spans = estate.spans();
    assert!(spans_of(&spans, "rdm.mesh.membership.update.via-version-gap", MEMBER, cut_at).is_empty(), "the full the primary replays on a heal repaired the member: no gap");
    let member_peer = nodes.iter().find(|n| n["name"] == MEMBER).map(|n| s(&n["endpoint_id"])).unwrap();
    let replays: Vec<&Value> = spans_of(&spans, "rdm.mesh.membership.update.via-neighbour-replay", "mesh1.admin.1", healed_at).into_iter().filter(|sp| attr(sp, "channel") == "mesh:mesh1" && member_peer.starts_with(&attr(sp, "peer"))).collect();
    assert!(replays.iter().any(|sp| num(sp, "full_chunks") >= 1), "mesh1's primary replayed its forwarded fulls to the member when it came back as a neighbour: {replays:?}");
    hold.signal("-CONT", false);

    let t_stop = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    // Nothing the injected delta claimed was applied: the member applied no delta that changed a
    // member to Draining, and the gap was the only one.
    let gaps = spans_of(&spans, "rdm.mesh.membership.update.via-version-gap", MEMBER, t_gap);
    assert_eq!(gaps.len(), 1, "exactly one gap after the injection, the injected one: {gaps:?}");
    // The join was answered by mesh1's primary; the top-up is a topology read (op 0x1E) of the
    // desynchronized mesh from the same primary and from no other: mesh2's admin served no read
    // after the gap, and the member installed mesh2 at the version mesh1's primary last published.
    let joins: Vec<&Value> = named(&spans, "rdm.mesh.entry.serve.via-pull").into_iter().filter(|sp| attr(sp, "node") == MEMBER).collect();
    assert_eq!(joins.len(), 1, "the member joined once and re-read nothing through the join: {joins:?}");
    assert_eq!(attr(joins[0], "served_by"), "mesh1.admin.1");
    let reads_after_gap = |who: &str| -> Vec<&Value> { named(&spans, "rdm.mesh.topology.serve.via-read").into_iter().filter(|sp| attr(sp, "node") == who && at(sp) >= t_gap).collect() };
    let by_mesh1 = reads_after_gap("mesh1.admin.1");
    assert_eq!(by_mesh1.len(), 1, "one top-up, one topology read after the gap: {by_mesh1:?}");
    assert_eq!((attr(by_mesh1[0], "requested"), attr(by_mesh1[0], "snapshots")), ("mesh2".to_string(), "1".to_string()));
    assert!(reads_after_gap("mesh2.admin.1").is_empty(), "the remote Mesh's primary is never queried");
    let topup_install = named(&spans, "rdm.mesh.topology.update.via-read-install").into_iter().find(|sp| attr(sp, "node") == MEMBER && at(sp) >= t_gap).expect("the member installed the top-up by a topology read");
    assert_eq!((attr(topup_install, "mesh"), num(topup_install, "topology_version")), ("mesh2".to_string(), latest));
    // The primary's evidence: the full it put in on a join, and its bytes.
    let admin_fulls: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-forwarded-full").into_iter().filter(|sp| attr(sp, "node") == "mesh1.admin.1").collect();
    assert!(!admin_fulls.is_empty(), "mesh1's primary put forwarded fulls into its Mesh");
    let deltas_forwarded: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-forwarded-delta").into_iter().filter(|sp| attr(sp, "node") == "mesh1.admin.1" && attr(sp, "mesh") == "mesh2" && at(sp) >= t_change_a && at(sp) < t_stop).collect();
    assert_eq!(deltas_forwarded.len(), 3, "three real changes in mesh2 (held, released, held again): three deltas from the forwarding primary");
    let bumps = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| attr(sp, "node") == "mesh2.admin.1").count();

    let result = json!({
        "cell": CELL,
        "publisher": publisher.to_string(),
        "baseline_version_at_member": baseline_version,
        "healthy_control": { "delta": { "base": delta_a.1, "to": delta_a.2, "changed": delta_a.3, "removed": delta_a.4 }, "deltas_heard_in_window": after_a.len() - deltas_before, "member_applied_span": { "trace_id": applied_a["trace_id"], "span_id": applied_a["span_id"] } },
        "cut": { "changed_during_cut": { "base": delta_e.1, "to": delta_e.2 }, "cut_ms": (healed_at - cut_at) / 1_000_000, "replays": replays.iter().map(|sp| json!({ "frames": attr(sp, "frames"), "full_chunks": attr(sp, "full_chunks"), "bytes": attr(sp, "bytes") })).collect::<Vec<_>>(), "member_full_span": { "trace_id": full_after_heal["trace_id"], "span_id": full_after_heal["span_id"], "topology_version": version_now } },
        "gap": {
            "injected": { "base": latest + 1, "to": latest + 2, "source_publisher": publisher.to_string() },
            "member_gap_span": { "trace_id": gap["trace_id"], "span_id": gap["span_id"], "held_version": num(&gap, "held_version") },
            "top_up_span": { "trace_id": topped["trace_id"], "span_id": topped["span_id"], "served_by": attr(&topped, "served_by"), "installed": attr(&topped, "installed") },
            "resumed_delta": { "base": delta_c.1, "to": delta_c.2, "member_applied_span": { "trace_id": applied_c["trace_id"], "span_id": applied_c["span_id"] } },
        },
        "join_serves_for_member": joins.len(),
        "topology_reads_after_gap": { "mesh1.admin.1": by_mesh1.len(), "mesh2.admin.1": reads_after_gap("mesh2.admin.1").len() },
        "top_up_install_span": { "trace_id": topup_install["trace_id"], "span_id": topup_install["span_id"], "topology_version": num(topup_install, "topology_version") },
        "forwarded_deltas_by_mesh1_primary": deltas_forwarded.len(),
        "topology_version_bumps_by_mesh2_primary": bumps,
        "measured_bytes": { "stripped_full_of_mesh2_on_mesh1_channel": stripped_full_bytes, "backbone_aggregate_of_mesh2_with_loads": backbone_full_bytes, "mesh2_members": 3, "forwarded_full_spans": admin_fulls.iter().map(|sp| json!({ "reason": attr(sp, "reason"), "chunks": attr(sp, "chunks"), "bytes": attr(sp, "bytes") })).collect::<Vec<_>>() },
        "loads_on_forwarded_full": loads_forwarded,
        "loads_on_backbone_aggregate": loads_on_backbone,
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    observer.endpoint.close().await;
}
