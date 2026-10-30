//! i143.e12.s52 (rafka-v2 #3082): a node-admin tops up a desynchronized mesh-channel source from its
//! own Mesh's primary, as every receiver does.
//!
//! The design (docs/architecture/gossip.md §3.3, "A version mismatch desynchronizes, and the
//! node tops up"): "A receiver applies a delta only when it holds exactly `base_version` of that
//! source and publisher. On a mismatch it marks that source's projection desynced, stops applying its
//! deltas, and pulls the current full fabric topology from its own current mesh-primary with
//! GetTopology, installs it atomically, records the publisher and version baseline, and resumes."
//!
//! The defect this cell pins (gate 784e5974, node_stop, rdm-node-admin.308223): mesh1.admin.2, a
//! non-primary node-admin, took an own-mesh delta out of order (`via-version-gap`, held 11, base 12)
//! and never topped up: only the testkit node spawned the top-up. A non-primary serves its own mesh
//! from its mesh-channel sources, and a desynchronized source is not served, so its topology door
//! answered `unknown-mesh` for mesh1 until the run ended.

use iroh::protocol::Router;
use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId};
use rdm_transport::topology::{gossip_addr, learn_addresses, Frame, TopologyView};
use rdm_transport::snapshot::PublisherId;
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::join::Deployed;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::collections::BTreeMap;
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);
/// A top-up is one GetTopology to the primary, signalled by the gap itself: no cadence stands
/// between the gap and the read.
const TOP_UP: Duration = Duration::from_secs(20);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
fn launch_by(root: &Running, name: &str, dir: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// What `root` verifies the first join of `launch`'s birth against.
fn deployed(launch: &Launch) -> Deployed {
    let key = rdm_test::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    }
}

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
}

/// Wait, bounded, for `what` to hold: gossip delivers on its own time.
async fn until(what: &str, bound: Duration, mut holds: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + bound;
    while !holds() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The spans named `name` whose fields `wanted` says yes to.
fn spans_named(spans: &crate::common::Spans, name: &str, wanted: impl Fn(&BTreeMap<String, String>) -> bool) -> Vec<BTreeMap<String, String>> {
    let held = spans.0.lock().unwrap();
    let mut found: Vec<(u64, BTreeMap<String, String>)> = held.iter().filter(|(_, (n, _, f))| n == name && wanted(f)).map(|(seq, (_, _, f))| (*seq, f.clone())).collect();
    found.sort_by_key(|(seq, _)| *seq);
    found.into_iter().map(|(_, f)| f).collect()
}

/// A gossip endpoint of the cell's own: it joins mesh1's channel and carries the one frame the cell
/// puts there.
struct GossipNode {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn gossip_node() -> GossipNode {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    GossipNode { ep, _router: router, gossip }
}

/// CONTRACT (gossip.md §3.3): a Day-0 node-admin (mesh1.admin.1, mesh1's primary) and mesh1.admin.2,
/// launched by it, both ready. mesh1.admin.2 holds mesh1's projection from mesh1.admin.1's publications
/// on mesh1's channel, among them the publication that carries its own Ready, and the Digest
/// of the primary its primary record names. The gap is forged only then: a top-up read answered older than
/// a full this node installed on another channel is refused (R-RX3), so a gap forged before the
/// primary's Ready publication can be topped up from a read the backbone's full has overtaken (gate
/// gates-e4s2-ca0d0046). A delta of mesh1 under
/// mesh1.admin.1's revision whose base is not the version mesh1.admin.2 holds reaches it on that channel
/// (`via-version-gap`): mesh1.admin.2 marks the source desynchronized and tops up from its own Mesh's
/// primary (`rdm.mesh.entry.update.via-top-up`, served by mesh1.admin.1). Afterwards it holds no
/// desynchronized source and its topology serves mesh1 again. mesh1.admin.1 hears the same delta with no
/// baseline of its own mesh on that channel; it is the Mesh's primary, so it has no primary to top up
/// from and says so by name (`rdm.mesh.entry.reject.via-top-up`,
/// `reason=this-node-is-the-mesh-primary`), dialing nobody. Must NOT happen: a node-admin holding a
/// desynchronized source with no top-up (its topology door answering `unknown-mesh` for its own mesh),
/// or a primary dialing itself for a top-up.
#[test]
fn a_node_admin_tops_up_a_desynchronized_source_from_its_own_mesh_primary() {
    let cell = "a_node_admin_tops_up_a_desynchronized_source_from_its_own_mesh_primary";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    let dir = cap.dir.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("admin-top-up-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();

        // The primary: a Day-0 node-admin. Its accepted Build names only itself, and a reconciling root
        // would delete the admin this cell launches in its own process: it does not reconcile.
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        let root_digest = root.digest.lock().unwrap().clone();
        let root_name = root_digest.node.name.to_string();
        let root_publisher = PublisherId { node: root_name.clone(), incarnation: root_digest.node.incarnation.clone(), node_id: root_digest.node.node_id.clone() };

        // mesh1.admin.2: launched by the root, a node-admin that is not its mesh's primary.
        let launch = launch_by(&root, "mesh1.admin.2", &admin_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_with(admin_cfg, Wiring { node_hooks: NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok))), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the launched admin comes up");
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        until("mesh1.admin.2 to hold mesh1's projection under the primary's revision", WRONG, || admin.topology_view.held_source_version("mesh1").is_some_and(|(p, _)| p == root_publisher)).await;
        assert!(!admin.publishes_its_mesh(), "mesh1.admin.2 is not its mesh's primary");
        // The top-up pulls from the birth the node's primary record names, through that birth's own
        // Digest (gossip.md §3.1, §3.3): a gap signalled before the node holds it is one refused
        // attempt, held for nothing, until the primary next speaks. A start whose topology read the
        // primary answered before its first publication carries no snapshot of mesh1 (a mesh with
        // no published snapshot is absent, node-rpc-envelope.md op 0x1E), so the cell waits for it.
        until("mesh1.admin.2 to hold its own Mesh's primary, the birth its primary record names", WRONG, || {
            admin.topology_view.mesh_primary().is_ok_and(|p| p.node.node_id == root_digest.node.node_id && p.node.incarnation == root_digest.node.incarnation)
        })
        .await;

        // The cell's own endpoint on mesh1's channel, through mesh1.admin.2. A neighbour coming up at the
        // primary hears the primary's full once (its neighbour replay): it is waited for, so no full of
        // the primary reaches mesh1.admin.2 after the delta below.
        let admin_digest = admin.digest.lock().unwrap().clone();
        let probe = gossip_node().await;
        let admin_addr = gossip_addr(&admin_digest).expect("mesh1.admin.2's digest names its endpoint");
        learn_addresses(&probe.ep, &[admin_addr.clone()]).unwrap();
        let mesh_id = root_digest.mesh_id.clone().expect("the root holds its mesh id");
        let channel = TopologyView::join(&probe.gossip, &probe.ep, &root_digest.fabric_id, "mesh1", &mesh_id, "mesh1.rpc.9", rdm_transport::clock::os_clock(), vec![admin_addr], &[]).await.expect("the cell joins mesh1's channel");
        crate::declare_gossip_probe(&channel);
        until("the cell's endpoint to hold the primary's replayed full", WRONG, || channel.held_source_version("mesh1").is_some_and(|(p, _)| p == root_publisher)).await;
        // mesh1.admin.2's own Ready moves the primary's projection, which the primary then
        // publishes on the backbone and on mesh1's channel. A top-up read older than a full installed on
        // another channel is refused, so the gap waits for mesh1.admin.2 to hold, on mesh1's channel,
        // the publication that carries its Ready: the cell's endpoint shows which version that is.
        until("the primary to publish mesh1.admin.2's Ready on mesh1's channel", WRONG, || {
            channel.projected_member_at(&admin_digest.node.endpoint_id.0).is_some_and(|d| d.status == MemberStatus::Ready)
        })
        .await;
        let (_, ready_at) = channel.held_source_version("mesh1").expect("the cell's endpoint holds mesh1");
        until("mesh1.admin.2 to hold the primary's publication of its Ready", WRONG, || {
            admin.topology_view.held_source_version("mesh1").is_some_and(|(p, v)| p == root_publisher && v >= ready_at)
        })
        .await;

        // A delta of mesh1 under the primary's revision whose base is not what mesh1.admin.2 holds.
        let (_, held) = admin.topology_view.held_source_version("mesh1").expect("mesh1.admin.2 holds mesh1");
        let (gap_base, gap_version) = (held + 5, held + 6);
        let delta = Frame::MeshTopologyDelta {
            mesh: "mesh1".into(),
            source_publisher: root_publisher.clone(),
            base_version: gap_base,
            topology_version: gap_version,
            published_at_rafka_ms: root.rafka_time.now_ms(),
            changed: vec![],
            removed: vec![],
            in_flight: vec![],
            deleted: vec![],
        };
        channel.forward(&delta).await.expect("the cell puts the delta on mesh1's channel");
        let base_s = gap_base.to_string();
        let gap = |f: &BTreeMap<String, String>| f.get("node").map(String::as_str) == Some("mesh1.admin.2") && f.get("mesh").map(String::as_str) == Some("mesh1") && f.get("base_version") == Some(&base_s) && f.get("repair_requested").map(String::as_str) == Some("true");
        tokio::time::timeout(WRONG, spans.recorded("rdm.mesh.topology.update.via-gap", gap)).await.expect("mesh1.admin.2 took the delta as a version gap");

        // The top-up: mesh1.admin.2 reads mesh1 from its own Mesh's primary and resumes the source.
        let topped = |f: &BTreeMap<String, String>| f.get("node").map(String::as_str) == Some("mesh1.admin.2") && f.get("meshes").is_some_and(|m| m.split(',').any(|m| m == "mesh1"));
        if tokio::time::timeout(TOP_UP, spans.recorded("rdm.mesh.entry.update.via-top-up", topped)).await.is_err() {
            let rejected = spans_named(&spans, "rdm.mesh.entry.reject.via-top-up", |f| f.get("node").map(String::as_str) == Some("mesh1.admin.2"));
            panic!(
                "mesh1.admin.2 never topped up its desynchronized source mesh1 from its own Mesh's primary within {TOP_UP:?}; desynchronized now: {:?}; refused top-ups: {rejected:?}",
                admin.topology_view.desynced_sources()
            );
        }
        let top_up = spans_named(&spans, "rdm.mesh.entry.update.via-top-up", topped).remove(0);
        assert_eq!((top_up["mesh_primary"].as_str(), top_up["served_by"].as_str(), top_up["still_desynced"].as_str()), (root_name.as_str(), root_name.as_str(), ""), "served by its own Mesh's primary, nothing left desynchronized: {top_up:?}");
        assert!(top_up["reasons"].contains("mesh1:"), "the span names the gap it repaired: {top_up:?}");
        assert!(admin.topology_view.desynced_sources().is_empty(), "no source stays desynchronized: {:?}", admin.topology_view.desynced_sources());
        let own = admin.digest.lock().unwrap().clone();
        let served = admin.topology_view.held_topology(&own);
        assert!(served.iter().any(|s| s.mesh == "mesh1" && s.publisher == root_publisher), "mesh1.admin.2 serves mesh1 again under the primary's revision: {:?}", served.iter().map(|s| (&s.mesh, &s.publisher, s.topology_version)).collect::<Vec<_>>());

        // The primary heard the same delta with no baseline of its own mesh on its channel: it is the
        // Mesh's primary, so it names that and dials nobody.
        let primary_says = |f: &BTreeMap<String, String>| f.get("node") == Some(&root_name) && f.get("reason").map(String::as_str) == Some("this-node-is-the-mesh-primary");
        tokio::time::timeout(WRONG, spans.recorded("rdm.mesh.entry.reject.via-top-up", primary_says)).await.expect("the primary named why it has no top-up to make");
        let root_pulls = spans_named(&spans, "rdm.mesh.entry.reject.via-top-up", |f| f.get("node") == Some(&root_name) && f.get("reason").map(String::as_str) == Some("pull-refused"));
        assert!(root_pulls.is_empty(), "the primary never dials itself for a top-up: {root_pulls:?}");
        let root_topped = spans_named(&spans, "rdm.mesh.entry.update.via-top-up", |f| f.get("node") == Some(&root_name));
        assert!(root_topped.is_empty(), "the primary tops up from nobody: {root_topped:?}");

        let _ = std::fs::write(dir.join("facts.txt"), format!("held={held} gap_base={gap_base} gap_version={gap_version}\ntop_up={top_up:?}\n"));
        drop(channel);
        probe.ep.close().await;
        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
