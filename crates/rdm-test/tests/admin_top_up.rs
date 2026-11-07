//! i143.e12.s52 (rafka-v2 #3082): a node-admin tops up a desynchronized mesh-channel source from its
//! own Mesh's primary, as every receiver does.
//!
//! The design (docs/architecture/gossip.md §3.3, "A version mismatch desynchronizes, and the
//! node tops up"): "A receiver applies a delta only when it holds exactly `base_version` of that
//! source and publisher. On a mismatch it marks that source's projection desynced, stops applying its
//! deltas, and pulls the current full fabric topology from its own current mesh-primary with
//! GetTopology, installs it atomically, records the publisher and version baseline, and resumes."
//!
//! The defect this cell pins (gate 784e5974, node_stop, rdm-node-admin.308223): mesh1.node-admin.2, a
//! non-primary node-admin, took an own-mesh delta out of order (`via-version-gap`, held 11, base 12)
//! and never topped up: only the testkit node spawned the top-up. A non-primary serves its own mesh
//! from its mesh-channel sources, and a desynchronized source is not served, so its `TopologyService`
//! answered `unknown-mesh` for mesh1 until the run ended.

use iroh::protocol::Router;
use crate::in_process_provider::{self as in_process, InProcessProvider};
use rdm_core::{IncarnationId, model::NodeStatus, MeshDigest, NodeId};
use rdm_gossip::location::{gossip_addr, learn_addresses};
use rdm_gossip::frame::Frame;
use rdm_gossip::topology::TopologyView;
use rdm_gossip::snapshot::PublisherId;
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::node_hooks::NodeHooks;
use rdm_admin::wiring::{FabricHooks, Wiring};
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

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != NodeStatus::Ready {
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

/// CONTRACT (gossip.md §3.3): a Day-0 node-admin (mesh1.node-admin.1, mesh1's primary) and mesh1.node-admin.2,
/// created by it through its `node.create` once its Day-0 Build is complete, both ready; mesh1.node-admin.2 holds mesh1 from its read and its own Mesh's primary
/// from that primary's Digest. A delta of a peer mesh, mesh9, whose base mesh1.node-admin.2 holds no copy
/// of, reaches it on mesh1's channel (`via-gap`): it marks the source desynchronized and tops up from its
/// own Mesh's primary (`rdm.mesh.entry.update.via-top-up`, served by mesh1.node-admin.1). That primary
/// holds no source of mesh9, so mesh9 alone stays desynchronized, and mesh1.node-admin.2 serves mesh1
/// under its primary's revision. mesh1.node-admin.1 hears the same delta; it is the Mesh's primary, so it
/// has no primary to top up from and says so by name (`rdm.mesh.entry.reject.via-top-up`,
/// `reason=this-node-is-the-mesh-primary`), dialing nobody. Must NOT happen: a node-admin holding a
/// desynchronized source with no top-up, or a primary dialing itself for a top-up (gate 784e5974: only the
/// testkit node spawned the top-up).
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

        // The primary: a Day-0 node-admin that creates mesh1.node-admin.2 through its node.create once its
        // Day-0 Build is complete, then reconciles nothing.
        let provider = InProcessProvider::new(crate::common::passing_start());
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), provider: provider.wiring(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        let root_digest = root.digest.lock().unwrap().clone();
        let root_id = root_digest.node.node_id.to_string();
        let root_publisher = PublisherId { incarnation: root_digest.node.incarnation.clone(), node_id: root_digest.node.node_id.clone() };

        // mesh1.node-admin.2: created by the root's node.create, a node-admin that is not its mesh's primary.
        let at = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        provider.create(&root, vec![in_process::node_spec(&at, "mesh1", "node-admin", &admin_dir)]).await;
        root.stop_reconciling().await;
        let launch = provider.launch(&at);
        let admin = provider.admin(&at).await;
        until_ready("mesh1.node-admin.2", || admin.digest.lock().unwrap().clone()).await;
        until("mesh1.node-admin.2 to hold mesh1's projection under the primary's revision", WRONG, || admin.topology_view.held_source_version("mesh1").is_some_and(|(p, _)| p == root_publisher)).await;
        assert!(!admin.publishes_its_mesh(), "mesh1.node-admin.2 is not its mesh's primary");
        // The top-up pulls from the birth the node's primary record names, through that birth's own
        // Digest (gossip.md §3.1, §3.3): a gap signalled before the node holds it is one refused
        // attempt, held for nothing, until the primary next speaks. A start whose topology read the
        // primary answered before its first publication carries no snapshot of mesh1 (a mesh with
        // no published snapshot is absent, node-rpc-envelope.md op 0x1E), so the cell waits for it.
        until("mesh1.node-admin.2 to hold its own Mesh's primary, the birth its primary record names", WRONG, || {
            admin.topology_view.mesh_primary().is_ok_and(|p| p.node.node_id == root_digest.node.node_id && p.node.incarnation == root_digest.node.incarnation)
        })
        .await;

        // The cell's own endpoint on mesh1's channel, through mesh1.node-admin.2. A neighbour coming up at the
        // primary hears the primary's full once (its neighbour replay): it is waited for, so no full of
        // the primary reaches mesh1.node-admin.2 after the delta below.
        let admin_digest = admin.digest.lock().unwrap().clone();
        let probe = gossip_node().await;
        let admin_addr = gossip_addr(&admin_digest).expect("mesh1.node-admin.2's digest names its endpoint");
        learn_addresses(&probe.ep, &[admin_addr.clone()]).unwrap();
        let mesh_id = root.topology_view.digest_view.mesh_id_of(&root_digest.node.mesh).expect("the root holds its mesh id");
        let observer_id = NodeId::mint();
        let topology_view = rdm_gossip::topology::TopologyView::new(&root_digest.fabric_id, "mesh1", &mesh_id, observer_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&probe.gossip, &probe.ep, &topology_view).expect("the cell joins mesh1's channel");
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![admin_addr]).await.expect("the cell joins mesh1's channel");
        crate::declare_gossip_probe(&channels);
        // A sync issue mesh1.node-admin.2 can meet on mesh1's channel (gossip.md §3.3, Q387: nothing of its own
        // mesh reaches it there but its nodes' own Digests and lifecycle frames): a node of mesh1 its primary
        // stopped, heard again by a Digest newer than its stop. mesh1.node-admin.2 marks its own mesh's source
        // desynchronized and pulls its primary's view.
        let (_, held) = admin.topology_view.held_source_version("mesh1").expect("mesh1.node-admin.2 holds mesh1 from its read");
        let (stopped_id, stopped_inc) = (NodeId::mint(), IncarnationId::mint());
        let stop = rdm_core::LifecycleOp { r#type: "node".into(), mesh: "mesh1".into(), build_id: "81ey2zm6h456".into(), operation: rdm_core::OperationId::new("node.stop", rdm_core::OperationSubject::Birth { node_id: stopped_id.clone(), incarnation: stopped_inc.clone() }), node_id: stopped_id.clone(), incarnation: stopped_inc.clone(), path: "mesh1.node".into(), name: "9".into(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        let stopped = rdm_gossip::status::StatusEvent::Stopped.frame(stop, held + 1).authored(&rdm_gossip::envelope::Birth::publisher(&root_digest.fabric_id, &root_publisher), 0, 1);
        channels.wait_for_neighbour().await;
        let mut heard_again = root_digest.clone();
        heard_again.node.node_id = stopped_id.clone();
        heard_again.node.incarnation = stopped_inc.clone();
        heard_again.node.r#type = "node".into();
        heard_again.node.path = "mesh1.node".into();
        heard_again.node.name = "9".into();
        heard_again.node.endpoint_id = rdm_core::EndpointId(iroh::SecretKey::generate().public().to_string());
        heard_again.digest_seq = 1;
        heard_again.status = NodeStatus::Ready;
        let (gap_base, gap_version) = (held, held + 1);
        let stopped_s = stopped_id.to_string();
        let gap = |f: &BTreeMap<String, String>| f.get("node_id") == Some(&stopped_s);
        tokio::time::timeout(WRONG, async {
            loop {
                let _ = channels.forward(&stopped).await;
                let _ = channels.forward(&Frame::digest(&heard_again.clone())).await;
                heard_again.digest_seq += 1;
                if !spans_named(&spans, "rdm.mesh.topology.update.via-stopped-birth-heard", &gap).is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("mesh1.node-admin.2 took the stopped node's newer Digest as a sync issue");

        // The top-up: mesh1.node-admin.2 reads from its own Mesh's primary.
        let topped = |f: &BTreeMap<String, String>| f.get("node").map(String::as_str) == Some(launch.node_id.as_str()) && f.get("meshes").is_some_and(|m| m.split(',').any(|m| m == "mesh1"));
        if tokio::time::timeout(TOP_UP, spans.recorded("rdm.mesh.entry.update.via-top-up", topped)).await.is_err() {
            let rejected = spans_named(&spans, "rdm.mesh.entry.reject.via-top-up", |f| f.get("node").map(String::as_str) == Some(launch.node_id.as_str()));
            panic!(
                "mesh1.node-admin.2 never topped up its desynchronized source mesh1 from its own Mesh's primary within {TOP_UP:?}; desynchronized now: {:?}; refused top-ups: {rejected:?}",
                admin.topology_view.desynced_sources()
            );
        }
        let top_up = spans_named(&spans, "rdm.mesh.entry.update.via-top-up", topped).remove(0);
        let root_at = root_digest.node.location();
        let field = |k: &str| top_up.get(k).cloned().unwrap_or_default();
        assert_eq!(
            (field("mesh_primary_path"), field("mesh_primary_name"), field("served_by_path"), field("served_by_name")),
            (root_at.path.clone(), root_at.name.clone(), root_at.path.clone(), root_at.name.clone()),
            "served by its own Mesh's primary: {top_up:?}"
        );
        assert!(top_up["reasons"].contains("mesh1:stopped-birth-heard"), "the span names the sync issue it pulled for: {top_up:?}");
        let own = admin.digest.lock().unwrap().clone();
        let served = admin.topology_view.held_topology(&own);
        assert!(served.iter().any(|s| s.mesh == "mesh1" && s.publisher == root_publisher), "mesh1.node-admin.2 serves mesh1 again under the primary's revision: {:?}", served.iter().map(|s| (&s.mesh, &s.publisher, s.topology_version)).collect::<Vec<_>>());

        // The primary decides its own mesh from its nodes' reports (Q197): a stopped node's Digest pulls
        // nothing there.
        let root_pulls = spans_named(&spans, "rdm.mesh.entry.reject.via-top-up", |f| f.get("node") == Some(&root_id) && f.get("reason").map(String::as_str) == Some("pull-refused"));
        assert!(root_pulls.is_empty(), "the primary never dials itself for a top-up: {root_pulls:?}");
        let root_topped = spans_named(&spans, "rdm.mesh.entry.update.via-top-up", |f| f.get("node") == Some(&root_id));
        assert!(root_topped.is_empty(), "the primary tops up from nobody: {root_topped:?}");

        let _ = std::fs::write(dir.join("facts.txt"), format!("held={held} gap_base={gap_base} gap_version={gap_version}\ntop_up={top_up:?}\n"));
        drop((channels, topology_view));
        probe.ep.close().await;
        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
