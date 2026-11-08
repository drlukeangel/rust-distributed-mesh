//! i143 R-G2 acceptance (Luke 2026-10-08), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-rg2-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! and whose command sets `RDM_ARTIFACTS_DIR` at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate: two meshes of two node-admins and two nodes each, settled through a Build.
//!
//! CONTRACT: forwarded topology carries topology, never liveness. An ordinary node (a node)
//! holds a peer mesh's members as topology: across 60 s with nothing changing, and across a cut of
//! every UDP path between the meshes, it never marks a peer mesh silent. A node-admin judges a
//! peer mesh from the backbone alone: it marks the mesh silent in the cut and learns it again
//! after the heal.


use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CELL: &str = "ordinary_nodes_hold_peer_mesh_topology_through_quiet_and_cut";

fn owner() -> Owner {
    owner_of(CELL)
}

fn owner_of(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-rg2".into(),
        subfeature: "forwarded-topology".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// A complete full of a peer mesh a node wrote to its topology view: its base, its version and the
/// delta it wrote (gossip.md §3.3, Q409).
const FULL_WRITTEN: &str = "rdm.mesh.topology.update.via-full";
const DELTA_FORWARDED: &str = "rdm.mesh.topology.update.via-forwarded-delta";
const DELTA_APPLIED: &str = "rdm.mesh.topology.update.via-delta";

fn num(sp: &Value, k: &str) -> u64 {
    attr(sp, k).parse().unwrap_or(0)
}

/// `(base_version, topology_version, added, changed, removed)` of a full's write or a delta.
fn moved(sp: &Value) -> (u64, u64, u64, u64, u64) {
    (num(sp, "base_version"), num(sp, "topology_version"), num(sp, "added"), num(sp, "changed"), num(sp, "removed"))
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rg2/chaos-process").join(CELL),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

/// Who marked which mesh silent after `since`.
fn silent_since(spans: &[Value], since: u64) -> Vec<(String, String)> {
    named(spans, "rdm.mesh.topology.update.via-mesh-silent").into_iter().filter(|sp| at(sp) >= since).map(|sp| (attr(sp, "node"), attr(sp, "mesh"))).collect()
}

/// CONTRACT: see the module docs. No node marks any mesh silent in 60 s of nothing changing
/// nor in a cut that outlasts the forwarded staleness floor; every node-admin marks the peer mesh
/// silent in the cut and learns it again after the heal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_nodes_hold_peer_mesh_topology_through_quiet_and_cut() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    estate.resize_shape("fabric1", &shape).await;
    let views = estate.settled_views(&shape).await;
    let nodes: Vec<Value> = views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect();
    let ordinary: BTreeSet<String> = views.iter().filter(|n| n.r#type == "node").map(|n| n.node_id.to_string()).collect();
    let admins: BTreeSet<String> = views.iter().filter(|n| n.r#type == "node-admin").map(|n| n.node_id.to_string()).collect();
    assert_eq!((ordinary.len(), admins.len()), (4, 4), "{nodes:#?}");
    let side = |m: &str| -> Vec<rdm_core::PathName> { views.iter().filter(|n| n.mesh() == m).map(|n| n.location()).collect() };
    let (mesh1, mesh2) = (side("mesh1"), side("mesh2"));

    // Quiet: every node holds both meshes, then 60 s pass with nothing changing.
    let peers = estate.peers().await;
    for n in &views {
        for m in ["mesh1", "mesh2"] {
            rdm_test::typed::peer_holds(&*peers, n, m, &format!("node {} has learned {m}", n.node_id), |held| !held.members.is_empty()).await;
        }
    }
    let quiet_from = now_ns();
    tokio::time::sleep(Duration::from_secs(60)).await;
    let quiet = silent_since(&estate.spans(), quiet_from);
    assert!(quiet.is_empty(), "no node marks any mesh silent in 60 s of nothing changing: {quiet:?}");

    // Cut: every UDP path between the meshes, held past the forwarded staleness floor.
    let cut = Partition::start(&udp_ports(&nodes, &mesh1), &udp_ports(&nodes, &mesh2)).unwrap_or_else(|why| panic!("RDM_REQUIRE_NETFAULT: {why}"));
    let cut_at = now_ns();
    let admin_silent = wait_for("every node-admin marks the peer mesh silent", Duration::from_secs(60), || async {
        let silent = silent_since(&estate.spans(), cut_at);
        admins.iter().all(|a| silent.iter().any(|(n, _)| n == a)).then_some(silent)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let during_cut = silent_since(&estate.spans(), cut_at);
    let ordinary_during: Vec<&(String, String)> = during_cut.iter().filter(|(n, _)| ordinary.contains(n)).collect();
    assert!(ordinary_during.is_empty(), "an ordinary node holds the peer mesh as topology through the cut: {ordinary_during:?}");
    drop(cut);
    let healed_at = now_ns();
    // The node-admins after the heal, by node id: the cut leaves mesh2 with no member the fabric-primary
    // hears, and a node-admin the fabric-primary reaches by no call is re-created at its path and name, a
    // new node (node-recovery.md step 2, Q335; on the process provider its old runtime
    // is terminated). Each node-admin at the four locations learns the peer mesh again.
    wait_for("every node-admin learns the peer mesh again", Duration::from_secs(60), || async {
        let now: BTreeSet<String> = estate.node_views().await.iter().filter(|n| n.r#type == "node-admin").map(|n| n.node_id.to_string()).collect();
        let spans = estate.spans();
        let learned: BTreeSet<String> = named(&spans, "rdm.mesh.topology.update.via-mesh-learned").into_iter().filter(|sp| at(sp) >= healed_at).map(|sp| attr(sp, "node")).collect();
        (now.len() == admins.len() && now.iter().all(|a| learned.contains(a))).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let after = silent_since(&estate.spans(), healed_at);
    let ordinary_after: Vec<&(String, String)> = after.iter().filter(|(n, _)| ordinary.contains(n)).collect();
    assert!(ordinary_after.is_empty(), "an ordinary node never marks a mesh silent after the cut: {ordinary_after:?}");

    let result = json!({
        "cell": CELL, "quiet_from_unix_nano": quiet_from, "quiet_silent": quiet.len(), "cut_at_unix_nano": cut_at, "healed_at_unix_nano": healed_at,
        "admins_silent_in_cut": admin_silent.iter().map(|(n, m)| json!({"node": n, "mesh": m})).collect::<Vec<_>>(),
        "ordinary_nodes": ordinary, "ordinary_silent_in_cut": ordinary_during.len(), "ordinary_silent_after_heal": ordinary_after.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    estate.enter_fabric_primary().await;
    estate.stop().await;
}

/// CONTRACT (gossip.md §3.3 "A node-admin holds one copy of each peer Mesh and writes only the delta",
/// Q409, Q410): one change of mesh2, a node added by a Build on a running MM fabric, reaches mesh1's
/// ordinary node as deltas only. Each complete full of mesh2 that mesh1's mesh-primary takes from the
/// backbone after the change writes its delta to that node-admin's topology view (`via-full`: the
/// version its copy held before, the full's version, and what it added, changed and removed); the
/// differential it puts into mesh1 (`via-forwarded-delta`) is that same delta from that same base; and
/// mesh1's node applies each at exactly its base (`via-delta`). The node then holds the added node,
/// never desynchronizes mesh2 and never reads it. Must NOT happen: a forwarded differential whose base or
/// counts are not the copy's write; a gap or a topology read of mesh2 at the ordinary node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_mesh_change_reaches_an_ordinary_node_as_the_delta_its_mesh_primary_wrote() {
    const TEST: &str = "a_peer_mesh_change_reaches_an_ordinary_node_as_the_delta_its_mesh_primary_wrote";
    let mut estate = Estate::bootstrap(owner_of(TEST), "fabric1", "mesh1").await;
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    estate.resize_shape("fabric1", &shape).await;
    let views = estate.settled_views(&shape).await;
    let at = |views: &[rdm_client::NodeView], path: &str, name: &str| views.iter().find(|n| n.location() == Estate::path(path, name)).cloned().unwrap_or_else(|| panic!("{path}.{name} in the settled views: {views:?}"));
    let (primary, member) = (at(&views, "mesh1.node-admin", "1"), at(&views, "mesh1.node", "1"));
    let (primary_id, member_id) = (primary.node_id.to_string(), member.node_id.to_string());
    let peers = estate.peers().await;
    peers.fact(&member, "mesh2", "mesh1.node.1 holds mesh2's node", |held| held.members.iter().any(|b| b.node.path == "mesh2.node")).await;

    // The change: mesh2 grows by one node.
    let changed_at = now_ns();
    let grown = [("mesh1", 1, 1), ("mesh2", 1, 2)];
    estate.resize_shape("fabric1", &grown).await;
    let after = estate.settled_views(&grown).await;
    let added = at(&after, "mesh2.node", "2");
    peers.fact(&member, "mesh2", "mesh1.node.1 holds mesh2.node.2 ready", |held| held.birth(&added.node_id).is_some_and(|b| b.status == rdm_core::model::NodeStatus::Ready)).await;
    // The last differential mesh1's primary put into mesh1 is applied by the member before the spans are read.
    let spans = wait_for("mesh1.node.1 applied every differential of mesh2 its mesh-primary sent since the change", Duration::from_secs(30), || async {
        let spans = estate.spans();
        let sent: BTreeSet<u64> = named(&spans, DELTA_FORWARDED).into_iter().filter(|sp| attr(sp, "node") == primary_id && attr(sp, "mesh") == "mesh2" && at_ns(sp) >= changed_at).map(|sp| num(sp, "topology_version")).collect();
        let applied: BTreeSet<u64> = named(&spans, DELTA_APPLIED).into_iter().filter(|sp| attr(sp, "node") == member_id && attr(sp, "mesh") == "mesh2" && at_ns(sp) >= changed_at).map(|sp| num(sp, "topology_version")).collect();
        (!sent.is_empty() && sent.is_subset(&applied)).then_some(spans)
    })
    .await;
    let of = |name: &str, node: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "mesh") == "mesh2" && at_ns(sp) >= changed_at).cloned().collect() };
    let written: Vec<Value> = of(FULL_WRITTEN, &primary_id).into_iter().filter(|sp| attr(sp, "channel") == "backbone").collect();
    let (forwarded, applied) = (of(DELTA_FORWARDED, &primary_id), of(DELTA_APPLIED, &member_id));
    for (what, sp) in written.iter().map(|sp| ("full written", sp)).chain(forwarded.iter().map(|sp| ("delta forwarded", sp))).chain(applied.iter().map(|sp| ("delta applied", sp))) {
        println!("{TEST}: {what} {} {:?} trace {} span {}", s(&sp["name"]), moved(sp), s(&sp["trace_id"]), s(&sp["span_id"]));
    }
    assert!(!forwarded.is_empty(), "mesh1's mesh-primary forwarded mesh2's change");
    for d in &forwarded {
        let (base, to, ..) = moved(d);
        let full = written.iter().find(|w| num(w, "topology_version") == to).unwrap_or_else(|| panic!("the differential {base} -> {to} is a full mesh1's mesh-primary wrote from the backbone: {written:#?}"));
        assert_eq!(moved(full), moved(d), "the differential put into mesh1 is the delta the full wrote to the node-admin's copy, from the version that copy held");
        assert!(applied.iter().any(|a| (num(a, "base_version"), num(a, "topology_version")) == (base, to)), "mesh1.node.1 applies {base} -> {to} at its base: {applied:#?}");
    }
    let gaps: Vec<Value> = of("rdm.mesh.topology.update.via-gap", &member_id);
    assert!(gaps.is_empty(), "mesh1.node.1 never desynchronizes mesh2: {gaps:#?}");
    let reads: Vec<Value> = named(&spans, "rdm.mesh.topology.update.via-read").into_iter().filter(|sp| attr(sp, "node") == member_id && attr(sp, "mesh") == "mesh2" && at_ns(sp) >= changed_at).cloned().collect();
    assert!(reads.is_empty(), "mesh1.node.1 never reads mesh2: the change reaches it as deltas: {reads:#?}");
    estate.enter_fabric_primary().await;
    estate.stop().await;
}

fn at_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
