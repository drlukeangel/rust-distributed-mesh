//! i143 R-T6 (Luke): topology is a Node RPC read, `GetTopology` (op `0x1E`), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-rt6-process`, which exports `I143_ACCEPTANCE_DIR` (each
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it).
//!
//! The estate: mesh1 (one node-admin, one rpc node) and mesh2 (one node-admin, two rpc nodes),
//! settled through a Build; every birth joined its maker with `JoinNode` and read the maker's
//! topology with `GetTopology`.

use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const MEMBER: &str = "mesh1.rpc.1";

fn owner(cell: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-rt6".into(),
        subfeature: "topology-read".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: cell.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rt6/process").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

async fn estate(cell: &str) -> (Estate, Vec<Value>) {
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [
            {"name": "mesh1", "node_admin": 1, "rpc_node": 1},
            {"name": "mesh2", "node_admin": 1, "rpc_node": 2},
        ]}))
        .await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let nodes = estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 1, 2)], Duration::from_secs(60)).await;
    (estate, nodes)
}

fn node_id(nodes: &[Value], name: &str) -> String {
    nodes.iter().find(|n| n["name"] == name).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("{name} is not in the view"))
}

/// The members of `mesh` across every chunk of its one snapshot, with the snapshot's identity.
fn snapshot_of(frames: &[Value], mesh: &str) -> (Vec<String>, String, u64, usize) {
    let chunks: Vec<&Value> = frames.iter().filter(|f| f["frame"] == "snapshot" && f["mesh"] == mesh).collect();
    assert!(!chunks.is_empty(), "no snapshot of {mesh} in {frames:?}");
    let count = chunks[0]["chunk_count"].as_u64().unwrap() as usize;
    assert_eq!(chunks.len(), count, "every chunk of the snapshot of {mesh} arrived");
    let ids: std::collections::BTreeSet<u64> = chunks.iter().map(|c| c["snapshot_id"].as_u64().unwrap()).collect();
    assert_eq!(ids.len(), 1, "one snapshot per mesh");
    let mut members: Vec<String> = chunks.iter().flat_map(|c| c["members"].as_array().unwrap().iter().map(s)).collect();
    members.sort();
    (members, s(&chunks[0]["publisher_node"]), chunks[0]["topology_version"].as_u64().unwrap(), count)
}

fn kinds(frames: &[Value]) -> Vec<String> {
    frames.iter().map(|f| s(&f["frame"])).collect()
}

/// CONTRACT: an ordinary member (an rpc node, neither a primary nor a maker) answers
/// `GetTopology` with the fabric topology it holds: its own mesh and the other mesh, each at its
/// publisher and `topology_version`, as `Started`, one snapshot per mesh, `End`. A read naming the
/// version it holds is `Unchanged` and sends no snapshot; a caller one version behind is sent the
/// snapshot; a mesh it does not hold is refused as `UnknownMesh` and nothing else is sent. The read
/// changes nothing at the member: the member's version of each mesh after the reads is the one
/// before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_member_answers_get_topology_with_the_fabric_topology_it_holds() {
    let cell = "ordinary_member_answers_get_topology_with_the_fabric_topology_it_holds";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut estate, nodes) = estate(cell).await;
    let target = format!("exact:{}", node_id(&nodes, MEMBER));
    let t0 = rafka_test_scenario_now();

    let all = estate.probe(&["topology", "--target", &target]);
    assert_eq!(all["outcome"], "Reply", "{all}");
    let frames = all["frames"].as_array().unwrap().clone();
    let k = kinds(&frames);
    assert_eq!(k.first().map(String::as_str), Some("started"), "{k:?}");
    assert_eq!(frames.last().unwrap(), &json!({"frame": "end", "meshes": 2}), "{frames:?}");
    assert!(k[1..k.len() - 1].iter().all(|f| f == "snapshot"), "{k:?}");
    let (mesh1, mesh1_pub, mesh1_v, _) = snapshot_of(&frames, "mesh1");
    let (mesh2, mesh2_pub, mesh2_v, _) = snapshot_of(&frames, "mesh2");
    assert_eq!(mesh1, vec!["mesh1.admin.1", "mesh1.rpc.1"], "its own mesh: the members its book holds, itself among them");
    assert_eq!(mesh2, vec!["mesh2.admin.1", "mesh2.rpc.1", "mesh2.rpc.2"]);
    assert_eq!(mesh1_pub, "mesh1.admin.1", "an own mesh's version belongs to its primary");
    assert_eq!(mesh2_pub, "mesh2.admin.1", "a source mesh's version belongs to its primary");
    assert!(mesh1_v >= 1 && mesh2_v >= 1);
    let incarnation = s(&frames.iter().find(|f| f["frame"] == "snapshot" && f["mesh"] == "mesh2").unwrap()["publisher_incarnation"]);

    // The version it holds: Unchanged, no snapshot.
    let since = format!("{mesh2_pub}|{incarnation}|{mesh2_v}");
    let held = estate.probe(&["topology", "--target", &target, "--mesh", "mesh2", "--since", &since]);
    let held_frames = held["frames"].as_array().unwrap().clone();
    assert_eq!(kinds(&held_frames), vec!["started", "unchanged", "end"], "{held_frames:?}");
    assert_eq!(held_frames[1]["topology_version"], mesh2_v);
    assert_eq!(held_frames[2]["meshes"], 1, "End counts Unchanged");
    // One version behind: the snapshot.
    let behind = format!("{mesh2_pub}|{incarnation}|{}", mesh2_v - 1);
    let b = estate.probe(&["topology", "--target", &target, "--mesh", "mesh2", "--since", &behind]);
    assert!(kinds(b["frames"].as_array().unwrap()).contains(&"snapshot".to_string()), "{b}");
    // A mesh it does not hold.
    let unknown = estate.probe(&["topology", "--target", &target, "--mesh", "mesh9"]);
    assert_eq!(kinds(unknown["frames"].as_array().unwrap()), vec!["unknown-mesh"], "{unknown}");
    // Read-only: the same versions after.
    let after = estate.probe(&["topology", "--target", &target]);
    let after_frames = after["frames"].as_array().unwrap().clone();
    assert_eq!((snapshot_of(&after_frames, "mesh1").2, snapshot_of(&after_frames, "mesh2").2), (mesh1_v, mesh2_v), "reading changed no version at the member");

    estate.stop().await;
    let spans = estate.spans();
    let served: Vec<&Value> = named(&spans, "rdm.mesh.topology.serve.via-read").into_iter().filter(|sp| attr(sp, "node") == MEMBER && at(sp) >= t0).collect();
    let full = served.iter().find(|sp| attr(sp, "requested") == "*" && attr(sp, "snapshots") == "2").unwrap_or_else(|| panic!("{served:?}"));
    assert_eq!((attr(full, "meshes"), attr(full, "unchanged")), ("2".to_string(), "0".to_string()));
    assert!(attr(full, "bytes").parse::<u64>().unwrap() > 0);
    let unchanged = served.iter().find(|sp| attr(sp, "unchanged") == "1").expect("the Unchanged read is a serve span of its own");
    assert_eq!(attr(unchanged, "snapshots"), "0");
    // Caller and server are one trace: the probe's call span (op 30) is the ancestor of the member's serve.
    let call = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| attr(sp, "op") == "30").find(|c| descends_from(&spans, full, c)).expect("the member's serve descends from a caller span of op 0x1E");
    let stream_serve = named(&spans, "rdm.node_rpc.stream.serve.via-direct").into_iter().find(|sp| attr(sp, "op") == "30" && descends_from(&spans, full, sp)).expect("the serve span is a child of the runtime's stream serve");
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&json!({
            "cell": cell,
            "mesh1": { "members": mesh1, "publisher": mesh1_pub, "topology_version": mesh1_v },
            "mesh2": { "members": mesh2, "publisher": mesh2_pub, "topology_version": mesh2_v },
            "unchanged_frames": held_frames,
            "serve_span": { "trace_id": full["trace_id"], "span_id": full["span_id"], "bytes": attr(full, "bytes") },
            "caller_span": { "trace_id": call["trace_id"], "span_id": call["span_id"] },
            "stream_serve_span": { "trace_id": stream_serve["trace_id"], "span_id": stream_serve["span_id"] },
            "spans_read": spans.len(),
        }))
        .unwrap(),
    )
    .unwrap();
}

fn rafka_test_scenario_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// CONTRACT: a node born into the fabric makes `JoinNode` its first call to its maker and then
/// reads the maker's topology with `GetTopology`: the maker's join span (`via-join`, installed)
/// precedes the maker's topology serve span, which precedes the node's own
/// `via-read-install` span, one per mesh it installed. The join carries no topology: the maker
/// records no join as `member`: a join of an already-held member is not a re-read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn born_node_joins_its_maker_then_reads_the_makers_topology_and_installs_it() {
    let cell = "born_node_joins_its_maker_then_reads_the_makers_topology_and_installs_it";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut estate, _nodes) = estate(cell).await;
    estate.stop().await;
    let spans = estate.spans();
    let mut births = Vec::new();
    for node in ["mesh1.rpc.1", "mesh2.rpc.1", "mesh2.rpc.2", "mesh2.admin.1"] {
        let joined = named(&spans, "rdm.mesh.entry.update.via-membership-pulled").into_iter().find(|sp| attr(sp, "node") == node).unwrap_or_else(|| panic!("{node} never completed a JoinNode"));
        let maker = attr(joined, "served_by");
        let join_served: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "served_by") == maker).collect();
        let join_span = join_served.iter().find(|sp| attr(sp, "outcome") == "installed").unwrap_or_else(|| panic!("{maker} installed no join of {node}: {join_served:?}"));
        let installs: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-read-install").into_iter().filter(|sp| attr(sp, "node") == node).collect();
        assert!(!installs.is_empty(), "{node} installed no mesh from a topology read");
        let first_install = installs.iter().min_by_key(|sp| at(sp)).unwrap();
        let serve = named(&spans, "rdm.mesh.topology.serve.via-read")
            .into_iter()
            .filter(|sp| attr(sp, "node") == maker && at(sp) >= at(join_span) && at(sp) <= at(first_install))
            .max_by_key(|sp| at(sp))
            .unwrap_or_else(|| panic!("{maker} served no topology read between the join of {node} and its first install"));
        assert!(at(join_span) < at(serve) && at(serve) < at(first_install), "join, then serve, then install");
        assert!(installs.iter().all(|sp| attr(sp, "publisher").contains('@') && attr(sp, "topology_version").parse::<u64>().unwrap() >= 1 && !attr(sp, "mesh").is_empty()), "{installs:?}");
        births.push(json!({
            "node": node,
            "maker": maker,
            "join_span": { "trace_id": join_span["trace_id"], "span_id": join_span["span_id"] },
            "serve_span": { "trace_id": serve["trace_id"], "span_id": serve["span_id"], "meshes": attr(serve, "meshes"), "snapshots": attr(serve, "snapshots") },
            "installed": installs.iter().map(|sp| json!({ "mesh": attr(sp, "mesh"), "publisher": attr(sp, "publisher"), "topology_version": attr(sp, "topology_version"), "members": attr(sp, "members") })).collect::<Vec<_>>(),
        }));
    }
    assert!(named(&spans, "rdm.node_admin.node.update.via-join").iter().all(|sp| attr(sp, "outcome") != "member"), "no join is answered for an already-held member");
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&json!({ "cell": cell, "births": births, "spans_read": spans.len() })).unwrap()).unwrap();
}
