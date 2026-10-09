//! Node-admin restart: product=mesh, feature=node-lifecycle, subfeature=admin-restart, rung=MM.
//! Spec: rafka-v2 `docs/architecture/fabric-node-lifecycle-events-ops-gossip.md`, Master Event
//! Matrix (restart: executing Mesh admin = owning mesh-admin; dissemination = own Mesh channel,
//! then the backbone aggregate), §13, §17 "Restart Evidence".

use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "admin-restart".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_peer_mesh_admin_restart_is_published_by_its_own_mesh_primary_and_reaches_the_other_mesh_through_the_aggregate".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn mesh(name: &str) -> Value {
    json!({"name": name, "node_admin": 2, "rpc_node": 2})
}

/// Every span the process `pid` wrote.
fn spans_of(estate: &Estate, pid: u64) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".spans.jsonl") && name.contains(&format!(".{pid}-")) {
            out.extend(std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    out
}

/// CONTRACT: a node-admin of a mesh that does not hold the fabric seat is restarted through the
/// fabric's REST door. The restart is executed by that mesh's own primary admin, never by the
/// fabric primary: the primary of the target's mesh is the only process that publishes
/// `NodeRestarting` for it (on its own mesh channel). The fabric primary's mesh hears the same
/// operation through the target mesh's aggregate on the backbone, and its members hear it from
/// their primary's forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_mesh_admin_restart_is_published_by_its_own_mesh_primary_and_reaches_the_other_mesh_through_the_aggregate() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let names: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")]).collect();
    let before = estate.settled(&names, Duration::from_secs(60)).await;
    // The target's mesh is the one that does not hold the fabric seat.
    let fabric_primary = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let (home, away) = if fabric_primary.starts_with("mesh1.") { ("mesh1", "mesh2") } else { ("mesh2", "mesh1") };
    let primary_of = |m: &str| before.iter().find(|n| n["mesh"] == m && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("a mesh primary");
    let (away_primary, home_primary) = (primary_of(away), primary_of(home));
    let target = before.iter().find(|n| n["mesh"] == away && n["kind"] == "node_admin" && s(&n["name"]) != away_primary).map(|n| s(&n["name"])).expect("a non-primary admin");
    assert_ne!(away_primary, fabric_primary, "the target's mesh primary is not the fabric primary");

    let mut pids = std::collections::BTreeMap::new();
    for n in &names {
        let pid = match estate.bootstrap_pid() {
            Some(p) if n == "mesh1.admin.1" => u64::from(p),
            _ => estate.pid_of(n).await,
        };
        pids.insert(n.clone(), pid);
    }
    let (status, restart) = estate.post(&format!("/api/nodes/{target}/restart"), &json!({})).await;
    assert_eq!(status, 202, "restart route: {restart}");
    let build_id = s(&restart["build_id"]);
    estate.await_attempt(&build_id, Estate::attempt_of(&restart), Duration::from_secs(120)).await;
    let operation = format!("restart-node:{target}");
    estate.stop().await;

    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or_default();
    let of_op = |sp: &[Value], name: &str| -> Vec<Value> { named(sp, name).into_iter().filter(|x| x["attributes"]["operation"] == operation.as_str()).cloned().collect() };

    // Publisher: the target mesh's primary, alone (Matrix: restart / owning mesh-admin; §17 "PUBLISHER").
    let published: Vec<(String, Value)> =
        pids.iter().flat_map(|(n, pid)| of_op(&spans_of(&estate, *pid), "rdm.node_admin.node.update.via-node-restarting").into_iter().map(|x| (n.clone(), x)).collect::<Vec<_>>()).collect();
    assert_eq!(published.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), vec![away_primary.as_str()], "only {away_primary}, the primary of {away}, publishes NodeRestarting for {target}: {published:?}");
    let publish = &published[0].1;
    assert_eq!(publish["attributes"]["node"], target.as_str());
    let trace_id = s(&publish["trace_id"]);
    estate.record_trace_url(&trace_id);
    eprintln!("RESTART-TRACE {trace_id} publisher={away_primary} target={target} fabric_primary={fabric_primary}");

    // Own mesh channel: the target mesh's rpc nodes hear it.
    for n in names.iter().filter(|n| n.starts_with(away) && n.contains(".rpc.")) {
        let heard = of_op(&spans_of(&estate, pids[n]), "rdm.mesh.membership.update.via-node-restarting");
        assert_eq!(heard.len(), 1, "{n} (mesh {away}) hears NodeRestarting once on its own mesh channel: {heard:?}");
    }
    // The backbone carries it as the target mesh's aggregate: the primary bumps its topology_version after publishing.
    let away_spans = spans_of(&estate, pids[&away_primary]);
    let bumped: Vec<&Value> = named(&away_spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|x| x["attributes"]["mesh"] == away && start(x) >= start(publish)).collect();
    assert!(!bumped.is_empty(), "{away_primary} bumps {away}'s topology_version after publishing NodeRestarting (the open overlay is in its aggregate)");
    // The other mesh's primary installs that aggregate from the backbone and forwards it onto its own mesh.
    let home_spans = spans_of(&estate, pids[&home_primary]);
    let installed: Vec<&Value> = named(&home_spans, "rdm.mesh.membership.update.via-snapshot-installed")
        .into_iter()
        .filter(|x| x["attributes"]["mesh"] == away && x["attributes"]["channel"] == "backbone" && start(x) >= start(publish))
        .collect();
    assert!(!installed.is_empty(), "{home_primary} installs {away}'s aggregate from the backbone after the publication");
    let forwarded: Vec<&Value> = named(&home_spans, "rdm.mesh.membership.update.via-forwarded-delta").into_iter().filter(|x| x["attributes"]["mesh"] == away && start(x) >= start(publish)).collect();
    assert!(!forwarded.is_empty(), "{home_primary} forwards {away}'s delta onto {home}'s channel after the publication");
    // Its members hear the same operation, after the forward.
    for n in names.iter().filter(|n| n.starts_with(home) && n.contains(".rpc.")) {
        let heard = of_op(&spans_of(&estate, pids[n]), "rdm.mesh.membership.update.via-node-restarting");
        assert_eq!(heard.len(), 1, "{n} (mesh {home}) hears the same operation once: {heard:?}");
        assert!(start(&heard[0]) >= start(forwarded.iter().min_by_key(|x| start(x)).unwrap()), "{n} hears it from its primary's forward, never before it");
        assert_eq!(heard[0]["attributes"]["node_id"], publish["attributes"]["node_id"], "the same target NodeId");
        assert_eq!(heard[0]["attributes"]["incarnation_id"], publish["attributes"]["incarnation_id"], "the same old incarnation");
    }
}
