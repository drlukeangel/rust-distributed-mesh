//! Restart canary: product=mesh, feature=node-lifecycle, subfeature=node-restart, rung=multi-node
//! (MN). A restart is node.stop then node.start (Q183: node.restart is removed). The provider is
//! `MESH_SPAWN_TYPE` (process by default); the body is `rdm_test::canary::restart_canary`.

use rdm_core::PathName;
use rdm_test::canary::restart_canary;
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-restart".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// CONTRACT: an RPC node restarted through Build, node.stop then node.start on the stopped process: the same
/// node id, incarnation, endpoint key and port, the same process; it still serves the value written
/// before the restart from its own data dir, resets an unfinished request with 499 (NotSent), and
/// leaves a Build -> node operation -> start step -> start span chain linked by ParentSpanId.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state() {
    restart_canary(Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "node-restart".into(),
        rung: "multi-node".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state".into(),
    })
    .await;
}

/// CONTRACT (node-primary-update.md steps 3-6; node-stop.md; node-start.md): a mesh's primary node-admin
/// stopped through `node.stop` first moves its mesh-primary role to the mesh's other admin (its issued
/// writes finished, the row, the successor installed); `node.start` brings it back as a member.
/// Stopping that admin moves the role back: the row names the first admin the successor, which installs
/// itself and is its mesh's writer from that moment, and it writes the second admin's stop rows. Both
/// stops and starts complete, each admin the same birth, ready. Must NOT happen: the second
/// stop not admitted, or the role not back on the first admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_restarted_and_made_mesh_primary_again_writes_its_mesh_rows() {
    let test = "a_mesh_primary_restarted_and_made_mesh_primary_again_writes_its_mesh_rows";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = estate.node_values().await;
    let admin = |primary: bool| before.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && (n["is_primary"] == true) == primary).cloned().unwrap_or_else(|| panic!("mesh2 holds an admin with is_primary={primary}: {before:#?}"));
    let (first, second) = (admin(true), admin(false));
    let name = |n: &Value| n["name"].as_str().unwrap_or_default().to_string();
    let nodes = estate.nodes_obj().await;
    let mut operations = Vec::new();
    for target in [&first, &second] {
        let path: PathName = name(target).parse().unwrap();
        let mut stream = nodes.stop(&path).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {path}: {e}"));
        Estate::complete(&mut stream).await;
        estate.until_stopped_at_the_fabric_primary(&path).await;
        let mut stream = nodes.start(&path).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.start of {path}: {e}"));
        Estate::complete(&mut stream).await;
        operations.push(format!("node.stop:{path}"));
        let after = estate.until_node(&name(target), &format!("{path} ready again after its stop and start"), Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["stopped"] == false).then_some(n)
        }).await;
        assert_eq!((&after["node_id"], &after["incarnation_id"]), (&target["node_id"], &target["incarnation_id"]), "{path} is the same birth after its stop and start: {after}");
    }
    let primary = estate.node_values().await.into_iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2 holds a primary");
    assert_eq!(primary["node_id"], first["node_id"], "the second stop moved mesh2's role back to {}", name(&first));

    estate.stop().await;
    let spans = estate.spans();
    let back: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "operation").contains(&operations[1])).collect();
    assert_eq!(back.len(), 1, "the second stop's successor, {}, installed itself once: {back:#?}", name(&first));
    let trace = back[0]["trace_id"].as_str().unwrap_or_default().to_string();
    println!("role given back: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
