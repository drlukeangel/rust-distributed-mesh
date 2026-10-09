//! A birth is one trace with its boot steps, and a running node marks its heartbeat.
//!
//! CONTRACT: `{node_admin: 1, rpc_node: 1}`. Each node's birth is one trace rooted at
//! `rdm.mesh.node.create.via-boot`; its identity, endpoint, gossip, ALPN, accept-loop and
//! membership steps and its `rdm.mesh.node.update.via-ready` are children of that root in the same
//! trace, every step finishing inside the root; the launched node also shows the join it took.
//! Every node's publish loop leaves `rdm.mesh.node.update.via-heartbeat` carrying its peer count
//! and clock reading. What must NOT happen: a step span outside the birth's trace, a ready span
//! that is its own root, or a node that publishes with no heartbeat in the trace.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-runtime".into(),
        subfeature: "boot-trace".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_birth_is_one_trace_of_its_boot_steps_and_its_heartbeat_is_marked".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_birth_is_one_trace_of_its_boot_steps_and_its_heartbeat_is_marked() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let want = ["mesh1.admin.1", "mesh1.rpc.1"].iter().map(|n| n.to_string()).collect();
    estate.settled(&want, Duration::from_secs(30)).await;
    // The heartbeat is the 5 s cadence: wait out one period so the second mark carries a peer.
    wait_for("both nodes mark a heartbeat that counts the other", Duration::from_secs(30), || async {
        let spans = estate.spans();
        ["mesh1.admin.1", "mesh1.rpc.1"]
            .iter()
            .all(|n| named(&spans, "rdm.mesh.node.update.via-heartbeat").iter().any(|sp| attr(sp, "node") == *n && attr(sp, "peer_count").parse::<u64>().unwrap_or(0) >= 1))
            .then_some(())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();

    let ns = |sp: &Value, k: &str| sp[k].as_u64().unwrap_or_else(|| panic!("{k} on {sp}"));
    let common = ["rdm.mesh.node.create.via-endpoint-bound", "rdm.mesh.node.create.via-gossip-started", "rdm.mesh.node.add.via-alpn-registered", "rdm.mesh.node.create.via-accept-loop-started", "rdm.mesh.node.update.via-membership-joined"];
    let mut report = Vec::new();
    for node in ["mesh1.admin.1", "mesh1.rpc.1"] {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| attr(sp, "node") == node).cloned().unwrap_or_else(|| panic!("{node} reports ready"));
        let trace = s(&ready["trace_id"]);
        let in_trace: Vec<&Value> = spans.iter().filter(|sp| s(&sp["trace_id"]) == trace).collect();
        let boot = in_trace.iter().find(|sp| sp["name"] == "rdm.mesh.node.create.via-boot").cloned().unwrap_or_else(|| panic!("{node}: the ready span is not in a boot trace: {ready}"));
        assert_eq!(s(&boot["parent_span_id"]), "", "{node}: the boot span is the root of the birth's trace");
        assert_eq!(attr(boot, "node"), node);
        assert_eq!(s(&ready["parent_span_id"]), s(&boot["span_id"]), "{node}: ready is a child of the boot root");
        let identity = in_trace.iter().filter(|sp| matches!(sp["name"].as_str(), Some("rdm.mesh.node.create.via-identity-minted" | "rdm.mesh.node.resolve.via-identity-loaded"))).count();
        assert_eq!(identity, 1, "{node}: identity is loaded or minted exactly once");
        let mut steps = Vec::new();
        for name in common {
            let step = in_trace.iter().find(|sp| sp["name"] == name).cloned().unwrap_or_else(|| panic!("{node}: no {name} in its boot trace"));
            assert_eq!(s(&step["parent_span_id"]), s(&boot["span_id"]), "{node}: {name} is a child of the boot root");
            assert!(ns(step, "start_unix_nano") >= ns(boot, "start_unix_nano") && ns(step, "end_unix_nano") <= ns(boot, "end_unix_nano"), "{node}: {name} runs inside the boot span");
            steps.push(json!({"step": name, "duration_ns": ns(step, "end_unix_nano") - ns(step, "start_unix_nano")}));
        }
        if node == "mesh1.rpc.1" {
            assert!(in_trace.iter().any(|sp| sp["name"] == "rdm.mesh.node.update.via-join-admitted" && attr(sp, "launcher") == "mesh1.admin.1"), "{node}: the join it took is a step of its boot");
        }
        let beats = named(&spans, "rdm.mesh.node.update.via-heartbeat").into_iter().filter(|sp| attr(sp, "node") == node).count();
        assert!(beats >= 1, "{node}: no heartbeat in the trace");
        report.push(json!({"node": node, "trace_id": trace, "boot_ns": ns(boot, "end_unix_nano") - ns(boot, "start_unix_nano"), "steps": steps, "heartbeats": beats}));
        if node == "mesh1.rpc.1" {
            estate.record_trace_url(&trace);
        }
    }
    estate.artifact("boot-trace.json", &json!(report));
}
