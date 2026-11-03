//! A birth is one trace with its boot steps, and a running node marks its heartbeat.
//!
//! CONTRACT: `{node-admin: 1, node: 1}`. Each node's birth is one trace rooted at
//! `rdm.mesh.node.create.via-boot`; its identity, endpoint, gossip, ALPN, accept-loop and
//! topology steps and its `rdm.mesh.node.update.via-ready` are children of that root in the same
//! trace, every step finishing inside the root; the launched node also shows the join it took.
//! Every node's publish loop leaves `rdm.mesh.node.update.via-heartbeat` carrying its peer count
//! and clock reading. What must NOT happen: a step span outside the birth's trace, a ready span
//! that is its own root, or a node that publishes with no heartbeat in the trace.

use rdm_test::estate::{named, of_node, wait_for, Estate, Owner};
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
    estate.resize_shape("fabric1", &[("mesh1", 1, 1)]).await;
    let views = estate.settles_to(&[("mesh1", 1, 1)], Duration::from_secs(30)).await;
    // Each node by its location and its node id: an admin's span names its node by `node_path` and
    // `node_name`, a node's own span by its id.
    let node = |path: &str, name: &str| {
        let location = rdm_core::PathName::new(path, name).unwrap();
        let id = views.iter().find(|v| v.location() == location).unwrap_or_else(|| panic!("{location:?} is in the settled view: {views:?}")).node_id.to_string();
        (location, id)
    };
    let (admin, rpc) = (node("mesh1.node-admin", "1"), node("mesh1.node", "1"));
    // The heartbeat is the 5 s cadence: wait out one period so the second mark carries a peer. No
    // object reports a heartbeat mark: the span the node records is the only place the fact lives.
    wait_for("both nodes mark a heartbeat that counts the other", Duration::from_secs(30), || async {
        let spans = estate.spans();
        [&admin, &rpc]
            .iter()
            .all(|(at, id)| named(&spans, "rdm.mesh.node.update.via-heartbeat").iter().any(|sp| of_node(sp, at, id) && attr(sp, "peer_count").parse::<u64>().unwrap_or(0) >= 1))
            .then_some(())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();

    let ns = |sp: &Value, k: &str| sp[k].as_u64().unwrap_or_else(|| panic!("{k} on {sp}"));
    let common = ["rdm.mesh.node.create.via-endpoint-bound", "rdm.mesh.node.create.via-gossip-started", "rdm.mesh.node.add.via-alpn-registered", "rdm.mesh.node.create.via-accept-loop-started", "rdm.mesh.node.update.via-topology-joined"];
    let mut report = Vec::new();
    for (at, id) in [&admin, &rpc] {
        let node = format!("{}.{}", at.path, at.name);
        let is_rpc = at == &rpc.0;
        let ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| of_node(sp, at, id)).cloned().unwrap_or_else(|| panic!("{node} reports ready"));
        let trace = s(&ready["trace_id"]);
        let in_trace: Vec<&Value> = spans.iter().filter(|sp| s(&sp["trace_id"]) == trace).collect();
        let boot = in_trace.iter().find(|sp| sp["name"] == "rdm.mesh.node.create.via-boot").cloned().unwrap_or_else(|| panic!("{node}: the ready span is not in a boot trace: {ready}"));
        assert_eq!(s(&boot["parent_span_id"]), "", "{node}: the boot span is the root of the birth's trace");
        assert!(of_node(boot, at, id), "{node}: the boot span names the node: {boot}");
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
        if is_rpc {
            assert!(in_trace.iter().any(|sp| sp["name"] == "rdm.mesh.node.update.via-join-admitted" && attr(sp, "launcher") == admin.1), "{node}: the join it took is a step of its boot, naming its launcher by node id");
        }
        let beats = named(&spans, "rdm.mesh.node.update.via-heartbeat").into_iter().filter(|sp| of_node(sp, at, id)).count();
        assert!(beats >= 1, "{node}: no heartbeat in the trace");
        report.push(json!({"node": node, "trace_id": trace, "boot_ns": ns(boot, "end_unix_nano") - ns(boot, "start_unix_nano"), "steps": steps, "heartbeats": beats}));
        if is_rpc {
            estate.record_trace_url(&trace);
        }
    }
    estate.artifact("boot-trace.json", &json!(report));
}
