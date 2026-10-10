//! The restart canary (i143 PRD §5): an RPC node restarted through Build is the same node, the same
//! process, under the same incarnation, endpoint key and port (restart is stop then start on the
//! parked process; nothing is deployed), serves the value written before the restart from its own
//! data dir, resets an unfinished request with 499, and leaves a Build -> deployment -> node chain
//! linked by ParentSpanId. `restart_canary` is run by `node_lifecycle__node_restart` and by the
//! registered cells of `i143_acceptance_2777`, each under its own `Owner`.

use crate::estate::{descends_from, named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);
const NODE: &str = "mesh1.rpc.2";

fn ready(node: &Value) -> bool {
    node["status"] == "ready-for-traffic"
}

/// The runtime `node` runs in: its pid (process provider) or its container id (container provider).
async fn runtime_of(estate: &Estate, node: &str) -> Value {
    if estate.owner.provider == "container" {
        json!({"container": estate.container_of(node), "provider": "container"})
    } else {
        json!({"pid": estate.pid_of(node).await, "provider": "process"})
    }
}

/// Run the canary on `owner.provider` and return the facts it observed.
pub async fn restart_canary(owner: Owner) -> Value {
    let estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;

    // 1. MN = 2 node-admins + 3 RPC nodes, through node-admin Build only.
    let shape = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]});
    estate.artifact("build-request.json", &json!({"route": "POST /api/build", "body": shape}));
    let (status, accepted) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "POST /api/build: {accepted}");
    let birth_build = accepted["build_id"].as_str().expect("build_id").to_string();
    estate.await_build(&birth_build, SETTLE).await;
    wait_for("MN settled: 2 admins + 3 rpc nodes ready-for-traffic", SETTLE, || async {
        let nodes = estate.nodes().await;
        let admins = nodes.iter().filter(|n| n["kind"] == "node_admin" && ready(n)).count();
        let rpc = nodes.iter().filter(|n| n["kind"] == "rpc_node" && ready(n)).count();
        (admins == 2 && rpc == 3).then_some(())
    })
    .await;

    // 2. Put{key=41, value="before-restart"} to rpc.2 by ExactNode.
    let before = estate.node(NODE).await;
    let node_id = before["node_id"].as_str().expect("node_id").to_string();
    let exact = format!("exact:{node_id}");
    let put = estate.probe(&["put", "--target", &exact, "--key", "41", "--value", "before-restart"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    assert_eq!(put["reply"]["executing_node"], node_id.as_str(), "{put}");

    // 3. Capture identity, ports, deployment id and the advertised admin endpoint.
    let (_, fabric_before) = estate.get("/api/fabric").await;
    let control_before = fabric_before["meshes"][0]["admin_api_base"].as_str().expect("advertised admin endpoint").to_string();
    let incarnation_before = before["incarnation_id"].as_str().expect("incarnation_id").to_string();
    let deployment_before = before["deployment_id"].as_str().expect("deployment_id").to_string();
    estate.artifact("nodes-before.json", &json!(estate.nodes().await));
    let process_before = runtime_of(&estate, NODE).await;
    let transport_before = before["transport_addr"].clone();

    // 4. Restart through the route; it answers with a Build id and does no bespoke work.
    let (status, restart) = estate.post(&format!("/api/nodes/{NODE}/restart"), &json!({})).await;
    assert_eq!(status, 202, "restart route: {restart}");
    let restart_build = restart["build_id"].as_str().expect("restart returns a build_id").to_string();
    assert_eq!(restart_build, birth_build, "a restart changes no topology: it is an attempt of the accepted Build");
    // The attempt is opened on the admin that took the POST (the fabric-primary); that admin's Build log is the one that holds it at once.
    let (_, submitted) = estate.http_get(&control_before, &format!("/api/builds?id={restart_build}")).await;
    assert_eq!(submitted["build_id"], restart_build.as_str(), "the Build is visible by id: {submitted}");
    assert_eq!(submitted["reason"], "restart", "{submitted}");
    assert_eq!(submitted["action"]["path"], NODE, "{submitted}");
    let status_after = estate.await_attempt(&restart_build, Estate::attempt_of(&restart), SETTLE).await;
    estate.artifact("build-status.json", &json!({"birth": birth_build, "restart": status_after}));

    // The node is ready again (observed, not slept on), and no longer parked.
    let after = wait_for("rpc.2 ready after its restart", SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["parked"] == false).then_some(n)
    })
    .await;
    estate.artifact("nodes-after.json", &json!(estate.nodes().await));

    // 5. Same logical node identity returns.
    assert_eq!(after["node_id"], before["node_id"], "same logical node id");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "same transport identity (data dir kept)");
    assert_eq!(after["name"], before["name"]);
    assert_eq!(after["deployment_id"].as_str(), Some(deployment_before.as_str()), "the same runtime deployment: nothing was deployed");
    assert_eq!(after["incarnation_id"].as_str(), Some(incarnation_before.as_str()), "the same incarnation: a restart is stop then start on the parked process");

    // 6. The process never exited: it keeps its endpoint and its port.
    assert_eq!(after["transport_addr"], transport_before, "a restart keeps the transport port");
    let process_after = runtime_of(&estate, NODE).await;
    assert_eq!(process_after, process_before, "the same process (or container) before and after the restart");

    // 7. The pre-restart value is still readable from the same node data dir, served by the new incarnation.
    let get = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    assert_eq!(get["outcome"], "Reply", "{get}");
    assert_eq!(get["reply"]["result"]["value"], "before-restart", "{get}");
    assert_eq!(get["reply"]["executing_node"], node_id.as_str(), "{get}");
    assert_eq!(get["reply"]["incarnation_id"], after["incarnation_id"], "served by the same incarnation: {get}");

    // 8. A request cut before full send is reset with 499 and reports NotSent; it was never dispatched.
    let cut = estate.probe(&["put", "--target", &exact, "--key", "42", "--value", "never", "--cut-before-finish"]);
    assert_eq!(cut["outcome"], "NotSent", "{cut}");
    assert_eq!(cut["reason"], "FrameNotSent", "the cut is the 499 FRAME_NOT_SENT reset: {cut}");
    let absent = estate.probe(&["get", "--target", &exact, "--key", "42"]);
    assert_eq!(absent["outcome"], "Reply", "{absent}");
    assert_eq!(absent["reply"]["result"]["found"], false, "the cut request was never applied: {absent}");

    // 10. Control did not move: the advertised admin endpoint is unchanged and still serving.
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(fabric_after["meshes"][0]["admin_api_base"].as_str(), Some(control_before.as_str()));

    assert_eq!(estate.nodes().await.iter().filter(|n| n["name"] == NODE).count(), 1, "the path holds exactly one birth");

    // 11. OTLP evidence: Build -> node operation -> start step -> the node's start, linked by ParentSpanId.
    let spans = wait_for("restart span chain exported", SETTLE, || async {
        let spans = estate.spans();
        let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().any(|s| s["attributes"]["node"] == NODE && s["attributes"]["outcome"] == "started");
        served.then_some(spans)
    })
    .await;
    // The restart's attempt is the REST call that opened it: its reconcile is a child of that
    // call's request span (the claim returns the attempt's context), never of the span that
    // accepted the Build.
    let created = named(&spans, "rdm.node_admin.build.create.via-rest")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == birth_build.as_str())
        .expect("rdm.node_admin.build.create.via-rest for the accepted Build");
    let rest = named(&spans, "rdm.node_admin.build.update.via-rest")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["node"] == NODE)
        .expect("rdm.node_admin.build.update.via-rest for the restart call");
    let restart_op = format!("restart-node:{NODE}");
    let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == restart_op)))
        .expect("the reconcile that executed the restart");
    assert_eq!(reconcile["parent_span_id"], rest["span_id"], "the restart's reconcile is the child of the restart call's request span: {reconcile}");
    assert_eq!(reconcile["trace_id"], rest["trace_id"], "one trace: the restart call and its reconcile");
    assert_ne!(reconcile["trace_id"], created["trace_id"], "the restart is not under the Build's original create span");
    let node_op = named(&spans, "rdm.node_admin.node.update.via-build")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["node"] == NODE)
        .expect("rdm.node_admin.node.update.via-build for rpc.2");
    assert!(descends_from(&spans, node_op, rest), "node.update.via-build descends from the restart call's build.update.via-rest");
    assert!(!descends_from(&spans, node_op, created), "and not from the Build's original create span");
    let attempt = Estate::attempt_of(&restart).to_string();
    let step = |name: &str| {
        named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["step"] == name && s["attributes"]["node"] == NODE && s["attributes"]["attempt"] == attempt.as_str())
            .unwrap_or_else(|| panic!("the {name} step span of the restart attempt for rpc.2"))
    };
    for name in ["NodeRestarting", "DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "StartNode", "WaitForNodeReady"] {
        assert!(descends_from(&spans, step(name), node_op), "the {name} step descends from the node Build operation");
        assert_eq!(step(name)["attributes"]["outcome"], "complete", "{name}");
    }
    assert!(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().all(|s| s["attributes"]["build_id"] != restart_build.as_str() || s["attributes"]["attempt"] != attempt.as_str() || !matches!(s["attributes"]["step"].as_str(), Some("DeployRuntime" | "TerminateRuntime"))), "a restart deploys nothing and terminates nothing");
    let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().find(|s| s["attributes"]["node"] == NODE).unwrap();
    assert!(descends_from(&spans, served, step("StartNode")), "the node's start descends from the StartNode step");
    estate.record_trace_url(rest["trace_id"].as_str().unwrap());
    let facts = json!({
        "node": NODE,
        "node_id": node_id,
        "endpoint_id": after["endpoint_id"],
        "incarnation_before": incarnation_before,
        "incarnation_after": after["incarnation_id"],
        "transport_before": transport_before,
        "transport_after": after["transport_addr"],
        "birth_build": birth_build,
        "restart_build": restart_build,
        "restart_attempt": Estate::attempt_of(&restart),
        "process_before": process_before,
        "process_after": process_after,
        "get_after_restart": get,
        "cut": cut,
        "cut_key_absent": absent,
        "trace_id": rest["trace_id"],
        "build_update_via_rest_span": rest["span_id"],
        "reconcile_span": reconcile["span_id"],
        "node_update_via_build_span": node_op["span_id"],
        "start_step_span": step("StartNode")["span_id"],
        "start_served_span": served["span_id"],
    });
    estate.artifact("canary-facts.json", &facts);
    estate.shutdown().await;
    facts
}
