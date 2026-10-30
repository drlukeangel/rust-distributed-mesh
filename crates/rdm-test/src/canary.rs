//! The restart canary (i143 PRD §5): an RPC node restarted by node.stop then node.start through Build
//! (Q183: node.restart is removed) is the same node, the same process, under the same incarnation,
//! endpoint key and port (nothing is deployed), serves the value written before the stop from its own
//! data dir, resets an unfinished request with 499, and leaves a Build -> deployment -> node chain
//! linked by ParentSpanId for each call. `restart_canary` is run by `node_restart` and by the
//! registered cells of `i143_acceptance_2777`, each under its own `Owner`.

use crate::estate::{descends_from, named, wait_for, Estate, Owner};
use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, PathName};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);
// The canary restarts the rpc node-type primary (`is_primary == true`).

fn ready(node: &Value) -> bool {
    node["status"] == "ready"
}

/// The runtime `node` runs in: its pid (process provider) or its container id (container provider).
async fn runtime_of(estate: &Estate, node: &str) -> Value {
    if estate.owner.provider == "container" {
        json!({"container": estate.container_of(node).await, "provider": "container"})
    } else {
        json!({"pid": estate.pid_of(node).await, "provider": "process"})
    }
}

/// Run the canary on `owner.provider` and return the facts it observed.
pub async fn restart_canary(owner: Owner) -> Value {
    let estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;

    // 1. MN = 2 node-admins + 3 RPC nodes, through node-admin Build only.
    let shape = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 3)])] };
    estate.artifact("build-request.json", &json!({"call": "build.create", "desired": shape}));
    let birth_build = estate.reconcile(&shape).await.build_id.to_string();
    estate.settles_to(&[("mesh1", 2, 3)], SETTLE).await;

    // 2. Select the rpc node-type primary (lowest node_id) and Put{key=41, value="before-restart"} by ExactNode.
    let (node_type_primary, before) = estate.until_view("rpc node-type primary", SETTLE, |v| v.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::RpcNode && n.is_primary).map(|n| (n.name.to_string(), serde_json::to_value(n).unwrap()))).await; let node_name = node_type_primary.as_str();
    let node_id = before["node_id"].as_str().expect("node_id").to_string();
    let exact = format!("exact:{node_id}");
    let put = estate.probe(&["put", "--target", &exact, "--key", "41", "--value", "before-restart"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    assert_eq!(put["reply"]["executing_node"], node_id.as_str(), "{put}");

    // 3. Capture identity, ports, deployment id and the advertised admin endpoint.
    let fabric_before = estate.fabric_view().await;
    let control_before = fabric_before.meshes[0].admin_api_base.clone().expect("advertised admin endpoint");
    let incarnation_before = before["incarnation_id"].as_str().expect("incarnation_id").to_string();
    let deployment_before = before["deployment_id"].as_str().expect("deployment_id").to_string();
    estate.artifact("nodes-before.json", &json!(estate.node_values().await));
    let process_before = runtime_of(&estate, node_name).await;
    let transport_before = before["transport_addr"].clone();

    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // 4. Restart: node.stop, then node.start; each answers with an attempt of the accepted Build and
    // does no bespoke work.
    let node: PathName = node_name.parse().unwrap();
    let nodes = estate.nodes_obj().await;
    let mut stream = nodes.stop(&node).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop {node_name}: {e}"));
    let restart_build = stream.accepted().build_id.to_string();
    assert_eq!(restart_build, birth_build, "a stop changes no topology: it is an attempt of the accepted Build");
    // The attempt is opened by the fabric-primary, the one admin the Build family answers at: build.get reads it there at once.
    let submitted = estate.carrier().await.build(&stream.accepted().build_id).await.unwrap_or_else(|e| panic!("build.get [id: {restart_build}]: {e}"));
    assert_eq!(submitted.build_id.to_string(), restart_build, "the Build is visible by id: {submitted:?}");
    assert_eq!(submitted.reason, "stop", "{submitted:?}");
    let stop = Estate::complete(&mut stream).await;
    estate.until_stopped_at_the_fabric_primary(&node).await;
    let mut stream = nodes.start(&node).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.start {node_name}: {e}"));
    assert_eq!(stream.accepted().build_id.to_string(), birth_build, "a start changes no topology: it is an attempt of the accepted Build");
    let start = Estate::complete(&mut stream).await;
    let status_after = estate.carrier().await.build(&start.build_id).await.unwrap_or_else(|e| panic!("build.get [id: {restart_build}]: {e}"));
    estate.artifact("build-status.json", &json!({"birth": birth_build, "restart": format!("{status_after:?}")}));

    // The node is ready again (observed, not slept on), and no longer stopped.
    let after = estate
        .until_node(node_name, "rpc.2 ready after its restart", SETTLE, |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (ready(&n) && n["stopped"] == false).then_some(n)
        })
        .await;
    estate.artifact("nodes-after.json", &json!(estate.node_values().await));

    // 5. Same logical node identity returns.
    assert_eq!(after["node_id"], before["node_id"], "same logical node id");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "same transport identity (data dir kept)");
    assert_eq!(after["name"], before["name"]);
    assert_eq!(after["deployment_id"].as_str(), Some(deployment_before.as_str()), "the same runtime deployment: nothing was deployed");
    assert_eq!(after["incarnation_id"].as_str(), Some(incarnation_before.as_str()), "the same incarnation: stop then start on the stopped process");

    // 6. The process never exited: it keeps its endpoint and its port.
    assert_eq!(after["transport_addr"], transport_before, "a stop and a start keep the transport port");
    let process_after = runtime_of(&estate, node_name).await;
    assert_eq!(process_after, process_before, "the same process (or container) before the stop and after the start");

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
    let fabric_after = estate.fabric_view().await;
    assert_eq!(fabric_after.meshes[0].admin_api_base.as_deref(), Some(control_before.as_str()));

    assert_eq!(estate.node_views().await.iter().filter(|n| n.name == node).count(), 1, "the path holds exactly one birth");

    // 11. OTLP evidence: Build -> node operation -> step -> the node's own span, linked by ParentSpanId,
    // for the stop and for the start. A span is exported once it ends, by each process's own exporter:
    // the chain is exported when the node's start (the rpc node's span), each call's request span and
    // each call's reconcile (the executor's outermost span, ending after the node operation and every
    // step under it) are.
    let (stop_op, start_op) = (format!("node.stop:{node_name}"), format!("node.start:{node_name}"));
    let reconciled = |spans: &[Value], op: &str| named(spans, "rdm.node_admin.build.update.via-reconcile").into_iter().any(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op)));
    let spans = wait_for("stop and start span chains exported", SETTLE, || async {
        let spans = estate.spans();
        let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().any(|s| s["attributes"]["node"] == node_name && s["attributes"]["outcome"] == "started");
        let called = named(&spans, "rdm.node_admin.build.update.via-node-rpc").into_iter().filter(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["node"] == node_name).count() >= 2;
        (served && called && reconciled(&spans, &stop_op) && reconciled(&spans, &start_op)).then_some(spans)
    })
    .await;
    // node-stop.md step 5: the stop's stopped is published as NodeStopped, once, and node.start clears
    // it at node.joined (gossip.md "Node-stopped lifecycle overlay").
    let published: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|s| s["attributes"]["node_id"] == node_id.as_str() && s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["operation"] == stop_op.as_str()).collect();
    assert_eq!(published.len(), 1, "the stop's stopped is published as NodeStopped once: {published:#?}");
    // Each attempt is the call that opened it: its reconcile is a child of that call's request span
    // (the claim returns the attempt's context), never of the span that accepted the Build. The
    // request span names the node the attempt acts on.
    let created = named(&spans, "rdm.node_admin.build.create.via-node-rpc")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == birth_build.as_str())
        .expect("rdm.node_admin.build.create.via-node-rpc for the accepted Build");
    let call_of = |attempt: u32| {
        named(&spans, "rdm.node_admin.build.update.via-node-rpc")
            .into_iter()
            .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["node"] == node_name && s["attributes"]["attempt"] == attempt.to_string().as_str())
            .unwrap_or_else(|| panic!("rdm.node_admin.build.update.via-node-rpc for attempt {attempt}"))
    };
    let rest = call_of(stop.attempt);
    let opened_attempt = stop.attempt.to_string();
    let handoff_dispatch = named(&spans, "rdm.node_admin.build.update.via-dispatch")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["attempt"] == opened_attempt.as_str())
        .expect("the attempt opened by the stop call dispatched the rpc-node primary's node.primary.update");
    assert_eq!(handoff_dispatch["attributes"]["outcome"], "handed-off", "the rpc-node primary's node.primary.update attempt ended handed-off: {handoff_dispatch}");
    assert_eq!(handoff_dispatch["parent_span_id"], rest["span_id"], "the hand-off dispatch is a child of the stop call's request span: {handoff_dispatch}");
    let primary_update = named(&spans, "rdm.node_admin.primary.update.via-command")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["attempt"] == opened_attempt.as_str() && s["attributes"]["primary"] == node_name)
        .expect("rdm.node_admin.primary.update.via-command for the stopped rpc-node primary");
    assert_eq!(primary_update["attributes"]["outcome"], "updated", "node.primary.update moved the rpc-node primary before the stop: {primary_update}");
    assert!(descends_from(&spans, primary_update, handoff_dispatch), "node.primary.update descends from the hand-off attempt's dispatch span");
    let chain = |call: &Value, op: &str, steps: &[&str]| {
        let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile")
            .into_iter()
            .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op)))
            .unwrap_or_else(|| panic!("the reconcile that executed {op}"));
        assert_eq!(reconcile["parent_span_id"], call["span_id"], "{op}'s reconcile is the child of its call's request span: {reconcile}");
        assert_eq!(reconcile["trace_id"], call["trace_id"], "one trace: the {op} call and its reconcile");
        assert_ne!(reconcile["trace_id"], created["trace_id"], "{op} is not under the Build's original create span");
        let attempt = reconcile["attributes"]["attempt"].as_str().expect("the reconcile span carries its attempt").to_string();
        let node_op = named(&spans, "rdm.node_admin.node.update.via-build")
            .into_iter()
            .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["node"] == node_name && s["attributes"]["attempt"] == attempt.as_str())
            .unwrap_or_else(|| panic!("rdm.node_admin.node.update.via-build of {op}"));
        assert!(descends_from(&spans, node_op, call), "{op}'s node.update.via-build descends from its call's build.update.via-node-rpc");
        assert!(!descends_from(&spans, node_op, created), "and not from the Build's original create span");
        let step = |name: &str| {
            named(&spans, "rdm.node_admin.deployment.update.via-step")
                .into_iter()
                .find(|s| s["attributes"]["build_id"] == restart_build.as_str() && s["attributes"]["step"] == name && s["attributes"]["node"] == node_name && s["attributes"]["attempt"] == attempt.as_str())
                .unwrap_or_else(|| panic!("the {name} step span of {op}'s attempt for {node_name}"))
        };
        for name in steps {
            assert!(descends_from(&spans, step(name), node_op), "the {name} step descends from {op}'s node Build operation");
            assert_eq!(step(name)["attributes"]["outcome"], "complete", "{name}");
        }
        assert!(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().all(|s| s["attributes"]["build_id"] != restart_build.as_str() || s["attributes"]["attempt"] != attempt.as_str() || !matches!(s["attributes"]["step"].as_str(), Some("DeployRuntime" | "TerminateRuntime"))), "{op} deploys nothing and terminates nothing");
        let first = step(steps[0]);
        (reconcile, node_op, attempt, first)
    };
    let (reconcile, node_op, attempt, _) = chain(rest, &stop_op, &["StopNode", "AwaitNodeLeft"]);
    assert_eq!(attempt, (stop.attempt + 1).to_string(), "the stop executes under the attempt after the role hand-off");
    let start_call = call_of(start.attempt);
    let (_, _, _, start_step) = chain(start_call, &start_op, &["StartNode", "WaitForNodeReady"]);
    let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().filter(|s| s["attributes"]["node"] == node_name).find(|s| descends_from(&spans, s, start_step)).expect("the node's start served under the start's StartNode step");
    assert!(descends_from(&spans, served, start_step), "the node's start descends from the start's StartNode step");
    estate.record_trace_url(rest["trace_id"].as_str().unwrap());
    let facts = json!({
        "node": node_name,
        "node_id": node_id,
        "endpoint_id": after["endpoint_id"],
        "incarnation_before": incarnation_before,
        "incarnation_after": after["incarnation_id"],
        "transport_before": transport_before,
        "transport_after": after["transport_addr"],
        "birth_build": birth_build,
        "restart_build": restart_build,
        "restart_attempt": stop.attempt,
        "executing_attempt": attempt,
        "start_attempt": start.attempt,
        "process_before": process_before,
        "process_after": process_after,
        "get_after_restart": get,
        "cut": cut,
        "cut_key_absent": absent,
        "trace_id": rest["trace_id"],
        "build_update_via_node_rpc_span": rest["span_id"],
        "handoff_dispatch_span": handoff_dispatch["span_id"],
        "primary_update_span": primary_update["span_id"],
        "reconcile_span": reconcile["span_id"],
        "node_update_via_build_span": node_op["span_id"],
        "start_step_span": start_step["span_id"],
        "start_served_span": served["span_id"],
    });
    estate.artifact("canary-facts.json", &facts);
    estate.shutdown().await;
    facts
}
