//! The restart canary (i143 PRD §5): a node restarted by node.stop then node.start (Q183:
//! node.restart is removed), each a node command of the primary above the node and never a Build
//! (Q342), is the same node, the same process, under the same incarnation, endpoint key and port
//! (nothing is deployed), serves the value written before the stop from its own data dir, resets an
//! unfinished request with 499, writes no Build, and leaves a node command -> deployment step -> node
//! chain linked by ParentSpanId for each call. `restart_canary` is run by `node_restart` and by the
//! registered cells of `i143_acceptance_2777`, each under its own `Owner`.

use crate::estate::{descends_from, named, wait_for, Estate, Owner};
use rdm_client::{FabricDesired, MeshDesired, PathName};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);
// The canary restarts the `node` type primary (`is_primary == true`).

fn ready(node: &Value) -> bool {
    node["status"] == "ready"
}

/// The runtime `node` runs in: its pid (process provider) or its container id (container provider).
async fn runtime_of(estate: &Estate, node: &PathName) -> Value {
    if estate.owner.provider == "container" {
        json!({"container": estate.container_of(node).await, "provider": "container"})
    } else {
        json!({"pid": estate.pid_of(node).await, "provider": "process"})
    }
}

/// Run the canary on `owner.provider` and return the facts it observed.
pub async fn restart_canary(owner: Owner) -> Value {
    let estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;

    // 1. MN = 2 node-admins + 3 nodes, through node-admin Build only.
    let shape = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 2), ("node", 3)])] };
    estate.artifact("build-request.json", &json!({"call": "build.create", "desired": shape}));
    let birth_build = estate.reconcile(&shape).await.build_id.to_string();
    estate.settles_to(&[("mesh1", 2, 3)], SETTLE).await;

    // 2. Select the `node` type primary (lowest node_id) and Put{key=41, value="before-restart"} by ExactNode.
    let (node_type_primary, before) = estate.until_view("`node` type primary", SETTLE, |v| v.iter().find(|n| n.mesh == "mesh1" && n.r#type == "node" && n.is_primary).map(|n| (n.location(), serde_json::to_value(n).unwrap()))).await; let node_name = &node_type_primary;
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
    // 4. Restart: node.stop, then node.start, node commands of the primary above the node (D40):
    // neither opens, reads or writes a Build (Q342).
    let node = node_name.clone();
    let nodes = estate.nodes_obj().await;
    let builds_before = estate.carrier().await.builds(&rdm_client::Filter::new()).await.expect("build.get");
    let mut stream = nodes.stop(&node).await.unwrap_or_else(|e| panic!("node.stop {node_name:?} was refused: {e}"));
    assert!(stream.accepted().is_none(), "a stop opens no Build");
    assert!(Estate::complete(&mut stream).await.is_none());
    estate.until_stopped_at_the_fabric_primary(&node).await;
    let mut stream = nodes.start(&node).await.unwrap_or_else(|e| panic!("node.start {node_name:?} was refused: {e}"));
    assert!(stream.accepted().is_none(), "a start opens no Build");
    assert!(Estate::complete(&mut stream).await.is_none());
    let builds_after = estate.carrier().await.builds(&rdm_client::Filter::new()).await.expect("build.get");
    let ids = |b: &[rdm_client::Build]| b.iter().map(|b| b.build_id.to_string()).collect::<Vec<_>>();
    assert_eq!(ids(&builds_after), ids(&builds_before), "a stop and a start write no Build");
    estate.artifact("build-status.json", &json!({"birth": birth_build, "builds": ids(&builds_after)}));

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
    assert_eq!(crate::estate::location(&after), crate::estate::location(&before));
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

    assert_eq!(estate.node_views().await.iter().filter(|n| n.location() == node).count(), 1, "the path holds exactly one birth");

    // 11. OTLP evidence: the primary's node command -> its steps -> the node's own span, linked by
    // ParentSpanId, for the stop and for the start. A span is exported once it ends, by each process's
    // own exporter.
    let (stop_op, start_op) = ("stop-node", "start-node");
    let at_location = |s: &Value| s["attributes"]["node_path"] == node.path && s["attributes"]["node_name"] == node.name;
    let command = |spans: &[Value], op: &str, outcome: &str| named(spans, "rdm.node_admin.node.update.via-node-command").into_iter().find(|s| s["attributes"]["command"] == op && s["attributes"]["subject_id"] == node_id.as_str() && s["attributes"]["outcome"] == outcome).cloned();
    let spans = wait_for("stop and start span chains exported", SETTLE, || async {
        let spans = estate.spans();
        let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().any(|s| s["attributes"]["node"] == node_id.as_str() && s["attributes"]["outcome"] == "started");
        (served && command(&spans, stop_op, "node-stopped").is_some() && command(&spans, start_op, "node-started").is_some()).then_some(spans)
    })
    .await;
    // node-stop.md step 5: the stop's stopped is published as NodeStopped, once, under no Build, and
    // node.start clears it at node.joined (gossip.md "Node-stopped lifecycle overlay").
    let published: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|s| s["attributes"]["node_id"] == node_id.as_str() && s["attributes"]["build_id"] == "" && s["attributes"]["operation"] == "node.stop").collect();
    assert_eq!(published.len(), 1, "the stop's stopped is published as NodeStopped once: {published:#?}");
    let stop_command = command(&spans, stop_op, "node-stopped").expect("the stop's node command span");
    // The canary's node is the rpc-node primary: its role moves by node.primary.update before its stop,
    // under the stop's node command (R-MT1).
    let primary_update = named(&spans, "rdm.node_admin.primary.update.via-command")
        .into_iter()
        .find(|s| s["attributes"]["primary_node_id"] == node_id.as_str() && descends_from(&spans, s, &stop_command))
        .expect("rdm.node_admin.primary.update.via-command for the stopped node primary, under its stop");
    assert_eq!(primary_update["attributes"]["outcome"], "updated", "node.primary.update moved the node primary before the stop: {primary_update}");
    let chain = |call: &Value, op: &str, steps: &[&str]| {
        let step = |name: &str| {
            named(&spans, "rdm.node_admin.deployment.update.via-step")
                .into_iter()
                .find(|s| s["attributes"]["step"] == name && at_location(s) && descends_from(&spans, s, call))
                .unwrap_or_else(|| panic!("the {name} step span of {op} for {node_name:?}"))
        };
        for name in steps {
            assert_eq!(step(name)["attributes"]["outcome"], "complete", "{name}");
            assert_eq!(step(name)["attributes"]["build_id"], "", "{op}'s {name} step belongs to no Build");
        }
        assert!(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().all(|s| !descends_from(&spans, s, call) || !matches!(s["attributes"]["step"].as_str(), Some("DeployRuntime" | "TerminateRuntime"))), "{op} deploys nothing and terminates nothing");
        step(steps[0])
    };
    chain(&stop_command, stop_op, &["StopNode", "AwaitNodeLeft"]);
    let start_command = command(&spans, start_op, "node-started").expect("the start's node command span");
    let start_step = chain(&start_command, start_op, &["StartNode", "WaitForNodeReady"]);
    let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().filter(|s| s["attributes"]["node"] == node_id.as_str()).find(|s| descends_from(&spans, s, start_step)).expect("the node's start served under the start's StartNode step");
    assert!(descends_from(&spans, served, start_step), "the node's start descends from the start's StartNode step");
    estate.record_trace_url(stop_command["trace_id"].as_str().unwrap());
    let facts = json!({
        "node": node_name,
        "node_id": node_id,
        "endpoint_id": after["endpoint_id"],
        "incarnation_before": incarnation_before,
        "incarnation_after": after["incarnation_id"],
        "transport_before": transport_before,
        "transport_after": after["transport_addr"],
        "birth_build": birth_build,
        "process_before": process_before,
        "process_after": process_after,
        "get_after_restart": get,
        "cut": cut,
        "cut_key_absent": absent,
        "trace_id": stop_command["trace_id"],
        "stop_command_span": stop_command["span_id"],
        "start_command_span": start_command["span_id"],
        "primary_update_span": primary_update["span_id"],
        "start_step_span": start_step["span_id"],
        "start_served_span": served["span_id"],
    });
    estate.artifact("canary-facts.json", &facts);
    estate.shutdown().await;
    facts
}
