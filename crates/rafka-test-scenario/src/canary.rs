//! The restart canary (i143 PRD §5): an RPC node restarted through Build is the same logical node
//! on fresh ports under a new incarnation, serves the value written before the restart from its own
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
    let old_birth = {
        let (_, b) = estate.http_get(&control_before, &format!("/api/builds?id={birth_build}")).await;
        b["steps"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|st| st["step"] == "DeployRuntime" && st["operation"] == format!("create-node:{NODE}").as_str())
            .map(|st| st["output"].clone())
            .expect("the birth's DeployRuntime output for rpc.2")
    };

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
    let new_birth = status_after["steps"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|st| st["step"] == "DeployRuntime" && st["attempt"] == Estate::attempt_of(&restart) && st["operation"].as_str().is_some_and(|o| o.ends_with(NODE)))
        .map(|st| st["output"].clone())
        .expect("the restart attempt's DeployRuntime output for rpc.2");
    estate.artifact("build-status.json", &json!({"birth": birth_build, "restart": status_after}));

    // The node comes back ready under a new process incarnation (observed, not slept on).
    let after = wait_for("rpc.2 ready under a new incarnation", SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["incarnation_id"].as_str() != Some(incarnation_before.as_str())).then_some(n)
    })
    .await;
    estate.artifact("nodes-after.json", &json!(estate.nodes().await));

    // 5. Same logical node identity returns.
    assert_eq!(after["node_id"], before["node_id"], "same logical node id");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "same transport identity (data dir kept)");
    assert_eq!(after["name"], before["name"]);
    assert_ne!(after["deployment_id"].as_str(), Some(deployment_before.as_str()), "a new runtime deployment");

    // 6. A restart binds fresh ports, never its recorded ones (fabric-node-lifecycle.md).
    assert_ne!(after["transport_addr"], before["transport_addr"], "a restart binds a fresh transport port");

    // 7. The pre-restart value is still readable from the same node data dir, served by the new incarnation.
    let get = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    assert_eq!(get["outcome"], "Reply", "{get}");
    assert_eq!(get["reply"]["result"]["value"], "before-restart", "{get}");
    assert_eq!(get["reply"]["executing_node"], node_id.as_str(), "{get}");
    assert_eq!(get["reply"]["incarnation_id"], after["incarnation_id"], "served by the new incarnation: {get}");

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

    // The old birth is gone: its runtime (process, or container) no longer runs, and the path holds exactly one birth.
    let old_gone = match old_birth["provider"].as_str() {
        Some("container") => {
            let old = old_birth["container"].as_str().expect("the container provider names the container");
            let running = estate.live_containers().into_iter().any(|(_, id)| id == old);
            assert!(!running, "the old birth's container {old} is not running: {old_birth}");
            json!({"container": old, "running": running})
        }
        _ => {
            let pid = old_birth["pid"].as_u64().expect("the process provider names the pid");
            let alive = std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')));
            assert!(!alive, "the old birth's process {pid} is gone: {old_birth}");
            json!({"pid": pid, "alive": alive})
        }
    };
    assert_eq!(estate.nodes().await.iter().filter(|n| n["name"] == NODE).count(), 1, "the path holds exactly one birth");

    // 11. OTLP evidence: Build -> deployment -> node lifecycle, linked by ParentSpanId.
    let spans = wait_for("restart span chain exported", SETTLE, || async {
        let spans = estate.spans();
        let boot = named(&spans, "rdm.mesh.node.create.via-deployment")
            .into_iter()
            .any(|s| s["attributes"]["incarnation_id"] == after["incarnation_id"]);
        boot.then_some(spans)
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
    let deploy = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|s| {
            s["attributes"]["build_id"] == restart_build.as_str()
                && s["attributes"]["step"] == "DeployRuntime"
                && s["attributes"]["node"] == NODE
                && s["attributes"]["attempt"] == Estate::attempt_of(&restart).to_string().as_str()
        })
        .expect("the DeployRuntime step span of the restart attempt for rpc.2");
    assert!(descends_from(&spans, deploy, node_op), "the deployment step descends from the node Build operation");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment")
        .into_iter()
        .find(|s| s["attributes"]["incarnation_id"] == after["incarnation_id"])
        .unwrap();
    assert!(descends_from(&spans, boot, deploy), "the new process's boot span descends from DeployRuntime");
    estate.record_trace_url(rest["trace_id"].as_str().unwrap());
    let facts = json!({
        "node": NODE,
        "node_id": node_id,
        "endpoint_id": after["endpoint_id"],
        "incarnation_before": incarnation_before,
        "incarnation_after": after["incarnation_id"],
        "transport_before": before["transport_addr"],
        "transport_after": after["transport_addr"],
        "birth_build": birth_build,
        "restart_build": restart_build,
        "restart_attempt": Estate::attempt_of(&restart),
        "old_birth": old_birth,
        "new_birth": new_birth,
        "old_birth_gone": old_gone,
        "get_after_restart": get,
        "cut": cut,
        "cut_key_absent": absent,
        "trace_id": rest["trace_id"],
        "build_update_via_rest_span": rest["span_id"],
        "reconcile_span": reconcile["span_id"],
        "node_update_via_build_span": node_op["span_id"],
        "deploy_runtime_span": deploy["span_id"],
        "boot_span": boot["span_id"],
    });
    estate.artifact("canary-facts.json", &facts);
    estate.shutdown().await;
    facts
}
