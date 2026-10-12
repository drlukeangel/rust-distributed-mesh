//! i143.e7.s3 acceptance (rafka-v2 #2775), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2775-process`, which exports `I143_ACCEPTANCE_DIR` (each
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it, feature `i143-2775`).
//!
//! The testkit proof store (op 0x70) on a real estate, driven only through the node-admin control
//! API and `rafka-rpc-probe` over real Node RPC: a restart is a Build the rectifier executes; a
//! replacement is the rectifier's answer to the runtime's death (an attempt whose action is
//! `replace`, reason proven drift).

use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const NODE: &str = "mesh1.rpc.1";
const OTHER: &str = "mesh1.rpc.2";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2775".into(),
        subfeature: "proof-store".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    let dir = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2775/process").join(cell),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The reply's provenance is exactly `node` as the typed view (the current projection) holds it.
fn landed_on(out: &Value, node: &Value, op: &str) {
    assert_eq!(out["outcome"], "Reply", "{out}");
    let r = &out["reply"];
    assert_eq!(r["executing_node"], node["node_id"], "{out}");
    assert_eq!(r["node"], node["name"], "{out}");
    assert_eq!(r["mesh"], node["mesh"], "{out}");
    assert_eq!(r["incarnation_id"], node["incarnation_id"], "{out}");
    assert_eq!(r["op"], op, "{out}");
}

/// mesh1 with one node-admin and two rpc nodes, through the rectifier: the estate and the Build that
/// accepted the shape.
async fn estate(cell: &str) -> (Estate, rafka_node_admin_client::BuildId) {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let accepted = estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    (estate, accepted.build_id)
}

/// `node.get` of `name`, as the JSON the cell records.
async fn view(estate: &Estate, name: &str) -> Value {
    serde_json::to_value(estate.node_view(name).await).expect("a view serializes")
}

fn birth(n: &Value) -> Value {
    json!({"node": n["name"], "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "mesh": n["mesh"]})
}

/// CONTRACT (#2775): a restart of the logical node holding the proof store, a `node.restart` call to the
/// fabric-primary (Node RPC -> reconcile -> node.update.via-build), is stop then start on the parked
/// process: it keeps its NodeId, incarnation and data dir and serves the value written before it, the
/// Put and the Get after the restart each naming the executing birth exactly as the node-admin view
/// holds it. What must NOT happen: the restarted node answering absent, or the restart reaching the
/// node other than through the Build it was accepted as.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proof_holder_restarts_preserves_same_node_store() {
    let cell = "proof_holder_restarts_preserves_same_node_store";
    let dir = acceptance_dir(cell);
    let (mut estate, _) = estate(cell).await;
    let path = format!("path:{NODE}");
    let first = view(&estate, NODE).await;
    let exact = format!("exact:{}", s(&first["node_id"]));
    let first_dir = estate.node_view(NODE).await.data_dir.expect("the launching admin advertises the node's data dir");
    let put = estate.probe(&["put", "--target", &path, "--key", "41", "--value", "before-restart"]);
    landed_on(&put, &first, "put");
    let before = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    landed_on(&before, &first, "get");
    assert_eq!(before["reply"]["result"], json!({"found": true, "value": "before-restart"}), "presence control: {before}");

    let mut restart = estate.nodes_obj().await.restart(&NODE.parse().unwrap()).await.expect("the fabric-primary accepts the restart");
    let restart_attempt = Estate::complete(&mut restart).await;
    let restart_build = restart_attempt.build_id.0.clone();
    let restarted = view(&estate, NODE).await;
    assert_eq!(restarted["status"], "ready-for-traffic", "the restarted birth is ready: {restarted}");
    assert_eq!(restarted["node_id"], first["node_id"], "a restart keeps the logical node");
    assert_eq!(restarted["incarnation_id"], first["incarnation_id"], "a restart is stop then start on the parked process: the same birth (ops-naming-proposal.md, Restart is not a re-birth)");
    let restarted_dir = estate.node_view(NODE).await.data_dir.expect("the launching admin advertises the node's data dir");
    assert_eq!(restarted_dir, first_dir, "a restart keeps the node's data dir");
    let kept = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    landed_on(&kept, &restarted, "get");
    assert_eq!(kept["reply"]["result"], json!({"found": true, "value": "before-restart"}), "the new incarnation serves the old value: {kept}");
    let store_file = std::fs::read_dir(&restarted_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).find(|p| p.file_name().is_some_and(|f| f.to_string_lossy().contains("proof"))).map(|p| p.display().to_string());
    estate.stop().await;

    let spans = estate.spans();
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request");
    let at = |inc: &Value, op: &str, outcome: &str| served.iter().find(|s| s["attributes"]["incarnation_id"] == *inc && s["attributes"]["op"] == op && s["attributes"]["outcome"] == outcome).cloned().cloned();
    let put_span = at(&first["incarnation_id"], "put", "stored").expect("the first birth served the put");
    let kept_span = at(&restarted["incarnation_id"], "get", "value").expect("the restarted birth served the kept value");
    // The restart ran as the Build attempt its REST request opened: the request, then the
    // rectifier's node update of this path under the same Build.
    let accepted = named(&spans, "rdm.node_admin.build.update.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"] == restart_build.as_str() && sp["attributes"]["node"] == NODE).cloned().expect("the restart's build.create span");
    let update = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| sp["attributes"]["build_id"] == restart_build.as_str() && sp["attributes"]["node"] == NODE).cloned().expect("the restart's node update");
    assert_eq!(update["attributes"]["attempt"], accepted["attributes"]["attempt"], "the node update runs the attempt the request opened");
    let reconcile = spans.iter().find(|sp| sp["span_id"] == update["parent_span_id"]).cloned().unwrap_or(Value::Null);
    // The restart is stop then start on the parked process: the Get after it is served by the incarnation that served the Put.
    assert_eq!(kept_span["attributes"]["incarnation_id"], put_span["attributes"]["incarnation_id"], "the restarted node is the same birth");

    let result = json!({
        "cell": cell,
        "first": birth(&first),
        "restarted": birth(&restarted),
        "data_dir": {"before": first_dir, "after": restarted_dir, "store_file": store_file},
        "put": put["reply"],
        "presence_control": before["reply"],
        "kept": kept["reply"],
        "restart_build": restart_build,
        "spans": {
            "put": {"trace_id": put_span["trace_id"], "span_id": put_span["span_id"], "parent_span_id": put_span["parent_span_id"]},
            "kept": {"trace_id": kept_span["trace_id"], "span_id": kept_span["span_id"], "parent_span_id": kept_span["parent_span_id"]},
            "restart": {
                "rest": {"span_id": accepted["span_id"], "trace_id": accepted["trace_id"], "attempt": accepted["attributes"]["attempt"]},
                "reconcile": {"span_id": reconcile["span_id"], "parent_span_id": reconcile["parent_span_id"], "trace_id": reconcile["trace_id"]},
                "node_update": {"span_id": update["span_id"], "trace_id": update["trace_id"]},
                "reconcile_descends_from_rest": descends_from(&spans, &update, &accepted),
            },
        },
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2775; node-recovery.md: "node.delete of the exited instance, node.create at the same
/// path.name over the inherited data dir, then node[2].update"): the runtime's death is answered by
/// node recovery under the accepted Build, a new NodeId at the same path.name over the old birth's
/// data dir: `path:` reaches the new holder, which serves what the old birth stored there; the old
/// exact NodeId never follows the path:
/// the probe's current-only resolver answers Unknown (NotSent), a node's own retained departure
/// resolver answers Gone for it, Found (the replacement) for the path, Unknown for an id never
/// seen. What must NOT happen: the replacement starting over another data dir, `exact:<old>`
/// reaching the replacement, or any handler success at the old birth after its replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proof_replacement_inherits_the_data_dir_exact_old_never_follows_path() {
    let cell = "proof_replacement_inherits_the_data_dir_exact_old_never_follows_path";
    let dir = acceptance_dir(cell);
    let (mut estate, build) = estate(cell).await;
    let path = format!("path:{NODE}");
    let first = view(&estate, NODE).await;
    let exact_old = format!("exact:{}", s(&first["node_id"]));
    let first_dir = estate.node_view(NODE).await.data_dir.expect("the launching admin advertises the node's data dir");
    let put = estate.probe(&["put", "--target", &path, "--key", "41", "--value", "old-birth"]);
    landed_on(&put, &first, "put");
    let build_id = build.0.clone();
    let attempt_before = u64::from(estate.builds_obj().await.get(&build).await.expect("build.get of the accepted Build").attempt);

    // The runtime dies (a fault); its mesh-primary answers it with node recovery.
    estate.kill_node(NODE).await;
    let replacement = estate
        .until_node(NODE, "a new logical node holds the path", Duration::from_secs(120), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready-for-traffic" && n["node_id"] != first["node_id"]).then_some(n)
        })
        .await;
    let replacement_dir = estate.node_view(NODE).await.data_dir.expect("the launching admin advertises the node's data dir");
    let fresh = estate.probe(&["get", "--target", &path, "--key", "41"]);
    landed_on(&fresh, &replacement, "get");
    assert_eq!(replacement_dir, first_dir, "node recovery creates the new node over the old birth's data dir");
    assert_eq!(fresh["reply"]["result"], json!({"found": true, "value": "old-birth"}), "the new node serves what the old birth stored in the inherited data dir: {fresh}");
    let old = estate.probe(&["get", "--target", &exact_old, "--key", "41"]);
    // The probe also prints its own traceparent; the verdict is the outcome, reason and route.
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> never follows a replacement: {old}");
    let other = format!("path:{OTHER}");
    let gone = wait_for("the other rpc node holds the old birth's departure", Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", &other, "--query", &exact_old]);
        (r["reply"]["resolution"] == "gone").then_some(r)
    })
    .await;
    let holder = estate.probe(&["resolve", "--target", &other, "--query", &path]);
    assert_eq!(holder["reply"]["resolution"], "found", "{holder}");
    assert_eq!(holder["reply"]["node_id"], replacement["node_id"], "the path's current holder is the replacement: {holder}");
    let never = estate.probe(&["resolve", "--target", &other, "--query", &format!("exact:{}", rafka_mesh_entity::NodeId::mint())]);
    assert_eq!(never["reply"]["resolution"], "unknown", "{never}");
    estate.stop().await;

    let spans = estate.spans();
    // Node recovery: the next attempt of the accepted Build (reason recovery), deleting the exited
    // birth and creating this path again, then updating it (node-recovery.md steps).
    let attempt_of = |r: &Value| r["attributes"]["attempt"].as_u64().or_else(|| r["attributes"]["attempt"].as_str().and_then(|a| a.parse().ok())).unwrap_or(0);
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == build_id.as_str() && attempt_of(r) > attempt_before).collect();
    let replace = reconciles
        .iter()
        .find(|r| r["attributes"]["reason"] == "recovery" && [format!("retire-node:{NODE}"), format!("create-node:{NODE}"), format!("update-node:{NODE}")].iter().all(|op| s(&r["attributes"]["operations"]).contains(op.as_str())))
        .cloned()
        .unwrap_or_else(|| panic!("a recovery attempt deleting, creating and updating {NODE}: {reconciles:?}"));
    let recovery = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .find(|d| d["attributes"]["node"] == NODE && d["attributes"]["build_id"] == build_id.as_str() && s(&d["attributes"]["outcome"]).starts_with("complete"))
        .cloned()
        .unwrap_or_else(|| panic!("node recovery of {NODE} completed under the accepted Build"));
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request");
    let stored = served.iter().find(|s| s["attributes"]["incarnation_id"] == first["incarnation_id"] && s["attributes"]["op"] == "put" && s["attributes"]["outcome"] == "stored").cloned().expect("the old birth stored the put");
    let inherited = served.iter().find(|s| s["attributes"]["node_id"] == replacement["node_id"] && s["attributes"]["op"] == "get" && s["attributes"]["outcome"] == "value").cloned().expect("the new node served the value the old birth stored");
    let replace_start = replace["start_unix_nano"].as_u64().unwrap_or(0);
    let old_after: Vec<&&Value> = served.iter().filter(|s| s["attributes"]["node_id"] == first["node_id"] && s["start_unix_nano"].as_u64().unwrap_or(0) > replace_start).collect();
    assert!(old_after.is_empty(), "no handler ran at the old birth after its replacement: {old_after:?}");

    let result = json!({
        "cell": cell,
        "old": birth(&first),
        "replacement": birth(&replacement),
        "data_dir": {"old": first_dir, "replacement": replacement_dir},
        "put": put["reply"],
        "fresh": fresh["reply"],
        "exact_old_probe": old,
        "resolver": {"old": gone["reply"], "path": holder["reply"], "never_seen": never["reply"]},
        "replace_attempt": {"build_id": build_id, "attempt": replace["attributes"]["attempt"], "reason": replace["attributes"]["reason"], "action": replace["attributes"]["action"], "operations": replace["attributes"]["operations"], "trace_id": replace["trace_id"], "span_id": replace["span_id"], "node_recovery": {"span_id": recovery["span_id"], "outcome": recovery["attributes"]["outcome"]}},
        "spans": {
            "stored": {"trace_id": stored["trace_id"], "span_id": stored["span_id"], "parent_span_id": stored["parent_span_id"]},
            "inherited": {"trace_id": inherited["trace_id"], "span_id": inherited["span_id"], "parent_span_id": inherited["parent_span_id"]},
        },
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
