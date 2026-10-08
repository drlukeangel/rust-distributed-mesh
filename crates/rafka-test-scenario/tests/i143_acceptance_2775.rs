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

/// mesh1 with one node-admin and two rpc nodes, through the rectifier.
async fn estate(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 1, 2)], Duration::from_secs(30)).await;
    estate
}

fn birth(n: &Value) -> Value {
    json!({"node": n["name"], "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "mesh": n["mesh"]})
}

/// CONTRACT (#2775): a restart of the logical node holding the proof store, executed by the
/// rectifier (REST -> reconcile -> node.update.via-build), keeps its NodeId and data dir and comes
/// back as a new incarnation that serves the value written before it: the Put is answered by the
/// first birth, the Get after the restart by the new incarnation, each naming its executing birth
/// exactly as the node-admin view holds it. What must NOT happen: the restarted node answering
/// absent, a reply naming the old incarnation after the restart, or the restart reaching the node
/// other than through the Build it was accepted as.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proof_holder_restarts_preserves_same_node_store() {
    let cell = "proof_holder_restarts_preserves_same_node_store";
    let dir = acceptance_dir(cell);
    let mut estate = estate(cell).await;
    let path = format!("path:{NODE}");
    let first = estate.node(NODE).await;
    let exact = format!("exact:{}", s(&first["node_id"]));
    let first_dir = estate.data_dir_of(NODE).await;
    let put = estate.probe(&["put", "--target", &path, "--key", "41", "--value", "before-restart"]);
    landed_on(&put, &first, "put");
    let before = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    landed_on(&before, &first, "get");
    assert_eq!(before["reply"]["result"], json!({"found": true, "value": "before-restart"}), "presence control: {before}");

    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/restart"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    let restart_build = s(&r["build_id"]);
    estate.await_attempt(&restart_build, Estate::attempt_of(&r), Duration::from_secs(120)).await;
    let restarted = wait_for("the restarted birth is ready", Duration::from_secs(60), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != first["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(restarted["node_id"], first["node_id"], "a restart keeps the logical node");
    let restarted_dir = estate.data_dir_of(NODE).await;
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
    let accepted = named(&spans, "rdm.node_admin.build.update.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == restart_build.as_str() && sp["attributes"]["node"] == NODE).cloned().expect("the restart's REST span");
    let update = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| sp["attributes"]["build_id"] == restart_build.as_str() && sp["attributes"]["node"] == NODE).cloned().expect("the restart's node update");
    assert_eq!(update["attributes"]["attempt"], accepted["attributes"]["attempt"], "the node update runs the attempt the request opened");
    let reconcile = spans.iter().find(|sp| sp["span_id"] == update["parent_span_id"]).cloned().unwrap_or(Value::Null);
    let restart_start = update["start_unix_nano"].as_u64().unwrap_or(0);
    let late_old: Vec<&&Value> = served.iter().filter(|s| s["attributes"]["incarnation_id"] == first["incarnation_id"] && s["start_unix_nano"].as_u64().unwrap_or(0) > restart_start).collect();
    // A served call at the old incarnation after the restart began is one in flight before it, never a later one.
    assert!(late_old.iter().all(|s| s["attributes"]["op"] != "get" || s["start_unix_nano"].as_u64() < kept_span["start_unix_nano"].as_u64()), "nothing reached the first birth after the restart: {late_old:?}");

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

/// CONTRACT (#2775): a permanent replacement of the logical node (the rectifier's replace attempt
/// after the runtime's death) is a new NodeId at the same path.name with a fresh, empty store:
/// `path:` reaches the new holder and finds nothing; the old exact NodeId never follows the path:
/// the probe's current-only resolver answers Unknown (NotSent), a node's own retained departure
/// resolver answers Gone for it, Found (the replacement) for the path, Unknown for an id never
/// seen. What must NOT happen: the replacement serving the old value, `exact:<old>` reaching the
/// replacement, or any handler success at the old birth after its replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proof_replacement_starts_fresh_exact_old_never_follows_path() {
    let cell = "proof_replacement_starts_fresh_exact_old_never_follows_path";
    let dir = acceptance_dir(cell);
    let mut estate = estate(cell).await;
    let path = format!("path:{NODE}");
    let first = estate.node(NODE).await;
    let exact_old = format!("exact:{}", s(&first["node_id"]));
    let first_dir = estate.data_dir_of(NODE).await;
    let put = estate.probe(&["put", "--target", &path, "--key", "41", "--value", "old-birth"]);
    landed_on(&put, &first, "put");
    let (_, b) = estate.get("/api/fabric").await;
    let build_id = s(&b["build_id"]);
    let attempt_before = estate.get(&format!("/api/builds?id={build_id}")).await.1["attempt"].as_u64().unwrap_or(0);

    // The runtime dies (a fault); the rectifier answers it with a replace attempt.
    estate.kill_node(NODE).await;
    let replacement = wait_for("a new logical node holds the path", Duration::from_secs(120), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != first["node_id"]).then_some(n)
    })
    .await;
    let replacement_dir = estate.data_dir_of(NODE).await;
    let fresh = estate.probe(&["get", "--target", &path, "--key", "41"]);
    landed_on(&fresh, &replacement, "get");
    assert_eq!(fresh["reply"]["result"], json!({"found": false}), "a replacement starts with an empty store: {fresh}");
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
    // The rectifier's recovery: the next attempt of the accepted Build, opened on the proven
    // death (reason proven-drift), re-creating this path.
    let attempt_of = |r: &Value| r["attributes"]["attempt"].as_u64().or_else(|| r["attributes"]["attempt"].as_str().and_then(|a| a.parse().ok())).unwrap_or(0);
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == build_id.as_str() && attempt_of(r) > attempt_before).collect();
    let replace = reconciles
        .iter()
        .find(|r| r["attributes"]["reason"] == "proven-drift" && s(&r["attributes"]["operations"]).contains(&format!("create-node:{NODE}")))
        .cloned()
        .unwrap_or_else(|| panic!("a proven-drift attempt re-creating {NODE}: {reconciles:?}"));
    let drift = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().find(|d| d["attributes"]["build_id"] == build_id.as_str() && attempt_of(d) == attempt_of(replace)).cloned().unwrap_or(Value::Null);
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request");
    let stored = served.iter().find(|s| s["attributes"]["incarnation_id"] == first["incarnation_id"] && s["attributes"]["op"] == "put" && s["attributes"]["outcome"] == "stored").cloned().expect("the old birth stored the put");
    let absent = served.iter().find(|s| s["attributes"]["node_id"] == replacement["node_id"] && s["attributes"]["op"] == "get" && s["attributes"]["outcome"] == "absent").cloned().expect("the replacement served an empty store");
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
        "replace_attempt": {"build_id": build_id, "attempt": replace["attributes"]["attempt"], "reason": replace["attributes"]["reason"], "action": replace["attributes"]["action"], "operations": replace["attributes"]["operations"], "trace_id": replace["trace_id"], "span_id": replace["span_id"], "proven_drift": {"span_id": drift["span_id"], "scope": drift["attributes"]["scope"]}},
        "spans": {
            "stored": {"trace_id": stored["trace_id"], "span_id": stored["span_id"], "parent_span_id": stored["parent_span_id"]},
            "absent": {"trace_id": absent["trace_id"], "span_id": absent["span_id"], "parent_span_id": absent["parent_span_id"]},
        },
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
