//! i143.e7.s3 acceptance (rafka-v2 #2775), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2775-process`, which exports `I143_ACCEPTANCE_DIR` (each
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it, feature `i143-2775`).
//!
//! The testkit testkit store (op 0x70) on a real estate, driven only through the `node` objects and
//! `rdm-rpc-probe` over real Node RPC: a restart is a Build the rectifier executes and keeps the
//! node.id and the incarnation (node-restart.md: stop then start on the stopped process); the new
//! birth is node recovery (node-recovery.md), the mesh-primary's answer to the runtime's death (its
//! own workflow, never a Build, Q342: a new node.id at the same (path, name)).

use rdm_test::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

fn node() -> rdm_core::PathName { rdm_core::PathName::new("mesh1.node", "1").unwrap() }
fn other() -> rdm_core::PathName { rdm_core::PathName::new("mesh1.node", "2").unwrap() }
fn at_node(sp: &Value, location: &rdm_core::PathName) -> bool { sp["attributes"]["node_path"] == location.path && sp["attributes"]["node_name"] == location.name }

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2775".into(),
        subfeature: "testkit-store".into(),
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
    assert_eq!((&r["path"], &r["name"]), (&node["path"], &node["name"]), "{out}");
    assert_eq!(r["mesh"], rdm_test::estate::mesh_of(&node), "{out}");
    assert_eq!(r["incarnation_id"], node["incarnation"], "{out}");
    assert_eq!(r["op"], op, "{out}");
}

/// mesh1 with one node-admin and two nodes, through the rectifier: the estate and the Build that
/// accepted the shape.
async fn estate(cell: &str) -> (Estate, rdm_client::BuildId) {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let accepted = estate.resize_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    // The initial fabric refuses a node restart until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    (estate, accepted.build_id)
}

/// `node.get` of `name`, as the JSON the cell records.
async fn view(estate: &Estate, name: &rdm_core::PathName) -> Value {
    serde_json::to_value(estate.node_view(name).await).expect("a view serializes")
}

fn birth(n: &Value) -> Value {
    json!({"node": n["name"], "node_id": n["node_id"], "incarnation_id": n["incarnation"], "mesh": rdm_test::estate::mesh_of(&n)})
}

/// CONTRACT (#2775): a restart of the logical node holding the testkit store, a `node.restart` call to the
/// fabric-primary (Node RPC -> reconcile -> node.update.via-build), is stop then start on the stopped
/// process: it keeps its NodeId, incarnation and data dir and serves the value written before it, the
/// Put and the Get after the restart each naming the executing birth exactly as the node-admin view
/// holds it. A holder of its type's role has the role moved first and is then restarted (node-stop.md:
/// "A target that is a primary hands over first by node.primary.update"). What must
/// NOT happen: the restarted node answering absent, or the restart reaching the node other than
/// through the Build it was accepted as.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn testkit_store_node_restarts_preserves_same_node_store() {
    let cell = "testkit_store_node_restarts_preserves_same_node_store";
    let dir = acceptance_dir(cell);
    let (mut estate, _) = estate(cell).await;
    let path = format!("path:{}", serde_json::to_string(&node()).unwrap());
    let first = view(&estate, &node()).await;
    let exact = format!("exact:{}", s(&first["node_id"]));
    let first_dir = estate.node_view(&node()).await.storage.data_dir.expect("the launching admin advertises the node's data dir");
    let put = estate.probe(&["put", "--target", &path, "--key", "41", "--value", "before-restart"]);
    landed_on(&put, &first, "put");
    let before = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    landed_on(&before, &first, "get");
    assert_eq!(before["reply"]["result"], json!({"found": true, "value": "before-restart"}), "presence control: {before}");

    // Whether the restarted node holds its type's role: then the role is moved first.
    let is_primary = view(&estate, &node()).await["is_primary"] == true;
    // The restart: node.stop then node.start (Q183).
    // Node commands of the primary above the node, never a Build or part of one (Q342).
    estate.stop_then_start(&node()).await;
    let restarted = view(&estate, &node()).await;
    assert_eq!(restarted["state"], "ready", "the restarted birth is ready: {restarted}");
    assert_eq!(restarted["node_id"], first["node_id"], "a restart keeps the logical node");
    assert_eq!(restarted["incarnation"], first["incarnation"], "a restart is node.stop then node.start on the stopped process: the same birth (ops-naming-proposal.md, Restart is not a re-birth)");
    let restarted_dir = estate.node_view(&node()).await.storage.data_dir.expect("the launching admin advertises the node's data dir");
    assert_eq!(restarted_dir, first_dir, "a restart keeps the node's data dir");
    let kept = estate.probe(&["get", "--target", &exact, "--key", "41"]);
    landed_on(&kept, &restarted, "get");
    assert_eq!(kept["reply"]["result"], json!({"found": true, "value": "before-restart"}), "the restarted node serves the old value: {kept}");
    let store_file = std::fs::read_dir(&restarted_dir).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).find(|p| p.file_name().is_some_and(|f| f.to_string_lossy().contains("testkit-store"))).map(|p| p.display().to_string());
    estate.stop().await;

    let spans = estate.spans();
    let served = named(&spans, "rdm.node_rpc.testkit_store.serve.via-request");
    let at = |inc: &Value, op: &str, outcome: &str| served.iter().find(|s| s["attributes"]["node_id"] == first["node_id"] && s["attributes"]["incarnation_id"] == *inc && s["attributes"]["op"] == op && s["attributes"]["outcome"] == outcome).cloned().cloned();
    let put_span = at(&first["incarnation"], "put", "stored").expect("the first birth served the put");
    let kept_span = at(&restarted["incarnation"], "get", "value").expect("the restarted birth served the kept value");
    // The stop ran as the Build its node.stop call was accepted as: the call, then the rectifier's
    // node update of this path under the same Build. A stop of its type's primary moves the role
    // first (node-stop.md: "A target that is a primary hands over first by node.primary.update"), and
    // the stop runs after it.
    let node_id = s(&first["node_id"]);
    let command = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().find(|sp| sp["attributes"]["command"] == "stop-node" && s(&sp["attributes"]["subject_id"]) == node_id && sp["attributes"]["outcome"] == "node-stopped").cloned().expect("the stop's node command span at the primary above the node");
    let update = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| at_node(sp, &node()) && sp["attributes"]["step"] == "StopNode" && descends_from(&spans, sp, &command)).cloned().expect("the stop's StopNode step under its node command");
    let role_move = if is_primary {
        let moved = named(&spans, "rdm.node_admin.primary.update.via-command")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["primary_node_id"]) == node_id && descends_from(&spans, sp, &command))
            .cloned()
            .unwrap_or_else(|| panic!("the selected node held its type's role: its stop moves it first, under the stop's node command"));
        json!({"operation": moved["attributes"]["operation"], "span_id": moved["span_id"]})
    } else {
        Value::Null
    };
    // The restart is node.stop then node.start on the stopped process: the Get after it is served by the incarnation that served the Put.
    assert_eq!(kept_span["attributes"]["incarnation_id"], put_span["attributes"]["incarnation_id"], "the restarted node is the same birth");

    let result = json!({
        "cell": cell,
        "first": birth(&first),
        "restarted": birth(&restarted),
        "data_dir": {"before": first_dir, "after": restarted_dir, "store_file": store_file},
        "put": put["reply"],
        "presence_control": before["reply"],
        "kept": kept["reply"],
        "is_primary": is_primary,
        "role_move": role_move,
        "spans": {
            "put": {"trace_id": put_span["trace_id"], "span_id": put_span["span_id"], "parent_span_id": put_span["parent_span_id"]},
            "kept": {"trace_id": kept_span["trace_id"], "span_id": kept_span["span_id"], "parent_span_id": kept_span["parent_span_id"]},
            "restart": {
                "node_command": {"span_id": command["span_id"], "trace_id": command["trace_id"]},
                "stop_step": {"span_id": update["span_id"], "trace_id": update["trace_id"]},
                "step_descends_from_node_command": descends_from(&spans, &update, &command),
            },
        },
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2775; node-recovery.md sequence steps 2-3; storage.md §1: "with `ephemeral` storage
/// ... on a new one"; Q358, Q359): the runtime's death is answered by node recovery, a new NodeId and
/// endpoint at the same (path, name) on a new data dir, a `node`'s storage being ephemeral: `path:`
/// reaches the new node, whose store is empty; the old exact NodeId never follows the path:
/// the probe's current-only resolver answers Unknown (NotSent), a node's own retained departure
/// resolver answers Gone for it, Found (the replacement) for the path, Unknown for an id never
/// seen. What must NOT happen: the replacement handed the old birth's data dir, `exact:<old>`
/// reaching the replacement, or any handler success at the old birth after its replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn testkit_store_replacement_starts_on_a_new_data_dir_exact_old_never_follows_path() {
    let cell = "testkit_store_replacement_starts_on_a_new_data_dir_exact_old_never_follows_path";
    let dir = acceptance_dir(cell);
    let (mut estate, build) = estate(cell).await;
    let path = format!("path:{}", serde_json::to_string(&node()).unwrap());
    let first = view(&estate, &node()).await;
    let exact_old = format!("exact:{}", s(&first["node_id"]));
    let first_dir = estate.node_view(&node()).await.storage.data_dir.expect("the launching admin advertises the node's data dir");
    let put = estate.probe(&["put", "--target", &path, "--key", "41", "--value", "old-birth"]);
    landed_on(&put, &first, "put");
    let build_id = build.to_string();

    // The runtime dies (a fault); its mesh-primary answers it with node recovery.
    estate.kill_node(&node()).await;
    let replacement = estate
        .until_node(&node(), "a new logical node holds the path", Duration::from_secs(120), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["state"] == "ready" && n["node_id"] != first["node_id"]).then_some(n)
        })
        .await;
    let replacement_dir = estate.node_view(&node()).await.storage.data_dir.expect("the launching admin advertises the node's data dir");
    let fresh = estate.probe(&["get", "--target", &path, "--key", "41"]);
    landed_on(&fresh, &replacement, "get");
    assert_ne!(replacement_dir, first_dir, "node recovery creates the new node on a new data dir: a `node`'s storage is ephemeral (Q359)");
    assert_ne!(replacement["endpoint_id"], first["endpoint_id"], "a new endpoint_id (Q358)");
    assert_eq!(fresh["reply"]["result"], json!({"found": false}), "the new node's store is empty: nothing is inherited: {fresh}");
    let old = estate.probe(&["get", "--target", &exact_old, "--key", "41"]);
    // The probe also prints its own traceparent; the verdict is the outcome, reason and route.
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> never follows a replacement: {old}");
    let other_target = format!("path:{}", serde_json::to_string(&other()).unwrap());
    estate.holds_departure(&other(), "mesh1", &s(&first["node_id"])).await;
    let gone = estate.probe(&["resolve", "--target", &other_target, "--query", &exact_old]);
    assert_eq!(gone["reply"]["resolution"], "gone", "the other node holds the old birth's departure: {gone}");
    let holder = estate.probe(&["resolve", "--target", &other_target, "--query", &path]);
    assert_eq!(holder["reply"]["resolution"], "found", "{holder}");
    assert_eq!(holder["reply"]["node_id"], replacement["node_id"], "the path's current primary is the replacement: {holder}");
    let never = estate.probe(&["resolve", "--target", &other_target, "--query", &format!("exact:{}", rdm_core::NodeId::mint())]);
    assert_eq!(never["reply"]["resolution"], "unknown", "{never}");
    estate.stop().await;

    let spans = estate.spans();
    // Node recovery: the mesh-primary's own workflow, never a Build or part of one (Q342), deleting the
    // exited birth and creating this path again under its submit, and nothing after (node-recovery.md
    // steps).
    let recovery = named(&spans, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .find(|d| d["attributes"]["node_id"] == first["node_id"] && s(&d["attributes"]["outcome"]).starts_with("complete"))
        .cloned()
        .unwrap_or_else(|| panic!("node recovery of the selected node completed"));
    let under = |name: &str| -> Vec<Value> {
        named(&spans, name).into_iter().filter(|sp| sp["parent_span_id"] == recovery["span_id"] && sp["attributes"]["node_id"] == first["node_id"] && sp["attributes"]["incarnation_id"] == first["incarnation"]).cloned().collect()
    };
    let (deletes, creates) = (under("rdm.node_admin.node.delete.via-node-recovery"), under("rdm.node_admin.node.create.via-node-recovery"));
    assert!(deletes.len() == 1 && creates.len() == 1, "one delete and one create of the exited birth under the recovery's submit: {deletes:?} {creates:?}");
    let replace = deletes[0].clone();
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["start_unix_nano"].as_u64() >= recovery["start_unix_nano"].as_u64() && s(&r["attributes"]["operations"]).contains(first["node_id"].as_str().unwrap_or("-"))).collect();
    assert!(ran.is_empty(), "no section of any Build ran the recovery (Q342): {ran:?}");
    let served = named(&spans, "rdm.node_rpc.testkit_store.serve.via-request");
    // The old birth's own serve span of the put is not evidence here: the cell SIGKILLs that process,
    // and spans still queued in its batch exporter die with it (rafka-mesh-telemetry init_telemetry:
    // a batch provider). The put's reply named the old birth as executor (`landed_on` above); the
    // caller's span is the put's trace.
    let stored = named(&spans, "rdm.node_rpc.testkit_store.resolve.via-probe").into_iter().find(|s| s["attributes"]["op"] == "put" && s["attributes"]["target"] == path.as_str()).cloned().expect("the probe's put call span");
    let inherited = served.iter().find(|s| s["attributes"]["node_id"] == replacement["node_id"] && s["attributes"]["op"] == "get" && s["attributes"]["outcome"] == "absent").cloned().expect("the new node answered the get, holding nothing of the old birth's");
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
        "recovery": {"build_id": build_id, "delete": {"trace_id": replace["trace_id"], "span_id": replace["span_id"]}, "create": {"span_id": creates[0]["span_id"], "created_node_id": creates[0]["attributes"]["created_node_id"]}, "node_recovery": {"span_id": recovery["span_id"], "outcome": recovery["attributes"]["outcome"]}},
        "spans": {
            "stored": {"trace_id": stored["trace_id"], "span_id": stored["span_id"], "parent_span_id": stored["parent_span_id"]},
            "inherited": {"trace_id": inherited["trace_id"], "span_id": inherited["span_id"], "parent_span_id": inherited["parent_span_id"]},
        },
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
