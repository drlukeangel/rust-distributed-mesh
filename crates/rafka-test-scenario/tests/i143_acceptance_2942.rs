//! i143.e4.s18 acceptance (rafka-v2 #2942, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2942-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest and every process's spans land under it, feature `i143-2942`, test the
//! cell's name) at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate is the mesh leave of `mesh_replace`: {mesh1, mesh2} -> {mesh2}, owned by the
//! fabric-primary (a mesh2 admin). The last admin of mesh1 is the final mesh-primary the
//! fabric-primary drains and stops itself, whose exit its `TerminateRuntime` step proves; its own
//! leave is read from its leg spans.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// The unchanged linger (`RDM_LEAVE_LINGER_MS` default) and the unchanged observation bound.
const LINGER_MS: u64 = 1000;
const OBSERVE_BOUND_MS: u64 = 10_000;
const ANNOUNCEMENTS: usize = 5;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2942".into(),
        subfeature: "leave-legs".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2942/process").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

fn attr_u64(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_str().and_then(|v| v.parse().ok()).unwrap_or(u64::MAX)
}

fn mesh(name: &str) -> Value {
    json!({"name": name, "node_admin": 2, "rpc_node": 2})
}

fn names(meshes: &[&str]) -> BTreeSet<String> {
    meshes.iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")]).collect()
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

/// A mesh1 shutdown through the mesh leave, run to its end and read back: every span.
async fn retire_mesh1(test: &str) -> (String, Vec<Value>) {
    let (b, spans, _) = retire_mesh1_with(test, &[]).await;
    (b, spans)
}

/// What a faulted retirement armed on one admin.
struct Cut {
    admin: &'static str,
    id: &'static str,
    spec: Value,
}

/// A cut's door: the acknowledgement of the arm, and the door's last reading of the cut.
struct Fired {
    admin: &'static str,
    arm_ack: Value,
    after: Value,
}

/// A mesh1 retirement under `cut`, when there is one. With a cut, every admin of the estate is the
/// testkit's faulted node-admin; the cut is armed on its admin's door before the retire Build is
/// posted, and its door state is read after the retired runtime is gone.
async fn retire_mesh1_with(test: &str, cuts: &[Cut]) -> (String, Vec<Value>, Vec<Fired>) {
    let mut estate = if cuts.is_empty() {
        Estate::bootstrap(owner(test), "fabric1", "mesh1").await
    } else {
        let sha = candidate_sha();
        Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted")
    };
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled(&names(&["mesh1", "mesh2"]), Duration::from_secs(30)).await;
    // Each armed admin's door dies with its runtime: the last reading is the one taken while its
    // leave was running (the transport shutdown after the linger keeps the door up for seconds).
    let mut armed = Vec::new();
    for c in cuts {
        let api = before.iter().find(|n| n["name"] == c.admin).map(|n| s(&n["admin_api_base"])).unwrap_or_else(|| panic!("{} advertises its control API", c.admin));
        let d = Door::open(&estate.root, c.admin, &api).await;
        let arm_ack = d.arm(c.id, c.spec.clone()).await;
        let last = std::sync::Arc::new(std::sync::Mutex::new(Value::Null));
        let poller = {
            let (d, last) = (d.clone(), last.clone());
            tokio::spawn(async move {
                loop {
                    if let Ok(r) = d.http.get(format!("{}/faults", d.base)).send().await {
                        if let Ok(v) = r.json::<Value>().await {
                            *last.lock().unwrap() = v;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
        };
        armed.push((c, arm_ack, last, poller));
    }
    let mut pids: Vec<(String, u64)> = Vec::new();
    for n in before.iter().filter(|n| n["mesh"] == "mesh1") {
        let name = s(&n["name"]);
        let pid = match estate.bootstrap_pid() {
            Some(p) if name == "mesh1.admin.1" => u64::from(p),
            _ => estate.pid_of(&name).await,
        };
        pids.push((name, pid));
    }
    // Control moves to mesh2 before mesh1 goes.
    estate.admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")
        .map(|n| s(&n["admin_api_base"]))
        .expect("a ready mesh2 admin");
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    estate.await_build(&b, Duration::from_secs(240)).await;
    estate.settled(&names(&["mesh2"]), Duration::from_secs(60)).await;
    for (name, pid) in &pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }
    let (_, fabric_now) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric_now["admin_api_base"]);
    let fired: Vec<Fired> = armed
        .into_iter()
        .map(|(c, arm_ack, last, poller)| {
            poller.abort();
            let after = last.lock().unwrap()["cuts"][c.id].clone();
            Fired { admin: c.admin, arm_ack, after }
        })
        .collect();
    estate.stop().await;
    (b, estate.spans(), fired)
}

/// The last admin of mesh1 completes five `Leaving` announcements on the mesh channel and five on
/// the backbone inside the unchanged one-second linger, and the fabric-primary's TerminateRuntime
/// for that exact birth completes inside the unchanged 10 s.
///
/// CONTRACT: a retiring last admin must put every one of its linger announcements out, on both
/// channels, before the linger ends; the fabric-primary that stops it proves its exit by the
/// provider's exact inspection, never from silence or a gossip frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retiring_admin_completes_five_leaving_announcements_within_linger() {
    let cell = "retiring_admin_completes_five_leaving_announcements_within_linger";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (b, spans) = retire_mesh1(cell).await;

    // The last admin: the mesh1 admin whose exit the fabric-primary proved last under B.
    let observe: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.admin.")).collect();
    assert!(!observe.is_empty(), "the mesh leave proved a mesh1 admin's exit under {b}");
    let obs = *observe.iter().max_by_key(|sp| start(sp)).unwrap();
    let last = s(&obs["attributes"]["node"]);
    assert!(last.starts_with("mesh1.admin."), "the exit proved last is an admin's: {last}");

    // Executor side: the step completed inside the unchanged bound, a child of the pipeline run
    // for this node and Build in the same trace.
    let obs_elapsed = attr_u64(obs, "elapsed_ms");
    assert_eq!(obs["attributes"]["outcome"], "complete", "TerminateRuntime of {last} completed: {obs}");
    assert!(obs_elapsed < OBSERVE_BOUND_MS, "TerminateRuntime of {last} completed inside {OBSERVE_BOUND_MS} ms ({obs_elapsed} ms)");
    let pipeline = named(&spans, "rdm.node_admin.deployment.update.via-pipeline")
        .into_iter()
        .find(|sp| sp["span_id"] == obs["parent_span_id"] && sp["trace_id"] == obs["trace_id"])
        .unwrap_or_else(|| panic!("TerminateRuntime's parent is a pipeline span of its trace: {obs}"));
    assert_eq!(pipeline["attributes"]["node"], last.as_str());
    assert_eq!(pipeline["attributes"]["build_id"], b.as_str());

    // The retiring admin's own leave: the signal span and every leg beneath it.
    let mine = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node"] == last.as_str()).collect() };
    let signals: Vec<&Value> = named(&spans, "rdm.mesh.node.delete.via-signal");
    // A stop-node has no drain leg: the drain was the separate drain-node, so this leave says no Draining.
    let draining = mine("rdm.mesh.node.update.via-leave-draining");
    assert!(draining.is_empty(), "{last} said no Draining in its stop-node leave: {draining:?}");
    let shutdown = mine("rdm.mesh.node.update.via-leave-shutdown");
    assert_eq!(shutdown.len(), 1, "one transport shutdown by {last}: {shutdown:?}");
    let signal = signals.iter().find(|sp| sp["span_id"] == shutdown[0]["parent_span_id"] && sp["trace_id"] == shutdown[0]["trace_id"]).unwrap_or_else(|| panic!("the shutdown leg sits under the leave's signal span: {}", shutdown[0]));
    assert_eq!(shutdown[0]["attributes"]["outcome"], "closed");

    let all = mine("rdm.mesh.node.update.via-leave-announcement");
    let on = |ch: &str| -> Vec<&Value> {
        let mut v: Vec<&Value> = all.iter().copied().filter(|sp| sp["attributes"]["channel"] == ch).collect();
        v.sort_by_key(|sp| start(sp));
        v
    };
    let (mesh_ch, bb_ch, view_ch) = (on("mesh"), on("backbone"), on("view"));
    let t0 = start(mesh_ch.first().expect("the first mesh announcement"));
    let deadline = t0 + LINGER_MS * 1_000_000;
    let mut rows = Vec::new();
    for (ch, legs) in [("mesh", &mesh_ch), ("backbone", &bb_ch)] {
        assert_eq!(legs.len(), ANNOUNCEMENTS, "{last} completed {ANNOUNCEMENTS} announcements on the {ch} channel inside its linger, not {}: {legs:#?}", legs.len());
        let mut seen: BTreeSet<u64> = BTreeSet::new();
        for sp in legs {
            assert_eq!(sp["attributes"]["outcome"], "sent", "{ch} announcement sent: {sp}");
            assert_eq!(sp["parent_span_id"], signal["span_id"], "{ch} announcement sits under the leave: {sp}");
            assert!(end(sp) <= deadline, "{ch} announcement {} completed inside the {LINGER_MS} ms linger ({} ms after the first)", sp["attributes"]["announcement"], (end(sp).saturating_sub(t0)) / 1_000_000);
            seen.insert(attr_u64(sp, "announcement"));
            rows.push(json!({
                "channel": ch, "announcement": sp["attributes"]["announcement"], "start_unix_nano": start(sp), "end_unix_nano": end(sp),
                "after_first_ms": (start(sp).saturating_sub(t0)) / 1_000_000, "elapsed_ms": sp["attributes"]["elapsed_ms"], "outcome": sp["attributes"]["outcome"],
                "publishing": sp["attributes"]["publishing"], "members": sp["attributes"]["members"], "trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"],
            }));
        }
        assert_eq!(seen, (1..=ANNOUNCEMENTS as u64).collect::<BTreeSet<_>>(), "{ch}: announcements 1..={ANNOUNCEMENTS} each once");
    }
    // A backbone say counts only from a publishing seat: a non-publisher's `sent` is a no-op.
    assert!(bb_ch.iter().all(|sp| sp["attributes"]["publishing"] == "true"), "the last admin of the mesh is the backbone's publisher for it: {bb_ch:#?}");
    assert_eq!(view_ch.len(), ANNOUNCEMENTS, "the topology read before each backbone say ran {ANNOUNCEMENTS} times");

    let result = json!({
        "cell": cell,
        "build_id": b,
        "retiring_admin": last,
        "linger_ms": LINGER_MS,
        "leave": { "signal_span_id": signal["span_id"], "trace_id": signal["trace_id"], "start_unix_nano": start(signal), "end_unix_nano": end(signal) },
        "draining_legs": draining.len(),
        "announcements": rows,
        "topology_reads": view_ch.iter().map(|sp| json!({ "announcement": sp["attributes"]["announcement"], "elapsed_ms": sp["attributes"]["elapsed_ms"], "span_id": sp["span_id"] })).collect::<Vec<_>>(),
        "shutdown": { "start_unix_nano": start(shutdown[0]), "elapsed_ms": shutdown[0]["attributes"]["elapsed_ms"], "outcome": shutdown[0]["attributes"]["outcome"], "span_id": shutdown[0]["span_id"] },
        "executor": { "step": "TerminateRuntime", "node": last, "outcome": obs["attributes"]["outcome"], "elapsed_ms": obs_elapsed, "bound_ms": OBSERVE_BOUND_MS, "trace_id": obs["trace_id"], "span_id": obs["span_id"], "pipeline_span_id": pipeline["span_id"] },
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

// ---- the faulted retirements ---------------------------------------------------------------------
//
// The retiring last admin withholds named announcements at its own publish seam (the testkit's
// `leave-announcement` cut: the leave skips that publish call, so the frame is never offered to
// the channel). This models a lost announcement at the send seam; it is not pressure on a peer
// queue, which no door of the product or the testkit can apply without touching iroh-gossip.

/// A retirement whose mesh1 admins both withhold `announcements` on `channel` (`mesh`, `backbone`
/// or `any`) from their leaves. Whichever admin of mesh1 is retired last, its legs withheld are
/// exactly the armed ones, its door acknowledged the cut active and counted the publishes it
/// withheld, and the fabric-primary's TerminateRuntime for that exact birth still completed inside the
/// unchanged bound. The matched healthy control is cell 1 of this job (the same retirement
/// unarmed); a cut with nothing left to repair it is the retire Build's failure, which is Luke's
/// to rule and has no cell here.
async fn retire_with_lost_announcements(cell: &'static str, announcements: &[u32], channel: &'static str) {
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let spec = json!({"kind": "leave-announcement", "announcements": announcements, "channel": channel});
    let cuts = [Cut { admin: "mesh1.admin.1", id: "lose-leaving", spec: spec.clone() }, Cut { admin: "mesh1.admin.2", id: "lose-leaving", spec: spec.clone() }];
    let (b, spans, fired) = retire_mesh1_with(cell, &cuts).await;
    for f in &fired {
        assert_eq!(f.arm_ack["armed"], "lose-leaving", "the door of {} acknowledged the arm: {}", f.admin, f.arm_ack);
    }

    let observe: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.admin.")).collect();
    let obs = *observe.iter().max_by_key(|sp| start(sp)).expect("the mesh leave proved a mesh1 admin's exit");
    let last = s(&obs["attributes"]["node"]);
    assert!(last.starts_with("mesh1.admin."), "the exit proved last is an admin's: {last}");
    let obs_elapsed = attr_u64(obs, "elapsed_ms");
    assert_eq!(obs["attributes"]["outcome"], "complete", "TerminateRuntime of {last} completed: {obs}");
    assert!(obs_elapsed < OBSERVE_BOUND_MS, "TerminateRuntime of {last} completed inside {OBSERVE_BOUND_MS} ms ({obs_elapsed} ms)");

    let legs_of = |node: &str, ch: &str| -> Vec<&Value> {
        let mut v: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-leave-announcement").into_iter().filter(|sp| sp["attributes"]["node"] == node && sp["attributes"]["channel"] == ch).collect();
        v.sort_by_key(|sp| attr_u64(sp, "announcement"));
        v
    };
    let mut withheld_total = 0u64;
    let mut rows = Vec::new();
    for ch in ["mesh", "backbone"] {
        let legs = legs_of(&last, ch);
        assert_eq!(legs.len(), ANNOUNCEMENTS, "{last} reached all {ANNOUNCEMENTS} announcement legs on the {ch} channel: {legs:#?}");
        for (i, sp) in legs.iter().enumerate() {
            let n = i as u32 + 1;
            let lost = announcements.contains(&n) && (channel == "any" || channel == ch);
            assert_eq!(attr_u64(sp, "announcement"), u64::from(n));
            assert_eq!(sp["attributes"]["outcome"], if lost { "withheld" } else { "sent" }, "{ch} announcement {n} of {last}: {sp}");
            withheld_total += u64::from(lost);
            rows.push(json!({"channel": ch, "announcement": n, "outcome": sp["attributes"]["outcome"], "elapsed_ms": sp["attributes"]["elapsed_ms"], "span_id": sp["span_id"], "trace_id": sp["trace_id"]}));
        }
    }
    // The injection was active on the retired-last admin's door, and the door counted exactly the
    // publishes the leave spans show withheld.
    let door = fired.iter().find(|f| f.admin == last).unwrap_or_else(|| panic!("{last} was armed"));
    assert_eq!(door.after["held"], true, "the cut was active when last read: {}", door.after);
    assert!(!door.after["hit"].is_null(), "the cut names the first publish it withheld: {}", door.after);
    assert_eq!(door.after["seen_while_held"], withheld_total, "the door counted every publish the leave spans show withheld: {}", door.after);

    let result = json!({
        "cell": cell, "build_id": b, "retiring_admin": last, "cut": spec, "linger_ms": LINGER_MS,
        "models": "announcements withheld at the retiring admin's send seam; not peer-queue pressure in iroh-gossip",
        "arm_acks": fired.iter().map(|f| json!({"admin": f.admin, "ack": f.arm_ack})).collect::<Vec<_>>(),
        "door_last_reading": door.after, "withheld_publishes": withheld_total, "legs": rows,
        "executor": { "step": "TerminateRuntime", "node": last, "outcome": obs["attributes"]["outcome"], "elapsed_ms": obs_elapsed, "bound_ms": OBSERVE_BOUND_MS, "trace_id": obs["trace_id"], "span_id": obs["span_id"] },
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// Only the last of the five announcements gets out on either channel: it alone carries the
/// departure to the executor.
///
/// CONTRACT: a retiring last admin whose first four announcements are lost on both channels is
/// still observed departing, by its exact birth, inside the unchanged 10 s; the cut was active
/// (acknowledged by its door), and the admin's unarmed sibling left whole in the same retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retire_executor_observes_exact_admin_departure_after_peer_queue_cut() {
    retire_with_lost_announcements("retire_executor_observes_exact_admin_departure_after_peer_queue_cut", &[1, 2, 3, 4], "any").await;
}

/// Announcement five is the one lost; one through four carry the departure.
///
/// CONTRACT: the last announcement of the linger is not load-bearing: with it withheld on both
/// channels the executor still observes the exact birth depart inside the unchanged 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_observes_admin_departure_when_the_last_announcement_is_lost() {
    retire_with_lost_announcements("executor_observes_admin_departure_when_the_last_announcement_is_lost", &[5], "any").await;
}

/// The first announcement is the one lost.
///
/// CONTRACT: the first announcement of the linger is not load-bearing: with it withheld on both
/// channels the executor still observes the exact birth depart inside the unchanged 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_observes_admin_departure_when_the_first_announcement_is_lost() {
    retire_with_lost_announcements("executor_observes_admin_departure_when_the_first_announcement_is_lost", &[1], "any").await;
}

/// The backbone loses announcements one through four; the mesh channel is whole.
///
/// CONTRACT: the executor, in the peer mesh, hears the retiring admin's departure through the
/// backbone aggregate; with its first four backbone announcements withheld the fifth alone carries
/// it, and it is observed inside the unchanged 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_observes_admin_departure_when_the_first_four_backbone_announcements_are_lost() {
    retire_with_lost_announcements("executor_observes_admin_departure_when_the_first_four_backbone_announcements_are_lost", &[1, 2, 3, 4], "backbone").await;
}
