//! product=mesh, feature=node-lifecycle, subfeature=provider-state, rung=MN, provider=process.
//!
//! i144 e2 (deployment-provider.md §2.5; node-recovery.md steps 3-4; node-tickle.md step 1; R-PV1):
//! what the mesh does with the provider's answer for a member whose runtime the operating system
//! holds suspended. The mesh-primary records the `os-stopped` fact on the birth's status row,
//! surfaced on `node.get`, opens no hold and terminates nothing; it inspects the exact runtime on
//! every tick, clears the fact when the runtime runs again and takes the ordinary exit proof when it
//! exits.

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::Value;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "provider-state".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Whether span `sp` names Build `bare` (its bare id): step spans carry the `bid_xxxx-xxxx-xxxx`
/// rendering (`BuildId::log`), command spans the bare id.
fn same_build(sp: &Value, bare: &str) -> bool {
    let a = attr(sp, "build_id");
    a == bare || a.strip_prefix("bid_").is_some_and(|l| l.replace('-', "") == bare)
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// mesh1 {1 node-admin, 2 rpc nodes}, settled and ready for traffic; the rpc member that holds no
/// seat (its stop moves no seat first) and its exact runtime.
async fn mesh_with_a_member(test: &str, env: &[(&str, &str)]) -> (Estate, String, ExactRuntime) {
    let estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", env).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let member = estate.node_values().await.into_iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && n["is_primary"] != true).map(|n| s(&n["name"])).min().expect("an rpc member that holds no seat");
    let dir = estate.data_dir(&member).await;
    let rt = ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{member}: no exact runtime published in {dir}: {r:?}"));
    (estate, member, rt)
}

async fn node_where(estate: &Estate, node: &str, what: &str, within: Duration, f: impl Fn(&Value) -> bool) -> Value {
    wait_for(what, within, || async { estate.node_value_opt(node).await.filter(|n| f(n)) }).await
}

/// CONTRACT (deployment-provider.md §2.5; node-recovery.md step 3; R-PV1): a member the operating
/// system holds suspended (SIGSTOP, acknowledged `T`) goes silent; its mesh-primary's ladder ends in
/// the provider's inspection, verdict `os-stopped`, and `node.get` lists the same birth with
/// `os_stopped { since, inspections }` and no `held_running`. Held past two hung-runtime hold
/// windows (`RDM_HUNG_HOLD_FLOORS=2`) it is still the same birth: no hold, no `TerminateRuntime`, no
/// recovery. Continued (SIGCONT), it is heard again: `os_stopped` is gone from `node.get` and the
/// same birth is ready for traffic. Spans: `via-provider-inspection` (os-stopped, by mesh1.admin.1),
/// `via-os-stopped` recorded then cleared (returned, reached or running). Must NOT happen: `held-running` while os-stopped, a
/// `TerminateRuntime` step, a recovery submit or a new birth at the path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_member_is_recorded_on_node_get_and_cleared_when_it_runs_again() {
    let test = "an_os_stopped_member_is_recorded_on_node_get_and_cleared_when_it_runs_again";
    let (mut estate, member, rt) = mesh_with_a_member(test, &[("RDM_HUNG_HOLD_FLOORS", "2")]).await;
    let before = estate.node_value(&member).await;
    let held = rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    assert_eq!(held.state_after, Some('T'), "the OS holds the runtime: {held:?}");
    let floor = rdm_transport::membership::staleness_floor();
    let recorded = node_where(&estate, &member, &format!("{member} lists os_stopped on node.get"), crate::node_recovery_due(), |n| n["os_stopped"]["since"].as_u64().unwrap_or(0) > 0).await;
    assert_eq!((&recorded["node_id"], &recorded["incarnation_id"]), (&before["node_id"], &before["incarnation_id"]), "the same birth: {recorded}");
    assert!(recorded["held_running"].is_null(), "no hold for an os-stopped runtime: {recorded}");
    // Two hold windows later: the same birth, still os-stopped, nothing held or terminated.
    tokio::time::sleep(floor * 2 * 2 + floor).await;
    let later = estate.node_value(&member).await;
    assert_eq!((&later["node_id"], &later["incarnation_id"]), (&before["node_id"], &before["incarnation_id"]), "no new birth at the path: {later}");
    assert!(later["held_running"].is_null() && later["os_stopped"]["inspections"].as_u64().unwrap_or(0) >= 2, "re-inspected each tick, still os-stopped and not held: {later}");
    let continued_at = now_ns();
    rt.apply(Fault::Continue).expect("the runtime is continued");
    let resumed = node_where(&estate, &member, &format!("{member} back ready with os_stopped cleared"), floor * 4 + Duration::from_secs(15), |n| n["status"] == "ready-for-traffic" && n["os_stopped"].is_null()).await;
    assert_eq!((&resumed["node_id"], &resumed["incarnation_id"]), (&before["node_id"], &before["incarnation_id"]), "the same birth runs on: {resumed}");

    estate.stop().await;
    let spans = estate.spans();
    let of_birth = |name: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let inspected = of_birth("rdm.node_admin.node.resolve.via-provider-inspection");
    assert!(inspected.iter().any(|sp| attr(sp, "verdict") == "os-stopped" && attr(sp, "inspector") == "mesh1.admin.1"), "the mesh-primary's inspection names os-stopped: {inspected:#?}");
    let facts: Vec<(String, String)> = of_birth("rdm.node_admin.node.update.via-os-stopped").iter().map(|sp| (attr(sp, "outcome"), attr(sp, "detail"))).collect();
    assert!(facts.len() == 2 && facts[0] == ("recorded".to_string(), String::new()) && facts[1].0 == "cleared" && ["returned", "reached", "running"].contains(&facts[1].1.as_str()), "recorded once, cleared when it runs again (its digest returned, it answered its tickle, or its inspection read it running): {facts:?}");
    // While os-stopped nothing is held; once continued, an inspection that reads it running before its
    // digest returns opens the ordinary hold, which its returning digest clears (resumed: held_running null).
    let held: Vec<Value> = of_birth("rdm.node_admin.node.update.via-held-running").into_iter().filter(|sp| start_ns(sp) < continued_at).collect();
    assert!(held.is_empty(), "no held-running verdict while the runtime was os-stopped: {held:#?}");
    assert!(resumed["held_running"].is_null(), "nothing is held once it runs again: {resumed}");
    assert!(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().all(|sp| !(attr(sp, "step") == "TerminateRuntime" && attr(sp, "node") == member)), "no TerminateRuntime of {member}");
    assert!(of_birth("rdm.node_admin.build.update.via-node-recovery").is_empty(), "no recovery submit");
    let trace = inspected.iter().find(|sp| attr(sp, "verdict") == "os-stopped").map(|sp| s(&sp["trace_id"])).unwrap_or_default();
    println!("os-stopped inspection: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// CONTRACT (deployment-provider.md §2.5: "`Exited` is the ordinary exit proof"): a member recorded
/// `os-stopped` is then killed (SIGKILL of the suspended runtime). The next inspection reads it
/// exited: the `os-stopped` fact clears (`cleared`, `exited`), the exit proof is recorded and the
/// ordinary recovery runs: a new birth (new node.id) at the same path.name, ready for traffic.
/// Must NOT happen: an exit inferred from the os-stopped fact itself, or a recovery before the exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_member_that_exits_gets_its_exit_proof_and_ordinary_recovery() {
    let test = "an_os_stopped_member_that_exits_gets_its_exit_proof_and_ordinary_recovery";
    let (mut estate, member, rt) = mesh_with_a_member(test, &[]).await;
    let before = estate.node_value(&member).await;
    rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    node_where(&estate, &member, &format!("{member} lists os_stopped on node.get"), crate::node_recovery_due(), |n| n["os_stopped"]["since"].as_u64().unwrap_or(0) > 0).await;
    let killed_at = now_ns();
    assert!(rt.apply(Fault::Kill).expect("the suspended runtime is killed").exited);
    let recovered = node_where(&estate, &member, &format!("{member} recovered as a new birth"), crate::node_recovery_due() * 2, |n| n["status"] == "ready-for-traffic" && n["node_id"] != before["node_id"] && n["os_stopped"].is_null()).await;
    assert_ne!(recovered["node_id"], before["node_id"], "a new node.id at the path: {recovered}");

    estate.stop().await;
    let spans = estate.spans();
    let of_birth = |name: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let facts: Vec<(String, String)> = of_birth("rdm.node_admin.node.update.via-os-stopped").iter().map(|sp| (attr(sp, "outcome"), attr(sp, "detail"))).collect();
    assert_eq!(facts, vec![("recorded".to_string(), String::new()), ("cleared".to_string(), "exited".to_string())], "recorded, then cleared by the exit");
    let exited = of_birth("rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| attr(sp, "verdict") == "exited").expect("the inspection proves the exit");
    assert!(start_ns(&exited) >= killed_at, "the exit is proven after the kill, never from the os-stopped fact");
    let submit = of_birth("rdm.node_admin.build.update.via-node-recovery").into_iter().next().expect("the ordinary recovery is submitted");
    assert!(start_ns(&submit) >= start_ns(&exited), "the recovery follows the exit proof");
    println!("exit proof: http://192.168.68.99:16687/trace/{}", s(&exited["trace_id"]));
    estate.record_trace_url(&s(&exited["trace_id"]));
}

/// CONTRACT (deployment-provider.md §2.5 and §3: "A `node.delete` of a target whose provider state is
/// `OsStopped` skips the stop by name and begins at `TerminateRuntime`: the stop cannot execute, and
/// the delete was explicitly commanded"; node-delete.md "OS-suspended target"): a member the operating
/// system holds suspended (recorded `os-stopped` on `node.get`, silent to every view) is deleted by an
/// explicit `node.delete`: the fabric-primary accepts it, the executor inspects the exact runtime at
/// the delete's instant (`via-provider-inspection`, caller `delete`, verdict `os-stopped`), names the
/// skipped stop (`rdm.node_admin.node.delete.via-os-stopped`), runs `TerminateRuntime` on the exact
/// handle, takes the exit proof, writes NodeDeleted and removes the `os-stopped` row (`cleared`,
/// `deleted`). `node.get` lists the path no more and the suspended process is gone. Must NOT happen: a
/// `node-not-live` refusal of the delete, a `stop-node` sent to the suspended birth (a StopNode or
/// AwaitNodeLeft step of the delete), the process continued, or a recovery of the deleted birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_delete_of_an_os_stopped_birth_skips_the_stop_by_name_and_terminates_the_exact_runtime() {
    let test = "an_explicit_delete_of_an_os_stopped_birth_skips_the_stop_by_name_and_terminates_the_exact_runtime";
    let (mut estate, member, rt) = mesh_with_a_member(test, &[]).await;
    let before = estate.node_value(&member).await;
    let held = rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    assert_eq!(held.state_after, Some('T'), "the OS holds the runtime: {held:?}");
    node_where(&estate, &member, &format!("{member} lists os_stopped on node.get"), crate::node_recovery_due(), |n| n["os_stopped"]["since"].as_u64().unwrap_or(0) > 0).await;
    let asked_at = now_ns();
    let path: rdm_core::PathName = member.parse().unwrap();
    let mut removal = estate.nodes_obj().await.delete(&path).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of the os-stopped {member}: {e}"));
    let build_id = Estate::complete(&mut removal).await.build_id.to_string();
    estate.settled_views(&[("mesh1", 1, 1)]).await;
    assert!(estate.node_value_opt(&member).await.is_none(), "node.get lists {member} no more");
    assert!(rt.check().is_err(), "the suspended runtime is gone after the delete's terminate");

    estate.stop().await;
    let spans = estate.spans();
    let steps: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| same_build(sp, &build_id) && attr(sp, "node") == member).map(|sp| format!("{}={}", attr(sp, "step"), attr(sp, "outcome"))).collect();
    assert!(steps.iter().all(|st| !st.starts_with("StopNode=") && !st.starts_with("AwaitNodeLeft=")), "no stop step of the delete: {steps:?}");
    assert!(steps.contains(&"TerminateRuntime=complete".to_string()) && steps.contains(&"NodeDeleted=complete".to_string()), "the delete terminates and deletes: {steps:?}");
    let inspected = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| attr(sp, "caller") == "delete" && sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the delete inspects the exact runtime at its instant");
    assert_eq!(attr(&inspected, "verdict"), "os-stopped", "{inspected}");
    let skipped = named(&spans, "rdm.node_admin.node.delete.via-os-stopped").into_iter().find(|sp| same_build(sp, &build_id) && sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the skipped stop is named");
    assert!(start_ns(&skipped) >= asked_at, "the skip follows the request");
    let sent: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().filter(|sp| same_build(sp, &build_id) && attr(sp, "node") == member).collect();
    assert!(sent.is_empty(), "no command was sent to the suspended birth: {sent:#?}");
    let facts: Vec<(String, String)> = named(&spans, "rdm.node_admin.node.update.via-os-stopped").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).map(|sp| (attr(sp, "outcome"), attr(sp, "detail"))).collect();
    assert_eq!(facts.last(), Some(&("cleared".to_string(), "deleted".to_string())), "the delete removes the os-stopped row: {facts:?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "no recovery of the deleted birth");
    let trace = s(&skipped["trace_id"]);
    println!("os-stopped delete: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
