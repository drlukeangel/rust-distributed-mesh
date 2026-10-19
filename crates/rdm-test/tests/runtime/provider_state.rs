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
/// `os-stopped` is then killed (SIGKILL of the suspended runtime). The fact clears on the first
/// inspection after the kill that does not read the runtime suspended (§2.5: "`Running` again clears
/// the fact"): `exited`, or `running` while the killed thread group's tasks are still exiting (§2.6: a
/// process is gone only once every task of its thread group has exited). An inspection then reads it
/// exited, the exit proof is recorded and the ordinary recovery runs: a new birth (new node.id) at the
/// same path.name, ready for traffic. Must NOT happen: the fact cleared before the kill, an exit
/// inferred from the os-stopped fact itself, or a recovery before the exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_member_that_exits_gets_its_exit_proof_and_ordinary_recovery() {
    let test = "an_os_stopped_member_that_exits_gets_its_exit_proof_and_ordinary_recovery";
    let (mut estate, member, rt) = mesh_with_a_member(test, &[]).await;
    let before = estate.node_value(&member).await;
    rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    node_where(&estate, &member, &format!("{member} lists os_stopped on node.get"), crate::node_recovery_due(), |n| n["os_stopped"]["since"].as_u64().unwrap_or(0) > 0).await;
    let killed_at = now_ns();
    assert!(rt.apply(Fault::Kill).expect("the suspended runtime is killed").exited);
    let recovery_due = std::time::Instant::now() + crate::node_recovery_due() * 2;
    let recovered = node_where(&estate, &member, &format!("{member} recovered as a new birth"), recovery_due.saturating_duration_since(std::time::Instant::now()), |n| n["status"] == "ready-for-traffic" && n["node_id"] != before["node_id"] && n["os_stopped"].is_null()).await;
    assert_ne!(recovered["node_id"], before["node_id"], "a new node.id at the path: {recovered}");
    // Ready precedes the recovery attempt's terminal receipt. Observe the settled recovery before
    // teardown aborts the executor, using the time left in the original recovery bound.
    wait_for("the recovery reaches its terminal receipt", recovery_due.saturating_duration_since(std::time::Instant::now()), || async {
        estate.spans().into_iter().find(|sp| sp["name"] == "rdm.node_admin.build.update.via-node-recovery"
            && sp["attributes"]["node_id"] == before["node_id"] && attr(sp, "outcome").starts_with("complete"))
    }).await;

    estate.stop().await;
    let spans = estate.spans();
    let of_birth = |name: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let facts: Vec<(String, String, u64)> = of_birth("rdm.node_admin.node.update.via-os-stopped").iter().map(|sp| (attr(sp, "outcome"), attr(sp, "detail"), start_ns(sp))).collect();
    assert!(
        facts.len() == 2 && (facts[0].0.as_str(), facts[0].1.as_str()) == ("recorded", "") && facts[1].0 == "cleared" && ["exited", "running"].contains(&facts[1].1.as_str()) && facts[1].2 >= killed_at,
        "recorded, then cleared after the kill by its first inspection that does not read it suspended (exited, or running while its tasks still exit): {facts:?}"
    );
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
/// handle `Immediate` (no graceful wait: a suspended runtime runs no handler; i144 R23), takes the
/// exit proof and writes NodeDeleted; the `os-stopped` row is removed once
/// (`cleared`: `deleted`, or `exited` when the mesh-primary's tick reads the exit first). `node.get`
/// lists the path no more and the suspended process is gone. Must NOT happen: a
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
    // i144 R23 (Luke Q4 B): the suspended runtime cannot run a handler, so its terminate is Immediate:
    // no graceful wait (the stop grace is 8 s).
    let terminated: Vec<u64> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| same_build(sp, &build_id) && attr(sp, "node") == member && attr(sp, "step") == "TerminateRuntime").filter_map(|sp| attr(sp, "elapsed_ms").parse().ok()).collect();
    assert!(matches!(terminated.as_slice(), [ms] if *ms < 2_000), "the os-stopped runtime's TerminateRuntime takes no graceful wait: {terminated:?} ms");
    let inspected = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| attr(sp, "caller") == "delete" && sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the delete inspects the exact runtime at its instant");
    assert_eq!(attr(&inspected, "verdict"), "os-stopped", "{inspected}");
    let skipped = named(&spans, "rdm.node_admin.node.delete.via-os-stopped").into_iter().find(|sp| same_build(sp, &build_id) && sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the skipped stop is named");
    assert!(start_ns(&skipped) >= asked_at, "the skip follows the request");
    let sent: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().filter(|sp| same_build(sp, &build_id) && attr(sp, "node") == member).collect();
    assert!(sent.is_empty(), "no command was sent to the suspended birth: {sent:#?}");
    let facts: Vec<(String, String)> = named(&spans, "rdm.node_admin.node.update.via-os-stopped").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).map(|sp| (attr(sp, "outcome"), attr(sp, "detail"))).collect();
    // The row's writer removes it once: the delete after its exit proof (`deleted`), or its own recovery
    // tick when that tick reads the terminated runtime `Exited` first (`exited`).
    let cleared: Vec<&(String, String)> = facts.iter().filter(|(outcome, _)| outcome == "cleared").collect();
    assert!(cleared.len() == 1 && ["deleted", "exited"].contains(&cleared[0].1.as_str()), "the os-stopped row is removed once, by the delete or the exit it proved: {facts:?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "no recovery of the deleted birth");
    let trace = s(&skipped["trace_id"]);
    println!("os-stopped delete: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

// ---- i144 e4: the provider state across a seat hand-over and an admin's re-birth ----

/// mesh1 {1 node-admin, 1 rpc node}, mesh2 {2 node-admins, 2 rpc nodes}, settled, the initial fabric
/// first ready for traffic. mesh2 holds no fabric seat: its seat moves between its own two admins.
async fn two_meshes(test: &str, env: &[(&str, &str)]) -> Estate {
    let estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", env).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate
}

/// The primary admin of `mesh` and its other admin, as `node.get` lists them.
async fn admins_of(estate: &Estate, mesh: &str) -> (String, String) {
    let nodes = estate.node_values().await;
    let admins: Vec<&Value> = nodes.iter().filter(|n| n["mesh"] == mesh && n["kind"] == "node_admin").collect();
    let primary = admins.iter().find(|n| n["is_primary"] == true).map(|n| s(&n["name"])).unwrap_or_else(|| panic!("{mesh} lists a primary admin: {admins:?}"));
    let other = admins.iter().map(|n| s(&n["name"])).find(|n| *n != primary).unwrap_or_else(|| panic!("{mesh} lists a second admin: {admins:?}"));
    (primary, other)
}

/// `mesh`'s rpc members, each with its exact runtime suspended by the operating system (SIGSTOP,
/// acknowledged `T`) and recorded `os-stopped` on `node.get` by the mesh's primary.
async fn suspended_members(estate: &Estate, mesh: &str) -> Vec<(String, Value, ExactRuntime)> {
    let names: Vec<String> = estate.node_values().await.into_iter().filter(|n| n["mesh"] == mesh && n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    let mut out = Vec::new();
    for name in names {
        let dir = estate.data_dir(&name).await;
        let rt = ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{name}: no exact runtime published in {dir}: {r:?}"));
        let before = estate.node_value(&name).await;
        let held = rt.apply(Fault::Stop).expect("the exact runtime is suspended");
        assert_eq!(held.state_after, Some('T'), "{name}: the OS holds the runtime: {held:?}");
        out.push((name, before, rt));
    }
    for (name, _, _) in &out {
        node_where(estate, name, &format!("{name} lists os_stopped on node.get"), crate::node_recovery_due(), |n| n["os_stopped"]["since"].as_u64().unwrap_or(0) > 0).await;
    }
    out
}

/// Every span the process `pid` wrote: the evidence files are named `<service>.<pid>-<pid namespace>`.
fn spans_of(estate: &Estate, pid: u64) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".spans.jsonl") && name.contains(&format!(".{pid}-")) {
            out.extend(std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    out
}

/// `node.restart` of the admin `admin`, completed: a seat holder's seat moves first (node.primary.update),
/// then its own stop and start, the same birth on the same process.
async fn restart(estate: &Estate, admin: &str) {
    let path: rdm_core::PathName = admin.parse().unwrap();
    let mut stream = estate.nodes_obj().await.restart(&path).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.restart of {admin}: {e}"));
    Estate::complete(&mut stream).await;
}

/// The admin that holds `mesh`'s seat once it is `want`, as `node.get` lists it.
async fn seat_with(estate: &Estate, mesh: &str, want: &str) {
    wait_for(&format!("{want} holds {mesh}'s seat"), Duration::from_secs(60), || async {
        estate.node_values().await.into_iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true && n["name"] == want).map(|_| ())
    })
    .await;
}

/// The first inspection `admin` (spans `own`) made of each member's exact birth while it held the seat, from
/// `from_ns` until `until_ns`, and its `via-os-stopped` revalidation of the row another admin wrote: verdict
/// `os-stopped`, outcome `revalidated`. Panics naming what is missing; the inspection spans, for the flame.
fn revalidated_by(own: &[Value], admin: &str, members: &[(String, Value, ExactRuntime)], (from_ns, until_ns): (u64, u64), when: &str) -> Vec<Value> {
    let mut firsts = Vec::new();
    for (name, before, _) in members {
        let mine = |n: &str| -> Vec<&Value> { named(own, n).into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"] && (from_ns..until_ns).contains(&start_ns(sp))).collect() };
        let first = mine("rdm.node_admin.node.resolve.via-provider-inspection").into_iter().filter(|sp| attr(sp, "inspector") == admin).min_by_key(|sp| start_ns(sp)).cloned().unwrap_or_else(|| panic!("{when}: {admin} never inspected {name}'s exact runtime"));
        assert_eq!(attr(&first, "verdict"), "os-stopped", "{when}: {admin}'s first inspection of {name} reads it os-stopped, from the provider state it took over: {first}");
        let facts: Vec<String> = mine("rdm.node_admin.node.update.via-os-stopped").iter().map(|sp| attr(sp, "outcome")).collect();
        assert_eq!(facts, vec!["revalidated".to_string()], "{when}: {admin} confirms {name}'s os-stopped row, which another admin wrote, once, by its own inspection: {facts:?}");
        firsts.push(first);
    }
    firsts
}

/// CONTRACT (i144 e4, the canary; deployment-provider.md §2, §2.5 "A new authority revalidates the
/// stored fact with its own inspection", §2.8; node-primary-update.md steps 5-8): mesh2's two rpc
/// members are held suspended by the operating system and recorded `os-stopped` by mesh2's primary P.
/// P is restarted: `node.primary.update` moves mesh2's seat to its other admin S before P stops. S
/// takes the provider state over: its first inspection of each member's exact runtime reads it
/// `os-stopped`, and it revalidates each member's row once (`via-os-stopped` `revalidated`), the row's
/// `since` kept. S is then restarted and the seat returns to P, which inspects each member again and
/// revalidates its row once more. Continued, both members run on as the same births, their rows
/// cleared. Must NOT happen: a member inspected `unknown` or `unavailable` by a successor (provider
/// state lost), a new `since`, a hold, a `TerminateRuntime`, a recovery or a new birth of a member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_state_survives_a_confirmed_seat_handover_and_its_return() {
    let test = "provider_state_survives_a_confirmed_seat_handover_and_its_return";
    let mut estate = two_meshes(test, &[]).await;
    let (p, other) = admins_of(&estate, "mesh2").await;
    let (p_pid, s_pid) = (estate.pid_of(&p).await, estate.pid_of(&other).await);
    let members = suspended_members(&estate, "mesh2").await;
    let mut since = Vec::new();
    for (name, _, _) in &members {
        since.push(estate.node_value(name).await["os_stopped"]["since"].as_u64().unwrap());
    }

    // P's seat moves to S before P stops; S's tick inspects each silent member and revalidates its row.
    let moved_at = now_ns();
    restart(&estate, &p).await;
    seat_with(&estate, "mesh2", &other).await;
    let floor = rdm_transport::membership::staleness_floor();
    wait_for(&format!("{other} revalidates every member's os-stopped row"), crate::node_recovery_due() + floor, || async {
        let own = spans_of(&estate, s_pid);
        members.iter().all(|(_, b, _)| named(&own, "rdm.node_admin.node.update.via-os-stopped").iter().any(|sp| sp["attributes"]["node_id"] == b["node_id"] && attr(sp, "outcome") == "revalidated" && start_ns(sp) >= moved_at)).then_some(())
    })
    .await;
    // The seat returns: S is restarted, and P, a living admin of the mesh, takes it again.
    let returned_at = now_ns();
    restart(&estate, &other).await;
    seat_with(&estate, "mesh2", &p).await;
    wait_for(&format!("{p} revalidates every member's os-stopped row on the seat's return"), crate::node_recovery_due() + floor, || async {
        let own = spans_of(&estate, p_pid);
        members.iter().all(|(_, b, _)| named(&own, "rdm.node_admin.node.update.via-os-stopped").iter().any(|sp| sp["attributes"]["node_id"] == b["node_id"] && attr(sp, "outcome") == "revalidated" && start_ns(sp) >= returned_at)).then_some(())
    })
    .await;
    for ((name, before, _), since) in members.iter().zip(&since) {
        let held = estate.node_value(name).await;
        assert_eq!((&held["node_id"], &held["incarnation_id"]), (&before["node_id"], &before["incarnation_id"]), "{name}: the same birth across both hand-overs: {held}");
        assert_eq!(held["os_stopped"]["since"].as_u64(), Some(*since), "{name}: the row's since is kept across both hand-overs: {held}");
        assert!(held["held_running"].is_null(), "{name}: nothing is held for an os-stopped runtime: {held}");
    }
    let continued_at = now_ns();
    for (_, _, rt) in &members {
        rt.apply(Fault::Continue).expect("the runtime is continued");
    }
    for (name, before, _) in &members {
        let back = node_where(&estate, name, &format!("{name} back ready with os_stopped cleared"), floor * 4 + Duration::from_secs(15), |n| n["status"] == "ready-for-traffic" && n["os_stopped"].is_null()).await;
        assert_eq!((&back["node_id"], &back["incarnation_id"]), (&before["node_id"], &before["incarnation_id"]), "{name}: the same birth runs on: {back}");
    }

    estate.stop().await;
    let spans = estate.spans();
    let s_first = revalidated_by(&spans_of(&estate, s_pid), &other, &members, (moved_at, returned_at), "the hand-over");
    let p_first = revalidated_by(&spans_of(&estate, p_pid), &p, &members, (returned_at, continued_at), "the return");
    for (name, before, _) in &members {
        let of_birth = |n: &str| -> Vec<&Value> { named(&spans, n).into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect() };
        assert!(of_birth("rdm.node_admin.node.resolve.via-provider-inspection").iter().all(|sp| !["unknown", "unavailable"].contains(&attr(sp, "verdict").as_str())), "{name}: every inspection reads the exact runtime: {:#?}", of_birth("rdm.node_admin.node.resolve.via-provider-inspection"));
        // Once continued, an inspection that reads a member running before its digest returns opens the ordinary
        // hold, which the returning digest clears: only a hold while it was suspended is a defect.
        assert!(of_birth("rdm.node_admin.node.update.via-held-running").iter().all(|sp| start_ns(sp) >= continued_at), "{name}: no hold for an os-stopped runtime");
        assert!(of_birth("rdm.node_admin.build.update.via-node-recovery").is_empty(), "{name}: no recovery");
        assert!(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().all(|sp| !(attr(sp, "step") == "TerminateRuntime" && attr(sp, "node") == *name)), "{name}: no TerminateRuntime");
    }
    for (when, first) in [("hand-over", &s_first), ("return", &p_first)] {
        for sp in first {
            println!("{when}: {} first inspects {}: http://192.168.68.99:16687/trace/{}", attr(sp, "inspector"), attr(sp, "node"), s(&sp["trace_id"]));
        }
    }
    estate.record_trace_url(&s(&s_first[0]["trace_id"]));
}

/// What the operating system shows of one process, strace-free: its state letter, its pending signal
/// masks (thread and process-wide, hex) and its CPU ticks; `None` once it is gone.
fn os_view(pid: u32) -> Option<(char, String, String, u64)> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let field = |k: &str| status.lines().find_map(|l| l.strip_prefix(k)).map(|v| v.trim().to_string()).unwrap_or_default();
    let state = field("State:").chars().next()?;
    Some((state, field("SigPnd:"), field("ShdPnd:"), rdm_test::process_faults::cpu_ticks(pid)))
}

/// A sampler of suspended runtimes: every 20 ms, every change of what the operating system shows of each.
struct Watch {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    seen: std::thread::JoinHandle<Vec<(u64, String, Option<(char, String, String, u64)>)>>,
}

impl Watch {
    fn start(runtimes: Vec<(String, u32)>) -> Watch {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = stop.clone();
        let seen = std::thread::spawn(move || {
            let mut last: std::collections::BTreeMap<String, Option<(char, String, String, u64)>> = Default::default();
            let mut out = Vec::new();
            while !flag.load(std::sync::atomic::Ordering::SeqCst) {
                for (name, pid) in &runtimes {
                    let now = os_view(*pid);
                    if last.get(name) != Some(&now) {
                        out.push((now_ns(), name.clone(), now.clone()));
                        last.insert(name.clone(), now);
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            out
        });
        Watch { stop, seen }
    }

    fn end(self) -> Vec<(u64, String, Option<(char, String, String, u64)>)> {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        self.seen.join().unwrap()
    }
}

/// The parent of process `pid`, as the operating system shows it.
fn parent_of(pid: u32) -> Option<u32> {
    std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?.lines().find_map(|l| l.strip_prefix("PPid:")).and_then(|v| v.trim().parse().ok())
}

/// CONTRACT (i144 e4; deployment-provider.md §2.5: the provider signals no runtime the operating system
/// holds suspended outside a terminate, §2.7; node-delete.md "OS-suspended target"; R23): the provider sends
/// a runtime no signal but its stop ladder and its terminate. mesh2's two rpc members are held suspended
/// (SIGSTOP) and watched, strace-free, from outside RDM: every 20 ms their state letter, their pending
/// signal masks and their CPU ticks. Through a seat hand-over and its return, through the death of the
/// admin that launched them, and through RDM's inspections on every tick, neither member is continued,
/// sent a signal or run: it stays `T`, nothing pending, no CPU. Then an explicit `node.delete` of one
/// member ends it by its terminate alone (`TerminateRuntime`, `Immediate`): it goes from `T` to gone with no
/// signal pending before and no CPU. The other is continued by the cell, never by RDM. Must NOT happen: a
/// member's state other than `T` before the cell continues it or its terminate ends it, a pending signal
/// (SIGTERM, SIGHUP, SIGCONT), CPU used while suspended, or a `TerminateRuntime` of the member not deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_provider_sends_no_signal_but_the_stop_ladder_and_the_terminate() {
    let test = "the_provider_sends_no_signal_but_the_stop_ladder_and_the_terminate";
    let mut estate = two_meshes(test, &[]).await;
    let (p, other) = admins_of(&estate, "mesh2").await;
    let members = suspended_members(&estate, "mesh2").await;
    let watch = Watch::start(members.iter().map(|(name, _, rt)| (name.clone(), rt.pid)).collect());
    let floor = rdm_transport::membership::staleness_floor();
    // The member deleted is the one that holds no cohort seat: a seat holder's delete first moves its seat, and
    // the other member, suspended, is no live secondary to take it.
    let deleted_at = members.iter().position(|(_, b, _)| b["is_primary"] != true).expect("a member that holds no seat");
    let (deleted, kept) = (&members[deleted_at], &members[1 - deleted_at]);

    // The terminate: an explicit delete of one suspended member ends it with no graceful wait.
    let path: rdm_core::PathName = deleted.0.parse().unwrap();
    let mut removal = estate.nodes_obj().await.delete(&path).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of the os-stopped {}: {e}", deleted.0));
    let delete_build = Estate::complete(&mut removal).await.build_id.to_string();
    assert!(deleted.2.check().is_err(), "{}: the suspended runtime is gone after its terminate", deleted.0);
    // A seat hand-over and its return, each successor's tick inspecting the suspended member.
    restart(&estate, &p).await;
    seat_with(&estate, "mesh2", &other).await;
    tokio::time::sleep(crate::node_recovery_due()).await;
    restart(&estate, &other).await;
    seat_with(&estate, "mesh2", &p).await;
    tokio::time::sleep(crate::node_recovery_due()).await;
    // The admin that launched the member dies.
    let launcher = parent_of(kept.2.pid).unwrap_or_else(|| panic!("{}: the runtime has a parent", kept.0));
    let launcher_name = {
        let mut name = None;
        for n in estate.node_values().await.into_iter().filter(|n| n["kind"] == "node_admin" && n["mesh"] == "mesh2") {
            if estate.pid_of(&s(&n["name"])).await == u64::from(launcher) {
                name = Some(s(&n["name"]));
            }
        }
        name.unwrap_or_else(|| panic!("{}: its parent {launcher} is an admin of the estate", kept.0))
    };
    let launcher_killed_at = now_ns();
    estate.kill_node(&launcher_name).await;
    tokio::time::sleep(floor).await;
    let suspended_still = os_view(kept.2.pid);
    let continued_at = now_ns();
    if suspended_still.is_some() {
        kept.2.apply(Fault::Continue).expect("the cell continues the member it suspended");
    }
    let seen = watch.end();
    estate.stop().await;
    let spans = estate.spans();

    let before = |name: &str, until: u64| -> Vec<&(u64, String, Option<(char, String, String, u64)>)> { seen.iter().filter(|(t, n, _)| n == name && *t < until).collect() };
    let held_cpu = |name: &str| seen.iter().find(|(_, n, v)| n == name && v.is_some()).and_then(|(_, _, v)| v.as_ref().map(|v| v.3));
    // The kept member: suspended, nothing pending, no CPU, until the cell continued it.
    let kept_seen = before(&kept.0, continued_at);
    let kept_cpu = held_cpu(&kept.0);
    let bad: Vec<_> = kept_seen.iter().filter(|(_, _, v)| !matches!(v, Some((st, sig, shd, cpu)) if *st == 'T' && sig.trim_start_matches('0').is_empty() && shd.trim_start_matches('0').is_empty() && Some(*cpu) == kept_cpu)).collect();
    assert!(bad.is_empty() && suspended_still.is_some(), "{}: RDM never continued, signalled or ran the suspended member, through the hand-overs and the death of its launcher {launcher_name} (killed at {launcher_killed_at}): {bad:?}; at the end {suspended_still:?}", kept.0);
    // The deleted member: suspended, nothing pending, no CPU, then gone: its terminate alone ended it.
    let deleted_seen = before(&deleted.0, continued_at);
    let deleted_cpu = held_cpu(&deleted.0);
    let alive: Vec<_> = deleted_seen.iter().filter(|(_, _, v)| v.is_some()).collect();
    assert!(alive.iter().all(|(_, _, v)| matches!(v, Some((st, sig, shd, cpu)) if *st == 'T' && sig.trim_start_matches('0').is_empty() && shd.trim_start_matches('0').is_empty() && Some(*cpu) == deleted_cpu)), "{}: suspended, nothing pending and no CPU until its terminate: {alive:?}", deleted.0);
    assert!(deleted_seen.last().is_some_and(|(_, _, v)| v.is_none()), "{}: gone after its terminate: {deleted_seen:?}", deleted.0);
    let terminates: Vec<(String, String)> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "step") == "TerminateRuntime" && members.iter().any(|(n, _, _)| attr(sp, "node") == *n)).map(|sp| (attr(sp, "node"), attr(sp, "build_id"))).collect();
    assert!(terminates.len() == 1 && terminates[0].0 == deleted.0 && same_build(named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| attr(sp, "step") == "TerminateRuntime" && attr(sp, "node") == deleted.0).unwrap(), &delete_build), "one TerminateRuntime of a member, the delete's of {}: {terminates:?}", deleted.0);
    println!("watched {} changes of the suspended members", seen.len());
}
