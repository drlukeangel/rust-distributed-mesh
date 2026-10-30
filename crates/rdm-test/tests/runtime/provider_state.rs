//! product=mesh, feature=node-lifecycle, subfeature=provider-state, rung=MN, provider=process.
//!
//! deployment-provider.md §2.5; node-recovery.md step 3 (Q111): what the mesh does with the provider's
//! answer for a member whose runtime the operating system holds suspended. The verdict `os-stopped` is
//! node investigation's like any other: the member is re-created at once over a newly allocated data
//! dir, and its suspended runtime is sent nothing (`TerminateRuntime` first only when it runs).

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

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

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// mesh1 {1 node-admin, 2 rpc nodes}, settled and ready; the rpc member that holds no
/// role (its stop moves no role first) and its exact runtime.
async fn mesh_with_a_member(test: &str) -> (Estate, String, ExactRuntime) {
    let estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let member = estate.node_values().await.into_iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && n["is_primary"] != true).map(|n| s(&n["name"])).min().expect("an rpc member that holds no primary role");
    let dir = estate.data_dir(&member).await;
    let rt = ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{member}: no exact runtime published in {dir}: {r:?}"));
    (estate, member, rt)
}

async fn node_where(estate: &Estate, node: &str, what: &str, within: Duration, f: impl Fn(&Value) -> bool) -> Value {
    wait_for(what, within, || async { estate.node_value_opt(node).await.filter(|n| f(n)) }).await
}

/// CONTRACT (node-recovery.md step 3: "Any other verdict (`running`, `os-stopped`, `exited`,
/// `unproven`): the recovery below, at once. Nothing holds"; sequence step 3: `TerminateRuntime` first
/// "when the verdict was `running`"; step 4 and `R-FM2`: a newly allocated data dir on any verdict but
/// `exited`): a member the operating system holds suspended (SIGSTOP, acknowledged `T`) goes silent;
/// its mesh-primary's ladder ends in the provider's inspection, verdict `os-stopped`, and the recovery
/// is submitted on that verdict: the path is re-created with a new node.id over a newly allocated data
/// dir, ready, and the suspended runtime is sent nothing: its node.delete's
/// `TerminateRuntime` reads it `os-stopped` at the step's instant and asks the provider nothing, and it
/// stays suspended, using no CPU. Must NOT happen: the member held, a signal to the suspended runtime,
/// or the new birth over the suspended runtime's data dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_member_is_recovered_at_once_over_a_new_data_dir_and_its_runtime_is_sent_nothing() {
    let test = "an_os_stopped_member_is_recovered_at_once_over_a_new_data_dir_and_its_runtime_is_sent_nothing";
    let (mut estate, member, rt) = mesh_with_a_member(test).await;
    let before = estate.node_value(&member).await;
    let old_dir = estate.data_dir(&member).await;
    let held = rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    assert_eq!(held.state_after, Some('T'), "the OS holds the runtime: {held:?}");
    let ticks = rdm_test::process_faults::cpu_ticks(rt.pid);
    let recovered = node_where(&estate, &member, &format!("{member} re-created as a new birth"), crate::node_recovery_due() * 2, |n| n["status"] == "ready" && n["node_id"] != before["node_id"]).await;
    assert_ne!(recovered["node_id"], before["node_id"], "a new node.id at the path: {recovered}");
    assert_ne!(estate.data_dir(&member).await, old_dir, "a newly allocated data dir, never the suspended runtime's");
    assert_eq!(rdm_test::process_faults::proc_state(rt.pid), Some('T'), "the suspended runtime is sent nothing: still stopped");
    assert_eq!(rdm_test::process_faults::cpu_ticks(rt.pid), ticks, "the suspended runtime used no CPU");
    // The cell's own cleanup: the suspended runtime the recovery left is no estate's.
    rt.apply(Fault::Kill(estate.fabric_ready())).expect("the cell kills the suspended runtime it made");

    estate.stop().await;
    let spans = estate.spans();
    let of_birth = |name: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let inspected = of_birth("rdm.node_admin.node.resolve.via-provider-inspection");
    let verdict = inspected.iter().find(|sp| attr(sp, "verdict") == "os-stopped" && attr(sp, "inspector") == "mesh1.admin.1").cloned().unwrap_or_else(|| panic!("the mesh-primary's inspection names os-stopped: {inspected:#?}"));
    let submit = of_birth("rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| attr(sp, "verdict") == "os-stopped").expect("the recovery is submitted on the os-stopped verdict");
    assert!(start_ns(&submit) >= start_ns(&verdict), "the submit follows the verdict");
    let terminate: Vec<Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "step") == "TerminateRuntime" && attr(sp, "node") == member).cloned().collect();
    assert!(terminate.iter().any(|sp| attr(sp, "outcome") == "complete"), "the delete's TerminateRuntime completes: {terminate:#?}");
    let at_delete = of_birth("rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| attr(sp, "caller") == "terminate").expect("the TerminateRuntime reads the exact runtime at its instant");
    assert_eq!(attr(&at_delete, "verdict"), "os-stopped", "{at_delete}");
    println!("os-stopped recovery: http://192.168.68.99:16687/trace/{}", s(&submit["trace_id"]));
    estate.record_trace_url(&s(&submit["trace_id"]));
}

/// One held birth: its path, its node id and its exact runtime, and the CPU ticks it had at the hold.
struct Held {
    path: String,
    node_id: String,
    rt: ExactRuntime,
}

/// Hold `path`'s exact runtime (SIGSTOP, acknowledged `T`) until node recovery has re-created the path
/// as a new birth over a newly allocated data dir; the held runtime is still `T` and used no CPU.
async fn hold_until_recovered(estate: &Estate, path: &str) -> Held {
    let before = estate.node_value(path).await;
    let node_id = s(&before["node_id"]);
    let old_dir = estate.data_dir(path).await;
    let rt = ExactRuntime::published(Path::new(&old_dir)).unwrap_or_else(|r| panic!("{path}: no exact runtime published in {old_dir}: {r:?}"));
    let held = rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    assert_eq!(held.state_after, Some('T'), "the OS holds {path}: {held:?}");
    let ticks = rdm_test::process_faults::cpu_ticks(rt.pid);
    let recovered = node_where(estate, path, &format!("{path} re-created as a new birth"), crate::node_recovery_due() * 2, |n| n["status"] == "ready" && s(&n["node_id"]) != node_id).await;
    assert_ne!(s(&recovered["node_id"]), node_id, "a new node.id at the path: {recovered}");
    assert_ne!(estate.data_dir(path).await, old_dir, "{path}: a newly allocated data dir, never the held runtime's");
    assert_eq!(rdm_test::process_faults::proc_state(rt.pid), Some('T'), "{path}: the held runtime is sent nothing by the recovery, its create included: still stopped");
    assert_eq!(rdm_test::process_faults::cpu_ticks(rt.pid), ticks, "{path}: the held runtime used no CPU");
    // The cell's own cleanup: the held runtime the recovery left is no estate's.
    rt.apply(Fault::Kill(estate.fabric_ready())).expect("the cell kills the held runtime it made");
    Held { path: path.to_string(), node_id, rt }
}

/// CONTRACT (node-recovery.md step 2: "For an unheard mesh-primary the fabric-primary runs it"; step 3:
/// "Any other verdict (`running`, `os-stopped`, `exited`, `unproven`): the recovery below, at once" and
/// TerminateRuntime "never skipped for what it reads, except `os-stopped`, which recovery leaves as it
/// is"; node-delete.md: the immediate terminate is "Explicit delete only, never recovery, which respects
/// suspension"; node-investigation.md "Spans": node.runtime.inspect's own; node-runtime-inspect.md "Its
/// span is the evidence"): two meshes, the fabric-primary in mesh1, mesh2 with two node-admins and one
/// rpc node. mesh2's mesh-primary mesh2.admin.1 is held by the operating system (SIGSTOP, acknowledged
/// `T`) and stays held through the fabric-primary's investigation of it: the investigation ends
/// `os-stopped` (`rdm.node_admin.primary.update.via-investigation`), the fabric-primary's own inspection
/// of the held runtime is a `rdm.node_admin.node.resolve.via-provider-inspection` span naming
/// `os-stopped`, and the path is re-created as a new birth over a newly allocated data dir while the
/// held runtime is sent nothing: still `T`, no CPU used, its TerminateRuntime reading `os-stopped`. Then
/// mesh2.admin.2 is held the same way, so each of mesh2's admins runs one recovery's create: the create
/// runs under the `node.create:<path.name>` key of the deleted birth's own create and finds no claim of
/// that birth: its row stands Deleted (node-delete.md step 3), written by mesh2's one row writer
/// (node-identity-and-state.md §4.3). Must NOT happen:
/// a `running` finding of the held runtime, an investigation with no inspection span, a signal to a held
/// runtime (`via-lost-executor` naming its deployment), or the new birth over its data dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_os_stopped_mesh_primary_is_found_os_stopped_by_the_fabric_primary_and_its_runtime_is_sent_nothing() {
    let test = "an_os_stopped_mesh_primary_is_found_os_stopped_by_the_fabric_primary_and_its_runtime_is_sent_nothing";
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 2, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    let views = estate.node_views().await;
    let fp = views.iter().find(|n| n.is_fabric_primary).cloned().unwrap_or_else(|| panic!("a fabric-primary: {views:#?}"));
    assert_eq!(fp.mesh, "mesh1", "the fabric-primary is mesh1's: {fp:?}");
    let mp = views.iter().find(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::NodeAdmin && n.is_primary).cloned().unwrap_or_else(|| panic!("mesh2's mesh-primary: {views:#?}"));
    let other = views.iter().find(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::NodeAdmin && n.name != mp.name).cloned().unwrap_or_else(|| panic!("mesh2's other node-admin: {views:#?}"));
    let fp_name = fp.name.to_string();
    let first = hold_until_recovered(&estate, &mp.name.to_string()).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 2, 1)]).await;
    // The second hold begins once the first recovery has its terminal at the fabric-primary (the
    // build.attempt.run of an attempt the fabric-primary opened for it, to mesh2.admin.2, answered
    // complete): the cell holds an admin between recoveries, never the executor of one still reporting
    // its receipts. Any attempt of the recovery: one that ended Failed is opened again (node-recovery.md
    // "Failure and receipts"), and the one that completes is the recovery's.
    let fp_view = fp_name.clone();
    let first_id = first.node_id.clone();
    wait_for("the first recovery's attempt complete at the fabric-primary", crate::node_recovery_due(), || async {
        let spans = estate.spans();
        let attempts: Vec<String> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| attr(sp, "node_id") == first_id && attr(sp, "outcome") == "admitted").map(|sp| attr(sp, "attempt")).collect();
        named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().any(|sp| attr(sp, "node") == fp_view && attempts.contains(&attr(sp, "attempt")) && attr(sp, "outcome") == "complete").then_some(())
    })
    .await;
    let second = hold_until_recovered(&estate, &other.name.to_string()).await;

    estate.stop().await;
    let spans = estate.spans();
    // The fabric-primary's investigation of the held mesh-primary, and its inspection under it.
    let investigations: Vec<Value> = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(|sp| attr(sp, "node") == fp_name && attr(sp, "suspect_node_id") == first.node_id).cloned().collect();
    let investigation = investigations.first().cloned().unwrap_or_else(|| panic!("the fabric-primary investigates the unheard mesh-primary: {investigations:#?}"));
    assert_eq!(attr(&investigation, "finding"), "os-stopped", "the fabric-primary's investigation finds the held mesh-primary os-stopped: {investigation}");
    let inspections = |held: &Held| -> Vec<Value> { named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().filter(|sp| attr(sp, "node_id") == held.node_id).cloned().collect() };
    let of_first = inspections(&first);
    let inspection = of_first
        .iter()
        .find(|sp| attr(sp, "inspector") == fp_name && attr(sp, "caller").is_empty() && sp["trace_id"] == investigation["trace_id"])
        .cloned()
        .unwrap_or_else(|| panic!("the fabric-primary's investigation inspects the held runtime under its own span: {of_first:#?}"));
    assert_eq!(attr(&inspection, "verdict"), "os-stopped", "{inspection}");
    assert!(attr(&inspection, "handle").contains(&first.rt.pid.to_string()), "the inspection names the exact runtime handle: {inspection}");
    // Each held birth: its recovery's TerminateRuntime reads it os-stopped and leaves it, and the
    // recovery's create, run by the other node-admin of mesh2, takes its claim for none of its own.
    let mut creators = Vec::new();
    for held in [&first, &second] {
        let read = inspections(held);
        let at_delete = read.iter().find(|sp| attr(sp, "caller") == "terminate").cloned().unwrap_or_else(|| panic!("{}: the recovery's TerminateRuntime reads the exact runtime at its instant: {read:#?}", held.path));
        assert_eq!(attr(&at_delete, "verdict"), "os-stopped", "{at_delete}");
        let stopped: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.reject.via-lost-executor").into_iter().filter(|sp| attr(sp, "node") == held.path).collect();
        assert!(stopped.is_empty(), "{}: the recovery's create stops no runtime as a lost executor's launch: {stopped:#?}", held.path);
        let claimed: Vec<&Value> = named(&spans, "rdm.node_admin.node.resolve.via-claimed-allocation").into_iter().filter(|sp| attr(sp, "node_id") == held.node_id).collect();
        assert!(claimed.is_empty(), "{}: the deleted birth's row stands Deleted, so the recovery's create finds no claim of it: {claimed:#?}", held.path);
        // The fabric-primary opened the recovery of the unheard mesh-primary and claimed every attempt of
        // it for the admin the election named in its place (node-recovery.md step 2 and "Execution"): its
        // own spans name both, as the held birth's executor is killed by this cell before its spans are
        // flushed. An attempt claimed for any other admin is a writer its mesh's rows refuse.
        let opened: Vec<String> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| attr(sp, "node_id") == held.node_id && attr(sp, "outcome") == "admitted").map(|sp| attr(sp, "attempt")).collect();
        assert!(!opened.is_empty(), "{}: the fabric-primary opens the recovery", held.path);
        let executors: std::collections::BTreeSet<String> = named(&spans, "rdm.node_admin.build.update.via-claim-decision")
            .into_iter()
            .filter(|sp| attr(sp, "node") == fp_name && attr(sp, "lane") == "fabric" && opened.contains(&attr(sp, "attempt")) && attr(sp, "outcome") == "won")
            .map(|sp| attr(sp, "executor"))
            .collect();
        assert_eq!(executors.len(), 1, "{}: every attempt {opened:?} of the recovery is claimed in the fabric lane for one executor: {executors:?}", held.path);
        creators.push((held.path.clone(), executors.into_iter().next().unwrap_or_default()));
        let deleted = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| attr(sp, "node_id") == held.node_id && attr(sp, "outcome") == "row-deleted").map(|sp| attr(sp, "node")).collect::<Vec<_>>();
        assert!(!deleted.iter().any(|w| *w == fp_name), "{}: the fabric-primary writes no row of a mesh with a primary: {deleted:?}", held.path);
    }
    println!("recovery creates: {creators:?}");
    assert_eq!(creators[0].1, second.path, "mesh2.admin.1's recovery is run by the other node-admin of mesh2, its elected mesh-primary: {creators:?}");
    assert_eq!(creators[1].1, first.path, "mesh2.admin.2's recovery is run by the re-created mesh2.admin.1: {creators:?}");
    // The delete written by mesh2's primary, and the held runtime left as it was: the second recovery's
    // trace, whose executor this cell does not kill.
    let written = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().find(|sp| attr(sp, "node_id") == second.node_id && attr(sp, "outcome") == "row-deleted").cloned().unwrap_or_else(|| panic!("{}: its Deleted row is written", second.path));
    assert_eq!(attr(&written, "node"), first.path, "the Deleted row of {} is written by mesh2's primary: {written}", second.path);
    let left = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| attr(sp, "node_id") == second.node_id && attr(sp, "caller") == "terminate").cloned().expect("the second recovery's TerminateRuntime reads its runtime");
    println!("delete written by mesh2's primary: http://192.168.68.99:16687/trace/{}", s(&written["trace_id"]));
    println!("held runtime left os-stopped: http://192.168.68.99:16687/trace/{}", s(&left["trace_id"]));
    let trace = s(&inspection["trace_id"]);
    println!("fabric-primary inspection: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// What the operating system shows of one process, strace-free: its state letter, its pending signal
/// masks (thread and process-wide, hex) and its CPU ticks; `None` once it is gone.
/// One sample of a member after its terminate (`os_view`: state, pending and shared-pending signal
/// masks, CPU): gone; a zombie (`Z`, `X`) holding at most the terminate's SIGKILL pending until its
/// parent reaps it; or, in any other state, the kernel killing it: the SIGKILL, and only it, pending.
fn ended_by_its_terminate(sample: &Option<(char, String, String, u64)>) -> bool {
    let kill = 1u64 << (libc::SIGKILL - 1);
    let bits = |mask: &str| u64::from_str_radix(mask, 16).ok();
    match sample {
        None => true,
        Some((st, sig, shd, _)) => match (bits(sig), bits(shd)) {
            (Some(sig), Some(shd)) if (sig | shd) & !kill == 0 => matches!(st, 'Z' | 'X') || (sig | shd) == kill,
            _ => false,
        },
    }
}

/// CONTRACT (the signal audit's reading of a member's end): the kernel delivers the terminate's
/// SIGKILL to a stopped task by waking it to die, so a sample can catch it still stopped (`T`),
/// runnable (`R`), sleeping (`S`) or in its exit (`D`) with that SIGKILL, and only it, pending; that is
/// its terminate ending it, not the member running again. A member with nothing pending outside a
/// zombie's state, or with any other signal, is not ended.
#[test]
fn a_member_the_kernel_is_killing_is_read_as_ended_by_its_terminate() {
    let kill = format!("{:016x}", 1u64 << (libc::SIGKILL - 1));
    let none = "0000000000000000".to_string();
    // Release run at d829b383: mesh2.rpc.2 sampled `R`, SIGKILL pending in both masks, then gone.
    assert!(ended_by_its_terminate(&Some(('R', kill.clone(), kill.clone(), 27))), "runnable with only the terminate's SIGKILL pending is being killed");
    assert!(ended_by_its_terminate(&Some(('D', none.clone(), kill.clone(), 27))), "in its exit with only the SIGKILL pending is being killed");
    assert!(ended_by_its_terminate(&Some(('Z', kill.clone(), none.clone(), 27))));
    assert!(ended_by_its_terminate(&None));
    assert!(!ended_by_its_terminate(&Some(('R', none.clone(), none.clone(), 27))), "runnable with nothing pending runs again");
    assert!(!ended_by_its_terminate(&Some(('R', format!("{:016x}", 1u64 << (libc::SIGCONT - 1)), none.clone(), 27))), "a SIGCONT is not the terminate");
    assert!(ended_by_its_terminate(&Some(('T', kill.clone(), none.clone(), 27))), "still stopped with the SIGKILL pending is being killed");
    assert!(ended_by_its_terminate(&Some(('S', kill.clone(), kill, 27))), "woken to die");
}
