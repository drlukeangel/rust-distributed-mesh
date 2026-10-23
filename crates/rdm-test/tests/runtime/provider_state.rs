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

/// mesh1 {1 node-admin, 2 rpc nodes}, settled and ready for traffic; the rpc member that holds no
/// seat (its stop moves no seat first) and its exact runtime.
async fn mesh_with_a_member(test: &str) -> (Estate, String, ExactRuntime) {
    let estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[]).await;
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

/// CONTRACT (node-recovery.md step 3: "Any other verdict (`running`, `os-stopped`, `exited`,
/// `unproven`): the recovery below, at once. Nothing holds"; sequence step 3: `TerminateRuntime` first
/// "when the verdict was `running`"; step 4 and `R-FM2`: a newly allocated data dir on any verdict but
/// `exited`): a member the operating system holds suspended (SIGSTOP, acknowledged `T`) goes silent;
/// its mesh-primary's ladder ends in the provider's inspection, verdict `os-stopped`, and the recovery
/// is submitted on that verdict: the path is re-created with a new node.id over a newly allocated data
/// dir, ready for traffic, and the suspended runtime is sent nothing: its node.delete's
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
    let recovered = node_where(&estate, &member, &format!("{member} re-created as a new birth"), crate::node_recovery_due() * 2, |n| n["status"] == "ready-for-traffic" && n["node_id"] != before["node_id"]).await;
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
