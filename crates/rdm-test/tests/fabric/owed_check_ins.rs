//! i146 process E2E: a round's check-ins are owed messages (node-rpc.md §43, `Budget::Owed`), and a
//! fabric-primary healed from a cut-off rebuilds its round's checklist (Q45). Each cell suspends or
//! kills a primary past the staleness floor and a call's former 10 s budget: a release-gate stem.
//!
//! A real estate whose node-admin is the testkit's (the fault door's round-action cut orders each
//! cell), carrying the testkit application. Evidence is read from the spans every process wrote.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeView, ScopeStatus};
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-states".into(),
        subfeature: "owed-check-ins".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn flag(v: &Value) -> bool {
    v == true || v == "true"
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

async fn estate_with(test: &str, app: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_TEST_APP", app)])
        .await
        .expect("the faulted-admin binding set is accepted")
}

/// Await the fabric under `build` in `state`, as `fabric.get` at the admin the estate asks reports it.
async fn until_fabric(estate: &Estate, state: ScopeStatus, build: Option<&str>, within: Duration) {
    let what = format!("the fabric is {state:?}{}", build.map(|b| format!(" under {b}")).unwrap_or_default());
    estate.fabrics_obj().await.fact(&what, within, |f| f.status == state && build.is_none_or(|b| f.build_id.as_deref() == Some(b))).await.unwrap_or_else(|end| panic!("{end}"));
}

fn signal(pid: u64, sig: &str) {
    let ok = std::process::Command::new("kill").args([sig, &pid.to_string()]).status().unwrap().success();
    assert!(ok, "kill {sig} {pid}");
}

/// The budget a member's check-in call had when it rode `CallOptions::default()`: one 10 s overall
/// bound, after which the call ended and its request was owed no longer.
const DEFAULT_CALL_BUDGET: Duration = Duration::from_secs(10);

/// CONTRACT (i146 e3; node-rpc.md §43; fabric-state-commit.md step 3, "Each member checks in with its
/// primary"): a member's check-in is owed to the mesh primary that commanded it until that primary
/// answers, however long the primary cannot be reached. In a fabric of two meshes, a resize of mesh2
/// puts the fabric back through its climb; mesh2's other admin takes the commit-state command and is
/// held in its action (the fault door's round-action cut). mesh2's primary, not the fabric-primary,
/// is suspended until every connection to it has idled out, the action released, and the primary
/// stays suspended past the 10 s a check-in call had; continued, it applies the check-in and the
/// fabric reaches ready-for-traffic under the resize.
/// The mesh primary marks a member's command answered on its `Applied` and never sends it again
/// (`fabric_rounds.rs` `spawn_command`, `fabric_state.rs` `take_due`), so the check-in is the only
/// path to its checklist. Must NOT happen: the check-in ending unanswered, the fabric held in
/// state-commit, the member's action run twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_members_check_in_owed_while_its_mesh_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready() {
    let test = "a_members_check_in_owed_while_its_mesh_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready";
    let mut estate = estate_with(test, "answer:0").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary == primary).cloned().expect("mesh2 has two admins");
    let (holder, member) = (admin_of(true), admin_of(false));
    assert!(!holder.is_fabric_primary, "mesh2's primary is not the fabric-primary: the climb goes on around it");
    let (holder_s, member_s) = (holder.name.to_string(), member.name.to_string());

    // The member takes the resize's commit-state command and is held in its action.
    let door = Door::at(&estate, &member_s).await;
    door.arm("round-action", json!({"kind": "round-action", "node": member_s, "round": "commit-state"})).await;
    let resize = estate.submit(&rdm_client::BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] })).await;
    let resize_id = resize.build_id().to_string();
    let hit = door.wait_held("round-action").await;
    assert_eq!(hit["hit"]["node"], member_s.as_str(), "{hit}");

    // Its primary is suspended before the check-in is owed, until every connection to it has idled out
    // (the QUIC idle timeout is the staleness floor: `rdm_transport::endpoint::transport_config`), so
    // the check-in has no live connection to be written on; then past the call budget.
    let pid = estate.pid_of(&holder_s).await;
    signal(pid, "-STOP");
    tokio::time::sleep(rdm_transport::membership::staleness_floor() + Duration::from_secs(2)).await;
    door.release("round-action").await;
    tokio::time::sleep(DEFAULT_CALL_BUDGET + Duration::from_secs(3)).await;
    signal(pid, "-CONT");
    estate.follow(resize).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(resize_id.as_str()), Duration::from_secs(60)).await;

    estate.stop().await;
    let spans = estate.spans();
    let calls: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-completion-call")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["node"]) == member_s && sp["attributes"]["completion"] == "state-committed" && s(&sp["attributes"]["build_id"]) == resize_id)
        .collect();
    assert_eq!(calls.len(), 1, "the member owed one check-in of the resize's commit-state: {calls:#?}");
    assert_eq!(calls[0]["attributes"]["commander"], json!(holder.node_id.to_string()), "to the primary that commanded it: {:#?}", calls[0]);
    assert_eq!(calls[0]["attributes"]["answer"], "applied", "the suspended primary applied it once it was continued: {:#?}", calls[0]);
    let holds: Vec<&Value> = named(&spans, "rdm.testkit.fault.update.via-hold").into_iter().filter(|sp| sp["attributes"]["cut"] == "round-action").collect();
    assert_eq!(holds.len(), 1, "the member's action ran once: {holds:#?}");
    println!("member check-in across its primary's suspension: {}; flame http://192.168.68.99:16687/trace/{}", s(&calls[0]["attributes"]["answer"]), s(&calls[0]["trace_id"]));
    estate.record_trace_url(&s(&calls[0]["trace_id"]));
}

/// CONTRACT (i146 Q44, Q39; node-rpc.md §43, §34; fabric-node-lifecycle-elections.md: "a holder that
/// yields or whose exact birth is proven gone hands the seat to the lowest ReadyForTraffic NodeId of
/// its own mesh"): a member's check-in owed to a mesh primary that dies is owed to the holder the seat
/// names next. mesh2 holds three admins; the member is the non-primary admin the election will not
/// pick. It is held in the resize's commit-state action, its primary suspended until every connection
/// to it idles out, the action released (the check-in owed to the suspended primary), then the
/// primary killed. Whichever reaches the member first, the seat's move (the call to the old holder is
/// dropped) or the proof of the exit (the call ends `UNAVAILABLE`), the completion is retargeted to the
/// new holder and answered there, and the fabric reaches ready-for-traffic under the resize. Must NOT
/// happen: the call ending before the primary was killed, the completion sent to an admin the seat
/// never named, the fabric held in state-commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_members_check_in_owed_to_a_primary_that_dies_is_owed_to_the_new_holder() {
    let test = "a_members_check_in_owed_to_a_primary_that_dies_is_owed_to_the_new_holder";
    let mut estate = estate_with(test, "answer:0").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 3, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 3, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let holder = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2 has a primary");
    assert!(!holder.is_fabric_primary, "mesh2's primary is not the fabric-primary: the climb goes on around it");
    let mut others: Vec<NodeView> = views.iter().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && !n.is_primary).cloned().collect();
    others.sort_by_key(|n| n.node_id.to_string());
    let (successor, member) = (others[0].clone(), others[1].clone());
    let (holder_s, member_s) = (holder.name.to_string(), member.name.to_string());
    let (holder_id, successor_id) = (holder.node_id.to_string(), successor.node_id.to_string());

    let door = Door::at(&estate, &member_s).await;
    door.arm("round-action", json!({"kind": "round-action", "node": member_s, "round": "commit-state"})).await;
    let resize = estate.submit(&rdm_client::BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 3), (NodeKind::RpcNode, 2)])] })).await;
    let resize_id = resize.build_id().to_string();
    door.wait_held("round-action").await;

    // The check-in is owed to a suspended primary, then the primary is killed.
    let pid = estate.pid_of(&holder_s).await;
    signal(pid, "-STOP");
    tokio::time::sleep(rdm_transport::membership::staleness_floor() + Duration::from_secs(2)).await;
    door.release("round-action").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let killed_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.kill_pid(pid as u32);
    estate.follow(resize).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(resize_id.as_str()), Duration::from_secs(120)).await;

    estate.stop().await;
    let spans = estate.spans();
    let mine = |name: &str| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == member_s && sp["attributes"]["completion"] == "state-committed" && s(&sp["attributes"]["build_id"]) == resize_id).cloned().collect();
        v.sort_by_key(start);
        v
    };
    let calls = mine("rdm.node_admin.status.update.via-completion-call");
    let retargeted = mine("rdm.node_admin.status.update.via-completion-retargeted");
    let moved = retargeted.iter().find(|r| s(&r["attributes"]["from"]) == holder_id && s(&r["attributes"]["to"]) == successor_id).unwrap_or_else(|| panic!("the completion was owed again to the new holder {successor_id}: {retargeted:#?}"));
    let why = s(&moved["attributes"]["why"]);
    assert!(why == "seat-moved" || why.ends_with(": UNAVAILABLE"), "retargeted on the seat's move or on the proof of the exit: {moved:#?}");
    assert!(start(moved) >= killed_at, "nothing ended the owed check-in before the primary was killed: {moved:#?}");
    for c in calls.iter().filter(|c| s(&c["attributes"]["commander"]) == holder_id) {
        assert!(s(&c["attributes"]["answer"]).ends_with(": UNAVAILABLE") && start(c) >= killed_at, "a call to the primary ends only on the proof of its exit: {c:#?}");
    }
    assert!(calls.iter().all(|c| [holder_id.as_str(), successor_id.as_str()].contains(&s(&c["attributes"]["commander"]).as_str())), "owed only to the seat's holders: {calls:#?}");
    let answered: Vec<&Value> = calls.iter().filter(|c| s(&c["attributes"]["commander"]) == successor_id && !s(&c["attributes"]["answer"]).contains(": ")).collect();
    assert!(!answered.is_empty(), "the new holder answered the completion: {calls:#?}");
    println!("check-in across its primary's death: retargeted ({why}), {} at the new holder; flame http://192.168.68.99:16687/trace/{}", s(&answered[0]["attributes"]["answer"]), s(&moved["trace_id"]));
    estate.record_trace_url(&s(&moved["trace_id"]));
}

/// The fabric-primary cut off during the resize's `round` and healed (i146 Q45, e3 owner 3): mesh2's
/// primary is held in its own `round` action (the fault door's round-action cut), the fabric-primary
/// suspended until every connection to it idles out and past the 10 s a call had (past the floor: it
/// is cut off), the action released (mesh2's check-in owed upward to the suspended fabric-primary),
/// then the fabric-primary continued. With another member in its mesh, its cut-off ended its tenure;
/// healed, it re-opens the round, re-runs its own mesh's round (its own action from its record). Alone
/// in its mesh it is never cut off and keeps its seat (gossip.md §6, "A cut-off view"); its round
/// completes once it wakes. Either way the fabric reaches ready-for-traffic under the resize, and every
/// round action runs once per round key. A cut-off is the suspension's only between the suspension and
/// the cell's own `estate.stop()`: once every node is signalled, a fabric-primary whose shutdown outlasts
/// a staleness floor hears no other member and spans a cut-off that belongs to the teardown.
async fn fabric_primary_cut_off(test: &str, mesh1: (u32, u32), round: &str) {
    let mut estate = estate_with(test, "answer:0").await;
    estate.reconcile_shape("fabric1", &[("mesh1", mesh1.0, mesh1.1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", mesh1.0, mesh1.1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let primary = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2 has a primary");
    let fp = views.iter().find(|n| n.is_fabric_primary).cloned().expect("the fabric has a primary");
    assert_eq!(fp.mesh, "mesh1", "the fabric-primary is mesh1's admin");
    let (primary_s, fp_s) = (primary.name.to_string(), fp.name.to_string());
    let members: Vec<String> = views.iter().filter(|n| n.mesh == "mesh1" && n.name != fp.name).map(|n| n.name.to_string()).collect();

    let door = Door::at(&estate, &primary_s).await;
    door.arm("round-action", json!({"kind": "round-action", "node": primary_s, "round": round})).await;
    let resize = estate.submit(&rdm_client::BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, mesh1.0), (NodeKind::RpcNode, mesh1.1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] })).await;
    let resize_id = resize.build_id().to_string();
    door.wait_held("round-action").await;

    // The fabric-primary is the estate's Day-0 admin: its process is the estate's own child.
    let pid = match estate.bootstrap_pid() {
        Some(p) if fp_s == "mesh1.admin.1" => u64::from(p),
        _ => estate.pid_of(&fp_s).await,
    };
    let suspended_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    signal(pid, "-STOP");
    tokio::time::sleep(rdm_transport::membership::staleness_floor() + Duration::from_secs(2)).await;
    door.release("round-action").await;
    tokio::time::sleep(DEFAULT_CALL_BUDGET + Duration::from_secs(3)).await;
    signal(pid, "-CONT");
    estate.follow(resize).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(resize_id.as_str()), Duration::from_secs(60)).await;

    let stopping_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.stop().await;
    let spans = estate.spans();
    let of_resize = |name: &str, node: &str, r: &str| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && sp["attributes"]["round"] == r && s(&sp["attributes"]["build_id"]) == resize_id).cloned().collect();
        v.sort_by_key(start);
        v
    };
    let own = of_resize("rdm.node_admin.fabric.update.via-mesh-round", &fp_s, round);
    // Woken, the fabric-primary is cut off only if its watch ticks before the member digests queued
    // during the suspension apply; those digests are fresh, and applied first they are hearing
    // (gossip.md §6, "A cut-off view"). Alone in its mesh it is never cut off and keeps its seat. When
    // the cut-off fired, it ended the fabric-primary's tenure; healed, it ran its own mesh's round
    // again, its own action from its record. A cut-off spanned once the cell began stopping the estate
    // is the teardown's (every peer signalled), never the suspension's.
    let cut_off = named(&spans, "rdm.mesh.membership.update.via-cut-off").iter().any(|sp| s(&sp["attributes"]["node"]) == fp_s && sp["attributes"]["role"] == "start" && (suspended_at..stopping_at).contains(&start(sp)));
    if cut_off {
        assert!(named(&spans, "rdm.node_admin.fabric.remove.via-tenure-ended").iter().any(|sp| s(&sp["attributes"]["node"]) == fp_s), "the cut-off ended {fp_s}'s tenure");
        assert!(own.len() >= 2, "{fp_s} ran its own mesh's {round} round again after the cut-off: {own:#?}");
        assert!(own.iter().skip(1).all(|r| r["attributes"]["own_action"] == "from-record"), "its own action came from its record: {own:#?}");
    }
    // Every round action ran once per round key across the cut-off: the fabric-primary's, mesh2's
    // primary's and each member's.
    for r in ["commit-state", "open-traffic"] {
        for admin in [&fp_s, &primary_s] {
            let ran = of_resize("rdm.node_admin.fabric.update.via-mesh-round", admin, r).into_iter().filter(|m| m["attributes"]["own_action"] == "run").count();
            assert_eq!(ran, 1, "{admin}'s own {r} action ran once");
        }
        for m in &members {
            let acted = of_resize("rdm.node_admin.fabric.update.via-member-round", m, r).into_iter().filter(|m| flag(&m["attributes"]["acted"])).count();
            assert_eq!(acted, 1, "{m}'s {r} action ran once");
        }
    }
    let flame = own.last().map(|r| s(&r["trace_id"])).unwrap_or_default();
    println!("fabric-primary suspended during {round} (cut off: {cut_off}): its own mesh round ran {} time(s); flame http://192.168.68.99:16687/trace/{flame}", own.len());
    estate.record_trace_url(&flame);
}

/// CONTRACT (i146 Q45; states.md "The round checklist": "A new primary, through election or recovery,
/// re-sends the current round. Members already done check in again, and the checklist rebuilds
/// itself"; gossip.md: "a cut-off view holds no fabric-primary authority"): a fabric-primary cut off
/// during state-commit and healed re-runs its own mesh's round, whose members check in again from
/// their record, and the fabric reaches ready-for-traffic. A cut-off during the estate's own teardown
/// (after the cell stops it, every peer signalled) is not the suspension's and asserts nothing. Must NOT
/// happen: the fabric held in state-commit on the fabric-primary's own check-in, a round action run
/// twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_cut_off_during_state_commit_re_runs_its_own_mesh_round_and_the_fabric_reaches_ready() {
    fabric_primary_cut_off("a_fabric_primary_cut_off_during_state_commit_re_runs_its_own_mesh_round_and_the_fabric_reaches_ready", (2, 1), "commit-state").await;
}

/// CONTRACT (i146 Q45): the same for open-traffic: a fabric-primary cut off during the open-traffic
/// round and healed re-runs its own mesh's round and the fabric reaches ready-for-traffic; no round
/// action runs twice. A cut-off during the estate's own teardown is not the suspension's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_cut_off_during_open_traffic_re_runs_its_own_mesh_round_and_the_fabric_reaches_ready() {
    fabric_primary_cut_off("a_fabric_primary_cut_off_during_open_traffic_re_runs_its_own_mesh_round_and_the_fabric_reaches_ready", (2, 1), "open-traffic").await;
}

/// CONTRACT (i146 e3 owner 3, Q45; node-rpc.md §43; fabric-state-commit.md step 3, "node →
/// mesh-primary → fabric-primary"): a mesh primary's check-in is owed to the fabric-primary until it
/// answers, however long it cannot be reached; and a fabric-primary that is its mesh's only node keeps
/// its seat while suspended (gossip.md §6: a view that holds no other member is not cut off), and its
/// round completes once it wakes. The fabric reaches ready-for-traffic. Must NOT happen: the check-in
/// ending unanswered, the fabric held in state-commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primarys_check_in_owed_while_the_fabric_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready() {
    fabric_primary_cut_off("a_mesh_primarys_check_in_owed_while_the_fabric_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready", (1, 0), "commit-state").await;
}
