//! i146 process E2E: a checklist's check-ins are owed messages (node-rpc.md §43, `Budget::Owed`), and a
//! fabric-primary held past the staleness floor is re-created at its path.name by fabric recovery, the restored node
//! re-sending the checklist (Q122). Each cell suspends or
//! kills a primary past the staleness floor and a call's former 10 s budget: a release-gate stem.
//!
//! A real estate whose node-admin is the testkit's (the fault door's state-action cut orders each
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
    Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_TEST_APP", app)])
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

/// How many times each birth among `admins` ran its own `checklist` action for `attempt` of `build`, by its
/// process's evidence file (one birth per process): `(file, runs)`.
fn runs_per_birth(evidence: &std::path::Path, admins: &[String], state_key: &str, build: &str, attempt: &str) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    for f in std::fs::read_dir(evidence).unwrap().flatten() {
        let Ok(text) = std::fs::read_to_string(f.path()) else { continue };
        let ran = text
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-mesh-checklist")
            .filter(|sp| admins.contains(&s(&sp["attributes"]["node"])) && sp["attributes"]["command"] == state_key && s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["own_action"] == "run")
            .count();
        out.push((f.file_name().to_string_lossy().into_owned(), ran));
    }
    out
}

/// The budget a member's check-in call had when it rode `CallOptions::default()`: one 10 s overall
/// bound, after which the call ended and its request was owed no longer.
const DEFAULT_CALL_BUDGET: Duration = Duration::from_secs(10);

/// CONTRACT (i146 Q44, Q39; node-rpc.md §43, §34; fabric-node-lifecycle-elections.md: "a holder that
/// yields or whose exact birth is proven gone hands the seat to the lowest ReadyForTraffic NodeId of
/// its own mesh"): a member's check-in owed to a mesh primary that dies is owed to the holder the seat
/// names next. mesh2 holds three admins; the member is the non-primary admin the election will not
/// pick. It is held in the resize's open-traffic action, its primary suspended, the action released
/// (the check-in owed to the suspended primary), then the primary killed, all inside the staleness floor
/// (past it the election names the next holder, Q119 arm A, before the check-in is sent). Whichever reaches the member first, the seat's move (the call to the old holder is
/// dropped) or the proof of the exit (the call ends `UNAVAILABLE`), the completion is retargeted to the
/// new holder and answered there, and the fabric reaches ready-for-traffic under the resize. Must NOT
/// happen: the call ending before the primary was killed, the completion sent to an admin the seat
/// never named, the fabric held pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_members_check_in_owed_to_a_primary_that_dies_is_owed_to_the_new_primary() {
    let test = "a_members_check_in_owed_to_a_primary_that_dies_is_owed_to_the_new_primary";
    let mut estate = estate_with(test, "answer").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 3, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 3, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let holder = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2 has a primary");
    assert!(!holder.is_fabric_primary, "mesh2's primary is not the fabric-primary: the change goes on around it");
    let mut others: Vec<NodeView> = views.iter().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && !n.is_primary).cloned().collect();
    others.sort_by_key(|n| n.node_id.to_string());
    let (successor, member) = (others[0].clone(), others[1].clone());
    let (holder_s, member_s) = (holder.name.to_string(), member.name.to_string());
    let (holder_id, successor_id) = (holder.node_id.to_string(), successor.node_id.to_string());

    let door = Door::at(&estate, &member_s).await;
    door.arm("state-action", json!({"kind": "state-action", "node": member_s, "command": "open-traffic"})).await;
    let resize = estate.submit(&rdm_test::typed::BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 3), (NodeKind::RpcNode, 2)])] })).await;
    let resize_id = resize.build_id().to_string();
    door.wait_held("state-action").await;

    // The check-in is owed to a suspended primary, then the primary is killed, all inside the staleness
    // floor: past it the election names the next holder (Q119 arm A), and a check-in sent after that is
    // owed to the new holder from the start.
    let floor = rdm_transport::topology::staleness_floor();
    let pid = estate.pid_of(&holder_s).await;
    signal(pid, "-STOP");
    tokio::time::sleep(floor / 4).await;
    door.release("state-action").await;
    tokio::time::sleep(floor / 4).await;
    let killed_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.kill_pid(pid as u32);
    estate.follow(resize).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(resize_id.as_str()), Duration::from_secs(120)).await;

    estate.stop().await;
    let spans = estate.spans();
    let mine = |name: &str| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == member_s && sp["attributes"]["completion"] == "traffic-opened" && s(&sp["attributes"]["build_id"]) == resize_id).cloned().collect();
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

/// The fabric-primary held during the resize's `checklist` and continued (Q122; fabric-recovery.md "The loss
/// of the fabric-primary while a node-admin lives"; states.md "The check-in checklist"): mesh2's primary is
/// held in its own `checklist` action (the fault door's state-action cut) and the fabric-primary suspended.
/// Unheard past the floor, nobody takes the fabric-primary: with another node-admin in its mesh, that
/// node-admin runs fabric recovery at the first tick after the mark; alone in its mesh, mesh2's primary
/// runs it from the backbone at mesh investigation's decision step. The node re-created at the
/// fabric-primary's path.name is restored as mesh-primary and fabric-primary. The action is released past
/// the 10 s a call had (mesh2's check-in owed upward, node-rpc.md §43) and the held birth continued. The
/// restored fabric-primary re-sends the current state's command, the members already done check in again, and the
/// fabric reaches ready-for-traffic under the resize. Every state action runs once per state key.
async fn fabric_primary_held(test: &str, mesh1: (u32, u32), state_key: &str) {
    let mut estate = estate_with(test, "answer").await;
    estate.reconcile_shape("fabric1", &[("mesh1", mesh1.0, mesh1.1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", mesh1.0, mesh1.1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let primary = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2 has a primary");
    let fp = views.iter().find(|n| n.is_fabric_primary).cloned().expect("the fabric has a primary");
    assert_eq!(fp.mesh, "mesh1", "the fabric-primary is mesh1's admin");
    assert!(fp.is_primary, "the fabric-primary holds its mesh's seat too");
    let (primary_s, fp_s) = (primary.name.to_string(), fp.name.to_string());
    let members: Vec<String> = views.iter().filter(|n| n.mesh == "mesh1" && n.kind == NodeKind::RpcNode).map(|n| n.name.to_string()).collect();
    let standby: Option<String> = views.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && n.name != fp.name).map(|n| n.name.to_string());
    // Who runs fabric recovery (Q122): the other node-admin of the fabric-primary's own mesh, else
    // mesh2's primary from the backbone at mesh investigation's decision step.
    let runner = standby.clone().unwrap_or_else(|| primary_s.clone());
    let arm = if standby.is_some() { "own-mesh" } else { "backbone" };
    let floor = rdm_transport::topology::staleness_floor();

    let door = Door::at(&estate, &primary_s).await;
    door.arm("state-action", json!({"kind": "state-action", "node": primary_s, "command": state_key})).await;
    let resize = estate.submit(&rdm_test::typed::BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, mesh1.0), (NodeKind::RpcNode, mesh1.1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] })).await;
    let resize_id = resize.build_id().to_string();
    door.wait_held("state-action").await;

    // The fabric-primary is the estate's Day-0 admin: its process is the estate's own child. Control is
    // read through the admin that runs fabric recovery from here: a held admin answers nothing.
    let pid = match estate.bootstrap_pid() {
        Some(p) if fp_s == "mesh1.admin.1" => u64::from(p),
        _ => estate.pid_of(&fp_s).await,
    };
    estate.enter(&Estate::path(&runner)).await;
    let suspended_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    signal(pid, "-STOP");
    // Nobody takes the fabric-primary: the node re-created at its path.name is restored as both.
    let restored = estate
        .until_view(&format!("the node re-created at {fp_s} is mesh-primary and fabric-primary while the old birth is held"), floor * 20 + Duration::from_secs(120), |views| {
            assert!(views.iter().filter(|n| n.is_fabric_primary).all(|n| n.name.to_string() == fp_s), "only the node at {fp_s} is ever fabric-primary");
            views.iter().find(|n| n.name.to_string() == fp_s && n.node_id != fp.node_id && n.is_fabric_primary && n.is_primary).map(|n| n.node_id.clone())
        })
        .await;
    door.release("state-action").await;
    tokio::time::sleep(DEFAULT_CALL_BUDGET + Duration::from_secs(3)).await;
    // Continued, the held birth comes back to a path.name a new birth holds.
    signal(pid, "-CONT");
    estate.follow(resize).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(resize_id.as_str()), Duration::from_secs(120)).await;

    estate.stop().await;
    let spans = estate.spans();
    let of_resize = |name: &str, node: &str, r: &str| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && sp["attributes"]["command"] == r && s(&sp["attributes"]["build_id"]) == resize_id).cloned().collect();
        v.sort_by_key(start);
        v
    };
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= suspended_at).collect();
    let run = ran.iter().find(|sp| s(&sp["attributes"]["node"]) == runner && sp["attributes"]["arm"] == arm && s(&sp["attributes"]["outcome"]).starts_with("restored")).cloned().unwrap_or_else(|| panic!("{runner} ran fabric recovery ({arm}) and restored {fp_s}: {ran:#?}"));
    assert!(ran.iter().all(|sp| s(&sp["attributes"]["node"]) == runner), "nobody else ran it: {ran:#?}");
    // The restored fabric-primary re-sent the current checklist as its new primary (states.md "The check-in checklist").
    let restored_at = at(run);
    let re_sent: Vec<Value> = of_resize("rdm.node_admin.fabric.update.via-mesh-checklist", &fp_s, state_key).into_iter().filter(|sp| start(sp) >= restored_at).collect();
    assert!(!re_sent.is_empty(), "the node restored at {fp_s} ({restored}) ran mesh1's {state_key} checklist after the restore: {re_sent:#?}");
    // Every state action ran once per state key on each node (states.md "The check-in checklist": the key is
    // the Build's attempt and the checklist; the record of what ran is each node's own,
    // rdm_node::fabric_states::ActedRecord): a primary's own action is its own node's work, so the held
    // birth's action and the re-created birth's at the same path.name are two nodes' actions, each run once; each process's evidence is one birth's.
    // mesh1 ran its checklist for every attempt that reached it; mesh2's primary's and each member's ran once
    // per attempt.
    let attempt_of = |sp: &Value| s(&sp["attributes"]["attempt"]);
    for r in ["open-traffic"] {
        let mesh1_admins: Vec<String> = [Some(fp_s.clone()), standby.clone()].into_iter().flatten().collect();
        let mesh1_check_ins: Vec<Value> = mesh1_admins.iter().flat_map(|admin| of_resize("rdm.node_admin.fabric.update.via-mesh-checklist", admin, r)).collect();
        let attempts: std::collections::BTreeSet<String> = mesh1_check_ins.iter().map(attempt_of).collect();
        assert!(!attempts.is_empty(), "mesh1 ran its {r} checklist");
        for a in &attempts {
            assert!(mesh1_check_ins.iter().any(|m| attempt_of(m) == *a && m["attributes"]["own_action"] == "run"), "mesh1's own {r} action ran for attempt {a}");
            for (birth, ran) in runs_per_birth(&estate.evidence, &mesh1_admins, r, &resize_id, a) {
                assert!(ran <= 1, "{birth}'s own {r} action ran once for attempt {a}: {ran}");
            }
        }
        let primary_check_ins = of_resize("rdm.node_admin.fabric.update.via-mesh-checklist", &primary_s, r);
        for a in primary_check_ins.iter().map(attempt_of).collect::<std::collections::BTreeSet<_>>() {
            let ran = primary_check_ins.iter().filter(|m| attempt_of(m) == a && m["attributes"]["own_action"] == "run").count();
            assert_eq!(ran, 1, "{primary_s}'s own {r} action ran once for attempt {a}");
        }
        for m in &members {
            let member_check_ins = of_resize("rdm.node_admin.fabric.update.via-member-check-in", m, r);
            for a in member_check_ins.iter().map(attempt_of).collect::<std::collections::BTreeSet<_>>() {
                let acted = member_check_ins.iter().filter(|x| attempt_of(x) == a && flag(&x["attributes"]["acted"])).count();
                assert_eq!(acted, 1, "{m}'s {r} action ran once for attempt {a}");
            }
        }
    }
    let flame = s(&run["trace_id"]);
    println!("fabric-primary held during {state_key}: {runner} ran fabric recovery ({arm}); the restored node re-sent the checklist; flame http://192.168.68.99:16687/trace/{flame}");
    estate.record_trace_url(&flame);
}

/// CONTRACT (Q122; states.md "The check-in checklist": "A new primary, through election or recovery,
/// re-sends the current state's command. Members already done check in again, and the checklist rebuilds
/// itself"): a fabric-primary held past the staleness floor during the open-traffic checklist is
/// re-created at its path.name by the fabric recovery its mesh's other node-admin runs; the restored
/// fabric-primary re-sends the checklist, the members check in again from their record and the fabric
/// reaches ready-for-traffic under the resize. Must NOT happen: another node named fabric-primary, the
/// fabric held pending, a state action run twice for one key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_held_during_open_traffic_is_re_created_by_fabric_recovery_which_re_sends_the_state_command() {
    fabric_primary_held("a_fabric_primary_held_during_open_traffic_is_re_created_by_fabric_recovery_which_re_sends_the_state_command", (2, 1), "open-traffic").await;
}

/// CONTRACT (Q122, fabric-recovery.md "Outside its mesh"; node-rpc.md §43; states.md "The check-in
/// checklist"): a mesh primary's check-in is owed to the fabric-primary until
/// it answers. A fabric-primary that is its mesh's only node, held past the floor, is probed by mesh2's
/// primary, which runs fabric recovery from the backbone at mesh investigation's decision step; the node
/// re-created at the fabric-primary's path.name is restored, re-sends the checklist, and the fabric reaches
/// ready-for-traffic under the resize. Must NOT happen: mesh2's primary named fabric-primary, the
/// check-in ending unanswered, the fabric held pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_alone_in_its_mesh_held_during_open_traffic_is_re_created_from_the_backbone_which_re_sends_the_state_command() {
    fabric_primary_held("a_fabric_primary_alone_in_its_mesh_held_during_open_traffic_is_re_created_from_the_backbone_which_re_sends_the_state_command", (1, 0), "open-traffic").await;
}
