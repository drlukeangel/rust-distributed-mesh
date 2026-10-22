//! i146 process E2E: a round's check-ins are owed messages (node-rpc.md §43, `Budget::Owed`), and a
//! fabric-primary held past the staleness floor is superseded, the interim re-sending the round (Q89). Each cell suspends or
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

/// The fabric-primary held during the resize's `round` and released (Q89, fabric-recovery.md "The loss of
/// the fabric-primary while a node-admin lives"; states.md "The round checklist"): mesh2's primary is held
/// in its own `round` action (the fault door's round-action cut), the fabric-primary suspended past the
/// staleness floor and past the 10 s a call had, the action released (mesh2's check-in owed upward), then
/// the fabric-primary continued. Unheard past the floor it is superseded: with another node-admin in its
/// mesh, that standby holds both seats by one row at the first tick after the mark; alone in its mesh, the
/// other mesh's primary runs mesh investigation's ladder on it and takes the fabric seat at the decision
/// step. The interim is the new primary of the round: it re-sends the current round, the members already
/// done check in again, the owed check-in is re-owed to it (node-rpc.md §43), and the fabric reaches
/// ready-for-traffic under the resize. Continued, the lost fabric-primary is commanded back through
/// node.mesh.join by the interim and commands nobody itself; one floor after it is ready the interim hands
/// every seat it took home. Every round action runs once per round key across the seat's move.
async fn fabric_primary_cut_off(test: &str, mesh1: (u32, u32), round: &str) {
    let mut estate = estate_with(test, "answer:0").await;
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
    // The interim the leadership rule names: the standby of the fabric-primary's own mesh, else mesh2's primary.
    let interim = standby.clone().unwrap_or_else(|| primary_s.clone());
    let floor = rdm_transport::membership::staleness_floor();
    // Its own mesh writes at the first tick after the mark; the backbone at the ladder's decision step (twice the floor).
    let seat_moves_within = if standby.is_some() { floor * 3 } else { floor * 5 } + Duration::from_secs(30);

    let door = Door::at(&estate, &primary_s).await;
    door.arm("round-action", json!({"kind": "round-action", "node": primary_s, "round": round})).await;
    let resize = estate.submit(&rdm_client::BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, mesh1.0), (NodeKind::RpcNode, mesh1.1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] })).await;
    let resize_id = resize.build_id().to_string();
    door.wait_held("round-action").await;

    // The fabric-primary is the estate's Day-0 admin: its process is the estate's own child. Control is
    // read through the interim from here: a held admin answers nothing.
    let pid = match estate.bootstrap_pid() {
        Some(p) if fp_s == "mesh1.admin.1" => u64::from(p),
        _ => estate.pid_of(&fp_s).await,
    };
    estate.enter(&Estate::path(&interim)).await;
    let suspended_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    signal(pid, "-STOP");
    let taken = estate
        .until_view(&format!("{interim} holds the fabric seat while {fp_s} is held"), seat_moves_within, |views| {
            views.iter().find(|n| n.is_fabric_primary).filter(|n| n.name.to_string() == interim).map(|n| (n.name.to_string(), n.is_primary))
        })
        .await;
    if standby.is_some() {
        assert!(taken.1, "the standby of the fabric-primary's own mesh holds both seats in the one row: {taken:?}");
    }
    door.release("round-action").await;
    tokio::time::sleep(DEFAULT_CALL_BUDGET + Duration::from_secs(3)).await;
    let released_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    signal(pid, "-CONT");
    estate.follow(resize).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(resize_id.as_str()), Duration::from_secs(120)).await;
    // The seats go home one floor after the node at the original path.name is ready again.
    estate
        .until_view(&format!("{interim} hands the seats back to {fp_s}"), floor * 6 + Duration::from_secs(90), |views| {
            views.iter().find(|n| n.is_fabric_primary).filter(|n| n.name.to_string() == fp_s && n.is_primary).map(|_| ())
        })
        .await;

    estate.stop().await;
    let spans = estate.spans();
    let of_resize = |name: &str, node: &str, r: &str| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node"]) == node && sp["attributes"]["round"] == r && s(&sp["attributes"]["build_id"]) == resize_id).cloned().collect();
        v.sort_by_key(start);
        v
    };
    let takeovers: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-take-over").into_iter().filter(|sp| s(&sp["attributes"]["outcome"]) == "primary-updated" && (suspended_at..released_at).contains(&start(sp))).collect();
    assert_eq!(takeovers.len(), 1, "one take-over while the fabric-primary was held: {takeovers:#?}");
    assert_eq!(s(&takeovers[0]["attributes"]["node"]), interim, "by the interim the election names: {takeovers:#?}");
    // The interim re-sent the current round as its new primary (states.md "The round checklist").
    let re_sent: Vec<Value> = of_resize("rdm.node_admin.fabric.update.via-mesh-round", &interim, round).into_iter().filter(|sp| start(sp) >= start(takeovers[0])).collect();
    assert!(!re_sent.is_empty(), "{interim} ran mesh1's {round} round as the new primary after the takeover: {re_sent:#?}");
    // Every round action ran once per round key on each node across the seat's move (states.md "The round
    // checklist": the key is the Build's attempt and the round; a new fabric-primary that claims the next
    // attempt opens a new key; the record of what ran is each node's own, rdm_node::rounds::ActedRecord):
    // a primary's own action is its own node's work (commit-state writes that node's scratchpad to the
    // store root), so the held fabric-primary's action, finished once it continues, and the standby's as
    // the new primary are two nodes' actions, each run once. mesh1 ran its round for every attempt that
    // reached it; mesh2's primary's and each member's ran once per attempt.
    let attempt_of = |sp: &Value| s(&sp["attributes"]["attempt"]);
    for r in ["commit-state", "open-traffic"] {
        let mesh1_admins: Vec<String> = [Some(fp_s.clone()), standby.clone()].into_iter().flatten().collect();
        let mesh1_rounds: Vec<Value> = mesh1_admins.iter().flat_map(|admin| of_resize("rdm.node_admin.fabric.update.via-mesh-round", admin, r)).collect();
        let attempts: std::collections::BTreeSet<String> = mesh1_rounds.iter().map(attempt_of).collect();
        assert!(!attempts.is_empty(), "mesh1 ran its {r} round");
        for a in &attempts {
            assert!(mesh1_rounds.iter().any(|m| attempt_of(m) == *a && m["attributes"]["own_action"] == "run"), "mesh1's own {r} action ran for attempt {a}");
            for admin in &mesh1_admins {
                let ran = of_resize("rdm.node_admin.fabric.update.via-mesh-round", admin, r).iter().filter(|m| attempt_of(m) == *a && m["attributes"]["own_action"] == "run").count();
                assert!(ran <= 1, "{admin}'s own {r} action ran once for attempt {a}, across the seat's move: {ran}");
            }
        }
        let primary_rounds = of_resize("rdm.node_admin.fabric.update.via-mesh-round", &primary_s, r);
        for a in primary_rounds.iter().map(attempt_of).collect::<std::collections::BTreeSet<_>>() {
            let ran = primary_rounds.iter().filter(|m| attempt_of(m) == a && m["attributes"]["own_action"] == "run").count();
            assert_eq!(ran, 1, "{primary_s}'s own {r} action ran once for attempt {a}");
        }
        for m in &members {
            let member_rounds = of_resize("rdm.node_admin.fabric.update.via-member-round", m, r);
            for a in member_rounds.iter().map(attempt_of).collect::<std::collections::BTreeSet<_>>() {
                let acted = member_rounds.iter().filter(|x| attempt_of(x) == a && flag(&x["attributes"]["acted"])).count();
                assert_eq!(acted, 1, "{m}'s {r} action ran once for attempt {a}");
            }
        }
    }
    // Continued, the lost fabric-primary was commanded back by the interim and commanded nobody itself.
    // The commands sent (a look that sent nothing is spanned too, `sent = false`, with why).
    let commanded: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded").into_iter().filter(|sp| start(sp) >= released_at && sp["attributes"]["sent"] != "false" && sp["attributes"]["sent"] != false).map(|sp| format!("{}:{}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["outcome"]))).collect();
    assert!(commanded.iter().any(|c| c == &format!("{fp_s}:mesh-joined")), "the interim commands the returned fabric-primary back through node.mesh.join: {commanded:?}");
    assert!(commanded.iter().all(|c| c.starts_with(&fp_s)), "the returned fabric-primary commands nobody on its stale book: {commanded:?}");
    let flame = s(&takeovers[0]["trace_id"]);
    println!("fabric-primary held during {round}: {interim} took the seat and re-sent the round; seats home; flame http://192.168.68.99:16687/trace/{flame}");
    estate.record_trace_url(&flame);
}

/// CONTRACT (Q89; states.md "The round checklist": "A new primary, through election or recovery,
/// re-sends the current round. Members already done check in again, and the checklist rebuilds
/// itself"): a fabric-primary held past the staleness floor during state-commit is superseded by its own
/// mesh's standby, which re-sends the round; the members check in again from their record and the fabric
/// reaches ready-for-traffic under the resize; continued, the lost fabric-primary re-enters through
/// node.mesh.join and gets both seats back one floor after it is ready. Must NOT happen: the fabric held
/// in state-commit on the fabric-primary's own check-in, a round action run twice, the seats left with
/// the standby.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_held_during_state_commit_is_superseded_by_its_standby_which_re_sends_the_round() {
    fabric_primary_cut_off("a_fabric_primary_held_during_state_commit_is_superseded_by_its_standby_which_re_sends_the_round", (2, 1), "commit-state").await;
}

/// CONTRACT (Q89): the same for open-traffic: a fabric-primary held past the floor during the open-traffic
/// round is superseded by its standby, which re-sends the round; the fabric reaches ready-for-traffic; no
/// round action runs twice; the seats go home.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_held_during_open_traffic_is_superseded_by_its_standby_which_re_sends_the_round() {
    fabric_primary_cut_off("a_fabric_primary_held_during_open_traffic_is_superseded_by_its_standby_which_re_sends_the_round", (2, 1), "open-traffic").await;
}

/// CONTRACT (Q89, fabric-recovery.md "Outside its mesh"; node-rpc.md §43; fabric-state-commit.md step 3,
/// "node → mesh-primary → fabric-primary"): a mesh primary's check-in is owed to the fabric-primary until
/// it answers, and is re-owed to the holder the seat names next. A fabric-primary that is its mesh's only
/// node, held past the floor, is probed by mesh2's primary and superseded by it at mesh investigation's
/// decision step; mesh2's primary, now the fabric-primary, re-sends the round and the fabric reaches
/// ready-for-traffic under the resize; continued, mesh1's admin re-enters and gets the fabric seat back
/// one floor after it is ready. Must NOT happen: the check-in ending unanswered, the fabric held in
/// state-commit, the seat left with mesh2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_alone_in_its_mesh_held_during_state_commit_is_superseded_at_the_decision_step_by_the_backbone() {
    fabric_primary_cut_off("a_fabric_primary_alone_in_its_mesh_held_during_state_commit_is_superseded_at_the_decision_step_by_the_backbone", (1, 0), "commit-state").await;
}
