//! i143.e12.s6 process E2E: the fabric states and their checklists (states.md; fabric-state-sync.md,
//! fabric-state-commit.md, lifecycles/fabric-ready-for-traffic.md).
//!
//! A real estate whose node-admin is the testkit's, carrying the testkit application
//! (`RDM_TEST_APP`). The fabric-primary alone moves the fabric: pending, state-sync (every mesh
//! ready-for-traffic), state-commit (the application answered state-synced), ready-for-traffic (the
//! commit-state and open-traffic checklists completed). Evidence is read from the spans every process
//! wrote.

use crate::build_log;
use rdm_client::{BuildFrame, BuildStream, CallEnd, Change, ExactBirth, FabricDesired, FabricView, LaunchKind as NodeKind, MeshDesired, NodeView, PathName, ScopeStatus};
use rdm_test::typed::BuildSpec;
use rdm_core::status::StatusReply;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, binding_set_with_roles, candidate_sha, Door};
use serde_json::{json, Value};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-states".into(),
        subfeature: "check-ins".into(),
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

/// A numeric span attribute, exported as a number or as its decimal string.
fn num(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str()?.parse().ok())
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

/// [`estate_with`] whose launch ids also bind the product role binaries (broker, gateway, compute).
async fn estate_with_roles(test: &str, app: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set_with_roles(&sha), &sha, &["broker", "gateway", "compute"], &[("RDM_TEST_APP", app)])
        .await
        .expect("the faulted-admin binding set is accepted")
}

/// Await the fabric under `build` in `state`, as `fabric.get` at the admin the estate asks reports it:
/// the Build the fabric accepted is the one whose change reached the state.
async fn until_fabric(estate: &Estate, state: ScopeStatus, build: Option<&str>, within: Duration) -> FabricView {
    let what = format!("the fabric is {state:?}{}", build.map(|b| format!(" under {b}")).unwrap_or_default());
    estate.fabrics_obj().await.fact(&what, within, |f| f.status == state && build.is_none_or(|b| f.build_id.as_deref() == Some(b))).await.unwrap_or_else(|end| panic!("{end}"))
}

/// The spans of the change whose root carries `build_id`.
fn change_of<'a>(spans: &'a [Value], build_id: &str) -> (&'a Value, Vec<&'a Value>) {
    let root = named(spans, "rdm.node_admin.fabric.update.via-change")
        .into_iter()
        .find(|r| r["attributes"]["build_id"] == build_id)
        .unwrap_or_else(|| panic!("no change of {build_id}"));
    let mut mine: Vec<&Value> = spans.iter().filter(|sp| sp["trace_id"] == root["trace_id"] && start(sp) >= start(root)).collect();
    mine.sort_by_key(|sp| start(sp));
    (root, mine)
}

/// CONTRACT: a fabric whose Build adds a second mesh goes back to pending, and reaches
/// ready-for-traffic only through state-sync, state-commit and open-traffic, in that order, all
/// authored by the fabric-primary. The application is called once at state-sync and its
/// state-synced starts state-commit; every mesh primary and every member takes each checklist's command
/// and checks in; the application's one traffic-opened notice follows ready-for-traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_is_ready_only_through_state_sync_state_commit_and_open_traffic() {
    let test = "a_two_mesh_fabric_is_ready_only_through_state_sync_state_commit_and_open_traffic";
    let estate = estate_with(test, "answer:300").await;
    let members = ["rpc.1", "rpc.2", "rpc.3"];
    two_mesh_change(estate, &[(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 3)], &members).await;
}

/// CONTRACT: a two-mesh fabric whose meshes each hold the product role nodes (a broker, a gateway
/// and a compute) changes to ready-for-traffic exactly as the rpc-node fabric does: every role node
/// takes both state commands, acts through the one member-checklist path, publishes its check-in hook
/// on its mesh channel and completes at its mesh primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_of_role_nodes_is_ready_only_through_the_commit_state_and_open_traffic_check_ins() {
    let test = "a_two_mesh_fabric_of_role_nodes_is_ready_only_through_the_commit_state_and_open_traffic_check_ins";
    let estate = estate_with_roles(test, "answer:300").await;
    let members = ["broker.1", "gateway.1", "compute.1"];
    two_mesh_change(estate, &[(NodeKind::NodeAdmin, 2), (NodeKind::Broker, 1), (NodeKind::Gateway, 1), (NodeKind::Compute, 1)], &members).await;
}

/// CONTRACT: the mesh-primary of a role-node mesh dies and the election names its mesh's other admin
/// mesh-primary. Each role node of that mesh (broker, gateway, compute) sees the mesh-primary move to
/// another birth, owes its state again (the `via-mesh-primary-moved` span, naming the new mesh-primary) and
/// declares it to the new mesh-primary (`via-declare-own`, definitive), the same re-owe the rpc node
/// makes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn role_nodes_declare_their_state_again_to_the_mesh_primary_the_election_names() {
    let test = "role_nodes_declare_their_state_again_to_the_mesh_primary_the_election_names";
    let mut estate = estate_with_roles(test, "answer:0").await;
    let mesh = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::Broker, 1), (NodeKind::Gateway, 1), (NodeKind::Compute, 1)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain(["broker.1", "gateway.1", "compute.1"].iter().map(move |n| format!("{m}.{n}")))).collect();
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(60)).await);
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    // The mesh that is not the fabric-primary's loses its mesh-primary.
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost_mesh = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let old = nodes.iter().find(|n| n["mesh"] == lost_mesh && n["is_primary"] == true && n["kind"] == "node_admin").map(|n| s(&n["name"])).expect("a primary admin");
    let heir = if old.ends_with(".1") { format!("{lost_mesh}.admin.2") } else { format!("{lost_mesh}.admin.1") };
    let heir_id = s(&nodes.iter().find(|n| n["name"] == heir.as_str()).unwrap()["node_id"]);
    if old == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        estate.kill_node(&old).await;
    }
    let roles: Vec<String> = ["broker.1", "gateway.1", "compute.1"].iter().map(|n| format!("{lost_mesh}.{n}")).collect();
    // A role node's re-declaration to the new mesh-primary is a call it makes: no object reports it, and the
    // span the node records is the only place the fact lives. Its `to` names the mesh-primary by path.name
    // when the node's view holds its digest, else by its node id; the call is to that exact birth either
    // way (rdm-node node.rs `declare_once`).
    wait_for("every role node of the mesh declared its state to the new primary", Duration::from_secs(90), || {
        let spans = estate.spans();
        let done = roles.iter().all(|r| {
            named(&spans, "rdm.node_rpc.status.update.via-declare-own").iter().any(|d| s(&d["attributes"]["node"]) == *r && (s(&d["attributes"]["to"]) == heir || s(&d["attributes"]["to"]) == heir_id) && flag(&d["attributes"]["definitive"]))
        });
        async move { done.then_some(()) }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    for r in &roles {
        let moved: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-mesh-primary-moved").into_iter().filter(|m| s(&m["attributes"]["node"]) == *r).collect();
        assert!(moved.iter().any(|m| s(&m["attributes"]["to"]) == heir_id), "{r} saw the mesh-primary move to {heir} ({heir_id}): {:?}", moved.iter().map(|m| &m["attributes"]).collect::<Vec<_>>());
    }
    estate.record_trace_url(&s(&named(&spans, "rdm.node_rpc.status.update.via-mesh-primary-moved").first().expect("a re-owe")["trace_id"]));
}

/// The two-mesh change over meshes of `shape` (their node-admins and members), whose members are named
/// `members` under each mesh.
async fn two_mesh_change(mut estate: Estate, shape: &[(NodeKind, u32)], members: &[&str]) {
    let mesh = |m: &str| MeshDesired::of(m, shape.iter().copied());
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1")] }).await;
    let two_meshes = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await.build_id.to_string();
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain(members.iter().map(move |n| format!("{m}.{n}"))))
        .collect();
    estate.settles_to_named(&want, Duration::from_secs(60)).await;
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(two_meshes.as_str()), Duration::from_secs(60)).await;
    let nodes = estate.node_values().await;
    estate.artifact("nodes.json", &json!(nodes));
    let primaries: std::collections::BTreeSet<String> = nodes.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).collect();
    // The gossip hooks of the last checklist are read after the stop, and the last of them is still in
    // flight when the fabric turns ready: a member's `traffic-opened` goes to its primary on the mesh
    // channel, onto the backbone by that primary, and onto the peer mesh's channel by the peer's
    // primary, while the fabric-primary completes the checklist on the member's declaration, not on
    // the hook (at `ce9224ae` mesh2.broker.1's hook reached its primary 71 ms after the fabric was
    // ready and the stop came 55 ms after that, before mesh1's admins heard the backbone hop). The
    // estate is stopped once every member's check-in has made its last hop, bounded; a hook that has
    // not landed by then fails the assertions below by name.
    {
        let id_of: std::collections::BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
        let landed_by = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let spans = estate.spans();
            let frames = named(&spans, "rdm.mesh.topology.update.via-state-frame");
            let landed = want.iter().filter(|n| !n.contains(".admin.")).all(|subject| {
                let other = if subject.starts_with("mesh1.") { "mesh2." } else { "mesh1." };
                let peer_primary = primaries.iter().find(|p| p.starts_with(other)).cloned().unwrap_or_default();
                frames.iter().any(|f| f["attributes"]["kind"] == "traffic-opened" && f["attributes"]["via"] == "mesh-channel" && s(&f["attributes"]["subject_id"]) == id_of[subject] && s(&f["attributes"]["forwarded_by"]) == peer_primary)
            });
            if landed || std::time::Instant::now() >= landed_by {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(primaries.len(), 2, "one primary per mesh: {primaries:?}");
    let (root, change) = change_of(&spans, &two_meshes);
    let fp = s(&root["attributes"]["node"]);
    let at = |name: &str, f: &dyn Fn(&Value) -> bool| -> &Value { change.iter().copied().find(|sp| sp["name"] == name && f(sp)).unwrap_or_else(|| panic!("no {name} in the change")) };
    let entered = |to: &str| at("rdm.node_admin.fabric.update.via-state-entered", &|sp| sp["attributes"]["to"] == to);
    let opened = |state_key: &str| at("rdm.node_admin.fabric.update.via-checklist-opened", &|sp| sp["attributes"]["command"] == state_key);
    let complete = |state_key: &str| at("rdm.node_admin.fabric.update.via-checklist-complete", &|sp| sp["attributes"]["command"] == state_key);
    let hook = at("rdm.node_admin.fabric.update.via-sync-state-hook", &|_| true);
    let app = at("rdm.testkit.app.update.via-sync-state", &|_| true);
    let synced = at("rdm.node_admin.fabric.update.via-state-synced", &|_| true);
    let notice = at("rdm.node_admin.fabric.update.via-traffic-opened-hook", &|_| true);
    // `already-applied` when an earlier change of this fabric put it (the one-mesh Build's, before the resize).
    let ready_row = at("rdm.node_admin.fabric.update.via-ready-row-put", &|sp| matches!(sp["attributes"]["outcome"].as_str(), Some("applied" | "already-applied")) && s(&sp["attributes"]["node"]) == fp);
    assert!(ready_row["end_unix_nano"].as_u64().unwrap() <= start(entered("ready-for-traffic")), "the row is durable before the fabric-primary enters ready-for-traffic: {ready_row}");
    let order = [
        ("state-sync", start(entered("state-sync"))),
        ("sync_state hook", start(hook)),
        ("application work", start(app)),
        ("state-synced", start(synced)),
        ("state-commit", start(entered("state-commit"))),
        ("commit-state opened", start(opened("commit-state"))),
        ("commit-state complete", start(complete("commit-state"))),
        ("open-traffic opened", start(opened("open-traffic"))),
        ("open-traffic complete", start(complete("open-traffic"))),
        // The fact is durable before anything acts on it: the fabric-primary puts its own mesh's
        // ready-for-traffic row on the store root, then enters ready-for-traffic and admits workflows.
        ("ready-for-traffic row put", start(ready_row)),
        ("ready-for-traffic", start(entered("ready-for-traffic"))),
        ("traffic_opened notice", start(notice)),
    ];
    for w in order.windows(2) {
        assert!(w[0].1 <= w[1].1, "{} came after {}: {order:?}", w[0].0, w[1].0);
    }
    assert!(app["end_unix_nano"].as_u64().unwrap() - start(app) >= 250_000_000, "the application's simulated work ran inside the hook: {app}");
    assert_eq!(hook["attributes"]["outcome"], "state-synced", "{hook}");
    assert_eq!(named(&spans, "rdm.testkit.app.update.via-sync-state").iter().filter(|a| a["attributes"]["build_id"] == two_meshes.as_str()).count(), 1, "the application is called once per checklist of state-sync");

    // Each checklist: the fabric-primary commands the other mesh's primary, every member of both meshes takes the command and checks in.
    for (state_key, completion) in [("commit-state", "state-committed"), ("open-traffic", "traffic-opened")] {
        let o = opened(state_key);
        let c = complete(state_key);
        let in_checklist = |sp: &&Value| start(sp) >= start(o) && start(sp) <= start(c) + 2_000_000_000;
        let commands: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-state-command").into_iter().filter(in_checklist).filter(|sp| sp["attributes"]["command"] == state_key).collect();
        let to: std::collections::BTreeSet<String> = commands.iter().filter(|c| s(&c["attributes"]["node"]) == fp).map(|c| s(&c["attributes"]["to"])).collect();
        assert!(to.iter().any(|t| t.starts_with("mesh2.admin.")), "{state_key}: the fabric-primary {fp} commanded mesh2's primary: {to:?}");
        let members: std::collections::BTreeSet<String> = named(&spans, "rdm.node_admin.fabric.update.via-member-check-in").into_iter().filter(in_checklist).filter(|sp| sp["attributes"]["command"] == state_key).map(|sp| s(&sp["attributes"]["node"])).collect();
        let expected: std::collections::BTreeSet<String> = want.difference(&primaries).cloned().collect();
        assert!(expected.is_subset(&members), "{state_key}: every member that is not a primary took the command and acted; missing {:?}", expected.difference(&members).collect::<Vec<_>>());
        let ups: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-state-op").into_iter().filter(in_checklist).filter(|sp| sp["attributes"]["op"] == completion).collect();
        assert!(ups.iter().all(|u| u["attributes"]["outcome"] == "applied"), "{state_key}: every {completion} was applied: {ups:#?}");
        assert!(ups.iter().any(|u| s(&u["attributes"]["node"]) == fp && s(&u["attributes"]["sender"]).starts_with("mesh2.admin.")), "{state_key}: the fabric-primary took mesh2's {completion}");
    }

    // The gossip hooks (gossip.md, "Fabric state gossip hooks"): each command is announced by its
    // commanding authority on its mesh channel and the backbone before the call; each check-in by the
    // subject (a node-admin on both channels, an ordinary node on its mesh channel, which its
    // mesh-admin carries onto the backbone); and a peer mesh's primary forwards them.
    let id_of: std::collections::BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    let name_of: std::collections::BTreeMap<&String, &String> = id_of.iter().map(|(n, i)| (i, n)).collect();
    let primary_of = |mesh: &str| primaries.iter().find(|p| p.starts_with(&format!("{mesh}."))).cloned().expect("a primary per mesh");
    let hooks: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-state-hook").into_iter().filter(|h| start(h) >= start(root) && flag(&h["attributes"]["bound"])).collect();
    let frames: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-state-frame").into_iter().filter(|f| start(f) >= start(root)).collect();
    let mut firsts = Vec::new();
    for (state_key, command_frame, completion, completion_frame) in [("commit-state", "state-committing", "state-committed", "state-committed"), ("open-traffic", "traffic-opening", "traffic-opened", "traffic-opened")] {
        let sent = |hook: &str, subject: &str| -> Vec<&Value> { hooks.iter().copied().filter(|h| h["attributes"]["hook"] == hook && s(&h["attributes"]["subject_id"]) == id_of[subject]).collect() };
        let mut cmd_at = Vec::new();
        let mut done_at = Vec::new();
        for subject in want.iter().filter(|n| **n != fp) {
            let mesh = subject.split('.').next().unwrap();
            let authority = if *subject == primary_of(mesh) { fp.clone() } else { primary_of(mesh) };
            let cmd = sent(state_key, subject);
            assert!(!cmd.is_empty(), "{state_key}: the commanding authority announced the command to {subject}");
            assert!(cmd.iter().all(|h| s(&h["attributes"]["node"]) == authority && h["attributes"]["channels"] == "mesh,backbone"), "{state_key}: {subject} was announced by {authority} on its mesh channel and the backbone: {cmd:?}");
            let done = sent(completion, subject);
            assert!(!done.is_empty(), "{state_key}: {subject} announced its {completion} check-in");
            let channels = if subject.contains(".admin.") { "mesh,backbone" } else { "mesh" };
            assert!(done.iter().all(|h| s(&h["attributes"]["node"]) == *subject && h["attributes"]["channels"] == channels), "{state_key}: {subject} announced {completion} itself on {channels}: {done:?}");
            assert!(start(cmd[0]) <= start(done[0]), "{state_key}: the command to {subject} was announced before its check-in");
            cmd_at.push(start(cmd[0]));
            done_at.push(start(done[0]));
            // An ordinary node's check-in reaches the backbone through its mesh-admin; a node-admin's is its own.
            if !subject.contains(".admin.") {
                let carried = frames.iter().any(|f| f["attributes"]["kind"] == completion_frame && f["attributes"]["via"] == "backbone" && s(&f["attributes"]["subject_id"]) == id_of[subject.as_str()] && s(&f["attributes"]["forwarded_by"]) == authority);
                assert!(carried, "{state_key}: {authority} carried {subject}'s {completion_frame} onto the backbone");
            }
            let heard_by_members = frames.iter().any(|f| f["attributes"]["kind"] == command_frame && f["attributes"]["via"] == "mesh-channel" && s(&f["attributes"]["subject_id"]) == id_of[subject.as_str()] && s(&f["attributes"]["forwarded_by"]).is_empty());
            assert!(heard_by_members, "{state_key}: the {command_frame} for {subject} was heard on a mesh channel as its author sent it");
        }
        // A peer mesh's primary forwards the other mesh's hooks onto its own channel, the author unchanged.
        for (from, to) in [("mesh1", "mesh2"), ("mesh2", "mesh1")] {
            for kind in [command_frame, completion_frame] {
                let forwarded = frames.iter().any(|f| {
                    f["attributes"]["kind"] == kind
                        && f["attributes"]["via"] == "mesh-channel"
                        && s(&f["attributes"]["publisher"]).starts_with(&format!("{from}."))
                        && s(&f["attributes"]["node"]).starts_with(&format!("{to}."))
                        && s(&f["attributes"]["forwarded_by"]) == primary_of(to)
                });
                assert!(forwarded, "{kind} authored in {from} was forwarded onto {to}'s channel by {}", primary_of(to));
            }
        }
        assert!(frames.iter().all(|f| name_of.contains_key(&s(&f["attributes"]["subject_id"]))), "every hook frame names a node of this fabric");
        firsts.push((state_key, *cmd_at.iter().min().unwrap(), *done_at.iter().max().unwrap()));
    }
    assert!(firsts[0].1 <= firsts[0].2 && firsts[0].2 <= firsts[1].1 && firsts[1].1 <= firsts[1].2, "StateCommitting, StateCommitted, TrafficOpening, TrafficOpened came in that order: {firsts:?}");

    // A command is sent again only on an eligible event (R-S2), named on the span that sends it: a birth
    // is commanded once as `first`, a send after it carries the event that made it due, and nothing is
    // sent again after the birth answered.
    let mut sends: std::collections::BTreeMap<(String, String, String), Vec<&Value>> = std::collections::BTreeMap::new();
    for c in named(&spans, "rdm.node_admin.fabric.update.via-state-command").into_iter().filter(|c| start(c) >= start(root)) {
        sends.entry((s(&c["attributes"]["node"]), s(&c["attributes"]["command"]), s(&c["attributes"]["to"]))).or_default().push(c);
    }
    assert!(!sends.is_empty());
    let mut resends = 0;
    for ((from, state_key, to), calls) in &mut sends {
        calls.sort_by_key(|c| start(c));
        assert_eq!(calls.iter().filter(|c| c["attributes"]["event"] == "first").count(), 1, "{from} commanded {to} once as first in {state_key}: {calls:?}");
        assert_eq!(calls[0]["attributes"]["event"], "first", "{from}'s first send to {to} in {state_key} is the first");
        for (i, c) in calls.iter().enumerate().skip(1) {
            resends += 1;
            let event = s(&c["attributes"]["event"]);
            assert!(["addressed", "destination-changed", "reachable-again", "state-changed"].contains(&event.as_str()), "{from} sent {state_key} to {to} again with no eligible event: {event}");
            let before = s(&calls[i - 1]["attributes"]["outcome"]);
            assert!(before != "applied" && before != "already-applied", "{from} sent {state_key} to {to} again after it answered {before}");
        }
    }
    eprintln!("[fabric_states] {} commands to {} (command, birth) pairs, {resends} sent again on an eligible event", sends.values().map(Vec::len).sum::<usize>(), sends.len());

    // One trace: every call of the change has its receiver's serve span under it, every mesh ran both
    // checklists in it, and mesh2's primary reported each checklist up in a new caller span of the same trace.
    let count = |name: &str| change.iter().filter(|sp| sp["name"] == name).count();
    assert_eq!(count("rdm.node_rpc.request.update.via-call"), count("rdm.node_rpc.request.serve.via-direct"), "every call of the change was served under it");
    assert_eq!(count("rdm.node_admin.fabric.update.via-mesh-checklist"), 4, "two meshes ran two checklists each inside the change's trace");
    assert_eq!(count("rdm.node_admin.status.update.via-state-completion"), 2, "the other mesh's primary reported each checklist up in the change's trace");

    // The fabric states went out as FabricStatus authored by the fabric-primary only.
    let sends: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| sp["attributes"]["scope"].as_str().is_some_and(|x| x.contains("fabric")) && start(sp) >= start(root)).collect();
    assert!(!sends.is_empty(), "FabricStatus sends are in the evidence");
    assert!(sends.iter().all(|sp| s(&sp["attributes"]["node"]) == fp), "only the fabric-primary authors FabricStatus: {:?}", sends.iter().map(|sp| (&sp["attributes"]["node"], &sp["attributes"]["status"])).collect::<Vec<_>>());
    let mut seen: Vec<String> = Vec::new();
    for sp in &sends {
        let st = s(&sp["attributes"]["status"]);
        if seen.last() != Some(&st) {
            seen.push(st);
        }
    }
    let change_states: Vec<&str> = vec!["state-sync", "state-commit", "ready-for-traffic"];
    assert!(seen.windows(3).any(|w| w == change_states), "FabricStatus went state-sync, state-commit, ready-for-traffic: {seen:?}");
    estate.record_trace_url(&s(&root["trace_id"]));
}

/// Every row under the fabric's store root, sorted: each record of a store file as
/// `<file>#<kind>/<key>` (its latest line), and every other file as a path relative to the root. The
/// directories the stores open, an empty store file and the layout marker are not rows.
fn rows_on(root: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            let rel = p.strip_prefix(root).unwrap().display().to_string();
            if p.is_dir() {
                walk(&p, root, out);
            } else if rel.ends_with(".jsonl") && dir == root {
                // A store file read while its nodes append: a line caught mid-write is read again.
                let latest = (0..50).find_map(|_| rdm_admin::store_log::latest_in(&p).ok().or_else(|| { std::thread::sleep(std::time::Duration::from_millis(20)); None })).unwrap_or_else(|| panic!("{} reads whole", p.display()));
                out.extend(latest.into_keys().map(|(kind, key)| format!("{rel}#{kind}/{key}")));
            } else if rel != rdm_admin::store_root::LAYOUT && !rel.ends_with(".reclaim") {
                out.push(rel);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Wait until the fabric is ready-for-traffic under the change of `build`.
async fn ready_under(estate: &Estate, build: &str) {
    until_fabric(estate, ScopeStatus::ReadyForTraffic, Some(build), Duration::from_secs(90)).await;
}

/// Read `stream`'s frames until the drive reports its Build Blocked naming the fabric's change, bounded by
/// `within`. A `Complete` before it is the defect: the Build said Complete while the fabric had not
/// changed under it, named with what `fabric.get` reports.
async fn until_blocked_on_the_fabric_change(estate: &Estate, stream: &mut BuildStream, within: Duration) -> BuildFrame {
    let build = stream.build_id().to_string();
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let frame = match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) => panic!("Build {build} reported no Blocked naming the fabric's change within {within:?}"),
            Ok(None) => panic!("the stream of Build {build} ended before it reported Blocked naming the fabric's change"),
            Ok(Some(Err(e))) => panic!("the stream of Build {build} broke before it reported Blocked naming the fabric's change: {e}"),
            Ok(Some(Ok(frame))) => frame,
        };
        match &frame {
            BuildFrame::Blocked { step, .. } if step == "fabric-change" => return frame,
            BuildFrame::Complete { attempt } => {
                let fabric = estate.fabric_view().await;
                panic!("Build {build} is Complete at attempt {attempt} while fabric.get says {:?} under {:?}: a Build that put the fabric back to pending is Complete only once its own ready-for-traffic state is set", fabric.status, fabric.build_id);
            }
            BuildFrame::Failed { .. } | BuildFrame::FabricPrimaryMoved { .. } => panic!("Build {build} ended {frame:?} before it reported Blocked naming the fabric's change"),
            _ => {}
        }
    }
}

/// `stream` followed to its `Complete` (`Estate::follow`), bounded by `within`: `what` names the wait.
async fn followed(estate: &Estate, stream: BuildStream, what: &str, within: Duration) -> rdm_test::typed::Reconciled {
    tokio::time::timeout(within, estate.follow(stream)).await.unwrap_or_else(|_| panic!("{what}: no Complete within {within:?}"))
}

/// CONTRACT (Q154; states.md "Resize and executable replacement": "Its Build is Complete only when the fabric is
/// ready-for-traffic again from this Build's own change, as fabric.create's is: while the fabric changes,
/// the Build reports Blocked naming the change, never Complete"; build.md §10: "A Build that put the
/// fabric back to pending completes when its ready-for-traffic state is set, not with its last step"): the application answers
/// Day 0's state-sync, so the Day-0 Build is Complete from its own ready-for-traffic; a resize whose change the
/// application holds in state-sync reports Blocked naming `fabric-change`, and while `fabric.get` says
/// state-sync under it neither its stream nor `build.get` says Complete; released, it is Complete only
/// after the fabric entered ready-for-traffic under it. Must NOT happen: a Build Complete while the
/// fabric it put back to pending has not changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resize_is_blocked_on_the_fabric_change_and_complete_only_after_its_own_ready_for_traffic_state_is_set() {
    let test = "a_resize_is_blocked_on_the_fabric_change_and_complete_only_after_its_own_ready_for_traffic_state_is_set";
    let mut estate = estate_with(test, "after-day0:hold").await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    let shape = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]);
    let mut stream = estate.submit(&BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![shape("mesh1"), shape("mesh2")] })).await;
    let build = stream.build_id().to_string();
    let blocked = until_blocked_on_the_fabric_change(&estate, &mut stream, Duration::from_secs(90)).await;
    estate.artifact("blocked-on-the-fabric-change.json", &json!(format!("{blocked:?}")));
    until_fabric(&estate, ScopeStatus::StateSync, Some(build.as_str()), Duration::from_secs(60)).await;
    door.wait_held("state-sync").await;
    // The application holds its answer: through the window fabric.get says state-sync under the Build, and
    // neither the Build's stream nor build.get says Complete.
    let id = rdm_client::BuildId::parse(&build).unwrap();
    let carrier = estate.carrier().await;
    let held_until = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < held_until {
        let fabric = estate.fabric_view().await;
        assert_eq!((fabric.status, fabric.build_id.as_deref()), (ScopeStatus::StateSync, Some(build.as_str())), "the fabric is held in state-sync under {build}");
        let got = carrier.get(&id).await.unwrap_or_else(|e| panic!("build.get {build}: {e}"));
        assert_ne!(got.phase, rdm_core::build::BuildPhase::Complete, "build.get says {build} Complete at attempt {} while fabric.get says state-sync under it", got.attempt);
        if let Ok(Some(frame)) = tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            let frame = frame.unwrap_or_else(|e| panic!("the stream of Build {build} broke while the fabric was held in state-sync: {e}"));
            assert!(!matches!(frame, BuildFrame::Complete { .. } | BuildFrame::Failed { .. }), "Build {build} ended {frame:?} while fabric.get says state-sync under it");
        }
    }
    door.release("state-sync").await;
    let done = followed(&estate, stream, &format!("Build {build} after the state-sync was released"), Duration::from_secs(90)).await;
    assert_eq!(carrier.get(&id).await.unwrap_or_else(|e| panic!("build.get {build}: {e}")).phase, rdm_core::build::BuildPhase::Complete, "build.get says {build} Complete once its stream did");
    ready_under(&estate, &build).await;
    estate.stop().await;

    // The order, from the spans: the fabric entered ready-for-traffic under this Build's change before the
    // attempt that completed it was dispatched.
    let spans = estate.spans();
    let (_, change) = change_of(&spans, &build);
    let ready = change.iter().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-state-entered" && sp["attributes"]["to"] == "ready-for-traffic").unwrap_or_else(|| panic!("the fabric entered ready-for-traffic under the change of {build}"));
    let completed = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| sp["attributes"]["build_id"] == build.as_str() && sp["attributes"]["outcome"] == "complete").unwrap_or_else(|| panic!("the attempt that completed {build} is spanned"));
    let Some(BuildFrame::Complete { attempt }) = done.frames.last() else { panic!("Build {build} ended {:?}", done.frames.last()) };
    assert_eq!(num(&completed["attributes"]["attempt"]), Some(u64::from(*attempt)), "the dispatch span is of the attempt the stream reported Complete: {completed}");
    assert!(start(ready) <= start(completed), "the fabric entered ready-for-traffic under {build} ({}) before the attempt that completed it was dispatched ({}): {completed}", start(ready), start(completed));
    estate.record_trace_url(&s(&ready["trace_id"]));
}

/// CONTRACT (Q158; states.md "Resize and executable replacement": "a state's check-in or a hook that reports failure fails
/// the Build on that step"; build.md: hooks "report failure through the lifecycle receipt path"): a
/// resize whose `sync_state` the application answers with its failure by name (Day 0's it answered, so the
/// Day-0 Build is Complete from its own ready-for-traffic) ends `Failed` on the step
/// it was Blocked on, `fabric-change`, its operation `state-sync:<fabric_id>`, its reason carrying the
/// application's; `build.get` reads it Failed with that reason, and the fabric never reaches
/// ready-for-traffic under it. Must NOT happen: a reported failure leaving the Build Blocked with no end,
/// or the Build Complete over a failed state-sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resize_whose_sync_state_reports_failure_fails_on_the_fabric_change_step_with_the_reason() {
    let test = "a_resize_whose_sync_state_reports_failure_fails_on_the_fabric_change_step_with_the_reason";
    let named_reason = "seed-image-unreadable";
    let mut estate = estate_with(test, &format!("after-day0:fail:{named_reason}")).await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    let shape = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]);
    let mut stream = estate.submit(&BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![shape("mesh1"), shape("mesh2")] })).await;
    let build = stream.build_id().to_string();
    until_blocked_on_the_fabric_change(&estate, &mut stream, Duration::from_secs(90)).await;
    until_fabric(&estate, ScopeStatus::StateSync, Some(build.as_str()), Duration::from_secs(60)).await;
    door.wait_held("state-sync").await;
    // The application's work ends: it reports failure, naming why.
    door.release("state-sync").await;
    let within = Duration::from_secs(60);
    let deadline = tokio::time::Instant::now() + within;
    let failed = loop {
        let frame = match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) => panic!("Build {build} did not end Failed within {within:?} of its sync_state reporting failure ({named_reason}): fabric.get says {:?}", estate.fabric_view().await.status),
            Ok(None) => panic!("the stream of Build {build} ended before its Failed"),
            Ok(Some(Err(e))) => panic!("the stream of Build {build} broke before its Failed: {e}"),
            Ok(Some(Ok(frame))) => frame,
        };
        match frame {
            BuildFrame::Failed { .. } => break frame,
            BuildFrame::Complete { .. } | BuildFrame::FabricPrimaryMoved { .. } => panic!("Build {build} ended {frame:?} after its sync_state reported failure"),
            _ => {}
        }
    };
    estate.artifact("failed-on-the-fabric-change.json", &json!(format!("{failed:?}")));
    let BuildFrame::Failed { operation, step, reason, .. } = &failed else { unreachable!() };
    assert_eq!(step, "fabric-change", "the Build fails on the step it was Blocked on: {failed:?}");
    assert_eq!(operation, &format!("state-sync:{}", estate.fabric_id), "the operation is the state-sync the application answered: {failed:?}");
    assert!(reason.contains(named_reason), "the Failed carries the application's reason {named_reason}: {failed:?}");
    let id = rdm_client::BuildId::parse(&build).unwrap();
    let got = estate.carrier().await.get(&id).await.unwrap_or_else(|e| panic!("build.get {build}: {e}"));
    assert_eq!(got.phase, rdm_core::build::BuildPhase::Failed, "build.get says {build} Failed: {got:?}");
    assert!(got.last_failure.as_deref().is_some_and(|f| f.contains(named_reason)), "build.get carries the application's reason as the last failure: {got:?}");
    let fabric = estate.fabric_view().await;
    assert!(!(fabric.status == ScopeStatus::ReadyForTraffic && fabric.build_id.as_deref() == Some(build.as_str())), "the fabric is never ready-for-traffic under the failed Build {build}: {fabric:?}");
    estate.stop().await;

    let spans = estate.spans();
    let hook = named(&spans, "rdm.node_admin.fabric.update.via-sync-state-hook")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == build.as_str() && sp["attributes"]["outcome"] == "failed")
        .unwrap_or_else(|| panic!("the sync_state call of {build} is spanned with its failure"));
    assert!(s(&hook["attributes"]["reason"]).contains(named_reason), "the hook's span names the application's reason: {hook}");
    estate.record_trace_url(&s(&hook["trace_id"]));
}

/// CONTRACT (fabric-create.md, Entry: "Day 0 is the first node-admin and its own mesh, nothing else. Every
/// further mesh, and every other node, is created by a Build accepted after the fabric's first
/// ready-for-traffic (Q170)"; Rows: "The fabric-primary creates the fabric row and its own mesh's row at
/// Day 0, held in the scratchpad until state-commit"; states.md, Setting a state: "Before state-commit, RDM
/// holds the Fabric and Mesh records in the scratchpad; state-commit publishes them to the configured
/// stores. A Node record is written straight to the store, before state-commit too"; fabric-state-commit.md
/// step 2: "a node writes its connection rows; a mesh primary writes its members' Node rows and its Mesh
/// row; the fabric-primary writes the Fabric row"; node-admin-lifecycle.md §7: "scratchpad before
/// state-commit, then each writer writes what it holds to the root, and writes go there directly
/// afterwards"; node-identity-and-state.md §2.7, The write: "After that it writes each create and update at
/// once"; R14 item 9).
///
/// Before the first state-commit: while the application holds Day 0 in state-sync, its one node, the
/// fabric-primary mesh1.admin.1 (mesh1's primary too), up and ready, the fabric's store root holds only
/// the acknowledged declared-state rows (Q54) and the Day-0 Build's committed facts, its lanes' keys and
/// its mesh's attempt claims (Q61, build.md §5.2): mesh1.admin.1's Node record, with its name claim and its
/// path.name entry, its declared states its own ReadyForTraffic alone (the Day-0 root joins nobody, so no
/// join report's Pending is on it: Q97, node-mesh-join.md step 10). The Fabric row, mesh1's Mesh row, its
/// Mesh name claim and the fabric's name claim (R-FN1) stay in the fabric-primary's scratchpad. The first
/// state-commit checklist publishes them: the fabric-primary's commit-state action writes what it holds
/// (its `fabrics` and `fabric` stores, mesh1's Mesh record and name claim; no connections row, since it
/// originated no data-plane connection: connections.md "The data plane is the connections log", R-CR6: the
/// control plane writes nothing to it) before its checklist completes.
///
/// After the first state-commit the store exists: a growth Build accepted after the first
/// ready-for-traffic adds mesh2 (its node-admin and an rpc node) and an rpc node to mesh1, and each of its
/// writers writes straight to the store: mesh2's primary its Mesh record and name claim, mesh2.rpc.1 the
/// connection it originates to mesh1.rpc.1 (its connections store opened on the store root), so the growth
/// checklist's commit-state action of every node has nothing held to publish.
///
/// `fabric/` has one writer, the fabric-primary (R14 item 2: "Fabric-primary: `fabric/`. ... Keys
/// never overlap across writers"): only its publication names a `fabric/` store, and mesh2's primary's
/// action names none. The Fabric row carries no pointer to the newest Build (R-BI3): when the
/// fabric-primary accepts the growth Build, it is written once to the builds store after the Day-0 Build
/// and nothing is written under `fabric/` for it, and mesh2's primary reads it as the current Build.
///
/// The growth adds nodes, a resize, and a resize puts the fabric back to pending (states.md, Resize and
/// executable replacement): the fabric-primary that accepts it holds the fabric pending before its
/// build.create door answers, so no `fabric.get` after the answer pairs the growth Build with the
/// ready-for-traffic the Day-0 Build's change reached, and the growth Build changes its own checklists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_store_root_holds_only_acknowledged_declarations_and_build_facts_until_state_commit_publishes_other_rows() {
    let test = "the_store_root_holds_only_acknowledged_declarations_and_build_facts_until_state_commit_publishes_other_rows";
    let mut estate = estate_with(test, "hold").await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    let root = estate.fabric_store_root();
    // The first Build is the Day-0 Build (fabric-create.md, Q170): no other Build is accepted before its own
    // ready-for-traffic, which the application holds. It is Complete only once its ready-for-traffic state is
    // set (Q154): it is followed from its first attempt to its Complete once the hold is released.
    let first = estate.accepted_build_id().await;
    let first_id = rdm_client::BuildId::parse(&first).unwrap_or_else(|e| panic!("fabric.get names the Day-0 Build {first}, not a Build id: {e}"));
    let mut first_stream = estate.carrier().await.resubmit(&first_id, 1).await.unwrap_or_else(|e| panic!("the fabric-primary does not stream the Day-0 Build {first}: {e}"));
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1"].iter().map(|n| n.to_string()).collect();
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(60)).await);
    // The application took Day 0's state-sync checklist and the door holds its answer: the fabric cannot
    // reach state-commit, and the Day-0 Build's run is handed back for the fabric's change, none of its
    // attempts in flight.
    until_fabric(&estate, ScopeStatus::StateSync, Some(first.as_str()), Duration::from_secs(60)).await;
    door.wait_held("state-sync").await;
    until_blocked_on_the_fabric_change(&estate, &mut first_stream, Duration::from_secs(60)).await;
    let before = rows_on(&root);
    estate.artifact("rows-before-state-commit.json", &json!(before));
    // Q54 with whole records (storage-whole-records.md, "Durability"): a Node record is written straight
    // to the store, before state-commit too, with its name claim (its path.name entry is the fold of its line); Q97: the
    // Day-0 root joined nobody, so its record holds its ReadyForTraffic alone.
    let mut declarations: Vec<String> = nodes.iter().flat_map(|n| {
        let name = s(&n["name"]);
        vec![format!("locks/name/{name}"), format!("nodes.jsonl#node-record/{}", s(&n["node_id"]))]
    }).collect();
    declarations.sort();
    for n in &nodes {
        let name = s(&n["name"]);
        let mesh = name.split('.').next().unwrap().to_string();
        let want: &[&str] = &["ReadyForTraffic"];
        // A mesh-primary's own ReadyForTraffic has no Applied reply to be durable before (states.md:51): its
        // writer records it on its own record after the view shows it, so the cell reads until it is there.
        let read = || {
            let record: Value = estate.node_record(&s(&n["node_id"])).unwrap_or_else(|| panic!("{mesh}: the record of {name} is in nodes.jsonl"));
            let birth = record["record"]["births"].as_array().into_iter().flatten().find(|b| b["incarnation_id"] == n["incarnation_id"]).cloned().unwrap_or_default();
            (birth["declared"].clone(), record)
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let (declared, record) = loop {
            let (declared, record) = read();
            if declared == json!(want) || std::time::Instant::now() > deadline {
                break (declared, record);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(declared, json!(want), "{name}'s record holds the state its ready recorded within 30 s: {record}");
    }
    // Q61: a committed Build fact stands on the root when it is committed: the lanes' keys and the
    // mesh's attempt claims are there before state-commit, beside the declarations.
    let (build_facts, rows): (Vec<String>, Vec<String>) = before.iter().cloned().partition(|r| r.starts_with("builds.jsonl#") || r.starts_with("locks/in-flight/"));
    // A birth's runtime row is its launching admin's durable map, appended at once, before
    // state-commit too (R-SR1: the fourth writer to the root before it).
    let (runtimes, rows): (Vec<String>, Vec<String>) = rows.into_iter().partition(|r| r.starts_with("nodes.jsonl#node-runtime/"));
    assert!(runtimes.len() <= want.len(), "at most one runtime row per launched birth before state-commit: {runtimes:#?}");
    assert!(build_facts.iter().any(|r| r.starts_with("builds.jsonl#build-record/")), "Q61: the Day-0 Build's committed facts stand on {} before state-commit: {before:#?}", root.display());
    assert_eq!(rows, declarations, "Q54, Q97: of every other row, only the Day-0 root's node record, with its name claim (its path.name entry the fold of its line), reaches {} before state-commit; the Fabric row and mesh1's Mesh row and name claim stay held", root.display());
    // R-FN1: the fabric's claim of its name sits beside the per-fabric roots, at
    // `<store_dir>/fabrics/fabric1`, and is held in the fabric-primary's scratchpad like every row.
    let name_claim = root.parent().expect("the store root sits under the store_dir").join(rdm_admin::store_root::FABRIC_NAMES).join("fabric1.json");
    assert!(!name_claim.exists(), "the fabric's name claim reaches {} only at state-commit", name_claim.display());
    door.release("state-sync").await;
    followed(&estate, first_stream, &format!("the Day-0 Build {first} after its state-sync was released"), Duration::from_secs(90)).await;
    ready_under(&estate, &first).await;
    let after = rows_on(&root);
    estate.artifact("rows-after-state-commit.json", &json!(after));
    let claimed: Value = serde_json::from_slice(&std::fs::read(&name_claim).unwrap_or_else(|e| panic!("the state-commit published the fabric's name claim at {}: {e}", name_claim.display()))).unwrap();
    assert_eq!(claimed["record"]["fabric_id"].as_str(), root.file_name().and_then(|n| n.to_str()), "the name claim names this fabric: {claimed}");
    let holds = |rows: &[String], prefix: &str| rows.iter().any(|r| r.starts_with(prefix));
    assert!(holds(&after, "fabric.jsonl#fabric-record/"), "the fabric-primary published the Fabric record: {after:#?}");
    assert!(holds(&after, "builds.jsonl#build-record/"), "the fabric-primary's Build journal stands in builds.jsonl (R-BI6): {after:#?}");
    // The per-seat rows under `fabric/seats` go (the approved proposal
    // node-primary-update-fabric-mesh-and-node-type.md, Part C2: "the leadership view is folded from the
    // transfer rows"): under fabric/ the fabric-primary holds its identity row, its `fabric/builds/`
    // Build journal (R-BI6) and the fabric's events (storage-by-object.md), and nothing else.
    let only_identity_builds_and_events = |rows: &[String]| !rows.iter().any(|r| r.starts_with("fabric/")) && rows.iter().filter(|r| r.starts_with("fabric.jsonl#")).all(|r| r.starts_with("fabric.jsonl#fabric-record/"));
    assert!(only_identity_builds_and_events(&after), "the Fabric row carries no pointer to the newest Build and no seat row stands beside it: the fabric-primary's fabric/ rows are the identity, fabric/builds/ and fabric/events/ and nothing else: {after:#?}");
    assert!(after.iter().any(|r| r == "locks/name/mesh1"), "mesh1's primary published its Mesh name claim: {after:#?}");
    assert_eq!(after.iter().filter(|r| r.starts_with("meshes.jsonl#mesh-record/")).count(), 1, "the Day-0 mesh's record stands on the root, and only it: {after:#?}");
    assert!(!holds(&after, "connections.jsonl#"), "the Day-0 root originated no data-plane connection, so no connections row stands on the root; the control plane writes none: {after:#?}");
    // A mesh's primary records each member's address on its Node record.
    let addressed = |mesh: &str, name: &str, id: &str| {
        let (mesh, name, id) = (mesh.to_string(), name.to_string(), id.to_string());
        let estate = &estate;
        async move {
            let addressed = || estate.node_record(&id).filter(|r| r["record"]["births"].as_array().and_then(|b| b.last()).is_some_and(|b| b["transport_addr"].is_string()));
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while addressed().is_none() && std::time::Instant::now() < until {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(addressed().is_some(), "{mesh}'s primary recorded {name}'s address on its Node record: {:?}", estate.node_record(&id));
        }
    };
    let fp_id = s(&nodes[0]["node_id"]);
    addressed("mesh1", "mesh1.admin.1", &fp_id).await;

    // After the first state-commit the store exists. The growth Build, accepted after the first
    // ready-for-traffic (Q170), adds mesh2 and an rpc node to mesh1: every row its nodes write goes straight
    // to the store, and its checklist's commit-state actions publish nothing: nothing is held any more. The
    // fault cut was spent by its release: the application answers this Build at once.
    let shape = |m: &str, rpc: u32| MeshDesired::of(m, [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, rpc)]);
    let second = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![shape("mesh1", 1), shape("mesh2", 1)] }).await.build_id.to_string();
    let mut want_later = want.clone();
    want_later.extend(["mesh1.rpc.1", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|n| n.to_string()));
    let later_nodes = Estate::values(&estate.settles_to_named(&want_later, Duration::from_secs(60)).await);
    ready_under(&estate, &second).await;
    // mesh2.rpc.1 learns of mesh1.rpc.1 from the MembersDelta its mesh-primary forwards (gossip.md §3.3),
    // not from the admin's view the cell settled on: the origination waits until mesh2.rpc.1's own
    // resolver holds the destination.
    wait_for("mesh2.rpc.1's own resolver holds mesh1.rpc.1", Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", "path:mesh2.rpc.1", "--query", "path:mesh1.rpc.1"]);
        (r["reply"]["resolution"] == "found").then_some(())
    })
    .await;
    let straight = estate.probe(&["originate", "--target", "path:mesh2.rpc.1", "--destination", "path:mesh1.rpc.1", "--key", "after-commit"]);
    estate.artifact("originate-after-state-commit.json", &straight);
    assert_eq!(straight["call_outcome"].as_str(), Some("reply"), "mesh2.rpc.1 originated its connection to mesh1.rpc.1 after state-commit: {straight}");
    let id_of: std::collections::BTreeMap<String, String> = later_nodes.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    for name in &want_later {
        addressed(name.split('.').next().unwrap(), name, &id_of[name]).await;
    }
    // The originated connection's row is owed on mesh2.rpc.1's writer queue and lands after the open
    // answered (Q169): the store is read once it stands, within the bound.
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !holds(&rows_on(&root), "connections.jsonl#node-connection/mesh2.rpc.1--") && std::time::Instant::now() < until {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let later = rows_on(&root);
    estate.artifact("rows-after-growth.json", &json!(later));
    for mesh in ["mesh1", "mesh2"] {
        assert!(later.iter().any(|r| *r == format!("locks/name/{mesh}")), "{mesh}'s primary's Mesh name claim stands on the root: {later:#?}");
    }
    assert_eq!(later.iter().filter(|r| r.starts_with("meshes.jsonl#mesh-record/")).count(), 2, "each mesh's record stands on the root: {later:#?}");
    assert!(holds(&later, "connections.jsonl#node-connection/mesh2.rpc.1--"), "mesh2.rpc.1 wrote the connection it originated straight to the store: {later:#?}");
    for name in want_later.iter().filter(|n| *n != "mesh2.rpc.1") {
        assert!(!holds(&later, &format!("connections.jsonl#node-connection/{name}--")), "{name} originated no connection, so it wrote no connections row; the control plane writes none: {later:#?}");
    }
    // The fabric-primary accepted the growth Build after the first state-commit: the Build is its own
    // entity in the builds store, so no pointer row under `fabric/` names it, and mesh2's primary reads it
    // as the current Build.
    assert!(only_identity_builds_and_events(&later), "accepting the growth Build writes no pointer row under `fabric/` for it: {later:#?}");
    let other_fabric = estate.fabric_view_at(&"mesh2.admin.1".parse().unwrap()).await.expect("mesh2.admin.1 answers fabric.get for this Fabric");
    estate.artifact("mesh2-admin-fabric-after-growth.json", &serde_json::to_value(&other_fabric).expect("a view serializes"));
    assert_eq!(other_fabric.build_id.as_deref(), Some(second.as_str()), "mesh2's primary reads the growth Build as the current Build: {other_fabric:?}");
    estate.stop().await;
    let spans = estate.spans();

    // The growth put the fabric back to pending inside the acceptance: the fabric-primary's reset for it
    // starts before its build.create door answers it (the door's span ends at the answer).
    let door_span = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"] == second.as_str()).unwrap_or_else(|| panic!("the growth Build's build.create door is spanned"));
    let resets = named(&spans, "rdm.node_admin.fabric.update.via-state-reset");
    let reset = resets.iter().find(|sp| sp["attributes"]["build_id"] == second.as_str()).unwrap_or_else(|| panic!("the fabric-primary put the fabric back to pending for the growth Build {second}: resets {:?}", resets.iter().map(|sp| &sp["attributes"]).collect::<Vec<_>>()));
    assert_eq!((s(&reset["attributes"]["node"]), s(&reset["attributes"]["from"])), ("mesh1.admin.1".to_string(), "ready-for-traffic".to_string()), "the fabric-primary put the ready-for-traffic fabric back to pending: {reset}");
    let answered = door_span["end_unix_nano"].as_u64().unwrap();
    assert!(start(reset) <= answered, "the fabric is pending before the door answers the growth Build: reset at {}, door answered at {answered} ({} us later than the answer): {reset}", start(reset), start(reset).saturating_sub(answered) / 1000);

    // The first checklist, Day 0's: the fabric-primary's commit-state action published what it held,
    // before its checklist completed.
    let (first_root, first_change) = change_of(&spans, &first);
    let actions = |change: &[&Value]| -> std::collections::BTreeMap<String, Value> {
        change.iter().filter(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-state-action" && sp["attributes"]["command"] == "commit-state").map(|sp| (s(&sp["attributes"]["node"]), (*sp).clone())).collect()
    };
    let published = actions(&first_change);
    assert_eq!(published.keys().cloned().collect::<std::collections::BTreeSet<_>>(), want, "Day 0's commit-state checklist is its one node's: {:?}", published.keys().collect::<Vec<_>>());
    let a = &published["mesh1.admin.1"];
    assert!(flag(&a["attributes"]["bound"]), "mesh1.admin.1's commit-state action publishes its scratchpad: {a}");
    assert!(num(&a["attributes"]["rows"]).unwrap_or(0) > 0, "mesh1.admin.1 published the rows it held: {a}");
    let fp_stores = s(&a["attributes"]["stores"]);
    assert!(!fp_stores.contains("connections"), "mesh1.admin.1 originated no connection and published no connections rows: {fp_stores}");
    // A Node record is written straight to the store (states.md :51, Q54): what a mesh's primary publishes
    // is its Mesh record with its name claim; its members' addresses are on their Node records.
    assert!(fp_stores.contains("meshes=") && fp_stores.contains("locks=") && !fp_stores.contains("/contacts"), "mesh1.admin.1, mesh1's primary, published its Mesh record and its name claim: {fp_stores}");
    for store in ["fabrics=", "fabric="] {
        assert!(fp_stores.split(',').any(|p| p.starts_with(store)), "the fabric-primary published {store}: {a}");
    }
    assert!(!fp_stores.split(',').any(|p| p.starts_with("fabric/seats")), "no per-seat row is published (Part C2: the per-seat rows under fabric/seats go): {a}");
    // The fabric-primary's own mesh checklist checks in locally: its checklist completes after it.
    let complete = first_change.iter().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-checklist-complete" && sp["attributes"]["command"] == "commit-state").expect("the first commit-state checklist completed");
    assert!(a["end_unix_nano"].as_u64().unwrap() <= start(complete), "the fabric-primary published before the commit-state checklist completed: action {a}, complete {complete}");
    // The growth Build was written once, by the fabric-primary, after the Day-0 Build (R-BI3: the Build is
    // its own entity, in acceptance order).
    let accepted = named(&spans, "rdm.node_admin.build.create.via-accepted");
    let written = accepted.iter().find(|sp| sp["attributes"]["build_id"] == second.as_str()).unwrap_or_else(|| panic!("the growth Build's acceptance is spanned: {:?}", accepted.iter().map(|sp| &sp["attributes"]).collect::<Vec<_>>()));
    assert_eq!((s(&written["attributes"]["node"]), s(&written["attributes"]["previous_build_id"])), ("mesh1.admin.1".to_string(), first.clone()), "the fabric-primary wrote the growth Build after the Day-0 Build: {written}");
    assert!(accepted.iter().all(|sp| sp["attributes"]["node"] == "mesh1.admin.1"), "only the fabric-primary writes a Build: {:?}", accepted.iter().map(|sp| (&sp["attributes"]["node"], &sp["attributes"]["via"])).collect::<Vec<_>>());

    // The nodes born after the first state-commit opened their stores on the store root; the growth
    // checklist's actions had nothing held, and no node but the fabric-primary names a fabric/ store.
    let opened = named(&spans, "rdm.mesh.node.resolve.via-connections-hydrated").into_iter().find(|sp| sp["attributes"]["node"] == "mesh2.rpc.1").expect("mesh2.rpc.1 opened its connections store");
    assert_eq!(opened["attributes"]["writes_to"], "store-root", "a node born after state-commit writes to the store root: {opened}");
    let (_, second_change) = change_of(&spans, &second);
    let republished = actions(&second_change);
    assert!(!republished.is_empty(), "the growth checklist ran commit-state actions");
    for (name, a) in &republished {
        assert_eq!(num(&a["attributes"]["rows"]), Some(0), "{name} held nothing to publish in the growth checklist: {a}");
        if name != "mesh1.admin.1" {
            // fabric/ has one writer, the fabric-primary (R14 item 2): no other node holds a row there.
            let stores = s(&a["attributes"]["stores"]);
            assert!(!stores.contains("fabric"), "{name} does not hold the fabric seat, yet its commit-state publication names a fabric/ store: {stores}");
        }
    }
    estate.record_trace_url(&s(&first_root["trace_id"]));
}

/// Bootstrap with the application `app`, wait until the fabric-primary is blocked on the
/// application, hold the fabric there, and return the evidence: nothing past state-sync happened.
async fn held_in_state_sync(test: &str, app: &str) -> (Vec<Value>, Value) {
    let mut estate = estate_with(test, app).await;
    until_fabric(&estate, ScopeStatus::StateSync, None, Duration::from_secs(60)).await;
    // The fabric stays where it is: it is read through the window, not slept on, so a state that moved shows.
    let held_until = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < held_until {
        assert_eq!(estate.fabric_view().await.status, ScopeStatus::StateSync, "the fabric left state-sync");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let fabric = serde_json::to_value(estate.fabric_view().await).expect("a view serializes");
    estate.stop().await;
    (estate.spans(), fabric)
}

fn assert_no_traffic_opened(spans: &[Value]) {
    for state_key in ["via-checklist-opened", "via-state-command", "via-member-check-in", "via-checklist-complete", "via-traffic-opened-hook"] {
        let name = format!("rdm.node_admin.fabric.update.{state_key}");
        assert!(named(spans, &name).is_empty(), "{name} ran although the fabric never left state-sync");
    }
    assert!(named(spans, "rdm.node_admin.fabric.update.via-state-entered").iter().all(|e| e["attributes"]["to"] == "state-sync"), "no state past state-sync was entered");
    assert!(named(spans, "rdm.node_admin.status.update.via-state-op").is_empty(), "no commit-state or open-traffic op was served");
}

/// CONTRACT: an application that takes `sync_state` and never answers keeps the fabric in
/// state-sync. The fabric-primary names what it waits for (the application's state-synced for the
/// accepted Build and attempt), calls the application once, runs no checklist and opens no traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_application_that_never_answers_keeps_the_fabric_in_state_sync_with_a_named_blocker() {
    let (spans, fabric) = held_in_state_sync("an_application_that_never_answers_keeps_the_fabric_in_state_sync_with_a_named_blocker", "never").await;
    assert_eq!(fabric["status"], "state-sync", "{fabric}");
    let blocked: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-blocked").into_iter().filter(|b| b["attributes"]["state"] == "state-sync").collect();
    assert!(
        blocked.iter().any(|b| b["attributes"]["blockers"] == "application(no-state-synced)" && s(&b["attributes"]["what"]).starts_with("sync_state awaits the application's state-synced for build ")),
        "the blocker is named: {:?}",
        blocked.iter().map(|b| &b["attributes"]).collect::<Vec<_>>()
    );
    assert_eq!(named(&spans, "rdm.node_admin.fabric.update.via-sync-state-called").len(), 1, "sync_state is called once and never again on a timer");
    assert_eq!(named(&spans, "rdm.testkit.app.update.via-sync-state-taken").len(), 1, "the application took the checklist once");
    assert_no_traffic_opened(&spans);
}

async fn refused_state_synced(test: &str, app: &str, field: &str) {
    let (spans, fabric) = held_in_state_sync(test, app).await;
    assert_eq!(fabric["status"], "state-sync", "{fabric}");
    let refusals: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.reject.via-state-synced");
    assert!(
        refusals.iter().any(|r| s(&r["attributes"]["reason"]).starts_with(&format!("{field}: sent "))),
        "the state-synced is refused by the field that is wrong ({field}): {:?}",
        refusals.iter().map(|r| &r["attributes"]["reason"]).collect::<Vec<_>>()
    );
    assert!(named(&spans, "rdm.node_admin.fabric.update.via-state-synced").is_empty(), "a refused state-synced starts no state-commit");
    assert_no_traffic_opened(&spans);
}

/// CONTRACT: a state-synced for another Build is not the checklist's completion: it is refused naming
/// `build_id` with what was sent and what was answered, and the fabric stays in state-sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_state_synced_for_a_foreign_build_is_refused_by_name() {
    refused_state_synced("a_state_synced_for_a_foreign_build_is_refused_by_name", "foreign-build", "build_id").await;
}

/// CONTRACT: a state-synced for an older attempt is refused naming `attempt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_state_synced_for_an_old_attempt_is_refused_by_name() {
    refused_state_synced("a_state_synced_for_an_old_attempt_is_refused_by_name", "old-attempt", "attempt").await;
}

/// The refusal a call ended with; a call the fabric-primary accepted is the defect, named.
fn refused<T>(what: &str, call: Result<T, CallEnd>) -> CallEnd {
    match call {
        Err(end) => end,
        Ok(_) => panic!("{what}: the fabric-primary accepted a workflow the initial fabric refuses"),
    }
}

/// CONTRACT (R-PB1, R-PC1; states.md "The initial fabric before ready-for-traffic"; node-rpc-envelope.md
/// "Build refusals": "the existing `Rejected { reason, detail }` reply with reason `fabric-not-ready` ... A
/// refused Build claims no attempt and sends no call"): while the initial fabric, the Day-0 Build's (its
/// one mesh, mesh1, and that mesh's one node-admin, the fabric-primary and mesh1's primary: fabric-create.md
/// step 3), is held in state-sync, `build.create` of every Build that would move a seat (a restart, stop or
/// node.connections.delete of the fabric-primary, a recipe deleting its connections) and of every caller
/// workflow that writes a row (a node delete, mesh.delete) answers `Rejected { reason: "fabric-not-ready",
/// detail }`, the detail naming the workflow and `state-sync`; and `node.tags.create` at mesh1's primary
/// answers `RejectedFabricNotReady` naming the workflow and `state-sync`. The Build log of the Day-0
/// Build, read from the admin's own journal over `FetchBuildFacts`, holds the attempt it held before: none
/// is claimed. Once the application answers and the fabric is ready-for-traffic the same kind of workflow
/// is accepted: a Build grows mesh1 by two rpc nodes, and the restart of the one holding no seat has one
/// attempt, the only one the current Build gains. One
/// `rdm.node_admin.build.reject.via-fabric-not-ready` span per refused Build and one
/// `rdm.node_admin.node.reject.via-fabric-not-ready` for the refused node.tags.create say the same, and no
/// node.primary.update span is ever written (lifecycles/node-primary-update.md "Precondition": "Before
/// the initial fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready`"; failure
/// contract: "Refused before the row ...: refused by name, nothing changed"): no command
/// (`primary.update.via-command`), no update at a primary (`primary.update.via-update-primary`), no issued
/// writes finished for it (`via-issued-writes-finished`), no install (`seat.update.via-primary-updated`).
/// What must NOT happen: a primary moved while the Fabric row, the Mesh rows and the Node rows live only
/// in their writers' scratchpads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_move_and_a_mesh_delete_during_the_initial_state_sync_are_refused_fabric_not_ready_and_claim_no_attempt() {
    let test = "a_mesh_primary_move_and_a_mesh_delete_during_the_initial_state_sync_are_refused_fabric_not_ready_and_claim_no_attempt";
    let mut estate = estate_with(test, "hold").await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    // The initial Build is the Day-0 Build (fabric-create.md): no other Build is accepted before its own
    // ready-for-traffic, which the application holds. It is Complete only once its ready-for-traffic state
    // is set (Q154): it is followed from its first attempt to its Complete once the hold is released.
    let first = estate.accepted_build_id().await;
    let first_id = rdm_client::BuildId::parse(&first).unwrap_or_else(|e| panic!("fabric.get names the Day-0 Build {first}, not a Build id: {e}"));
    let mut first_stream = estate.carrier().await.resubmit(&first_id, 1).await.unwrap_or_else(|e| panic!("the fabric-primary does not stream the Day-0 Build {first}: {e}"));
    let path = |n: &str| -> PathName { n.parse().unwrap() };
    let held_in_state_sync = |f: &FabricView| f.status == ScopeStatus::StateSync && f.build_id.as_deref() == Some(first.as_str()) && f.meshes.len() == 1 && f.meshes.iter().all(|m| m.status == ScopeStatus::ReadyForTraffic);
    estate.fabrics_obj().await.fact("state-sync under the Day-0 Build, its mesh ready", Duration::from_secs(60), held_in_state_sync).await.unwrap_or_else(|end| panic!("{end}"));
    door.wait_held("state-sync").await;
    // The Build's run ended, handed back for the fabric's change: no attempt of it is in flight, and none is claimed
    // while the fabric changes (Q154).
    until_blocked_on_the_fabric_change(&estate, &mut first_stream, Duration::from_secs(60)).await;

    let carrier = estate.carrier().await;
    let log_before = build_log::build_value(&carrier, &first).await;
    let seats = |views: &[NodeView]| -> Vec<(String, bool, bool)> { views.iter().map(|n| (n.name.to_string(), n.is_primary, n.is_fabric_primary)).filter(|(_, p, f)| *p || *f).collect() };
    let seats_before = seats(&estate.node_views().await);

    // Seat-moving Builds and caller workflows, each refused by name at build.create.
    let nodes = estate.nodes_obj().await;
    let refusals: Vec<(&str, CallEnd)> = vec![
        ("node.restart mesh1.admin.1", refused("restart", nodes.restart(&path("mesh1.admin.1")).await)),
        ("node.stop mesh1.admin.1", refused("stop", nodes.stop(&path("mesh1.admin.1")).await)),
        ("node.connections.delete mesh1.admin.1 (a seat holder)", refused("node.connections.delete", carrier.create(Change::DeleteConnections { node: path("mesh1.admin.1") }).await)),
        ("recipe (node.connections.delete:mesh1.admin.1)", refused("recipe", carrier.create(Change::Recipe { steps: vec![rdm_core::build::RecipeStep::DeleteConnections { node: path("mesh1.admin.1") }] }).await)),
        ("node.delete mesh1.admin.1", refused("delete", nodes.delete(&path("mesh1.admin.1")).await)),
        ("mesh.delete mesh1", refused("mesh.delete", carrier.create(Change::RemoveMesh { mesh: "mesh1".into() }).await)),
    ];
    for (workflow, end) in &refusals {
        match end {
            CallEnd::Rejected { reason, detail } => {
                assert_eq!(reason, "fabric-not-ready", "{workflow}: {end}");
                assert!(detail.contains(workflow) && detail.contains("state-sync"), "the detail names the workflow ({workflow}) and the status: {detail}");
            }
            other => panic!("{workflow}: expected Rejected fabric-not-ready, got {other:?}"),
        }
    }

    // node.tags.create at mesh1's primary: the Status op's own refusal, nothing written.
    let primary1 = estate.node_view("mesh1.admin.1").await;
    let birth1 = ExactBirth { target: rdm_transport::NodeTarget::ExactNode(primary1.node_id.clone()), node_id: primary1.node_id.clone(), incarnation: primary1.incarnation_id.clone().expect("mesh1.admin.1 has a known birth") };
    let created = nodes.tags_create(&birth1, &primary1.node_id, &rdm_transport::RouteChoice::Direct, &["rabbit".to_string()].into()).await.unwrap_or_else(|end| panic!("node.tags.create at mesh1.admin.1: {end}"));
    assert_eq!(created, StatusReply::RejectedFabricNotReady { workflow: "node.tags.create mesh1.admin.1".into(), status: "state-sync".into() });
    assert!(estate.node_view("mesh1.admin.1").await.tags.is_empty(), "a refused node.tags.create writes no tags");

    // Nothing moved: no attempt was claimed, the fabric is still held, every seat is where it was.
    let log_after = build_log::build_value(&carrier, &first).await;
    assert_eq!((&log_after["attempt"], &log_after["state"]), (&log_before["attempt"], &log_before["state"]), "no attempt is claimed: {log_before} -> {log_after}");
    assert_eq!(estate.accepted_build_id().await, first, "Fabric.build_id does not move");
    assert_eq!(estate.fabric_view().await.status, ScopeStatus::StateSync, "the fabric is still held in state-sync");
    let held_seats = seats(&estate.node_views().await);
    assert_eq!(held_seats, seats_before, "every seat is where it was");
    assert_eq!(held_seats, vec![("mesh1.admin.1".to_string(), true, true)], "{held_seats:?}");

    // The application answers: the fabric changes, and the same kind of workflow is accepted.
    door.release("state-sync").await;
    followed(&estate, first_stream, &format!("the Day-0 Build {first} after its state-sync was released"), Duration::from_secs(90)).await;
    estate.fabrics_obj().await.fact("ready-for-traffic under the Day-0 Build", Duration::from_secs(90), |f| f.status == ScopeStatus::ReadyForTraffic && f.build_id.as_deref() == Some(first.as_str())).await.unwrap_or_else(|end| panic!("{end}"));
    // mesh1 grows by two rpc nodes: the restart admitted is of the one holding no seat, so no seat moves (a
    // seat holder's restart runs node.primary.update first, and a node type of one node in its mesh has no
    // successor, Q7). The fault cut was spent by its release: the application answers this Build at once.
    let shape = [("mesh1", 1, 2)];
    let grown = estate.reconcile_shape("fabric1", &shape).await.build_id.to_string();
    estate.settled_views(&shape).await;
    ready_under(&estate, &grown).await;
    // The attempt that completed the growth after its change is the last before the restart.
    let log_done = build_log::build_value(&carrier, &grown).await;
    let member = estate.node_views().await.into_iter().filter(|n| n.name.to_string().starts_with("mesh1.rpc.") && !n.is_primary).map(|n| n.name.to_string()).min().expect("an rpc node of mesh1 holding no seat");
    let mut admitted = estate.nodes_obj().await.restart(&path(&member)).await.unwrap_or_else(|end| panic!("once the fabric is ready-for-traffic a restart is accepted: {end}"));
    let accepted = Estate::complete(&mut admitted).await;
    assert_eq!((accepted.build_id.as_str(), accepted.attempt), (grown.as_str(), log_done["attempt"].as_u64().unwrap() as u32 + 1), "the admitted restart is the next attempt of the current Build");
    let log_end = build_log::build_value(&carrier, &grown).await;
    assert_eq!(log_end["attempt"].as_u64(), Some(u64::from(accepted.attempt)), "the admitted restart is the only attempt the Build gained: {log_end}");
    estate.stop().await;

    let spans = estate.spans();
    let mut rejects = named(&spans, "rdm.node_admin.build.reject.via-fabric-not-ready");
    rejects.sort_by_key(|sp| start(sp));
    assert_eq!(rejects.len(), refusals.len(), "one refusal span per refused Build: {:?}", rejects.iter().map(|r| &r["attributes"]["workflow"]).collect::<Vec<_>>());
    for (r, (workflow, _)) in rejects.iter().zip(&refusals) {
        assert_eq!((s(&r["attributes"]["workflow"]).as_str(), s(&r["attributes"]["status"]).as_str()), (*workflow, "state-sync"), "{:?}", r["attributes"]);
    }
    let node_rejects = named(&spans, "rdm.node_admin.node.reject.via-fabric-not-ready");
    assert_eq!(node_rejects.len(), 1, "one refusal span for the refused node.tags.create: {node_rejects:?}");
    assert_eq!((s(&node_rejects[0]["attributes"]["workflow"]).as_str(), s(&node_rejects[0]["attributes"]["status"]).as_str()), ("node.tags.create mesh1.admin.1", "state-sync"));
    for update_span in [
        "rdm.node_admin.primary.update.via-command",
        "rdm.node_admin.primary.update.via-update-primary",
        "rdm.node_admin.primary.update.via-issued-writes-finished",
        "rdm.node_admin.seat.update.via-primary-updated",
    ] {
        assert!(named(&spans, update_span).is_empty(), "no primary moved: {update_span} was written");
    }
    estate.record_trace_url(&s(&rejects[0]["trace_id"]));
    for r in &rejects {
        eprintln!("trace {}: rdm.node_admin.build.reject.via-fabric-not-ready workflow={} status=state-sync", s(&r["trace_id"]), s(&r["attributes"]["workflow"]));
    }
    eprintln!("trace {}: rdm.node_admin.node.reject.via-fabric-not-ready workflow=node.tags.create mesh1.admin.1 status=state-sync", s(&node_rejects[0]["trace_id"]));
}


/// CONTRACT (states.md: "The fabric enters state-sync when every planned mesh is ready-for-traffic."; gossip.md
/// §3.2: timers only for heartbeat and anti-entropy): the Day-0 Build's change enters state-sync when its
/// attempt is claimed; a Build adding a member to a ready mesh puts the fabric back to pending, and the
/// fabric-primary enters state-sync when the last planned member's ready-for-traffic is applied, and
/// announces that member's node-type primary then, never on its next gossip interval. The estate runs at
/// the default gossip interval (2 s), so a wait on that interval is a gap of up to 2 s; the bound is a
/// quarter of it. Must NOT happen: the fabric, or the node-type primary's announcement, waiting for the
/// gossip interval after the claim or the member's ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_enters_state_sync_and_announces_the_node_type_primary_at_the_last_planned_members_ready() {
    let test = "the_fabric_enters_state_sync_and_announces_the_node_type_primary_at_the_last_planned_members_ready";
    let gossip = Duration::from_millis(2000);
    let sha = candidate_sha();
    let env = [("RDM_TEST_APP", "answer:0"), ("RDM_GOSSIP_INTERVAL_MS", "2000"), ("RDM_BACKBONE_INTERVAL_MS", "2000"), ("RDM_STALENESS_MS", "30000")];
    let mut estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &env).await.expect("the faulted-admin binding set is accepted");
    // The Day-0 Build's change ends first: the admin alone, whose attempt's claim is the last thing the fabric waits on.
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, None, Duration::from_secs(60)).await;
    let build = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])] }).await.build_id.to_string();
    until_fabric(&estate, ScopeStatus::ReadyForTraffic, Some(build.as_str()), Duration::from_secs(60)).await;
    estate.stop().await;
    let spans = estate.spans();
    let ready = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|d| d["attributes"]["op"] == "declare-node-state" && d["attributes"]["state"] == "ReadyForTraffic" && d["attributes"]["sender"] == "mesh1.rpc.1" && d["attributes"]["outcome"] == "applied")
        .min_by_key(|d| start(d))
        .expect("mesh1.rpc.1's ready-for-traffic was applied by its mesh-primary");
    let (_, change) = change_of(&spans, &build);
    let entered = change.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-state-entered" && sp["attributes"]["to"] == "state-sync").unwrap_or_else(|| panic!("the fabric entered state-sync under {build}"));
    let announced = named(&spans, "rdm.mesh.seat.update.via-announcement")
        .into_iter()
        .filter(|a| a["attributes"]["seat"] == "rpc_node-primary" && a["attributes"]["via"] == "own" && s(&a["attributes"]["node"]).starts_with("mesh1.admin."))
        .min_by_key(|a| start(a))
        .expect("the mesh-primary announced mesh1's rpc_node primary");
    // The Day-0 Build: the first Build whose change entered state-sync, and its attempt's claim.
    let day0 = named(&spans, "rdm.node_admin.fabric.update.via-change").into_iter().filter(|r| r["attributes"]["build_id"] != build.as_str()).min_by_key(|r| start(r)).map(|r| s(&r["attributes"]["build_id"])).expect("the Day-0 Build's change");
    let (_, day0_change) = change_of(&spans, &day0);
    let day0_entered = day0_change.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-state-entered" && sp["attributes"]["to"] == "state-sync").expect("the Day-0 change entered state-sync");
    let day0_claimed = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|c| c["attributes"]["build_id"] == day0.as_str() && c["attributes"]["outcome"] == "won").min_by_key(|c| start(c)).expect("the Day-0 Build's attempt was claimed");
    let ms = |from: &Value, to: &Value| (start(to) as i128 - start(from) as i128) / 1_000_000;
    let (to_state_sync, to_announced, claim_to_state_sync) = (ms(ready, entered), ms(ready, announced), ms(day0_claimed, day0_entered));
    estate.artifact("gaps.json", &json!({ "ready_to_state_sync_ms": to_state_sync, "ready_to_announced_ms": to_announced, "day0_claim_to_state_sync_ms": claim_to_state_sync, "gossip_interval_ms": gossip.as_millis() as u64 }));
    eprintln!("ready -> state-sync {to_state_sync} ms, ready -> rpc_node-primary announced {to_announced} ms, Day-0 claim -> state-sync {claim_to_state_sync} ms (gossip interval {} ms)", gossip.as_millis());
    let bound = (gossip / 4).as_millis() as i128;
    assert!(claim_to_state_sync < bound, "the Day-0 fabric entered state-sync {claim_to_state_sync} ms after its Build's attempt was claimed, not within {bound} ms: it waited for the gossip interval");
    assert!(to_state_sync < bound, "the fabric entered state-sync {to_state_sync} ms after mesh1.rpc.1's ready was applied, not within {bound} ms: it waited for the gossip interval");
    assert!(to_announced < bound, "the rpc_node primary was announced {to_announced} ms after mesh1.rpc.1's ready was applied, not within {bound} ms: it waited for the gossip interval");
    estate.record_trace_url(&s(&entered["trace_id"]));
}
