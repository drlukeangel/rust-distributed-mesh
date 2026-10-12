//! i143.e12.s6 process E2E: the fabric states and their rounds (states.md; fabric-state-sync.md,
//! fabric-state-commit.md, fabric-open-traffic.md).
//!
//! A real estate whose node-admin is the testkit's, carrying the testkit application
//! (`RDM_TEST_APP`). The fabric-primary alone moves the fabric: pending, state-sync (every mesh
//! ready-for-traffic), state-commit (the application answered state-synced), ready-for-traffic (the
//! commit-state and open-traffic rounds completed). Evidence is read from the spans every process
//! wrote.

use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, binding_set_with_roles, candidate_sha, Door};
use serde_json::{json, Value};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-states".into(),
        subfeature: "rounds".into(),
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
    Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_TEST_APP", app)])
        .await
        .expect("the faulted-admin binding set is accepted")
}

/// [`estate_with`] whose launch ids also bind the product role binaries (broker, gateway, compute).
async fn estate_with_roles(test: &str, app: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set_with_roles(&sha), &sha, &["broker", "gateway", "compute"], &[("RDM_TEST_APP", app)])
        .await
        .expect("the faulted-admin binding set is accepted")
}

async fn fabric_state(estate: &Estate) -> String {
    s(&estate.get("/api/fabric").await.1["status"])
}

/// The spans of the climb whose root carries `build_id`.
fn climb_of<'a>(spans: &'a [Value], build_id: &str) -> (&'a Value, Vec<&'a Value>) {
    let root = named(spans, "rdm.node_admin.fabric.update.via-state-climb")
        .into_iter()
        .find(|r| r["attributes"]["build_id"] == build_id)
        .unwrap_or_else(|| panic!("no climb of {build_id}"));
    let mut mine: Vec<&Value> = spans.iter().filter(|sp| sp["trace_id"] == root["trace_id"] && start(sp) >= start(root)).collect();
    mine.sort_by_key(|sp| start(sp));
    (root, mine)
}

/// CONTRACT: a fabric whose Build adds a second mesh goes back to pending, and reaches
/// ready-for-traffic only through state-sync, state-commit and open-traffic, in that order, all
/// authored by the fabric-primary. The application is called once at state-sync and its
/// state-synced starts state-commit; every mesh primary and every member takes each round's command
/// and checks in; the application's one traffic-opened notice follows ready-for-traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_is_ready_only_through_state_sync_state_commit_and_open_traffic() {
    let test = "a_two_mesh_fabric_is_ready_only_through_state_sync_state_commit_and_open_traffic";
    let estate = estate_with(test, "answer:300").await;
    let members = ["rpc.1", "rpc.2", "rpc.3"];
    two_mesh_climb(estate, json!({"node_admin": 2, "rpc_node": 3}), &members).await;
}

/// CONTRACT: a two-mesh fabric whose meshes each hold the product role nodes (a broker, a gateway
/// and a compute) climbs to ready-for-traffic exactly as the rpc-node fabric does: every role node
/// takes both round commands, acts through the one member-round path, publishes its check-in hook
/// on its mesh channel and completes at its mesh primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_of_role_nodes_is_ready_only_through_the_commit_state_and_open_traffic_rounds() {
    let test = "a_two_mesh_fabric_of_role_nodes_is_ready_only_through_the_commit_state_and_open_traffic_rounds";
    let estate = estate_with_roles(test, "answer:300").await;
    let members = ["broker.1", "gateway.1", "compute.1"];
    two_mesh_climb(estate, json!({"node_admin": 2, "broker": 1, "gateway": 1, "compute": 1}), &members).await;
}

/// CONTRACT: the mesh primary of a role-node mesh dies and its mesh's other admin takes the seat.
/// Each role node of that mesh (broker, gateway, compute) sees the seat move to another birth, owes
/// its state again (`via-authority-moved`, naming the new holder) and declares it to the new primary
/// (`via-declare-own`, definitive), the same re-owe the rpc node makes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn role_nodes_owe_their_state_again_to_the_mesh_primary_that_took_the_seat() {
    let test = "role_nodes_owe_their_state_again_to_the_mesh_primary_that_took_the_seat";
    let mut estate = estate_with_roles(test, "answer:0").await;
    let mesh = |m: &str| json!({"name": m, "node_admin": 2, "broker": 1, "gateway": 1, "compute": 1});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let build = s(&a["build_id"]);
    estate.await_build(&build, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain(["broker.1", "gateway.1", "compute.1"].iter().map(move |n| format!("{m}.{n}")))).collect();
    let nodes = estate.settled(&want, Duration::from_secs(60)).await;
    wait_for("the fabric is ready-for-traffic", Duration::from_secs(60), || async { (fabric_state(&estate).await == "ready-for-traffic").then_some(()) }).await;
    // The mesh that does not hold the fabric seat loses its primary.
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost_mesh = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let old = nodes.iter().find(|n| n["mesh"] == lost_mesh && n["is_primary"] == true && n["kind"] == "node_admin").map(|n| s(&n["name"])).expect("a primary admin");
    let heir = if old.ends_with(".1") { format!("{lost_mesh}.admin.2") } else { format!("{lost_mesh}.admin.1") };
    let heir_id = s(&nodes.iter().find(|n| n["name"] == heir.as_str()).unwrap()["node_id"]);
    if old == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        let pid = estate.pid_of(&old).await;
        assert!(std::process::Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success(), "kill -9 {pid}");
    }
    let roles: Vec<String> = ["broker.1", "gateway.1", "compute.1"].iter().map(|n| format!("{lost_mesh}.{n}")).collect();
    wait_for("every role node of the mesh declared its state to the new primary", Duration::from_secs(90), || {
        let spans = estate.spans();
        let done = roles.iter().all(|r| {
            named(&spans, "rdm.node_rpc.status.update.via-declare-own").iter().any(|d| s(&d["attributes"]["node"]) == *r && s(&d["attributes"]["to"]) == heir && flag(&d["attributes"]["definitive"]))
        });
        async move { done.then_some(()) }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    for r in &roles {
        let moved: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-authority-moved").into_iter().filter(|m| s(&m["attributes"]["node"]) == *r).collect();
        assert!(moved.iter().any(|m| s(&m["attributes"]["to"]) == heir_id), "{r} saw the seat move to {heir} ({heir_id}): {:?}", moved.iter().map(|m| &m["attributes"]).collect::<Vec<_>>());
    }
    estate.record_trace_url(&s(&named(&spans, "rdm.node_rpc.status.update.via-authority-moved").first().expect("a re-owe")["trace_id"]));
}

/// The two-mesh climb over meshes of `shape` (their node-admins and members), whose members are named
/// `members` under each mesh.
async fn two_mesh_climb(mut estate: Estate, shape: Value, members: &[&str]) {
    let mesh = |m: &str| {
        let mut v = shape.clone();
        v["name"] = json!(m);
        v
    };
    let (_, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1")]})).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let two_meshes = s(&a["build_id"]);
    estate.await_build(&two_meshes, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain(members.iter().map(move |n| format!("{m}.{n}"))))
        .collect();
    estate.settled(&want, Duration::from_secs(60)).await;
    wait_for("the fabric is ready-for-traffic under the two-mesh Build", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let climbed = named(&spans, "rdm.node_admin.fabric.update.via-state-entered").into_iter().any(|e| e["attributes"]["to"] == "ready-for-traffic" && climb_root_build(&spans, e).as_deref() == Some(two_meshes.as_str()));
        (climbed && fabric_state(&estate).await == "ready-for-traffic").then_some(())
    })
    .await;
    let nodes = estate.nodes().await;
    estate.artifact("nodes.json", &json!(nodes));
    estate.stop().await;
    let spans = estate.spans();
    let primaries: std::collections::BTreeSet<String> = nodes.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).collect();
    assert_eq!(primaries.len(), 2, "one primary per mesh: {primaries:?}");
    let (root, climb) = climb_of(&spans, &two_meshes);
    let fp = s(&root["attributes"]["node"]);
    let at = |name: &str, f: &dyn Fn(&Value) -> bool| -> &Value { climb.iter().copied().find(|sp| sp["name"] == name && f(sp)).unwrap_or_else(|| panic!("no {name} in the climb")) };
    let entered = |to: &str| at("rdm.node_admin.fabric.update.via-state-entered", &|sp| sp["attributes"]["to"] == to);
    let opened = |round: &str| at("rdm.node_admin.fabric.update.via-round-opened", &|sp| sp["attributes"]["round"] == round);
    let complete = |round: &str| at("rdm.node_admin.fabric.update.via-round-complete", &|sp| sp["attributes"]["round"] == round);
    let hook = at("rdm.node_admin.fabric.update.via-sync-state-hook", &|_| true);
    let app = at("rdm.testkit.app.update.via-sync-state", &|_| true);
    let synced = at("rdm.node_admin.fabric.update.via-state-synced", &|_| true);
    let notice = at("rdm.node_admin.fabric.update.via-traffic-opened-hook", &|_| true);
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
        ("ready-for-traffic", start(entered("ready-for-traffic"))),
        ("traffic_opened notice", start(notice)),
    ];
    for w in order.windows(2) {
        assert!(w[0].1 <= w[1].1, "{} came after {}: {order:?}", w[0].0, w[1].0);
    }
    assert!(app["end_unix_nano"].as_u64().unwrap() - start(app) >= 250_000_000, "the application's simulated work ran inside the hook: {app}");
    assert_eq!(hook["attributes"]["outcome"], "state-synced", "{hook}");
    assert_eq!(named(&spans, "rdm.testkit.app.update.via-sync-state").iter().filter(|a| a["attributes"]["build_id"] == two_meshes.as_str()).count(), 1, "the application is called once per round of state-sync");

    // Each round: the fabric-primary commands the other mesh's primary, every member of both meshes takes the command and checks in.
    for (round, completion) in [("commit-state", "state-committed"), ("open-traffic", "traffic-opened")] {
        let o = opened(round);
        let c = complete(round);
        let in_round = |sp: &&Value| start(sp) >= start(o) && start(sp) <= start(c) + 2_000_000_000;
        let commands: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-round-command").into_iter().filter(in_round).filter(|sp| sp["attributes"]["round"] == round).collect();
        let to: std::collections::BTreeSet<String> = commands.iter().filter(|c| s(&c["attributes"]["node"]) == fp).map(|c| s(&c["attributes"]["to"])).collect();
        assert!(to.iter().any(|t| t.starts_with("mesh2.admin.")), "{round}: the fabric-primary {fp} commanded mesh2's primary: {to:?}");
        let members: std::collections::BTreeSet<String> = named(&spans, "rdm.node_admin.fabric.update.via-member-round").into_iter().filter(in_round).filter(|sp| sp["attributes"]["round"] == round).map(|sp| s(&sp["attributes"]["node"])).collect();
        let expected: std::collections::BTreeSet<String> = want.difference(&primaries).cloned().collect();
        assert!(expected.is_subset(&members), "{round}: every member that is not a primary took the command and acted; missing {:?}", expected.difference(&members).collect::<Vec<_>>());
        let ups: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-round-op").into_iter().filter(in_round).filter(|sp| sp["attributes"]["op"] == completion).collect();
        assert!(ups.iter().all(|u| u["attributes"]["outcome"] == "applied"), "{round}: every {completion} was applied: {ups:#?}");
        assert!(ups.iter().any(|u| s(&u["attributes"]["node"]) == fp && s(&u["attributes"]["sender"]).starts_with("mesh2.admin.")), "{round}: the fabric-primary took mesh2's {completion}");
    }

    // The gossip hooks (gossip.md, "Fabric round gossip hooks"): each command is announced by its
    // commanding authority on its mesh channel and the backbone before the call; each check-in by the
    // subject (a node-admin on both channels, an ordinary node on its mesh channel, which its
    // mesh-admin carries onto the backbone); and a peer mesh's primary forwards them.
    let id_of: std::collections::BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    let name_of: std::collections::BTreeMap<&String, &String> = id_of.iter().map(|(n, i)| (i, n)).collect();
    let primary_of = |mesh: &str| primaries.iter().find(|p| p.starts_with(&format!("{mesh}."))).cloned().expect("a primary per mesh");
    let hooks: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-round-hook").into_iter().filter(|h| start(h) >= start(root) && flag(&h["attributes"]["bound"])).collect();
    let frames: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-round-frame").into_iter().filter(|f| start(f) >= start(root)).collect();
    let mut firsts = Vec::new();
    for (round, command_frame, completion, completion_frame) in [("commit-state", "state-committing", "state-committed", "state-committed"), ("open-traffic", "traffic-opening", "traffic-opened", "traffic-opened")] {
        let sent = |hook: &str, subject: &str| -> Vec<&Value> { hooks.iter().copied().filter(|h| h["attributes"]["hook"] == hook && s(&h["attributes"]["subject_id"]) == id_of[subject]).collect() };
        let mut cmd_at = Vec::new();
        let mut done_at = Vec::new();
        for subject in want.iter().filter(|n| **n != fp) {
            let mesh = subject.split('.').next().unwrap();
            let authority = if *subject == primary_of(mesh) { fp.clone() } else { primary_of(mesh) };
            let cmd = sent(round, subject);
            assert!(!cmd.is_empty(), "{round}: the commanding authority announced the command to {subject}");
            assert!(cmd.iter().all(|h| s(&h["attributes"]["node"]) == authority && h["attributes"]["channels"] == "mesh,backbone"), "{round}: {subject} was announced by {authority} on its mesh channel and the backbone: {cmd:?}");
            let done = sent(completion, subject);
            assert!(!done.is_empty(), "{round}: {subject} announced its {completion} check-in");
            let channels = if subject.contains(".admin.") { "mesh,backbone" } else { "mesh" };
            assert!(done.iter().all(|h| s(&h["attributes"]["node"]) == *subject && h["attributes"]["channels"] == channels), "{round}: {subject} announced {completion} itself on {channels}: {done:?}");
            assert!(start(cmd[0]) <= start(done[0]), "{round}: the command to {subject} was announced before its check-in");
            cmd_at.push(start(cmd[0]));
            done_at.push(start(done[0]));
            // An ordinary node's check-in reaches the backbone through its mesh-admin; a node-admin's is its own.
            if !subject.contains(".admin.") {
                let carried = frames.iter().any(|f| f["attributes"]["kind"] == completion_frame && f["attributes"]["via"] == "backbone" && s(&f["attributes"]["subject_id"]) == id_of[subject.as_str()] && s(&f["attributes"]["forwarded_by"]) == authority);
                assert!(carried, "{round}: {authority} carried {subject}'s {completion_frame} onto the backbone");
            }
            let heard_by_members = frames.iter().any(|f| f["attributes"]["kind"] == command_frame && f["attributes"]["via"] == "mesh-channel" && s(&f["attributes"]["subject_id"]) == id_of[subject.as_str()] && s(&f["attributes"]["forwarded_by"]).is_empty());
            assert!(heard_by_members, "{round}: the {command_frame} for {subject} was heard on a mesh channel as its author sent it");
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
        firsts.push((round, *cmd_at.iter().min().unwrap(), *done_at.iter().max().unwrap()));
    }
    assert!(firsts[0].1 <= firsts[0].2 && firsts[0].2 <= firsts[1].1 && firsts[1].1 <= firsts[1].2, "StateCommitting, StateCommitted, TrafficOpening, TrafficOpened came in that order: {firsts:?}");

    // A command is sent again only on an eligible event (R-S2), named on the span that sends it: a birth
    // is commanded once as `first`, a send after it carries the event that made it due, and nothing is
    // sent again after the birth answered.
    let mut sends: std::collections::BTreeMap<(String, String, String), Vec<&Value>> = std::collections::BTreeMap::new();
    for c in named(&spans, "rdm.node_admin.fabric.update.via-round-command").into_iter().filter(|c| start(c) >= start(root)) {
        sends.entry((s(&c["attributes"]["node"]), s(&c["attributes"]["round"]), s(&c["attributes"]["to"]))).or_default().push(c);
    }
    assert!(!sends.is_empty());
    let mut resends = 0;
    for ((from, round, to), calls) in &mut sends {
        calls.sort_by_key(|c| start(c));
        assert_eq!(calls.iter().filter(|c| c["attributes"]["event"] == "first").count(), 1, "{from} commanded {to} once as first in {round}: {calls:?}");
        assert_eq!(calls[0]["attributes"]["event"], "first", "{from}'s first send to {to} in {round} is the first");
        for (i, c) in calls.iter().enumerate().skip(1) {
            resends += 1;
            let event = s(&c["attributes"]["event"]);
            assert!(["addressed", "destination-changed", "reachable-again", "state-changed"].contains(&event.as_str()), "{from} sent {round} to {to} again with no eligible event: {event}");
            let before = s(&calls[i - 1]["attributes"]["outcome"]);
            assert!(before != "applied" && before != "already-applied", "{from} sent {round} to {to} again after it answered {before}");
        }
    }
    eprintln!("[rounds] {} commands to {} (round, birth) pairs, {resends} sent again on an eligible event", sends.values().map(Vec::len).sum::<usize>(), sends.len());

    // One trace: every call of the climb has its receiver's serve span under it, every mesh ran both
    // rounds in it, and mesh2's primary reported each round up in a new caller span of the same trace.
    let count = |name: &str| climb.iter().filter(|sp| sp["name"] == name).count();
    assert_eq!(count("rdm.node_rpc.request.update.via-call"), count("rdm.node_rpc.request.serve.via-direct"), "every call of the climb was served under it");
    assert_eq!(count("rdm.node_admin.fabric.update.via-mesh-round"), 4, "two meshes ran two rounds each inside the climb's trace");
    assert_eq!(count("rdm.node_admin.status.update.via-round-completion"), 2, "the other mesh's primary reported each round up in the climb's trace");

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
    let climb_states: Vec<&str> = vec!["state-sync", "state-commit", "ready-for-traffic"];
    assert!(seen.windows(3).any(|w| w == climb_states), "FabricStatus went state-sync, state-commit, ready-for-traffic: {seen:?}");
    estate.record_trace_url(&s(&root["trace_id"]));
}

/// Every row under the fabric's store root, as a path relative to it, sorted. A row is a file: the
/// directories the stores open are not rows.
fn rows_on(root: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, root, out);
            } else {
                out.push(p.strip_prefix(root).unwrap().display().to_string());
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Wait until the fabric is ready-for-traffic under the climb of `build`.
async fn ready_under(estate: &Estate, build: &str) {
    wait_for(&format!("the fabric is ready-for-traffic under {build}"), Duration::from_secs(90), || async {
        let spans = estate.spans();
        let climbed = named(&spans, "rdm.node_admin.fabric.update.via-state-entered").into_iter().any(|e| e["attributes"]["to"] == "ready-for-traffic" && climb_root_build(&spans, e).as_deref() == Some(build));
        (climbed && fabric_state(estate).await == "ready-for-traffic").then_some(())
    })
    .await;
}

/// CONTRACT (states.md, Rounds: "Before state-commit, RDM holds its rows in the scratchpad;
/// state-commit publishes the required held rows to the configured stores"; fabric-state-commit.md
/// step 2; node-identity-and-state.md §2.7, The write; node-admin-lifecycle.md §7; R14 item 9): while
/// the fabric is held in state-sync, every node of a two-mesh fabric up and ready (the fabric-primary,
/// the other mesh's primary and a member in each mesh), the fabric's store root holds no row: each
/// node holds what it wrote in its scratchpad. The state-commit round publishes it: each node's
/// commit-state action writes what it holds before its check-in (the fabric-primary the Fabric row,
/// each mesh primary its Mesh row and its members' Node rows, every node its connections rows). After
/// that first state-commit the store exists: a node born later writes straight to it, and the next
/// round's commit-state action of every node has nothing held to publish.
///
/// `fabric/` has one writer, the fabric-primary (R14 item 2: "Fabric-primary: `fabric/`. ... Keys
/// never overlap across writers"): only its publication names a `fabric/` store (the Fabric
/// identity, its pointers, the seat rows); the other mesh's primary holds and publishes none of
/// them, and holds nothing back. When the fabric-primary accepts the next Build after state-commit,
/// the pointer moves on the store root and the other mesh's primary reads the new `Fabric.build_id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_store_root_holds_no_row_until_state_commit_publishes_what_each_node_holds() {
    let test = "the_store_root_holds_no_row_until_state_commit_publishes_what_each_node_holds";
    let mut estate = estate_with(test, "hold").await;
    let door = Door::open(&estate.root, "mesh1.admin.1", "").await;
    let root = estate.fabric_store_root();
    let shape = |m: &str, rpc: u32| json!({"name": m, "node_admin": 1, "rpc_node": rpc});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [shape("mesh1", 1), shape("mesh2", 1)]})).await;
    assert_eq!(status, 202, "{a}");
    let first = s(&a["build_id"]);
    estate.await_build(&first, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.rpc.1", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|n| n.to_string()).collect();
    let nodes = estate.settled(&want, Duration::from_secs(60)).await;
    // The application took the two-mesh Build's state-sync round and the door holds its answer: the
    // fabric cannot reach state-commit.
    wait_for("the fabric is in state-sync under the two-mesh Build, its answer held at the door", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let taken = named(&spans, "rdm.testkit.app.update.via-sync-state-taken").into_iter().any(|t| t["attributes"]["build_id"] == first.as_str());
        let held = door.cut("state-sync").await["held"] == true;
        (taken && held && fabric_state(&estate).await == "state-sync").then_some(())
    })
    .await;
    let before = rows_on(&root);
    estate.artifact("rows-before-state-commit.json", &json!(before));
    assert!(before.is_empty(), "no row reaches the store root {} before state-commit; found {}: {before:#?}", root.display(), before.len());
    // R-FN1: the fabric's claim of its name sits beside the per-fabric roots, at
    // `<store_dir>/fabrics/fabric1`, and is held in the fabric-primary's scratchpad like every row.
    let name_claim = root.parent().expect("the store root sits under the store_dir").join(rafka_node_admin_core::store_root::FABRIC_NAMES).join("fabric1.json");
    assert!(!name_claim.exists(), "the fabric's name claim reaches {} only at state-commit", name_claim.display());
    door.release("state-sync").await;
    ready_under(&estate, &first).await;
    let after = rows_on(&root);
    estate.artifact("rows-after-state-commit.json", &json!(after));
    let claimed: Value = serde_json::from_slice(&std::fs::read(&name_claim).unwrap_or_else(|e| panic!("the state-commit published the fabric's name claim at {}: {e}", name_claim.display()))).unwrap();
    assert_eq!(claimed["record"]["fabric_id"].as_str(), root.file_name().and_then(|n| n.to_str()), "the name claim names this fabric: {claimed}");
    let id_of: std::collections::BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    let holds = |prefix: &str| after.iter().any(|r| r.starts_with(prefix));
    assert!(holds("fabric/identity.json") && holds("fabric/pointers/"), "the fabric-primary published the Fabric row: {after:#?}");
    for mesh in ["mesh1", "mesh2"] {
        assert!(holds(&format!("meshes/{mesh}.json")), "{mesh}'s primary published its Mesh row: {after:#?}");
    }
    for name in &want {
        let mesh = name.split('.').next().unwrap();
        assert!(holds(&format!("nodes/{mesh}/contacts/{}.json", id_of[name])), "{mesh}'s primary published {name}'s Node row: {after:#?}");
        assert!(holds(&format!("connections/{name}/")), "{name} published its connections rows: {after:#?}");
    }

    // A node born after the first state-commit writes straight to the store, and the second round's
    // commit-state actions publish nothing: nothing is held any more.
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [shape("mesh1", 1), shape("mesh2", 2)]})).await;
    assert_eq!(status, 202, "{b}");
    let second = s(&b["build_id"]);
    estate.await_build(&second, Duration::from_secs(120)).await;
    let mut want_later = want.clone();
    want_later.insert("mesh2.rpc.2".into());
    estate.settled(&want_later, Duration::from_secs(60)).await;
    ready_under(&estate, &second).await;
    let later = rows_on(&root);
    estate.artifact("rows-after-second-state-commit.json", &json!(later));
    // The fabric-primary accepted the second Build after the first state-commit: its pointer row is
    // on the store root, and the other mesh's primary reads the new Fabric.build_id.
    let newest_pointer = later.iter().filter(|r| r.starts_with("fabric/pointers/")).max().cloned().unwrap_or_default();
    assert!(newest_pointer.contains(&second), "the fabric-primary's newest pointer row on the store root names the second Build {second}: {later:#?}");
    let mesh2_admin = nodes.iter().find(|n| n["name"] == "mesh2.admin.1").and_then(|n| n["admin_api_base"].as_str()).expect("mesh2.admin.1 advertises its control API").to_string();
    let (status, other_fabric) = estate.http_get(&mesh2_admin, "/api/fabric").await;
    estate.artifact("mesh2-admin-fabric-after-second-build.json", &other_fabric);
    assert_eq!(status, 200, "{other_fabric}");
    assert_eq!(s(&other_fabric["build_id"]), second, "the other mesh's primary reads the Fabric.build_id the fabric-primary moved on the store root: {other_fabric}");
    estate.stop().await;
    let spans = estate.spans();

    // The first round: each node's commit-state action published what it held, before its check-in.
    let (first_root, first_climb) = climb_of(&spans, &first);
    let actions = |climb: &[&Value]| -> std::collections::BTreeMap<String, Value> {
        climb.iter().filter(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-round-action" && sp["attributes"]["round"] == "commit-state").map(|sp| (s(&sp["attributes"]["node"]), (*sp).clone())).collect()
    };
    let published = actions(&first_climb);
    for name in &want {
        let a = published.get(name).unwrap_or_else(|| panic!("{name}'s commit-state action is in the first climb: {:?}", published.keys().collect::<Vec<_>>()));
        assert!(flag(&a["attributes"]["bound"]), "{name}'s commit-state action publishes its scratchpad: {a}");
        assert!(num(&a["attributes"]["rows"]).unwrap_or(0) > 0, "{name} published the rows it held: {a}");
        let stores = s(&a["attributes"]["stores"]);
        assert!(stores.contains("connections"), "{name} published its connections rows: {stores}");
        if name.contains(".admin.") {
            assert!(stores.contains("meshes") && stores.contains("nodes"), "{name}, its mesh's primary, published its Mesh row and its members' Node rows: {stores}");
        }
        if name != "mesh1.admin.1" {
            // fabric/ has one writer, the fabric-primary (R14 item 2): no other node holds a row there.
            assert!(!stores.contains("fabric"), "{name} does not hold the fabric seat, yet its commit-state publication names a fabric/ store: {stores}");
        }
        let check_in = named(&spans, "rdm.node_admin.fabric.update.via-round-hook")
            .into_iter()
            .find(|h| h["attributes"]["hook"] == "state-committed" && s(&h["attributes"]["subject_id"]) == id_of[name] && flag(&h["attributes"]["bound"]) && start(h) >= start(first_root));
        let end = a["end_unix_nano"].as_u64().unwrap();
        if name == "mesh1.admin.1" {
            // The fabric-primary's own mesh round checks in locally: its round completes after it.
            let complete = first_climb.iter().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-round-complete" && sp["attributes"]["round"] == "commit-state").expect("the first commit-state round completed");
            assert!(end <= start(complete), "the fabric-primary published before the commit-state round completed: action {a}, complete {complete}");
        } else {
            let h = check_in.unwrap_or_else(|| panic!("{name} announced its state-committed check-in in the first round"));
            assert!(end <= start(h), "{name} published before it checked in: action {a}, check-in {h}");
        }
    }
    let fp_stores = s(&published["mesh1.admin.1"]["attributes"]["stores"]);
    for store in ["fabrics=", "fabric=", "fabric/pointers=", "fabric/seats="] {
        assert!(fp_stores.split(',').any(|p| p.starts_with(store)), "the fabric-primary published {store}: {}", published["mesh1.admin.1"]);
    }
    // The other mesh's primary took the second Build's pointer as its own runtime state, not as a row.
    let learned = named(&spans, "rdm.node_admin.fabric.update.via-pointer-learned").into_iter().find(|sp| sp["attributes"]["node"] == "mesh2.admin.1" && sp["attributes"]["build_id"] == second.as_str());
    assert!(learned.is_some(), "mesh2.admin.1 learned the second Build's pointer");
    assert!(
        named(&spans, "rdm.node_admin.fabric.update.via-build-accepted").into_iter().all(|sp| sp["attributes"]["node"] == "mesh1.admin.1"),
        "only the fabric-primary moved a pointer through fabric.storage: {:?}",
        named(&spans, "rdm.node_admin.fabric.update.via-build-accepted").iter().map(|sp| (&sp["attributes"]["node"], &sp["attributes"]["via"])).collect::<Vec<_>>()
    );

    // The node born after the first state-commit opened its store on the store root; the second
    // round's actions had nothing held.
    assert!(later.iter().any(|r| r.starts_with("connections/mesh2.rpc.2/")), "mesh2.rpc.2 wrote its connections rows straight to the store: {later:#?}");
    let opened = named(&spans, "rdm.mesh.node.resolve.via-connections-hydrated").into_iter().find(|sp| sp["attributes"]["node"] == "mesh2.rpc.2").expect("mesh2.rpc.2 opened its connections store");
    assert_eq!(opened["attributes"]["writes_to"], "store-root", "a node born after state-commit writes to the store root: {opened}");
    let (_, second_climb) = climb_of(&spans, &second);
    let republished = actions(&second_climb);
    assert!(!republished.is_empty(), "the second round ran commit-state actions");
    for (name, a) in &republished {
        assert_eq!(num(&a["attributes"]["rows"]), Some(0), "{name} held nothing to publish in the second round: {a}");
    }
    estate.record_trace_url(&s(&first_root["trace_id"]));
}

fn climb_root_build(spans: &[Value], entered: &Value) -> Option<String> {
    named(spans, "rdm.node_admin.fabric.update.via-state-climb").into_iter().find(|r| r["trace_id"] == entered["trace_id"]).map(|r| s(&r["attributes"]["build_id"]))
}

/// Bootstrap with the application `app`, wait until the fabric-primary is blocked on the
/// application, hold the fabric there, and return the evidence: nothing past state-sync happened.
async fn held_in_state_sync(test: &str, app: &str) -> (Vec<Value>, Value) {
    let mut estate = estate_with(test, app).await;
    wait_for("the fabric reaches state-sync", Duration::from_secs(60), || async { (fabric_state(&estate).await == "state-sync").then_some(()) }).await;
    // The fabric stays where it is: it is polled, not slept on, so a state that moved shows.
    let held_until = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < held_until {
        assert_eq!(fabric_state(&estate).await, "state-sync", "the fabric left state-sync");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let fabric = estate.get("/api/fabric").await.1;
    estate.stop().await;
    (estate.spans(), fabric)
}

fn assert_no_traffic_opened(spans: &[Value]) {
    for round in ["via-round-opened", "via-round-command", "via-member-round", "via-round-complete", "via-traffic-opened-hook"] {
        let name = format!("rdm.node_admin.fabric.update.{round}");
        assert!(named(spans, &name).is_empty(), "{name} ran although the fabric never left state-sync");
    }
    assert!(named(spans, "rdm.node_admin.fabric.update.via-state-entered").iter().all(|e| e["attributes"]["to"] == "state-sync"), "no state past state-sync was entered");
    assert!(named(spans, "rdm.node_admin.status.update.via-round-op").is_empty(), "no commit-state or open-traffic op was served");
}

/// CONTRACT: an application that takes `sync_state` and never answers keeps the fabric in
/// state-sync. The fabric-primary names what it waits for (the application's state-synced for the
/// accepted Build and attempt), calls the application once, runs no round and opens no traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_application_that_never_answers_keeps_the_fabric_in_state_sync_with_a_named_blocker() {
    let (spans, fabric) = held_in_state_sync("an_application_that_never_answers_keeps_the_fabric_in_state_sync_with_a_named_blocker", "never").await;
    assert_eq!(fabric["status"], "state-sync", "{fabric}");
    let blocked: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-blocked").into_iter().filter(|b| b["attributes"]["state"] == "state-sync").collect();
    assert!(
        blocked.iter().any(|b| b["attributes"]["blockers"] == "application(no-state-synced)" && s(&b["attributes"]["what"]).starts_with("sync_state awaits the application's state-synced for build bld-")),
        "the blocker is named: {:?}",
        blocked.iter().map(|b| &b["attributes"]).collect::<Vec<_>>()
    );
    assert_eq!(named(&spans, "rdm.node_admin.fabric.update.via-sync-state-called").len(), 1, "sync_state is called once and never again on a timer");
    assert_eq!(named(&spans, "rdm.testkit.app.update.via-sync-state-taken").len(), 1, "the application took the round once");
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

/// CONTRACT: a state-synced for another Build is not the round's completion: it is refused naming
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

/// CONTRACT (R-PB1, R-PC1; states.md "The initial fabric before ready-for-traffic"; node-rpc-envelope.md
/// "Build refusals"): while the initial fabric is held in state-sync, every Build that would move a seat
/// (a restart, stop or drain of the fabric-primary, a replace of mesh2's primary) and every caller
/// workflow that writes a row (a node stop, a node delete, mesh.delete) is refused 422
/// `fabric-not-ready` at the fabric-primary, the detail naming the workflow and `state-sync`. None
/// claims an attempt: the initial Build stays at its one attempt, `Fabric.build_id` does not move, and
/// the fabric-primary keeps both seats. Once the application answers and the fabric is
/// ready-for-traffic the same kind of workflow is admitted as usual, and is the only attempt ever
/// opened. One `rdm.node_admin.build.reject.via-fabric-not-ready` span per refusal says the same, and
/// no seat span (`seats-committed`, `seats-installed`) is ever written.
/// What must NOT happen: a seat or a row moved while the Fabric row, the Mesh rows and the Node rows live
/// only in their holders' scratchpads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seat_move_and_a_mesh_delete_during_the_initial_state_sync_are_refused_fabric_not_ready_and_claim_no_attempt() {
    let test = "a_seat_move_and_a_mesh_delete_during_the_initial_state_sync_are_refused_fabric_not_ready_and_claim_no_attempt";
    let mut estate = estate_with(test, "hold").await;
    let door = Door::open(&estate.root, "mesh1.admin.1", "").await;
    let shape = |m: &str| json!({"name": m, "node_admin": 1, "rpc_node": 1});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [shape("mesh1"), shape("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let first = s(&a["build_id"]);
    estate.await_build(&first, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.rpc.1", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|n| n.to_string()).collect();
    estate.settled(&want, Duration::from_secs(60)).await;
    wait_for("the fabric is in state-sync under the Build, its answer held at the door", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let taken = named(&spans, "rdm.testkit.app.update.via-sync-state-taken").into_iter().any(|t| t["attributes"]["build_id"] == first.as_str());
        let held = door.cut("state-sync").await["held"] == true;
        (taken && held && fabric_state(&estate).await == "state-sync").then_some(())
    })
    .await;
    let (_, build_before) = estate.get(&format!("/api/builds?id={first}")).await;
    let (_, fabric_before) = estate.get("/api/fabric").await;

    // Seat-moving Builds and caller workflows, each refused by name.
    let refusals = [
        ("POST", "/api/nodes/mesh1.admin.1/restart", "node.restart mesh1.admin.1"),
        ("POST", "/api/nodes/mesh1.admin.1/stop", "node.stop mesh1.admin.1"),
        ("POST", "/api/nodes/mesh1.admin.1/drain", "node.drain mesh1.admin.1 (a seat holder)"),
        ("POST", "/api/nodes/mesh2.admin.1/replace", "node.replace mesh2.admin.1"),
        ("POST", "/api/nodes/mesh2.rpc.1/stop", "node.stop mesh2.rpc.1"),
        ("DELETE", "/api/nodes/mesh2.rpc.1", "node.delete mesh2.rpc.1"),
        ("DELETE", "/api/meshes/mesh2", "mesh.delete mesh2"),
    ];
    for (method, path, workflow) in refusals {
        let (status, v) = if method == "POST" { estate.post(path, &json!({})).await } else { estate.delete(path).await };
        assert_eq!((status, v["error"].as_str()), (422, Some("fabric-not-ready")), "{method} {path}: {v}");
        assert!(v["detail"].as_str().is_some_and(|d| d.contains(workflow) && d.contains("state-sync")), "the detail names the workflow ({workflow}) and the status: {v}");
    }
    let (_, build_after) = estate.get(&format!("/api/builds?id={first}")).await;
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!((build_after["attempt"].clone(), build_after["state"].clone()), (build_before["attempt"].clone(), build_before["state"].clone()), "no attempt is claimed: {build_before} -> {build_after}");
    assert_eq!(fabric_after["build_id"], fabric_before["build_id"], "Fabric.build_id does not move");
    assert_eq!(fabric_state(&estate).await, "state-sync", "the fabric is still held in state-sync");
    let seats = |nodes: &[Value]| -> Vec<(String, bool, bool)> { nodes.iter().map(|n| (s(&n["name"]), n["is_primary"] == true, n["is_fabric_primary"] == true)).filter(|(_, p, f)| *p || *f).collect() };
    let held = seats(&estate.nodes().await);
    assert!(held.contains(&("mesh1.admin.1".to_string(), true, true)) && held.contains(&("mesh2.admin.1".to_string(), true, false)), "every seat is where it was: {held:?}");

    // The application answers: the fabric climbs, and the same kind of workflow is admitted.
    door.release("state-sync").await;
    ready_under(&estate, &first).await;
    let (status, admitted) = estate.post("/api/nodes/mesh2.rpc.1/restart", &json!({})).await;
    assert_eq!(status, 202, "once the fabric is ready-for-traffic a restart is admitted: {admitted}");
    estate.await_attempt(&first, Estate::attempt_of(&admitted), Duration::from_secs(120)).await;
    estate.stop().await;

    let spans = estate.spans();
    let mut rejects = named(&spans, "rdm.node_admin.build.reject.via-fabric-not-ready");
    rejects.sort_by_key(|sp| start(sp));
    assert_eq!(rejects.len(), refusals.len(), "one refusal span per refused workflow: {:?}", rejects.iter().map(|r| &r["attributes"]["workflow"]).collect::<Vec<_>>());
    for (r, (_, _, workflow)) in rejects.iter().zip(refusals) {
        assert_eq!((s(&r["attributes"]["workflow"]).as_str(), s(&r["attributes"]["status"]).as_str()), (workflow, "state-sync"), "{:?}", r["attributes"]);
    }
    // A request span names the Build and attempt it opened only once it opened one: the refused requests'
    // spans carry neither.
    let opened: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-rest").into_iter().filter(|o| o["attributes"].get("build_id").is_some()).collect();
    assert_eq!(opened.len(), 1, "the one admitted restart is the only attempt ever opened: {:?}", opened.iter().map(|o| &o["attributes"]).collect::<Vec<_>>());
    let accepted: Vec<&Value> = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().filter(|o| o["attributes"].get("build_id").is_some()).collect();
    assert_eq!(accepted.len(), 1, "the initial Build is the only Build ever accepted: {:?}", accepted.iter().map(|o| &o["attributes"]).collect::<Vec<_>>());
    assert!(start(opened[0]) > rejects.iter().map(|r| start(r)).max().unwrap(), "the attempt opened after every refusal");
    for seat_span in ["rdm.node_admin.seat.update.via-seats-committed", "rdm.node_admin.seat.update.via-seats-installed"] {
        assert!(named(&spans, seat_span).is_empty(), "no seat moved: {seat_span} was written");
    }
    estate.record_trace_url(&s(&rejects[0]["trace_id"]));
    for r in &rejects {
        eprintln!("trace {}: rdm.node_admin.build.reject.via-fabric-not-ready workflow={} status=state-sync", s(&r["trace_id"]), s(&r["attributes"]["workflow"]));
    }
    eprintln!("trace {}: rdm.node_admin.build.update.via-rest (the restart admitted once ready-for-traffic)", s(&opened[0]["trace_id"]));
}
