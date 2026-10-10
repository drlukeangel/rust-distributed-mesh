//! i143.e12.s6 process E2E: the fabric states and their rounds (states.md; fabric-state-sync.md,
//! fabric-state-commit.md, fabric-open-traffic.md).
//!
//! A real estate whose node-admin is the testkit's, carrying the testkit application
//! (`RDM_TEST_APP`). The fabric-primary alone moves the fabric: pending, state-sync (every mesh
//! ready-for-traffic), state-commit (the application answered state-synced), ready-for-traffic (the
//! commit-state and open-traffic rounds completed). Evidence is read from the spans every process
//! wrote.

use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha};
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

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

async fn estate_with(test: &str, app: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_TEST_APP", app)])
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
    let mut estate = estate_with("a_two_mesh_fabric_is_ready_only_through_state_sync_state_commit_and_open_traffic", "answer:300").await;
    let mesh = |m: &str| json!({"name": m, "node_admin": 2, "rpc_node": 3});
    let (_, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1")]})).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let two_meshes = s(&a["build_id"]);
    estate.await_build(&two_meshes, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=3).map(move |i| format!("{m}.rpc.{i}"))))
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
