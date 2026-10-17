//! i144 e6.2 process E2E: the fabric seat moves without a person when the fabric-primary's mesh is lost
//! and no surviving admin can inspect it (fabric-node-lifecycle-elections.md §4.3, §5; Q24, Q28, Q35,
//! Q48).
//!
//! The lost mesh is made uninspectable with the testkit's provider-domain cut on every surviving
//! admin: each one's provider reports a foreign control domain and refuses the lost runtimes' locators,
//! so its look at the lost mesh's admins answers `unproven` and no exit proof is recorded. A lost mesh
//! the survivors can inspect moves the seat on exit proof by the ordinary rule
//! (`fabric_primary::losing_the_fabric_primary_mesh_moves_control`).

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "fabric-takeover".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A faulted-admin estate (every admin serves the fault door) of `shape`, born ready, with the hold at
/// two staleness floors.
async fn estate_of(test: &str, shape: &[(&str, u32, u32)]) -> Estate {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_HUNG_HOLD_FLOORS", "2")]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", shape).await;
    estate.settles_to(shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    estate
}

/// Make mesh1 uninspectable to every node-admin outside it (the provider-domain cut), then send
/// `signal` to every process of mesh1, the Day-0 admin included. The pids signalled.
async fn lose_mesh1(estate: &Estate, nodes: &[Value], signal: &str) -> Vec<u64> {
    for n in nodes.iter().filter(|n| n["mesh"] != "mesh1" && n["kind"] == "node_admin") {
        Door::at(estate, &s(&n["name"])).await.arm("foreign-domain", json!({"kind": "provider-domain"})).await;
    }
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    pids.extend(estate.bootstrap_pid().map(u64::from));
    for pid in &pids {
        assert!(std::process::Command::new("kill").args([signal, &pid.to_string()]).status().unwrap().success(), "kill {signal} {pid}");
    }
    pids
}

/// The fabric seat's revision in the last seat move that moved it.
fn fabric_revision(estate: &Estate) -> u64 {
    seat_moves(estate).iter().rev().find_map(|m| m["seats"].as_array()?.iter().find(|x| x["seat"] == "FabricPrimary").map(|x| x["revision"].as_u64().unwrap_or(0))).expect("the fabric seat has a row")
}

/// The fabric's seat moves on its store root, `seat-moves/<T>`, in order: each row's record.
fn seat_moves(estate: &Estate) -> Vec<Value> {
    let dir = estate.fabric_store_root().join("seat-moves");
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&dir).into_iter().flatten().flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "json")).collect();
    files.sort();
    files.iter().filter_map(|f| serde_json::from_slice::<Value>(&std::fs::read(f).ok()?).ok()).map(|v| v["record"].clone()).collect()
}

/// CONTRACT (elections §5: "For a fabric-primary that is unreached by the tickle ladder and whose exact
/// runtime no surviving admin can inspect (its mesh or provider domain gone), the hold
/// (`RDM_HUNG_HOLD_FLOORS`) runs, and when it expires the seat moves by §4.3's backbone rule. The
/// takeover commits only through the store's authority fence ... Every mesh-primary that notices tells
/// the winner (`node.fabric-primary.update`)"; Q48: every seat change is one row of kind
/// `election | takeover | handover`): mesh1 holds the fabric seat. mesh2's admins cannot inspect mesh1's
/// runtimes (provider-domain cut), and every process of mesh1 is killed. mesh2's primary finds mesh1's
/// admins unreached and `unproven`; nothing records their exit. The seat does not move before the hold.
/// Past it, mesh2's primary, the backbone rule's winner, commits ONE `takeover` seat move: the fabric
/// seat from mesh1's holder at its revision `N` to itself at `N+1`, and seals the fabric lane at `N`.
/// node.get names it the fabric-primary. Must NOT happen: the seat moving before the hold, an exit
/// proof recorded for mesh1's admins, two takeover rows, or the seat moving with no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_taken_over_past_the_hold_through_one_takeover_row() {
    let test = "a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_taken_over_past_the_hold_through_one_takeover_row";
    // The faulted-admin binding set: every admin serves the testkit's fault door.
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_HUNG_HOLD_FLOORS", "2")]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 2, 1)], Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;

    let nodes = estate.node_values().await;
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&holder["mesh"]), "mesh1", "the Day-0 admin's mesh holds the fabric seat: {nodes:#?}");
    let lost_revision = seat_moves(&estate).iter().rev().find_map(|m| m["seats"].as_array()?.iter().find(|x| x["seat"] == "FabricPrimary").map(|x| x["revision"].as_u64().unwrap_or(0))).expect("the fabric seat has a row");
    let winner = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["is_primary"] == true && n["kind"] == "node_admin").cloned().expect("mesh2's primary");
    let winner_name = s(&winner["name"]);

    // mesh2's admins cannot inspect mesh1's runtimes from here on.
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        Door::at(&estate, &s(&n["name"])).await.arm("foreign-domain", json!({"kind": "provider-domain"})).await;
    }
    // Lose mesh1: every one of its processes.
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    for pid in pids {
        estate.kill_pid(pid as u32);
    }
    estate.kill_bootstrap();
    let killed = std::time::Instant::now();

    let hold = rdm_transport::membership::staleness_floor() * 2;
    let winner_path = Estate::path(&winner_name);
    estate.enter(&winner_path).await;
    let took = wait_for(&format!("{winner_name} holds the fabric seat"), hold + rdm_transport::membership::staleness_floor() * 6 + Duration::from_secs(60), || async {
        let n = estate.node_value(&winner_name).await;
        (n["is_fabric_primary"] == true).then_some(n)
    })
    .await;
    let elapsed = killed.elapsed();
    assert!(elapsed >= hold, "the seat moved before the hold: {elapsed:?} < {hold:?}");
    assert_eq!(took["is_fabric_primary"], json!(true));

    let takeovers: Vec<Value> = seat_moves(&estate).into_iter().filter(|m| m["cause"] == "takeover").collect();
    assert_eq!(takeovers.len(), 1, "one takeover row: {takeovers:#?}");
    let seat = &takeovers[0]["seats"][0];
    assert_eq!((seat["seat"].clone(), seat["from_revision"].as_u64(), seat["revision"].as_u64(), s(&seat["holder"]["node_id"]), s(&seat["from"]["node_id"]), s(&takeovers[0]["by"]["node_id"])), (json!("FabricPrimary"), Some(lost_revision), Some(lost_revision + 1), s(&winner["node_id"]), s(&holder["node_id"]), s(&winner["node_id"])), "the fabric seat from mesh1's holder at {lost_revision} to {winner_name}: {takeovers:#?}");
    let seal = estate.fabric_store_root().join("fabric/builds").join(format!("rev-{lost_revision:020}"));
    let sealed = std::fs::read_dir(&seal).into_iter().flatten().flatten().any(|e| std::fs::read_to_string(e.path()).is_ok_and(|t| t.contains("\"format\":\"lane-seal/1\"")));
    assert!(sealed, "the fabric lane is sealed at revision {lost_revision}: {}", seal.display());

    // Every process's evidence is flushed as it exits: the spans are read after the stop.
    estate.stop().await;
    let spans = estate.spans();
    let committed = named(&spans, "rdm.node_admin.seat.update.via-takeover").into_iter().filter(|sp| sp["attributes"]["outcome"] == "committed").cloned().collect::<Vec<_>>();
    assert_eq!(committed.len(), 1, "one committed takeover: {committed:#?}");
    let looks = named(&spans, "rdm.node_admin.seat.update.via-investigation").into_iter().filter(|sp| s(&sp["attributes"]["suspect"]).starts_with("mesh1.admin.")).cloned().collect::<Vec<_>>();
    assert!(!looks.is_empty() && looks.iter().all(|sp| sp["attributes"]["finding"] != "exit-proven"), "mesh1's admins are looked at and never proven exited: {looks:#?}");
    estate.record_trace_url(s(&committed[0]["trace_id"]).as_str());
    eprintln!("takeover: {winner_name} took the fabric seat at revision {} after {elapsed:?} (hold {hold:?}), trace {}", lost_revision + 1, s(&committed[0]["trace_id"]));
}

/// CONTRACT (elections §5: "Every mesh-primary that notices tells the winner
/// (`node.fabric-primary.update`); repeated calls for the same takeover and committed successor answer
/// `AlreadyApplied`"): mesh1 holds the fabric seat and is lost, uninspectable, while mesh2 and mesh3
/// both live. Both mesh-primaries hold mesh1 past the hold; the backbone rule's winner (the lower
/// NodeId of the two) writes ONE `takeover` row, and the other's proposal to it is answered from that
/// takeover: `PrimaryUpdated` when it came first, `AlreadyApplied` after. Must NOT happen: two takeover
/// rows, or the seat taken by the admin the rule does not seat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_mesh_primaries_noticing_the_lost_fabric_primary_together_commit_one_takeover() {
    let test = "two_mesh_primaries_noticing_the_lost_fabric_primary_together_commit_one_takeover";
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1), ("mesh3", 1, 1)];
    let mut estate = estate_of(test, &shape).await;
    let nodes = estate.node_values().await;
    let primaries: Vec<Value> = nodes.iter().filter(|n| n["mesh"] != "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().collect();
    assert_eq!(primaries.len(), 2, "mesh2's and mesh3's primaries: {nodes:#?}");
    let winner = primaries.iter().min_by_key(|n| s(&n["node_id"])).cloned().unwrap();
    let other = primaries.iter().max_by_key(|n| s(&n["node_id"])).cloned().unwrap();
    lose_mesh1(&estate, &nodes, "-KILL").await;
    estate.enter(&Estate::path(&s(&winner["name"]))).await;
    let floor = rdm_transport::membership::staleness_floor();
    wait_for(&format!("{} holds the fabric seat", s(&winner["name"])), floor * 8 + Duration::from_secs(60), || async {
        let n = estate.node_value(&s(&winner["name"])).await;
        (n["is_fabric_primary"] == true).then_some(())
    })
    .await;
    // The other mesh-primary holds the new record before the estate stops.
    wait_for(&format!("{} holds the new record", s(&other["name"])), floor * 4 + Duration::from_secs(30), || async {
        let f = estate.fabric_view_at(&Estate::path(&s(&other["name"]))).await?;
        (f.fabric_primary.as_ref().map(|p| p.to_string()) == Some(s(&winner["name"]))).then_some(())
    })
    .await;
    let takeovers: Vec<Value> = seat_moves(&estate).into_iter().filter(|m| m["cause"] == "takeover").collect();
    assert_eq!(takeovers.len(), 1, "one takeover row: {takeovers:#?}");
    assert_eq!(s(&takeovers[0]["by"]["node_id"]), s(&winner["node_id"]), "written by the rule's winner");
    estate.stop().await;
    let spans = estate.spans();
    let committed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-takeover").into_iter().filter(|sp| sp["attributes"]["outcome"] == "committed").collect();
    assert_eq!(committed.len(), 1, "one committed takeover: {committed:#?}");
    let proposed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-takeover-proposed").into_iter().filter(|sp| sp["attributes"]["node"] == other["name"]).collect();
    assert!(!proposed.is_empty(), "{} told the winner: {proposed:#?}", s(&other["name"]));
    assert!(proposed.iter().all(|sp| ["primary-updated", "already-applied"].contains(&sp["attributes"]["outcome"].as_str().unwrap_or(""))), "every proposal is answered from the one takeover: {proposed:#?}");
    estate.record_trace_url(s(&committed[0]["trace_id"]).as_str());
}

/// CONTRACT (elections §5: "the store validates it against the current committed seat revision,
/// rejects every stale one, and serializes the takeover with writes and claims so no old-authority
/// operation commits afterwards"; Q28 A): mesh1, the fabric-primary's mesh, is suspended whole while
/// mesh2's admins cannot inspect it; past the hold mesh2's primary takes the seat and seals the fabric
/// lane at the old revision. mesh1 resumes: the old fabric-primary, asked at once to accept a Build,
/// is refused (stale against the seal, or by the record it hears), and nothing of the old revision is
/// written past the seal; it yields, and node.get names mesh2's primary. Must NOT happen: an
/// old-authority Build fact committed after the takeover, or two fabric-primaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_fabric_primary_resumed_after_the_takeover_commits_nothing_past_the_seal() {
    let test = "an_old_fabric_primary_resumed_after_the_takeover_commits_nothing_past_the_seal";
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let mut estate = estate_of(test, &shape).await;
    let nodes = estate.node_values().await;
    let revision = fabric_revision(&estate);
    let winner = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let paused = lose_mesh1(&estate, &nodes, "-STOP").await;
    estate.enter(&Estate::path(&s(&winner["name"]))).await;
    let floor = rdm_transport::membership::staleness_floor();
    wait_for(&format!("{} holds the fabric seat", s(&winner["name"])), floor * 8 + Duration::from_secs(60), || async {
        let n = estate.node_value(&s(&winner["name"])).await;
        (n["is_fabric_primary"] == true).then_some(())
    })
    .await;
    let lane = estate.fabric_store_root().join("fabric/builds").join(format!("rev-{revision:020}"));
    let keys = |dir: &std::path::Path| {
        let mut k: Vec<String> = std::fs::read_dir(dir).into_iter().flatten().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|f| f.ends_with(".json")).collect();
        k.sort();
        k
    };
    let sealed = keys(&lane);
    let last = sealed.last().cloned().expect("the old revision holds its facts and its seal");
    assert!(std::fs::read_to_string(lane.join(&last)).unwrap().contains("\"format\":\"lane-seal/1\""), "the seal is the old revision's last key: {last}");
    // mesh1 resumes; its old fabric-primary is asked to accept a Build at once.
    for pid in &paused {
        let _ = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status();
    }
    let old = Estate::path("mesh1.admin.1");
    estate.enter(&old).await;
    let refused = estate.nodes_obj().await.create(&rdm_client::NodeSpec { mesh: "mesh1".into(), kind: rdm_client::LaunchKind::RpcNode }).await;
    assert!(refused.is_err(), "the old fabric-primary accepts nothing after the takeover: {:?}", refused.as_ref().map(|s| s.accepted().build_id.to_string()));
    wait_for("mesh1.admin.1 yields the fabric seat", floor * 6 + Duration::from_secs(30), || async {
        let n = estate.node_value("mesh1.admin.1").await;
        (n["is_fabric_primary"] == false).then_some(())
    })
    .await;
    assert_eq!(keys(&lane), sealed, "nothing of the old revision is written past its seal");
    estate.stop().await;
    let spans = estate.spans();
    let committed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-takeover").into_iter().filter(|sp| sp["attributes"]["outcome"] == "committed").collect();
    assert_eq!(committed.len(), 1, "{committed:#?}");
    let stale: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-stale-seat").into_iter().collect();
    eprintln!("old authority: refused {:?}; stale-seat refusals {}", refused.err().map(|e| e.to_string()), stale.len());
    estate.record_trace_url(s(&committed[0]["trace_id"]).as_str());
}

/// CONTRACT (elections §5: "if the store cannot provide the fence the takeover stays held. A held
/// takeover is surfaced on `fabric.get` as `takeover { lost, expected_revision, winner, proposer,
/// since, reason }` until it commits"): mesh1 is lost, uninspectable, and the fabric's seat-move store
/// cannot be written. Past the hold the winner cannot commit: the takeover stays held, and the
/// winner's `fabric.get` names it (the lost birth, the revision, the winner, why). The store writable
/// again, the next round commits the one takeover row and `fabric.get` no longer names a held takeover.
/// Must NOT happen: the seat moving with no row, or a held takeover left on `fabric.get` once committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_the_store_cannot_commit_is_held_on_fabric_get_until_it_commits() {
    use std::os::unix::fs::PermissionsExt as _;
    let test = "a_takeover_the_store_cannot_commit_is_held_on_fabric_get_until_it_commits";
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let mut estate = estate_of(test, &shape).await;
    let nodes = estate.node_values().await;
    let revision = fabric_revision(&estate);
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().unwrap();
    let winner = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let moves = estate.fabric_store_root().join("seat-moves");
    std::fs::set_permissions(&moves, std::fs::Permissions::from_mode(0o555)).unwrap();
    lose_mesh1(&estate, &nodes, "-KILL").await;
    let winner_path = Estate::path(&s(&winner["name"]));
    estate.enter(&winner_path).await;
    let floor = rdm_transport::membership::staleness_floor();
    let held = wait_for(&format!("{}'s fabric.get names a held takeover", s(&winner["name"])), floor * 8 + Duration::from_secs(60), || async {
        let f = estate.fabric_view_at(&winner_path).await?;
        f.takeover.clone()
    })
    .await;
    std::fs::set_permissions(&moves, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!((held.lost.node_id.to_string(), held.expected_revision, held.winner.node_id.to_string()), (s(&holder["node_id"]), revision, s(&winner["node_id"])), "{held:?}");
    assert!(held.since > 0 && held.reason.contains("seat"), "{held:?}");
    assert_eq!(seat_moves(&estate).iter().filter(|m| m["cause"] == "takeover").count(), 0, "nothing committed while the store cannot be written");
    wait_for(&format!("{} holds the fabric seat and no held takeover", s(&winner["name"])), floor * 4 + Duration::from_secs(30), || async {
        let f = estate.fabric_view_at(&winner_path).await?;
        (f.takeover.is_none() && f.fabric_primary.as_ref().map(|p| p.to_string()) == Some(s(&winner["name"]))).then_some(())
    })
    .await;
    assert_eq!(seat_moves(&estate).iter().filter(|m| m["cause"] == "takeover").count(), 1, "the one takeover row, once the store writes");
    estate.stop().await;
    let spans = estate.spans();
    let held_spans: Vec<&Value> = named(&spans, "rdm.node_admin.seat.reject.via-takeover-held").into_iter().collect();
    assert!(!held_spans.is_empty(), "the held takeover is spanned");
    estate.record_trace_url(s(&held_spans[0]["trace_id"]).as_str());
}
