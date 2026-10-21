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
    if signal == "-KILL" {
        estate.fabric_ready();
    }
    for pid in &pids {
        assert!(std::process::Command::new("kill").args([signal, &pid.to_string()]).status().unwrap().success(), "kill {signal} {pid}");
    }
    pids
}

/// Make mesh1's runtimes uninspectable to every node-admin outside it while each can still launch and
/// inspect its own (the provider-unproven cut, by pid: their inspection answers `Unknown`), then SIGKILL
/// every process of mesh1, the Day-0 admin included. The pids killed.
async fn lose_mesh1_unproven(estate: &Estate, nodes: &[Value]) -> Vec<u64> {
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    pids.extend(estate.bootstrap_pid().map(u64::from));
    for n in nodes.iter().filter(|n| n["mesh"] != "mesh1" && n["kind"] == "node_admin") {
        Door::at(estate, &s(&n["name"])).await.arm("mesh1-unproven", json!({"kind": "provider-unproven", "pids": pids})).await;
    }
    estate.fabric_ready();
    for pid in &pids {
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().unwrap().success(), "kill -KILL {pid}");
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

/// CONTRACT (Q89; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 2,
/// "Outside its mesh": a mesh-primary runs mesh investigation's ladder on the fabric-primary's mesh; "No
/// answer from any of them by the decision step is the backbone rule, and the lowest ReadyForTraffic
/// mesh-primary writes the row"; elections §5: "The takeover commits only through the store's authority
/// fence ... Every mesh-primary that notices tells the winner (`node.fabric-primary.update`)"; Q48: every
/// seat change is one row of kind `election | takeover | handover`): mesh1 holds the fabric seat. mesh2's
/// admins cannot inspect mesh1's runtimes (provider-domain cut), and every process of mesh1 is killed.
/// mesh2's primary probes mesh1's admins, each `unreachable`; nothing records their exit. The seat does
/// not move before the decision step (twice the floor). At it, mesh2's primary, the backbone rule's winner,
/// commits ONE `takeover` seat move: the fabric seat from mesh1's holder at its revision `N` to itself at
/// `N+1`, and seals the fabric lane at `N`. node.get names it the fabric-primary. Must NOT happen: the seat
/// moving before the decision step, an exit proof recorded for mesh1's admins, two takeover rows, or the
/// seat moving with no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_taken_over_at_the_decision_step_through_one_takeover_row() {
    let test = "a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_taken_over_at_the_decision_step_through_one_takeover_row";
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

    // Mesh investigation's decision step stands at twice the staleness floor (mesh-investigation.md).
    let decision = rdm_transport::membership::staleness_floor() * 2;
    let winner_path = Estate::path(&winner_name);
    estate.enter(&winner_path).await;
    let took = wait_for(&format!("{winner_name} holds the fabric seat"), decision + rdm_transport::membership::staleness_floor() * 6 + Duration::from_secs(60), || async {
        let n = estate.node_value(&winner_name).await;
        (n["is_fabric_primary"] == true).then_some(n)
    })
    .await;
    let elapsed = killed.elapsed();
    assert!(elapsed >= decision, "the seat moved before the decision step: {elapsed:?} < {decision:?}");
    assert_eq!(took["is_fabric_primary"], json!(true));

    let takeovers: Vec<Value> = seat_moves(&estate).into_iter().filter(|m| m["cause"] == "takeover").collect();
    assert_eq!(takeovers.len(), 1, "one takeover row: {takeovers:#?}");
    let seat = &takeovers[0]["seats"][0];
    assert_eq!((seat["seat"].clone(), seat["from_revision"].as_u64(), seat["revision"].as_u64(), s(&seat["holder"]["node_id"]), s(&seat["from"]["node_id"]), s(&takeovers[0]["by"]["node_id"])), (json!("FabricPrimary"), Some(lost_revision), Some(lost_revision + 1), s(&winner["node_id"]), s(&holder["node_id"]), s(&winner["node_id"])), "the fabric seat from mesh1's holder at {lost_revision} to {winner_name}: {takeovers:#?}");
    let seal = estate.fabric_store_root().join("fabric/builds").join(format!("rev-{lost_revision:020}"));
    let sealed = std::fs::read_dir(&seal).into_iter().flatten().flatten().any(|e| std::fs::read_to_string(e.path()).is_ok_and(|t| t.contains("\"format\":\"lane-seal/1\"")));
    assert!(sealed, "the fabric lane is sealed at revision {lost_revision}: {}", seal.display());

    // node.get names the winner once its book takes the seat; its takeover call goes on to release the
    // lost birth's name and announce the seat, and only then records its outcome. The estate stops
    // once that outcome is in the winner's evidence, so the stop never cuts the call short.
    wait_for(&format!("{winner_name}'s takeover call ends committed"), Duration::from_secs(10), || async {
        named(&estate.spans(), "rdm.node_admin.seat.update.via-takeover").iter().any(|sp| sp["attributes"]["outcome"] == "committed").then_some(())
    })
    .await;
    // Every process's evidence is flushed as it exits: the spans are read after the stop.
    estate.stop().await;
    let spans = estate.spans();
    let committed = named(&spans, "rdm.node_admin.seat.update.via-takeover").into_iter().filter(|sp| sp["attributes"]["outcome"] == "committed").cloned().collect::<Vec<_>>();
    assert_eq!(committed.len(), 1, "one committed takeover: {committed:#?}");
    let looks = named(&spans, "rdm.node_admin.seat.update.via-investigation").into_iter().filter(|sp| s(&sp["attributes"]["suspect"]).starts_with("mesh1.admin.")).cloned().collect::<Vec<_>>();
    assert!(!looks.is_empty() && looks.iter().all(|sp| sp["attributes"]["finding"] != "exit-proven"), "mesh1's admins are looked at and never proven exited: {looks:#?}");
    estate.record_trace_url(s(&committed[0]["trace_id"]).as_str());
    let probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == winner_name && sp["attributes"]["mesh"] == "mesh1").collect();
    assert!(probes.len() >= 2 && probes.iter().all(|sp| sp["attributes"]["outcome"] != "admin-alive"), "mesh2's primary probed mesh1's admins twice, none answering, before the decision: {probes:#?}");
    eprintln!("takeover: {winner_name} took the fabric seat at revision {} after {elapsed:?} (decision step {decision:?}), trace {}", lost_revision + 1, s(&committed[0]["trace_id"]));
}

/// CONTRACT (elections §5: "Every mesh-primary that notices tells the winner
/// (`node.fabric-primary.update`); repeated calls for the same takeover and committed successor answer
/// `AlreadyApplied`"): mesh1 holds the fabric seat and is lost, uninspectable, while mesh2 and mesh3
/// both live. The store cannot commit a seat move until the other mesh-primary (the higher NodeId of
/// the two) has told the winner: its proposal reaches the winner, which holds the takeover with the
/// other as proposer (`fabric.get`). Once the store writes, the backbone rule's winner writes ONE
/// `takeover` row, and every proposal of the other is answered `RejectedSourceFailed` while the store
/// could not commit, then from that takeover (`PrimaryUpdated` or `AlreadyApplied`). Must NOT happen:
/// two takeover rows, or the seat taken by the admin the rule does not seat. Holding the commit is what
/// makes both notice: with the store writable, the winner commits the moment its own hold expires and
/// the other learns the new record before its hold does, and correctly proposes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_mesh_primaries_noticing_the_lost_fabric_primary_together_commit_one_takeover() {
    use std::os::unix::fs::PermissionsExt;
    let test = "two_mesh_primaries_noticing_the_lost_fabric_primary_together_commit_one_takeover";
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1), ("mesh3", 1, 1)];
    let mut estate = estate_of(test, &shape).await;
    let nodes = estate.node_values().await;
    let primaries: Vec<Value> = nodes.iter().filter(|n| n["mesh"] != "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().collect();
    assert_eq!(primaries.len(), 2, "mesh2's and mesh3's primaries: {nodes:#?}");
    let winner = primaries.iter().min_by_key(|n| s(&n["node_id"])).cloned().unwrap();
    let other = primaries.iter().max_by_key(|n| s(&n["node_id"])).cloned().unwrap();
    let moves = estate.fabric_store_root().join("seat-moves");
    std::fs::set_permissions(&moves, std::fs::Permissions::from_mode(0o555)).unwrap();
    lose_mesh1(&estate, &nodes, "-KILL").await;
    let winner_path = Estate::path(&s(&winner["name"]));
    estate.enter(&winner_path).await;
    let floor = rdm_transport::membership::staleness_floor();
    wait_for(&format!("{} holds the takeover the other proposed", s(&winner["name"])), floor * 8 + Duration::from_secs(60), || async {
        let f = estate.fabric_view_at(&winner_path).await?;
        f.takeover.filter(|t| t.proposer.node_id.to_string() == s(&other["node_id"])).map(|_| ())
    })
    .await;
    assert_eq!(seat_moves(&estate).iter().filter(|m| m["cause"] == "takeover").count(), 0, "nothing committed while the store cannot be written");
    std::fs::set_permissions(&moves, std::fs::Permissions::from_mode(0o755)).unwrap();
    wait_for(&format!("{} holds the fabric seat", s(&winner["name"])), floor * 4 + Duration::from_secs(30), || async {
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
    // The winner's takeover call records its outcome after the release and the announcement: the estate
    // stops once that outcome is in its evidence, so the stop never cuts the call short.
    wait_for(&format!("{}'s takeover call ends committed", s(&winner["name"])), Duration::from_secs(20), || async {
        named(&estate.spans(), "rdm.node_admin.seat.update.via-takeover").iter().any(|sp| sp["attributes"]["outcome"] == "committed").then_some(())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    let committed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-takeover").into_iter().filter(|sp| sp["attributes"]["outcome"] == "committed").collect();
    assert_eq!(committed.len(), 1, "one committed takeover: {committed:#?}");
    let proposed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-takeover-proposed").into_iter().filter(|sp| sp["attributes"]["node"] == other["name"]).collect();
    assert!(!proposed.is_empty(), "{} told the winner: {proposed:#?}", s(&other["name"]));
    let outcomes: Vec<String> = proposed.iter().map(|sp| s(&sp["attributes"]["outcome"])).collect();
    assert!(outcomes.iter().all(|o| ["rejected-source-failed", "primary-updated", "already-applied"].contains(&o.as_str())), "every proposal is held while the store cannot commit, then answered from the one takeover: {outcomes:?}");
    estate.record_trace_url(s(&committed[0]["trace_id"]).as_str());
}

/// CONTRACT (elections §5: "the store validates it against the current committed seat revision,
/// rejects every stale one, and serializes the takeover with writes and claims so no old-authority
/// operation commits afterwards"; Q28 A): mesh1, the fabric-primary's mesh, is suspended whole while
/// mesh2's admins cannot inspect it; past the hold mesh2's primary takes the seat and seals the fabric
/// lane at the old revision. mesh1 resumes: the old fabric-primary, asked at once to accept a Build,
/// accepts nothing: it is refused stale against the seal, or it hears the record and sends the caller
/// to mesh2's primary, which may accept it. Nothing of the old revision is written past the seal; it
/// yields, and node.get names mesh2's primary. Must NOT happen: a Build accepted by the old
/// fabric-primary after the takeover, an old-authority Build fact committed past the seal, or two
/// fabric-primaries.
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
    let asked_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let refused = estate.nodes_obj().await.create(&rdm_client::NodeSpec { mesh: "mesh1".into(), kind: rdm_client::LaunchKind::RpcNode }).await;
    let sent_on = refused.as_ref().ok().map(|s| s.accepted().build_id.to_string());
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
    // The old fabric-primary accepted nothing once asked; a Build the caller was sent on with was accepted
    // by mesh2's primary.
    let accepted: Vec<(String, String)> = named(&spans, "rdm.node_admin.build.create.via-accepted").into_iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) >= asked_at).map(|sp| (s(&sp["attributes"]["node"]), s(&sp["attributes"]["build_id"]))).collect();
    assert!(accepted.iter().all(|(node, _)| *node == s(&winner["name"])), "only mesh2's primary accepts after the takeover: {accepted:?}");
    if let Some(id) = &sent_on {
        assert!(accepted.iter().any(|(_, b)| b.ends_with(&id.replace('-', "")) || b.contains(id.as_str()) || b.replace('-', "").ends_with(id.as_str())), "the Build the caller was sent on with was accepted by mesh2's primary: {id} {accepted:?}");
    }
    let stale: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-stale-seat").into_iter().collect();
    eprintln!("old authority: answered {:?}; stale-seat refusals {}", refused.as_ref().err().map(|e| e.to_string()).or(sent_on.clone()), stale.len());
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

/// The birth `node` holds in the entry admin's view once it is a birth other than `old` and ready
/// for traffic, asked until `within` passes; `None` past it.
async fn new_birth_within(estate: &Estate, node: &str, old: &str, within: Duration) -> Option<rdm_core::views::NodeView> {
    let until = std::time::Instant::now() + within;
    while std::time::Instant::now() < until {
        let found = estate.node_views().await.into_iter().find(|n| n.name.to_string() == node && n.node_id.to_string() != old && n.status == rdm_core::views::NodeStatus::ReadyForTraffic);
        if found.is_some() {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

/// What the create pipeline told the process `pid` about its data dir: its `RDM_INHERITED_DATA_DIR`.
fn inherited_env(pid: u64) -> Option<String> {
    let environ = std::fs::read(format!("/proc/{pid}/environ")).ok()?;
    environ.split(|b| *b == 0).filter_map(|kv| std::str::from_utf8(kv).ok()).find_map(|kv| kv.strip_prefix("RDM_INHERITED_DATA_DIR=").map(str::to_string))
}

/// The spans named `name` whose `attributes[key]` is `value`, each as one line of its fields.
fn spans_where(spans: &[Value], name: &str, key: &str, value: &str) -> Vec<String> {
    named(spans, name).into_iter().filter(|sp| s(&sp["attributes"][key]) == value).map(|sp| format!("{} {}", sp["trace_id"], sp["attributes"])).collect()
}

/// CONTRACT (elections §4.3: "The new fabric-primary re-creates the lost mesh's node-admin (mesh
/// investigation's fully-crashed arm, run by it): over the inherited data dir only with exit proof of
/// the old birth; otherwise over a newly allocated data dir with a new endpoint key, `inherited =
/// false`"; node-recovery.md `R-FM2`: "For a lost mesh's node-admin, the fabric-primary runs this arm
/// from the fabric lane ... with no inspection possible there is no exit proof and no gone-floors wait;
/// the node-admin is created at its path.name over a newly allocated `data_dir` with a newly minted
/// endpoint key, `inherited = false`, and the old birth's data dir is reused only once its exit is
/// proven"; Q51 A, Luke 2026-10-17: one takeover row moves both seats the lost birth holds, the lost
/// mesh's to no holder; the lost birth's row is written `Deleted` with exit proof `takeover { revision }`;
/// the re-born admin takes the vacant seat by the ordinary rule): mesh1 holds the fabric seat and is lost,
/// uninspectable from mesh2. mesh2's primary takes the seat over past the hold in ONE row that also moves
/// mesh1's primary seat to no holder, releases the lost birth (its exit proof `takeover { revision: T }`,
/// its row `Deleted`), then re-creates mesh1.admin.1 from the fabric lane: a new node.id at mesh1.admin.1,
/// ready for traffic, over a data dir that is not the old birth's, with a newly minted endpoint key, told
/// `RDM_INHERITED_DATA_DIR=0`, and seated as mesh1's primary by an `election` row from the vacated
/// revision; the old birth's data dir is left as it was. Must NOT happen: the old birth's data dir or
/// endpoint key reused, a provider exit proof recorded for mesh1's admins, the re-creation opened before
/// the takeover committed, or mesh1's seat moved by a second takeover row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_meshs_node_admin_is_re_created_from_the_fabric_lane_over_a_new_dir_with_inherited_false() {
    let test = "a_lost_meshs_node_admin_is_re_created_from_the_fabric_lane_over_a_new_dir_with_inherited_false";
    let mut estate = estate_of(test, &[("mesh1", 1, 1), ("mesh2", 1, 1)]).await;
    let nodes = estate.node_values().await;
    let old = nodes.iter().find(|n| n["name"] == "mesh1.admin.1").cloned().expect("mesh1.admin.1");
    assert_eq!(old["is_fabric_primary"], json!(true), "the Day-0 admin holds the fabric seat: {nodes:#?}");
    let (old_id, old_endpoint) = (s(&old["node_id"]), s(&old["endpoint_id"]));
    let old_dir = estate.bootstrap_data_dir("mesh1");
    let old_key = std::fs::read(old_dir.join("node-key")).expect("the old birth's endpoint key");
    let winner = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin").cloned().expect("mesh2's admin");
    let winner_name = s(&winner["name"]);
    lose_mesh1_unproven(&estate, &nodes).await;
    estate.enter(&Estate::path(&winner_name)).await;
    let floor = rdm_transport::membership::staleness_floor();
    wait_for(&format!("{winner_name} holds the fabric seat"), floor * 8 + Duration::from_secs(60), || async {
        (estate.node_value(&winner_name).await["is_fabric_primary"] == true).then_some(())
    })
    .await;
    let recreated = new_birth_within(&estate, "mesh1.admin.1", &old_id, floor * 12 + Duration::from_secs(120)).await;
    let told = match &recreated {
        Some(_) => inherited_env(estate.pid_of("mesh1.admin.1").await),
        None => None,
    };
    // Elections §4.3: once the re-created admin is Ready, the fabric-primary hands the seat back to it by
    // the committed handover of §4.4, naming it as the successor.
    let handed_back = match &recreated {
        Some(n) => {
            let id = n.node_id.to_string();
            let until = std::time::Instant::now() + floor * 12 + Duration::from_secs(60);
            let mut back = false;
            // The handback row is read the moment it is durable, from the fabric's seat moves.
            while !back && std::time::Instant::now() < until {
                back = seat_moves(&estate).iter().any(|m| m["cause"]["handover"].is_object() && m["seats"].as_array().into_iter().flatten().any(|x| x["seat"] == "FabricPrimary" && s(&x["holder"]["node_id"]) == id));
                if !back {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            back
        }
        None => false,
    };
    // The handback's Build ends before the estate stops: its seat row commits inside its attempt, and that
    // attempt's dispatch, asserted below, ends only once the run behind the row has (§4.4: the new holder
    // continues the Build from the row). Then the fabric's views name the re-created admin fabric-primary.
    if let (true, Some(n)) = (handed_back, &recreated) {
        let id = n.node_id.to_string();
        let handback = seat_moves(&estate).into_iter().find(|m| m["cause"]["handover"].is_object() && m["seats"].as_array().into_iter().flatten().any(|x| x["seat"] == "FabricPrimary" && s(&x["holder"]["node_id"]) == id));
        if let Some(cause) = handback.map(|m| m["cause"]["handover"].clone()) {
            let build_id = rdm_core::BuildId::parse(&s(&cause["build_id"])).unwrap_or_else(|e| panic!("the handback row names a Build id: {cause}: {e}"));
            let builds = estate.builds_obj().await;
            wait_for(&format!("the handback's Build {build_id} is complete"), floor * 12 + Duration::from_secs(60), || async { builds.get(&build_id).await.ok().filter(|b| b.phase == rdm_core::build::BuildPhase::Complete).map(|_| ()) }).await;
        }
        wait_for(&format!("the fabric's views name {id} fabric-primary"), floor * 4 + Duration::from_secs(30), || async { estate.node_views().await.iter().any(|v| v.node_id.to_string() == id && v.is_fabric_primary).then_some(()) }).await;
    }
    let moves = seat_moves(&estate);
    estate.stop().await;
    let spans = estate.spans();
    let admitted = spans_where(&spans, "rdm.node_admin.mesh.update.via-takeover-recreate", "lost_node_id", &old_id);
    let claims = spans_where(&spans, "rdm.node_admin.node.create.via-node-row", "subject", "mesh1.admin.1");
    let steps = spans_where(&spans, "rdm.node_admin.deployment.update.via-step", "node", "mesh1.admin.1");
    let Some(new) = recreated else {
        panic!("mesh1.admin.1 was not re-created from the fabric lane; admitted {admitted:#?}; name claims {claims:#?}; create steps {steps:#?}");
    };
    assert!(!admitted.is_empty(), "the re-creation was admitted on the takeover, with no exit proof");
    assert_ne!(new.endpoint_id.as_deref(), Some(old_endpoint.as_str()), "a newly minted endpoint key: {new:?}");
    let new_dir = new.data_dir.clone().expect("the re-created admin's data dir");
    assert_ne!(std::path::Path::new(&new_dir), old_dir.as_path(), "a newly allocated data dir: {new:?}");
    assert_eq!(std::fs::read(old_dir.join("node-key")).ok(), Some(old_key), "the old birth's data dir is left as it was");
    assert_eq!(told.as_deref(), Some("0"), "the re-created admin was told inherited = false");
    let proofs: Vec<String> = named(&spans, "rdm.node_admin.mesh.update.via-crash-proof").into_iter().filter(|sp| s(&sp["attributes"]["mesh"]) == "mesh1" && s(&sp["attributes"]["outcome"]) == "proven").map(|sp| sp["attributes"].to_string()).collect();
    assert!(proofs.is_empty(), "no provider exit proof of mesh1 was read: {proofs:#?}");
    // One takeover row moves both seats the lost birth held: the fabric seat to mesh2's admin, mesh1's
    // primary seat to no holder.
    let takeovers: Vec<&Value> = moves.iter().filter(|m| m["cause"] == "takeover").collect();
    assert_eq!(takeovers.len(), 1, "one takeover row: {moves:#?}");
    let mesh_seat = |m: &Value| m["seats"].as_array().into_iter().flatten().find(|x| x["seat"]["MeshPrimary"]["mesh"] == "mesh1").cloned();
    let vacated = mesh_seat(takeovers[0]).unwrap_or_else(|| panic!("the takeover row moves mesh1's primary seat too: {:#}", takeovers[0]));
    assert!(vacated["holder"].is_null() && s(&vacated["from"]["node_id"]) == old_id, "mesh1's seat moved from the lost birth to no holder: {vacated}");
    let vacated_at = vacated["revision"].as_u64().unwrap_or(0);
    // The lost birth holds nothing any more: its exit proof is the takeover's, its row Deleted.
    let t = takeovers.len();
    let released = spans_where(&spans, "rdm.node_admin.node.delete.via-takeover-release", "subject", "mesh1.admin.1");
    assert!(released.iter().any(|r| r.contains("\"exit_proof\":\"takeover { revision: ") && r.contains("\"row\":\"deleted\"") && r.contains("\"exit_proof_row\":\"put\"")), "the lost birth is released by the takeover ({t} row): {released:#?}");
    // The re-born admin takes the vacant seat by the ordinary rule: an election row from the vacated revision.
    let seated = moves.iter().filter(|m| m["cause"] == "election").filter_map(mesh_seat).find(|x| s(&x["holder"]["node_id"]) == new.node_id.to_string()).unwrap_or_else(|| panic!("mesh1's re-born admin is seated by an election row: {moves:#?}"));
    assert_eq!((seated["from_revision"].as_u64(), seated["revision"].as_u64()), (Some(vacated_at), Some(vacated_at + 1)), "seated from the vacated revision: {seated}");
    // The handback (§4.3, §4.4): one handover row moves the fabric seat from mesh2's admin to mesh1's
    // re-created admin, from the takeover's revision to the next.
    let fabric_seat = |m: &Value| m["seats"].as_array().into_iter().flatten().find(|x| x["seat"] == "FabricPrimary").cloned();
    let taken_at = fabric_seat(takeovers[0]).and_then(|x| x["revision"].as_u64()).unwrap_or_else(|| panic!("the takeover row moves the fabric seat: {:#}", takeovers[0]));
    let handbacks: Vec<&Value> = moves.iter().filter(|m| m["cause"]["handover"].is_object() && fabric_seat(m).is_some_and(|x| s(&x["holder"]["node_id"]) == new.node_id.to_string())).collect();
    assert!(handed_back && handbacks.len() == 1, "the fabric-primary hands the seat back to mesh1's re-created admin {} by one handover row: {moves:#?}", new.node_id);
    let back = fabric_seat(handbacks[0]).expect("the handover row moves the fabric seat");
    assert_eq!((back["from_revision"].as_u64(), back["revision"].as_u64()), (Some(taken_at), Some(taken_at + 1)), "handed back from the takeover's revision: {back}");
    // The fabric-primary runs the handback itself: its attempt is dispatched to no other node.
    let cause = &handbacks[0]["cause"]["handover"];
    let dispatched: Vec<String> = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|sp| s(&sp["attributes"]["attempt"]) == cause["attempt"].to_string() && s(&sp["attributes"]["build_id"]).replace('-', "").trim_start_matches("bid_") == s(&cause["build_id"])).map(|sp| s(&sp["attributes"]["executor"])).collect();
    assert!(!dispatched.is_empty() && dispatched.iter().all(|e| *e == winner_name), "the handback attempt is the fabric-primary's own, dispatched to {winner_name} alone: {dispatched:?} for {cause}");
    estate.record_trace_url(admitted[0].split(' ').next().unwrap_or("").trim_matches('"'));
}

/// CONTRACT (elections §4.3: "over the inherited data dir only with exit proof of the old birth";
/// node-recovery.md "Retained": "inherited data dir and endpoint key"): mesh1 holds the fabric seat and
/// is lost; mesh2's admin can inspect it, so every node of mesh1 is proven exited, the seat moves by the
/// ordinary rule, and mesh2's primary re-creates mesh1.admin.1 (the fully crashed arm) over the old
/// birth's data dir: a new node.id at mesh1.admin.1, ready for traffic, with the old birth's endpoint
/// key, told `RDM_INHERITED_DATA_DIR=1`. Must NOT happen: a newly allocated data dir or a newly minted
/// key for an admin whose exit is proven.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_meshs_node_admin_is_re_created_over_its_inherited_dir_only_with_exit_proof() {
    let test = "a_lost_meshs_node_admin_is_re_created_over_its_inherited_dir_only_with_exit_proof";
    let mut estate = estate_of(test, &[("mesh1", 1, 1), ("mesh2", 1, 1)]).await;
    let nodes = estate.node_values().await;
    let old = nodes.iter().find(|n| n["name"] == "mesh1.admin.1").cloned().expect("mesh1.admin.1");
    let (old_id, old_endpoint) = (s(&old["node_id"]), s(&old["endpoint_id"]));
    let old_dir = estate.bootstrap_data_dir("mesh1");
    let winner_name = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin").cloned().expect("mesh2's admin")["name"]);
    // Lose mesh1, inspectable from mesh2: no provider-domain cut.
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    for pid in pids {
        estate.kill_pid(pid as u32);
    }
    estate.kill_bootstrap();
    estate.enter(&Estate::path(&winner_name)).await;
    let floor = rdm_transport::membership::staleness_floor();
    let recreated = new_birth_within(&estate, "mesh1.admin.1", &old_id, floor * 12 + Duration::from_secs(120)).await;
    let told = match &recreated {
        Some(_) => inherited_env(estate.pid_of("mesh1.admin.1").await),
        None => None,
    };
    estate.stop().await;
    let spans = estate.spans();
    let steps = spans_where(&spans, "rdm.node_admin.deployment.update.via-step", "node", "mesh1.admin.1");
    let Some(new) = recreated else {
        panic!("mesh1.admin.1 was not re-created; create steps {steps:#?}");
    };
    assert_eq!(new.endpoint_id.as_deref(), Some(old_endpoint.as_str()), "the old birth's endpoint key, loaded from its data dir: {new:?}");
    assert_eq!(new.data_dir.as_deref().map(std::path::PathBuf::from), Some(old_dir.clone()), "the old birth's data dir: {new:?}");
    assert_eq!(told.as_deref(), Some("1"), "the re-created admin was told inherited = true");
    let proof = named(&spans, "rdm.node_admin.mesh.update.via-crash-proof").into_iter().find(|sp| s(&sp["attributes"]["mesh"]) == "mesh1" && s(&sp["attributes"]["outcome"]) == "proven").cloned().expect("every node of mesh1 proven exited");
    estate.record_trace_url(&s(&proof["trace_id"]));
}
