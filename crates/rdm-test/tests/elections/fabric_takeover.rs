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

/// CONTRACT (Q89; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 2,
/// "Outside its mesh": a mesh-primary runs mesh investigation's ladder on the fabric-primary's mesh; "No
/// answer from any of them by the decision step is the backbone rule, and the lowest ReadyForTraffic
/// mesh-primary is fabric-primary and records it on its own row"; node-rpc-envelope.md 0x21: the winner
/// "is fabric-primary ... and answers `PrimaryUpdated { revisions }`"): mesh1 is the fabric-primary's mesh.
/// mesh2's admins cannot inspect mesh1's runtimes (provider-domain cut), and every process of mesh1 is
/// killed. mesh2's primary probes mesh1's admins, each `unreachable`; nothing records their exit. The
/// fabric-primary does not move before the decision step (twice the floor). At it, mesh2's primary, the
/// election's winner on the backbone, takes over once: the fabric-primary from mesh1's at its revision `N`
/// to itself at `N+1`, and node.get names it the fabric-primary. Must NOT happen: the fabric-primary moving
/// before the decision step, an exit recorded for mesh1's admins, or two take-overs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_taken_over_once_at_the_decision_step() {
    let test = "a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_taken_over_once_at_the_decision_step";
    // The faulted-admin binding set: every admin serves the testkit's fault door.
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_HUNG_HOLD_FLOORS", "2")]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 2, 1)], Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;

    let nodes = estate.node_values().await;
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&holder["mesh"]), "mesh1", "the Day-0 admin's mesh holds the fabric-primary: {nodes:#?}");
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
    let took = wait_for(&format!("{winner_name} is the fabric-primary"), decision + rdm_transport::membership::staleness_floor() * 6 + Duration::from_secs(60), || async {
        let n = estate.node_value(&winner_name).await;
        (n["is_fabric_primary"] == true).then_some(n)
    })
    .await;
    let elapsed = killed.elapsed();
    assert!(elapsed >= decision, "the fabric-primary moved before the decision step: {elapsed:?} < {decision:?}");
    assert_eq!(took["is_fabric_primary"], json!(true));
    // The take-over records its outcome after the release: the estate stops once it is in the winner's
    // evidence, so the stop never cuts the call short.
    wait_for(&format!("{winner_name}'s take-over ends"), Duration::from_secs(10), || async {
        named(&estate.spans(), "rdm.node_admin.primary.update.via-take-over").iter().any(|sp| sp["attributes"]["outcome"] == "primary-updated").then_some(())
    })
    .await;
    // Every process's evidence is flushed as it exits: the spans are read after the stop.
    estate.stop().await;
    let spans = estate.spans();
    let took_over = named(&spans, "rdm.node_admin.primary.update.via-take-over").into_iter().filter(|sp| sp["attributes"]["outcome"] == "primary-updated").cloned().collect::<Vec<_>>();
    assert_eq!(took_over.len(), 1, "one take-over: {took_over:#?}");
    assert_eq!(s(&took_over[0]["attributes"]["node"]), winner_name, "by the backbone's winner: {took_over:#?}");
    assert!(s(&took_over[0]["attributes"]["lost"]).contains(&s(&holder["node_id"])), "of mesh1's fabric-primary: {took_over:#?}");
    let looks = named(&spans, "rdm.node_admin.seat.update.via-investigation").into_iter().filter(|sp| s(&sp["attributes"]["suspect"]).starts_with("mesh1.admin.")).cloned().collect::<Vec<_>>();
    assert!(!looks.is_empty() && looks.iter().all(|sp| sp["attributes"]["finding"] != "exit-proven"), "mesh1's admins are looked at and never proven exited: {looks:#?}");
    estate.record_trace_url(s(&took_over[0]["trace_id"]).as_str());
    let probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == winner_name && sp["attributes"]["mesh"] == "mesh1").collect();
    assert!(probes.len() >= 2 && probes.iter().all(|sp| sp["attributes"]["outcome"] != "admin-alive"), "mesh2's primary probed mesh1's admins twice, none answering, before the decision: {probes:#?}");
    eprintln!("take-over: {winner_name} is the fabric-primary after {elapsed:?} (decision step {decision:?}), trace {}", s(&took_over[0]["trace_id"]));
}

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
/// proven"; fabric-recovery.md step 4: once the node at the lost fabric-primary's path.name is
/// ReadyForTraffic for one floor and its mesh's mesh-primary, the temporary fabric-primary hands back by
/// `node.fabric-primary.update`): mesh1 is the fabric-primary's mesh and is lost, uninspectable from mesh2.
/// mesh2's primary takes the fabric-primary over once, releases the lost birth (its row `Deleted`), then
/// re-creates mesh1.admin.1 from the fabric lane: a new node.id at mesh1.admin.1, ready for traffic, over a
/// data dir that is not the old birth's, with a newly minted endpoint key, told `RDM_INHERITED_DATA_DIR=0`,
/// mesh1's mesh-primary by the election; the old birth's data dir is left as it was. The fabric-primary is
/// handed back to it by the temporary fabric-primary's own update, whose successor installs it. Must NOT
/// happen: the old birth's data dir or endpoint key reused, a provider exit proof recorded for mesh1's
/// admins, or a second take-over.
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
    // fabric-recovery.md step 4: once the re-created admin is Ready for one floor and mesh1's mesh-primary,
    // the temporary fabric-primary hands back to it; the fabric's views then name it fabric-primary.
    let handed_back = match &recreated {
        Some(n) => {
            let id = n.node_id.to_string();
            wait_for(&format!("the fabric's views name {id} fabric-primary"), floor * 12 + Duration::from_secs(60), || async { estate.node_views().await.iter().any(|v| v.node_id.to_string() == id && v.is_fabric_primary && v.is_primary).then_some(()) }).await;
            true
        }
        None => false,
    };
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
    // One take-over, by mesh2's admin, of the lost birth.
    let took_over: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-take-over").into_iter().filter(|sp| sp["attributes"]["outcome"] == "primary-updated").collect();
    assert_eq!(took_over.len(), 1, "one take-over: {took_over:#?}");
    assert!(s(&took_over[0]["attributes"]["node"]) == winner_name && s(&took_over[0]["attributes"]["lost"]).contains(&old_id), "{winner_name} took over from the lost birth: {took_over:#?}");
    // The lost birth holds nothing any more: its row Deleted.
    let released = spans_where(&spans, "rdm.node_admin.node.delete.via-takeover-release", "subject", "mesh1.admin.1");
    assert!(released.iter().any(|r| r.contains("\"row\":\"deleted\"")), "the lost birth is released by the take-over: {released:#?}");
    // The hand-back: the temporary fabric-primary's own update moves the fabric-primary to mesh1's
    // re-created admin, which installs it.
    let back: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-update-primary").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == winner_name && sp["attributes"]["moving"] == "fabric-primary" && sp["attributes"]["outcome"] == "updated" && sp["attributes"]["successor"] == "mesh1.admin.1").collect();
    assert!(handed_back && back.len() == 1, "the fabric-primary is handed back to mesh1's re-created admin {} by one update: {back:#?}", new.node_id);
    let installed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && s(&sp["attributes"]["primaries"]).starts_with("fabric ") && sp["attributes"]["outcome"] == "held").collect();
    assert_eq!(installed.len(), 1, "mesh1's re-created admin installed the fabric-primary once: {installed:#?}");
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
