//! i143 process E2E: the current Build, a complete Build, is the one accepted topology (RDM #47).
//!
//! From the typed objects and the estate's evidence only:
//! 1. Day 0 accepts the first Build; `fabric.get` names it and the Build log holds its exact topology.
//! 2. A change compiles to the next complete Build, current once it is written; a second change while
//!    it reconciles is refused `build-in-progress`, naming it.
//! 3. The current Build cannot be forgotten; older Builds are history and can.
//! 4. An admin born later holds the current Build before Ready, and none of the history: it reads
//!    the Build facts last in, first out from its own Build log (R-BI3; R14: the fabric-primary is
//!    the one writer of `fabric/`), the Build its own Build log's copy.
//! 5. A birth stopped (silent, its runtime alive) is node investigation's (node-recovery.md step 3): a
//!    birth that answers its tickle, or is heard again by its digest, is reached and commanded back
//!    through node.mesh.join; the fabric-primary opens no drift for it.
//! 6. An admin stopped for less than the staleness floor misses a Build's acceptance and catches up to
//!    the current Build when it is back: it reads the Build facts written while it was stopped and
//!    holds the newest in progress or completed (R-BI3: no pointer names the current Build).
//! 7. A member killed (its exact runtime exited) is node recovery (node-recovery.md): its
//!    mesh-primary proves the exit and runs the recovery itself, its own workflow and never a Build
//!    (R-BI7, Q342), once: node.delete of the exited birth, then node.create at the same path and name
//!    with a new node.id over the inherited data dir and endpoint key. The current Build and its record
//!    stay what they were, and no Build runs the recovery.
//! 8. A stop and a start change no topology: each is a node command of the primary above the node,
//!    never a Build or part of one (Q342).

use crate::build_log;
use rdm_core::{NodeId, OperationId, PathName};
use rdm_client::{BuildId, CallEnd, FabricDesired, Fabrics, MeshDesired, NodeStatus};
use rdm_test::typed::BuildSpec;
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "accepted-topology".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_current_build_read_from_the_build_facts_is_the_one_accepted_topology".into(),
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn signal(pid: u64, sig: &str) {
    let ok = std::process::Command::new("kill").args([sig, &pid.to_string()]).status().unwrap().success();
    assert!(ok, "kill {sig} {pid}");
}

/// The current build_id as the admin the estate asks holds it (`fabric.get`).
async fn current_build(estate: &Estate) -> String {
    let id = estate.accepted_build_id().await;
    assert!(!id.is_empty(), "the fabric names its current build_id");
    id
}

fn location(path: &str, name: &str) -> PathName {
    PathName::new(path, name).unwrap()
}

fn actor_is(span: &Value, at: &PathName) -> bool {
    span["attributes"]["node_path"] == at.path && span["attributes"]["node_name"] == at.name
}

fn paths(build: &Value, mesh: &str) -> Vec<PathName> {
    serde_json::from_value(build["topology"]["meshes"][mesh]["nodes"].clone()).expect("accepted topology carries structured locations")
}

fn fabric_of(meshes: &[(&str, u32, u32)]) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)])).collect() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_current_build_read_from_the_build_facts_is_the_one_accepted_topology() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;

    // 1. Day 0: the first Build, exact.
    let day0_admin = estate.node_view(&location("mesh1.node-admin", "1")).await;
    let b0 = current_build(&estate).await;
    // The fabric-primary role appears one gossip interval before the executor's next pass: wait for the Build
    // itself, never for the role that precedes it.
    build_log::build_fact(&estate, &b0, "complete", Duration::from_secs(60), |b| b.state == rdm_admin::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    let day0 = build_log::build_value(&estate, &b0).await;
    assert_eq!(paths(&day0, "mesh1"), [location("mesh1.node-admin", "1")], "{day0}");
    assert_eq!(day0["state"], "complete", "{day0}");

    // 2. A change: the next complete Build; one in flight at a time.
    let stream = estate.submit(&BuildSpec::Fabric(fabric_of(&[("mesh1", 2, 3)]))).await;
    let a = stream.build_id().to_string();
    assert_eq!(current_build(&estate).await, a, "the Build is current on acceptance, before it converges");
    let refused = estate.carrier().await.create(estate.shape_change(&fabric_of(&[("mesh1", 2, 4)])).await).await;
    assert!(matches!(&refused, Err(CallEnd::BuildInProgress { current }) if current.as_str() == a), "a second change while {a} reconciles is refused build-in-progress, naming it: {:?}", refused.as_ref().err());
    estate.follow(stream).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let built = build_log::build_value(&estate, &a).await;
    assert_eq!(paths(&built, "mesh1"), [location("mesh1.node", "1"), location("mesh1.node", "2"), location("mesh1.node", "3"), location("mesh1.node-admin", "1"), location("mesh1.node-admin", "2")], "{built}");
    // The change is history the fabric-primary records with the Build, whole, in the journal on the store
    // root, and the Build read from there carries it (Q292).
    let recorded = rdm_admin::store_log::read_record(&estate.fabric_store_root().join(rdm_admin::store_root::BUILDS_FILE), "build-record", Some(&a.to_string())).unwrap_or_else(|| panic!("the journal holds Build {a}'s acceptance"));
    // The change is the Create as it came in: the nodes it adds, never the whole topology (build.md §1).
    let created = |change: &Value| -> Vec<PathName> {
        let mut nodes: Vec<PathName> = change["Create"]["nodes"].as_array().unwrap_or_else(|| panic!("a Create: {change}")).iter().map(|n| location(n["path"].as_str().unwrap(), n["name"].as_str().unwrap())).collect();
        nodes.sort();
        nodes
    };
    let added = vec![location("mesh1.node", "1"), location("mesh1.node", "2"), location("mesh1.node", "3"), location("mesh1.node-admin", "2")];
    assert_eq!(created(&recorded["data"]["fact"]["change"]), added, "the change is kept as history, as it came in: {recorded}");
    assert_eq!(created(&built["change"]), added, "the Build read whole from the store carries its change: {built}");

    // 3. The accepted Build is never history; Day 0's is.
    let refused = estate.builds_obj().await.delete(&BuildId::parse(&a).unwrap()).await;
    assert!(matches!(&refused, Err(CallEnd::Rejected { reason, .. }) if reason == "cannot-delete"), "the accepted Build {a} stays in history: {refused:?}");
    estate.builds_obj().await.delete(&BuildId::parse(&b0).unwrap()).await.unwrap_or_else(|e| panic!("Day 0's Build {b0} leaves history: {e}"));
    let facts = build_log::log_read(&estate, &b0).await.unwrap_or_else(|end| panic!("the fabric-primary's log answers the read of the forgotten Build {b0}: {end}"));
    assert!(build_log::projection(&estate, &facts, &b0).is_none(), "the forgotten Build {b0} is no Build of the fabric-primary's log: {facts:?}");
    assert_eq!(current_build(&estate).await, a, "forget is history administration only");

    // 4. An admin born later hydrates the current Build, not the history.
    let mut spawn = estate.create_node("mesh1", "node-admin").await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of a node-admin in mesh1: {e}"));
    let c = Estate::complete(&mut spawn).await.expect("the call opened a Build").build_id.to_string();
    let nodes = Estate::values(&estate.settled_views(&[("mesh1", 3, 3)]).await);
    let late = nodes.iter().find(|n| n["path"] == "mesh1.node-admin" && n["name"] == "3").cloned().unwrap();
    let late_path: PathName = rdm_test::estate::location(&late);
    assert_eq!(estate.fabric_view_at(&late_path).await.expect("the late admin answers fabric.get").build_id.as_deref(), Some(c.as_str()));
    // The late admin reads the Build records from the store root as every admin does (Q185); the completion is a
    // later record the fabric-primary appended, so the read awaits it as step 1 does.
    let held = build_log::build_fact(&estate, &c, "complete", Duration::from_secs(60), |b| b.state == rdm_admin::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    let held = serde_json::to_value(held).expect("a Build view serializes");
    assert_eq!(held["state"], "complete", "{held}");
    assert_eq!(paths(&held, "mesh1").len(), 6, "the late admin holds the accepted Build's topology: {held}");

    // 5. A held birth that falls silent is not drift. The hold is a hang (every task but the leader in
    // a tracer's stop), which the provider reads `running`, so node investigation's tickle reaches it
    // once released; a SIGSTOP reads `os-stopped`, which re-creates it (node-investigation.md, Q273).
    let held_pid = estate.pid_of(&rdm_core::PathName::new("mesh1.node", "3").unwrap()).await as u32;
    let held_rt = rdm_test::process_faults::ExactRuntime { deployment_id: String::new(), control_domain: rdm_test::process_faults::local_control_domain(), pid: held_pid, start: rdm_test::process_faults::start_token(held_pid).expect("mesh1.node.3 has a start token") };
    let hung = held_rt.hang().expect("the hang is acknowledged by the OS");
    estate.until_node(&rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1.node.3 is silent in the view", rdm_gossip::topology::staleness_floor() + Duration::from_secs(15), |n| n.filter(|n| matches!(n.state, NodeStatus::Dead | NodeStatus::PendingReconnect)).map(|_| ())).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    hung.release();
    estate.until_node(&rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1.node.3 is heard again", Duration::from_secs(20), |n| n.filter(|n| n.state == NodeStatus::Ready).map(|_| ())).await;

    // 6. An admin that misses a Build catches up to the current Build when it is back.
    let quiet = estate.node_values().await.into_iter().find(|n| n["type"] == "node-admin" && n["is_primary"] == false && n["is_fabric_primary"] == false).expect("a mesh1 admin that holds no primary role");
    let quiet_name: PathName = rdm_test::estate::location(&quiet);
    let stopped = estate.pid_of(&quiet_name).await;
    // Stopped across the acceptance only, never past the staleness floor: the admin misses the Build's
    // acceptance and is never silent, so no investigation looks at it. The `BuildService` answers once
    // the acceptance is written, so the admin is back as soon as the `BuildService` answers. A stop held on to the
    // Build's later progress outlasts the floor on a slow host, and then the stopped birth is node
    // investigation's (node-investigation.md: an `os-stopped` verdict re-creates it at its path and name,
    // and a birth that comes back is a deleted birth, node-recovery.md step 3).
    signal(stopped, "-STOP");
    let r4_stream = estate.submit(&BuildSpec::Fabric(fabric_of(&[("mesh1", 3, 4)]))).await;
    let r4 = r4_stream.build_id().to_string();
    assert_eq!(current_build(&estate).await, r4);
    let resumed_at = now_ns();
    signal(stopped, "-CONT");
    // The Build is Complete only once its ready state is set (Q154), and a fabric state is set
    // only when every member has checked in, the admin that was stopped too.
    tokio::time::timeout(Duration::from_secs(90), estate.follow(r4_stream)).await.unwrap_or_else(|_| panic!("{r4} reached no Complete within 90s of {quiet_name:?} being back"));
    let quiet_carrier = estate.peers().await.carrier_for(NodeId::parse(quiet["node_id"].as_str().unwrap()).unwrap());
    Fabrics::new(quiet_carrier.clone()).fact(&format!("the paused admin names {r4} its current Build"), Duration::from_secs(30), |f| f.build_id.as_deref() == Some(r4.as_str())).await.unwrap_or_else(|end| panic!("{end}"));
    build_log::build_fact(&estate, &r4, "the store holds the Build the paused admin reads", Duration::from_secs(30), |_| true).await.unwrap_or_else(|end| panic!("{end}"));
    estate.settled_views(&[("mesh1", 3, 4)]).await;

    // 7. A killed member is its mesh-primary's node recovery, the primary's own workflow, never a Build
    // or part of one (Q342): the current Build and its record are what they were.
    let before = build_log::build_value(&estate, &r4).await;
    let builds = estate.build_ids().await;
    let lost = estate.node_value(&rdm_core::PathName::new("mesh1.node", "2").unwrap()).await;
    let mesh_primary = estate.node_values().await.into_iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh1" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh1's primary")["node_id"].as_str().unwrap().to_string();
    estate.kill_node(&rdm_core::PathName::new("mesh1.node", "2").unwrap()).await;
    let back = estate
        .until_node(&rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1.node.2 is recovered", crate::node_recovery_due(), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["state"] == "ready" && n["node_id"] != lost["node_id"] && n["incarnation"] != lost["incarnation"]).then_some(n)
        })
        .await;
    estate.settled_views(&[("mesh1", 3, 4)]).await;
    assert_eq!(current_build(&estate).await, r4, "recovery writes no Build");
    assert_eq!(estate.build_ids().await, builds, "recovery opens no Build");
    assert_eq!(build_log::build_value(&estate, &r4).await, before, "the current Build's record is what it was: a recovery is no part of it");
    assert_ne!(back["node_id"], lost["node_id"], "a lost birth's path gets a new logical node");
    assert_ne!(back["endpoint_id"], lost["endpoint_id"], "a new endpoint_id (Q358)");

    // 8. A stop, then a start: node commands of the primary above the node, never a Build or part of
    // one (Q342): the Builds are what they were.
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let restarted_from = estate.node_value(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await;
    let builds = estate.build_ids().await;
    estate.stop_then_start(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await;
    assert_eq!(estate.build_ids().await, builds, "a stop and a start write no Build");
    estate
        .until_node(&rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1.node.1 is ready again as the same birth", Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["state"] == "ready" && n["incarnation"] == restarted_from["incarnation"] && n["node_id"] == restarted_from["node_id"]).then_some(())
        })
        .await;
    assert_eq!(current_build(&estate).await, r4);

    estate.stop().await;
    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap();
    // 5: nothing while the held birth was only silent: no recovery of mesh1.node.3.
    let recovered_held: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_path"] == "mesh1.node" && sp["attributes"]["node_name"] == "3").collect();
    assert!(recovered_held.is_empty(), "a silent, live birth was recovered: {recovered_held:?}");
    // 7: the member's own mesh-primary recovers it, its own workflow: the submit, its delete and its
    // create, under the submit (node-recovery.md "Execution", Q342).
    let submit = named(&spans, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == lost["node_id"] && sp["attributes"]["incarnation_id"] == lost["incarnation"])
        .unwrap_or_else(|| panic!("mesh1's primary submitted the recovery of the killed birth"));
    let sa = &submit["attributes"];
    assert_eq!(sa["submitter"], mesh_primary.as_str(), "the member's own mesh-primary submits: {submit}");
    assert_eq!(sa["incarnation_id"], lost["incarnation"], "keyed on the exact exited birth: {submit}");
    assert_eq!(sa["outcome"], "complete", "the submit followed the recovery to its end: {submit}");
    for (name, what) in [("rdm.node_admin.node.delete.via-node-recovery", "delete"), ("rdm.node_admin.node.create.via-node-recovery", "create")] {
        let child = named(&spans, name).into_iter().find(|sp| sp["attributes"]["node_id"] == lost["node_id"] && sp["parent_span_id"] == submit["span_id"]).unwrap_or_else(|| panic!("the recovery's {what} of the killed birth runs under its submit"));
        assert_eq!(child["trace_id"], submit["trace_id"], "{child}");
    }
    let ran = named(&spans, "rdm.node_admin.build.update.via-reconcile");
    assert!(ran.iter().all(|sp| sp["attributes"]["reason"] != "recovery"), "no Build ran the recovery: {ran:?}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == lost["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], lost["incarnation"], "named by the exact exited birth");
    estate.record_trace_url(submit["trace_id"].as_str().unwrap_or(""));
    // 8: the stop ran as a node command of the primary above the node, never a Build or part of one (Q342).
    let stop_command = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().find(|sp| sp["attributes"]["command"] == "stop-node" && sp["attributes"]["subject_id"] == restarted_from["node_id"] && sp["attributes"]["outcome"] == "node-stopped").cloned();
    assert!(stop_command.is_some(), "the stop's node command span at the primary above mesh1.node.1");
    assert!(ran.iter().all(|sp| sp["attributes"]["reason"] != "stop"), "no Build ran the stop: {ran:?}");
    // 4 and 6: the fabric-primary writes each Build once; the late admin committed Ready holding the
    // current Build, and the paused admin caught up to the Build written while it was stopped.
    let moved = named(&spans, "rdm.node_admin.build.create.via-accepted");
    let late_ready = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == late["node_id"] && sp["attributes"]["incarnation_id"] == late["incarnation"])
        .cloned()
        .expect("the late admin is ready");
    assert_eq!(late_ready["attributes"]["build_id"], c.as_str(), "the late admin held the current Build when it committed Ready: {late_ready}");
    assert!(
        moved.iter().any(|sp| sp["attributes"]["node"] == day0_admin.node_id.as_str() && sp["attributes"]["build_id"] == c.as_str() && at(sp) < at(&late_ready)),
        "the fabric-primary wrote {c} before the late admin was ready: {moved:?}"
    );
    assert!(
        moved.iter().any(|sp| sp["attributes"]["node"] == day0_admin.node_id.as_str() && sp["attributes"]["build_id"] == r4.as_str() && at(sp) < resumed_at),
        "the fabric-primary wrote {r4} while {quiet_name:?} was stopped: {moved:?}"
    );
    assert!(named(&spans, "rdm.node_admin.build.create.via-proven-drift").is_empty(), "no Build is minted for drift");
}
