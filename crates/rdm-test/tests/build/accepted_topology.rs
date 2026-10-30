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
//!    mesh-primary proves the exit and opens the recovery itself, the next attempt of its mesh's lane
//!    of the same Build (R-BI7), once: node.delete of the exited birth, then node.create at the same
//!    path.name with a new node.id over the inherited data dir and endpoint key. The current Build
//!    stays the same; the attempt converges; the fabric lane gains no attempt; the fabric-primary opens no drift
//!    attempt for the member.
//! 8. A stop and a start change no topology: each is an attempt of the accepted Build, under the same id.

use crate::build_log;
use rdm_client::{BuildId, CallEnd, FabricDesired, Fabrics, LaunchKind as NodeKind, MeshDesired, NodeStatus};
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

fn paths(build: &Value, mesh: &str) -> Vec<String> {
    build["topology"]["meshes"][mesh]["nodes"].as_array().map(|a| a.iter().filter_map(|p| p.as_str().map(String::from)).collect()).unwrap_or_default()
}

fn fabric_of(meshes: &[(&str, u32, u32)]) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)])).collect() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_current_build_read_from_the_build_facts_is_the_one_accepted_topology() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;

    // 1. Day 0: the first Build, exact.
    let b0 = current_build(&estate).await;
    // The fabric-primary role appears one gossip interval before the executor's next pass: wait for the Build
    // itself, never for the role that precedes it.
    build_log::build_fact(&estate.carrier().await, &b0, "complete", Duration::from_secs(60), |b| b.state == rdm_admin::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    let day0 = build_log::build_value(&estate.carrier().await, &b0).await;
    assert_eq!(paths(&day0, "mesh1"), ["mesh1.admin.1"], "{day0}");
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
    let built = build_log::build_value(&estate.carrier().await, &a).await;
    assert_eq!(paths(&built, "mesh1"), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"], "{built}");
    // The change is history the fabric-primary records with the Build, whole, in the journal on the store
    // root; the Build's facts travel with its acceptance as sections, which carry no change (Q284).
    let recorded = rdm_admin::store_log::read_record(&estate.fabric_store_root().join(rdm_admin::store_root::BUILDS_FILE), "build-record", Some(&format!("{a}/accepted"))).unwrap_or_else(|| panic!("the journal holds Build {a}'s acceptance"));
    assert_eq!(recorded["record"]["fact"]["submitted_change"]["kind"], "reconcile", "the change is kept as history: the complete topology the request named: {recorded}");
    assert!(built["submitted_change"].is_null(), "a section carries no change: {built}");

    // 3. The accepted Build is never history; Day 0's is.
    let refused = estate.builds_obj().await.delete(&BuildId::parse(&a).unwrap()).await;
    assert!(matches!(&refused, Err(CallEnd::Rejected { reason, .. }) if reason == "cannot-delete"), "the accepted Build {a} stays in history: {refused:?}");
    estate.builds_obj().await.delete(&BuildId::parse(&b0).unwrap()).await.unwrap_or_else(|e| panic!("Day 0's Build {b0} leaves history: {e}"));
    let facts = build_log::log_read(&estate.carrier().await, &b0).await.unwrap_or_else(|end| panic!("the fabric-primary's log answers the read of the forgotten Build {b0}: {end}"));
    assert!(build_log::projection(&facts, &b0).is_none(), "the forgotten Build {b0} is no Build of the fabric-primary's log: {facts:?}");
    assert_eq!(current_build(&estate).await, a, "forget is history administration only");

    // 4. An admin born later hydrates the current Build, not the history.
    let mut spawn = estate.create_node("mesh1", NodeKind::NodeAdmin).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of a node-admin in mesh1: {e}"));
    let c = Estate::complete(&mut spawn).await.build_id.to_string();
    let nodes = Estate::values(&estate.settled_views(&[("mesh1", 3, 3)]).await);
    let late = nodes.iter().find(|n| n["name"] == "mesh1.admin.3").cloned().unwrap();
    let late_path = late["name"].as_str().unwrap().parse().unwrap();
    assert_eq!(estate.fabric_view_at(&late_path).await.expect("the late admin answers fabric.get").build_id.as_deref(), Some(c.as_str()));
    // The late admin reads the Build records from the store root as every admin does (Q185); the completion is a
    // later record the fabric-primary appended, so the read awaits it as step 1 does.
    let held = build_log::build_fact(&estate.carrier_at(&late_path).await, &c, "complete", Duration::from_secs(60), |b| b.state == rdm_admin::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    let held = serde_json::to_value(held).expect("a Build view serializes");
    assert_eq!(held["state"], "complete", "{held}");
    assert_eq!(paths(&held, "mesh1").len(), 6, "the late admin holds the accepted Build's topology: {held}");

    // 5. A held birth that falls silent is not drift. The hold is a hang (every task but the leader in
    // a tracer's stop), which the provider reads `running`, so node investigation's tickle reaches it
    // once released; a SIGSTOP reads `os-stopped`, which re-creates it (node-investigation.md, Q273).
    let held_pid = estate.pid_of("mesh1.rpc.3").await as u32;
    let held_rt = rdm_test::process_faults::ExactRuntime { deployment_id: String::new(), control_domain: rdm_test::process_faults::local_control_domain(), pid: held_pid, start: rdm_test::process_faults::start_token(held_pid).expect("mesh1.rpc.3 has a start token") };
    let quiet_from = now_ns();
    let hung = held_rt.hang().expect("the hang is acknowledged by the OS");
    estate.until_node("mesh1.rpc.3", "mesh1.rpc.3 is silent in the view", rdm_transport::topology::staleness_floor() + Duration::from_secs(15), |n| n.filter(|n| matches!(n.status, NodeStatus::Dead | NodeStatus::PendingReconnect)).map(|_| ())).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    hung.release();
    let quiet_until = now_ns();
    estate.until_node("mesh1.rpc.3", "mesh1.rpc.3 is heard again", Duration::from_secs(20), |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;

    // 6. An admin that misses a Build catches up to the current Build when it is back.
    let quiet = estate.node_values().await.into_iter().find(|n| n["kind"] == "node_admin" && n["name"] != "mesh1.admin.1" && n["is_primary"] == false && n["is_fabric_primary"] == false).expect("a mesh1 admin that holds no primary role");
    let quiet_name = quiet["name"].as_str().unwrap().to_string();
    let stopped = estate.pid_of(&quiet_name).await;
    // Stopped across the acceptance only, never past the staleness floor: the admin misses the Build's
    // acceptance and is never silent, so no investigation looks at it. The build.create door answers once
    // the acceptance is written, so the admin is back as soon as the door answers. A stop held on to the
    // Build's later progress outlasts the floor on a slow host, and then the stopped birth is node
    // investigation's (node-investigation.md: an `os-stopped` verdict re-creates it at its path.name,
    // and the fabric-primary terminates the birth that comes back, an attempt of the fabric lane).
    signal(stopped, "-STOP");
    let r4_stream = estate.submit(&BuildSpec::Fabric(fabric_of(&[("mesh1", 3, 4)]))).await;
    let r4 = r4_stream.build_id().to_string();
    assert_eq!(current_build(&estate).await, r4);
    let resumed_at = now_ns();
    signal(stopped, "-CONT");
    // The Build is Complete only once its ready state is set (Q154), and a fabric state is set
    // only when every member has checked in, the admin that was stopped too.
    tokio::time::timeout(Duration::from_secs(90), estate.follow(r4_stream)).await.unwrap_or_else(|_| panic!("{r4} reached no Complete within 90s of {quiet_name} being back"));
    let quiet_carrier = estate.carrier_at(&quiet_name.parse().unwrap()).await;
    Fabrics::new(quiet_carrier.clone()).fact(&format!("the paused admin names {r4} its current Build"), Duration::from_secs(30), |f| f.build_id.as_deref() == Some(r4.as_str())).await.unwrap_or_else(|end| panic!("{end}"));
    build_log::build_fact(&quiet_carrier, &r4, "the paused admin's log holds the Build", Duration::from_secs(30), |_| true).await.unwrap_or_else(|end| panic!("{end}"));
    estate.settled_views(&[("mesh1", 3, 4)]).await;

    // 7. A killed member is its mesh-primary's node recovery: the next attempt of mesh1's lane of the
    // same Build, once.
    let before = build_log::build_value(&estate.carrier().await, &r4).await;
    let attempts_before = before["attempt"].as_u64().unwrap();
    let lane_before = before["lanes"]["mesh1"]["attempt"].as_u64().unwrap_or(0);
    let lost = estate.node_value("mesh1.rpc.2").await;
    let mesh_primary = estate.node_values().await.into_iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh1's primary")["name"].as_str().unwrap().to_string();
    estate.kill_node("mesh1.rpc.2").await;
    let back = estate
        .until_node("mesh1.rpc.2", "mesh1.rpc.2 is recovered", crate::node_recovery_due(), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["incarnation_id"] != lost["incarnation_id"]).then_some(n)
        })
        .await;
    assert_eq!(current_build(&estate).await, r4, "recovery writes no Build");
    // Exactly the next attempt of mesh1's, complete: a later one is a second recovery, named.
    let expected = lane_before + 1;
    build_log::build_fact(&estate.carrier().await, &r4, &format!("attempt {expected} of mesh1 complete, exactly"), Duration::from_secs(120), |b| {
        let Some(m) = b.lanes.get("mesh1") else { return false };
        assert!(u64::from(m.attempt) <= expected, "mesh1 of Build {r4} is past attempt {expected}: a second recovery was opened: {b:#?}");
        match m.state {
            rdm_admin::build_state::BuildState::Complete => u64::from(m.attempt) == expected,
            rdm_admin::build_state::BuildState::Failed => panic!("attempt {} of mesh1 of Build {r4} failed: {b:#?}", m.attempt),
            _ => false,
        }
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    estate.settled_views(&[("mesh1", 3, 4)]).await;
    let after = build_log::build_value(&estate.carrier().await, &r4).await;
    let lane = &after["lanes"]["mesh1"];
    assert_eq!((lane["state"].as_str(), lane["attempt"].as_u64()), (Some("complete"), Some(expected)), "one more attempt of mesh1's lane of the same Build: {after}");
    assert_eq!(lane["reason"], "recovery", "the lane's attempt is the recovery: {after}");
    assert_eq!((after["state"].as_str(), after["attempt"].as_u64()), (Some("complete"), Some(attempts_before)), "the fabric lane gained no attempt: {after}");
    assert_ne!(back["node_id"], lost["node_id"], "a lost birth's path gets a new logical node");
    assert_eq!(back["endpoint_id"], lost["endpoint_id"], "over the inherited data dir and its endpoint key");

    // 8. A stop, then a start, each an attempt of the accepted Build, under the same id.
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let restarted_from = estate.node_value("mesh1.rpc.1").await;
    let (r, started) = estate.stop_then_start(&"mesh1.rpc.1".parse().unwrap()).await;
    assert_eq!(r.build_id.to_string(), r4, "a stop opens an attempt of the accepted Build: {r:?}");
    assert_eq!(u64::from(r.attempt), attempts_before + 1, "the stop opens the fabric lane's next attempt (the recovery was mesh1's lane's): {r:?}");
    assert!(started.build_id.to_string() == r4 && started.attempt > r.attempt, "the start opens a later attempt of the same Build: {started:?} after {r:?}");
    estate
        .until_node("mesh1.rpc.1", "mesh1.rpc.1 is ready again as the same birth", Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["stopped"] == false && n["incarnation_id"] == restarted_from["incarnation_id"] && n["node_id"] == restarted_from["node_id"]).then_some(())
        })
        .await;
    assert_eq!(current_build(&estate).await, r4);

    estate.stop().await;
    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap();
    let drift = named(&spans, "rdm.node_admin.build.update.via-proven-drift");
    // 5: nothing while the stopped birth was only silent.
    assert!(drift.iter().all(|sp| at(sp) < quiet_from || at(sp) > quiet_until), "a silent, live birth opened a repair: {drift:?}");
    // 7: the fabric-primary opens no drift attempt for a member: its mesh-primary recovers it.
    assert!(drift.is_empty(), "a killed member is node recovery, never the fabric-primary's drift: {drift:?}");
    let attempt = (lane_before + 1).to_string();
    let submit = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == "mesh1.rpc.2" && sp["attributes"]["node_id"] == lost["node_id"])
        .unwrap_or_else(|| panic!("mesh1's primary submitted the recovery of the killed birth"));
    let sa = &submit["attributes"];
    assert_eq!(sa["submitter"], mesh_primary.as_str(), "the member's own mesh-primary submits: {submit}");
    assert_eq!(sa["incarnation_id"], lost["incarnation_id"], "keyed on the exact exited birth: {submit}");
    assert_eq!((sa["build_id"].as_str(), sa["mesh"].as_str(), sa["attempt"].as_str()), (Some(r4.as_str()), Some("mesh1"), Some(attempt.as_str())), "the recovery is the next attempt of mesh1's lane of the accepted Build: {submit}");
    assert_eq!(sa["outcome"], format!("complete: attempt {attempt}"), "the submit followed the recovery to its terminal: {submit}");
    // The mesh-primary's own door admitted it (R-BI7: it opens the recovery attempt itself, whoever
    // holds the fabric-primary role), in process, under the submit.
    let opened = named(&spans, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["attempt"] == attempt.as_str() && sp["attributes"]["node"] == "mesh1.rpc.2")
        .unwrap_or_else(|| panic!("the mesh-primary's door opened the recovery attempt of {r4}"));
    assert_eq!((opened["attributes"]["outcome"].as_str(), opened["attributes"]["route"].as_str()), (Some("admitted"), Some("node-recovery")), "{opened}");
    assert_eq!(opened["parent_span_id"], submit["span_id"], "the door's span is the submit's child: {opened}");
    let ran = named(&spans, "rdm.node_admin.build.update.via-reconcile");
    let repaired = ran
        .iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["lane"] == "mesh1" && sp["attributes"]["reason"] == "recovery" && sp["attributes"]["outcome"] == "converged")
        .unwrap_or_else(|| panic!("the recovery attempt of mesh1's lane of {r4} converged: {ran:?}"));
    assert_eq!(repaired["attributes"]["attempt"], attempt.as_str());
    assert_eq!(repaired["attributes"]["operations"], "node.delete:mesh1.rpc.2,node.create:mesh1.rpc.2", "the recovery's two operations, in order: {repaired}");
    // The mesh-primary runs the attempt it opened under the submit: the recovery's reconcile is the
    // submit's child, in its trace.
    assert_eq!(repaired["parent_span_id"], submit["span_id"], "the recovery's reconcile runs under the submit that opened it: {repaired}");
    assert_eq!(repaired["trace_id"], submit["trace_id"]);
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == lost["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], lost["incarnation_id"], "named by the exact exited birth");
    assert!(
        named(&spans, "rdm.node_admin.node.create.via-build").iter().any(|sp| sp["trace_id"] == submit["trace_id"] && sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["attempt"] == attempt.as_str() && sp["attributes"]["node"] == "mesh1.rpc.2"),
        "the recovery created mesh1.rpc.2 under the same Build"
    );
    estate.record_trace_url(submit["trace_id"].as_str().unwrap_or(""));
    // 8: the stop ran as an attempt of the same Build.
    let restart = ran
        .iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["reason"] == "stop" && sp["attributes"]["outcome"] == "converged")
        .unwrap_or_else(|| panic!("the stop attempt of {r4} converged: {ran:?}"));
    // The stop attempt's context is the stop call's request span, not the span that accepted the Build.
    let rest = named(&spans, "rdm.node_admin.build.update.via-node-rpc")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["node"] == "mesh1.rpc.1" && sp["attributes"]["attempt"] == r.attempt.to_string().as_str())
        .expect("the stop call's request span");
    assert_eq!(restart["parent_span_id"], rest["span_id"], "the stop's reconcile is the child of the stop call's request span: {restart}");
    assert_eq!(restart["trace_id"], rest["trace_id"]);
    // 4 and 6: the fabric-primary writes each Build once; the late admin committed Ready holding the
    // current Build, and the paused admin caught up to the Build written while it was stopped.
    let moved = named(&spans, "rdm.node_admin.build.create.via-accepted");
    let late_ready = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == "mesh1.admin.3" && sp["attributes"]["incarnation_id"] == late["incarnation_id"])
        .cloned()
        .expect("the late admin is ready");
    assert_eq!(late_ready["attributes"]["build_id"], c.as_str(), "the late admin held the current Build when it committed Ready: {late_ready}");
    assert!(
        moved.iter().any(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["build_id"] == c.as_str() && at(sp) < at(&late_ready)),
        "the fabric-primary wrote {c} before the late admin was ready: {moved:?}"
    );
    assert!(
        moved.iter().any(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["build_id"] == r4.as_str() && at(sp) < resumed_at),
        "the fabric-primary wrote {r4} while {quiet_name} was stopped: {moved:?}"
    );
    assert!(named(&spans, "rdm.node_admin.build.create.via-proven-drift").is_empty(), "no Build is minted for drift");
}
