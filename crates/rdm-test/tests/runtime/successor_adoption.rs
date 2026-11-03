//! i143.e4.s16 process E2E: a successor manages births it never launched
//! (rafka-v2 #2850).
//!
//! Build A completes (the bootstrap admin launches three nodes). Only then is
//! the successor admin born, so it never holds A's receipts: the Build topic hands
//! a new admin only active Builds. The launching admin is killed. From public
//! surfaces only:
//! - the successor holds the mesh and the fabric, and its view advertises
//!   every node's data dir (current birth metadata, from the topology view);
//! - it restarts one of A's births (same node id and incarnation) and
//!   deletes another (its runtime is gone), from the runtime each birth
//!   publishes with its topology view, never from Build history.
//!
//! Evidence: the bootstrap admin's ready span names its own self-adopted
//! runtime (day 0); each node's ready span names the runtime its provider
//! recorded; the successor's `rdm.node_admin.runtime.update.via-adopt`
//! names the same locator fingerprint, `source = self-published-topology`,
//! and the successor as adopter and executor.
//!
//! And each prerequisite is its own committed step before Ready (PRD §5.3):
//! the Day-0 admin's `AdoptCurrentRuntime`, `RegisterExactRuntimeHandle`,
//! `ResolveProviderControlDomain`, `PublishRuntimeFactAndCurrentRuntimeMetadata`
//! all complete, in order, before its ready span; every launched birth's
//! `RegisterExactRuntimeHandle`, `ResolveProviderControlDomain`,
//! `MakeRuntimeFactAvailableToBirth`,
//! `PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata` complete, in
//! order, before its `WaitForNodeReady` step begins.

use rdm_client::{NodeStatus, PathName};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-runtime".into(),
        subfeature: "successor-adoption".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_successor_manages_births_it_never_launched".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_manages_births_it_never_launched() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    // Build A: the bootstrap admin launches three nodes; A completes.
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 3)]).await;
    let a_births = estate.settles_to(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    // Only now is the successor born: A is no longer active.
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let nodes = estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    // The initial fabric refuses a node restart and a node delete until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let successor = nodes.iter().find(|n| n.location() == PathName::new("mesh1.node-admin", "2").unwrap()).cloned().unwrap();
    let launcher = nodes.iter().find(|n| n.location() == PathName::new("mesh1.node-admin", "1").unwrap()).cloned().unwrap();
    let rpc = |name: &str| a_births.iter().find(|n| n.location() == PathName::new("mesh1.node", name).unwrap()).cloned().unwrap();
    let (restarted, deleted) = (rpc("2"), rpc("3"));
    let deleted_pid = estate.pid_of(&rdm_core::PathName::new("mesh1.node", "3").unwrap()).await;

    // Stop the launching admin (`node.stop` runs `node.primary.update` to `mesh1.node-admin.2` first and
    // takes `mesh1.node-admin.1` down only after the confirmed transfer).
    estate.enter(&successor.location()).await;
    // A stop is a node command, never a Build (Q342): its stream ends with the stop's own completion.
    let mut stop = estate.nodes_obj().await.stop(&launcher.location()).await.expect("node.stop of mesh1.node-admin.1");
    let frames = Estate::frames_of(&mut stop).await;
    assert_eq!(frames.last(), Some(&rdm_client::Frame::Complete), "node.stop of mesh1.node-admin.1 completes: {frames:?}");
    estate.kill_bootstrap();
    let view = estate
        .until_view("the successor holds the mesh and the fabric", Duration::from_secs(30), |v| {
            let me = v.iter().find(|n| n.node_id == successor.node_id)?;
            (me.is_primary && me.is_fabric_primary).then(|| v.to_vec())
        })
        .await;
    for n in view.iter().filter(|n| n.r#type == "node") {
        assert!(n.data_dir.as_deref().is_some_and(|d| !d.is_empty()), "the successor's view carries {:?}'s data dir: {n:?}", n.name);
    }

    // Restart one of A's births and delete another, through the successor.
    estate.stop_then_start(&restarted.location()).await;
    // A stop then a start keep the birth (node-stop.md "Kept": "node.id, incarnation, endpoint key, port,
    // bin, path/name and storage"): the node is ready again as the same birth.
    let after = estate.until_node(&rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1.node.2 ready again as the same birth", Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Ready).cloned()).await;
    assert_eq!((&after.node_id, &after.incarnation_id), (&restarted.node_id, &restarted.incarnation_id), "a restart keeps the node id and the birth");
    Estate::workflow("node.delete of mesh1.node.3", estate.nodes_obj().await.delete(&deleted.location()).await).await;
    wait_for("mesh1.node.3's runtime is gone", Duration::from_secs(30), || async { (!alive(deleted_pid)).then_some(()) }).await;

    // The admins that hold mesh1 now: the successor, and the drift recovery's rebirth of the
    // killed launcher's path, a new NodeId that may take the role back by NodeId order.
    let admins_now: Vec<String> = estate.node_views().await.iter().filter(|n| n.r#type == "node-admin" && !matches!(n.status, NodeStatus::Dead | NodeStatus::PendingReconnect)).map(|n| n.node_id.to_string()).collect();
    let (launcher, restarted, deleted) = (serde_json::to_value(&launcher).unwrap(), serde_json::to_value(&restarted).unwrap(), serde_json::to_value(&deleted).unwrap());
    estate.stop().await;
    let spans = estate.spans();
    // Day 0: the bootstrap admin published the runtime it adopted for itself.
    let boot = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node_id"] == launcher["node_id"] && sp["attributes"]["incarnation_id"] == launcher["incarnation_id"]).cloned().expect("the bootstrap admin is ready");
    assert_eq!(boot["attributes"]["runtime_locator_kind"], if estate.owner.provider == "container" { "container-id" } else { "process-pid-start" });
    assert_eq!(boot["attributes"]["source"], "self-published-topology");
    let ns = |sp: &Value, k: &str| sp[k].as_u64().unwrap_or_else(|| panic!("{k} on {sp}"));
    let steps_of = |node: &PathName, pipeline_step: &dyn Fn(&Value) -> bool| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .filter(|sp| sp["attributes"]["node_path"] == node.path && sp["attributes"]["node_name"] == node.name && pipeline_step(sp))
            .cloned()
            .collect();
        v.sort_by_key(|sp| ns(sp, "start_unix_nano"));
        v
    };
    // Day 0: the adoption, step by step, before Ready.
    // Its steps of no Build that began before its Ready: the later stop of the launcher is a node command,
    // which belongs to no Build either (Q342).
    let day0 = steps_of(&PathName::new("mesh1.node-admin", "1").unwrap(), &|sp| sp["attributes"]["build_id"] == "" && sp["start_unix_nano"].as_u64().unwrap_or(u64::MAX) <= boot["start_unix_nano"].as_u64().unwrap_or(0));
    let names: Vec<&str> = day0.iter().map(|sp| sp["attributes"]["step"].as_str().unwrap()).collect();
    assert_eq!(names, ["AdoptCurrentRuntime", "RegisterExactRuntimeHandle", "ResolveProviderControlDomain", "PublishRuntimeFactAndCurrentRuntimeMetadata"]);
    for sp in &day0 {
        assert_eq!(sp["attributes"]["outcome"], "complete", "{sp}");
        assert!(ns(sp, "end_unix_nano") <= ns(&boot, "start_unix_nano"), "{} committed before the bootstrap admin's Ready", sp["attributes"]["step"]);
    }
    // Every launched birth: the four prerequisites, in order, before
    // WaitForNodeReady begins.
    const PREREQUISITES: [&str; 4] =
        ["RegisterExactRuntimeHandle", "ResolveProviderControlDomain", "MakeRuntimeFactAvailableToBirth", "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata"];
    let launched: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "WaitForNodeReady").collect();
    // The killed bootstrap admin is desired (2 node-admins): once its exact runtime is proven exited, the
    // successor, its mesh's primary, may recover it as its own workflow before the test stops, never a
    // Build (Q342): one completed recovery at most for one exited birth.
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == launcher["node_id"] && sp["attributes"]["outcome"] == "complete").collect();
    assert!(recoveries.len() <= 1, "one recovery at most for one exited birth: {recoveries:#?}");
    // A restart, node.stop then node.start, deploys nothing (node-start.md: the same process rejoins):
    // its start's WaitForNodeReady follows StartNode and none of the launch steps. Every other wait is a
    // launch.
    let location_of = |sp: &Value| PathName::new(sp["attributes"]["node_path"].as_str().expect("step path"), sp["attributes"]["node_name"].as_str().expect("step name")).unwrap();
    let steps_before = |wait: &Value| -> Vec<Value> {
        let (node, build) = (location_of(wait), wait["attributes"]["build_id"].clone());
        steps_of(&node, &|sp| sp["attributes"]["build_id"] == build && sp["attributes"]["attempt"] == wait["attributes"]["attempt"] && sp["trace_id"] == wait["trace_id"] && sp["parent_span_id"] == wait["parent_span_id"] && ns(sp, "end_unix_nano") <= ns(wait, "start_unix_nano"))
    };
    let (restarts, launches): (Vec<&Value>, Vec<&Value>) =
        launched.iter().filter(|w| location_of(w) != PathName::new("mesh1.node-admin", "1").unwrap()).partition(|w| steps_before(w).iter().any(|sp| sp["attributes"]["step"] == "StartNode"));
    let restarted_nodes: Vec<PathName> = restarts.iter().map(|w| location_of(w)).collect();
    assert_eq!(restarted_nodes, [PathName::new("mesh1.node", "2").unwrap()], "only the restarted fixture waited for Ready after StartNode");
    for node in ["1", "2", "3"].map(|name| PathName::new("mesh1.node", name).unwrap()) {
        assert!(launches.iter().any(|w| location_of(w) == node), "{node:?} was launched and waited for Ready");
    }
    for wait in &restarts {
        let node = location_of(wait);
        let before = steps_before(wait);
        assert!(!before.iter().any(|sp| PREREQUISITES.contains(&sp["attributes"]["step"].as_str().unwrap()) || sp["attributes"]["step"] == "DeployRuntime"), "{node:?}: a restart deploys nothing: {before:?}");
    }
    for wait in launches {
        let node = location_of(wait);
        let before = steps_before(wait);
        let names: Vec<&str> = before.iter().map(|sp| sp["attributes"]["step"].as_str().unwrap()).filter(|s| PREREQUISITES.contains(s)).collect();
        assert_eq!(names, PREREQUISITES, "{node:?}: every prerequisite committed, in order, before WaitForNodeReady");
        assert!(before.iter().filter(|sp| PREREQUISITES.contains(&sp["attributes"]["step"].as_str().unwrap())).all(|sp| sp["attributes"]["outcome"] == "complete"));
    }
    // The rebirth of the killed launcher's path checks the birth its view holds there first
    // (`via-path-investigation`): an unreached one ends in the provider's inspection, whose span is the
    // evidence (node-runtime-inspect.md "Observability contract"), in the same trace, naming the
    // investigated birth and the same verdict.
    let checked: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.delete.via-path-investigation").into_iter().filter(|sp| sp["attributes"]["outcome"] == "unreached" && sp["attributes"]["node_id"] == launcher["node_id"] && sp["attributes"]["incarnation"] == launcher["incarnation_id"]).collect();
    assert!(!checked.is_empty(), "the rebirth of the killed launcher's path investigates the birth its view holds there");
    for check in checked {
        let inspected = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection")
            .into_iter()
            .find(|sp| sp["trace_id"] == check["trace_id"] && sp["attributes"]["node_id"] == check["attributes"]["node_id"] && sp["attributes"]["incarnation_id"] == check["attributes"]["incarnation"] && sp["attributes"]["caller"].as_str().unwrap_or("").is_empty())
            .unwrap_or_else(|| panic!("the path check's investigation inspects the exact runtime under its own span: {check}"));
        assert_eq!(inspected["attributes"]["verdict"], check["attributes"]["verdict"], "the inspection's verdict is the investigation's: {inspected} {check}");
        assert!(!s(&inspected["attributes"]["inspector"]).is_empty(), "the inspection names its inspector: {inspected}");
    }
    for birth in [&restarted, &deleted] {
        let name = s(&birth["node_id"]);
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == name.as_str() && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .cloned()
            .unwrap_or_else(|| panic!("{name} reports ready"));
        let adopted = named(&spans, "rdm.node_admin.runtime.update.via-adopt")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == name.as_str() && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .cloned()
            .unwrap_or_else(|| panic!("the successor adopted {name}'s runtime"));
        let (r, a) = (&ready["attributes"], &adopted["attributes"]);
        // Adopted by an admin that never launched it: the successor, or the launcher path's rebirth
        // when it holds the role by then. Never the killed launcher.
        assert_ne!(a["adopter_node_id"], launcher["node_id"], "{name}: the killed launcher never adopts");
        assert!(admins_now.contains(&s(&a["adopter_node_id"])), "{name}: adopted by a current admin {admins_now:?}: {a}");
        assert_eq!(a["execution_node_id"], a["adopter_node_id"], "{name}: the adopter executes");
        assert_eq!(a["source"], "self-published-topology");
        for k in ["deployment_id", "provider", "provider_control_domain_fingerprint", "runtime_locator_kind", "runtime_locator_fingerprint"] {
            assert_eq!(a[k], r[k], "{name}: the adopted runtime is the one it published ({k})");
        }
    }
}
