//! i143.e4.s16 process E2E: a successor manages births it never launched
//! (rafka-v2 #2850).
//!
//! Build A completes (mesh1.admin.1 launches three rpc nodes). Only then is
//! mesh1.admin.2 born, so it never holds A's receipts: the Build topic hands
//! a new admin only active Builds. The launching admin is killed. From public
//! surfaces only:
//! - the successor holds the mesh and the fabric, and its view advertises
//!   every rpc node's data dir (current birth metadata, from the topology view);
//! - it restarts one of A's births (same node id, new incarnation) and
//!   deletes another (its runtime is gone), from the runtime each birth
//!   publishes with its topology view, never from Build history.
//!
//! Evidence: the bootstrap admin's ready span names its own self-adopted
//! runtime (day 0); each rpc node's ready span names the runtime its provider
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

use rdm_client::{LaunchKind as NodeKind, NodeStatus};
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
    // Build A: the bootstrap admin launches three rpc nodes; A completes.
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 3)]).await;
    let a_births = estate.settles_to(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    // Only now is the successor born: A is no longer active.
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let nodes = estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    // The initial fabric refuses a node restart and a node delete until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let successor = nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").cloned().unwrap();
    let launcher = nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    let rpc = |name: &str| a_births.iter().find(|n| n.name.to_string() == name).cloned().unwrap();
    let (restarted, deleted) = (rpc("mesh1.rpc.2"), rpc("mesh1.rpc.3"));
    let deleted_pid = estate.pid_of("mesh1.rpc.3").await;

    // Stop the launching admin (`node.stop` runs `node.primary.update` to `mesh1.admin.2` first and
    // takes `mesh1.admin.1` down only after the confirmed transfer).
    estate.enter(&successor.name).await;
    let mut stop = estate.nodes_obj().await.stop(&launcher.name).await.expect("node.stop of mesh1.admin.1");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.kill_bootstrap();
    let view = estate
        .until_view("the successor holds the mesh and the fabric", Duration::from_secs(30), |v| {
            let me = v.iter().find(|n| n.name == successor.name)?;
            (me.is_primary && me.is_fabric_primary).then(|| v.to_vec())
        })
        .await;
    for n in view.iter().filter(|n| n.kind == NodeKind::RpcNode) {
        assert!(n.data_dir.as_deref().is_some_and(|d| !d.is_empty()), "the successor's view carries {}'s data dir: {n:?}", n.name);
    }

    // Restart one of A's births and delete another, through the successor.
    estate.stop_then_start(&restarted.name).await;
    // A stop then a start keep the birth (node-stop.md "Kept": "node.id, incarnation, endpoint key, port,
    // bin, path/name and storage"): the node is ready again as the same birth.
    let after = estate.until_node("mesh1.rpc.2", "mesh1.rpc.2 ready again as the same birth", Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Ready).cloned()).await;
    assert_eq!((&after.node_id, &after.incarnation_id), (&restarted.node_id, &restarted.incarnation_id), "a restart keeps the node id and the birth");
    Estate::workflow("node.delete of mesh1.rpc.3", estate.nodes_obj().await.delete(&deleted.name).await).await;
    wait_for("mesh1.rpc.3's runtime is gone", Duration::from_secs(30), || async { (!alive(deleted_pid)).then_some(()) }).await;

    // The admins that hold mesh1 now: the successor, and the drift recovery's rebirth of the
    // killed launcher's path, a new NodeId that may take the seat back by NodeId order.
    let admins_now: Vec<String> = estate.node_views().await.iter().filter(|n| n.kind == NodeKind::NodeAdmin && !matches!(n.status, NodeStatus::Dead | NodeStatus::PendingReconnect)).map(|n| n.node_id.to_string()).collect();
    let (launcher, restarted, deleted) = (serde_json::to_value(&launcher).unwrap(), serde_json::to_value(&restarted).unwrap(), serde_json::to_value(&deleted).unwrap());
    estate.stop().await;
    let spans = estate.spans();
    // Day 0: the bootstrap admin published the runtime it adopted for itself.
    let boot = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node"] == "mesh1.admin.1").cloned().expect("the bootstrap admin is ready");
    assert_eq!(boot["attributes"]["runtime_locator_kind"], if estate.owner.provider == "container" { "container-id" } else { "process-pid-start" });
    assert_eq!(boot["attributes"]["source"], "self-published-topology");
    let ns = |sp: &Value, k: &str| sp[k].as_u64().unwrap_or_else(|| panic!("{k} on {sp}"));
    let steps_of = |node: &str, pipeline_step: &dyn Fn(&Value) -> bool| -> Vec<Value> {
        let mut v: Vec<Value> = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .filter(|sp| sp["attributes"]["node"] == node && pipeline_step(sp))
            .cloned()
            .collect();
        v.sort_by_key(|sp| ns(sp, "start_unix_nano"));
        v
    };
    // Day 0: the adoption, step by step, before Ready.
    let day0 = steps_of("mesh1.admin.1", &|sp| sp["attributes"]["build_id"] == "");
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
    // The killed bootstrap admin is desired (2 node-admins): once its exact
    // runtime is proven exited, the successor may recreate it under a new
    // proven-drift Build before the test stops.
    let recoveries = named(&spans, "rdm.node_admin.build.create.via-proven-drift");
    assert!(recoveries.len() <= 1, "one recovery at most for one exited birth");
    for r in &recoveries {
        assert_eq!(r["attributes"]["scope"], "mesh1.node_admin: 1 present of 2 desired (exited: mesh1.admin.1)");
    }
    // A restart, node.stop then node.start, deploys nothing (node-start.md: the same process rejoins):
    // its start's WaitForNodeReady follows StartNode and none of the launch steps. Every other wait is a
    // launch.
    let steps_before = |wait: &Value| -> Vec<Value> {
        let (node, build) = (wait["attributes"]["node"].as_str().unwrap(), wait["attributes"]["build_id"].clone());
        steps_of(node, &|sp| sp["attributes"]["build_id"] == build && ns(sp, "end_unix_nano") <= ns(wait, "start_unix_nano"))
    };
    let (restarts, launches): (Vec<&Value>, Vec<&Value>) =
        launched.iter().filter(|w| w["attributes"]["node"] != "mesh1.admin.1").partition(|w| steps_before(w).iter().any(|sp| sp["attributes"]["step"] == "StartNode"));
    let restarted_nodes: Vec<&str> = restarts.iter().map(|w| w["attributes"]["node"].as_str().unwrap()).collect();
    assert_eq!(restarted_nodes, ["mesh1.rpc.2"], "mesh1.rpc.2's restart waited for Ready, and no other node was restarted");
    for node in ["mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"] {
        assert!(launches.iter().any(|w| w["attributes"]["node"] == node), "{node} was launched and waited for Ready");
    }
    for wait in &restarts {
        let node = wait["attributes"]["node"].as_str().unwrap();
        let before = steps_before(wait);
        assert!(!before.iter().any(|sp| PREREQUISITES.contains(&sp["attributes"]["step"].as_str().unwrap()) || sp["attributes"]["step"] == "DeployRuntime"), "{node}: a restart deploys nothing: {before:?}");
    }
    for wait in launches {
        let node = wait["attributes"]["node"].as_str().unwrap();
        let before = steps_before(wait);
        let names: Vec<&str> = before.iter().map(|sp| sp["attributes"]["step"].as_str().unwrap()).filter(|s| PREREQUISITES.contains(s)).collect();
        assert_eq!(names, PREREQUISITES, "{node}: every prerequisite committed, in order, before WaitForNodeReady");
        assert!(before.iter().filter(|sp| PREREQUISITES.contains(&sp["attributes"]["step"].as_str().unwrap())).all(|sp| sp["attributes"]["outcome"] == "complete"));
    }
    // The rebirth of the killed launcher's path checks the birth its view holds there first
    // (`via-path-investigation`): an unreached one ends in the provider's inspection, whose span is the
    // evidence (node-runtime-inspect.md "Observability contract"), in the same trace, naming the
    // investigated birth and the same verdict.
    let checked: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.delete.via-path-investigation").into_iter().filter(|sp| sp["attributes"]["outcome"] == "unreached").collect();
    assert!(!checked.is_empty(), "the rebirth of the killed launcher's path investigates the birth its view holds there");
    for check in checked {
        let inspected = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection")
            .into_iter()
            .find(|sp| sp["trace_id"] == check["trace_id"] && sp["attributes"]["incarnation_id"] == check["attributes"]["incarnation"] && sp["attributes"]["caller"].as_str().unwrap_or("").is_empty())
            .unwrap_or_else(|| panic!("the path check's investigation inspects the exact runtime under its own span: {check}"));
        assert_eq!(inspected["attributes"]["verdict"], check["attributes"]["verdict"], "the inspection's verdict is the investigation's: {inspected} {check}");
        assert!(!s(&inspected["attributes"]["inspector"]).is_empty(), "the inspection names its inspector: {inspected}");
    }
    for birth in [&restarted, &deleted] {
        let name = s(&birth["name"]);
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == name.as_str() && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .cloned()
            .unwrap_or_else(|| panic!("{name} reports ready"));
        let adopted = named(&spans, "rdm.node_admin.runtime.update.via-adopt")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == name.as_str() && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .cloned()
            .unwrap_or_else(|| panic!("the successor adopted {name}'s runtime"));
        let (r, a) = (&ready["attributes"], &adopted["attributes"]);
        // Adopted by an admin that never launched it: the successor, or the launcher path's rebirth
        // when it holds the seat by then. Never the killed launcher.
        assert_ne!(a["adopter_node_id"], launcher["node_id"], "{name}: the killed launcher never adopts");
        assert!(admins_now.contains(&s(&a["adopter_node_id"])), "{name}: adopted by a current admin {admins_now:?}: {a}");
        assert_eq!(a["execution_node_id"], a["adopter_node_id"], "{name}: the adopter executes");
        assert_eq!(a["source"], "self-published-topology");
        for k in ["deployment_id", "provider", "provider_control_domain_fingerprint", "runtime_locator_kind", "runtime_locator_fingerprint"] {
            assert_eq!(a[k], r[k], "{name}: the adopted runtime is the one it published ({k})");
        }
    }
}
