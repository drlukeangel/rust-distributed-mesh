//! Chaos-kit proofs of concept: the smallest real use of each fault piece, one cell per piece,
//! so the chaos run starts from pieces known to work. Process provider; telemetry on through
//! `OTEL_EXPORTER_OTLP_ENDPOINT`.

use rdm_core::NodeKind;
use rdm_client::{FabricDesired, MeshDesired, NodeStatus};
use rdm_test::typed::BuildSpec;
use rdm_core::build::BuildPhase;
use rdm_test::estate::{Estate, Owner};
use rdm_test::netfault::{udp_ports, Partition};
use rdm_test::process_faults::{ExactRuntime, Fault, Refusal};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "chaos-kit".into(),
        subfeature: "poc".into(),
        rung: "multi-node".into(),
        provider: "process".into(),
        test: test.into(),
    }
}

fn ready(n: &rdm_client::NodeView) -> bool {
    n.status == NodeStatus::Ready
}

/// Bootstraps mesh1 and builds 1 more node-admin and 2 rpc nodes, all ready, on a fabric ready.
async fn small_estate(test: &str) -> Estate {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settles_to(&[("mesh1", 2, 2)], SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

/// CONTRACT: the process fault backend stops, continues and kills one EXACT runtime read from
/// the birth's published `runtime.json`, acknowledges each by the OS state it leaves, and refuses
/// a target whose start token no longer matches (a recycled pid) without signalling anything.
#[tokio::test(flavor = "multi_thread")]
async fn poc_process_faults_hold_release_kill_and_refuse_a_recycled_pid() {
    let mut estate = small_estate("poc_process_faults_hold_release_kill_and_refuse_a_recycled_pid").await;
    let dir = PathBuf::from(estate.data_dir("mesh1.rpc.2").await);
    let rt = ExactRuntime::published(&dir).expect("rpc.2 published its runtime");

    // A recycled pid is refused by name, and nothing is signalled.
    let wrong = rt.with_start(rt.start + 1);
    match wrong.apply(Fault::Stop) {
        Err(Refusal::NotThisRuntime { .. }) => {}
        other => panic!("a wrong start token must be refused as NotThisRuntime: {other:?}"),
    }

    let stopped = rt.apply(Fault::Stop).expect("stop applies");
    assert_eq!(stopped.state_after, Some('T'), "a stopped runtime is in state T: {stopped:?}");
    let resumed = rt.apply(Fault::Continue).expect("continue applies");
    assert_ne!(resumed.state_after, Some('T'), "a continued runtime runs again: {resumed:?}");
    let killed = rt.apply(Fault::Kill(estate.fabric_ready())).expect("kill applies");
    assert!(killed.exited, "a kill is acknowledged by the exit: {killed:?}");
    match rt.apply(Fault::Kill(estate.fabric_ready())) {
        Err(Refusal::AlreadyExited { .. }) | Err(Refusal::NotThisRuntime { .. }) => {}
        other => panic!("a second kill of a gone runtime is refused: {other:?}"),
    }
    estate.stop().await;
}

/// CONTRACT: a netfault cut drops UDP between one rpc node and the rest of the estate; while it
/// holds, the admin's view stops hearing the node; when the cut is dropped, the node is heard and
/// ready again with the same node id. Needs nft as root or through `sudo -n`.
#[tokio::test(flavor = "multi_thread")]
async fn poc_netfault_cut_unhears_one_node_and_heal_restores_it() {
    let mut estate = small_estate("poc_netfault_cut_unhears_one_node_and_heal_restores_it").await;
    let nodes = estate.node_values().await;
    let victim = "mesh1.rpc.2".to_string();
    let rest: Vec<String> = nodes.iter().filter_map(|n| n["name"].as_str()).filter(|n| *n != victim).map(String::from).collect();
    let id_before = estate.node_view(&victim).await.node_id;
    let cut = match Partition::start(&udp_ports(&nodes, &[victim.clone()]), &udp_ports(&nodes, &rest)) {
        Ok(c) => c,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").is_err() => {
            eprintln!("skip: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("netfault required: {why}"),
    };
    let held = estate.until_node(&victim, "the cut node is no longer ready in the admin's view", SETTLE, |n| n.filter(|n| !ready(n)).map(|n| n.status)).await;
    eprintln!("poc: under the cut {victim} reads {held:?}");
    drop(cut);
    let back = estate.until_node(&victim, "the healed node is ready again", SETTLE, |n| n.filter(|n| ready(n)).cloned()).await;
    assert_eq!(back.node_id, id_before, "the heal brings back the same node: {back:?}");
    estate.stop().await;
}

/// CONTRACT: the admin fault door holds a real node-admin at one named cut (the create of an rpc
/// node, after DeployRuntime's work and before its receipt is durable): the Build does not complete
/// while the cut holds, the door reports the held call, and on release the Build completes and the
/// node comes up ready. Needs `cargo build -p rafka-node-rpc-testkit --bins` (faulted-node-admin).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_admin_fault_door_holds_a_create_at_its_receipt_and_releases_it() {
    use rdm_test::faults::{binding_set, candidate_sha, Door};
    let mut o = owner("poc_admin_fault_door_holds_a_create_at_its_receipt_and_releases_it");
    o.rung = "fault-door".into();
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(o, "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("faulted-admin binding set accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 0)]).await;
    estate.settles_to(&[("mesh1", 2, 0)], Duration::from_secs(60)).await;

    // Either admin may execute: arm the same cut on both doors.
    let mut doors = Vec::new();
    for n in estate.node_views().await.iter().filter(|n| n.kind == NodeKind::NodeAdmin) {
        doors.push(Door::at(&estate, &n.name.to_string()).await);
    }
    let id = "poc:deploy-runtime";
    let spec = json!({"kind": "receipt", "step": "DeployRuntime", "operation": "node.create", "node": "mesh1.rpc.1"});
    for d in &doors {
        d.arm(id, spec.clone()).await;
    }
    let desired = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)])] };
    let mut stream = estate.submit(&BuildSpec::Fabric(desired)).await;
    let build = stream.build_id().to_string();
    // The door of whichever admin executes the create reports the held call.
    let holder = futures_lite::future::or(doors[0].wait_held(id), doors[1].wait_held(id)).await;
    eprintln!("poc: a door holds {holder}");
    let during = estate.carrier().await.build(stream.build_id()).await.unwrap_or_else(|e| panic!("build.get [id] of {build}: {e}"));
    assert_ne!(during.phase, BuildPhase::Complete, "the Build cannot complete while its step is held: {during:?}");
    for d in &doors {
        d.release(id).await;
    }
    Estate::until_complete(&mut stream).await;
    estate.until_node("mesh1.rpc.1", "rpc.1 ready after the release", SETTLE, |n| n.filter(|n| ready(n)).map(|_| ())).await;
    estate.stop().await;
}

/// CONTRACT: the probe drives real Node RPC by exact node id. A put/get round-trips through the
/// target, and `--cut-before-finish` cuts the call before its frame is finished, so it is refused
/// as `FrameNotSent` (the 499 reset) and never executed.
#[tokio::test(flavor = "multi_thread")]
async fn poc_probe_round_trips_and_a_cut_frame_is_not_sent() {
    let mut estate = small_estate("poc_probe_round_trips_and_a_cut_frame_is_not_sent").await;
    let id = estate.node_value("mesh1.rpc.1").await["node_id"].as_str().unwrap().to_string();
    let exact = format!("exact:{id}");
    let put = estate.probe(&["put", "--target", &exact, "--key", "7", "--value", "poc"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    let get = estate.probe(&["get", "--target", &exact, "--key", "7"]);
    eprintln!("poc: get -> {get}");
    let cut = estate.probe(&["put", "--target", &exact, "--key", "8", "--value", "never", "--cut-before-finish"]);
    assert_eq!(cut["reason"], "FrameNotSent", "{cut}");
    let missing = estate.probe(&["get", "--target", &exact, "--key", "8"]);
    eprintln!("poc: get of the cut key -> {missing}");
    estate.stop().await;
}

/// CONTRACT (R-P1): chaos may kill every node-admin; a person (here the cell) then starts ONE
/// node-admin on the former fabric primary's data dir with both recovery flags, and the fabric comes
/// back by itself: the same Build, the same MeshId, every admin restored, live members preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_rp1_kill_every_admin_and_one_restarted_admin_restores_the_fabric() {
    let mut estate = Estate::bootstrap(owner("poc_rp1_kill_every_admin_and_one_restarted_admin_restores_the_fabric"), "fabric1", "mesh1").await;
    let build = estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await.build_id.to_string();
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"].iter().map(|s| s.to_string()).collect();
    let before = estate.settles_to_named(&want, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let primary = before.iter().find(|n| n.is_fabric_primary).map(|n| n.name.clone()).unwrap();
    let primary_dir = if primary.to_string() == "mesh1.admin.1" { estate.bootstrap_data_dir("mesh1") } else { PathBuf::from(estate.data_dir(&primary.to_string()).await) };
    let rpc_id = estate.node_view("mesh1.rpc.1").await.node_id;
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy().contains(".admin.") {
            estate.kill_pid(pid);
        }
    }
    estate.kill_bootstrap();
    estate.restart_admin_with(&primary_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.enter(&primary).await;
    let after = estate.settles_to_named(&want, Duration::from_secs(240)).await;
    let fabric = estate.fabric_view().await;
    assert_eq!(fabric.build_id.as_deref(), Some(build.as_str()), "the same Build: {fabric:?}");
    let rpc_after = after.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap();
    assert_eq!(rpc_after.node_id, rpc_id, "the live member is preserved");
    eprintln!("poc: restarted {primary}; fabric {:?}", fabric.status);
    estate.stop().await;
}
