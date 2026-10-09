//! Chaos-kit proofs of concept: the smallest real use of each fault piece, one cell per piece,
//! so the chaos run starts from pieces known to work. Process provider; telemetry on through
//! `OTEL_EXPORTER_OTLP_ENDPOINT`.

use rafka_test_scenario::estate::{wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use rafka_test_scenario::process_faults::{ExactRuntime, Fault, Refusal};
use serde_json::{json, Value};
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

fn ready(n: &Value) -> bool {
    n["status"] == "ready-for-traffic"
}

/// Bootstraps mesh1 and builds 1 more node-admin and 2 rpc nodes, all ready.
async fn small_estate(test: &str) -> Estate {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]});
    let (status, accepted) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "POST /api/build: {accepted}");
    estate.await_build(accepted["build_id"].as_str().unwrap(), SETTLE).await;
    wait_for("2 admins + 2 rpc ready", SETTLE, || async {
        let nodes = estate.nodes().await;
        (nodes.iter().filter(|n| ready(n)).count() == 4).then_some(())
    })
    .await;
    estate
}

/// CONTRACT: the process fault backend stops, continues and kills one EXACT runtime read from
/// the birth's published `runtime.json`, acknowledges each by the OS state it leaves, and refuses
/// a target whose start token no longer matches (a recycled pid) without signalling anything.
#[tokio::test(flavor = "multi_thread")]
async fn poc_process_faults_hold_release_kill_and_refuse_a_recycled_pid() {
    let mut estate = small_estate("poc_process_faults_hold_release_kill_and_refuse_a_recycled_pid").await;
    let dir = PathBuf::from(estate.data_dir_of("mesh1.rpc.2").await);
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
    let killed = rt.apply(Fault::Kill).expect("kill applies");
    assert!(killed.exited, "a kill is acknowledged by the exit: {killed:?}");
    match rt.apply(Fault::Kill) {
        Err(Refusal::AlreadyExited { .. }) | Err(Refusal::NotThisRuntime { .. }) => {}
        other => panic!("a second kill of a gone runtime is refused: {other:?}"),
    }
    estate.stop().await;
}

/// CONTRACT: a netfault cut drops UDP between one rpc node and the rest of the estate; while it
/// holds, the admin's view stops hearing the node; when the cut is dropped, the node is heard and
/// ready again with the same node id. Needs iptables as root or through `sudo -n`.
#[tokio::test(flavor = "multi_thread")]
async fn poc_netfault_cut_unhears_one_node_and_heal_restores_it() {
    let mut estate = small_estate("poc_netfault_cut_unhears_one_node_and_heal_restores_it").await;
    let nodes = estate.nodes().await;
    let victim = "mesh1.rpc.2".to_string();
    let rest: Vec<String> = nodes.iter().filter_map(|n| n["name"].as_str()).filter(|n| *n != victim).map(String::from).collect();
    let id_before = estate.node(&victim).await["node_id"].clone();
    let cut = match Partition::start(&udp_ports(&nodes, &[victim.clone()]), &udp_ports(&nodes, &rest)) {
        Ok(c) => c,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").is_err() => {
            eprintln!("skip: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("netfault required: {why}"),
    };
    let held = wait_for("the cut node is no longer ready in the admin's view", SETTLE, || async {
        let n = estate.node_opt(&victim).await;
        n.filter(|n| !ready(n)).map(|n| n["status"].clone())
    })
    .await;
    eprintln!("poc: under the cut {victim} reads {held}");
    drop(cut);
    let back = wait_for("the healed node is ready again", SETTLE, || async { estate.node_opt(&victim).await.filter(ready) }).await;
    assert_eq!(back["node_id"], id_before, "the heal brings back the same node: {back}");
    estate.stop().await;
}

/// CONTRACT: the admin fault door holds a real node-admin at one named cut (the create of an rpc
/// node, after DeployRuntime's work and before its receipt is durable): the Build does not complete
/// while the cut holds, the door reports the held call, and on release the Build completes and the
/// node comes up ready. Needs `cargo build -p rafka-node-rpc-testkit --bins` (faulted-node-admin).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_admin_fault_door_holds_a_create_at_its_receipt_and_releases_it() {
    use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
    let mut o = owner("poc_admin_fault_door_holds_a_create_at_its_receipt_and_releases_it");
    o.rung = "fault-door".into();
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(o, "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("faulted-admin binding set accepted");
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), SETTLE).await;
    estate.settled(&["mesh1.admin.1", "mesh1.admin.2"].iter().map(|n| n.to_string()).collect(), Duration::from_secs(60)).await;

    // Either admin may execute: arm the same cut on both doors.
    let mut doors = Vec::new();
    for n in estate.nodes().await.iter().filter(|n| n["kind"] == "node_admin") {
        let name = n["name"].as_str().unwrap().to_string();
        let api = n["admin_api_base"].as_str().unwrap().to_string();
        doors.push(Door::open(&estate.root, &name, &api).await);
    }
    let id = "poc:deploy-runtime";
    let spec = json!({"kind": "receipt", "step": "DeployRuntime", "operation": "create-node", "node": "mesh1.rpc.1"});
    for d in &doors {
        d.arm(id, spec.clone()).await;
    }
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 1}]})).await;
    assert_eq!(status, 202, "{b}");
    let build = b["build_id"].as_str().unwrap().to_string();
    let holder = wait_for("one admin's door holds the create", Duration::from_secs(90), || async {
        for d in &doors {
            let c = d.cut(id).await;
            if c["held"] == true && !c["hit"].is_null() {
                return Some((d.name.clone(), c));
            }
        }
        None
    })
    .await;
    eprintln!("poc: {} holds {}", holder.0, holder.1);
    let (_, during) = estate.get(&format!("/api/builds?id={build}")).await;
    assert_ne!(during["status"], "complete", "the Build cannot complete while its step is held: {during}");
    for d in &doors {
        d.release(id).await;
    }
    estate.await_build(&build, SETTLE).await;
    wait_for("rpc.1 ready after the release", SETTLE, || async { estate.node_opt("mesh1.rpc.1").await.filter(ready) }).await;
    estate.stop().await;
}

/// CONTRACT: the probe drives real Node RPC by exact node id. A put/get round-trips through the
/// target, and `--cut-before-finish` cuts the call before its frame is finished, so it is refused
/// as `FrameNotSent` (the 499 reset) and never executed.
#[tokio::test(flavor = "multi_thread")]
async fn poc_probe_round_trips_and_a_cut_frame_is_not_sent() {
    let mut estate = small_estate("poc_probe_round_trips_and_a_cut_frame_is_not_sent").await;
    let id = estate.node("mesh1.rpc.1").await["node_id"].as_str().unwrap().to_string();
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
async fn poc_rp1_kill_every_admin_and_one_relaunch_restores_the_fabric() {
    let mut estate = Estate::bootstrap(owner("poc_rp1_kill_every_admin_and_one_relaunch_restores_the_fabric"), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    let build = a["build_id"].as_str().unwrap().to_string();
    estate.await_build(&build, SETTLE).await;
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"].iter().map(|s| s.to_string()).collect();
    let before = estate.settled(&want, Duration::from_secs(60)).await;
    let holder = before.iter().find(|n| n["is_fabric_primary"] == true).and_then(|n| n["name"].as_str()).unwrap().to_string();
    let holder_dir = if holder == "mesh1.admin.1" { estate.bootstrap_data_dir("mesh1") } else { PathBuf::from(estate.data_dir_of(&holder).await) };
    let rpc_id = estate.node("mesh1.rpc.1").await["node_id"].clone();
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy().contains(".admin.") {
            estate.kill_pid(pid);
        }
    }
    estate.kill_bootstrap();
    let base = estate.restart_admin_with(&holder_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.admin = base;
    let after = estate.settled(&want, Duration::from_secs(240)).await;
    let (_, fabric) = estate.get("/api/fabric").await;
    assert_eq!(fabric["build_id"], build.as_str(), "the same Build: {fabric}");
    let rpc_after = after.iter().find(|n| n["name"] == "mesh1.rpc.1").unwrap();
    assert_eq!(rpc_after["node_id"], rpc_id, "the live member is preserved");
    eprintln!("poc: relaunched {holder}; fabric {}", fabric["status"]);
    estate.stop().await;
}
