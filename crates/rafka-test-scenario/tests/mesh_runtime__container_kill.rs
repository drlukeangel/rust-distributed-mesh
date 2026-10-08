//! i143 container proof (PRD §12.2), the kill cell: product=mesh, feature=mesh-runtime,
//! subfeature=container-kill, rung=MN, provider=container.
//!
//! A real container kill (`docker kill`, SIGKILL to the container's init) of an rpc node. The
//! fabric-primary hears the birth fall silent, proves it gone by its exact runtime's own terminal
//! status (the provider inspecting the immutable container id in its control domain), opens the
//! next attempt of the accepted Build, and that attempt re-creates the node at the same path in
//! a new container. Nothing here signals the fabric: the Build and runtime-adoption path does the
//! recovery.
//!
//! The node-admins stay host processes: a node-admin in a container would need the Docker daemon
//! inside it. One node-admin drives the container fabric.
//!
//! Opt-in: it runs only with `RDM_CONTAINER_PROOF=1` (the container-proof step); otherwise it
//! skips by name and starts nothing.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::process::Command;
use std::time::Duration;

const NODE: &str = "mesh1.rpc.1";

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn docker_state(id: &str) -> String {
    let o = Command::new("docker").args(["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", id]).output().unwrap();
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

/// CONTRACT: a killed container is proven terminal by the provider inspecting its exact immutable
/// id, and that proof alone opens the next attempt of the accepted Build, which re-creates the node
/// at the same path in a new container under a new incarnation; the node id is kept by path, the
/// container id never is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_container_is_proven_terminal_by_inspection_and_the_build_recreates_it() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container kill: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    let mut estate = Estate::bootstrap(
        Owner {
            product: "mesh".into(),
            feature: "mesh-runtime".into(),
            subfeature: "container-kill".into(),
            rung: "MN".into(),
            provider: "container".into(),
            test: "a_killed_container_is_proven_terminal_by_inspection_and_the_build_recreates_it".into(),
        },
        "fabric1",
        "mesh1",
    )
    .await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    let build_id = s(&a["build_id"]);
    estate.await_build(&build_id, Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 1, 2)], Duration::from_secs(30)).await;

    let before = estate.node(NODE).await;
    assert_eq!(before["provider"], "container", "{before}");
    let killed = estate.container_of(NODE).expect("the rpc node runs in a container labelled with its fabric and path");
    assert_eq!(killed.len(), 64, "the immutable container id: {killed}");
    let survivor = estate.container_of("mesh1.rpc.2").expect("the other rpc node's container");

    // The real kill: SIGKILL to the container's init, from outside every admin.
    let out = Command::new("docker").args(["kill", &killed]).output().unwrap();
    assert!(out.status.success(), "docker kill: {}", String::from_utf8_lossy(&out.stderr));

    // Silent past the staleness floor, proven exited, re-created by the next attempt.
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let after = wait_for("mesh1.rpc.1 re-created ready under a new incarnation", floor + Duration::from_secs(90), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"]).then_some(n)
    })
    .await;
    let reborn = estate.container_of(NODE).expect("the re-created node runs in a container");
    assert_ne!(reborn, killed, "a new container, never the killed one restarted");
    assert!(docker_state(&killed).starts_with("exited 137"), "the killed container is terminal by SIGKILL: {}", docker_state(&killed));
    assert_eq!(estate.container_of("mesh1.rpc.2").as_deref(), Some(survivor.as_str()), "the other node's container was untouched");
    assert_eq!(after["provider"], "container");

    // Evidence: the proof opened the next attempt of the same Build, and that attempt re-created
    // the node through every create step.
    const SCOPE: &str = "mesh1.rpc_node: 1 present of 2 accepted (exited: mesh1.rpc.1)";
    let is_drift = |sp: &&Value| s(&sp["attributes"]["build_id"]) == build_id && s(&sp["attributes"]["scope"]) == SCOPE;
    let spans = wait_for("the drift attempt's spans are exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.update.via-proven-drift").iter().any(is_drift).then_some(spans)
    })
    .await;
    let drift = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().find(|sp| is_drift(sp)).unwrap();
    let born = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().any(|sp| sp["attributes"]["incarnation_id"] == after["incarnation_id"]);
    assert!(born, "the re-created container's own boot span landed beside the admin's");
    let attempt = s(&drift["attributes"]["attempt"]);
    let deployed = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().any(|sp| {
        let at = &sp["attributes"];
        s(&at["build_id"]) == build_id && s(&at["attempt"]) == attempt && at["node"] == NODE && at["step"] == "DeployRuntime" && at["outcome"] == "complete"
    });
    assert!(deployed, "attempt {attempt} of {build_id} deployed {NODE} anew");
    estate.record_trace_url(drift["trace_id"].as_str().unwrap_or(""));

    estate.stop().await;
    assert_eq!(estate.live_containers(), Vec::<(String, String)>::new(), "no container of the fabric is left running");
}
