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

fn gate_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rdm-giveup-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn give_up_leaves_nothing(tag: &str, exe: &std::path::Path, program: &str) {
    let node = format!("giveup{tag}{}.admin.1", std::process::id());
    let dir = gate_dir(tag);
    let t = std::time::Instant::now();
    let e = rafka_test_scenario::estate::spawn_admin_in_container_within(Vec::new(), &dir, &dir, &node, exe, &[], Duration::from_secs(3), program).expect_err("the Day-0 container never advertises within 3 s");
    assert!(!e.fabric_id.is_empty());
    // The give-up has returned: whatever the Docker daemon was still committing is awaited first, so nothing appears after it.
    std::thread::sleep(Duration::from_secs(8));
    let ls = |args: &[&str]| String::from_utf8_lossy(&Command::new("docker").args(args).output().unwrap().stdout).trim().to_string();
    let containers = ls(&["ps", "-aq", "--filter", &format!("label=rafka.fabric={}", e.fabric_id)]);
    let networks = ls(&["network", "ls", "-q", "--filter", &format!("label=rafka.fabric={}", e.fabric_id)]);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!((containers.as_str(), networks.as_str()), ("", ""), "after the give-up ({:?}) the fabric {} left a container or network: {}", t.elapsed(), e.fabric_id, e.message);
}

/// CONTRACT: when the harness gives up on the Day-0 container because `docker create` is still in
/// flight (a gated delay before the create commits), it waits for the create to finish and removes
/// the container and the fabric's network: no container or network labelled with the fabric exists
/// afterwards, and none appears later.
#[test]
fn a_give_up_while_docker_create_is_in_flight_leaves_no_container_or_network() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container give-up: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    let dir = gate_dir("gate");
    let shim = dir.join("docker-gated");
    std::fs::write(&shim, "#!/bin/sh\nif [ \"$1\" = create ]; then sleep 6; fi\nexec docker \"$@\"\n").unwrap();
    std::fs::set_permissions(&shim, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    give_up_leaves_nothing("gate", &rafka_test_scenario::estate::binary("rafka-node-admin"), shim.to_str().unwrap());
}

/// CONTRACT: when the Day-0 container is created and started but never advertises its API base,
/// the give-up kills the attached start and removes the container and the fabric's network.
#[test]
fn a_give_up_on_a_container_that_never_advertises_leaves_no_container_or_network() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container give-up: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    let dir = gate_dir("silent");
    let exe = dir.join("silent.sh");
    std::fs::write(&exe, "#!/bin/sh\nexec /usr/bin/sleep 1000\n").unwrap();
    std::fs::set_permissions(&exe, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    give_up_leaves_nothing("silent", &exe, "docker");
}

// ---- an estate outlives neither its test binary nor its teardown ----

const CHILD_ENV: &str = "RDM_ESTATE_SIGKILL_CHILD";

/// The estate's processes: every process whose environment carries this estate's root.
fn estate_processes(root: &str) -> Vec<u32> {
    let needle = format!("RDM_ESTATE_ROOT={root}\0");
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| std::fs::read(format!("/proc/{pid}/environ")).is_ok_and(|b| b.windows(needle.len()).any(|w| w == needle.as_bytes())))
        .collect()
}

/// The body of the estate a parent cell SIGKILLs: it runs only when the parent names a file for
/// the estate's root; otherwise it does nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_cell_estate_body() {
    let Ok(file) = std::env::var(CHILD_ENV) else { return };
    let provider = std::env::var("RDM_ESTATE_SIGKILL_PROVIDER").unwrap();
    let estate = Estate::bootstrap(
        Owner { product: "mesh".into(), feature: "mesh-runtime".into(), subfeature: "estate-sigkill".into(), rung: "MN".into(), provider: provider.clone(), test: format!("sigkill_cell_estate_body_{provider}") },
        "fabric1",
        "mesh1",
    )
    .await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
    std::fs::write(&file, estate.root.display().to_string()).unwrap();
    tokio::time::sleep(Duration::from_secs(600)).await;
}

/// Run the estate body in a child test binary, SIGKILL the binary mid-estate, and return what the
/// estate left: its processes, its root directory, and (container) its labelled containers.
fn sigkill_mid_estate(provider: &str) -> (Vec<u32>, bool, String) {
    let file = std::env::temp_dir().join(format!("rdm-sigkill-{provider}-{}", std::process::id()));
    let _ = std::fs::remove_file(&file);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sigkill_cell_estate_body", "--test-threads=1"])
        .env(CHILD_ENV, &file)
        .env("RDM_ESTATE_SIGKILL_PROVIDER", provider)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(150);
    while !file.exists() && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(200));
    }
    let root = std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("the estate body never reached its built fabric within 150 s ({:?})", child.try_wait()));
    let fabric = std::fs::read_to_string(std::path::Path::new(&root).join("fabric_id")).unwrap_or_default();
    let live = estate_processes(&root);
    let containers = |fabric: &str| String::from_utf8_lossy(&Command::new("docker").args(["ps", "-aq", "--filter", &format!("label=rafka.fabric={fabric}")]).output().unwrap().stdout).trim().to_string();
    if provider == "container" {
        assert!(!containers(&fabric).is_empty(), "the container estate runs containers before the kill");
    } else {
        assert!(live.len() >= 2, "the process estate runs an admin and a node before the kill: {live:?}");
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let left = estate_processes(&root);
        let dir = std::path::Path::new(&root).exists();
        let boxes = if provider == "container" { containers(&fabric) } else { String::new() };
        let net = if provider == "container" { String::from_utf8_lossy(&Command::new("docker").args(["network", "ls", "-q", "--filter", &format!("label=rafka.fabric={fabric}")]).output().unwrap().stdout).trim().to_string() } else { String::new() };
        if (left.is_empty() && !dir && boxes.is_empty() && net.is_empty()) || std::time::Instant::now() > until {
            let _ = std::fs::remove_file(&file);
            return (left, dir, format!("containers=[{boxes}] networks=[{net}]"));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn container_cells_enabled() -> bool {
    std::env::var("RDM_CONTAINER_PROOF").as_deref() == Ok("1") || std::env::var("RDM_REQUIRE_CONTAINER").as_deref() == Ok("1")
}

/// CONTRACT: when the test binary is SIGKILLed while its process-provider estate runs an admin
/// and a node, no process of that estate and no directory of it remains.
#[test]
fn a_sigkilled_test_binary_leaves_no_process_or_directory_of_its_process_estate() {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let (left, dir, rest) = sigkill_mid_estate("process");
    assert_eq!((left.as_slice(), dir), (&[][..], false), "processes {left:?} root-exists {dir} {rest}");
}

/// CONTRACT: when the test binary is SIGKILLed while its container estate runs, no container and
/// no network labelled with its fabric, no process and no directory of it remains.
#[test]
fn a_sigkilled_test_binary_leaves_no_container_network_or_directory_of_its_container_estate() {
    if std::env::var(CHILD_ENV).is_ok() || !container_cells_enabled() {
        return;
    }
    let (left, dir, rest) = sigkill_mid_estate("container");
    assert_eq!((left.as_slice(), dir, rest.as_str()), (&[][..], false, "containers=[] networks=[]"), "processes {left:?} root-exists {dir} {rest}");
}
