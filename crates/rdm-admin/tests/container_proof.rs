//! i143 container proof (PRD §12.2), the provider cells: exact container identity, same-domain
//! successor control, and foreign-domain refusal. The kill cell runs through Build and runtime
//! adoption in the scenario suite (`mesh_runtime__container_kill`).
//!
//! Each cell runs one real container on its own fabric network. The workload is the host's
//! `tail -f /dev/null` (the runtime image is empty; the host's `/usr` is mounted read-only), so
//! a cell proves the provider, not a node.
//!
//! Opt-in: it runs only with `RDM_CONTAINER_PROOF=1` (the container-proof step); otherwise it
//! skips by name and starts nothing. With `RDM_REQUIRE_CONTAINER=1` a host that cannot run
//! containers fails instead of skipping.

use rdm_admin::deployment::container::ContainerDeploymentProvider;
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{adopt, AdoptRefusal, DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::DeploymentId;
use rdm_core::runtime::{RuntimeFact, RuntimeLocator};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

fn enabled() -> bool {
    std::env::var("RDM_CONTAINER_PROOF").as_deref() == Ok("1") || std::env::var("RDM_REQUIRE_CONTAINER").as_deref() == Ok("1")
}

/// The container provider for `fabric`, or `None` (named) when the proof is not asked for or the
/// host cannot run containers and the proof is not required.
pub(crate) async fn provider(fabric: &str) -> Option<ContainerDeploymentProvider> {
    if !enabled() {
        eprintln!("SKIP container proof: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return None;
    }
    match ContainerDeploymentProvider::prepare(fabric).await {
        Ok(p) => Some(p),
        Err(DeployError::Unsupported { reason, .. }) if std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") => {
            eprintln!("SKIP container proof: this host cannot run containers: {reason}");
            None
        }
        Err(e) => panic!("the container provider cannot prepare {fabric}: {e}"),
    }
}

/// Removes every container and the network of one cell's fabric, however the cell ends.
pub(crate) struct Fabric(pub(crate) String);

impl Drop for Fabric {
    fn drop(&mut self) {
        let docker = |args: &[&str]| std::process::Command::new("docker").args(args).output().ok().map(|o| String::from_utf8_lossy(&o.stdout).to_string()).unwrap_or_default();
        for id in docker(&["ps", "-aq", "--filter", &format!("label=rafka.fabric={}", self.0)]).split_whitespace() {
            docker(&["rm", "-f", id]);
        }
        docker(&["network", "rm", &format!("rafka-{}", self.0)]);
        // The cell's node data dirs (`launch`): `<fabric>-node<n>` under the temp dir.
        for e in std::fs::read_dir(std::env::temp_dir()).into_iter().flatten().flatten() {
            if e.file_name().to_string_lossy().starts_with(&format!("{}-node", self.0)) {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    }
}

pub(crate) fn fabric_name(cell: &str) -> String {
    format!("cproof-{cell}-{}", std::process::id())
}

/// One node's launch of the host's `tail -f /dev/null` at the network's `n`-th node address.
pub(crate) fn launch(p: &ContainerDeploymentProvider, fabric: &str, n: u32) -> ResolvedNodeLaunch {
    let (first, _) = p.network().node_range();
    let ip = Ipv4Addr::from(u32::from(first) + n);
    let data_dir = std::env::temp_dir().join(format!("{fabric}-node{n}"));
    ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(),
        node: format!("mesh1.rpc.{}", n + 1).parse().unwrap(),
        deployment_id: DeploymentId::mint(),
        executable: PathBuf::from("/usr/bin/tail"),
        args: vec!["-f".into(), "/dev/null".into()],
        env: Default::default(),
        data_dir,
        transport: SocketAddr::new(IpAddr::V4(ip), 20000),
        listeners: Vec::new(),
    }
}

fn container_id(h: &DeploymentHandle) -> String {
    h.container.clone().expect("a container handle names its container")
}

/// `docker inspect` straight from the daemon: the oracle the provider is checked against.
fn daemon_state(id: &str) -> String {
    let o = std::process::Command::new("docker").args(["inspect", "--format", "{{.State.Status}}", id]).output().unwrap();
    if o.status.success() {
        String::from_utf8_lossy(&o.stdout).trim().to_string()
    } else {
        "absent".into()
    }
}

/// CONTRACT (lifecycles/node-create.md: exact provider proof of a runtime's state, even for a
/// deployment that never joined): the container provider's `find` tells the three facts of one exact
/// launch apart, whichever admin of its control domain asks. A launch never made is `NeverLaunched`;
/// a launch whose container runs is `Running`; once this provider terminated it (which removes the
/// container, so the daemon holds nothing by its name) it is `Exited`, to this provider and to a fresh
/// one that never saw it. Must NOT happen: a launch that ran read as never launched once its container
/// is gone, so a re-run starts it under the same incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_tells_a_running_container_launch_a_terminated_one_and_one_never_launched_apart() {
    let fabric = fabric_name("find-three");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let (spec, never) = (launch(&p, &fabric, 0), launch(&p, &fabric, 1));
    assert_eq!(p.find(&never).await.unwrap(), Found::NeverLaunched, "no container and no launch of it");
    let h = p.spawn(&spec).await.expect("the launch");
    assert_eq!(p.find(&spec).await.unwrap(), Found::Running(h.clone()), "its container runs");
    p.terminate(&h, TerminationMode::Immediate).await.expect("terminate the launch");
    assert_eq!(daemon_state(&container_id(&h)), "absent", "terminate removes the container");
    assert!(matches!(p.find(&spec).await.unwrap(), Found::Exited(_)), "the launch ran and its container is gone: exited");
    let fresh = ContainerDeploymentProvider::prepare(&fabric).await.unwrap();
    assert!(matches!(fresh.find(&spec).await.unwrap(), Found::Exited(_)), "a provider of the same domain that never saw the launch proves it exited too");
    assert_eq!(fresh.find(&never).await.unwrap(), Found::NeverLaunched);
}

/// CONTRACT (Q79, node-replace.md: a replace re-births at the same path.name while its predecessor,
/// renamed and stopped, still runs; container.rs: "each node owns one address on the fabric network
/// (`--ip`)"): the address the container provider binds belongs to the birth that holds it. A new birth
/// at a name whose earlier birth's container still runs is given an address no container on the network
/// holds, and its container starts beside the earlier one. Must NOT happen: the new birth handed the
/// earlier birth's address (docker run: "Address already in use", the rshape replace on 0a10139b).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_birth_at_a_name_whose_stopped_predecessor_still_runs_gets_its_own_address() {
    let fabric = fabric_name("new-birth-address");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let name = launch(&p, &fabric, 0).node;
    let first = launch(&p, &fabric, 0);
    let first_ip = p.bind_ip(&name, &first.deployment_id).await.expect("an address for the first birth");
    let first = ResolvedNodeLaunch { transport: SocketAddr::new(first_ip, 20000), ..first };
    let held = p.spawn(&first).await.expect("the first birth's container runs");
    assert_eq!(p.bind_ip(&name, &first.deployment_id).await.expect("the first birth's address again"), first_ip, "a birth binds the address it holds again");
    // The first birth is renamed and stopped: its container keeps running, and its address with it.
    let second = ResolvedNodeLaunch { node: name.clone(), ..launch(&p, &fabric, 1) };
    let second_ip = p.bind_ip(&name, &second.deployment_id).await.expect("an address for the new birth");
    let second = ResolvedNodeLaunch { transport: SocketAddr::new(second_ip, 20000), ..second };
    p.spawn(&second).await.unwrap_or_else(|e| panic!("the new birth at {name} starts on its own address ({second_ip}; the stopped birth holds {first_ip}): {e}"));
    assert_ne!(second_ip, first_ip, "the new birth at {name} is given an address of its own");
    assert_eq!(daemon_state(&container_id(&held)), "running", "the stopped predecessor still runs on its address");
}

/// CONTRACT (lifecycles/node-create.md step 1: "Record the exact deployment ... and launch its
/// executable"; a record with no live runtime reads as exited): the launch's record is durable before
/// the container is created, as the process provider's is before its executable runs. A launch whose
/// `docker run` fails (its address is not on the fabric network) leaves no container, and its record
/// says the launch was made: `find` answers `Exited` to this provider and to a fresh one. Must NOT
/// happen: `NeverLaunched` for a launch whose record was written before the container was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_launch_whose_container_is_never_created_leaves_a_record_that_reads_exited() {
    let fabric = fabric_name("record-first");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let spec = ResolvedNodeLaunch { transport: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)), 20000), ..launch(&p, &fabric, 0) };
    let refused = p.spawn(&spec).await.expect_err("docker cannot place an address outside the fabric network");
    let listed = std::process::Command::new("docker").args(["ps", "-aq", "--filter", &format!("label=rafka.fabric={fabric}")]).output().unwrap();
    assert_eq!(String::from_utf8_lossy(&listed.stdout).trim(), "", "no container of the fabric exists after {refused}");
    assert!(matches!(p.find(&spec).await.unwrap(), Found::Exited(_)), "the launch's record names it and no container is left");
    let fresh = ContainerDeploymentProvider::prepare(&fabric).await.unwrap();
    assert!(matches!(fresh.find(&spec).await.unwrap(), Found::Exited(_)), "a provider of the same domain that never saw the launch reads the same record");
    assert_eq!(fresh.find(&launch(&p, &fabric, 1)).await.unwrap(), Found::NeverLaunched, "another launch has no record");
}

/// CONTRACT: a container runtime is named by its provider control domain (the Docker daemon) and
/// its immutable container id. The deterministic container name is reused by the next birth at the
/// same path, and it never stands for the old runtime (deployment-provider.md: "a reused name never
/// impersonates"): the old id inspects as not running, and a TerminateRuntime of it, a runtime already
/// removed, terminates as done (Q145) without reaching the new runtime, which keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_container_is_its_docker_domain_and_immutable_id_and_a_reused_name_never_impersonates_it() {
    let fabric = fabric_name("identity");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let spec = launch(&p, &fabric, 0);

    let first = p.spawn(&spec).await.expect("first birth");
    let id1 = container_id(&first);
    assert!(id1.len() == 64 && id1.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()), "the immutable id, never the name: {id1}");
    assert_eq!(first.domain.as_deref(), Some(p.control_domain().as_str()));
    assert!(p.control_domain().starts_with("container:"), "{}", p.control_domain());
    let fact = first.fact().expect("an exact container handle publishes its fact");
    assert_eq!(fact.locator, RuntimeLocator::Container { id: id1.clone() });
    assert_eq!(fact.control_domain, p.control_domain());

    p.terminate(&first, TerminationMode::Immediate).await.expect("terminate the first birth");
    let second = p.spawn(&spec).await.expect("second birth at the same path and name");
    let id2 = container_id(&second);
    assert_ne!(id1, id2, "the reused name is a new runtime");
    assert_eq!(daemon_state(&id2), "running");

    // A provider that never saw either birth: the old id is not the runtime the name now runs.
    let fresh = ContainerDeploymentProvider::prepare(&fabric).await.unwrap();
    let old = adopt(&fresh, &fact).expect("same domain: the old fact is adopted as what it names");
    assert_ne!(fresh.inspect(&old).await, DeploymentStatus::Running, "the old id never inspects as the new runtime");
    assert_ne!(p.inspect(&first).await, DeploymentStatus::Running);
    assert!(fresh.terminate(&old, TerminationMode::Immediate).await.is_ok(), "the old id, already removed, terminates as done (Q145)");
    assert_eq!(daemon_state(&id2), "running", "no control of the old id reached the runtime the name now runs");
    let found = p.find(&spec).await.unwrap().running().expect("the name resolves to its current runtime");
    assert_eq!(container_id(&found), id2);
}

/// CONTRACT: an admin in the same provider control domain that never launched a container adopts
/// it from its published fact alone, inspects it and controls it; its stop reaches the exact
/// container the launcher started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_successor_in_the_same_docker_domain_adopts_inspects_and_controls_a_container_it_never_launched() {
    let fabric = fabric_name("successor");
    let Some(launcher) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let launched = launcher.spawn(&launch(&launcher, &fabric, 0)).await.expect("launch");
    let id = container_id(&launched);
    let fact: RuntimeFact = launched.fact().unwrap();

    let successor = ContainerDeploymentProvider::prepare(&fabric).await.unwrap();
    assert!(successor.launched().is_empty(), "the successor launched nothing");
    assert_eq!(successor.control_domain(), launcher.control_domain(), "one Docker daemon, one control domain");
    let adopted = adopt(&successor, &fact).expect("same domain: adopted");
    assert_eq!(container_id(&adopted), id, "the exact container");
    assert_eq!(adopted.deployment_id, launched.deployment_id);
    assert_eq!(successor.inspect(&adopted).await, DeploymentStatus::Running);

    successor.terminate(&adopted, TerminationMode::Immediate).await.expect("the successor stops it");
    assert_eq!(daemon_state(&id), "absent", "the successor's control reached the exact container");
    assert!(matches!(successor.inspect(&adopted).await, DeploymentStatus::Exited { .. }), "its terminal state is held");
    assert_ne!(launcher.inspect(&launched).await, DeploymentStatus::Running, "the launcher sees it gone too");
}

/// CONTRACT: an authority in another provider control domain never treats a container locator as
/// local. Adoption refuses by name, naming both domains, and nothing reaches the container. A
/// process provider refuses a container fact as another provider's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_admin_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing() {
    let fabric = fabric_name("foreign");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let launched = p.spawn(&launch(&p, &fabric, 0)).await.expect("launch");
    let id = container_id(&launched);
    let fact = launched.fact().unwrap();

    let elsewhere = RuntimeFact { control_domain: "container:another-docker-daemon".into(), ..fact.clone() };
    match adopt(&p, &elsewhere) {
        Err(AdoptRefusal::ForeignControlDomain { fact_domain, here }) => {
            assert_eq!(fact_domain, "container:another-docker-daemon");
            assert_eq!(here, p.control_domain());
        }
        other => panic!("a foreign locator was not refused by its domain: {other:?}"),
    }
    let process = ProcessDeploymentProvider::new();
    assert!(matches!(adopt(&process, &fact), Err(AdoptRefusal::OtherProvider { .. })), "a process provider never adopts a container");
    assert_eq!(daemon_state(&id), "running", "a refusal touches nothing");
    assert_eq!(p.inspect(&launched).await, DeploymentStatus::Running);
}

/// R14 item 6: two independent runtimes see one mounted namespace. Hard-link insertion
/// excludes an existing key; replacements are whole; file and directory fsync succeed;
/// writes, removals and listings are coherent in both directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn containers_share_a_store_root_with_exclusive_inserts_atomic_replacements_and_synced_files_and_directories() {
    let fabric = fabric_name("store-root");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let store = std::env::temp_dir().join(format!("{fabric}-node-store"));
    std::fs::create_dir(&store).unwrap();
    let control = store.join(".qualification");
    std::fs::create_dir(&control).unwrap();
    let probe = std::env::current_exe().unwrap().parent().unwrap().parent().unwrap().join(format!("rdm-store-root-probe{}", std::env::consts::EXE_SUFFIX));
    assert!(probe.is_file(), "the workspace build must supply {}", probe.display());
    let mut a = launch(&p, &fabric, 0);
    let mut b = launch(&p, &fabric, 1);
    for (spec, role) in [(&mut a, "writer"), (&mut b, "reader")] {
        spec.executable = probe.clone();
        spec.args = vec![role.into()];
        spec.env.insert(rdm_core::launch::ENV_STORE_DIR.into(), store.display().to_string());
    }
    let (a, b) = tokio::join!(p.spawn(&a), p.spawn(&b));
    let a = a.expect("writer container");
    let b = b.expect("reader container");
    let actors = [&a, &b];
    store_phase(&actors, &control, "native participants ready", None, &["writer-ready", "reader-ready"]).await;
    store_phase(&actors, &control, "hard-link insert and sync", Some("start"), &["inserted"]).await;
    store_phase(&actors, &control, "collision and shared listing", None, &["collision-complete"]).await;
    store_phase(&actors, &control, "atomic replacements and concurrent reads", Some("atomic"), &["writer-atomic", "reader-complete"]).await;
    store_phase(&actors, &control, "reverse read and listing", Some("reverse"), &["writer-complete"]).await;
    let reads: usize = std::fs::read_to_string(control.join("reads")).unwrap().parse().unwrap();
    assert!(reads > 0, "the reader observed concurrent whole-file replacements");
    println!("3 atomic 128 KiB replacements, file/directory fsync, {reads} published whole-file reads, coherent insert/refusal/read/listing in both directions");
    assert_eq!(std::fs::read(store.join("created")).unwrap(), b"first");
    assert_eq!(std::fs::read(store.join("atomic")).unwrap(), vec![b'B'; 131072]);
    assert!(rdm_admin::store_root::qualify(rdm_admin::model::ProviderKind::Container).is_ok(), "the proven container provider is admitted at boot");
    println!("R14 item 6 qualified: store={}, containers={} and {}", store.display(), container_id(&a), container_id(&b));
}

async fn store_phase(actors: &[&DeploymentHandle], control: &std::path::Path, phase: &str, command: Option<&str>, completed: &[&str]) {
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        if let Some(command) = command { tokio::fs::write(control.join(command), b"start").await.unwrap(); }
        loop {
            if completed.iter().all(|name| control.join(name).is_file()) { break; }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }).await;
    if let Err(e) = result {
        let mut evidence = Vec::new();
        for actor in actors {
            let id = container_id(actor);
            let out = tokio::process::Command::new("docker").args(["logs", &id]).output().await.unwrap();
            evidence.push(format!("container {id}: {}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr)));
        }
        panic!("store-root {phase} exceeded its 15 s bound: {e}; participants: {}", evidence.join("; "));
    }
    println!("store-root {phase}: complete in {:?}", started.elapsed());
}

/// R14 item 8: the provider mounts the supplied store, never creates a missing namespace
/// through Docker's volume shorthand or substitutes the birth's data directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_container_store_root_is_refused_without_creating_it() {
    let fabric = fabric_name("missing-store");
    let Some(p) = provider(&fabric).await else { return };
    let _cleanup = Fabric(fabric.clone());
    let missing = std::env::temp_dir().join(format!("{fabric}-node-missing"));
    let mut spec = launch(&p, &fabric, 0);
    spec.env.insert(rdm_core::launch::ENV_STORE_DIR.into(), missing.display().to_string());
    let err = p.spawn(&spec).await.expect_err("a missing store namespace is refused");
    assert!(err.to_string().contains("RDM_STORE_DIR") && err.to_string().contains(missing.to_str().unwrap()), "{err}");
    assert!(!missing.exists(), "the missing store namespace was not created");
}
