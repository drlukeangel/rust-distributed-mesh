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

use rafka_node_admin_core::deployment::container::ContainerDeploymentProvider;
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{adopt, AdoptRefusal, DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::model::DeploymentId;
use rafka_mesh_entity::runtime::{RuntimeFact, RuntimeLocator};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

fn enabled() -> bool {
    std::env::var("RDM_CONTAINER_PROOF").as_deref() == Ok("1") || std::env::var("RDM_REQUIRE_CONTAINER").as_deref() == Ok("1")
}

/// The container provider for `fabric`, or `None` (named) when the proof is not asked for or the
/// host cannot run containers and the proof is not required.
async fn provider(fabric: &str) -> Option<ContainerDeploymentProvider> {
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
struct Fabric(String);

impl Drop for Fabric {
    fn drop(&mut self) {
        let docker = |args: &[&str]| std::process::Command::new("docker").args(args).output().ok().map(|o| String::from_utf8_lossy(&o.stdout).to_string()).unwrap_or_default();
        for id in docker(&["ps", "-aq", "--filter", &format!("label=rafka.fabric={}", self.0)]).split_whitespace() {
            docker(&["rm", "-f", id]);
        }
        docker(&["network", "rm", &format!("rafka-{}", self.0)]);
    }
}

fn fabric_name(cell: &str) -> String {
    format!("cproof-{cell}-{}", std::process::id())
}

/// One node's launch of the host's `tail -f /dev/null` at the network's `n`-th node address.
fn launch(p: &ContainerDeploymentProvider, fabric: &str, n: u32) -> ResolvedNodeLaunch {
    let (first, _) = p.network().node_range();
    let ip = Ipv4Addr::from(u32::from(first) + n);
    let data_dir = std::env::temp_dir().join(format!("{fabric}-node{n}"));
    ResolvedNodeLaunch {
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

/// CONTRACT: a container runtime is named by its provider control domain (the Docker daemon) and
/// its immutable container id. The deterministic container name is reused by the next birth at the
/// same path, and it never stands for the old runtime: the old id inspects as not running and
/// cannot be controlled, while the new runtime keeps running.
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
    assert!(fresh.terminate(&old, TerminationMode::Immediate).await.is_err(), "controlling the old id fails by name");
    assert_eq!(daemon_state(&id2), "running", "no control of the old id reached the runtime the name now runs");
    let found = p.find(&spec).await.expect("the name resolves to its current runtime");
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
async fn an_authority_in_another_docker_domain_refuses_the_locator_by_name_and_touches_nothing() {
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
