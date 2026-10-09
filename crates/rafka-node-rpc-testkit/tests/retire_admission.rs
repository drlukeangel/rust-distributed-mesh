//! Functional: a retire is commanded and proven per exact birth, whatever the fabric last heard.
//!
//! The observer hides every `Draining` and `Leaving` the node says, as if each announcement were
//! lost on the fabric. The retire's proof is the commands and their completion calls
//! (`drain-node` / `node-drained`, `stop-node` / `node-left`) and the exact runtime's `Exited`,
//! never a digest. A predecessor's exit at the same path stands for none of a successor's commands.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::accepted::FabricTopology;
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CommandAdmission, Completion,
    CreateRequest, DeploymentPipeline, NodeObserver, RetireRequest, Timeouts,
};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch,
    TerminationMode,
};
use rafka_node_admin_core::model::{Node, ProviderKind};
use std::net::IpAddr;
use std::time::Duration;

/// The live observer, except every departure the node announces is lost.
struct DeparturesLost<'a> {
    live: &'a LiveMesh,
}

#[async_trait::async_trait]
impl NodeObserver for DeparturesLost<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        self.live.ready(node).await
    }
    async fn send_command(&self, node: &Node, cmd: rafka_node_admin_core::node_commands::NodeCommand, ctx: &rafka_node_admin_core::deployment::pipeline::CommandContext) -> CommandAdmission {
        self.live.send_command(node, cmd, ctx).await
    }
    async fn await_completion(&self, node: &Node, cmd: rafka_node_admin_core::node_commands::NodeCommand, ctx: &rafka_node_admin_core::deployment::pipeline::CommandContext, within: Duration) -> Completion {
        self.live.await_completion(node, cmd, ctx, within).await
    }
    async fn departed(&self, _: &Node) -> bool {
        false
    }
}

/// The real provider; `signal_stop` is ignored when armed, so the runtime
/// keeps running through its retire.
struct Stubborn<'a> {
    inner: &'a ProcessDeploymentProvider,
    ignore_stop: bool,
}

#[async_trait::async_trait]
impl DeploymentProvider for Stubborn<'_> {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }
    fn control_domain(&self) -> String {
        self.inner.control_domain()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        self.inner.spawn(spec).await
    }
    async fn terminate(&self, h: &DeploymentHandle, m: TerminationMode) -> Result<(), DeployError> {
        self.inner.terminate(h, m).await
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        self.inner.inspect(h).await
    }
    async fn signal_stop(&self, h: &DeploymentHandle) -> Result<(), DeployError> {
        if self.ignore_stop {
            return Ok(());
        }
        self.inner.signal_stop(h).await
    }
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        self.inner.find(spec).await
    }
}

fn timeouts() -> Timeouts {
    Timeouts {
        drain: Duration::from_secs(3),
        ..Timeouts::default()
    }
}

async fn exited(provider: &ProcessDeploymentProvider, h: &DeploymentHandle) -> bool {
    for _ in 0..100 {
        if matches!(provider.inspect(h).await, DeploymentStatus::Exited { .. }) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retire_completes_on_the_exact_runtimes_exit_when_every_departure_is_lost() {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    let builds = MemoryBuildStateAdapter::new();
    let sink = Published::default();
    let process = ProcessDeploymentProvider::new();
    let provider = Stubborn {
        inner: &process,
        ignore_stop: false,
    };
    let observer = DeparturesLost {
        live: &admin.observer,
    };
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &admin.joins,
        observer: &observer,
        sink: &sink,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: timeouts(),
    };
    let created = pipeline
        .create(&CreateRequest {
            build_id: publish_build(&builds, add_node()).await,
            attempt: 1,
            node: "mesh1.rpc.1".parse().unwrap(),
            spec: &RPC_NODE,
            restart_of: None,
            held_runtimes: Vec::new(),
        })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));

    let build = publish_build(
        &builds,
        FabricTopology::root("fabric1", "mesh1"),
    )
    .await;
    let retired = pipeline
        .retire(&RetireRequest {
            build_id: build,
            attempt: 1,
            node: created.node.clone(),
            handle: created.handle.clone(),
            kind: rafka_node_admin_core::deployment::pipeline::RetireKind::Removal,
            observe_departure: false,
        })
        .await;
    assert_eq!(
        retired,
        Ok(()),
        "the retired runtime exited: its commands were completed and its exit proven, whatever the fabric last heard"
    );
    assert!(exited(&process, &created.handle).await);
    let _ = std::fs::remove_dir_all(&template.data_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predecessors_exit_never_stands_for_its_successors_commands() {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    let builds = MemoryBuildStateAdapter::new();
    let sink = Published::default();
    let process = ProcessDeploymentProvider::new();
    let provider = Stubborn {
        inner: &process,
        ignore_stop: true,
    };
    let observer = DeparturesLost {
        live: &admin.observer,
    };
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &admin.joins,
        observer: &observer,
        sink: &sink,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: timeouts(),
    };
    let create = |build_id, restart_of| CreateRequest {
        build_id,
        attempt: 1,
        node: "mesh1.rpc.1".parse().unwrap(),
        spec: &RPC_NODE,
        restart_of,
        held_runtimes: Vec::new(),
    };

    // Birth A, then A exits.
    let a = pipeline
        .create(&create(publish_build(&builds, add_node()).await, None))
        .await
        .unwrap_or_else(|e| panic!("create A: {e}"));
    process
        .terminate(&a.handle, TerminationMode::Immediate)
        .await
        .unwrap();
    assert!(exited(&process, &a.handle).await, "A exited");

    // Birth B at the same path, ready.
    let b = pipeline
        .create(&create(publish_build(&builds, add_node()).await, None))
        .await
        .unwrap_or_else(|e| panic!("create B: {e}"));
    assert_ne!(b.node.incarnation_id, a.node.incarnation_id, "a new birth");
    assert_eq!(b.node.name, a.node.name, "at the same path");

    // Retiring B: B ignores its stop and keeps running, every departure is lost.
    let build = publish_build(
        &builds,
        FabricTopology::root("fabric1", "mesh1"),
    )
    .await;
    let retired = pipeline
        .retire(&RetireRequest {
            build_id: build.clone(),
            attempt: 1,
            node: b.node.clone(),
            handle: b.handle.clone(),
            kind: rafka_node_admin_core::deployment::pipeline::RetireKind::Restart,
            observe_departure: false,
        })
        .await;
    // B is live and admits its node-admin: its drain-node and stop-node were admitted, and its
    // node-drained and node-left calls reached the commanding side. A's exit counts for nothing:
    // every command and every proof is on B's own birth and handle.
    retired.unwrap_or_else(|e| panic!("retire B: {e}"));
    assert!(matches!(process.inspect(&b.handle).await, DeploymentStatus::Exited { .. }), "B's own runtime is terminal");
    let steps = builds.read_build(&build).await.unwrap().steps;
    let output = |name: &str| steps.iter().find(|r| r.step == name).and_then(|r| r.output.clone()).unwrap_or_else(|| panic!("{name} receipted with an output"));
    for step in ["DrainNode", "StopNode"] {
        let admission: CommandAdmission = serde_json::from_value(output(step)).unwrap();
        assert!(matches!(admission, CommandAdmission::Admitted), "B admitted {step}: {admission:?}");
    }
    for step in ["AwaitNodeDrained", "AwaitNodeLeft"] {
        let completion: Completion = serde_json::from_value(output(step)).unwrap();
        assert!(matches!(completion, Completion::Received), "B's {step} completion call reached the commanding side: {completion:?}");
    }
}

/// CONTRACT (Luke 2026-10-05, a mesh retire's departure barrier): inside a whole-mesh retire, the
/// local cleanup waits until this admin's own membership view has heard the birth's own `Leaving`.
/// When every departure is lost, the retire stops at `ObserveDeparture` by name and the node is NOT
/// removed from this admin's view: a completed `StopNode` step is not proof. Must NOT happen:
/// `RemoveTopologyMembership` running, or success synthesised from the runtime's exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_retire_holds_the_local_cleanup_until_the_departure_is_heard() {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    let builds = MemoryBuildStateAdapter::new();
    let sink = Published::default();
    let process = ProcessDeploymentProvider::new();
    let provider = Stubborn { inner: &process, ignore_stop: false };
    let observer = DeparturesLost { live: &admin.observer };
    let pipeline = DeploymentPipeline { provider: &provider, joins: &admin.joins, observer: &observer, sink: &sink, lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents, builds: &builds, template: &template, timeouts: timeouts() };
    let created = pipeline
        .create(&CreateRequest { build_id: publish_build(&builds, add_node()).await, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let build = publish_build(&builds, FabricTopology::root("fabric1", "mesh1")).await;
    let retired = pipeline
        .retire(&RetireRequest { build_id: build, attempt: 1, node: created.node.clone(), handle: created.handle.clone(), kind: rafka_node_admin_core::deployment::pipeline::RetireKind::Removal, observe_departure: true })
        .await;
    let err = retired.expect_err("an unheard departure holds the mesh retire");
    assert!(err.to_string().contains("never heard its own Leaving"), "{err}");
    assert!(sink.removed.lock().unwrap().is_empty(), "the local cleanup did not run");
    let _ = std::fs::remove_dir_all(&template.data_root);
}

/// CONTRACT (same ruling): with the live membership view, the birth's own `Leaving` is heard and
/// the mesh retire completes: `ObserveDeparture` and then `RemoveTopologyMembership` run, and the
/// node leaves this admin's view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_retire_cleans_up_once_the_departure_is_heard() {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    let builds = MemoryBuildStateAdapter::new();
    let sink = Published::default();
    let process = ProcessDeploymentProvider::new();
    let pipeline = DeploymentPipeline { provider: &process, joins: &admin.joins, observer: &admin.observer, sink: &sink, lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents, builds: &builds, template: &template, timeouts: timeouts() };
    let created = pipeline
        .create(&CreateRequest { build_id: publish_build(&builds, add_node()).await, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let build = publish_build(&builds, FabricTopology::root("fabric1", "mesh1")).await;
    let retired = pipeline
        .retire(&RetireRequest { build_id: build, attempt: 1, node: created.node.clone(), handle: created.handle.clone(), kind: rafka_node_admin_core::deployment::pipeline::RetireKind::Removal, observe_departure: true })
        .await;
    assert_eq!(retired, Ok(()), "the departure was heard: the mesh retire completes");
    assert_eq!(sink.removed.lock().unwrap().as_slice(), &[created.node.name.clone()], "then the local cleanup ran");
    let _ = std::fs::remove_dir_all(&template.data_root);
}
