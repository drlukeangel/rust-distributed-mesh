//! Functional: a delete is commanded and proven per exact birth, whatever the fabric last heard.
//!
//! The observer hides every `ConnectionsDeleting` and `Leaving` the node says, as if each announcement were
//! lost on the fabric. The delete's proof is the commands and their completion calls
//! (`node.connection.delete` / `node.connections.deleted`, `stop-node` / `node-left`) and the exact runtime's `Exited`,
//! never a digest. A predecessor's exit at the same path stands for none of a successor's commands.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::accepted::FabricTopology;
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{CommandAdmission, Completion,
    CreateRequest, DeploymentPipeline, NodeObserver, DeleteRequest, Timeouts,
};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch,
    TerminationMode,
};
use rdm_admin::model::{Node, ProviderKind};
use std::net::IpAddr;
use std::time::Duration;

/// The live observer, except every departure the node announces is lost.
struct DeparturesLost<'a> {
    live: &'a LiveMesh,
}

#[async_trait::async_trait]
impl NodeObserver for DeparturesLost<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rdm_admin::deployment::pipeline::Publication> {
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        self.live.ready(node).await
    }
    async fn send_command(&self, node: &Node, cmd: rdm_admin::node_commands::NodeCommand, ctx: &rdm_admin::deployment::pipeline::CommandContext) -> CommandAdmission {
        self.live.send_command(node, cmd, ctx).await
    }
    async fn await_completion(&self, node: &Node, cmd: rdm_admin::node_commands::NodeCommand, ctx: &rdm_admin::deployment::pipeline::CommandContext, within: Duration) -> Completion {
        self.live.await_completion(node, cmd, ctx, within).await
    }
}

/// The real provider; `signal_stop` is ignored when armed, so the runtime
/// keeps running through its delete.
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
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, rdm_admin::deployment::provider::DeployError> {
        self.inner.find(spec).await
    }
}

fn timeouts() -> Timeouts {
    Timeouts {
        completion: Duration::from_secs(3),
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

#[test]
fn a_node_delete_completes_on_the_exact_runtimes_exit_when_every_departure_is_lost() {
    let cap = crate::common::capture("a_node_delete_completes_on_the_exact_runtimes_exit_when_every_departure_is_lost");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
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
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
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
                data_dir: None,
                held_runtimes: Vec::new(), entry: None
            })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));

        let build = publish_build(
            &builds,
            FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()),
        )
        .await;
        let deleted = pipeline
            .delete_node(&DeleteRequest {
                build_id: build,
                attempt: 1,
                node: created.node.clone(),
                handle: created.handle.clone(),
                unreached: false,
            })
            .await;
        let terminal = deleted.unwrap_or_else(|e| panic!("the deleted runtime exited: its commands were completed and its exit proven, whatever the fabric last heard: {e:?}"));
        assert_eq!((terminal.node_id.as_str(), Some(terminal.incarnation_id.as_str())), (created.node.node_id.as_str(), created.node.incarnation_id.as_ref().map(|i| i.as_str())), "the terminal receipt names the deleted birth: {terminal:?}");
        assert!(exited(&process, &created.handle).await);
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
}

#[test]
fn a_predecessors_exit_never_stands_for_its_successors_commands() {
    let cap = crate::common::capture("a_predecessors_exit_never_stands_for_its_successors_commands");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
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
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: timeouts(),
        };
        let create = |build_id| CreateRequest {
            build_id,
            attempt: 1,
            node: "mesh1.rpc.1".parse().unwrap(),
            spec: &RPC_NODE,
            data_dir: None,
            held_runtimes: Vec::new(), entry: None
        };

        // Birth A, then A exits.
        let a = pipeline
            .create(&create(publish_build(&builds, add_node()).await))
            .await
            .unwrap_or_else(|e| panic!("create A: {e}"));
        process
            .terminate(&a.handle, TerminationMode::Immediate)
            .await
            .unwrap();
        assert!(exited(&process, &a.handle).await, "A exited");

        // Birth B at the same path, ready.
        let b = pipeline
            .create(&create(publish_build(&builds, add_node()).await))
            .await
            .unwrap_or_else(|e| panic!("create B: {e}"));
        assert_ne!(b.node.incarnation_id, a.node.incarnation_id, "a new birth");
        assert_eq!(b.node.name, a.node.name, "at the same path");

        // Retiring B: B ignores its stop and keeps running, every departure is lost.
        let build = publish_build(
            &builds,
            FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()),
        )
        .await;
        let deleted = pipeline
            .delete_node(&DeleteRequest {
                build_id: build.clone(),
                attempt: 1,
                node: b.node.clone(),
                handle: b.handle.clone(),
                unreached: false,
            })
            .await;
        // B is live and admits its node-admin: its stop-node answered `stopped`, the completion the
        // commanding side awaits. A's exit counts for nothing:
        // every command and every proof is on B's own birth and handle.
        deleted.unwrap_or_else(|e| panic!("delete B: {e}"));
        assert!(matches!(process.inspect(&b.handle).await, DeploymentStatus::Exited { .. }), "B's own runtime is terminal");
        let steps = builds.read_build(&build).await.unwrap().steps;
        let output = |name: &str| steps.iter().find(|r| r.step == name).and_then(|r| r.output.clone()).unwrap_or_else(|| panic!("{name} receipted with an output"));
        let admission: CommandAdmission = serde_json::from_value(output("StopNode")).unwrap();
        assert!(matches!(admission, CommandAdmission::Stopped { .. }), "B answered StopNode left: {admission:?}");
        for step in ["AwaitNodeLeft"] {
            let completion: Completion = serde_json::from_value(output(step)).unwrap();
            assert!(matches!(completion, Completion::Received {}), "B's {step} completion call reached the commanding side: {completion:?}");
        }
    })
}
