//! Functional: `CloseRpcAdmission` takes the exact retired runtime's exit as
//! proof that it admits no Node RPC, and only that runtime's.
//!
//! The observer hides every `Draining` and `Leaving` the node says, as if each
//! announcement were lost on the fabric: its view keeps the last
//! `ReadyForTraffic`. A runtime that exited still admits nothing, so the retire
//! completes. A successor birth at the same path that still runs is never
//! closed by its predecessor's exit.

mod common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build::BuildIntent;
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::deployment::endpoint::{EndpointAllocator, RPC_NODE_SLOTS};
use rafka_node_admin_core::deployment::pipeline::{
    CreateRequest, DeploymentPipeline, NodeObserver, RetireRequest, Timeouts,
};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch,
    TerminationMode,
};
use rafka_node_admin_core::model::{Node, ProviderKind};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Duration;

/// The live observer, except every departure the node announces is lost.
struct DeparturesLost<'a> {
    live: &'a LiveMesh,
}

#[async_trait::async_trait]
impl NodeObserver for DeparturesLost<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        self.live.ready(node).await
    }
    async fn drained(&self, _: &Node) -> bool {
        true
    }
    async fn admission_closed(&self, node: &Node) -> Result<(), String> {
        Err(format!(
            "{} still reports ReadyForTraffic (its departure was lost)",
            node.name
        ))
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
async fn an_exited_runtime_closes_its_admission_when_every_departure_is_lost() {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone());
    let builds = MemoryBuildStateAdapter::new();
    let allocator = Mutex::new(EndpointAllocator::new(
        IpAddr::from([127, 0, 0, 1]),
        58700,
        58701,
    ));
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
        allocator: &allocator,
        observer: &observer,
        sink: &sink,
        builds: &builds,
        template: &template,
        timeouts: timeouts(),
    };
    let created = pipeline
        .create(&CreateRequest {
            build_id: publish_build(&builds, add_node()).await,
            attempt: 1,
            node: "mesh1.rpc.1".parse().unwrap(),
            slots: RPC_NODE_SLOTS,
            restart_of: None,
        })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));

    let build = publish_build(
        &builds,
        BuildIntent::RemoveNode {
            node: created.node.name.clone(),
            incarnation: None,
        },
    )
    .await;
    let retired = pipeline
        .retire(&RetireRequest {
            build_id: build,
            attempt: 1,
            node: created.node.clone(),
            handle: created.handle.clone(),
            permanent: true,
        })
        .await;
    assert_eq!(
        retired,
        Ok(()),
        "the retired runtime exited: it admits nothing, whatever the fabric last heard"
    );
    assert!(exited(&process, &created.handle).await);
    let _ = std::fs::remove_dir_all(&template.data_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predecessors_exit_never_closes_its_running_successors_admission() {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone());
    let builds = MemoryBuildStateAdapter::new();
    let allocator = Mutex::new(EndpointAllocator::new(
        IpAddr::from([127, 0, 0, 1]),
        58710,
        58713,
    ));
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
        allocator: &allocator,
        observer: &observer,
        sink: &sink,
        builds: &builds,
        template: &template,
        timeouts: timeouts(),
    };
    let create = |build_id, restart_of| CreateRequest {
        build_id,
        attempt: 1,
        node: "mesh1.rpc.1".parse().unwrap(),
        slots: RPC_NODE_SLOTS,
        restart_of,
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
        BuildIntent::RemoveNode {
            node: b.node.name.clone(),
            incarnation: None,
        },
    )
    .await;
    let retired = pipeline
        .retire(&RetireRequest {
            build_id: build,
            attempt: 1,
            node: b.node.clone(),
            handle: b.handle.clone(),
            permanent: false,
        })
        .await;
    let err = retired.expect_err("B still runs and admits work: A's exit does not close B");
    assert_eq!(err.step, "CloseRpcAdmission", "{err}");
    assert!(
        matches!(process.inspect(&b.handle).await, DeploymentStatus::Running),
        "B runs"
    );

    process
        .terminate(&b.handle, TerminationMode::Immediate)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&template.data_root);
}
