//! i143.e12.s40 functional: one member's shutdown inside a mesh leave composes node.drain and the
//! whole node.stop (mesh-leave.md, "Operations and lifecycle events"), so the leaving mesh's primary
//! publishes `NodeStopped` from the member's matching `stopped` reply (node-stop.md step 5,
//! gossip.md "Node-stopped lifecycle overlay"). That overlay is what its aggregate carries in
//! backbone `Members.in_flight` to every peer mesh.

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published};
use rafka_mesh_entity::{FabricId, LifecycleOp};
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, LifecycleEvents, Timeouts};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

/// The lifecycle events the member's shutdown publishes, in order, each with the runtime's state at
/// the moment it left the executor.
#[derive(Default, Clone)]
struct Recorder(Arc<Mutex<Vec<LifecycleOp>>>, Arc<Mutex<Vec<String>>>);

#[async_trait::async_trait]
impl LifecycleEvents for Recorder {
    async fn deleting(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeDeleting:{}", op.operation));
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeDeleted:{}", op.operation));
    }
    async fn restarting(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeRestarting:{}", op.operation));
    }
    async fn stopped(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeStopped:{}", op.operation));
        self.0.lock().unwrap().push(op.clone());
    }
    async fn draining(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeDraining:{}", op.operation));
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeLeaving:{}", op.operation));
    }
    // A member's shutdown publishes no mesh-level event: those are the owner's and the leaving
    // primary's (mesh-leave.md), recorded here so a stray one shows in the order.
    async fn mesh_leaving(&self, event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {
        self.1.lock().unwrap().push(format!("MeshLeaving:{}", event.operation));
    }
    async fn mesh_leave(&self, event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {
        self.1.lock().unwrap().push(format!("MeshLeave:{}", event.operation));
    }
    async fn mesh_left(&self, event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {
        self.1.lock().unwrap().push(format!("MeshLeft:{}", event.operation));
    }
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

/// CONTRACT: a member's shutdown in a mesh leave publishes `NodeStopped` once, from its matching
/// `stopped` reply: after its `node-leaving` hook, naming the member's exact birth under its own
/// `stop-node:<path>` operation of the leave's Build and attempt. Without it no peer mesh hears the
/// member parked: the leaving primary's aggregate carries no overlay of it.
#[test]
fn a_mesh_leave_member_shutdown_publishes_node_stopped_from_its_stopped_reply() {
    let cap = crate::common::capture("a_mesh_leave_member_shutdown_publishes_node_stopped_from_its_stopped_reply");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
        let builds = MemoryBuildStateAdapter::new();
        let sink = Published::default();
        let provider = ProcessDeploymentProvider::new();
        let recorder = Recorder::default();
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &admin.joins,
            observer: &admin.observer,
            sink: &sink,
            lifecycle: &recorder,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };
        let first = pipeline
            .create(&CreateRequest { build_id: publish_build(&builds, add_node()).await, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));

        let leave_build = publish_build(&builds, add_node()).await;
        let mut node = first.node.clone();
        pipeline.shutdown_member(&leave_build, 1, "shutdown-mesh:mesh1", &mut node, &first.handle).await.unwrap_or_else(|e| panic!("shutdown member: {e}"));
        assert!(matches!(provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }), "the member's runtime is proven exited");

        let events = recorder.1.lock().unwrap().clone();
        assert_eq!(events, vec!["NodeDraining:drain-node:mesh1.rpc.1".to_string(), "NodeLeaving:stop-node:mesh1.rpc.1".into(), "NodeStopped:stop-node:mesh1.rpc.1".into()], "the member's lifecycle events, in order");
        let stopped = recorder.0.lock().unwrap().clone();
        assert_eq!(stopped.len(), 1, "NodeStopped once: {stopped:?}");
        let op = &stopped[0];
        assert!(op.is_stop(), "a stop overlay, told from a restart or a removal by its operation: {op:?}");
        assert_eq!((op.build_id.as_str(), op.attempt), (leave_build.0.as_str(), 1), "the leave's Build and attempt");
        assert_eq!(op.node_id, first.node.node_id, "the exact member");
        assert_eq!(Some(&op.incarnation), first.node.incarnation_id.as_ref(), "the exact birth");
        assert_eq!(op.name, first.node.name);
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
}
