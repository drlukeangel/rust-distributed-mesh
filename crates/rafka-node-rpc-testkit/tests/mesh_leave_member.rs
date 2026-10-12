//! i143.e12.s40, s49 functional: one member's end inside a mesh delete is the whole node.delete
//! (mesh-delete.md step 3), which composes node.drain and the whole node.stop, so the deleting mesh's
//! primary publishes `NodeStopped` from the member's matching `stopped` reply (node-stop.md step 5,
//! gossip.md "Node-stopped lifecycle overlay") and then `NodeDeleted` from the provider's exact exit
//! proof (gossip.md §6.1). Those are what its aggregate carries in backbone `Members.in_flight` and
//! `Members.departed` to every peer mesh.

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published};
use rafka_mesh_entity::{FabricId, LifecycleOp};
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, LifecycleEvents, RetireRequest, Timeouts};
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
    async fn joined(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("node.joined:{}", op.operation));
    }
    async fn connections_deleting(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeDraining:{}", op.operation));
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(format!("NodeLeaving:{}", op.operation));
    }
    // A member's node.delete publishes no mesh-level event: those are the owner's and the deleting
    // primary's (mesh-delete.md), recorded here so a stray one shows in the order.
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

/// CONTRACT: a member's node.delete in a mesh delete publishes `NodeDeleting` first, then
/// `NodeStopped` once, from its matching `stopped` reply after its `node-leaving` hook, naming the
/// member's exact birth under its own `stop-node:<path>` operation of the mesh delete's Build and
/// attempt, and `NodeDeleted` last, from the provider's exact exit proof, under node.delete's own
/// `retire-node:<path>`. Without NodeStopped no peer mesh hears the member parked; without
/// NodeDeleted no peer holds it departed, and its own mesh-primary takes its exit for a member to
/// recover.
#[test]
fn a_mesh_delete_member_node_delete_publishes_node_stopped_then_node_deleted() {
    let cap = crate::common::capture("a_mesh_delete_member_node_delete_publishes_node_stopped_then_node_deleted");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
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
            .create(&CreateRequest { build_id: publish_build(&builds, add_node()).await, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));

        let leave_build = publish_build(&builds, add_node()).await;
        let terminal = pipeline
            .retire(&RetireRequest { build_id: leave_build.clone(), attempt: 1, node: first.node.clone(), handle: first.handle.clone(), exit: None })
            .await
            .unwrap_or_else(|e| panic!("the member's node.delete: {e}"));
        assert!(matches!(provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }), "the member's runtime is proven exited");
        assert_eq!((terminal.node_id.as_str(), Some(terminal.incarnation_id.as_str())), (first.node.node_id.as_str(), first.node.incarnation_id.as_ref().map(|i| i.0.as_str())), "the terminal receipt names the member's exact birth");

        let events = recorder.1.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                "NodeDeleting:retire-node:mesh1.rpc.1".to_string(),
                "NodeLeaving:stop-node:mesh1.rpc.1".into(),
                "NodeStopped:stop-node:mesh1.rpc.1".into(),
                "NodeDeleted:retire-node:mesh1.rpc.1".into()
            ],
            "the member's lifecycle events, in order"
        );
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
