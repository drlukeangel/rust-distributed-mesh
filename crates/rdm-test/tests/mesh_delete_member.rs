//! i143.e12.s40, s49 functional: one member's end inside a mesh delete is the whole node.delete
//! (mesh-delete.md step 3), which composes node.connection.delete and the whole node.stop, so the deleting mesh's
//! primary publishes `NodeStopped` from the member's matching `stopped` reply (node-stop.md step 5,
//! gossip.md "Node-stopped lifecycle overlay") and then `NodeDeleted` from the provider's exact exit
//! proof (gossip.md §6.1). Those are what its mesh topology carries in backbone `TopologyFull.in_flight` and
//! `TopologyFull.deleted` to every peer mesh.

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published};
use rdm_core::{FabricId, LifecycleOp};
use rdm_admin::build_state::MemoryBuildStateAdapter;
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, LifecycleEvents, DeleteRequest, Timeouts};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, DeploymentStatus};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

/// The lifecycle events the member's shutdown publishes, in order, each with the runtime's state at
/// the moment it left the executor.
#[derive(Default, Clone)]
struct Recorder(Arc<Mutex<Vec<LifecycleOp>>>, Arc<Mutex<Vec<(&'static str, rdm_core::OperationId)>>>);

#[async_trait::async_trait]
impl LifecycleEvents for Recorder {
    async fn deleting(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(("NodeDeleting", op.operation.clone()));
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(("NodeDeleted", op.operation.clone()));
    }
    async fn stopped(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(("NodeStopped", op.operation.clone()));
        self.0.lock().unwrap().push(op.clone());
    }
    async fn joined(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(("node.joined", op.operation.clone()));
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.1.lock().unwrap().push(("stopping", op.operation.clone()));
    }
    // A member's node.delete publishes no mesh-level event: those are the owner's and the deleting
    // primary's (mesh-delete.md), recorded here so a stray one shows in the order.
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

/// CONTRACT: a member's node.delete in a mesh delete publishes `NodeDeleting` first, then
/// `NodeStopped` once, from its matching `stopped` reply after that reply's admission set it stopping, naming the
/// member's exact birth under its own `node.stop` with the explicit node ID subject operation of the mesh delete's Build,
/// and `NodeDeleted` last, from the provider's exact exited verdict, under node.delete's own
/// `node.delete` with the explicit node ID subject. Without NodeStopped no peer mesh hears the member stopped; without
/// NodeDeleted no peer holds it deleted, and its own mesh-primary takes its exit for a member to
/// recover.
#[test]
fn a_mesh_delete_member_node_delete_publishes_node_stopped_then_node_deleted() {
    let cap = crate::common::capture("a_mesh_delete_member_node_delete_publishes_node_stopped_then_node_deleted");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
        let builds = crate::common::stepping(&fabric, "mesh1");
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
            .create(&CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "1").unwrap()))), node_id: (rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "1").unwrap()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(publish_build(&builds, add_node()).await), path: "mesh1.node".into(), name: "1".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));

        let leave_build = publish_build(&builds, add_node()).await;
        let terminal = pipeline
            .delete_node(&DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(first.node).node_id), build_id: Some(leave_build.clone()), node: first.node.clone(), handle: first.handle.clone(), unreached: false })
            .await
            .unwrap_or_else(|e| panic!("the member's node.delete: {e}"));
        assert!(matches!(provider.inspect(&first.handle, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the member's runtime is proven exited");
        assert_eq!((terminal.node_id.as_str(), Some(terminal.incarnation_id.as_str())), (first.node.node_id.as_str(), first.node.incarnation_id.as_ref().map(|i| i.as_str())), "the terminal receipt names the member's exact birth");

        let frames: std::collections::BTreeMap<_, _> = spans.0.lock().unwrap().iter().filter(|(_, (n, _, f))| n == "rdm.node_admin.node.update.via-command-frame" && f.get("node_id").map(String::as_str) == Some(first.node.node_id.as_str()) && f.get("incarnation_id").map(String::as_str) == first.node.incarnation_id.as_ref().map(|id| id.as_str())).map(|(n, (_, _, f))| (*n, f["frame"].clone())).collect();
        assert_eq!(frames.values().map(String::as_str).collect::<Vec<_>>(), vec!["command-accepted", "connections-deleting", "stop-connections-deleted", "leaving", "stop-mesh-left", "node-stopped"], "the stream owner records admission, each primitive's acknowledgement and completion, then the stopped completion (Q205)");
        let events = recorder.1.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                ("NodeDeleting", rdm_core::OperationId::node("node.delete", &first.node.node_id)),
                ("NodeStopped", rdm_core::OperationId::node("node.stop", &first.node.node_id)),
                ("NodeDeleted", rdm_core::OperationId::node("node.delete", &first.node.node_id))
            ],
            "the member's lifecycle events, in order"
        );
        let stopped = recorder.0.lock().unwrap().clone();
        assert_eq!(stopped.len(), 1, "NodeStopped once: {stopped:?}");
        let op = &stopped[0];
        assert!(op.is_stop(), "a stop overlay, told from a restart or a removal by its operation: {op:?}");
        assert_eq!(op.build_id.as_str(), leave_build.as_str(), "the leave's Build");
        assert_eq!(op.node_id, first.node.node_id, "the exact member");
        assert_eq!(Some(&op.incarnation), first.node.incarnation_id.as_ref(), "the exact birth");
        assert_eq!(op.name, first.node.name);
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
}
