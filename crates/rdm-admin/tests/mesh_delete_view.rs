//! The fabric-primary's view of a deleted mesh, as its publication of that mesh reads it
//! (mesh-delete.md "Command, completion and gossip messages" and step 7: "A separate node.delete
//! deletes the mesh-primary. The fabric-primary is its executor ... from that admin's stop on it
//! publishes the deleted mesh's topology"; gossip.md "Mesh delete": "From the stop on the
//! fabric-primary is that mesh's publisher of record ... the stopped admin in `in_flight` as
//! NodeStopped"). The fabric-primary takes the mesh as deleted at its own mesh.deleted receipt
//! (`admin::mesh_delete::record_mesh_deleted`: `Records::hold_mesh_deleted`, then the view installed
//! again), and its backbone holds the meshes that view names (`Records::set_backbone`), which is
//! what `Backbone::publishes_deleted_mesh_of` reads when the stop's status event is published.
// @feature: node-lifecycle

use crate::mesh_delete_resume::{capture, trace_id};
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{Backbone, TopologyView};
use rdm_admin::admin::{project, project_with, GossipLifecycle, Records, ViewReads};
use rdm_admin::deployment::pipeline::LifecycleEvents;
use rdm_admin::model::{PathName, ProviderKind, ScopeStatus};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::Instrument as _;

const FP: &str = "mesh3.admin.1";
const LAST: &str = "mesh1.admin.1";

fn digest(fabric: &FabricId, name: &str, mesh_id: &MeshId) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
        fabric_id: fabric.clone(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}-{}", "x".repeat(40))),
            transport_addr: "127.0.0.1:20001".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Ready,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000_000,
        data_dir: None,
        mesh_id: Some(mesh_id.clone()),
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_ns"].as_u64().unwrap_or(0)
}

/// CONTRACT: mesh1's last admin, mesh1.admin.1, still its primary after mesh.deleted, is stopped by the
/// fabric-primary mesh3.admin.1 as the executor of its node.delete right after the fabric-primary
/// recorded mesh1's mesh.deleted receipt. The receipt is what `record_mesh_deleted` does
/// (admin/leave.rs: "self.records.hold_mesh_deleted(&key.mesh_id); ... self.refresh_view().await"):
/// the mesh held deleted, then the view installed again from the stores. Before it the view names
/// mesh1 live, and the backbone holds that view's meshes. The stop's status event follows at once,
/// with no workflow of the admin's hierarchy in between. The view holds mesh1 deleted, the backbone
/// takes it deleted from that install, and the stop makes the fabric-primary mesh1's publisher of
/// record: the status event is stamped as the deleted mesh's publisher and mesh1's topology leaves
/// on the backbone, no digests, the stopped admin in `in_flight`. Must NOT happen: the backbone
/// still holding mesh1 live from the view before the receipt, so the stop is published as a plain
/// executor's and the stopped-stage mesh topology never leaves (s23 part 9's
/// mesh_lifecycle__fabric_primary_handover: receipt at -439.6 ms, the stop's status event at
/// +7.4 ms in trace c1ea5c0096b1230d0370d6c8dc484a7d, the view moved at the next workflow, +54.9 ms).
#[test]
fn the_fabric_primary_stopping_a_deleted_meshs_last_admin_right_after_its_mesh_deleted_receipt_publishes_the_stopped_aggregate() {
    let cell = "the_fabric_primary_stopping_a_deleted_meshs_last_admin_right_after_its_mesh_deleted_receipt_publishes_the_stopped_aggregate";
    let cap = capture();
    let (trace_id, live_before, after_receipt) = cap.run(async {
        let fabric = FabricId::mint();
        let (mesh1, mesh3) = (MeshId::mint(), MeshId::mint());
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let topology_view = TopologyView::join(&gossip, &ep, &fabric, "mesh3", &mesh3, FP, Arc::new(crate::adopted_time()), vec![], &[]).await.unwrap();
        let own = digest(&fabric, FP, &mesh3);
        let last = digest(&fabric, LAST, &mesh1);
        let backbone = Backbone::join(&gossip, &ep, &topology_view, "mesh3", rdm_core::PublisherId { node: FP.to_string(), incarnation: own.node.incarnation.clone(), node_id: own.node.node_id.clone() }, vec![]).await.unwrap();
        backbone.set_mesh_primary(true);
        backbone.set_fabric_primary(true);
        assert!(topology_view.digest_view.record(own.clone()) && topology_view.digest_view.record(last.clone()), "the view holds both admins");
        let digest_view = topology_view.digest_view.clone();
        let records = Records::default();
        let me: PathName = FP.parse().unwrap();
        let rebuild = |reads: &ViewReads| project_with("fabric1", &fabric, ProviderKind::Process, &digest_view, &records, reads);
        let status_of = |t: &rdm_admin::topology::Topology| t.meshes.iter().find(|m| m.name == "mesh1").map(|m| m.status);

        // The view before the receipt names mesh1 live; the backbone holds the meshes it names.
        let view = RwLock::new(project("fabric1", &fabric, ProviderKind::Process, &digest_view, &records));
        records.install_read(&me, &view, &digest_view, &fabric, rebuild).await;
        let live_before = status_of(&*view.read().await);
        records.set_backbone(backbone.clone(), &*view.read().await);
        let lifecycle = GossipLifecycle { topology_view: topology_view.clone(), backbone: backbone.clone(), me: me.clone(), mesh_deleter: Arc::new(std::sync::OnceLock::new()), project: None, status_door: Default::default() };

        let root = tracing::info_span!("cell.mesh-delete-view", cell);
        let trace_id = trace_id(&root);
        let after_receipt = async {
            // The receipt, as record_mesh_deleted takes it: the mesh held deleted, the view installed again.
            records.hold_mesh_deleted(&mesh1);
            records.install_read(&me, &view, &digest_view, &fabric, rebuild).await;
            let after_receipt = status_of(&*view.read().await);
            // At once: the node.delete's stop of the last admin, from its matching stopped reply.
            let stop = LifecycleOp { build_id: "3vzt99cnx0xe".into(), attempt: 1, operation: format!("node.stop:{LAST}"), node_id: last.node.node_id.clone(), incarnation: last.node.incarnation.clone(), name: last.node.name.clone(), event_at_rafka_ms: 1_000_100, traceparent: None, tracestate: None };
            lifecycle.stopped(&stop).await;
            after_receipt
        }
        .instrument(root)
        .await;
        drop(backbone);
        ep.close().await;
        (trace_id, live_before, after_receipt)
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_delete_view").join(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("cell={cell} trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let mine = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<Value>>();

    assert_eq!(live_before, Some(ScopeStatus::Ready), "before the receipt the view names mesh1 live");
    assert_eq!(after_receipt, Some(ScopeStatus::Dead), "the view installed at the receipt holds mesh1 deleted");
    let event = mine("rdm.mesh.topology.update.via-status-event").into_iter().find(|s| attr(s, "subject") == LAST && attr(s, "frame") == "node-stopped");
    let event = event.unwrap_or_else(|| panic!("the stop of {LAST} was published as a status event in the cell's trace {trace_id}"));
    assert_eq!(attr(&event, "role"), "executor", "the fabric-primary stops the deleted mesh's last admin as executor and never stamps a deleted mesh's topology: {event}");
    let published: Vec<Value> = mine("rdm.mesh.backbone.update.via-deleted-mesh");
    assert!(published.is_empty(), "the fabric-primary never publishes a deleted mesh's topology: {published:?}");
}
