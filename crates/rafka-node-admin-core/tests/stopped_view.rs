//! A stop the executor applies to its own book is in its view before the operation's next step
//! decides from it (node-replace.md step 4: the successor is stopped, then renamed at the cutover;
//! node-name-update.md: the rename's precondition is "the node admits no connection: stopped"). The
//! mesh-primary's `GossipLifecycle::stopped` publishes the stop's status event, which applies it to
//! the book first (`Backbone::publish_status_event`), and then installs its view at once
//! (`admin::view_projector`, the same install every view refresh runs); `node.name.update` reads that
//! installed view (`node_name_update::update`).
// @feature: node-lifecycle

use crate::mesh_delete_resume::{capture, trace_id};
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{Backbone, Membership};
use rafka_node_admin_core::admin::{project, view_projector, GossipLifecycle, Records};
use rafka_node_admin_core::deployment::pipeline::LifecycleEvents;
use rafka_node_admin_core::model::{NodeStatus, PathName, ProviderKind};
use rafka_node_admin_core::storage::{MemoryNodesStorage, NodeRow, NodesStorage};
use rdm_core::status::{StatusReply, StatusRequest};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::Instrument as _;

const MP: &str = "mesh1.admin.1";
const SUCCESSOR: &str = "mesh1.rpc.1.new";

fn digest(fabric: &FabricId, name: &str, mesh_id: &MeshId) -> MeshDigest {
    MeshDigest {
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
        status: MemberStatus::ReadyForTraffic,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000_000,
        data_dir: None,
        mesh_id: Some(mesh_id.clone()),
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

/// CONTRACT (node-replace.md step 4, node-name-update.md precondition "stopped"): mesh1's primary
/// executes the stop of the replace's successor `mesh1.rpc.1.new`. Its view was last installed while
/// the successor was Leaving (the stop's admission). `GossipLifecycle::stopped`, wired as the admin
/// wires it (`project` = `view_projector` over the admin's own view, book and records), publishes the
/// stop and returns; at once, with no refresh of the view in between, the cutover's `node.name.update`
/// of the successor to `mesh1.rpc.1` is decided from the installed view: it lists the successor
/// stopped, and the rename ends `NameUpdated` (`via-writer` `traffic=Stopped`). What must NOT
/// happen: `RejectedAdmittingConnections { status: Leaving, held_connections: [] }` from a view still
/// holding the successor Leaving (the full node_replace stem's red at RenameSuccessor, 2 of 17).
#[test]
fn a_stop_the_executor_applied_is_in_its_view_before_the_cutover_rename_decides_from_it() {
    let cell = "a_stop_the_executor_applied_is_in_its_view_before_the_cutover_rename_decides_from_it";
    let cap = capture();
    let (trace_id, admitted_before, reply) = cap.run(async {
        let fabric = FabricId::mint();
        let mesh1 = MeshId::mint();
        let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let membership = Membership::join(&gossip, &ep, &fabric, "mesh1", &mesh1, MP, Arc::new(crate::adopted_time()), vec![]).await.unwrap();
        let own = digest(&fabric, MP, &mesh1);
        let succ = digest(&fabric, SUCCESSOR, &mesh1);
        let backbone = Backbone::join(&gossip, &ep, &membership, "mesh1", MP, own.node.incarnation.clone(), vec![]).await.unwrap();
        backbone.set_mesh_primary(true);
        backbone.set_fabric_primary(true);
        assert!(membership.book.record(own.clone()) && membership.book.record(succ.clone()), "the book holds the admin and the successor");
        let book = membership.book.clone();
        let records = Arc::new(Records::default());
        let me: PathName = MP.parse().unwrap();
        let view = Arc::new(RwLock::new(project("fabric1", &fabric, ProviderKind::Process, &book, &records)));
        let install = view_projector(me.clone(), view.clone(), "fabric1".into(), fabric.clone(), ProviderKind::Process, book.clone(), records.clone());
        install().await;
        assert!(view.read().await.holds_mesh_seat("mesh1", &own.node.node_id), "{MP} holds mesh1's seat in its view");
        let lifecycle = GossipLifecycle { membership: membership.clone(), backbone: backbone.clone(), me: me.clone(), mesh_deleter: Arc::new(std::sync::OnceLock::new()), project: Some(install.clone()) };

        // The successor's row at its create spec's name, and the door its record changes take turns at.
        let rows = MemoryNodesStorage::default();
        let row = NodeRow { name: succ.node.name.clone(), node_id: succ.node.node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None };
        rows.put_node_row(&row).await.unwrap().unwrap();
        let client = Arc::new(rafka_node_rpc::NodeRpcClient::new(ep.clone(), Arc::new(rafka_node_rpc::StaticResolver::new())).with_caller_system("rdm"));
        let configs = rafka_node_admin_core::node_config::NodeConfigs::new(client, Default::default(), Default::default());
        let (moved, rename): (rafka_node_admin_core::status_rpc::RowMoved, rafka_node_admin_core::status_rpc::Rename) = Default::default();
        // The view takes a moved row as the admin wires it: projected at once.
        let reproject = install.clone();
        let _ = moved.set(Arc::new(move |_from: PathName, _node_id: NodeId, _incarnation: IncarnationId, _to: PathName| reproject()));
        let first_ready = rafka_node_admin_core::fabric_state::FirstReady::new();
        first_ready.reach("the cell's fabric reached ready-for-traffic");
        let status = rafka_node_admin_core::status_storage::MemoryStatusStorage::default();

        let root = tracing::info_span!("cell.stopped-view", cell);
        let trace_id = trace_id(&root);
        let (admitted_before, reply) = async {
            // The stop's admission: the successor Leaving, and a view installed from it.
            let stop = LifecycleOp { build_id: "3vzt99cnx0xe".into(), attempt: 3, operation: format!("node.stop:{SUCCESSOR}"), node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation.clone(), name: succ.node.name.clone(), event_at_rafka_ms: 1_000_100 };
            lifecycle.leaving(&stop).await;
            install().await;
            let admitted_before = view.read().await.own(&succ.node.node_id).map(|n| (n.status, n.parked));
            // The node's stopped reply: the executor's stop, then at once the cutover's rename.
            lifecycle.stopped(&stop).await;
            let req = StatusRequest::UpdateName { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation.clone(), name: "1".into() };
            let doors = rafka_node_admin_core::node_name_update::NameDoors { configs: Some(&configs), moved: &moved, rename: &rename };
            let on_root = rafka_node_admin_core::node_properties::FirstReadyOnRoot { first_ready: &first_ready, status: &status };
            let reply = rafka_node_admin_core::node_name_update::update(&me, &own.node.node_id, &view, &rows, doors, None, on_root, &req, None).await;
            (admitted_before, reply)
        }
        .instrument(root)
        .await;
        drop(backbone);
        ep.close().await;
        (trace_id, admitted_before, reply)
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/stopped_view").join(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("cell={cell} trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let mine = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<Value>>();
    assert_eq!(admitted_before.map(|(_, stopped)| stopped), Some(false), "before the stop the installed view holds the successor admitted, not stopped: {admitted_before:?}");
    assert!(matches!(reply, StatusReply::NameUpdated { .. }), "the cutover rename right after the stop decides from a view listing the successor stopped: {reply:?}");
    let writer = mine("rdm.node_admin.node.name.update.via-writer");
    assert_eq!(writer.len(), 1, "{writer:?}");
    assert_eq!((attr(&writer[0], "from"), attr(&writer[0], "to"), attr(&writer[0], "traffic"), attr(&writer[0], "outcome")), (SUCCESSOR, "mesh1.rpc.1", "Stopped", "name-updated"), "{writer:?}");
    let stopped = mine("rdm.node_admin.node.update.via-node-stopped");
    assert_eq!(stopped.len(), 1, "{stopped:?}");
    eprintln!("cell={cell} trace {trace_id}: via-node-stopped node={SUCCESSOR}, then via-writer from={SUCCESSOR} to=mesh1.rpc.1 traffic=Stopped outcome=name-updated");
}
