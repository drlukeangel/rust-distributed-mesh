//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`rdm_admin::entry::run_on`). All configuration is the environment.
//!
//! It is the primer for the seam a product's node-admin plugs in besides its hooks and channels: its
//! own storage backend, handed over through `run_on`. [`ConsumerBackend`] is asked for every boundary
//! the admin opens (its own row, then the six boundaries), here by delegating to RDM's file backend; a
//! product's own backend (Rafka's topic-backed one) takes the file backend's place. A product's work on
//! a node's way into traffic enters only through the Fabric and node hooks (`pre_node_ready`,
//! `on_node_ready`, `on_mesh_ready`, `on_fabric_ready`, `on_build_started`, `on_build_ended`; build.md §9).
//! This admin registers the five Fabric notices ([`notices`]), each one log line (target `rshape`) with
//! what it was told: the notices gate nothing, and a product puts its own work where the line is.
//! `on_node_investigated` also acts on `recreate`, as Rafka's node-admin takes a re-created broker out
//! of its topics' write sets: the admin records the re-creation in the replica's `system` topic through
//! a gateway, under an operation id of the investigated birth, so a repeated notice appends nothing twice.
//! `on_fabric_ready` also does the replica's own day-0 work, as Rafka's does its system creations: it
//! produces one record to the `system` topic through a gateway ([`rshape_consumer::replica`]), under
//! an operation id of the ready Build, so a notice repeated for one Build appends nothing twice; then
//! it opens a data connection to each Ready broker of its mesh on the shape's data protocol
//! (`rshape_consumer::data_alpn`), as Rafka's admin talks to its brokers. The admin registers that
//! protocol and answers every stream a node opens to it (`echo <admin's node id>: <bytes>`), as the role nodes do.
use async_trait::async_trait;
use rdm_admin::model::{IncarnationId, ProviderKind};
use rdm_admin::storage::NodeRecord;
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope};
use rdm_admin::storage_error::StorageError;
use rdm_admin::wiring::{BuildEndedCtx, BuildEndedNotice, BuildStartedCtx, BuildStartedNotice, FabricHooks, FabricReadyCtx, FabricReadyNotice, MeshReadyCtx, MeshReadyNotice, NodeInvestigatedCtx, NodeInvestigatedNext, NodeInvestigatedNotice, Wiring};
use rshape_consumer::replica::{ask_each_gateway, Asked, Records, RecordsRequest};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;

const SERVICE: &str = "rshape-node-admin";

/// The consumer's storage backend: every boundary RDM's node-admin opens is asked of it.
struct ConsumerBackend(Arc<dyn StorageBackend>);

#[async_trait]
impl StorageBackend for ConsumerBackend {
    fn qualify(&self, provider: ProviderKind) -> Result<(), String> {
        self.0.qualify(provider)
    }

    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        self.0.own_row(data_dir, writer).await
    }

    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
        self.0.put_own_row(data_dir, writer, record).await
    }

    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
        // Where the boundaries write, as the boot's storage span records it: this backend, over the file
        // backend's own answer.
        self.0.open(scope).await.map(|b| StorageBoundaries { writes_to: format!("rshape-backend:{}", b.writes_to), ..b })
    }
}

/// What the started admin hands over (`rdm_admin::entry::run_on_started`): its Node RPC client, its
/// topology view and its mesh, where `on_fabric_ready` finds a gateway and calls it. A watch, so a
/// notice that comes before the admin is started waits for it.
type Own = Arc<tokio::sync::watch::Sender<Option<Arc<Started>>>>;

/// The started admin, once it is: waits for [`Own`] to be set.
async fn wait_started(own: &Own) -> Option<Arc<Started>> {
    own.subscribe().wait_for(Option::is_some).await.ok().and_then(|s| s.clone())
}

/// The ready gateways of the admin's mesh, node id and incarnation, in the topology's order.
fn ready_gateways(started: &Started) -> Vec<(rdm_core::NodeId, rdm_core::IncarnationId)> {
    let filter = rdm_core::filter::Filter::new().eq("mesh", [started.mesh.clone()]).eq("type", ["gateway"]).eq("status", ["ready"]);
    started.view.nodes_get(&filter).unwrap_or_default().into_iter().map(|d| (d.node.node_id, d.node.incarnation)).collect()
}

/// Ask `gateway` for `request` (op [`Records`]); RDM resends it ([`rdm_transport::Send`]).
async fn ask_gateway(started: &Started, gateway: rdm_core::NodeId, request: &RecordsRequest) -> Asked {
    let (outcome, _) = started.client.call::<Records>(&rdm_transport::NodeTarget::ExactNode(gateway), request, &rdm_transport::CallOptions::default()).await;
    Asked::of(outcome)
}

/// The started admin's handles the notices use.
struct Started {
    client: Arc<rdm_transport::NodeRpcClient>,
    view: rdm_transport::topology::TopologyView,
    mesh: String,
    node_id: String,
    /// Its dialer on the shape's data protocol.
    dialer: rdm_transport::connections::Dialer,
}

/// Answer every stream a node opens to this admin on the data protocol: `echo <admin's node id>: <bytes>`.
async fn answer(mut acceptor: rdm_transport::connections::Acceptor, own: String) {
    while let Some(conn) = acceptor.accept().await {
        let own = own.clone();
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let Ok(bytes) = recv.read_to_end(1 << 20).await else { break };
                let peer = conn.peer();
                tracing::info!(target: "rshape", node = %own, peer_path = %peer.path, peer_name = %peer.name, bytes = bytes.len(), "a data-plane stream answered by the node-admin");
                if send.write_all(format!("echo {own}: {}", String::from_utf8_lossy(&bytes)).as_bytes()).await.is_err() || send.finish().is_err() {
                    break;
                }
            }
        });
    }
}

/// Open a data connection to each Ready broker of the admin's mesh and write one stream; each broker's
/// answer is a log line. A broker that cannot be reached is named.
async fn talk_to_brokers(started: &Started) {
    let filter = rdm_core::filter::Filter::new().eq("mesh", [started.mesh.clone()]).eq("type", ["broker"]).eq("status", ["ready"]);
    for broker in started.view.nodes_get(&filter).unwrap_or_default() {
        let to = rdm_transport::connections::Birth { mesh: broker.node.mesh.clone(), r#type: broker.node.r#type.clone(), path: broker.node.path.clone(), name: broker.node.name.clone(), node_id: broker.node.node_id.clone(), incarnation: broker.node.incarnation.clone() };
        let answered = async {
            let conn = started.dialer.open(&to).await.map_err(|e| format!("{e:?}"))?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| format!("{e:?}"))?;
            send.write_all(format!("from {}", started.node_id).as_bytes()).await.map_err(|e| format!("{e:?}"))?;
            send.finish().map_err(|e| format!("{e:?}"))?;
            let bytes = recv.read_to_end(1 << 16).await.map_err(|e| format!("{e:?}"))?;
            Ok::<String, String>(String::from_utf8_lossy(&bytes).into_owned())
        };
        match tokio::time::timeout(Duration::from_secs(10), answered).await {
            Ok(Ok(answer)) => tracing::info!(target: "rshape", broker_path = %to.path, broker_name = %to.name, %answer, "the node-admin's data connection to a broker answered"),
            Ok(Err(e)) => tracing::warn!(target: "rshape", broker_path = %to.path, broker_name = %to.name, error = %e, "the node-admin's data connection to a broker failed"),
            Err(_) => tracing::warn!(target: "rshape", broker_path = %to.path, broker_name = %to.name, "the node-admin's data connection to a broker did not answer within 10 s"),
        }
    }
}

/// The day-0 work of `ready`: one record to the `system` topic through a Ready gateway of this admin's
/// mesh. Each ready gateway is asked once, in order ([`ask_each_gateway`]); a gateway whose call is
/// spent is never asked it again. When no gateway produced it, the next round waits for the topology
/// to change (`DigestView::heard_changes`: a member first held, a new birth or status, or heard again
/// after silence), never a timer. The operation id is the ready Build's, so a repeat of the notice for
/// one Build is the same record.
async fn day_zero(own: Own, fabric_id: String, build_id: String) {
    let operation = format!("fabric-ready:{build_id}");
    let request = RecordsRequest::Produce { operation: operation.clone(), topic: "system".into(), record: format!("fabric {fabric_id} ready at Build {build_id}") };
    let Some(started) = wait_started(&own).await else { return };
    let mut heard = started.view.digest_view.heard_changes();
    let mut spent = BTreeMap::new();
    loop {
        // Marked seen before the view is read: a change after this read wakes the next round.
        heard.borrow_and_update();
        match ask_each_gateway(&ready_gateways(&started), &mut spent, |gateway| ask_gateway(&started, gateway, &request)).await {
            Ok(offset) => {
                tracing::info!(target: "rshape", %operation, offset, "on_fabric_ready: the day-0 record is produced through a gateway");
                return talk_to_brokers(&started).await;
            }
            Err(answers) => tracing::info!(target: "rshape", %operation, ?answers, "on_fabric_ready: no gateway produced the day-0 record; the next round waits for the topology to change"),
        }
        if heard.changed().await.is_err() {
            return;
        }
    }
}

/// `on_node_investigated`'s work on `recreate`: one record of the re-creation to the `system` topic,
/// one round of the ready gateways of this admin's mesh ([`ask_each_gateway`]): each asked once, in
/// the topology's order, until one produces it. The operation id is the investigated birth's, so a
/// repeated notice for one birth is the same record.
async fn record_recreate(started: &Started, told: &NodeInvestigatedCtx) {
    let operation = format!("node-recreate:{}:{}", told.node_id, told.incarnation.as_str());
    let request = RecordsRequest::Produce {
        operation: operation.clone(),
        topic: "system".into(),
        record: format!("path {:?}, name {:?}, node {} investigated ({}): re-created as a new node id", told.path, told.name, told.node_id, told.verdict.as_str()),
    };
    match ask_each_gateway(&ready_gateways(started), &mut BTreeMap::new(), |gateway| ask_gateway(started, gateway, &request)).await {
        Ok(offset) => tracing::info!(target: "rshape", %operation, offset, "on_node_investigated: the re-creation is recorded in the system topic"),
        Err(answers) => tracing::warn!(target: "rshape", %operation, ?answers, "on_node_investigated: the re-creation is not recorded: no gateway produced it"),
    }
}

/// The Fabric notices this admin registers (fabric-node-hooks.md): each is fire and forget and writes one
/// log line naming what it was told. `on_mesh_ready` is called by a mesh-primary once its mesh is ready,
/// `on_node_investigated` by a mesh-primary once it records a node investigation's verdict;
/// `on_fabric_ready`, `on_build_started` and `on_build_ended` by the fabric-primary.
fn notices(own: Own) -> FabricHooks {
    let own_investigated = own.clone();
    let on_mesh_ready: MeshReadyNotice = Arc::new(|ready: MeshReadyCtx| {
        Box::pin(async move { tracing::info!(target: "rshape", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, "on_mesh_ready: the mesh is ready") })
    });
    let on_fabric_ready: FabricReadyNotice = Arc::new(move |ready: FabricReadyCtx| {
        let own = own.clone();
        Box::pin(async move {
            tracing::info!(target: "rshape", fabric_id = %ready.fabric_id, build_id = %ready.build_id, attempt = ready.attempt, "on_fabric_ready: the fabric is ready");
            // The work runs on its own: a notice gates nothing.
            tokio::spawn(day_zero(own, ready.fabric_id.to_string(), ready.build_id.clone()));
        })
    });
    let on_build_started: BuildStartedNotice = Arc::new(|started: BuildStartedCtx| {
        Box::pin(async move {
            tracing::info!(target: "rshape", fabric_id = %started.fabric_id, build_id = %started.build_id, attempt = started.attempt, reason = started.reason.as_str(), "on_build_started: a Build started")
        })
    });
    let on_build_ended: BuildEndedNotice = Arc::new(|ended: BuildEndedCtx| {
        Box::pin(async move {
            match &ended.state {
                rdm_admin::wiring::BuildEndedState::Complete => {
                    tracing::info!(target: "rshape", fabric_id = %ended.fabric_id, build_id = %ended.build_id, attempt = ended.attempt, reason = ended.reason.as_str(), state = ended.state.as_str(), "on_build_ended: a Build ended")
                }
                rdm_admin::wiring::BuildEndedState::Failed { step, reason, code } => {
                    tracing::warn!(target: "rshape", fabric_id = %ended.fabric_id, build_id = %ended.build_id, attempt = ended.attempt, reason = ended.reason.as_str(), state = ended.state.as_str(), step = %step, failure = %reason, code = ?code, "on_build_ended: a Build ended")
                }
            }
        })
    });
    let on_node_investigated: NodeInvestigatedNotice = Arc::new(move |told: NodeInvestigatedCtx| {
        let own = own_investigated.clone();
        Box::pin(async move {
            tracing::info!(target: "rshape", fabric_id = %told.fabric_id, mesh_id = %told.mesh_id, node_id = %told.node_id, node_path = %told.path, node_name = %told.name, verdict = told.verdict.as_str(), next = told.next.as_str(), "on_node_investigated: a node investigation's verdict is recorded");
            if told.next != NodeInvestigatedNext::Recreate {
                return;
            }
            // The work runs on its own, under the notice's span: a notice gates nothing.
            tokio::spawn(
                async move {
                    if let Some(started) = wait_started(&own).await {
                        record_recreate(&started, &told).await;
                    }
                }
                .instrument(tracing::Span::current()),
            );
        })
    });
    FabricHooks { on_mesh_ready: Some(on_mesh_ready), on_fabric_ready: Some(on_fabric_ready), on_build_started: Some(on_build_started), on_build_ended: Some(on_build_ended), on_node_investigated: Some(on_node_investigated) }
}

#[tokio::main]
async fn main() {
    let own: Own = Arc::new(tokio::sync::watch::channel(None).0);
    let started = own.clone();
    rdm_admin::entry::run_on_started(
        SERVICE,
        // Its backend wraps RDM's file backend, registered under that backend's name (storage.md §2).
        |cfg| rdm_admin::storage_backend::TopologyStorageProviders::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, Arc::new(ConsumerBackend(rdm_admin::storage_backend::file_backend(cfg.store_dir.clone())))),
        |cfg| Wiring { fabric_hooks: notices(own), alpns: vec![rshape_consumer::data_alpn()], certs: rdm_admin::entry::material_or_exit(SERVICE, cfg), ..Wiring::default() },
        // The started admin hands its client, view and data-protocol connections over to the day-0
        // work, and answers the streams nodes open to it.
        move |running| {
            let birth = running.birth();
            let Ok(connections) = running.connections(&rshape_consumer::data_alpn().name) else {
                return tracing::error!(target: "rshape", "the node-admin holds no data-protocol handle: it answers no stream and talks to no broker");
            };
            let (dialer, acceptor) = connections.split();
            tokio::spawn(answer(acceptor, birth.node_id.to_string()));
            started.send_replace(Some(Arc::new(Started { client: running.node_rpc.client.clone(), view: running.topology_view.clone(), mesh: birth.mesh.clone(), node_id: birth.node_id.to_string(), dialer })));
        },
    )
    .await
}
