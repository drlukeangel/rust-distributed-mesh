//! A node birth serving the commands of its owning mesh-admin: `node.connection.delete`,
//! `node.stop` and `node.start` (node-connections-delete.md, node-stop.md, node-start.md). Every node
//! kind runs this one logic over its own surface: a node, a role node and a node-admin differ
//! only in how they refuse new work, how they publish and how they leave and rejoin the mesh (their
//! [`Session`]).
//!
//! - `DeleteConnections`: `All` blocks admission of new application work, lets the eligible work in
//!   flight finish under the birth's bound ([`NodeSelf::connections_delete_deadline`], from its launch or
//!   its admin config), closes every open Direct or Proxy entry
//!   of the connections log and answers `ConnectionsDeleted` with its `ConnectionsReceipt`
//!   (`Established`, or `Deadline` at the bound). `Connections` closes exactly the named entries.
//!   Gossip neighbours and the Node RPC pool are not connections: neither scope touches them.
//! - `StopNode` (a workflow over its primitives, Q205): the whole `node.connection.delete { All }`,
//!   streaming its acknowledgement `ConnectionsDeleting` and its completion `StopConnectionsDeleted`
//!   with the `All`'s receipt (Data), then hold the node's own tasks, publish a mesh-primary
//!   node-admin's last mesh topology with itself `Leaving` (gossip.md, "Mesh delete"), then
//!   `node.mesh.leave` (gossip neighbours and the Node RPC pool; never the endpoint and never the
//!   commander's connection the call rides), streaming its acknowledgement `Leaving` and its completion
//!   `StopMeshLeft` (Data), and answer `NodeStopped` (Terminal) on that stream. A primitive the birth
//!   already completed since it last started is answered at once and not run again. The process stays
//!   alive, its endpoint bound.
//! - `StartNode`: a stopped birth rejoins as itself (same node id, incarnation, endpoint key and
//!   port) through its [`Session`], and answers `Started`, or `StartFailed` naming the step.
//! - `Reserved6` and `Reserved7` are reserved Status positions (R-CR4): refused by name.
//!
//! Nothing here waits on an answer for correctness: a lost reply is the commander's indeterminate,
//! never this node's retry.

use crate::connections_writer::HeldConnections;
use crate::model::{IncarnationId, NodeId, PathName};
use rdm_core::connections::{ConnectionKind, ConnectionState, NodeConnection};
use rdm_core::views::{ConnectionView, ConnectionsView};
use rdm_core::{LifecycleOp, MeshDigest};
use rdm_gossip::topology::TopologyView;
use rdm_transport::{NodeRpcClient, NodeRpcServer};
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, NodeState, StatusReply, StatusRequest, WhichMesh};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Whether this process is stopped: it was stopped and not yet started. A stopped process exits at
/// once on a termination signal (nothing is left to wait for); only a delete ends it.
pub struct ProcessStop {
    stopped: std::sync::atomic::AtomicBool,
}

/// The process's stopping.
pub fn process_stop() -> &'static ProcessStop {
    static PROCESS_STOP: std::sync::OnceLock<ProcessStop> = std::sync::OnceLock::new();
    PROCESS_STOP.get_or_init(|| ProcessStop { stopped: std::sync::atomic::AtomicBool::new(false) })
}

impl ProcessStop {
    /// A `stop-node` completed: the process is stopped.
    pub fn stop(&self) {
        self.stopped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// A `start-node` rejoined: the process is live again.
    pub fn start(&self) {
        self.stopped.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    /// Whether the process is stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// What a hard cut closed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CutReport {
    /// Gossip neighbours the node held on its mesh channel before the cut.
    pub neighbours: usize,
    /// Pooled peer connections this node had dialled, closed.
    pub pooled: usize,
    /// Connections peers had made to this node, closed (the commander's spared).
    pub accepted: usize,
    /// Turns of the node's gated tasks in progress when the cut began (a node-admin's stop gate,
    /// quiesced by the stop before it; an ordinary node holds none).
    pub turns_in_progress: usize,
}

/// What holding a node's own tasks for its stop waited for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TasksStopped {
    /// Turns of its gated tasks already past their wait point when they were held; the stop waited
    /// for each to end.
    pub turns_in_progress: usize,
    /// Those turns by task (`task` or `task:n`, comma-separated); empty when there were none.
    pub waited_for: String,
}

/// What a `node.connection.delete` did at this birth (R-CR3): the connections it cut, and every
/// connection id it accounts for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Deleted {
    /// The open entries closed, their drops written under the name the birth holds.
    pub closed: Vec<rdm_core::ConnectionId>,
    /// The ids named that are no open entry of the birth: unknown, or already closed.
    pub not_open: Vec<rdm_core::ConnectionId>,
    /// The open entries the birth still holds after the cut, a drop the storage refused included.
    pub held: Vec<rdm_core::ConnectionId>,
    /// For `All`, what the closure established; `None` for `Connections`.
    pub receipt: Option<ConnectionsReceipt>,
}

/// What waiting for the eligible work in flight established.
enum InFlight {
    /// It reached zero: `began` was the count when the wait began.
    Zero { began: u64 },
    /// The bound passed: `last` was the last count observed.
    Deadline { last: u64 },
}

/// The reason every drop `node.connection.delete` writes carries.
pub const CONNECTIONS_DELETE: &str = "node.connection.delete";

/// Why a start stopped, and at which step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartFailed {
    /// `node.join`, `node.pre-node-ready`, `node.topology` or `node.rafka-time`.
    pub step: &'static str,
    /// Why.
    pub reason: String,
}

impl std::fmt::Display for StartFailed {
    /// The reason: it names the node, the step's authority and what failed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for StartFailed {}

/// A first start whose refusal is a string (a node-admin's) carries the step's reason.
impl From<StartFailed> for String {
    fn from(failed: StartFailed) -> Self {
        failed.reason
    }
}

/// How a process leaves its mesh when stopped and joins it again when started: the part of
/// stop and start that differs by node type.
#[async_trait::async_trait]
pub trait Session: Send + Sync {
    /// Hold this process's own tasks for its stop, once its connections delete is done: no turn begins, and this
    /// resolves once every turn already past its wait point has ended, so nothing a turn does
    /// follows the stop's declaration or its cut. A surface whose tasks need no holding has nothing
    /// to wait for. `commanded_by` is the Build whose run on this node commanded this stop (a node
    /// executing its own node.delete, fabric-delete.md step 4): that run and the work it runs under
    /// wait on this stop, so it never waits for them; `None` for every other stop.
    async fn hold_tasks(&self, _commanded_by: Option<&str>) -> TasksStopped {
        TasksStopped::default()
    }
    /// The endpoint keys of the callers Build `build`'s run on this node answers (the calls reading
    /// its frames): a stop that run commands spares their connections at its cut, as every stop
    /// spares the call it answers on (node-stop.md "Transport cut"). None on a surface that runs no
    /// Build.
    fn callers_of(&self, _build: &str) -> Vec<iroh::PublicKey> {
        Vec::new()
    }
    /// Hard-cut the mesh connections: leave the gossip channels, close the pooled peer connections and
    /// the connections peers made, sparing the connections from `spare` (the commanders whose calls
    /// are in flight and answered on their connections) and, a stop's, the pooled connection to
    /// `mesh_primary` its owed declarations ride (node-stop.md "Transport cut"). The endpoint stays
    /// bound.
    async fn cut(&self, spare: &[iroh::PublicKey], mesh_primary: Option<iroh::PublicKey>) -> CutReport;
    /// Rejoin as the same birth: join the authority, rejoin the mesh channel, take the topology and
    /// run the `pre_node_ready` hook with `AfterStop` in its context under `delete`, the new cycle's token.
    /// `commander` holds the endpoint key of the call that commanded the start last: each join
    /// attempt goes back over that call ([`crate::workflows::node_start::JoinTo::Commander`]), and
    /// the admin that answers is the hook's accepting authority. `rows` is the start's hold on this
    /// birth's connections rows: the session drops it once it holds the name its join answer handed,
    /// and no earlier, so the join's connection is recorded under that name (node-start.md step 2).
    /// `operation` is the start's own: the Ready the rejoin declares names it (i146 Q36).
    /// `held_again` is sent once the start holds its mesh again (node-mesh-join.md step 9: back on its
    /// channel with a neighbour, its Pending presence published): what a commanded `node.mesh.join` answers on.
    async fn rejoin(&self, birth_cancel: rdm_transport::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>, operation: rdm_core::status::DeclaredOperation, progress: Option<CommandProgress>, held_again: Option<tokio::sync::oneshot::Sender<()>>) -> Result<(), StartFailed>;
    /// Leave the channels this birth is on (its mesh; the backbone too, a node-admin), its hook task
    /// ended and its hook gate closed, its connections kept: what a live birth does before its start
    /// sequence runs again on a commanded `node.mesh.join` (node-mesh-join.md step 10, Q87). A stop's
    /// cut does this and then cuts the connections.
    async fn leave_channels(&self);
}

/// Declares a state this birth entered to its mesh-primary, with the operation that produced it when
/// one did (i146 Q36). A node owes it when this is called, in that order (Q210), and the future
/// ends once its call ended.
pub type Declare = Arc<dyn Fn(NodeState, Option<rdm_core::status::DeclaredOperation>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The operation a stop by this process's own signal names on its leave step's spans.
pub const SIGNAL_STOP: &str = "signal";

/// Who a stop answers to (i146 Q55): the mesh-primary's `StopNode` on its command stream, this
/// process's own signal, the node its own subject, or a commanded start that failed at a step. One
/// workflow serves them all ([`NodeSelf::stop_steps`]).
enum StopBy<'a> {
    /// The command: who commanded it, the connection its call rides, its operation and its stream.
    Command { commander: Option<&'a NodeId>, spare: Option<iroh::PublicKey>, op: &'a LifecycleOp, progress: Option<CommandProgress> },
    /// This process's own signal.
    Signal,
    /// A commanded start that failed at a step (node-start.md: `CommandAccepted`, then `Failed { step,
    /// reason, code }`, nothing downstream): the birth is stopped again, as it was, the stop naming the
    /// start's operation. The start's stream carries only its `Failed`; `calls`, every call commanding
    /// the start, are in flight and spared, so each is answered.
    FailedStart { commander: Option<&'a NodeId>, calls: &'a [iroh::PublicKey], op: &'a LifecycleOp },
}

/// What a stop by this process's own signal leaves behind (i146 Q55): the receipt of its
/// `node.connection.delete { All }`. Each declaration it owed its mesh-primary has ended by then
/// (Q206, [`NodeSelf::stop_on_signal`]).
pub struct SignalStopped {
    /// The `All`'s receipt.
    pub receipt: ConnectionsReceipt,
}

/// The operation `op` names, as a declaration carries it (i146 Q36).
pub(crate) fn declared(op: &LifecycleOp) -> rdm_core::status::DeclaredOperation {
    rdm_core::status::DeclaredOperation { operation: op.operation.clone() }
}

/// Where a birth stands between a stop and a start.
enum Cycle {
    /// Running: a stop has not been admitted. A birth a start brought back holds that start's
    /// operation, its identity (Q342): its reissue answers `AlreadyApplied` (node-rpc-envelope.md:
    /// "Reissuing a naturally idempotent uncertain step can settle it with AlreadyApplied"); its
    /// node.joined and node.started were taken when it ran. A birth that never stopped holds `None`.
    Live(Option<rdm_core::OperationId>),
    /// A stop is under way; later stops of the birth wait for the same receipt.
    Stopping(tokio::sync::watch::Receiver<Option<ConnectionsReceipt>>),
    /// Stopped, its process alive, with the receipt its node.connection.delete { All } established.
    Stopped(ConnectionsReceipt),
    /// Stopped and a start is under way (the natural key `(node_id, incarnation, Pending)`): later
    /// starts of the birth answer the same outcome, and a stop is refused naming the start. A failed
    /// start leaves the birth stopped again with the receipt it was stopped with.
    Starting {
        /// The outcome of the start under way, once it ends.
        outcome: tokio::sync::watch::Receiver<Option<StatusReply>>,
        /// The endpoint key of the call that commanded the start last: the start's join goes back
        /// over it (node-start.md step 2). A later start's call hands its own key here. `None` for a
        /// start that came on no Node RPC call.
        commander: Option<tokio::sync::watch::Sender<iroh::PublicKey>>,
        /// The endpoint keys of every call commanding the start, each awaiting its answer: a failed
        /// start that stops the birth again spares their connections, so each call is answered.
        calls: Vec<iroh::PublicKey>,
    },
}

/// What this birth's primitives completed since it last started or joined (Q205): a stop of a birth
/// already connections-deleted and left answers each primitive's acknowledgement and completion at
/// once and repeats no work (node-stop.md step 3).
#[derive(Default)]
struct Completed {
    /// The receipt of an `All` that completed `Established`: from it on the birth holds no connection
    /// and opens none until it starts or joins again.
    deleted: Option<ConnectionsReceipt>,
    /// The meshes its node.mesh.leave left.
    left: Option<Vec<WhichMesh>>,
}

/// The surface a node type gives the commands.
pub struct NodeSelf {
    /// This birth's immutable type.
    pub r#type: String,
    /// This birth's logical node.
    pub node_id: NodeId,
    /// This birth's incarnation.
    pub incarnation: IncarnationId,
    /// This birth's path at its birth; the name it holds is [`Self::held_name`].
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// This birth's Node RPC server: it refuses new calls while connections-deleting and counts the work in flight.
    pub server: NodeRpcServer,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// The process's live resolver: an admin that commanded this birth is applied to it at the path
    /// its call came in on, so the completion call goes back there.
    pub resolver: Arc<rdm_transport::LiveNodeResolver>,
    /// This birth's topology view.
    pub topology_view: TopologyView,
    /// This birth's channels: the mesh channel its completion frames go out on.
    pub channels: rdm_gossip::channel::Channels,
    /// Set this birth's status and answer the digest to publish.
    pub set_status: Arc<dyn Fn(rdm_core::model::NodeStatus) -> MeshDigest + Send + Sync>,
    /// This birth's status now.
    pub status: Arc<dyn Fn() -> rdm_core::model::NodeStatus + Send + Sync>,
    /// Declare the state this birth entered to its authority (a node-admin of its mesh); `None` for
    /// a surface whose declarations are made elsewhere (a node-admin).
    pub declare: Option<Declare>,
    /// Cancelled when `node.connection.delete { All }` or `stop-node` is admitted: the birth is
    /// deleted, so its `pre_node_ready` hook and any pull it has in flight end.
    pub birth_cancel: Option<rdm_transport::BirthCancel>,
    /// What this birth owes its mesh-primary, for a signal stop's outcome span to name what it left.
    pub owed: Option<Arc<crate::node::OwedDeclarations>>,
    /// The backbone, for a node-admin: its state hooks go out on its mesh channel and the backbone.
    /// `None` for an ordinary node: its hooks go out on its mesh channel and its mesh-admin carries
    /// them onto the backbone.
    pub backbone: Option<rdm_gossip::backbone::Backbone>,
    /// The bound of this birth's `node.connection.delete { All }`, standalone or inside a stop
    /// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, read once where the process reads its launch or its
    /// admin config): the eligible work in flight is let finish until it passes. Strictly shorter
    /// than the stop ladder's grace.
    pub connections_delete_deadline: Duration,
    /// Where this birth stands between a stop and a start.
    cycle: Mutex<Cycle>,
    /// What its primitives completed since it last started or joined.
    completed: Mutex<Completed>,
    /// How this process leaves and rejoins its mesh; filled once the process holds the parts.
    session: Mutex<Option<Arc<dyn Session>>>,
    /// The client this birth originates its data-plane connections on, the ones its connections log
    /// holds as Direct entries (connections.md "Two planes, two words"); `None` for a process whose
    /// data plane is not a Node RPC client of its own. `node.connection.delete` closes its pooled
    /// connection to the destination of each Direct entry it closes, and nothing of the Node RPC pool.
    data_plane: Mutex<Option<Arc<NodeRpcClient>>>,
    /// The process's connections writer: what this birth holds (R-CR1) and where the drops of a
    /// `node.connection.delete` are written; filled once the process holds it.
    connections: Mutex<Option<Arc<crate::connections_writer::ConnectionsWriter>>>,
    /// The process's data-plane `ConnectionsRegistry`: `node.connection.delete` closes active
    /// `Connection`s by their row id and blocks new connections under `All`.
    data_connections: Mutex<Option<rdm_transport::connections::ConnectionsRegistry>>,
    /// The process's storage change moves (storage.md §2 "Changing it"): filled once the process holds
    /// its storage handle.
    storage: Mutex<Option<Arc<crate::storage_move::StorageMoves>>>,
    /// The process's writer of its own record (Q356): each state a command moves this birth to is
    /// written there before the frame that reports it; filled once the process holds it.
    records: Mutex<Option<crate::node_records::NodePropertyWriter>>,
}

impl NodeSelf {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl NodeSelf {
    /// A node surface over its parts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        incarnation: IncarnationId,
        name: PathName,
        r#type: String,
        server: NodeRpcServer,
        client: Arc<NodeRpcClient>,
        resolver: Arc<rdm_transport::LiveNodeResolver>,
        channels: rdm_gossip::channel::Channels,
        set_status: Arc<dyn Fn(rdm_core::model::NodeStatus) -> MeshDigest + Send + Sync>,
        status: Arc<dyn Fn() -> rdm_core::model::NodeStatus + Send + Sync>,
        declare: Option<Declare>,
        connections_delete_deadline: Duration,
    ) -> Self {
        channels.set_author(&rdm_core::PublisherId { node_id: node_id.clone(), incarnation: incarnation.clone() }, None);
        let topology_view = channels.topology_view().clone();
        Self { r#type, node_id, incarnation, path: name.path.clone(), name: name.name.clone(), server, client, resolver, topology_view, channels, set_status, status, declare, connections_delete_deadline, birth_cancel: None, owed: None, backbone: None, cycle: Mutex::new(Cycle::Live(None)), completed: Mutex::new(Completed::default()), session: Mutex::new(None), data_plane: Mutex::new(None), connections: Mutex::new(None), data_connections: Mutex::new(None), storage: Mutex::new(None), records: Mutex::new(None) }
    }

    /// Hold `records`, this process's writer of its own record.
    pub fn set_records(&self, records: crate::node_records::NodePropertyWriter) {
        *self.records.lock().unwrap() = Some(records);
    }

    /// Write `to` as this birth's status on its own record (Q356) under one `Send`, through the one
    /// status move ([`write_own_status`]), before the frame that reports it,
    /// and publish the status the record then holds at once in its Digest (gossip.md §3.1 `Digest`: "at
    /// once when its status changes"): the one status its record, its Digest and its answers carry. A
    /// write refused or whose `Send` is spent moves nothing: the Digest keeps the record's last status,
    /// and the end is named on the writer's span. A process that holds no record yet publishes `to`,
    /// with nothing recorded to differ from. A Digest sent while the birth is on no channel reaches
    /// nobody; its heartbeats carry the status once it is.
    async fn write_state(&self, to: crate::model::NodeStatus) {
        if let Ok(d) = self.record_state(to).await {
            let _ = self.channels.publish(&d).await;
        }
    }

    /// [`Self::write_state`]'s record write and status set, without the publish: the Digest carrying the
    /// status the record holds, for a caller that publishes it elsewhere first (a stop's last mesh
    /// topology). `Err` when the write was refused or its `Send` spent, named on the writer's span.
    async fn record_state(&self, to: crate::model::NodeStatus) -> Result<MeshDigest, StatusUnwritten> {
        let writer = self.records.lock().unwrap().clone();
        write_own_status(writer.as_ref(), to, |_| {}, |status| (self.set_status)(status)).await
    }

    /// The name this birth holds and publishes: its row's name once it took one (live through
    /// `UpdateName`, or from its join answer), else its birth's (R12). Every span, refusal and
    /// frame of this surface names it.
    pub fn held_name(&self) -> PathName {
        self.topology_view.held_name(&self.location())
    }

    /// This surface, publishing its state hooks on `backbone` as well as its mesh channel.
    pub fn with_backbone(mut self, backbone: rdm_gossip::backbone::Backbone) -> Self {
        self.backbone = Some(backbone);
        self
    }

    /// This surface, naming `owed` on a signal stop's outcome span.
    pub fn with_owed(mut self, owed: Arc<crate::node::OwedDeclarations>) -> Self {
        self.owed = Some(owed);
        self
    }

    /// This surface, cancelling `delete` when `node.connection.delete { All }` or a stop is admitted.
    pub fn with_cancel(mut self, birth_cancel: rdm_transport::BirthCancel) -> Self {
        self.birth_cancel = Some(birth_cancel);
        self
    }

    /// This surface, leaving and rejoining its mesh through `session` when it is stopped and started.
    pub fn set_session(&self, session: Arc<dyn Session>) {
        *self.session.lock().unwrap() = Some(session);
    }

    /// This surface, closing the data-plane connections of the Direct entries a
    /// `node.connection.delete` closes on `client`.
    pub fn set_data_plane(&self, client: Arc<NodeRpcClient>) {
        *self.data_plane.lock().unwrap() = Some(client);
    }

    /// This surface, holding the process's connections writer: what this birth holds and where a
    /// `node.connection.delete` writes its drops.
    pub fn set_connections(&self, connections: Arc<crate::connections_writer::ConnectionsWriter>) {
        *self.connections.lock().unwrap() = Some(connections);
    }

    /// This surface, serving the process's storage change moves through `storage`.
    pub fn set_storage_moves(&self, storage: Arc<crate::storage_move::StorageMoves>) {
        *self.storage.lock().unwrap() = Some(storage);
    }

    /// The process's storage change moves, once it holds them.
    pub fn storage_moves(&self) -> Option<Arc<crate::storage_move::StorageMoves>> {
        self.storage.lock().unwrap().clone()
    }

    /// This surface, holding the process's data-plane `ConnectionsRegistry`.
    pub fn set_data_connections(&self, registry: rdm_transport::connections::ConnectionsRegistry) {
        *self.data_connections.lock().unwrap() = Some(registry);
    }

    /// `node.cert.renew` on this birth (node-cert-renew.md step 4, Reply): the cert must decode and
    /// name this exact birth; it is installed, so every connection this birth opens from now on
    /// presents it, and answered `node.cert.renewed`. Nothing is closed, set or published. A cert
    /// that does not decode, or names another birth, is refused by name and the held one kept.
    pub fn renew_cert(&self, cert: &[u8]) -> StatusReply {
        let me = self.held_name();
        // The refusal is the reply's: the authority's `via-renewal` span names it.
        let refuse = rdm_core::reply::status::rejected_cert;
        let signed = match rdm_core::certs::cert_from_bytes(cert) {
            Ok(s) => s,
            Err(e) => return refuse(format!("{me:?} ({} {}): the cert does not decode: {e}", self.node_id, self.incarnation.as_str())),
        };
        if signed.cert.node_id != self.node_id || signed.cert.incarnation != self.incarnation {
            return refuse(format!(
                "the cert names {:?} ({} {}), not this birth {me:?} ({} {})",
                signed.cert.location(),
                signed.cert.node_id,
                signed.cert.incarnation.as_str(),
                self.node_id,
                self.incarnation.as_str()
            ));
        }
        let Some(registry) = self.data_connections.lock().unwrap().clone() else {
            return rdm_core::reply::status::not_ready(format!("{me:?} is still starting: it holds no connections registry to present a cert from"));
        };
        registry.set_member_cert(cert.to_vec());
        tracing::info_span!("rdm.node.cert.update.via-installed", node = %self.node_id, incarnation_id = %self.incarnation.as_str(), expires_at = signed.cert.expiry_ms)
            .in_scope(|| tracing::info!("the renewed member cert is installed: every connection opened from now presents it"));
        StatusReply::CertRenewed { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), expires_at: signed.cert.expiry_ms }
    }

    /// The fabric's root set this birth admits on (`node.connection.create`): the one its admissions
    /// read.
    pub fn roots(&self) -> Vec<rdm_core::certs::VerifyingKey> {
        self.data_connections.lock().unwrap().as_ref().map(|r| r.roots().current()).unwrap_or_default()
    }

    /// The registry this birth's admissions are judged on, once the process handed it one: whose root set
    /// `node.roots.update` replaces.
    pub fn registry(&self) -> Option<rdm_transport::connections::ConnectionsRegistry> {
        self.data_connections.lock().unwrap().clone()
    }

    /// The connections this birth holds now (R-CR1), from its connections writer; refused by name
    /// when the process has not handed it one, so nothing is decided on a set it cannot read.
    pub async fn held_routes(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, String> {
        let writer = self.connections.lock().unwrap().clone();
        let data_connections = self.data_connections.lock().unwrap().clone();
        match writer {
            Some(w) => {
                let mut routes = w.held_routes().await;
                if let Some(reg) = data_connections {
                    routes.extend(reg.held_open_tunnels());
                }
                Ok(routes)
            }
            None => Err(format!("{}: this birth holds no connections writer, so the connections it holds cannot be read", self.node_id)),
        }
    }

    /// The connections this birth has open now (R-CR1; Q169): read from its live connections, its
    /// data-plane client's pool and its connections registry, never from the rows that have landed,
    /// so a connection whose Connected row is still owed is held. What a rename and a leave decide
    /// from. Refused by name when the process has not handed it a connections writer.
    pub fn live_routes(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, String> {
        let writer = self.connections.lock().unwrap().clone();
        let data_plane = self.data_plane.lock().unwrap().clone();
        let registry = self.data_connections.lock().unwrap().clone();
        match writer {
            Some(w) => Ok(w.live_routes(data_plane.as_deref(), registry.as_ref())),
            None => Err(format!("{}: this birth holds no connections writer, so the connections it has open cannot be read", self.node_id)),
        }
    }

    /// `node.connection.get { node }` at this birth (fabric-mesh-node-lifecycles.md: "Reading or cutting a
    /// node's held connections"; Q167 R-RD7): the connections it holds now that match every pair of
    /// `filter`, under the name it holds, answered from what is held: a connection whose row has not
    /// landed is not held yet, and the read never waits for a write (Q169); the ids are those
    /// `node.connection.delete` names.
    pub async fn connections_get(&self, filter: &rdm_core::filter::Filter) -> StatusReply {
        let writer = self.connections.lock().unwrap().clone();
        let data_connections = self.data_connections.lock().unwrap().clone();
        match writer {
            Some(w) => {
                let mut held = w.held_connections().await;
                if let Some(reg) = data_connections {
                    held.routes.extend(reg.held_open_tunnels());
                }
                connections_answer(&held, filter)
            }
            None => rdm_core::reply::status::not_ready(format!("{}: this birth holds no connections writer, so the connections it holds cannot be read", self.node_id)),
        }
    }

    /// `node.connection.delete` at this birth (R-CR5; node-connections-delete.md): a connection is
    /// an open Direct or Proxy connection of this birth, read from what is open now (Q169), and
    /// nothing else. `All` closes every open connection; `Connections` closes exactly the ones it
    /// names. Each closed connection's drop is queued on the writer, under the name the birth holds,
    /// and lands behind the answer; an id that is no open connection is named `not_open`. Gossip neighbours and the Node RPC pool are not connections: neither scope touches
    /// them, so a birth whose connections are deleted is still heard on gossip and still answers calls.
    pub async fn delete_connections(&self, scope: &ConnectionScope) -> Deleted {
        self.delete_connections_reporting(scope, || async {}).await
    }

    /// [`Self::delete_connections`], calling `admitted` once an `All`'s block on new application work
    /// is in force and before it waits for the work in flight: where the node command reports
    /// connections-deleting on its stream (i144 R22). `Connections` never calls it: it sets no
    /// node-wide state.
    pub async fn delete_connections_reporting<F, Fut>(&self, scope: &ConnectionScope, admitted: F) -> Deleted
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        self.delete_connections_asked(scope, 1, admitted).await
    }

    /// [`Self::delete_connections_reporting`], `asking` the calls in flight that are the asker's own
    /// and not eligible work: the command's one call, none for this process's own signal.
    async fn delete_connections_asked<F, Fut>(&self, scope: &ConnectionScope, asking: u64, admitted: F) -> Deleted
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let writer = self.connections.lock().unwrap().clone();
        // `All` is graceful node-wide closure (node-connections-delete.md step 3): admission of new
        // application work is blocked, and the eligible work in flight (every dispatched handler but
        // this call's own) is let finish, bounded by the birth's `connections_delete_deadline`.
        // The birth is deleted at the admission of `All`: its `pre_node_ready` hook and any pull
        // it has in flight end; it returns to service only through a stop and a start.
        let in_flight = match scope {
            ConnectionScope::All => {
                if let Some(birth_cancel) = &self.birth_cancel {
                    birth_cancel.cancel();
                }
                // The block on new application work: from here the node refuses new non-status calls
                // (Node RPC §35); the node is connections-deleting, and says so.
                self.server.begin_connections_delete();
                admitted().await;
                let waited = self.await_in_flight(asking).await;
                // Every connection open once admitted work has finished is closed, the ones that
                // work opened while `All` waited included (step 3.2: "they belong to the same
                // closure"). From here the birth opens no new connection until it starts (proposal
                // node-properties-own-commands.md §2.1, §2.4): blocked before the open set is read,
                // so none opens between the read and the close.
                self.block_new_connections("node.connection.delete All closed this birth's connections");
                Some(waited)
            }
            ConnectionScope::Connections(_) => None,
        };
        // The connections open now (Q169): a connection whose Connected row is still owed is open
        // and closed with the rest; the delete never waits for a row.
        let open = self.live_routes().unwrap_or_default();
        let open_ids: Vec<rdm_core::ConnectionId> = open.iter().map(|r| r.id.clone()).collect();
        let (routes, not_open) = match scope {
            ConnectionScope::All => (open, Vec::new()),
            ConnectionScope::Connections(ids) => {
                let (mut routes, mut not_open): (Vec<rdm_core::connections::NodeConnection>, Vec<rdm_core::ConnectionId>) = (Vec::new(), Vec::new());
                for id in ids {
                    match open.iter().find(|r| &r.id == id) {
                        Some(route) if !routes.iter().any(|r| r.id == route.id) => routes.push(route.clone()),
                        Some(_) => {}
                        None => not_open.push(id.clone()),
                    }
                }
                (routes, not_open)
            }
        };
        let data_connections = self.data_connections.lock().unwrap().clone();
        if let Some(reg) = &data_connections {
            for route in &routes {
                reg.close_by_id(&route.id, rdm_transport::connections::Closed::ConnectionsDeleted);
            }
        }
        // Each Direct entry closed is its data-plane connection closed, on that connection's own
        // span naming the entry (R-CR6: the pool's hard cut is the stop's transport cut alone); a
        // Proxy rides its carrier's Direct and only its route deletes.
        let data_plane = self.data_plane.lock().unwrap().clone();
        if let Some(data_plane) = &data_plane {
            for route in routes.iter().filter(|r| r.kind == rdm_core::connections::ConnectionKind::Direct) {
                if let Some(peer) = data_plane.endpoint_of(&route.destination.node_id) {
                    for c in data_plane.close_connections_to(&peer, CONNECTIONS_DELETE) {
                        if let (Some(w), Some(inc)) = (&writer, &c.key.incarnation) {
                            w.direct_closed(&c.key.peer, inc);
                        }
                        tracing::info_span!(
                            "rdm.node_admin.connection.delete.via-connection-delete",
                            node = %self.node_id,
                            entry = %route.id,
                            destination_path = %route.destination.path, destination_name = %route.destination.name,
                            peer = %c.key.peer.fmt_short(),
                            incarnation_id = %c.key.birth(),
                            connection = c.connection,
                        )
                        .in_scope(|| tracing::info!("the data-plane connection of a closed Direct entry was closed"));
                    }
                }
            }
        }
        // Each drop is queued on the writer's one ordered queue (Q169) and lands behind the answer
        // (node-connections-delete.md step 3.5); the connections still open are those open now that
        // the delete did not close.
        let closed = match &writer {
            Some(w) => w.write_drops(&routes, CONNECTIONS_DELETE),
            None => Vec::new(),
        };
        let held: Vec<rdm_core::ConnectionId> = open_ids.into_iter().filter(|id| !closed.contains(id)).collect();
        let receipt = in_flight.map(|w| match w {
            InFlight::Zero { began } => ConnectionsReceipt::Established { in_flight: began, closed: closed.clone() },
            InFlight::Deadline { last } => ConnectionsReceipt::Deadline { last_in_flight: last, closed: closed.clone(), held: held.clone() },
        });
        // An `All` completed `Established`: the birth is connections-deleted until it starts or joins
        // again, and a stop answers this receipt rather than closing again.
        if let Some(r @ ConnectionsReceipt::Established { .. }) = &receipt {
            self.completed.lock().unwrap().deleted = Some(r.clone());
        }
        Deleted { closed, not_open, held, receipt }
    }

    /// The admission block (node-name-update.md Precondition: "No new connection is admitted from the
    /// row write until the node rejoins its mesh"): the client this birth originates its logged
    /// connections on opens none from here until it starts or joins its mesh again. The Node RPC pool
    /// and gossip are not connections and are untouched.
    fn block_new_connections(&self, cause: &str) {
        if let Some(data_plane) = self.data_plane.lock().unwrap().clone() {
            data_plane.block_new_connections(format!("{} ({}): {cause}; it opens no new connection until it starts or joins its mesh again", self.node_id, self.node_id));
        }
        if let Some(reg) = self.data_connections.lock().unwrap().clone() {
            reg.block_new_connections();
        }
    }

    /// Lift the admission block: a birth that starts, or that joins its mesh again, opens
    /// connections again.
    fn admit_new_connections(&self) {
        // A birth that starts or joins again holds connections again and is on its mesh again: what
        // its primitives completed before is not this cycle's.
        *self.completed.lock().unwrap() = Completed::default();
        if let Some(data_plane) = self.data_plane.lock().unwrap().clone() {
            data_plane.admit_new_connections();
        }
        if let Some(reg) = self.data_connections.lock().unwrap().clone() {
            reg.unblock_new_connections();
        }
    }

    /// Block admission of new application work and wait for the eligible work in flight to reach
    /// zero, bounded by the birth's `connections_delete_deadline`. The `asking` calls that asked are in
    /// flight while it waits and are not eligible work. The node refuses new non-status calls with typed
    /// `ConnectionsDeleting` from here on (Node RPC §35); control streams stay usable.
    async fn await_in_flight(&self, asking: u64) -> InFlight {
        self.server.begin_connections_delete();
        let stats = self.server.stats();
        let eligible = || rdm_transport::ServerStats::get(&stats.in_flight).saturating_sub(asking);
        let began = eligible();
        let until = tokio::time::Instant::now() + self.connections_delete_deadline;
        // Woken by each handler's end, never on a timer; the bound is the one deadline.
        loop {
            let ended = stats.in_flight_ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            let n = eligible();
            if n == 0 {
                return InFlight::Zero { began };
            }
            tokio::select! {
                () = ended => {}
                () = tokio::time::sleep_until(until) => return InFlight::Deadline { last: eligible() },
            }
        }
    }

    /// Serve a node command from `commander` on its ordered reply stream (op 0x22, i144 R22; Luke Q6,
    /// Q7): every refusal is decided before `CommandAccepted`; after it the stream ends only with the
    /// command's completion. `node.connection.delete { All }` reports `ConnectionsDeleting` once its
    /// block on new application work is in force, then closes and answers `ConnectionsDeleted`;
    /// `Connections([…])` answers its completion right after the acceptance. A caller gone after the
    /// acceptance stops nothing: the node completes what it accepted.
    pub async fn serve_node_command(self: &Arc<Self>, commander: Option<&NodeId>, req: &NodeCommandRequest, frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        self.serve_node_command_on(commander, None, req, frames).await
    }

    /// A stop or start this birth's own run commands of it, in process: a step of the Build `build_id`
    /// the node-admin runs from its own context (a node.delete's composed stop of itself).
    /// The command is served as [`Self::serve_node_command`]; its lifecycle op names that Build, so the
    /// stop's hold spares that run's turns, which wait on the stop (node-stop.md step 3; Q342 flag 1 B:
    /// "`LifecycleOp.build_id` is the Build's when the publishing mesh-primary runs the step from its own
    /// context"). Any other command is served as [`Self::serve_node_command`].
    pub async fn serve_node_command_of_run(self: &Arc<Self>, commander: Option<&NodeId>, build_id: &str, req: &NodeCommandRequest, frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        match req {
            NodeCommandRequest::StopNode { .. } | NodeCommandRequest::StartNode { .. } => self.serve_lifecycle_command_in(commander, None, Some(build_id), req, frames).await,
            _ => self.serve_node_command_on(commander, None, req, frames).await,
        }
    }

    /// Serve the command on its caller's connection, which a stop spares and a start joins back over.
    pub async fn serve_node_command_on(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &NodeCommandRequest, mut frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        use tracing::Instrument;
        if let NodeCommandRequest::StorageBlock { incarnation, change_id, .. } = req {
            if *incarnation != self.incarnation {
                return rdm_core::reply::node_command::rejected_stale_incarnation(self.incarnation.clone());
            }
            let Some(storage) = self.storage_moves() else {
                return rdm_core::reply::node_command::not_ready("this birth has no storage handle yet".into());
            };
            if frames.accepted(NodeCommandReply::CommandAccepted { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }).await.is_err() {
                return rdm_core::reply::node_command::not_ready("caller gone before storage block acceptance".into());
            }
            // Accepted: the stream ends with `StorageBlocked` or `Failed`, never a refusal
            // (node-rpc-envelope.md, NodeCommand op 0x22).
            return rdm_core::reply::node_command::after_acceptance("storage-block", storage.block(change_id).await);
        }
        if let NodeCommandRequest::StorageStep { incarnation, change_id, provider, .. } = req {
            return self.serve_storage_step(incarnation, change_id, provider, frames).await;
        }
        // `mesh.block` and `mesh.unblock` are a mesh-primary's: its node-admin serves them before this
        // birth's own path is reached, and any other birth is no mesh-primary.
        if let NodeCommandRequest::BlockMesh { .. } | NodeCommandRequest::UnblockMesh { .. } = req {
            return rdm_core::reply::node_command::not_ready(format!("{}: {} is a mesh-primary's command, and this birth serves none", self.node_id, req.op()));
        }
        // Position 4 is reserved (Q345): node.mesh.leave is node.stop's step only.
        if let NodeCommandRequest::Reserved4 { .. } = req {
            return rdm_core::reply::node_command::rejected("reserved-position", format!("{}: NodeCommand position 4 is reserved and never executed: node.mesh.leave is a step of node.stop", self.node_id));
        }
        if !matches!(req, NodeCommandRequest::DeleteConnections { .. }) {
            return self.serve_lifecycle_command(commander, spare, req, frames).await;
        }
        let NodeCommandRequest::DeleteConnections { incarnation, scope, .. } = req else { unreachable!("checked") };
        if *incarnation != self.incarnation {
            return rdm_core::reply::node_command::rejected_stale_incarnation(self.incarnation.clone());
        }
        if self.connections.lock().unwrap().is_none() {
            return rdm_core::reply::node_command::not_ready(format!("{}: this birth holds no connections writer yet; nothing is cut", self.node_id));
        }
        if let Some(reason) = scope.refusal() {
            return rdm_core::reply::node_command::rejected_connection_scope(format!("{}: {reason}", self.node_id));
        }
        let accepted = NodeCommandReply::CommandAccepted { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() };
        if let Err(e) = frames.accepted(accepted).await {
            // Nothing was accepted: the caller is gone before the acceptance left, so nothing runs.
            return rdm_core::reply::node_command::not_ready(format!("{}: the command's caller is gone before its acceptance ({e}); nothing is cut", self.node_id));
        }
        let sink = std::sync::Arc::new(tokio::sync::Mutex::new(frames));
        let (node, node_id, incarnation) = (self.node_id.clone(), self.node_id.clone(), self.incarnation.clone());
        let commander_name = named(commander);
        let all = matches!(scope, ConnectionScope::All);
        let report = {
            let sink = sink.clone();
            let me = self.clone();
            move || async move {
                // The node's own record first (Q356), then its Digest at once (gossip.md §3.1: its
                // connections-deleting reaches its own mesh in its own Digest): before its report, under
                // one `Send`; a refusal or a spent `Send` is named on the writer's `via-status` span and
                // moves neither, and the report still goes on the command's stream.
                if all {
                    me.write_state(crate::model::NodeStatus::ConnectionsDeleting).await;
                }
                // The span opens before the report leaves, so it starts before whatever the report causes
                // (an in-process recorder may take it before the send returns).
                let span = tracing::info_span!("rdm.node_admin.connection.delete.via-connections-deleting", node = %node, commander = %commander_name, outcome = tracing::field::Empty, "otel.kind" = "internal");
                let sent = sink.lock().await.progress(rdm_core::reply::node_command::connections_deleting(node_id, incarnation)).instrument(span.clone()).await;
                let outcome = match &sent {
                    Ok(()) => "sent".to_string(),
                    Err(e) => format!("caller gone: {e}"),
                };
                span.record("outcome", outcome.as_str());
                span.in_scope(|| tracing::info!("node.connection.delete All is admitted: the block on new application work is in force; the node reports connections-deleting on the command's stream"));
            }
        };
        let span = tracing::info_span!(
            "rdm.node_admin.connection.delete.via-delete-connections",
            node = %self.node_id, commander = %named(commander), scope = scope.name(), named = match scope { ConnectionScope::All => 0, ConnectionScope::Connections(ids) => ids.len() },
            deadline_ms = tracing::field::Empty, closed = tracing::field::Empty, not_open = tracing::field::Empty, held = tracing::field::Empty, receipt = tracing::field::Empty,
            outcome = tracing::field::Empty, reason = tracing::field::Empty, "otel.kind" = "internal"
        );
        let d = async {
            if matches!(scope, ConnectionScope::All) {
                tracing::Span::current().record("deadline_ms", self.connections_delete_deadline.as_millis() as u64);
            }
            self.delete_connections_reporting(scope, report).await
        }
        .instrument(span.clone())
        .await;
        span.record("closed", d.closed.len()).record("not_open", d.not_open.len()).record("held", d.held.len());
        if let Some(r) = &d.receipt {
            span.record("receipt", r.name());
        }
        if matches!(d.receipt, Some(ConnectionsReceipt::Established { .. })) {
            self.write_state(crate::model::NodeStatus::ConnectionsDeleted).await;
        }
        span.record("outcome", "connections-deleted");
        span.in_scope(|| tracing::info!("node.connection.delete decided at this birth"));
        NodeCommandReply::ConnectionsDeleted { closed: d.closed, not_open: d.not_open, held: d.held, receipt: d.receipt }
    }

    /// A storage change's move of this exact birth (storage.md §2 "Changing it"): a command of an older
    /// change is refused by name before it is accepted; an accepted one ends with the birth writing the
    /// provider (`StorageStepped`) or refused by name.
    async fn serve_storage_step(&self, incarnation: &IncarnationId, change_id: &rdm_core::ChangeId, provider: &str, mut frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        if *incarnation != self.incarnation {
            return rdm_core::reply::node_command::rejected_stale_incarnation(self.incarnation.clone());
        }
        let Some(storage) = self.storage_moves() else {
            return rdm_core::reply::node_command::not_ready(format!("{}: this birth holds no storage handle yet; nothing is done", self.node_id));
        };
        if let Some(refused) = storage.refusal(change_id, provider).await {
            return refused;
        }
        if frames.accepted(NodeCommandReply::CommandAccepted { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }).await.is_err() {
            return rdm_core::reply::node_command::not_ready(format!("{}: caller gone before CommandAccepted; nothing done", self.node_id));
        }
        // Refused after the acceptance (an older change raced it): `Failed`, the command's terminal.
        rdm_core::reply::node_command::after_acceptance("storage-step", storage.serve(change_id, provider).await)
    }

    /// Whether this birth is stopped: stopped, its process alive, waiting for a start or a delete.
    /// A birth whose start is under way is stopped until the start ends.
    pub fn is_stopped(&self) -> bool {
        matches!(*self.cycle.lock().unwrap(), Cycle::Stopped(_) | Cycle::Starting { .. })
    }

    /// Whether a stop of this birth has been admitted: under way, done, or followed by a start that
    /// has not ended. Only a birth with no stop admitted runs node.stop's steps on its own signal.
    pub fn stop_admitted(&self) -> bool {
        !matches!(*self.cycle.lock().unwrap(), Cycle::Live(_))
    }

    /// Serve `StopNode` or `StartNode` on the exact birth it names (`node.connection.delete` reaches a
    /// birth on the node command op 0x22, [`Self::serve_node_command`]); `Reserved6`
    /// and `Reserved7` are reserved positions, refused by name, whoever calls
    /// (node-rpc.md §14: nothing inside the mesh checks who is calling): `commander` is the caller as
    /// the receiver resolved it (`None` when it holds no birth for the calling endpoint), `spare` the
    /// call's own connection. A state command is served for `commander`, the primary of this birth's
    /// mesh, which its check-in goes to. `None` for any other request.
    pub async fn serve(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &StatusRequest) -> Option<StatusReply> {
        if matches!(req, StatusRequest::Reserved6 { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::Reserved12 { .. } | StatusRequest::Reserved13 { .. } | StatusRequest::Reserved14 { .. } | StatusRequest::Reserved15 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::Reserved29 { .. }) {
            return Some(refuse_reserved(&self.node_id.to_string(), &named(commander), req));
        }
        if let StatusRequest::JoinMesh { incarnation, operation, .. } = req {
            if *incarnation != self.incarnation {
                return Some(rdm_core::reply::stale(self.incarnation.clone()));
            }
            let declared = rdm_core::status::DeclaredOperation { operation: operation.clone() };
            return Some(self.serve_join_mesh(spare, declared).await);
        }
        None
    }

    /// Q47: admission and progress of the exact node's stop/start on 0x22.
    async fn serve_lifecycle_command(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &NodeCommandRequest, frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        self.serve_lifecycle_command_in(commander, spare, None, req, frames).await
    }

    /// [`Self::serve_lifecycle_command`], with `run`: the Build of this birth's own run that
    /// commands it in process ([`Self::serve_node_command_of_run`]), named by the command's lifecycle op.
    async fn serve_lifecycle_command_in(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, run: Option<&str>, req: &NodeCommandRequest, mut frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        // The command's identity is its operation, keyed on the node.id (Q342): it belongs to no Build.
        let (incarnation, operation) = match req {
            NodeCommandRequest::StopNode { incarnation, operation, .. } | NodeCommandRequest::StartNode { incarnation, operation, .. } => (incarnation, operation),
            _ => unreachable!("lifecycle command"),
        };
        if *incarnation != self.incarnation {
            return rdm_core::reply::node_command::rejected_stale_incarnation(self.incarnation.clone());
        }
        let stopping = matches!(req, NodeCommandRequest::StopNode { .. });
        let refusal = {
            let cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Starting { .. } if stopping => Some(rdm_core::reply::node_command::not_ready(format!("{}: a start is under way; the stop is decided on the cycle it ends in", self.node_id))),
                Cycle::Stopping(_) if !stopping => Some(rdm_core::reply::node_command::not_ready(format!("{}: a stop is still under way; start is the transition out of Leaving and only for a stopped process", self.node_id))),
                Cycle::Live(Some(op)) if !stopping && op == operation => None,
                Cycle::Live(_) if !stopping => Some(rdm_core::reply::node_command::rejected_invalid_node_transition(crate::status::declared_of_member((self.status)()))),
                _ => None,
            }
        };
        if let Some(refusal) = refusal { return refusal; }
        self.write_state(if stopping { crate::model::NodeStatus::Stopping } else { crate::model::NodeStatus::Starting }).await;
        if frames.accepted(NodeCommandReply::CommandAccepted { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }).await.is_err() {
            return rdm_core::reply::node_command::not_ready(format!("{}: caller gone before CommandAccepted; nothing executed", self.node_id));
        }
        let progress = Arc::new(tokio::sync::Mutex::new(frames));
        let build_id = run.unwrap_or(LifecycleOp::NO_BUILD);
        let held = self.held_name();
        let op = LifecycleOp { r#type: self.r#type.clone(), mesh: self.topology_view.mesh_name().to_string(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), path: held.path, name: held.name, build_id: build_id.into(), operation: operation.clone(), event_at_rafka_ms: self.topology_view.clock().now_rafka_ms(), traceparent: None, tracestate: None };
        let result = if stopping {
            self.stop(commander.cloned(), spare, op, tracing::Span::current(), Some(progress)).await
        } else {
            self.start(commander.cloned(), spare, op, tracing::Span::current(), Some(progress)).await
        };
        match result {
            StatusReply::Stopped { .. } => NodeCommandReply::NodeStopped,
            StatusReply::Started | StatusReply::AlreadyApplied => NodeCommandReply::NodeStarted,
            StatusReply::StartFailed { step, reason } => rdm_core::reply::node_command::failed(step, reason, rdm_core::outcome::Code::FailedPrecondition),
            other => rdm_core::reply::node_command::failed(req.op().into(), format!("{}: {other:?}", other.name()), <rdm_core::status::Status as rdm_core::protocol::NodeProtocol>::classify_reply(&other).code()),
        }
    }

    /// The endpoint of this birth's mesh-primary, when that is another node: the pooled connection to it
    /// its owed declarations ride stays open through a leave's cut (node-mesh-leave.md step 4, Q206).
    fn mesh_primary_endpoint(&self) -> Option<iroh::PublicKey> {
        self.topology_view.primaries().mesh(self.topology_view.mesh_name()).filter(|p| p.node_id != self.node_id).and_then(|p| self.client.endpoint_of(&p.node_id))
    }

    /// The `node.mesh.leave` primitive (node-mesh-leave.md): `node.stop` step 3. Emits `rdm.node.mesh.update.via-leaving`, leaves the mesh channel (and
    /// the backbone for a node-admin, each emitting `rdm.node.mesh.update.via-left`), and cuts the
    /// Node RPC pool while sparing `spare`, and the pooled connection to `mesh_primary` its owed
    /// declarations ride.
    async fn leave_mesh(&self, spare: &[iroh::PublicKey], mesh_primary: Option<iroh::PublicKey>, operation: &str) -> (CutReport, Vec<WhichMesh>) {
        use tracing::Instrument;
        let leaving_span = tracing::info_span!(
            "rdm.node.mesh.update.via-leaving",
            node = %self.node_id,
            node_id = %self.node_id,
            mesh = %self.topology_view.mesh_name(),
            mesh_id = %self.topology_view.mesh_id(),
            operation = %operation,
            "otel.kind" = "internal"
        );
        async {
            // The primitive's progress, `leaving`, is on the record from its command (the stop's step
            // before this one) until the mesh is left.
            let session = self.session.lock().unwrap().clone();
            let report = match &session {
                Some(session) => session.cut(spare, mesh_primary).await,
                None => {
                    leave_channels(&self.channels, self.backbone.as_ref(), &self.node_id).await;
                    CutReport::default()
                }
            };
            let left = self.meshes();
            self.completed.lock().unwrap().left = Some(left.clone());
            (report, left)
        }
        .instrument(leaving_span)
        .await
    }

    /// The meshes this birth is on: its own, and the backbone for a node-admin.
    fn meshes(&self) -> Vec<WhichMesh> {
        if self.backbone.is_some() { vec![WhichMesh::Own, WhichMesh::Backbone] } else { vec![WhichMesh::Own] }
    }

    /// node.stop's frames for primitives already completed (Q205; node-stop.md step 3): each one's
    /// acknowledgement and completion at once, `ConnectionsDeleting`, `StopConnectionsDeleted` with
    /// the receipt `receipt`, `Leaving`, `StopMeshLeft`. No work is done again.
    async fn answer_completed(&self, progress: &Option<CommandProgress>, receipt: &ConnectionsReceipt) {
        let Some(progress) = progress else { return };
        let left = self.completed.lock().unwrap().left.clone().unwrap_or_else(|| self.meshes());
        let mut frames = progress.lock().await;
        let _ = frames.progress(rdm_core::reply::node_command::connections_deleting(self.node_id.clone(), self.incarnation.clone())).await;
        let _ = frames.progress(NodeCommandReply::StopConnectionsDeleted { receipt: receipt.clone() }).await;
        let _ = frames.progress(NodeCommandReply::Leaving { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }).await;
        let _ = frames.progress(NodeCommandReply::StopMeshLeft { left }).await;
    }

    /// `node.mesh.join` commanded on this live birth (`JoinMesh` on 0x1B; node-mesh-join.md step 10,
    /// node-tickle.md "What follows"; Q87): the whole start sequence runs again on it, from the join
    /// back over the commanding call to its hook, its Build floor, then its one topology read and the
    /// rafka-time of that read (node-start.md steps 2 to 5), the way a start after a stop does. The birth leaves
    /// its channels first; it writes Pending at its accepted join (node-mesh-join.md step 10); the command is answered `MeshJoined` once the join is
    /// accepted and the channel entered (node.mesh.joined), while the rest of the start runs on, and a
    /// start that ends before that answers how it ended. A stopped birth is started by `node.start`,
    /// never by this.
    async fn serve_join_mesh(self: &Arc<Self>, spare: Option<iroh::PublicKey>, operation: rdm_core::status::DeclaredOperation) -> StatusReply {
        let Some(key) = spare else {
            return rdm_core::reply::status::not_ready(format!("{}: commander-uncalled: JoinMesh came on no Node RPC call, so no endpoint names the admin to join back", self.node_id));
        };
        let Some(session) = self.session.lock().unwrap().clone() else {
            return rdm_core::reply::status::not_ready(format!("{}: this process holds no session to join its mesh with", self.node_id));
        };
        let (outcome_tx, outcome_rx) = tokio::sync::watch::channel(None);
        let (commander_tx, commander_rx) = tokio::sync::watch::channel(key);
        {
            let mut cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Live(_) => {}
                Cycle::Starting { .. } => return rdm_core::reply::status::not_ready(format!("{}: a start is under way; node.mesh.join is decided on the cycle it ends in", self.node_id)),
                Cycle::Stopping(_) | Cycle::Stopped(_) => return rdm_core::reply::status::not_ready(format!("{}: a stopped birth is started by node.start, not by node.mesh.join", self.node_id)),
            }
            *cycle = Cycle::Starting { outcome: outcome_rx.clone(), commander: Some(commander_tx), calls: vec![key] };
        }
        // The birth leaves the channels it is on: its hook gates its app ops again, it writes pending at
        // its accepted join (node-mesh-join.md step 10, its session's channel entry), and it is
        // published Ready only once the hook has passed.
        session.leave_channels().await;
        self.server.resume();
        self.admit_new_connections();
        // The command begins the birth's next cycle, which ends the one it replaces: a first start still
        // held at its hook ends Cancelled and runs nothing after it, so only this sequence runs
        // (node-mesh-join.md step 10, "runs the whole sequence again on the commanding call").
        let birth_cancel = match &self.birth_cancel {
            Some(r) => r.renew(),
            None => rdm_transport::CancelToken::new(),
        };
        // The command's completion is the mesh held again (node-mesh-join.md step 9): the start's
        // channel step re-subscribes the channel left above and says so once. No progress stream: the
        // start owes its Ready declaration to its authority as a start after a stop does.
        let (held_tx, held_again) = tokio::sync::oneshot::channel();
        let me = self.clone();
        tokio::spawn(async move {
            let result = session.rejoin(birth_cancel, commander_rx, None, operation, None, Some(held_tx)).await;
            let reply = match result {
                Ok(()) => StatusReply::Started,
                Err(failed) => {
                    // The sequence stopped at a step: the birth stays Pending and leaves its channels
                    // again, so its authority holds it unheard and commands it back once more
                    // (node-recovery.md step 2); nothing here retries by hand.
                    tracing::info_span!("rdm.mesh.node.reject.via-mesh-join-start-failed", node = %me.node_id, step = failed.step, reason = %failed.reason)
                        .in_scope(|| tracing::info!("the commanded start stopped at its step: the birth leaves its channels for its authority's next command"));
                    session.leave_channels().await;
                    rdm_core::reply::status::start_failed(failed.step.to_string(), failed.reason)
                }
            };
            *me.cycle.lock().unwrap() = Cycle::Live(None);
            let _ = outcome_tx.send(Some(reply));
        });
        let joined = if self.backbone.is_some() { vec![WhichMesh::Own, WhichMesh::Backbone] } else { vec![WhichMesh::Own] };
        let mesh_joined = || StatusReply::MeshJoined { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), joined: joined.clone() };
        let mut outcome = outcome_rx;
        let mut held_again = Some(held_again);
        loop {
            tokio::select! {
                held = async { held_again.as_mut().unwrap().await }, if held_again.is_some() => match held {
                    Ok(()) => return mesh_joined(),
                    Err(_) => held_again = None,
                },
                changed = outcome.changed() => {
                    if changed.is_err() {
                        return rdm_core::reply::status::not_ready(format!("{}: the start that was under way ended without an outcome", self.node_id));
                    }
                    if let Some(reply) = outcome.borrow().clone() {
                        return match reply {
                            StatusReply::Started => mesh_joined(),
                            other => other,
                        };
                    }
                }
            }
        }
    }

    /// The stop (node-stop.md): the whole `node.connection.delete { All }`, declare `Leaving`, the
    /// stop's own transport cut. The receipt is the `All`'s. A second stop of the same birth, while this one runs or after it, answers the same
    /// receipt: stopping a stopped birth is done. A stop while a start of the birth is under way is
    /// refused naming the start.
    async fn stop(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span, progress: Option<CommandProgress>) -> StatusReply {
        use tracing::Instrument;
        enum Next {
            Done(ConnectionsReceipt),
            Wait(tokio::sync::watch::Receiver<Option<ConnectionsReceipt>>),
            Run(tokio::sync::watch::Sender<Option<ConnectionsReceipt>>),
        }
        let next = {
            let mut cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Stopped(r) => Next::Done(r.clone()),
                // The start ends the stopped cycle: the receipt that stopped the birth does not
                // answer for a birth on its way out of Leaving, as a start does not run beside a stop.
                Cycle::Starting { .. } => return rdm_core::reply::status::not_ready(format!("{}: a start is under way; the stop is decided on the cycle it ends in", self.node_id)),
                Cycle::Stopping(rx) => Next::Wait(rx.clone()),
                Cycle::Live(_) => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    *cycle = Cycle::Stopping(rx);
                    Next::Run(tx)
                }
            }
        };
        let receipt = match next {
            Next::Done(r) => {
                self.answer_completed(&progress, &r).await;
                r
            },
            Next::Wait(mut rx) => {
                let receipt = match rx.wait_for(|r| r.is_some()).await {
                    Ok(r) => r.clone().expect("waited for a receipt"),
                    Err(_) => return rdm_core::reply::status::not_ready(format!("{}: the stop that was under way ended without a receipt", self.node_id)),
                };
                self.answer_completed(&progress, &receipt).await;
                receipt
            },
            Next::Run(tx) => {
                let span = tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.status.update.via-stop-node",
                    node = %self.node_id, operation = %op.operation.action, operation_subject = ?op.operation.subject, build_id = %op.build_id,
                    commander = %named(commander.as_ref()), state = "Stopped", receipt = tracing::field::Empty, "otel.kind" = "internal"
                );
                let (receipt, _) = self.stop_steps(StopBy::Command { commander: commander.as_ref(), spare, op: &op, progress: progress.clone() }).instrument(span.clone()).await;
                span.record("receipt", receipt.name());
                *self.cycle.lock().unwrap() = Cycle::Stopped(receipt.clone());
                let _ = tx.send(Some(receipt.clone()));
                receipt
            }
        };
        StatusReply::Stopped { receipt }
    }

    /// node.stop's steps (node-stop.md steps 2-4), one workflow over the primitives for each of its
    /// subjects (i146 Q55): the mesh-primary's `StopNode` on its command stream, this process's own
    /// signal, or a commanded start that failed at a step. The whole `node.connection.delete { All }`,
    /// the node's own tasks held, `node.mesh.leave`, then `Stopped` to the mesh-primary. A command's stop
    /// answers on its stream and declares `Stopped` naming its operation; a signal's stop owes
    /// connections-deleting at the block and `Stopped` after the leave, waits on no answer, and hands
    /// each owed declaration back for its process end to wait on (Q206); a failed start's
    /// stop declares `Stopped` naming the start's operation and puts nothing on the start's stream. The
    /// receipt is the All's.
    async fn stop_steps(&self, by: StopBy<'_>) -> (ConnectionsReceipt, Vec<(String, tokio::task::JoinHandle<()>)>) {
        use tracing::Instrument;
        // Who commanded it, the calls to spare, the operation, the stream, how many calls in flight are
        // the asker's own (not eligible work), and whether a command's own delete runs (step 4's view).
        let (commander, mut spare, op, progress, asking, by_command): (Option<&NodeId>, Vec<iroh::PublicKey>, Option<&LifecycleOp>, Option<CommandProgress>, u64, bool) = match by {
            StopBy::Command { commander, spare, op, progress } => (commander, spare.into_iter().collect(), Some(op), progress, 1, true),
            StopBy::Signal => (None, Vec::new(), None, None, 0, false),
            StopBy::FailedStart { commander, calls, op } => (commander, calls.to_vec(), Some(op), None, calls.len() as u64, false),
        };
        let operation = op.map_or(SIGNAL_STOP, |o| o.operation.action.as_str());
        // A commanded stop leaves its live process stopped; a signal's process ends with its stop.
        if op.is_some() {
            process_stop().stop();
        }
        if let Some(birth_cancel) = &self.birth_cancel {
            birth_cancel.cancel();
        }
        // 1. node.connection.delete { All } (node-stop.md step 2), whole: admission of new
        // application work is blocked, the eligible work in flight finishes under the bound, and every
        // open Direct or Proxy entry is closed, its drop written under the name the birth holds. The
        // receipt never turns a deadline into zero. A command's own call is in flight and is not
        // eligible work, nor are the calls commanding a failed start; a signal has none, and owes its
        // connections-deleting once the block is in force. A command's stop streams the primitive's
        // acknowledgement, `ConnectionsDeleting`, once the block is in force, and its completion,
        // `StopConnectionsDeleted` (Q205). A birth already connections-deleted (an `All` completed
        // `Established` since it last started) answers both at once with that receipt and closes
        // nothing again.
        let closure = tracing::info_span!("rdm.node_admin.connection.delete.via-stop", node = %self.node_id, operation = %operation, commander = %named(commander), outcome = tracing::field::Empty);
        let already = self.completed.lock().unwrap().deleted.clone();
        let deleting = Mutex::new(None);
        // `owe_deleting`: a signal's stop owes its connections-deleting, once it is on the record.
        let ack = |progress: &Option<CommandProgress>, owe_deleting: bool| {
            let progress = progress.clone();
            let frame = rdm_core::reply::node_command::connections_deleting(self.node_id.clone(), self.incarnation.clone());
            let deleting = &deleting;
            async move {
                // The node's own record first (Q356): connections-deleting before its declaration and
                // its frame.
                self.write_state(crate::model::NodeStatus::ConnectionsDeleting).await;
                if owe_deleting {
                    *deleting.lock().unwrap() = self.owe(NodeState::ConnectionsDeleting, None);
                }
                if let Some(progress) = progress {
                    let _ = progress.lock().await.progress(frame).await;
                }
            }
        };
        let receipt = match already {
            Some(receipt) => {
                ack(&progress, false).await;
                closure.record("outcome", "already-deleted");
                receipt
            }
            None => {
                let deleted = self
                    .delete_connections_asked(&ConnectionScope::All, asking, || ack(&progress, op.is_none()))
                    .instrument(closure.clone())
                    .await;
                closure.record("outcome", "deleted");
                deleted.receipt.expect("All carries its receipt")
            }
        };
        closure.in_scope(|| tracing::info!("the stop's node.connection.delete All is complete"));
        drop(closure);
        let mut owed: Vec<(String, tokio::task::JoinHandle<()>)> = deleting.into_inner().unwrap().into_iter().collect();
        if matches!(receipt, ConnectionsReceipt::Established { .. }) {
            self.write_state(crate::model::NodeStatus::ConnectionsDeleted).await;
        }
        if let Some(progress) = &progress {
            let _ = progress.lock().await.progress(NodeCommandReply::StopConnectionsDeleted { receipt: receipt.clone() }).await;
        }
        let session = self.session.lock().unwrap().clone();
        // A stop this node commanded itself is its own run's (the in-process command of an executor
        // that is its own subject): the hold does not wait for the turns of that run's Build, and the
        // cut spares the connections of the callers that run answers, as a dialled stop spares its
        // commander's (node-stop.md "Transport cut": never the stop call's stream).
        let commanded_by = op.filter(|_| by_command && commander == Some(&self.node_id)).map(|o| o.build_id.as_str());
        if let (Some(session), Some(build)) = (&session, commanded_by) {
            spare.extend(session.callers_of(build));
        }
        // 2. The node's own tasks are held: no turn begins, and every turn already past its wait
        // point runs to its end before anything more is said or cut.
        if let Some(session) = &session {
            let span = tracing::info_span!(
                "rdm.node_admin.node.update.via-stop-quiesce",
                node = %self.node_id, operation = %operation, commanded_by = commanded_by.unwrap_or(""), turns_in_progress = tracing::field::Empty, waited_for = tracing::field::Empty
            );
            let held = session.hold_tasks(commanded_by).instrument(span.clone()).await;
            span.record("turns_in_progress", held.turns_in_progress);
            span.record("waited_for", held.waited_for.as_str());
        }
        // 4. A stopping node gossips nothing (node-stop.md step 4, R-ON1; Q95), a command's stop and a
        // signal's alike. The one exception is R-NS1 item 2: when node.delete reaches a deleted mesh's
        // last admin, still its primary, its final mesh topology carries the mesh deleted and itself
        // deleting, then nothing. It follows the hold, so no gated turn publishes after it.
        let mesh_deleted = self.topology_view.digest_view.statuses.mesh(self.topology_view.mesh_id()).is_some_and(|h| h.status == "deleted" || h.status == "dead");
        // node.delete runs node.stop's steps: a deleted mesh's last admin is stopped only as its delete.
        let final_delete = op.filter(|_| by_command && mesh_deleted);
        let leaving_recorded = if let (Some(backbone), Some(op)) = (self.backbone.as_ref().filter(|b| b.is_mesh_primary()), final_delete) {
            let del_op = if op.is_stop() { LifecycleOp { operation: op.operation.command("node.delete", &self.node_id), ..op.clone() } } else { op.clone() };
            self.topology_view.digest_view.deleting(del_op);
            // Its leaving on its own record first (Q356); the aggregate carries the status the record
            // holds, its last one when the write was refused (named on the writer's span).
            let me = match self.record_state(rdm_core::model::NodeStatus::Leaving).await {
                Ok(me) => me,
                Err(_) => (self.set_status)((self.status)()),
            };
            let mut members: Vec<MeshDigest> = backbone.own_members(&self.topology_view);
            match members.iter_mut().find(|d| d.node.node_id == me.node.node_id) {
                Some(slot) => *slot = me.clone(),
                None => members.push(me.clone()),
            }
            let count = members.len();
            // Best-effort dissemination (R-NS2): the stop does not wait for its delivery, and the cut
            // below follows its send at once.
            let published = if backbone.withholds("stopping-aggregate", "backbone") { "withheld" } else if backbone.publish(&self.topology_view, members).await { "true" } else { "false" };
            tracing::info_span!("rdm.node_admin.node.update.via-leaving-aggregate", node = %self.node_id, mesh = %self.topology_view.mesh_name(), operation = %operation, status = ?me.status, members = count, published)
                .in_scope(|| tracing::info!("the last mesh topology before the stop carries this node Leaving"));
            true
        } else {
            false
        };
        // 5. node.mesh.leave, called after its connections are deleted (node-stop.md step 3;
        // node-mesh-leave.md): leave the mesh and cut its transport, its gossip neighbours and its
        // Node RPC pool, sparing the commander's connection the stop call rides and the connection to
        // its mesh-primary its owed declarations ride (Q206). Never its endpoint.
        // A command's stop streams its acknowledgement, `Leaving`, and its completion, `StopMeshLeft`
        // (Q205). A birth already left since it last started leaves nothing again and answers both.
        // Leaving is on its record once: the final delete's aggregate above wrote it already.
        // Leaving goes on its record and, at once, in its Digest: the node's last Digest carries
        // leaving before it leaves the mesh, and it publishes nothing after it (node-mesh-leave.md,
        // Q415). The final delete's aggregate above already carries it when it wrote it.
        if !leaving_recorded {
            self.write_state(crate::model::NodeStatus::Leaving).await;
        }
        if let Some(progress) = &progress {
            let _ = progress.lock().await.progress(NodeCommandReply::Leaving { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }).await;
        }
        let already_left = self.completed.lock().unwrap().left.clone();
        let left = match already_left {
            Some(left) => left,
            None => {
                let mesh_primary = self.mesh_primary_endpoint();
                let cut = tracing::info_span!(
                    "rdm.node_admin.node.update.via-stop-transport-cut",
                    node = %self.node_id, operation = %operation,
                    spared = spare.len(), mesh_primary_spared = mesh_primary.is_some(), neighbours = tracing::field::Empty, pooled = tracing::field::Empty, accepted = tracing::field::Empty, turns_in_progress = tracing::field::Empty
                );
                let (report, left) = self.leave_mesh(spare.as_slice(), mesh_primary, operation).instrument(cut.clone()).await;
                cut.record("neighbours", report.neighbours);
                cut.record("pooled", report.pooled);
                cut.record("accepted", report.accepted);
                cut.record("turns_in_progress", report.turns_in_progress);
                // The cut's span ends with the cut: what follows is the declaration, not the cut.
                cut.in_scope(|| tracing::info!("the stop cut its transport: gossip neighbours and the Node RPC pool; the endpoint stays bound, and the commander's connection and the mesh-primary's stay open"));
                left
            }
        };
        self.write_state(crate::model::NodeStatus::Left).await;
        if let Some(progress) = &progress {
            let _ = progress.lock().await.progress(NodeCommandReply::StopMeshLeft { left }).await;
        }
        // 6. This birth declares `Stopped` to its mesh-primary (i146 Q29, Q36, Q55; node-stop.md step 4): an
        // owed message whose delivery is the runtime's (node-rpc.md §43), riding the connection the cut
        // spared. The stop's receipt and reply never wait for the mesh-primary's answer; a signal's
        // process ends once each is answered or its receiver is proven gone (Q206). A command's names its stop: the owed
        // twin of the reply's `stopped`; whichever of the two reaches the mesh-primary first is applied and
        // the other answers AlreadyApplied (node-stop.md step 5, Q49). A signal's names no operation.
        // Its Stopped is on its own record first, before the declaration and before node.stopped answers
        // (node-stop.md "Completion": the matching node.stopped and the stop recorded on the node record;
        // Q356).
        self.write_state(crate::model::NodeStatus::Stopped).await;
        owed.extend(self.owe(NodeState::Stopped, op.map(declared)));
        (receipt, owed)
    }

    /// Owe `state` to the mesh-primary without waiting for its answer (node-rpc.md §43: an owed message's
    /// delivery is the runtime's): owed at this call, after every declaration owed before it, and sent
    /// once each of those is answered (Q210). What is handed back ends once its call ended, for a
    /// signal's process end to wait on (Q206). A command's stop names its operation; a signal's names
    /// none (i146 Q55).
    fn owe(&self, state: NodeState, operation: Option<rdm_core::status::DeclaredOperation>) -> Option<(String, tokio::task::JoinHandle<()>)> {
        let declare = self.declare.clone()?;
        match &operation {
            Some(op) => tracing::info_span!("rdm.node_rpc.status.update.via-owed-on-stop", node = %self.node_id, state = ?state, operation = %op.operation.action, operation_subject = ?op.operation.subject, operation_occurrence = ?op.operation.occurrence)
                .in_scope(|| tracing::info!("the state is owed to the mesh-primary, naming the stop; the stop does not wait for its answer")),
            None => tracing::info_span!("rdm.node_rpc.status.update.via-owed-on-signal", node = %self.node_id, state = ?state)
                .in_scope(|| tracing::info!("the state is owed to the mesh-primary; the stop does not wait for its answer")),
        }
        Some((format!("{state:?}"), tokio::spawn(declare(state, operation))))
    }

    /// The stop this process's own signal runs: node.stop's steps with the node its own subject
    /// (`Self::stop_steps`, i146 Q55), then the wait its process end follows (Q206): each owed
    /// declaration's call ends on its answer or on the proof its receiver is gone, over the
    /// connection to the mesh-primary the cut spared. A mesh-primary exiting beside this node
    /// leaves no proof this node can hear once its mesh is left, so the wait is bounded by the
    /// birth's `RDM_CONNECTIONS_DELETE_DEADLINE_MS`; a call still open at the bound ends with the
    /// process, and the span names it.
    pub async fn stop_on_signal(&self) -> SignalStopped {
        let (receipt, mut declarations) = self.stop_steps(StopBy::Signal).await;
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-stop-declarations",
            node = %self.node_id, operation = SIGNAL_STOP, declarations = declarations.len(),
            deadline_ms = self.connections_delete_deadline.as_millis() as u64, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty, delivered = tracing::field::Empty, left = tracing::field::Empty
        );
        let began = std::time::Instant::now();
        let ended = {
            let each = async {
                for (_, d) in declarations.iter_mut() {
                    let _ = d.await;
                }
            };
            tokio::time::timeout(self.connections_delete_deadline, each).await.is_ok()
        };
        // What was delivered and what was left: the stop's own declarations by their state, then
        // whatever else the birth still owed its mesh-primary ahead of them (a config update). A
        // declaration whose call ended unanswered, its attempts spent (Q311), is still owed: left.
        let still_owed: Vec<String> = self.owed.as_ref().map(|o| o.names()).unwrap_or_default();
        let delivered: Vec<&str> = declarations.iter().filter(|(n, d)| d.is_finished() && !still_owed.contains(n)).map(|(n, _)| n.as_str()).collect();
        let mut left: Vec<String> = declarations.iter().filter(|(n, d)| !d.is_finished() || still_owed.contains(n)).map(|(n, _)| n.clone()).collect();
        for name in still_owed {
            if !left.contains(&name) {
                left.push(name);
            }
        }
        let unanswered = ended && !left.is_empty();
        for (_, d) in &declarations {
            d.abort();
        }
        span.record("elapsed_ms", began.elapsed().as_millis() as u64);
        span.record("outcome", if unanswered { "unanswered" } else if ended { "ended" } else { "deadline" });
        span.record("delivered", delivered.join(",").as_str());
        span.record("left", left.join(",").as_str());
        span.in_scope(|| {
            if unanswered {
                tracing::info!("each owed declaration's call ended, and one ended unanswered, its attempts spent: it is left undelivered");
            } else if ended {
                tracing::info!("each owed declaration's call ended: answered, or its receiver proven gone");
            } else {
                tracing::info!("a declaration's call was still open at the birth's bound: it ends with the process, undelivered");
            }
        });
        SignalStopped { receipt }
    }

    /// The start: a stopped birth rejoins as itself through its session. A birth that is not stopped
    /// refuses it, naming the state it is in. A start that reaches the birth while another start of
    /// it is under way is the same natural key `(node_id, incarnation, Pending)`: it runs no rejoin
    /// of its own and answers that start's outcome, and its call is the one the start's join goes
    /// back over from then on (node-start.md step 2: the start joins back over the call that
    /// commanded it, and this call commanded it last).
    async fn start(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span, progress: Option<CommandProgress>) -> StatusReply {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-start-node",
            node = %self.node_id, operation = %op.operation.action, operation_subject = ?op.operation.subject, build_id = %op.build_id,
            commander = %named(commander.as_ref()), joined = tracing::field::Empty, outcome = tracing::field::Empty, failed_step = tracing::field::Empty, "otel.kind" = "internal"
        );
        let reply = async {
            enum Next {
                Run(tokio::sync::watch::Sender<Option<StatusReply>>, Arc<dyn Session>, Option<tokio::sync::watch::Receiver<iroh::PublicKey>>),
                Join(tokio::sync::watch::Receiver<Option<StatusReply>>),
            }
            let next = {
                let mut cycle = self.cycle.lock().unwrap();
                match &mut *cycle {
                    Cycle::Stopped(_) => {
                        let Some(session) = self.session.lock().unwrap().clone() else {
                            return rdm_core::reply::status::not_ready(format!("{}: this process holds no session to rejoin its mesh with", self.node_id));
                        };
                        let (tx, rx) = tokio::sync::watch::channel(None);
                        let (commander, commanded) = match spare {
                            Some(key) => {
                                let (tx, rx) = tokio::sync::watch::channel(key);
                                (Some(tx), Some(rx))
                            }
                            None => (None, None),
                        };
                        *cycle = Cycle::Starting { outcome: rx, commander, calls: spare.into_iter().collect() };
                        Next::Run(tx, session, commanded)
                    }
                    Cycle::Starting { outcome, commander, calls } => {
                        // This call commanded the start last: its join goes back over this call from
                        // its next attempt on.
                        if let (Some(key), Some(commander)) = (spare, commander.as_ref()) {
                            commander.send_replace(key);
                            calls.push(key);
                        }
                        Next::Join(outcome.clone())
                    }
                    Cycle::Stopping(_) => return rdm_core::reply::status::not_ready(format!("{}: a stop is still under way; start is the transition out of Leaving and only for a stopped process", self.node_id)),
                    Cycle::Live(Some(operation)) if *operation == op.operation => return rdm_core::reply::already_applied(),
                    Cycle::Live(_) => {
                        return rdm_core::reply::status::rejected_invalid_node_transition(crate::status::declared_of_member((self.status)()));
                    }
                }
            };
            let (outcome, session, commanded) = match next {
                Next::Join(mut rx) => {
                    tracing::Span::current().record("joined", true);
                    let reply = match rx.wait_for(|r| r.is_some()).await {
                        Ok(r) => r.clone().expect("waited for an outcome"),
                        Err(_) => rdm_core::reply::status::not_ready(format!("{}: the start that was under way ended without an outcome", self.node_id)),
                    };
                    if matches!(reply, StatusReply::Started) {
                        if let Some(progress) = &progress { let _ = progress.lock().await.progress(NodeCommandReply::NodeJoined).await; }
                    }
                    return reply;
                }
                Next::Run(tx, session, commanded) => (tx, session, commanded),
            };
            let birth_cancel = match &self.birth_cancel {
                Some(r) => r.renew(),
                None => rdm_transport::CancelToken::new(),
            };
            // No row is written from here until the session holds the name its join answer hands:
            // every connection the start makes or accepts is recorded under that name.
            let writer = self.connections.lock().unwrap().clone();
            let rows = match writer {
                Some(w) => Some(w.hold_rows().await),
                None => None,
            };
            // Its pending is written at its accepted join (node-start.md step 2, its session's channel
            // entry), never before it: starting holds until then.
            self.server.resume();
            self.admit_new_connections();
            // The admin that commanded the start answers its join, reached back over its own call:
            // the call's endpoint key and path (node-start.md step 2).
            let joined = match commanded {
                Some(commander) => session.rejoin(birth_cancel, commander, rows, declared(&op), progress, None).await,
                None => Err(StartFailed { step: "node.join", reason: format!("{}: commander-uncalled: the start came on no Node RPC call, so no endpoint names the admin to join back", self.node_id) }),
            };
            let reply = match joined {
                Ok(()) => {
                    // A new cycle: its next connections delete is a new connections delete.
                    let mut cycle = self.cycle.lock().unwrap();
                    *cycle = Cycle::Live(Some(op.operation.clone()));
                    drop(cycle);
                    process_stop().start();
                    StatusReply::Started
                }
                Err(failed) if failed.step == crate::node_hooks::PRE_NODE_READY_STEP => {
                    // The start ended Failed at its hook (Q350; fabric-node-hooks.md "Outcomes and the
                    // gate": a start that ends Failed leaves the node Pending, and nothing retries until
                    // someone acts). The node is rejoined and stays Pending, its process running and
                    // never stopped again: its probe answers StartFailed with the hook's last reason, and
                    // a later node.stop or node.delete is what acts on it.
                    *self.cycle.lock().unwrap() = Cycle::Live(Some(op.operation.clone()));
                    process_stop().start();
                    tracing::Span::current().record("failed_step", failed.step);
                    rdm_core::reply::status::start_failed(failed.step.to_string(), failed.reason)
                }
                Err(failed) => {
                    // The start stopped at its step: the birth is stopped again, as it was, through
                    // node.stop's own steps ([`Self::stop_steps`]), the stop naming the start's
                    // operation: its connections deleted (the calls commanding the start are in flight
                    // and not eligible work), its tasks held (a turn its rejoin let begin runs to its
                    // end), its mesh left sparing every call commanding the start so each is answered,
                    // its `Stopped` owed to the mesh-primary. The receipt it stops with is this stop's.
                    let calls = match &*self.cycle.lock().unwrap() {
                        Cycle::Starting { calls, .. } => calls.clone(),
                        _ => spare.into_iter().collect(),
                    };
                    let (receipt, _) = self.stop_steps(StopBy::FailedStart { commander: commander.as_ref(), calls: &calls, op: &op }).await;
                    *self.cycle.lock().unwrap() = Cycle::Stopped(receipt);
                    tracing::Span::current().record("failed_step", failed.step);
                    rdm_core::reply::status::start_failed(failed.step.to_string(), failed.reason)
                }
            };
            outcome.send_replace(Some(reply.clone()));
            reply
        }
        .instrument(span.clone())
        .await;
        span.record("node", self.node_id.to_string().as_str());
        span.record("outcome", reply.name());
        reply
    }
}

/// Why a birth's own status did not move: its record's write was refused by name, or its `Send` was
/// spent (named on the writer's `rdm.node.record.update.via-status` span).
pub type StatusUnwritten = rdm_transport::client::SendEnd<crate::storage_error::StorageError>;

/// The one move of a birth's own status (Q356): `to` written on its own record first, under one `Send`
/// ([`crate::node_records::NodePropertyWriter::write_status_with`], `also` on the same line), then its
/// status set by `set` to the status the record then holds (the record's one status order), answering the
/// Digest that carries it, for its caller to publish at once (gossip.md §3.1 `Digest`: "at once when its
/// status changes"). Every status a birth moves itself to goes through here: each state of a stop and a
/// start ([`NodeSelf::write_state`]), its pending at an accepted join, its Ready at node.started, and a
/// node-admin's Ready at its gate. A process that holds no record yet sets `to`, with nothing recorded
/// to differ from. A write refused, or whose `Send` is spent, sets nothing: the Digest keeps the record's
/// last status, and the end is answered.
pub async fn write_own_status(
    writer: Option<&crate::node_records::NodePropertyWriter>,
    to: crate::model::NodeStatus,
    also: impl Fn(&mut crate::nodes_storage::NodeRecord) + Clone + Send + 'static,
    set: impl FnOnce(crate::model::NodeStatus) -> MeshDigest,
) -> Result<MeshDigest, StatusUnwritten> {
    let held = match writer {
        Some(writer) => writer.write_status_with(to, also).await,
        None => None,
    };
    match held {
        None => Ok(set(to)),
        Some(Ok(held)) => Ok(set(held)),
        Some(Err(end)) => Err(end),
    }
}

/// Leave the channels a birth is on (node-mesh-leave.md step 3): its mesh channel, and the backbone
/// when it is a node-admin's, each left emitting `rdm.node.mesh.update.via-left`. The one place the
/// channel leave is written: each session's `leave_channels`, a birth that holds no session, and a
/// commanded `node.mesh.join` whose start failed at a step call it.
pub async fn leave_channels(channels: &rdm_gossip::channel::Channels, backbone: Option<&rdm_gossip::backbone::Backbone>, node_id: &NodeId) {
    let topology_view = channels.topology_view();
    channels.stop().await;
    tracing::info_span!(
        "rdm.node.mesh.update.via-left",
        node = %topology_view.node_name(),
        node_id = %node_id,
        mesh = %topology_view.mesh_name(),
        mesh_id = %topology_view.mesh_id(),
        which = WhichMesh::Own.name(),
    )
    .in_scope(|| tracing::info!("left the mesh channel"));
    if let Some(backbone) = backbone {
        backbone.stop().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-left",
            node = %topology_view.node_name(),
            node_id = %node_id,
            mesh = %topology_view.mesh_name(),
            mesh_id = %topology_view.mesh_id(),
            which = WhichMesh::Backbone.name(),
        )
        .in_scope(|| tracing::info!("left the backbone channel"));
    }
}

/// A commander as a span field: its node id, or empty when the call resolved none.
fn named(commander: Option<&NodeId>) -> String {
    commander.map(ToString::to_string).unwrap_or_default()
}

/// The record this runtime's provider wrote for it (`rdm_core::runtime::read_own_record`),
/// waited for on the caller's runtime for at most `within`: a boot that ends (a stop signal) drops
/// the wait at once, where a blocking wait would hold the runtime's shutdown until `within` passed.
pub async fn wait_for_own_record(dir: &std::path::Path, within: std::time::Duration) -> Result<rdm_core::RuntimeFact, String> {
    let until = tokio::time::Instant::now() + within;
    loop {
        let last = match rdm_core::runtime::read_own_record(dir) {
            Ok(f) => return Ok(f),
            Err(e) => e,
        };
        if tokio::time::Instant::now() >= until {
            return Err(format!("the provider wrote no runtime record for this runtime within {within:?}: {last}"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// What a receiver of a `NodeLeft` call holds instead of a command it could complete: nothing, by
/// design (node-rpc-envelope.md, "Node stop command pair").
pub const NODE_LEFT_IS_NEVER_A_CALL: &str = "no command: node-left is never a call; a stopped birth reports left on the reply of its stop-node call";

/// What a reserved Status position is refused with: none is ever a call (R-CR4).
pub const RETIRED_PAIR_IS_RESERVED: &str = "no command: Status positions 6, 7, 8 (StopNode), 12 to 15, 16 (StartNode), 29 and 34 are reserved, never executed; lifecycle commands use NodeCommand 0x22";

/// A reserved Status position at `receiver` (node-rpc-envelope.md "Node stop command
/// pair": "a receiver refuses them by name"): nothing starts, and the reply names the position.
pub fn refuse_reserved(receiver: &str, caller: &str, req: &StatusRequest) -> StatusReply {
    // Position 34, the removed upward property copy, position 21, the caller's connections delete
    // that goes on NodeCommand 0x22 (D18), and positions 17 to 20, the name and config commands that go
    // on it too (Q367), name no operation: refused by name, nothing written.
    if let StatusRequest::Reserved34 { node_id, .. } | StatusRequest::Reserved21 { node_id, .. } | StatusRequest::Reserved17 { node_id, .. } | StatusRequest::Reserved18 { node_id, .. } | StatusRequest::Reserved19 { node_id, .. } | StatusRequest::Reserved20 { node_id, .. } = req {
        let reported = format!("{} of {node_id}", req.op());
        tracing::info_span!(
            "rdm.node_admin.status.reject.via-reserved-position",
            node = %receiver, op = req.op(), subject = %node_id, caller = %caller, field = "command", expected = RETIRED_PAIR_IS_RESERVED, reported = %reported, "otel.kind" = "internal"
        )
        .in_scope(|| tracing::info!("a reserved Status position was refused by name: it is never a call and starts nothing"));
        return rdm_core::reply::status::rejected_unmatched_completion("command".into(), RETIRED_PAIR_IS_RESERVED.into(), reported);
    }
    let (StatusRequest::Reserved6 { node_id, incarnation, build_id, operation } | StatusRequest::Reserved7 { node_id, incarnation, build_id, operation } | StatusRequest::StopNode { node_id, incarnation, build_id, operation } | StatusRequest::StartNode { node_id, incarnation, build_id, operation } | StatusRequest::Reserved29 { node_id, incarnation, build_id, operation, .. } | StatusRequest::Reserved12 { node_id, incarnation, build_id, operation, .. } | StatusRequest::Reserved13 { node_id, incarnation, build_id, operation, .. } | StatusRequest::Reserved14 { node_id, incarnation, build_id, operation, .. } | StatusRequest::Reserved15 { node_id, incarnation, build_id, operation, .. }) = req else {
        unreachable!("refuse_reserved is called for the reserved positions only")
    };
    let reported = format!("{} {operation:?} (build {build_id})", req.op());
    tracing::info_span!(
        "rdm.node_admin.status.reject.via-reserved-position",
        node = %receiver, op = req.op(), operation = %operation.action, operation_subject = ?operation.subject, build_id = %build_id, subject = %node_id, incarnation_id = %incarnation.as_str(),
        caller = %caller, field = "command", expected = RETIRED_PAIR_IS_RESERVED, reported = %reported, "otel.kind" = "internal"
    )
    .in_scope(|| tracing::info!("a reserved Status position was refused by name: it is never a call and starts nothing"));
    rdm_core::reply::status::rejected_unmatched_completion("command".into(), RETIRED_PAIR_IS_RESERVED.into(), reported)
}

/// The answer of a Status service that has no handler for `req` (R24, Luke Q11): `RequestNotHandled`
/// naming the request, decided by dispatch alone, never by node type, role or sender, on the
/// receiver's reject span `rdm.node.status.reject.via-request-not-handled`. `receiver` is the service's
/// held name. Nothing is done.
pub fn request_not_handled(receiver: &str, req: &StatusRequest) -> StatusReply {
    tracing::info_span!("rdm.node.status.reject.via-request-not-handled", node = %receiver, request = req.op(), "otel.kind" = "internal")
        .in_scope(|| tracing::info!("this Status service has no handler for the request: not handled here, nothing done"));
    rdm_core::reply::not_handled(req.op().to_string())
}

/// The refusal of a `NodeLeft` call at any receiver, a node-admin or a node (node-rpc-envelope.md,
/// "Node stop command pair": "NodeLeft keeps its position and is never sent; a receiver refuses it by
/// name"). A stop's completion is its `Stopped` reply, so no command is open that a node-left call
/// completes: it is `RejectedUnmatchedCompletion { field: "command" }`, `expected` what the receiver
/// holds instead ([`NODE_LEFT_IS_NEVER_A_CALL`]) and `reported` what the call named. Nothing is
/// completed, whatever command is open for the birth. `name` is the subject's path and name as the
/// receiver holds it (empty when it holds none) and `caller` who called, both for the reject span
/// only; nothing checks who is calling (node-rpc.md §14). Never `NotReady`, the readiness gate's
/// answer (R-NR1).
pub fn refuse_node_left(receiver: &str, name: &str, caller: &str, req: &StatusRequest) -> StatusReply {
    let StatusRequest::NodeLeft { node_id, incarnation, build_id, operation } = req else { unreachable!("refuse_node_left is called for node-left only") };
    let reported = format!("{} {operation:?} (build {build_id})", req.op());
    tracing::info_span!(
        "rdm.node_admin.status.reject.via-node-left",
        node = %receiver,
        op = req.op(),
        operation = %operation.action, operation_subject = ?operation.subject,
        build_id = %build_id,
        subject = %node_id,
        incarnation_id = %incarnation.as_str(),
        name = %name,
        caller = %caller,
        field = "command",
        expected = NODE_LEFT_IS_NEVER_A_CALL,
        reported = %reported,
        "otel.kind" = "internal"
    )
    .in_scope(|| tracing::info!("a node-left call was refused by name: it is never a call, and it completes no command"));
    rdm_core::reply::status::rejected_unmatched_completion("command".into(), NODE_LEFT_IS_NEVER_A_CALL.into(), reported)
}


/// One fact as a connection list carries it.
pub fn connection_view(f: &NodeConnection) -> ConnectionView {
    ConnectionView {
        id: f.id.to_string(),
        source: f.source.node_id.to_string(),
        destination: f.destination.node_id.to_string(),
        kind: match f.kind {
            ConnectionKind::Direct => "direct",
            ConnectionKind::Proxy => "proxy",
        }
        .into(),
        state: match f.state {
            ConnectionState::Connected => "connected",
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Failed => "failed",
            ConnectionState::Refused => "refused",
        }
        .into(),
        carrier: f.carrier.as_ref().map(|c| c.node_id.to_string()),
        reason: f.reason.clone(),
        logged_at_ms: f.logged_at_ms,
    }
}

/// `node.connection.get { node }` answered from `held`, one read of the node's connections writer (Q167
/// R-RD7): the connections the node holds (its `connected` latest Directs and its active Proxies,
/// each named by the minted id `node.connection.delete` names it by) that match every pair of
/// `filter`, under the name it holds; a filter the connection schema refuses is `RejectedFilter`
/// naming the pair. Its span names the node, the pairs' properties, how many it answered and whether
/// its set is complete.
pub fn connections_answer(held: &HeldConnections, filter: &rdm_core::filter::Filter) -> StatusReply {
    let read = rdm_core::filter::Entity::Connections.read();
    let node = &held.node_id;
    if let Some(reason) = filter.refusal(rdm_core::filter::Entity::Connections) {
        tracing::info_span!("rdm.node_admin.node.reject.via-filter", node = %node, read, properties = %filter.properties(), detail = %reason, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the filter names a pair the connection schema refuses; nothing is read"));
        return rdm_core::reply::status::bounded_rejected_filter(&reason);
    }
    let view = ConnectionsView { node: held.node_id.to_string(), complete: held.complete, connections: held.routes.iter().map(connection_view).filter(|c| filter.matches(c)).collect() };
    tracing::info_span!("rdm.node_admin.node.serve.via-connection-get", node = %node, properties = %filter.properties(), connections = view.connections.len(), complete = view.complete, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the connections the node holds answered"));
    StatusReply::Connections { connections: view }
}


/// Where a node command's frames before its completion go (i144 R22; Luke Q9): the command's own
/// ordered reply stream (op 0x22, [`StreamFrames`]), or an in-process channel when a mesh-primary is the
/// subject of the request it carries ([`ChannelFrames`]). The frames are the same either way.
#[async_trait::async_trait]
pub trait CommandFrames: Send {
    /// The command's acceptance: its first frame.
    async fn accepted(&mut self, frame: NodeCommandReply) -> Result<(), String>;
    /// A progress frame after the acceptance.
    async fn progress(&mut self, frame: NodeCommandReply) -> Result<(), String>;
}

/// The admitted command's progress, shared with its lifecycle steps.
pub type CommandProgress = Arc<tokio::sync::Mutex<Box<dyn CommandFrames>>>;

/// A node command's frames on its ordered reply stream (op 0x22).
pub struct StreamFrames {
    not_started: Option<rdm_transport::stream::ReplySink<NodeCommand, rdm_transport::stream::NotStarted>>,
    streaming: Option<rdm_transport::stream::ReplySink<NodeCommand, rdm_transport::stream::Streaming>>,
}

impl StreamFrames {
    /// The frames of the stream `sink` writes.
    pub fn new(sink: rdm_transport::stream::ReplySink<NodeCommand, rdm_transport::stream::NotStarted>) -> Self {
        Self { not_started: Some(sink), streaming: None }
    }
}

#[async_trait::async_trait]
impl CommandFrames for StreamFrames {
    async fn accepted(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        let sink = self.not_started.take().ok_or("the command was already accepted")?;
        self.streaming = Some(sink.started(frame).await.map_err(|e| format!("{e:?}"))?);
        Ok(())
    }
    async fn progress(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        self.streaming.as_mut().ok_or("no acceptance was sent")?.data(frame).await.map_err(|e| format!("{e:?}"))
    }
}

/// A node command's frames in-process: a mesh-primary that is the subject of the request it carries
/// runs its own birth's command path and reads the same frames from here.
pub struct ChannelFrames(pub tokio::sync::mpsc::UnboundedSender<NodeCommandReply>);

#[async_trait::async_trait]
impl CommandFrames for ChannelFrames {
    async fn accepted(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        self.0.send(frame).map_err(|_| "the mesh-primary's carrier is gone".to_string())
    }
    async fn progress(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        self.0.send(frame).map_err(|_| "the mesh-primary's carrier is gone".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections_writer::ConnectionsWriter;
    use crate::node::OwedDeclarations;
    use crate::storage::MemoryConnectionsStorage;
    use crate::storage_move::StorageMoves;
    use rdm_core::certs::{cert_to_bytes, issue, BirthIdentity, CaKey, IssuingKey};
    use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
    use rdm_core::filter::Filter;
    use rdm_core::{EndpointId, FabricId, MeshId, MeshNode, RuntimeFact};
    use rdm_transport::connections::ConnectionsRegistry;
    use rdm_gossip::backbone::Backbone;
    use rdm_transport::{BirthCancel, LiveNodeResolver, ServerBuilder, ServedBirth};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:5
    /// "A primitive. `node.cert.renew` installs a fresh member cert on a running node before the one it holds expires. It is run by the birth's issuer on its own tick, never by the node: the node holds no key and asks for nothing. It is a single call, not a Build. From the install the node presents the new cert on every connection it opens; nothing is terminated, nothing reconnects, no status changes and no gossip frame is published. A renewal is invisible to every other node."
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:16
    /// "| Target | One node, by exact `(node.id, incarnation)`; a cert naming another birth is refused by name |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:18
    /// "| Request | `RenewCert { node_id, incarnation, cert }` on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm); the cert is signed with the mesh's current issuing key, its expiry cut to the issuer's when the issuer expires first (`MemberOutlivesIssuer`) |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:19
    /// "| Reply | `CertRenewed { node_id, incarnation, expires_at }`, the receipt: the node holds and presents the new cert. A node that cannot decode the cert answers a named refusal and keeps the one it holds |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:23
    /// "| Spans | `rdm.node_admin.cert.update.via-renewal` (node, node_id, incarnation_id, issued_at, expires_at, previous_expires_at, outcome: `installed` / `not-sent` / `refused`) on the issuer; `rdm.node.cert.update.via-installed` on the node |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:31
    /// "4. The node checks the cert names its own `(node.id, incarnation)`, installs it, and answers `CertRenewed`. Every connection it opens from now presents the new cert."
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:38
    /// "node.cert.renew { node_id, incarnation, cert } ← node.cert.renewed { node_id, incarnation, expires_at }"
    #[test]
    fn renew_cert_installs_matching_birth_cert_and_refuses_corrupt_or_mismatched_certs() {
        let dir = std::env::temp_dir().join(format!("node-self-renew-cert-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "node-self-renew-cert").unwrap();

        capture.run(1, async {
            let endpoint = rdm_transport::endpoint::bind(
                iroh::SecretKey::generate(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap();
            let ep_id = EndpointId(endpoint.secret_key().public().to_string());
            let node_id = NodeId::mint();
            let incarnation = IncarnationId::mint();
            let name: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
            let fabric_id = FabricId::mint();
            let mesh_id = MeshId::mint();
            let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
            clock.adopt(1_000_000);
            let g = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
            let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh_id, node_id.as_str(), clock, Default::default());
            let channels = rdm_gossip::channel::Channels::new(&g, &endpoint, &topology_view).unwrap();
            channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await
            .unwrap();
            let resolver = Arc::new(LiveNodeResolver::default());
            let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm"));
            let server = ServerBuilder::new()
                .serve_core(client.clone(), None)
                .build(ServedBirth {
                    node_id: node_id.to_string(),
                    incarnation: incarnation.to_string(),
                })
                .unwrap();
            let digest = MeshDigest {
                fabric_id,
                node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
                    node_id: node_id.clone(),
                    path: name.path.clone(), name: name.name.clone(),
                    endpoint_id: ep_id.clone(),
                    transport_addr: "127.0.0.1:41000".parse().unwrap(),
                    incarnation: incarnation.clone(),
                    supersedes: None,
                },
                status: rdm_core::model::NodeStatus::Ready,
                digest_seq: 1,
                emitted_at_rafka_ms: 1_000,
                primary: Vec::new(),
            };
            let own = NodeSelf::new(
                node_id.clone(),
                incarnation.clone(),
                name.clone(),
                digest.node.r#type.clone(),
                server,
                client,
                resolver.clone(),
                channels.clone(),
                Arc::new(move |_| digest.clone()),
                Arc::new(|| rdm_core::model::NodeStatus::Ready),
                None,
                Duration::from_millis(200),
            );

            assert!(own.storage_moves().is_none());
            let sm = Arc::new(StorageMoves::new(
                node_id.clone(),
                Arc::new(|| "file".to_string()),
                Arc::new(|_| Box::pin(async { Ok(()) })),
            ));
            own.set_storage_moves(sm);
            assert!(own.storage_moves().is_some());

            // Corrupt bytes are refused `RejectedCert`.
            assert!(matches!(own.renew_cert(b"not-a-cert"), StatusReply::RejectedCert { .. }));

            let root = rdm_core::certs::mint_key();
            let ca = CaKey::mint(&root, 3600, 1_000);
            let issuer = IssuingKey::mint(&ca, "mesh1", 1_000, 2_000_000).unwrap();
            let valid_cert = issue(
                &issuer,
                &BirthIdentity {
                    node_id: node_id.clone(),
                    incarnation: incarnation.clone(),
                    path: name.path.clone(), name: name.name.clone(),
                    mesh: "mesh1".into(),
                    endpoint_key: ep_id.clone(),
                },
                1_000,
                900_000,
            )
            .unwrap();
            let valid_bytes = cert_to_bytes(&valid_cert);
            assert!(
                matches!(own.renew_cert(&valid_bytes), StatusReply::NotReady { .. }),
                "before set_data_connections, a valid cert answers NotReady"
            );

            let conn_store = Arc::new(MemoryConnectionsStorage::default());
            let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
            let own_end = ConnectionEnd {
                mesh: "mesh1".into(), r#type: "node".into(),
                path: name.path.clone(), name: name.name.clone(),
                node_id: node_id.clone(),
                incarnation: Some(incarnation.clone()),
            };
            let writer = Arc::new(ConnectionsWriter::new(own_end, conn_store, held));
            let (registry, _handlers) =
                ConnectionsRegistry::new(endpoint.clone(), resolver, writer, &[], Vec::new()).unwrap();
            own.set_data_connections(registry);

            let other_cert = issue(
                &issuer,
                &BirthIdentity {
                    node_id: NodeId::mint(),
                    incarnation: incarnation.clone(),
                    path: name.path.clone(), name: name.name.clone(),
                    mesh: "mesh1".into(),
                    endpoint_key: ep_id,
                },
                1_000,
                900_000,
            )
            .unwrap();
            assert!(
                matches!(own.renew_cert(&cert_to_bytes(&other_cert)), StatusReply::RejectedCert { .. }),
                "a cert naming another birth is refused RejectedCert"
            );
            assert_eq!(
                own.renew_cert(&valid_bytes),
                StatusReply::CertRenewed {
                    node_id,
                    incarnation,
                    expires_at: 900_000,
                }
            );
            endpoint.close().await;
        });
        let spans = capture.spans().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            spans.iter().any(|s| s["name"] == "rdm.node.cert.update.via-installed"),
            "expected rdm.node.cert.update.via-installed span, got {:?}",
            spans.iter().map(|s| &s["name"]).collect::<Vec<_>>()
        );
    }

    /// CONTRACT: docs/architecture/lifecycles/node-connections-delete.md:38-48
    /// "`All` blocks admission of new application work, lets the eligible work in flight finish ...
    /// closes every open Direct or Proxy entry of the connections log and answers `ConnectionsDeleted`"
    /// CONTRACT: docs/architecture/lifecycles/node-stop.md:33-54
    /// "The stop this process's own signal runs: node.stop's steps with the node its own subject,
    /// then awaits owed declarations up to `connections_delete_deadline`"
    /// CONTRACT: docs/architecture/query.md:20
    /// "`node.connection.get { node }` at this birth: the connections it holds now that match every pair
    /// of `filter`, under the name it holds"
    /// CONTRACT: docs/architecture/node-rpc-envelope.md:242
    /// "The answer of a Status service that has no handler for `req`: `RequestNotHandled` naming the request"
    /// CONTRACT: docs/architecture/deployment-provider.md:62
    /// "`MakeRuntimeFactAvailableToBirth`: the runtime reads the RuntimeFact record the provider wrote
    /// in its data_dir"
    #[test]
    fn connections_get_and_delete_connections_reporting_and_stop_on_signal_and_wait_for_own_record_behave_as_specified() {
        let dir = std::env::temp_dir().join(format!("node-self-ops-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "node-self-ops").unwrap();

        capture.run(1, async {
            // 1. `process_stop` tracks stop and start transitions.
            let ps = process_stop();
            ps.stop();
            assert!(ps.is_stopped());
            ps.start();
            assert!(!ps.is_stopped());

            // 2. `request_not_handled` emits `rdm.node.status.reject.via-request-not-handled` and
            //    returns `StatusReply::RequestNotHandled`.
            let req = StatusRequest::GetFabric;
            let unhandled = request_not_handled("mesh1.node.1", &req);
            assert!(matches!(unhandled, StatusReply::RequestNotHandled { ref request } if request == req.op()));

            // 3. `wait_for_own_record` reads the RuntimeFact written to `data_dir` or errors on timeout.
            let empty_dir = std::env::temp_dir().join(format!("wait-own-record-empty-{}", NodeId::mint()));
            std::fs::create_dir_all(&empty_dir).unwrap();
            let err = wait_for_own_record(&empty_dir, Duration::from_millis(25)).await.unwrap_err();
            assert!(err.contains("wrote no runtime record"), "{err}");
            let dep_id = rdm_core::DeploymentId::mint();
            let fact = RuntimeFact::of_this_process(dep_id.as_str()).unwrap();
            fact.write_record(&empty_dir).unwrap();
            let read_fact = wait_for_own_record(&empty_dir, Duration::from_millis(50)).await.unwrap();
            assert_eq!(read_fact, fact);
            let _ = std::fs::remove_dir_all(&empty_dir);

            // 4. `with_backbone`, `with_owed`, `with_cancel`, `connections_get`, `connections_answer`,
            //    `delete_connections_reporting`, and `stop_on_signal`.
            let endpoint = rdm_transport::endpoint::bind(
                iroh::SecretKey::generate(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap();
            let ep_id = EndpointId(endpoint.secret_key().public().to_string());
            let node_id = NodeId::mint();
            let incarnation = IncarnationId::mint();
            let name: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
            let fabric_id = FabricId::mint();
            let mesh_id = MeshId::mint();
            let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
            clock.adopt(1_000_000);
            let g = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
            let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh_id, node_id.as_str(), clock, Default::default());
            let channels = rdm_gossip::channel::Channels::new(&g, &endpoint, &topology_view).unwrap();
            channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await
            .unwrap();
            channels.set_publisher(&rdm_core::PublisherId {
                    node_id: node_id.clone(),
                    incarnation: incarnation.clone(),
                });
            channels.join(rdm_gossip::channel::ChannelId::backbone(topology_view.fabric_id()), Vec::new()).await
            .unwrap();
            let backbone = channels.backbone().expect("the backbone is joined");
            let resolver = Arc::new(LiveNodeResolver::default());
            let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm"));
            let server = ServerBuilder::new()
                .serve_core(client.clone(), None)
                .build(ServedBirth {
                    node_id: node_id.to_string(),
                    incarnation: incarnation.to_string(),
                })
                .unwrap();
            let digest = MeshDigest {
                fabric_id,
                node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
                    node_id: node_id.clone(),
                    path: name.path.clone(), name: name.name.clone(),
                    endpoint_id: ep_id,
                    transport_addr: "127.0.0.1:41001".parse().unwrap(),
                    incarnation: incarnation.clone(),
                    supersedes: None,
                },
                status: rdm_core::model::NodeStatus::Ready,
                digest_seq: 1,
                emitted_at_rafka_ms: 1_000,
                primary: Vec::new(),
            };
            let cancel = BirthCancel::new();
            let owed: Arc<OwedDeclarations> = Arc::default();
            let _ = owed.owe((NodeState::Ready, None));
            let declare: Declare = Arc::new(|_, _| Box::pin(async {}));
            let own = NodeSelf::new(
                node_id.clone(),
                incarnation.clone(),
                name.clone(),
                digest.node.r#type.clone(),
                server,
                client,
                resolver,
                channels.clone(),
                Arc::new(move |_| digest.clone()),
                Arc::new(|| rdm_core::model::NodeStatus::Ready),
                Some(declare),
                Duration::from_millis(50),
            )
            .with_backbone(backbone)
            .with_owed(owed)
            .with_cancel(cancel.clone());

            // Before `set_connections`, `connections_get` returns `NotReady`.
            assert!(matches!(own.connections_get(&Filter::default()).await, StatusReply::NotReady { .. }));

            let conn_store = Arc::new(MemoryConnectionsStorage::default());
            let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
            let own_end = ConnectionEnd {
                mesh: "mesh1".into(), r#type: "node".into(),
                path: name.path.clone(), name: name.name.clone(),
                node_id: node_id.clone(),
                incarnation: Some(incarnation.clone()),
            };
            let writer = Arc::new(ConnectionsWriter::new(own_end, conn_store, held));
            own.set_connections(writer);

            // With `set_connections`, `connections_get` and `connections_answer` answer `StatusReply::Connections`
            // for a valid filter and `RejectedFilter` for an invalid filter property.
            assert!(matches!(own.connections_get(&Filter::default()).await, StatusReply::Connections { .. }));
            let bad_filter = Filter {
                pairs: vec![("nonexistent_prop".to_string(), vec!["x".to_string()])],
            };
            assert!(matches!(
                connections_answer(
                    &HeldConnections {
                        node_id: node_id.clone(),
                        path: name.path.clone(), name: name.name.clone(),
                        complete: true,
                        routes: Vec::new(),
                    },
                    &bad_filter,
                ),
                StatusReply::RejectedFilter { .. }
            ));

            // `delete_connections_reporting`: `Connections` does not invoke `admitted`, while `All`
            // cancels `BirthCancel` and invokes `admitted`.
            let admitted_called = Arc::new(AtomicBool::new(false));
            let c1 = admitted_called.clone();
            own.delete_connections_reporting(&ConnectionScope::Connections(Vec::new()), move || async move {
                c1.store(true, Ordering::SeqCst);
            })
            .await;
            assert!(!admitted_called.load(Ordering::SeqCst));
            assert!(!cancel.token().is_cancelled());

            let c2 = admitted_called.clone();
            let deleted = own
                .delete_connections_reporting(&ConnectionScope::All, move || async move {
                    c2.store(true, Ordering::SeqCst);
                })
                .await;
            assert!(admitted_called.load(Ordering::SeqCst));
            assert!(cancel.token().is_cancelled());
            assert_eq!(
                deleted.receipt,
                Some(ConnectionsReceipt::Established {
                    in_flight: 0,
                    closed: Vec::new(),
                })
            );

            // `stop_on_signal` runs stop steps and records `via-stop-declarations`.
            let sig = own.stop_on_signal().await;
            assert_eq!(
                sig.receipt,
                ConnectionsReceipt::Established {
                    in_flight: 0,
                    closed: Vec::new(),
                }
            );
            endpoint.close().await;
        });
        let spans = capture.spans().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        for expected in [
            "rdm.node.status.reject.via-request-not-handled",
            "rdm.node_admin.node.serve.via-connection-get",
            "rdm.node_admin.node.reject.via-filter",
            "rdm.node_admin.node.update.via-stop-declarations",
        ] {
            assert!(
                spans.iter().any(|s| s["name"] == expected),
                "expected span {expected}, got {:?}",
                spans.iter().map(|s| &s["name"]).collect::<Vec<_>>()
            );
        }
    }

    /// A nodes store whose writes are refused (`Io`, not an answer) while `refuse` holds.
    struct SwitchedNodes {
        held: crate::nodes_storage::MemoryNodesStorage,
        refuse: AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::nodes_storage::NodesStorage for SwitchedNodes {
        async fn node(&self, node_id: &NodeId) -> Result<Option<crate::nodes_storage::NodeLine>, crate::storage_error::StorageError> {
            self.held.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::nodes_storage::NodeRecord>, crate::storage_error::StorageError> {
            self.held.nodes(mesh).await
        }
        async fn put_node(&self, record: &crate::nodes_storage::NodeRecord) -> Result<(), crate::storage_error::StorageError> {
            if self.refuse.load(Ordering::SeqCst) {
                return Err(crate::storage_error::StorageError::Io { file: format!("nodes/records/{}", record.node_id), reason: "simulated disk refusal".into() });
            }
            self.held.put_node(record).await
        }
        async fn records_at(&self, name: &PathName) -> Result<Vec<crate::nodes_storage::NodeRecord>, crate::storage_error::StorageError> {
            self.held.records_at(name).await
        }
    }

    /// CONTRACT: `NodeSelf::write_state` (Q356: "the one status its record, its Digest and its answers
    /// carry"; Q344: one `Send` on a store's write): a node writes its status on its own record first,
    /// and its Digest carries the status the record then holds. A nodes.storage that refuses the write to
    /// the end of the `Send` moves neither: the record keeps its last status and so does the Digest.
    /// Must NOT happen: a Digest that says a status the node's record does not hold.
    #[tokio::test]
    async fn a_status_the_record_refuses_is_not_published_in_the_digest() {
        use crate::nodes_storage::NodesStorage as _;
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let ep_id = EndpointId(endpoint.secret_key().public().to_string());
        let (node_id, incarnation, name) = (NodeId::mint(), IncarnationId::mint(), rdm_core::PathName::new("mesh1.node", "1").unwrap());
        let (fabric_id, mesh_id) = (FabricId::mint(), MeshId::mint());
        let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
        clock.adopt(1_000_000);
        let g = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh_id, node_id.as_str(), clock, Default::default());
        let channels = rdm_gossip::channel::Channels::new(&g, &endpoint, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await.unwrap();
        let resolver = Arc::new(LiveNodeResolver::default());
        let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm"));
        let server = ServerBuilder::new().serve_core(client.clone(), None).build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).unwrap();
        let digest = MeshDigest {
            fabric_id,
            node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(), node_id: node_id.clone(), path: name.path.clone(), name: name.name.clone(), endpoint_id: ep_id, transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None },
            status: rdm_core::model::NodeStatus::Ready,
            digest_seq: 1,
            emitted_at_rafka_ms: 1_000,
            primary: Vec::new(),
        };
        let published: Arc<Mutex<Vec<rdm_core::model::NodeStatus>>> = Arc::default();
        let into = published.clone();
        let set_status: Arc<dyn Fn(rdm_core::model::NodeStatus) -> MeshDigest + Send + Sync> = Arc::new(move |s| {
            into.lock().unwrap().push(s);
            MeshDigest { status: s, ..digest.clone() }
        });
        let own = NodeSelf::new(node_id.clone(), incarnation.clone(), name.clone(), "node".into(), server, client, resolver, channels, set_status, Arc::new(|| rdm_core::model::NodeStatus::Ready), None, Duration::from_millis(200));
        let store = Arc::new(SwitchedNodes { held: Default::default(), refuse: AtomicBool::new(false) });
        let writer = crate::node_records::NodePropertyWriter::new(store.clone(), || "file".to_string());
        writer.bound(crate::nodes_storage::fixture_record(&name, "node", node_id.clone(), incarnation.clone(), rdm_core::model::NodeStatus::Ready), |_| {}).await.unwrap();
        own.set_records(writer);
        let held = || async { store.held.node(&node_id).await.unwrap().expect("its record").record.state };
        store.refuse.store(true, Ordering::SeqCst);
        own.write_state(rdm_core::model::NodeStatus::ConnectionsDeleting).await;
        assert_eq!(held().await, rdm_core::model::NodeStatus::Ready, "the refused write leaves the record at its last status");
        assert!(published.lock().unwrap().is_empty(), "the Digest keeps the record's last status: nothing is published over it");
        store.refuse.store(false, Ordering::SeqCst);
        own.write_state(rdm_core::model::NodeStatus::ConnectionsDeleting).await;
        assert_eq!(held().await, rdm_core::model::NodeStatus::ConnectionsDeleting, "the record holds the status");
        assert_eq!(*published.lock().unwrap(), vec![rdm_core::model::NodeStatus::ConnectionsDeleting], "and then the Digest carries it");
    }

    // @feature: node-lifecycle
    /// CONTRACT (gossip.md §3.1: "connections-deleting, connections-deleted and stopping reach the node's
    /// own mesh in its own Digest", sent "at once when its status changes"; node-connections-delete.md
    /// step 3.1: the node "writes connections-deleting on its own record. Peers hear it later"; Q356): a
    /// standalone `node.connection.delete { All }` moves the node's Digest with its record, to
    /// connections-deleting when it reports it, then connections-deleted from its `Established`
    /// completion. Must NOT happen: a record at connections-deleting while the Digest still says Ready,
    /// or a Digest that goes from Ready to connections-deleted with nothing between.
    #[test]
    fn a_standalone_connections_delete_moves_the_digest_with_the_record() {
        use crate::nodes_storage::NodesStorage as _;
        let dir = std::env::temp_dir().join(format!("rdm-connections-delete-digest-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-node-connections-delete-digest").expect("the cell's evidence file can be created");
        capture.run(2, async {
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let ep_id = EndpointId(endpoint.secret_key().public().to_string());
        let (node_id, incarnation, name) = (NodeId::mint(), IncarnationId::mint(), rdm_core::PathName::new("mesh1.node", "1").unwrap());
        let (fabric_id, mesh_id) = (FabricId::mint(), MeshId::mint());
        let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
        clock.adopt(1_000_000);
        let g = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh_id, node_id.as_str(), clock, Default::default());
        let channels = rdm_gossip::channel::Channels::new(&g, &endpoint, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await.unwrap();
        let resolver = Arc::new(LiveNodeResolver::default());
        let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm"));
        let server = ServerBuilder::new().serve_core(client.clone(), None).build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).unwrap();
        let digest = MeshDigest {
            fabric_id,
            node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(), node_id: node_id.clone(), path: name.path.clone(), name: name.name.clone(), endpoint_id: ep_id, transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None },
            status: rdm_core::model::NodeStatus::Ready,
            digest_seq: 1,
            emitted_at_rafka_ms: 1_000,
            primary: Vec::new(),
        };
        let published: Arc<Mutex<Vec<rdm_core::model::NodeStatus>>> = Arc::default();
        let into = published.clone();
        let set_status: Arc<dyn Fn(rdm_core::model::NodeStatus) -> MeshDigest + Send + Sync> = Arc::new(move |s| {
            into.lock().unwrap().push(s);
            MeshDigest { status: s, ..digest.clone() }
        });
        let own = Arc::new(NodeSelf::new(node_id.clone(), incarnation.clone(), name.clone(), "node".into(), server, client, resolver, channels, set_status, Arc::new(|| rdm_core::model::NodeStatus::Ready), None, Duration::from_millis(200)));
        let store = Arc::new(crate::nodes_storage::MemoryNodesStorage::default());
        let writer = crate::node_records::NodePropertyWriter::new(store.clone(), || "file".to_string());
        writer.bound(crate::nodes_storage::fixture_record(&name, "node", node_id.clone(), incarnation.clone(), rdm_core::model::NodeStatus::Ready), |_| {}).await.unwrap();
        own.set_records(writer);
        let own_end = ConnectionEnd { mesh: "mesh1".into(), r#type: "node".into(), path: name.path.clone(), name: name.name.clone(), node_id: node_id.clone(), incarnation: Some(incarnation.clone()) };
        own.set_connections(Arc::new(ConnectionsWriter::new(own_end, Arc::new(MemoryConnectionsStorage::default()), Arc::new(Mutex::new(ConnectionsHeld::new())))));
        let (tx, mut frames) = tokio::sync::mpsc::unbounded_channel();
        let req = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: ConnectionScope::All };
        let reply = own.serve_node_command_on(None, None, &req, Box::new(ChannelFrames(tx))).await;
        assert!(matches!(reply, NodeCommandReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Established { .. }), .. }), "{reply:?}");
        let mut streamed = Vec::new();
        while let Ok(frame) = frames.try_recv() {
            streamed.push(frame);
        }
        assert!(streamed.iter().any(|f| matches!(f, NodeCommandReply::ConnectionsDeleting { .. })), "the report went on the command's stream: {streamed:?}");
        assert_eq!(store.node(&node_id).await.unwrap().expect("its record").record.state, rdm_core::model::NodeStatus::ConnectionsDeleted, "the record holds connections-deleted");
        assert_eq!(
            *published.lock().unwrap(),
            vec![rdm_core::model::NodeStatus::ConnectionsDeleting, rdm_core::model::NodeStatus::ConnectionsDeleted],
            "the Digest moved with the record: connections-deleting when it was reported, then connections-deleted"
        );
        endpoint.close().await;
        });
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let written: Vec<String> = spans.iter().filter(|s| s["name"] == "rdm.node.record.update.via-status" && s["attributes"]["outcome"] == "written").filter_map(|s| s["attributes"]["held"].as_str().map(String::from)).collect();
        assert_eq!(written, ["ConnectionsDeleting", "ConnectionsDeleted"], "each status is on the record, written, before the Digest carries it: {written:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
