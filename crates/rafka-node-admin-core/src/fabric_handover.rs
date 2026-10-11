//! The fabric-primary handover (R-ST2; lifecycles/fabric-primary-handover.md; node-rpc-envelope.md
//! "Fabric-primary-handover, op `0x21`, rdm"): before a mesh that holds the fabric seat leaves, the
//! seat moves to an outside mesh-primary, and only the confirmed transfer opens the mesh leave.
//!
//! ```text
//! incumbent fabric-primary                      selected outside mesh-primary
//!   select (leadership calculation, mesh excluded)
//!   record the payload, then TakeFabricPrimary ----------------------------->
//!                                                validate; durable put_seat at expected_epoch + 1;
//!                                                take the record; via-handover-committed
//!   <------------------------------------------- Applied
//!                                                new-fabric-primary on the backbone
//!   <--------------------------------------- FabricPrimaryTaken(committed_epoch)
//!   match the committed transfer; take the record (fabric writes fenced);
//!   via-handover-confirmed -------------------> Applied
//!                                                confirmed: the new fabric-primary may start mesh-leave
//! ```
//!
//! `Applied` acknowledges a call. A timeout, an `Applied` to the command and a heard
//! new-fabric-primary never open the gate; only the matched `FabricPrimaryTaken`, answered
//! `Applied` or `AlreadyApplied`, does. An uncertain handover is reconciled against its recorded
//! payload and the committed seat: the identical command or completion is sent again and
//! replayed, never a second epoch.
//!
//! Every stage a node reaches is a durable row in `fabric.storage` (`fabric_storage::HandoverRow`),
//! written before the call or reply that depends on it and read back when the admin starts. The
//! incumbent waits for the matching completion with no bound: it reacts to the completion, to the
//! successor becoming reachable or changing, and to a seat record changing, and each of those
//! sends the identical command again. The attempt stays open until the transfer is confirmed or a
//! call is refused by name.
//!
//! The same op carries the Build-driven seat transfer (node-rpc-envelope.md, "TakeSeats: Build-driven
//! seat transfer"; R-EL1a): the executor N of a drain, stop, restart or replace of a seat holder P
//! calls P before its DrainNode step.
//!
//! ```text
//! N (executor)                                   P (holder)
//!   TakeSeats { holder: P, successor: N, seats } ->
//!                                                validate: N is the Won claim's executor and the
//!                                                admin seated once P is gone; births; epochs
//!                                                ONE durable row (the fence and the commit)
//!                                                install both seats under one lock; fence Builds
//!                                                SeatedTogether on the mesh channel and backbone
//!   <------------------------------------------- SeatsCommitted { seats }
//!   install the committed seats, then DrainNode
//! ```
//!
//! The row lives under P's own mesh on the fabric's store root (`meshes/<P's mesh>/seat-transfers/`,
//! R-SR1): P is always its mesh's primary. An identical retry is answered from the row (a point read)
//! before any stale check; another payload for the same operation is refused. N never moves past the
//! step on silence: once P's birth is proven gone it point-reads P's committed row; a row naming N is
//! the committed result and N installs it, no row means the transfer never happened.

use crate::fabric_storage::{FabricStorage, HandoverRow, HandoverStage, SeatRow, SeatTransferRow};
use crate::model::{IncarnationId, NodeId, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::{LifecycleOp, Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, DigestBook, Membership, SeatTaken};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::handover::{FabricPrimaryHandover, FabricPrimaryHandoverReply as Reply, FabricPrimaryHandoverRequest as Request, CommittedSeat, HandoverBirth, Seat as WireSeat, SeatEpoch};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{watch, RwLock};
use tracing::Instrument as _;

/// The operation identity of the handover that precedes `mesh_id`'s leave: one per shutdown.
pub fn operation(mesh_id: impl std::fmt::Display) -> String {
    format!("fabric-primary-handover:{mesh_id}")
}

/// The payload of one handover, as both calls carry it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    /// The fabric's name.
    pub fabric: String,
    /// The fabric-primary that yields.
    pub incumbent: HandoverBirth,
    /// The mesh-primary that takes the seat.
    pub successor: HandoverBirth,
    /// The incumbent's seat epoch.
    pub expected_epoch: u64,
    /// The operation identity.
    pub operation: String,
}

impl Payload {
    fn take(&self) -> Request {
        Request::TakeFabricPrimary { fabric: self.fabric.clone(), incumbent: self.incumbent.clone(), successor: self.successor.clone(), expected_epoch: self.expected_epoch, operation: self.operation.clone() }
    }
    fn taken(&self, committed_epoch: u64) -> Request {
        Request::FabricPrimaryTaken { fabric: self.fabric.clone(), incumbent: self.incumbent.clone(), successor: self.successor.clone(), expected_epoch: self.expected_epoch, committed_epoch, operation: self.operation.clone() }
    }
}

/// Where one handover stands at this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// The incumbent sent the command: nothing is confirmed.
    Sent,
    /// The successor committed the seat at `committed_epoch`: the completion is not acknowledged.
    Committed(u64),
    /// The incumbent matched the completion (or the successor heard it acknowledged): confirmed.
    Confirmed(u64),
}

impl Stage {
    fn durable(self) -> HandoverStage {
        match self {
            Self::Sent => HandoverStage::Sent,
            Self::Committed(committed_epoch) => HandoverStage::Committed { committed_epoch },
            Self::Confirmed(committed_epoch) => HandoverStage::Confirmed { committed_epoch },
        }
    }
    fn of(durable: HandoverStage) -> Self {
        match durable {
            HandoverStage::Sent => Self::Sent,
            HandoverStage::Committed { committed_epoch } => Self::Committed(committed_epoch),
            HandoverStage::Confirmed { committed_epoch } => Self::Confirmed(committed_epoch),
        }
    }
}

struct Entry {
    payload: Payload,
    stage: Stage,
}

/// The handovers this node took part in, by operation: the recorded payload and how far each got.
#[derive(Default)]
pub struct HandoverBook {
    entries: Mutex<HashMap<String, Entry>>,
    changed: watch::Sender<u64>,
    /// One command or completion is decided at a time: the replay lookup, the commit or the
    /// confirmation and the record are one step, so a repeat that arrives while its first is being
    /// served finds the record and replays it.
    serving: tokio::sync::Mutex<()>,
}

impl HandoverBook {
    fn put(&self, payload: Payload, stage: Stage) {
        self.entries.lock().unwrap().insert(payload.operation.clone(), Entry { payload, stage });
        self.changed.send_modify(|v| *v += 1);
    }
    /// The recorded payload and stage of `operation`.
    pub fn get(&self, operation: &str) -> Option<(Payload, Stage)> {
        self.entries.lock().unwrap().get(operation).map(|e| (e.payload.clone(), e.stage))
    }
}

/// The executor an attempt's Won claim names, as the fabric-primary decided it: what a holder checks a
/// TakeSeats caller against.
#[async_trait::async_trait]
pub trait WonClaims: Send + Sync {
    /// The path.name of the executor of `attempt` of `build_id`; `None` when no claim of it is known.
    async fn executor(&self, build_id: &str, attempt: u32) -> Result<Option<String>, String>;
}

/// What a SeatTransfer step established at the executor: its receipt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case", tag = "outcome")]
pub enum SeatTransferOutcome {
    /// The target holds no seat in this executor's records: nothing to transfer.
    NotAHolder,
    /// The holder committed the seats and answered; the executor installed them.
    Committed {
        /// The committed seats.
        seats: Vec<CommittedSeat>,
    },
    /// The holder's birth is proven gone and its committed seat-transfer row, point-read on the
    /// fabric's store root, names this executor: the executor installed the seats it names.
    CommittedRow {
        /// The committed seats the row names.
        seats: Vec<CommittedSeat>,
    },
    /// The holder's birth is proven gone and it committed no seat-transfer row for the operation:
    /// none happened, and the ordinary election seats the successor.
    NoTransfer,
    /// The executor is not of the holder's mesh and the holder holds its own mesh's seat alone (no
    /// other admin of that mesh executes): the seat stays with it until its birth is gone and the
    /// ordinary election seats the next. A fabric seat is never taken across meshes: that is a
    /// refusal by name.
    OtherMesh {
        /// The holder's mesh.
        holder_mesh: String,
    },
}

/// The contact addresses this node announces as a fabric-primary.
#[derive(Debug, Clone)]
pub struct OwnContacts {
    /// Its endpoint key.
    pub endpoint_id: String,
    /// Its bound QUIC UDP address.
    pub transport_addr: std::net::SocketAddr,
    /// Its control API base URL.
    pub admin_api_base: Option<String>,
}

/// Why a handover did not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoverError {
    /// No outside mesh-primary is eligible: the mesh keeps running and nothing is sent.
    NoEligibleSuccessor(String),
    /// The command or the completion was refused, or could not be put, by name.
    Refused(String),
    /// The outcome is uncertain: the same handover is reconciled before any teardown.
    Unconfirmed(String),
}

impl std::fmt::Display for HandoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEligibleSuccessor(r) | Self::Refused(r) | Self::Unconfirmed(r) => write!(f, "{r}"),
        }
    }
}

/// Both sides of the handover on one node-admin: the incumbent's initiator, the successor's and
/// incumbent's doors.
pub struct HandoverDoor {
    /// The fabric's name.
    pub fabric: String,
    /// This admin's path.
    pub me: PathName,
    /// This admin's node id.
    pub node_id: NodeId,
    /// This admin's incarnation.
    pub incarnation: IncarnationId,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// This admin's mesh channel (its seat book, its clock).
    pub membership: Membership,
    /// This admin's place on the backbone.
    pub backbone: Backbone,
    /// Where a seat record is made durable.
    pub storage: Arc<dyn FabricStorage>,
    /// The Node RPC client both calls are made through.
    pub client: Arc<NodeRpcClient>,
    /// What this admin holds of the digests (the election's inputs).
    pub book: DigestBook,
    /// The records the election's inputs come from.
    pub records: Arc<crate::admin::Records>,
    /// This admin's own contact addresses: what its new-fabric-primary announces.
    pub contacts: OwnContacts,
    /// The handovers this node took part in.
    pub handovers: HandoverBook,
    /// The lock of this admin's Build-log decisions (`FabricBuildStateAdapter::decisions`): a seat
    /// commit here holds it across the record's move, so a decision in flight commits wholly before
    /// the record moves. The moved record is the fence; nothing else is written.
    pub decisions: Arc<tokio::sync::Mutex<()>>,
    /// The executors the fabric-primary's claims name: what a TakeSeats caller is checked against.
    pub claims: Arc<dyn WonClaims>,
    /// Projects this admin's view from the records it holds now. Run once a committed seat record is
    /// held, before the call that depends on it is answered: the committed record is the resolver's
    /// authoritative input (fabric-node-lifecycle-elections.md §4.4), and every reader of the view
    /// (the Build door, the claim door, the drive, the executor plan) decides from it.
    pub project_view: ProjectView,
    /// Cuts off and drains the writes of the stores the moving seats write, before the seat row
    /// (R14 item 4), or lifts the cut-off when the row was not written.
    pub cut_off_writes: CutOffWrites,
}

/// What the holder's SeatTransfer step does to the stores its moving seats write (R14 item 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteCutOff {
    /// Refuse every write the moving seats authorize (the mesh-primary's mesh, node and status rows; the
    /// fabric record as well when the fabric seat moves), then take each store's write lock once,
    /// which waits for every write already issued.
    Set {
        /// Why: the transfer, naming the successor and the operation.
        reason: String,
        /// The fabric-primary seat moves too.
        fabric_seat: bool,
    },
    /// The seat row was not written: nothing moved, and the writes are this admin's again.
    Lift,
}

/// Applies a [`WriteCutOff`] to this admin's stores (`crate::admin`'s wiring).
pub type CutOffWrites = Arc<dyn Fn(WriteCutOff) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The file stores on the fabric's store root a seat transfer cuts off (R14 item 4): the
/// mesh-primary seat's (the Mesh row, its members' Node rows and contacts, the mesh's status rows) and the fabric-primary
/// seat's (the Fabric record and its pointers, and every other mesh's rows).
pub struct SeatStores {
    /// The Mesh rows.
    pub mesh: Arc<crate::storage::FileMeshStorage>,
    /// The Node rows and contacts.
    pub nodes: Arc<crate::storage::FileNodesStorage>,
    /// The Fabric record and its pointers.
    pub fabric: Arc<crate::fabric_storage::FileFabricStorage>,
    /// The mesh's status rows.
    pub status: Arc<crate::status_storage::FileStatusStorage>,
}

impl SeatStores {
    /// Apply `c`: set cuts off and drains every store the moving seats write; lift gives them back.
    pub async fn apply(&self, c: WriteCutOff) {
        let mut seat: Vec<&crate::record_store::FileRecords> = self.mesh.mesh_primary_records().into_iter().chain(self.nodes.mesh_primary_records()).chain(self.status.mesh_primary_records()).collect();
        match c {
            WriteCutOff::Set { reason, fabric_seat } => {
                if fabric_seat {
                    seat.extend(self.fabric.fabric_primary_records());
                    // The fabric-primary seat authorizes other meshes' rows on the shared root too.
                    self.nodes.cut_off_other_meshes(&reason).await;
                }
                crate::record_store::cut_off_and_drain(&seat, &reason).await;
            }
            WriteCutOff::Lift => {
                for s in seat.into_iter().chain(self.fabric.fabric_primary_records()) {
                    s.lift_cut_off();
                }
                self.nodes.lift_other_meshes();
            }
        }
    }

    /// The [`CutOffWrites`] a handover door applies through.
    pub fn cut_off_writes(self: Arc<Self>) -> CutOffWrites {
        Arc::new(move |c: WriteCutOff| {
            let stores = self.clone();
            Box::pin(async move { stores.apply(c).await })
        })
    }
}

/// What projects an admin's view from its held records now (`crate::admin::view_projector`).
pub type ProjectView = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

fn birth_of(n: &crate::model::Node) -> Option<HandoverBirth> {
    Some(HandoverBirth { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone()? })
}

fn reject(call: &'static str, side: &'static str, reason: &'static str, detail: &str, p: &Payload) {
    tracing::info_span!(
        "rdm.node_admin.fabric.reject.via-handover",
        call,
        side,
        reason,
        detail,
        fabric = %p.fabric,
        operation = %p.operation,
        incumbent = %format!("{}:{}:{}", p.incumbent.mesh, p.incumbent.node_id, p.incumbent.incarnation.0),
        successor = %format!("{}:{}:{}", p.successor.mesh, p.successor.node_id, p.successor.incarnation.0),
        expected_epoch = p.expected_epoch,
    )
    .in_scope(|| tracing::info!("a fabric-primary handover call was refused"));
}

/// One TakeSeats call, as both sides hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TakeSeatsCall {
    fabric: String,
    holder: HandoverBirth,
    successor: HandoverBirth,
    seats: Vec<SeatEpoch>,
    build_id: String,
    attempt: u32,
    operation: String,
}

impl TakeSeatsCall {
    fn request(&self) -> Request {
        Request::TakeSeats { fabric: self.fabric.clone(), holder: self.holder.clone(), successor: self.successor.clone(), seats: self.seats.clone(), build_id: self.build_id.clone(), attempt: self.attempt, operation: self.operation.clone() }
    }
    fn matches(&self, row: &SeatTransferRow) -> bool {
        row.fabric == self.fabric && row.holder == self.holder && row.successor == self.successor && row.expected == self.seats && row.build_id == self.build_id && row.attempt == self.attempt
    }
}

/// The detail of a refused epoch overflow: who refused, the seat and the epoch that cannot advance.
fn overflow_detail(me: &PathName, seat: &WireSeat, epoch: u64) -> String {
    let mesh = match seat {
        WireSeat::MeshPrimary { mesh } => format!(" of {mesh}"),
        WireSeat::FabricPrimary => String::new(),
    };
    format!("{me}: the {}{mesh} seat is at epoch {epoch}, the largest a seat epoch holds; it cannot advance, so no transfer commits", seat.name())
}

fn births(b: &HandoverBirth) -> String {
    format!("{}:{}:{}", b.mesh, b.node_id, b.incarnation.0)
}

fn epoch_of(seats: &[CommittedSeat], seat: Seat) -> String {
    seats.iter().find(|c| c.seat.kind() == seat).map(|c| c.epoch.to_string()).unwrap_or_default()
}

fn reject_seats(side: &'static str, reason: &'static str, detail: &str, t: &TakeSeatsCall) {
    tracing::info_span!(
        "rdm.node_admin.seat.reject.via-take-seats",
        side,
        reason,
        detail,
        fabric = %t.fabric,
        operation = %t.operation,
        build_id = %t.build_id,
        attempt = t.attempt,
        holder = %births(&t.holder),
        successor = %births(&t.successor),
    )
    .in_scope(|| tracing::info!("a seat transfer call was refused"));
}

impl HandoverDoor {
    fn incumbency(&self) -> crate::election::Incumbency {
        crate::admin::incumbency_of(&self.book, &self.records)
    }

    fn me_birth(&self) -> HandoverBirth {
        HandoverBirth { mesh: self.me.mesh.clone(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }
    }

    /// The birth a payload names as `role`, checked against the birth of that node this admin's view
    /// holds: the exact-birth check a handover makes on the births it names. Nothing checks who is
    /// calling (node-rpc.md §14). `None` when they match; else the refusal detail.
    async fn named_birth_not_held(&self, named: &HandoverBirth) -> Option<String> {
        let held = self.topology.read().await.births().find(|n| n.node_id == named.node_id).cloned();
        match held.as_ref().and_then(birth_of) {
            Some(b) if &b == named => None,
            Some(b) => Some(format!("names {}, this admin's view holds {}", births(named), births(&b))),
            None => Some(format!("names {}, a node this admin's view holds no birth of", births(named))),
        }
    }

    /// Make `stage` of `payload`'s handover durable, then hold it. A stage that could not be made
    /// durable is not held: the caller answers the failure by name.
    async fn record(&self, payload: &Payload, stage: Stage) -> Result<(), String> {
        self.persist(payload, stage).await?;
        self.handovers.put(payload.clone(), stage);
        Ok(())
    }

    /// Make `stage` of `payload`'s handover durable: a blind put of its own row.
    async fn persist(&self, payload: &Payload, stage: Stage) -> Result<(), String> {
        let row = HandoverRow { operation: payload.operation.clone(), fabric: payload.fabric.clone(), incumbent: payload.incumbent.clone(), successor: payload.successor.clone(), expected_epoch: payload.expected_epoch, stage: stage.durable() };
        self.storage.put_handover(&row).await.map_err(|e| format!("{}: handover {} could not be made durable at {stage:?}: {e}", self.me, payload.operation))
    }

    /// Hold the handovers `fabric.storage` records, as this admin started with them. Returns how many
    /// handovers it holds. A row this build does not recognise is refused by name. A seat transfer is
    /// never held here: its committed row is point-read where it lives, by the holder replaying it and
    /// by an executor whose holder is proven lost.
    pub async fn restore(&self) -> Result<usize, String> {
        let rows = self.storage.handovers().await.map_err(|e| format!("fabric.storage handovers: {e}"))?;
        let n = rows.len();
        for r in rows {
            self.handovers.put(Payload { fabric: r.fabric, incumbent: r.incumbent, successor: r.successor, expected_epoch: r.expected_epoch, operation: r.operation }, Stage::of(r.stage));
        }
        Ok(n)
    }

    async fn call(&self, target: &NodeId, req: &Request) -> Result<Reply, String> {
        let (out, _) = self.client.call::<FabricPrimaryHandover>(&NodeTarget::ExactNode(target.clone()), req, &CallOptions::default()).await;
        match out {
            RpcOutcome::Reply(r) => Ok(r.value().clone()),
            other => Err(format!("the {} call to {target} ended {} ({other:?})", req.op(), other.name())),
        }
    }

    // ---- the incumbent: initiate ----

    /// Hand the fabric seat held inside the leaving mesh `mesh` to the eligible outside
    /// mesh-primary, and return only once the transfer is confirmed. The payload is recorded before
    /// the first send and a repeat replays from the record, so an uncertain handover is reconciled
    /// and never started twice.
    pub async fn hand_over(&self, mesh: &str, mesh_id: &impl std::fmt::Display) -> Result<(), HandoverError> {
        let operation = operation(mesh_id);
        let view = self.topology.read().await.clone();
        let nodes: Vec<crate::model::Node> = view.members().cloned().collect();
        let held = self.membership.seats().fabric();
        let recorded = self.handovers.get(&operation);
        let payload = match recorded {
            Some((p, Stage::Confirmed(_))) => {
                let _ = p;
                return Ok(());
            }
            Some((p, _)) => p,
            None => {
                let incumbent = view.fabric_primary().filter(|n| n.node_id == self.node_id).and_then(birth_of).ok_or_else(|| HandoverError::Refused(format!("{} does not hold the fabric seat in its view: it cannot hand it over", self.me)))?;
                let successor = crate::election::fabric_successor_excluding(&nodes, &self.incumbency(), mesh).and_then(|n| birth_of(&n));
                let Some(successor) = successor else {
                    let p = Payload { fabric: self.fabric.clone(), incumbent: incumbent.clone(), successor: incumbent, expected_epoch: held.as_ref().map(|h| h.epoch).unwrap_or(0), operation: operation.clone() };
                    let reason = format!("no Ready mesh-primary outside {mesh} is eligible to take the fabric seat: the handover is blocked and {mesh} keeps running");
                    reject("take", "incumbent", "no-eligible-successor", &reason, &p);
                    return Err(HandoverError::NoEligibleSuccessor(reason));
                };
                let Some(held) = held else {
                    return Err(HandoverError::Refused(format!("{} holds no fabric seat record: the epoch the handover names is unknown", self.me)));
                };
                let p = Payload { fabric: self.fabric.clone(), incumbent, successor, expected_epoch: held.epoch, operation: operation.clone() };
                self.record(&p, Stage::Sent).await.map_err(HandoverError::Refused)?;
                p
            }
        };
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-handover", fabric = %payload.fabric, operation = %payload.operation, expected_epoch = payload.expected_epoch, successor = %payload.successor.node_id, successor_mesh = %payload.successor.mesh);
        async {
            // The events that reconcile an unconfirmed handover: the book moving (the completion),
            // a seat record changing, a member of the fabric becoming reachable or changing. Each
            // is subscribed before the first send, so none is missed.
            let mut book = self.handovers.changed.subscribe();
            let mut seats = self.membership.seats().subscribe();
            let mut heard = self.book.heard_changes();
            loop {
                if matches!(self.handovers.get(&payload.operation), Some((_, Stage::Confirmed(_)))) {
                    return Ok(());
                }
                match self.call(&payload.successor.node_id, &payload.take()).await {
                    Ok(Reply::Applied | Reply::AlreadyApplied) => {}
                    Ok(refusal) => {
                        let (reason, detail) = refusal_reason(&refusal);
                        reject("take", "incumbent", reason, &detail, &payload);
                        return Err(HandoverError::Refused(format!("{} refused {}: {} ({detail})", payload.successor.node_id, payload.take().op(), refusal.name())));
                    }
                    Err(e) => reject("take", "incumbent", "uncertain", &e, &payload),
                }
                if matches!(self.handovers.get(&payload.operation), Some((_, Stage::Confirmed(_)))) {
                    return Ok(());
                }
                let closed = tokio::select! {
                    r = book.changed() => r.is_err().then_some("the handover book"),
                    r = seats.changed() => r.is_err().then_some("the seat book"),
                    r = heard.changed() => r.is_err().then_some("the digest book"),
                };
                if let Some(what) = closed {
                    return Err(HandoverError::Unconfirmed(format!("{}: {what} closed while {} awaited the completion of {}", self.me, payload.operation, payload.successor.node_id)));
                }
            }
        }
        .instrument(span)
        .await
    }

    // ---- the successor: serve the command ----

    async fn serve_take(self: &Arc<Self>, payload: Payload) -> Reply {
        let _one_at_a_time = self.handovers.serving.lock().await;
        let side = "successor";
        let refuse = |reason: &'static str, detail: String, reply: Reply| {
            reject("take", side, reason, &detail, &payload);
            reply
        };
        if payload.fabric != self.fabric {
            return refuse("conflict", format!("the command names fabric {}, this admin is in {}", payload.fabric, self.fabric), Reply::RejectedConflict { operation: payload.operation.clone() });
        }
        if payload.successor != self.me_birth() {
            return refuse("wrong-birth", format!("the command names successor {}:{}:{}, this admin is {}:{}:{}", payload.successor.mesh, payload.successor.node_id, payload.successor.incarnation.0, self.me.mesh, self.node_id, self.incarnation.0), Reply::RejectedWrongBirth { role: "successor".into() });
        }
        if let Some(detail) = self.named_birth_not_held(&payload.incumbent).await {
            return refuse("wrong-birth", format!("the command's incumbent {detail}"), Reply::RejectedWrongBirth { role: "incumbent".into() });
        }
        // Replay lookup precedes stale-epoch rejection.
        if let Some((recorded, stage)) = self.handovers.get(&payload.operation) {
            if recorded != payload {
                return refuse("conflict", "the operation names a different payload".into(), Reply::RejectedConflict { operation: payload.operation.clone() });
            }
            if let Stage::Committed(committed) = stage {
                // The completion was not acknowledged: the same completion is sent again.
                self.spawn_completion(payload.clone(), committed, false);
            }
            return Reply::AlreadyApplied;
        }
        let held = self.membership.seats().fabric();
        let Some(held) = held else {
            return refuse("not-ready", "this admin holds no fabric seat record".into(), Reply::NotReady { reason: format!("{} holds no fabric seat record to compare the command's epoch with", self.me) });
        };
        if !held.is_birth(&payload.incumbent.node_id, &payload.incumbent.incarnation) {
            return refuse("not-authority", format!("the fabric seat is held by {held}, not the caller"), Reply::RejectedNotAuthority);
        }
        if held.epoch != payload.expected_epoch {
            return refuse("stale-epoch", format!("the seat is at epoch {}", held.epoch), Reply::RejectedStaleEpoch { expected: payload.expected_epoch, current: held.epoch });
        }
        let nodes: Vec<crate::model::Node> = self.topology.read().await.members().cloned().collect();
        let selected = crate::election::fabric_successor_excluding(&nodes, &self.incumbency(), &payload.incumbent.mesh);
        if selected.as_ref().and_then(birth_of).as_ref() != Some(&payload.successor) {
            let detail = format!("the leadership calculation without {} seats {}", payload.incumbent.mesh, selected.map(|n| n.name.to_string()).unwrap_or_else(|| "nobody".into()));
            return refuse("not-eligible", detail.clone(), Reply::RejectedNotEligible { reason: format!("{}: {detail}", self.me) });
        }
        // Epoch overflow cannot commit and is refused by name (node-rpc-envelope.md, op 0x21).
        let Some(committed) = payload.expected_epoch.checked_add(1) else {
            return refuse("epoch-overflow", overflow_detail(&self.me, &WireSeat::FabricPrimary, payload.expected_epoch), Reply::RejectedEpochOverflow { seat: WireSeat::FabricPrimary, epoch: payload.expected_epoch });
        };
        let holder = SeatHolder { mesh: payload.successor.mesh.clone(), node_id: payload.successor.node_id.clone(), incarnation: payload.successor.incarnation.clone(), epoch: committed };
        // The record is durable with or before the seat commit, and held only once the seat is.
        if let Err(e) = self.persist(&payload, Stage::Committed(committed)).await {
            return refuse("not-ready", e.clone(), Reply::NotReady { reason: e });
        }
        if let Err(e) = self.storage.put_seat(&SeatRow { seat: Seat::FabricPrimary, holder: holder.clone() }).await {
            return refuse("not-ready", e.to_string(), Reply::NotReady { reason: format!("{}: the fabric seat could not be made durable: {e}", self.me) });
        }
        match self.membership.seats().take(Seat::FabricPrimary, &holder) {
            SeatTaken::Held { .. } | SeatTaken::Same => {}
            SeatTaken::Refused { held } => {
                return refuse("stale-epoch", format!("a later seat record is held: {held}"), Reply::RejectedStaleEpoch { expected: payload.expected_epoch, current: held.epoch });
            }
        }
        // The view is the resolver's output over the records held: it names this admin before the
        // command is answered, so the incumbent's caller finds the seat here once it is told it moved.
        (self.project_view)().await;
        self.handovers.put(payload.clone(), Stage::Committed(committed));
        tracing::info_span!(
            "rdm.node_admin.fabric.update.via-handover-committed",
            fabric = %payload.fabric,
            operation = %payload.operation,
            incumbent = %format!("{}:{}:{}", payload.incumbent.mesh, payload.incumbent.node_id, payload.incumbent.incarnation.0),
            successor = %format!("{}:{}:{}", payload.successor.mesh, payload.successor.node_id, payload.successor.incarnation.0),
            expected_epoch = payload.expected_epoch,
            committed_epoch = committed,
        )
        .in_scope(|| tracing::info!("the fabric seat is committed to this admin at the next epoch"));
        self.spawn_completion(payload, committed, true);
        Reply::Applied
    }

    /// Publish new-fabric-primary (first time only) and call `FabricPrimaryTaken` on the incumbent,
    /// continuing the trace of the command that caused it.
    fn spawn_completion(self: &Arc<Self>, payload: Payload, committed: u64, publish: bool) {
        let door = self.clone();
        let parent = tracing::Span::current();
        tokio::spawn(async move { door.complete(payload, committed, publish).instrument(parent).await });
    }

    async fn complete(&self, payload: Payload, committed: u64, publish: bool) {
        if publish {
            let holder = SeatHolder { mesh: payload.successor.mesh.clone(), node_id: payload.successor.node_id.clone(), incarnation: payload.successor.incarnation.clone(), epoch: committed };
            let c = &self.contacts;
            self.backbone.announce_new_fabric_primary(holder, c.endpoint_id.clone(), c.transport_addr, c.admin_api_base.clone()).await;
        }
        match self.call(&payload.incumbent.node_id, &payload.taken(committed)).await {
            Ok(Reply::Applied | Reply::AlreadyApplied) => {
                if let Err(e) = self.record(&payload, Stage::Confirmed(committed)).await {
                    reject("taken", "successor", "storage", &e, &payload);
                    return;
                }
                tracing::info_span!("rdm.node_admin.fabric.update.via-handover-taken-acknowledged", fabric = %payload.fabric, operation = %payload.operation, committed_epoch = committed)
                    .in_scope(|| tracing::info!("the incumbent acknowledged the completion: the transfer is confirmed"));
            }
            Ok(refusal) => {
                let (reason, detail) = refusal_reason(&refusal);
                reject("taken", "successor", reason, &detail, &payload);
            }
            Err(e) => reject("taken", "successor", "uncertain", &e, &payload),
        }
    }

    /// The gate on the new fabric-primary: a handover this node committed and has not seen
    /// acknowledged is reconciled by sending the identical completion once more; the leave opens
    /// only on a confirmation. No recorded handover leaves nothing to reconcile.
    pub async fn confirmed(&self, mesh_id: &impl std::fmt::Display) -> Result<(), HandoverError> {
        let operation = operation(mesh_id);
        let Some((payload, stage)) = self.handovers.get(&operation) else { return Ok(()) };
        let committed = match stage {
            Stage::Confirmed(_) => return Ok(()),
            Stage::Committed(c) => c,
            Stage::Sent => return Err(HandoverError::Unconfirmed(format!("{operation}: this admin sent the command and holds no confirmation"))),
        };
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-handover-reconcile", fabric = %payload.fabric, operation = %operation, committed_epoch = committed);
        async {
            // The record says committed: the durable fabric seat must say so too before anything is sent.
            let seats = self.storage.seats().await.map_err(|e| HandoverError::Unconfirmed(format!("{operation}: the durable fabric seat could not be read: {e}")))?;
            let committed_seat = seats.iter().any(|r| r.seat == Seat::FabricPrimary && (r.holder.epoch > committed || (r.holder.epoch == committed && r.holder.is_birth(&payload.successor.node_id, &payload.successor.incarnation))));
            if !committed_seat {
                let held = seats.iter().find(|r| r.seat == Seat::FabricPrimary).map(|r| r.holder.to_string()).unwrap_or_else(|| "absent".into());
                let reason = format!("{operation}: the handover record says committed at epoch {committed}, the durable fabric seat is {held}");
                reject("taken", "successor", "seat-not-durable", &reason, &payload);
                return Err(HandoverError::Unconfirmed(reason));
            }
            match self.call(&payload.incumbent.node_id, &payload.taken(committed)).await {
                Ok(Reply::Applied | Reply::AlreadyApplied) => {
                    self.record(&payload, Stage::Confirmed(committed)).await.map_err(HandoverError::Unconfirmed)
                }
                Ok(refusal) => {
                    let (reason, detail) = refusal_reason(&refusal);
                    reject("taken", "successor", reason, &detail, &payload);
                    Err(HandoverError::Refused(format!("{}: the incumbent refused the completion: {} ({detail})", operation, refusal.name())))
                }
                Err(e) => {
                    reject("taken", "successor", "uncertain", &e, &payload);
                    Err(HandoverError::Unconfirmed(e))
                }
            }
        }
        .instrument(span)
        .await
    }

    // ---- the incumbent: serve the completion ----

    async fn serve_taken(&self, payload: Payload, committed: u64) -> Reply {
        let _one_at_a_time = self.handovers.serving.lock().await;
        let side = "incumbent";
        let refuse = |reason: &'static str, detail: String, reply: Reply| {
            reject("taken", side, reason, &detail, &payload);
            reply
        };
        if payload.fabric != self.fabric {
            return refuse("conflict", format!("the completion names fabric {}, this admin is in {}", payload.fabric, self.fabric), Reply::RejectedConflict { operation: payload.operation.clone() });
        }
        if payload.incumbent != self.me_birth() {
            return refuse("wrong-birth", format!("the completion names incumbent {}:{}:{}, this admin is {}:{}:{}", payload.incumbent.mesh, payload.incumbent.node_id, payload.incumbent.incarnation.0, self.me.mesh, self.node_id, self.incarnation.0), Reply::RejectedWrongBirth { role: "incumbent".into() });
        }
        if let Some(detail) = self.named_birth_not_held(&payload.successor).await {
            return refuse("wrong-birth", format!("the completion's successor {detail}"), Reply::RejectedWrongBirth { role: "successor".into() });
        }
        let recorded = self.handovers.get(&payload.operation);
        if let Some((rec, stage)) = &recorded {
            if rec != &payload {
                return refuse("conflict", "the operation names a different payload".into(), Reply::RejectedConflict { operation: payload.operation.clone() });
            }
            if let Stage::Confirmed(c) = stage {
                // Replay lookup precedes stale-epoch rejection.
                return if *c == committed { Reply::AlreadyApplied } else { refuse("stale-epoch", format!("the confirmed transfer is at epoch {c}"), Reply::RejectedStaleEpoch { expected: *c, current: committed }) };
            }
        }
        // The transfer this completion names cannot have committed past an epoch at its maximum.
        let Some(next) = payload.expected_epoch.checked_add(1) else {
            return refuse("epoch-overflow", overflow_detail(&self.me, &WireSeat::FabricPrimary, payload.expected_epoch), Reply::RejectedEpochOverflow { seat: WireSeat::FabricPrimary, epoch: payload.expected_epoch });
        };
        if committed != next {
            return refuse("stale-epoch", format!("the transfer commits epoch {next}"), Reply::RejectedStaleEpoch { expected: next, current: committed });
        }
        let holder = SeatHolder { mesh: payload.successor.mesh.clone(), node_id: payload.successor.node_id.clone(), incarnation: payload.successor.incarnation.clone(), epoch: committed };
        // A completion nothing recorded (a restarted incumbent) must still match the seat it held.
        if recorded.is_none() {
            let held = self.membership.seats().fabric();
            let consistent = held.as_ref().is_some_and(|h| (h.is_birth(&payload.incumbent.node_id, &payload.incumbent.incarnation) && h.epoch == payload.expected_epoch) || h == &holder);
            if !consistent {
                return refuse("not-authority", format!("this admin's fabric seat record is {}", held.map(|h| h.to_string()).unwrap_or_else(|| "absent".into())), Reply::RejectedNotAuthority);
            }
        }
        // The committed transfer fences this admin's fabric writes: its seat record moves, under the
        // Build log's decision lock, and the moved record is the fence.
        let taken = {
            let _decisions = self.decisions.clone().lock_owned().await;
            self.membership.seats().take(Seat::FabricPrimary, &holder)
        };
        if let SeatTaken::Refused { held } = taken {
            return refuse("stale-epoch", format!("a later seat record is held: {held}"), Reply::RejectedStaleEpoch { expected: committed, current: held.epoch });
        }
        (self.project_view)().await;
        if let Err(e) = self.record(&payload, Stage::Confirmed(committed)).await {
            return refuse("not-ready", e.clone(), Reply::NotReady { reason: e });
        }
        tracing::info_span!(
            "rdm.node_admin.fabric.update.via-handover-confirmed",
            fabric = %payload.fabric,
            operation = %payload.operation,
            incumbent = %format!("{}:{}:{}", payload.incumbent.mesh, payload.incumbent.node_id, payload.incumbent.incarnation.0),
            successor = %format!("{}:{}:{}", payload.successor.mesh, payload.successor.node_id, payload.successor.incarnation.0),
            expected_epoch = payload.expected_epoch,
            committed_epoch = committed,
        )
        .in_scope(|| tracing::info!("the committed transfer matches the handover: this admin yields fabric authority and keeps its mesh-primary role"));
        Reply::Applied
    }

    // ---- the seat holder: serve TakeSeats ----

    async fn serve_take_seats(self: &Arc<Self>, t: TakeSeatsCall) -> Reply {
        let _one_at_a_time = self.handovers.serving.lock().await;
        let refuse = |reason: &'static str, detail: String, reply: Reply| {
            reject_seats("holder", reason, &detail, &t);
            reply
        };
        if t.fabric != self.fabric {
            return refuse("conflict", format!("the call names fabric {}, this admin is in {}", t.fabric, self.fabric), Reply::RejectedConflict { operation: t.operation.clone() });
        }
        if t.holder != self.me_birth() {
            return refuse("wrong-birth", format!("the call names holder {}, this admin is {}", births(&t.holder), births(&self.me_birth())), Reply::RejectedWrongBirth { role: "holder".into() });
        }
        // The successor the call names, at its exact birth as this admin's view holds it: N, whose
        // path.name the Won claim names. Nothing checks who is calling (node-rpc.md §14).
        let successor = self.topology.read().await.births().find(|n| n.node_id == t.successor.node_id).cloned();
        let Some(successor) = successor.filter(|n| birth_of(n).as_ref() == Some(&t.successor)) else {
            let detail = self.named_birth_not_held(&t.successor).await.unwrap_or_default();
            return refuse("wrong-birth", format!("the call's successor {detail}"), Reply::RejectedWrongBirth { role: "successor".into() });
        };
        // An identical retry is answered from the row before any other check: a point read of the
        // operation's row under this holder's mesh.
        let recorded = match self.storage.seat_transfer(&self.me.mesh, &t.operation).await {
            Ok(r) => r,
            Err(e) => return refuse("not-ready", e.to_string(), Reply::NotReady { reason: format!("{}: the seat-transfer row of {} under mesh {} could not be read: {e}", self.me, t.operation, self.me.mesh) }),
        };
        if let Some(row) = recorded {
            if !t.matches(&row) {
                return refuse("conflict", format!("operation {} committed another payload (Build {} attempt {}, successor {})", t.operation, row.build_id, row.attempt, births(&row.successor)), Reply::RejectedConflict { operation: t.operation.clone() });
            }
            self.committed_span(&t, &row.seats, true);
            return Reply::SeatsCommitted { seats: row.seats };
        }
        // N must be the executor the attempt's Won claim names ...
        match self.claims.executor(&t.build_id, t.attempt).await {
            Ok(Some(e)) if e == successor.name.to_string() => {}
            Ok(Some(e)) => {
                let detail = format!("the Won claim of attempt {} of Build {} names {e}, not {}", t.attempt, t.build_id, successor.name);
                return refuse("not-the-claimed-executor", detail.clone(), Reply::RejectedNotEligible { reason: format!("{}: {detail}", self.me) });
            }
            Ok(None) => {
                let detail = format!("no Won claim of attempt {} of Build {} is known to this admin or its fabric-primary", t.attempt, t.build_id);
                return refuse("not-the-claimed-executor", detail.clone(), Reply::RejectedNotEligible { reason: format!("{}: {detail}", self.me) });
            }
            Err(e) => return refuse("not-ready", e.clone(), Reply::NotReady { reason: format!("{}: the claim of attempt {} of Build {} could not be read: {e}", self.me, t.attempt, t.build_id) }),
        }
        // ... and the admin the election seats once this birth is gone, inside this mesh.
        let view = self.topology.read().await.clone();
        let seated = crate::executor::seat_successor(&self.me, &view);
        if seated.as_ref().and_then(birth_of).as_ref() != Some(&t.successor) {
            let detail = format!("the election seats {} once {} is gone, not {}", seated.map(|n| n.name.to_string()).unwrap_or_else(|| "no admin of its mesh".into()), self.me, successor.name);
            return refuse("not-seated-once-gone", detail.clone(), Reply::RejectedNotEligible { reason: format!("{}: {detail}", self.me) });
        }
        // Every seat this birth holds is named, at the epoch it holds.
        let book = self.membership.seats();
        let mine = |h: &Option<SeatHolder>| h.clone().filter(|h| h.is_birth(&self.node_id, &self.incarnation));
        let held: Vec<(Seat, SeatHolder)> = [(Seat::MeshPrimary, mine(&book.mesh(&self.me.mesh))), (Seat::FabricPrimary, mine(&book.fabric()))].into_iter().filter_map(|(s, h)| h.map(|h| (s, h))).collect();
        if held.is_empty() {
            return refuse("not-authority", format!("{} holds no seat", self.me), Reply::RejectedNotAuthority);
        }
        for (seat, h) in &held {
            if !t.seats.iter().any(|e| e.seat == WireSeat::of(*seat, &self.me.mesh)) {
                return refuse("stale-seat", format!("the call does not name the {} seat this admin holds at epoch {}", seat.name(), h.epoch), Reply::RejectedStaleSeat { seat: WireSeat::of(*seat, &self.me.mesh), expected: 0, current: h.epoch });
            }
        }
        let mut committed = Vec::new();
        for e in &t.seats {
            let Some((_, h)) = held.iter().find(|(s, _)| WireSeat::of(*s, &self.me.mesh) == e.seat) else {
                return refuse("not-authority", format!("{} does not hold the {} seat", self.me, e.seat.name()), Reply::RejectedNotAuthority);
            };
            if h.epoch != e.expected_epoch {
                return refuse("stale-seat", format!("the {} seat is at epoch {}", e.seat.name(), h.epoch), Reply::RejectedStaleSeat { seat: e.seat.clone(), expected: e.expected_epoch, current: h.epoch });
            }
            // The row moves each seat at its next epoch: a seat at its maximum cannot move, and
            // nothing is cut off or written for a transfer that cannot commit.
            let Some(next) = h.epoch.checked_add(1) else {
                return refuse("epoch-overflow", overflow_detail(&self.me, &e.seat, h.epoch), Reply::RejectedEpochOverflow { seat: e.seat.clone(), epoch: h.epoch });
            };
            committed.push(CommittedSeat { seat: e.seat.clone(), holder: t.successor.clone(), epoch: next });
        }
        // The cut-off and the drain (R14 item 4): every write the moving seats authorize is refused
        // from here, and each of those stores' write locks is taken once, which waits for every
        // write already issued (each holds its lock until its disk job ends). So no write of the old
        // seat lands after the row.
        let fabric_seat = committed.iter().any(|c| c.seat.kind() == Seat::FabricPrimary);
        let reason = format!("the seats of {} moved to {} {} by {}", self.me, t.successor.mesh, t.successor.node_id, t.operation);
        (self.cut_off_writes)(WriteCutOff::Set { reason, fabric_seat }).await;
        tracing::info_span!("rdm.node_admin.seat.update.via-writes-drained", fabric = %t.fabric, operation = %t.operation, build_id = %t.build_id, attempt = t.attempt, holder = %births(&t.holder), successor = %births(&t.successor), fabric_seat)
            .in_scope(|| tracing::info!("the holder cut off the writes its moving seats authorize and every issued one finished before the seat row"));
        // The one row: the fence and the commit. Before it nothing has moved; after it the row is the answer.
        let row = SeatTransferRow { operation: t.operation.clone(), fabric: t.fabric.clone(), build_id: t.build_id.clone(), attempt: t.attempt, holder: t.holder.clone(), successor: t.successor.clone(), expected: t.seats.clone(), seats: committed.clone() };
        // Build dispatch from the old seat ends at the row: the row and the records it moves are
        // written under the Build log's decision lock, and the moved fabric record is the fence.
        let decisions = self.decisions.clone().lock_owned().await;
        if let Err(e) = self.storage.put_seat_transfer(&row).await {
            // Nothing moved: the writes are this admin's again.
            (self.cut_off_writes)(WriteCutOff::Lift).await;
            return refuse("not-ready", e.to_string(), Reply::NotReady { reason: format!("{}: the seat transfer {} could not be made durable: {e}", self.me, t.operation) });
        }
        let op = LifecycleOp { build_id: t.build_id.clone(), attempt: t.attempt, operation: t.operation.clone(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), name: self.me.clone(), event_at_rafka_ms: self.membership.clock().now_rafka_ms() };
        let successor = committed[0].record();
        book.take_together(&op, &successor, &committed, true);
        drop(decisions);
        (self.project_view)().await;
        self.committed_span(&t, &committed, false);
        self.backbone.announce_seats_together(op, successor, committed.clone()).await;
        Reply::SeatsCommitted { seats: committed }
    }

    fn committed_span(&self, t: &TakeSeatsCall, seats: &[CommittedSeat], replay: bool) {
        tracing::info_span!(
            "rdm.node_admin.seat.update.via-seats-committed",
            fabric = %t.fabric,
            operation = %t.operation,
            build_id = %t.build_id,
            attempt = t.attempt,
            holder = %births(&t.holder),
            successor = %births(&t.successor),
            mesh_epoch = %epoch_of(seats, Seat::MeshPrimary),
            fabric_epoch = %epoch_of(seats, Seat::FabricPrimary),
            replay,
        )
        .in_scope(|| tracing::info!("the seat holder committed its seats to the executor in one row"));
    }

    // ---- the executor: the SeatTransfer step ----

    /// The SeatTransfer step of a drain, stop, restart or replace of `holder` executed by this admin:
    /// TakeSeats to the holder for every seat this admin's records say it holds, the committed seats
    /// installed from the reply. A lost reply is retried with the identical call, woken by a seat
    /// record or a heard digest changing; once the holder's birth is proven gone the seat records
    /// held here are the answer. A refusal ends the step by name.
    pub async fn take_seats(&self, holder: &crate::model::Node, build_id: &str, attempt: u32, operation: &str) -> Result<SeatTransferOutcome, String> {
        let Some(hb) = birth_of(holder) else { return Err(format!("{} has no known birth: its seats cannot be named", holder.name)) };
        let book = self.membership.seats();
        let is_holder = |h: &Option<SeatHolder>| h.clone().filter(|h| h.is_birth(&hb.node_id, &hb.incarnation));
        let seats: Vec<SeatEpoch> = [(Seat::MeshPrimary, is_holder(&book.mesh(&holder.mesh))), (Seat::FabricPrimary, is_holder(&book.fabric()))]
            .into_iter()
            .filter_map(|(seat, h)| h.map(|h| SeatEpoch { seat: WireSeat::of(seat, &holder.mesh), expected_epoch: h.epoch }))
            .collect();
        if seats.is_empty() {
            return Ok(SeatTransferOutcome::NotAHolder);
        }
        let t = TakeSeatsCall { fabric: self.fabric.clone(), holder: hb.clone(), successor: self.me_birth(), seats, build_id: build_id.to_string(), attempt, operation: operation.to_string() };
        if holder.mesh != self.me.mesh {
            // TakeSeats moves seats inside the holder's mesh only (node-rpc-envelope.md, Boundary): the
            // fabric seat leaves the holder's mesh only through TakeFabricPrimary inside a mesh-leave.
            if t.seats.iter().any(|e| e.seat.kind() == Seat::FabricPrimary) {
                let detail = format!(
                    "{} holds the fabric-primary seat in mesh {} and this executor ({}) is of mesh {}: TakeSeats moves seats inside the holder's mesh only, and the fabric seat leaves mesh {} only through TakeFabricPrimary inside a mesh-leave; nothing is drained",
                    holder.name, holder.mesh, self.me, self.me.mesh, holder.mesh
                );
                reject_seats("executor", "other-mesh-fabric-seat", &detail, &t);
                return Err(detail);
            }
            // Its own mesh's seat alone, and no other admin of that mesh executes: the seat stays with
            // it until its birth is gone and the ordinary election seats the next.
            return Ok(SeatTransferOutcome::OtherMesh { holder_mesh: holder.mesh.clone() });
        }
        let req = t.request();
        let mut seats_changed = book.subscribe();
        let mut heard = self.book.heard_changes();
        loop {
            // A holder whose birth is proven lost is never called: its committed row is the answer.
            if self.incumbency().lost.contains(&hb.incarnation) {
                return self.committed_row(&t, &holder.name).await;
            }
            match self.call(&hb.node_id, &req).await {
                Ok(Reply::SeatsCommitted { seats }) => {
                    let op = LifecycleOp { build_id: t.build_id.clone(), attempt, operation: t.operation.clone(), node_id: hb.node_id.clone(), incarnation: hb.incarnation.clone(), name: holder.name.clone(), event_at_rafka_ms: self.membership.clock().now_rafka_ms() };
                    let successor = seats.first().map(|c| c.record()).ok_or_else(|| format!("{} answered SeatsCommitted with no seat", holder.name))?;
                    let taken = book.take_together(&op, &successor, &seats, false);
                    // The executor's view names it in the committed seats before its next step runs, and before
                    // the caller resubmits the Build here when the holder was the fabric-primary.
                    (self.project_view)().await;
                    tracing::info_span!(
                        "rdm.node_admin.seat.update.via-seats-installed",
                        fabric = %t.fabric,
                        operation = %t.operation,
                        build_id = %t.build_id,
                        attempt,
                        holder = %births(&t.holder),
                        successor = %births(&t.successor),
                        mesh_epoch = %epoch_of(&seats, Seat::MeshPrimary),
                        fabric_epoch = %epoch_of(&seats, Seat::FabricPrimary),
                        outcome = if taken.iter().any(|x| matches!(x, SeatTaken::Refused { .. })) { "refused-not-newer" } else { "held" },
                    )
                    .in_scope(|| tracing::info!("the executor installed the seats the holder committed"));
                    return Ok(SeatTransferOutcome::Committed { seats });
                }
                Ok(refusal) => {
                    let (reason, detail) = refusal_reason(&refusal);
                    reject_seats("executor", reason, &detail, &t);
                    return Err(format!("{} refused take-seats: {} ({detail})", holder.name, refusal.name()));
                }
                Err(uncertain) => reject_seats("executor", "uncertain", &uncertain, &t),
            }
            let closed = tokio::select! {
                r = seats_changed.changed() => r.is_err().then_some("the seat book"),
                r = heard.changed() => r.is_err().then_some("the digest book"),
            };
            if let Some(what) = closed {
                return Err(format!("{}: {what} closed while the seat transfer {} to {} was uncertain", self.me, t.operation, holder.name));
            }
        }
    }

    /// The transfer a proven-lost holder committed, as its committed seat row says (node-rpc-envelope.md,
    /// TakeSeats: "once P's birth is proven lost it reads the committed seat row. A row naming N is the
    /// committed result and N installs it; no row means the transfer never happened"): a point read of
    /// the operation's row under the holder's mesh on the fabric's store root. A row naming another
    /// executor or another holder birth is refused by name; a row that cannot be read is an error, never
    /// "no transfer".
    async fn committed_row(&self, t: &TakeSeatsCall, holder_name: &PathName) -> Result<SeatTransferOutcome, String> {
        let row = self
            .storage
            .seat_transfer(&t.holder.mesh, &t.operation)
            .await
            .map_err(|e| format!("{}: the seat-transfer row of {} under mesh {} (holder {} proven lost) could not be read: {e}", self.me, t.operation, t.holder.mesh, holder_name))?;
        let Some(row) = row else {
            tracing::info_span!("rdm.node_admin.seat.resolve.via-committed-row", fabric = %t.fabric, operation = %t.operation, build_id = %t.build_id, attempt = t.attempt, holder = %births(&t.holder), successor = %births(&t.successor), outcome = "no-row")
                .in_scope(|| tracing::info!("the proven-lost holder committed no seat-transfer row for the operation: no transfer happened"));
            return Ok(SeatTransferOutcome::NoTransfer);
        };
        if row.holder != t.holder || row.successor != t.successor {
            let detail = format!(
                "the committed seat-transfer row of {} under mesh {} moves the seats of {} to {}, and this executor's call names holder {} and successor {}",
                t.operation,
                t.holder.mesh,
                births(&row.holder),
                births(&row.successor),
                births(&t.holder),
                births(&t.successor)
            );
            reject_seats("executor", "committed-to-another", &detail, t);
            return Err(format!("{}: {detail}", self.me));
        }
        let op = LifecycleOp { build_id: row.build_id.clone(), attempt: row.attempt, operation: row.operation.clone(), node_id: t.holder.node_id.clone(), incarnation: t.holder.incarnation.clone(), name: holder_name.clone(), event_at_rafka_ms: self.membership.clock().now_rafka_ms() };
        let successor = row.seats.first().map(|c| c.record()).ok_or_else(|| format!("{}: the committed seat-transfer row of {} names no seat", self.me, t.operation))?;
        let taken = self.membership.seats().take_together(&op, &successor, &row.seats, false);
        (self.project_view)().await;
        tracing::info_span!(
            "rdm.node_admin.seat.update.via-committed-row",
            fabric = %t.fabric,
            operation = %t.operation,
            build_id = %row.build_id,
            attempt = row.attempt,
            holder = %births(&row.holder),
            successor = %births(&row.successor),
            mesh_epoch = %epoch_of(&row.seats, Seat::MeshPrimary),
            fabric_epoch = %epoch_of(&row.seats, Seat::FabricPrimary),
            outcome = if taken.iter().any(|x| matches!(x, SeatTaken::Refused { .. })) { "refused-not-newer" } else { "held" },
        )
        .in_scope(|| tracing::info!("the executor read the proven-lost holder's committed seat row and installed the seats it names"));
        Ok(SeatTransferOutcome::CommittedRow { seats: row.seats })
    }

    /// A request off the wire, decided on the births it names, whoever calls (node-rpc.md §14).
    pub async fn serve(self: &Arc<Self>, req: Request) -> Reply {
        match req {
            Request::TakeFabricPrimary { fabric, incumbent, successor, expected_epoch, operation } => self.serve_take(Payload { fabric, incumbent, successor, expected_epoch, operation }).await,
            Request::FabricPrimaryTaken { fabric, incumbent, successor, expected_epoch, committed_epoch, operation } => self.serve_taken(Payload { fabric, incumbent, successor, expected_epoch, operation }, committed_epoch).await,
            Request::TakeSeats { fabric, holder, successor, seats, build_id, attempt, operation } => self.serve_take_seats(TakeSeatsCall { fabric, holder, successor, seats, build_id, attempt, operation }).await,
        }
    }
}

fn refusal_reason(r: &Reply) -> (&'static str, String) {
    match r {
        Reply::RejectedStaleEpoch { expected, current } => ("stale-epoch", format!("expected {expected}, current {current}")),
        Reply::RejectedWrongBirth { role } => ("wrong-birth", role.clone()),
        Reply::RejectedNotAuthority => ("not-authority", String::new()),
        Reply::RejectedNotEligible { reason } => ("not-eligible", reason.clone()),
        Reply::RejectedConflict { operation } => ("conflict", operation.clone()),
        Reply::NotReady { reason } => ("not-ready", reason.clone()),
        Reply::Unauthorized { reason } => ("unauthorized", reason.clone()),
        Reply::PeerUnresolved { reason } => ("peer-unresolved", reason.clone()),
        Reply::Busy { reason } => ("busy", reason.clone()),
        Reply::Draining { reason } => ("draining", reason.clone()),
        Reply::Malformed { kind } => ("malformed", format!("{kind:?}")),
        Reply::RejectedStaleSeat { seat, expected, current } => ("stale-seat", format!("{}: expected {expected}, current {current}", seat.name())),
        Reply::RejectedEpochOverflow { seat, epoch } => ("epoch-overflow", format!("{}: epoch {epoch} cannot advance", seat.name())),
        Reply::Applied | Reply::AlreadyApplied | Reply::SeatsCommitted { .. } => ("applied", String::new()),
    }
}

/// The door a running admin fills once it holds a view.
pub type HandoverSlot = Arc<OnceLock<Arc<HandoverDoor>>>;

/// Serve the fabric-primary handover on this admin. Until `slot` is filled a call is `NotReady`.
pub fn serve(b: ServerBuilder, slot: HandoverSlot) -> ServerBuilder {
    b.serve::<FabricPrimaryHandover, _, _>(OpOwner::Product("rdm".into()), move |_peer: PeerContext, req: Request| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(Reply::NotReady { reason: "this admin holds no view yet".into() });
            };
            Ok(door.serve(req).await)
        }
    })
}
