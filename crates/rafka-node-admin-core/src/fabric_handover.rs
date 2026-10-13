//! node.primary.update and node.fabric-primary.update on op `0x21` (the approved proposal
//! rafka-v2 docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md, Part C, Luke yes
//! 2026-10-12): "One writer, one row, one point of no return." For the node-admin cohort the holder
//! NA1 is its mesh's row writer (R-OW1, R14), so it writes the one committed row itself; the commander
//! is the fabric-primary (a fabric-primary moving its own seats commands itself, having nothing above
//! it).
//!
//! ```text
//! fabric-primary              NA1 (holder, row writer)                   NA2 (successor)
//!   UpdatePrimary ------------> 2 pre-check: the commanded successor search (holder excluded)
//!                                 none -> refused by name (Q7), nothing changed
//!                               3 "primary updating" (mesh channel, backbone)
//!                               4 cut off its seat's writes, every issued one finished (R14 item 4)
//!                               5 ONE row meshes/<mesh>/seat-transfers/<operation>, one or both seats
//!                                 (the commit: NA1 is no longer primary), under its Build-log lock
//!                               6 PrimaryUpdate (the row's content) -----> point-read the row; install
//!                                                                          from it
//!                                 <--------------------------------------- PrimaryUpdated (node.primary.updated)
//!                                                                        7 "primary updated"
//!   <------------------------- PrimaryUpdated
//!   install, then dispatch what the update preceded (8)
//! ```
//!
//! The successor refuses only a conflicting payload and a fabric not first ready-for-traffic; it never
//! refuses a call for the birth it names (R-UC1a, R-RX5). After the row the seat is the successor's
//! whatever the call returns: a call that does not answer node.primary.updated ends the update
//! `Incomplete`, and a later attempt replays from the row (an identical call answers from it). A
//! resuming executor that reads the row of a holder proven gone has discovered the commit, not
//! installed it: it calls PrimaryUpdate on the successor and installs only on node.primary.updated.
//! "primary updated" is published by the holder once the successor answered node.primary.updated
//! (C1 step 7; R-MT1: "NA1 gossips that the primary updated to NA2"). The holder lost after the row,
//! before "updated": the next writer to read the row publishes it (C1 failure contract). That is the
//! successor, from the row's epoch the cohort's row writer, when it installs from the row of a holder
//! whose exit-proof row it reads; and the holder itself when it restarts over the store root and
//! reads its own committed row as it enters ([`enter_seats`]).

pub use crate::election::Moving;
use crate::fabric_storage::{FabricStorage, SeatTransferRow};
use crate::model::{IncarnationId, NodeId, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::{LifecycleOp, Seat};
use rafka_mesh_transport::membership::{Backbone, DigestBook, Membership, SeatBook, SeatTaken};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::handover::{CommittedSeat, HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request, Seat as WireSeat, SeatEpoch, SeatUpdate};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tracing::Instrument as _;

/// The operation identity of the seat update that precedes the Build operation `op_key` of Build
/// `build_id` (R-MT1): node.primary.update for a mesh-primary seat (and both seats, which move in its
/// one row), node.fabric-primary.update for the fabric seat alone. It names the Build: an operation
/// identifies one update, and its seat-transfer row is keyed by it, so a later Build's update of the
/// same seat before the same node operation is a new update, never the replay of an earlier Build's row.
pub fn update_operation(seat: Seat, build_id: &str, op_key: &str) -> String {
    match seat {
        Seat::MeshPrimary => format!("node.primary.update:{build_id}:{op_key}"),
        Seat::FabricPrimary => format!("node.fabric-primary.update:{build_id}:{op_key}"),
    }
}

/// The fabric-primary's bound on reaching the holder and sending it the command (`UpdatePrimary`).
/// The command's reply is the holder's whole update, its own call to the successor included, which
/// runs under that call's own budget; so nothing but the holder's liveness bounds the reply: the
/// transport's idle timeout and keepalive, as for the Build carrier (node-rpc.md §34). A reply bound
/// of its own would end the command before the holder can answer it whenever the successor's call
/// runs to its bound, recording an uncertainty about the holder, which is alive, in place of the
/// holder's answer naming the successor.
const COMMAND_SEND: std::time::Duration = std::time::Duration::from_secs(10);


/// What a receiver had to read or write to decide or commit a call, and why it failed: the fields a
/// `RejectedSourceFailed` reply names. A runtime fact of the receiver, never `NotReady`, which is the
/// readiness door's answer (node-rpc-envelope.md, "Node RPC readiness gate"). A record never written
/// is not a failure of what was read: it is `RejectedRecordAbsent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFailure {
    /// What failed, named with its owner and row.
    pub source: String,
    /// The operation on it that failed, with its subject.
    pub op: String,
    /// Why it failed: the store's or the peer's own error.
    pub cause: String,
}

impl SourceFailure {
    /// The refusal that names this failure, within the reply's budgets.
    pub fn reply(&self) -> Reply {
        Reply::source_failed(&self.source, &self.op, &self.cause)
    }
}

impl std::fmt::Display for SourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}: {}", self.source, self.op, self.cause)
    }
}


/// What the holder's node.primary.update does to the stores its moving seats write (R14 item 4;
/// proposal Part C1 step 4).
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
    /// The seat row was not written: nothing moved, and the writes are this admin's again under the
    /// unchanged epoch; the rows its stores held through the cut-off are still its own (proposal Part
    /// C1 failure contract: "NA1 lifts its cut-off under the unchanged epoch").
    Lift,
    /// The seat row was written: this admin's authority over the moving seats' keys ended at it
    /// (R14 item 4), so every row the cut-off stores still hold is dropped and named, never written.
    Committed {
        /// The cut-off's reason: the transfer, naming the successor and the operation.
        reason: String,
        /// The fabric-primary seat moved too.
        fabric_seat: bool,
    },
}

/// Applies a [`WriteCutOff`] to this admin's stores (`crate::admin`'s wiring).
pub type CutOffWrites = Arc<dyn Fn(WriteCutOff) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The file stores on the fabric's store root a seat transfer cuts off (R14 item 4): the
/// mesh-primary seat's (the Mesh row, its members' Node rows and contacts, the mesh's status rows) and the fabric-primary
/// seat's (the Fabric record, and every other mesh's rows).
pub struct SeatStores {
    /// The Mesh rows.
    pub mesh: Arc<crate::storage::FileMeshStorage>,
    /// The Node rows and contacts.
    pub nodes: Arc<crate::storage::FileNodesStorage>,
    /// The Fabric record.
    pub fabric: Arc<crate::fabric_storage::FileFabricStorage>,
    /// The mesh's status rows.
    pub status: Arc<crate::status_storage::FileStatusStorage>,
}

impl SeatStores {
    /// Apply `c`: set cuts off every store the moving seats write and waits for every write already
    /// issued; lift gives them back.
    pub async fn apply(&self, c: WriteCutOff) {
        // A label's rows of the tags index opened from now on are cut off at their open: set before
        // the seat's stores are collected, so a label store opened in between is in the collection
        // or cut off at its open, never neither.
        if let WriteCutOff::Set { reason, .. } = &c {
            self.nodes.cut_off_own_labels(Some(reason.clone()));
        }
        let nodes = self.nodes.mesh_primary_records();
        let mut seat: Vec<&crate::record_store::FileRecords> = self.mesh.mesh_primary_records().into_iter().chain(nodes.iter()).chain(self.status.mesh_primary_records()).collect();
        match c {
            WriteCutOff::Set { reason, fabric_seat } => {
                if fabric_seat {
                    seat.extend(self.fabric.fabric_primary_records());
                    // The fabric-primary seat authorizes other meshes' rows on the shared root too
                    // (their node rows, and the status rows it writes as acting primary).
                    self.nodes.cut_off_other_meshes(&reason).await;
                    self.status.cut_off_other_meshes(&reason).await;
                }
                crate::record_store::cut_off_and_drain(&seat, &reason).await;
            }
            WriteCutOff::Lift => {
                self.nodes.cut_off_own_labels(None);
                // A lifted store writes the rows it held through a publication made while it was
                // cut off; one that cannot keeps holding them, and says which.
                for s in seat.into_iter().chain(self.fabric.fabric_primary_records()) {
                    if let Err(e) = s.lift_cut_off().await {
                        tracing::info!(error = %e, "a store whose cut-off was lifted could not write the rows it held: they stay held");
                    }
                }
                if let Err(e) = self.nodes.lift_other_meshes().await {
                    tracing::info!(error = %e, "another mesh's rows whose cut-off was lifted could not write the rows they held: they stay held");
                }
                if let Err(e) = self.status.lift_other_meshes().await {
                    tracing::info!(error = %e, "another mesh's status rows whose cut-off was lifted could not write the rows they held: they stay held");
                }
            }
            WriteCutOff::Committed { reason, fabric_seat } => {
                if fabric_seat {
                    seat.extend(self.fabric.fabric_primary_records());
                }
                let mut dropped: Vec<String> = Vec::new();
                for s in seat {
                    let n = s.drop_held().await;
                    if n > 0 {
                        dropped.push(format!("{} {n}", s.store_name()));
                    }
                }
                if fabric_seat {
                    dropped.extend(self.nodes.drop_other_meshes().await.into_iter().map(|(store, n)| format!("{store} {n}")));
                    dropped.extend(self.status.drop_other_meshes().await.into_iter().map(|(store, n)| format!("{store} {n}")));
                }
                if !dropped.is_empty() {
                    tracing::info_span!("rdm.node_admin.seat.reject.via-held-rows", reason = %reason, fabric_seat, dropped = %dropped.join(","))
                        .in_scope(|| tracing::info!("the seat row ended this admin's authority over the moving seats' keys: the rows its stores still held are dropped, never written"));
                }
            }
        }
    }

    /// The [`CutOffWrites`] a node.primary.update door applies through.
    pub fn cut_off_writes(self: Arc<Self>) -> CutOffWrites {
        Arc::new(move |c: WriteCutOff| {
            let stores = self.clone();
            Box::pin(async move { stores.apply(c).await })
        })
    }
}

/// What projects an admin's view from its held records now (`crate::admin::view_projector`).
pub type ProjectView = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// One update at a time at each side: a repeat that arrives while its first runs finds the row and is
/// answered from it.
#[derive(Default)]
pub struct UpdateLocks {
    /// The successor's install from a row.
    serving: tokio::sync::Mutex<()>,
    /// The holder's update.
    updating: tokio::sync::Mutex<()>,
}

/// Every side of node.primary.update on one node-admin: the holder's update, the successor's
/// install, the commander's command and its resume.
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
    /// This admin's place on the backbone: where "primary updating" and "primary updated" are said.
    pub backbone: Backbone,
    /// The fabric's store root, where the one committed row is written and point-read.
    pub storage: Arc<dyn FabricStorage>,
    /// The Node RPC client every call is made through.
    pub client: Arc<NodeRpcClient>,
    /// What this admin holds of the digests (the election's inputs).
    pub book: DigestBook,
    /// The records the election's inputs come from, and whether the initial fabric first reached
    /// ready-for-traffic.
    pub records: Arc<crate::admin::Records>,
    /// One update at a time at each side.
    pub locks: UpdateLocks,
    /// The lock of this admin's Build-log decisions (`FabricBuildStateAdapter::decisions`): the holder
    /// holds it across its row and the move of its records, so a decision in flight commits wholly
    /// before the records move. The moved record is the target every later decision is judged
    /// against; nothing else is written.
    pub decisions: Arc<tokio::sync::Mutex<()>>,
    /// Projects this admin's view from the records it holds now. Run once a committed seat record is
    /// held, before the call that depends on it is answered: the committed record is the resolver's
    /// authoritative input (fabric-node-lifecycle-elections.md §4.4), and every reader of the view
    /// (the Build door, the claim door, the drive, the executor plan) decides from it.
    pub project_view: ProjectView,
    /// Cuts off the writes of the stores the moving seats write and waits for every write already
    /// issued, before the seat row
    /// (R14 item 4), or lifts the cut-off when the row was not written.
    pub cut_off_writes: CutOffWrites,
}

fn birth_of(n: &crate::model::Node) -> Option<HandoverBirth> {
    Some(HandoverBirth { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone()? })
}

fn births(b: &HandoverBirth) -> String {
    format!("{}:{}:{}", b.mesh, b.node_id, b.incarnation.as_str())
}

fn epoch_of(seats: &[CommittedSeat], seat: Seat) -> String {
    seats.iter().find(|c| c.seat.kind() == seat).map(|c| c.epoch.to_string()).unwrap_or_default()
}

/// The detail of a refused epoch overflow: who refused, the seat and the epoch that cannot advance.
fn overflow_detail(me: &PathName, seat: &WireSeat, epoch: u64) -> String {
    let mesh = match seat {
        WireSeat::MeshPrimary { mesh } => format!(" of {mesh}"),
        WireSeat::FabricPrimary => String::new(),
    };
    format!("{me}: the {} seat{mesh} is at epoch {epoch}, the largest a seat epoch holds; it cannot advance, so no row commits", seat.name())
}

/// What one refusal span of the primitive names: the update it refuses, as far as it is known.
struct Refusing<'a> {
    operation: &'a str,
    build_id: &'a str,
    attempt: u32,
    holder: Option<&'a HandoverBirth>,
    successor: Option<&'a HandoverBirth>,
}

/// The refusal span of node.primary.update at `side` (`holder` or `successor`), with `reason`, its
/// detail and, for a failed read or write, the three fields the reply names.
fn refused(side: &'static str, reason: &'static str, detail: &str, of: &Refusing<'_>, failed: Option<&SourceFailure>) {
    let span = tracing::info_span!(
        "rdm.node_admin.seat.reject.via-primary-update",
        side,
        reason,
        detail,
        operation = of.operation,
        build_id = of.build_id,
        attempt = of.attempt,
        holder = %of.holder.map(births).unwrap_or_default(),
        successor = %of.successor.map(births).unwrap_or_default(),
        source = tracing::field::Empty,
        op = tracing::field::Empty,
        cause = tracing::field::Empty,
    );
    if let Some(f) = failed {
        span.record("source", f.source.as_str());
        span.record("op", f.op.as_str());
        span.record("cause", f.cause.as_str());
    }
    span.in_scope(|| tracing::info!("node.primary.update was refused"));
}

/// The PrimaryUpdate call carrying `row`'s content (proposal Part C1 step 6).
fn call_of(row: &SeatTransferRow) -> Request {
    Request::PrimaryUpdate {
        seats: row.seats.iter().map(|c| SeatUpdate { seat: c.seat.clone(), holder: row.holder.clone(), successor: row.successor.clone(), epoch: c.epoch }).collect(),
        build_id: row.build_id.clone(),
        attempt: row.attempt,
        operation: row.operation.clone(),
    }
}

/// Whether a PrimaryUpdate's content is `row`'s: the same update, whatever attempt sends it (the
/// operation is the row's key; the Build, both births and every seat at its epoch are compared; the
/// attempt is not part of the update's identity).
fn is_row(row: &SeatTransferRow, seats: &[SeatUpdate], build_id: &str) -> bool {
    row.build_id == build_id && row.seats.len() == seats.len() && seats.iter().zip(&row.seats).all(|(u, c)| u.holder == row.holder && u.successor == row.successor && u.committed() == *c)
}

impl HandoverDoor {
    fn incumbency(&self) -> crate::election::Incumbency {
        crate::admin::incumbency_of(&self.book, &self.records)
    }

    fn me_birth(&self) -> HandoverBirth {
        HandoverBirth { mesh: self.me.mesh.clone(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }
    }

    /// The lifecycle op of an update whose subject is the holder `holder`, named by its path.name.
    fn op_of(&self, holder: &HandoverBirth, name: PathName, build_id: &str, attempt: u32, operation: &str) -> LifecycleOp {
        LifecycleOp { build_id: build_id.to_string(), attempt, operation: operation.to_string(), node_id: holder.node_id.clone(), incarnation: holder.incarnation.clone(), name, event_at_rafka_ms: self.membership.clock().now_rafka_ms() }
    }

    /// The incumbent the installed update of the fabric seat this birth holds names: the update held
    /// together whose fabric seat is the held record (its lifecycle op names the former holder).
    /// `None` when this birth holds no fabric seat or took it by no update.
    fn fabric_seat_incumbent(&self) -> Option<HandoverBirth> {
        let book = self.membership.seats();
        let held = book.fabric().filter(|h| h.is_birth(&self.node_id, &self.incarnation))?;
        book.held_frames().into_iter().find_map(|f| match f {
            rafka_mesh_transport::membership::Frame::PrimaryUpdated { op, seats } if seats.iter().any(|c| c.seat.kind() == Seat::FabricPrimary && c.record() == held) => {
                Some(HandoverBirth { mesh: op.name.mesh.clone(), node_id: op.node_id, incarnation: op.incarnation })
            }
            _ => None,
        })
    }

    /// One call to `target` under `opts`, its outcome as node-rpc-envelope.md "Caller execution and certainty"
    /// reads it: a reply; proof that the request never reached the receiver's dispatch (NotSent, an
    /// unserved op, a stale target); or Indeterminate, committed with no stronger proof. Never sent
    /// again here.
    async fn call_once(&self, target: &NodeId, req: &Request, opts: &CallOptions) -> Called {
        let (out, _) = self.client.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(target.clone()), req, opts).await;
        let why = |o: &RpcOutcome<_>| format!("the {} call to {target} ended {} ({o:?})", req.op(), o.name());
        match out {
            RpcOutcome::Reply(r) => Called::Reply(r.value().clone()),
            o @ RpcOutcome::Indeterminate(_) => Called::Indeterminate(why(&o)),
            o => Called::NotDispatched(why(&o)),
        }
    }

    /// The R-PB1 refusal of every node.primary.update call, at the holder or the successor (`side`):
    /// before the initial fabric first reaches ready-for-traffic no seat moves (proposal Part C1:
    /// "Before the initial fabric first reaches ready-for-traffic, every call is refused
    /// `fabric-not-ready`"), decided on the durable fact, the ready-for-traffic row among this mesh's
    /// status rows on the store root, which the fabric state this admin hears by gossip can lag. `None`:
    /// the fabric has first reached ready-for-traffic. A store that cannot be read is named.
    async fn refused_before_first_ready(&self, side: &'static str, of: &Refusing<'_>) -> Option<Reply> {
        let (fabric_id, status) = {
            let now = self.topology.read().await;
            (now.fabric.id.clone(), now.fabric.status)
        };
        let operation = of.operation;
        match self.records.first_ready_on_root(&fabric_id, status).await {
            Ok(true) => None,
            Ok(false) => {
                let detail = format!("{}: node.primary.update {operation} is refused: the initial fabric has not first reached ready-for-traffic (it is {}, and its mesh's status rows on the store root hold no ready-for-traffic row of fabric {fabric_id}), and no seat moves before it does", self.me, status.name());
                refused(side, "fabric-not-ready", &detail, of, None);
                Some(Reply::fabric_not_ready(&detail))
            }
            Err(e) => {
                let failed = SourceFailure { source: format!("status storage of {}", self.me), op: format!("read whether fabric {fabric_id} first reached ready-for-traffic for node.primary.update {operation}"), cause: e };
                refused(side, "source-failed", &failed.to_string(), of, Some(&failed));
                Some(failed.reply())
            }
        }
    }

    // ---- the successor: install from the row (C1 step 6) ----

    /// node.primary.update at its receiver (proposal Part C1 step 6: "The successor reads the row,
    /// installs itself as primary ... and answers `node.primary.updated { epochs }` on that call"; C3
    /// edit 5: the refusals are a conflicting payload and `fabric-not-ready`, "There is no
    /// RejectedWrongBirth ... no RejectedStaleSeat and no RejectedNotEligible on the receiver").
    /// Whoever calls and whichever birth receives it, the call installs the committed view from the
    /// row the store root holds for its operation under the holder's mesh and answers from it; the
    /// holder, not this receiver, says "primary updated" on the answer (R-MT1), unless the holder the
    /// row names is proven exited: then this receiver, the next writer to read the row, says it (C1
    /// failure contract). A row never written there, or a store that cannot be read, is a runtime fact
    /// named by its own reply, never an answer.
    async fn serve_primary_update(self: &Arc<Self>, seats: Vec<SeatUpdate>, build_id: String, attempt: u32, operation: String) -> Reply {
        let _one_at_a_time = self.locks.serving.lock().await;
        let (named_holder, named_successor) = (seats.first().map(|u| u.holder.clone()), seats.first().map(|u| u.successor.clone()));
        let of = Refusing { operation: &operation, build_id: &build_id, attempt, holder: named_holder.as_ref(), successor: named_successor.as_ref() };
        if let Some(refusal) = self.refused_before_first_ready("successor", &of).await {
            return refusal;
        }
        let Some(holder_mesh) = named_holder.as_ref().map(|h| h.mesh.clone()) else {
            refused("successor", "conflict", &format!("{}: the call names no seat, so it is no committed row's content", self.me), &of, None);
            return Reply::RejectedConflict { operation };
        };
        let row = match self.storage.seat_transfer(&holder_mesh, &operation).await {
            Ok(r) => r,
            Err(e) => {
                let failed = SourceFailure { source: format!("fabric.storage of {}, seat-transfer row {operation} under mesh {holder_mesh}", self.me), op: format!("read the seat-transfer row of {operation}"), cause: e.to_string() };
                refused("successor", "source-failed", &failed.to_string(), &of, Some(&failed));
                return failed.reply();
            }
        };
        let Some(row) = row else {
            let record = format!("the seat-transfer row {operation} under mesh {holder_mesh}");
            refused("successor", "record-absent", &format!("{}: the store root holds no {record}: no row was committed for this call", self.me), &of, None);
            return Reply::record_absent(&record, &self.me.to_string());
        };
        if !is_row(&row, &seats, &build_id) {
            let detail = format!("operation {operation} committed another payload: Build {} attempt {}, holder {}, successor {}, seats {:?}", row.build_id, row.attempt, births(&row.holder), births(&row.successor), row.seats);
            refused("successor", "conflict", &detail, &of, None);
            return Reply::RejectedConflict { operation };
        }
        let name = self.topology.read().await.births().find(|n| n.node_id == row.holder.node_id).map(|n| n.name.clone());
        let Some(name) = name else {
            let record = format!("the node record of {}", births(&row.holder));
            refused("successor", "record-absent", &format!("{} holds no {record}: the update's subject cannot be named", self.me), &of, None);
            return Reply::record_absent(&record, &self.me.to_string());
        };
        let op = self.op_of(&row.holder, name.clone(), &row.build_id, row.attempt, &row.operation);
        let taken = {
            let _decisions = self.decisions.clone().lock_owned().await;
            self.membership.seats().take_together(&op, &row.seats)
        };
        (self.project_view)().await;
        let outcome = if taken.iter().any(|t| matches!(t, SeatTaken::Refused { .. })) {
            "refused-not-newer"
        } else if taken.iter().all(|t| matches!(t, SeatTaken::Same)) {
            "same"
        } else {
            "held"
        };
        // The holder lost after the row, before "updated" (proposal Part C1 failure contract: "the next
        // writer to read the row publishes 'updated'"): from the row's epoch this receiver is the
        // cohort's row writer, and the holder the row names is proven gone only by the provider's exit
        // proof (elections §4.1), its exit-proof row among its mesh's status rows. A row present: this
        // reader says "primary updated". None: the holder is alive and says it on this answer. The read
        // is of the birth the row names as holder, never of whoever calls (R-RX1).
        let exit = self.records.exit_row_of_birth(&row.holder.mesh, &row.holder.node_id, &row.holder.incarnation, &name.to_string()).await;
        let (holder_exit, holder_exit_detail) = match &exit {
            Ok(Some(e)) => ("proven", format!("{} exited, proof {} observed at {} ms", births(&row.holder), e.proof, e.observed_at_ms)),
            Ok(None) => ("none", String::new()),
            Err(e) => ("unreadable", e.clone()),
        };
        tracing::info_span!(
            "rdm.node_admin.seat.update.via-primary-updated",
            node = %self.me,
            fabric = %row.fabric,
            operation = %row.operation,
            build_id = %row.build_id,
            attempt,
            holder = %births(&row.holder),
            successor = %births(&row.successor),
            mesh_epoch = %epoch_of(&row.seats, Seat::MeshPrimary),
            fabric_epoch = %epoch_of(&row.seats, Seat::FabricPrimary),
            replay = outcome != "held",
            outcome,
            holder_exit,
            holder_exit_detail = %holder_exit_detail,
        )
        .in_scope(|| tracing::info!("the receiver installed the committed row and answers node.primary.updated"));
        if holder_exit == "proven" {
            self.backbone.announce_primary_updated(op, row.seats.clone()).await;
        }
        Reply::PrimaryUpdated { epochs: row.seats }
    }

    // ---- the holder: node.primary.update (C1 steps 2-7) ----

    /// node.primary.update commanded from above, off the wire: the seats named, each at the epoch the
    /// commander holds it at. Judged by the seats this holder holds and their epochs, never by the
    /// birth the command names (R-UC1a, R-RX5) nor by who calls (R-RX1).
    async fn serve_update_primary(self: &Arc<Self>, fabric: String, seats: Vec<SeatEpoch>, build_id: String, attempt: u32, operation: String) -> Reply {
        let me = self.me_birth();
        let of = Refusing { operation: &operation, build_id: &build_id, attempt, holder: Some(&me), successor: None };
        if fabric != self.fabric {
            refused("holder", "conflict", &format!("the command names fabric {fabric}, this admin is in {}", self.fabric), &of, None);
            return Reply::RejectedConflict { operation };
        }
        let mut mesh_expected = None;
        let mut fabric_named = false;
        for e in &seats {
            match &e.seat {
                WireSeat::MeshPrimary { mesh } if *mesh == self.me.mesh => mesh_expected = Some(e.expected_epoch),
                WireSeat::FabricPrimary => {
                    fabric_named = true;
                    if let Some(h) = self.membership.seats().fabric().filter(|h| h.is_birth(&self.node_id, &self.incarnation) && h.epoch != e.expected_epoch) {
                        refused("holder", "stale-seat", &format!("{}: the fabric-primary seat is at epoch {}", self.me, h.epoch), &of, None);
                        return Reply::RejectedStaleSeat { seat: e.seat.clone(), expected: e.expected_epoch, current: h.epoch };
                    }
                }
                WireSeat::MeshPrimary { mesh } => {
                    let reason = format!("{}: the command names the mesh-primary seat of {mesh}; this admin is of mesh {}", self.me, self.me.mesh);
                    refused("holder", "not-authority", &reason, &of, None);
                    return Reply::not_authority(&reason);
                }
            }
        }
        let moving = match (mesh_expected.is_some(), fabric_named) {
            (true, true) => Moving::BothSeats,
            (true, false) => Moving::MeshSeat,
            (false, true) => Moving::FabricSeatOutside,
            (false, false) => {
                let reason = format!("{}: the command names no seat to move", self.me);
                refused("holder", "not-authority", &reason, &of, None);
                return Reply::not_authority(&reason);
            }
        };
        match self.update_primary(moving, mesh_expected, &build_id, attempt, &operation).await {
            Ok(epochs) => Reply::PrimaryUpdated { epochs },
            Err(refusal) => refusal,
        }
    }

    /// node.primary.update at the holder of `moving`'s seats, its cohort's row writer (proposal Part
    /// C1 steps 2-7): pre-check the commanded successor, say "primary updating", cut off the writes the
    /// seats authorize and wait for every one already issued, write the ONE committed row under its Build-log lock and take its
    /// seats, then call PrimaryUpdate on the successor and answer its node.primary.updated.
    /// `expected` is the epoch the commander holds the mesh seat at. Refused before the row: nothing
    /// changed. The row write refused: `RejectedSourceFailed`, the cut-off lifted under the unchanged
    /// epoch. After the row the seat is the successor's whatever the call returns: a call with no
    /// node.primary.updated ends `Incomplete`, and a later attempt or an identical retry replays from
    /// the row.
    pub async fn update_primary(&self, moving: Moving, expected: Option<u64>, build_id: &str, attempt: u32, operation: &str) -> Result<Vec<CommittedSeat>, Reply> {
        let span = tracing::info_span!(
            "rdm.node_admin.seat.update.via-primary-updating",
            node = %self.me,
            seat = moving.name(),
            build_id,
            attempt,
            operation,
            successor = tracing::field::Empty,
            successor_node_id = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
        );
        let r = self.update_primary_in(moving, expected, build_id, attempt, operation).instrument(span.clone()).await;
        match &r {
            Ok(_) => span.record("outcome", "updated"),
            Err(incomplete @ Reply::Incomplete { .. }) => span.record("outcome", "incomplete").record("detail", refusal_reason(incomplete).1.as_str()),
            Err(refusal) => span.record("outcome", "refused").record("detail", refusal_reason(refusal).1.as_str()),
        };
        r
    }

    async fn update_primary_in(&self, moving: Moving, expected: Option<u64>, build_id: &str, attempt: u32, operation: &str) -> Result<Vec<CommittedSeat>, Reply> {
        let _one_at_a_time = self.locks.updating.lock().await;
        let me = self.me_birth();
        let book = self.membership.seats();
        let of = Refusing { operation, build_id, attempt, holder: Some(&me), successor: None };
        // Before the initial fabric first reaches ready-for-traffic: refused before anything, the row
        // included; nothing is said, cut off or written.
        if let Some(refusal) = self.refused_before_first_ready("holder", &of).await {
            return Err(refusal);
        }
        // A retry or a later attempt after the row: the row stands, and the update completes from it.
        match self.storage.seat_transfer(&self.me.mesh, operation).await {
            Ok(Some(row)) if row.holder == me => return self.complete_from_row(&row).await,
            Ok(Some(row)) => {
                let detail = format!("operation {operation} committed another update (holder {}, successor {})", births(&row.holder), births(&row.successor));
                refused("holder", "conflict", &detail, &of, None);
                return Err(Reply::RejectedConflict { operation: operation.to_string() });
            }
            Ok(None) => {}
            Err(e) => {
                let failed = SourceFailure { source: format!("fabric.storage of {}, seat-transfer row {operation} under mesh {}", self.me, self.me.mesh), op: format!("read the seat-transfer row of {operation}"), cause: e.to_string() };
                refused("holder", "source-failed", &failed.to_string(), &of, Some(&failed));
                return Err(failed.reply());
            }
        }
        // The seats this holder holds, each at the epoch the row moves it from.
        let mut held = Vec::new();
        if moving.mesh_seat() {
            let wire = WireSeat::of(Seat::MeshPrimary, &self.me.mesh);
            let Some(h) = book.mesh(&self.me.mesh).filter(|h| h.is_birth(&self.node_id, &self.incarnation)) else {
                let reason = format!("{} holds no mesh-primary seat of {}: its record names {}", self.me, self.me.mesh, book.mesh(&self.me.mesh).map(|h| h.to_string()).unwrap_or_else(|| "no holder".into()));
                refused("holder", "not-authority", &reason, &of, None);
                return Err(Reply::not_authority(&reason));
            };
            if let Some(e) = expected.filter(|e| *e != h.epoch) {
                refused("holder", "stale-seat", &format!("{}: the mesh-primary seat is at epoch {}", self.me, h.epoch), &of, None);
                return Err(Reply::RejectedStaleSeat { seat: wire, expected: e, current: h.epoch });
            }
            held.push((wire, h));
        }
        if moving.fabric_seat() {
            let Some(h) = book.fabric().filter(|h| h.is_birth(&self.node_id, &self.incarnation)) else {
                let reason = format!("{} holds no fabric-primary seat: its record names {}", self.me, book.fabric().map(|h| h.to_string()).unwrap_or_else(|| "no holder".into()));
                refused("holder", "not-authority", &reason, &of, None);
                return Err(Reply::not_authority(&reason));
            };
            held.push((WireSeat::FabricPrimary, h));
        }
        for (seat, h) in &held {
            if h.epoch.checked_add(1).is_none() {
                let detail = overflow_detail(&self.me, seat, h.epoch);
                refused("holder", "epoch-overflow", &detail, &of, None);
                return Err(Reply::RejectedEpochOverflow { seat: seat.clone(), epoch: h.epoch });
            }
        }
        // Step 2: the pre-check, from this writer's own view.
        let view = self.topology.read().await.clone();
        let nodes: Vec<crate::model::Node> = view.members().cloned().collect();
        let Some(me_node) = nodes.iter().find(|n| n.node_id == self.node_id).cloned() else {
            let record = format!("the node record of {}", self.me);
            refused("holder", "record-absent", &format!("{} is not a member of its own view", self.me), &of, None);
            return Err(Reply::record_absent(&record, &self.me.to_string()));
        };
        let successor = match crate::election::commanded_successor(&nodes, &self.incumbency(), &me_node, moving) {
            Ok(n) => n,
            Err(text) => {
                refused("holder", "no-successor", &text, &of, None);
                return Err(Reply::no_successor(&text));
            }
        };
        let Some(to) = birth_of(&successor) else {
            let record = format!("the birth of {}", successor.name);
            refused("holder", "record-absent", &format!("{}'s view holds {} with no birth", self.me, successor.name), &of, None);
            return Err(Reply::record_absent(&record, &self.me.to_string()));
        };
        tracing::Span::current().record("successor", successor.name.to_string().as_str()).record("successor_node_id", to.node_id.as_str());
        let seats: Vec<CommittedSeat> = held.iter().map(|(seat, h)| CommittedSeat { seat: seat.clone(), holder: to.clone(), epoch: h.epoch + 1 }).collect();
        let row = SeatTransferRow {
            operation: operation.to_string(),
            fabric: self.fabric.clone(),
            build_id: build_id.to_string(),
            attempt,
            holder: me.clone(),
            successor: to.clone(),
            expected: held.iter().map(|(seat, h)| SeatEpoch { seat: seat.clone(), expected_epoch: h.epoch }).collect(),
            seats: seats.clone(),
        };
        let op = self.op_of(&me, self.me.clone(), build_id, attempt, operation);
        // Step 3: "primary updating", intent only.
        self.backbone.announce_primary_updating(op.clone(), seats.clone()).await;
        // Step 4: the cut-off (R14 item 4): every write the seats authorize is refused from
        // here, and each of those stores' write locks is taken once, which waits for every write
        // already issued. No write of this holder's seats lands after the row.
        let fabric_seat = moving.fabric_seat();
        let reason = format!("the {} seat(s) of {} are updating to {} {} by {operation}", moving.name(), self.me, to.mesh, to.node_id);
        (self.cut_off_writes)(WriteCutOff::Set { reason: reason.clone(), fabric_seat }).await;
        tracing::info_span!("rdm.node_admin.seat.update.via-writes-cut-off", node = %self.me, fabric = %self.fabric, operation, build_id, attempt, holder = %births(&me), successor = %births(&to), fabric_seat)
            .in_scope(|| tracing::info!("the holder cut off the writes its seats authorize and every issued one finished before its row"));
        // Step 5: the ONE row, the commit, under the Build-log decision lock; from it this holder is no
        // longer primary.
        {
            let _decisions = self.decisions.clone().lock_owned().await;
            if let Err(e) = self.storage.put_seat_transfer(&row).await {
                drop(_decisions);
                // A named error is a confirmed non-commit: the writes are this holder's again under the
                // unchanged epoch.
                (self.cut_off_writes)(WriteCutOff::Lift).await;
                let failed = SourceFailure { source: format!("fabric.storage of {}, seat-transfer row {operation} under mesh {}", self.me, self.me.mesh), op: format!("put the seat-transfer row of {operation} moving {} seat(s) to {}", seats.len(), births(&to)), cause: e.to_string() };
                refused("holder", "source-failed", &failed.to_string(), &Refusing { successor: Some(&to), ..of }, Some(&failed));
                return Err(failed.reply());
            }
            book.take_together(&op, &seats);
        }
        (self.cut_off_writes)(WriteCutOff::Committed { reason, fabric_seat }).await;
        tracing::info_span!(
            "rdm.node_admin.seat.update.via-seat-row-written",
            node = %self.me,
            fabric = %self.fabric,
            operation,
            build_id,
            attempt,
            holder = %births(&me),
            successor = %births(&to),
            mesh_epoch = %epoch_of(&seats, Seat::MeshPrimary),
            fabric_epoch = %epoch_of(&seats, Seat::FabricPrimary),
        )
        .in_scope(|| tracing::info!("the holder, its cohort's row writer, wrote the one committed row: the seats are the successor's"));
        (self.project_view)().await;
        // Steps 6-7: the call carrying the row, then "primary updated".
        self.update_successor(&row).await
    }

    /// The holder's steps 6-7 for its committed `row`: PrimaryUpdate carrying it, and on the
    /// successor's node.primary.updated, "primary updated" (R-MT1: "NA2 updates itself ... and answers
    /// node.primary.updated; NA1 gossips that the primary updated to NA2"; the announcer is the
    /// mesh-primary, the holder itself for the node-admin cohort).
    async fn update_successor(&self, row: &SeatTransferRow) -> Result<Vec<CommittedSeat>, Reply> {
        let epochs = self.call_successor(row, "holder").await?;
        let op = self.op_of(&row.holder, self.me.clone(), &row.build_id, row.attempt, &row.operation);
        self.backbone.announce_primary_updated(op, row.seats.clone()).await;
        Ok(epochs)
    }

    /// The update `row` committed, completed from it: this holder takes the row's seats if an earlier
    /// attempt left them untaken (its authority over the cut-off writes ended at the row), then calls
    /// the successor carrying the row and says "primary updated" on its answer.
    async fn complete_from_row(&self, row: &SeatTransferRow) -> Result<Vec<CommittedSeat>, Reply> {
        let op = self.op_of(&row.holder, self.me.clone(), &row.build_id, row.attempt, &row.operation);
        let taken = {
            let _decisions = self.decisions.clone().lock_owned().await;
            self.membership.seats().take_together(&op, &row.seats)
        };
        if taken.iter().any(|t| matches!(t, SeatTaken::Held { .. })) {
            let reason = format!("the seat(s) of {} moved to {} by {}", self.me, births(&row.successor), row.operation);
            (self.cut_off_writes)(WriteCutOff::Committed { reason, fabric_seat: row.seats.iter().any(|c| c.seat.kind() == Seat::FabricPrimary) }).await;
            (self.project_view)().await;
        }
        self.update_successor(row).await
    }

    /// PrimaryUpdate carrying `row`, made once on the row's successor by `side`: its node.primary.updated
    /// is the answer. Anything else ends the update `Incomplete`, named; the seat stays the
    /// successor's from the row and no row is rewritten.
    async fn call_successor(&self, row: &SeatTransferRow, side: &'static str) -> Result<Vec<CommittedSeat>, Reply> {
        let of = Refusing { operation: &row.operation, build_id: &row.build_id, attempt: row.attempt, holder: Some(&row.holder), successor: Some(&row.successor) };
        let (reason, why) = match self.call_once(&row.successor.node_id, &call_of(row), &CallOptions::default()).await {
            Called::Reply(Reply::PrimaryUpdated { epochs }) if !epochs.is_empty() => return Ok(epochs),
            Called::Reply(other) => ("refused", format!("answered {}: {}", other.name(), refusal_reason(&other).1)),
            Called::NotDispatched(why) => ("not-dispatched", why),
            Called::Indeterminate(why) => ("indeterminate", why),
        };
        let detail = format!("{}: node.primary.update {} to {} did not answer node.primary.updated; the seat is the successor's from the row and the update is incomplete until a later attempt obtains its answer: {why}", self.me, row.operation, births(&row.successor));
        refused(side, reason, &detail, &of, None);
        Err(Reply::incomplete(&detail))
    }

    // ---- the fabric-primary: command the update, and resume it ----

    /// The fabric-primary commands the mesh-primary `holder` to update its seat (proposal Part C1 step
    /// 1: "The holder is commanded from above"), before the Build's stop, delete, restart, replace,
    /// or node.connections.delete of it runs: UpdatePrimary to the exact holder birth, whose answer
    /// is the successor's node.primary.updated. The committed seat is installed here from that answer,
    /// so this admin's view names the successor before it dispatches what the update preceded (step
    /// 8). The command is sent once: an uncertain one ends the step incomplete, and the next attempt
    /// sends the identical command, which the holder completes from its row. A holder proven gone
    /// answers nothing: its operation's row is point-read on the fabric's store root, and the
    /// successor's node.primary.updated is obtained on it before anything is installed ("Discovery is
    /// not installation"); with no row no update happened.
    pub async fn command_update_primary(&self, holder: &crate::model::Node, build_id: &str, attempt: u32, operation: &str) -> Result<Vec<CommittedSeat>, String> {
        let span = tracing::info_span!(
            "rdm.node_admin.seat.update.via-primary-update",
            node = %self.me,
            holder = %holder.name,
            holder_node_id = %holder.node_id,
            seat = Seat::MeshPrimary.name(),
            build_id,
            attempt,
            operation,
            successor = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
        );
        let r = self.command_update_primary_in(holder, build_id, attempt, operation).instrument(span.clone()).await;
        match &r {
            Ok((successor, _)) => span.record("outcome", "updated").record("successor", successor.as_str()),
            Err((outcome, e)) => span.record("outcome", *outcome).record("detail", e.as_str()),
        };
        r.map(|(_, seats)| seats).map_err(|(_, e)| e)
    }

    /// The command's end: `Err` carries the outcome its span names (`refused`, `incomplete`,
    /// `no-update`) and why.
    async fn command_update_primary_in(&self, holder: &crate::model::Node, build_id: &str, attempt: u32, operation: &str) -> Result<(String, Vec<CommittedSeat>), (&'static str, String)> {
        let Some(hb) = birth_of(holder) else { return Err(("refused", format!("{} has no known birth: its seat cannot be named", holder.name))) };
        if self.incumbency().lost.contains(&hb.incarnation) {
            return self.committed_row(holder, &hb, build_id, attempt, operation).await;
        }
        let book = self.membership.seats();
        let Some(held) = book.mesh(&holder.mesh).filter(|h| h.is_birth(&hb.node_id, &hb.incarnation)) else {
            return Err(("refused", format!("{}'s seat records name no mesh-primary seat of {} held by {}", self.me, holder.mesh, births(&hb))));
        };
        let req = Request::UpdatePrimary { fabric: self.fabric.clone(), holder: hb.clone(), seats: vec![SeatEpoch { seat: WireSeat::of(Seat::MeshPrimary, &holder.mesh), expected_epoch: held.epoch }], build_id: build_id.to_string(), attempt, operation: operation.to_string() };
        match self.call_once(&hb.node_id, &req, &CallOptions { budget: Budget::Stream { send: COMMAND_SEND }, ..CallOptions::default() }).await {
            Called::Reply(Reply::PrimaryUpdated { epochs }) if !epochs.is_empty() => Ok(self.install_committed(holder, &hb, epochs, build_id, attempt, operation, "primary-updated").await),
            Called::Reply(Reply::Incomplete { reason }) => Err(("incomplete", format!("{}: node.primary.update {operation} of {} is incomplete: {reason}", self.me, holder.name))),
            Called::Reply(refusal) => {
                let (reason, detail) = refusal_reason(&refusal);
                Err(("refused", format!("{} refused node.primary.update: {} ({reason}: {detail})", holder.name, refusal.name())))
            }
            Called::NotDispatched(why) | Called::Indeterminate(why) => Err(("incomplete", format!("{}: node.primary.update {operation} of {} is uncertain and the step is incomplete; a later attempt sends the identical command, or reads the committed row once {} is proven gone: {why}", self.me, holder.name, holder.name))),
        }
    }

    /// What a holder proven gone committed for `operation`: its seat-transfer row under its mesh on the
    /// fabric's store root. A row naming `hb` is discovered, not installed: PrimaryUpdate carrying it
    /// is called on its successor under this read's span, and the seats are installed here only on
    /// the successor's node.primary.updated. None is no update.
    async fn committed_row(&self, holder: &crate::model::Node, hb: &HandoverBirth, build_id: &str, attempt: u32, operation: &str) -> Result<(String, Vec<CommittedSeat>), (&'static str, String)> {
        let span = |outcome: &str, successor: &str| tracing::info_span!("rdm.node_admin.seat.update.via-committed-row", node = %self.me, operation, build_id, attempt, holder = %births(hb), successor, outcome);
        match self.storage.seat_transfer(&holder.mesh, operation).await {
            Ok(Some(row)) if row.holder == *hb && !row.seats.is_empty() => {
                let read = span("held", &births(&row.successor));
                read.in_scope(|| tracing::info!("the fabric-primary read the committed row of a holder proven gone: it obtains the successor's node.primary.updated before it installs"));
                match self.call_successor(&row, "commander").instrument(read).await {
                    Ok(epochs) => Ok(self.install_committed(holder, hb, epochs, build_id, attempt, operation, "committed-row").await),
                    Err(incomplete) => Err(("incomplete", refusal_reason(&incomplete).1)),
                }
            }
            Ok(Some(row)) => {
                span("refused", &births(&row.successor)).in_scope(|| tracing::info!("the committed row of the operation names another holder"));
                Err(("refused", format!("{}: operation {operation} committed the seat of {} to {}, not a seat of {}", self.me, births(&row.holder), births(&row.successor), holder.name)))
            }
            Ok(None) => {
                span("no-row", "").in_scope(|| tracing::info!("no committed row: no update happened"));
                Err(("no-update", format!("{}: {} is proven gone and no seat row records {operation} under mesh {}: no update happened, and the ordinary election seats its successor", self.me, holder.name, holder.mesh)))
            }
            Err(e) => Err(("refused", format!("{}: fabric.storage, the seat-transfer row {operation} under mesh {} could not be read: {e}", self.me, holder.mesh))),
        }
    }

    /// Install the seats `holder`'s update committed (`via` names where the answer came from), so this
    /// admin's view names the successor before it dispatches what the update preceded.
    #[allow(clippy::too_many_arguments)]
    async fn install_committed(&self, holder: &crate::model::Node, hb: &HandoverBirth, seats: Vec<CommittedSeat>, build_id: &str, attempt: u32, operation: &str, via: &str) -> (String, Vec<CommittedSeat>) {
        let op = self.op_of(hb, holder.name.clone(), build_id, attempt, operation);
        let taken = self.membership.seats().take_together(&op, &seats);
        (self.project_view)().await;
        let successor = self.topology.read().await.births().find(|n| n.node_id == seats[0].holder.node_id).map(|n| n.name.to_string()).unwrap_or_else(|| births(&seats[0].holder));
        tracing::info_span!(
            "rdm.node_admin.seat.update.via-seats-installed",
            node = %self.me,
            fabric = %self.fabric,
            operation,
            build_id,
            attempt,
            via,
            holder = %births(hb),
            successor = %births(&seats[0].holder),
            mesh_epoch = %epoch_of(&seats, Seat::MeshPrimary),
            fabric_epoch = %epoch_of(&seats, Seat::FabricPrimary),
            outcome = if taken.iter().any(|x| matches!(x, SeatTaken::Refused { .. })) { "refused-not-newer" } else { "held" },
        )
        .in_scope(|| tracing::info!("the commander installed the seats node.primary.updated answered"));
        (successor, seats)
    }

    /// The gate of a mesh.delete's leave on its owner, the fabric-primary (mesh-delete.md step 1 as the
    /// proposal edits it: "`node.primary.updated` confirms the committed transfer before any member is
    /// deleted"): the row node.fabric-primary.update committed under the departing mesh for
    /// `operation` is installed here before anything is dismantled. A row naming this birth the
    /// successor is installed from the row (this admin's own node.primary.updated); a row naming
    /// another successor is discovered, and that successor's node.primary.updated is obtained first.
    /// No row: the departing mesh did not hold the fabric seat, and nothing moved.
    pub async fn fabric_seat_updated(&self, departing: &str, operation: &str) -> Result<(), String> {
        let row = self.storage.seat_transfer(departing, operation).await.map_err(|e| format!("{}: fabric.storage, the seat-transfer row {operation} under mesh {departing} could not be read: {e}", self.me))?;
        let Some(row) = row else { return Ok(()) };
        let epochs = if row.successor == self.me_birth() {
            let name = self.topology.read().await.births().find(|n| n.node_id == row.holder.node_id).map(|n| n.name.clone()).unwrap_or_else(|| self.me.clone());
            let op = self.op_of(&row.holder, name, &row.build_id, row.attempt, &row.operation);
            {
                let _decisions = self.decisions.clone().lock_owned().await;
                self.membership.seats().take_together(&op, &row.seats);
            }
            (self.project_view)().await;
            row.seats.clone()
        } else {
            self.call_successor(&row, "commander").await.map_err(|e| refusal_reason(&e).1)?
        };
        tracing::info_span!("rdm.node_admin.seat.update.via-fabric-seat-updated", node = %self.me, operation, departing, holder = %births(&row.holder), successor = %births(&row.successor), fabric_epoch = %epoch_of(&epochs, Seat::FabricPrimary))
            .in_scope(|| tracing::info!("the fabric seat's committed row is installed before the departing mesh is dismantled"));
        Ok(())
    }

    /// A request off the wire, decided on the seats and the row it names, whoever calls (node-rpc.md §14).
    pub async fn serve(self: &Arc<Self>, req: Request) -> Reply {
        match req {
            Request::PrimaryUpdate { seats, build_id, attempt, operation } => self.serve_primary_update(seats, build_id, attempt, operation).await,
            Request::UpdatePrimary { fabric, seats, build_id, attempt, operation, .. } => self.serve_update_primary(fabric, seats, build_id, attempt, operation).await,
        }
    }
}

/// Hold every seat the store root's seat-transfer rows commit, as an admin entering the fabric does:
/// the leadership view is folded from the transfer rows (proposal Part C2), and a holder that died
/// between its cut-off and its rename reads here whether its row committed (C1 failure contract). A
/// record older than one already held changes nothing. Returns how many rows it read.
pub async fn hydrate_seats(storage: &dyn FabricStorage, seats: &SeatBook) -> Result<usize, String> {
    Ok(hydrated(storage, seats).await?.len())
}

/// [`hydrate_seats`], returning the rows it read.
async fn hydrated(storage: &dyn FabricStorage, seats: &SeatBook) -> Result<Vec<SeatTransferRow>, String> {
    let rows = storage.seat_transfers().await.map_err(|e| format!("fabric.storage, the seat-transfer rows could not be read: {e}"))?;
    for row in &rows {
        for c in &row.seats {
            seats.take(c.seat.kind(), &c.record());
        }
    }
    Ok(rows)
}

/// The read of the store root's seat-transfer rows an admin makes as it enters the fabric, its
/// restart included ([`hydrate_seats`] into its mesh channel's seat book), once it is on the backbone.
/// A holder lost after its row, before "primary updated", that restarts here reads its own committed
/// row (proposal Part C1 failure contract: "the next writer to read the row publishes 'updated'.
/// Completion needs neither gossip delivery nor the holder's survival"): for each row naming this node
/// (`node_id`, whichever birth it is now) the holder, whose every committed record is the one the
/// book holds after the read, it says "primary updated" once. A row a later update superseded is
/// history and is said by no one again. Returns how many rows it read.
pub async fn enter_seats(storage: &dyn FabricStorage, membership: &Membership, backbone: &Backbone, me: &PathName, node_id: &NodeId) -> Result<usize, String> {
    let book = membership.seats();
    let rows = hydrated(storage, book).await?;
    for row in rows.iter().filter(|r| r.holder.node_id == *node_id && !r.seats.is_empty()) {
        let current = row.seats.iter().all(|c| c.mesh_key().map_or_else(|| book.fabric(), |m| book.mesh(m)) == Some(c.record()));
        if !current {
            continue;
        }
        let op = LifecycleOp { build_id: row.build_id.clone(), attempt: row.attempt, operation: row.operation.clone(), node_id: row.holder.node_id.clone(), incarnation: row.holder.incarnation.clone(), name: me.clone(), event_at_rafka_ms: membership.clock().now_rafka_ms() };
        let span = tracing::info_span!(
            "rdm.node_admin.seat.update.via-entered-own-row",
            node = %me,
            fabric = %row.fabric,
            operation = %row.operation,
            build_id = %row.build_id,
            attempt = row.attempt,
            holder = %births(&row.holder),
            successor = %births(&row.successor),
            mesh_epoch = %epoch_of(&row.seats, Seat::MeshPrimary),
            fabric_epoch = %epoch_of(&row.seats, Seat::FabricPrimary),
        );
        span.in_scope(|| tracing::info!("the holder entering the fabric read its own committed row, current in its book: it says \"primary updated\""));
        backbone.announce_primary_updated(op, row.seats.clone()).instrument(span).await;
    }
    Ok(rows.len())
}

/// How one call ended ([`HandoverDoor::call_once`]).
enum Called {
    /// The receiver's typed reply.
    Reply(Reply),
    /// The request never reached the receiver's dispatch: nothing it would do was done.
    NotDispatched(String),
    /// Committed, with no stronger proof: the receiver may have done what it asks.
    Indeterminate(String),
}

fn refusal_reason(r: &Reply) -> (&'static str, String) {
    match r {
        Reply::RejectedConflict { operation } => ("conflict", operation.clone()),
        Reply::RejectedFabricNotReady { reason } => ("fabric-not-ready", reason.clone()),
        Reply::RejectedNoSuccessor { reason } => ("no-successor", reason.clone()),
        Reply::RejectedNotAuthority { reason } => ("not-authority", reason.clone()),
        Reply::RejectedStaleSeat { seat, expected, current } => ("stale-seat", format!("{}: expected {expected}, current {current}", seat.name())),
        Reply::RejectedEpochOverflow { seat, epoch } => ("epoch-overflow", format!("{}: epoch {epoch} cannot advance", seat.name())),
        Reply::RejectedSourceFailed { source, op, cause } => ("source-failed", format!("{source}: {op}: {cause}")),
        Reply::RejectedRecordAbsent { record, holder } => ("record-absent", format!("{holder} holds no {record}: it was never written there")),
        Reply::Incomplete { reason } => ("incomplete", reason.clone()),
        Reply::NotReady { reason } => ("not-ready", reason.clone()),
        Reply::Unauthorized { reason } => ("unauthorized", reason.clone()),
        Reply::PeerUnresolved { reason } => ("peer-unresolved", reason.clone()),
        Reply::Busy { reason } => ("busy", reason.clone()),
        Reply::Draining { reason } => ("draining", reason.clone()),
        Reply::Malformed { kind } => ("malformed", format!("{kind:?}")),
        Reply::PrimaryUpdated { .. } => ("primary-updated", String::new()),
    }
}

/// The door a running admin fills once it holds a view.
pub type HandoverSlot = Arc<OnceLock<Arc<HandoverDoor>>>;

/// The new seat reads a re-submitted Build it has not heard yet from the incumbent its installed
/// update names, which keeps serving after the row: one `FetchBuildFacts` to that exact node.
#[async_trait::async_trait]
impl crate::build_op::IncumbentFacts for HandoverDoor {
    async fn read(&self, build_id: &crate::build::BuildId) -> crate::build_op::IncumbentRead {
        let Some(incumbent) = self.fabric_seat_incumbent() else { return crate::build_op::IncumbentRead::NoTransfer };
        let name = self.topology.read().await.births().find(|n| n.node_id == incumbent.node_id).map(|n| n.name.to_string()).unwrap_or_else(|| format!("the {} admin", incumbent.mesh));
        let read = crate::build_facts_read::fetch_build_facts(&self.client, &NodeTarget::ExactNode(incumbent.node_id.clone()), build_id).await;
        crate::build_op::IncumbentRead::Read { incumbent: format!("{name} ({}, birth {})", incumbent.node_id, incumbent.incarnation.as_str()), read }
    }
}

/// Serve node.primary.update on this admin. Until `slot` is filled a call is `NotReady`.
pub fn serve(b: ServerBuilder, slot: HandoverSlot) -> ServerBuilder {
    b.serve::<NodePrimaryUpdate, _, _>(OpOwner::Product("rdm".into()), move |_peer: PeerContext, req: Request| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(Reply::NotReady { reason: "this admin holds no view yet".into() });
            };
            Ok(door.serve(req).await)
        }
    })
}
