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

use crate::fabric_storage::{FabricStorage, HandoverRow, HandoverStage, SeatRow};
use crate::model::{EndpointId, IncarnationId, NodeId, NodeKind, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::{Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, DigestBook, Membership, SeatTaken};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::handover::{FabricPrimaryHandover, FabricPrimaryHandoverReply as Reply, FabricPrimaryHandoverRequest as Request, HandoverBirth};
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
    /// This admin's own contact addresses, once its digest is built.
    pub contacts: OnceLock<OwnContacts>,
    /// The handovers this node took part in.
    pub handovers: HandoverBook,
}

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

impl HandoverDoor {
    fn incumbency(&self) -> crate::election::Incumbency {
        crate::admin::incumbency_of(&self.book, &self.records)
    }

    fn me_birth(&self) -> HandoverBirth {
        HandoverBirth { mesh: self.me.mesh.clone(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }
    }

    /// The node of the view an authenticated peer is.
    async fn sender_of(&self, peer: &EndpointId) -> Option<crate::model::Node> {
        self.topology.read().await.births().find(|n| n.endpoint_id.as_ref() == Some(peer)).cloned()
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

    /// Hold the handovers `fabric.storage` records, as this admin started with them. Returns how
    /// many it holds. A row this build does not recognise is refused by name.
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
                let incumbent = view.fabric_primary().filter(|n| n.name == self.me).and_then(birth_of).ok_or_else(|| HandoverError::Refused(format!("{} does not hold the fabric seat in its view: it cannot hand it over", self.me)))?;
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

    async fn serve_take(self: &Arc<Self>, peer: EndpointId, payload: Payload) -> Reply {
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
        let Some(sender) = self.sender_of(&peer).await else {
            return refuse("unauthorized", format!("the calling endpoint {} is not a node of this admin's view", peer.0), Reply::Unauthorized { reason: format!("{}: the calling endpoint {} is not a node of this admin's view", self.me, peer.0) });
        };
        if sender.kind != NodeKind::NodeAdmin {
            return refuse("unauthorized", format!("{} is a {:?}", sender.name, sender.kind), Reply::Unauthorized { reason: format!("{}: {} is a {:?}, and only a node-admin hands the fabric over", self.me, sender.name, sender.kind) });
        }
        if birth_of(&sender).as_ref() != Some(&payload.incumbent) {
            return refuse("wrong-birth", format!("the command names incumbent {}:{}:{}, the caller is {}", payload.incumbent.mesh, payload.incumbent.node_id, payload.incumbent.incarnation.0, sender.name), Reply::RejectedWrongBirth { role: "incumbent".into() });
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
        let Some(committed) = payload.expected_epoch.checked_add(1) else {
            return refuse("not-ready", "the seat epoch cannot advance".into(), Reply::NotReady { reason: format!("{}: the fabric seat epoch {} cannot advance", self.me, payload.expected_epoch) });
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
            if let Some(c) = self.contacts.get() {
                let holder = SeatHolder { mesh: payload.successor.mesh.clone(), node_id: payload.successor.node_id.clone(), incarnation: payload.successor.incarnation.clone(), epoch: committed };
                self.backbone.announce_new_fabric_primary(holder, c.endpoint_id.clone(), c.transport_addr, c.admin_api_base.clone()).await;
            }
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

    async fn serve_taken(&self, peer: EndpointId, payload: Payload, committed: u64) -> Reply {
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
        let Some(sender) = self.sender_of(&peer).await else {
            return refuse("unauthorized", format!("the calling endpoint {} is not a node of this admin's view", peer.0), Reply::Unauthorized { reason: format!("{}: the calling endpoint {} is not a node of this admin's view", self.me, peer.0) });
        };
        if birth_of(&sender).as_ref() != Some(&payload.successor) {
            return refuse("wrong-birth", format!("the completion names successor {}:{}:{}, the caller is {}", payload.successor.mesh, payload.successor.node_id, payload.successor.incarnation.0, sender.name), Reply::RejectedWrongBirth { role: "successor".into() });
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
        let Some(next) = payload.expected_epoch.checked_add(1) else {
            return refuse("not-ready", "the seat epoch cannot advance".into(), Reply::NotReady { reason: format!("{}: the fabric seat epoch {} cannot advance", self.me, payload.expected_epoch) });
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
        // The committed transfer fences this admin's fabric writes: its seat record moves.
        if let SeatTaken::Refused { held } = self.membership.seats().take(Seat::FabricPrimary, &holder) {
            return refuse("stale-epoch", format!("a later seat record is held: {held}"), Reply::RejectedStaleEpoch { expected: committed, current: held.epoch });
        }
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

    /// A request off the wire.
    pub async fn serve(self: &Arc<Self>, peer: EndpointId, req: Request) -> Reply {
        match req {
            Request::TakeFabricPrimary { fabric, incumbent, successor, expected_epoch, operation } => self.serve_take(peer, Payload { fabric, incumbent, successor, expected_epoch, operation }).await,
            Request::FabricPrimaryTaken { fabric, incumbent, successor, expected_epoch, committed_epoch, operation } => self.serve_taken(peer, Payload { fabric, incumbent, successor, expected_epoch, operation }, committed_epoch).await,
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
        Reply::Applied | Reply::AlreadyApplied => ("applied", String::new()),
    }
}

/// The door a running admin fills once it holds a view.
pub type HandoverSlot = Arc<OnceLock<Arc<HandoverDoor>>>;

/// Serve the fabric-primary handover on this admin. Until `slot` is filled a call is `NotReady`.
pub fn serve(b: ServerBuilder, slot: HandoverSlot) -> ServerBuilder {
    b.serve::<FabricPrimaryHandover, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: Request| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(Reply::NotReady { reason: "this admin holds no view yet".into() });
            };
            Ok(door.serve(EndpointId(peer.endpoint_id.to_string()), req).await)
        }
    })
}
