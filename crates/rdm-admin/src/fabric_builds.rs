//! An admin's Build state over its Build journal (PRD §1.4, §7; mesh-control-plane.md §3–§4.1), and
//! the Build facts' wire message.
//!
//! Every node-admin's Build journal is the fabric's Build journal on the shared store root (build.md
//! §5.3, R-BI6): each fact the fabric-primary commits is one record there (Q118: it writes every
//! Build record, a mesh's drift attempts' among them), and only the fabric-primary reads the records
//! (Q227), at its take and from then on; every other admin holds what reached it (its pulls,
//! FetchBuildFacts 0x1F, an attempt's intent). A successor therefore continues a Build from the store
//! root, never from the executing admin's disk or process.
//!
//! The adapter here gates the fabric-primary's decisions on the primary record this admin holds
//! (ruling R-A2) and refuses a fact with no wire shape before it commits: `FetchBuildFacts` (0x1F)
//! and a build.attempt.run's intent carry the facts as [`BuildMessage`]s.
//!
//! Facts fold deterministically and absorb idempotently: intents and claims are insert-and-fail,
//! receipts deduplicate. Split-primary safety comes from reconciliation over pinned intents and
//! idempotent operation keys, not from the claim alone (mesh-control-plane.md §4.1).

use crate::build::BuildId;
use crate::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter,
    BuildStateError, BuildStepReceipt, ClaimOutcome, LocalBuildLog,
};
use std::sync::Arc;

/// One Build message, one postcard frame (`rdm_transport::wire`): the Build facts `FetchBuildFacts`
/// (0x1F) streams and a build.attempt.run's intent carries, in their wire shapes (`crate::wire`),
/// never the JSON-shaped domain types.
pub struct BuildMessage {
    /// The message's nonce.
    pub nonce: u64,
    /// The Build facts the message carries.
    pub facts: Vec<BuildFact>,
}

impl BuildMessage {
    /// This message as one frame; the frame is not bounded here (see `encode`).
    pub fn to_bytes(&self) -> Result<bytes::Bytes, BuildStateError> {
        let wire = crate::wire::WireMessage::of(self).map_err(|e| BuildStateError::Unencodable(e.to_string()))?;
        rdm_transport::wire::encode(&wire).map(Into::into).map_err(|e| BuildStateError::Unencodable(e.to_string()))
    }

    /// The message a frame carries, or why it is not one.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rdm_transport::wire::WireError> {
        let wire: crate::wire::WireMessage = rdm_transport::wire::decode(bytes)?;
        wire.into_message().map_err(|e| rdm_transport::wire::WireError::new(format!("a Build message this build does not read: {e}")))
    }
}

pub use rdm_transport::chunking::{GOSSIP_FRAME_LIMIT, MAX_MESSAGE_BYTES};

fn encode(facts: Vec<BuildFact>) -> Result<bytes::Bytes, BuildStateError> {
    let bytes = BuildMessage { nonce: rand::random(), facts }.to_bytes()?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(BuildStateError::Io(format!(
            "a Build message of {} bytes exceeds the message limit of {MAX_MESSAGE_BYTES} (frame limit {GOSSIP_FRAME_LIMIT})",
            bytes.len()
        )));
    }
    Ok(bytes.into())
}

/// `facts` packed, in order, into messages that each fit the message limit.
/// A fact that fits no message on its own is left out and named.
pub fn encode_chunks(facts: Vec<BuildFact>) -> (Vec<bytes::Bytes>, Vec<BuildStateError>) {
    // Each encoding draws a fresh nonce, whose length varies: a batch is sent as the bytes that
    // were measured to fit, never encoded a second time.
    let (runs, refused) = rdm_transport::chunking::pack_in_order(facts, |batch| encode(batch.to_vec()));
    (runs.into_iter().map(|(_, b)| b).collect(), refused)
}

/// A node-admin's Build state: the fabric's Build journal, read and written on the shared store root,
/// behind this admin's primary records.
pub struct FabricBuildStateAdapter {
    local: Arc<dyn LocalBuildLog>,
    /// The decision gate every fabric-primary mutation passes (ruling R-A2), a mesh's drift attempt's
    /// open and claim among them: the fabric-primary writes every Build record (Q118), so a
    /// mesh-primary that is not the fabric-primary reports its drift up and writes none.
    gate: DecisionGate,
    location_view: rdm_transport::topology::LocationView,
}

/// What the fabric-primary record this admin holds says of this birth's Build log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatRecord {
    /// The record names this birth, names none, or names a birth proven gone: it ends nothing.
    Open {
        /// The path.name of this birth when the record names it (from the digests held); `None`
        /// when the record names none or a birth proven gone.
        holder: Option<String>,
    },
    /// The record names another live birth: this birth's authority ended at that record's revision.
    Moved {
        /// The record, in words.
        by: String,
        /// The path.name of the birth the record names, when a digest of it is held.
        to: Option<String>,
    },
}

/// Where a Build log reads its target: the primary record this admin holds, and a receiver that wakes
/// each time a record is held.
#[derive(Clone)]
pub struct SeatSource {
    /// The record's answer for this birth, read now.
    pub read: Arc<dyn Fn() -> SeatRecord + Send + Sync>,
    /// Ticks whenever a seat record is held (`PrimaryView::subscribe`); none for a log with no seat.
    pub changed: Option<tokio::sync::watch::Receiver<u64>>,
}

impl SeatSource {
    /// A Build log no seat record governs (a fixture with no topology view).
    pub fn unrecorded() -> Self {
        Self { read: Arc::new(|| SeatRecord::Open { holder: None }), changed: None }
    }
}

/// The gate of the fabric-primary's mutations: one lock, held from the decision through its
/// durable commit and its broadcast, and the fabric-primary record this admin holds read under it.
/// The successor's install of node.primary.update on this admin holds the same lock across the
/// record's move, so a decision is either wholly committed before the record moves or refused by it.
/// The record is the target every decision is judged against (lifecycles/node-primary-update.md
/// step 4): nothing else is written for the log.
pub struct DecisionGate {
    node: String,
    decisions: Arc<tokio::sync::Mutex<()>>,
    /// The primary record this birth's target is read from.
    seat: SeatSource,
}

impl DecisionGate {
    /// A gate for `node`'s Build log, its seat read from `seat`.
    pub fn new(node: String, seat: SeatSource) -> Self {
        Self { node, decisions: Arc::default(), seat }
    }

    /// Resolves once the seat record ends this birth's seat (at once when it already has;
    /// never for a log no seat record governs).
    pub async fn target_set(&self) {
        let Some(mut changed) = self.seat.changed.clone() else { return std::future::pending().await };
        loop {
            if matches!((self.seat.read)(), SeatRecord::Moved { .. }) {
                return;
            }
            if changed.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// Enter the gate for one mutation: refused by name once the seat record names another live
    /// birth.
    pub async fn enter(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, BuildStateError> {
        self.enter_naming().await.map(|(held, _)| held)
    }

    /// [`Self::enter`], with the holder an open record names (`SeatRecord::Open::holder`), read
    /// under the same lock.
    pub async fn enter_naming(&self) -> Result<(tokio::sync::MutexGuard<'_, ()>, Option<String>), BuildStateError> {
        let held = self.decisions.lock().await;
        match (self.seat.read)() {
            SeatRecord::Open { holder } => Ok((held, holder)),
            SeatRecord::Moved { by, to } => Err(BuildStateError::SeatYielded { node: self.node.clone(), by, to }),
        }
    }

    /// The lock a seat commit on this admin holds across the record's move.
    pub fn decisions(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.decisions.clone()
    }
}

impl FabricBuildStateAdapter {
    /// The Build state of the admin named `node` over its Build journal `local`, its fabric-primary
    /// decisions gated by `seat`. `location_view` names this admin's path.name as its view holds it.
    pub fn new(local: Arc<dyn LocalBuildLog>, location_view: &rdm_transport::topology::LocationView, node: String, seat: SeatSource) -> Self {
        Self { local, gate: DecisionGate::new(node, seat), location_view: location_view.clone() }
    }

    /// Whether this admin's Build log still decides anything: refused by name once the committed
    /// seat record names another live birth, else the holder the open record names. The
    /// fabric-primary's drive passes this before every dispatch; it holds the gate only for the
    /// check, because an attempt lasts as long as its steps and the target is kept.
    pub async fn target_check(&self) -> Result<Option<String>, BuildStateError> {
        self.gate.enter_naming().await.map(|(_, holder)| holder)
    }

    /// Resolves once this admin's Build log is targeted: the drive's wait on an executor's run ends
    /// there.
    pub async fn target_set(&self) {
        self.gate.target_set().await
    }

    /// The lock a seat commit on this admin holds across the record's move: a decision in flight
    /// commits wholly before the record moves, and every decision after it is refused by the record.
    pub fn decisions(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.gate.decisions()
    }
}

/// A fact is encoded before it commits: one with no wire shape is refused by name and lands in no
/// log, since `FetchBuildFacts` (0x1F) and a build.attempt.run's intent could not carry it.
fn wire_shaped(fact: &BuildFact) -> Result<(), BuildStateError> {
    encode(vec![fact.clone()]).map(|_| ())
}

#[async_trait::async_trait]
impl BuildStateAdapter for FabricBuildStateAdapter {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let _gate = self.gate.enter().await?;
        wire_shaped(&BuildFact::Accepted(accepted.clone()))?;
        self.local.publish_accepted(accepted).await
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let _gate = self.gate.enter().await?;
        wire_shaped(&BuildFact::Opened(opened.clone()))?;
        self.local.open_attempt(opened).await
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.local.read_build(build_id).await
    }

    async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, BuildStateError> {
        self.local.current_build().await
    }

    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.local.newest_accepted_at_ms().await
    }

    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.local.accepted_newest_first().await
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.local.list_active().await
    }


    /// The fabric-primary's decision: called by `build_claim::ClaimDoor` on the seat's own log.
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let _gate = self.gate.enter().await?;
        wire_shaped(&BuildFact::Claim(claim.clone()))?;
        self.local.claim_attempt(claim).await
    }

    /// [`Self::claim_attempt`] with the mesh's in-flight row taken in the local log's own turn.
    async fn claim_in_flight(&self, claim: &BuildAttemptClaim, claims: &dyn crate::storage::MeshStorage, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<crate::build_state::InFlightClaim, BuildStateError> {
        let _gate = self.gate.enter().await?;
        wire_shaped(&BuildFact::Claim(claim.clone()))?;
        self.local.claim_in_flight(claim, claims, mesh, row).await
    }

    async fn in_flight_live(&self, held: &crate::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        self.local.in_flight_live(held).await
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.local.adopt_claim(claim).await
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        // Encoded before it commits: a receipt with no wire shape is refused by name and lands in no
        // log. Once the write commits, the receipt is the step's durable commit.
        wire_shaped(&BuildFact::Step(receipt.clone()))?;
        self.local.append_step_receipt(receipt).await
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        wire_shaped(&BuildFact::Attempt(receipt.clone()))?;
        self.local.append_attempt_receipt(receipt).await
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.local.facts().await
    }
    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.local.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, BuildStateError> {
        self.local.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, BuildStateError> {
        self.local.exits_owed(build_id, mesh).await
    }

    fn held_name(&self) -> Option<crate::model::PathName> {
        self.location_view.held_path_name()
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let _gate = self.gate.enter().await?;
        wire_shaped(&BuildFact::Forget { build_id: build_id.clone() })?;
        self.local.forget(build_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::BuildAttemptClaim;

    /// CONTRACT (node-identity-and-state.md §6.9 Age and Wire: "the age lives in the receipt", "No frame
    /// gains a field"): a standalone stop's `Complete` receipt commits the stop's lifecycle op (the
    /// exact birth and its rafka-time), and it travels in a Build message (0x1F, a build.attempt.run's intent) in the shape that already
    /// carries a lifecycle op: it is encoded, decoded to the same receipt, and refused for no other
    /// operation's `Complete`, which still commits no output.
    #[test]
    fn a_stop_receipt_carries_its_birth_and_time_on_the_build_topic() {
        let op = rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 2,
            operation: "node.stop:mesh1.rpc.1".into(),
            node_id: crate::model::NodeId::mint(),
            incarnation: crate::model::IncarnationId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            event_at_rafka_ms: 4_242,
            traceparent: None,
            tracestate: None,
        };
        let receipt = |operation: &str| BuildStepReceipt {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt: 2,
            operation: operation.into(),
            step: "Complete".into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: Some(serde_json::to_value(&op).unwrap()),
            executor: Some("mesh1.admin.1".into()),
        };
        let stop = BuildFact::Step(receipt("node.stop:mesh1.rpc.1"));
        let (messages, refused) = encode_chunks(vec![stop.clone()]);
        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(BuildMessage::from_bytes(&messages[0]).unwrap().facts, vec![stop]);
        let (_, refused) = encode_chunks(vec![BuildFact::Step(receipt("node.create:mesh1.rpc.1"))]);
        assert_eq!(refused.len(), 1, "another operation's Complete commits no output");
    }

    /// Packing measures each batch once and sends those bytes: a fresh
    /// nonce of another length can never push a sent message over the limit.
    #[test]
    fn every_packed_message_fits_whatever_nonce_it_drew() {
        for n in 0..200u32 {
            let facts: Vec<BuildFact> = (0..60u32)
                .map(|i| {
                    BuildFact::Claim(BuildAttemptClaim {
                        claimant: None,
                        mesh: None,
                        build_id: BuildId::mint(),
                        attempt: i,
                        executor: format!("mesh{}.admin.{}", n, "x".repeat(((i * 37 + n) % 90) as usize)),
                    })
                })
                .collect();
            let (messages, refused) = encode_chunks(facts);
            assert!(refused.is_empty());
            assert!(messages.iter().all(|m| m.len() <= MAX_MESSAGE_BYTES), "every message fits");
            let sent: usize = messages.iter().map(|m| BuildMessage::from_bytes(m).unwrap().facts.len()).sum();
            assert_eq!(sent, 60, "no fact lost");
        }
    }

    /// CONTRACT (build.md §1: "Rafka's replace steps (one Build of node primitives ...)";
    /// lifecycles/node-replace.md): the attempt the steps open travels in a Build message with its steps
    /// and the births it pinned, so every admin folds the same steps and plans the same remaining
    /// steps from it.
    #[test]
    fn an_attempt_travels_the_build_topic_with_its_steps_and_pinned_births() {
        use rdm_core::build::Step;
        let node: crate::model::PathName = "mesh1.rpc.1".parse().unwrap();
        let pin = crate::build::PinnedBirth { node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
        let opened = crate::build_state::AttemptOpened {
            mesh: None,
            build_id: BuildId::parse("3qcfk1kq47m9").unwrap(),
            attempt: 2,
            reason: crate::build_state::AttemptReason::Steps,
            action: Some(crate::accepted::AttemptAction::Steps {
                steps: vec![Step::DeleteConnections { node: node.clone() }, Step::LeaveMesh { node: node.clone() }, Step::Update { node: node.clone(), name: node.renamed() }, Step::Stop { node: node.renamed() }, Step::Create { node: node.clone(), executable: None }, Step::Delete { node: node.renamed() }],
                pinned: vec![(node.clone(), pin)],
            }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 7,
            context: None,
        };
        let bytes = match encode(vec![BuildFact::Opened(opened.clone())]) {
            Ok(b) => b,
            Err(e) => panic!("the Build's Opened fact must have a wire shape: {e}"),
        };
        assert_eq!(BuildMessage::from_bytes(&bytes).unwrap().facts, vec![BuildFact::Opened(opened)], "a peer folds the same steps");
    }

    /// CONTRACT (proposal node-primary-update Part C1 step 5: "This is the commit. From this write the
    /// holder is no longer primary"; Build dispatch from the old seat is refused `SeatYielded`, and nothing
    /// else is written): the Build log reads its authority from the committed
    /// fabric seat record. Once the record names another live birth, every decision is refused by
    /// name, naming that birth's path.name from the digest view even when the projected view does not
    /// hold it yet, and a wait on the target resolves; no target write is made. A record naming a birth
    /// proven gone (a vacancy the election fills before this birth's own record lands) ends nothing.
    /// What must NOT happen: a moved record that still lets the old seat decide, or a refusal naming
    /// no admin while a digest of the holder is held.
    #[tokio::test]
    async fn a_fabric_primary_record_naming_another_birth_refuses_this_admins_decisions_by_name() {
        use futures_util::FutureExt as _;
        use rdm_core::{IncarnationId, NodeId, Seat, Primary};
        let seats = rdm_transport::topology::PrimaryView::default();
        let me = (NodeId::parse("300000000000").unwrap(), IncarnationId::parse(&rdm_core::fixture_crockford60("p1")).unwrap());
        let next = Primary { mesh: "mesh2".into(), node_id: NodeId::parse("100000000000").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("n1")).unwrap(), revision: 2 };
        let lost = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::<IncarnationId>::new()));
        let source = {
            let changed = Some(seats.subscribe());
            let (seats, me, lost) = (seats.clone(), me.clone(), lost.clone());
            // The digest view holds mesh2.admin.1; nothing here consults a projected view.
            let read = std::sync::Arc::new(move || crate::seat_watch::fabric_seat_record(seats.fabric().as_ref(), &me, &lost.lock().unwrap(), |id| (id.as_str() == "100000000000").then(|| "mesh2.admin.1".to_string())));
            SeatSource { read, changed }
        };
        let gate = DecisionGate::new("mesh2.admin.2".into(), source);
        seats.take(Seat::FabricPrimary, &Primary { mesh: "mesh2".into(), node_id: me.0.clone(), incarnation: me.1.clone(), revision: 1 });
        assert!(gate.enter().await.is_ok(), "the record names this birth: it decides");
        assert!(gate.target_set().now_or_never().is_none(), "no target while the record names this birth");
        seats.take(Seat::FabricPrimary, &next);
        lost.lock().unwrap().insert(next.incarnation.clone());
        assert!(gate.enter().await.is_ok(), "a record naming a birth proven gone ends nothing");
        lost.lock().unwrap().clear();
        match gate.enter().await {
            Err(BuildStateError::SeatYielded { to, by, .. }) => {
                assert_eq!(to.as_deref(), Some("mesh2.admin.1"), "the refusal names the holder the record names");
                assert!(by.contains("100000000000") && by.contains("revision 2"), "the reason names the record: {by}");
            }
            other => panic!("the moved record must refuse the old seat's decision by name, got {:?}", other.map(|_| ())),
        }
        assert!(gate.target_set().now_or_never().is_some(), "a wait on the target resolves once the record moved");
    }

    /// CONTRACT (R-A2 item 2): a seat commit on this admin waits for the decision in flight (it
    /// commits wholly before the record moves), and every decision after the move is refused by
    /// name.
    #[tokio::test]
    async fn a_fabric_primary_change_waits_for_the_decision_in_flight_and_refuses_every_later_one() {
        use rdm_core::{IncarnationId, NodeId, Seat, Primary};
        let seats = rdm_transport::topology::PrimaryView::default();
        let me = (NodeId::parse("300000000000").unwrap(), IncarnationId::parse(&rdm_core::fixture_crockford60("p1")).unwrap());
        let source = {
            let changed = Some(seats.subscribe());
            let (seats, me) = (seats.clone(), me.clone());
            SeatSource { read: std::sync::Arc::new(move || crate::seat_watch::fabric_seat_record(seats.fabric().as_ref(), &me, &Default::default(), |_| None)), changed }
        };
        seats.take(Seat::FabricPrimary, &Primary { mesh: "mesh1".into(), node_id: me.0.clone(), incarnation: me.1.clone(), revision: 1 });
        let gate_ref = std::sync::Arc::new(DecisionGate::new("mesh1.admin.1".into(), source));
        let committed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = gate_ref.enter().await.unwrap();
        let (decisions, c, primary_view) = (gate_ref.decisions(), committed.clone(), seats.clone());
        let moving = tokio::spawn(async move {
            let _held = decisions.lock_owned().await;
            primary_view.take(Seat::FabricPrimary, &Primary { mesh: "mesh1".into(), node_id: NodeId::parse("400000000000").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("n1")).unwrap(), revision: 2 });
            c.load(std::sync::atomic::Ordering::SeqCst)
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!moving.is_finished(), "the seat commit waits for the decision holding the gate");
        committed.store(true, std::sync::atomic::Ordering::SeqCst);
        drop(gate);
        assert!(moving.await.unwrap(), "the record moved after the decision committed");
        let later = gate_ref.enter().await.map(|_| ());
        match later {
            Err(BuildStateError::SeatYielded { node, by, to }) => {
                assert_eq!(node, "mesh1.admin.1");
                assert!(by.contains("400000000000") && by.contains("no digest of that birth is held"), "the reason names the record and why no path.name is named: {by}");
                assert_eq!(to, None);
            }
            other => panic!("a decision after the record moved must be refused by name, got {other:?}"),
        }
    }
}
