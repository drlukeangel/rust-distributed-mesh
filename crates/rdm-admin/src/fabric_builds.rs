//! An admin's Build state over its Build journal (PRD §1.4, §7; mesh-control-plane.md §3–§4.1), and
//! the Build facts' wire message.
//!
//! Every node-admin's Build journal is the fabric's Build journal on the shared store root (build.md
//! §5.3, R-BI6): each fact the fabric-primary commits is one record there (Q118: it writes every
//! Build record, a mesh's drift attempts' among them). The fabric-primary takes the records at its
//! take and reads them from then on; an executor, a starting admin and an admin taking a primary role
//! read the named Build's committed records fresh (Q292, `committed_facts`). A successor therefore
//! continues a Build from the store root, never from the executing admin's disk or process.
//!
//! The adapter here gates the fabric-primary's decisions on the primary record this admin holds
//! (ruling R-A2) and refuses a fact with no typed shape before it commits ([`BuildMessage`]: every
//! fact and step output in its positional shape, the check a receipt passes before it is a record).
//!
//! Facts fold deterministically and absorb idempotently: intents and claims are insert-and-fail,
//! receipts deduplicate. Split-primary safety comes from reconciliation over pinned intents and
//! idempotent operation keys, not from the claim alone (mesh-control-plane.md §4.1).

use crate::build::BuildId;
use crate::build_state::{BuildAccepted, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, LocalBuildLog};
use std::sync::Arc;

/// One Build message, one postcard frame (`rdm_transport::wire`): Build facts in their typed shapes
/// (`crate::wire`), never the JSON-shaped domain types. No call carries one (Q292): it is the shape
/// check a fact passes before it commits ([`wire_shaped`]).
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

/// `facts` cut as Q284 cut them (no call carries them since Q292): each whole acceptance replaced by its sections of `meshes` (every
/// mesh it names when `None`), in place; every other fact, and a section already cut, as it is.
pub fn sectioned(facts: Vec<BuildFact>, meshes: Option<&std::collections::BTreeSet<String>>) -> Vec<BuildFact> {
    facts
        .into_iter()
        .flat_map(|f| match f {
            BuildFact::Accepted(a) => a.topology.sections(meshes).into_iter().map(|topology| BuildFact::Accepted(BuildAccepted { topology, ..a.clone() })).collect(),
            other => vec![other],
        })
        .collect()
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
pub enum PrimaryRecord {
    /// The record names this birth, names none, or names a birth proven gone: it ends nothing.
    Open {
        /// The node ID of this birth when the record names it; `None`
        /// when the record names none or a birth proven gone.
        primary: Option<rdm_core::NodeId>,
    },
    /// The record names another live birth: this birth's authority ended at that record's revision.
    Moved {
        /// The record, in words.
        by: String,
        /// The exact birth named directly by the primary record.
        to: Option<rdm_core::framing::Birth>,
    },
}

/// Where a Build log reads its target: the primary record this admin holds, and a receiver that wakes
/// each time a record is held.
#[derive(Clone)]
pub struct PrimarySource {
    /// The record's answer for this birth, read now.
    pub read: Arc<dyn Fn() -> PrimaryRecord + Send + Sync>,
    /// Ticks whenever a primary record is held (`PrimaryView::subscribe`); none for a log with no role.
    pub changed: Option<tokio::sync::watch::Receiver<u64>>,
}

impl PrimarySource {
    /// A Build log no primary record governs (a fixture with no topology view).
    pub fn unrecorded() -> Self {
        Self { read: Arc::new(|| PrimaryRecord::Open { primary: None }), changed: None }
    }
}

/// The gate of the fabric-primary's mutations: one lock, held from the decision through its
/// durable commit and its broadcast, and the fabric-primary record this admin holds read under it.
/// The successor's install of node.primary.update on this admin holds the same lock across the
/// record's move, so a decision is either wholly committed before the record moves or refused by it.
/// The record is the target every decision is judged against (lifecycles/node-primary-update.md
/// step 4): nothing else is written for the log.
pub struct DecisionGate {
    node: rdm_core::NodeId,
    decisions: Arc<tokio::sync::Mutex<()>>,
    /// The primary record this birth's target is read from.
    role: PrimarySource,
}

impl DecisionGate {
    /// A gate for `node`'s Build log, its role read from `role`.
    pub fn new(node: rdm_core::NodeId, role: PrimarySource) -> Self {
        Self { node, decisions: Arc::default(), role }
    }

    /// Resolves once the primary record ends this birth's role (at once when it already has;
    /// never for a log no primary record governs).
    pub async fn target_set(&self) {
        let Some(mut changed) = self.role.changed.clone() else { return std::future::pending().await };
        loop {
            if matches!((self.role.read)(), PrimaryRecord::Moved { .. }) {
                return;
            }
            if changed.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// Enter the gate for one mutation: refused by name once the primary record names another live
    /// birth.
    pub async fn enter(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, BuildStateError> {
        self.enter_naming().await.map(|(held, _)| held)
    }

    /// [`Self::enter`], with the primary an open record names (`PrimaryRecord::Open::primary`), read
    /// under the same lock.
    pub async fn enter_naming(&self) -> Result<(tokio::sync::MutexGuard<'_, ()>, Option<rdm_core::NodeId>), BuildStateError> {
        let held = self.decisions.lock().await;
        match (self.role.read)() {
            PrimaryRecord::Open { primary } => Ok((held, primary)),
            PrimaryRecord::Moved { by, to } => Err(BuildStateError::RoleYielded { node: self.node.clone(), by, to }),
        }
    }

    /// The lock a role commit on this admin holds across the record's move.
    pub fn decisions(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.decisions.clone()
    }
}

impl FabricBuildStateAdapter {
    /// The Build state of the admin named `node` over its Build journal `local`, its fabric-primary
    /// decisions gated by `role`. `location_view` names this admin's path and name as its view holds it.
    pub fn new(local: Arc<dyn LocalBuildLog>, location_view: &rdm_transport::topology::LocationView, node: rdm_core::NodeId, role: PrimarySource) -> Self {
        Self { local, gate: DecisionGate::new(node, role), location_view: location_view.clone() }
    }

    /// Whether this admin's Build log still decides anything: refused by name once the committed
    /// primary record names another live birth, else the primary the open record names. The
    /// fabric-primary's drive passes this before every dispatch; it holds the gate only for the
    /// check, because an attempt lasts as long as its steps and the target is kept.
    pub async fn target_check(&self) -> Result<Option<rdm_core::NodeId>, BuildStateError> {
        self.gate.enter_naming().await.map(|(_, primary)| primary)
    }

    /// Resolves once this admin's Build log is targeted: the drive's wait on an executor's run ends
    /// there.
    pub async fn target_set(&self) {
        self.gate.target_set().await
    }

    /// The lock a role commit on this admin holds across the record's move: a decision in flight
    /// commits wholly before the record moves, and every decision after it is refused by the record.
    pub fn decisions(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.gate.decisions()
    }
}

/// A fact is encoded before it commits: one with no typed shape (a step output that is not the type
/// its step produces) is refused by name and lands in no log. Its size is not bounded here: the
/// record is whole on the store, which has no frame.
fn wire_shaped(fact: &BuildFact) -> Result<(), BuildStateError> {
    BuildMessage { nonce: 0, facts: vec![fact.clone()] }.to_bytes().map(|_| ())
}

#[async_trait::async_trait]
impl BuildStateAdapter for FabricBuildStateAdapter {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let _gate = self.gate.enter().await?;
        wire_shaped(&BuildFact::Accepted(accepted.clone()))?;
        self.local.publish_accepted(accepted).await
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


    fn held_name(&self) -> Option<crate::model::PathName> {
        self.location_view.held_path_name()
    }

    fn events(&self) -> Arc<dyn crate::events_storage::EventsStorage> {
        self.local.events()
    }

    async fn write_step(&self, step: &crate::build_state::StepWrite) -> Result<(), BuildStateError> {
        self.local.write_step(step).await
    }

    async fn note_pending(&self, build_id: &BuildId, operation: &rdm_core::OperationId, step: &str, reason: &str) {
        self.local.note_pending(build_id, operation, step, reason).await
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

    /// L4's shape (load-testing.md "Profiles"): ten meshes of three node-admins and seventeen rpc
    /// nodes, a pin on `mesh2.node.1`.
    fn l4_acceptance() -> BuildAccepted {
        
        let meshes = (1..=10)
            .map(|i| {
                let name = format!("mesh{i}");
                let m = crate::accepted::MeshTopology::with_id(&crate::build::MeshDesired::of(name.clone(), [("node-admin", 3), ("node", 17)]), crate::model::MeshId::mint());
                (name, m)
            })
            .collect();
        let pin = crate::build::PinnedBirth { mesh: "mesh2".into(), r#type: "node".into(), node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
        let topology = crate::accepted::FabricTopology {
            created: Vec::new(), section_of: None,
            pinned: vec![(rdm_core::PathName::new("mesh2.node", "1").unwrap(), pin)],
            topology_storage_provider: Some("file".into()),
            fabric: "fabric1".into(),
            meshes,
        };
        BuildAccepted { build_id: BuildId::mint(), topology, change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 7 }
    }

    /// CONTRACT (INDEX Q284: "The fabric-primary records the accepted Build whole in the journal, and
    /// the gossip-sized check at commit goes"; "build.attempt.run carries the BuildSections"): an
    /// acceptance larger than one message (L3's ten meshes) has a wire shape and commits; it travels as
    /// one section per mesh, each fitting a message, each naming every mesh of the Build, each mesh's
    /// pinned births in its own section only.
    #[test]
    fn an_acceptance_larger_than_a_message_commits_and_travels_as_its_sections() {
        let whole = l4_acceptance();
        let fact = BuildFact::Accepted(whole.clone());
        let bytes = BuildMessage { nonce: 0, facts: vec![fact.clone()] }.to_bytes().unwrap().len();
        assert!(bytes > MAX_MESSAGE_BYTES, "the acceptance does not fit one message whole: {bytes} bytes");
        wire_shaped(&fact).expect("the whole acceptance commits: the store has no frame");
        let sections = sectioned(vec![fact], None);
        assert_eq!(sections.len(), 10, "one section per mesh");
        let (messages, refused) = encode_chunks(sections.clone());
        assert!(refused.is_empty(), "every section fits a message: {refused:?}");
        assert!(messages.iter().all(|m| m.len() <= MAX_MESSAGE_BYTES));
        let heard: Vec<BuildFact> = messages.iter().flat_map(|m| BuildMessage::from_bytes(m).unwrap().facts).collect();
        assert_eq!(heard.len(), 10, "no section lost");
        let names: std::collections::BTreeSet<String> = whole.topology.meshes.keys().cloned().collect();
        for f in &heard {
            let BuildFact::Accepted(a) = f else { panic!("a section is an acceptance: {f:?}") };
            assert_eq!((a.build_id.clone(), a.time, a.topology.fabric.as_str(), a.topology.topology_storage_provider.as_deref()), (whole.build_id.clone(), 7, "fabric1", Some("file")));
            assert_eq!(a.topology.meshes.len(), 1, "one mesh's section");
            assert_eq!(a.topology.mesh_names(), names, "every section names every mesh of the Build");
            let (mesh, m) = a.topology.meshes.iter().next().unwrap();
            assert_eq!(m.nodes, whole.topology.meshes[mesh].nodes, "the mesh's nodes, whole");
            assert_eq!(a.topology.pinned.len(), usize::from(mesh == "mesh2"), "a pin travels in its own mesh's section only");
        }
    }

    /// CONTRACT (INDEX Q284 "the BuildSections of the meshes its operations touch"; the lead's
    /// mechanics, Q284 lines 2-3): a run's sections are the meshes it acts in; a Build that names no
    /// mesh (a fabric delete) still travels, as one section with none, and says it deletes the fabric.
    #[test]
    fn a_run_carries_its_meshes_sections_and_a_build_with_no_mesh_carries_one_with_none() {
        let whole = l4_acceptance();
        let touched = std::collections::BTreeSet::from(["mesh3".to_string()]);
        let cut = sectioned(vec![BuildFact::Accepted(whole.clone())], Some(&touched));
        let [BuildFact::Accepted(a)] = cut.as_slice() else { panic!("one section: {cut:?}") };
        assert_eq!(a.topology.meshes.keys().collect::<Vec<_>>(), vec!["mesh3"]);
        assert!(a.topology.names_mesh("mesh7") && !a.topology.meshes.contains_key("mesh7"), "mesh7 is the Build's, its section not here");
        let delete = BuildAccepted {
            topology: crate::accepted::FabricTopology { meshes: Default::default(), pinned: Vec::new(), ..whole.topology.clone() },
            change: rdm_core::build::BuildChange::DeleteFabric,
            ..whole
        };
        for wanted in [None, Some(&touched)] {
            let cut = sectioned(vec![BuildFact::Accepted(delete.clone())], wanted);
            let (messages, refused) = encode_chunks(cut);
            assert!(refused.is_empty());
            let heard = BuildMessage::from_bytes(&messages[0]).unwrap().facts;
            let [BuildFact::Accepted(a)] = heard.as_slice() else { panic!("the Build's one section: {heard:?}") };
            assert!(a.topology.meshes.is_empty() && a.topology.mesh_names().is_empty());
            assert_eq!(a.change, rdm_core::build::BuildChange::DeleteFabric, "it says it deletes the fabric");
        }
    }

    /// Packing measures each batch once and sends those bytes: a fresh
    /// nonce of another length can never push a sent message over the limit.
    #[test]
    fn every_packed_message_fits_whatever_nonce_it_drew() {
        for n in 0..200u32 {
            let facts: Vec<BuildFact> = (0..60u32)
                .map(|i| {
                    let _ = (n, i);
                    BuildFact::Forget { build_id: BuildId::mint() }
                })
                .collect();
            let (messages, refused) = encode_chunks(facts);
            assert!(refused.is_empty());
            assert!(messages.iter().all(|m| m.len() <= MAX_MESSAGE_BYTES), "every message fits");
            let sent: usize = messages.iter().map(|m| BuildMessage::from_bytes(m).unwrap().facts.len()).sum();
            assert_eq!(sent, 60, "no fact lost");
        }
    }

    /// CONTRACT (proposal node-primary-update Part C1 step 5: "This is the commit. From this write the
    /// primary is no longer primary"; Build dispatch from the old role is refused `RoleYielded`, and nothing
    /// else is written): the Build log reads its authority from the committed
    /// fabric primary record. Once the record names another live birth, every decision is refused by
    /// name, naming that birth's path and name from the digest view even when the projected view does not
    /// hold it yet, and a wait on the target resolves; no target write is made. A record naming a birth
    /// proven gone (a vacancy the election fills before this birth's own record lands) ends nothing.
    /// What must NOT happen: a moved record that still lets the old role decide, or a refusal naming
    /// no admin while a digest of the primary is held.
    #[tokio::test]
    async fn a_fabric_primary_record_naming_another_birth_refuses_this_admins_decisions_by_name() {
        use futures_util::FutureExt as _;
        use rdm_core::{IncarnationId, NodeId, PrimaryRole, Primary};
        let primaries = rdm_transport::topology::PrimaryView::default();
        let me = (NodeId::parse("300000000000").unwrap(), IncarnationId::parse(&rdm_core::fixture_crockford60("p1")).unwrap());
        let next = Primary { mesh: "mesh2".into(), node_id: NodeId::parse("100000000000").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("n1")).unwrap(), revision: 2 };
        let lost = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::<IncarnationId>::new()));
        let source = {
            let changed = Some(primaries.subscribe());
            let (primaries, me, lost) = (primaries.clone(), me.clone(), lost.clone());
            // The digest view holds mesh2.node-admin.1; nothing here consults a projected view.
            let read = std::sync::Arc::new(move || crate::primary_watch::fabric_primary_record(primaries.fabric().as_ref(), &me, &lost.lock().unwrap()));
            PrimarySource { read, changed }
        };
        let gate = DecisionGate::new(me.0.clone(), source);
        primaries.take(PrimaryRole::FabricPrimary, &Primary { mesh: "mesh2".into(), node_id: me.0.clone(), incarnation: me.1.clone(), revision: 1 });
        assert!(gate.enter().await.is_ok(), "the record names this birth: it decides");
        assert!(gate.target_set().now_or_never().is_none(), "no target while the record names this birth");
        primaries.take(PrimaryRole::FabricPrimary, &next);
        lost.lock().unwrap().insert(next.incarnation.clone());
        assert!(gate.enter().await.is_ok(), "a record naming a birth proven gone ends nothing");
        lost.lock().unwrap().clear();
        match gate.enter().await {
            Err(BuildStateError::RoleYielded { to, by, .. }) => {
                assert_eq!(to.as_ref().map(|b| (&b.node_id, &b.incarnation)), Some((&next.node_id, &next.incarnation)), "the refusal names the primary the record names");
                assert!(by.contains("100000000000") && by.contains("revision 2"), "the reason names the record: {by}");
            }
            other => panic!("the moved record must refuse the old role's decision by name, got {:?}", other.map(|_| ())),
        }
        assert!(gate.target_set().now_or_never().is_some(), "a wait on the target resolves once the record moved");
    }

    #[tokio::test]
    async fn a_new_incarnation_of_the_same_node_yields_with_exact_birth() {
        use rdm_core::{IncarnationId, NodeId, Primary};
        let node_id = NodeId::parse("300000000000").unwrap();
        let old = IncarnationId::parse(&rdm_core::fixture_crockford60("old")).unwrap();
        let new = IncarnationId::parse(&rdm_core::fixture_crockford60("new")).unwrap();
        let target = Primary { mesh: "mesh1".into(), node_id: node_id.clone(), incarnation: new.clone(), revision: 2 };
        let me = (node_id.clone(), old.clone());
        let source = PrimarySource {
            read: std::sync::Arc::new(move || crate::primary_watch::fabric_primary_record(Some(&target), &me, &Default::default())),
            changed: None,
        };
        let gate = DecisionGate::new(node_id.clone(), source);
        match gate.enter().await {
            Err(BuildStateError::RoleYielded { to: Some(to), .. }) => {
                assert_eq!(to.node_id, node_id);
                assert_eq!(to.incarnation, new);
                assert_ne!(to.incarnation, old);
            }
            other => panic!("a newer incarnation must end this birth's decisions: {:?}", other.map(|_| ())),
        };
    }

    /// CONTRACT (R-A2 item 2): a role commit on this admin waits for the decision in flight (it
    /// commits wholly before the record moves), and every decision after the move is refused by
    /// name.
    #[tokio::test]
    async fn a_fabric_primary_change_waits_for_the_decision_in_flight_and_refuses_every_later_one() {
        use rdm_core::{IncarnationId, NodeId, PrimaryRole, Primary};
        let primaries = rdm_transport::topology::PrimaryView::default();
        let me = (NodeId::parse("300000000000").unwrap(), IncarnationId::parse(&rdm_core::fixture_crockford60("p1")).unwrap());
        let source = {
            let changed = Some(primaries.subscribe());
            let (primaries, me) = (primaries.clone(), me.clone());
            PrimarySource { read: std::sync::Arc::new(move || crate::primary_watch::fabric_primary_record(primaries.fabric().as_ref(), &me, &Default::default())), changed }
        };
        primaries.take(PrimaryRole::FabricPrimary, &Primary { mesh: "mesh1".into(), node_id: me.0.clone(), incarnation: me.1.clone(), revision: 1 });
        let gate_ref = std::sync::Arc::new(DecisionGate::new(me.0.clone(), source));
        let committed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = gate_ref.enter().await.unwrap();
        let (decisions, c, primary_view) = (gate_ref.decisions(), committed.clone(), primaries.clone());
        let moving = tokio::spawn(async move {
            let _held = decisions.lock_owned().await;
            primary_view.take(PrimaryRole::FabricPrimary, &Primary { mesh: "mesh1".into(), node_id: NodeId::parse("400000000000").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("n1")).unwrap(), revision: 2 });
            c.load(std::sync::atomic::Ordering::SeqCst)
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!moving.is_finished(), "the role commit waits for the decision holding the gate");
        committed.store(true, std::sync::atomic::Ordering::SeqCst);
        drop(gate);
        assert!(moving.await.unwrap(), "the record moved after the decision committed");
        let later = gate_ref.enter().await.map(|_| ());
        match later {
            Err(BuildStateError::RoleYielded { node, by, to }) => {
                assert_eq!(node, me.0);
                assert!(by.contains("400000000000") && by.contains("revision 2"), "the reason names the record without requiring a digest: {by}");
                assert_eq!(to.as_ref().map(|b| &b.node_id), Some(&NodeId::parse("400000000000").unwrap()));
            }
            other => panic!("a decision after the record moved must be refused by name, got {other:?}"),
        }
    }
}
