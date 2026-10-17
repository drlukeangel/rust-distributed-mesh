//! The lifecycle/status declarations node-admin applies as an authority (i143.e6.s7;
//! node-rpc.md §38; node-rpc-envelope.md §3).
//!
//! A declaration reaches this admin over Node RPC (`Status`, op `0x1B`). Nothing inside the mesh
//! checks who is calling (node-rpc.md §14): a declaration is decided for the subject the request
//! names, as this admin's view holds it. The admin applies it only when it holds the seat the
//! subject's declaration needs (`RejectedNotAuthority` names the receiver, never the caller),
//! idempotently on the natural key, forward along the legal order, and answers what happened.
//! `Applied` means the declared state is held in this admin's entity state and, for a node,
//! written to `nodes.storage` as the subject's row.
//!
//! Gossip is untouched: the view's `status` stays what membership says; what the authority
//! applied is the view's `declared`.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, EndpointId};
use crate::topology::Topology;
use rdm_transport::{PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::status::{transition, DeclaredOperation, FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest, Transition};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

fn request_name(start: bool) -> &'static str { if start { "node.start" } else { "node.stop" } }

fn operation_row(row: &crate::status_storage::NodeDeclaredRow, operation: &DeclaredOperation) -> crate::status_storage::NodeOperationRow {
    crate::status_storage::NodeOperationRow { node_id: row.node_id.clone(), incarnation_id: row.incarnation_id.clone(), name: row.name.clone(), state: row.state, operation: operation.clone() }
}

/// What this admin applied, by natural key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Declared {
    /// Applied operation effects survive every cycle begin; forgotten only with the exact birth.
    pub operations: BTreeMap<String, crate::status_storage::NodeOperationRow>,
    /// Per birth `(node id, incarnation)`: the greatest state it declared.
    pub nodes: std::collections::HashMap<(NodeId, IncarnationId), NodeState>,
    /// Per birth held in `nodes`: the path.name it declared at.
    pub names: std::collections::HashMap<(NodeId, IncarnationId), crate::model::PathName>,
    /// Per mesh id.
    pub meshes: std::collections::HashMap<MeshId, MeshState>,
    /// Per fabric id: the events applied, in order, each once.
    pub fabric: std::collections::HashMap<FabricId, Vec<String>>,
    /// Per mesh name, per reporter: when this admin, as the fabric-primary, last received a
    /// `ReadyForTraffic` declaration of that mesh from that reporter ([`Declared::report_of`] reads
    /// the one that counts). Memory of this process's tenure; it is never stored or rehydrated,
    /// because a report is evidence only while it is fresh.
    pub reports: BTreeMap<String, std::collections::HashMap<Reporter, std::time::Instant>>,
    /// Per birth `(node id, incarnation)` of this admin's mesh: the `node.connections.deleted` it
    /// answered the last `node.connections.delete` this mesh-primary carried to it, in its current
    /// cycle (R-CR2: what a rename of a live birth needs). Memory of this tenure: a successor holds
    /// none, and refuses a live rename until the delete is carried again.
    pub connections_deleted: std::collections::HashMap<(NodeId, IncarnationId), ConnectionsDeletedReceipt>,
}

/// A birth's `node.connections.deleted`, as its mesh-primary holds it for the rename precondition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionsDeletedReceipt {
    /// Whether the cut was `All` (gossip neighbours and every pooled peer connection).
    pub all: bool,
    /// The open entries the birth still held after the cut.
    pub held: Vec<rdm_core::ConnectionId>,
    /// The birth's status in this admin's view when the cut was carried.
    pub status_at_cut: crate::model::NodeStatus,
}

/// A mesh primary's round-complete report, as the fabric-primary received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshReport {
    /// The birth of the node-admin that reported.
    pub incarnation: IncarnationId,
    /// When this admin received it.
    pub at: std::time::Instant,
}

/// Who sent a Mesh's round-complete report, as this admin could name it when it arrived: the birth
/// its view held for the calling endpoint, or only that endpoint when its view held no birth for it
/// yet (nothing inside the mesh checks who is calling, node-rpc.md §14, so such a report is taken).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Reporter {
    /// The exact birth that sent it.
    Birth(IncarnationId),
    /// The transport-proven endpoint that sent it, its birth not yet in this admin's view.
    Endpoint(String),
}

impl Declared {
    /// The state the birth `(node_id, incarnation)` declared, when it declared one.
    pub fn node(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<NodeState> {
        self.nodes.get(&(node_id.clone(), incarnation.clone())).copied()
    }

    /// Hold `mesh`'s round-complete report from `reporter`, received `at`: one per reporter, so a
    /// report from one birth never displaces another's.
    pub fn record_report(&mut self, mesh: &str, reporter: Reporter, at: std::time::Instant) {
        self.reports.entry(mesh.to_string()).or_default().insert(reporter, at);
    }

    /// The report of `mesh` that counts for `primary`, the birth this admin's view names its primary
    /// now: the one that birth sent, or the one its endpoint sent before the view held the birth.
    /// Else the latest report another birth sent, named by that birth (`report-of-another-birth` at
    /// the reader); `None` when no birth reported.
    pub fn report_of(&self, mesh: &str, primary: Option<&crate::model::Node>) -> Option<MeshReport> {
        let by = self.reports.get(mesh)?;
        if let Some((p, incarnation)) = primary.and_then(|p| p.incarnation_id.clone().map(|i| (p, i))) {
            let own = by.get(&Reporter::Birth(incarnation.clone())).or_else(|| p.endpoint_id.as_ref().and_then(|e| by.get(&Reporter::Endpoint(e.0.clone()))));
            if let Some(at) = own {
                return Some(MeshReport { incarnation, at: *at });
            }
        }
        by.iter()
            .filter_map(|(r, at)| match r {
                Reporter::Birth(i) => Some(MeshReport { incarnation: i.clone(), at: *at }),
                Reporter::Endpoint(_) => None,
            })
            .max_by_key(|r| r.at)
    }
}

impl Declared {
    /// Fold status rows into what this admin applied: the greatest state per Mesh and per birth,
    /// each Fabric event once, and the path.name each birth declared at. What this admin already
    /// holds stands where it is the greater; nothing is written back.
    pub fn fold_rows(&mut self, meshes: std::collections::HashMap<MeshId, MeshState>, events: Vec<crate::status_storage::FabricEventRow>, rows: Vec<crate::status_storage::NodeDeclaredRow>) {
        for (mesh, state) in meshes {
            let held = self.meshes.entry(mesh).or_insert(state);
            *held = (*held).max(state);
        }
        for e in events {
            let held = self.fabric.entry(e.fabric_id).or_default();
            if !held.contains(&e.event) {
                held.push(e.event);
            }
        }
        for (birth, name) in crate::status_storage::declared_names(&rows) {
            self.names.entry(birth).or_insert(name);
        }
        for (birth, state) in crate::status_storage::fold_declared(rows) {
            let held = self.nodes.entry(birth).or_insert(state);
            *held = (*held).max(state);
        }
    }

    /// The rows `status` holds, folded ([`Declared::fold_rows`]): what a seat holder rebuilds from.
    pub async fn rehydrate(status: &dyn crate::status_storage::StatusStorage) -> Result<Self, String> {
        let meshes = status.mesh_statuses().await.map_err(|e| format!("status.storage: {e}"))?;
        let events = status.fabric_events().await.map_err(|e| format!("status.storage: {e}"))?;
        let rows = status.node_declared().await.map_err(|e| format!("status.storage: {e}"))?;
        let mut declared = Self::default();
        declared.fold_rows(meshes, events, rows);
        for row in status.node_operations().await.map_err(|e| format!("status.storage: {e}"))? {
            declared.operations.insert(row.key(), row);
        }
        Ok(declared)
    }
}

/// The status rows of this admin's own mesh, which it folds into what it applied when it takes
/// the mesh's seat.
pub struct SeatFold {
    storage: Arc<dyn crate::status_storage::StatusStorage>,
    mesh: String,
    /// This admin's `path.name`: what the fold span names it by, as every status span does.
    name: crate::model::PathName,
    me: NodeId,
    /// Whether this admin holds the seat with the rows folded: cleared when a view names another
    /// holder, so each tenure folds once, at its start.
    folded: Mutex<bool>,
}

impl SeatFold {
    /// The rows of `mesh` in `storage`, folded by the admin `name` born as `me`.
    pub fn new(storage: Arc<dyn crate::status_storage::StatusStorage>, mesh: String, name: crate::model::PathName, me: NodeId) -> Self {
        Self { storage, mesh, name, me, folded: Mutex::new(false) }
    }

    /// The status store this fold reads: this admin's own mesh's rows, and another mesh's through
    /// its `_of` reads.
    pub fn storage(&self) -> &Arc<dyn crate::status_storage::StatusStorage> {
        &self.storage
    }

    /// The mesh whose status rows this fold reads.
    pub fn mesh(&self) -> &str {
        &self.mesh
    }

    /// The view `now` is being installed: when it names this admin its mesh's primary and this
    /// tenure has not folded the mesh's rows, fold them into `declared` (R14 item 7: the rebuild on
    /// a seat change reads the shared root). Every other admin of the mesh folds none of them: what
    /// it holds is only what it applied itself. Called under the view's write lock, so two installs
    /// never fold at once. A read that fails leaves the tenure unfolded, named in the span.
    ///
    /// The fold is the successor's acquisition of the held member statuses (node-status-heartbeat-
    /// connections.md §A.3), and `book`, this admin's digest book, is what its member projection is
    /// published from: each folded birth of the mesh whose state is ahead of what the book holds for
    /// it is adopted there ([`DigestBook::adopt_status`]), before the view naming this admin primary is
    /// installed and so before its first projection (fabric-authority.md §7: hydration, not
    /// transition). One `rdm.node_admin.status.update.via-seat-fold` per birth adopted or refused.
    pub async fn on_install(&self, now: &Topology, declared: &Mutex<Declared>, book: Option<&rdm_transport::membership::DigestBook>) {
        let holds = now.holds_mesh_seat(&self.mesh, &self.me);
        {
            let mut folded = self.folded.lock().unwrap();
            if !holds {
                *folded = false;
                return;
            }
            if *folded {
                return;
            }
        }
        let me_name = book.and_then(|b| b.held_path_name()).or_else(|| now.own(&self.me).map(|n| n.name.clone())).unwrap_or_else(|| self.name.clone());
        let span = tracing::info_span!(
            "rdm.node_admin.status.resolve.via-seat-taken",
            node = %me_name,
            node_id = %self.me,
            mesh = %self.mesh,
            node_declared_rows = tracing::field::Empty,
            mesh_status_rows = tracing::field::Empty,
            fabric_event_rows = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let read = async {
            let meshes = self.storage.mesh_statuses().await.map_err(|e| format!("Mesh status rows of {}: {e}", self.mesh))?;
            let events = self.storage.fabric_events().await.map_err(|e| format!("Fabric event rows of {}: {e}", self.mesh))?;
            let rows = self.storage.node_declared().await.map_err(|e| format!("node declared-state rows of {}: {e}", self.mesh))?;
            let operations = self.storage.node_operations().await.map_err(|e| format!("node operation rows of {}: {e}", self.mesh))?;
            Ok::<_, String>((meshes, events, rows, operations))
        };
        match tracing::Instrument::instrument(read, span.clone()).await {
            Ok((meshes, events, rows, operations)) => {
                for row in operations { declared.lock().unwrap().operations.insert(row.key(), row); }
                span.record("node_declared_rows", rows.len());
                span.record("mesh_status_rows", meshes.len());
                span.record("fabric_event_rows", events.len());
                declared.lock().unwrap().fold_rows(meshes, events, rows);
                *self.folded.lock().unwrap() = true;
                span.in_scope(|| tracing::info!("this admin took its mesh's seat: the mesh's status rows are folded into what it applied"));
                if let Some(book) = book {
                    span.in_scope(|| self.adopt_into(declared, book, &me_name));
                }
            }
            Err(e) => {
                span.record("error", e.as_str());
                span.in_scope(|| tracing::info!("this admin took its mesh's seat and could not read the mesh's status rows: it holds only what it applied"));
            }
        }
    }
}

impl SeatFold {
    /// Adopt into `book` every birth of this admin's mesh that `declared` holds a state for ahead of
    /// what `book` holds for that exact birth: the status its predecessor set, held by the successor.
    fn adopt_into(&self, declared: &Mutex<Declared>, book: &rdm_transport::membership::DigestBook, me_name: &crate::model::PathName) {
        let held: Vec<(NodeId, IncarnationId, crate::model::PathName, NodeState)> = {
            let declared = declared.lock().unwrap();
            declared
                .nodes
                .iter()
                .filter(|((node_id, _), _)| *node_id != self.me)
                .filter_map(|((node_id, incarnation), state)| declared.names.get(&(node_id.clone(), incarnation.clone())).filter(|n| n.mesh == self.mesh).map(|n| (node_id.clone(), incarnation.clone(), n.clone(), *state)))
                .collect()
        };
        for (node_id, incarnation, name, state) in held {
            let prior = book.member_status(&node_id, &incarnation);
            if transition(prior.map(declared_of_member), state) != Transition::Apply {
                continue;
            }
            let status = member_of_declared(state);
            let outcome = match book.adopt_status(&node_id, &incarnation, &name, status) {
                Ok(_) => "adopted",
                Err(why) => why,
            };
            tracing::info_span!(
                "rdm.node_admin.status.update.via-seat-fold",
                node = %me_name,
                member = %name,
                node_id = %node_id,
                incarnation_id = %incarnation.as_str(),
                status = ?status,
                prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
                outcome,
                "otel.kind" = "internal"
            )
            .in_scope(|| tracing::info!("the seat taker holds the status its predecessor set for the member: its projection carries it"));
        }
    }
}

pub use rdm_core::status::{declared_of_member, member_of_declared};

/// What the service reads and writes: this admin's identity, its current view, what it applied,
/// and the rows it writes.
pub struct StatusAuthority {
    /// The fabric-seat takeover this admin holds because the store could not commit it, shared with
    /// its handover door; `fabric.get` surfaces it (elections §5).
    pub held_takeover: crate::fabric_handover::HeldTakeovers,
    /// This admin's `path.name`: what it is named by in spans and refusals.
    pub me: crate::model::PathName,
    /// Asked before a node's declaration is decided ([`crate::wiring::DeclarationSeam`]); the product's
    /// is `None`.
    pub declaration_seam: Option<Arc<dyn crate::wiring::DeclarationSeam>>,
    /// Executable decoration before the command owner records NodeStopped.
    pub command_frame_seam: Option<Arc<dyn crate::wiring::CommandFrameSeam>>,
    /// Serialize operation effects with cycle begins; state-only declarations retain their own rule.
    pub operation_effects: tokio::sync::RwLock<()>,
    /// What publishes NodeStopped for a stop this admin recorded as its birth's mesh-primary (i146
    /// Q29, Q36); filled once the admin's backbone exists.
    pub stopped: Arc<OnceLock<StoppedPublisher>>,
    /// The node id this admin was born with: how it finds its own row and its seats in a view
    /// ([`Topology::own`]), never by `path.name`.
    pub node_id: NodeId,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// This admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// What this admin applied.
    pub declared: Arc<Mutex<Declared>>,
    /// The nodes store: the node rows a config update puts (`UpdateNodeConfig`).
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// Where an applied Mesh status, Fabric event or node declared state is a keyed row, put before
    /// it is answered.
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// The mesh ids this admin holds, by mesh name.
    pub mesh_ids: Arc<dyn Fn() -> BTreeMap<String, MeshId> + Send + Sync>,
    /// This node's re-publish of its presence (its peers handed to its mesh channel again, its
    /// digest published), filled once it has joined: what a node-admin's status kick asks of it.
    pub republish: Republish,
    /// The commands this admin sent (as an owning mesh-admin) and awaits a completion call for.
    pub commands: Arc<crate::node_commands::CommandBook>,
    /// This admin as the subject of `node.connections.delete` / `stop-node` from its own mesh-admin; filled once
    /// its digest and server exist.
    pub own: Arc<OnceLock<Arc<crate::node_self::NodeSelf>>>,
    /// What serves `DeleteMesh` (as a leaving mesh's primary) and `MeshDeleted` (as the
    /// fabric-primary); filled once the runner exists.
    pub mesh_deleter: crate::mesh_delete::MeshDeleterSlot,
    /// Testkit knob: hold the next reply past the caller's bound after applying (acceptance 2:
    /// apply + reply loss is `Indeterminate`, and the retry is `AlreadyApplied`).
    pub hold_next_reply: Arc<std::sync::atomic::AtomicBool>,
    /// Wakes this admin's declarer: a view this authority changed, and a down op's receipt.
    pub wake: Arc<crate::status_declare::DeclareWake>,
    /// This admin's rounds (commit-state, open-traffic); filled once the admin holds the handles
    /// they act through.
    pub rounds: Arc<OnceLock<Arc<crate::fabric_rounds::FabricRounds>>>,
    /// This admin's `node.config.update` door, served while it holds a mesh's primary seat (the writer
    /// of that mesh's rows); filled once the admin's node-RPC client exists.
    pub configs: crate::node_config::NodeConfigsSlot,
    /// This admin as the subject of `ApplyNodeConfig`: it holds the config and publishes its digest
    /// carrying it; filled once its digest exists.
    pub configure: Configure,
    /// Where this admin's own `hydrate_before_ready` stands (the cell its start writes): while it is
    /// Blocked, a probe naming this admin is answered `Blocked` carrying its blocker.
    pub hydration: Hydrating,
    /// This admin as the subject of `ApplyNodeName`: it takes the name holding no connection (R-CR1)
    /// and publishes its digest carrying it; filled once its digest exists.
    pub rename: Rename,
    /// This admin's view taking a node's row its `node.name.update` moved (R13): the view is projected
    /// again, reading the moved row. Filled with the view.
    pub row_moved: RowMoved,
    /// Each member's load and gossip stats, as its latest digest carries them: what `node.get`
    /// answers beside the view. `None` only where no admin owns a digest book; a view read there is
    /// refused `NotReady` by name.
    pub loads: Option<crate::views::Loads>,
    /// This admin's digest book: as its mesh's primary it sets each member's status there from the
    /// member's applied report (`DigestBook::started` for its node.started, `DigestBook::reported`
    /// otherwise). `None` only where no admin owns a digest book.
    pub book: Option<rdm_transport::membership::DigestBook>,
    /// Whether the initial fabric has first reached ready-for-traffic: until it has, a caller's
    /// property command is refused `RejectedFabricNotReady` and nothing is written.
    pub first_ready: crate::fabric_state::FirstReady,
    /// This admin's builds store: `fabric.get` answers the current Build from it, read last in, first
    /// out (build.md §4; R-BI3: "Each accepted Build is its own entity in the builds store, written
    /// once"; "The Fabric row carries no pointer to the newest Build").
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
}

/// What an `Applied` decision changes once its row is acknowledged. A decision never mutates
/// `Declared`: the effect is committed only after the put returned `Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// A node's declared-state row: the state its exact birth declared, and the operation that
    /// produced it when one did (i146 Q36).
    Node(crate::status_storage::NodeDeclaredRow, NodeState, Option<DeclaredOperation>),
    /// A mesh and the state applied to it.
    Mesh(MeshId, MeshState),
    /// A fabric event applied.
    Fabric(FabricId, String),
}

/// The decision, pure over a view and what was applied: what the admin does with a declaration.
/// It is decided for the subject the request names, never for its caller (node-rpc.md §14). It
/// reads `declared` and returns the effect to commit; it writes nothing.
pub fn decide(auth: &StatusAuthority, view: &Topology, declared: &Declared, req: &StatusRequest) -> (StatusReply, Option<Effect>) {
    let me = match view.own(&auth.node_id) {
        Some(n) => n,
        None => return (rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() }), None),
    };
    let me_name = auth.held_name_in(view);
    match req {
        StatusRequest::DeclareNodeState { node_id, incarnation, state, operation } => {
            // The subject, as this admin's view holds it.
            let Some(subject) = view.births().find(|n| n.node_id == *node_id) else {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown), None);
            };
            // The receiver's seat: every kind, a node-admin included, reports to its mesh's primary,
            // which sets its status (node-status.md "Status report").
            if !(me.is_primary && me.mesh == subject.mesh) {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: format!("mesh-primary of {}", subject.mesh) }), None);
            }
            // The exact birth: a declaration for an incarnation the view no longer holds is stale.
            match subject.incarnation_id.as_ref() {
                Some(held) if held == incarnation => {}
                Some(held) => return (rdm_core::reply::stale(held.clone()), None),
                None => return (rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown), None),
            }
            if let Some(operation) = operation {
                let row = crate::status_storage::NodeDeclaredRow { node_id: subject.node_id.clone(), incarnation_id: incarnation.clone(), name: subject.name.clone(), state: *state };
                if declared.operations.contains_key(&operation_row(&row, operation).key()) {
                    return (rdm_core::reply::already_applied(), None);
                }
            }
            let current = declared.node(&subject.node_id, incarnation);
            match transition(current, *state) {
                Transition::AlreadyApplied if operation.is_some() => {
                    let row = crate::status_storage::NodeDeclaredRow { node_id: subject.node_id.clone(), incarnation_id: incarnation.clone(), name: subject.name.clone(), state: *state };
                    (rdm_core::reply::already_applied(), Some(Effect::Node(row, *state, operation.clone())))
                }
                Transition::AlreadyApplied => (rdm_core::reply::already_applied(), None),
                Transition::Backward { current } => (rdm_core::reply::status::rejected_invalid_node_transition(current), None),
                Transition::Apply => {
                    let row = crate::status_storage::NodeDeclaredRow { node_id: subject.node_id.clone(), incarnation_id: incarnation.clone(), name: subject.name.clone(), state: *state };
                    (rdm_core::reply::applied(), Some(Effect::Node(row, *state, operation.clone())))
                }
            }
        }
        StatusRequest::DeclareMeshState { mesh_id, state } => {
            if !me.is_fabric_primary {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: "fabric-primary".into() }), None);
            }
            // The subject Mesh, as this holder knows it: an id its own Build created, else the id a
            // Mesh carries in the view. The fabric seat moves to whichever admin has the lowest node
            // id, which may hold a Mesh it never created.
            let held = (auth.mesh_ids)().values().any(|id| id == mesh_id) || view.meshes.iter().any(|m| m.id.as_ref() == Some(mesh_id));
            if !held {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown), None);
            }
            apply_mesh(declared, mesh_id, *state)
        }
        // Downward exact-node operations are answered by the birth they reach (`self_subject`), which
        // this admin answers as once its view holds it.
        StatusRequest::ApplyNodeState { .. } | StatusRequest::ProbeNodeState { .. } | StatusRequest::ApplyNodeConfig { .. } | StatusRequest::ApplyNodeName { .. } => {
            (rdm_core::reply::status::not_ready(format!("{me_name} holds no view of itself yet: {} is answered by the birth it reaches once its view holds it", req.op())), None)
        }
        // Commands, completions and reads are answered before the decision (`StatusAuthority::apply`):
        // the decision has no handler for them, so not handled here (R24), decided by dispatch alone.
        StatusRequest::Reserved6 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::NodeLeft { .. } | StatusRequest::DeleteMesh { .. } | StatusRequest::MeshDeleted { .. } | StatusRequest::CommitState { .. } | StatusRequest::StateCommitted { .. } | StatusRequest::OpenTraffic { .. } | StatusRequest::TrafficOpened { .. } | StatusRequest::UpdateNodeConfig { .. } | StatusRequest::UpdateName { .. } | StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. } | StatusRequest::DeleteConnections { .. } | StatusRequest::GetNodes { .. } | StatusRequest::GetMeshes { .. } | StatusRequest::GetFabric | StatusRequest::GetConnections { .. } | StatusRequest::JoinMesh { .. } | StatusRequest::LeaveMesh { .. } => {
            (crate::node_self::request_not_handled(&me_name.to_string(), req), None)
        }
        StatusRequest::ApplyMeshState { mesh_id, mesh_name, state } => {
            if me.mesh != *mesh_name {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: format!("an admin of {mesh_name}") }), None);
            }
            // The mesh's primary writes its status (node-identity-and-state.md 4.3): an admin whose view
            // names another admin the mesh's primary is not the receiver. One whose view names none
            // yet is the admin the hand-off created, before its own seat shows in its view.
            if let Some(primary) = view.cohort_primary(mesh_name, NodeKind::NodeAdmin).filter(|p| p.node_id != auth.node_id) {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: format!("the mesh-primary of {mesh_name}, which this admin's view names {} ({})", primary.name, primary.node_id) }), None);
            }
            // Never a replacement mesh id to satisfy the operation.
            if let Some(held) = (auth.mesh_ids)().get(mesh_name) {
                if held != mesh_id {
                    return (rdm_core::reply::status::rejected_stale_mesh(held.clone()), None);
                }
            }
            apply_mesh(declared, mesh_id, *state)
        }
        StatusRequest::ApplyFabricEvent { fabric_id, event } => {
            if !me.is_primary {
                return (rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: "mesh-primary".into() }), None);
            }
            if *fabric_id != auth.fabric_id {
                return (rdm_core::reply::status::rejected_stale_fabric(auth.fabric_id.clone()), None);
            }
            let key = event_key(event);
            if declared.fabric.get(fabric_id).is_some_and(|events| events.contains(&key)) {
                return (rdm_core::reply::already_applied(), None);
            }
            (rdm_core::reply::applied(), Some(Effect::Fabric(fabric_id.clone(), key)))
        }
    }
}

pub(crate) fn event_key(e: &FabricEvent) -> String {
    match e {
        FabricEvent::ReadyForTraffic => "ready-for-traffic".into(),
        FabricEvent::ShutdownInitiated { initiated_by } => format!("shutdown-initiated:{initiated_by}"),
    }
}

/// One commit of a Fabric event among the status rows of this admin's mesh: the keyed row put and
/// acknowledged, then held as applied once and answered (`Applied` the first time, `AlreadyApplied`
/// after). A refused put answers `NotReady` naming the store and holds nothing, so the same commit again
/// is decided afresh. The fabric-primary's own ready-for-traffic row, put as it enters ready-for-traffic
/// (`crate::fabric_rounds`), and an `ApplyFabricEvent` it sends a mesh primary commit here alike.
pub(crate) async fn commit_fabric_event(storage: &dyn crate::status_storage::StatusStorage, declared: &Mutex<Declared>, fabric_id: &FabricId, event: &str) -> StatusReply {
    if let Err(e) = storage.put_fabric_event(&crate::status_storage::FabricEventRow { fabric_id: fabric_id.clone(), event: event.to_string() }).await {
        return rdm_core::reply::status::not_ready(format!("status.storage refused the Fabric event row: {e}"));
    }
    let mut declared = declared.lock().unwrap();
    let events = declared.fabric.entry(fabric_id.clone()).or_default();
    if events.iter().any(|e| e == event) {
        rdm_core::reply::already_applied()
    } else {
        events.push(event.to_string());
        rdm_core::reply::applied()
    }
}

/// One commit of a Mesh status at its writer: decided over what is held, the keyed row put and
/// acknowledged, then held as applied and answered. A refused put answers `NotReady` naming the
/// store and holds nothing, so the same commit again is decided afresh; the commit decides again
/// over what is held after the put, so two commits of one natural key racing answer `Applied` once.
pub(crate) async fn commit_mesh_state(storage: &dyn crate::status_storage::StatusStorage, declared: &Mutex<Declared>, mesh_id: &MeshId, state: MeshState) -> StatusReply {
    let (decided, effect) = apply_mesh(&declared.lock().unwrap(), mesh_id, state);
    if effect.is_none() {
        return decided;
    }
    if let Err(e) = storage.put_mesh_status(&crate::status_storage::MeshStatusRow { mesh_id: mesh_id.clone(), state }).await {
        return rdm_core::reply::status::not_ready(format!("status.storage refused the Mesh status row {mesh_id}-{state:?}: {e}"));
    }
    let mut declared = declared.lock().unwrap();
    match apply_mesh(&declared, mesh_id, state) {
        (StatusReply::Applied, _) => {
            declared.meshes.insert(mesh_id.clone(), state);
            rdm_core::reply::applied()
        }
        (other, _) => other,
    }
}

fn apply_mesh(declared: &Declared, mesh_id: &MeshId, state: MeshState) -> (StatusReply, Option<Effect>) {
    match transition(declared.meshes.get(mesh_id).copied(), state) {
        Transition::AlreadyApplied => (rdm_core::reply::already_applied(), None),
        Transition::Backward { current } => (rdm_core::reply::status::rejected_invalid_mesh_transition(current), None),
        Transition::Apply => (rdm_core::reply::applied(), Some(Effect::Mesh(mesh_id.clone(), state))),
    }
}

impl StatusAuthority {
    /// The `path.name` this admin holds now: what its digest book or its view names its birth,
    /// falling back to `self.me` before its birth is in either.
    pub async fn held_name(&self) -> crate::model::PathName {
        if let Some(held) = self.book.as_ref().and_then(|b| b.held_path_name()) {
            return held;
        }
        self.topology.read().await.own(&self.node_id).map(|n| n.name.clone()).unwrap_or_else(|| self.me.clone())
    }

    fn held_name_in(&self, view: &Topology) -> crate::model::PathName {
        self.book.as_ref().and_then(|b| b.held_path_name()).or_else(|| view.own(&self.node_id).map(|n| n.name.clone())).unwrap_or_else(|| self.me.clone())
    }

    /// The node a call's authenticated endpoint is, from the view this authority decides from: a
    /// birth the view holds, or `None` for an endpoint it holds no birth for. It names the caller in
    /// spans and is where a reply owed later goes (a drain's completion, a leave's handoff); it
    /// decides nothing (node-rpc.md §14): a call from an endpoint the view does not hold is decided
    /// as any other.
    pub async fn sender_of(&self, peer: &EndpointId) -> Option<crate::model::Node> {
        self.topology.read().await.births().find(|n| n.endpoint_id.as_ref() == Some(peer)).cloned()
    }

    /// The one door every declaration goes through, whoever the sender is: a peer over Node RPC,
    /// or this admin itself when its view names it as the authority. Decide over the view; an
    /// `Applied` node state is durable (the subject's row) before it is answered, and the live
    /// view shows it at once; one span per decision names op, sender, outcome and whether the
    /// receiver holds its mesh's seat (accepting a Pending hand-off is not an election).
    pub async fn apply(&self, sender: Option<crate::model::Node>, req: &StatusRequest) -> StatusReply {
        let peer = sender.as_ref().and_then(|n| n.endpoint_id.as_ref()).and_then(|e| e.0.parse::<iroh::PublicKey>().ok());
        self.apply_from(peer, sender, req).await
    }

    /// `node.name.update` (`UpdateName`) at this admin, a caller's (`cutover` `None`) or a replace
    /// recipe's step in-process, which names its cutover's retained row
    /// ([`crate::node_name_update::Cutover`]): the one validator and writer of a rename.
    pub async fn update_name(&self, req: &StatusRequest, cutover: Option<&crate::node_name_update::Cutover>) -> StatusReply {
        let StatusRequest::UpdateName { node_id, incarnation, .. } = req else { unreachable!("update_name is called for UpdateName only") };
        let me = self.held_name().await;
        // The mesh-primary writes the record through its config door, the one door its record changes take turns at.
        let deleted = self.declared.lock().unwrap().connections_deleted.get(&(node_id.clone(), incarnation.clone())).cloned();
        let doors = crate::node_name_update::NameDoors { configs: self.configs.get().map(|c| &**c), moved: &self.row_moved, rename: &self.rename };
        let first_ready = crate::node_properties::FirstReadyOnRoot { first_ready: &self.first_ready, status: &*self.status_storage };
        crate::node_name_update::update(&me, &self.node_id, &self.topology, &*self.nodes_storage, doors, deleted.as_ref(), first_ready, req, cutover).await
    }

    /// [`Self::apply`] of a call whose transport-proven endpoint is `peer` (`PeerContext`): the
    /// connection a command spares is the call's own, whether or not the view holds a birth for it.
    pub async fn apply_from(&self, peer: Option<iroh::PublicKey>, sender: Option<crate::model::Node>, req: &StatusRequest) -> StatusReply {
        if let (Some(seam), StatusRequest::DeclareNodeState { node_id, state, .. }) = (&self.declaration_seam, req) {
            let subject = self.topology.read().await.births().find(|n| n.node_id == *node_id).map(|n| n.name.to_string()).unwrap_or_else(|| node_id.to_string());
            seam.before_declaration(&subject, *state).await;
        }
        let view = self.topology.read().await.clone();
        let me_name = self.held_name_in(&view);
        if let (Some(from), Some(rounds)) = (sender.as_ref(), self.rounds.get()) {
            rounds.heard_from(&from.node_id);
        }
        match req {
            StatusRequest::Reserved6 { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } => return crate::node_commands::refuse_reserved(&me_name.to_string(), &sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(), req),
            StatusRequest::NodeLeft { node_id, .. } => {
                let name = view.births().find(|n| n.node_id == *node_id).map(|n| n.name.to_string()).unwrap_or_default();
                return crate::node_commands::refuse_node_left(&me_name.to_string(), &name, &sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(), req);
            }
            StatusRequest::JoinMesh { .. } | StatusRequest::LeaveMesh { .. } => return self.serve_command(&view, sender.as_ref(), peer, req).await,
            StatusRequest::DeleteConnections { node_id, incarnation, scope } => return self.carry_delete_connections(&view, sender.as_ref(), node_id, incarnation, scope).await.status_reply(),
            StatusRequest::DeleteMesh { .. } | StatusRequest::MeshDeleted { .. } => {
                let Some(mesh_deleter) = self.mesh_deleter.get().cloned() else {
                    return rdm_core::reply::status::not_ready(format!("{me_name} is still starting: it cannot serve {} yet", req.op()));
                };
                let reply = if matches!(req, StatusRequest::DeleteMesh { .. }) { mesh_deleter.delete_mesh(peer, sender.as_ref(), req).await } else { mesh_deleter.mesh_deleted(sender.as_ref(), req).await };
                tracing::info_span!("rdm.node_admin.status.update.via-command", node = %me_name, op = req.op(), sender = %sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a mesh-deleted call naming this admin was decided"));
                return reply;
            }
            StatusRequest::CommitState { .. } | StatusRequest::StateCommitted { .. } | StatusRequest::OpenTraffic { .. } | StatusRequest::TrafficOpened { .. } => {
                return match self.rounds.get() {
                    Some(rounds) => rounds.serve(&view, sender.as_ref(), peer, req, self.own.get()).await,
                    None => rdm_core::reply::status::not_ready(format!("{me_name} has not yet built its rounds; {} is refused", req.op())),
                };
            }
            StatusRequest::UpdateName { .. } => return self.update_name(req, None).await,
            StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. } => {
                let first_ready = crate::node_properties::FirstReadyOnRoot { first_ready: &self.first_ready, status: &*self.status_storage };
                return crate::node_properties::tags(&me_name, &self.node_id, &self.topology, &*self.nodes_storage, self.configs.get().map(|c| &**c), first_ready, req).await;
            }
            StatusRequest::UpdateNodeConfig { node_id, .. } => {
                // A caller's config put, like every property command, is refused until the initial fabric
                // has first reached ready-for-traffic (proposal node-properties-own-commands.md §1).
                let first_ready = crate::node_properties::FirstReadyOnRoot { first_ready: &self.first_ready, status: &*self.status_storage };
                if let Some(refused) = crate::node_properties::refused_before_first_ready(&me_name, &self.topology, first_ready, "node.config.update", node_id, &tracing::Span::current()).await {
                    return refused;
                }
                return match self.configs.get() {
                    Some(configs) => configs.update(&me_name, &self.node_id, &view, &*self.nodes_storage, req).await,
                    None => rdm_core::reply::status::not_ready(format!("{me_name} has no node-RPC client yet; {} is refused", req.op())),
                };
            }
            // The reads: answered from what this admin holds, changing nothing.
            StatusRequest::GetNodes { select } => {
                return match self.loads.as_ref() {
                    Some(loads) => {
                        let planned = match self.rounds.get() {
                            Some(rounds) => rounds.planned_paths().await,
                            None => Err(format!("{me_name} holds no rounds yet: the current Build is not read")),
                        };
                        crate::views::nodes_answer_with_status(&me_name, &view, loads, &*self.nodes_storage, Some(&*self.status_storage), &planned, select).await
                    }
                    None => rdm_core::reply::status::not_ready(format!("{me_name} reads no digest book; {} is refused", req.op())),
                };
            }
            StatusRequest::GetMeshes { .. } | StatusRequest::GetFabric => {
                if self.loads.is_none() {
                    return rdm_core::reply::status::not_ready(format!("{me_name} reads no digest book; {} is refused", req.op()));
                }
                if matches!(req, StatusRequest::GetFabric) {
                    // The fabric's `build_id` is the current Build, read from this admin's builds store (R-BI3):
                    // a read that fails is named, never answered as no Build.
                    return match self.builds.current_build().await {
                        Ok(current) => {
                            let takeover = self.held_takeover.lock().unwrap().clone();
                            crate::views::fabric_answer_with_status(&me_name, &view, current.into_build_id().map(|b| b.to_string()), &*self.status_storage, takeover).await
                        }
                        Err(e) => rdm_core::reply::status::bounded_source_failed("builds.storage", "fabric.get", &format!("{me_name} could not read its current Build: {e}")),
                    };
                }
                return crate::views::view_answer(&me_name, &view, req).unwrap_or_else(|| rdm_core::reply::status::not_ready(format!("{me_name} answers no {} from its view", req.op())));
            }
            StatusRequest::GetConnections { incarnation, .. } => return self.read_connections(&view, incarnation).await,
            _ => {}
        }
        // A downward exact-node operation naming this admin itself (a probe, an apply) is
        // answered by the subject, through the same door.
        if let Some(me) = view.own(&self.node_id) {
            if let Some(reply) = self_subject(me, sender.as_ref(), peer, req, &self.republish, &self.configure, &self.rename, &self.hydration, &self.wake).await {
                return reply;
            }
        }
        let receiver_is_primary = view.own(&self.node_id).is_some_and(|n| n.is_primary);
        let (reply, effect) = {
            let declared = self.declared.lock().unwrap();
            decide(self, &view, &declared, req)
        };
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => reply,
        };
        // A `ReadyForTraffic` Mesh declaration the authority took (applied now or already applied) is
        // the mesh primary's round-complete report: the primary sends it only when its checklist is
        // complete. Whichever it was, the report is what this admin received now from that exact birth.
        // It is kept for the mesh the declaration names, from the birth that sent it (or its endpoint
        // while this admin's view holds no birth for it), never under the caller's own mesh.
        if let (StatusRequest::DeclareMeshState { mesh_id, state: MeshState::ReadyForTraffic }, StatusReply::Applied | StatusReply::AlreadyApplied) = (req, &reply) {
            let mesh = (self.mesh_ids)().into_iter().find(|(_, id)| id == mesh_id).map(|(name, _)| name).or_else(|| view.meshes.iter().find(|m| m.id.as_ref() == Some(mesh_id)).map(|m| m.name.clone()));
            let reporter = sender.as_ref().and_then(|from| from.incarnation_id.clone()).map(Reporter::Birth).or_else(|| peer.map(|k| Reporter::Endpoint(k.to_string())));
            if let (Some(mesh), Some(reporter)) = (mesh, reporter) {
                self.declared.lock().unwrap().record_report(&mesh, reporter, std::time::Instant::now());
                self.wake.report_notify.notify_waiters();
            }
        }
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node = %me_name,
            op = req.op(),
            state = %declared_state(req),
            sender = %sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(),
            outcome = reply.name(),
            detail = %detail(&reply),
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("declaration decided by the authority"));
        reply
    }
}

impl StatusAuthority {
    /// `node.connections.get` of this admin's exact birth: the connections it holds, under the name
    /// it holds. The birth the call reached answers it (R-RX5: no receiver refuses a call for the
    /// node id it names); another incarnation is refused naming the one held.
    async fn read_connections(&self, view: &Topology, incarnation: &IncarnationId) -> StatusReply {
        let Some(me) = view.own(&self.node_id) else {
            return rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() });
        };
        match me.incarnation_id.as_ref() {
            None => return rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown),
            Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
            Some(_) => {}
        }
        match self.own.get() {
            Some(own) => own.connections_get().await,
            None => rdm_core::reply::status::not_ready(format!("{} is still starting: it holds no connections writer yet, so node.connections.get is refused", me.name)),
        }
    }
}

impl StatusAuthority {
    /// A start begins `birth`'s next cycle (node-start.md): what it declared before its stop
    /// (`Leaving`) is no longer where its next declarations are measured from, so `Pending` and
    /// `ReadyForTraffic` run forward again from the start. The cleared state is a row put before it
    /// is held, as every applied state is.
    pub async fn begin_cycle(&self, node: &crate::model::Node, birth: &IncarnationId) {
        let _operation = self.operation_effects.write().await;
        // The cut of its last cycle is not its next cycle's: a started birth holds connections again.
        self.declared.lock().unwrap().connections_deleted.remove(&(node.node_id.clone(), birth.clone()));
        let held = self.declared.lock().unwrap().node(&node.node_id, birth);
        let Some(state) = held else { return };
        let outcome = match self.status_storage.remove_node_declared(&node.node_id, birth).await {
            Ok(()) => {
                let mut declared = self.declared.lock().unwrap();
                declared.nodes.remove(&(node.node_id.clone(), birth.clone()));
                declared.names.remove(&(node.node_id.clone(), birth.clone()));
                "begun"
            }
            Err(_) => "refused",
        };
        tracing::info_span!("rdm.node_admin.status.update.via-cycle-begun", node = %node.name, node_id = %node.node_id, incarnation_id = %birth.as_str(), held = ?state, outcome, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("a start begins the birth's next cycle: its declared state is measured from Pending again"));
    }

    /// The declared-state rows of every birth this admin holds that `book` says is superseded: the
    /// book holds a later incarnation of the node (a restart), holds the node departed
    /// (node.deleted), or holds the birth's path.name under another node_id (a recovery's new node,
    /// node-recovery.md). Each such birth's keys are tombstoned, blind, and then forgotten in memory.
    /// A birth the book holds at the same incarnation keeps its rows, however long it is silent, and
    /// so does a birth the book does not hold at all whose path no other node holds (nothing is
    /// proven of it).
    pub async fn retire_superseded(&self, book: &rdm_transport::membership::DigestBook) {
        let me_name = book.held_path_name().unwrap_or_else(|| self.me.clone());
        // The births held and their paths, read under the lock and the lock released before any await.
        let held: Vec<((NodeId, IncarnationId), Option<crate::model::PathName>)> = {
            let declared = self.declared.lock().unwrap();
            let mut births: std::collections::HashMap<_, _> = declared.nodes.keys().map(|k| (k.clone(), declared.names.get(k).cloned())).collect();
            for row in declared.operations.values() { births.entry((row.node_id.clone(), row.incarnation_id.clone())).or_insert(Some(row.name.clone())); }
            births.into_iter().collect()
        };
        // Who the book holds at each path.name now.
        let at_path: std::collections::HashMap<crate::model::PathName, NodeId> = book.all().into_iter().map(|d| (d.node.name, d.node.node_id)).collect();
        for ((node_id, incarnation), name) in held {
            let (why, by) = match book.get(node_id.as_str()) {
                Some((d, _)) if d.node.incarnation != incarnation => ("new-incarnation", d.node.incarnation.to_string()),
                Some(_) => continue,
                None if book.is_departed(node_id.as_str()) => ("node-deleted", String::new()),
                None => match name.as_ref().and_then(|n| at_path.get(n)) {
                    Some(other) if *other != node_id => ("new-node", other.to_string()),
                    _ => continue,
                },
            };
            let operations: Vec<_> = self.declared.lock().unwrap().operations.values().filter(|r| r.node_id == node_id && r.incarnation_id == incarnation).cloned().collect();
            let mut failed = false;
            for row in &operations {
                if self.status_storage.remove_node_operation(row).await.is_err() { failed = true; break; }
            }
            if failed { continue; }
            self.declared.lock().unwrap().operations.retain(|_, r| r.node_id != node_id || r.incarnation_id != incarnation);
            let (outcome, error) = match self.status_storage.remove_node_declared(&node_id, &incarnation).await {
                Ok(()) => {
                    let mut declared = self.declared.lock().unwrap();
                    declared.nodes.remove(&(node_id.clone(), incarnation.clone()));
                    declared.names.remove(&(node_id.clone(), incarnation.clone()));
                    ("retired", String::new())
                }
                Err(e) => ("refused", e.to_string()),
            };
            let span = match why {
                "new-incarnation" => tracing::info_span!("rdm.node_admin.status.remove.via-new-incarnation", node = %me_name, node_id = %node_id, incarnation_id = %incarnation.as_str(), held_incarnation_id = %by, outcome, error = %error, "otel.kind" = "internal"),
                "new-node" => tracing::info_span!("rdm.node_admin.status.remove.via-new-node", node = %me_name, node_id = %node_id, incarnation_id = %incarnation.as_str(), path = %name.as_ref().map(ToString::to_string).unwrap_or_default(), held_node_id = %by, outcome, error = %error, "otel.kind" = "internal"),
                _ => tracing::info_span!("rdm.node_admin.status.remove.via-node-deleted", node = %me_name, node_id = %node_id, incarnation_id = %incarnation.as_str(), outcome, error = %error, "otel.kind" = "internal"),
            };
            span.in_scope(|| tracing::info!("a superseded birth's declared-state keys are tombstoned: nothing of that birth is folded back"));
        }
    }

    /// Q47: the mesh-primary records lifecycle state only from the exact node's ordered frames.
    pub async fn carry_lifecycle(&self, node: &crate::model::Node, start: bool, ctx: &crate::deployment::pipeline::CommandContext) -> crate::deployment::pipeline::CommandAdmission {
        self.carry_lifecycle_reaching(node, start, ctx).await.0
    }

    /// Carry the lifecycle command, preserving a typed never-sent reachability cause.
    pub async fn carry_lifecycle_reaching(&self, node: &crate::model::Node, start: bool, ctx: &crate::deployment::pipeline::CommandContext) -> (crate::deployment::pipeline::CommandAdmission, Option<crate::node_commands::Unreachable>) {
        let mut unreachable = None;
        let admission = async {
        use crate::deployment::pipeline::CommandAdmission as A;
        use rdm_core::node_command::{NodeCommand, NodeCommandReply as R, NodeCommandRequest as Q};
        use rdm_core::outcome::Code;
        use rdm_core::protocol::NodeProtocol;
        use tracing::Instrument;
        let Some(incarnation) = node.incarnation_id.clone() else { return A::NotSent { reason: "the command names no exact birth".into(), code: Code::FailedPrecondition }; };
        let op = rdm_core::LifecycleOp { node_id: node.node_id.clone(), incarnation: incarnation.clone(), name: node.name.clone(), build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone(), event_at_rafka_ms: 0 };
        let request = if start { Q::StartNode { node_id: node.node_id.clone(), incarnation: incarnation.clone(), build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone() } } else { Q::StopNode { node_id: node.node_id.clone(), incarnation: incarnation.clone(), build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone() } };
        let key = crate::node_commands::CommandKey { node_id: node.node_id.clone(), incarnation: incarnation.clone(), build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone() };
        let _ = self.commands.open(key.clone());
        let mut frames = if node.node_id == self.node_id {
            let Some(own) = self.own.get().cloned() else { return A::NotSent { reason: "the own birth has not joined".into(), code: Code::Unavailable }; };
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let commander = self.node_id.clone();
            tokio::spawn(async move {
                let terminal = own.serve_node_command(Some(&commander), &request, Box::new(crate::node_self::ChannelFrames(tx.clone()))).await;
                let _ = tx.send(terminal);
            }.instrument(tracing::Span::current()));
            CarriedFrames::InProcess(rx)
        } else {
            let Some(configs) = self.configs.get() else { return A::NotSent { reason: "this mesh-primary holds no Node RPC client".into(), code: Code::Unavailable }; };
            let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: std::time::Duration::from_secs(5), reply: if start { std::time::Duration::from_secs(60) } else { self.commands.connections_delete_reply() } }, ..Default::default() };
            match configs.client().call_stream::<NodeCommand>(&rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), &request, &opts).await {
                Ok((frames, _)) => CarriedFrames::Dialled(frames),
                Err((out, _)) => return match out {
                    rdm_core::outcome::RpcOutcome::NotSent(n) => {
                        unreachable = crate::node_commands::unreachable_of(n.reason());
                        A::NotSent { reason: format!("{:?}", n.reason()), code: n.reason().code() }
                    },
                    rdm_core::outcome::RpcOutcome::Reply(r) => { let r = r.into_value(); A::Refused { code: NodeCommand::classify_reply(&r).code(), reply: format!("{}: {r:?}", r.name()) } },
                    other @ (rdm_core::outcome::RpcOutcome::Unserved(_) | rdm_core::outcome::RpcOutcome::RejectedStale(_)) => A::Refused { code: other.code(), reply: format!("{}: {other:?}", other.name()) },
            other => A::Indeterminate { code: other.code(), reason: format!("{}: {other:?}", other.name()) },
                },
            }
        };
        let mut accepted = false;
        let mut closure = None;
        while let Some(item) = frames.next().await {
            let frame = match item {
                Ok(frame) => frame,
                Err(f) => return A::Indeterminate { code: match &f { rdm_transport::stream::StreamFailure::Indeterminate(c) => c.code(), _ => Code::Internal }, reason: format!("{} stream: {f:?}", request_name(start)) },
            };
            let position = frames.position();
            let result = match &frame {
                R::CommandAccepted { .. } => {
                    // Admission and completion share this operation's serialization: a declaration
                    // cannot publish stopped between the seen check and setting stopping.
                    let _admission = self.operation_effects.write().await;
                    accepted = true;
                    let declared = crate::status_storage::NodeDeclaredRow { node_id: op.node_id.clone(), incarnation_id: op.incarnation.clone(), name: op.name.clone(), state: NodeState::Leaving };
                    let identity = DeclaredOperation { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() };
                    let completed = self.declared.lock().unwrap().operations.contains_key(&operation_row(&declared, &identity).key());
                    if !start && !completed {
                        if let Some(book) = &self.book { book.admitted(&op, rdm_core::MemberStatus::Leaving); }
                        tracing::info_span!("rdm.node_admin.node.update.via-stop-admitted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation).in_scope(|| tracing::info!("CommandAccepted reports the stop's admission; this mesh-primary sets stopping"));
                    }
                    None
                }
                R::StopConnectionsDeleted { receipt } if !start => { closure = Some(receipt.clone()); None }
                R::NodeJoined if start => {
                    self.begin_cycle(node, &incarnation).await;
                    if let Some(book) = &self.book { book.joined(&op); }
                    let row = crate::status_storage::NodeDeclaredRow { node_id: node.node_id.clone(), incarnation_id: incarnation.clone(), name: node.name.clone(), state: NodeState::Pending };
                    let applied = self.persist_then_commit(Effect::Node(row, NodeState::Pending, None)).await;
                    if !matches!(applied, StatusReply::Applied | StatusReply::AlreadyApplied) { return A::Indeterminate { code: <Status as NodeProtocol>::classify_reply(&applied).code(), reason: format!("NodeJoined not recorded: {applied:?}") }; }
                    None
                }
                R::NodeStopped if !start => {
                    if let Some(seam) = &self.command_frame_seam { seam.before_stopped(&op).await; }
                    let applied = self.stop_received(&op).await;
                    if !matches!(applied, StatusReply::Applied | StatusReply::AlreadyApplied) { return A::Indeterminate { code: <Status as NodeProtocol>::classify_reply(&applied).code(), reason: format!("NodeStopped not recorded: {applied:?}") }; }
                    Some(match closure.clone() { Some(receipt) => A::Stopped { receipt }, None => A::Indeterminate { code: Code::Internal, reason: "NodeStopped without StopConnectionsDeleted".into() } })
                }
                R::NodeStarted if start => {
                    let row = crate::status_storage::NodeDeclaredRow { node_id: node.node_id.clone(), incarnation_id: incarnation.clone(), name: node.name.clone(), state: NodeState::ReadyForTraffic };
                    let operation = DeclaredOperation { build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone() };
                    let applied = self.persist_then_commit(Effect::Node(row, NodeState::ReadyForTraffic, Some(operation))).await;
                    Some(if matches!(applied, StatusReply::Applied | StatusReply::AlreadyApplied) { A::Started {} } else { A::StartFailed { code: <Status as NodeProtocol>::classify_reply(&applied).code(), step: "node.started".into(), reason: format!("ReadyForTraffic not recorded: {applied:?}") } })
                }
                R::Failed { step, reason, code } => Some(A::StartFailed { step: step.clone(), reason: reason.clone(), code: *code }),
                _ => Some(if accepted { A::Indeterminate { code: Code::Internal, reason: format!("unexpected {} after acceptance", frame.name()) } } else { A::Refused { code: NodeCommand::classify_reply(&frame).code(), reply: format!("{}: {frame:?}", frame.name()) } }),
            };
            let span = tracing::info_span!("rdm.node_admin.node.update.via-command-frame", recorder = %self.me, node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation.as_str(), operation = %ctx.operation, frame = frame.name(), invocation_id = tracing::field::Empty, seq = tracing::field::Empty, code = NodeCommand::classify_reply(&frame).code().name(), detail = NodeCommand::reply_detail(&frame));
            if let Some((invocation, seq)) = position { span.record("invocation_id", invocation.to_string()).record("seq", seq); }
            span.in_scope(|| tracing::info!("the mesh-primary recorded the node's command frame before acknowledging its recovery"));
            frames.accepted().await;
            if let Some(result) = result {
                if matches!(result, A::Stopped { .. }) {
                    if let Some(configs) = self.configs.get() {
                        if let Some(peer) = node.endpoint_id.as_ref().and_then(|e| e.0.parse::<iroh::PublicKey>().ok()) { configs.client().close_pooled_to(&peer, "node.stopped"); }
                    }
                }
                let _ = self.commands.complete(&key);
                return result;
            }
        }
        A::Indeterminate { code: Code::Unknown, reason: format!("{} ended without its terminal frame", request_name(start)) }
        }.await;
        (admission, unreachable)
    }

    /// `node.connections.delete` of a birth of this admin's mesh, carried in-process: the mesh-primary's
    /// own pipeline step (i144 R22). The birth is read from this admin's view.
    pub async fn carry_connections_delete(&self, node_id: &NodeId, incarnation: &IncarnationId, scope: &rdm_core::status::ConnectionScope) -> CarryEnd {
        let view = self.topology.read().await.clone();
        self.carry_delete_connections(&view, None, node_id, incarnation, scope).await
    }

    /// `node.connections.delete` naming a birth of this admin's mesh (R-CR2, R-CR3; i144 R22, Luke Q6,
    /// Q7): this mesh-primary validates the scope, commands the exact birth on the node command op
    /// 0x22, and reads the command's ordered reply stream: it records nothing before the command
    /// leaves; it records and publishes the birth connections-deleting when the node reports
    /// `ConnectionsDeleting`; it holds the birth's `node.connections.deleted` as its receipt for that
    /// birth's rename (an `Established` closure completes it; a `Deadline` one leaves the birth
    /// connections-deleting with its incomplete receipt). A scope that names nothing to cut is refused
    /// here, sending nothing. A command never sent, refused by the node, lost before the node accepted
    /// it, or broken after it accepted it are each their own end ([`CarryEnd`]). This mesh-primary its
    /// own subject runs its own birth's command path in-process and reads the same frames (Luke Q9).
    async fn carry_delete_connections(&self, view: &Topology, sender: Option<&crate::model::Node>, node_id: &NodeId, incarnation: &IncarnationId, scope: &rdm_core::status::ConnectionScope) -> CarryEnd {
        use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
        use tracing::Instrument;
        let me_name = self.held_name_in(view);
        let span = tracing::info_span!(
            "rdm.node_admin.node.connections.delete.via-mesh-primary",
            node = %me_name, subject_id = %node_id, subject = tracing::field::Empty, sender = %sender.map(|n| n.name.to_string()).unwrap_or_default(),
            scope = scope.name(), status_at_cut = tracing::field::Empty, carried = tracing::field::Empty, closed = tracing::field::Empty, held = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal"
        );
        let end = async {
            // A subject this receiver does not hold is the receiver's own fact (`SubjectUnknown`), never
            // a peer that did not resolve.
            let Some(subject) = view.nodes.iter().find(|n| n.node_id == *node_id) else {
                tracing::Span::current().record("detail", format!("{me_name}: node {node_id} is not in the mesh-primary's view; its connections cannot be deleted through it").as_str());
                return CarryEnd::Refused(rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown));
            };
            tracing::Span::current().record("subject", subject.name.to_string().as_str()).record("status_at_cut", format!("{:?}", subject.status).as_str());
            if !view.own(&self.node_id).is_some_and(|n| n.is_primary && n.mesh == subject.mesh) {
                return CarryEnd::Refused(rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed: format!("the mesh-primary of {}", subject.mesh) }));
            }
            if subject.incarnation_id.as_ref() != Some(incarnation) {
                return match subject.incarnation_id.clone() {
                    Some(held) => CarryEnd::Refused(rdm_core::reply::stale(held)),
                    None => {
                        tracing::Span::current().record("detail", format!("{me_name}: {} has no known birth; its connections cannot be deleted", subject.name).as_str());
                        CarryEnd::Refused(rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown))
                    }
                };
            }
            if let Some(reason) = scope.refusal() {
                return CarryEnd::Refused(rdm_core::reply::status::rejected_connection_scope(format!("{}: {reason}", subject.name)));
            }
            let all = matches!(scope, rdm_core::status::ConnectionScope::All);
            let command = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: scope.clone() };
            let mut frames = if *node_id == self.node_id {
                // This mesh-primary is its own subject: an endpoint does not dial itself, so its own
                // birth's command path runs in-process and its frames come back on a channel.
                let Some(own) = self.own.get().cloned() else {
                    return CarryEnd::Refused(rdm_core::reply::status::not_ready(format!("{me_name} has not yet joined its mesh; node.connections.delete of its own birth is refused and nothing is cut")));
                };
                tracing::Span::current().record("carried", "in-process");
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let commander = self.node_id.clone();
                tokio::spawn(
                    async move {
                        let completion = own.serve_node_command(Some(&commander), &command, Box::new(crate::node_self::ChannelFrames(tx.clone()))).await;
                        let _ = tx.send(completion);
                    }
                    .instrument(tracing::Span::current()),
                );
                CarriedFrames::InProcess(rx)
            } else {
                let Some(configs) = self.configs.get() else {
                    return CarryEnd::Refused(rdm_core::reply::status::not_ready(format!("{me_name} has no node-RPC client yet to carry node.connections.delete to {}; nothing is cut", subject.name)));
                };
                tracing::Span::current().record("carried", "dialled");
                let target = rdm_transport::NodeTarget::ExactNode(node_id.clone());
                let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: std::time::Duration::from_secs(5), reply: self.commands.connections_delete_reply() }, ..Default::default() };
                match configs.client().call_stream::<NodeCommand>(&target, &command, &opts).await {
                    Ok((stream, _)) => CarriedFrames::Dialled(stream),
                    Err((out, _)) => {
                        let end = match out {
                            rdm_core::outcome::RpcOutcome::NotSent(n) => CarryEnd::NotSent(n.reason().clone()),
                            rdm_core::outcome::RpcOutcome::Reply(r) => CarryEnd::RefusedByNode(r.into_value()),
                            other => CarryEnd::Lost { code: other.code(), reason: format!("{me_name} carried node.connections.delete to {} and the call ended {} before the node accepted it: {other:?}", subject.name, other.name()) },
                        };
                        tracing::Span::current().record("detail", end.detail().as_str());
                        return end;
                    }
                }
            };
            let mut accepted = false;
            let mut reported = false;
            while let Some(item) = frames.next().await {
                match item {
                    Ok(NodeCommandReply::CommandAccepted { .. }) => {
                        accepted = true;
                        frames.accepted().await;
                    }
                    Ok(NodeCommandReply::ConnectionsDeleting { .. }) => {
                        // The node reported it: this mesh-primary records and publishes the state the
                        // node entered (states.md "Who sets each state"), never before.
                        reported = true;
                        let recorded = self.book.as_ref().is_some_and(|book| book.reported(node_id, incarnation, rdm_core::MemberStatus::Draining));
                        let position = frames.position();
                        let span = tracing::info_span!("rdm.node_admin.node.update.via-connections-deleting", invocation_id = tracing::field::Empty, seq = tracing::field::Empty, node = %subject.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), recorder = %me_name, reported_by = "node", recorded, "otel.kind" = "internal");
                        if let Some((invocation, seq)) = position {
                            span.record("invocation_id", invocation.to_string()).record("seq", seq);
                        }
                        span.in_scope(|| tracing::info!("the node reported connections-deleting on its command's stream: this mesh-primary records it, and its projection carries it from its next round"));
                        frames.accepted().await;
                    }
                    Ok(NodeCommandReply::ConnectionsDeleted { closed, not_open, held, receipt }) => {
                        tracing::Span::current().record("closed", closed.len()).record("held", held.len());
                        let kept = ConnectionsDeletedReceipt { all, held: held.clone(), status_at_cut: if all { crate::model::NodeStatus::Draining } else { subject.status } };
                        self.declared.lock().unwrap().connections_deleted.insert((node_id.clone(), incarnation.clone()), kept);
                        if let Some(r) = &receipt {
                            // Established completes the closure; Deadline leaves the birth connections-deleting
                            // with its incomplete receipt (node-connections-delete.md steps 4-5).
                            tracing::info_span!("rdm.node_admin.node.update.via-connections-deleted", node = %subject.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), recorder = %me_name, receipt = r.name(), held = held.len(), "otel.kind" = "internal")
                                .in_scope(|| tracing::info!("the node's node.connections.deleted is held as the birth's receipt"));
                        }
                        frames.accepted().await;
                        return CarryEnd::Deleted { closed, not_open, held, receipt };
                    }
                    Ok(other) => {
                        let reason = format!("{} answered {} on its command's stream after {}", subject.name, other.name(), if accepted { "accepting it" } else { "nothing" });
                        tracing::Span::current().record("detail", reason.as_str());
                        frames.accepted().await;
                        return if accepted { CarryEnd::Incomplete { reported, reason, code: <NodeCommand as rdm_core::protocol::NodeProtocol>::classify_reply(&other).code() } } else { CarryEnd::RefusedByNode(other) };
                    }
                    Err(f) => {
                        let code = match &f {
                            rdm_transport::stream::StreamFailure::Indeterminate(cause) => cause.code(),
                            rdm_transport::stream::StreamFailure::Violation(_) => rdm_core::outcome::Code::Internal,
                        };
                        let reason = format!("{me_name}'s node.connections.delete stream to {} broke {}: {f:?}", subject.name, if accepted { "after the node accepted it" } else { "before the node accepted it" });
                        tracing::Span::current().record("detail", reason.as_str());
                        return if accepted { CarryEnd::Incomplete { reported, reason, code } } else { CarryEnd::Lost { reason, code } };
                    }
                }
            }
            let reason = format!("{me_name}'s node.connections.delete stream to {} ended without its completion", subject.name);
            if accepted { CarryEnd::Incomplete { reported, reason, code: rdm_core::outcome::Code::Unknown } } else { CarryEnd::Lost { reason, code: rdm_core::outcome::Code::Unknown } }
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", end.name());
        span.in_scope(|| tracing::info!("node.connections.delete carried by the mesh-primary"));
        end
    }

    /// A command naming this admin, served on the target and the exact birth it names (node-rpc.md
    /// §14): the commander is the caller as the view holds it, where its completion goes; the
    /// connection spared is the call's own (`peer`).
    async fn serve_command(&self, view: &Topology, sender: Option<&crate::model::Node>, peer: Option<iroh::PublicKey>, req: &StatusRequest) -> StatusReply {
        let (StatusRequest::StopNode { incarnation, .. } | StatusRequest::StartNode { incarnation, .. } | StatusRequest::JoinMesh { incarnation, .. } | StatusRequest::LeaveMesh { incarnation, .. }) = req else {
            unreachable!("serve_command is called for commands only")
        };
        let me_name = self.held_name_in(view);
        // The birth the call reached obeys it (R-RX5: no receiver refuses a downward call for the
        // node id it names); the exact birth it names is checked.
        let me = view.own(&self.node_id);
        let reply = match (me, self.own.get()) {
            (Some(me), _) if me.incarnation_id.as_ref().is_some_and(|held| held != incarnation) => rdm_core::reply::stale(me.incarnation_id.clone().expect("checked")),
            (_, Some(own)) => own.serve(sender.map(|n| &n.node_id), peer, req).await.expect("a command"),
            (_, None) => rdm_core::reply::status::not_ready(format!("{me_name} has not yet joined its mesh; {} is refused", req.op())),
        };
        tracing::info_span!("rdm.node_admin.status.update.via-command", node = %me_name, op = req.op(), sender = %sender.map(|n| n.name.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("a node command naming this admin was decided"));
        reply
    }

    /// An `Applied` decision: the fact's row is put and acknowledged, and only then is it held as
    /// applied and answered. A refused put answers `NotReady` naming the store and holds nothing,
    /// so the same declaration again is decided afresh. The commit decides again over what is
    /// held now, so two declarations of one natural key racing each other answer `Applied` once.
    async fn persist_then_commit(&self, effect: Effect) -> StatusReply {
        let operation = match &effect { Effect::Node(row, _, Some(op)) => Some(operation_row(row, op)), _ => None };
        let _operation = if operation.is_some() { Some(self.operation_effects.write().await) } else { None };
        let _state = if matches!(&effect, Effect::Node(_, _, None)) { Some(self.operation_effects.read().await) } else { None };
        if let Some(row) = &operation {
            if self.declared.lock().unwrap().operations.contains_key(&row.key()) {
                return rdm_core::reply::already_applied();
            }
        }
        if let Effect::Node(row, state, _) = &effect {
            if let Transition::Backward { current } = transition(self.declared.lock().unwrap().node(&row.node_id, &row.incarnation_id), *state) {
                return rdm_core::reply::status::rejected_invalid_node_transition(current);
            }
        }
        if let Effect::Mesh(mesh_id, state) = &effect {
            return commit_mesh_state(&*self.status_storage, &self.declared, mesh_id, *state).await;
        }
        if let Effect::Fabric(fabric_id, event) = &effect {
            let reply = commit_fabric_event(&*self.status_storage, &self.declared, fabric_id, event).await;
            // The row of the ready-for-traffic event is the durable fact that the fabric first reached
            // ready-for-traffic: the admin that put it knows it from that moment (`FirstReady`).
            if matches!(reply, StatusReply::Applied | StatusReply::AlreadyApplied) && *event == event_key(&FabricEvent::ReadyForTraffic) {
                self.first_ready.reach("ready-for-traffic-row-put");
            }
            return reply;
        }
        let put = match &effect {
            Effect::Node(row, ..) => self.status_storage.put_node_declared(row).await.map_err(|e| format!("status.storage refused the node declared-state row: {e}")),
            Effect::Mesh(..) => unreachable!("a Mesh status is committed by commit_mesh_state above"),
            Effect::Fabric(..) => unreachable!("a Fabric event is committed by commit_fabric_event above"),
        };
        if let Err(reason) = put {
            return rdm_core::reply::status::not_ready(reason);
        }
        if let Some(row) = &operation {
            if let Err(error) = self.status_storage.put_node_operation(row).await {
                return rdm_core::reply::status::not_ready(format!("status.storage refused the applied operation: {error}"));
            }
        }
        let reply = {
            let mut declared = self.declared.lock().unwrap();
            if let Some(row) = &operation { declared.operations.insert(row.key(), row.clone()); }
            match &effect {
                Effect::Node(row, state, _) => match transition(declared.node(&row.node_id, &row.incarnation_id), *state) {
                    Transition::Apply => {
                        declared.nodes.insert((row.node_id.clone(), row.incarnation_id.clone()), *state);
                        declared.names.insert((row.node_id.clone(), row.incarnation_id.clone()), row.name.clone());
                        rdm_core::reply::applied()
                    }
                    Transition::AlreadyApplied => rdm_core::reply::already_applied(),
                    Transition::Backward { current } => rdm_core::reply::status::rejected_invalid_node_transition(current),
                },
                Effect::Mesh(..) => unreachable!("a Mesh status is committed by commit_mesh_state above"),
                Effect::Fabric(..) => unreachable!("a Fabric event is committed by commit_fabric_event above"),
            }
        };
        if let (StatusReply::Applied, Effect::Node(row, ..)) = (&reply, &effect) {
            // The node's report, applied, sets its status (node-status.md "Status report": "A node
            // reports to its mesh's primary, which sets its status"), carried by this primary's member
            // projection. node.started, its ReadyForTraffic, also ends that exact birth's restart
            // (gossip.md "NodeRestarting stays until node.started of the same (node.id, incarnation)
            // arrives").
            if let Some(book) = &self.book {
                match row.state {
                    NodeState::ReadyForTraffic => {
                        book.started(&row.node_id, &row.incarnation_id);
                    }
                    state => {
                        book.reported(&row.node_id, &row.incarnation_id, member_of_declared(state));
                    }
                }
            }
            let mut t = self.topology.write().await;
            if let Some(n) = t.nodes.iter_mut().find(|n| n.node_id == row.node_id && n.incarnation_id.as_ref() == Some(&row.incarnation_id)) {
                n.declared = Some(format!("{:?}", row.state));
            }
            drop(t);
            self.wake.poke();
        }
        // A birth's Leaving, applied, that names the operation which stopped it (i146 Q29, Q36): this
        // mesh-primary publishes NodeStopped with that identity, as from the stop stream's `stopped`,
        // whichever of the two reached it first; the other finds the row held (`AlreadyApplied`) and
        // publishes nothing. A restart's stop is never NodeStopped (NodeRestarting holds it).
        if let (StatusReply::Applied, Effect::Node(row, NodeState::Leaving, Some(operation))) = (&reply, &effect) {
            if !operation.operation.starts_with("node.restart:") {
                if let Some(publish) = self.stopped.get() {
                    publish(row.clone(), operation.clone()).await;
                }
            }
        }
        reply
    }

    /// The stop stream's `stopped` of `op`'s exact birth, received by this mesh-primary as the stop's
    /// executor (node-stop.md steps 4-5): its Leaving is recorded in the same declared-state row the
    /// birth's own declaration names, so NodeStopped is published once, from whichever arrived first.
    pub async fn stop_received(&self, op: &rdm_core::LifecycleOp) -> StatusReply {
        let row = crate::status_storage::NodeDeclaredRow { node_id: op.node_id.clone(), incarnation_id: op.incarnation.clone(), name: op.name.clone(), state: NodeState::Leaving };
        let operation = DeclaredOperation { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() };
        let reply = self.persist_then_commit(Effect::Node(row, NodeState::Leaving, Some(operation))).await;
        tracing::info_span!("rdm.node_admin.node.update.via-stop-received", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation, outcome = reply.name(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the stop stream's stopped is recorded in the birth's declared-state row"));
        reply
    }

    /// Initial fabric bootstrap: the Day-0 root has no upstream authority to hand it its mesh's
    /// Pending, so it applies it to itself (e4.s11 "single-admin recovery root"). Only Pending, only
    /// for its own mesh; the seat check does not apply because no seat exists yet. The same span
    /// as every decision, with the sender named as itself.
    pub(crate) async fn self_apply_mesh_pending(&self, mesh_id: &MeshId) -> StatusReply {
        let me_name = self.held_name().await;
        let receiver_is_primary = self.topology.read().await.own(&self.node_id).is_some_and(|n| n.is_primary);
        let (decided, effect) = apply_mesh(&self.declared.lock().unwrap(), mesh_id, MeshState::Pending);
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => decided,
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node = %me_name,
            op = "apply-mesh-state",
            state = "Pending",
            sender = %me_name,
            outcome = reply.name(),
            detail = "self-applied: initial fabric bootstrap, no upstream authority",
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("the Day-0 root applies its own mesh's Pending"));
        reply
    }
}

/// Serve `Status` on this admin. `authority` is filled once the admin holds a view; until then a
/// declaration is `NotReady` by name.
/// A node's re-publish of its presence, filled once it has joined its mesh.
pub(crate) type Republish = Arc<OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// Where a node's own `hydrate_before_ready` stands: `None` until its start registered a hook.
pub type Hydrating = Arc<Mutex<Option<crate::app_hydration::HydrationHandle>>>;

/// This node holding the config it is told and publishing its digest carrying it (`ApplyNodeConfig`).
pub type Configure = Arc<OnceLock<Arc<dyn Fn(BTreeMap<String, String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// This node deciding a name its mesh-primary's row gives it (`ApplyNodeName`): the name it held
/// and the decision, the name taken and published only on `Applied`.
pub type Rename = Arc<OnceLock<Arc<dyn Fn(rdm_core::PathName) -> std::pin::Pin<Box<dyn std::future::Future<Output = (rdm_core::PathName, crate::node_name::NameApply)> + Send>> + Send + Sync>>>;

/// This mesh-primary's view taking a row `node.name.update` moved: `(from, node_id, incarnation, to)`.
pub type RowMoved = Arc<OnceLock<Arc<dyn Fn(rdm_core::PathName, NodeId, rdm_core::IncarnationId, rdm_core::PathName) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// What a node is now, in the declaration vocabulary.
pub fn node_state_of(s: crate::model::NodeStatus) -> NodeState {
    use crate::model::NodeStatus as S;
    match s {
        S::Pending => NodeState::Pending,
        S::ReadyForTraffic => NodeState::ReadyForTraffic,
        S::Draining => NodeState::Draining,
        // A birth under restart is leaving; the frozen status vocabulary has no Restarting.
        S::Leaving | S::Restarting | S::PendingReconnect | S::Dead | S::Deleted => NodeState::Leaving,
    }
}


/// The downward exact-node operations, answered by the birth they reach, whoever sends them
/// (node-rpc.md §14) and whatever node id they name (R-RX5). `Some(reply)` for those requests;
/// `None` for every other request.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// re-publishes its presence and answers its current state; no transition. While its
/// `hydrate_before_ready` hook is Blocked it answers `Blocked` with the hook's blocker and
/// re-publishes nothing. `ApplyNodeState` is not served: a node's connections are deleted by `node.connections.delete` and a stop is `stop-node`.
#[allow(clippy::too_many_arguments)]
async fn self_subject(me: &crate::model::Node, sender: Option<&crate::model::Node>, peer: Option<iroh::PublicKey>, req: &StatusRequest, republish: &Republish, configure: &Configure, rename: &Rename, hydration: &Hydrating, wake: &crate::status_declare::DeclareWake) -> Option<StatusReply> {
    let incarnation = match req {
        StatusRequest::ProbeNodeState { incarnation, .. } | StatusRequest::ApplyNodeState { incarnation, .. } | StatusRequest::ApplyNodeConfig { incarnation, .. } | StatusRequest::ApplyNodeName { incarnation, .. } => incarnation,
        _ => return None,
    };
    // The exact birth the call names is checked, so a call that reached another birth is refused
    // naming the one held.
    let Some(held) = me.incarnation_id.as_ref() else {
        return Some(rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown));
    };
    if held != incarnation {
        return Some(rdm_core::reply::stale(held.clone()));
    }
    let sender_name = sender.map(|n| n.name.to_string()).unwrap_or_default();
    match req {
        StatusRequest::ProbeNodeState { .. } => {
            // The hook's blocker as it stood when the probe arrived, before the probe addresses
            // this admin (a hook blocked on its authority's word runs again on it).
            let blocker = hydration.lock().unwrap().as_ref().and_then(|h| h.state().probe_blocker());
            // The authority addressed this admin (R-S3's down op): its endpoint is counted for a hook
            // blocked on its word, whether or not a record names it, and every declaration owed to it
            // is sent again, whatever its last outcome.
            wake.addressed_by(sender.map(|n| &n.node_id), peer);
            // A parked admin presents nothing: it says it is parked.
            if crate::node_self::parking().is_parked() {
                tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = "parked", "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("probed by a node-admin: parked, nothing re-published"));
                return Some(StatusReply::CurrentParked { node_id: me.node_id.clone(), incarnation: held.clone() });
            }
            // node-start.md step 3: while its hook is Blocked, the probe served this exact birth and
            // found an active readiness blocker; it answers Blocked with the hook's own reason and
            // retry event, before any republish, so a polled blocked birth republishes nothing.
            if let Some((reason, retry_on)) = blocker {
                let on = retry_on.name();
                let reply = rdm_core::reply::status::bounded_blocked(me.node_id.clone(), held.clone(), &reason, retry_on);
                tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = "blocked", blocker = %reason, retry_on = on, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("probed by a node-admin: the hook's blocker answered Blocked, nothing re-published"));
                return Some(reply);
            }
            if let Some(republish) = republish.get() {
                republish().await;
            }
            let state = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = ?state, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
            Some(StatusReply::Current { node_id: me.node_id.clone(), incarnation: held.clone(), state })
        }
        StatusRequest::ApplyNodeState { state, .. } => {
            // Not node.connections.delete and not a stop: neither is set by apply-node-state. The reply
            // carries the state this node holds; the span names why.
            let current = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.reject.via-apply-node-state", node = %me.name, sender = %sender_name, requested = ?state, current = ?current, reason = "node.connections.delete is the only path to connections-deleting and stop-node the only stop; apply-node-state is served for no state", "otel.kind" = "internal")
                .in_scope(|| tracing::info!("apply-node-state refused"));
            Some(rdm_core::reply::status::rejected_invalid_node_transition(current))
        }
        StatusRequest::ApplyNodeConfig { config, .. } => {
            // ops-naming "Node data": the node applies the config and carries it in its next digest.
            let Some(configure) = configure.get() else {
                return Some(rdm_core::reply::status::not_ready(format!("{}: its digest does not exist yet; the config is not held", me.name)));
            };
            configure(config.clone()).await;
            tracing::info_span!("rdm.node_admin.node.update.via-apply-node-config", node = %me.name, sender = %sender_name, keys = config.len(), outcome = "applied", "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the config its mesh-primary wrote is held and rides this admin's digest"));
            Some(rdm_core::reply::applied())
        }
        StatusRequest::ApplyNodeName { name, .. } => {
            // R12, R-CR1: the node owns its name and takes the row's name only while it holds no connection.
            let asked = match name.parse::<rdm_core::PathName>() {
                Ok(n) => n,
                Err(e) => {
                    tracing::info_span!("rdm.node_admin.node.reject.via-apply-node-name", node = %me.name, sender = %sender_name, to = %name, reason = %format!("the name {name:?} is not a path.name: {e}"), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("apply-node-name refused: the name does not parse"));
                    return Some(rdm_core::reply::status::malformed(rdm_core::outcome::MalformedKind::Corrupt));
                }
            };
            let Some(rename) = rename.get() else {
                return Some(rdm_core::reply::status::not_ready(format!("{}: its digest does not exist yet; the name {asked} is not taken", me.name)));
            };
            let (held, decision) = rename(asked.clone()).await;
            let routes = match &decision {
                crate::node_name::NameApply::Busy { routes, .. } => routes.join(", "),
                _ => String::new(),
            };
            tracing::info_span!("rdm.node_admin.node.update.via-apply-node-name", node = %held, sender = %sender_name, from = %held, to = %asked, routes = %routes, outcome = decision.name(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the mesh-primary's name for this admin was decided"));
            Some(crate::node_name::reply(&decision))
        }
        _ => None,
    }
}

/// Serve `Status` on this admin, and the node command op 0x22 for this admin as a commanded node
/// (i144 R22). `authority` is filled once the admin holds a view; until then a declaration is
/// `NotReady` by name.
pub fn serve(b: ServerBuilder, authority: Arc<OnceLock<Arc<StatusAuthority>>>) -> ServerBuilder {
    serve_node_command(b, authority.clone()).serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: StatusRequest| {
        let authority = authority.clone();
        async move {
            let Some(auth) = authority.get().cloned() else {
                return Ok(rdm_core::reply::status::not_ready("this admin holds no view yet".into()));
            };
            let peer_id = EndpointId(peer.endpoint_id.to_string());
            let sender = auth.sender_of(&peer_id).await;
            let reply = auth.apply_from(Some(peer.endpoint_id), sender, &req).await;
            if auth.hold_next_reply.swap(false, std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            Ok(reply)
        }
    })
}

/// The node command op 0x22 on this admin: a command its mesh-primary sends this admin's own birth,
/// answered on the command's ordered reply stream by the admin's own birth
/// ([`crate::node_self::NodeSelf::serve_node_command`]). Nothing checks who is calling (node-rpc.md
/// §14): the caller, as this admin's view holds it, is the command's commander.
fn serve_node_command(b: ServerBuilder, authority: Arc<OnceLock<Arc<StatusAuthority>>>) -> ServerBuilder {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
    b.serve_stream::<NodeCommand, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: NodeCommandRequest, sink| {
        let authority = authority.clone();
        async move {
            let Some(auth) = authority.get().cloned() else {
                return Ok(rdm_core::reply::node_command::not_ready("this admin holds no view yet".into()));
            };
            let Some(own) = auth.own.get().cloned() else {
                return Ok(rdm_core::reply::node_command::not_ready(format!("{} has not yet joined its mesh; {} is refused", auth.me, req.op())));
            };
            let sender = auth.sender_of(&EndpointId(peer.endpoint_id.to_string())).await;
            let reply = own.serve_node_command_on(sender.as_ref().map(|n| &n.node_id), Some(peer.endpoint_id), &req, Box::new(crate::node_self::StreamFrames::new(sink))).await;
            tracing::info_span!("rdm.node_admin.status.update.via-command", node = %auth.me, op = req.op(), sender = %sender.map(|n| n.name.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("a node command naming this admin was decided"));
            Ok(reply)
        }
    })
}

/// The frames of a carried node command, in order (Luke Q9): the dialled node's reply stream on op
/// 0x22, or, this mesh-primary its own subject, its own birth's command path run in-process. The frames
/// are the same either way.
enum CarriedFrames {
    Dialled(rdm_transport::stream::ReplyStream<rdm_core::node_command::NodeCommand>),
    InProcess(tokio::sync::mpsc::UnboundedReceiver<rdm_core::node_command::NodeCommandReply>),
}

impl CarriedFrames {
    /// Transport deduplication hands each position to this owner once. A local execution has no
    /// transport header, reconnect or replay; it reports through its one in-process channel.
    fn position(&self) -> Option<(&rdm_core::framing::DeliveryId, u32)> {
        match self {
            Self::Dialled(stream) => stream.position(),
            Self::InProcess(_) => None,
        }
    }

    /// This mesh-primary holds every frame handed over so far (recorded, or in the end it returns):
    /// the dialled node's frames are acked (node-rpc.md §31). In process there is nothing to ack.
    async fn accepted(&mut self) {
        if let Self::Dialled(stream) = self {
            stream.accepted().await;
        }
    }

    /// The next frame, or why the frames broke; `None` once they ended.
    async fn next(&mut self) -> Option<Result<rdm_core::node_command::NodeCommandReply, rdm_transport::stream::StreamFailure>> {
        use rdm_transport::stream::StreamItem;
        match self {
            Self::Dialled(stream) => stream.next().await.map(|item| match item {
                StreamItem::Frame(_, reply) => Ok(reply),
                StreamItem::Failed(f) => Err(f),
            }),
            Self::InProcess(rx) => rx.recv().await.map(Ok),
        }
    }
}

/// The state a declaration declares (`DeclareNodeState`, `DeclareMeshState`); empty for any other op.
fn declared_state(req: &StatusRequest) -> String {
    match req {
        StatusRequest::DeclareNodeState { state, .. } => format!("{state:?}"),
        StatusRequest::DeclareMeshState { state, .. } => format!("{state:?}"),
        _ => String::new(),
    }
}

fn detail(r: &StatusReply) -> String {
    match r {
        StatusReply::RejectedNotAuthority { why } => why.as_str().to_string(),
        StatusReply::RejectedStaleIncarnation { held } => held.to_string(),
        StatusReply::RejectedStaleMesh { held } => held.to_string(),
        StatusReply::RejectedStaleFabric { held } => held.to_string(),
        StatusReply::RejectedInvalidNodeTransition { current } => format!("{current:?}"),
        StatusReply::RejectedInvalidMeshTransition { current } => format!("{current:?}"),
        _ => String::new(),
    }
}

/// How a carried `node.connections.delete` ended (i144 R22, Luke Q6, Q7): an in-process answer, never
/// a wire reply. Each end is its own fact: nothing was sent, the node refused, the call was lost before
/// the node accepted, the stream broke after it accepted (incomplete), or the node completed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CarryEnd {
    /// This mesh-primary refused it before sending anything (a refusal it decided).
    Refused(StatusReply),
    /// The command never left: nothing changed on the node.
    NotSent(rdm_core::outcome::NotSentReason),
    /// The node refused it before accepting it (its refusal frame).
    RefusedByNode(rdm_core::node_command::NodeCommandReply),
    /// The call ended before the node accepted it: nothing is known to have run.
    Lost {
        /// The delivery explanation.
        reason: String,
        /// The typed delivery code.
        code: rdm_core::outcome::Code,
    },
    /// The node accepted it and the stream broke before its completion: incomplete. What the node
    /// reported stands (`reported`: it reported connections-deleting); no completion is claimed.
    Incomplete {
        /// Whether the node reported connections-deleting before the break.
        reported: bool,
        /// Why, by name.
        reason: String,
        /// The typed cause that interrupted the stream.
        code: rdm_core::outcome::Code,
    },
    /// The node's `node.connections.deleted`.
    Deleted {
        /// The open entries cut.
        closed: Vec<rdm_core::ConnectionId>,
        /// The ids named that are no open entry of the birth.
        not_open: Vec<rdm_core::ConnectionId>,
        /// The open entries the birth still holds.
        held: Vec<rdm_core::ConnectionId>,
        /// For `All`, what the closure established.
        receipt: Option<rdm_core::status::ConnectionsReceipt>,
    },
}

impl CarryEnd {
    /// The typed outcome of the command carried to the birth.
    pub fn code(&self) -> rdm_core::outcome::Code {
        use rdm_core::protocol::NodeProtocol;
        match self {
            Self::Refused(reply) => rdm_core::status::Status::classify_reply(reply).code(),
            Self::RefusedByNode(reply) => rdm_core::node_command::NodeCommand::classify_reply(reply).code(),
            Self::NotSent(cause) => cause.code(),
            Self::Lost { code, .. } | Self::Incomplete { code, .. } => *code,
            Self::Deleted { receipt: Some(rdm_core::status::ConnectionsReceipt::Deadline { .. }), .. } => rdm_core::outcome::Code::DeadlineExceeded,
            Self::Deleted { .. } => rdm_core::outcome::Code::Ok,
        }
    }

    /// The end's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Refused(_) => "refused",
            Self::NotSent(_) => "not-sent",
            Self::RefusedByNode(_) => "refused-by-node",
            Self::Lost { .. } => "lost",
            Self::Incomplete { .. } => "incomplete",
            Self::Deleted { .. } => "connections-deleted",
        }
    }

    /// What the end says, by name.
    pub fn detail(&self) -> String {
        match self {
            Self::Refused(r) => format!("{}: {r:?}", r.name()),
            Self::NotSent(n) => format!("never sent: {n:?}"),
            Self::RefusedByNode(r) => format!("{}: {r:?}", r.name()),
            Self::Lost { reason, .. } | Self::Incomplete { reason, .. } => reason.clone(),
            Self::Deleted { receipt, .. } => receipt.as_ref().map(|r| r.name().to_string()).unwrap_or_else(|| "connections".into()),
        }
    }

    /// The Status reply a caller of this mesh-primary's carrier gets (the caller-to-mesh-primary hop).
    pub fn status_reply(&self) -> StatusReply {
        use rdm_core::node_command::NodeCommandReply as N;
        match self {
            Self::Refused(r) => r.clone(),
            Self::Deleted { closed, not_open, held, receipt } => StatusReply::ConnectionsDeleted { closed: closed.clone(), not_open: not_open.clone(), held: held.clone(), receipt: receipt.clone() },
            Self::RefusedByNode(N::RejectedConnectionScope { reason }) => rdm_core::reply::status::rejected_connection_scope(reason.clone()),
            Self::RefusedByNode(N::RejectedStaleIncarnation { held }) => rdm_core::reply::stale(held.clone()),
            other => rdm_core::reply::status::not_ready(format!("node.connections.delete {}: {}", other.name(), other.detail())),
        }
    }
}

/// Shared handles, for the admin's wiring.
pub type AuthoritySlot = Arc<OnceLock<Arc<StatusAuthority>>>;

/// Publishes NodeStopped for the birth of `row` stopped by `operation` (i146 Q29, Q36).
pub type StoppedPublisher = Arc<dyn Fn(crate::status_storage::NodeDeclaredRow, DeclaredOperation) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

#[allow(dead_code)]
fn _assert_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<StatusAuthority>();
    let _ = RwLock::new(());
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::status::StatusReply as R;

    fn node(name: &str) -> crate::model::Node {
        let mut n = crate::model::Node::allocated(name.parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n
    }

    /// The silence status the mesh primary stored for `node` at `incarnation`, as a reader folds it.
    async fn stored_silence(status: &dyn crate::status_storage::StatusStorage, node: &crate::model::Node) -> Option<crate::model::NodeStatus> {
        status.node_silences().await.unwrap().into_iter().find(|c| c.node_id == node.node_id && Some(&c.incarnation_id) == node.incarnation_id.as_ref()).and_then(|c| c.status)
    }

    /// CONTRACT (#3051, CLAUDE.md read-modify-write ban): a node's declared state and the silence
    /// status its mesh primary holds are two facts, each its own keyed row with its own writer. A
    /// declaration the authority applies, and a start beginning the node's next cycle, put only the
    /// declared fact: neither erases the `PendingReconnect` the mesh primary put for the same birth.
    #[tokio::test]
    async fn a_declaration_and_a_cycle_begin_leave_the_mesh_primarys_silence_status_standing() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut rpc = node("mesh1.rpc.1");
        rpc.mesh = "mesh1".into();
        rpc.kind = NodeKind::RpcNode;
        rpc.endpoint_id = Some(EndpointId("rpc-endpoint".into()));
        rpc.transport_addr = Some("127.0.0.1:1".parse().unwrap());
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), rpc.clone()],
        }));
        let status: Arc<crate::status_storage::MemoryStatusStorage> = Arc::new(Default::default());
        let authority = StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        crate::offline::put_silence_status(&*status, &rpc, Some(crate::model::NodeStatus::PendingReconnect)).await.unwrap();
        let declare = StatusRequest::DeclareNodeState { node_id: rpc.node_id.clone(), incarnation: rpc.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None };
        assert_eq!(authority.apply(Some(rpc.clone()), &declare).await, R::Applied);
        assert_eq!(stored_silence(&*status, &rpc).await, Some(crate::model::NodeStatus::PendingReconnect), "the authority's declared-state put erased the mesh primary's silence status");
        authority.begin_cycle(&rpc, rpc.incarnation_id.as_ref().unwrap()).await;
        assert_eq!(stored_silence(&*status, &rpc).await, Some(crate::model::NodeStatus::PendingReconnect), "a cycle begin erased the mesh primary's silence status");
    }

    /// CONTRACT (i146 Q29, Q36; node-stop.md step 5): a birth's Leaving that names the operation which
    /// stopped it, applied by its mesh-primary, publishes NodeStopped once with that identity, whether
    /// the birth's declaration or the stop stream's `stopped` reached it first; the other finds the
    /// row held (`AlreadyApplied`) and publishes nothing. A Leaving naming no operation (a state the
    /// birth measured itself) and a restart's stop publish nothing. Must NOT happen: NodeStopped twice
    /// for one stop, or a lifecycle frame from a declaration that names no operation.
    #[tokio::test]
    async fn a_stops_leaving_publishes_node_stopped_once_from_whichever_reaches_the_mesh_primary_first() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let rpc = |name: &str| {
            let mut n = node(name);
            n.mesh = "mesh1".into();
            n.kind = NodeKind::RpcNode;
            n
        };
        let (declared_first, stream_first, measured, restarted) = (rpc("mesh1.rpc.1"), rpc("mesh1.rpc.2"), rpc("mesh1.rpc.3"), rpc("mesh1.rpc.4"));
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), declared_first.clone(), stream_first.clone(), measured.clone(), restarted.clone()],
        }));
        let authority = StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let published: Arc<Mutex<Vec<(NodeId, String, u32)>>> = Arc::default();
        let into = published.clone();
        let publisher: StoppedPublisher = Arc::new(move |row: crate::status_storage::NodeDeclaredRow, op: DeclaredOperation| {
            into.lock().unwrap().push((row.node_id, op.operation, op.attempt));
            Box::pin(async {})
        });
        let _ = authority.stopped.set(publisher);
        let stop_of = |n: &crate::model::Node, kind: &str| DeclaredOperation { build_id: "b".into(), attempt: 2, operation: format!("{kind}:{}", n.name) };
        let declare = |n: &crate::model::Node, operation: Option<DeclaredOperation>| StatusRequest::DeclareNodeState { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::Leaving, operation };
        let stream = |n: &crate::model::Node, op: &DeclaredOperation| rdm_core::LifecycleOp { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), name: n.name.clone(), event_at_rafka_ms: 0 };

        // The declaration first, then a repeat of it, then the stop stream's stopped.
        let stop = stop_of(&declared_first, "node.stop");
        assert_eq!(authority.apply(Some(declared_first.clone()), &declare(&declared_first, Some(stop.clone()))).await, R::Applied);
        assert_eq!(authority.apply(Some(declared_first.clone()), &declare(&declared_first, Some(stop.clone()))).await, R::AlreadyApplied, "a repeat of the declaration");
        assert_eq!(authority.stop_received(&stream(&declared_first, &stop)).await, R::AlreadyApplied, "the stop stream's stopped after the declaration");
        // The stop stream's stopped first, then the declaration.
        let stop2 = stop_of(&stream_first, "node.stop");
        assert_eq!(authority.stop_received(&stream(&stream_first, &stop2)).await, R::Applied);
        assert_eq!(authority.apply(Some(stream_first.clone()), &declare(&stream_first, Some(stop2.clone()))).await, R::AlreadyApplied, "the declaration after the stop stream's stopped");
        // Q49: beginning a new cycle never forgets an operation already applied for the birth.
        authority.begin_cycle(&stream_first, stream_first.incarnation_id.as_ref().unwrap()).await;
        let ready = StatusRequest::DeclareNodeState { node_id: stream_first.node_id.clone(), incarnation: stream_first.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None };
        assert_eq!(authority.apply(Some(stream_first.clone()), &ready).await, R::Applied);
        assert_eq!(authority.apply(Some(stream_first.clone()), &declare(&stream_first, Some(stop2.clone()))).await, R::AlreadyApplied, "Q49: the old stop declaration after the next cycle began cannot apply again");
        assert_eq!(authority.stop_received(&stream(&stream_first, &stop2)).await, R::AlreadyApplied, "Q49: the frame names the same applied operation");
        assert_eq!(authority.apply(Some(stream_first.clone()), &declare(&stream_first, None)).await, R::Applied, "Q49: without an operation the normal forward transition still applies");
        // A Leaving the birth measured itself, and a restart's stop: applied, and nothing published.
        assert_eq!(authority.apply(Some(measured.clone()), &declare(&measured, None)).await, R::Applied);
        assert_eq!(authority.apply(Some(restarted.clone()), &declare(&restarted, Some(stop_of(&restarted, "node.restart")))).await, R::Applied);

        assert_eq!(
            *published.lock().unwrap(),
            vec![(declared_first.node_id.clone(), stop.operation.clone(), 2), (stream_first.node_id.clone(), stop2.operation.clone(), 2)],
            "NodeStopped once per stop, from whichever reached the mesh-primary first, and from nothing else"
        );
        let recovered = Declared::rehydrate(&*authority.status_storage).await.unwrap();
        assert_eq!(recovered.operations, authority.declared.lock().unwrap().operations, "Q49: the successor folds every applied operation, including the previous cycle's stop");

    }

    /// A status store whose `Leaving` declared-state put returns only once the `ReadyForTraffic`
    /// put for the same birth has landed: two declarations decided against the same held state,
    /// their puts landing Leaving then ReadyForTraffic.
    #[derive(Default)]
    struct RacingPuts {
        inner: crate::status_storage::MemoryStatusStorage,
        ready_landed: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for RacingPuts {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<std::collections::HashMap<MeshId, MeshState>, crate::record_store::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::record_store::StorageError> { self.inner.put_node_operation(row).await }
        async fn node_operations(&self) -> Result<Vec<crate::status_storage::NodeOperationRow>, crate::record_store::StorageError> { self.inner.node_operations().await }
        async fn remove_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::record_store::StorageError> { self.inner.remove_node_operation(row).await }

        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await?;
            match row.state {
                NodeState::Leaving => self.ready_landed.notified().await,
                NodeState::ReadyForTraffic => self.ready_landed.notify_one(),
                _ => {}
            }
            Ok(())
        }
        async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::record_store::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit_of(mesh, row).await
        }
    }

    /// CONTRACT (#3051, CLAUDE.md read-modify-write ban): two declarations of one birth decided
    /// against the same held state both put their row before either is committed. Whatever order
    /// the puts land in, what a restarted authority folds from the rows is the state it held in
    /// memory: a node's declared state never goes backward across a restart.
    #[tokio::test]
    async fn two_racing_declarations_rehydrate_to_the_state_held_and_never_backward() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut rpc = node("mesh1.rpc.1");
        rpc.mesh = "mesh1".into();
        rpc.kind = NodeKind::RpcNode;
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), rpc.clone()],
        }));
        let status: Arc<RacingPuts> = Arc::new(Default::default());
        let authority = StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let birth = rpc.incarnation_id.clone().unwrap();
        let declare = |state| StatusRequest::DeclareNodeState { node_id: rpc.node_id.clone(), incarnation: birth.clone(), state, operation: None };
        let (leaving, ready) = (declare(NodeState::Leaving), declare(NodeState::ReadyForTraffic));
        let (a, b) = tokio::join!(authority.apply(Some(rpc.clone()), &leaving), authority.apply(Some(rpc.clone()), &ready));
        assert_eq!((a, b), (R::Applied, R::Applied), "both decided forward from the same held state");
        let held = authority.declared.lock().unwrap().node(&rpc.node_id, &birth);
        assert_eq!(held, Some(NodeState::Leaving), "memory holds the greater state");
        let rehydrated = Declared::rehydrate(&*status).await.unwrap();
        assert_eq!(rehydrated.node(&rpc.node_id, &birth), held, "a restarted authority folds the rows back to a state behind the one it held");
    }

    /// CONTRACT (#2803 detection; node-rpc.md §14): a fabric primary probes ANOTHER mesh's node-admin
    /// through a member of that mesh (Forward carries no origin identity). The subject answers
    /// whoever reaches it: the probe is answered with the node-admin's current state from the
    /// carrier, from the node-admin itself and from an endpoint no view holds, and an apply is the
    /// subject's to answer too (here refused by its own rule: apply-node-state drives no state).
    /// What must NOT happen: a call naming this admin left unanswered because of who sent it.
    #[tokio::test]
    async fn a_probe_or_an_apply_naming_this_admin_is_answered_by_it_whoever_sends_it() {
        let admin = node("mesh2.admin.1");
        let carrier = node("mesh2.rpc.1");
        let (node_id, incarnation) = (admin.node_id.clone(), admin.incarnation_id.clone().unwrap());
        let republish = Republish::default();
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        for sender in [Some(&carrier), Some(&admin), None] {
            let answered = self_subject(&admin, sender, None, &probe, &republish, &Configure::default(), &Rename::default(), &Hydrating::default(), &crate::status_declare::DeclareWake::default()).await;
            assert!(matches!(answered, Some(R::Current { .. })), "from {:?}: {answered:?}", sender.map(|n| n.name.to_string()));
        }
        let apply = StatusRequest::ApplyNodeState { node_id, incarnation, state: NodeState::Draining };
        let answered = self_subject(&admin, Some(&carrier), None, &apply, &republish, &Configure::default(), &Rename::default(), &Hydrating::default(), &crate::status_declare::DeclareWake::default()).await;
        assert!(matches!(answered, Some(R::RejectedInvalidNodeTransition { .. })), "the subject answers the apply by its own rule: {answered:?}");
    }

    /// The status authority of the admin born as `me`, deciding over `view`.
    fn authority_of(me: &crate::model::Node, view: Topology) -> StatusAuthority {
        StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            fabric_id: view.fabric.id.clone(),
            topology: Arc::new(tokio::sync::RwLock::new(view)),
            declared: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            status_storage: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Default::default(),
            commands: Default::default(),
            own: Default::default(),
            mesh_deleter: Default::default(),
            hold_next_reply: Default::default(),
            wake: Default::default(),
            rounds: Default::default(),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        }
    }

    /// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"; states.md "Who sets each state": node ReadyForTraffic is set by "the node's
    /// mesh-primary" from "the node's node.started report"): a node-admin's declaration is applied by
    /// its own mesh's primary like every other kind's. mesh2's primary applies mesh2.admin.2's
    /// ReadyForTraffic; the fabric-primary, of mesh1, refuses it naming the seat it needs, the
    /// mesh-primary of mesh2. Must NOT happen: a node-admin's status set by the fabric-primary.
    #[test]
    fn a_node_admins_declaration_is_applied_by_its_own_mesh_primary_never_the_fabric_primary() {
        let admin = |name: &str, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name);
            n.kind = NodeKind::NodeAdmin;
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::ReadyForTraffic;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n
        };
        let fp = admin("mesh1.admin.1", "mesh1", true, true);
        let m2p = admin("mesh2.admin.1", "mesh2", true, false);
        let m2a = admin("mesh2.admin.2", "mesh2", false, false);
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), m2p.clone(), m2a.clone()],
        };
        let declare = StatusRequest::DeclareNodeState { node_id: m2a.node_id.clone(), incarnation: m2a.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None };
        let (reply, effect) = decide(&authority_of(&m2p, view.clone()), &view, &Declared::default(), &declare);
        assert_eq!(reply, R::Applied, "mesh2's primary applies its own mesh's admin's ReadyForTraffic");
        assert!(matches!(effect, Some(Effect::Node(ref row, NodeState::ReadyForTraffic, None)) if row.node_id == m2a.node_id), "{effect:?}");
        let (reply, effect) = decide(&authority_of(&fp, view.clone()), &view, &Declared::default(), &declare);
        assert_eq!(reply, R::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } }, "the fabric-primary of mesh1 is not mesh2.admin.2's authority");
        assert!(effect.is_none());
    }

    /// CONTRACT (R24, Luke Q11: "the same variant at every Status door that can lack a handler for a
    /// request it receives"): the node-admin's declaration decision has no handler for a request its
    /// door answers before the decision (here `fabric.get`). It answers `RequestNotHandled` naming the
    /// request, decided by dispatch alone, on the reject span `rdm.node.status.reject.via-request-not-handled`
    /// naming the request and the admin, and changes nothing. Must NOT happen: an authority refusal or
    /// `NotReady` for a request it has no handler for.
    #[test]
    fn a_request_the_declaration_decision_has_no_handler_for_is_answered_not_handled_by_name() {
        let mut me = node("mesh1.admin.1");
        me.kind = NodeKind::NodeAdmin;
        me.is_primary = true;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![me.clone()],
        };
        let capture = crate::span_capture::Capture::new("i144-r24-admin-door");
        let (reply, effect) = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            decide(&authority_of(&me, view.clone()), &view, &Declared::default(), &StatusRequest::GetFabric)
        };
        assert_eq!(reply, R::RequestNotHandled { request: "fabric.get".into() });
        assert!(effect.is_none(), "{effect:?}");
        let rejected: Vec<(String, String)> = capture.named("rdm.node.status.reject.via-request-not-handled").into_iter().map(|s| (s.attributes["request"].clone(), s.attributes["node"].clone())).collect();
        assert_eq!(rejected, vec![("fabric.get".to_string(), "mesh1.admin.1".to_string())], "the reject span names the request and the admin");
    }

    /// CONTRACT (node-identity-and-state.md 4.3: "Once a node-admin is created in a mesh it is that
    /// mesh's primary ... and writes every status from then on"): the fabric-primary's
    /// ApplyMeshState is applied by the mesh's primary. A receiver whose view names ANOTHER admin
    /// its mesh's primary refuses it naming that holder, and applies nothing; the admin the view
    /// names applies it, and so does an admin of the mesh whose view names no primary of it yet
    /// (the bootstrap admin at the hand-off, before its own seat shows in its view).
    #[test]
    fn an_apply_mesh_state_at_an_admin_whose_view_names_another_primary_is_refused_naming_it() {
        let admin = |name: &str, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name);
            n.kind = NodeKind::NodeAdmin;
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::ReadyForTraffic;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n
        };
        let fp = admin("mesh1.admin.1", "mesh1", true, true);
        let first = admin("mesh2.admin.1", "mesh2", true, false);
        let second = admin("mesh2.admin.2", "mesh2", false, false);
        let view = |nodes: Vec<crate::model::Node>| Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes,
        };
        let seated = view(vec![fp.clone(), first.clone(), second.clone()]);
        let pending = StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh2".into(), state: MeshState::Pending };

        let (reply, effect) = decide(&authority_of(&second, seated.clone()), &seated, &Declared::default(), &pending);
        assert!(
            matches!(&reply, R::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed.contains("mesh2.admin.1")) && effect.is_none(),
            "an admin whose view names mesh2.admin.1 mesh2's primary applies no Mesh status of mesh2: {reply:?}"
        );

        let (reply, effect) = decide(&authority_of(&first, seated.clone()), &seated, &Declared::default(), &pending);
        assert!(matches!(reply, R::Applied) && effect.is_some(), "the primary its view names applies it: {reply:?}");

        let mut unseated_first = first.clone();
        unseated_first.is_primary = false;
        let unseated = view(vec![fp.clone(), unseated_first.clone(), second.clone()]);
        let (reply, effect) = decide(&authority_of(&unseated_first, unseated.clone()), &unseated, &Declared::default(), &pending);
        assert!(matches!(reply, R::Applied) && effect.is_some(), "an admin of the mesh whose view names no primary of it applies it: {reply:?}");
    }

    /// CONTRACT (R-RC1, node-replace.md "Successor | New node.id"): a replace or a recovery keeps the
    /// path.name and mints a new node id. The view holds one row per path.name (`admin::project_at`
    /// keys it by `PathName`), so until the retired birth's digest goes non-live or departs, the row
    /// at this admin's path can be the retired birth's, live and seated, and this admin's own row is
    /// absent. The authority finds itself by the node id it was born with: it decides nothing from
    /// the retired birth's seat and refuses naming the true reason, "a view holding this admin"; a
    /// drain-node naming its node id goes on to the subject's own door (`NodeSelf::serve`, which
    /// checks the exact birth itself), here not yet filled. What must NOT happen: a member's
    /// declaration `Applied` on the retired birth's mesh-primary seat, or a drain-node naming this
    /// admin refused as "not the subject node itself".
    #[tokio::test]
    async fn an_authority_whose_view_holds_its_retired_birth_at_its_path_answers_as_itself() {
        let mut retired = node("mesh1.admin.1");
        retired.kind = NodeKind::NodeAdmin;
        retired.mesh = "mesh1".into();
        retired.status = crate::model::NodeStatus::Draining;
        retired.is_primary = true;
        retired.is_fabric_primary = true;
        let mut successor = node("mesh1.admin.1");
        successor.kind = NodeKind::NodeAdmin;
        successor.mesh = "mesh1".into();
        let mut member = node("mesh1.rpc.1");
        member.kind = NodeKind::RpcNode;
        member.mesh = "mesh1".into();
        member.status = crate::model::NodeStatus::ReadyForTraffic;
        let mut owner = node("mesh1.admin.2");
        owner.kind = NodeKind::NodeAdmin;
        owner.mesh = "mesh1".into();
        owner.status = crate::model::NodeStatus::ReadyForTraffic;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![retired.clone(), member.clone(), owner.clone()],
        };
        assert_ne!(retired.node_id, successor.node_id, "a replace mints a new node id");
        assert_eq!(view.cohort_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.node_id.clone()), Some(retired.node_id.clone()), "the retired birth holds mesh1's seat in this view");
        let auth = authority_of(&successor, view.clone());
        let holding_me = |r: &StatusReply| matches!(r, R::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "a view holding this admin");

        let declare = StatusRequest::DeclareNodeState { node_id: member.node_id.clone(), incarnation: member.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None };
        let (reply, effect) = decide(&auth, &view, &Declared::default(), &declare);
        assert!(holding_me(&reply) && effect.is_none(), "the retired birth's seat is not this admin's: {reply:?}");

        let stop = StatusRequest::JoinMesh { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.mesh.join:mesh1.admin.1".into() };
        let reply = auth.apply(Some(owner.clone()), &stop).await;
        assert!(matches!(&reply, R::NotReady { reason } if reason.contains("has not yet joined its mesh")), "a JoinMesh naming this admin's node id reaches the subject's own door: {reply:?}");
    }

    /// CONTRACT (node-rpc.md §14: "Nothing inside the mesh checks who sent a frame or call: a Node RPC
    /// handler is addressed by target node id + op"): a call naming this admin from an endpoint its view holds no birth for is decided
    /// for the subject it names, as one from an admin it holds is. A drain-node naming this admin's
    /// exact birth reaches the subject's own door (`NodeSelf::serve`, here not yet filled, so
    /// `NotReady` by name), and a probe naming it is answered by the subject. The direction refusals
    /// stay: a command naming another birth of this admin is `RejectedStaleIncarnation`. What must
    /// NOT happen: `SenderNotSubject` "unknown peer", or a probe of the subject answered as if this
    /// admin were not the subject.
    #[tokio::test]
    async fn a_call_naming_this_admin_from_an_endpoint_it_never_heard_of_is_decided_for_the_subject() {
        let mut me = node("mesh1.admin.2");
        me.kind = NodeKind::NodeAdmin;
        me.mesh = "mesh1".into();
        me.status = crate::model::NodeStatus::ReadyForTraffic;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![me.clone()],
        };
        let auth = authority_of(&me, view);
        let incarnation = me.incarnation_id.clone().unwrap();
        let stop = StatusRequest::JoinMesh { node_id: me.node_id.clone(), incarnation: incarnation.clone(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.mesh.join:mesh1.admin.2".into() };
        let reply = auth.apply(None, &stop).await;
        assert!(matches!(&reply, R::NotReady { reason } if reason.contains("has not yet joined its mesh")), "a JoinMesh from a stranger reaches the subject's own door: {reply:?}");
        let probe = StatusRequest::ProbeNodeState { node_id: me.node_id.clone(), incarnation: incarnation.clone() };
        let reply = auth.apply(None, &probe).await;
        assert!(matches!(&reply, R::Current { node_id, state: NodeState::ReadyForTraffic, .. } if *node_id == me.node_id), "a probe from a stranger is answered by the subject: {reply:?}");
        let stale = StatusRequest::JoinMesh { node_id: me.node_id.clone(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 2, operation: "node.mesh.join:mesh1.admin.2".into() };
        assert_eq!(auth.apply(None, &stale).await, R::RejectedStaleIncarnation { held: incarnation });
    }

    /// CONTRACT (node-rpc.md §14; round.rs `fabric_missing`: "a report counts only from the exact
    /// birth that is the mesh's primary now"): a Mesh's `ReadyForTraffic` declaration is decided for
    /// the mesh it names, whoever sends it, and the fabric-primary keeps it as that mesh's report from
    /// the birth that sent it. A later declaration of the same mesh from another birth (a member, an
    /// old primary) never displaces the primary's report, and a declaration from a node of mesh1 is
    /// never kept as mesh1's report. What must NOT happen: the primary's report lost, which holds the
    /// fabric round on `report-of-another-birth` with nothing owed to re-send it.
    #[tokio::test]
    async fn a_mesh_report_is_kept_for_the_mesh_it_names_and_the_birth_that_sent_it() {
        let admin = |name: &str, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name);
            n.kind = NodeKind::NodeAdmin;
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::ReadyForTraffic;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n.endpoint_id = Some(EndpointId(format!("key-{name}")));
            n
        };
        let fp = admin("mesh1.admin.1", "mesh1", true, true);
        let m1_member = admin("mesh1.admin.2", "mesh1", false, false);
        let m2_primary = admin("mesh2.admin.1", "mesh2", true, false);
        let m2_member = admin("mesh2.admin.2", "mesh2", false, false);
        let m2_id = MeshId::mint();
        let mut view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), m1_member.clone(), m2_primary.clone(), m2_member.clone()],
        };
        view.meshes.push(crate::model::Mesh { id: Some(m2_id.clone()), name: "mesh2".into(), status: crate::model::ScopeStatus::ReadyForTraffic });
        let auth = authority_of(&fp, view);
        let ready = StatusRequest::DeclareMeshState { mesh_id: m2_id.clone(), state: MeshState::ReadyForTraffic };
        assert_eq!(auth.apply(Some(m2_primary.clone()), &ready).await, R::Applied);
        assert_eq!(auth.apply(Some(m2_member.clone()), &ready).await, R::AlreadyApplied, "decided for mesh2, whoever sends it");
        assert_eq!(auth.apply(Some(m1_member.clone()), &ready).await, R::AlreadyApplied);
        let held = auth.declared.lock().unwrap().clone();
        assert_eq!(held.report_of("mesh2", Some(&m2_primary)).map(|r| r.incarnation), m2_primary.incarnation_id.clone(), "the primary's report stands: {:?}", held.reports);
        assert!(held.report_of("mesh1", Some(&fp)).is_none(), "a declaration of mesh2 is never mesh1's report: {:?}", held.reports);
        // A mesh's primary this admin's view does not hold yet: its report is kept by its endpoint and
        // counts for its birth once the view holds it.
        let m3_id = MeshId::mint();
        let m3_primary = admin("mesh3.admin.1", "mesh3", true, false);
        auth.topology.write().await.meshes.push(crate::model::Mesh { id: Some(m3_id.clone()), name: "mesh3".into(), status: crate::model::ScopeStatus::Pending });
        let key = iroh::SecretKey::generate().public();
        let mut m3_primary = m3_primary;
        m3_primary.endpoint_id = Some(EndpointId(key.to_string()));
        let ready3 = StatusRequest::DeclareMeshState { mesh_id: m3_id, state: MeshState::ReadyForTraffic };
        assert_eq!(auth.apply_from(Some(key), None, &ready3).await, R::Applied, "decided for mesh3 from an endpoint the view holds no birth for");
        let held = auth.declared.lock().unwrap().clone();
        assert_eq!(held.report_of("mesh3", Some(&m3_primary)).map(|r| r.incarnation), m3_primary.incarnation_id.clone(), "the endpoint's report counts for the birth the view holds at it: {:?}", held.reports);
        assert!(held.report_of("mesh3", Some(&m2_member)).is_none(), "and for no other birth");
    }

    fn digest_of(n: &crate::model::Node, incarnation: IncarnationId) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest {
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode {
                node_id: n.node_id.clone(),
                name: n.name.clone(),
                endpoint_id: EndpointId(format!("key-{}", n.name)),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation,
                supersedes: None,
                runtime: None,
            },
            status: rdm_core::MemberStatus::ReadyForTraffic,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            config: BTreeMap::new(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    /// CONTRACT (#3051, node-recovery.md "Steps": node.delete of the exited instance, then node.create
    /// with a new node.id; node-restart: a new incarnation of the same node.id): a birth's declared
    /// state is a fact about that birth only. Once the book holds a later incarnation of the node, or
    /// holds the node departed (node.deleted), the authority tombstones the old birth's declared-state
    /// keys, blind, and forgets them; a birth the book still holds at the same incarnation (silent or
    /// not) keeps its rows. Must NOT happen: an old birth's rows on disk forever, folded back on every
    /// restart of the authority.
    #[tokio::test]
    async fn a_superseded_or_deleted_birth_leaves_no_declared_state_row_behind() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let member = |name: &str| {
            let mut n = node(name);
            n.mesh = "mesh1".into();
            n.kind = NodeKind::RpcNode;
            n.endpoint_id = Some(EndpointId(format!("{name}-endpoint")));
            n.transport_addr = Some("127.0.0.1:1".parse().unwrap());
            n
        };
        let (restarted, deleted, standing) = (member("mesh1.rpc.1"), member("mesh1.rpc.2"), member("mesh1.rpc.3"));
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), restarted.clone(), deleted.clone(), standing.clone()],
        }));
        let status: Arc<crate::status_storage::MemoryStatusStorage> = Arc::new(Default::default());
        let authority = StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        for n in [&restarted, &deleted, &standing] {
            let declare = StatusRequest::DeclareNodeState { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None };
            assert_eq!(authority.apply(Some(n.clone()), &declare).await, R::Applied, "{}", n.name);
        }
        let book = rdm_transport::membership::DigestBook::default();
        // rpc.1 restarted: the book holds its next incarnation.
        book.record(digest_of(&restarted, IncarnationId::mint()));
        // rpc.2 deleted: its exact birth departed.
        book.record(digest_of(&deleted, deleted.incarnation_id.clone().unwrap()));
        book.depart(rdm_core::LifecycleOp {
            build_id: "build1".into(),
            attempt: 1,
            operation: "node.delete:mesh1.rpc.2".into(),
            node_id: deleted.node_id.clone(),
            incarnation: deleted.incarnation_id.clone().unwrap(),
            name: deleted.name.clone(),
            event_at_rafka_ms: 0,
        });
        // rpc.3 is held at the birth that declared (silent or not, it is not superseded).
        book.record(digest_of(&standing, standing.incarnation_id.clone().unwrap()));
        let capture = crate::span_capture::Capture::new("i143-3051-superseded-rows");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            authority.retire_superseded(&book).await;
        }
        let by_incarnation = capture.named("rdm.node_admin.status.remove.via-new-incarnation");
        let by_deletion = capture.named("rdm.node_admin.status.remove.via-node-deleted");
        for s in by_incarnation.iter().chain(&by_deletion) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        assert_eq!(by_incarnation.iter().map(|s| (s.attributes["node_id"].clone(), s.attributes["outcome"].clone())).collect::<Vec<_>>(), vec![(restarted.node_id.to_string(), "retired".to_string())], "the restarted node's old birth is retired for its new incarnation");
        assert_eq!(by_deletion.iter().map(|s| (s.attributes["node_id"].clone(), s.attributes["outcome"].clone())).collect::<Vec<_>>(), vec![(deleted.node_id.to_string(), "retired".to_string())], "the deleted node's birth is retired for its deletion");
        use crate::status_storage::StatusStorage as _;
        let rows: Vec<String> = status.node_declared().await.unwrap().into_iter().map(|r| r.node_id.to_string()).collect();
        assert_eq!(rows, vec![standing.node_id.to_string()], "only the birth the book still holds keeps its declared-state row");
        let held = authority.declared.lock().unwrap().clone();
        assert_eq!(held.node(&restarted.node_id, restarted.incarnation_id.as_ref().unwrap()), None, "the restarted node's old birth is forgotten");
        assert_eq!(held.node(&deleted.node_id, deleted.incarnation_id.as_ref().unwrap()), None, "the deleted birth is forgotten");
        assert_eq!(held.node(&standing.node_id, standing.incarnation_id.as_ref().unwrap()), Some(NodeState::ReadyForTraffic), "the held birth keeps its state");
        assert_eq!(Declared::rehydrate(&*status).await.unwrap().nodes.len(), 1, "a restarted authority folds only the held birth back");
    }

    /// CONTRACT (node-recovery.md "Steps": "node.create at the same path.name", "New: node.id"): a
    /// recovered birth is superseded by a NEW node at its path.name. Once the book holds that path
    /// under another node_id, the exited birth's declared-state rows are tombstoned (via-new-node),
    /// whether or not the book still holds the exited birth's departure (that is held only
    /// DEPARTED_RETENTION; an admin that rehydrates later holds none).
    /// Must NOT happen: the exited birth's rows folded back for ever because its node_id is gone.
    #[tokio::test]
    async fn a_birth_whose_path_the_book_holds_under_a_new_node_leaves_no_declared_state_row_behind() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut exited = node("mesh1.rpc.1");
        exited.mesh = "mesh1".into();
        exited.kind = NodeKind::RpcNode;
        exited.endpoint_id = Some(EndpointId("mesh1.rpc.1-endpoint".into()));
        exited.transport_addr = Some("127.0.0.1:1".parse().unwrap());
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), exited.clone()],
        }));
        let status: Arc<crate::status_storage::MemoryStatusStorage> = Arc::new(Default::default());
        let authority = StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let declare = StatusRequest::DeclareNodeState { node_id: exited.node_id.clone(), incarnation: exited.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None };
        assert_eq!(authority.apply(Some(exited.clone()), &declare).await, R::Applied);
        // The recovery created a new node at the path; the exited birth's departure is no longer held.
        let mut recovered = exited.clone();
        recovered.node_id = NodeId::mint();
        recovered.incarnation_id = Some(IncarnationId::mint());
        let book = rdm_transport::membership::DigestBook::default();
        book.record(digest_of(&recovered, recovered.incarnation_id.clone().unwrap()));
        let capture = crate::span_capture::Capture::new("i143-3051-recovered-rows");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            authority.retire_superseded(&book).await;
        }
        let by_new_node = capture.named("rdm.node_admin.status.remove.via-new-node");
        for s in &by_new_node {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        assert_eq!(
            by_new_node.iter().map(|s| (s.attributes["node_id"].clone(), s.attributes["held_node_id"].clone(), s.attributes["outcome"].clone())).collect::<Vec<_>>(),
            vec![(exited.node_id.to_string(), recovered.node_id.to_string(), "retired".to_string())],
            "the exited birth is retired for the new node at its path"
        );
        use crate::status_storage::StatusStorage as _;
        assert!(status.node_declared().await.unwrap().is_empty(), "no declared-state row of the exited birth is left");
        assert_eq!(authority.declared.lock().unwrap().node(&exited.node_id, exited.incarnation_id.as_ref().unwrap()), None, "the exited birth is forgotten");
        assert!(Declared::rehydrate(&*status).await.unwrap().nodes.is_empty(), "a restarted authority folds nothing of it back");
    }
}
