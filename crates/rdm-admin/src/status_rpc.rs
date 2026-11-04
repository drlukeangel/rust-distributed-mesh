//! The lifecycle/status declarations node-admin applies as an authority (i143.e6.s7;
//! node-rpc.md §38; node-rpc-envelope.md §3).
//!
//! A declaration reaches this admin over Node RPC (`Status`, op `0x1B`). Nothing inside the mesh
//! checks who is calling (node-rpc.md §14): a declaration is decided for the subject the request
//! names, as this admin's view holds it. The admin applies it only when it holds the role the
//! subject's declaration needs (`RejectedNotPrimary` names the receiver, never the caller),
//! idempotently on the natural key, forward along the legal order, and answers what happened.
//! `Applied` means the declared state is held in this admin's entity state and, for a node,
//! written to `nodes.storage` as the subject's row.
//!
//! Gossip is untouched: the view's `status` stays what the topology view says; what the authority
//! applied is the view's `declared`.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, EndpointId};
use crate::topology::Topology;
use rdm_transport::{PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::status::{transition, DeclaredOperation, FabricEvent, MeshState, NodeState, NotPrimary, Status, StatusReply, StatusRequest, Transition};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

fn request_name(start: bool) -> &'static str { if start { "node.start" } else { "node.stop" } }

fn operation_row(row: &crate::status_rpc::Declaration, operation: &DeclaredOperation) -> crate::status_rpc::OperationEffect {
    crate::status_rpc::OperationEffect { node_id: row.node_id.clone(), incarnation_id: row.incarnation_id.clone(), name: row.name.clone(), state: row.state, operation: operation.clone() }
}

/// One declared state of one birth, as its authority applied it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    /// Kind declared by the exact subject birth.
    pub r#type: String,
    /// Explicit mesh of the subject.
    pub mesh: String,
    /// The node.
    pub node_id: NodeId,
    /// The birth that declared it.
    pub incarnation_id: IncarnationId,
    /// The birth's path and name as it declared: what a recovery's new node at that path supersedes it by.
    pub name: crate::model::PathName,
    /// The state applied.
    pub state: NodeState,
}

/// An operation effect already applied to one exact birth (Q49), held by the admin that applied it,
/// independent of the birth's current cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEffect {
    /// The birth's node.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation_id: IncarnationId,
    /// The path this birth held.
    pub name: crate::model::PathName,
    /// The operation that produced this effect.
    pub operation: DeclaredOperation,
    /// The effect applied.
    pub state: NodeState,
}

impl OperationEffect {
    /// The operation identity: exact birth and operation, never its current state.
    pub fn key(&self) -> OperationEffectKey {
        (self.node_id.clone(), self.incarnation_id.clone(), self.operation.operation.clone())
    }
}

/// Exact birth and structured operation. State is intentionally excluded.
pub type OperationEffectKey = (NodeId, IncarnationId, rdm_core::OperationId);

/// What this admin applied, by natural key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Declared {
    /// Applied operation effects survive every cycle begin; forgotten only with the exact birth.
    pub operations: std::collections::HashMap<OperationEffectKey, crate::status_rpc::OperationEffect>,
    /// Per birth `(node id, incarnation)`: the greatest state it declared.
    pub nodes: std::collections::HashMap<(NodeId, IncarnationId), NodeState>,
    /// Per birth held in `nodes`: the path and name it declared at.
    pub names: std::collections::HashMap<(NodeId, IncarnationId), (crate::model::PathName, String)>,
    /// Per mesh id.
    pub meshes: std::collections::HashMap<MeshId, MeshState>,
    /// Per mesh id held in `meshes`: the Rafka-time its state changed, as the declaration or the
    /// hand-off carried it (node-create.md "A node-admin created into a live mesh" step 4: a successor republishes it unchanged).
    pub mesh_changed_at: std::collections::HashMap<MeshId, u64>,
    /// Per fabric id: the events applied, in order, each once.
    pub fabric: std::collections::HashMap<FabricId, Vec<String>>,
    /// Per mesh name, per reporter: when this admin, as the fabric-primary, last received a
    /// `Ready` declaration of that mesh from that reporter ([`Declared::report_of`] reads
    /// the one that counts). Memory of this process's tenure; it is never stored or rehydrated,
    /// because a report is evidence only while it is fresh.
    pub reports: BTreeMap<String, std::collections::HashMap<Reporter, std::time::Instant>>,
    /// Per birth `(node id, incarnation)` of this admin's mesh: the `node.connections.deleted` it
    /// answered the last `node.connection.delete` this mesh-primary carried to it, in its current
    /// cycle (R-CR2: what a rename of a live birth needs). Memory of this tenure: a successor holds
    /// none, and refuses a live rename until the delete is carried again.
    pub connections_deleted: std::collections::HashMap<(NodeId, IncarnationId), ConnectionsDeletedReceipt>,
    /// Moves each time a state the view's projection reads changes here: a birth's declared state
    /// ([`Self::hold_node`], [`Self::forget_node`]) or a Mesh's ([`Self::hold_mesh`]), or what
    /// [`Self::fold_records`] folds. `nodes` and `meshes` are written through those alone. What the
    /// view rebuild waits on (`crate::admin::ViewWake`); nothing is sent.
    pub moved: DeclaredMoved,
}

/// The watch of [`Declared::moved`]. A copy of [`Declared`] moves the same watch, and two copies equal
/// in what they hold are equal whatever their watches.
#[derive(Debug, Clone)]
pub struct DeclaredMoved(Arc<tokio::sync::watch::Sender<u64>>);

impl Default for DeclaredMoved {
    fn default() -> Self {
        Self(Arc::new(tokio::sync::watch::Sender::new(0)))
    }
}

impl PartialEq for DeclaredMoved {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for DeclaredMoved {}

impl DeclaredMoved {
    fn moved(&self) {
        self.0.send_modify(|v| *v += 1);
    }
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

/// A mesh primary's checklist-complete report, as the fabric-primary received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshReport {
    /// The birth of the node-admin that reported.
    pub birth: rdm_core::framing::Birth,
    /// When this admin received it.
    pub at: std::time::Instant,
}

/// Who sent a Mesh's checklist-complete report, as this admin could name it when it arrived: the birth
/// its view held for the calling endpoint, or only that endpoint when its view held no birth for it
/// yet (nothing inside the mesh checks who is calling, node-rpc.md §14, so such a report is taken).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Reporter {
    /// The exact birth that sent it.
    Birth(rdm_core::framing::Birth),
    /// The transport-proven endpoint that sent it, its birth not yet in this admin's view.
    Endpoint(String),
}

impl Declared {
    /// The state the birth `(node_id, incarnation)` declared, when it declared one.
    pub fn node(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<NodeState> {
        self.nodes.get(&(node_id.clone(), incarnation.clone())).copied()
    }

    /// Hold `state` as what the birth `birth` declared; a change moves [`Self::moved`].
    pub fn hold_node(&mut self, birth: (NodeId, IncarnationId), state: NodeState) {
        if self.nodes.insert(birth, state) != Some(state) {
            self.moved.moved();
        }
    }

    /// Forget what the birth `birth` declared; a state forgotten moves [`Self::moved`].
    pub fn forget_node(&mut self, birth: &(NodeId, IncarnationId)) {
        if self.nodes.remove(birth).is_some() {
            self.moved.moved();
        }
    }

    /// Hold `state` as the Mesh `mesh_id`'s; a change moves [`Self::moved`].
    pub fn hold_mesh(&mut self, mesh_id: MeshId, state: MeshState) {
        if self.meshes.insert(mesh_id, state) != Some(state) {
            self.moved.moved();
        }
    }

    /// [`Self::hold_mesh`], with the Rafka-time the state changed (`at`, 0 when none is known).
    pub fn hold_mesh_at(&mut self, mesh_id: MeshId, state: MeshState, at: u64) {
        if at > 0 {
            self.mesh_changed_at.insert(mesh_id.clone(), at);
        }
        self.hold_mesh(mesh_id, state);
    }

    /// The watch [`Self::moved`] moves on.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.moved.0.subscribe()
    }

    /// Hold `mesh`'s checklist-complete report from `reporter`, received `at`: one per reporter, so a
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
        if let Some((p, birth)) = primary.and_then(|p| p.birth().map(|birth| (p, birth))) {
            let own = by.get(&Reporter::Birth(birth.clone())).or_else(|| p.endpoint_id.as_ref().and_then(|e| by.get(&Reporter::Endpoint(e.0.clone()))));
            if let Some(at) = own {
                return Some(MeshReport { birth, at: *at });
            }
        }
        by.iter()
            .filter_map(|(r, at)| match r {
                Reporter::Birth(i) => Some(MeshReport { birth: i.clone(), at: *at }),
                Reporter::Endpoint(_) => None,
            })
            .max_by_key(|r| r.at)
    }
}

impl Declared {
    /// Fold the Mesh and Fabric records into what this admin applied: the greatest state per Mesh and
    /// each Fabric event once. What this admin already holds stands where it is the greater; nothing is
    /// written back. A node's state is never folded from a record: each node writes its own (Q356) and
    /// owes it again to a new mesh-primary (R-S2), and a stopped node is held stopped from the old
    /// primary's `NodeStopped` (Q260).
    pub fn fold_records(&mut self, meshes: &[crate::storage::MeshRecord], fabric: Option<&crate::fabric_storage::FabricRecord>) {
        let meshes_before = self.meshes.clone();
        for m in meshes {
            let held = self.meshes.entry(m.mesh_id.clone()).or_insert(m.state);
            *held = (*held).max(m.state);
        }
        if let Some(f) = fabric {
            let held = self.fabric.entry(f.fabric_id.clone()).or_default();
            if f.state == crate::fabric_state::FabricState::Ready && !held.iter().any(|e| e == "ready") {
                held.push("ready".into());
            }
        }
        if self.meshes != meshes_before {
            self.moved.moved();
        }
    }
}

/// The status rows of this admin's own mesh, which it folds into what it applied when it takes
/// the mesh's role.
pub struct RoleFold {
    /// The Mesh and Fabric records this admin writes: the copy its create, start and join hydrated, which
    /// it folds when it takes its mesh's role (Q227, Q260).
    caches: crate::record_cache::Caches,
    nodes: Arc<dyn crate::storage::NodesStorage>,
    meshes: Arc<dyn crate::storage::MeshStorage>,
    fabric: Arc<dyn crate::fabric_storage::FabricStorage>,
    mesh: String,
    /// This admin's `(path, name)`: what the fold span names it by, as every status span does.
    name: crate::model::PathName,
    me: NodeId,
    /// Whether this admin holds the role with the rows folded: cleared when a view names another
    /// holder, so each tenure folds once, at its start.
    folded: Mutex<bool>,
}

impl RoleFold {
    /// The facts on `mesh`'s births, on every mesh and on the fabric, folded by the admin `name`
    /// born as `me`.
    pub fn new(caches: crate::record_cache::Caches, nodes: Arc<dyn crate::storage::NodesStorage>, meshes: Arc<dyn crate::storage::MeshStorage>, fabric: Arc<dyn crate::fabric_storage::FabricStorage>, mesh: String, name: crate::model::PathName, me: NodeId) -> Self {
        Self { caches, nodes, meshes, fabric, mesh, name, me, folded: Mutex::new(false) }
    }

    /// The records this admin writes.
    pub fn caches(&self) -> &crate::record_cache::Caches {
        &self.caches
    }

    /// The fabric store this fold reads the fabric's events from.
    pub fn fabric(&self) -> &Arc<dyn crate::fabric_storage::FabricStorage> {
        &self.fabric
    }

    /// The nodes store of this admin's records.
    pub fn nodes(&self) -> &Arc<dyn crate::storage::NodesStorage> {
        &self.nodes
    }

    /// The mesh store the mesh records are written to.
    pub fn meshes(&self) -> &Arc<dyn crate::storage::MeshStorage> {
        &self.meshes
    }

    /// The mesh whose status rows this fold reads.
    pub fn mesh(&self) -> &str {
        &self.mesh
    }

    /// The view `now` is being installed: when it names this admin its mesh's primary and this
    /// tenure has not folded, fold the Mesh and Fabric records into `declared` from this admin's caches,
    /// reading no storage and no node record (Q227, Q260, Q356). Every other admin of the mesh folds none of them: what
    /// it holds is only what it applied itself. Called under the view's write lock, so two installs
    /// never fold at once. A read that fails leaves the tenure unfolded, named in the span.
    ///
    /// The fold is the successor's acquisition of the held member statuses (node-status-heartbeat-
    /// connections.md §A.3), and `digest_view`, this admin's digest view, is what its member projection is
    /// published from: each folded birth of the mesh whose state is ahead of what the view holds for
    /// it is adopted there ([`DigestView::adopt_status`](rdm_transport::topology::DigestView::adopt_status)), before the view naming this admin primary is
    /// installed and so before its first projection (fabric-authority.md §7: hydration, not
    /// transition). One `rdm.node_admin.status.update.via-successor-fold` per birth adopted or refused.
    pub async fn on_install(&self, now: &Topology, declared: &Mutex<Declared>, digest_view: Option<&rdm_transport::topology::DigestView>) {
        let holds = now.is_mesh_primary(&self.mesh, &self.me);
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
        let me_name = digest_view.and_then(|b| b.held_path_name()).or_else(|| now.own(&self.me).map(|n| n.location())).unwrap_or_else(|| self.name.clone());
        let span = tracing::info_span!(
            "rdm.node_admin.status.resolve.via-successor-fold",
            node_path = %me_name.path, node_name = %me_name.name,
            node_id = %self.me,
            mesh = %self.mesh,
            mesh_status_rows = tracing::field::Empty,
            fabric_state = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        // A node-admin taking its mesh's role reads no storage (Q227, Q242, Q260): it folds the Mesh and
        // Fabric records its create, start and join hydrated into its caches, the copy every later write
        // of them starts from. Its members' states are the view's digests and the declarations each owes
        // the new mesh-primary. Storage is read only by a re-born fabric-primary that is the lone node of a
        // single mesh.
        let read = Ok::<_, String>((self.caches.meshes.all(), self.caches.fabric.all().into_iter().next()));
        match read {
            Ok((meshes, fabric)) => {
                declared.lock().unwrap().fold_records(&meshes, fabric.as_ref());
                span.record("mesh_status_rows", meshes.len());
                span.record("fabric_state", fabric.as_ref().map_or("none", |f| f.state.as_str()));
                *self.folded.lock().unwrap() = true;
                span.in_scope(|| tracing::info!("this admin took its mesh's role: the mesh's status rows are folded into what it applied"));
                if let Some(digest_view) = digest_view {
                    span.in_scope(|| self.adopt_into(declared, digest_view, &me_name));
                }
            }
            Err(e) => {
                span.record("error", e.as_str());
                span.in_scope(|| tracing::info!("this admin took its mesh's role and could not read the mesh's status rows: it holds only what it applied"));
            }
        }
    }
}

impl RoleFold {
    /// Adopt into `digest_view` every birth of this admin's mesh that `declared` holds a state for ahead of
    /// what `digest_view` holds for that exact birth: the status its predecessor set, held by the successor.
    fn adopt_into(&self, declared: &Mutex<Declared>, digest_view: &rdm_transport::topology::DigestView, me_name: &crate::model::PathName) {
        let held: Vec<(NodeId, IncarnationId, crate::model::PathName, NodeState)> = {
            let declared = declared.lock().unwrap();
            declared
                .nodes
                .iter()
                .filter(|((node_id, _), _)| *node_id != self.me)
                .filter_map(|((node_id, incarnation), state)| declared.names.get(&(node_id.clone(), incarnation.clone())).filter(|(_, mesh)| *mesh == self.mesh).map(|(n, _)| (node_id.clone(), incarnation.clone(), n.clone(), *state)))
                .collect()
        };
        for (node_id, incarnation, name, state) in held {
            let prior = digest_view.member_status(&node_id, &incarnation);
            if transition(prior.map(declared_of_member), state) != Transition::Apply {
                continue;
            }
            let status = member_of_declared(state);
            let outcome = match set_member_status(digest_view, MemberStatusReceipt::RoleFold { mesh: &self.mesh, node_id: &node_id, incarnation: &incarnation, name: &name, state }) {
                Ok(_) => "adopted",
                Err(why) => why,
            };
            tracing::info_span!(
                "rdm.node_admin.status.update.via-successor-fold",
                node_path = %me_name.path, node_name = %me_name.name,
                member_path = %name.path, member_name = %name.name,
                node_id = %node_id,
                incarnation_id = %incarnation.as_str(),
                status = ?status,
                prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
                outcome,
                "otel.kind" = "internal"
            )
            .in_scope(|| tracing::info!("the role taker holds the status its predecessor set for the member: its projection carries it"));
        }
    }
}

pub use rdm_core::status::{declared_of_member, member_of_declared};

/// What a member's status, set at its mesh-primary, is the receipt of: the frame or declaration that
/// caused it. The mesh-primary is the sole author of member status (node-primary-update.md step 3) and
/// sets it from its member's report (node-status.md "Status report": "A node reports to its mesh's
/// primary, which sets its status"); every such write goes through [`set_member_status`], naming its
/// cause. The rule a cause applies before its write (a forward move of the declared state, a stop not
/// yet completed, a predecessor's state ahead of what the view holds) is its caller's.
#[derive(Debug, Clone, Copy)]
pub enum MemberStatusReceipt<'a> {
    /// The birth's declared state, its row put, acknowledged and applied by this admin's status
    /// authority: the Pending of its accepted join report, the Ready of its node.started,
    /// the Stopped of its stop, or the state its `DeclareNodeState` names.
    Declared(&'a crate::status_rpc::Declaration),
    /// `NodeJoined` on the start's command stream (`node.mesh.joined`): the birth's stop, if one is
    /// held, ends, and the birth is held Pending; or, when `already` names the state its declared state
    /// holds in its new cycle (that Pending, or the Ready it declared since), held with that
    /// state, never set back to Pending.
    NodeJoined {
        /// The start's operation on the exact birth.
        op: &'a rdm_core::LifecycleOp,
        /// The state the birth's declared state already holds in its new cycle, when it holds one.
        already: Option<NodeState>,
    },
    /// `CommandAccepted` on the stop's command stream: the stop's admission sets the birth stopping
    /// (`Leaving`).
    StopAdmitted(&'a rdm_core::LifecycleOp),
    /// `ConnectionsDeleting` on `node.connection.delete`'s command stream: the node's report.
    ConnectionsDeleting {
        /// The node.
        node_id: &'a NodeId,
        /// Its exact birth.
        incarnation: &'a IncarnationId,
    },
    /// The role fold: the state its predecessor set for the birth, as the mesh's status rows hold it,
    /// adopted by the admin that took the mesh's role (fabric-authority.md §7: hydration, not
    /// transition).
    RoleFold {
        /// Explicit mesh of the subject.
        mesh: &'a str,
        /// The node.
        node_id: &'a NodeId,
        /// Its exact birth.
        incarnation: &'a IncarnationId,
        /// The path and name the birth declared at.
        name: &'a crate::model::PathName,
        /// The state the status rows hold for the birth.
        state: NodeState,
    },
}

/// Set a member's status at its mesh-primary in `digest_view`, the digest view its member projection is
/// published from, from `receipt`, its cause: the one place this admin, as its members' status
/// authority, writes a member's status. `Ok(true)` when the view holds the status for the birth;
/// `Ok(false)` when it sets nothing (a birth of another mesh, or one the view holds nothing of);
/// `Err` naming why a role fold adopts nothing. The view's own span of each cause is its record.
pub fn set_member_status(digest_view: &rdm_transport::topology::DigestView, receipt: MemberStatusReceipt<'_>) -> Result<bool, &'static str> {
    use rdm_core::MemberStatus as M;
    match receipt {
        MemberStatusReceipt::Declared(row) => Ok(match row.state {
            NodeState::Ready => digest_view.started(&row.node_id, &row.incarnation_id, &row.name, &row.mesh),
            state => digest_view.reported(&row.node_id, &row.incarnation_id, member_of_declared(state)),
        }),
        MemberStatusReceipt::NodeJoined { op, already: Some(state) } => Ok(digest_view.joined_already_applied(op, member_of_declared(state))),
        MemberStatusReceipt::NodeJoined { op, already: None } => Ok(digest_view.joined(op)),
        MemberStatusReceipt::StopAdmitted(op) => Ok(digest_view.admitted(op, M::Leaving)),
        MemberStatusReceipt::ConnectionsDeleting { node_id, incarnation } => Ok(digest_view.reported(node_id, incarnation, M::ConnectionsDeleting)),
        MemberStatusReceipt::RoleFold { mesh, node_id, incarnation, state, .. } => digest_view.adopt_status(node_id, incarnation, mesh, member_of_declared(state)).map(|_| true),
    }
}

/// What the service reads and writes: this admin's identity, its current view, what it applied,
/// and the rows it writes.
pub struct StatusService {
    /// This admin's topology view.
    pub topology_view: Option<rdm_transport::topology::TopologyView>,
    /// This admin's `(path, name)`: what it is named by in spans and refusals.
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
    /// The node id this admin was born with: how it finds its own row and its roles in a view
    /// ([`Topology::own`]), never by `(path, name)`.
    pub node_id: NodeId,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// This admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// What this admin applied.
    pub declared: Arc<Mutex<Declared>>,
    /// The nodes store: the node records a config update writes (`UpdateNodeConfig`), carrying the
    /// facts on each birth an applied declaration writes before it is answered.
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// The records this admin writes, as it holds them: every write of a node, mesh or Fabric record
    /// starts from its copy here (storage-whole-records.md, "The rule").
    pub caches: crate::record_cache::Caches,
    /// The mesh store: an applied Mesh status is a fact on its mesh, put before it is answered.
    pub meshes_storage: Arc<dyn crate::storage::MeshStorage>,
    /// The fabric store: the fabric's events, put by the fabric-primary before it answers and read by
    /// every other mesh-primary that applies them.
    pub fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage>,
    /// The mesh ids this admin holds, by mesh name.
    pub mesh_ids: Arc<dyn Fn() -> BTreeMap<String, MeshId> + Send + Sync>,
    /// The births this admin's offline tick watches through durable rows alone (`durable_births`: a
    /// birth of a planned path its view holds nothing at, one born while this admin was silent), as its
    /// last tick held them (`Records::durable`), none unless this admin is its mesh's primary (the rows
    /// read once as it takes the role, `TenureRows`): a declaration for one is decided as for a birth
    /// the view holds (Q11).
    pub durable: Arc<Mutex<Vec<crate::model::Node>>>,
    /// The commands this admin sent (as an owning mesh-admin) and awaits a completion call for.
    pub commands: Arc<crate::node_commands::CommandView>,
    /// This admin as the subject of `node.connection.delete` / `stop-node` from its own mesh-admin; filled once
    /// its digest and server exist.
    pub own: Arc<OnceLock<Arc<crate::node_self::NodeSelf>>>,
    /// What this admin signs with: the issuing key `node.issuer.update` installs, and the rafka-time
    /// `fabric.get` judges a stalled renewal on. `None` for a `StatusService` that holds no certs (a
    /// testkit admin side).
    pub certs: Option<Arc<crate::certs::CertIssuer>>,
    /// What serves `DeleteMesh` (as a leaving mesh's primary) and `MeshDeleted` (as the
    /// fabric-primary); filled once the runner exists.
    pub mesh_deleter: crate::mesh_delete::MeshDeleterSlot,
    /// The node commands this admin runs as the primary above a node, for a caller (D40; Q342).
    pub primary_commands: crate::node_commands::PrimaryCommandsSlot,
    /// Testkit knob: hold the next reply past the caller's bound after applying (acceptance 2:
    /// apply + reply loss is `Indeterminate`, and the retry is `AlreadyApplied`).
    pub hold_next_reply: Arc<std::sync::atomic::AtomicBool>,
    /// This admin's storage changes (storage.md §2 "Changing it"): the one it runs as fabric-primary,
    /// requested by `fabric.storage.update`, and the `mesh.block` and `mesh.unblock` it serves as a
    /// mesh-primary. Set once at its boot.
    pub storage_changes: Arc<OnceLock<Arc<crate::storage_change::StorageChanges>>>,
    /// Wakes this admin's declarer: a view this `StatusService` changed, and a down op's receipt.
    pub wake: Arc<crate::status_declare::DeclareWake>,
    /// This admin's fabric states; filled once the admin holds the handles they act through.
    pub fabric_states: Arc<OnceLock<Arc<crate::fabric_status::FabricStates>>>,
    /// This admin's atlas (fabric-atlas.md, Q224), the one its control plane's Build acceptance reads:
    /// what `fabric.atlas.get` answers.
    pub atlas: Arc<OnceLock<Arc<crate::atlas::Atlas>>>,
    /// Fabric recovery in a view (Q122): while this admin runs it and the target mesh has no mesh-primary,
    /// it takes the declarations of the node re-created at the target's path and name, which its own node.create
    /// waits on (build.md §5.3).
    pub fabric_recovery: Arc<OnceLock<crate::fabric_recovery::Reader>>,
    /// This admin's `NodeConfigs`, served while it holds a mesh's primary role (the writer
    /// of that mesh's rows); filled once the admin's node-RPC client exists.
    pub configs: crate::node_config::NodeConfigsSlot,
    /// This admin's re-publish of its presence on the channel it is on (its digest published, no
    /// re-join), filled once its digest exists: the check-in a probed birth owes (states.md "The
    /// check-in checklist"), what the checklist's down op reads.
    pub republish: Republish,
    /// Where this admin's own `pre_node_ready` stands (the cell its start writes): while it answered
    /// `Pending` a probe naming this admin is answered `Pending` with its reason, and once its start
    /// ended Failed `StartFailed`.
    pub pre_node_ready: PreNodeReadyCell,
    /// This admin as the subject of `ApplyNodeName`: it takes the name holding no connection (R-CR1)
    /// and publishes its digest carrying it; filled once its digest exists.
    pub rename: Rename,
    /// This admin's view taking a node's row its `node.name.update` moved (R13): the view is projected
    /// again, reading the moved row. Filled with the view.
    pub row_moved: RowMoved,
    /// This admin's command of `node.mesh.join` to a renamed live birth of its mesh (node-name-update.md
    /// step 6; [`crate::heard_again::HeardAgain::rejoin`]).
    pub rejoin: Rejoin,
    /// Each member's load and gossip stats, as its latest digest carries them: what `node.get`
    /// answers beside the view. `None` only where no admin owns a digest view; a view read there is
    /// refused `NotReady` by name.
    pub loads: Option<crate::views::Loads>,
    /// This admin's digest view: as its mesh's primary it sets each member's status there through
    /// [`set_member_status`], from the frame or declaration that caused it. `None` only where no admin
    /// owns a digest view.
    pub digest_view: Option<rdm_transport::topology::DigestView>,
    /// The births this admin, as their mesh-primary, marks `PendingReconnect` at once for its tick
    /// (node-recovery.md step 1): a `ReportCallFailed` it takes marks there (node-rpc.md §43).
    pub unreachable: crate::offline::UnreachableMarks,
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
    Node(crate::status_rpc::Declaration, NodeState, Option<DeclaredOperation>),
    /// A mesh and the state applied to it, with the mesh's name when the request carried it, and
    /// the Rafka-time of the change (0 when none was carried).
    Mesh(MeshId, MeshState, Option<String>, u64),
    /// A fabric event applied.
    Fabric(FabricId, String),
}

/// The decision, pure over a view and what was applied: what the admin does with a declaration.
/// It is decided for the subject the request names, never for its caller (node-rpc.md §14). It
/// reads `declared` and returns the effect to commit; it writes nothing.
pub fn decide(service: &StatusService, view: &Topology, declared: &Declared, req: &StatusRequest) -> (StatusReply, Option<Effect>) {
    let me = match view.own(&service.node_id) {
        Some(n) => n,
        None => return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "a view holding this admin".into() }), None),
    };
    let me_name = service.held_name_in(view);
    match req {
        StatusRequest::DeclareNodeState { node_id, incarnation, state, operation } => {
            // The role first: an admin that is no mesh-primary, and runs no fabric recovery, answers that
            // it is not the primary, whatever it holds of the subject (node-status.md "Status report").
            let recovering = service.fabric_recovery.get().is_some_and(|read| read(view).is_some_and(|fr| service.running_birth(view).is_some_and(|birth| crate::fabric_recovery::executes_for(Some(read), view, &birth, &fr.target.mesh))));
            // The mesh-primary role this admin installed (fabric-recovery.md, the restore: "the node installs
            // both roles, acknowledges directly"; node.primary.update's receiver): its primary record names
            // this admin's exact birth, and it is that role's primary for declarations at once, before its own
            // election's recompute names it. Who wins the election does not move.
            let installed = service.digest_view.as_ref().and_then(|v| v.primaries.mesh(&me.mesh)).is_some_and(|p| p.node_id == me.node_id && me.incarnation_id.as_ref() == Some(&p.incarnation));
            let is_primary = me.is_primary || installed;
            if !is_primary && !recovering {
                let mesh = view.births().find(|n| n.node_id == *node_id).map_or(me.mesh.as_str(), |n| n.mesh.as_str());
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("mesh-primary of {mesh}") }), None);
            }
            // The subject, as this admin's view holds it, else as its offline tick watches it through
            // durable rows alone (a birth born while this admin was silent, Q11): this admin is its
            // mesh-primary all the same, and its view takes the birth in once its digest is installed.
            let watched;
            let subject = match view.births().find(|n| n.node_id == *node_id) {
                Some(n) => n,
                None => {
                    watched = service.durable.lock().unwrap().iter().find(|n| n.node_id == *node_id).cloned();
                    match watched.as_ref() {
                        Some(n) => n,
                        None => return (rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown), None),
                    }
                }
            };
            // The receiver's role: every type, a node-admin included, reports to its mesh's primary,
            // which sets its status (node-status.md "Status report"). While fabric recovery runs and the
            // mesh has no mesh-primary, its executor takes the declarations of the node it re-creates at
            // the target's path and name: its own node.create waits on them (build.md §5.3, Q122).
            let recovers = service.fabric_recovery.get().is_some_and(|read| service.running_birth(view).is_some_and(|birth| crate::fabric_recovery::executes_for(Some(read), view, &birth, &subject.mesh)) && read(view).is_some_and(|fr| fr.at.as_ref() == Some(&subject.location())));
            if !(is_primary && me.mesh == subject.mesh) && !recovers {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("mesh-primary of {}", subject.mesh) }), None);
            }
            // The exact birth: a declaration for an incarnation the view no longer holds is stale.
            match subject.incarnation_id.as_ref() {
                Some(held) if held == incarnation => {}
                Some(held) => return (rdm_core::reply::stale(held.clone()), None),
                None => return (rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown), None),
            }
            // A birth deleted (its delete completed, as this admin's atlas holds it) is not held, whatever
            // the view still shows before the departure lands: refused `SubjectUnknown` and decided as
            // every refused caller is, deleted and never re-admitted (node-investigation.md; Q207, Q217),
            // as the `JoinService` refuses a record `Deleted`.
            if service.atlas.get().is_some_and(|a| a.holds_deleted_by_delete(&subject.node_id, incarnation, None)) {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown), None);
            }
            if let Some(operation) = operation {
                let row = crate::status_rpc::Declaration { r#type: subject.r#type.clone(), mesh: subject.mesh.clone(), node_id: subject.node_id.clone(), incarnation_id: incarnation.clone(), name: subject.location(), state: *state };
                if declared.operations.contains_key(&operation_row(&row, operation).key()) {
                    return (rdm_core::reply::already_applied(), None);
                }
            }
            let current = declared.node(&subject.node_id, incarnation);
            match transition(current, *state) {
                Transition::AlreadyApplied if operation.is_some() => {
                    let row = crate::status_rpc::Declaration { r#type: subject.r#type.clone(), mesh: subject.mesh.clone(), node_id: subject.node_id.clone(), incarnation_id: incarnation.clone(), name: subject.location(), state: *state };
                    (rdm_core::reply::already_applied(), Some(Effect::Node(row, *state, operation.clone())))
                }
                Transition::AlreadyApplied => (rdm_core::reply::already_applied(), None),
                Transition::Backward { current } => (rdm_core::reply::status::rejected_invalid_node_transition(current), None),
                Transition::Apply => {
                    let row = crate::status_rpc::Declaration { r#type: subject.r#type.clone(), mesh: subject.mesh.clone(), node_id: subject.node_id.clone(), incarnation_id: incarnation.clone(), name: subject.location(), state: *state };
                    (rdm_core::reply::applied(), Some(Effect::Node(row, *state, operation.clone())))
                }
            }
        }
        StatusRequest::DeclareMeshState { mesh_id, state, changed_at_rafka_ms } => {
            if !me.is_fabric_primary {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "fabric-primary".into() }), None);
            }
            // The subject Mesh, as this holder knows it: an id its own Build created, else the id a
            // Mesh carries in the view. The fabric role moves to whichever admin has the lowest node
            // id, which may hold a Mesh it never created.
            let held = (service.mesh_ids)().values().any(|id| id == mesh_id) || view.meshes.iter().any(|m| m.id.as_ref() == Some(mesh_id));
            if !held {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown), None);
            }
            // The fabric-primary decides; the mesh-primary records the state on its own Mesh record once
            // it is answered, and owes the declaration until that write lands (Q144): a repeated
            // declaration is answered `Applied` again, so a successor that declares again writes it.
            match apply_mesh(declared, mesh_id, *state, *changed_at_rafka_ms) {
                (StatusReply::AlreadyApplied, _) => (rdm_core::reply::applied(), None),
                decided => decided,
            }
        }
        // Downward exact-node operations are answered by the birth they reach (`self_subject`), which
        // this admin answers as once its view holds it.
        StatusRequest::ApplyNodeState { .. } | StatusRequest::ProbeNodeState { .. } | StatusRequest::ApplyNodeConfig { .. } | StatusRequest::ApplyNodeProperties { .. } | StatusRequest::ApplyNodeName { .. } => {
            (rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} holds no view of itself yet: {} is answered by the birth it reaches once its view holds it", req.op(), me_name_path = &me_name.path, me_name_name = &me_name.name)), None)
        }
        // Commands, completions and reads are answered before the decision (`StatusAuthority::apply`):
        // the decision has no handler for them, so not handled here (R24), decided by dispatch alone.
        StatusRequest::Reserved6 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::Reserved12 { .. } | StatusRequest::Reserved13 { .. } | StatusRequest::Reserved14 { .. } | StatusRequest::Reserved15 { .. } | StatusRequest::NodeLeft { .. } | StatusRequest::DeleteMesh { .. } | StatusRequest::MeshDeleted { .. } | StatusRequest::UpdateNodeConfig { .. } | StatusRequest::UpdateRoots { .. } | StatusRequest::UpdateRoot { .. } | StatusRequest::UpdateName { .. } | StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. } | StatusRequest::Reserved21 { .. } | StatusRequest::GetNodes { .. } | StatusRequest::GetMeshes { .. } | StatusRequest::GetFabric | StatusRequest::GetAtlas | StatusRequest::GetConnections { .. } | StatusRequest::Get { .. } | StatusRequest::JoinMesh { .. } | StatusRequest::Reserved29 { .. } | StatusRequest::RenewCert { .. } | StatusRequest::UpdateIssuer { .. } | StatusRequest::Reserved34 { .. } | StatusRequest::ReportCallFailed { .. } | StatusRequest::UpdateStorage { .. } => {
            (crate::node_self::request_not_handled(service.node_id.as_str(), req), None)
        }
        StatusRequest::ApplyMeshState { mesh_id, mesh_name, state, changed_at_rafka_ms } => {
            if me.mesh != *mesh_name {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("an admin of {mesh_name}") }), None);
            }
            // The mesh's primary writes its status (node-identity-and-state.md 4.3): an admin whose view
            // names another admin the mesh's primary is not the receiver. One whose view names none
            // yet is the admin the hand-off created, before its own role shows in its view.
            if let Some(primary) = view.node_type_primary(mesh_name, "node-admin").filter(|p| p.node_id != service.node_id) {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("the mesh-primary of {mesh_name}, which this admin's view names node {}", primary.node_id) }), None);
            }
            // Never a replacement mesh id to satisfy the operation.
            if let Some(held) = (service.mesh_ids)().get(mesh_name) {
                if held != mesh_id {
                    return (rdm_core::reply::status::rejected_stale_mesh(held.clone()), None);
                }
            }
            let (decided, effect) = apply_mesh(declared, mesh_id, *state, *changed_at_rafka_ms);
            (decided, effect.map(|e| match e {
                Effect::Mesh(id, state, _, at) => Effect::Mesh(id, state, Some(mesh_name.clone()), at),
                other => other,
            }))
        }
        StatusRequest::ApplyFabricEvent { fabric_id, event } => {
            if !me.is_primary {
                return (rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "mesh-primary".into() }), None);
            }
            if *fabric_id != service.fabric_id {
                return (rdm_core::reply::status::rejected_stale_fabric(service.fabric_id.clone()), None);
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
        FabricEvent::Ready => "ready".into(),
    }
}

/// The Fabric state the Fabric event `event` moves the record to: the one event, `ready`, is its move
/// to ready.
fn fabric_state_of(event: &str) -> Option<crate::fabric_state::FabricState> {
    (event == "ready").then_some(crate::fabric_state::FabricState::Ready)
}

/// One commit of a Fabric event by its writer, the fabric-primary (states.md, "Who sets each state"):
/// the record's state moved to the one the event names on the Fabric record it holds, the record
/// written whole and acknowledged, then the event held as applied once and answered (`Applied` the
/// first time, `AlreadyApplied` after). A refused write answers `NotReady` naming the store and holds
/// nothing, so the same commit again is decided afresh. The fabric-primary's own ready event, written
/// as it enters ready (`crate::fabric_status`), and the `ApplyFabricEvent` it sends its own mesh commit
/// here alike.
pub(crate) async fn commit_fabric_event(storage: &dyn crate::fabric_storage::FabricStorage, caches: &crate::record_cache::Caches, declared: &Mutex<Declared>, fabric_id: &FabricId, name: Option<&str>, event: &str) -> StatusReply {
    let Some(state) = fabric_state_of(event) else {
        return rdm_core::reply::status::not_ready(format!("the Fabric event {event} names no state of the Fabric record"));
    };
    let base = name.map(|name| crate::fabric_storage::FabricRecord::new(crate::fabric_storage::FabricIdentity { fabric_id: fabric_id.clone(), name: name.to_string() }));
    let written = caches.update_fabric(storage, fabric_id, base, |r| {
        r.state = r.state.max(state);
    });
    match written.await {
        Ok(Some(())) => hold_fabric_event(declared, fabric_id, event),
        Ok(None) => rdm_core::reply::status::not_ready(format!("fabric.storage holds no Fabric record of {fabric_id} for the event {event} to be applied to")),
        Err(e) => rdm_core::reply::status::not_ready(e),
    }
}

/// An `ApplyFabricEvent` at a mesh-primary that is not the fabric-primary: the event is the
/// fabric-primary's to write, and is held as applied once the Fabric record holds it (a point read of
/// the record); until then the answer is `NotReady` naming the store, and nothing is held or written.
pub(crate) async fn hold_written_fabric_event(storage: &dyn crate::fabric_storage::FabricStorage, declared: &Mutex<Declared>, fabric_id: &FabricId, event: &str) -> StatusReply {
    match storage.fabric().await {
        Ok(Some(r)) if r.fabric_id == *fabric_id && fabric_state_of(event).is_some_and(|s| r.state >= s) => hold_fabric_event(declared, fabric_id, event),
        Ok(_) => rdm_core::reply::status::not_ready(format!("fabric.storage does not hold the Fabric event {event} its writer, the fabric-primary, writes first")),
        Err(e) => rdm_core::reply::status::not_ready(format!("fabric.storage could not be read for the Fabric event {event}: {e}")),
    }
}

/// The Fabric event `event` held as applied: `Applied` the first time, `AlreadyApplied` after.
fn hold_fabric_event(declared: &Mutex<Declared>, fabric_id: &FabricId, event: &str) -> StatusReply {
    let mut declared = declared.lock().unwrap();
    let events = declared.fabric.entry(fabric_id.clone()).or_default();
    if events.iter().any(|e| e == event) {
        rdm_core::reply::already_applied()
    } else {
        events.push(event.to_string());
        rdm_core::reply::applied()
    }
}

/// One commit of a Mesh status at its writer: decided over what is held, the state applied to the
/// mesh's record it holds (`base`, the mesh's identity, when it holds none), the record written whole
/// and acknowledged, then held as applied and answered. A refused write answers `NotReady` naming the
/// store and holds nothing, so the same commit again is decided afresh; the commit decides again over
/// what is held after the write, so two commits of one state racing answer `Applied` once.
pub(crate) async fn commit_mesh_state(storage: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &Mutex<Declared>, mesh: &str, mesh_id: &MeshId, state: MeshState, at: u64) -> StatusReply {
    let (decided, effect) = apply_mesh(&declared.lock().unwrap(), mesh_id, state, at);
    if effect.is_none() {
        return decided;
    }
    // The record's move to deleted is its removal, written by `mesh_delete::mesh_row_deleted` alone:
    // writing the deleted record frees the name (R-MN1 item 2); every other state is written here.
    if state != MeshState::Deleted {
        let base = crate::storage::MeshRecord::new(mesh_id.clone(), mesh);
        if let Err(e) = caches.update_mesh(storage, mesh_id, Some(base), |r| { r.state = r.state.max(state); }).await {
            return rdm_core::reply::status::not_ready(format!("{e} (the Mesh status {state:?} of {mesh})"));
        }
    }
    let mut declared = declared.lock().unwrap();
    match apply_mesh(&declared, mesh_id, state, at) {
        (StatusReply::Applied, _) => {
            declared.hold_mesh_at(mesh_id.clone(), state, at);
            rdm_core::reply::applied()
        }
        (other, _) => other,
    }
}

/// The held Mesh status word of the Mesh lifecycle state `state` (states.md: pending -> ready ->
/// leaving -> dead).
fn mesh_status_name(state: MeshState) -> &'static str {
    match state {
        MeshState::Pending => "pending",
        MeshState::Ready => "ready",
        MeshState::Deleting => "leaving",
        MeshState::Deleted => "dead",
    }
}

fn apply_mesh(declared: &Declared, mesh_id: &MeshId, state: MeshState, at: u64) -> (StatusReply, Option<Effect>) {
    match transition(declared.meshes.get(mesh_id).copied(), state) {
        Transition::AlreadyApplied => (rdm_core::reply::already_applied(), None),
        Transition::Backward { current } => (rdm_core::reply::status::rejected_invalid_mesh_transition(current), None),
        Transition::Apply => (rdm_core::reply::applied(), Some(Effect::Mesh(mesh_id.clone(), state, None, at))),
    }
}

impl StatusService {
    /// The `(path, name)` this admin holds now: what its digest view or its view names its birth,
    /// falling back to `self.me` before its birth is in either.
    pub async fn held_name(&self) -> crate::model::PathName {
        if let Some(held) = self.digest_view.as_ref().and_then(|b| b.held_path_name()) {
            return held;
        }
        self.topology.read().await.own(&self.node_id).map(|n| n.location()).unwrap_or_else(|| self.me.clone())
    }

    /// The birth this admin runs as, as its view `view` holds it.
    fn running_birth(&self, view: &Topology) -> Option<crate::storage::BirthRef> {
        view.own(&self.node_id).and_then(|n| Some(crate::storage::BirthRef { node_id: n.node_id.clone(), incarnation_id: n.incarnation_id.clone()? }))
    }

    fn held_name_in(&self, view: &Topology) -> crate::model::PathName {
        self.digest_view.as_ref().and_then(|b| b.held_path_name()).or_else(|| view.own(&self.node_id).map(|n| n.location())).unwrap_or_else(|| self.me.clone())
    }

    /// The node a call's authenticated endpoint is, from the view this authority decides from: a
    /// birth the view holds, or `None` for an endpoint it holds no birth for. It names the caller in
    /// spans and is where a reply owed later goes (a connections delete's completion, a leave's handoff); it
    /// decides nothing (node-rpc.md §14): a call from an endpoint the view does not hold is decided
    /// as any other.
    pub async fn sender_of(&self, peer: &EndpointId) -> Option<crate::model::Node> {
        self.topology.read().await.births().find(|n| n.endpoint_id.as_ref() == Some(peer)).cloned()
    }

    /// The one `StatusService` call every declaration goes through, whoever the sender is: a peer over Node RPC,
    /// or this admin itself when its view names it as the authority. Decide over the view; an
    /// `Applied` node state is durable (the subject's row) before it is answered, and the live
    /// view shows it at once; one span per decision names op, sender, outcome and whether the
    /// receiver holds its mesh's role (accepting a Pending hand-off is not an election).
    pub async fn apply(&self, sender: Option<crate::model::Node>, req: &StatusRequest) -> StatusReply {
        let peer = sender.as_ref().and_then(|n| n.endpoint_id.as_ref()).and_then(|e| e.0.parse::<iroh::PublicKey>().ok());
        self.apply_from(peer, sender, req).await
    }

    /// Persist this admin's own config at its exact node ID through its shared property writer.
    pub async fn own_config_update(&self, incarnation: crate::model::IncarnationId, config: std::collections::BTreeMap<String, String>) -> StatusReply {
        let view = self.topology.read().await.clone();
        let me = self.held_name_in(&view);
        let Some(subject) = view.own(&self.node_id) else {
            return rdm_core::reply::status::not_ready(format!("path {me_path:?}, name {me_name:?} holds no own birth yet", me_path = &me.path, me_name = &me.name));
        };
        if subject.incarnation_id.as_ref() != Some(&incarnation) {
            return rdm_core::reply::stale(subject.incarnation_id.clone().unwrap_or(incarnation));
        }
        let result = match self.configs.get() {
            Some(configs) => configs.own_config(subject, config).await,
            None => None,
        };
        match result {
            Some(Ok(reply)) => reply,
            Some(Err(error)) => rdm_core::reply::status::bounded_config_row_refused(self.node_id.as_str(), &error.to_string()),
            None => rdm_core::reply::status::not_ready(format!("path {me_path:?}, name {me_name:?} has no own property writer yet", me_path = &me.path, me_name = &me.name)),
        }
    }



    /// `node.name.update` (`UpdateName`) at this admin: the one validator and writer of a rename.
    pub async fn update_name(&self, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateName { node_id, incarnation, .. } = req else { unreachable!("update_name is called for UpdateName only") };
        let me = self.held_name().await;
        // The mesh-primary writes the record through its `NodeConfigs`, the one place its record changes take turns at.
        let deleted = self.declared.lock().unwrap().connections_deleted.get(&(node_id.clone(), incarnation.clone())).cloned();
        let service = crate::node_name_update::NodeNameUpdateService { configs: self.configs.get().map(|c| &**c), moved: &self.row_moved, rename: &self.rename, rejoin: &self.rejoin };
        crate::node_name_update::update(&me, &self.node_id, &self.topology, &*self.nodes_storage, service, deleted.as_ref(), req).await
    }

    /// `node.config.update` (`UpdateNodeConfig`) of a Build's create spec at this admin, in-process
    /// (build.md §1, "The Build applies the diff"): the one config door.
    pub async fn update_config_from_spec(&self, req: &StatusRequest) -> StatusReply {
        let me = self.held_name().await;
        let view = self.topology.read().await.clone();
        match self.configs.get() {
            Some(configs) => configs.update(&me, &self.node_id, &view, &*self.nodes_storage, req).await,
            None => rdm_core::reply::status::not_ready(format!("path {me_path:?}, name {me_name:?} has no node-RPC client yet; {} is refused", req.op(), me_path = &me.path, me_name = &me.name)),
        }
    }

    /// A Build's entry follows the same downward route and node writer as caller changes.
    pub async fn apply_build_entry(&self, birth: &crate::build::PinnedBirth, entry: rdm_core::properties::EntryProperties) -> Result<(), String> {
        self.commands.admitted().await;
        let view = self.topology.read().await.clone();
        let me = self.held_name_in(&view);
        self.configs.get().ok_or_else(|| format!("path {me_path:?}, name {me_name:?}: node property client is not bound", me_path = &me.path, me_name = &me.name))?
            .apply_entry(&me, &self.node_id, &view, birth, entry).await
    }

    /// Close one exact birth's own-property admission before the provider copy. Silence or
    /// an unanswered command is never a successful block.
    pub async fn carry_storage_block(&self, subject: &crate::model::Node, change_id: &rdm_core::ChangeId) -> Result<(), String> {
        use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
        let incarnation = subject.incarnation_id.as_ref().ok_or_else(|| format!("{} has no exact incarnation to block", subject.node_id))?;
        let result = if subject.node_id == self.node_id {
            let moves = self.own.get().and_then(|own| own.storage_moves())
                .ok_or_else(|| format!("{} has no storage handle to block", subject.node_id))?;
            moves.block(change_id).await
        } else {
            let configs = self.configs.get().ok_or_else(|| "this admin has no client to block node properties".to_string())?;
            let request = NodeCommandRequest::StorageBlock { node_id: subject.node_id.clone(), incarnation: incarnation.clone(), change_id: change_id.clone() };
            crate::storage_change::command(configs.client(), &self.commands, &subject.node_id, &request).await?
        };
        match result {
            NodeCommandReply::StorageBlocked => Ok(()),
            other => Err(format!("node {}, incarnation {} did not block its property writer: {}", subject.node_id, incarnation, other.name())),
        }
    }

    /// A storage change's move (storage.md §2 "Changing it", step 4) of the birth `(node_id, incarnation)`
    /// of this admin's mesh onto `provider`, carried by this mesh-primary in its `mesh.unblock` to the
    /// exact birth on the node command op 0x22 (its own birth in-process). Once the birth moved, this
    /// mesh-primary, the writer of its mesh's rows, records the provider on its row.
    pub async fn carry_storage_step(&self, node_id: &NodeId, incarnation: &IncarnationId, change_id: &rdm_core::ChangeId, provider: &str) -> crate::storage_change::Carried {
        use crate::storage_change::Carried;
        use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
        let view = self.topology.read().await.clone();
        let me_name = self.held_name_in(&view);
        let Some(subject) = view.births().find(|n| n.node_id == *node_id).cloned() else {
            return Carried::NotDone(format!("path {me_name_path:?}, name {me_name_name:?} holds no birth {node_id} in its view", me_name_path = &me_name.path, me_name_name = &me_name.name));
        };
        if !view.own(&self.node_id).is_some_and(|n| n.is_primary && n.mesh == subject.mesh) {
            return Carried::NotDone(format!("path {me_name_path:?}, name {me_name_name:?} is not the mesh-primary of {}", subject.mesh, me_name_path = &me_name.path, me_name_name = &me_name.name));
        }
        if subject.incarnation_id.as_ref() != Some(incarnation) {
            return Carried::NotDone(format!("{} is another birth now", subject.node_id));
        }
        let command = NodeCommandRequest::StorageStep { node_id: node_id.clone(), incarnation: incarnation.clone(), change_id: change_id.clone(), provider: provider.to_string() };
        // A decoration holding the command (the testkit's cut): it is carried once released, its answer
        // dropped, and the move is not done now.
        if let (Some(release), Some(configs)) = (self.command_frame_seam.as_ref().and_then(|seam| seam.hold_storage_step(&subject.location(), change_id)), self.configs.get()) {
            if *node_id != self.node_id {
                let (client, commands, node) = (configs.client_arc(), self.commands.clone(), node_id.clone());
                tokio::spawn(async move {
                    release.await;
                    let _ = crate::storage_change::command(&client, &commands, &node, &command).await;
                });
                return Carried::NotDone(format!("path {me_name_path:?}, name {me_name_name:?} holds the move of {} until its release", subject.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name));
            }
        }
        let done = if *node_id == self.node_id {
            let Some(storage) = self.own.get().and_then(|own| own.storage_moves()) else {
                return Carried::NotDone(format!("path {me_name_path:?}, name {me_name_name:?} holds no storage handle yet", me_name_path = &me_name.path, me_name_name = &me_name.name));
            };
            match storage.refusal(change_id, provider).await {
                Some(refused) => refused,
                None => storage.serve(change_id, provider).await,
            }
        } else {
            let Some(configs) = self.configs.get() else {
                return Carried::NotDone(format!("path {me_name_path:?}, name {me_name_name:?} has no node-RPC client yet to carry the move to {}", subject.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name));
            };
            match crate::storage_change::command(configs.client(), &self.commands, node_id, &command).await {
                Ok(reply) => reply,
                Err(e) => return Carried::Unanswered(format!("path {me_name_path:?}, name {me_name_name:?} carried the move to {}: {e}", subject.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name)),
            }
        };
        match done {
            // The provider a node writes is the Fabric record's (Q361), never the node's.
            NodeCommandReply::StorageStepped { provider } => Carried::Stepped(provider),
            NodeCommandReply::RejectedStaleChange { held } => Carried::Stale(held),
            NodeCommandReply::Failed { reason, .. } => Carried::Refused(reason),
            other => Carried::NotDone(format!("{} answered the move {}", subject.node_id, other.name())),
        }
    }

    /// [`Self::apply`] of a call whose transport-proven endpoint is `peer` (`PeerContext`): the
    /// connection a command spares is the call's own, whether or not the view holds a birth for it.
    pub async fn apply_from(&self, peer: Option<iroh::PublicKey>, sender: Option<crate::model::Node>, req: &StatusRequest) -> StatusReply {
        if let (Some(seam), StatusRequest::DeclareNodeState { node_id, state, .. }) = (&self.declaration_seam, req) {
            let location = self.topology.read().await.own(node_id).map(|node| node.location());
            seam.before_declaration(node_id, location.as_ref(), *state).await;
        }
        let view = self.topology.read().await.clone();
        let me_name = self.held_name_in(&view);
        // A birth this admin's node recovery holds deleted from its verdict is a deleted birth here: its
        // declarations and commands are refused by name (node-recovery.md step 3; Q342).
        if let (Some(n), Some(channels)) = (sender.as_ref(), self.topology_view.as_ref()) {
            if n.incarnation_id.as_ref().is_some_and(|inc| channels.is_recovering(n.node_id.as_str(), inc)) {
                tracing::info_span!("rdm.node_admin.status.reject.via-deleted-birth", node_path = %me_name.path, node_name = %me_name.name, op = req.op(), sender = %n.node_id, sender_path = %n.path, sender_name = %n.name, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a call from a birth held deleted from its recovery's verdict: refused"));
                return rdm_core::reply::status::rejected_not_primary(rdm_core::status::NotPrimary::SubjectUnknown);
            }
        }
        // Exact-self downward property commands terminate at this node's writer. A node-admin
        // serving its own record is a node here, regardless of the primary roles it holds.
        let own_property = match req {
            StatusRequest::ApplyNodeConfig { incarnation, .. } => Some(incarnation),
            StatusRequest::ApplyNodeProperties { node_id, incarnation, .. } if *node_id == self.node_id => Some(incarnation),
            StatusRequest::CreateTags { node_id, incarnation, .. } | StatusRequest::DeleteTags { node_id, incarnation, .. } if *node_id == self.node_id => Some(incarnation),
            _ => None,
        };
        if let Some(incarnation) = own_property {
            let Some(subject) = view.own(&self.node_id) else {
                return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} holds no own birth yet", me_name_path = &me_name.path, me_name_name = &me_name.name));
            };
            if subject.incarnation_id.as_ref() != Some(incarnation) {
                return rdm_core::reply::stale(subject.incarnation_id.clone().unwrap_or_else(|| incarnation.clone()));
            }
            let Some(configs) = self.configs.get() else {
                return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} has no own property writer yet", me_name_path = &me_name.path, me_name_name = &me_name.name));
            };
            // A tags command on this admin's own record is decided by its own writer, on the span every
            // tags command's writer opens.
            let tags = matches!(req, StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. }).then(|| crate::node_properties::writer_span(&me_name, req));
            let result = match req {
                StatusRequest::ApplyNodeConfig { config, .. } => configs.own_config(subject, config.clone()).await,
                StatusRequest::ApplyNodeProperties { entry, .. } => configs.own_entry(subject, entry.clone()).await,
                StatusRequest::CreateTags { labels, .. } => configs.own_tags(subject, rdm_node::node_records::TagChange::Create(labels.clone())).await,
                StatusRequest::DeleteTags { scope, .. } => configs.own_tags(subject, rdm_node::node_records::TagChange::Delete(scope.clone())).await,
                _ => unreachable!("selected own property commands only"),
            };
            let reply = match result {
                Some(Ok(reply)) => if matches!(req, StatusRequest::ApplyNodeConfig { .. }) { rdm_core::reply::applied() } else { reply },
                Some(Err(error)) => rdm_core::reply::status::bounded_node_record_refused(self.node_id.as_str(), &error.to_string()),
                None => rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} has no own property writer yet", me_name_path = &me_name.path, me_name_name = &me_name.name)),
            };
            if let Some(span) = tags {
                span.record("subject_path", subject.path.as_str()).record("subject_name", subject.name.as_str());
                crate::node_properties::decided(&span, req, &reply);
            }
            return reply;
        }
        match req {
            StatusRequest::Reserved34 { .. } | StatusRequest::Reserved21 { .. } | StatusRequest::Reserved6 { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::Reserved12 { .. } | StatusRequest::Reserved13 { .. } | StatusRequest::Reserved14 { .. } | StatusRequest::Reserved15 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::Reserved29 { .. } => return crate::node_commands::refuse_reserved(self.node_id.as_str(), &sender.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(), req),
            StatusRequest::NodeLeft { node_id, .. } => {
                let name = view.births().find(|n| n.node_id == *node_id).map(|n| n.node_id.to_string()).unwrap_or_default();
                return crate::node_commands::refuse_node_left(self.node_id.as_str(), &name, &sender.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(), req);
            }
            StatusRequest::JoinMesh { .. } => return self.serve_command(&view, sender.as_ref(), peer, req).await,
            StatusRequest::DeleteMesh { .. } | StatusRequest::MeshDeleted { .. } => {
                let Some(mesh_deleter) = self.mesh_deleter.get().cloned() else {
                    return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} is still starting: it cannot serve {} yet", req.op(), me_name_path = &me_name.path, me_name_name = &me_name.name));
                };
                let reply = if matches!(req, StatusRequest::DeleteMesh { .. }) { mesh_deleter.delete_mesh(peer, sender.as_ref(), req).await } else { mesh_deleter.mesh_deleted(sender.as_ref(), req).await };
                tracing::info_span!("rdm.node_admin.status.update.via-command", node_path = %me_name.path, node_name = %me_name.name, op = req.op(), sender = %sender.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a mesh-deleted call naming this admin was decided"));
                return reply;
            }
            // A caller's property command is admitted at any time (Q166, node-config-update.md); a
            // mesh-primary that replied `blocked` to a storage change issues it, its row write and its
            // command to the node, only once it is unblocked (storage.md §2 step 1).
            // A caller's goes up to the fabric-primary and down to the node's writer (node-name-update.md
            // step 1), the fabric-primary's atlas written on its way back.
            StatusRequest::UpdateName { .. } => {
                self.commands.admitted().await;
                let routed = crate::node_name_update::routed(&me_name, &self.node_id, &self.topology, self.configs.get().map(|c| &**c), crate::node_config::SentBy::of_writer_call(sender.as_ref()), req).await;
                return match routed {
                    crate::node_name_update::Routed::Answered(reply) => reply,
                    crate::node_name_update::Routed::Here => self.update_name(req).await,
                };
            }
            StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. } => {
                self.commands.admitted().await;
                return crate::node_properties::tags(&me_name, &self.node_id, &self.topology, &*self.nodes_storage, self.configs.get().map(|c| &**c), crate::node_config::SentBy::of_writer_call(sender.as_ref()), req).await;
            }
            StatusRequest::UpdateNodeConfig { node_id, .. } => {
                self.commands.admitted().await;
                let view = self.topology.read().await.clone();
                return match self.configs.get() {
                    Some(configs) => configs.serve(&me_name, &self.node_id, &view, &self.topology, &*self.nodes_storage, crate::node_config::SentBy::of_writer_call(sender.as_ref()), req).await,
                    None => rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} has no node-RPC client yet; {} is refused", req.op(), me_name_path = &me_name.path, me_name_name = &me_name.name)),
                };
            }
            // The reads (Q167): answered from what this admin holds, changing nothing.
            // A mesh-primary that is not the fabric-primary answers a read of its own mesh from its atlas
            // and carries any other read up to the fabric-primary (query.md §1; Q224); a node-admin that
            // is not a primary keeps no atlas and forwards every read but its own node.connection.get to its
            // own mesh-primary, never to the fabric-primary (query.md §1; Q243).
            StatusRequest::Get { .. } | StatusRequest::GetNodes { .. } | StatusRequest::GetMeshes { .. } | StatusRequest::GetFabric | StatusRequest::GetConnections { .. } => {
                let read = req.as_read().expect("a read request is a read");
                // The reads are op 0x23's, a stream (Q285): Status's `Get` of any entity, and the retained
                // `GetNodes`, `GetMeshes` and `GetConnections`, are refused by name naming it; the retained
                // `GetFabric` answers its one fabric.
                if !matches!(req, StatusRequest::GetFabric) {
                    tracing::info_span!("rdm.node_admin.status.reject.via-read-on-0x23", node_path = %me_name.path, node_name = %me_name.name, read = read.entity.read(), request = req.op(), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("a read on Status refused by name: it is op 0x23's stream"));
                    return rdm_core::reply::status::bounded_rejected_filter(&format!("{} is read on op 0x23 (read), a stream (Q285)", read.entity.read()));
                }
                return match self.read_rows(&read).await {
                    Ok(rdm_core::read::Rows::Fabrics(mut fabrics)) if !fabrics.is_empty() => StatusReply::Fabric { fabric: fabrics.remove(0) },
                    Ok(rdm_core::read::Rows::Fabrics(fabrics)) => StatusReply::Fabrics { fabrics },
                    Ok(_) => rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} answered fabric.get with another entity's rows", me_name_path = &me_name.path, me_name_name = &me_name.name)),
                    Err(refusal) => status_of_read_refusal(refusal),
                };
            }
            StatusRequest::GetAtlas => {
                let reply = async {
                    let Some(atlas) = self.atlas.get() else {
                        return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} holds no fabric atlas yet; {} is refused", req.op(), me_name_path = &me_name.path, me_name_name = &me_name.name));
                    };
                    // One row for every node the current Build's topology names (fabric-atlas.md R-FA4).
                    let paths: Vec<crate::model::PathName> = match self.builds.current().await {
                        Ok(Some(b)) => b.topology.meshes.values().flat_map(|m| m.nodes.iter().cloned()).collect(),
                        Ok(None) => Vec::new(),
                        Err(e) => return rdm_core::reply::status::bounded_source_failed("builds.storage", req.op(), &format!("path {me_name_path:?}, name {me_name_name:?} could not read its current Build: {e}", me_name_path = &me_name.path, me_name_name = &me_name.name)),
                    };
                    if self.configs.get().and_then(|c| c.records()).is_none() {
                        return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} holds no node records yet; {} is refused", req.op(), me_name_path = &me_name.path, me_name_name = &me_name.name));
                    };
                    let records = crate::current_record::CurrentRecords { nodes: &*self.nodes_storage };
                    return match atlas.view(paths, &records, &view).await {
                        Ok(atlas) => StatusReply::within_reply_bound(StatusReply::Atlas { atlas }, req.op()),
                        Err(e) => rdm_core::reply::status::bounded_source_failed("nodes.storage", req.op(), &e),
                    };
                }
                .await;
                // A refusal of the atlas names its source and cause here, at the fabric-primary: the
                // caller that acts on the atlas (a refused caller's decision) waits on it.
                match &reply {
                    StatusReply::Atlas { .. } => {}
                    StatusReply::RejectedSourceFailed { source, cause, .. } => tracing::info_span!("rdm.node_admin.fabric.reject.via-atlas-unread", node_path = %me_name.path, node_name = %me_name.name, outcome = "rejected-source-failed", source = %source, cause = %cause, "otel.kind" = "internal").in_scope(|| tracing::info!("fabric.atlas.get is refused: a source of the atlas could not be read")),
                    other => tracing::info_span!("rdm.node_admin.fabric.reject.via-atlas-unread", node_path = %me_name.path, node_name = %me_name.name, outcome = %other.name(), "otel.kind" = "internal").in_scope(|| tracing::info!("fabric.atlas.get is refused")),
                }
                return reply;
            }
            StatusRequest::RenewCert { incarnation, cert, .. } => return self.renew_own_cert(&view, incarnation, cert, req).await,
            StatusRequest::UpdateIssuer { incarnation, key, cert, ca_key, .. } => {
                let from = sender.as_ref().map(|n| n.node_id.to_string()).or_else(|| peer.map(|k| k.fmt_short().to_string())).unwrap_or_default();
                return self.update_own_issuer(&view, &from, incarnation, key, cert, ca_key).await;
            }
            // node.roots.update (fabric-ca-renew.md "node.roots.update, the primitive"): this admin's exact
            // birth installs the fabric's trusted root set.
            StatusRequest::UpdateRoots { node_id, incarnation, roots, phase, secret } => {
                let me = self.node_id.as_str();
                if view.own(&self.node_id).and_then(|n| n.incarnation_id.as_ref()) != Some(incarnation) {
                    return rdm_core::reply::stale(view.own(&self.node_id).and_then(|n| n.incarnation_id.clone()).unwrap_or_else(|| incarnation.clone()));
                }
                let Some(registry) = self.own.get().and_then(|o| o.registry()) else {
                    return rdm_core::reply::status::not_ready(format!("{me} holds no connections registry yet; {} is refused", req.op()));
                };
                let installed = rdm_node::roots_update::install_roots(&registry, &me, node_id, incarnation, roots, *phase, secret.is_some());
                // The root secret a node-admin is carried (fabric-ca-renew.md step 2 and step 5): held as the
                // next root through the overlap, the fabric's root once the set replaces the old one; kept
                // only once the set it rides with is installed.
                if let (StatusReply::RootsUpdated { .. }, Some(secret), Some(certs)) = (&installed, secret, &self.certs) {
                    let kept = rdm_core::certs::decode_key(&secret.0).and_then(|k| if *phase == rdm_core::status::RootsPhase::Updated { certs.replace_root_with(k) } else { certs.hold_next_root(k) });
                    if let Err(e) = kept {
                        return rdm_core::reply::status::rejected_cert(format!("{me}: the root's secret: {e}"));
                    }
                }
                return installed;
            }
            // fabric.root.update (fabric-ca-renew.md "fabric.root.update, the caller op"): the fabric-primary
            // holds the new root as the next root, writes the roots row `minted` and answers; the phases
            // follow on its renewal tick (`cert_renewal::rotate_roots`).
            StatusRequest::UpdateRoot { secret } => return self.update_root(&view, secret).await,
            // fabric.storage.update (storage.md §2 "Changing it"): the fabric-primary starts the storage
            // change, its own workflow and never a Build, and answers its change_id.
            StatusRequest::UpdateStorage { topology_storage_provider } => {
                return match self.storage_changes.get() {
                    Some(changes) => changes.update(topology_storage_provider).await,
                    None => rdm_core::reply::status::not_ready(format!("{}: this admin holds no storage changes yet", self.node_id)),
                };
            }
            // A caller's call to a birth ended with its attempts spent (node-rpc.md §43; Q311).
            StatusRequest::ReportCallFailed { sender: from, receiver, reason } => return self.report_call_failed(&view, from, receiver, reason),
            _ => {}
        }
        // A downward exact-node operation naming this admin itself (a probe, an apply) is
        // answered by the subject, through the same `StatusService`.
        if let Some(me) = view.own(&self.node_id) {
            if let Some(reply) = self_subject(me, sender.as_ref(), peer, req, &self.republish, &self.rename, &self.pre_node_ready, &self.wake).await {
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
        // A caller birth this admin does not hold (Q207): refused `SubjectUnknown`, then deleted or
        // re-admitted by its path and name and this admin's atlas, on its own task.
        if let (StatusRequest::DeclareNodeState { node_id, incarnation, .. }, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, Some(atlas)) = (req, &reply, self.atlas.get()) {
            atlas.refused("declaration", node_id, incarnation);
        }
        // A `Ready` Mesh declaration the authority took (applied now or already applied) is
        // the mesh primary's checklist-complete report: the primary sends it only when its checklist is
        // complete. Whichever it was, the report is what this admin received now from that exact birth.
        // It is kept for the mesh the declaration names, from the birth that sent it (or its endpoint
        // while this admin's view holds no birth for it), never under the caller's own mesh.
        if let (StatusRequest::DeclareMeshState { mesh_id, state: MeshState::Ready, .. }, StatusReply::Applied | StatusReply::AlreadyApplied) = (req, &reply) {
            let mesh = (self.mesh_ids)().into_iter().find(|(_, id)| id == mesh_id).map(|(name, _)| name).or_else(|| view.meshes.iter().find(|m| m.id.as_ref() == Some(mesh_id)).map(|m| m.name.clone()));
            let reporter = sender.as_ref().and_then(|from| from.birth()).map(Reporter::Birth).or_else(|| peer.map(|k| Reporter::Endpoint(k.to_string())));
            if let (Some(mesh), Some(reporter)) = (mesh, reporter) {
                self.declared.lock().unwrap().record_report(&mesh, reporter, std::time::Instant::now());
                self.wake.report_notify.notify_waiters();
            }
        }
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node_path = %me_name.path, node_name = %me_name.name,
            op = req.op(),
            state = %declared_state(req),
            sender = %sender.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(),
            outcome = reply.name(),
            detail = %detail(&reply),
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("declaration decided by the authority"));
        reply
    }
}

impl StatusService {
    /// A caller's `ReportCallFailed` (node-rpc.md §43; Q311, Q322): its call to the exact birth
    /// `receiver` ended with its attempts spent. This admin, the primary above that birth (its mesh's
    /// mesh-primary; the fabric-primary for a mesh-primary, Q338), marks it as its own spent call marks
    /// it ([`crate::offline::mark_spent`]) and answers `Applied`; a birth it does not hold, or is not the
    /// primary above, is refused `RejectedNotPrimary { why: SubjectUnknown }`. Nothing is reported on
    /// from here.
    fn report_call_failed(&self, view: &Topology, from: &rdm_core::status::ReportedBirth, receiver: &rdm_core::status::ReportedBirth, reason: &str) -> StatusReply {
        let above = view.own(&receiver.node_id).filter(|n| n.incarnation_id.as_ref() == Some(&receiver.incarnation) && view.is_primary_above(&self.node_id, &n.mesh, &n.node_id));
        let Some(node) = above else {
            tracing::info_span!(
                "rdm.node_admin.node.reject.via-call-failed",
                node_path = %self.me.path, node_name = %self.me.name,
                node_id = %receiver.node_id, incarnation_id = %receiver.incarnation.as_str(),
                sender = %from.node_id, reason,
                outcome = "subject-unknown",
                "otel.kind" = "internal",
            )
            .in_scope(|| tracing::info!("a report of a spent call for a birth this admin is not the primary above: refused subject-unknown"));
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        };
        crate::offline::mark_spent(&self.unreachable, self.digest_view.as_ref(), node, from.node_id.as_str(), reason);
        rdm_core::reply::applied()
    }

    /// This admin's exact birth in `view`, when `incarnation` is it; else the reply that refuses the
    /// call naming the birth held (R-RX5: the birth the call reached answers it).
    fn own_birth(&self, view: &Topology, incarnation: &IncarnationId) -> Result<crate::model::Node, StatusReply> {
        let Some(me) = view.own(&self.node_id) else {
            return Err(rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "a view holding this admin".into() }));
        };
        match me.incarnation_id.as_ref() {
            None => Err(rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown)),
            Some(held) if held != incarnation => Err(rdm_core::reply::stale(held.clone())),
            Some(_) => Ok(me.clone()),
        }
    }

    /// `node.cert.renew` of this admin's exact birth (node-cert-renew.md step 4): installed on the
    /// app protocols it opens connections on, as a node installs it.
    async fn renew_own_cert(&self, view: &Topology, incarnation: &IncarnationId, cert: &[u8], req: &StatusRequest) -> StatusReply {
        let me = match self.own_birth(view, incarnation) {
            Ok(me) => me.clone(),
            Err(refused) => return refused,
        };
        let reply = match self.own.get() {
            Some(own) => own.renew_cert(cert),
            None => return rdm_core::reply::status::not_ready(format!("{} is still starting: it holds no connections registry yet", self.node_id)),
        };
        // An admin writes its own record (Q356): the dates of the cert it installed, as its issuer
        // computed them, else as the cert carries them.
        if let StatusReply::CertRenewed { .. } = &reply {
            let dates = match req {
                StatusRequest::RenewCert { cert_dates: Some(dates), .. } => Some(dates.clone()),
                _ => self.certs.as_ref().and_then(|certs| crate::cert_renewal::installed_dates(req, certs)),
            };
            if let Some(dates) = dates {
                if let Err(why) = self.write_own(move |r| r.cert = Some(dates)).await {
                    return rdm_core::reply::status::not_ready(format!("{}: the cert is installed and its dates could not be written on its record: {why}", me.node_id));
                }
            }
        }
        reply
    }

    /// Write `change` on this admin's own record, through its own property writer (Q356).
    async fn write_own(&self, change: impl FnOnce(&mut crate::storage::NodeRecord) + Send + 'static) -> Result<(), String> {
        let configs = self.configs.get().ok_or_else(|| format!("{} has no own property writer yet", self.node_id))?;
        configs.own_update(change).await
    }

    /// `node.issuer.update` of this admin's exact birth (node-issuer-update.md step 3): the material must
    /// decode, name this admin's mesh, be the key its cert names, and verify to a root this admin
    /// holds; it is installed, so every member cert it signs from now carries it, and answered
    /// `node.issuer.updated`. Anything else is refused by name and what it holds is kept.
    /// `fabric.root.update` at the fabric-primary: refused by name for a root already trusted and while a
    /// rotation is in flight; otherwise the next root held and kept, the roots row written `minted`
    /// (the old root and the new, and the old anchor's bound, the current CA's expiry), answered
    /// `fabric.root.updated`.
    async fn update_root(&self, view: &Topology, secret: &rdm_core::status::RootSecretHex) -> StatusReply {
        let me = self.node_id.as_str();
        if !view.own(&self.node_id).is_some_and(|n| n.is_fabric_primary) {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "fabric-primary".into() });
        }
        let Some(certs) = &self.certs else {
            return rdm_core::reply::status::rejected_cert(format!("{me}: this admin holds no certs"));
        };
        let new = match rdm_core::certs::decode_key(&secret.0) {
            Ok(k) => k,
            Err(e) => return rdm_core::reply::status::rejected_cert(format!("the root's secret: {e}")),
        };
        let hex = |k: &rdm_core::certs::VerifyingKey| rdm_core::launch::encode_hex(k.as_bytes());
        let new_pub = new.verifying_key();
        if certs.root() == Some(new_pub) || certs.next_root() == Some(new_pub) {
            return rdm_core::reply::status::root_already_trusted(hex(&new_pub));
        }
        if let Some(row) = self.caches.fabric.get(&self.fabric_id).and_then(|r| r.roots).filter(|r| r.phase != rdm_core::status::RootsPhase::Updated) {
            return rdm_core::reply::status::rotation_in_progress(row.phase);
        }
        let Some(old) = certs.root() else {
            return rdm_core::reply::status::rejected_cert(format!("{me}: this admin holds no root"));
        };
        if let Err(e) = certs.hold_next_root(new) {
            return rdm_core::reply::status::rejected_cert(e);
        }
        let expires_at = certs.ca_dates().map(|(_, expires, _)| expires).unwrap_or(0);
        let roots = vec![hex(&old), hex(&new_pub)];
        let span = tracing::info_span!("rdm.node_admin.root.update.via-caller", node = %me, phase = "minted", root_expires_at = tracing::field::Empty, previous_root_expires_at = expires_at, nodes = 0u64, outcome = tracing::field::Empty);
        let row = crate::fabric_storage::RootsRow { phase: rdm_core::status::RootsPhase::Minted, roots, expires_at };
        let written = self.caches.update_fabric(&*self.fabric_storage, &self.fabric_id, None, |record| record.roots = Some(row.clone())).await;
        match written {
            Ok(_) => {
                span.record("outcome", "minted");
                span.in_scope(|| tracing::info!("the new root is held beside the old one: the rotation's phases follow on the renewal tick"));
                rdm_core::reply::status::root_updated(rdm_core::status::RootsPhase::Minted)
            }
            Err(e) => {
                span.record("outcome", "unwritten");
                span.in_scope(|| tracing::warn!(error = %e, "the roots row is not written: the rotation is not accepted"));
                rdm_core::reply::status::not_ready(format!("{me}: the roots row is not written: {e}"))
            }
        }
    }

    async fn update_own_issuer(&self, view: &Topology, sender: &str, incarnation: &IncarnationId, key: &rdm_core::status::IssuingKeyHex, cert: &str, ca_key: &rdm_core::status::CaKeyHex) -> StatusReply {
        let me = match self.own_birth(view, incarnation) {
            Ok(me) => me.clone(),
            Err(refused) => return refused,
        };
        let Some(certs) = &self.certs else {
            return rdm_core::reply::status::rejected_cert(format!("{}: this admin holds no certs", self.node_id));
        };
        let checked = (|| {
            let secret = rdm_core::certs::decode_key(&key.0).map_err(|e| format!("the issuing key: {e}"))?;
            let signed = rdm_core::certs::decode_issuing_cert(cert).map_err(|e| format!("the issuing cert: {e}"))?;
            if signed.cert.mesh != me.mesh {
                return Err(format!("the issuing cert is mesh {}'s, and {} is of mesh {}", signed.cert.mesh, self.node_id, me.mesh));
            }
            if signed.cert.issuer_pub != secret.verifying_key() {
                return Err("the issuing key is not the key its cert names".to_string());
            }
            let roots: Vec<rdm_core::certs::VerifyingKey> = certs.root().into_iter().chain(self.own.get().map(|o| o.roots()).unwrap_or_default()).collect();
            rdm_core::certs::verify_issuer(&signed, &roots).map_err(|e| format!("{e}: the issuing cert's chain does not verify to a root this admin holds"))?;
            // The CA the issuer is minted under, its cert the one the issuing cert carries (verified to
            // the root just above): its key must be the one that cert names (Q275).
            let ca_secret = rdm_core::certs::decode_key(&ca_key.0).map_err(|e| format!("the CA key: {e}"))?;
            if ca_secret.verifying_key() != signed.ca.cert.ca_pub {
                return Err("the CA key is not the key the issuing cert's CA cert names".to_string());
            }
            let ca = rdm_core::certs::CaKey::from_parts(ca_secret, signed.ca.clone());
            Ok((rdm_core::certs::IssuingKey::from_parts(secret, signed), ca))
        })();
        match checked {
            Ok((key, ca)) => {
                // The CA first, then the issuer minted under it, installed together as the launch writes
                // them; a CA older than the one held installs nothing (node-issuer-update.md, `ca-older`).
                if let crate::certs::CaInstall::Older { held_expires_at } = certs.install_ca(ca.clone()) {
                    return rdm_core::reply::status::rejected_cert(format!("ca-older: the CA {sender} sent expires at {}, before the one {} holds ({held_expires_at})", ca.expiry_ms(), self.node_id));
                }
                let expiry = key.expiry_ms();
                let installed = key.cert().clone();
                let previous = certs.install_issuer(key);
                tracing::info_span!("rdm.node_admin.issuer.update.via-installed", node_path = %self.me.path, node_name = %self.me.name, node_id = %self.node_id, mesh = %me.mesh, sender_node_id = sender, issuer_expires_at = expiry, previous_issuer_expires_at = previous.unwrap_or(0))
                    .in_scope(|| tracing::info!("the mesh's new issuer is installed: member certs are signed with it from now"));
                // An admin writes its own signing cert dates on its own record at its install (Q356).
                let dates = crate::cert_renewal::installed_signing_dates(&installed, certs);
                if let Err(why) = self.write_own(move |r| r.signing_cert = Some(dates)).await {
                    return rdm_core::reply::status::not_ready(format!("{}: the issuer is installed and its dates could not be written on its record: {why}", self.node_id));
                }
                StatusReply::IssuerUpdated { node_id: self.node_id.clone(), incarnation: incarnation.clone(), issuer_expires_at: expiry }
            }
            Err(reason) => rdm_core::reply::status::rejected_cert(format!("{}: {reason}", self.node_id)),
        }
    }

    /// A read (Q167, R-RD1 to R-RD3) answered from what this admin holds: `node.get` from its atlas,
    /// `mesh.get` from its view, `fabric.get` from its view and its fabric and builds records, and
    /// `node.connection.get { node }` of its own exact birth. The node is `node.connection.get`'s address and
    /// no other read's: a read without it, or another read with it, is `RejectedFilter`.
    async fn read(&self, me_name: &crate::model::PathName, view: &Topology, read: &rdm_core::status::Read) -> StatusReply {
        use rdm_core::filter::Entity;
        let op = read.entity.read();
        match (read.entity, &read.node) {
            (Entity::Connections, None) => return rdm_core::reply::status::bounded_rejected_filter(&format!("{op} names no node: the exact birth (id, incarnation) is the read's address")),
            (Entity::Connections, Some(_)) => {}
            (_, Some((node_id, _))) => return rdm_core::reply::status::bounded_rejected_filter(&format!("{op} names the node {node_id}: only node.connection.get takes a node; a {op} pair names one (id)")),
            (_, None) => {}
        }
        match read.entity {
            Entity::Connections => {
                let Some((_, incarnation)) = &read.node else { unreachable!("checked above") };
                self.read_connections(view, incarnation, &read.filter).await
            }
            Entity::Nodes => {
                // Each record read through the reader (query.md §1); the lifecycle the ops left each birth
                // in from this admin's atlas.
                let Some(atlas) = self.atlas.get() else {
                    return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} holds no fabric atlas yet; {op} is refused", me_name_path = &me_name.path, me_name_name = &me_name.name));
                };
                let Some(loads) = self.loads.as_ref() else {
                    return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} reads no digest view; {op} is refused", me_name_path = &me_name.path, me_name_name = &me_name.name));
                };
                // Each record through the reader (query.md §1): both parts of each node's record and the
                // name claims, in this admin's nodes store, once its node records are bound.
                let records = self.configs.get().and_then(|c| c.records());
                let current = records.map(|_| crate::current_record::CurrentRecords { nodes: &*self.nodes_storage });
                crate::views::nodes_answer_with_status(me_name, view, loads, atlas, &read.filter, current).await
            }
            Entity::Meshes => {
                if self.loads.is_none() {
                    return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} reads no digest view; {op} is refused", me_name_path = &me_name.path, me_name_name = &me_name.name));
                }
                crate::views::meshes_answer(me_name, view, &read.filter)
            }
            Entity::Fabrics => {
                if self.loads.is_none() {
                    return rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} reads no digest view; {op} is refused", me_name_path = &me_name.path, me_name_name = &me_name.name));
                }
                // The fabric's `build_id` is the current Build, read from this admin's builds store (R-BI3):
                // a read that fails is named, never answered as no Build.
                // Its topology_storage_provider and the providers it left are the Fabric row's (storage.md §2),
                // read from fabric.storage: a read that fails is named, never answered as none.
                let record = match self.fabric_storage.fabric().await {
                    Ok(r) => r,
                    Err(e) => return rdm_core::reply::status::bounded_source_failed("fabric.storage", op, &format!("path {me_name_path:?}, name {me_name_name:?} could not read the Fabric record: {e}", me_name_path = &me_name.path, me_name_name = &me_name.name)),
                };
                // The cert dates of the births of the meshes this admin is primary for (its atlas's scope,
                // Q207), each on its record read through the reader (query.md §1): a read that fails is
                // named, never answered as no expiry.
                let mut certs = None;
                if let (Some(atlas), Some(_)) = (self.atlas.get(), self.configs.get().and_then(|c| c.records())) {
                    let reader = crate::current_record::CurrentRecords { nodes: &*self.nodes_storage };
                    let scope = atlas.meshes();
                    let mut rows = Vec::new();
                    for n in view.births().filter(|n| scope.contains(&n.mesh)) {
                        match reader.row(&n.node_id).await {
                            Ok(Some(row)) => rows.push(row),
                            Ok(None) => {}
                            Err(e) => return rdm_core::reply::status::bounded_source_failed("node records", op, &format!("path {me_name_path:?}, name {me_name_name:?} could not read the record of node {}: {e}", n.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name)),
                        }
                    }
                    certs = Some(rows);
                }
                match self.builds.current_build().await {
                    Ok(current) => {
                        crate::views::fabric_answer_with_status(me_name, view, current.into_build_id().map(|b| b.to_string()), certs.as_deref(), self.certs.as_ref().and_then(|c| c.now_ms()), record.as_ref(), &read.filter)
                    }
                    Err(e) => rdm_core::reply::status::bounded_source_failed("builds.storage", op, &format!("path {me_name_path:?}, name {me_name_name:?} could not read its current Build: {e}", me_name_path = &me_name.path, me_name_name = &me_name.name)),
                }
            }
        }
    }

    /// `node.connection.get { node }` of this admin's exact birth: the connections it holds that match
    /// `filter`, under the name it holds. The birth the call reached answers it (R-RX5: no receiver
    /// refuses a call for the node id it names); another incarnation is refused naming the one held.
    async fn read_connections(&self, view: &Topology, incarnation: &IncarnationId, filter: &rdm_core::filter::Filter) -> StatusReply {
        let Some(me) = view.own(&self.node_id) else {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "a view holding this admin".into() });
        };
        match me.incarnation_id.as_ref() {
            None => return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown),
            Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
            Some(_) => {}
        }
        match self.own.get() {
            Some(own) => own.connections_get(filter).await,
            None => rdm_core::reply::status::not_ready(format!("node {} is still starting: it holds no connections writer yet, so node.connection.get is refused", me.node_id)),
        }
    }
}

/// Where a read sent to the admin born `me_id` is answered (query.md §1; Q224).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadAt {
    /// Here: the fabric-primary (its atlas holds every mesh), a mesh-primary's read of its own mesh, and
    /// an exact birth's `node.connection.get`.
    Here,
    /// Carried up to the fabric-primary: at a mesh-primary that is not it, a read its atlas does not
    /// hold.
    FabricPrimary,
    /// Forwarded to its own mesh-primary: every read but `node.connection.get` at a node-admin that is not a
    /// primary, which keeps no atlas (query.md §1; Q243).
    MeshPrimary,
}

/// [`ReadAt`] of `read` at the admin born `me_id` in `view`, whose atlas holds the meshes `held` (a
/// mesh-primary's: its own mesh). At a node-admin that is not a primary every read but `node.connection.get`
/// is its mesh-primary's (Q243). At a mesh-primary, a `node.get` is of its own mesh when one of its
/// `mesh` pairs names only meshes its atlas holds, a `mesh.get` when one of its `name` (or `id`) pairs
/// does; any other read, `fabric.get` included, is the fabric-primary's.
pub(crate) fn read_answered_at(view: &Topology, me_id: &rdm_core::NodeId, held: &std::collections::BTreeSet<String>, read: &rdm_core::status::Read) -> ReadAt {
    use rdm_core::filter::Entity;
    let Some(me) = view.own(me_id) else { return ReadAt::Here };
    if read.entity == Entity::Connections || view.is_fabric_primary(me_id) {
        return ReadAt::Here;
    }
    if !view.is_mesh_primary(&me.mesh, me_id) {
        return ReadAt::MeshPrimary;
    }
    let mesh_name_of_id = |id: &str| view.meshes.iter().find(|m| m.id.as_ref().is_some_and(|i| i.to_string() == id)).map(|m| m.name.clone());
    let of_held = |property: &str, by_id: bool| {
        read.filter.pairs.iter().filter(|(p, _)| p == property).any(|(_, values)| {
            !values.is_empty() && values.iter().all(|v| if by_id { mesh_name_of_id(v).is_some_and(|m| held.contains(&m)) } else { held.contains(v) })
        })
    };
    let own = match read.entity {
        Entity::Nodes => of_held("mesh", false),
        Entity::Meshes => of_held("name", false) || of_held("id", true),
        _ => false,
    };
    if own { ReadAt::Here } else { ReadAt::FabricPrimary }
}

impl StatusService {
    /// The rows a plural read answers (Q167; query.md §1): from this admin's atlas when it holds them,
    /// else carried to the primary that does (Q224, Q243) over op `0x23`; captured once, in the read's
    /// stable order. A refusal is the read's own frame: `RejectedFilter` naming the pair,
    /// `RejectedSourceFailed` naming the source, `NotReady` naming what this admin waits for.
    pub async fn read_rows(&self, read: &rdm_core::status::Read) -> Result<rdm_core::read::Rows, rdm_core::read::ReadReply> {
        use rdm_core::read::{ReadReply, Rows};
        let view = self.topology.read().await.clone();
        let me_name = self.held_name_in(&view);
        let held = self.atlas.get().map(|a| a.meshes()).unwrap_or_default();
        let to = match read_answered_at(&view, &self.node_id, &held, read) {
            ReadAt::Here => None,
            ReadAt::FabricPrimary => Some(crate::node_config::ReadTo::FabricPrimary),
            ReadAt::MeshPrimary => Some(crate::node_config::ReadTo::MeshPrimary),
        };
        if let Some(to) = to {
            return match self.configs.get() {
                Some(configs) => configs.carry_read(&me_name, &self.node_id, &view, read, to).await,
                None => Err(ReadReply::NotReady { reason: format!("path {me_name_path:?}, name {me_name_name:?} has no node-RPC client yet to send {} on to {}", read.entity.read(), to.whom(), me_name_path = &me_name.path, me_name_name = &me_name.name) }),
            };
        }
        match self.read(&me_name, &view, read).await {
            StatusReply::Nodes { nodes } => Ok(Rows::Nodes(nodes)),
            StatusReply::Meshes { meshes } => Ok(Rows::Meshes(meshes)),
            StatusReply::Fabrics { fabrics } => Ok(Rows::Fabrics(fabrics)),
            StatusReply::Connections { connections } => Ok(Rows::Connections(connections)),
            StatusReply::RejectedFilter { reason } => Err(ReadReply::RejectedFilter { reason }),
            StatusReply::RejectedSourceFailed { source, op, cause } => Err(ReadReply::RejectedSourceFailed { source, op, cause }),
            StatusReply::NotReady { reason } => Err(ReadReply::NotReady { reason }),
            other => Err(ReadReply::NotReady { reason: format!("path {me_name_path:?}, name {me_name_name:?} answered {} with {}", read.entity.read(), other.name(), me_name_path = &me_name.path, me_name_name = &me_name.name) }),
        }
    }

    /// The name of the mesh carrying `mesh_id`: a mesh this admin's Build created, else one its view
    /// holds.
    async fn mesh_name_of(&self, mesh_id: &MeshId) -> Option<String> {
        if let Some((name, _)) = (self.mesh_ids)().into_iter().find(|(_, id)| id == mesh_id) {
            return Some(name);
        }
        self.topology.read().await.meshes.iter().find(|m| m.id.as_ref() == Some(mesh_id)).map(|m| m.name.clone())
    }

    /// A start begins `birth`'s next cycle (node-start.md): what it declared before its stop
    /// (`Leaving`) is no longer where its next declarations are measured from, so `Pending` and
    /// `Ready` run forward again from the start. The node writes its own states on its record
    /// (Q356); this is what this admin holds of them.
    pub async fn begin_cycle(&self, node: &crate::model::Node, birth: &IncarnationId) {
        let _operation = self.operation_effects.write().await;
        self.begin_cycle_held(&node.node_id, &node.location(), birth).await;
    }

    /// `begin_cycle` under a hold of `operation_effects` its caller took.
    async fn begin_cycle_held(&self, node_id: &NodeId, name: &crate::model::PathName, birth: &IncarnationId) {
        // The cut of its last cycle is not its next cycle's: a started birth holds connections again.
        self.declared.lock().unwrap().connections_deleted.remove(&(node_id.clone(), birth.clone()));
        let held = self.declared.lock().unwrap().node(node_id, birth);
        let Some(state) = held else { return };
        let outcome = {
            let mut declared = self.declared.lock().unwrap();
            declared.forget_node(&(node_id.clone(), birth.clone()));
            declared.names.remove(&(node_id.clone(), birth.clone()));
            "begun"
        };
        tracing::info_span!("rdm.node_admin.status.update.via-cycle-begun", node_path = %name.path, node_name = %name.name, node_id = %node_id, incarnation_id = %birth.as_str(), held = ?state, outcome, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("a start begins the birth's next cycle: its declared state is measured from Pending again"));
    }

    /// The node's join report, accepted at this admin's `JoinService` (node-mesh-join.md step 10: the
    /// node's mesh-primary "records and publishes the node once as Pending when it accepts the node's
    /// join report, before it answers it"): at a first start, a start after a stop and a commanded join
    /// alike, a new join (`new_join`) begins the birth's next cycle here, at its join and never before
    /// the command reached it (node-start.md step 2), and it is recorded and published Pending, even
    /// while the birth is still Pending from its last join (a birth cut off before its hook passed and
    /// commanded back, Q87): one Pending per join, not per birth (Q99). It is published
    /// ready only when its hook has passed and it declares so. One hold of
    /// `operation_effects`, so a declaration of the birth is committed before the begin or against the
    /// Pending, never between them. A later report of that join (a retry, `new_join` false) finds the
    /// birth Pending and applies as already applied: it starts no other. One
    /// `rdm.node_admin.status.update.via-mesh-join-admitted` span with the outcome.
    pub async fn joined_pending(&self, node_id: &NodeId, name: &crate::model::PathName, birth: &IncarnationId, new_join: bool) -> StatusReply {
        let reply = {
            let _operation = self.operation_effects.write().await;
            self.pending_held(node_id, name, birth, new_join).await
        };
        tracing::info_span!("rdm.node_admin.status.update.via-mesh-join-admitted", node_path = %name.path, node_name = %name.name, node_id = %node_id, incarnation_id = %birth.as_str(), outcome = reply.name(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the join report was accepted: the birth's next cycle begins and it is published Pending before the join is answered"));
        reply
    }

    /// The birth's Pending at its accepted join report, under a hold of `operation_effects` its caller
    /// took: a later report of the join (`new_join` false) is already applied while the birth is
    /// Pending; a new join, and a later report that finds the birth not Pending, begins its next cycle
    /// and records Pending.
    async fn pending_held(&self, node_id: &NodeId, name: &crate::model::PathName, birth: &IncarnationId, new_join: bool) -> StatusReply {
        if !new_join && self.declared.lock().unwrap().node(node_id, birth) == Some(NodeState::Pending) {
            return rdm_core::reply::already_applied();
        }
        self.begin_cycle_held(node_id, name, birth).await;
        let held = self.topology.read().await.births().find(|n| n.node_id == *node_id).map(|n| (n.mesh.clone(), n.r#type.clone()));
        let (mesh, r#type) = match held {
            Some(held) => held,
            None => match self.nodes_storage.node(node_id).await {
                Ok(Some(line)) => (line.record.mesh(), line.record.r#type),
                Ok(None) => return rdm_core::reply::status::not_ready(format!("node {node_id}: neither the view nor the node records hold it at the accepted join")),
                Err(e) => return rdm_core::reply::status::not_ready(format!("node {node_id}: node record read failed: {e}")),
            },
        };
        let row = crate::status_rpc::Declaration { r#type, mesh, node_id: node_id.clone(), incarnation_id: birth.clone(), name: name.clone(), state: NodeState::Pending };
        self.commit_held(Effect::Node(row, NodeState::Pending, None)).await
    }

    /// A start's `NodeJoined` frame (`node.mesh.joined`, `op` its exact birth): it belongs to the
    /// Pending the birth's accepted join report recorded and starts no other (node-mesh-join.md step
    /// 10). Under one hold of `operation_effects`: while the birth's declared state is in its new cycle
    /// already (that Pending, or the Ready it declared since), the frame applies as already
    /// applied: no new cycle and no second Pending row; its stop, if the view holds one, ends here all
    /// the same, and the view holds the birth again with that state, never set back to Pending. A birth
    /// whose join report this admin did not record is held Pending as before: its stop ends, its next
    /// cycle begins and Pending is recorded.
    pub async fn node_joined(&self, op: &rdm_core::LifecycleOp, node: &crate::model::Node, birth: &IncarnationId) -> StatusReply {
        let _operation = self.operation_effects.write().await;
        let held = self.declared.lock().unwrap().node(&node.node_id, birth);
        if let Some(state @ (NodeState::Pending | NodeState::Ready)) = held {
            if let Some(digest_view) = &self.digest_view {
                let _ = set_member_status(digest_view, MemberStatusReceipt::NodeJoined { op, already: Some(state) });
            }
            return rdm_core::reply::already_applied();
        }
        if let Some(digest_view) = &self.digest_view {
            let _ = set_member_status(digest_view, MemberStatusReceipt::NodeJoined { op, already: None });
        }
        // The frame is a later report of the start's join, never a join of its own.
        self.pending_held(&node.node_id, &node.location(), birth, false).await
    }

    /// Forget effects only when exact-ID records prove the node deleted, or exact-ID digest
    /// evidence identifies a different incarnation or deletion. A historical location and its
    /// current occupant do not prove anything about the original node's lifetime.
    pub async fn drop_superseded(&self, digest_view: &rdm_transport::topology::DigestView) {
        let me_name = digest_view.held_path_name().unwrap_or_else(|| self.me.clone());
        // Collect identities only; historical operation locations are not lifecycle evidence.
        let held: std::collections::HashSet<(NodeId, IncarnationId)> = {
            let declared = self.declared.lock().unwrap();
            declared.nodes.keys().cloned().chain(declared.operations.values().map(|row| (row.node_id.clone(), row.incarnation_id.clone()))).collect()
        };
        for (node_id, incarnation) in held {
            let (why, by) = match self.superseded_on_rows(&node_id).await {
                Some(on_rows) => on_rows,
                None => match digest_view.get(node_id.as_str()) {
                    Some((d, _)) if d.node.incarnation != incarnation => ("new-incarnation", d.node.incarnation.to_string()),
                    Some(_) => continue,
                    None if digest_view.is_deleted(node_id.as_str()) => ("node-deleted", String::new()),
                    None => continue,
                },
            };
            // The birth's declared states and operation effects are forgotten here; nothing is written:
            // each node writes its own record (Q356).
            let (outcome, error) = {
                let mut declared = self.declared.lock().unwrap();
                declared.operations.retain(|_, r| r.node_id != node_id || r.incarnation_id != incarnation);
                declared.forget_node(&(node_id.clone(), incarnation.clone()));
                declared.names.remove(&(node_id.clone(), incarnation.clone()));
                ("deleted", String::new())
            };
            let span = match why {
                "new-incarnation" => tracing::info_span!("rdm.node_admin.status.remove.via-new-incarnation", node_path = %me_name.path, node_name = %me_name.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), held_incarnation_id = %by, outcome, error = %error, "otel.kind" = "internal"),
                "new-node" => tracing::info_span!("rdm.node_admin.status.remove.via-new-node", node_path = %me_name.path, node_name = %me_name.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), held_node_id = %by, outcome, error = %error, "otel.kind" = "internal"),
                _ => tracing::info_span!("rdm.node_admin.status.remove.via-node-deleted", node_path = %me_name.path, node_name = %me_name.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), outcome, error = %error, "otel.kind" = "internal"),
            };
            span.in_scope(|| tracing::info!("a superseded birth's declared-state keys are tombstoned: nothing of that birth is folded back"));
        }
    }

    /// Only this node's retained released record proves it deleted. The claim at that
    /// record's current location optionally identifies a successor for the diagnostic;
    /// it never substitutes for the exact node's deletion evidence.
    async fn superseded_on_rows(&self, node_id: &NodeId) -> Option<(&'static str, String)> {
        let storage = &*self.nodes_storage;
        let own = storage.node(node_id).await.ok()??.record;
        if !crate::node_rows::released_its_name(&own) {
            return None;
        }
        if let Some(by) = storage.nodes(&own.mesh()).await.ok().into_iter().flatten().find(|r| r.location() == own.location() && r.node_id != *node_id && !crate::node_rows::released_its_name(r)) {
            return Some(("new-node", by.node_id.to_string()));
        }
        Some(("node-deleted", String::new()))
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
        let op = rdm_core::LifecycleOp { r#type: node.r#type.clone(), mesh: node.mesh.clone(), node_id: node.node_id.clone(), incarnation: incarnation.clone(), path: node.path.clone(), name: node.name.clone(), build_id: ctx.build_id.clone(), operation: ctx.operation.clone(), event_at_rafka_ms: 0, traceparent: None, tracestate: None };
        let request = if start { Q::StartNode { node_id: node.node_id.clone(), incarnation: incarnation.clone(), operation: ctx.operation.clone() } } else { Q::StopNode { node_id: node.node_id.clone(), incarnation: incarnation.clone(), operation: ctx.operation.clone() } };
        let key = crate::node_commands::CommandKey { node_id: node.node_id.clone(), incarnation: incarnation.clone(), operation: ctx.operation.clone() };
        // A mesh-primary that replied `blocked` issues no new command until it is unblocked (storage.md §2).
        self.commands.admitted().await;
        let _ = self.commands.open(key.clone());
        let mut frames = if node.node_id == self.node_id {
            let Some(own) = self.own.get().cloned() else { return A::NotSent { reason: "the own birth has not joined".into(), code: Code::Unavailable }; };
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let commander = self.node_id.clone();
            // This admin's own run commands its own birth: the command names that run's Build, so the
            // stop's hold spares the run's turns, which wait on it (node-stop.md step 3).
            let run_build = ctx.build_id.clone();
            tokio::spawn(async move {
                let terminal = own.serve_node_command_of_run(Some(&commander), &run_build, &request, Box::new(crate::node_self::ChannelFrames(tx.clone()))).await;
                let _ = tx.send(terminal);
            }.instrument(tracing::Span::current()));
            CarriedFrames::InProcess(rx)
        } else {
            let Some(configs) = self.configs.get() else { return A::NotSent { reason: "this mesh-primary holds no Node RPC client".into(), code: Code::Unavailable }; };
            let opts = rdm_transport::CallOptions { send: if start { rdm_transport::Send::default() } else { rdm_transport::Send::with_time_limit(self.commands.connections_delete_reply() + rdm_transport::TRANSPORT_LIMIT) }, ..Default::default() };
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
        // Whether this stream carried the start's NodeJoined: a start an earlier run completed is
        // answered without one (`AlreadyApplied`, node_self.rs `Cycle::Live`).
        let mut joined = false;
        let mut closure = None;
        while let Some(item) = frames.next().await {
            let mut joined_outcome: Option<&'static str> = None;
            let frame = match item {
                Ok(frame) => frame,
                Err(f @ rdm_transport::stream::StreamFailure::Unserved(_)) => return A::Refused { code: f.code(), reply: format!("Unserved: {f:?}") },
                Err(f) => return A::Indeterminate { code: f.code(), reason: format!("{} stream: {f:?}", request_name(start)) },
            };
            let position = frames.position();
            let result = match &frame {
                R::CommandAccepted { .. } => {
                    // Admission and completion share this operation's serialization: a declaration
                    // cannot publish stopped between the seen check and setting stopping.
                    let _admission = self.operation_effects.write().await;
                    accepted = true;
                    let declared = crate::status_rpc::Declaration { r#type: op.r#type.clone(), mesh: op.mesh.clone(), node_id: op.node_id.clone(), incarnation_id: op.incarnation.clone(), name: op.location(), state: NodeState::Stopped };
                    let identity = DeclaredOperation { operation: op.operation.clone() };
                    let completed = self.declared.lock().unwrap().operations.contains_key(&operation_row(&declared, &identity).key());
                    if !start && !completed {
                        if let Some(digest_view) = &self.digest_view { let _ = set_member_status(digest_view, MemberStatusReceipt::StopAdmitted(&op)); }
                        tracing::info_span!("rdm.node_admin.node.update.via-stop-admitted", node_path = %op.path, node_name = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, operation = %op.operation.action, operation_subject = ?op.operation.subject).in_scope(|| tracing::info!("CommandAccepted reports the stop's admission; this mesh-primary sets stopping"));
                    }
                    // The node wrote its state on its own record before it sent the frame (Q356).
                    None
                }
                // node.stop calls its primitives and streams each one's acknowledgement and completion
                // (Q205); the node writes each state on its own record before it sends the frame (Q356).
                R::ConnectionsDeleting { .. } if !start => None,
                R::StopConnectionsDeleted { receipt } if !start => {
                    closure = Some(receipt.clone());
                    None
                }
                R::Leaving { .. } if !start => None,
                R::StopMeshLeft { .. } if !start => None,
                R::NodeJoined if start => {
                    let applied = self.node_joined(&op, node, &incarnation).await;
                    if !matches!(applied, StatusReply::Applied | StatusReply::AlreadyApplied) { return A::Indeterminate { code: <Status as NodeProtocol>::classify_reply(&applied).code(), reason: format!("NodeJoined not recorded: {applied:?}") }; }
                    joined_outcome = Some(applied.name());
                    joined = true;
                    None
                }
                R::NodeStopped if !start => {
                    if let Some(seam) = &self.command_frame_seam { seam.before_stopped(&op).await; }
                    let applied = self.stop_received(&op).await;
                    if !matches!(applied, StatusReply::Applied | StatusReply::AlreadyApplied) { return A::Indeterminate { code: <Status as NodeProtocol>::classify_reply(&applied).code(), reason: format!("NodeStopped not recorded: {applied:?}") }; }
                    Some(match closure.clone() { Some(receipt) => A::Stopped { receipt }, None => A::Indeterminate { code: Code::Internal, reason: "NodeStopped without StopConnectionsDeleted".into() } })
                }
                R::NodeStarted if start => {
                    // A start an earlier run of the same Build completed, reissued by this one, is
                    // answered NodeStarted with no NodeJoined: the birth joined when that start ran, at
                    // its executor then, which may never have published the clear. A mesh-primary whose
                    // overlay holds that exact birth stopped takes the answer as the join it did not see:
                    // the stop mark clears in its overlay
                    // and the birth is held ready again; no row is written for it and no join is made
                    // (node-start.md step 2, Q249).
                    if !joined {
                        if let Some(digest_view) = self.digest_view.as_ref().filter(|v| v.holds_stopped(op.node_id.as_str(), &op.incarnation)) {
                            if matches!(set_member_status(digest_view, MemberStatusReceipt::NodeJoined { op: &op, already: Some(NodeState::Ready) }), Ok(true)) {
                                joined_outcome = Some("stop-mark-cleared");
                            }
                        }
                    }
                    let row = crate::status_rpc::Declaration { r#type: node.r#type.clone(), mesh: node.mesh.clone(), node_id: node.node_id.clone(), incarnation_id: incarnation.clone(), name: node.location(), state: NodeState::Ready };
                    let operation = DeclaredOperation { operation: ctx.operation.clone() };
                    let applied = self.persist_then_commit(Effect::Node(row, NodeState::Ready, Some(operation))).await;
                    Some(if matches!(applied, StatusReply::Applied | StatusReply::AlreadyApplied) { A::Started {} } else { A::StartFailed { code: <Status as NodeProtocol>::classify_reply(&applied).code(), step: "node.started".into(), reason: format!("Ready not recorded: {applied:?}") } })
                }
                R::Failed { step, reason, code } => Some(A::StartFailed { step: step.clone(), reason: reason.clone(), code: *code }),
                _ => Some(if accepted { A::Indeterminate { code: Code::Internal, reason: format!("unexpected {} after acceptance", frame.name()) } } else { A::Refused { code: NodeCommand::classify_reply(&frame).code(), reply: format!("{}: {frame:?}", frame.name()) } }),
            };
            let span = tracing::info_span!("rdm.node_admin.node.update.via-command-frame", recorder_node_id = %self.node_id, node_path = %node.path, node_name = %node.name, node_id = %node.node_id, incarnation_id = %incarnation.as_str(), operation = %ctx.operation.action, operation_subject = ?ctx.operation.subject, frame = frame.name(), invocation_id = tracing::field::Empty, seq = tracing::field::Empty, code = NodeCommand::classify_reply(&frame).code().name(), detail = NodeCommand::reply_detail(&frame), outcome = tracing::field::Empty);
            if let Some((invocation, seq)) = position { span.record("invocation_id", invocation.to_string()).record("seq", seq); }
            if let Some(outcome) = joined_outcome { span.record("outcome", outcome); }
            span.in_scope(|| tracing::info!("the mesh-primary recorded the node's command frame before acknowledging its recovery"));
            // Recorded, the frame goes on to the caller whose node command this primary runs on the birth
            // (node-stop.md "Upward": two one-hop streams, never a forwarded one).
            self.commands.on_to_caller(&node.node_id, &incarnation, &frame);
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
        // The command is over with its carry, admitted or not: its step receipt is the record
        // (node-recovery.md step 2 reads "open" as from the send to the completion).
        if let Some(incarnation) = node.incarnation_id.clone() {
            self.commands.close(&crate::node_commands::CommandKey { node_id: node.node_id.clone(), incarnation, operation: ctx.operation.clone() });
        }
        (admission, unreachable)
    }

    /// `node.connection.delete` of a birth of this admin's mesh, carried in-process: the mesh-primary's
    /// own pipeline step (i144 R22). The birth is read from this admin's view.
    pub async fn carry_connections_delete(&self, node_id: &NodeId, incarnation: &IncarnationId, scope: &rdm_core::status::ConnectionScope) -> CarryEnd {
        let view = self.topology.read().await.clone();
        self.carry_delete_connections(&view, None, node_id, incarnation, scope).await
    }

    /// `node.connection.delete` naming a birth of this admin's mesh (R-CR2, R-CR3; i144 R22, Luke Q6,
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
            "rdm.node_admin.connection.delete.via-mesh-primary",
            node_path = %me_name.path, node_name = %me_name.name, subject_id = %node_id, subject = tracing::field::Empty, sender = %sender.map(|n| n.node_id.to_string()).unwrap_or_default(),
            scope = scope.name(), status_at_cut = tracing::field::Empty, carried = tracing::field::Empty, closed = tracing::field::Empty, held = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal"
        );
        let end = async {
            // A subject this receiver does not hold is the receiver's own fact (`SubjectUnknown`), never
            // a peer that did not resolve.
            let Some(subject) = view.nodes.iter().find(|n| n.node_id == *node_id) else {
                tracing::Span::current().record("detail", format!("path {me_name_path:?}, name {me_name_name:?}: node {node_id} is not in the mesh-primary's view; its connections cannot be deleted through it", me_name_path = &me_name.path, me_name_name = &me_name.name).as_str());
                return CarryEnd::Refused(rdm_core::reply::node_command::rejected("unknown-node", format!("node {node_id} is not in the view of mesh-primary {}", self.node_id)));
            };
            tracing::Span::current().record("subject", subject.node_id.to_string().as_str()).record("status_at_cut", format!("{:?}", subject.status).as_str());
            if !view.own(&self.node_id).is_some_and(|n| n.is_primary && n.mesh == subject.mesh) {
                return CarryEnd::Refused(rdm_core::reply::node_command::rejected("receiver-not-primary", format!("node {} is not the mesh-primary of {}", self.node_id, subject.mesh)));
            }
            if subject.incarnation_id.as_ref() != Some(incarnation) {
                return match subject.incarnation_id.clone() {
                    Some(held) => CarryEnd::Refused(rdm_core::reply::node_command::rejected_stale_incarnation(held)),
                    None => {
                        tracing::Span::current().record("detail", format!("path {me_name_path:?}, name {me_name_name:?}: {} has no known birth; its connections cannot be deleted", subject.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name).as_str());
                        CarryEnd::Refused(rdm_core::reply::node_command::rejected("unknown-node", format!("node {} has no known birth", subject.node_id)))
                    }
                };
            }
            if let Some(reason) = scope.refusal() {
                return CarryEnd::Refused(rdm_core::reply::node_command::rejected("rejected-connection-scope", format!("{}: {reason}", subject.node_id)));
            }
            let all = matches!(scope, rdm_core::status::ConnectionScope::All);
            let command = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: scope.clone() };
            let mut frames = if *node_id == self.node_id {
                // This mesh-primary is its own subject: an endpoint does not dial itself, so its own
                // birth's command path runs in-process and its frames come back on a channel.
                let Some(own) = self.own.get().cloned() else {
                    return CarryEnd::Refused(rdm_core::reply::node_command::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} has not yet joined its mesh; node.connection.delete of its own birth is refused and nothing is cut", me_name_path = &me_name.path, me_name_name = &me_name.name)));
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
                    return CarryEnd::Refused(rdm_core::reply::node_command::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} has no node-RPC client yet to carry node.connection.delete to {}; nothing is cut", subject.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name)));
                };
                tracing::Span::current().record("carried", "dialled");
                let target = rdm_transport::NodeTarget::ExactNode(node_id.clone());
                let opts = rdm_transport::CallOptions { send: rdm_transport::Send::with_time_limit(self.commands.connections_delete_reply() + rdm_transport::TRANSPORT_LIMIT), ..Default::default() };
                match configs.client().call_stream::<NodeCommand>(&target, &command, &opts).await {
                    Ok((stream, _)) => CarriedFrames::Dialled(stream),
                    Err((out, _)) => {
                        let end = match out {
                            rdm_core::outcome::RpcOutcome::NotSent(n) => CarryEnd::NotSent(n.reason().clone()),
                            rdm_core::outcome::RpcOutcome::Reply(r) => CarryEnd::RefusedByNode(r.into_value()),
                            other => CarryEnd::Lost { code: other.code(), reason: format!("path {me_name_path:?}, name {me_name_name:?} carried node.connection.delete to {} and the call ended {} before the node accepted it: {other:?}", subject.node_id, other.name(), me_name_path = &me_name.path, me_name_name = &me_name.name) },
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
                    Ok(frame @ NodeCommandReply::CommandAccepted { .. }) => {
                        accepted = true;
                        // Each recorded frame goes on to the caller whose node command this primary runs on
                        // the birth (node-connections-delete.md; D40).
                        self.commands.on_to_caller(node_id, incarnation, &frame);
                        frames.accepted().await;
                    }
                    Ok(frame @ NodeCommandReply::ConnectionsDeleting { .. }) => {
                        self.commands.on_to_caller(node_id, incarnation, &frame);
                        // The node reported it, its own record written first (Q356): this mesh-primary
                        // publishes the state the node entered, never before.
                        reported = true;
                        let recorded = self.digest_view.as_ref().is_some_and(|digest_view| set_member_status(digest_view, MemberStatusReceipt::ConnectionsDeleting { node_id, incarnation }) == Ok(true));
                        let position = frames.position();
                        let span = tracing::info_span!("rdm.node_admin.node.update.via-connections-deleting", invocation_id = tracing::field::Empty, seq = tracing::field::Empty, node_path = %subject.path, node_name = %subject.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), recorder_node_id = %self.node_id, reported_by = "node", recorded, "otel.kind" = "internal");
                        if let Some((invocation, seq)) = position {
                            span.record("invocation_id", invocation.to_string()).record("seq", seq);
                        }
                        span.in_scope(|| tracing::info!("the node reported connections-deleting on its command's stream: this mesh-primary records it, and its projection carries it from its next checklist"));
                        frames.accepted().await;
                    }
                    Ok(NodeCommandReply::ConnectionsDeleted { closed, not_open, held, receipt }) => {
                        self.commands.on_to_caller(node_id, incarnation, &NodeCommandReply::ConnectionsDeleted { closed: closed.clone(), not_open: not_open.clone(), held: held.clone(), receipt: receipt.clone() });
                        tracing::Span::current().record("closed", closed.len()).record("held", held.len());
                        let kept = ConnectionsDeletedReceipt { all, held: held.clone(), status_at_cut: if all { crate::model::NodeStatus::ConnectionsDeleting } else { subject.status } };
                        self.declared.lock().unwrap().connections_deleted.insert((node_id.clone(), incarnation.clone()), kept);
                        if let Some(r) = &receipt {
                            // Established completes the closure; Deadline leaves the birth connections-deleting
                            // with its incomplete receipt (node-connections-delete.md steps 4-5).
                            tracing::info_span!("rdm.node_admin.node.update.via-connections-deleted", node_path = %subject.path, node_name = %subject.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), recorder_node_id = %self.node_id, receipt = r.name(), held = held.len(), "otel.kind" = "internal")
                                .in_scope(|| tracing::info!("the node's node.connections.deleted is held as the birth's receipt"));
                        }
                        frames.accepted().await;
                        return CarryEnd::Deleted { closed, not_open, held, receipt };
                    }
                    Ok(other) => {
                        let reason = format!("{} answered {} on its command's stream after {}", subject.node_id, other.name(), if accepted { "accepting it" } else { "nothing" });
                        tracing::Span::current().record("detail", reason.as_str());
                        frames.accepted().await;
                        return if accepted { CarryEnd::Incomplete { reported, reason, code: <NodeCommand as rdm_core::protocol::NodeProtocol>::classify_reply(&other).code() } } else { CarryEnd::RefusedByNode(other) };
                    }
                    Err(f) => {
                        let code = f.code();
                        let reason = format!("path {me_name_path:?}, name {me_name_name:?}'s node.connection.delete stream to {} broke {}: {f:?}", subject.node_id, if accepted { "after the node accepted it" } else { "before the node accepted it" }, me_name_path = &me_name.path, me_name_name = &me_name.name);
                        tracing::Span::current().record("detail", reason.as_str());
                        return if accepted { CarryEnd::Incomplete { reported, reason, code } } else { CarryEnd::Lost { reason, code } };
                    }
                }
            }
            let reason = format!("path {me_name_path:?}, name {me_name_name:?}'s node.connection.delete stream to {} ended without its completion", subject.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name);
            if accepted { CarryEnd::Incomplete { reported, reason, code: rdm_core::outcome::Code::Unknown } } else { CarryEnd::Lost { reason, code: rdm_core::outcome::Code::Unknown } }
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", end.name());
        span.in_scope(|| tracing::info!("node.connection.delete carried by the mesh-primary"));
        end
    }

    /// A command naming this admin, served on the target and the exact birth it names (node-rpc.md
    /// §14): the commander is the caller as the view holds it, where its completion goes; the
    /// connection spared is the call's own (`peer`).
    async fn serve_command(&self, view: &Topology, sender: Option<&crate::model::Node>, peer: Option<iroh::PublicKey>, req: &StatusRequest) -> StatusReply {
        let (StatusRequest::StopNode { incarnation, .. } | StatusRequest::StartNode { incarnation, .. } | StatusRequest::JoinMesh { incarnation, .. }) = req else {
            unreachable!("serve_command is called for commands only")
        };
        let me_name = self.held_name_in(view);
        // The exact birth the call names is checked.
        let me = view.own(&self.node_id);
        // A commanded node.mesh.join runs the one election over the commander's record first (D57).
        let elected = match (req, me) {
            (StatusRequest::JoinMesh { primary, .. }, Some(me)) if me.incarnation_id.as_ref() == Some(incarnation) => self.join_elects(me, primary.as_ref()),
            _ => None,
        };
        let reply = match (me, self.own.get()) {
            _ if elected.is_some() => elected.expect("checked"),
            (Some(me), _) if me.incarnation_id.as_ref().is_some_and(|held| held != incarnation) => rdm_core::reply::stale(me.incarnation_id.clone().expect("checked")),
            (_, Some(own)) => own.serve(sender.map(|n| &n.node_id), peer, req).await.expect("a command"),
            (_, None) => rdm_core::reply::status::not_ready(format!("path {me_name_path:?}, name {me_name_name:?} has not yet joined its mesh; {} is refused", req.op(), me_name_path = &me_name.path, me_name_name = &me_name.name)),
        };
        tracing::info_span!("rdm.node_admin.status.update.via-command", node_path = %me_name.path, node_name = %me_name.name, op = req.op(), sender = %sender.map(|n| n.node_id.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("a node command naming this admin was decided"));
        reply
    }

    /// The one election a commanded node-admin runs before it joins (D57; Q335: "they both take each
    /// other's ids and run the election"): it takes the commander's held mesh-primary record of its mesh
    /// by the supersedes rule, then reads the record it holds. Both admins live (one commands, the other
    /// answers), so the record decides between them. Named: the command is refused by name and it joins
    /// no one (`Some`, the refusal). Not named: `None`; its own record is superseded, and the start that
    /// follows drops its primary role before node.mesh.join (`AdminSession::rejoin`). One span,
    /// `rdm.mesh.election.resolve.via-commanded-join`.
    fn join_elects(&self, me: &crate::model::Node, commander: Option<&rdm_core::Primary>) -> Option<StatusReply> {
        let channels = self.topology_view.as_ref()?;
        let incarnation = me.incarnation_id.as_ref()?;
        let primaries = channels.primaries();
        let taken = commander.map(|r| match primaries.take(rdm_core::PrimaryRole::MeshPrimary, r) {
            rdm_transport::topology::RoleTaken::Held { .. } => "held",
            rdm_transport::topology::RoleTaken::Same => "same",
            rdm_transport::topology::RoleTaken::Refused { .. } => "refused",
        });
        let held = primaries.mesh(&me.mesh);
        let named = held.as_ref().is_some_and(|h| h.is_birth(&self.node_id, incarnation));
        tracing::info_span!(
            "rdm.mesh.election.resolve.via-commanded-join",
            node_path = %me.path, node_name = %me.name,
            node_id = %self.node_id,
            mesh = %me.mesh,
            commander_primary = %commander.map(|r| r.to_string()).unwrap_or_default(),
            commander_record = taken.unwrap_or("none"),
            held_primary = %held.as_ref().map(|r| r.to_string()).unwrap_or_default(),
            named,
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!(named, "a commanded node-admin ran its election over the commander's record"));
        named.then(|| {
            let revision = held.as_ref().map(|h| h.revision).unwrap_or_default();
            rdm_core::reply::status::rejected_not_primary(rdm_core::status::NotPrimary::ReceiverNotPrimary {
                needed: format!("a node-admin of {} its election does not name (this admin is {}'s mesh-primary by revision {revision})", me.mesh, me.mesh),
            })
        })
    }

    /// An `Applied` decision: the fact's row is put and acknowledged, and only then is it held as
    /// applied and answered. A refused put answers `NotReady` naming the store and holds nothing,
    /// so the same declaration again is decided afresh. The commit decides again over what is
    /// held now, so two declarations of one natural key racing each other answer `Applied` once.
    async fn persist_then_commit(&self, effect: Effect) -> StatusReply {
        let operation = matches!(&effect, Effect::Node(_, _, Some(_)));
        let _operation = if operation { Some(self.operation_effects.write().await) } else { None };
        let _state = if matches!(&effect, Effect::Node(_, _, None)) { Some(self.operation_effects.read().await) } else { None };
        self.commit_held(effect).await
    }

    /// `persist_then_commit` under the hold of `operation_effects` its caller took.
    async fn commit_held(&self, effect: Effect) -> StatusReply {
        let operation = match &effect { Effect::Node(row, _, Some(op)) => Some(operation_row(row, op)), _ => None };
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
        if let Effect::Mesh(mesh_id, state, named, at) = &effect {
            let Some(mesh) = named else {
                // A mesh-primary's declaration at the fabric-primary: decided and held here, written by
                // the mesh's own primary on its record (one writer per object; Q144), with the instant
                // its MeshStatus carries for the change.
                self.declared.lock().unwrap().hold_mesh_at(mesh_id.clone(), *state, *at);
                return rdm_core::reply::applied();
            };
            let reply = commit_mesh_state(&*self.meshes_storage, &self.caches, &self.declared, mesh, mesh_id, *state, *at).await;
            // The hand-off's state is handed straight to this admin's own mesh StatusPublisher as its
            // adopted held fact at the instant it carries, never through the gossip receive view, so the
            // publisher republishes it with that changed_at once its checklist completes
            // (node-create.md "A node-admin created into a live mesh" step 4: "republishes its held MeshStatus, preserving changed_at").
            // A hand-off with no instant (a new mesh's first admin handed Pending) reaches the publisher
            // too: it is the mesh's first state, which the publisher stamps once (Q272).
            if matches!(reply, StatusReply::Applied | StatusReply::AlreadyApplied) {
                if let Some(view) = &self.topology_view {
                    let taken = view.adopt_mesh_status(rdm_transport::topology::StatusFact { status: mesh_status_name(*state).to_string(), changed_at_rafka_ms: *at });
                    self.wake.report_notify.notify_waiters();
                    tracing::info_span!("rdm.node_admin.mesh.update.via-held-mesh-status", mesh = %mesh, state = ?state, changed_at_rafka_ms = *at, taken).in_scope(|| tracing::info!("the hand-off's state is held as this admin's MeshStatus at the instant it carries"));
                }
            }
            return reply;
        }
        if let Effect::Fabric(fabric_id, event) = &effect {
            let writes = self.topology.read().await.fabric_primary().is_some_and(|f| f.node_id == self.node_id);
            let reply = if writes {
                let name = self.topology.read().await.fabric.name.clone();
                commit_fabric_event(&*self.fabric_storage, &self.caches, &self.declared, fabric_id, Some(&name), event).await
            } else {
                hold_written_fabric_event(&*self.fabric_storage, &self.declared, fabric_id, event).await
            };
            return reply;
        }
        // A node's declared state is on its own record already: the node writes it before it declares
        // it (Q356); this admin holds it in its view and atlas.
        let reply = {
            let mut declared = self.declared.lock().unwrap();
            if let Some(row) = &operation { declared.operations.insert(row.key(), row.clone()); }
            match &effect {
                Effect::Node(row, state, _) => match transition(declared.node(&row.node_id, &row.incarnation_id), *state) {
                    Transition::Apply => {
                        declared.hold_node((row.node_id.clone(), row.incarnation_id.clone()), *state);
                        declared.names.insert((row.node_id.clone(), row.incarnation_id.clone()), (row.name.clone(), row.mesh.clone()));
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
            // projection.
            if let Some(digest_view) = &self.digest_view {
                let _ = set_member_status(digest_view, MemberStatusReceipt::Declared(row));
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
        // publishes nothing.
        if let (StatusReply::Applied, Effect::Node(row, NodeState::Stopped, Some(operation))) = (&reply, &effect) {
            if let Some(publish) = self.stopped.get() {
                publish(row.clone(), operation.clone()).await;
            }
        }
        reply
    }

    /// The stop stream's `stopped` of `op`'s exact birth, received by this mesh-primary as the stop's
    /// executor (node-stop.md steps 4-5): its Leaving is recorded in the same declared-state row the
    /// birth's own declaration names, so NodeStopped is published once, from whichever arrived first.
    pub async fn stop_received(&self, op: &rdm_core::LifecycleOp) -> StatusReply {
        let row = crate::status_rpc::Declaration { r#type: op.r#type.clone(), mesh: op.mesh.clone(), node_id: op.node_id.clone(), incarnation_id: op.incarnation.clone(), name: op.location(), state: NodeState::Stopped };
        let operation = DeclaredOperation { operation: op.operation.clone() };
        let reply = self.persist_then_commit(Effect::Node(row, NodeState::Stopped, Some(operation))).await;
        tracing::info_span!("rdm.node_admin.node.update.via-stop-received", node_path = %op.path, node_name = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, operation = %op.operation.action, operation_subject = ?op.operation.subject, outcome = reply.name(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the stop stream's stopped is recorded in the birth's declared-state row"));
        reply
    }

    /// The admin with no upstream fabric-primary applies its own mesh's state (Q272): the Day-0 root its
    /// new mesh's `Pending`, with no instant (`at` 0), the mesh's first state; the adoption launch its
    /// Mesh record's state at its own instant, which its publisher adopts and republishes once its
    /// checklist completes, as any successor's (node-create.md "A node-admin created into a live mesh" step 4). The applied state is held
    /// as this admin's MeshStatus, so its publisher holds what it sends.
    pub(crate) async fn self_apply_mesh(&self, mesh_id: &MeshId, state: MeshState, at: u64) -> StatusReply {
        let me_name = self.held_name().await;
        let receiver_is_primary = self.topology.read().await.own(&self.node_id).is_some_and(|n| n.is_primary);
        let (decided, effect) = apply_mesh(&self.declared.lock().unwrap(), mesh_id, state, at);
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => decided,
        };
        let day0 = state == MeshState::Pending && at == 0;
        let mut taken = false;
        if matches!(reply, StatusReply::Applied | StatusReply::AlreadyApplied) {
            if let Some(view) = &self.topology_view {
                taken = view.adopt_mesh_status(rdm_transport::topology::StatusFact { status: mesh_status_name(state).to_string(), changed_at_rafka_ms: at });
                self.wake.report_notify.notify_waiters();
            }
        }
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node_path = %me_name.path, node_name = %me_name.name,
            op = "apply-mesh-state",
            state = ?state,
            changed_at_rafka_ms = at,
            taken,
            sender_node_id = %self.node_id,
            outcome = reply.name(),
            detail = if day0 { "self-applied: initial fabric bootstrap, no upstream authority" } else { "self-applied: the adoption launch, its Mesh record's state" },
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("the admin with no upstream fabric-primary applies its own mesh's state"));
        reply
    }
}

/// Serve `Status` on this admin. `authority` is filled once the admin holds a view; until then a
/// declaration is `NotReady` by name.

/// Where a node's own `pre_node_ready` stands: `None` until its start registered a hook.
pub type PreNodeReadyCell = Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>;

/// A node's re-publish of its presence (its digest published on the channel it is on), filled once it has joined its mesh.
pub type Republish = Arc<OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;
/// This node deciding a name its mesh-primary's row gives it (`ApplyNodeName`): the name it held
/// and the decision, the name taken and published only on `Applied`.
pub type Rename = Arc<OnceLock<Arc<dyn Fn(rdm_core::PathName) -> std::pin::Pin<Box<dyn std::future::Future<Output = (rdm_core::PathName, crate::node_name::NameApply)> + Send>> + Send + Sync>>>;

/// This mesh-primary's command of `node.mesh.join` to a renamed live birth: `(the birth at its new
/// name, its incarnation)`, answered with the birth's reply or how the call ended.
pub type Rejoin = Arc<OnceLock<Arc<dyn Fn(crate::model::Node, rdm_core::IncarnationId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<StatusReply, String>> + Send>> + Send + Sync>>>;

/// This mesh-primary's view taking a row `node.name.update` moved: `(from, node_id, incarnation, to)`.
pub type RowMoved = Arc<OnceLock<Arc<dyn Fn(rdm_core::PathName, NodeId, rdm_core::IncarnationId, rdm_core::PathName) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// The `rdm.node_admin.node.update.via-row-status` span of a status write: `written` when the record
/// moved, `unchanged` when it already stood there, `no-record` when no record of the node is held,
/// `refused` naming the store's error. The record carries no revision (node-identity-and-state.md
/// §2.5): its writes are made one at a time under the node's lock, and each write's span is opened
/// under that same lock ([`crate::node_rows::WrittenRows::update_spanned`]), so the spans follow the
/// order of the writes.
pub(crate) fn row_status_span(me: &crate::model::PathName, subject: &crate::model::PathName, node_id: &NodeId, incarnation: &IncarnationId, status: crate::model::NodeStatus, written: &Result<Option<bool>, String>) {
    let outcome = match written {
        Ok(Some(true)) => "written",
        Ok(Some(false)) => "unchanged",
        Ok(None) => "no-record",
        Err(_) => "refused",
    };
    row_status_span_as(me, subject, node_id, incarnation, status, outcome, written.as_ref().err().map(String::as_str).unwrap_or(""));
}

/// The `rdm.node_admin.node.update.via-row-status` span of a status `me` refused before any write
/// ([`crate::node_rows::NodeRows::status`]): its outcome names the refusal (`not-mesh-primary`), its
/// `error` why, naming the writer.
pub(crate) fn row_status_refused_span(me: &crate::model::PathName, subject: &crate::model::PathName, node_id: &NodeId, incarnation: &IncarnationId, status: crate::model::NodeStatus, refused: &crate::node_rows::RowRefused) {
    row_status_span_as(me, subject, node_id, incarnation, status, refused.outcome(), &refused.to_string());
}

fn row_status_span_as(me: &crate::model::PathName, subject: &crate::model::PathName, node_id: &NodeId, incarnation: &IncarnationId, status: crate::model::NodeStatus, outcome: &str, error: &str) {
    tracing::info_span!("rdm.node_admin.node.update.via-row-status", node_path = %me.path, node_name = %me.name, subject_path = %subject.path, subject_name = %subject.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), status = ?status, outcome, error = %error, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the node's status is written on its record"));
}

/// The status a node's row carries for the state its birth declared.
pub fn node_status_of(s: NodeState) -> crate::model::NodeStatus {
    use crate::model::NodeStatus as S;
    match s {
        NodeState::Pending => S::Pending,
        NodeState::Ready => S::Ready,
        NodeState::ConnectionsDeleting => S::ConnectionsDeleting,
        NodeState::Stopped => S::Stopped,
    }
}

/// What a node is now, in the declaration vocabulary.
pub fn node_state_of(s: crate::model::NodeStatus) -> NodeState {
    use crate::model::NodeStatus as S;
    match s {
        S::Pending => NodeState::Pending,
        S::Ready => NodeState::Ready,
        S::ConnectionsDeleting => NodeState::ConnectionsDeleting,
        S::Starting => NodeState::Pending,
        S::ConnectionsDeleted | S::Stopping | S::Left => NodeState::ConnectionsDeleting,
        S::Leaving | S::Reserved5 | S::PendingReconnect | S::Dead | S::Deleted | S::Withdrawn | S::Stopped | S::Deleting => NodeState::Stopped,
    }
}

/// The downward exact-node operations, answered by the birth they reach, whoever sends them
/// (node-rpc.md §14) and whatever node id they name (R-RX5). `Some(reply)` for those requests;
/// `None` for every other request.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// re-publishes its presence and answers its current state; no transition. While its
/// `pre_node_ready` hook answered `Pending` it answers `Pending` with the hook's reason, and once its
/// start ended Failed `StartFailed`, and re-publishes nothing. `ApplyNodeState` is not served: a node's connections are deleted by `node.connection.delete` and a stop is `stop-node`.
#[allow(clippy::too_many_arguments)]
async fn self_subject(me: &crate::model::Node, sender: Option<&crate::model::Node>, peer: Option<iroh::PublicKey>, req: &StatusRequest, republish: &Republish, rename: &Rename, pre_node_ready: &PreNodeReadyCell, wake: &crate::status_declare::DeclareWake) -> Option<StatusReply> {
    let incarnation = match req {
        StatusRequest::ProbeNodeState { incarnation, .. } | StatusRequest::ApplyNodeState { incarnation, .. } | StatusRequest::ApplyNodeName { incarnation, .. } => incarnation,
        _ => return None,
    };
    // The exact birth the call names is checked, so a call that reached another birth is refused
    // naming the one held.
    let Some(held) = me.incarnation_id.as_ref() else {
        return Some(rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown));
    };
    if held != incarnation {
        return Some(rdm_core::reply::stale(held.clone()));
    }
    let sender_name = sender.map(|n| n.node_id.to_string()).unwrap_or_default();
    match req {
        StatusRequest::ProbeNodeState { .. } => {
            // The hook's answer as it stood when the probe arrived.
            let hook_answer = pre_node_ready.lock().unwrap().as_ref().and_then(|h| h.state().probe_answer());
            // The authority addressed this admin (R-S3's down op): every declaration owed to it is
            // sent again, whatever its last outcome.
            wake.addressed_by(sender.map(|n| &n.node_id));
            // A stopped admin presents nothing: it says it is stopped.
            if crate::node_self::process_stop().is_stopped() {
                tracing::info_span!("rdm.node_admin.status.update.via-probe", node_path = %me.path, node_name = %me.name, sender = %sender_name, state = "stopped", "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("probed by a node-admin: stopped, nothing re-published"));
                return Some(StatusReply::CurrentStopped { node_id: me.node_id.clone(), incarnation: held.clone() });
            }
            // node-rpc-envelope.md, Status: while its hook answered Pending the probe served this exact
            // node and found an active readiness blocker, answered Pending with the hook's own reason;
            // once its start ended Failed, StartFailed with the last reason. Either is answered before
            // any republish, so a polled pending node republishes nothing.
            if let Some(answer) = hook_answer {
                return Some(crate::node_hooks::probe_reply(&answer, &me.node_id, held, &sender_name));
            }
            // The check-in a probed birth owes (states.md "The check-in checklist": a new primary re-sends
            // the checklist and members already done check in again): its presence re-published on the
            // channel it is on, its state answered; no transition, no re-join. A birth its authority
            // holds off its mesh is commanded back through `node.mesh.join` (node-tickle.md, Q87).
            if let Some(republish) = republish.get() {
                republish().await;
            }
            let state = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.update.via-probe", node_path = %me.path, node_name = %me.name, sender = %sender_name, state = ?state, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
            Some(StatusReply::Current { node_id: me.node_id.clone(), incarnation: held.clone(), state })
        }
        StatusRequest::ApplyNodeState { state, .. } => {
            // Not node.connection.delete and not a stop: neither is set by apply-node-state. The reply
            // carries the state this node holds; the span names why.
            let current = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.reject.via-apply-node-state", node_path = %me.path, node_name = %me.name, sender = %sender_name, requested = ?state, current = ?current, reason = "node.connection.delete is the only path to connections-deleting and stop-node the only stop; apply-node-state is served for no state", "otel.kind" = "internal")
                .in_scope(|| tracing::info!("apply-node-state refused"));
            Some(rdm_core::reply::status::rejected_invalid_node_transition(current))
        }
        StatusRequest::ApplyNodeName { name, .. } => {
            // R12, R-CR1: the node owns its name and takes the row's name only while it holds no connection.
            let asked = match rdm_core::PathName::new(name.path.clone(), name.name.clone()) {
                Ok(n) => n,
                Err(e) => {
                    tracing::info_span!("rdm.node_admin.node.reject.via-apply-node-name", node_path = %me.path, node_name = %me.name, sender = %sender_name, to_path = %name.path, to_name = %name.name, reason = %e, "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("apply-node-name refused: invalid path or name"));
                    return Some(rdm_core::reply::status::malformed(rdm_core::outcome::MalformedKind::Corrupt));
                }
            };
            let Some(rename) = rename.get() else {
                return Some(rdm_core::reply::status::not_ready(format!("node {}: its digest does not exist yet; path {:?}, name {:?} is not taken", me.node_id, asked.path, asked.name)));
            };
            let (held, decision) = rename(asked.clone()).await;
            let routes = match &decision {
                crate::node_name::NameApply::Busy { routes, .. } => routes.join(", "),
                _ => String::new(),
            };
            tracing::info_span!("rdm.node_admin.node.update.via-apply-node-name", node_path = %held.path, node_name = %held.name, sender = %sender_name, from_path = %held.path, from_name = %held.name, to_path = %asked.path, to_name = %asked.name, routes = %routes, outcome = decision.name(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the mesh-primary's name for this admin was decided"));
            Some(crate::node_name::reply(&decision))
        }
        _ => None,
    }
}

/// Serve `Status` on this admin, and the node command op 0x22 for this admin as a commanded node
/// (i144 R22). `service` is filled once the admin holds a view; until then a declaration is
/// `NotReady` by name.
pub fn serve(b: ServerBuilder, service: Arc<OnceLock<Arc<StatusService>>>) -> ServerBuilder {
    serve_node_command(b, service.clone()).serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: StatusRequest| {
        let service = service.clone();
        async move {
            let Some(service) = service.get().cloned() else {
                return Ok(rdm_core::reply::status::not_ready("this admin holds no view yet".into()));
            };
            let peer_id = EndpointId(peer.endpoint_id.to_string());
            let sender = service.sender_of(&peer_id).await;
            let reply = service.apply_from(Some(peer.endpoint_id), sender, &req).await;
            if service.hold_next_reply.swap(false, std::sync::atomic::Ordering::SeqCst) {
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
fn serve_node_command(b: ServerBuilder, service: Arc<OnceLock<Arc<StatusService>>>) -> ServerBuilder {
    use rdm_core::node_command::{NodeCommand, NodeCommandRequest};
    b.serve_stream::<NodeCommand, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: NodeCommandRequest, sink| {
        let service = service.clone();
        async move {
            let Some(service) = service.get().cloned() else {
                return Ok(rdm_core::reply::node_command::not_ready("this admin holds no view yet".into()));
            };
            let Some(own) = service.own.get().cloned() else {
                return Ok(rdm_core::reply::node_command::not_ready(format!("{} has not yet joined its mesh; {} is refused", service.node_id, req.op())));
            };
            let sender = service.sender_of(&EndpointId(peer.endpoint_id.to_string())).await;
            // `mesh.block` and `mesh.unblock`, the fabric-primary's commands to this mesh-primary's exact
            // birth in its storage change (storage.md §2 "Changing it", steps 1 and 4), are this admin's.
            if let NodeCommandRequest::BlockMesh { incarnation, change_id, from, to, .. } | NodeCommandRequest::UnblockMesh { incarnation, change_id, from, to, .. } = &req {
                if *incarnation != own.incarnation {
                    return Ok(rdm_core::reply::node_command::rejected_stale_incarnation(own.incarnation.clone()));
                }
                let Some(changes) = service.storage_changes.get().cloned() else {
                    return Ok(rdm_core::reply::node_command::not_ready(format!("{} holds no storage changes yet; {} is refused", service.node_id, req.op())));
                };
                let mut frames = crate::node_self::StreamFrames::new(sink);
                if crate::node_self::CommandFrames::accepted(&mut frames, rdm_core::node_command::NodeCommandReply::CommandAccepted { node_id: service.node_id.clone(), incarnation: own.incarnation.clone() }).await.is_err() {
                    return Ok(rdm_core::reply::node_command::not_ready(format!("the caller left before {} was accepted", req.op())));
                }
                let change = rdm_core::build::StorageChange { change_id: change_id.clone(), from: from.clone(), to: to.clone() };
                // Accepted: the stream ends with the command's terminal or `Failed`, never a refusal
                // (node-rpc-envelope.md, NodeCommand op 0x22).
                let reply = match &req {
                    NodeCommandRequest::BlockMesh { .. } => rdm_core::reply::node_command::after_acceptance("mesh.block", changes.serve_block(&change).await),
                    _ => rdm_core::reply::node_command::after_acceptance("mesh.unblock", changes.serve_unblock(&change).await),
                };
                tracing::info_span!("rdm.node_admin.status.update.via-command", node_path = %service.me.path, node_name = %service.me.name, op = req.op(), sender = %sender.map(|n| n.node_id.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a node command naming this admin was decided"));
                return Ok(reply);
            }
            // A caller's node.stop, node.start, node.mesh.leave or node.connection.delete of any scope (D40, D18, D61; Q342): of another birth, or of this one from a
            // sender no node-admin holds (the fabric-primary is the primary above itself), it is this
            // admin's to run as the primary above the node. A node-admin's command to this birth is the
            // primary above it commanding it, served by the birth itself.
            let caller_command = match &req {
                NodeCommandRequest::StopNode { node_id, .. } | NodeCommandRequest::StartNode { node_id, .. } | NodeCommandRequest::DeleteConnections { node_id, .. } => *node_id != service.node_id || !sender.as_ref().is_some_and(|n| rdm_core::is_node_admin(&n.r#type)),
                _ => false,
            };
            if caller_command {
                let Some(commands) = service.primary_commands.get().cloned() else {
                    return Ok(rdm_core::reply::node_command::not_ready(format!("{} holds no NodeCommandService yet; {} is refused", service.node_id, req.op())));
                };
                return Ok(commands.run(sender.as_ref().map(|n| n.node_id.clone()), req, Box::new(crate::node_self::StreamFrames::new(sink))).await);
            }
            let reply = own.serve_node_command_on(sender.as_ref().map(|n| &n.node_id), Some(peer.endpoint_id), &req, Box::new(crate::node_self::StreamFrames::new(sink))).await;
            tracing::info_span!("rdm.node_admin.status.update.via-command", node_path = %service.me.path, node_name = %service.me.name, op = req.op(), sender = %sender.map(|n| n.node_id.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
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
        StatusReply::RejectedNotPrimary { why } => why.as_str().to_string(),
        StatusReply::RejectedStaleIncarnation { held } => held.to_string(),
        StatusReply::RejectedStaleMesh { held } => held.to_string(),
        StatusReply::RejectedStaleFabric { held } => held.to_string(),
        StatusReply::RejectedInvalidNodeTransition { current } => format!("{current:?}"),
        StatusReply::RejectedInvalidMeshTransition { current } => format!("{current:?}"),
        _ => String::new(),
    }
}

/// How a carried `node.connection.delete` ended (i144 R22, Luke Q6, Q7): an in-process answer, never
/// a wire reply. Each end is its own fact: nothing was sent, the node refused, the call was lost before
/// the node accepted, the stream broke after it accepted (incomplete), or the node completed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CarryEnd {
    /// This mesh-primary refused it before sending anything (a refusal it decided).
    Refused(rdm_core::node_command::NodeCommandReply),
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
            Self::Refused(reply) | Self::RefusedByNode(reply) => rdm_core::node_command::NodeCommand::classify_reply(reply).code(),
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
}

/// Shared handles, for the admin's wiring.
pub type StatusServiceSlot = Arc<OnceLock<Arc<StatusService>>>;

/// Publishes NodeStopped for the birth of `row` stopped by `operation` (i146 Q29, Q36).
pub type StoppedPublisher = Arc<dyn Fn(crate::status_rpc::Declaration, DeclaredOperation) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

#[allow(dead_code)]
fn _assert_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<StatusService>();
    let _ = RwLock::new(());
}

/// A plural read's refusal as Status answers it (the retained `GetFabric`).
fn status_of_read_refusal(refusal: rdm_core::read::ReadReply) -> StatusReply {
    use rdm_core::read::ReadReply;
    match refusal {
        ReadReply::RejectedFilter { reason } => rdm_core::reply::status::bounded_rejected_filter(&reason),
        ReadReply::RejectedSourceFailed { source, op, cause } => rdm_core::reply::status::bounded_source_failed(&source, &op, &cause),
        ReadReply::NotReady { reason } => rdm_core::reply::status::not_ready(reason),
        other => rdm_core::reply::status::not_ready(format!("fabric.get ended {}", other.name())),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: applied operation effects are keyed by exact birth and structured operation, its
    /// occurrence included, independent of the node's mutable name and lifecycle state.
    // @feature: node-records
    #[test]
    fn operation_effect_identity_retains_each_field_across_rename() {
        let id = NodeId::mint();
        let effect = OperationEffect {
            node_id: id.clone(), incarnation_id: IncarnationId::mint(),
            name: rdm_core::PathName::new("arbitrary.location", "worker").unwrap(),
            operation: DeclaredOperation { operation: rdm_core::OperationId::node("node.stop", &id).at_step(Some(3)) },
            state: NodeState::Pending,
        };
        let mut renamed = effect.clone();
        renamed.name.name = "worker-old".into();
        renamed.state = NodeState::Ready;
        assert_eq!(effect.key(), renamed.key());
        let mut variants = Vec::new();
        let mut other = effect.clone(); other.node_id = NodeId::mint(); variants.push(other);
        let mut other = effect.clone(); other.incarnation_id = IncarnationId::mint(); variants.push(other);
        let mut other = effect.clone(); other.operation.operation.occurrence = Some(rdm_core::OperationOccurrenceId::mint()); variants.push(other);
        let mut other = effect.clone(); other.operation.operation.step = Some(4); variants.push(other);
        let mut other = effect.clone(); other.operation.operation.action = "node.delete".into(); variants.push(other);
        let mut other = effect.clone(); other.operation.operation.subject = Some(rdm_core::OperationSubject::Node(NodeId::mint())); variants.push(other);
        assert!(variants.iter().all(|other| other.key() != effect.key()));
        let mut applied = std::collections::HashMap::new();
        applied.insert(effect.key(), effect);
        applied.insert(renamed.key(), renamed);
        for other in variants { applied.insert(other.key(), other); }
        assert_eq!(applied.len(), 7);
    }


    use rdm_core::status::StatusReply as R;

    fn node(name: crate::model::PathName, mesh: &str, r#type: &str) -> crate::model::Node {
        let mut n = crate::model::Node::allocated(name, mesh, r#type);
        n.incarnation_id = Some(IncarnationId::mint());
        n
    }

    /// CONTRACT (i146 Q29, Q36; node-stop.md step 5): a birth's Leaving that names the operation which
    /// stopped it, applied by its mesh-primary, publishes NodeStopped once with that identity, whether
    /// the birth's declaration or the stop stream's `stopped` reached it first; the other finds the
    /// row held (`AlreadyApplied`) and publishes nothing. A Stopped naming no operation (a state the
    /// birth measured itself) publishes nothing. Must NOT happen: NodeStopped twice
    /// for one stop, or a lifecycle frame from a declaration that names no operation.
    #[tokio::test]
    async fn a_stops_leaving_publishes_node_stopped_once_from_whichever_reaches_the_mesh_primary_first() {
        let mut admin = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        admin.mesh = "mesh1".into();
        admin.r#type = "node-admin".to_string();
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let rpc = |name: rdm_core::PathName| {
            let mut n = node(name, "mesh1", "node");
            n.mesh = "mesh1".into();
            n.r#type = "node".to_string();
            n
        };
        let (declared_first, stream_first, measured) = (rpc(rdm_core::PathName::new("mesh1.node", "1").unwrap()), rpc(rdm_core::PathName::new("mesh1.node", "2").unwrap()), rpc(rdm_core::PathName::new("mesh1.node", "3").unwrap()));
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), declared_first.clone(), stream_first.clone(), measured.clone()],
        }));
        let service = StatusService {
            certs: None,
            topology_view: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.location(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(crate::storage::MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            primary_commands: Default::default(),
            storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            caches: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(), rejoin: Default::default(),
            loads: None,
            digest_view: None,
            unreachable: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let published: Arc<Mutex<Vec<(NodeId, rdm_core::OperationId)>>> = Arc::default();
        let into = published.clone();
        let publisher: StoppedPublisher = Arc::new(move |row: crate::status_rpc::Declaration, op: DeclaredOperation| {
            into.lock().unwrap().push((row.node_id, op.operation));
            Box::pin(async {})
        });
        let _ = service.stopped.set(publisher);
        // What a node declares: its stop's operation, keyed on its node.id, and nothing of a Build (Q342).
        let stop_of = |n: &crate::model::Node, kind: &str| DeclaredOperation { operation: rdm_core::OperationId::node(kind, &n.node_id) };
        let declare = |n: &crate::model::Node, operation: Option<DeclaredOperation>| StatusRequest::DeclareNodeState { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::Stopped, operation };
        let stream = |n: &crate::model::Node, op: &DeclaredOperation| rdm_core::LifecycleOp { r#type: n.r#type.clone(), mesh: n.mesh.clone(), build_id: rdm_core::LifecycleOp::NO_BUILD.into(), operation: op.operation.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), path: n.path.clone(), name: n.name.clone(), event_at_rafka_ms: 0, traceparent: None, tracestate: None };

        // The declaration first, then a repeat of it, then the stop stream's stopped.
        let stop = stop_of(&declared_first, "node.stop");
        assert_eq!(service.apply(Some(declared_first.clone()), &declare(&declared_first, Some(stop.clone()))).await, R::Applied);
        assert_eq!(service.apply(Some(declared_first.clone()), &declare(&declared_first, Some(stop.clone()))).await, R::AlreadyApplied, "a repeat of the declaration");
        assert_eq!(service.stop_received(&stream(&declared_first, &stop)).await, R::AlreadyApplied, "the stop stream's stopped after the declaration");
        // The stop stream's stopped first, then the declaration.
        let stop2 = stop_of(&stream_first, "node.stop");
        assert_eq!(service.stop_received(&stream(&stream_first, &stop2)).await, R::Applied);
        assert_eq!(service.apply(Some(stream_first.clone()), &declare(&stream_first, Some(stop2.clone()))).await, R::AlreadyApplied, "the declaration after the stop stream's stopped");
        // Q49: beginning a new cycle never forgets an operation already applied for the birth.
        service.begin_cycle(&stream_first, stream_first.incarnation_id.as_ref().unwrap()).await;
        let ready = StatusRequest::DeclareNodeState { node_id: stream_first.node_id.clone(), incarnation: stream_first.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        assert_eq!(service.apply(Some(stream_first.clone()), &ready).await, R::Applied);
        assert_eq!(service.apply(Some(stream_first.clone()), &declare(&stream_first, Some(stop2.clone()))).await, R::AlreadyApplied, "Q49: the old stop declaration after the next cycle began cannot apply again");
        assert_eq!(service.stop_received(&stream(&stream_first, &stop2)).await, R::AlreadyApplied, "Q49: the frame names the same applied operation");
        assert_eq!(service.apply(Some(stream_first.clone()), &declare(&stream_first, None)).await, R::Applied, "Q49: without an operation the normal forward transition still applies");
        // A Leaving the birth measured itself: applied, and nothing published.
        assert_eq!(service.apply(Some(measured.clone()), &declare(&measured, None)).await, R::Applied);

        assert_eq!(
            *published.lock().unwrap(),
            vec![(declared_first.node_id.clone(), stop.operation.clone()), (stream_first.node_id.clone(), stop2.operation.clone())],
            "NodeStopped once per stop, from whichever reached the mesh-primary first, and from nothing else"
        );

    }

    /// The status authority of `admin`, with `topology` as its view and `status` as its status store.
    /// Each node's record as its create's claim leaves it in the store: a join is accepted for a node
    /// that has one, and its mesh and kind are read from it.
    async fn claimed(status: &crate::storage::MemoryNodesStorage, nodes: &[&crate::model::Node]) {
        for n in nodes {
            let row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), n.incarnation_id.clone().unwrap_or_else(IncarnationId::mint), n.status);
            crate::storage::NodesStorage::put_node(status, &row).await.unwrap();
        }
    }

    fn mesh_primary_with_store(admin: &crate::model::Node, topology: Arc<tokio::sync::RwLock<Topology>>, fabric_id: FabricId, status: Arc<crate::storage::MemoryNodesStorage>) -> StatusService {
        StatusService {
            certs: None,
            primary_commands: Default::default(),
            topology_view: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.location(),
            node_id: admin.node_id.clone(),
            fabric_id,
            topology,
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(crate::storage::MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
            nodes_storage: status,
            caches: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(), rejoin: Default::default(),
            loads: None,
            digest_view: None,
            unreachable: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        }
    }

    /// CONTRACT (Q99, Q97; node-mesh-join.md step 10: the mesh-primary "records and publishes the node
    /// once as Pending when it accepts the node's join report, before it answers it. Every later report
    /// of that join, its retries and its `NodeJoined` included, belongs to that Pending and starts no
    /// other"; states.md, the node pending row: "once per join"): one Pending per accepted join, not
    /// per birth. A birth accepted once and still Pending (cut off before its hook passed, Q87) is
    /// commanded back: the held deployment its commanded join registers at the `JoinService`, and the same
    /// held deployment registered again by its next commanded join, each begin the birth's next cycle
    /// at their accepted report (its last cycle's cut forgotten) and record its Pending again, applied.
    /// A retry of a report a registration already heard applies as already applied and begins nothing.
    /// Must NOT happen: the same birth's next commanded join answered already applied because the birth
    /// is still Pending from its last join, its cycle never begun.
    #[tokio::test]
    async fn every_accepted_join_of_a_pending_birth_records_its_own_pending_and_a_retry_of_a_join_records_none() {
        use rdm_core::join::{JoinReply, JoinRequest};
        let mut admin = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        admin.r#type = "node-admin".to_string();
        admin.is_primary = true;
        let mut rpc = node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        rpc.r#type = "node".to_string();
        let birth = rpc.incarnation_id.clone().unwrap();
        let fabric_id = FabricId::mint();
        let fabric = crate::model::Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: crate::model::ScopeStatus::Pending, provider: crate::model::ProviderKind::Process };
        let topology = Arc::new(tokio::sync::RwLock::new(Topology { fabric, meshes: Vec::new(), nodes: vec![admin.clone(), rpc.clone()] }));
        let status = Arc::new(crate::storage::MemoryNodesStorage::default());
        let status_service = Arc::new(mesh_primary_with_store(&admin, topology, fabric_id.clone(), status.clone()));
        claimed(&status, &[&rpc]).await;
        // The `JoinService`, its accepted report's Pending recorded by this authority as boot wires it.
        let replies: Arc<Mutex<Vec<R>>> = Arc::default();
        let joins = Arc::new(crate::join::Joins::default());
        let join_service = crate::join::JoinService {
            nodes: None,
            me_path: admin.path.clone(), me_name: admin.name.clone(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(crate::wire::JoinAnswer {
                        served_by: NodeId::mint(),
                        control: crate::wire::JoinControl { cert_policy: None, provider: crate::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), member_cert_dates: None, signing_cert_dates: None, name: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(|_| {}),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new({
                let (status_service, replies) = (status_service.clone(), replies.clone());
                move |d: rdm_core::MeshDigest, new_join: bool| {
                    let (status_service, replies) = (status_service.clone(), replies.clone());
                    Box::pin(async move {
                        let reply = status_service.joined_pending(&d.node.node_id, &d.node.location(), &d.node.incarnation, new_join).await;
                        replies.lock().unwrap().push(reply);
                    })
                }
            }),
            primary: Arc::new(|| None),
            atlas: None,
            issuer: Arc::new(crate::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let first = crate::join::Deployed { mesh: rpc.mesh.clone(), r#type: rpc.r#type.clone(),
            path: rpc.path.clone(), name: rpc.name.clone(),
            node_id: rpc.node_id.clone(),
            incarnation: birth.clone(),
            supersedes: None,
            endpoint_id: EndpointId("rpc-endpoint".into()),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: "/data/mesh1.node.1".into(),
            last_accepted_name: None,
            held: false,
            listeners: Vec::new(),
        };
        let report = rdm_core::MeshDigest {
            fabric_id,
            node: rdm_core::MeshNode { mesh: rpc.mesh.clone(), r#type: rpc.r#type.clone(),
                node_id: rpc.node_id.clone(),
                path: rpc.path.clone(), name: rpc.name.clone(),
                endpoint_id: first.endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: birth.clone(),
                supersedes: None,
                runtime: Some(first.runtime.clone()),
            },
            status: rdm_core::MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(first.data_dir.clone()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        // The birth's last cycle's cut, as this mesh-primary holds it: a begun cycle forgets it.
        let cut = || { status_service.declared.lock().unwrap().connections_deleted.insert((rpc.node_id.clone(), birth.clone()), ConnectionsDeletedReceipt { all: true, held: Vec::new(), status_at_cut: crate::model::NodeStatus::ConnectionsDeleted }); };
        let cut_held = || status_service.declared.lock().unwrap().connections_deleted.contains_key(&(rpc.node_id.clone(), birth.clone()));
        let join_service = &join_service;
        let serve = move |what: &'static str| {
            let (report, replies) = (report.clone(), replies.clone());
            async move {
                let answered = join_service.serve(EndpointId("rpc-endpoint".into()), JoinRequest::JoinNode { digest: (&report).into(), listeners: Vec::new() }).await;
                assert!(matches!(answered, JoinReply::Joined { .. }), "{what}: {answered:?}");
                replies.lock().unwrap().pop().unwrap_or_else(|| panic!("{what}: the accepted report recorded no Pending"))
            }
        };

        // The first start's join (its create's registration).
        let _create = joins.expect(first.clone());
        assert_eq!(serve("the first join").await, R::Applied, "the first accepted join report records the birth's Pending");
        cut();
        assert_eq!(serve("a retry of the first join").await, R::AlreadyApplied, "a retry of the first join belongs to its Pending");
        assert!(cut_held(), "a retry begins no cycle");

        // Cut off before its hook passed, still Pending, and commanded back: its commanded join's
        // registration (heard_again's, a held deployment).
        let commanded = crate::join::Deployed { held: true, last_accepted_name: Some(rpc.location()), ..first.clone() };
        let _commanded = joins.expect(commanded.clone());
        assert_eq!(serve("the commanded join").await, R::Applied, "the same birth's commanded join, while it is still Pending from its last join, records its own Pending");
        assert!(!cut_held(), "the commanded join's accepted report begins the birth's next cycle");
        cut();
        assert_eq!(serve("a retry of the commanded join").await, R::AlreadyApplied, "a retry of the commanded join belongs to its Pending");
        assert!(cut_held(), "a retry begins no cycle");

        // Cut off again before its hook passed, and commanded back again: the same held deployment
        // registered again is this commanded join's own registration.
        let _again = joins.expect(commanded);
        assert_eq!(serve("the next commanded join").await, R::Applied, "the next commanded join of the still-Pending birth records its own Pending");
        assert!(!cut_held(), "the next commanded join's accepted report begins the birth's next cycle");
        assert_eq!(serve("a retry of the next commanded join").await, R::AlreadyApplied, "a retry of the next commanded join belongs to its Pending");
    }

    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:71
    /// "`{ \"Current\": { \"node_id\": \"k8pag1qx95gw\", \"incarnation\": \"0000000000000008\", \"state\": \"Ready\" } }`"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:74
    /// "A stopped node answers `CurrentStopped { node_id, incarnation }`; a node whose hook answered `Pending` answers
    ///  `Pending { node_id, incarnation, reason }`. All three are `reached`."
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:100
    /// "The probed node re-publishes its presence on the channel it is on, the check-in it owes its
    ///  primary, and answers its current state; while its `pre_node_ready` hook answered `Pending` it
    ///  answers `Pending` and re-publishes nothing."
    /// CONTRACT (#2803 detection; node-rpc.md §14): a fabric primary probes ANOTHER mesh's node-admin
    /// through a member of that mesh (Forward carries no origin identity). The subject answers
    /// whoever reaches it: the probe is answered with the node-admin's current state from the
    /// carrier, from the node-admin itself and from an endpoint no view holds, and an apply is the
    /// subject's to answer too (here refused by its own rule: apply-node-state drives no state).
    /// What must NOT happen: a call naming this admin left unanswered because of who sent it.
    #[tokio::test]
    async fn a_probe_or_an_apply_naming_this_admin_is_answered_by_it_whoever_sends_it() {
        let mut admin = node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin");
        admin.status = crate::model::NodeStatus::Ready;
        let carrier = node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node");
        let (node_id, incarnation) = (admin.node_id.clone(), admin.incarnation_id.clone().unwrap());
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let republished = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let republished_cb = republished.clone();
        let republish: Republish = Arc::new(OnceLock::new());
        let _ = republish.set(Arc::new(move || {
            let republished_cb = republished_cb.clone();
            Box::pin(async move {
                republished_cb.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
        }));
        for sender in [Some(&carrier), Some(&admin), None] {
            let answered = self_subject(&admin, sender, None, &probe, &republish, &Rename::default(), &PreNodeReadyCell::default(), &crate::status_declare::DeclareWake::default()).await;
            assert_eq!(answered, Some(R::Current { node_id: node_id.clone(), incarnation: incarnation.clone(), state: NodeState::Ready }), "from {:?}: {answered:?}", sender.map(|n| n.node_id.to_string()));
        }
        assert_eq!(republished.load(std::sync::atomic::Ordering::SeqCst), 3, "each answered Current probe re-publishes presence once");
        let apply = StatusRequest::ApplyNodeState { node_id, incarnation, state: NodeState::ConnectionsDeleting };
        let answered = self_subject(&admin, Some(&carrier), None, &apply, &republish, &Rename::default(), &PreNodeReadyCell::default(), &crate::status_declare::DeclareWake::default()).await;
        assert!(matches!(answered, Some(R::RejectedInvalidNodeTransition { .. })), "the subject answers the apply by its own rule: {answered:?}");
    }

    /// The status authority of the admin born as `me`, deciding over `view`.
    /// CONTRACT (i151.s2; node-rpc.md §43, Q311, Q322: `ReportCallFailed { sender, receiver, reason }`
    /// "answered `Applied`, or `RejectedNotPrimary { why: NotPrimary::SubjectUnknown }` for a birth the
    /// receiver is not the primary above"; Q338: the fabric-primary is the primary above a
    /// mesh-primary): mesh2's primary takes a report of a spent call to its member and marks the exact
    /// birth at once, as its own spent call marks it; it refuses `SubjectUnknown` a report naming a
    /// birth of another mesh, another incarnation of its member, a birth it holds nothing of, and
    /// itself; an admin of mesh2 that is not its primary refuses its member's report `SubjectUnknown`.
    /// The fabric-primary takes a report of a spent call to mesh2's primary and marks it, and refuses a
    /// report of mesh2's member (its mesh-primary's) and of itself (nothing is above it). Must NOT
    /// happen: a refused report marking anything.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_report_of_a_spent_call_is_applied_only_by_the_primary_above_the_birth() {
        let admin = |name: rdm_core::PathName, mesh: &str, primary: bool| {
            let mut n = node(name, mesh, "node-admin");
            n.r#type = "node-admin".to_string();
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::Ready;
            n.is_primary = primary;
            n
        };
        let member = |name: rdm_core::PathName, mesh: &str| {
            let mut n = node(name, mesh, "node");
            n.r#type = "node".to_string();
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::Ready;
            n
        };
        let m1p = crate::model::Node { is_fabric_primary: true, ..admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", true) };
        let m2p = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", true);
        let m2a = admin(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", false);
        let own = member(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2");
        let other = member(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1");
        let stranger = member(rdm_core::PathName::new("mesh2.node", "9").unwrap(), "mesh2");
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![m1p.clone(), m2p.clone(), m2a.clone(), own.clone(), other.clone()],
        };
        let birth = |n: &crate::model::Node| rdm_core::status::ReportedBirth { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), path: n.path.clone(), name: n.name.clone() };
        let report = |n: rdm_core::status::ReportedBirth| StatusRequest::report_call_failed(birth(&m1p), n, "not-sent: Connection(\"connect refused\")");
        let service = mesh_primary_of(&m2p, view.clone());
        let mut later_birth = birth(&own);
        later_birth.incarnation = crate::model::IncarnationId::mint();
        for (receiver, what) in [(birth(&other), "a birth of another mesh"), (later_birth, "another incarnation of its member"), (birth(&stranger), "a birth it holds nothing of"), (birth(&m2p), "itself")] {
            assert_eq!(service.apply(None, &report(receiver)).await, R::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "{what}");
        }
        assert!(service.unreachable.lock().unwrap().is_empty(), "a refused report marks nothing");
        let standby = mesh_primary_of(&m2a, view.clone());
        assert_eq!(standby.apply(None, &report(birth(&own))).await, R::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "an admin that is not the primary above the birth");
        assert!(standby.unreachable.lock().unwrap().is_empty());
        assert_eq!(service.apply(None, &report(birth(&own))).await, R::Applied, "the primary above the birth takes it");
        let mark = service.unreachable.lock().unwrap().get(&own.node_id).cloned().expect("the birth is marked at once");
        assert_eq!((mark.incarnation, mark.reason), (own.incarnation_id.clone().unwrap(), "attempts-spent"));
        // The fabric-primary: the primary above mesh2's primary, and above nothing of mesh2 beside it.
        let fabric_primary = mesh_primary_of(&m1p, view.clone());
        for (receiver, what) in [(birth(&own), "a member of a peer mesh, its mesh-primary's"), (birth(&m1p), "itself: nothing is above the fabric-primary")] {
            assert_eq!(fabric_primary.apply(None, &report(receiver)).await, R::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "{what}");
        }
        assert!(fabric_primary.unreachable.lock().unwrap().is_empty(), "a refused report marks nothing");
        assert_eq!(fabric_primary.apply(None, &report(birth(&m2p))).await, R::Applied, "the fabric-primary takes a report of a spent call to a mesh-primary");
        let mark = fabric_primary.unreachable.lock().unwrap().get(&m2p.node_id).cloned().expect("the mesh-primary is marked at the fabric-primary at once");
        assert_eq!((mark.incarnation, mark.reason), (m2p.incarnation_id.clone().unwrap(), "attempts-spent"));
    }

    fn mesh_primary_of(me: &crate::model::Node, view: Topology) -> StatusService {
        StatusService {
            certs: None,
            primary_commands: Default::default(),
            topology_view: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: me.location(),
            node_id: me.node_id.clone(),
            fabric_id: view.fabric.id.clone(),
            topology: Arc::new(tokio::sync::RwLock::new(view)),
            declared: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            caches: Default::default(),
            meshes_storage: Arc::new(crate::storage::MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Default::default(),
            own: Default::default(),
            mesh_deleter: Default::default(),
            storage_changes: Default::default(), hold_next_reply: Default::default(),
            wake: Default::default(),
            fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(), rejoin: Default::default(),
            loads: None,
            digest_view: None,
            unreachable: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        }
    }

    /// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"; states.md "Who sets each state": node Ready is set by "the node's
    /// mesh-primary" from "the node's node.started report"): a node-admin's declaration is applied by
    /// its own mesh's primary like every other kind's. mesh2's primary applies mesh2.node-admin.2's
    /// Ready; the fabric-primary, of mesh1, refuses it naming the role it needs, the
    /// mesh-primary of mesh2. Must NOT happen: a node-admin's status set by the fabric-primary.
    /// CONTRACT (Q11 = a; node-status.md "Status report": every type reports to its mesh's primary, which
    /// sets its status): a mesh-primary decides a declaration for a birth its offline tick watches through
    /// durable rows alone (born while this admin was silent: its view holds nothing at the path) as the
    /// birth's authority: the exact birth's Ready is applied. A node id neither its view nor its
    /// durable births hold is still `SubjectUnknown`. Must NOT happen: the re-born mesh-primary answering a
    /// resumed member's Ready `SubjectUnknown`, which ends the member's owed declaration and
    /// leaves it `pending` (kept/e6s7-i144-e4-re-born-admin-follower-hold-race, trace
    /// f1c56aff7dee1b7a4d25e8564d306141).
    #[test]
    fn a_declaration_for_a_birth_watched_through_durable_rows_is_decided_by_its_mesh_primary() {
        let mut m2p = node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin");
        m2p.r#type = "node-admin".to_string();
        m2p.mesh = "mesh2".into();
        m2p.status = crate::model::NodeStatus::Ready;
        m2p.is_primary = true;
        let mut member = node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node");
        member.r#type = "node".to_string();
        member.mesh = "mesh2".into();
        member.status = crate::model::NodeStatus::PendingReconnect;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![m2p.clone()],
        };
        let service = mesh_primary_of(&m2p, view.clone());
        service.durable.lock().unwrap().push(member.clone());
        let declare = |n: &crate::model::Node| StatusRequest::DeclareNodeState { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        let (reply, effect) = decide(&service, &view, &Declared::default(), &declare(&member));
        assert_eq!(reply, R::Applied, "the mesh-primary applies the Ready of a birth it watches through durable rows");
        assert!(matches!(effect, Some(Effect::Node(ref row, NodeState::Ready, None)) if row.node_id == member.node_id && row.name == member.location()), "{effect:?}");
        let stranger = node(rdm_core::PathName::new("mesh2.node", "9").unwrap(), "mesh2", "node");
        let (reply, effect) = decide(&service, &view, &Declared::default(), &declare(&stranger));
        assert_eq!(reply, R::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "a birth neither viewed nor watched is unknown");
        assert!(effect.is_none());
    }

    /// CONTRACT (node-status.md "Status report": a node reports to its mesh's primary, which sets its
    /// status; node-rpc-envelope.md: `ReceiverNotPrimary` when the receiver does not hold the role the
    /// request needs): an admin that is not its mesh's primary answers every node declaration
    /// `ReceiverNotPrimary` before it looks for the subject: a birth its view holds, one only a durable
    /// runtime row names, and one it holds nothing of alike. Its truthful answer is that it is not the
    /// primary, never a fact about the subject. Must NOT happen: `SubjectUnknown` from an admin that is
    /// no primary (which runs the refused-caller decision there), or the answer turning on whether that
    /// admin watches births through durable rows (rust-distributed-mesh#99: only the mesh-primary does).
    // @feature: node-lifecycle
    #[test]
    fn an_admin_that_is_not_its_meshs_primary_answers_every_node_declaration_receiver_not_primary() {
        let mut m2p = node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin");
        m2p.r#type = "node-admin".to_string();
        m2p.mesh = "mesh2".into();
        m2p.status = crate::model::NodeStatus::Ready;
        m2p.is_primary = true;
        let mut m2a = node(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin");
        m2a.r#type = "node-admin".to_string();
        m2a.mesh = "mesh2".into();
        m2a.status = crate::model::NodeStatus::Ready;
        let mut viewed = node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node");
        viewed.r#type = "node".to_string();
        viewed.mesh = "mesh2".into();
        let mut row_only = node(rdm_core::PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node");
        row_only.r#type = "node".to_string();
        row_only.mesh = "mesh2".into();
        let stranger = node(rdm_core::PathName::new("mesh2.node", "9").unwrap(), "mesh2", "node");
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![m2p.clone(), m2a.clone(), viewed.clone()],
        };
        let service = mesh_primary_of(&m2a, view.clone());
        service.durable.lock().unwrap().push(row_only.clone());
        let declare = |n: &crate::model::Node| StatusRequest::DeclareNodeState { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        for (subject, what) in [(&viewed, "a birth its view holds"), (&row_only, "a birth only a durable row names"), (&stranger, "a birth it holds nothing of")] {
            let (reply, effect) = decide(&service, &view, &Declared::default(), &declare(subject));
            assert_eq!(reply, R::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } }, "{what}: the admin that is no primary says so");
            assert!(effect.is_none(), "{what}: nothing applied");
        }
    }

    #[test]
    fn a_node_admins_declaration_is_applied_by_its_own_mesh_primary_never_the_fabric_primary() {
        let admin = |name: rdm_core::PathName, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name, mesh, "node-admin");
            n.r#type = "node-admin".to_string();
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::Ready;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n
        };
        let fp = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", true, true);
        let m2p = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", true, false);
        let m2a = admin(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", false, false);
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), m2p.clone(), m2a.clone()],
        };
        let declare = StatusRequest::DeclareNodeState { node_id: m2a.node_id.clone(), incarnation: m2a.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        let (reply, effect) = decide(&mesh_primary_of(&m2p, view.clone()), &view, &Declared::default(), &declare);
        assert_eq!(reply, R::Applied, "mesh2's primary applies its own mesh's admin's Ready");
        assert!(matches!(effect, Some(Effect::Node(ref row, NodeState::Ready, None)) if row.node_id == m2a.node_id), "{effect:?}");
        let (reply, effect) = decide(&mesh_primary_of(&fp, view.clone()), &view, &Declared::default(), &declare);
        assert_eq!(reply, R::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } }, "the fabric-primary of mesh1 is not mesh2.node-admin.2's authority");
        assert!(effect.is_none());
    }

    /// CONTRACT (R24, Luke Q11: "the same variant at every Status door that can lack a handler for a
    /// request it receives"): the node-admin's declaration decision has no handler for a request its
    /// `StatusService` answers before the decision (here `fabric.get`). It answers `RequestNotHandled` naming the
    /// request, decided by dispatch alone, on the reject span `rdm.node.status.reject.via-request-not-handled`
    /// naming the request and the admin, and changes nothing. Must NOT happen: an authority refusal or
    /// `NotReady` for a request it has no handler for.
    #[test]
    fn a_request_the_declaration_decision_has_no_handler_for_is_answered_not_handled_by_name() {
        let mut me = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        me.r#type = "node-admin".to_string();
        me.is_primary = true;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![me.clone()],
        };
        let capture = crate::span_capture::Capture::new("i144-r24-admin-status-service");
        let (reply, effect) = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            decide(&mesh_primary_of(&me, view.clone()), &view, &Declared::default(), &StatusRequest::GetFabric)
        };
        assert_eq!(reply, R::RequestNotHandled { request: "fabric.get".into() });
        assert!(effect.is_none(), "{effect:?}");
        let rejected: Vec<(String, String)> = capture.named("rdm.node.status.reject.via-request-not-handled").into_iter().map(|s| (s.attributes["request"].clone(), s.attributes["node"].clone())).collect();
        assert_eq!(rejected, vec![("fabric.get".to_string(), me.node_id.to_string())], "the reject span names the request and the admin, by its node id");
    }

    /// CONTRACT (node-identity-and-state.md 4.3: "Once a node-admin is created in a mesh it is that
    /// mesh's primary ... and writes every status from then on"): the fabric-primary's
    /// ApplyMeshState is applied by the mesh's primary. A receiver whose view names ANOTHER admin
    /// its mesh's primary refuses it naming that holder, and applies nothing; the admin the view
    /// names applies it, and so does an admin of the mesh whose view names no primary of it yet
    /// (the bootstrap admin at the hand-off, before its own role shows in its view).
    #[test]
    fn an_apply_mesh_state_at_an_admin_whose_view_names_another_primary_is_refused_naming_it() {
        let admin = |name: rdm_core::PathName, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name, mesh, "node-admin");
            n.r#type = "node-admin".to_string();
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::Ready;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n
        };
        let fp = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", true, true);
        let first = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", true, false);
        let second = admin(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", false, false);
        let view = |nodes: Vec<crate::model::Node>| Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes,
        };
        let with_primary = view(vec![fp.clone(), first.clone(), second.clone()]);
        let pending = StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh2".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 };

        let (reply, effect) = decide(&mesh_primary_of(&second, with_primary.clone()), &with_primary, &Declared::default(), &pending);
        assert!(
            matches!(&reply, R::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed.contains(first.node_id.as_str())) && effect.is_none(),
            "an admin whose view names mesh2.node-admin.1 mesh2's primary applies no Mesh status of mesh2: {reply:?}"
        );

        let (reply, effect) = decide(&mesh_primary_of(&first, with_primary.clone()), &with_primary, &Declared::default(), &pending);
        assert!(matches!(reply, R::Applied) && effect.is_some(), "the primary its view names applies it: {reply:?}");

        let mut non_primary_first = first.clone();
        non_primary_first.is_primary = false;
        let without_primary = view(vec![fp.clone(), non_primary_first.clone(), second.clone()]);
        let (reply, effect) = decide(&mesh_primary_of(&non_primary_first, without_primary.clone()), &without_primary, &Declared::default(), &pending);
        assert!(matches!(reply, R::Applied) && effect.is_some(), "an admin of the mesh whose view names no primary of it applies it: {reply:?}");
    }

    /// CONTRACT (R-RC1, node-recovery.md: a new node.id at the same path and name): a recovery keeps the
    /// path and name and mints a new node id. The view holds one row per path and name (`admin::project_at`
    /// keys it by `PathName`), so until the deleted birth's digest goes non-live or departs, the row
    /// at this admin's path can be the deleted birth's, live and in its role, and this admin's own row is
    /// absent. The authority finds itself by the node id it was born with: it decides nothing from
    /// the deleted birth's role and refuses naming the true reason, "a view holding this admin"; a
    /// node command naming its node id goes on to the subject's own `NodeSelf` (`NodeSelf::serve`, which
    /// checks the exact birth itself), here not yet filled. What must NOT happen: a member's
    /// declaration `Applied` on the deleted birth's mesh-primary role, or a node command naming this
    /// admin refused as "not the subject node itself".
    #[tokio::test]
    async fn a_mesh_primary_whose_view_holds_its_superseded_birth_at_its_path_answers_as_itself() {
        let mut deleted = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        deleted.r#type = "node-admin".to_string();
        deleted.mesh = "mesh1".into();
        deleted.status = crate::model::NodeStatus::ConnectionsDeleting;
        deleted.is_primary = true;
        deleted.is_fabric_primary = true;
        let mut successor = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        successor.r#type = "node-admin".to_string();
        successor.mesh = "mesh1".into();
        let mut member = node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        member.r#type = "node".to_string();
        member.mesh = "mesh1".into();
        member.status = crate::model::NodeStatus::Ready;
        let mut owner = node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin");
        owner.r#type = "node-admin".to_string();
        owner.mesh = "mesh1".into();
        owner.status = crate::model::NodeStatus::Ready;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![deleted.clone(), member.clone(), owner.clone()],
        };
        assert_ne!(deleted.node_id, successor.node_id, "a re-birth mints a new node id");
        assert_eq!(view.node_type_primary("mesh1", "node-admin").map(|n| n.node_id.clone()), Some(deleted.node_id.clone()), "the deleted birth holds mesh1's role in this view");
        let service = mesh_primary_of(&successor, view.clone());
        let holding_me = |r: &StatusReply| matches!(r, R::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "a view holding this admin");

        let declare = StatusRequest::DeclareNodeState { node_id: member.node_id.clone(), incarnation: member.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        let (reply, effect) = decide(&service, &view, &Declared::default(), &declare);
        assert!(holding_me(&reply) && effect.is_none(), "the deleted birth's role is not this admin's: {reply:?}");

        let stop = StatusRequest::JoinMesh { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap(), operation: rdm_core::OperationId::node("node.mesh.join", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap())), primary: None };
        let reply = service.apply(Some(owner.clone()), &stop).await;
        assert!(matches!(&reply, R::NotReady { reason } if reason.contains("has not yet joined its mesh")), "a JoinMesh naming this admin's node id reaches the subject's own NodeSelf: {reply:?}");
    }

    /// CONTRACT (node-rpc.md §14: "Nothing inside the mesh checks who sent a frame or call: a Node RPC
    /// handler is addressed by target node id + op"): a call naming this admin from an endpoint its view holds no birth for is decided
    /// for the subject it names, as one from an admin it holds is. A node command naming this admin's
    /// exact birth reaches the subject's own `NodeSelf` (`NodeSelf::serve`, here not yet filled, so
    /// `NotReady` by name), and a probe naming it is answered by the subject. The direction refusals
    /// stay: a command naming another birth of this admin is `RejectedStaleIncarnation`. What must
    /// NOT happen: `SenderNotSubject` "unknown peer", or a probe of the subject answered as if this
    /// admin were not the subject.
    #[tokio::test]
    async fn a_call_naming_this_admin_from_an_endpoint_it_never_heard_of_is_decided_for_the_subject() {
        let mut me = node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin");
        me.r#type = "node-admin".to_string();
        me.mesh = "mesh1".into();
        me.status = crate::model::NodeStatus::Ready;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![me.clone()],
        };
        let service = mesh_primary_of(&me, view);
        let incarnation = me.incarnation_id.clone().unwrap();
        let stop = StatusRequest::JoinMesh { node_id: me.node_id.clone(), incarnation: incarnation.clone(), operation: rdm_core::OperationId::node("node.mesh.join", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node-admin", "2").unwrap())), primary: None };
        let reply = service.apply(None, &stop).await;
        assert!(matches!(&reply, R::NotReady { reason } if reason.contains("has not yet joined its mesh")), "a JoinMesh from a stranger reaches the subject's own NodeSelf: {reply:?}");
        let probe = StatusRequest::ProbeNodeState { node_id: me.node_id.clone(), incarnation: incarnation.clone() };
        let reply = service.apply(None, &probe).await;
        assert!(matches!(&reply, R::Current { node_id, state: NodeState::Ready, .. } if *node_id == me.node_id), "a probe from a stranger is answered by the subject: {reply:?}");
        let stale = StatusRequest::JoinMesh { node_id: me.node_id.clone(), incarnation: IncarnationId::mint(), operation: rdm_core::OperationId::node("node.mesh.join", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node-admin", "2").unwrap())), primary: None };
        assert_eq!(service.apply(None, &stale).await, R::RejectedStaleIncarnation { held: incarnation });
    }

    /// CONTRACT (node-rpc.md §14; checklist.rs `fabric_missing`: "a report counts only from the exact
    /// birth that is the mesh's primary now"): a Mesh's `Ready` declaration is decided for
    /// the mesh it names, whoever sends it, and the fabric-primary keeps it as that mesh's report from
    /// the birth that sent it. A later declaration of the same mesh from another birth (a member, an
    /// old primary) never displaces the primary's report, and a declaration from a node of mesh1 is
    /// never kept as mesh1's report. What must NOT happen: the primary's report lost, which holds the
    /// fabric checklist on `report-of-another-birth` with nothing owed to re-send it.
    #[tokio::test]
    async fn a_mesh_report_is_kept_for_the_mesh_it_names_and_the_birth_that_sent_it() {
        let admin = |name: rdm_core::PathName, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name, mesh, "node-admin");
            n.r#type = "node-admin".to_string();
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::Ready;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n.endpoint_id = Some(EndpointId(format!("key-{}", n.node_id)));
            n
        };
        let fp = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", true, true);
        let m1_member = admin(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", false, false);
        let m2_primary = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", true, false);
        let m2_member = admin(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", false, false);
        let m2_id = MeshId::mint();
        let mut view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), m1_member.clone(), m2_primary.clone(), m2_member.clone()],
        };
        view.meshes.push(crate::model::Mesh { id: Some(m2_id.clone()), name: "mesh2".into(), status: crate::model::ScopeStatus::Ready });
        let service = mesh_primary_of(&fp, view);
        let ready = StatusRequest::DeclareMeshState { mesh_id: m2_id.clone(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
        assert_eq!(service.apply(Some(m2_primary.clone()), &ready).await, R::Applied);
        assert_eq!(service.apply(Some(m2_member.clone()), &ready).await, R::Applied, "decided for mesh2, whoever sends it, and answered Applied again (Q144)");
        assert_eq!(service.apply(Some(m1_member.clone()), &ready).await, R::Applied);
        let held = service.declared.lock().unwrap().clone();
        assert_eq!(held.report_of("mesh2", Some(&m2_primary)).map(|r| r.birth), m2_primary.birth(), "the primary's report stands: {:?}", held.reports);
        assert!(held.report_of("mesh1", Some(&fp)).is_none(), "a declaration of mesh2 is never mesh1's report: {:?}", held.reports);
        // A mesh's primary this admin's view does not hold yet: its report is kept by its endpoint and
        // counts for its birth once the view holds it.
        let m3_id = MeshId::mint();
        let m3_primary = admin(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", true, false);
        service.topology.write().await.meshes.push(crate::model::Mesh { id: Some(m3_id.clone()), name: "mesh3".into(), status: crate::model::ScopeStatus::Pending });
        let key = iroh::SecretKey::generate().public();
        let mut m3_primary = m3_primary;
        m3_primary.endpoint_id = Some(EndpointId(key.to_string()));
        let ready3 = StatusRequest::DeclareMeshState { mesh_id: m3_id, state: MeshState::Ready, changed_at_rafka_ms: 0 };
        assert_eq!(service.apply_from(Some(key), None, &ready3).await, R::Applied, "decided for mesh3 from an endpoint the view holds no birth for");
        let held = service.declared.lock().unwrap().clone();
        assert_eq!(held.report_of("mesh3", Some(&m3_primary)).map(|r| r.birth), m3_primary.birth(), "the endpoint's report counts for the birth the view holds at it: {:?}", held.reports);
        assert!(held.report_of("mesh3", Some(&m2_member)).is_none(), "and for no other birth");
    }

    fn digest_of(n: &crate::model::Node, incarnation: IncarnationId) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest { 
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(),
                node_id: n.node_id.clone(),
                path: n.path.clone(), name: n.name.clone(),
                endpoint_id: EndpointId(format!("key-{}", n.node_id)),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation,
                supersedes: None,
                runtime: None,
            },
            status: rdm_core::MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None, is_primary: None, is_fabric_primary: None
        }
    }

    /// CONTRACT (Q197: an action decision reads the records, never gossip; node-identity-and-state.md
    /// §2.3 "the deleted row releases the `(path, name)`"; Luke 2026-10-26: "the birth at a path is the one
    /// the records say is current, stopped or not"): the birth that holds a path and name is the one whose
    /// record holds it live. A deleted predecessor whose digest this admin's view still holds (its
    /// deletion never applied here) does not hold the path, so the current birth at that path keeps its
    /// declared state.
    /// Must NOT happen: the current birth's declared state tombstoned via-new-node, naming the deleted
    /// predecessor as the node at its path.
    #[tokio::test]
    async fn a_current_birth_keeps_its_declared_state_while_the_view_still_holds_its_deleted_predecessor() {
        let mut admin = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        admin.mesh = "mesh1".into();
        admin.r#type = "node-admin".to_string();
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut predecessor = node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        predecessor.mesh = "mesh1".into();
        predecessor.r#type = "node".to_string();
        predecessor.endpoint_id = Some(EndpointId("mesh1.node.1-endpoint".into()));
        predecessor.transport_addr = Some("127.0.0.1:1".parse().unwrap());
        // The recovery's new node at the same path and name (node-recovery.md: "New: node.id").
        let mut current = predecessor.clone();
        current.node_id = NodeId::mint();
        current.incarnation_id = Some(IncarnationId::mint());
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), current.clone()],
        }));
        let status: Arc<crate::storage::MemoryNodesStorage> = Arc::new(Default::default());
        let service = StatusService {
            certs: None,
            topology_view: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin.location(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(crate::storage::MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            primary_commands: Default::default(),
            storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
            nodes_storage: status.clone(),
            caches: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(), rejoin: Default::default(),
            loads: None,
            digest_view: None,
            unreachable: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        // The records: the predecessor's record is Deleted (it released the path and name), and the
        // current birth wrote its own record at the name over it (Q356).
        claimed(&status, &[&predecessor]).await;
        let deleted = crate::node_rows::write_deleted(&*status, &predecessor.location(), &predecessor.node_id).await;
        assert!(deleted.is_ok(), "the predecessor's Deleted record is written: {deleted:?}");
        claimed(&status, &[&current]).await;
        let declare = StatusRequest::DeclareNodeState { node_id: current.node_id.clone(), incarnation: current.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        assert_eq!(service.apply(Some(current.clone()), &declare).await, R::Applied);
        use crate::storage::NodesStorage as _;
        assert_eq!(crate::node_rows::record_at(&*status, &current.location()).await.unwrap().map(|r| r.node_id), Some(current.node_id.clone()), "the current birth's record holds the (path, name)");
        // This admin's view still holds the predecessor's digest (its deletion never applied here), and
        // holds nothing of the current birth yet.
        let digest_view = rdm_transport::topology::DigestView::default();
        digest_view.record(digest_of(&predecessor, predecessor.incarnation_id.clone().unwrap()));
        let capture = crate::span_capture::Capture::new("superseded-rows-read-from-records");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            service.drop_superseded(&digest_view).await;
        }
        let removed: Vec<_> = ["rdm.node_admin.status.remove.via-new-node", "rdm.node_admin.status.remove.via-new-incarnation", "rdm.node_admin.status.remove.via-node-deleted"]
            .into_iter()
            .flat_map(|n| capture.named(n))
            .filter(|s| s.attributes.get("node_id") == Some(&current.node_id.to_string()))
            .collect();
        for s in &removed {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        assert!(removed.is_empty(), "the current birth's declared state is not removed: {:?}", removed.iter().map(|s| (s.name.clone(), s.attributes.clone())).collect::<Vec<_>>());
        assert_eq!(service.declared.lock().unwrap().node(&current.node_id, current.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "the current birth keeps its state");
    }

    /// CONTRACT (query.md §1: "a mesh-primary answers a read of its own mesh from its atlas and carries
    /// any other read up to the fabric-primary"; Q224): the fabric-primary answers every read; mesh2's
    /// mesh-primary, its atlas holding mesh2, answers `node.get` whose `mesh` pair names only mesh2 and
    /// `mesh.get` of mesh2 by name or id, and carries up a read of mesh1, a read naming mesh1 and mesh2,
    /// a read naming no mesh and `fabric.get`; `node.connection.get` is the exact birth's; a node-admin that is
    /// not a primary forwards every other read to its own mesh-primary (Q243). Must NOT happen: a
    /// mesh-primary answering another mesh from its own view; its own mesh's read carried up; a node-admin
    /// that is not a primary answering a read from its own view, or sending it to the fabric-primary.
    #[test]
    fn a_mesh_primary_answers_its_own_meshs_reads_and_carries_every_other_up() {
        use rdm_core::filter::{Entity, Filter};
        use rdm_core::status::Read;
        let admin = |name: rdm_core::PathName, mesh: &str, primary: bool, fabric_primary: bool| {
            let mut n = node(name, mesh, "node-admin");
            n.r#type = "node-admin".to_string();
            n.mesh = mesh.into();
            n.status = crate::model::NodeStatus::Ready;
            n.is_primary = primary;
            n.is_fabric_primary = fabric_primary;
            n
        };
        let (fp, mp, other) = (admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", true, true), admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", true, false), admin(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", false, false));
        let mesh2_id = MeshId::mint();
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: vec![
                crate::model::Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: crate::model::ScopeStatus::Ready },
                crate::model::Mesh { id: Some(mesh2_id.clone()), name: "mesh2".into(), status: crate::model::ScopeStatus::Ready },
            ],
            nodes: vec![fp.clone(), mp.clone(), other.clone()],
        };
        let read = |entity: Entity, filter: Filter| Read { entity, node: None, filter };
        let mesh2: std::collections::BTreeSet<String> = ["mesh2".to_string()].into();
        let every: std::collections::BTreeSet<String> = ["mesh1".to_string(), "mesh2".to_string()].into();
        let reads = [
            ("node.get [mesh: mesh2]", read(Entity::Nodes, Filter::new().eq("mesh", ["mesh2"]).eq("type", ["node"])), ReadAt::Here),
            ("node.get [mesh: mesh1]", read(Entity::Nodes, Filter::new().eq("mesh", ["mesh1"])), ReadAt::FabricPrimary),
            ("node.get [mesh: mesh1, mesh2]", read(Entity::Nodes, Filter::new().eq("mesh", ["mesh1", "mesh2"])), ReadAt::FabricPrimary),
            ("node.get [tags: red]", read(Entity::Nodes, Filter::new().eq("tags", ["red"])), ReadAt::FabricPrimary),
            ("node.get []", read(Entity::Nodes, Filter::new()), ReadAt::FabricPrimary),
            ("mesh.get [name: mesh2]", read(Entity::Meshes, Filter::new().eq("name", ["mesh2"])), ReadAt::Here),
            ("mesh.get [id: mesh2's]", read(Entity::Meshes, Filter::new().eq("id", [mesh2_id.to_string()])), ReadAt::Here),
            ("mesh.get []", read(Entity::Meshes, Filter::new()), ReadAt::FabricPrimary),
            ("fabric.get []", read(Entity::Fabrics, Filter::new()), ReadAt::FabricPrimary),
        ];
        for (what, r, at) in &reads {
            assert_eq!(read_answered_at(&view, &mp.node_id, &mesh2, r), *at, "{what} at mesh2's mesh-primary");
            assert_eq!(read_answered_at(&view, &fp.node_id, &every, r), ReadAt::Here, "{what} at the fabric-primary");
            assert_eq!(read_answered_at(&view, &other.node_id, &Default::default(), r), ReadAt::MeshPrimary, "{what} at a node-admin that is not a primary: forwarded to its mesh-primary");
        }
        let connections = Read { entity: Entity::Connections, node: Some((mp.node_id.clone(), mp.incarnation_id.clone().unwrap())), filter: Filter::new() };
        assert_eq!(read_answered_at(&view, &mp.node_id, &mesh2, &connections), ReadAt::Here, "node.connection.get is the exact birth's");
        let own_connections = Read { entity: Entity::Connections, node: Some((other.node_id.clone(), other.incarnation_id.clone().unwrap())), filter: Filter::new() };
        assert_eq!(read_answered_at(&view, &other.node_id, &Default::default(), &own_connections), ReadAt::Here, "node.connection.get at a node-admin that is not a primary is its own");
        assert_eq!(read_answered_at(&view, &mp.node_id, &Default::default(), &read(Entity::Nodes, Filter::new().eq("mesh", ["mesh2"]))), ReadAt::FabricPrimary, "a mesh-primary whose atlas does not hold its mesh yet carries the read up");
    }
}

