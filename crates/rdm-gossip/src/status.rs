//! Status (Q374): node, mesh and fabric status as gossip carries it (gossip.md §3.1, §3.2): the
//! status facts, the status publisher and its reinforcement, and the held statuses.


use crate::snapshot::PublisherId;
use rdm_core::{FabricId, LifecycleOp};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};


use crate::frame::*;

/// The status events (node-status-heartbeat-connections.md R-HB1, "Events vs statuses": "NodeStopped,
/// NodeDeleting and NodeRestarting stay immediate events"; NodeRestarting is reserved since Q183):
/// each carries its publisher and the
/// `topology_version` at which its subject changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusEvent {
    /// `NodeDeleting`: the executor started removing the exact birth.
    Deleting,
    /// `NodeStopped`: the executor holds the exact birth's matching stopped reply.
    Stopped,
}

impl StatusEvent {
    /// The frame's name in spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::Deleting => "node-deleting",
            Self::Stopped => "node-stopped",
        }
    }

    /// The frame of this event for `op`, published by `publisher` at `topology_version`.
    pub fn frame(self, op: LifecycleOp, publisher: PublisherId, topology_version: u64) -> Frame {
        match self {
            Self::Deleting => Status::node_deleting(op, publisher, topology_version),
            Self::Stopped => Status::node_stopped(op, publisher, topology_version),
        }
        .frame()
    }
}

/// What a [`Status`] is the status of (Q374): a node's birth, a mesh, or the fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusType {
    /// A node's birth: `stopped`, `deleting` or `deleted`.
    Node,
    /// A mesh: its state (`pending`, `ready`, `leaving`, ...).
    Mesh,
    /// The fabric: its state.
    Fabric,
}

/// The subject of a [`Status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusSubject {
    /// A node's birth, by the operation that changed it.
    Node(LifecycleOp),
    /// A mesh, by its name.
    Mesh(String),
    /// The fabric, by its id.
    Fabric(FabricId),
}

/// How a [`Status`] is ordered against the one held: each type keeps its own rule (gossip-by-kind.md
/// §5.3; receive_order.rs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusOrder {
    /// A node's `stopped` and `deleting`: the `topology_version` at which its subject changed, per
    /// publisher and subject mesh.
    TopologyVersion {
        /// The exact birth that published it.
        publisher: PublisherId,
        /// The subject mesh's topology version at the change.
        topology_version: u64,
    },
    /// A node's `deleted`: no order; applied once per `(type, build_id, operation)`.
    Once,
    /// A mesh's and the fabric's: the primary revision of its envelope, then `changed_at_rafka_ms`.
    Changed {
        /// The node that published it, by its node id.
        publisher: String,
        /// The instant of the change, Rafka-time.
        changed_at_rafka_ms: u64,
    },
}

/// One status (Q374; gossip-by-kind.md §5.3): the one Rust type every status publisher builds its
/// frame from and every status receiver reads a frame as. It encodes to today's five frames
/// (`NodeStopped`, `NodeDeleting`, `NodeDeleted`, `MeshStatus`, `FabricStatus`) and decodes from
/// them, the wire unchanged (D1); each type keeps its own order ([`StatusOrder`]), recovery and sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    subject: StatusSubject,
    status: String,
    order: StatusOrder,
    forwarded_by: Option<String>,
}

impl Status {
    /// A node's `stopped` (`NodeStopped`), published by `publisher` at `topology_version`.
    pub fn node_stopped(op: LifecycleOp, publisher: PublisherId, topology_version: u64) -> Self {
        Self { subject: StatusSubject::Node(op), status: "stopped".into(), order: StatusOrder::TopologyVersion { publisher, topology_version }, forwarded_by: None }
    }

    /// A node's `deleting` (`NodeDeleting`), published by `publisher` at `topology_version`.
    pub fn node_deleting(op: LifecycleOp, publisher: PublisherId, topology_version: u64) -> Self {
        Self { subject: StatusSubject::Node(op), status: "deleting".into(), order: StatusOrder::TopologyVersion { publisher, topology_version }, forwarded_by: None }
    }

    /// A node's `deleted` (`NodeDeleted`).
    pub fn node_deleted(op: LifecycleOp) -> Self {
        Self { subject: StatusSubject::Node(op), status: "deleted".into(), order: StatusOrder::Once, forwarded_by: None }
    }

    /// `mesh`'s `status` (`MeshStatus`), published by `publisher` at `changed_at_rafka_ms`.
    pub fn mesh(mesh: &str, status: &str, publisher: &str, changed_at_rafka_ms: u64) -> Self {
        Self { subject: StatusSubject::Mesh(mesh.to_string()), status: status.to_string(), order: StatusOrder::Changed { publisher: publisher.to_string(), changed_at_rafka_ms }, forwarded_by: None }
    }

    /// The fabric's `status` (`FabricStatus`), published by `publisher` at `changed_at_rafka_ms`.
    pub fn fabric(fabric: &FabricId, status: &str, publisher: &str, changed_at_rafka_ms: u64) -> Self {
        Self { subject: StatusSubject::Fabric(fabric.clone()), status: status.to_string(), order: StatusOrder::Changed { publisher: publisher.to_string(), changed_at_rafka_ms }, forwarded_by: None }
    }

    /// The same status, forwarded by `by` (`None`: as its publisher sent it).
    pub fn forwarded_by(mut self, by: Option<String>) -> Self {
        self.forwarded_by = by;
        self
    }

    /// What it is the status of.
    pub fn r#type(&self) -> StatusType {
        match self.subject {
            StatusSubject::Node(_) => StatusType::Node,
            StatusSubject::Mesh(_) => StatusType::Mesh,
            StatusSubject::Fabric(_) => StatusType::Fabric,
        }
    }

    /// Its subject.
    pub fn subject(&self) -> &StatusSubject {
        &self.subject
    }

    /// The status itself.
    pub fn status(&self) -> &str {
        &self.status
    }

    /// How it is ordered against the one held.
    pub fn order(&self) -> &StatusOrder {
        &self.order
    }

    /// The node that forwarded it, if any.
    pub fn forwarder(&self) -> Option<&str> {
        self.forwarded_by.as_deref()
    }

    /// Its frame on the wire: one of today's five.
    pub fn frame(&self) -> Frame {
        let forwarded_by = self.forwarded_by.clone();
        match (&self.subject, &self.order) {
            (StatusSubject::Node(op), StatusOrder::TopologyVersion { publisher, topology_version }) if self.status == "stopped" => Frame::NodeStopped { op: op.clone(), forwarded_by, publisher: publisher.clone(), topology_version: *topology_version },
            (StatusSubject::Node(op), StatusOrder::TopologyVersion { publisher, topology_version }) => Frame::NodeDeleting { op: op.clone(), forwarded_by, publisher: publisher.clone(), topology_version: *topology_version },
            (StatusSubject::Node(op), _) => Frame::NodeDeleted { op: op.clone(), forwarded_by },
            (StatusSubject::Mesh(mesh), StatusOrder::Changed { publisher, changed_at_rafka_ms }) => Frame::MeshStatus { mesh: mesh.clone(), status: self.status.clone(), publisher: publisher.clone(), forwarded_by, changed_at_rafka_ms: *changed_at_rafka_ms },
            (StatusSubject::Fabric(fabric), StatusOrder::Changed { publisher, changed_at_rafka_ms }) => Frame::FabricStatus { fabric: fabric.clone(), status: self.status.clone(), publisher: publisher.clone(), forwarded_by, changed_at_rafka_ms: *changed_at_rafka_ms },
            (StatusSubject::Mesh(_) | StatusSubject::Fabric(_), _) => unreachable!("a mesh's and the fabric's status are built ordered by their change (Status::mesh, Status::fabric)"),
        }
    }

    /// The status `f` carries, when it is one of the five status frames.
    pub fn of(f: &Frame) -> Option<Self> {
        match f.data() {
            Frame::NodeStopped { op, forwarded_by, publisher, topology_version } => Some(Self::node_stopped(op.clone(), publisher.clone(), *topology_version).forwarded_by(forwarded_by.clone())),
            Frame::NodeDeleting { op, forwarded_by, publisher, topology_version } => Some(Self::node_deleting(op.clone(), publisher.clone(), *topology_version).forwarded_by(forwarded_by.clone())),
            Frame::NodeDeleted { op, forwarded_by } => Some(Self::node_deleted(op.clone()).forwarded_by(forwarded_by.clone())),
            Frame::MeshStatus { mesh, status, publisher, forwarded_by, changed_at_rafka_ms } => Some(Self::mesh(mesh, status, publisher, *changed_at_rafka_ms).forwarded_by(forwarded_by.clone())),
            Frame::FabricStatus { fabric, status, publisher, forwarded_by, changed_at_rafka_ms } => Some(Self::fabric(fabric, status, publisher, *changed_at_rafka_ms).forwarded_by(forwarded_by.clone())),
            _ => None,
        }
    }
}

/// The fabric status a node holds, with its publisher and the instant of the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricStatus {
    /// The status.
    pub status: String,
    /// The node that authored it.
    pub publisher: String,
    /// The Rafka-time of the change.
    pub changed_at_rafka_ms: u64,
}

/// A mesh's status a node holds, with its publisher and the instant of the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshStatus {
    /// The status.
    pub status: String,
    /// The node that authored it.
    pub publisher: String,
    /// The Rafka-time of the change.
    pub changed_at_rafka_ms: u64,
}

/// How many times one status change is sent: at once, then once per [`STATUS_EVERY`]
/// (gossip.md §3.2). After the fifth send there is nothing more until the next change.
pub const STATUS_SENDS: u32 = 5;

/// The spacing of a status change's reinforcing sends.
pub const STATUS_EVERY: Duration = Duration::from_secs(1);

/// A status change: the status and the instant it changed. Every send of one change carries both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusFact {
    /// The status.
    pub status: String,
    /// The Rafka-time of the change.
    pub changed_at_rafka_ms: u64,
}

/// The bounded reinforcement of one status: a change is sent at once, then once per second, five
/// sends in all, then nothing; a status that did not change sends nothing. A pure state machine
/// over an explicit clock: the caller says when it observes the status and when it asks what is
/// due, and sends what it is given.
#[derive(Debug, Default)]
pub(crate) struct StatusReinforcement {
    pub(crate) held: Option<StatusFact>,
    pub(crate) sent: u32,
    pub(crate) next_at: Option<Instant>,
    /// The held fact was adopted from the previous publisher and has not been republished: it waits
    /// for the new publisher's checklist to complete.
    pub(crate) awaiting_checklist: bool,
}

impl StatusReinforcement {
    /// The status as observed at `now` (`now_ms` stamps a change). A change returns the fact to
    /// send at once (the first of its five sends) and replaces any change still being reinforced;
    /// the same status returns nothing.
    pub fn observe(&mut self, status: &str, now: Instant, now_ms: u64) -> Option<StatusFact> {
        if self.held.as_ref().is_some_and(|h| h.status == status) {
            return None;
        }
        let fact = StatusFact { status: status.to_string(), changed_at_rafka_ms: now_ms };
        self.held = Some(fact.clone());
        self.awaiting_checklist = false;
        self.sent = 1;
        self.next_at = (self.sent < STATUS_SENDS).then(|| now + STATUS_EVERY);
        Some(fact)
    }

    /// Hold `fact` as heard from the previous publisher: no transition is invented and nothing is
    /// sent until [`Self::republish`] says the new publisher's checklist completed.
    pub fn adopt(&mut self, fact: StatusFact) {
        self.held = Some(fact);
        self.sent = STATUS_SENDS;
        self.next_at = None;
        self.awaiting_checklist = true;
    }

    /// Whether the held fact is adopted and not yet republished.
    pub fn awaiting_checklist(&self) -> bool {
        self.awaiting_checklist
    }

    /// The adopted fact, sent for the first of its five sends at `now`: the same status and the
    /// same `changed_at_rafka_ms` the previous publisher sent, because the state did not change.
    /// Nothing when no adopted fact waits.
    pub fn republish(&mut self, now: Instant) -> Option<StatusFact> {
        if !self.awaiting_checklist {
            return None;
        }
        self.awaiting_checklist = false;
        self.sent = 1;
        self.next_at = (self.sent < STATUS_SENDS).then(|| now + STATUS_EVERY);
        self.held.clone()
    }

    /// The next reinforcing send due at `now`, if any.
    pub fn due(&mut self, now: Instant) -> Option<StatusFact> {
        let at = self.next_at?;
        if now < at {
            return None;
        }
        self.sent += 1;
        self.next_at = (self.sent < STATUS_SENDS).then(|| at + STATUS_EVERY);
        self.held.clone()
    }

    /// When the next reinforcing send is due; `None` once the five sends are done.
    pub fn next_due(&self) -> Option<Instant> {
        self.next_at
    }

    pub fn held(&self) -> Option<&StatusFact> {
        self.held.as_ref()
    }

    /// Forget the status and any sends still to come: the publisher role ended.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Which status a [`StatusPublisher`] authors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusScope {
    /// A mesh's status, by mesh name.
    Mesh(String),
    /// The fabric's status.
    Fabric(FabricId),
}

/// The author of one status frame: `node`, while it holds the role (the mesh primary for a mesh's
/// status, the fabric primary for the fabric's) and only then. Losing the role ends the sends still
/// to come.
#[derive(Debug)]
pub struct StatusPublisher {
    pub(crate) node: String,
    pub(crate) scope: StatusScope,
    pub(crate) author: Option<(crate::envelope::Birth, u64)>,
    pub(crate) context: Option<(StatusFact, crate::envelope::Context)>,
    pub(crate) role: bool,
    pub(crate) schedule: StatusReinforcement,
}

impl StatusPublisher {
    /// A publisher run by `node` for `scope`; it holds no role until [`StatusPublisher::set_role`].
    pub fn new(node: &str, scope: StatusScope) -> Self {
        Self { node: node.to_string(), scope, author: None, context: None, role: false, schedule: StatusReinforcement::default() }
    }

    /// Hold (or lose) the publisher role. Losing it forgets the status and the sends still to come.
    pub fn set_role(&mut self, primary: bool) {
        if self.role && !primary {
            self.schedule.reset();
            self.author = None;
            self.context = None;
        }
        self.role = primary;
    }

    /// Whether the node holds the publisher role.
    pub fn has_role(&self) -> bool {
        self.role
    }

    /// Set the author birth and committed primary revision this publisher stamps onto new status frames.
    pub fn set_author(&mut self, author: Option<(crate::envelope::Birth, u64)>) {
        self.author = author;
    }

    /// Hand `fact` directly to this mesh status publisher as its adopted held fact (Q272;
    /// node-create.md "A node-admin created into a live mesh" step 4), never through the gossip receive view.
    pub fn adopt(&mut self, fact: StatusFact) -> bool {
        if mesh_state_rank(&fact.status).is_none() || (fact.changed_at_rafka_ms == 0 && fact.status != "pending") {
            return false;
        }
        if let Some(held) = self.schedule.held() {
            if held == &fact && self.schedule.awaiting_checklist() {
                return true;
            }
            let advances = mesh_state_rank(&fact.status) > mesh_state_rank(&held.status)
                || (held.status == fact.status && fact.changed_at_rafka_ms > held.changed_at_rafka_ms);
            if !advances {
                return false;
            }
        }
        self.schedule.adopt(fact);
        self.context = None;
        true
    }

    /// The status fact this publisher currently holds, if any.
    pub fn held(&self) -> Option<StatusFact> {
        self.schedule.held().cloned()
    }

    pub(crate) fn frame(&mut self, fact: &StatusFact) -> Frame {
        let frame = match &self.scope {
            StatusScope::Mesh(mesh) => Status::mesh(mesh, &fact.status, &self.node, fact.changed_at_rafka_ms),
            StatusScope::Fabric(fabric) => Status::fabric(fabric, &fact.status, &self.node, fact.changed_at_rafka_ms),
        }
        .frame();
        if let Some((_, context)) = self.context.as_ref().filter(|(held, _)| held == fact) {
            return frame.with_context(Some(context.clone()));
        }
        if let Some((author, revision)) = &self.author {
            let frame = frame.authored(author, *revision, fact.changed_at_rafka_ms);
            self.context = Some((fact.clone(), frame.context().expect("authored status context").clone()));
            frame
        } else { frame }
    }

    /// The status as observed: a change is the frame to send at once. Without the role: nothing.
    ///
    /// A mesh's status (Q272; node-create.md "A node-admin created into a live mesh" step 4: the joined node-admin, as mesh-primary,
    /// authors its MeshStatus after its fresh planned-birth checklist): the publisher sends only a state
    /// it holds. Holding nothing and hearing nothing, it sends nothing, whatever `status` (its own
    /// projection) says. A heard state with an instant is adopted exactly, the state and its
    /// `changed_at_rafka_ms` (fabric-primary.md §7: a takeover adopts the existing state and invents no
    /// transition), and nothing is sent while the checklist is open; when it completes the state held
    /// goes out, or the projection's forward change stamped then. A heard `pending` with no instant is
    /// a mesh's first state (Day 0, a new mesh's first admin): stamped here, once, and sent at once.
    /// A projection behind the state held never replaces it.
    ///
    /// The fabric's status: a publisher holding the role for the first time adopts the status heard
    /// from the previous publisher when it is the one observed, silent until `checklist_complete`;
    /// any other status is a change like any other.
    pub fn observe(&mut self, status: &str, heard: Option<StatusFact>, checklist_complete: bool, now: Instant, now_ms: u64) -> Option<Frame> {
        if !self.role {
            return None;
        }
        if matches!(self.scope, StatusScope::Mesh(_)) {
            return self.observe_mesh(status, heard, checklist_complete, now, now_ms).map(|f| self.frame(&f));
        }
        if self.schedule.held().is_none() {
            if let Some(h) = heard.filter(|h| h.status == status) {
                self.schedule.adopt(h);
            }
        }
        if self.schedule.awaiting_checklist() && self.schedule.held().is_some_and(|h| h.status == status) {
            return checklist_complete.then(|| self.schedule.republish(now)).flatten().map(|f| self.frame(&f));
        }
        self.schedule.observe(status, now, now_ms).map(|f| self.frame(&f))
    }

    /// [`Self::observe`] for a mesh: the fact to send, if any.
    pub(crate) fn observe_mesh(&mut self, status: &str, heard: Option<StatusFact>, checklist_complete: bool, now: Instant, now_ms: u64) -> Option<StatusFact> {
        if self.schedule.held().is_none() || self.schedule.awaiting_checklist() {
            if let Some(h) = heard.filter(|h| mesh_state_rank(&h.status).is_some()) {
                if h.changed_at_rafka_ms == 0 && self.schedule.held().is_none() {
                    return (h.status == "pending").then(|| self.schedule.observe(&h.status, now, now_ms)).flatten();
                }
                let _ = self.adopt(h);
            }
        }
        let held = self.schedule.held()?.clone();
        if held.changed_at_rafka_ms == 0 {
            self.schedule.reset();
            return (held.status == "pending").then(|| self.schedule.observe(&held.status, now, now_ms)).flatten();
        }
        let forward = mesh_state_rank(status).zip(mesh_state_rank(&held.status)).is_some_and(|(projected, held)| projected > held);
        // A forward change is stamped after the state it replaces: a holder takes only a newer instant.
        let stamp = now_ms.max(held.changed_at_rafka_ms + 1);
        if self.schedule.awaiting_checklist() {
            if !checklist_complete {
                return None;
            }
            return if forward { self.schedule.observe(status, now, stamp) } else { self.schedule.republish(now) };
        }
        forward.then(|| self.schedule.observe(status, now, stamp)).flatten()
    }

    /// Whether the publisher holds an adopted status it has not republished: its checklist is open.
    pub fn awaiting_checklist(&self) -> bool {
        self.role && self.schedule.awaiting_checklist() && self.schedule.held().is_some_and(|h| h.changed_at_rafka_ms > 0)
    }

    /// The reinforcing send due at `now`: the same fact, the same instant, as the first send.
    pub fn due(&mut self, now: Instant) -> Option<Frame> {
        if !self.role {
            return None;
        }
        self.schedule.due(now).map(|f| self.frame(&f))
    }

    /// When the next send of the status is due, when the role is held and a send remains.
    pub fn next_due(&self) -> Option<Instant> {
        self.role.then(|| self.schedule.next_due()).flatten()
    }
}

/// A mesh's lifecycle state by its order (states.md: a mesh is `pending` -> `ready` -> `leaving` ->
/// `dead`); `None` for any other word.
pub(crate) fn mesh_state_rank(status: &str) -> Option<u8> {
    ["pending", "ready", "leaving", "dead"].iter().position(|s| *s == status).map(|p| p as u8)
}

/// The mesh status a `Frame::MeshStatus` carries for a ready mesh (states.md: a mesh is
/// `pending` -> `ready` -> `leaving` -> `dead`).
pub const MESH_READY: &str = "ready";

/// The statuses a node holds, each until the next change: a mesh's by its name, the fabric's. A
/// frame older than the one held is refused by name, the same one again is taken once.
#[derive(Debug, Clone)]
pub struct StatusView {
    pub(crate) meshes: Arc<Mutex<HashMap<String, HeldStatus<MeshStatus>>>>,
    pub(crate) fabric: Arc<Mutex<Option<HeldStatus<FabricStatus>>>>,
    /// Ticks each time a mesh status is taken or dropped: a routing fact (states.md "Routing"). A
    /// local observation; nothing is sent.
    pub(crate) mesh_changes: Arc<tokio::sync::watch::Sender<u64>>,
}

#[derive(Debug, Clone)]
pub(crate) struct HeldStatus<T> { value: T, context: Option<crate::envelope::Context> }
impl<T> std::ops::Deref for HeldStatus<T> { type Target = T; fn deref(&self) -> &T { &self.value } }


impl Default for StatusView {
    fn default() -> Self {
        Self { meshes: Arc::default(), fabric: Arc::default(), mesh_changes: Arc::new(tokio::sync::watch::Sender::new(0)) }
    }
}

impl StatusView {
    /// Hold what `f` carries if it is newer than what is held; whether it was taken.
    pub fn take(&self, f: &Frame, fabric: &FabricId) -> bool {
        let context = f.context();
        // A mesh's or the fabric's status, read as its one Rust type (gossip-by-kind.md §5.3).
        let Some(taken) = Status::of(f) else { return false };
        let StatusOrder::Changed { publisher, changed_at_rafka_ms } = taken.order() else { return false };
        let status = &taken.status;
        match taken.subject() {
            StatusSubject::Mesh(mesh) => {
                let mut held = self.meshes.lock().unwrap();
                // The same fact under its author's context replaces one held without any (a hand-off's
                // state or a self-applied one, held before its publisher sent it; Q272): what this node
                // holds and serves from then on is the authored frame.
                let authors_held = context.is_some() && held.get(mesh).is_some_and(|h| h.context.is_none() && h.status == *status && h.changed_at_rafka_ms == *changed_at_rafka_ms);
                if !authors_held && !crate::receive_order::admit_status_context("mesh-status", mesh, held.get(mesh).and_then(|h| h.context.as_ref()), f.context(), held.get(mesh).map(|h| h.status.as_str()).unwrap_or(""), status).unwrap_or_else(|| crate::receive_order::admit_status("mesh-status", mesh, held.get(mesh).map(|h| (h.publisher.as_str(), h.changed_at_rafka_ms, h.status.as_str())), status, publisher, *changed_at_rafka_ms)) {
                    return false;
                }
                held.insert(mesh.clone(), HeldStatus { value: MeshStatus { status: status.clone(), publisher: publisher.clone(), changed_at_rafka_ms: *changed_at_rafka_ms }, context: context.cloned() });
                drop(held);
                self.mesh_changes.send_modify(|v| *v += 1);
                true
            }
            StatusSubject::Fabric(f) if f == fabric => {
                let mut held = self.fabric.lock().unwrap();
                if !crate::receive_order::admit_status_context("fabric-status", f.as_str(), held.as_ref().and_then(|h| h.context.as_ref()), context, held.as_ref().map(|h| h.status.as_str()).unwrap_or(""), status).unwrap_or_else(|| crate::receive_order::admit_status("fabric-status", f.as_str(), held.as_ref().map(|h| (h.publisher.as_str(), h.changed_at_rafka_ms, h.status.as_str())), status, publisher, *changed_at_rafka_ms)) {
                    return false;
                }
                *held = Some(HeldStatus { value: FabricStatus { status: status.clone(), publisher: publisher.clone(), changed_at_rafka_ms: *changed_at_rafka_ms }, context: context.cloned() });
                true
            }
            _ => false,
        }
    }

    /// Ticks each time a mesh status is taken or dropped.
    pub fn mesh_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.mesh_changes.subscribe()
    }

    /// The status held for `mesh`.
    pub fn mesh(&self, mesh: &str) -> Option<MeshStatus> {
        self.meshes.lock().unwrap().get(mesh).map(|h| h.value.clone())
    }

    /// The committed primary revision of the status frame held for `mesh`, if authored.
    pub fn mesh_revision(&self, mesh: &str) -> Option<u64> {
        self.meshes.lock().unwrap().get(mesh).and_then(|h| h.context.as_ref()?.primary_revision())
    }

    /// Every mesh status entry held, copied out under one short lock.
    pub(crate) fn held_meshes_snapshot(&self) -> HashMap<String, HeldStatus<MeshStatus>> {
        self.meshes.lock().unwrap().iter().map(|(m, h)| (m.clone(), h.clone())).collect()
    }

    /// Forget the status held for `mesh`: the mesh left, and nothing of it is kept or replayed.
    /// Answers the status that was held. A status dropped is a routing fact as one taken is.
    pub fn forget_mesh(&self, mesh: &str) -> Option<String> {
        let dropped = self.meshes.lock().unwrap().remove(mesh).map(|h| h.value.status);
        if dropped.is_some() {
            self.mesh_changes.send_modify(|v| *v += 1);
        }
        dropped
    }

    /// Every mesh status held, by mesh name.
    pub fn meshes(&self) -> Vec<(String, MeshStatus)> {
        self.meshes.lock().unwrap().iter().map(|(m, h)| (m.clone(), h.value.clone())).collect()
    }

    /// The fabric status held.
    pub fn fabric(&self) -> Option<FabricStatus> {
        self.fabric.lock().unwrap().as_ref().map(|h| h.value.clone())
    }

    /// Every status held, as the frames its holder `me` replays: the original publisher and the
    /// original instant are kept; only a status `me` did not author names `me` as `forwarded_by`. A
    /// mesh status held without its author's context (a hand-off's state or a self-applied one, before
    /// its publisher sent it; Q272) is not replayed: no author sent it, and its publisher's authored
    /// frame replaces it when sent.
    pub fn held_frames(&self, fabric: &FabricId, me: &str) -> Vec<Frame> {
        let by = |publisher: &str| (publisher != me).then(|| me.to_string());
        let held_meshes = self.held_meshes_snapshot();
        let held_fabric = self.fabric.lock().unwrap().clone();
        let mut out: Vec<Frame> = held_meshes
            .into_iter()
            .filter(|(_, h)| h.context.is_some())
            .map(|(mesh, h)| Status::mesh(&mesh, &h.status, &h.publisher, h.changed_at_rafka_ms).forwarded_by(by(&h.publisher)).frame().with_context(h.context))
            .collect();
        out.sort_by_key(|f| if let Frame::MeshStatus { mesh, .. } = f.data() { mesh.clone() } else { String::new() });
        if let Some(h) = held_fabric {
            out.push(Status::fabric(fabric, &h.status, &h.publisher, h.changed_at_rafka_ms).forwarded_by(by(&h.publisher)).frame().with_context(h.context));
        }
        out
    }
}
