//! What a node's topology view takes from a frame: the receive rules that hold a `Digest`, write a
//! full or a delta to the one copy of its mesh and to the digest view, and hold a status (gossip.md
//! §3.1, §3.3).


use crate::aggregate::{Chunk, DeltaChunk, Forwarder, Full, Gap, Moved, PublisherId, AggregateReceiver, HeldAggregate, Taken, Written};
use rdm_core::{FabricId, LifecycleOp, MeshDigest, Primary, PrimaryRole};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;


use crate::{frame::*, status::*, primary::*, channel::*, digest_view::*};

/// What a mesh topology frame (a full or a delta) left behind.
#[derive(Default)]
pub(crate) struct TopologyFrameTaken {
    /// The digests applied to the view, for their addresses.
    pub(crate) carried: Vec<MeshDigest>,
    /// A complete backbone full of a peer Mesh written to the topology view: its base, its version and
    /// the delta it wrote, for the forwarding primary.
    pub(crate) source: Option<Box<Written>>,
    /// A complete full written from the backbone: who published it, when, and the publisher's own
    /// digest as the full lists it. Evidence for the clock's discipline.
    pub(crate) publication: Option<Publication>,
}

/// What a receiver held of one source before a projection frame of it applied.
pub(crate) struct Prior {
    pub(crate) held: Option<(Option<PublisherId>, u64)>,
    pub(crate) stops: BTreeSet<(String, String)>,
}

impl Prior {
    pub(crate) fn of(rx: &AggregateReceiver, mesh: &str) -> Self {
        Self { held: rx.held_version(mesh), stops: Target::stops_of(rx.held_full(mesh)) }
    }

    /// The target of a frame sent by `publisher` (its author's label, empty when none was heard) at
    /// `version` arriving on `side`, `after` the projection the receiver holds once it applied.
    pub(crate) fn target(self, publisher: &impl std::fmt::Display, side: Side, version: u64, after: Option<&Full>) -> Target {
        Target {
            publisher: publisher.to_string(),
            channel: side.name(),
            held: self.held.map(|(p, v)| (p.map(|p| p.to_string()).unwrap_or_default(), v)),
            version,
            prior_stops: self.stops,
            post_stops: Target::stops_of(after),
        }
    }
}

/// A backbone aggregate as its author stamped it.
pub(crate) struct Publication {
    pub(crate) publisher: PublisherId,
    /// The author's rafka-time when it published (the `TopologyFull` envelope's `time`).
    pub(crate) published_at_rafka_ms: u64,
    /// The publisher's own digest in the aggregate, matched by its exact birth.
    pub(crate) publisher_digest: Option<MeshDigest>,
}

/// What a node's view holds, fed by its mesh channel (and, for an admin, the
/// backbone): every member's latest digest, the fabric status, and where it
/// learned each mesh from.
#[derive(Clone, Default)]
pub(crate) struct View {
    /// This node's own mesh: its members are heard directly on the mesh channel.
    pub(crate) mesh: String,
    /// This node's path name, for the spans it emits.
    pub(crate) node: String,
    pub(crate) digest_view: DigestView,
    /// Whether this node is its mesh's primary now: only a primary replays the statuses it holds.
    pub(crate) primary: Arc<AtomicBool>,
    pub(crate) via: Arc<Mutex<HashMap<String, &'static str>>>,
    /// The one copy of each mesh this node holds in its topology view (gossip.md §3.3, Q409): what the
    /// backbone's fulls, its mesh channel's deltas and its topology reads all write, each only its
    /// delta, under this one hold.
    pub(crate) copies: Arc<Mutex<AggregateReceiver>>,
    /// The one version this node holds per source (R-RX3): consulted by the backbone's and the mesh
    /// channel's receive paths and by the topology reads, and moved by what each writes.
    pub(crate) order: Arc<Mutex<crate::receive_order::ReceiveOrder>>,
    /// What this node, as its Mesh's primary, has forwarded into its Mesh and published of it.
    pub(crate) forwarder: Arc<Mutex<Forwarder>>,
    /// Wakes the top-up when a source of the Mesh channel desynchronizes.
    pub(crate) desync: Arc<tokio::sync::Notify>,
    /// The name the mesh-primary's row last gave this birth (`UpdateName`, R12), shared with its
    /// the topology view; `None` while the birth holds its launch name (`node`).
    pub(crate) held_name: Arc<Mutex<Option<rdm_core::PathName>>>,
    /// The highest version of this node's own mesh a topology read wrote while this node was its
    /// mesh's primary, with that topology: its next publication continues the mesh's version from it
    /// (Q391: the mesh's sequence never goes back, whichever mesh-primary sends it).
    pub(crate) read_version: Arc<Mutex<Option<(u64, Full)>>>,
}

impl View {
    /// Whether this node is a node-admin: it manages its mesh's births with the runtime fact of each,
    /// which their mesh-primary took from their join reports and which reaches it by a read of its own
    /// mesh from that primary (Q400), or, with no live primary to read from (its mesh's primary record
    /// names a birth proven gone, or it is its mesh's primary), from each birth's record in node-storage
    /// (Q404 A).
    pub(crate) fn reads_runtime_facts(&self) -> bool {
        self.digest_view.own_is_node_admin()
    }

    /// The sources this node serves on a topology read, one per mesh, from the one copy of each it
    /// holds in its topology view (Q409; R-RX3, gossip.md §3.1: one held version per source, whichever
    /// channel or read wrote it). As its mesh's primary, its own mesh is its last publication of it in
    /// this term ([`Forwarder::own_aggregate`]), and none before it. `mesh`'s alone when one is named:
    /// no other mesh's source is copied.
    pub(crate) fn held_sources_of(&self, mesh: Option<&str>) -> Vec<HeldAggregate> {
        let mut held = self.copies.lock().unwrap().aggregates_of(mesh);
        if self.primary.load(Ordering::Relaxed) {
            held.retain(|s| s.mesh != self.mesh);
            if let Some(own) = self.forwarder.lock().unwrap().own_aggregate().filter(|own| mesh.is_none_or(|want| want == own.mesh)) {
                held.push(own);
                held.sort_by(|a, b| a.mesh.cmp(&b.mesh));
            }
        }
        held
    }

    pub(crate) fn new(mesh: &str, node: &str) -> Self {
        let mut view = Self { mesh: mesh.to_string(), node: node.to_string(), ..Self::default() };
        *view.digest_view.observer_name.lock().unwrap() = node.to_string();
        *view.digest_view.own_mesh.lock().unwrap() = mesh.to_string();
        view.digest_view.held_name = view.held_name.clone();
        // One flag: the view replays as its mesh's primary while the digest view authors its members' statuses.
        view.primary = view.digest_view.primary.clone();
        view.forwarder.lock().unwrap().set_own_mesh(mesh);
        let forwarder = view.forwarder.clone();
        *view.digest_view.on_mesh_deleted.0.lock().unwrap() = Some(Arc::new(move |m: &str| forwarder.lock().unwrap().remove(m)));
        view
    }

    /// The name this birth currently holds: the name its row gave it (`held_name`), else its launch name.
    pub(crate) fn current_name(&self) -> String {
        self.digest_view.own.lock().unwrap().clone().unwrap_or_else(|| self.node.clone())
    }

    /// Hold a status frame if it is newer than the one held: the one function for a status heard on a
    /// channel, replayed or answered by an entry pull. Whoever published it, a status is taken for what
    /// it carries (R-RX1); the held status orders it by its `changed_at_rafka_ms`. Every mesh status
    /// heard opens one span saying whether it was taken (gossip.md: "the held status, until the next
    /// change"; a repeat of the change held, or an older one, is not); one taken is a routing fact on
    /// receipt (states.md "Routing"), and the span names how many of the mesh's held births
    /// application routing selects from now on.
    pub(crate) fn take_status(&self, f: &Frame, fabric: &FabricId, via: &'static str) {
        let Frame::MeshStatus { mesh, status, publisher, forwarded_by, changed_at_rafka_ms } = f.data() else {
            if let Frame::FabricStatus { fabric: subject, status, publisher, forwarded_by, changed_at_rafka_ms } = f.data() {
                let span = tracing::info_span!(
                    "rdm.mesh.topology.update.via-fabric-status",
                    node = %self.current_name(),
                    fabric = %subject,
                    status = %status,
                    publisher = %publisher,
                    forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    changed_at_rafka_ms = *changed_at_rafka_ms,
                    via,
                    taken = tracing::field::Empty,
                );
                let _in = span.enter();
                span.record("taken", self.digest_view.statuses.take(f, fabric));
            }
            return;
        };
        let span = tracing::info_span!(
            "rdm.mesh.topology.update.via-mesh-status",
            node = %self.current_name(),
            mesh = %mesh,
            status = %status,
            publisher = %publisher,
            forwarded_by = forwarded_by.as_deref().unwrap_or(""),
            changed_at_rafka_ms = *changed_at_rafka_ms,
            via,
            taken = tracing::field::Empty,
            births_held = tracing::field::Empty,
            births_routable = tracing::field::Empty,
        );
        let _in = span.enter();
        let taken = self.digest_view.statuses.take(f, fabric);
        span.record("taken", taken);
        if !taken {
            // The receive framework has already named this refusal or duplicate.
            return;
        }
        // Counted under the digest view's lock, never by copying every digest held (Q268).
        let births = self.digest_view.node_ids_of_mesh(mesh);
        let routable = births.iter().filter(|id| self.digest_view.routable(id)).count();
        span.record("births_held", births.len());
        span.record("births_routable", routable);
        tracing::info!("a mesh status is held: application routing selects the mesh's births only while it is ready");
    }

    /// Hold what `f` carries; the digests it carries (for their addresses). A mesh topology frame is
    /// not taken here: see [`View::take_topology_frame`].
    pub(crate) fn take(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        self.take_with(f, fabric, via).0
    }

    /// [`View::take`], and whether a digest the frame carried was newer than the one held: the
    /// view's admission of a heartbeat. A heartbeat taken is answered as the digest view now holds it, with
    /// the status held for its birth (a heartbeat carries none).
    pub(crate) fn take_with(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> (Vec<MeshDigest>, bool) {
        if let (Frame::Digest { .. }, Some(source)) = (f.data(), f.context().map(|c| c.source.clone())) {
            if &source.fabric == fabric {
                // Every Digest carries the birth's details (gossip.md §3.1): the birth is named from the
                // Digest heard, whichever of its Digests this node missed.
                let Some(digest) = f.digest_of(&self.mesh) else { return (Vec::new(), false) };
                let digest = &digest;
                let channel = if via == "mesh-channel" { format!("mesh:{}", self.mesh) } else { via.to_string() };
                let fresh = self.digest_view.record_with(digest.clone(), Instant::now(), &self.current_name(), &channel);
                if !fresh {
                    // A node held stopped heard again: its mesh-primary's view is pulled (a sync issue).
                    // Each such Digest pulls again until a read moves past the stop.
                    if self.digest_view.stopped_heard_again(digest) {
                        if self.copies.lock().unwrap().mark_desynced(&self.mesh, crate::aggregate::Gap::StoppedBirthHeard) {
                            tracing::info_span!("rdm.mesh.topology.update.via-stopped-birth-heard", node = %self.current_name(), node_id = %digest.node.node_id, incarnation_id = %digest.node.incarnation, digest_seq = digest.digest_seq)
                                .in_scope(|| tracing::info!("a node held stopped is heard again: its mesh-primary's view is pulled"));
                        }
                        self.desync.notify_one();
                    }
                    return (Vec::new(), false);
                }
                self.note(digest, via);
                // A node-admin manages its mesh's births with their runtime facts, which the mesh-primary
                // took from their join reports and serves on a read (Q400): a Digest of a birth of its mesh
                // it holds no runtime fact of is a sync issue, and each such Digest pulls its mesh-primary's
                // view again until a read supplies the fact, or, with no live primary to read from, reads
                // the birth's record in node-storage (Q404 A).
                if digest.node.mesh == self.mesh
                    && digest.status != rdm_core::model::NodeStatus::Leaving
                    && !self.digest_view.holds_runtime_fact(&digest.node.node_id, &digest.node.incarnation)
                    && self.reads_runtime_facts()
                {
                    if self.copies.lock().unwrap().mark_desynced(&self.mesh, crate::aggregate::Gap::NoRuntimeFact) {
                        tracing::info_span!("rdm.mesh.topology.update.via-no-runtime-fact", node = %self.current_name(), node_id = %digest.node.node_id, incarnation_id = %digest.node.incarnation, digest_seq = digest.digest_seq)
                            .in_scope(|| tracing::info!("a node-admin holds a birth of its mesh with no runtime fact of it: its mesh-primary's view is read"));
                    }
                    self.desync.notify_one();
                }
                tracing::info_span!(
                    "rdm.mesh.topology.update.via-digest",
                    node = %self.current_name(), node_id = %digest.node.node_id,
                    incarnation_id = %digest.node.incarnation, digest_seq = digest.digest_seq, via,
                ).in_scope(|| tracing::info!("the birth's newer heartbeat projection was installed"));
                let held = self.digest_view.get(digest.node.node_id.as_str()).map(|(d, _)| d).unwrap_or_else(|| digest.clone());
                return (vec![held], true);
            }
        }
        (self.take_other(f, fabric, via), false)
    }

    pub(crate) fn take_other(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        if crate::receive_order::refuse_reserved(f, &self.current_name(), via) { return Vec::new(); }
        // A status frame is read as its one Rust type (gossip-by-kind.md §5.3); each type keeps its rule.
        let Some(status) = Status::of(f) else { return Vec::new() };
        match (status.subject(), status.order()) {
            (StatusSubject::Node(op), StatusOrder::TopologyVersion { topology_version }) => {
                let kind = if status.status() == "deleting" { StatusEvent::Deleting } else { StatusEvent::Stopped };
                let op = &traced_by_envelope(f, op);
                // The mesh-primary that published it, for spans: its envelope's source.
                let publisher = f.context().map(|c| format!("{}@{}", c.source.node, c.source.incarnation)).unwrap_or_default();
                if !self.refuses_status_event(kind, op, &publisher, *topology_version, via) {
                    self.apply_status_event(kind, op, Some((publisher, *topology_version)), via);
                }
                Vec::new()
            }
            (StatusSubject::Node(op), _) => {
                let forwarded_by = status.forwarder();
                let op = &traced_by_envelope(f, op);
                let span = tracing::info_span!(
                    "rdm.mesh.topology.update.via-node-deleted",
                    node = %self.current_name(),
                    subject_path = %op.path, subject_name = %op.name,
                    subject_id = %op.node_id,
                    incarnation_id = %op.incarnation,
                    build_id = %op.build_id,
                    operation = %op.operation.action, operation_subject = ?op.operation.subject,
                    forwarded_by = forwarded_by.unwrap_or(""),
                    via,
                    applied = tracing::field::Empty,
                );
                let _in = span.enter();
                span.record("applied", self.digest_view.depart(op.clone()));
                Vec::new()
            }
            (StatusSubject::Mesh(_) | StatusSubject::Fabric(_), _) => {
                self.take_status(f, fabric, via);
                Vec::new()
            }
        }
    }

    /// Whether the status event `kind` of `op`'s birth, published by `publisher` at
    /// `topology_version` and heard through `via`, is refused. Whoever published it, the event is
    /// taken for what it carries (R-RX1): an event published before the newest of its mesh was
    /// already ignored by the receive path (R-RX3, [`crate::receive_order`]). A `NodeStopped` older
    /// than a held projection of its mesh that lists the exact birth without the stop is refused
    /// (node-status-heartbeat-connections.md §C "late stopped after starting"): the projection is the
    /// mesh's later word on the birth, and the mesh's version orders them, whichever of its
    /// mesh-primaries or executors published either (Q389, Q391). Only a projection strictly newer than
    /// the stop's version refuses it: an executor that is not the mesh's primary stamps the version it
    /// holds of the mesh, which the projection at that version predates, and a mesh-primary's
    /// projection at its stop's version already carries the stop.
    pub(crate) fn refuses_status_event(&self, kind: StatusEvent, op: &LifecycleOp, publisher: &str, topology_version: u64, via: &'static str) -> bool {
        let mesh = op.mesh.as_str();
        if kind != StatusEvent::Stopped {
            return false;
        }
        let key = (op.node_id.to_string(), op.incarnation.to_string());
        let copies = self.copies.lock().unwrap();
        let Some((_, held_version)) = copies.held_version(mesh) else { return false };
        if held_version <= topology_version {
            return false;
        }
        let Some(full) = copies.held_full(mesh) else { return false };
        let listed = full.digests().iter().any(|d| d.node.node_id == op.node_id && d.node.incarnation == op.incarnation);
        if !listed || Target::stops_of(Some(full)).contains(&key) {
            return false;
        }
        drop(copies);
        crate::receive_order::reject_lifecycle_version(kind.name(), mesh, held_version, topology_version);
        tracing::info_span!(
            "rdm.mesh.topology.reject.via-stop-older-than-projection",
            node = %self.current_name(),
            subject_path = %op.path, subject_name = %op.name,
            subject_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            build_id = %op.build_id,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            publisher = %publisher,
            topology_version,
            held_version,
            via,
        )
        .in_scope(|| tracing::info!("a NodeStopped older than the held projection of its mesh, which lists the birth without the stop: refused, the birth started after it"));
        true
    }

    /// Apply the status event `kind` of `op`'s birth to the view: the frame its publisher sent
    /// (`carried`: its publisher and the version at which the subject changed), or the event the
    /// executor itself publishes (`carried: None`; it authors the projection).
    pub(crate) fn apply_status_event(&self, kind: StatusEvent, op: &LifecycleOp, carried: Option<(String, u64)>, via: &str) -> bool {
        match kind {
            StatusEvent::Deleting => self.digest_view.deleting(op.clone()),
            StatusEvent::Stopped => {
                let prior = self.digest_view.get(op.node_id.as_str()).map(|(h, _)| h.status);
                let taken = self.digest_view.stopped_carried(op.clone(), carried);
                if taken {
                    self.note_member_stopped(op, prior, via);
                }
                taken
            }
        }
    }

    /// A `TopologyFull` chunk or a `TopologyDelta`, arrived on `side`. An aggregate is applied to the view
    /// only once every chunk of its (mesh, version, author) is held; a delta only at exactly its base.
    /// A frame that does not belong on its channel is refused by name: an aggregate travels on the
    /// backbone only (Q387). Whoever published it, a frame is taken for what it carries: one
    /// published before the newest the channel holds of its mesh was already ignored by the receive
    /// path (R-RX3, [`crate::receive_order`]), and only the mesh-primary publishes a mesh (R-RX2).
    /// A projection of this node's own mesh on its own mesh channel is refused by name: its mesh-primary
    /// puts nothing of its own mesh there, whose nodes publish themselves, and its view reaches this
    /// node only by a read (gossip.md §2, §3.3, Q387). What arrives is a frame from before that rule.
    fn own_mesh_on_its_channel(&self, mesh: &str, side: Side, publisher: &impl std::fmt::Display, frame: &'static str) -> bool {
        if side != Side::MeshChannel || mesh != self.mesh {
            return false;
        }
        tracing::info_span!("rdm.mesh.topology.reject.via-own-mesh-on-its-channel", node = %self.current_name(), mesh = %mesh, publisher = %publisher, frame)
            .in_scope(|| tracing::info!("a projection of this node's own mesh on its mesh channel: refused, its nodes publish themselves and its view comes by a read"));
        true
    }

    pub(crate) fn take_topology_frame(&self, f: &Frame, fabric: &FabricId, side: Side, order: &mut crate::receive_order::ReceiveOrder) -> TopologyFrameTaken {
        match f.data() {
            Frame::TopologyFull { mesh, mesh_id, chunk_index, chunk_count, nodes } => {
                // The envelope names the aggregate's author, the mesh's version and the time (Q389).
                let Some((author, version, time_ms)) = f.context().and_then(|c| Some((c.source.clone(), c.version()?, c.time_ms))) else { return TopologyFrameTaken::default() };
                if side != Side::Backbone {
                    tracing::info_span!(
                        "rdm.mesh.topology.reject.via-members-on-wrong-channel",
                        node = %self.current_name(),
                        channel = side.name(),
                        mesh = %mesh,
                        publisher = %format!("{}@{}", author.node, author.incarnation),
                        forwarded_by = "",
                    )
                    .in_scope(|| tracing::warn!("a TopologyFull off the backbone, the one channel an aggregate travels on: refused"));
                    return TopologyFrameTaken::default();
                }
                if self.outside_build(mesh, side) {
                    return TopologyFrameTaken::default();
                }
                let chunk = Chunk {
                    mesh: mesh.clone(),
                    mesh_id: mesh_id.clone(),
                    author: Some(author.clone()),
                    topology_version: version,
                    time_ms,
                    chunk_index: *chunk_index,
                    chunk_count: *chunk_count,
                    digests: nodes.iter().map(|n| n.digest(&author.fabric, mesh)).collect(),
                    runtimes: Vec::new(),
                };
                let author_name = chunk.author_name();
                // The receiver's lock is held until the view holds what the version names: a reader of
                // the held version never sees a version ahead of the view it describes.
                let mut rx = self.copies.lock().unwrap();
                let prior = Prior::of(&rx, mesh);
                let taken = rx.take_chunk(chunk);
                match taken {
                    Taken::Waiting { .. } => TopologyFrameTaken::default(),
                    Taken::Refused(crate::aggregate::Refusal::Duplicate | crate::aggregate::Refusal::Inconsistent | crate::aggregate::Refusal::OlderThanHeld { .. } | crate::aggregate::Refusal::OlderThanPending { .. }) => TopologyFrameTaken::default(),
                    Taken::Refused(r) => {
                        drop(rx);
                        tracing::info_span!(
                            "rdm.mesh.topology.reject.via-aggregate",
                            node = %self.current_name(),
                            channel = side.name(),
                            mesh = %mesh,
                            publisher = %author_name,
                            topology_version = version,
                            time_ms,
                            reason = ?r,
                        )
                        .in_scope(|| tracing::info!("an aggregate chunk refused: the held list stands"));
                        TopologyFrameTaken::default()
                    }
                    Taken::Written(w) => {
                        order.hold(&w.mesh, w.topology_version, rx.held_full(&w.mesh).and_then(Full::mesh_id));
                        let written = self.written(&w, fabric, side, prior);
                        drop(rx);
                        written
                    }
                }
            }
            Frame::TopologyDelta { mesh, base_version, chunk_count, .. } => {
                // The forwarding mesh-primary that authored it, and the version it moves the mesh to:
                // its envelope's `source` and `sequence` (Q389).
                let Some(chunk) = DeltaChunk::of_frame(f) else { return TopologyFrameTaken::default() };
                let (author, topology_version) = (chunk.author.clone(), &chunk.version.clone());
                let source = author.uri();
                if side == Side::Backbone {
                    tracing::info_span!("rdm.mesh.topology.reject.via-members-on-wrong-channel", node = %self.current_name(), channel = side.name(), mesh = %mesh, publisher = %source, forwarded_by = "")
                        .in_scope(|| tracing::warn!("a TopologyDelta on the backbone, where only complete aggregates travel: refused"));
                    return TopologyFrameTaken::default();
                }
                if self.own_mesh_on_its_channel(mesh, side, &source, "mesh-topology-delta") {
                    return TopologyFrameTaken::default();
                }
                if self.outside_build(mesh, side) {
                    return TopologyFrameTaken::default();
                }
                let mut rx = self.copies.lock().unwrap();
                let prior = Prior::of(&rx, mesh);
                let moved = rx.take_delta_chunk(chunk);
                match moved {
                    Moved::Applied { delta, moved, .. } => {
                        let held_full = rx.held_full(mesh);
                        tracing::info_span!(
                            "rdm.mesh.topology.update.via-delta",
                            node = %self.current_name(),
                            mesh = %mesh,
                            source = %source,
                            base_version = *base_version,
                            topology_version = *topology_version,
                            chunk_count = *chunk_count,
                            added = delta.added.len(),
                            changed = delta.changed.len(),
                            removed = delta.removed.len(),
                            deleted = held_full.map_or(0, |held| held.digests().iter().filter(|d| d.status == rdm_core::model::NodeStatus::Deleted).count()),
                        )
                        .in_scope(|| tracing::info!("a differential applied at exactly its base version, every chunk of it held"));
                        let full = Full::new(moved, Vec::new(), Vec::new());
                        let target = prior.target(&source, side, *topology_version, held_full);
                        let carried = self.apply_full(mesh, &full, held_full, fabric, side, &target, f.context().map(|c| c.time_ms).unwrap_or_default());
                        order.hold(mesh, *topology_version, held_full.and_then(Full::mesh_id));
                        drop(rx);
                        TopologyFrameTaken { carried, ..TopologyFrameTaken::default() }
                    }
                    Moved::Waiting { .. } | Moved::Duplicate { .. } | Moved::Inconsistent | Moved::Stale => TopologyFrameTaken::default(),
                    Moved::Malformed { chunk_index, chunk_count } => {
                        drop(rx);
                        tracing::info_span!("rdm.mesh.topology.reject.via-malformed-chunk", node = %self.current_name(), mesh = %mesh, source = %source, base_version = *base_version, topology_version = *topology_version, chunk_index, chunk_count)
                            .in_scope(|| tracing::info!("a differential chunk whose index is outside its count, or whose count is not its differential's: refused"));
                        TopologyFrameTaken::default()
                    }
                    Moved::Desynced { gap, .. } => {
                        drop(rx);
                        let held_version = match &gap {
                            Gap::Version { held, .. } => *held as i64,
                            _ => -1,
                        };
                        tracing::info_span!(
                            "rdm.mesh.topology.update.via-gap",
                            frame = "mesh-topology-delta", repair_requested = true,
                            node = %self.current_name(),
                            mesh = %mesh,
                            source = %source,
                            gap = %gap,
                            held_version,
                            base_version = *base_version,
                            topology_version = *topology_version,
                        )
                        .in_scope(|| tracing::info!("the delta does not follow what is held: this source is desynchronized and tops up from its own primary"));
                        self.desync.notify_one();
                        TopologyFrameTaken::default()
                    }
                }
            }
            _ => TopologyFrameTaken::default(),
        }
    }

    /// Whether a full or delta of the peer mesh `mesh` writes nothing: the accepted Build is the
    /// authority on which meshes the fabric has, and a mesh it does not name, of which this node
    /// holds no copy, is not a mesh this node takes members of from an old mesh topology. A mesh this
    /// node holds is a mesh it still tracks (a Leaving mesh) and is updated; a node that holds no
    /// Build takes everything.
    pub(crate) fn outside_build(&self, mesh: &str, side: Side) -> bool {
        if mesh == self.mesh || self.digest_view.accepted_has_mesh(mesh) != Some(false) {
            return false;
        }
        if self.copies.lock().unwrap().held_version(mesh).is_some() {
            return false;
        }
        tracing::info_span!("rdm.mesh.topology.reject.via-mesh-not-in-build", node = %self.current_name(), channel = side.name(), mesh)
            .in_scope(|| tracing::info!("a topology of a mesh the accepted Build does not name, of which this node holds no copy: nothing is written to the topology view"));
        true
    }

    /// One chunk of a topology read, heard on `side` (`Read` from this node's own mesh, `ReadPeer`
    /// from another's): consulted against the one held version of its source first (R-RX3), so a
    /// chunk below the version this node holds is ignored by name before the receiver reads it; a
    /// full that completes writes its delta to the one copy in the topology view and moves the held
    /// source version (gossip.md §3.1, Q409).
    pub(crate) fn take_read_chunk(&self, chunk: Chunk, side: Side, fabric: &FabricId) -> Taken {
        // The runtime and data dir of each exact birth the read serves: what a node-admin manages the
        // birth with (hydration), whichever version the read is of.
        for (node_id, incarnation, runtime) in &chunk.runtimes {
            self.digest_view.hold_runtime(node_id, incarnation, runtime.clone());
        }
        let own_digests = (side == Side::Read && chunk.mesh == self.mesh).then(|| chunk.digests.clone());
        let record_own = |digests: Option<Vec<MeshDigest>>| {
            if let Some(digests) = digests {
                let me_name = self.current_name();
                for d in digests.into_iter().filter(|d| &d.fabric_id == fabric && d.node.mesh == self.mesh && d.node.node_id.to_string() != me_name) {
                    if self.digest_view.get(d.node.node_id.as_str()).is_none_or(|(held, _)| held.node.incarnation != d.node.incarnation || held.digest_seq < d.digest_seq)
                        && self.digest_view.record_joined(d.clone())
                    {
                        self.note(&d, side.via());
                    }
                }
            }
        };
        let mut order = self.order.lock().unwrap();
        let offered_id = chunk.mesh_id.clone();
        if let Err(e) = order.admit_read(&chunk.mesh, chunk.topology_version, offered_id.as_ref()) {
            tracing::info_span!(
                "rdm.mesh.topology.reject.via-stale",
                node = %self.current_name(),
                channel = side.name(),
                frame = e.frame,
                subject = %e.source,
                held_key = %e.held,
                frame_key = %e.offered,
                publisher = %chunk.author_name(),
            )
            .in_scope(|| tracing::info!("a topology read of a version below the one this node holds of its source: ignored"));
            let held_version = order.floor(&chunk.mesh, offered_id.as_ref()).unwrap_or_default();
            drop(order);
            record_own(own_digests);
            return Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { held_version, offered_version: chunk.topology_version });
        }
        let mut rx = self.copies.lock().unwrap();
        let prior = Prior::of(&rx, &chunk.mesh);
        let taken = rx.take_read_chunk(chunk);
        if let Taken::Written(w) = &taken {
            order.hold(&w.mesh, w.topology_version, rx.held_full(&w.mesh).and_then(Full::mesh_id));
            self.written(w, fabric, side, prior);
            drop(rx);
            drop(order);
            // A read of its own mesh at the mesh-primary, after it took the role: the mesh's version
            // continues from it at its next publication (Q391).
            if w.mesh == self.mesh && self.primary.load(Ordering::Relaxed) {
                let mut read = self.read_version.lock().unwrap();
                if read.as_ref().is_none_or(|(held, _)| *held < w.topology_version) {
                    *read = Some((w.topology_version, w.full.clone()));
                }
            }
        } else {
            drop(rx);
            drop(order);
            if matches!(&taken, Taken::Refused(_)) {
                record_own(own_digests);
            }
        }
        taken
    }

    /// A complete full was written to the topology view (Q409, Q410): its delta from the copy held, or
    /// the mesh added by its first full. Apply its nodes to the digest view. `prior` is what the copy
    /// held of the source before it.
    pub(crate) fn written(&self, w: &Written, fabric: &FabricId, side: Side, prior: Prior) -> TopologyFrameTaken {
        let count = |status: rdm_core::model::NodeStatus| w.full.digests().iter().filter(|d| d.status == status).count();
        tracing::info_span!(
            "rdm.mesh.topology.update.via-full",
            node = %self.current_name(),
            channel = side.name(),
            mesh = %w.mesh,
            publisher = %w.publisher.as_ref().map(ToString::to_string).unwrap_or_default(),
            base_version = w.base_version,
            topology_version = w.topology_version,
            time_ms = w.time_ms,
            members = w.full.member_count(),
            added = w.delta.added.len(),
            changed = w.delta.changed.len(),
            removed = w.delta.removed.len(),
            stopped = count(rdm_core::model::NodeStatus::Stopped),
            deleting = count(rdm_core::model::NodeStatus::Deleting),
            deleted = count(rdm_core::model::NodeStatus::Deleted),
            first = w.first,
            resumed = w.resumed,
            refreshed = w.refreshed,
        )
        .in_scope(|| tracing::debug!("a complete full written to the topology view: a mesh's first full adds it, a later one writes only its delta from the copy held"));
        let target = prior.target(&w.publisher.as_ref().map(ToString::to_string).unwrap_or_default(), side, w.topology_version, Some(&w.full));
        let carried = self.apply_full(&w.mesh, &w.full, Some(&w.full), fabric, side, &target, w.time_ms);
        let source = (side == Side::Backbone && w.mesh != self.mesh).then(|| Box::new(w.clone()));
        // A full with no nodes lists no birth of its author: it carries no rafka-time sample.
        let publication = w.publisher.clone().filter(|_| side == Side::Backbone && w.full.member_count() > 0).map(|publisher| Publication {
            publisher_digest: w.full.digests().into_iter().find(|d| d.node.node_id == publisher.node_id && d.node.incarnation == publisher.incarnation),
            publisher,
            published_at_rafka_ms: w.time_ms,
        });
        TopologyFrameTaken { carried, source, publication }
    }

    /// Apply the nodes of an aggregate, a read or a delta to the view, each by its status (Q390): a node
    /// listed `deleted` departs for the retention, one listed `stopped` is stopped, one listed
    /// `deleting` is held under its removal; a differential's open operations and departures, while it
    /// carries them, the same. Departures first: a digest of a deleted birth in the same list is
    /// refused. A listed birth this view holds stopped ends its stop only when the source moved, in
    /// order, from a version listing it stopped to this one (`target`); any other entry of it is
    /// refused by name. An own-mesh birth's entry gives its status and nothing else (gossip.md §3.1:
    /// liveness, endpoints and the runtime fact come from its Digest), unless this node is the primary
    /// that authors it. A peer mesh's primary carries only the members it hears (R-RX4: only a mesh's
    /// own primary publishes it). `at_rafka_ms` is when the list was published: what an operation said
    /// by a status alone is dated.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_full(&self, mesh: &str, full: &Full, complete: Option<&Full>, fabric: &FabricId, side: Side, target: &Target, at_rafka_ms: u64) -> Vec<MeshDigest> {
        use rdm_core::model::NodeStatus as S;
        let complete_digests = complete.map(Full::digests).unwrap_or_default();
        // A deleted mesh's final aggregate: its last node-admin, the only node, deleting (gossip.md "Mesh
        // delete": "its final mesh topology, the mesh deleted and itself deleting"). A node it lists
        // `deleted` is a departure held for the deleted retention (Q390), not a node of the mesh.
        let nodes: Vec<&MeshDigest> = complete_digests.iter().filter(|d| d.status != S::Deleted).collect();
        let final_delete = (nodes.len() == 1 && nodes[0].status == S::Deleting && rdm_core::is_node_admin(&nodes[0].node.r#type))
            .then(|| DigestView::listed_op(nodes[0], "node.delete", at_rafka_ms));
        if let Some(op) = &final_delete {
            let deleted_mesh_id = complete.and_then(Full::mesh_id);
            if self.digest_view.holds_birth_of_mesh_under_another_id(mesh, &deleted_mesh_id) {
                return Vec::new();
            }
            if self.digest_view.is_deleted(op.node_id.as_str()) {
                for d in full.digests().into_iter().filter(|d| d.status == S::Deleted && &d.fabric_id == fabric) {
                    self.digest_view.depart_listed(&d, at_rafka_ms);
                }
                for d in full.deleted() {
                    self.digest_view.depart(d.clone());
                }
                self.digest_view.statuses.forget_mesh(mesh);
                return Vec::new();
            }
        }
        self.digest_view.hold_mesh_id(mesh, complete.and_then(Full::mesh_id).as_ref());
        let listed: Vec<MeshDigest> = full.digests().into_iter().filter(|d| &d.fabric_id == fabric).collect();
        for d in listed.iter().filter(|d| d.status == S::Deleted) {
            self.digest_view.depart_listed(d, at_rafka_ms);
        }
        for op in full.deleted() {
            self.digest_view.depart(op);
        }
        for d in listed.iter().filter(|d| d.status == S::Stopped) {
            let prior = self.digest_view.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
            if self.digest_view.stopped_listed(d, at_rafka_ms) {
                self.note_member_stopped(&DigestView::listed_op(d, "node.stop", at_rafka_ms), prior, side.name());
            }
        }
        for d in listed.iter().filter(|d| d.status == S::Deleting) {
            self.digest_view.deleting_listed(d, at_rafka_ms);
        }
        for op in full.in_flight() {
            if op.is_stop() {
                let prior = self.digest_view.get(op.node_id.as_str()).map(|(h, _)| h.status);
                if self.digest_view.stopped(op.clone()) {
                    self.note_member_stopped(&op, prior, side.name());
                }
            } else {
                self.digest_view.deleting(op);
            }
        }
        self.digest_view.note_stops_carried(target);
        let authors = self.primary.load(Ordering::Relaxed);
        let me_name = self.current_name();
        let mut carried = Vec::new();
        // A node listed stopped or deleted is held by its stop or its departure, never as a node heard.
        for d in listed.into_iter().filter(|d| !matches!(d.status, S::Stopped | S::Deleted)) {
            // The primary records the entry carries, this node's own among them (gossip.md "A primary
            // change on gossip": the mesh-primary's `TopologyFull` projection carries them): held by revision
            // whatever else of the entry is taken, and however old its copy of the digest is.
            self.digest_view.take_primaries(&d);
            // A read carries this node's own digest as the node read holds it: this node's own
            // view is the authority on itself.
            if matches!(side, Side::Read | Side::ReadPeer) && d.node.node_id.to_string() == me_name {
                continue;
            }
            let own = d.node.mesh == self.mesh;
            // The primary authors its own mesh's statuses: a projection of its own mesh, on its own
            // channel or on the backbone (a predecessor's last mesh topology after this node took the
            // role), moves none of them.
            if own && authors && matches!(side, Side::MeshChannel | Side::Backbone) {
                continue;
            }
            if !(own && authors) && self.digest_view.stopped_entry(&d, target) == Some(false) {
                continue;
            }
            let prior = self.digest_view.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
            let taken = match (side, own) {
                // An own-mesh node's status comes only from its own Digest (gossip.md §3.1 `Digest`): its
                // primary's projection gives nothing of a node this view hears, and holds one it does not.
                (Side::Backbone, true) | (Side::MeshChannel, true) => {
                    if self.digest_view.own_mesh_member(&d) {
                        self.digest_view.hold_projected_at(&d, std::time::Instant::now());
                    }
                    continue;
                }
                // An aggregate's entry carries no Digest sequence: the aggregate's version orders it
                // (Q389), and the birth keeps the sequence this view accepted of it, a read's or a join's
                // ("preserve accepted member sequence across reads and gossip").
                (Side::Backbone, false) => {
                    let mut d = d.clone();
                    if let Some((held, _)) = self.digest_view.get(d.node.node_id.as_str()).filter(|(held, _)| held.node.incarnation == d.node.incarnation) {
                        d.digest_seq = d.digest_seq.max(held.digest_seq);
                    }
                    self.digest_view.record_forwarded(d)
                }
                (Side::MeshChannel, false) => self.digest_view.record_topology(d.clone()),
                // A read holds what an entry answer holds, by the rule `learn` applies
                // (R-G2): a peer mesh's member is topology and refreshes no liveness; a node of this
                // node's own mesh is held as the answerer heard it, its status the one its own Digest
                // carried, ordered by that Digest's sequence (gossip.md §3.1, four orderings): a later
                // Digest already held refuses the primary's older copy, status included.
                (Side::Read, true) => self.digest_view.record_forwarded(d.clone()),
                (Side::Read, false) | (Side::ReadPeer, false) => self.digest_view.record_topology(d.clone()),
                (Side::ReadPeer, true) => false,
            };
            if taken {
                self.note(&d, side.via());
                self.note_member_state(&d, prior, side);
                carried.push(d);
            }
        }
        if let Some(op) = final_delete {
            let (mesh, build_id, at_rafka_ms) = (op.mesh.clone(), op.build_id.clone(), op.event_at_rafka_ms);
            if !self.digest_view.is_deleted(op.node_id.as_str()) {
                self.digest_view.depart(op);
            }
            self.digest_view.depart_mesh_births(&mesh, &build_id, at_rafka_ms);
        }
        carried
    }

    /// A peer mesh's member whose own state this node now holds changed (or was first held saying
    /// connections-deleting or Leaving): what this node routes to it follows that state alone (R4).
    pub(crate) fn note_member_state(&self, d: &MeshDigest, prior: Option<rdm_core::model::NodeStatus>, side: Side) {
        use rdm_core::model::NodeStatus::{ConnectionsDeleting, Leaving};
        if d.node.mesh == self.mesh || prior == Some(d.status) || (prior.is_none() && !matches!(d.status, ConnectionsDeleting | Leaving)) {
            return;
        }
        tracing::info_span!(
            "rdm.mesh.topology.update.via-member-state",
            node = %self.current_name(),
            member_path = %d.node.path, member_name = %d.node.name,
            member_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            status = ?d.status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            channel = side.name(),
            routable = self.digest_view.routable(d.node.node_id.as_str()),
        )
        .in_scope(|| tracing::info!("a peer mesh's member state held from what this node wrote to its topology view: routing follows it"));
    }

    /// A peer mesh's member this node now holds stopped, from its mesh primary's `NodeStopped` overlay
    /// (gossip.md, node-stopped lifecycle overlay) or its aggregate listing it stopped: Leaving and
    /// stopped, not routable. `channel` is where the stop arrived.
    pub(crate) fn note_member_stopped(&self, op: &rdm_core::LifecycleOp, prior: Option<rdm_core::model::NodeStatus>, channel: &str) {
        if op.mesh == self.mesh {
            return;
        }
        tracing::info_span!(
            "rdm.mesh.topology.update.via-member-state",
            node = %self.current_name(),
            member_path = %op.path, member_name = %op.name,
            member_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            status = ?rdm_core::model::NodeStatus::Leaving,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            channel,
            routable = self.digest_view.routable(op.node_id.as_str()),
            stopped = true,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            build_id = %op.build_id,
        )
        .in_scope(|| tracing::info!("a peer mesh's member is stopped by its mesh primary's NodeStopped: Leaving, not routable"));
    }

    pub(crate) fn note(&self, d: &MeshDigest, via: &'static str) {
        self.via.lock().unwrap().entry(d.node.mesh.clone()).or_insert(via);
    }

    /// Hold a primary record this node elected or read (an entry read), if it supersedes the one held.
    pub(crate) fn note_primary(&self, role: PrimaryRole, primary: &Primary, via: &'static str) -> bool {
        let RoleTaken::Held { previous } = self.digest_view.primaries.take(role.clone(), primary) else { return false };
        let previous = previous.as_ref().map(ToString::to_string).unwrap_or_default();
        tracing::info_span!("rdm.mesh.primary.update.via-announcement", node = %self.current_name(), role = role.name(), primary = %primary, via, outcome = "held", previous = %previous)
            .in_scope(|| tracing::info!("a primary record was held"));
        true
    }

}
