//! What a node's topology view takes from a frame: the receive rules that install a `Digest`,
//! a full topology, a delta and a status into the digest view (gossip.md §3.1, §3.3).


use crate::snapshot::{Chunk, Delta, Forwarder, Full, Gap, Install, Moved, PublisherId, SnapshotReceiver, SourceSnapshot, Taken};
use rdm_core::{FabricId, LifecycleOp, MeshDigest, Primary, PrimaryRole};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;


use crate::{frame::*, status::*, primary::*, channel::*, digest_view::*};

/// What a snapshot frame left behind.
#[derive(Default)]
pub(crate) struct SnapshotTaken {
    /// The digests applied to the view, for their addresses.
    pub(crate) carried: Vec<MeshDigest>,
    /// A complete backbone snapshot of a source Mesh: `(mesh, publisher, version, full)`, for the
    /// forwarding primary.
    pub(crate) source: Option<(String, PublisherId, u64, Full)>,
    /// A complete snapshot installed from the backbone: who published it, when, and the
    /// publisher's own digest as the snapshot lists it. Evidence for the clock's discipline.
    pub(crate) publication: Option<Publication>,
}

/// What a receiver held of one source before a projection frame of it applied.
pub(crate) struct Prior {
    pub(crate) held: Option<(PublisherId, u64)>,
    pub(crate) stops: BTreeSet<(String, String)>,
}

impl Prior {
    pub(crate) fn of(rx: &SnapshotReceiver, mesh: &str) -> Self {
        Self { held: rx.held_version(mesh), stops: Target::stops_of(rx.held_full(mesh)) }
    }

    /// The target of a frame of `publisher` at `version` arriving on `side`, `after` the projection the
    /// receiver holds once it applied.
    pub(crate) fn target(self, publisher: &PublisherId, side: Side, version: u64, after: Option<&Full>) -> Target {
        Target {
            publisher: publisher.to_string(),
            channel: side.name(),
            held: self.held.map(|(p, v)| (p.to_string(), v)),
            version,
            prior_stops: self.stops,
            post_stops: Target::stops_of(after),
        }
    }
}

/// A backbone snapshot as its publisher stamped it.
pub(crate) struct Publication {
    pub(crate) publisher: PublisherId,
    /// The publisher's rafka-time when it published (`Frame::TopologyFull::published_at_rafka_ms`).
    pub(crate) published_at_rafka_ms: u64,
    /// The publisher's own digest in the snapshot, matched by its exact birth.
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
    /// The cross-Mesh projection the backbone carries: each source complete, loads included.
    pub(crate) backbone_rx: Arc<Mutex<SnapshotReceiver>>,
    /// The cross-Mesh projection this Mesh's own channel carries: what the local primary forwards.
    pub(crate) mesh_rx: Arc<Mutex<SnapshotReceiver>>,
    /// The one version this node holds per source (R-RX3): consulted by the backbone's and the mesh
    /// channel's receive paths and by the topology-read install, and moved by what each installs.
    pub(crate) order: Arc<Mutex<crate::receive_order::ReceiveOrder>>,
    /// What this node, as its Mesh's primary, has published into its Mesh.
    pub(crate) forwarder: Arc<Mutex<Forwarder>>,
    /// Wakes the top-up when a source of the Mesh channel desynchronizes.
    pub(crate) desync: Arc<tokio::sync::Notify>,
    /// The name the mesh-primary's row last gave this birth (`UpdateName`, R12), shared with its
    /// the topology view; `None` while the birth holds its launch name (`node`).
    pub(crate) held_name: Arc<Mutex<Option<rdm_core::PathName>>>,
}

impl View {
    /// The sources this node serves on a topology read, one per mesh: what it forwards as its
    /// mesh's primary, else the newer of what its two receivers hold (R-RX3, gossip.md §3.1: one held
    /// version per source, whichever channel or read it arrives by). A node-admin hears a peer mesh on
    /// the backbone and, forwarded, on its mesh channel; a forwarded frame the backbone already moved
    /// past is ignored, so the mesh channel's copy can stay behind the version the node holds (rdm#154).
    /// `mesh`'s alone when one is named: no other mesh's source is copied.
    pub(crate) fn held_sources_of(&self, mesh: Option<&str>) -> Vec<SourceSnapshot> {
        if self.primary.load(Ordering::Relaxed) {
            return self.forwarder.lock().unwrap().snapshots_of(mesh);
        }
        let channel = self.mesh_rx.lock().unwrap().snapshots_of(mesh);
        let backbone = self.backbone_rx.lock().unwrap().snapshots_of(mesh);
        let order = self.order.lock().unwrap();
        let mut by_mesh: std::collections::BTreeMap<String, SourceSnapshot> = Default::default();
        for s in channel.into_iter().chain(backbone) {
            let held = order.installed_snapshot(&s.mesh).map(|(publisher, version, ..)| (publisher.clone(), version));
            let is_held = |c: &SourceSnapshot| held.as_ref().is_some_and(|(p, v)| &c.publisher == p && c.topology_version == *v);
            match by_mesh.entry(s.mesh.clone()) {
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(s);
                }
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    let kept = e.get();
                    // The one this node holds of the source; else, of one publisher, the higher version.
                    let newer = !is_held(kept) && (is_held(&s) || (s.publisher == kept.publisher && s.topology_version > kept.topology_version));
                    if newer {
                        e.insert(s);
                    }
                }
            }
        }
        by_mesh.into_values().collect()
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
        *view.digest_view.on_mesh_deleted.0.lock().unwrap() = Some(Arc::new(move |m: &str| {
            if let Ok(mut fw) = forwarder.try_lock() {
                fw.remove(m);
            }
        }));
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

    /// Hold what `f` carries; the digests it carries (for their addresses). A snapshot frame is
    /// not taken here: see [`View::take_snapshot`].
    pub(crate) fn take(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        self.take_with(f, fabric, via).0
    }

    /// [`View::take`], and whether a digest the frame carried was newer than the one held: the
    /// view's admission of a heartbeat. A heartbeat taken is answered as the digest view now holds it, with
    /// the status held for its birth (a heartbeat carries none).
    pub(crate) fn take_with(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> (Vec<MeshDigest>, bool) {
        if let Frame::Digest { digest } = f.data() {
            if &digest.fabric_id == fabric {
                let channel = if via == "mesh-channel" { format!("mesh:{}", self.mesh) } else { via.to_string() };
                let fresh = self.digest_view.record_with(digest.clone(), Instant::now(), &self.current_name(), &channel);
                if !fresh {
                    // A node held stopped heard again: its mesh-primary's view is pulled (a sync issue).
                    // Each such Digest pulls again until a read moves past the stop.
                    if self.digest_view.stopped_heard_again(digest) {
                        if self.mesh_rx.lock().unwrap().mark_desynced(&self.mesh, crate::snapshot::Gap::StoppedBirthHeard) {
                            tracing::info_span!("rdm.mesh.topology.update.via-stopped-birth-heard", node = %self.current_name(), node_id = %digest.node.node_id, incarnation_id = %digest.node.incarnation, digest_seq = digest.digest_seq)
                                .in_scope(|| tracing::info!("a node held stopped is heard again: its mesh-primary's view is pulled"));
                        }
                        self.desync.notify_one();
                    }
                    return (Vec::new(), false);
                }
                self.note(digest, via);
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
            (StatusSubject::Node(op), StatusOrder::TopologyVersion { publisher, topology_version }) => {
                let kind = if status.status() == "deleting" { StatusEvent::Deleting } else { StatusEvent::Stopped };
                let op = &traced_by_envelope(f, op);
                if !self.refuses_status_event(kind, op, publisher, *topology_version, via) {
                    self.apply_status_event(kind, op, Some((publisher.to_string(), *topology_version)), via);
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
    /// taken for what it carries (R-RX1): an event published before the newest of its publisher and
    /// mesh was already ignored by the receive path (R-RX3, [`crate::receive_order`]). A `NodeStopped`
    /// older than a held projection of the same publisher that lists the exact birth without the stop
    /// is refused (node-status-heartbeat-connections.md §C "late stopped after starting"): the
    /// projection is the publisher's later word on the birth, and the version orders them only within
    /// that publisher's counter.
    pub(crate) fn refuses_status_event(&self, kind: StatusEvent, op: &LifecycleOp, publisher: &PublisherId, topology_version: u64, via: &'static str) -> bool {
        let mesh = op.mesh.as_str();
        if kind != StatusEvent::Stopped {
            return false;
        }
        let key = (op.node_id.to_string(), op.incarnation.to_string());
        for (channel, rx) in [("mesh-channel", &self.mesh_rx), ("backbone", &self.backbone_rx)] {
            let rx = rx.lock().unwrap();
            let Some((held_publisher, held_version)) = rx.held_version(mesh) else { continue };
            if held_publisher != *publisher || held_version < topology_version {
                continue;
            }
            let Some(full) = rx.held_full(mesh) else { continue };
            let listed = full.digests().iter().any(|d| d.node.node_id == op.node_id && d.node.incarnation == op.incarnation);
            if !listed || Target::stops_of(Some(full)).contains(&key) {
                continue;
            }
            drop(rx);
            crate::receive_order::reject_lifecycle_version(kind.name(), &format!("{publisher} {mesh}"), held_version, topology_version);
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
                held_channel = channel,
                via,
            )
            .in_scope(|| tracing::info!("a NodeStopped older than the held projection of its publisher, which lists the birth without the stop: refused, the birth started after it"));
            return true;
        }
        false
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

    /// A `TopologyFull` chunk or a `TopologyDelta`, arrived on `side`. A snapshot is applied to the view
    /// only once complete; a delta only at exactly its base. A frame that does not belong on its
    /// channel is refused by name. Whoever published it, a frame is taken for what it carries: one
    /// published before the newest the channel holds of its mesh was already ignored by the receive
    /// path (R-RX3, [`crate::receive_order`]), and only the mesh-primary publishes a mesh (R-RX2).
    /// A projection of this node's own mesh on its own mesh channel is refused by name: its mesh-primary
    /// puts nothing of its own mesh there, whose nodes publish themselves, and its view reaches this
    /// node only by a read (gossip.md §2, §3.3, Q387). What arrives is a frame from before that rule.
    fn own_mesh_on_its_channel(&self, mesh: &str, side: Side, publisher: &PublisherId, frame: &'static str) -> bool {
        if side != Side::MeshChannel || mesh != self.mesh {
            return false;
        }
        tracing::info_span!("rdm.mesh.topology.reject.via-own-mesh-on-its-channel", node = %self.current_name(), mesh = %mesh, publisher = %publisher, frame)
            .in_scope(|| tracing::info!("a projection of this node's own mesh on its mesh channel: refused, its nodes publish themselves and its view comes by a read"));
        true
    }

    pub(crate) fn take_snapshot(&self, f: &Frame, fabric: &FabricId, side: Side, order: &mut crate::receive_order::ReceiveOrder) -> SnapshotTaken {
        match f.data() {
            Frame::TopologyFull { mesh, publisher, forwarded_by, topology_version, published_at_rafka_ms, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted } => {
                if forwarded_by.is_some() == (side == Side::Backbone) {
                    tracing::info_span!(
                        "rdm.mesh.topology.reject.via-members-on-wrong-channel",
                        node = %self.current_name(),
                        channel = side.name(),
                        mesh = %mesh,
                        publisher = %publisher,
                        forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    )
                    .in_scope(|| tracing::warn!("a TopologyFull frame on a channel it is not published on: refused"));
                    return SnapshotTaken::default();
                }
                if self.own_mesh_on_its_channel(mesh, side, publisher, "mesh-topology") {
                    return SnapshotTaken::default();
                }
                if self.outside_build(mesh, side) {
                    return SnapshotTaken::default();
                }
                let chunk = Chunk {
                    mesh: mesh.clone(),
                    publisher: publisher.clone(),
                    forwarded_by: forwarded_by.clone(),
                    topology_version: *topology_version,
                    snapshot_id: *snapshot_id,
                    chunk_index: *chunk_index,
                    chunk_count: *chunk_count,
                    digests: digests.clone(),
                    in_flight: in_flight.clone(),
                    deleted: deleted.clone(),
                };
                // The receiver's lock is held until the view holds what the version names: a reader of
                // the held version never sees a version ahead of the view it describes.
                let mut rx = self.receiver(side).lock().unwrap();
                let prior = Prior::of(&rx, mesh);
                let owner = f.context().and_then(|c| c.publication.as_ref()).map(|(birth, _)| birth.clone());
                let taken = rx.take_authored_chunk(chunk, owner.clone(), order.installed_for_owner(mesh, owner.as_ref(), *snapshot_id));
                match taken {
                    Taken::Waiting { .. } => SnapshotTaken::default(),
                    Taken::Refused(crate::snapshot::Refusal::Duplicate | crate::snapshot::Refusal::Inconsistent | crate::snapshot::Refusal::OlderThanHeld { .. } | crate::snapshot::Refusal::OlderThanPending { .. }) => SnapshotTaken::default(),
                    Taken::Refused(r) => {
                        drop(rx);
                        tracing::info_span!(
                            "rdm.mesh.topology.reject.via-snapshot",
                            node = %self.current_name(),
                            channel = side.name(),
                            mesh = %mesh,
                            publisher = %publisher,
                            topology_version = *topology_version,
                            snapshot_id = *snapshot_id,
                            reason = ?r,
                        )
                        .in_scope(|| tracing::info!("a snapshot chunk refused: the held projection stands"));
                        SnapshotTaken::default()
                    }
                    Taken::Installed(i) => {
                        if let Some(owner) = &owner { order.publication_installed(mesh, publisher, i.topology_version, owner, i.snapshot_id); }
                        let installed = self.installed(&i, fabric, side, *published_at_rafka_ms, prior);
                        drop(rx);
                        order.installed_projection(&i.mesh, &i.publisher, i.topology_version, Some(*published_at_rafka_ms), &i.full, Some(i.snapshot_id));
                        installed
                    }
                }
            }
            Frame::TopologyDelta { mesh, source_publisher, base_version, topology_version, published_at_rafka_ms, changed, removed, in_flight, deleted } => {
                if side == Side::Backbone {
                    tracing::info_span!("rdm.mesh.topology.reject.via-members-on-wrong-channel", node = %self.current_name(), channel = side.name(), mesh = %mesh, publisher = %source_publisher, forwarded_by = "")
                        .in_scope(|| tracing::warn!("a TopologyDelta on the backbone, where only complete snapshots travel: refused"));
                    return SnapshotTaken::default();
                }
                if self.own_mesh_on_its_channel(mesh, side, source_publisher, "mesh-topology-delta") {
                    return SnapshotTaken::default();
                }
                if self.outside_build(mesh, side) {
                    return SnapshotTaken::default();
                }
                let delta = Delta { changed: changed.clone(), removed: removed.clone(), in_flight: in_flight.clone(), deleted: deleted.clone() };
                let mut rx = self.mesh_rx.lock().unwrap();
                let prior = Prior::of(&rx, mesh);
                let moved = rx.take_delta_against(mesh, source_publisher, *base_version, *topology_version, &delta, order.installed_snapshot(mesh));
                match moved {
                    Moved::Applied { delta, .. } => {
                        tracing::info_span!(
                            "rdm.mesh.topology.update.via-delta",
                            node = %self.current_name(),
                            mesh = %mesh,
                            source_publisher = %source_publisher,
                            base_version = *base_version,
                            topology_version = *topology_version,
                            changed = delta.changed.len(),
                            removed = delta.removed.len(),
                            in_flight = delta.in_flight.len(),
                            deleted = delta.deleted.len(),
                        )
                        .in_scope(|| tracing::info!("a delta applied at exactly its base version"));
                        let full = Full::new(delta.changed.clone(), delta.in_flight.clone(), delta.deleted.clone());
                        let held_full = rx.held_full(mesh);
                        let target = prior.target(source_publisher, side, *topology_version, held_full);
                        let carried = self.apply_full(mesh, &full, held_full, fabric, side, &target);
                        let installed_full = held_full.expect("applied delta has a full").clone();
                        drop(rx);
                        order.installed_projection(mesh, source_publisher, *topology_version, Some(*published_at_rafka_ms), &installed_full, None);
                        SnapshotTaken { carried, ..SnapshotTaken::default() }
                    }
                    Moved::Duplicate { .. } | Moved::Inconsistent | Moved::Stale => SnapshotTaken::default(),
                    Moved::Desynced { gap, .. } => {
                        drop(rx);
                        let (held_version, held_publisher) = match &gap {
                            Gap::Version { held, .. } => (*held as i64, String::new()),
                            Gap::OtherRevision { held } => (-1, held.to_string()),
                            _ => (-1, String::new()),
                        };
                        tracing::info_span!(
                            "rdm.mesh.topology.update.via-gap",
                            frame = "mesh-topology-delta", repair_requested = true,
                            node = %self.current_name(),
                            mesh = %mesh,
                            source_publisher = %source_publisher,
                            gap = %gap,
                            held_version,
                            held_publisher = %held_publisher,
                            base_version = *base_version,
                            topology_version = *topology_version,
                        )
                        .in_scope(|| tracing::info!("the delta does not follow what is held: this source is desynchronized and tops up from its own primary"));
                        self.desync.notify_one();
                        SnapshotTaken::default()
                    }
                }
            }
            _ => SnapshotTaken::default(),
        }
    }

    /// Whether a snapshot or delta of the peer mesh `mesh` installs nothing: the accepted Build is
    /// the authority on which meshes the fabric has, and a mesh it does not name, of which this
    /// node holds no source projection, is not a mesh this node takes members of from an old
    /// mesh topology. A source this node holds is a mesh it still tracks (a Leaving mesh) and is
    /// updated; a node that holds no Build takes everything.
    pub(crate) fn outside_build(&self, mesh: &str, side: Side) -> bool {
        if mesh == self.mesh || self.digest_view.accepted_has_mesh(mesh) != Some(false) {
            return false;
        }
        if self.receiver(side).lock().unwrap().held_version(mesh).is_some() {
            return false;
        }
        tracing::info_span!("rdm.mesh.topology.reject.via-mesh-not-in-build", node = %self.current_name(), channel = side.name(), mesh)
            .in_scope(|| tracing::info!("a topology of a mesh the accepted Build does not name, of which this node holds no source: installs nothing"));
        true
    }

    /// One chunk of a topology read, heard on `side` (`Read` from this node's own mesh, `ReadPeer`
    /// from another's): consulted against the one held version of its source first (R-RX3), so a
    /// chunk below the source version this node installed is ignored by name before the receiver
    /// reads it; a snapshot that completes installs through the Mesh channel's receiver and moves the
    /// held source version (gossip.md §3.1).
    pub(crate) fn take_read_chunk(&self, chunk: Chunk, side: Side, fabric: &FabricId) -> Taken {
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
        if let Err(e) = order.admit_read(&chunk.mesh, &chunk.publisher, chunk.topology_version) {
            tracing::info_span!(
                "rdm.mesh.topology.reject.via-stale",
                node = %self.current_name(),
                channel = side.name(),
                frame = e.frame,
                subject = %e.source,
                held_key = %e.held,
                frame_key = %e.offered,
                publisher = %chunk.publisher,
            )
            .in_scope(|| tracing::info!("a topology read of a version below the one this node installed of its source: ignored"));
            let held_version = order.floor(&chunk.mesh, &chunk.publisher).unwrap_or_default();
            drop(order);
            record_own(own_digests);
            return Taken::Refused(crate::snapshot::Refusal::OlderThanHeld { held_version, offered_version: chunk.topology_version });
        }
        let mut rx = self.mesh_rx.lock().unwrap();
        let prior = Prior::of(&rx, &chunk.mesh);
        let installed = order.installed_snapshot(&chunk.mesh);
        let taken = rx.take_chunk_against(chunk, true, installed);
        if let Taken::Installed(i) = &taken {
            self.installed(i, fabric, side, 0, prior);
            drop(rx);
            order.installed_projection(&i.mesh, &i.publisher, i.topology_version, None, &i.full, None);
        } else {
            drop(rx);
            drop(order);
            if matches!(&taken, Taken::Refused(_)) {
                record_own(own_digests);
            }
        }
        taken
    }

    pub(crate) fn receiver(&self, side: Side) -> &Arc<Mutex<SnapshotReceiver>> {
        match side {
            Side::Backbone => &self.backbone_rx,
            Side::MeshChannel | Side::Read | Side::ReadPeer => &self.mesh_rx,
        }
    }

    /// A complete snapshot is held: apply it to the view. `prior` is what the receiver held of the
    /// source before it.
    pub(crate) fn installed(&self, i: &Install, fabric: &FabricId, side: Side, published_at_rafka_ms: u64, prior: Prior) -> SnapshotTaken {
        tracing::info_span!(
            "rdm.mesh.topology.update.via-snapshot-installed",
            node = %self.current_name(),
            channel = side.name(),
            mesh = %i.mesh,
            publisher = %i.publisher,
            topology_version = i.topology_version,
            snapshot_id = i.snapshot_id,
            members = i.full.member_count(),
            in_flight = i.full.in_flight().len(),
            deleted = i.full.deleted().len(),
            new_revision = i.new_revision,
            resumed = i.resumed,
            refreshed = i.refreshed,
        )
        .in_scope(|| tracing::debug!("a complete snapshot installed"));
        let target = prior.target(&i.publisher, side, i.topology_version, Some(&i.full));
        let carried = self.apply_full(&i.mesh, &i.full, Some(&i.full), fabric, side, &target);
        let source = (side == Side::Backbone && i.mesh != self.mesh).then(|| (i.mesh.clone(), i.publisher.clone(), i.topology_version, i.full.clone()));
        // A mesh topology with no digests (a deleted mesh's, gossip.md "Mesh delete": "`digests`
        // empty") lists no birth of its publisher: it carries no rafka-time sample.
        let publication = (side == Side::Backbone && i.full.member_count() > 0).then(|| Publication {
            publisher: i.publisher.clone(),
            published_at_rafka_ms,
            publisher_digest: i.full.digests().into_iter().find(|d| d.node.node_id == i.publisher.node_id && d.node.incarnation == i.publisher.incarnation),
        });
        SnapshotTaken { carried, source, publication }
    }

    /// Apply the members, overlays and departures of a snapshot or a delta to the view. Departures
    /// first: a digest of a deleted birth in the same snapshot is refused. A listed birth this view
    /// holds stopped ends its stop only when the source moved, in order, from a version carrying the
    /// stop to this one (`target`); any other entry of it is refused by name. An own-mesh birth's
    /// entry gives its status and nothing else (gossip.md §3.1: liveness, endpoints and the runtime
    /// fact come from its Digest), unless this node is the primary that authors it. A peer mesh's
    /// primary carries only the members it hears (R-RX4: only a mesh's own primary publishes it).
    pub(crate) fn apply_full(&self, mesh: &str, full: &Full, complete: Option<&Full>, fabric: &FabricId, side: Side, target: &Target) -> Vec<MeshDigest> {
        let complete_digests = complete.map(Full::digests).unwrap_or_default();
        let final_delete = if complete_digests.len() == 1
            && complete_digests[0].status == rdm_core::model::NodeStatus::Leaving
            && rdm_core::is_node_admin(&complete_digests[0].node.r#type)
        {
            let last_id = &complete_digests[0].node.node_id;
            complete
                .into_iter()
                .flat_map(Full::in_flight)
                .find(|op| !op.is_stop() && &op.node_id == last_id)
        } else {
            None
        };
        if let Some(op) = &final_delete {
            let deleted_mesh_id = complete_digests[0].mesh_id.clone();
            if deleted_mesh_id.is_some()
                && self.digest_view.holds_birth_of_mesh_under_another_id(&mesh, &deleted_mesh_id)
            {
                return Vec::new();
            }
            if self.digest_view.is_deleted(op.node_id.as_str()) {
                for d in full.deleted() {
                    self.digest_view.depart(d.clone());
                }
                self.digest_view.statuses.forget_mesh(mesh);
                return Vec::new();
            }
        }
        for op in full.deleted() {
            self.digest_view.depart(op);
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
        for d in full.digests().into_iter().filter(|d| &d.fabric_id == fabric) {
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
                (Side::Backbone, false) => self.digest_view.record_forwarded(d.clone()),
                (Side::MeshChannel, false) => self.digest_view.record_topology(d.clone()),
                // A read installs what an entry answer installed, by the rule `learn` applies
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
            self.digest_view.depart(op);
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
        .in_scope(|| tracing::info!("a peer mesh's member state held from what this node installed: routing follows it"));
    }

    /// A peer mesh's member this node now holds stopped, from its mesh primary's `NodeStopped` overlay
    /// (gossip.md, node-stopped lifecycle overlay): Leaving and stopped, not routable. `channel` is
    /// where the overlay arrived: the frame itself, or the mesh topology's `in_flight`.
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
