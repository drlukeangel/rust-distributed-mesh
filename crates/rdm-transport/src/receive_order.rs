//! Receive classification for gossip and topology-read installs. Decisions use the frame's
//! declared subject, identity and ordering fields, never the transport sender.
//!
//! MeshStatus and FabricStatus compare the envelope's composite written-primary revision and
//! change-time sequence through [`admit_status_context`] under the status view's installation lock. A delayed predecessor loses
//! by revision even with a later timestamp (R10). Within one held author/revision, [`admit_status`]
//! preserves change-time ordering and the existing named duplicate/inconsistency outcomes.
//! Forwarding metadata never changes status content; retained context survives entry installation.
//!
//! Digest uses [`admit_digest`] against the actual installed heartbeat under the digest view's
//! lock, including a read-installed floor. Lower is `via-stale`; equal wire content is
//! `via-duplicate`; equal sequence with different wire content is `via-inconsistent`.
//! The primary's separately held membership status is not heartbeat content. A refused or
//! repeated digest supplies no address and no clock observation to its caller.
//!
//! [`ReceiveOrder`] holds the remaining keyed families' ordering state, shared by all channels
//! and topology-read installs:
//! - MeshTopology and MeshTopologyDelta: the actual installed publisher/version floor orders that source,
//!   regardless of publication timestamp; a node's own publications are its floor for its own source
//!   under its exact birth, a topology read included ([`ReceiveOrder::published`]). The existing cross-publisher publication check remains.
//!   SnapshotReceiver compares complete normalized projection content, chunk identity and delta
//!   content under its installation lock: duplicates install nothing; conflicts are inconsistent.
//!   Publication identity includes its owner birth and counter; equal-key unfinished chunks remain admissible. An ahead-base delta requests repair via-gap;
//!   the exact repeated publication never completes repair. A new publication at the same material
//!   version carries fresh hearing evidence. A directed topology read can resume the held version.
//! - NodeDeleting and NodeStopped: operation identity is recorded before the
//!   existing topology-version check per publisher and subject mesh. A carried revision refused
//!   with `via-stale` still leaves that identity seen; a repeat emits `via-duplicate` and runs no
//!   handler. The carried revision is still judged independently, even for a repeated operation:
//!   an admissible higher revision advances its publisher floor without replaying the fact.
//!
//! Distinct equal-key chunks of an unfinished publication remain admissible; an exact repeat is
//! ignored without discarding its other chunks. Publication state advances only when a complete
//! snapshot installs or a delta applies. [`ReceiveOrder::installed_projection`] retains the source
//! image and version installed by any channel or topology read; no refused frame moves that image.
//!
//! Lifecycle overlay installation uses [`admit_overlay`] under the actual overlay lock for
//! local execution, received frames and snapshots. Retained completion is keyed by the complete
//! operation identity and exact birth; it cannot reopen. A held repeat never rewrites the authored
//! event evidence. NodeDeleting remains an overlay read only by application routing.
//!
//! NodeDeleted uses [`node_deleted_receipt`] against the retained deletion under its installation
//! lock: an exact operation/birth repeat emits `via-duplicate`, another deletion of the retained
//! NodeId is refused by the existing deleted-identity rule, and an unheld terminal fact applies
//! regardless of unrelated traffic. Event timestamps never order or expire that fact.
//!
//! The envelope refuses reserved types before domain decoding. [`refuse_reserved`] also protects
//! in-process domain calls for NodeRestarting (position 7, Q183), Concern (position 10, Q23), NodeConnectionsDeleting (11),
//! NodeLeaving (12), NodeLeft (13), NodeConnectionsDeleted (14), and NewFabricPrimary (15).
//! Receipt emits `via-reserved` before any identity view, apply span or carrier runs. No reserved
//! body installs a projection, records a fact, registers a contact or commands investigation.

use crate::topology::Frame;
use crate::snapshot::PublisherId;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Reserved page positions have one receive outcome. Inspect only the kind, before any view,
/// identity receipt, ordinary apply span or completion carrier can run.
pub(crate) fn refuse_reserved(f: &Frame, node: &str, via: &str) -> bool {
    let (frame, position) = match f.data() {
        Frame::NodeRestarting { .. } => ("node-restarting", 7),
        Frame::Concern { .. } => ("concern", 10),
        Frame::NodeConnectionsDeleting { .. } => ("node-connections-deleting", 11),
        Frame::NodeLeaving { .. } => ("node-leaving", 12),
        Frame::NodeLeft { .. } => ("node-left", 13),
        Frame::NodeConnectionsDeleted { .. } => ("node-connections-deleted", 14),
        Frame::NewFabricPrimary { .. } => ("new-fabric-primary", 15),
        _ => return false,
    };
    tracing::info_span!("rdm.mesh.topology.reject.via-reserved", node, frame, position, via)
        .in_scope(|| tracing::info!("the reserved position installs nothing and invokes no carrier"));
    true
}

/// Compare composite status sequences under the actual status installation lock. `None`
/// delegates same-author/revision content to the established timestamp classifier.
pub(crate) fn admit_status_context(frame: &str, subject: &str, held: Option<&crate::envelope::Context>, offered: Option<&crate::envelope::Context>, held_status: &str, status: &str) -> Option<bool> {
    let (held, offered) = (held?, offered?);
    let (h, o) = (held.sequence.as_ref()?, offered.sequence.as_ref()?);
    if held.source == offered.source && held.primary_revision() == offered.primary_revision() { return None; }
    if o < h {
        tracing::info_span!("rdm.mesh.topology.reject.via-stale", frame, subject, held_key = %h, frame_key = %o)
            .in_scope(|| tracing::info!("status sequence is below the held committed revision and change time"));
        return Some(false);
    }
    if o > h { return Some(true); }
    if held_status == status {
        tracing::info_span!("rdm.mesh.topology.update.via-duplicate", frame, identity = %offered.id, outcome = "ignored")
            .in_scope(|| tracing::info!("identical status sequence and content"));
    } else {
        tracing::info_span!("rdm.mesh.topology.reject.via-inconsistent", frame, subject, revision = %o)
            .in_scope(|| tracing::info!("equal status sequence with conflicting content"));
    }
    Some(false)
}

/// Admit a replacement status against the projection held under the status view's lock. The
/// declared subject, publisher and change instant identify it; forwarding metadata is not content.
/// This also serves statuses installed by a read, so classification and installation share the
/// same held fact instead of maintaining a separate receive history.
pub(crate) fn admit_status(frame: &'static str, subject: &str, held: Option<(&str, u64, &str)>, status: &str, publisher: &str, revision: u64) -> bool {
    let Some((held_publisher, held_revision, held_status)) = held else { return true };
    if revision < held_revision {
        tracing::info_span!(
            "rdm.mesh.topology.reject.via-stale",
            frame,
            subject = %format!("{subject} {publisher}"),
            held_key = %format!("changed_at_rafka_ms={}", held_revision),
            frame_key = %format!("changed_at_rafka_ms={revision}"),
        ).in_scope(|| tracing::info!("a status older than the held revision is refused"));
        return false;
    }
    if publisher == held_publisher && revision == held_revision {
        if status != held_status {
            tracing::info_span!(
                "rdm.mesh.topology.reject.via-inconsistent",
                frame,
                subject = %format!("{subject} {publisher}"),
                revision,
                held_status = %held_status,
                frame_status = status,
            ).in_scope(|| tracing::info!("a status at the held revision has different content: refused"));
        } else {
            tracing::info_span!(
                "rdm.mesh.topology.update.via-duplicate",
                frame,
                identity = %format!("{subject} {publisher} changed_at_rafka_ms={revision}"),
                outcome = "ignored",
            ).in_scope(|| tracing::info!("an identical status fact is ignored"));
        }
        return false;
    }
    true
}

/// Classify a heartbeat against the actual installed heartbeat while its installation lock is
/// held. Sequence orders only the same exact birth. Compare the wire shape: the primary's held
/// membership status is not carried by Digest and cannot make an identical heartbeat inconsistent.
pub(crate) fn admit_digest(held: &rdm_core::MeshDigest, offered: &rdm_core::MeshDigest, node: &str, channel: &str) -> bool {
    if held.node.node_id != offered.node.node_id || held.node.incarnation != offered.node.incarnation {
        return true;
    }
    let subject = format!("{}/{}", offered.node.node_id, offered.node.incarnation);
    if offered.digest_seq < held.digest_seq {
        tracing::info_span!(
            "rdm.mesh.topology.reject.via-stale",
            node, channel, frame = "digest", subject = %subject,
            held_key = %format!("digest_seq={}", held.digest_seq),
            frame_key = %format!("digest_seq={}", offered.digest_seq),
        ).in_scope(|| tracing::info!("the digest sequence is below the installed exact birth's sequence"));
        return false;
    }
    if offered.digest_seq == held.digest_seq {
        if rdm_core::wire::WireDigest::from(held) == rdm_core::wire::WireDigest::from(offered) {
            tracing::info_span!(
                "rdm.mesh.topology.update.via-duplicate",
                node, channel, frame = "digest", identity = %format!("{subject} digest_seq={}", offered.digest_seq),
                outcome = "ignored",
            ).in_scope(|| tracing::info!("the identical heartbeat is ignored, with no address registration"));
        } else {
            tracing::info_span!(
                "rdm.mesh.topology.reject.via-inconsistent",
                node, channel, frame = "digest", subject = %subject, revision = offered.digest_seq,
            ).in_scope(|| tracing::info!("equal digest sequence with different heartbeat content: nothing installed"));
        }
        return false;
    }
    true
}

/// Name a publication's classification at the installation door. The caller holds the actual
/// source projection lock; forwarding metadata never supplies its identity or ordering.
pub(crate) fn publication_duplicate(frame: &'static str, identity: &str) {
    tracing::info_span!("rdm.mesh.topology.update.via-duplicate", frame, identity, outcome = "ignored")
        .in_scope(|| tracing::info!("the publication identity and content are already held"));
}

pub(crate) fn publication_inconsistent(frame: &'static str, mesh: &str, publisher: &PublisherId, revision: u64) {
    tracing::info_span!("rdm.mesh.topology.reject.via-inconsistent", frame, subject = %format!("{mesh} {publisher}"), revision)
        .in_scope(|| tracing::info!("equal publication revision or chunk identity with different content: refused"));
}

pub(crate) fn publication_stale(frame: &'static str, mesh: &str, publisher: &PublisherId, held: u64, offered: u64) {
    tracing::info_span!("rdm.mesh.topology.reject.via-stale", frame, subject = %format!("{mesh} {publisher}"), held_key = %format!("topology_version={held}"), frame_key = %format!("topology_version={offered}"))
        .in_scope(|| tracing::info!("the publication is below the actual installed source version"));
}

/// The disposition of a NodeDeleted fact against the retained terminal record.
pub(crate) enum NodeDeletedReceipt {
    /// No terminal record is held for the NodeId: apply the fact.
    Apply,
    /// The operation and exact birth were already applied: change nothing.
    Duplicate,
    /// Another terminal record already deletes this NodeId: retain it and refuse the offered fact.
    AlreadyDeleted,
}

/// Classify a terminal operation fact using only its operation identity and exact subject birth.
/// The caller holds the retained-deletion lock through classification and installation. There is
/// no separate receipt history, ordering counter or timestamp floor for an operation fact.
pub(crate) fn node_deleted_receipt(held: Option<&rdm_core::LifecycleOp>, offered: &rdm_core::LifecycleOp) -> NodeDeletedReceipt {
    let Some(held) = held else { return NodeDeletedReceipt::Apply };
    if held.key() == offered.key() && held.node_id == offered.node_id && held.incarnation == offered.incarnation {
        tracing::info_span!(
            "rdm.mesh.topology.update.via-duplicate",
            frame = "node-deleted",
            identity = %format!("node-deleted {} {} {} {}/{}", offered.build_id, offered.attempt, offered.operation, offered.node_id, offered.incarnation),
            build_id = %offered.build_id,
            attempt = offered.attempt,
            operation = %offered.operation,
            node_id = %offered.node_id,
            incarnation_id = %offered.incarnation,
            outcome = "ignored",
        ).in_scope(|| tracing::info!("the terminal operation fact is already held: ignored"));
        NodeDeletedReceipt::Duplicate
    } else {
        NodeDeletedReceipt::AlreadyDeleted
    }
}

/// The operation fact's identity, independent of its carried publisher revision and event time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LifecycleIdentity {
    build_id: String,
    attempt: u32,
    operation: String,
    node_id: rdm_core::NodeId,
    incarnation: rdm_core::IncarnationId,
}

impl LifecycleIdentity {
    pub(crate) fn of(op: &rdm_core::LifecycleOp) -> Self {
        Self { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone(), node_id: op.node_id.clone(), incarnation: op.incarnation.clone() }
    }
}

fn duplicate_lifecycle(frame: &str, op: &rdm_core::LifecycleOp) {
    tracing::info_span!(
        "rdm.mesh.topology.update.via-duplicate",
        frame,
        identity = %format!("{} {} {} {}/{}", op.build_id, op.attempt, op.operation, op.node_id, op.incarnation),
        outcome = "ignored",
    ).in_scope(|| tracing::info!("the lifecycle operation identity is already held or seen: ignored"));
}

/// Classify an overlay against the actual held operation and retained exact-birth completion.
/// The caller holds the overlay installation lock through this decision and its insertion. Local
/// execution and snapshot application use this same classifier as the frame handler.
pub(crate) fn admit_overlay(frame: &str, op: &rdm_core::LifecycleOp, held: Option<&rdm_core::LifecycleOp>, ended: bool) -> bool {
    if ended {
        tracing::info_span!(
            "rdm.mesh.topology.update.via-lifecycle-reconciled",
            frame, build_id = %op.build_id, attempt = op.attempt, operation = %op.operation,
            node_id = %op.node_id, incarnation_id = %op.incarnation,
            outcome = "ignored", evidence = "operation-ended",
        ).in_scope(|| tracing::info!("held completion or later lifecycle evidence already ended this exact operation: nothing reopens"));
        return false;
    }
    if held.is_some_and(|h| LifecycleIdentity::of(h) == LifecycleIdentity::of(op)) {
        duplicate_lifecycle(frame, op);
        return false;
    }
    true
}

/// Name the carried projection refusal separately from recording the operation fact as seen.
pub(crate) fn reject_lifecycle_version(frame: &str, subject: &str, held: u64, offered: u64) {
    tracing::info_span!(
        "rdm.mesh.topology.reject.via-stale",
        frame, subject, held_key = %format!("topology_version={held}"), frame_key = %format!("topology_version={offered}"),
    ).in_scope(|| tracing::info!("the lifecycle frame's carried projection revision is below the held one: refused"));
}

/// Exact applied counters as inclusive runs: quiet anti-entropy does not allocate a scoped
/// identity record per publication, and an unheard counter in a gap remains admissible.
#[derive(Debug, Default)]
struct PublicationCounters(BTreeMap<u64, u64>);
impl PublicationCounters {
    fn contains(&self, counter: u64) -> bool { self.0.range(..=counter).next_back().is_some_and(|(_, end)| *end >= counter) }
    fn insert(&mut self, counter: u64) {
        let previous = self.0.range(..=counter).next_back().map(|(&start, &end)| (start, end));
        if previous.is_some_and(|(_, end)| end >= counter) { return; }
        let (mut start, mut end) = (counter, counter);
        if let Some((a, _)) = previous.filter(|(_, b)| b.checked_add(1) == Some(counter)) { self.0.remove(&a); start = a; }
        let next = self.0.range(counter..).next().map(|(&a, &b)| (a, b));
        if let Some((a, b)) = next.filter(|(a, _)| counter.checked_add(1) == Some(*a)) { self.0.remove(&a); end = b; }
        self.0.insert(start, end);
    }
}

/// The source a frame's order key orders within.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Source {
    /// One mesh's publications.
    Mesh(String),
    /// One publisher's lifecycle facts of one mesh.
    Lifecycle { publisher: PublisherId, mesh: String },

}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Mesh(mesh) => f.write_str(mesh),
            Source::Lifecycle { publisher, mesh } => write!(f, "{publisher} {mesh}"),
        }
    }
}

/// A frame's order key. One source only ever holds one kind of key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Publication { published_at_rafka_ms: u64, topology_version: u64 },
    TopologyVersion(u64),
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Publication { published_at_rafka_ms, topology_version } => write!(f, "published_at_rafka_ms={published_at_rafka_ms} topology_version={topology_version}"),
            Key::TopologyVersion(v) => write!(f, "topology_version={v}"),
        }
    }
}

/// The frame's name in spans and each source with its order key; empty for a frame with no order key.
fn orders_of(f: &Frame) -> (&'static str, Vec<(Source, Key)>) {
    match order_of(f) {
        Some((name, source, key)) => (name, vec![(source, key)]),
        None => ("", Vec::new()),
    }
}

/// The frame's name in spans, its source and its order key; `None` for a frame with no order key.
fn order_of(f: &Frame) -> Option<(&'static str, Source, Key)> {
    match f.data() {
        Frame::MeshTopology { mesh, topology_version, published_at_rafka_ms, .. } => {
            Some(("mesh-topology", Source::Mesh(mesh.clone()), Key::Publication { published_at_rafka_ms: *published_at_rafka_ms, topology_version: *topology_version }))
        }
        Frame::MeshTopologyDelta { mesh, topology_version, published_at_rafka_ms, .. } => {
            Some(("mesh-topology-delta", Source::Mesh(mesh.clone()), Key::Publication { published_at_rafka_ms: *published_at_rafka_ms, topology_version: *topology_version }))
        }
        Frame::NodeDeleting { op, publisher, topology_version, .. } => Some(("node-deleting", Source::Lifecycle { publisher: publisher.clone(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        Frame::NodeStopped { op, publisher, topology_version, .. } => Some(("node-stopped", Source::Lifecycle { publisher: publisher.clone(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        _ => None,
    }
}

/// A frame the receive path ignores: received after a newer one of its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Earlier {
    /// The frame's name.
    pub frame: &'static str,
    /// The source its key orders within.
    pub source: String,
    /// The newest key this channel holds for the source.
    pub held: String,
    /// The frame's key.
    pub offered: String,
}

/// The newest order key one node holds for each source, and the source version it installed of
/// each mesh: one for the node, shared by every channel it hears and by its topology-read install.
#[derive(Debug, Default)]
pub(crate) struct ReceiveOrder {
    publications: HashMap<(String, PublisherId, u64, crate::envelope::Birth), PublicationCounters>,
    newest: HashMap<Source, Key>,
    /// Operation receipt is independent of projection admission: even a version-refused frame is
    /// seen, so another publisher or a higher carried version cannot replay its operation effect.
    lifecycle_seen: HashSet<LifecycleIdentity>,
    /// Each mesh's installed publisher, version, complete image and last gossip publication.
    /// Read-answer snapshot ordinals never replace an author's retained publication identity.
    installed: HashMap<String, InstalledSource>,
    /// The version this node published last of each mesh it publishes, under its exact birth: its
    /// own publications are its floor for its own source, a read included. Never an installed image.
    published: HashMap<String, (PublisherId, u64)>,
}

#[derive(Debug)]
struct InstalledSource {
    publisher: PublisherId,
    version: u64,
    full: Option<crate::snapshot::Full>,
    snapshot_id: Option<u64>,
}

/// The publisher a `MeshTopology` or `MeshTopologyDelta` names and its `topology_version`.
fn publication_of(f: &Frame) -> Option<(&str, &PublisherId, u64)> {
    match f.data() {
        Frame::MeshTopology { mesh, publisher, topology_version, .. } => Some((mesh, publisher, *topology_version)),
        Frame::MeshTopologyDelta { mesh, source_publisher, topology_version, .. } => Some((mesh, source_publisher, *topology_version)),
        _ => None,
    }
}

impl ReceiveOrder {
    /// Record a lifecycle identity before its projection revision is judged. No handler runs on
    /// repeat, even when the first receipt's carried revision was refused. This identity is shared
    /// by every channel, and contains the exact birth but neither publisher nor event timestamp.
    pub(crate) fn admit_lifecycle_identity(&mut self, f: &Frame) -> bool {
        let (frame, op) = match f.data() {
            Frame::NodeDeleting { op, .. } => ("node-deleting", op),
            Frame::NodeStopped { op, .. } => ("node-stopped", op),
            _ => return true,
        };
        if self.lifecycle_seen.insert(LifecycleIdentity::of(op)) { return true; }
        duplicate_lifecycle(frame, op);
        false
    }

    /// Whether `f` is taken: `Err` names the frame ignored, its source and both keys. A taken frame's
    /// key is held when it is newer than the held one, except a `MeshTopology` or `MeshTopologyDelta` key, held
    /// when its snapshot installs or its delta applies ([`Self::installed`]).
    pub(crate) fn admit(&mut self, f: &Frame) -> Result<(), Earlier> {
        let (frame, keys) = orders_of(f);
        if let Some((mesh, publisher, version)) = publication_of(f) {
            if self.installed.get(mesh).is_some_and(|h| &h.publisher == publisher) {
                // Within an installed publisher, the committed version orders the projection.
                // A lower publication timestamp cannot hide a newer version or an ahead-base gap.
                self.below_installed(frame, mesh, publisher, version, || keys.first().map(|(_, k)| k.to_string()).unwrap_or_default())?;
                return Ok(());
            }
        }
        // Every key is judged before any is held: a frame ignored for one of its sources moves none.
        for (source, key) in &keys {
            if let Some(held) = self.newest.get(source).filter(|held| key < *held) {
                return Err(Earlier { frame, source: source.to_string(), held: held.to_string(), offered: key.to_string() });
            }
        }
        if let Some((mesh, publisher, topology_version)) = publication_of(f) {
            self.below_installed(frame, mesh, publisher, topology_version, || keys.first().map(|(_, k)| k.to_string()).unwrap_or_default())?;
            return Ok(());
        }
        for (source, key) in keys {
            if self.newest.get(&source).is_none_or(|held| key > *held) {
                self.newest.insert(source, key);
            }
        }
        Ok(())
    }

    /// Whether a chunk of a topology read of `mesh`, `publisher`'s `topology_version`, is taken: a read
    /// carries no stamp, so only the source version installed orders it.
    pub(crate) fn admit_read(&self, mesh: &str, publisher: &PublisherId, topology_version: u64) -> Result<(), Earlier> {
        self.below_installed("topology-read", mesh, publisher, topology_version, || format!("topology_version={topology_version}"))
    }

    /// `Err` when `publisher`'s floor of `mesh` ([`Self::floor`]) is above `topology_version`.
    fn below_installed(&self, frame: &'static str, mesh: &str, publisher: &PublisherId, topology_version: u64, offered: impl FnOnce() -> String) -> Result<(), Earlier> {
        match self.floor(mesh, publisher) {
            Some(held) if topology_version < held => {
                Err(Earlier { frame, source: mesh.to_string(), held: format!("publisher={publisher} topology_version={held}"), offered: offered() })
            }
            _ => Ok(()),
        }
    }

    /// The version of `mesh` that orders `publisher`'s publications and reads here: the higher of the
    /// source version installed, when it is `publisher`'s, and the version this node published itself
    /// as `publisher`. `None` when neither is held.
    pub(crate) fn floor(&self, mesh: &str, publisher: &PublisherId) -> Option<u64> {
        let installed = self.installed.get(mesh).filter(|h| &h.publisher == publisher).map(|h| h.version);
        let published = self.published.get(mesh).filter(|(p, _)| p == publisher).map(|(_, v)| *v);
        installed.max(published)
    }

    /// This node published `mesh` as `publisher` at `topology_version`: from now on its floor for that
    /// source (gossip.md receive rules: "against the installed floor, including a topology read").
    /// The caller holds the publisher's version lock, so the floor moves with the version it assigns.
    pub(crate) fn published(&mut self, mesh: &str, publisher: &PublisherId, topology_version: u64) {
        match self.published.get_mut(mesh) {
            Some((p, v)) if p == publisher => *v = (*v).max(topology_version),
            _ => { self.published.insert(mesh.to_string(), (publisher.clone(), topology_version)); }
        }
    }

    /// `mesh`'s snapshot state moved to `publisher`'s `topology_version`: a snapshot installed or a
    /// delta applied, stamped `published_at_rafka_ms`, or a topology read installed (`None`, a read
    /// carries no stamp). The source version is held, and the stamped key when newer.
    pub(crate) fn installed(&mut self, mesh: &str, publisher: &PublisherId, topology_version: u64, published_at_rafka_ms: Option<u64>) {
        if let Some(published_at_rafka_ms) = published_at_rafka_ms {
            let key = Key::Publication { published_at_rafka_ms, topology_version };
            let source = Source::Mesh(mesh.to_string());
            if self.newest.get(&source).is_none_or(|held| key > *held) {
                self.newest.insert(source, key);
            }
        }
        self.installed.insert(mesh.to_string(), InstalledSource { publisher: publisher.clone(), version: topology_version, full: None, snapshot_id: None });
    }
    /// The actual installed source image, shared by the backbone, mesh channel and read path.
    pub(crate) fn installed_snapshot(&self, mesh: &str) -> Option<crate::snapshot::InstalledProjection<'_>> {
        let h = self.installed.get(mesh)?;
        Some((&h.publisher, h.version, h.full.as_ref()?, h.snapshot_id))
    }

    pub(crate) fn installed_for_owner(&self, mesh: &str, owner: Option<&crate::envelope::Birth>, offered_counter: u64) -> Option<crate::snapshot::InstalledProjection<'_>> {
        let (publisher, version, full, counter) = self.installed_snapshot(mesh)?;
        let counter = match owner { Some(owner) => self.publications.get(&(mesh.to_string(), publisher.clone(), version, owner.clone())).is_some_and(|c| c.contains(offered_counter)).then_some(offered_counter), None => counter };
        Some((publisher, version, full, counter))
    }

    pub(crate) fn publication_installed(&mut self, mesh: &str, publisher: &PublisherId, version: u64, owner: &crate::envelope::Birth, counter: u64) {
        self.publications.retain(|(m, p, v, _), _| m != mesh || (p == publisher && *v == version));
        self.publications.entry((mesh.to_string(), publisher.clone(), version, owner.clone())).or_default().insert(counter);
    }

    /// The image and its floor move only after the complete projection was installed. Keeping
    /// them in one record prevents another channel from classifying against an older local image.
    pub(crate) fn installed_projection(&mut self, mesh: &str, publisher: &PublisherId, version: u64, stamp: Option<u64>, full: &crate::snapshot::Full, snapshot_id: Option<u64>) {
        let retained = self.installed.get(mesh).filter(|h| &h.publisher == publisher && h.version == version).and_then(|h| h.snapshot_id);
        self.installed(mesh, publisher, version, stamp);
        let h = self.installed.get_mut(mesh).expect("just installed");
        h.full = Some(full.clone());
        h.snapshot_id = snapshot_id.or(retained);
    }

}

#[cfg(test)]
mod tests {

    #[test]
    fn publication_receipts_compact_runs_without_treating_unheard_gaps_as_duplicates() {
        let mut ids = PublicationCounters::default();
        ids.insert(1); ids.insert(3);
        assert!(ids.contains(1) && ids.contains(3)); assert!(!ids.contains(2));
        ids.insert(2); assert_eq!(ids.0.len(), 1);
        for counter in (4..=10_000).rev() { ids.insert(counter); }
        assert_eq!(ids.0.len(), 1, "a quiet source keeps one run rather than one record per heartbeat publication");
        assert!(ids.contains(1) && ids.contains(10_000)); assert!(!ids.contains(10_001));
        ids.insert(0); ids.insert(u64::MAX);
        assert!(ids.contains(0) && ids.contains(u64::MAX)); assert!(!ids.contains(u64::MAX - 1));
    }

    /// The source's descriptive row name cannot reset the exact birth's installed floor.
    #[test]
    fn a_renamed_publisher_keeps_its_installed_version_floor() {
        let publisher = PublisherId { node: "mesh1.admin.1".into(), node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
        let mut order = ReceiveOrder::default();
        order.installed_projection("mesh1", &publisher, 10, None, &crate::snapshot::Full::default(), None);
        let renamed = PublisherId { node: "mesh1.admin.2".into(), ..publisher };
        let old = Frame::MeshTopology { mesh: "mesh1".into(), publisher: renamed.clone(), forwarded_by: None, topology_version: 9, published_at_rafka_ms: 999, snapshot_id: 1, chunk_index: 0, chunk_count: 1, digests: vec![], in_flight: vec![], deleted: vec![] };
        assert!(order.admit(&old).is_err());
        let operation = rdm_core::LifecycleOp { build_id: "b".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into(), node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint(), name: "mesh1.rpc.1".parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        assert!(order.admit(&StatusEvent::Stopped.frame(operation.clone(), renamed.clone(), 20)).is_ok());
        let relabeled = PublisherId { node: "mesh1.admin.3".into(), ..renamed };
        assert!(order.admit(&StatusEvent::Stopped.frame(operation, relabeled, 19)).is_err(), "a carried projection floor belongs to the birth, not its label");
    }

    /// CONTRACT: every topology Frame variant has exactly one receive-table row naming its
    /// category, subject, ordering and identity; a new variant without a rule fails by name.
    #[test]
    fn every_frame_kind_has_a_row_in_the_receive_rules() {
        let source = include_str!("topology.rs");
        let declaration = source.split_once("pub enum Frame {").unwrap().1.split_once("\n}\n").unwrap().0;
        let spec = include_str!("../../../docs/architecture/gossip.md");
        let table = spec.split_once("| frame / type | category | source / subject | sequence / data validity | id / identity |").unwrap().1.split_once("\n\n").unwrap().0;
        let rows: Vec<Vec<&str>> = table.lines().skip(2).map(|line| line.trim_matches('|').split(" | ").map(str::trim).collect()).collect();
        let mut count = 0;
        for line in declaration.lines() {
            let Some(line) = line.strip_prefix("    ") else { continue };
            let name = line.split([' ', '(', ',', '{']).next().unwrap_or_default();
            if name == "Authored" { continue; }
            if !name.chars().next().is_some_and(char::is_uppercase) { continue; }
            count += 1;
            let named = format!("`{name}`");
            let matches: Vec<_> = rows.iter().filter(|row| row[0].contains(&named)).collect();
            assert_eq!(matches.len(), 1, "Frame::{name} must map to exactly one receive-rules row");
            let row = matches[0];
            assert_eq!(row.len(), 5, "Frame::{name}: category, subject, ordering and identity are required");
            for (field, value) in ["category", "subject", "ordering", "identity"].into_iter().zip(&row[1..]) {
                assert!(!value.is_empty(), "Frame::{name} has no {field} rule");
            }
        }
        assert!(count > 0, "the topology Frame declaration must contain variants");
    }

    use super::*;
    use crate::topology::StatusEvent;
    use crate::snapshot::PublisherId;
    use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshNode, NodeId};

    fn birth(path: &str) -> MeshDigest {
        MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { node_id: NodeId::mint(), name: path.parse().unwrap(), endpoint_id: EndpointId(format!("key-{path}")), transport_addr: "127.0.0.1:1".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None, is_fabric_primary: None
        }
    }

    fn mesh_topology(mesh: &str, publisher: &PublisherId, published_at_rafka_ms: u64, topology_version: u64, chunk_index: u32) -> Frame {
        Frame::MeshTopology { mesh: mesh.into(), publisher: publisher.clone(), forwarded_by: None, topology_version, published_at_rafka_ms, snapshot_id: published_at_rafka_ms, chunk_index, chunk_count: 2, digests: vec![], in_flight: vec![], deleted: vec![] }
    }

    fn delta(mesh: &str, publisher: &PublisherId, published_at_rafka_ms: u64, base_version: u64, topology_version: u64) -> Frame {
        Frame::MeshTopologyDelta { mesh: mesh.into(), source_publisher: publisher.clone(), base_version, topology_version, published_at_rafka_ms, changed: vec![], removed: vec![], in_flight: vec![], deleted: vec![] }
    }

    fn publisher(path: &str) -> PublisherId {
        PublisherId { node: path.into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }
    }

    fn op(d: &MeshDigest, operation: &str) -> LifecycleOp {
        LifecycleOp { build_id: "3qcfk1kq47m9".into(), attempt: 1, operation: format!("{operation}:{}", d.node.name), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
    }

    /// CONTRACT (R-RX3: MeshTopology and MeshTopologyDelta order by `published_at_rafka_ms` then
    /// `topology_version`, per mesh; R-RX2: no cross-publisher rule; gossip.md §3.1: a snapshot install
    /// and a delta move the held source version): both chunks of one publication are taken (they share
    /// its key), and its key is held once it installed; a chunk of the installed publication reaches its content classifier
    /// again (the classifier ignores its identical repeat); a full of another publisher stamped later is taken whatever its
    /// version; a delta stamped later is taken; then a full and a delta stamped before what is held are
    /// ignored, each naming the mesh, the key held and the key offered, while another mesh's publication
    /// of any stamp is taken. Must NOT happen: a later chunk of a held publication ignored; a
    /// publication ordered against another mesh's.
    #[test]
    fn a_publication_stamped_before_the_newest_held_of_its_mesh_is_ignored_and_one_publications_chunks_are_all_taken() {
        let mut order = ReceiveOrder::default();
        let (p1, p2) = (publisher("mesh1.admin.1"), publisher("mesh1.admin.2"));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p1, 100, 7, 0)), Ok(()));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p1, 100, 7, 1)), Ok(()), "the second chunk of the same publication is taken");
        order.installed("mesh1", &p1, 7, Some(100));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p1, 100, 7, 0)), Ok(()), "a chunk of the installed publication is taken: the handler absorbs it");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p2, 150, 1, 0)), Ok(()), "a later publication is taken whatever its version and publisher");
        order.installed("mesh1", &p2, 1, Some(150));
        assert_eq!(order.admit(&delta("mesh1", &p2, 160, 1, 2)), Ok(()));
        order.installed("mesh1", &p2, 2, Some(160));
        let ignored_full = order.admit(&mesh_topology("mesh1", &p1, 120, 9, 0)).unwrap_err();
        assert_eq!(ignored_full, Earlier { frame: "mesh-topology", source: "mesh1".into(), held: "published_at_rafka_ms=160 topology_version=2".into(), offered: "published_at_rafka_ms=120 topology_version=9".into() });
        let ignored_delta = order.admit(&delta("mesh1", &p1, 110, 7, 8)).unwrap_err();
        assert_eq!((ignored_delta.frame, ignored_delta.offered.as_str()), ("mesh-topology-delta", "published_at_rafka_ms=110 topology_version=8"));
        assert_eq!(order.admit(&delta("mesh1", &p2, 160, 1, 3)), Ok(()), "the same stamp at a later version is newer");
        assert_eq!(order.admit(&mesh_topology("mesh2", &p1, 1, 1, 0)), Ok(()), "another mesh is another source");
    }

    /// CONTRACT (Luke 2026-10-13: "Snapshot state advances atomically once every chunk has arrived";
    /// gossip.md §3.1: "a topology read install move[s] the held source version"): a mesh's version
    /// moves only when a snapshot installs, so a chunk of a newer publication that never completed
    /// holds nothing and an older publication's chunks are still taken. A topology read carries no
    /// stamp: the source version it installed (publisher, `topology_version`) orders what follows by
    /// the publisher's revision, so that publisher's full, delta or read chunk at a lower version is
    /// ignored, naming the version held, and its full at the installed version is taken; another
    /// publisher's publication is not ordered against that revision. Must NOT happen: a partial
    /// snapshot's stamp ignoring the publication still installing; a version below the one a read
    /// installed reaching a handler.
    #[test]
    fn a_mesh_version_moves_when_its_snapshot_installs_and_a_read_installed_version_orders_its_publishers_lower_versions() {
        let mut order = ReceiveOrder::default();
        let (p, other) = (publisher("mesh1.admin.1"), publisher("mesh1.admin.2"));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 200, 8, 0)), Ok(()), "chunk 0 of v8: its snapshot never completes");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 150, 7, 1)), Ok(()), "v7's chunk is taken: nothing installed is newer");
        order.installed("mesh1", &p, 7, Some(150));
        order.installed("mesh1", &p, 9, None);
        let held = format!("publisher={p} topology_version=9");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 240, 8, 0)), Err(Earlier { frame: "mesh-topology", source: "mesh1".into(), held: held.clone(), offered: "published_at_rafka_ms=240 topology_version=8".into() }));
        assert_eq!(order.admit(&delta("mesh1", &p, 245, 7, 8)).map_err(|e| (e.frame, e.held)), Err(("mesh-topology-delta", held.clone())));
        assert_eq!(order.admit_read("mesh1", &p, 8), Err(Earlier { frame: "topology-read", source: "mesh1".into(), held, offered: "topology_version=8".into() }));
        assert_eq!(order.admit_read("mesh1", &p, 9), Ok(()), "a read of the installed version is taken");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 250, 9, 0)), Ok(()), "the publisher's full at the installed version is taken");
        assert_eq!(order.admit(&mesh_topology("mesh1", &other, 260, 1, 0)), Ok(()), "another publisher is not ordered against that revision");
        assert_eq!(order.admit_read("mesh2", &p, 1), Ok(()), "another mesh is another source");
    }

    /// CONTRACT (gossip.md receive rules, `MeshTopology`: "`P(topology_version)` against the installed
    /// floor, including a topology read"; #3211 red 2): what this node published of its own mesh is its
    /// floor for that source, so its own exact birth's read or full below it is refused naming the
    /// version published, at it is taken, and another publisher of the mesh is not ordered against it.
    /// The installed floor of the same publisher still orders when it is the higher. Must NOT happen:
    /// a primary at its published v28 installing a read of its own v25.
    #[test]
    fn a_read_of_its_own_source_below_the_version_it_published_is_refused() {
        let mut order = ReceiveOrder::default();
        let (me, other) = (publisher("mesh2.admin.2"), publisher("mesh2.admin.1"));
        order.published("mesh2", &me, 27);
        order.published("mesh2", &me, 28);
        order.published("mesh2", &me, 26);
        let held = format!("publisher={me} topology_version=28");
        assert_eq!(order.admit_read("mesh2", &me, 25), Err(Earlier { frame: "topology-read", source: "mesh2".into(), held: held.clone(), offered: "topology_version=25".into() }));
        assert_eq!(order.floor("mesh2", &me), Some(28), "the floor never moves down");
        assert_eq!(order.admit(&mesh_topology("mesh2", &me, 1_000, 25, 0)).map_err(|e| e.held), Err(held), "its own full below what it published is refused too");
        assert_eq!(order.admit_read("mesh2", &me, 28), Ok(()), "a read of the version it published is not refused for its order");
        assert_eq!(order.admit_read("mesh2", &other, 3), Ok(()), "another publisher is not ordered against this node's publications");
        assert_eq!(order.admit_read("mesh1", &me, 1), Ok(()), "another mesh is another source");
        order.installed("mesh2", &me, 30, None);
        assert_eq!(order.floor("mesh2", &me), Some(30), "the installed floor of the same publisher orders when it is the higher");
        assert!(order.admit_read("mesh2", &me, 29).is_err());
    }

    /// CONTRACT (R-RX3: Digest orders by `digest_seq`, per birth; gossip.md §3.1: a new incarnation's
    /// `digest_seq` starts again at 1): a heartbeat below the held `digest_seq` of its exact birth is
    /// ignored, naming the birth and both sequences; a copy of the held one is taken (the view absorbs
    /// it); a new incarnation of the same node at sequence 1 is taken. Must NOT happen: a restarted
    /// node's heartbeats ignored against its previous birth's sequence.
    #[test]
    fn a_heartbeat_below_the_held_digest_seq_of_its_birth_is_ignored_and_a_new_incarnation_starts_again() {
        let digest_view = crate::topology::DigestView::default();
        let d = birth("mesh1.rpc.1");
        let at = |seq| MeshDigest { digest_seq: seq, ..d.clone() };
        assert!(digest_view.record(at(7)));
        assert!(!digest_view.record(at(7)), "the installed heartbeat classifier ignores its identical repeat");
        assert!(!digest_view.record(at(3)), "a heartbeat below the installed exact birth's sequence is ignored");
        assert_eq!(digest_view.get(d.node.node_id.as_str()).unwrap().0.digest_seq, 7);
        let restarted = MeshDigest { node: MeshNode { incarnation: IncarnationId::mint(), ..d.node.clone() }, digest_seq: 1, ..d.clone() };
        assert!(digest_view.record(restarted), "a new incarnation is another source");
    }

    /// CONTRACT (R-RX3: lifecycle facts order by `topology_version`; the version is the publisher's
    /// own counter of the subject's mesh, so the source is the publisher and that mesh): a fact below
    /// the held version of its publisher and mesh is ignored, naming both versions; two facts at one
    /// version (different subjects) are both taken; another publisher's fact at a lower version is
    /// taken (a successor counts from 1 again), and so is the same publisher's fact of another mesh.
    /// Must NOT happen: a successor's facts ignored against its predecessor's counter.
    #[test]
    fn a_lifecycle_fact_below_the_held_version_of_its_publisher_and_mesh_is_ignored() {
        let mut order = ReceiveOrder::default();
        let (primary, successor, acting) = (publisher("mesh1.admin.1"), publisher("mesh1.admin.2"), publisher("mesh2.admin.1"));
        let (a, b, c) = (birth("mesh1.rpc.1"), birth("mesh1.rpc.2"), birth("mesh3.rpc.1"));
        assert_eq!(order.admit(&StatusEvent::Deleting.frame(op(&a, "delete-node"), primary.clone(), 8)), Ok(()));
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&b, "stop-node"), primary.clone(), 8)), Ok(()), "a second fact at the held version is taken");
        assert_eq!(
            order.admit(&StatusEvent::Deleting.frame(op(&b, "delete-node"), primary.clone(), 7)),
            Err(Earlier { frame: "node-deleting", source: format!("{primary} mesh1"), held: "topology_version=8".into(), offered: "topology_version=7".into() })
        );
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&b, "stop-node"), successor, 1)), Ok(()), "another publisher counts its own versions");
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&c, "stop-node"), acting.clone(), 9)), Ok(()));
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&a, "node.stop"), acting, 2)), Ok(()), "the same publisher's fact of another mesh is another source");
    }

    /// CONTRACT (R-RX3): a frame with no order key (a status, a NodeDeleted) is taken however often
    /// it arrives.
    #[test]
    fn a_frame_with_no_order_key_is_always_taken() {
        let mut order = ReceiveOrder::default();
        let d = birth("mesh1.rpc.1");
        let deleted = Frame::NodeDeleted { op: op(&d, "delete-node"), forwarded_by: None };
        let status = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 };
        for _ in 0..2 {
            assert_eq!(order.admit(&deleted), Ok(()));
            assert_eq!(order.admit(&status), Ok(()));
        }
    }
}
