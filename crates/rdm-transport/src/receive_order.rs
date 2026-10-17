//! Receive classification for gossip and topology-read installs. Decisions use the frame's
//! declared subject, identity and ordering fields, never the transport sender.
//!
//! MeshStatus and FabricStatus use [`admit_status`] against the actual held projection under the status
//! book's installation lock. An older timestamp emits `via-stale` with both keys. Equal declared
//! publisher/timestamp and identical status emits `via-duplicate` with outcome `ignored`;
//! different status content emits `via-inconsistent`. Forwarding metadata is not status content.
//! No publisher revision is inferred: the MeshStatus/FabricStatus publisher-revision gap (R10)
//! remains open, including the existing timestamp comparison across publishers.
//!
//! Digest uses [`admit_digest`] against the actual installed heartbeat under the digest book's
//! lock, including a read-installed floor. Lower is `via-stale`; equal wire content is
//! `via-duplicate`; equal sequence with different wire content is `via-inconsistent`.
//! The primary's separately held membership status is not heartbeat content. A refused or
//! repeated digest supplies no address and no clock observation to its caller.
//!
//! Seated and PrimaryUpdating/PrimaryUpdated use [`SeatReceipts`] under the seat installation
//! lock, against the actual installed records and retained primary publication content. A read or
//! local seat install supplies the settled floor; updating moves no holder. Every named cohort is
//! checked before any receipt or record moves. Lower revision/stage is `via-stale`, equal identical
//! content is `via-duplicate`, and equal conflicting content is `via-inconsistent`. Local election
//! candidate selection remains SeatHolder::supersedes; receipt never uses its equal-revision tie.
//!
//! [`ReceiveOrder`] holds the remaining keyed families' ordering state, shared by all channels
//! and topology-read installs:
//! - Members and MembersDelta: the actual installed publisher/version floor orders that source,
//!   regardless of publication timestamp. The existing cross-publisher publication check remains.
//!   SnapshotReceiver compares complete normalized projection content, chunk identity and delta
//!   content under its installation lock: duplicates install nothing; conflicts are inconsistent.
//!   Equal-key unfinished chunks remain admissible. An ahead-base delta requests repair via-gap;
//!   the exact repeated publication never completes repair. A new publication at the same material
//!   version carries fresh hearing evidence. A directed topology read can resume the held version.
//! - NodeDeleting, NodeRestarting and NodeStopped: operation identity is recorded before the
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
//! Round hooks use [`RoundReceipts`] shared by the view's local apply path and all channels. Each
//! full operation identity has one ordinary receive span and at most one carrier invocation;
//! repeats emit `via-duplicate`. These receipts drive no checklist or lifecycle. Only the admin's
//! existing open-round book decides whether an ordinary check-in is carried (Q21: x).
//!
//! [`refuse_reserved`] handles Concern (position 10, Q23), NodeConnectionsDeleting (11),
//! NodeLeaving (12), NodeLeft (13), NodeConnectionsDeleted (14), and NewFabricPrimary (15).
//! Receipt emits `via-reserved` before any identity book, apply span or carrier runs. No reserved
//! body installs a projection, records a fact, registers a contact or commands investigation.

use crate::membership::Frame;
use crate::snapshot::PublisherId;
use std::collections::{HashMap, HashSet};

/// Reserved wire positions have one receive outcome. Inspect only the kind, before any book,
/// identity receipt, ordinary apply span or completion carrier can run.
pub(crate) fn refuse_reserved(f: &Frame, node: &str, via: &str) -> bool {
    let (frame, position) = match f {
        Frame::Concern { .. } => ("concern", 10),
        Frame::NodeConnectionsDeleting { .. } => ("node-connections-deleting", 11),
        Frame::NodeLeaving { .. } => ("node-leaving", 12),
        Frame::NodeLeft { .. } => ("node-left", 13),
        Frame::NodeConnectionsDeleted { .. } => ("node-connections-deleted", 14),
        Frame::NewFabricPrimary { .. } => ("new-fabric-primary", 15),
        _ => return false,
    };
    tracing::info_span!("rdm.mesh.membership.reject.via-reserved", node, frame, position, via)
        .in_scope(|| tracing::info!("the reserved position installs nothing and invokes no carrier"));
    true
}

/// Admit a replacement status against the projection held under the status book's lock. The
/// declared subject, publisher and change instant identify it; forwarding metadata is not content.
/// This also serves statuses installed by a read, so classification and installation share the
/// same held fact instead of maintaining a separate receive history.
pub(crate) fn admit_status(frame: &'static str, subject: &str, held: Option<(&str, u64, &str)>, status: &str, publisher: &str, revision: u64) -> bool {
    let Some((held_publisher, held_revision, held_status)) = held else { return true };
    if revision < held_revision {
        tracing::info_span!(
            "rdm.mesh.membership.reject.via-stale",
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
                "rdm.mesh.membership.reject.via-inconsistent",
                frame,
                subject = %format!("{subject} {publisher}"),
                revision,
                held_status = %held_status,
                frame_status = status,
            ).in_scope(|| tracing::info!("a status at the held revision has different content: refused"));
        } else {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-duplicate",
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
            "rdm.mesh.membership.reject.via-stale",
            node, channel, frame = "digest", subject = %subject,
            held_key = %format!("digest_seq={}", held.digest_seq),
            frame_key = %format!("digest_seq={}", offered.digest_seq),
        ).in_scope(|| tracing::info!("the digest sequence is below the installed exact birth's sequence"));
        return false;
    }
    if offered.digest_seq == held.digest_seq {
        if rdm_core::wire::WireDigest::from(held) == rdm_core::wire::WireDigest::from(offered) {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-duplicate",
                node, channel, frame = "digest", identity = %format!("{subject} digest_seq={}", offered.digest_seq),
                outcome = "ignored",
            ).in_scope(|| tracing::info!("the identical heartbeat is ignored, with no address registration"));
        } else {
            tracing::info_span!(
                "rdm.mesh.membership.reject.via-inconsistent",
                node, channel, frame = "digest", subject = %subject, revision = offered.digest_seq,
            ).in_scope(|| tracing::info!("equal digest sequence with different heartbeat content: nothing installed"));
        }
        return false;
    }
    true
}

/// One seat publication's retained receive content. Installed records themselves remain the
/// authority for settled revisions, including local elections and topology reads. Only a primary
/// publication needs extra evidence: its updating stage and the operation/body at that revision.
#[derive(Debug, Default)]
pub(crate) struct SeatReceipts {
    publications: HashMap<rdm_core::seat::transfer::Seat, SeatPublication>,
}

#[derive(Debug)]
struct SeatPublication {
    revision: u64,
    settled: bool,
    record: rdm_core::SeatHolder,
    frame: &'static str,
    content: Vec<u8>,
}

impl SeatReceipts {
    /// Local execution committed this exact primary publication. Retain its content too: gossip
    /// cannot rewrite an operation at the same revision just because its row was installed locally.
    pub(crate) fn installed_primary(&mut self, op: &rdm_core::LifecycleOp, seats: &[rdm_core::seat::transfer::CommittedSeat]) {
        let content = Frame::PrimaryUpdated { op: op.clone(), seats: seats.to_vec() }.encode();
        for c in seats {
            let subject = receipt_seat(c);
            if self.publications.get(&subject).is_none_or(|h| (c.revision, true) >= (h.revision, h.settled)) {
                self.publications.insert(subject, SeatPublication { revision: c.revision, settled: true, record: c.record(), frame: "primary-updated", content: content.clone() });
            }
        }
    }

    /// A local election installed a holder. Any older receive content no longer describes that
    /// installed record; a genuinely newer updating publication still retains its own stage.
    pub(crate) fn installed_seat(&mut self, seat: rdm_core::Seat, holder: &rdm_core::SeatHolder) {
        let subject = rdm_core::seat::transfer::Seat::of(seat, &holder.mesh);
        if self.publications.get(&subject).is_some_and(|h| (holder.revision, true) >= (h.revision, h.settled)) {
            self.publications.remove(&subject);
        }
    }

    /// Called under the seat installation lock. Judge every named cohort against both its actual
    /// installed record and the retained primary stage before changing any receipt or projection.
    /// No refused frame can advance another cohort's floor. The caller installs accepted settled
    /// records under this same lock; updating never installs a holder.
    pub(crate) fn admit(&mut self, f: &Frame, current: &[(rdm_core::seat::transfer::CommittedSeat, Option<rdm_core::SeatHolder>)]) -> bool {
        let (frame, settled) = match f {
            Frame::Seated { .. } => ("seated", true),
            Frame::PrimaryUpdating { .. } => ("primary-updating", false),
            Frame::PrimaryUpdated { .. } => ("primary-updated", true),
            _ => unreachable!("only seat publications use the seat classifier"),
        };
        let content = f.encode();
        let mut advances = false;
        // Staleness wins across the complete row. A settled record at the same revision is later
        // than updating, whether that record arrived in gossip, a read, or local execution.
        for (offered, installed) in current {
            let previous = self.publications.get(&receipt_seat(offered));
            let held = installed.as_ref().map(|h| (h.revision, true)).into_iter()
                .chain(previous.map(|h| (h.revision, h.settled))).max();
            let key = (offered.revision, settled);
            if let Some(held) = held {
                if key < held {
                    let subject = seat_subject(&offered.seat);
                    tracing::info_span!(
                        "rdm.mesh.membership.reject.via-stale", frame, subject = %subject,
                        held_key = %seat_order(held), frame_key = %seat_order(key),
                    ).in_scope(|| tracing::info!("a seat publication below the held cohort revision and stage is refused"));
                    return false;
                }
                advances |= key > held;
            } else { advances = true; }
        }
        for (offered, installed) in current {
            let key = (offered.revision, settled);
            let previous = self.publications.get(&receipt_seat(offered)).filter(|h| (h.revision, h.settled) == key);
            let installed = installed.as_ref().filter(|h| (h.revision, true) == key);
            let inconsistent = installed.is_some_and(|h| *h != offered.record()) || previous.is_some_and(|h|
                h.record != offered.record() || (h.frame == frame && h.content != content));
            if inconsistent {
                let subject = seat_subject(&offered.seat);
                tracing::info_span!(
                    "rdm.mesh.membership.reject.via-inconsistent", frame, subject = %subject, revision = offered.revision,
                ).in_scope(|| tracing::info!("equal seat revision and stage with different content: no cohort installed"));
                return false;
            }
        }
        if !advances {
            let identity = current.iter().map(|(c, _)| format!("{} {}", seat_subject(&c.seat), seat_order((c.revision, settled)))).collect::<Vec<_>>().join(", ");
            tracing::info_span!(
                "rdm.mesh.membership.update.via-duplicate", frame, identity = %identity, outcome = "ignored",
            ).in_scope(|| tracing::info!("the identical seat publication is ignored"));
            return false;
        }
        for (c, _) in current {
            if frame == "seated" {
                self.publications.remove(&receipt_seat(c));
            } else {
                self.publications.insert(receipt_seat(c), SeatPublication { revision: c.revision, settled, record: c.record(), frame, content: content.clone() });
            }
        }
        true
    }
}

fn receipt_seat(c: &rdm_core::seat::transfer::CommittedSeat) -> rdm_core::seat::transfer::Seat {
    rdm_core::seat::transfer::Seat::of(c.seat.kind(), c.mesh_key().unwrap_or_default())
}

fn seat_subject(seat: &rdm_core::seat::transfer::Seat) -> String {
    match seat {
        rdm_core::seat::transfer::Seat::FabricPrimary => seat.name().to_string(),
        rdm_core::seat::transfer::Seat::MeshPrimary { mesh } | rdm_core::seat::transfer::Seat::Cohort { mesh, .. } => format!("{} {mesh}", seat.name()),
    }
}

fn seat_order((revision, settled): (u64, bool)) -> String {
    format!("revision={revision} {}", if settled { "updated" } else { "updating" })
}

/// Name a publication's classification at the installation door. The caller holds the actual
/// source projection lock; forwarding metadata never supplies its identity or ordering.
pub(crate) fn publication_duplicate(frame: &'static str, identity: &str) {
    tracing::info_span!("rdm.mesh.membership.update.via-duplicate", frame, identity, outcome = "ignored")
        .in_scope(|| tracing::info!("the publication identity and content are already held"));
}

pub(crate) fn publication_inconsistent(frame: &'static str, mesh: &str, publisher: &PublisherId, revision: u64) {
    tracing::info_span!("rdm.mesh.membership.reject.via-inconsistent", frame, subject = %format!("{mesh} {publisher}"), revision)
        .in_scope(|| tracing::info!("equal publication revision or chunk identity with different content: refused"));
}

pub(crate) fn publication_stale(frame: &'static str, mesh: &str, publisher: &PublisherId, held: u64, offered: u64) {
    tracing::info_span!("rdm.mesh.membership.reject.via-stale", frame, subject = %format!("{mesh} {publisher}"), held_key = %format!("topology_version={held}"), frame_key = %format!("topology_version={offered}"))
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
            "rdm.mesh.membership.update.via-duplicate",
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
        "rdm.mesh.membership.update.via-duplicate",
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
            "rdm.mesh.membership.update.via-lifecycle-reconciled",
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
        "rdm.mesh.membership.reject.via-stale",
        frame, subject, held_key = %format!("topology_version={held}"), frame_key = %format!("topology_version={offered}"),
    ).in_scope(|| tracing::info!("the lifecycle frame's carried projection revision is below the held one: refused"));
}

/// One hook's identity; authored evidence time, publisher path and forwarding metadata do not
/// distinguish another operation fact. The exact subject birth and kind always do.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RoundIdentity {
    kind: &'static str,
    fabric_id: rdm_core::FabricId,
    build_id: String,
    attempt: u32,
    operation: String,
    node_id: rdm_core::NodeId,
    incarnation: rdm_core::IncarnationId,
}

/// Hook effects already applied during this receiver's lifetime. This records receipt identity,
/// never command admission or round completion; no gossip receipt can change a checklist.
#[derive(Default)]
pub(crate) struct RoundReceipts {
    applied: HashSet<RoundIdentity>,
}

impl RoundReceipts {
    /// Admit each full operation identity once, before its span and carrier effect. Non-hook
    /// frames are untouched. The caller serializes admission across local apply and all channels.
    pub(crate) fn admit(&mut self, f: &Frame) -> bool {
        let (kind, hook) = match f {
            Frame::StateCommitting { hook, .. } => ("state-committing", hook),
            Frame::StateCommitted { hook, .. } => ("state-committed", hook),
            Frame::TrafficOpening { hook, .. } => ("traffic-opening", hook),
            Frame::TrafficOpened { hook, .. } => ("traffic-opened", hook),
            _ => return true,
        };
        if self.applied.insert(RoundIdentity { kind, fabric_id: hook.fabric_id.clone(), build_id: hook.build_id.clone(), attempt: hook.attempt, operation: hook.operation.clone(), node_id: hook.node_id.clone(), incarnation: hook.incarnation.clone() }) {
            return true;
        }
        tracing::info_span!(
            "rdm.mesh.membership.update.via-duplicate",
            frame = kind,
            identity = %format!("{kind} {} {} {} {} {}/{}", hook.fabric_id, hook.build_id, hook.attempt, hook.operation, hook.node_id, hook.incarnation),
            outcome = "ignored",
        ).in_scope(|| tracing::info!("the round hook identity was already applied: ignored"));
        false
    }
}

/// The source a frame's order key orders within.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Source {
    /// One mesh's publications.
    Mesh(String),
    /// One publisher's lifecycle facts of one mesh.
    Lifecycle { publisher: String, mesh: String },

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
    match f {
        Frame::Members { mesh, topology_version, published_at_rafka_ms, .. } => {
            Some(("members", Source::Mesh(mesh.clone()), Key::Publication { published_at_rafka_ms: *published_at_rafka_ms, topology_version: *topology_version }))
        }
        Frame::MembersDelta { mesh, topology_version, published_at_rafka_ms, .. } => {
            Some(("members-delta", Source::Mesh(mesh.clone()), Key::Publication { published_at_rafka_ms: *published_at_rafka_ms, topology_version: *topology_version }))
        }
        Frame::NodeDeleting { op, publisher, topology_version, .. } => Some(("node-deleting", Source::Lifecycle { publisher: publisher.to_string(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        Frame::NodeRestarting { op, publisher, topology_version, .. } => Some(("node-restarting", Source::Lifecycle { publisher: publisher.to_string(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        Frame::NodeStopped { op, publisher, topology_version, .. } => Some(("node-stopped", Source::Lifecycle { publisher: publisher.to_string(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
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
    newest: HashMap<Source, Key>,
    /// Operation receipt is independent of projection admission: even a version-refused frame is
    /// seen, so another publisher or a higher carried version cannot replay its operation effect.
    lifecycle_seen: HashSet<LifecycleIdentity>,
    /// Each mesh's installed publisher, version, complete image and last gossip publication.
    /// Read-answer snapshot ordinals never replace an author's retained publication identity.
    installed: HashMap<String, InstalledSource>,
}

#[derive(Debug)]
struct InstalledSource {
    publisher: PublisherId,
    version: u64,
    full: Option<crate::snapshot::Full>,
    snapshot_id: Option<u64>,
}

/// The publisher a `Members` or `MembersDelta` names and its `topology_version`.
fn publication_of(f: &Frame) -> Option<(&str, &PublisherId, u64)> {
    match f {
        Frame::Members { mesh, publisher, topology_version, .. } => Some((mesh, publisher, *topology_version)),
        Frame::MembersDelta { mesh, source_publisher, topology_version, .. } => Some((mesh, source_publisher, *topology_version)),
        _ => None,
    }
}

impl ReceiveOrder {
    /// Record a lifecycle identity before its projection revision is judged. No handler runs on
    /// repeat, even when the first receipt's carried revision was refused. This identity is shared
    /// by every channel, and contains the exact birth but neither publisher nor event timestamp.
    pub(crate) fn admit_lifecycle_identity(&mut self, f: &Frame) -> bool {
        let (frame, op) = match f {
            Frame::NodeDeleting { op, .. } => ("node-deleting", op),
            Frame::NodeRestarting { op, .. } => ("node-restarting", op),
            Frame::NodeStopped { op, .. } => ("node-stopped", op),
            _ => return true,
        };
        if self.lifecycle_seen.insert(LifecycleIdentity::of(op)) { return true; }
        duplicate_lifecycle(frame, op);
        false
    }

    /// Whether `f` is taken: `Err` names the frame ignored, its source and both keys. A taken frame's
    /// key is held when it is newer than the held one, except a `Members` or `MembersDelta` key, held
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

    /// `Err` when the source version installed of `mesh` is `publisher`'s, above `topology_version`.
    fn below_installed(&self, frame: &'static str, mesh: &str, publisher: &PublisherId, topology_version: u64, offered: impl FnOnce() -> String) -> Result<(), Earlier> {
        match self.installed.get(mesh) {
            Some(h) if &h.publisher == publisher && topology_version < h.version => {
                Err(Earlier { frame, source: mesh.to_string(), held: format!("publisher={} topology_version={}", h.publisher, h.version), offered: offered() })
            }
            _ => Ok(()),
        }
    }

    /// The `topology_version` of the source version installed of `mesh`.
    pub(crate) fn installed_version(&self, mesh: &str) -> Option<u64> {
        self.installed.get(mesh).map(|h| h.version)
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

    /// CONTRACT: every membership Frame variant has exactly one receive-table row naming its
    /// category, subject, ordering and identity; a new variant without a rule fails by name.
    #[test]
    fn every_frame_kind_has_a_row_in_the_receive_rules() {
        let source = include_str!("membership.rs");
        let declaration = source.split_once("pub enum Frame {").unwrap().1.split_once("\n}\n").unwrap().0;
        let spec = include_str!("../../../docs/architecture/gossip.md");
        let table = spec.split_once("| frame | category | subject | ordering | identity |").unwrap().1.split_once("\n\n").unwrap().0;
        let rows: Vec<Vec<&str>> = table.lines().skip(2).map(|line| line.trim_matches('|').split(" | ").map(str::trim).collect()).collect();
        let mut count = 0;
        for line in declaration.lines() {
            let Some(line) = line.strip_prefix("    ") else { continue };
            let name = line.split([' ', '(', ',', '{']).next().unwrap_or_default();
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
        assert!(count > 0, "the membership Frame declaration must contain variants");
    }

    use super::*;
    use crate::membership::StatusEvent;
    use crate::snapshot::PublisherId;
    use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshNode, NodeId};

    fn birth(path: &str) -> MeshDigest {
        MeshDigest {
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { node_id: NodeId::mint(), name: path.parse().unwrap(), endpoint_id: EndpointId(format!("key-{path}")), transport_addr: "127.0.0.1:1".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None },
            status: MemberStatus::ReadyForTraffic,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            config: Default::default(),
        }
    }

    fn members(mesh: &str, publisher: &PublisherId, published_at_rafka_ms: u64, topology_version: u64, chunk_index: u32) -> Frame {
        Frame::Members { mesh: mesh.into(), publisher: publisher.clone(), forwarded_by: None, topology_version, published_at_rafka_ms, snapshot_id: published_at_rafka_ms, chunk_index, chunk_count: 2, digests: vec![], in_flight: vec![], departed: vec![] }
    }

    fn delta(mesh: &str, publisher: &PublisherId, published_at_rafka_ms: u64, base_version: u64, topology_version: u64) -> Frame {
        Frame::MembersDelta { mesh: mesh.into(), source_publisher: publisher.clone(), base_version, topology_version, published_at_rafka_ms, changed: vec![], removed: vec![], in_flight: vec![], departed: vec![] }
    }

    fn publisher(path: &str) -> PublisherId {
        PublisherId { node: path.into(), incarnation: IncarnationId::mint() }
    }

    fn op(d: &MeshDigest, operation: &str) -> LifecycleOp {
        LifecycleOp { build_id: "3qcfk1kq47m9".into(), attempt: 1, operation: format!("{operation}:{}", d.node.name), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1 }
    }

    /// CONTRACT (R-RX3: Members and MembersDelta order by `published_at_rafka_ms` then
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
        assert_eq!(order.admit(&members("mesh1", &p1, 100, 7, 0)), Ok(()));
        assert_eq!(order.admit(&members("mesh1", &p1, 100, 7, 1)), Ok(()), "the second chunk of the same publication is taken");
        order.installed("mesh1", &p1, 7, Some(100));
        assert_eq!(order.admit(&members("mesh1", &p1, 100, 7, 0)), Ok(()), "a chunk of the installed publication is taken: the handler absorbs it");
        assert_eq!(order.admit(&members("mesh1", &p2, 150, 1, 0)), Ok(()), "a later publication is taken whatever its version and publisher");
        order.installed("mesh1", &p2, 1, Some(150));
        assert_eq!(order.admit(&delta("mesh1", &p2, 160, 1, 2)), Ok(()));
        order.installed("mesh1", &p2, 2, Some(160));
        let ignored_full = order.admit(&members("mesh1", &p1, 120, 9, 0)).unwrap_err();
        assert_eq!(ignored_full, Earlier { frame: "members", source: "mesh1".into(), held: "published_at_rafka_ms=160 topology_version=2".into(), offered: "published_at_rafka_ms=120 topology_version=9".into() });
        let ignored_delta = order.admit(&delta("mesh1", &p1, 110, 7, 8)).unwrap_err();
        assert_eq!((ignored_delta.frame, ignored_delta.offered.as_str()), ("members-delta", "published_at_rafka_ms=110 topology_version=8"));
        assert_eq!(order.admit(&delta("mesh1", &p2, 160, 1, 3)), Ok(()), "the same stamp at a later version is newer");
        assert_eq!(order.admit(&members("mesh2", &p1, 1, 1, 0)), Ok(()), "another mesh is another source");
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
        assert_eq!(order.admit(&members("mesh1", &p, 200, 8, 0)), Ok(()), "chunk 0 of v8: its snapshot never completes");
        assert_eq!(order.admit(&members("mesh1", &p, 150, 7, 1)), Ok(()), "v7's chunk is taken: nothing installed is newer");
        order.installed("mesh1", &p, 7, Some(150));
        order.installed("mesh1", &p, 9, None);
        let held = format!("publisher={p} topology_version=9");
        assert_eq!(order.admit(&members("mesh1", &p, 240, 8, 0)), Err(Earlier { frame: "members", source: "mesh1".into(), held: held.clone(), offered: "published_at_rafka_ms=240 topology_version=8".into() }));
        assert_eq!(order.admit(&delta("mesh1", &p, 245, 7, 8)).map_err(|e| (e.frame, e.held)), Err(("members-delta", held.clone())));
        assert_eq!(order.admit_read("mesh1", &p, 8), Err(Earlier { frame: "topology-read", source: "mesh1".into(), held, offered: "topology_version=8".into() }));
        assert_eq!(order.admit_read("mesh1", &p, 9), Ok(()), "a read of the installed version is taken");
        assert_eq!(order.admit(&members("mesh1", &p, 250, 9, 0)), Ok(()), "the publisher's full at the installed version is taken");
        assert_eq!(order.admit(&members("mesh1", &other, 260, 1, 0)), Ok(()), "another publisher is not ordered against that revision");
        assert_eq!(order.admit_read("mesh2", &p, 1), Ok(()), "another mesh is another source");
    }

    /// CONTRACT (R-RX3: Digest orders by `digest_seq`, per birth; gossip.md §3.1: a new incarnation's
    /// `digest_seq` starts again at 1): a heartbeat below the held `digest_seq` of its exact birth is
    /// ignored, naming the birth and both sequences; a copy of the held one is taken (the book absorbs
    /// it); a new incarnation of the same node at sequence 1 is taken. Must NOT happen: a restarted
    /// node's heartbeats ignored against its previous birth's sequence.
    #[test]
    fn a_heartbeat_below_the_held_digest_seq_of_its_birth_is_ignored_and_a_new_incarnation_starts_again() {
        let book = crate::membership::DigestBook::default();
        let d = birth("mesh1.rpc.1");
        let at = |seq| MeshDigest { digest_seq: seq, ..d.clone() };
        assert!(book.record(at(7)));
        assert!(!book.record(at(7)), "the installed heartbeat classifier ignores its identical repeat");
        assert!(!book.record(at(3)), "a heartbeat below the installed exact birth's sequence is ignored");
        assert_eq!(book.get(d.node.node_id.as_str()).unwrap().0.digest_seq, 7);
        let restarted = MeshDigest { node: MeshNode { incarnation: IncarnationId::mint(), ..d.node.clone() }, digest_seq: 1, ..d.clone() };
        assert!(book.record(restarted), "a new incarnation is another source");
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
            order.admit(&StatusEvent::Restarting.frame(op(&b, "restart-node"), primary.clone(), 7)),
            Err(Earlier { frame: "node-restarting", source: format!("{primary} mesh1"), held: "topology_version=8".into(), offered: "topology_version=7".into() })
        );
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&b, "stop-node"), successor, 1)), Ok(()), "another publisher counts its own versions");
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&c, "stop-node"), acting.clone(), 9)), Ok(()));
        assert_eq!(order.admit(&StatusEvent::Stopped.frame(op(&a, "node.stop"), acting, 2)), Ok(()), "the same publisher's fact of another mesh is another source");
    }

    /// CONTRACT: updated is after updating at one revision, and every named cohort is checked
    /// atomically against the installed seat and retained publication. Duplicates run no handler.
    #[test]
    fn a_primary_updating_received_after_its_primary_updated_is_ignored() {
        use rdm_core::{Seat, SeatHolder};
        use rdm_core::seat::transfer::CommittedSeat;
        let book = crate::membership::SeatBook::default();
        let d = birth("mesh1.admin.1");
        let holder = SeatHolder { mesh: "mesh1".into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision: 4 };
        let row = |seat, revision| CommittedSeat::of(seat, &SeatHolder { revision, ..holder.clone() });
        let updating = |seats| Frame::PrimaryUpdating { op: op(&d, "node.stop"), seats };
        let updated = |seats| Frame::PrimaryUpdated { op: op(&d, "node.stop"), seats };
        assert!(book.receive(&updating(vec![row(Seat::MeshPrimary, 4)])));
        assert!(book.mesh("mesh1").is_none(), "updating moves no holder");
        assert!(book.receive(&updated(vec![row(Seat::MeshPrimary, 4)])));
        assert!(!book.receive(&updating(vec![row(Seat::MeshPrimary, 4)])));
        assert!(!book.receive(&updated(vec![row(Seat::MeshPrimary, 4)])));
        assert!(book.receive(&updating(vec![row(Seat::MeshPrimary, 5)])));
        assert!(!book.receive(&updated(vec![row(Seat::MeshPrimary, 3)])));
        assert!(!book.receive(&updated(vec![row(Seat::FabricPrimary, 7), row(Seat::MeshPrimary, 3)])), "one stale cohort refuses all");
        assert!(book.fabric().is_none());
        assert!(book.receive(&updated(vec![row(Seat::FabricPrimary, 1)])), "the refused row did not move this floor");
        assert!(book.receive(&updated(vec![CommittedSeat::of(Seat::MeshPrimary, &SeatHolder { mesh: "mesh2".into(), revision: 1, ..holder })])));
    }

    /// CONTRACT: each (mesh, node_type) cohort is independent, while the fabric seat has one floor
    /// across holder meshes. Equal content is ignored; a higher revision installs.
    #[test]
    fn a_seated_record_orders_within_its_complete_seat_key() {
        use rdm_core::{Seat, SeatHolder, NodeKind};
        let book = crate::membership::SeatBook::default();
        let d = birth("mesh1.rpc.1");
        let frame = |seat, mesh: &str, revision| Frame::Seated { seat, holder: SeatHolder { mesh: mesh.into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision } };
        assert!(book.receive(&frame(Seat::MeshPrimary, "mesh1", 5)));
        assert!(book.receive(&frame(Seat::Cohort(NodeKind::RpcNode), "mesh1", 2)));
        assert!(book.receive(&frame(Seat::Cohort(NodeKind::Broker), "mesh1", 1)));
        assert!(book.receive(&frame(Seat::Cohort(NodeKind::RpcNode), "mesh2", 1)));
        assert!(book.receive(&frame(Seat::FabricPrimary, "mesh1", 3)));
        assert!(!book.receive(&frame(Seat::Cohort(NodeKind::RpcNode), "mesh1", 2)));
        assert!(!book.receive(&frame(Seat::Cohort(NodeKind::RpcNode), "mesh1", 1)));
        assert!(!book.receive(&frame(Seat::FabricPrimary, "mesh2", 2)));
        assert!(book.receive(&frame(Seat::Cohort(NodeKind::RpcNode), "mesh1", 3)));
    }

    /// CONTRACT (R-RX3): a frame with no order key (a status, a NodeDeleted) is taken however often
    /// it arrives.
    #[test]
    fn a_frame_with_no_order_key_is_always_taken() {
        let mut order = ReceiveOrder::default();
        let d = birth("mesh1.rpc.1");
        let deleted = Frame::NodeDeleted { op: op(&d, "delete-node"), forwarded_by: None };
        let status = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 };
        for _ in 0..2 {
            assert_eq!(order.admit(&deleted), Ok(()));
            assert_eq!(order.admit(&status), Ok(()));
        }
    }
}
