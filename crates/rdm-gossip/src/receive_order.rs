//! Receive classification for gossip and topology reads. Decisions use the frame's
//! declared subject, identity and ordering fields, never the transport sender.
//!
//! MeshStatus and FabricStatus compare the envelope's composite written-primary revision and
//! change-time sequence through [`admit_status_context`] under the status view's lock. A delayed predecessor loses
//! by revision even with a later timestamp (R10). Within one held author/revision, [`admit_status`]
//! preserves change-time ordering and the existing named duplicate/inconsistency outcomes.
//! Forwarding metadata never changes status content; retained context survives a read's write.
//!
//! Digest uses [`admit_digest`] against the actual held heartbeat under the digest view's
//! lock, including the floor a read wrote. Lower is `via-stale`; equal wire content is
//! `via-duplicate`; equal sequence with different wire content is `via-inconsistent`.
//! The primary's separately held membership status is not heartbeat content. A refused or
//! repeated digest supplies no address and no clock observation to its caller.
//!
//! [`ReceiveOrder`] holds the remaining keyed families' ordering state, shared by all channels
//! and topology reads:
//! - TopologyFull and TopologyDelta: the mesh's version orders its topology, whichever mesh-primary
//!   sent it (Q389): the floor is the version the mesh's one copy in the topology view holds, written
//!   by any channel or a topology read, and a node's own publications are its floor for its own mesh
//!   ([`ReceiveOrder::published`]). AggregateReceiver, which holds that copy, compares complete
//!   normalized projection content, chunk identity and delta content under its hold: duplicates
//!   write nothing; conflicts are inconsistent.
//!   An aggregate's chunk set is its (mesh, version, author) from the envelope, and a publication is
//!   that set at one envelope `time`; equal-key unfinished chunks remain admissible. An ahead-base delta requests repair via-gap;
//!   the exact repeated publication never completes repair. A new publication at the same material
//!   version carries fresh hearing evidence. A directed topology read can resume the held version.
//! - NodeDeleting and NodeStopped: operation identity is recorded before the
//!   existing topology-version check per subject mesh, whichever mesh-primary published the fact
//!   (Q389, Q391: a mesh-primary taking the role continues the mesh's version). A carried revision
//!   refused with `via-stale` still leaves that identity seen; a repeat emits `via-duplicate` and runs
//!   no handler. The carried revision is still judged independently, even for a repeated operation:
//!   an admissible higher revision advances the mesh's floor without replaying the fact.
//!
//! Distinct equal-key chunks of an unfinished publication remain admissible; an exact repeat is
//! ignored without discarding its other chunks. Publication state advances only when a complete
//! full is written or a delta applies. [`ReceiveOrder::hold`] keeps the version the copy holds,
//! written by any channel or topology read; no refused frame moves it.
//!
//! Lifecycle overlays are opened through [`admit_overlay`] under the actual overlay lock for
//! local execution, received frames and aggregates. Retained completion is keyed by the complete
//! operation identity and exact birth; it cannot reopen. A held repeat never rewrites the authored
//! event evidence. NodeDeleting remains an overlay read only by application routing.
//!
//! NodeDeleted uses [`node_deleted_receipt`] against the retained deletion under its lock: an exact
//! operation/birth repeat emits `via-duplicate`, another deletion of the retained NodeId is refused by
//! the existing deleted-identity rule, and an unheld terminal fact applies regardless of unrelated
//! traffic. Event timestamps never order or expire that fact.
//!
//! The envelope refuses reserved types before domain decoding. [`refuse_reserved`] also protects
//! in-process domain calls for NodeRestarting (position 7, Q183), Concern (position 10, Q23), NodeConnectionsDeleting (11),
//! NodeLeaving (12), NodeLeft (13), NodeConnectionsDeleted (14), and NewFabricPrimary (15).
//! Receipt emits `via-reserved` before any identity view, apply span or carrier runs. No reserved
//! body writes a projection, records a fact, registers a contact or commands investigation.

use crate::frame::Frame;
use std::collections::{HashMap, HashSet};

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
/// This also serves statuses a read wrote, so classification and the write share the
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

/// Name a publication's classification at installation. The caller holds the actual
/// source projection lock; forwarding metadata never supplies its identity or ordering.
pub(crate) fn publication_duplicate(frame: &'static str, identity: &str) {
    tracing::info_span!("rdm.mesh.topology.update.via-duplicate", frame, identity, outcome = "ignored")
        .in_scope(|| tracing::info!("the publication identity and content are already held"));
}

pub(crate) fn publication_inconsistent(frame: &'static str, mesh: &str, publisher: &impl std::fmt::Display, revision: u64) {
    tracing::info_span!("rdm.mesh.topology.reject.via-inconsistent", frame, subject = %format!("{mesh} {publisher}"), revision)
        .in_scope(|| tracing::info!("equal publication revision or chunk identity with different content: refused"));
}

pub(crate) fn publication_stale(frame: &'static str, mesh: &str, publisher: &impl std::fmt::Display, held: u64, offered: u64) {
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
            operation_subject = ?offered.operation.subject,
            build_id = %offered.build_id,
            operation = %offered.operation.action, operation_step = ?offered.operation.step,
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
    operation: rdm_core::OperationId,
    node_id: rdm_core::NodeId,
    incarnation: rdm_core::IncarnationId,
}

impl LifecycleIdentity {
    pub(crate) fn of(op: &rdm_core::LifecycleOp) -> Self {
        Self { build_id: op.build_id.clone(), operation: op.operation.clone(), node_id: op.node_id.clone(), incarnation: op.incarnation.clone() }
    }
}

fn duplicate_lifecycle(frame: &str, op: &rdm_core::LifecycleOp) {
    tracing::info_span!(
        "rdm.mesh.topology.update.via-duplicate",
        frame,
        build_id = %op.build_id, operation = %op.operation.action, operation_subject = ?op.operation.subject, operation_step = ?op.operation.step, node_id = %op.node_id, incarnation_id = %op.incarnation,
        outcome = "ignored",
    ).in_scope(|| tracing::info!("the lifecycle operation identity is already held or seen: ignored"));
}

/// Classify an overlay against the actual held operation and retained exact-birth completion.
/// The caller holds the overlay installation lock through this decision and its insertion. Local
/// execution and aggregate application use this same classifier as the frame handler.
pub(crate) fn admit_overlay(frame: &str, op: &rdm_core::LifecycleOp, held: Option<&rdm_core::LifecycleOp>, ended: bool) -> bool {
    if ended {
        tracing::info_span!(
            "rdm.mesh.topology.update.via-lifecycle-reconciled",
            frame, build_id = %op.build_id, operation = %op.operation.action, operation_subject = ?op.operation.subject,
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

/// The source a frame's order key orders within.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Source {
    /// One mesh's lifecycle facts, whichever of its mesh-primaries published them.
    Lifecycle(String),
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Lifecycle(mesh) => f.write_str(mesh),
        }
    }
}

/// A frame's order key. One source only ever holds one kind of key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    TopologyVersion(u64),
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

/// The frame's name in spans, its source and its order key; `None` for a frame with no order key or a
/// publication ([`publication_of`]).
fn order_of(f: &Frame) -> Option<(&'static str, Source, Key)> {
    match f.data() {
        Frame::NodeDeleting { op, topology_version, .. } => Some(("node-deleting", Source::Lifecycle(op.mesh.clone()), Key::TopologyVersion(*topology_version))),
        Frame::NodeStopped { op, topology_version, .. } => Some(("node-stopped", Source::Lifecycle(op.mesh.clone()), Key::TopologyVersion(*topology_version))),
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

/// The newest order key one node holds for each source, and the version of each mesh its copy in the
/// topology view holds: one for the node, shared by every channel it hears and by its topology reads.
/// It keeps versions and order keys only, never a copy of a mesh: what a frame's content is compared
/// against is the one copy in the topology view ([`crate::aggregate::AggregateReceiver`], Q409).
#[derive(Debug, Default)]
pub(crate) struct ReceiveOrder {
    newest: HashMap<Source, Key>,
    /// Operation receipt is independent of projection admission: even a version-refused frame is
    /// seen, so another publisher or a higher carried version cannot replay its operation effect.
    lifecycle_seen: HashSet<LifecycleIdentity>,
    /// The version of each mesh the copy in the topology view holds, and the mesh's id as its
    /// topology named it.
    held: HashMap<String, HeldVersion>,
    /// The version this node published last of each mesh it publishes, under its exact birth: its
    /// own publications are its floor for its own source, a read included.
    published: HashMap<String, u64>,
}

#[derive(Debug)]
struct HeldVersion {
    version: u64,
    mesh_id: Option<rdm_core::MeshId>,
}

/// The frame's name in spans, the mesh a `TopologyFull` or `TopologyDelta` is of, its version (an
/// aggregate's or a differential's is its envelope's `sequence`, Q389), and the mesh's id as the frame
/// names it beside the mesh's name.
fn publication_of(f: &Frame) -> Option<(&'static str, &str, u64, Option<rdm_core::MeshId>)> {
    match f.data() {
        Frame::TopologyFull { mesh, mesh_id, .. } => Some(("mesh-topology", mesh, f.aggregate_version()?, mesh_id.clone())),
        Frame::TopologyDelta { mesh, mesh_id, .. } => Some(("mesh-topology-delta", mesh, f.delta_version().unwrap_or_default(), mesh_id.clone())),
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
    /// key is held when it is newer than the held one. A `TopologyFull` or `TopologyDelta` is ordered by
    /// the version its mesh's copy holds alone, which moves when a full or a read writes it or a delta
    /// applies ([`Self::hold`]), whichever mesh-primary sent it (Q389).
    pub(crate) fn admit(&mut self, f: &Frame) -> Result<(), Earlier> {
        if let Some((frame, mesh, version, mesh_id)) = publication_of(f) {
            return self.below_held(frame, mesh, version, mesh_id.as_ref(), || format!("topology_version={version}"));
        }
        let (frame, keys) = orders_of(f);
        // Every key is judged before any is held: a frame ignored for one of its sources moves none.
        for (source, key) in &keys {
            if let Some(held) = self.newest.get(source).filter(|held| key < *held) {
                return Err(Earlier { frame, source: source.to_string(), held: held.to_string(), offered: key.to_string() });
            }
        }
        for (source, key) in keys {
            if self.newest.get(&source).is_none_or(|held| key > *held) {
                self.newest.insert(source, key);
            }
        }
        Ok(())
    }

    /// Whether a chunk of a topology read of `mesh` (whose node-admins' entries carry `mesh_id`) at
    /// `topology_version` is taken: only the version held of the mesh orders it.
    pub(crate) fn admit_read(&self, mesh: &str, topology_version: u64, mesh_id: Option<&rdm_core::MeshId>) -> Result<(), Earlier> {
        self.below_held("topology-read", mesh, topology_version, mesh_id, || format!("topology_version={topology_version}"))
    }

    /// `Err` when the floor of `mesh` ([`Self::floor`]) is above `topology_version`.
    fn below_held(&self, frame: &'static str, mesh: &str, topology_version: u64, mesh_id: Option<&rdm_core::MeshId>, offered: impl FnOnce() -> String) -> Result<(), Earlier> {
        match self.floor(mesh, mesh_id) {
            Some(held) if topology_version < held => Err(Earlier { frame, source: mesh.to_string(), held: format!("topology_version={held}"), offered: offered() }),
            _ => Ok(()),
        }
    }

    /// The version of `mesh` that orders its publications and reads here, whichever mesh-primary sent
    /// them (Q389): the higher of the version its copy holds and the version this node published
    /// itself. `None` when neither is held. A later mesh of the same name (`mesh_id` another id than
    /// the held one's) is another mesh: the held version orders nothing of it.
    pub(crate) fn floor(&self, mesh: &str, mesh_id: Option<&rdm_core::MeshId>) -> Option<u64> {
        let held = self.held.get(mesh).filter(|h| !crate::aggregate::another_mesh(h.mesh_id.as_ref(), mesh_id)).map(|h| h.version);
        let published = self.published.get(mesh).copied();
        held.max(published)
    }

    /// This node published `mesh` at `topology_version`: from now on its floor for that mesh (gossip.md
    /// receive rules: "against the held floor, including a topology read"). The caller holds the
    /// publisher's version lock, so the floor moves with the version it assigns.
    pub(crate) fn published(&mut self, mesh: &str, topology_version: u64) {
        let v = self.published.entry(mesh.to_string()).or_insert(topology_version);
        *v = (*v).max(topology_version);
    }

    /// `mesh`'s copy in the topology view moved to `topology_version`, of the mesh whose id is
    /// `mesh_id`: a full or a topology read wrote its delta, or a differential applied. Called under
    /// the receiver's hold that wrote it, so the floor moves with the copy.
    pub(crate) fn hold(&mut self, mesh: &str, topology_version: u64, mesh_id: Option<rdm_core::MeshId>) {
        self.held.insert(mesh.to_string(), HeldVersion { version: topology_version, mesh_id });
    }
}

#[cfg(test)]
mod tests {

    /// The source's descriptive row name cannot reset the exact birth's held floor.
    #[test]
    fn a_renamed_publisher_keeps_its_held_version_floor() {
        let publisher = PublisherId { node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
        let mut order = ReceiveOrder::default();
        order.hold("mesh1", 10, None);
        let renamed = PublisherId { ..publisher };
        let old = aggregate("mesh1", None, &renamed, 999, 9, 0, 1, vec![]);
        assert!(order.admit(&old).is_err());
        let operation = rdm_core::LifecycleOp { r#type: "node".into(), mesh: "mesh1".into(), build_id: "b".into(), operation: rdm_core::OperationId::node("node.stop", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())), node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint(), path: "mesh1.node".into(), name: "1".into(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        assert!(order.admit(&StatusEvent::Stopped.frame(operation.clone(), 20)).is_ok());
        assert!(order.admit(&StatusEvent::Stopped.frame(operation, 19)).is_err(), "a carried projection floor belongs to the mesh, not a label");
    }

    /// CONTRACT: every topology Frame variant has exactly one receive-table row naming its
    /// category, subject, ordering and identity; a new variant without a rule fails by name.
    #[test]
    fn every_frame_kind_has_a_row_in_the_receive_rules() {
        let source = include_str!("frame.rs");
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
    use crate::status::StatusEvent;
    use crate::aggregate::PublisherId;
    use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshNode, NodeId};

    fn birth(path: rdm_core::PathName, mesh: &str, r#type: &str) -> MeshDigest {
        MeshDigest { 
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(), node_id: NodeId::mint(), path: path.path.clone(), name: path.name.clone(), endpoint_id: EndpointId(NodeId::mint().to_string()), transport_addr: "127.0.0.1:1".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, },
            status: NodeStatus::Ready,
            digest_seq: 1,
            emitted_at_rafka_ms: 1,
            primary: Vec::new(),
        }
    }

    /// One chunk of `mesh`'s aggregate, authored by `author` at the mesh's `version` and `time_ms`.
    #[allow(clippy::too_many_arguments)]
    fn aggregate(mesh: &str, mesh_id: Option<rdm_core::MeshId>, author: &PublisherId, time_ms: u64, version: u64, chunk_index: u32, chunk_count: u32, nodes: Vec<MeshDigest>) -> Frame {
        let birth = crate::envelope::Birth::publisher(&FabricId::parse("fab000000001").unwrap(), author);
        Frame::TopologyFull { mesh: mesh.into(), mesh_id, chunk_index, chunk_count, nodes: nodes.iter().map(rdm_core::wire::NodeEntry::of).collect() }.authored_aggregate(&birth, version, time_ms)
    }

    fn mesh_topology(mesh: &str, publisher: &PublisherId, published_at_rafka_ms: u64, topology_version: u64, chunk_index: u32) -> Frame {
        aggregate(mesh, None, publisher, published_at_rafka_ms, topology_version, chunk_index, 2, vec![])
    }

    /// The birth `p` names, of the test fabric.
    fn birth_of(p: &PublisherId) -> crate::envelope::Birth {
        crate::envelope::Birth { fabric: FabricId::parse("fab000000001").unwrap(), node: p.node_id.clone(), incarnation: p.incarnation.clone() }
    }

    /// An empty differential of `mesh` from `base_version` to `topology_version`, authored by the
    /// forwarder `by` at `at`.
    fn delta(mesh: &str, by: &PublisherId, at: u64, base_version: u64, topology_version: u64) -> Frame {
        Frame::TopologyDelta { mesh: mesh.into(), mesh_id: None, base_version, chunk_index: 0, chunk_count: 1, added: vec![], changed: vec![], removed: vec![] }.authored_delta(&birth_of(by), topology_version, at)
    }

    /// `kind`'s frame of `op` at `topology_version`, published by the mesh-primary `by`.
    fn event(kind: StatusEvent, op: LifecycleOp, by: &PublisherId, topology_version: u64) -> Frame {
        kind.frame(op, topology_version).authored(&birth_of(by), 0, 1)
    }

    fn publisher(path: &str) -> PublisherId {
        PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }
    }

    fn op(d: &MeshDigest, action: &str) -> LifecycleOp {
        LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(), build_id: "3qcfk1kq47m9".into(), operation: rdm_core::OperationId::new(action, rdm_core::OperationSubject::Birth { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), path: d.node.path.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
    }

    /// CONTRACT (Q389: the mesh's version orders its topology, whichever mesh-primary sent it;
    /// gossip.md §3.1: a backbone full's delta and a delta move the held version): both chunks of one
    /// publication are taken, and a chunk of the written publication reaches its content classifier
    /// again (the classifier ignores its identical repeat); a successor's next version is taken whatever
    /// its publication stamp; then the previous mesh-primary's full and delta below the held version are
    /// ignored however late their stamp, each naming the mesh and both versions, while a version above
    /// the held one is taken whoever sent it, and another mesh's publication of any version is taken.
    /// Must NOT happen: a stamp ordering a mesh's topology; a lower version of another mesh-primary
    /// taken over the held one; a publication ordered against another mesh's.
    #[test]
    fn a_mesh_topology_below_the_held_version_of_its_mesh_is_ignored_whichever_mesh_primary_sent_it() {
        let mut order = ReceiveOrder::default();
        let (p1, p2) = (publisher("mesh1.node-admin.1"), publisher("mesh1.node-admin.2"));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p1, 100, 7, 0)), Ok(()));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p1, 100, 7, 1)), Ok(()), "the second chunk of the same publication is taken");
        order.hold("mesh1", 7, None);
        assert_eq!(order.admit(&mesh_topology("mesh1", &p1, 100, 7, 0)), Ok(()), "a chunk of the written publication is taken: the handler absorbs it");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p2, 50, 8, 0)), Ok(()), "the successor's next version is taken, whatever its stamp");
        order.hold("mesh1", 8, None);
        assert_eq!(order.admit(&delta("mesh1", &p2, 60, 8, 9)), Ok(()));
        order.hold("mesh1", 9, None);
        let ignored_full = order.admit(&mesh_topology("mesh1", &p1, 500, 8, 0)).unwrap_err();
        assert_eq!(ignored_full, Earlier { frame: "mesh-topology", source: "mesh1".into(), held: "topology_version=9".into(), offered: "topology_version=8".into() }, "the previous mesh-primary's lower version is older, however late its stamp");
        let ignored_delta = order.admit(&delta("mesh1", &p1, 510, 7, 8)).unwrap_err();
        assert_eq!((ignored_delta.frame, ignored_delta.offered.as_str()), ("mesh-topology-delta", "topology_version=8"));
        assert_eq!(order.admit(&delta("mesh1", &p1, 1, 9, 10)), Ok(()), "a version above the held one is taken, whoever sent it and whatever its stamp");
        assert_eq!(order.admit(&mesh_topology("mesh2", &p1, 1, 1, 0)), Ok(()), "another mesh is another source");
    }

    /// CONTRACT (Luke 2026-10-13: "Snapshot state advances atomically once every chunk has arrived";
    /// gossip.md §3.1: "a topology read's delta move[s] the held source version"; Q389): a mesh's
    /// version moves only when a full is written, so a chunk of a newer publication that never
    /// completed holds nothing and an older publication's chunks are still taken. The version a
    /// topology read wrote orders what follows: a full, delta or read chunk at a lower version is
    /// ignored, naming the version held, whichever mesh-primary sent it, and a full at the held version
    /// is taken. Must NOT happen: a partial full ignoring the publication still assembling; a version
    /// below the one a read wrote reaching a handler.
    #[test]
    fn a_mesh_version_moves_when_its_full_is_written_and_a_read_written_version_orders_lower_versions() {
        let mut order = ReceiveOrder::default();
        let (p, other) = (publisher("mesh1.node-admin.1"), publisher("mesh1.node-admin.2"));
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 200, 8, 0)), Ok(()), "chunk 0 of v8: its full never completes");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 150, 7, 1)), Ok(()), "v7's chunk is taken: nothing held is newer");
        order.hold("mesh1", 7, None);
        order.hold("mesh1", 9, None);
        let held = "topology_version=9".to_string();
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 240, 8, 0)), Err(Earlier { frame: "mesh-topology", source: "mesh1".into(), held: held.clone(), offered: "topology_version=8".into() }));
        assert_eq!(order.admit(&delta("mesh1", &p, 245, 7, 8)).map_err(|e| (e.frame, e.held)), Err(("mesh-topology-delta", held.clone())));
        assert_eq!(order.admit_read("mesh1", 8, None), Err(Earlier { frame: "topology-read", source: "mesh1".into(), held: held.clone(), offered: "topology_version=8".into() }));
        assert_eq!(order.admit_read("mesh1", 9, None), Ok(()), "a read of the held version is taken");
        assert_eq!(order.admit(&mesh_topology("mesh1", &p, 250, 9, 0)), Ok(()), "a full at the held version is taken");
        assert_eq!(order.admit(&mesh_topology("mesh1", &other, 260, 1, 0)).map_err(|e| e.held), Err(held), "another mesh-primary's lower version is older too (Q389)");
        assert_eq!(order.admit_read("mesh2", 1, None), Ok(()), "another mesh is another source");
    }

    /// CONTRACT (gossip.md receive rules, `TopologyFull`: "`P(topology_version)` against the mesh's
    /// held floor, ..., including a topology read"; #3211 red 2; Q389): what this node published of its
    /// own mesh is its floor for that mesh, so a read or full below it is refused naming the version
    /// published, whoever sent it, and at it is taken. The held floor still orders when it is the
    /// higher. Must NOT happen: a primary at its published v28 writing a read of its own v25.
    #[test]
    fn a_read_of_its_own_mesh_below_the_version_it_published_is_refused() {
        let mut order = ReceiveOrder::default();
        let me = publisher("mesh2.node-admin.2");
        order.published("mesh2", 27);
        order.published("mesh2", 28);
        order.published("mesh2", 26);
        let held = "topology_version=28".to_string();
        assert_eq!(order.admit_read("mesh2", 25, None), Err(Earlier { frame: "topology-read", source: "mesh2".into(), held: held.clone(), offered: "topology_version=25".into() }));
        assert_eq!(order.floor("mesh2", None), Some(28), "the floor never moves down");
        assert_eq!(order.admit(&mesh_topology("mesh2", &me, 1_000, 25, 0)).map_err(|e| e.held), Err(held), "a full below what it published is refused too");
        assert_eq!(order.admit_read("mesh2", 28, None), Ok(()), "a read of the version it published is not refused for its order");
        assert_eq!(order.admit_read("mesh1", 1, None), Ok(()), "another mesh is another source");
        order.hold("mesh2", 30, None);
        assert_eq!(order.floor("mesh2", None), Some(30), "the held floor orders when it is the higher");
        assert!(order.admit_read("mesh2", 29, None).is_err());
    }

    /// CONTRACT (gossip.md "Mesh delete": a mesh is created once; a later mesh of the same name is
    /// another mesh, known by the id its topology frames name beside its name): a node holding the deleted mesh2
    /// at v33 takes a recreated mesh2's aggregate at v1, a read of it and its first differential, at the
    /// receive path, in the receiver and in the forwarder, while the deleted mesh's own lower versions
    /// stay ignored. Must NOT happen: a recreated mesh's every publication ignored below the deleted
    /// mesh's version (canonical_ids red at b6f75c432: 67 via-stale at the fabric-primary, held 33).
    #[test]
    fn a_later_mesh_of_the_same_name_is_another_mesh_which_the_held_version_orders_nothing_of() {
        let admin_of = || birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin");
        let (old_id, new_id) = (rdm_core::MeshId::mint(), rdm_core::MeshId::mint());
        let (old_admin, new_admin) = (admin_of(), admin_of());
        let (old, new) = (publisher("mesh2.node-admin.1"), publisher("mesh2.node-admin.1"));
        // The mesh's id rides each topology frame beside its name: old_id on the deleted mesh's
        // publications, new_id on the recreated one's.
        let id_of = |p: &PublisherId| if p == &old { old_id.clone() } else { new_id.clone() };
        let full_of = |p: &PublisherId, version: u64, d: &MeshDigest| aggregate("mesh2", Some(id_of(p)), p, version, version, 0, 1, vec![d.clone()]);

        // The receive path.
        let mut order = ReceiveOrder::default();
        order.hold("mesh2", 33, Some(old_id.clone()));
        assert!(order.admit(&full_of(&old, 32, &old_admin)).is_err(), "the deleted mesh's own lower version is still older");
        assert_eq!(order.admit(&full_of(&new, 1, &new_admin)), Ok(()), "the recreated mesh2's aggregate is another mesh's");
        assert_eq!(order.admit_read("mesh2", 1, Some(&new_id)), Ok(()));
        assert!(order.admit_read("mesh2", 1, Some(&old_id)).is_err());

        // The receiver.
        let mut rx = crate::aggregate::AggregateReceiver::default();
        let chunk_of = |p: &PublisherId, version: u64, d: &MeshDigest| crate::aggregate::Chunk { mesh: "mesh2".into(), mesh_id: Some(id_of(p)), author: Some(crate::envelope::Birth::publisher(&FabricId::parse("fab000000001").unwrap(), p)), topology_version: version, time_ms: version, chunk_index: 0, chunk_count: 1, digests: vec![d.clone()], runtimes: Vec::new() };
        let crate::aggregate::Taken::Written(old_written) = rx.take_chunk(chunk_of(&old, 33, &old_admin)) else { panic!("the deleted mesh's full is written") };
        let taken = rx.take_chunk(chunk_of(&new, 1, &new_admin));
        assert!(matches!(&taken, crate::aggregate::Taken::Written(w) if w.first && w.base_version == 0 && w.topology_version == 1), "the recreated mesh's first full adds it to the topology view: {taken:?}");
        let crate::aggregate::Taken::Written(new_written) = taken else { unreachable!() };
        assert_eq!(rx.held_full("mesh2").map(|f| f.member_count()), Some(1), "nothing of the deleted mesh is kept in the copy");
        let mut members = crate::aggregate::AggregateReceiver::default();
        assert!(matches!(members.take_delta("mesh2", &birth_of(&old), 0, 33, &crate::aggregate::Delta { added: vec![old_admin.clone()], mesh_id: Some(old_id.clone()), ..Default::default() }), crate::aggregate::Moved::Applied { .. }));
        let first = members.take_delta("mesh2", &birth_of(&new), 0, 1, &crate::aggregate::Delta { added: vec![new_admin.clone()], mesh_id: Some(new_id.clone()), ..Default::default() });
        assert!(matches!(first, crate::aggregate::Moved::Applied { topology_version: 1, .. }), "the recreated mesh's first differential starts from nothing: {first:?}");
        assert_eq!(members.held_full("mesh2").map(|f| f.member_count()), Some(1), "nothing of the deleted mesh is kept");

        // The forwarder: what the copy's two writes put into mesh1.
        let mut fwd = crate::aggregate::Forwarder::default();
        fwd.set_author(crate::envelope::Birth { fabric: rdm_core::FabricId::parse("fab000000001").unwrap(), node: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() });
        assert!(matches!(fwd.source("mesh1.node-admin.1", &old_written, 1), crate::aggregate::Forward::Delta(_)));
        let crate::aggregate::Forward::Delta(frames) = fwd.source("mesh1.node-admin.1", &new_written, 2) else { panic!("the recreated mesh goes in") };
        assert!(matches!(frames[0].data(), Frame::TopologyDelta { base_version: 0, .. }) && frames[0].delta_version() == Some(1), "from nothing: {:?}", frames[0].data());
    }

    /// CONTRACT (R-RX3: Digest orders by `digest_seq`, per birth; gossip.md §3.1: a new incarnation's
    /// `digest_seq` starts again at 1): a heartbeat below the held `digest_seq` of its exact birth is
    /// ignored, naming the birth and both sequences; a copy of the held one is taken (the view absorbs
    /// it); a new incarnation of the same node at sequence 1 is taken. Must NOT happen: a restarted
    /// node's heartbeats ignored against its previous birth's sequence.
    #[test]
    fn a_heartbeat_below_the_held_digest_seq_of_its_birth_is_ignored_and_a_new_incarnation_starts_again() {
        let digest_view = crate::digest_view::DigestView::default();
        let d = birth(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        let at = |seq| MeshDigest { digest_seq: seq, ..d.clone() };
        assert!(digest_view.record(at(7)));
        assert!(!digest_view.record(at(7)), "the installed heartbeat classifier ignores its identical repeat");
        assert!(!digest_view.record(at(3)), "a heartbeat below the installed exact birth's sequence is ignored");
        assert_eq!(digest_view.get(d.node.node_id.as_str()).unwrap().0.digest_seq, 7);
        let restarted = MeshDigest { node: MeshNode { incarnation: IncarnationId::mint(), ..d.node.clone() }, digest_seq: 1, ..d.clone() };
        assert!(digest_view.record(restarted), "a new incarnation is another source");
    }

    /// CONTRACT (R-RX3: lifecycle facts order by `topology_version`; Q389, Q391: the version is the
    /// mesh's, and a mesh-primary taking the role continues it, so the source is the subject's mesh,
    /// whichever of its mesh-primaries published the fact): a fact below the held version of its mesh is
    /// ignored, naming the mesh and both versions; two facts at one version (different subjects) are both
    /// taken; a successor mesh-primary's fact below the mesh's held version is ignored too, and one above
    /// it is taken; a fact of another mesh is another source. Must NOT happen: a lifecycle fact ordered
    /// per mesh-primary, so a predecessor's late fact passes its successor's newer one.
    #[test]
    fn a_lifecycle_fact_below_the_held_version_of_its_mesh_is_ignored_whichever_mesh_primary_published_it() {
        let mut order = ReceiveOrder::default();
        let (primary, successor, other_mesh) = (publisher("mesh1.node-admin.1"), publisher("mesh1.node-admin.2"), publisher("mesh3.node-admin.1"));
        let (a, b, c) = (birth(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), birth(rdm_core::PathName::new("mesh3.node", "1").unwrap(), "mesh3", "node"));
        assert_eq!(order.admit(&event(StatusEvent::Deleting, op(&a, "node.delete"), &primary, 8)), Ok(()));
        assert_eq!(order.admit(&event(StatusEvent::Stopped, op(&b, "node.stop"), &primary, 8)), Ok(()), "a second fact at the held version is taken");
        assert_eq!(
            order.admit(&event(StatusEvent::Deleting, op(&b, "node.delete"), &primary, 7)),
            Err(Earlier { frame: "node-deleting", source: "mesh1".into(), held: "topology_version=8".into(), offered: "topology_version=7".into() })
        );
        assert_eq!(
            order.admit(&event(StatusEvent::Stopped, op(&b, "node.stop"), &successor, 1)),
            Err(Earlier { frame: "node-stopped", source: "mesh1".into(), held: "topology_version=8".into(), offered: "topology_version=1".into() }),
            "the successor continues the mesh's version: its fact below the held one is older"
        );
        assert_eq!(order.admit(&event(StatusEvent::Stopped, op(&a, "node.stop"), &successor, 9)), Ok(()), "the successor's fact above the held version is taken");
        assert_eq!(order.admit(&event(StatusEvent::Stopped, op(&c, "node.stop"), &other_mesh, 2)), Ok(()), "a fact of another mesh is another source");
        assert_eq!(order.admit(&event(StatusEvent::Deleting, op(&c, "node.delete"), &other_mesh, 1)).map_err(|e| e.source), Err("mesh3".to_string()));
    }

    /// CONTRACT (R-RX3): a frame with no order key (a status, a NodeDeleted) is taken however often
    /// it arrives.
    #[test]
    fn a_frame_with_no_order_key_is_always_taken() {
        let mut order = ReceiveOrder::default();
        let d = birth(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        let deleted = Frame::NodeDeleted { op: op(&d, "node.delete"), forwarded_by: None };
        let status = Frame::MeshStatus { mesh: "mesh1".into(), mesh_id: rdm_core::fixture_mesh_id("mesh1"), status: "ready".into(), publisher: "mesh1.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 };
        for _ in 0..2 {
            assert_eq!(order.admit(&deleted), Ok(()));
            assert_eq!(order.admit(&status), Ok(()));
        }
    }
}
