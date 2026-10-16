//! Receive classification for gossip and topology-read installs. Decisions use the frame's
//! declared subject, identity and ordering fields, never the transport sender.
//!
//! MeshStatus uses [`admit_mesh_status`] against the actual held projection under the status
//! book's installation lock. An older timestamp emits `via-stale` with both keys. Equal declared
//! publisher/timestamp and identical status emits `via-duplicate` with outcome `ignored`;
//! different status content emits `via-inconsistent`. Forwarding metadata is not status content.
//! No publisher revision is inferred: the MeshStatus/FabricStatus publisher-revision gap (R10)
//! remains open, including the existing timestamp comparison across publishers.
//!
//! [`ReceiveOrder`] holds the existing keyed families' ordering state, shared by all channels
//! and topology-read installs:
//! - Digest: `digest_seq` per exact birth; another incarnation starts its sequence again.
//! - Members and MembersDelta: publication timestamp then topology version per mesh, with the
//!   installed publisher/version floor also enforced regardless of publication timestamp.
//! - NodeDeleting, NodeRestarting and NodeStopped: the existing topology-version check per
//!   publisher and subject mesh; operation-fact conformance is a remaining row.
//! - Seated: holder revision per complete seat key (mesh and node type, or the fabric seat).
//! - PrimaryUpdating/PrimaryUpdated: committed revision per cohort, settled after updating at
//!   one revision; every named cohort is checked before any held key moves.
//!
//! Equal keys in those keyed families remain admissible to their handlers, including distinct
//! chunks of an unfinished publication. A Members or MembersDelta key advances only when the
//! complete snapshot installs or the delta applies; [`ReceiveOrder::installed`] also records
//! the source version installed by a topology read, whose chunks carry no publication timestamp.
//!
//! FabricStatus, the operation facts without keys, Concern and reserved variants retain their
//! existing handler data rules pending their receive-conformance rows.

use crate::membership::Frame;
use crate::snapshot::PublisherId;
use std::collections::HashMap;

/// Admit a mesh status against the projection held under the status book's lock. The declared
/// mesh, publisher and change instant identify the revision; forwarding metadata is not content.
/// This also serves statuses installed by a read, so classification and installation share the
/// same held fact instead of maintaining a separate receive history.
pub(crate) fn admit_mesh_status(mesh: &str, held: Option<&crate::membership::MeshStatus>, status: &str, publisher: &str, revision: u64) -> bool {
    let Some(held) = held else { return true };
    if revision < held.changed_at_rafka_ms {
        tracing::info_span!(
            "rdm.mesh.membership.reject.via-stale",
            frame = "mesh-status",
            subject = %format!("{mesh} {publisher}"),
            held_key = %format!("changed_at_rafka_ms={}", held.changed_at_rafka_ms),
            frame_key = %format!("changed_at_rafka_ms={revision}"),
        ).in_scope(|| tracing::info!("a mesh status older than the held revision is refused"));
        return false;
    }
    if publisher == held.publisher && revision == held.changed_at_rafka_ms {
        if status != held.status {
            tracing::info_span!(
                "rdm.mesh.membership.reject.via-inconsistent",
                frame = "mesh-status",
                subject = %format!("{mesh} {publisher}"),
                revision,
                held_status = %held.status,
                frame_status = status,
            ).in_scope(|| tracing::info!("a mesh status at the held revision has different content: refused"));
        } else {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-duplicate",
                frame = "mesh-status",
                identity = %format!("{mesh} {publisher} changed_at_rafka_ms={revision}"),
                outcome = "ignored",
            ).in_scope(|| tracing::info!("an identical mesh status fact is ignored"));
        }
        return false;
    }
    true
}

/// The source a frame's order key orders within.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Source {
    /// One exact birth's heartbeats.
    Birth { node_id: String, incarnation: String },
    /// One mesh's publications.
    Mesh(String),
    /// One publisher's lifecycle facts of one mesh.
    Lifecycle { publisher: String, mesh: String },
    /// One complete seat key: a `(mesh, node_type)` cohort (`mesh-primary` for `node_admin`), or the
    /// fabric seat (`mesh` empty).
    Cohort { seat: &'static str, mesh: String },
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Birth { node_id, incarnation } => write!(f, "{node_id}/{incarnation}"),
            Source::Mesh(mesh) => f.write_str(mesh),
            Source::Lifecycle { publisher, mesh } => write!(f, "{publisher} {mesh}"),
            Source::Cohort { seat, mesh } if mesh.is_empty() => f.write_str(seat),
            Source::Cohort { seat, mesh } => write!(f, "{seat} {mesh}"),
        }
    }
}

/// How a seat key's revision arrived: "updating" precedes a settled record ("updated" or `Seated`) at
/// the same revision.
#[derive(Debug, Clone, Copy)]
enum SeatStage {
    Updating,
    Updated,
    Seated,
}

impl SeatStage {
    fn settled(self) -> bool {
        !matches!(self, Self::Updating)
    }
}

impl PartialEq for SeatStage {
    fn eq(&self, other: &Self) -> bool {
        self.settled() == other.settled()
    }
}

impl Eq for SeatStage {}

impl PartialOrd for SeatStage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SeatStage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.settled().cmp(&other.settled())
    }
}

/// A frame's order key. One source only ever holds one kind of key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    DigestSeq(u64),
    Publication { published_at_rafka_ms: u64, topology_version: u64 },
    TopologyVersion(u64),
    /// A seat's revision; at one revision a settled record ("updated" or `Seated`) is after "updating".
    Seat { revision: u64, stage: SeatStage },
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::DigestSeq(seq) => write!(f, "digest_seq={seq}"),
            Key::Publication { published_at_rafka_ms, topology_version } => write!(f, "published_at_rafka_ms={published_at_rafka_ms} topology_version={topology_version}"),
            Key::TopologyVersion(v) => write!(f, "topology_version={v}"),
            Key::Seat { revision, stage: SeatStage::Updating } => write!(f, "revision={revision} updating"),
            Key::Seat { revision, stage: SeatStage::Updated } => write!(f, "revision={revision} updated"),
            Key::Seat { revision, stage: SeatStage::Seated } => write!(f, "revision={revision}"),
        }
    }
}

/// The frame's name in spans and each source with its order key; empty for a frame with no order key.
fn orders_of(f: &Frame) -> (&'static str, Vec<(Source, Key)>) {
    let seats = |name: &'static str, seats: &[rdm_core::seat::transfer::CommittedSeat], stage: SeatStage| {
        (name, seats.iter().map(|c| (Source::Cohort { seat: c.seat.name(), mesh: c.mesh_key().unwrap_or_default().to_string() }, Key::Seat { revision: c.revision, stage })).collect())
    };
    match f {
        Frame::PrimaryUpdating { seats: s, .. } => seats("primary-updating", s, SeatStage::Updating),
        Frame::PrimaryUpdated { seats: s, .. } => seats("primary-updated", s, SeatStage::Updated),
        other => match order_of(other) {
            Some((name, source, key)) => (name, vec![(source, key)]),
            None => ("", Vec::new()),
        },
    }
}

/// The frame's name in spans, its source and its order key; `None` for a frame with no order key.
fn order_of(f: &Frame) -> Option<(&'static str, Source, Key)> {
    match f {
        Frame::Digest { digest } => Some(("digest", Source::Birth { node_id: digest.node.node_id.to_string(), incarnation: digest.node.incarnation.to_string() }, Key::DigestSeq(digest.digest_seq))),
        Frame::Members { mesh, topology_version, published_at_rafka_ms, .. } => {
            Some(("members", Source::Mesh(mesh.clone()), Key::Publication { published_at_rafka_ms: *published_at_rafka_ms, topology_version: *topology_version }))
        }
        Frame::MembersDelta { mesh, topology_version, published_at_rafka_ms, .. } => {
            Some(("members-delta", Source::Mesh(mesh.clone()), Key::Publication { published_at_rafka_ms: *published_at_rafka_ms, topology_version: *topology_version }))
        }
        Frame::NodeDeleting { op, publisher, topology_version, .. } => Some(("node-deleting", Source::Lifecycle { publisher: publisher.to_string(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        Frame::NodeRestarting { op, publisher, topology_version, .. } => Some(("node-restarting", Source::Lifecycle { publisher: publisher.to_string(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        Frame::NodeStopped { op, publisher, topology_version, .. } => Some(("node-stopped", Source::Lifecycle { publisher: publisher.to_string(), mesh: op.name.mesh.clone() }, Key::TopologyVersion(*topology_version))),
        Frame::Seated { seat, holder } => Some((
            "seated",
            Source::Cohort { seat: seat.name(), mesh: if *seat == rdm_core::Seat::FabricPrimary { String::new() } else { holder.mesh.clone() } },
            Key::Seat { revision: holder.revision, stage: SeatStage::Seated },
        )),
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
    /// The source version each mesh's last install moved to: its publisher and `topology_version`.
    installed: HashMap<String, (PublisherId, u64)>,
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
    /// Whether `f` is taken: `Err` names the frame ignored, its source and both keys. A taken frame's
    /// key is held when it is newer than the held one, except a `Members` or `MembersDelta` key, held
    /// when its snapshot installs or its delta applies ([`Self::installed`]).
    pub(crate) fn admit(&mut self, f: &Frame) -> Result<(), Earlier> {
        let (frame, keys) = orders_of(f);
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
            Some((held, version)) if held == publisher && topology_version < *version => {
                Err(Earlier { frame, source: mesh.to_string(), held: format!("publisher={held} topology_version={version}"), offered: offered() })
            }
            _ => Ok(()),
        }
    }

    /// The `topology_version` of the source version installed of `mesh`.
    pub(crate) fn installed_version(&self, mesh: &str) -> Option<u64> {
        self.installed.get(mesh).map(|(_, v)| *v)
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
        self.installed.insert(mesh.to_string(), (publisher.clone(), topology_version));
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
    /// its key), and its key is held once it installed; a chunk of the installed publication is taken
    /// again (the handler absorbs it); a full of another publisher stamped later is taken whatever its
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
        let mut order = ReceiveOrder::default();
        let d = birth("mesh1.rpc.1");
        let at = |seq: u64| Frame::Digest { digest: MeshDigest { digest_seq: seq, ..d.clone() } };
        assert_eq!(order.admit(&at(7)), Ok(()));
        assert_eq!(order.admit(&at(7)), Ok(()), "a copy of the held heartbeat is the book's to absorb");
        assert_eq!(order.admit(&at(3)), Err(Earlier { frame: "digest", source: format!("{}/{}", d.node.node_id, d.node.incarnation.as_str()), held: "digest_seq=7".into(), offered: "digest_seq=3".into() }));
        let restarted = MeshDigest { node: MeshNode { incarnation: IncarnationId::mint(), ..d.node.clone() }, digest_seq: 1, ..d.clone() };
        assert_eq!(order.admit(&Frame::Digest { digest: restarted }), Ok(()), "a new incarnation is another source");
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

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part C1 step 7:
    /// "A late 'updating' never undoes an 'updated': the committed epoch, per cohort, is these two
    /// frames' order key in the one gossip receive path (R-RX3), already in the body so no new field;
    /// nothing orders inside a handler"): per cohort, "primary updating" at an epoch then "primary
    /// updated" at the same epoch are taken; a "primary updating" received after the "primary updated"
    /// of its epoch is ignored, naming the cohort and both keys; a copy of the held "updated" is taken;
    /// the next update's "updating" is newer; an "updated" of an earlier epoch is ignored. A frame
    /// naming both seats (Q1) is ignored when either cohort holds a newer key, and another mesh's cohort
    /// is another source. Must NOT happen: a late "updating" reaching a handler after its "updated".
    #[test]
    fn a_primary_updating_received_after_its_primary_updated_is_ignored() {
        use rdm_core::seat::transfer::{CommittedSeat, HandoverBirth, Seat as TransferSeat};
        let mut order = ReceiveOrder::default();
        let (holder, successor) = (birth("mesh1.admin.1"), birth("mesh1.admin.2"));
        let to = HandoverBirth { mesh: "mesh1".into(), node_id: successor.node.node_id.clone(), incarnation: successor.node.incarnation.clone() };
        let mesh_seat = |mesh: &str, revision: u64| CommittedSeat { seat: TransferSeat::MeshPrimary { mesh: mesh.into() }, holder: to.clone(), revision };
        let fabric_seat = |revision: u64| CommittedSeat { seat: TransferSeat::FabricPrimary, holder: to.clone(), revision };
        let updating = |seats: Vec<CommittedSeat>| Frame::PrimaryUpdating { op: op(&holder, "node.stop"), seats };
        let updated = |seats: Vec<CommittedSeat>| Frame::PrimaryUpdated { op: op(&holder, "node.stop"), seats };
        assert_eq!(order.admit(&updating(vec![mesh_seat("mesh1", 4)])), Ok(()));
        assert_eq!(order.admit(&updated(vec![mesh_seat("mesh1", 4)])), Ok(()), "the updated of the same revision follows its updating");
        assert_eq!(
            order.admit(&updating(vec![mesh_seat("mesh1", 4)])),
            Err(Earlier { frame: "primary-updating", source: "mesh-primary mesh1".into(), held: "revision=4 updated".into(), offered: "revision=4 updating".into() }),
            "a late updating never undoes an updated"
        );
        assert_eq!(order.admit(&updated(vec![mesh_seat("mesh1", 4)])), Ok(()), "a copy of the held updated is the handler's to absorb");
        assert_eq!(order.admit(&updating(vec![mesh_seat("mesh1", 5)])), Ok(()), "the next update is newer");
        assert_eq!(order.admit(&updated(vec![mesh_seat("mesh1", 3)])).map_err(|e| (e.frame, e.offered)), Err(("primary-updated", "revision=3 updated".to_string())), "an earlier committed revision is ignored");
        // Both seats in one frame (Q1): the fabric cohort's newer key ignores the whole frame.
        assert_eq!(order.admit(&updated(vec![fabric_seat(8)])), Ok(()));
        let both = order.admit(&updating(vec![mesh_seat("mesh1", 6), fabric_seat(8)])).unwrap_err();
        assert_eq!((both.frame, both.source.as_str(), both.held.as_str(), both.offered.as_str()), ("primary-updating", "fabric-primary", "revision=8 updated", "revision=8 updating"));
        assert_eq!(order.admit(&updating(vec![mesh_seat("mesh2", 1)])), Ok(()), "another mesh's cohort is another source");
    }

    /// CONTRACT (docs/architecture/gossip.md, R-RX3, Luke 2026-10-13 Q2: "`Seated { seat, holder }` |
    /// Holder record's `revision`, per complete seat key (`(mesh, node_type)` for a cohort, `fabric`
    /// for the fabric seat)"): a `Seated` record orders by its holder revision within its complete seat
    /// key; records for different cohorts (`mesh-primary`, `rpc_node-primary`, `broker-primary`,
    /// another mesh's cohort, `fabric-primary`) never suppress each other; a late record at an earlier
    /// revision for the same seat is ignored by name (`revision=<held>` vs `revision=<offered>`); a
    /// copy of the held revision is taken. Must NOT happen: a cohort's higher revision suppressing
    /// another cohort's lower revision, or an older revision of the same seat passing the receive path.
    #[test]
    fn seated_records_order_by_revision_per_complete_seat_key_and_different_cohorts_never_suppress_each_other() {
        use rdm_core::{NodeKind, Seat, SeatHolder};
        let mut order = ReceiveOrder::default();
        let seated = |seat: Seat, mesh: &str, revision: u64| Frame::Seated {
            seat,
            holder: SeatHolder { mesh: mesh.into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision },
        };
        assert_eq!(order.admit(&seated(Seat::MeshPrimary, "mesh1", 5)), Ok(()));
        assert_eq!(order.admit(&seated(Seat::Cohort(NodeKind::RpcNode), "mesh1", 2)), Ok(()), "mesh1's rpc cohort is its own seat key, never suppressed by mesh1's admin revision 5");
        assert_eq!(order.admit(&seated(Seat::Cohort(NodeKind::Broker), "mesh1", 1)), Ok(()), "mesh1's broker cohort is its own seat key");
        assert_eq!(order.admit(&seated(Seat::Cohort(NodeKind::RpcNode), "mesh2", 1)), Ok(()), "mesh2's rpc cohort is its own seat key");
        assert_eq!(order.admit(&seated(Seat::FabricPrimary, "mesh1", 3)), Ok(()), "the fabric seat is its own seat key");
        assert_eq!(order.admit(&seated(Seat::Cohort(NodeKind::RpcNode), "mesh1", 2)), Ok(()), "a copy of the held revision is taken");
        assert_eq!(
            order.admit(&seated(Seat::Cohort(NodeKind::RpcNode), "mesh1", 1)),
            Err(Earlier { frame: "seated", source: "rpc_node-primary mesh1".into(), held: "revision=2".into(), offered: "revision=1".into() }),
            "a late older revision of the same cohort is ignored by name"
        );
        assert_eq!(
            order.admit(&seated(Seat::FabricPrimary, "mesh2", 2)),
            Err(Earlier { frame: "seated", source: "fabric-primary".into(), held: "revision=3".into(), offered: "revision=2".into() }),
            "the fabric seat orders across meshes under one fabric key"
        );
        assert_eq!(order.admit(&seated(Seat::Cohort(NodeKind::RpcNode), "mesh1", 3)), Ok(()), "a newer revision of the cohort advances its key");
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
