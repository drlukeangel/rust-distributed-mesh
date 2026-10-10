//! Which heartbeats may discipline a node's rafka-time, and the spans that say what became of them.
//!
//! The clock ([`crate::clock::RafkaTime::observe_at`]) decides what a sample does; this module
//! decides whether a stamp is a sample at all. The rule is the lineage and nothing else:
//!
//! - A member (any node that is not its mesh's primary) observes the digest of its CURRENT mesh
//!   primary: the exact birth (node id and incarnation) its seat record for its own mesh names.
//! - A mesh primary observes the Members aggregate its CURRENT fabric-primary published on the
//!   backbone: the publisher's incarnation is the one the fabric seat record names, and the
//!   publisher's own digest in that aggregate is the authority's status.
//! - The fabric-primary observes nothing; no other node's stamp is ever offered.
//!
//! A stamp is offered only when the authority is `ReadyForTraffic` ("No node is ready-for-traffic
//! before it has rafka-time", origin-ordering.md), because a pending publisher may not hold the
//! lineage's time yet. A digest the book did not admit as newer is not a sample either.
//!
//! The stamps are publication time: `MeshDigest::emitted_at_rafka_ms` is read from the composed
//! clock inside [`crate::membership::Membership::publish`] as the frame is built, and
//! `Frame::Members::published_at_rafka_ms` is read from the same clock at the start of
//! `Backbone::publish`, before the chunks are built and sent.

use crate::clock::{Ignored, Observed, SharedClock};
use rafka_mesh_entity::{MemberStatus, MeshDigest, PublisherId};

use crate::membership::SeatBook;

/// What a received frame is to the clock.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Not the authority's stamp: nothing is offered and nothing is said (every other node's
    /// heartbeat lands here, every two seconds).
    NotTheAuthority,
    /// The authority's stamp, which may not be taken, and why.
    Refused { authority: String, reason: &'static str },
    /// The authority's stamp, eligible.
    Sample { authority: String, stamp_ms: u64 },
}

/// A member's view of one mesh-channel digest.
pub(crate) fn from_mesh_primary_digest(mesh: &str, me: &str, i_am_mesh_primary: bool, seats: &SeatBook, d: &MeshDigest, admitted_as_newer: bool) -> Verdict {
    if i_am_mesh_primary || d.node.name.mesh != mesh || d.node.name.to_string() == me {
        return Verdict::NotTheAuthority;
    }
    let Some(holder) = seats.mesh(mesh) else { return Verdict::NotTheAuthority };
    if !holder.is_birth(&d.node.node_id, &d.node.incarnation) {
        return Verdict::NotTheAuthority;
    }
    let authority = d.node.name.to_string();
    if !admitted_as_newer {
        return Verdict::Refused { authority, reason: "digest-not-newer" };
    }
    if d.status != MemberStatus::ReadyForTraffic {
        return Verdict::Refused { authority, reason: "authority-not-ready-for-traffic" };
    }
    Verdict::Sample { authority, stamp_ms: d.emitted_at_rafka_ms }
}

/// A mesh primary's view of one backbone Members aggregate that installed.
pub(crate) fn from_fabric_primary_members(me: &str, i_am_mesh_primary: bool, seats: &SeatBook, publisher: &PublisherId, publisher_digest: Option<&MeshDigest>, published_at_rafka_ms: u64) -> Verdict {
    if !i_am_mesh_primary || publisher.node == me {
        return Verdict::NotTheAuthority;
    }
    let Some(holder) = seats.fabric() else { return Verdict::NotTheAuthority };
    if holder.incarnation != publisher.incarnation {
        return Verdict::NotTheAuthority;
    }
    let authority = publisher.node.clone();
    let Some(d) = publisher_digest else { return Verdict::Refused { authority, reason: "authority-not-in-its-aggregate" } };
    if d.node.node_id != holder.node_id {
        return Verdict::Refused { authority, reason: "aggregate-birth-is-not-the-seat-holder" };
    }
    if d.status != MemberStatus::ReadyForTraffic {
        return Verdict::Refused { authority, reason: "authority-not-ready-for-traffic" };
    }
    Verdict::Sample { authority, stamp_ms: published_at_rafka_ms }
}

/// Offer a verdict to `clock` and say what became of it. Spans are written for what moved the
/// clock or decided it (a step, a slew, a full window), and for an authority's stamp refused;
/// a sample that only joins the window says nothing.
pub(crate) fn offer(clock: &SharedClock, node: &str, source: &'static str, verdict: Verdict) {
    match verdict {
        Verdict::NotTheAuthority => {}
        Verdict::Refused { authority, reason } => {
            tracing::info_span!("rdm.mesh.entry.reject.via-rafka-time-sample", node, authority = %authority, source, reason)
                .in_scope(|| tracing::info!("the authority's stamp is not an eligible sample: the clock is unchanged"));
        }
        Verdict::Sample { authority, stamp_ms } => match clock.observe(stamp_ms) {
            Observed::Collecting => {}
            Observed::Stepped { by_ms } => {
                tracing::info_span!("rdm.mesh.entry.update.via-rafka-time-observed", node, authority = %authority, source, outcome = "stepped-forward", stamp_ms, by_ms)
                    .in_scope(|| tracing::info!("the authority's stamp was ahead of this clock: its reference stepped forward to it"));
            }
            Observed::SlewStarted { best_offset_ms, excess_ms, ppm, recovers_in_ms } => {
                tracing::info_span!("rdm.mesh.entry.update.via-rafka-time-observed", node, authority = %authority, source, outcome = "slew-started", stamp_ms, best_offset_ms, excess_ms, slew_ppm = ppm, recovers_in_ms)
                    .in_scope(|| tracing::info!("a full window of the authority's stamps stood behind this clock by more than the jitter allowance: it sheds the excess"));
            }
            Observed::Within { best_offset_ms, jitter_ms } => {
                tracing::info_span!("rdm.mesh.entry.update.via-rafka-time-observed", node, authority = %authority, source, outcome = "within-allowance", stamp_ms, best_offset_ms, jitter_ms)
                    .in_scope(|| tracing::info!("a full window of the authority's stamps stood within the jitter allowance: the clock is left as it is"));
            }
            Observed::Ignored(Ignored::NotDisciplined) => {}
            Observed::Ignored(why) => {
                tracing::info_span!("rdm.mesh.entry.reject.via-rafka-time-sample", node, authority = %authority, source, reason = why.reason(), stamp_ms)
                    .in_scope(|| tracing::info!("the authority's stamp moved nothing"));
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MeshNode, NodeId, Seat, SeatHolder};

    const MESH: &str = "mesh1";

    fn digest(name: &str, node_id: &NodeId, incarnation: &IncarnationId, status: MemberStatus, at: u64) -> MeshDigest {
        MeshDigest {
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode {
                node_id: node_id.clone(),
                name: name.parse().unwrap(),
                endpoint_id: EndpointId("key".into()),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: incarnation.clone(),
                supersedes: None,
                runtime: None,
            },
            status,
            admin_api_base: None,
            emitted_at_rafka_ms: at,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    fn seated(seat: Seat, mesh: &str, id: &NodeId, inc: &IncarnationId, epoch: u64) -> SeatBook {
        let b = SeatBook::default();
        b.take(seat, &SeatHolder { mesh: mesh.into(), node_id: id.clone(), incarnation: inc.clone(), epoch });
        b
    }

    /// CONTRACT: a member takes its mesh primary's ready, newer digest, and the stamp is the
    /// digest's publication time.
    #[test]
    fn a_member_takes_its_mesh_primarys_ready_digest() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::MeshPrimary, MESH, &id, &inc, 1);
        let d = digest("mesh1.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 7_000);
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &seats, &d, true), Verdict::Sample { authority: "mesh1.admin.1".into(), stamp_ms: 7_000 });
    }

    /// CONTRACT: a publisher that has not declared itself ready-for-traffic (pre-adoption) offers no sample.
    #[test]
    fn a_pending_or_leaving_authority_offers_no_sample() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::MeshPrimary, MESH, &id, &inc, 1);
        for status in [MemberStatus::Pending, MemberStatus::Draining, MemberStatus::Leaving] {
            let d = digest("mesh1.admin.1", &id, &inc, status, 7_000);
            assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &seats, &d, true), Verdict::Refused { authority: "mesh1.admin.1".into(), reason: "authority-not-ready-for-traffic" }, "{status:?}");
        }
    }

    /// CONTRACT: a digest the book did not admit as newer is no sample.
    #[test]
    fn a_digest_the_book_did_not_admit_offers_no_sample() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::MeshPrimary, MESH, &id, &inc, 1);
        let d = digest("mesh1.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 7_000);
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &seats, &d, false), Verdict::Refused { authority: "mesh1.admin.1".into(), reason: "digest-not-newer" });
    }

    /// CONTRACT: only the seat holder's exact birth is the authority: another member, another
    /// birth of the same node, another mesh and a node with no seat record all offer nothing.
    #[test]
    fn only_the_exact_birth_the_seat_record_names_is_the_authority() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::MeshPrimary, MESH, &id, &inc, 1);
        let other_member = digest("mesh1.rpc.2", &NodeId::mint(), &IncarnationId::mint(), MemberStatus::ReadyForTraffic, 7_000);
        let older_birth = digest("mesh1.admin.1", &id, &IncarnationId::mint(), MemberStatus::ReadyForTraffic, 7_000);
        let other_mesh = digest("mesh2.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 7_000);
        for d in [&other_member, &older_birth, &other_mesh] {
            assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &seats, d, true), Verdict::NotTheAuthority, "{}", d.node.name);
        }
        let d = digest("mesh1.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 7_000);
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &SeatBook::default(), &d, true), Verdict::NotTheAuthority, "no seat record");
        // The authority retargets with the seat record: the superseded holder offers nothing.
        let (id2, inc2) = (NodeId::mint(), IncarnationId::mint());
        seats.take(Seat::MeshPrimary, &SeatHolder { mesh: MESH.into(), node_id: id2.clone(), incarnation: inc2.clone(), epoch: 2 });
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &seats, &d, true), Verdict::NotTheAuthority);
        let heir = digest("mesh1.admin.2", &id2, &inc2, MemberStatus::ReadyForTraffic, 9_000);
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.rpc.1", false, &seats, &heir, true), Verdict::Sample { authority: "mesh1.admin.2".into(), stamp_ms: 9_000 });
    }

    /// CONTRACT: a mesh primary takes no mesh-channel digest (it observes the backbone), and a node
    /// never takes its own.
    #[test]
    fn a_mesh_primary_and_a_node_itself_take_no_mesh_channel_digest() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::MeshPrimary, MESH, &id, &inc, 1);
        let d = digest("mesh1.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 7_000);
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.admin.2", true, &seats, &d, true), Verdict::NotTheAuthority);
        assert_eq!(from_mesh_primary_digest(MESH, "mesh1.admin.1", false, &seats, &d, true), Verdict::NotTheAuthority);
    }

    fn publisher(name: &str, inc: &IncarnationId) -> PublisherId {
        PublisherId { node: name.into(), incarnation: inc.clone() }
    }

    /// CONTRACT: a mesh primary takes the stamp of the aggregate its fabric-primary published,
    /// when the aggregate lists that exact birth ready-for-traffic.
    #[test]
    fn a_mesh_primary_takes_the_fabric_primarys_aggregate_stamp() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::FabricPrimary, "mesh2", &id, &inc, 3);
        let auth = digest("mesh2.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 1);
        let p = publisher("mesh2.admin.1", &inc);
        assert_eq!(from_fabric_primary_members("mesh1.admin.1", true, &seats, &p, Some(&auth), 12_000), Verdict::Sample { authority: "mesh2.admin.1".into(), stamp_ms: 12_000 });
        // Pre-adoption fabric-primary: still pending in its own aggregate.
        let pending = digest("mesh2.admin.1", &id, &inc, MemberStatus::Pending, 1);
        assert_eq!(from_fabric_primary_members("mesh1.admin.1", true, &seats, &p, Some(&pending), 12_000), Verdict::Refused { authority: "mesh2.admin.1".into(), reason: "authority-not-ready-for-traffic" });
        assert_eq!(from_fabric_primary_members("mesh1.admin.1", true, &seats, &p, None, 12_000), Verdict::Refused { authority: "mesh2.admin.1".into(), reason: "authority-not-in-its-aggregate" });
    }

    /// CONTRACT: the fabric-primary never adjusts from a follower, and a mesh primary takes no
    /// aggregate but the one its fabric seat record names; a non-primary takes none.
    #[test]
    fn only_the_current_fabric_primary_is_observed_and_only_by_a_mesh_primary() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let seats = seated(Seat::FabricPrimary, "mesh2", &id, &inc, 3);
        let auth = digest("mesh2.admin.1", &id, &inc, MemberStatus::ReadyForTraffic, 1);
        let other_inc = IncarnationId::mint();
        let other = publisher("mesh3.admin.1", &other_inc);
        let other_digest = digest("mesh3.admin.1", &NodeId::mint(), &other_inc, MemberStatus::ReadyForTraffic, 1);
        // A follower mesh primary's aggregate.
        assert_eq!(from_fabric_primary_members("mesh1.admin.1", true, &seats, &other, Some(&other_digest), 5), Verdict::NotTheAuthority);
        // The fabric-primary itself, and a node that is not a mesh primary.
        let p = publisher("mesh2.admin.1", &inc);
        assert_eq!(from_fabric_primary_members("mesh2.admin.1", true, &seats, &p, Some(&auth), 5), Verdict::NotTheAuthority);
        assert_eq!(from_fabric_primary_members("mesh1.admin.2", false, &seats, &p, Some(&auth), 5), Verdict::NotTheAuthority);
        // A superseded fabric-primary's aggregate once the seat moved.
        let (id2, inc2) = (NodeId::mint(), IncarnationId::mint());
        seats.take(Seat::FabricPrimary, &SeatHolder { mesh: "mesh3".into(), node_id: id2, incarnation: inc2, epoch: 4 });
        assert_eq!(from_fabric_primary_members("mesh1.admin.1", true, &seats, &p, Some(&auth), 5), Verdict::NotTheAuthority);
    }
}
