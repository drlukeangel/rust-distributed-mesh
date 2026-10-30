//! Rafka-time: node-admin's time, adopted from the authority and never from a node's own clock.
//!
//! [`RafkaTime`] (the one reader every stamp of a process reads) is defined in
//! `rdm_transport::clock`. This module is where a process takes a value into it, through
//! the one entry point [`adopt_pulled`]:
//!
//! - The fabric's Day-0 root, a node-admin with no launcher, no seeds and no recovery flag,
//!   adopts its own OS clock once at boot (`adopt_own_clock`); so does the node-admin a fabric
//!   recovery launches on the fabric-primary's inherited data dir when every node the store names,
//!   the old process on that dir included, is confirmed gone by the provider, never by a wait.
//! - Every other process adopts from the answer to its pull: the `JoinNode` answer
//!   ([`crate::wire::JoinControl::rafka_time_ms`]), its start's last step, which refreshes rafka-time
//!   from a `GetTopology` read ([`rdm_core::topology::TopologyReply::RafkaTime`]), a
//!   re-entry's read, and a mesh primary's pull from a new fabric-primary (`run_role_move_pull`).
//!   A member adopts from whoever answers. A mesh primary adopts only from a fabric-primary role; any
//!   other answer is refused by name and the mesh primary keeps the rafka-time it holds.
//! - Between pulls a node's rafka-time is disciplined from its own authority's heartbeats only. A
//!   delta-gap top-up ([`crate::topology_read::top_up_fetch`]) is a topology read repair and adopts
//!   nothing.
//! - A node answers a pull with the rafka-time it adopted ([`crate::topology_read::TopologyDoor`],
//!   `admin`'s join answer), never its own clock, so every served time is one lineage.
//!
//! Neither side estimates network delay: the value is adopted as the authority read it.

use rdm_core::topology::PrimaryKind;
use rdm_core::NodeId;
use rdm_transport::clock::{Adopted, RafkaTime};
use rdm_transport::topology::PrimaryView;

/// Who is pulling, by the primary it is in its own view when it pulls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Puller {
    /// Is no primary: adopts from whichever node answers.
    Member,
    /// Is its mesh's mesh-primary: adopts only from the fabric-primary.
    MeshPrimary,
}

impl Puller {
    /// The puller `node_id` of `mesh` is, from the primaries its view holds.
    pub fn of(primaries: &PrimaryView, mesh: &str, node_id: &NodeId) -> Self {
        if primary_held(primaries, mesh, node_id).is_some() {
            Self::MeshPrimary
        } else {
            Self::Member
        }
    }
}

/// Which primary `node_id` of `mesh` is in the primaries `primaries` holds: the fabric-primary when it is,
/// else its mesh's mesh-primary, else none.
pub fn primary_held(primaries: &PrimaryView, mesh: &str, node_id: &NodeId) -> Option<PrimaryKind> {
    if primaries.fabric().is_some_and(|h| &h.node_id == node_id) {
        return Some(PrimaryKind::FabricPrimary);
    }
    primaries.mesh(mesh).filter(|h| &h.node_id == node_id).map(|_| PrimaryKind::MeshPrimary)
}

/// Why a mesh primary refused an answer's time, by its span `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The answering node holds no primary role: a replica answers with a time it adopted from its own
    /// primary, which a mesh primary does not take.
    ServedByAReplica,
    /// The answering node is a mesh primary, not the fabric-primary.
    AMeshPrimaryTakesOnlyTheFabricPrimary,
}

impl Refusal {
    /// The reason as it appears in the span.
    pub fn reason(self) -> &'static str {
        match self {
            Self::ServedByAReplica => "served-by-a-replica",
            Self::AMeshPrimaryTakesOnlyTheFabricPrimary => "a-mesh-primary-takes-only-the-fabric-primary",
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

/// One answer's time: who served it, which primary it was, and the value.
#[derive(Debug, Clone, Copy)]
pub struct Served<'a> {
    /// The pull that carried it: `join` (a birth's `JoinNode`) or `get-topology`.
    pub via: &'static str,
    /// The answering node's `path.name`.
    pub served_by: &'a str,
    /// The authority's rafka-time in milliseconds.
    pub ms: u64,
    /// Which primary the answering node was, `None` for none. A join answer carries none: a join is
    /// a birth, a birth is a member, and a member adopts from whoever answers.
    pub kind: Option<PrimaryKind>,
}

/// The verdict, with no side effect: whether `puller` may adopt from an answer served as `kind`.
pub fn verdict(puller: Puller, kind: Option<PrimaryKind>) -> Result<(), Refusal> {
    match (puller, kind) {
        (Puller::Member, _) | (Puller::MeshPrimary, Some(PrimaryKind::FabricPrimary)) => Ok(()),
        (Puller::MeshPrimary, Some(PrimaryKind::MeshPrimary)) => Err(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary),
        (Puller::MeshPrimary, None) => Err(Refusal::ServedByAReplica),
    }
}

/// Adopt the time `served` carries into `time`: the single entry point every pull goes through.
///
/// Span `rdm.mesh.entry.update.via-rafka-time-adopted{source=pull}` on adoption;
/// `rdm.mesh.entry.reject.via-rafka-time-from-non-primary{reason}` on a refusal, after which `time`
/// is unchanged.
pub fn adopt_pulled(time: &RafkaTime, puller: Puller, node: &str, served: Served<'_>) -> Result<Adopted, Refusal> {
    adopt_pulled_in(&tracing::Span::current(), time, puller, node, served)
}

/// [`adopt_pulled`], its spans children of `parent` (a birth's boot span is never entered: a step
/// of it names it as its parent).
pub fn adopt_pulled_in(parent: &tracing::Span, time: &RafkaTime, puller: Puller, node: &str, served: Served<'_>) -> Result<Adopted, Refusal> {
    let kind = if served.via == "join" { "not-carried" } else { served.kind.map_or("none", PrimaryKind::name) };
    if let Err(refusal) = verdict(puller, served.kind) {
        tracing::info_span!(
            parent: parent,
            "rdm.mesh.entry.reject.via-rafka-time-from-non-primary",
            node,
            via = served.via,
            served_by = served.served_by,
            kind,
            reason = refusal.reason(),
            offered_ms = served.ms,
            held_ms = time.try_now_ms().map_or(-1, |m| m as i64),
        )
        .in_scope(|| tracing::warn!("a mesh primary takes rafka-time only from the fabric-primary: it keeps the rafka-time it holds"));
        return Err(refusal);
    }
    let adopted = time.adopt(served.ms);
    let reason = if served.via == "join" { "join" } else { "recovery-pull" };
    tracing::info_span!(
        parent: parent,
        "rdm.mesh.entry.update.via-rafka-time-adopted",
        node,
        source = "pull",
        via = served.via,
        reason,
        served_by = served.served_by,
        kind,
        reference_ms = adopted.reference_ms,
        previous_ms = adopted.previous_ms.map_or(-1, |m| m as i64),
        stalls_ms = adopted.stalls_ms,
    )
    .in_scope(|| tracing::info!("rafka-time adopted from the authority's answer"));
    Ok(adopted)
}

/// A birth adopts the time its `JoinNode` answer carries: the one entry point of a join.
pub fn adopt_join_answer(parent: &tracing::Span, time: &RafkaTime, node: &str, answer: &crate::wire::JoinAnswer) -> Adopted {
    adopt_pulled_in(parent, time, Puller::Member, node, Served { via: "join", served_by: &answer.served_by, ms: answer.control.rafka_time_ms, kind: None })
        .expect("a member adopts from whoever answers")
}

/// Why the node-admin a fabric recovery launches on an inherited data dir adopts its own clock when every
/// node the store names, the old process on that dir included, is confirmed gone by the provider, never by
/// a wait (fabric-recovery.md "Nothing alive"; origin-ordering.md).
pub const OWN_CLOCK_NOTHING_ALIVE: &str = "nothing-alive";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_member_adopts_from_whoever_answers() {
        for kind in [None, Some(PrimaryKind::MeshPrimary), Some(PrimaryKind::FabricPrimary)] {
            assert_eq!(verdict(Puller::Member, kind), Ok(()), "{kind:?}");
        }
    }

    #[test]
    fn a_mesh_primary_adopts_only_from_the_fabric_primary() {
        assert_eq!(verdict(Puller::MeshPrimary, Some(PrimaryKind::FabricPrimary)), Ok(()));
        assert_eq!(verdict(Puller::MeshPrimary, None), Err(Refusal::ServedByAReplica));
        assert_eq!(verdict(Puller::MeshPrimary, Some(PrimaryKind::MeshPrimary)), Err(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary));
        assert_eq!(Refusal::ServedByAReplica.reason(), "served-by-a-replica");
        assert_eq!(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary.reason(), "a-mesh-primary-takes-only-the-fabric-primary");
    }

    #[test]
    fn a_refused_answer_leaves_the_held_time_unchanged() {
        let time = RafkaTime::unadopted();
        time.adopt(1_000_000);
        let served = Served { via: "get-topology", served_by: "mesh2.admin.2", ms: 9_000_000, kind: None };
        assert_eq!(adopt_pulled(&time, Puller::MeshPrimary, "mesh1.admin.1", served).unwrap_err(), Refusal::ServedByAReplica);
        assert!(time.now_ms() < 2_000_000, "the offered time was not taken");
        let served = Served { kind: Some(PrimaryKind::FabricPrimary), ..served };
        adopt_pulled(&time, Puller::MeshPrimary, "mesh1.admin.1", served).unwrap();
        assert!(time.now_ms() >= 9_000_000);
    }

}
