//! Rafka-time: node-admin's time, adopted from the authority and never from a node's own clock.
//!
//! [`RafkaTime`] (the one reader every stamp of a process reads) is defined in
//! `rafka_mesh_transport::clock`. This module is where a process takes a value into it, through
//! the one entry point [`adopt_pulled`]:
//!
//! - The fabric's Day-0 root, a node-admin with no launcher, no seeds and no recovery flag,
//!   adopts its own OS clock once at boot ([`adopt_own_clock`]); so does a restarted node-admin
//!   whose own seat records name it the fabric-primary and that holds no rafka-time.
//! - Every other process adopts from the answer to its pull: the `JoinNode` answer
//!   ([`crate::wire::JoinControl::rafka_time_ms`]) and every `GetTopology` read
//!   ([`rafka_node_rpc_contract::topology::TopologyReply::RafkaTime`]). A member adopts from
//!   whoever answers. A mesh primary adopts only from a fabric-primary seat; any other answer is
//!   refused by name and the mesh primary keeps the rafka-time it holds.
//! - A node answers a pull with the rafka-time it adopted ([`crate::topology_read::TopologyDoor`],
//!   [`crate::admin`]'s join answer), never its own clock, so every served time is one lineage.
//!
//! Neither side estimates network delay: the value is adopted as the authority read it.

use rafka_mesh_entity::{NodeId, Seat};
use rafka_mesh_transport::clock::{Adopted, RafkaTime};
use rafka_mesh_transport::membership::SeatBook;
use std::sync::Arc;
use tracing::Instrument as _;

/// Who is pulling, by the seat it holds in its own seat records when it pulls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Puller {
    /// Holds no mesh seat: adopts from whichever node answers.
    Member,
    /// Holds its mesh's primary seat: adopts only from a fabric-primary.
    MeshPrimary,
}

impl Puller {
    /// The puller `node_id` of `mesh` is, from its own seat records.
    pub fn of(seats: &SeatBook, mesh: &str, node_id: &NodeId) -> Self {
        if seat_held(seats, mesh, node_id).is_some() {
            Self::MeshPrimary
        } else {
            Self::Member
        }
    }
}

/// The seat `node_id` of `mesh` holds in `seats`: the fabric seat when it holds it, else its
/// mesh's, else none (a replica).
pub fn seat_held(seats: &SeatBook, mesh: &str, node_id: &NodeId) -> Option<Seat> {
    if seats.fabric().is_some_and(|h| &h.node_id == node_id) {
        return Some(Seat::FabricPrimary);
    }
    seats.mesh(mesh).filter(|h| &h.node_id == node_id).map(|_| Seat::MeshPrimary)
}

/// Why a mesh primary refused an answer's time, by its span `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The answering node holds no seat: a replica answers with a time it adopted from its own
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

/// One answer's time: who served it, the seat it held, and the value.
#[derive(Debug, Clone, Copy)]
pub struct Served<'a> {
    /// The pull that carried it: `join` (a birth's `JoinNode`) or `get-topology`.
    pub via: &'static str,
    /// The answering node's `path.name`.
    pub served_by: &'a str,
    /// The authority's rafka-time in milliseconds.
    pub ms: u64,
    /// The seat the answering node held, `None` for a replica. A join answer carries no seat: a
    /// join is a birth, a birth is a member, and a member adopts from whoever answers.
    pub seat: Option<Seat>,
}

/// The verdict, with no side effect: whether `puller` may adopt from an answer served under `seat`.
pub fn verdict(puller: Puller, seat: Option<Seat>) -> Result<(), Refusal> {
    match (puller, seat) {
        (Puller::Member, _) | (Puller::MeshPrimary, Some(Seat::FabricPrimary)) => Ok(()),
        (Puller::MeshPrimary, Some(Seat::MeshPrimary)) => Err(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary),
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
    let seat = if served.via == "join" { "not-carried" } else { served.seat.map_or("none", Seat::name) };
    if let Err(refusal) = verdict(puller, served.seat) {
        tracing::info_span!(
            parent: parent,
            "rdm.mesh.entry.reject.via-rafka-time-from-non-primary",
            node,
            via = served.via,
            served_by = served.served_by,
            seat,
            reason = refusal.reason(),
            offered_ms = served.ms,
            held_ms = time.try_now_ms().map_or(-1, |m| m as i64),
        )
        .in_scope(|| tracing::warn!("a mesh primary takes rafka-time only from the fabric-primary: it keeps the rafka-time it holds"));
        return Err(refusal);
    }
    let adopted = time.adopt(served.ms);
    tracing::info_span!(
        parent: parent,
        "rdm.mesh.entry.update.via-rafka-time-adopted",
        node,
        source = "pull",
        via = served.via,
        served_by = served.served_by,
        seat,
        reference_ms = adopted.reference_ms,
        previous_ms = adopted.previous_ms.map_or(-1, |m| m as i64),
        stalls_ms = adopted.stalls_ms,
    )
    .in_scope(|| tracing::info!("rafka-time adopted from the authority's answer"));
    Ok(adopted)
}

/// A birth adopts the time its `JoinNode` answer carries: the one entry point of a join.
pub fn adopt_join_answer(parent: &tracing::Span, time: &RafkaTime, node: &str, answer: &crate::wire::JoinAnswer) -> Adopted {
    adopt_pulled_in(parent, time, Puller::Member, node, Served { via: "join", served_by: &answer.served_by, ms: answer.control.rafka_time_ms, seat: None })
        .expect("a member adopts from whoever answers")
}

/// Why the Day-0 root adopts its own clock: no authority exists above it.
pub const OWN_CLOCK_DAY0_ROOT: &str = "day0-root";
/// Why a restarted fabric-seat holder adopts its own clock: it is the fabric's root and holds none.
pub const OWN_CLOCK_FABRIC_SEAT: &str = "fabric-seat-held-without-rafka-time";

/// The fabric's root adopts its own OS clock as rafka-time, once: the Day-0 root at boot, and an
/// admin that holds the fabric seat and no rafka-time. There is no authority above either.
pub fn adopt_own_clock(parent: &tracing::Span, time: &RafkaTime, node: &str, reason: &'static str) -> Adopted {
    let now = rafka_mesh_transport::clock::Clock::now_rafka_ms(&rafka_mesh_transport::clock::OsClock);
    let adopted = time.adopt(now);
    tracing::info_span!(
        parent: parent,
        "rdm.mesh.entry.update.via-rafka-time-adopted",
        node,
        source = "own-clock",
        reason,
        reference_ms = adopted.reference_ms,
    )
    .in_scope(|| tracing::info!("the fabric's root adopted its own OS clock as rafka-time"));
    adopted
}

/// How many reads one seat move owes before the pull ends unanswered, and the pause between them
/// (grows by this much per attempt). Read-only spacing: nothing else retries.
const SEAT_MOVE_ATTEMPTS: u32 = 12;
const SEAT_MOVE_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);

/// A mesh primary pulls rafka-time once from the new fabric-primary each time the fabric seat moves
/// to another holder (a record of a later epoch naming another node). The fabric-primary itself
/// keeps the rafka-time it holds: a handover changes the server, not the clock, so the fabric stays
/// on one lineage. A replica holds no mesh seat and adopted its time at its join.
///
/// The pull is `GetTopology` of the holder, adopted through [`adopt_pulled`] under the rule of a
/// mesh primary. Each read targets the holder this admin's seat records name at that moment: a
/// holder superseded while the pull runs (a hand-over whose new holder was itself replaced) answers
/// `served-by-a-replica` and is refused by name, and the next read goes to the holder that
/// superseded it. A holder that never answers ends the pull with the time held.
pub async fn run_seat_move_pull(
    time: RafkaTime,
    me: crate::model::PathName,
    me_id: NodeId,
    client: Arc<rafka_node_rpc::NodeRpcClient>,
    membership: rafka_mesh_transport::membership::Membership,
) {
    let seats = membership.seats().clone();
    let mut changed = seats.subscribe();
    let mut last = seats.fabric().map(|h| (h.node_id, h.epoch));
    loop {
        if changed.changed().await.is_err() {
            return;
        }
        let Some(holder) = seats.fabric() else { continue };
        let now = Some((holder.node_id.clone(), holder.epoch));
        if now == last {
            continue;
        }
        let from = last.replace((holder.node_id.clone(), holder.epoch));
        // The first record heard is not a move; and the fabric-primary pulls from no one.
        let Some((from_node, _)) = from else { continue };
        if holder.node_id == me_id || from_node == holder.node_id {
            continue;
        }
        if Puller::of(&seats, &me.mesh, &me_id) != Puller::MeshPrimary {
            continue;
        }
        let span = tracing::info_span!("rdm.mesh.entry.resolve.via-fabric-seat-moved", node = %me, from = %from_node, to = %holder.node_id, to_mesh = %holder.mesh, epoch = holder.epoch, outcome = tracing::field::Empty, attempts = tracing::field::Empty, adopted_from_epoch = tracing::field::Empty);
        let (outcome, attempts, adopted_from) = pull_from_fabric_primary(&time, &me, &me_id, &client, &membership, holder).instrument(span.clone()).await;
        span.record("outcome", outcome.as_str());
        span.record("attempts", attempts);
        if let Some(h) = adopted_from {
            span.record("adopted_from_epoch", h.epoch);
            // The record adopted from is the last move answered: a newer record is a newer move.
            last = Some((h.node_id, h.epoch));
        }
        span.in_scope(|| tracing::info!("the fabric seat moved: this mesh primary pulled rafka-time once from the new fabric-primary"));
    }
}

async fn pull_from_fabric_primary(
    time: &RafkaTime,
    me: &crate::model::PathName,
    me_id: &NodeId,
    client: &rafka_node_rpc::NodeRpcClient,
    membership: &rafka_mesh_transport::membership::Membership,
    moved_to: rafka_mesh_entity::SeatHolder,
) -> (String, u32, Option<rafka_mesh_entity::SeatHolder>) {
    let mut last = String::from("no attempt");
    let mut holder = moved_to;
    for attempt in 1..=SEAT_MOVE_ATTEMPTS {
        // The holder this admin's records name now: a seat that moved again is pulled from where it is.
        if let Some(current) = membership.seats().fabric() {
            if current.node_id == *me_id {
                return ("kept: this admin took the fabric seat itself".into(), attempt - 1, None);
            }
            holder = current;
        }
        let read = crate::topology_read::read_topology(client, &rafka_node_rpc::NodeTarget::ExactNode(holder.node_id.clone()), &me.to_string(), Some(&holder.mesh), None).await;
        match read {
            Ok(read) => {
                let served_by = membership.book.all().into_iter().find(|d| d.node.node_id == holder.node_id).map(|d| d.node.name.to_string()).unwrap_or_else(|| holder.node_id.to_string());
                match read.adopt_rafka_time(time, membership, &me.to_string(), &me.mesh, me_id, &served_by) {
                    Ok(_) => return ("adopted".into(), attempt, Some(holder)),
                    Err(e) => last = e.to_string(),
                }
            }
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(SEAT_MOVE_PAUSE * attempt).await;
    }
    tracing::info_span!("rdm.mesh.entry.reject.via-rafka-time-pull-unanswered", node = %me, to = %holder.node_id, attempts = SEAT_MOVE_ATTEMPTS, reason = %last)
        .in_scope(|| tracing::warn!("the fabric-primary did not serve rafka-time: this mesh primary keeps the rafka-time it holds"));
    (format!("kept: {last}"), SEAT_MOVE_ATTEMPTS, None)
}

/// How long a recovering admin looks for a node that holds rafka-time before it refuses to start.
pub const RECOVERY_PULL_WITHIN: std::time::Duration = std::time::Duration::from_secs(60);

/// A node-admin with no launcher and no `JoinNode` to make (a restart from its own data dir, or a
/// recovery started with a seed) takes rafka-time from the first node it reaches that holds one.
/// It judges nothing and publishes nothing until it has: `contacts` are the births its durable map
/// names (own mesh first), and the book of the membership it joined adds whoever it hears. A
/// member adopts from whoever answers, so the first reachable node serves it; the lineage descends
/// from the fabric-primary whichever node that is.
///
/// Returns the `path.name` of the node it adopted from, or a named refusal listing each node
/// tried with the outcome of its read.
pub(crate) async fn adopt_from_reachable(
    time: &RafkaTime,
    me: &crate::model::PathName,
    client: &rafka_node_rpc::NodeRpcClient,
    resolver: &rafka_node_rpc::LiveNodeResolver,
    membership: &rafka_mesh_transport::membership::Membership,
    contacts: Vec<crate::reenter::MapNode>,
    within: std::time::Duration,
    parent: &tracing::Span,
) -> Result<String, String> {
    use rafka_node_rpc::NodeTarget;
    let started = std::time::Instant::now();
    let span = tracing::info_span!(parent: parent, "rdm.mesh.entry.resolve.via-recovery-pull", node = %me, contacts = contacts.len(), served_by = tracing::field::Empty, attempts = tracing::field::Empty);
    let outcome = async {
        let mut tried: std::collections::BTreeMap<String, String> = Default::default();
        let mut attempts = 0u32;
        loop {
            let mut candidates = contacts.clone();
            for d in membership.book.current(membership.book.staleness_floor()) {
                if d.node.name != *me && !candidates.iter().any(|c| c.node_id == d.node.node_id) {
                    candidates.push(crate::reenter::MapNode {
                        node_id: d.node.node_id.clone(),
                        name: d.node.name.clone(),
                        endpoint_id: d.node.endpoint_id.clone(),
                        transport_addr: d.node.transport_addr,
                        incarnation: d.node.incarnation.clone(),
                        settled: true,
                        ready: false,
                        data_dir: None,
                        admin_api_base: None,
                    });
                }
            }
            candidates.retain(|c| c.name != *me);
            candidates.sort_by_key(|c| (c.name.mesh != me.mesh, c.name.to_string()));
            for c in &candidates {
                if let Some(r) = c.resolved() {
                    resolver.apply(r, None);
                }
                attempts += 1;
                match crate::topology_read::read_topology(client, &NodeTarget::ExactNode(c.node_id.clone()), &me.to_string(), Some(&c.name.mesh), None).await {
                    Ok(read) => match read.rafka_time {
                        Some(t) => {
                            adopt_pulled(time, Puller::Member, &me.to_string(), Served { via: "get-topology", served_by: &c.name.to_string(), ms: t.ms, seat: t.seat }).expect("a member adopts from whoever answers");
                            return Ok(c.name.to_string());
                        }
                        None => {
                            tried.insert(c.name.to_string(), "its answer carries no rafka-time".into());
                        }
                    },
                    Err(e) => {
                        tried.insert(c.name.to_string(), e.to_string());
                    }
                }
            }
            if started.elapsed() >= within {
                return Err(format!("{me}: no node it could reach holds rafka-time within {} s (tried {} reads of {tried:?})", within.as_secs(), attempts));
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    }
    .instrument(span.clone())
    .await;
    if let Ok(by) = &outcome {
        span.record("served_by", by.as_str());
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_member_adopts_from_whoever_answers() {
        for seat in [None, Some(Seat::MeshPrimary), Some(Seat::FabricPrimary)] {
            assert_eq!(verdict(Puller::Member, seat), Ok(()), "{seat:?}");
        }
    }

    #[test]
    fn a_mesh_primary_adopts_only_from_a_fabric_primary_seat() {
        assert_eq!(verdict(Puller::MeshPrimary, Some(Seat::FabricPrimary)), Ok(()));
        assert_eq!(verdict(Puller::MeshPrimary, None), Err(Refusal::ServedByAReplica));
        assert_eq!(verdict(Puller::MeshPrimary, Some(Seat::MeshPrimary)), Err(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary));
        assert_eq!(Refusal::ServedByAReplica.reason(), "served-by-a-replica");
        assert_eq!(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary.reason(), "a-mesh-primary-takes-only-the-fabric-primary");
    }

    #[test]
    fn a_refused_answer_leaves_the_held_time_unchanged() {
        let time = RafkaTime::unadopted();
        time.adopt(1_000_000);
        let served = Served { via: "get-topology", served_by: "mesh2.admin.2", ms: 9_000_000, seat: None };
        assert_eq!(adopt_pulled(&time, Puller::MeshPrimary, "mesh1.admin.1", served).unwrap_err(), Refusal::ServedByAReplica);
        assert!(time.now_ms() < 2_000_000, "the offered time was not taken");
        let served = Served { seat: Some(Seat::FabricPrimary), ..served };
        adopt_pulled(&time, Puller::MeshPrimary, "mesh1.admin.1", served).unwrap();
        assert!(time.now_ms() >= 9_000_000);
    }

    #[test]
    fn the_day0_root_adopts_its_own_clock_once() {
        let time = RafkaTime::unadopted();
        let adopted = adopt_own_clock(&tracing::Span::none(), &time, "mesh1.admin.1", OWN_CLOCK_DAY0_ROOT);
        assert_eq!(adopted.previous_ms, None);
        assert!(time.now_ms() >= adopted.reference_ms);
    }
}
