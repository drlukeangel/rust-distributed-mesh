//! Elections (`docs/architecture/node-lifecycle-elections.md` in rafka-v2;
//! `docs/i143/design.md` §2.1).
//!
//! An election is a projection of converged topology facts, not a voting
//! protocol: no ballot, term, quorum or election message exists. Every
//! election applies one function: among the candidates that are
//! `ReadyForTraffic`, the one with the lowest complete `NodeId` wins. A
//! canonical NodeId is a fixed-width Crockford value, so comparing the
//! strings compares the values. The id is random; its order means nothing
//! but itself. Ready time, ordinal, path, mesh name, MeshId, FabricId,
//! incarnation, transport identity and incumbency are not election inputs.
//!
//! The hierarchy:
//! - node-type (cohort) election: a cohort is the members of one kind in one
//!   mesh; node-admin owns the elections of its own mesh's cohorts, ordinary
//!   nodes only publish their facts;
//! - mesh primary: the winner of the mesh's node-admin cohort, the same seat
//!   (no second election);
//! - fabric-primary election: the candidates are the mesh primaries, the
//!   function is the same; mesh primaries own it.
//!
//! Every observer holding the same facts computes the same winners. A
//! partition lets each side elect from what it hears; when the views heal
//! they agree again. A restarted node keeps its NodeId and may retake a seat;
//! a replacement has a new NodeId and wins or not by that id.
//!
//! An admin reports each change it owns (`ElectionLog`):
//! `rafka.mesh.election.resolve.via-recompute` (its mesh's cohorts),
//! `rafka.mesh.election.resolve.via-mesh-primary` (its mesh's primary), and,
//! while it is a mesh primary, `rafka.mesh.election.resolve.via-fabric-recompute`: on each change
//! of the fabric seat, and on becoming a mesh primary, when it takes ownership of the seat it was
//! tracking.

use crate::model::{Node, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::ids::NodeId;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// The election key every span names.
pub const ELECTION_KEY: &str = "node_id_crockford";

/// One candidate as the election sees it: its identity, and whether it is
/// eligible (committed `ReadyForTraffic`).
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub node_id: &'a NodeId,
    pub ready: bool,
}

/// The index of the winner among `candidates`: the eligible candidate with
/// the lowest NodeId; `None` when none is eligible.
pub fn elect(candidates: &[Candidate<'_>]) -> Option<usize> {
    (0..candidates.len()).filter(|&i| candidates[i].ready).min_by_key(|&i| candidates[i].node_id)
}

fn candidate(n: &Node) -> Candidate<'_> {
    Candidate { node_id: &n.node_id, ready: n.status == NodeStatus::ReadyForTraffic }
}

/// Mark each cohort's primary and the fabric primary in `nodes`.
pub fn resolve(nodes: &mut [Node]) {
    for n in nodes.iter_mut() {
        n.is_primary = false;
        n.is_fabric_primary = false;
    }
    let mut cohorts: BTreeMap<(String, NodeKind), Vec<usize>> = BTreeMap::new();
    for (i, n) in nodes.iter().enumerate() {
        cohorts.entry((n.mesh.clone(), n.kind)).or_default().push(i);
    }
    let mut winners = Vec::new();
    for members in cohorts.values() {
        let c: Vec<Candidate<'_>> = members.iter().map(|&i| candidate(&nodes[i])).collect();
        if let Some(k) = elect(&c) {
            winners.push(members[k]);
        }
    }
    for &i in &winners {
        nodes[i].is_primary = true;
    }
    // The fabric's candidates: every mesh primary.
    let mesh_primaries: Vec<usize> = winners.into_iter().filter(|&i| nodes[i].kind == NodeKind::NodeAdmin).collect();
    let c: Vec<Candidate<'_>> = mesh_primaries.iter().map(|&i| candidate(&nodes[i])).collect();
    if let Some(k) = elect(&c) {
        nodes[mesh_primaries[k]].is_fabric_primary = true;
    }
}

/// A seat's holder as a span names it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seat {
    node_id: String,
    path: String,
    mesh: String,
}

impl Seat {
    fn of(n: &Node) -> Self {
        Self { node_id: n.node_id.to_string(), path: n.name.to_string(), mesh: n.mesh.clone() }
    }
}

fn field<T>(s: &Option<Seat>, f: impl Fn(&Seat) -> &T) -> String
where
    T: ToString + ?Sized,
{
    s.as_ref().map(|s| f(s).to_string()).unwrap_or_default()
}

/// What one admin last saw of the seats it owns; reports every change.
pub struct ElectionLog {
    /// This admin's path.
    observer: PathName,
    last: Mutex<BTreeMap<NodeKind, Option<Seat>>>,
    /// `None` until the first view.
    last_mesh: Mutex<Option<Option<Seat>>>,
    last_fabric: Mutex<Option<Option<Seat>>>,
    /// Whether this admin was a mesh primary (an owner of the fabric election) at its last view.
    owned_fabric: Mutex<bool>,
}

impl ElectionLog {
    pub fn new(observer: PathName) -> Self {
        Self { observer, last: Mutex::new(BTreeMap::new()), last_mesh: Mutex::new(None), last_fabric: Mutex::new(None), owned_fabric: Mutex::new(false) }
    }

    /// Record `t`'s winners. Each change of a seat this admin owns, and each
    /// first resolution, emits one span.
    pub fn observe(&self, t: &Topology) {
        let mesh = self.observer.mesh.clone();
        let mut last = self.last.lock().unwrap();
        let mut now: BTreeMap<NodeKind, Option<Seat>> = BTreeMap::new();
        for n in t.nodes.iter().filter(|n| n.mesh == mesh) {
            let e = now.entry(n.kind).or_default();
            if n.is_primary {
                *e = Some(Seat::of(n));
            }
        }
        for (kind, winner) in &now {
            let previous = last.get(kind).cloned().flatten();
            if last.contains_key(kind) && previous == *winner {
                continue;
            }
            let candidates = t.cohort(&mesh, *kind).count();
            let eligible = t.cohort(&mesh, *kind).filter(|n| n.status == NodeStatus::ReadyForTraffic).count();
            // The exact inputs: every candidate as this view holds it. Two observers that name
            // different winners differ here, never in the function.
            let inputs: Vec<String> = t.cohort(&mesh, *kind).map(|n| format!("{}={}:{:?}", n.name, n.node_id, n.status)).collect();
            let span = tracing::info_span!(
                parent: None,
                "rafka.mesh.election.resolve.via-recompute",
                election_level = "node_type",
                observer = %self.observer,
                mesh = %mesh,
                kind = kind_name(*kind),
                candidate_count = candidates,
                eligible_count = eligible,
                inputs = %inputs.join(","),
                winner_node_id = %field(winner, |s| &s.node_id),
                winner_path = %field(winner, |s| &s.path),
                election_key = ELECTION_KEY,
                previous_node_id = %field(&previous, |s| &s.node_id),
                primary = %field(winner, |s| &s.path),
                previous = %field(&previous, |s| &s.path),
                members = candidates,
                ready = eligible,
            );
            span.in_scope(|| tracing::info!("cohort primary resolved"));
        }
        *last = now.clone();
        drop(last);

        // The mesh primary is the node-admin cohort's winner: the same seat.
        let mesh_primary = now.get(&NodeKind::NodeAdmin).cloned().flatten();
        let mut last_mesh = self.last_mesh.lock().unwrap();
        if last_mesh.as_ref() != Some(&mesh_primary) {
            let previous = last_mesh.clone().flatten();
            let span = tracing::info_span!(
                parent: None,
                "rafka.mesh.election.resolve.via-mesh-primary",
                election_level = "mesh_primary",
                observer = %self.observer,
                mesh = %mesh,
                winner_node_id = %field(&mesh_primary, |s| &s.node_id),
                winner_path = %field(&mesh_primary, |s| &s.path),
                previous_node_id = %field(&previous, |s| &s.node_id),
                source_kind = "node_admin",
            );
            span.in_scope(|| tracing::info!("mesh primary resolved"));
            *last_mesh = Some(mesh_primary.clone());
        }
        drop(last_mesh);

        // The fabric election is the mesh primaries' to own.
        let fabric = t.fabric_primary().map(Seat::of);
        let mut last_fabric = self.last_fabric.lock().unwrap();
        let owner = mesh_primary.as_ref().is_some_and(|s| s.path == self.observer.to_string());
        let mut owned = self.owned_fabric.lock().unwrap();
        let gained = owner && !*owned;
        *owned = owner;
        drop(owned);
        if owner && (gained || last_fabric.as_ref() != Some(&fabric)) {
            let previous = last_fabric.clone().flatten();
            let candidates: Vec<String> = t.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).collect();
            // The exact inputs: every node-admin of every mesh as this view holds it, with the id
            // the order is decided on and the status the eligibility is decided on.
            let inputs: Vec<String> = t.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin).map(|n| format!("{}={}:{:?}{}", n.name, n.node_id, n.status, if n.is_primary { ":primary" } else { "" })).collect();
            let span = tracing::info_span!(
                parent: None,
                "rafka.mesh.election.resolve.via-fabric-recompute",
                election_level = "fabric_primary",
                observer = %self.observer,
                fabric = %t.fabric.name,
                fabric_id = %t.fabric.id,
                candidate_mesh_primaries = %candidates.join(","),
                inputs = %inputs.join(","),
                winner_node_id = %field(&fabric, |s| &s.node_id),
                winner_path = %field(&fabric, |s| &s.path),
                winner_mesh = %field(&fabric, |s| &s.mesh),
                previous_node_id = %field(&previous, |s| &s.node_id),
                election_key = ELECTION_KEY,
                primary = %field(&fabric, |s| &s.path),
                previous = %field(&previous, |s| &s.path),
                meshes = t.meshes.len(),
            );
            span.in_scope(|| tracing::info!("fabric primary resolved"));
        }
        // A non-owner still tracks the seat; it reports the seat when it becomes an owner and
        // every change after.
        *last_fabric = Some(fabric);
    }
}

fn kind_name(k: NodeKind) -> &'static str {
    match k {
        NodeKind::NodeAdmin => "node_admin",
        NodeKind::RpcNode => "rpc_node",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> NodeId {
        NodeId::parse(s).unwrap()
    }

    fn node(path: &str, node_id: &str, ready: bool) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = id(node_id);
        n.status = if ready { NodeStatus::ReadyForTraffic } else { NodeStatus::Pending };
        n
    }

    fn primaries(nodes: &[Node]) -> Vec<String> {
        nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect()
    }

    fn fabric(nodes: &[Node]) -> Vec<String> {
        nodes.iter().filter(|n| n.is_fabric_primary).map(|n| n.name.to_string()).collect()
    }

    #[test]
    fn lexical_order_of_canonical_node_ids_is_crockford_numeric_order() {
        // Boundaries: 0 < 9 < a < z, at the first and the last place.
        let ordered = ["000000000000", "000000000009", "00000000000a", "00000000000z", "000000000010", "900000000000", "a00000000000", "zzzzzzzzzzzz"];
        for w in ordered.windows(2) {
            assert!(id(w[0]) < id(w[1]), "{} < {}", w[0], w[1]);
            let (a, b) = (rafka_mesh_entity::ids::decode_crockford60(w[0]).unwrap(), rafka_mesh_entity::ids::decode_crockford60(w[1]).unwrap());
            assert!(a < b, "numeric {} < {}", w[0], w[1]);
        }
    }

    #[test]
    fn the_lowest_ready_node_id_wins_and_nothing_else_counts() {
        let (lo, hi) = (id("1aaaaaaaaaaa"), id("1aaaaaaaaaab"));
        assert_eq!(elect(&[Candidate { node_id: &hi, ready: true }, Candidate { node_id: &lo, ready: true }]), Some(1));
        assert_eq!(elect(&[Candidate { node_id: &hi, ready: true }, Candidate { node_id: &lo, ready: false }]), Some(0), "only ready candidates");
        assert_eq!(elect(&[Candidate { node_id: &lo, ready: false }]), None);
        assert_eq!(elect(&[]), None);
    }

    #[test]
    fn ordinal_and_mesh_name_never_decide_a_seat() {
        // rpc.3 and the highest-named mesh hold the lowest ids.
        let mut nodes = vec![
            node("mesh1.admin.1", "z00000000001", true),
            node("mesh1.rpc.1", "z00000000002", true),
            node("mesh1.rpc.3", "100000000000", true),
            node("mesh3.admin.2", "000000000005", true),
            node("mesh3.admin.1", "000000000006", true),
            node("mesh2.admin.1", "500000000000", true),
            node("mesh2.rpc.1", "000000000001", false),
            node("mesh2.rpc.2", "600000000000", true),
        ];
        resolve(&mut nodes);
        let mut p = primaries(&nodes);
        p.sort();
        assert_eq!(p, ["mesh1.admin.1", "mesh1.rpc.3", "mesh2.admin.1", "mesh2.rpc.2", "mesh3.admin.2"]);
        assert_eq!(fabric(&nodes), ["mesh3.admin.2"], "the lowest mesh-primary NodeId holds the fabric");
    }

    #[test]
    fn losing_a_winner_moves_each_seat_to_the_next_lowest() {
        let mut nodes = vec![
            node("mesh1.admin.1", "300000000000", true),
            node("mesh1.admin.2", "100000000000", true),
            node("mesh1.admin.3", "200000000000", true),
            node("mesh2.admin.1", "150000000000", true),
        ];
        resolve(&mut nodes);
        assert_eq!(fabric(&nodes), ["mesh1.admin.2"]);
        nodes[1].status = NodeStatus::Dead;
        resolve(&mut nodes);
        assert_eq!(primaries(&nodes), ["mesh1.admin.3", "mesh2.admin.1"]);
        assert_eq!(fabric(&nodes), ["mesh2.admin.1"], "the fabric candidates are the mesh primaries only");
        // The same node back under its NodeId retakes both seats once ready.
        nodes[1].status = NodeStatus::ReadyForTraffic;
        resolve(&mut nodes);
        assert_eq!(fabric(&nodes), ["mesh1.admin.2"]);
    }

    #[test]
    fn one_admin_holds_every_seat_on_day_0() {
        let mut nodes = vec![node("mesh1.admin.1", "k00000000000", true)];
        resolve(&mut nodes);
        assert!(nodes[0].is_primary && nodes[0].is_fabric_primary);
    }

    #[test]
    fn every_observer_of_the_same_facts_resolves_the_same_winners() {
        let base = vec![
            node("mesh1.admin.1", "300000000000", true),
            node("mesh1.admin.2", "100000000000", true),
            node("mesh2.admin.1", "200000000000", true),
            node("mesh2.rpc.1", "400000000000", true),
            node("mesh2.rpc.2", "050000000000", true),
        ];
        let mut a = base.clone();
        let mut b: Vec<Node> = base.into_iter().rev().collect();
        resolve(&mut a);
        resolve(&mut b);
        let (mut pa, mut pb) = (primaries(&a), primaries(&b));
        pa.sort();
        pb.sort();
        assert_eq!(pa, pb);
        assert_eq!(fabric(&a), fabric(&b));
    }
}
