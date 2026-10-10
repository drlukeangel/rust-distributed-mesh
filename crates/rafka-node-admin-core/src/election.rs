//! Elections (`docs/architecture/node-lifecycle-elections.md` in rafka-v2; ruling R-A2).
//!
//! An election is a projection of converged facts, not a voting protocol: no ballot, term, quorum,
//! timer or election message exists. A seat stays with its holder until that holder is proven
//! unable to hold it. The facts an election takes are the topology's nodes and their statuses, the
//! HOLDER RECORDS this observer holds ([`Incumbency`]: a seat's exact birth, gossiped when a seat
//! changes hands and handed to a joiner in its entry), and the births proven gone.
//!
//! The rules, one function ([`resolve_with`]):
//! - A holder that lives keeps its seat. A lower NodeId never displaces it. Lives means the record's
//!   exact birth is in the view and is not yielding (`Draining`, `Leaving`, `Restarting`), not
//!   replaced by a later birth of its NodeId, and not proven gone. Silent (`PendingReconnect`),
//!   an observer-inferred `Dead` and an unheard route are NOT proof: the holder keeps the seat.
//! - A holder whose record names a birth this view has not heard keeps its seat vacant for it.
//! - A holder that yields, or whose exact birth is proven gone, hands the seat to its own mesh:
//!   the lowest `ReadyForTraffic` NodeId among that mesh's node-admins fills it. A canonical
//!   NodeId is a fixed-width Crockford value, so comparing the strings compares the values; the id
//!   is random and its order means nothing but itself. If only Pending admins survive, the seat
//!   waits and the mesh keeps priority.
//! - The fabric seat leaves the incumbent mesh only when EVERY node-admin birth of that mesh is
//!   proven gone (or has left). Then the lowest NodeId among the remaining mesh primaries wins and
//!   its mesh is the incumbent. Nothing reclaims the seat for the mesh that lost it.
//! - A seat with no record (a fabric's first election) is filled by the lowest Ready NodeId.
//!
//! Ready time, ordinal, path, mesh name, MeshId, FabricId, incarnation order and transport identity
//! are not election inputs. The other cohorts (rpc, gateway, ...) are elected by the lowest Ready
//! NodeId alone.
//!
//! The hierarchy:
//! - node-type (cohort) election: a cohort is the members of one kind in one mesh; node-admin owns
//!   the elections of its own mesh's cohorts, ordinary nodes only publish their facts;
//! - mesh primary: the holder of the mesh's node-admin cohort, the same seat (no second election);
//! - fabric-primary election: the candidates are the mesh primaries, mesh primaries own it.
//!
//! Every observer holding the same records and facts computes the same winners; an observer that
//! lacks a record learns it from the new holder's announcement, from an entry read, or from a
//! neighbour's replay. A restarted node keeps its NodeId but is a later birth: the seat it held is
//! filled by its mesh, and it holds none until a vacancy finds it the lowest Ready.
//!
//! An admin reports each change it owns (`ElectionLog`):
//! `rdm.mesh.election.resolve.via-recompute` (its mesh's cohorts),
//! `rdm.mesh.election.resolve.via-mesh-primary` (its mesh's primary), and,
//! while it is a mesh primary, `rdm.mesh.election.resolve.via-fabric-recompute`: on each change
//! of the fabric seat, and on becoming a mesh primary, when it takes ownership of the seat it was
//! tracking.

use crate::model::{Node, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::ids::{IncarnationId, NodeId};
use rafka_mesh_entity::SeatHolder;
use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

/// The election key every span names.
pub const ELECTION_KEY: &str = "node_id_crockford";

/// One candidate as the election sees it: its identity, and whether it is
/// eligible (committed `ReadyForTraffic`).
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    /// The candidate's node id.
    pub node_id: &'a NodeId,
    /// Whether the candidate is eligible.
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

/// What an election takes beside the nodes: the seat holders this observer holds, and the births
/// proven gone.
#[derive(Debug, Clone, Default)]
pub struct Incumbency {
    /// The fabric primary's holder record.
    pub fabric: Option<SeatHolder>,
    /// Each mesh's node-admin primary holder record, by mesh name.
    pub meshes: BTreeMap<String, SeatHolder>,
    /// Exact births proven gone, by incarnation (an incarnation names one birth of one node): the
    /// provider found their runtime exited, a departure was heard, or a later birth announced it
    /// replaced them. Silence, an unreachable path and an observer-inferred `Dead` are never in it.
    pub lost: HashSet<IncarnationId>,
}

impl Incumbency {
    fn is_lost(&self, n: &Node) -> bool {
        n.incarnation_id.as_ref().is_some_and(|i| self.lost.contains(i))
    }
}

/// What a view shows of a seat's recorded holder.
enum Holding {
    /// The exact birth is in the view and keeps the seat.
    Living(usize),
    /// The birth yields, was replaced by a later one, or is proven gone.
    Yielded,
    /// The view has not heard the birth the record names.
    Unseen,
}

fn holding(nodes: &[Node], h: &SeatHolder, inc: &Incumbency) -> Holding {
    if inc.lost.contains(&h.incarnation) {
        return Holding::Yielded;
    }
    let Some(i) = nodes.iter().position(|n| n.node_id == h.node_id) else { return Holding::Unseen };
    let n = &nodes[i];
    if n.incarnation_id.as_ref().is_some_and(|inc| *inc != h.incarnation) {
        return Holding::Yielded;
    }
    match n.status {
        NodeStatus::Draining | NodeStatus::Leaving | NodeStatus::Restarting => Holding::Yielded,
        _ => Holding::Living(i),
    }
}

/// The lowest Ready node among `members` that is not proven gone.
fn lowest_ready(nodes: &[Node], members: &[usize], inc: &Incumbency) -> Option<usize> {
    let c: Vec<Candidate<'_>> = members.iter().map(|&i| Candidate { node_id: &nodes[i].node_id, ready: nodes[i].status == NodeStatus::ReadyForTraffic && !inc.is_lost(&nodes[i]) }).collect();
    elect(&c).map(|k| members[k])
}

/// Mark each cohort's primary and the fabric primary in `nodes`, with no holder record held: a
/// fabric's first election, and a hypothetical (a successor asked for before any drain).
pub fn resolve(nodes: &mut [Node]) {
    resolve_with(nodes, &Incumbency::default())
}

/// Mark each cohort's primary and the fabric primary in `nodes` by the rules in the module docs,
/// given the seat holders held and the births proven gone. `nodes` are members: a replaced
/// predecessor (`<path>.old`) is not passed (`Topology::members`).
pub fn resolve_with(nodes: &mut [Node], inc: &Incumbency) {
    for n in nodes.iter_mut() {
        n.is_primary = false;
        n.is_fabric_primary = false;
    }
    let mut cohorts: BTreeMap<(String, NodeKind), Vec<usize>> = BTreeMap::new();
    for (i, n) in nodes.iter().enumerate() {
        cohorts.entry((n.mesh.clone(), n.kind)).or_default().push(i);
    }
    // Mesh primary of each mesh: the node-admin cohort's seat. Every other cohort: lowest Ready.
    let mut winners = Vec::new();
    let mut mesh_primary: BTreeMap<String, usize> = BTreeMap::new();
    for ((mesh, kind), members) in &cohorts {
        let winner = if *kind == NodeKind::NodeAdmin {
            match inc.meshes.get(mesh) {
                Some(h) => match holding(nodes, h, inc) {
                    Holding::Living(i) if nodes[i].mesh == *mesh && nodes[i].kind == NodeKind::NodeAdmin => Some(i),
                    Holding::Unseen => None,
                    _ => lowest_ready(nodes, members, inc),
                },
                None => lowest_ready(nodes, members, inc),
            }
        } else {
            lowest_ready(nodes, members, inc)
        };
        if let Some(i) = winner {
            winners.push(i);
            if *kind == NodeKind::NodeAdmin {
                mesh_primary.insert(mesh.clone(), i);
            }
        }
    }
    for &i in &winners {
        nodes[i].is_primary = true;
    }
    if let Some(i) = fabric_seat(nodes, inc, &mesh_primary) {
        nodes[i].is_fabric_primary = true;
    }
}

/// The fabric seat: the recorded holder while it lives, its own mesh while that mesh has an admin
/// that is not proven gone, another mesh only once the whole incumbent mesh is.
fn fabric_seat(nodes: &[Node], inc: &Incumbency, mesh_primary: &BTreeMap<String, usize>) -> Option<usize> {
    let lowest_primary = |except: Option<&str>| {
        let members: Vec<usize> = mesh_primary.iter().filter(|(m, _)| Some(m.as_str()) != except).map(|(_, &i)| i).collect();
        lowest_ready(nodes, &members, inc)
    };
    let Some(h) = inc.fabric.as_ref() else { return lowest_primary(None) };
    match holding(nodes, h, inc) {
        Holding::Living(i) if mesh_primary.get(&nodes[i].mesh) == Some(&i) => return Some(i),
        Holding::Unseen => return None,
        _ => {}
    }
    // The holder yielded, or is no longer its mesh's primary. Its mesh keeps priority while any
    // node-admin birth of it is not proven gone or departed.
    let survives = |n: &Node| n.kind == NodeKind::NodeAdmin && n.mesh == h.mesh && !inc.is_lost(n) && n.status != NodeStatus::Leaving && !(n.node_id == h.node_id && n.incarnation_id.as_ref() == Some(&h.incarnation));
    if nodes.iter().any(survives) {
        return mesh_primary.get(&h.mesh).copied().filter(|&i| nodes[i].status == NodeStatus::ReadyForTraffic && !inc.is_lost(&nodes[i]) && !(nodes[i].node_id == h.node_id && nodes[i].incarnation_id.as_ref() == Some(&h.incarnation)));
    }
    lowest_primary(Some(h.mesh.as_str()))
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
    /// A log for the admin `observer`.
    pub fn new(observer: PathName) -> Self {
        Self { observer, last: Mutex::new(BTreeMap::new()), last_mesh: Mutex::new(None), last_fabric: Mutex::new(None), owned_fabric: Mutex::new(false) }
    }

    /// Record `t`'s winners. Each change of a seat this admin owns, and each
    /// first resolution, emits one span.
    pub fn observe(&self, t: &Topology) {
        let mesh = self.observer.mesh.clone();
        let mut last = self.last.lock().unwrap();
        let mut now: BTreeMap<NodeKind, Option<Seat>> = BTreeMap::new();
        for n in t.members().filter(|n| n.mesh == mesh) {
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
                "rdm.mesh.election.resolve.via-recompute",
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
                "rdm.mesh.election.resolve.via-mesh-primary",
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
            let candidates: Vec<String> = t.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).collect();
            // The exact inputs: every node-admin of every mesh as this view holds it, with the id
            // the order is decided on and the status the eligibility is decided on.
            let inputs: Vec<String> = t.members().filter(|n| n.kind == NodeKind::NodeAdmin).map(|n| format!("{}={}:{:?}{}", n.name, n.node_id, n.status, if n.is_primary { ":primary" } else { "" })).collect();
            let span = tracing::info_span!(
                parent: None,
                "rdm.mesh.election.resolve.via-fabric-recompute",
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
    k.name()
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

    /// CONTRACT: a replaced predecessor (`<path>.old`) holds no seat and is no candidate, even when its
    /// digest still reads ready and its NodeId is the lowest: the successor at `<path>` is the cohort's.
    #[test]
    fn a_renamed_predecessor_is_never_a_candidate_or_a_primary() {
        let mut old = node("mesh1.rpc.1", "000000000001", true);
        old.name = old.name.renamed();
        let successor = node("mesh1.rpc.1", "000000000009", true);
        let view = crate::topology::Topology {
            fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "f".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: vec![],
            nodes: vec![old, successor, node("mesh1.admin.1", "000000000005", true)],
        };
        // An election takes the view's members: the renamed predecessor is not among them.
        let mut members: Vec<Node> = view.members().cloned().collect();
        resolve(&mut members);
        assert_eq!(members.len(), 2);
        assert_eq!(primaries(&members), vec!["mesh1.rpc.1".to_string(), "mesh1.admin.1".to_string()]);
        assert_eq!(view.births().count(), 3, "the predecessor is still a birth the view holds");
        assert_eq!(view.predecessors().count(), 1);
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

    /// CONTRACT (i143 export gate: provider control domain, locality and runtime metadata are
    /// never election keys): candidates on another provider, data dir, deployment, transport
    /// address or admin API elect exactly as the NodeIds alone decide.
    #[test]
    fn provider_locality_and_runtime_metadata_never_decide_a_seat() {
        let mut nodes = vec![
            node("mesh1.admin.1", "300000000000", true),
            node("mesh1.admin.2", "100000000000", true),
            node("mesh1.admin.3", "200000000000", true),
        ];
        let plain = {
            let mut n = nodes.clone();
            resolve(&mut n);
            (primaries(&n), fabric(&n))
        };
        for (i, n) in nodes.iter_mut().enumerate() {
            n.provider = Some(if i == 1 { crate::model::ProviderKind::Container } else { crate::model::ProviderKind::Process });
            n.data_dir = Some(format!("/srv/domain-{}/{}", 9 - i, n.name));
            n.deployment_id = Some(crate::model::DeploymentId::mint());
            n.transport_addr = Some(std::net::SocketAddr::from(([10, 0, i as u8, 1], 40000 - i as u16)));
            n.admin_api_base = Some(format!("http://host-{}:{}", 9 - i, 9000 + i));
        }
        resolve(&mut nodes);
        assert_eq!((primaries(&nodes), fabric(&nodes)), plain, "the same seats with or without runtime metadata");
        assert_eq!(fabric(&nodes), ["mesh1.admin.2"], "the lowest ready NodeId, on a container, still holds the seat");
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
        nodes[1].status = NodeStatus::PendingReconnect;
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

    // ---- Sticky seats (R-A2): a living holder keeps its seat; the holder record is an input. ----

    fn born(path: &str, node_id: &str, inc: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = id(node_id);
        n.incarnation_id = Some(IncarnationId(inc.into()));
        n.status = status;
        n
    }

    fn holder(mesh: &str, node_id: &str, inc: &str, epoch: u64) -> SeatHolder {
        SeatHolder { mesh: mesh.into(), node_id: id(node_id), incarnation: IncarnationId(inc.into()), epoch }
    }

    fn incumbency(fabric: SeatHolder, meshes: &[SeatHolder]) -> Incumbency {
        Incumbency { fabric: Some(fabric), meshes: meshes.iter().map(|h| (h.mesh.clone(), h.clone())).collect(), lost: HashSet::new() }
    }

    fn lose(inc: &mut Incumbency, _node_id: &str, incarnation: &str) {
        inc.lost.insert(IncarnationId(incarnation.into()));
    }

    fn fabric_name(nodes: &[Node]) -> Option<String> {
        nodes.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string())
    }

    fn mesh_primary(nodes: &[Node], mesh: &str) -> Option<String> {
        nodes.iter().find(|n| n.is_primary && n.mesh == mesh && n.kind == NodeKind::NodeAdmin).map(|n| n.name.to_string())
    }

    use NodeStatus::{Draining, Leaving, Pending, PendingReconnect, ReadyForTraffic as Ready, Restarting};

    /// CONTRACT (R-A2 cell 1): an admin with a lower NodeId appearing in another mesh leaves the
    /// fabric primary where it is. Must NOT happen: the lower id taking the seat.
    #[test]
    fn a_lower_node_id_in_another_mesh_never_displaces_the_fabric_primary() {
        let inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", Ready), born("mesh3.admin.1", "100000000000", "c1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"));
        assert_eq!(mesh_primary(&nodes, "mesh3").as_deref(), Some("mesh3.admin.1"), "the other mesh still has its own primary");
    }

    /// CONTRACT (R-A2 cell 2): a lower NodeId appearing inside the incumbent mesh leaves the
    /// healthy holder as the mesh primary and the fabric primary.
    #[test]
    fn a_lower_node_id_inside_the_incumbent_mesh_never_displaces_the_healthy_holder() {
        let inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", Ready), born("mesh1.admin.2", "100000000000", "a2", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"));
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"));
    }

    /// CONTRACT (R-A2 cell 3): the holder's exact birth is proven lost with another admin
    /// surviving in its mesh: the lowest Ready NodeId of THAT mesh fills the seat, not a lower id
    /// of another mesh.
    #[test]
    fn a_lost_holder_is_replaced_by_the_lowest_ready_admin_of_its_own_mesh() {
        let mut inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        lose(&mut inc, "500000000000", "a1");
        let mut nodes = vec![
            born("mesh1.admin.1", "500000000000", "a1", PendingReconnect),
            born("mesh1.admin.2", "800000000000", "a2", Ready),
            born("mesh1.admin.3", "700000000000", "a3", Ready),
            born("mesh2.admin.1", "100000000000", "b1", Ready),
        ];
        resolve_with(&mut nodes, &inc);
        assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.3"));
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.3"), "the lowest Ready NodeId of the SAME mesh");
    }

    /// CONTRACT (R-A2 cell 4): only Pending admins survive in the incumbent mesh: the seat waits
    /// and no other mesh takes it; the first of them to be Ready fills it.
    #[test]
    fn a_seat_waits_for_a_ready_admin_of_the_incumbent_mesh_while_only_pending_ones_survive() {
        let mut inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        lose(&mut inc, "500000000000", "a1");
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", PendingReconnect), born("mesh1.admin.2", "800000000000", "a2", Pending), born("mesh2.admin.1", "100000000000", "b1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes), None, "no fabric primary while the incumbent mesh has only Pending admins");
        assert_eq!(mesh_primary(&nodes, "mesh1"), None);
        nodes[1].status = Ready;
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.2"));
    }

    /// CONTRACT (R-A2 cell 7): every node-admin birth of the incumbent mesh is proven lost: the
    /// lowest NodeId among the remaining mesh primaries wins.
    #[test]
    fn the_lowest_remaining_mesh_primary_wins_when_every_admin_of_the_incumbent_mesh_is_lost() {
        let mut inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        lose(&mut inc, "500000000000", "a1");
        lose(&mut inc, "600000000000", "a2");
        let mut nodes = vec![
            born("mesh1.admin.1", "500000000000", "a1", PendingReconnect),
            born("mesh1.admin.2", "600000000000", "a2", PendingReconnect),
            born("mesh2.admin.1", "900000000000", "b1", Ready),
            born("mesh3.admin.1", "700000000000", "c1", Ready),
            born("mesh3.admin.2", "100000000000", "c2", Ready),
        ];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh3.admin.2"), "the lowest NodeId among mesh2 and mesh3's primaries");
    }

    /// CONTRACT (R-A2 cell 8): one incumbent admin still runs, Pending or silent included: no
    /// other mesh takes the seat.
    #[test]
    fn one_surviving_incumbent_admin_blocks_a_cross_mesh_takeover() {
        for survivor in [Pending, PendingReconnect, Restarting] {
            let mut inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
            lose(&mut inc, "500000000000", "a1");
            let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", PendingReconnect), born("mesh1.admin.2", "600000000000", "a2", survivor), born("mesh2.admin.1", "100000000000", "b1", Ready)];
            resolve_with(&mut nodes, &inc);
            assert_eq!(fabric_name(&nodes), None, "{survivor:?} survives in the incumbent mesh");
        }
    }

    /// CONTRACT (R-A2 cell 10): the old mesh recovers after another mesh took the seat: the new
    /// holder keeps it, whatever ids the recovered admins hold.
    #[test]
    fn the_old_mesh_recovering_never_reclaims_the_seat() {
        let inc = incumbency(holder("mesh3", "700000000000", "c1", 2), &[holder("mesh3", "700000000000", "c1", 1), holder("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", Ready), born("mesh3.admin.1", "700000000000", "c1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh3.admin.1"));
        assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"), "the old mesh still has its own mesh primary");
    }

    /// CONTRACT (R-A2 cell 12): two admins survive in the incumbent mesh when the holder dies:
    /// exactly one is the primary, the lowest Ready NodeId, on every observer's order of facts.
    #[test]
    fn two_survivors_of_the_incumbent_mesh_yield_exactly_one_holder() {
        let mut inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        lose(&mut inc, "500000000000", "a1");
        let base = vec![born("mesh1.admin.1", "500000000000", "a1", PendingReconnect), born("mesh1.admin.2", "800000000000", "a2", Ready), born("mesh1.admin.3", "700000000000", "a3", Ready)];
        let mut reversed: Vec<Node> = base.iter().cloned().rev().collect();
        let mut forward = base;
        resolve_with(&mut forward, &inc);
        resolve_with(&mut reversed, &inc);
        for nodes in [&forward, &reversed] {
            assert_eq!(nodes.iter().filter(|n| n.is_fabric_primary).count(), 1);
            assert_eq!(fabric_name(nodes).as_deref(), Some("mesh1.admin.3"));
        }
    }

    /// CONTRACT (R-A2 cell 6, election side): a holder that is silent but not proven lost keeps
    /// the seat; observer-inferred `Dead` is not proof either. Nobody else takes it.
    #[test]
    fn a_silent_or_inferred_dead_holder_keeps_the_seat() {
        for status in [PendingReconnect, NodeStatus::Dead] {
            let inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
            let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", status), born("mesh1.admin.2", "600000000000", "a2", Ready), born("mesh2.admin.1", "100000000000", "b1", Ready)];
            resolve_with(&mut nodes, &inc);
            assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"), "{status:?}");
            assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"), "{status:?}");
        }
    }

    /// CONTRACT (R-A2): a holder that yields (Draining, Leaving, Restarting) or whose birth a
    /// later birth of the same NodeId replaced no longer holds; its mesh fills the seat.
    #[test]
    fn a_yielding_or_replaced_holder_hands_the_seat_to_its_own_mesh() {
        let inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        for yielded in [Draining, Leaving, Restarting] {
            let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", yielded), born("mesh1.admin.2", "800000000000", "a2", Ready), born("mesh2.admin.1", "100000000000", "b1", Ready)];
            resolve_with(&mut nodes, &inc);
            assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.2"), "{yielded:?}");
        }
        // The same NodeId, a later birth: the recorded birth is gone.
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a9", Pending), born("mesh1.admin.2", "800000000000", "a2", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.2"));
    }

    /// CONTRACT (R-A2): a holder record names a birth this view has not heard: the seat waits for
    /// it. A joiner that knows the record but not yet the holder's digest takes nothing.
    #[test]
    fn a_holder_this_view_has_not_heard_keeps_its_seat_vacant_for_it() {
        let inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh2.admin.1", "100000000000", "b1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes), None);
    }

    /// CONTRACT (R-A2): mesh-primary seats follow the same rule in every mesh: a living holder
    /// keeps it against a lower id; a vacancy is filled by the lowest Ready NodeId.
    #[test]
    fn a_mesh_primary_seat_follows_the_same_rule_in_every_mesh() {
        let inc = incumbency(holder("mesh1", "500000000000", "a1", 1), &[holder("mesh1", "500000000000", "a1", 1), holder("mesh2", "900000000000", "b1", 1)]);
        let mut nodes = vec![
            born("mesh1.admin.1", "500000000000", "a1", Ready),
            born("mesh2.admin.1", "900000000000", "b1", Ready),
            born("mesh2.admin.2", "200000000000", "b2", Ready),
        ];
        resolve_with(&mut nodes, &inc);
        assert_eq!(mesh_primary(&nodes, "mesh2").as_deref(), Some("mesh2.admin.1"), "the living holder of a non-fabric mesh keeps its seat too");
        // With no record for a mesh, the lowest Ready NodeId fills it.
        let mut fresh = vec![born("mesh2.admin.1", "900000000000", "b1", Ready), born("mesh2.admin.2", "200000000000", "b2", Ready)];
        resolve_with(&mut fresh, &Incumbency::default());
        assert_eq!(mesh_primary(&fresh, "mesh2").as_deref(), Some("mesh2.admin.2"));
    }
}
