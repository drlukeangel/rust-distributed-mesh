//! Elections (`docs/architecture/fabric-node-lifecycle-elections.md`; ruling R-A2).
//!
//! An election is a projection of converged facts, not a voting protocol: no ballot, term, quorum,
//! timer or election message exists. A seat stays with its holder until that holder yields, is known gone or
//! unheard past the staleness floor. The facts an election takes are the topology's nodes and their statuses, the
//! HOLDER RECORDS this observer holds ([`Incumbency`]: a seat's exact birth, gossiped when a seat
//! changes hands and handed to a joiner in its entry), and the births known gone.
//!
//! The rules, one function ([`resolve_with`]):
//! - A holder that lives keeps its seat. A lower NodeId never displaces it. Lives means the record's
//!   exact birth is in the view, not replaced by a later birth of its NodeId, and not known gone,
//!   whatever its status: a `ConnectionsDeleting` or `Leaving` holder keeps the seat until its
//!   explicit yield (a new holder record, PrimaryUpdated) is held (R-EL1). A holder this view holds
//!   unheard past the staleness floor (`PendingReconnect`, or an observer-inferred `Dead`) is superseded
//!   (elections §5: "silence past the staleness floor supersedes it").
//! - A holder whose record names a birth this view has not heard keeps its seat vacant for it.
//! - A holder that yields, is known gone or is unheard past the floor hands the seat to its own
//!   mesh: the lowest `Ready` NodeId among that mesh's nodes of its type fills it. A
//!   canonical NodeId is a fixed-width Crockford value, so comparing the strings compares the values;
//!   the id is random and its order means nothing but itself.
//! - One exception, fabric recovery (§3, Q122): while the fabric record names a birth that is not a
//!   heard holder, nobody is fabric-primary and no node-admin of that mesh is elected; the mesh's other
//!   types elect as ever. The workflow restores the node re-created at the target's path.name, whose
//!   record then names it ([`fabric_recovery_target`], [`fabric_recovery_executor`]).
//! - A seat with no record (a fabric's first election) is filled by the lowest Ready NodeId.
//!
//! Ready time, ordinal, path, mesh name, MeshId, FabricId, incarnation order and transport identity
//! are not election inputs. Every other node type (rpc, gateway, ...) is elected by the same rules over
//! its own holder record (`Incumbency::node_type_primaries`), which its mesh's primary writes (R-MT1 Q3).
//!
//! The hierarchy:
//! - node-type election: the nodes of one kind in one mesh; node-admin owns
//!   the elections of its own mesh's node types, ordinary nodes only publish their facts;
//! - mesh primary: the holder of the mesh's node-admin primary, the same seat (no second election);
//! - fabric-primary election: the candidates are the mesh primaries, mesh primaries own it.
//!
//! Every observer holding the same records and facts computes the same winners; an observer that
//! lacks a record learns it from the new holder's announcement, from an entry read, or from a
//! neighbour's replay. A restarted node keeps its NodeId but is a later birth: the seat it held is
//! vacant, and it holds none until a vacancy finds it the lowest Ready.
//!
//! An admin reports each change it owns (`ElectionLog`):
//! `rdm.mesh.election.resolve.via-recompute` (its mesh's node types),
//! `rdm.mesh.election.resolve.via-mesh-primary` (its mesh's primary), and,
//! while it is a mesh primary, `rdm.mesh.election.resolve.via-fabric-recompute`: on each change
//! of the fabric seat, and on becoming a mesh primary, when it takes ownership of the seat it was
//! tracking.

use crate::model::{Node, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::ids::{IncarnationId, NodeId};
use rdm_core::Primary;
use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

/// The election key every span names.
pub const ELECTION_KEY: &str = "node_id_crockford";

pub use rdm_core::election::{elect, no_secondary, Candidate};

/// What an election takes beside the nodes: the seat holders this observer holds, and the births
/// known gone.
#[derive(Debug, Clone, Default)]
pub struct Incumbency {
    /// The fabric primary's holder record.
    pub fabric: Option<Primary>,
    /// Each mesh's node-admin primary holder record, by mesh name.
    pub meshes: BTreeMap<String, Primary>,
    /// The holder record of every other (mesh, node type) node-type primary seat, by (mesh, kind)
    /// (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part B1). Written by
    /// the mesh's primary, the mesh's one row writer (R-MT1 Q3).
    pub node_type_primaries: BTreeMap<(String, NodeKind), Primary>,
    /// Exact births known gone, by incarnation (an incarnation names one birth of one node): a
    /// departure was heard, or a later birth announced it replaced them.
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
    /// The birth was replaced by a later one, is known gone, or is unheard past the staleness floor
    /// (elections §5).
    Yielded,
    /// The view has not heard the birth the record names.
    Unseen,
}

fn holding(nodes: &[Node], h: &Primary, inc: &Incumbency) -> Holding {
    if inc.lost.contains(&h.incarnation) {
        return Holding::Yielded;
    }
    let Some(i) = nodes.iter().position(|n| n.node_id == h.node_id) else { return Holding::Unseen };
    let n = &nodes[i];
    if n.incarnation_id.as_ref().is_some_and(|inc| *inc != h.incarnation) || matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead) {
        return Holding::Yielded;
    }
    Holding::Living(i)
}

/// The fabric-primary record fabric recovery restores (elections §3, Q122): the fabric record, while
/// the birth it names is not a heard holder in `nodes` (unheard past the floor, known gone, replaced or
/// not in the view). `None` with no record, or while its holder is heard.
pub fn fabric_recovery_target<'a>(nodes: &[Node], inc: &'a Incumbency) -> Option<&'a Primary> {
    inc.fabric.as_ref().filter(|h| !matches!(holding(nodes, h, inc), Holding::Living(_)))
}

/// Who runs fabric recovery for `target` in its own mesh (fabric-recovery.md step 2, Q122): the lowest
/// Ready node-admin of `target`'s mesh other than the target's birth and the node at its
/// path.name `at`, the one the workflow re-creates. `None` when the mesh has no such admin: the backbone
/// arm, run from mesh investigation's decision step. The executor takes no primary role.
pub fn fabric_recovery_executor<'a>(nodes: &'a [Node], inc: &Incumbency, target: &Primary, at: Option<&PathName>) -> Option<&'a Node> {
    let members: Vec<usize> = nodes.iter().enumerate().filter(|(_, n)| n.mesh == target.mesh && n.kind == NodeKind::NodeAdmin && n.node_id != target.node_id && Some(&n.name) != at).map(|(i, _)| i).collect();
    lowest_ready(nodes, &members, inc).map(|i| &nodes[i])
}

/// The lowest Ready node among `members` that is not known gone.
fn lowest_ready(nodes: &[Node], members: &[usize], inc: &Incumbency) -> Option<usize> {
    let c: Vec<Candidate<'_>> = members.iter().map(|&i| Candidate { node_id: &nodes[i].node_id, ready: nodes[i].status == NodeStatus::Ready && !inc.is_lost(&nodes[i]) }).collect();
    elect(&c).map(|k| members[k])
}

/// Mark each node type's primary and the fabric primary in `nodes`, with no holder record held: a
/// fabric's first election, and a hypothetical (a successor asked for before any connections delete).
pub fn resolve(nodes: &mut [Node]) {
    resolve_with(nodes, &Incumbency::default())
}

/// Mark each node type's primary and the fabric primary in `nodes` by the rules in the module docs,
/// given the seat holders held and the births known gone. `nodes` are members: a replaced
/// predecessor (`<path>.old`) is not passed (`Topology::members`).
pub fn resolve_with(nodes: &mut [Node], inc: &Incumbency) {
    for n in nodes.iter_mut() {
        n.is_primary = false;
        n.is_fabric_primary = false;
    }
    // Fabric recovery restores the fabric-primary (§3, Q122): while it runs, no node-admin of that mesh is
    // elected and nobody is fabric-primary; the workflow restores the node re-created at the target's
    // path.name, and its record names it.
    let recovering = fabric_recovery_target(nodes, inc).map(|h| h.mesh.clone());
    let mut node_types: BTreeMap<(String, NodeKind), Vec<usize>> = BTreeMap::new();
    for (i, n) in nodes.iter().enumerate() {
        node_types.entry((n.mesh.clone(), n.kind)).or_default().push(i);
    }
    // Every node type by the one rule: its recorded holder while it lives, vacant while the view has not
    // heard the recorded birth, else the lowest Ready. The node-admin type's seat is the mesh primary.
    let mut winners = Vec::new();
    let mut mesh_primary: BTreeMap<String, usize> = BTreeMap::new();
    for ((mesh, kind), members) in &node_types {
        if *kind == NodeKind::NodeAdmin && recovering.as_deref() == Some(mesh.as_str()) {
            continue;
        }
        let record = if *kind == NodeKind::NodeAdmin { inc.meshes.get(mesh) } else { inc.node_type_primaries.get(&(mesh.clone(), *kind)) };
        let winner = match record {
            Some(h) => match holding(nodes, h, inc) {
                Holding::Living(i) if nodes[i].kind == *kind => (nodes[i].mesh == *mesh).then_some(i),
                Holding::Unseen => None,
                _ => lowest_ready(nodes, members, inc),
            },
            None => lowest_ready(nodes, members, inc),
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
    if let Some(i) = elected_fabric_primary(nodes, inc, &mesh_primary).filter(|_| recovering.is_none()) {
        nodes[i].is_fabric_primary = true;
    }
}

/// The fabric seat, while no fabric recovery runs: the recorded holder while it lives and is its mesh's
/// primary; with no record, the lowest Ready mesh primary.
fn elected_fabric_primary(nodes: &[Node], inc: &Incumbency, mesh_primary: &BTreeMap<String, usize>) -> Option<usize> {
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
    // The holder lives but is not its mesh's primary in this view (a record this view has not caught up
    // with): its mesh keeps priority while any node-admin birth of it is not known gone or Leaving.
    let survives = |n: &Node| n.kind == NodeKind::NodeAdmin && n.mesh == h.mesh && !inc.is_lost(n) && n.status != NodeStatus::Leaving;
    if nodes.iter().any(survives) {
        return mesh_primary.get(&h.mesh).copied().filter(|&i| nodes[i].status == NodeStatus::Ready && !inc.is_lost(&nodes[i]) && !(nodes[i].node_id == h.node_id && nodes[i].incarnation_id.as_ref() == Some(&h.incarnation)));
    }
    lowest_primary(Some(h.mesh.as_str()))
}

/// The fabric-primary the leadership calculation seats once `mesh` is excluded from it (proposal Part
/// C2 search step 2: "the lowest NodeId among the other Ready mesh-primaries"): the excluded
/// mesh holds no candidate, and the fabric record is the one the update vacates, so the lowest Ready
/// mesh primary of another mesh wins. `None` when no other mesh has an eligible Ready mesh primary.
pub fn fabric_successor_excluding(nodes: &[Node], inc: &Incumbency, mesh: &str) -> Option<Node> {
    let mut hypothetical: Vec<Node> = nodes.iter().filter(|n| n.mesh != mesh).cloned().collect();
    let vacated = Incumbency { fabric: None, meshes: inc.meshes.clone(), node_type_primaries: inc.node_type_primaries.clone(), lost: inc.lost.clone() };
    resolve_with(&mut hypothetical, &vacated);
    hypothetical.into_iter().find(|n| n.is_fabric_primary)
}

/// The fabric-primary's successor after a fabric-primary is lost (Q89, elections §4.3: "the lowest
/// Ready node-admin of the fabric-primary's own mesh while it has one, else the lowest
/// Ready mesh-primary on the backbone"): the lowest Ready node-admin of `lost`'s mesh other than
/// `lost`, not known gone; else [`fabric_successor_excluding`] with `lost`'s mesh excluded.
pub fn fabric_successor_after(nodes: &[Node], inc: &Incumbency, lost: &rdm_core::handover::HandoverBirth) -> Option<Node> {
    let own: Vec<usize> = nodes.iter().enumerate().filter(|(_, n)| n.mesh == lost.mesh && n.kind == NodeKind::NodeAdmin && n.node_id != lost.node_id).map(|(i, _)| i).collect();
    if let Some(i) = lowest_ready(nodes, &own, inc) {
        return Some(nodes[i].clone());
    }
    fabric_successor_excluding(nodes, inc, &lost.mesh)
}

/// The fabric seat's successor its holder names (elections §4.4: "or the successor the holder names
/// (the re-created node-admin of the mesh that lost the seat, §4.3)"): `named`, when the fabric-successor
/// resolution with the holder's mesh excluded holds it as a Ready node-admin that holds its own
/// mesh's primary seat. Refused by name otherwise: in the holder's own mesh, unknown to the view, not
/// Ready, or holding no mesh-primary seat.
pub fn named_fabric_successor(nodes: &[Node], inc: &Incumbency, holder: &Node, named: &PathName) -> Result<Node, String> {
    // The named successor may be in the holder's own mesh (Q89: the hand-back to the node at the lost
    // fabric-primary's path.name): a Ready node-admin there is the one the row makes primary.
    let mut hypothetical: Vec<Node> = nodes.iter().filter(|n| named.mesh == holder.mesh || n.mesh != holder.mesh).cloned().collect();
    let vacated = Incumbency { fabric: None, meshes: inc.meshes.clone(), node_type_primaries: inc.node_type_primaries.clone(), lost: inc.lost.clone() };
    resolve_with(&mut hypothetical, &vacated);
    match hypothetical.into_iter().find(|n| n.name == *named) {
        Some(n) if n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::Ready => Ok(n),
        Some(n) => Err(format!("cannot hand the fabric seat to {named}: {}'s view holds it {:?}", holder.name, n.status)),
        None => Err(format!("cannot hand the fabric seat to {named}: {}'s view holds no such node-admin", holder.name)),
    }
}

/// The successor a yielding holder computes for its node-type primary seat (R-MT1, node.primary.update: "the MP
/// determines who is the next MP"; fabric-node-lifecycle-elections.md §4.1 and §5: "a holder that yields
/// ... hands the seat to the lowest Ready NodeId of its own mesh"): the lowest Ready node of
/// `holder`'s (mesh, kind) node type other than `holder`, not known gone. `None` when no other node of
/// its type can take the seat.
pub fn node_type_successor(nodes: &[Node], inc: &Incumbency, holder: &Node) -> Option<Node> {
    let members: Vec<usize> = nodes.iter().enumerate().filter(|(_, n)| n.mesh == holder.mesh && n.kind == holder.kind && n.node_id != holder.node_id).map(|(i, _)| i).collect();
    lowest_ready(nodes, &members, inc).map(|i| nodes[i].clone())
}

/// The node type of the node-admin type as the no-successor refusal names it (proposal Part A1:
/// "`node-admin` is the one value RDM gives meaning to"); every node carries its own `node_type` from
/// e12.s41, and the node type is read from the parsed kind until then.
pub const NODE_ADMIN_TYPE: &str = "node-admin";

/// The `node_type` of `kind` as Q7's refusal names it: [`NODE_ADMIN_TYPE`] for the
/// node-admin type, the kind's name with `-` for `_` for every other (`rpc-node`, `broker`, ...),
/// the tag-label grammar of proposal Part A1 (Q5).
pub fn node_type_of(kind: crate::model::NodeKind) -> &'static str {
    match kind {
        NodeKind::NodeAdmin => NODE_ADMIN_TYPE,
        NodeKind::RpcNode => "rpc-node",
        NodeKind::Broker => "broker",
        NodeKind::Gateway => "gateway",
        NodeKind::Compute => "compute",
    }
}

/// The successor the mesh-primary's pre-check names for a commanded node.primary.update of `holder`'s
/// node-type primary seat, a node type that is not node-admin (proposal Part C1 step 2): the lowest Ready node of
/// `holder`'s (mesh, kind) node type other than `holder`. `Err` is Q7's exact text naming its type and mesh.
pub fn commanded_node_type_successor(nodes: &[Node], inc: &Incumbency, holder: &Node) -> Result<Node, String> {
    node_type_successor(nodes, inc, holder).ok_or_else(|| no_secondary(node_type_of(holder.kind), &holder.mesh))
}

/// Which seats a commanded node.primary.update moves from its primary (the approved proposal
/// node-primary-update-fabric-mesh-and-node-type.md, Part C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Moving {
    /// node.primary.update of the holder's mesh-primary seat.
    MeshSeat,
    /// The holder holds both seats and both move in one row to its same-mesh successor (Q1: "the one
    /// row of C1 commits both seats to NA2 at their next revisions").
    BothSeats,
    /// node.fabric-primary.update with the holder's own mesh excluded (C2 search step 2, mesh.delete):
    /// the mesh seat does not move.
    FabricSeatOutside,
}

impl Moving {
    /// The seats as a span names them.
    pub fn name(self) -> &'static str {
        match self {
            Self::MeshSeat => "mesh-primary",
            Self::BothSeats => "mesh-primary+fabric-primary",
            Self::FabricSeatOutside => "fabric-primary",
        }
    }
    /// The mesh-primary seat moves.
    pub fn moves_mesh_primary(self) -> bool {
        matches!(self, Self::MeshSeat | Self::BothSeats)
    }
    /// The fabric-primary seat moves.
    pub fn moves_fabric_primary(self) -> bool {
        matches!(self, Self::BothSeats | Self::FabricSeatOutside)
    }
}

/// The fabric-primary's refusal, naming both searches (lifecycles/node-primary-update.md "The
/// fabric-primary", search step 3): `cannot transfer fabric-primary: no other node-admin Ready
/// in mesh <mesh>, no other mesh-primary Ready`.
pub fn no_fabric_successor(mesh: &str) -> String {
    format!("cannot transfer fabric-primary: no other node-admin Ready in mesh {mesh}, no other mesh-primary Ready")
}

/// The successor the writer's pre-check names for a commanded transfer of `holder`'s seats (proposal
/// Part C1 step 2; C2 "Successor search for the commanded transfer": "1. the holder's own
/// mesh: the lowest Ready node-admin NodeId there ... 2. only when that mesh has none, or it
/// is the departing mesh under mesh.delete: the lowest NodeId among the other Ready
/// mesh-primaries; 3. neither found: refused by name"). `Err` is the refusal's exact text (Q7): the
/// mesh seat with no secondary names its type and mesh; the fabric seat with neither search finding
/// one names both. A holder of both seats whose mesh has no secondary while another mesh's primary is
/// Ready cannot move its mesh seat: Q7's mesh text. The ordinary election is untouched by this search.
pub fn commanded_successor(nodes: &[Node], inc: &Incumbency, holder: &Node, moving: Moving) -> Result<Node, String> {
    let admin = Node { kind: NodeKind::NodeAdmin, ..holder.clone() };
    match moving {
        Moving::MeshSeat => node_type_successor(nodes, inc, &admin).ok_or_else(|| no_secondary(NODE_ADMIN_TYPE, &holder.mesh)),
        Moving::BothSeats => match node_type_successor(nodes, inc, &admin) {
            Some(n) => Ok(n),
            None if fabric_successor_excluding(nodes, inc, &holder.mesh).is_none() => Err(no_fabric_successor(&holder.mesh)),
            None => Err(no_secondary(NODE_ADMIN_TYPE, &holder.mesh)),
        },
        Moving::FabricSeatOutside => fabric_successor_excluding(nodes, inc, &holder.mesh).ok_or_else(|| no_fabric_successor(&holder.mesh)),
    }
}

/// [`commanded_successor`] over the members of the view `t`: the search a commanded transfer runs
/// (node-primary-update.md "Successor search for the commanded transfer"). Another mesh's members are
/// candidates only while that mesh is neither Dead nor Leaving (step 2: "the other Ready
/// mesh-primaries"): a deleted mesh's final primary lives on until its own node.delete (mesh-delete.md
/// step 7), and a mesh whose mesh.delete runs is leaving; neither mesh's primary takes a primary.
pub fn commanded_successor_in(t: &Topology, inc: &Incumbency, primary: &Node, moving: Moving) -> Result<Node, String> {
    let gone = |mesh: &str| t.meshes.iter().any(|m| m.name == mesh && matches!(m.status, crate::model::ScopeStatus::Dead | crate::model::ScopeStatus::Leaving));
    let nodes: Vec<Node> = t.members().filter(|n| n.mesh == primary.mesh || !gone(&n.mesh)).cloned().collect();
    commanded_successor(&nodes, inc, primary, moving)
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
        self.observe_as(&self.observer, t)
    }

    /// [`Self::observe`], naming the observer by the `path.name` `observer` it holds now.
    pub fn observe_as(&self, observer: &PathName, t: &Topology) {
        let mesh = observer.mesh.clone();
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
            let candidates = t.nodes_of_type(&mesh, *kind).count();
            let eligible = t.nodes_of_type(&mesh, *kind).filter(|n| n.status == NodeStatus::Ready).count();
            // The exact inputs: every candidate as this view holds it. Two observers that name
            // different winners differ here, never in the function.
            let inputs: Vec<String> = t.nodes_of_type(&mesh, *kind).map(|n| format!("{}={}:{:?}", n.name, n.node_id, n.status)).collect();
            let span = tracing::info_span!(
                parent: None,
                "rdm.mesh.election.resolve.via-recompute",
                election_level = "node_type",
                observer = %observer,
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
            span.in_scope(|| tracing::info!("node-type primary resolved"));
        }
        *last = now.clone();
        drop(last);

        // The mesh primary is the node-admin type's winner: the same seat.
        let mesh_primary = now.get(&NodeKind::NodeAdmin).cloned().flatten();
        let mut last_mesh = self.last_mesh.lock().unwrap();
        if last_mesh.as_ref() != Some(&mesh_primary) {
            let previous = last_mesh.clone().flatten();
            let span = tracing::info_span!(
                parent: None,
                "rdm.mesh.election.resolve.via-mesh-primary",
                election_level = "mesh_primary",
                observer = %observer,
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
        let owner = mesh_primary.as_ref().is_some_and(|s| s.path == observer.to_string());
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
                observer = %observer,
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
        n.status = if ready { NodeStatus::Ready } else { NodeStatus::Pending };
        n
    }

    fn primaries(nodes: &[Node]) -> Vec<String> {
        nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect()
    }

    fn fabric(nodes: &[Node]) -> Vec<String> {
        nodes.iter().filter(|n| n.is_fabric_primary).map(|n| n.name.to_string()).collect()
    }

    /// CONTRACT: a replaced predecessor (`<path>.old`) holds no seat and is no candidate, even when its
    /// digest still reads ready and its NodeId is the lowest: the successor at `<path>` is the node type's.
    #[test]
    fn a_renamed_predecessor_is_never_a_candidate_or_a_primary() {
        let mut old = node("mesh1.rpc.1", "000000000001", true);
        old.name = old.name.renamed();
        let successor = node("mesh1.rpc.1", "000000000009", true);
        let view = crate::topology::Topology {
            fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "f".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
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
            let (a, b) = (rdm_core::ids::decode_crockford60(w[0]).unwrap(), rdm_core::ids::decode_crockford60(w[1]).unwrap());
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
    fn ordinal_and_mesh_name_never_decide_a_primary() {
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
    fn provider_locality_and_runtime_metadata_never_decide_a_primary() {
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
    fn losing_a_winner_moves_each_primary_to_the_next_lowest() {
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
        nodes[1].status = NodeStatus::Ready;
        resolve(&mut nodes);
        assert_eq!(fabric(&nodes), ["mesh1.admin.2"]);
    }

    #[test]
    fn one_admin_is_every_primary_on_day_0() {
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

    // ---- R-A2: a living holder keeps its seat; the holder record is an input. ----

    fn born(path: &str, node_id: &str, inc: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = id(node_id);
        n.incarnation_id = Some(IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap());
        n.status = status;
        n
    }

    fn primary(mesh: &str, node_id: &str, inc: &str, revision: u64) -> Primary {
        Primary { mesh: mesh.into(), node_id: id(node_id), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap(), revision }
    }

    fn incumbency(fabric: Primary, meshes: &[Primary]) -> Incumbency {
        Incumbency { fabric: Some(fabric), meshes: meshes.iter().map(|h| (h.mesh.clone(), h.clone())).collect(), node_type_primaries: BTreeMap::new(), lost: HashSet::new() }
    }

    fn lose(inc: &mut Incumbency, _node_id: &str, incarnation: &str) {
        inc.lost.insert(IncarnationId::parse(&rdm_core::fixture_crockford60(incarnation)).unwrap());
    }

    fn fabric_name(nodes: &[Node]) -> Option<String> {
        nodes.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string())
    }

    fn mesh_primary(nodes: &[Node], mesh: &str) -> Option<String> {
        nodes.iter().find(|n| n.is_primary && n.mesh == mesh && n.kind == NodeKind::NodeAdmin).map(|n| n.name.to_string())
    }

    use NodeStatus::{ConnectionsDeleting, Leaving, Pending, PendingReconnect, Ready as Ready};

    /// CONTRACT (R-A2 cell 1): an admin with a lower NodeId appearing in another mesh leaves the
    /// fabric primary where it is. Must NOT happen: the lower id taking the seat.
    #[test]
    fn a_lower_node_id_in_another_mesh_never_displaces_the_fabric_primary() {
        let inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", Ready), born("mesh3.admin.1", "100000000000", "c1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"));
        assert_eq!(mesh_primary(&nodes, "mesh3").as_deref(), Some("mesh3.admin.1"), "the other mesh still has its own primary");
    }

    /// CONTRACT (R-A2 cell 2): a lower NodeId appearing inside the incumbent mesh leaves the
    /// healthy holder as the mesh primary and the fabric primary.
    #[test]
    fn a_lower_node_id_inside_the_incumbent_mesh_never_displaces_the_healthy_primary() {
        let inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", Ready), born("mesh1.admin.2", "100000000000", "a2", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"));
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"));
    }

    /// CONTRACT (R-A2 cell 8): one incumbent admin still runs, Pending or silent included: no
    /// other mesh takes the seat.
    #[test]
    fn one_surviving_incumbent_admin_blocks_a_cross_mesh_takeover() {
        for survivor in [Pending, PendingReconnect] {
            let mut inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1)]);
            lose(&mut inc, "500000000000", "a1");
            let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", PendingReconnect), born("mesh1.admin.2", "600000000000", "a2", survivor), born("mesh2.admin.1", "100000000000", "b1", Ready)];
            resolve_with(&mut nodes, &inc);
            assert_eq!(fabric_name(&nodes), None, "{survivor:?} survives in the incumbent mesh");
        }
    }

    /// CONTRACT (R-A2 cell 10): the old mesh recovers after another mesh took the seat: the new
    /// holder keeps it, whatever ids the recovered admins hold.
    #[test]
    fn the_old_mesh_recovering_never_reclaims_the_fabric_primary() {
        let inc = incumbency(primary("mesh3", "700000000000", "c1", 2), &[primary("mesh3", "700000000000", "c1", 1), primary("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", Ready), born("mesh3.admin.1", "700000000000", "c1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh3.admin.1"));
        assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"), "the old mesh still has its own mesh primary");
    }

    /// CONTRACT (elections §4.3, §5): another mesh's view right after a takeover. The standby
    /// mesh1.admin.2 took the fabric seat from the lost mesh1.admin.1 (record revision 2); this view
    /// still holds mesh1's record naming the lost birth (revision 1) and holds the standby
    /// PendingReconnect. With no Ready admin of its mesh left the seat waits in that mesh (mesh
    /// investigation decides the backbone move): it never moves to another mesh on a status.
    #[test]
    fn a_living_primary_another_view_holds_silent_keeps_the_fabric_primary_in_its_mesh() {
        let mut inc = incumbency(primary("mesh1", "720000000000", "s1", 2), &[primary("mesh1", "500000000000", "a1", 1), primary("mesh2", "200000000000", "b1", 1)]);
        lose(&mut inc, "500000000000", "a1");
        let mut nodes = vec![
            born("mesh1.admin.1", "500000000000", "a1", NodeStatus::Dead),
            born("mesh1.admin.2", "720000000000", "s1", PendingReconnect),
            born("mesh2.admin.1", "200000000000", "b1", Ready),
            born("mesh2.admin.2", "270000000000", "b2", Ready),
        ];
        resolve_with(&mut nodes, &inc);
        assert_ne!(fabric_name(&nodes).as_deref(), Some("mesh2.admin.1"), "the fabric seat left mesh1 while its holder lives: {:?}", fabric_name(&nodes));
        nodes[1].status = Ready;
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.2"), "heard again, the holder is its mesh's primary and keeps the seat");
    }

    /// CONTRACT (elections §5: "silence past the staleness floor supersedes it"; §4.1): a mesh-primary
    /// that is not the fabric-primary, held unheard past the floor (`PendingReconnect`, or an
    /// observer-inferred `Dead`), is superseded by the lowest Ready node-admin of its own mesh; the
    /// fabric-primary, heard in another mesh, stays where it is.
    #[test]
    fn a_mesh_primary_unheard_past_the_floor_is_superseded_by_its_meshs_lowest_ready_admin() {
        for status in [PendingReconnect, NodeStatus::Dead] {
            let inc = incumbency(primary("mesh2", "100000000000", "b1", 1), &[primary("mesh1", "500000000000", "a1", 1), primary("mesh2", "100000000000", "b1", 1)]);
            let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", status), born("mesh1.admin.2", "600000000000", "a2", Ready), born("mesh2.admin.1", "100000000000", "b1", Ready)];
            resolve_with(&mut nodes, &inc);
            assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.2"), "{status:?}");
            assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh2.admin.1"), "{status:?}");
        }
    }

    /// The fabric-primary `mesh1.admin.1` (a1, NodeId 500…), mesh1's primary too, recorded at revision 1;
    /// mesh1's standby a2, a Ready rpc node r1 whose node type's recorded primary r2 is unheard, and mesh2's
    /// primary b1.
    fn recovering() -> (Incumbency, Vec<Node>) {
        let mut inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1), primary("mesh2", "100000000000", "b1", 1)]);
        inc.node_type_primaries.insert(("mesh1".into(), NodeKind::RpcNode), primary("mesh1", "800000000000", "r2", 1));
        let nodes = vec![
            born("mesh1.admin.1", "500000000000", "a1", PendingReconnect),
            born("mesh1.admin.2", "600000000000", "a2", Ready),
            born("mesh1.rpc.1", "700000000000", "r1", Ready),
            born("mesh1.rpc.2", "800000000000", "r2", PendingReconnect),
            born("mesh2.admin.1", "100000000000", "b1", Ready),
        ];
        (inc, nodes)
    }

    /// CONTRACT (elections §3 and §10, Q122): while fabric recovery restores the fabric-primary, no other
    /// node becomes fabric-primary or node-admin primary of that mesh, whether the recorded birth is
    /// unheard past the floor, known gone, or no longer in the view; the mesh's other types elect as
    /// ever, and another mesh keeps its own primary. Must NOT happen: mesh1's standby, or mesh2's primary,
    /// named fabric-primary or mesh1's primary while the target is restored.
    #[test]
    fn while_fabric_recovery_restores_the_fabric_primary_no_other_node_takes_either_role() {
        let (inc, mut nodes) = recovering();
        let check = |nodes: &[Node], case: &str| {
            assert_eq!(fabric_name(nodes), None, "{case}: nobody is fabric-primary");
            assert_eq!(mesh_primary(nodes, "mesh1"), None, "{case}: no node-admin of mesh1 is elected");
            assert_eq!(rpc_primary(nodes, "mesh1").as_deref(), Some("mesh1.rpc.1"), "{case}: mesh1's rpc nodes elect as ever");
            assert_eq!(mesh_primary(nodes, "mesh2").as_deref(), Some("mesh2.admin.1"), "{case}: mesh2 keeps its primary");
        };
        resolve_with(&mut nodes, &inc);
        check(&nodes, "unheard");
        let mut gone = inc.clone();
        lose(&mut gone, "500000000000", "a1");
        resolve_with(&mut nodes, &gone);
        check(&nodes, "known gone");
        nodes.retain(|n| n.name.to_string() != "mesh1.admin.1");
        resolve_with(&mut nodes, &gone);
        check(&nodes, "deleted from the view");
    }

    /// CONTRACT (elections §3, fabric-recovery.md step 4, Q122): the node re-created at the target's
    /// path.name, once the workflow restores it, is mesh-primary and fabric-primary by its record, though
    /// mesh1's standby holds a lower NodeId; the election follows the record.
    #[test]
    fn the_node_the_workflow_restores_is_both_primaries_by_its_record() {
        let (mut inc, mut nodes) = recovering();
        nodes.retain(|n| n.name.to_string() != "mesh1.admin.1");
        nodes.push(born("mesh1.admin.1", "900000000000", "a9", Ready));
        let restored = primary("mesh1", "900000000000", "a9", 2);
        inc.fabric = Some(restored.clone());
        inc.meshes.insert("mesh1".into(), restored);
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"));
        assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"));
        assert_eq!(nodes.iter().find(|n| n.is_fabric_primary).map(|n| n.node_id.to_string()).as_deref(), Some("900000000000"), "the restored birth, not the standby");
    }

    /// CONTRACT (fabric-recovery.md step 2, its own mesh; Q122): fabric recovery's executor is the lowest
    /// Ready node-admin of the fabric-primary's mesh other than the target birth and the node at
    /// the target's path.name; none when the mesh has no such admin (the backbone arm, from mesh
    /// investigation's decision step). It takes no primary role.
    #[test]
    fn the_lowest_ready_admin_of_the_fabric_primarys_mesh_runs_fabric_recovery() {
        let (inc, mut nodes) = recovering();
        nodes.push(born("mesh1.admin.3", "550000000000", "a3", Pending));
        let at: PathName = "mesh1.admin.1".parse().unwrap();
        let target = fabric_recovery_target(&nodes, &inc).cloned().expect("the fabric-primary is unheard");
        assert_eq!(target.node_id, id("500000000000"));
        assert_eq!(fabric_recovery_executor(&nodes, &inc, &target, Some(&at)).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "a Pending admin is no candidate");
        // The node re-created at the target's path.name, Ready with a lower id, is the target, never the executor.
        nodes.push(born("mesh1.admin.1", "200000000000", "a9", Ready));
        assert_eq!(fabric_recovery_executor(&nodes, &inc, &target, Some(&at)).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"));
        nodes.retain(|n| n.name.mesh != "mesh1" || n.kind != NodeKind::NodeAdmin || n.name == at);
        assert_eq!(fabric_recovery_executor(&nodes, &inc, &target, Some(&at)), None, "no admin of the mesh: the backbone arm");
        let mut heard = nodes.clone();
        heard.retain(|n| n.name != at);
        heard.push(born("mesh1.admin.1", "500000000000", "a1", Ready));
        assert_eq!(fabric_recovery_target(&heard, &inc), None, "a heard fabric-primary is restored by nothing");
    }

    /// CONTRACT (R-EL1, fabric-node-lifecycle-elections.md sections 5-6): a seat moves only by an
    /// explicit yield, a known loss or silence past the floor, never by a lifecycle status. A holder that is ConnectionsDeleting (a
    /// FabricShutdown this observer has not installed yet) or Leaving keeps both its
    /// seats against a lower Ready NodeId of its own mesh. Must NOT happen: the observer reading the
    /// status as a yield and seating the lower id (gate 144a45bd: mesh2.admin.2 seated itself over
    /// the ConnectionsDeleting mesh2.admin.1 95 ms after that holder installed the shutdown).
    #[test]
    fn a_connections_deleting_or_leaving_primary_stays_both_primaries() {
        let inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1)]);
        for status in [ConnectionsDeleting, Leaving] {
            let mut nodes = vec![born("mesh1.admin.1", "500000000000", "a1", status), born("mesh1.admin.2", "200000000000", "a2", Ready), born("mesh2.admin.1", "100000000000", "b1", Ready)];
            resolve_with(&mut nodes, &inc);
            assert_eq!(mesh_primary(&nodes, "mesh1").as_deref(), Some("mesh1.admin.1"), "{status:?}");
            assert_eq!(fabric_name(&nodes).as_deref(), Some("mesh1.admin.1"), "{status:?}");
        }
    }

    /// CONTRACT (section 4.4): the explicit hand-over's successor is the leadership calculation with
    /// the departing mesh excluded, whatever the incumbent's status: the lowest Ready mesh primary
    /// of another mesh. Must NOT happen: the incumbent, still a living holder, selected as its own
    /// successor (no eligible successor, the hand-over blocked).
    #[test]
    fn the_handover_successor_is_the_lowest_ready_primary_outside_the_departing_mesh() {
        let inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1), primary("mesh2", "900000000000", "b1", 1), primary("mesh3", "700000000000", "c1", 1)]);
        for status in [Ready, ConnectionsDeleting] {
            let nodes = vec![
                born("mesh1.admin.1", "500000000000", "a1", status),
                born("mesh1.admin.2", "100000000000", "a2", Ready),
                born("mesh2.admin.1", "900000000000", "b1", Ready),
                born("mesh2.admin.2", "200000000000", "b2", Ready),
                born("mesh3.admin.1", "700000000000", "c1", Ready),
            ];
            let successor = fabric_successor_excluding(&nodes, &inc, "mesh1").map(|n| n.name.to_string());
            assert_eq!(successor.as_deref(), Some("mesh3.admin.1"), "the lowest Ready mesh primary outside mesh1, its seats held ({status:?})");
        }
    }

    /// CONTRACT (R-A2): a holder record names a birth this view has not heard: the seat waits for
    /// it. A joiner that knows the record but not yet the holder's digest takes nothing.
    #[test]
    fn a_primary_this_view_has_not_heard_is_held_vacant_for_it() {
        let inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1)]);
        let mut nodes = vec![born("mesh2.admin.1", "100000000000", "b1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(fabric_name(&nodes), None);
    }

    /// CONTRACT (R-A2): mesh-primary seats follow the same rule in every mesh: a living holder
    /// keeps it against a lower id; a vacancy is filled by the lowest Ready NodeId.
    #[test]
    fn the_mesh_primary_follows_the_same_rule_in_every_mesh() {
        let inc = incumbency(primary("mesh1", "500000000000", "a1", 1), &[primary("mesh1", "500000000000", "a1", 1), primary("mesh2", "900000000000", "b1", 1)]);
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

    // ---- Every node type keeps a seat record (proposal Part B1, R-MT1: "same for NA as it is for RPC node"). ----

    fn rpc_primary(nodes: &[Node], mesh: &str) -> Option<String> {
        nodes.iter().find(|n| n.is_primary && n.mesh == mesh && n.kind == NodeKind::RpcNode).map(|n| n.name.to_string())
    }

    fn with_rpc_primary(seat: Primary) -> Incumbency {
        let mut inc = incumbency(primary("mesh1", "900000000000", "a1", 1), &[primary("mesh1", "900000000000", "a1", 1)]);
        inc.node_type_primaries.insert((seat.mesh.clone(), NodeKind::RpcNode), seat);
        inc
    }

    /// CONTRACT (Part B1, elections §5 "a living holder keeps its seat; a lower-NodeId candidate that
    /// later becomes Ready never displaces it"): an rpc node type whose seat record names a living birth
    /// keeps that holder against a lower Ready NodeId of the node type, as the node-admin type does.
    /// Must NOT happen: the node type recomputed to its lowest Ready NodeId each checklist, so a seat that
    /// node.primary.update moved to mesh1.rpc.2 falls back to mesh1.rpc.1 on the next projection.
    #[test]
    fn a_living_rpc_primary_stays_primary_over_a_lower_ready_node_id() {
        let inc = with_rpc_primary(primary("mesh1", "500000000000", "r2", 2));
        let mut nodes = vec![born("mesh1.admin.1", "900000000000", "a1", Ready), born("mesh1.rpc.1", "100000000000", "r1", Ready), born("mesh1.rpc.2", "500000000000", "r2", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(rpc_primary(&nodes, "mesh1").as_deref(), Some("mesh1.rpc.2"), "the recorded living holder keeps the rpc seat");
        // A lifecycle status never yields it (R-EL1): a ConnectionsDeleting or Leaving holder keeps it.
        for status in [ConnectionsDeleting, Leaving] {
            nodes[2].status = status;
            resolve_with(&mut nodes, &inc);
            assert_eq!(rpc_primary(&nodes, "mesh1").as_deref(), Some("mesh1.rpc.2"), "{status:?}");
        }
        // Unheard past the floor it is superseded by the lowest Ready node of its type (elections §4.1).
        nodes[2].status = PendingReconnect;
        resolve_with(&mut nodes, &inc);
        assert_eq!(rpc_primary(&nodes, "mesh1").as_deref(), Some("mesh1.rpc.1"));
    }

    /// CONTRACT (module docs, "A holder whose record names a birth this view has not heard keeps its
    /// seat vacant for it"): an rpc node type's record naming a birth this view has not heard leaves the
    /// node type with no primary. Must NOT happen: the lowest Ready rpc node advertised as primary while
    /// the record names another living holder.
    #[test]
    fn an_rpc_primary_recorded_but_unheard_is_held_vacant_for_it() {
        let inc = with_rpc_primary(primary("mesh1", "500000000000", "r2", 2));
        let mut nodes = vec![born("mesh1.admin.1", "900000000000", "a1", Ready), born("mesh1.rpc.1", "100000000000", "r1", Ready)];
        resolve_with(&mut nodes, &inc);
        assert_eq!(rpc_primary(&nodes, "mesh1"), None);
    }

    /// CONTRACT (elections §5: "a holder that yields or whose exact birth is known gone hands the seat
    /// to the lowest Ready NodeId of its own mesh"; "restart preserves NodeId; a restarted
    /// node ... never displaces the holder"): the recorded rpc holder known gone, or replaced by a later
    /// birth of its NodeId, hands the seat to the node type's lowest Ready NodeId.
    #[test]
    fn an_rpc_primary_proven_gone_or_restarted_hands_over_to_the_lowest_ready() {
        let mut inc = with_rpc_primary(primary("mesh1", "100000000000", "r1", 2));
        let mut nodes = vec![born("mesh1.admin.1", "900000000000", "a1", Ready), born("mesh1.rpc.1", "100000000000", "r1", Ready), born("mesh1.rpc.2", "500000000000", "r2", Ready), born("mesh1.rpc.3", "300000000000", "r3", Ready)];
        lose(&mut inc, "100000000000", "r1");
        resolve_with(&mut nodes, &inc);
        assert_eq!(rpc_primary(&nodes, "mesh1").as_deref(), Some("mesh1.rpc.3"), "known gone: the lowest Ready of the rest");
        let inc = with_rpc_primary(primary("mesh1", "500000000000", "r2", 2));
        nodes[2] = born("mesh1.rpc.2", "500000000000", "r9", Ready);
        resolve_with(&mut nodes, &inc);
        assert_eq!(rpc_primary(&nodes, "mesh1").as_deref(), Some("mesh1.rpc.1"), "a later birth of the holder's NodeId holds no seat");
    }
}
