//! The current desired topology (i143.e1.s7, rafka-v2#2851;
//! `docs/i143/design.md` §2.2).
//!
//! Three separate facts:
//!
//! ```text
//! DesiredTopology               what Fabric/Mesh shape should exist (this module)
//! membership + RuntimeFacts     what exact births/runtimes exist now
//! Build facts                   how one reconciliation attempt is executing
//! ```
//!
//! [`DesiredTopology`] is bounded current state: the desired fabric shape at
//! one `revision`, the Build whose request set it (`source_build_id`), and a
//! bounded lineage of the revisions it descends from. Completing or
//! forgetting a Build never touches it.
//!
//! A topology-changing request compiles against the current revision and
//! proposes the next ([`DesiredTopology::next`]). Every admin's
//! [`DesiredStore`] decides an offered record the same way:
//!
//! - it descends from the held one: taken (a later revision);
//! - the held one descends from it: stale, ignored;
//! - neither (a fork: two writers proposed from the same base): the branch
//!   whose first diverging revision has the lower `source_build_id` wins on
//!   every admin, whichever it heard first. The losing branch's revisions are
//!   refused by name and a Build that proposed one is never executed. A
//!   longer branch does not win by being longer: no last-write-wins.
//!
//! A fork deeper than the lineage is refused outright: the held record stays.

use crate::build::{BuildId, BuildIntent, FabricDesired, MeshDesired};
use crate::model::{FabricId, NodeKind};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

/// How many revisions a record names behind itself (bounds a record, and
/// the deepest fork two admins can still resolve).
pub const LINEAGE: usize = 16;
/// How many refused revisions a store remembers (bounded evidence).
const REFUSED: usize = 64;

/// One revision of the desired topology, by identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DesiredMark {
    pub revision: u64,
    /// The Build whose request set this revision; `None` for the Day-0 root.
    pub source_build_id: Option<BuildId>,
}

impl std::fmt::Display for DesiredMark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source_build_id {
            Some(b) => write!(f, "revision {} (Build {b})", self.revision),
            None => write!(f, "revision {} (Day 0)", self.revision),
        }
    }
}

/// The desired Fabric/Mesh shape at one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredTopology {
    pub fabric_id: FabricId,
    pub revision: u64,
    pub desired: FabricDesired,
    pub source_build_id: Option<BuildId>,
    /// The revisions this one descends from, oldest first, at most
    /// [`LINEAGE`]; this record's own mark is not in it.
    pub lineage: Vec<DesiredMark>,
}

impl DesiredTopology {
    /// Day 0: the bootstrap admin's own mesh, one node-admin.
    pub fn root(fabric_id: FabricId, fabric: &str, mesh: &str) -> Self {
        Self {
            fabric_id,
            revision: 1,
            desired: FabricDesired { fabric: fabric.into(), meshes: vec![MeshDesired { name: mesh.into(), node_admin: 1, rpc_node: 0 }] },
            source_build_id: None,
            lineage: Vec::new(),
        }
    }

    pub fn mark(&self) -> DesiredMark {
        DesiredMark { revision: self.revision, source_build_id: self.source_build_id.clone() }
    }

    /// The next revision: `desired`, set by `build`, descending from this one.
    pub fn next(&self, desired: FabricDesired, build: &BuildId) -> Self {
        let mut lineage = self.lineage.clone();
        lineage.push(self.mark());
        if lineage.len() > LINEAGE {
            lineage.drain(..lineage.len() - LINEAGE);
        }
        Self { fabric_id: self.fabric_id.clone(), revision: self.revision + 1, desired: normalized(desired), source_build_id: Some(build.clone()), lineage }
    }

    /// Every mark of this record, its own last.
    fn marks(&self) -> impl Iterator<Item = DesiredMark> + '_ {
        self.lineage.iter().cloned().chain(std::iter::once(self.mark()))
    }

    fn holds(&self, m: &DesiredMark) -> bool {
        self.marks().any(|x| &x == m)
    }

    /// One mesh's desired counts.
    pub fn mesh(&self, name: &str) -> Option<&MeshDesired> {
        self.desired.meshes.iter().find(|m| m.name == name)
    }
}

fn normalized(mut d: FabricDesired) -> FabricDesired {
    d.meshes.sort_by(|a, b| a.name.cmp(&b.name));
    d
}

/// The desired shape `intent` asks for, from `current` (and `observed`, for
/// a mesh the desired state does not name yet). `None`: the intent changes
/// no desired count or mesh (a restart or replacement keeps the shape).
pub fn apply(intent: &BuildIntent, current: &FabricDesired, observed: &Topology) -> Option<FabricDesired> {
    let mut d = current.clone();
    let counts = |d: &FabricDesired, mesh: &str| -> MeshDesired {
        d.meshes.iter().find(|m| m.name == mesh).cloned().unwrap_or_else(|| MeshDesired {
            name: mesh.into(),
            node_admin: observed.cohort(mesh, NodeKind::NodeAdmin).filter(|n| n.status.is_live()).count() as u32,
            rpc_node: observed.cohort(mesh, NodeKind::RpcNode).filter(|n| n.status.is_live()).count() as u32,
        })
    };
    let set = |d: &mut FabricDesired, m: MeshDesired| {
        d.meshes.retain(|x| x.name != m.name);
        d.meshes.push(m);
    };
    match intent {
        BuildIntent::ReconcileFabric { desired } => d = desired.clone(),
        BuildIntent::ReconcileMesh { desired } | BuildIntent::CreateMesh { desired } => set(&mut d, desired.clone()),
        BuildIntent::RemoveMesh { mesh } => d.meshes.retain(|m| &m.name != mesh),
        BuildIntent::AddNode { mesh, node_kind, .. } => {
            let mut m = counts(&d, mesh);
            match node_kind {
                NodeKind::NodeAdmin => m.node_admin += 1,
                NodeKind::RpcNode => m.rpc_node += 1,
            }
            set(&mut d, m);
        }
        BuildIntent::RemoveNode { node, .. } => {
            let mut m = counts(&d, &node.mesh);
            match node.kind {
                NodeKind::NodeAdmin => m.node_admin = m.node_admin.saturating_sub(1),
                NodeKind::RpcNode => m.rpc_node = m.rpc_node.saturating_sub(1),
            }
            set(&mut d, m);
        }
        BuildIntent::RestartNode { .. } | BuildIntent::ReplaceNode { .. } => return None,
    }
    let d = normalized(d);
    (d != normalized(current.clone())).then_some(d)
}

/// Where a Build's desired revision stands against the held record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Standing {
    /// The held revision, or one it descends from: the Build may run.
    Current,
    /// Newer than anything held: not heard yet; the Build waits for it.
    Ahead,
    /// At or below the held revision but not in its lineage: it lost a fork
    /// (whether or not this admin saw the fork itself). Never executed.
    Lost,
    /// Older than the held lineage reaches: it cannot be judged, and is
    /// taken as an ancestor.
    Beyond,
    /// This admin holds no desired topology yet.
    Unhydrated,
}

/// What a [`DesiredStore`] did with an offered record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Offer {
    /// The store holds it now (the first record, or a later revision).
    Taken,
    /// The store already holds it, or one that descends from it.
    Held,
    /// A fork, decided: `winner` is held, `loser` refused by name.
    Fork { winner: DesiredMark, loser: DesiredMark, at: u64 },
    /// A fork deeper than the lineage: refused, the held record stays.
    Unresolvable { held: DesiredMark, offered: DesiredMark },
    /// Another fabric's record.
    OtherFabric,
}

/// One admin's current desired topology.
#[derive(Debug, Default)]
pub struct DesiredStore {
    inner: Mutex<StoreState>,
}

#[derive(Debug, Default)]
struct StoreState {
    current: Option<DesiredTopology>,
    /// Revisions that lost a fork, newest last (bounded).
    refused: Vec<DesiredMark>,
}

impl DesiredStore {
    /// A store holding `t`.
    pub fn holding(t: DesiredTopology) -> Self {
        let s = Self::default();
        s.offer(t);
        s
    }

    pub fn current(&self) -> Option<DesiredTopology> {
        self.inner.lock().unwrap().current.clone()
    }

    /// Did `mark` lose a fork this store decided?
    pub fn refused(&self, mark: &DesiredMark) -> bool {
        self.inner.lock().unwrap().refused.contains(mark)
    }

    /// Where `mark` stands against the held record (see [`Standing`]).
    pub fn standing(&self, mark: &DesiredMark) -> Standing {
        let s = self.inner.lock().unwrap();
        let Some(held) = &s.current else { return Standing::Unhydrated };
        if s.refused.contains(mark) {
            return Standing::Lost;
        }
        if held.holds(mark) {
            return Standing::Current;
        }
        if mark.revision > held.revision {
            return Standing::Ahead;
        }
        match held.lineage.first() {
            Some(oldest) if mark.revision < oldest.revision => Standing::Beyond,
            _ => Standing::Lost,
        }
    }

    /// Decide `offered` against the held record (module docs).
    pub fn offer(&self, offered: DesiredTopology) -> Offer {
        let mut s = self.inner.lock().unwrap();
        let Some(held) = s.current.clone() else {
            s.current = Some(offered);
            return Offer::Taken;
        };
        if held.fabric_id != offered.fabric_id {
            return Offer::OtherFabric;
        }
        if held.holds(&offered.mark()) {
            return Offer::Held;
        }
        if offered.holds(&held.mark()) {
            s.current = Some(offered);
            return Offer::Taken;
        }
        // A fork: the newest revision both name is where it starts.
        let common = held.marks().filter(|m| offered.holds(m)).max_by_key(|m| m.revision);
        let Some(common) = common else {
            let o = Offer::Unresolvable { held: held.mark(), offered: offered.mark() };
            refuse(&mut s.refused, std::iter::once(offered.mark()));
            return o;
        };
        let at = common.revision + 1;
        let first = |t: &DesiredTopology| t.marks().find(|m| m.revision == at).expect("a branch past the fork names its first revision");
        let branch = |t: &DesiredTopology| t.marks().filter(|m| m.revision >= at).collect::<Vec<_>>();
        let (h, o) = (first(&held), first(&offered));
        if o.source_build_id < h.source_build_id {
            refuse(&mut s.refused, branch(&held).into_iter());
            s.current = Some(offered);
            Offer::Fork { winner: o, loser: h, at }
        } else {
            refuse(&mut s.refused, branch(&offered).into_iter());
            Offer::Fork { winner: h, loser: o, at }
        }
    }
}

/// How a record reached this admin (the `via-` reason of its evidence).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// The bootstrap admin's root (`via-day-0`).
    Day0,
    /// The entry pull from the launching admin (`via-hydration`).
    Hydration,
    /// The fabric control topic: a new revision, or a neighbour's catch-up
    /// after it (re)connected (`via-catch-up`).
    CatchUp,
}

/// Offer `t` to `store` and leave the evidence: the update taken, or the
/// fork decided, by name. `node` is this admin, `from` who sent it.
pub fn take(store: &DesiredStore, t: DesiredTopology, via: Via, node: &str, from: &str) -> Offer {
    let (fabric_id, mark) = (t.fabric_id.clone(), t.mark());
    let previous = store.current().map(|c| c.revision).unwrap_or(0);
    let o = store.offer(t);
    let source = mark.source_build_id.as_ref().map(|b| b.0.as_str()).unwrap_or("").to_string();
    match &o {
        Offer::Taken => {
            let span = match via {
                Via::Day0 => tracing::info_span!("rafka.node_admin.desired_topology.update.via-day-0", node, fabric_id = %fabric_id, desired_revision = mark.revision, previous_revision = previous, source_build_id = %source, from),
                Via::Hydration => tracing::info_span!("rafka.node_admin.desired_topology.update.via-hydration", node, fabric_id = %fabric_id, desired_revision = mark.revision, previous_revision = previous, source_build_id = %source, from),
                Via::CatchUp => tracing::info_span!("rafka.node_admin.desired_topology.update.via-catch-up", node, fabric_id = %fabric_id, desired_revision = mark.revision, previous_revision = previous, source_build_id = %source, from),
            };
            span.in_scope(|| tracing::info!("current desired topology"));
        }
        Offer::Fork { winner, loser, at } => {
            tracing::info_span!(
                "rafka.node_admin.desired_topology.reject.via-revision-conflict",
                node,
                fabric_id = %fabric_id,
                desired_revision = at,
                winner = %winner,
                loser = %loser,
                from,
            )
            .in_scope(|| tracing::info!("two desired updates from one base: the lower source Build's branch holds, the other is refused"));
        }
        Offer::Unresolvable { held, offered } => {
            tracing::info_span!("rafka.node_admin.desired_topology.reject.via-fork-beyond-lineage", node, fabric_id = %fabric_id, held = %held, offered = %offered, from)
                .in_scope(|| tracing::info!("a desired fork deeper than the lineage: refused, the held revision stays"));
        }
        Offer::Held | Offer::OtherFabric => {}
    }
    o
}

fn refuse(refused: &mut Vec<DesiredMark>, marks: impl Iterator<Item = DesiredMark>) {
    for m in marks {
        if !refused.contains(&m) {
            refused.push(m);
        }
    }
    if refused.len() > REFUSED {
        refused.drain(..refused.len() - REFUSED);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, ProviderKind, ScopeStatus};

    fn fabric() -> FabricId {
        FabricId::parse("0123456789ab").unwrap()
    }

    fn mesh(name: &str, a: u32, r: u32) -> MeshDesired {
        MeshDesired { name: name.into(), node_admin: a, rpc_node: r }
    }

    fn shape(meshes: Vec<MeshDesired>) -> FabricDesired {
        FabricDesired { fabric: "fabric1".into(), meshes }
    }

    fn empty() -> Topology {
        Topology {
            fabric: Fabric { id: fabric(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![],
            nodes: vec![],
        }
    }

    fn b(s: &str) -> BuildId {
        BuildId(s.into())
    }

    #[test]
    fn each_intent_updates_only_the_shape_it_names() {
        let d = shape(vec![mesh("mesh1", 2, 3), mesh("mesh2", 2, 2)]);
        let t = empty();
        let one = |intent: BuildIntent| apply(&intent, &d, &t);
        // ReconcileMesh leaves its sibling alone.
        assert_eq!(one(BuildIntent::ReconcileMesh { desired: mesh("mesh1", 2, 5) }), Some(shape(vec![mesh("mesh1", 2, 5), mesh("mesh2", 2, 2)])));
        // ReconcileFabric replaces the whole shape.
        assert_eq!(one(BuildIntent::ReconcileFabric { desired: shape(vec![mesh("mesh1", 1, 1)]) }), Some(shape(vec![mesh("mesh1", 1, 1)])));
        // Create and remove a mesh.
        assert_eq!(one(BuildIntent::CreateMesh { desired: mesh("mesh3", 1, 0) }).unwrap().meshes.len(), 3);
        assert_eq!(one(BuildIntent::RemoveMesh { mesh: "mesh2".into() }), Some(shape(vec![mesh("mesh1", 2, 3)])));
        // Add and remove one node.
        let add = BuildIntent::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::RpcNode, target: None };
        assert_eq!(one(add), Some(shape(vec![mesh("mesh1", 2, 3), mesh("mesh2", 2, 3)])));
        let rm = BuildIntent::RemoveNode { node: "mesh1.admin.2".parse().unwrap(), incarnation: None };
        assert_eq!(one(rm), Some(shape(vec![mesh("mesh1", 1, 3), mesh("mesh2", 2, 2)])));
        // A restart or a replacement keeps the shape.
        assert_eq!(one(BuildIntent::RestartNode { node: "mesh1.rpc.1".parse().unwrap(), from_incarnation: None }), None);
        assert_eq!(one(BuildIntent::ReplaceNode { node: "mesh1.rpc.1".parse().unwrap() }), None);
        // Asking for what is already desired changes nothing.
        assert_eq!(one(BuildIntent::ReconcileMesh { desired: mesh("mesh1", 2, 3) }), None);
    }

    #[test]
    fn a_later_revision_is_taken_and_an_earlier_one_is_stale() {
        let s = DesiredStore::default();
        let r1 = DesiredTopology::root(fabric(), "fabric1", "mesh1");
        let r2 = r1.next(shape(vec![mesh("mesh1", 2, 3)]), &b("bld-a"));
        let r3 = r2.next(shape(vec![mesh("mesh1", 2, 4)]), &b("bld-b"));
        assert_eq!(s.offer(r1.clone()), Offer::Taken);
        // Catch-up skips a revision it missed: r3 descends from r1.
        assert_eq!(s.offer(r3.clone()), Offer::Taken);
        assert_eq!(s.offer(r2), Offer::Held, "an older revision never replaces a newer one");
        assert_eq!(s.offer(r1), Offer::Held);
        assert_eq!(s.current(), Some(r3));
    }

    #[test]
    fn two_updates_from_one_base_resolve_to_the_same_winner_on_every_admin() {
        let r1 = DesiredTopology::root(fabric(), "fabric1", "mesh1");
        let a = r1.next(shape(vec![mesh("mesh1", 2, 3)]), &b("bld-a"));
        let z = r1.next(shape(vec![mesh("mesh1", 1, 9)]), &b("bld-z"));
        // The losing side kept writing; it is not chosen for being longer.
        let z2 = z.next(shape(vec![mesh("mesh1", 1, 10)]), &b("bld-z2"));
        for order in [vec![a.clone(), z2.clone()], vec![z2.clone(), a.clone()]] {
            let s = DesiredStore::default();
            s.offer(r1.clone());
            let outcomes: Vec<Offer> = order.into_iter().map(|t| s.offer(t)).collect();
            assert_eq!(s.current().unwrap().mark(), a.mark(), "{outcomes:?}");
            assert!(matches!(outcomes.last().unwrap(), Offer::Fork { at: 2, .. }));
            assert!(s.refused(&z.mark()) && s.refused(&z2.mark()), "the whole losing branch is refused");
            assert!(!s.refused(&a.mark()));
        }
    }

    #[test]
    fn a_builds_revision_is_judged_by_the_held_lineage_not_by_having_seen_the_fork() {
        let r1 = DesiredTopology::root(fabric(), "fabric1", "mesh1");
        let won = r1.next(shape(vec![mesh("mesh1", 2, 3)]), &b("bld-a"));
        let lost = r1.next(shape(vec![mesh("mesh1", 1, 9)]), &b("bld-z"));
        let later = won.next(shape(vec![mesh("mesh1", 2, 4)]), &b("bld-c"));
        assert_eq!(DesiredStore::default().standing(&won.mark()), Standing::Unhydrated);
        // This store only ever heard the winning branch.
        let s = DesiredStore::holding(r1.clone());
        s.offer(won.clone());
        assert_eq!(s.standing(&r1.mark()), Standing::Current, "an ancestor");
        assert_eq!(s.standing(&won.mark()), Standing::Current);
        assert_eq!(s.standing(&later.mark()), Standing::Ahead, "not heard yet: wait");
        assert_eq!(s.standing(&lost.mark()), Standing::Lost, "lost a fork it never saw");
    }

    #[test]
    fn a_fork_deeper_than_the_lineage_is_refused_and_the_held_record_stays() {
        let mut held = DesiredTopology::root(fabric(), "fabric1", "mesh1");
        let mut other = held.next(shape(vec![mesh("mesh1", 1, 9)]), &b("bld-0"));
        for i in 0..(LINEAGE as u32 + 2) {
            held = held.next(shape(vec![mesh("mesh1", 2, i)]), &b(&format!("bld-h{i:02}")));
            other = other.next(shape(vec![mesh("mesh1", 3, i)]), &b(&format!("bld-o{i:02}")));
        }
        assert!(held.lineage.len() == LINEAGE && serde_json::to_vec(&held).unwrap().len() < 2048, "a record stays bounded");
        let s = DesiredStore::default();
        s.offer(held.clone());
        assert!(matches!(s.offer(other.clone()), Offer::Unresolvable { .. }));
        assert_eq!(s.current(), Some(held));
        assert!(s.refused(&other.mark()));
    }

    #[test]
    fn another_fabrics_record_is_not_taken() {
        let s = DesiredStore::default();
        s.offer(DesiredTopology::root(fabric(), "fabric1", "mesh1"));
        let other = DesiredTopology::root(FabricId::parse("ba9876543210").unwrap(), "fabric2", "mesh1");
        assert_eq!(s.offer(other), Offer::OtherFabric);
    }
}
