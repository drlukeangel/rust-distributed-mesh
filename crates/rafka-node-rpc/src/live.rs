//! The live resolver: one per process, fed by that process's membership.
//!
//! Location and identity only. It answers where an exact node is now
//! (`Found`), that it departed (`Gone`) or that this process does not know it
//! (`Unknown`); it owns no topology, Build, election, lifecycle or death
//! policy. A `Draining` or `Leaving` birth still resolves `Found`, and
//! silence, a failed dial or a partition never makes a node `Gone`.
//!
//! `Gone` comes only from a departure the process was told of: the exact
//! birth's removal or replacement, proven by whoever executed it and carried
//! by membership. A departed node is held `Gone` for the departed retention
//! (local age since this process accepted it, never another machine's clock),
//! then forgotten: it resolves `Unknown`. A departed node never returns; a
//! replacement is a new logical node at the same path.
//!
//! The process composition (node-admin's running state, the rpc node's) feeds
//! it: [`LiveNodeResolver::apply`] for each current birth it holds,
//! [`LiveNodeResolver::depart`] for each departure. Neither membership nor
//! Node RPC depends on the other.

use crate::resolve::{NodeResolver, NodeTarget, ResolvedNode};
use rafka_mesh_entity::{IncarnationId, NodeId, PathName};
use rafka_node_rpc_contract::outcome::ResolveFailure;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub use rafka_mesh_entity::DEPARTED_RETENTION;

/// What applying one fact did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// A node this process did not hold, now `Found`.
    Joined,
    /// The node's next birth (it names the held one as superseded).
    Restarted { old: IncarnationId },
    /// The held birth's address changed.
    Updated,
    /// The fact matches what is held.
    Unchanged,
    /// Not taken; the held answer stands.
    Refused(Refusal),
    /// A departure recorded: the node is `Gone`.
    Departed,
}

/// Why a fact was not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The logical node departed; a departed node never returns.
    Departed { node_id: NodeId },
    /// A birth this process already saw superseded.
    StaleIncarnation { node_id: NodeId, incarnation: IncarnationId },
    /// Another birth of the node that does not name the held one as superseded.
    UnknownLineage { node_id: NodeId, held: IncarnationId, offered: IncarnationId },
    /// A restart keeps the node's transport identity.
    TransportChanged { node_id: NodeId },
}

#[derive(Debug, Default)]
struct Live {
    nodes: HashMap<NodeId, ResolvedNode>,
    /// The path's holder: the first live node heard at it, until it departs.
    by_path: HashMap<PathName, NodeId>,
    retired: HashMap<NodeId, HashSet<IncarnationId>>,
    departed: HashMap<NodeId, Instant>,
    /// Births known to have exited (an executor's proof): the node resolves nothing until a
    /// successor birth is applied, so nothing dials or installs the dead birth's address.
    exited: HashSet<(NodeId, IncarnationId)>,
}

impl Live {
    fn expire(&mut self, retention: Duration, now: Instant) {
        self.departed.retain(|_, at| now.duration_since(*at) < retention);
    }

    /// The path's holder after `gone` left it: another live node heard at the
    /// same path, if any.
    fn rehold(&mut self, path: &PathName) {
        if self.by_path.get(path).is_some_and(|id| self.nodes.contains_key(id)) {
            return;
        }
        self.by_path.remove(path);
        let mut at: Vec<&NodeId> = self.nodes.values().filter(|n| &n.name == path).map(|n| &n.node_id).collect();
        at.sort();
        if let Some(id) = at.first() {
            self.by_path.insert(path.clone(), (*id).clone());
        }
    }
}

/// One per process; shared by `Arc` with the process's one Node RPC client.
#[derive(Debug)]
pub struct LiveNodeResolver {
    live: RwLock<Live>,
    retention: Duration,
    changed: tokio::sync::watch::Sender<u64>,
}

impl Default for LiveNodeResolver {
    fn default() -> Self {
        Self::new(DEPARTED_RETENTION)
    }
}

impl LiveNodeResolver {
    pub fn new(departed_retention: Duration) -> Self {
        Self { live: RwLock::default(), retention: departed_retention, changed: tokio::sync::watch::Sender::new(0) }
    }

    fn tick(&self) {
        self.changed.send_modify(|v| *v += 1);
    }

    /// Take one current birth as membership holds it. `supersedes` is the
    /// birth this one replaces (its lineage), `None` for a first birth.
    pub fn apply(&self, birth: ResolvedNode, supersedes: Option<&IncarnationId>) -> Applied {
        let mut live = self.live.write().unwrap();
        live.expire(self.retention, Instant::now());
        let id = birth.node_id.clone();
        if live.departed.contains_key(&id) {
            return Applied::Refused(Refusal::Departed { node_id: id });
        }
        if live.retired.get(&id).is_some_and(|r| r.contains(&birth.incarnation)) {
            return Applied::Refused(Refusal::StaleIncarnation { node_id: id, incarnation: birth.incarnation });
        }
        let applied = match live.nodes.get(&id) {
            None => Applied::Joined,
            Some(held) if held.endpoint_id != birth.endpoint_id => return Applied::Refused(Refusal::TransportChanged { node_id: id }),
            Some(held) if held.incarnation == birth.incarnation => {
                if held.transport_addr == birth.transport_addr {
                    return Applied::Unchanged;
                }
                Applied::Updated
            }
            Some(held) if supersedes == Some(&held.incarnation) => Applied::Restarted { old: held.incarnation.clone() },
            Some(held) => {
                return Applied::Refused(Refusal::UnknownLineage { node_id: id, held: held.incarnation.clone(), offered: birth.incarnation })
            }
        };
        if let Applied::Restarted { old } = &applied {
            live.retired.entry(id.clone()).or_default().insert(old.clone());
            live.exited.remove(&(id.clone(), old.clone()));
        }
        let path = birth.name.clone();
        live.nodes.insert(id.clone(), birth);
        live.by_path.entry(path.clone()).or_insert(id);
        live.rehold(&path);
        drop(live);
        self.tick();
        applied
    }

    /// The exact birth `incarnation` of `node_id` at `path` departed: its
    /// removal or replacement was proven. The logical node is `Gone` for the
    /// departed retention, and the path passes to another live node heard at
    /// it, if any.
    pub fn depart(&self, node_id: &NodeId, incarnation: &IncarnationId, path: &PathName) -> Applied {
        self.depart_at(node_id, incarnation, path, Instant::now())
    }

    /// [`Self::depart`], accepted at `now`.
    pub fn depart_at(&self, node_id: &NodeId, incarnation: &IncarnationId, path: &PathName, now: Instant) -> Applied {
        let mut live = self.live.write().unwrap();
        live.expire(self.retention, now);
        if live.departed.contains_key(node_id) {
            return Applied::Unchanged;
        }
        live.departed.insert(node_id.clone(), now);
        live.retired.entry(node_id.clone()).or_default().insert(incarnation.clone());
        let held_path = live.nodes.remove(node_id).map(|n| n.name);
        for p in held_path.iter().chain(std::iter::once(path)) {
            if live.by_path.get(p) == Some(node_id) {
                live.by_path.remove(p);
            }
            live.rehold(p);
        }
        drop(live);
        self.tick();
        Applied::Departed
    }
}

impl LiveNodeResolver {
    /// The exact birth `incarnation` of `node_id` is known to have exited (the provider proved
    /// it): until a successor birth is applied the node resolves `Unavailable`, a dial in flight
    /// to the dead birth is cancelled, and nothing dials or installs its address again.
    pub fn retire_birth(&self, node_id: &NodeId, incarnation: &IncarnationId) {
        let held = self.live.read().unwrap().nodes.get(node_id).map(|n| n.incarnation.clone());
        if held.as_ref() != Some(incarnation) {
            return;
        }
        self.live.write().unwrap().exited.insert((node_id.clone(), incarnation.clone()));
        self.tick();
    }

    /// [`NodeResolver::resolve`] as of `now`.
    /// The live node whose Iroh key is `endpoint`: the peer of an accepted connection. A key
    /// this process holds no live node for (a probe's ephemeral key, a departed birth) is `None`.
    pub fn by_endpoint(&self, endpoint: &iroh::PublicKey) -> Option<ResolvedNode> {
        self.live.read().unwrap().nodes.values().find(|n| &n.endpoint_id == endpoint).cloned()
    }

    pub fn resolve_at(&self, target: &NodeTarget, now: Instant) -> Result<ResolvedNode, ResolveFailure> {
        let live = self.live.read().unwrap();
        let current = |n: &ResolvedNode| if live.exited.contains(&(n.node_id.clone(), n.incarnation.clone())) { Err(ResolveFailure::Unavailable) } else { Ok(n.clone()) };
        match target {
            NodeTarget::ExactNode(id) => match live.nodes.get(id) {
                Some(n) => current(n),
                None => Err(match live.departed.get(id) {
                    Some(at) if now.saturating_duration_since(*at) < self.retention => ResolveFailure::Gone,
                    _ => ResolveFailure::Unknown,
                }),
            },
            NodeTarget::CurrentPath(p) => match live.by_path.get(p).and_then(|id| live.nodes.get(id)) {
                Some(n) => current(n),
                None => Err(ResolveFailure::Unknown),
            },
        }
    }
}

impl NodeResolver for LiveNodeResolver {
    fn resolve(&self, target: &NodeTarget) -> Result<ResolvedNode, ResolveFailure> {
        self.resolve_at(target, Instant::now())
    }

    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.changed.subscribe())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> iroh::PublicKey {
        iroh::SecretKey::generate().public()
    }

    fn birth(path: &str, port: u16) -> ResolvedNode {
        ResolvedNode {
            node_id: NodeId::mint(),
            name: path.parse().unwrap(),
            endpoint_id: key(),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
        }
    }

    fn restart(n: &ResolvedNode, port: u16) -> ResolvedNode {
        ResolvedNode { incarnation: IncarnationId::mint(), transport_addr: format!("127.0.0.1:{port}").parse().unwrap(), ..n.clone() }
    }

    fn exact(r: &LiveNodeResolver, n: &ResolvedNode) -> Result<ResolvedNode, ResolveFailure> {
        r.resolve(&NodeTarget::ExactNode(n.node_id.clone()))
    }

    #[test]
    fn a_restart_is_the_same_node_under_its_next_birth_and_the_old_birth_never_returns() {
        let r = LiveNodeResolver::default();
        let a = birth("mesh1.rpc.1", 7000);
        assert_eq!(r.apply(a.clone(), None), Applied::Joined);
        let b = restart(&a, 7001);
        assert_eq!(r.apply(b.clone(), Some(&a.incarnation)), Applied::Restarted { old: a.incarnation.clone() });
        assert_eq!(exact(&r, &a).unwrap().incarnation, b.incarnation);
        assert!(matches!(r.apply(a.clone(), None), Applied::Refused(Refusal::StaleIncarnation { .. })), "a late fact of the old birth");
        let stranger = restart(&a, 7002);
        assert!(matches!(r.apply(stranger, None), Applied::Refused(Refusal::UnknownLineage { .. })), "a birth outside the lineage");
        assert_eq!(exact(&r, &a).unwrap().incarnation, b.incarnation);
    }

    #[test]
    fn reapplying_is_idempotent_and_a_moved_address_is_an_update() {
        let r = LiveNodeResolver::default();
        let a = birth("mesh1.rpc.1", 7000);
        r.apply(a.clone(), None);
        assert_eq!(r.apply(a.clone(), None), Applied::Unchanged);
        let moved = ResolvedNode { transport_addr: "127.0.0.1:7001".parse().unwrap(), ..a.clone() };
        assert_eq!(r.apply(moved.clone(), None), Applied::Updated);
        assert_eq!(exact(&r, &a).unwrap().transport_addr, moved.transport_addr);
    }

    #[test]
    fn a_draining_or_silent_node_is_found_and_only_a_departure_makes_it_gone() {
        let r = LiveNodeResolver::default();
        let a = birth("mesh1.rpc.1", 7000);
        r.apply(a.clone(), None);
        assert!(exact(&r, &a).is_ok(), "the resolver holds no lifecycle: nothing but a departure removes a node");
        assert_eq!(r.depart(&a.node_id, &a.incarnation, &a.name), Applied::Departed);
        assert_eq!(exact(&r, &a), Err(ResolveFailure::Gone));
        assert_eq!(r.resolve(&NodeTarget::CurrentPath(a.name.clone())), Err(ResolveFailure::Unknown));
        assert!(matches!(r.apply(restart(&a, 7001), Some(&a.incarnation)), Applied::Refused(Refusal::Departed { .. })), "a departed node never returns");
        assert_eq!(exact(&r, &birth("mesh1.rpc.9", 7003)), Err(ResolveFailure::Unknown), "never seen");
    }

    #[test]
    fn membership_names_the_birth_a_connection_fact_may_be_judged_against() {
        use rafka_mesh_entity::connections::{Birth, CurrentIncarnations};
        let r = LiveNodeResolver::default();
        let a = birth("mesh1.rpc.1", 7000);
        assert_eq!(r.birth(&a.node_id, &a.name), Birth::Unknown, "never heard of: nothing is judged");
        r.apply(a.clone(), None);
        assert_eq!(r.birth(&a.node_id, &a.name), Birth::Current(a.incarnation.clone()));
        let b = restart(&a, 7001);
        r.apply(b.clone(), Some(&a.incarnation));
        assert_eq!(r.birth(&a.node_id, &a.name), Birth::Current(b.incarnation.clone()), "a restart is the next birth of the same node");
        r.depart(&a.node_id, &b.incarnation, &a.name);
        assert_eq!(r.birth(&a.node_id, &a.name), Birth::Departed);
        let other = birth("mesh1.rpc.2", 7002);
        let replacement = birth("mesh1.rpc.2", 7003);
        r.apply(replacement.clone(), None);
        assert_eq!(r.birth(&other.node_id, &other.name), Birth::Departed, "another node holds the path");
    }

    #[test]
    fn a_replacement_takes_the_path_once_the_old_node_departs() {
        let r = LiveNodeResolver::default();
        let old = birth("mesh1.rpc.1", 7000);
        let new = birth("mesh1.rpc.1", 7001);
        r.apply(old.clone(), None);
        r.apply(new.clone(), None);
        let path = NodeTarget::CurrentPath(old.name.clone());
        assert_eq!(r.resolve(&path).unwrap().node_id, old.node_id, "two live nodes at a path: the holder stays until it departs");
        assert!(exact(&r, &new).is_ok());
        r.depart(&old.node_id, &old.incarnation, &old.name);
        assert_eq!(exact(&r, &old), Err(ResolveFailure::Gone));
        assert_eq!(r.resolve(&path).unwrap().node_id, new.node_id);
    }

    #[test]
    fn a_departure_heard_before_the_birth_still_holds() {
        let r = LiveNodeResolver::default();
        let old = birth("mesh1.rpc.1", 7000);
        r.depart(&old.node_id, &old.incarnation, &old.name);
        assert!(matches!(r.apply(old.clone(), None), Applied::Refused(Refusal::Departed { .. })));
        assert_eq!(exact(&r, &old), Err(ResolveFailure::Gone));
    }

    #[test]
    fn a_departure_is_forgotten_after_the_retention() {
        let r = LiveNodeResolver::new(Duration::from_millis(30));
        let a = birth("mesh1.rpc.1", 7000);
        r.apply(a.clone(), None);
        let t0 = Instant::now();
        r.depart_at(&a.node_id, &a.incarnation, &a.name, t0);
        assert_eq!(r.resolve_at(&NodeTarget::ExactNode(a.node_id.clone()), t0), Err(ResolveFailure::Gone));
        let after = t0 + Duration::from_millis(50);
        assert_eq!(r.resolve_at(&NodeTarget::ExactNode(a.node_id.clone()), after), Err(ResolveFailure::Unknown), "Gone is what this process holds now, not an archive");
    }

    #[tokio::test]
    async fn every_answer_change_ticks_and_an_unchanged_fact_does_not() {
        let r = LiveNodeResolver::default();
        let mut rx = r.changes().unwrap();
        let a = birth("mesh1.rpc.1", 7000);
        r.apply(a.clone(), None);
        assert!(rx.has_changed().unwrap());
        rx.borrow_and_update();
        r.apply(a.clone(), None);
        assert!(!rx.has_changed().unwrap(), "nothing moved");
        r.apply(restart(&a, 7001), Some(&a.incarnation));
        assert!(rx.has_changed().unwrap(), "the birth moved");
        rx.borrow_and_update();
        r.depart(&a.node_id, &a.incarnation, &a.name);
        assert!(rx.has_changed().unwrap());
    }
}

/// Membership as the connections holder reads it: the birth each node holds now. A node this
/// process has not heard of is `Unknown` and nothing is judged; a node that departed, or whose path
/// another node holds, is `Departed`.
impl rafka_mesh_entity::connections::CurrentIncarnations for LiveNodeResolver {
    fn birth(&self, node_id: &NodeId, path: &PathName) -> rafka_mesh_entity::connections::Birth {
        use rafka_mesh_entity::connections::Birth;
        match self.resolve(&NodeTarget::ExactNode(node_id.clone())) {
            Ok(n) => Birth::Current(n.incarnation),
            Err(ResolveFailure::Gone) => Birth::Departed,
            Err(_) => match self.resolve(&NodeTarget::CurrentPath(path.clone())) {
                Ok(holder) if &holder.node_id != node_id => Birth::Departed,
                _ => Birth::Unknown,
            },
        }
    }
}
