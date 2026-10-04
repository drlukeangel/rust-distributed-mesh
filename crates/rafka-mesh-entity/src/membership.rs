//! Membership projection: exact-node identity, incarnation lineage and
//! endpoint-slot freshness (i143.e4.s1).
//!
//! A [`MeshNode`] fact is what one process birth of a logical node publishes.
//! A new incarnation names the incarnation it supersedes; that explicit
//! lineage, not any ordering of ids, decides which fact is current. A fact for
//! a retired incarnation is stale and refused by name, a superseded freshness
//! token never becomes current again, and a departed logical node is `Gone`
//! forever — its path may be taken over by a replacement with a new node id.

use crate::endpoint::EndpointSet;
use crate::ids::{FabricId, FreshnessToken, IncarnationId, NodeId};
use crate::path::PathName;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// One process birth of a logical node, as it publishes itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshNode {
    pub node_id: NodeId,
    pub name: PathName,
    pub fabric_id: FabricId,
    pub incarnation: IncarnationId,
    /// The incarnation this birth replaces; `None` for the first birth.
    pub supersedes: Option<IncarnationId>,
    pub endpoints: EndpointSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Joined { node_id: NodeId, incarnation: IncarnationId },
    IncarnationSuperseded { node_id: NodeId, old: IncarnationId, new: IncarnationId },
    /// Exactly one slot's freshness moved; siblings are untouched.
    SlotSuperseded { node_id: NodeId, slot: String, old: FreshnessToken, new: Option<FreshnessToken> },
    Departed { node_id: NodeId, incarnation: IncarnationId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipRefusal {
    /// The fact names an incarnation that was already superseded.
    StaleIncarnation { node_id: NodeId, incarnation: IncarnationId },
    /// The fact's incarnation does not descend from the current one.
    UnknownLineage { node_id: NodeId, current: IncarnationId, offered: IncarnationId, supersedes: Option<IncarnationId> },
    /// A departed logical node never returns; a replacement gets a new id.
    Departed { node_id: NodeId },
    /// A restart keeps the transport identity; a different one is a different node.
    FabricIdChanged { node_id: NodeId },
    /// A live node already holds this path.
    PathHeld { path: PathName, holder: NodeId },
    /// A fact re-offered a token this slot already superseded.
    TokenReused { node_id: NodeId, slot: String },
    /// Departure named an incarnation that is not current.
    NotCurrent { node_id: NodeId, incarnation: IncarnationId },
}

impl fmt::Display for MembershipRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleIncarnation { node_id, incarnation } => {
                write!(f, "node {node_id}: incarnation {incarnation} was already superseded")
            }
            Self::UnknownLineage { node_id, current, offered, supersedes } => write!(
                f,
                "node {node_id}: incarnation {offered} (supersedes {supersedes:?}) does not descend from current {current}"
            ),
            Self::Departed { node_id } => write!(f, "node {node_id} departed and never returns"),
            Self::FabricIdChanged { node_id } => write!(f, "node {node_id}: transport identity changed"),
            Self::PathHeld { path, holder } => write!(f, "path {path} is held by live node {holder}"),
            Self::TokenReused { node_id, slot } => write!(f, "node {node_id}: slot {slot} re-offered a superseded token"),
            Self::NotCurrent { node_id, incarnation } => write!(f, "node {node_id}: incarnation {incarnation} is not current"),
        }
    }
}

/// A pool's freshness question about one exact slot target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Current,
    Superseded,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution<'a> {
    Found(&'a MeshNode),
    /// The logical node existed and departed.
    Gone,
    Unknown,
}

#[derive(Debug, Default)]
pub struct Membership {
    nodes: HashMap<NodeId, MeshNode>,
    by_path: HashMap<PathName, NodeId>,
    retired: HashMap<NodeId, HashSet<IncarnationId>>,
    superseded_tokens: HashMap<(NodeId, String), HashSet<FreshnessToken>>,
    departed: HashSet<NodeId>,
}

impl Membership {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one published fact.
    pub fn apply(&mut self, fact: MeshNode) -> Result<Vec<Change>, MembershipRefusal> {
        let id = fact.node_id.clone();
        if self.departed.contains(&id) {
            return Err(MembershipRefusal::Departed { node_id: id });
        }
        if self.retired.get(&id).is_some_and(|r| r.contains(&fact.incarnation)) {
            return Err(MembershipRefusal::StaleIncarnation { node_id: id, incarnation: fact.incarnation });
        }
        for s in &fact.endpoints.0 {
            if self.superseded_tokens.get(&(id.clone(), s.slot.clone())).is_some_and(|t| t.contains(&s.freshness)) {
                return Err(MembershipRefusal::TokenReused { node_id: id, slot: s.slot.clone() });
            }
        }
        let mut changes = Vec::new();
        match self.nodes.get(&id) {
            None => {
                if let Some(holder) = self.by_path.get(&fact.name) {
                    if holder != &id {
                        return Err(MembershipRefusal::PathHeld { path: fact.name, holder: holder.clone() });
                    }
                }
                changes.push(Change::Joined { node_id: id.clone(), incarnation: fact.incarnation.clone() });
            }
            Some(cur) => {
                if cur.fabric_id != fact.fabric_id {
                    return Err(MembershipRefusal::FabricIdChanged { node_id: id });
                }
                if cur.incarnation == fact.incarnation {
                    if cur.endpoints == fact.endpoints {
                        return Ok(Vec::new());
                    }
                } else if fact.supersedes.as_ref() == Some(&cur.incarnation) {
                    changes.push(Change::IncarnationSuperseded {
                        node_id: id.clone(),
                        old: cur.incarnation.clone(),
                        new: fact.incarnation.clone(),
                    });
                } else {
                    return Err(MembershipRefusal::UnknownLineage {
                        node_id: id,
                        current: cur.incarnation.clone(),
                        offered: fact.incarnation,
                        supersedes: fact.supersedes,
                    });
                }
                for (slot, old, new) in cur.endpoints.superseded_by(&fact.endpoints) {
                    changes.push(Change::SlotSuperseded { node_id: id.clone(), slot, old, new });
                }
            }
        }
        if let Some(Change::IncarnationSuperseded { old, .. }) = changes.first() {
            self.retired.entry(id.clone()).or_default().insert(old.clone());
        }
        for c in &changes {
            if let Change::SlotSuperseded { slot, old, .. } = c {
                self.superseded_tokens.entry((id.clone(), slot.clone())).or_default().insert(old.clone());
            }
        }
        self.by_path.insert(fact.name.clone(), id.clone());
        self.nodes.insert(id, fact);
        Ok(changes)
    }

    /// The logical node leaves for good (permanent retire / replacement).
    pub fn depart(&mut self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Vec<Change>, MembershipRefusal> {
        match self.nodes.get(node_id) {
            Some(cur) if &cur.incarnation == incarnation => {}
            Some(_) | None => {
                return Err(MembershipRefusal::NotCurrent { node_id: node_id.clone(), incarnation: incarnation.clone() })
            }
        }
        let cur = self.nodes.remove(node_id).expect("checked above");
        if self.by_path.get(&cur.name) == Some(node_id) {
            self.by_path.remove(&cur.name);
        }
        for s in &cur.endpoints.0 {
            self.superseded_tokens.entry((node_id.clone(), s.slot.clone())).or_default().insert(s.freshness.clone());
        }
        self.retired.entry(node_id.clone()).or_default().insert(cur.incarnation.clone());
        self.departed.insert(node_id.clone());
        Ok(vec![Change::Departed { node_id: node_id.clone(), incarnation: cur.incarnation }])
    }

    /// Is `token` the current freshness of `node_id`'s `slot`?
    pub fn freshness(&self, node_id: &NodeId, slot: &str, token: &FreshnessToken) -> Freshness {
        if let Some(n) = self.nodes.get(node_id) {
            if n.endpoints.get(slot).is_some_and(|s| &s.freshness == token) {
                return Freshness::Current;
            }
        }
        if self.departed.contains(node_id)
            || self.superseded_tokens.get(&(node_id.clone(), slot.to_string())).is_some_and(|t| t.contains(token))
        {
            return Freshness::Superseded;
        }
        Freshness::Unknown
    }

    /// `ExactNode(node_id)`: never follows a replacement.
    pub fn resolve_exact(&self, node_id: &NodeId) -> Resolution<'_> {
        match self.nodes.get(node_id) {
            Some(n) => Resolution::Found(n),
            None if self.departed.contains(node_id) => Resolution::Gone,
            None => Resolution::Unknown,
        }
    }

    /// `CurrentPath(path)`: the node holding the path now.
    pub fn resolve_path(&self, path: &PathName) -> Resolution<'_> {
        match self.by_path.get(path).and_then(|id| self.nodes.get(id)) {
            Some(n) => Resolution::Found(n),
            None => Resolution::Unknown,
        }
    }

    pub fn nodes(&self) -> impl Iterator<Item = &MeshNode> {
        self.nodes.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::{EndpointSet, EndpointSlot};

    fn addr(port: u16) -> std::net::SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn first_birth(path: &str) -> MeshNode {
        MeshNode {
            node_id: NodeId::mint(),
            name: path.parse().unwrap(),
            fabric_id: FabricId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            endpoints: EndpointSet(vec![EndpointSlot::assign("rpc-0", addr(41001)), EndpointSlot::assign("rpc-1", addr(41002))]),
        }
    }

    /// A restart through node-admin: same node id and transport identity, new
    /// incarnation naming the old one, the fresh slot reassigned, the stable slot kept.
    fn restart(n: &MeshNode, fresh_port: u16) -> MeshNode {
        let mut r = n.clone();
        r.supersedes = Some(n.incarnation.clone());
        r.incarnation = IncarnationId::mint();
        r.endpoints = EndpointSet(vec![EndpointSlot::assign("rpc-0", addr(fresh_port)), n.endpoints.get("rpc-1").unwrap().clone()]);
        r
    }

    #[test]
    fn restart_yields_a_new_incarnation_and_a_new_token_only_on_the_changed_slot() {
        let mut m = Membership::new();
        let a = first_birth("mesh1.rpc.2");
        assert_eq!(m.apply(a.clone()).unwrap(), vec![Change::Joined { node_id: a.node_id.clone(), incarnation: a.incarnation.clone() }]);
        let b = restart(&a, 41009);
        let changes = m.apply(b.clone()).unwrap();
        let old0 = a.endpoints.get("rpc-0").unwrap().freshness.clone();
        let new0 = b.endpoints.get("rpc-0").unwrap().freshness.clone();
        assert_ne!(old0, new0);
        assert_eq!(
            changes,
            vec![
                Change::IncarnationSuperseded { node_id: a.node_id.clone(), old: a.incarnation.clone(), new: b.incarnation.clone() },
                Change::SlotSuperseded { node_id: a.node_id.clone(), slot: "rpc-0".into(), old: old0.clone(), new: Some(new0.clone()) },
            ],
            "exactly the moved slot is superseded"
        );
        let tok1 = &a.endpoints.get("rpc-1").unwrap().freshness;
        assert_eq!(m.freshness(&a.node_id, "rpc-1", tok1), Freshness::Current, "the unchanged sibling stays current");
        assert_eq!(m.freshness(&a.node_id, "rpc-0", &old0), Freshness::Superseded);
        assert_eq!(m.freshness(&a.node_id, "rpc-0", &new0), Freshness::Current);
    }

    #[test]
    fn freshness_is_equality_and_lineage_never_numeric_order() {
        let mut m = Membership::new();
        let mut a = first_birth("mesh1.rpc.1");
        a.incarnation = IncarnationId("ffff".into());
        a.endpoints.0[0].freshness = FreshnessToken("zzzz".into());
        m.apply(a.clone()).unwrap();
        let mut b = restart(&a, 41100);
        b.incarnation = IncarnationId("0000".into()); // "smaller", still newer by lineage
        b.endpoints.0[0].freshness = FreshnessToken("aaaa".into());
        m.apply(b.clone()).unwrap();
        assert_eq!(m.freshness(&a.node_id, "rpc-0", &FreshnessToken("aaaa".into())), Freshness::Current);
        assert_eq!(m.freshness(&a.node_id, "rpc-0", &FreshnessToken("zzzz".into())), Freshness::Superseded);
    }

    #[test]
    fn a_stale_incarnation_is_refused_and_its_token_never_reenters() {
        let mut m = Membership::new();
        let a = first_birth("mesh1.rpc.1");
        m.apply(a.clone()).unwrap();
        let b = restart(&a, 41200);
        m.apply(b.clone()).unwrap();
        assert_eq!(
            m.apply(a.clone()),
            Err(MembershipRefusal::StaleIncarnation { node_id: a.node_id.clone(), incarnation: a.incarnation.clone() })
        );
        // A newer birth that tries to hand the old rpc-0 token back is refused.
        let mut c = restart(&b, 41300);
        c.endpoints.0[0] = a.endpoints.get("rpc-0").unwrap().clone();
        assert_eq!(m.apply(c), Err(MembershipRefusal::TokenReused { node_id: a.node_id.clone(), slot: "rpc-0".into() }));
    }

    #[test]
    fn an_incarnation_outside_the_lineage_is_refused() {
        let mut m = Membership::new();
        let a = first_birth("mesh1.rpc.1");
        m.apply(a.clone()).unwrap();
        let mut stray = restart(&a, 41400);
        stray.supersedes = Some(IncarnationId::mint());
        assert!(matches!(m.apply(stray), Err(MembershipRefusal::UnknownLineage { .. })));
        let mut moved = restart(&a, 41401);
        moved.fabric_id = FabricId::mint();
        assert_eq!(m.apply(moved), Err(MembershipRefusal::FabricIdChanged { node_id: a.node_id.clone() }));
    }

    #[test]
    fn reapplying_a_fact_is_idempotent_and_an_in_incarnation_slot_move_supersedes_only_that_slot() {
        let mut m = Membership::new();
        let a = first_birth("mesh1.rpc.1");
        m.apply(a.clone()).unwrap();
        assert_eq!(m.apply(a.clone()).unwrap(), vec![]);
        let mut moved = a.clone();
        moved.endpoints.0[1] = EndpointSlot::assign("rpc-1", addr(41500));
        let changes = m.apply(moved).unwrap();
        assert!(matches!(&changes[..], [Change::SlotSuperseded { slot, .. }] if slot == "rpc-1"), "{changes:?}");
        assert_eq!(m.freshness(&a.node_id, "rpc-0", &a.endpoints.0[0].freshness), Freshness::Current);
    }

    #[test]
    fn replacement_takes_the_path_and_the_old_identity_is_gone() {
        let mut m = Membership::new();
        let old = first_birth("mesh1.rpc.3");
        m.apply(old.clone()).unwrap();
        let replacement = first_birth("mesh1.rpc.3");
        assert_eq!(
            m.apply(replacement.clone()),
            Err(MembershipRefusal::PathHeld { path: "mesh1.rpc.3".parse().unwrap(), holder: old.node_id.clone() }),
            "a live holder must depart first"
        );
        m.depart(&old.node_id, &old.incarnation).unwrap();
        m.apply(replacement.clone()).unwrap();
        assert_eq!(m.resolve_exact(&old.node_id), Resolution::Gone, "ExactNode(old) never follows a replacement");
        assert_eq!(m.resolve_path(&"mesh1.rpc.3".parse().unwrap()), Resolution::Found(&replacement), "CurrentPath follows it");
        assert_eq!(m.resolve_exact(&NodeId::mint()), Resolution::Unknown);
        assert_eq!(m.apply(old.clone()), Err(MembershipRefusal::Departed { node_id: old.node_id.clone() }));
        assert_eq!(m.freshness(&old.node_id, "rpc-1", &old.endpoints.0[1].freshness), Freshness::Superseded);
    }

    #[test]
    fn departure_must_name_the_current_incarnation() {
        let mut m = Membership::new();
        let a = first_birth("mesh1.rpc.1");
        m.apply(a.clone()).unwrap();
        let stale = IncarnationId::mint();
        assert_eq!(m.depart(&a.node_id, &stale), Err(MembershipRefusal::NotCurrent { node_id: a.node_id.clone(), incarnation: stale }));
    }

    #[test]
    fn facts_round_trip_as_json_for_gossip() {
        let a = first_birth("mesh1.rpc.1");
        let back: MeshNode = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(back, a);
    }
}
