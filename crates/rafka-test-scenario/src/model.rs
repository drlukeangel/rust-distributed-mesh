//! The topology/RPC action model (i143.e8.s1, PRD §15 layer 1): a seeded generator of legal
//! actions over a fabric's meshes, checked step by step against each action's preconditions, and
//! a shrinker that reduces a failing sequence to a minimal legal one that still fails by the same
//! rule.
//!
//! The model knows node CLASSES and their CAPABILITIES (`node_admin` holds seats and executes
//! Builds; `rpc_node` serves the proof store) and the provider's capabilities (a network fault is
//! a container-provider capability). It knows no application role and no authority: who holds a
//! seat after a hand-off is decided by the live election, never by the model.
//!
//! Same seed + same initial model = the same ordered actions, every time (SplitMix64; every
//! collection the generator walks is ordered).

use crate::scenario::Operation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// A node class, as the node-admin view names it (`kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeClass {
    NodeAdmin,
    RpcNode,
}

impl NodeClass {
    /// The path segment of the class (`<mesh>.<segment>.<ordinal>`).
    pub fn segment(self) -> &'static str {
        match self {
            NodeClass::NodeAdmin => "admin",
            NodeClass::RpcNode => "rpc",
        }
    }
    /// A node of this class can hold a seat and execute Builds.
    pub fn holds_seats(self) -> bool {
        self == NodeClass::NodeAdmin
    }
    /// A node of this class serves the proof store (0x70).
    pub fn serves_proof(self) -> bool {
        self == NodeClass::RpcNode
    }
}

/// How many nodes of one class a mesh may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassBounds {
    pub min: u32,
    pub max: u32,
}

/// What the run's provider can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Capabilities {
    /// The provider can make a mesh unheard by the others (container provider only).
    pub network_faults: bool,
}

/// One node of the model: its path.name, class, birth (a replacement is a new NodeId at the same
/// path.name) and incarnation within that birth (a restart keeps the NodeId).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelNode {
    pub class: NodeClass,
    pub birth: u32,
    pub incarnation: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelMesh {
    /// path.name -> node.
    pub nodes: BTreeMap<String, ModelNode>,
    /// The mesh is unheard by every other mesh (a network fault in force).
    pub unheard: bool,
}

/// A legal action. Topology actions are Builds the node-admin rectifier executes; `Unheard` and
/// `Heal` are provider network faults; `Proof` is one typed proof-store operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// A Build adds one node of `class` to `mesh`, born at the lowest free ordinal.
    Grow { mesh: String, class: NodeClass },
    /// A Build deletes the node (drain, then terminate).
    Shrink { node: String },
    /// A Build restarts the node: same NodeId, a new incarnation.
    Restart { node: String },
    /// A Build replaces the node: a new NodeId born at the same path.name.
    Replace { node: String },
    /// A Build drains the mesh's seat holder; the remaining admins elect.
    HandOff { mesh: String },
    /// The provider cuts the mesh off from every other mesh.
    Unheard { mesh: String },
    /// The provider restores the mesh's network.
    Heal { mesh: String },
    /// The provider ends the node's runtime (SIGKILL of the exact process; a container's
    /// interface removed): drift recovery re-creates the path as a new birth. The model knows no
    /// authority, so a scenario refuses a target that is the live fabric-primary.
    Kill { node: String },
    /// The provider holds the node's runtime in place (alive and silent) until the view marks it
    /// unheard, then releases it: the same birth comes back.
    Wedge { node: String },
    /// One proof-store operation on an rpc node.
    Proof(Operation),
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

/// An action refused by its precondition, named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Illegal {
    /// The row of the sequence (0-based).
    pub step: usize,
    pub action: Action,
    /// The precondition that does not hold, by name.
    pub precondition: String,
    pub detail: String,
}

impl fmt::Display for Illegal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "row {} {}: precondition {} does not hold: {}", self.step, self.action, self.precondition, self.detail)
    }
}

/// The proof keys the generator writes (a small space, so a key is written, overwritten and
/// deleted in one run).
pub const PROOF_KEYS: u64 = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Model {
    pub meshes: BTreeMap<String, ModelMesh>,
    pub bounds: BTreeMap<NodeClass, ClassBounds>,
    pub capabilities: Capabilities,
}

fn illegal(step: usize, action: &Action, precondition: &str, detail: String) -> Illegal {
    Illegal { step, action: action.clone(), precondition: precondition.into(), detail }
}

impl Model {
    /// A fabric of `meshes`, each `(name, node_admins, rpc_nodes)`, born at ordinals 1..=n.
    pub fn new(meshes: &[(&str, u32, u32)], bounds: BTreeMap<NodeClass, ClassBounds>, capabilities: Capabilities) -> Self {
        let mut m = Model { meshes: BTreeMap::new(), bounds, capabilities };
        for (name, admins, rpcs) in meshes {
            let mut mesh = ModelMesh::default();
            for (class, n) in [(NodeClass::NodeAdmin, *admins), (NodeClass::RpcNode, *rpcs)] {
                for i in 1..=n {
                    mesh.nodes.insert(format!("{name}.{}.{i}", class.segment()), ModelNode { class, birth: 1, incarnation: 1 });
                }
            }
            m.meshes.insert((*name).to_string(), mesh);
        }
        m
    }

    /// The topology alone: every mesh's path.names and their classes, with no birth or
    /// incarnation.
    pub fn shape(&self) -> BTreeMap<String, BTreeMap<String, NodeClass>> {
        self.meshes.iter().map(|(m, ms)| (m.clone(), ms.nodes.iter().map(|(p, n)| (p.clone(), n.class)).collect())).collect()
    }

    pub fn count(&self, mesh: &str, class: NodeClass) -> u32 {
        self.meshes.get(mesh).map(|m| m.nodes.values().filter(|n| n.class == class).count() as u32).unwrap_or(0)
    }

    /// The mesh a node's path.name belongs to, and the node.
    pub fn node(&self, path: &str) -> Option<(&str, &ModelNode)> {
        self.meshes.iter().find_map(|(name, m)| m.nodes.get(path).map(|n| (name.as_str(), n)))
    }

    fn bounds_of(&self, class: NodeClass) -> ClassBounds {
        self.bounds.get(&class).copied().unwrap_or(ClassBounds { min: 0, max: u32::MAX })
    }

    fn heard_mesh(&self, step: usize, action: &Action, mesh: &str) -> Result<(), Illegal> {
        match self.meshes.get(mesh) {
            None => Err(illegal(step, action, "mesh-exists", format!("{mesh} is not a mesh of the fabric ({:?})", self.meshes.keys().collect::<Vec<_>>()))),
            Some(m) if m.unheard => Err(illegal(step, action, "mesh-heard", format!("{mesh} is unheard; a Build cannot reach it"))),
            Some(_) => Ok(()),
        }
    }

    fn live_node(&self, step: usize, action: &Action, node: &str) -> Result<(String, NodeClass), Illegal> {
        let Some((mesh, n)) = self.node(node) else {
            return Err(illegal(step, action, "node-exists", format!("{node} is not a node of the fabric")));
        };
        let (mesh, class) = (mesh.to_string(), n.class);
        self.heard_mesh(step, action, &mesh)?;
        Ok((mesh, class))
    }

    /// Never every node-admin of a mesh down: a seat-holding class keeps one admin up.
    fn keeps_an_admin(&self, step: usize, action: &Action, mesh: &str, class: NodeClass) -> Result<(), Illegal> {
        if class.holds_seats() && self.count(mesh, class) < 2 {
            return Err(illegal(step, action, "another-admin-stays-up", format!("{mesh} has {} node-admin; taking it down leaves the mesh without one", self.count(mesh, class))));
        }
        Ok(())
    }

    /// Check `action`'s preconditions against this state (row `step` of a sequence).
    pub fn check(&self, step: usize, action: &Action) -> Result<(), Illegal> {
        match action {
            Action::Grow { mesh, class } => {
                self.heard_mesh(step, action, mesh)?;
                let b = self.bounds_of(*class);
                if self.count(mesh, *class) >= b.max {
                    return Err(illegal(step, action, "below-class-max", format!("{mesh} holds {} {:?}, max {}", self.count(mesh, *class), class, b.max)));
                }
            }
            Action::Shrink { node } => {
                let (mesh, class) = self.live_node(step, action, node)?;
                let b = self.bounds_of(class);
                if self.count(&mesh, class) <= b.min {
                    return Err(illegal(step, action, "above-class-min", format!("{mesh} holds {} {:?}, min {}", self.count(&mesh, class), class, b.min)));
                }
                self.keeps_an_admin(step, action, &mesh, class)?;
            }
            Action::Restart { node } | Action::Replace { node } | Action::Kill { node } | Action::Wedge { node } => {
                let (mesh, class) = self.live_node(step, action, node)?;
                self.keeps_an_admin(step, action, &mesh, class)?;
            }
            Action::HandOff { mesh } => {
                self.heard_mesh(step, action, mesh)?;
                self.keeps_an_admin(step, action, mesh, NodeClass::NodeAdmin)?;
            }
            Action::Unheard { mesh } => {
                if !self.capabilities.network_faults {
                    return Err(illegal(step, action, "provider-network-faults", "the provider has no network fault capability".into()));
                }
                self.heard_mesh(step, action, mesh)?;
                let heard = self.meshes.values().filter(|m| !m.unheard).count();
                if heard < 2 {
                    return Err(illegal(step, action, "another-mesh-heard", format!("{heard} heard mesh; nothing is left to be unheard by")));
                }
            }
            Action::Heal { mesh } => match self.meshes.get(mesh) {
                None => return Err(illegal(step, action, "mesh-exists", format!("{mesh} is not a mesh of the fabric"))),
                Some(m) if !m.unheard => return Err(illegal(step, action, "mesh-unheard", format!("{mesh} is heard; there is nothing to heal"))),
                Some(_) => {}
            },
            Action::Proof(op) => {
                let (target, key) = match op {
                    Operation::Put { target, key, .. } | Operation::Cas { target, key, .. } | Operation::Delete { target, key } => (target, *key),
                };
                let (_, class) = self.live_node(step, action, target)?;
                if !class.serves_proof() {
                    return Err(illegal(step, action, "target-serves-proof", format!("{target} is a {class:?}; it serves no proof store")));
                }
                if key >= PROOF_KEYS {
                    return Err(illegal(step, action, "key-in-space", format!("key {key} is outside 0..{PROOF_KEYS}")));
                }
            }
        }
        Ok(())
    }

    /// Check, then apply `action` (row `step`).
    pub fn apply(&mut self, step: usize, action: &Action) -> Result<(), Illegal> {
        self.check(step, action)?;
        match action {
            Action::Grow { mesh, class } => {
                let m = self.meshes.get_mut(mesh).expect("checked");
                let ordinal = (1..).find(|i| !m.nodes.contains_key(&format!("{mesh}.{}.{i}", class.segment()))).expect("a free ordinal");
                m.nodes.insert(format!("{mesh}.{}.{ordinal}", class.segment()), ModelNode { class: *class, birth: 1, incarnation: 1 });
            }
            Action::Shrink { node } => {
                for m in self.meshes.values_mut() {
                    m.nodes.remove(node);
                }
            }
            Action::Restart { node } => {
                for m in self.meshes.values_mut() {
                    if let Some(n) = m.nodes.get_mut(node) {
                        n.incarnation += 1;
                    }
                }
            }
            Action::Replace { node } => {
                for m in self.meshes.values_mut() {
                    if let Some(n) = m.nodes.get_mut(node) {
                        n.birth += 1;
                        n.incarnation = 1;
                    }
                }
            }
            Action::HandOff { .. } | Action::Proof(_) | Action::Kill { .. } | Action::Wedge { .. } => {}
            Action::Unheard { mesh } => self.meshes.get_mut(mesh).expect("checked").unheard = true,
            Action::Heal { mesh } => self.meshes.get_mut(mesh).expect("checked").unheard = false,
        }
        Ok(())
    }

    /// Every legal action in this state, in a fixed order. A proof operation is listed once per
    /// (target, kind); its key and value are drawn by the generator.
    fn candidates(&self) -> Vec<Action> {
        let mut out = Vec::new();
        for (mesh, m) in &self.meshes {
            for class in [NodeClass::NodeAdmin, NodeClass::RpcNode] {
                out.push(Action::Grow { mesh: mesh.clone(), class });
            }
            for node in m.nodes.keys() {
                out.push(Action::Shrink { node: node.clone() });
                out.push(Action::Restart { node: node.clone() });
                out.push(Action::Replace { node: node.clone() });
                out.push(Action::Kill { node: node.clone() });
                out.push(Action::Wedge { node: node.clone() });
            }
            out.push(Action::HandOff { mesh: mesh.clone() });
            out.push(Action::Unheard { mesh: mesh.clone() });
            out.push(Action::Heal { mesh: mesh.clone() });
            for (node, n) in &m.nodes {
                if n.class.serves_proof() {
                    out.push(Action::Proof(Operation::Put { target: node.clone(), key: 0, value: String::new() }));
                    out.push(Action::Proof(Operation::Cas { target: node.clone(), key: 0, expected: String::new(), value: String::new() }));
                    out.push(Action::Proof(Operation::Delete { target: node.clone(), key: 0 }));
                }
            }
        }
        out.retain(|a| self.check(0, a).is_ok());
        out
    }
}

/// SplitMix64: the seed is the whole sequence.
#[derive(Debug, Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// `len` legal actions from `initial`, drawn by `seed`. Every action is checked against the
/// state it is drawn in and applied before the next is drawn; a state with no legal action ends
/// the sequence early.
pub fn generate(seed: u64, initial: &Model, len: usize) -> Vec<Action> {
    let mut rng = Rng(seed);
    let mut state = initial.clone();
    let mut out = Vec::with_capacity(len);
    for step in 0..len {
        let cands = state.candidates();
        if cands.is_empty() {
            break;
        }
        let mut action = cands[rng.below(cands.len() as u64) as usize].clone();
        if let Action::Proof(op) = &mut action {
            let key = rng.below(PROOF_KEYS);
            let value = format!("v{}", rng.below(1 << 16));
            match op {
                Operation::Put { key: k, value: v, .. } => (*k, *v) = (key, value),
                Operation::Cas { key: k, expected, value: v, .. } => {
                    (*k, *v) = (key, value);
                    *expected = format!("v{}", rng.below(1 << 16));
                }
                Operation::Delete { key: k, .. } => *k = key,
            }
        }
        state.apply(step, &action).expect("a candidate is legal in the state it was drawn in");
        out.push(action);
    }
    out
}

/// Replay `actions` from `initial`, checking every row's preconditions; the final state, or the
/// first illegal row.
pub fn replay(initial: &Model, actions: &[Action]) -> Result<Model, Illegal> {
    let mut state = initial.clone();
    for (step, a) in actions.iter().enumerate() {
        state.apply(step, a)?;
    }
    Ok(state)
}

/// A property's failure: the rule that broke, by name, and the row and action it broke at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub rule: String,
    pub step: usize,
    pub action: Action,
    pub detail: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: row {} {}: {}", self.rule, self.step, self.action, self.detail)
    }
}

/// The shrinker's result: the original failure, the minimal sequence and its failure (the same
/// rule), and how many candidate sequences were replayed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shrunk {
    pub original: Vec<Action>,
    pub original_failure: Failure,
    pub minimized: Vec<Action>,
    pub failure: Failure,
    pub candidates_tried: usize,
}

/// The shrinking strategy: windows of halving size (n/2, n/4, ... 1), each slid one row at a
/// time; a removal is kept when the shorter sequence is still LEGAL from `initial` and still
/// fails by the SAME rule. Whole passes repeat until one removes nothing, so the result holds no
/// contiguous window of any size whose removal still reproduces: in particular removing any one
/// remaining action, or any two adjacent ones, makes it illegal or stops it reproducing that
/// rule. `None` when the property holds on `actions` (nothing to shrink).
pub fn shrink<P>(initial: &Model, actions: &[Action], property: P) -> Option<Shrunk>
where
    P: Fn(&Model, &[Action]) -> Result<(), Failure>,
{
    let original_failure = property(initial, actions).err()?;
    let rule = original_failure.rule.clone();
    let mut tried = 0usize;
    let mut reproduces = |cand: &[Action]| -> Option<Failure> {
        tried += 1;
        replay(initial, cand).ok()?;
        property(initial, cand).err().filter(|f| f.rule == rule)
    };
    let mut cur = actions.to_vec();
    let mut failure = original_failure.clone();
    loop {
        let mut removed_any = false;
        let mut chunk = (cur.len() / 2).max(1);
        loop {
            let mut i = 0;
            while i + chunk <= cur.len() {
                let cand: Vec<Action> = cur[..i].iter().chain(cur[i + chunk..].iter()).cloned().collect();
                if let Some(f) = reproduces(&cand) {
                    cur = cand;
                    failure = f;
                    removed_any = true;
                } else {
                    i += 1;
                }
            }
            if chunk == 1 {
                break;
            }
            chunk /= 2;
        }
        if !removed_any {
            break;
        }
    }
    Some(Shrunk { original: actions.to_vec(), original_failure, minimized: cur, failure, candidates_tried: tried })
}

/// Replay `actions` from `initial` and run `observe` after every applied row. An illegal row is a
/// failure of the rule `legal-sequence`.
pub fn observe<F>(initial: &Model, actions: &[Action], mut observe: F) -> Result<Model, Failure>
where
    F: FnMut(usize, &Action, &Model) -> Result<(), Failure>,
{
    let mut state = initial.clone();
    for (step, a) in actions.iter().enumerate() {
        state.apply(step, a).map_err(|e| Failure { rule: "legal-sequence".into(), step, action: a.clone(), detail: e.to_string() })?;
        observe(step, a, &state)?;
    }
    Ok(state)
}
