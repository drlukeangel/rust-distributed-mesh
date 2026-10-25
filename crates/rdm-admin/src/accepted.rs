//! The accepted topology: what a Build carries (RDM #47).
//!
//! The current Build (the Build entities read last in, first out, R-BI3:
//! [`crate::build_state::BuildStateAdapter::current_build`]) holds the complete Fabric topology as
//! exact node `path.name`s per Mesh ([`FabricTopology`]). A submitted change ([`TopologyChange`]) is
//! input only: [`compile`] applies it once to the current accepted topology and yields the next
//! complete map; the change itself is kept on the Build as history, never read to plan. Planning
//! is always `topology − observed` ([`plan`]). A restart, a recovery or a recipe changes no topology:
//! it is an attempt of the current Build carrying its [`AttemptAction`].
//!
//! Counts (`FabricDesired`, `MeshDesired`) stay API convenience: the compiler resolves them to
//! exact paths before anything is persisted, so the executor never decides which node a count
//! means.

use rdm_core::meta::PathConfig;
use crate::build::{BuildId, BuildOperation, BuildPlan, BuildReject, FabricDesired, MeshDesired, PinnedBirth};
use rdm_core::build::RecipeStep;
use crate::build_state::{BuildStateAdapter, BuildStateError};
use crate::fabric_storage::{FabricIdentity, FabricStorage, FabricStorageError};
use crate::model::{is_valid_mesh_name, IncarnationId, MeshId, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// One Mesh of the accepted topology: every node it should have, by `path.name`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshTopology {
    /// The mesh's name.
    pub name: String,
    /// The mesh's id (node-identity-and-state.md §2.7, "Who mints it"): minted once, when the mesh
    /// enters the accepted topology, and carried by every Build after it, never reissued. Every run of
    /// the mesh's `create-mesh` operation, by any executor and any attempt, claims the mesh's name with
    /// it; a mesh removed and created again enters the topology anew with a new id (R-MN1).
    pub mesh_id: MeshId,
    /// Every node the mesh should have, by `path.name`.
    pub nodes: BTreeSet<PathName>,
    /// The typed desired-state config of every materialized path, explicit after ingress
    /// normalization: one entry per node, no entry without a node (`validate`).
    #[serde(default)]
    pub path_config_by_path: BTreeMap<PathName, PathConfig>,
}

impl MeshTopology {
    /// A new mesh from its counts, entering the accepted topology: its id minted, and every path
    /// minted with its kind's migration default path config (ingress normalization; the Build is
    /// explicit from here).
    pub fn of(desired: &MeshDesired) -> Self {
        Self::with_id(desired, MeshId::mint())
    }

    /// The mesh `mesh_id` from its counts: every path minted with its kind's migration default path config.
    fn with_id(desired: &MeshDesired, mesh_id: MeshId) -> Self {
        let mut nodes = BTreeSet::new();
        let mut path_config_by_path = BTreeMap::new();
        for (kind, want) in desired.counts() {
            for ord in 1..=want {
                let path = PathName::new(&desired.name, kind, ord);
                path_config_by_path.insert(path.clone(), PathConfig::default_for(kind));
                nodes.insert(path);
            }
        }
        Self { name: desired.name.clone(), mesh_id, nodes, path_config_by_path }
    }
    /// The path config of one of this mesh's paths; `None` for a path this mesh does not hold.
    pub fn path_config(&self, path: &PathName) -> Option<&PathConfig> {
        self.path_config_by_path.get(path)
    }
    /// Set one path's config explicitly (a build request that carries it); refused by name for a
    /// path the mesh does not hold.
    pub fn set_path_config(&mut self, path: &PathName, config: PathConfig) -> Result<(), BuildReject> {
        if !self.nodes.contains(path) {
            return Err(BuildReject::UnknownNode { node: path.to_string() });
        }
        self.path_config_by_path.insert(path.clone(), config);
        Ok(())
    }

    /// This mesh's shape as a desired mesh: every kind's count.
    pub fn desired(&self) -> MeshDesired {
        MeshDesired::of(self.name.clone(), NodeKind::ALL.map(|k| (k, self.count(k))))
    }

    /// The nodes of `kind`.
    pub fn nodes_of_type(&self, kind: NodeKind) -> impl Iterator<Item = &PathName> {
        self.nodes.iter().filter(move |p| p.kind == kind)
    }

    /// The number of nodes of `kind`.
    pub fn count(&self, kind: NodeKind) -> u32 {
        self.nodes_of_type(kind).count() as u32
    }
}

/// The complete accepted Fabric topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricTopology {
    /// The fabric's name.
    pub fabric: String,
    /// The accepted meshes, by name.
    pub meshes: BTreeMap<String, MeshTopology>,
    /// The fabric's launch set (proposal roll.md step 2): the executable set every launch of the
    /// fabric runs, by kind, read from the current Build at each launch. Day 0's is the bootstrap
    /// admin's `RDM_EXECUTABLE_BINDINGS`; every compile carries it forward unchanged, and only a
    /// [`TopologyChange::Roll`] replaces it. A topology written before it was carried decodes with
    /// none, and an admin whose current Build carries none launches from the set it started with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bindings: Option<rdm_core::binding::BindingSet>,
}

impl FabricTopology {
    /// Day 0: the bootstrap admin's own mesh `mesh_id`, one node-admin. The mesh's id is the one the
    /// bootstrap admin was born with, never minted here.
    pub fn root(fabric: &str, mesh: &str, mesh_id: MeshId) -> Self {
        let mut meshes = BTreeMap::new();
        meshes.insert(mesh.to_string(), MeshTopology::with_id(&MeshDesired::of(mesh, [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 0)]), mesh_id));
        Self { fabric: fabric.into(), meshes, bindings: None }
    }

    /// The binding the launch set fixes for `kind`, when the topology carries a set that binds it.
    pub fn binding_of(&self, kind: NodeKind) -> Option<&rdm_core::binding::Binding> {
        self.bindings.as_ref().and_then(|s| s.of(kind))
    }

    /// The accepted mesh named `name`.
    pub fn mesh(&self, name: &str) -> Option<&MeshTopology> {
        self.meshes.get(name)
    }

    /// Whether the topology names `node`.
    pub fn contains(&self, node: &PathName) -> bool {
        self.meshes.get(&node.mesh).is_some_and(|m| m.nodes.contains(node))
    }

    /// Every invariant a persisted topology holds.
    pub fn validate(&self) -> Result<(), BuildReject> {
        if self.meshes.is_empty() {
            return Err(BuildReject::EmptyFabric);
        }
        for (name, m) in &self.meshes {
            if name != &m.name {
                return Err(BuildReject::InvalidMeshName { mesh: m.name.clone() });
            }
            if !is_valid_mesh_name(name) {
                return Err(BuildReject::InvalidMeshName { mesh: name.clone() });
            }
            if m.count(NodeKind::NodeAdmin) == 0 {
                return Err(BuildReject::MeshWithoutAdmin { mesh: name.clone() });
            }
            if m.nodes.iter().any(|p| p.mesh != *name) {
                return Err(BuildReject::InvalidMeshName { mesh: name.clone() });
            }
            let missing: Vec<String> = m.nodes.iter().filter(|p| !m.path_config_by_path.contains_key(p)).map(ToString::to_string).collect();
            let extra: Vec<String> = m.path_config_by_path.keys().filter(|p| !m.nodes.contains(p)).map(ToString::to_string).collect();
            if !missing.is_empty() || !extra.is_empty() {
                return Err(BuildReject::PathConfigMismatch { mesh: name.clone(), missing, extra });
            }
        }
        Ok(())
    }

    /// The topology an observed view realizes: every live node's path, per mesh (a view with no
    /// live node of a mesh still names the mesh), each mesh under the id the view holds for it. For
    /// fixtures and Day-0 adoption, never planning: a mesh whose id no member of the view has stated
    /// takes a fresh id, as a fixture's new mesh does.
    pub fn of_observed(observed: &Topology) -> Self {
        let empty = |name: &str, mesh_id: Option<MeshId>| MeshTopology { name: name.to_string(), mesh_id: mesh_id.unwrap_or_else(MeshId::mint), nodes: BTreeSet::new(), path_config_by_path: BTreeMap::new() };
        let mut meshes: BTreeMap<String, MeshTopology> = observed.meshes.iter().map(|m| (m.name.clone(), empty(&m.name, m.id.clone()))).collect();
        for n in observed.members().filter(|n| n.status.is_live()) {
            let mesh = meshes.entry(n.mesh.clone()).or_insert_with(|| empty(&n.mesh, None));
            mesh.path_config_by_path.insert(n.name.clone(), PathConfig::default_for(n.name.kind));
            mesh.nodes.insert(n.name.clone());
        }
        Self { fabric: observed.fabric.name.clone(), meshes, bindings: None }
    }

    /// The topology as per-mesh counts (the view's convenience shape).
    pub fn desired(&self) -> FabricDesired {
        FabricDesired {
            fabric: self.fabric.clone(),
            meshes: self.meshes.values().map(MeshTopology::desired).collect(),
        }
    }
}

/// What a control surface submits. Applied once by [`compile`]; kept on the Build as history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TopologyChange {
    /// The whole fabric's meshes and counts (`POST /api/build`).
    ReconcileFabric {
        /// The fabric's desired meshes.
        desired: FabricDesired,
    },
    /// One mesh's counts (grow/shrink).
    ReconcileMesh {
        /// The mesh's desired counts.
        desired: MeshDesired,
    },
    /// `POST /api/nodes/spawn`: one more node of `node_kind` in `mesh`.
    AddNode {
        /// The mesh to add the node to.
        mesh: String,
        /// The kind of node to add.
        node_kind: NodeKind,
    },
    /// `DELETE /api/nodes/{name}`.
    RemoveNode {
        /// The node to remove.
        node: PathName,
    },
    /// `POST /api/meshes`.
    CreateMesh {
        /// The mesh's desired counts.
        desired: MeshDesired,
    },
    /// `DELETE /api/meshes/{id|name}`.
    RemoveMesh {
        /// The mesh to remove.
        mesh: String,
    },
    /// `fabric.delete` (lifecycles/fabric-delete.md).
    DeleteFabric,
    /// `build.create { Change(Roll { bindings }) }` (proposal roll.md): the set becomes the
    /// topology's launch set, and a mesh with one node-admin gains a second (R4).
    Roll {
        /// The executable set rolled to.
        bindings: rdm_core::binding::BindingSet,
    },
    /// `build.create { Change(Reconcile { based_on, nodes }) }` (proposal executable-bindings-at-create.md
    /// rule 1): the complete topology, every node by its `path.name` with its config, the executable
    /// included. `based_on` is checked against the current Build at the door, before this compiles.
    Reconcile {
        /// The build_id of the current Build the caller read.
        based_on: BuildId,
        /// Every node of the fabric, by `path.name`, with its config.
        nodes: Vec<(PathName, PathConfig)>,
    },
}

impl TopologyChange {
    /// Whether the change is a roll.
    pub fn is_roll(&self) -> bool {
        matches!(self, Self::Roll { .. })
    }

    /// Whether the Build this change opens owes the replacements its rows say are left: each path whose
    /// target executable ([`target_executable`]) differs from what its row records is replaced, in RDM's
    /// order (proposal executable-bindings-at-create.md rule 5).
    pub fn plans_replacements(&self) -> bool {
        matches!(self, Self::Roll { .. } | Self::Reconcile { .. })
    }
}

/// The executable `topology` fixes for the node at `path`: its entry's (a request named it, proposal
/// executable-bindings-at-create.md rule 2), else the topology's launch set's for its kind. `None` when
/// neither names one: no replacement is owed for it.
pub fn target_executable<'a>(topology: &'a FabricTopology, path: &PathName) -> Option<&'a rdm_core::binding::Binding> {
    topology.mesh(&path.mesh).and_then(|m| m.path_config(path)).and_then(|c| c.executable.as_ref()).or_else(|| topology.binding_of(path.kind))
}

/// Whether the node at a path is replaced: its target names an executable and its live row records
/// another (or none). A path no live row is written at is created, never replaced.
fn replaced_by_target(target: Option<&rdm_core::binding::Binding>, row: Option<&RecordedRow>) -> bool {
    target.is_some_and(|b| row.is_some_and(|r| r.sha256.as_deref() != Some(b.sha256.as_str())))
}

/// What one attempt of a Build does beyond realizing its topology: a same-path restart, a standalone
/// node op, a recovery or a recipe, fenced to the births it acts on. Never part of the topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AttemptAction {
    /// Restart the exact birth at a path.
    Restart {
        /// The node.
        path: PathName,
        /// The birth restarted.
        from_incarnation: IncarnationId,
    },
    /// `node.connections.delete { All }` of the exact birth at a path; it keeps running
    /// (node-connections-delete.md).
    DeleteConnections {
        /// The node.
        path: PathName,
        /// The birth whose connections are deleted.
        from_incarnation: IncarnationId,
    },
    /// Stop the exact birth at a path: it drains, cuts its mesh connections and parks, its process
    /// alive; no departure (node-stop.md).
    Stop {
        /// The node.
        path: PathName,
        /// The birth stopped.
        from_incarnation: IncarnationId,
    },
    /// Start the parked exact birth at a path: the same process rejoins as itself (node-start.md).
    Start {
        /// The node.
        path: PathName,
        /// The birth started.
        from_incarnation: IncarnationId,
    },
    /// Recover the node at a path from its exact exited birth (node-recovery.md): the exited
    /// instance is deleted and a new node is created at the path over its data dir.
    Recover {
        /// The node.
        path: PathName,
        /// The exited instance's node.id.
        node_id: NodeId,
        /// The exited birth.
        from_incarnation: IncarnationId,
        /// Fabric recovery's backbone arm (fabric-recovery.md step 3, Q142, Q148): it terminates nothing
        /// in the mesh. Its node.delete of the exact lost birth writes that birth's row deleted with no
        /// `TerminateRuntime` and releases the name, then node.create at the path.name; a delete that
        /// finds another birth at the path writes nothing.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        backbone: bool,
    },
    /// Delete the stopped exact birth a rename left at `<path>.old` (node-delete.md): its stop steps
    /// are done; the provider's exact Exited is the proof, then `NodeDeleted` and its Deleted row. The
    /// topology never names a `.old` path: this action is its only door outside a recipe.
    Delete {
        /// The node's `.old` path.
        path: PathName,
        /// The birth deleted.
        from_incarnation: IncarnationId,
    },
    /// A recipe (`BuildChange::Recipe`; build.md §1: "Rafka's replace recipe is submitted as one Build
    /// of node primitives"; lifecycles/node-replace.md): its steps run in order, each as its op's own
    /// operation ([`BuildOperation::RecipeStep`]), and a step whose `Complete` is receipted for this
    /// recipe never runs again. `pinned` is the exact birth each path the recipe acts on held when it
    /// was accepted; a path an earlier step creates holds no birth yet and is not pinned.
    Recipe {
        /// The steps, in order.
        steps: Vec<RecipeStep>,
        /// The birth each path a step acts on held at acceptance.
        pinned: Vec<(PathName, PinnedBirth)>,
    },
    /// Terminate the runtime of a deleted birth heard again (node-investigation.md, Q110, Q128): the
    /// existing `TerminateRuntime` on its exact runtime; its row and departure stand.
    Terminate {
        /// The path.name the birth held.
        path: PathName,
        /// The birth's node.id.
        node_id: NodeId,
        /// The birth.
        from_incarnation: IncarnationId,
    },
}

/// A recipe accepted against `observed` (the fabric-primary's view): refused by name when it has no
/// step, names one operation twice, or acts on a path that holds no birth and that no earlier step
/// creates or renames a node to; otherwise the action that pins every path's birth.
pub(crate) fn recipe_action(steps: Vec<RecipeStep>, observed: &Topology) -> Result<AttemptAction, BuildReject> {
    if steps.is_empty() {
        return Err(BuildReject::EmptyRecipe);
    }
    let mut keys = BTreeSet::new();
    for step in &steps {
        let key = crate::build::recipe_step_key(step);
        if !keys.insert(key.clone()) {
            return Err(BuildReject::RecipeStepRepeated { operation: key });
        }
    }
    // Each path a step acts on before any earlier step makes a node there is pinned to the birth the
    // view holds at it now.
    let mut pinned: Vec<(PathName, PinnedBirth)> = Vec::new();
    let mut made: BTreeSet<PathName> = BTreeSet::new();
    for step in &steps {
        let path = step.node();
        if !made.contains(path) && !pinned.iter().any(|(p, _)| p == path) {
            if let Some((node_id, incarnation)) = observed.node(path).and_then(|n| n.incarnation_id.clone().map(|i| (n.node_id.clone(), i))) {
                pinned.push((path.clone(), PinnedBirth { node_id, incarnation }));
            }
        }
        match step {
            RecipeStep::Create { node, .. } => {
                made.insert(node.clone());
            }
            RecipeStep::Update { name, .. } => {
                made.insert(name.clone());
            }
            _ => {}
        }
    }
    recipe_births(&steps, &pinned)?;
    Ok(AttemptAction::Recipe { steps, pinned })
}

/// The exact birth each step of a recipe is fenced to: the pinned birth of the node its path names
/// when the step runs, followed through the recipe's own creates and renames; `None` for a node an
/// earlier step creates (its executor resolves it at its path). `Err` names the first step acting on
/// a path that holds no node when it runs.
pub(crate) fn recipe_births(steps: &[RecipeStep], pinned: &[(PathName, PinnedBirth)]) -> Result<Vec<Option<PinnedBirth>>, BuildReject> {
    let mut at: BTreeMap<PathName, Option<PinnedBirth>> = pinned.iter().map(|(p, b)| (p.clone(), Some(b.clone()))).collect();
    let mut out = Vec::with_capacity(steps.len());
    for (i, step) in steps.iter().enumerate() {
        let unknown = || BuildReject::RecipeNodeUnknown { step: i as u32 + 1, node: step.node().to_string() };
        let birth = match step {
            RecipeStep::Create { node, .. } => {
                at.insert(node.clone(), None);
                None
            }
            RecipeStep::Update { node, name } => {
                let birth = at.remove(node).ok_or_else(unknown)?;
                at.insert(name.clone(), birth.clone());
                birth
            }
            RecipeStep::Delete { node } => at.remove(node).ok_or_else(unknown)?,
            RecipeStep::Stop { node } | RecipeStep::Start { node } | RecipeStep::DeleteConnections { node } | RecipeStep::LeaveMesh { node } => at.get(node).cloned().ok_or_else(unknown)?,
        };
        out.push(birth);
    }
    Ok(out)
}

fn validate_counts(m: &MeshDesired) -> Result<(), BuildReject> {
    if !is_valid_mesh_name(&m.name) {
        return Err(BuildReject::InvalidMeshName { mesh: m.name.clone() });
    }
    if m.node_admin == 0 {
        return Err(BuildReject::MeshWithoutAdmin { mesh: m.name.clone() });
    }
    Ok(())
}

/// The ordinal `observed` holds the cohort's primary at, if any: a shrink never drops it.
fn primary_ordinal(observed: &Topology, mesh: &str, kind: NodeKind) -> Option<u32> {
    observed.node_type_primary(mesh, kind).map(|n| n.name.ordinal)
}

/// Bring `mesh`'s cohorts to `counts`: grow fills the lowest free ordinals, shrink drops the
/// highest ordinals that are not the observed primary.
fn resize(mesh: &mut MeshTopology, counts: &MeshDesired, observed: &Topology) {
    for (kind, want) in counts.counts() {
        let mut have: Vec<u32> = mesh.nodes_of_type(kind).map(|p| p.ordinal).collect();
        have.sort_unstable();
        let keep_ord = primary_ordinal(observed, &mesh.name, kind);
        while (have.len() as u32) < want {
            let ord = (1..).find(|o| !have.contains(o)).expect("a free ordinal");
            have.push(ord);
            have.sort_unstable();
            let path = PathName::new(&mesh.name, kind, ord);
            mesh.path_config_by_path.insert(path.clone(), PathConfig::default_for(kind));
            mesh.nodes.insert(path);
        }
        while (have.len() as u32) > want {
            let drop = have.iter().rev().copied().find(|o| Some(*o) != keep_ord).or_else(|| have.last().copied()).expect("a member to drop");
            have.retain(|o| *o != drop);
            let path = PathName::new(&mesh.name, kind, drop);
            mesh.path_config_by_path.remove(&path);
            mesh.nodes.remove(&path);
        }
    }
}

/// Apply `change` once to `current`, against `observed` (which decides only what a count means:
/// the primary a shrink keeps). The result is the complete next topology, or a refusal by name.
pub fn compile(current: &FabricTopology, change: &TopologyChange, observed: &Topology) -> Result<FabricTopology, BuildReject> {
    compile_against(current, change, observed, &Recorded::new())
}

/// [`compile`], given what the node rows record of each path's executable (`recorded`,
/// [`recorded_executables`]): a [`TopologyChange::Reconcile`] that replaces a mesh's only node-admin
/// adds that mesh a second (R4), which only the rows can say.
pub fn compile_against(current: &FabricTopology, change: &TopologyChange, observed: &Topology, recorded: &Recorded) -> Result<FabricTopology, BuildReject> {
    let mut next = current.clone();
    match change {
        TopologyChange::ReconcileFabric { desired } => {
            if desired.fabric != current.fabric {
                return Err(BuildReject::FabricMismatch { requested: desired.fabric.clone(), fabric: current.fabric.clone() });
            }
            if desired.meshes.is_empty() {
                return Err(BuildReject::EmptyFabric);
            }
            let mut names = BTreeSet::new();
            for m in &desired.meshes {
                validate_counts(m)?;
                if !names.insert(m.name.clone()) {
                    return Err(BuildReject::DuplicateMesh { mesh: m.name.clone() });
                }
            }
            next.meshes.retain(|name, _| names.contains(name));
            for m in &desired.meshes {
                // A mesh the topology does not hold enters it here: its id is minted now, once.
                let mesh = next.meshes.entry(m.name.clone()).or_insert_with(|| MeshTopology { name: m.name.clone(), mesh_id: MeshId::mint(), nodes: BTreeSet::new(), path_config_by_path: BTreeMap::new() });
                resize(mesh, m, observed);
            }
        }
        TopologyChange::ReconcileMesh { desired } => {
            validate_counts(desired)?;
            let mesh = next.meshes.get_mut(&desired.name).ok_or_else(|| BuildReject::UnknownMesh { mesh: desired.name.clone() })?;
            resize(mesh, desired, observed);
        }
        TopologyChange::AddNode { mesh, node_kind } => {
            let m = next.meshes.get_mut(mesh).ok_or_else(|| BuildReject::UnknownMesh { mesh: mesh.clone() })?;
            let mut grown = m.desired();
            *grown.count_mut(*node_kind) += 1;
            resize(m, &grown, observed);
        }
        TopologyChange::RemoveNode { node } => {
            let m = next.meshes.get_mut(&node.mesh).ok_or_else(|| BuildReject::UnknownMesh { mesh: node.mesh.clone() })?;
            if !m.nodes.contains(node) {
                return Err(BuildReject::UnknownNode { node: node.to_string() });
            }
            // The authority removes a birth it sees: a view that has not heard the node yet refuses
            // by name rather than accepting a Build it would close with nothing to do. Which birth it
            // sees may be deleted (live, stopped, or recorded os-stopped) is its door's, which reads the
            // status rows (`ControlPlane::submit_checked`).
            if observed.node(node).is_none() {
                return Err(BuildReject::UnknownNode { node: node.to_string() });
            }
            if node.kind == NodeKind::NodeAdmin && m.count(NodeKind::NodeAdmin) <= 1 {
                return Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh: node.mesh.clone() });
            }
            m.nodes.remove(node);
            m.path_config_by_path.remove(node);
        }
        TopologyChange::CreateMesh { desired } => {
            validate_counts(desired)?;
            if next.meshes.contains_key(&desired.name) {
                return Err(BuildReject::MeshAlreadyExists { mesh: desired.name.clone() });
            }
            // The new mesh's id is minted here, once, at acceptance.
            next.meshes.insert(desired.name.clone(), MeshTopology::of(desired));
        }
        TopologyChange::RemoveMesh { mesh } => {
            if !next.meshes.contains_key(mesh) {
                return Err(BuildReject::UnknownMesh { mesh: mesh.clone() });
            }
            if next.meshes.len() <= 1 {
                return Err(BuildReject::EmptyFabric);
            }
            next.meshes.remove(mesh);
        }
        TopologyChange::DeleteFabric => {
            next.meshes.clear();
            return Ok(next);
        }
        // A roll changes no path.name: the set becomes every path's target binding by kind, and a mesh
        // whose only node-admin is its primary gains a second at its next free ordinal, created first
        // from the set and kept in the topology after the roll (proposal roll.md step 6, R4 arm A).
        TopologyChange::Roll { bindings } => {
            next.bindings = Some(bindings.clone());
            for m in next.meshes.values_mut() {
                // The set is every path's target: an entry a request named gives way to it.
                m.path_config_by_path.values_mut().for_each(|c| c.executable = None);
                if m.count(NodeKind::NodeAdmin) == 1 {
                    let mut grown = m.desired();
                    *grown.count_mut(NodeKind::NodeAdmin) += 1;
                    resize(m, &grown, observed);
                }
            }
        }
        TopologyChange::Reconcile { nodes, .. } => next.meshes = reconciled(current, nodes, recorded)?,
    }
    next.validate()?;
    Ok(next)
}

/// The meshes a [`TopologyChange::Reconcile`] names (proposal executable-bindings-at-create.md rule 1):
/// exactly the listed nodes, each with its config. A mesh the topology holds keeps its id; a mesh named
/// for the first time enters under an id minted now. A path the request creates names its executable,
/// else it is refused by name; a path the topology holds that the request lists with none keeps its
/// entry's. A mesh whose only node-admin the request replaces (its entry names another executable than
/// its row records) gains a second node-admin at the next free `path.name` of that type, on the new
/// executable (R4): it is in the topology from acceptance, created first, and stays. A created path's
/// entry carries the tag labels the request lists, which the node carries from its claim; a held path
/// keeps its entry's. Refused by name: no node
/// listed (that is fabric.delete), a path named twice, a `.old` path, a mesh name outside the grammar,
/// tags outside the label grammar or over the cap.
fn reconciled(current: &FabricTopology, nodes: &[(PathName, PathConfig)], recorded: &Recorded) -> Result<BTreeMap<String, MeshTopology>, BuildReject> {
    if nodes.is_empty() {
        return Err(BuildReject::NoNodeListed);
    }
    let mut by_mesh: BTreeMap<String, BTreeMap<PathName, PathConfig>> = BTreeMap::new();
    for (path, config) in nodes {
        if path.old {
            return Err(BuildReject::UnknownNode { node: path.to_string() });
        }
        if !is_valid_mesh_name(&path.mesh) {
            return Err(BuildReject::InvalidMeshName { mesh: path.mesh.clone() });
        }
        if let Some(detail) = rdm_core::status::tags_refusal(&config.tags) {
            return Err(BuildReject::TagsRefused { node: path.to_string(), detail });
        }
        if by_mesh.entry(path.mesh.clone()).or_default().insert(path.clone(), config.clone()).is_some() {
            return Err(BuildReject::NodeNamedTwice { node: path.to_string() });
        }
    }
    let mut meshes = BTreeMap::new();
    for (name, paths) in by_mesh {
        let held = current.meshes.get(&name);
        let mesh_id = held.map(|m| m.mesh_id.clone()).unwrap_or_else(MeshId::mint);
        let mut mesh = MeshTopology { name: name.clone(), mesh_id, nodes: BTreeSet::new(), path_config_by_path: BTreeMap::new() };
        for (path, mut config) in paths {
            match held.and_then(|m| m.path_config(&path)) {
                // A held path keeps its entry's tags: a request's tags are a create's input (rule 1), a
                // replacement inherits its predecessor's (rule 5: "its config and tags are inherited from
                // the Deleted row"), and a live node's labels are node.tags.create's and node.tags.delete's.
                Some(entry) => {
                    if config.executable.is_none() {
                        config.executable = entry.executable.clone();
                    }
                    config.tags = entry.tags.clone();
                }
                None if config.executable.is_none() => return Err(BuildReject::ExecutableMissing { node: path.to_string() }),
                None => {}
            }
            mesh.nodes.insert(path.clone());
            mesh.path_config_by_path.insert(path, config);
        }
        let admins: Vec<PathName> = mesh.nodes_of_type(NodeKind::NodeAdmin).cloned().collect();
        if let [admin] = admins.as_slice() {
            let entry = mesh.path_config(admin).cloned().expect("a listed path carries its config");
            if held.is_some_and(|m| m.nodes.contains(admin)) && replaced_by_target(entry.executable.as_ref(), recorded.get(admin)) {
                let ordinal = (1..).find(|o| !mesh.nodes.contains(&PathName::new(&name, NodeKind::NodeAdmin, *o))).expect("a free ordinal");
                let added = PathName::new(&name, NodeKind::NodeAdmin, ordinal);
                mesh.nodes.insert(added.clone());
                mesh.path_config_by_path.insert(added, entry);
            }
        }
        meshes.insert(name, mesh);
    }
    Ok(meshes)
}

/// Plan `topology − observed`, plus `action` where it still applies. Deterministic, idempotent:
/// what observed reality already satisfies is not planned. Creates come before retirements, meshes
/// in name order, nodes in path order.
pub fn plan(topology: &FabricTopology, observed: &Topology, action: Option<&AttemptAction>) -> BuildPlan {
    plan_with(topology, observed, action, ActionProgress::default(), &BTreeSet::new(), &[], None)
}

/// How far an attempt's action got, from the Build's receipts. The view alone cannot say: a stopped
/// birth leaves every book, and a recovered or renamed path names another birth.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActionProgress {
    /// A standalone node.connections.delete, stop, start or delete began for its birth: its first step's receipt.
    started: bool,
    /// Its `Complete` is receipted for the birth.
    finished: bool,
    /// How far a recovery's two operations got for its exited birth.
    recovery: RecoveryProgress,
    /// Per step of a recipe, whether its operation's `Complete` is receipted in an attempt that
    /// carried the recipe.
    recipe: Vec<bool>,
}

/// How far a node recovery's two operations (node-recovery.md "Steps": node.delete, then node.create)
/// got for its exited birth, from the Build's receipts: each is done once its `Complete` follows the
/// one before it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RecoveryProgress {
    /// Nothing of the recovery is done: node.delete of the exited instance is owed.
    #[default]
    Owed,
    /// The node.delete completed for the exited birth: node.create at its path.name is owed.
    Deleted,
    /// The node.create completed after it: the exited birth is recovered. Nothing follows the create:
    /// its claim seeded config and tags from the Deleted row (proposal node-properties-own-commands.md
    /// §8).
    Recovered,
}

impl ActionProgress {
    /// The progress of `build`'s action, `default` for an attempt that carries none.
    pub(crate) fn of(build: &crate::build_state::BuildProjection) -> Self {
        // A standalone drain, stop, start or parked delete: the receipts of its operation in an
        // attempt that carried this exact action (its `Opened` fact fenced the attempt to the birth)
        // are this birth's. Its first step's receipt says the operation began; its `Complete`, that
        // it completed for the birth.
        if let Some(action @ (AttemptAction::DeleteConnections { path, .. } | AttemptAction::Stop { path, .. } | AttemptAction::Start { path, .. } | AttemptAction::Delete { path, .. })) = &build.action {
            let (operation, first) = match action {
                AttemptAction::DeleteConnections { .. } => (format!("node.connections.delete:{path}"), "DeleteConnections"),
                AttemptAction::Start { .. } => (format!("node.start:{path}"), "StartNode"),
                // node.delete's one key, whether the birth is deleted stopped or live (`plan_with`).
                AttemptAction::Delete { .. } => (format!("node.delete:{path}"), "NodeDeleting"),
                _ => (format!("node.stop:{path}"), "StopNode"),
            };
            let done = |step: &str| build.steps.iter().any(|s| s.operation == operation && s.step == step && s.outcome == crate::build_state::StepOutcome::Complete && build.carried(s.attempt, action));
            return Self { started: done(first), finished: done("Complete"), ..Self::default() };
        }
        // A termination of a deleted birth heard again: its one operation's receipts in an attempt that
        // carried it (Q128).
        if let Some(action @ AttemptAction::Terminate { path, from_incarnation, .. }) = &build.action {
            let operation = format!("node.terminate:{path}@{}", from_incarnation.as_str());
            let done = |step: &str| build.steps.iter().any(|s| s.operation == operation && s.step == step && s.outcome == crate::build_state::StepOutcome::Complete && build.carried(s.attempt, action));
            return Self { started: done("TerminateRuntime"), finished: done("Complete"), ..Self::default() };
        }
        // A recovery: how far its two operations got for the exited birth.
        if let Some(AttemptAction::Recover { path, node_id, from_incarnation, .. }) = &build.action {
            let recovery = recovery_progress(build, path, node_id, from_incarnation).0;
            return Self { started: false, finished: recovery == RecoveryProgress::Recovered, recovery, ..Self::default() };
        }
        // A recipe: each step is done once its operation's `Complete` is receipted in an attempt that
        // carried this recipe (a recipe names each operation once, `recipe_action`).
        if let Some(action @ AttemptAction::Recipe { steps, .. }) = &build.action {
            let recipe = steps
                .iter()
                .map(|step| {
                    let operation = crate::build::recipe_step_key(step);
                    build.steps.iter().any(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && build.carried(s.attempt, action))
                })
                .collect();
            return Self { recipe, ..Self::default() };
        }
        Self::default()
    }
}

/// How far the recovery of the exited birth `(node_id, incarnation)` at `path` got in `build`
/// (node-recovery.md "Steps"), with the attempt that completed its last done operation. The delete
/// is done once its `node.delete:<path>` `NodeDeleted` names the exited birth (its output is the
/// node.delete's lifecycle operation) and the operation's `Complete` follows at that attempt or later
/// (a rerun reuses the `NodeDeleted`); the create, and with it the recovery, once a
/// `node.create:<path>` `Complete` follows that. A `Complete` from before the deletion (the path's
/// original create, an earlier recovery's) is not this recovery's.
pub(crate) fn recovery_progress(build: &crate::build_state::BuildProjection, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> (RecoveryProgress, Option<u32>) {
    let delete = format!("node.delete:{path}");
    let completed_from = |operation: &str, from: u32| {
        build
            .steps
            .iter()
            .filter(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && s.attempt >= from)
            .map(|s| s.attempt)
            .min()
    };
    let Some(deleted) = build
        .steps
        .iter()
        .filter(|s| s.operation == delete)
        .filter(|s| crate::build_state::recovery_deleted(s).is_some_and(|(p, n, i)| p == *path && n == *node_id && i == *incarnation))
        .map(|s| s.attempt)
        .min()
    else {
        return (RecoveryProgress::Owed, None);
    };
    let Some(deleted) = completed_from(&delete, deleted) else { return (RecoveryProgress::Owed, None) };
    match completed_from(&format!("node.create:{path}"), deleted) {
        Some(created) => (RecoveryProgress::Recovered, Some(created)),
        None => (RecoveryProgress::Deleted, Some(deleted)),
    }
}

/// The attempt whose `node.create:<path>`, the last operation of the recovery of the exited birth
/// `(node_id, incarnation)` ([`recovery_progress`]), completed in `build`. `None` while no attempt
/// recovered that birth: a recovery that deleted the exited instance and has not created the new
/// node is not one.
pub fn recovered_at(build: &crate::build_state::BuildProjection, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Option<u32> {
    match recovery_progress(build, path, node_id, incarnation) {
        (RecoveryProgress::Recovered, at) => at,
        _ => None,
    }
}

/// [`plan`] for `build`'s current attempt, given what its receipts say already ran.
pub fn plan_for_build(build: &crate::build_state::BuildProjection, observed: &Topology) -> BuildPlan {
    plan_with(&build.topology, observed, build.action.as_ref(), ActionProgress::of(build), &open_mesh_deletes(build, observed), &build.steps, None)
}

/// [`plan_for_build`] for a roll's Build (proposal roll.md step 3), given what the node rows record of
/// each member's executable (`recorded`, [`recorded_executables`]): the topology's creates, then each
/// member still owed its replacement. A Build that is not a roll plans as [`plan_for_build`] does.
pub fn plan_for_roll(build: &crate::build_state::BuildProjection, observed: &Topology, recorded: &Recorded) -> BuildPlan {
    let roll = build.submitted_change.as_ref().is_some_and(TopologyChange::plans_replacements).then_some(recorded);
    // A replacement's progress is read from the receipts of the Build's own topology attempts: a create
    // an attempt carrying an action made (a recovery's, on the row's executable) replaced nothing, and
    // the path's replacement follows it as the next attempt's (proposal executable-bindings-at-create.md
    // rule 5: "never in the same attempt").
    let topology_steps: Vec<crate::build_state::BuildStepReceipt> = build.steps.iter().filter(|s| !build.attempt_actions.contains_key(&s.attempt)).cloned().collect();
    plan_with(&build.topology, observed, build.action.as_ref(), ActionProgress::of(build), &open_mesh_deletes(build, observed), &build.steps, roll.map(|r| (r, topology_steps.as_slice())))
}

/// The replacements `topology` owes against `observed` and `recorded` before any attempt ran: what a
/// Build that plans replacements ([`TopologyChange::plans_replacements`]) would replace now, in its
/// order. The door reads it to admit the Build (one attempt in flight per mesh, the initial fabric
/// first ready).
pub(crate) fn replacement_operations(topology: &FabricTopology, observed: &Topology, recorded: &Recorded) -> Vec<BuildOperation> {
    roll_operations(topology, observed, recorded, &[])
}

/// What the node rows record of one member, for a roll's plan: the node the row at its `path.name`
/// names and the sha256 of the executable it records (`NodeRow::executable`), none for a row written
/// before the field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRow {
    /// The node the row names.
    pub node_id: NodeId,
    /// The sha256 its executable binding records.
    pub sha256: Option<String>,
}

/// [`RecordedRow`] of each path of a topology that a live row is written at.
pub type Recorded = BTreeMap<PathName, RecordedRow>;

/// What `rows` record of every path of `topology` (proposal roll.md step 3: "the members whose row
/// records a sha256 other than the set's for their kind"): one point read per path, a `Deleted` row
/// and a path no row is written at left out. A read that fails is named.
pub async fn recorded_executables(rows: &dyn crate::storage::NodesStorage, topology: &FabricTopology) -> Result<Recorded, String> {
    let mut out = Recorded::new();
    for p in topology.meshes.values().flat_map(|m| m.nodes.iter()) {
        let row = crate::node_rows::record_at(rows, p).await.map_err(|e| format!("the row at {p} could not be read for its executable: {e}"))?;
        if let Some(row) = row.filter(|r| r.status != NodeStatus::Deleted) {
            out.insert(p.clone(), RecordedRow { node_id: row.node_id, sha256: row.executable.map(|b| b.sha256) });
        }
    }
    Ok(out)
}

/// The members of `topology` whose live row does not record the launch set's sha256 for its kind
/// (states.md "Resize and roll": "a member still on its old binary/identity does not satisfy its
/// planned replacement"), in path order. Empty when the topology carries no set.
pub fn off_the_set(topology: &FabricTopology, recorded: &Recorded) -> Vec<PathName> {
    topology
        .meshes
        .values()
        .flat_map(|m| m.nodes.iter())
        .filter(|p| target_executable(topology, p).is_some_and(|b| recorded.get(*p).and_then(|r| r.sha256.as_deref()) != Some(b.sha256.as_str())))
        .cloned()
        .collect()
}

/// How far a roll's replacement of the member at `path` got (proposal roll.md step 3: "Interruptions
/// resume from observed state"): read from the receipts, keyed by path.name and step so they name the
/// member, and from the row at the path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RollProgress {
    /// Nothing of this Build at the path: the member is replaced when its row is off the set.
    Owed,
    /// This Build's `node.delete` began on this birth (its `NodeDeleting`), wrote no `NodeDeleted`
    /// receipt, and the row at the path still names the birth: the delete is finished, then the create.
    Deleting(crate::build::ExitedBirth),
    /// The predecessor deleted (its `NodeDeleted` receipted, or begun and its row no longer live: the
    /// row at the path.name Deleted, the name free) and no create at the path completed since: the
    /// remaining work is the create over that birth.
    Deleted(crate::build::ExitedBirth),
    /// This Build created a node at the path that no later delete of it deleted (a replaced member's
    /// successor, or a path the roll added): never replaced again.
    Created,
}

/// [`RollProgress`] of the member at `path` in `steps`, the latest delete of the path this Build began
/// read against what `recorded` holds at the path.
fn roll_progress(steps: &[crate::build_state::BuildStepReceipt], path: &PathName, recorded: &Recorded) -> RollProgress {
    let delete = format!("node.delete:{path}");
    let create = format!("node.create:{path}");
    let created_from = |from: u32| steps.iter().any(|s| s.operation == create && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && s.attempt >= from);
    let latest = |step: &str| {
        steps
            .iter()
            .filter(|s| s.operation == delete)
            .filter_map(|s| crate::build_state::delete_step_birth(s, step).filter(|(p, ..)| p == path).map(|(_, node_id, incarnation)| (s.attempt, crate::build::ExitedBirth { node_id, incarnation })))
            .max_by_key(|(a, _)| *a)
    };
    let (at, birth, deleted) = match (latest("NodeDeleted"), latest("NodeDeleting")) {
        (Some((d, birth)), begun) if begun.as_ref().is_none_or(|(b, _)| *b <= d) => (d, birth, true),
        // Begun and no `NodeDeleted` receipted after it: its `NodeDeleted` step writes the row Deleted
        // before the receipt, so a row that no longer names the birth is the predecessor deleted.
        (_, Some((b, birth))) => {
            let row_live = recorded.get(path).is_some_and(|r| r.node_id == birth.node_id);
            (b, birth, !row_live)
        }
        _ => return if created_from(0) { RollProgress::Created } else { RollProgress::Owed },
    };
    match (created_from(at), deleted) {
        (true, _) => RollProgress::Created,
        (false, true) => RollProgress::Deleted(birth),
        (false, false) => RollProgress::Deleting(birth),
    }
}

/// The creates a roll owes at the paths whose predecessor its delete began, by path, with the birth
/// each is created over ([`roll_progress`]): the generic create never plans them.
fn roll_owed_creates(topology: &FabricTopology, steps: &[crate::build_state::BuildStepReceipt], recorded: &Recorded) -> BTreeMap<PathName, crate::build::ExitedBirth> {
    topology
        .meshes
        .values()
        .flat_map(|m| m.nodes.iter())
        .filter_map(|p| match roll_progress(steps, p, recorded) {
            RollProgress::Deleting(b) | RollProgress::Deleted(b) => Some((p.clone(), b)),
            RollProgress::Owed | RollProgress::Created => None,
        })
        .collect()
}

/// A roll's replacements still owed (proposal roll.md steps 3-5), in the order it replaces: mesh by
/// mesh in name order; in a mesh its non-primary node-admins, then its primary node-admin, then the
/// nodes of each other type, non-primary before primary, the primaries read from `observed` now. A
/// member is replaced by two operations keyed by its path.name: `node.delete` of the birth the view
/// holds there (the whole stop composed inside it; a primary with a successor moved first by
/// node.primary.update, as every delete of a primary is; R5: a type's only node moves none), then
/// `node.create` at the same path over that birth's kept data dir launching the set's binding of its
/// kind. A member whose live row records the set's sha256, and one this Build already replaced or
/// created, is left alone; a member deleted and not yet created is owed its create alone, and one whose
/// delete began and wrote no Deleted row is owed the rest of that delete, then the create.
fn roll_operations(topology: &FabricTopology, observed: &Topology, recorded: &Recorded, steps: &[crate::build_state::BuildStepReceipt]) -> Vec<BuildOperation> {
    let mut ops = Vec::new();
    for (name, m) in &topology.meshes {
        let primary = |kind: NodeKind| observed.node_type_primary(name, kind).map(|n| n.name.clone());
        let mut order: Vec<&PathName> = Vec::with_capacity(m.nodes.len());
        for kind in NodeKind::ALL {
            let at = primary(kind);
            order.extend(m.nodes_of_type(kind).filter(|p| Some(*p) != at.as_ref()));
            order.extend(m.nodes_of_type(kind).filter(|p| Some(*p) == at.as_ref()));
        }
        for p in order {
            let Some(binding) = target_executable(topology, p) else { continue };
            let held = observed.node(p).filter(|n| (n.status.is_live() || n.parked) && n.incarnation_id.is_some());
            let on_set = |n: &crate::model::Node| recorded.get(p).is_some_and(|r| r.node_id == n.node_id && r.sha256.as_deref() == Some(binding.sha256.as_str()));
            match (roll_progress(steps, p, recorded), held) {
                // The successor its create launched is live on the set's sha, its receipt never written:
                // it is the member, and nothing remains for it.
                (RollProgress::Deleted(birth), Some(n)) if n.node_id != birth.node_id && on_set(n) => {}
                // The predecessor deleted, the successor not yet created: the create alone is owed.
                (RollProgress::Deleted(birth), held) if held.is_none_or(|n| n.node_id == birth.node_id) => {
                    ops.push(BuildOperation::CreateNode { node: p.clone(), over: Some(birth), executable: Some(binding.clone()) });
                }
                // The predecessor's delete began and its row still names it: the delete of that exact
                // birth is finished, from the stop when the view holds it live or stopped, else targeted at
                // its incarnation with its runtime read from its row (as a resumed delete whose birth the
                // view no longer holds is), then the create over it.
                (RollProgress::Deleting(birth), held) if held.is_none_or(|n| n.node_id == birth.node_id) => {
                    ops.push(match held {
                        Some(_) => BuildOperation::RetireNode { node: p.clone(), exited: None },
                        None => BuildOperation::DeleteNode { node: p.clone(), from_incarnation: birth.incarnation.clone() },
                    });
                    ops.push(BuildOperation::CreateNode { node: p.clone(), over: Some(birth), executable: Some(binding.clone()) });
                }
                // Replaced or created by this Build: never replaced again. Every create of a topology
                // attempt launches the path's target (the step names it; the launching admin reads the
                // stored entry otherwise); a recovery's create is an action attempt's, never counted here.
                (RollProgress::Created, _) => {}
                (_, Some(n)) if !on_set(n) => {
                    let incarnation = n.incarnation_id.clone().expect("a held birth names its incarnation");
                    ops.push(BuildOperation::RetireNode { node: p.clone(), exited: None });
                    ops.push(BuildOperation::CreateNode { node: p.clone(), over: Some(crate::build::ExitedBirth { node_id: n.node_id.clone(), incarnation }), executable: Some(binding.clone()) });
                }
                _ => {}
            }
        }
    }
    ops
}

/// The meshes, by name, whose mesh.delete `build` admitted (its `AcceptedRoster` receipt) and has not
/// completed (no `MeshDeleted` or `MeshLeft` receipt): a deleted mesh among them is still the Build's
/// to resume from its receipts, a deleted mesh outside them is done.
fn open_mesh_deletes(build: &crate::build_state::BuildProjection, observed: &Topology) -> BTreeSet<String> {
    observed
        .meshes
        .iter()
        .filter_map(|m| {
            let op = crate::mesh_delete::operation(m.id.as_ref()?);
            let open = crate::mesh_delete::admitted_of(&build.steps, &op).is_some() && matches!(crate::mesh_delete::owner_resume(&build.steps, &op), Ok(crate::mesh_delete::OwnerResume::Lead));
            open.then(|| m.name.clone())
        })
        .collect()
}

fn plan_with(
    topology: &FabricTopology,
    observed: &Topology,
    action: Option<&AttemptAction>,
    progress: ActionProgress,
    open_deletes: &BTreeSet<String>,
    steps: &[crate::build_state::BuildStepReceipt],
    roll: Option<(&Recorded, &[crate::build_state::BuildStepReceipt])>,
) -> BuildPlan {
    let mut ops = Vec::new();
    // A termination of a deleted birth heard again is its one operation, the topology unchanged, until
    // its `Complete` is receipted for the birth (Q128).
    if let Some(AttemptAction::Terminate { path, node_id, from_incarnation }) = action {
        if !progress.finished {
            ops.push(BuildOperation::TerminateNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() });
        }
        return BuildPlan { operations: ops };
    }
    // A standalone drain or stop is its one operation and nothing else: the topology is unchanged,
    // and the path it acts on is neither created nor repaired by this attempt. It is planned while
    // the view holds the exact birth, or while its receipts say it began (a stop's view drops the
    // Leaving birth), and not once its `Complete` receipt names the birth.
    if let Some(
        AttemptAction::DeleteConnections { path, from_incarnation } | AttemptAction::Stop { path, from_incarnation } | AttemptAction::Start { path, from_incarnation } | AttemptAction::Delete { path, from_incarnation },
    ) = action
    {
        // A delete of a stopped `<path>.old` is admitted from the record the fabric-primary holds of
        // that exact birth, never from a view (only the renaming executor's view names the path):
        // it is planned until its `Complete` names the birth. A delete of the last admin of a deleted
        // mesh (mesh-delete.md step 7) is the ordinary node.delete of that exact birth, planned while
        // the view holds it.
        let parked_old = matches!(action, Some(AttemptAction::Delete { .. })) && path.old;
        let held = parked_old || observed.node(path).is_some_and(|n| n.incarnation_id.as_ref() == Some(from_incarnation));
        if !progress.finished && (held || progress.started) {
            let (node, from_incarnation) = (path.clone(), from_incarnation.clone());
            ops.push(match action {
                Some(AttemptAction::DeleteConnections { .. }) => BuildOperation::DeleteConnections { node, from_incarnation },
                Some(AttemptAction::Start { .. }) => BuildOperation::StartNode { node, from_incarnation },
                Some(AttemptAction::Delete { .. }) if parked_old => BuildOperation::DeleteNode { node, from_incarnation },
                Some(AttemptAction::Delete { .. }) => BuildOperation::RetireNode { node, exited: None },
                _ => BuildOperation::StopNode { node, from_incarnation },
            });
        }
        return BuildPlan { operations: ops };
    }
    // A recovery is its two operations of the exited birth, in order (node-recovery.md "Steps"):
    // node.delete of the exited instance, then node.create at the same path.name over its data dir,
    // whose claim seeds config and tags from the Deleted row (proposal node-properties-own-commands.md
    // §8); nothing is copied after it. The topology is unchanged, the path is never created by the
    // generic loop, and no other member is swept. Each is planned whatever the view holds at
    // the path (the exited birth unheard, nothing, or the new node mid-attempt) until the receipts say
    // it is done, so a later attempt runs only what is left and never deletes again.
    if let Some(AttemptAction::Recover { path, node_id, from_incarnation, .. }) = action {
        let exited = crate::build::ExitedBirth { node_id: node_id.clone(), incarnation: from_incarnation.clone() };
        let done = progress.recovery;
        if done < RecoveryProgress::Deleted {
            ops.push(BuildOperation::RetireNode { node: path.clone(), exited: Some(exited.clone()) });
        }
        if done < RecoveryProgress::Recovered {
            ops.push(BuildOperation::CreateNode { node: path.clone(), over: Some(exited), executable: None });
        }
        return BuildPlan { operations: ops };
    }
    // A recipe is its steps, in order, each its op's own operation, until the receipts say it is done.
    // The topology is unchanged, the generic loop plans nothing beside it, and no other member is
    // swept. A step on a node an earlier step creates carries no birth: its executor resolves the node
    // at its path when the step runs.
    if let Some(AttemptAction::Recipe { steps, pinned }) = action {
        match recipe_births(steps, pinned) {
            Ok(births) => {
                for (i, (step, birth)) in steps.iter().zip(births).enumerate() {
                    if !progress.recipe.get(i).copied().unwrap_or(false) {
                        ops.push(BuildOperation::RecipeStep { step: step.clone(), birth });
                    }
                }
            }
            Err(e) => tracing::info_span!("rdm.node_admin.build.reject.via-recipe-node-unknown", reason = %e).in_scope(|| tracing::info!("the recipe's steps name a node no step can act on: nothing is planned")),
        }
        return BuildPlan { operations: ops };
    }
    // fabric.delete (lifecycles/fabric-delete.md): run mesh.delete on each mesh leaf-inward (every
    // other mesh in name order first, the fabric-primary's own mesh last), run node.delete of each
    // other mesh's final primary after its mesh.deleted, then release the fabric name and report
    // fabric.deleted while the fabric-primary stays alive for its separate node.delete.
    if topology.meshes.is_empty() && action.is_none() {
        let fp_mesh = observed.fabric_primary().map(|n| n.mesh.as_str());
        // A deleted mesh the view no longer holds: its final primary's node.delete, once this Build began
        // it, is owed its receipt (fabric-delete.md "Failure": a runtime whose delete has no receipt is
        // never reported deleted).
        let mut begun_ops: std::collections::BTreeSet<&str> = steps.iter().filter(|r| r.operation.starts_with("mesh.delete:")).map(|r| r.operation.as_str()).collect();
        begun_ops.retain(|op| !observed.meshes.iter().any(|m| m.id.as_ref().is_some_and(|id| crate::mesh_delete::operation(id) == *op)));
        for op_key in begun_ops {
            if let Ok(crate::mesh_delete::OwnerResume::Deleted(manifest)) = crate::mesh_delete::owner_resume(steps, op_key) {
                if let Ok(node) = manifest.final_primary.node.parse::<PathName>() {
                    let retire_key = BuildOperation::RetireNode { node: node.clone(), exited: None }.key();
                    let completed = steps.iter().any(|r| r.operation == retire_key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete);
                    let begun = steps.iter().any(|r| r.operation == retire_key);
                    if !completed && begun && Some(node.mesh.as_str()) != fp_mesh {
                        ops.push(BuildOperation::RetireNode { node, exited: None });
                    }
                }
            }
        }
        let mut meshes: Vec<&crate::model::Mesh> = observed.meshes.iter().collect();
        meshes.sort_by(|a, b| (Some(a.name.as_str()) == fp_mesh).cmp(&(Some(b.name.as_str()) == fp_mesh)).then_with(|| a.name.cmp(&b.name)));
        for m in meshes {
            let is_fp_mesh = Some(m.name.as_str()) == fp_mesh;
            let Some(mesh_id) = m.id.clone() else {
                tracing::info_span!("rdm.node_admin.build.reject.via-mesh-id-unknown", mesh = %m.name).in_scope(|| tracing::info!("the mesh's shutdown is not planned"));
                continue;
            };
            let op_key = crate::mesh_delete::operation(&mesh_id);
            match crate::mesh_delete::owner_resume(steps, &op_key) {
                Ok(crate::mesh_delete::OwnerResume::Left) => {}
                Ok(crate::mesh_delete::OwnerResume::Deleted(manifest)) => {
                    if !is_fp_mesh {
                        if let Ok(node) = manifest.final_primary.node.parse::<PathName>() {
                            let retire_key = BuildOperation::RetireNode { node: node.clone(), exited: None }.key();
                            let completed = steps.iter().any(|r| r.operation == retire_key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete);
                            let held = observed.nodes.iter().any(|n| n.node_id.as_str() == manifest.final_primary.node_id);
                            // A delete this Build began is owed its receipt whether or not the view still
                            // holds the birth (fabric-delete.md "Failure").
                            let begun = steps.iter().any(|r| r.operation == retire_key);
                            if !completed && (held || begun) {
                                ops.push(BuildOperation::RetireNode { node, exited: None });
                            }
                        }
                    }
                }
                Ok(crate::mesh_delete::OwnerResume::Lead) | Err(_) => {
                    if m.status != crate::model::ScopeStatus::Dead || open_deletes.contains(&m.name) {
                        ops.push(BuildOperation::ShutdownMesh { mesh: m.name.clone(), mesh_id });
                    }
                }
            }
        }
        let del_op = BuildOperation::DeleteFabric { fabric_id: observed.fabric.id.clone() };
        let del_key = del_op.key();
        if !steps.iter().any(|r| r.operation == del_key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete) {
            ops.push(del_op);
        }
        return BuildPlan { operations: ops };
    }
    let mesh_exists = |m: &str| observed.meshes.iter().any(|x| x.name == m);
    // A restart's birth: the one the attempt is fenced to, when the view still holds exactly it and
    // it is not leaving to a departure (a leaving birth keeps the path to its departure). A parked
    // birth is the restart's own: only this restart's attempts carry a restart of a parked birth (the
    // door refuses a fresh one, `node-not-live`), and its run resumes at the start (R-PV1). The action
    // owns its path: the generic create below never plans it a second time.
    // A roll's attempt (no action) owes its replacements after the topology's creates; a path whose
    // predecessor a roll's delete removed, or began to and its row still names, is created by the
    // roll, over that predecessor's data dir, never by the generic create below. Every create of a
    // roll launches the set's binding.
    let roll = roll.filter(|_| action.is_none());
    let roll_owed = roll.map(|(recorded, steps)| roll_owed_creates(topology, steps, recorded)).unwrap_or_default();
    let restarted: Option<&PathName> = match action {
        Some(AttemptAction::Restart { path, from_incarnation }) => observed.node(path).filter(|n| n.incarnation_id.as_ref() == Some(from_incarnation) && (n.status != NodeStatus::Leaving || n.parked)).map(|_| path),
        _ => None,
    };
    for (name, m) in &topology.meshes {
        if !mesh_exists(name) {
            ops.push(BuildOperation::CreateMesh { mesh: name.clone(), mesh_id: m.mesh_id.clone() });
        }
        for p in &m.nodes {
            // A path whose birth the view holds live is satisfied; any other path is planned and the
            // create pipeline's fence decides against the world (a running runtime at the path is
            // held, never replaced: `AdminRunner::revise_predecessor`).
            if restarted == Some(p) {
                continue;
            }
            // An attempt that carries an action repairs exactly its birth. Another unheard birth is
            // that birth's own proven drift, repaired by its own attempt, never swept up here.
            if let Some(AttemptAction::Restart { path, .. }) = action {
                if path != p || observed.node(p).is_some_and(|n| matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead)) {
                    continue;
                }
            }
            if roll_owed.contains_key(p) {
                continue;
            }
            if !observed.node(p).is_some_and(|n| n.status.is_live()) {
                // The executable the path's entry names (a request named it); a Build that plans
                // replacements launches its target, the launch set's for a path whose entry names none.
                let executable = match roll {
                    Some(_) => target_executable(topology, p),
                    None => m.path_config(p).and_then(|c| c.executable.as_ref()),
                }
                .cloned();
                ops.push(BuildOperation::CreateNode { node: p.clone(), over: None, executable });
            }
        }
    }
    if let Some(path) = restarted {
        ops.push(BuildOperation::RestartNode { node: path.clone() });
    }
    if let Some((recorded, steps)) = roll {
        ops.extend(roll_operations(topology, observed, recorded, steps));
    }
    // A live or stopped (parked) node of a kept mesh that the topology no longer names is retired
    // (a node.delete of a stopped node retires it: lifecycles/node-delete.md), and so is one its
    // mesh-primary recorded os-stopped, silent as it is (node-delete.md "OS-suspended target": its
    // delete skips the stop by name); a birth that is only silent is never retired for its silence. A
    // node leaving under another operation is that operation's. A mesh the topology no longer names is retired whole
    // (every member, dead or live, by the retire pipeline), never member by member.
    // A surplus birth's retirement is its own attempt (no action): an attempt that carries an action
    // repairs exactly its birth.
    // A recipe that failed stops at its step, and resuming it or running its compensation steps is the
    // caller's (lifecycles/node-replace.md "Rollback").
    let mut extra: Vec<&PathName> = observed
        .members()
        .filter(|n| action.is_none() && (n.status.is_live() || n.parked) && topology.meshes.contains_key(&n.mesh) && !topology.contains(&n.name))
        .map(|n| &n.name)
        .collect();
    extra.sort();
    for p in extra {
        ops.push(BuildOperation::RetireNode { node: p.clone(), exited: None });
    }
    // A deleted mesh (its row Dead at mesh.deleted) is done: its mesh.delete ended, and its last
    // admin's end is a separate node.delete (mesh-delete.md steps 5-7), never planned from here. Only
    // the Build that admitted its mesh.delete and holds no completion of it plans it again, to resume.
    let mut gone: Vec<&String> = observed.meshes.iter().filter(|m| m.status != crate::model::ScopeStatus::Dead || open_deletes.contains(&m.name)).map(|m| &m.name).filter(|m| !topology.meshes.contains_key(*m)).collect();
    gone.sort();
    for m in gone {
        // A mesh whose id no member has stated has no operation key: it is named, and the Build
        // stays open until a member states the id (the next attempt re-plans).
        match crate::build::shutdown_mesh(observed, m) {
            Ok(op) => ops.push(op),
            Err(e) => tracing::info_span!("rdm.node_admin.build.reject.via-mesh-id-unknown", mesh = %m).in_scope(|| tracing::info!(error = %e, "the mesh's shutdown is not planned")),
        }
    }
    BuildPlan { operations: ops }
}

/// The Fabric identity as this admin holds it: the identity row in `fabric.storage`, or, while the
/// store holds none, the identity it learned (its entry answer, the Build topic) as its own runtime
/// state, never as a row. `fabric/` has one writer, the fabric-primary (R14 item 2). The Fabric row
/// carries no pointer to the newest Build (R-BI3): which Build is current is read from the Build
/// entities alone ([`BuildStateAdapter::current_build`]).
pub struct AcceptedStore {
    storage: Arc<dyn FabricStorage>,
    /// The Fabric identity this admin learned: runtime state, read while the store holds none.
    learned: Mutex<Option<FabricIdentity>>,
    node: String,
}

impl AcceptedStore {
    /// A store over `storage`, held by the admin named `node`.
    pub fn new(storage: Arc<dyn FabricStorage>, node: impl Into<String>) -> Self {
        Self { storage, learned: Mutex::new(None), node: node.into() }
    }

    /// The Fabric identity held, when one is: the identity row, or the learned identity while the
    /// store holds none.
    pub async fn record(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        let held = self.storage.fabric().await?.map(|r| r.identity());
        Ok(held.or_else(|| self.learned.lock().unwrap().clone()))
    }

    /// A Fabric identity heard in an entry answer or on the Build topic (`from`), taken as this
    /// admin's runtime state (nothing is written: the fabric-primary writes `fabric/`). One naming
    /// another Fabric than the one held is not taken.
    pub async fn learn(&self, identity: FabricIdentity, from: &str) {
        // An identity already held (on the store root, or learned) stands: the same Fabric's is
        // nothing new, and another Fabric's is not this admin's.
        if !matches!(self.record().await, Ok(None)) {
            return;
        }
        let mut learned = self.learned.lock().unwrap();
        if learned.is_some() {
            return;
        }
        tracing::info_span!("rdm.node_admin.fabric.update.via-identity-learned", node = %self.node, fabric_id = %identity.fabric_id, from = %from)
            .in_scope(|| tracing::info!("this admin holds the Fabric identity it heard as its own runtime state; the fabric-primary writes fabric/"));
        *learned = Some(identity);
    }
}

impl AcceptedStore {
    /// A store over memory `fabric.storage` holding the Fabric's identity, and a Build of `topology`
    /// that `builds` holds settled (accepted, attempt 1 claimed by `me`, converged): the current
    /// Build. Fixtures: what a Day 0 plus one converged Build leaves behind.
    pub async fn seeded(builds: &dyn BuildStateAdapter, fabric_id: crate::model::FabricId, topology: FabricTopology, me: &str) -> Result<Arc<Self>, BuildStateError> {
        let storage = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        crate::fabric_storage::claim_identity(&*storage, &FabricIdentity { fabric_id, name: topology.fabric.clone() })
            .await.map_err(|e| BuildStateError::Io(e.to_string()))?
            .map_err(|held| BuildStateError::Io(held.to_string()))?;
        let store = Arc::new(Self::new(storage, me));
        let build_id = BuildId::mint();
        crate::build_state::accept(builds, &crate::build_state::BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, submitted_at_ms: 0 }, None, me, "seeded").await?;
        builds.claim_attempt(&crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: me.into() }).await?;
        builds
            .append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { mesh: None, build_id: build_id.clone(), attempt: 1, outcome: crate::build_state::AttemptOutcome::Converged })
            .await?;
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn n(name: &str, status: NodeStatus, primary: bool) -> Node {
        let mut node = Node::allocated(name.parse().unwrap());
        node.status = status;
        node.is_primary = primary;
        node.incarnation_id = Some(IncarnationId::mint());
        node
    }

    fn observed(nodes: Vec<Node>, meshes: &[&str]) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: meshes.iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
            nodes,
        }
    }

    fn mn() -> Topology {
        use NodeStatus::ReadyForTraffic as R;
        observed(vec![n("mesh1.admin.1", R, true), n("mesh1.admin.2", R, false), n("mesh1.rpc.1", R, true), n("mesh1.rpc.2", R, false), n("mesh1.rpc.3", R, false)], &["mesh1"])
    }

    fn t(meshes: &[(&str, u32, u32)]) -> FabricTopology {
        FabricTopology { fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&MeshDesired::of(*m, [(rdm_core::NodeKind::NodeAdmin, *a), (rdm_core::NodeKind::RpcNode, *r)])))).collect(), bindings: None }
    }

    fn paths(m: &MeshTopology) -> Vec<String> {
        m.nodes.iter().map(|p| p.to_string()).collect()
    }

    fn counts(name: &str, a: u32, r: u32) -> MeshDesired {
        MeshDesired::of(name.to_string(), [(rdm_core::NodeKind::NodeAdmin, a), (rdm_core::NodeKind::RpcNode, r)])
    }

    #[test]
    fn a_submitted_change_compiles_to_exact_paths_and_a_shrink_keeps_the_primary() {
        let cur = t(&[("mesh1", 2, 3)]);
        // Grow: the lowest free ordinals.
        let grown = compile(&cur, &TopologyChange::ReconcileMesh { desired: counts("mesh1", 3, 5) }, &mn()).unwrap();
        assert_eq!(paths(grown.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.admin.3", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3", "mesh1.rpc.4", "mesh1.rpc.5"]);
        // Shrink: the highest ordinals go; the observed primary (rpc.1 here) never does.
        let mut o = mn();
        o.nodes.iter_mut().for_each(|x| x.is_primary = x.name.to_string() == "mesh1.rpc.3" || x.name.to_string() == "mesh1.admin.1");
        let shrunk = compile(&cur, &TopologyChange::ReconcileMesh { desired: counts("mesh1", 1, 1) }, &o).unwrap();
        assert_eq!(paths(shrunk.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.rpc.3"], "the primary rpc.3 is kept over lower ordinals");
        // Exact removal: exactly that path, nothing the planner chooses.
        let removed = compile(&cur, &TopologyChange::RemoveNode { node: "mesh1.rpc.2".parse().unwrap() }, &mn()).unwrap();
        assert_eq!(paths(removed.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.3"]);
        // Add fills the gap the removal left.
        let added = compile(&removed, &TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, &mn()).unwrap();
        assert_eq!(paths(added.mesh("mesh1").unwrap()), paths(cur.mesh("mesh1").unwrap()));
        assert_eq!(removed.desired().meshes[0], counts("mesh1", 2, 2), "counts are a view of the paths");
    }

    #[test]
    fn mesh_changes_compile_and_the_refusals_are_named() {
        let cur = t(&[("mesh1", 2, 3)]);
        let two = compile(&cur, &TopologyChange::CreateMesh { desired: counts("mesh2", 1, 1) }, &mn()).unwrap();
        assert_eq!(two.meshes.keys().collect::<Vec<_>>(), ["mesh1", "mesh2"]);
        assert_eq!(paths(two.mesh("mesh2").unwrap()), ["mesh2.admin.1", "mesh2.rpc.1"]);
        let whole = compile(&two, &TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh3", 1, 0), counts("mesh2", 1, 2)] } }, &mn()).unwrap();
        assert_eq!(whole.meshes.keys().collect::<Vec<_>>(), ["mesh2", "mesh3"], "a mesh the request leaves out is removed");
        assert_eq!(paths(whole.mesh("mesh2").unwrap()), ["mesh2.admin.1", "mesh2.rpc.1", "mesh2.rpc.2"]);
        let one = compile(&two, &TopologyChange::RemoveMesh { mesh: "mesh2".into() }, &mn()).unwrap();
        assert_eq!(one, cur);
        for (change, want) in [
            (TopologyChange::RemoveMesh { mesh: "mesh1".into() }, BuildReject::EmptyFabric),
            (TopologyChange::RemoveMesh { mesh: "mesh9".into() }, BuildReject::UnknownMesh { mesh: "mesh9".into() }),
            (TopologyChange::CreateMesh { desired: counts("mesh1", 1, 0) }, BuildReject::MeshAlreadyExists { mesh: "mesh1".into() }),
            (TopologyChange::CreateMesh { desired: counts("Mesh2", 1, 0) }, BuildReject::InvalidMeshName { mesh: "Mesh2".into() }),
            (TopologyChange::CreateMesh { desired: counts("mesh2", 0, 1) }, BuildReject::MeshWithoutAdmin { mesh: "mesh2".into() }),
            (TopologyChange::RemoveNode { node: "mesh1.rpc.9".parse().unwrap() }, BuildReject::UnknownNode { node: "mesh1.rpc.9".into() }),
            (TopologyChange::ReconcileMesh { desired: counts("mesh7", 1, 1) }, BuildReject::UnknownMesh { mesh: "mesh7".into() }),
            (
                TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "other".into(), meshes: vec![counts("mesh1", 1, 1)] } },
                BuildReject::FabricMismatch { requested: "other".into(), fabric: "fabric1".into() },
            ),
            (TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![] } }, BuildReject::EmptyFabric),
            (
                TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 1, 1), counts("mesh1", 1, 1)] } },
                BuildReject::DuplicateMesh { mesh: "mesh1".into() },
            ),
        ] {
            assert_eq!(compile(&cur, &change, &mn()), Err(want), "{change:?}");
        }
        let lone = t(&[("mesh1", 1, 1)]);
        assert_eq!(
            compile(&lone, &TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, &mn()),
            Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh: "mesh1".into() })
        );
    }

    /// CONTRACT (fabric.delete is its own Build compiled from the current topology plus its change;
    /// lifecycles/fabric-delete.md, "Numbered end-to-end sequence"): a `DeleteFabric` change compiles
    /// the current topology to the Fabric with no mesh, and that Build's plan is the page's sequence:
    /// step 1, `mesh.delete` (`ShutdownMesh`) of each mesh, the fabric-primary's own mesh last (here
    /// mesh2, so the order is not the names' order); step 2, once a mesh's `mesh.deleted` stands (its
    /// `MeshDeleted` receipt), `node.delete` (`RetireNode`) of that mesh's mesh-primary in its place;
    /// step 3, `fabric.deleted` (`DeleteFabric`) last. The fabric-primary's own `node.delete` (step 4)
    /// is a separate call from outside the fabric, never this Build's. Must NOT happen: the
    /// fabric-primary's own mesh deleted before another, a deleted mesh's mesh.delete planned again,
    /// or fabric.deleted planned before the meshes.
    // @feature: mesh-lifecycle
    #[test]
    fn a_fabric_delete_compiles_to_the_build_its_page_names() {
        use NodeStatus::ReadyForTraffic as R;
        let cur = t(&[("mesh1", 1, 1), ("mesh2", 1, 1), ("mesh3", 1, 1)]);
        let compiled = compile(&cur, &TopologyChange::DeleteFabric, &mn()).unwrap();
        assert_eq!((compiled.fabric.as_str(), compiled.meshes.len()), ("fabric1", 0), "the Fabric with no mesh");
        let mut fabric_primary = n("mesh2.admin.1", R, true);
        fabric_primary.is_fabric_primary = true;
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.rpc.1", R, false), fabric_primary, n("mesh2.rpc.1", R, false), n("mesh3.admin.1", R, true), n("mesh3.rpc.1", R, false)], &["mesh1", "mesh2", "mesh3"]);
        let id = |m: &str| o.meshes.iter().find(|x| x.name == m).and_then(|x| x.id.clone()).unwrap();
        let shutdown = |m: &str| BuildOperation::ShutdownMesh { mesh: m.into(), mesh_id: id(m) };
        let deleted = BuildOperation::DeleteFabric { fabric_id: o.fabric.id.clone() };
        assert_eq!(plan(&compiled, &o, None).operations, vec![shutdown("mesh1"), shutdown("mesh3"), shutdown("mesh2"), deleted.clone()], "step 1, the fabric-primary's own mesh last, then step 3");

        let primary = o.node(&"mesh1.admin.1".parse().unwrap()).unwrap();
        let manifest = crate::mesh_delete::ExitManifest { mesh_id: id("mesh1").to_string(), roster: vec![], final_primary: crate::mesh_delete::RosterBirth::of(primary).unwrap(), final_runtime: None, receipts: vec![] };
        let mesh_deleted = crate::build_state::BuildStepReceipt {
            mesh: None,
            build_id: BuildId::mint(),
            attempt: 1,
            operation: crate::mesh_delete::operation(&id("mesh1")),
            step: crate::mesh_delete::STEP_MESH_DELETED.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: serde_json::to_value(&manifest).ok(),
            executor: Some("mesh2.admin.1".into()),
        };
        assert_eq!(
            plan_with(&compiled, &o, None, ActionProgress::default(), &BTreeSet::new(), &[mesh_deleted.clone()], None).operations,
            vec![BuildOperation::RetireNode { node: "mesh1.admin.1".parse().unwrap(), exited: None }, shutdown("mesh3"), shutdown("mesh2"), deleted],
            "step 2: mesh1's mesh.deleted stands, so its mesh-primary's node.delete takes its place"
        );

        // The view no longer holds mesh1 or its final primary: a node.delete of it this Build began is
        // owed its receipt, planned first (fabric-delete.md "Failure").
        let mut fp = n("mesh2.admin.1", R, true);
        fp.is_fabric_primary = true;
        let gone = observed(vec![fp, n("mesh2.rpc.1", R, false), n("mesh3.admin.1", R, true), n("mesh3.rpc.1", R, false)], &["mesh2", "mesh3"]);
        let gid = |m: &str| gone.meshes.iter().find(|x| x.name == m).and_then(|x| x.id.clone()).unwrap();
        let gshutdown = |m: &str| BuildOperation::ShutdownMesh { mesh: m.into(), mesh_id: gid(m) };
        let gdeleted = BuildOperation::DeleteFabric { fabric_id: gone.fabric.id.clone() };
        let begun = crate::build_state::BuildStepReceipt { attempt: 2, operation: "node.delete:mesh1.admin.1".into(), step: "TerminateRuntime".into(), output: None, ..mesh_deleted.clone() };
        assert_eq!(
            plan_with(&compiled, &gone, None, ActionProgress::default(), &BTreeSet::new(), &[mesh_deleted, begun], None).operations,
            vec![BuildOperation::RetireNode { node: "mesh1.admin.1".parse().unwrap(), exited: None }, gshutdown("mesh3"), gshutdown("mesh2"), gdeleted],
            "the begun node.delete of a deleted mesh's final primary the view no longer holds is still owed"
        );
    }

    /// CONTRACT (node-identity-and-state.md §2.7, "Who mints it": "The Build that creates a mesh's first
    /// node-admin mints the mesh id, never reissued"; R-MN1: "A mesh created again under the same name is
    /// a new mesh with a new id"): a mesh's id is minted once, when the mesh enters the accepted topology
    /// (`CreateMesh`, or a `ReconcileFabric` that names a mesh the topology does not hold), and every later
    /// Build carries it unchanged through every other change. Every plan of a Build that holds the mesh
    /// unrealized carries that id in its `create-mesh` operation. A mesh removed and created again enters
    /// anew with a new id. Must NOT happen: a plan minting an id of its own, a kept mesh's id moving, or a
    /// recreated mesh reusing the removed one's.
    // @feature: mesh-lifecycle
    #[test]
    fn a_meshs_id_is_minted_once_when_it_is_accepted_and_every_plan_carries_it() {
        let cur = t(&[("mesh1", 2, 3)]);
        let m1 = cur.mesh("mesh1").unwrap().mesh_id.clone();
        let two = compile(&cur, &TopologyChange::CreateMesh { desired: counts("mesh2", 1, 1) }, &mn()).unwrap();
        let m2 = two.mesh("mesh2").unwrap().mesh_id.clone();
        assert_ne!(m2, m1, "mesh2 enters the topology under an id of its own");
        assert_eq!(two.mesh("mesh1").unwrap().mesh_id, m1, "a kept mesh keeps its id");
        let create = |t: &FabricTopology| plan(t, &mn(), None).operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. }));
        let want = Some(BuildOperation::CreateMesh { mesh: "mesh2".into(), mesh_id: m2.clone() });
        assert_eq!((create(&two), create(&two)), (want.clone(), want.clone()), "every plan of the Build carries the id minted at acceptance");
        let grown = compile(&two, &TopologyChange::ReconcileMesh { desired: counts("mesh2", 1, 3) }, &mn()).unwrap();
        let whole = compile(&grown, &TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 3), counts("mesh2", 1, 3), counts("mesh3", 1, 0)] } }, &mn()).unwrap();
        assert_eq!((whole.mesh("mesh1").unwrap().mesh_id.clone(), whole.mesh("mesh2").unwrap().mesh_id.clone()), (m1.clone(), m2.clone()), "a later Build carries every kept mesh's id unchanged");
        let m3 = whole.mesh("mesh3").unwrap().mesh_id.clone();
        assert!(m3 != m1 && m3 != m2, "a mesh a ReconcileFabric adds enters under an id of its own");
        let removed = compile(&whole, &TopologyChange::RemoveMesh { mesh: "mesh2".into() }, &mn()).unwrap();
        let again = compile(&removed, &TopologyChange::CreateMesh { desired: counts("mesh2", 1, 1) }, &mn()).unwrap();
        assert_ne!(again.mesh("mesh2").unwrap().mesh_id, m2, "a mesh removed and created again is a new mesh with a new id");
    }

    #[test]
    fn the_plan_is_topology_minus_observed_and_a_satisfied_topology_plans_nothing() {
        let cur = t(&[("mesh1", 2, 3)]);
        assert_eq!(plan(&cur, &mn(), None).operations, vec![]);
        let fresh = plan(&cur, &observed(vec![], &[]), None);
        assert_eq!(fresh.operations[0], BuildOperation::CreateMesh { mesh: "mesh1".into(), mesh_id: cur.mesh("mesh1").unwrap().mesh_id.clone() });
        assert_eq!(fresh.operations.len(), 6);
        // A dead (unheard) birth's path is planned again — the create pipeline's fence then holds a
        // runtime that still runs; a live node outside the topology is retired.
        let mut o = mn();
        o.nodes[3].status = NodeStatus::PendingReconnect;
        o.nodes.push(n("mesh1.rpc.7", NodeStatus::ReadyForTraffic, false));
        assert_eq!(
            plan(&cur, &o, None).operations,
            vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None, executable: None }, BuildOperation::RetireNode { node: "mesh1.rpc.7".parse().unwrap(), exited: None }]
        );
        // A mesh outside the topology is retired after everything else.
        let mut o2 = mn();
        let id2 = MeshId::mint();
        o2.meshes.push(Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
        assert_eq!(plan(&cur, &o2, None).operations, vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2 }]);
    }

    /// CONTRACT (mesh-delete.md steps 5-7): a deleted mesh (its row Dead at mesh.deleted) whose last
    /// admin lives on is done for every Build: the plan names nothing of it, so no later Build runs
    /// its mesh.delete again or deletes its last admin unasked. Only the Build that admitted its
    /// mesh.delete and holds no completion of it plans it again, to resume from the receipts; a
    /// deleting mesh (not yet Dead) is planned as before. Must NOT happen: a later Build re-running
    /// mesh.delete on a deleted mesh, or retiring its last admin, whose end is a separate node.delete.
    #[test]
    fn a_deleted_mesh_is_planned_only_by_the_build_that_admitted_its_mesh_delete_and_holds_no_completion() {
        let cur = t(&[("mesh1", 2, 3)]);
        let id2 = MeshId::mint();
        let mut o = mn();
        o.nodes.push(n("mesh2.admin.1", NodeStatus::ReadyForTraffic, true));
        o.meshes.push(Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Dead });
        assert_eq!(plan(&cur, &o, None).operations, vec![], "a deleted mesh with its last admin alive is planned by no Build");
        let open: BTreeSet<String> = ["mesh2".to_string()].into();
        assert_eq!(
            plan_with(&cur, &o, None, ActionProgress::default(), &open, &[], None).operations,
            vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2.clone() }],
            "the Build that admitted its mesh.delete and holds no completion resumes it"
        );
        o.meshes.iter_mut().filter(|m| m.name == "mesh2").for_each(|m| m.status = ScopeStatus::Leaving);
        assert_eq!(plan(&cur, &o, None).operations, vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2 }], "a deleting mesh is planned as before");
    }

    /// CONTRACT (mesh-delete.md step 7: "A separate node.delete deletes the mesh-primary"): the
    /// fenced `Delete` action on a birth of a deleted mesh (no `.old` path: the topology names no
    /// such mesh) is the ordinary node.delete of that exact birth, `RetireNode`, planned while the
    /// view holds it, and nothing once it left or its `Complete` is receipted; a `Delete` of a parked
    /// `.old` stays `DeleteNode`. Must NOT happen: the last admin of a deleted mesh deleted through a
    /// copy that skips its stop (`DeleteNode` sends no command to a live process).
    #[test]
    fn a_delete_of_a_deleted_meshs_last_admin_is_the_ordinary_node_delete() {
        let cur = t(&[("mesh1", 2, 3)]);
        let mut o = mn();
        let last = n("mesh2.admin.1", NodeStatus::ReadyForTraffic, true);
        let birth = last.incarnation_id.clone().unwrap();
        o.nodes.push(last);
        o.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Dead });
        let path: PathName = "mesh2.admin.1".parse().unwrap();
        let delete = AttemptAction::Delete { path: path.clone(), from_incarnation: birth.clone() };
        assert_eq!(plan(&cur, &o, Some(&delete)).operations, vec![BuildOperation::RetireNode { node: path.clone(), exited: None }]);
        let finished = ActionProgress { started: true, finished: true, ..ActionProgress::default() };
        assert_eq!(plan_with(&cur, &o, Some(&delete), finished, &BTreeSet::new(), &[], None).operations, vec![], "nothing once its Complete names the birth");
        o.nodes.retain(|n| n.name != path);
        assert_eq!(plan(&cur, &o, Some(&delete)).operations, vec![], "nothing once the view no longer holds it");
        let old: PathName = "mesh1.rpc.3.old".parse().unwrap();
        let parked = AttemptAction::Delete { path: old.clone(), from_incarnation: birth.clone() };
        assert_eq!(plan(&cur, &o, Some(&parked)).operations, vec![BuildOperation::DeleteNode { node: old, from_incarnation: birth }]);
    }

    /// CONTRACT (lifecycles/node-delete.md, node-identity-and-state.md 6.9 Effect): a stopped node (Leaving,
    /// parked, its process alive) that the accepted topology no longer names, the topology a
    /// node.delete of it accepts, is retired: the plan is its one `RetireNode`. A node leaving under
    /// another operation (Leaving, not parked) is that operation's, never retired beside it. Must NOT
    /// happen: a node.delete of a stopped node that converges with nothing planned while its process
    /// lives on.
    #[test]
    fn a_stopped_node_the_topology_no_longer_names_is_deleted() {
        let cur = t(&[("mesh1", 2, 3)]);
        let mut o = mn();
        let mut stopped = n("mesh1.rpc.7", NodeStatus::Leaving, false);
        stopped.parked = true;
        o.nodes.push(stopped);
        assert_eq!(plan(&cur, &o, None).operations, vec![BuildOperation::RetireNode { node: "mesh1.rpc.7".parse().unwrap(), exited: None }]);
        let mut leaving = mn();
        leaving.nodes.push(n("mesh1.rpc.7", NodeStatus::Leaving, false));
        assert_eq!(plan(&cur, &leaving, None).operations, vec![], "a departure already under way is not retired again");
    }

    /// CONTRACT: a replaced predecessor (`<path>.old`) is neither the live birth at `<path>` nor a missing
    /// node nor a surplus one: the plan of the settled topology is empty with it in the view, and a
    /// successor at the path is the one birth the plan counts.
    #[test]
    fn a_renamed_predecessor_changes_no_plan() {
        let cur = t(&[("mesh1", 2, 3)]);
        for status in [NodeStatus::ConnectionsDeleting, NodeStatus::Leaving, NodeStatus::Dead, NodeStatus::ReadyForTraffic] {
            let mut o = mn();
            let mut old = n("mesh1.rpc.2", status, false);
            old.name = old.name.renamed();
            old.parked = status == NodeStatus::Leaving;
            o.nodes.push(old);
            assert_eq!(plan(&cur, &o, None).operations, vec![], "{status:?}");
            let mut missing = mn();
            let mut old = n("mesh1.rpc.2", status, false);
            old.name = old.name.renamed();
            missing.nodes.retain(|x| x.name.to_string() != "mesh1.rpc.2");
            missing.nodes.push(old);
            assert_eq!(plan(&cur, &missing, None).operations, vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None, executable: None }], "the path itself is missing: {status:?}");
        }
    }

    #[test]
    fn an_attempt_action_targets_its_exact_birth_and_never_changes_the_topology() {
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &o, Some(&restart)).operations, vec![BuildOperation::RestartNode { node: path.clone() }]);
        // Once the path runs another birth, the action is satisfied: a re-plan does nothing.
        let mut later = mn();
        later.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&cur, &later, Some(&restart)).operations, vec![]);
        // Day 0 roots one admin; a topology without an admin is refused.
        assert_eq!(paths(FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()).mesh("mesh1").unwrap()), ["mesh1.admin.1"]);
        let mut bad = cur.clone();
        bad.meshes.get_mut("mesh1").unwrap().nodes.retain(|p| p.kind == NodeKind::RpcNode);
        assert_eq!(bad.validate(), Err(BuildReject::MeshWithoutAdmin { mesh: "mesh1".into() }));
    }

    /// CONTRACT: a restart fenced to a birth the view holds exited plans that path exactly once: it
    /// re-creates the SAME identity (no fresh create beside it). A restart of another birth at the path
    /// is satisfied, and the unheard birth is not swept up by it (its own proven drift opens its own
    /// attempt). A birth already leaving keeps its path to its departure: a restart plans the create.
    #[test]
    fn an_action_on_an_exited_birth_plans_its_path_once() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        for dead in [NodeStatus::PendingReconnect, NodeStatus::Dead] {
            let mut o = mn();
            o.nodes.iter_mut().find(|x| x.name == path).unwrap().status = dead;
            let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
            let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from.clone() };
            assert_eq!(plan(&cur, &o, Some(&restart)).operations, vec![BuildOperation::RestartNode { node: path.clone() }], "{dead:?}");
            let other = AttemptAction::Restart { path: path.clone(), from_incarnation: IncarnationId::mint() };
            assert_eq!(plan(&cur, &o, Some(&other)).operations, vec![], "{dead:?}");
            assert_eq!(plan(&cur, &o, None).operations, vec![BuildOperation::CreateNode { node: path.clone(), over: None, executable: None }], "no action: the generic create decides");
        }
        // A surplus live birth is retired by an attempt of its own, never beside an action.
        let mut surplus = mn();
        surplus.nodes.push(n("mesh1.rpc.7", NodeStatus::ReadyForTraffic, false));
        let from = surplus.node(&path).unwrap().incarnation_id.clone().unwrap();
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &surplus, None).operations, vec![BuildOperation::RetireNode { node: "mesh1.rpc.7".parse().unwrap(), exited: None }]);
        assert_eq!(plan(&cur, &surplus, Some(&restart)).operations, vec![BuildOperation::RestartNode { node: path.clone() }]);
        let mut leaving = mn();
        leaving.nodes.iter_mut().find(|x| x.name == path).unwrap().status = NodeStatus::Leaving;
        let from = leaving.node(&path).unwrap().incarnation_id.clone().unwrap();
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &leaving, Some(&restart)).operations, vec![BuildOperation::CreateNode { node: path.clone(), over: None, executable: None }]);
    }

    /// CONTRACT (R-PV1: "The pending lifecycle action is resumed per its receipts: `node.start` when
    /// start is the pending step (the node did stop)"; node-restart.md: a restart runs on the stopped
    /// process, which never exits): an attempt carrying the restart of the exact birth its own earlier
    /// attempt parked (the view holds it `Leaving` and parked, the same incarnation) plans that restart
    /// once, whose run resumes at its start. Must NOT happen: a create planned at the path of a parked
    /// birth that is still alive and still the restart's.
    #[test]
    fn a_restart_of_a_birth_its_own_restart_stopped_plans_the_restart_never_a_create() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let mut parked = mn();
        let birth = parked.nodes.iter_mut().find(|x| x.name == path).unwrap();
        birth.status = NodeStatus::Leaving;
        birth.parked = true;
        let from = parked.node(&path).unwrap().incarnation_id.clone().unwrap();
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from };
        assert_eq!(plan(&cur, &parked, Some(&restart)).operations, vec![BuildOperation::RestartNode { node: path.clone() }]);
    }

    /// CONTRACT: an attempt that carries an action repairs exactly its birth, from whichever admin's
    /// view plans it. Another member the view does not hold (silent longer than this view keeps it)
    /// is that member's own attempt, never swept into this plan: two admins whose views keep a
    /// different set of silent members would otherwise plan different first operations, and each
    /// would believe the other executes it.
    #[test]
    fn an_action_plans_only_its_birth_whatever_other_members_the_view_lacks() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.3".parse().unwrap();
        let exited = crate::build::ExitedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let recover = AttemptAction::Recover { path: path.clone(), node_id: exited.node_id.clone(), from_incarnation: exited.incarnation.clone(), backbone: false };
        let mut o = mn();
        o.nodes.retain(|x| x.name != path && x.name.to_string() != "mesh1.rpc.2" && x.name.to_string() != "mesh1.admin.2");
        let want = vec![
            BuildOperation::RetireNode { node: path.clone(), exited: Some(exited.clone()) },
            BuildOperation::CreateNode { node: path.clone(), over: Some(exited), executable: None },
        ];
        assert_eq!(plan(&cur, &o, Some(&recover)).operations, want);
        // With no action the generic create still plans every missing member.
        assert!(plan(&cur, &o, None).operations.len() >= 3);
    }

    /// CONTRACT (fabric-recovery.md step 3, Q142, Q148: "its node.delete of the exact lost birth writes
    /// that birth's row deleted with no `TerminateRuntime` and releases the name, then node.create at the
    /// path.name"): fabric recovery's backbone attempt plans the same two operations as every recovery,
    /// the delete keyed on the lost birth, then the create; what differs is its delete's runtime step,
    /// which the action carries, and the action crosses the Build topic as itself. Must NOT happen: a
    /// backbone attempt planned without its delete, so a path.name whose row stands is never freed.
    #[test]
    fn a_backbone_recovery_plans_the_delete_of_the_lost_birth_then_the_create() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.admin.1".parse().unwrap();
        let lost = crate::build::ExitedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let backbone = AttemptAction::Recover { path: path.clone(), node_id: lost.node_id.clone(), from_incarnation: lost.incarnation.clone(), backbone: true };
        let o = mn();
        assert_eq!(
            plan(&cur, &o, Some(&backbone)).operations,
            vec![BuildOperation::RetireNode { node: path.clone(), exited: Some(lost.clone()) }, BuildOperation::CreateNode { node: path.clone(), over: Some(lost), executable: None }]
        );
        let wire = crate::wire::action_round_trip(&backbone);
        assert_eq!(wire, backbone, "the backbone action crosses the Build topic as itself");
    }

    /// The replace recipe of `node` (lifecycles/node-replace.md steps 1-3, the predecessor retained).
    fn replace_recipe(node: &PathName) -> Vec<RecipeStep> {
        vec![
            RecipeStep::DeleteConnections { node: node.clone() },
            RecipeStep::LeaveMesh { node: node.clone() },
            RecipeStep::Update { node: node.clone(), name: node.renamed() },
            RecipeStep::Stop { node: node.renamed() },
            RecipeStep::Create { node: node.clone(), executable: None },
        ]
    }

    /// CONTRACT (build.md §1: "Rafka's replace recipe is submitted as one Build of node primitives";
    /// lifecycles/node-replace.md steps 1-3; the Build carrier: completed steps are not rerun): an
    /// accepted recipe pins the birth its predecessor path holds and plans its steps in order, each its
    /// op's own operation (`node.connections.delete:<name>`, `node.mesh.leave:<name>`,
    /// `node.update:<name>`, `node.stop:<name>.old`, `node.create:<name>`), never a replace operation
    /// and never a generic create or retire beside them. The predecessor's steps carry its pinned
    /// birth, followed through its rename to `.old`; the successor's create carries none (its executor
    /// resolves the node its create made at the name the rename released). A step whose `Complete` is
    /// receipted in an attempt that carried this recipe is not planned again; a `Complete` of the same
    /// operation in an attempt that carried another action is no progress. Refused by name: an empty
    /// recipe, an operation named twice, and a step on a path no birth holds and no earlier step makes.
    #[test]
    fn a_recipe_plans_its_steps_in_order_and_never_reruns_a_completed_one() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.1".parse().unwrap();
        let held = o.node(&path).unwrap();
        let pin = PinnedBirth { node_id: held.node_id.clone(), incarnation: held.incarnation_id.clone().unwrap() };
        let steps = replace_recipe(&path);
        let action = recipe_action(steps.clone(), &o).unwrap();
        assert_eq!(action, AttemptAction::Recipe { steps: steps.clone(), pinned: vec![(path.clone(), pin.clone())] }, "only the predecessor's path holds a birth at acceptance");
        let births = [Some(pin.clone()), Some(pin.clone()), Some(pin.clone()), Some(pin.clone()), None];
        let all: Vec<BuildOperation> = steps.iter().zip(births.iter()).map(|(s, b)| BuildOperation::RecipeStep { step: s.clone(), birth: b.clone() }).collect();
        assert_eq!(plan(&cur, &o, Some(&action)).operations, all);
        let keys: Vec<String> = all.iter().map(BuildOperation::key).collect();
        assert_eq!(
            keys,
            ["node.connections.delete:mesh1.rpc.1", "node.mesh.leave:mesh1.rpc.1", "node.update:mesh1.rpc.1", "node.stop:mesh1.rpc.1.old", "node.create:mesh1.rpc.1"]
        );
        assert!(!keys.iter().any(|k| k.starts_with("replace-node:")));
        let receipt = |attempt: u32, operation: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: operation.into(),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Running,
            attempt: 4,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Recipe,
            action: Some(action.clone()),
            attempt_actions: [(3, AttemptAction::Restart { path: path.clone(), from_incarnation: pin.incarnation.clone() }), (4, action.clone())].into(),
            attempt_executors: Default::default(),
            opened: 4,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        assert_eq!(plan_for_build(&build(vec![]), &o).operations, all, "nothing receipted: every step");
        let first_three = vec![receipt(4, &keys[0]), receipt(4, &keys[1]), receipt(4, &keys[2])];
        assert_eq!(plan_for_build(&build(first_three.clone()), &o).operations, all[3..].to_vec(), "a resume runs only what is left");
        assert_eq!(plan_for_build(&build(vec![receipt(3, &keys[1])]), &o).operations, all, "another action's Complete of the same operation is no progress");
        let mut done = first_three;
        done.extend(keys[3..].iter().map(|k| receipt(4, k)));
        assert_eq!(plan_for_build(&build(done), &o).operations, vec![], "every step receipted: the recipe is done");
        // Refusals.
        assert_eq!(recipe_action(vec![], &o), Err(BuildReject::EmptyRecipe));
        assert_eq!(
            recipe_action(vec![RecipeStep::Stop { node: path.clone() }, RecipeStep::Stop { node: path.clone() }], &o),
            Err(BuildReject::RecipeStepRepeated { operation: "node.stop:mesh1.rpc.1".into() })
        );
        assert_eq!(recipe_action(vec![RecipeStep::Start { node: "mesh1.rpc.9".parse().unwrap() }], &o), Err(BuildReject::RecipeNodeUnknown { step: 1, node: "mesh1.rpc.9".into() }));
        assert_eq!(
            recipe_action(vec![RecipeStep::Update { node: path.clone(), name: path.renamed() }, RecipeStep::Stop { node: path.clone() }], &o),
            Err(BuildReject::RecipeNodeUnknown { step: 2, node: "mesh1.rpc.1".into() }),
            "the node left the path at its rename: nothing is at it for the stop"
        );
    }

    /// CONTRACT: a standalone node.connections.delete or stop plans its one operation (`node.connections.delete:<path>` /
    /// `node.stop:<path>`) while the view holds the exact birth, never a create, a retire or a repair
    /// of another member whatever the view lacks; a path run by another birth plans nothing.
    #[test]
    fn a_connections_delete_or_stop_plans_only_its_operation_for_its_exact_birth() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let mut o = mn();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        o.nodes.retain(|x| x.name.to_string() != "mesh1.rpc.3");
        let connections = AttemptAction::DeleteConnections { path: path.clone(), from_incarnation: from.clone() };
        let stop = AttemptAction::Stop { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &o, Some(&connections)).operations, vec![BuildOperation::DeleteConnections { node: path.clone(), from_incarnation: from.clone() }]);
        assert_eq!(plan(&cur, &o, Some(&stop)).operations, vec![BuildOperation::StopNode { node: path.clone(), from_incarnation: from.clone() }]);
        assert_eq!(BuildOperation::DeleteConnections { node: path.clone(), from_incarnation: from.clone() }.key(), "node.connections.delete:mesh1.rpc.2");
        assert_eq!(BuildOperation::StopNode { node: path.clone(), from_incarnation: from.clone() }.key(), "node.stop:mesh1.rpc.2");
        // A ConnectionsDeleting or Leaving birth of the path is still the birth the operation acts on.
        for status in [NodeStatus::ConnectionsDeleting, NodeStatus::Leaving] {
            o.nodes.iter_mut().find(|x| x.name == path).unwrap().status = status;
            assert_eq!(plan(&cur, &o, Some(&stop)).operations.len(), 1, "{status:?}");
        }
        let mut later = mn();
        later.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&cur, &later, Some(&connections)).operations, vec![]);
        assert_eq!(plan(&cur, &later, Some(&stop)).operations, vec![]);
    }

    /// CONTRACT (lifecycles/node-delete.md): the delete of the parked predecessor at `<path>.old` plans its
    /// one operation `node.delete:<path>.old` until its `Complete` names the birth, whatever the view
    /// holds: the fabric-primary admitted it from the record it holds of that exact birth, and no view
    /// names a `.old` path but its replace's executor's. It never plans a create or repair of the
    /// path's successor.
    #[test]
    fn a_delete_of_a_stopped_old_birth_plans_only_its_operation_until_its_complete_names_the_birth() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let mut held = mn();
        let from = held.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().incarnation_id.clone().unwrap();
        let mut parked = held.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().clone();
        parked.name = path.clone();
        parked.status = NodeStatus::Leaving;
        parked.parked = true;
        held.nodes.iter_mut().find(|x| x.name.to_string() == "mesh1.rpc.2").unwrap().incarnation_id = Some(IncarnationId::mint());
        held.nodes.push(parked);
        held.nodes.retain(|x| x.name.to_string() != "mesh1.rpc.3");
        let delete = AttemptAction::Delete { path: path.clone(), from_incarnation: from.clone() };
        let op = vec![BuildOperation::DeleteNode { node: path.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan(&cur, &held, Some(&delete)).operations, op, "only the delete, never a repair of mesh1.rpc.3");
        assert_eq!(op[0].key(), "node.delete:mesh1.rpc.2.old");
        // The Complete commits no output: the attempt's fenced action names the birth.
        let step = |attempt: u32, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("node.delete:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let other = IncarnationId::mint();
        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 9,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Delete,
            action: Some(delete.clone()),
            // Attempt 8 deleted another parked birth at the path; attempt 9 is this delete.
            attempt_actions: [(8, AttemptAction::Delete { path: path.clone(), from_incarnation: other.clone() }), (9, delete.clone())].into(),
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let gone = mn();
        assert_eq!(plan_for_build(&build(vec![]), &gone).operations, op, "the view never names the parked birth (a fabric-primary that did not run its replace) and nothing began: owed");
        assert_eq!(plan_for_build(&build(vec![step(9, "NodeDeleting")]), &gone).operations, op, "begun, the birth gone from the view: still owed");
        assert_eq!(plan_for_build(&build(vec![step(9, "NodeDeleting"), step(9, "Complete")]), &gone).operations, vec![], "completed for this birth");
        assert_eq!(plan_for_build(&build(vec![step(8, "NodeDeleting"), step(8, "Complete")]), &gone).operations, op, "another birth's Complete is not this delete's");
    }

    /// CONTRACT: a standalone stop is planned until its `Complete` receipt names the exact birth; after it,
    /// whatever the view holds (nothing, once the Leaving birth is pruned) nothing is planned, and a
    /// `Complete` receipt of an attempt that carried a stop of another birth is not this stop's progress.
    #[test]
    fn a_stop_is_planned_until_its_complete_receipt_names_its_birth() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.3".parse().unwrap();
        let from = IncarnationId::mint();
        // The Complete commits no output: the attempt's fenced action names the birth.
        let step = |attempt: u32, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("node.stop:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let other = IncarnationId::mint();
        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 7,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Stop,
            action: Some(AttemptAction::Stop { path: path.clone(), from_incarnation: from.clone() }),
            // Attempt 6 stopped an earlier birth of the path; attempt 7 is this stop.
            attempt_actions: [(6, AttemptAction::Stop { path: path.clone(), from_incarnation: other.clone() }), (7, AttemptAction::Stop { path: path.clone(), from_incarnation: from.clone() })].into(),
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let mut gone = mn();
        gone.nodes.retain(|x| x.name != path);
        let stop = vec![BuildOperation::StopNode { node: path.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan_for_build(&build(vec![]), &gone).operations, vec![], "the view never held the birth and nothing began");
        assert_eq!(plan_for_build(&build(vec![step(7, "StopNode")]), &gone).operations, stop, "begun, the birth pruned from the view: still owed");
        assert_eq!(plan_for_build(&build(vec![step(7, "StopNode"), step(7, "Complete")]), &gone).operations, vec![], "completed for this birth");
        assert_eq!(plan_for_build(&build(vec![step(6, "StopNode"), step(6, "Complete")]), &gone).operations, vec![], "another birth's stop neither began nor completed this one");
        assert_eq!(plan_for_build(&build(vec![step(6, "StopNode"), step(6, "Complete"), step(7, "StopNode")]), &gone).operations, stop, "another birth's Complete is not this stop's");
    }

    /// CONTRACT (node-recovery.md, Operation summary "Steps"; proposal node-properties-own-commands.md
    /// §5 item 8, §8: "Replace's caller-meta copy step goes with recovery's"; rdm-code-conformance.md
    /// §0.1 and §2 item 4): an admitted recovery is two operations of its exited birth, in the doc's
    /// order and each by the delete and create workflows' own keys: node.delete of the exited instance
    /// (`node.delete:<path>`), then node.create at the same path.name over the inherited data dir
    /// (`node.create:<path>`), whose claim seeds config and tags from the Deleted row. There is no
    /// third step: nothing is copied after the create. It is planned whatever the view holds at the
    /// path (the exited birth unheard, nothing, or the new node live), never as a generic create, and
    /// no other member is swept. Every operation is executed by the exited birth's recoverer: a
    /// member's mesh-primary; a mesh-primary's own recovery by the admin its mesh seats in its place,
    /// never the exited primary the view may still seat. What is left is read from the Build's
    /// receipts: once the delete's `NodeDeleted` names the exited birth and its `Complete` follows,
    /// the create; once the create's `Complete` follows, nothing. Another birth's `NodeDeleted`, a
    /// `NodeDeleted` with no `Complete`, and a `Complete` from before the deletion (the path's original
    /// create) are no progress.
    /// Must NOT happen: one fused `recover-node:<path>` operation, a node.update (`node.update:<path>`)
    /// planned after the create, or the delete planned again once it completed (a second
    /// `NodeDeleted` of the exited birth).
    #[test]
    fn a_recovery_plans_node_delete_then_node_create_of_its_exited_birth_and_nothing_after() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let (node_id, from) = (NodeId::mint(), IncarnationId::mint());
        let receipt = |attempt: u32, operation: &str, step: &str, output: Option<serde_json::Value>| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("{operation}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output,
            executor: None,
        };
        let deleted = |attempt: u32, id: &NodeId, inc: &IncarnationId| {
            let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("node.delete:{path}"), node_id: id.clone(), incarnation: inc.clone(), name: path.clone(), event_at_rafka_ms: 1 };
            receipt(attempt, "node.delete", "NodeDeleted", Some(serde_json::to_value(op).unwrap()))
        };
        let complete = |attempt: u32, operation: &str| receipt(attempt, operation, "Complete", None);
        let build = |path: &PathName, node_id: &NodeId, steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 4,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Recovery,
            action: Some(AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone(), backbone: false }),
            attempt_actions: Default::default(),
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let keys = |steps: Vec<BuildStepReceipt>, o: &Topology| plan_for_build(&build(&path, &node_id, steps), o).operations.iter().map(|op| op.key()).collect::<Vec<_>>();
        let all = vec![format!("node.delete:{path}"), format!("node.create:{path}")];
        // Whatever the view holds at the path, and with another member unheard and a third missing:
        // the two operations, nothing else.
        for dead in [NodeStatus::PendingReconnect, NodeStatus::Dead] {
            let mut o = mn();
            o.nodes.iter_mut().find(|x| x.name == path).unwrap().status = dead;
            o.nodes.iter_mut().find(|x| x.name.to_string() == "mesh1.rpc.3").unwrap().status = dead;
            o.nodes.retain(|x| x.name.to_string() != "mesh1.admin.2");
            assert_eq!(keys(vec![], &o), all, "{dead:?}");
            for op in &plan_for_build(&build(&path, &node_id, vec![]), &o).operations {
                assert_eq!(crate::executor::executor_for(op, &o).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.1"), "{} is the member's mesh-primary's", op.key());
            }
        }
        let mut gone = mn();
        gone.nodes.retain(|x| x.name != path);
        assert_eq!(keys(vec![], &gone), all, "absent from the view");
        assert_eq!(keys(vec![], &mn()), all, "another birth live at the path");
        // Progress, read from the receipts.
        let original_create = complete(1, "node.create");
        assert_eq!(keys(vec![original_create.clone(), deleted(4, &NodeId::mint(), &from), complete(4, "node.delete")], &gone), all, "another node_id's NodeDeleted");
        assert_eq!(keys(vec![deleted(4, &node_id, &IncarnationId::mint()), complete(4, "node.delete")], &gone), all, "another incarnation's NodeDeleted");
        assert_eq!(keys(vec![deleted(4, &node_id, &from)], &gone), all, "NodeDeleted without the delete's Complete: the delete is owed");
        assert_eq!(keys(vec![complete(3, "node.delete"), deleted(4, &node_id, &from)], &gone), all, "an earlier attempt's Complete is not this deletion's");
        let created_left = all[1..].to_vec();
        assert_eq!(keys(vec![original_create.clone(), deleted(4, &node_id, &from), complete(4, "node.delete")], &gone), created_left, "deleted: the create is left, the path's original create is not this one");
        assert_eq!(keys(vec![deleted(4, &node_id, &from), complete(5, "node.delete")], &gone), created_left, "deleted on a rerun that reused the NodeDeleted");
        let recovered = vec![original_create, deleted(4, &node_id, &from), complete(4, "node.delete"), complete(5, "node.create")];
        assert_eq!(keys(recovered.clone(), &mn()), Vec::<String>::new(), "created after the deletion: recovered, nothing is left");
        assert_eq!(keys(recovered, &gone), Vec::<String>::new(), "recovered, whatever the view holds");
        // A mesh-primary's own recovery: the view may still seat the exited primary; every operation
        // is executed by the admin its mesh seats in its place.
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let mut o = mn();
        o.nodes.iter_mut().find(|x| x.name == admin).unwrap().status = NodeStatus::PendingReconnect;
        let ops = plan_for_build(&build(&admin, &node_id, vec![]), &o).operations;
        assert_eq!(ops.iter().map(|op| op.key()).collect::<Vec<_>>(), vec![format!("node.delete:{admin}"), format!("node.create:{admin}")]);
        for op in &ops {
            assert_eq!(crate::executor::executor_for(op, &o).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.2"), "{} of the exited mesh-primary is executed by its mesh's next admin", op.key());
        }
    }

    /// CONTRACT (lock D): a mesh from its counts carries an explicit path config for every path, the
    /// kind's migration default; a grow mints a path config with the path and a shrink drops it; a
    /// path without a path config, or a path config without a path, is refused by name; a path
    /// config set explicitly stands.
    #[test]
    fn every_materialized_path_carries_exactly_one_path_config() {
        use rdm_core::meta::{PathConfig, PersistentRetireDisposition, StorageMeta};
        let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 2), (NodeKind::Gateway, 1), (NodeKind::RpcNode, 1)]));
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.path_config_by_path.len(), 5);
        let preserve = StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve };
        assert_eq!(m.path_config(&"mesh1.broker.2".parse().unwrap()).unwrap().storage, preserve);
        assert_eq!(m.path_config(&"mesh1.admin.1".parse().unwrap()).unwrap().storage, preserve);
        assert_eq!(m.path_config(&"mesh1.gateway.1".parse().unwrap()).unwrap().storage, StorageMeta::Ephemeral);
        assert_eq!(m.path_config(&"mesh1.rpc.1".parse().unwrap()).unwrap().storage, StorageMeta::Ephemeral);
        let mut topology = FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m.clone())].into_iter().collect(), bindings: None };
        assert_eq!(topology.validate(), Ok(()));
        // Grow and shrink keep the path configs in step with the paths.
        let observed = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![],
            nodes: vec![],
        };
        resize(&mut m, &MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 3), (NodeKind::Gateway, 0), (NodeKind::RpcNode, 1)]), &observed);
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.path_config_by_path.len(), 5);
        assert_eq!(m.path_config(&"mesh1.broker.3".parse().unwrap()).unwrap().storage, preserve, "the grown path carries its default");
        assert!(m.path_config(&"mesh1.gateway.1".parse().unwrap()).is_none(), "the dropped path's config goes with it");
        // An explicit path config stands; a path the mesh does not hold is refused.
        let release = PathConfig { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }, placement: Default::default(), executable: None, tags: Default::default() };
        assert_eq!(m.set_path_config(&"mesh1.rpc.1".parse().unwrap(), release.clone()), Ok(()));
        assert_eq!(m.path_config(&"mesh1.rpc.1".parse().unwrap()), Some(&release));
        assert!(matches!(m.set_path_config(&"mesh1.rpc.9".parse().unwrap(), release.clone()), Err(BuildReject::UnknownNode { .. })));
        // The consistency ratchet.
        m.path_config_by_path.remove(&"mesh1.rpc.1".parse().unwrap());
        m.path_config_by_path.insert("mesh1.compute.7".parse().unwrap(), release);
        topology.meshes.insert("mesh1".into(), m);
        assert_eq!(
            topology.validate(),
            Err(BuildReject::PathConfigMismatch { mesh: "mesh1".into(), missing: vec!["mesh1.rpc.1".into()], extra: vec!["mesh1.compute.7".into()] })
        );
    }

    /// A binding set whose every binding names `tag` (its paths and sha256s), for `launch_ids`.
    fn set_of(tag: &str, launch_ids: &[&str]) -> rdm_core::binding::BindingSet {
        let bind = |id: &str| rdm_core::binding::Binding { launch_id: id.into(), executable: format!("/{tag}/{id}").into(), sha256: format!("{tag}-{id}"), image: None };
        rdm_core::binding::BindingSet { candidate: rdm_core::binding::Candidate { sha: tag.into(), build: tag.into() }, launch_ids: launch_ids.iter().map(|s| s.to_string()).collect(), bindings: launch_ids.iter().map(|id| bind(id)).collect() }
    }

    fn set(tag: &str) -> rdm_core::binding::BindingSet {
        set_of(tag, &["node_admin", "rpc_node"])
    }

    /// CONTRACT (proposal roll.md step 2: "The set is a field of the accepted topology ... every Build
    /// compiles from the current topology, so a resize, an added node, a mesh created or removed after a
    /// roll carries the roll's set forward unchanged, and only a `Roll` replaces it"; "Wire and storage":
    /// "a record written before it decodes with none"): a roll's compile makes the set the topology's
    /// launch set and changes no path of a mesh with two node-admins; every other change carries it
    /// forward; a second roll replaces it. The topology keeps it through its JSON shape, and one written
    /// before the field decodes with none. Must NOT happen: a resize after a roll dropping the set.
    #[test]
    fn a_rolls_set_is_the_topologys_launch_set_carried_by_every_compile_and_replaced_only_by_a_roll() {
        let cur = t(&[("mesh1", 2, 3)]);
        let rolled = compile(&cur, &TopologyChange::Roll { bindings: set("b") }, &mn()).unwrap();
        assert_eq!(rolled.bindings, Some(set("b")), "the roll's set is the topology's");
        assert_eq!(rolled.meshes, cur.meshes, "a roll changes no path.name of a mesh with two node-admins");
        assert_eq!(rolled.binding_of(NodeKind::RpcNode).map(|b| b.sha256.as_str()), Some("b-rpc_node"));
        let changes = [
            TopologyChange::ReconcileMesh { desired: counts("mesh1", 2, 4) },
            TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode },
            TopologyChange::CreateMesh { desired: counts("mesh2", 1, 1) },
            TopologyChange::RemoveNode { node: "mesh1.rpc.3".parse().unwrap() },
        ];
        for change in &changes {
            assert_eq!(compile(&rolled, change, &mn()).unwrap().bindings, Some(set("b")), "{change:?} carries the roll's set forward");
        }
        assert_eq!(compile(&rolled, &TopologyChange::Roll { bindings: set("c") }, &mn()).unwrap().bindings, Some(set("c")), "only a roll replaces it");
        let json = serde_json::to_value(&rolled).unwrap();
        assert_eq!(serde_json::from_value::<FabricTopology>(json).unwrap(), rolled, "the journal's shape keeps it");
        let old = serde_json::to_value(&cur).unwrap();
        assert!(old.get("bindings").is_none(), "a topology with no set writes no field");
        assert_eq!(serde_json::from_value::<FabricTopology>(old).unwrap().bindings, None, "a topology written before the field decodes with none");
    }

    /// CONTRACT (proposal roll.md step 6, R4 arm A, Luke 2026-10-24: "The roll's compile adds a second
    /// node-admin path to such a mesh (`<mesh>.admin.<next free>`), created first from the set ... The
    /// added admin stays in the accepted topology after the roll"): mesh1's only node-admin gains
    /// `mesh1.admin.2` with its kind's path config; mesh2, with two, gains none. The roll's plan creates
    /// the added admin first, launching the set's node_admin binding, before any member of its mesh is
    /// replaced. Must NOT happen: the only admin of a mesh replaced with no other listening admin.
    #[test]
    fn a_roll_gives_a_mesh_with_one_node_admin_a_second_created_first_from_the_set() {
        use NodeStatus::ReadyForTraffic as R;
        let cur = t(&[("mesh1", 1, 1), ("mesh2", 2, 0)]);
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.rpc.1", R, true), n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false)], &["mesh1", "mesh2"]);
        let rolled = compile(&cur, &TopologyChange::Roll { bindings: set("b") }, &o).unwrap();
        assert_eq!(paths(rolled.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"], "mesh1 gains its second node-admin");
        assert_eq!(rolled.mesh("mesh1").unwrap().path_config(&"mesh1.admin.2".parse().unwrap()), Some(&PathConfig::default_for(NodeKind::NodeAdmin)));
        assert_eq!(rolled.mesh("mesh2").unwrap(), cur.mesh("mesh2").unwrap(), "a mesh with two admins gains none");
        assert_eq!(rolled.validate(), Ok(()));
        let ops = roll_plan(&rolled, &o, &Recorded::new(), vec![]);
        assert_eq!(ops.first(), Some(&BuildOperation::CreateNode { node: "mesh1.admin.2".parse().unwrap(), over: None, executable: set("b").of(NodeKind::NodeAdmin).cloned() }), "the added admin is created first, from the set: {ops:?}");
        let first_replacement = ops.iter().position(|op| matches!(op, BuildOperation::RetireNode { .. })).expect("a replacement");
        assert!(first_replacement > 0);
    }

    /// A roll Build of `topology` (its change a Roll) with `steps`, planned against `o` and `recorded`.
    fn roll_plan(topology: &FabricTopology, o: &Topology, recorded: &Recorded, steps: Vec<crate::build_state::BuildStepReceipt>) -> Vec<BuildOperation> {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState};
        let build = BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: topology.clone(),
            submitted_change: Some(TopologyChange::Roll { bindings: topology.bindings.clone().unwrap() }),
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 1,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Requested,
            action: None,
            attempt_actions: Default::default(),
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        plan_for_roll(&build, o, recorded).operations
    }

    /// CONTRACT (proposal roll.md step 3: "Each attempt computes remaining work as the members whose row
    /// records a sha256 other than the set's for their kind, or none ... and orders them ... mesh by mesh;
    /// in a mesh its non-primary node-admins, then its primary node-admin, then the nodes of each other
    /// type, non-primary before primary"; step 4: "`node.delete` ... Then `node.create` at the same
    /// `path.name`, the Create step carrying the set's binding of its kind ... over the kept data dir";
    /// "Interruptions resume from observed state": "predecessor deleted and the successor not yet
    /// created ...: the remaining work is the create; successor created and its receipt not written (the
    /// row names a live birth on the set's sha): nothing remains for it ... No member is replaced
    /// twice"): mesh1's members are replaced non-primary admin first, then its primary admin, then its
    /// rpc primary, the rpc node already on the set left alone; mesh2's only rpc node is replaced like
    /// any member (R5). Each replacement is node.delete of the birth the view holds, then node.create at
    /// its path over that birth, carrying the set's binding of its kind. Resumed: a member whose delete
    /// is receipted owes its create alone, never a create of the generic loop; a create receipted after
    /// it leaves nothing; a successor live on the set's sha whose create was never receipted leaves
    /// nothing; a member a row records with no executable is owed. A Build that is not a roll plans no
    /// replacement. Must NOT happen: a member replaced twice, a primary replaced before its type's other
    /// members, or a successor launched from the binding its Deleted row records.
    #[test]
    fn a_rolls_plan_is_the_members_off_the_set_in_order_and_resumes_from_its_receipts() {
        use crate::build_state::{BuildStepReceipt, StepOutcome};
        use NodeStatus::ReadyForTraffic as R;
        let cur = t(&[("mesh1", 2, 2), ("mesh2", 2, 1)]);
        let nodes = vec![n("mesh1.admin.1", R, true), n("mesh1.admin.2", R, false), n("mesh1.rpc.1", R, false), n("mesh1.rpc.2", R, true), n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false), n("mesh2.rpc.1", R, true)];
        let o = observed(nodes.clone(), &["mesh1", "mesh2"]);
        let rolled = compile(&cur, &TopologyChange::Roll { bindings: set("b") }, &o).unwrap();
        let birth = |name: &str, o: &Topology| o.node(&name.parse().unwrap()).map(|n| crate::build::ExitedBirth { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap() }).unwrap();
        let on = |name: &str, tag: Option<&str>, o: &Topology| {
            let node = o.node(&name.parse().unwrap()).unwrap();
            (node.name.clone(), RecordedRow { node_id: node.node_id.clone(), sha256: tag.map(|t| format!("{t}-{}", node.name.kind.name())) })
        };
        // Every member on set "a", but mesh1.rpc.1 on "b" already and mesh2.admin.2 a row with no executable.
        let mut recorded: Recorded = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.2", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|p| on(p, Some("a"), &o)).collect();
        recorded.extend([on("mesh1.rpc.1", Some("b"), &o), on("mesh2.admin.2", None, &o)]);
        let replace = |name: &str, o: &Topology| {
            let node: PathName = name.parse().unwrap();
            let executable = rolled.binding_of(node.kind).cloned();
            vec![BuildOperation::RetireNode { node: node.clone(), exited: None }, BuildOperation::CreateNode { node, over: Some(birth(name, o)), executable }]
        };
        let all: Vec<BuildOperation> = ["mesh1.admin.2", "mesh1.admin.1", "mesh1.rpc.2", "mesh2.admin.2", "mesh2.admin.1", "mesh2.rpc.1"].iter().flat_map(|p| replace(p, &o)).collect();
        assert_eq!(roll_plan(&rolled, &o, &recorded, vec![]), all, "non-primary admins, the primary admin, then each type non-primary before primary; the member on the set is left alone");
        assert_eq!(plan(&rolled, &o, None).operations, vec![], "a Build that is not a roll plans no replacement");

        let step = |attempt: u32, operation: &str, step: &str, output: Option<serde_json::Value>| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: operation.into(),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output,
            executor: None,
        };
        let deleted = |attempt: u32, name: &str, b: &crate::build::ExitedBirth| {
            let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("node.delete:{name}"), node_id: b.node_id.clone(), incarnation: b.incarnation.clone(), name: name.parse().unwrap(), event_at_rafka_ms: 1 };
            step(attempt, &format!("node.delete:{name}"), "NodeDeleted", Some(serde_json::to_value(op).unwrap()))
        };
        // The executor lost after mesh1.admin.2's node.deleted: its create alone is owed, over the deleted
        // birth, launching the set's binding; the view no longer holds it, and the generic create does not plan it.
        let gone = birth("mesh1.admin.2", &o);
        let mut after_delete = o.clone();
        after_delete.nodes.retain(|n| n.name.to_string() != "mesh1.admin.2");
        let receipts = vec![deleted(1, "mesh1.admin.2", &gone), step(1, "node.delete:mesh1.admin.2", "Complete", None)];
        let owed = roll_plan(&rolled, &after_delete, &recorded, receipts.clone());
        assert_eq!(owed[0], BuildOperation::CreateNode { node: "mesh1.admin.2".parse().unwrap(), over: Some(gone.clone()), executable: rolled.binding_of(NodeKind::NodeAdmin).cloned() }, "{owed:?}");
        assert_eq!(owed.iter().filter(|op| op.key() == "node.create:mesh1.admin.2").count(), 1, "one create of the member, the roll's: {owed:?}");
        assert_eq!(owed[1..], all[2..], "the rest as before");
        // Killed after the successor's check-in and before its receipt: the successor is live on the set's
        // sha, and nothing remains for it.
        let mut successor = n("mesh1.admin.2", R, false);
        successor.node_id = NodeId::mint();
        let mut live_again = after_delete.clone();
        live_again.nodes.push(successor.clone());
        let mut on_set = recorded.clone();
        on_set.insert("mesh1.admin.2".parse().unwrap(), RecordedRow { node_id: successor.node_id.clone(), sha256: Some("b-node_admin".into()) });
        assert_eq!(roll_plan(&rolled, &live_again, &on_set, receipts.clone()), all[2..].to_vec(), "a live successor on the set is the member");
        // Its create receipted: never replaced again, whatever its row reads.
        let mut done = receipts;
        done.push(step(2, "node.create:mesh1.admin.2", "Complete", None));
        assert_eq!(roll_plan(&rolled, &live_again, &recorded, done), all[2..].to_vec(), "no member is replaced twice");
    }

    /// CONTRACT (proposal roll.md step 3: "Interruptions resume from observed state ...: predecessor
    /// deleted and the successor not yet created (the row at the path.name is Deleted, recording the old
    /// sha, the name free): the remaining work is the create"; "Each attempt computes remaining work as
    /// the members whose row records a sha256 other than the set's"; step 4: the predecessor's
    /// `node.delete`, then `node.create` "over the kept data dir"; R1): mesh2.rpc.1's delete began
    /// (`NodeDeleting` of its birth, its `TerminateRuntime` receipted) and no `NodeDeleted` was receipted,
    /// and the view no longer holds the birth live. Its row Deleted: the create alone is owed, over that
    /// birth. Its row still live naming that birth: the delete of that exact birth is finished first
    /// (targeted at its incarnation, as a resumed delete whose birth the view no longer holds is), then
    /// the create over it. A view that still holds the birth Leaving plans the same.
    /// Must NOT happen: the generic create at the path (over a newly allocated data dir), or two creates.
    #[test]
    fn a_rolls_delete_without_its_node_deleted_receipt_is_resumed_from_its_receipts_and_its_row() {
        use crate::build_state::{BuildStepReceipt, StepOutcome};
        use NodeStatus::ReadyForTraffic as R;
        let cur = t(&[("mesh2", 2, 1)]);
        let nodes = vec![n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false), n("mesh2.rpc.1", R, true)];
        let o = observed(nodes, &["mesh2"]);
        let rolled = compile(&cur, &TopologyChange::Roll { bindings: set("b") }, &o).unwrap();
        let path: PathName = "mesh2.rpc.1".parse().unwrap();
        let node = o.node(&path).unwrap().clone();
        let birth = crate::build::ExitedBirth { node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().unwrap() };
        let mut recorded: Recorded = o.nodes.iter().filter(|n| n.name.kind == NodeKind::NodeAdmin).map(|n| (n.name.clone(), RecordedRow { node_id: n.node_id.clone(), sha256: Some(format!("b-{}", n.name.kind.name())) })).collect();
        let step = |s: &str, output: Option<serde_json::Value>| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt: 1,
            operation: "node.delete:mesh2.rpc.1".into(),
            step: s.into(),
            outcome: StepOutcome::Complete,
            output,
            executor: None,
        };
        let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt: 1, operation: "node.delete:mesh2.rpc.1".into(), node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 };
        let receipts = vec![step("NodeDeleting", Some(serde_json::to_value(op).unwrap())), step("StopNode", None), step("AwaitNodeLeft", None), step("TerminateRuntime", None)];
        let create = BuildOperation::CreateNode { node: path.clone(), over: Some(birth.clone()), executable: rolled.binding_of(NodeKind::RpcNode).cloned() };
        let mut gone = o.clone();
        gone.nodes.retain(|n| n.name != path);
        let mut leaving = o.clone();
        leaving.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Leaving);
        // The row Deleted (no live row at the path): the create alone, over the deleted birth.
        for view in [&gone, &leaving] {
            assert_eq!(roll_plan(&rolled, view, &recorded, receipts.clone()), vec![create.clone()], "the row Deleted: the remaining work is the create");
        }
        // The row still live, naming the birth on the old sha: the delete of that exact birth is finished, then the create.
        recorded.insert(path.clone(), RecordedRow { node_id: birth.node_id.clone(), sha256: Some("a-rpc_node".into()) });
        let finish = BuildOperation::DeleteNode { node: path.clone(), from_incarnation: birth.incarnation.clone() };
        for view in [&gone, &leaving] {
            assert_eq!(roll_plan(&rolled, view, &recorded, receipts.clone()), vec![finish.clone(), create.clone()], "the row live: the delete is finished, then the create");
        }
    }

    /// CONTRACT (states.md "Resize and roll": "During a bin roll, a member still on its old
    /// binary/identity does not satisfy its planned replacement ... the fabric cannot climb, until the
    /// roll's last planned birth checks in"): the members whose live row does not record the set's sha256
    /// for their kind are named, in path order, a row with no executable and a path no live row is
    /// written at among them; a topology with no set names none.
    #[test]
    fn the_members_off_the_rolled_set_are_named_until_each_row_records_its_sha() {
        let mut rolled = t(&[("mesh1", 2, 1)]);
        rolled.bindings = Some(set("b"));
        let row = |tag: Option<&str>, kind: &str| RecordedRow { node_id: NodeId::mint(), sha256: tag.map(|t| format!("{t}-{kind}")) };
        let mut recorded: Recorded = [("mesh1.admin.1".parse().unwrap(), row(Some("b"), "node_admin")), ("mesh1.admin.2".parse().unwrap(), row(None, "node_admin")), ("mesh1.rpc.1".parse().unwrap(), row(Some("a"), "rpc_node"))].into();
        assert_eq!(off_the_set(&rolled, &recorded).iter().map(ToString::to_string).collect::<Vec<_>>(), ["mesh1.admin.2", "mesh1.rpc.1"]);
        recorded.insert("mesh1.admin.2".parse().unwrap(), row(Some("b"), "node_admin"));
        recorded.insert("mesh1.rpc.1".parse().unwrap(), row(Some("b"), "rpc_node"));
        assert_eq!(off_the_set(&rolled, &recorded), Vec::<PathName>::new());
        recorded.remove(&"mesh1.rpc.1".parse::<PathName>().unwrap());
        assert_eq!(off_the_set(&rolled, &recorded).iter().map(ToString::to_string).collect::<Vec<_>>(), ["mesh1.rpc.1"], "a path no live row is written at is not on the set");
        assert_eq!(off_the_set(&t(&[("mesh1", 2, 1)]), &Recorded::new()), Vec::<PathName>::new(), "no set, no blocker");
    }

    /// The binding `tag` names for the node at `path`: its kind's launch id, a path and sha of `tag`.
    fn exe(tag: &str, kind: NodeKind) -> rdm_core::binding::Binding {
        rdm_core::binding::Binding { launch_id: kind.name().into(), executable: format!("/{tag}/{}", kind.name()).into(), sha256: format!("{tag}-{}", kind.name()), image: None }
    }

    /// `path`'s entry in a complete-topology request: its kind's default config on `tag`'s executable.
    fn entry(path: &str, tag: Option<&str>) -> (PathName, PathConfig) {
        let p: PathName = path.parse().unwrap();
        let mut c = PathConfig::default_for(p.kind);
        c.executable = tag.map(|t| exe(t, p.kind));
        (p, c)
    }

    fn reconcile(nodes: Vec<(PathName, PathConfig)>) -> TopologyChange {
        TopologyChange::Reconcile { based_on: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b0")).unwrap(), nodes }
    }

    /// What the rows record: every live member of `o` on `tag`'s executable of its kind.
    fn rows_on(o: &Topology, tag: &str) -> Recorded {
        o.nodes.iter().map(|n| (n.name.clone(), RecordedRow { node_id: n.node_id.clone(), sha256: Some(format!("{tag}-{}", n.name.kind.name())) })).collect()
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: "the complete topology, an array of nodes,
    /// each identified by its `path.name` ... From the accepted result: a node left out is removed; one added
    /// is created ... A mesh named for the first time comes into being with the create of its first
    /// node-admin; a mesh absent from the request is mesh.delete; a request that ... lists no nodes at all, is
    /// refused by name pointing to fabric.delete ... a created path whose config names no executable is
    /// refused by name at acceptance"): a request compiles to exactly its listed paths, each with its listed
    /// config; a kept mesh keeps its id, a new mesh enters under an id of its own, a mesh left out is gone;
    /// a held path listed with no executable keeps its entry's. Refused by name, nothing compiled: no node
    /// listed (naming fabric.delete), a created path with no executable, a path named twice, a `.old`
    /// path, a mesh with no node-admin. Must NOT happen: a stale or missing executable run on a created path,
    /// or an empty list read as delete everything.
    #[test]
    fn a_complete_topology_request_compiles_to_exactly_its_listed_paths_and_refuses_by_name() {
        let mut cur = t(&[("mesh1", 2, 3), ("mesh3", 1, 0)]);
        let held: PathName = "mesh1.rpc.1".parse().unwrap();
        cur.meshes.get_mut("mesh1").unwrap().path_config_by_path.get_mut(&held).unwrap().executable = Some(exe("a", NodeKind::RpcNode));
        let request = vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), entry("mesh1.rpc.1", None), entry("mesh1.rpc.2", Some("b")), entry("mesh2.admin.1", Some("b")), entry("mesh2.rpc.1", Some("b"))];
        let next = compile(&cur, &reconcile(request), &mn()).unwrap();
        assert_eq!(next.meshes.keys().collect::<Vec<_>>(), ["mesh1", "mesh2"], "mesh3, left out, is gone (its mesh.delete)");
        assert_eq!(paths(next.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2"], "mesh1.rpc.3, left out, is removed");
        assert_eq!(next.mesh("mesh1").unwrap().mesh_id, cur.mesh("mesh1").unwrap().mesh_id, "a kept mesh keeps its id");
        assert!(next.meshes.values().all(|m| m.mesh_id != next.mesh("mesh2").unwrap().mesh_id || m.name == "mesh2") && cur.meshes.values().all(|m| m.mesh_id != next.mesh("mesh2").unwrap().mesh_id), "mesh2 enters under an id of its own");
        assert_eq!(paths(next.mesh("mesh2").unwrap()), ["mesh2.admin.1", "mesh2.rpc.1"], "a mesh named for the first time, with its first node-admin");
        let config = |p: &str| next.mesh(&p.parse::<PathName>().unwrap().mesh).unwrap().path_config(&p.parse().unwrap()).unwrap().executable.clone();
        assert_eq!(config("mesh1.rpc.1"), Some(exe("a", NodeKind::RpcNode)), "a held path listed with none keeps its entry's");
        assert_eq!(config("mesh1.rpc.2"), Some(exe("b", NodeKind::RpcNode)), "a held path's entry takes the executable the request names");
        assert_eq!(config("mesh1.admin.1"), None, "a held path whose entry named none and that the request names none for stays as it was: nothing to replace");
        assert_eq!(config("mesh2.admin.1"), Some(exe("b", NodeKind::NodeAdmin)));
        assert_eq!(next.validate(), Ok(()));
        for (request, want) in [
            (vec![], BuildReject::NoNodeListed),
            (vec![entry("mesh1.admin.1", None), entry("mesh1.rpc.4", None)], BuildReject::ExecutableMissing { node: "mesh1.rpc.4".into() }),
            (vec![entry("mesh1.admin.1", None), entry("mesh2.admin.1", None)], BuildReject::ExecutableMissing { node: "mesh2.admin.1".into() }),
            (vec![entry("mesh1.admin.1", None), entry("mesh1.admin.1", Some("b"))], BuildReject::NodeNamedTwice { node: "mesh1.admin.1".into() }),
            (vec![entry("mesh1.admin.1", None), entry("mesh1.rpc.1.old", Some("b"))], BuildReject::UnknownNode { node: "mesh1.rpc.1.old".into() }),
            (vec![entry("mesh1.admin.1", None), entry("mesh2.rpc.1", Some("b"))], BuildReject::MeshWithoutAdmin { mesh: "mesh2".into() }),
        ] {
            assert_eq!(compile(&cur, &reconcile(request.clone()), &mn()), Err(want), "{request:?}");
        }
        assert!(BuildReject::NoNodeListed.to_string().contains("fabric.delete"), "the refusal names fabric.delete");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: the request is "an array of nodes, each
    /// identified by its `path.name` ... with `tags`, `storage`, `placement` and `executable`";
    /// node-rpc-envelope.md node.tags: "A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading
    /// '.'"; rule 5: a replacement's "config and tags are inherited from the Deleted row"): a created
    /// path's entry keeps the tag labels the request lists for it, which the node carries from its claim; a
    /// held path keeps its entry's whatever the request lists (a live node's labels are node.tags.create's
    /// and node.tags.delete's), so a request built from a read that holds no tags changes nothing; a label
    /// outside the grammar is refused by name, `tags-refused`, nothing compiled. Must NOT happen: a created
    /// path's tags dropped, a held path's entry changed by a request's tags, or a label the tags index
    /// cannot key accepted.
    #[test]
    fn a_request_entrys_tags_are_kept_on_the_entry_and_a_label_outside_the_grammar_is_refused_by_name() {
        let cur = t(&[("mesh1", 2, 3)]);
        let tagged = |path: &str, tags: &[&str]| {
            let (p, mut c) = entry(path, Some("b"));
            c.tags = tags.iter().map(|t| t.to_string()).collect();
            (p, c)
        };
        let next = compile(&cur, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &["zone_a", "v1.2~rc"])]), &mn()).unwrap();
        let p: PathName = "mesh1.rpc.4".parse().unwrap();
        assert_eq!(next.mesh("mesh1").unwrap().path_config(&p).unwrap().tags, ["v1.2~rc".to_string(), "zone_a".to_string()].into(), "the created path's entry keeps its tags");
        let again = compile(&next, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &[])]), &mn()).unwrap();
        assert_eq!(again, next, "a held path keeps its entry's tags: the same request with none changes nothing");
        let other = compile(&next, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &["zone_b"])]), &mn()).unwrap();
        assert_eq!(other, next, "nor with others");
        let refused = compile(&cur, &reconcile(vec![entry("mesh1.admin.1", None), tagged("mesh1.rpc.4", &["Zone A"])]), &mn());
        assert!(matches!(&refused, Err(BuildReject::TagsRefused { node, .. }) if node == "mesh1.rpc.4"), "{refused:?}");
        assert_eq!(refused.unwrap_err().reason(), "tags-refused");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 5, R4 arm A: "a mesh whose only node-admin is to
    /// be replaced first gets a second node-admin on the new executable at a new `path.name` of the
    /// node-admin type in that mesh (the next free one, `mesh1.admin.2`), whose entry the compiler adds to
    /// the accepted topology at acceptance, so every attempt and every read holds it"): a request naming
    /// another executable for mesh1's only node-admin than its row records adds `mesh1.admin.2` on the new
    /// executable; mesh2, with two admins, gains none; a request naming the executable the row records adds
    /// none. The plan creates the added admin first, on the new executable, before the original's
    /// replacement. Must NOT happen: a mesh's only admin replaced with no other admin listening.
    #[test]
    fn a_request_replacing_a_meshs_only_admin_adds_a_second_on_the_new_executable_at_acceptance() {
        use NodeStatus::ReadyForTraffic as R;
        let cur = t(&[("mesh1", 1, 1), ("mesh2", 2, 0)]);
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.rpc.1", R, true), n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false)], &["mesh1", "mesh2"]);
        let rows = rows_on(&o, "a");
        let request = |admin: &str| reconcile(vec![entry("mesh1.admin.1", Some(admin)), entry("mesh1.rpc.1", Some("a")), entry("mesh2.admin.1", Some("b")), entry("mesh2.admin.2", Some("b"))]);
        let next = compile_against(&cur, &request("b"), &o, &rows).unwrap();
        assert_eq!(paths(next.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"], "mesh1 gains its second node-admin at acceptance");
        assert_eq!(next.mesh("mesh1").unwrap().path_config(&"mesh1.admin.2".parse().unwrap()).unwrap().executable, Some(exe("b", NodeKind::NodeAdmin)), "on the new executable");
        assert_eq!(paths(next.mesh("mesh2").unwrap()), ["mesh2.admin.1", "mesh2.admin.2"], "a mesh with two admins gains none");
        let same = compile_against(&cur, &request("a"), &o, &rows).unwrap();
        assert_eq!(paths(same.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.rpc.1"], "an admin on the executable its row records is not replaced: no second admin");
        let ops = replacement_plan(&next, &o, &rows, vec![]);
        let created = ops.iter().position(|op| op == &BuildOperation::CreateNode { node: "mesh1.admin.2".parse().unwrap(), over: None, executable: Some(exe("b", NodeKind::NodeAdmin)) }).unwrap_or_else(|| panic!("the added admin is created: {ops:?}"));
        let replaced = ops.iter().position(|op| op == &BuildOperation::RetireNode { node: "mesh1.admin.1".parse().unwrap(), exited: None }).unwrap_or_else(|| panic!("the original is replaced: {ops:?}"));
        assert!(created < replaced, "the added admin is created first: {ops:?}");
    }

    /// A Build accepted from `request`'s change over `topology` with `steps`, planned against `o` and `recorded`.
    fn replacement_plan(topology: &FabricTopology, o: &Topology, recorded: &Recorded, steps: Vec<crate::build_state::BuildStepReceipt>) -> Vec<BuildOperation> {
        replacement_build_plan(topology, o, recorded, steps, None, Default::default())
    }

    fn replacement_build_plan(topology: &FabricTopology, o: &Topology, recorded: &Recorded, steps: Vec<crate::build_state::BuildStepReceipt>, action: Option<AttemptAction>, attempt_actions: BTreeMap<u32, AttemptAction>) -> Vec<BuildOperation> {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState};
        let build = BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: topology.clone(),
            submitted_change: Some(reconcile(vec![])),
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 1,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Requested,
            action,
            attempt_actions,
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        plan_for_roll(&build, o, recorded).operations
    }

    fn receipt(attempt: u32, operation: &str, step: &str, output: Option<serde_json::Value>) -> crate::build_state::BuildStepReceipt {
        crate::build_state::BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: operation.into(),
            step: step.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output,
            executor: None,
        }
    }

    fn deleted_receipt(attempt: u32, name: &str, b: &crate::build::ExitedBirth) -> crate::build_state::BuildStepReceipt {
        let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("node.delete:{name}"), node_id: b.node_id.clone(), incarnation: b.incarnation.clone(), name: name.parse().unwrap(), event_at_rafka_ms: 1 };
        receipt(attempt, &format!("node.delete:{name}"), "NodeDeleted", Some(serde_json::to_value(op).unwrap()))
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 2: "A live node whose row already records its
    /// entry's config and executable is not deleted, created or restarted because another node changes. A
    /// request that changes only the gateways' executables redeploys the gateways and leaves every broker,
    /// compute and admin running as it was, and an attempt interrupted halfway finishes the remaining
    /// gateways the same way"; rule 5's order, `plan_for_roll`'s rules reused: a type's non-primary members before
    /// its primary): a two-mesh fabric whose request changes only the gateways' executables plans exactly the
    /// gateways' replacements, mesh by mesh, each gateway type's non-primary first: node.delete of the birth
    /// the view holds, then node.create at its path over it, on the entry's executable. Resumed after the
    /// first gateway's create is receipted, the next attempt owes the rest and never that gateway again.
    /// A Build whose change plans no replacement (a count resize) replaces nothing whatever its entries
    /// name. Must NOT happen: a broker, compute or admin replaced, or a gateway replaced twice.
    #[test]
    fn only_the_paths_whose_entry_differs_from_their_row_are_replaced_and_a_resume_finishes_the_rest() {
        use NodeStatus::ReadyForTraffic as R;
        let mesh = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::Broker, 1), (NodeKind::Gateway, 2), (NodeKind::Compute, 1)]);
        let cur = FabricTopology { fabric: "fabric1".into(), meshes: ["mesh1", "mesh2"].iter().map(|m| (m.to_string(), MeshTopology::of(&mesh(m)))).collect(), bindings: None };
        let mut nodes = Vec::new();
        for m in ["mesh1", "mesh2"] {
            nodes.extend([n(&format!("{m}.admin.1"), R, true), n(&format!("{m}.admin.2"), R, false), n(&format!("{m}.broker.1"), R, true), n(&format!("{m}.gateway.1"), R, true), n(&format!("{m}.gateway.2"), R, false), n(&format!("{m}.compute.1"), R, true)]);
        }
        let o = observed(nodes, &["mesh1", "mesh2"]);
        let rows = rows_on(&o, "a");
        let request: Vec<(PathName, PathConfig)> = o.nodes.iter().map(|x| entry(&x.name.to_string(), Some(if x.name.kind == NodeKind::Gateway { "b" } else { "a" }))).collect();
        let next = compile_against(&cur, &reconcile(request), &o, &rows).unwrap();
        assert_eq!(next.meshes.iter().map(|(k, m)| (k.clone(), paths(m))).collect::<Vec<_>>(), cur.meshes.iter().map(|(k, m)| (k.clone(), paths(m))).collect::<Vec<_>>(), "the paths are the same");
        let birth = |name: &str| o.node(&name.parse().unwrap()).map(|x| crate::build::ExitedBirth { node_id: x.node_id.clone(), incarnation: x.incarnation_id.clone().unwrap() }).unwrap();
        let replace = |name: &str| {
            let node: PathName = name.parse().unwrap();
            vec![BuildOperation::RetireNode { node: node.clone(), exited: None }, BuildOperation::CreateNode { node, over: Some(birth(name)), executable: Some(exe("b", NodeKind::Gateway)) }]
        };
        let all: Vec<BuildOperation> = ["mesh1.gateway.2", "mesh1.gateway.1", "mesh2.gateway.2", "mesh2.gateway.1"].iter().flat_map(|p| replace(p)).collect();
        assert_eq!(replacement_plan(&next, &o, &rows, vec![]), all, "the gateways alone, each type's non-primary first");
        assert_eq!(plan(&next, &o, None).operations, vec![], "a Build whose change plans no replacement replaces nothing");
        assert_eq!(off_the_set(&next, &rows).iter().map(ToString::to_string).collect::<Vec<_>>(), ["mesh1.gateway.1", "mesh1.gateway.2", "mesh2.gateway.1", "mesh2.gateway.2"], "the fabric is ready-for-traffic again only once every gateway's row records its entry's");
        // Interrupted after mesh2.gateway.2's successor was created: the rest, never it again.
        let mut after = o.clone();
        let gone = birth("mesh2.gateway.2");
        let mut successor = n("mesh2.gateway.2", R, false);
        successor.node_id = NodeId::mint();
        after.nodes.retain(|x| x.name.to_string() != "mesh2.gateway.2");
        after.nodes.push(successor.clone());
        let mut on = rows.clone();
        on.insert("mesh2.gateway.2".parse().unwrap(), RecordedRow { node_id: successor.node_id.clone(), sha256: Some("b-gateway".into()) });
        let done = vec![deleted_receipt(1, "mesh2.gateway.2", &gone), receipt(1, "node.delete:mesh2.gateway.2", "Complete", None), receipt(1, "node.create:mesh2.gateway.2", "Complete", None)];
        let left: Vec<BuildOperation> = ["mesh1.gateway.2", "mesh1.gateway.1", "mesh2.gateway.1"].iter().flat_map(|p| replace(p)).collect();
        assert_eq!(replacement_plan(&next, &after, &on, done.clone()), left, "the next attempt finishes the rest");
        assert_eq!(replacement_plan(&next, &after, &rows, done), left, "a gateway this Build created is never replaced again, whatever a stale row read says");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 5: "A path whose birth is under node recovery
    /// is replaced after that recovery's attempt completes, never in the same attempt"; rule 6: the recovery's
    /// create "names no executable on its step, so it takes the row's ..., never the topology's next entry:
    /// a heal never silently upgrades; a pending replacement follows as its own step"): mesh1.rpc.1's entry
    /// names executable b, its exited birth's row a. The attempt carrying its recovery plans node.delete of
    /// the exited birth then node.create naming no executable (the row's), nothing else. Once that attempt
    /// completed its create, the next attempt (no action) plans the replacement of the recovered birth on b:
    /// the recovery's create replaced nothing. Must NOT happen: the recovery launching the entry's executable,
    /// or the replacement skipped because a create of the path is receipted.
    #[test]
    fn a_recovery_runs_on_the_rows_executable_and_the_replacement_is_the_next_attempts() {
        use NodeStatus::ReadyForTraffic as R;
        let cur = t(&[("mesh1", 2, 2)]);
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.admin.2", R, false), n("mesh1.rpc.1", R, false), n("mesh1.rpc.2", R, true)], &["mesh1"]);
        let rows = rows_on(&o, "a");
        let request: Vec<(PathName, PathConfig)> = o.nodes.iter().map(|x| entry(&x.name.to_string(), Some(if x.name.to_string() == "mesh1.rpc.1" { "b" } else { "a" }))).collect();
        let next = compile_against(&cur, &reconcile(request), &o, &rows).unwrap();
        let path: PathName = "mesh1.rpc.1".parse().unwrap();
        let exited = crate::build::ExitedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let recover = AttemptAction::Recover { path: path.clone(), node_id: exited.node_id.clone(), from_incarnation: exited.incarnation.clone(), backbone: false };
        let mut lost = o.clone();
        lost.nodes.retain(|x| x.name != path);
        assert_eq!(
            replacement_build_plan(&next, &lost, &rows, vec![], Some(recover.clone()), [(2, recover.clone())].into()),
            vec![BuildOperation::RetireNode { node: path.clone(), exited: Some(exited.clone()) }, BuildOperation::CreateNode { node: path.clone(), over: Some(exited.clone()), executable: None }],
            "the recovery alone, on the row's executable"
        );
        // Recovered by attempt 2 on a: the next attempt replaces it on b.
        let recovered = o.node(&path).unwrap();
        let steps = vec![deleted_receipt(2, "mesh1.rpc.1", &exited), receipt(2, "node.delete:mesh1.rpc.1", "Complete", None), receipt(2, "node.create:mesh1.rpc.1", "Complete", None)];
        let birth = crate::build::ExitedBirth { node_id: recovered.node_id.clone(), incarnation: recovered.incarnation_id.clone().unwrap() };
        assert_eq!(
            replacement_build_plan(&next, &o, &rows, steps, None, [(2, recover)].into()),
            vec![BuildOperation::RetireNode { node: path.clone(), exited: None }, BuildOperation::CreateNode { node: path, over: Some(birth), executable: Some(exe("b", NodeKind::RpcNode)) }],
            "the replacement follows as the next attempt's"
        );
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: "a Build decoded from the journal whose
    /// paths name none ... is valid history with its births' rows as the record, and an admin restarted over
    /// an old store reads its current Build as it is, never as malformed"; durable format moved): a topology
    /// written before `PathConfig.executable` (its path configs carrying no such field) decodes with none,
    /// validates, and plans no replacement; a complete-topology request compiles over it. The new field
    /// crosses, unchanged, through the journal's JSON (the Build topic carries no executable, rule 7:
    /// `gossip_frames_wire`).
    #[test]
    fn a_topology_written_before_the_executable_field_is_valid_history() {
        let old = serde_json::json!({
            "fabric": "fabric1",
            "meshes": {"mesh1": {"name": "mesh1", "mesh_id": "meshd0000001", "nodes": ["mesh1.admin.1", "mesh1.rpc.1"], "path_config_by_path": {
                "mesh1.admin.1": {"storage": {"persistence": "persistent", "on_retire": "preserve"}, "placement": {}},
                "mesh1.rpc.1": {"storage": {"persistence": "ephemeral"}, "placement": {}}
            }}}
        });
        let decoded: FabricTopology = serde_json::from_value(old).expect("a topology written before the field decodes");
        assert!(decoded.meshes["mesh1"].path_config_by_path.values().all(|c| c.executable.is_none()), "with none");
        assert_eq!(decoded.validate(), Ok(()));
        assert_eq!(off_the_set(&decoded, &Recorded::new()), Vec::<PathName>::new(), "no replacement is owed from it");
        let next = compile(&decoded, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.rpc.1", None), entry("mesh1.rpc.2", Some("b"))]), &mn()).unwrap();
        assert_eq!(paths(next.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.rpc.1", "mesh1.rpc.2"]);
        let json = serde_json::to_value(&next).unwrap();
        assert_eq!(serde_json::from_value::<FabricTopology>(json).unwrap(), next, "the journal's shape keeps it");
        let change = reconcile(vec![entry("mesh1.admin.1", Some("b")), entry("mesh1.rpc.1", None), entry("mesh1.rpc.2", Some("b"))]);
        let json = serde_json::to_value(&change).unwrap();
        assert_eq!(serde_json::from_value::<TopologyChange>(json).unwrap(), change, "the journal keeps the change's executables");
    }
}
