//! The accepted topology: what a Build carries (RDM #47).
//!
//! The current Build (the Build entities read last in, first out, R-BI3:
//! [`crate::build_state::BuildStateAdapter::current_build`]) holds the complete Fabric topology as
//! exact node `path.name`s per Mesh ([`FabricTopology`]). A submitted change ([`TopologyChange`]) is
//! input only: [`compile`] applies it once to the current accepted topology and yields the next
//! complete map; the change itself is kept on the Build as history, never read to plan. Planning
//! is always `topology − observed` ([`plan`]). A restart or replacement changes no topology: it is
//! an attempt of the current Build carrying its [`AttemptAction`].
//!
//! Counts (`FabricDesired`, `MeshDesired`) stay API convenience: the compiler resolves them to
//! exact paths before anything is persisted, so the executor never decides which node a count
//! means.

use rafka_mesh_entity::meta::NodeMeta;
use crate::build::{BuildId, BuildOperation, BuildPlan, BuildReject, FabricDesired, MeshDesired};
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
    /// The typed desired-state meta of every materialized path, explicit after ingress
    /// normalization: one entry per node, no entry without a node (`validate`).
    #[serde(default)]
    pub node_meta_by_path: BTreeMap<PathName, NodeMeta>,
}

impl MeshTopology {
    /// A new mesh from its counts, entering the accepted topology: its id minted, and every path
    /// minted with its kind's migration default meta (ingress normalization; the Build is explicit
    /// from here).
    pub fn of(desired: &MeshDesired) -> Self {
        Self::with_id(desired, MeshId::mint())
    }

    /// The mesh `mesh_id` from its counts: every path minted with its kind's migration default meta.
    fn with_id(desired: &MeshDesired, mesh_id: MeshId) -> Self {
        let mut nodes = BTreeSet::new();
        let mut node_meta_by_path = BTreeMap::new();
        for (kind, want) in desired.counts() {
            for ord in 1..=want {
                let path = PathName::new(&desired.name, kind, ord);
                node_meta_by_path.insert(path.clone(), NodeMeta::default_for(kind));
                nodes.insert(path);
            }
        }
        Self { name: desired.name.clone(), mesh_id, nodes, node_meta_by_path }
    }
    /// The meta of one of this mesh's paths; `None` for a path this mesh does not hold.
    pub fn meta(&self, path: &PathName) -> Option<&NodeMeta> {
        self.node_meta_by_path.get(path)
    }
    /// Set one path's meta explicitly (a build request that carries it); refused by name for a
    /// path the mesh does not hold.
    pub fn set_meta(&mut self, path: &PathName, meta: NodeMeta) -> Result<(), BuildReject> {
        if !self.nodes.contains(path) {
            return Err(BuildReject::UnknownNode { node: path.to_string() });
        }
        self.node_meta_by_path.insert(path.clone(), meta);
        Ok(())
    }

    /// This mesh's shape as a desired mesh: every kind's count.
    pub fn desired(&self) -> MeshDesired {
        MeshDesired::of(self.name.clone(), NodeKind::ALL.map(|k| (k, self.count(k))))
    }

    /// The nodes of `kind`.
    pub fn cohort(&self, kind: NodeKind) -> impl Iterator<Item = &PathName> {
        self.nodes.iter().filter(move |p| p.kind == kind)
    }

    /// The number of nodes of `kind`.
    pub fn count(&self, kind: NodeKind) -> u32 {
        self.cohort(kind).count() as u32
    }
}

/// The complete accepted Fabric topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricTopology {
    /// The fabric's name.
    pub fabric: String,
    /// The accepted meshes, by name.
    pub meshes: BTreeMap<String, MeshTopology>,
}

impl FabricTopology {
    /// Day 0: the bootstrap admin's own mesh `mesh_id`, one node-admin. The mesh's id is the one the
    /// bootstrap admin was born with, never minted here.
    pub fn root(fabric: &str, mesh: &str, mesh_id: MeshId) -> Self {
        let mut meshes = BTreeMap::new();
        meshes.insert(mesh.to_string(), MeshTopology::with_id(&MeshDesired::of(mesh, [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 0)]), mesh_id));
        Self { fabric: fabric.into(), meshes }
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
            let missing: Vec<String> = m.nodes.iter().filter(|p| !m.node_meta_by_path.contains_key(p)).map(ToString::to_string).collect();
            let extra: Vec<String> = m.node_meta_by_path.keys().filter(|p| !m.nodes.contains(p)).map(ToString::to_string).collect();
            if !missing.is_empty() || !extra.is_empty() {
                return Err(BuildReject::NodeMetaMismatch { mesh: name.clone(), missing, extra });
            }
        }
        Ok(())
    }

    /// The topology an observed view realizes: every live node's path, per mesh (a view with no
    /// live node of a mesh still names the mesh), each mesh under the id the view holds for it. For
    /// fixtures and Day-0 adoption, never planning: a mesh whose id no member of the view has stated
    /// takes a fresh id, as a fixture's new mesh does.
    pub fn of_observed(observed: &Topology) -> Self {
        let empty = |name: &str, mesh_id: Option<MeshId>| MeshTopology { name: name.to_string(), mesh_id: mesh_id.unwrap_or_else(MeshId::mint), nodes: BTreeSet::new(), node_meta_by_path: BTreeMap::new() };
        let mut meshes: BTreeMap<String, MeshTopology> = observed.meshes.iter().map(|m| (m.name.clone(), empty(&m.name, m.id.clone()))).collect();
        for n in observed.members().filter(|n| n.status.is_live()) {
            let mesh = meshes.entry(n.mesh.clone()).or_insert_with(|| empty(&n.mesh, None));
            mesh.node_meta_by_path.insert(n.name.clone(), NodeMeta::default_for(n.name.kind));
            mesh.nodes.insert(n.name.clone());
        }
        Self { fabric: observed.fabric.name.clone(), meshes }
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
}

/// What one attempt of a Build does beyond realizing its topology: a same-path restart or
/// replacement, fenced to the birth it acts on. Never part of the topology.
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
    /// Replace the birth at a path with a new node.
    Replace {
        /// The node.
        path: PathName,
        /// The birth replaced.
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
    },
    /// Delete the parked exact birth a replacement left at `<path>.old` (node-decommission.md): its
    /// stop steps are done; the provider's exact Exited is the proof, then `NodeDeleted` and its
    /// Deleted row. The topology never names a `.old` path: this action is its only door.
    Delete {
        /// The node's `.old` path.
        path: PathName,
        /// The birth deleted.
        from_incarnation: IncarnationId,
    },
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
    observed.cohort_primary(mesh, kind).map(|n| n.name.ordinal)
}

/// Bring `mesh`'s cohorts to `counts`: grow fills the lowest free ordinals, shrink drops the
/// highest ordinals that are not the observed primary.
fn resize(mesh: &mut MeshTopology, counts: &MeshDesired, observed: &Topology) {
    for (kind, want) in counts.counts() {
        let mut have: Vec<u32> = mesh.cohort(kind).map(|p| p.ordinal).collect();
        have.sort_unstable();
        let keep_ord = primary_ordinal(observed, &mesh.name, kind);
        while (have.len() as u32) < want {
            let ord = (1..).find(|o| !have.contains(o)).expect("a free ordinal");
            have.push(ord);
            have.sort_unstable();
            let path = PathName::new(&mesh.name, kind, ord);
            mesh.node_meta_by_path.insert(path.clone(), NodeMeta::default_for(kind));
            mesh.nodes.insert(path);
        }
        while (have.len() as u32) > want {
            let drop = have.iter().rev().copied().find(|o| Some(*o) != keep_ord).or_else(|| have.last().copied()).expect("a member to drop");
            have.retain(|o| *o != drop);
            let path = PathName::new(&mesh.name, kind, drop);
            mesh.node_meta_by_path.remove(&path);
            mesh.nodes.remove(&path);
        }
    }
}

/// Apply `change` once to `current`, against `observed` (which decides only what a count means:
/// the primary a shrink keeps). The result is the complete next topology, or a refusal by name.
pub fn compile(current: &FabricTopology, change: &TopologyChange, observed: &Topology) -> Result<FabricTopology, BuildReject> {
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
                let mesh = next.meshes.entry(m.name.clone()).or_insert_with(|| MeshTopology { name: m.name.clone(), mesh_id: MeshId::mint(), nodes: BTreeSet::new(), node_meta_by_path: BTreeMap::new() });
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
            // by name rather than accepting a Build it would close with nothing to do.
            match observed.node(node) {
                None => return Err(BuildReject::UnknownNode { node: node.to_string() }),
                Some(n) if !n.status.is_live() && !n.parked => return Err(BuildReject::NodeNotLive { node: node.to_string() }),
                Some(_) => {}
            }
            if node.kind == NodeKind::NodeAdmin && m.count(NodeKind::NodeAdmin) <= 1 {
                return Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh: node.mesh.clone() });
            }
            m.nodes.remove(node);
            m.node_meta_by_path.remove(node);
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
    }
    next.validate()?;
    Ok(next)
}

/// Plan `topology − observed`, plus `action` where it still applies. Deterministic, idempotent:
/// what observed reality already satisfies is not planned. Creates come before retirements, meshes
/// in name order, nodes in path order.
pub fn plan(topology: &FabricTopology, observed: &Topology, action: Option<&AttemptAction>) -> BuildPlan {
    plan_with(topology, observed, action, false, ReplaceProgress::default(), &BTreeSet::new())
}

/// [`plan`] for an attempt of the Build's `reason`: a requested replacement (`Replace`) of a birth
/// the view does not hold at all is still a replacement of that exact birth, which the operation
/// resolves from the durable records.
#[cfg(test)]
pub(crate) fn plan_for(topology: &FabricTopology, observed: &Topology, action: Option<&AttemptAction>, reason: crate::build_state::AttemptReason) -> BuildPlan {
    plan_with(topology, observed, action, reason == crate::build_state::AttemptReason::Replace, ReplaceProgress::default(), &BTreeSet::new())
}

/// How far the `replace-node:<path>` operation of a Replace attempt's exact birth got, from the
/// Build's receipts. The view alone cannot say: once the predecessor is parked it leaves every book,
/// and once the successor holds the path the view names another birth.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReplaceProgress {
    /// The operation began for this birth: a replace's `StopNode`, or a receipt naming the exact
    /// birth (a recovery's `NodeDeleting`, the `RenamePredecessor` of either).
    started: bool,
    /// The operation's `Complete` is receipted, at or after the attempt it began in.
    finished: bool,
    /// How far a recovery's two operations got for its exited birth.
    recovery: RecoveryProgress,
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

impl ReplaceProgress {
    /// The progress of `build`'s Replace action, `default` for any other.
    pub(crate) fn of(build: &crate::build_state::BuildProjection) -> Self {
        // A standalone drain, stop, start or parked delete: the receipts of its operation in an
        // attempt that carried this exact action (its `Opened` fact fenced the attempt to the birth)
        // are this birth's. Its first step's receipt says the operation began; its `Complete`, that
        // it completed for the birth.
        if let Some(action @ (AttemptAction::DeleteConnections { path, .. } | AttemptAction::Stop { path, .. } | AttemptAction::Start { path, .. } | AttemptAction::Delete { path, .. })) = &build.action {
            let (operation, first) = match action {
                AttemptAction::DeleteConnections { .. } => (format!("node.connections.delete:{path}"), "DeleteConnections"),
                AttemptAction::Start { .. } => (format!("start-node:{path}"), "StartNode"),
                AttemptAction::Delete { .. } if path.old => (format!("delete-node:{path}"), "NodeDeleting"),
                // The last admin of a deleted mesh: the ordinary node.delete (`plan_with`).
                AttemptAction::Delete { .. } => (format!("retire-node:{path}"), "NodeDeleting"),
                _ => (format!("stop-node:{path}"), "StopNode"),
            };
            let done = |step: &str| build.steps.iter().any(|s| s.operation == operation && s.step == step && s.outcome == crate::build_state::StepOutcome::Complete && build.carried(s.attempt, action));
            return Self { started: done(first), finished: done("Complete"), ..Self::default() };
        }
        // A recovery: how far its two operations got for the exited birth.
        if let Some(AttemptAction::Recover { path, node_id, from_incarnation }) = &build.action {
            let recovery = recovery_progress(build, path, node_id, from_incarnation).0;
            return Self { started: false, finished: recovery == RecoveryProgress::Recovered, recovery };
        }
        let Some(AttemptAction::Replace { path, from_incarnation }) = &build.action else { return Self::default() };
        let operation = format!("replace-node:{path}");
        let birth = serde_json::to_value(from_incarnation).unwrap_or_default();
        let done = |step: &str, only_birth: bool| {
            build
                .steps
                .iter()
                .filter(|s| s.operation == operation && s.step == step && s.outcome == crate::build_state::StepOutcome::Complete)
                .filter(|s| !only_birth || s.output.as_ref().and_then(|o| o.get("incarnation")) == Some(&birth))
                .map(|s| s.attempt)
                .min()
        };
        let began = [done("StopNode", false), done("NodeDeleting", true), done("RenamePredecessor", true)].into_iter().flatten().min();
        let finished = began.is_some_and(|began| build.steps.iter().any(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && s.attempt >= began));
        Self { started: began.is_some(), finished, ..Self::default() }
    }
}

/// How far the recovery of the exited birth `(node_id, incarnation)` at `path` got in `build`
/// (node-recovery.md "Steps"), with the attempt that completed its last done operation. The delete
/// is done once its `retire-node:<path>` `NodeDeleted` names the exited birth (its output is the
/// node.delete's lifecycle operation) and the operation's `Complete` follows at that attempt or later
/// (a rerun reuses the `NodeDeleted`); the create, and with it the recovery, once a
/// `create-node:<path>` `Complete` follows that. A `Complete` from before the deletion (the path's
/// original create, an earlier recovery's) is not this recovery's.
pub(crate) fn recovery_progress(build: &crate::build_state::BuildProjection, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> (RecoveryProgress, Option<u32>) {
    let delete = format!("retire-node:{path}");
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
    match completed_from(&format!("create-node:{path}"), deleted) {
        Some(created) => (RecoveryProgress::Recovered, Some(created)),
        None => (RecoveryProgress::Deleted, Some(deleted)),
    }
}

/// The attempt whose `create-node:<path>`, the last operation of the recovery of the exited birth
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
pub(crate) fn plan_for_build(build: &crate::build_state::BuildProjection, observed: &Topology) -> BuildPlan {
    plan_with(&build.topology, observed, build.action.as_ref(), build.reason == crate::build_state::AttemptReason::Replace, ReplaceProgress::of(build), &open_mesh_deletes(build, observed))
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

fn plan_with(topology: &FabricTopology, observed: &Topology, action: Option<&AttemptAction>, decommission_unheard: bool, replace_progress: ReplaceProgress, open_deletes: &BTreeSet<String>) -> BuildPlan {
    let mut ops = Vec::new();
    // A standalone drain or stop is its one operation and nothing else: the topology is unchanged,
    // and the path it acts on is neither created nor repaired by this attempt. It is planned while
    // the view holds the exact birth, or while its receipts say it began (a stop's view drops the
    // Leaving birth), and not once its `Complete` receipt names the birth.
    if let Some(
        AttemptAction::DeleteConnections { path, from_incarnation } | AttemptAction::Stop { path, from_incarnation } | AttemptAction::Start { path, from_incarnation } | AttemptAction::Delete { path, from_incarnation },
    ) = action
    {
        // A delete of a parked `<path>.old` is admitted from the record the fabric-primary holds of
        // that exact birth, never from a view (only its replace's executor's view names the path):
        // it is planned until its `Complete` names the birth. A delete of the last admin of a deleted
        // mesh (mesh-delete.md step 7) is the ordinary node.delete of that exact birth, planned while
        // the view holds it.
        let parked_old = matches!(action, Some(AttemptAction::Delete { .. })) && path.old;
        let held = parked_old || observed.node(path).is_some_and(|n| n.incarnation_id.as_ref() == Some(from_incarnation));
        if !replace_progress.finished && (held || replace_progress.started) {
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
    if let Some(AttemptAction::Recover { path, node_id, from_incarnation }) = action {
        let exited = crate::build::ExitedBirth { node_id: node_id.clone(), incarnation: from_incarnation.clone() };
        let done = replace_progress.recovery;
        if done < RecoveryProgress::Deleted {
            ops.push(BuildOperation::RetireNode { node: path.clone(), exited: Some(exited.clone()) });
        }
        if done < RecoveryProgress::Recovered {
            ops.push(BuildOperation::CreateNode { node: path.clone(), over: Some(exited) });
        }
        return BuildPlan { operations: ops };
    }
    let mesh_exists = |m: &str| observed.meshes.iter().any(|x| x.name == m);
    // A restart's birth: the one the attempt is fenced to, when the view still holds exactly it and
    // it is not already leaving (a leaving birth keeps the path to its departure). The action owns
    // its path: the generic create below never plans it a second time.
    let restarted: Option<&PathName> = match action {
        Some(AttemptAction::Restart { path, from_incarnation }) => observed.node(path).filter(|n| n.incarnation_id.as_ref() == Some(from_incarnation) && n.status != NodeStatus::Leaving).map(|_| path),
        _ => None,
    };
    // One replace operation for the exact birth, never a retirement followed by a creation. The
    // operation is planned while the view holds that birth, while its receipts say it began (the
    // view then holds the successor, or nothing, at the path: a parked predecessor has left every
    // book), and for a birth the view never held again that a requested replacement names (resolved
    // from the durable records). It owns its path: the generic create below never plans it a second
    // time.
    let replaced: Option<(&PathName, &IncarnationId)> = match action {
        Some(AttemptAction::Replace { path, from_incarnation }) if !replace_progress.finished => {
            let planned = match observed.node(path) {
                Some(n) => n.incarnation_id.as_ref() == Some(from_incarnation) || replace_progress.started,
                None => (decommission_unheard && topology.contains(path)) || replace_progress.started,
            };
            planned.then_some((path, from_incarnation))
        }
        _ => None,
    };
    for (name, m) in &topology.meshes {
        if !mesh_exists(name) {
            ops.push(BuildOperation::CreateMesh { mesh: name.clone(), mesh_id: m.mesh_id.clone() });
        }
        for p in &m.nodes {
            // A path whose birth the view holds live is satisfied; any other path is planned and the
            // create pipeline's fence decides against the world (a running runtime at the path is
            // held, never replaced: `AdminRunner::fence_predecessor`).
            if restarted == Some(p) || replaced.is_some_and(|(r, _)| r == p) {
                continue;
            }
            // An attempt that carries an action repairs exactly its birth. Another unheard birth is
            // that birth's own proven drift, repaired by its own attempt, never swept up here.
            if let Some(AttemptAction::Restart { path, .. } | AttemptAction::Replace { path, .. }) = action {
                if path != p || observed.node(p).is_some_and(|n| matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead)) {
                    continue;
                }
            }
            if !observed.node(p).is_some_and(|n| n.status.is_live()) {
                ops.push(BuildOperation::CreateNode { node: p.clone(), over: None });
            }
        }
    }
    if let Some(path) = restarted {
        ops.push(BuildOperation::RestartNode { node: path.clone() });
    }
    if let Some((path, from_incarnation)) = replaced {
        ops.push(BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from_incarnation.clone() });
    }
    // A live or stopped (parked) node of a kept mesh that the topology no longer names is retired
    // (a node.delete of a stopped node retires it: node-decommission.md); a node leaving under
    // another operation is that operation's. A mesh the topology no longer names is retired whole
    // (every member, dead or live, by the retire pipeline), never member by member.
    // A surplus birth's retirement is its own attempt (no action): an attempt that carries an action
    // repairs exactly its birth.
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
        let held = self.storage.fabric().await?;
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
        storage
            .claim_identity(&FabricIdentity { fabric_id, name: topology.fabric.clone() })
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
        FabricTopology { fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&MeshDesired::of(*m, [(rafka_mesh_entity::NodeKind::NodeAdmin, *a), (rafka_mesh_entity::NodeKind::RpcNode, *r)])))).collect() }
    }

    fn paths(m: &MeshTopology) -> Vec<String> {
        m.nodes.iter().map(|p| p.to_string()).collect()
    }

    fn counts(name: &str, a: u32, r: u32) -> MeshDesired {
        MeshDesired::of(name.to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, a), (rafka_mesh_entity::NodeKind::RpcNode, r)])
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
            vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None }, BuildOperation::RetireNode { node: "mesh1.rpc.7".parse().unwrap(), exited: None }]
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
            plan_with(&cur, &o, None, false, ReplaceProgress::default(), &open).operations,
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
        let finished = ReplaceProgress { started: true, finished: true, ..ReplaceProgress::default() };
        assert_eq!(plan_with(&cur, &o, Some(&delete), false, finished, &BTreeSet::new()).operations, vec![], "nothing once its Complete names the birth");
        o.nodes.retain(|n| n.name != path);
        assert_eq!(plan(&cur, &o, Some(&delete)).operations, vec![], "nothing once the view no longer holds it");
        let old: PathName = "mesh1.rpc.3.old".parse().unwrap();
        let parked = AttemptAction::Delete { path: old.clone(), from_incarnation: birth.clone() };
        assert_eq!(plan(&cur, &o, Some(&parked)).operations, vec![BuildOperation::DeleteNode { node: old, from_incarnation: birth }]);
    }

    /// CONTRACT (node-decommission.md, fabric-node-lifecycle.md 6.9 Effect): a stopped node (Leaving,
    /// parked, its process alive) that the accepted topology no longer names, the topology a
    /// node.delete of it accepts, is retired: the plan is its one `RetireNode`. A node leaving under
    /// another operation (Leaving, not parked) is that operation's, never retired beside it. Must NOT
    /// happen: a node.delete of a stopped node that converges with nothing planned while its process
    /// lives on.
    #[test]
    fn a_stopped_node_the_topology_no_longer_names_is_retired() {
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
        for status in [NodeStatus::Draining, NodeStatus::Leaving, NodeStatus::Dead, NodeStatus::ReadyForTraffic] {
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
            assert_eq!(plan(&cur, &missing, None).operations, vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None }], "the path itself is missing: {status:?}");
        }
    }

    #[test]
    fn an_attempt_action_is_fenced_to_its_birth_and_never_changes_the_topology() {
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &o, Some(&restart)).operations, vec![BuildOperation::RestartNode { node: path.clone() }]);
        let replace = AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(
            plan(&cur, &o, Some(&replace)).operations,
            vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }],
            "a replace is one operation, never a retire then a create"
        );
        // Once the path runs another birth, the action is satisfied: a re-plan does nothing.
        let mut later = mn();
        later.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&cur, &later, Some(&restart)).operations, vec![]);
        assert_eq!(plan(&cur, &later, Some(&replace)).operations, vec![]);
        // Day 0 roots one admin; a topology without an admin is refused.
        assert_eq!(paths(FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()).mesh("mesh1").unwrap()), ["mesh1.admin.1"]);
        let mut bad = cur.clone();
        bad.meshes.get_mut("mesh1").unwrap().nodes.retain(|p| p.kind == NodeKind::RpcNode);
        assert_eq!(bad.validate(), Err(BuildReject::MeshWithoutAdmin { mesh: "mesh1".into() }));
    }

    /// CONTRACT: an action fenced to a birth the view holds exited plans that path exactly once, as
    /// the action says: a Restart re-creates the SAME identity (no fresh create beside it); a Replace
    /// is the one `replace-node` operation fenced to the exact birth, whatever reason opened it. A
    /// birth already leaving keeps its path to its departure: a restart plans the create, a replace
    /// resumes its operation.
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
            let replace = AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() };
            let replace_op = vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }];
            assert_eq!(plan(&cur, &o, Some(&replace)).operations, replace_op, "{dead:?}");
            for reason in [crate::build_state::AttemptReason::Replace, crate::build_state::AttemptReason::ProvenDrift] {
                assert_eq!(plan_for(&cur, &o, Some(&replace), reason).operations, replace_op, "{dead:?} {reason:?}");
            }
            // Another birth at the path: the action is satisfied, and the unheard birth is not swept
            // up by it (its own proven drift opens its own attempt).
            let other = AttemptAction::Replace { path: path.clone(), from_incarnation: IncarnationId::mint() };
            assert_eq!(plan(&cur, &o, Some(&other)).operations, vec![], "{dead:?}");
            assert_eq!(plan(&cur, &o, None).operations, vec![BuildOperation::CreateNode { node: path.clone(), over: None }], "no action: the generic create decides");
        }
        // A surplus live birth is retired by an attempt of its own, never beside an action.
        let mut surplus = mn();
        surplus.nodes.push(n("mesh1.rpc.7", NodeStatus::ReadyForTraffic, false));
        let from = surplus.node(&path).unwrap().incarnation_id.clone().unwrap();
        let replace = AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &surplus, None).operations, vec![BuildOperation::RetireNode { node: "mesh1.rpc.7".parse().unwrap(), exited: None }]);
        assert_eq!(plan(&cur, &surplus, Some(&replace)).operations, vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }]);
        let mut leaving = mn();
        leaving.nodes.iter_mut().find(|x| x.name == path).unwrap().status = NodeStatus::Leaving;
        let from = leaving.node(&path).unwrap().incarnation_id.clone().unwrap();
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &leaving, Some(&restart)).operations, vec![BuildOperation::CreateNode { node: path.clone(), over: None }]);
        let replace = AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &leaving, Some(&replace)).operations, vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }]);
    }

    /// CONTRACT (node-admin-lifecycle.md 4.2): a replacement of a birth the view does not hold at
    /// all (it is silent past every window the view keeps) is still the one replace operation: the
    /// exact runtime is resolved from the durable records and replaced. The generic create alone would
    /// find the runtime still running and hold the path forever. A view that holds another birth at
    /// the path satisfies the action, and a restart of an absent birth plans nothing (its identity is
    /// not held to re-create).
    #[test]
    fn a_replace_of_a_birth_the_view_lacks_is_still_one_replace_operation() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.3".parse().unwrap();
        let mut o = mn();
        o.nodes.retain(|x| x.name != path);
        let from = IncarnationId::mint();
        let replace = AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() };
        let want = vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan_for(&cur, &o, Some(&replace), crate::build_state::AttemptReason::Replace).operations, want);
        // Proven drift names a birth from a view that has since lost it: nothing to replace, the path
        // is missing and the generic create fills it.
        assert_eq!(plan_for(&cur, &o, Some(&replace), crate::build_state::AttemptReason::ProvenDrift).operations, vec![BuildOperation::CreateNode { node: path.clone(), over: None }]);
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
        let from = IncarnationId::mint();
        let replace = AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() };
        let mut o = mn();
        o.nodes.retain(|x| x.name != path && x.name.to_string() != "mesh1.rpc.2" && x.name.to_string() != "mesh1.admin.2");
        let want = vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan_for(&cur, &o, Some(&replace), crate::build_state::AttemptReason::Replace).operations, want);
        // With no action the generic create still plans every missing member.
        assert!(plan(&cur, &o, None).operations.len() >= 3);
    }

    /// CONTRACT: a standalone node.connections.delete or stop plans its one operation (`node.connections.delete:<path>` /
    /// `stop-node:<path>`) while the view holds the exact birth, never a create, a retire or a repair
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
        assert_eq!(BuildOperation::StopNode { node: path.clone(), from_incarnation: from.clone() }.key(), "stop-node:mesh1.rpc.2");
        // A Draining or Leaving birth of the path is still the birth the operation acts on.
        for status in [NodeStatus::Draining, NodeStatus::Leaving] {
            o.nodes.iter_mut().find(|x| x.name == path).unwrap().status = status;
            assert_eq!(plan(&cur, &o, Some(&stop)).operations.len(), 1, "{status:?}");
        }
        let mut later = mn();
        later.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&cur, &later, Some(&connections)).operations, vec![]);
        assert_eq!(plan(&cur, &later, Some(&stop)).operations, vec![]);
    }

    /// CONTRACT (node-decommission.md): the delete of the parked predecessor at `<path>.old` plans its
    /// one operation `delete-node:<path>.old` until its `Complete` names the birth, whatever the view
    /// holds: the fabric-primary admitted it from the record it holds of that exact birth, and no view
    /// names a `.old` path but its replace's executor's. It never plans a create or repair of the
    /// path's successor.
    #[test]
    fn a_delete_of_a_parked_old_birth_plans_only_its_operation_until_its_complete_names_the_birth() {
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
        assert_eq!(op[0].key(), "delete-node:mesh1.rpc.2.old");
        // The Complete commits no output: the attempt's fenced action names the birth.
        let step = |attempt: u32, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("delete-node:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let other = IncarnationId::mint();
        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
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
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("stop-node:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let other = IncarnationId::mint();
        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
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

    /// CONTRACT: the replace operation is planned until its `Complete` is receipted. Once it began
    /// (a replace's `StopNode`, or a receipt naming the exact birth: a recovery's `NodeDeleting`, the
    /// `RenamePredecessor` of either) it is planned whatever the view holds at the path (the
    /// successor, or nothing: a parked predecessor has left every book). A receipt of another birth of
    /// the path is not this replace's progress.
    #[test]
    fn a_replace_is_planned_until_the_operation_is_complete() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.3".parse().unwrap();
        let from = IncarnationId::mint();
        let step = |attempt: u32, step: &str, birth: Option<&IncarnationId>| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("replace-node:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: birth.map(|b| serde_json::json!({ "incarnation": b })),
            executor: None,
        };
        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 7,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::ProvenDrift,
            action: Some(AttemptAction::Replace { path: path.clone(), from_incarnation: from.clone() }),
            attempt_actions: Default::default(),
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let replace = vec![BuildOperation::ReplaceNode { node: path.clone(), from_incarnation: from.clone() }];
        // The view holds the successor (another birth) at the path.
        let mut o = mn();
        o.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan_for_build(&build(vec![]), &o).operations, vec![], "no receipt of this birth: the path runs another birth, nothing to replace");
        assert_eq!(plan_for_build(&build(vec![step(6, "NodeDeleting", Some(&IncarnationId::mint()))]), &o).operations, vec![], "another birth's receipt is not this replace's");
        assert_eq!(plan_for_build(&build(vec![step(6, "NodeDeleting", Some(&from))]), &o).operations, replace, "begun: the successor holds the path, the departure is owed");
        let deleted = vec![step(6, "NodeDeleting", Some(&from)), step(6, "NodeDeleted", Some(&from))];
        assert_eq!(plan_for_build(&build(deleted.clone()), &o).operations, replace, "NodeDeleted without Complete: the successor's Complete is owed");
        let mut finished = deleted;
        finished.push(step(6, "Complete", None));
        assert_eq!(plan_for_build(&build(finished), &o).operations, vec![], "finished");
        // The view holds nothing at the path.
        let mut gone = mn();
        gone.nodes.retain(|x| x.name != path);
        assert_eq!(plan_for_build(&build(vec![step(6, "NodeDeleting", Some(&from))]), &gone).operations, replace);
        // A replace of a live birth (node-replace.md): it began at its stop, the stopped predecessor
        // left the view, the rename names the birth, and it finishes at Complete with no NodeDeleted.
        assert_eq!(plan_for_build(&build(vec![step(6, "StopNode", None)]), &gone).operations, replace, "begun at the stop: the stopped predecessor left the view");
        assert_eq!(plan_for_build(&build(vec![step(6, "RenamePredecessor", Some(&IncarnationId::mint()))]), &o).operations, vec![], "another birth's rename is not this replace's");
        let renamed = vec![step(6, "StopNode", None), step(6, "RenamePredecessor", Some(&from))];
        assert_eq!(plan_for_build(&build(renamed.clone()), &o).operations, replace, "renamed, the successor not complete");
        let mut complete = renamed;
        complete.push(step(7, "Complete", None));
        assert_eq!(plan_for_build(&build(complete), &o).operations, vec![], "the replace is complete: the predecessor stays parked at <path>.old");
        assert_eq!(plan_for_build(&build(vec![step(5, "Complete", None), step(6, "StopNode", None)]), &o).operations, replace, "a Complete before the operation began is not its end");
    }

    /// CONTRACT (node-recovery.md, Operation summary "Steps"; proposal node-properties-own-commands.md
    /// §5 item 8, §8: "Replace's caller-meta copy step goes with recovery's"; rdm-code-conformance.md
    /// §0.1 and §2 item 4): an admitted recovery is two operations of its exited birth, in the doc's
    /// order and each by the delete and create workflows' own keys: node.delete of the exited instance
    /// (`retire-node:<path>`), then node.create at the same path.name over the inherited data dir
    /// (`create-node:<path>`), whose claim seeds config and tags from the Deleted row. There is no
    /// third step: nothing is copied after the create. It is planned whatever the view holds at the
    /// path (the exited birth unheard, nothing, or the new node live), never as a generic create, and
    /// no other member is swept. Every operation is executed by the exited birth's recoverer: a
    /// member's mesh-primary; a mesh-primary's own recovery by the admin its mesh seats in its place,
    /// never the exited primary the view may still seat. What is left is read from the Build's
    /// receipts: once the delete's `NodeDeleted` names the exited birth and its `Complete` follows,
    /// the create; once the create's `Complete` follows, nothing. Another birth's `NodeDeleted`, a
    /// `NodeDeleted` with no `Complete`, and a `Complete` from before the deletion (the path's original
    /// create) are no progress.
    /// Must NOT happen: one fused `recover-node:<path>` operation, a node.update (`update-node:<path>`)
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
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("{operation}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output,
            executor: None,
        };
        let deleted = |attempt: u32, id: &NodeId, inc: &IncarnationId| {
            let op = rafka_mesh_entity::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("retire-node:{path}"), node_id: id.clone(), incarnation: inc.clone(), name: path.clone(), event_at_rafka_ms: 1 };
            receipt(attempt, "retire-node", "NodeDeleted", Some(serde_json::to_value(op).unwrap()))
        };
        let complete = |attempt: u32, operation: &str| receipt(attempt, operation, "Complete", None);
        let build = |path: &PathName, node_id: &NodeId, steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 4,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Recovery,
            action: Some(AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }),
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
        let all = vec![format!("retire-node:{path}"), format!("create-node:{path}")];
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
        let original_create = complete(1, "create-node");
        assert_eq!(keys(vec![original_create.clone(), deleted(4, &NodeId::mint(), &from), complete(4, "retire-node")], &gone), all, "another node_id's NodeDeleted");
        assert_eq!(keys(vec![deleted(4, &node_id, &IncarnationId::mint()), complete(4, "retire-node")], &gone), all, "another incarnation's NodeDeleted");
        assert_eq!(keys(vec![deleted(4, &node_id, &from)], &gone), all, "NodeDeleted without the delete's Complete: the delete is owed");
        assert_eq!(keys(vec![complete(3, "retire-node"), deleted(4, &node_id, &from)], &gone), all, "an earlier attempt's Complete is not this deletion's");
        let created_left = all[1..].to_vec();
        assert_eq!(keys(vec![original_create.clone(), deleted(4, &node_id, &from), complete(4, "retire-node")], &gone), created_left, "deleted: the create is left, the path's original create is not this one");
        assert_eq!(keys(vec![deleted(4, &node_id, &from), complete(5, "retire-node")], &gone), created_left, "deleted on a rerun that reused the NodeDeleted");
        let recovered = vec![original_create, deleted(4, &node_id, &from), complete(4, "retire-node"), complete(5, "create-node")];
        assert_eq!(keys(recovered.clone(), &mn()), Vec::<String>::new(), "created after the deletion: recovered, nothing is left");
        assert_eq!(keys(recovered, &gone), Vec::<String>::new(), "recovered, whatever the view holds");
        // A mesh-primary's own recovery: the view may still seat the exited primary; every operation
        // is executed by the admin its mesh seats in its place.
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let mut o = mn();
        o.nodes.iter_mut().find(|x| x.name == admin).unwrap().status = NodeStatus::PendingReconnect;
        let ops = plan_for_build(&build(&admin, &node_id, vec![]), &o).operations;
        assert_eq!(ops.iter().map(|op| op.key()).collect::<Vec<_>>(), vec![format!("retire-node:{admin}"), format!("create-node:{admin}")]);
        for op in &ops {
            assert_eq!(crate::executor::executor_for(op, &o).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.2"), "{} of the exited mesh-primary is executed by its mesh's next admin", op.key());
        }
    }

    /// CONTRACT (lock D): a mesh from its counts carries explicit meta for every path, the kind's
    /// migration default; a grow mints meta with the path and a shrink drops it; a path without
    /// meta, or meta without a path, is refused by name; meta set explicitly stands.
    #[test]
    fn every_materialized_path_carries_exactly_one_node_meta() {
        use rafka_mesh_entity::meta::{NodeMeta, PersistentRetireDisposition, StorageMeta};
        let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 2), (NodeKind::Gateway, 1), (NodeKind::RpcNode, 1)]));
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.node_meta_by_path.len(), 5);
        let preserve = StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve };
        assert_eq!(m.meta(&"mesh1.broker.2".parse().unwrap()).unwrap().storage, preserve);
        assert_eq!(m.meta(&"mesh1.admin.1".parse().unwrap()).unwrap().storage, preserve);
        assert_eq!(m.meta(&"mesh1.gateway.1".parse().unwrap()).unwrap().storage, StorageMeta::Ephemeral);
        assert_eq!(m.meta(&"mesh1.rpc.1".parse().unwrap()).unwrap().storage, StorageMeta::Ephemeral);
        let mut topology = FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m.clone())].into_iter().collect() };
        assert_eq!(topology.validate(), Ok(()));
        // Grow and shrink keep the meta in step with the paths.
        let observed = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![],
            nodes: vec![],
        };
        resize(&mut m, &MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 3), (NodeKind::Gateway, 0), (NodeKind::RpcNode, 1)]), &observed);
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.node_meta_by_path.len(), 5);
        assert_eq!(m.meta(&"mesh1.broker.3".parse().unwrap()).unwrap().storage, preserve, "the grown path carries its default");
        assert!(m.meta(&"mesh1.gateway.1".parse().unwrap()).is_none(), "the dropped path's meta goes with it");
        // Explicit meta stands; a path the mesh does not hold is refused.
        let release = NodeMeta { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }, placement: Default::default() };
        assert_eq!(m.set_meta(&"mesh1.rpc.1".parse().unwrap(), release.clone()), Ok(()));
        assert_eq!(m.meta(&"mesh1.rpc.1".parse().unwrap()), Some(&release));
        assert!(matches!(m.set_meta(&"mesh1.rpc.9".parse().unwrap(), release.clone()), Err(BuildReject::UnknownNode { .. })));
        // The consistency ratchet.
        m.node_meta_by_path.remove(&"mesh1.rpc.1".parse().unwrap());
        m.node_meta_by_path.insert("mesh1.compute.7".parse().unwrap(), release);
        topology.meshes.insert("mesh1".into(), m);
        assert_eq!(
            topology.validate(),
            Err(BuildReject::NodeMetaMismatch { mesh: "mesh1".into(), missing: vec!["mesh1.rpc.1".into()], extra: vec!["mesh1.compute.7".into()] })
        );
    }
}
