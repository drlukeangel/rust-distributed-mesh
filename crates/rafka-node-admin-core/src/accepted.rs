//! The accepted topology: what a Build carries (RDM #47).
//!
//! `Fabric.build_id` names one Build, and that Build holds the complete Fabric topology as exact
//! node `path.name`s per Mesh ([`FabricTopology`]). A submitted change ([`TopologyChange`]) is
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
use crate::build_state::{BuildProjection, BuildStateAdapter, BuildStateError};
use crate::fabric_storage::{FabricRecord, FabricStorage, FabricStorageError};
use crate::model::{is_valid_mesh_name, IncarnationId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// One Mesh of the accepted topology: every node it should have, by `path.name`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshTopology {
    pub name: String,
    pub nodes: BTreeSet<PathName>,
    /// The typed desired-state meta of every materialized path, explicit after ingress
    /// normalization: one entry per node, no entry without a node (`validate`).
    #[serde(default)]
    pub node_meta_by_path: BTreeMap<PathName, NodeMeta>,
}

impl MeshTopology {
    /// A mesh from its counts: every path minted with its kind's migration default meta (ingress
    /// normalization; the Build is explicit from here).
    pub fn of(desired: &MeshDesired) -> Self {
        let mut nodes = BTreeSet::new();
        let mut node_meta_by_path = BTreeMap::new();
        for (kind, want) in desired.counts() {
            for ord in 1..=want {
                let path = PathName::new(&desired.name, kind, ord);
                node_meta_by_path.insert(path.clone(), NodeMeta::default_for(kind));
                nodes.insert(path);
            }
        }
        Self { name: desired.name.clone(), nodes, node_meta_by_path }
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

    pub fn cohort(&self, kind: NodeKind) -> impl Iterator<Item = &PathName> {
        self.nodes.iter().filter(move |p| p.kind == kind)
    }

    pub fn count(&self, kind: NodeKind) -> u32 {
        self.cohort(kind).count() as u32
    }
}

/// The complete accepted Fabric topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricTopology {
    pub fabric: String,
    pub meshes: BTreeMap<String, MeshTopology>,
}

impl FabricTopology {
    /// Day 0: the bootstrap admin's own mesh, one node-admin.
    pub fn root(fabric: &str, mesh: &str) -> Self {
        let mut meshes = BTreeMap::new();
        meshes.insert(mesh.to_string(), MeshTopology::of(&MeshDesired::of(mesh, [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 0)])));
        Self { fabric: fabric.into(), meshes }
    }

    pub fn mesh(&self, name: &str) -> Option<&MeshTopology> {
        self.meshes.get(name)
    }

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
    /// live node of a mesh still names the mesh). For fixtures and Day-0 adoption, never planning.
    pub fn of_observed(observed: &Topology) -> Self {
        let mut meshes: BTreeMap<String, MeshTopology> =
            observed.meshes.iter().map(|m| (m.name.clone(), MeshTopology { name: m.name.clone(), nodes: BTreeSet::new(), node_meta_by_path: BTreeMap::new() })).collect();
        for n in observed.nodes.iter().filter(|n| n.status.is_live()) {
            let mesh = meshes.entry(n.mesh.clone()).or_insert_with(|| MeshTopology { name: n.mesh.clone(), nodes: BTreeSet::new(), node_meta_by_path: BTreeMap::new() });
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
    ReconcileFabric { desired: FabricDesired },
    /// One mesh's counts (grow/shrink).
    ReconcileMesh { desired: MeshDesired },
    /// `POST /api/nodes/spawn`: one more node of `node_kind` in `mesh`.
    AddNode { mesh: String, node_kind: NodeKind },
    /// `DELETE /api/nodes/{name}`.
    RemoveNode { node: PathName },
    /// `POST /api/meshes`.
    CreateMesh { desired: MeshDesired },
    /// `DELETE /api/meshes/{id|name}`.
    RemoveMesh { mesh: String },
}

/// What one attempt of a Build does beyond realizing its topology: a same-path restart or
/// replacement, fenced to the birth it acts on. Never part of the topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AttemptAction {
    Restart { path: PathName, from_incarnation: IncarnationId },
    Replace { path: PathName, from_incarnation: IncarnationId },
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
                let mesh = next.meshes.entry(m.name.clone()).or_insert_with(|| MeshTopology { name: m.name.clone(), nodes: BTreeSet::new(), node_meta_by_path: BTreeMap::new() });
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
                Some(n) if !n.status.is_live() => return Err(BuildReject::NodeNotLive { node: node.to_string() }),
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
    let mut ops = Vec::new();
    let mesh_exists = |m: &str| observed.meshes.iter().any(|x| x.name == m);
    for (name, m) in &topology.meshes {
        if !mesh_exists(name) {
            ops.push(BuildOperation::CreateMesh { mesh: name.clone() });
        }
        for p in &m.nodes {
            // A path whose birth the view holds live is satisfied; any other path is planned and the
            // create pipeline's fence decides against the world (a running runtime at the path is
            // held, never replaced: `AdminRunner::fence_predecessor`).
            if !observed.node(p).is_some_and(|n| n.status.is_live()) {
                ops.push(BuildOperation::CreateNode { node: p.clone() });
            }
        }
    }
    if let Some(a) = action {
        match a {
            AttemptAction::Restart { path, from_incarnation } => {
                // Done once the path runs a birth other than the one replaced.
                if observed.node(path).is_some_and(|n| n.incarnation_id.as_ref() == Some(from_incarnation) && n.status != NodeStatus::Leaving) {
                    ops.push(BuildOperation::RestartNode { node: path.clone() });
                }
            }
            AttemptAction::Replace { path, from_incarnation } => {
                if observed.node(path).is_some_and(|n| n.incarnation_id.as_ref() == Some(from_incarnation) && n.status.is_live()) {
                    ops.push(BuildOperation::RetireNode { node: path.clone(), permanent: true });
                    ops.push(BuildOperation::CreateNode { node: path.clone() });
                }
            }
        }
    }
    // A live node of a kept mesh that the topology no longer names is retired; a mesh the
    // topology no longer names is retired whole (every member, dead or live, by the retire
    // pipeline), never member by member.
    let mut extra: Vec<&PathName> =
        observed.nodes.iter().filter(|n| n.status.is_live() && topology.meshes.contains_key(&n.mesh) && !topology.contains(&n.name)).map(|n| &n.name).collect();
    extra.sort();
    for p in extra {
        ops.push(BuildOperation::RetireNode { node: p.clone(), permanent: true });
    }
    let mut gone: Vec<&String> = observed.meshes.iter().map(|m| &m.name).filter(|m| !topology.meshes.contains_key(*m)).collect();
    gone.sort();
    for m in gone {
        ops.push(BuildOperation::RetireMesh { mesh: m.clone() });
    }
    BuildPlan { operations: ops }
}

/// `Fabric.build_id` as this admin holds it: the pointer in `fabric.storage`, and the Build it
/// names in `builds.storage`. The fabric-primary moves it on acceptance (Build persisted first);
/// every other admin learns the record on the Build topic and persists its own copy. A learned
/// record is taken only once its Build is held locally and is not older than the one held, so a
/// lagging peer's copy never moves the pointer back.
pub struct AcceptedStore {
    storage: Arc<dyn FabricStorage>,
    /// A learned record whose Build is not held yet.
    wanted: Mutex<Option<FabricRecord>>,
    node: String,
}

impl AcceptedStore {
    pub fn new(storage: Arc<dyn FabricStorage>, node: impl Into<String>) -> Self {
        Self { storage, wanted: Mutex::new(None), node: node.into() }
    }

    pub async fn record(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        self.storage.fabric().await
    }

    pub async fn build_id(&self) -> Option<BuildId> {
        self.storage.fabric().await.ok().flatten().and_then(|r| r.build_id)
    }

    /// The accepted Build: the one the pointer names, as `builds` holds it.
    pub async fn current(&self, builds: &dyn BuildStateAdapter) -> Option<BuildProjection> {
        let id = self.build_id().await?;
        builds.read_build(&id).await.ok()
    }

    /// Move the pointer to `build_id` (its Build is already durable in `builds.storage`), persisting
    /// the record. Returns the record now held.
    pub async fn point(&self, build_id: &BuildId, via: &str) -> Result<FabricRecord, FabricStorageError> {
        let mut record = self.storage.fabric().await?.ok_or_else(|| FabricStorageError::Io { file: "fabric.json".into(), reason: "no Fabric record to point".into() })?;
        let previous = record.build_id.clone();
        record.build_id = Some(build_id.clone());
        self.storage.put_fabric(&record).await?;
        tracing::info_span!(
            "rafka.node_admin.fabric.update.via-build-accepted",
            node = %self.node,
            fabric_id = %record.fabric_id,
            build_id = %build_id,
            previous_build_id = %previous.as_ref().map(|b| b.0.as_str()).unwrap_or(""),
            via,
        )
        .in_scope(|| tracing::info!("Fabric.build_id moved"));
        Ok(record)
    }

    /// A Fabric record heard on the Build topic (`from`). Taken when its Build is held and is not
    /// older than the current one; otherwise remembered until the Build's facts arrive.
    pub async fn learn(&self, record: FabricRecord, builds: &dyn BuildStateAdapter, from: &str) {
        let Some(id) = record.build_id.clone() else { return };
        match self.storage.fabric().await {
            Ok(Some(mine)) if mine.fabric_id != record.fabric_id => return,
            Ok(Some(mine)) if mine.build_id.as_ref() == Some(&id) => return,
            Err(_) => return,
            _ => {}
        }
        let Ok(named) = builds.read_build(&id).await else {
            *self.wanted.lock().unwrap() = Some(record);
            return;
        };
        if let Some(cur) = self.current(builds).await {
            if named.submitted_at_ms < cur.submitted_at_ms {
                return;
            }
        }
        let _ = self.point(&id, &format!("gossip:{from}")).await;
        *self.wanted.lock().unwrap() = None;
    }

    /// Facts arrived: a remembered record whose Build is now held is taken.
    pub async fn resolve_wanted(&self, builds: &dyn BuildStateAdapter) {
        let wanted = self.wanted.lock().unwrap().clone();
        if let Some(r) = wanted {
            self.learn(r, builds, "catch-up").await;
        }
    }
}

impl AcceptedStore {
    /// A store over memory `fabric.storage` whose pointer names a Build of `topology` that `builds`
    /// holds settled (accepted, attempt 1 claimed by `me`, converged). Fixtures: what a Day 0 plus
    /// one converged Build leaves behind.
    pub async fn seeded(builds: &dyn BuildStateAdapter, fabric_id: crate::model::FabricId, topology: FabricTopology, me: &str) -> Result<Arc<Self>, BuildStateError> {
        let storage = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        storage
            .put_fabric(&FabricRecord { fabric_id, name: topology.fabric.clone(), build_id: None })
            .await.map_err(|e| BuildStateError::Io(e.to_string()))?;
        let store = Arc::new(Self::new(storage, me));
        let build_id = BuildId::mint();
        builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, traceparent: None, submitted_at_ms: 0 }).await?;
        builds.claim_attempt(&crate::build_state::BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: me.into() }).await?;
        builds
            .append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { build_id: build_id.clone(), attempt: 1, outcome: crate::build_state::AttemptOutcome::Converged })
            .await?;
        store.point(&build_id, "seeded").await.map_err(|e| BuildStateError::Io(e.to_string()))?;
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

    #[test]
    fn the_plan_is_topology_minus_observed_and_a_satisfied_topology_plans_nothing() {
        let cur = t(&[("mesh1", 2, 3)]);
        assert_eq!(plan(&cur, &mn(), None).operations, vec![]);
        let fresh = plan(&cur, &observed(vec![], &[]), None);
        assert_eq!(fresh.operations[0], BuildOperation::CreateMesh { mesh: "mesh1".into() });
        assert_eq!(fresh.operations.len(), 6);
        // A dead (unheard) birth's path is planned again — the create pipeline's fence then holds a
        // runtime that still runs; a live node outside the topology is retired.
        let mut o = mn();
        o.nodes[3].status = NodeStatus::PendingReconnect;
        o.nodes.push(n("mesh1.rpc.7", NodeStatus::ReadyForTraffic, false));
        assert_eq!(
            plan(&cur, &o, None).operations,
            vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap() }, BuildOperation::RetireNode { node: "mesh1.rpc.7".parse().unwrap(), permanent: true }]
        );
        // A mesh outside the topology is retired after everything else.
        let mut o2 = mn();
        o2.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
        assert_eq!(plan(&cur, &o2, None).operations, vec![BuildOperation::RetireMesh { mesh: "mesh2".into() }]);
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
            vec![BuildOperation::RetireNode { node: path.clone(), permanent: true }, BuildOperation::CreateNode { node: path.clone() }]
        );
        // Once the path runs another birth, the action is satisfied: a re-plan does nothing.
        let mut later = mn();
        later.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&cur, &later, Some(&restart)).operations, vec![]);
        assert_eq!(plan(&cur, &later, Some(&replace)).operations, vec![]);
        // Day 0 roots one admin; a topology without an admin is refused.
        assert_eq!(paths(FabricTopology::root("fabric1", "mesh1").mesh("mesh1").unwrap()), ["mesh1.admin.1"]);
        let mut bad = cur.clone();
        bad.meshes.get_mut("mesh1").unwrap().nodes.retain(|p| p.kind == NodeKind::RpcNode);
        assert_eq!(bad.validate(), Err(BuildReject::MeshWithoutAdmin { mesh: "mesh1".into() }));
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
