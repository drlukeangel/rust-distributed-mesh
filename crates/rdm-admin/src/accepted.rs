//! The accepted topology: what a Build carries (RDM #47).
//!
//! The current Build (the Build entities read last in, first out, R-BI3:
//! [`crate::build_state::BuildStateAdapter::current_build`]) holds the complete Fabric topology as
//! exact node `(path, name)`s per Mesh ([`FabricTopology`]). A submitted change ([`BuildChange`]) is
//! input only: [`compile`] applies it once to the current accepted topology and yields the next
//! complete map; the change itself is kept on the Build as it came in, never read to plan. Planning
//! is always `topology − observed` ([`plan`]). A stop, a start or a recovery changes no topology and
//! is no Build: it is a node command, or the mesh-primary's own workflow.
//!
//! A caller names every node it adds or removes: counts are the product's API's vocabulary, expanded
//! into named nodes before the typed call (build.md §1).

use rdm_core::build::BuildChange;
use rdm_core::meta::NodeSpec;
use crate::build::{BuildId, BuildOperation, BuildPlan, BuildReject, FabricDesired, MeshDesired};
use crate::build_state::{BuildStateAdapter, BuildStateError};
use crate::fabric_storage::{FabricIdentity, FabricStorage, FabricStorageError};
use crate::model::{is_valid_mesh_name, IncarnationId, MeshId, NodeId, NodeStatus, PathName};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// One Mesh of the accepted topology: every node it should have, by `(path, name)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshTopology {
    /// The mesh's name.
    pub name: String,
    /// The mesh's id (node-identity-and-state.md §2.7, "Who mints it"): minted once, when the mesh
    /// enters the accepted topology, and carried by every Build after it, never reissued. Every run of
    /// the mesh's `create-mesh` operation, by any executor and any attempt, claims the mesh's name with
    /// it; a mesh removed and created again enters the topology anew with a new id (R-MN1).
    pub mesh_id: MeshId,
    /// Every node the mesh should have, by `(path, name)`.
    pub nodes: BTreeSet<PathName>,
    /// The create spec of every materialized path (Q166: the node's properties, flat), explicit after
    /// ingress normalization: one entry per node, no entry without a node (`validate`). The journal
    /// wrote this map as `path_config_by_path` before Q166, each entry without its `path` and its
    /// `config`: such a record decodes, each spec's path its key and its config empty (durable format
    /// moved).
    #[serde(deserialize_with = "specs_by_path", serialize_with = "write_specs")]
    pub spec_by_path: BTreeMap<PathName, NodeSpec>,
}

/// Journal entries retain the pair as fields rather than object-map keys.
fn specs_by_path<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<PathName, NodeSpec>, D::Error> {
    let mut specs = BTreeMap::new();
    for spec in Vec::<NodeSpec>::deserialize(d)? {
        let path = spec.location();
        if specs.insert(path.clone(), spec).is_some() {
            return Err(serde::de::Error::custom(format!("the accepted entries repeat path {:?}, name {:?}", path.path, path.name)));
        }
    }
    Ok(specs)
}

fn write_specs<S: serde::Serializer>(specs: &BTreeMap<PathName, NodeSpec>, s: S) -> Result<S::Ok, S::Error> {
    specs.values().collect::<Vec<_>>().serialize(s)
}

/// The explicit node-admin storage default written at the Day-0 claim.
pub fn root_storage() -> rdm_core::meta::StoragePersistence {
    rdm_core::meta::StoragePersistence::of_type(rdm_core::NODE_ADMIN)
}

impl MeshTopology {
    /// A new mesh from its counts, entering the accepted topology: its id minted, and every path
    /// minted with its type's default create spec (ingress normalization; the Build is
    /// explicit from here).
    pub fn of(desired: &MeshDesired) -> Self {
        Self::with_id(desired, MeshId::mint())
    }

    /// The mesh `mesh_id` from its counts: every path minted with its type's default create spec.
    pub(crate) fn with_id(desired: &MeshDesired, mesh_id: MeshId) -> Self {
        let mut nodes = BTreeSet::new();
        let mut spec_by_path = BTreeMap::new();
        for (r#type, want) in desired.counts() {
            for ord in 1..=want {
                let path = PathName::numbered(&desired.name, &r#type, ord);
                spec_by_path.insert(path.clone(), NodeSpec::default_for(path.clone(), &desired.name, &r#type));
                nodes.insert(path);
            }
        }
        Self { name: desired.name.clone(), mesh_id, nodes, spec_by_path }
    }
    /// The create spec of one of this mesh's paths; `None` for a path this mesh does not hold.
    pub fn spec(&self, path: &PathName) -> Option<&NodeSpec> {
        self.spec_by_path.get(path)
    }
    /// Set the create spec of the path it names explicitly (a build request that carries it); refused by
    /// name for a path the mesh does not hold.
    pub fn set_spec(&mut self, spec: NodeSpec) -> Result<(), BuildReject> {
        if !self.nodes.contains(&spec.location()) {
            return Err(BuildReject::UnknownNode { path: spec.path.clone(), name: spec.name.clone() });
        }
        if spec.mesh != self.name {
            return Err(BuildReject::NodeSpecMismatch { mesh: self.name.clone(), missing: vec![], extra: vec![crate::build::NodeSpecConflict { path: spec.path.clone(), name: spec.name.clone(), declared_path: spec.path.clone(), declared_name: spec.name.clone(), declared_mesh: spec.mesh.clone() }] });
        }
        self.spec_by_path.insert(spec.location(), spec);
        Ok(())
    }

    /// This mesh's shape as a desired mesh: every type's count.
    pub fn desired(&self) -> MeshDesired {
        MeshDesired::of(self.name.clone(), self.types().into_iter().map(|t| { let n = self.count(&t); (t, n) }))
    }

    /// The types of this mesh's nodes, `node-admin` first, then by name.
    pub fn types(&self) -> Vec<String> {
        let mut all: Vec<String> = self.spec_by_path.values().map(|s| s.r#type.clone()).collect::<BTreeSet<_>>().into_iter().collect();
        all.sort_by_key(|t| !rdm_core::is_node_admin(t));
        all
    }

    /// The nodes of `type`.
    pub fn nodes_of_type(&self, r#type: &str) -> impl Iterator<Item = &PathName> + '_ {
        let r#type = r#type.to_string();
        self.nodes.iter().filter(move |path| self.spec(path).is_some_and(|spec| spec.r#type == r#type))
    }

    /// The number of nodes of `type`.
    pub fn count(&self, r#type: &str) -> u32 {
        self.nodes_of_type(r#type).count() as u32
    }
}

/// The complete accepted Fabric topology. Each path's executable is its entry's
/// ([`NodeSpec::executable`]); the fabric holds no executable of its own (build-deployment.md §2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricTopology {
    /// The fabric's name.
    pub fabric: String,
    /// The accepted meshes, by name.
    pub meshes: BTreeMap<String, MeshTopology>,
    /// The fabric's `topology_storage_provider` at the Build's acceptance (storage.md §2), the Fabric
    /// row's: a Build never changes it. The journal's record of the accepted Build carries it.
    pub topology_storage_provider: Option<String>,
    /// The exact birth each node this Build deletes held when it was accepted (build.md §4.4: "The
    /// operation carries the exact birth the gates pinned"), by path: every node.delete the Build plans
    /// acts on its path's pinned birth and on no other. Set at acceptance, never compiled forward.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pinned: Vec<(PathName, crate::build::PinnedBirth)>,
    /// The node.id the fabric-primary minted for each node this Build creates when it accepted it
    /// (D58), by path and name: each create runs with this node.id and allocates no other. Set at
    /// acceptance, never compiled forward: a Build that creates nothing names none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created: Vec<(PathName, NodeId)>,
}

impl FabricTopology {
    /// Day 0: the bootstrap admin's own mesh `mesh_id`, one node-admin, on RDM's file backend. The
    /// mesh's id is the one the bootstrap admin was born with, never minted here.
    pub fn root(fabric: &str, mesh: &str, mesh_id: MeshId) -> Self {
        Self::root_on(fabric, mesh, mesh_id, rdm_core::launch::TOPOLOGY_STORAGE_FILE)
    }

    /// [`Self::root`] on the `topology_storage_provider` the bootstrap admin was started on
    /// (`RDM_TOPOLOGY_STORAGE_PROVIDER`, storage.md §2 Day 0).
    pub fn root_on(fabric: &str, mesh: &str, mesh_id: MeshId, topology_storage_provider: &str) -> Self {
        let mut meshes = BTreeMap::new();
        meshes.insert(mesh.to_string(), MeshTopology::with_id(&MeshDesired::of(mesh, [("node-admin", 1), ("node", 0)]), mesh_id));
        Self { fabric: fabric.into(), meshes, topology_storage_provider: Some(topology_storage_provider.into()), pinned: Vec::new(), created: Vec::new() }
    }

    /// The node.id this Build minted for the node it creates at `path` ([`Self::created`]); `None`: the
    /// Build creates no node there.
    pub fn created_id(&self, path: &PathName) -> Option<&NodeId> {
        self.created.iter().find(|(at, _)| at == path).map(|(_, id)| id)
    }

    /// The key of this Build's create at `path` (`node.create:<node_id>`), the node.id its record names.
    pub fn create_key(&self, path: &PathName) -> Option<rdm_core::OperationId> {
        self.created_id(path).map(|id| rdm_core::OperationId::node("node.create", id))
    }

    /// Mint a node.id for each path in `paths` this Build does not yet name one for (D58).
    pub fn mint_created<'a>(&mut self, paths: impl IntoIterator<Item = &'a PathName>) {
        for path in paths {
            if self.created_id(path).is_none() {
                self.created.push((path.clone(), NodeId::mint()));
            }
        }
    }

    /// The accepted mesh named `name`.
    pub fn mesh(&self, name: &str) -> Option<&MeshTopology> {
        self.meshes.get(name)
    }

    /// Every mesh the Build names.
    pub fn mesh_names(&self) -> BTreeSet<String> {
        self.meshes.keys().cloned().collect()
    }

    /// This topology with only the nodes at `paths`, every mesh kept.
    pub fn restricted_to(&self, paths: &BTreeSet<PathName>) -> FabricTopology {
        let mut t = self.clone();
        for m in t.meshes.values_mut() {
            m.nodes.retain(|p| paths.contains(p));
            m.spec_by_path.retain(|p, _| paths.contains(p));
        }
        t
    }

    /// Whether the topology names `node`.
    pub fn contains(&self, node: &PathName) -> bool {
        self.node_spec(node).is_some()
    }

    /// The explicit accepted entry at a path/name, independent of its spelling.
    pub fn node_spec(&self, node: &PathName) -> Option<&NodeSpec> {
        self.meshes.values().find_map(|m| m.spec(node))
    }

    /// Every invariant a persisted topology holds.
    pub fn validate(&self) -> Result<(), BuildReject> {
        if self.meshes.is_empty() {
            return Err(BuildReject::EmptyFabric);
        }
        let mut named = BTreeSet::new();
        for (name, m) in &self.meshes {
            if let Some(path) = m.nodes.iter().find(|path| !named.insert((*path).clone())) {
                return Err(BuildReject::NodeNamedTwice { path: path.path.clone(), name: path.name.clone()});
            }
            if name != &m.name {
                return Err(BuildReject::InvalidMeshName { mesh: m.name.clone() });
            }
            if !is_valid_mesh_name(name) {
                return Err(BuildReject::InvalidMeshName { mesh: name.clone() });
            }
            if m.count("node-admin") == 0 {
                return Err(BuildReject::MeshWithoutAdmin { mesh: name.clone() });
            }
            let missing = m.nodes.iter().filter(|p| !m.spec_by_path.contains_key(p)).cloned().collect::<Vec<_>>();
            let extra = m.spec_by_path.iter().filter(|(path, spec)| !m.nodes.contains(*path) || spec.mesh != *name || spec.location() != **path).map(|(path, spec)| crate::build::NodeSpecConflict { path: path.path.clone(), name: path.name.clone(), declared_path: spec.path.clone(), declared_name: spec.name.clone(), declared_mesh: spec.mesh.clone() }).collect::<Vec<_>>();
            if !missing.is_empty() || !extra.is_empty() {
                return Err(BuildReject::NodeSpecMismatch { mesh: name.clone(), missing, extra });
            }
        }
        Ok(())
    }

    /// The topology an observed view realizes: every live node's path, per mesh (a view with no
    /// live node of a mesh still names the mesh), each mesh under the id the view holds for it. For
    /// fixtures and Day-0 adoption, never planning: a mesh whose id no member of the view has stated
    /// takes a fresh id, as a fixture's new mesh does.
    pub fn of_observed(observed: &Topology) -> Self {
        let empty = |name: &str, mesh_id: Option<MeshId>| MeshTopology { name: name.to_string(), mesh_id: mesh_id.unwrap_or_else(MeshId::mint), nodes: BTreeSet::new(), spec_by_path: BTreeMap::new() };
        let mut meshes: BTreeMap<String, MeshTopology> = observed.meshes.iter().map(|m| (m.name.clone(), empty(&m.name, m.id.clone()))).collect();
        for n in observed.births().filter(|n| n.status.is_live()) {
            let mesh = meshes.entry(n.mesh.clone()).or_insert_with(|| empty(&n.mesh, None));
            mesh.spec_by_path.insert(n.location(), NodeSpec::default_for(n.location(), &n.mesh, &n.r#type));
            mesh.nodes.insert(n.location());
        }
        Self { fabric: observed.fabric.name.clone(), meshes, topology_storage_provider: None, pinned: Vec::new(), created: Vec::new() }
    }

    /// The topology the fabric-primary's records hold now (build.md §4: "the fabric-primary composes
    /// the complete topology from its records at acceptance"): every mesh not deleted, and every node
    /// that is the current birth at its path and name, live, stopped or unheard. A deleted mesh is held
    /// while a node of it stands (its last admin, until its own node.delete: mesh-delete.md step 7).
    /// Each mesh takes the id the records hold for it, else the id `accepted` holds for its name; each
    /// node takes the spec `accepted` holds at its path and name when its type and mesh match, else
    /// its type's defaults.
    pub fn of_records(observed: &Topology, accepted: &FabricTopology) -> Self {
        let held_id = |name: &str, id: Option<&MeshId>| id.cloned().or_else(|| accepted.meshes.get(name).map(|m| m.mesh_id.clone())).unwrap_or_else(MeshId::mint);
        let empty = |name: &str, mesh_id: MeshId| MeshTopology { name: name.to_string(), mesh_id, nodes: BTreeSet::new(), spec_by_path: BTreeMap::new() };
        let standing: Vec<&crate::model::Node> = observed.births().filter(|n| observed.node(&n.location()).is_some_and(|current| current.node_id == n.node_id)).collect();
        let mut meshes: BTreeMap<String, MeshTopology> = observed
            .meshes
            .iter()
            .filter(|m| m.status != crate::model::ScopeStatus::Dead || standing.iter().any(|n| n.mesh == m.name))
            .map(|m| (m.name.clone(), empty(&m.name, held_id(&m.name, m.id.as_ref()))))
            .collect();
        for n in standing {
            let mesh = meshes.entry(n.mesh.clone()).or_insert_with(|| empty(&n.mesh, held_id(&n.mesh, None)));
            let at = n.location();
            let spec = accepted.node_spec(&at).filter(|s| s.r#type == n.r#type && s.mesh == n.mesh).cloned().unwrap_or_else(|| NodeSpec::default_for(at.clone(), &n.mesh, &n.r#type));
            mesh.spec_by_path.insert(at.clone(), spec);
            mesh.nodes.insert(at);
        }
        Self { fabric: observed.fabric.name.clone(), meshes, topology_storage_provider: accepted.topology_storage_provider.clone(), pinned: Vec::new(), created: Vec::new() }
    }

    /// The topology as per-mesh counts (the view's convenience shape).
    pub fn desired(&self) -> FabricDesired {
        FabricDesired {
            fabric: self.fabric.clone(),
            meshes: self.meshes.values().map(MeshTopology::desired).collect(),
        }
    }
}

/// The paths and names a [`BuildChange::Create`] adds; `None` for every other change.
pub fn created_paths(change: &BuildChange) -> Option<BTreeSet<PathName>> {
    match change {
        BuildChange::Create { nodes, .. } => Some(nodes.iter().filter_map(|n| PathName::new(&n.path, &n.name).ok()).collect()),
        _ => None,
    }
}

/// Apply `change` once to the topology `observed`, the fabric-primary's records, holds now
/// ([`FabricTopology::of_records`], build.md §4: "the fabric-primary composes the complete topology from
/// its records at acceptance"), `accepted` the current Build's topology, read only for each standing
/// node's spec and each mesh's id the records do not state. The result is the complete next topology,
/// or a refusal by name.
pub fn compile(accepted: &FabricTopology, change: &BuildChange, observed: &Topology) -> Result<FabricTopology, BuildReject> {
    let mut next = FabricTopology::of_records(observed, accepted);
    match change {
        BuildChange::DeleteFabric => {
            next.meshes.clear();
            return Ok(next);
        }
        BuildChange::Create { meshes, nodes } => created(&mut next, meshes, nodes, observed)?,
        BuildChange::Delete { meshes, node_ids } => deleted(&mut next, meshes, node_ids, observed)?,
    }
    if !next.meshes.is_empty() {
        next.validate()?;
    }
    // Each node a Create names is born with a node.id minted now, written in the Build record (D58).
    next.mint_created(&created_paths(change).unwrap_or_default());
    Ok(next)
}

/// The birth holding `node_id` now, the current one at its path and name (build.md §4.4: "A deleted
/// node.id stands"): `None` for a node.id no birth holds, or one a newer birth at its path and name
/// replaced.
pub fn birth_of_node_id<'a>(observed: &'a Topology, node_id: &NodeId) -> Option<&'a crate::model::Node> {
    let n = observed.nodes.iter().find(|n| &n.node_id == node_id)?;
    observed.node(&n.location()).filter(|current| current.node_id == *node_id)
}

/// What a [`BuildChange::Create`] adds to `next` (build.md §1): each mesh under an id minted now,
/// each node whole from its Nodes schema, the type's `storage_persistence` where the node names none.
/// Refused by name: nothing listed, a mesh name outside the grammar, held or named twice, a node outside
/// the name grammar, of an undeclared type, at a path and name already held or named twice, with no
/// executable, tags outside the label grammar, in a mesh neither in the request nor in the topology,
/// and a new mesh with no node-admin among the nodes.
fn created(next: &mut FabricTopology, meshes: &[rdm_core::build::Meshes], nodes: &[rdm_core::build::Nodes], observed: &Topology) -> Result<(), BuildReject> {
    if meshes.is_empty() && nodes.is_empty() {
        return Err(BuildReject::NoNodeListed);
    }
    let mut added = BTreeSet::new();
    for m in meshes {
        if !is_valid_mesh_name(&m.name) {
            return Err(BuildReject::InvalidMeshName { mesh: m.name.clone() });
        }
        if next.meshes.contains_key(&m.name) {
            return Err(BuildReject::MeshAlreadyExists { mesh: m.name.clone() });
        }
        if !added.insert(m.name.clone()) {
            return Err(BuildReject::DuplicateMesh { mesh: m.name.clone() });
        }
        next.meshes.insert(m.name.clone(), MeshTopology { name: m.name.clone(), mesh_id: MeshId::mint(), nodes: BTreeSet::new(), spec_by_path: BTreeMap::new() });
    }
    let mut named = BTreeSet::new();
    for n in nodes {
        let path = PathName::new(&n.path, &n.name).map_err(|e| BuildReject::InvalidNodeName { path: n.path.clone(), name: n.name.clone(), detail: e.to_string() })?;
        // A path and name a node holds now, by the records, is held (build.md §1: the fabric-primary
        // composes the topology from its records); so is one this request names twice.
        if observed.node(&path).is_some() || !named.insert(path.clone()) {
            return Err(BuildReject::NameHeld { path: path.path, name: path.name });
        }
        let executable = n.executable.clone().ok_or_else(|| BuildReject::ExecutableMissing { path: path.path.clone(), name: path.name.clone() })?;
        if let Some(detail) = rdm_core::status::tags_refusal(&n.tags) {
            return Err(BuildReject::TagsRefused { path: path.path, name: path.name, detail });
        }
        let mesh = next.meshes.get_mut(&n.mesh).ok_or_else(|| BuildReject::UnknownMesh { mesh: n.mesh.clone() })?;
        let spec = NodeSpec {
            path: path.path.clone(),
            name: path.name.clone(),
            mesh: n.mesh.clone(),
            r#type: n.r#type.clone(),
            storage_persistence: n.storage_persistence.clone().unwrap_or_else(|| rdm_core::meta::StoragePersistence::of_type(&n.r#type)),
            executable: Some(executable),
            tags: n.tags.clone(),
            config: n.config.clone(),
        };
        mesh.nodes.insert(path.clone());
        mesh.spec_by_path.insert(path, spec);
    }
    if let Some(mesh) = added.iter().find(|m| next.meshes[*m].count("node-admin") == 0) {
        return Err(BuildReject::MeshWithoutAdmin { mesh: mesh.clone() });
    }
    Ok(())
}

/// What a [`BuildChange::Delete`] removes from `next` (build.md §1): each node the birth holding its
/// node.id stands at ([`birth_of_node_id`]), and each mesh by name. The last node of a deleted mesh (its
/// last admin, mesh-delete.md step 7) takes its mesh with it. Refused by name: nothing listed, a node.id
/// no birth holds now, a live mesh's last node-admin, a mesh the fabric does not hold, and the fabric's
/// last mesh.
fn deleted(next: &mut FabricTopology, meshes: &[String], node_ids: &[NodeId], observed: &Topology) -> Result<(), BuildReject> {
    if meshes.is_empty() && node_ids.is_empty() {
        return Err(BuildReject::NoNodeListed);
    }
    for id in node_ids {
        let node = birth_of_node_id(observed, id).map(|b| b.location()).ok_or_else(|| BuildReject::UnknownNodeId { node_id: id.clone() })?;
        let spec = next.node_spec(&node).ok_or_else(|| BuildReject::UnknownNodeId { node_id: id.clone() })?;
        let (mesh, admin) = (spec.mesh.clone(), rdm_core::is_node_admin(&spec.r#type));
        let dead = observed.meshes.iter().any(|m| m.name == mesh && m.status == crate::model::ScopeStatus::Dead);
        let m = next.meshes.get_mut(&mesh).expect("the entry belongs to this accepted mesh");
        if admin && m.count("node-admin") <= 1 && !meshes.contains(&mesh) && !dead {
            return Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh });
        }
        m.nodes.remove(&node);
        m.spec_by_path.remove(&node);
        if dead && m.nodes.is_empty() {
            next.meshes.remove(&mesh);
        }
    }
    for mesh in meshes {
        if next.meshes.remove(mesh).is_none() {
            return Err(BuildReject::UnknownMesh { mesh: mesh.clone() });
        }
    }
    // The fabric-primary, the last node of a deleted fabric, is deleted on its own after fabric.deleted
    // (fabric-delete.md step 4): only a fabric with a live mesh left is refused its last mesh.
    if next.meshes.is_empty() && observed.meshes.iter().any(|m| m.status != crate::model::ScopeStatus::Dead) {
        return Err(BuildReject::EmptyFabric);
    }
    Ok(())
}

/// Plan `topology − observed`. Deterministic, idempotent: what observed reality already satisfies is
/// not planned. Creates come before deletions, meshes in name order, nodes in path order.
pub fn plan(topology: &FabricTopology, observed: &Topology) -> BuildPlan {
    plan_with(topology, observed, &BTreeSet::new(), &BTreeSet::new(), &[], &BTreeSet::new())
}

/// [`plan`] at a Build's acceptance, `held` the peer meshes the fabric-primary holds unheard (R-D1: a peer
/// mesh held unheard is held as a mesh, and its nodes are not restarted, replaced or deleted from here until
/// it is heard again). A birth of one of them under a silence mark satisfies its path: the mark is never
/// itself a reason to act (states.md, "Node row silence marks"), so gate 6 reads only what the request
/// changes there. The drive plans every pass without the hold and stops at such a path by name until the
/// mesh is heard (Q221).
pub fn plan_holding(topology: &FabricTopology, observed: &Topology, held: &BTreeSet<String>) -> BuildPlan {
    plan_with(topology, observed, &BTreeSet::new(), held, &[], &BTreeSet::new())
}

/// The births of a kept mesh `observed` holds live or stopped at a path `topology` no longer names: the
/// nodes a topology Build deletes, each pinned at its acceptance (build.md §4.4).
pub(crate) fn surplus_births<'a>(topology: &'a FabricTopology, observed: &'a Topology) -> impl Iterator<Item = &'a crate::model::Node> + 'a {
    observed.births().filter(move |n| {
        let pinned = topology.pinned.iter().any(|(at, b)| *at == n.location() && b.is(n));
        // A pinned birth is the Build's to delete whatever its mesh: a deleted mesh's last admin leaves
        // with its mesh (mesh-delete.md step 7).
        (n.status.is_live() || n.stopped || pinned) && (topology.meshes.contains_key(&n.mesh) || pinned) && !topology.contains(&n.location())
    })
}

/// [`plan`] for `build`, given what its `rdm.build.step` events say already ran.
pub fn plan_for_build(build: &crate::build_state::BuildProjection, observed: &Topology) -> BuildPlan {
    plan_for_build_created(build, observed, &BTreeSet::new())
}

/// [`plan_for_build`], with `created` the paths the fabric-primary's atlas holds a create of this
/// Build at ([`crate::atlas::Atlas::created_by`]): a birth at one of them is not surplus to the accepted
/// topology (Luke 2026-10-30: drift is decided from the atlas, not the topology). Each create carries
/// its path's entry in the Build's topology ([`crate::build::EntrySpec`]): the executable, tags and
/// config the dispatch does not carry.
pub fn plan_for_build_created(build: &crate::build_state::BuildProjection, observed: &Topology, created: &BTreeSet<PathName>) -> BuildPlan {
    let mut plan = plan_with(&build.topology, observed, &open_mesh_deletes(build, observed), &BTreeSet::new(), &build.steps, created);
    for op in &mut plan.operations {
        if let BuildOperation::CreateNode { path, name, entry, .. } = op {
            *entry = build.topology.node_spec(&PathName { path: path.clone(), name: name.clone() }).map(crate::build::EntrySpec::of);
        }
    }
    plan
}

/// `observed` with every mesh of `rows` (the Mesh rows on the store) it does not hold by id added, for a
/// fabric.delete's plan: the delete deletes every mesh the store holds a row of, never only the meshes the
/// view still projects, which drops a mesh once its members are gone (fabric-delete.md "Failure": "a runtime
/// inside the workflow whose delete has no receipt is never reported deleted"; mesh-delete.md "Resume after
/// the primary's loss": the new executor runs only the tiers without a terminal receipt). A row written deleted
/// is added Dead: the plan resumes it only while its mesh.delete is open in the Build's receipts.
pub fn with_mesh_rows(observed: &Topology, rows: &[crate::storage::MeshRecord]) -> Topology {
    use rdm_core::status::MeshState;
    let mut t = observed.clone();
    // A live row first: of a name the store holds rows of more than one mesh for, the live one is the
    // current mesh, and a Dead row of an earlier mesh of that name is planned only as the one row of it.
    let (live, dead): (Vec<&crate::storage::MeshRecord>, Vec<&crate::storage::MeshRecord>) = rows.iter().partition(|r| !r.is_deleted());
    for r in live.into_iter().chain(dead.into_iter().filter(|d| rows.iter().filter(|r| r.name == d.name).count() == 1)) {
        // A mesh the view holds, by id or by name (a mesh the view names by its name is the current one
        // of that name), is planned as the view holds it.
        if t.meshes.iter().any(|m| m.id.as_ref() == Some(&r.mesh_id) || m.name == r.name) {
            continue;
        }
        let status = match r.state {
            MeshState::Deleted => crate::model::ScopeStatus::Dead,
            MeshState::Deleting => crate::model::ScopeStatus::Leaving,
            MeshState::Ready => crate::model::ScopeStatus::Ready,
            MeshState::Pending => crate::model::ScopeStatus::Pending,
        };
        t.meshes.push(crate::model::Mesh { id: Some(r.mesh_id.clone()), name: r.name.clone(), status });
    }
    t
}

/// `observed` with the newest birth of each node record in `rows` it does not hold by node.id added, at
/// the record's status, for a fabric.delete's plan: a node.delete the Build began is owed its `Complete`
/// whatever the view still holds (fabric-delete.md "Failure"), and the birth a deleted mesh's final
/// primary's delete acts on is read from its node record, which is durable (D67), once the view has
/// let it go.
pub fn with_node_rows(observed: &Topology, rows: &[crate::storage::NodeRow]) -> Topology {
    let mut t = observed.clone();
    for r in rows {
        if t.nodes.iter().any(|n| n.node_id == r.node_id) {
            continue;
        }
        let Some(birth) = r.newest_birth() else { continue };
        let mut n = crate::model::Node::allocated(r.location(), r.mesh.clone(), &r.r#type);
        n.node_id = r.node_id.clone();
        n.incarnation_id = Some(birth.incarnation_id.clone());
        n.status = r.status;
        t.nodes.push(n);
    }
    t
}

/// The meshes, by name, whose mesh.delete `build` began (an `AcceptedRoster` step ended `complete`)
/// and has not completed (no `MeshDeleted` or `MeshLeft` step ended `complete`): a deleted mesh among
/// them is still the Build's to resume from its events, a deleted mesh outside them is done.
fn open_mesh_deletes(build: &crate::build_state::BuildProjection, observed: &Topology) -> BTreeSet<String> {
    observed
        .meshes
        .iter()
        .filter_map(|m| {
            let op = crate::mesh_delete::operation(m.id.as_ref()?);
            let op = op.to_string();
            let open = completed(&build.steps, &op, crate::mesh_delete::STEP_ACCEPTED_ROSTER) && crate::mesh_delete::owner_resume(&build.steps, &op) == crate::mesh_delete::OwnerResume::Lead;
            open.then(|| m.name.clone())
        })
        .collect()
}

/// Whether `operation`'s `step` ended `complete` in `steps`.
fn completed(steps: &[crate::build_state::BuildStep], operation: &str, step: &str) -> bool {
    steps.iter().any(|r| r.operation == operation && r.step == step && r.outcome == crate::build_state::StepOutcome::Complete)
}

/// Whether `operation` is done in `steps`: its `Complete` step ended `complete`.
fn done(steps: &[crate::build_state::BuildStep], operation: &rdm_core::OperationId) -> bool {
    completed(steps, &operation.to_string(), "Complete")
}

fn plan_with(
    topology: &FabricTopology,
    observed: &Topology,
    open_deletes: &BTreeSet<String>,
    held: &BTreeSet<String>,
    steps: &[crate::build_state::BuildStep],
    created: &BTreeSet<PathName>,
) -> BuildPlan {
    let mut ops = Vec::new();
    // fabric.delete (lifecycles/fabric-delete.md): run mesh.delete on each mesh leaf-inward (every
    // other mesh in name order first, the fabric-primary's own mesh last), run node.delete of each
    // other mesh's final primary after its mesh.deleted, then release the fabric name and report
    // fabric.deleted while the fabric-primary stays alive for its separate node.delete. That separate
    // node.delete is a Build with no mesh left and the fabric-primary's birth pinned: never this.
    if topology.meshes.is_empty() && topology.pinned.is_empty() {
        let fp_mesh = observed.fabric_primary().map(|n| n.mesh.as_str());
        let mut meshes: Vec<&crate::model::Mesh> = observed.meshes.iter().collect();
        meshes.sort_by(|a, b| (Some(a.name.as_str()) == fp_mesh).cmp(&(Some(b.name.as_str()) == fp_mesh)).then_with(|| a.name.cmp(&b.name)));
        for m in meshes {
            let is_fp_mesh = Some(m.name.as_str()) == fp_mesh;
            let Some(mesh_id) = m.id.clone() else {
                tracing::info_span!("rdm.node_admin.build.reject.via-mesh-id-unknown", mesh = %m.name).in_scope(|| tracing::info!("the mesh's shutdown is not planned"));
                continue;
            };
            let op_key = crate::mesh_delete::operation(&mesh_id).to_string();
            match crate::mesh_delete::owner_resume(steps, &op_key) {
                crate::mesh_delete::OwnerResume::Left => {}
                // The mesh's final primary is its mesh-primary, the one birth of the mesh its records still
                // hold (mesh-delete.md step 7; fabric-delete.md step 2): its node.delete follows the
                // mesh.deleted, pinned to that birth. The mesh's other births are its mesh.delete's. A
                // node.delete this Build began is owed its `Complete` whatever the view now holds of its
                // birth (fabric-delete.md "Failure").
                crate::mesh_delete::OwnerResume::Deleted => {
                    if !is_fp_mesh {
                        let begun = |n: &crate::model::Node| { let key = rdm_core::OperationId::node("node.delete", &n.node_id).to_string(); steps.iter().any(|s| s.operation == key) };
                        for n in observed.births().filter(|n| n.mesh == m.name && ((n.is_primary && (n.status.is_live() || n.stopped)) || begun(n))) {
                            let Some(birth) = crate::build::PinnedBirth::of(n) else { continue };
                            let op = BuildOperation::DeleteLiveNode { r#type: birth.r#type.clone(), mesh: birth.mesh.clone(), path: n.path.clone(), name: n.name.clone(), exited: None, birth: Some(birth) };
                            if !done(steps, &op.key()) {
                                ops.push(op);
                            }
                        }
                    }
                }
                crate::mesh_delete::OwnerResume::Lead => {
                    if m.status != crate::model::ScopeStatus::Dead || open_deletes.contains(&m.name) {
                        ops.push(BuildOperation::ShutdownMesh { mesh: m.name.clone(), mesh_id });
                    }
                }
            }
        }
        let del_op = BuildOperation::DeleteFabric { fabric_id: observed.fabric.id.clone() };
        if !done(steps, &del_op.key()) {
            ops.push(del_op);
        }
        return BuildPlan { operations: ops };
    }
    let mesh_exists = |m: &str| observed.meshes.iter().any(|x| x.name == m);
    for (name, m) in &topology.meshes {
        if !mesh_exists(name) {
            ops.push(BuildOperation::CreateMesh { mesh: name.clone(), mesh_id: m.mesh_id.clone() });
        }
        // A mesh's node-admins are planned first, then its other nodes, each in path order.
        let mut order: Vec<&PathName> = m.nodes.iter().collect();
        order.sort_by_key(|p| !m.spec(p).is_some_and(|s| rdm_core::is_node_admin(&s.r#type)));
        for p in order {
            // A path whose birth the view holds live is satisfied; any other path is planned and the
            // create pipeline's target decides against the world (a running runtime at the path is
            // held, never replaced: `AdminRunner::revise_predecessor`).
            // A stopped birth (not live, still present: states.md "Stopped nodes") satisfies its path once
            // this Build's create of the path is complete: the create's check reached it and held it
            // (`AdminRunner::create`), and a completed operation is never planned again. A stopped birth
            // no create of this Build has checked is planned, and the check decides: reached, held;
            // unreached, deleted and the path created again.
            let create_done = || topology.create_key(p).is_some_and(|key| done(steps, &key));
            // A held peer mesh's birth under a silence mark satisfies its path ([`plan_holding`]).
            let held_silent = |n: &crate::model::Node| held.contains(name) && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead);
            // Only a node this Build creates is created, with the node.id its record names (D58): a path
            // of an earlier Build is no create of this one.
            let Some(node_id) = topology.created_id(p) else { continue };
            // This Build's create of the path began (a step event of it) and holds no `Complete`: it is
            // planned again whatever the view holds, and resumes from what is durable (build.md §1, §4: a
            // resubmit runs every step whose event did not end `complete`). A launched birth in the view
            // is never what satisfies its own unfinished create.
            let create_open = topology.create_key(p).is_some_and(|key| {
                let op = key.to_string();
                steps.iter().any(|s| s.operation == op) && !done(steps, &key)
            });
            if create_open || !observed.node(p).is_some_and(|n| n.mesh == *name && m.spec(p).is_some_and(|s| s.r#type == n.r#type) && (n.status.is_live() || (n.stopped && create_done()) || held_silent(n))) {
                // The executable the path's entry names (a request named it).
                ops.push(BuildOperation::CreateNode { r#type: m.spec(p).expect("listed node has a spec").r#type.clone(), mesh: name.clone(), path: p.path.clone(), name: p.name.clone(), node_id: node_id.clone(), over: None, executable: topology.node_spec(p).and_then(|s| s.executable.as_ref()).cloned(), entry: None });
            }
        }
    }
    // A live or stopped node of a kept mesh that the topology no longer names is deleted
    // (a node.delete of a stopped node deletes it: lifecycles/node-delete.md), and so is one its
    // mesh-primary recorded os-stopped, silent as it is (node-delete.md "OS-suspended target": its
    // delete skips the stop by name); a birth that is only silent is never deleted for its silence. A
    // node leaving under another operation is that operation's. A mesh the topology no longer names is deleted whole
    // (every member, dead or live, by the delete pipeline), never member by member.
    // A surplus birth's deletion is its own attempt (no action): an attempt that carries an action
    // repairs exactly its birth.
    // A Build that failed stops at its step; nothing is ever reverted, and going back is a Build of the
    // former topology (Q345).
    // A birth the fabric-primary's atlas holds a create of this Build at is not surplus: a recipe's
    // `Create` step adds no path to the accepted topology, which only a forward Build or the gossiped
    // topology finalizes (Luke 2026-10-30: drift is decided from the atlas, not the topology).
    // Only a birth the Build's acceptance pinned is deleted: a Build removes exactly what its change
    // names, never a node it does not touch (build.md §1).
    let mut extra: Vec<PathName> = surplus_births(topology, observed).map(|n| n.location()).filter(|p| !created.contains(p) && topology.pinned.iter().any(|(at, _)| at == p)).collect();
    extra.sort();
    // Each delete carries the exact birth it acts on (build.md §4.4): the one its Build pinned at
    // acceptance, else, for a birth that became surplus after it (runtime drift: a failed create's node),
    // the birth this fabric-primary's view holds as it opens the drift attempt. The executing
    // mesh-primary verifies it before any effect either way. A delete whose `Complete` this Build holds is
    // never planned again (build.md §1), whatever a view still holds: the fabric-primary's own delete
    // after fabric.deleted is run by the birth it deletes, whose view keeps itself.
    for p in extra {
        let birth = topology.pinned.iter().find(|(at, _)| *at == p).map(|(_, b)| b.clone()).or_else(|| observed.node(&p).and_then(crate::build::PinnedBirth::of));
        if let Some(birth) = birth {
            let op = BuildOperation::DeleteLiveNode { r#type: birth.r#type.clone(), mesh: birth.mesh.clone(), path: p.path.clone(), name: p.name.clone(), exited: None, birth: Some(birth) };
            if !done(steps, &op.key()) {
                ops.push(op);
            }
        }
    }
    // A deleted mesh (its row deleted at mesh.deleted) is done: its mesh.delete ended, and its last
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
/// store holds none, the identity it learned (its entry answer) as its own runtime
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

    /// The fabric's `topology_storage_provider` as its Fabric row names it (storage.md §2); `None` while
    /// the store holds no Fabric record.
    pub async fn topology_storage_provider(&self) -> Result<Option<String>, FabricStorageError> {
        Ok(self.storage.fabric().await?.and_then(|r| r.topology_storage_provider))
    }

    /// The application's node types as the Fabric record declares them (the Fabrics schema's `types`,
    /// fixed by Day 0's `RDM_NODE_TYPES`); none while the store holds no Fabric record.
    pub async fn types(&self) -> Result<Vec<rdm_core::NodeType>, FabricStorageError> {
        Ok(self.storage.fabric().await?.map(|r| r.types).unwrap_or_default())
    }

    /// The `fabric.storage` this store reads the Fabric record from.
    pub fn storage(&self) -> &Arc<dyn FabricStorage> {
        &self.storage
    }

    /// A Fabric identity heard in an entry answer (`from`), taken as this admin's runtime state
    /// (nothing is written: the fabric-primary writes `fabric/`), never reading the store (Q227: a node
    /// reads nothing from the store on create, start or rejoin; rdm#89). [`Self::record`] answers the
    /// store's identity over it whenever the store holds one.
    pub async fn learn(&self, identity: FabricIdentity, from: &str) {
        // An identity already learned stands: the same Fabric's is nothing new, and another Fabric's is
        // not this admin's.
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
    /// that `builds` holds complete (accepted, `rdm.build.started` and `rdm.build.ended` complete
    /// written by `me`): the current Build. Fixtures: what a Day 0 plus one complete Build leaves behind.
    pub async fn seeded(builds: &dyn BuildStateAdapter, fabric_id: crate::model::FabricId, topology: FabricTopology, me: &rdm_core::framing::Birth) -> Result<Arc<Self>, BuildStateError> {
        let storage = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        crate::fabric_storage::write_identity(&*storage, &FabricIdentity { fabric_id: fabric_id.clone(), name: topology.fabric.clone() })
            .await.map_err(|e| BuildStateError::Io(e.to_string()))?
            .map_err(|held| BuildStateError::Io(held.to_string()))?;
        let store = Arc::new(Self::new(storage, me.node_id.as_str()));
        let source = crate::events_storage::EventSource { fabric_id: fabric_id.clone(), node_id: me.node_id.clone(), incarnation: me.incarnation.clone() };
        let build_id = BuildId::mint();
        crate::build_state::accept(builds, &crate::build_state::BuildAccepted { build_id: build_id.clone(), topology, change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 }, None, me.node_id.as_str(), "seeded").await?;
        let events = builds.events();
        events.append(&crate::events_storage::Event::build_started(&source, 1, &build_id)).await?;
        events.append(&crate::events_storage::Event::build_ended(&source, 2, &build_id, &crate::events_storage::EndedState::Complete)).await?;
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn n(name: PathName, mesh: &str, r#type: &str, status: NodeStatus, primary: bool) -> Node {
        let mut node = Node::allocated(name.clone(), mesh, r#type);
        node.status = status;
        node.is_primary = primary;
        node.incarnation_id = Some(IncarnationId::mint());
        node
    }

    /// The birth `o` holds at `path`, as the gates pin it (build.md §4.4).
    fn pin_of(o: &Topology, path: PathName) -> crate::build::PinnedBirth {
        crate::build::PinnedBirth::of(o.node(&path.clone()).unwrap()).unwrap()
    }

    /// `t` with every birth of `o` it deletes pinned, as its acceptance pins them.
    fn pinned(mut t: FabricTopology, o: &Topology) -> FabricTopology {
        t.pinned = surplus_births(&t, o).filter_map(|n| crate::build::PinnedBirth::of(n).map(|b| (n.location(), b))).collect();
        t
    }

    /// The node.delete of the birth `o` holds at `path`, pinned to it.
    fn delete_of(o: &Topology, path: PathName) -> BuildOperation {
        { let pin = pin_of(o, path.clone()); BuildOperation::DeleteLiveNode { mesh: pin.mesh.clone(), r#type: pin.r#type.clone(), path: path.path.clone(), name: path.name.clone(), exited: None, birth: Some(pin) } }
    }

    fn observed(nodes: Vec<Node>, meshes: &[&str]) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: meshes.iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::Ready }).collect(),
            nodes,
        }
    }

    fn mn() -> Topology {
        use NodeStatus::Ready as R;
        observed(vec![n(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", R, true), n(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", R, false), n(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", R, true), n(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", R, false), n(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", R, false)], &["mesh1"])
    }

    /// A Build of the whole topology `meshes`: it creates every node, each under its fixture node.id.
    fn t(meshes: &[(&str, u32, u32)]) -> FabricTopology {
        creating(FabricTopology { created: Vec::new(), pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&MeshDesired::of(*m, [("node-admin", *a), ("node", *r)])))).collect() })
    }

    /// `topology` as a Build that creates every node it names, each under its fixture node.id (D58: the
    /// record names the node.id of each node the Build creates).
    fn creating(mut topology: FabricTopology) -> FabricTopology {
        topology.created = topology.meshes.values().flat_map(|m| m.nodes.iter()).map(|p| (p.clone(), rdm_core::fixture_node_id(p))).collect();
        topology
    }

    fn paths(m: &MeshTopology) -> Vec<PathName> {
        let mut all: Vec<PathName> = m.nodes.iter().cloned().collect();
        all.sort_by_key(|p| !m.spec(p).is_some_and(|s| rdm_core::is_node_admin(&s.r#type)));
        all
    }

    /// CONTRACT (fabric.delete is its own Build compiled from the current topology plus its change;
    /// lifecycles/fabric-delete.md, "Numbered end-to-end sequence"): a `DeleteFabric` change compiles
    /// the current topology to the Fabric with no mesh, and that Build's plan is the page's sequence:
    /// step 1, `mesh.delete` (`ShutdownMesh`) of each mesh, the fabric-primary's own mesh last (here
    /// mesh2, so the order is not the names' order); step 2, once a mesh's `mesh.deleted` stands (its
    /// `MeshDeleted` receipt), `node.delete` (`DeleteLiveNode`) of that mesh's mesh-primary in its place;
    /// step 3, `fabric.deleted` (`DeleteFabric`) last. The fabric-primary's own `node.delete` (step 4)
    /// is a separate call from outside the fabric, never this Build's. Must NOT happen: the
    /// fabric-primary's own mesh deleted before another, a deleted mesh's mesh.delete planned again,
    /// or fabric.deleted planned before the meshes.
    // @feature: mesh-lifecycle
    #[test]
    fn a_fabric_delete_compiles_to_the_build_its_page_names() {
        use NodeStatus::Ready as R;
        let cur = t(&[("mesh1", 1, 1), ("mesh2", 1, 1), ("mesh3", 1, 1)]);
        let compiled = compile(&cur, &BuildChange::DeleteFabric, &mn()).unwrap();
        assert_eq!((compiled.fabric.as_str(), compiled.meshes.len()), ("fabric1", 0), "the Fabric with no mesh");
        let mut fabric_primary = n(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", R, true);
        fabric_primary.is_fabric_primary = true;
        let o = observed(vec![n(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", R, true), n(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", R, false), fabric_primary, n(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", R, false), n(PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", R, true), n(PathName::new("mesh3.node", "1").unwrap(), "mesh3", "node", R, false)], &["mesh1", "mesh2", "mesh3"]);
        let id = |m: &str| o.meshes.iter().find(|x| x.name == m).and_then(|x| x.id.clone()).unwrap();
        let shutdown = |m: &str| BuildOperation::ShutdownMesh { mesh: m.into(), mesh_id: id(m) };
        let deleted = BuildOperation::DeleteFabric { fabric_id: o.fabric.id.clone() };
        assert_eq!(plan(&compiled, &o).operations, vec![shutdown("mesh1"), shutdown("mesh3"), shutdown("mesh2"), deleted.clone()], "step 1, the fabric-primary's own mesh last, then step 3");

        let primary = o.node(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).unwrap();
        let mesh_deleted = crate::build_state::BuildStep {
            build_id: BuildId::mint(),
            mesh: "mesh1".into(),
            operation: crate::mesh_delete::operation(&id("mesh1")).to_string(),
            step: crate::mesh_delete::STEP_MESH_DELETED.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            subject: None,
            source: String::new(),
        };
        assert_eq!(
            plan_with(&compiled, &o, &BTreeSet::new(), &BTreeSet::new(), &[mesh_deleted.clone()], &BTreeSet::new()).operations,
            vec![delete_of(&o, PathName::new("mesh1.node-admin", "1").unwrap()), shutdown("mesh3"), shutdown("mesh2"), deleted],
            "step 2: mesh1's mesh.deleted stands, so its mesh-primary's node.delete takes its place"
        );

        // The view no longer holds mesh1 or its final primary: a node.delete of it this Build began is
        // owed its receipt, planned first (fabric-delete.md "Failure"). The drive plans over the view with
        // mesh1's Mesh row (`with_mesh_rows`) and the begun delete's node record (`with_node_rows`, D67).
        let mut fp = n(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", R, true);
        fp.is_fabric_primary = true;
        let view = observed(vec![fp, n(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", R, false), n(PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", R, true), n(PathName::new("mesh3.node", "1").unwrap(), "mesh3", "node", R, false)], &["mesh2", "mesh3"]);
        let mesh1_row = crate::storage::MeshRecord { state: rdm_core::status::MeshState::Deleted, ..crate::storage::MeshRecord::new(id("mesh1"), "mesh1") };
        let mut record = crate::storage::NodeRow::new(primary.location(), "mesh1", "node-admin", primary.node_id.clone(), NodeStatus::Deleted);
        record.births.push(crate::storage::BirthFacts::new(primary.incarnation_id.clone().unwrap(), primary.location()));
        let gone = with_node_rows(&with_mesh_rows(&view, &[mesh1_row]), &[record]);
        let gid = |m: &str| gone.meshes.iter().find(|x| x.name == m).and_then(|x| x.id.clone()).unwrap();
        let gshutdown = |m: &str| BuildOperation::ShutdownMesh { mesh: m.into(), mesh_id: gid(m) };
        let gdeleted = BuildOperation::DeleteFabric { fabric_id: gone.fabric.id.clone() };
        let begun = crate::build_state::BuildStep { operation: rdm_core::OperationId::node("node.delete", &primary.node_id).to_string(), step: "TerminateRuntime".into(), ..mesh_deleted.clone() };
        assert_eq!(
            plan_with(&compiled, &gone, &BTreeSet::new(), &BTreeSet::new(), &[mesh_deleted, begun], &BTreeSet::new()).operations,
            vec![delete_of(&o, PathName::new("mesh1.node-admin", "1").unwrap()), gshutdown("mesh3"), gshutdown("mesh2"), gdeleted],
            "the begun node.delete of a deleted mesh's final primary the view no longer holds is still owed"
        );
    }

    /// CONTRACT (build.md §1: a step whose event ended `complete` is never run again; build.md §4: a
    /// resubmitted Build runs every step whose event did not end `complete`; Q346): a create this Build
    /// began (its `AllocateIdentity` and `DeployRuntime` events `complete`) whose mesh-primary was lost
    /// before its `Complete` is planned again although its launched birth is live in the view, and a
    /// create whose `Complete` stands is not. Must NOT happen: the launched birth in the view taken as
    /// its own unfinished create done.
    // @feature: build
    #[test]
    fn a_begun_create_with_no_complete_is_planned_again_though_its_node_is_in_the_view() {
        use NodeStatus::Ready as R;
        let build = t(&[("mesh1", 1, 2)]);
        let o = observed(vec![n(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", R, true), n(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", R, true), n(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", R, false)], &["mesh1"]);
        let step = |at: &PathName, step: &str| crate::build_state::BuildStep {
            build_id: BuildId::mint(),
            mesh: "mesh1".into(),
            operation: build.create_key(at).unwrap().to_string(),
            step: step.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            subject: None,
            source: String::new(),
        };
        let (one, two) = (PathName::new("mesh1.node", "1").unwrap(), PathName::new("mesh1.node", "2").unwrap());
        let steps = vec![step(&one, "AllocateIdentity"), step(&one, "Complete"), step(&two, "AllocateIdentity"), step(&two, "DeployRuntime")];
        let planned: Vec<PathName> = plan_with(&build, &o, &BTreeSet::new(), &BTreeSet::new(), &steps, &BTreeSet::new())
            .operations
            .into_iter()
            .filter_map(|op| match op {
                BuildOperation::CreateNode { path, name, .. } => Some(PathName { path, name }),
                _ => None,
            })
            .collect();
        assert_eq!(planned, vec![two], "only the begun create with no Complete is planned again; the view's live births satisfy the rest");
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
        // Each Build composes from the records of the one before, realized (build.md §4).
        let realized = crate::test_support::records_of;
        let mesh2 = || crate::test_support::create_of(&["mesh2"], &[("mesh2.node-admin", "1", "mesh2", "node-admin"), ("mesh2.node", "1", "mesh2", "node")]);
        let two = compile(&cur, &mesh2(), &realized(&cur)).unwrap();
        let m2 = two.mesh("mesh2").unwrap().mesh_id.clone();
        assert_ne!(m2, m1, "mesh2 enters the topology under an id of its own");
        assert_eq!(two.mesh("mesh1").unwrap().mesh_id, m1, "a kept mesh keeps its id");
        let create = |t: &FabricTopology| plan(t, &mn()).operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. }));
        let want = Some(BuildOperation::CreateMesh { mesh: "mesh2".into(), mesh_id: m2.clone() });
        assert_eq!((create(&two), create(&two)), (want.clone(), want.clone()), "every plan of the Build carries the id minted at acceptance");
        let grown = compile(&two, &crate::test_support::create_of(&[], &[("mesh2.node", "2", "mesh2", "node")]), &realized(&two)).unwrap();
        let whole = compile(&grown, &crate::test_support::create_of(&["mesh3"], &[("mesh3.node-admin", "1", "mesh3", "node-admin")]), &realized(&grown)).unwrap();
        assert_eq!((whole.mesh("mesh1").unwrap().mesh_id.clone(), whole.mesh("mesh2").unwrap().mesh_id.clone()), (m1.clone(), m2.clone()), "a later Build carries every kept mesh's id unchanged");
        let m3 = whole.mesh("mesh3").unwrap().mesh_id.clone();
        assert!(m3 != m1 && m3 != m2, "a mesh a Create adds enters under an id of its own");
        let removed = compile(&whole, &BuildChange::Delete { meshes: vec!["mesh2".into()], node_ids: Vec::new() }, &realized(&whole)).unwrap();
        let again = compile(&removed, &mesh2(), &realized(&removed)).unwrap();
        assert_ne!(again.mesh("mesh2").unwrap().mesh_id, m2, "a mesh removed and created again is a new mesh with a new id");
    }

    #[test]
    fn the_plan_is_topology_minus_observed_and_a_satisfied_topology_plans_nothing() {
        let cur = t(&[("mesh1", 2, 3)]);
        assert_eq!(plan(&cur, &mn()).operations, vec![]);
        let fresh = plan(&cur, &observed(vec![], &[]));
        assert_eq!(fresh.operations[0], BuildOperation::CreateMesh { mesh: "mesh1".into(), mesh_id: cur.mesh("mesh1").unwrap().mesh_id.clone() });
        assert_eq!(fresh.operations.len(), 6);
        // A dead (unheard) birth's path is planned again — the create pipeline's target then holds a
        // runtime that still runs; a live node outside the topology the Build pinned is deleted.
        let mut o = mn();
        o.nodes[3].status = NodeStatus::PendingReconnect;
        o.nodes.push(n(PathName::new("mesh1.node", "7").unwrap(), "mesh1", "node", NodeStatus::Ready, false));
        assert_eq!(
            plan(&pinned(cur.clone(), &o), &o).operations,
            vec![BuildOperation::CreateNode { r#type: "node".into(), mesh: "mesh1".into(), path: "mesh1.node".into(), name: "2".into(), node_id: rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "2").unwrap()), over: None, executable: None, entry: None }, delete_of(&o, PathName::new("mesh1.node", "7").unwrap())]
        );
        // A Build deletes only what its acceptance pinned (build.md §1: never a node it does not touch):
        // a node outside the topology it never pinned is left as it is. One the Build pinned keeps its
        // pin whatever the view holds at the path.
        assert_eq!(plan(&cur, &o).operations, vec![BuildOperation::CreateNode { r#type: "node".into(), mesh: "mesh1".into(), path: "mesh1.node".into(), name: "2".into(), node_id: rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "2").unwrap()), over: None, executable: None, entry: None }]);
        let mut later = o.clone();
        later.nodes.iter_mut().find(|x| x.location() == PathName::new("mesh1.node", "7").unwrap()).unwrap().node_id = NodeId::mint();
        assert_eq!(plan(&pinned(cur.clone(), &o), &later).operations[1], delete_of(&o, PathName::new("mesh1.node", "7").unwrap()));
        // A mesh outside the topology is deleted after everything else.
        let mut o2 = mn();
        let id2 = MeshId::mint();
        o2.meshes.push(Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready });
        assert_eq!(plan(&cur, &o2).operations, vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2 }]);
    }

    /// CONTRACT (states.md "Node row silence marks": "never themselves a reason to act"; R-D1: a peer mesh
    /// held unheard is held as a mesh; Q221 gate 6): at acceptance, a birth of a peer mesh the
    /// fabric-primary holds unheard under a silence mark satisfies its path, so a request that adds a node to
    /// the fabric-primary's own mesh plans that create alone; a path the request changes in the held mesh is
    /// still planned, for gate 6 to refuse; a mesh not held plans the silent path again as ever.
    /// Must NOT happen: a create planned in the held mesh from its silence marks alone.
    #[test]
    fn a_held_peer_meshs_silent_births_satisfy_their_paths_at_acceptance() {
        let mut o = mn();
        o.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
        for (name, r#type, status, primary) in [(PathName::new("mesh2.node-admin", "1").unwrap(), "node-admin", NodeStatus::PendingReconnect, true), (PathName::new("mesh2.node", "1").unwrap(), "node", NodeStatus::Dead, true), (PathName::new("mesh2.node", "2").unwrap(), "node", NodeStatus::PendingReconnect, false)] {
            o.nodes.push(n(name, "mesh2", r#type, status, primary));
        }
        let mut cur = t(&[("mesh1", 2, 4), ("mesh2", 1, 2)]);
        cur.meshes.get_mut("mesh2").unwrap().mesh_id = o.meshes[1].id.clone().unwrap();
        cur.meshes.get_mut("mesh1").unwrap().mesh_id = o.meshes[0].id.clone().unwrap();
        let held: BTreeSet<String> = ["mesh2".to_string()].into();
        let specs = t(&[("mesh1", 2, 4), ("mesh2", 1, 3)]);
        let created = |p: PathName| BuildOperation::CreateNode { mesh: specs.node_spec(&p).unwrap().mesh.clone(), r#type: specs.node_spec(&p).unwrap().r#type.clone(), node_id: rdm_core::fixture_node_id(&p), path: p.path.clone(), name: p.name.clone(), over: None, executable: None, entry: None };
        assert_eq!(plan_holding(&cur, &o, &held).operations, vec![created(PathName::new("mesh1.node", "4").unwrap())], "the own mesh's added path alone");
        let grown = t(&[("mesh1", 2, 4), ("mesh2", 1, 3)]);
        let mut grown = grown;
        grown.meshes.get_mut("mesh2").unwrap().mesh_id = o.meshes[1].id.clone().unwrap();
        grown.meshes.get_mut("mesh1").unwrap().mesh_id = o.meshes[0].id.clone().unwrap();
        assert_eq!(plan_holding(&grown, &o, &held).operations, vec![created(PathName::new("mesh1.node", "4").unwrap()), created(PathName::new("mesh2.node", "3").unwrap())], "a path the request adds in the held mesh is planned");
        let keys: Vec<rdm_core::OperationId> = plan_holding(&cur, &o, &BTreeSet::new()).operations.iter().map(|op| op.key()).collect();
        assert_eq!(keys, [rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh1.node", "4").unwrap())), rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh2.node-admin", "1").unwrap())), rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh2.node", "1").unwrap())), rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh2.node", "2").unwrap()))], "not held: the silent paths are planned again");
    }

    /// CONTRACT (mesh-delete.md steps 5-7): a deleted mesh (its row deleted at mesh.deleted) whose last
    /// admin lives on is done for every Build: the plan names nothing of it, so no later Build runs
    /// its mesh.delete again or deletes its last admin unasked. Only the Build that admitted its
    /// mesh.delete and holds no completion of it plans it again, to resume from the receipts; a
    /// deleting mesh (not yet Dead) is planned as before. Must NOT happen: a later Build re-running
    /// mesh.delete on a deleted mesh, or deleting its last admin, whose end is a separate node.delete.
    #[test]
    fn a_deleted_mesh_is_planned_only_by_the_build_that_admitted_its_mesh_delete_and_holds_no_completion() {
        let cur = t(&[("mesh1", 2, 3)]);
        let id2 = MeshId::mint();
        let mut o = mn();
        o.nodes.push(n(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", NodeStatus::Ready, true));
        o.meshes.push(Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Dead });
        assert_eq!(plan(&cur, &o).operations, vec![], "a deleted mesh with its last admin alive is planned by no Build");
        let open: BTreeSet<String> = ["mesh2".to_string()].into();
        assert_eq!(
            plan_with(&cur, &o, &open, &BTreeSet::new(), &[], &BTreeSet::new()).operations,
            vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2.clone() }],
            "the Build that admitted its mesh.delete and holds no completion resumes it"
        );
        o.meshes.iter_mut().filter(|m| m.name == "mesh2").for_each(|m| m.status = ScopeStatus::Leaving);
        assert_eq!(plan(&cur, &o).operations, vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2 }], "a deleting mesh is planned as before");
    }

    /// CONTRACT (lifecycles/node-delete.md, node-identity-and-state.md 6.9 Effect): a stopped node (Leaving,
    /// stopped, its process alive) that the accepted topology no longer names, the topology a
    /// node.delete of it accepts, is deleted: the plan is its one `DeleteLiveNode`. A node leaving under
    /// another operation (Leaving, not stopped) is that operation's, never deleted beside it. Must NOT
    /// happen: a node.delete of a stopped node that converges with nothing planned while its process
    /// lives on.
    #[test]
    fn a_stopped_node_the_topology_no_longer_names_is_deleted() {
        let cur = t(&[("mesh1", 2, 3)]);
        let mut o = mn();
        let mut stopped = n(PathName::new("mesh1.node", "7").unwrap(), "mesh1", "node", NodeStatus::Leaving, false);
        stopped.stopped = true;
        o.nodes.push(stopped);
        assert_eq!(plan(&pinned(cur.clone(), &o), &o).operations, vec![delete_of(&o, PathName::new("mesh1.node", "7").unwrap())]);
        let mut leaving = mn();
        leaving.nodes.push(n(PathName::new("mesh1.node", "7").unwrap(), "mesh1", "node", NodeStatus::Leaving, false));
        assert_eq!(plan(&cur, &leaving).operations, vec![], "a departure already under way is not deleted again");
    }

    /// CONTRACT (node-stop.md step 6, states.md "Stopped nodes": "stopped is not live and is still
    /// present"; a completed operation is never planned again): a stopped birth (its row Stopped,
    /// stopped) at a path the accepted topology names. A Build that removes another path of its mesh
    /// plans that path's delete and, once, the stopped path's create, whose check decides against the
    /// world (reached: held, its `Complete` receipted; unreached: deleted and created again). Once the
    /// Build's receipts hold that create's `Complete`, the plan is the other path's delete alone, and
    /// the settled topology plans nothing. Must NOT happen: `node.create` at the stopped path planned
    /// again after its `Complete` (#3233: every attempt of the delete Build planned it, its check held
    /// the stopped birth and the next attempt planned it again, 850 times).
    #[test]
    fn a_stopped_births_create_is_planned_once_and_never_again_once_its_complete_is_receipted() {
        let mut o = mn();
        let stopped = o.nodes.iter_mut().find(|x| x.location() == PathName::new("mesh1.node", "1").unwrap()).unwrap();
        stopped.status = NodeStatus::Stopped;
        stopped.stopped = true;
        stopped.is_primary = false;
        let other_removed = pinned(t(&[("mesh1", 2, 2)]), &o);
        let create = BuildOperation::CreateNode { r#type: "node".into(), mesh: "mesh1".into(), path: "mesh1.node".into(), name: "1".into(), node_id: rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap()), over: None, executable: None, entry: None };
        assert_eq!(plan(&other_removed, &o).operations, vec![create, delete_of(&o, PathName::new("mesh1.node", "3").unwrap())], "the stopped path's create is planned for its check, beside the other path's delete");
        let held = crate::build_state::BuildStep {
            build_id: BuildId::mint(),
            mesh: "mesh1".into(),
            operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())).to_string(),
            step: "Complete".into(),
            outcome: crate::build_state::StepOutcome::Complete,
            subject: None,
            source: String::new(),
        };
        let steps = [held];
        assert_eq!(plan_with(&other_removed, &o, &BTreeSet::new(), &BTreeSet::new(), &steps, &BTreeSet::new()).operations, vec![delete_of(&o, PathName::new("mesh1.node", "3").unwrap())], "once its Complete is receipted the stopped path's create is never planned again");
        assert_eq!(plan_with(&pinned(t(&[("mesh1", 2, 3)]), &o), &o, &BTreeSet::new(), &BTreeSet::new(), &steps, &BTreeSet::new()).operations, vec![], "the settled topology with a held stopped birth plans nothing");
        let mut failed = steps[0].clone();
        failed.outcome = crate::build_state::StepOutcome::Failed { reason: "x".into(), code: rdm_core::outcome::Code::Unavailable };
        assert!(plan_with(&other_removed, &o, &BTreeSet::new(), &BTreeSet::new(), &[failed], &BTreeSet::new()).operations.iter().any(|op| op.key() == rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh1.node", "1").unwrap()))), "a create that did not complete is planned again");
    }

    /// CONTRACT: a stopped renamed node (a name node.name.update gave it) is neither the live birth at `<path>` nor a missing
    /// node nor a surplus one: the plan of the settled topology is empty with it in the view, and a
    /// successor at the path is the one birth the plan counts.
    #[test]
    fn a_renamed_predecessor_changes_no_plan() {
        let cur = t(&[("mesh1", 2, 3)]);
        for status in [NodeStatus::ConnectionsDeleting, NodeStatus::Leaving, NodeStatus::Dead, NodeStatus::Ready] {
            let mut o = mn();
            let mut old = n(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", status, false);
            old.renamed_from = Some(old.name.clone());
            { let at = old.location().with_name("renamed").unwrap(); old.path = at.path; old.name = at.name; }
            old.stopped = status == NodeStatus::Leaving;
            o.nodes.push(old);
            assert_eq!(plan(&cur, &o).operations, vec![], "{status:?}");
            let mut missing = mn();
            let mut old = n(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", status, false);
            old.renamed_from = Some(old.name.clone());
            { let at = old.location().with_name("renamed").unwrap(); old.path = at.path; old.name = at.name; }
            missing.nodes.retain(|x| x.location() != PathName::new("mesh1.node", "2").unwrap());
            missing.nodes.push(old);
            assert_eq!(plan(&cur, &missing).operations, vec![BuildOperation::CreateNode { r#type: "node".into(), mesh: "mesh1".into(), path: "mesh1.node".into(), name: "2".into(), node_id: rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "2").unwrap()), over: None, executable: None, entry: None }], "the path itself is missing: {status:?}");
        }
    }

    /// CONTRACT (lock D): a mesh from its counts carries an explicit create spec for every path, its
    /// type's default; a grow mints a create spec with the path and a shrink drops it; a
    /// path without a create spec, or a create spec without a path, is refused by name; a create spec
    /// set explicitly stands.
    #[test]
    fn every_materialized_path_carries_exactly_one_create_spec() {
        use rdm_core::meta::{NodeSpec, PersistentDisposition, StoragePersistence};
        let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [("node-admin", 1), ("broker", 2), ("gateway", 1), ("node", 1)]));
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.spec_by_path.len(), 5);
        let preserve = StoragePersistence::Persistent { disposition: PersistentDisposition::Preserve };
        assert_eq!(m.spec(&rdm_core::PathName::new("mesh1.broker", "2").unwrap()).unwrap().storage_persistence, StoragePersistence::Ephemeral, "an undeclared type's storage is `node`'s");
        assert_eq!(m.spec(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().storage_persistence, preserve);
        assert_eq!(m.spec(&rdm_core::PathName::new("mesh1.gateway", "1").unwrap()).unwrap().storage_persistence, StoragePersistence::Ephemeral);
        assert_eq!(m.spec(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).unwrap().storage_persistence, StoragePersistence::Ephemeral);
        let mut topology = FabricTopology { created: Vec::new(), pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m.clone())].into_iter().collect() };
        assert_eq!(topology.validate(), Ok(()));
        // An explicit create spec stands; a path the mesh does not hold is refused.
        let release = NodeSpec { storage_persistence: StoragePersistence::Persistent { disposition: PersistentDisposition::Release }, ..NodeSpec::default_for(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node") };
        assert_eq!(m.set_spec(release.clone()), Ok(()));
        assert_eq!(m.spec(&rdm_core::PathName::new("mesh1.node", "1").unwrap()), Some(&release));
        assert!(matches!(m.set_spec(NodeSpec { path: "mesh1.node".into(), name: "9".into(), ..release.clone() }), Err(BuildReject::UnknownNode { .. })));
        // The consistency ratchet.
        m.spec_by_path.remove(&rdm_core::PathName::new("mesh1.node", "1").unwrap());
        m.spec_by_path.insert(rdm_core::PathName::new("mesh1.compute", "7").unwrap(), NodeSpec { path: "mesh1.compute".into(), name: "7".into(), ..release });
        topology.meshes.insert("mesh1".into(), m);
        assert_eq!(
            topology.validate(),
            Err(BuildReject::NodeSpecMismatch { mesh: "mesh1".into(), missing: vec![rdm_core::PathName::new("mesh1.node", "1").unwrap()], extra: vec![crate::build::NodeSpecConflict { path: "mesh1.compute".into(), name: "7".into(), declared_path: "mesh1.compute".into(), declared_name: "7".into(), declared_mesh: "mesh1".into() }] })
        );
    }

    /// The binding `tag` names for the node at `path`: its type's launch id, a path and sha of `tag`.
    fn exe(tag: &str, r#type: &str) -> rdm_core::binding::Binding {
        rdm_core::binding::Binding { launch_id: r#type.into(), executable: format!("/{tag}/{}", r#type).into(), sha256: format!("{tag}-{}", r#type), image: None }
    }

    /// CONTRACT (storage.md §2: the Build's topology carries `topology_storage_provider`): the journal's
    /// accepted topology written with it reads back as written. A record of the layout before node
    /// records is refused, never read with a guessed provider (i150 format changes: "named rejection of
    /// unsupported stored layouts").
    #[test]
    fn an_accepted_topology_reads_back_its_topology_storage_provider() {
        let named = FabricTopology::root_on("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap(), "app");
        assert_eq!(serde_json::from_value::<FabricTopology>(serde_json::to_value(&named).unwrap()).unwrap(), named);
        assert_eq!(named.topology_storage_provider.as_deref(), Some("app"));
    }

    /// A Build recorded before explicit ownership cannot supply it from a parsed name. The full
    /// rebirth cut refuses that record by the missing field; it never guesses a mesh or a kind.
    #[test]
    fn a_stored_topology_without_explicit_node_ownership_is_refused_by_name() {
        let old = serde_json::json!({
            "fabric": "fabric1",
            "meshes": {"mesh1": {"name": "mesh1", "mesh_id": "meshd0000001", "nodes": [PathName::new("mesh1.node-admin", "1").unwrap()], "spec_by_path": [
                {"path": "mesh1.node-admin", "name": "1", "storage_persistence": {"persistence": "persistent", "disposition": "preserve"}}
            ]}}
        });
        let error = serde_json::from_value::<FabricTopology>(old).unwrap_err().to_string();
        assert!(error.contains("missing field `mesh`"), "{error}");
    }

    /// The entry owns mesh and type even when its path suggests different values. JSON
    /// preserves those values and validation refuses a different owning mesh instead of moving it.
    #[test]
    fn a_complete_topology_preserves_explicit_ownership_independently_of_the_path() {
        let path: PathName = rdm_core::PathName::new("elsewhere.gateway", "north").unwrap();
        let spec = NodeSpec { executable: Some(exe("worker", "compute")), ..NodeSpec::default_for(path.clone(), "mesh2", "compute") };
        let mut mesh = MeshTopology::of(&MeshDesired::of("mesh2", [("node-admin", 1)]));
        mesh.nodes.insert(path.clone());
        mesh.spec_by_path.insert(path.clone(), spec.clone());
        assert_eq!(mesh.nodes_of_type("compute").collect::<Vec<_>>(), vec![&path]);
        assert_eq!(mesh.nodes_of_type("gateway").count(), 0);
        let mut topology = FabricTopology::root("fabric1", "mesh1", MeshId::mint());
        topology.meshes.insert("mesh2".into(), mesh.clone());
        assert_eq!(topology.validate(), Ok(()));
        let encoded = serde_json::to_value(&topology).unwrap();
        assert_eq!(serde_json::from_value::<FabricTopology>(encoded).unwrap(), topology);
        assert!(topology.contains(&path));
        mesh.set_spec(NodeSpec { mesh: "mesh3".into(), ..spec }).unwrap_err();
        assert_eq!(mesh.spec(&path).unwrap().mesh, "mesh2");
        let mut duplicate = topology.clone();
        let other = duplicate.meshes.get_mut("mesh1").unwrap();
        other.nodes.insert(path.clone());
        other.spec_by_path.insert(path.clone(), NodeSpec::default_for(path.clone(), "mesh1", "compute"));
        assert_eq!(duplicate.validate(), Err(BuildReject::NodeNamedTwice { path: path.path.clone(), name: path.name.clone() }));
    }

}
