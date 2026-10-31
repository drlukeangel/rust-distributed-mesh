//! The accepted topology: what a Build carries (RDM #47).
//!
//! The current Build (the Build entities read last in, first out, R-BI3:
//! [`crate::build_state::BuildStateAdapter::current_build`]) holds the complete Fabric topology as
//! exact node `path.name`s per Mesh ([`FabricTopology`]). A submitted change ([`TopologyChange`]) is
//! input only: [`compile`] applies it once to the current accepted topology and yields the next
//! complete map; the change itself is kept on the Build as history, never read to plan. Planning
//! is always `topology − observed` ([`plan`]). A stop, a start, a recovery or a Build's steps change no topology:
//! it is an attempt of the current Build carrying its [`AttemptAction`].
//!
//! Counts (`FabricDesired`, `MeshDesired`) stay API convenience: the compiler resolves them to
//! exact paths before anything is persisted, so the executor never decides which node a count
//! means.

use rdm_core::meta::NodeSpec;
use crate::build::{BuildId, BuildOperation, BuildPlan, BuildReject, FabricDesired, MeshDesired, PinnedBirth};
use rdm_core::build::Step;
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
    /// The create spec of every materialized path (Q166: the node's properties, flat), explicit after
    /// ingress normalization: one entry per node, no entry without a node (`validate`). The journal
    /// wrote this map as `path_config_by_path` before Q166, each entry without its `path` and its
    /// `config`: such a record decodes, each spec's path its key and its config empty (durable format
    /// moved).
    #[serde(default, alias = "path_config_by_path", deserialize_with = "specs_by_path")]
    pub spec_by_path: BTreeMap<PathName, NodeSpec>,
}

/// A create spec as the journal holds it: one written before Q166 carries no `path` (its key, or the
/// first element of the pair it was written in, names it) and no `config`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSpec {
    #[serde(default)]
    path: Option<PathName>,
    /// Read as `storage` too: a spec written before storage.md §1's rename (durable format moved).
    #[serde(alias = "storage")]
    storage_persistence: rdm_core::meta::StorageMeta,
    placement: rdm_core::meta::PlacementMeta,
    #[serde(default)]
    executable: Option<rdm_core::binding::Binding>,
    #[serde(default)]
    tags: BTreeSet<String>,
    #[serde(default)]
    config: BTreeMap<String, String>,
}

impl StoredSpec {
    /// The spec at `path`, the path the record names it by.
    fn at(self, path: PathName) -> NodeSpec {
        NodeSpec { path: self.path.unwrap_or(path), storage_persistence: self.storage_persistence, placement: self.placement, executable: self.executable, tags: self.tags, config: self.config }
    }
}

/// [`MeshTopology::spec_by_path`] from the journal, a spec written before Q166 named by its key.
fn specs_by_path<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeMap<PathName, NodeSpec>, D::Error> {
    let stored = BTreeMap::<PathName, StoredSpec>::deserialize(d)?;
    Ok(stored.into_iter().map(|(path, spec)| (path.clone(), spec.at(path))).collect())
}

/// One node of a stored [`TopologyChange::Reconcile`]: a create spec, or the `[path.name, entry]` pair
/// the journal wrote before Q166.
#[derive(Deserialize)]
#[serde(untagged)]
enum StoredNode {
    Spec(StoredSpec),
    Pair(PathName, StoredSpec),
}

/// [`TopologyChange::Reconcile`]'s nodes from the journal, a pair written before Q166 read as the spec
/// it names (durable format moved).
fn stored_nodes<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<NodeSpec>, D::Error> {
    Vec::<StoredNode>::deserialize(d)?
        .into_iter()
        .map(|n| match n {
            StoredNode::Pair(path, spec) => Ok(spec.at(path)),
            StoredNode::Spec(StoredSpec { path: Some(path), storage_persistence, placement, executable, tags, config }) => Ok(NodeSpec { path, storage_persistence, placement, executable, tags, config }),
            StoredNode::Spec(_) => Err(serde::de::Error::custom("a stored create spec names no path")),
        })
        .collect()
}

/// The storage and placement of `name` as the Day-0 topology's entry names them ([`FabricTopology::root`]:
/// its kind's migration default), written on the Day-0 admin's row at its claim.
pub fn root_placed(name: &PathName) -> (rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta) {
    let spec = NodeSpec::default_for(name.clone());
    (spec.storage_persistence, spec.placement)
}

impl MeshTopology {
    /// A new mesh from its counts, entering the accepted topology: its id minted, and every path
    /// minted with its kind's migration default create spec (ingress normalization; the Build is
    /// explicit from here).
    pub fn of(desired: &MeshDesired) -> Self {
        Self::with_id(desired, MeshId::mint())
    }

    /// The mesh `mesh_id` from its counts: every path minted with its kind's migration default create spec.
    pub(crate) fn with_id(desired: &MeshDesired, mesh_id: MeshId) -> Self {
        let mut nodes = BTreeSet::new();
        let mut spec_by_path = BTreeMap::new();
        for (kind, want) in desired.counts() {
            for ord in 1..=want {
                let path = PathName::new(&desired.name, kind, ord);
                spec_by_path.insert(path.clone(), NodeSpec::default_for(path.clone()));
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
        if !self.nodes.contains(&spec.path) {
            return Err(BuildReject::UnknownNode { node: spec.path.to_string(), step: None });
        }
        self.spec_by_path.insert(spec.path.clone(), spec);
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

/// The complete accepted Fabric topology. Each path's executable is its entry's
/// ([`NodeSpec::executable`]); the fabric holds no executable of its own (build-deployment.md §2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "StoredTopology")]
pub struct FabricTopology {
    /// The fabric's name.
    pub fabric: String,
    /// The accepted meshes, by name.
    pub meshes: BTreeMap<String, MeshTopology>,
    /// The fabric's `topology_storage_provider` the Build realizes (storage.md §2): the request's, or,
    /// when it names none, the Fabric row's at acceptance (Q166). The journal's record of the accepted
    /// Build carries it, a record written before the field read as `file`; the Build topic does not
    /// carry it, so a copy heard there holds `None`.
    pub topology_storage_provider: Option<String>,
    /// The storage change this Build is (storage.md §2 "Changing it"): its `change_id`, minted and
    /// recorded with the accepted Build before its first command, and the old and new providers. `None`
    /// for every Build that does not change the provider. The Build topic does not carry it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_change: Option<rdm_core::build::StorageChange>,
    /// The exact birth each node this Build deletes held when it was accepted (build.md §4.4: "The
    /// operation carries the exact birth the gates pinned"), by path: every node.delete the Build plans
    /// acts on its path's pinned birth and on no other. Set at acceptance, never compiled forward.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pinned: Vec<(PathName, crate::build::PinnedBirth)>,
    /// Every mesh the Build names, by name and id, when this topology holds sections of it, not the
    /// whole (Q284): `meshes` then holds only the sections cut from it, and `pinned` only theirs. `None`: the whole accepted Build, as the
    /// fabric-primary records it. Never journaled: only the fabric-primary writes the record, whole.
    #[serde(skip)]
    pub section_of: Option<BTreeMap<String, MeshId>>,
}

/// A [`FabricTopology`] as the journal holds it: a record written before the executable was each
/// path's carried the fabric's per-kind set beside the meshes (`bindings`), the removed operation's.
#[derive(Deserialize)]
struct StoredTopology {
    fabric: String,
    meshes: BTreeMap<String, MeshTopology>,
    #[serde(default)]
    bindings: Option<StoredSet>,
    #[serde(default = "crate::fabric_storage::file_provider")]
    topology_storage_provider: Option<String>,
    #[serde(default)]
    storage_change: Option<rdm_core::build::StorageChange>,
    #[serde(default)]
    pinned: Vec<(PathName, crate::build::PinnedBirth)>,
}

/// The per-kind set an old record carries: only its bindings are read.
#[derive(Deserialize)]
struct StoredSet {
    #[serde(default)]
    bindings: Vec<rdm_core::binding::Binding>,
}

/// The durable-format move's fill (proposal executable-bindings-at-create.md §1 "Corrected"): an entry
/// of a record written with the old per-kind set that names no executable takes the set's binding of
/// its kind, once, here; the record writes back with no set. The fill is the desired topology only,
/// never a birth's record: what a birth runs is its row's.
impl From<StoredTopology> for FabricTopology {
    fn from(t: StoredTopology) -> Self {
        let mut meshes = t.meshes;
        if let Some(set) = t.bindings {
            for m in meshes.values_mut() {
                for (path, spec) in m.spec_by_path.iter_mut().filter(|(_, s)| s.executable.is_none()) {
                    spec.executable = set.bindings.iter().find(|b| b.launch_id == path.kind.name()).cloned();
                }
            }
        }
        Self { fabric: t.fabric, meshes, topology_storage_provider: t.topology_storage_provider, storage_change: t.storage_change, pinned: t.pinned, section_of: None }
    }
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
        meshes.insert(mesh.to_string(), MeshTopology::with_id(&MeshDesired::of(mesh, [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 0)]), mesh_id));
        Self { fabric: fabric.into(), meshes, topology_storage_provider: Some(topology_storage_provider.into()), storage_change: None, pinned: Vec::new(), section_of: None }
    }

    /// The accepted mesh named `name`.
    pub fn mesh(&self, name: &str) -> Option<&MeshTopology> {
        self.meshes.get(name)
    }

    /// Every mesh the Build names: the whole topology's, or, for sections, the Build's list they carry.
    pub fn mesh_names(&self) -> BTreeSet<String> {
        match &self.section_of {
            Some(named) => named.keys().cloned().collect(),
            None => self.meshes.keys().cloned().collect(),
        }
    }

    /// Whether the Build names mesh `name` (its section held here or not).
    pub fn names_mesh(&self, name: &str) -> bool {
        match &self.section_of {
            Some(named) => named.contains_key(name),
            None => self.meshes.contains_key(name),
        }
    }

    /// The sections of this topology for `meshes` (Q284): one per mesh named there that the topology
    /// holds, each with that mesh's pinned births and every mesh the Build names; one with no mesh when
    /// none is held (a Build with no mesh, or none touched), so the Build itself still travels. A
    /// section is never cut again: it is returned as it is when it holds a mesh asked for.
    pub fn sections(&self, meshes: Option<&BTreeSet<String>>) -> Vec<FabricTopology> {
        let wanted = |m: &String| meshes.is_none_or(|w| w.contains(m));
        if self.section_of.is_some() {
            return if self.meshes.keys().any(wanted) || (self.meshes.is_empty() && meshes.is_none()) { vec![self.clone()] } else { Vec::new() };
        }
        let named: BTreeMap<String, MeshId> = self.meshes.iter().map(|(n, m)| (n.clone(), m.mesh_id.clone())).collect();
        let of = |held: BTreeMap<String, MeshTopology>| FabricTopology {
            pinned: self.pinned.iter().filter(|(p, _)| held.contains_key(&p.mesh)).cloned().collect(),
            fabric: self.fabric.clone(),
            meshes: held,
            topology_storage_provider: self.topology_storage_provider.clone(),
            storage_change: self.storage_change.clone(),
            section_of: Some(named.clone()),
        };
        let cut: Vec<FabricTopology> = self.meshes.iter().filter(|(n, _)| wanted(n)).map(|(n, m)| of(BTreeMap::from([(n.clone(), m.clone())]))).collect();
        if cut.is_empty() { vec![of(BTreeMap::new())] } else { cut }
    }

    /// Fold `other`, a topology of the same Build, into this one: the whole replaces sections, a
    /// section adds its mesh to sections, and nothing changes the whole.
    pub fn absorb(&mut self, other: &FabricTopology) {
        match (&self.section_of, &other.section_of) {
            (None, _) => {}
            (Some(_), None) => *self = other.clone(),
            (Some(_), Some(_)) => {
                for (n, m) in &other.meshes {
                    if !self.meshes.contains_key(n) {
                        self.meshes.insert(n.clone(), m.clone());
                        self.pinned.extend(other.pinned.iter().filter(|(p, _)| &p.mesh == n).cloned());
                    }
                }
            }
        }
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
            let missing: Vec<String> = m.nodes.iter().filter(|p| !m.spec_by_path.contains_key(p)).map(ToString::to_string).collect();
            let extra: Vec<String> = m.spec_by_path.keys().filter(|p| !m.nodes.contains(p)).map(ToString::to_string).collect();
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
        for n in observed.members().filter(|n| n.status.is_live()) {
            let mesh = meshes.entry(n.mesh.clone()).or_insert_with(|| empty(&n.mesh, None));
            mesh.spec_by_path.insert(n.name.clone(), NodeSpec::default_for(n.name.clone()));
            mesh.nodes.insert(n.name.clone());
        }
        Self { fabric: observed.fabric.name.clone(), meshes, topology_storage_provider: None, storage_change: None, pinned: Vec::new(), section_of: None }
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
    /// A stored change of the removed per-kind executable-set operation (proposal
    /// executable-bindings-at-create.md §1, Q157), read from the journal or a Build message as history
    /// only: it owes no replacement (the repair is a [`Self::Reconcile`] naming the executables) and
    /// nothing submits it to [`compile`]. Its set is not kept: the topology it was accepted with had
    /// its entries filled from that set when it was read ([`FabricTopology`]).
    #[serde(rename = "roll")]
    Reserved {},
    /// `build.create { Change(Reconcile { based_on, nodes }) }` (proposal executable-bindings-at-create.md
    /// rule 1): the complete topology, every node by its create spec (Q166), the executable included.
    /// `based_on` is checked against the current Build at the door, before this compiles.
    Reconcile {
        /// The build_id of the current Build the caller read.
        based_on: BuildId,
        /// Every node of the fabric, each by its create spec.
        #[serde(deserialize_with = "stored_nodes")]
        nodes: Vec<NodeSpec>,
        /// The `topology_storage_provider` the request names (storage.md §2); `None` leaves the
        /// fabric's unchanged (Q166). A change stored before the field names none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        topology_storage_provider: Option<String>,
    },
}

impl TopologyChange {
    /// Whether the Build this change opens owes the replacements its rows say are left: each path whose
    /// target executable ([`target_executable`]) differs from what its row records is replaced, in RDM's
    /// order (proposal executable-bindings-at-create.md rule 5).
    pub fn plans_replacements(&self) -> bool {
        matches!(self, Self::Reconcile { .. })
    }
}

/// The executable `topology` fixes for the node at `path`: its entry's (a request named it, proposal
/// executable-bindings-at-create.md rule 2). `None` when it names none: no replacement is owed for it.
pub fn target_executable<'a>(topology: &'a FabricTopology, path: &PathName) -> Option<&'a rdm_core::binding::Binding> {
    topology.mesh(&path.mesh).and_then(|m| m.spec(path)).and_then(|s| s.executable.as_ref())
}

/// Whether the node at a path is replaced: its target names an executable and its live row records
/// another (or none). A path no live row is written at is created, never replaced.
fn replaced_by_target(target: Option<&rdm_core::binding::Binding>, row: Option<&RecordedRow>) -> bool {
    target.is_some_and(|b| row.is_some_and(|r| r.sha256.as_deref() != Some(b.sha256.as_str())))
}

/// What one attempt of a Build does beyond realizing its topology: a standalone node op, a recovery
/// or steps, targeted to the births they act on. Never part of the topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AttemptAction {
    /// `node.connection.delete { All }` of the exact birth at a path; it keeps running
    /// (node-connections-delete.md).
    DeleteConnections {
        /// The node.
        path: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth whose connections are deleted.
        from_incarnation: IncarnationId,
    },
    /// Stop the exact birth at a path: it deletes its connections, cuts its mesh connections and stops, its process
    /// alive; no departure (node-stop.md).
    Stop {
        /// The node.
        path: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth stopped.
        from_incarnation: IncarnationId,
    },
    /// Start the stopped exact birth at a path: the same process rejoins as itself (node-start.md).
    Start {
        /// The node.
        path: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
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
    /// topology never names a `.old` path: this action is its only door outside a Build's steps.
    Delete {
        /// The node's `.old` path.
        path: PathName,
        /// The node id of the birth the gates pinned (build.md §4.4).
        node_id: NodeId,
        /// The birth deleted.
        from_incarnation: IncarnationId,
    },
    /// Steps (`BuildChange::Steps`; build.md §1: "Rafka's replace steps (one Build of node
    /// primitives)"; lifecycles/node-replace.md): they run in order, each as its op's own operation
    /// ([`BuildOperation::Step`]), and a step whose `Complete` is receipted for these steps never runs
    /// again. `pinned` is the exact birth each path a step acts on held when the Build was accepted; a
    /// path an earlier step creates holds no birth yet and is not pinned. Its journal tag stays
    /// `recipe`, the tag `builds.jsonl` already holds (Q211 renamed the variant only, as `roll` before it).
    #[serde(rename = "recipe")]
    Steps {
        /// The steps, in order.
        steps: Vec<Step>,
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

/// Steps accepted against `observed` (the fabric-primary's view): refused by name when there is no
/// step, when they name one operation twice, or acts on a path that holds no birth and that no earlier step
/// creates or renames a node to; otherwise the action that pins every path's birth.
pub(crate) fn steps_action(steps: Vec<Step>, observed: &Topology) -> Result<AttemptAction, BuildReject> {
    if steps.is_empty() {
        return Err(BuildReject::EmptySteps);
    }
    let mut keys = BTreeSet::new();
    for step in &steps {
        let key = crate::build::step_key(step);
        if !keys.insert(key.clone()) {
            return Err(BuildReject::StepRepeated { operation: key });
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
            Step::Create { node, .. } => {
                made.insert(node.clone());
            }
            Step::Update { name, .. } => {
                made.insert(name.clone());
            }
            _ => {}
        }
    }
    step_births(&steps, &pinned)?;
    Ok(AttemptAction::Steps { steps, pinned })
}

/// The exact birth each step of a Build is targeted to: the pinned birth of the node its path names
/// when the step runs, followed through the Build's own creates and renames; `None` for a node an
/// earlier step creates (its executor resolves it at its path). `Err` names the first step acting on
/// a path that holds no node when it runs.
pub(crate) fn step_births(steps: &[Step], pinned: &[(PathName, PinnedBirth)]) -> Result<Vec<Option<PinnedBirth>>, BuildReject> {
    let mut at: BTreeMap<PathName, Option<PinnedBirth>> = pinned.iter().map(|(p, b)| (p.clone(), Some(b.clone()))).collect();
    let mut out = Vec::with_capacity(steps.len());
    for (i, step) in steps.iter().enumerate() {
        let unknown = || BuildReject::UnknownNode { node: step.node().to_string(), step: Some(i as u32 + 1) };
        let birth = match step {
            Step::Create { node, .. } => {
                at.insert(node.clone(), None);
                None
            }
            Step::Update { node, name } => {
                let birth = at.remove(node).ok_or_else(unknown)?;
                at.insert(name.clone(), birth.clone());
                birth
            }
            Step::Delete { node } => at.remove(node).ok_or_else(unknown)?,
            Step::Stop { node } | Step::Start { node } | Step::DeleteConnections { node } | Step::LeaveMesh { node } => at.get(node).cloned().ok_or_else(unknown)?,
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

/// The ordinal `observed` holds the node-type primary at, if any: a shrink never drops it.
fn primary_ordinal(observed: &Topology, mesh: &str, kind: NodeKind) -> Option<u32> {
    observed.node_type_primary(mesh, kind).map(|n| n.name.ordinal)
}

/// Bring `mesh`'s node types to `counts`: grow fills the lowest free ordinals, shrink drops the
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
            mesh.spec_by_path.insert(path.clone(), NodeSpec::default_for(path.clone()));
            mesh.nodes.insert(path);
        }
        while (have.len() as u32) > want {
            let drop = have.iter().rev().copied().find(|o| Some(*o) != keep_ord).or_else(|| have.last().copied()).expect("a member to drop");
            have.retain(|o| *o != drop);
            let path = PathName::new(&mesh.name, kind, drop);
            mesh.spec_by_path.remove(&path);
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
                let mesh = next.meshes.entry(m.name.clone()).or_insert_with(|| MeshTopology { name: m.name.clone(), mesh_id: MeshId::mint(), nodes: BTreeSet::new(), spec_by_path: BTreeMap::new() });
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
                return Err(BuildReject::UnknownNode { node: node.to_string(), step: None });
            }
            // The authority removes a birth it sees: a view that has not heard the node yet refuses
            // by name rather than accepting a Build it would close with nothing to do. Which birth it
            // sees may be deleted (live, stopped, or recorded os-stopped) is its door's, which reads the
            // status rows (`ControlPlane::submit_checked`).
            if observed.node(node).is_none() {
                return Err(BuildReject::UnknownNode { node: node.to_string(), step: None });
            }
            if node.kind == NodeKind::NodeAdmin && m.count(NodeKind::NodeAdmin) <= 1 {
                return Err(BuildReject::WouldLeaveMeshWithoutAdmin { mesh: node.mesh.clone() });
            }
            m.nodes.remove(node);
            m.spec_by_path.remove(node);
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
        // A stored change of the removed operation is history only, decoded from the journal or the Build
        // topic and read by `plans_replacements`; nothing compiles it. The one door that builds a change
        // to compile (build_op.rs `admit_change`) builds it from a decoded `BuildChange`, whose reserved
        // positions are refused `UnknownVariant` before it, and never builds this variant.
        TopologyChange::Reserved {} => unreachable!("a stored change of the removed executable-set operation is history: nothing submits it to compile"),
        TopologyChange::Reconcile { nodes, .. } => next.meshes = reconciled(current, nodes, recorded)?,
    }
    next.validate()?;
    Ok(next)
}

/// The meshes a [`TopologyChange::Reconcile`] names (proposal executable-bindings-at-create.md rule 1):
/// exactly the listed nodes, each by its create spec (Q166). A mesh the topology holds keeps its id; a
/// mesh named for the first time enters under an id minted now. A path the request creates names its
/// executable, else it is refused by name; a path the topology holds that the request lists with none
/// keeps its entry's. A mesh whose only node-admin the request replaces (its entry names another
/// executable than its row records) gains a second node-admin at the next free `path.name` of that
/// type, on the new executable (R4): it is in the topology from acceptance, created first, and stays.
/// Every listed path's entry carries the tag labels and the config the request names: a created node
/// carries them from its claim, a held one has them applied as the Build applies every property that
/// differs from its row (build.md §1). Refused by name: no node listed (that is
/// fabric.delete), a path named twice, a `.old` path, a mesh name outside the grammar, tags outside the
/// label grammar or over the cap, a config over the cap `node.config.update` holds a map to.
fn reconciled(current: &FabricTopology, nodes: &[NodeSpec], recorded: &Recorded) -> Result<BTreeMap<String, MeshTopology>, BuildReject> {
    if nodes.is_empty() {
        return Err(BuildReject::NoNodeListed);
    }
    let mut by_mesh: BTreeMap<String, BTreeMap<PathName, NodeSpec>> = BTreeMap::new();
    for spec in nodes {
        let path = &spec.path;
        if path.old {
            return Err(BuildReject::UnknownNode { node: path.to_string(), step: None });
        }
        if !is_valid_mesh_name(&path.mesh) {
            return Err(BuildReject::InvalidMeshName { mesh: path.mesh.clone() });
        }
        if let Some(detail) = rdm_core::status::tags_refusal(&spec.tags) {
            return Err(BuildReject::TagsRefused { node: path.to_string(), detail });
        }
        if by_mesh.entry(path.mesh.clone()).or_default().insert(path.clone(), spec.clone()).is_some() {
            return Err(BuildReject::NodeNamedTwice { node: path.to_string() });
        }
    }
    let mut meshes = BTreeMap::new();
    for (name, paths) in by_mesh {
        let held = current.meshes.get(&name);
        let mesh_id = held.map(|m| m.mesh_id.clone()).unwrap_or_else(MeshId::mint);
        let mut mesh = MeshTopology { name: name.clone(), mesh_id, nodes: BTreeSet::new(), spec_by_path: BTreeMap::new() };
        for (path, mut spec) in paths {
            match held.and_then(|m| m.spec(&path)) {
                // A held path takes the request's properties (build.md §1: the Build applies every property
                // of the entry that differs from the node's row); only an executable the request lists none
                // for keeps its entry's (rule 2).
                Some(entry) => {
                    if spec.executable.is_none() {
                        spec.executable = entry.executable.clone();
                    }
                }
                None if spec.executable.is_none() => return Err(BuildReject::ExecutableMissing { node: path.to_string() }),
                None => {}
            }
            mesh.nodes.insert(path.clone());
            mesh.spec_by_path.insert(path, spec);
        }
        let admins: Vec<PathName> = mesh.nodes_of_type(NodeKind::NodeAdmin).cloned().collect();
        if let [admin] = admins.as_slice() {
            let entry = mesh.spec(admin).cloned().expect("a listed path carries its create spec");
            if held.is_some_and(|m| m.nodes.contains(admin)) && replaced_by_target(entry.executable.as_ref(), recorded.get(admin)) {
                let ordinal = (1..).find(|o| !mesh.nodes.contains(&PathName::new(&name, NodeKind::NodeAdmin, *o))).expect("a free ordinal");
                let added = PathName::new(&name, NodeKind::NodeAdmin, ordinal);
                mesh.nodes.insert(added.clone());
                mesh.spec_by_path.insert(added.clone(), NodeSpec { path: added, ..entry });
            }
        }
        meshes.insert(name, mesh);
    }
    Ok(meshes)
}

/// Plan `topology − observed`, plus `action` where it still applies. Deterministic, idempotent:
/// what observed reality already satisfies is not planned. Creates come before deletions, meshes
/// in name order, nodes in path order.
pub fn plan(topology: &FabricTopology, observed: &Topology, action: Option<&AttemptAction>) -> BuildPlan {
    plan_with(topology, observed, action, ActionProgress::default(), &BTreeSet::new(), &BTreeSet::new(), &[], None, None, &BTreeSet::new())
}

/// [`plan`] at a Build's acceptance, `held` the peer meshes the fabric-primary holds unheard (R-D1: a peer
/// mesh held unheard is held as a mesh, and its nodes are not restarted, replaced or deleted from here until
/// it is heard again). A birth of one of them under a silence mark satisfies its path: the mark is never
/// itself a reason to act (states.md, "Node row silence marks"), so gate 6 reads only what the request
/// changes there. The drive plans every pass without the hold and stops at such a path by name until the
/// mesh is heard (Q221).
pub fn plan_holding(topology: &FabricTopology, observed: &Topology, held: &BTreeSet<String>) -> BuildPlan {
    plan_with(topology, observed, None, ActionProgress::default(), &BTreeSet::new(), held, &[], None, None, &BTreeSet::new())
}

/// How far an attempt's action got, from the Build's receipts. The view alone cannot say: a stopped
/// birth leaves every view, and a recovered or renamed path names another birth.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActionProgress {
    /// A standalone node.connection.delete, stop, start or delete began for its birth: its first step's receipt.
    started: bool,
    /// Its `Complete` is receipted for the birth.
    finished: bool,
    /// How far a recovery's two operations got for its exited birth.
    recovery: RecoveryProgress,
    /// Per step of a Build, whether its operation's `Complete` is receipted in an attempt that
    /// carried these steps.
    steps_done: Vec<bool>,
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
        // A standalone connections delete, stop, start or stopped delete: the receipts of its operation in an
        // attempt that carried this exact action (its `Opened` fact targeted the attempt to the birth)
        // are this birth's, from the attempt the request opened on (`opened`): an earlier request's run
        // of the same action (a stop, a start, then a second stop of one birth, Q183) is not this
        // run's progress. Its first step's receipt says the operation began; its `Complete`, that it
        // completed for the birth.
        if let Some(action @ (AttemptAction::DeleteConnections { path, .. } | AttemptAction::Stop { path, .. } | AttemptAction::Start { path, .. } | AttemptAction::Delete { path, .. })) = &build.action {
            let (operation, first) = match action {
                AttemptAction::DeleteConnections { .. } => (format!("node.connection.delete:{path}"), "DeleteConnections"),
                AttemptAction::Start { .. } => (format!("node.start:{path}"), "StartNode"),
                // node.delete's one key, whether the birth is deleted stopped or live (`plan_with`).
                AttemptAction::Delete { .. } => (format!("node.delete:{path}"), "NodeDeleting"),
                _ => (format!("node.stop:{path}"), "StopNode"),
            };
            let done = |step: &str| build.steps.iter().any(|s| s.operation == operation && s.step == step && s.outcome == crate::build_state::StepOutcome::Complete && s.attempt >= build.opened && build.carried(s.attempt, action));
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
        // Steps: each step is done once its operation's `Complete` is receipted in an attempt that
        // carried these steps (a Build names each operation once, `steps_action`).
        if let Some(action @ AttemptAction::Steps { steps, .. }) = &build.action {
            let steps_done = steps
                .iter()
                .map(|step| {
                    let operation = crate::build::step_key(step);
                    build.steps.iter().any(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && build.carried(s.attempt, action))
                })
                .collect();
            return Self { steps_done, ..Self::default() };
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
/// `(node_id, incarnation)` (`recovery_progress`), completed in `build`. `None` while no attempt
/// recovered that birth: a recovery that deleted the exited instance and has not created the new
/// node is not one.
pub fn recovered_at(build: &crate::build_state::BuildProjection, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Option<u32> {
    match recovery_progress(build, path, node_id, incarnation) {
        (RecoveryProgress::Recovered, at) => at,
        _ => None,
    }
}

/// The births of a kept mesh `observed` holds live or stopped at a path `topology` no longer names: the
/// nodes a topology Build deletes, each pinned at its acceptance (build.md §4.4).
pub(crate) fn surplus_births<'a>(topology: &'a FabricTopology, observed: &'a Topology) -> impl Iterator<Item = &'a crate::model::Node> + 'a {
    observed.members().filter(move |n| (n.status.is_live() || n.stopped || topology.pinned.iter().any(|(at, b)| *at == n.name && b.is(n))) && topology.meshes.contains_key(&n.mesh) && !topology.contains(&n.name))
}

/// The final primary a mesh.deleted manifest names, as the path and the exact birth its fabric.delete
/// node.delete is pinned to (mesh-delete.md step 7: mesh.deleted carries the final primary's own exact
/// birth). `None` for a manifest naming no parseable birth.
fn final_primary_birth(manifest: &crate::mesh_delete::ExitManifest) -> Option<(PathName, crate::build::PinnedBirth)> {
    let b = &manifest.final_primary;
    Some((b.node.parse().ok()?, crate::build::PinnedBirth { node_id: NodeId::parse(&b.node_id).ok()?, incarnation: IncarnationId::parse(&b.incarnation_id).ok()? }))
}

/// [`plan`] for `build`'s current attempt, given what its receipts say already ran.
pub fn plan_for_build(build: &crate::build_state::BuildProjection, observed: &Topology) -> BuildPlan {
    plan_for_build_with(build, observed, None, None)
}

/// [`plan_for_build`] for a Build that plans replacements ([`TopologyChange::plans_replacements`];
/// build-deployment.md §3), given what the node rows record of each member's executable (`recorded`,
/// [`recorded_executables`]): the topology's creates, then each member still owed its replacement. Any
/// other Build plans as [`plan_for_build`] does.
pub fn plan_with_replacements(build: &crate::build_state::BuildProjection, observed: &Topology, recorded: &Recorded) -> BuildPlan {
    plan_for_build_with(build, observed, Some(recorded), None)
}

/// The birth the fabric-primary's atlas row holds at the path the Build's standalone stop, start,
/// connections delete or delete names (fabric-atlas.md R-FA1: the fabric-primary decides from its atlas,
/// "from it, and from nothing else"; R-FA3: gossip decides nothing): the birth the path's node row names
/// current, as the ops left it; `None` when no node row is written at the path or an op deleted the birth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtlasBirth(pub Option<crate::build::PinnedBirth>);

/// The path the standalone stop, start, connections delete or delete of `build`'s current attempt acts
/// on, whose birth is planned from the fabric-primary's atlas ([`AtlasBirth`]); `None` for any other
/// attempt, and for a delete of a stopped `<path>.old`, planned from the record of that exact birth.
pub fn action_path(build: &crate::build_state::BuildProjection) -> Option<PathName> {
    match build.action.as_ref()? {
        AttemptAction::Delete { path, .. } if path.old => None,
        AttemptAction::DeleteConnections { path, .. } | AttemptAction::Stop { path, .. } | AttemptAction::Start { path, .. } | AttemptAction::Delete { path, .. } => Some(path.clone()),
        _ => None,
    }
}

/// The fabric-primary's plan of `build`'s current attempt ([`plan_with_replacements`] given `recorded`,
/// else [`plan_for_build`]), its standalone action planned from `atlas`, the birth its atlas row holds at
/// the action's path, never from the gossip view (fabric-atlas.md R-FA1, R-FA3): the view drops a birth
/// a stop removed until the restarted birth's digest arrives, and a stop the atlas admitted is not
/// planned to nothing from it. `atlas` `None`: no atlas was read, and the view's birth is planned.
pub fn plan_for_build_with(build: &crate::build_state::BuildProjection, observed: &Topology, recorded: Option<&Recorded>, atlas: Option<&AtlasBirth>) -> BuildPlan {
    plan_for_build_created(build, observed, recorded, atlas, &BTreeSet::new())
}

/// [`plan_for_build_with`], with `created` the paths the fabric-primary's atlas holds a create of this
/// Build at ([`crate::atlas::Atlas::created_by`]): a birth at one of them is not surplus to the accepted
/// topology (Luke 2026-10-30: drift is decided from the atlas, not the topology).
pub fn plan_for_build_created(build: &crate::build_state::BuildProjection, observed: &Topology, recorded: Option<&Recorded>, atlas: Option<&AtlasBirth>, created: &BTreeSet<String>) -> BuildPlan {
    let replacing = recorded.filter(|_| build.submitted_change.as_ref().is_some_and(TopologyChange::plans_replacements));
    // A replacement's progress is read from the receipts of the Build's own topology attempts: a create
    // an attempt carrying an action made (a recovery's, on the row's executable) replaced nothing, and
    // the path's replacement follows it as the next attempt's (proposal executable-bindings-at-create.md
    // rule 5: "never in the same attempt").
    let topology_steps: Vec<crate::build_state::BuildStepReceipt> = build.steps.iter().filter(|s| !build.attempt_actions.contains_key(&s.attempt)).cloned().collect();
    plan_with(&build.topology, observed, build.action.as_ref(), ActionProgress::of(build), &open_mesh_deletes(build, observed), &BTreeSet::new(), &build.steps, replacing.map(|r| (r, topology_steps.as_slice())), atlas, created)
}

/// The replacements `topology` owes against `observed` and `recorded` before any attempt ran: what a
/// Build that plans replacements ([`TopologyChange::plans_replacements`]) would replace now, in its
/// order. The door reads it to admit the Build (one attempt in flight per mesh, the initial fabric
/// first ready).
pub(crate) fn replacement_operations(topology: &FabricTopology, observed: &Topology, recorded: &Recorded) -> Vec<BuildOperation> {
    replacements_owed(topology, observed, recorded, &[])
}

/// The births a Reconcile's acceptance pins for the replacements it owes (`replacements`, its
/// [`replacement_operations`]; build.md §4.4: "The operation carries the exact birth the gates
/// pinned"), by path, beside the births it deletes (`pinned`): each replaced member's delete acts on
/// the birth the acceptance saw there.
pub(crate) fn replacement_pins(replacements: &[BuildOperation], pinned: &[(PathName, crate::build::PinnedBirth)]) -> Vec<(PathName, crate::build::PinnedBirth)> {
    replacements
        .iter()
        .filter_map(|op| match op {
            BuildOperation::DeleteLiveNode { node, exited: None, birth: Some(b) } if !pinned.iter().any(|(at, _)| at == node) => Some((node.clone(), b.clone())),
            _ => None,
        })
        .collect()
}

/// The properties a Reconcile applies in place on the members it neither creates nor replaces (build.md
/// §1, "The Build applies the diff": "compares each node's entry with what the node's row records and
/// applies every property that differs, each by its own operation"), in path order: `node.config.update`
/// where the entry's `config` differs from the row's, or its `storage` or `placement` from what the row
/// records (a row written before those fields records none and is not compared); `node.tag.create` of
/// the labels the entry names and the row does not carry, `node.tag.delete` of those the row carries
/// and the entry does not. A property this Build already applied to a path is not planned again (the
/// last write wins: build.md §1; node-config-update.md, admitted at any time): its config (with its
/// storage and placement) once its `node.config.update` is `Complete`, its labels once its
/// `node.tag.create` / `node.tag.delete` is, and every property once its create claimed the name (`PrepareStorage`, which writes the
/// entry's onto the row: a plain create, or a replacement's successor whose minted node is the row's,
/// Q305). A write the node or a caller makes after it stands until the next Build. A path `planned` creates, or whose executable the row does not record, is the create's or
/// the replacement's, whose claim writes its properties.
fn properties_owed(topology: &FabricTopology, recorded: &Recorded, steps: &[crate::build_state::BuildStepReceipt], planned: &[BuildOperation]) -> Vec<BuildOperation> {
    let receipted = |operation: &str, step: &str| steps.iter().any(|r| r.operation == operation && r.step == step && r.outcome == crate::build_state::StepOutcome::Complete);
    // The node this Build's create at `path` minted (its AllocateIdentity receipt's output).
    let minted = |path: &PathName| {
        steps
            .iter()
            .filter(|r| r.operation == format!("node.create:{path}") && r.step == "AllocateIdentity" && r.outcome == crate::build_state::StepOutcome::Complete)
            .filter_map(|r| r.output.as_ref()?.get("node_id")?.as_str().map(str::to_string))
            .last()
    };
    // A create that claimed the name wrote its entry's properties (Q305): a plain create's, and a
    // replace's successor's once the node the claim minted is the row's node (an earlier birth at the
    // path, or a recovery that applied no entry, proves nothing). Its predecessor's delete does not
    // undo that evidence.
    let claimed = |path: &PathName, row: &RecordedRow| {
        receipted(&format!("node.create:{path}"), "PrepareStorage")
            && (!steps.iter().any(|r| r.operation == format!("node.delete:{path}")) || minted(path).as_deref() == Some(row.node_id.as_str()))
    };
    let replacing = paths_owed_replacement(topology, recorded);
    let mut ops = Vec::new();
    for (path, spec) in topology.meshes.values().flat_map(|m| m.spec_by_path.iter()) {
        let Some(row) = recorded.get(path) else { continue };
        if replacing.contains(path) || planned.iter().any(|op| op.node() == Some(path)) {
            continue;
        }
        if claimed(path, row) {
            continue;
        }
        let config = row.config.as_ref().is_some_and(|c| *c != spec.config) || row.storage_persistence.is_some_and(|s| s != spec.storage_persistence) || row.placement.is_some_and(|p| p != spec.placement);
        let mut owed = Vec::new();
        if config {
            owed.push(BuildOperation::UpdateConfig { node: path.clone(), entry: None });
        }
        if let Some(tags) = &row.tags {
            let added: BTreeSet<String> = spec.tags.difference(tags).cloned().collect();
            let removed: BTreeSet<String> = tags.difference(&spec.tags).cloned().collect();
            if !added.is_empty() {
                owed.push(BuildOperation::CreateTags { node: path.clone(), labels: added });
            }
            if !removed.is_empty() {
                owed.push(BuildOperation::DeleteTags { node: path.clone(), labels: removed });
            }
        }
        ops.extend(owed.into_iter().filter(|op| !receipted(&op.key(), "Complete")));
    }
    ops
}

/// What the node rows record of one member, for a Reconcile's plan: the node the row at its `path.name`
/// names and the sha256 of the executable it records (`NodeRow::executable`), none for a row written
/// before the field; and the node's own properties the Build compares with its entry (build.md §1, "The
/// Build applies the diff"), each `None` where the row was not read for it or records none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRow {
    /// The node the row names.
    pub node_id: NodeId,
    /// The sha256 its executable binding records.
    pub sha256: Option<String>,
    /// The application's config the row carries.
    pub config: Option<BTreeMap<String, String>>,
    /// The tag labels the row carries.
    pub tags: Option<BTreeSet<String>>,
    /// The storage_persistence the row records.
    pub storage_persistence: Option<rdm_core::meta::StorageMeta>,
    /// The placement the row records.
    pub placement: Option<rdm_core::meta::PlacementMeta>,
}

/// [`RecordedRow`] of each path of a topology that a live row is written at.
pub type Recorded = BTreeMap<PathName, RecordedRow>;

/// What `rows` record of every path of `topology` (proposal executable-bindings-at-create.md rule 2: each
/// attempt computes its remaining work from "the accepted topology, the current node rows, observed
/// state and its receipts"): one point read per path, a `Deleted` row and a path no row is written at
/// left out. A read that fails is named.
pub async fn recorded_executables(rows: &dyn crate::storage::NodesStorage, topology: &FabricTopology) -> Result<Recorded, String> {
    let mut out = Recorded::new();
    for p in topology.meshes.values().flat_map(|m| m.nodes.iter()) {
        let row = crate::node_rows::record_at(rows, p).await.map_err(|e| format!("the row at {p} could not be read for its executable: {e}"))?;
        if let Some(row) = row.filter(|r| r.status != NodeStatus::Deleted) {
            out.insert(p.clone(), RecordedRow { node_id: row.node_id, sha256: row.executable.map(|b| b.sha256), config: Some(row.config), tags: Some(row.tags), storage_persistence: row.storage_persistence, placement: row.placement });
        }
    }
    Ok(out)
}

/// Whether `next` is neither a resize nor an executable replacement of `held` (states.md "Resize and
/// executable replacement"), each path's executable resolved against what its live row records
/// (`recorded`): an entry that names an executable runs it, one that names none runs what its row records.
/// The two hold the same meshes (by name and id) with the same paths, and at every path the same
/// executable (by sha256, as a replacement is planned: [`paths_owed_replacement`]). An entry that names
/// the executable its row already records is no executable replacement; an entry naming another one is,
/// and a path or mesh added or removed is a resize. A path's config, tags, storage and placement are
/// neither: the Build applies them in place (build.md §1; `properties_owed`), and a storage or placement
/// change re-births a path only with its executable (proposal executable-bindings-at-create.md rule 5), so
/// a request changing only those leaves the fabric where it is (states.md "Resize and executable
/// replacement").
pub fn same_on_the_rows(held: &FabricTopology, next: &FabricTopology, recorded: &Recorded) -> bool {
    fn paths(t: &FabricTopology) -> Vec<(&str, &MeshId, &BTreeSet<PathName>)> {
        t.meshes.iter().map(|(name, m)| (name.as_str(), &m.mesh_id, &m.nodes)).collect()
    }
    let runs = |t: &FabricTopology, p: &PathName| target_executable(t, p).map(|b| b.sha256.clone()).or_else(|| recorded.get(p).and_then(|r| r.sha256.clone()));
    paths(held) == paths(next) && next.meshes.values().flat_map(|m| m.nodes.iter()).all(|p| runs(held, p) == runs(next, p))
}

/// The members of `topology` whose live row does not record the sha256 of the executable their entry
/// names (states.md: "During an executable replacement, a member still on its old executable and
/// identity does not satisfy its planned replacement"), in path order. Empty when no entry names one.
pub fn paths_owed_replacement(topology: &FabricTopology, recorded: &Recorded) -> Vec<PathName> {
    topology
        .meshes
        .values()
        .flat_map(|m| m.nodes.iter())
        .filter(|p| target_executable(topology, p).is_some_and(|b| recorded.get(*p).and_then(|r| r.sha256.as_deref()) != Some(b.sha256.as_str())))
        .cloned()
        .collect()
}

/// How far a replacement of the member at `path` got (proposal executable-bindings-at-create.md rule 2:
/// "an attempt interrupted halfway finishes the remaining ... the same way"): read from the receipts,
/// keyed by path.name and step so they name the member, and from the row at the path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplacementProgress {
    /// Nothing of this Build at the path: the member is replaced when its row records another executable
    /// than its entry names.
    Owed,
    /// This Build's `node.delete` began on this birth (its `NodeDeleting`), wrote no `NodeDeleted`
    /// receipt, and the row at the path still names the birth: the delete is finished, then the create.
    Deleting(crate::build::ExitedBirth),
    /// The predecessor deleted (its `NodeDeleted` receipted, or begun and its row no longer live: the
    /// row at the path.name Deleted, the name free) and no create at the path completed since: the
    /// remaining work is the create over that birth.
    Deleted(crate::build::ExitedBirth),
    /// This Build created a node at the path that no later delete of it deleted (a replaced member's
    /// successor, or a path the Build added): never replaced again.
    Created,
}

/// [`ReplacementProgress`] of the member at `path` in `steps`, the latest delete of the path this Build began
/// read against what `recorded` holds at the path.
fn replacement_progress(steps: &[crate::build_state::BuildStepReceipt], path: &PathName, recorded: &Recorded) -> ReplacementProgress {
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
        _ => return if created_from(0) { ReplacementProgress::Created } else { ReplacementProgress::Owed },
    };
    match (created_from(at), deleted) {
        (true, _) => ReplacementProgress::Created,
        (false, true) => ReplacementProgress::Deleted(birth),
        (false, false) => ReplacementProgress::Deleting(birth),
    }
}

/// The creates a replacement owes at the paths whose predecessor its delete began, by path, with the
/// birth each is created over ([`replacement_progress`]): the generic create never plans them.
fn replacement_creates_owed(topology: &FabricTopology, steps: &[crate::build_state::BuildStepReceipt], recorded: &Recorded) -> BTreeMap<PathName, crate::build::ExitedBirth> {
    topology
        .meshes
        .values()
        .flat_map(|m| m.nodes.iter())
        .filter_map(|p| match replacement_progress(steps, p, recorded) {
            ReplacementProgress::Deleting(b) | ReplacementProgress::Deleted(b) => Some((p.clone(), b)),
            ReplacementProgress::Owed | ReplacementProgress::Created => None,
        })
        .collect()
}

/// The replacements still owed (build-deployment.md §3; proposal executable-bindings-at-create.md rule 5),
/// in RDM's order: mesh by mesh in name order; in a mesh its non-primary node-admins, then its primary node-admin, then the
/// nodes of each other type, non-primary before primary, the primaries read from `observed` now. A
/// member is replaced by two operations keyed by its path.name: `node.delete` of the birth the view
/// holds there (the whole stop composed inside it; a primary with a successor moved first by
/// node.primary.update, as every delete of a primary is; R5: a type's only node moves none), then
/// `node.create` at the same path over that birth's kept data dir launching the executable its entry
/// names. A member whose live row records its entry's sha256, and one this Build already replaced or
/// created, is left alone; a member deleted and not yet created is owed its create alone, and one whose
/// delete began and wrote no Deleted row is owed the rest of that delete, then the create.
fn replacements_owed(topology: &FabricTopology, observed: &Topology, recorded: &Recorded, steps: &[crate::build_state::BuildStepReceipt]) -> Vec<BuildOperation> {
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
            let held = observed.node(p).filter(|n| (n.status.is_live() || n.stopped) && n.incarnation_id.is_some());
            let on_entry = |n: &crate::model::Node| recorded.get(p).is_some_and(|r| r.node_id == n.node_id && r.sha256.as_deref() == Some(binding.sha256.as_str()));
            match (replacement_progress(steps, p, recorded), held) {
                // The successor its create launched is live on its entry's sha, its receipt never written:
                // it is the member, and nothing remains for it.
                (ReplacementProgress::Deleted(birth), Some(n)) if n.node_id != birth.node_id && on_entry(n) => {}
                // The predecessor deleted, the successor not yet created: the create alone is owed.
                (ReplacementProgress::Deleted(birth), held) if held.is_none_or(|n| n.node_id == birth.node_id) => {
                    ops.push(BuildOperation::CreateNode { node: p.clone(), over: Some(birth), executable: Some(binding.clone()), entry: None });
                }
                // The predecessor's delete began and its row still names it: the delete of that exact
                // birth is finished, from the stop when the view holds it live or stopped, else targeted at
                // its incarnation, a birth absent from the executor's view rebuilt from its record and its
                // data-root runtime record (as a resumed delete's is), then the create over it.
                (ReplacementProgress::Deleting(birth), held) if held.is_none_or(|n| n.node_id == birth.node_id) => {
                    let pin = crate::build::PinnedBirth { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() };
                    ops.push(match held {
                        Some(_) => BuildOperation::DeleteLiveNode { node: p.clone(), exited: None, birth: Some(pin) },
                        None => BuildOperation::DeleteNode { node: p.clone(), node_id: pin.node_id, from_incarnation: pin.incarnation },
                    });
                    ops.push(BuildOperation::CreateNode { node: p.clone(), over: Some(birth), executable: Some(binding.clone()), entry: None });
                }
                // Replaced or created by this Build: never replaced again. Every create of a topology
                // attempt launches the path's target (the step names it; the launching admin reads the
                // stored entry otherwise); a recovery's create is an action attempt's, never counted here.
                (ReplacementProgress::Created, _) => {}
                (_, Some(n)) if !on_entry(n) => {
                    // The delete and the create over it name the one birth the replacement acts on: the one
                    // the Build's acceptance pinned at the path (build.md §4.4; Q228, decided by Q218 and
                    // Q221), else, for a replacement a later attempt finds, the one the view holds now. A
                    // birth replaced since its pin is refused `birth-replaced` by the executing mesh-primary.
                    let pin = topology.pinned.iter().find(|(at, _)| at == p).map(|(_, b)| b.clone()).unwrap_or_else(|| crate::build::PinnedBirth { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().expect("a held birth names its incarnation") });
                    ops.push(BuildOperation::DeleteLiveNode { node: p.clone(), exited: None, birth: Some(pin.clone()) });
                    ops.push(BuildOperation::CreateNode { node: p.clone(), over: Some(crate::build::ExitedBirth { node_id: pin.node_id, incarnation: pin.incarnation }), executable: Some(binding.clone()), entry: None });
                }
                _ => {}
            }
        }
    }
    ops
}

/// `observed` with every mesh of `rows` (the Mesh rows on the store) it does not hold by id added, for a
/// fabric.delete's plan: the delete deletes every mesh the store holds a row of, never only the meshes the
/// view still projects, which drops a mesh once its members are gone (fabric-delete.md "Failure": "a runtime
/// inside the workflow whose delete has no receipt is never reported deleted"; mesh-delete.md "Resume after
/// the primary's loss": the new executor runs only the tiers without a terminal receipt). A row written Dead
/// is added Dead: the plan resumes it only while its mesh.delete is open in the Build's receipts.
pub fn with_mesh_rows(observed: &Topology, rows: &[crate::storage::MeshRecord]) -> Topology {
    use rdm_core::status::MeshState;
    let mut t = observed.clone();
    // A live row first: of a name the store holds rows of more than one mesh for, the live one is the
    // current mesh, and a Dead row of an earlier mesh of that name is planned only as the one row of it.
    let (live, dead): (Vec<&crate::storage::MeshRecord>, Vec<&crate::storage::MeshRecord>) = rows.iter().partition(|r| !r.dead);
    for r in live.into_iter().chain(dead.into_iter().filter(|d| rows.iter().filter(|r| r.name == d.name).count() == 1)) {
        // A mesh the view holds, by id or by name (a mesh the view names by its name is the current one
        // of that name), is planned as the view holds it.
        if t.meshes.iter().any(|m| m.id.as_ref() == Some(&r.mesh_id) || m.name == r.name) {
            continue;
        }
        let status = match (r.dead, r.state()) {
            (true, _) | (_, Some(MeshState::Dead)) => crate::model::ScopeStatus::Dead,
            (_, Some(MeshState::Leaving)) => crate::model::ScopeStatus::Leaving,
            (_, Some(MeshState::Ready)) => crate::model::ScopeStatus::Ready,
            _ => crate::model::ScopeStatus::Pending,
        };
        t.meshes.push(crate::model::Mesh { id: Some(r.mesh_id.clone()), name: r.name.clone(), status });
    }
    t
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
    held: &BTreeSet<String>,
    steps: &[crate::build_state::BuildStepReceipt],
    replacements: Option<(&Recorded, &[crate::build_state::BuildStepReceipt])>,
    atlas: Option<&AtlasBirth>,
    created: &BTreeSet<String>,
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
    // A standalone connections delete or stop is its one operation and nothing else: the topology is unchanged,
    // and the path it acts on is neither created nor repaired by this attempt. It is planned while
    // the fabric-primary's atlas holds the exact birth at the path (`atlas`, the record its admission
    // read: fabric-atlas.md R-FA1, R-FA3; the view only where no atlas was read), or while its receipts
    // say it began (a stop's view drops the Leaving birth), and not once its `Complete` receipt names
    // the birth.
    if let Some(
        AttemptAction::DeleteConnections { path, node_id, from_incarnation }
        | AttemptAction::Stop { path, node_id, from_incarnation }
        | AttemptAction::Start { path, node_id, from_incarnation }
        | AttemptAction::Delete { path, node_id, from_incarnation },
    ) = action
    {
        // A delete of a stopped `<path>.old` is admitted from the record the fabric-primary holds of
        // that exact birth, never from a view (only the renaming executor's view names the path):
        // it is planned until its `Complete` names the birth. A delete of the last admin of a deleted
        // mesh (mesh-delete.md step 7) is the ordinary node.delete of that exact birth, planned while
        // the view holds it.
        let stopped_old = matches!(action, Some(AttemptAction::Delete { .. })) && path.old;
        let pin = crate::build::PinnedBirth { node_id: node_id.clone(), incarnation: from_incarnation.clone() };
        let held = stopped_old
            || match atlas {
                Some(AtlasBirth(birth)) => birth.as_ref() == Some(&pin),
                None => observed.node(path).is_some_and(|n| pin.is(n)),
            };
        if !progress.finished && (held || progress.started) {
            let (node, node_id, from_incarnation) = (path.clone(), node_id.clone(), from_incarnation.clone());
            ops.push(match action {
                Some(AttemptAction::DeleteConnections { .. }) => BuildOperation::DeleteConnections { node, node_id, from_incarnation },
                Some(AttemptAction::Start { .. }) => BuildOperation::StartNode { node, node_id, from_incarnation },
                Some(AttemptAction::Delete { .. }) if stopped_old => BuildOperation::DeleteNode { node, node_id, from_incarnation },
                Some(AttemptAction::Delete { .. }) => BuildOperation::DeleteLiveNode { node, exited: None, birth: Some(pin) },
                _ => BuildOperation::StopNode { node, node_id, from_incarnation },
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
            ops.push(BuildOperation::DeleteLiveNode { node: path.clone(), exited: Some(exited.clone()), birth: None });
        }
        if done < RecoveryProgress::Recovered {
            ops.push(BuildOperation::CreateNode { node: path.clone(), over: Some(exited), executable: None, entry: None });
        }
        return BuildPlan { operations: ops };
    }
    // Steps are planned in order, each its op's own operation, until the receipts say each is done.
    // The topology is unchanged, the generic loop plans nothing beside them, and no other member is
    // swept. A step on a node an earlier step creates carries no birth: its executor resolves the node
    // at its path when the step runs.
    if let Some(AttemptAction::Steps { steps, pinned }) = action {
        match step_births(steps, pinned) {
            Ok(births) => {
                for (i, (step, birth)) in steps.iter().zip(births).enumerate() {
                    if !progress.steps_done.get(i).copied().unwrap_or(false) {
                        ops.push(BuildOperation::Step { step: step.clone(), birth, entry: None });
                    }
                }
            }
            Err(e) => tracing::info_span!("rdm.node_admin.build.reject.via-unknown-node", reason = %e).in_scope(|| tracing::info!("the Build's steps name a node no step can act on: nothing is planned")),
        }
        return BuildPlan { operations: ops };
    }
    // fabric.delete (lifecycles/fabric-delete.md): run mesh.delete on each mesh leaf-inward (every
    // other mesh in name order first, the fabric-primary's own mesh last), run node.delete of each
    // other mesh's final primary after its mesh.deleted, then release the fabric name and report
    // fabric.deleted while the fabric-primary stays alive for its separate node.delete.
    if topology.meshes.is_empty() && action.is_none() {
        // A storage change the delete interrupted: each mesh-primary is sent `mesh.unblock` before the
        // teardown (storage.md §2 "Changing it").
        if let Some(change) = &topology.storage_change {
            let done = |key: &str| steps.iter().any(|r| r.operation == key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete);
            let mut meshes: Vec<&crate::model::Mesh> = observed.meshes.iter().filter(|m| m.status != crate::model::ScopeStatus::Dead).collect();
            meshes.sort_by(|a, b| a.name.cmp(&b.name));
            for m in meshes.into_iter().filter(|m| !done(&crate::storage_change::unblock_operation(&change.change_id, &m.name))) {
                ops.push(BuildOperation::UnblockStorage { mesh: m.name.clone(), change: change.clone() });
            }
        }
        let fp_mesh = observed.fabric_primary().map(|n| n.mesh.as_str());
        // A deleted mesh the view no longer holds: its final primary's node.delete, once this Build began
        // it, is owed its receipt (fabric-delete.md "Failure": a runtime whose delete has no receipt is
        // never reported deleted).
        let mut begun_ops: std::collections::BTreeSet<&str> = steps.iter().filter(|r| r.operation.starts_with("mesh.delete:")).map(|r| r.operation.as_str()).collect();
        begun_ops.retain(|op| !observed.meshes.iter().any(|m| m.id.as_ref().is_some_and(|id| crate::mesh_delete::operation(id) == *op)));
        for op_key in begun_ops {
            if let Ok(crate::mesh_delete::OwnerResume::Deleted(manifest)) = crate::mesh_delete::owner_resume(steps, op_key) {
                if let Some((node, birth)) = final_primary_birth(&manifest) {
                    let delete_key = format!("node.delete:{node}");
                    let completed = steps.iter().any(|r| r.operation == delete_key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete);
                    let begun = steps.iter().any(|r| r.operation == delete_key);
                    if !completed && begun && Some(node.mesh.as_str()) != fp_mesh {
                        ops.push(BuildOperation::DeleteLiveNode { node, exited: None, birth: Some(birth) });
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
                        if let Some((node, birth)) = final_primary_birth(&manifest) {
                            let delete_key = format!("node.delete:{node}");
                            let completed = steps.iter().any(|r| r.operation == delete_key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete);
                            let held = observed.nodes.iter().any(|n| n.node_id.as_str() == manifest.final_primary.node_id);
                            // A delete this Build began is owed its receipt whether or not the view still
                            // holds the birth (fabric-delete.md "Failure").
                            let begun = steps.iter().any(|r| r.operation == delete_key);
                            if !completed && (held || begun) {
                                ops.push(BuildOperation::DeleteLiveNode { node, exited: None, birth: Some(birth) });
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
    // A replacing attempt (no action) owes its replacements after the topology's creates; a path whose
    // predecessor its delete removed, or began to and its row still names, is created by the
    // replacement, over that predecessor's data dir, never by the generic create below. Every create
    // launches the path's entry's executable.
    let replacements = replacements.filter(|_| action.is_none());
    let creates_owed = replacements.map(|(recorded, steps)| replacement_creates_owed(topology, steps, recorded)).unwrap_or_default();
    for (name, m) in &topology.meshes {
        if !mesh_exists(name) {
            ops.push(BuildOperation::CreateMesh { mesh: name.clone(), mesh_id: m.mesh_id.clone() });
        }
        for p in &m.nodes {
            // A path whose birth the view holds live is satisfied; any other path is planned and the
            // create pipeline's target decides against the world (a running runtime at the path is
            // held, never replaced: `AdminRunner::revise_predecessor`).
            if creates_owed.contains_key(p) {
                continue;
            }
            // A stopped birth (not live, still present: states.md "Stopped nodes") satisfies its path once
            // this Build's create of the path is complete: the create's check reached it and held it
            // (`AdminRunner::create`), and a completed operation is never planned again. A stopped birth
            // no create of this Build has checked is planned, and the check decides: reached, held;
            // unreached, deleted and the path created again.
            let create_done = || {
                let key = format!("node.create:{p}");
                steps.iter().any(|r| r.operation == key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete)
            };
            // A held peer mesh's birth under a silence mark satisfies its path ([`plan_holding`]).
            let held_silent = |n: &crate::model::Node| held.contains(name) && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead);
            if !observed.node(p).is_some_and(|n| n.status.is_live() || (n.stopped && create_done()) || held_silent(n)) {
                // The executable the path's entry names (a request named it).
                ops.push(BuildOperation::CreateNode { node: p.clone(), over: None, executable: target_executable(topology, p).cloned(), entry: None });
            }
        }
    }
    if let Some((recorded, steps)) = replacements {
        ops.extend(replacements_owed(topology, observed, recorded, steps));
        let applied = properties_owed(topology, recorded, steps, &ops);
        ops.extend(applied);
    }
    // A live or stopped node of a kept mesh that the topology no longer names is deleted
    // (a node.delete of a stopped node deletes it: lifecycles/node-delete.md), and so is one its
    // mesh-primary recorded os-stopped, silent as it is (node-delete.md "OS-suspended target": its
    // delete skips the stop by name); a birth that is only silent is never deleted for its silence. A
    // node leaving under another operation is that operation's. A mesh the topology no longer names is deleted whole
    // (every member, dead or live, by the delete pipeline), never member by member.
    // A surplus birth's deletion is its own attempt (no action): an attempt that carries an action
    // repairs exactly its birth.
    // A Build that failed stops at its step, and resuming it or running its compensation steps is the
    // caller's (lifecycles/node-replace.md "Rollback").
    // A birth the fabric-primary's atlas holds a create of this Build at is not surplus: a recipe's
    // `Create` step adds no path to the accepted topology, which only a forward Build or the gossiped
    // topology finalizes (Luke 2026-10-30: drift is decided from the atlas, not the topology).
    let mut extra: Vec<&PathName> = surplus_births(topology, observed).filter(|_| action.is_none()).map(|n| &n.name).filter(|p| !created.contains(&p.to_string())).collect();
    extra.sort();
    // Each delete carries the exact birth it acts on (build.md §4.4): the one its Build pinned at
    // acceptance, else, for a birth that became surplus after it (runtime drift: a failed create's node),
    // the birth this fabric-primary's view holds as it opens the drift attempt. The executing
    // mesh-primary verifies it before any effect either way.
    for p in extra {
        let birth = topology.pinned.iter().find(|(at, _)| at == p).map(|(_, b)| b.clone()).or_else(|| observed.node(p).and_then(crate::build::PinnedBirth::of));
        ops.push(BuildOperation::DeleteLiveNode { node: p.clone(), exited: None, birth });
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
    // A storage change's operations follow every other the Build plans (storage.md §2 "Changing it"):
    // each mesh's `mesh.block`, its mesh-primary's, the fabric-primary's own mesh first; the copy and the
    // switch; each mesh's `mesh.unblock`; the Build's verdict. Each is planned until its `Complete` is
    // receipted.
    if let Some(change) = topology.storage_change.as_ref().filter(|_| action.is_none()) {
        let done = |key: &str| steps.iter().any(|r| r.operation == key && r.step == "Complete" && r.outcome == crate::build_state::StepOutcome::Complete);
        let waited = |key: &str| steps.iter().filter(|r| r.operation == key && r.step == "Waiting").count();
        let fp_mesh = observed.fabric_primary().map(|n| n.mesh.clone());
        let mut meshes: Vec<&String> = topology.meshes.keys().collect();
        meshes.sort_by_key(|m| (Some(m.as_str()) != fp_mesh.as_deref(), m.to_string()));
        for (key_of, block) in [(crate::storage_change::block_operation as fn(&rdm_core::ChangeId, &str) -> String, true), (crate::storage_change::unblock_operation, false)] {
            let mut owed: Vec<&String> = meshes.iter().copied().filter(|m| !done(&key_of(&change.change_id, m))).collect();
            owed.sort_by_key(|m| waited(&key_of(&change.change_id, m)));
            for mesh in owed {
                ops.push(if block { BuildOperation::BlockStorage { mesh: mesh.clone(), change: change.clone() } } else { BuildOperation::UnblockStorage { mesh: mesh.clone(), change: change.clone() } });
            }
            if block && !done(&crate::storage_change::switch_operation(&change.change_id)) {
                ops.push(BuildOperation::SwitchStorage { change: change.clone() });
            }
        }
        if !done(&crate::storage_change::operation(&change.change_id)) {
            ops.push(BuildOperation::FinishStorage { change: change.clone() });
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

    /// The birth `o` holds at `path`, as the gates pin it (build.md §4.4).
    fn pin_of(o: &Topology, path: &str) -> crate::build::PinnedBirth {
        crate::build::PinnedBirth::of(o.node(&path.parse().unwrap()).unwrap()).unwrap()
    }

    /// `t` with every birth of `o` it deletes pinned, as its acceptance pins them.
    fn pinned(mut t: FabricTopology, o: &Topology) -> FabricTopology {
        t.pinned = surplus_births(&t, o).filter_map(|n| crate::build::PinnedBirth::of(n).map(|b| (n.name.clone(), b))).collect();
        t
    }

    /// The node.delete of the birth `o` holds at `path`, pinned to it.
    fn delete_of(o: &Topology, path: &str) -> BuildOperation {
        BuildOperation::DeleteLiveNode { node: path.parse().unwrap(), exited: None, birth: Some(pin_of(o, path)) }
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
        observed(vec![n("mesh1.admin.1", R, true), n("mesh1.admin.2", R, false), n("mesh1.rpc.1", R, true), n("mesh1.rpc.2", R, false), n("mesh1.rpc.3", R, false)], &["mesh1"])
    }

    fn t(meshes: &[(&str, u32, u32)]) -> FabricTopology {
        FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&MeshDesired::of(*m, [(rdm_core::NodeKind::NodeAdmin, *a), (rdm_core::NodeKind::RpcNode, *r)])))).collect() }
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
            (TopologyChange::RemoveNode { node: "mesh1.rpc.9".parse().unwrap() }, BuildReject::UnknownNode { node: "mesh1.rpc.9".into(), step: None }),
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
            plan_with(&compiled, &o, None, ActionProgress::default(), &BTreeSet::new(), &BTreeSet::new(), &[mesh_deleted.clone()], None, None, &BTreeSet::new()).operations,
            vec![delete_of(&o, "mesh1.admin.1"), shutdown("mesh3"), shutdown("mesh2"), deleted],
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
            plan_with(&compiled, &gone, None, ActionProgress::default(), &BTreeSet::new(), &BTreeSet::new(), &[mesh_deleted, begun], None, None, &BTreeSet::new()).operations,
            vec![delete_of(&o, "mesh1.admin.1"), gshutdown("mesh3"), gshutdown("mesh2"), gdeleted],
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
        // A dead (unheard) birth's path is planned again — the create pipeline's target then holds a
        // runtime that still runs; a live node outside the topology is deleted.
        let mut o = mn();
        o.nodes[3].status = NodeStatus::PendingReconnect;
        o.nodes.push(n("mesh1.rpc.7", NodeStatus::Ready, false));
        assert_eq!(
            plan(&pinned(cur.clone(), &o), &o, None).operations,
            vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None, executable: None, entry: None }, delete_of(&o, "mesh1.rpc.7")]
        );
        // A birth surplus after the Build's acceptance (drift) is pinned as the view holds it when planned;
        // one the Build pinned keeps its pin whatever the view holds at the path.
        assert_eq!(plan(&cur, &o, None).operations, vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None, executable: None, entry: None }, delete_of(&o, "mesh1.rpc.7")]);
        let mut later = o.clone();
        later.nodes.iter_mut().find(|x| x.name.to_string() == "mesh1.rpc.7").unwrap().node_id = NodeId::mint();
        assert_eq!(plan(&pinned(cur.clone(), &o), &later, None).operations[1], delete_of(&o, "mesh1.rpc.7"));
        // A mesh outside the topology is deleted after everything else.
        let mut o2 = mn();
        let id2 = MeshId::mint();
        o2.meshes.push(Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready });
        assert_eq!(plan(&cur, &o2, None).operations, vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2 }]);
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
        for (name, status, primary) in [("mesh2.admin.1", NodeStatus::PendingReconnect, true), ("mesh2.rpc.1", NodeStatus::Dead, true), ("mesh2.rpc.2", NodeStatus::PendingReconnect, false)] {
            o.nodes.push(n(name, status, primary));
        }
        let mut cur = t(&[("mesh1", 2, 4), ("mesh2", 1, 2)]);
        cur.meshes.get_mut("mesh2").unwrap().mesh_id = o.meshes[1].id.clone().unwrap();
        cur.meshes.get_mut("mesh1").unwrap().mesh_id = o.meshes[0].id.clone().unwrap();
        let held: BTreeSet<String> = ["mesh2".to_string()].into();
        let created = |p: &str| BuildOperation::CreateNode { node: p.parse().unwrap(), over: None, executable: None, entry: None };
        assert_eq!(plan_holding(&cur, &o, &held).operations, vec![created("mesh1.rpc.4")], "the own mesh's added path alone");
        let grown = t(&[("mesh1", 2, 4), ("mesh2", 1, 3)]);
        let mut grown = grown;
        grown.meshes.get_mut("mesh2").unwrap().mesh_id = o.meshes[1].id.clone().unwrap();
        grown.meshes.get_mut("mesh1").unwrap().mesh_id = o.meshes[0].id.clone().unwrap();
        assert_eq!(plan_holding(&grown, &o, &held).operations, vec![created("mesh1.rpc.4"), created("mesh2.rpc.3")], "a path the request adds in the held mesh is planned");
        let keys: Vec<String> = plan_holding(&cur, &o, &BTreeSet::new()).operations.iter().map(|op| op.key()).collect();
        assert_eq!(keys, ["node.create:mesh1.rpc.4", "node.create:mesh2.admin.1", "node.create:mesh2.rpc.1", "node.create:mesh2.rpc.2"], "not held: the silent paths are planned again");
    }

    /// CONTRACT (mesh-delete.md steps 5-7): a deleted mesh (its row Dead at mesh.deleted) whose last
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
        o.nodes.push(n("mesh2.admin.1", NodeStatus::Ready, true));
        o.meshes.push(Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Dead });
        assert_eq!(plan(&cur, &o, None).operations, vec![], "a deleted mesh with its last admin alive is planned by no Build");
        let open: BTreeSet<String> = ["mesh2".to_string()].into();
        assert_eq!(
            plan_with(&cur, &o, None, ActionProgress::default(), &open, &BTreeSet::new(), &[], None, None, &BTreeSet::new()).operations,
            vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2.clone() }],
            "the Build that admitted its mesh.delete and holds no completion resumes it"
        );
        o.meshes.iter_mut().filter(|m| m.name == "mesh2").for_each(|m| m.status = ScopeStatus::Leaving);
        assert_eq!(plan(&cur, &o, None).operations, vec![BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2 }], "a deleting mesh is planned as before");
    }

    /// CONTRACT (mesh-delete.md step 7: "A separate node.delete deletes the mesh-primary"): the
    /// targeted `Delete` action on a birth of a deleted mesh (no `.old` path: the topology names no
    /// such mesh) is the ordinary node.delete of that exact birth, `DeleteLiveNode`, planned while the
    /// view holds it, and nothing once it left or its `Complete` is receipted; a `Delete` of a stopped
    /// `.old` stays `DeleteNode`. Must NOT happen: the last admin of a deleted mesh deleted through a
    /// copy that skips its stop (`DeleteNode` sends no command to a live process).
    #[test]
    fn a_delete_of_a_deleted_meshs_last_admin_is_the_ordinary_node_delete() {
        let cur = t(&[("mesh1", 2, 3)]);
        let mut o = mn();
        let last = n("mesh2.admin.1", NodeStatus::Ready, true);
        let birth = last.incarnation_id.clone().unwrap();
        let node_id = last.node_id.clone();
        o.nodes.push(last);
        o.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Dead });
        let path: PathName = "mesh2.admin.1".parse().unwrap();
        let delete = AttemptAction::Delete { path: path.clone(), node_id: node_id.clone(), from_incarnation: birth.clone() };
        assert_eq!(plan(&cur, &o, Some(&delete)).operations, vec![delete_of(&o, "mesh2.admin.1")]);
        let finished = ActionProgress { started: true, finished: true, ..ActionProgress::default() };
        assert_eq!(plan_with(&cur, &o, Some(&delete), finished, &BTreeSet::new(), &BTreeSet::new(), &[], None, None, &BTreeSet::new()).operations, vec![], "nothing once its Complete names the birth");
        o.nodes.retain(|n| n.name != path);
        assert_eq!(plan(&cur, &o, Some(&delete)).operations, vec![], "nothing once the view no longer holds it");
        let old: PathName = "mesh1.rpc.3.old".parse().unwrap();
        let stopped = AttemptAction::Delete { path: old.clone(), node_id: node_id.clone(), from_incarnation: birth.clone() };
        assert_eq!(plan(&cur, &o, Some(&stopped)).operations, vec![BuildOperation::DeleteNode { node: old, node_id, from_incarnation: birth }]);
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
        let mut stopped = n("mesh1.rpc.7", NodeStatus::Leaving, false);
        stopped.stopped = true;
        o.nodes.push(stopped);
        assert_eq!(plan(&pinned(cur.clone(), &o), &o, None).operations, vec![delete_of(&o, "mesh1.rpc.7")]);
        let mut leaving = mn();
        leaving.nodes.push(n("mesh1.rpc.7", NodeStatus::Leaving, false));
        assert_eq!(plan(&cur, &leaving, None).operations, vec![], "a departure already under way is not deleted again");
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
        let stopped = o.nodes.iter_mut().find(|x| x.name.to_string() == "mesh1.rpc.1").unwrap();
        stopped.status = NodeStatus::Stopped;
        stopped.stopped = true;
        stopped.is_primary = false;
        let other_removed = pinned(t(&[("mesh1", 2, 2)]), &o);
        let create = BuildOperation::CreateNode { node: "mesh1.rpc.1".parse().unwrap(), over: None, executable: None, entry: None };
        assert_eq!(plan(&other_removed, &o, None).operations, vec![create, delete_of(&o, "mesh1.rpc.3")], "the stopped path's create is planned for its check, beside the other path's delete");
        let held = crate::build_state::BuildStepReceipt {
            mesh: None,
            build_id: BuildId::mint(),
            attempt: 1,
            operation: "node.create:mesh1.rpc.1".into(),
            step: "Complete".into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: None,
            executor: Some("mesh1.admin.1".into()),
        };
        let steps = [held];
        assert_eq!(plan_with(&other_removed, &o, None, ActionProgress::default(), &BTreeSet::new(), &BTreeSet::new(), &steps, None, None, &BTreeSet::new()).operations, vec![delete_of(&o, "mesh1.rpc.3")], "once its Complete is receipted the stopped path's create is never planned again");
        assert_eq!(plan_with(&pinned(t(&[("mesh1", 2, 3)]), &o), &o, None, ActionProgress::default(), &BTreeSet::new(), &BTreeSet::new(), &steps, None, None, &BTreeSet::new()).operations, vec![], "the settled topology with a held stopped birth plans nothing");
        let mut failed = steps[0].clone();
        failed.outcome = crate::build_state::StepOutcome::Failed { reason: "x".into(), code: rdm_core::outcome::Code::Unavailable };
        assert!(plan_with(&other_removed, &o, None, ActionProgress::default(), &BTreeSet::new(), &BTreeSet::new(), &[failed], None, None, &BTreeSet::new()).operations.iter().any(|op| op.key() == "node.create:mesh1.rpc.1"), "a create that did not complete is planned again");
    }

    /// CONTRACT: a replaced predecessor (`<path>.old`) is neither the live birth at `<path>` nor a missing
    /// node nor a surplus one: the plan of the settled topology is empty with it in the view, and a
    /// successor at the path is the one birth the plan counts.
    #[test]
    fn a_renamed_predecessor_changes_no_plan() {
        let cur = t(&[("mesh1", 2, 3)]);
        for status in [NodeStatus::ConnectionsDeleting, NodeStatus::Leaving, NodeStatus::Dead, NodeStatus::Ready] {
            let mut o = mn();
            let mut old = n("mesh1.rpc.2", status, false);
            old.name = old.name.renamed();
            old.stopped = status == NodeStatus::Leaving;
            o.nodes.push(old);
            assert_eq!(plan(&cur, &o, None).operations, vec![], "{status:?}");
            let mut missing = mn();
            let mut old = n("mesh1.rpc.2", status, false);
            old.name = old.name.renamed();
            missing.nodes.retain(|x| x.name.to_string() != "mesh1.rpc.2");
            missing.nodes.push(old);
            assert_eq!(plan(&cur, &missing, None).operations, vec![BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None, executable: None, entry: None }], "the path itself is missing: {status:?}");
        }
    }

    #[test]
    fn an_attempt_action_targets_its_exact_birth_and_never_changes_the_topology() {
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        let node_id = o.node(&path).unwrap().node_id.clone();
        let stop = AttemptAction::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &o, Some(&stop)).operations, vec![BuildOperation::StopNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }]);
        // Once the path runs another birth, the action is satisfied: a re-plan does nothing.
        let mut later = mn();
        later.nodes.iter_mut().find(|x| x.name == path).unwrap().incarnation_id = Some(IncarnationId::mint());
        assert_eq!(plan(&cur, &later, Some(&stop)).operations, vec![]);
        // Another birth under the same incarnation at the path is another birth: nothing is planned.
        let mut same = o.clone();
        same.nodes.iter_mut().find(|x| x.name == path).unwrap().node_id = NodeId::mint();
        assert_eq!(plan(&cur, &same, Some(&stop)).operations, vec![]);
        // Day 0 roots one admin; a topology without an admin is refused.
        assert_eq!(paths(FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()).mesh("mesh1").unwrap()), ["mesh1.admin.1"]);
        let mut bad = cur.clone();
        bad.meshes.get_mut("mesh1").unwrap().nodes.retain(|p| p.kind == NodeKind::RpcNode);
        assert_eq!(bad.validate(), Err(BuildReject::MeshWithoutAdmin { mesh: "mesh1".into() }));
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
            BuildOperation::DeleteLiveNode { node: path.clone(), exited: Some(exited.clone()), birth: None },
            BuildOperation::CreateNode { node: path.clone(), over: Some(exited), executable: None, entry: None },
        ];
        assert_eq!(plan(&cur, &o, Some(&recover)).operations, want);
        // With no action the generic create still plans every missing member.
        assert!(plan(&cur, &o, None).operations.len() >= 3);
    }

    /// CONTRACT (fabric-recovery.md step 3, Q142, Q148: "its node.delete of the exact lost birth writes
    /// that birth's row deleted with no `TerminateRuntime` and releases the name, then node.create at the
    /// path.name"): fabric recovery's backbone attempt plans the same two operations as every recovery,
    /// the delete keyed on the lost birth, then the create; what differs is its delete's runtime step,
    /// which the action carries, and the action crosses a Build message as itself. Must NOT happen: a
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
            vec![BuildOperation::DeleteLiveNode { node: path.clone(), exited: Some(lost.clone()), birth: None }, BuildOperation::CreateNode { node: path.clone(), over: Some(lost), executable: None, entry: None }]
        );
        let wire = crate::wire::action_round_trip(&backbone);
        assert_eq!(wire, backbone, "the backbone action crosses a Build message as itself");
    }

    /// The replace steps of `node` (lifecycles/node-replace.md steps 1-3, the predecessor retained).
    fn replace_steps(node: &PathName) -> Vec<Step> {
        vec![
            Step::DeleteConnections { node: node.clone() },
            Step::LeaveMesh { node: node.clone() },
            Step::Update { node: node.clone(), name: node.renamed() },
            Step::Stop { node: node.renamed() },
            Step::Create { node: node.clone(), executable: None },
        ]
    }

    /// CONTRACT (build.md §1: "Rafka's replace steps (one Build of node primitives ...)";
    /// lifecycles/node-replace.md steps 1-3; the Build carrier: completed steps are not rerun): an
    /// accepted Build of steps pins the birth its predecessor path holds and plans its steps in order, each its
    /// op's own operation (`node.connection.delete:<name>`, `node.mesh.leave:<name>`,
    /// `node.update:<name>`, `node.stop:<name>.old`, `node.create:<name>`), never a replace operation
    /// and never a generic create or delete beside them. The predecessor's steps carry its pinned
    /// birth, followed through its rename to `.old`; the successor's create carries none (its executor
    /// resolves the node its create made at the name the rename released). A step whose `Complete` is
    /// receipted in an attempt that carried these steps is not planned again; a `Complete` of the same
    /// operation in an attempt that carried another action is no progress. Refused by name: no
    /// step, an operation named twice, and a step on a path no birth holds and no earlier step makes
    /// (`unknown-node`, build.md §4.4 gate 4, its detail naming the step and the target; Q223).
    #[test]
    fn a_build_plans_its_steps_in_order_and_never_reruns_a_completed_one() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.1".parse().unwrap();
        let held = o.node(&path).unwrap();
        let pin = PinnedBirth { node_id: held.node_id.clone(), incarnation: held.incarnation_id.clone().unwrap() };
        let steps = replace_steps(&path);
        let action = steps_action(steps.clone(), &o).unwrap();
        assert_eq!(action, AttemptAction::Steps { steps: steps.clone(), pinned: vec![(path.clone(), pin.clone())] }, "only the predecessor's path holds a birth at acceptance");
        let births = [Some(pin.clone()), Some(pin.clone()), Some(pin.clone()), Some(pin.clone()), None];
        let all: Vec<BuildOperation> = steps.iter().zip(births.iter()).map(|(s, b)| BuildOperation::Step { step: s.clone(), birth: b.clone(), entry: None }).collect();
        assert_eq!(plan(&cur, &o, Some(&action)).operations, all);
        let keys: Vec<String> = all.iter().map(BuildOperation::key).collect();
        assert_eq!(
            keys,
            ["node.connection.delete:mesh1.rpc.1", "node.mesh.leave:mesh1.rpc.1", "node.update:mesh1.rpc.1", "node.stop:mesh1.rpc.1.old", "node.create:mesh1.rpc.1"]
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
            reason: AttemptReason::Steps,
            action: Some(action.clone()),
            attempt_actions: [(3, AttemptAction::Stop { path: path.clone(), node_id: pin.node_id.clone(), from_incarnation: pin.incarnation.clone() }), (4, action.clone())].into(),
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
        assert_eq!(plan_for_build(&build(done), &o).operations, vec![], "every step receipted: the Build is done");
        // Refusals.
        assert_eq!(steps_action(vec![], &o), Err(BuildReject::EmptySteps));
        assert_eq!(
            steps_action(vec![Step::Stop { node: path.clone() }, Step::Stop { node: path.clone() }], &o),
            Err(BuildReject::StepRepeated { operation: "node.stop:mesh1.rpc.1".into() })
        );
        // build.md §4.4 gate 4 (Q223): a step on a path no birth holds is refused `unknown-node`, the one
        // name the gate gives, its detail naming the step and the target.
        let unknown = [
            (vec![Step::Start { node: "mesh1.rpc.9".parse().unwrap() }], 1, "mesh1.rpc.9", "no birth at the path"),
            (vec![Step::Update { node: path.clone(), name: path.renamed() }, Step::Stop { node: path.clone() }], 2, "mesh1.rpc.1", "the node left the path at its rename: nothing is at it for the stop"),
        ];
        for (steps, step, node, why) in unknown {
            let reject = steps_action(steps, &o).expect_err(why);
            assert_eq!(reject.reason(), "unknown-node", "{why}: {reject}");
            let detail = reject.to_string();
            assert!(detail.contains(&format!("step {step} ")) && detail.contains(&format!("`{node}`")), "{why}: the detail names the step and the target: {detail}");
        }
    }

    /// CONTRACT: a standalone node.connection.delete or stop plans its one operation (`node.connection.delete:<path>` /
    /// `node.stop:<path>`) while the view holds the exact birth, never a create, a delete or a repair
    /// of another member whatever the view lacks; a path run by another birth plans nothing.
    #[test]
    fn a_connections_delete_or_stop_plans_only_its_operation_for_its_exact_birth() {
        let cur = t(&[("mesh1", 2, 3)]);
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let mut o = mn();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        let node_id = o.node(&path).unwrap().node_id.clone();
        o.nodes.retain(|x| x.name.to_string() != "mesh1.rpc.3");
        let connections = AttemptAction::DeleteConnections { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() };
        let stop = AttemptAction::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() };
        assert_eq!(plan(&cur, &o, Some(&connections)).operations, vec![BuildOperation::DeleteConnections { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }]);
        assert_eq!(plan(&cur, &o, Some(&stop)).operations, vec![BuildOperation::StopNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }]);
        assert_eq!(BuildOperation::DeleteConnections { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }.key(), "node.connection.delete:mesh1.rpc.2");
        assert_eq!(BuildOperation::StopNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }.key(), "node.stop:mesh1.rpc.2");
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

    /// CONTRACT (lifecycles/node-delete.md): the delete of the stopped predecessor at `<path>.old` plans its
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
        let node_id = held.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().node_id.clone();
        let mut stopped = held.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().clone();
        stopped.name = path.clone();
        stopped.status = NodeStatus::Leaving;
        stopped.stopped = true;
        held.nodes.iter_mut().find(|x| x.name.to_string() == "mesh1.rpc.2").unwrap().incarnation_id = Some(IncarnationId::mint());
        held.nodes.push(stopped);
        held.nodes.retain(|x| x.name.to_string() != "mesh1.rpc.3");
        let delete = AttemptAction::Delete { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() };
        let op = vec![BuildOperation::DeleteNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan(&cur, &held, Some(&delete)).operations, op, "only the delete, never a repair of mesh1.rpc.3");
        assert_eq!(op[0].key(), "node.delete:mesh1.rpc.2.old");
        // The Complete commits no output: the attempt's targeted action names the birth.
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
            // Attempt 8 deleted another stopped birth at the path; attempt 9 is this delete.
            attempt_actions: [(8, AttemptAction::Delete { path: path.clone(), node_id: NodeId::mint(), from_incarnation: other.clone() }), (9, delete.clone())].into(),
            attempt_executors: Default::default(),
            opened: 0,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let gone = mn();
        assert_eq!(plan_for_build(&build(vec![]), &gone).operations, op, "the view never names the stopped birth (a fabric-primary that did not run its replace) and nothing began: owed");
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
        let node_id = NodeId::mint();
        // The Complete commits no output: the attempt's targeted action names the birth.
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
            action: Some(AttemptAction::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }),
            // Attempt 6 stopped an earlier birth of the path; attempt 7 is this stop.
            attempt_actions: [(6, AttemptAction::Stop { path: path.clone(), node_id: NodeId::mint(), from_incarnation: other.clone() }), (7, AttemptAction::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() })].into(),
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
        let stop = vec![BuildOperation::StopNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan_for_build(&build(vec![]), &gone).operations, vec![], "the view never held the birth and nothing began");
        assert_eq!(plan_for_build(&build(vec![step(7, "StopNode")]), &gone).operations, stop, "begun, the birth pruned from the view: still owed");
        assert_eq!(plan_for_build(&build(vec![step(7, "StopNode"), step(7, "Complete")]), &gone).operations, vec![], "completed for this birth");
        assert_eq!(plan_for_build(&build(vec![step(6, "StopNode"), step(6, "Complete")]), &gone).operations, vec![], "another birth's stop neither began nor completed this one");
        assert_eq!(plan_for_build(&build(vec![step(6, "StopNode"), step(6, "Complete"), step(7, "StopNode")]), &gone).operations, stop, "another birth's Complete is not this stop's");
    }

    /// CONTRACT (node-stop.md "Completion": the matching node.stopped and the recorded stop receipt;
    /// node-start.md; Q183: callers run node.stop then node.start, and may again): a stop, a start and a
    /// second stop of the same birth in one Build are three requests, each its own run. The second
    /// stop (attempt 6, the latest opened) is planned although attempt 4's stop of the same birth
    /// receipted `Complete`: an earlier run's receipts are not this run's progress. Likewise a second
    /// start after it. Must NOT happen: the second stop answered `Complete` with no `StopNode` sent,
    /// the node left running.
    #[test]
    fn a_second_stop_or_start_of_the_same_birth_in_one_build_is_planned_again() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.3".parse().unwrap();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        let node_id = o.node(&path).unwrap().node_id.clone();
        let receipt = |attempt: u32, operation: &str, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("{operation}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let (stop, start) = (AttemptAction::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }, AttemptAction::Start { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() });
        let build = |action: &AttemptAction, opened: u32, actions: Vec<(u32, AttemptAction)>, steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: opened,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Stop,
            action: Some(action.clone()),
            attempt_actions: actions.into_iter().collect(),
            attempt_executors: Default::default(),
            opened,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let first_runs = vec![receipt(4, "node.stop", "StopNode"), receipt(4, "node.stop", "Complete"), receipt(5, "node.start", "StartNode"), receipt(5, "node.start", "Complete")];
        let second_stop = build(&stop, 6, vec![(4, stop.clone()), (5, start.clone()), (6, stop.clone())], first_runs.clone());
        assert_eq!(plan_for_build(&second_stop, &o).operations, vec![BuildOperation::StopNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }], "the second stop of the live birth is planned");
        let mut stopped = o.clone();
        stopped.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        let mut steps = first_runs;
        steps.extend([receipt(6, "node.stop", "StopNode"), receipt(6, "node.stop", "Complete")]);
        let second_start = build(&start, 7, vec![(4, stop.clone()), (5, start.clone()), (6, stop), (7, start.clone())], steps);
        assert_eq!(plan_for_build(&second_start, &stopped).operations, vec![BuildOperation::StartNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from }], "the second start of the stopped birth is planned");
    }

    /// CONTRACT (fabric-atlas.md R-FA1: the fabric-primary decides from its atlas, "from it, and from
    /// nothing else"; R-FA3: gossip decides nothing; R-FA5: a node.stop sent the moment its node.start
    /// streams `Complete` is accepted): a standalone stop, start, connections delete or delete is planned
    /// from the birth the fabric-primary's atlas row holds at its path, whatever the gossip view holds. A
    /// second stop admitted the moment the start completed, from a view that dropped the birth (the first
    /// stop removed its digest and the restarted birth's has not arrived), is planned. Must NOT happen:
    /// the stop the atlas admitted planned to nothing from the view and answered `Complete`, the node
    /// left running; or a birth the atlas holds deleted or replaced acted on because the view holds it.
    #[test]
    fn a_standalone_action_is_planned_from_the_atlas_birth_whatever_the_view_holds() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 3)]);
        let o = mn();
        let path: PathName = "mesh1.rpc.3".parse().unwrap();
        let from = o.node(&path).unwrap().incarnation_id.clone().unwrap();
        let node_id = o.node(&path).unwrap().node_id.clone();
        let mut gone = o.clone();
        gone.nodes.retain(|x| x.name != path);
        let receipt = |attempt: u32, operation: &str, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("{operation}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let build = |action: &AttemptAction, actions: Vec<(u32, AttemptAction)>, steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 6,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Stop,
            action: Some(action.clone()),
            attempt_actions: actions.into_iter().collect(),
            attempt_executors: Default::default(),
            opened: 6,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let pin = crate::build::PinnedBirth { node_id: node_id.clone(), incarnation: from.clone() };
        let (held, none) = (AtlasBirth(Some(pin.clone())), AtlasBirth(None));
        let replaced = AtlasBirth(Some(crate::build::PinnedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }));
        let (stop, start) = (AttemptAction::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }, AttemptAction::Start { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() });
        // Attempt 4 stopped the birth, attempt 5 started it, attempt 6 is the stop sent the moment the
        // start completed.
        let first_runs = vec![receipt(4, "node.stop", "StopNode"), receipt(4, "node.stop", "Complete"), receipt(5, "node.start", "StartNode"), receipt(5, "node.start", "Complete")];
        let second_stop = build(&stop, vec![(4, stop.clone()), (5, start.clone()), (6, stop.clone())], first_runs.clone());
        let stop_op = vec![BuildOperation::StopNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }];
        assert_eq!(plan_for_build_with(&second_stop, &gone, None, Some(&held)).operations, stop_op, "the atlas holds the birth the view dropped: the stop is planned");
        assert_eq!(plan_for_build_with(&second_stop, &o, None, Some(&none)).operations, vec![], "the atlas holds no birth at the path: nothing, whatever the view holds");
        assert_eq!(plan_for_build_with(&second_stop, &o, None, Some(&replaced)).operations, vec![], "the atlas holds another birth at the path: nothing, whatever the view holds");
        let mut begun = first_runs.clone();
        begun.push(receipt(6, "node.stop", "StopNode"));
        assert_eq!(plan_for_build_with(&build(&stop, vec![(4, stop.clone()), (5, start.clone()), (6, stop.clone())], begun), &gone, None, Some(&none)).operations, stop_op, "begun: owed until its Complete names the birth");
        let mut done = first_runs.clone();
        done.extend([receipt(6, "node.stop", "StopNode"), receipt(6, "node.stop", "Complete")]);
        assert_eq!(plan_for_build_with(&build(&stop, vec![(4, stop.clone()), (5, start.clone()), (6, stop.clone())], done), &gone, None, Some(&held)).operations, vec![], "completed for this birth");
        // A start, a connections delete and a delete of the birth the atlas holds, from a view without it.
        let start_only = build(&start, vec![(6, start.clone())], vec![]);
        assert_eq!(plan_for_build_with(&start_only, &gone, None, Some(&held)).operations, vec![BuildOperation::StartNode { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }], "the start of the birth the atlas holds is planned");
        let connections = AttemptAction::DeleteConnections { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() };
        assert_eq!(plan_for_build_with(&build(&connections, vec![(6, connections.clone())], vec![]), &gone, None, Some(&held)).operations, vec![BuildOperation::DeleteConnections { node: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() }], "the connections delete of the birth the atlas holds is planned");
        let delete = AttemptAction::Delete { path: path.clone(), node_id: node_id.clone(), from_incarnation: from.clone() };
        assert_eq!(plan_for_build_with(&build(&delete, vec![(6, delete.clone())], vec![]), &gone, None, Some(&held)).operations, vec![BuildOperation::DeleteLiveNode { node: path.clone(), exited: None, birth: Some(pin) }], "the delete of the birth the atlas holds is planned");
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
    /// member's mesh-primary; a mesh-primary's own recovery by the admin its mesh elects in its place,
    /// never the exited primary the view may still role. What is left is read from the Build's
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
            let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("node.delete:{path}"), node_id: id.clone(), incarnation: inc.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
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
        // A mesh-primary's own recovery: the view may still role the exited primary; every operation
        // is executed by the admin its mesh elects in its place.
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let mut o = mn();
        o.nodes.iter_mut().find(|x| x.name == admin).unwrap().status = NodeStatus::PendingReconnect;
        let ops = plan_for_build(&build(&admin, &node_id, vec![]), &o).operations;
        assert_eq!(ops.iter().map(|op| op.key()).collect::<Vec<_>>(), vec![format!("node.delete:{admin}"), format!("node.create:{admin}")]);
        for op in &ops {
            assert_eq!(crate::executor::executor_for(op, &o).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.2"), "{} of the exited mesh-primary is executed by its mesh's next admin", op.key());
        }
    }

    /// CONTRACT (lock D): a mesh from its counts carries an explicit create spec for every path, the
    /// kind's migration default; a grow mints a create spec with the path and a shrink drops it; a
    /// path without a create spec, or a create spec without a path, is refused by name; a create spec
    /// set explicitly stands.
    #[test]
    fn every_materialized_path_carries_exactly_one_create_spec() {
        use rdm_core::meta::{NodeSpec, PersistentDisposition, StorageMeta};
        let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 2), (NodeKind::Gateway, 1), (NodeKind::RpcNode, 1)]));
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.spec_by_path.len(), 5);
        let preserve = StorageMeta::Persistent { disposition: PersistentDisposition::Preserve };
        assert_eq!(m.spec(&"mesh1.broker.2".parse().unwrap()).unwrap().storage_persistence, preserve);
        assert_eq!(m.spec(&"mesh1.admin.1".parse().unwrap()).unwrap().storage_persistence, preserve);
        assert_eq!(m.spec(&"mesh1.gateway.1".parse().unwrap()).unwrap().storage_persistence, StorageMeta::Ephemeral);
        assert_eq!(m.spec(&"mesh1.rpc.1".parse().unwrap()).unwrap().storage_persistence, StorageMeta::Ephemeral);
        let mut topology = FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m.clone())].into_iter().collect() };
        assert_eq!(topology.validate(), Ok(()));
        // Grow and shrink keep the create specs in step with the paths.
        let observed = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![],
            nodes: vec![],
        };
        resize(&mut m, &MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 3), (NodeKind::Gateway, 0), (NodeKind::RpcNode, 1)]), &observed);
        assert_eq!(m.nodes.len(), 5);
        assert_eq!(m.spec_by_path.len(), 5);
        assert_eq!(m.spec(&"mesh1.broker.3".parse().unwrap()).unwrap().storage_persistence, preserve, "the grown path carries its default");
        assert!(m.spec(&"mesh1.gateway.1".parse().unwrap()).is_none(), "the dropped path's create spec goes with it");
        // An explicit create spec stands; a path the mesh does not hold is refused.
        let release = NodeSpec { storage_persistence: StorageMeta::Persistent { disposition: PersistentDisposition::Release }, ..NodeSpec::default_for("mesh1.rpc.1".parse().unwrap()) };
        assert_eq!(m.set_spec(release.clone()), Ok(()));
        assert_eq!(m.spec(&"mesh1.rpc.1".parse().unwrap()), Some(&release));
        assert!(matches!(m.set_spec(NodeSpec { path: "mesh1.rpc.9".parse().unwrap(), ..release.clone() }), Err(BuildReject::UnknownNode { .. })));
        // The consistency ratchet.
        m.spec_by_path.remove(&"mesh1.rpc.1".parse().unwrap());
        m.spec_by_path.insert("mesh1.compute.7".parse().unwrap(), NodeSpec { path: "mesh1.compute.7".parse().unwrap(), ..release });
        topology.meshes.insert("mesh1".into(), m);
        assert_eq!(
            topology.validate(),
            Err(BuildReject::NodeSpecMismatch { mesh: "mesh1".into(), missing: vec!["mesh1.rpc.1".into()], extra: vec!["mesh1.compute.7".into()] })
        );
    }

    /// `topology` with every path's entry naming `tag`'s executable of its kind ([`exe`]): what a
    /// complete-topology request naming one executable per kind compiles to over a mesh with two
    /// node-admins.
    fn on_entries(topology: &FabricTopology, tag: &str) -> FabricTopology {
        let mut next = topology.clone();
        for m in next.meshes.values_mut() {
            for (p, c) in m.spec_by_path.iter_mut() {
                c.executable = Some(exe(tag, p.kind));
            }
        }
        next
    }

    /// CONTRACT (build-deployment.md §3: "RDM realizes it as node.delete then node.create at the path, a
    /// new node.id and incarnation, in its order with its guards: a type's non-primary members before its
    /// primary ... a type's only node replaced with no update"; proposal executable-bindings-at-create.md
    /// rule 2: "Each attempt then computes remaining work from the accepted topology, the current node
    /// rows, observed state and its receipts ... an attempt interrupted halfway finishes the remaining
    /// ... the same way"; §1: the ordering rules "become the reconciler's rules for replacement work in
    /// any Build"): every entry names executable "b"; mesh1's members are replaced non-primary admin
    /// first, then its primary admin, then its rpc primary, the rpc node already on "b" left alone; mesh2's
    /// only rpc node is replaced like any member (R5). Each replacement is node.delete of the birth the
    /// view holds, then node.create at its path over that birth, carrying its entry's executable.
    /// Resumed: a member whose delete is receipted owes its create alone, never a create of the generic
    /// loop; a create receipted after it leaves nothing; a successor live on its entry's sha whose create
    /// was never receipted leaves nothing; a member a row records with no executable is owed. A Build
    /// that does not plan replacements plans none. Must NOT happen: a member replaced twice, a primary
    /// replaced before its type's other members, or a successor launched from the binding its Deleted row
    /// records.
    #[test]
    fn a_replacements_plan_is_the_members_off_their_entries_in_order_and_resumes_from_its_receipts() {
        use crate::build_state::{BuildStepReceipt, StepOutcome};
        use NodeStatus::Ready as R;
        let cur = t(&[("mesh1", 2, 2), ("mesh2", 2, 1)]);
        let nodes = vec![n("mesh1.admin.1", R, true), n("mesh1.admin.2", R, false), n("mesh1.rpc.1", R, false), n("mesh1.rpc.2", R, true), n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false), n("mesh2.rpc.1", R, true)];
        let o = observed(nodes.clone(), &["mesh1", "mesh2"]);
        let next = on_entries(&cur, "b");
        let birth = |name: &str, o: &Topology| o.node(&name.parse().unwrap()).map(|n| crate::build::ExitedBirth { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap() }).unwrap();
        let on = |name: &str, tag: Option<&str>, o: &Topology| {
            let node = o.node(&name.parse().unwrap()).unwrap();
            (node.name.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: node.node_id.clone(), sha256: tag.map(|t| format!("{t}-{}", node.name.kind.name())) })
        };
        // Every member on "a", but mesh1.rpc.1 on "b" already and mesh2.admin.2 a row with no executable.
        let mut recorded: Recorded = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.2", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|p| on(p, Some("a"), &o)).collect();
        recorded.extend([on("mesh1.rpc.1", Some("b"), &o), on("mesh2.admin.2", None, &o)]);
        let replace = |name: &str, o: &Topology| {
            let node: PathName = name.parse().unwrap();
            let executable = Some(exe("b", node.kind));
            let b = birth(name, o);
            vec![BuildOperation::DeleteLiveNode { node: node.clone(), exited: None, birth: Some(crate::build::PinnedBirth { node_id: b.node_id.clone(), incarnation: b.incarnation.clone() }) }, BuildOperation::CreateNode { node, over: Some(b), executable, entry: None }]
        };
        let all: Vec<BuildOperation> = ["mesh1.admin.2", "mesh1.admin.1", "mesh1.rpc.2", "mesh2.admin.2", "mesh2.admin.1", "mesh2.rpc.1"].iter().flat_map(|p| replace(p, &o)).collect();
        assert_eq!(replacement_plan(&next, &o, &recorded, vec![]), all, "non-primary admins, the primary admin, then each type non-primary before primary; the member on its entry is left alone");
        assert_eq!(plan(&next, &o, None).operations, vec![], "a Build that does not plan replacements plans none");

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
            let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("node.delete:{name}"), node_id: b.node_id.clone(), incarnation: b.incarnation.clone(), name: name.parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
            step(attempt, &format!("node.delete:{name}"), "NodeDeleted", Some(serde_json::to_value(op).unwrap()))
        };
        // The executor lost after mesh1.admin.2's node.deleted: its create alone is owed, over the deleted
        // birth, launching its entry's executable; the view no longer holds it, and the generic create does not plan it.
        let gone = birth("mesh1.admin.2", &o);
        let mut after_delete = o.clone();
        after_delete.nodes.retain(|n| n.name.to_string() != "mesh1.admin.2");
        let receipts = vec![deleted(1, "mesh1.admin.2", &gone), step(1, "node.delete:mesh1.admin.2", "Complete", None)];
        let owed = replacement_plan(&next, &after_delete, &recorded, receipts.clone());
        assert_eq!(owed[0], BuildOperation::CreateNode { node: "mesh1.admin.2".parse().unwrap(), over: Some(gone.clone()), executable: Some(exe("b", NodeKind::NodeAdmin)), entry: None }, "{owed:?}");
        assert_eq!(owed.iter().filter(|op| op.key() == "node.create:mesh1.admin.2").count(), 1, "one create of the member, the replacement's: {owed:?}");
        assert_eq!(owed[1..], all[2..], "the rest as before");
        // Killed after the successor's check-in and before its receipt: the successor is live on its
        // entry's sha, and nothing remains for it.
        let mut successor = n("mesh1.admin.2", R, false);
        successor.node_id = NodeId::mint();
        let mut live_again = after_delete.clone();
        live_again.nodes.push(successor.clone());
        let mut on_entry = recorded.clone();
        on_entry.insert("mesh1.admin.2".parse().unwrap(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: successor.node_id.clone(), sha256: Some("b-node_admin".into()) });
        assert_eq!(replacement_plan(&next, &live_again, &on_entry, receipts.clone()), all[2..].to_vec(), "a live successor on its entry is the member");
        // Its create receipted: never replaced again, whatever its row reads.
        let mut done = receipts;
        done.push(step(2, "node.create:mesh1.admin.2", "Complete", None));
        assert_eq!(replacement_plan(&next, &live_again, &recorded, done), all[2..].to_vec(), "no member is replaced twice");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 2: "Each attempt then computes remaining
    /// work from the accepted topology, the current node rows, observed state and its receipts"; rule 5:
    /// "node.delete then node.create at the path ... The successor's storage follows its create spec's
    /// `storage`: `Persistent` inherits the predecessor's data dir"): mesh2.rpc.1's delete began
    /// (`NodeDeleting` of its birth, its `TerminateRuntime` receipted) and no `NodeDeleted` was receipted,
    /// and the view no longer holds the birth live. Its row Deleted: the create alone is owed, over that
    /// birth. Its row still live naming that birth: the delete of that exact birth is finished first
    /// (targeted at its incarnation, as a resumed delete whose birth the view no longer holds is), then
    /// the create over it. A view that still holds the birth Leaving plans the same.
    /// Must NOT happen: the generic create at the path (over a newly allocated data dir), or two creates.
    #[test]
    fn a_replacements_delete_without_its_node_deleted_receipt_is_resumed_from_its_receipts_and_its_row() {
        use crate::build_state::{BuildStepReceipt, StepOutcome};
        use NodeStatus::Ready as R;
        let cur = t(&[("mesh2", 2, 1)]);
        let nodes = vec![n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false), n("mesh2.rpc.1", R, true)];
        let o = observed(nodes, &["mesh2"]);
        let next = on_entries(&cur, "b");
        let path: PathName = "mesh2.rpc.1".parse().unwrap();
        let node = o.node(&path).unwrap().clone();
        let birth = crate::build::ExitedBirth { node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().unwrap() };
        let mut recorded: Recorded = o.nodes.iter().filter(|n| n.name.kind == NodeKind::NodeAdmin).map(|n| (n.name.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: n.node_id.clone(), sha256: Some(format!("b-{}", n.name.kind.name())) })).collect();
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
        let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt: 1, operation: "node.delete:mesh2.rpc.1".into(), node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        let receipts = vec![step("NodeDeleting", Some(serde_json::to_value(op).unwrap())), step("StopNode", None), step("AwaitNodeLeft", None), step("TerminateRuntime", None)];
        let create = BuildOperation::CreateNode { node: path.clone(), over: Some(birth.clone()), executable: Some(exe("b", NodeKind::RpcNode)), entry: None };
        let mut gone = o.clone();
        gone.nodes.retain(|n| n.name != path);
        let mut leaving = o.clone();
        leaving.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Leaving);
        // The row Deleted (no live row at the path): the create alone, over the deleted birth.
        for view in [&gone, &leaving] {
            assert_eq!(replacement_plan(&next, view, &recorded, receipts.clone()), vec![create.clone()], "the row Deleted: the remaining work is the create");
        }
        // The row still live, naming the birth on its old sha: the delete of that exact birth is finished, then the create.
        recorded.insert(path.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: birth.node_id.clone(), sha256: Some("a-rpc_node".into()) });
        let finish = BuildOperation::DeleteNode { node: path.clone(), node_id: birth.node_id.clone(), from_incarnation: birth.incarnation.clone() };
        for view in [&gone, &leaving] {
            assert_eq!(replacement_plan(&next, view, &recorded, receipts.clone()), vec![finish.clone(), create.clone()], "the row live: the delete is finished, then the create");
        }
    }

    /// CONTRACT (states.md "Resize and executable replacement": "During an executable replacement, a
    /// member still on its old executable and identity does not satisfy its planned replacement ... the
    /// fabric cannot change, until the replacement's last planned birth checks in"): the members whose
    /// live row does not record their entry's sha256 are named, in path order, a row with no executable
    /// and a path no live row is written at among them; a topology whose entries name none names none.
    #[test]
    fn the_members_off_their_entries_are_named_until_each_row_records_its_sha() {
        let next = on_entries(&t(&[("mesh1", 2, 1)]), "b");
        let row = |tag: Option<&str>, kind: &str| RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: NodeId::mint(), sha256: tag.map(|t| format!("{t}-{kind}")) };
        let mut recorded: Recorded = [("mesh1.admin.1".parse().unwrap(), row(Some("b"), "node_admin")), ("mesh1.admin.2".parse().unwrap(), row(None, "node_admin")), ("mesh1.rpc.1".parse().unwrap(), row(Some("a"), "rpc_node"))].into();
        assert_eq!(paths_owed_replacement(&next, &recorded).iter().map(ToString::to_string).collect::<Vec<_>>(), ["mesh1.admin.2", "mesh1.rpc.1"]);
        recorded.insert("mesh1.admin.2".parse().unwrap(), row(Some("b"), "node_admin"));
        recorded.insert("mesh1.rpc.1".parse().unwrap(), row(Some("b"), "rpc_node"));
        assert_eq!(paths_owed_replacement(&next, &recorded), Vec::<PathName>::new());
        recorded.remove(&"mesh1.rpc.1".parse::<PathName>().unwrap());
        assert_eq!(paths_owed_replacement(&next, &recorded).iter().map(ToString::to_string).collect::<Vec<_>>(), ["mesh1.rpc.1"], "a path no live row is written at is not on its entry");
        assert_eq!(paths_owed_replacement(&t(&[("mesh1", 2, 1)]), &Recorded::new()), Vec::<PathName>::new(), "no entry names one, no blocker");
    }

    /// The binding `tag` names for the node at `path`: its kind's launch id, a path and sha of `tag`.
    fn exe(tag: &str, kind: NodeKind) -> rdm_core::binding::Binding {
        rdm_core::binding::Binding { launch_id: kind.name().into(), executable: format!("/{tag}/{}", kind.name()).into(), sha256: format!("{tag}-{}", kind.name()), image: None }
    }

    /// `path`'s entry in a complete-topology request: its kind's default create spec on `tag`'s executable.
    fn entry(path: &str, tag: Option<&str>) -> NodeSpec {
        let p: PathName = path.parse().unwrap();
        let executable = tag.map(|t| exe(t, p.kind));
        NodeSpec { executable, ..NodeSpec::default_for(p) }
    }

    fn reconcile(nodes: Vec<NodeSpec>) -> TopologyChange {
        TopologyChange::Reconcile { topology_storage_provider: None, based_on: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b0")).unwrap(), nodes }
    }

    /// What the rows record: every live member of `o` on `tag`'s executable of its kind.
    fn rows_on(o: &Topology, tag: &str) -> Recorded {
        o.nodes.iter().map(|n| (n.name.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: n.node_id.clone(), sha256: Some(format!("{tag}-{}", n.name.kind.name())) })).collect()
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
        cur.meshes.get_mut("mesh1").unwrap().spec_by_path.get_mut(&held).unwrap().executable = Some(exe("a", NodeKind::RpcNode));
        let request = vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), entry("mesh1.rpc.1", None), entry("mesh1.rpc.2", Some("b")), entry("mesh2.admin.1", Some("b")), entry("mesh2.rpc.1", Some("b"))];
        let next = compile(&cur, &reconcile(request), &mn()).unwrap();
        assert_eq!(next.meshes.keys().collect::<Vec<_>>(), ["mesh1", "mesh2"], "mesh3, left out, is gone (its mesh.delete)");
        assert_eq!(paths(next.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2"], "mesh1.rpc.3, left out, is removed");
        assert_eq!(next.mesh("mesh1").unwrap().mesh_id, cur.mesh("mesh1").unwrap().mesh_id, "a kept mesh keeps its id");
        assert!(next.meshes.values().all(|m| m.mesh_id != next.mesh("mesh2").unwrap().mesh_id || m.name == "mesh2") && cur.meshes.values().all(|m| m.mesh_id != next.mesh("mesh2").unwrap().mesh_id), "mesh2 enters under an id of its own");
        assert_eq!(paths(next.mesh("mesh2").unwrap()), ["mesh2.admin.1", "mesh2.rpc.1"], "a mesh named for the first time, with its first node-admin");
        let config = |p: &str| next.mesh(&p.parse::<PathName>().unwrap().mesh).unwrap().spec(&p.parse().unwrap()).unwrap().executable.clone();
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
            (vec![entry("mesh1.admin.1", None), entry("mesh1.rpc.1.old", Some("b"))], BuildReject::UnknownNode { node: "mesh1.rpc.1.old".into(), step: None }),
            (vec![entry("mesh1.admin.1", None), entry("mesh2.rpc.1", Some("b"))], BuildReject::MeshWithoutAdmin { mesh: "mesh2".into() }),
        ] {
            assert_eq!(compile(&cur, &reconcile(request.clone()), &mn()), Err(want), "{request:?}");
        }
        assert!(BuildReject::NoNodeListed.to_string().contains("fabric.delete"), "the refusal names fabric.delete");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: the request is "an array of nodes, each
    /// identified by its `path.name` ... with `tags`, `storage`, `placement` and `executable`";
    /// node-rpc-envelope.md node.tags: "A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading
    /// '.'"; build.md §1, Q166: "The reconciler compares each node's entry with what the node's row records
    /// and applies every property that differs"): a created path's entry keeps the tag labels the request
    /// lists for it, which the node carries from its claim; a held path's entry takes the labels the
    /// request lists (applied to the node's tag rows by the Build, `properties_owed`), so the same request
    /// changes nothing and another request's labels stand on the entry; a label outside the grammar is
    /// refused by name, `tags-refused`, nothing compiled. Must NOT happen: a request's tags dropped from its
    /// entry, or a label the tags index cannot key accepted.
    #[test]
    fn a_request_entrys_tags_stand_on_its_entry_and_a_label_outside_the_grammar_is_refused_by_name() {
        let cur = t(&[("mesh1", 2, 3)]);
        let tagged = |path: &str, tags: &[&str]| NodeSpec { tags: tags.iter().map(|t| t.to_string()).collect(), ..entry(path, Some("b")) };
        let next = compile(&cur, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &["zone_a", "v1.2~rc"])]), &mn()).unwrap();
        let p: PathName = "mesh1.rpc.4".parse().unwrap();
        assert_eq!(next.mesh("mesh1").unwrap().spec(&p).unwrap().tags, ["v1.2~rc".to_string(), "zone_a".to_string()].into(), "the created path's entry keeps its tags");
        let again = compile(&next, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &[])]), &mn()).unwrap();
        assert!(again.mesh("mesh1").unwrap().spec(&p).unwrap().tags.is_empty(), "a held path takes the request's tags: none");
        let same = compile(&next, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &["zone_a", "v1.2~rc"])]), &mn()).unwrap();
        assert_eq!(same, next, "the same request changes nothing");
        let other = compile(&next, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), tagged("mesh1.rpc.4", &["zone_b"])]), &mn()).unwrap();
        assert_eq!(other.mesh("mesh1").unwrap().spec(&p).unwrap().tags, ["zone_b".to_string()].into(), "another request's labels stand on the entry");
        let refused = compile(&cur, &reconcile(vec![entry("mesh1.admin.1", None), tagged("mesh1.rpc.4", &["Zone A"])]), &mn());
        assert!(matches!(&refused, Err(BuildReject::TagsRefused { node, .. }) if node == "mesh1.rpc.4"), "{refused:?}");
        assert_eq!(refused.unwrap_err().reason(), "tags-refused");
    }

    /// CONTRACT (build.md §1, Q166, "The Build applies the diff": "The reconciler compares each node's
    /// entry with what the node's row records and applies every property that differs, each by its own
    /// operation"; `config` "in place, as node.config.update applies a map", `tags` "in place: the node's
    /// tag rows", `storage`, `placement` "onto the row"): mesh1.rpc.1's entry names config {zone: b}, the
    /// label x and Persistent storage, its row {zone: a}, the label y and Ephemeral: the plan is
    /// `node.config.update`, `node.tag.create {x}` and `node.tag.delete {y}` of mesh1.rpc.1. mesh1.admin.1's
    /// row, written before the storage field, records none, and its entry differs from nothing: no
    /// operation. A storage-only difference is `node.config.update` alone (the storage rides its row
    /// write). An operation this Build's receipts name Complete is not planned again (a later
    /// node.config.update is the last write), and a path the plan creates is the create's. Must NOT
    /// happen: an operation for a property that does not differ, one for a row that records none, or one
    /// planned again after its Complete.
    #[test]
    fn a_reconcile_applies_each_property_that_differs_from_the_row_by_its_own_operation() {
        use crate::build_state::{BuildStepReceipt, StepOutcome};
        use rdm_core::meta::{PersistentDisposition, StorageMeta};
        let cur = t(&[("mesh1", 1, 1)]);
        let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let mut topology = cur.clone();
        let persistent = StorageMeta::Persistent { disposition: PersistentDisposition::Preserve };
        let labels = |l: &[&str]| l.iter().map(|x| x.to_string()).collect::<BTreeSet<String>>();
        let zone = |z: &str| [("zone".to_string(), z.to_string())].into_iter().collect::<BTreeMap<String, String>>();
        topology.meshes.get_mut("mesh1").unwrap().set_spec(NodeSpec { config: zone("b"), tags: labels(&["x"]), storage_persistence: persistent, ..NodeSpec::default_for(rpc.clone()) }).unwrap();
        let row = |config: BTreeMap<String, String>, tags: BTreeSet<String>, storage: StorageMeta| RecordedRow { node_id: NodeId::mint(), sha256: None, config: Some(config), tags: Some(tags), storage_persistence: Some(storage), placement: Some(Default::default()) };
        let mut recorded: Recorded = [(rpc.clone(), row(zone("a"), labels(&["y"]), StorageMeta::Ephemeral))].into();
        recorded.insert(admin.clone(), RecordedRow { node_id: NodeId::mint(), sha256: None, config: Some(BTreeMap::new()), tags: Some(BTreeSet::new()), storage_persistence: None, placement: None });
        let all = vec![BuildOperation::UpdateConfig { node: rpc.clone(), entry: None }, BuildOperation::CreateTags { node: rpc.clone(), labels: labels(&["x"]) }, BuildOperation::DeleteTags { node: rpc.clone(), labels: labels(&["y"]) }];
        assert_eq!(properties_owed(&topology, &recorded, &[], &[]), all, "each property that differs, by its own operation");
        assert_eq!(all.iter().map(BuildOperation::key).collect::<Vec<_>>(), ["node.config.update:mesh1.rpc.1", "node.tag.create:mesh1.rpc.1", "node.tag.delete:mesh1.rpc.1"]);
        let storage_only: Recorded = [(rpc.clone(), row(zone("b"), labels(&["x"]), StorageMeta::Ephemeral))].into();
        assert_eq!(properties_owed(&topology, &storage_only, &[], &[]), vec![BuildOperation::UpdateConfig { node: rpc.clone(), entry: None }], "a storage-only difference is the config op's row write");
        let same: Recorded = [(rpc.clone(), row(zone("b"), labels(&["x"]), persistent))].into();
        assert_eq!(properties_owed(&topology, &same, &[], &[]), vec![], "nothing differs");
        let complete = BuildStepReceipt { mesh: None, build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), attempt: 1, operation: "node.config.update:mesh1.rpc.1".into(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
        assert_eq!(properties_owed(&topology, &recorded, &[complete], &[]), all[1..].to_vec(), "a Complete operation is not planned again");
        let created = [BuildOperation::CreateNode { node: rpc.clone(), over: None, executable: None, entry: None }];
        assert_eq!(properties_owed(&topology, &recorded, &[], &created), vec![], "a path the plan creates is the create's");
        // An attempt's plan through the journal and back: the operations keep their names.
        let json = serde_json::to_value(&all).unwrap();
        assert_eq!(json[0]["op"], "update_config");
        assert_eq!(serde_json::from_value::<Vec<BuildOperation>>(json).unwrap(), all);
    }

    /// CONTRACT (build.md §1, "The Build applies the diff", and its last line: "A node's `config` and
    /// `tags` change at any time between Builds as well, through `node.config.update` and `node.tag.*`;
    /// the last write wins"; node-config-update.md: "It is admitted at any time (Q166) ... never refused
    /// because a Build is running: ... any interleaving with a Build is a serial order of the two"): a
    /// property this Build already applied to a path is not applied again by a later attempt of it,
    /// whether its create's claim wrote it (`PrepareStorage` of the path's `node.create`, the entry's
    /// config, tags, storage and placement onto the row) or its own `node.config.update` /
    /// `node.tag.*` completed; a write the node or a caller made after it stands. mesh1.rpc.1's entry
    /// names config {} and no label; its row carries {port: 7} and the label y, the node's own writes
    /// after this Build created it: no operation. A replacement's successor's claim wrote its entry's
    /// properties too (Q305, node-replace.md Properties): once the node its AllocateIdentity minted is the
    /// row's, the predecessor's delete in the same Build does not undo that, and the successor's own
    /// writes since stand; a create whose minted node is not the row's (an earlier birth at the path, or
    /// a create that recorded no identity) proves nothing, and the entry's are applied. A later Build,
    /// whose receipts hold none of these, applies the entry's config and tags. Must NOT happen: a later
    /// attempt of the Build that created the node, a replacement's successor included, writing the
    /// node's own config back to the entry's.
    #[test]
    fn a_property_this_build_applied_is_not_applied_again_and_a_later_write_stands() {
        let cur = t(&[("mesh1", 1, 1)]);
        let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
        let labels = |l: &[&str]| l.iter().map(|x| x.to_string()).collect::<BTreeSet<String>>();
        let port = [("port".to_string(), "7".to_string())].into_iter().collect::<BTreeMap<String, String>>();
        let own: Recorded = [(rpc.clone(), RecordedRow { node_id: NodeId::mint(), sha256: None, config: Some(port.clone()), tags: Some(labels(&["y"])), storage_persistence: Some(rdm_core::meta::StorageMeta::Ephemeral), placement: Some(Default::default()) })].into();
        let create = "node.create:mesh1.rpc.1";
        let claimed = vec![receipt(1, create, "AllocateIdentity", None), receipt(1, create, "PrepareStorage", None), receipt(1, create, "Complete", None)];
        assert_eq!(properties_owed(&cur, &own, &claimed, &[]), vec![], "the create's claim applied the entry: the node's own writes since stand");
        let cut = vec![receipt(1, create, "AllocateIdentity", None), receipt(1, create, "PrepareStorage", None)];
        assert_eq!(properties_owed(&cur, &own, &cut, &[]), vec![], "an executor lost after the claim: the claim applied the entry all the same");
        let unclaimed = vec![receipt(1, create, "AllocateIdentity", None)];
        let all = vec![BuildOperation::UpdateConfig { node: rpc.clone(), entry: None }, BuildOperation::DeleteTags { node: rpc.clone(), labels: labels(&["y"]) }];
        assert_eq!(properties_owed(&cur, &own, &unclaimed, &[]), all, "a create that never claimed the name applied nothing");
        let untagged = vec![receipt(1, "node.tag.delete:mesh1.rpc.1", "Complete", None)];
        assert_eq!(properties_owed(&cur, &own, &untagged, &[]), all[..1].to_vec(), "the label this Build deleted, written again since, stands; the config it never applied is applied");
        let b = crate::build::ExitedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let successor = own[&rpc].node_id.clone();
        let minted = |id: &NodeId| receipt(2, create, "AllocateIdentity", Some(serde_json::json!({ "node_id": id.as_str() })));
        let replaced = vec![deleted_receipt(1, "mesh1.rpc.1", &b), minted(&successor), receipt(2, create, "PrepareStorage", None), receipt(2, create, "Complete", None)];
        assert_eq!(properties_owed(&cur, &own, &replaced, &[]), vec![], "a replacement's successor's claim wrote its entry's properties: its own writes since stand");
        let other = vec![deleted_receipt(1, "mesh1.rpc.1", &b), minted(&NodeId::mint()), receipt(2, create, "PrepareStorage", None)];
        assert_eq!(properties_owed(&cur, &own, &other, &[]), all, "a create whose minted node is not the row's proves nothing of it: the entry's are applied");
        let unnamed = vec![deleted_receipt(1, "mesh1.rpc.1", &b), receipt(2, create, "PrepareStorage", None)];
        assert_eq!(properties_owed(&cur, &own, &unnamed, &[]), all, "a replacement's create that recorded no identity proves nothing: the entry's are applied");
        assert_eq!(properties_owed(&cur, &own, &[], &[]), all, "a later Build applies the entry's config and tags");
    }

    /// CONTRACT (Q166, build.md §1: "`config` is the application's map and nothing else (RDM never reads
    /// it)"): a created path's create spec names config {zone: a}; the accepted entry carries it, and the
    /// journal's form keeps it. Must NOT happen: a created path's config dropped.
    #[test]
    fn a_created_paths_config_is_on_its_entry() {
        let cur = t(&[("mesh1", 2, 3)]);
        let configured = |config: BTreeMap<String, String>| NodeSpec { config, ..entry("mesh1.rpc.4", Some("b")) };
        let zone: BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        let next = compile(&cur, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.admin.2", None), configured(zone.clone())]), &mn()).unwrap();
        let p: PathName = "mesh1.rpc.4".parse().unwrap();
        assert_eq!(next.mesh("mesh1").unwrap().spec(&p).unwrap().config, zone, "the created path's entry carries its config");
        assert_eq!(serde_json::from_value::<FabricTopology>(serde_json::to_value(&next).unwrap()).unwrap(), next, "the journal's form keeps it");
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
        use NodeStatus::Ready as R;
        let cur = t(&[("mesh1", 1, 1), ("mesh2", 2, 0)]);
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.rpc.1", R, true), n("mesh2.admin.1", R, true), n("mesh2.admin.2", R, false)], &["mesh1", "mesh2"]);
        let rows = rows_on(&o, "a");
        let request = |admin: &str| reconcile(vec![entry("mesh1.admin.1", Some(admin)), entry("mesh1.rpc.1", Some("a")), entry("mesh2.admin.1", Some("b")), entry("mesh2.admin.2", Some("b"))]);
        let next = compile_against(&cur, &request("b"), &o, &rows).unwrap();
        assert_eq!(paths(next.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1"], "mesh1 gains its second node-admin at acceptance");
        assert_eq!(next.mesh("mesh1").unwrap().spec(&"mesh1.admin.2".parse().unwrap()).unwrap().executable, Some(exe("b", NodeKind::NodeAdmin)), "on the new executable");
        assert_eq!(paths(next.mesh("mesh2").unwrap()), ["mesh2.admin.1", "mesh2.admin.2"], "a mesh with two admins gains none");
        let same = compile_against(&cur, &request("a"), &o, &rows).unwrap();
        assert_eq!(paths(same.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.rpc.1"], "an admin on the executable its row records is not replaced: no second admin");
        let ops = replacement_plan(&next, &o, &rows, vec![]);
        let created = ops.iter().position(|op| op == &BuildOperation::CreateNode { node: "mesh1.admin.2".parse().unwrap(), over: None, executable: Some(exe("b", NodeKind::NodeAdmin)), entry: None }).unwrap_or_else(|| panic!("the added admin is created: {ops:?}"));
        let replaced = ops.iter().position(|op| op == &delete_of(&o, "mesh1.admin.1")).unwrap_or_else(|| panic!("the original is replaced: {ops:?}"));
        assert!(created < replaced, "the added admin is created first: {ops:?}");
    }

    /// CONTRACT (build.md §1, "The Build applies the diff" and "the last write wins"; node-config-update.md:
    /// "It is admitted at any time (Q166) ... never refused because a Build is running"): a Reconcile
    /// whose attempt 1 created mesh1.rpc.1 (its entry's config {}) and whose rpc node then wrote its own
    /// bound port into its config through `node.config.update`, plans its attempt 2 with no
    /// `node.config.update` of mesh1.rpc.1 and no other operation: the node keeps its write, nothing is
    /// created or deleted. A later Build whose entry names another config applies it. Must NOT happen:
    /// attempt 2 writing the node's config back to {} (rafka 141, Build 7fzjdqr1sh66).
    #[test]
    fn a_later_attempt_of_the_build_that_created_a_node_keeps_the_config_the_node_wrote_since() {
        use NodeStatus::Ready as R;
        let cur = t(&[("mesh1", 1, 1)]);
        let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.rpc.1", R, true)], &["mesh1"]);
        let row = |config: BTreeMap<String, String>| RecordedRow { node_id: NodeId::mint(), sha256: None, config: Some(config), tags: Some(BTreeSet::new()), storage_persistence: Some(rdm_core::meta::StorageMeta::Ephemeral), placement: Some(Default::default()) };
        let port = [("port".to_string(), "7".to_string())].into_iter().collect::<BTreeMap<String, String>>();
        let mut recorded: Recorded = [(rpc.clone(), row(port))].into();
        recorded.insert("mesh1.admin.1".parse().unwrap(), RecordedRow { storage_persistence: None, placement: None, ..row(BTreeMap::new()) });
        let create = "node.create:mesh1.rpc.1";
        let attempt1: Vec<_> = ["AllocateIdentity", "PrepareStorage", "DeployRuntime", "WaitForNodeReady", "Complete"].iter().map(|s| receipt(1, create, s, None)).collect();
        assert_eq!(replacement_plan(&cur, &o, &recorded, attempt1), vec![], "attempt 2 plans nothing: the node's own config stands");
        let mut later = cur.clone();
        let zone = [("zone".to_string(), "b".to_string())].into_iter().collect::<BTreeMap<String, String>>();
        later.meshes.get_mut("mesh1").unwrap().set_spec(NodeSpec { config: zone, ..NodeSpec::default_for(rpc.clone()) }).unwrap();
        assert_eq!(replacement_plan(&later, &o, &recorded, vec![]), vec![BuildOperation::UpdateConfig { node: rpc, entry: None }], "a later Build naming another config applies it");
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
        plan_with_replacements(&build, o, recorded).operations
    }

    /// CONTRACT (build.md §4.4 closing rule; Q218: "the fabric-primary pins (node_id, incarnation) at
    /// acceptance"; Q228, decided by Q218 and Q221): a Reconcile accepted on a member whose executable it
    /// changes pins the birth its acceptance saw at that path, and the replacement's delete and the create
    /// over it carry that birth. The birth replaced between acceptance and the delete (recovered at the
    /// same path.name under a new node id), the delete still carries the acceptance pin, so the executing
    /// mesh-primary refuses it `birth-replaced` and the replacement birth is untouched.
    /// Must NOT happen: the delete pinned to whatever birth the view holds when the attempt is planned.
    #[test]
    fn a_replacements_delete_carries_the_birth_its_acceptance_pinned() {
        use NodeStatus::Ready as R;
        let desired = MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 1)]);
        let cur = FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), MeshTopology::of(&desired))].into() };
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.gateway.1", R, true)], &["mesh1"]);
        let rows = rows_on(&o, "a");
        let request: Vec<NodeSpec> = o.nodes.iter().map(|x| entry(&x.name.to_string(), Some(if x.name.kind == NodeKind::Gateway { "b" } else { "a" }))).collect();
        let mut next = compile_against(&cur, &reconcile(request), &o, &rows).unwrap();
        // The acceptance, as the control plane runs it: the replacements owed, then their pins.
        let replacements = replacement_operations(&next, &o, &rows);
        let more = replacement_pins(&replacements, &next.pinned);
        next.pinned.extend(more);
        let path: PathName = "mesh1.gateway.1".parse().unwrap();
        let seen = crate::build::PinnedBirth::of(o.node(&path).unwrap()).unwrap();
        assert!(next.pinned.iter().any(|(p, b)| *p == path && *b == seen), "the acceptance pins the birth it saw at the replaced path: {:?}", next.pinned);
        // Recovered at the same path.name before the replacement's delete: a new node id there.
        let mut now = o.clone();
        let held = now.nodes.iter_mut().find(|x| x.name == path).unwrap();
        held.node_id = NodeId::mint();
        held.incarnation_id = Some(IncarnationId::mint());
        let replacement = crate::build::PinnedBirth::of(now.node(&path).unwrap()).unwrap();
        let ops = replacement_plan(&next, &now, &rows, vec![]);
        let delete = ops.iter().find(|op| op.key() == "node.delete:mesh1.gateway.1").unwrap_or_else(|| panic!("the replacement's delete is planned: {ops:?}"));
        let Some(BuildOperation::DeleteLiveNode { birth: Some(carried), .. }) = Some(delete) else { panic!("a replacement's delete composes its stop: {delete:?}") };
        assert_eq!(*carried, seen, "the delete carries the acceptance pin, not the birth the view holds now");
        let create = ops.iter().find_map(|op| match op { BuildOperation::CreateNode { node, over: Some(b), .. } if *node == path => Some(b.clone()), _ => None }).unwrap_or_else(|| panic!("the create over it is planned: {ops:?}"));
        assert_eq!((create.node_id, create.incarnation), (seen.node_id.clone(), seen.incarnation.clone()), "the create is over the pinned birth");
        let refused = carried.held_in(&now, "node.delete:mesh1.gateway.1", &path).expect_err("the executing mesh-primary refuses the replaced birth");
        assert_eq!((refused.reason(), refused.observed.clone()), ("birth-replaced", Some(replacement.clone())));
        assert_eq!(crate::build::PinnedBirth::of(now.node(&path).unwrap()), Some(replacement), "the replacement birth is untouched");
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
        let op = rdm_core::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("node.delete:{name}"), node_id: b.node_id.clone(), incarnation: b.incarnation.clone(), name: name.parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        receipt(attempt, &format!("node.delete:{name}"), "NodeDeleted", Some(serde_json::to_value(op).unwrap()))
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 2: "A live node whose row already records its
    /// entry's config and executable is not deleted, created or restarted because another node changes. A
    /// request that changes only the gateways' executables redeploys the gateways and leaves every broker,
    /// compute and admin running as it was, and an attempt interrupted halfway finishes the remaining
    /// gateways the same way"; rule 5's order, `plan_with_replacements`'s rules: a type's non-primary members before
    /// its primary): a two-mesh fabric whose request changes only the gateways' executables plans exactly the
    /// gateways' replacements, mesh by mesh, each gateway type's non-primary first: node.delete of the birth
    /// the view holds, then node.create at its path over it, on the entry's executable. Resumed after the
    /// first gateway's create is receipted, the next attempt owes the rest and never that gateway again.
    /// A Build whose change plans no replacement (a count resize) replaces nothing whatever its entries
    /// name. Must NOT happen: a broker, compute or admin replaced, or a gateway replaced twice.
    #[test]
    fn only_the_paths_whose_entry_differs_from_their_row_are_replaced_and_a_resume_finishes_the_rest() {
        use NodeStatus::Ready as R;
        let mesh = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::Broker, 1), (NodeKind::Gateway, 2), (NodeKind::Compute, 1)]);
        let cur = FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: ["mesh1", "mesh2"].iter().map(|m| (m.to_string(), MeshTopology::of(&mesh(m)))).collect() };
        let mut nodes = Vec::new();
        for m in ["mesh1", "mesh2"] {
            nodes.extend([n(&format!("{m}.admin.1"), R, true), n(&format!("{m}.admin.2"), R, false), n(&format!("{m}.broker.1"), R, true), n(&format!("{m}.gateway.1"), R, true), n(&format!("{m}.gateway.2"), R, false), n(&format!("{m}.compute.1"), R, true)]);
        }
        let o = observed(nodes, &["mesh1", "mesh2"]);
        let rows = rows_on(&o, "a");
        let request: Vec<NodeSpec> = o.nodes.iter().map(|x| entry(&x.name.to_string(), Some(if x.name.kind == NodeKind::Gateway { "b" } else { "a" }))).collect();
        let next = compile_against(&cur, &reconcile(request), &o, &rows).unwrap();
        assert_eq!(next.meshes.iter().map(|(k, m)| (k.clone(), paths(m))).collect::<Vec<_>>(), cur.meshes.iter().map(|(k, m)| (k.clone(), paths(m))).collect::<Vec<_>>(), "the paths are the same");
        let birth = |name: &str| o.node(&name.parse().unwrap()).map(|x| crate::build::ExitedBirth { node_id: x.node_id.clone(), incarnation: x.incarnation_id.clone().unwrap() }).unwrap();
        let replace = |name: &str| {
            let node: PathName = name.parse().unwrap();
            let b = birth(name);
            vec![BuildOperation::DeleteLiveNode { node: node.clone(), exited: None, birth: Some(crate::build::PinnedBirth { node_id: b.node_id.clone(), incarnation: b.incarnation.clone() }) }, BuildOperation::CreateNode { node, over: Some(b), executable: Some(exe("b", NodeKind::Gateway)), entry: None }]
        };
        let all: Vec<BuildOperation> = ["mesh1.gateway.2", "mesh1.gateway.1", "mesh2.gateway.2", "mesh2.gateway.1"].iter().flat_map(|p| replace(p)).collect();
        assert_eq!(replacement_plan(&next, &o, &rows, vec![]), all, "the gateways alone, each type's non-primary first");
        assert_eq!(plan(&next, &o, None).operations, vec![], "a Build whose change plans no replacement replaces nothing");
        assert_eq!(paths_owed_replacement(&next, &rows).iter().map(ToString::to_string).collect::<Vec<_>>(), ["mesh1.gateway.1", "mesh1.gateway.2", "mesh2.gateway.1", "mesh2.gateway.2"], "the fabric is ready again only once every gateway's row records its entry's");
        // Interrupted after mesh2.gateway.2's successor was created: the rest, never it again.
        let mut after = o.clone();
        let gone = birth("mesh2.gateway.2");
        let mut successor = n("mesh2.gateway.2", R, false);
        successor.node_id = NodeId::mint();
        after.nodes.retain(|x| x.name.to_string() != "mesh2.gateway.2");
        after.nodes.push(successor.clone());
        let mut on = rows.clone();
        on.insert("mesh2.gateway.2".parse().unwrap(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: successor.node_id.clone(), sha256: Some("b-gateway".into()) });
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
        use NodeStatus::Ready as R;
        let cur = t(&[("mesh1", 2, 2)]);
        let o = observed(vec![n("mesh1.admin.1", R, true), n("mesh1.admin.2", R, false), n("mesh1.rpc.1", R, false), n("mesh1.rpc.2", R, true)], &["mesh1"]);
        let rows = rows_on(&o, "a");
        let request: Vec<NodeSpec> = o.nodes.iter().map(|x| entry(&x.name.to_string(), Some(if x.name.to_string() == "mesh1.rpc.1" { "b" } else { "a" }))).collect();
        let next = compile_against(&cur, &reconcile(request), &o, &rows).unwrap();
        let path: PathName = "mesh1.rpc.1".parse().unwrap();
        let exited = crate::build::ExitedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let recover = AttemptAction::Recover { path: path.clone(), node_id: exited.node_id.clone(), from_incarnation: exited.incarnation.clone(), backbone: false };
        let mut lost = o.clone();
        lost.nodes.retain(|x| x.name != path);
        assert_eq!(
            replacement_build_plan(&next, &lost, &rows, vec![], Some(recover.clone()), [(2, recover.clone())].into()),
            vec![BuildOperation::DeleteLiveNode { node: path.clone(), exited: Some(exited.clone()), birth: None }, BuildOperation::CreateNode { node: path.clone(), over: Some(exited.clone()), executable: None, entry: None }],
            "the recovery alone, on the row's executable"
        );
        // Recovered by attempt 2 on a: the next attempt replaces it on b.
        let recovered = o.node(&path).unwrap();
        let steps = vec![deleted_receipt(2, "mesh1.rpc.1", &exited), receipt(2, "node.delete:mesh1.rpc.1", "Complete", None), receipt(2, "node.create:mesh1.rpc.1", "Complete", None)];
        let birth = crate::build::ExitedBirth { node_id: recovered.node_id.clone(), incarnation: recovered.incarnation_id.clone().unwrap() };
        assert_eq!(
            replacement_build_plan(&next, &o, &rows, steps, None, [(2, recover)].into()),
            vec![BuildOperation::DeleteLiveNode { node: path.clone(), exited: None, birth: Some(crate::build::PinnedBirth { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() }) }, BuildOperation::CreateNode { node: path, over: Some(birth), executable: Some(exe("b", NodeKind::RpcNode)), entry: None }],
            "the replacement follows as the next attempt's"
        );
    }

    /// CONTRACT (storage.md §2: the Build's topology carries `topology_storage_provider`; i149 e1: a
    /// record written before the field reads `file`): the journal's accepted topology written before the
    /// field reads `file`; one written with it reads back as written; the Build topic's copy carries none.
    #[test]
    fn an_accepted_topology_written_before_the_provider_reads_file() {
        let old = serde_json::json!({"fabric": "fabric1", "meshes": {}});
        assert_eq!(serde_json::from_value::<FabricTopology>(old).unwrap().topology_storage_provider.as_deref(), Some("file"));
        let named = FabricTopology::root_on("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap(), "app");
        assert_eq!(serde_json::from_value::<FabricTopology>(serde_json::to_value(&named).unwrap()).unwrap(), named);
        assert_eq!(named.topology_storage_provider.as_deref(), Some("app"));
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: "a Build decoded from the journal whose
    /// paths name none ... is valid history with its births' rows as the record, and an admin restarted over
    /// an old store reads its current Build as it is, never as malformed"; durable format moved): a topology
    /// written before `NodeSpec.executable` (its path configs carrying no such field) decodes with none,
    /// validates, and plans no replacement; a complete-topology request compiles over it. The new field
    /// crosses, unchanged, through the journal's JSON (a Build message carries no executable, rule 7:
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
        assert!(decoded.meshes["mesh1"].spec_by_path.values().all(|c| c.executable.is_none()), "with none");
        assert_eq!(decoded.validate(), Ok(()));
        assert_eq!(paths_owed_replacement(&decoded, &Recorded::new()), Vec::<PathName>::new(), "no replacement is owed from it");
        let next = compile(&decoded, &reconcile(vec![entry("mesh1.admin.1", None), entry("mesh1.rpc.1", None), entry("mesh1.rpc.2", Some("b"))]), &mn()).unwrap();
        assert_eq!(paths(next.mesh("mesh1").unwrap()), ["mesh1.admin.1", "mesh1.rpc.1", "mesh1.rpc.2"]);
        let json = serde_json::to_value(&next).unwrap();
        assert_eq!(serde_json::from_value::<FabricTopology>(json).unwrap(), next, "the journal's shape keeps it");
        let change = reconcile(vec![entry("mesh1.admin.1", Some("b")), entry("mesh1.rpc.1", None), entry("mesh1.rpc.2", Some("b"))]);
        let json = serde_json::to_value(&change).unwrap();
        assert_eq!(serde_json::from_value::<TopologyChange>(json).unwrap(), change, "the journal keeps the change's executables");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md §1 "Corrected": "the durable-format move's decoder
    /// fills each path's executable in an accepted Build written before the field from that Build's old
    /// per-kind set, by the path's kind, once, so the source is converted before it is removed, and that
    /// fill is the desired topology only, never a birth's record"; the removed change's
    /// positional slots reserved, no format move; durable format moved): a topology the journal
    /// holds with the fabric's old per-kind set decodes with each entry that names none filled from the
    /// set by its kind, an entry that names one kept, a kind the set does not bind left with none; it
    /// writes back with no set, and reads back equal. A stored change of the removed operation (`kind:
    /// "roll"`) decodes as history that owes no replacement and writes back as it was read. Must NOT
    /// happen: an admin restarted over such a store refusing its Build as malformed, or the set read as
    /// any birth's executable.
    #[test]
    fn a_topology_stored_with_the_old_per_kind_set_has_each_entry_filled_from_it_once() {
        let bound = |id: &str, tag: &str| serde_json::json!({"launch_id": id, "executable": format!("/set/{id}"), "sha256": tag, "image": null});
        let old = serde_json::json!({
            "fabric": "fabric1",
            "meshes": {"mesh1": {"name": "mesh1", "mesh_id": "meshd0000001", "nodes": ["mesh1.admin.1", "mesh1.broker.1", "mesh1.rpc.1", "mesh1.rpc.2"], "path_config_by_path": {
                "mesh1.admin.1": {"storage": {"persistence": "persistent", "on_retire": "preserve"}, "placement": {}},
                "mesh1.broker.1": {"storage": {"persistence": "ephemeral"}, "placement": {}},
                "mesh1.rpc.1": {"storage": {"persistence": "ephemeral"}, "placement": {}},
                "mesh1.rpc.2": {"storage": {"persistence": "ephemeral"}, "placement": {}, "executable": {"launch_id": "rpc_node", "executable": "/named/rpc", "sha256": "named", "image": null}, "tags": []}
            }}},
            "bindings": {"candidate": {"sha": "c", "build": "b"}, "launch_ids": ["node_admin", "rpc_node"], "bindings": [bound("node_admin", "set-admin"), bound("rpc_node", "set-rpc")]}
        });
        let decoded: FabricTopology = serde_json::from_value(old).expect("a topology stored with the old set decodes");
        let exe = |p: &str| decoded.meshes["mesh1"].spec(&p.parse().unwrap()).unwrap().executable.as_ref().map(|b| (b.executable.display().to_string(), b.sha256.clone()));
        assert_eq!(exe("mesh1.admin.1"), Some(("/set/node_admin".into(), "set-admin".into())), "filled from the set by its kind");
        assert_eq!(exe("mesh1.rpc.1"), Some(("/set/rpc_node".into(), "set-rpc".into())));
        assert_eq!(exe("mesh1.rpc.2"), Some(("/named/rpc".into(), "named".into())), "an entry that names one keeps it");
        assert_eq!(exe("mesh1.broker.1"), None, "a kind the set does not bind is left with none");
        let json = serde_json::to_value(&decoded).unwrap();
        assert!(json.get("bindings").is_none(), "written back with no set: {json}");
        assert_eq!(serde_json::from_value::<FabricTopology>(json).unwrap(), decoded, "and read back equal");
        let stored = serde_json::json!({"kind": "roll", "bindings": {"candidate": {"sha": "c", "build": "b"}, "launch_ids": ["node_admin"], "bindings": [bound("node_admin", "set-admin")]}});
        let change: TopologyChange = serde_json::from_value(stored).expect("a stored change of the removed operation decodes as history");
        assert!(!change.plans_replacements(), "it owes no replacement: the repair is a request naming the executables");
        assert_eq!(serde_json::to_value(&change).unwrap()["kind"], "roll", "written back as it was read");
    }

    // @feature: node-replace
    /// CONTRACT: docs/architecture/lifecycles/node-replace.md:30
    /// "1. node[1]: node.connection.delete `{ scope: All }`, then node.mesh.leave.
    ///  2. node.name.update of node[1] to `<name>.old` ... Then node[1].stop ...
    ///  3. node.create of node[2] at `<name>`, free only because the row above moved the key: a re-birth with a new node.id, incarnation and the specified bin."
    #[test]
    fn replace_steps_pin_predecessor_across_rename_leave_successor_create_unpinned_and_resume_from_receipts() {
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let cur = t(&[("mesh1", 2, 2)]);
        let o = mn();
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let old_name = name.renamed();
        let pred = o.node(&name).unwrap();
        let pred_pin = PinnedBirth {
            node_id: pred.node_id.clone(),
            incarnation: pred.incarnation_id.clone().unwrap(),
        };
        let succ_exe = exe("v2", NodeKind::RpcNode);
        let forward_steps = vec![
            Step::DeleteConnections { node: name.clone() },
            Step::LeaveMesh { node: name.clone() },
            Step::Update {
                node: name.clone(),
                name: old_name.clone(),
            },
            Step::Stop {
                node: old_name.clone(),
            },
            Step::Create {
                node: name.clone(),
                executable: Some(succ_exe.clone()),
            },
        ];
        let action = steps_action(forward_steps.clone(), &o).expect("valid replace steps");
        let AttemptAction::Steps { pinned, .. } = &action else {
            panic!("expected AttemptAction::Steps");
        };
        assert_eq!(pinned, &vec![(name.clone(), pred_pin.clone())]);

        let build = |steps: Vec<BuildStepReceipt>| BuildProjection {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            topology: cur.clone(),
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 3,
            executor: None,
            steps,
            last_failure: None,
            reason: AttemptReason::Steps,
            action: Some(action.clone()),
            attempt_actions: [(3, action.clone())].into(),
            attempt_executors: Default::default(),
            opened: 3,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };

        let initial_ops = plan_for_build(&build(vec![]), &o).operations;
        assert_eq!(
            initial_ops,
            vec![
                BuildOperation::Step {
                    step: forward_steps[0].clone(),
                    birth: Some(pred_pin.clone()),
                    entry: None,
                },
                BuildOperation::Step {
                    step: forward_steps[1].clone(),
                    birth: Some(pred_pin.clone()),
                    entry: None,
                },
                BuildOperation::Step {
                    step: forward_steps[2].clone(),
                    birth: Some(pred_pin.clone()),
                    entry: None,
                },
                BuildOperation::Step {
                    step: forward_steps[3].clone(),
                    birth: Some(pred_pin),
                    entry: None,
                },
                BuildOperation::Step {
                    step: forward_steps[4].clone(),
                    birth: None,
                    entry: None,
                },
            ]
        );

        let complete = |op: &str| BuildStepReceipt {
            mesh: None,
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt: 3,
            operation: op.into(),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        let after_rename = plan_for_build(
            &build(vec![
                complete("node.connection.delete:mesh1.rpc.1"),
                complete("node.mesh.leave:mesh1.rpc.1"),
                complete("node.update:mesh1.rpc.1"),
            ]),
            &o,
        )
        .operations;
        assert_eq!(after_rename, initial_ops[3..].to_vec());

        assert_eq!(steps_action(vec![], &o), Err(BuildReject::EmptySteps));
        assert_eq!(
            steps_action(
                vec![
                    Step::Stop { node: name.clone() },
                    Step::Stop { node: name.clone() },
                ],
                &o
            ),
            Err(BuildReject::StepRepeated {
                operation: "node.stop:mesh1.rpc.1".into()
            })
        );
        assert_eq!(
            steps_action(
                vec![Step::Stop {
                    node: "mesh1.rpc.9".parse().unwrap(),
                }],
                &o
            ),
            Err(BuildReject::UnknownNode {
                node: "mesh1.rpc.9".into(),
                step: Some(1),
            })
        );
    }
}
