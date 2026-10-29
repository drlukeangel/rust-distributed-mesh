//! The harness's executables and its complete-topology requests (proposal
//! executable-bindings-at-create.md rule 1 and §2, Q157): every Build the harness submits names each
//! path's executable, `build.create { Change(Reconcile { based_on, nodes }) }`. A cell's counts are
//! expanded here into named paths with their configs, reading the current topology, as Rafka's door
//! expands its per-type input into per-path entries before the typed call: RDM is never handed a count
//! by the harness, and nothing it launches is taken from a directory with no sha.
//!
//! [`Executables`] is where each kind's executable comes from (the testkit's own binaries, each file
//! hashed once per process, or an explicit set); [`Current`] is the read a request is built from (the
//! current Build at the fabric-primary, its accepted topology and every node's row) and expands counts
//! the way the compiler's count form resized them (`accepted::resize`): a grow takes the lowest free
//! ordinal of the kind, a shrink drops the highest ordinal that is not the kind's primary.

use rdm_admin::accepted::FabricTopology;
use rdm_client::binding::{sha256_file, Binding, BindingSet};
use rdm_client::{BuildCarrier, BuildId, Fabrics, LaunchKind as NodeKind, MeshDesired, NodeStatus, NodeView, PathName};
use rdm_core::meta::{NodeSpec, PersistentDisposition, PlacementMeta, StorageMeta};
use rdm_transport::NodeTarget;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

/// Where each kind's executable comes from.
#[derive(Debug, Clone)]
pub enum Executables {
    /// The testkit's own binaries in [`crate::estate::bin_dir`]: `rdm-node-admin`, `rdm-rpc-node`, and
    /// `rdm-node` for every other kind (it takes its node_type from the launch). `container` binds each
    /// to the container provider's runtime image.
    Testkit {
        /// The fabric's provider is the container provider.
        container: bool,
    },
    /// An explicit set: a consumer's executables, the faulted node-admin's.
    Set(BindingSet),
}

impl Executables {
    /// The testkit's binaries under a provider named `provider` (`process` or `container`).
    pub fn testkit(provider: &str) -> Self {
        Self::Testkit { container: provider == "container" }
    }

    /// The binding a created node of `kind` runs. A set that binds no `kind` is a defect of the cell,
    /// named: nothing falls back to another executable.
    pub fn of(&self, kind: NodeKind) -> Binding {
        self.binding(kind).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Self::of`], an unbound `kind` named in the error.
    pub fn binding(&self, kind: NodeKind) -> Result<Binding, String> {
        match self {
            Self::Testkit { container } => {
                let name = match kind {
                    NodeKind::NodeAdmin => "rdm-node-admin",
                    NodeKind::RpcNode => "rdm-rpc-node",
                    _ => "rdm-node",
                };
                let executable = crate::estate::binary(name);
                let sha256 = sha256_once(&executable)?;
                Ok(Binding { launch_id: kind.name().into(), executable, sha256, image: container.then(|| crate::estate::RUNTIME_IMAGE.to_string()) })
            }
            Self::Set(set) => set.of(kind).cloned().ok_or_else(|| format!("the executable set binds no `{}`: a {} path cannot be named (launch ids {:?})", kind.name(), kind.name(), set.launch_ids)),
        }
    }
}

/// The sha256 of the file at `path`, hashed once per process: the testkit binaries are hundreds of
/// megabytes, and every request names them.
fn sha256_once(path: &Path) -> Result<String, String> {
    static HASHED: OnceLock<Mutex<BTreeMap<PathBuf, Arc<OnceLock<Result<String, String>>>>>> = OnceLock::new();
    let cell = HASHED.get_or_init(Default::default).lock().unwrap().entry(path.to_path_buf()).or_default().clone();
    cell.get_or_init(|| sha256_file(path).map_err(|e| format!("{} could not be hashed: {e}", path.display()))).clone()
}

/// The create spec of `path`, a path the request creates: its storage by kind, written out (brokers and
/// node-admins keep theirs across a restart and a delete; gateways, compute and proof rpc nodes are
/// ephemeral), no placement, no tags, no config, and `executable`.
pub fn created_spec(path: PathName, executable: Binding) -> NodeSpec {
    let storage_persistence = match path.kind {
        NodeKind::NodeAdmin | NodeKind::Broker => StorageMeta::Persistent { disposition: PersistentDisposition::Preserve },
        NodeKind::Gateway | NodeKind::Compute | NodeKind::RpcNode => StorageMeta::Ephemeral,
    };
    NodeSpec { path, storage_persistence, placement: PlacementMeta::default(), executable: Some(executable), tags: Default::default(), config: Default::default() }
}

/// The counts of `mesh`, by kind, in the compiler's order.
fn counts(mesh: &MeshDesired) -> [(NodeKind, u32); 5] {
    [(NodeKind::NodeAdmin, mesh.node_admin), (NodeKind::RpcNode, mesh.rpc_node), (NodeKind::Broker, mesh.broker), (NodeKind::Gateway, mesh.gateway), (NodeKind::Compute, mesh.compute)]
}

/// What a complete-topology request is built from: the current Build at the fabric-primary with its
/// accepted topology, and every node's row as `fabric.nodes.get` answers it.
#[derive(Debug, Clone)]
pub struct Current {
    /// The current Build: the request's `based_on`.
    pub based_on: BuildId,
    /// That Build's accepted topology.
    pub topology: FabricTopology,
    /// Every node of the fabric, from the rows.
    pub rows: Vec<NodeView>,
}

impl Current {
    /// Read it through `carrier`: `fabric.get` at the admin it starts at names the fabric-primary,
    /// whose current Build is read with its facts (`FetchCurrentBuild`), and `node.get` with no pair.
    pub async fn read(carrier: &BuildCarrier) -> Result<Self, String> {
        let view = crate::typed::the_fabric(&Fabrics::new(carrier.clone())).await.map_err(|e| format!("fabric.get at {}: {e}", carrier.fabric_primary()))?;
        let at = view.fabric_primary.clone().unwrap_or_else(|| carrier.fabric_primary().clone());
        let fetched = rdm_admin::build_facts_read::fetch_current_build(carrier.client(), &NodeTarget::CurrentPath(at.clone())).await.map_err(|e| format!("the current Build at the fabric-primary {at}: {e}"))?;
        let topology = rdm_admin::build_state::fold(&fetched.facts).remove(&fetched.build_id).map(|p| p.topology).ok_or_else(|| format!("the fabric-primary {at}'s facts of its current Build {} hold no acceptance of it", fetched.build_id))?;
        // The rows one mesh at a time: node.get's answer is bounded (node-rpc-envelope.md, node.get:
        // "read bound RESOURCE_EXHAUSTED"), and the whole fabric's rows outgrow it (L4's 200 nodes
        // encode to 93 KB, past the 64 KB Status reply bound).
        let mut rows = Vec::new();
        for mesh in topology.mesh_names() {
            let of = rdm_client::Nodes::new(carrier.clone()).get(&rdm_client::Filter::new().eq("mesh", [mesh.clone()])).await.map_err(|e| format!("node.get of {mesh} at {}: {e}", carrier.fabric_primary()))?;
            rows.extend(of);
        }
        Ok(Self { based_on: fetched.build_id, topology, rows })
    }

    /// The executable the row at `path` records, a live birth's first.
    fn row_executable(&self, path: &PathName) -> Option<Binding> {
        let at = || self.rows.iter().filter(|n| &n.name == path);
        at().find(|n| !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)).or_else(|| at().next()).and_then(|n| n.executable.clone())
    }

    /// The live birth's row at `path`, if one stands.
    fn live_row(&self, path: &PathName) -> Option<&NodeView> {
        self.rows.iter().find(|n| &n.name == path && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead))
    }

    /// The ordinal of `kind`'s primary in `mesh`, as the rows say.
    fn primary_ordinal(&self, mesh: &str, kind: NodeKind) -> Option<u32> {
        self.rows.iter().find(|n| n.mesh == mesh && n.kind == kind && n.is_primary && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)).map(|n| n.name.ordinal)
    }

    /// The executable a path the request creates runs: `executables`' for its kind.
    fn created_executable(&self, kind: NodeKind, executables: &Executables) -> Result<Binding, String> {
        executables.binding(kind)
    }

    /// Every path the topology holds, with its create spec: its entry's, the executable its entry names,
    /// else the one its row records, else `executables`' for its kind (a path no birth was claimed at); the
    /// tags and the config a live birth's row carries, so the request names what the node holds and the
    /// Build, which applies every property that differs from the row (build.md §1), changes neither.
    pub fn held(&self, executables: &Executables) -> Result<BTreeMap<String, BTreeMap<PathName, NodeSpec>>, String> {
        let mut out: BTreeMap<String, BTreeMap<PathName, NodeSpec>> = BTreeMap::new();
        for (name, mesh) in &self.topology.meshes {
            let paths = out.entry(name.clone()).or_default();
            for path in &mesh.nodes {
                let mut spec = match mesh.spec(path) {
                    Some(s) => s.clone(),
                    None => created_spec(path.clone(), self.created_executable(path.kind, executables)?),
                };
                if spec.executable.is_none() {
                    spec.executable = Some(match self.row_executable(path) {
                        Some(b) => b,
                        None => self.created_executable(path.kind, executables)?,
                    });
                }
                if let Some(row) = self.live_row(path) {
                    (spec.tags, spec.config) = (row.tags.clone(), row.config.clone());
                }
                paths.insert(path.clone(), spec);
            }
        }
        Ok(out)
    }

    /// The complete request that resizes each of `meshes` to its counts, as the compiler's count form
    /// resized it: a grow takes the lowest free ordinal of the kind, a shrink drops the highest ordinal
    /// that is not the kind's primary (else the highest). Every path the topology holds keeps its spec
    /// ([`Self::held`]); every created path is named on its executable. A mesh the topology holds that
    /// `meshes` leaves out is left out of the request (its mesh.delete) when `whole`, the fabric's
    /// complete shape, and kept as it is otherwise.
    pub fn resized(&self, meshes: &[MeshDesired], whole: bool, executables: &Executables) -> Vec<NodeSpec> {
        self.try_resized(meshes, whole, executables).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Self::resized`], a path that cannot be named (its kind unbound) named in the error.
    pub fn try_resized(&self, meshes: &[MeshDesired], whole: bool, executables: &Executables) -> Result<Vec<NodeSpec>, String> {
        let mut by_mesh = self.held(executables)?;
        if whole {
            by_mesh.retain(|name, _| meshes.iter().any(|m| &m.name == name));
        }
        for desired in meshes {
            let paths = by_mesh.entry(desired.name.clone()).or_default();
            for (kind, want) in counts(desired) {
                let mut have: Vec<u32> = paths.keys().filter(|p| p.kind == kind).map(|p| p.ordinal).collect();
                have.sort_unstable();
                let keep = self.primary_ordinal(&desired.name, kind);
                while (have.len() as u32) < want {
                    let ordinal = (1..).find(|o| !have.contains(o)).expect("a free ordinal");
                    have.push(ordinal);
                    have.sort_unstable();
                    let path = PathName::new(&desired.name, kind, ordinal);
                    paths.insert(path.clone(), created_spec(path, self.created_executable(kind, executables)?));
                }
                while (have.len() as u32) > want {
                    let drop = have.iter().rev().copied().find(|o| Some(*o) != keep).or_else(|| have.last().copied()).expect("a member to drop");
                    have.retain(|o| *o != drop);
                    paths.remove(&PathName::new(&desired.name, kind, drop));
                }
            }
        }
        Ok(by_mesh.into_values().flat_map(BTreeMap::into_values).collect())
    }

    /// The counts the topology holds of `mesh` (zero for a mesh it does not hold).
    pub fn counts_of(&self, mesh: &str) -> MeshDesired {
        let of = |kind: NodeKind| self.topology.meshes.get(mesh).map(|m| m.nodes.iter().filter(|p| p.kind == kind).count() as u32).unwrap_or(0);
        MeshDesired { name: mesh.into(), node_admin: of(NodeKind::NodeAdmin), rpc_node: of(NodeKind::RpcNode), broker: of(NodeKind::Broker), gateway: of(NodeKind::Gateway), compute: of(NodeKind::Compute) }
    }

    /// The complete request that adds one node of `kind` to `mesh` (`node.create`), at the lowest free
    /// ordinal, as the count form's `AddNode` did.
    pub fn with_one_more(&self, mesh: &str, kind: NodeKind, executables: &Executables) -> Vec<NodeSpec> {
        self.try_with_one_more(mesh, kind, executables).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Self::with_one_more`], a path that cannot be named named in the error.
    pub fn try_with_one_more(&self, mesh: &str, kind: NodeKind, executables: &Executables) -> Result<Vec<NodeSpec>, String> {
        let mut grown = self.counts_of(mesh);
        match kind {
            NodeKind::NodeAdmin => grown.node_admin += 1,
            NodeKind::RpcNode => grown.rpc_node += 1,
            NodeKind::Broker => grown.broker += 1,
            NodeKind::Gateway => grown.gateway += 1,
            NodeKind::Compute => grown.compute += 1,
        }
        self.try_resized(&[grown], false, executables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_admin::accepted::MeshTopology;

    fn binding(id: &str, sha: &str) -> Binding {
        Binding { launch_id: id.into(), executable: format!("/b/{id}").into(), sha256: sha.into(), image: None }
    }

    fn set() -> Executables {
        let bindings = vec![binding("node_admin", "a"), binding("rpc_node", "r")];
        Executables::Set(BindingSet { candidate: rdm_client::binding::Candidate { sha: "c".into(), build: String::new() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings })
    }

    fn row(name: &str, primary: bool, sha: &str) -> NodeView {
        let p: PathName = name.parse().unwrap();
        let mut v: NodeView = serde_json::from_value(serde_json::json!({
            "name": name, "kind": p.kind, "mesh": p.mesh, "node_id": rdm_core::fixture_crockford60(name), "status": "ready",
            "is_primary": primary, "is_fabric_primary": false, "tags": [], "config": {}
        }))
        .unwrap();
        v.executable = Some(binding(p.kind.name(), sha));
        v
    }

    /// A topology holding `mesh1.admin.1` (entry naming none) and `mesh1.rpc.1..=3` (entries naming `r0`).
    fn current() -> Current {
        let mut mesh = MeshTopology { name: "mesh1".into(), mesh_id: rdm_core::MeshId::mint(), nodes: Default::default(), spec_by_path: Default::default() };
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let mut admin_spec = created_spec(admin.clone(), binding("node_admin", "x"));
        admin_spec.executable = None;
        mesh.nodes.insert(admin.clone());
        mesh.spec_by_path.insert(admin, admin_spec);
        for o in 1..=3 {
            let p = PathName::new("mesh1", NodeKind::RpcNode, o);
            mesh.nodes.insert(p.clone());
            mesh.spec_by_path.insert(p.clone(), created_spec(p, binding("rpc_node", "r0")));
        }
        let topology = FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), mesh)].into_iter().collect() };
        Current { based_on: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), topology, rows: vec![row("mesh1.admin.1", true, "a0"), row("mesh1.rpc.3", true, "r0")] }
    }

    fn names(request: &[NodeSpec]) -> Vec<String> {
        request.iter().map(|s| s.path.to_string()).collect()
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1, "the door expands a count into named
    /// paths with configs, reading the current topology for the ordinals"; `accepted::resize`): a grow
    /// takes the lowest free ordinals and names each created path's executable; a held path keeps its
    /// entry's executable, else the one its row records; a shrink drops the highest ordinal that is not
    /// the kind's primary; a whole-fabric shape leaves out a mesh it does not list, a mesh's own shape
    /// keeps the others.
    #[test]
    fn counts_expand_into_named_paths_the_way_the_compiler_resized_them() {
        let c = current();
        let grown = c.resized(&[MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 3)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])], true, &set());
        assert_eq!(names(&grown), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3", "mesh2.admin.1", "mesh2.rpc.1"]);
        let sha = |p: &str| grown.iter().find(|s| s.path.to_string() == p).and_then(|s| s.executable.clone()).map(|b| b.sha256).unwrap();
        assert_eq!((sha("mesh1.admin.1"), sha("mesh1.admin.2"), sha("mesh1.rpc.2"), sha("mesh2.rpc.1")), ("a0".into(), "a".into(), "r0".into(), "r".into()));
        let shrunk = c.resized(&[MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])], true, &set());
        assert_eq!(names(&shrunk), ["mesh1.admin.1", "mesh1.rpc.3"], "the primary mesh1.rpc.3 is kept");
        let one_more = c.with_one_more("mesh1", NodeKind::RpcNode, &set());
        assert_eq!(names(&one_more), ["mesh1.admin.1", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3", "mesh1.rpc.4"]);
        let other = c.resized(&[MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1)])], false, &set());
        assert_eq!(names(&other), ["mesh1.admin.1", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3", "mesh2.admin.1"]);
    }
}
