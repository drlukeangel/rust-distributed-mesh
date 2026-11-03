//! The node-admin this UI reads and drives, over Node RPC: RDM serves no HTTP API
//! (rdm-crate-and-test-layout.md §2), so the UI enters at the birth a node-admin printed at start
//! (`RDM_NODE_ADMIN_BIRTH`, the JSON after `RDM_NODE_BIRTH=`) and calls the typed objects there.
//!
//! Every Build the UI submits names each path's executable (proposal executable-bindings-at-create.md
//! rule 1): the UI is the door, expanding its counts into the complete topology, built from the current
//! Build `fabric.get` names and every node's row (`fabric.nodes.get`), each held path on the executable
//! its row records and each created path on the consumer's executable of its type, found beside the
//! UI's own executable (`rshape-node-admin`, `rshape-gateway`, `rshape-compute`, `rshape-broker`). A
//! grow takes the lowest free ordinal of the type; a shrink drops the highest ordinal that is not the
//! type's primary.

use rdm_core::{PathName};
use rdm_client::{
    birth_of_view, Accepted, BuildCarrier, Build, BuildId, CallEnd, Change, Connections, ConnectionsView, ExactBirth, FabricDesired, FabricView, Fabrics,
    Filter, Frame, MeshDesired, MeshNode, NodeStatus, NodeView, Peers, ProviderKind, WorkflowStream,
};
use rdm_core::build::{Meshes, Nodes};
use rdm_core::binding::{sha256_file, Binding};
use rdm_core::meta::{NodeSpec, PersistentDisposition, StoragePersistence};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

/// The node-admin the UI enters at, the peers it reaches over Node RPC, and the executable of each
/// type the UI's Builds name.
pub struct Admin {
    peers: Arc<Peers>,
    entry: PathName,
    executables: BTreeMap<String, PathBuf>,
}

/// The consumer's executable of each type found in `dir`.
fn executables_in(dir: &std::path::Path) -> BTreeMap<String, PathBuf> {
    [("node-admin", "rshape-node-admin"), ("gateway", "rshape-gateway"), ("compute", "rshape-compute"), ("broker", "rshape-broker")]
        .into_iter()
        .map(|(r#type, name)| (r#type.to_string(), dir.join(name)))
        .filter(|(_, path)| path.is_file())
        .collect()
}

impl Admin {
    /// The admin `RDM_NODE_ADMIN_BIRTH` names, `None` when it is unset; a birth that does not decode
    /// is refused by name.
    pub async fn from_env() -> Result<Option<Admin>, String> {
        let Some(json) = std::env::var("RDM_NODE_ADMIN_BIRTH").ok().filter(|b| !b.trim().is_empty()) else { return Ok(None) };
        let birth: MeshNode = serde_json::from_str(&json).map_err(|e| format!("RDM_NODE_ADMIN_BIRTH {json:?}: not a birth: {e}"))?;
        Ok(Some(Admin::at(&birth).await?))
    }

    /// The admin of `birth`, its Builds naming the consumer's executables beside the UI's own.
    pub async fn at(birth: &MeshNode) -> Result<Admin, String> {
        let peers = Arc::new(Peers::at(birth).await?);
        let beside = std::env::current_exe().ok().and_then(|exe| exe.parent().map(executables_in)).unwrap_or_default();
        Ok(Admin { peers, entry: birth.location(), executables: beside })
    }

    /// This admin, its Builds naming `executables` by type.
    pub fn with_executables(mut self, executables: BTreeMap<String, PathBuf>) -> Self {
        self.executables = executables;
        self
    }

    /// The topology a request is built from: the current Build `fabric.get` names, and every live
    /// path the rows hold (`node.get`) that the current Build's topology names (`build.get`), with the
    /// executable its row records. A replaced predecessor renamed `<name>-old` is a row the topology
    /// does not name (a name ending `-old` does not make a predecessor: lifecycles/node-replace.md).
    async fn current(&self) -> Result<Current, CallEnd> {
        let fabric = self.fabric().await?;
        let unread = |reason: String| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason };
        let based_on = fabric.build_id.as_deref().ok_or_else(|| unread("fabric.get names no current Build".into()))?;
        let based_on = BuildId::parse(based_on).map_err(|e| unread(format!("fabric.get names {based_on}, not a Build id: {e}")))?;
        let accepted: BTreeSet<PathName> = self.build(&based_on).await?.nodes.into_iter().map(|spec| spec.location()).collect();
        let rows = self.rows().await?;
        let mut current = Current { paths: BTreeMap::new(), ids: BTreeMap::new(), rows: BTreeMap::new(), primaries: BTreeSet::new(), mesh_primaries: BTreeMap::new(), image: (fabric.provider == ProviderKind::Container).then(|| CONTAINER_IMAGE.to_string()) };
        for row in rows.into_iter().filter(|n| accepted.contains(&n.location()) && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)) {
            if row.is_primary {
                current.primaries.insert(row.location());
                if row.r#type == "node-admin" {
                    current.mesh_primaries.insert(row.mesh.clone(), row.node_id.clone());
                }
            }
            current.ids.insert(row.location(), row.node_id.clone());
            current.rows.insert(row.location(), row.clone());
            let executable = match row.executable {
                Some(ref b) => b,
                None => &self.binding(&row.r#type, &current.image)?,
            };
            let at = row.location();
            let spec = NodeSpec { tags: row.tags, config: row.config, ..spec(at.clone(), &row.mesh, &row.r#type, executable.clone()) };
            current.paths.insert(at, spec);
        }
        Ok(current)
    }

    /// The binding a created node of `type` runs: the consumer's executable of the type, hashed once,
    /// in `image`. A type with no executable is refused `executable-missing` before anything is sent.
    fn binding(&self, r#type: &str, image: &Option<String>) -> Result<Binding, CallEnd> {
        let executable = self.executables.get(r#type).cloned().ok_or_else(|| missing(format!("the UI has no `{}` executable: a {} path cannot be named (named: {:?})", r#type, r#type, self.executables.keys().collect::<Vec<_>>())))?;
        static HASHED: OnceLock<Mutex<BTreeMap<PathBuf, String>>> = OnceLock::new();
        let known = HASHED.get_or_init(Default::default).lock().unwrap().get(&executable).cloned();
        let sha256 = match known {
            Some(sha) => sha,
            None => {
                let sha = sha256_file(&executable).map_err(|e| missing(format!("{} could not be hashed: {e}", executable.display())))?;
                HASHED.get_or_init(Default::default).lock().unwrap().insert(executable.clone(), sha.clone());
                sha
            }
        };
        Ok(Binding { launch_id: r#type.to_string(), executable, sha256, image: image.clone() })
    }

    /// The complete topology with each of `meshes` resized to its counts; a mesh it leaves out is left
    /// out (its mesh.delete) when `whole`, kept otherwise. A path's mesh and type are its spec's.
    fn resized(&self, current: &Current, meshes: &[MeshDesired], whole: bool) -> Result<Vec<NodeSpec>, CallEnd> {
        let mut paths = current.paths.clone();
        if whole {
            paths.retain(|_, s| meshes.iter().any(|m| m.name == s.mesh));
        }
        for m in meshes {
            for r#type in ["node-admin", "broker", "gateway", "compute"] {
                let want = m.count(r#type);
                let mut have: Vec<PathName> = paths.values().filter(|s| s.mesh == m.name && s.r#type == r#type).map(|s| s.location()).collect();
                have.sort();
                let keep = current.primaries.iter().find(|p| have.contains(p)).cloned();
                while (have.len() as u32) < want {
                    let path = (1..).map(|ordinal| PathName::numbered(&m.name, r#type, ordinal)).find(|p| !paths.contains_key(p)).expect("a free generated pair");
                    have.push(path.clone());
                    have.sort();
                    paths.insert(path.clone(), spec(path, &m.name, r#type, self.binding(r#type, &current.image)?));
                }
                while (have.len() as u32) > want {
                    let drop = have.iter().rev().find(|p| Some(*p) != keep.as_ref()).or_else(|| have.last()).expect("a member to drop").clone();
                    have.retain(|p| p != &drop);
                    paths.remove(&drop);
                }
            }
        }
        Ok(paths.into_values().collect())
    }

    /// The counts `current` holds of `mesh`.
    fn counts_of(current: &Current, mesh: &str) -> MeshDesired {
        let of = |r#type: &str| current.paths.values().filter(|s| s.mesh == mesh && s.r#type == r#type).count() as u32;
        MeshDesired::of(mesh, [("node-admin", of("node-admin")), ("broker", of("broker")), ("gateway", of("gateway")), ("compute", of("compute"))])
    }

    fn carrier(&self) -> BuildCarrier {
        self.peers.carrier_at(self.entry.clone())
    }

    /// `fabric.get` at the entry admin: the one fabric it holds.
    pub async fn fabric(&self) -> Result<FabricView, CallEnd> {
        Fabrics::new(self.carrier()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::NotFound, reason: format!("path {:?}, name {:?} answered fabric.get with no fabric", self.entry.path, self.entry.name) })
    }

    /// `node.get [mesh: m]` of every mesh the entry admin's view holds.
    pub async fn nodes(&self) -> Result<Vec<NodeView>, CallEnd> {
        let nodes = rdm_client::Nodes::new(self.carrier());
        let mut out = Vec::new();
        for m in self.fabric().await?.meshes {
            out.extend(nodes.get(&Filter::new().eq("mesh", [m.name.clone()])).await?);
        }
        for n in &out {
            if let Some(birth) = birth_of_view(n) {
                let _ = self.peers.learn_birth(&birth);
            }
        }
        Ok(out)
    }

    /// The mesh the row at `node` names (a live birth's first), `None` when no row holds it or the rows
    /// cannot be read: a node's mesh is its row's, never its `(path, name)`'s.
    pub async fn mesh_of(&self, node: &PathName) -> Option<String> {
        let rows = self.rows().await.ok()?;
        let at = || rows.iter().filter(|n| &n.location() == node);
        at().find(|n| !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)).or_else(|| at().next()).map(|n| n.mesh.clone())
    }

    /// `node.get` with no pair: every node's row, the executable it records included.
    pub async fn rows(&self) -> Result<Vec<NodeView>, CallEnd> {
        rdm_client::Nodes::new(self.carrier()).get(&Filter::new()).await
    }

    /// Put `key` = `value` on the config of the entry admin's own row, beside what the row holds: one
    /// `node.config.update` to the fabric-primary for its exact birth (node-config-update.md step 1),
    /// admitted at any time (Q166).
    pub async fn put_own_config(&self, key: &str, value: &str) -> Result<rdm_core::status::StatusReply, String> {
        let rows = self.rows().await.map_err(|e| format!("node.get: {e:?}"))?;
        let own = rows.iter().find(|n| n.location() == self.entry && n.incarnation_id.is_some()).ok_or_else(|| format!("no row holds path {:?}, name {:?}", self.entry.path, self.entry.name))?;
        let primary = rows.iter().find(|n| n.r#type == "node-admin" && n.is_fabric_primary).ok_or_else(|| "no row names the fabric-primary".to_string())?;
        let mut config = own.config.clone();
        config.insert(key.to_string(), value.to_string());
        let birth = ExactBirth { target: rdm_transport::NodeTarget::ExactNode(own.node_id.clone()), node_id: own.node_id.clone(), incarnation: own.incarnation_id.clone().expect("checked above") };
        let carrier = self.carrier();
        rdm_client::NodeRpc::new(carrier.client()).config_update(&primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, config, &rdm_transport::CallOptions::default()).await.map_err(|e| format!("node.config.update: {e:?}"))
    }

    /// `node.connection.get { node }` of `n`'s exact birth.
    pub async fn connections(&self, n: &NodeView) -> Option<Result<ConnectionsView, CallEnd>> {
        let incarnation = n.incarnation_id.clone()?;
        let birth = ExactBirth { target: rdm_transport::NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation };
        Some(Connections::new(self.carrier()).get(&birth, &Filter::new()).await)
    }

    /// `build.get` of `id`: its receipts.
    pub async fn build(&self, id: &BuildId) -> Result<Build, CallEnd> {
        self.carrier().build(id).await
    }

    /// node.create of a `type` node in `mesh`, at the lowest free ordinal of its type: a Create of that
    /// node alone (build.md §1).
    pub async fn spawn(&self, mesh: &str, r#type: &str) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        if !current.paths.values().any(|s| s.mesh == mesh) {
            return Err(CallEnd::Rejected { reason: "unknown-mesh".into(), detail: format!("the fabric holds no mesh {mesh}") });
        }
        let mut grown = Self::counts_of(&current, mesh);
        match r#type {
            // The demo is a rafka replica (Q295): it runs no plain RDM `node`.
            "node-admin" | "broker" | "gateway" | "compute" => *grown.counts.entry(r#type.to_string()).or_insert(0) += 1,
            _ => return Err(CallEnd::Rejected { reason: "unknown-type".into(), detail: "the demo runs node-admins, gateways, brokers and computes only".into() }),
        }
        let next = self.resized(&current, &[grown], false)?;
        let created: Vec<Nodes> = next.into_iter().filter(|s| !current.paths.contains_key(&s.location())).map(nodes_of).collect();
        let stream = rdm_client::Nodes::new(self.carrier()).create(created).await?;
        opened(stream.accepted())
    }

    /// node.stop of `node`: a node command, never a Build; its terminal frame.
    pub async fn stop(&self, node: &PathName) -> Result<Frame, CallEnd> {
        terminal(rdm_client::Nodes::new(self.carrier()).stop(node).await?).await
    }

    /// node.start of the stopped `node`: a node command, never a Build; its terminal frame.
    pub async fn start(&self, node: &PathName) -> Result<Frame, CallEnd> {
        terminal(rdm_client::Nodes::new(self.carrier()).start(node).await?).await
    }

    /// node.delete of `node`: a Delete naming its node.id (build.md §1).
    pub async fn remove(&self, node: &PathName) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        let id = current.ids.get(node).cloned().ok_or_else(|| unknown_node(node))?;
        let stream = rdm_client::Nodes::new(self.carrier()).delete(vec![id]).await?;
        opened(stream.accepted())
    }

    /// build.create taking the fabric to `desired`: a Create of the meshes and nodes it adds, or a
    /// Delete of the meshes and nodes it removes. A shape that both adds and removes is two Builds,
    /// refused here by name: the UI submits one at a time.
    pub async fn build_fabric(&self, desired: FabricDesired) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        let next = self.resized(&current, &desired.meshes, true)?;
        let held: BTreeSet<String> = current.paths.values().map(|s| s.mesh.clone()).collect();
        let kept: BTreeSet<PathName> = next.iter().map(|s| s.location()).collect();
        let created: Vec<Nodes> = next.iter().filter(|s| !current.paths.contains_key(&s.location())).cloned().map(nodes_of).collect();
        let new_meshes: Vec<Meshes> = desired.meshes.iter().filter(|m| !held.contains(&m.name)).map(|m| Meshes { name: m.name.clone() }).collect();
        let gone_meshes: Vec<String> = held.iter().filter(|m| !desired.meshes.iter().any(|d| &d.name == *m)).cloned().collect();
        let deleted: Vec<rdm_core::NodeId> = current
            .paths
            .values()
            .filter(|s| !kept.contains(&s.location()) && !gone_meshes.contains(&s.mesh))
            .filter_map(|s| current.ids.get(&s.location()).cloned())
            .collect();
        let change = match (created.is_empty() && new_meshes.is_empty(), deleted.is_empty() && gone_meshes.is_empty()) {
            (false, true) => Change::Create { meshes: new_meshes, nodes: created },
            (true, false) => Change::Delete { meshes: gone_meshes, node_ids: deleted },
            (true, true) => return Err(CallEnd::Rejected { reason: "no-change".into(), detail: "the fabric is at that shape already".into() }),
            (false, false) => return Err(CallEnd::Rejected { reason: "add-and-remove".into(), detail: "a Build adds or removes nodes: submit the growth and the shrink one at a time".into() }),
        };
        let s = self.carrier().create(change).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create of a new mesh with `desired`'s counts: a Create of the mesh and its nodes.
    pub async fn create_mesh(&self, desired: MeshDesired) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        if current.paths.values().any(|s| s.mesh == desired.name) {
            return Err(CallEnd::Rejected { reason: "mesh-already-exists".into(), detail: format!("the fabric holds mesh {} already", desired.name) });
        }
        let nodes = self.resized(&current, std::slice::from_ref(&desired), false)?.into_iter().filter(|s| s.mesh == desired.name).map(nodes_of).collect();
        let s = self.carrier().create(Change::Create { meshes: vec![Meshes { name: desired.name.clone() }], nodes }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// `node`'s replacement on the executable at `executable`, hashed here (lifecycles/node-replace.md):
    /// node commands free its name (its connections deleted, it leaves its mesh, it is renamed
    /// `<name>-old` and stopped, keeping its data), then one Create adds its successor at the same path and
    /// name. A path the rows hold no live node at is refused `unknown-node`.
    pub async fn replace(&self, node: &PathName, executable: PathBuf) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        let sha256 = sha256_file(&executable).map_err(|e| missing(format!("{} could not be hashed: {e}", executable.display())))?;
        let spec = current.paths.get(node).cloned().ok_or_else(|| unknown_node(node))?;
        let row = current.rows.get(node).cloned().ok_or_else(|| unknown_node(node))?;
        let primary = current.mesh_primaries.get(&spec.mesh).cloned().ok_or_else(|| CallEnd::Rejected { reason: "no-mesh-primary".into(), detail: format!("node.get names no primary of {}", spec.mesh) })?;
        let old = node.renamed().map_err(|e| CallEnd::Rejected { reason: "invalid-name".into(), detail: format!("{node:?} has no <name>-old: {e:?}") })?;
        let nodes = rdm_client::Nodes::new(self.carrier());
        terminal(nodes.delete_connections(node, &rdm_core::status::ConnectionScope::All).await?).await?;
        terminal(nodes.leave_mesh(node).await?).await?;
        let incarnation = row.incarnation_id.clone().ok_or_else(|| unknown_node(node))?;
        let birth = ExactBirth { target: rdm_transport::NodeTarget::ExactNode(row.node_id.clone()), node_id: row.node_id.clone(), incarnation };
        match nodes.name_update(&birth, &primary, &rdm_transport::RouteChoice::Direct, &old).await {
            Ok(rdm_core::status::StatusReply::NameUpdated { .. }) => {}
            Ok(other) => return Err(CallEnd::Rejected { reason: "name-update-refused".into(), detail: format!("node.name.update of {node:?} to {old:?} ended {other:?}") }),
            Err(e) => return Err(e),
        }
        terminal(nodes.stop(&old).await?).await?;
        let binding = Binding { launch_id: spec.r#type.clone(), executable, sha256, image: current.image.clone() };
        let successor = Nodes { executable: Some(binding), tags: Default::default(), config: Default::default(), ..nodes_of(spec) };
        opened(nodes.create(vec![successor]).await?.accepted())
    }

    /// build.create removing the mesh `mesh`: a Delete of it by name.
    pub async fn remove_mesh(&self, mesh: &str) -> Result<Accepted, CallEnd> {
        let s = self.carrier().create(Change::Delete { meshes: vec![mesh.to_string()], node_ids: Vec::new() }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }
}

/// The Build a call opened, or why it opened none.
fn opened(accepted: Option<&Accepted>) -> Result<Accepted, CallEnd> {
    accepted.cloned().ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the call opened no Build".into() })
}

/// A workflow stream read to its terminal frame: `Complete`, or a failure named.
async fn terminal(mut stream: WorkflowStream) -> Result<Frame, CallEnd> {
    let mut last = None;
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        let end = frame.is_terminal();
        last = Some(frame);
        if end {
            break;
        }
    }
    match last {
        Some(Frame::Complete) => Ok(Frame::Complete),
        Some(other) => Err(CallEnd::Rejected { reason: "workflow-failed".into(), detail: format!("{other:?}") }),
        None => Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the stream ended with no frame".into() }),
    }
}

/// A path the rows hold no live node at.
fn unknown_node(node: &PathName) -> CallEnd {
    CallEnd::Rejected { reason: "unknown-node".into(), detail: format!("the fabric holds no live node at path {:?}, name {:?}", node.path, node.name) }
}

/// `spec` as the Build's Nodes schema (node-rpc-envelope.md "Build schema").
fn nodes_of(spec: NodeSpec) -> Nodes {
    Nodes { path: spec.path, name: spec.name, mesh: spec.mesh, r#type: spec.r#type, storage_persistence: Some(spec.storage_persistence), executable: spec.executable, tags: spec.tags, config: spec.config }
}

/// A path the UI cannot name an executable for, refused before anything is sent.
fn missing(detail: String) -> CallEnd {
    CallEnd::Rejected { reason: "executable-missing".into(), detail }
}

/// The container provider's runtime image every node container runs from.
const CONTAINER_IMAGE: &str = "rafka-node-runtime:empty";

/// What a request is built from: the current Build, every live path with its create spec, and the primaries.
struct Current {
    paths: BTreeMap<PathName, NodeSpec>,
    ids: BTreeMap<PathName, rdm_core::NodeId>,
    rows: BTreeMap<PathName, NodeView>,
    primaries: BTreeSet<PathName>,
    mesh_primaries: BTreeMap<String, rdm_core::NodeId>,
    image: Option<String>,
}

/// The create spec of `path` in `mesh`, of `type`, on `executable`: its storage the demo's type's,
/// written out (node-admins and brokers keep theirs; gateways and compute are ephemeral), no tags, no
/// config.
fn spec(path: PathName, mesh: &str, r#type: &str, executable: Binding) -> NodeSpec {
    let storage_persistence = match r#type {
        "node-admin" | "broker" => StoragePersistence::Persistent { disposition: PersistentDisposition::Preserve },
        _ => StoragePersistence::Ephemeral,
    };
    NodeSpec { path: path.path.clone(), name: path.name.clone(), mesh: mesh.into(), r#type: r#type.to_string(), storage_persistence, executable: Some(executable), tags: Default::default(), config: Default::default() }
}
