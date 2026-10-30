//! The node-admin this UI reads and drives, over Node RPC: RDM serves no HTTP API
//! (rdm-crate-and-test-layout.md §2), so the UI enters at the birth a node-admin printed at start
//! (`RDM_NODE_ADMIN_BIRTH`, the JSON after `RDM_NODE_BIRTH=`) and calls the typed objects there.
//!
//! Every Build the UI submits names each path's executable (proposal executable-bindings-at-create.md
//! rule 1): the UI is the door, expanding its counts into the complete topology, built from the current
//! Build `fabric.get` names and every node's row (`fabric.nodes.get`), each held path on the executable
//! its row records and each created path on the consumer's executable of its kind, found beside the
//! UI's own executable (`rshape-node-admin`, `rshape-gateway`, `rshape-compute`, `rshape-broker`). A
//! grow takes the lowest free ordinal of the kind; a shrink drops the highest ordinal that is not the
//! kind's primary.

use rdm_core::{NodeKind, PathName};
use rdm_client::{
    birth_of_view, Accepted, BuildCarrier, Build, BuildId, CallEnd, Change, Connections, ConnectionsView, ExactBirth, FabricDesired, FabricView, Fabrics,
    Filter, MeshDesired, MeshNode, NodeStatus, NodeView, Nodes, Peers, ProviderKind,
};
use rdm_core::binding::{sha256_file, Binding};
use rdm_core::meta::{NodeSpec, PersistentDisposition, PlacementMeta, StorageMeta};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

/// The node-admin the UI enters at, the peers it reaches over Node RPC, and the executable of each
/// kind the UI's Builds name.
pub struct Admin {
    peers: Arc<Peers>,
    entry: PathName,
    executables: BTreeMap<NodeKind, PathBuf>,
}

/// The consumer's executable of each kind found in `dir`.
fn executables_in(dir: &std::path::Path) -> BTreeMap<NodeKind, PathBuf> {
    [(NodeKind::NodeAdmin, "rshape-node-admin"), (NodeKind::Gateway, "rshape-gateway"), (NodeKind::Compute, "rshape-compute"), (NodeKind::Broker, "rshape-broker")]
        .into_iter()
        .map(|(kind, name)| (kind, dir.join(name)))
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
        Ok(Admin { peers, entry: birth.name.clone(), executables: beside })
    }

    /// This admin, its Builds naming `executables` by kind.
    pub fn with_executables(mut self, executables: BTreeMap<NodeKind, PathBuf>) -> Self {
        self.executables = executables;
        self
    }

    /// The topology a request is built from: the current Build `fabric.get` names, and every live
    /// path the rows hold (`node.get`) with the executable its row records.
    async fn current(&self) -> Result<Current, CallEnd> {
        let fabric = self.fabric().await?;
        let unread = |reason: String| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason };
        let based_on = fabric.build_id.as_deref().ok_or_else(|| unread("fabric.get names no current Build".into()))?;
        let based_on = BuildId::parse(based_on).map_err(|e| unread(format!("fabric.get names {based_on}, not a Build id: {e}")))?;
        let rows = self.rows().await?;
        let mut current = Current { based_on, paths: BTreeMap::new(), primaries: BTreeSet::new(), image: (fabric.provider == ProviderKind::Container).then(|| CONTAINER_IMAGE.to_string()) };
        for row in rows.into_iter().filter(|n| !n.name.old && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)) {
            if row.is_primary {
                current.primaries.insert(row.name.clone());
            }
            let executable = match row.executable {
                Some(b) => b,
                None => self.binding(row.kind, &current.image)?,
            };
            // The node's own tags and config, as its row holds them: the Build applies every property of
            // the spec that differs from the row (build.md §1), so the request names what the row holds.
            let spec = NodeSpec { tags: row.tags, config: row.config, ..spec(row.name.clone(), executable) };
            current.paths.insert(row.name, spec);
        }
        Ok(current)
    }

    /// The binding a created node of `kind` runs: the consumer's executable of the kind, hashed once,
    /// in `image`. A kind with no executable is refused `executable-missing` before anything is sent.
    fn binding(&self, kind: NodeKind, image: &Option<String>) -> Result<Binding, CallEnd> {
        let executable = self.executables.get(&kind).cloned().ok_or_else(|| missing(format!("the UI has no `{}` executable: a {} path cannot be named (named: {:?})", kind.name(), kind.name(), self.executables.keys().map(|k| k.name()).collect::<Vec<_>>())))?;
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
        Ok(Binding { launch_id: kind.name().into(), executable, sha256, image: image.clone() })
    }

    /// The complete topology with each of `meshes` resized to its counts; a mesh it leaves out is left
    /// out (its mesh.delete) when `whole`, kept otherwise.
    fn resized(&self, current: &Current, meshes: &[MeshDesired], whole: bool) -> Result<Vec<NodeSpec>, CallEnd> {
        let mut paths = current.paths.clone();
        if whole {
            paths.retain(|p, _| meshes.iter().any(|m| m.name == p.mesh));
        }
        for m in meshes {
            for (kind, want) in [(NodeKind::NodeAdmin, m.node_admin), (NodeKind::Broker, m.broker), (NodeKind::Gateway, m.gateway), (NodeKind::Compute, m.compute)] {
                let mut have: Vec<u32> = paths.keys().filter(|p| p.mesh == m.name && p.kind == kind).map(|p| p.ordinal).collect();
                have.sort_unstable();
                let keep = current.primaries.iter().find(|p| p.mesh == m.name && p.kind == kind).map(|p| p.ordinal);
                while (have.len() as u32) < want {
                    let ordinal = (1..).find(|o| !have.contains(o)).expect("a free ordinal");
                    have.push(ordinal);
                    have.sort_unstable();
                    let path = PathName::new(&m.name, kind, ordinal);
                    paths.insert(path.clone(), spec(path, self.binding(kind, &current.image)?));
                }
                while (have.len() as u32) > want {
                    let drop = have.iter().rev().copied().find(|o| Some(*o) != keep).or_else(|| have.last().copied()).expect("a member to drop");
                    have.retain(|o| *o != drop);
                    paths.remove(&PathName::new(&m.name, kind, drop));
                }
            }
        }
        Ok(paths.into_values().collect())
    }

    /// The counts `current` holds of `mesh`.
    fn counts_of(current: &Current, mesh: &str) -> MeshDesired {
        let of = |kind: NodeKind| current.paths.keys().filter(|p| p.mesh == mesh && p.kind == kind).count() as u32;
        MeshDesired { name: mesh.into(), node_admin: of(NodeKind::NodeAdmin), rpc_node: 0, broker: of(NodeKind::Broker), gateway: of(NodeKind::Gateway), compute: of(NodeKind::Compute) }
    }

    fn carrier(&self) -> BuildCarrier {
        self.peers.carrier_at(self.entry.clone())
    }

    /// `fabric.get` at the entry admin: the one fabric it holds.
    pub async fn fabric(&self) -> Result<FabricView, CallEnd> {
        Fabrics::new(self.carrier()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::NotFound, reason: format!("{} answered fabric.get with no fabric", self.entry) })
    }

    /// `node.get [mesh: m]` of every mesh the entry admin's view holds.
    pub async fn nodes(&self) -> Result<Vec<NodeView>, CallEnd> {
        let nodes = Nodes::new(self.carrier());
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

    /// `node.get` with no pair: every node's row, the executable it records included.
    pub async fn rows(&self) -> Result<Vec<NodeView>, CallEnd> {
        Nodes::new(self.carrier()).get(&Filter::new()).await
    }

    /// Put `key` = `value` on the config of the entry admin's own row, beside what the row holds: one
    /// `node.config.update` to the fabric-primary for its exact birth (node-config-update.md step 1),
    /// admitted at any time (Q166).
    pub async fn put_own_config(&self, key: &str, value: &str) -> Result<rdm_core::status::StatusReply, String> {
        let rows = self.rows().await.map_err(|e| format!("node.get: {e:?}"))?;
        let own = rows.iter().find(|n| n.name == self.entry && n.incarnation_id.is_some()).ok_or_else(|| format!("no row holds {}", self.entry))?;
        let primary = rows.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.is_fabric_primary).ok_or_else(|| "no row names the fabric-primary".to_string())?;
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

    /// node.create of a `kind` node in `mesh`, at the lowest free ordinal of its kind.
    pub async fn spawn(&self, mesh: &str, kind: NodeKind) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        if !current.paths.keys().any(|p| p.mesh == mesh) {
            return Err(CallEnd::Rejected { reason: "unknown-mesh".into(), detail: format!("the fabric holds no mesh {mesh}") });
        }
        let mut grown = Self::counts_of(&current, mesh);
        match kind {
            NodeKind::NodeAdmin => grown.node_admin += 1,
            // The demo is a rafka replica (Q295): it runs no RDM rpc node.
            NodeKind::RpcNode => return Err(CallEnd::Rejected { reason: "unknown-kind".into(), detail: "the demo runs no rpc node: spawn a gateway, a broker or a compute".into() }),
            NodeKind::Broker => grown.broker += 1,
            NodeKind::Gateway => grown.gateway += 1,
            NodeKind::Compute => grown.compute += 1,
        }
        let nodes = self.resized(&current, &[grown], false)?;
        Ok(Nodes::new(self.carrier()).create_named(current.based_on, nodes).await?.accepted().clone())
    }

    /// node.stop of `node`.
    pub async fn stop(&self, node: &PathName) -> Result<Accepted, CallEnd> {
        Ok(Nodes::new(self.carrier()).stop(node).await?.accepted().clone())
    }

    /// node.start of the stopped `node`.
    pub async fn start(&self, node: &PathName) -> Result<Accepted, CallEnd> {
        Ok(Nodes::new(self.carrier()).start(node).await?.accepted().clone())
    }

    /// node.delete of `node`.
    pub async fn remove(&self, node: &PathName) -> Result<Accepted, CallEnd> {
        Ok(Nodes::new(self.carrier()).delete(node).await?.accepted().clone())
    }

    /// build.create reconciling the fabric to `desired`.
    pub async fn build_fabric(&self, desired: FabricDesired) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        let nodes = self.resized(&current, &desired.meshes, true)?;
        let s = self.carrier().create(Change::Reconcile { topology_storage_provider: None, based_on: current.based_on, nodes }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create of a new mesh with `desired`'s counts.
    pub async fn create_mesh(&self, desired: MeshDesired) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        if current.paths.keys().any(|p| p.mesh == desired.name) {
            return Err(CallEnd::Rejected { reason: "mesh-already-exists".into(), detail: format!("the fabric holds mesh {} already", desired.name) });
        }
        let nodes = self.resized(&current, std::slice::from_ref(&desired), false)?;
        let s = self.carrier().create(Change::Reconcile { topology_storage_provider: None, based_on: current.based_on, nodes }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create of the current topology as it stands: every live path on the executable its row records,
    /// based on the current Build. It changes nothing any node runs, so its Build has nothing to execute
    /// and is Complete once the fabric holds ready (states.md "Resize and executable replacement").
    pub async fn resubmit(&self) -> Result<Accepted, CallEnd> {
        let current = self.current().await?;
        let nodes = current.paths.into_values().collect();
        let s = self.carrier().create(Change::Reconcile { topology_storage_provider: None, based_on: current.based_on, nodes }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create of the current topology with `node`'s entry naming the executable at `executable`,
    /// hashed here: its replacement, which RDM realizes as node.delete then node.create at the path
    /// (build-deployment.md §3). A path the rows hold no live node at is refused `unknown-node`.
    pub async fn replace(&self, node: &PathName, executable: PathBuf) -> Result<Accepted, CallEnd> {
        let mut current = self.current().await?;
        let sha256 = sha256_file(&executable).map_err(|e| missing(format!("{} could not be hashed: {e}", executable.display())))?;
        let image = current.image.clone();
        let spec = current.paths.get_mut(node).ok_or_else(|| CallEnd::Rejected { reason: "unknown-node".into(), detail: format!("the fabric holds no live node at {node}") })?;
        spec.executable = Some(Binding { launch_id: node.kind.name().into(), executable, sha256, image });
        let nodes = current.paths.into_values().collect();
        let s = self.carrier().create(Change::Reconcile { topology_storage_provider: None, based_on: current.based_on, nodes }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }

    /// build.create removing the mesh `mesh`.
    pub async fn remove_mesh(&self, mesh: &str) -> Result<Accepted, CallEnd> {
        let s = self.carrier().create(Change::RemoveMesh { mesh: mesh.to_string() }).await?;
        Ok(Accepted { build_id: s.build_id().clone(), attempt: s.attempt() })
    }
}

/// A path the UI cannot name an executable for, refused before anything is sent.
fn missing(detail: String) -> CallEnd {
    CallEnd::Rejected { reason: "executable-missing".into(), detail }
}

/// The container provider's runtime image every node container runs from.
const CONTAINER_IMAGE: &str = "rafka-node-runtime:empty";

/// What a request is built from: the current Build, every live path with its create spec, and the primaries.
struct Current {
    based_on: BuildId,
    paths: BTreeMap<PathName, NodeSpec>,
    primaries: BTreeSet<PathName>,
    image: Option<String>,
}

/// The create spec of `path` on `executable`: its storage by kind, written out (node-admins and
/// brokers keep theirs; gateways, compute and rpc nodes are ephemeral), no placement, no tags, no config.
fn spec(path: PathName, executable: Binding) -> NodeSpec {
    let storage_persistence = match path.kind {
        NodeKind::NodeAdmin | NodeKind::Broker => StorageMeta::Persistent { disposition: PersistentDisposition::Preserve },
        NodeKind::Gateway | NodeKind::Compute | NodeKind::RpcNode => StorageMeta::Ephemeral,
    };
    NodeSpec { path, storage_persistence, placement: PlacementMeta::default(), executable: Some(executable), tags: Default::default(), config: Default::default() }
}
