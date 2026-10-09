//! The node-admin control API, as a client sees it.
//!
//! Every topology change is a Build request: the client submits it and gets
//! the Build's id back; it never starts or stops a runtime itself. Reads are
//! the views node-admin publishes. Refusals keep node-admin's named reason
//! (`{"error", "detail"}`) and HTTP status.
//!
//! The DTOs mirror node-admin core's JSON; `rafka-node-admin-core`'s
//! `client_contract` test pins that every route decodes into them.
#![deny(missing_docs)]


use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
/// The executable-binding contract an operator hands node-admin (`RDM_EXECUTABLE_BINDINGS`).
pub use rafka_mesh_entity::binding;
pub use rafka_mesh_entity::NodeKind as LaunchKind;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A Build's id as node-admin returns it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BuildId(pub String);

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A node's lifecycle state as node-admin reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Born and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work while it finishes what it holds.
    Draining,
    /// Leaving: it has announced its departure and is closing.
    Leaving,
    /// Unheard past the staleness floor: held, never death, and never a reason to restart or
    /// delete it; its next digest flips it back.
    PendingReconnect,
    /// Commanded silence: its mesh executor is restarting this exact birth and owns bringing it
    /// back.
    Restarting,
    /// True offline: the mesh primary's connect found no path on two rounds a staleness floor
    /// apart. Inferred by an observer, never announced by the node.
    Dead,
}

/// The lifecycle state of a mesh or of a fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work while it winds down.
    Draining,
    /// Retired: it has been taken out of service.
    Retired,
    /// Degraded: the fabric primary has decided a peer mesh is reborn.
    Degraded,
}

/// The deployment provider that runs a fabric's nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Each node is an OS process.
    Process,
    /// Each node is a container.
    Container,
}

/// `NodeView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's kind.
    pub kind: NodeKind,
    /// The name of the mesh the node belongs to.
    pub mesh: String,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's fabric endpoint id, once known.
    pub endpoint_id: Option<String>,
    /// The incarnation of the node's current birth, once launched.
    pub incarnation_id: Option<IncarnationId>,
    /// The deployment that runs the node, once created.
    pub deployment_id: Option<String>,
    /// The provider that runs the node.
    pub provider: Option<ProviderKind>,
    /// The node's data directory.
    pub data_dir: Option<String>,
    /// The node's lifecycle state.
    pub status: NodeStatus,
    /// Whether the node holds its mesh's primary seat.
    pub is_primary: bool,
    /// Whether the node holds the fabric-primary seat.
    pub is_fabric_primary: bool,
    /// The control API base the node advertises, for a node-admin.
    pub admin_api_base: Option<String>,
    /// The address the node's mesh transport is bound to.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// The named listeners the node serves, each with its bound address.
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The lifecycle state the birth declared to its authority, once applied.
    #[serde(default)]
    pub declared: Option<String>,
    /// The node's CPU and RAM from its latest digest, as the answering admin holds it.
    #[serde(default)]
    pub load: Option<rafka_mesh_entity::NodeLoad>,
}

/// `MeshView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshView {
    /// The mesh's minted id.
    pub id: String,
    /// The mesh's name.
    pub name: String,
    /// The mesh's lifecycle state.
    pub status: ScopeStatus,
    /// The node-admin that is the mesh's primary, when one is seated.
    pub primary_admin: Option<PathName>,
    /// The control API base of the mesh's primary.
    pub admin_api_base: Option<String>,
    /// The `path.name` of every node of the mesh.
    pub nodes: Vec<PathName>,
}

/// `FabricView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricView {
    /// The fabric's name.
    pub name: String,
    /// The fabric's lifecycle state.
    pub status: ScopeStatus,
    /// The provider that runs the fabric's nodes.
    pub provider: ProviderKind,
    /// The node-admin that is the fabric primary, when one is seated.
    pub fabric_primary: Option<PathName>,
    /// The control API base of the fabric primary.
    pub admin_api_base: Option<String>,
    /// The fabric's meshes.
    pub meshes: Vec<MeshView>,
}

/// One mesh's desired counts (`POST /api/meshes`, and inside a fabric Build).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDesired {
    /// The mesh's name.
    pub name: String,
    /// The number of node-admins.
    pub node_admin: u32,
    /// The number of rpc nodes.
    #[serde(default)]
    pub rpc_node: u32,
    /// The number of brokers.
    #[serde(default)]
    pub broker: u32,
    /// The number of gateways.
    #[serde(default)]
    pub gateway: u32,
    /// The number of compute nodes.
    #[serde(default)]
    pub compute: u32,
}

impl MeshDesired {
    /// A mesh named `name` with `counts` (`rafka_mesh_entity::NodeKind`, count); every other kind at 0.
    pub fn of(name: impl Into<String>, counts: impl IntoIterator<Item = (NodeKind, u32)>) -> Self {
        let mut d = MeshDesired { name: name.into(), node_admin: 0, rpc_node: 0, broker: 0, gateway: 0, compute: 0 };
        for (k, n) in counts {
            match k {
                NodeKind::NodeAdmin => d.node_admin = n,
                NodeKind::RpcNode => d.rpc_node = n,
                NodeKind::Broker => d.broker = n,
                NodeKind::Gateway => d.gateway = n,
                NodeKind::Compute => d.compute = n,
            }
        }
        d
    }
}

/// The whole fabric's desired meshes (`POST /api/build`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricDesired {
    /// The fabric's name.
    pub fabric: String,
    /// The fabric's meshes with their desired counts.
    pub meshes: Vec<MeshDesired>,
}

/// Where a Build is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// Accepted and not yet executed.
    Pending,
    /// An attempt is executing.
    Running,
    /// Every step is complete.
    Complete,
    /// The current attempt failed.
    Failed,
}

/// One step receipt of a Build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepView {
    /// The attempt the receipt belongs to.
    pub attempt: u32,
    /// The operation the step belongs to.
    pub operation: String,
    /// The step.
    pub step: String,
    /// `"complete"` or `{"failed": {"reason"}}`.
    pub outcome: serde_json::Value,
}

/// The folded Build view (`GET /api/builds?id=`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildView {
    /// The Build's id.
    pub build_id: BuildId,
    /// The complete accepted topology this Build realizes: per mesh, its node `path.name`s.
    pub topology: serde_json::Value,
    /// The change that produced it, tagged by `kind` (`add_node`, `remove_node`, ...): history.
    #[serde(default)]
    pub submitted_change: Option<serde_json::Value>,
    /// When the Build was submitted, in milliseconds since the Unix epoch.
    pub submitted_at_ms: u64,
    /// Where the Build is in its life.
    pub state: BuildState,
    /// The current attempt, counting from 1.
    pub attempt: u32,
    /// The executor running the current attempt, when one is.
    pub executor: Option<String>,
    /// The step receipts of the Build.
    pub steps: Vec<StepView>,
    /// The reason the last attempt failed, when one did.
    pub last_failure: Option<String>,
    /// Why the current attempt exists (`requested`, `proven-drift`, `restart`, `replace`, ...).
    pub reason: String,
    /// The fenced action the current attempt carries, if any.
    #[serde(default)]
    pub action: Option<serde_json::Value>,
}

/// Why a call did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Node-admin refused, with its status and named reason.
    /// Node-admin refused the call.
    Refused {
        /// The HTTP status of the refusal.
        status: u16,
        /// The named reason.
        error: String,
        /// The detail node-admin gave with the reason.
        detail: String,
    },
    /// Node-admin could not be reached or answered something undecodable.
    /// The call did not complete.
    Transport {
        /// The URL that was called.
        url: String,
        /// Why the call failed.
        reason: String,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { status, error, detail } => write!(f, "node-admin refused ({status} {error}): {detail}"),
            Self::Transport { url, reason } => write!(f, "node-admin at {url}: {reason}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// The 202 of a request that opens an attempt: the Build and the attempt this request opened
/// (attempt 1 for a Build it accepted). Wait for exactly this attempt, never for the Build alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accepted {
    /// The Build the request opened or joined.
    pub build_id: BuildId,
    /// The attempt the request opened.
    pub attempt: u32,
}

#[derive(Deserialize)]
struct Nodes {
    nodes: Vec<NodeView>,
}

#[derive(Deserialize, Default)]
struct RefusalBody {
    #[serde(default)]
    error: String,
    #[serde(default)]
    detail: String,
}

/// A client of one node-admin's control API.
#[derive(Debug, Clone)]
pub struct NodeAdminClient {
    base: String,
    http: reqwest::Client,
}

impl NodeAdminClient {
    /// `base` is what node-admin advertises (`RDM_NODE_ADMIN_API_BASE`).
    pub fn new(base: impl Into<String>) -> Self {
        Self { base: base.into().trim_end_matches('/').to_string(), http: reqwest::Client::new() }
    }

    /// The control API base this client calls.
    pub fn base(&self) -> &str {
        &self.base
    }

    async fn send<T: serde::de::DeserializeOwned>(&self, req: reqwest::RequestBuilder, url: &str) -> Result<T, ClientError> {
        let transport = |reason: String| ClientError::Transport { url: url.to_string(), reason };
        let res = req.send().await.map_err(|e| transport(e.to_string()))?;
        let status = res.status();
        let bytes = res.bytes().await.map_err(|e| transport(e.to_string()))?;
        if !status.is_success() {
            let body: RefusalBody = serde_json::from_slice(&bytes).unwrap_or_default();
            return Err(ClientError::Refused { status: status.as_u16(), error: body.error, detail: body.detail });
        }
        let bytes = if bytes.is_empty() { &b"null"[..] } else { &bytes[..] };
        serde_json::from_slice(bytes).map_err(|e| transport(format!("undecodable answer: {e}")))
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let url = format!("{}{path}", self.base);
        self.send(self.http.get(&url), &url).await
    }

    async fn post<T: serde::de::DeserializeOwned>(&self, path: &str, body: Option<serde_json::Value>) -> Result<T, ClientError> {
        let url = format!("{}{path}", self.base);
        let req = self.http.post(&url);
        let req = match body {
            Some(b) => req.json(&b),
            None => req,
        };
        self.send(req, &url).await
    }

    async fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let url = format!("{}{path}", self.base);
        self.send(self.http.delete(&url), &url).await
    }

    /// `POST /api/nodes/spawn`: add one node of `kind` to `mesh`.
    pub async fn spawn(&self, mesh: &str, kind: NodeKind) -> Result<Accepted, ClientError> {
        let a: Accepted = self.post("/api/nodes/spawn", Some(serde_json::json!({ "mesh": mesh, "kind": kind }))).await?;
        Ok(a)
    }

    /// `DELETE /api/nodes/{name}`.
    pub async fn remove(&self, node: &PathName) -> Result<Accepted, ClientError> {
        let a: Accepted = self.delete(&format!("/api/nodes/{node}")).await?;
        Ok(a)
    }

    /// `POST /api/nodes/{name}/restart`.
    pub async fn restart(&self, node: &PathName) -> Result<Accepted, ClientError> {
        let a: Accepted = self.post(&format!("/api/nodes/{node}/restart"), None).await?;
        Ok(a)
    }

    /// `POST /api/nodes/{name}/replace`: the next attempt of the accepted Build retires the live
    /// birth and creates a new node at the path.
    pub async fn replace(&self, node: &PathName) -> Result<Accepted, ClientError> {
        let a: Accepted = self.post(&format!("/api/nodes/{node}/replace"), None).await?;
        Ok(a)
    }

    /// `POST /api/nodes/{name}/replace?incarnation=`: [`replace`](Self::replace) of the exact birth
    /// `incarnation`, for a node the Build names that the admin does not hear.
    pub async fn replace_birth(&self, node: &PathName, incarnation: &str) -> Result<Accepted, ClientError> {
        let a: Accepted = self.post(&format!("/api/nodes/{node}/replace?incarnation={incarnation}"), None).await?;
        Ok(a)
    }

    /// `POST /api/build`: reconcile the whole fabric to `desired`.
    pub async fn build(&self, desired: &FabricDesired) -> Result<Accepted, ClientError> {
        let body = serde_json::to_value(desired).map_err(|e| ClientError::Transport { url: self.base.clone(), reason: e.to_string() })?;
        let a: Accepted = self.post("/api/build", Some(body)).await?;
        Ok(a)
    }

    /// `GET /api/builds?id=`.
    pub async fn build_view(&self, id: &BuildId) -> Result<BuildView, ClientError> {
        self.get(&format!("/api/builds?id={id}")).await
    }

    /// `DELETE /api/builds?id=`: drop a finished Build from history.
    pub async fn forget(&self, id: &BuildId) -> Result<(), ClientError> {
        self.delete::<serde_json::Value>(&format!("/api/builds?id={id}")).await.map(|_| ())
    }

    /// `GET /api/nodes`.
    pub async fn nodes(&self) -> Result<Vec<NodeView>, ClientError> {
        Ok(self.get::<Nodes>("/api/nodes").await?.nodes)
    }

    /// `GET /api/meshes/{id|name}`.
    pub async fn mesh(&self, id_or_name: &str) -> Result<MeshView, ClientError> {
        self.get(&format!("/api/meshes/{id_or_name}")).await
    }

    /// `GET /api/fabric`.
    pub async fn fabric(&self) -> Result<FabricView, ClientError> {
        self.get("/api/fabric").await
    }

    /// `POST /api/meshes`.
    pub async fn create_mesh(&self, desired: &MeshDesired) -> Result<Accepted, ClientError> {
        let body = serde_json::to_value(desired).map_err(|e| ClientError::Transport { url: self.base.clone(), reason: e.to_string() })?;
        let a: Accepted = self.post("/api/meshes", Some(body)).await?;
        Ok(a)
    }

    /// `DELETE /api/meshes/{id|name}`.
    pub async fn remove_mesh(&self, id_or_name: &str) -> Result<Accepted, ClientError> {
        let a: Accepted = self.delete(&format!("/api/meshes/{id_or_name}")).await?;
        Ok(a)
    }

    /// `POST /api/shutdown`: runtime administration, not a Build.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.post::<serde_json::Value>("/api/shutdown", None).await.map(|_| ())
    }
}
