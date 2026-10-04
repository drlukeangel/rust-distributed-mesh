//! The node-admin control API, as a client sees it (PRD §4, §7;
//! `docs/i143/design.md` §4).
//!
//! Every topology change is a Build request: the client submits it and gets
//! the Build's id back; it never starts or stops a runtime itself. Reads are
//! the views node-admin publishes. Refusals keep node-admin's named reason
//! (`{"error", "detail"}`) and HTTP status.
//!
//! The DTOs mirror node-admin core's JSON; `rafka-node-admin-core`'s
//! `client_contract` test pins that every route decodes into them.

use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId, NodeKind, PathName};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    Pending,
    ReadyForTraffic,
    Draining,
    Leaving,
    Dead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    Pending,
    ReadyForTraffic,
    Draining,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Process,
    Container,
}

/// `NodeView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    pub name: PathName,
    pub kind: NodeKind,
    pub mesh: String,
    pub node_id: NodeId,
    pub fabric_id: Option<String>,
    pub incarnation_id: Option<IncarnationId>,
    pub deployment_id: Option<String>,
    pub provider: Option<ProviderKind>,
    pub data_dir: Option<String>,
    pub status: NodeStatus,
    pub is_primary: bool,
    pub is_fabric_primary: bool,
    pub admin_api_base: Option<String>,
    pub endpoints: Vec<EndpointSlot>,
}

/// `MeshView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshView {
    pub id: String,
    pub name: String,
    pub status: ScopeStatus,
    pub primary_admin: Option<PathName>,
    pub admin_api_base: Option<String>,
    pub nodes: Vec<PathName>,
}

/// `FabricView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricView {
    pub name: String,
    pub status: ScopeStatus,
    pub provider: ProviderKind,
    pub fabric_primary: Option<PathName>,
    pub admin_api_base: Option<String>,
    pub meshes: Vec<MeshView>,
}

/// One mesh's desired counts (`POST /api/meshes`, and inside a fabric Build).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDesired {
    pub name: String,
    pub node_admin: u32,
    pub rpc_node: u32,
}

/// The whole fabric's desired meshes (`POST /api/build`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricDesired {
    pub fabric: String,
    pub meshes: Vec<MeshDesired>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    Pending,
    Running,
    Complete,
    Failed,
}

/// One step receipt of a Build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepView {
    pub attempt: u32,
    pub operation: String,
    pub step: String,
    /// `"complete"` or `{"failed": {"reason"}}`.
    pub outcome: serde_json::Value,
}

/// The folded Build view (`GET /api/builds?id=`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildView {
    pub build_id: BuildId,
    /// The Build's intent, tagged by `kind` (`add_node`, `restart_node`, ...).
    pub intent: serde_json::Value,
    pub traceparent: Option<String>,
    pub state: BuildState,
    pub attempt: u32,
    pub executor: Option<String>,
    pub steps: Vec<StepView>,
    pub last_failure: Option<String>,
}

/// Why a call did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Node-admin refused, with its status and named reason.
    Refused { status: u16, error: String, detail: String },
    /// Node-admin could not be reached or answered something undecodable.
    Transport { url: String, reason: String },
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

#[derive(Deserialize)]
struct Accepted {
    build_id: BuildId,
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
    /// `base` is what node-admin advertises (`RAFKA_NODE_ADMIN_API_BASE`).
    pub fn new(base: impl Into<String>) -> Self {
        Self { base: base.into().trim_end_matches('/').to_string(), http: reqwest::Client::new() }
    }

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
    pub async fn spawn(&self, mesh: &str, kind: NodeKind) -> Result<BuildId, ClientError> {
        let a: Accepted = self.post("/api/nodes/spawn", Some(serde_json::json!({ "mesh": mesh, "kind": kind }))).await?;
        Ok(a.build_id)
    }

    /// `DELETE /api/nodes/{name}`.
    pub async fn remove(&self, node: &PathName) -> Result<BuildId, ClientError> {
        let a: Accepted = self.delete(&format!("/api/nodes/{node}")).await?;
        Ok(a.build_id)
    }

    /// `POST /api/nodes/{name}/restart`.
    pub async fn restart(&self, node: &PathName) -> Result<BuildId, ClientError> {
        let a: Accepted = self.post(&format!("/api/nodes/{node}/restart"), None).await?;
        Ok(a.build_id)
    }

    /// `POST /api/build`: reconcile the whole fabric to `desired`.
    pub async fn build(&self, desired: &FabricDesired) -> Result<BuildId, ClientError> {
        let body = serde_json::to_value(desired).map_err(|e| ClientError::Transport { url: self.base.clone(), reason: e.to_string() })?;
        let a: Accepted = self.post("/api/build", Some(body)).await?;
        Ok(a.build_id)
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
    pub async fn create_mesh(&self, desired: &MeshDesired) -> Result<BuildId, ClientError> {
        let body = serde_json::to_value(desired).map_err(|e| ClientError::Transport { url: self.base.clone(), reason: e.to_string() })?;
        let a: Accepted = self.post("/api/meshes", Some(body)).await?;
        Ok(a.build_id)
    }

    /// `DELETE /api/meshes/{id|name}`.
    pub async fn remove_mesh(&self, id_or_name: &str) -> Result<BuildId, ClientError> {
        let a: Accepted = self.delete(&format!("/api/meshes/{id_or_name}")).await?;
        Ok(a.build_id)
    }

    /// `POST /api/shutdown`: runtime administration, not a Build.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.post::<serde_json::Value>("/api/shutdown", None).await.map(|_| ())
    }
}
