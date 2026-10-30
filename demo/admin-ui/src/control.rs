//! The Admin UI's topology routes: thin front doors to node-admin (PRD §4,
//! i143.e1.s6). Each one submits one Build over Node RPC through the typed objects of
//! `rafka-node-admin-client` and answers `202 {"build_id","attempt"}`, or node-admin's
//! refusal with its named reason. The UI never starts or stops a
//! runtime itself.
//!
//! ```text
//! POST   /api/nodes/spawn          {mesh, kind}  -> Reconcile: the topology with the next path of that kind
//! POST   /api/nodes/{name}/stop                  -> Stop (a restart is a stop, then a start: Q183)
//! POST   /api/nodes/{name}/start                 -> Start
//! DELETE /api/nodes/{name}                       -> RemoveNode
//! POST   /api/bootstrap   [{meshes: [{name, node_admin, gateway?, compute?, broker?}]}] -> one Build to that
//!                                                   shape (no body: the MN shape), Reconcile
//! POST   /api/meshes      {name, node_admin, gateway?, compute?, broker?} -> Reconcile: the topology
//!                                                   with the new mesh
//! DELETE /api/meshes/{name}                      -> RemoveMesh
//! POST   /api/builds/resubmit                    -> Reconcile: the current topology as it stands, each
//!                                                   node on the executable its row records
//! POST   /api/nodes/{name}/replace {executable}  -> Reconcile: the current topology with that node on the
//!                                                   executable at that file (its replacement)
//! ```
//!
//! The counts are the UI's vocabulary: it expands them into named paths, each on its executable
//! (`crate::admin::Admin`), before the typed call.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::{Json, Router};
use rdm_core::{NodeKind, PathName};
use crate::admin::Admin;
use rdm_client::{Accepted, BuildId, CallEnd, FabricDesired, MeshDesired};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

/// Told about every Build the UI submitted (the UI's event timeline).
pub trait BuildEvents: Send + Sync {
    fn submitted(&self, what: &str, build_id: &BuildId, node: Option<&str>, mesh: Option<&str>);
}

#[derive(Clone)]
struct Control {
    admin: Option<Arc<Admin>>,
    events: Arc<dyn BuildEvents>,
}

#[derive(Deserialize)]
struct MeshRequest {
    name: String,
    node_admin: u32,
    #[serde(default)]
    gateway: u32,
    #[serde(default)]
    compute: u32,
    #[serde(default)]
    broker: u32,
}

impl MeshRequest {
    fn desired(&self) -> MeshDesired {
        MeshDesired::of(
            &self.name,
            [(NodeKind::NodeAdmin, self.node_admin), (NodeKind::Gateway, self.gateway), (NodeKind::Compute, self.compute), (NodeKind::Broker, self.broker)],
        )
    }
}

/// The shape a bootstrap reconciles the fabric to; absent, the MN shape (PRD §1.13).
#[derive(Deserialize)]
struct BootstrapRequest {
    meshes: Vec<MeshRequest>,
}

#[derive(Deserialize)]
struct ReplaceRequest {
    /// The file the node's replacement runs, hashed by the UI.
    executable: std::path::PathBuf,
}

#[derive(Deserialize)]
struct SpawnRequest {
    mesh: String,
    kind: NodeKind,
}

/// The routes, driving the node-admin at `admin` (`None`: every route
/// answers a named 503).
pub fn router(admin: Option<Arc<Admin>>, events: Arc<dyn BuildEvents>) -> Router {
    Router::new()
        .route("/api/nodes/spawn", post(spawn))
        .route("/api/nodes/{node_name}/stop", post(stop))
        .route("/api/nodes/{node_name}/start", post(start))
        .route("/api/nodes/{node_name}/replace", post(replace))
        .route("/api/builds/resubmit", post(resubmit))
        .route("/api/nodes/{node_name}", delete(remove))
        .route("/api/bootstrap", post(bootstrap))
        .route("/api/meshes", post(create_mesh))
        .route("/api/meshes/{mesh_name}", delete(remove_mesh))
        .with_state(Control { admin, events })
}

fn client(c: &Control) -> Result<&Admin, Response> {
    c.admin.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "no-node-admin",
                "detail": "set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed (RDM_NODE_BIRTH=...) for this UI to drive",
            })),
        )
            .into_response()
    })
}

fn answer(c: &Control, what: &str, r: Result<Accepted, CallEnd>, node: Option<&str>, mesh: Option<&str>) -> Response {
    match r {
        Ok(Accepted { build_id, attempt }) => {
            c.events.submitted(what, &build_id, node, mesh);
            tracing::info!(%build_id, "{what} submitted to node-admin");
            (StatusCode::ACCEPTED, Json(json!({ "build_id": build_id, "attempt": attempt }))).into_response()
        }
        Err(CallEnd::Rejected { reason, detail }) => (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({ "error": reason, "detail": detail }))).into_response(),
        Err(e @ CallEnd::BuildInProgress { .. }) => (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({ "error": "build-in-progress", "detail": e.to_string() }))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": "node-admin-unreachable", "detail": e.to_string() }))).into_response(),
    }
}

fn parse_node(name: &str) -> Result<PathName, Response> {
    name.parse::<PathName>()
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": "invalid-request", "detail": e.to_string() }))).into_response())
}

async fn spawn(State(c): State<Control>, Json(body): Json<SpawnRequest>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let r = admin.spawn(&body.mesh, body.kind).await;
    answer(&c, "add node", r, None, Some(&body.mesh))
}

async fn stop(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let (admin, node) = match (client(&c), parse_node(&name)) {
        (Ok(a), Ok(n)) => (a, n),
        (Err(r), _) | (_, Err(r)) => return r,
    };
    let r = admin.stop(&node).await;
    answer(&c, "stop node", r, Some(&name), Some(&node.mesh))
}

async fn start(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let (admin, node) = match (client(&c), parse_node(&name)) {
        (Ok(a), Ok(n)) => (a, n),
        (Err(r), _) | (_, Err(r)) => return r,
    };
    let r = admin.start(&node).await;
    answer(&c, "start node", r, Some(&name), Some(&node.mesh))
}

async fn replace(State(c): State<Control>, Path(name): Path<String>, Json(body): Json<ReplaceRequest>) -> Response {
    let (admin, node) = match (client(&c), parse_node(&name)) {
        (Ok(a), Ok(n)) => (a, n),
        (Err(r), _) | (_, Err(r)) => return r,
    };
    let r = admin.replace(&node, body.executable).await;
    answer(&c, "replace executable", r, Some(&name), Some(&node.mesh))
}

/// The current topology submitted again as it stands: one Build with nothing to execute.
async fn resubmit(State(c): State<Control>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let r = admin.resubmit().await;
    answer(&c, "resubmit topology", r, None, None)
}

async fn remove(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let (admin, node) = match (client(&c), parse_node(&name)) {
        (Ok(a), Ok(n)) => (a, n),
        (Err(r), _) | (_, Err(r)) => return r,
    };
    let r = admin.remove(&node).await;
    answer(&c, "remove node", r, Some(&name), Some(&node.mesh))
}

/// One Build to the shape the body names; with no body, the MN proof shape (PRD §1.13): `mesh1`
/// with 2 node-admins and 3 rpc nodes. node-admin computes what is missing.
async fn bootstrap(State(c): State<Control>, body: axum::body::Bytes) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    // An empty body is the MN shape (a JSON content-type with no body is how the UI and its cell send it);
    // a body that is not a shape is refused by name.
    let body: Option<BootstrapRequest> = if body.iter().all(u8::is_ascii_whitespace) {
        None
    } else {
        match serde_json::from_slice(&body) {
            Ok(b) => Some(b),
            Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({ "error": "invalid-request", "detail": e.to_string() }))).into_response(),
        }
    };
    let fabric = match admin.fabric().await {
        Ok(f) => f.name,
        Err(e) => return answer(&c, "bootstrap", Err(e), None, None),
    };
    let meshes = match &body {
        Some(b) if !b.meshes.is_empty() => b.meshes.iter().map(MeshRequest::desired).collect(),
        // The demo is a rafka replica (Q295): a gateway and brokers, never an RDM rpc node.
        _ => vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::Gateway, 1), (NodeKind::Broker, 2)])],
    };
    let first = meshes.first().map(|m| m.name.clone()).unwrap_or_default();
    let desired = FabricDesired { fabric, meshes };
    let r = admin.build_fabric(desired).await;
    answer(&c, "bootstrap", r, None, Some(&first))
}

/// A new mesh of the counts the body names (node-admins, rpc nodes, gateways, computes, brokers):
/// one Build; node-admin births its first node-admin and the mesh fills itself.
async fn create_mesh(State(c): State<Control>, Json(body): Json<MeshRequest>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let r = admin.create_mesh(body.desired()).await;
    answer(&c, "create mesh", r, None, Some(&body.name))
}

/// Delete a whole mesh: one Build (members first, its node-admins last).
async fn remove_mesh(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let r = admin.remove_mesh(&name).await;
    answer(&c, "remove mesh", r, None, Some(&name))
}
