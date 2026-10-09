//! The Admin UI's topology routes: thin front doors to node-admin (PRD §4,
//! i143.e1.s6). Each one submits one Build through
//! `rafka-node-admin-client` and answers `202 {"build_id","attempt"}`, or node-admin's
//! refusal with its status and named reason. The UI never starts or stops a
//! runtime itself.
//!
//! ```text
//! POST   /api/nodes/spawn          {mesh, kind}  -> AddNode
//! POST   /api/nodes/{name}/restart               -> RestartNode
//! DELETE /api/nodes/{name}                       -> RemoveNode
//! POST   /api/bootstrap                          -> reconcile to the canonical R-shape x 2
//! POST   /api/meshes      {name}                -> CreateMesh (the canonical per-mesh shape)
//! DELETE /api/meshes/{name}                      -> RemoveMesh
//! ```

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::{Json, Router};
use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_admin_client::{Accepted, BuildId, ClientError, FabricDesired, MeshDesired, NodeAdminClient};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

/// Told about every Build the UI submitted (the UI's event timeline).
pub trait BuildEvents: Send + Sync {
    fn submitted(&self, what: &str, build_id: &BuildId, node: Option<&str>, mesh: Option<&str>);
}

#[derive(Clone)]
struct Control {
    admin: Option<NodeAdminClient>,
    events: Arc<dyn BuildEvents>,
}

/// The canonical R-shape mesh (tools/mesh-audit/i143-rshape-matrix.json `shapes.canonical.per_mesh`):
/// 2 node-admins, 3 gateways, 3 brokers, 2 computes.
pub const CANONICAL_PER_MESH: [(NodeKind, u32); 4] = [(NodeKind::NodeAdmin, 2), (NodeKind::Gateway, 3), (NodeKind::Broker, 3), (NodeKind::Compute, 2)];

/// The meshes the canonical R-shape holds.
pub const CANONICAL_MESHES: [&str; 2] = ["mesh1", "mesh2"];

#[derive(Deserialize)]
struct MeshRequest {
    name: String,
}

#[derive(Deserialize)]
struct SpawnRequest {
    mesh: String,
    kind: NodeKind,
}

/// The routes, driving the node-admin at `admin` (`None`: every route
/// answers a named 503).
pub fn router(admin: Option<NodeAdminClient>, events: Arc<dyn BuildEvents>) -> Router {
    Router::new()
        .route("/api/nodes/spawn", post(spawn))
        .route("/api/nodes/{node_name}/restart", post(restart))
        .route("/api/nodes/{node_name}", delete(remove))
        .route("/api/bootstrap", post(bootstrap))
        .route("/api/meshes", post(create_mesh))
        .route("/api/meshes/{mesh_name}", delete(remove_mesh))
        .with_state(Control { admin, events })
}

fn client(c: &Control) -> Result<&NodeAdminClient, Response> {
    c.admin.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "no-node-admin",
                "detail": "set RDM_NODE_ADMIN_API_BASE to the node-admin control API this UI drives",
            })),
        )
            .into_response()
    })
}

fn answer(c: &Control, what: &str, r: Result<Accepted, ClientError>, node: Option<&str>, mesh: Option<&str>) -> Response {
    match r {
        Ok(Accepted { build_id, attempt }) => {
            c.events.submitted(what, &build_id, node, mesh);
            tracing::info_span!("rdm.ui.build.create.via-node-admin", what, build_id = %build_id, "otel.kind" = "internal")
                .in_scope(|| tracing::info!(%build_id, "{what} submitted to node-admin"));
            (StatusCode::ACCEPTED, Json(json!({ "build_id": build_id, "attempt": attempt }))).into_response()
        }
        Err(ClientError::Refused { status, error, detail }) => {
            (StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY), Json(json!({ "error": error, "detail": detail })))
                .into_response()
        }
        Err(e @ ClientError::Transport { .. }) => {
            (StatusCode::BAD_GATEWAY, Json(json!({ "error": "node-admin-unreachable", "detail": e.to_string() }))).into_response()
        }
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
    if body.kind == NodeKind::RpcNode {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error": "kind-not-managed-in-the-r-shape", "detail": "the R-shape's nodes are node_admin, gateway, broker and compute; rpc_node is not one of them"}))).into_response();
    }
    let r = admin.spawn(&body.mesh, body.kind).await;
    answer(&c, "add node", r, None, Some(&body.mesh))
}

async fn restart(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let (admin, node) = match (client(&c), parse_node(&name)) {
        (Ok(a), Ok(n)) => (a, n),
        (Err(r), _) | (_, Err(r)) => return r,
    };
    let r = admin.restart(&node).await;
    answer(&c, "restart node", r, Some(&name), Some(&node.mesh))
}

async fn remove(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let (admin, node) = match (client(&c), parse_node(&name)) {
        (Ok(a), Ok(n)) => (a, n),
        (Err(r), _) | (_, Err(r)) => return r,
    };
    let r = admin.remove(&node).await;
    answer(&c, "remove node", r, Some(&name), Some(&node.mesh))
}

/// The canonical R-shape: `mesh1` and `mesh2`, each 2 node-admins, 3 gateways, 3 brokers and 2
/// computes. One Build; node-admin computes what is missing.
async fn bootstrap(State(c): State<Control>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let fabric = match admin.fabric().await {
        Ok(f) => f.name,
        Err(e) => return answer(&c, "bootstrap", Err(e), None, None),
    };
    let desired = FabricDesired { fabric, meshes: CANONICAL_MESHES.iter().map(|m| MeshDesired::of(*m, CANONICAL_PER_MESH)).collect() };
    let r = admin.build(&desired).await;
    answer(&c, "bootstrap", r, None, Some("mesh1,mesh2"))
}

/// A new mesh of `node_admin` node-admins and `rpc_node` rpc nodes: one Build; node-admin births
/// its first node-admin and the mesh fills itself.
async fn create_mesh(State(c): State<Control>, Json(body): Json<MeshRequest>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let desired = MeshDesired::of(&body.name, CANONICAL_PER_MESH);
    let r = admin.create_mesh(&desired).await;
    answer(&c, "create mesh", r, None, Some(&body.name))
}

/// Retire a whole mesh: one Build (members first, its node-admins last).
async fn remove_mesh(State(c): State<Control>, Path(name): Path<String>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let r = admin.remove_mesh(&name).await;
    answer(&c, "remove mesh", r, None, Some(&name))
}
