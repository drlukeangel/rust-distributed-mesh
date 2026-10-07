//! The Admin UI's topology routes: thin front doors to node-admin (PRD §4,
//! i143.e1.s6). Each one submits one Build through
//! `rafka-node-admin-client` and answers `202 {"build_id"}`, or node-admin's
//! refusal with its status and named reason. The UI never starts or stops a
//! runtime itself.
//!
//! ```text
//! POST   /api/nodes/spawn          {mesh, kind}  -> AddNode
//! POST   /api/nodes/{name}/restart               -> RestartNode
//! DELETE /api/nodes/{name}                       -> RemoveNode
//! POST   /api/bootstrap                          -> reconcile to the MN shape
//! ```

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::{Json, Router};
use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_admin_client::{BuildId, ClientError, FabricDesired, MeshDesired, NodeAdminClient};
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
        .with_state(Control { admin, events })
}

fn client(c: &Control) -> Result<&NodeAdminClient, Response> {
    c.admin.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "no-node-admin",
                "detail": "set RAFKA_NODE_ADMIN_API_BASE to the node-admin control API this UI drives",
            })),
        )
            .into_response()
    })
}

fn answer(c: &Control, what: &str, r: Result<BuildId, ClientError>, node: Option<&str>, mesh: Option<&str>) -> Response {
    match r {
        Ok(build_id) => {
            c.events.submitted(what, &build_id, node, mesh);
            tracing::info_span!("rafka.ui.build.create.via-node-admin", what, build_id = %build_id, "otel.kind" = "internal")
                .in_scope(|| tracing::info!(%build_id, "{what} submitted to node-admin"));
            (StatusCode::ACCEPTED, Json(json!({ "build_id": build_id }))).into_response()
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

/// The MN proof shape (PRD §1.13): `mesh1` with 2 node-admins and 3 rpc
/// nodes. One Build; node-admin computes what is missing.
async fn bootstrap(State(c): State<Control>) -> Response {
    let admin = match client(&c) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let fabric = match admin.fabric().await {
        Ok(f) => f.name,
        Err(e) => return answer(&c, "bootstrap", Err(e), None, None),
    };
    let desired = FabricDesired { fabric, meshes: vec![MeshDesired::of("mesh1", [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 3)])] };
    let r = admin.build(&desired).await;
    answer(&c, "bootstrap", r, None, Some("mesh1"))
}
