//! The demo admin UI server. `RDM_NODE_ADMIN_API_BASE` names the node-admin it reads and drives,
//! `RDM_EVIDENCE_DIR` the estate's evidence folder the Timeline reads, `RDM_UI_STATIC_DIR` the
//! built React app, `RDM_ADMIN_UI_BIND_ADDR` where it listens (default 127.0.0.1:19090).

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rafka_admin_ui::chaos::{Chaos, FABRIC_PRIMARY_REFUSAL};
use rafka_admin_ui::control::{self, BuildEvents};
use rafka_admin_ui::timeline::{Event, Evidence};
use rafka_admin_ui::view;
use rafka_node_admin_client::{BuildId, NodeAdminClient};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tower_http::services::ServeDir;

mod tests;

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    admin: Option<NodeAdminClient>,
    jaeger_url: String,
    chaos: Arc<Chaos>,
    evidence_dir: Option<PathBuf>,
    evidence: Arc<Mutex<Evidence>>,
    /// The UI's own actions, shown on the Timeline.
    ui_events: Arc<Mutex<Vec<Event>>>,
    /// The Builds this UI submitted, oldest first.
    submitted: Arc<Mutex<Vec<String>>>,
}

impl AppState {
    fn ui_event(&self, name: &str, node: &str, summary: String) {
        self.ui_events.lock().unwrap().push(Event { ts_ms: now_ms(), node: node.into(), name: name.into(), summary, high_volume: false, source: "ui" });
    }
}

struct Submitted(AppState);
impl BuildEvents for Submitted {
    fn submitted(&self, what: &str, build_id: &BuildId, node: Option<&str>, mesh: Option<&str>) {
        self.0.submitted.lock().unwrap().push(build_id.0.clone());
        self.0.ui_event("ui.build.create.via-node-admin", "ui", format!("{what} node={} mesh={} build_id={build_id}", node.unwrap_or("-"), mesh.unwrap_or("-")));
    }
}

fn no_node_admin() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no-node-admin", "detail": "set RDM_NODE_ADMIN_API_BASE to the node-admin control API this UI reads and drives"}))).into_response()
}

fn unreachable(e: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({"error": "node-admin-unreachable", "detail": e.to_string()}))).into_response()
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true}))
}

async fn overview(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    Json(view::overview(admin, &s.http).await).into_response()
}

async fn topology(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    match view::topology(admin, &s.http).await {
        Ok(t) => Json(t).into_response(),
        Err(e) => unreachable(e),
    }
}

async fn builds(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let ids = s.submitted.lock().unwrap().clone();
    Json(view::builds(admin, &s.http, &ids).await).into_response()
}

async fn chaos_state(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let nodes = match view::nodes(admin).await {
        Ok(n) => n,
        Err(e) => return unreachable(e),
    };
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| n["name"].clone());
    Json(json!({"fabric_primary": fp, "faults": s.chaos.faults(), "cuts": s.chaos.cuts()})).into_response()
}

#[derive(Deserialize)]
struct FaultBody {
    node: String,
    action: String,
}

/// A record answers 403 when the fabric-primary was aimed at, 200 otherwise (applied or refused by the kit).
fn answer(record: rafka_admin_ui::chaos::FaultRecord) -> Response {
    let status = if record.detail["refusal"] == FABRIC_PRIMARY_REFUSAL { StatusCode::FORBIDDEN } else { StatusCode::OK };
    (status, Json(record)).into_response()
}

async fn chaos_fault(State(s): State<AppState>, Json(b): Json<FaultBody>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let nodes = match view::nodes(admin).await {
        Ok(n) => view::facts(&n),
        Err(e) => return unreachable(e),
    };
    let r = s.chaos.fault(&nodes, &b.node, &b.action).await;
    s.ui_event(&format!("ui.chaos.fault.{}", r.outcome), &b.node, format!("{} {}: {}", b.action, b.node, r.detail));
    answer(r)
}

#[derive(Deserialize)]
struct CutBody {
    scope: String,
    target: String,
}

async fn chaos_cut(State(s): State<AppState>, Json(b): Json<CutBody>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let nodes = match view::nodes(admin).await {
        Ok(n) => view::facts(&n),
        Err(e) => return unreachable(e),
    };
    let r = s.chaos.cut(&nodes, &b.scope, &b.target).await;
    s.ui_event(&format!("ui.chaos.cut.{}", r.outcome), &b.target, format!("{} {}: {}", r.action, b.target, r.detail));
    answer(r)
}

#[derive(Deserialize)]
struct HealBody {
    id: u64,
}

async fn chaos_heal(State(s): State<AppState>, Json(b): Json<HealBody>) -> Response {
    let r = s.chaos.heal(b.id).await;
    s.ui_event(&format!("ui.chaos.heal.{}", r.outcome), &r.target, r.detail.to_string());
    answer(r)
}

#[derive(Deserialize)]
struct TimelineQuery {
    #[serde(default)]
    all: u8,
    #[serde(default = "default_limit")]
    limit: usize,
}
fn default_limit() -> usize {
    400
}

async fn timeline(State(s): State<AppState>, Query(q): Query<TimelineQuery>) -> Response {
    let Some(dir) = s.evidence_dir.clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no-evidence-dir", "detail": "set RDM_EVIDENCE_DIR to the estate's evidence folder the nodes write their span records into"}))).into_response();
    };
    let ui = s.ui_events.lock().unwrap().clone();
    let evidence = s.evidence.clone();
    let read = tokio::task::spawn_blocking(move || {
        let mut e = evidence.lock().unwrap();
        let files = e.refresh(&dir)?;
        let (events, hidden) = e.newest(&ui, q.all == 1, q.limit.min(2000));
        Ok::<_, std::io::Error>((files, events, hidden))
    })
    .await;
    match read {
        Ok(Ok((files, events, hidden))) => Json(json!({"events": events, "shown": events.len(), "hidden_high_volume": hidden, "files": files})).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "evidence-unreadable", "detail": e.to_string()}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "evidence-task-failed", "detail": e.to_string()}))).into_response(),
    }
}

#[derive(Deserialize)]
struct BootQuery {
    service: String,
}

/// The node's boot trace, from Jaeger.
async fn boot_trace(State(s): State<AppState>, Query(q): Query<BootQuery>) -> Response {
    let service = if q.service.contains(".admin.") { "rafka-node-admin" } else { "rafka-rpc-node" };
    let tags = serde_json::to_string(&json!({"node": q.service})).unwrap_or_default();
    let url = format!("{}/api/traces?service={service}&operation=rdm.mesh.node.update.via-ready&limit=1&lookback=2h&tags={}", s.jaeger_url, urlencoding::encode(&tags));
    match s.http.get(&url).send().await {
        Ok(r) => match r.json::<Value>().await {
            Ok(body) => match body["data"].as_array().and_then(|a| a.first()) {
                Some(first) => Json(json!({"data": [first]})).into_response(),
                None => (StatusCode::BAD_GATEWAY, Json(json!({"error": "no-boot-trace", "detail": format!("Jaeger at {} holds no boot trace for {}", s.jaeger_url, q.service)}))).into_response(),
            },
            Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "jaeger-undecodable", "detail": e.to_string()}))).into_response(),
        },
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "jaeger-unreachable", "detail": format!("{url}: {e}")}))).into_response(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = rafka_mesh_telemetry::init_telemetry("rafka-admin-ui");
    let bind = std::env::var("RDM_ADMIN_UI_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:19090".into());
    let static_dir = std::env::var("RDM_UI_STATIC_DIR").unwrap_or_else(|_| format!("{}/web/dist", env!("CARGO_MANIFEST_DIR")));
    let state = AppState {
        http: reqwest::Client::builder().timeout(Duration::from_secs(4)).build()?,
        admin: std::env::var("RDM_NODE_ADMIN_API_BASE").ok().filter(|b| !b.trim().is_empty()).map(NodeAdminClient::new),
        jaeger_url: std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://localhost:16686".into()),
        chaos: Arc::new(Chaos::default()),
        evidence_dir: std::env::var("RDM_EVIDENCE_DIR").ok().filter(|d| !d.trim().is_empty()).map(PathBuf::from),
        evidence: Arc::new(Mutex::new(Evidence::default())),
        ui_events: Arc::new(Mutex::new(Vec::new())),
        submitted: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/overview", get(overview))
        .route("/api/topology", get(topology))
        .route("/api/builds", get(builds))
        .route("/api/chaos", get(chaos_state))
        .route("/api/chaos/fault", post(chaos_fault))
        .route("/api/chaos/cut", post(chaos_cut))
        .route("/api/chaos/heal", post(chaos_heal))
        .route("/api/timeline", get(timeline))
        .route("/api/boot-trace", get(boot_trace))
        .fallback_service(ServeDir::new(&static_dir).append_index_html_on_directories(true))
        .with_state(state.clone())
        .merge(control::router(state.admin.clone(), Arc::new(Submitted(state))))
        .merge(tests::router());
    tracing::info!(%bind, %static_dir, "admin-ui listening");
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
