//! The demo admin UI server. `RDM_NODE_ADMIN_BIRTH` (the birth a node-admin printed at start) names the
//! node-admin it reads and drives over Node RPC,
//! `RDM_EVIDENCE_DIR` the estate's evidence folder the Timeline reads, `RDM_UI_STATIC_DIR` the
//! built React app, `RDM_ADMIN_UI_BIND_ADDR` where it listens (default 127.0.0.1:0: the operating system picks the port, and
//! the UI puts the port it bound on its admin's own row config as `admin_ui_port`).

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rdm_admin_ui::chaos::{Chaos, FABRIC_PRIMARY_REFUSAL};
use rdm_admin_ui::control::{self, BuildEvents};
use rdm_admin_ui::timeline::{Event, Evidence};
use rdm_admin_ui::view;
use rdm_admin_ui::admin::Admin;
use rdm_client::BuildId;
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
    admin: Option<Arc<Admin>>,
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
        self.0.submitted.lock().unwrap().push(build_id.to_string());
        self.0.ui_event("ui.build.create.via-node-admin", "ui", format!("{what} node={} mesh={} build_id={build_id}", node.unwrap_or("-"), mesh.unwrap_or("-")));
    }
}

fn no_node_admin() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no-node-admin", "detail": "set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed (RDM_NODE_BIRTH=...) for this UI to read and drive"}))).into_response()
}

fn unreachable(e: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({"error": "node-admin-unreachable", "detail": e.to_string()}))).into_response()
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true}))
}

async fn overview(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    Json(view::overview(admin).await).into_response()
}

async fn topology(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    match view::topology(admin).await {
        Ok(t) => Json(t).into_response(),
        Err(e) => unreachable(e),
    }
}

async fn builds(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let ids = s.submitted.lock().unwrap().clone();
    Json(view::builds(admin, &ids).await).into_response()
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
fn answer(record: rdm_admin_ui::chaos::FaultRecord) -> Response {
    let status = if record.detail["refusal"] == FABRIC_PRIMARY_REFUSAL { StatusCode::FORBIDDEN } else { StatusCode::OK };
    (status, Json(record)).into_response()
}

async fn chaos_fault(State(s): State<AppState>, Json(b): Json<FaultBody>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let nodes = match view::nodes(admin).await {
        Ok(n) => view::facts(&n),
        Err(e) => return unreachable(e),
    };
    let fabric = admin.fabric().await.ok().map(|f| f.status);
    let r = s.chaos.fault(&nodes, &b.node, &b.action, fabric).await;
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

/// The node's boot trace, from Jaeger: by the trace id of the node's own boot span in the estate's
/// evidence (any consumer's service names; Jaeger's by-id read, which its v1 and v2 query APIs both
/// serve), else, with no evidence folder, by Jaeger's v1 search under the built-in service names.
async fn boot_trace(State(s): State<AppState>, Query(q): Query<BootQuery>) -> Response {
    let from_evidence = match s.evidence_dir.clone() {
        Some(dir) => {
            let evidence = s.evidence.clone();
            let node = q.service.clone();
            tokio::task::spawn_blocking(move || {
                let mut e = evidence.lock().unwrap();
                let _ = e.refresh(&dir);
                e.boot_trace_of(&node)
            })
            .await
            .ok()
            .flatten()
        }
        None => None,
    };
    let url = match from_evidence {
        Some(trace) => format!("{}/api/traces/{trace}", s.jaeger_url),
        None => {
            let service = if q.service.contains(".admin.") { "rdm-node-admin" } else { "rdm-rpc-node" };
            let tags = serde_json::to_string(&json!({"node": q.service})).unwrap_or_default();
            format!("{}/api/traces?service={service}&operation=rdm.mesh.node.update.via-ready&limit=1&lookback=2h&tags={}", s.jaeger_url, urlencoding::encode(&tags))
        }
    };
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

/// Put the port the UI bound on its admin's own row config (`admin_ui_port`), once the fabric takes it:
/// a refusal before the first ready is put again a second later.
async fn put_ui_port(admin: Arc<Admin>, port: u16) {
    loop {
        match admin.put_own_config("admin_ui_port", &port.to_string()).await {
            Ok(rdm_core::status::StatusReply::ConfigUpdated { .. }) => {
                tracing::info!(port, "the UI's port is on its admin's row config as admin_ui_port");
                return;
            }
            other => tracing::info!(port, ?other, "the UI's port is not on its admin's row config yet"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = rdm_transport::telemetry::init_telemetry("rdm-admin-ui");
    let bind = std::env::var("RDM_ADMIN_UI_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:0".into());
    let static_dir = std::env::var("RDM_UI_STATIC_DIR").unwrap_or_else(|_| format!("{}/web/dist", env!("CARGO_MANIFEST_DIR")));
    let state = AppState {
        http: reqwest::Client::builder().timeout(Duration::from_secs(4)).build()?,
        admin: Admin::from_env().await.map_err(anyhow::Error::msg)?.map(Arc::new),
        jaeger_url: std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://localhost:16686".into()),
        chaos: Arc::new(Chaos::default()),
        evidence_dir: std::env::var("RDM_EVIDENCE_DIR").ok().filter(|d| !d.trim().is_empty()).map(PathBuf::from),
        evidence: Arc::new(Mutex::new(Evidence::default())),
        ui_events: Arc::new(Mutex::new(Vec::new())),
        submitted: Arc::new(Mutex::new(Vec::new())),
    };
    let own_admin = state.admin.clone();
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
    let port = listener.local_addr()?.port();
    if let Some(admin) = own_admin {
        tokio::spawn(put_ui_port(admin, port));
    }
    axum::serve(listener, app).await?;
    Ok(())
}
