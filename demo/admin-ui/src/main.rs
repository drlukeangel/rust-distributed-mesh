//! The demo admin UI server. `RDM_NODE_ADMIN_API_BASE` names the node-admin it reads and drives,
//! `RDM_EVIDENCE_DIR` the estate's evidence folder the Timeline reads, `RDM_UI_STATIC_DIR` the
//! built React app, `RDM_ADMIN_UI_BIND_ADDR` where it listens (default 127.0.0.1:19090).

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rafka_admin_ui::alerts::{Alerts, Thresholds};
use rafka_admin_ui::chaos::{Chaos, FABRIC_PRIMARY_REFUSAL};
use rafka_admin_ui::control::{self, BuildEvents};
use rafka_admin_ui::timeline::{Event, Evidence};
use rafka_admin_ui::view;
use rafka_node_admin_client::{BuildId, NodeAdminClient};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use tracing::Instrument;
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
    /// The alerts raised: node state, load thresholds, every chaos action.
    alerts: Arc<Mutex<Alerts>>,
    /// The traffic driver's status file (`traffic.json` of `rshape-demo`), when one drives this estate.
    traffic_file: Option<PathBuf>,
}

impl AppState {
    fn alert(&self, severity: &'static str, kind: &'static str, message: String, node: Option<&str>, mesh: Option<&str>) {
        self.alerts.lock().unwrap().raise(now_ms(), severity, kind, message, node, mesh);
    }

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
    let mut v = view::overview(admin, &s.http).await;
    // The old header line: spawned, meshes, chaos per minute, mean peers.
    let names: Vec<String> = view::nodes(admin).await.unwrap_or_default().iter().filter_map(|n| n["name"].as_str().map(String::from)).collect();
    let mean_peers = match s.evidence_dir.clone() {
        Some(dir) => {
            let evidence = s.evidence.clone();
            let n = names.clone();
            tokio::task::spawn_blocking(move || {
                let mut e = evidence.lock().unwrap();
                let _ = e.refresh(&dir);
                e.mean_peers(&n)
            })
            .await
            .ok()
            .flatten()
        }
        None => None,
    };
    let since = now_ms().saturating_sub(60_000);
    let chaos = s.chaos.faults().iter().filter(|f| f.ts_ms >= since && f.outcome == "applied").count();
    v["summary"] = json!({"spawned": names.len(), "meshes": v["meshes"].as_array().map(|m| m.iter().filter_map(|x| x["name"].as_str()).collect::<Vec<_>>()).unwrap_or_default(), "chaos_per_min": chaos, "mean_peers": mean_peers});
    Json(v).into_response()
}

#[derive(Deserialize)]
struct MessagesQuery {
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default = "default_msg_limit")]
    limit: usize,
    #[serde(default)]
    q: String,
}
fn default_kind() -> String {
    "all".into()
}
fn default_msg_limit() -> usize {
    300
}

/// A raw id in a span (`ExactNode(NodeId("x"))`, a node id, an endpoint id) as the node's name.
fn name_of(raw: &str, by_id: &std::collections::HashMap<String, String>) -> String {
    let inner = raw.split("NodeId(\"").nth(1).and_then(|r| r.split('"').next()).unwrap_or(raw);
    by_id.get(inner).cloned().unwrap_or_else(|| if inner.len() == 64 && inner.chars().all(|c| c.is_ascii_hexdigit()) { format!("endpoint {}…", &inner[..8]) } else { raw.to_string() })
}

/// The live feed of messages the nodes exchange: Node RPC calls and serves (op, caller to target,
/// outcome, elapsed) and the gossip channels' seat, forwarded and backbone activity.
async fn messages(State(s): State<AppState>, Query(q): Query<MessagesQuery>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let Some(dir) = s.evidence_dir.clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no-evidence-dir", "detail": "set RDM_EVIDENCE_DIR to the estate's evidence folder"}))).into_response();
    };
    let by_id: std::collections::HashMap<String, String> = view::nodes(admin)
        .await
        .unwrap_or_default()
        .iter()
        .flat_map(|n| ["node_id", "endpoint_id"].into_iter().filter_map(|k| Some((n[k].as_str()?.to_string(), n["name"].as_str()?.to_string()))).collect::<Vec<_>>())
        .collect();
    let evidence = s.evidence.clone();
    let (kind, limit) = (q.kind.clone(), q.limit.min(2000));
    let raw = tokio::task::spawn_blocking(move || {
        let mut e = evidence.lock().unwrap();
        let _ = e.refresh(&dir);
        // Over-read so the text filter still fills the page.
        e.messages(&kind, limit * 8)
    })
    .await
    .unwrap_or_default();
    let needle = q.q.to_lowercase();
    let rows: Vec<Value> = raw
        .into_iter()
        .map(|m| {
            let m = rafka_admin_ui::timeline::Message { node: if m.node.starts_with("rafka-rpc-probe") { "traffic probe".into() } else { m.node.clone() }, ..m };
            let from_to = match m.span.as_str() {
                "rdm.node_rpc.request.update.via-call" => (m.node.clone(), name_of(&m.target, &by_id)),
                s if s.starts_with("rdm.node_rpc.request.serve") || s.starts_with("rdm.node_rpc.request.reject") => (name_of(&m.peer, &by_id), m.node.clone()),
                _ => (m.node.clone(), m.target.clone()),
            };
            json!({"ts_ms": m.ts_ms, "kind": m.kind, "span": m.span, "node": m.node, "from": from_to.0, "to": from_to.1, "op": m.op, "protocol": m.protocol, "outcome": m.outcome, "elapsed_ms": m.elapsed_ms, "detail": m.detail})
        })
        .filter(|r| needle.is_empty() || r.to_string().to_lowercase().contains(&needle))
        .take(limit)
        .collect();
    Json(json!({"messages": rows})).into_response()
}

async fn topology(State(s): State<AppState>) -> Response {
    let Some(admin) = &s.admin else { return no_node_admin() };
    let (facts, listeners, publishers) = evidence_signals(&s).await;
    let cuts: Vec<Vec<String>> = s.chaos.cuts().into_iter().map(|c| c.members).collect();
    match view::topology(admin, &s.http, &view::TopoCtx { facts: &facts, cuts: &cuts, listeners: &listeners, publishers: &publishers }).await {
        Ok(t) => Json(t).into_response(),
        Err(e) => unreachable(e),
    }
}

/// Every node's latest connection fact, the backbone listeners and the backbone publishers, from
/// the span records the nodes write.
async fn evidence_signals(s: &AppState) -> (Vec<rafka_admin_ui::timeline::ConnFact>, Vec<String>, Vec<String>) {
    let Some(dir) = s.evidence_dir.clone() else { return (Vec::new(), Vec::new(), Vec::new()) };
    let evidence = s.evidence.clone();
    tokio::task::spawn_blocking(move || {
        let mut e = evidence.lock().unwrap();
        let _ = e.refresh(&dir);
        let (l, p) = e.backbone();
        (e.connection_facts(), l, p)
    })
    .await
    .unwrap_or_default()
}

async fn alerts(State(s): State<AppState>) -> Json<Value> {
    Json(json!({"alerts": s.alerts.lock().unwrap().newest()}))
}

/// The traffic driver's status (what it has issued and how each operation ended), or a named 503.
async fn traffic(State(s): State<AppState>) -> Response {
    let Some(file) = &s.traffic_file else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no-traffic-driver", "detail": "set RSHAPE_DEMO_STATE to the folder rshape-demo writes traffic.json into"}))).into_response();
    };
    match std::fs::read(file).map(|b| serde_json::from_slice::<Value>(&b)) {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "traffic-undecodable", "detail": e.to_string()}))).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "traffic-not-written", "detail": format!("{}: {e}", file.display())}))).into_response(),
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
    let span = tracing::info_span!("rdm.ui.chaos.fault.create.via-kit", node = %b.node, action = %b.action, outcome = tracing::field::Empty);
    let r = s.chaos.fault(&nodes, &b.node, &b.action).instrument(span.clone()).await;
    span.record("outcome", r.outcome);
    s.ui_event(&format!("ui.chaos.fault.{}", r.outcome), &b.node, format!("{} {}: {}", b.action, b.node, r.detail));
    s.alert(if r.outcome == "applied" { "warn" } else { "info" }, "chaos", format!("{} {}: {} {}", b.action, b.node, r.outcome, r.detail), Some(&b.node), b.node.split('.').next());
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
    let span = tracing::info_span!("rdm.ui.chaos.cut.create.via-kit", scope = %b.scope, target = %b.target, outcome = tracing::field::Empty);
    let r = s.chaos.cut(&nodes, &b.scope, &b.target).instrument(span.clone()).await;
    span.record("outcome", r.outcome);
    s.alert(if r.outcome == "applied" { "warn" } else { "info" }, "chaos", format!("{} {}: {} {}", r.action, b.target, r.outcome, r.detail), Some(&b.target), None);
    s.ui_event(&format!("ui.chaos.cut.{}", r.outcome), &b.target, format!("{} {}: {}", r.action, b.target, r.detail));
    answer(r)
}

#[derive(Deserialize)]
struct HealBody {
    id: u64,
}

async fn chaos_heal(State(s): State<AppState>, Json(b): Json<HealBody>) -> Response {
    let span = tracing::info_span!("rdm.ui.chaos.cut.delete.via-heal", cut = b.id, outcome = tracing::field::Empty);
    let r = s.chaos.heal(b.id).instrument(span.clone()).await;
    span.record("outcome", r.outcome);
    s.alert("info", "chaos", format!("heal cut {}: {} {}", b.id, r.outcome, r.detail), Some(&r.target), None);
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

/// The Jaeger service a node's spans are exported under: its binary's name. The consumer's role
/// binaries export as `rshape-<kind>`, RDM's own as `rafka-node-admin` / `rafka-rpc-node`; the one
/// Jaeger lists is the one asked.
async fn service_of(s: &AppState, node: &str) -> String {
    let seg = node.split('.').nth(1).unwrap_or_default();
    let candidates: Vec<String> = if seg == "admin" {
        vec!["rshape-node-admin".into(), "rafka-node-admin".into()]
    } else {
        vec![format!("rshape-{seg}"), "rafka-rpc-node".into()]
    };
    let listed: Vec<String> = match s.http.get(format!("{}/api/v3/services", s.jaeger_url)).send().await {
        Ok(r) => r.json::<Value>().await.ok().and_then(|v| v["services"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())).unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    candidates.iter().find(|c| listed.contains(c)).cloned().unwrap_or_else(|| candidates[0].clone())
}

/// Every span in a Jaeger v3 answer (a stream of `{"result": {"resourceSpans": [...]}}` objects),
/// grouped by trace id, as `(trace_id, [(name, start_us, duration_us)])`.
fn v3_traces(body: &str) -> Vec<(String, Vec<(String, u64, u64)>)> {
    let mut by_trace: Vec<(String, Vec<(String, u64, u64)>)> = Vec::new();
    for v in serde_json::Deserializer::from_str(body).into_iter::<Value>().flatten() {
        let rs = v["result"]["resourceSpans"].as_array().or_else(|| v["resourceSpans"].as_array());
        for r in rs.into_iter().flatten() {
            for sc in r["scopeSpans"].as_array().into_iter().flatten() {
                for sp in sc["spans"].as_array().into_iter().flatten() {
                    let n = |k: &str| sp[k].as_str().and_then(|x| x.parse::<u64>().ok()).or_else(|| sp[k].as_u64()).unwrap_or(0);
                    let (start, end) = (n("startTimeUnixNano"), n("endTimeUnixNano"));
                    let tid = sp["traceId"].as_str().unwrap_or_default().to_string();
                    let row = (sp["name"].as_str().unwrap_or_default().to_string(), start / 1000, end.saturating_sub(start) / 1000);
                    match by_trace.iter_mut().find(|(t, _)| *t == tid) {
                        Some((_, rows)) => rows.push(row),
                        None => by_trace.push((tid, vec![row])),
                    }
                }
            }
        }
    }
    by_trace
}

/// The node's boot trace, from Jaeger (its v3 query API): the newest trace holding the node's
/// ready span, as the spans the Boot Waterfall draws, with the link that opens it in Jaeger.
async fn boot_trace(State(s): State<AppState>, Query(q): Query<BootQuery>) -> Response {
    let service = service_of(&s, &q.service).await;
    let now = now_ms();
    let rfc = |ms: u64| {
        let secs = (ms / 1000) as i64;
        let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
        // civil-from-days (Howard Hinnant)
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + i64::from(m <= 2);
        format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
    };
    let url = format!("{}/api/v3/traces", s.jaeger_url);
    let params = [
        ("query.service_name".to_string(), service.clone()),
        ("query.operation_name".to_string(), "rdm.mesh.node.update.via-ready".to_string()),
        ("query.start_time_min".to_string(), rfc(now.saturating_sub(3 * 3_600_000))),
        ("query.start_time_max".to_string(), rfc(now + 300_000)),
        ("query.num_traces".to_string(), "1".to_string()),
        ("query.attributes[node]".to_string(), q.service.clone()),
    ];
    let qs = params.iter().map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))).collect::<Vec<_>>().join("&");
    let sent = s.http.get(format!("{url}?{qs}")).timeout(Duration::from_secs(20)).send().await;
    match sent {
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            if !status.is_success() {
                return (StatusCode::BAD_GATEWAY, Json(json!({"error": "jaeger-refused", "detail": format!("{url} answered {status}: {}", body.chars().take(300).collect::<String>())}))).into_response();
            }
            match v3_traces(&body).into_iter().next() {
                Some((trace_id, spans)) => {
                    let spans: Vec<Value> = spans.into_iter().map(|(name, start, dur)| json!({"operationName": name, "startTime": start, "duration": dur})).collect();
                    Json(json!({"data": [{"traceID": trace_id, "spans": spans}], "service": service, "jaeger_url": s.jaeger_url, "trace_url": format!("{}/trace/{trace_id}", s.jaeger_url)})).into_response()
                }
                None => (StatusCode::BAD_GATEWAY, Json(json!({"error": "no-boot-trace", "detail": format!("Jaeger at {} holds no boot trace for {} under service {service}", s.jaeger_url, q.service)}))).into_response(),
            }
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "jaeger-unreachable", "detail": format!("{url}: {e}")}))).into_response(),
    }
}

/// One span per request served, named by the route that matched.
async fn trace_requests(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let method = req.method().to_string();
    let route = req.extensions().get::<axum::extract::MatchedPath>().map(|m| m.as_str().to_string()).unwrap_or_else(|| req.uri().path().to_string());
    let span = tracing::info_span!("rdm.ui.http.request.serve", method = %method, route = %route, status = tracing::field::Empty, "otel.kind" = "server");
    let res = next.run(req).instrument(span.clone()).await;
    span.record("status", res.status().as_u16());
    res
}

/// Watches the fabric: holds every network cut over members born while it stands, and judges node
/// state and load for the Alerts tab.
fn watch(s: AppState) {
    let held = s.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            if held.chaos.cuts().is_empty() {
                continue;
            }
            let Some(admin) = &held.admin else { continue };
            // A view that cannot be read leaves the OS's own list of the cut mesh's processes to hold the cut.
            let facts = view::nodes(admin).await.map(|n| view::facts(&n)).unwrap_or_default();
            for (cut, members) in held.chaos.hold_cuts(&facts).await {
                held.ui_event("ui.chaos.cut.update.via-new-member", &members.join(","), format!("cut {cut} now also holds {}", members.join(", ")));
                held.alert("info", "chaos", format!("cut {cut} holds {} born while it stands", members.join(", ")), members.first().map(String::as_str), None);
            }
        }
    });
    tokio::spawn(async move {
        let thresholds = Thresholds::from_env();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let Some(admin) = &s.admin else { continue };
            let Ok(mut nodes) = view::nodes(admin).await else { continue };
            let load = view::loads(&nodes, &s.http).await;
            for n in nodes.iter_mut() {
                if let Some(l) = n["name"].as_str().and_then(|name| load.get(name)) {
                    n["load"] = l.clone();
                }
            }
            s.alerts.lock().unwrap().observe(now_ms(), &nodes, thresholds);
            // Members a node reports leaving its view (its own span record), once each.
            if let Some(dir) = s.evidence_dir.clone() {
                let evidence = s.evidence.clone();
                let stale = tokio::task::spawn_blocking(move || {
                    let mut e = evidence.lock().unwrap();
                    let _ = e.refresh(&dir);
                    e.take_stale()
                })
                .await
                .unwrap_or_default();
                for m in stale {
                    s.alert("warn", "node-state", format!("{} stopped hearing {} for {} ms (floor {} ms)", m.observer, m.member, m.silent_ms, m.staleness_ms), Some(&m.member), m.member.split('.').next());
                }
            }
        }
    });
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
        chaos: Arc::new(match std::env::var("RDM_ESTATE_ROOT").ok().filter(|d| !d.trim().is_empty()) {
            Some(root) => Chaos::with_estate_root(PathBuf::from(root)),
            None => Chaos::default(),
        }),
        evidence_dir: std::env::var("RDM_EVIDENCE_DIR").ok().filter(|d| !d.trim().is_empty()).map(PathBuf::from),
        evidence: Arc::new(Mutex::new(Evidence::default())),
        ui_events: Arc::new(Mutex::new(Vec::new())),
        submitted: Arc::new(Mutex::new(Vec::new())),
        alerts: Arc::new(Mutex::new(Alerts::default())),
        traffic_file: std::env::var("RSHAPE_DEMO_STATE").ok().filter(|d| !d.trim().is_empty()).map(|d| PathBuf::from(d).join("traffic.json")),
    };
    watch(state.clone());
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
        .route("/api/alerts", get(alerts))
        .route("/api/messages", get(messages))
        .route("/api/traffic", get(traffic))
        .route("/api/boot-trace", get(boot_trace))
        .with_state(state.clone())
        .merge(control::router(state.admin.clone(), Arc::new(Submitted(state))))
        .merge(tests::router())
        .route_layer(axum::middleware::from_fn(trace_requests))
        .fallback_service(ServeDir::new(&static_dir).append_index_html_on_directories(true));
    tracing::info!(%bind, %static_dir, "admin-ui listening");
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
