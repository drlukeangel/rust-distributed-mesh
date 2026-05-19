use anyhow::Result;
use axum::{
    Router,
    extract::{Json, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
};
use dashmap::DashMap;
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{process::Child, sync::Mutex};
use tracing::{info, info_span, Instrument};

const KNOWN_NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry"];

const HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>rafka mesh — boot waterfall</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #0d1117; color: #c9d1d9; padding: 1.5rem; }
  header { border-bottom: 1px solid #30363d; padding-bottom: 1rem; margin-bottom: 1.5rem; }
  h1 { font-size: 1.2rem; font-weight: bold; color: #58a6ff; }
  #status-bar { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1.5rem; font-size: 0.85rem; }
  #status-dot { width: 10px; height: 10px; border-radius: 50%; background: #3fb950; }
  #status-dot.error { background: #f85149; }
  select, button { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; padding: 0.4rem 0.75rem; font-family: monospace; font-size: 0.85rem; cursor: pointer; border-radius: 4px; }
  button:hover { background: #21262d; }
  #spawn-row { display: flex; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap; }
  .spawn-btn { border-color: #3fb950; color: #3fb950; }
  .spawn-btn:hover { background: #0d2a14; }
  .spawn-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  #controls { display: flex; gap: 0.75rem; margin-bottom: 1.5rem; align-items: center; }
  #toast { font-size: 0.8rem; margin-bottom: 1rem; min-height: 1.2em; color: #3fb950; }
  #toast.error { color: #f85149; }
  #waterfall {
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 1rem;
    min-height: 200px;
  }
  #waterfall-header {
    font-size: 0.8rem;
    color: #8b949e;
    margin-bottom: 0.75rem;
  }
  .wf-row {
    display: flex;
    align-items: center;
    margin-bottom: 0.4rem;
    gap: 0.5rem;
  }
  .wf-label {
    width: 260px;
    min-width: 260px;
    font-size: 0.75rem;
    color: #8b949e;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .wf-track {
    flex: 1;
    position: relative;
    height: 20px;
    background: #161b22;
    border-radius: 3px;
  }
  .wf-bar {
    position: absolute;
    top: 0;
    height: 100%;
    border-radius: 3px;
    min-width: 2px;
    display: flex;
    align-items: center;
    padding: 0 4px;
  }
  .wf-bar-label {
    font-size: 0.65rem;
    color: rgba(255,255,255,0.85);
    white-space: nowrap;
    overflow: hidden;
  }
  .wf-empty {
    color: #8b949e;
    font-size: 0.85rem;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 160px;
  }
  .wf-error {
    color: #f85149;
    font-size: 0.85rem;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 160px;
    text-align: center;
    padding: 1rem;
  }
</style>
</head>
<body>
<header>
  <h1>rafka mesh — boot waterfall</h1>
</header>

<div id="status-bar">
  <div id="status-dot"></div>
  <span id="status-text">connecting…</span>
</div>

<div id="spawn-row">
  <button class="spawn-btn" data-type="gateway">+ Spawn gateway</button>
  <button class="spawn-btn" data-type="broker">+ Spawn broker</button>
  <button class="spawn-btn" data-type="compute">+ Spawn compute</button>
  <button class="spawn-btn" data-type="registry">+ Spawn registry</button>
</div>

<div id="toast"></div>

<div id="controls">
  <select id="node-selector">
    <option value="">select a node</option>
  </select>
  <button id="refresh">Refresh</button>
  <button id="kill-btn" disabled style="border-color:#f85149;color:#f85149;display:none">Kill selected</button>
</div>

<div id="waterfall">
  <div class="wf-empty">select a node to view its boot waterfall</div>
</div>

<script>
(function() {
  var dot      = document.getElementById('status-dot');
  var txt      = document.getElementById('status-text');
  var sel      = document.getElementById('node-selector');
  var wf       = document.getElementById('waterfall');
  var toast    = document.getElementById('toast');
  var killBtn  = document.getElementById('kill-btn');

  // node_name → node_type for subprocesses spawned by this UI session
  var uiSpawned = {};

  var COLORS = {
    'rafka.mesh.node.ready':              '#1f6feb',
    'rafka.mesh.boot.identity_':          '#3fb950',
    'rafka.mesh.boot.endpoint_created':   '#e3b341',
    'rafka.mesh.boot.alpn_registered':    '#8957e5',
    'rafka.mesh.boot.gossip_started':     '#39c5cf',
    'rafka.mesh.boot.accept_loop_started':'#f85149',
  };

  function spanColor(opName) {
    for (var prefix in COLORS) {
      if (opName === prefix || opName.indexOf(prefix) === 0) return COLORS[prefix];
    }
    return '#484f58';
  }

  function setStatus(ok, msg) {
    dot.className = ok ? '' : 'error';
    txt.textContent = msg;
  }

  function showToast(ok, msg) {
    toast.className = ok ? '' : 'error';
    toast.textContent = msg;
    setTimeout(function() { if (toast.textContent === msg) toast.textContent = ''; }, 8000);
  }

  function pollHealth() {
    fetch('/api/health')
      .then(function(r) { return r.json(); })
      .then(function(d) { setStatus(true, 'api: ' + d.status); })
      .catch(function() { setStatus(false, 'api unreachable'); });
  }

  function loadNodes() {
    fetch('/api/nodes')
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var nodes = d.nodes || [];
        var prev = sel.value;
        while (sel.options.length > 1) sel.remove(1);
        nodes.forEach(function(n) {
          var opt = document.createElement('option');
          opt.value = n;
          opt.textContent = n;
          sel.appendChild(opt);
        });
        if (prev && nodes.indexOf(prev) !== -1) sel.value = prev;
        setStatus(true, 'nodes: ' + (nodes.length ? nodes.join(', ') : '(none in jaeger yet)'));
      })
      .catch(function() { setStatus(false, 'node list unavailable'); });
  }

  function renderWaterfall(svc, traceData) {
    var spans = traceData.spans || [];
    var rafkaSpans = spans.filter(function(s) {
      return s.operationName && s.operationName.indexOf('rafka.') === 0;
    });

    if (rafkaSpans.length === 0) {
      wf.innerHTML = '<div class="wf-error">no rafka spans found in boot trace for ' + svc + '</div>';
      return;
    }

    rafkaSpans.sort(function(a, b) { return a.startTime - b.startTime; });

    var rootTime = rafkaSpans[0].startTime;
    var endTimes = rafkaSpans.map(function(s) { return s.startTime + s.duration; });
    var maxEnd   = Math.max.apply(null, endTimes);
    var totalUs  = maxEnd - rootTime;
    if (totalUs <= 0) totalUs = 1;

    var rootDate   = new Date(rootTime / 1000);
    var headerText = svc + ' boot @ ' + rootDate.toISOString();
    var html       = '<div id="waterfall-header">' + headerText + '</div>';

    rafkaSpans.forEach(function(sp) {
      var name       = sp.operationName;
      var shortName  = name.replace('rafka.mesh.', '');
      var offsetUs   = sp.startTime - rootTime;
      var leftPct    = (offsetUs / totalUs * 100).toFixed(2);
      var widthPct   = (sp.duration / totalUs * 100).toFixed(2);
      var durationMs = (sp.duration / 1000).toFixed(2);
      var color      = spanColor(name);

      html += '<div class="wf-row">' +
        '<div class="wf-label" title="' + name + '">' + shortName + '</div>' +
        '<div class="wf-track">' +
          '<div class="wf-bar" style="left:' + leftPct + '%;width:max(' + widthPct + '%,2px);background:' + color + '" title="' + name + ' — ' + durationMs + 'ms">' +
            '<span class="wf-bar-label">' + durationMs + 'ms</span>' +
          '</div>' +
        '</div>' +
        '</div>';
    });

    wf.innerHTML = html;
    setStatus(true, svc + ': ' + rafkaSpans.length + ' rafka spans, total ' + (totalUs / 1000).toFixed(2) + 'ms');
  }

  function loadTrace(svc) {
    if (!svc) return;
    setStatus(true, 'fetching boot trace for ' + svc + '…');
    wf.innerHTML = '<div class="wf-empty">loading…</div>';
    fetch('/api/boot-trace?service=' + encodeURIComponent(svc))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) {
          wf.innerHTML = '<div class="wf-error">no boot trace found for <strong>' + svc + '</strong><br>has it run recently? (traces age out after ~10 min)</div>';
          setStatus(false, 'no boot trace: ' + svc);
        } else {
          var trace = d.data && d.data[0];
          if (trace) renderWaterfall(svc, trace);
          else {
            wf.innerHTML = '<div class="wf-error">empty trace data for ' + svc + '</div>';
            setStatus(false, 'empty trace: ' + svc);
          }
        }
      })
      .catch(function() {
        wf.innerHTML = '<div class="wf-error">boot-trace fetch failed</div>';
        setStatus(false, 'boot-trace fetch failed');
      });
  }

  function updateKillBtn() {
    var svc = sel.value;
    if (svc && uiSpawned[svc]) {
      killBtn.style.display = '';
      killBtn.disabled = false;
      killBtn.textContent = 'Kill ' + svc;
    } else {
      killBtn.style.display = 'none';
      killBtn.disabled = true;
    }
  }

  function spawnNode(nodeType, btn) {
    btn.disabled = true;
    fetch('/api/nodes/spawn', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({node_type: nodeType})
    })
      .then(function(r) { return r.json().then(function(d) { return {ok: r.ok, d: d}; }); })
      .then(function(res) {
        btn.disabled = false;
        if (res.ok) {
          uiSpawned[res.d.node_name] = nodeType;
          showToast(true, 'Spawned ' + res.d.node_name + ' (pid=' + res.d.pid + ')');
          setTimeout(loadNodes, 5000);
        } else {
          showToast(false, 'Spawn failed: ' + (res.d.error || 'unknown error'));
        }
      })
      .catch(function() {
        btn.disabled = false;
        showToast(false, 'Spawn request failed');
      });
  }

  function killSelected() {
    var svc = sel.value;
    if (!svc || !uiSpawned[svc]) return;
    killBtn.disabled = true;
    fetch('/api/nodes/' + encodeURIComponent(svc), { method: 'DELETE' })
      .then(function(r) { return r.json().then(function(d) { return {ok: r.ok, d: d}; }); })
      .then(function(res) {
        if (res.ok) {
          delete uiSpawned[res.d.node_name];
          showToast(true, 'Killed ' + res.d.node_name + ' (' + res.d.reason + ')');
          loadNodes();
          updateKillBtn();
          wf.innerHTML = '<div class="wf-empty">select a node to view its boot waterfall</div>';
        } else {
          showToast(false, 'Kill failed: ' + (res.d.error || 'unknown error'));
          killBtn.disabled = false;
        }
      })
      .catch(function() {
        showToast(false, 'Kill request failed');
        killBtn.disabled = false;
      });
  }

  killBtn.addEventListener('click', killSelected);

  document.querySelectorAll('.spawn-btn').forEach(function(btn) {
    btn.addEventListener('click', function() { spawnNode(btn.dataset.type, btn); });
  });

  sel.addEventListener('change', function() { loadTrace(sel.value); updateKillBtn(); });

  document.getElementById('refresh').addEventListener('click', function() {
    pollHealth();
    loadNodes();
    if (sel.value) loadTrace(sel.value);
  });

  pollHealth();
  loadNodes();
  setInterval(pollHealth, 5000);
  setInterval(loadNodes, 30000);
})();
</script>
</body>
</html>"#;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    jaeger_url: String,
    cargo_target_dir: String,
    processes: Arc<DashMap<String, Mutex<Child>>>,
}

#[derive(Deserialize)]
struct BootTraceQuery {
    service: String,
}

#[derive(Deserialize)]
struct SpawnRequest {
    node_type: String,
}

async fn handle_root() -> Html<&'static str> {
    Html(HTML)
}

async fn handle_health() -> impl IntoResponse {
    axum::Json(json!({"status": "ok"}))
}

async fn handle_spawned_list(State(state): State<AppState>) -> impl IntoResponse {
    let names: Vec<String> = state.processes.iter().map(|e| e.key().clone()).collect();
    let span = info_span!(
        "rafka.ui.spawned_list",
        count = names.len() as i64,
        "otel.kind" = "internal",
    );
    span.in_scope(|| info!(count = names.len(), "spawned subprocesses listed"));
    (StatusCode::OK, axum::Json(json!({"spawned": names}))).into_response()
}

async fn handle_nodes(State(state): State<AppState>) -> impl IntoResponse {
    let url = format!("{}/api/services", state.jaeger_url);
    let span = info_span!("rafka.ui.jaeger.query", endpoint = "/api/services", "otel.kind" = "client");
    let result = state.http.get(&url).send().instrument(span).await;

    match result {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(body) => {
                let nodes: Vec<&str> = body["data"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .filter(|s| KNOWN_NODE_TYPES.contains(s))
                            .collect()
                    })
                    .unwrap_or_default();
                (StatusCode::OK, axum::Json(json!({"nodes": nodes}))).into_response()
            }
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "invalid response from jaeger"})),
            )
                .into_response(),
        },
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({"error": "jaeger unreachable"})),
        )
            .into_response(),
    }
}

async fn handle_boot_trace(
    State(state): State<AppState>,
    Query(params): Query<BootTraceQuery>,
) -> impl IntoResponse {
    let svc = &params.service;
    let url = format!(
        "{}/api/traces?service={}&operation=rafka.mesh.node.ready&limit=1&lookback=10m",
        state.jaeger_url, svc
    );
    let span = info_span!(
        "rafka.ui.jaeger.query",
        endpoint = "/api/traces",
        service = %svc,
        "otel.kind" = "client",
    );
    let result = state.http.get(&url).send().instrument(span).await;

    match result {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(body) => {
                let traces = body["data"].as_array();
                match traces.and_then(|arr| arr.first()) {
                    Some(first) => (StatusCode::OK, axum::Json(json!({"data": [first]}))).into_response(),
                    None => (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": format!("no boot trace found for service {svc}")})),
                    )
                        .into_response(),
                }
            }
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "invalid response from jaeger"})),
            )
                .into_response(),
        },
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({"error": "jaeger unreachable"})),
        )
            .into_response(),
    }
}

async fn handle_heartbeat(
    State(state): State<AppState>,
    Query(params): Query<BootTraceQuery>,
) -> impl IntoResponse {
    let svc = &params.service;
    let url = format!(
        "{}/api/traces?service={}&operation=rafka.mesh.heartbeat&limit=1&lookback=10m",
        state.jaeger_url, svc
    );
    let span = info_span!(
        "rafka.ui.jaeger.query",
        endpoint = "/api/traces/heartbeat",
        service = %svc,
        "otel.kind" = "client",
    );
    let result = state.http.get(&url).send().instrument(span).await;

    match result {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(body) => {
                let first_span = body["data"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|t| t["spans"].as_array())
                    .and_then(|a| a.first())
                    .cloned();
                match first_span {
                    Some(sp) => {
                        let tags: std::collections::HashMap<String, Value> = sp["tags"]
                            .as_array()
                            .unwrap_or(&vec![])
                            .iter()
                            .filter_map(|t| Some((t["key"].as_str()?.to_string(), t["value"].clone())))
                            .collect();
                        let node_id = tags.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let peer_count = tags.get("peer_count").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
                        let last_heartbeat_us = sp["startTime"].as_i64().unwrap_or(0);
                        (StatusCode::OK, axum::Json(json!({
                            "node_id": node_id,
                            "peer_count": peer_count,
                            "last_heartbeat_us": last_heartbeat_us,
                        }))).into_response()
                    }
                    None => (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": format!("no heartbeat trace found for {svc}")})),
                    ).into_response(),
                }
            }
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "invalid response from jaeger"})),
            ).into_response(),
        },
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            axum::Json(json!({"error": "jaeger unreachable"})),
        ).into_response(),
    }
}

async fn handle_spawn(
    State(state): State<AppState>,
    Json(body): Json<SpawnRequest>,
) -> impl IntoResponse {
    let node_type = body.node_type.as_str();
    if !KNOWN_NODE_TYPES.contains(&node_type) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": format!("unknown node_type: {node_type}")})),
        )
            .into_response();
    }

    let suffix: String = {
        let mut rng = rand::thread_rng();
        (0..8).map(|_| format!("{:x}", rng.gen::<u8>() & 0xf)).collect()
    };
    let node_name = format!("{}-{}", node_type, suffix);

    let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", node_name);
    if let Err(e) = std::fs::create_dir_all(&spawn_dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("failed to create spawn dir: {e}")})),
        )
            .into_response();
    }

    let binary = format!(
        "{}/debug/rafka-{}.exe",
        state.cargo_target_dir, node_type
    );

    let otlp = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4316".to_string());
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    let mut cmd = tokio::process::Command::new(&binary);
    cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", &otlp)
        .env("OTEL_SERVICE_NAME", node_type)
        .env("RAFKA_DATA_DIR", &spawn_dir)
        .env("RUST_LOG", &rust_log);

    let node_name_c = node_name.clone();
    let node_type_c = node_type.to_string();

    match cmd.spawn() {
        Ok(child) => {
            let pid = child.id().unwrap_or(0);
            state.processes.insert(node_name.clone(), Mutex::new(child));

            let span = info_span!(
                "rafka.ui.subprocess.spawned",
                node_name = %node_name_c,
                node_type = %node_type_c,
                pid = pid,
                "otel.kind" = "internal",
            );
            span.in_scope(|| {
                info!(node_name = %node_name_c, node_type = %node_type_c, pid, "subprocess spawned");
            });

            (
                StatusCode::CREATED,
                axum::Json(json!({"node_name": node_name, "pid": pid})),
            )
                .into_response()
        }
        Err(e) => {
            let span = info_span!(
                "rafka.ui.subprocess.spawn_failed",
                node_name = %node_name_c,
                node_type = %node_type_c,
                error = %e,
                "otel.kind" = "internal",
            );
            span.in_scope(|| {
                tracing::error!(error = %e, binary = %binary, "subprocess spawn failed");
            });

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("spawn failed: {e}")})),
            )
                .into_response()
        }
    }
}

async fn handle_kill(
    State(state): State<AppState>,
    Path(node_name): Path<String>,
) -> impl IntoResponse {
    let entry = state.processes.remove(&node_name);
    let (_, mutex_child) = match entry {
        Some(e) => e,
        None => {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": format!("no subprocess named {node_name}")})),
            )
                .into_response();
        }
    };

    let mut child = mutex_child.into_inner();
    let pid = child.id().unwrap_or(0);

    // Phase 1: start_kill (Windows terminate-process; SIGTERM-equivalent on Unix)
    let _ = child.start_kill();

    let reason = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(_) => "graceful",
        Err(_) => {
            // Timeout — force kill and wait
            let _ = child.kill().await;
            let _ = child.wait().await;
            "forced"
        }
    };

    // Best-effort data dir cleanup — don't fail response if this errors
    let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", node_name);
    if let Err(e) = tokio::fs::remove_dir_all(&spawn_dir).await {
        tracing::warn!(dir = %spawn_dir, error = %e, "failed to remove subprocess data dir");
    }

    let span = info_span!(
        "rafka.ui.subprocess.killed",
        node_name = %node_name,
        pid = pid,
        reason = reason,
        "otel.kind" = "internal",
    );
    span.in_scope(|| {
        info!(node_name = %node_name, pid, reason, "subprocess killed");
    });

    (StatusCode::OK, axum::Json(json!({"node_name": node_name, "reason": reason}))).into_response()
}

async fn trace_middleware(req: Request, next: Next) -> Response {
    use opentelemetry::global;
    use opentelemetry_http::HeaderExtractor;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let method = req.method().to_string();
    let path = req.uri().path().to_string();

    // Extract incoming W3C traceparent so the rafka.ui.http.request span chains
    // under the caller's trace (e.g. rfa CLI invocation). When no traceparent
    // header is present, set_parent on a default context is a no-op and the span
    // becomes its own root — matches in-browser-fetch behaviour.
    let parent_ctx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(req.headers()))
    });

    let span = info_span!(
        "rafka.ui.http.request",
        method = %method,
        path = %path,
        "otel.kind" = "server",
    );
    span.set_parent(parent_ctx);

    next.run(req).instrument(span).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry("topology-ui");

    let bind_addr = std::env::var("RAFKA_TOPOLOGY_UI_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:19090".to_string());

    let jaeger_url = std::env::var("JAEGER_QUERY_URL")
        .unwrap_or_else(|_| "http://localhost:16686".to_string());

    let cargo_target_dir = std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_| "./target".to_string());

    let addr: SocketAddr = bind_addr.parse()?;

    let state = AppState {
        http: reqwest::Client::new(),
        jaeger_url,
        cargo_target_dir,
        processes: Arc::new(DashMap::new()),
    };

    let app = Router::new()
        .route("/", get(handle_root))
        .route("/api/health", get(handle_health))
        .route("/api/nodes", get(handle_nodes))
        .route("/api/boot-trace", get(handle_boot_trace))
        .route("/api/heartbeat", get(handle_heartbeat))
        .route("/api/nodes/spawn", post(handle_spawn))
        .route("/api/nodes/spawned", get(handle_spawned_list))
        .route("/api/nodes/{node_name}", delete(handle_kill))
        .with_state(state)
        .layer(middleware::from_fn(trace_middleware));

    info!("topology-ui listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
