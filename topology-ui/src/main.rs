use anyhow::Result;
use axum::{
    Router,
    extract::{Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
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
  #controls { display: flex; gap: 0.75rem; margin-bottom: 1.5rem; align-items: center; }
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

<div id="controls">
  <select id="node-selector">
    <option value="">select a node</option>
  </select>
  <button id="refresh">Refresh</button>
</div>

<div id="waterfall">
  <div class="wf-empty">select a node to view its boot waterfall</div>
</div>

<script>
(function() {
  var dot = document.getElementById('status-dot');
  var txt = document.getElementById('status-text');
  var sel = document.getElementById('node-selector');
  var wf  = document.getElementById('waterfall');

  // Color map by operation name prefix (D-019 phase buckets)
  var COLORS = {
    'rafka.mesh.node.ready':              '#1f6feb',  // dark blue — root
    'rafka.mesh.boot.identity_':          '#3fb950',  // green
    'rafka.mesh.boot.endpoint_created':   '#e3b341',  // orange/amber
    'rafka.mesh.boot.alpn_registered':    '#8957e5',  // purple
    'rafka.mesh.boot.gossip_started':     '#39c5cf',  // teal
    'rafka.mesh.boot.accept_loop_started':'#f85149',  // red
  };

  function spanColor(opName) {
    for (var prefix in COLORS) {
      if (opName === prefix || opName.indexOf(prefix) === 0) return COLORS[prefix];
    }
    return '#484f58';  // fallback gray for any other rafka span
  }

  function setStatus(ok, msg) {
    dot.className = ok ? '' : 'error';
    txt.textContent = msg;
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
        if (prev && nodes.indexOf(prev) !== -1) {
          sel.value = prev;
        }
        setStatus(true, 'nodes: ' + nodes.join(', '));
      })
      .catch(function() { setStatus(false, 'node list unavailable'); });
  }

  function renderWaterfall(svc, traceData) {
    var spans = traceData.spans || [];
    // Filter to rafka.* spans only
    var rafkaSpans = spans.filter(function(s) {
      return s.operationName && s.operationName.indexOf('rafka.') === 0;
    });

    if (rafkaSpans.length === 0) {
      wf.innerHTML = '<div class="wf-error">no rafka spans found in boot trace for ' + svc + '</div>';
      return;
    }

    // Sort by startTime ascending
    rafkaSpans.sort(function(a, b) { return a.startTime - b.startTime; });

    var rootTime = rafkaSpans[0].startTime;
    var endTimes = rafkaSpans.map(function(s) { return s.startTime + s.duration; });
    var maxEnd = Math.max.apply(null, endTimes);
    var totalUs = maxEnd - rootTime;
    if (totalUs <= 0) totalUs = 1;

    // Header: service + ISO timestamp of root span
    var rootDate = new Date(rootTime / 1000);  // us → ms
    var headerText = svc + ' boot @ ' + rootDate.toISOString();

    var html = '<div id="waterfall-header">' + headerText + '</div>';

    rafkaSpans.forEach(function(sp) {
      var name = sp.operationName;
      var shortName = name.replace('rafka.mesh.', '');
      var offsetUs = sp.startTime - rootTime;
      var leftPct  = (offsetUs / totalUs * 100).toFixed(2);
      var widthPct = (sp.duration / totalUs * 100).toFixed(2);
      var durationMs = (sp.duration / 1000).toFixed(2);
      var color = spanColor(name);

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
          console.log('boot-trace error:', d);
        } else {
          var trace = d.data && d.data[0];
          if (trace) {
            renderWaterfall(svc, trace);
            console.log('boot-trace for', svc, d);
          } else {
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

  sel.addEventListener('change', function() { loadTrace(sel.value); });

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
}

#[derive(Deserialize)]
struct BootTraceQuery {
    service: String,
}

async fn handle_root() -> Html<&'static str> {
    Html(HTML)
}

async fn handle_health() -> impl IntoResponse {
    axum::Json(json!({"status": "ok"}))
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

async fn trace_middleware(req: Request, next: Next) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let span = info_span!(
        "rafka.ui.http.request",
        method = %method,
        path = %path,
        "otel.kind" = "server",
    );
    next.run(req).instrument(span).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry("topology-ui");

    let bind_addr = std::env::var("RAFKA_TOPOLOGY_UI_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:19090".to_string());

    let jaeger_url = std::env::var("JAEGER_QUERY_URL")
        .unwrap_or_else(|_| "http://localhost:16686".to_string());

    let addr: SocketAddr = bind_addr.parse()?;

    let state = AppState {
        http: reqwest::Client::new(),
        jaeger_url,
    };

    let app = Router::new()
        .route("/", get(handle_root))
        .route("/api/health", get(handle_health))
        .route("/api/nodes", get(handle_nodes))
        .route("/api/boot-trace", get(handle_boot_trace))
        .with_state(state)
        .layer(middleware::from_fn(trace_middleware));

    info!("topology-ui listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
