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
  #waterfall { border: 1px solid #30363d; border-radius: 6px; padding: 1rem; min-height: 200px; color: #8b949e; font-size: 0.85rem; display: flex; align-items: center; justify-content: center; }
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
  waterfall canvas placeholder — chunk 3
</div>

<script>
(function() {
  var dot = document.getElementById('status-dot');
  var txt = document.getElementById('status-text');
  var sel = document.getElementById('node-selector');

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
        if (prev && nodes.indexOf(prev) !== -1) sel.value = prev;
        setStatus(true, 'nodes loaded: ' + nodes.join(', '));
      })
      .catch(function() { setStatus(false, 'node list unavailable'); });
  }

  sel.addEventListener('change', function() {
    var svc = sel.value;
    if (!svc) return;
    setStatus(true, 'fetching boot trace for ' + svc + '…');
    fetch('/api/boot-trace?service=' + encodeURIComponent(svc))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) {
          setStatus(false, 'no boot trace: ' + svc);
          console.log('boot-trace error:', d);
        } else {
          var spans = (d.data && d.data[0] && d.data[0].spans) ? d.data[0].spans.length : 0;
          setStatus(true, svc + ': ' + spans + ' spans');
          console.log('boot-trace for', svc, d);
        }
      })
      .catch(function() { setStatus(false, 'boot-trace fetch failed'); });
  });

  document.getElementById('refresh').addEventListener('click', function() {
    pollHealth();
    loadNodes();
  });

  pollHealth();
  loadNodes();
  setInterval(pollHealth, 5000);
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
