use anyhow::Result;
use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde_json::json;
use std::net::SocketAddr;
use tracing::{info, info_span, Instrument};

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

  function poll() {
    fetch('/api/health')
      .then(function(r) { return r.json(); })
      .then(function(d) {
        dot.className = '';
        txt.textContent = 'api: ' + d.status;
      })
      .catch(function() {
        dot.className = 'error';
        txt.textContent = 'api unreachable';
      });
  }

  poll();
  setInterval(poll, 5000);

  document.getElementById('refresh').addEventListener('click', poll);
})();
</script>
</body>
</html>"#;

async fn handle_root() -> Html<&'static str> {
    Html(HTML)
}

async fn handle_health() -> impl IntoResponse {
    axum::Json(json!({"status": "ok"}))
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

    let _jaeger_url = std::env::var("JAEGER_QUERY_URL")
        .unwrap_or_else(|_| "http://localhost:16686".to_string());

    let addr: SocketAddr = bind_addr.parse()?;

    let app = Router::new()
        .route("/", get(handle_root))
        .route("/api/health", get(handle_health))
        .layer(middleware::from_fn(trace_middleware));

    info!("topology-ui listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
