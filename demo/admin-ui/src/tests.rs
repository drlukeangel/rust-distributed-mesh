//! The Tests tab's door: `/api/tests/*` is forwarded verbatim to the test runner
//! (`rdm-test-runner`, the RDM root workspace's `tools/test-runner`) named by `RDM_TEST_RUNNER_URL`.
//! The UI starts no process; the runner does the inventory, the runs and the evidence.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::{Json, Router};
use serde_json::json;

#[derive(Clone)]
struct Runner {
    http: reqwest::Client,
    base: Option<String>,
}

async fn forward(State(r): State<Runner>, req: Request) -> Response {
    let Some(base) = &r.base else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "no-test-runner", "detail": "set RDM_TEST_RUNNER_URL to the rdm-test-runner this UI lists and runs tests through (start it: target/debug/rdm-test-runner, default 127.0.0.1:19190)"})),
        )
            .into_response();
    };
    let (parts, body) = req.into_parts();
    let url = format!("{}{}", base.trim_end_matches('/'), parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));
    let bytes = match axum::body::to_bytes(body, 1 << 20).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({"error": "unreadable-body", "detail": e.to_string()}))).into_response(),
    };
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut rb = r.http.request(method, &url).timeout(std::time::Duration::from_secs(60));
    if let Some(ct) = parts.headers.get(header::CONTENT_TYPE) {
        rb = rb.header(header::CONTENT_TYPE, ct.as_bytes());
    }
    match rb.body(bytes).send().await {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let ct = resp.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(str::to_string);
            match resp.bytes().await {
                Ok(b) => {
                    let mut out = Response::new(Body::from(b));
                    *out.status_mut() = status;
                    if let Some(ct) = ct.and_then(|c| c.parse().ok()) {
                        out.headers_mut().insert(header::CONTENT_TYPE, ct);
                    }
                    out
                }
                Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "test-runner-body", "detail": format!("{url}: {e}")}))).into_response(),
            }
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "test-runner-unreachable", "detail": format!("{url}: {e}")}))).into_response(),
    }
}

pub fn router() -> Router {
    let state = Runner {
        http: reqwest::Client::new(),
        base: std::env::var("RDM_TEST_RUNNER_URL").ok().filter(|b| !b.trim().is_empty()),
    };
    Router::new().route("/api/tests/{*rest}", any(forward)).with_state(state)
}
