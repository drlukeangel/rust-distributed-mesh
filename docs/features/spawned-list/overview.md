# spawned-list — overview

> **Source:** Operator visibility shim. topology-ui exposes its in-memory subprocess registry so operators (and chaos primitives) can enumerate UI-spawned nodes by name.

## What it is

`GET /api/nodes/spawned` returns the live keys of topology-ui's `Arc<DashMap<String, Mutex<Child>>>` as a JSON array of node_names.

Returns ONLY UI-spawned subprocesses — baseline nodes launched directly (e.g. via `cargo run -p rafka-gateway`) are not in the DashMap and don't appear here. This separation is intentional: UI can only kill what it spawned.

## How it works

`topology-ui/src/main.rs::handle_spawned_list`:

```rust
async fn handle_spawned_list(State(state): State<AppState>) -> impl IntoResponse {
    let names: Vec<String> = state.processes.iter().map(|e| e.key().clone()).collect();
    let span = info_span!("rafka.ui.spawned_list", count = names.len() as i64, "otel.kind" = "internal");
    span.in_scope(|| info!(count = names.len(), "spawned subprocesses listed"));
    (StatusCode::OK, axum::Json(json!({"spawned": names}))).into_response()
}
```

Used by:
- Chaos primitives' `pick_random_spawned()` — picks a target without operator input.
- Operators wanting to know which spawned subprocesses are still alive (e.g., before chaos kill: "what can I target?").

## Locked spans

- `rafka.ui.spawned_list{count, otel.kind="internal"}` — one span per query

## Invariants

1. **Only UI-spawned subprocesses listed.** Baseline nodes are not visible; for them, use Jaeger `/api/services` (proxied via topology-ui's `/api/nodes`).
2. **Names are the spawn-time-generated `<type>-<8hex>`.** Stable for the subprocess lifetime.
3. **Listing is read-only — no spawn or kill side-effects.**

## Cross-references

* Sibling: [`subprocess-control`](../subprocess-control/overview.md) (spawn/kill backend), [`chaos-harness`](../chaos-harness/overview.md) (consumer for random targeting).
* Code: `topology-ui/src/main.rs::handle_spawned_list`.
