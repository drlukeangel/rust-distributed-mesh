# telemetry-substrate — how-to

## Use from a long-running service

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry("gateway");
    // ... your service ...
    // _guard drops at scope exit; flushes spans
    Ok(())
}
```

## Use from a CLI

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry_for_cli("rfa");
    let result = run_command().await;
    result
}
```

No pre-exit sleep needed — SimpleSpanProcessor exports synchronously.

## Point at a different collector

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://other-host:4317 cargo run -p rafka-gateway
```

## Override service name (e.g., for multi-instance staging)

```bash
OTEL_SERVICE_NAME=gateway-staging1 cargo run -p rafka-gateway
```

## Confirm pipeline is up

```bash
curl -s http://localhost:16686/api/services | python -m json.tool
# → should include "gateway", "broker", "compute", "registry", "topology-ui", "rfa" after their runs
```
