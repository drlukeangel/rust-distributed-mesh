# cross-service-tracing — how-to

## Fire a cross-service trace via the CLI

```bash
rfa mesh node add --type broker
# fresh spawn → rfa.cli.command + rfa.cli.http.request + ui.http.request + ui.subprocess.spawned, all under one trace_id
```

## Inspect the unified trace

```bash
TID=$(curl -s "http://localhost:16686/api/traces?service=rfa&operation=rafka.cli.command&limit=1&lookback=1m" | python -c "import sys,json; print(json.load(sys.stdin)['data'][0]['traceID'])")
echo "trace: http://localhost:16686/trace/$TID"

# count spans + services in the trace
curl -s "http://localhost:16686/api/traces/$TID" | python -c "import sys,json; d=json.load(sys.stdin); proc=d['data'][0]['processes']; spans=d['data'][0]['spans']; svcs=sorted(set(proc.get(s.get('processID'),{}).get('serviceName','?') for s in spans)); print(f'{len(spans)} spans across {svcs}')"
# expect: 4 spans across ['rfa', 'topology-ui']
```

## Add traceparent injection to a new HTTP client

```rust
async fn my_http_call(client: &reqwest::Client, url: &str) -> Result<...> {
    let span = info_span!("rafka.foo.http.request", method = "GET", path = %url, "otel.kind" = "client");
    let resp = async {
        let headers = current_traceparent_headers();  // captures current span's context
        client.get(url).headers(headers).send().await
    }
    .instrument(span)  // span entered before headers fn runs
    .await?;
    ...
}
```

## Add traceparent extraction to a new HTTP server

```rust
async fn my_middleware(req: Request, next: Next) -> Response {
    use opentelemetry::global;
    use opentelemetry_http::HeaderExtractor;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let parent_ctx = global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(req.headers())));
    let span = info_span!("rafka.foo.http.request", "otel.kind" = "server");
    span.set_parent(parent_ctx);
    next.run(req).instrument(span).await
}
```
