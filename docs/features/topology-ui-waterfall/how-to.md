# topology-ui-waterfall — how-to

## Launch + use

```bash
# in one terminal:
CARGO_TARGET_DIR=E:/cargo-target-v2 cargo run -p rafka-topology-ui

# in your browser:
http://localhost:19090
```

Pick a node from the dropdown. See the 5-span waterfall render below.

## Run all 4 mesh nodes + topology-ui together

```bash
RAFKA_DATA_DIR=./data/g cargo run -p rafka-gateway &
RAFKA_DATA_DIR=./data/b cargo run -p rafka-broker &
RAFKA_DATA_DIR=./data/c cargo run -p rafka-compute &
RAFKA_DATA_DIR=./data/r cargo run -p rafka-registry &
cargo run -p rafka-topology-ui &
# wait 5s — UI dropdown auto-populates with [broker, compute, gateway, registry]
```

## API endpoints

```bash
curl -s http://localhost:19090/api/health
# → {"status":"ok"}

curl -s http://localhost:19090/api/nodes
# → {"nodes":["broker","compute","gateway","registry"]}

curl -s "http://localhost:19090/api/boot-trace?service=gateway" | python -m json.tool | head -30
# → full Jaeger trace JSON

curl -s "http://localhost:19090/api/heartbeat?service=gateway"
# → {"node_id":"...","peer_count":3,"last_heartbeat_us":<microseconds>}
```

## See UI's own telemetry

```
http://localhost:16686/search?service=topology-ui&lookback=15m
```

Every browser request fires `rafka.ui.http.request`; every Jaeger query fires `rafka.ui.jaeger.query`.
