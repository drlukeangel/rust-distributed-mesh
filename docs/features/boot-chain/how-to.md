# boot-chain — how-to

## Watch a node boot

```bash
# Direct binary launch:
RAFKA_DATA_DIR=./data/g1 cargo run -p rafka-gateway

# Or via topology-ui (auto-tracked):
rfa mesh node add --type gateway
```

## View the boot chain in Jaeger

Open the search page filtered by node type and operation:

```
http://localhost:16686/search?service=gateway&operation=rafka.mesh.node.ready&lookback=15m
```

Pick the most recent trace. The trace view shows all 6 spans as a horizontal waterfall.

## View in topology-ui's boot waterfall

```
http://localhost:19090
```

Pick a node from the dropdown. The right panel renders the same 6 spans color-coded by phase.

## Pull the trace JSON for assertion

```bash
TID=$(curl -s "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.node.ready&limit=1&lookback=5m" | python -c "import sys,json; print(json.load(sys.stdin)['data'][0]['traceID'])")
curl -s "http://localhost:16686/api/traces/$TID" | python -c "import sys,json; d=json.load(sys.stdin); print(sorted(set(s['operationName'] for s in d['data'][0]['spans'] if s['operationName'].startswith('rafka.'))))"
```

Expected output: list of all 5–6 `rafka.mesh.*` ops (5 if identity_loaded/_minted shows on its own trace per D-025 restructure).

## CLI shortcut

```bash
rfa mesh node describe <type>
# → node_id + boot span timings as a table
```
