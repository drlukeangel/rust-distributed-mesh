# chaos-timeline — how-to

## See the live timeline

Open the topology-ui (default `http://localhost:19090`, currently running on `19091` or `19092` due to a Windows kernel zombie holding 19090 — check process output for the actual port). Click the **Timeline** tab. It auto-refreshes every 5 seconds.

## Trigger a chaos event manually

```bash
# Any one-shot primitive
rfa --api-url http://localhost:19092 mesh chaos kill
rfa --api-url http://localhost:19092 mesh chaos nat-shift
rfa --api-url http://localhost:19092 mesh chaos clock-skew --skew-ms 30000
```

The event appears in the Timeline tab within ≤6s of completion (chaos `detect()` exit + Jaeger indexing + UI poll cycle).

## Run continuous chaos via soak

```bash
rfa --api-url http://localhost:19092 mesh chaos soak \
    --duration 5m --interval 10s --seed 42
```

Timeline shows ~30 events at the end of a 5-minute soak (interval=10s).

## Query the endpoint directly

```bash
curl -s http://localhost:19092/api/chaos/timeline | jq
```

Returns `{events: [...]}` with the most recent 200-trace window worth of events.

## Filter by primitive (UI doesn't have this yet)

Use Jaeger UI directly: `http://localhost:16686/search?service=rfa&operation=rafka.chaos.primitive.executed&tags={"name":"nat_shift"}`.
