# frame-exchange — how-to

## Trigger a round trip

Run gateway + at least one peer. Gateway auto-sends Ping every 10s once peers connect; no manual trigger needed.

```bash
rfa mesh node add --type gateway
rfa mesh node add --type broker
# wait 15s — gateway has fired at least one ping by now
```

## Find a unified round-trip trace

```
http://localhost:16686/search?service=gateway&operation=rafka.mesh.frame.sent&lookback=5m
```

Open any trace — it should contain 4 spans across `gateway` + the peer service. Same trace_id across all four. The waterfall shows: gateway.frame.sent → peer.frame.received → peer.frame.sent → gateway.frame.received.

## Pull the 4-span breakdown

```bash
TID=$(curl -s "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.frame.sent&limit=1&lookback=2m" | python -c "import sys,json; print(json.load(sys.stdin)['data'][0]['traceID'])")
curl -s "http://localhost:16686/api/traces/$TID" | python -c "import sys,json; d=json.load(sys.stdin); proc=d['data'][0]['processes']; [print(s['operationName'], proc.get(s.get('processID'),{}).get('serviceName')) for s in d['data'][0]['spans']]"
```

## Force a decode failure to verify the error path

(Substrate-internal — no CLI command yet.) Manually open a uni stream and write random bytes; the receiver fires `rafka.mesh.frame.decode_failed` with the byte length and error message.

```
http://localhost:16686/search?service=broker&operation=rafka.mesh.frame.decode_failed&lookback=15m
```
