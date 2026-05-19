# heartbeat — how-to

## Watch heartbeats in Jaeger

```
http://localhost:16686/search?service=gateway&operation=rafka.mesh.heartbeat&lookback=15m
```

Each trace = one tick. Click any to see `peer_count` tag.

## Via CLI

```bash
rfa mesh status
# table shows: peer_count + last-heartbeat-age per node
```

## API directly

```bash
curl -s "http://localhost:19090/api/heartbeat?service=gateway" | python -m json.tool
```

Returns the most recent heartbeat: `{node_id, peer_count, last_heartbeat_us}`.

## Verify peer_count is correct (not 0 falsely)

```bash
PYTHONIOENCODING=utf-8 python -c "
import urllib.request, json
d=json.loads(urllib.request.urlopen('http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.heartbeat&limit=20&lookback=2m').read())
counts = set()
for t in d.get('data', []):
    for tag in t['spans'][0]['tags']:
        if tag['key'] == 'peer_count':
            counts.add(str(tag['value']))
print('peer_count values seen:', sorted(counts))
"
```

For a 4-node mesh, expect `['0', '1', '2', '3']` (ramp-up early, then stable at 3 once converged).
