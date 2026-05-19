# peer-discovery — how-to

## Watch peers connect on a fresh mesh

```bash
# spawn 4 binaries with mdns (no seed env var) — they auto-discover
for t in gateway broker compute registry; do
  rfa mesh node add --type $t
done

# wait 3-5s, then check
rfa mesh status
```

Output should show `peers=3` on every node (full mesh).

## Use explicit seeds instead of mdns

```bash
# Launch broker first, capture its node_id from boot logs
RAFKA_DATA_DIR=./data/broker rafka-broker
# stdout: identity ready node_id=<hex>

# Launch other nodes with seed
RAFKA_SEED_NODES=<broker_hex>@127.0.0.1:14820 rafka-gateway
```

## View pairwise handshakes in Jaeger

```
http://localhost:16686/search?service=gateway&operation=rafka.mesh.peer.connected&lookback=15m
```

Each trace's `peer_id` tag identifies the counterpart. For an N-node mesh: N×(N-1)/2 pairs, 2 spans per pair (one inbound, one outbound).

## Trigger a disconnect to verify cleanup

```bash
rfa mesh node remove <name>
# survivors emit rafka.mesh.peer.disconnected within ~30s (QUIC idle timeout)
```

```
http://localhost:16686/search?service=broker&operation=rafka.mesh.peer.disconnected&lookback=15m
```

The disconnect span's `reason` tag is the iroh `Connection::closed()` reason (e.g., `timed_out`, `application_closed`).
