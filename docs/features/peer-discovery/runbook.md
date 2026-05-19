# peer-discovery — runbook

## Failure modes

### Mode 1 — Nodes boot but don't see each other (peers=0 forever)

**Cause:** mdns blocked at OS/firewall layer (Windows network profile = Public), OR all nodes have different `RAFKA_SEED_NODES` mismatched against actual node_ids.

**Detection:**
```bash
rfa mesh status
# → all rows show peers=0 despite multiple nodes running
```

**Recovery:**
- Confirm mdns can reach: `Get-NetFirewallRule | ? DisplayName -match 'mdns'`. Profile must include LAN traffic.
- If using seeds, re-verify node_id hex matches: each node's stdout `identity ready` log line is the source of truth.

### Mode 2 — mdns republish storm — duplicate peer.discovered spans every 1s

**Cause:** `watch_mdns` lost its dedup guard. iroh's `LocalSwarmDiscovery` re-announces every ~1s; without `registry.contains_key()` check, each republish triggers a re-dial.

**Detection:**
```bash
# Count discovered events for one peer over 30s — should be 1, not 30+
curl -s "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.peer.discovered&limit=100&lookback=1m" | python -c "import sys,json; d=json.load(sys.stdin); peer='<hex>'; n=sum(1 for t in d['data'] for tag in t['spans'][0]['tags'] if tag['key']=='peer_id' and tag['value']==peer); print(f'discoveries for {peer[:8]}: {n}')"
```

**Recovery:** Re-enable the dedup guard in `crates/rafka-node-base/src/lib.rs::watch_mdns`:
```rust
if registry.contains_key(&peer_id_str) { continue; }
```

### Mode 3 — peer.connected fires but registry stays empty (peer_count=0 in heartbeats)

**Cause:** Insertion path missing in dial/accept code. Multiple insert sites must all use the SAME `Arc::clone(&peer_registry)` — never a deep clone of the DashMap itself.

**Recovery:** Grep `crates/rafka-node-base/src/lib.rs` for `registry.insert` — three sites must exist (`dial_seeds`, `watch_mdns`, `start_accept_loop`). All must operate on `Arc::clone(&peer_registry)`, not on a new DashMap.

### Mode 4 — Cross-NAT discovery fails

**Cause:** iroh's DERP relay disabled or unreachable. mdns is LAN-only; cross-NAT requires the relay tier.

**Recovery:** Currently disabled in v2 (`RelayMode::Disabled` in `IrohMeshTransport::new`). Add seed-list addresses for cross-NAT in the meantime. Future sprint: enable DERP for cross-NAT.

## Cross-references

* Parent: substrate.
* Sibling: [`boot-chain runbook`](../boot-chain/runbook.md).
