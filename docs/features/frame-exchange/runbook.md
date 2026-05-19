# frame-exchange — runbook

## Failure modes

### Mode 1 — frame.sent fires but no matching frame.received on peer

**Cause:** Peer's accept_uni loop crashed OR connection dropped between send and receive.

**Detection:**
```bash
# Count sent vs received over 1 min — should match within a few
SENT=$(curl -s "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.frame.sent&limit=100&lookback=1m" | python -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))")
RECV=$(curl -s "http://localhost:16686/api/traces?service=broker&operation=rafka.mesh.frame.received&limit=100&lookback=1m" | python -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))")
echo "sent=$SENT received=$RECV"
```

**Recovery:** Check broker stdout for `accept_uni: ...` errors or panics. If `run_frame_reader` returned Err early, the connection was registry-removed and broker is no longer listening — restart broker or wait for re-discovery.

### Mode 2 — decode_failed spans firing constantly

**Cause:** Wire-format break — sender and receiver have different envelope assumptions (e.g., one running pre-trace-propagation code, other running post).

**Recovery:** Pin both binaries to the same commit. Wire format: 32-byte W3C context header + bincode-encoded `InternalMeshFrame` enum.

### Mode 3 — cross-service trace_id is wrong (each side has its own trace_id)

**Cause:** `set_parent` not called on the receive-side span, OR encode_with_context wrote a default/empty context.

**Recovery:** Verify `crates/rafka-mesh-ops/src/lib.rs::encode_with_context` reads the current span context (not Context::default()), and `crates/rafka-node-base/src/lib.rs::run_frame_reader` calls `recv_span.set_parent(parent_ctx)` immediately after creating the span.

### Mode 4 — frame.sent_failed with `open_uni` error

**Cause:** iroh connection closed mid-tick (peer died OR network drop).

**Recovery:** Self-healing — `run_ping_sender` skips on error and tries the next tick. If consistently failing, peer is dead; topology should converge and remove it from registry within ~30s (QUIC idle timeout).

## Cross-references

* Parent: substrate.
* Sibling: [`peer-discovery runbook`](../peer-discovery/runbook.md), [`cross-service-tracing runbook`](../cross-service-tracing/runbook.md).
