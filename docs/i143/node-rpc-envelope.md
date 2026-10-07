# Node RPC envelope: the fence, then the invocation

One QUIC bi-stream is one invocation. Two length-prefixed frames go out; one reply (or a stream of
reply frames, then FIN) comes back.

```text
fence_len: varint | fence | body_len: varint | body | FIN
```

## Frame 0: the fence (`schemas/node-rpc/fence.schema.json`)

```json
{ "target_node_id": "0h7k2m9q4ztd", "op": 27 }
```

The receiver reads exactly these bytes and checks them against what it is and what it serves,
before the body is read, allocated or parsed:

| check | refusal | connection |
|---|---|---|
| `target_node_id` is not my minted id | `-32025 STALE_TARGET` (425) | dropped: the caller reached a replacement at that path, or misrouted; it re-resolves |
| `op` is not an op I serve | `-32027 UNSERVED_OP` (421) | kept |

The fence is core: the same two checks for every family. It carries nothing about
the process birth: which birth a caller is talking to is the resolver's and the pool's knowledge
(the incarnation, held locally, never sent), and a dial to a superseded birth is invalidated before
dispatch. A restarted node keeps its id; a replacement is a new id.

Encoding: JSON on the control plane. The data plane (op `0x12`, `data-frame`) carries the same
two fields in the same order as postcard, because that path is hot and nothing else is.

## Frame 1: the invocation (`schemas/node-rpc/invocation.schema.json`)

```json
{
  "context": {
    "caller_system": "node-admin",
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "baggage": { "scenario": "fabric_soak__seeded", "operation": "node-restart" }
  },
  "rpc": {
    "jsonrpc": "2.0",
    "id": "req-4421",
    "method": "rdm.status.declare_node_state",
    "params": { "subject": "7q2d8n3kx0vm", "state": "ready-for-traffic" }
  }
}
```

- `context` is observability only: W3C `traceparent`/`tracestate`, allowlisted baggage, the caller
  system. A part over its bound is dropped by name and the call continues.
- `rpc` is JSON-RPC 2.0 with named params. `method` must be registered under the fence's `op` in
  the op ledger, or the call is refused `-32028 METHOD_NOT_UNDER_OP`.

## Reply

```json
{ "jsonrpc": "2.0", "id": "req-4421", "result": { "outcome": "applied" } }
```

An error carries `data.http_status`, `data.receiver{node_id,path}` and the refused field verbatim,
so one span holds both sides of every refusal.

## The op ledger (`schemas/node-rpc/openrpc.json` → `x-op-ledger`)

The one registry of codes: a code names a family and its owner (`core` = exactly `echo` and
`forward`; `rdm`; `rafka`; `testkit` in `0x70–0x7F`), and the methods registered under it. The
fence carries the code; the body carries the method name. A retired code is never reissued.

## What this replaces

`RequestTarget{node_id, incarnation, slot, freshness}` and the postcard `RequestHeader`.
`FreshnessToken`, `SlotPolicy`, `EndpointSlot`, the slots themselves (no port, no named service
surface: one endpoint per process and the op selects the handler) and the resolver's `SlotsMoved`
are deleted; `TransportId` is `EndpointId`. Incarnation stays where it was off the wire: the Node row, the digest, the launch, the
lifecycle events, the connections rows, the resolver's lineage and the pool key.
