# node.connection.create

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Fabric certs](../fabric-certs.md#on-a-connection) · [Connections](../connections.md)

A primitive. `node.connection.create` is the acceptor's verification of a dialer's member cert on an
inbound connection of a registered product ALPN, before it serves anything on that connection. It
answers `admitted` or `refused { reason }`, and the answer is a connection fact. It is not a Node RPC
op: it runs on the connection's first bi-stream, in RDM's framing, inside the accept loop RDM owns. It
runs on no Node RPC connection and no gossip connection: RDM never registers it for its own two ALPNs,
and a registration naming them is refused by name at `bind`.

What it checks is what the connection carries, never who opened it: a correctly signed, unexpired cert
of this fabric is admitted whoever presents it, and the step reads no node kind, no primary and no sender
identity. The cert check is the credential R-RX1 assumed, not a sender gate.

## Operation contract

| Field | Contract |
|---|---|
| Caller | The acceptor, for every inbound connection whose ALPN the embedding app registered the step for at `bind` |
| Target | One inbound connection, identified by the dialer's endpoint key and the connection id |
| Exchange | The dialer writes its member cert on the connection's first bi-stream and keeps the send half open; the acceptor reads it within one staleness floor (`RDM_STALENESS_MS`, so a slow dial is not refused before QUIC's own idle timer would have reaped a dead one), writes the verdict back on the same stream, then serves or drops. After the verdict the dialer writes its intent on the same stream, `Direct` or `Tunnel { origin, destination }` (i148), and the acceptor answers `Opened { peer }` or `Refused { reason }`; application streams are admissible only after `Opened`, and the control stream stays open for the connection's life: it is never closed after the exchange, and its close is the connection's |
| Verification | The member's signature against its issuer's key; the issuer's signature against the fabric CA's key and the CA's against a root of the node's set; the issuer's mesh is the first segment of the member's path.name; the three expiries (CA, issuer, member), judged on rafka-time (else `local-bootstrap`) with one staleness floor of skew on either side |
| Result | `admitted`: the connection is served. `refused { reason }` with `no-credential`, `ca-bad-signature`, `ca-expired`, `issuer-bad-signature`, `issuer-expired`, `issuer-mesh-mismatch`, `member-outlives-issuer`, `issuer-outlives-ca`, `bad-signature` or `expired`: the verdict is written, then the connection is closed |
| Row | A refusal is written as the connection fact `Refused` with its reason, source the dialer when a topology view names its key, destination the acceptor; a dialer no view names is refused with the span only, no row ([connections.md §1.2](../connections.md#12-connection-state)); an admission is the ordinary `Connected` fact |
| Never | Anything about the dialer inferred beyond this one connection: `Refused` is the acceptor's fact about one dial, never proof about the dialer; no status set, no frame published, no mark on any node |
| `NoCerts` | A node-admin started `NoCerts` admits every connection by name, spanned `rdm.node.connection.serve.via-no-certs-configured`; an empty cert presented to a fabric with certs is `no-credential` |
| Spans | `rdm.node.connection.serve.via-admitted` (source, destination, connection, alpn, cert_expires_at, clock) or `rdm.node.connection.reject.via-refused` (the same, plus `reason`) on the acceptor, as the dial span's child; `rdm.node.connection.create.via-dial` (destination, alpn, cert_expires_at, outcome, reason, elapsed_ms) on the dialer |

## Numbered end-to-end sequence

1. The dialer's QUIC connection on a registered ALPN completes; iroh has checked the peer holds the
   private key of the endpoint id dialed, which proves key possession and nothing more.
2. The dialer opens the first bi-stream, writes the member cert it holds (the one most recently
   installed: its JoinNode's or its last [node.cert.renew](node-cert-renew.md)'s), and waits.
3. The acceptor reads it within the floor and verifies as the contract says.
4. `admitted`: the verdict is written back; the connection is served. `refused { reason }`: the verdict is
   written back, the `Refused` fact is written, the connection is closed.
5. The dialer reads the verdict before it sends anything else: a refusal is a named end of this dial, and
   the dialer's own reconnect rule ([connections.md §9](../connections.md#9-direct-reconnect-is-owned-by-every-node))
   decides what follows.

## Command and completion messages

There is no wire op. The exchange on the first bi-stream, as the spans record it:

```text
→ cert { bytes }
← admitted
← refused { reason: "expired" }
```

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant D as Dialer
    participant A as Acceptor
    D->>A: 1. QUIC connect on a registered ALPN (key possession checked by iroh)
    D->>A: 2. first bi-stream: member cert
    Note over A: 3. verify: member → issuer → root, mesh, expiries, skew
    alt admitted
        A-->>D: 4. admitted
        Note over A: the connection is served; Connected fact
    else refused
        A-->>D: 4. refused { reason }
        Note over A: Refused fact with the reason; the connection is closed
    end
    Note over D: 5. reads the verdict first; its reconnect rule decides what follows
```

## Failure and receipts

A dialer that writes no cert within the floor is `no-credential`. A read the acceptor cannot complete
(the stream reset, the connection gone) is neither verdict: nothing is written and the connection is
gone on its own. Rafka's `rafka.mesh.auth.*` spans and its `RAFKA_NO_CERT` / `RAFKA_CA_PUBKEY` modes are
retired with its accept loop; a fabric runs with certs or is started `NoCerts` by name.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The cell: one admitted dial with a test-kit
cert and one refused by each named reason, with the connection facts beside the spans; the control: a
Node RPC and a gossip connection on the same endpoint with no admission span.
