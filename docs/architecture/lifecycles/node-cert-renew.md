# node.cert.renew

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Fabric certs](../fabric-certs.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

A primitive. `node.cert.renew` installs a fresh member cert on a running node before the one it holds
expires. It is run by the birth's issuer on its own tick, never by the node: the node holds
no key and asks for nothing. It is a single call, not a Build. From the install the node presents the
new cert on every connection it opens; nothing is terminated, nothing reconnects, no status changes and
no gossip frame is published. A renewal is invisible to every other node.

## Operation contract

| Field | Contract |
|---|---|
| Caller | The birth's issuer: its mesh-primary for a member, the fabric-primary for a mesh-primary; the fabric-primary renews its own cert in-process with its mesh's issuing key, with no call (`via-renewal`, outcome `installed`) |
| Target | One node, by exact `(node.id, incarnation)`; a cert naming another birth is refused by name |
| When | Once the held cert has lived `RDM_CERT_RENEW_AT` (default two thirds) of its TTL, plus a jitter of up to 5 % of the TTL, read from the birth's cert row ([tracking](../fabric-certs.md#tracking)) |
| Request | `RenewCert { node_id, incarnation, cert }` on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm); the cert is signed with the mesh's current issuing key, its expiry cut to the issuer's when the issuer expires first (`MemberOutlivesIssuer`) |
| Reply | `CertRenewed { node_id, incarnation, expires_at }`, the receipt: the node holds and presents the new cert. A node that cannot decode the cert answers a named refusal and keeps the one it holds |
| Row | On `CertRenewed` the node's mesh-primary writes the birth's cert dates (`issued_at`, `expires_at`, `renew_at`) on the node's record and writes the record whole: the written expiry is the receipt. A mesh-primary's own cert, issued by the fabric-primary, it writes on its own record when it installs it. A renewal not answered writes nothing, and the row's `renew_at`, already passed, renews it on the next tick |
| Overlap | The old cert stays valid until its own expiry; a dial in flight during the install is admitted on either |
| Never | A connection closed, a status set, a frame published, a node marked. A node past its TTL whose renewal has not reached it is refused on its next new dial by name ([node.connection.create](node-connections-admit.md)) and its existing connections stay |
| Spans | `rdm.node_admin.cert.update.via-renewal` (node, node_id, incarnation_id, issued_at, expires_at, previous_expires_at, outcome: `installed` / `not-sent` / `refused`) on the issuer; `rdm.node.cert.update.via-installed` on the node |

## Numbered end-to-end sequence

1. The issuer's tick reads its cert rows; a row whose `renew_at` has passed names a birth to renew.
2. It signs a fresh member cert for that exact birth with the mesh's current issuing key, on rafka-time.
3. It sends `RenewCert` to the birth's held address. `NotSent` or `Indeterminate` is spanned and
   retried on the next tick, until the old cert expires; nothing else happens.
4. The node checks the cert names its own `(node.id, incarnation)`, installs it, and answers
   `CertRenewed`. Every connection it opens from now presents the new cert.
5. The node's mesh-primary writes the expiry the node answered on the node's record. The old cert expires on its own.

## Command and completion messages

```text
node.cert.renew { node_id, incarnation, cert }
  ← node.cert.renewed { node_id, incarnation, expires_at }
```

The same primitive carries a re-minted issuing cert's members: after [mesh.issuer.renew](mesh-issuer-renew.md),
members renewed from then on carry the new issuer, and members not yet due keep the old one until their
own turn.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant A as Issuer (mesh-primary)
    participant N as Node (exact birth)
    Note over A: 1. tick: a cert row past renew_at
    Note over A: 2. sign a fresh cert
    A->>N: 3. RenewCert { node_id, incarnation, cert }
    Note over N: 4. install; presented on every new connection
    N-->>A: CertRenewed { expires_at }
    Note over A: 5. write the expiry the node answered; nothing else moves
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty). A renewal that
does not reach the node is retried every tick and marks the node nothing: reachability is
[node recovery](node-recovery.md)'s business, not this primitive's. A successor primary folds the cert
rows and continues the schedule from them.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The cell: a member whose TTL is set to seconds
in the test kit dials past its first expiry and is admitted, with both spans and the admission fact.
