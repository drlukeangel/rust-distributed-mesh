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
| Request | `RenewCert { node_id, incarnation, cert, cert_dates }` on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm); the cert is signed with the mesh's current issuing key, its expiry cut to the issuer's when the issuer expires first (`MemberOutlivesIssuer`); `cert_dates` the cert's dates as the issuer computed them |
| Reply | `CertRenewed { node_id, incarnation, expires_at }`, the receipt: the node holds and presents the new cert. A node that cannot decode the cert answers a named refusal and keeps the one it holds |
| Row | The node writes the cert's dates, `cert_dates`, on its own record when it installs the cert (Q356), then answers `CertRenewed`. A renewal not answered writes nothing, and the record's `renew_at`, already passed, renews it on the next tick |
| Overlap | The old cert stays valid until its own expiry; a dial in flight during the install is admitted on either |
| Never | A connection closed, a status set, a frame published by the renewal. A spent renewal call marks the birth as any spent call does (step 3). A node past its TTL whose renewal has not reached it is refused on its next new dial by name ([node.connection.create](node-connections-admit.md)) and its existing connections stay |
| Spans | `rdm.node_admin.cert.update.via-renewal` (node, node_id, incarnation_id, issued_at, expires_at, previous_expires_at, outcome: `installed` / `not-sent` / `refused`) on the issuer; `rdm.node.cert.update.via-installed` on the node |

## Numbered end-to-end sequence

1. The issuer's tick reads its cert rows; a row whose `renew_at` has passed names a birth to renew.
2. It signs a fresh member cert for that exact birth with the mesh's current issuing key, on rafka-time.
3. It sends `RenewCert` to the birth's held address on `Send` ([Node RPC §43](../node-rpc.md#43-send-delivery-is-the-runtimes)). A call that ends
   `NotSent` or `Indeterminate` is spanned; the spent call marks the birth ([node recovery](node-recovery.md) step 1), and the tick sends it nothing while the mark holds.
4. The node checks the cert names its own `(node.id, incarnation)`, installs it, and answers
   `CertRenewed`. Every connection it opens from now presents the new cert.
5. The node's own record holds the dates. The old cert expires on its own.

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
    A->>N: 3. RenewCert { node_id, incarnation, cert, cert_dates }
    Note over N: 4. install; presented on every new connection; writes cert on its own record
    N-->>A: CertRenewed { expires_at }
```

## States written

This operation writes no state. The node writes the cert's dates on its own record (Q356), as `cert` on a `node-record` line in `nodes.jsonl`.

| written | by | when |
|---|---|---|
| `cert` | the node, on its own record | it installed the cert, before it answers `CertRenewed` |

As stored:

`cert`:

```json
{"len":1402,"specversion":"1.0","id":"01a1ae8c-ec21-73c0-9479-ba6a5711d2d9","source":"rdm://ysmfv4vw1kf5/f588axng119c/6m7gvt5yr8a0","type":"node-record","subject":"f588axng119c","time":"2026-11-06T01:50:33.505Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"f588axng119c","path":"mesh1.node-admin","name":"1","type":"node-admin","incarnation":"6m7gvt5yr8a0","state":"ready","endpoint_id":"babee7216b4204145d2e16726952ed50ad837a717eaa07aa5a4035d64fbd0291","transport_addr":"127.0.0.1:53841","listeners":[],"runtime":{"deployment_id":"cenec4vf1wsd","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1512764,"start":65842742}}},"is_primary":1,"is_fabric_primary":1,"cert":{"issued_at":1793929833503,"expires_at":1793929845503,"issuer_expires_at":1796521824989,"renew_at":1793929841608},"signing_cert":{"issued_at":1793929824989,"expires_at":1796521824989,"renew_at":1795772887740},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1512719/cert-renewal-renewed-402e0wx2gpsq"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/deps/main-43afc48aad99affc","sha256":"43f0ad77d7958fbdca5cbbeed875f9b1edc680b5fbccbe489401ab3dc82f28b6","image":null},"config":{},"tags":[]},"crc32":"27b98682"}
```

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty). A renewal that
does not reach the node ends on its `Send`; its spent call marks the birth for
[node recovery](node-recovery.md), and the tick renews it once the mark ends. A successor primary folds the cert
rows and continues the schedule from them.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The cell: a member whose TTL is set to seconds
in the test kit dials past its first expiry and is admitted, with both spans and the admission fact.
