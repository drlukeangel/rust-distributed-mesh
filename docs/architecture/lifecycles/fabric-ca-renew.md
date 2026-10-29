# fabric.ca.renew and fabric.root.update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Fabric certs](../fabric-certs.md) · [mesh.issuer.renew](mesh-issuer-renew.md)

Two levels sit above a mesh's issuers ([the chain](../fabric-certs.md#the-chain)): the **fabric root**,
the trust anchor the embedding app passed in, which signs nothing but RDM's CA; and the **fabric CA**,
RDM's own intermediate, signed by the root, which signs every mesh's issuing cert. They rotate
differently, and this page owns both:

- `fabric.ca.renew` rotates the CA internally, on policy, with nothing pushed to any node but the
  ordinary issuer and member renewals: the new CA chains to the same anchor, so every verifier's trust
  set is unchanged.
- `fabric.root.update` rotates the anchor itself, only on the app's or a person's call with a new root,
  through an overlap in which every node trusts both anchors (`node.roots.update`), and retires the old
  one once nothing chains to it.

Nothing is cut and nothing reconnects at any step of either.

## `fabric.ca.renew`

| Field | Contract |
|---|---|
| Runner | The fabric-primary, which holds the root secret (as every node-admin does, [custody](../fabric-certs.md#custody)) |
| When | Its own tick, once the CA has lived `RDM_CERT_RENEW_AT` of its TTL (`RDM_CA_CERT_TTL_SECS`, default 365 days); also run by `fabric.root.update` step 3, under the new anchor |
| Steps | 1 mint a new CA keypair and sign its cert with the root; write the CA row; 2 [mesh.issuer.renew](mesh-issuer-renew.md) for every mesh under the new CA, each `node.issuer.update` carrying the new CA material; members renew under the new issuers at their own points |
| Trust set | Unchanged: the new CA chains to the same anchor every node already trusts |
| Overlap | The old CA, and every issuer and member it signed, stay valid to their own expiry; an issuer never outlives its CA, a member never its issuer |
| Receipt | The CA row carries each mesh's issuer receipt; complete when every mesh in the view has one |
| Never | A cert cut short, a status set, a frame published, a connection closed, a root changed |
| Spans | `rdm.node_admin.ca.update.via-renewal` (ca_expires_at, previous_ca_expires_at, meshes, outcome) on the fabric-primary, and each `mesh.issuer.renew`'s spans |

## `fabric.root.update`, the caller op

| Field | Contract |
|---|---|
| Caller | The embedding app or a person, to the fabric-primary; a single call |
| Request | `fabric.root.update { secret }`, the new anchor's secret |
| Reply | `fabric.root.updated` once the rotation is accepted (the roots row `minted`); its progress is `fabric.get`'s `roots { phase, expires_at }`. Refused by name while a rotation is in flight (`rotation-in-progress`) and for a root already trusted (`root-already-trusted`) |
| Phases | `minted` → `distributed` (both anchors trusted everywhere) → `ca-rotated` → `retired` (one anchor again); the roots row is durable and a successor continues from it |
| Steps | 1 write the new root secret beside the old one on the fabric-primary; row `minted`; 2 `node.roots.update { roots: [old, new] }` to every node, admins first (the secret to admins, the public set to every other node); row `distributed`; 3 `fabric.ca.renew` under the new anchor, which re-mints every issuer; row `ca-rotated`; 4 wait until every cert row chains to the new anchor (at most one member TTL after step 3); 5 `node.roots.update { roots: [new] }` to every node; row `retired`; the old secret is destroyed everywhere |
| Bound | The old anchor's own expiry: a rotation not retired by then is surfaced on `fabric.get` as `roots: stalled` |
| Never | A cert cut short, a status set, a frame published, a connection closed; a node that has not yet received a phase keeps the set it holds, which admits both anchors' chains throughout |
| Spans | `rdm.node_admin.root.update.via-caller` (phase, root_expires_at, previous_root_expires_at, nodes, outcome) on the fabric-primary; `rdm.node.root.update.via-installed` (node, roots, phase, secret ∈ {`carried`, `public-only`}) on each node |

### `node.roots.update`, the primitive

| Field | Contract |
|---|---|
| Caller | The fabric-primary |
| Target | One node, by exact `(node.id, incarnation)` |
| Request | `UpdateRoots { node_id, incarnation, roots: [root public key, …], phase, secret? }` on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm); one or two roots, never none; the secret only to a node-admin |
| Reply | `RootsUpdated { node_id, incarnation, roots }`, the receipt; a set that drops a root the node still admits a chain under is refused by name (`root-still-chained`), which is what keeps step 5 after step 4 |
| Launch | A launched node is written the current set at launch (`RDM_ROOTS`), the way `RDM_MESH_ID` is, so a node born mid-overlap trusts both; a launched node-admin is also written the root secret (`RDM_CA_SECRET`) and the current CA material (`RDM_FABRIC_CA_KEY`; the CA's cert travels inside the issuing cert) |

## Numbered end-to-end sequence (root update)

1. `fabric.root.update { secret }` arrives at the fabric-primary. It writes the new secret beside the old
   one, writes the roots row `minted`, and answers `fabric.root.updated`.
2. `node.roots.update { [old, new] }` to every node of the fabric, node-admins first. From its install a
   node admits a chain to either anchor. The row goes `distributed` on the last receipt.
3. `fabric.ca.renew` under the new anchor: a new CA signed by the new root, every mesh's issuer
   re-minted under it ([mesh.issuer.renew](mesh-issuer-renew.md)). The row goes `ca-rotated`. Members
   renew under the new issuers at their own points ([node.cert.renew](node-cert-renew.md)).
4. The tick waits until every cert row chains to the new anchor: at most one member TTL after step 3. A
   member whose renewal has not reached it is the ordinary not-sent case, retried; the rotation waits
   rather than retiring an anchor something still chains to.
5. `node.roots.update { [new] }` to every node. The row goes `retired` on the last receipt, and the old
   secret is destroyed on every admin.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller (the app or a person)
    participant F as Fabric-primary
    participant N as Every node
    participant M as Every mesh (issuer)
    C->>F: 1. fabric.root.update { secret }
    F-->>C: fabric.root.updated (row minted)
    F->>N: 2. node.roots.update [old, new]
    N-->>F: node.roots.updated
    Note over F: row distributed
    Note over F: 3. fabric.ca.renew under the new anchor
    F->>M: mesh.issuer.renew under the new CA
    Note over F: row ca-rotated; members renew at their own points
    Note over F: 4. every cert row chains to the new anchor
    F->>N: 5. node.roots.update [new]
    N-->>F: node.roots.updated
    Note over F: row retired; old secret destroyed
```

An internal `fabric.ca.renew` is step 3 alone, with no caller and no `node.roots.update`.

## Failure and receipts

A node not reached in a phase is retried every tick; the phase does not advance without it, and
nothing expires meanwhile that the overlap does not cover. A successor fabric-primary continues from
the roots row and the CA row, and from its own copy of the root secret, which every node-admin holds
([custody](../fabric-certs.md#custody)). A successor that does not hold the CA the CA row names (a renewal whose step 2 had not reached it) never mints under a CA it does not hold: it runs `fabric.ca.renew` again from the root secret, and step 2 reaches every admin under the new CA.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The cells, with the TTLs set to seconds in the
test kit: an internal `fabric.ca.renew` with a dial admitted before, during and after and no
`node.roots.update` span; a `fabric.root.update` through all five phases with a dial admitted at every
phase, the spans and the roots row beside it, and a retire attempted before step 4 refused by name.
