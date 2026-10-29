# Fabric certs

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Hooks](fabric-node-hooks.md) · [Node create](lifecycles/node-create.md)

This page owns the cert chain, its issuance, its renewal and its verification at a connection: who
holds each level, where a node-admin's signing material comes from, when a cert is issued or renewed
and to which exact birth, where the app reads it, and on which connections it is checked.

The embedding app starts the bootstrap node-admin with the fabric's CA material (`CertChoice::Ca`),
or with neither by name (`NoCerts`). RDM owns the cert format (`rdm_core::certs`), mints the issuing
certs, signs every member
cert, renews it before expiry ([node.cert.renew](lifecycles/node-cert-renew.md)), carries it to the
exact birth, and verifies the cert a dialer presents on every connection of a registered product ALPN
([node.connections.admit](lifecycles/node-connections-admit.md)). The app owns the CA material it
passes in and what the cert means outward, to clients and other systems. The whole vocabulary is the
[cert workflows table](#the-cert-workflows).

## The chain

| Level | Held by | Signs |
|---|---|---|
| fabric root | every node-admin, from the app at bootstrap (`CertChoice::Ca`) or minted on day 0; the trust anchor every verifier holds | the fabric CA, nothing else |
| fabric CA `CaCert { ca_pub, expiry_ms }` | every node-admin, kept as `fabric-ca` beside its data dir; minted under the root by the bootstrap admin, the fabric-primary, on day 0 (`fabric.ca.create`, step 1 of fabric.create) and rotated internally (`fabric.ca.renew`) | each mesh's issuing cert |
| mesh issuing cert `IssuingCert { mesh, issuer_pub, issued_ms, expiry_ms }` | that mesh's node-admins, in memory | member certs whose path.name is in that mesh |
| member cert `SignedCert { cert: NodeCert { name, mesh, node_id, incarnation, endpoint, issued_ms, expiry_ms }, sig, issuer: SignedIssuingCert }` | the birth it was issued to | nothing |

A cert's issue date (`issued_ms`, X.509's notBefore) is signed with the rest and records its issuance
only: no verifier refuses a cert on it. Every cert row takes its dates from the cert it records
(`issued_ms`, `expiry_ms`), never from its writer's clock, and its `renew_at` is derived from those and
the fabric's policy alone, so the issuer, the installer and any successor hold one value
([tracking](#tracking)).

The chain verifies member → issuer → CA → root against the fabric's root set, the anchors every
node holds (one, or two during a root rotation). A node-admin never issues a member cert that
outlives its issuer (`MemberOutlivesIssuer`), nor an issuing cert that outlives its CA
(`IssuerOutlivesCa`). Every node-admin holds the root (§Custody).

Verification (RDM's, in [node.connections.admit](lifecycles/node-connections-admit.md); Rafka's own accept
loop too until i145 e4 removes it) checks the chain in this order and
refuses at the first failure:

1. `CaBadSignature`: no root in the node's set verifies the CA cert;
2. `CaExpired`: the CA cert has expired;
3. `IssuerBadSignature`: the CA does not verify the issuing cert;
4. `IssuerExpired`: the issuing cert has expired;
5. `IssuerMeshMismatch`: the first segment of the member's path.name is not the issuer's mesh;
6. `BadSignature`: the issuing key does not verify the member cert;
7. `Expired`: the member cert has expired.

## Where a node-admin's signing material comes from

The bootstrap admin is started with the fabric root (`CertChoice::Ca`: `RDM_CA_SECRET`,
else `ca-secret` beside its data dir, else minted on day 0 when it has no seeds and no anchor:
`fabric.ca.create`, a step of [fabric.create](lifecycles/fabric-create.md)). A launched admin is
started with the root, the fabric CA's key and its mesh's issuing material, written by the launching
admin into its environment at launch (`RDM_CA_SECRET`, `RDM_FABRIC_CA_KEY`, `RDM_ISSUING_KEY`,
`RDM_ISSUING_CERT`; the CA's cert travels inside the issuing cert), the way `RDM_MESH_ID` is
(`mesh.issuer.create`, a step of [mesh.create](lifecycles/mesh-create.md)). It holds its mesh's issuer
and the fabric CA as given and mints no second one; it mints another mesh's issuer under that CA only
while it is fabric-primary (§Custody: only the fabric-primary mints). An admin with seeds or an
anchor and no root refuses to start by name, and so does an admin with a root off day 0 and no CA. No admin
signs a member cert that outlives its issuer (`MemberOutlivesIssuer`).

## How RDM is instantiated

```rust
pub enum CertChoice {
    /// The fabric root: the bootstrap admin, and every admin RDM launches, with its mesh's issuing
    /// material as its launch gave it.
    Ca { secret: CaSecret, issuing: Option<(IssuingKey, SignedIssuingCert)> },
    /// No certs, by explicit choice: every issuance is empty and every admission admits, each by name.
    NoCerts,
}
```

A node-admin started with neither is refused at start. RDM signs and verifies with what it
was given; the app never signs or verifies anything.

## When a cert is issued

| Event | Issuer | Carried in |
|---|---|---|
| a node's JoinNode is accepted (the first start inside node.create, and every node.start after a stop) | the accepting primary: the maker for a node-admin, the mesh-primary for an ordinary node | the JoinNode answer, as opaque bytes |
| a new mesh's first node-admin is deployed | the maker, which holds the root or an issuing cert that may mint for that mesh | that admin's launch |
| a member cert has lived two thirds of its TTL (`RDM_CERT_RENEW_AT`, with up to 5 % jitter) | the birth's issuer (its mesh-primary; the fabric-primary for a mesh-primary) | [node.cert.renew](lifecycles/node-cert-renew.md) to the running birth; the old cert stays valid to its own expiry |

Every JoinNode issues a fresh member cert (`node.cert.create`, [node.mesh.join](lifecycles/node-mesh-join.md)
step 5), so a node that starts again after a stop holds a new cert, and a new node over an inherited
data dir never reuses its predecessor's cert. A mesh's issuing cert is re-minted by the fabric-primary
at two thirds of its TTL and carried to that mesh's node-admins the same way
([mesh.issuer.renew](lifecycles/mesh-issuer-renew.md)); members issued after that carry the new issuer. A refused issuance refuses the join by name; the birth gets no cert and
is not admitted.

## Where the app reads it

The member cert is in [`NodeReadyCtx`](fabric-node-hooks.md#pre_node_ready) before
`pre_node_ready` runs, and on the running node handle RDM's start returns.

## Time

Cert validity and expiry are judged on rafka-time, and a birth's first JoinNode, judged before it
holds rafka-time, on the local clock named `local-bootstrap`
([rafka-time](fabric-node-hooks.md#rafka-time)).
A presented cert is judged with a skew allowance of one staleness floor (`RDM_STALENESS_MS`) on either
side of its validity.

## Tracking

The node's mesh-primary writes the birth's cert dates (`issued_at`, `expires_at`, `issuer_expires_at`, `renew_at`) on the node's record when the birth
holds the cert: a member's at its join's admission and on its renewal's `CertRenewed`, the mesh-primary's own when it installs the cert the fabric-primary
issued. They sit beside `data-dir-lost` and `data-dir-unreadable`; the written expiry is the receipt, and the dates are the birth's renewal state.
`nodes.get` surfaces them as `cert { issued_at, expires_at, issuer_expires_at }`. A successor mesh-primary reads its nodes' records and continues the
schedule from them; nothing about renewal lives only in memory. Every cert row is in the
fabric-level projection: `fabrics.get` shows the next expiry across the fabric, and a mesh whose renewals
stall (a row past `renew_at` by more than a floor with no `node.cert.renewed`) is surfaced on `fabrics.get` as `renewals: stalled`. There is no revocation: expiry is the revocation, which is why the
member TTL is a day. The TTLs and the renewal point are fabric policy, read once by the bootstrap admin
and fixed for the fabric, carried to every later admin in its entry pull the way `MESH_SPAWN_TYPE` is:
`RDM_MEMBER_CERT_TTL_SECS` (default 86400), `RDM_ISSUING_CERT_TTL_SECS` (default 2592000, 30 days),
`RDM_CA_CERT_TTL_SECS` (default 31536000, 365 days), `RDM_CERT_RENEW_AT` (default 0.67; the jitter, up
to 5 % of the TTL, is fixed). A member cert never outlives its issuer, an issuer never its CA, and the CA
never its root, whatever the policy says. The app passes in the fabric root once, at bootstrap (`CertChoice::Ca`), and never an issuer: it is the
trust anchor, and it signs nothing but RDM's CA. RDM mints its CA under it on day 0 and rotates the CA
internally on policy ([fabric.ca.renew](lifecycles/fabric-ca-renew.md)): a new CA chained to the same
anchor, every issuer re-minted, nothing pushed to any node but the ordinary renewals. The anchor itself
rotates only on the app's or a person's call with a new root
([fabric.root.update](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op)), through an
overlap in which every node trusts both anchors (`node.roots.update`), and the old one is retired once
nothing chains to it.

## Custody

There is one root. The one the app passes in at bootstrap (`CertChoice::Ca`) or later
(`fabric.root.update`) is the fabric's root from then on, in RDM's custody, rotated by RDM; the app's
own outward credentials never enter this chain. Every node-admin holds the root secret, as it does
today: written into its launch environment beside its mesh's issuing material (`RDM_CA_SECRET`, the
way `RDM_ISSUING_KEY` is) and kept as `ca-secret` beside its data dir; a root rotation refreshes it on
every admin through `node.roots.update` (the secret to admins, the public set to every other node); an
internal CA rotation touches no node's trust set. Every admin also holds the current CA material:
written into its launch beside the root (`RDM_FABRIC_CA_KEY`; the bootstrap admin mints it on day 0,
`fabric.ca.create`) and kept as `fabric-ca` beside its data dir, which it reads back on a restart the
way it reads `ca-secret`. At each CA renewal `node.issuer.update` re-writes it, carrying the CA material with
the issuing material minted under it ([fabric.ca.renew](lifecycles/fabric-ca-renew.md) step 2).
Only the fabric-primary mints and rotates with it; a successor the election names over an unheard
fabric-primary mints and rotates from its own copy, so the loss of a fabric-primary costs the chain nothing.
The root is never in a row, a frame or a digest.

## On a connection

No cert is checked on a Node RPC call or a gossip frame: inside RDM's control plane a node's identity
is its iroh endpoint key, and admission to the mesh is the JoinNode check against the deployed birth
([node-create.md](lifecycles/node-create.md) step 4). On every inbound connection of an ALPN the
embedding app registered at `bind`, the acceptor reads the dialer's member cert on the connection's
first bi-stream, verifies it (signature to the issuer, the issuer to the CA, the CA to a root the node
holds, the issuer's mesh, the three expiries), writes the verdict back, and admits or drops the connection
([node.connections.admit](lifecycles/node-connections-admit.md)); the verdict is a connection fact
([connections.md](connections.md) §1.2, §12). RDM never registers the step for its own two ALPNs: a
registration naming `rdm_transport::ALPN` or `iroh_gossip::ALPN` is refused by name at `bind`. The
member cert is also what the app presents outward, to clients and other systems.

## The cert workflows

One vocabulary, `<entity>.<thing>.<verb>`, the event in the past tense.

| Workflow / command | Kind | Caller → target | Wire | Page |
|---|---|---|---|---|
| `fabric.root.create` | step inside fabric.create | the bootstrap admin, day 0 | none (local) | [fabric.create](lifecycles/fabric-create.md) step 1 |
| `fabric.ca.create` | step inside fabric.create | the bootstrap admin, day 0, under the root | none (local) | [fabric.create](lifecycles/fabric-create.md) step 1 |
| `mesh.issuer.create` | step inside mesh.create | the fabric-primary, at the mesh's acceptance | none (the first admin's launch env) | [mesh.create](lifecycles/mesh-create.md) step 2 |
| `mesh.issuer.renew` | workflow, on the fabric-primary's tick | the fabric-primary → the mesh-primary, and through it (forwarded) every other node-admin of the mesh | `node.issuer.update` per admin | [mesh.issuer.renew](lifecycles/mesh-issuer-renew.md) |
| `node.issuer.update` | primitive | the fabric-primary → one node-admin, exact birth: the mesh-primary directly, any other admin forwarded through the mesh-primary | Status 0x1B `UpdateIssuer`, reply `IssuerUpdated` | [node.issuer.update](lifecycles/node-issuer-update.md) |
| `node.cert.create` | step inside node.mesh.join | the accepting primary, before it admits | none (the join answer) | [node.mesh.join](lifecycles/node-mesh-join.md) step 5 |
| `node.cert.renew` | primitive, on the issuer's tick | mesh-primary → member; fabric-primary → mesh-primary | Status 0x1B `RenewCert`, reply `CertRenewed` | [node.cert.renew](lifecycles/node-cert-renew.md) |
| `node.cert.get` | a field of nodes.get | any caller → the view | `NodeView.cert` on the `Nodes` reply | [typed-object table](node-rpc-envelope.md#typed-object-contract) |
| `node.connections.admit` | primitive, on accept | the acceptor, every inbound connection of a registered ALPN | the connection's first bi-stream; not a Node RPC op | [node.connections.admit](lifecycles/node-connections-admit.md) |
| `fabric.ca.renew` | workflow, on the fabric-primary's tick | the fabric-primary → every mesh (issuer) | `mesh.issuer.renew` per mesh; no trust-set change | [fabric.ca.renew](lifecycles/fabric-ca-renew.md) |
| `fabric.root.update` | caller op | the app or a person → the fabric-primary | a single call like `fabric.update`; reply `fabric.root.updated`; then `node.roots.update` per node and `fabric.ca.renew` | [fabric.root.update](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op) |
| `node.roots.update` | primitive, inside `fabric.root.update` only | the fabric-primary → one node, exact birth | Status 0x1B `UpdateRoots`, reply `RootsUpdated` | [node.roots.update](lifecycles/fabric-ca-renew.md#noderootsupdate-the-primitive) |
