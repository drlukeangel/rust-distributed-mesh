# Fabric certs

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Hooks](fabric-node-hooks.md) · [Node create](lifecycles/node-create.md)

This page owns the cert chain and its issuance: who holds each level, where a node-admin's
signing authority comes from, when a cert is issued and to which exact birth, and where the app
reads it.

Rafka instantiates RDM with its signing capability. RDM decides when a cert is issued and
to which exact birth, and carries the cert to that birth. Rafka owns the cert format, the
keys and every use of the cert. RDM never parses or verifies a cert.

## The chain

| Level | Held by | Signs |
|---|---|---|
| fabric root CA | the fabric-primary, put there by its launcher or minted on day 0 | each mesh's issuing cert |
| mesh issuing cert `IssuingCert { mesh, issuer_pub, expiry_ms }` | that mesh's node-admins, in memory | member certs whose path.name is in that mesh |
| member cert `SignedCert { cert: NodeCert, sig, issuer: SignedIssuingCert }` | the birth it was issued to | nothing |

The chain verifies member → issuer → root against the one fabric CA public key
(`RAFKA_CA_PUBKEY`). A mesh's node-admin without the root signs only members of its own mesh
(`NoRootToMint`), and never issues a member cert that outlives its issuer
(`MemberOutlivesIssuer`). The root stays where its launcher put it: a node-admin of another mesh
holds its mesh's issuing key, never the root.

Rafka's verification (`crates/rafka-node-transport/src/cert.rs::verify_cert_sig_expiry`) checks
the chain in this order and refuses at the first failure:

1. `IssuerBadSignature`: the root does not verify the issuing cert;
2. `IssuerExpired`: the issuing cert has expired;
3. `IssuerMeshMismatch`: the first segment of the member's path.name is not the issuer's mesh;
4. `BadSignature`: the issuing key does not verify the member cert;
5. `Expired`: the member cert has expired.

## Where a node-admin's signing authority comes from

At boot a node-admin resolves its authority in this order
(`node-admin/src/lib.rs::resolve_boot_authority`):

1. `RAFKA_CA_SECRET_HEX`, else `ca-secret.json`: the root.
2. Else `RAFKA_ISSUING_KEY_HEX` with `RAFKA_ISSUING_CERT_HEX`: no root; the key is its own mesh's
   issuing key. It refuses to start when the cert names another mesh or `RAFKA_CA_PUBKEY` does not
   verify it.
3. Else, with no seeds and no admin anchor: day 0 mints a root.
4. Else, with seeds or an anchor but neither a root nor an issuing key: it refuses to start by
   name.

## How RDM is instantiated

The embedding app gives RDM one `CertSigner` when it starts a node-admin:

```rust
trait CertSigner: Send + Sync {
    /// A member cert for this exact birth, as opaque bytes.
    fn issue_member(&self, birth: &BirthIdentity) -> Result<Vec<u8>, CertRefusal>;
    /// The issuing material a new mesh's first node-admin is launched with.
    fn issue_mesh_issuer(&self, mesh: &MeshName) -> Result<Vec<u8>, CertRefusal>;
}
```

`BirthIdentity` is the node id, incarnation, path.name, mesh and endpoint key. RDM calls the
signer. It never holds or reads key material, and a `CertRefusal` is carried by name.

## When a cert is issued

| Event | Issuer | Carried in |
|---|---|---|
| a node's JoinNode is accepted (the first start inside node.create, and every node.start after a stop, node.restart included) | the accepting authority: the maker for a node-admin, the mesh-primary for an ordinary node | the JoinNode answer, as opaque bytes |
| a new mesh's first node-admin is deployed | the maker, which holds the root or an issuing cert that may mint for that mesh | that admin's launch |

Every JoinNode issues a fresh member cert, so a node that starts again after a stop holds a
new cert, and a new node over an inherited data dir never reuses its predecessor's cert. A refused issuance refuses the join by name; the birth gets no cert and
is not admitted.

## Where the app reads it

The member cert is in [`HydrateContext`](fabric-node-hooks.md#hydrate_before_ready) before
`hydrate_before_ready` runs, and on the running node handle RDM's start returns.

## Time

Cert validity and expiry are judged on rafka-time, and a birth's first JoinNode, judged before it
holds rafka-time, on the local clock named `local-bootstrap`
([origin-ordering.md §rafka-time](/home/admin/rafka-v2/docs/architecture/origin-ordering.md#rafka-time)).

## Inside the mesh

There is no authentication or authorization layer between mesh nodes. A node's identity on
the mesh is its iroh endpoint key. Admission to the mesh is the JoinNode check against the
deployed birth ([node-create.md](lifecycles/node-create.md) step 4). No cert is verified on a
Node RPC call, a gossip frame or a `rafka-mesh-v1` connection (gateway→broker data frames,
snapshot pulls, forwards); a connection between two mesh nodes is trusted because both ends are
members. The member cert issued at JoinNode is Rafka's, for what Rafka presents to clients and
to other systems.
