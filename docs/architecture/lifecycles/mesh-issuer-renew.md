# mesh.issuer.renew

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Fabric certs](../fabric-certs.md) · [node.issuer.update](node-issuer-update.md)

`mesh.issuer.renew` re-mints a mesh's issuing cert before it expires and carries it to that mesh's
node-admins. It is run by the fabric-primary on its own tick, once the issuing cert has lived
`RDM_CERT_RENEW_AT` (default two thirds) of its TTL (`RDM_ISSUING_CERT_TTL_SECS`, default 30 days). It is
not a Build and not a caller op. Member certs issued after it carry the new issuer; the old issuer, and
every member cert it signed, stay valid to their own expiry, so nothing is cut and nothing reconnects.

## Operation contract

| Field | Contract |
|---|---|
| Runner | The fabric-primary, which holds the fabric root |
| Target | One mesh; every node-admin of it, exact births, in the view |
| Steps | Mint the issuing cert, then [node.issuer.update](node-issuer-update.md) to the mesh-primary, then to each other admin as a command forwarded through the mesh-primary; the step keys are `mesh.issuer.renew:<mesh>` and `node.issuer.update:<path.name>` |
| Order | The mesh-primary first (the fabric-primary's own mesh: installed in-process, with no call, spanned `via-installed`), then the others; a member renewal due in the window is signed with the new key once its own admin holds it |
| Receipt | On each admin's `IssuerUpdated` its mesh-primary writes its signing cert issue date and expiry on the admin's record (its own at its install); the workflow is complete when every admin in the view holds the new dates |
| Never | A member cert re-issued by this workflow (that is [node.cert.renew](node-cert-renew.md)'s on its own schedule), the root rotated (a fabric re-birth), a status set, a frame published |
| Spans | `rdm.fabric.issuer.update.via-renewal` (mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome) on the fabric-primary, and each `node.issuer.update`'s spans |

## Numbered end-to-end sequence

1. The fabric-primary's tick reads the signing cert dates it holds in its cache, the dates it issued and each admin's `IssuerUpdated` confirmed; a mesh with a
   row whose `renew_at` has passed, and no newer issuing cert held, is renewed. It mints the issuing cert
   for that mesh with the root, on rafka-time, and holds it in process.
2. [node.issuer.update](node-issuer-update.md) to the mesh-primary, then to each other node-admin of the
   mesh as a command forwarded through the mesh-primary ([Forward 0x1A](../node-rpc-envelope.md#forward-op-0x1a-core), the mesh-primary the carrier). An admin not reached is retried on the next tick until the old issuer expires.
3. Each admin signs member certs with the new key from its install. [node.cert.renew](node-cert-renew.md)
   runs on its own schedule; a member whose cert would outlive the old issuer is renewed early, cut to
   the issuer's expiry (`MemberOutlivesIssuer`), which is what forces this workflow to run first.
4. Each admin's record holds the dates it answered, written by its mesh-primary. The old issuer expires on its own.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant F as Fabric-primary
    participant P as Mesh-primary (admin)
    participant A as Other node-admins
    Note over F: 1. tick: a node-admin row's signing cert past renew_at; mint
    F->>P: 2. node.issuer.update
    P-->>F: node.issuer.updated
    F->>P: 2. Forward(node.issuer.update → each other admin)
    P->>A: node.issuer.update
    A-->>P: node.issuer.updated
    P-->>F: Forwarded(node.issuer.updated)
    Note over P,A: 3. member certs signed with the new key from here
```

## Failure and receipts

An admin that refuses the material by name (another mesh, a CA no root in its set verifies, an issuing
cert the CA does not verify) ends the
workflow `Failed { step: node.issuer.update:<path.name>, reason }`; the next tick runs it again from
the rows. A successor fabric-primary reads the node-admin rows and continues the schedule from them.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
