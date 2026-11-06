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
| Steps | Mint the issuing cert, then [node.issuer.update](node-issuer-update.md) to the mesh-primary, then to each other admin as a command forwarded through the mesh-primary; the step keys are `mesh.issuer.renew:<mesh>` and `node.issuer.update:<node_id>` |
| Order | The mesh-primary first (the fabric-primary's own mesh: installed in-process, with no call, spanned `via-installed`), then the others; a member renewal due in the window is signed with the new key once its own admin holds it |
| Receipt | Each admin writes its signing cert issue date and expiry on its own record at its install (Q356); the workflow is complete when every admin in the view holds the new dates |
| Never | A member cert re-issued by this workflow (that is [node.cert.renew](node-cert-renew.md)'s on its own schedule), the root rotated (a fabric re-birth), a status set, a frame published |
| Spans | `rdm.node_admin.issuer.create.via-renewal` (mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome) on the fabric-primary, and each `node.issuer.update`'s spans |

## Numbered end-to-end sequence

1. The fabric-primary's tick reads the signing cert dates it holds in its cache, the dates it issued and each admin's `IssuerUpdated` confirmed; a mesh with a
   row whose `renew_at` has passed, and no newer issuing cert held, is renewed. It mints the issuing cert
   for that mesh with the root, on rafka-time, and holds it in process.
2. [node.issuer.update](node-issuer-update.md) to the mesh-primary, then to each other node-admin of the
   mesh as a command forwarded through the mesh-primary ([Forward 0x1A](../node-rpc-envelope.md#forward-op-0x1a-core), the mesh-primary the carrier). An admin not reached is marked by its spent call and sent again once the mark ends.
3. Each admin signs member certs with the new key from its install. [node.cert.renew](node-cert-renew.md)
   runs on its own schedule; a member whose cert would outlive the old issuer is renewed early, cut to
   the issuer's expiry (`MemberOutlivesIssuer`), which is what forces this workflow to run first.
4. Each admin's record holds the dates it answered, written by the admin at its install. The old issuer expires on its own.

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
    Note over A: writes signing_cert on its own record
    A-->>P: node.issuer.updated
    P-->>F: Forwarded(node.issuer.updated)
    Note over P,A: 3. member certs signed with the new key from here
```

## States written

This operation writes no state. Each admin writes the new issuer's dates on its own record (Q356), as `signing_cert` on a `node-record` line in `nodes.jsonl`, at [node.issuer.update](node-issuer-update.md).

| written | by | when |
|---|---|---|
| `signing_cert` | the admin, on its own record | it installed the new issuer |

As stored:

`signing_cert`:

```json
{"len":1458,"specversion":"1.0","id":"01a1ae8c-3ff3-7823-b0d7-f69fa460afdc","source":"rdm://xy07jkt4hj3a/k9sryj6v7e24/p0hkkgfz890n","type":"node-record","subject":"k9sryj6v7e24","time":"2026-11-06T01:49:49.424Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"k9sryj6v7e24","path":"mesh2.node-admin","name":"2","type":"node-admin","incarnation":"p0hkkgfz890n","state":"ready","endpoint_id":"ea2d28c89d7f6a5fe5b8efedd571581d085a192e79eab07466166b188e3f1e18","transport_addr":"127.0.0.1:58183","listeners":[],"runtime":{"deployment_id":"4y7qqdk28h1w","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1509131,"start":65839164}}},"is_primary":null,"is_fabric_primary":null,"cert":{"issued_at":1793929788889,"expires_at":1793929833008,"issuer_expires_at":1793929833008,"renew_at":1793929818779},"signing_cert":{"issued_at":1793929788008,"expires_at":1793929833008,"renew_at":1793929819822},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1508883/i143-an_issuer_update_reaches_a_secondary_admin_through_its_mesh_primary-1508926-0/k9sryj6v7e24"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/rdm-node-admin","sha256":"8cb2122a53f91157b0e89b0cca40fbcb00822a487e44b9a71dc53cc14834ce5f","image":null},"config":{},"tags":[]},"crc32":"d9802d65"}
```

## Failure

An admin that refuses the material by name (another mesh, a CA no root in its set verifies, an issuing
cert the CA does not verify) ends the
workflow `Failed { step: node.issuer.update:<node_id>, reason }`; the next tick runs it again from
the rows. A successor fabric-primary reads the node-admin rows and continues the schedule from them.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
