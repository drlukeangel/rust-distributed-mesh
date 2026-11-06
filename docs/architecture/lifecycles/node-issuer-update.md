# node.issuer.update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [Fabric certs](../fabric-certs.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

A primitive. `node.issuer.update` installs a mesh's issuing material (the issuing key and its cert,
signed by the fabric CA) on one running node-admin of that mesh. It is run by the fabric-primary,
one admin at a time, inside [mesh.issuer.renew](mesh-issuer-renew.md): to the mesh-primary directly, and
to each other node-admin of the mesh as a command forwarded through the mesh-primary ([Forward 0x1A](../node-rpc-envelope.md#forward-op-0x1a-core), the mesh-primary the carrier); commands go down
([fabric primary](../fabric-primary.md#11-identity-creation-order)). It is a single call, not a
Build. From the install the admin signs member certs with the new key; certs it issued before stay
valid to their own expiry.

## Operation contract

| Field | Contract |
|---|---|
| Caller | The fabric-primary: to the mesh-primary directly, to every other node-admin of the mesh as a command forwarded through the mesh-primary (Forward 0x1A) |
| Target | One node-admin, by exact `(node.id, incarnation)`, of the mesh the issuing cert names; material for another mesh or another birth is refused by name |
| Request | `UpdateIssuer { node_id, incarnation, key, cert, ca_key }`: the issuing material and the key of the CA it is minted under (the issuing cert carries that CA's cert), installed together as the launch writes them (`RDM_ISSUING_KEY` beside `RDM_FABRIC_CA_KEY`) on [Status 0x1B](../node-rpc-envelope.md#status-op-0x1b-rdm) |
| Reply | `IssuerUpdated { node_id, incarnation, issuer_expires_at }`, the receipt; an admin that cannot verify the issuing cert to a root it holds (the issuing cert against the CA, the CA against the root) answers a named refusal and keeps what it holds; a CA older than the one it holds is refused `ca-older` and nothing is installed |
| Never | A member cert re-issued by this call, a connection closed, a status set, a frame published |
| Spans | `rdm.node_admin.issuer.update.via-renewal` (node, node_id, incarnation_id, mesh, issuer_expires_at, previous_issuer_expires_at, outcome) on the fabric-primary; `rdm.node_admin.issuer.update.via-installed` (node, node_id, mesh, sender, issuer_expires_at, previous_issuer_expires_at) on the admin, `sender` the node the call reached it from: the fabric-primary, or the mesh-primary carrying it |

## Numbered end-to-end sequence

1. The fabric-primary holds the mesh's new issuing material ([mesh.issuer.renew](mesh-issuer-renew.md) step 1).
2. It sends `UpdateIssuer` to the mesh-primary's held address, and to each other admin as a Forward 0x1A
   through the mesh-primary, once the mesh-primary's node row holds the new dates; the carrier makes the one inner
   call and returns its reply. `NotSent` or `Indeterminate` is spanned;
   the spent call marks the admin, and it is sent nothing while the mark holds.
3. The admin checks the material names its own mesh and its own `(node.id, incarnation)` and verifies
   the issuing cert to a root it holds (the issuing cert against the CA, the CA against the root),
   installs it, and answers `IssuerUpdated`.
4. The admin writes the signing cert's issue date and expiry on its own record at its install (Q356).

## Command and completion messages

```text
node.issuer.update { node_id, incarnation, key, cert }
  ← node.issuer.updated { node_id, incarnation, issuer_expires_at }
```

## States written

This operation writes no state. The admin writes the signing cert's dates on its own record (Q356), as `signing_cert` on a `node-record` line in `nodes.jsonl`.

| written | by | when |
|---|---|---|
| `signing_cert` | the admin, on its own record | it installed the issuer |

As stored:

`signing_cert`:

```json
{"len":1458,"specversion":"1.0","id":"01a1ae8c-3ff3-7823-b0d7-f69fa460afdc","source":"rdm://xy07jkt4hj3a/k9sryj6v7e24/p0hkkgfz890n","type":"node-record","subject":"k9sryj6v7e24","time":"2026-11-06T01:49:49.424Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"k9sryj6v7e24","path":"mesh2.node-admin","name":"2","type":"node-admin","incarnation":"p0hkkgfz890n","state":"ready","endpoint_id":"ea2d28c89d7f6a5fe5b8efedd571581d085a192e79eab07466166b188e3f1e18","transport_addr":"127.0.0.1:58183","listeners":[],"runtime":{"deployment_id":"4y7qqdk28h1w","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1509131,"start":65839164}}},"is_primary":null,"is_fabric_primary":null,"cert":{"issued_at":1793929788889,"expires_at":1793929833008,"issuer_expires_at":1793929833008,"renew_at":1793929818779},"signing_cert":{"issued_at":1793929788008,"expires_at":1793929833008,"renew_at":1793929819822},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1508883/i143-an_issuer_update_reaches_a_secondary_admin_through_its_mesh_primary-1508926-0/k9sryj6v7e24"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/rdm-node-admin","sha256":"8cb2122a53f91157b0e89b0cca40fbcb00822a487e44b9a71dc53cc14834ce5f","image":null},"config":{},"tags":[]},"crc32":"d9802d65"}
```

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty). A launched admin
that was started with the material in its environment (`RDM_ISSUING_KEY`, `RDM_ISSUING_CERT`) needs no
call until the next renewal.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
