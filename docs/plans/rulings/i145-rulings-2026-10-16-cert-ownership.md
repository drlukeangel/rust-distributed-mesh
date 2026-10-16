# R-CT1 — the cert chain, its verification at a connection and its renewal are RDM's (Luke 2026-10-16)

**Ruling (Luke 2026-10-16, in conversation, Q1–Q6 of
`docs/plans/proposal/cert-verification-ownership.md`): "the connection like the rest of the mesh should be
in rdm. rafka can pass in a signing cert but the connections and cert management is already handled by
node-admin and should be rdm."**

- **R-SV1's sentence is superseded.** "No cert is verified between mesh nodes" becomes: no cert is checked
  on Node RPC or gossip; the member cert is verified by RDM at the admission of a registered product ALPN
  ([node.connections.admit](../../architecture/lifecycles/node-connections-admit.md)). Iroh `EndpointId`
  stays the transport identity.
- **R-RX1 does not reach the cert check.** Luke: "my previous ruling was based on the fact that the certs
  were in place; by definition this should not cover the cert itself." The belt is the credential R-RX1
  assumed, not a sender gate it removes. The admission step reads no node kind, seat or sender identity:
  a valid cert of this fabric is admitted whoever presents it.
- **e12.s69's delete line is withdrawn.** The SVID belt, Rafka's accept loop and Rafka's node-admin
  minting move to RDM under i145; they are not deleted.
- **Ownership (Q3).** RDM owns the cert format (`rdm_core::certs`), signs and verifies; the app passes in
  the CA material (`CertChoice::Ca` / `Issuer` / `NoCerts`) and nothing else. `CertSigner` goes. The
  accept loop moves to rdm-transport (Q2). Luke's two scopes: "I don't want to build it into gossip or
  rpc, I want to leave this open" and "rdm will supersede rafka; this is the point of the exercise in
  141, 142".
- **Renewal (Q4) is in the epic**, as standard practice recorded in the proposal's Q4 list, not asked
  item by item (Luke: "build in best practices for this, don't make it annoying"). `node.cert.renew` is
  seamless: presented on new connections, terminates nothing. Renewal dates are tracked in a durable cert
  row per birth; the mesh-primary renews its members, the fabric-primary renews mesh-primaries and rotates
  issuers, and watches the fabric's rows.
- **The vocabulary (Q6)**: `fabric.root.create`, `fabric.ca.create`, `fabric.ca.renew`, `fabric.root.update`, `mesh.issuer.create`,
  `mesh.issuer.renew`, `node.issuer.update`, `node.roots.update`, `node.cert.create`, `node.cert.renew`,
  `node.cert.get`, `node.connections.admit`, on
  [fabric-certs.md](../../architecture/fabric-certs.md#the-cert-workflows).
- **Root rotation is in** (Luke: "no deferral, root rotation should be built now"): e5 of i145,
  [fabric.ca.renew](../../architecture/lifecycles/fabric-ca-renew.md). Policy, not per-node: the TTLs and
  the renewal point are fabric policy carried in the entry pull; Rafka passes the root in once and never
  an issuer.
- **Custody (Q7)**: every node-admin holds the one root (Luke: "that's the way it works now"); an
  elected successor mints and rotates from its own copy. One root, never two: the root the app passes
  in is the fabric's root from then on. Two levels above the issuers: the app's root is the anchor and signs only RDM's CA;
  RDM's CA rotates internally on policy (`fabric.ca.renew`, no trust-set change); the anchor rotates only on
  `fabric.root.update { secret }` through an overlap (`node.roots.update`).
- **The page set (Q5+Q6)** was approved as one on 2026-10-16 ("a") and applied the same day.
