# Who owns the member cert's verification: RDM's connection, or Rafka's data plane

**Proposal, 2026-10-16.** Read-only; nothing here is changed. Every page line is quoted verbatim with its
line number on RDM `origin/main` (3110984e) and rafka-v2 `main` (5845639d09). One question is asked at a
time, recommendation first, arms ending in "x: something else".

**Scope.** This is its own epic, not an i144 item. It is cross-repo (rafka-node-base, rafka's
`node-admin/`, rdm-transport, rdm-admin), it is a mesh change (connection admission is part of a
connection's life), so it lands only on Luke's yes with a Jaeger flame, and it changes a standing ruling's
text either way (§2).

**Two heads, two node-admins.** rafka main pins RDM at `5532aabd` (2026-10-07, Cargo.toml:26-31, the
pre-rename crate names), eight days before RDM's `CertSigner` existed (certs.rs born 6a8ba1c1,
2026-10-15). rafka-v2 builds its own binary named `rafka-node-admin` (`node-admin/`, a workspace member);
RDM builds another with the same name (`rdm-admin/src/bin/rafka_node_admin.rs`). The "today" in §1 is
therefore two binaries that do not run together: Rafka's lanes run Rafka's node-admin; RDM's gate runs
RDM's. Directories read: rafka-v2 `crates/`, `node-admin/`, `scripts/`, `docs/`; RDM `crates/`, `docs/`.

**What this proposal does not touch.** Node RPC and gossip. Whatever is decided here, no cert is checked
on a Node RPC call or a gossip frame. The admission step, if RDM owns it, runs only on the ALPNs a
product registers it for; RDM never registers it for its own two.

## 1. What is true today

### 1.1 The chain, and the one place it is checked

Three levels (fabric-certs.md:15-19): a fabric root, a per-mesh issuing cert, a member cert per birth.
The member cert is checked in exactly one place: rafka-node-base's `rafka-mesh-v1` accept loop, the
"SVID belt". A dialer writes its cert hex on a bi-stream (`dialer_send_svid`, lib.rs:2817); the acceptor
reads it, verifies signature and expiry against `RAFKA_CA_PUBKEY` on the judgement clock, writes an
`Admission` verdict back, and drops the connection on refusal (`acceptor_verify_svid`, lib.rs:3056;
the call site in `start_accept_loop`, lib.rs:10823-10826). Three modes (`cert_belt_mode`, lib.rs:2724):
`Enforce(ca)` when `RAFKA_CA_PUBKEY` parses; `InsecureDevBypass` only on an explicit `RAFKA_NO_CERT`;
`RefuseNoCaConfigured` otherwise, which admits no one. An empty or undecodable cert is refused
`cert_decode_failed` (lib.rs:3128-3137).

That loop also multiplexes the Node RPC and gossip ALPNs (lib.rs:10785-10807): `rafka_node_rpc::ALPN`
is handed to RDM's server, `iroh_gossip::ALPN` to gossip, and the default arm is `rafka-mesh-v1`, where
the belt runs. The belt is in rafka-node-base because the accept loop is.

### 1.2 Issuance: two node-admins, two mechanisms

**Rafka's node-admin mints, outside RDM.** `node-admin/src/lib.rs:5806 resolve_boot_authority` reads
`RAFKA_CA_SECRET_HEX` or `ca-secret.json`, else an issuing key and cert, else mints a root on day 0 (the
order fabric-certs.md:36-47 states). `node-admin/src/main.rs:4713 issue_member_cert` mints a member cert
with a one-day TTL (`86_400`, main.rs:5619, :12761) for every child it spawns, strips any inherited
`RAFKA_NODE_CERT` and sets the child's own (main.rs:5593-5624, :12761-12763). The child reads it
(`own_cert_hex`, lib.rs:2692-2694) and presents it on every `rafka-mesh-v1` dial; the belt verifies it
against `RAFKA_CA_PUBKEY`. The lane deploy passes both the CA secret and the public key to Rafka's
node-admin (`scripts/lane-mesh-deploy.sh:1262-1265`), so a Rafka lane runs the belt in `Enforce` with
real certs. The chain is live there. Rafka's node-admin implements no `CertSigner`: it predates the trait
and mints on its own; the cert travels by env at spawn, not on the join answer.

**RDM's node-admin issues through the trait, and nothing of Rafka's plugs into it.** `CertSigner`
(rdm-admin/src/certs.rs:29-36), `issue_member(birth, now_ms)` called before a JoinNode is admitted, the
bytes carried opaque in `JoinControl::member_cert` into `HydrateCtx`; a `CertRefusal` refuses the join by
name; `NoCerts` by explicit choice; an unchosen `CertChoice` is refused at start. RDM never parses,
verifies or stores a cert (certs.rs:3-6). The only implementations are the test kit's `TestCerts` and
`NoCerts`; no rafka-v2 code implements the trait. RDM's binary is run by RDM's gate; its member certs are
test certs, and there is no belt on RDM's side to check them.

**So the page describes a merge that exists in neither binary.** fabric-certs.md says Rafka instantiates
RDM with a signer and RDM issues at JoinNode (:9-11, :65-75): that is RDM's node-admin with a signer Rafka
has not written. It also says the signing authority is resolved by `node-admin/src/lib.rs` and verified by
`rafka-node-transport` (:27-47): that is Rafka's node-admin, which does not use RDM's trait. The merge is
the pending adoption of RDM's node-admin by Rafka. Whichever arm Q1 takes, that adoption is where the
signer (and on A, the verifier) gets written.

Correction to the lead's statements to Luke on 2026-10-16: "the certs guard every data-plane link" is true
of a Rafka lane (Rafka's node-admin, `Enforce`), and false of RDM's gate (RDM's node-admin, test certs, no
belt). The lead's first answer, "they are being used", stands for Rafka; the later "nothing mints" was a
grep confined to `crates/` and is withdrawn.

### 1.3 The pages, verbatim

fabric-certs.md:89-95:

> There is no authentication or authorization layer between mesh nodes. A node's identity on
> the mesh is its iroh endpoint key. Admission to the mesh is the JoinNode check against the
> deployed birth ([node-create.md](lifecycles/node-create.md) step 4). No cert is verified on a
> Node RPC call, a gossip frame or a `rafka-mesh-v1` connection (gateway→broker data frames,
> snapshot pulls, forwards); a connection between two mesh nodes is trusted because both ends are
> members. The member cert issued at JoinNode is Rafka's, for what Rafka presents to clients and
> to other systems.

The code contradicts the third sentence (§1.1) and the page's §"Where a node-admin's signing authority
comes from" (:36-47) cites code that is gone (§1.2).

rafka-v2 internal-mesh-auth.md, the table: "Gateway → broker, connection | NONE | — | —", and the
paragraph above it: "A `rafka-mesh-v1` connection between mesh nodes carries no credential either: both
ends are mesh members". Same contradiction.

rafka-v2 data-plane-transport.md:148: "`NodeCert`, `SignedCert` (`cert.rs`); `verify_node_cert` (`lib.rs`)
| gateway peer selection". That row records the belt as live.

## 2. What Luke has already ruled

Three standing statements, all Luke's, all say the belt goes:

- **R-SV1** (i141 PRD line 61): "No cert is verified between mesh nodes (R-SV1); the member cert is
  Rafka's for clients and other systems. Iroh `EndpointId` remains transport identity."
- **R-RX1** (rulings 2026-10-12): "Every place a receiver (gossip receiver, Node RPC server or door,
  status/declaration/join/start/stop handler) refuses, ignores or fences an input because of WHO sent it
  is removed." A belt that drops a dial for lacking a valid cert refuses an input because of who sent it,
  as that text reads.
- **e12.s69** (i143-prd.md:1760-1768, not started, absorbed into s70): "delete `dialer_send_svid` /
  `acceptor_verify_svid` and the admission verdict, `connect_verified` becomes a plain connect";
  criterion e12.s69.2 "no SVID frame on rafka-mesh-v1".

Luke's direction of 2026-10-16, in conversation: "i don't want to build it into gossip or rpc i want to
leave this open. my gut is telling me this should not live in rafka and the connection like the rest of
the mesh should be in rdm. rafka can pass in a signing cert but the connections and cert management is
already handled by node-admin and should be rdm."

Those two are not the same decision. The first question is which one stands.

## 3. Questions

### Q1. Does e12.s69 stand (delete the belt), or is it superseded (RDM owns connection admission)?

**Ruled A (Luke, 2026-10-16: "the connection like the rest of the mesh should be in rdm. rafka can pass
in a signing cert but the connections and cert management is already handled by node-admin and should be
rdm"; confirmed "wasn't I the one who just said it should own this"). R-SV1's sentence, e12.s69's delete
line and fabric-certs.md:89-95 are superseded; their replacement text is Q5's.**

**Recommendation was A.** A yes to A supersedes R-SV1's sentence as written, e12.s69's delete line, and
the fabric-certs.md paragraph in §1.3; their replacement text comes to Luke as exact old → new (Q5).
R-RX1 does not reach the cert check: Luke, 2026-10-16, "my previous ruling was based on the fact that the
certs were in place; by definition this should not cover the cert itself." The belt is the credential
R-RX1 assumed, not a sender gate it removes.

- **A (recommended): superseded.** RDM owns connection admission as a step in a connection's life, on
  the ALPNs a product registers for it; Rafka supplies a signer and a verifier and never runs a gate of
  its own. Why:
  - RDM already owns the connection: the endpoint and its ALPNs (rdm-transport/src/endpoint.rs:30-33),
    the Direct/Proxy records and reconnect (connections.md §9-§10), `node.connections.*`. Admit-or-drop
    on accept is one more transition of the same object, with its own span and connections-log fact.
  - RDM already owns the other half of the credential's life: when it is issued, to which exact birth,
    fresh per start, refused issuance refuses the join (fabric-certs.md:65-75). One owner for both halves
    ends the split that produced the contradiction in §1.3.
  - The pattern exists: `CertSigner` is Rafka's code run at RDM's moment. A `CertVerifier` beside it is
    the same shape: opaque bytes in, a verdict out, RDM reads nothing.
  - Node RPC and gossip stay check-free by construction: RDM never registers the step for its own ALPNs.
    The no-authorization rule is untouched; this is admission of a connection, never of an op.
- **B: e12.s69 stands.** Delete the belt and the SVID frame; `connect_verified` becomes a plain connect;
  the member cert is Rafka's for clients and other systems only; the pages in §1.3 are corrected to say
  the chain's only use is outward (and the stale boot-authority section is deleted). Smaller, and it is
  what three standing rulings say. It leaves the data plane with key-possession plus JoinNode as its
  whole admission, which is what R-SV1 chose.
- x: something else.

### Q2 (only on A). Where does the admission step run?

The fact: `rafka-node-base::start_accept_loop` (lib.rs:10748) owns the endpoint's accept loop and
multiplexes the three ALPNs; RDM's Node RPC server is called from it (lib.rs:10793). The belt sits in
that loop's `rafka-mesh-v1` arm.

- **A: the loop moves to rdm-transport.** RDM accepts every connection on its endpoint, dispatches by
  ALPN, runs the registered admission step for product ALPNs, then hands an admitted connection to the
  product. rafka-node-base stops accepting. This is the shape "the connection, like the rest of the
  mesh, is RDM's"; it is also the larger move (the gossip and Node RPC arms come with it).
- **B: the step is an RDM hook rafka-node-base calls.** The loop stays where it is; its `rafka-mesh-v1`
  arm calls `rdm_transport::admission::admit(conn, &verifier)` and drops on refusal. Smaller; the loop's
  ownership stays split.
- x: something else.

**Ruled A (Luke, 2026-10-16: "rdm will supersede rafka; this is the point of the exercise in 141, 142:
absorbing rdm into it. so yes A").** The accept loop moves to rdm-transport; Rafka's accept loop goes with
the rest of the superseded Rafka transport. Recommendation was A.

### Q3. The delta

**Ruled A (Luke, 2026-10-16), with item 1 revised on his question "shouldn't the verify be the node's
job, not in Rafka?": RDM owns the cert format, signs and verifies; Rafka passes in the CA material and
nothing else. `CertSigner` and the proposed `CertVerifier` trait both go.**

1. The cert format (`NodeCert`, `IssuingCert`, `SignedCert`, the Ed25519 sign and verify, today in
   Rafka's `rafka-node-transport/src/cert.rs`) moves into `rdm_core::certs`. RDM mints the mesh issuing
   cert, signs every member cert and verifies every presented one. `CertChoice` becomes `Ca { secret }`
   (the fabric root, for the bootstrap admin), `Issuer { key, cert }` (a mesh's issuing material, for a
   launched admin), or `NoCerts` by name (admits without a credential, with its span). Rafka passes the
   root in; it never signs or verifies anything. The cert is a mesh credential in RDM vocabulary: the
   exact birth, its mesh, its expiry, the issuer's signature.
2. The credential exchange (one bi-stream: the dialer's cert, the acceptor's verdict) moves to
   rdm-transport. The 30 s read bound and its reason (lib.rs:3061-3069) come with it.
3. Admission is a connection transition: recorded in the connections log with `source`, `destination`,
   `kind`, `state`, `reason` (connections.md §12), spanned `rdm.node.connections.create.via-admitted` /
   `rdm.node.connections.reject.via-refused` with the named refusal (`bad-signature`, `expired`,
   `issuer-mesh-mismatch`, `no-credential`). Rafka's `rafka.mesh.auth.*` spans retire with its loop.
4. Exclusion by construction: the step is registered per ALPN at `bind`; `rdm_transport::ALPN` and
   `iroh_gossip::ALPN` cannot be registered (refused by name at bind).
5. Judgement time is rafka-time once held, else `local-bootstrap`, the rule fabric-certs.md:83-85 already
   states.

What Rafka keeps: the CA material it passes in (today `RAFKA_CA_SECRET_HEX` / `ca-secret.json`), and
what the cert means outward, to clients and other systems. What Rafka loses: its node-admin's
`resolve_boot_authority` and `issue_member_cert`, env-at-spawn delivery (`RAFKA_NODE_CERT`), its accept
loop, the belt and its three modes (`RAFKA_NO_CERT`, `RAFKA_CA_PUBKEY` become unread).

### Q4. The gaps RDM then owns

**Ruled A (Luke, 2026-10-16): renewal is in this epic. "Build in best practices for this, don't make
it annoying": the mechanics below are standard practice, recorded here to be objected to, not asked.**

- **Renewal.** A member cert lives one day in Rafka's node-admin (main.rs:5619, :12761) and nothing
  re-issues it; a node up longer than its TTL presents an expired cert on its next new data-plane dial and
  is refused (`Expired`), while its pooled connections stay. A restart mints fresh, so today's lanes are
  shorter than the gap. RDM re-issues before expiry and carries the new bytes to the running node. The
  design, standard practice throughout:
  1. **Who and when.** The birth's issuing authority (its mesh-primary for a member, the fabric-primary
     for a mesh-primary) renews on its own tick, never the node: the node holds no key and asks for
     nothing. Renewal fires once the cert has lived two thirds of its TTL (`RDM_CERT_RENEW_AT = 0.67`),
     with a jitter of up to 5 % of the TTL so a mesh born together does not renew together.
  2. **`node.cert.renew`, a primitive** (Luke, 2026-10-16: "you need to add a node.cert.renew workflow
     which it will use to present to all new connections, not terminate existing ones; it should be a
     seamless update"). One call from the issuing authority to the exact birth `(node.id, incarnation)`,
     carrying the new cert; the node installs it and answers `node.cert.renewed`, the receipt. A cert for
     another birth is refused by name. From the install, the node presents the new cert on every
     connection it opens; nothing is terminated, nothing reconnects, no status changes, no gossip frame:
     a renewal is invisible to every other node. Its page is `lifecycles/node-cert-renew.md`, with its
     rows in the index's workflow and wire-op tables (Q5 item 15). The same primitive carries a re-minted
     issuing cert to a mesh's node-admins.
  3. **Overlap.** The old cert stays valid until its own expiry; a dial in flight during the swap is
     admitted on either. No revocation list: expiry is the revocation, which is why the TTL is short.
  4. **TTLs.** Member cert 24 h (`RDM_MEMBER_CERT_TTL_SECS = 86400`, Rafka's value kept); mesh issuing
     cert 30 days (`RDM_ISSUING_CERT_TTL_SECS`, cert.rs:40 kept); a member cert never outlives its issuer
     (`MemberOutlivesIssuer`, the rule fabric-certs.md:23 already has: a member renewed near the issuer's
     expiry is cut to the issuer's expiry, which forces the issuer's rotation first).
  5. **Issuer rotation.** The fabric-primary re-mints a mesh's issuing cert at two thirds of its TTL and
     carries it to that mesh's node-admins the same way (the fabric-level config push); member certs
     issued after that carry the new issuer, old ones stay valid to their expiry. The fabric root does not
     rotate inside the mesh: it is the material Rafka passes in, and replacing it is a fabric re-birth.
  6. **Clock.** Issue and expiry on rafka-time; a presented cert is judged with a skew allowance of one
     staleness floor (`RDM_STALENESS_MS`) on either side, so a node a tick behind is not refused at the
     boundary.
  7. **Failure.** A renewal whose push is not admitted (the node unreachable, the call `NotSent` or
     `Indeterminate`) is retried on every tick until the old cert expires, each attempt spanned; it never
     marks the node, which is recovery's business. A node whose cert did expire is refused on its next new
     dial, by name, until the next renewal reaches it; its existing connections are not cut.
  8. **Evidence.** `rdm.node_admin.cert.update.via-renewal` (node, node_id, incarnation_id, issued_at,
     expires_at, previous_expires_at, outcome: `installed` / `not-sent` / `refused`) on the issuer;
     `rdm.node.cert.update.via-installed` on the node. The cell: a member whose TTL is set to seconds in
     the test kit dials past its first expiry and is admitted, with both spans and the admission fact.
  9. **Tracking (Luke, 2026-10-16: "we will need to track the cert renewal dates").** The issuer writes
     a cert row for the exact birth at every issue and renew: `NodeCertRow { node_id, name, incarnation,
     issued_at, expires_at, issuer_expires_at, renew_at }`, a status-row kind beside `held-running` and
     `os-stopped`, keyed by the birth, with its own format marker (`node-cert/1`; the landing carries the
     marker). `node.get` surfaces it (Q6 `node.cert.get`). A successor mesh-primary folds the rows and
     continues the schedule from them; nothing about renewal lives only in memory. The tick renews every
     row whose `renew_at` has passed.
  10. **Who renews, who watches.** The mesh-primary renews its own members on the tick it already runs
     every gossip interval: it holds the mesh's issuing key, and commands go down. The fabric-primary
     renews mesh-primaries' certs and rotates each mesh's issuing cert, one level up, the same way. Not the
     fabric-primary for every node: it holds the root and no mesh's issuing key by design, and a tick over
     every node of the fabric is the wrong level. The fabric-primary tracks it: every cert row is in the
     fabric-level projection, `fabric.get` shows the next expiry across the fabric, and a mesh whose
     renewals stall (a row past `renew_at` by more than a floor with no `node.cert.renewed`) raises
     `Concern`, as a held fabric-primary does today.
  11. **Policy, not per-node (Luke, 2026-10-16: "should we make cert rotation time issuer/node a
     configuration?").** The TTLs and the renewal point are fabric policy: read once by the bootstrap
     admin, fixed for the fabric, carried to every later admin in its entry pull like `MESH_SPAWN_TYPE`,
     so no two admins disagree on when a cert is due. `RDM_MEMBER_CERT_TTL_SECS`,
     `RDM_ISSUING_CERT_TTL_SECS`, `RDM_CERT_RENEW_AT`; the jitter is fixed. Rafka passes in the root
     once and never an issuer ("do we need to supply Rafka the ability to pass in a new issuer cert?":
     no); RDM mints every issuing cert from the root. Root rotation is in (Luke, 2026-10-16: "no deferral, root rotation should be built now"):
     [fabric.ca.renew](../../architecture/lifecycles/fabric-ca-renew.md), a root set trusted through an
     overlap, `node.roots.update` on every node, issuers re-minted, the old root retired once nothing
     chains to it; custody across a seat move on fabric-certs.md §Custody.
  12. **Bootstrap.** Day 0 is unchanged: the bootstrap admin mints the root (or is given it), the first
     issuing cert, and its own member cert; every JoinNode still issues fresh (fabric-certs.md:72).
- **Delivery.** Rafka's node-admin hands the cert by env at spawn; RDM's by the JoinNode answer
  (fabric-certs.md:65-75). The JoinNode answer stands; renewal uses the same carrier the Node RPC already
  has for a config update.
- **Env.** `RDM_CA_SECRET` (or a file beside the data dir) for the bootstrap admin; the issuing material
  for a launched admin is written by node-admin at launch like `RDM_MESH_ID` is. `RAFKA_NO_CERT` has no
  successor: a fabric runs with certs or is started `NoCerts` by name in its wiring.

### Q5. The page lines, exact old → new (one set)

**Ruled A with Q6 (Luke, 2026-10-16: "a"): the set is applied, the four pages written, the epic recorded in its PRD.**

**RDM `docs/architecture/fabric-certs.md`**

1. `:5-7`. Old: "This page owns the cert chain and its issuance: who holds each level, where a node-admin's
   signing authority comes from, when a cert is issued and to which exact birth, and where the app reads
   it." New: "This page owns the cert chain, its issuance, its renewal and its verification at a
   connection: who holds each level, where a node-admin's signing material comes from, when a cert is
   issued or renewed and to which exact birth, where the app reads it, and on which connections it is
   checked."
2. `:9-11`. Old: "Rafka instantiates RDM with its signing capability. RDM decides when a cert is issued and
   to which exact birth, and carries the cert to that birth. Rafka owns the cert format, the keys and every
   use of the cert. RDM never parses or verifies a cert." New: "The embedding app starts the bootstrap
   node-admin with the fabric's CA material (`CertChoice::Ca`), or a launched node-admin with its mesh's
   issuing material (`CertChoice::Issuer`), or neither by name (`NoCerts`). RDM owns the cert format
   (`rdm_core::certs`), mints the issuing certs, signs every member cert, renews it before expiry, carries
   it to the exact birth, and verifies the cert a dialer presents on every connection of a registered
   product ALPN. The app owns the CA material it passes in and what the cert means outward, to clients and
   other systems."
3. `:36-47` (§"Where a node-admin's signing authority comes from"). Old: the whole section (it cites
   `node-admin/src/lib.rs::resolve_boot_authority` and `RAFKA_*` env, Rafka's own node-admin). New:
   "## Where a node-admin's signing material comes from\n\nThe bootstrap admin is started with the fabric
   root (`CertChoice::Ca { secret }`: `RDM_CA_SECRET`, else `ca-secret` beside its data dir, else minted
   on day 0 when it has no seeds and no anchor). A launched admin is started with its mesh's issuing
   material, written by the launching admin into its environment at launch (`RDM_ISSUING_KEY`,
   `RDM_ISSUING_CERT`), the way `RDM_MESH_ID` is. An admin with seeds or an anchor and neither refuses to
   start by name. The root never leaves the fabric-primary; a mesh's admin signs only members of its own
   mesh (`NoRootToMint`) and never a member cert that outlives its issuer (`MemberOutlivesIssuer`)."
4. `:65-75` (§"When a cert is issued"), a row appended to the table: "| a member cert has lived two thirds
   of its TTL (`RDM_CERT_RENEW_AT`, with up to 5 % jitter) | the birth's issuing authority (its mesh-primary;
   the fabric-primary for a mesh-primary) | [node.cert.renew](lifecycles/node-cert-renew.md) to the
   running birth; the old cert stays valid to its own expiry |" and the sentence after the table gains: "A mesh's issuing
   cert is re-minted by the fabric-primary at two thirds of its TTL and carried to that mesh's node-admins
   the same way; members issued after that carry the new issuer."
5. `:83-85` (§Time), appended: "A presented cert is judged with a skew allowance of one staleness floor
   (`RDM_STALENESS_MS`) on either side of its validity."
6. `:87-95` (§"Inside the mesh"). Old: the paragraph quoted in §1.3 above. New: "## On a connection\n\n
   No cert is checked on a Node RPC call or a gossip frame: inside RDM's control plane a node's identity
   is its iroh endpoint key, and admission to the mesh is the JoinNode check against the deployed birth
   ([node-create.md](lifecycles/node-create.md) step 4). On every inbound connection of an ALPN the
   embedding app registered at `bind`, the acceptor reads the dialer's member cert on the connection's
   first bi-stream, verifies it (signature to the issuer, the issuer to the root, the issuer's mesh, both
   expiries), writes the verdict back, and admits or drops the connection; the verdict is a connection
   fact ([connections.md](connections.md) §1.2, §12). RDM never registers the step for its own two ALPNs.
   The member cert is also what the app presents outward, to clients and other systems."

**RDM `docs/architecture/connections.md`**

7. `:96-100` (§1.2, the durable state block). Old: "Connected\nDisconnected\nFailed". New: "Connected\n
   Disconnected\nFailed\nRefused". And after :111, appended: "`Refused` is written by an acceptor for an
   inbound connection it dropped at admission ([fabric-certs.md](fabric-certs.md#on-a-connection)),
   naming the reason (`no-credential`, `bad-signature`, `expired`, `issuer-mesh-mismatch`). It is the
   acceptor's fact about one dial, never proof about the dialer." Mapping line added at :104-108:
   "Refused      -> state=refused". (Durable format: a new state value, the landing carries the marker.)
8. `:614-620` (§12), the field list gains nothing; a sentence appended after :620: "An admission verdict
   is spanned `rdm.node.connections.create.via-admitted` or `rdm.node.connections.reject.via-refused`
   with the named reason, on the acceptor."

**RDM `docs/architecture/fabric-mesh-node-lifecycles.md`**

9. `:19`. Old: "| [Fabric certs](fabric-certs.md) | the cert chain and issuance |" New: "| [Fabric
   certs](fabric-certs.md) | the cert chain, issuance, renewal and verification at a connection |"

**rafka-v2 `docs/architecture/internal-mesh-auth.md`**

10. `:12-14`. Old: "A `rafka-mesh-v1` connection between mesh nodes carries no credential either: both
    ends are mesh members ([`fabric-certs.md`](…#inside-the-mesh))." New: "A `rafka-mesh-v1` connection
    between mesh nodes carries the dialer's member cert, issued and verified by RDM at admission
    ([`fabric-certs.md`](…#on-a-connection)); nothing on the frames is re-verified."
11. `:20`. Old: "| Gateway → broker, connection | NONE | — | — |" New: "| Gateway → broker, connection |
    the dialer's RDM member cert | RDM, at the acceptor's admission step | the connection: dropped on
    refusal |"

**rafka-v2 `docs/architecture/data-plane-transport.md`**

12. `:148`. Old: "| `NodeCert`, `SignedCert` (`cert.rs`); `verify_node_cert` (`lib.rs`) | gateway peer
    selection (…) |" New: the row deleted (the types and the verification are RDM's, `rdm_core::certs`;
    the right-hand consumer keeps its own row if it has other transport items, else the row goes).

**The record and the rulings**

13. `docs/plans/i143-prd.md:1760-1768` (e12.s69): the "delete `dialer_send_svid` / `acceptor_verify_svid`
    and the admission verdict, `connect_verified` becomes a plain connect" line is replaced by "the SVID
    belt, the accept loop and Rafka's node-admin minting move to RDM under the cert epic
    (`docs/plans/proposal/cert-verification-ownership.md`); criterion e12.s69.2 becomes `no
    rafka.mesh.auth.* span; the admission fact and span are RDM's`".
14. A ruling entry (`docs/plans/rulings/`, dated 2026-10-16): R-SV1's sentence "No cert is verified
    between mesh nodes" is superseded by "no cert is checked on Node RPC or gossip; the member cert is
    verified by RDM at the admission of a registered product ALPN"; R-RX1 is recorded as ruled on the
    assumption that the certs stood and as not reaching the cert check.

**New pages and their index rows** (superseded by Q6's table; kept for the renew page's shape)

15. `docs/architecture/lifecycles/node-cert-renew.md`, a new primitive page in the shape of
    [node.config.update](lifecycles/node-config-update.md): caller the birth's issuing authority; target
    the exact `(node.id, incarnation)`; request `node.cert.renew { cert }`; reply `node.cert.renewed`
    (the receipt), or a named refusal (another birth, a cert the node cannot decode); what it never does
    (terminate a connection, change a status, publish a frame); failure (retried on the authority's next
    tick until the old cert expires; marks nothing); spans `rdm.node_admin.cert.update.via-renewal` on
    the authority and `rdm.node.cert.update.via-installed` on the node. The index
    (`fabric-mesh-node-lifecycles.md`) gains its row in the page table, the workflow table and the
    wire-op → workflow table; the envelope's typed-object table gains the op's row.

### Q6. The cert vocabulary: every workflow and command, named (Luke, 2026-10-16: "and all the cert
workflows / commands actually")

One set, in the ops vocabulary (`<entity>.<thing>.<verb>`, past tense for the event). Each row is a page
in `lifecycles/` or a row on an existing page, as the last column says. No revocation exists by design:
expiry is the revocation, which is why the member TTL is a day. The fabric root never rotates inside the
mesh: replacing it is a fabric re-birth.

| Workflow / command | Kind | Caller → target | Wire | What it does | Page |
|---|---|---|---|---|---|
| `fabric.ca.create` | step inside [fabric.create](../../architecture/lifecycles/fabric-create.md) | the bootstrap admin, day 0 | none (local) | takes the root from `CertChoice::Ca` (`RDM_CA_SECRET`, else `ca-secret` beside the data dir) or mints one when it has no seeds and no anchor; the root never leaves the fabric-primary | a numbered step on fabric-create.md; fabric-certs.md §material |
| `mesh.issuer.create` | step inside [mesh.create](../../architecture/lifecycles/mesh-create.md) | the fabric-primary, at the mesh's acceptance | none (carried in the first admin's launch env, `RDM_ISSUING_KEY` / `RDM_ISSUING_CERT`) | mints the mesh's issuing cert (`IssuingCert { mesh, issuer_pub, expiry }`, 30 d) signed by the root | a numbered step on mesh-create.md; fabric-certs.md §chain |
| `mesh.issuer.renew` | workflow, on the fabric-primary's tick | the fabric-primary → every node-admin of the mesh | `node.issuer.update` per admin | re-mints the issuing cert at two thirds of its TTL; members issued after it carry the new issuer; the old issuer stays valid to its expiry | `lifecycles/mesh-issuer-renew.md` (new) |
| `node.issuer.update` | primitive | the fabric-primary → one node-admin, exact birth | Node RPC object call; reply `node.issuer.updated` | installs a mesh's issuing material on a running node-admin; refused by name for another mesh or another birth | `lifecycles/node-issuer-update.md` (new) |
| `node.cert.create` | step inside [node.mesh.join](../../architecture/lifecycles/node-mesh-join.md) | the accepting authority, before it admits the JoinNode | none (carried in the join answer, `JoinControl::member_cert`) | signs the member cert for the exact birth (`NodeCert { name, mesh, node_id, incarnation, endpoint, expiry }`, 24 h); a refused issuance refuses the join by name | a numbered step on node-mesh-join.md (today's step 5, named); fabric-certs.md §issued |
| `node.cert.renew` | primitive, on the issuing authority's tick | mesh-primary → member; fabric-primary → mesh-primary; exact birth | Node RPC object call `node.cert.renew { cert }`; reply `node.cert.renewed` | installs a fresh member cert on a running node at two thirds of TTL (+ jitter); presented on every connection opened after; terminates nothing, changes no status, publishes no frame; retried each tick on failure until the old cert expires | `lifecycles/node-cert-renew.md` (new) |
| `node.cert.get` | read, a field of [node.get](../../architecture/lifecycles/node-status.md) | any caller → the mesh-primary's view | the `Nodes` reply's `NodeView` gains `cert { issued_at, expires_at, issuer_expires_at }` | what the authority last issued to the birth; the evidence for a renewal and where a person sees expiry coming | a row on node-status.md's view table (wire format: the landing carries the marker) |
| `node.connections.admit` | primitive, on accept | the acceptor, for every inbound connection of a registered product ALPN | the connection's first bi-stream: the dialer's cert, the acceptor's verdict (RDM framing, not a Node RPC op) | verifies the presented cert (signature to issuer, issuer to root, issuer's mesh, both expiries, one floor of skew) and admits or drops; writes the `Refused` connection fact on a drop | `lifecycles/node-connections-admit.md` (new); connections.md §1.2 `Refused` |
| `fabric.ca.create` / `fabric.root.create` | steps inside fabric.create | the bootstrap admin, day 0 | none | the root from the app (the anchor); RDM's CA minted under it | fabric-create.md step 1 |
| `fabric.ca.renew` | workflow, fabric-primary's tick | the fabric-primary → every mesh | `mesh.issuer.renew` per mesh | rotates RDM's CA internally under the same anchor; no trust-set change (Luke: "using the caller root to issue a new root that I then pass on and we can rotate internally") | `lifecycles/fabric-ca-renew.md` (new) |
| `fabric.root.update` | caller op | the app or a person → the fabric-primary | a single call; reply `fabric.root.updated`; then `node.roots.update` per node, `fabric.ca.renew` | rotates the anchor through an overlap in five durable phases; nothing cut | on fabric-ca-renew.md |
| `node.roots.update` | primitive, inside `fabric.root.update` | the fabric-primary → one node, exact birth | Node RPC object call; reply `node.roots.updated` | installs the fabric's root set (one or two anchors; the secret to admins); a retire before every chain moved is refused by name | on fabric-ca-renew.md |

The ALPN registration itself is not a workflow: it is a `bind` argument, stated on
[fabric-certs.md](../../architecture/fabric-certs.md#on-a-connection) item 6 and refused by name for RDM's own two ALPNs.

Three new primitive pages (`node-issuer-update.md`, `node-cert-renew.md`, `node-connections-admit.md`)
and one new workflow page (`mesh-issuer-renew.md`), each in the shape of
[node.config.update](../../architecture/lifecycles/node-config-update.md): caller, target, request and reply, what it never
does, failure and receipts, spans, mermaid. Four index rows each in the page table, the workflow tables and
the wire-op → workflow table; the envelope's typed-object table gains `node.cert.renew` and
`node.issuer.update`. Q5 item 15 is absorbed by this table.

### Q7. Custody of the root secret

**Ruled A (Luke, 2026-10-16: "yes that's the way it works now, every node-admin has a root cert").
One root, not two: the root the app passes in is the fabric's root from then on, in RDM's custody; the
app's outward credentials never enter the chain. fabric-certs.md §Custody rewritten to A the same day.**

fabric-certs.md §Custody was first written as arm B below, without a yes; corrected on the answer. The failure that decides it: under B, an ordinary crash of the fabric-primary (the R-FM1 /
recovery case) loses the root; no issuer can be re-minted; every issuer expires within 30 days and every
member cert within a day after; every data-plane dial is then refused fabric-wide, with a person as the
only recovery.

- **A (recommended): every node-admin that can take the fabric seat holds the root**, written into its
  launch environment beside its mesh's issuing material and refreshed by `node.roots.update`, which
  carries the secret to admins and the public set to every other node. A crash of the fabric-primary
  then costs nothing: the elected successor mints and rotates from its own copy. Inside the mesh there
  is no authorization layer and every admin already holds its mesh's issuing key, so the added exposure
  is one key on hosts that already hold signing keys; the gain is that the fabric self-heals. It deletes
  the sealed carriage, `root: absent` and half of e5.
- **B (on the page now): the fabric-primary alone holds it**, carried to the successor inside the seat
  handover's direct Node RPC; an elected successor over a proven-exited holder has no copy, surfaces
  `root: absent` and `Concern`, and a person supplies a root (`fabric.ca.update`).
- x: something else.

## 4. Evidence to bring with Q1's landing

- The before: a Rafka lane's `rafka.mesh.auth.resolve.via-verified` on one `rafka-mesh-v1` accept
  (Rafka's node-admin, `Enforce`), and RDM's gate showing no such span on the same hop.
- On A: a flame of one `rafka-mesh-v1` dial through RDM's admission step, admitted with a `TestCerts`
  credential, and one refused by the verifier's named reason, with the connections-log facts beside the
  spans; the control: a Node RPC and a gossip connection on the same endpoint with no admission span.
- On B: a flame of a plain `rafka-mesh-v1` connect with no SVID frame and no `rafka.mesh.auth.*` span.
