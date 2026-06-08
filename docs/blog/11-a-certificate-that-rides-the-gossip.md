# 11 — A certificate that rides the gossip

> Part 11 of *Building a multi-mesh substrate*. iroh authenticates *who a node is*. It says nothing
> about *whether that node is allowed here*. This is the trust layer that fills the gap — one signed
> cert, checked twice: at the connection a node opens (the belt) and inside the membership gossip it
> broadcasts (the suspenders — which is also what lets a cross-mesh relay carry trust). And why it
> isn't HMAC, and isn't quite mTLS.

Parts 2–8 built *awareness* and *delivery* across meshes. Part 10 gave every node a local cache. All of
it assumed something we never actually enforced: that a node showing up in the gossip *belongs* there.
Up to now, anything that could speak the wire and join the topic became a member. That's fine for a
demo. It is not a trust boundary.

This post adds one. The shape is small on purpose.

## Identity is not authorization

iroh gives us identity for free. A node's `NodeId` **is** its ed25519 public key, and every QUIC
connection cryptographically proves the peer holds the matching secret. There is no spoofing a NodeId —
the transport already settled that.

What iroh deliberately does *not* answer is policy:

- Is this (authenticated) node **allowed on this mesh**?
- What **role** does it have — gateway, broker, admin?
- **Until when**?

Those are application questions. iroh is a networking library; it correctly refuses to have an opinion
about your membership model. So the gap we have to fill is *authorization*, not authentication. Keep
that distinction — it's the whole reason the design looks the way it does.

## The shape: node-admin is the CA, the cert is checked twice

The node-admin already creates every node (Part 6 — it spawns them with their topology). So it's the
natural certificate authority. It holds **one ed25519 keypair** — reusing iroh's own crypto, no new
stack — and that key is the **root**.

When the admin spawns a node, it signs a tiny certificate:

```rust
pub struct NodeCert { pub node_id: String, pub node_type: String, pub expiry_ms: u64 }
// signed by the admin's CA key over postcard(cert) -> SignedCert { cert, sig }
```

That one cert is enforced at **two points** — belt and suspenders:

- **The belt (primary)** — the node presents the cert at **mesh-connect**, as the first frame when it
  opens a bi-stream to actually do work. That gates *role and actions*, on the very connection you're
  about to trust for data. It's the mutual-auth-at-connect idea (more in *Why not mTLS*).
- **The suspenders (secondary)** — the node *also* carries the cert in its **gossip digest**, the
  heartbeat it already broadcasts. Every receiver verifies it before admitting the node to
  `live_digests()`. That gates *membership* — uncertified ⇒ invisible — and, as we'll see, is the thing
  that lets the cross-mesh repeater carry trust.

Both gates run the same three checks:

1. **CA signature** valid against the admin's public key,
2. the cert's `node_id` **matches the iroh-authenticated sender's NodeId**,
3. not **expired**.

This post is mostly about the suspenders — riding the gossip is the novel part, and it's what makes the
relay work — but the belt is the primary, connection-time check. For the gossip gate: no valid cert →
the digest is dropped → the node never enters anyone's topology. The enforcement sits at the one place
every membership announcement must pass through: the gossip receive path. There's no extra handshake to
bolt on — and the connection-time belt reuses the bi-stream the node already opens to work.

Check (2) is the anti–cert-stuffing bolt. The cert asserts "node X is authorized." A different node that
*replays* X's perfectly valid cert is caught because the receiver compares the cert's `node_id` against
the NodeId iroh already authenticated on the wire. You can present your own cert; you can't wear
someone else's.

![An uncertified node is rejected: the admin spawned 6 processes, but only 5 enter the
topology](screenshots/11-cert-rejected.png)

The console tells the story in one line: **6 spawned · 5 nodes**. We launched a sixth process with no
cert. The admin knows it spawned it. The *trust boundary* never let it into the graph. The drop emits a
`rafka.cert.reject` span (`reason=bad-signature`) on every enforcing peer — so the rejection is a
first-class, queryable event in Jaeger, not a silent nothing.

## Why not HMAC

A shared-secret HMAC was the earlier direction in the project, and retiring it in favour of signing is
most of why this post exists.

HMAC is symmetric: the key that **verifies** a MAC is the same key that **mints** one. That single fact
breaks everything we need:

- **No issuer/verifier split.** We want a CA that issues and a fleet that only *verifies*. With HMAC,
  every verifier is also a forger. Any node that can check membership can fabricate it for any identity.
- **Blast radius is the whole mesh.** Compromise one node, recover the shared secret, forge every node.
  There's no per-node revocation and no way to bind a MAC to a specific identity that a holder of the
  key couldn't just re-mint.
- **The shared root becomes a shared *forging* key.** Cross-mesh trust (below) needs the same root on
  every admin. With HMAC that means handing a key that can *create* members to every admin and every
  verifier. That's not a root of trust; it's a skeleton key.

Asymmetric signing fixes all three: the CA's *secret* issues, the CA's *public* key verifies, and a
verifier can prove a cert is genuine without being able to produce one.

## Why not mTLS

Here's the honest version: the **belt *is* the mTLS idea** — present a credential at connection time and
verify it mutually. We just don't reach for *X.509* mTLS to do it, and mTLS can only ever be the belt,
never the suspenders.

**Why not literal X.509 mTLS for the belt.** iroh's QUIC is *already* mutually authenticated — by the
NodeId, an ed25519 public key both ends cryptographically prove. X.509 mTLS would re-prove the identity
we have for free and drag in a PKI: cert chains, ASN.1, CRL/OCSP revocation plumbing. The only thing we
actually need to *add* is the authorization layer — `{NodeId, role, expiry}`, signed by the node-admin
CA — and a short-lived ed25519 SVID presented as the first bi-stream frame does exactly that, on iroh's
existing crypto, with revocation as a short TTL + re-issue (no CRL). (Transport *encryption* at the edge
is a separate deployment concern in our `crypto.md`.) So the belt is mTLS-shaped, built the cheap way.

**Why mTLS can't be the suspenders.** A TLS or SVID handshake authenticates a *connection*; the proof is
a property of a live session and **terminates at each hop**. That makes it structurally unable to do the
second gate:

- it can't ride *inside* the membership digest to gate who's even **admitted** to the mesh, and
- it can't pass through the **repeater** — the trust-agnostic forwarder that relays one mesh's *digests*
  onto another's topic *without inspecting them*, so the far-mesh *receiver* validates end-to-end against
  the shared root.

A signed blob inside a gossip message does both; a terminated handshake does neither. So we use the
connection check for the belt (lightweight, on iroh) and the gossip check for the suspenders — the part
mTLS simply can't reach. Belt and suspenders, one cert.

## The shared root, and why the relay can stay dumb

The CA key is one keypair, and it's **shareable**. Copy the admin's `ca-secret.json` into a second
admin's data dir before it boots, and now two meshes issue certs from the *same* root. A `mesh1` node
can verify a `mesh2` node's cert — they trace to one authority — even though the two meshes are separate
gossip swarms that never exchange membership directly.

![Two admins, one shared root: each console renders both meshes; `/api/ca` returns the byte-identical CA
pubkey on both](screenshots/11-multimesh-shared-root.png)

That's what lets the repeater stay a postbox (Part 5's principle, now at the *trust* layer): it forwards
a `mesh1` digest into `mesh2` without inspecting the cert, and `mesh2`'s receiver — running the exact
same three checks — admits it *only because the roots match*. Compromising the relay buys an attacker
nothing: the relay has no trust to grant. Hand it an uncertified or foreign-root node and it'll forward
it just the same; every certified receiver still drops it.

## What it cost

The whole trust layer is one ~100-line module (`cert.rs`: `issue_cert` / `verify_cert`, hex-postcard
on the wire) and two enforcement points that reuse machinery already in place — the bi-stream a node
opens to work (the belt) and the heartbeat it already broadcasts (the suspenders). No PKI, no new crypto
dependency, no extra round-trips. The gossip gate was verified the way everything in this series is: a
unit suite (valid / wrong-CA / tampered / cert-stuffing / expired / round-trip), the live API (5
certified nodes up, the uncertified one suppressed after the join grace), the telemetry
(`rafka.cert.reject` in Jaeger), and the console screenshots above.

The lesson is the same one Part 6 taught about killing nodes and Part 5 taught about the relay: don't
reach for the heavyweight X.509 machine when one small signed assertion will do — and put it where each
layer needs it. Identity was iroh's job. Authorization is one signed cert, checked at the connection and
again in the gossip — belt, and suspenders.
