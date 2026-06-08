# 11 — A certificate that rides the gossip (and why it shouldn't)

> Part 11 of *Building a multi-mesh substrate*. iroh authenticates *who a node is*; it says nothing
> about *whether that node is allowed here*. I built the trust layer two ways — riding the membership
> gossip, and presented at the connection — and only one of them survives contact with a real design.
> This is which, and why it isn't HMAC and isn't quite mTLS.

Parts 2–8 built *awareness* and *delivery* across meshes. Part 10 gave every node a local cache. All of
it assumed something we never enforced: that a node showing up in the gossip *belongs* there. Up to now,
anything that could speak the wire and join the topic became a member. Fine for a demo; not a trust
boundary.

This post adds one. The shape is small — and the interesting part is a wrong turn I took first.

## Identity is not authorization

iroh gives us identity for free. A node's `NodeId` **is** its ed25519 public key, and every QUIC
connection cryptographically proves the peer holds the matching secret. There is no spoofing a NodeId —
the transport settled that.

What iroh deliberately does *not* answer is policy:

- Is this (authenticated) node **allowed on this mesh**?
- What **role** does it have — gateway, broker, admin?
- **Until when**?

Those are application questions. iroh is a networking library; it correctly refuses to have an opinion
about your membership model. So the gap we fill is *authorization*, not authentication. Hold that
distinction — it's the whole reason the answer lands where it does.

## The shape: node-admin is the CA

The node-admin already creates every node (Part 6 — it spawns them with their topology), so it's the
natural certificate authority. It holds **one ed25519 keypair** — reusing iroh's own crypto, no new
stack — and that key is the **root**. When it spawns a node, it signs a tiny certificate:

```rust
pub struct NodeCert { pub node_id: String, pub node_type: String, pub expiry_ms: u64 }
// signed by the admin's CA key over postcard(cert) -> SignedCert { cert, sig }
```

The cert is just a signed assertion: *the CA says NodeId X has role R until T.* The only real question
is **where you verify it** — and there are two candidates.

## The wrong turn: verify it in the gossip

I built this one first, because it has a tempting property. The node carries the cert in its **gossip
digest** — the heartbeat it already broadcasts — and every receiver verifies it before admitting the
node to `live_digests()`. No valid cert → the digest is dropped → the node never enters anyone's
topology. Three checks on receipt:

1. **CA signature** valid against the admin's public key,
2. the cert's `node_id` **matches the iroh-authenticated sender's NodeId** (the anti–cert-stuffing
   bolt — you can present your own cert, not wear someone else's),
3. not **expired**.

And it works:

![An uncertified node is rejected: the admin spawned 6 processes, but only 5 enter the
topology](screenshots/11-cert-rejected.png)

The console says it in one line: **6 spawned · 5 nodes**. We launched a sixth process with no cert; the
trust boundary never let it into the graph, and the drop emits a `rafka.cert.reject` span on every
peer — queryable in Jaeger, not a silent nothing. It was the one thing that let a cross-mesh **repeater**
carry trust: the repeater relays *digests*, so a signed cert riding the digest could be re-broadcast into
another mesh and validated end-to-end against a shared root.

So why is this a wrong turn?

## ...and the answer: verify it at the connection

Riding the gossip had exactly **one** load-bearing reason — the repeater. And the repeater is an
experiment, not a destination. Cross-mesh in a real deployment isn't a digest-relaying middlebox; it's
two nodes that **open a direct connection**. Take the repeater away and the gossip-cert has no job left:
the only thing it still buys is "uncertified node is invisible in topology" — marginal defense-in-depth,
paid for by parsing security-critical content inside the gossip path, the one place you most want to stay
dumb and fast.

So the cert belongs at the **connection**, not in the gossip. The node presents the SVID as the **first
bi-stream frame** when it opens a connection to do work; the far side verifies it right there — same three
checks — before trusting the connection for data. Gossip goes back to being pure soft-state membership,
carrying no trust at all. One gate, at the door, on the connection you're actually about to use.

That's the lesson the series keeps re-teaching (Part 7 made the relay *prove* it carries; Part 8 was a
bug that wasn't where the theory said): the tidy, clever mechanism — trust that propagates through the
gossip and survives a relay — loses to the boring one that checks the credential where it matters.

## Why not HMAC

A shared-secret HMAC was the earlier direction in the project, and retiring it for signing is half of why
this post exists. HMAC is symmetric: the key that **verifies** a MAC is the same key that **mints** one.
That single fact breaks everything:

- **No issuer/verifier split.** We want a CA that issues and a fleet that only *verifies*. With HMAC every
  verifier is also a forger — anyone who can check a cert can fabricate one for any identity.
- **Blast radius is the whole mesh.** Recover the shared secret from one node, forge every node. No way to
  bind a MAC to an identity that a key-holder couldn't just re-mint.
- **The shared root becomes a shared *forging* key.** Cross-mesh trust needs the same root on every admin;
  with HMAC that's handing a *create-members* key to everyone. Not a root of trust — a skeleton key.

Asymmetric signing fixes all three: the CA's *secret* issues, its *public* key verifies, and a verifier
proves a cert genuine without being able to produce one.

## Why not mTLS

The honest version: presenting the SVID at connect **is** the mTLS idea — a credential exchanged and
verified at connection time. We just don't reach for *X.509* mTLS to do it.

iroh's QUIC is *already* mutually authenticated — by the NodeId, an ed25519 key both ends prove. X.509
mTLS would re-prove the identity we have for free and drag in a PKI: cert chains, ASN.1, CRL/OCSP. The
only thing we need to *add* is the authorization layer — `{NodeId, role, expiry}`, CA-signed — and a
short-lived ed25519 SVID as the first bi-stream frame does exactly that, on iroh's existing crypto, with
revocation as a short TTL + re-issue (no CRL). (Transport *encryption* at the edge is a separate
deployment concern in `crypto.md`.) So it's mTLS-shaped, built the cheap way: identity from iroh,
authorization from one tiny signed frame on top.

## The shared root still earns its keep

The CA key is one keypair, and it's **shareable**: copy `ca-secret.json` into a second admin's data dir
before it boots and now two meshes issue certs from the *same* root.

![Two admins, one shared root: each console renders both meshes; `/api/ca` returns the byte-identical CA
pubkey on both](screenshots/11-multimesh-shared-root.png)

That's what makes cross-mesh trust work **without** the repeater: when a `mesh1` node opens a direct
connection to a `mesh2` node, it presents its SVID and the far side verifies it against the shared root —
they trace to one authority, so the connection is trusted even though the two meshes never share gossip.
The shared root was the genuinely good idea; the repeater that rode on it was the part to drop.

## What it cost

The trust layer is one ~100-line module (`cert.rs`: `issue_cert` / `verify_cert`, hex-postcard on the
wire) plus one verification point — the bi-stream a node already opens to work. No PKI, no new crypto
dependency, no extra round-trips. The gossip experiment was real and verified (unit suite, the
`rafka.cert.reject` spans and screenshot above), and it taught the actual lesson by being the more elegant
answer that wasn't the right one.

Identity was iroh's job. Authorization is one signed cert — checked at the door, where the trust is
actually spent.
