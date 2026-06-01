# 5 — The relay is a postbox, not a peer

> Part 5 of *Building a multi-mesh substrate*. How a cross-mesh write actually travels when the two
> nodes can't reach each other directly — and why the thing in the middle can't read it.

Parts 2 and 4 solved *awareness*: a `mesh1` gateway can find out where a `mesh2` broker lives. But
knowing the address isn't reaching it. On a real network the two nodes are often behind NATs or
firewalls that won't let them open a direct peer-to-peer path. That's what the **relay** is for.

## Direct when possible, relay when not

The relay is **not** something we built — it's iroh's, and that's deliberate (the substrate's whole
premise is *use the library, don't hand-roll mesh infrastructure*). The behaviour is iroh-native:

- a connection **starts** over the relay (the one path that's reliably reachable),
- iroh then tries to **hole-punch a direct path** in parallel,
- if direct works, it **migrates to direct**; if it never works, it just **stays on the relay**.

So "relay is the fallback" really means *the connection stays on the relay when the direct upgrade can't
be made*. Nodes direct-connect when possible; the relay is there when it isn't.

## Why the relay ever has "better luck" than direct

It doesn't have magic — it has **structural** luck. Direct peer-to-peer fails under symmetric NAT or
restrictive firewalls because neither side will accept an unsolicited inbound connection. The relay is a
**publicly reachable rendezvous both sides connect *outbound* to**, and outbound is almost always
allowed. So `A → relay → B` works when `A → B` directly doesn't. That's the whole and only advantage.

The flip side: on **localhost** — every node a process on one box — there's no NAT and no firewall, so
direct *always* works and the relay sits idle. That isn't a bug; it's the system working. The relay only
earns its keep across real network boundaries.

## Where it lives

The relay is **infrastructure, not a mesh node**. It's the `iroh-relay` binary, addressed purely by a
URL (`RAFKA_RELAY_URL`). It doesn't run rafka, doesn't join gossip, doesn't know what a mesh is. It has
to live somewhere **both meshes can reach outbound** — a cloud VM, a DMZ host, an edge box — *outside*
any single mesh's NAT. One relay can serve many meshes; for HA/latency you run a few, geo-distributed,
and each node uses its nearest as its "home relay" (you reach a peer through *that peer's* home relay).
In dev it's just a throwaway local process — which, again, is exactly why localhost can't prove anything
about fallback.

## Isn't that a lot of overhead?

Less than it looks. Each node keeps **one** standing link to the relay, reused for all its relayed
traffic — and iroh holds that link open anyway for discovery and hole-punch coordination, so falling
back to it adds essentially *no new connection*. An idle relay link is just keepalives. And the relay
**server's** load scales with the *fraction of traffic stuck on relay*, not total traffic — classic
STUN/TURN economics: coordination is cheap and serves everyone, actual relaying only happens for the
minority that can't go direct. The real costs, when a flow *is* relayed, are latency (an extra hop) and
the operator's bandwidth — paid only by the connections that need it.

## The part that matters: the relay can't read your mail

The relay secures *nothing* about the conversation — and that's the point. Security is **end-to-end
between the two nodes**, identical whether the path is direct or relayed:

1. **Identity is the public key.** A node's `node_id` *is* its Ed25519 public key. You don't dial an IP,
   you dial a key — which is why the directory carries `node_id` and `endpoint.connect` is
   identity-based.
2. **The peer connection is authenticated by those keys.** The end-to-end QUIC/TLS 1.3 handshake proves
   the remote end holds the **private** key matching the `node_id` you dialed. Same guarantee on a
   relayed path as a direct one.
3. **The relay is a dumb forwarder of already-encrypted packets.** It sees ciphertext plus the
   destination key to route on. It can't read the data (it holds no key), can't impersonate either peer
   (a MITM attempt fails the end-to-end handshake), can't forge or inject.

The subtlety worth keeping straight: there are **two separate TLS layers**.

- **node ↔ relay** uses the *relay's own* server cert (Let's Encrypt in prod, self-signed in dev). This
  only protects the hop *to* the relay.
- **node ↔ node** is the end-to-end QUIC encrypted under the *peers'* keys, riding *inside* that.

So when a test trusts a dev relay's self-signed cert (`insecure_skip_verify`), **peer-to-peer security is
untouched** — that flag says "trust this dev relay box," not "trust whoever's on the other end." Peer
identity is always verified by key.

**Honest threat model:** a malicious or compromised relay can hurt **availability** (drop or delay your
packets) and observe **metadata** (which `node_id`s talk, when, how much) — but never **content** or
**identity**. And you can restrict *who is allowed to use* a relay via its access control, so strangers
don't burn your bandwidth. The relay is an untrusted postbox: you trust it to *forward*, not to *read or
vouch*. The keys do the vouching, point to point, direct or relayed alike.

## The catch

Localhost can't demonstrate any of this — direct always wins, so the relay never carries a byte. To
actually see a live connection fall over to the relay, you have to *create* the blocked condition. We
proved it with iroh's **built-in** test facilities (cross-platform, no Linux-only netsim): stand up a
local relay, connect two endpoints over a direct path, then kill the direct path mid-connection. iroh
detected the dead path and **migrated the same live connection to the relay**, with data still flowing
end to end — exactly the fallback, proven without a firewall or a second machine.

One number fell out of that worth flagging: the direct→relay cutover took **~15 seconds** — iroh's QUIC
path-death detection timeout. A write *in flight* when a path dies stalls for that window before it
reroutes; writes after it go straight to relay. It's a one-time cutover cost, and it's tunable via the
transport's keepalive/idle settings — a knob to weigh against whatever failover target a real deployment
needs.
