# 7 — Prove the relay carries: make direct impossible

> Part 7 of *Building a multi-mesh substrate*. Post 5 explained what the relay is *for*. This one is
> about the harder thing: **proving it actually carries traffic** — and why the obvious proof is a lie.

The relay had been plumbing for a while. `RAFKA_RELAY_URL` set, `RelayMode::Custom` registered, the
path *existed*. But on a single host every node is loopback-reachable, so iroh always picks the direct
path and the relay sits idle. "The relay works" was an assertion, not a fact. The standing note in my
memory said as much: *relay carries traffic stays OPEN until green.*

## The obvious proof is a lie

The obvious move: give a node a **relay-only address** — an `EndpointAddr` with a relay URL and no
direct socket addr — so the only way to reach it is through the relay. Dial it, send bytes, done.

That proves the *first* packet went via relay. It does not prove the relay *carries* anything. Once the
QUIC connection is up, the two endpoints exchange their direct addresses over it and hole-punch. On
loopback that succeeds in milliseconds, the connection silently upgrades to direct, and any "is it
relayed?" check flips to false the moment after you looked. So the test either flakes, or "passes" by
checking before the upgrade — proving connect-via-relay, not relay-carriage. That's the version that had
burned this before.

## Make direct impossible

The fix isn't a cleverer assertion — it's removing the alternative. iroh's endpoint builder has
`.clear_ip_transports()`: bind with **no IP transport at all**. Then a direct hole-punch isn't slow or
unlikely, it's *impossible* — there is no socket to punch. The relay is the only transport that exists.
Now a delivered byte can only have come one way, and there's no timing window to race.

The whole proof, using iroh's built-in `test_utils` (cross-platform, no Docker, no WSL, no external
network simulator):

1. `run_relay_server()` — a real local relay with a self-signed cert.
2. Two endpoints: `RelayMode::Custom(relay_map)` + `CaRootsConfig::insecure_skip_verify()` (trust the
   test cert) + **`.clear_ip_transports()`** (no direct path can exist).
3. The client dials a relay-only `EndpointAddr` and runs a bi-stream echo.
4. Assert two things: the bytes round-trip **and** the selected QUIC path `.is_relay()`.

Bytes came back, over a connection that had no direct path to fall back to. That's relay-carriage, and
it's deterministic — green three times out of three, no sleep, no retry.

## Where I'd have gone wrong

My first instinct *was* the relay-only-address version, and I'd have shipped it green-on-loopback and
called the claim closed. The thing that stopped me was writing down the failure mode before coding it:
"relay-only controls how you *first* reach the peer, not which path carries traffic after." Said out
loud, it's obviously not a proof. The discipline that mattered wasn't iroh knowledge — it was refusing
to let "the test passed" stand in for "the test checks the thing."

## The honest caveat

This runs against test endpoints, not the live `IrohMeshTransport`. The production transport takes a
relay URL as a string, not a `RelayMap`, and has no hook to trust a self-signed cert — and bolting an
insecure-skip-verify into the real transport just to test it would be exactly the kind of
substrate-edit-for-a-test that doesn't earn its keep. A production relay has a real certificate and needs
no bypass. So the claim is precise: **the substrate can carry a write over the relay when there is no
direct path** — proven — not "the live mesh was forced onto the relay in the UI." Know which sentence
your green checkmark is under.
