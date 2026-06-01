# 6 — Kill by message, not by ownership

> Part 6 of *Building a multi-mesh substrate*. Making "kill that node" a mesh operation instead of an
> OS one — and turning a node's whole lifecycle into something the mesh broadcasts.

The console could kill nodes. Sort of. It worked by holding the OS child handle of every process it had
spawned and calling `TerminateProcess`. Which means a console could only kill **its own** spawns — not a
node it merely *saw* in another mesh. That's backwards for a self-aware fleet: which OS process happens
to own a node should have nothing to do with who can operate on it.

## A kill is a message

So a kill became a **control op**. Any participant — including a console in a different mesh — sends the
target a `Shutdown` frame over the mesh (direct or via the relay). The target receives it, shuts *itself*
down gracefully (emits `node.stopping`, broadcasts its own tombstone), and exits. The caller resolves the
target's address from what it can already see — its own gossip, the cross-mesh backbone directory, or its
spawn registry — and dials it. No process ownership anywhere.

The proof: from the **mesh1** console, kill a **mesh2** gateway the mesh1 console never spawned. The
gateway process dies, and it disappears from both consoles immediately. mesh1 *commanded* it; it didn't
own it.

## A node is a node

While wiring that up, a sharper question: a console is in a mesh — why doesn't *its* mesh show up to
other consoles? Because only gateways published a mesh's summary to the backbone, and the admin-ui was a
subscribe-only Observer. So a mesh whose only node was its console was invisible cross-mesh.

A node is a node. The admin-ui now publishes too — it's a backbone publisher *candidate* alongside
gateways, and the soft lease still elects exactly one publisher per mesh. So every mesh advertises
itself, even a bare console. Both consoles now show both meshes whether or not either has a gateway —
and we test that with a deliberately **non-balanced** fleet, because a symmetric one can pass on
coincidence.

## A node is a *state*

The tombstone proved a nice pattern: broadcast "this node is gone" as an event and everyone evicts
instantly — no waiting for a timeout. The natural generalization is to make the *whole lifecycle* an
event. Not a binary join/leave, but a state:

```
Joining · Alive · Degraded · Updating · Draining · Leaving · Dead
```

A node publishes its own lifecycle on every transition; `Leaving` is the old fast-delete. The one it
*can't* publish is `Dead` — a crashed node announces nothing — so `Dead` is what observers assign when a
node vanishes without a `Leaving`. The operator gets the difference for free: "left cleanly" vs "crashed"
vs "just rolling an update" are now visibly distinct, not all collapsed into "gone."

## The catch (and the most expensive lesson)

Additions were *slow* — a node took ~40s to appear, while removals were instant. The cause wasn't the
publish cadence; it was that a node couldn't resolve the *address* of a peer it learned about via gossip
(mDNS is off to avoid localhost cross-contamination, so only seed addresses were known) — iroh fell back
to a discovery lookup that failed and retried. The fix is to use the address that's already in the gossip
digest (`location`) and register it, so connections resolve directly. Topology-independent — it works
across the relay too, which link-local mDNS never could.

But the real lesson of this stretch wasn't in the mesh at all. For *hours* the cross-mesh view looked
broken, and I nearly wrote off two earlier sprints as defective. They weren't. **I was testing stale
binaries.** On Windows a running `.exe` is file-locked, so rebuilding while a node runs silently leaves
the old binary in place — and worse, the console spawns its child nodes from `target/debug` while I'd
been building `--release`. So the nodes that actually ran were ancient code, broadcasting a wire format
the new code couldn't decode. A one-line `Get-Process | Select Path` showed it instantly, once I stopped
trusting "the build succeeded" and started gating on *"is the binary I'm about to run actually newer than
the source I changed?"*

The mesh code was right the whole time. The discipline — kill everything, rebuild, verify the binary is
fresh, *then* conclude — is the part that wasn't. That one's framed on the wall now.
