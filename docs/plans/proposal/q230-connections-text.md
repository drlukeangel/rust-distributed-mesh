# Q230: connections.md text (#3230)

The exact old and new text of every `docs/architecture/connections.md` line Q230 changes (the lead, 2026-10-27, INDEX Q230: the data plane speaks proxy; forward stays the management plane's word; words only, the carrier list kept). Line numbers are origin/main's (529b978dd, after #3229); the issue's (:329, :373-376, :408, :441-442, :682) predate #3229 and moved to :322, :366-369, :406, :439-440, :680. The page in this branch carries the new text. Each change is one forward word on a Proxy line becoming carry or proxy; nothing else on the page changes.

Not changed, as the ruling says: :18 (the page's scope sentence), :72-87 (the two words, one per plane: Forward the control plane's, Proxy the data plane's), and :440's "the Forward reply `CarrierEdgeLost`" (a Node RPC call carried over a Proxy's carrier is a Forward, `NodeRpcClient::call_resolved` in `crates/rdm-transport/src/route.rs:172-200`, node-rpc.md §36 :935; see 7 below).

## 1. §5 (:322), how the source learns the carrier's edge

Old:

````text
edge to the destination is learned from the carrier when it is asked to forward (§8).
````

New:

````text
edge to the destination is learned from the carrier when it is asked to carry (§8).
````

## 2. §6 (:366-367), Rafka's broker-data policy

Old:

````text
A protocol's policy is the application's (§14); Rafka's broker-data policy, forwardable with gateway
carriers only, is stated on Rafka's pages.
````

New:

````text
A protocol's policy is the application's (§14); Rafka's broker-data policy, proxied with gateway
carriers only, is stated on Rafka's pages.
````

## 3. §6 (:369), a protocol that does not proxy

Old:

````text
A non-forwardable protocol never consumes Proxy as a legal route even if one exists.
````

New:

````text
A protocol that does not proxy never consumes Proxy as a legal route even if one exists.
````

## 4. §7 step 3 (:406)

Old:

````text
3. It forwards the invocation once through that carrier.
````

New:

````text
3. It proxies the invocation once through that carrier.
````

## 5. §8 (:439), the carrier asked

Old:

````text
the carrier's own fact and travels nowhere: when the carrier is asked to forward and its own latest Direct to
````

New:

````text
the carrier's own fact and travels nowhere: when the carrier is asked to carry and its own latest Direct to
````

## 6. §13 A (:680)

Old:

````text
dial ladder before forwarding.
````

New:

````text
dial ladder before proxying.
````

## 7. For the lead, not applied: §8 (:439-441), the carrier's refusal as the code makes it

The ruling asks the tunnel's real refusal to be read from code before any page text names it. Read at 529b978dd, `carrier-edge-lost` reaches the source on two paths, and :440 names only the second:

- A Proxy stream connection (`Dialer::open` → `open_tunnel`): the carrier answers `ControlAnswer::Refused { reason: "carrier-edge-lost" }` on the tunnel's control stream when it holds no Active Direct of its own to the destination, or cannot resolve, dial or open a stream to it (`crates/rdm-transport/src/connections.rs:1007-1096`). The source records the refusal on its `rdm.node.connections.create.via-tunnel` span (`outcome = refused: carrier-edge-lost`), closes its leg with close code 412 (`CLOSE_CODE_REFUSED`) carrying the reason, disconnects a held Proxy `carrier-edge-lost` (:1804-1819), and, choosing a carrier after a NotSent Direct, asks the next (:1478). No close code from the carrier is the refusal; the 412 is the source's own close after it.
- A Node RPC call carried over a Proxy's carrier (`call_resolved`, `route.rs:172-200`): a Forward (0x1A), answered `ForwardReply::CarrierEdgeLost` from the carrier's failed inner dial (`crates/rdm-transport/src/forward.rs:85`, :217), which the origin reads as `NotSentReason::CarrierEdgeLost` and disconnects the Proxy (`carrier-edge-lost`).

Old:

````text
the carrier's own fact and travels nowhere: when the carrier is asked to carry and its own latest Direct to
the destination is not Active, it refuses the carried call by name (`carrier-edge-lost`, the Forward reply
`CarrierEdgeLost`), and that refusal is how the source learns the edge is gone.
````

New (dev recommendation, if the lead takes it to Luke):

````text
the carrier's own fact and travels nowhere: when the carrier is asked to carry and its own latest Direct to
the destination is not Active, it refuses by name (`carrier-edge-lost`: on a Proxy connection the tunnel's
refusal on its control stream, on a Node RPC call carried over the Proxy the Forward reply
`CarrierEdgeLost`), and that refusal is how the source learns the edge is gone.
````
