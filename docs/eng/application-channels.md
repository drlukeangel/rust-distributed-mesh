# Getting started with application channels

An application channel is a gossip topic your nodes share for your own data: rows, events, anything
your code names. RDM carries the bytes. Your code names the channels, decides what to publish, and
decides what a received event means. The contract is
[gossip.md §8](../architecture/gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)
("RDM provides channels; an application's nodes join the channels its own code names", Q362). This
page shows how to use it.

The API is in `rdm-gossip`:

| What | Where |
|---|---|
| A channel's id | `rdm_gossip::channel::ChannelId::app(&fabric_id, name)` |
| Join it | `Channels::join(id, peers)` |
| Take its publisher and its one receiver | `Channels::app(name)` → `(Publisher, Subscriber)` |
| One event you publish | `rdm_gossip::app::Event { subject, sequence, dataschema, data }` |
| One event you receive | `rdm_gossip::app::Received { source, subject, sequence, time_ms, dataschema, data, span }` |
| A named failure | `rdm_gossip::app::ChannelError` |
| A peer from a topology entry | `rdm_gossip::channel::peer(&endpoint_id)` → `EndpointAddr` |

A working example that uses only these public surfaces: `demo/tests/app_channels.rs`, and
`demo/tests/projections.rs` for the pull that goes with it.

## 1. Name the channel

```rust
use rdm_gossip::channel::ChannelId;

let id = ChannelId::app(topology_view.fabric_id(), "myapp.orders");
```

The id is a function of the fabric id and your name, so two fabrics on one network never share a
channel. Pick one name per stream of data your nodes need apart. Rafka names one channel per
projected kind, `rafka.entity.<kind>`, so a node joins only the kinds it projects.

## 2. Join it, in `pre_node_ready`

Join your channels in the node's [`pre_node_ready`](../architecture/fabric-node-hooks.md#pre_node_ready)
hook, before you pull current data. Then whatever changes during the pull reaches you by gossip.
The hook's context carries the `Channels` handle, the topology view, and the node-admin that
answered the node's join (`accepting_primary`), which makes a good first peer:

```rust
use rdm_gossip::channel::{peer, ChannelId};

async fn join_orders(ctx: &rdm_node::node_hooks::NodeReadyCtx) -> anyhow::Result<()> {
    let id = ChannelId::app(ctx.topology_view.fabric_id(), "myapp.orders");
    // First neighbours: the node-admin that answered this node's join, and any nodes
    // your code names from its topology view, each by its endpoint_id.
    let mut peers = Vec::new();
    if let Some(admin) = ctx.accepting_primary.accepted() {
        peers.push(iroh::EndpointAddr::new(admin.endpoint));
    }
    // e.g. peers.push(peer(&some_node.endpoint_id)?);
    ctx.channels.join(id, peers).await
}
```

The first `join` of an id subscribes with `peers` as its first neighbours. A later `join` of the same
id hands any new peers to the channel it already holds. Outside the hook, a running node has the
same handle as `RunningNode::channels`.

A node-admin is one of your application's nodes. It joins the channels its code names, like any
node. Joined by nodes of two meshes, a channel carries events between them.

## 3. Take the publisher and the receiver

```rust
use rdm_gossip::app::ChannelError;

match ctx.channels.app("myapp.orders").await {
    Ok((publisher, subscriber)) => { /* keep both: this is the channel's one pair */ }
    Err(ChannelError::ReceiverHeld { .. }) => { /* you already hold the live pair: keep it */ }
    Err(e) => return Err(e.into()), // Subscribe: the channel is not joined, or gossip refused it
}
```

`Channels::app` hands each joined channel's pair out once. A second call while that channel is live
answers `ReceiverHeld`, so the code that holds the pair keeps it.

## 4. Subscribe: receive events

```rust
while let Some(event) = subscriber.recv().await {
    // Apply inside the event's span: it is parented to the author's span, so your apply shows up
    // under the write that caused it in the trace.
    event.span.in_scope(|| apply(&event.subject, event.sequence, &event.data));
}
// recv() returns None once the channel is left.
```

Each `Received` is a whole event, reassembled if it was sent in chunks. It carries its exact author
(`source`, the publishing birth) and the authored Rafka-time `time_ms`.

**RDM compares nothing.** Stale, duplicate and out-of-order events are your code's call. Order by
your own `sequence` and fields (the demo orders by a value it carries, then `sequence`). Never rely
on arrival order.

**Loss is counted, never hidden.** Gossip is best effort, and a slow receiver's queue can overflow.
`subscriber.lost()` is a `watch` of the local-loss generation: when it changes, events were lost
here. Pull the current data again from where you pulled it in `pre_node_ready`. `subscriber.dropped()`
counts queue overflows. A lagged or full queue is also named by span
(`rdm.mesh.channel.reject.via-lagged`, `via-queue-full`).

## 5. Publish

```rust
use rdm_gossip::app::Event;

publisher
    .publish(Event {
        subject: "order/42".into(),            // your key
        sequence: Some(7),                     // your version, if you have one; RDM never compares it
        dataschema: Some("myapp.order.v1".into()),
        data: postcard::to_allocvec(&order)?.into(),
    })
    .await?;
```

Publish inside the span of the work that caused the event: RDM takes `traceparent` from the
current span, so receivers' spans join your trace. RDM sets the event's `id`, `source` and `time`.
An event too large for one gossip message goes as chunks under one `id` and is handed over whole.
An event larger than sixteen chunks is refused as `ChannelError::Oversize`. Each event is published
once, with no resend: a receiver that missed it pulls the current data (see "Loss" above).

## 6. Leave, and come back

- `subscriber.leave().await` leaves the channel. Dropping the `Subscriber` closes it as well.
- When the node leaves its mesh (a stop, or its node-admin commanding it back), RDM leaves every
  application channel the node joined. From then on, publishing on a pair from before answers
  `ChannelError::Left`, and that `Subscriber`'s `recv()` returns `None`.
- The node then runs `pre_node_ready` again (`start_kind = AfterStop`). Join again, then call
  `Channels::app`: a left channel is subscribed fresh, and `Channels::app` hands you the **new**
  pair. RDM keeps nothing of the old one. Publish and receive on the new pair from then on.

So, the one rule: in every `pre_node_ready`, join your channels, then call `Channels::app`. Take the
pair it returns, or keep the one you hold on `ReceiverHeld`. Treat `Left` on a publish as "this node
left its mesh": the next `pre_node_ready` hands you a new pair.

## 7. Errors

| `ChannelError` | Means |
|---|---|
| `Subscribe { type, detail }` | the channel is not joined, or gossip refused the subscription |
| `ReceiverHeld { type }` | the live pair was already handed out; keep the one you hold |
| `Left` | the channel was left (the node left its mesh, or the receiver left) |
| `NoAuthor` | the node's author identity is not set yet: publish after the node has joined |
| `Oversize { bytes, max }` | the event does not fit sixteen chunk messages |
| `Encode(detail)` | the event or one of its chunks could not be encoded |
| `Broadcast { chunk, of, detail }` | gossip refused the event, or one of its chunks |

## 8. What to read next

- [gossip.md §8](../architecture/gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth):
  the channel contract, the event envelope, and the pull at entry.
- [fabric-node-hooks.md, `pre_node_ready`](../architecture/fabric-node-hooks.md#pre_node_ready):
  what the hook's context carries, and its outcomes.
- [Consumer guide](consumer-guide.md): the other surfaces an application uses.
- `demo/tests/app_channels.rs` and `demo/tests/projections.rs`: the worked example.
