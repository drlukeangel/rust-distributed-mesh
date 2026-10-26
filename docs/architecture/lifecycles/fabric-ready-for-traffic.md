# Fabric ready-for-traffic

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric ready-for-traffic is the state that opens traffic. A node is ready-for-traffic once its
[`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready) returned `Ok` and it reported node.started; a
mesh is ready-for-traffic when every planned birth of the accepted Build is ([States](states.md#who-sets-each-state)).
When every mesh the accepted Build names is ready-for-traffic and the Build's attempt is claimed, the
fabric-primary sets the fabric ready-for-traffic and Rafka receives one
[`on_fabric_ready`](../fabric-node-hooks.md#on_fabric_ready) notice, on which it opens client traffic
(Q174, Q175). No node is commanded to open traffic and none checks in.

## The fabric-primary's step

```text
every mesh the accepted Build names ready-for-traffic, its attempt claimed
fabric-primary                    FabricStatus ready-for-traffic; one on_fabric_ready notice to Rafka
```

FabricStatus ready-for-traffic travels under the [frame catalog](../gossip.md#31-frame-catalog). Who
declares the transition is [States](states.md#states)'s. A resize or an executable replacement puts the
fabric back to pending ([States](states.md#resize-and-executable-replacement)), and it is ready-for-traffic
again once every mesh of the new topology is.

### Trace evidence

The fabric-primary's change span (`rdm.node_admin.fabric.update.via-change`) holds, in order,
`rdm.node_admin.fabric.update.via-every-mesh-ready`, `rdm.node_admin.fabric.update.via-state-entered`
(to ready-for-traffic) and `rdm.node_admin.fabric.update.via-on-fabric-ready`.
