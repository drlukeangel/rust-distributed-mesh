# Goal: Centralized Controller Architecture

The current architecture relies on decentralized, full-mesh gossip (via `iroh-gossip` and `mDNS`). As you've seen:
1. **With mDNS ON**: Nodes automatically discover everyone, creating a massive $O(N^2)$ full mesh of connections. This is resilient but extremely CPU heavy (like ZooKeeper).
2. **With mDNS OFF**: Nodes only connect to the 3 hardcoded `--seeds` provided by the Admin UI at boot. This causes the "fragmented topology" you are seeing, where some nodes only have 3 peers, while the Admin UI (which acts as a seed for everyone) has 18.
3. **The CPU Floor**: Even with pings disabled and sysinfo throttled, running 18 separate `tokio` runtimes maintaining peer-to-peer QUIC background tasks on a single Windows machine will always have a baseline CPU cost. 

To solve this fundamentally, we need to transition to a **Pulsar-style Centralized Controller** model.

## Proposed Changes

### 1. Introduce `Role::Controller`
- Update `Role` in `crates/rafka-node-base/src/lib.rs` to include a `Controller` variant.
- The `Controller` will act as the singular source of truth for cluster state and routing. 

### 2. Strip Decentralized Gossip from Data Nodes
- **Brokers, Compute, and Gateway nodes** will no longer run the complex `iroh-gossip` subsystem or `mDNS`.
- Data nodes will establish exactly **one** persistent control-plane connection to the `Controller`.
- This will drastically drop the CPU floor because the data nodes will no longer be maintaining a swarm of idle peer connections or processing network-wide state diffs.

### 3. Controller-Directed Traffic
- When a Gateway node needs to route a request, it will ask the `Controller` for the topology, rather than relying on its own fragmented gossip table.
- The `Controller` will aggregate heartbeats and push routing updates down to nodes only when necessary.

## User Review Required

> [!WARNING]
> This is a major architectural pivot. We will be ripping out `iroh-gossip` from the data nodes and replacing it with a centralized Hub-and-Spoke model. 

1. **Does this Hub-and-Spoke Controller model align with how you want the cluster to behave?**
2. **Shall we begin by implementing the `Controller` role and having the data nodes dial it exclusively?**
