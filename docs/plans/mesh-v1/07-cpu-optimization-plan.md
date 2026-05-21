# Mesh CPU Optimization Analysis & Plan

Based on the metrics you provided and an analysis of the codebase, the high CPU usage during the "bootstrap-2-mesh" phase is a classic symptom of a P2P connection storm and aggressive background timers. 

Here is an analysis of why this happens and what the industry best practices recommend for addressing it:

## 1. The Root Cause: O(N²) Connection Scaling
In a cluster of 18 nodes, if every node discovers every other node via mDNS and forms a QUIC connection, you have 18 × 17 = 306 active QUIC connections cluster-wide. 
* **The QUIC Tax**: Each QUIC connection runs congestion control (BBR/Cubic), packet pacing, and TLS keepalives in the background. This scales linearly per node with the number of connections, consuming massive CPU even when no application data is flowing.
* **The Plumtree Fanout**: When a message is broadcast, Plumtree (the eager-push protocol in `iroh-gossip`) forwards it to peers in the active view. If the active view isn't heavily pruned, or if mDNS forces all peers into the active view, the broadcast fanout approaches $O(N^2)$, explaining why you saw 277,043 gossip events in 50 seconds.

## 2. Industry Best Practices

### A. Explicit Seeds over mDNS (Production Standard)
* **Best Practice**: Local broadcast discovery (mDNS) is excellent for zero-config developer setups but scales terribly in production. Industry mesh systems (like HashiCorp Serf/Consul, Libp2p) disable local broadcast in production. Instead, they use a small set of explicit seed nodes. The gossip protocol then disseminates peer information to form a scalable topology.
* **Impact**: Disabling mDNS and relying on `RAFKA_SEED_NODES` will slash your QUIC connection count from $O(N)$ per node to a bounded constant.

### B. Bounded Active Views (HyParView)
* **Best Practice**: The HyParView protocol is designed to keep the active connection count small and constant (typically 4 to 7 peers) regardless of cluster size. 
* **Code Issue**: I see in the code (`Feed mdns-discovered peers to gossip so the swarm forms`) that all mDNS peers are being fed directly into the gossip layer. This might be bypassing the HyParView active-view limits, forcing a full mesh.

### C. Relaxed Gossip Intervals
* **Best Practice**: 500ms is too aggressive for steady-state cluster state sharing. Most systems default to 1,000ms - 2,000ms. Keep the 2,000ms change we just made.

### D. Critical Path Logging
* **Best Practice**: Logging per-message events in a fanout tree at `INFO` level creates massive I/O contention. The TRACE demote was 100% correct and standard practice.

## Proposed Changes

I recommend a 3-step approach:

### 1. Toggleable mDNS (Highest Impact)
Introduce a new environment variable `RAFKA_MDNS_ENABLE` (defaulting to `true` for dev, but you will set it to `false` for your 18-node test).
#### [MODIFY] [lib.rs](file:///E:/dev/rafka-V2-new-mesh/crates/rafka-node-base/src/lib.rs)
* Add `mdns_enable` to the boot config.
* Conditionally skip the mDNS address lookup and `mdns.subscribe()` loop in `rafka-mesh-transport` and `lib.rs` based on this flag.

### 2. Keep the TRACE Demote
We already committed this. It should remain.

### 3. Keep the 2000ms Gossip Interval
We already committed this. It is a one-line change that will quarter the Plumtree periodic overhead.

## User Review Required

> [!IMPORTANT]
> Does introducing `RAFKA_MDNS_ENABLE` to conditionally disable mDNS align with your deployment strategy? If you disable mDNS, you must ensure that all nodes are given at least one valid address in `RAFKA_SEED_NODES` to form the mesh.
> 
> Let me know if you approve this approach and I will implement the mDNS toggle!
