# Mesh CPU Optimization Walkthrough

I have successfully implemented the `RAFKA_MDNS_ENABLE` toggle to address the $O(N^2)$ connection storm and committed the changes.

## What Was Accomplished

1. **mDNS Toggle**
   - Added a `RAFKA_MDNS_ENABLE` environment variable (defaults to `true` to preserve out-of-the-box local dev behavior).
   - Modified `rafka_mesh_transport::IrohMeshTransport::new` to accept a boolean flag to enable or disable mDNS entirely.
   - When disabled, the iroh node skips attaching the `MdnsAddressLookup` service to the QUIC endpoint and skips the background task that feeds local network discoveries to the peer registry.

2. **Admin UI Updates**
   - Updated the `rafka-admin-ui` observer to also respect `RAFKA_MDNS_ENABLE` when standing up its own `IrohMeshTransport` connection.

3. **Combined CPU Impact**
   - Paired with the 2,000ms gossip interval and the TRACE demote logging fix, disabling mDNS will keep your cluster's QUIC connection count bounded by explicit seeds. This solves the primary source of background CPU overhead and keeps the HyParView active-view limits intact.

## Next Steps

To verify this fix in your 18-node mesh test:
1. Export `RAFKA_MDNS_ENABLE=false` for all 18 nodes.
2. Ensure you provide explicit `--seed-nodes` (or the equivalent env var `RAFKA_SEED_NODES`) pointing to at least 1 or 2 nodes so the cluster can bootstrap.
3. Observe the total CPU usage during the "bootstrap-2-mesh" phase—it should dramatically drop from the 100% peg.
