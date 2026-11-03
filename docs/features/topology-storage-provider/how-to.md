# Topology storage provider: how-to

How an application registers its backend, reads where RDM's records go, and moves them there. The
contract is [storage.md §2](../../architecture/storage.md#2-topology_storage_provider-where-rdms-records-go);
the parts and the failure paths are on the [overview](overview.md). Each example names the cell in
`crates/rdm-test/tests/fabric/topology_storage_provider.rs` that runs the same calls
(`scripts/cells.sh rdm-test main -- topology_storage_provider::`).

## 1. Register your provider in every executable

Every executable of the fabric registers each provider its nodes may write, by the same name: the
node-admin executable and each role node executable. A node moves only between providers its own
executable registers, and a name a launch names that its executable does not register refuses the
start by name.

The node-admin executable hands its providers to `rdm_admin::entry::run_on`. The testkit's, which every
cell runs (`crates/rdm-test/src/test_app.rs`, `crates/rdm-test/src/bin/faulted_node_admin.rs`):

```rust
use rdm_admin::storage_backend::{file_backend, TopologyStorageProviders};

fn providers(cfg: &rdm_admin::admin::AdminConfig) -> TopologyStorageProviders {
    // `file` on the fabric's store dir (RDM_STORE_DIR), every RDM binary's; then the application's
    // backend under its own name, bound to its own place.
    TopologyStorageProviders::file(cfg.store_dir.clone()).register("test-target", file_backend(my_place(cfg)))
}

#[tokio::main]
async fn main() {
    rdm_admin::entry::run_on("my-node-admin", providers, |cfg| my_wiring(cfg)).await
}
```

A role node registers the same names with `rdm_node::storage_backend::NodeStorageProviders` (the
testkit's node, `crates/rdm-test/src/node.rs`):

```rust
use rdm_node::storage_backend::{FileNodeBackend, NodeStorageProviders};

let providers = NodeStorageProviders::file()
    .register("test-target", std::sync::Arc::new(FileNodeBackend::at(my_place)));
```

An application's own backend supplies all six storage boundaries behind
`rdm_admin::storage_backend::StorageBackend` and `rdm_node::storage_backend::NodeStorageBackend`:
fabric, meshes, nodes, connections, Builds and events. Storage is persistence only: it stores and
returns what it is given and decides nothing (Q109). Two places of one backend are two registered names.

Cells: every cell of the stem runs these registrations.

## 2. Choose the provider on Day 0

The bootstrap admin reads `RDM_TOPOLOGY_STORAGE_PROVIDER` (`file` when unset; an empty value is refused
by name) and writes it on the Fabric row; every node it launches is written the provider it writes.

```sh
RDM_TOPOLOGY_STORAGE_PROVIDER=test-target rdm-node-admin
```

Cell: `an_omitted_provider_preserves_the_current_provider` (the fabric's records are on `test-target`
from Day 0, and the file provider's store dir holds nothing of it).

## 3. Read the fabric's provider and each node's

```rust
use rdm_client::{Fabrics, Meshes};

let fabric = Fabrics::new(carrier.clone()).get().await?;            // fabric.get
assert_eq!(fabric.topology_storage_provider.as_deref(), Some("file"));
assert!(fabric.previous_topology_storage_providers.is_empty());     // each provider the fabric left, in order

for node in Meshes::new(carrier.clone()).nodes("mesh1", None).await? { // mesh.nodes.get
    // The provider this node writes: the fact, recorded by RDM. During a change it differs
    // from the fabric's; that is the change's progress.
    println!("{} writes {:?}", node.name, node.topology_storage_provider);
}
```

`node.get` (`Nodes::get(&node_id)`) and `fabric.nodes.get` (`Fabrics::nodes(None)`) answer the same
field. `storage_persistence` is another property, on the same row
([storage.md §1](../../architecture/storage.md#1-storage_persistence-a-nodes-own-storage)).

Cell: `a_customer_reads_each_nodes_topology_provider`.

## 4. Move RDM's records to your provider

Send the new value to the fabric-primary with `fabric.storage.update`. The storage change is the
fabric-primary's own workflow, never a Build ([storage.md §2](../../architecture/storage.md)): it
answers the change's id once its `rdm.storage.started` event is written, and the change ends with its
`rdm.storage.ended` event.

```rust
use rdm_client::Fabrics;

// fabric.storage.update: the change's id, or a refusal by name (`build-in-progress` while a Build or
// another change runs, or a provider the fabric's wiring does not register or cannot open).
let change_id = Fabrics::new(carrier.clone()).update_storage("test-target").await?;
```

The fabric, every mesh and every node stay ready; no node restarts and no node changes
state. Once the change has ended, `fabric.get` names `test-target` with `file` previous, and every
node's row names `test-target`.

Cells: `a_customer_changes_topology_storage_without_restarting_members` (the change on a two-mesh fabric
with history, every line carried in source order), `every_serving_node_keeps_its_birth_and_ready_through_a_change`
(`node.get` of every node answers the same node id and incarnation, ready, before, while every
mesh-primary is blocked, and after).

### Your commands during a change

A command sent while a change is in progress is admitted; its mesh-primary issues it once it is
unblocked, and the row it writes lands on the provider the change decided. A `node.config.update`
during the change:

```rust
let rpc = rdm_client::NodeRpc::new(carrier.client());
// CallOptions::default() carries Send::default(): the one Send on every call.
rpc.config_update(&mesh_primary_id, &rdm_transport::RouteChoice::Direct, &node, config, &Default::default()).await?;
```

is answered, and the node applies the map only once its mesh-primary's `mesh.unblock` began. Cell:
`a_customer_changes_topology_storage_without_restarting_members`.

A node's own connection events never wait for the change (Q169). Cell:
`a_connection_event_during_a_change_lands_once_after_the_unblock`.

## 5. Leave the property out

A Build never carries the provider (build.md §1: it adds or removes nodes), so a Build leaves the
fabric's provider unchanged; it is never read as `file`. Naming the provider the fabric already writes
is no change either.

Nodes a Build creates write the fabric's current provider. Cells:
`an_omitted_provider_preserves_the_current_provider`, `a_previous_provider_remains_unchanged_after_cutover`
(a node created after the switch writes `test-target` alone).

## 6. Return to a previous provider

A later `fabric.storage.update` naming a provider the fabric left is a change like any other: it copies
into it only the lines it lacks, and the Fabric row then lists both previous providers in order.

```rust
let change_id = Fabrics::new(carrier.clone()).update_storage("file").await?;
```

After it, `fabric.get` names `file` with `["file", "test-target"]` previous. Cell:
`a_customer_returns_to_a_previous_topology_provider`.

## 7. Refusals you can meet

| what you did | how it ends | detail | cell |
|---|---|---|---|
| named a provider the fabric-primary does not register | `CallEnd::Rejected { reason: "topology-storage-provider-not-registered", .. }`; nothing accepted, nothing moved | names the provider and the ones the fabric-primary registers | `an_unsupported_provider_changes_nothing` |
| submitted any Build but `fabric.delete`, or another change (a change back included), while a change is in progress | `CallEnd::Rejected { reason: "build-in-progress", .. }`, the detail naming the change; nothing accepted | wait for the change's `rdm.storage.ended` event, then read again and send | `a_build_other_than_fabric_delete_is_refused_during_a_change` |
| the new provider holds lines the old one does not | the change ends with its `rdm.storage.ended` event `failed`, on the old provider; nothing switched, every node writes the old provider | the event's `reason`: `the new topology storage provider ... is not a copy of the old one ... in source order: ...` | `a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider` |
| started an executable whose launch names a provider it does not register | the start is refused by name | `RDM_TOPOLOGY_STORAGE_PROVIDER names topology storage provider ..., which this executable does not register (it registers: ...)` | `a_provider_is_opened_by_its_registered_name_and_an_unregistered_one_is_refused_by_name` (`crates/rdm-admin/src/storage_backend.rs`) |

`fabric.delete` is the one Build admitted during a change: before its teardown it reads the change's
`rdm.storage.committed` event, unblocks every mesh onto the provider the change decided and ends the
change `failed` (`interrupted by fabric.delete`), and keeps the records on both providers. Cell:
`a_delete_resolves_the_storage_change_before_teardown`.

When a change does not end the way you expect, the [runbook](runbook.md) says what to read.
