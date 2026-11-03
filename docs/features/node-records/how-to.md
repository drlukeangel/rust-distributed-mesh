# Node records: integration

1. **Storage.** Register the same topology-storage backend in the admin and in every node
   executable. Its nodes boundary comes through the storage composition, with no separate
   file fallback ([provider how-to](../topology-storage-provider/how-to.md)). A backend that keeps
   its stores inside one process serves a single-process fabric only
   ([storage.md §2](../../architecture/storage.md)).
2. **Launch.** Pass the path and the name separately (`RDM_NODE_PATH`, `RDM_NODE_NAME`), and the
   type explicitly (`RDM_NODE_TYPE`). Keep the launch's node id and incarnation
   ([generated configuration](../../reference/rdm/config.md)).
3. **A node's own config.** A node changes its own config with `RunningNode::config_update`. The call
   answers once the node's record is durable. Nothing travels up through the primaries.
4. **A caller's config.** A caller's `node.config.update` travels down to the node under the call's
   one `Send`. The node applies it whether it is running or stopped. A node that does not answer
   ends the command `ConfigNotApplied` by name, and nothing is recorded for it (Q310).
5. **Builds.** A Build writes the config and tags of the nodes it creates, and of no other node.
6. **Reads.** `Nodes::get` reads the records:
   - A `(path, name)` pair resolves to a node id from the node records.
   - An `id` reads that node's record, Deleted included.
   - A node with no property record is listed with its properties unavailable, and no property pair
     matches it.

Examples executed by the fast gate, in `crates/rdm-test/tests/node_records.rs`:
- `a_nodes_config_update_stands_through_a_later_build`
- `a_node_with_no_connection_renamed_rejoins_its_mesh_under_the_new_name_keeping_its_identity`
- `a_deleted_birth_is_read_by_its_id`
- `all_node_fields_survive_node_restart`
- `a_start_preserves_a_newer_owed_update`
- `current_node_reads_survive_fabric_primary_restart`
