# Node records: integration

This is the i150 target integration checklist. It is not evidence that the pending runtime and
process acceptance is complete; consult the [work record](../../plans/i150-node-writes-lifecyle-updates.md).

1. Register the same topology-storage backend in the admin and every ordinary node executable.
   Supply its node-record boundary through the existing storage composition interfaces; do not
   introduce a separate file fallback. See the [provider how-to](../topology-storage-provider/how-to.md).
2. Pass path and name separately (`RDM_NODE_PATH`, `RDM_NODE_NAME`) and node kind explicitly
   (`RDM_NODE_KIND`). Preserve the launch's node ID and incarnation. Names do not encode kind or
   ownership. Path is immutable; a rename refuses a different path. Replacement changes only
   the predecessor name to `<name>-old`. See [generated configuration](../../reference/rdm/config.md).
3. Treat launch properties as initialization of an absent own record. Preserve retained properties
   and application recognition on startup; publish fresh bound facts through the same property writer.
4. Use the node's property-update API for local config changes. A caller's command still travels
   down through the primaries. Completion follows persistence; no config map travels up for copying.
5. Apply Build config and tags atomically with the Build's semantic application identity. Keep the
   existing exact-target and workflow receipts. Never treat a timeout, a missing receipt, or generic
   Ready as proof that an application did or did not occur.
6. Prepare lifecycle writing before installing a new writing role or admitting its commands. Wait
   for earlier issued writes of that writer birth to settle, including work retained after caller
   cancellation. Keep the existing election mechanism and timers.

Before release, the acceptance work must cover lost replies followed by later local updates,
stopped startup obligation A followed by B, same-writer role regain, provider migration, and the
full fast gate and exported flame at the final tip. Unit-test names alone do not certify these paths.
