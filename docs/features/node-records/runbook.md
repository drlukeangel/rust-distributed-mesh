# Node records: diagnosis

This runbook follows the [approved design](../../plans/proposal/node-record.md). i150 acceptance
is still tracked separately; consult its [work record](../../plans/i150-node-writes-lifecyle-updates.md)
before using a development tip as a certified release.

| Symptom | Evidence to inspect |
|---|---|
| Wrong properties after a Build retry | Exact target birth, semantic application identity, property record and workflow receipts; the marker must persist with config and tags. |
| A successor shows predecessor ports | Current path/name claim and successor's initial entry; recovery and replacement have different property sources. |
| A late primary changes observed lifecycle state | Installed election result and exact subject/writer-birth record; do not select the last-written record across writers. |
| New primary never admits commands | Preparation's retained outstanding writes and frozen takeover work, not a new role counter. |
| A stopped update disappears on startup | Identified deferred obligation and its actual application acknowledgement; acknowledgement of A must not clear B. |
| Provider migration waits | Every required node's freeze, existing command deadline and investigation/recovery; silence is not a freeze acknowledgement. |
| Properties revert after restart | Retained own record and applied identities versus the older launch environment. |

A stopped process remains alive with its RPC endpoint. An OS-paused or exited process cannot
answer freeze: use the existing investigation and recovery outcome before the mesh reports blocked.
Do not add a second timer, retry queue, or unresolved-investigation state.

Preserve both node-record kinds, application recognition and claim snapshots with the other five
storage boundaries. See the [provider runbook](../topology-storage-provider/runbook.md).
Record subjects, writer births, operations, backend and outcomes in evidence; never config values,
credentials or launch secrets. A transport timeout is an unknown outcome, not proof of absence.
