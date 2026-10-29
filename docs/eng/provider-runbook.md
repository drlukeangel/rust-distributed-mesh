# Provider runbook: two facts a mesh-primary records about a member's data dir

When a member goes silent, its mesh-primary asks the deployment provider about the member's exact
runtime: the process or container recorded at launch. Any verdict but `reached` recovers the member at
once ([node-recovery.md](../architecture/lifecycles/node-recovery.md) step 3, Q111): `TerminateRuntime`
first when the runtime runs, then a new birth at the same `path.name`, over the inherited data dir when
the runtime exited and a newly allocated one otherwise. When it exited, the recovery can leave one of two
durable facts on the member's birth:

- `data-dir-lost`
- `data-dir-unreadable`

Each fact is listed on `node.get`. None is ever sent as a gossip `MemberStatus`.

This page says, for each fact:
- what `node.get` shows;
- which spans show it in Jaeger;
- what RDM does about it on its own;
- what a person does about it.

The contract is in these pages; this runbook only reads them for the person on call:
- [node-recovery.md](../architecture/lifecycles/node-recovery.md), steps 3-4, and its data-dir arms;
- [deployment-provider.md §2.5](../architecture/deployment-provider.md#25-runtime-state-what-the-provider-reports).

## Before you start

- **Ask the admin.** `node.get` of the member's mesh answers from the admin you ask. Ask the
  mesh's primary (`is_primary` on its `node_admin`): the primary writes and removes these rows. Another
  admin of the mesh lists the same rows but acts on none of them.
- **Expect a dead status.** A member with one of these facts is silent. Its `status` is usually
  `dead` or `pending-reconnect`, which only means "unheard". The fact on the birth says why.
- **Searching Jaeger.**
  - The spans named below are the mesh-primary's.
  - Filter on the span name, then on `node` (the member's `path.name`) or `node_id`.
  - `inspector` and `recorder` name the admin that inspected or recorded.
- **Cadence.** All the windows below count in staleness floors (`RDM_STALENESS_MS`, default 30 s):

  | Env var | Default | What it sets |
  |---|---|---|
  | `RDM_STALENESS_MS` | `30000` | One floor. A member unheard for about two floors is TRUE OFFLINE and inspected. |
  | `RDM_DATA_DIR_GONE_FLOORS` | `2` | How long an absent data dir must stay absent before it is `data-dir-lost` (60 s). |

- **The inspection span.** Every inspection that finds a member's verdict for the first time, or
  finds it changed, is spanned as `rdm.node_admin.node.resolve.via-provider-inspection`. Its
  attributes:
  - `node`, `node_id`, `incarnation_id`;
  - `inspector`, the admin that inspected;
  - `verdict`: `exited`, `running`, `os-stopped` or `unavailable`;
  - `exit_code`, `reason`.

  A verdict that stands is inspected again on every tick but is not spanned again.

## `data-dir-lost`: the exited member's data dir is gone

**What it is.** The member exited, and the data dir it would be recovered over (its `data_dir`) is
absent: `NotFound` on the directory itself. A gone directory is decided from that filesystem error,
never guessed.

**`node.get`.** The birth carries `data_dir_lost: { path, since }`. The fact is indexed by `path.name`:
a member recovered over a fresh dir still lists the loss it was recovered over.

**Jaeger.**
- `rdm.node_admin.node.update.via-data-dir-lost`, with `node`, `node_id`, `incarnation_id`, `path`
  and `since`.
- Then the recovery submit, `rdm.node_admin.build.update.via-node-recovery`.
- The new birth starts with `inherited = false` (`RDM_INHERITED_DATA_DIR=0` in its environment).

**What RDM does.** It waits `RDM_DATA_DIR_GONE_FLOORS` floors, to ride out a remount, then recovers the
member at the same `path.name`:
- over a newly allocated `data_dir` beside the old path;
- with a newly minted endpoint key (the old key is never minted again);
- with `pre_node_ready` told `inherited = false`.

For a lost mesh's node-admin, the fabric-primary runs this arm itself, without the wait.

**What a person does.**
1. Find why the directory vanished: an unmounted volume, a deleted path, a host moved under the node.
2. The new birth starts with nothing of the old one's local state. If the application keeps state
   there, restore it through the application's own means.
3. If the old volume comes back, RDM does not use it. The new birth owns its own dir. Remove the old
   one by hand once you no longer need it.

## `data-dir-unreadable`: the exited member's data dir is there but broken

**What it is.** The member exited, and its `data_dir` exists, but its `node-key` cannot be read:
- `NotFound` on `node-key` inside the existing directory;
- `EIO`;
- `EACCES`;
- a key that does not decode.

**`node.get`.** The birth carries `data_dir_unreadable: { path, cause }`.

**Jaeger.**
- `rdm.node_admin.node.reject.via-data-dir-unreadable`, with `path` and `cause`, on the first
  `PrepareStorage` failure of its recovery.
- `rdm.node_admin.node.resolve.via-data-dir-repaired` once the key reads again.

**What RDM does.**
- It holds the member's recovery: the recovery is not due while the fact stands, so it is not
  resubmitted every tick.
- It deletes nothing on disk.
- When `<data_dir>/node-key` reads again, the next tick clears the fact and recovers the member over
  the kept dir with its retained endpoint key (`inherited = true`).

**What a person does.**
1. Repair the key, using `cause`:
   - fix permissions (`EACCES`);
   - fix the disk or the mount (`EIO`);
   - restore `node-key` from a backup of the same birth (`NotFound`, or a key that does not decode).
2. The next tick clears the fact and recovery completes.
3. If the key cannot be repaired, the choice is a person's: remove the member with `node.delete`.
   Removing the directory does not turn this fact into `data-dir-lost`: the hold stands while the
   unreadable row does.
