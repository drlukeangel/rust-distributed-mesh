# Provider runbook: four facts a mesh-primary records about a member's runtime

When a member goes silent, its mesh-primary asks the deployment provider about the member's exact
runtime: the process or container recorded at launch. The answer can leave one of four durable facts
on the member's birth:

- `os-stopped`
- `held-running`
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
- [deployment-provider.md §2.5](../architecture/deployment-provider.md#25-runtime-state-what-the-provider-reports);
- [node-delete.md](../architecture/lifecycles/node-delete.md), "OS-suspended target".

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
  | `RDM_HUNG_HOLD_FLOORS` | `10` | How long `held-running` is held before RDM terminates the runtime (300 s at the default floor). |
  | `RDM_DATA_DIR_GONE_FLOORS` | `2` | How long an absent data dir must stay absent before it is `data-dir-lost` (60 s). |

- **The inspection span.** Every inspection that finds a member's verdict for the first time, or
  finds it changed, is spanned as `rdm.node_admin.node.resolve.via-provider-inspection`. Its
  attributes:
  - `node`, `node_id`, `incarnation_id`;
  - `inspector`, the admin that inspected;
  - `verdict`: `exited`, `running`, `os-stopped` or `unavailable`;
  - `exit_code`, `exit_proof`.

  A verdict that stands is inspected again on every tick but is not spanned again.

## `os-stopped`: the operating system holds the runtime suspended

**What it is.** The member's exact runtime is alive but held by the operating system:
- the process provider reads its thread group stopped (state `T`, from `SIGSTOP`, a debugger or a
  cgroup freezer);
- the container provider reads the container `paused`.

A suspended runtime answers nothing, so it is silent. It is not hung and not dead.

**`node.get`.** The birth carries `os_stopped: { since, inspections }`:
- `since` is when it was first recorded;
- `inspections` counts the inspections that confirmed it.

`held_running` is null.

**Jaeger.**
- `rdm.node_admin.node.resolve.via-provider-inspection` with `verdict = os-stopped`.
- `rdm.node_admin.node.update.via-os-stopped`, with `outcome`:
  - `recorded`: first written;
  - `revalidated`: a new mesh-primary confirmed a row another admin wrote, by its own inspection;
  - `cleared`: removed, with `detail` saying why:
    - `returned`: its digest came back;
    - `reached`: it answered its tickle;
    - `running`: an inspection read it running;
    - `exited`: an inspection proved its exit;
    - `deleted`: an explicit delete removed it.
  - `refused`: the row could not be read or written. The next tick tries again.
- `rdm.node_admin.node.update.via-held-running` with `outcome = cancelled-os-stopped`: a hold that was
  already open is cancelled, so its terminate is void.
- On an explicit delete:
  - `rdm.node_admin.node.delete.via-os-stopped` with `skipped = StopNode AwaitNodeLeft`;
  - then the delete's `TerminateRuntime` step (`rdm.node_admin.deployment.update.via-step`), as
    `Immediate`: no graceful wait;
  - then `NodeDeleted`.

**What RDM does.** It records the fact and inspects the runtime on every tick. It does not:
- open a hold;
- terminate the runtime;
- start a recovery;
- resume the runtime.

The fact ends only when:
- the runtime runs again (`cleared`);
- it exits (`cleared` with `exited`), after which ordinary recovery follows: a new `node.id` at the
  same `path.name`;
- a person deletes it.

**What a person does.**
1. Find who suspended it. Look for:
   - a debugger attached to the pid;
   - a cgroup freezer;
   - a `docker pause`;
   - a `kill -STOP` in a script.
2. Then decide:
   - **Resume it.** Run `kill -CONT <pid>` or `docker unpause <id>`. The next tick reads it running, or
     its digest returns, and the fact clears. If it runs but stays unheard, the `held-running` rule
     applies.
   - **Remove it.** `node.delete` of the member. The delete skips the stop by name, terminates the exact
     runtime immediately and writes `NodeDeleted`. Nothing of the runtime is resumed.
   - **End it.** Kill the runtime (`kill -KILL <pid>`, `docker kill <id>`). The next inspection proves
     the exit, and RDM recovers the member as a new birth.
3. Open finding (i144 e4): with the process provider, if the admin that launched a suspended member
   dies, the kernel ends the member too. Its process group is orphaned, and the kernel sends `SIGHUP`
   then `SIGCONT`. Until that finding is closed, do not kill the launching admin of a member you mean
   to keep suspended.

## `held-running`: silent, yet the provider reads the runtime running

**What it is.** The member has been unheard past TRUE OFFLINE, but the provider reports its exact
runtime `Running`. Typical causes:
- a hung process (deadlock, a blocked event loop, swap);
- a network cut that leaves the process alive.

**`node.get`.** The birth carries `held_running: { since, verdicts }`:
- `since` is when the hold began;
- `verdicts` counts the `Running` verdicts held.

For a hung fabric-primary, `held_running` is also on `fabric.get`.

**Jaeger.**
- `rdm.node_admin.node.resolve.via-provider-inspection` with `verdict = running`.
- `rdm.node_admin.node.update.via-held-running`, with `recorder`, `since`, `verdicts` and `outcome`:
  - `recorded`: each tick of the hold;
  - `voided`: a terminate failed at its own inspection, because the runtime read `os-stopped` or
    `unavailable` at that instant. A fresh hold begins if it still reads running.
- When the hold expires:
  - `rdm.node_admin.build.update.via-node-recovery` (the submit), for the recovery Build;
  - its first step, `TerminateRuntime`, which inspects the runtime again immediately before it asks
    the provider to terminate.
- `rdm.node_admin.build.reject.via-terminate-refused`, with `stage` and `error`: the provider refused
  the terminate or could not prove it. No hold submits that birth's terminate again.
- `rdm.node_admin.build.reject.via-terminate-receipts-unreadable`: the birth's earlier terminates could
  not be read, so nothing is submitted this tick.

**What RDM does.** It holds for `RDM_HUNG_HOLD_FLOORS` floors and keeps running the ladder every tick.
- If the member answers or its digest returns, the hold ends and the row clears. No action follows.
- If the hold expires with the runtime still `Running`, RDM submits the recovery Build. It:
  1. terminates the exact runtime gracefully, then forced after the stop grace;
  2. records the exit proof;
  3. re-births the member at its `path.name` under a new `node.id`.

**What a person does.**
- **During the hold.** Look at the runtime:
  - its threads (`/proc/<pid>/task/*/stat`, `top -H -p <pid>`);
  - its logs (`<data_dir>/stderr.log`);
  - its network path.

  If it recovers by itself, nothing more is needed. If you want it gone sooner, delete the member, or
  kill the runtime and let recovery follow.
- **After `via-terminate-refused`.** RDM has stopped trying.
  1. End the runtime by hand: `kill -KILL <pid>` on the exact pid in `<data_dir>/deployment.json`, or
     `docker kill <id>`.
  2. The next inspection reads it exited, and ordinary recovery follows.
  3. If the provider itself is broken (no permission to signal, a daemon down), repair that first.

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
- with `before_ready` told `inherited = false`.

For a lost mesh's node-admin, the fabric-primary runs this arm from the fabric lane, without the wait.

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
