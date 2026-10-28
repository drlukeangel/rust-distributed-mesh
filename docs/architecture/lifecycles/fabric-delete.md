# Fabric delete

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm)

`fabric.delete` removes every mesh of the fabric, leaf-inward, the fabric-primary's own mesh
last. It is a workflow: one call that the fabric-primary runs as one Build through the
[Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm). It completes with
`fabric.deleted`, reported by the fabric-primary while it is still alive. The fabric-primary
itself is then deleted by a separate `node.delete`, called from outside the fabric.

## Operation contract

| Field | Contract |
|---|---|
| Caller | Typed caller |
| Precondition | None on the fabric's state: admitted before the first ready, and while a Build is in progress, which it interrupts as [build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine) states |
| Driver | The fabric-primary, one Build |
| Steps | [mesh.delete](mesh-delete.md) of each mesh, the fabric-primary's own mesh last; then [node.delete](node-delete.md) of each other mesh's mesh-primary, after its mesh.deleted |
| Executor | The fabric-primary executes each other mesh-primary's node.delete and proves its exit |
| Fabric-primary | Stays on its own mesh, because no other mesh is left to take it; that mesh.delete skips its [node.fabric-primary.update](node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) step |
| Completion | `fabric.deleted`, reported by the fabric-primary, still alive |
| After completion | A separate `node.delete` of the fabric-primary, called from outside the fabric; its `node.deleted` is its last reply and the caller takes its word |
| Receipts | Every runtime the workflow deletes has its node.delete receipt. The fabric-primary's own delete is the one exception: nobody is left in the fabric to record it |

## Numbered end-to-end sequence

1. Run [mesh.delete](mesh-delete.md) on each mesh, the fabric-primary's own mesh last. Each
   mesh.delete keeps its mesh-primary alive until every other member is deleted and it has
   reported mesh.deleted. On its own mesh the fabric-primary stays, because no other
   mesh is left to take it, so that mesh.delete skips its
   [node.fabric-primary.update](node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) step. A mesh whose mesh-primary does not
   answer is reached through [mesh investigation](mesh-investigation.md#during-fabricdelete),
   whose outcome during fabric.delete is the delete.
2. After each other mesh's mesh.deleted, run [node.delete](node-delete.md) of that mesh's
   mesh-primary. The fabric-primary is the executor and proves its exit.
3. The fabric-primary, still alive, reports fabric.deleted. The primary runs the workflow
   needs are retained until it completes ([Elections](../fabric-node-lifecycle-elections.md)).
4. A separate call from outside the fabric runs node.delete of the fabric-primary. Its
   node.deleted is its last reply, and the caller takes its word, because nobody is left in
   the fabric to record it. This is the one exception to the receipt.

A local OS signal or a node stop is node lifecycle and starts no fabric.delete.

## Command and completion messages

```text
fabric.delete
  > mesh.delete          each mesh, the fabric-primary's own mesh last; on its own mesh the
                         the fabric-primary stays (no other mesh is left to take it), so that
                         mesh.delete skips its node.fabric-primary.update step
                         a mesh-primary that does not answer: reached through mesh
                         investigation, whose outcome here is the delete
  > node.delete          each other mesh's mesh-primary, after its mesh.deleted; the
                         fabric-primary is the executor and proves its exit
    < fabric.deleted     reported by the fabric-primary, still alive
then a separate call, from outside the fabric:
node.delete              the fabric-primary
    < node.deleted       its last reply; the caller takes its word, because nobody is left
                         in the fabric to prove the exit
```

## Failure

A step that cannot complete ends the workflow `Failed { step, reason }`, naming that step, and
no fabric.deleted follows. Completion of steps 1 to 3 rests on each deleted runtime's exact
its node.delete receipt: a runtime inside the workflow whose delete has no receipt is never
reported deleted. A node the fabric-primary cannot reach, directly or through a node-admin, and
cannot prove exited is named in `Failed { step, reason }`.

A fabric-primary lost mid-delete is fabric recovery's: its executor continues the delete as
fabric-primary and re-creates nothing ([fabric recovery](fabric-recovery.md) step 4, Q155); a mesh
left with no node-admin is deleted in process at the executor, as any mesh with no mesh-primary is.
Once a runtime's deletion receipt is durable, nothing recovers it for being absent.
