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
| Executor | The fabric-primary executes each other mesh-primary's node.delete and shows its exit |
| Fabric-primary | Stays on its own mesh, because no other mesh is left to take it; that mesh.delete skips its [node.fabric-primary.update](node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) step |
| Completion | `fabric.deleted`, reported by the fabric-primary, still alive |
| After completion | A separate `node.delete` of the fabric-primary, called from outside the fabric; its `node.deleted` is its last reply and the caller takes its word |
| Step events | Every runtime the workflow deletes has its node.delete step event. The fabric-primary's own delete is the one exception: nobody is left in the fabric to record it |

## Numbered end-to-end sequence

1. Run [mesh.delete](mesh-delete.md) on each mesh, the fabric-primary's own mesh last. Each
   mesh.delete keeps its mesh-primary alive until every other member is deleted and it has
   reported mesh.deleted. On its own mesh the fabric-primary stays, because no other
   mesh is left to take it, so that mesh.delete skips its
   [node.fabric-primary.update](node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) step. A mesh whose mesh-primary does not answer its mesh.delete is reached the way an unheard mesh-primary is reached in a live fabric ([node investigation](node-investigation.md): its exact birth, direct then via peers of its mesh). A lost mesh-primary is re-created at its path and name, as without a delete, and it runs its mesh's [mesh.delete](mesh-delete.md) (Q370). Nothing of the fabric's topology is left behind.
2. After each other mesh's mesh.deleted, run [node.delete](node-delete.md) of that mesh's
   mesh-primary. The fabric-primary is the executor and shows its exit.
3. The fabric-primary, still alive, reports fabric.deleted. The primary runs the workflow
   needs are retained until it completes ([Elections](../fabric-node-lifecycle-elections.md)).
4. A separate call from outside the fabric runs node.delete of the fabric-primary. Its
   node.deleted is its last reply, and the caller takes its word, because nobody is left in
   the fabric to record it. This is the one exception to the step event.

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
                         fabric-primary is the executor and shows its exit
    < fabric.deleted     reported by the fabric-primary, still alive
then a separate call, from outside the fabric:
node.delete              the fabric-primary
    < node.deleted       its last reply; the caller takes its word, because nobody is left
                         in the fabric to show the exit
```

## States written

This operation writes no fabric state. Each mesh's states are written by [mesh.delete](mesh-delete.md) and each node's by [node.delete](node-delete.md). Every runtime the workflow deletes has its step event, the fabric-primary's own delete excepted.

| written | by | when |
|---|---|---|
| `rdm.build.step` in `events.jsonl`, `state` `complete` | the executor of the step | the step completes |

As stored:

`rdm.build.step`, a node.delete step:

```json
{"len":488,"specversion":"1.0","id":"01a1ae8b-fcdd-7cc8-b8fb-a19e908a520c","source":"rdm://s0artk0pkmpp/bd7ebr4x2mng/zd4y41k3zta5","type":"rdm.build.step","subject":"h72998zyjysr","time":"2026-11-06T01:49:32.251Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-55bd55356d44f87c520f93b0bbfd3212-dbbf65d237c50ce6-01","data":{"build_id":"erskwynp44gw","mesh":"mesh2","op":"node.delete:h72998zyjysr","state":"complete","step":"NodeDeleting"},"crc32":"bde91f70"}
```

## Failure

A step that cannot complete ends its mesh's section, and the fabric.delete Build ends `incomplete`,
naming that step (Q346); no fabric.deleted follows. Completion of steps 1 to 3 rests on each deleted runtime's exact
its node.delete step event: a runtime inside the workflow whose delete has no `complete` step event is never
reported deleted. A node the fabric-primary cannot reach, directly or through a node-admin, and
cannot show exited is named in the Build's `incomplete` section.

A fabric-primary lost mid-delete is fabric recovery's: fabric recovery re-creates it at its original path and name, and the restored fabric-primary continues the delete (Q370) ([fabric recovery](fabric-recovery.md) step 4, Q155); a mesh
left with no node-admin is deleted in process at the executor, as any mesh with no mesh-primary is.
Once a runtime's node.delete step event is durable, nothing recovers it for being absent.
