# Q420–Q424 page text (for Luke's yes)

## 1. lifecycles/node-investigation.md, opening (lines 5–7)
OLD: A primitive. Node investigation is the one function every caller uses on one exact birth, ending in one verdict: `reached`, `running`, `os-stopped` or `exited`; a birth not proven by the end of its retries is `dead` (Q393). Its steps, in order:
NEW: A primitive. Node investigation is the one function every caller uses on one exact birth, ending in one result for its client (Q420–Q423): `ready` (the birth answered: it was ready the whole time), `rejoined` (the investigation brought it back: commanded back through node.mesh.join, or started) or `re-created` (it could not be brought back: written `dead`, and [node recovery](node-recovery.md) deletes it and creates it again at its path and name with a new node.id). node.runtime.inspect's answers (`running`, `os-stopped`, `exited`, `unavailable`) are steps inside it, never a result. Its steps, in order:

## 2. node-investigation.md, step 2 (line 11)
OLD: 2. [node.runtime.inspect](node-runtime-inspect.md). `exited` or `os-stopped` is the verdict; no tickle is sent.
NEW: 2. [node.runtime.inspect](node-runtime-inspect.md). On `exited` or `os-stopped` no tickle is sent: an `exited` birth is written `dead`; an `os-stopped` one is commanded to start (step 5).

## 3. node-investigation.md, step 4 (lines 13–15), and a new step 5
OLD: 4. The same tickle via those peers. Unreached, the verdict is the inspect's answer: `running`; an inspect that answers `unavailable` is asked again under the investigation's retries, and a birth still not proven at their end is `dead`.
NEW: 4. The same tickle via those peers. Unreached, a `running` birth is commanded back through [node.mesh.join](node-mesh-join.md) (step 5); an inspect that answers `unavailable` is asked again under the investigation's retries, and a birth still not proven at their end is written `dead`.
5. The command back or the start, each under its `Send`: answered, the investigation ends `rejoined`; unanswered at the end of its attempts, the birth is written `dead`. A birth written `dead` ends it `re-created`.

## 4. node-investigation.md, line 17
OLD: Only an answer from the exact birth counts as `reached`.
NEW: Only an answer from the exact birth counts: to a tickle, the investigation ends `ready`; to the command back or the start, `rejoined`.

## 5. node-investigation.md, the decision paragraph (lines 28–29)
OLD: The verdict is the one decision (Q111). The primary that investigated writes it as one `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, verdict and what follows ([build.md §5](../build.md#5-buildstateadapter)); an investigation is never an entity of its own. Reached: the node is commanded back through [node.mesh.join](node-mesh-join.md). Any other verdict follows the investigation's reason ([node recovery](node-recovery.md) step 3, Q393); a birth written `dead` is re-created at the same path and name with a new node.id (Q110). `running` from an investigator that hears no other node (every other birth it holds is unheard past the floor, so it is the one cut off) is unreachable from that investigator, not lost: nothing is written, and the next investigation decides again (Q153). Fabric recovery's backbone arm excepts `running` ([fabric recovery](fabric-recovery.md) step 3). A wrong verdict is fixed in the investigation.
NEW: The result is the one decision (Q111). The primary that investigated writes it as one `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, result and the inspect's answer that led to it ([build.md §5](../build.md#5-buildstateadapter)); an investigation is never an entity of its own. `ready`: a held birth off its mesh is commanded back through [node.mesh.join](node-mesh-join.md) for its view (Q87), and is still `ready`. `re-created`: [node recovery](node-recovery.md) deletes the birth and creates it again at the same path and name with a new node.id (Q110). An investigator that hears no other node (every other birth it holds is unheard past the floor, so it is the one cut off) ends with no result: nothing is written, and the next investigation decides (Q153). Fabric recovery's backbone arm excepts that case ([fabric recovery](fabric-recovery.md) step 3). A wrong result is fixed in the investigation.

## 6. node-investigation.md, contract table rows (lines 60–61, 70)
OLD: | Result | `reached`, `running`, `os-stopped` or `exited`, or `dead` when not proven by the end of the retries |
NEW: | Result | `ready`, `rejoined` or `re-created` (Q420–Q423); none from an investigator that hears no other node (Q153) |
OLD: | What follows | Reached: node.mesh.join. Any other verdict: [node recovery](node-recovery.md) step 3 (Q393); a birth written `dead` (except `running` from an investigator that hears no other node, Q153) is re-created at the same path and name with a new node.id, over the predecessor's data dir when the verdict is `exited`, its storage `persistent` and the directory there, over a newly allocated one otherwise |
NEW: | What follows | `ready` and `rejoined`: nothing more from RDM; `on_node_investigated` tells the application. `re-created`: [node recovery](node-recovery.md), over the predecessor's data dir when the inspect answered `exited`, its storage `persistent` and the directory there, over a newly allocated one otherwise |
OLD: | `rdm.node.investigated` | the primary that investigated | the verdict is decided, except `running` from an investigator that hears no other node, which writes nothing |
NEW: | `rdm.node.investigated` | the primary that investigated | the result is decided; an investigator that hears no other node writes nothing |

## 7. lifecycles/node-recovery.md, step 3's opening (line 45)
OLD: 3. **The verdict is the one decision (Q111).** `reached`: a held birth that answered, …
NEW: 3. **The result is the one decision (Q111; [node investigation](node-investigation.md), Q420–Q423).** `ready`: a held birth that answered, …
(The rest of step 3 keeps its mechanics; every later "verdict" in it becomes "the inspect's answer" where it names `running`/`os-stopped`/`exited`, and the sentence "`exited`, a birth not proven by the end of the retries, or a join or start unanswered at the end of its attempts is written `dead`, and the recovery below runs at once" gains: "The investigation then ends `re-created`; a command back or start that is answered ends it `rejoined`.")

## 8. node-recovery.md, sequence step 1 (line 66)
OLD: 1. The investigation ends in a verdict other than `reached`, and the mesh-primary …
NEW: 1. The investigation ends `re-created`, and the mesh-primary …

## 9. fabric-node-hooks.md, on_node_investigated (line 171)
OLD: … with `NodeInvestigatedCtx { fabric_id, mesh_id, node_id, incarnation, path, name, verdict, next, context }`, on a task of its own. `verdict` is the investigation's; `next` is what RDM does with the birth: `rejoin` (commanded back through node.mesh.join) or `recreate` (re-created at the same path and name with a new node.id). The notice gates nothing in RDM and orders nothing: a re-create may be under way when the application's hook runs.
NEW: … with `NodeInvestigatedCtx { fabric_id, mesh_id, node_id, incarnation, path, name, result, inspected, context }`, on a task of its own. `result` is the investigation's: `ready`, `rejoined` or `re-created` (Q420–Q423); `inspected` is node.runtime.inspect's answer that led to it. A `rejoined` node was out of reach or stopped by no command and missed what happened meanwhile: the application re-syncs it from this notice (Q422, Q424). The notice gates nothing in RDM and orders nothing: a re-create may be under way when the application's hook runs.
