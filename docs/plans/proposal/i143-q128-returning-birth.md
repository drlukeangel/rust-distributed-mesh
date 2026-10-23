# Q128: a re-created node's old birth that comes back (task #34)

## What happens today (cell `node_replace::a_superseded_birth_that_comes_back_is_observed`, lane e6s8, branch `superseded-birth`)

`mesh1.rpc.2` held (SIGSTOP) until its mesh-primary re-creates it (`h56t8ksepm7c` -> `zxxbr03sgvws`), then continued:

- What refuses it: gossip membership. Every node's book drops its digests because it applied that birth's
  NodeDeleted (`rdm.mesh.membership.reject.via-departed-birth`, 132 spans in 10 s). No view lists the old
  node id again; the path keeps one live birth, the re-created one.
- Nothing refuses it at node.connections.admit or node.mesh.join: no `superseded-birth` reason exists in
  the code, and node-connections-admit.md's reasons do not list one.
- Its runtime is not terminated: the verdict was `os-stopped`, node-recovery.md step 3 terminates only on
  `running`, and nothing acts on the old birth after it comes back. It runs until stopped from outside.

## Arm A (recommended): the pages say what refuses it; an os-stopped runtime is terminated too

Code: the delete's `TerminateRuntime` also fires on `os-stopped`, `Immediate` (a stop would thaw it,
deployment-provider.md §2.7). Exact text:

node-recovery.md:65-66
- old: `(Q110, Q111). A birth that comes back after it was re-created is refused at admission`
  `(\`superseded-birth\`), reported to the fabric-primary, and its runtime terminated.`
- new: `(Q110, Q111). A birth that comes back after it was re-created is refused by membership: every`
  `node's book drops the digests of a birth whose NodeDeleted it applied, so no view lists it and nothing`
  `is routed to it.`

node-recovery.md:77
- old: `no RPC to the old process and no stop frames. When the verdict was \`running\`, the first step is`
- new: `no RPC to the old process and no stop frames. When the verdict was \`running\` or \`os-stopped\`, the first step is`
  (and after "the stop grace": `; an os-stopped runtime is terminated \`Immediate\`, since a stop would thaw it`)

node-investigation.md:26-28
- old: `A node that comes back after it was re-created is refused at`
  `[node.connections.admit](node-connections-admit.md) (\`superseded-birth\`). The node that refused it`
  `reports it to the fabric-primary, which terminates its runtime.`
- new: `A node that comes back after it was re-created is refused by membership: every node's book drops the`
  `digests of a birth whose NodeDeleted it applied (gossip.md).`

fabric-recovery.md:59-60, 164, 182: `refused at admission ([node.mesh.join](node-mesh-join.md), \`superseded-birth\`) and its runtime is terminated (Q110)`
and `refused at admission and terminated` / `refused at admission and its runtime terminated (Q110)`
- new, each: `refused by membership (its NodeDeleted applied)`.

## Arm B (no code): the pages say what refuses it; nothing terminates a returning runtime

The same text as A for the refusal; node-recovery.md:77 unchanged. An os-stopped or uninspectable old
runtime keeps running, refused, until stopped from outside.
