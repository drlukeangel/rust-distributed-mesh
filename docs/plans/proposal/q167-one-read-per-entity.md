# Proposal: one read per entity, plural, one mechanism (Q167)

## The question

Why would RDM have a singular read and a plural read of the same thing? Today (after Q165) it has
both: `node.get { id }` and `fabric.nodes.get`/`mesh.nodes.get`, `mesh.get { name }` and
`fabric.meshes.get`. Each pair is the same wire request (`GetNodes`, `GetMeshes`) with a different
selector, two client methods, two spans, two page rows and two answers for "not there"
(`NOT_FOUND` vs an empty list). The second name adds surface and no capability.

## The answer: there is no reason

A read of one is a read of the list filtered to one. One read per entity covers both:

| Read | Filters (all optional, combined with AND) | Returns |
|---|---|---|
| `builds.get` | `ids`, `current` | Builds, newest first: each with its id, state and, for every node, its `NodeSpec` (executable, storage, placement, tags, config); the current Build marked |
| `fabrics.get` | `ids` | fabrics: id, name, state, provider, fabric-primary |
| `meshes.get` | `names`, `fabric` | meshes |
| `nodes.get` | `ids`, `mesh`, `tags` (all or any) | node rows; no filter: every node of the fabric, its topology |

One mechanism for all four: the same filter shape, a list in a stable order, an empty list when
nothing matches. "One node" is `nodes.get { ids: [id] }`. Builds are their own entity, outside the
fabric: the fabric is an artifact of the Builds.

## Why

- **One mechanism.** One request shape, one client method, one span name and one page row per entity.
  There is no second path to keep consistent with the first.
- **Precedent on our own pages.** `node.tags.create { label | [labels] }`: "One item or a list,
  uniform: a single item is the same as a list of one" (node-rpc-envelope.md). `node.connections.delete`
  takes `All | [ids]` the same way. Reads should follow the same rule.
- **Industry practice.** Kubernetes reads one or many by selector (`kubectl get nodes`,
  `-l`, `--field-selector`, a name). AWS `Describe*` calls take `Ids`/`Filters` and always return a list.
  Google AIP-131/132 split Get and List; this proposal does not, because it costs the
  second path and RDM's `NOT_FOUND` for a read adds nothing an empty list does not already say.
- **No lost capability.** A caller that needs exactly one checks the list's length. A caller that edits
  the current Build reads `builds.get { current: true }` and gets every node's full `NodeSpec`, tags and
  config included, so a read, edit and resubmit never clears them.

## What changes

| Today | After |
|---|---|
| `node.get { id }`, `mesh.nodes.get`, `fabric.nodes.get` | `nodes.get { ids?, mesh?, tags? }` |
| `mesh.get { name }`, `fabric.meshes.get` | `meshes.get { names?, fabric? }` |
| `fabric.get` | `fabrics.get { ids? }` |
| `build.get { id }` (receipts), `FetchCurrentBuild` (no tags, config or executable) | `builds.get { ids?, current? }`, each with its full `NodeSpec`s |

- **Wire.** The existing requests carry the filters: `GetNodes { select }`, `GetMeshes`, `GetFabric`.
  A filter they cannot express is a variant appended last; no variant moves. `builds.get` reads the
  fabric-primary's store over Node RPC; nothing new rides gossip (rule 7: executables, tags and
  config never do).
- **Client.** One method per entity in rdm-client (`Nodes::get(filter)`, `Meshes::get(filter)`,
  `Fabrics::get(filter)`, `Builds::get(filter)`); the singular forms and the nested
  `Fabrics::nodes`/`Fabrics::meshes`/`Meshes::nodes` go.
- **Spans.** `rdm.node_admin.<entities>.get.via-call`, one per read.
- **Changes stay as they are.** A change names what it acts on (`node.stop`, `node.config.update`,
  `mesh.delete`, `build.create`); this proposal is about reads only.
- **Pages.** node-rpc-envelope.md's read rows, build.md §2, build-deployment.md §5 and the consumer
  guide's reads table state the four reads. Q165's table and Q167's singular/plural row are
  superseded.

## Arms

- **A (recommended): one plural read per entity**, as above.
- **B: keep a singular and a plural read per entity** (Q165/Q167 as recorded): two calls, two
  "not there" answers, two spans per entity.
- **X: something else.**
