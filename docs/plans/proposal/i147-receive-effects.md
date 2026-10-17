# i147 R11a — receive-rules effect column, page line for Luke

Status: proposed exact page text, awaiting Luke's yes before the architecture page is edited.
Concern's Q23 retirement is implemented against the already-approved page. Replacement state
remains next and is not started in this landing.

Page: `docs/architecture/gossip.md`, §3.1, “Receive rules: stale, duplicate and gap, per frame”.
Only the frame table changes. The preceding sentence “It decides nothing on a frame” stays.
Each later row's cells must prove the listed effect and the absence of any other effect.
The approved table and the catalog cell `every_frame_kind_has_a_row_in_the_receive_rules`
will land together: its header and required-column assertions move from five columns to six.
That catalog change is not made against the current five-column architecture page.
“Refuse” is the reserved-frame effect; its diagnostic span installs nothing and carries nothing.

Old:

| frame | category | subject | ordering | identity |
|---|---|---|---|---|
| `Digest` | replacement state | the exact birth | `digest_seq`, higher is newer | `(birth, digest_seq)` |
| `Members` (source publication and forwarded full) | snapshot | `(mesh, source_publisher)` | `topology_version` against the installed-version floor: within one publisher revision a lower version never overwrites a higher installed one whatever its `published_at_rafka_ms`, including a version installed by a topology read | `(mesh, source_publisher, topology_version, snapshot_id, chunk_index)` |
| `MembersDelta` | delta | `(mesh, source_publisher)` | `base_version` = installed: apply; `topology_version` ≤ installed: stale (duplicate if already applied); `base_version` > installed: gap, repair | `(mesh, source_publisher, base_version, topology_version)` |
| `NodeDeleting`, `NodeRestarting`, `NodeStopped` | operation fact, carrying the publisher's `topology_version` as the projection it rides | `(build_id, attempt, operation, node_id, incarnation)` | the fact: reconciled against the operation's completion and the birth's later lifecycle (a `NodeRestarting` after that restart's `node.started` reopens nothing); the carried `topology_version`: replacement state for the projection | the operation identity |
| `NodeDeleted` | operation fact, terminal | `(build_id, attempt, operation, node_id, incarnation)` | stands after any unrelated frame; never discarded because other traffic arrived first | the operation identity |
| `MeshStatus`, `FabricStatus` | replacement state | `(mesh \| fabric, publisher)` | `changed_at_rafka_ms`, higher is newer. **Gap:** they carry a publisher string and a timestamp, not the publisher's revision, so a delayed predecessor's frame is not refused by revision; closing it is an i147 packet (a field, or the status riding the publisher's projection) | `(subject, publisher, changed_at_rafka_ms)` |
| `Seated` | replacement state | the cohort `(mesh, node_type)`, or the fabric seat | the committed `revision` | `(cohort, revision)` |
| `PrimaryUpdating`, `PrimaryUpdated` | replacement state | each named cohort `(mesh, node_type)` or the fabric seat | the committed `revision` per cohort; at one revision `updated` is later than `updating`; a frame naming two cohorts is ignored when either holds a newer key | `(cohort, revision, stage)` |
| `Concern` (position 10) | reserved | — | — | refused by name (`rdm.mesh.membership.reject.via-reserved`), nothing applied. Retired (Luke, Q23, 2026-10-16): a receiver that started an investigation because a frame told it to was deciding on gossip (§10); the fabric-primary's silence is measured by each admin itself and acted on with the directed tickle |
| `StateCommitting`, `StateCommitted`, `TrafficOpening`, `TrafficOpened` | operation fact | `(kind, fabric_id, build_id, attempt, operation, node_id, incarnation)` | none: a hook drives no checklist and reopens nothing by construction (gossip receipt never replaces the directed completion). Its only effects are its span and, on the mesh-admin holding an open round, carrying an ordinary node's check-in onto the backbone; a hook of a closed or superseded round is carried nowhere (`via-round-frame` with `carried = closed-round`) | the identity |
| `NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving`, `NodeLeft`, `NewFabricPrimary` | reserved | — | — | refused by name, nothing applied |

Proposed new:

| frame | category | subject | ordering | identity | effect |
|---|---|---|---|---|---|
| `Digest` | replacement state | the exact birth | `digest_seq`, higher is newer | `(birth, digest_seq)` | install a projection |
| `Members` (source publication and forwarded full) | snapshot | `(mesh, source_publisher)` | `topology_version` against the installed-version floor: within one publisher revision a lower version never overwrites a higher installed one whatever its `published_at_rafka_ms`, including a version installed by a topology read | `(mesh, source_publisher, topology_version, snapshot_id, chunk_index)` | install a projection; carry onward when forwarding the source publication |
| `MembersDelta` | delta | `(mesh, source_publisher)` | `base_version` = installed: apply; `topology_version` ≤ installed: stale (duplicate if already applied); `base_version` > installed: gap, repair | `(mesh, source_publisher, base_version, topology_version)` | install a projection; carry onward when forwarding the source delta |
| `NodeDeleting`, `NodeRestarting`, `NodeStopped` | operation fact, carrying the publisher's `topology_version` as the projection it rides | `(build_id, attempt, operation, node_id, incarnation)` | the fact: reconciled against the operation's completion and the birth's later lifecycle (a `NodeRestarting` after that restart's `node.started` reopens nothing); the carried `topology_version`: replacement state for the projection | the operation identity | record a fact; carry onward when forwarding it |
| `NodeDeleted` | operation fact, terminal | `(build_id, attempt, operation, node_id, incarnation)` | stands after any unrelated frame; never discarded because other traffic arrived first | the operation identity | record a fact; carry onward when forwarding the terminal fact |
| `MeshStatus`, `FabricStatus` | replacement state | `(mesh \| fabric, publisher)` | `changed_at_rafka_ms`, higher is newer. **Gap:** they carry a publisher string and a timestamp, not the publisher's revision, so a delayed predecessor's frame is not refused by revision; closing it is an i147 packet (a field, or the status riding the publisher's projection) | `(subject, publisher, changed_at_rafka_ms)` | install a projection; carry onward when forwarding the status |
| `Seated` | replacement state | the cohort `(mesh, node_type)`, or the fabric seat | the committed `revision` | `(cohort, revision)` | install a projection |
| `PrimaryUpdating`, `PrimaryUpdated` | replacement state | each named cohort `(mesh, node_type)` or the fabric seat | the committed `revision` per cohort; at one revision `updated` is later than `updating`; a frame naming two cohorts is ignored when either holds a newer key | `(cohort, revision, stage)` | install a projection; carry onward when forwarding the committed update |
| `Concern` (position 10) | reserved | — | — | refused by name (`rdm.mesh.membership.reject.via-reserved`), nothing applied. Retired (Luke, Q23, 2026-10-16): a receiver that started an investigation because a frame told it to was deciding on gossip (§10); the fabric-primary's silence is measured by each admin itself and acted on with the directed tickle | refuse |
| `StateCommitting`, `StateCommitted`, `TrafficOpening`, `TrafficOpened` | operation fact | `(kind, fabric_id, build_id, attempt, operation, node_id, incarnation)` | none: a hook drives no checklist and reopens nothing by construction (gossip receipt never replaces the directed completion). Its only effects are its span and, on the mesh-admin holding an open round, carrying an ordinary node's check-in onto the backbone; a hook of a closed or superseded round is carried nowhere (`via-round-frame` with `carried = closed-round`) | the identity | carry onward only for an ordinary check-in of an open round; otherwise no state effect |
| `NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving`, `NodeLeft`, `NewFabricPrimary` | reserved | — | — | refused by name, nothing applied | refuse |
