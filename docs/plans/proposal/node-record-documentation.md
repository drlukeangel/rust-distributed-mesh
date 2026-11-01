# Node-record documentation review

This audit accompanies i150 in the existing lane. The CLI owns implementation, compilation and
acceptance; this documentation pass does not certify the moving source tree or close issue 245.
All outstanding i150 requirements remain BUILD until their recorded proofs pass at the final tip.

## Corrections made in the working tree

- The node-record proposal distinguishes node-owned properties, writer-birth lifecycle records,
  pair claims and the independent existing election revision. Build replay recognition persists
  with config and tags; existing nodes can receive Build overrides. Recovery carries acknowledged
  properties and application identities; replacement starts from its entry.
- The plan and i143 work record distinguish implemented increments from pending process proofs.
  No pending gate, flame, Rafka acceptance, or A/B startup proof is marked complete.
- Provider guides now include all six storage boundaries, both node-record kinds, applied identities,
  claim snapshots and deferred obligations. Freeze uses existing command clocks and recovery;
  a paused or exited birth cannot acknowledge it, and no new timer or investigation state is added.
- The environment table names separate path/name and explicit node kind. The removed node.restart
  operation is no longer prescribed by the storage runbook.
- The new [overview](../../features/node-records/overview.md),
  [how-to](../../features/node-records/how-to.md), and
  [runbook](../../features/node-records/runbook.md) describe integration with an explicit acceptance
  status. The book index and consumer guide link them.
- Earlier FabricAtlas and whole-record proposals are labelled historical where i150 supersedes
  their combined node-row design. Historical rulings retain their decision wording.
- Broken local destinations were repaired where the intended page is identifiable. Deleted historical
  sources are retained as source labels, not links to unrelated current pages.
- Generated config/span references were refreshed using the existing doc generator binary without
  starting a compiler build. The final implementation gate must regenerate them again.

## Architecture review status

The [architecture packet](node-record-architecture.patch) with SHA-256
`dfa84492791a24b6bbdc7c109894e36400f02a887271f7228e9827b6131b44cb` was approved and applied.
It is retained unchanged as that approval's exact artifact. The subsequent ID-first and separate-field
corrections below require a new exact-text packet; they are not covered by that approval.
The page changes and completed code must land together. Approval
of the documentation is not proof that runtime behavior matches it, and does not complete i150
or the startup correction tracked in [issue 247](https://github.com/drlukeangel/rust-distributed-mesh/issues/247).

The changes cover:

- [lifecycles/node-config-update.md](../../architecture/lifecycles/node-config-update.md)
- [query.md](../../architecture/query.md)
- [node-identity-and-state.md](../../architecture/node-identity-and-state.md)
- [lifecycles/node-name-update.md](../../architecture/lifecycles/node-name-update.md)
- [lifecycles/node-primary-update.md](../../architecture/lifecycles/node-primary-update.md)
- [storage.md](../../architecture/storage.md)
- [lifecycles/node-create.md](../../architecture/lifecycles/node-create.md)
- [lifecycles/node-start.md](../../architecture/lifecycles/node-start.md)
- [lifecycles/node-topology-get.md](../../architecture/lifecycles/node-topology-get.md)
- [node-admin-lifecycle.md](../../architecture/node-admin-lifecycle.md)
- [build.md](../../architecture/build.md)
- [node-rpc-envelope.md](../../architecture/node-rpc-envelope.md)
- [lifecycles/node-replace.md](../../architecture/lifecycles/node-replace.md)

Path is immutable: replacement renames only the predecessor name to `<name>-old` at its unchanged
path; rename refuses a different path. Globally unique node IDs are the first choice for references;
the composite `(path, name)` index resolves caller-supplied locations without joining the fields.
Replacement provenance is explicit metadata, never inferred from a name ending in `-old`.

They replace atlas-held node properties, keyed tag rows and row revisions; define direct record
reads and separate path/name; retain Build overrides and atomic application recognition; and
require lifecycle preparation before installing a writing role. The existing election revision,
command routes, clocks and gossip formats remain independently defined.

Architect 1 reviewed the approved packet, proposal and guides. That packet addresses: no independent row-retry clock after premature primary acknowledgement, no
install-first or next-row-revision sequence, and current pair selection occurs at claim rather
than Ready. This is a documentation review, not process or fault-injection acceptance.

Architect 1's current correction review remains open. The next exact-text packet must:

- Replace joined operation examples with structured operation identities and separate location fields.
- Replace the identity page's joined-reference headings and its prohibition on stored NodeId references;
  code references use NodeId first, and location lookup resolves the composite pair once.
- Keep predecessor and successor examples at the same path, with separate original and suffixed names.
- Separate record filtering (including Deleted rows) from current-location claim resolution, and name
  the individual path/name sort fields.
- Distinguish explicit replacement provenance from pending old-name claim release; ordinary `-old`
  names establish neither identity nor provenance.

These corrections are specified in the proposal. The architecture pages and approved packet remain
unchanged pending the new exact-text review.

## Validation and final integration work

`git apply --check` passed before application of the exact architecture packet.
The source-link scan covered 201 Markdown files with no missing local source targets; 13 links
point to generated rustdoc outputs. `git diff --check -- docs CLAUDE.md` also passes. Full mdBook/rustdoc validation belongs with the final CLI gate so it
runs against one coherent source tip. It has not been run by this documentation pass.

The CLI must reconcile Rust documentation and final typed examples with the completed code:
provider-boundary comments still describing five stores, update/name comments assuming a leaf-only
rename, and any old ConfigUpdated/OwnRow/OwnConfigUpdated revision-bearing schema. Do not remove
`Primary.revision`: it belongs to elections. Do not hand-edit generated reference pages to hide a
stale Rust comment. Regenerate references and run the repository's documentation check at the final
source tip, then confirm all proposed architecture statements have their implementation proofs.

Source-link evidence and old/new architecture content hashes are retained in
`target/evidence/i150/docs-link-audit-final.json` and
`target/evidence/i150/docs-architecture-manifest.json`. These are local audit evidence, not gates.

## Topology repair clarification

Traced the production callers: `top_up_fetch` in `crates/rdm-node/src/topology_read.rs` requests
the affected mesh from the current own mesh-primary with `since: None`. It sends no publisher or
version. The server compares a source token only when one was supplied; otherwise it returns
snapshot chunks. The receiver installs a complete snapshot before resuming differentials.

The startup workflow first reads with no source token, retains the first installed snapshot's
publisher/version, and may echo it during the final Rafka-time read to avoid resending unchanged
content. This conditional read is not the gap-repair request. The architecture patch now makes
that distinction explicit. This was a source trace; no runtime tests or wire changes were made.

Luke subsequently corrected startup ordering: the required target is Build facts first, then one
final topology fetch carrying current Rafka-time. The two-call trace above describes existing
code, not the required design. The packet includes node-start.md and removes the early-fetch
requirement; i150 records shared-workflow changes, dependency audit and a one-fetch trace as owed.
