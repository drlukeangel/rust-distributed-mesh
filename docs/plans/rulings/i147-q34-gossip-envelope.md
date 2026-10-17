**Q34: A — Luke, 2026-10-17. The three §3.1 replacements in gossip-envelope-binding.md are approved together with the envelope and R10 landing.**

# Q34 — freeze the CloudEvents gossip envelope

**Awaiting Luke's ruling.** a1 approved the mapping and its six resolutions, relayed by Luke
2026-10-17. Publisher-birth prerequisite: b07d9c14 (separate landing). No envelope code or
architecture edit has started. This packet follows the repository's ruling-packet workflow.

## 1. Spec

[gossip.md §3.4](../../architecture/gossip.md#34-the-envelope): “Every frame on either channel is a
CloudEvents 1.0 event with the Sequence extension.” The section fixes source to the author's
`rdm://<fabric_id>/<node_id>/<incarnation>` URI and time to evidence, not authority order.
Owner: the author supplies context; intermediaries retain it; each receiving node classifies the
event against its held subject. Sequence: author fact → forward unchanged as to authorship →
classify at receipt → apply valid data or carry only where the existing directed owner permits.

The [reviewed mapping](../proposal/gossip-envelope-mapping.md#a1-disposition-supplied-by-luke-2026-10-17)
settles: operations omit sequence; carried lifecycle versions and atomic per-cohort seat revisions
are data rules; statuses use `P(seat_revision).P(changed_at_rafka_ms)`; publication identity includes
the forwarder's exact birth and counter; real and synthetic cells supply their own author births.
§3.4 is unchanged. The [binding, all 22 types, byte accounting and exact §3.1 replacements](../proposal/gossip-envelope-binding.md) are the arm-A appendix.

## 2. Issue

At b07d9c14 all e2 behavioral cells pass, but all 22 membership kinds still use the positional
Frame enum rather than CloudEvents context, and both status kinds still lack a committed authority
revision; this is an always-present schema gap (R10), not an intermittent gossip failure.

## 3. Flame

### Observations

These existing live controls prove the authorship path the envelope must preserve; none claims
an implemented envelope or R10 GREEN.

- [Empty source received on backbone](http://localhost:16686/trace/98284e918223efcc1922d5ad3583728d)
- [Empty forwarded replay installed](http://localhost:16686/trace/a6d50212447cd47bf652df7e14fa4a31)
- [Same author installed through topology-read door](http://localhost:16686/trace/4baf18caae13a47b0c87f2c80c53c308)

Verbatim captured spans (the publications have zero members, so no body member supplied the author):

```json
{"attributes":{"busy_ns":"1904","channel":"backbone","code.filepath":"crates/rdm-transport/src/membership.rs","code.lineno":"2384","code.namespace":"rdm_transport::membership","departed":"0","idle_ns":"7392","in_flight":"0","members":"0","mesh":"mesh2","new_revision":"true","node":"mesh1.admin.1","publisher":"mesh2.admin.1:xr5b1seak43n@y99t01wt6y4z","refreshed":"false","resumed":"false","snapshot_id":"1","thread.id":"10","thread.name":"tokio-rt-worker","topology_version":"1"},"end_unix_nano":1792230980791911044,"events":[],"name":"rdm.mesh.membership.update.via-snapshot-installed","parent_span_id":"","service":"rdm-publisher-birth","span_id":"5b3f7085a3eda2e1","start_unix_nano":1792230980791903140,"trace_id":"98284e918223efcc1922d5ad3583728d"}
{"attributes":{"busy_ns":"1056","channel":"mesh-channel","code.filepath":"crates/rdm-transport/src/membership.rs","code.lineno":"2384","code.namespace":"rdm_transport::membership","departed":"0","idle_ns":"6000","in_flight":"0","members":"0","mesh":"mesh2","new_revision":"true","node":"mesh1.rpc.1","publisher":"mesh2.admin.1:xr5b1seak43n@y99t01wt6y4z","refreshed":"false","resumed":"false","snapshot_id":"2","thread.id":"10","thread.name":"tokio-rt-worker","topology_version":"1"},"end_unix_nano":1792230980817363755,"events":[],"name":"rdm.mesh.membership.update.via-snapshot-installed","parent_span_id":"","service":"rdm-publisher-birth","span_id":"515c5d7bf173a057","start_unix_nano":1792230980817357115,"trace_id":"a6d50212447cd47bf652df7e14fa4a31"}
{"attributes":{"busy_ns":"3536","channel":"topology-read","code.filepath":"crates/rdm-transport/src/membership.rs","code.lineno":"2384","code.namespace":"rdm_transport::membership","departed":"0","idle_ns":"17120","in_flight":"0","members":"0","mesh":"mesh2","new_revision":"true","node":"mesh1.rpc.2","publisher":"mesh2.admin.1:xr5b1seak43n@y99t01wt6y4z","refreshed":"false","resumed":"false","snapshot_id":"1","thread.id":"7","thread.name":"publisher_handover::an_empty_publication_retains_its_supplied_author_birth_everywhere","topology_version":"1"},"end_unix_nano":1792230980847713965,"events":[],"name":"rdm.mesh.membership.update.via-snapshot-installed","parent_span_id":"","service":"rdm-publisher-birth","span_id":"7aa89a13818eb8a3","start_unix_nano":1792230980847696877,"trace_id":"4baf18caae13a47b0c87f2c80c53c308"}
```

The e2 [new publication at the same material version](http://localhost:16686/trace/80277ebfb8e059ff71f759b87acbf073)
and [exact repeat ignored](http://localhost:16686/trace/0e5a602e6588596d2c738324b259a8f5) are the two
publication controls. Full retrieved trace JSON is retained in target/evidence/publisher-birth/
and target/evidence/envelope-mapping-review/. The actual real Node RPC recovery cell remains the
required post-envelope flame, not a substituted read.

## 4. Code

### Observations (commit b07d9c14)

- [code] `crates/rdm-core/src/publisher.rs:10`: PublisherId retains the supplied node id and incarnation;
  `:19` copies them from a node's own birth. Receipt and forwarders do not resolve an author by name.
- [code] `crates/rdm-transport/src/membership.rs:332`: 22 Frame variants; `:615`–`:622` encode the enum
  with the one postcard codec. There are no standard CloudEvents context attributes yet.
- [code] `crates/rdm-transport/src/membership.rs:792`–`:825`: StatusPublisher has a node name/scope and
  change timestamp, no held committed-seat revision input; R10 therefore remains open.
- [code] `crates/rdm-transport/src/membership.rs:3297`: current_publisher retains the author birth;
  `:3331` requires that supplied birth at backbone join. `snapshot.rs:773` generates forwarding
  publications; `:815` derives a delta while retaining its source publisher.
- [code] `crates/rdm-transport/src/receive_order.rs:1`: e2 requires separate operation admission and
  projection validity, complete snapshot installation, fresh same-version hearing, stage-aware
  atomic seat validity, and floors shared with topology reads.
- [code] `tools/mesh-audit/src/lock.rs:724`–`:740`: the codec, membership goldens and topology goldens
  are sealed together. Changing to the reviewed binding is a named schema cut.
- [flame] The three span records above carry publisher `mesh2.admin.1:xr5b1seak43n@y99t01wt6y4z`
  across channels and read installation with `members=0`.

### Inferred

- [inferred] A generic scalar high-water check would break e2's operation facts and same-version
  publication hearing; the reviewed per-category context and data-validity split is necessary
  to retain those proven controls. No new ordering counter or ownership tier is inferred.

## 5. Delta

Spec says each frame's author, subject, identity and version are carried by CloudEvents context; code has no such path.

## 6. Arms

Every arm assumes e2 and the separate author-birth landing stand, a1's supplied review is accepted,
§3.4 stays unchanged, and no envelope code is authorized before this ruling.

- **A:** approve the reviewed binding and the exact §3.1 page replacements in the appendix, then implement
  the envelope and R10 in one landing — wire: **wire format moved**, one envelope reseal and all
  goldens/size ceilings together; publisher: supply owned author birth and held seat revision;
  timer: unchanged; tier: unchanged; fork: none. No dual decoder. All e2 assertions, sequencing
  and bounds stand, plus the new R10/type/header cells and real read-recovery flame.
- **B:** return this packet with the exact mapping or page-line correction required before the
  schema is cut — wire/publisher/timer/tier unchanged while awaiting the corrected ruling; fork: none.

