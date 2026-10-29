//! R-W1 (Luke 2026-10-08), UNIT layer: run by `scripts/i143-acceptance-gate.sh i143-rw1-unit`, which
//! exports `I143_ACCEPTANCE_DIR`; the cell leaves its `result.json` there. A pure model cell:
//! no estate, no spans.
//!
//! CONTRACT: every internal mesh gossip frame is postcard. Every `Frame` variant of the
//! topology channels and every shape a `BuildMessage` carries (each Build fact,
//! each typed step output, the shutdown; the removed Fabric record refused) encodes, decodes to the value it was
//! made from, and re-encodes to the same bytes; a chunk the packers produce encodes under
//! `MAX_MESSAGE_BYTES`; a frame this build does not read is refused with a named reason, a
//! JSON frame included; a step output that has no wire shape is refused naming its Build,
//! attempt and step while the facts around it still travel.

use rdm_core::meta::{NodeSpec, PersistentDisposition, StorageMeta};
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeKind, PathName, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rdm_transport::chunking::MAX_MESSAGE_BYTES;
use rdm_transport::topology::Frame;
use rdm_transport::snapshot::{chunks_of, Full, PublisherId};
use rdm_admin::accepted::{AttemptAction, FabricTopology, MeshTopology, TopologyChange};
use rdm_admin::build::{BuildId, FabricDesired, MeshDesired};
use rdm_admin::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateError, BuildStepReceipt, StepOutcome};
use rdm_admin::deployment::pipeline::Bound;
use rdm_admin::deployment::pipeline::{CommandAdmission, Completion, CreateStep, DeleteStep, StorageDisposition};
use rdm_admin::deployment::provider::DeploymentHandle;
use rdm_admin::fabric_builds::{encode_chunks, BuildMessage};
use rdm_admin::model::{DeploymentId, ProviderKind};
use serde_json::{json, Value};
use std::path::PathBuf;

const CELL: &str = "every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling";

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rw1/unit").join(CELL),
    }
}

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(ordinal: u32, runtime: Option<RuntimeFact>, full: bool) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
        fabric_id: fabric(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: format!("mesh1.rpc.{ordinal}").parse().unwrap(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: full.then(IncarnationId::mint),
            runtime,
        },
        status: MemberStatus::Ready,
        node_state: rdm_core::NodeState::Alive,
        admin_api_base: full.then(|| "http://127.0.0.1:7000".to_string()),
        digest_seq: 3 + ordinal as u64,
        emitted_at_rafka_ms: 1_000 + ordinal as u64,
        data_dir: full.then(|| "/var/lib/rafka/mesh1.rpc".to_string()),
        mesh_id: full.then(MeshId::mint),
        in_flight: full.then_some(4),
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

/// A member's heartbeat: the digest with no status, for a heartbeat carries none (gossip.md §3.1
/// `Digest`); a decoded one holds `Pending`, the status of a birth its receiver was not told of.
fn heartbeat(ordinal: u32, runtime: Option<RuntimeFact>, full: bool) -> MeshDigest {
    MeshDigest { status: MemberStatus::Pending, ..digest(ordinal, runtime, full) }
}

fn process_runtime() -> RuntimeFact {
    RuntimeFact { deployment_id: rdm_core::fixture_crockford60("dep1"), provider: RuntimeProvider::Process, control_domain: "host-a".into(), locator: RuntimeLocator::Process { pid: 4242, start: 987_654 } }
}

fn container_runtime() -> RuntimeFact {
    RuntimeFact { deployment_id: rdm_core::fixture_crockford60("dep2"), provider: RuntimeProvider::Container, control_domain: "docker-a".into(), locator: RuntimeLocator::Container { id: "f".repeat(64) } }
}

fn op(operation: &str) -> LifecycleOp {
    LifecycleOp { build_id: "3qcfk1kq47m9".into(), attempt: 2, operation: operation.into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.1".parse().unwrap(), event_at_rafka_ms: 7, traceparent: None, tracestate: None }
}

fn publisher() -> PublisherId {
    PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }
}

/// Every `Frame` variant, each digest shape (absent and present optionals, both runtime locators).
fn frames() -> Vec<Frame> {
    let p = publisher();
    vec![
        Frame::Digest { digest: heartbeat(1, None, false) },
        Frame::Digest { digest: heartbeat(2, Some(process_runtime()), true) },
        Frame::Digest { digest: heartbeat(3, Some(container_runtime()), true) },
        Frame::MeshTopology {
            mesh: "mesh2".into(),
            publisher: p.clone(),
            forwarded_by: Some("mesh1.admin.1".into()),
            topology_version: 9,
            published_at_rafka_ms: 11,
            snapshot_id: 5,
            chunk_index: 1,
            chunk_count: 3,
            digests: vec![digest(1, None, false), digest(2, Some(process_runtime()), true)],
            in_flight: vec![op("node.delete:mesh2.rpc.2")],
            deleted: vec![op("node.delete:mesh2.rpc.3")],
        },
        Frame::MeshTopology {
            mesh: "mesh2".into(),
            publisher: p.clone(),
            forwarded_by: None,
            topology_version: 1,
            published_at_rafka_ms: 1,
            snapshot_id: 1,
            chunk_index: 0,
            chunk_count: 1,
            digests: vec![],
            in_flight: vec![],
            deleted: vec![],
        },
        Frame::MeshTopologyDelta {
            mesh: "mesh2".into(),
            source_publisher: p.clone(),
            base_version: 8,
            topology_version: 9,
            published_at_rafka_ms: 12,
            changed: vec![digest(4, Some(container_runtime()), true)],
            removed: vec!["mesh2.rpc.9".into()],
            in_flight: vec![op("node.stop:mesh2.rpc.4")],
            deleted: vec![],
        },
        Frame::NodeDeleting { op: op("node.delete:mesh2.rpc.2"), forwarded_by: None, publisher: p.clone(), topology_version: 10 },
        Frame::NodeDeleted { op: op("node.delete:mesh2.rpc.2"), forwarded_by: Some("mesh1.admin.1".into()) },
        Frame::NodeRestarting { op: op("node.restart:mesh2.rpc.2"), forwarded_by: None, publisher: p.clone(), topology_version: 11 },
        Frame::NodeStopped { op: op("node.stop:mesh2.rpc.2"), forwarded_by: Some("mesh1.admin.1".into()), publisher: p.clone(), topology_version: 12 },
        Frame::NodeConnectionsDeleting { op: op("node.connection.delete:mesh2.rpc.2"), forwarded_by: None },
        Frame::NodeLeaving { op: op("node.stop:mesh2.rpc.2"), forwarded_by: Some("mesh1.admin.1".into()) },
        Frame::NodeLeft { op: op("node.stop:mesh2.rpc.2"), forwarded_by: Some("mesh2.admin.1".into()) },
        Frame::NodeConnectionsDeleted { op: op("node.connection.delete:mesh2.rpc.2"), forwarded_by: None },
        Frame::MeshStatus { mesh: "mesh2".into(), status: "ready".into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 77 },
        Frame::FabricStatus { fabric: fabric(), status: "ready".into(), publisher: "mesh1.admin.1".into(), forwarded_by: Some("mesh2.admin.1".into()), changed_at_rafka_ms: 78 },
        Frame::Concern { seat: rdm_core::Seat::FabricPrimary, node_id: rdm_core::NodeId::mint(), incarnation: IncarnationId::mint(), observer: "mesh2.admin.1".into() },
        Frame::NewFabricPrimary { holder: rdm_core::Primary { mesh: "mesh2".into(), node_id: rdm_core::NodeId::mint(), incarnation: IncarnationId::mint(), revision: 4 }, endpoint_id: "key2".into(), transport_addr: "127.0.0.1:41002".parse().unwrap(), admin_api_base: Some("http://127.0.0.1:41003".into()) },
    ]
}

fn bid(s: &str) -> BuildId {
    BuildId::parse(s).unwrap()
}

fn topology() -> FabricTopology {
    let mut t = FabricTopology::root("fabric-1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap());
    let mut m2 = MeshTopology::of(&MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 2), (NodeKind::Broker, 1)]));
    let broker: PathName = "mesh2.broker.1".parse().unwrap();
    let rpc: PathName = "mesh2.rpc.1".parse().unwrap();
    m2.set_spec(NodeSpec { storage_persistence: StorageMeta::Persistent { disposition: PersistentDisposition::Release }, ..NodeSpec::default_for(broker) }).unwrap();
    m2.set_spec(NodeSpec { storage_persistence: StorageMeta::Ephemeral, ..NodeSpec::default_for(rpc) }).unwrap();
    t.meshes.insert("mesh2".into(), m2);
    // A Build message carries no topology_storage_provider (Q184: it is written in the records, like the
    // executable), so the shape that round-trips names none.
    t.topology_storage_provider = None;
    t
}

fn changes() -> Vec<Option<TopologyChange>> {
    let desired = MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 3)]);
    vec![
        None,
        Some(TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric-1".into(), meshes: vec![desired.clone(), MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1)])] } }),
        Some(TopologyChange::ReconcileMesh { desired: desired.clone() }),
        Some(TopologyChange::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::Gateway }),
        Some(TopologyChange::RemoveNode { node: "mesh2.rpc.2".parse().unwrap() }),
        Some(TopologyChange::CreateMesh { desired }),
        Some(TopologyChange::RemoveMesh { mesh: "mesh2".into() }),
        // A stored change of the removed operation, read as history: its reserved position (proposal
        // executable-bindings-at-create.md §1).
        Some(TopologyChange::Reserved {}),
    ]
}

fn step(step: &str, output: Option<Value>) -> BuildFact {
    BuildFact::Step(BuildStepReceipt { mesh: None, build_id: bid("3qcfk1kq47m9"), attempt: 2, operation: "node.create:mesh2.rpc.1".into(), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some("key@127.0.0.1:1".into()) })
}

fn to_json<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).unwrap()
}

/// Every Build fact, each step output as the pipeline commits it (the JSON of the typed result).
fn facts() -> Vec<BuildFact> {
    let mut out = Vec::new();
    for change in changes() {
        out.push(BuildFact::Accepted(BuildAccepted { build_id: bid("3qcfk1kq47m9"), topology: topology(), submitted_change: change, submitted_at_ms: 55 }));
    }
    let action = |a: Option<AttemptAction>| BuildFact::Opened(AttemptOpened { mesh: None, build_id: bid("3qcfk1kq47m9"), attempt: 2, reason: AttemptReason::Stop, action: a, opened_by: "mesh1.admin.1".into(), opened_at_ms: 56, context: None });
    out.push(action(None));
    out.push(action(Some(AttemptAction::Stop { path: "mesh2.rpc.1".parse().unwrap(), node_id: NodeId::mint(), from_incarnation: IncarnationId::mint() })));
    // Steps (build.md §1; lifecycles/node-replace.md): the steps and the births they pinned.
    {
        use rdm_core::build::Step;
        let node: rdm_admin::model::PathName = "mesh2.rpc.1".parse().unwrap();
        let pin = rdm_admin::build::PinnedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        out.push(BuildFact::Opened(AttemptOpened {
            mesh: None,
            build_id: bid("3qcfk1kq47m9"),
            attempt: 2,
            reason: AttemptReason::Steps,
            action: Some(AttemptAction::Steps {
                steps: vec![
                    Step::DeleteConnections { node: node.clone() },
                    Step::LeaveMesh { node: node.clone() },
                    Step::Update { node: node.clone(), name: node.renamed() },
                    Step::Stop { node: node.renamed() },
                    // The executable the Build fixes rides the positional wire with every field written.
                    Step::Create { node: node.clone(), executable: Some(rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/opt/rpc-node".into(), sha256: "ab".repeat(32), image: None }) },
                    Step::Delete { node: node.renamed() },
                ],
                pinned: vec![(node, pin)],
            }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 56,
            context: None,
        }));
    }
    // A member's recovery (node-recovery.md): the attempt the mesh-primary's submit opens.
    out.push(BuildFact::Opened(AttemptOpened {
        mesh: None,
        build_id: bid("3qcfk1kq47m9"),
        attempt: 2,
        reason: AttemptReason::Recovery,
        action: Some(AttemptAction::Recover { path: "mesh2.rpc.1".parse().unwrap(), node_id: NodeId::mint(), from_incarnation: IncarnationId::mint(), backbone: false }),
        opened_by: "mesh1.admin.1".into(),
        opened_at_ms: 56,
        context: None,
    }));

    out.push(BuildFact::Claim(BuildAttemptClaim { claimant: None, mesh: None, build_id: bid("3qcfk1kq47m9"), attempt: 2, executor: "mesh1.admin.1".into() }));
    out.push(step("PrepareNetwork", None));
    out.push(BuildFact::Step(BuildStepReceipt {
        mesh: None,
        build_id: bid("3qcfk1kq47m9"),
        attempt: 2,
        operation: "node.create:mesh2.rpc.1".into(),
        step: "WaitForBind".into(),
        outcome: StepOutcome::failed("runtime exited (code Some(1)) before binding"),
        output: None,
        executor: None,
    }));
    // An unreached birth's TerminateRuntime: refused by the provider, then the receipt of what the step
    // read at its instant (node-recovery.md step 3), in a Build message.
    out.push(BuildFact::Step(BuildStepReceipt {
        mesh: Some("mesh2".into()),
        build_id: bid("3qcfk1kq47m9"),
        attempt: 3,
        operation: "node.delete:mesh2.rpc.1".into(),
        step: DeleteStep::TerminateRuntime.name().into(),
        outcome: StepOutcome::failed("terminating deployment an16er0436pg failed: the daemon refused"),
        output: None,
        executor: Some("key@127.0.0.1:1".into()),
    }));
    out.push(BuildFact::Step(BuildStepReceipt {
        mesh: Some("mesh2".into()),
        build_id: bid("3qcfk1kq47m9"),
        attempt: 4,
        operation: "node.delete:mesh2.rpc.1".into(),
        step: DeleteStep::TerminateRuntime.name().into(),
        outcome: StepOutcome::Complete,
        output: Some(to_json(&rdm_admin::deployment::pipeline::TerminalReceipt { node: "mesh2.rpc.1".into(), node_id: NodeId::mint().to_string(), incarnation_id: IncarnationId::mint().to_string(), runtime: None, exit_code: None, verdict: Some("running".into()), data_dir: Some("/data/mesh2.rpc.1-3qcfk1kq47m9".into()) })),
        executor: Some("key@127.0.0.1:1".into()),
    }));
    out.push(step(CreateStep::AllocateIdentity.name(), Some(json!({ "node_id": to_json(&NodeId::mint()), "incarnation": to_json(&IncarnationId::mint()), "supersedes": null, "deployment_id": "an16er0436pg" }))));
    out.push(step(CreateStep::AllocateIdentity.name(), Some(json!({ "node_id": to_json(&NodeId::mint()), "incarnation": to_json(&IncarnationId::mint()), "supersedes": to_json(&IncarnationId::mint()), "deployment_id": "an16er0436pg" }))));
    out.push(step(CreateStep::WaitForBind.name(), Some(to_json(&Bound { transport: "127.0.0.1:41000".parse().unwrap(), listeners: vec![("control".into(), "127.0.0.1:41001".parse().unwrap())] }))));
    out.push(step(CreateStep::PrepareStorage.name(), Some(to_json(&EndpointId("abcd".into())))));
    out.push(step(
        CreateStep::DeployRuntime.name(),
        Some(to_json(&DeploymentHandle { deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60("dep-1")).unwrap(), provider: ProviderKind::Process, pid: Some(4242), start: Some(99), container: None, domain: Some("host-a".into()) })),
    ));
    out.push(step(
        CreateStep::DeployRuntime.name(),
        Some(to_json(&DeploymentHandle { deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60("dep-2")).unwrap(), provider: ProviderKind::Container, pid: None, start: None, container: Some("c".repeat(64)), domain: None })),
    ));
    let evidence = |data_dir: Option<&str>| {
        let mut v = json!({ "deployment_id": "an16er0436pg", "provider": "process", "provider_control_domain_fingerprint": "aa11", "runtime_locator_kind": "process-pid-start", "runtime_locator_fingerprint": "bb22" });
        if let Some(d) = data_dir {
            v["data_dir"] = d.into();
        }
        v
    };
    for s in [CreateStep::RegisterExactRuntimeHandle, CreateStep::ResolveProviderControlDomain, CreateStep::MakeRuntimeFactAvailableToBirth] {
        out.push(step(s.name(), Some(evidence(None))));
    }
    out.push(step(CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata.name(), Some(evidence(Some("/var/lib/rafka/mesh2.rpc.1")))));
    out.push(step(CreateStep::ApplyMeshPending.name(), Some(json!({ "applied": "Pending" }))));
    out.push(step(CreateStep::ApplyMeshPending.name(), Some(json!({ "applied": null, "reason": "no Pending owed: not a new mesh's first admin" }))));
    for s in [DeleteStep::NodeDeleting, DeleteStep::NodeDeleted] {
        out.push(step(s.name(), Some(to_json(&op("node.delete:mesh2.rpc.1")))));
    }
    for (command, completion) in [(DeleteStep::StopNode, DeleteStep::AwaitNodeLeft)] {
        for d in [
            CommandAdmission::Admitted {},
            CommandAdmission::AlreadyAdmitted {},
            CommandAdmission::NotSent { reason: "NoRoute".into() , code: rdm_core::outcome::Code::FailedPrecondition },
            CommandAdmission::Indeterminate { reason: "no reply".into() , code: rdm_core::outcome::Code::FailedPrecondition },
            CommandAdmission::Refused { reply: "StaleIncarnation".into() , code: rdm_core::outcome::Code::FailedPrecondition },
        ] {
            out.push(step(command.name(), Some(to_json(&d))));
        }
        for c in [Completion::Received {}, Completion::Deadline {}, Completion::RuntimeExited {}, Completion::NotAwaited { admission: "NotSent".into() }] {
            out.push(step(completion.name(), Some(to_json(&c))));
        }
    }
    for s in [StorageDisposition::Released { locator: "/data/x".into() }, StorageDisposition::Preserved {}, StorageDisposition::PreservedNoMeta {}] {
        // A receipt an earlier build wrote: no step of this build runs it, and it still decodes.
        out.push(step(rdm_admin::wire::RELEASE_STORAGE_STEP, Some(to_json(&s))));
    }
    out.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: bid("3qcfk1kq47m9"), attempt: 2, outcome: AttemptOutcome::Converged }));
    out.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: bid("3qcfk1kq47m9"), attempt: 2, outcome: AttemptOutcome::Failed { reason: "step failed".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
    out.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: bid("3qcfk1kq47m9"), attempt: 3, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }));
    out.push(BuildFact::Forget { build_id: bid("3qcfk1kq47m9") });
    out
}

#[test]
fn every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling() {
    let mut result = serde_json::Map::new();

    // ---- every Frame variant: decode(encode(f)) is f, and re-encodes to the same bytes ----
    let all_frames = frames();
    for f in &all_frames {
        let event = crate::gossip_wire::authored(f);
        let bytes = event.encode();
        crate::gossip_wire::assert_roundtrip(f, &bytes);
        assert!(bytes.len() <= MAX_MESSAGE_BYTES, "{f:?} fits one complete gossip message");
    }
    result.insert("frame_variants_round_tripped".into(), json!(all_frames.len()));

    // ---- every Build fact and shape, one BuildMessage each ----
    let all_facts = facts();
    for fact in &all_facts {
        let (messages, refused) = encode_chunks(vec![fact.clone()]);
        assert!(refused.is_empty(), "{fact:?}: {refused:?}");
        assert_eq!(messages.len(), 1);
        let back = BuildMessage::from_bytes(&messages[0]).unwrap_or_else(|e| panic!("{fact:?} does not decode: {e}"));
        assert_eq!(back.facts, vec![fact.clone()], "a fact decodes to the fact it was made from");
    }
    result.insert("build_fact_shapes_round_tripped".into(), json!(all_facts.len()));

    // The removed Build channel's Fabric record (Q185): its position is reserved, and a message
    // carrying one is refused by name.
    #[derive(serde::Serialize)]
    struct WithFabricRecord {
        nonce: u64,
        facts: Vec<u8>,
        fabric: Option<(FabricId, String)>,
    }
    let bytes = rdm_transport::wire::encode(&WithFabricRecord { nonce: 7, facts: vec![], fabric: Some((fabric(), "fabric-1".into())) }).unwrap();
    let refused = BuildMessage::from_bytes(&bytes).err().expect("a message carrying the Fabric record is refused").to_string();
    assert!(refused.contains("Fabric record") && refused.contains("reserved"), "{refused}");
    let none = rdm_transport::wire::encode(&WithFabricRecord { nonce: 7, facts: vec![], fabric: None }).unwrap();
    assert_eq!(BuildMessage { nonce: 7, facts: vec![] }.to_bytes().unwrap().as_ref(), none.as_slice(), "the reserved position keeps the frame's bytes");

    // ---- the packers keep every message under the ceiling and lose no fact ----
    let many: Vec<BuildFact> = all_facts.iter().cloned().cycle().take(all_facts.len() * 8).collect();
    let (messages, refused) = encode_chunks(many.clone());
    assert!(refused.is_empty(), "{refused:?}");
    assert!(messages.len() > 1, "{} facts need more than one message", many.len());
    let mut got = Vec::new();
    for m in &messages {
        assert!(m.len() <= MAX_MESSAGE_BYTES, "a Build message of {} bytes fits the ceiling {MAX_MESSAGE_BYTES}", m.len());
        got.extend(BuildMessage::from_bytes(m).unwrap().facts);
    }
    assert_eq!(got, many, "the messages carry every fact, in order");
    result.insert("build_messages".into(), json!({ "facts": many.len(), "messages": messages.len(), "largest_bytes": messages.iter().map(|m| m.len()).max(), "ceiling": MAX_MESSAGE_BYTES }));

    let members: Vec<MeshDigest> = (1..=150).map(|k| digest(k, (k % 2 == 0).then(process_runtime), k % 3 == 0)).collect();
    let overlays: Vec<LifecycleOp> = (0..12).map(|_| op("node.delete:mesh1.rpc.1")).collect();
    let full = Full::new(members.clone(), overlays.clone(), overlays.iter().take(5).cloned().collect());
    let p = publisher();
    let chunks = chunks_of(&full, |digests, in_flight, deleted, chunk_index, chunk_count| Frame::MeshTopology {
        mesh: "mesh1".into(),
        publisher: p.clone(),
        forwarded_by: None,
        topology_version: 5,
        published_at_rafka_ms: 1,
        snapshot_id: 9,
        chunk_index,
        chunk_count,
        digests,
        in_flight,
        deleted,
    }.authored(&crate::gossip_wire::author(), 0, 1));
    assert!(chunks.len() > 2);
    let mut heard = 0;
    for c in &chunks {
        let bytes = c.encode();
        assert!(bytes.len() <= MAX_MESSAGE_BYTES, "a MeshTopology chunk of {} bytes fits the ceiling", bytes.len());
        match Frame::decode(&bytes).unwrap().into_data() {
            Frame::MeshTopology { digests, .. } => heard += digests.len(),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(heard, members.len(), "the chunks carry every member");
    result.insert("nodes_snapshot".into(), json!({ "members": members.len(), "chunks": chunks.len(), "largest_bytes": chunks.iter().map(|c| c.encode().len()).max(), "ceiling": MAX_MESSAGE_BYTES }));

    // ---- sizes: one digest, postcard against the JSON-shaped MeshDigest ----
    let one = digest(1, Some(process_runtime()), true);
    let wire_bytes = crate::gossip_wire::authored(&Frame::Digest { digest: one.clone() }).encode().len();
    let json_bytes = serde_json::to_vec(&one).unwrap().len();
    assert!(wire_bytes < json_bytes, "postcard {wire_bytes} B against JSON {json_bytes} B");
    result.insert("digest_bytes".into(), json!({ "postcard_frame": wire_bytes, "json_digest": json_bytes }));

    // ---- a frame this build does not read is refused by name ----
    let garbage = Frame::decode(&[0xff, 0xff, 0xff, 0xff]).unwrap_err().to_string();
    assert!(garbage.contains("postcard decode of 4 bytes as"), "{garbage}");
    let json_frame = Frame::decode(br#"{"frame":"digest","digest":{}}"#).unwrap_err().to_string();
    assert!(json_frame.contains("postcard decode"), "a JSON frame is refused, not read: {json_frame}");
    let mut padded = crate::gossip_wire::authored(&all_frames[5]).encode();
    padded.push(0);
    let trailing = Frame::decode(&padded).unwrap_err().to_string();
    assert!(trailing.contains("1 bytes left after the frame"), "{trailing}");
    let build_garbage = BuildMessage::from_bytes(br#"{"nonce":1,"facts":[]}"#).err().expect("a JSON Build message is refused").to_string();
    assert!(build_garbage.contains("postcard decode"), "{build_garbage}");
    result.insert("refusals".into(), json!({ "garbage": garbage, "json_frame": json_frame, "trailing_bytes": trailing, "json_build_message": build_garbage }));

    // ---- an output with no wire shape is refused naming Build, attempt and step; neighbours travel ----
    let good = step(CreateStep::WaitForBind.name(), Some(to_json(&Bound { transport: "127.0.0.1:41000".parse().unwrap(), listeners: vec![] })));
    let stray = step(CreateStep::PrepareNetwork.name(), Some(json!({ "anything": 1 })));
    let mistyped = step(CreateStep::AllocateIdentity.name(), Some(json!({ "pid": 7 })));
    let (messages, refused) = encode_chunks(vec![good.clone(), stray, mistyped, good.clone()]);
    let named: Vec<String> = refused.iter().map(|e| e.to_string()).collect();
    assert_eq!(named.len(), 2, "{named:?}");
    assert!(refused.iter().all(|e| matches!(e, BuildStateError::Unencodable(_))), "{named:?}");
    assert!(named[0].contains("step PrepareNetwork (Build 3qcfk1kq47m9, attempt 2)") && named[0].contains("commits no output"), "{}", named[0]);
    assert!(named[1].contains("step AllocateIdentity (Build 3qcfk1kq47m9, attempt 2)"), "{}", named[1]);
    let sent: usize = messages.iter().map(|m| BuildMessage::from_bytes(m).unwrap().facts.len()).sum();
    assert_eq!(sent, 2, "the facts around a refused one still travel");
    result.insert("unencodable".into(), json!({ "refused": named, "sent": sent }));

    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&Value::Object(result)).unwrap()).unwrap();
}

/// All 19 kinds declare their literal standard type; reserved kinds remain named refusals.
#[test]
fn every_kind_declares_its_ruled_type_string() {
    let cases = frames();
    let expected = ["rdm.mesh.digest", "rdm.mesh.digest", "rdm.mesh.digest", "rdm.mesh.members", "rdm.mesh.members", "rdm.mesh.members.delta", "rdm.node.deleting", "rdm.node.deleted", "rdm.node.restarting", "rdm.node.stopped", "rdm.node.connections.deleting", "rdm.node.leaving", "rdm.node.left", "rdm.node.connections.deleted", "rdm.mesh.status", "rdm.fabric.status", "rdm.mesh.concern", "rdm.fabric.primary.new"];
    assert_eq!(cases.len(), expected.len());
    for (frame, kind) in cases.iter().zip(expected) {
        let bytes = crate::gossip_wire::authored(frame).encode();
        let (attrs, _): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&bytes).unwrap();
        assert_eq!(attrs.iter().find(|(n, _)| n == "type").unwrap().1, kind);
        crate::gossip_wire::assert_roundtrip(frame, &bytes);
        assert!(bytes.len() <= 4032);
    }
    // 19 catalog types; the two reserved since Q171 and the two since Q174 are never sent;
    // rdm.node.restarting, reserved since Q183, is encoded here only to prove its refusal.
    assert_eq!(expected.into_iter().collect::<std::collections::BTreeSet<_>>().len(), 15);
}

/// A twelve-node fabric whose every entry names its executable, as a complete-topology request
/// compiles it: three meshes of four, each path on its own launch id's binding, each with a tag and a
/// config.
fn twelve_node_topology() -> FabricTopology {
    let mut t = FabricTopology::root("fabric-1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap());
    t.meshes.clear();
    for (mesh, kind) in [("mesh1", NodeKind::RpcNode), ("mesh2", NodeKind::Broker), ("mesh3", NodeKind::Gateway)] {
        let mut m = MeshTopology::of(&MeshDesired::of(mesh, [(NodeKind::NodeAdmin, 2), (kind, 2)]));
        for p in m.nodes.clone() {
            let id = p.kind.name().replace('-', "_");
            let executable = rdm_core::binding::Binding { launch_id: id.clone(), executable: format!("/opt/rafka/candidates/0123456789abcdef/{id}").into(), sha256: "ab".repeat(32), image: Some("rafka-node-runtime:0123456789abcdef".into()) };
            let config = [("zone".to_string(), "a".to_string())].into();
            m.set_spec(NodeSpec { storage_persistence: StorageMeta::Ephemeral, executable: Some(executable), tags: ["zone_a".to_string()].into(), config, ..NodeSpec::default_for(p) }).unwrap();
        }
        t.meshes.insert(mesh.into(), m);
    }
    t
}

/// `t` with no entry naming an executable, tags or a config, and no `topology_storage_provider` (Q184: it is
/// written in the records, like the executable): what a Build message carries of it.
fn without_executables(t: &FabricTopology) -> FabricTopology {
    let mut t = t.clone();
    t.topology_storage_provider = None;
    for m in t.meshes.values_mut() {
        for c in m.spec_by_path.values_mut() {
            c.executable = None;
            c.tags.clear();
            c.config.clear();
        }
    }
    t
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 7: "Nothing about executables rides gossip or
/// `node.topology.get`"; Luke 2026-10-24: "the executable is not gossiped, it's only in the node storage"):
/// the accepted fact in a Build message carries no executable (nor an entry's tags or config, Q166), of its topology's
/// entries or of the complete-topology change it was submitted with; its bytes are those of the same fact
/// naming none, so
/// a twelve-node fabric's accepted fact fits one gossip message, as it did before the field. The stored
/// accepted Build (builds.storage) keeps them. Must NOT happen: an executable, or a table of them, in a
/// Build message.
#[test]
fn the_accepted_fact_on_the_build_topic_carries_no_executable_and_a_twelve_node_fabric_fits_one_message() {
    let named = twelve_node_topology();
    assert_eq!(named.meshes.values().map(|m| m.nodes.len()).sum::<usize>(), 12);
    let nodes: Vec<NodeSpec> = named.meshes.values().flat_map(|m| m.spec_by_path.values().cloned()).collect();
    let fact = |t: FabricTopology, nodes: Vec<NodeSpec>| BuildFact::Accepted(BuildAccepted { build_id: bid("3qcfk1kq47m9"), topology: t, submitted_change: Some(TopologyChange::Reconcile { topology_storage_provider: None, based_on: bid("2qcfk1kq47m8"), nodes }), submitted_at_ms: 1_700_000_000_000 });
    let stripped_nodes: Vec<NodeSpec> = nodes.iter().map(|s| NodeSpec { executable: None, tags: Default::default(), config: Default::default(), ..s.clone() }).collect();
    let with = fact(named.clone(), nodes);
    let without = fact(without_executables(&named), stripped_nodes);

    let (messages, refused) = encode_chunks(vec![with.clone()]);
    assert!(refused.is_empty(), "a twelve-node fabric's accepted fact fits one gossip message: {refused:?}");
    assert_eq!(messages.len(), 1);
    assert!(messages[0].len() <= MAX_MESSAGE_BYTES);
    let back = BuildMessage::from_bytes(&messages[0]).unwrap();
    assert_eq!(back.facts, vec![without.clone()], "a Build message carries the fact with no executable");
    let bytes = |f: &BuildFact| BuildMessage { nonce: 7, facts: vec![f.clone()] }.to_bytes().unwrap();
    assert_eq!(bytes(&with), bytes(&without), "the executables add no byte to a Build message's frame");
}
