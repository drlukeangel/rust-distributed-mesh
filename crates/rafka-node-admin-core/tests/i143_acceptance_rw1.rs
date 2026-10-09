//! R-W1 (Luke 2026-10-08), UNIT layer: run by `scripts/i143-acceptance-gate.sh i143-rw1-unit`, which
//! exports `I143_ACCEPTANCE_DIR`; the cell leaves its `result.json` there. A pure model cell:
//! no estate, no spans.
//!
//! CONTRACT: every internal mesh gossip frame is postcard. Every `Frame` variant of the
//! membership channels and every shape the Build topic's `BuildMessage` carries (each Build fact,
//! each typed step output, the Fabric record, the shutdown) encodes, decodes to the value it was
//! made from, and re-encodes to the same bytes; a chunk the packers produce encodes under
//! `MAX_MESSAGE_BYTES`; a frame this build does not read is refused with a named reason, a
//! JSON frame included; a step output that has no wire shape is refused naming its Build,
//! attempt and step while the facts around it still travel.

use rafka_mesh_entity::meta::{NodeMeta, PersistentRetireDisposition, StorageMeta};
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeKind, PathName, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_mesh_transport::chunking::MAX_MESSAGE_BYTES;
use rafka_mesh_transport::membership::Frame;
use rafka_mesh_transport::snapshot::{chunks_of, Full, PublisherId};
use rafka_node_admin_core::accepted::{AttemptAction, FabricTopology, MeshTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildId, FabricDesired, MeshDesired};
use rafka_node_admin_core::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateError, BuildStepReceipt, StepOutcome};
use rafka_node_admin_core::deployment::pipeline::Bound;
use rafka_node_admin_core::deployment::pipeline::{AdmissionClosure, CreateStep, DrainOutcome, RetireStep, StorageDisposition};
use rafka_node_admin_core::deployment::provider::DeploymentHandle;
use rafka_node_admin_core::fabric_builds::{encode_chunks, BuildMessage};
use rafka_node_admin_core::fabric_storage::{FabricRecord, FabricShutdown};
use rafka_node_admin_core::model::{DeploymentId, ProviderKind};
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
    MeshDigest {
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
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: full.then(|| "http://127.0.0.1:7000".to_string()),
        digest_seq: 3 + ordinal as u64,
        emitted_at_rafka_ms: 1_000 + ordinal as u64,
        data_dir: full.then(|| "/var/lib/rafka/mesh1.rpc".to_string()),
        mesh_id: full.then(MeshId::mint),
        in_flight: full.then_some(4),
        extra: if full { [("role".to_string(), "proof".to_string())].into() } else { Default::default() },
    }
}

fn process_runtime() -> RuntimeFact {
    RuntimeFact { deployment_id: "dep1".into(), provider: RuntimeProvider::Process, control_domain: "host-a".into(), locator: RuntimeLocator::Process { pid: 4242, start: 987_654 } }
}

fn container_runtime() -> RuntimeFact {
    RuntimeFact { deployment_id: "dep2".into(), provider: RuntimeProvider::Container, control_domain: "docker-a".into(), locator: RuntimeLocator::Container { id: "f".repeat(64) } }
}

fn op(operation: &str) -> LifecycleOp {
    LifecycleOp { build_id: "bld-1".into(), attempt: 2, operation: operation.into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.1".parse().unwrap(), event_at_rafka_ms: 7 }
}

fn publisher() -> PublisherId {
    PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() }
}

/// Every `Frame` variant, each digest shape (absent and present optionals, both runtime locators).
fn frames() -> Vec<Frame> {
    let p = publisher();
    vec![
        Frame::Digest { digest: digest(1, None, false) },
        Frame::Digest { digest: digest(2, Some(process_runtime()), true) },
        Frame::Digest { digest: digest(3, Some(container_runtime()), true) },
        Frame::Members {
            mesh: "mesh2".into(),
            publisher: p.clone(),
            forwarded_by: Some("mesh1.admin.1".into()),
            topology_version: 9,
            published_at_rafka_ms: 11,
            snapshot_id: 5,
            chunk_index: 1,
            chunk_count: 3,
            digests: vec![digest(1, None, false), digest(2, Some(process_runtime()), true)],
            in_flight: vec![op("retire-node:mesh2.rpc.2")],
            departed: vec![op("retire-node:mesh2.rpc.3")],
        },
        Frame::Members {
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
            departed: vec![],
        },
        Frame::MembersDelta {
            mesh: "mesh2".into(),
            source_publisher: p,
            base_version: 8,
            topology_version: 9,
            published_at_rafka_ms: 12,
            changed: vec![digest(4, Some(container_runtime()), true)],
            removed: vec!["mesh2.rpc.9".into()],
            in_flight: vec![op("restart-node:mesh2.rpc.4")],
            departed: vec![],
        },
        Frame::NodeDeleting { op: op("retire-node:mesh2.rpc.2"), forwarded_by: None },
        Frame::NodeDeleted { op: op("retire-node:mesh2.rpc.2"), forwarded_by: Some("mesh1.admin.1".into()) },
        Frame::NodeRestarting { op: op("restart-node:mesh2.rpc.2"), forwarded_by: None },
        Frame::MeshStatus { mesh: "mesh2".into(), status: "ready-for-traffic".into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 77 },
        Frame::FabricStatus { fabric: fabric(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: Some("mesh2.admin.1".into()), changed_at_rafka_ms: 78 },
        Frame::Seated { seat: rafka_mesh_entity::Seat::FabricPrimary, holder: rafka_mesh_entity::SeatHolder { mesh: "mesh2".into(), node_id: rafka_mesh_entity::NodeId::mint(), incarnation: IncarnationId::mint(), epoch: 3 } },
        Frame::Seated { seat: rafka_mesh_entity::Seat::MeshPrimary, holder: rafka_mesh_entity::SeatHolder { mesh: "mesh1".into(), node_id: rafka_mesh_entity::NodeId::mint(), incarnation: IncarnationId::mint(), epoch: 1 } },
        Frame::Concern { seat: rafka_mesh_entity::Seat::FabricPrimary, node_id: rafka_mesh_entity::NodeId::mint(), incarnation: IncarnationId::mint(), observer: "mesh2.admin.1".into() },
    ]
}

fn bid(s: &str) -> BuildId {
    BuildId(s.into())
}

fn topology() -> FabricTopology {
    let mut t = FabricTopology::root("fabric-1", "mesh1");
    let mut m2 = MeshTopology::of(&MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 2), (NodeKind::Broker, 1)]));
    let broker: PathName = "mesh2.broker.1".parse().unwrap();
    let rpc: PathName = "mesh2.rpc.1".parse().unwrap();
    m2.set_meta(&broker, NodeMeta { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }, placement: Default::default() }).unwrap();
    m2.set_meta(&rpc, NodeMeta { storage: StorageMeta::Ephemeral, placement: Default::default() }).unwrap();
    t.meshes.insert("mesh2".into(), m2);
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
    ]
}

fn step(step: &str, output: Option<Value>) -> BuildFact {
    BuildFact::Step(BuildStepReceipt { build_id: bid("bld-1"), attempt: 2, operation: "create-node:mesh2.rpc.1".into(), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some("key@127.0.0.1:1".into()) })
}

fn to_json<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).unwrap()
}

/// Every Build fact, each step output as the pipeline commits it (the JSON of the typed result).
fn facts() -> Vec<BuildFact> {
    let mut out = Vec::new();
    for change in changes() {
        out.push(BuildFact::Accepted(BuildAccepted { build_id: bid("bld-1"), topology: topology(), submitted_change: change, submitted_at_ms: 55 }));
    }
    let action = |a: Option<AttemptAction>| BuildFact::Opened(AttemptOpened { build_id: bid("bld-1"), attempt: 2, reason: AttemptReason::Restart, action: a, opened_by: "mesh1.admin.1".into(), opened_at_ms: 56 });
    out.push(action(None));
    out.push(action(Some(AttemptAction::Restart { path: "mesh2.rpc.1".parse().unwrap(), from_incarnation: IncarnationId::mint() })));
    out.push(action(Some(AttemptAction::Replace { path: "mesh2.rpc.1".parse().unwrap(), from_incarnation: IncarnationId::mint() })));
    out.push(BuildFact::Claim(BuildAttemptClaim { build_id: bid("bld-1"), attempt: 2, executor: "mesh1.admin.1".into() }));
    out.push(step("PrepareNetwork", None));
    out.push(BuildFact::Step(BuildStepReceipt {
        build_id: bid("bld-1"),
        attempt: 2,
        operation: "create-node:mesh2.rpc.1".into(),
        step: "WaitForBind".into(),
        outcome: StepOutcome::Failed { reason: "runtime exited (code Some(1)) before binding".into() },
        output: None,
        executor: None,
    }));
    out.push(step(CreateStep::AllocateIdentity.name(), Some(json!({ "node_id": to_json(&NodeId::mint()), "incarnation": to_json(&IncarnationId::mint()), "supersedes": null, "deployment_id": "dep-1" }))));
    out.push(step(CreateStep::AllocateIdentity.name(), Some(json!({ "node_id": to_json(&NodeId::mint()), "incarnation": to_json(&IncarnationId::mint()), "supersedes": to_json(&IncarnationId::mint()), "deployment_id": "dep-1" }))));
    out.push(step(CreateStep::WaitForBind.name(), Some(to_json(&Bound { transport: "127.0.0.1:41000".parse().unwrap(), listeners: vec![("control".into(), "127.0.0.1:41001".parse().unwrap())] }))));
    out.push(step(CreateStep::PrepareStorage.name(), Some(to_json(&EndpointId("abcd".into())))));
    out.push(step(
        CreateStep::DeployRuntime.name(),
        Some(to_json(&DeploymentHandle { deployment_id: DeploymentId("dep-1".into()), provider: ProviderKind::Process, pid: Some(4242), start: Some(99), container: None, domain: Some("host-a".into()) })),
    ));
    out.push(step(
        CreateStep::DeployRuntime.name(),
        Some(to_json(&DeploymentHandle { deployment_id: DeploymentId("dep-2".into()), provider: ProviderKind::Container, pid: None, start: None, container: Some("c".repeat(64)), domain: None })),
    ));
    let evidence = |data_dir: Option<&str>| {
        let mut v = json!({ "deployment_id": "dep-1", "provider": "process", "provider_control_domain_fingerprint": "aa11", "runtime_locator_kind": "process-pid-start", "runtime_locator_fingerprint": "bb22" });
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
    out.push(step(CreateStep::ApplyMeshPending.name(), Some(json!({ "applied": null, "reason": "not a mesh's first admin" }))));
    for s in [RetireStep::NodeDeleting, RetireStep::NodeRestarting, RetireStep::NodeDeleted] {
        out.push(step(s.name(), Some(to_json(&op("retire-node:mesh2.rpc.1")))));
    }
    for d in [
        DrainOutcome::Established { in_flight: 3 },
        DrainOutcome::NotSent { reason: "NoRoute".into() },
        DrainOutcome::Indeterminate { reason: "no reply".into() },
        DrainOutcome::Refused { reply: "StaleIncarnation".into() },
        DrainOutcome::Deadline { last_in_flight: Some(1) },
        DrainOutcome::Deadline { last_in_flight: None },
    ] {
        out.push(step(RetireStep::MarkDraining.name(), Some(to_json(&d))));
        out.push(step(RetireStep::WaitForDrain.name(), Some(to_json(&d))));
    }
    for a in [AdmissionClosure::Heard, AdmissionClosure::ThisRuntimeExited, AdmissionClosure::Deadline { last_refusal: "busy".into() }, AdmissionClosure::DrainNotEstablished] {
        out.push(step(RetireStep::CloseRpcAdmission.name(), Some(to_json(&a))));
    }
    for s in [StorageDisposition::Released { locator: "/data/x".into() }, StorageDisposition::Preserved, StorageDisposition::PreservedNoMeta] {
        out.push(step(RetireStep::ReleaseStorage.name(), Some(to_json(&s))));
    }
    out.push(BuildFact::Attempt(BuildAttemptReceipt { build_id: bid("bld-1"), attempt: 2, outcome: AttemptOutcome::Converged }));
    out.push(BuildFact::Attempt(BuildAttemptReceipt { build_id: bid("bld-1"), attempt: 2, outcome: AttemptOutcome::Failed { reason: "step failed".into() } }));
    out.push(BuildFact::Attempt(BuildAttemptReceipt { build_id: bid("bld-1"), attempt: 3, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }));
    out.push(BuildFact::Forget { build_id: bid("bld-1") });
    out
}

#[test]
fn every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling() {
    let mut result = serde_json::Map::new();

    // ---- every Frame variant: decode(encode(f)) is f, and re-encodes to the same bytes ----
    let all_frames = frames();
    for f in &all_frames {
        let bytes = f.encode();
        let back = Frame::decode(&bytes).unwrap_or_else(|e| panic!("{f:?} does not decode: {e}"));
        assert_eq!(format!("{back:?}"), format!("{f:?}"), "a frame decodes to the value it was made from");
        assert_eq!(back.encode(), bytes, "and re-encodes to the same bytes");
        assert!(bytes.len() <= MAX_MESSAGE_BYTES, "{f:?} fits one gossip message");
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
        assert!(back.fabric.is_none() && back.shutdown.is_none());
    }
    result.insert("build_fact_shapes_round_tripped".into(), json!(all_facts.len()));

    // The Fabric record (with and without its Build) and a shutdown ride a message of their own.
    let record = |build_id: Option<BuildId>| FabricRecord { fabric_id: fabric(), name: "fabric-1".into(), build_id };
    let shutdown = FabricShutdown { initiated_by: "mesh1.admin.1".into(), initiated_by_node_id: NodeId::mint().to_string(), initiated_at_ms: 99 };
    for r in [record(None), record(Some(bid("bld-1")))] {
        let m = BuildMessage { nonce: 7, facts: vec![], fabric: Some(r.clone()), shutdown: Some(shutdown.clone()) };
        let bytes = m.to_bytes().unwrap();
        let back = BuildMessage::from_bytes(&bytes).unwrap();
        assert_eq!((back.nonce, back.fabric, back.shutdown), (7, Some(r), Some(shutdown.clone())));
    }

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
    let overlays: Vec<LifecycleOp> = (0..12).map(|_| op("retire-node:mesh1.rpc.1")).collect();
    let full = Full::new(members.clone(), overlays.clone(), overlays.iter().take(5).cloned().collect());
    let p = publisher();
    let chunks = chunks_of(&full, |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
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
        departed,
    });
    assert!(chunks.len() > 2);
    let mut heard = 0;
    for c in &chunks {
        let bytes = c.encode();
        assert!(bytes.len() <= MAX_MESSAGE_BYTES, "a Members chunk of {} bytes fits the ceiling", bytes.len());
        match Frame::decode(&bytes).unwrap() {
            Frame::Members { digests, .. } => heard += digests.len(),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(heard, members.len(), "the chunks carry every member");
    result.insert("members_snapshot".into(), json!({ "members": members.len(), "chunks": chunks.len(), "largest_bytes": chunks.iter().map(|c| c.encode().len()).max(), "ceiling": MAX_MESSAGE_BYTES }));

    // ---- sizes: one digest, postcard against the JSON-shaped MeshDigest ----
    let one = digest(1, Some(process_runtime()), true);
    let wire_bytes = Frame::Digest { digest: one.clone() }.encode().len();
    let json_bytes = serde_json::to_vec(&one).unwrap().len();
    assert!(wire_bytes < json_bytes, "postcard {wire_bytes} B against JSON {json_bytes} B");
    result.insert("digest_bytes".into(), json!({ "postcard_frame": wire_bytes, "json_digest": json_bytes }));

    // ---- a frame this build does not read is refused by name ----
    let garbage = Frame::decode(&[0xff, 0xff, 0xff, 0xff]).unwrap_err().to_string();
    assert!(garbage.contains("postcard decode of 4 bytes as"), "{garbage}");
    let json_frame = Frame::decode(br#"{"frame":"digest","digest":{}}"#).unwrap_err().to_string();
    assert!(json_frame.contains("postcard decode"), "a JSON frame is refused, not read: {json_frame}");
    let mut padded = all_frames[5].encode();
    padded.push(0);
    let trailing = Frame::decode(&padded).unwrap_err().to_string();
    assert!(trailing.contains("1 bytes left after the frame"), "{trailing}");
    let build_garbage = BuildMessage::from_bytes(br#"{"nonce":1,"facts":[]}"#).err().expect("a JSON Build message is refused").to_string();
    assert!(build_garbage.contains("postcard decode"), "{build_garbage}");
    result.insert("refusals".into(), json!({ "garbage": garbage, "json_frame": json_frame, "trailing_bytes": trailing, "json_build_message": build_garbage }));

    // ---- an output with no wire shape is refused naming Build, attempt and step; neighbours travel ----
    let good = step(CreateStep::WaitForBind.name(), Some(to_json(&Bound { transport: "127.0.0.1:41000".parse().unwrap(), listeners: vec![] })));
    let stray = step(CreateStep::Complete.name(), Some(json!({ "anything": 1 })));
    let mistyped = step(CreateStep::AllocateIdentity.name(), Some(json!({ "pid": 7 })));
    let (messages, refused) = encode_chunks(vec![good.clone(), stray, mistyped, good.clone()]);
    let named: Vec<String> = refused.iter().map(|e| e.to_string()).collect();
    assert_eq!(named.len(), 2, "{named:?}");
    assert!(refused.iter().all(|e| matches!(e, BuildStateError::Unencodable(_))), "{named:?}");
    assert!(named[0].contains("step Complete (Build bld-1, attempt 2)") && named[0].contains("commits no output"), "{}", named[0]);
    assert!(named[1].contains("step AllocateIdentity (Build bld-1, attempt 2)"), "{}", named[1]);
    let sent: usize = messages.iter().map(|m| BuildMessage::from_bytes(m).unwrap().facts.len()).sum();
    assert_eq!(sent, 2, "the facts around a refused one still travel");
    result.insert("unencodable".into(), json!({ "refused": named, "sent": sent }));

    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&Value::Object(result)).unwrap()).unwrap();
}
