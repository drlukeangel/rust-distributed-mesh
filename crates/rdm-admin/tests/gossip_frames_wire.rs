//! R-W1 (Luke 2026-10-08), UNIT layer: run by `scripts/i143-acceptance-gate.sh i143-rw1-unit`, which
//! exports `I143_ACCEPTANCE_DIR`; the cell leaves its `result.json` there. A pure model cell:
//! no estate, no spans.
//!
//! CONTRACT: every internal mesh gossip frame is postcard. Every `Frame` variant of the
//! topology channels and every shape a `BuildMessage` carries (each Build fact; the removed Fabric
//! record refused) encodes, decodes to the value it was
//! made from, and re-encodes to the same bytes; a chunk the packers produce encodes under
//! `MAX_MESSAGE_BYTES`; a frame this build does not read is refused with a named reason, a
//! JSON frame included.

use rdm_core::meta::{NodeSpec, PersistentDisposition, StoragePersistence};
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PathName, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rdm_transport::chunking::MAX_MESSAGE_BYTES;
use rdm_transport::topology::Frame;
use rdm_transport::snapshot::{chunks_of, Full, PublisherId};
use rdm_admin::accepted::{FabricTopology, MeshTopology};
use rdm_core::build::BuildChange;
use rdm_admin::build::{BuildId, MeshDesired};
use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildStateError, StepOutcome};
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
        node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
            node_id: NodeId::mint(),
            path: "mesh1.node".into(), name: (ordinal.to_string()).to_string(),
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
        data_dir: full.then(|| "/var/lib/rafka/mesh1.node".to_string()),
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

fn op(action: &str, name: PathName, mesh: &str, r#type: &str) -> LifecycleOp {
    let node_id = NodeId::mint();
    LifecycleOp { r#type: r#type.to_string(), mesh: mesh.into(), build_id: "3qcfk1kq47m9".into(), operation: rdm_core::OperationId::node(action, &node_id), node_id, incarnation: IncarnationId::mint(), path: name.path.clone(), name: name.name.clone(), event_at_rafka_ms: 7, traceparent: None, tracestate: None }
}

fn publisher() -> PublisherId {
    let node_id = NodeId::mint();
    PublisherId { incarnation: IncarnationId::mint(), node_id }
}

/// One explicit executor birth shared by these independent wire-shape samples.
fn executor() -> rdm_core::framing::Birth {
    rdm_core::framing::Birth { node_id: NodeId::parse("100000000001").unwrap(), incarnation: IncarnationId::parse("100000000002").unwrap() }
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
            forwarded_by: Some(executor().node_id.to_string()),
            topology_version: 9,
            published_at_rafka_ms: 11,
            snapshot_id: 5,
            chunk_index: 1,
            chunk_count: 3,
            digests: vec![digest(1, None, false), digest(2, Some(process_runtime()), true)],
            in_flight: vec![op("node.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node")],
            deleted: vec![op("node.delete", PathName::new("mesh2.node", "3").unwrap(), "mesh2", "node")],
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
            removed: vec![NodeId::mint().to_string()],
            in_flight: vec![op("node.stop", PathName::new("mesh2.node", "4").unwrap(), "mesh2", "node")],
            deleted: vec![],
        },
        Frame::NodeDeleting { op: op("node.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None, publisher: p.clone(), topology_version: 10 },
        Frame::NodeDeleted { op: op("node.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(executor().node_id.to_string()) },
        Frame::NodeRestarting { op: op("node.restart", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None, publisher: p.clone(), topology_version: 11 },
        Frame::NodeStopped { op: op("node.stop", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(executor().node_id.to_string()), publisher: p.clone(), topology_version: 12 },
        Frame::NodeConnectionsDeleting { op: op("node.connection.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None },
        Frame::NodeLeaving { op: op("node.stop", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(executor().node_id.to_string()) },
        Frame::NodeLeft { op: op("node.stop", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(NodeId::parse("200000000001").unwrap().to_string()) },
        Frame::NodeConnectionsDeleted { op: op("node.connection.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None },
        Frame::MeshStatus { mesh: "mesh2".into(), status: "ready".into(), publisher: NodeId::parse("200000000001").unwrap().to_string(), forwarded_by: None, changed_at_rafka_ms: 77 },
        Frame::FabricStatus { fabric: fabric(), status: "ready".into(), publisher: executor().node_id.to_string(), forwarded_by: Some(NodeId::parse("200000000001").unwrap().to_string()), changed_at_rafka_ms: 78 },
        Frame::Concern { role: rdm_core::PrimaryRole::FabricPrimary, node_id: rdm_core::NodeId::mint(), incarnation: IncarnationId::mint(), observer: NodeId::parse("200000000001").unwrap().to_string() },
        Frame::NewFabricPrimary { primary: rdm_core::Primary { mesh: "mesh2".into(), node_id: rdm_core::NodeId::mint(), incarnation: IncarnationId::mint(), revision: 4 }, endpoint_id: "key2".into(), transport_addr: "127.0.0.1:41002".parse().unwrap(), admin_api_base: Some("http://127.0.0.1:41003".into()) },
    ]
}

fn bid(s: &str) -> BuildId {
    BuildId::parse(s).unwrap()
}

fn topology() -> FabricTopology {
    let mut t = FabricTopology::root("fabric-1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap());
    let mut m2 = MeshTopology::of(&MeshDesired::of("mesh2", [("node-admin", 1), ("node", 2), ("broker", 1)]));
    let broker: PathName = rdm_core::PathName::new("mesh2.broker", "1").unwrap();
    let rpc: PathName = rdm_core::PathName::new("mesh2.node", "1").unwrap();
    m2.set_spec(NodeSpec { storage_persistence: StoragePersistence::Persistent { disposition: PersistentDisposition::Release }, ..NodeSpec::default_for(broker, "mesh2", "broker") }).unwrap();
    m2.set_spec(NodeSpec { storage_persistence: StoragePersistence::Ephemeral, ..NodeSpec::default_for(rpc, "mesh2", "node") }).unwrap();
    t.meshes.insert("mesh2".into(), m2);
    // A Build message carries no topology_storage_provider (Q184: it is written in the records, like the
    // executable), so the shape that round-trips names none.
    t.topology_storage_provider = None;
    t
}

fn changes() -> Vec<Option<BuildChange>> {
    let node = |path: &str, name: &str, mesh: &str, r#type: &str| rdm_core::build::Nodes { path: path.into(), name: name.into(), mesh: mesh.into(), r#type: r#type.into(), storage_persistence: None, executable: None, tags: Default::default(), config: Default::default() };
    vec![
        None,
        Some(BuildChange::Create { meshes: vec![rdm_core::build::Meshes { name: "mesh2".into() }], nodes: vec![node("mesh2.node-admin", "1", "mesh2", "node-admin"), node("mesh2.broker", "1", "mesh2", "broker")] }),
        Some(BuildChange::Create { meshes: Vec::new(), nodes: vec![node("mesh2.gateway", "1", "mesh2", "gateway")] }),
        Some(BuildChange::Delete { meshes: Vec::new(), node_ids: vec![rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh2.node", "2").unwrap())] }),
        Some(BuildChange::Delete { meshes: vec!["mesh2".into()], node_ids: Vec::new() }),
        Some(BuildChange::DeleteFabric),
    ]
}

/// Every Build fact: the acceptance of each change, and the history's forget. A step is an
/// `rdm.build.step` event, never a Build fact (build.md §5).
fn facts() -> Vec<BuildFact> {
    let mut out = Vec::new();
    for change in changes() {
        out.push(BuildFact::Accepted(BuildAccepted { build_id: bid("3qcfk1kq47m9"), topology: topology(), change: change, source: None, time: 55 }));
    }
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
    let overlays: Vec<LifecycleOp> = (0..12).map(|_| op("node.delete", PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node")).collect();
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
    for (mesh, r#type) in [("mesh1", "node"), ("mesh2", "broker"), ("mesh3", "gateway")] {
        let mut m = MeshTopology::of(&MeshDesired::of(mesh, [("node-admin", 2), (r#type, 2)]));
        for p in m.nodes.clone() {
            let entry_kind = m.spec(&p).unwrap().r#type.clone();
            let id = entry_kind.replace('-', "_");
            let executable = rdm_core::binding::Binding { launch_id: id.clone(), executable: format!("/opt/rafka/candidates/0123456789abcdef/{id}").into(), sha256: "ab".repeat(32), image: Some("rafka-node-runtime:0123456789abcdef".into()) };
            let config = [("zone".to_string(), "a".to_string())].into();
            m.set_spec(NodeSpec { storage_persistence: StoragePersistence::Ephemeral, executable: Some(executable), tags: ["zone_a".to_string()].into(), config, ..NodeSpec::default_for(p.clone(), mesh, &entry_kind) }).unwrap();
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
    let fact = |t: FabricTopology| BuildFact::Accepted(BuildAccepted { build_id: bid("3qcfk1kq47m9"), topology: t, change: None, source: None, time: 1_700_000_000_000 });
    let with = fact(named.clone());
    let without = fact(without_executables(&named));

    let (messages, refused) = encode_chunks(vec![with.clone()]);
    assert!(refused.is_empty(), "a twelve-node fabric's accepted fact fits one gossip message: {refused:?}");
    assert_eq!(messages.len(), 1);
    assert!(messages[0].len() <= MAX_MESSAGE_BYTES);
    let back = BuildMessage::from_bytes(&messages[0]).unwrap();
    assert_eq!(back.facts, vec![without.clone()], "a Build message carries the fact with no executable");
    let bytes = |f: &BuildFact| BuildMessage { nonce: 7, facts: vec![f.clone()] }.to_bytes().unwrap();
    assert_eq!(bytes(&with), bytes(&without), "the executables add no byte to a Build message's frame");
}
