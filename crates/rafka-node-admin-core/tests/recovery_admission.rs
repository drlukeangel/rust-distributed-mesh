//! The fabric-primary's admission of a node recovery (node-recovery.md "Execution", #3051 cell 4):
//! the mesh-primary submits one recovery Build keyed on the exited birth `(node.id, incarnation)`;
//! the fabric-primary admits it as the next attempt of the accepted Build under the one-Build-in-
//! flight rule, and a repeat submit answers `AlreadyApplied` or `BuildInProgress`, never a second
//! recovery.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::build_state::AttemptReason;
use rafka_node_admin_core::http::{ActionKind, ControlPlane, Recovery, Refusal};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt;

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    if n.kind == NodeKind::NodeAdmin {
        n.is_fabric_primary = primary;
    }
    n
}

/// One mesh of two node-admins and two rpc nodes, every one of them in the accepted Build.
fn view() -> Topology {
    let exited = node("mesh1.rpc.2", false);
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.admin.2", false), node("mesh1.rpc.1", true), exited],
    }
}

struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    pipeline: rafka_mesh_telemetry::SpanCapture,
}

impl Capture {
    fn spans(&self, name: &str) -> Vec<(String, std::collections::BTreeMap<String, String>)> {
        self.pipeline.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| (s.span_context.trace_id().to_string(), s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect()))
            .collect()
    }
}

/// The Build `id`'s first attempt claimed and converged: it is settled.
async fn settle_other(builds: &MemoryBuildStateAdapter, id: &rafka_node_admin_core::build::BuildId) {
    let attempt = builds.read_build(id).await.unwrap().attempt;
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
}

fn opened(facts: &[BuildFact]) -> usize {
    facts.iter().filter(|f| matches!(f, BuildFact::Opened(_))).count()
}

/// CONTRACT (#3051 acceptance 4, node-recovery.md "Numbered end-to-end sequence" step 1: "A repeat
/// submit answers AlreadyApplied or BuildInProgress, never a second recovery"): the fabric-primary
/// admits the first recovery of `mesh1.rpc.2`'s exited birth as the next attempt of the accepted
/// Build. A repeat while that attempt is open answers `BuildInProgress`; a repeat once its three
/// operations (node.delete of the exited birth, node.create, node.update) completed answers
/// `AlreadyApplied` naming the attempt that recovered it; a recovery submitted while another Build runs answers that Build's
/// `BuildInProgress`. A node-admin whose mesh holds no other live node-admin is mesh recovery, never
/// node recovery: refused by name. A repeat after a later
/// topology Build was accepted is still `AlreadyApplied`: the recovery is keyed on the exited birth
/// across the Build log, not on the accepted Build's receipts. Must NOT happen: a second attempt
/// opened for a birth already recovered.
#[tokio::test]
async fn a_duplicate_recovery_submit_for_the_same_exited_birth_opens_one_recovery() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-3051-cell4", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view();
    let path: PathName = "mesh1.rpc.2".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    // `mesh1.rpc.2` has gone silent since: TRUE OFFLINE, Dead in the view.
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    let settle = |attempt: u32| {
        let (builds, b0) = (builds.clone(), b0.clone());
        async move {
            builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
            builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0, attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
        }
    };
    settle(1).await;

    // The first submit: the next attempt of the accepted Build, fenced to the exited birth.
    let first = match cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("the first recovery is admitted: {other:?}"),
    };
    assert_eq!((first.build_id.clone(), first.attempt), (b0.clone(), 2), "the recovery is the accepted Build's next attempt");
    let p = builds.read_build(&b0).await.unwrap();
    assert_eq!(p.reason.as_str(), "recovery");
    assert_eq!(serde_json::to_value(&p.action).unwrap()["action"], "recover", "{:?}", p.action);
    let after_first = opened(&builds.facts().await.unwrap());

    // A repeat while the recovery attempt is open.
    let repeat = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&repeat, Err(Refusal::BuildInProgress(id)) if *id == b0), "{repeat:?}");

    // The executor completes the recovery of the exited birth: its delete, create and update.
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt: 2, executor: "mesh1.admin.1".into() }).await.unwrap();
    for r in recovery_receipts(&b0, 2, &path, &node_id, &birth, "mesh1.admin.1") {
        builds.append_step_receipt(&r).await.unwrap();
    }
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 2, outcome: AttemptOutcome::Converged }).await.unwrap();

    // A repeat once the birth was recovered.
    let again = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&again, Ok(Recovery::AlreadyApplied(o)) if *o == first), "a repeat once the birth was recovered names the attempt that recovered it, and opens nothing: {again:?}");
    assert_eq!(opened(&builds.facts().await.unwrap()), after_first, "exactly one recovery attempt was opened for the exited birth");

    // A node-admin whose mesh holds no other live node-admin is its mesh's lost mesh-primary: mesh
    // recovery, never node recovery (mesh-recovery.md:5-9).
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.admin.2").for_each(|n| n.status = NodeStatus::Dead);
    let admin = cp.topology.read().await.node(&"mesh1.admin.1".parse().unwrap()).unwrap().clone();
    let refused = cp.open_recovery("build.create", admin.name.clone(), admin.node_id.clone(), admin.incarnation_id.clone().unwrap()).await;
    assert!(matches!(&refused, Err(Refusal::Reject(r)) if r.reason() == "node-admin-not-node-recovered"), "the node recovery of a mesh's last node-admin is refused by name: {refused:?}");

    // Another Build in flight: the recovery waits for it.
    let other = cp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await.expect("a topology change is accepted");
    let later = cp.open_recovery("build.create", path.clone(), NodeId::mint(), IncarnationId::mint()).await;
    assert!(matches!(&later, Err(Refusal::BuildInProgress(id)) if *id == other.build_id), "{later:?}");

    // The topology Build settles and is the accepted Build now: the exited birth's recovery lives in
    // an earlier Build's receipts. A repeat submit of it is still the recovery already applied,
    // never a recovery of the path that a new node now runs.
    settle_other(&builds, &other.build_id).await;
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id().as_ref(), Some(&other.build_id), "the topology Build is the accepted one");
    let before_late = opened(&builds.facts().await.unwrap());
    let late = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&late, Ok(Recovery::AlreadyApplied(o)) if *o == first), "a repeat after a later Build was accepted names the attempt that recovered the birth: {late:?}");
    assert_eq!(opened(&builds.facts().await.unwrap()), before_late, "no recovery attempt is opened on the later Build");

    let admitted = capture.spans("rdm.node_admin.build.update.via-recovery");
    assert_eq!(admitted.iter().map(|(_, a)| a["outcome"].as_str()).collect::<Vec<_>>(), vec!["admitted", "build-in-progress", "already-applied", "refused", "build-in-progress", "already-applied"], "one admission span per submit, its outcome named");
    assert_eq!(capture.spans("rdm.node_admin.build.reject.via-recovery-already-applied").len(), 2, "the duplicate is answered from the receipt that recovered the birth");
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-recovery-already-applied", "rdm.node_admin.build.reject.via-build-in-progress", "rdm.node_admin.build.reject.via-node-admin-not-node-recovered"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
}

/// CONTRACT (node-recovery.md:5-7 "Node recovery is RDM's drift reconciliation for a member that
/// exits outside any workflow" and "Failure and receipts": "An exit during a workflow belongs to
/// that workflow"): a birth a `replace-node` attempt of the accepted Build deleted (its
/// `NodeDeleted` receipt names the exact birth, and the operation completed) exited inside that
/// workflow. A recovery submitted for it afterwards (the mesh-primary's tickle proved it TRUE
/// OFFLINE and the provider proved it exited: the replace terminated it) answers `AlreadyApplied`,
/// naming the replace attempt, from the Build log keyed on the exited birth, and opens nothing.
/// Must NOT happen: a recovery attempt opened
/// for a birth a workflow already deleted, which deletes it again and creates over the data dir
/// that workflow released (gate 71b06a15, mesh_identity__canonical_ids: `recover-node:mesh2.rpc.2:
/// NodeDeleting failed: the inherited data dir .../mesh2.rpc.2-ekne99d5g23b does not exist`).
#[tokio::test]
async fn a_recovery_submit_for_a_birth_a_replace_deleted_is_answered_already_applied_by_the_replace() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-3016-replaced-birth", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view();
    let path: PathName = "mesh1.rpc.2".parse().unwrap();
    let replaced = t.node(&path).unwrap().clone();
    let (node_id, birth) = (replaced.node_id.clone(), replaced.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    // `mesh1.rpc.2` is replaced: attempt 2 terminates the birth, creates its successor at the
    // path.name and deletes the birth.
    let replace = cp.open_attempt("build.create", AttemptReason::Replace, path.clone(), ActionKind::Replace, Some(birth.clone())).await.expect("the replace is admitted");
    assert_eq!((replace.build_id.clone(), replace.attempt), (b0.clone(), 2));
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt: 2, executor: "mesh1.admin.1".into() }).await.unwrap();
    for r in deletion_receipts("replace-node", &b0, 2, &path, &node_id, &birth, "mesh1.admin.1") {
        builds.append_step_receipt(&r).await.unwrap();
    }
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 2, outcome: AttemptOutcome::Converged }).await.unwrap();
    // The mesh-primary's tick read the birth silent before the replace removed it: its tickle ends
    // TRUE OFFLINE and the provider proves the exit (the replace terminated it).
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    let before = opened(&builds.facts().await.unwrap());

    let submitted = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(
        matches!(&submitted, Ok(Recovery::AlreadyApplied(o)) if *o == replace),
        "the recovery of a birth the replace deleted names the replace attempt and opens nothing: {submitted:?}"
    );
    assert_eq!(opened(&builds.facts().await.unwrap()), before, "no recovery attempt is opened for a birth a workflow deleted");

    let applied = capture.spans("rdm.node_admin.build.reject.via-recovery-already-applied");
    assert_eq!(
        applied.iter().map(|(_, a)| a.get("operation").cloned().unwrap_or_default()).collect::<Vec<_>>(),
        vec![format!("replace-node:{path}")],
        "the answer names the workflow operation the birth's exit belongs to"
    );
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-recovery-already-applied"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
}

/// A node-admin on the fabric's Build topic: its own endpoint, gossip and Build log.
async fn gossip_admin(name: &str, fabric: &FabricId, peers: Vec<iroh::EndpointAddr>) -> (iroh::Endpoint, iroh::protocol::Router, Arc<rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter>) {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(iroh::SecretKey::generate())
        .alpns(vec![iroh_gossip::ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = iroh::protocol::Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let builds = rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter::join(
        &gossip,
        &rafka_mesh_transport::membership::LocationBook::new(&endpoint, name).unwrap(),
        fabric,
        peers,
        Arc::new(MemoryBuildStateAdapter::new()),
        Arc::new(AcceptedStore::new(Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new()), name)),
        rafka_node_admin_core::shutdown::ShutdownControl::memory(name).await,
        name.into(),
        rafka_node_admin_core::fabric_builds::SeatSource::unrecorded(),
        None,
    )
    .await
    .unwrap();
    (endpoint, router, Arc::new(builds))
}

/// Wait until `cond` holds of `builds`'s projection of `id`, naming what was awaited.
async fn until(what: &str, builds: &dyn BuildStateAdapter, id: &rafka_node_admin_core::build::BuildId, cond: impl Fn(&rafka_node_admin_core::build_state::BuildProjection) -> bool) {
    for _ in 0..400 {
        if builds.read_build(id).await.is_ok_and(|p| cond(&p)) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("{what}: never held; last projection {:?}", builds.read_build(id).await);
}

/// The step receipts a recovery attempt writes for the exited birth (node-recovery.md "Steps"), as
/// its three workflows write them: the delete's (`retire-node:<path>`: `NodeDeleting` and
/// `NodeDeleted` carry the lifecycle operation naming the exited birth, `Complete` commits no
/// output), then the create's `Complete` (`create-node:<path>`) and the update's (`update-node:<path>`);
/// the create's steps before its `Complete` and the update's `UpdateNode` are elided (they name the
/// new node, not the exited one).
fn recovery_receipts(build: &rafka_node_admin_core::build::BuildId, attempt: u32, path: &PathName, node_id: &NodeId, birth: &IncarnationId, executor: &str) -> Vec<BuildStepReceipt> {
    let mut receipts = deletion_receipts("retire-node", build, attempt, path, node_id, birth, executor);
    for kind in ["create-node", "update-node"] {
        receipts.push(BuildStepReceipt { mesh: None, build_id: build.clone(), attempt, operation: format!("{kind}:{path}"), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: Some(executor.into()) });
    }
    receipts
}

/// The step receipts of node workflow `kind` (`replace-node`, `retire-node`) at
/// `path` that deleted the exact birth: `NodeDeleting` and `NodeDeleted` carry the node.delete's
/// lifecycle operation naming that birth, as every deleting pipeline writes them; `Complete` ends
/// the operation. The steps between them name the successor, never the deleted birth: elided.
fn deletion_receipts(kind: &str, build: &rafka_node_admin_core::build::BuildId, attempt: u32, path: &PathName, node_id: &NodeId, birth: &IncarnationId, executor: &str) -> Vec<BuildStepReceipt> {
    let operation = format!("{kind}:{path}");
    let op = rafka_mesh_entity::LifecycleOp { build_id: build.to_string(), attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: birth.clone(), name: path.clone(), event_at_rafka_ms: 1 };
    let receipt = |step: &str, output: Option<serde_json::Value>| BuildStepReceipt { mesh: None, build_id: build.clone(), attempt, operation: operation.clone(), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some(executor.into()) };
    vec![
        receipt("NodeDeleting", Some(serde_json::to_value(&op).unwrap())),
        receipt("NodeDeleted", Some(serde_json::to_value(&op).unwrap())),
        receipt("Complete", None),
    ]
}

/// CONTRACT (node-recovery.md "Numbered end-to-end sequence" steps 1-2: the mesh-primary submits
/// the recovery to the fabric-primary, keyed on the exited birth, and "A repeat submit answers
/// AlreadyApplied or BuildInProgress, never a second recovery"): when the mesh-primary that
/// executes the recovery is NOT the fabric-primary, every receipt of its three operations rides the
/// fabric's Build topic to the fabric-primary, and the fabric-primary answers a repeat
/// submit for that exited birth `AlreadyApplied`, naming the attempt that recovered it, from the
/// receipts it heard. Must NOT happen: a receipt of the recovery that cannot be broadcast (the
/// executor's own log alone holds it), or a second recovery attempt opened by the fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_run_by_a_mesh_primary_that_is_not_the_fabric_primary_is_answered_already_applied_at_the_fabric_primary() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-3051-cross-mesh-dedup", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let t = view();
    let (fp_ep, fp_router, fp) = gossip_admin("mesh1.admin.1", &t.fabric.id, vec![]).await;
    let fp_addr = iroh::EndpointAddr::new(fp_ep.id()).with_ip_addr(fp_ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap());
    let (_mp_ep, mp_router, mp) = gossip_admin("mesh2.admin.1", &t.fabric.id, vec![fp_addr]).await;
    let path: PathName = "mesh1.rpc.2".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*fp, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(fp.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    fp.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    fp.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);

    // The mesh-primary's submit: admitted as the accepted Build's next attempt, claimed by the
    // mesh-primary at the fabric-primary.
    let first = match cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("the first recovery is admitted: {other:?}"),
    };
    assert_eq!(fp.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt: first.attempt, executor: "mesh2.admin.1".into() }).await.unwrap(), rafka_node_admin_core::build_state::ClaimOutcome::Won);
    until("the mesh-primary hears its claim of the recovery attempt", &*mp, &b0, |p| p.attempt == first.attempt && p.executor.as_deref() == Some("mesh2.admin.1")).await;

    // The mesh-primary runs the recovery: every receipt is appended to its own log and broadcast.
    for r in recovery_receipts(&b0, first.attempt, &path, &node_id, &birth, "mesh2.admin.1") {
        let step = r.step.clone();
        mp.append_step_receipt(&r).await.unwrap_or_else(|e| panic!("the recovery's {step} receipt is not broadcast to the fabric-primary: {e}"));
    }
    mp.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: first.attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    until("the fabric-primary hears the recovery attempt converge", &*fp, &b0, |p| p.state == rafka_node_admin_core::build_state::BuildState::Complete && p.attempt == first.attempt).await;
    assert_eq!(fp.recovery_of(&path, &node_id, &birth).await.unwrap(), Some((b0.clone(), first.attempt, format!("retire-node:{path}"))), "the fabric-primary's recovery index holds the exited birth from the heard receipts");

    // The repeat submit, at the fabric-primary.
    let before = opened(&fp.facts().await.unwrap());
    let again = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&again, Ok(Recovery::AlreadyApplied(o)) if *o == first), "a repeat once the mesh-primary recovered the birth names the attempt that recovered it: {again:?}");
    assert_eq!(opened(&fp.facts().await.unwrap()), before, "the fabric-primary opened no second recovery attempt");
    let applied = capture.spans("rdm.node_admin.build.reject.via-recovery-already-applied");
    assert_eq!(applied.len(), 1, "the duplicate is answered from the heard receipts");
    for (trace_id, attrs) in applied {
        eprintln!("span rdm.node_admin.build.reject.via-recovery-already-applied trace_id={trace_id} {attrs:?}");
    }
    fp_router.shutdown().await.unwrap();
    mp_router.shutdown().await.unwrap();
}

/// Two meshes: mesh1's node-admin holds the fabric-primary seat; mesh2's node-admin is mesh2's
/// mesh-primary and nothing more. Every node is in the accepted Build.
fn two_meshes() -> Topology {
    let mut fp = node("mesh1.admin.1", true);
    fp.is_fabric_primary = true;
    let mut mp = node("mesh2.admin.1", true);
    mp.is_fabric_primary = false;
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![
            Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
            Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
        ],
        nodes: vec![fp, node("mesh1.rpc.1", true), mp, node("mesh2.rpc.1", true)],
    }
}

/// The fabric-primary's and mesh2's mesh-primary's control planes over one Build log (what each
/// holds once the Build topic delivered the other's facts), the birth Build settled, and
/// `mesh2.rpc.1` proven exited (Dead in both views): (fabric-primary, mesh-primary, Build log, the
/// exited birth's path, node id and incarnation).
async fn fabric_of_two_meshes() -> (Arc<ControlPlane>, Arc<ControlPlane>, Arc<MemoryBuildStateAdapter>, PathName, NodeId, IncarnationId) {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = two_meshes();
    let path: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let fp = Arc::new(ControlPlane::new(builds.clone(), accepted.clone(), "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time()));
    let mp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh2.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = fp.builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;
    for cp in [&fp, &mp] {
        cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    }
    (fp, mp, builds, path, exited.node_id.clone(), exited.incarnation_id.clone().unwrap())
}

/// A refusal as the control route renders it.
async fn rendered(refusal: Refusal) -> serde_json::Value {
    use axum::response::IntoResponse as _;
    let body = axum::body::to_bytes(refusal.into_response().into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// CONTRACT (R-BI7, proposal Part C caveats 2 and 3; node-recovery.md as R-BI7 moves it: "the
/// mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's rows,
/// then run; no call to the fabric-primary"): a node.create runs in mesh1 (a topology Build, its
/// attempt claimed for mesh1's admin and running) when mesh2's mesh-primary proves its member
/// `mesh2.rpc.1` exited. The mesh-primary's own door opens the recovery as an attempt of the current
/// Build and claims it on mesh2's rows (`(build_id, mesh2, attempt)`, decided by mesh2's
/// mesh-primary, won); the node.create's attempt in mesh1 is untouched and still running. Must NOT
/// happen: the recovery refused because a Build runs in another mesh (`BuildInProgress`), or
/// refused because the submitter is not the fabric-primary (`NotAuthority`).
#[tokio::test]
async fn a_recovery_in_mesh2_is_opened_by_its_mesh_primary_while_a_node_create_runs_in_mesh1() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-r-bi7-two-meshes", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };
    let (fp, mp, builds, path, node_id, birth) = fabric_of_two_meshes().await;

    // node.create in mesh1: a topology Build, its first attempt claimed for mesh1's admin.
    let create = fp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await.expect("the node.create is accepted");
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: create.build_id.clone(), attempt: create.attempt, executor: "mesh1.admin.1".into() }).await.unwrap();

    // mesh2's mesh-primary opens its member's recovery at its own door.
    let recovery = mp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    let opened = match recovery {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("mesh2's mesh-primary opens the recovery of its member while a node.create runs in mesh1: {other:?}"),
    };
    assert_eq!(opened.build_id, create.build_id, "the recovery is an attempt of the current Build (no shape change, no new Build)");
    let p = builds.read_build(&create.build_id).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref(), p.state), (create.attempt, Some("mesh1.admin.1"), rafka_node_admin_core::build_state::BuildState::Running), "the node.create's attempt in mesh1 still runs: {p:?}");

    let claims = capture.spans("rdm.node_admin.build.update.via-claim-decision");
    let mine: Vec<_> = claims.iter().filter(|(_, a)| a.get("node").map(String::as_str) == Some("mesh2.admin.1")).collect();
    assert_eq!(mine.len(), 1, "mesh2's mesh-primary decides its recovery's claim itself: {claims:?}");
    let (_, claim) = mine[0];
    assert_eq!((claim["mesh"].as_str(), claim["outcome"].as_str(), claim["build_id"].as_str()), ("mesh2", "won", create.build_id.as_str()), "claimed on mesh2's rows: {claim:?}");
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.update.via-claim-decision"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
}

/// CONTRACT (R-BI7 caveat 3, proposal Part C: "A topology Build's operation on mesh X and mesh X's
/// own drift attempt cannot both be open: whichever claims first on mesh X's rows wins, the other is
/// refused build-in-progress naming the holder"; envelope row `BuildInProgress { current_build_id,
/// mesh, held_by }`): first, a node.create in mesh2 holds mesh2 (its attempt claimed by the
/// fabric-primary for mesh2's admin) and mesh2's mesh-primary's recovery of its exited member is
/// refused build-in-progress naming the Build, mesh2 and the fabric-primary that holds it. Then,
/// the node.create converged, the recovery opens and holds mesh2, and a second node.create in mesh2
/// is refused build-in-progress naming mesh2 and the mesh-primary that holds it. Must NOT happen:
/// both open on mesh2, or a refusal that does not say which mesh and who holds it.
#[tokio::test]
async fn a_topology_operation_and_a_drift_attempt_on_one_mesh_never_both_open() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-r-bi7-one-mesh", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };
    let (fp, mp, builds, path, node_id, birth) = fabric_of_two_meshes().await;

    // The fabric-primary's topology operation holds mesh2.
    let create = fp.submit("build.create", TopologyChange::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::RpcNode }).await.expect("the node.create is accepted");
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: create.build_id.clone(), attempt: create.attempt, executor: "mesh2.admin.1".into() }).await.unwrap();
    let refused = match mp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("the recovery is refused build-in-progress while the node.create holds mesh2: {other:?}"),
    };
    assert_eq!((refused["current_build_id"].as_str(), refused["mesh"].as_str(), refused["held_by"].as_str()), (Some(create.build_id.as_str()), Some("mesh2"), Some("mesh1.admin.1")), "the refusal names the Build, the mesh and its holder: {refused}");

    // The node.create converges; the mesh-primary's recovery opens and holds mesh2.
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: create.build_id.clone(), attempt: create.attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    let recovery = match mp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("the recovery opens once mesh2 is free: {other:?}"),
    };
    let second = match fp.submit("build.create", TopologyChange::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::RpcNode }).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("a node.create in mesh2 is refused build-in-progress while the recovery holds mesh2: {other:?}"),
    };
    assert_eq!((second["current_build_id"].as_str(), second["mesh"].as_str(), second["held_by"].as_str()), (Some(recovery.build_id.as_str()), Some("mesh2"), Some("mesh2.admin.1")), "the refusal names the Build, the mesh and the mesh-primary that holds it: {second}");
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-build-in-progress", "rdm.node_admin.build.update.via-claim-decision"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
}
