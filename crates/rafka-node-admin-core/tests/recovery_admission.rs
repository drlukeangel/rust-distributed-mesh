//! A node recovery's admission at its mesh-primary's own door (node-recovery.md "Execution"; R-BI7):
//! the member's mesh-primary opens one recovery keyed on the exited birth `(node.id, incarnation)`
//! as the next attempt of the current Build in its mesh's drift lane and claims it on its mesh's
//! rows; one attempt is in flight per mesh, and a repeat submit answers `AlreadyApplied` or
//! `BuildInProgress`, never a second recovery.

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
/// submit answers AlreadyApplied or BuildInProgress, never a second recovery"; R-BI7): mesh1's
/// mesh-primary opens the first recovery of `mesh1.rpc.2`'s exited birth as attempt 1 of mesh1's
/// drift lane of the current Build and claims it. A repeat while that attempt is open answers
/// `BuildInProgress` naming the Build, mesh1 and the mesh-primary holding it; a repeat once its three
/// operations (node.delete of the exited birth, node.create, node.update) completed answers
/// `AlreadyApplied` naming the attempt that recovered it; a recovery submitted while another Build's
/// attempt runs in mesh1 answers that Build's `BuildInProgress`. A node-admin whose mesh holds no other live node-admin is mesh recovery, never
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
    assert_eq!((first.build_id.clone(), first.attempt, first.mesh.as_deref()), (b0.clone(), 1, Some("mesh1")), "the recovery is attempt 1 of mesh1's drift lane of the current Build");
    let p = builds.read_build(&b0).await.unwrap();
    let lane = &p.lanes["mesh1"];
    assert_eq!((lane.reason.as_str(), lane.claimant.as_deref(), lane.executor.as_deref()), ("recovery", Some("mesh1.admin.1"), Some("mesh1.admin.1")), "opened and claimed by mesh1's mesh-primary: {lane:?}");
    assert_eq!(serde_json::to_value(&lane.action).unwrap()["action"], "recover", "{:?}", lane.action);
    let after_first = opened(&builds.facts().await.unwrap());

    // A repeat while the recovery attempt is open.
    let repeat = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&repeat, Err(Refusal::BuildInProgress(held)) if held.current_build_id == b0 && held.mesh.as_deref() == Some("mesh1") && held.held_by.as_deref() == Some("mesh1.admin.1")), "{repeat:?}");

    // The mesh-primary completes the recovery of the exited birth: its delete, create and update.
    for r in recovery_receipts(Some("mesh1"), &b0, 1, &path, &node_id, &birth, "mesh1.admin.1") {
        builds.append_step_receipt(&r).await.unwrap();
    }
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: Some("mesh1".into()), build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();

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

    // Another Build's attempt runs in mesh1 (claimed by the fabric-primary): the recovery waits for it.
    let other = cp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await.expect("a topology change is accepted");
    builds.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: other.build_id.clone(), attempt: other.attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    let later = cp.open_recovery("build.create", path.clone(), NodeId::mint(), IncarnationId::mint()).await;
    assert!(matches!(&later, Err(Refusal::BuildInProgress(held)) if held.current_build_id == other.build_id && held.mesh.as_deref() == Some("mesh1")), "{later:?}");

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
    for r in deletion_receipts(None, "replace-node", &b0, 2, &path, &node_id, &birth, "mesh1.admin.1") {
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

/// The step receipts a recovery attempt in `lane` writes for the exited birth (node-recovery.md "Steps"), as
/// its three workflows write them: the delete's (`retire-node:<path>`: `NodeDeleting` and
/// `NodeDeleted` carry the lifecycle operation naming the exited birth, `Complete` commits no
/// output), then the create's `Complete` (`create-node:<path>`) and the update's (`update-node:<path>`);
/// the create's steps before its `Complete` and the update's `UpdateNode` are elided (they name the
/// new node, not the exited one).
fn recovery_receipts(lane: Option<&str>, build: &rafka_node_admin_core::build::BuildId, attempt: u32, path: &PathName, node_id: &NodeId, birth: &IncarnationId, executor: &str) -> Vec<BuildStepReceipt> {
    let mut receipts = deletion_receipts(lane, "retire-node", build, attempt, path, node_id, birth, executor);
    for kind in ["create-node", "update-node"] {
        receipts.push(BuildStepReceipt { mesh: lane.map(str::to_string), build_id: build.clone(), attempt, operation: format!("{kind}:{path}"), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: Some(executor.into()) });
    }
    receipts
}

/// The step receipts of node workflow `kind` (`replace-node`, `retire-node`) at
/// `path` that deleted the exact birth: `NodeDeleting` and `NodeDeleted` carry the node.delete's
/// lifecycle operation naming that birth, as every deleting pipeline writes them; `Complete` ends
/// the operation. The steps between them name the successor, never the deleted birth: elided. `lane`
/// is the attempt's lane (`None` the fabric lane, `Some(mesh)` that mesh's drift lane).
#[allow(clippy::too_many_arguments)]
fn deletion_receipts(lane: Option<&str>, kind: &str, build: &rafka_node_admin_core::build::BuildId, attempt: u32, path: &PathName, node_id: &NodeId, birth: &IncarnationId, executor: &str) -> Vec<BuildStepReceipt> {
    let operation = format!("{kind}:{path}");
    let op = rafka_mesh_entity::LifecycleOp { build_id: build.to_string(), attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: birth.clone(), name: path.clone(), event_at_rafka_ms: 1 };
    let receipt = |step: &str, output: Option<serde_json::Value>| BuildStepReceipt { mesh: lane.map(str::to_string), build_id: build.clone(), attempt, operation: operation.clone(), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some(executor.into()) };
    vec![
        receipt("NodeDeleting", Some(serde_json::to_value(&op).unwrap())),
        receipt("NodeDeleted", Some(serde_json::to_value(&op).unwrap())),
        receipt("Complete", None),
    ]
}

/// CONTRACT (R-BI7: "the mesh-primary opens the recovery attempt of the current Build itself: claim
/// on its mesh's rows, then run; no call to the fabric-primary"; node-recovery.md "Numbered
/// end-to-end sequence" step 1: "A repeat submit answers AlreadyApplied or BuildInProgress, never a
/// second recovery"): mesh2's mesh-primary, which is not the fabric-primary, opens and claims the
/// recovery of its member `mesh2.rpc.1` in mesh2's drift lane on its own Build log. Every fact of
/// the attempt (its open, its claim, its receipts) rides the fabric's Build topic to the
/// fabric-primary, whose fold holds mesh2's lane claimed by the mesh-primary, and whose door
/// answers a repeat submit for that exited birth `AlreadyApplied`, naming the attempt, from the
/// facts it heard. Must NOT happen: a claim decided at the fabric-primary, a fact of the recovery
/// the fabric-primary never hears, or a second recovery attempt opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_a_mesh_primary_opens_and_claims_on_its_own_log_is_heard_by_the_fabric_primary_and_answered_already_applied_there() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-r-bi7-heard-at-fabric-primary", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let t = two_meshes();
    let (fp_ep, fp_router, fp) = gossip_admin("mesh1.admin.1", &t.fabric.id, vec![]).await;
    let fp_addr = iroh::EndpointAddr::new(fp_ep.id()).with_ip_addr(fp_ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap());
    let (_mp_ep, mp_router, mp) = gossip_admin("mesh2.admin.1", &t.fabric.id, vec![fp_addr]).await;
    let path: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*fp, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let fp_cp = Arc::new(ControlPlane::new(fp.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time()));
    let b0 = fp_cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    fp.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: b0.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    fp.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    until("the mesh-primary hears the birth Build settle", &*mp, &b0, |p| p.state == rafka_node_admin_core::build_state::BuildState::Complete && p.attempt == 1).await;
    let mp_accepted = Arc::new(AcceptedStore::new(Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new()), "mesh2.admin.1"));
    let mp_cp = Arc::new(ControlPlane::new(mp.clone(), mp_accepted, "mesh2.admin.1".parse().unwrap(), t, crate::adopted_time()));
    for cp in [&fp_cp, &mp_cp] {
        cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    }

    // The mesh-primary opens and claims the recovery on its own log.
    let first = match mp_cp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("mesh2's mesh-primary opens the recovery of its member: {other:?}"),
    };
    assert_eq!((first.build_id.clone(), first.attempt, first.mesh.as_deref()), (b0.clone(), 1, Some("mesh2")), "attempt 1 of mesh2's drift lane of the current Build");
    until("the fabric-primary hears the mesh-primary's claim of mesh2's lane", &*fp, &b0, |p| p.lanes.get("mesh2").is_some_and(|l| l.attempt == 1 && l.claimant.as_deref() == Some("mesh2.admin.1"))).await;

    // The mesh-primary runs the recovery: every receipt is appended to its own log and broadcast.
    for r in recovery_receipts(Some("mesh2"), &b0, first.attempt, &path, &node_id, &birth, "mesh2.admin.1") {
        let step = r.step.clone();
        mp.append_step_receipt(&r).await.unwrap_or_else(|e| panic!("the recovery's {step} receipt is not broadcast to the fabric-primary: {e}"));
    }
    mp.append_attempt_receipt(&BuildAttemptReceipt { mesh: Some("mesh2".into()), build_id: b0.clone(), attempt: first.attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    until("the fabric-primary hears the recovery attempt converge", &*fp, &b0, |p| p.lanes.get("mesh2").is_some_and(|l| l.state == rafka_node_admin_core::build_state::BuildState::Complete && l.attempt == 1)).await;
    assert_eq!(fp.recovery_of(&path, &node_id, &birth).await.unwrap(), Some((b0.clone(), Some("mesh2".into()), first.attempt, format!("retire-node:{path}"))), "the fabric-primary's recovery index holds the exited birth from the heard facts");

    // The repeat submit, at the fabric-primary.
    let before = opened(&fp.facts().await.unwrap());
    let again = fp_cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&again, Ok(Recovery::AlreadyApplied(o)) if *o == first), "a repeat once the mesh-primary recovered the birth names the attempt that recovered it: {again:?}");
    assert_eq!(opened(&fp.facts().await.unwrap()), before, "the fabric-primary opened no second recovery attempt");
    let decided = capture.spans("rdm.node_admin.build.update.via-claim-decision");
    assert_eq!(decided.iter().map(|(_, a)| (a["node"].as_str(), a["mesh"].as_str(), a["outcome"].as_str())).collect::<Vec<_>>(), vec![("mesh2.admin.1", "mesh2", "won")], "the one claim decision is the mesh-primary's on mesh2: none at the fabric-primary");
    assert_eq!(capture.spans("rdm.node_admin.build.reject.via-recovery-already-applied").len(), 1, "the duplicate is answered from the heard facts");
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.update.via-claim-decision", "rdm.node_admin.build.reject.via-recovery-already-applied"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
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
    builds.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: create.build_id.clone(), attempt: create.attempt, executor: "mesh1.admin.1".into() }).await.unwrap();

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
    builds.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: create.build_id.clone(), attempt: create.attempt, executor: "mesh2.admin.1".into() }).await.unwrap();
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

/// The recovery's three workflows as a fixture runs them: each operation writes the receipts its
/// workflow writes, through the executor's framed Build state (`deletion_receipts` for the
/// node.delete of the exited birth, `Complete` for the node.create and the node.update).
struct RecoveryReceipts {
    builds: Arc<dyn BuildStateAdapter>,
    executor: String,
}

#[async_trait::async_trait]
impl rafka_node_admin_core::executor::OperationRunner for RecoveryReceipts {
    async fn run(&self, build_id: &rafka_node_admin_core::build::BuildId, lane: Option<&str>, attempt: u32, op: &rafka_node_admin_core::build::BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        use rafka_node_admin_core::build::BuildOperation;
        let receipts = match op {
            BuildOperation::RetireNode { node, exited: Some(from) } => deletion_receipts(lane, "retire-node", build_id, attempt, node, &from.node_id, &from.incarnation, &self.executor),
            BuildOperation::CreateNode { .. } | BuildOperation::UpdateNode { .. } => {
                vec![BuildStepReceipt { mesh: lane.map(str::to_string), build_id: build_id.clone(), attempt, operation: op.key(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: Some(self.executor.clone()) }]
            }
            other => return Err(format!("a recovery runs no {}", other.key()).into()),
        };
        for r in receipts {
            self.builds.append_step_receipt(&r).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

/// `executor`'s own door for `build.attempt.run` over `builds`, its view `topology` and the
/// recovery's fixture runner.
fn run_door(executor: &str, builds: &Arc<MemoryBuildStateAdapter>, topology: Arc<tokio::sync::RwLock<Topology>>) -> rafka_node_admin_core::build_run::RunDoor {
    let runs = Arc::new(rafka_node_admin_core::build_run::AttemptRuns::default());
    let framed: Arc<dyn BuildStateAdapter> = Arc::new(rafka_node_admin_core::build_run::FramedBuilds::new(builds.clone(), runs.clone()));
    let runner = Arc::new(RecoveryReceipts { builds: framed.clone(), executor: executor.into() });
    let exec = Arc::new(rafka_node_admin_core::executor::BuildExecutor { executor: executor.into(), builds: framed.clone(), topology, runner });
    rafka_node_admin_core::build_run::RunDoor { me: executor.parse().unwrap(), builds: framed, exec, runs, local: builds.clone() }
}

/// CONTRACT (R-BI7 caveats 2 and 3, proposal Part C; node-recovery.md as R-BI7 moves it: "the
/// mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's rows,
/// then run; no call to the fabric-primary"): mesh2's mesh-primary recovers its member itself. Its
/// own door opens and claims the recovery as attempt 1 of mesh2's lane of the current Build, its own
/// executor runs it to `Complete` (node.delete of the exited birth, node.create, node.update), every
/// receipt in mesh2's lane, and the attempt's verdict releases mesh2's in-flight row. The node that
/// recovery created exits too: its recovery is admitted and runs as attempt 2 of mesh2's lane. The
/// fabric lane of the Build is untouched throughout. Must NOT happen: the run refused because the
/// executor's door read the fabric lane (`StaleClaim`), mesh2 still held after the verdict
/// (`BuildInProgress` naming the completed attempt), or a receipt of the recovery in the fabric lane.
#[tokio::test]
async fn a_mesh_primary_runs_its_members_recovery_to_complete_and_its_mesh_admits_the_next_one() {
    use rafka_node_admin_core::build_run::{run_recovery, Recovered};
    use rafka_node_admin_core::build_state::BuildState;
    use rafka_node_rpc_contract::build::BuildReply;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-r-bi7-run-in-mesh", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };
    let (_fp, mp, builds, path, node_id, birth) = fabric_of_two_meshes().await;
    let b0 = builds.current_build().await.unwrap().into_build_id().unwrap();
    let before = builds.read_build(&b0).await.unwrap();
    let door = run_door("mesh2.admin.1", &builds, mp.topology.clone());

    let (opened, terminal) = match run_recovery(&mp, &door, "node-recovery", path.clone(), node_id.clone(), birth.clone()).await {
        Ok(Recovered::Ran { opened, terminal }) => (opened, terminal),
        other => panic!("mesh2's mesh-primary runs its member's recovery itself: {other:?}"),
    };
    assert_eq!((opened.build_id.clone(), opened.mesh.as_deref(), opened.attempt), (b0.clone(), Some("mesh2"), 1), "attempt 1 of mesh2's lane of the current Build");
    assert_eq!(terminal, BuildReply::Complete { build_id: b0.to_string(), attempt: 1 }, "the recovery ran to Complete");
    let p = builds.read_build(&b0).await.unwrap();
    let lane = p.lanes.get("mesh2").expect("mesh2's lane");
    assert_eq!((lane.state, lane.attempt, lane.executor.as_deref(), lane.claimant.as_deref()), (BuildState::Complete, 1, Some("mesh2.admin.1"), Some("mesh2.admin.1")), "mesh2's lane: {lane:?}");
    let ran: Vec<&str> = lane.steps.iter().filter(|s| s.step == "Complete").map(|s| s.operation.as_str()).collect();
    assert_eq!(ran, vec!["retire-node:mesh2.rpc.1", "create-node:mesh2.rpc.1", "update-node:mesh2.rpc.1"], "node.delete, node.create, node.update, in mesh2's lane");
    assert_eq!((p.attempt, p.state, p.executor.clone(), p.steps.len()), (before.attempt, before.state, before.executor.clone(), before.steps.len()), "the fabric lane is untouched");
    assert_eq!(mp.claims.held("mesh2").await.unwrap(), None, "the attempt's verdict released mesh2's in-flight row");

    // The node the recovery created exits as well: mesh2 admits its recovery.
    let (node2, birth2) = (NodeId::mint(), IncarnationId::mint());
    match run_recovery(&mp, &door, "node-recovery", path.clone(), node2, birth2).await {
        Ok(Recovered::Ran { opened, terminal }) => {
            assert_eq!((opened.mesh.as_deref(), opened.attempt), (Some("mesh2"), 2), "attempt 2 of mesh2's lane");
            assert_eq!(terminal, BuildReply::Complete { build_id: b0.to_string(), attempt: 2 });
        }
        other => panic!("mesh2 admits the next recovery once the first one's verdict is in: {other:?}"),
    }
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.update.via-claim-decision", "rdm.node_admin.build.serve.via-attempt-run", "rdm.node_admin.build.update.via-reconcile"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
}

/// CONTRACT (build.md §1 as R-BI7 moves it: "one attempt in flight per mesh; a second
/// topology-changing request is still refused fabric-wide while a topology Build is in progress"):
/// mesh2's mesh-primary opens its member's recovery in mesh2's lane of the current Build, and its
/// node.create has put a new node at the path (live in the fabric-primary's view) while the attempt
/// is still open. The fabric-primary then accepts a node.create in mesh1, a new topology Build whose
/// plan touches no mesh an attempt holds; from then on it is the current Build (the LIFO read answers
/// the newest Build in progress, R-BI3), and a second node.create submitted while it is in progress is
/// refused build-in-progress naming it. Must NOT happen: the second change admitted as a third Build
/// because the current read still answers the recovery's Build, whose fabric lane is complete.
#[tokio::test]
async fn a_topology_change_accepted_while_a_recovery_runs_is_the_current_build_and_a_second_change_waits_for_it() {
    let (fp, mp, builds, path, node_id, birth) = fabric_of_two_meshes().await;
    let b0 = builds.current_build().await.unwrap().into_build_id().unwrap();
    let Ok(Recovery::Opened(recovery)) = mp.open_recovery("node-recovery", path.clone(), node_id, birth).await else { panic!("mesh2's mesh-primary opens its member's recovery") };
    assert_eq!((recovery.build_id.clone(), recovery.mesh.as_deref()), (b0.clone(), Some("mesh2")));
    // The recovery's node.create put a new node at the path; its node.update is still to run.
    fp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| {
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::ReadyForTraffic;
    });
    let create = fp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await.expect("a topology change that touches no held mesh is admitted while a recovery runs in another mesh");
    assert_ne!(create.build_id, b0, "a new topology Build");
    assert_eq!(builds.current().await.unwrap().map(|b| b.build_id), Some(create.build_id.clone()), "the newest Build in progress is the current one");
    match fp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await {
        Err(Refusal::BuildInProgress(held)) => assert_eq!(held.current_build_id, create.build_id, "refused naming the topology Build in progress: {held:?}"),
        other => panic!("a second topology change waits for the one in progress: {other:?}"),
    }
}

/// Two meshes: mesh1's node-admin holds the fabric-primary seat; mesh2 has two node-admins, and the
/// one that holds mesh2's primary seat is `mesh2_primary`; mesh2's other members are live but
/// `mesh2.rpc.1`, which is exited (Dead). Every node is in the accepted Build.
fn two_meshes_two_mesh2_admins(mesh2_primary: &str) -> Topology {
    let mut t = two_meshes();
    for n in t.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin) {
        n.is_primary = false;
    }
    t.nodes.push(node("mesh2.admin.2", false));
    for n in t.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin) {
        n.is_primary = n.name.to_string() == mesh2_primary;
    }
    t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.rpc.1").for_each(|n| n.status = NodeStatus::Dead);
    t
}

/// CONTRACT (R-BI7 caveat 2: "Each mesh-primary claims drift attempts for its own mesh ... under its
/// mesh on the fabric's shared store root, which is the writer ladder"; build.md :365 as the approved
/// proposal A4 words it: "The next attempt is claimed after the running attempt's terminal receipt
/// (Complete, Failed or HandedOff), or, when the executor is lost with the attempt unfinished, after
/// exact departure proof. A live run is reattached, never re-claimed"; mesh-recovery.md:8-9: a lost
/// mesh-primary is node-recovered by its mesh's live primary): mesh2's primary `mesh2.admin.1`
/// opens and claims the recovery of its exited member `mesh2.rpc.1` as attempt 1 of mesh2's lane and
/// leaves mid-run (it runs nothing more). `mesh2.admin.2` takes mesh2's seat over the same Build log
/// and the same in-flight rows. While the lost primary is only silent (no exit of its exact birth
/// recorded), every recovery the new primary opens is refused build-in-progress naming mesh2 and the
/// lost claimant: silence is never departure. Once its exit is proven and recorded (the new primary's
/// own proof, before it submits the lost primary's recovery), the new primary's recovery of the lost
/// primary ends attempt 1 Failed naming the departed claimant, releases mesh2's in-flight row, and
/// opens and claims attempt 2 of mesh2's lane for itself; `mesh2.rpc.1`'s exited birth is owed a
/// submit again from attempt 1's Failed verdict (node-recovery.md "Failure and receipts"). Must NOT
/// happen: the lane held for good by the departed claimant (every later recovery and topology
/// operation on mesh2 refused naming it), a take-over on silence alone, or the member's recovery lost.
#[tokio::test]
async fn a_recovery_whose_mesh_primary_departed_mid_run_is_ended_by_the_new_mesh_primary_on_its_exit_proof() {
    use rafka_node_admin_core::build_state::BuildState;
    use rafka_node_admin_core::status_storage::{MemoryStatusStorage, NodeExitRow, StatusStorage};
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-r-bi7-departed-claimant", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let claims = Arc::new(rafka_node_admin_core::mesh_claims::MeshClaims::in_memory());
    let rows: Arc<MemoryStatusStorage> = Arc::new(MemoryStatusStorage::default());
    let before = two_meshes_two_mesh2_admins("mesh2.admin.1");
    let member: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = before.node(&member).unwrap().clone();
    // The accepted Build names every node, the member included (it was live when accepted).
    let mut accepted_view = before.clone();
    accepted_view.nodes.iter_mut().for_each(|n| n.status = NodeStatus::ReadyForTraffic);
    let accepted = AcceptedStore::seeded(&*builds, before.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let b0 = builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;

    // mesh2's primary opens and claims its member's recovery, then leaves mid-run.
    let lost = Arc::new(ControlPlane::new(builds.clone(), accepted.clone(), "mesh2.admin.1".parse().unwrap(), before.clone(), crate::adopted_time()).with_claims(claims.clone()));
    let first = match lost.open_recovery("node-recovery", member.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("mesh2's primary opens its member's recovery: {other:?}"),
    };
    assert_eq!((first.mesh.as_deref(), first.attempt), (Some("mesh2"), 1), "attempt 1 of mesh2's lane");
    let departed: PathName = "mesh2.admin.1".parse().unwrap();
    let departed_birth = before.node(&departed).unwrap().clone();

    // mesh2.admin.2 holds mesh2's seat; in its view the lost primary is silent.
    let mut after = two_meshes_two_mesh2_admins("mesh2.admin.2");
    after.nodes.iter_mut().filter(|n| n.name == departed).for_each(|n| {
        n.node_id = departed_birth.node_id.clone();
        n.incarnation_id = departed_birth.incarnation_id.clone();
        n.status = NodeStatus::Dead;
    });
    after.nodes.iter_mut().filter(|n| n.name == member).for_each(|n| {
        n.node_id = exited.node_id.clone();
        n.incarnation_id = exited.incarnation_id.clone();
    });
    let heir = Arc::new(
        ControlPlane::new(builds.clone(), accepted, "mesh2.admin.2".parse().unwrap(), after, crate::adopted_time())
            .with_claims(claims.clone())
            .with_first_ready(rafka_node_admin_core::fabric_state::FirstReady::new(), rows.clone()),
    );
    let incarnation = departed_birth.incarnation_id.clone().unwrap();

    // Silence is never departure: no exit of the lost primary's exact birth is recorded.
    let silent = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while the lost primary is only silent, mesh2 stays held by its attempt: {other:?}"),
    };
    assert_eq!((silent["current_build_id"].as_str(), silent["mesh"].as_str(), silent["held_by"].as_str()), (Some(b0.as_str()), Some("mesh2"), Some("mesh2.admin.1")), "named by the Build, the mesh and the claimant: {silent}");

    // The new primary proves the lost primary's exit and records it, then submits its recovery.
    rows.put_node_exit(&NodeExitRow { node_id: departed_birth.node_id.clone(), name: departed.clone(), incarnation_id: incarnation.clone(), exit_code: None, proof: "provider".into(), observed_at_ms: 1 }).await.unwrap();
    let second = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation.clone()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("on the lost claimant's recorded exit, the new primary takes mesh2's lane over: {other:?}"),
    };
    assert_eq!((second.build_id.clone(), second.mesh.as_deref(), second.attempt), (b0.clone(), Some("mesh2"), 2), "attempt 2 of mesh2's lane");
    let p = builds.read_build(&b0).await.unwrap();
    let lane = p.lanes.get("mesh2").expect("mesh2's lane");
    assert_eq!((lane.state, lane.attempt, lane.claimant.as_deref(), lane.executor.as_deref()), (BuildState::Running, 2, Some("mesh2.admin.2"), Some("mesh2.admin.2")), "the new primary claimed attempt 2: {lane:?}");
    let ended = builds.facts().await.unwrap().into_iter().find_map(|f| match f {
        BuildFact::Attempt(a) if a.mesh.as_deref() == Some("mesh2") && a.attempt == 1 => Some(a.outcome),
        _ => None,
    });
    match ended {
        Some(AttemptOutcome::Failed { reason }) => assert!(reason.contains("mesh2.admin.1") && reason.contains(incarnation.as_str()), "attempt 1's verdict names the departed claimant's exact birth: {reason}"),
        other => panic!("attempt 1 of mesh2's lane ends Failed on its claimant's departure: {other:?}"),
    }
    let held = claims.held("mesh2").await.unwrap().expect("mesh2's in-flight row");
    assert_eq!((held.attempt, held.lane.as_deref(), held.claimant.as_str()), (2, Some("mesh2"), "mesh2.admin.2"), "the row names attempt 2 and its claimant");
    let owed = builds.exits_owed(&b0, "mesh2").await.unwrap();
    assert!(owed.iter().any(|o| o.path == member && o.node_id == exited.node_id && o.attempt == 1), "the member's exited birth is owed a submit from attempt 1's verdict: {owed:?}");
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-build-in-progress", "rdm.node_admin.build.update.via-departed-claimant", "rdm.node_admin.build.update.via-claim-decision"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
    let taken = capture.spans("rdm.node_admin.build.update.via-departed-claimant");
    assert_eq!(taken.len(), 1, "the take-over is spanned once: {taken:?}");
    let (_, t) = &taken[0];
    assert_eq!((t["lane"].as_str(), t["attempt"].as_str(), t["claimant"].as_str(), t["node"].as_str()), ("mesh2", "1", "mesh2.admin.1", "mesh2.admin.2"), "{t:?}");
}

/// CONTRACT (R-BI7 caveat 3: one attempt in flight per mesh, decided by the mesh's in-flight row on
/// the store root; build.md :365 as the approved proposal A4 words it: the next attempt is claimed
/// after the running attempt's terminal receipt, or, with its executor lost, after exact departure
/// proof): mesh2's primary `mesh2.admin.1` runs its member's recovery, attempt 1 of mesh2's lane, to
/// its Converged verdict and is lost before it releases mesh2's in-flight row. `mesh2.admin.2`
/// takes the seat: in its fold the lane holds nothing, but the row still names attempt 1 by the lost
/// primary. While the lost primary is only silent its recovery is refused build-in-progress naming
/// it; once its exit is recorded, the new primary's recovery of it releases the row the departed
/// claimant left and opens and claims attempt 2 of mesh2's lane. Must NOT happen: mesh2 held for good
/// by a row whose attempt ended, or the row released on silence alone.
#[tokio::test]
async fn an_in_flight_row_a_departed_mesh_primary_left_after_its_verdict_is_released_by_the_new_mesh_primary_on_its_exit_proof() {
    use rafka_node_admin_core::status_storage::{MemoryStatusStorage, NodeExitRow, StatusStorage};
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let claims = Arc::new(rafka_node_admin_core::mesh_claims::MeshClaims::in_memory());
    let rows: Arc<MemoryStatusStorage> = Arc::new(MemoryStatusStorage::default());
    let before = two_meshes_two_mesh2_admins("mesh2.admin.1");
    let member: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = before.node(&member).unwrap().clone();
    let mut accepted_view = before.clone();
    accepted_view.nodes.iter_mut().for_each(|n| n.status = NodeStatus::ReadyForTraffic);
    let accepted = AcceptedStore::seeded(&*builds, before.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let b0 = builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;

    // mesh2's primary runs its member's recovery to its verdict and is lost before the release.
    let lost = Arc::new(ControlPlane::new(builds.clone(), accepted.clone(), "mesh2.admin.1".parse().unwrap(), before.clone(), crate::adopted_time()).with_claims(claims.clone()));
    let Ok(Recovery::Opened(first)) = lost.open_recovery("node-recovery", member.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await else { panic!("mesh2's primary opens its member's recovery") };
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: Some("mesh2".into()), build_id: b0.clone(), attempt: first.attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    assert_eq!(claims.held("mesh2").await.unwrap().map(|r| (r.attempt, r.claimant)), Some((1, "mesh2.admin.1".to_string())), "the row the lost primary never released");

    let departed: PathName = "mesh2.admin.1".parse().unwrap();
    let departed_birth = before.node(&departed).unwrap().clone();
    let mut after = two_meshes_two_mesh2_admins("mesh2.admin.2");
    after.nodes.iter_mut().filter(|n| n.name == departed).for_each(|n| {
        n.node_id = departed_birth.node_id.clone();
        n.incarnation_id = departed_birth.incarnation_id.clone();
        n.status = NodeStatus::Dead;
    });
    let heir = Arc::new(
        ControlPlane::new(builds.clone(), accepted, "mesh2.admin.2".parse().unwrap(), after, crate::adopted_time())
            .with_claims(claims.clone())
            .with_first_ready(rafka_node_admin_core::fabric_state::FirstReady::new(), rows.clone()),
    );
    let incarnation = departed_birth.incarnation_id.clone().unwrap();
    let silent = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while the lost primary is only silent, its row holds mesh2: {other:?}"),
    };
    assert_eq!((silent["mesh"].as_str(), silent["held_by"].as_str()), (Some("mesh2"), Some("mesh2.admin.1")), "{silent}");

    rows.put_node_exit(&NodeExitRow { node_id: departed_birth.node_id.clone(), name: departed.clone(), incarnation_id: incarnation.clone(), exit_code: Some(137), proof: "provider".into(), observed_at_ms: 1 }).await.unwrap();
    let second = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("on the lost claimant's recorded exit, the row it left is released and mesh2 admits the next attempt: {other:?}"),
    };
    assert_eq!((second.mesh.as_deref(), second.attempt), (Some("mesh2"), 2), "attempt 2 of mesh2's lane");
    let held = claims.held("mesh2").await.unwrap().expect("mesh2's in-flight row");
    assert_eq!((held.attempt, held.claimant.as_str()), (2, "mesh2.admin.2"), "the row names attempt 2 and its claimant");
}
