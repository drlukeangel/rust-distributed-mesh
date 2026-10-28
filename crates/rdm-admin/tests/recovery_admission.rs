//! A node recovery's admission at its mesh-primary's own door (node-recovery.md "Execution"; R-BI7):
//! the member's mesh-primary opens one recovery keyed on the exited birth `(node.id, incarnation)`
//! as the next attempt of the current Build in its mesh's drift lane and claims it on its mesh's
//! rows; one attempt is in flight per mesh, and a repeat submit answers `AlreadyApplied` or
//! `BuildInProgress`, never a second recovery.

use rdm_admin::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build_state::{AttemptOutcome, AttemptReason, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::control_plane::{ControlPlane, Recovery, Refusal};
use rdm_admin::model::*;
use rdm_admin::storage::MeshStorage as _;
use rdm_admin::topology::Topology;
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
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    pipeline: rdm_transport::SpanCapture,
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
async fn settle_other(builds: &MemoryBuildStateAdapter, id: &rdm_admin::build::BuildId) {
    let attempt = builds.read_build(id).await.unwrap().attempt;
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
}

fn opened(facts: &[BuildFact]) -> usize {
    facts.iter().filter(|f| matches!(f, BuildFact::Opened(_))).count()
}

/// The drift lane's departure proof a cell flips (build.md: "after exact departure proof"): the
/// claimant's exact runtime reads exited once it is set.
#[derive(Default)]
struct Departure(std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl rdm_admin::build_drive::DepartureProof for Departure {
    async fn proven(&self, executor: &PathName) -> Result<(), String> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Ok(())
        } else {
            Err(format!("{executor}'s exact runtime does not read exited"))
        }
    }
}

/// CONTRACT (#3051 acceptance 4, node-recovery.md "Numbered end-to-end sequence" step 1: "A repeat
/// submit answers AlreadyApplied or BuildInProgress, never a second recovery"; R-BI7): mesh1's
/// mesh-primary opens the first recovery of `mesh1.rpc.2`'s exited birth as attempt 1 of mesh1's
/// drift lane of the current Build and claims it. A repeat while that attempt is open answers
/// `BuildInProgress` naming the Build, mesh1 and the mesh-primary holding it; a repeat once its two
/// operations (node.delete of the exited birth, then node.create) completed answers
/// `AlreadyApplied` naming the attempt that recovered it; a recovery submitted while another Build's
/// attempt runs in mesh1 answers that Build's `BuildInProgress`. A node-admin whose mesh holds no other live node-admin is mesh recovery, never
/// node recovery: refused by name. A repeat after a later
/// topology Build was accepted is still `AlreadyApplied`: the recovery is keyed on the exited birth
/// across the Build log, not on the accepted Build's receipts. Must NOT happen: a second attempt
/// opened for a birth already recovered.
#[tokio::test]
async fn a_duplicate_recovery_submit_for_the_same_exited_birth_opens_one_recovery() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-3051-cell4", exporter.clone());
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

    // The mesh-primary completes the recovery of the exited birth: its delete, then its create.
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
    let other = cp.submit("build.create", crate::named_request(&cp, TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await).await.expect("a topology change is accepted");
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
/// that workflow"): a birth a node.delete (`retire-node`) of a Build deleted (its `NodeDeleted`
/// receipt names the exact birth, and the operation completed) exited inside that workflow. A
/// recovery submitted for it afterwards (the mesh-primary's tickle proved it TRUE OFFLINE and the
/// provider proved it exited: the delete terminated it) answers `AlreadyApplied`, naming the delete's
/// attempt, from the Build log keyed on the exited birth, and opens nothing.
/// Must NOT happen: a recovery attempt opened
/// for a birth a workflow already deleted, which deletes it again and creates over the data dir
/// that workflow released (gate 71b06a15, mesh_identity__canonical_ids: `recover-node:mesh2.rpc.2:
/// NodeDeleting failed: the inherited data dir .../mesh2.rpc.2-ekne99d5g23b does not exist`).
#[tokio::test]
async fn a_recovery_submit_for_a_birth_a_delete_deleted_is_answered_already_applied_by_the_delete() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-3016-deleted-birth", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view();
    let path: PathName = "mesh1.rpc.2".parse().unwrap();
    let deleted = t.node(&path).unwrap().clone();
    let (node_id, birth) = (deleted.node_id.clone(), deleted.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    // `mesh1.rpc.2` is deleted: a Build of the topology without it terminates the birth and deletes
    // it.
    let delete = cp.submit("build.create", TopologyChange::RemoveNode { node: path.clone() }).await.expect("the delete is admitted");
    let b1 = delete.build_id.clone();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b1.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    for r in deletion_receipts(None, "node.delete", &b1, 1, &path, &node_id, &birth, "mesh1.admin.1") {
        builds.append_step_receipt(&r).await.unwrap();
    }
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b1.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    // The mesh-primary's tick read the birth silent before the delete removed it: its tickle ends
    // TRUE OFFLINE and the provider proves the exit (the delete terminated it).
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    let before = opened(&builds.facts().await.unwrap());

    let submitted = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(
        matches!(&submitted, Ok(Recovery::AlreadyApplied(o)) if *o == delete),
        "the recovery of a birth the delete deleted names the delete's attempt and opens nothing: {submitted:?}"
    );
    assert_eq!(opened(&builds.facts().await.unwrap()), before, "no recovery attempt is opened for a birth a workflow deleted");

    let applied = capture.spans("rdm.node_admin.build.reject.via-recovery-already-applied");
    assert_eq!(
        applied.iter().map(|(_, a)| a.get("operation").cloned().unwrap_or_default()).collect::<Vec<_>>(),
        vec![format!("node.delete:{path}")],
        "the answer names the workflow operation the birth's exit belongs to"
    );
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-recovery-already-applied"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
}

/// A node-admin of the fabric whose store root is `root`: its own endpoint, and its Build journal on
/// that root. No admin is connected to another.
async fn store_admin(name: &str, fabric: &FabricId, root: &std::path::Path) -> (iroh::Endpoint, Arc<rdm_admin::fabric_builds::FabricBuildStateAdapter>) {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(iroh::SecretKey::generate())
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    // Its Build journal as its file backend opens it, on the fabric's store root.
    let mesh = name.split('.').next().unwrap_or_default().to_string();
    let scope = rdm_admin::storage_backend::StorageScope { data_dir: root.join(format!("data-{name}")), fabric_id: fabric.clone(), own_mesh: mesh, writer: IncarnationId::mint(), node_id: NodeId::mint() };
    let journal = {
        use rdm_admin::storage_backend::StorageBackend as _;
        rdm_admin::storage_backend::FileBackend::at(root).open(&scope).await.unwrap().builds
    };
    let builds = rdm_admin::fabric_builds::FabricBuildStateAdapter::new(journal, &rdm_transport::topology::LocationView::new(&endpoint, name).unwrap(), name.into(), rdm_admin::fabric_builds::SeatSource::unrecorded());
    (endpoint, Arc::new(builds))
}

/// Wait until `cond` holds of `builds`'s projection of `id`, naming what was awaited.
async fn until(what: &str, builds: &dyn BuildStateAdapter, id: &rdm_admin::build::BuildId, cond: impl Fn(&rdm_admin::build_state::BuildProjection) -> bool) {
    for _ in 0..400 {
        if builds.read_build(id).await.is_ok_and(|p| cond(&p)) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("{what}: never held; last projection {:?}", builds.read_build(id).await);
}

/// The step receipts a recovery attempt in `lane` writes for the exited birth (node-recovery.md "Steps"), as
/// its two workflows write them: the delete's (`node.delete:<path>`: `NodeDeleting` and
/// `NodeDeleted` carry the lifecycle operation naming the exited birth, `Complete` commits no
/// output), then the create's `Complete` (`node.create:<path>`), the recovery's last; the create's
/// steps before its `Complete` are elided (they name the new node, not the exited one).
fn recovery_receipts(lane: Option<&str>, build: &rdm_admin::build::BuildId, attempt: u32, path: &PathName, node_id: &NodeId, birth: &IncarnationId, executor: &str) -> Vec<BuildStepReceipt> {
    let mut receipts = deletion_receipts(lane, "node.delete", build, attempt, path, node_id, birth, executor);
    receipts.push(BuildStepReceipt { mesh: lane.map(str::to_string), build_id: build.clone(), attempt, operation: format!("node.create:{path}"), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: Some(executor.into()) });
    receipts
}

/// The step receipts of node workflow `kind` (`retire-node`) at
/// `path` that deleted the exact birth: `NodeDeleting` and `NodeDeleted` carry the node.delete's
/// lifecycle operation naming that birth, as every deleting pipeline writes them; `Complete` ends
/// the operation. The steps between them name the successor, never the deleted birth: elided. `lane`
/// is the attempt's lane (`None` the fabric lane, `Some(mesh)` that mesh's drift lane).
#[allow(clippy::too_many_arguments)]
fn deletion_receipts(lane: Option<&str>, kind: &str, build: &rdm_admin::build::BuildId, attempt: u32, path: &PathName, node_id: &NodeId, birth: &IncarnationId, executor: &str) -> Vec<BuildStepReceipt> {
    let operation = format!("{kind}:{path}");
    let op = rdm_core::LifecycleOp { build_id: build.to_string(), attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: birth.clone(), name: path.clone(), event_at_rafka_ms: 1 };
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
/// the attempt (its open, its claim, its receipts) is written on the fabric's store root, where the
/// fabric-primary reads it (Q185: no Build channel), whose fold holds mesh2's lane claimed by the mesh-primary, and whose door
/// answers a repeat submit for that exited birth `AlreadyApplied`, naming the attempt, from the
/// facts it heard. Must NOT happen: a claim decided at the fabric-primary, a fact of the recovery
/// the fabric-primary never hears, or a second recovery attempt opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_a_mesh_primary_opens_and_claims_on_its_own_log_is_heard_by_the_fabric_primary_and_answered_already_applied_there() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-heard-at-fabric-primary", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let t = two_meshes();
    let root = std::env::temp_dir().join(format!("recovery-admission-{}", NodeId::mint()));
    std::fs::create_dir_all(&root).unwrap();
    let (_fp_ep, fp) = store_admin("mesh1.admin.1", &t.fabric.id, &root).await;
    let (_mp_ep, mp) = store_admin("mesh2.admin.1", &t.fabric.id, &root).await;
    let path: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*fp, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let fp_cp = Arc::new(ControlPlane::new(fp.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time()));
    let b0 = fp_cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    fp.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: b0.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    fp.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    until("the mesh-primary hears the birth Build settle", &*mp, &b0, |p| p.state == rdm_admin::build_state::BuildState::Complete && p.attempt == 1).await;
    let mp_accepted = Arc::new(AcceptedStore::new(Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()), "mesh2.admin.1"));
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

    // The mesh-primary runs the recovery: every receipt is appended to the Build journal on the store root.
    for r in recovery_receipts(Some("mesh2"), &b0, first.attempt, &path, &node_id, &birth, "mesh2.admin.1") {
        let step = r.step.clone();
        mp.append_step_receipt(&r).await.unwrap_or_else(|e| panic!("the recovery's {step} receipt is not written for the fabric-primary: {e}"));
    }
    mp.append_attempt_receipt(&BuildAttemptReceipt { mesh: Some("mesh2".into()), build_id: b0.clone(), attempt: first.attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    until("the fabric-primary hears the recovery attempt converge", &*fp, &b0, |p| p.lanes.get("mesh2").is_some_and(|l| l.state == rdm_admin::build_state::BuildState::Complete && l.attempt == 1)).await;
    assert_eq!(fp.recovery_of(&path, &node_id, &birth).await.unwrap(), Some((b0.clone(), Some("mesh2".into()), first.attempt, format!("node.delete:{path}"))), "the fabric-primary's recovery index holds the exited birth from the heard facts");

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
    let _ = std::fs::remove_dir_all(&root);
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
/// holds once it read the other's facts from the store root), the birth Build settled, and
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

/// A build-in-progress refusal's fields, for the assertions: the Build, the mesh and the holder it names.
async fn rendered(refusal: Refusal) -> serde_json::Value {
    match refusal {
        Refusal::BuildInProgress(p) => serde_json::json!({ "error": "build-in-progress", "current_build_id": p.current_build_id, "mesh": p.mesh, "held_by": p.held_by }),
        other => panic!("not a build-in-progress refusal: {other:?}"),
    }
}

/// CONTRACT (R-BI7, proposal Part C caveats 2 and 3; node-recovery.md as R-BI7 moves it: "the
/// mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's rows,
/// then run; no call to the fabric-primary"): a node.create runs in mesh1 (a topology Build, its
/// attempt claimed for mesh1's admin and running) when mesh2's mesh-primary proves its member
/// `mesh2.rpc.1` exited. The mesh-primary's own door opens the recovery as an attempt of the current
/// Build and claims it on mesh2's rows (`(build_id, mesh2, attempt)`, decided by mesh2's
/// mesh-primary, won); the node.create's attempt in mesh1 is untouched and still running. Must NOT
/// happen: the recovery refused because a Build runs in another mesh (`BuildInProgress`), or
/// refused because the submitter is not the fabric-primary (`NotPrimary`).
#[tokio::test]
async fn a_recovery_in_mesh2_is_opened_by_its_mesh_primary_while_a_node_create_runs_in_mesh1() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-two-meshes", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };
    let (fp, mp, builds, path, node_id, birth) = fabric_of_two_meshes().await;

    // node.create in mesh1: a topology Build, its first attempt claimed for mesh1's admin.
    let create = fp.submit("build.create", crate::named_request(&fp, TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await).await.expect("the node.create is accepted");
    builds.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: create.build_id.clone(), attempt: create.attempt, executor: "mesh1.admin.1".into() }).await.unwrap();

    // mesh2's mesh-primary opens its member's recovery at its own door.
    let recovery = mp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    let opened = match recovery {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("mesh2's mesh-primary opens the recovery of its member while a node.create runs in mesh1: {other:?}"),
    };
    assert_eq!(opened.build_id, create.build_id, "the recovery is an attempt of the current Build (no shape change, no new Build)");
    let p = builds.read_build(&create.build_id).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref(), p.state), (create.attempt, Some("mesh1.admin.1"), rdm_admin::build_state::BuildState::Running), "the node.create's attempt in mesh1 still runs: {p:?}");

    let claims = capture.spans("rdm.node_admin.build.update.via-claim-decision");
    let mine: Vec<_> = claims.iter().filter(|(_, a)| a.get("node").map(String::as_str) == Some("mesh2.admin.1")).collect();
    assert_eq!(mine.len(), 1, "mesh2's mesh-primary decides its recovery's claim itself: {claims:?}");
    let (_, claim) = mine[0];
    assert_eq!((claim["mesh"].as_str(), claim["outcome"].as_str(), claim["build_id"].as_str()), ("mesh2", "won", create.build_id.log().to_string().as_str()), "claimed on mesh2's rows: {claim:?}");
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
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-one-mesh", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };
    let (fp, mp, builds, path, node_id, birth) = fabric_of_two_meshes().await;

    // The fabric-primary's topology operation holds mesh2.
    let create = fp.submit("build.create", crate::named_request(&fp, TopologyChange::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::RpcNode }).await).await.expect("the node.create is accepted");
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
    let second = match fp.submit("build.create", crate::named_request(&fp, TopologyChange::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::RpcNode }).await).await {
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

/// The recovery's two workflows as a fixture runs them: each operation writes the receipts its
/// workflow writes, through the executor's framed Build state (`deletion_receipts` for the
/// node.delete of the exited birth, `Complete` for the node.create).
struct RecoveryReceipts {
    builds: Arc<dyn BuildStateAdapter>,
    executor: String,
}

#[async_trait::async_trait]
impl rdm_admin::executor::OperationRunner for RecoveryReceipts {
    async fn run(&self, build_id: &rdm_admin::build::BuildId, lane: Option<&str>, attempt: u32, op: &rdm_admin::build::BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        use rdm_admin::build::BuildOperation;
        let receipts = match op {
            BuildOperation::RetireNode { node, exited: Some(from), .. } => deletion_receipts(lane, "node.delete", build_id, attempt, node, &from.node_id, &from.incarnation, &self.executor),
            BuildOperation::CreateNode { .. } => {
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
fn run_door(executor: &str, builds: &Arc<MemoryBuildStateAdapter>, topology: Arc<tokio::sync::RwLock<Topology>>) -> rdm_admin::build_run::RunDoor {
    let runs = Arc::new(rdm_admin::build_run::AttemptRuns::default());
    let framed: Arc<dyn BuildStateAdapter> = Arc::new(rdm_admin::build_run::FramedBuilds::new(builds.clone(), runs.clone()));
    let runner = Arc::new(RecoveryReceipts { builds: framed.clone(), executor: executor.into() });
    let exec = Arc::new(rdm_admin::executor::BuildExecutor { executor: executor.into(), builds: framed.clone(), topology, runner });
    rdm_admin::build_run::RunDoor { me: executor.parse().unwrap(), builds: framed, exec, runs, local: builds.clone(), gate: rdm_admin::stop_gate::StopGate::new() }
}

/// CONTRACT (R-BI7 caveats 2 and 3, proposal Part C; node-recovery.md as R-BI7 moves it: "the
/// mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's rows,
/// then run; no call to the fabric-primary"): mesh2's mesh-primary recovers its member itself. Its
/// own door opens and claims the recovery as attempt 1 of mesh2's lane of the current Build, its own
/// executor runs it to `Complete` (node.delete of the exited birth, then node.create), every
/// receipt in mesh2's lane, and the attempt's verdict releases mesh2's in-flight row. The node that
/// recovery created exits too: its recovery is admitted and runs as attempt 2 of mesh2's lane. The
/// fabric lane of the Build is untouched throughout. Must NOT happen: the run refused because the
/// executor's door read the fabric lane (`StaleClaim`), mesh2 still held after the verdict
/// (`BuildInProgress` naming the completed attempt), or a receipt of the recovery in the fabric lane.
#[tokio::test]
async fn a_mesh_primary_runs_its_members_recovery_to_complete_and_its_mesh_admits_the_next_one() {
    use rdm_admin::build_run::{run_recovery, Recovered};
    use rdm_admin::build_state::BuildState;
    use rdm_core::build::BuildReply;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-run-in-mesh", exporter.clone());
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
    assert_eq!(ran, vec!["node.delete:mesh2.rpc.1", "node.create:mesh2.rpc.1"], "node.delete, then node.create, in mesh2's lane, and nothing after");
    assert_eq!((p.attempt, p.state, p.executor.clone(), p.steps.len()), (before.attempt, before.state, before.executor.clone(), before.steps.len()), "the fabric lane is untouched");
    assert_eq!(mp.claims.in_flight("mesh2").await.unwrap(), None, "the attempt's verdict released mesh2's in-flight row");

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
    // The recovery's node.create put a new node at the path; its Complete is not receipted yet, so
    // the recovery is still open.
    fp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| {
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::ReadyForTraffic;
    });
    let create = fp.submit("build.create", crate::named_request(&fp, TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await).await.expect("a topology change that touches no held mesh is admitted while a recovery runs in another mesh");
    assert_ne!(create.build_id, b0, "a new topology Build");
    assert_eq!(builds.current().await.unwrap().map(|b| b.build_id), Some(create.build_id.clone()), "the newest Build in progress is the current one");
    match fp.submit("build.create", crate::named_request(&fp, TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await).await {
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
async fn a_recovery_whose_mesh_primary_is_lost_mid_run_is_ended_by_the_new_mesh_primary_once_its_exit_is_recorded() {
    use rdm_admin::build_state::BuildState;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-departed-claimant", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let claims = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
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
            .with_claims(claims.clone()),
    );
    let departure = Arc::new(Departure::default());
    let _ = heir.departure.set(departure.clone());
    let incarnation = departed_birth.incarnation_id.clone().unwrap();

    // Silence is never departure: the lost primary's departure is not proven.
    let silent = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while the lost primary is only silent, mesh2 stays held by its attempt: {other:?}"),
    };
    assert_eq!((silent["current_build_id"].as_str(), silent["mesh"].as_str(), silent["held_by"].as_str()), (Some(b0.as_str()), Some("mesh2"), Some("mesh2.admin.1")), "named by the Build, the mesh and the claimant: {silent}");

    // The lost primary's departure is proven (its exact runtime reads exited), then the new primary
    // submits its recovery.
    departure.0.store(true, std::sync::atomic::Ordering::SeqCst);
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
        Some(AttemptOutcome::Failed { reason , .. }) => assert!(reason.contains("mesh2.admin.1") && reason.contains(incarnation.as_str()), "attempt 1's verdict names the departed claimant's exact birth: {reason}"),
        other => panic!("attempt 1 of mesh2's lane ends Failed on its claimant's departure: {other:?}"),
    }
    let held = claims.in_flight("mesh2").await.unwrap().expect("mesh2's in-flight row");
    assert_eq!((held.attempt, held.lane.as_deref(), held.claimant.as_str()), (2, Some("mesh2"), "mesh2.admin.2"), "the row names attempt 2 and its claimant");
    let owed = builds.exits_owed(&b0, "mesh2").await.unwrap();
    assert!(owed.iter().any(|o| o.path == member && o.node_id == exited.node_id && o.attempt == 1), "the member's exited birth is owed a submit from attempt 1's verdict: {owed:?}");
    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-build-in-progress", "rdm.node_admin.build.update.via-claimant-departure", "rdm.node_admin.build.update.via-claim-decision"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
    let taken = capture.spans("rdm.node_admin.build.update.via-claimant-departure");
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
async fn an_in_flight_row_a_lost_mesh_primary_left_after_its_verdict_is_released_by_the_new_mesh_primary_once_its_exit_is_recorded() {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let claims = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
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
    assert_eq!(claims.in_flight("mesh2").await.unwrap().map(|r| (r.attempt, r.claimant)), Some((1, "mesh2.admin.1".to_string())), "the row the lost primary never released");

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
            .with_claims(claims.clone()),
    );
    let departure = Arc::new(Departure::default());
    let _ = heir.departure.set(departure.clone());
    let incarnation = departed_birth.incarnation_id.clone().unwrap();
    let silent = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while the lost primary is only silent, its row holds mesh2: {other:?}"),
    };
    assert_eq!((silent["mesh"].as_str(), silent["held_by"].as_str()), (Some("mesh2"), Some("mesh2.admin.1")), "{silent}");

    departure.0.store(true, std::sync::atomic::Ordering::SeqCst);
    let second = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), incarnation).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("on the lost claimant's recorded exit, the row it left is released and mesh2 admits the next attempt: {other:?}"),
    };
    assert_eq!((second.mesh.as_deref(), second.attempt), (Some("mesh2"), 2), "attempt 2 of mesh2's lane");
    let held = claims.in_flight("mesh2").await.unwrap().expect("mesh2's in-flight row");
    assert_eq!((held.attempt, held.claimant.as_str()), (2, "mesh2.admin.2"), "the row names attempt 2 and its claimant");
}

/// CONTRACT (build.md :365 as the approved proposal A4 words it: "The next attempt is claimed after
/// the running attempt's terminal receipt (Complete, Failed or HandedOff), or, when the executor is
/// lost with the attempt unfinished, after exact departure proof ... Claims are insert-and-fail on
/// `(build_id, attempt)`, which prevents two claims of one attempt"; R-BI7 caveat 2: each mesh-primary
/// claims its mesh's drift attempts on its mesh's rows; caveat 3: one attempt in flight per mesh):
/// mesh2's primary `mesh2.admin.1` takes mesh2's in-flight row for attempt 1 of mesh2's lane and opens
/// it with the recovery of its exited member `mesh2.rpc.1`, and is lost before it claims it. No one
/// claimed attempt 1, and no executor ran it. `mesh2.admin.2` takes mesh2's seat over the same Build
/// log and rows. Its recovery of another birth (the lost primary's own) is refused build-in-progress
/// naming mesh2 and the opener the row names: the open attempt holds the mesh. Its recovery of the
/// member's exited birth is a second claimant of attempt 1: the row the opener took is its claim on the
/// store (build.md §5.2, Q142), held while the opener's departure is unproven; once proven, the row is
/// released and the member's recovery claims the open attempt 1, which runs under `mesh2.admin.2`
/// with the action the opener opened.
/// Must NOT happen: mesh2 held for good by an attempt no one claimed, a verdict written for an
/// attempt no one ran, or a second attempt opened for the birth an open attempt already carries.
#[tokio::test]
async fn a_recovery_attempt_a_lost_mesh_primary_opened_and_never_claimed_is_claimed_by_the_new_mesh_primary() {
    use rdm_admin::accepted::AttemptAction;
    use rdm_admin::build_state::{AttemptOpened, BuildState};
    use rdm_admin::mesh_claims::InFlight;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-unclaimed-open", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let claims = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
    let before = two_meshes_two_mesh2_admins("mesh2.admin.1");
    let member: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = before.node(&member).unwrap().clone();
    let mut accepted_view = before.clone();
    accepted_view.nodes.iter_mut().for_each(|n| n.status = NodeStatus::ReadyForTraffic);
    let accepted = AcceptedStore::seeded(&*builds, before.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let b0 = builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;

    // What mesh2's primary wrote before it was lost: mesh2's in-flight row for attempt 1, then the
    // attempt's open carrying the member's recovery. Its claim was never written.
    let action = AttemptAction::Recover { path: member.clone(), node_id: exited.node_id.clone(), from_incarnation: exited.incarnation_id.clone().unwrap(), backbone: false };
    let row = InFlight { build_id: b0.clone(), lane: Some("mesh2".into()), attempt: 1, claimant: "mesh2.admin.1".into(), claimant_node: None };
    assert_eq!(claims.take_in_flight("mesh2", &row).await.unwrap(), rdm_admin::mesh_claims::Taken::Won);
    builds
        .open_attempt(&AttemptOpened { build_id: b0.clone(), attempt: 1, reason: AttemptReason::Recovery, action: Some(action.clone()), opened_by: "mesh2.admin.1".into(), opened_at_ms: 1, mesh: Some("mesh2".into()), context: None })
        .await
        .unwrap();
    let lane = builds.read_build(&b0).await.unwrap().lanes.get("mesh2").cloned().expect("mesh2's lane");
    assert_eq!((lane.state, lane.attempt, lane.opened, lane.claimant.as_deref()), (BuildState::Pending, 0, 1, None), "attempt 1 is open and unclaimed: {lane:?}");

    let departed: PathName = "mesh2.admin.1".parse().unwrap();
    let departed_birth = before.node(&departed).unwrap().clone();
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
            .with_claims(claims.clone()),
    );
    let departure = Arc::new(Departure::default());
    let _ = heir.departure.set(departure.clone());
    let departed_incarnation = departed_birth.incarnation_id.clone().unwrap();

    // Another birth's recovery waits for the open attempt: one attempt in flight per mesh.
    let other_before = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), departed_incarnation.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while attempt 1 is open for the member, mesh2 refuses another birth's recovery: {other:?}"),
    };
    assert_eq!((other_before["current_build_id"].as_str(), other_before["mesh"].as_str()), (Some(b0.as_str()), Some("mesh2")), "{other_before}");

    // The row the lost opener took is its claim on the store (build.md §5.2, Q142): the member's recovery
    // is a second claimant of attempt 1 and is held while that claimant's departure is unproven.
    match heir.open_recovery("node-recovery", member.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await {
        Err(Refusal::BuildInProgress(..)) => {}
        other => panic!("while the lost opener's claim of attempt 1 stands, the member's recovery is held: {other:?}"),
    }
    // Its departure proven, the departed claimant's row is released and the member's recovery claims the
    // open attempt: it is not reopened.
    departure.0.store(true, std::sync::atomic::Ordering::SeqCst);
    let taken = match heir.open_recovery("node-recovery", member.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("the new mesh-primary claims the attempt its departed opener left: {other:?}"),
    };
    assert_eq!((taken.build_id.clone(), taken.mesh.as_deref(), taken.attempt), (b0.clone(), Some("mesh2"), 1), "attempt 1 of mesh2's lane, the one open");
    let p = builds.read_build(&b0).await.unwrap();
    let lane = p.lanes.get("mesh2").expect("mesh2's lane");
    assert_eq!((lane.state, lane.attempt, lane.claimant.as_deref(), lane.executor.as_deref()), (BuildState::Running, 1, Some("mesh2.admin.2"), Some("mesh2.admin.2")), "the new primary claimed attempt 1: {lane:?}");
    assert_eq!(lane.attempt_actions.get(&1), Some(&action), "attempt 1 runs the action its opener opened");
    let facts = builds.facts().await.unwrap();
    let opens: Vec<u32> = facts.iter().filter_map(|f| match f {
        BuildFact::Opened(o) if o.mesh.as_deref() == Some("mesh2") => Some(o.attempt),
        _ => None,
    }).collect();
    assert_eq!(opens, vec![1], "one open in mesh2's lane: the attempt is claimed, never reopened");
    assert!(!facts.iter().any(|f| matches!(f, BuildFact::Attempt(a) if a.mesh.as_deref() == Some("mesh2"))), "no verdict for an attempt no one ran: {facts:?}");
    assert!(builds.exits_owed(&b0, "mesh2").await.unwrap().is_empty(), "nothing is owed: the member's recovery is the running attempt");
    let held = claims.in_flight("mesh2").await.unwrap().expect("mesh2's in-flight row");
    assert_eq!((held.build_id.clone(), held.lane.as_deref(), held.attempt), (b0.clone(), Some("mesh2"), 1), "the row names attempt 1");

    // Once claimed, the attempt holds mesh2 under its claimant.
    let other_after = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), departed_incarnation).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while attempt 1 runs, mesh2 refuses another birth's recovery: {other:?}"),
    };
    assert_eq!((other_after["mesh"].as_str(), other_after["held_by"].as_str()), (Some("mesh2"), Some("mesh2.admin.2")), "{other_after}");

    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-build-in-progress", "rdm.node_admin.build.update.via-claimant-departure", "rdm.node_admin.build.update.via-claim-decision"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
    let decided = capture.spans("rdm.node_admin.build.update.via-claim-decision");
    assert_eq!(decided.len(), 1, "one claim decided: {decided:?}");
    let (_, d) = &decided[0];
    assert_eq!((d["lane"].as_str(), d["attempt"].as_str(), d["node"].as_str(), d["outcome"].as_str()), ("mesh2", "1", "mesh2.admin.2", "won"), "{d:?}");
    assert!(!capture.spans("rdm.node_admin.build.update.via-claimant-departure").is_empty(), "the departed opener's row is released on its departure (Q142: the row is its claim)");
    let recovery = capture.spans("rdm.node_admin.build.update.via-recovery").into_iter().find(|(_, a)| a["node"] == member.to_string() && a["outcome"] == "admitted").expect("the member's admitted recovery span");
    assert_eq!((recovery.1["attempt"].as_str(), recovery.1["outcome"].as_str(), recovery.1["open_attempt"].as_str()), ("1", "admitted", "held-open"), "{recovery:?}");
    // The refusal before the claim named the opener the row names: an open attempt's holder.
    assert_eq!(other_before["held_by"].as_str(), Some("mesh2.admin.1"), "{other_before}");
}

/// CONTRACT (node-recovery.md "Failure and receipts": "the mesh-primary is the submitter that
/// resubmits by `build_id` ... the mesh-primary's next tick finds the member still proven exited and
/// submits the recovery"; build.md :365 as the approved proposal A4 words it: "Claims are
/// insert-and-fail on `(build_id, attempt)`", so an open attempt's first claim takes it, whoever opened
/// it): mesh2's primary `mesh2.admin.1` runs the recovery of its exited member `mesh2.rpc.1` as attempt
/// 1 of mesh2's lane; its node.delete departs the birth (no view holds it after) and its create fails,
/// so attempt 1 ends Failed. The primary resubmits: it takes mesh2's in-flight row for attempt 2 and
/// opens it with the same recovery, and is lost before it claims it. `mesh2.admin.2` takes mesh2's
/// seat over the same Build log and rows and records the lost primary's exit. The member is owed a
/// submit from attempt 1's Failed verdict: an open no one ran ends no run. The row the lost primary took
/// is its claim on the store (build.md §5.2, Q142), released on its recorded departure; the owed submit
/// then claims the open attempt 2 as it stands, and once claimed nothing is owed. Until then the lost primary's own recovery is refused
/// build-in-progress naming mesh2 and the opener the row names. Must NOT happen: the member's exit
/// owed nothing while no attempt runs it (mesh2 held for good by the open attempt; seen at
/// node_replace's a_failed_build_ cell, s23 part 16: mesh2.admin.2's recovery of mesh2.admin.1
/// refused build-in-progress 209 times, held_by mesh2.admin.1), a verdict written for an attempt
/// no one ran, or a second open of attempt 2.
#[tokio::test]
async fn an_exit_whose_resubmit_a_lost_mesh_primary_opened_and_never_claimed_is_owed_from_its_failed_attempt() {
    use rdm_admin::accepted::AttemptAction;
    use rdm_admin::build_state::{AttemptOpened, BuildState};
    use rdm_admin::mesh_claims::InFlight;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("i143-r-bi7-unclaimed-resubmit", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let claims = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
    let before = two_meshes_two_mesh2_admins("mesh2.admin.1");
    let member: PathName = "mesh2.rpc.1".parse().unwrap();
    let exited = before.node(&member).unwrap().clone();
    let birth = exited.incarnation_id.clone().unwrap();
    let mut accepted_view = before.clone();
    accepted_view.nodes.iter_mut().for_each(|n| n.status = NodeStatus::ReadyForTraffic);
    let accepted = AcceptedStore::seeded(&*builds, before.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let b0 = builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;

    // Attempt 1: opened and claimed by mesh2's primary; its node.delete departs the birth and its
    // create fails; the run releases mesh2's in-flight row at its verdict.
    let lost = Arc::new(ControlPlane::new(builds.clone(), accepted.clone(), "mesh2.admin.1".parse().unwrap(), before.clone(), crate::adopted_time()).with_claims(claims.clone()));
    let Ok(Recovery::Opened(first)) = lost.open_recovery("node-recovery", member.clone(), exited.node_id.clone(), birth.clone()).await else { panic!("mesh2's primary opens its member's recovery") };
    assert_eq!((first.mesh.as_deref(), first.attempt), (Some("mesh2"), 1), "attempt 1 of mesh2's lane");
    for r in deletion_receipts(Some("mesh2"), "node.delete", &b0, 1, &member, &exited.node_id, &birth, "mesh2.admin.1") {
        builds.append_step_receipt(&r).await.unwrap();
    }
    let failed = "node.create:mesh2.rpc.1 step PrepareStorage: node-key is missing in the inherited data dir";
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: Some("mesh2".into()), build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: failed.into() , code: rdm_core::outcome::Code::FailedPrecondition } }).await.unwrap();
    claims.release_in_flight("mesh2", &InFlight { build_id: b0.clone(), lane: Some("mesh2".into()), attempt: 1, claimant: "mesh2.admin.1".into(), claimant_node: None }).await.unwrap();
    assert_eq!(builds.exits_owed(&b0, "mesh2").await.unwrap().iter().map(|o| (o.path.clone(), o.attempt)).collect::<Vec<_>>(), vec![(member.clone(), 1)], "after attempt 1's Failed verdict the member is owed a submit");

    // The resubmit: mesh2's in-flight row for attempt 2, then attempt 2's open carrying the same
    // recovery. Its claim was never written: the primary was lost between the two.
    let action = AttemptAction::Recover { path: member.clone(), node_id: exited.node_id.clone(), from_incarnation: birth.clone(), backbone: false };
    let row = InFlight { build_id: b0.clone(), lane: Some("mesh2".into()), attempt: 2, claimant: "mesh2.admin.1".into(), claimant_node: None };
    assert_eq!(claims.take_in_flight("mesh2", &row).await.unwrap(), rdm_admin::mesh_claims::Taken::Won);
    builds
        .open_attempt(&AttemptOpened { build_id: b0.clone(), attempt: 2, reason: AttemptReason::Recovery, action: Some(action.clone()), opened_by: "mesh2.admin.1".into(), opened_at_ms: 2, mesh: Some("mesh2".into()), context: None })
        .await
        .unwrap();
    let lane = builds.read_build(&b0).await.unwrap().lanes.get("mesh2").cloned().expect("mesh2's lane");
    assert_eq!((lane.state, lane.attempt, lane.opened, lane.claimant.as_deref()), (BuildState::Pending, 1, 2, None), "attempt 2 is open and unclaimed: {lane:?}");

    // mesh2.admin.2 holds the seat; the member departed its view; the lost primary is Dead in it.
    let departed: PathName = "mesh2.admin.1".parse().unwrap();
    let departed_birth = before.node(&departed).unwrap().clone();
    let departed_incarnation = departed_birth.incarnation_id.clone().unwrap();
    let mut after = two_meshes_two_mesh2_admins("mesh2.admin.2");
    after.nodes.retain(|n| n.name != member);
    after.nodes.iter_mut().filter(|n| n.name == departed).for_each(|n| {
        n.node_id = departed_birth.node_id.clone();
        n.incarnation_id = departed_birth.incarnation_id.clone();
        n.status = NodeStatus::Dead;
    });
    let heir = Arc::new(
        ControlPlane::new(builds.clone(), accepted, "mesh2.admin.2".parse().unwrap(), after, crate::adopted_time())
            .with_claims(claims.clone()),
    );
    let departure = Arc::new(Departure::default());
    let _ = heir.departure.set(departure.clone());
    departure.0.store(true, std::sync::atomic::Ordering::SeqCst);

    // The lost primary's own recovery waits for the open attempt: one attempt in flight per mesh.
    let refused = match heir.open_recovery("node-recovery", departed.clone(), departed_birth.node_id.clone(), departed_incarnation.clone()).await {
        Err(r @ Refusal::BuildInProgress(..)) => rendered(r).await,
        other => panic!("while attempt 2 is open for the member, mesh2 refuses another birth's recovery: {other:?}"),
    };
    assert_eq!((refused["mesh"].as_str(), refused["held_by"].as_str()), (Some("mesh2"), Some("mesh2.admin.1")), "named by the mesh and the opener the row names: {refused}");

    // The member stays owed from attempt 1's Failed verdict: the open no one claimed ran nothing.
    let owed = builds.exits_owed(&b0, "mesh2").await.unwrap();
    assert_eq!(
        owed.iter().map(|o| (o.path.clone(), o.node_id.clone(), o.incarnation.clone(), o.attempt, o.reason.clone())).collect::<Vec<_>>(),
        vec![(member.clone(), exited.node_id.clone(), birth.clone(), 1, failed.to_string())],
        "the member's exit is owed a submit from attempt 1's verdict while attempt 2 stands opened and unclaimed: {owed:?}"
    );

    // Its owed submit claims the open attempt 2 as it stands.
    let taken = match heir.open_recovery("node-recovery", owed[0].path.clone(), owed[0].node_id.clone(), owed[0].incarnation.clone()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("the owed submit claims the attempt its lost opener left unclaimed: {other:?}"),
    };
    assert_eq!((taken.build_id.clone(), taken.mesh.as_deref(), taken.attempt), (b0.clone(), Some("mesh2"), 2), "attempt 2 of mesh2's lane, the one open");
    let lane = builds.read_build(&b0).await.unwrap().lanes.get("mesh2").cloned().expect("mesh2's lane");
    assert_eq!((lane.state, lane.attempt, lane.claimant.as_deref()), (BuildState::Running, 2, Some("mesh2.admin.2")), "the new primary claimed attempt 2: {lane:?}");
    assert_eq!(lane.attempt_actions.get(&2), Some(&action), "attempt 2 runs the action its opener opened");
    let facts = builds.facts().await.unwrap();
    let opens: Vec<u32> = facts.iter().filter_map(|f| match f {
        BuildFact::Opened(o) if o.mesh.as_deref() == Some("mesh2") => Some(o.attempt),
        _ => None,
    }).collect();
    assert_eq!(opens, vec![1, 2], "attempt 2 is claimed, never opened again");
    assert!(!facts.iter().any(|f| matches!(f, BuildFact::Attempt(a) if a.mesh.as_deref() == Some("mesh2") && a.attempt == 2)), "no verdict for an attempt no one ran");
    assert!(builds.exits_owed(&b0, "mesh2").await.unwrap().is_empty(), "nothing is owed once the member's recovery is the running attempt");

    for name in ["rdm.node_admin.build.update.via-recovery", "rdm.node_admin.build.reject.via-build-in-progress", "rdm.node_admin.build.update.via-claimant-departure", "rdm.node_admin.build.update.via-claim-decision"] {
        for (trace_id, attrs) in capture.spans(name) {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
    }
    let recovery = capture.spans("rdm.node_admin.build.update.via-recovery").into_iter().find(|(_, a)| a["node"] == member.to_string() && a["attempt"] == "2").expect("the owed submit's recovery span");
    assert_eq!((recovery.1["outcome"].as_str(), recovery.1["open_attempt"].as_str()), ("admitted", "held-open"), "{recovery:?}");
    assert!(!capture.spans("rdm.node_admin.build.update.via-claimant-departure").is_empty(), "the departed opener's row, its claim on the store (Q142), is released on its departure before the owed submit claims attempt 2");
}

/// CONTRACT (node-investigation.md: "A node that comes back after it was re-created is a deleted birth
/// ... No admit reason exists for it, and none is added"; node-recovery.md step 3 and fabric-recovery.md
/// step 3: "An old birth that comes back after that is a deleted birth"; Q181, Luke 2026-10-26: "a node
/// cut off should have been replaced, and so blocked from joining"): an admin whose own birth the
/// store's node rows hold deleted opens no recovery, whatever its own view says. mesh1.admin.1 opens
/// the recovery of its exited member mesh1.rpc.2: while its own row is `Deleted`, and again while
/// another node's live claim holds its path.name (it was re-created at it), the open is refused by
/// name (`deleted-birth`) before anything is read or written: no attempt opened, mesh1's in-flight row
/// untaken. The same open by the live birth (its own row live, its name its own) is admitted. Must
/// NOT happen: a deleted birth opening, claiming or running a recovery attempt.
#[tokio::test]
async fn a_recovery_opened_by_a_deleted_birth_is_refused_by_name_and_writes_nothing() {
    use rdm_admin::storage::{NodeRow, NodesStorage as _};
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("q181-deleted-birth-opens-nothing", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view();
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let me_id = t.node(&me).unwrap().node_id.clone();
    let path: PathName = "mesh1.rpc.2".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, me.clone(), t, crate::adopted_time()));
    let rows = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
    let _ = cp.node_rows.set(rows.clone());
    let _ = cp.node_id.set(me_id.clone());
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    let before = opened(&builds.facts().await.unwrap());

    // Its own row is Deleted: its node recovery re-created it, and this is the old birth.
    rows.put_node(&NodeRow::new(me.clone(), me_id.clone(), NodeStatus::Deleted)).await.unwrap();
    let deleted = cp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&deleted, Err(Refusal::Conflict(why)) if why.contains("deleted birth") && why.contains("mesh1.admin.1")), "a birth whose own row is Deleted opens nothing, refused by name: {deleted:?}");

    // Its own row reads live, but a new birth's claim holds its path.name: it was re-created there.
    let successor = NodeId::mint();
    rows.put_node(&NodeRow::new(me.clone(), me_id.clone(), NodeStatus::ReadyForTraffic)).await.unwrap();
    rows.put_node(&NodeRow::new(me.clone(), successor.clone(), NodeStatus::Pending)).await.unwrap();
    rows.claim_node_name(&me, &successor).await.unwrap().unwrap();
    let replaced = cp.open_recovery("fabric-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&replaced, Err(Refusal::Conflict(why)) if why.contains("deleted birth") && why.contains(successor.as_str())), "a birth whose path.name another node's live claim holds opens nothing, refused by name: {replaced:?}");

    assert_eq!(opened(&builds.facts().await.unwrap()), before, "a deleted birth opens no attempt");
    assert!(cp.claims.in_flight("mesh1").await.unwrap().is_none(), "a deleted birth takes no in-flight row");
    let refusals = capture.spans("rdm.node_admin.build.reject.via-deleted-birth");
    assert_eq!(refusals.len(), 2, "each refusal is spanned by name: {refusals:?}");
    assert_eq!((refusals[0].1.get("node").map(String::as_str), refusals[0].1.get("node_id").map(String::as_str)), (Some("mesh1.admin.1"), Some(me_id.as_str())), "{:?}", refusals[0].1);

    // The live birth: its own row live and its name its own.
    rows.release_node_name(&me, &successor).await.unwrap();
    rows.put_node(&NodeRow::new(me.clone(), successor.clone(), NodeStatus::Deleted)).await.unwrap();
    rows.claim_node_name(&me, &me_id).await.unwrap().unwrap();
    let live = cp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&live, Ok(Recovery::Opened(o)) if o.mesh.as_deref() == Some("mesh1")), "the live birth opens its member's recovery: {live:?}");
    for (trace_id, attrs) in refusals {
        eprintln!("span rdm.node_admin.build.reject.via-deleted-birth trace_id={trace_id} {attrs:?}");
    }
}

/// The fabric-primary's view of a fabric of two meshes whose mesh2 holds two node-admins (or one,
/// `second_admin` false), mesh2's primary `mesh2.admin.1` unheard (`PendingReconnect`, the view still
/// naming it primary) and every other node of mesh2 unheard with it: the view the fabric-primary holds
/// of a mesh it hears only through that mesh's primary. Returns (that view, the accepted view: every
/// node ready for traffic, as it was when accepted).
fn unheard_mesh2_primary(second_admin: bool) -> (Topology, Topology) {
    let mut t = two_meshes();
    if second_admin {
        t.nodes.push(node("mesh2.admin.2", false));
    }
    let accepted = t.clone();
    t.nodes.iter_mut().filter(|n| n.mesh == "mesh2").for_each(|n| n.status = NodeStatus::PendingReconnect);
    (t, accepted)
}

/// The fabric-primary's primaries as they stand once mesh2's election replaced `unheard` with `named`:
/// the record naming the unheard birth, then the one naming its successor at the next revision.
fn election_replaced(fp: &ControlPlane, t: &Topology, unheard: &str, named: &str) {
    let seats = rdm_transport::topology::PrimaryView::default();
    for (revision, name) in [(1, unheard), (2, named)] {
        let n = t.node(&name.parse().unwrap()).unwrap();
        seats.take(rdm_core::Seat::MeshPrimary, &rdm_core::Primary { mesh: "mesh2".into(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), revision });
    }
    assert!(seats.replaced(rdm_core::Seat::MeshPrimary, "mesh2", t.node(&unheard.parse().unwrap()).unwrap().incarnation_id.as_ref().unwrap()), "the record naming {named} replaced {unheard}'s");
    let _ = fp.seats.set(seats);
}

/// CONTRACT (node-recovery.md step 2: "For an unheard mesh-primary the fabric-primary runs it, without
/// the half-floor wait, after the election has named the new mesh-primary"; "Execution": "the
/// fabric-primary opens the attempt ...; the mesh-primary claims it through BuildClaim ...; the
/// mesh-primary runs it"; node-identity-and-state.md: "The fabric-primary writes a row only as acting
/// primary of a mesh with no primary. No other process writes a `Node` row"): the claim of an unheard
/// mesh-primary's recovery decides the attempt's executor, the writer of its rows, once, and follows
/// the election's result as the view names it, the view the rows are written against. While the
/// fabric-primary's view still names the unheard `mesh2.admin.1` mesh2's primary (its primaries already
/// hold the election's record naming `mesh2.admin.2`, which the view has not taken yet) and mesh2 holds
/// another node-admin (`mesh2.admin.2`, unheard in that view as all of mesh2 is), the view names no new
/// mesh-primary: the open is refused by name, naming the mesh and the admin it waits for, and nothing is opened or claimed. Must NOT happen: an attempt claimed for the fabric-primary itself while mesh2
/// holds another node-admin, whose rows mesh2's elected primary then writes (gate 45918ae5e: attempt 4
/// claimed for mesh1.admin.1 at 977.238, its Deleted row refused `not-mesh-primary` at 977.620).
#[tokio::test]
async fn the_fabric_primary_opens_no_recovery_of_an_unheard_mesh_primary_until_the_election_names_its_mesh_primary() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("rdm110-claim-follows-election", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let (t, accepted_view) = unheard_mesh2_primary(true);
    let path: PathName = "mesh2.admin.1".parse().unwrap();
    let unheard = t.node(&path).unwrap().clone();
    let (node_id, birth) = (unheard.node_id.clone(), unheard.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let fp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time()));
    // Its primaries already hold the election's record (the seat watch woke on it), the view not yet.
    election_replaced(&fp, &t, "mesh2.admin.1", "mesh2.admin.2");
    let b0 = fp.builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;
    let before = opened(&builds.facts().await.unwrap());

    // The election has not named mesh2's new primary in this view.
    let unnamed = fp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    let claimed = builds.read_build(&b0).await.unwrap().attempt_executors;
    assert!(matches!(&unnamed, Err(Refusal::Unavailable(why)) if why.contains("mesh2") && why.contains("mesh2.admin.2")), "refused by name while the election has not named mesh2's primary, naming the admin it waits for: {unnamed:?}; claims: {claimed:?}");
    assert_eq!(opened(&builds.facts().await.unwrap()), before, "nothing is opened before the election names mesh2's primary");
    assert!(fp.claims.in_flight("mesh2").await.unwrap().is_none(), "mesh2's in-flight row is not taken");
    let refusals = capture.spans("rdm.node_admin.build.reject.via-mesh-primary-not-named");
    assert_eq!(refusals.len(), 1, "the refusal is spanned by name: {refusals:?}");
    let (_, r) = &refusals[0];
    assert_eq!((r["node"].as_str(), r["mesh"].as_str(), r["member"].as_str(), r["admins"].as_str()), ("mesh1.admin.1", "mesh2", "mesh2.admin.1", "mesh2.admin.2"), "{r:?}");

    for (trace_id, attrs) in &refusals {
        eprintln!("span rdm.node_admin.build.reject.via-mesh-primary-not-named trace_id={trace_id} {attrs:?}");
    }
}

/// CONTRACT (the same pages): the fabric-primary's view names `mesh2.admin.2` mesh2's primary, the
/// election's result, while it still holds every node of mesh2 unheard: the unheard `mesh2.admin.1`'s
/// recovery is opened in the fabric lane and claimed by the fabric-primary for `mesh2.admin.2` (the
/// election's result, not the view's liveness, names the writer), the claim decided once. Must NOT
/// happen: the attempt claimed for the fabric-primary because the named mesh-primary is unheard in its
/// view.
#[tokio::test]
async fn the_fabric_primary_claims_an_unheard_mesh_primarys_recovery_for_the_mesh_primary_the_election_named() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture("rdm110-claim-for-the-named-mesh-primary", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    let capture = Capture { exporter, pipeline };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let (mut t, accepted_view) = unheard_mesh2_primary(true);
    t.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin).for_each(|n| n.is_primary = n.name.to_string() == "mesh2.admin.2");
    let path: PathName = "mesh2.admin.1".parse().unwrap();
    let unheard = t.node(&path).unwrap().clone();
    let (node_id, birth) = (unheard.node_id.clone(), unheard.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let fp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time()));
    election_replaced(&fp, &t, "mesh2.admin.1", "mesh2.admin.2");
    let b0 = fp.builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;
    let named = fp.open_recovery("node-recovery", path.clone(), node_id.clone(), birth.clone()).await;
    let o = match named {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("once the election named mesh2's primary the recovery is opened: {other:?}"),
    };
    assert_eq!(o.mesh, None, "opened in the fabric lane: {o:?}");
    let p = builds.read_build(&b0).await.unwrap();
    assert_eq!(p.attempt_executors.get(&o.attempt).map(String::as_str), Some("mesh2.admin.2"), "claimed for the mesh-primary the election named, the writer of mesh2's rows: {:?}", p.attempt_executors);
    let decisions = capture.spans("rdm.node_admin.build.update.via-claim-decision");
    for (trace_id, attrs) in &decisions {
        eprintln!("span trace_id={trace_id} {attrs:?}");
    }
    let won = decisions.iter().find(|(_, a)| a["attempt"] == o.attempt.to_string() && a["outcome"] == "won").unwrap_or_else(|| panic!("the claim is decided once: {decisions:?}"));
    assert_eq!(won.1["executor"].as_str(), "mesh2.admin.2", "{:?}", won.1);
}

/// CONTRACT (node-identity-and-state.md: "The fabric-primary writes a row only as acting primary of a
/// mesh with no primary"): an unheard mesh-primary that is its mesh's only node-admin leaves no admin
/// for an election to name: the fabric-primary claims its recovery for itself, the acting primary of a
/// mesh with no primary. Must NOT happen: that recovery refused waiting for an election that has no
/// node-admin to name.
#[tokio::test]
async fn the_fabric_primary_claims_the_recovery_of_a_meshs_only_node_admin_for_itself() {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let (t, accepted_view) = unheard_mesh2_primary(false);
    let path: PathName = "mesh2.admin.1".parse().unwrap();
    let unheard = t.node(&path).unwrap().clone();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&accepted_view), "mesh1.admin.1").await.unwrap();
    let fp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = fp.builds.current_build().await.unwrap().into_build_id().unwrap();
    settle_other(&builds, &b0).await;
    let o = match fp.open_recovery("node-recovery", path, unheard.node_id.clone(), unheard.incarnation_id.clone().unwrap()).await {
        Ok(Recovery::Opened(o)) => o,
        other => panic!("the recovery of mesh2's only node-admin is opened: {other:?}"),
    };
    let p = builds.read_build(&b0).await.unwrap();
    assert_eq!((o.mesh.as_deref(), p.attempt_executors.get(&o.attempt).map(String::as_str)), (None, Some("mesh1.admin.1")), "claimed in the fabric lane for the fabric-primary, mesh2's acting primary: {:?}", p.attempt_executors);
}
