//! The fabric-primary's admission of a node recovery (node-recovery.md "Execution", #3051 cell 4):
//! the mesh-primary submits one recovery Build keyed on the exited birth `(node.id, incarnation)`;
//! the fabric-primary admits it as the next attempt of the accepted Build under the one-Build-in-
//! flight rule, and a repeat submit answers `AlreadyApplied` or `BuildInProgress`, never a second
//! recovery.

use opentelemetry::trace::TracerProvider as _;
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::http::{ControlPlane, Recovery, Refusal};
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
    provider: opentelemetry_sdk::trace::TracerProvider,
}

impl Capture {
    fn spans(&self, name: &str) -> Vec<(String, std::collections::BTreeMap<String, String>)> {
        let _ = self.provider.force_flush();
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
    builds.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
}

fn opened(facts: &[BuildFact]) -> usize {
    facts.iter().filter(|f| matches!(f, BuildFact::Opened(_))).count()
}

/// CONTRACT (#3051 acceptance 4, node-recovery.md "Numbered end-to-end sequence" step 1: "A repeat
/// submit answers AlreadyApplied or BuildInProgress, never a second recovery"): the fabric-primary
/// admits the first recovery of `mesh1.rpc.2`'s exited birth as the next attempt of the accepted
/// Build. A repeat while that attempt is open answers `BuildInProgress`; a repeat once its
/// `recover-node` operation completed for that exact birth answers `AlreadyApplied` naming the
/// attempt that recovered it; a recovery submitted while another Build runs answers that Build's
/// `BuildInProgress`. A node-admin is never node-recovered: refused by name. A repeat after a later
/// topology Build was accepted is still `AlreadyApplied`: the recovery is keyed on the exited birth
/// across the Build log, not on the accepted Build's receipts. Must NOT happen: a second attempt
/// opened for a birth already recovered.
#[tokio::test]
async fn a_duplicate_recovery_submit_for_the_same_exited_birth_opens_one_recovery() {
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", "i143-3051-cell4")]))
        .build();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-3051"))));
    let capture = Capture { exporter, provider };

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view();
    let path: PathName = "mesh1.rpc.2".parse().unwrap();
    let exited = t.node(&path).unwrap().clone();
    let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = cp.accepted.build_id().await.unwrap();
    // `mesh1.rpc.2` has gone silent since: TRUE OFFLINE, Dead in the view.
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == path).for_each(|n| n.status = NodeStatus::Dead);
    let settle = |attempt: u32| {
        let (builds, b0) = (builds.clone(), b0.clone());
        async move {
            builds.claim_attempt(&BuildAttemptClaim { build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
            builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: b0, attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
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

    // The executor completes `recover-node:mesh1.rpc.2` for the exited birth.
    builds.claim_attempt(&BuildAttemptClaim { build_id: b0.clone(), attempt: 2, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds
        .append_step_receipt(&BuildStepReceipt {
            build_id: b0.clone(),
            attempt: 2,
            operation: "recover-node:mesh1.rpc.2".into(),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: Some(serde_json::json!({ "node_id": node_id, "incarnation": birth })),
            executor: Some("mesh1.admin.1".into()),
        })
        .await
        .unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: b0.clone(), attempt: 2, outcome: AttemptOutcome::Converged }).await.unwrap();

    // A repeat once the birth was recovered.
    let again = cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await;
    assert!(matches!(&again, Ok(Recovery::AlreadyApplied(o)) if *o == first), "a repeat once the birth was recovered names the attempt that recovered it, and opens nothing: {again:?}");
    assert_eq!(opened(&builds.facts().await.unwrap()), after_first, "exactly one recovery attempt was opened for the exited birth");

    // A node-admin is never node-recovered.
    let admin = cp.topology.read().await.node(&"mesh1.admin.2".parse().unwrap()).unwrap().clone();
    let refused = cp.open_recovery("build.create", admin.name.clone(), admin.node_id.clone(), admin.incarnation_id.clone().unwrap()).await;
    assert!(matches!(&refused, Err(Refusal::Reject(r)) if r.reason() == "node-admin-not-node-recovered"), "a node-admin's node recovery is refused by name: {refused:?}");

    // Another Build in flight: the recovery waits for it.
    let other = cp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await.expect("a topology change is accepted");
    let later = cp.open_recovery("build.create", path.clone(), NodeId::mint(), IncarnationId::mint()).await;
    assert!(matches!(&later, Err(Refusal::BuildInProgress(id)) if *id == other.build_id), "{later:?}");

    // The topology Build settles and is the accepted Build now: the exited birth's recovery lives in
    // an earlier Build's receipts. A repeat submit of it is still the recovery already applied,
    // never a recovery of the path that a new node now runs.
    settle_other(&builds, &other.build_id).await;
    assert_eq!(cp.accepted.build_id().await.as_ref(), Some(&other.build_id), "the topology Build is the accepted one");
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
