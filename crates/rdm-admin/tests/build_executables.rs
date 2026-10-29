//! Functional cells for per-node executable bindings in a Build (`docs/architecture/build-deployment.md`).

use rdm_admin::accepted::{AcceptedStore, FabricTopology, Recorded, RecordedRow, TopologyChange};
use rdm_admin::build::{BuildId, BuildOperation, BuildReject};
use rdm_admin::build_claim::{AttemptContexts, ClaimDoor};
use rdm_admin::build_drive::{Drive, DriveEnd, DriveEnv, Gate};
use rdm_admin::build_state::{
    BuildState, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::executor::OperationRunner;
use rdm_admin::model::*;
use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::topology::Topology;
use rdm_core::meta::NodeSpec;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn named(&self, name: &str) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured {
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }

    fn all_names(&self) -> Vec<String> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| s.name.to_string())
            .collect()
    }
}

fn ready_node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn fixture_topology() -> Topology {
    Topology {
        fabric: Fabric {
            id: FabricId::mint(),
            name: "fabric1".into(),
            status: ScopeStatus::Ready,
            provider: ProviderKind::Process,
        },
        meshes: vec![Mesh {
            id: Some(MeshId::mint()),
            name: "mesh1".into(),
            status: ScopeStatus::Ready,
        }],
        nodes: vec![
            ready_node("mesh1.admin.1", false, true),
            ready_node("mesh1.admin.2", true, false),
            ready_node("mesh1.rpc.1", false, false),
        ],
    }
}

fn recorded_binding(kind: NodeKind, sha: &str) -> rdm_core::binding::Binding {
    rdm_core::binding::Binding {
        launch_id: kind.name().into(),
        executable: format!("/b/{}-{sha}", kind.name()).into(),
        sha256: sha.into(),
        image: None,
    }
}

struct ReadyGate {
    ready: AtomicBool,
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl Gate for ReadyGate {
    fn fabric_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    async fn recorded_executables(&self, _topology: &FabricTopology) -> Result<Recorded, String> {
        Ok(self.recorded.lock().unwrap().clone())
    }
}

struct RecordedRuns {
    ran: Mutex<Vec<String>>,
    builds: Arc<MemoryBuildStateAdapter>,
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl OperationRunner for RecordedRuns {
    async fn run(
        &self,
        build: &BuildId,
        _mesh: Option<&str>,
        attempt: u32,
        op: &BuildOperation,
    ) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.lock().unwrap().push(op.key());
        match op {
            BuildOperation::CreateNode { node, .. } => {
                let topology =
                    self.builds.read_build(build).await.map_err(|e| e.to_string())?.topology;
                let sha =
                    rdm_admin::accepted::target_executable(&topology, node).map(|b| b.sha256.clone());
                self.recorded.lock().unwrap().insert(
                    node.clone(),
                    RecordedRow {
                        config: None,
                        tags: None,
                        storage_persistence: None,
                        placement: None,
                        node_id: NodeId::mint(),
                        sha256: sha,
                    },
                );
            }
            BuildOperation::DeleteNode { node, .. } | BuildOperation::DeleteLiveNode { node, .. } => {
                self.recorded.lock().unwrap().remove(node);
            }
            _ => {}
        }
        let receipt = BuildStepReceipt {
            mesh: None,
            build_id: build.clone(),
            attempt,
            operation: op.key(),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        };
        self.builds.append_step_receipt(&receipt).await.map_err(|e| e.to_string().into())
    }
}

/// Page: `docs/architecture/build-deployment.md:18`
/// "A created path whose spec names no executable is refused by name at acceptance and nothing runs."
/// Page: `docs/architecture/build-deployment.md:29`
/// "A missing file or a differing hash is refused by name; nothing is launched in its place."
/// Page: `docs/architecture/build-deployment.md:38`
/// "A live node whose row already records its entry's config and executable is not deleted, created
/// or restarted because another node changes."
/// Page: `docs/architecture/build-deployment.md:44`
/// "A request that names a different executable for an existing path is a planned replacement. RDM
/// realizes it as node.delete then node.create at the path, a new node.id and incarnation, in its
/// order with its guards"
#[test]
fn build_executable_admission_refuses_missing_or_mismatched_bindings_and_drive_replaces_only_changed_paths()
{
    let capture = Capture::new(
        "build_executable_admission_refuses_missing_or_mismatched_bindings_and_drive_replaces_only_changed_paths",
    );
    let dispatch = capture.dispatch.clone();
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    rt.block_on(async {
        let t = fixture_topology();
        let mut held = FabricTopology::of_observed(&t);
        for (p, sha) in [("mesh1.admin.2", "sha-admin-2"), ("mesh1.rpc.1", "sha-rpc-old")] {
            let path: PathName = p.parse().unwrap();
            let mesh = held.meshes.get_mut("mesh1").unwrap();
            let mut spec = mesh.spec(&path).cloned().unwrap();
            spec.executable = Some(recorded_binding(path.kind, sha));
            mesh.set_spec(spec).unwrap();
        }

        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let accepted = AcceptedStore::seeded(
            &*builds,
            t.fabric.id.clone(),
            held.clone(),
            "mesh1.admin.1",
        )
        .await
        .unwrap();
        let topology = Arc::new(RwLock::new(t.clone()));
        let store = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
        let mut recorded = Recorded::new();
        for (p, sha) in [
            ("mesh1.admin.1", "sha-admin-1"),
            ("mesh1.admin.2", "sha-admin-2"),
            ("mesh1.rpc.1", "sha-rpc-old"),
        ] {
            let path: PathName = p.parse().unwrap();
            let n = t.node(&path).unwrap();
            let mut row = rdm_admin::storage::NodeRow::new(
                n.name.clone(),
                n.node_id.clone(),
                NodeStatus::Ready,
            );
            row.executable = Some(recorded_binding(n.name.kind, sha));
            store.write_record(&row).await.unwrap();
            recorded.insert(
                n.name.clone(),
                RecordedRow {
                    config: None,
                    tags: None,
                    storage_persistence: None,
                    placement: None,
                    node_id: n.node_id.clone(),
                    sha256: Some(sha.into()),
                },
            );
        }

        let mut cp = ControlPlane::new(
            builds.clone(),
            accepted.clone(),
            "mesh1.admin.1".parse().unwrap(),
            t.clone(),
            crate::adopted_time(),
        );
        cp.topology = topology.clone();
        let _ = cp.node_rows.set(store);

        let base_build = builds.current().await.unwrap().unwrap().build_id;
        let base_specs: Vec<NodeSpec> =
            held.meshes.values().flat_map(|m| m.spec_by_path.values().cloned()).collect();

        // 1. A created path whose spec names no executable is refused by name at acceptance.
        let mut missing_specs = base_specs.clone();
        missing_specs.push(NodeSpec::default_for("mesh1.gateway.1".parse().unwrap()));
        let missing_res = cp
            .submit(
                "test-missing-exe",
                TopologyChange::Reconcile {
                    topology_storage_provider: None,
                    based_on: base_build.clone(),
                    nodes: missing_specs,
                },
            )
            .await;
        match missing_res {
            Err(Refusal::Reject(BuildReject::ExecutableMissing { node })) => {
                assert_eq!(node, "mesh1.gateway.1");
            }
            other => panic!("expected ExecutableMissing for mesh1.gateway.1, got {other:?}"),
        }
        assert!(
            builds.list_active().await.unwrap().is_empty(),
            "refused missing executable must not open an active Build"
        );

        // 2. A created or replaced path with a differing sha256 is refused by name at acceptance.
        let mut wrong_sha_specs = base_specs.clone();
        let rpc_idx = wrong_sha_specs
            .iter()
            .position(|s| s.path.to_string() == "mesh1.rpc.1")
            .unwrap();
        let mut bad_binding = crate::test_executable(NodeKind::RpcNode);
        bad_binding.sha256 =
            "0000000000000000000000000000000000000000000000000000000000000000".into();
        wrong_sha_specs[rpc_idx].executable = Some(bad_binding);
        let wrong_sha_res = cp
            .submit(
                "test-wrong-sha",
                TopologyChange::Reconcile {
                    topology_storage_provider: None,
                    based_on: base_build.clone(),
                    nodes: wrong_sha_specs,
                },
            )
            .await;
        match wrong_sha_res {
            Err(Refusal::Reject(BuildReject::ExecutableRefused { node, detail })) => {
                assert_eq!(node, "mesh1.rpc.1");
                assert!(
                    detail.contains("hashes to") && detail.contains("the binding expects"),
                    "refusal detail must report hash mismatch: {detail}"
                );
            }
            other => panic!("expected ExecutableRefused for mesh1.rpc.1, got {other:?}"),
        }
        assert!(
            builds.list_active().await.unwrap().is_empty(),
            "refused mismatched hash must not open an active Build"
        );

        // 3. Valid replacement of mesh1.rpc.1's executable + creation of mesh1.gateway.1 is accepted
        //    and replaces only mesh1.rpc.1 while creating mesh1.gateway.1 and leaving mesh1.admin.2 untouched.
        let mut valid_specs = base_specs.clone();
        valid_specs[rpc_idx].executable = Some(crate::test_executable(NodeKind::RpcNode));
        let mut gw_spec = NodeSpec::default_for("mesh1.gateway.1".parse().unwrap());
        gw_spec.executable = Some(crate::test_executable(NodeKind::Gateway));
        valid_specs.push(gw_spec);

        let opened = cp
            .submit(
                "test-valid-reconcile",
                TopologyChange::Reconcile {
                    topology_storage_provider: None,
                    based_on: base_build,
                    nodes: valid_specs,
                },
            )
            .await
            .expect("valid executable bindings must be accepted");

        let recorded = Arc::new(Mutex::new(recorded));
        let gate = Arc::new(ReadyGate { ready: AtomicBool::new(true), recorded: recorded.clone() });
        let runs = Arc::new(RecordedRuns {
            ran: Mutex::new(Vec::new()),
            builds: builds.clone(),
            recorded,
        });
        let loopback = crate::loopback::Loopback::new(
            builds.clone(),
            builds.clone(),
            topology.clone(),
            runs.clone(),
        );
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let fp_id = t.node(&me).unwrap().node_id.clone();
        let env = Arc::new(DriveEnv {
            me: me.clone(),
            topology: topology.clone(),
            builds: builds.clone(),
            door: Arc::new(ClaimDoor {
                node_id: fp_id,
                me,
                topology,
                builds: builds.clone(),
                contexts: Arc::new(AttemptContexts::in_memory()),
                claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
                claimed: Default::default(),
            }),
            dispatcher: loopback,
            gate,
            departure: Arc::new(crate::loopback::Proofs::default()),
            verdicts: Arc::new(crate::loopback::NoVerdicts),
            notices: Default::default(),
            turns: rdm_admin::stop_gate::StopGate::new(),
        });

        let drive = Drive::detached(opened.build_id.clone());
        let mut finished = false;
        for _ in 0..4 {
            if env.run(&drive).await == DriveEnd::Terminal {
                finished = true;
                break;
            }
        }
        assert!(finished, "the Build drive must reach Terminal");
        assert_eq!(
            builds.read_build(&opened.build_id).await.unwrap().state,
            BuildState::Complete,
            "the accepted Build must reach Complete"
        );

        let mut executed = runs.ran.lock().unwrap().clone();
        executed.sort();
        assert_eq!(
            executed,
            vec![
                "node.create:mesh1.gateway.1",
                "node.create:mesh1.rpc.1",
                "node.delete:mesh1.rpc.1",
            ],
            "only the replaced path (mesh1.rpc.1) and created path (mesh1.gateway.1) run operations; untouched mesh1.admin.2 is neither deleted nor created"
        );
    });
    drop(rt);

    let hash_spans = capture.named("rdm.node_admin.deployment.resolve.via-executable-hash");
    assert!(
        hash_spans
            .iter()
            .any(|s| s.attributes.get("outcome").map(String::as_str) == Some("refused")),
        "the mismatched sha256 check must record outcome=refused on rdm.node_admin.deployment.resolve.via-executable-hash"
    );
    assert!(
        hash_spans
            .iter()
            .any(|s| s.attributes.get("outcome").map(String::as_str) == Some("hashed")),
        "the accepted executable bindings must record outcome=hashed on rdm.node_admin.deployment.resolve.via-executable-hash"
    );
    let accepted_spans = capture.named("rdm.node_admin.build.create.via-accepted");
    assert!(
        !accepted_spans.is_empty(),
        "expected rdm.node_admin.build.create.via-accepted for the accepted Build"
    );
    let names = capture.all_names();
    crate::assert_own_story(names.iter().map(String::as_str));
}
