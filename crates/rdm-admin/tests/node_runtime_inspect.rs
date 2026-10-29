//! Functional cell for `node.runtime.inspect` (`docs/architecture/lifecycles/node-runtime-inspect.md`).

use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, Inspected};
use rdm_admin::model::{IncarnationId, Node, NodeId, NodeStatus};
use rdm_admin::node_investigation::{inspect_birth_with_handle, span_inspection};
use rdm_admin::storage::{MemoryNodesStorage, NodesStorage, RuntimeRow};
use rdm_core::runtime::ExitRecord;
use rdm_core::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use std::collections::BTreeMap;
use std::time::Duration;
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
}

fn allocated_birth(name: &str) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.node_id = NodeId::mint();
    n.incarnation_id = Some(IncarnationId::mint());
    n.status = NodeStatus::PendingReconnect;
    n
}

// @feature: node-runtime-inspect
/// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:28
/// "Result: `running`: the exact runtime is alive, however silent the node. `os-stopped`: the
///  operating system holds the exact runtime suspended, so no `node.stop` completed. `exited { code }`:
///  the provider's terminal status for that exact handle.
///  Not a result: A RuntimeFact's presence says neither running nor gone; a provider that cannot
///  answer for the domain answers `unavailable { reason }`, which is investigation's `unproven`
///  Spans: `rdm.node_admin.node.resolve.via-provider-inspection` with node, node_id, incarnation,
///  the handle, `verdict` (`running` / `os-stopped` / `exited` / `unavailable`), `exit_code` and the reason"
#[test]
fn node_runtime_inspect_reports_running_exited_with_proven_code_and_unavailable_across_domains() {
    let capture = Capture::new("node-runtime-inspect-functional");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = std::env::temp_dir().join(format!(
        "rdm-node-runtime-inspect-{}",
        rdm_core::IncarnationId::mint()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    rt.block_on(async {
        let provider = ProcessDeploymentProvider::new();
        let digest_view = rdm_transport::topology::DigestView::default();
        let store = MemoryNodesStorage::default();

        // 1. Live process (this process's own exact PID + start token in this host's control domain) -> Running.
        let running_node = allocated_birth("mesh1.rpc.1");
        let running_inc = running_node.incarnation_id.clone().unwrap();
        let running_dep = rdm_core::fixture_crockford60("dep-running-1");
        let running_fact = RuntimeFact::of_this_process(&running_dep).expect("of_this_process");
        store
            .put_runtime(&RuntimeRow {
                node_id: running_node.node_id.clone(),
                name: running_node.name.clone(),
                incarnation_id: running_inc,
                runtime: running_fact,
                data_dir: None,
            })
            .await
            .unwrap();

        let (out1, handle1) =
            inspect_birth_with_handle(&provider, &digest_view, Some(&store), &running_node).await;
        assert_eq!(out1, Inspected::Running);
        assert!(handle1.is_some());
        span_inspection(
            "mesh1.admin.1",
            &running_node,
            &out1,
            handle1.as_ref(),
            "node-runtime-inspect",
        );

        // 2. Gone process with a proven ExitRecord in its data_dir -> Exited(Some(4)).
        let exited_node = allocated_birth("mesh1.rpc.2");
        let exited_inc = exited_node.incarnation_id.clone().unwrap();
        let exited_dep = rdm_core::fixture_crockford60("dep-exited-2");
        ExitRecord {
            deployment_id: exited_dep.clone(),
            incarnation: exited_inc.as_str().to_string(),
            code: 4,
            reason: "gossip refused".into(),
        }
        .write(&dir)
        .unwrap();
        store
            .put_runtime(&RuntimeRow {
                node_id: exited_node.node_id.clone(),
                name: exited_node.name.clone(),
                incarnation_id: exited_inc,
                runtime: RuntimeFact {
                    deployment_id: exited_dep,
                    provider: RuntimeProvider::Process,
                    control_domain: provider.control_domain(),
                    locator: RuntimeLocator::Process {
                        pid: 999_999,
                        start: 1,
                    },
                },
                data_dir: Some(dir.display().to_string()),
            })
            .await
            .unwrap();

        let (out2, handle2) =
            inspect_birth_with_handle(&provider, &digest_view, Some(&store), &exited_node).await;
        assert_eq!(out2, Inspected::Exited(Some(4)));
        assert!(handle2.is_some());
        span_inspection(
            "mesh1.admin.1",
            &exited_node,
            &out2,
            handle2.as_ref(),
            "node-runtime-inspect",
        );

        // 3. Foreign control domain -> Unavailable with no handle.
        let foreign_node = allocated_birth("mesh1.rpc.3");
        let foreign_inc = foreign_node.incarnation_id.clone().unwrap();
        store
            .put_runtime(&RuntimeRow {
                node_id: foreign_node.node_id.clone(),
                name: foreign_node.name.clone(),
                incarnation_id: foreign_inc,
                runtime: RuntimeFact {
                    deployment_id: rdm_core::fixture_crockford60("dep-foreign-3"),
                    provider: RuntimeProvider::Process,
                    control_domain: "process:foreign-host:999".into(),
                    locator: RuntimeLocator::Process {
                        pid: 12345,
                        start: 100,
                    },
                },
                data_dir: None,
            })
            .await
            .unwrap();

        let (out3, handle3) =
            inspect_birth_with_handle(&provider, &digest_view, Some(&store), &foreign_node).await;
        assert!(matches!(out3, Inspected::Unavailable(_)));
        assert!(handle3.is_none());
        span_inspection(
            "mesh1.admin.1",
            &foreign_node,
            &out3,
            handle3.as_ref(),
            "node-runtime-inspect",
        );
    });

    drop(rt);
    let _ = std::fs::remove_dir_all(&dir);

    let spans = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
    assert_eq!(spans.len(), 3);

    let verdicts: Vec<(&str, &str, &str)> = spans
        .iter()
        .map(|s| {
            (
                s.attributes
                    .get("node")
                    .map(String::as_str)
                    .unwrap_or_default(),
                s.attributes
                    .get("verdict")
                    .map(String::as_str)
                    .unwrap_or_default(),
                s.attributes
                    .get("exit_code")
                    .map(String::as_str)
                    .unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        verdicts,
        vec![
            ("mesh1.rpc.1", "running", ""),
            ("mesh1.rpc.2", "exited", "4"),
            ("mesh1.rpc.3", "unavailable", ""),
        ]
    );
    assert!(
        spans[2]
            .attributes
            .get("reason")
            .is_some_and(|r| r.contains("control domain")),
        "unavailable span must record the foreign control domain reason"
    );
}
