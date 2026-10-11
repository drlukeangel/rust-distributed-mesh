//! i143 R-J1 (Luke): the join is a Node RPC op (`JoinNode`, `0x1D`), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-rj1-unit`, which exports `I143_ACCEPTANCE_DIR`; the cell
//! leaves `result.json` (its direct observations) and `spans.json` (every span of its own
//! runtime, captured by an in-memory OTel exporter) there.
//!
//! The admin holds what it deployed and verifies the digest a node reports against it. A digest
//! that disagrees (another incarnation, another node id, another endpoint) is refused by name,
//! with both values, and installs nothing. A digest that matches installs the address the node
//! reported for its key and completes the wait for the node's own report.

use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rafka_node_admin_core::join::{Deployed, JoinDoor, Joins};
use rafka_node_rpc_contract::join::{JoinReply, JoinRequest};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rj1/unit").join(cell),
    }
}

/// This cell's own span capture: the product span pipeline (`rafka_mesh_telemetry::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on
/// every thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-rj1-{cell}");
    let (capture, layer) = rafka_mesh_telemetry::span_capture(&service, exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self) -> Vec<Value> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}


fn deployed() -> Deployed {
    Deployed {
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        endpoint_id: EndpointId("node-key".into()),
        runtime: RuntimeFact::of_this_process("dep-rj1").unwrap(),
        data_dir: "/data/mesh1.rpc.1".into(),
        last_accepted_name: None,
        held: false,
    }
}

fn digest_of(d: &Deployed, addr: &str) -> MeshDigest {
    MeshDigest {
        fabric_id: FabricId::mint(),
        node: MeshNode {
            node_id: d.node_id.clone(),
            name: d.name.clone(),
            endpoint_id: d.endpoint_id.clone(),
            transport_addr: addr.parse().unwrap(),
            incarnation: d.incarnation.clone(),
            supersedes: d.supersedes.clone(),
            runtime: Some(d.runtime.clone()),
        },
        status: MemberStatus::Pending,
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: Some(d.data_dir.clone()),
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

/// CONTRACT (R-J1): the admin that deployed a birth answers its `JoinNode` only for the birth it
/// deployed. A digest with another incarnation is refused as `JoinMismatch` naming the field and
/// both incarnations; a digest of a node this admin deployed nothing for is `NotAuthority`;
/// neither installs an address or completes the wait for the node's report. The matching digest
/// installs exactly the address the node reported (the OS-assigned port), and that report is what
/// the deployment's `WaitForBind` receives. A digest sent by an endpoint other than the one it
/// names is `Unauthorized`.
#[test]
fn join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match() {
    let cell = "join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let log = installed.clone();
        let door = JoinDoor {
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rafka_node_admin_core::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: rafka_node_admin_core::wire::JoinControl { provider: rafka_node_admin_core::model::ProviderKind::Process, fabric: None, shutdown: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.2".into())),
            row: Arc::new(|_| Box::pin(async { Ok(None) })),
            issuer: Arc::new(rafka_node_admin_core::certs::CertIssuer::no_certs({
                let t = rafka_mesh_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let dep = deployed();
        let reported = joins.expect(dep.clone());
        let ask = |d: &MeshDigest| JoinRequest::JoinNode { digest: d.into() };
        let peer = dep.endpoint_id.clone();

        let mut wrong_incarnation = digest_of(&dep, "127.0.0.1:34567");
        wrong_incarnation.node.incarnation = IncarnationId::mint();
        let mismatch = door.serve(peer.clone(), ask(&wrong_incarnation)).await;
        match &mismatch {
            JoinReply::JoinMismatch { field, deployed, reported } => {
                assert_eq!(field, "incarnation");
                assert!(deployed.contains(&dep.incarnation.0) && reported.contains(&wrong_incarnation.node.incarnation.0), "{mismatch:?}");
            }
            other => panic!("{other:?}"),
        }
        let mut wrong_node = digest_of(&dep, "127.0.0.1:34567");
        wrong_node.node.node_id = NodeId::mint();
        let unknown = door.serve(peer.clone(), ask(&wrong_node)).await;
        assert_eq!(unknown, JoinReply::NotAuthority { primary: Some("mesh1.admin.2".into()) });
        assert!(installed.lock().unwrap().is_empty(), "a refused join installs nothing");
        assert!(reported.borrow().is_none(), "a refused join completes no WaitForBind");

        let good = digest_of(&dep, "127.0.0.1:34567");
        let joined = door.serve(peer.clone(), ask(&good)).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.node.transport_addr), Some(good.node.transport_addr), "WaitForBind receives the node's own report");
        let forged = door.serve(EndpointId("someone-else".into()), ask(&good)).await;
        assert!(matches!(forged, JoinReply::Unauthorized { .. }), "{forged:?}");
        json!({
            "cell": cell,
            "mismatch": format!("{mismatch:?}"),
            "unknown_node": format!("{unknown:?}"),
            "installed": installed.lock().unwrap().iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            "forged_sender": format!("{forged:?}"),
        })
    });
    let spans = cap.spans();
    let names = |n: &str| spans.iter().filter(|s| s["name"] == n).count();
    assert_eq!(names("rdm.node_admin.node.reject.via-join-mismatch"), 1, "the refusal is a span of its own");
    assert_eq!(names("rdm.node_admin.node.reject.via-join-unknown"), 1);
    let refusal = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").unwrap();
    assert_eq!(refusal["attributes"]["field"], "incarnation", "{refusal}");
    assert!(spans.iter().any(|s| s["name"] == "rdm.node_admin.node.update.via-join" && s["attributes"]["outcome"] == "installed"));
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// The door of an admin whose node row at `mesh1.rpc.1` is `row`, its installs logged in `log`.
fn door_over(joins: Arc<Joins>, log: Arc<Mutex<Vec<std::net::SocketAddr>>>, row: rafka_node_admin_core::storage::NodeRow) -> JoinDoor {
    JoinDoor {
        me: "mesh1.admin.1".parse().unwrap(),
        joins,
        answer: Arc::new(|| {
            Box::pin(async {
                Ok(rafka_node_admin_core::wire::JoinAnswer {
                    served_by: "mesh1.admin.1".into(),
                    control: rafka_node_admin_core::wire::JoinControl { provider: rafka_node_admin_core::model::ProviderKind::Process, fabric: None, shutdown: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                    statuses: vec![],
                })
            })
        }),
        install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
        known: Arc::new(|| Box::pin(async {})),
        primary: Arc::new(|| Some("mesh1.admin.2".into())),
        row: Arc::new(move |_| {
            let row = row.clone();
            Box::pin(async move { Ok(Some(row)) })
        }),
        issuer: Arc::new(rafka_node_admin_core::certs::CertIssuer::no_certs({
            let t = rafka_mesh_transport::clock::RafkaTime::unadopted();
            t.adopt(1_000);
            t
        })),
    }
}

/// CONTRACT (R-JN1 items 2 and 5; node-start.md step 2): a held birth's start registered by an
/// admin that holds no stopped digest of it (it joined after the stop: no last accepted name) is
/// still a held birth: its join reads the row at its name and hands the row's name and config in
/// the answer. Whether a join is reconciled to its row is what registered the deployment says
/// (`Deployed::held`), never inferred from a last accepted name.
#[test]
fn a_held_births_join_with_no_last_accepted_name_is_handed_its_rows_config() {
    let cell = "a_held_births_join_with_no_last_accepted_name_is_handed_its_rows_config";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = Deployed { held: true, ..deployed() };
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rafka_node_admin_core::storage::NodeRow { name: dep.name.clone(), node_id: dep.node_id.clone(), status: rafka_node_admin_core::model::NodeStatus::Leaving, config: [("tier".to_string(), "gold".to_string())].into_iter().collect() };
        let door = door_over(joins.clone(), installed.clone(), row);
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34569");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("{joined:?}") };
        let answer = rafka_node_admin_core::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.config.as_ref().and_then(|c| c.get("tier")).map(String::as_str), Some("gold"), "the held birth is handed its row's config: {answer:?}");
        assert_eq!(answer.control.name.as_ref().map(|n| n.to_string()).as_deref(), Some("mesh1.rpc.1"), "and its row's name");
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!((join["attributes"]["outcome"].as_str(), join["attributes"]["config_keys"].as_str()), (Some("installed"), Some("1")), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (R-JN1 item 6; node-create.md step 4; node-recovery.md steps 3-4): a first start (a
/// birth this authority does not hold: no last accepted name) is admitted under its create spec's
/// name whatever row stands at that `path.name`. A recovery creates the new node at the exited
/// node's `path.name`; where another node's row stands there at the join (a create that wrote no
/// claim over the predecessor's `Deleted` row), the join installs the reported address and answers
/// `Joined` with no config handed, never `JoinMismatch` naming the predecessor's row. Only a held
/// birth's join is refused over another node's row.
#[test]
fn a_first_start_at_a_path_name_whose_row_names_its_exited_predecessor_is_admitted_under_its_create_spec_name() {
    let cell = "a_first_start_at_a_path_name_whose_row_names_its_exited_predecessor_is_admitted_under_its_create_spec_name";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = deployed();
    let predecessor = NodeId::mint();
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let log = installed.clone();
        let row = rafka_node_admin_core::storage::NodeRow {
            name: dep.name.clone(),
            node_id: predecessor.clone(),
            status: rafka_node_admin_core::model::NodeStatus::Deleted,
            config: [("tier".to_string(), "gold".to_string())].into_iter().collect(),
        };
        let door = JoinDoor {
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rafka_node_admin_core::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: rafka_node_admin_core::wire::JoinControl { provider: rafka_node_admin_core::model::ProviderKind::Process, fabric: None, shutdown: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.2".into())),
            row: Arc::new(move |_| {
                let row = row.clone();
                Box::pin(async move { Ok(Some(row)) })
            }),
            issuer: Arc::new(rafka_node_admin_core::certs::CertIssuer::no_certs({
                let t = rafka_mesh_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34568");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("a first start is admitted under its create spec's name over its predecessor's row: {joined:?}") };
        let answer = rafka_node_admin_core::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.config, None, "another node's row hands the first start no config: {answer:?}");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.node.node_id.clone()), Some(dep.node_id.clone()), "WaitForBind receives the new node's own report");
        json!({ "cell": cell, "joined": format!("{joined:?}"), "predecessor": predecessor.as_str() })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!((join["attributes"]["outcome"].as_str(), join["attributes"]["admitted_as"].as_str()), (Some("installed"), Some("mesh1.rpc.1")), "{join}");
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").count(), 0, "no refusal names the predecessor's row");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (node-recovery.md step 4: "Its config is seeded from the Deleted row at that
/// path.name"; node-create.md step 1: the claim is written before anything is launched; node-start.md
/// step 2: the join answer hands the row's config, which the node installs before it publishes
/// anything or declares readiness): a first start whose row at its create spec's name is its own
/// (the create's claim, seeded from the exited predecessor's Deleted row) is handed that row's
/// config in its join answer, and the digest the authority installs for it carries that config, so
/// the writer's arrival finds the birth already carrying its row's config and tells it nothing (no
/// config call racing the node's start, which a node whose authority is not filled answers
/// NotReady and never applies: gate 7fd59dec, node_replace, via-config config-not-applied).
#[test]
fn a_first_start_whose_row_is_its_own_seeded_claim_is_handed_and_installed_with_the_rows_config() {
    let cell = "a_first_start_whose_row_is_its_own_seeded_claim_is_handed_and_installed_with_the_rows_config";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = deployed();
    let gold: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into_iter().collect();
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::collections::BTreeMap<String, String>>>> = Arc::default();
        let log = installed.clone();
        let row = rafka_node_admin_core::storage::NodeRow { name: dep.name.clone(), node_id: dep.node_id.clone(), status: rafka_node_admin_core::model::NodeStatus::Pending, config: gold.clone() };
        let mut door = door_over(joins.clone(), Arc::default(), row);
        door.install = Arc::new(move |d| log.lock().unwrap().push(d.config.clone()));
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34570");
        assert!(good.config.is_empty(), "the first start reports no config of its own");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("{joined:?}") };
        let answer = rafka_node_admin_core::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.config.as_ref(), Some(&gold), "the first start is handed its own row's config: {answer:?}");
        assert_eq!(*installed.lock().unwrap(), vec![gold.clone()], "the installed digest carries the row's config");
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!((join["attributes"]["outcome"].as_str(), join["attributes"]["config_keys"].as_str()), (Some("installed"), Some("1")), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
