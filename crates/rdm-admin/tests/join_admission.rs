//! i143 R-J1 (Luke): the join is a Node RPC op (`JoinNode`, `0x1D`), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-rj1-unit`, which exports `I143_ACCEPTANCE_DIR`; the cell
//! leaves `result.json` (its direct observations) and `spans.json` (every span of its own
//! runtime, captured by an in-memory OTel exporter) there.
//!
//! The admin holds what it deployed and verifies the digest a node reports against it. A digest
//! that disagrees (another incarnation, another node id, another endpoint) is refused by name,
//! with both values, and installs nothing. A digest that matches installs the address the node
//! reported for its key and completes the wait for the node's own report.

use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rdm_admin::join::{Deployed, JoinService, Joins};
use rdm_core::join::{JoinReply, JoinRequest};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rj1/unit").join(cell),
    }
}

/// This cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on
/// every thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let service = format!("i143-rj1-{cell}");
    let (capture, layer) = rdm_transport::span_capture(&service, exporter.clone());
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
    Deployed { mesh: "mesh1".into(), r#type: "node".into(),
        path: "mesh1.node".into(), name: "1".into(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        endpoint_id: EndpointId("node-key".into()),
        runtime: RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-rj1")).unwrap(),
        data_dir: "/data/mesh1.node.1".into(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    }
}

fn digest_of(d: &Deployed, addr: &str) -> MeshDigest {
    MeshDigest { 
        fabric_id: FabricId::mint(),
        node: MeshNode { mesh: d.mesh.clone(), r#type: d.r#type.clone(),
            node_id: d.node_id.clone(),
            path: d.path.clone(), name: d.name.clone(),
            endpoint_id: d.endpoint_id.clone(),
            transport_addr: addr.parse().unwrap(),
            incarnation: d.incarnation.clone(),
            supersedes: d.supersedes.clone(),
            runtime: Some(d.runtime.clone()),
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: Some(d.data_dir.clone()),
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

/// CONTRACT (R-J1): the admin that deployed a birth answers its `JoinNode` only for the birth it
/// deployed. A digest with another incarnation is refused as `JoinMismatch` naming the field and
/// both incarnations; a digest of a node this admin deployed nothing for is `NotPrimary`;
/// neither installs an address or completes the wait for the node's report. The matching digest
/// installs exactly the address the node reported (the OS-assigned port), and that report is what
/// the deployment's `WaitForBind` receives. Nothing inside the mesh checks who is calling
/// (node-rpc.md §14): a digest sent by an endpoint other than the one it names is decided on the
/// digest, as the node's own is.
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
        let service = JoinService {
            nodes: None,
            me_path: "mesh1.node-admin".into(), me_name: "1".into(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rdm_admin::wire::JoinAnswer {
                        served_by: NodeId::parse("0123456789ab").unwrap(),
                        control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), member_cert_dates: None, signing_cert_dates: None, name: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| Some(rdm_core::NodeId::parse("0123456789ab").unwrap())),
            atlas: None,
            issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let dep = deployed();
        let reported = joins.expect(dep.clone());
        let ask = |d: &MeshDigest| JoinRequest::JoinNode { digest: d.into(), listeners: Vec::new() };
        let peer = dep.endpoint_id.clone();

        let mut wrong_incarnation = digest_of(&dep, "127.0.0.1:34567");
        wrong_incarnation.node.incarnation = IncarnationId::mint();
        let mismatch = service.serve(peer.clone(), ask(&wrong_incarnation)).await;
        match &mismatch {
            JoinReply::JoinMismatch { field, deployed, reported } => {
                assert_eq!(field, "incarnation");
                assert!(deployed.contains(&dep.incarnation.as_str()) && reported.contains(&wrong_incarnation.node.incarnation.as_str()), "{mismatch:?}");
            }
            other => panic!("{other:?}"),
        }
        let mut wrong_node = digest_of(&dep, "127.0.0.1:34567");
        wrong_node.node.node_id = NodeId::mint();
        let unknown = service.serve(peer.clone(), ask(&wrong_node)).await;
        assert_eq!(unknown, JoinReply::NotPrimary { primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) });
        assert!(installed.lock().unwrap().is_empty(), "a refused join installs nothing");
        assert!(reported.borrow().is_none(), "a refused join completes no WaitForBind");

        let good = digest_of(&dep, "127.0.0.1:34567");
        let joined = service.serve(peer.clone(), ask(&good)).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.digest.node.transport_addr), Some(good.node.transport_addr), "WaitForBind receives the node's own report");
        let forged = service.serve(EndpointId("someone-else".into()), ask(&good)).await;
        assert!(matches!(forged, JoinReply::Joined { .. }), "a join is decided on its digest, never on its caller: {forged:?}");
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

/// The records of an admin, the writer of `row`'s mesh, holding `row` (Q234: a joiner's row is read from
/// them, never the store), over a nodes store of its own.
fn writer_holding(row: rdm_admin::storage::NodeRecord) -> Option<(Arc<dyn rdm_admin::storage::NodesStorage>, Arc<rdm_admin::node_rows::WrittenRows>)> {
    let written = Arc::new(rdm_admin::node_rows::WrittenRows::default());
    written.hold(&row);
    Some((Arc::new(rdm_admin::storage::MemoryNodesStorage::default()), written))
}

/// The `JoinService` of an admin whose node row at `mesh1.node.1` is `row`, its installs logged in `log`.
fn service_over(joins: Arc<Joins>, log: Arc<Mutex<Vec<std::net::SocketAddr>>>, row: rdm_admin::storage::NodeRecord) -> JoinService {
    JoinService {
        nodes: writer_holding(row),
        me_path: "mesh1.node-admin".into(), me_name: "1".into(),
        joins,
        answer: Arc::new(|| {
            Box::pin(async {
                Ok(rdm_admin::wire::JoinAnswer {
                    served_by: NodeId::parse("0123456789ab").unwrap(),
                    control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), member_cert_dates: None, signing_cert_dates: None, name: None },
                    statuses: vec![],
                })
            })
        }),
        install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
        known: Arc::new(|| Box::pin(async {})),
        pending: Arc::new(|_, _| Box::pin(async {})),
        primary: Arc::new(|| Some(rdm_core::NodeId::parse("0123456789ab").unwrap())),
        atlas: None,
        issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
            let t = rdm_transport::clock::RafkaTime::unadopted();
            t.adopt(1_000);
            t
        })),
    }
}

/// CONTRACT (R-JN1 items 2 and 5; node-start.md step 2): a held birth's start registered by an
/// admin that holds no stopped digest of it (it joined after the stop: no last accepted name) is
/// still a held birth: its join reads the row at its name and hands the row's name in the answer; the
/// node's properties are its own record (node-config-update.md). Whether a join is reconciled to its row is what registered the deployment says
/// (`Deployed::held`), never inferred from a last accepted name.
#[test]
fn a_held_births_join_with_no_last_accepted_name_is_handed_its_rows_name() {
    let cell = "a_held_births_join_with_no_last_accepted_name_is_handed_its_rows_name";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = Deployed { held: true, ..deployed() };
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), &dep.r#type, dep.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Leaving);
        let service = service_over(joins.clone(), installed.clone(), row);
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34569");
        let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("{joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.name.as_ref(), Some(&dep.location()), "its row's name: the answer carries no config, the node's properties are its own record");
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!(join["attributes"]["outcome"].as_str(), Some("installed"), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (R-JN1 items 1, 2 and 4; node-start.md step 2): the real admin `JoinService` holds a birth stopped
/// as `mesh1.node.1` whose row its mesh-primary renamed `(mesh1.node, 1-old)` while it was stopped. A join
/// of that exact birth naming `mesh1.node.7`, neither the row's name nor the name the authority last
/// accepted, is refused `JoinMismatch("name")` naming all three, installs nothing and completes no
/// report (`via-join-mismatch`, field `name`). The join naming `mesh1.node.1`, the last accepted name,
/// is admitted under the row's: the installed digest and the answer name `(mesh1.node, 1-old)`. Not a
/// scenario cell: a real node presents the name it holds, which is the name its authority last
/// accepted (it publishes only that name, and `node.update` of a stopped node tells it nothing), so no
/// sequence of operations makes a real node present a third name.
#[test]
fn a_held_births_join_naming_neither_its_rows_nor_its_last_accepted_name_is_refused_naming_all_three() {
    let cell = "a_held_births_join_naming_neither_its_rows_nor_its_last_accepted_name_is_refused_naming_all_three";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = Deployed { path: "mesh1.node".into(), name: "1-old".into(), last_accepted_name: Some(rdm_core::PathName::new("mesh1.node", "1").unwrap()), held: true, ..deployed() };
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), "node", dep.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Leaving);
        let service = service_over(joins.clone(), installed.clone(), row);
        let reported = joins.expect(dep.clone());
        let mut third = digest_of(&dep, "127.0.0.1:34570");
        { let at = rdm_core::PathName::new("mesh1.node", "7").unwrap(); third.node.path = at.path; third.node.name = at.name; }
        let refused = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&third).into(), listeners: Vec::new() }).await;
        match &refused {
            JoinReply::JoinMismatch { field, deployed, reported } => {
                assert_eq!(field, "name", "{refused:?}");
                assert!(deployed.contains("\"1-old\"") && deployed.contains("last accepted") && deployed.contains("\"1\"") && reported.contains("\"7\""), "the refusal names the row's, the last accepted and the reported name: {refused:?}");
            }
            other => panic!("a third name is refused by name: {other:?}"),
        }
        assert!(installed.lock().unwrap().is_empty(), "a refused join installs nothing");
        assert!(reported.borrow().is_none(), "a refused join completes no report");
        let mut held = digest_of(&dep, "127.0.0.1:34570");
        { let at = rdm_core::PathName::new("mesh1.node", "1").unwrap(); held.node.path = at.path; held.node.name = at.name; }
        let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&held).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("the last accepted name is admitted: {joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.name.as_ref(), Some(&dep.location()), "the answer hands the row's name");
        assert_eq!(reported.borrow().as_ref().map(|d| d.digest.node.name.clone()), Some(dep.name.clone()), "the digest installed is canonicalized to the row's name");
        json!({ "cell": cell, "refused": format!("{refused:?}"), "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let refusal = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").expect("the refusal is a span of its own");
    assert_eq!((refusal["attributes"]["field"].as_str(), refusal["attributes"]["node_path"].as_str(), refusal["attributes"]["node_name"].as_str()), (Some("name"), Some("mesh1.node"), Some("7")), "{refusal}");
    let admitted = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join" && s["attributes"]["outcome"] == "installed").expect("the held name's join is installed");
    assert_eq!((admitted["attributes"]["node_path"].as_str(), admitted["attributes"]["node_name"].as_str(), admitted["attributes"]["admitted_path"].as_str(), admitted["attributes"]["admitted_name"].as_str()), (Some("mesh1.node"), Some("1"), Some("mesh1.node"), Some("1-old")), "{admitted}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (R-JN1 item 6; node-create.md step 4; node-recovery.md steps 3-4): a first start (a
/// birth this authority does not hold: no last accepted name) is admitted under its create spec's
/// name whatever row stands at that `(path, name)`. A recovery creates the new node at the exited
/// node's `(path, name)`; where another node's row stands there at the join (a create that wrote no
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
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), &dep.r#type, predecessor.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Deleted);
        let service = JoinService {
            nodes: writer_holding(row),
            me_path: "mesh1.node-admin".into(), me_name: "1".into(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rdm_admin::wire::JoinAnswer {
                        served_by: NodeId::parse("0123456789ab").unwrap(),
                        control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), member_cert_dates: None, signing_cert_dates: None, name: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| Some(rdm_core::NodeId::parse("0123456789ab").unwrap())),
            atlas: None,
            issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34568");
        let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("a first start is admitted under its create spec's name over its predecessor's row: {joined:?}") };
        rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.digest.node.node_id.clone()), Some(dep.node_id.clone()), "WaitForBind receives the new node's own report");
        json!({ "cell": cell, "joined": format!("{joined:?}"), "predecessor": predecessor.as_str() })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!((join["attributes"]["outcome"].as_str(), join["attributes"]["admitted_path"].as_str(), join["attributes"]["admitted_name"].as_str()), (Some("installed"), Some("mesh1.node"), Some("1")), "{join}");
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").count(), 0, "no refusal names the predecessor's row");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (node-recovery.md step 4: "Its config is seeded from the Deleted row at that
/// path.name"; node-create.md step 1: the claim is written before anything is launched;
/// node-config-update.md "Launch properties initialize an absent own record"): a first start whose row
/// at its create spec's name is its own (the create's claim) is installed once; its properties are its
/// own record, initialized from its launch snapshot. No digest carries the
/// config (Q203).
#[test]
fn a_first_start_whose_row_its_own_create_wrote_is_installed_once() {
    let cell = "a_first_start_whose_row_its_own_create_wrote_is_installed_once";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = deployed();
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<String>>> = Arc::default();
        let log = installed.clone();
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), &dep.r#type, dep.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Pending);
        let mut service = service_over(joins.clone(), Arc::default(), row);
        service.install = Arc::new(move |d| log.lock().unwrap().push(d.node.node_id.to_string()));
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34570");
        let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("{joined:?}") };
        rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(*installed.lock().unwrap(), vec![dep.node_id.to_string()], "the birth is installed once");
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!(join["attributes"]["outcome"].as_str(), Some("installed"), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (node-mesh-join.md step 10: the node's mesh-primary "records and publishes the node once
/// as Pending when it accepts the node's join report, before it answers it. Every later report of
/// that join, its retries and its `NodeJoined` included, belongs to that Pending and starts no
/// other"): the `JoinService` hands every accepted join report, under the row's name it admits it as, to its
/// status authority's Pending before it answers `Joined`, a retry of the same report included (the
/// authority applies that one as already applied); a refused report reaches no Pending. Must NOT
/// happen: the answer built before the Pending, or a refused join recording one.
#[test]
fn the_join_service_records_every_accepted_join_reports_pending_before_it_answers_and_no_refused_ones() {
    let cell = "the_join_service_records_every_accepted_join_reports_pending_before_it_answers_and_no_refused_ones";
    let cap = capture(cell);
    let dep = Deployed { path: "mesh1.node".into(), name: "1-old".into(), last_accepted_name: Some(rdm_core::PathName::new("mesh1.node", "1").unwrap()), held: true, ..deployed() };
    cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), "node", dep.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Leaving);
        let mut service = service_over(joins.clone(), installed.clone(), row);
        let order: Arc<Mutex<Vec<String>>> = Arc::default();
        service.pending = {
            let order = order.clone();
            Arc::new(move |d, _| {
                order.lock().unwrap().push(format!("pending {} {}", d.node.node_id, d.node.incarnation.as_str()));
                Box::pin(async {})
            })
        };
        let answer = service.answer.clone();
        service.answer = {
            let order = order.clone();
            Arc::new(move || {
                order.lock().unwrap().push("answer".into());
                answer()
            })
        };
        let _reported = joins.expect(dep.clone());
        let mut third = digest_of(&dep, "127.0.0.1:34571");
        { let at = rdm_core::PathName::new("mesh1.node", "7").unwrap(); third.node.path = at.path; third.node.name = at.name; }
        let refused = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&third).into(), listeners: Vec::new() }).await;
        assert!(matches!(refused, JoinReply::JoinMismatch { .. }), "{refused:?}");
        assert!(order.lock().unwrap().is_empty(), "a refused report records no Pending: {:?}", order.lock().unwrap());
        let mut report = digest_of(&dep, "127.0.0.1:34571");
        { let at = rdm_core::PathName::new("mesh1.node", "1").unwrap(); report.node.path = at.path; report.node.name = at.name; }
        for _ in 0..2 {
            let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&report).into(), listeners: Vec::new() }).await;
            assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");
        }
        let pending = format!("pending {} {}", dep.node_id, dep.incarnation.as_str());
        assert_eq!(*order.lock().unwrap(), vec![pending.clone(), "answer".to_string(), pending, "answer".to_string()], "each accepted report's Pending, under the row's name, precedes its answer");
    });
}

/// CONTRACT (Q151; node-create.md step 4: the accepting primary "refuses a mismatch by name,
/// installing nothing and releasing no wait"): a birth assigned the listeners `kafka`, `rest` and
/// `proxy` on 127.0.0.1, port 0, reports exactly those names, each on its assigned address with the
/// port the operating system selected, or its report is refused whole as `JoinMismatch("listeners")`:
/// a listener missing, one reported twice, one never assigned, one on another address, one on an
/// unspecified address and one on port 0 each install nothing and complete no `WaitForBind`. Nothing
/// is dropped: the report that carries exactly the three is admitted, and its listeners, ports and
/// all, are what `WaitForBind` receives. Must NOT happen: a report with a bad listener admitted, with
/// that listener dropped or kept.
#[test]
fn a_join_report_whose_listeners_are_not_exactly_the_assigned_is_refused_by_name_and_installs_nothing() {
    let cell = "a_join_report_whose_listeners_are_not_exactly_the_assigned_is_refused_by_name_and_installs_nothing";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let assigned: Vec<(String, std::net::SocketAddr)> = ["kafka", "rest", "proxy"].iter().map(|n| (n.to_string(), "127.0.0.1:0".parse().unwrap())).collect();
    let dep = Deployed { r#type: "gateway".into(), path: "mesh1.gateway".into(), name: "1".into(), listeners: assigned, ..deployed() };
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), "gateway", dep.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Pending);
        let service = service_over(joins.clone(), installed.clone(), row);
        let reported = joins.expect(dep.clone());
        let digest = digest_of(&dep, "127.0.0.1:34580");
        let l = |n: &str, a: &str| (n.to_string(), a.parse::<std::net::SocketAddr>().unwrap());
        let good = vec![l("kafka", "127.0.0.1:41001"), l("rest", "127.0.0.1:41002"), l("proxy", "127.0.0.1:41003")];
        let mut refusals = Vec::new();
        for (case, listeners) in [
            ("missing", vec![l("kafka", "127.0.0.1:41001"), l("rest", "127.0.0.1:41002")]),
            ("duplicated", vec![l("kafka", "127.0.0.1:41001"), l("rest", "127.0.0.1:41002"), l("proxy", "127.0.0.1:41003"), l("proxy", "127.0.0.1:41004")]),
            ("undeclared", vec![l("kafka", "127.0.0.1:41001"), l("rest", "127.0.0.1:41002"), l("proxy", "127.0.0.1:41003"), l("admin", "127.0.0.1:41004")]),
            ("another address", vec![l("kafka", "127.0.0.1:41001"), l("rest", "127.0.0.2:41002"), l("proxy", "127.0.0.1:41003")]),
            ("unspecified address", vec![l("kafka", "127.0.0.1:41001"), l("rest", "0.0.0.0:41002"), l("proxy", "127.0.0.1:41003")]),
            ("port 0", vec![l("kafka", "127.0.0.1:41001"), l("rest", "127.0.0.1:0"), l("proxy", "127.0.0.1:41003")]),
        ] {
            let refused = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&digest).into(), listeners }).await;
            match &refused {
                JoinReply::JoinMismatch { field, deployed, reported } => {
                    assert_eq!(field, "listeners", "{case}: the refusal names the listeners: {refused:?}");
                    assert!(deployed.contains("kafka=127.0.0.1:0") && deployed.contains("rest=127.0.0.1:0") && deployed.contains("proxy=127.0.0.1:0"), "{case}: it names what was assigned: {deployed}");
                    refusals.push(json!({ "case": case, "reported": reported }));
                }
                other => panic!("{case}: a report whose listeners are not exactly the assigned is refused by name, not {other:?}"),
            }
            assert!(installed.lock().unwrap().is_empty(), "{case}: nothing is installed");
            assert!(reported.borrow().is_none(), "{case}: no WaitForBind is completed");
        }
        let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&digest).into(), listeners: good.clone() }).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "the report carrying exactly the assigned listeners is admitted: {joined:?}");
        assert_eq!(reported.borrow().as_ref().map(|r| r.listeners.clone()), Some(good), "WaitForBind receives every reported listener with its port: nothing is dropped");
        json!({ "cell": cell, "refusals": refusals })
    });
    let spans = cap.spans();
    let refusals: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").collect();
    assert_eq!(refusals.len(), 6, "each refusal is a span of its own");
    assert!(refusals.iter().all(|s| s["attributes"]["field"] == "listeners"), "{refusals:?}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (Q227: no node reads the store on create, start or rejoin; Q234: what a node is handed
/// about its own row comes from its mesh-primary, never the store; query.md §1: never an atlas copy):
/// the admin that deployed a birth holds its row as its mesh's writer (every row it claims or writes),
/// and the fabric's nodes store ends in an object another writer has not finished (an unended final
/// line). The birth's join is admitted from the writer's records, handed no config. Must NOT happen:
/// the join refused `not-ready` because the store could not be read (`via-join`, outcome `not-ready`).
#[test]
fn a_join_is_admitted_from_the_writers_records_with_the_store_unreadable() {
    let cell = "a_join_is_admitted_from_the_writers_records_with_the_store_unreadable";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = Deployed { held: true, ..deployed() };
    let result = cap.run(async {
        // The fabric's nodes store, holding the row, its tail then unended.
        let store = std::env::temp_dir().join(format!("join-atlas-{}", NodeId::mint()));
        std::fs::create_dir_all(&store).unwrap();
        let writer = rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &NodeId::mint(), &IncarnationId::mint(), &rdm_transport::clock::RafkaTime::unadopted());
        let storage: Arc<dyn rdm_admin::storage::NodesStorage> = Arc::new(rdm_admin::storage::FileNodesStorage::open(&store, &rdm_admin::store_root::StoreRoot::at(&store), "mesh1", &writer).await.unwrap());
        let row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(dep.path.clone(), dep.name.clone()).unwrap(), &dep.r#type, dep.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Leaving);
        let written = Arc::new(rdm_admin::node_rows::WrittenRows::default());
        written.put(&*storage, &row).await.unwrap();
        let file = std::fs::read_dir(&store).unwrap().map(|e| e.unwrap().path()).find(|p| p.file_name().is_some_and(|n| n == "nodes.jsonl")).expect("the nodes store's file");
        use std::io::Write as _;
        std::fs::OpenOptions::new().append(true).open(&file).unwrap().write_all(br#"{"v":1,"kind":"record","key":"unended","record":{"name":"mesh1.node.9""#).unwrap();

        let joins = Arc::new(Joins::default());
        let mut service = service_over(joins.clone(), Arc::default(), row);
        // Wired as the admin's boot wires it: the records its writer holds.
        service.nodes = Some((storage.clone(), written.clone()));
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34571");
        let joined = service.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("the join was refused with the store unreadable: {joined:?}") };
        rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        let _ = std::fs::remove_dir_all(&store);
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!(join["attributes"]["outcome"].as_str(), Some("installed"), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
