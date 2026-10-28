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
use rdm_admin::join::{Deployed, JoinDoor, Joins};
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
    Deployed {
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        endpoint_id: EndpointId("node-key".into()),
        runtime: RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-rj1")).unwrap(),
        data_dir: "/data/mesh1.rpc.1".into(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    }
}

fn digest_of(d: &Deployed, addr: &str) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
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
        let door = JoinDoor {
            nodes: None,
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rdm_admin::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.2".into())),
            atlas: None,
            issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let dep = deployed();
        let reported = joins.expect(dep.clone());
        let ask = |d: &MeshDigest| JoinRequest::JoinNode { types: Vec::new(), digest: d.into(), listeners: Vec::new() };
        let peer = dep.endpoint_id.clone();

        let mut wrong_incarnation = digest_of(&dep, "127.0.0.1:34567");
        wrong_incarnation.node.incarnation = IncarnationId::mint();
        let mismatch = door.serve(peer.clone(), ask(&wrong_incarnation)).await;
        match &mismatch {
            JoinReply::JoinMismatch { field, deployed, reported } => {
                assert_eq!(field, "incarnation");
                assert!(deployed.contains(&dep.incarnation.as_str()) && reported.contains(&wrong_incarnation.node.incarnation.as_str()), "{mismatch:?}");
            }
            other => panic!("{other:?}"),
        }
        let mut wrong_node = digest_of(&dep, "127.0.0.1:34567");
        wrong_node.node.node_id = NodeId::mint();
        let unknown = door.serve(peer.clone(), ask(&wrong_node)).await;
        assert_eq!(unknown, JoinReply::NotPrimary { primary: Some("mesh1.admin.2".into()) });
        assert!(installed.lock().unwrap().is_empty(), "a refused join installs nothing");
        assert!(reported.borrow().is_none(), "a refused join completes no WaitForBind");

        let good = digest_of(&dep, "127.0.0.1:34567");
        let joined = door.serve(peer.clone(), ask(&good)).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.digest.node.transport_addr), Some(good.node.transport_addr), "WaitForBind receives the node's own report");
        let forged = door.serve(EndpointId("someone-else".into()), ask(&good)).await;
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

/// An admin's atlas holding `row` (Q234: a joiner's row is read from it, never the store).
fn atlas_holding(row: rdm_admin::storage::NodeRow) -> Arc<rdm_admin::atlas::Atlas> {
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    atlas.write_record(&row);
    atlas
}

/// The door of an admin whose node row at `mesh1.rpc.1` is `row`, its installs logged in `log`.
fn door_over(joins: Arc<Joins>, log: Arc<Mutex<Vec<std::net::SocketAddr>>>, row: rdm_admin::storage::NodeRow) -> JoinDoor {
    JoinDoor {
        nodes: None,
        me: "mesh1.admin.1".parse().unwrap(),
        joins,
        answer: Arc::new(|| {
            Box::pin(async {
                Ok(rdm_admin::wire::JoinAnswer {
                    served_by: "mesh1.admin.1".into(),
                    control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                    statuses: vec![],
                })
            })
        }),
        types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
        install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
        known: Arc::new(|| Box::pin(async {})),
        pending: Arc::new(|_, _| Box::pin(async {})),
        primary: Arc::new(|| Some("mesh1.admin.2".into())),
        atlas: Some(atlas_holding(row)),
        issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
            let t = rdm_transport::clock::RafkaTime::unadopted();
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
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Leaving, config: [("tier".to_string(), "gold".to_string())].into_iter().collect(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let door = door_over(joins.clone(), installed.clone(), row);
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34569");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("{joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
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

/// CONTRACT (R-JN1 items 1, 2 and 4; node-start.md step 2): the real admin door holds a birth stopped
/// as `mesh1.rpc.1` whose row its mesh-primary renamed `mesh1.rpc.1.old` while it was stopped. A join
/// of that exact birth naming `mesh1.rpc.7`, neither the row's name nor the name the authority last
/// accepted, is refused `JoinMismatch("name")` naming all three, installs nothing and completes no
/// report (`via-join-mismatch`, field `name`). The join naming `mesh1.rpc.1`, the last accepted name,
/// is admitted under the row's: the installed digest and the answer name `mesh1.rpc.1.old`. Not a
/// scenario cell: a real node presents the name it holds, which is the name its authority last
/// accepted (it publishes only that name, and `node.update` of a stopped node tells it nothing), so no
/// sequence of operations makes a real node present a third name.
#[test]
fn a_held_births_join_naming_neither_its_rows_nor_its_last_accepted_name_is_refused_naming_all_three() {
    let cell = "a_held_births_join_naming_neither_its_rows_nor_its_last_accepted_name_is_refused_naming_all_three";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = Deployed { name: "mesh1.rpc.1.old".parse().unwrap(), last_accepted_name: Some("mesh1.rpc.1".parse().unwrap()), held: true, ..deployed() };
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let door = door_over(joins.clone(), installed.clone(), row);
        let reported = joins.expect(dep.clone());
        let mut third = digest_of(&dep, "127.0.0.1:34570");
        third.node.name = "mesh1.rpc.7".parse().unwrap();
        let refused = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&third).into(), listeners: Vec::new() }).await;
        match &refused {
            JoinReply::JoinMismatch { field, deployed, reported } => {
                assert_eq!(field, "name", "{refused:?}");
                assert!(deployed.contains("mesh1.rpc.1.old") && deployed.contains("last accepted") && deployed.contains("\"mesh1.rpc.1\"") && reported.contains("mesh1.rpc.7"), "the refusal names the row's, the last accepted and the reported name: {refused:?}");
            }
            other => panic!("a third name is refused by name: {other:?}"),
        }
        assert!(installed.lock().unwrap().is_empty(), "a refused join installs nothing");
        assert!(reported.borrow().is_none(), "a refused join completes no report");
        let mut held = digest_of(&dep, "127.0.0.1:34570");
        held.node.name = "mesh1.rpc.1".parse().unwrap();
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&held).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("the last accepted name is admitted: {joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.name.as_ref().map(|n| n.to_string()).as_deref(), Some("mesh1.rpc.1.old"), "the answer hands the row's name");
        assert_eq!(reported.borrow().as_ref().map(|d| d.digest.node.name.to_string()).as_deref(), Some("mesh1.rpc.1.old"), "the digest installed is canonicalized to the row's name");
        json!({ "cell": cell, "refused": format!("{refused:?}"), "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let refusal = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").expect("the refusal is a span of its own");
    assert_eq!((refusal["attributes"]["field"].as_str(), refusal["attributes"]["node"].as_str()), (Some("name"), Some("mesh1.rpc.7")), "{refusal}");
    let admitted = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join" && s["attributes"]["outcome"] == "installed").expect("the held name's join is installed");
    assert_eq!((admitted["attributes"]["node"].as_str(), admitted["attributes"]["admitted_as"].as_str()), (Some("mesh1.rpc.1"), Some("mesh1.rpc.1.old")), "{admitted}");
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
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None,
            name: dep.name.clone(),
            node_id: predecessor.clone(),
            status: rdm_admin::model::NodeStatus::Deleted,
            config: [("tier".to_string(), "gold".to_string())].into_iter().collect(), tags: Default::default(), allocated: None,
            renamed_from: None,
            app_types: None, inherited_by: None,
            executable: None,
            births: Vec::new(),
            predecessor: None,
            mesh_id: None,
            storage_persistence: None,
            placement: None, revision: 0,
        };
        let door = JoinDoor {
            nodes: None,
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rdm_admin::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: rdm_admin::wire::JoinControl { cert_policy: None, provider: rdm_admin::model::ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.2".into())),
            atlas: Some(atlas_holding(row)),
            issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34568");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("a first start is admitted under its create spec's name over its predecessor's row: {joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.config, None, "another node's row hands the first start no config: {answer:?}");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.digest.node.node_id.clone()), Some(dep.node_id.clone()), "WaitForBind receives the new node's own report");
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
/// config in its join answer and installed once, so the writer's arrival finds the row its own and
/// tells it nothing (no config call racing the node's start, which a node whose authority is not
/// filled answers NotReady and never applies: gate 7fd59dec, node_replace, via-config
/// config-not-applied). No digest carries the config (Q203).
#[test]
fn a_first_start_whose_row_is_its_own_seeded_claim_is_handed_the_rows_config() {
    let cell = "a_first_start_whose_row_is_its_own_seeded_claim_is_handed_the_rows_config";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = deployed();
    let gold: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into_iter().collect();
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<String>>> = Arc::default();
        let log = installed.clone();
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Pending, config: gold.clone(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let mut door = door_over(joins.clone(), Arc::default(), row);
        door.install = Arc::new(move |d| log.lock().unwrap().push(d.node.name.to_string()));
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34570");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("{joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.config.as_ref(), Some(&gold), "the first start is handed its own row's config: {answer:?}");
        assert_eq!(*installed.lock().unwrap(), vec![dep.name.to_string()], "the birth is installed once");
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!((join["attributes"]["outcome"].as_str(), join["attributes"]["config_keys"].as_str()), (Some("installed"), Some("1")), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// The bound is checked before any address, declaration or WaitForBind report is installed.
#[test]
fn a_join_over_the_types_bound_is_malformed_by_name_and_never_a_member() {
    let cap = capture("app-types-bound");
    cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed = Arc::new(Mutex::new(Vec::new()));
        let dep = deployed();
        let reported = joins.expect(dep.clone());
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Pending, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None, app_types: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let mut door = door_over(joins, installed.clone(), row);
        let declarations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        door.types = Arc::new({let declarations=declarations.clone(); move |_, _| {declarations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);Box::pin(async{Ok(())})}});
        let digest = digest_of(&dep, "127.0.0.1:34567");
        for types in [(0..257).map(|n|format!("app.kind.{n}")).collect(), vec!["x".repeat(8192)]] {
            let answer = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { digest: (&digest).into(), types, listeners: Vec::new() }).await;
            assert!(matches!(answer, JoinReply::Malformed { kind: rdm_core::outcome::MalformedKind::TypesOverCap }), "{answer:?}");
            assert!(reported.borrow().is_none());
            assert!(installed.lock().unwrap().is_empty());
            assert_eq!(declarations.load(std::sync::atomic::Ordering::SeqCst),0);
        }
        let answer=door.serve(dep.endpoint_id, JoinRequest::JoinNode {digest: (&digest).into(),types:(0..256).map(|n|format!("app.kind.{n}")).collect(), listeners: Vec::new() }).await;
        assert!(matches!(answer,JoinReply::Joined{..}),"{answer:?}");
        assert_eq!(declarations.load(std::sync::atomic::Ordering::SeqCst),1);
        assert_eq!(installed.lock().unwrap().len(),1);
        assert!(reported.borrow().is_some());
    });
}

#[test]
fn an_exact_birth_cannot_change_its_declared_types_on_a_repeated_join() {
    let cap=capture("immutable-app-types");
    cap.run(async {
        let dep=deployed();let joins=Arc::new(Joins::default());let reported=joins.expect(dep.clone());
        let installed=Arc::new(Mutex::new(Vec::new()));
        let row=rdm_admin::storage::NodeRow{name:dep.name.clone(),node_id:dep.node_id.clone(),status:rdm_admin::model::NodeStatus::Pending,config:Default::default(),tags:Default::default(),allocated:None,renamed_from:None,inherited_by:None,app_types:Some(rdm_admin::storage::AppTypes{incarnation:dep.incarnation.clone(),types:["app.one".to_string()].into()}),executable:None,births:Vec::new(),predecessor:None,mesh_id:None,storage_persistence:None,topology_storage_provider:None,placement:None, revision: 0};
        let door=door_over(joins,installed.clone(),row);
        let digest=digest_of(&dep,"127.0.0.1:34567");
        let changed=door.serve(dep.endpoint_id.clone(),JoinRequest::JoinNode{digest:(&digest).into(),types:vec!["app.two".into()],listeners:Vec::new()}).await;
        assert!(matches!(changed,JoinReply::JoinMismatch{field,..}if field=="types"));assert!(installed.lock().unwrap().is_empty());assert!(reported.borrow().is_none());
        let same=door.serve(dep.endpoint_id,JoinRequest::JoinNode{digest:(&digest).into(),types:vec!["app.one".into()],listeners:Vec::new()}).await;
        assert!(matches!(same,JoinReply::Joined{..}));assert_eq!(installed.lock().unwrap().len(),1);
    });
}

/// CONTRACT (node-mesh-join.md step 10: the node's mesh-primary "records and publishes the node once
/// as Pending when it accepts the node's join report, before it answers it. Every later report of
/// that join, its retries and its `NodeJoined` included, belongs to that Pending and starts no
/// other"): the door hands every accepted join report, under the row's name it admits it as, to its
/// status authority's Pending before it answers `Joined`, a retry of the same report included (the
/// authority applies that one as already applied); a refused report reaches no Pending. Must NOT
/// happen: the answer built before the Pending, or a refused join recording one.
#[test]
fn the_door_records_every_accepted_join_reports_pending_before_it_answers_and_no_refused_ones() {
    let cell = "the_door_records_every_accepted_join_reports_pending_before_it_answers_and_no_refused_ones";
    let cap = capture(cell);
    let dep = Deployed { name: "mesh1.rpc.1.old".parse().unwrap(), last_accepted_name: Some("mesh1.rpc.1".parse().unwrap()), held: true, ..deployed() };
    cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let mut door = door_over(joins.clone(), installed.clone(), row);
        let order: Arc<Mutex<Vec<String>>> = Arc::default();
        door.pending = {
            let order = order.clone();
            Arc::new(move |d, _| {
                order.lock().unwrap().push(format!("pending {} {}", d.node.name, d.node.incarnation.as_str()));
                Box::pin(async {})
            })
        };
        let answer = door.answer.clone();
        door.answer = {
            let order = order.clone();
            Arc::new(move || {
                order.lock().unwrap().push("answer".into());
                answer()
            })
        };
        let _reported = joins.expect(dep.clone());
        let mut third = digest_of(&dep, "127.0.0.1:34571");
        third.node.name = "mesh1.rpc.7".parse().unwrap();
        let refused = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&third).into(), listeners: Vec::new() }).await;
        assert!(matches!(refused, JoinReply::JoinMismatch { .. }), "{refused:?}");
        assert!(order.lock().unwrap().is_empty(), "a refused report records no Pending: {:?}", order.lock().unwrap());
        let mut report = digest_of(&dep, "127.0.0.1:34571");
        report.node.name = "mesh1.rpc.1".parse().unwrap();
        for _ in 0..2 {
            let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&report).into(), listeners: Vec::new() }).await;
            assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");
        }
        let pending = format!("pending mesh1.rpc.1.old {}", dep.incarnation.as_str());
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
    let dep = Deployed { name: "mesh1.gateway.1".parse().unwrap(), listeners: assigned, ..deployed() };
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Pending, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let door = door_over(joins.clone(), installed.clone(), row);
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
            let refused = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&digest).into(), listeners }).await;
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
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&digest).into(), listeners: good.clone() }).await;
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
/// about its own row comes from its mesh-primary's atlas, never the store): the admin that deployed a
/// birth holds its row in its atlas (its writer feeds every row it claims or writes there), and the
/// fabric's nodes store ends in an object another writer has not finished (an unended final line).
/// The birth's join is admitted and handed its row's config from the atlas. Must NOT happen: the join
/// refused `not-ready` because the store could not be read (`via-join`, outcome `not-ready`).
#[test]
fn a_join_is_handed_its_rows_config_from_the_atlas_with_the_store_unreadable() {
    let cell = "a_join_is_handed_its_rows_config_from_the_atlas_with_the_store_unreadable";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let dep = Deployed { held: true, ..deployed() };
    let gold: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into_iter().collect();
    let result = cap.run(async {
        // The fabric's nodes store, holding the row, its tail then unended.
        let store = std::env::temp_dir().join(format!("join-atlas-{}", NodeId::mint()));
        std::fs::create_dir_all(&store).unwrap();
        let writer = rdm_admin::store_log::LogWriter::new(&NodeId::mint(), &IncarnationId::mint());
        let storage: Arc<dyn rdm_admin::storage::NodesStorage> = Arc::new(rdm_admin::storage::FileNodesStorage::open(&store, &rdm_admin::store_root::StoreRoot::at(&store), "mesh1", &writer).await.unwrap());
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: dep.name.clone(), node_id: dep.node_id.clone(), status: rdm_admin::model::NodeStatus::Leaving, config: gold.clone(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0 };
        let written = Arc::new(rdm_admin::node_rows::WrittenRows::default());
        let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
        written.feeds(atlas.clone());
        written.put(&*storage, &row).await.unwrap();
        let file = std::fs::read_dir(&store).unwrap().map(|e| e.unwrap().path()).find(|p| p.file_name().is_some_and(|n| n == "nodes.jsonl")).expect("the nodes store's file");
        use std::io::Write as _;
        std::fs::OpenOptions::new().append(true).open(&file).unwrap().write_all(br#"{"v":1,"kind":"record","key":"unended","record":{"name":"mesh1.rpc.9""#).unwrap();

        let joins = Arc::new(Joins::default());
        let mut door = door_over(joins.clone(), Arc::default(), row);
        door.nodes = Some((storage.clone(), written.clone()));
        // Wired as the admin's boot wires it: the atlas its writer feeds.
        door.atlas = Some(atlas.clone());
        let _reported = joins.expect(dep.clone());
        let good = digest_of(&dep, "127.0.0.1:34571");
        let joined = door.serve(dep.endpoint_id.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&good).into(), listeners: Vec::new() }).await;
        let JoinReply::Joined { answer } = &joined else { panic!("the join was refused with the store unreadable: {joined:?}") };
        let answer = rdm_admin::wire::answer_from_wire(answer).expect("the answer decodes");
        assert_eq!(answer.control.config.as_ref(), Some(&gold), "the birth is handed its row's config from the atlas: {answer:?}");
        let _ = std::fs::remove_dir_all(&store);
        json!({ "cell": cell, "joined": format!("{joined:?}") })
    });
    let spans = cap.spans();
    let join = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.update.via-join").expect("the join is spanned");
    assert_eq!((join["attributes"]["outcome"].as_str(), join["attributes"]["config_keys"].as_str()), (Some("installed"), Some("1")), "{join}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
