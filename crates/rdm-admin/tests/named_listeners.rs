//! Functional cell for `named-listeners` (`docs/architecture/node-rpc-envelope.md:1033`).

use rdm_admin::deployment::endpoint::ListenerNames;
use rdm_admin::join::{Deployed, JoinDoor, Joins};
use rdm_admin::model::{
    EndpointId, FabricId, IncarnationId, NodeId, NodeKind, PathName, ProviderKind,
};
use rdm_admin::node_rows::WrittenRows;
use rdm_admin::storage::{MemoryNodesStorage, NodesStorage};
use rdm_admin::wire::{JoinAnswer, JoinControl};
use rdm_core::join::{JoinReply, JoinRequest};
use rdm_core::launch::DeclaredListeners;
use rdm_core::{MemberStatus, MeshDigest, MeshNode, RuntimeFact};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    name: String,
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

    fn all(&self) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let spans = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(spans.iter().map(|s| s.name.as_ref()));
        spans
            .into_iter()
            .map(|s| Captured {
                name: s.name.to_string(),
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

// @feature: named-listeners
/// CONTRACT: docs/architecture/node-rpc-envelope.md:1033
/// "`listeners` is each listener the application's wiring declares on the birth's kind, by name, at
///  the address the birth bound: the IP address node-admin assigned and a nonzero port the operating
///  system chose (Q151). The serving node-admin validates the whole report before it installs any
///  part of the join, and refuses it by name, installing nothing, unless it carries exactly the
///  declared names, each once, on its assigned IP address with a nonzero OS-selected port; it writes
///  them on the birth's row."
#[test]
fn join_door_refuses_mismatched_listener_reports_and_publishes_verified_listeners_on_the_birth_row(
) {
    let capture = Capture::new("named-listeners-functional");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    rt.block_on(async {
        let registry = ListenerNames::read(&[DeclaredListeners {
            kind: NodeKind::Gateway,
            names: vec!["kafka".into(), "rest".into()],
        }])
        .expect("valid listener declaration");
        let spec = registry.spec_for(NodeKind::Gateway);
        assert_eq!(spec.listeners, vec!["kafka".to_string(), "rest".to_string()]);

        let assigned_listeners: Vec<(String, SocketAddr)> = spec
            .listeners
            .iter()
            .map(|n| (n.clone(), "127.0.0.1:0".parse().expect("addr")))
            .collect();

        let dep = Deployed {
            name: PathName::new("mesh1", NodeKind::Gateway, 1),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            endpoint_id: EndpointId("gateway-endpoint-key".into()),
            runtime: RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-listeners"))
                .expect("runtime fact"),
            data_dir: "/data/mesh1.gateway.1".into(),
            last_accepted_name: None,
            held: false,
            listeners: assigned_listeners,
        };

        let digest = MeshDigest {
            held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: MeshNode {
                node_id: dep.node_id.clone(),
                name: dep.name.clone(),
                endpoint_id: dep.endpoint_id.clone(),
                transport_addr: "127.0.0.1:41000".parse().expect("addr"),
                incarnation: dep.incarnation.clone(),
                supersedes: None,
                runtime: Some(dep.runtime.clone()),
            },
            status: MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(dep.data_dir.clone()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };

        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<SocketAddr>>> = Arc::default();
        let installed_for_door = installed.clone();
        let nodes_storage: Arc<dyn NodesStorage> = Arc::new(MemoryNodesStorage::default());
        let written_rows = Arc::new(WrittenRows::default());

        let door = JoinDoor {
            nodes: Some((nodes_storage.clone(), written_rows)),
            me: PathName::new("mesh1", NodeKind::NodeAdmin, 1),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: JoinControl {
                            cert_policy: None,
                            provider: ProviderKind::Process,
                            fabric: None,
                            build: None,
                            rafka_time_ms: 1_000,
                            member_cert: Vec::new(),
                            name: None,
                            config: None,
                        },
                        statuses: Vec::new(),
                    })
                })
            }),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(move |d| {
                installed_for_door
                    .lock()
                    .unwrap()
                    .push(d.node.transport_addr)
            }),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.1".into())),
            atlas: None,
            issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs(crate::adopted_time())),
        };

        let watch = joins.expect(dep.clone());

        // 1. Missing listener ("rest" omitted), duplicate listener, undeclared listener, port 0,
        //    unspecified IP, and wrong IP are each refused with JoinMismatch { field: "listeners" },
        //    installing nothing and writing no row.
        let bad_reports: Vec<(Vec<(String, SocketAddr)>, &str)> = vec![
            (
                vec![("kafka".into(), "127.0.0.1:41001".parse().unwrap())],
                "rest is missing",
            ),
            (
                vec![
                    ("kafka".into(), "127.0.0.1:41001".parse().unwrap()),
                    ("kafka".into(), "127.0.0.1:41002".parse().unwrap()),
                ],
                "kafka is reported twice",
            ),
            (
                vec![
                    ("kafka".into(), "127.0.0.1:41001".parse().unwrap()),
                    ("rest".into(), "127.0.0.1:41002".parse().unwrap()),
                    ("extra".into(), "127.0.0.1:41003".parse().unwrap()),
                ],
                "extra was not assigned",
            ),
            (
                vec![
                    ("kafka".into(), "127.0.0.1:0".parse().unwrap()),
                    ("rest".into(), "127.0.0.1:41002".parse().unwrap()),
                ],
                "is not an address a peer can dial",
            ),
            (
                vec![
                    ("kafka".into(), "0.0.0.0:41001".parse().unwrap()),
                    ("rest".into(), "127.0.0.1:41002".parse().unwrap()),
                ],
                "is not an address a peer can dial",
            ),
            (
                vec![
                    ("kafka".into(), "127.0.0.2:41001".parse().unwrap()),
                    ("rest".into(), "127.0.0.1:41002".parse().unwrap()),
                ],
                "is not on its assigned address",
            ),
        ];

        for (listeners, why) in bad_reports {
            let reply = door
                .serve(
                    dep.endpoint_id.clone(),
                    JoinRequest::JoinNode {
                        types: Vec::new(),
                        digest: (&digest).into(),
                        listeners,
                    },
                )
                .await;
            match reply {
                JoinReply::JoinMismatch {
                    field,
                    deployed,
                    reported,
                } => {
                    assert_eq!(field, "listeners");
                    assert_eq!(deployed, "kafka=127.0.0.1:0,rest=127.0.0.1:0");
                    assert!(
                        reported.contains(why),
                        "expected `{why}` in reported `{reported}`"
                    );
                }
                other => panic!("expected JoinMismatch for `{why}`, got {other:?}"),
            }
        }
        assert!(installed.lock().unwrap().is_empty());
        assert!(watch.borrow().is_none());
        assert!(nodes_storage
            .node(&dep.node_id)
            .await
            .expect("read storage")
            .is_none());

        // 2. Valid listener report (each declared listener once on its assigned IP with a nonzero
        //    OS-selected port) is admitted, installs the address, completes WaitForBind with the
        //    listeners, and writes the listeners on the birth's row.
        let good_listeners: Vec<(String, SocketAddr)> = vec![
            ("kafka".into(), "127.0.0.1:41001".parse().unwrap()),
            ("rest".into(), "127.0.0.1:41002".parse().unwrap()),
        ];
        let joined = door
            .serve(
                dep.endpoint_id.clone(),
                JoinRequest::JoinNode {
                    types: Vec::new(),
                    digest: (&digest).into(),
                    listeners: good_listeners.clone(),
                },
            )
            .await;
        assert!(matches!(joined, JoinReply::Joined { .. }));
        assert_eq!(*installed.lock().unwrap(), vec![digest.node.transport_addr]);
        assert_eq!(
            watch.borrow().as_ref().map(|r| r.listeners.clone()),
            Some(good_listeners.clone())
        );

        let row = nodes_storage
            .node(&dep.node_id)
            .await
            .expect("read storage")
            .expect("birth row written");
        assert_eq!(
            row.births.last().map(|b| b.listeners.clone()),
            Some(good_listeners)
        );
    });

    drop(rt);

    let spans = capture.all();
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_admin.node.reject.via-join-mismatch"
                && s.attributes.get("field").map(String::as_str) == Some("listeners")
        }),
        "expected rdm.node_admin.node.reject.via-join-mismatch span naming field=listeners"
    );
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_admin.node.update.via-join"
                && s.attributes.get("outcome").map(String::as_str) == Some("installed")
                && s.attributes.get("listeners").map(String::as_str)
                    == Some("kafka=127.0.0.1:41001,rest=127.0.0.1:41002")
        }),
        "expected rdm.node_admin.node.update.via-join span recording bound listeners"
    );
}
