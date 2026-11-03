//! The demo's data connection, gateway to broker, on the demo's own node processes (Q295, Q296).
//!
//! The estate runs the consumer's executables, nothing of RDM's testkit nodes: `rshape-node-admin` is
//! the Day-0 admin, and the gateways and the broker are `rshape-gateway` and `rshape-broker`
//! processes it creates through a Build. A gateway opens a data connection to the broker on the
//! demo's data protocol (`rshape_consumer::data_alpn`), writes its bytes and reads what the broker's
//! role answers (`rshape_consumer::run`: "echo <broker's node id>: <bytes>"). It does so through its originate
//! door (op `0x73`), which the gateway role serves.
//!
//! CONTRACT (connections.md §4.1, §5 and §7): a gateway's data connection to a broker carries the bytes
//! the gateway wrote, unchanged, and answers them from that broker: as a Direct connection, and, with
//! the gateway's Direct dials to the broker blocked, as a Proxy connection through the other gateway,
//! the demo's carrier kind (`rshape_consumer::carrier_policy`), which carries over its own Direct
//! connection to the broker. The carrier's Direct connection is waited for in its held facts
//! (`node.connection.get`, what a carrier decides on), never its store row. Must NOT happen: bytes
//! changed or answered by another node, or a Proxy through anything but the other gateway.

use rdm_client::binding::{sha256_file, Binding, BindingSet, Candidate};
use rdm_client::{ExactBirth, FabricDesired, MeshDesired, NodeRpc, NodeStatus};
use rdm_core::filter::Filter;
use rdm_core::outcome::RpcOutcome;
use rdm_core::views::ConnectionView;
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_test::estate::{Estate, Owner};
use rdm_test::originate::{Originate, OriginateReply, OriginateRequest};
use rdm_transport::{Resend, CallOptions, NodeTarget};
use std::path::PathBuf;
use std::time::Duration;

fn broker() -> PathName {
    PathName::numbered("mesh1", "broker", 1)
}
fn gateway() -> PathName {
    PathName::numbered("mesh1", "gateway", 1)
}
fn carrier() -> PathName {
    PathName::numbered("mesh1", "gateway", 2)
}
const SETTLE: Duration = Duration::from_secs(120);

/// The candidate the binding set names: the RDM commit the consumer build imported when it says,
/// else this checkout's own label.
fn candidate() -> String {
    std::env::var("RDM_CANDIDATE_SHA").unwrap_or_else(|_| "demo-local".into())
}

/// The demo's own executables, as cargo built them for this test: every launch id the shape uses.
fn binding_set(candidate: &str) -> BindingSet {
    let bind = |launch_id: &str, exe: &str| {
        let executable = PathBuf::from(exe);
        let sha256 = sha256_file(&executable).unwrap_or_else(|e| panic!("the demo's {launch_id} executable {}: {e}", executable.display()));
        Binding { launch_id: launch_id.into(), executable, sha256, image: None }
    };
    let bindings = vec![
        bind("node-admin", env!("CARGO_BIN_EXE_rshape-node-admin")),
        bind("gateway", env!("CARGO_BIN_EXE_rshape-gateway")),
        bind("broker", env!("CARGO_BIN_EXE_rshape-broker")),
    ];
    BindingSet { candidate: Candidate { sha: candidate.into(), build: String::new() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings }
}

fn owner(test: &str) -> Owner {
    Owner { product: "rshape".into(), feature: "demo-data-connection".into(), subfeature: "gateway-to-broker".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

/// The estate: the demo's node-admin, two gateways and one broker in mesh1, every node Ready.
async fn estate(test: &str) -> Estate {
    let candidate = candidate();
    let estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&candidate), &candidate, &["gateway", "broker"])
        .await
        .unwrap_or_else(|e| panic!("the demo's binding set is refused: {e}"));
    estate
        .reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("gateway", 2), ("broker", 1)])] })
        .await;
    for node in [broker(), gateway(), carrier()] {
        estate.until_node(&node, &format!("{node:?} ready"), SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;
    }
    estate
}

/// Ask `from`'s originate door (op `0x73`) for `request`: three attempts, each answered within 30 s.
async fn originate(estate: &Estate, from: &PathName, request: &OriginateRequest) -> OriginateReply {
    let node_id = id_at(estate, from).await;
    let options = CallOptions { resend: Resend::with_time_limit(Duration::from_secs(30)), ..CallOptions::default() };
    let (outcome, _) = estate.carrier().await.client().call::<Originate>(&NodeTarget::ExactNode(node_id), request, &options).await;
    match outcome {
        RpcOutcome::Reply(reply) => reply.into_value(),
        other => panic!("{from:?}'s originate door answered {other:?} to {request:?}"),
    }
}

/// The node id of `node`'s current birth.
async fn id_at(estate: &Estate, node: &PathName) -> NodeId {
    serde_json::from_value(estate.node_value(node).await["node_id"].clone()).expect("a node id")
}

/// `from` writes `payload` on a data connection to the broker; the connection's kind, its far peer
/// and the broker's answer.
async fn send(estate: &Estate, from: &PathName, payload: &str) -> (String, PathName, String) {
    match originate(estate, from, &OriginateRequest::Stream { destination: id_at(estate, &broker()).await, payload: payload.as_bytes().to_vec() }).await {
        OriginateReply::Streamed { kind, peer, answered, .. } => (kind, peer.node, String::from_utf8_lossy(&answered).into_owned()),
        other => panic!("{from:?}'s data connection to {:?} did not carry {payload:?}: {other:?}", broker()),
    }
}

/// The connection to the broker `node` holds now matching `holds`, as its `node.connection.get` answers
/// (the facts it holds, every write handed to its writer landed), awaited for up to 30 s.
async fn held(estate: &Estate, node: &PathName, what: &str, holds: impl Fn(&ConnectionView) -> bool) -> ConnectionView {
    let row = estate.node_value(node).await;
    let destination = id_at(estate, &broker()).await.to_string();
    let node_id: NodeId = serde_json::from_value(row["node_id"].clone()).expect("a node id");
    let incarnation: IncarnationId = serde_json::from_value(row["incarnation_id"].clone()).expect("an incarnation id");
    let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id, incarnation };
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let carrier = estate.carrier().await;
        let read = NodeRpc::new(carrier.client()).connections_get(&birth, &Filter::new(), &CallOptions::default()).await;
        if let Ok(view) = &read {
            if let Some(c) = view.connections.iter().find(|c| c.destination == destination && holds(c)) {
                return c.clone();
            }
        }
        assert!(std::time::Instant::now() < until, "{node:?} does not hold {what} within 30 s: {read:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn a_gateways_data_connection_to_the_broker_carries_its_bytes_direct_and_through_a_gateway_carrier() {
    let test = "a_gateways_data_connection_to_the_broker_carries_its_bytes_direct_and_through_a_gateway_carrier";
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap().block_on(async {
        let estate = estate(test).await;

        let (broker_id, carrier_id) = (id_at(&estate, &broker()).await, id_at(&estate, &carrier()).await);
        // Direct: each gateway's own connection to the broker.
        for gateway in [gateway(), carrier()] {
            let payload = format!("produce from path {:?}, name {:?}: topic-a record-1", gateway.path, gateway.name);
            let (kind, peer, answered) = send(&estate, &gateway, &payload).await;
            assert_eq!((kind.as_str(), peer), ("direct", broker()), "{gateway:?}'s data connection reaches the broker directly");
            assert_eq!(answered, format!("echo {broker_id}: {payload}"), "the broker answers exactly the bytes {gateway:?} wrote");
        }
        // The carrier decides on its own held Direct connection: waited for there.
        held(&estate, &carrier(), "its Direct connection to the broker", |c| c.kind.eq_ignore_ascii_case("direct") && c.state.eq_ignore_ascii_case("connected")).await;

        // Proxy: the gateway's Direct dials to the broker blocked, the same bytes through the other gateway.
        match originate(&estate, &gateway(), &OriginateRequest::BlockDirect { destination: broker_id.clone(), blocked: true }).await {
            OriginateReply::DirectBlocked { .. } => {}
            other => panic!("{:?} did not block its Direct dials to {:?}: {other:?}", gateway(), broker()),
        }
        let payload = "produce: topic-a record-2";
        let (kind, peer, answered) = send(&estate, &gateway(), payload).await;
        assert_eq!((kind, peer), (format!("proxy via {carrier_id}"), broker()), "with its Direct dials blocked, the gateway reaches the broker through the other gateway");
        assert_eq!(answered, format!("echo {broker_id}: {payload}"), "the broker answers exactly the bytes the gateway wrote, through the carrier");
        let proxy = held(&estate, &gateway(), "its Proxy connection to the broker", |c| c.kind.eq_ignore_ascii_case("proxy")).await;
        assert_eq!(proxy.carrier, Some(carrier_id.to_string()), "the carrier is the other gateway: {proxy:?}");

        estate.shutdown().await;
    });
}
