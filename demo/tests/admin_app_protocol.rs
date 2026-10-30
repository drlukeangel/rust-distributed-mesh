//! The consumer's node-admin opens and accepts connections on the shape's data protocol (`demo/echo`,
//! `rshape_consumer::data_alpn`), on the demo's own node processes (Q295), as an embedding's
//! node-admin does with its own app protocols (Rafka's admin to its brokers).
//!
//! CONTRACT (fabric-certs.md §On a connection; node-connections-admit.md): `rshape-node-admin`, the
//! Day-0 admin holding the fabric's root, registers `demo/echo`: a gateway's data connection to it is
//! admitted and answered by the admin's app; and once the fabric is ready, the admin's own data
//! connection to the broker is admitted. Must NOT happen: either dial refused (`no-credential` or any
//! other refusal).

mod common;

use rdm_core::NodeKind;
use rdm_test::originate::{Originate, OriginateReply, OriginateRequest};
use rdm_transport::{Budget, CallOptions, NodeTarget};
use serde_json::Value;
use std::time::Duration;

const ADMIN: &str = "mesh1.admin.1";
const GATEWAY: &str = "mesh1.gateway.1";
const BROKER: &str = "mesh1.broker.1";

#[test]
fn the_consumers_day0_admin_answers_a_gateway_and_reaches_the_broker_on_the_data_protocol() {
    common::run(async {
        let test = "the_consumers_day0_admin_answers_a_gateway_and_reaches_the_broker_on_the_data_protocol";
        let mut estate = common::estate("demo-admin-app-protocol", test, &[(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 1), (NodeKind::Broker, 1)]).await;
        estate.until_fabric_ready(common::SETTLE).await;

        // The gateway's data connection to the admin: admitted, answered by the admin's app.
        let options = CallOptions { budget: Budget::Overall(Duration::from_secs(30)), ..CallOptions::default() };
        let gateway = NodeTarget::ExactNode(common::node_id(&estate, GATEWAY).await);
        let (outcome, _) = estate.carrier().await.client().call::<Originate>(&gateway, &OriginateRequest::Stream { destination: ADMIN.into(), payload: b"from the gateway".to_vec() }, &options).await;
        match outcome {
            rdm_core::outcome::RpcOutcome::Reply(reply) => match reply.into_value() {
                OriginateReply::Streamed { peer, answered, .. } => {
                    assert_eq!(peer.node, ADMIN, "the gateway's data connection reaches the admin");
                    assert_eq!(String::from_utf8_lossy(&answered), format!("echo {ADMIN}: from the gateway"), "the admin's app answers the gateway's bytes");
                }
                other => panic!("the gateway's data connection to the admin failed: {other:?}"),
            },
            other => panic!("the gateway's originate door answered {other:?}"),
        }

        // The admin's own data connection to the broker, made after the fabric is ready: admitted.
        let until = std::time::Instant::now() + common::SETTLE;
        let dial = |spans: &[Value]| {
            spans.iter().any(|s| s["service"] == "rshape-node-admin" && s["name"] == "rdm.node.connection.create.via-dial" && s["attributes"]["destination"] == BROKER && s["attributes"]["outcome"] == "connected")
        };
        while !dial(&estate.spans()) {
            assert!(std::time::Instant::now() < until, "the admin's data connection to {BROKER} was not admitted within {:?}", common::SETTLE);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        estate.stop().await;
    });
}
