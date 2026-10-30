//! Loss and repair on the replica's topic channel, on the demo's own node processes (Q295; gossip.md
//! §8: live events are best-effort, and a node's entry ends with a pull of what it holds).
//!
//! CONTRACT: a gateway stopped while a topic is announced misses the event; started again, its entry's
//! pull from the node-admin repairs it before it reports Ready. Must NOT happen: the started gateway
//! Ready without the topic announced while it was stopped.

mod common;

use rdm_core::NodeKind;
use rdm_test::estate::Estate;

#[test]
fn a_gateway_stopped_through_an_announcement_holds_the_topic_again_at_its_start() {
    common::run(async {
        let test = "a_gateway_stopped_through_an_announcement_holds_the_topic_again_at_its_start";
        let estate = common::estate("demo-app-channels", test, &[(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 2), (NodeKind::Broker, 1)]).await;
        common::produce(&estate, "mesh1.gateway.1", "p-orders", "orders", "order 1").await;
        common::until_topics(&estate, "mesh1.gateway.2", &["orders"]).await;

        // gateway.2 stopped; a topic announced while it is down.
        let stopped: rdm_core::PathName = "mesh1.gateway.2".parse().unwrap();
        Estate::workflow("node.stop of mesh1.gateway.2", estate.nodes_obj().await.stop(&stopped).await).await;
        common::produce(&estate, "mesh1.gateway.1", "p-refunds", "refunds", "refund 1").await;
        common::until_topics(&estate, "mesh1.admin.1", &["orders", "refunds"]).await;

        // Started again: Ready only after its entry pulled the node-admin's topics.
        Estate::workflow("node.start of mesh1.gateway.2", estate.nodes_obj().await.start(&stopped).await).await;
        common::ready(&estate, "mesh1.gateway.2").await;
        let held = common::topics(&estate, "mesh1.gateway.2").await;
        assert!(held.iter().any(|t| t == "refunds"), "the started gateway holds the topic announced while it was stopped, from its entry's pull: {held:?}");
        estate.shutdown().await;
    });
}
