//! The replica's topics projection on the demo's own node processes (Q295; gossip.md §8: node-admin
//! holds every channel as the app; "Entry ends with a pull"; fabric-node-hooks.md `pre_node_ready`):
//! the broker announces every topic it takes a record for on the `demo.topic` channel; the node-admin
//! and each gateway hold what they hear; a gateway born later hydrates the node-admin's topics through
//! its `pre_node_ready` before it reports Ready, and what is announced after arrives live.
//!
//! CONTRACT: topics produced through a gateway are held by that gateway and by the node-admin; a
//! gateway born afterwards holds them the moment it is Ready, with nothing produced since (only its
//! entry's pull could have brought them); a topic produced after that reaches both gateways live.
//! Must NOT happen: a gateway Ready without the topics announced before its birth.

mod common;

use rdm_client::{FabricDesired, MeshDesired};
use rdm_core::NodeKind;

#[test]
fn a_gateway_born_later_holds_every_topic_at_ready_and_hears_the_next_live() {
    common::run(async {
        let test = "a_gateway_born_later_holds_every_topic_at_ready_and_hears_the_next_live";
        let estate = common::estate("demo-projections", test, &[(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 1), (NodeKind::Broker, 1)]).await;
        common::produce(&estate, "mesh1.gateway.1", "p-orders", "orders", "order 1").await;
        common::produce(&estate, "mesh1.gateway.1", "p-payments", "payments", "payment 1").await;
        common::until_topics(&estate, "mesh1.gateway.1", &["orders", "payments"]).await;
        common::until_topics(&estate, "mesh1.admin.1", &["orders", "payments"]).await;

        // A gateway born now: Ready only after its entry pulled the node-admin's topics.
        estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 2), (NodeKind::Broker, 1)])] }).await;
        common::ready(&estate, "mesh1.gateway.2").await;
        let at_ready = common::topics(&estate, "mesh1.gateway.2").await;
        assert_eq!(at_ready, vec!["orders".to_string(), "payments".to_string()], "the gateway born later holds every topic at Ready, from its entry's pull");

        // A topic produced now reaches both gateways live.
        common::produce(&estate, "mesh1.gateway.1", "p-shipments", "shipments", "shipment 1").await;
        for gateway in ["mesh1.gateway.1", "mesh1.gateway.2"] {
            common::until_topics(&estate, gateway, &["orders", "payments", "shipments"]).await;
        }
        estate.shutdown().await;
    });
}
