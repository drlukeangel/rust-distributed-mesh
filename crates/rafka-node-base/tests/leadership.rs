//! e11.s4 — a product reads a cohort's seat from node-admin's view and never computes it.

use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind};
use rafka_node_admin_client::{NodeStatus, NodeView};
use rafka_node_base::leadership::{fabric_primary, primary_of};

fn row(name: &str, kind: NodeKind, primary: bool, fabric: bool) -> NodeView {
    NodeView {
        name: name.parse().unwrap(),
        kind,
        mesh: "mesh1".into(),
        node_id: NodeId::mint(),
        endpoint_id: None,
        incarnation_id: Some(IncarnationId::mint()),
        deployment_id: None,
        provider: None,
        data_dir: None,
        status: NodeStatus::ReadyForTraffic,
        is_primary: primary,
        is_fabric_primary: fabric,
        admin_api_base: None,
        transport_addr: None,
        listeners: vec![],
        declared: None,
        load: None,
        gossip: None,
    }
}

#[test]
fn the_seat_is_the_one_the_view_advertises_whatever_the_node_ids_order() {
    // The view's seat is read as advertised: the product neither re-elects nor second-guesses it.
    let view = vec![
        row("mesh1.admin.1", NodeKind::NodeAdmin, true, true),
        row("mesh1.broker.1", NodeKind::Broker, false, false),
        row("mesh1.broker.2", NodeKind::Broker, true, false),
        row("mesh1.broker.3", NodeKind::Broker, false, false),
        row("mesh1.gateway.1", NodeKind::Gateway, true, false),
    ];
    assert_eq!(primary_of(&view, "mesh1", NodeKind::Broker).map(|n| n.name.to_string()), Some("mesh1.broker.2".into()));
    assert_eq!(primary_of(&view, "mesh1", NodeKind::Gateway).map(|n| n.name.to_string()), Some("mesh1.gateway.1".into()));
    assert_eq!(primary_of(&view, "mesh1", NodeKind::Compute), None, "a cohort the view holds no seat for");
    assert_eq!(primary_of(&view, "mesh2", NodeKind::Broker), None, "another mesh's cohort");
    assert_eq!(fabric_primary(&view).map(|n| n.name.to_string()), Some("mesh1.admin.1".into()));
}

#[test]
fn an_election_in_flight_is_no_seat_not_a_guess() {
    let view = vec![row("mesh1.broker.1", NodeKind::Broker, false, false), row("mesh1.broker.2", NodeKind::Broker, false, false)];
    assert_eq!(primary_of(&view, "mesh1", NodeKind::Broker), None);
}

/// The product base holds no election: no NodeId ordering, no seat computation, anywhere in it.
#[test]
fn node_base_holds_no_comparator() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    for entry in std::fs::read_dir(&src).unwrap().flatten() {
        let text = std::fs::read_to_string(entry.path()).unwrap();
        for (i, line) in text.lines().enumerate() {
            let l = line.trim_start();
            if l.starts_with("//") {
                continue;
            }
            for token in ["min_by", "max_by", ".cmp(", "fn elect", "is_primary =", "is_fabric_primary ="] {
                if line.contains(token) {
                    hits.push(format!("{}:{}: {token}: {}", entry.path().display(), i + 1, line.trim()));
                }
            }
        }
    }
    assert!(hits.is_empty(), "node-base computes a seat:\n{}", hits.join("\n"));
}
