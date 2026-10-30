//! e11.s4 — a product reads a node-type primary seat from node-admin's view and never computes it.

use rdm_core::{IncarnationId, NodeId, NodeKind};
use rdm_client::{NodeStatus, NodeView};
use rdm_node::leadership::{fabric_primary, primary_of};

fn row(name: &str, kind: NodeKind, primary: bool, fabric: bool) -> NodeView {
    NodeView { cert: None, executable: None, storage_persistence: None, topology_storage_provider: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
        name: name.parse().unwrap(),
        kind,
        mesh: "mesh1".into(),
        node_id: NodeId::mint(),
        endpoint_id: None,
        incarnation_id: Some(IncarnationId::mint()),
        deployment_id: None,
        provider: None,
        data_dir: None,
        status: NodeStatus::Ready,
        is_primary: primary,
        is_fabric_primary: fabric,
        admin_api_base: None,
        transport_addr: None,
        listeners: vec![],
        declared: None,
        stopped: false,
        load: None,
        gossip: None,
        tags: Default::default(),
        config: Default::default(),
        data_dir_lost: None,
        data_dir_unreadable: None,
    }
}

#[test]
fn the_primary_is_the_one_the_view_advertises_whatever_the_node_ids_order() {
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
    assert_eq!(primary_of(&view, "mesh1", NodeKind::Compute), None, "a node type the view holds no seat for");
    assert_eq!(primary_of(&view, "mesh2", NodeKind::Broker), None, "another mesh's node type");
    assert_eq!(fabric_primary(&view).map(|n| n.name.to_string()), Some("mesh1.admin.1".into()));
}

#[test]
fn an_election_in_flight_is_no_primary_not_a_guess() {
    let view = vec![row("mesh1.broker.1", NodeKind::Broker, false, false), row("mesh1.broker.2", NodeKind::Broker, false, false)];
    assert_eq!(primary_of(&view, "mesh1", NodeKind::Broker), None);
}

/// The product base holds no election: no NodeId ordering, no seat computation, anywhere in it.
#[test]
fn node_base_holds_no_comparator() {
    // Every source file under src/, its executables' (src/bin/) included.
    let mut files = Vec::new();
    let mut dirs = vec![std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            if entry.file_type().unwrap().is_dir() {
                dirs.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    assert!(files.iter().any(|f| f.ends_with("workflows/node_start.rs")), "the scan reaches the node executable: {files:?}");
    let mut hits = Vec::new();
    for path in files {
        let text = std::fs::read_to_string(&path).unwrap();
        for (i, line) in text.lines().enumerate() {
            let l = line.trim_start();
            if l.starts_with("//") {
                continue;
            }
            for token in ["min_by", "max_by", ".cmp(", "fn elect", "is_primary =", "is_fabric_primary ="] {
                if line.contains(token) {
                    hits.push(format!("{}:{}: {token}: {}", path.display(), i + 1, line.trim()));
                }
            }
        }
    }
    assert!(hits.is_empty(), "node-base computes a seat:\n{}", hits.join("\n"));
}
