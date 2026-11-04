//! e11.s4 — a product reads a node-type primary role from node-admin's view and never computes it.

use rdm_core::{IncarnationId, NodeId};
use rdm_client::{NodeStatus, NodeView};
use rdm_node::leadership::{fabric_primary, primary_of};

fn row(name: rdm_core::PathName, r#type: &str, primary: bool, fabric: bool) -> NodeView {
    let mut view = NodeView::new(&name, r#type, NodeId::mint(), NodeStatus::Ready);
    view.incarnation = Some(IncarnationId::mint());
    view.is_primary = primary;
    view.is_fabric_primary = fabric;
    view
}

#[test]
fn the_primary_is_the_one_the_view_advertises_whatever_the_node_ids_order() {
    // The view's primary is read as advertised: the product neither re-elects nor second-guesses it.
    let view = vec![
        row(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", true, true),
        row(rdm_core::PathName::new("mesh1.broker", "1").unwrap(), "broker", false, false),
        row(rdm_core::PathName::new("mesh1.broker", "2").unwrap(), "broker", true, false),
        row(rdm_core::PathName::new("mesh1.broker", "3").unwrap(), "broker", false, false),
        row(rdm_core::PathName::new("mesh1.gateway", "1").unwrap(), "gateway", true, false),
    ];
    assert_eq!(primary_of(&view, "mesh1", "broker").map(|n| &n.node_id), Some(&view[2].node_id));
    assert_eq!(primary_of(&view, "mesh1", "gateway").map(|n| &n.node_id), Some(&view[4].node_id));
    assert_eq!(primary_of(&view, "mesh1", "compute"), None, "a node type the view holds no primary for");
    assert_eq!(primary_of(&view, "mesh2", "broker"), None, "another mesh's node type");
    assert_eq!(fabric_primary(&view).map(|n| &n.node_id), Some(&view[0].node_id));
}

#[test]
fn an_election_in_flight_is_no_primary_not_a_guess() {
    let view = vec![row(rdm_core::PathName::new("mesh1.broker", "1").unwrap(), "broker", false, false), row(rdm_core::PathName::new("mesh1.broker", "2").unwrap(), "broker", false, false)];
    assert_eq!(primary_of(&view, "mesh1", "broker"), None);
}

/// The product base holds no election: no NodeId ordering, no primary computation, anywhere in it. A
/// node writing its own record's `is_primary` / `is_fabric_primary` from its PrimaryView (Q356) records
/// the election's result and computes nothing.
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
                // The node recording on its own record the role its PrimaryView names it (Q356,
                // `node_records::write_own_roles`) writes the election's result; it computes no primary.
                let recorded = token.ends_with(" =") && line.contains(&format!("record.{token}"));
                if line.contains(token) && !recorded {
                    hits.push(format!("{}:{}: {token}: {}", path.display(), i + 1, line.trim()));
                }
            }
        }
    }
    assert!(hits.is_empty(), "node-base computes a primary:\n{}", hits.join("\n"));
}
