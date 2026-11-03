//! The expected election outcome, computed from a public view alone (`node.get`). Every role, a
//! node-admin type's (the mesh primary), the fabric primary's and every other node type's (rpc,
//! gateway, ...), is held by its primary until that primary yields it or its exact birth is proven gone
//! (docs/architecture/fabric-node-lifecycle-elections.md §5: "For every role: a living primary
//! keeps its role; a lower-NodeId candidate that later becomes Ready never displaces it"; "a restarted
//! node is a candidate for a later vacancy and never displaces the primary"), so a view alone cannot
//! name a holder: `roles_as_expected` checks that exactly one primary is advertised per node type with a
//! ready member and that the fabric primary is a mesh primary, and `primaries_kept` checks that a holder
//! still present and live in a later view still holds its role. [`expected_primaries`] is the role a
//! vacancy goes to: the lowest Ready NodeId of the node type.

use serde_json::Value;
use std::collections::BTreeMap;

/// `(mesh, kind)`.
pub type NodeType = (String, String);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The lowest ready NodeId of every node type that has a ready member: the node a vacancy of
/// the node type's role goes to (elections §5: "a primary that yields or whose exact birth is proven gone
/// hands the role to the lowest Ready NodeId of its own mesh"), never one that displaces a
/// living primary.
pub fn expected_primaries(nodes: &[Value]) -> BTreeMap<NodeType, String> {
    let mut best: BTreeMap<NodeType, String> = BTreeMap::new();
    for n in nodes.iter().filter(|n| n["status"] == "ready") {
        let id = s(&n["node_id"]);
        let best_id = best.entry((s(&n["mesh"]), s(&n["type"]))).or_insert_with(|| id.clone());
        if id < *best_id { *best_id = id; }
    }
    best
}

/// The primaries a view advertises, per node type.
pub fn advertised_primaries(nodes: &[Value]) -> BTreeMap<NodeType, Vec<String>> {
    let mut out: BTreeMap<NodeType, Vec<String>> = BTreeMap::new();
    for n in nodes {
        let e = out.entry((s(&n["mesh"]), s(&n["type"]))).or_default();
        if n["is_primary"] == true {
            e.push(s(&n["node_id"]));
        }
    }
    out
}

/// The fabric primaries a view advertises.
pub fn advertised_fabric_primaries(nodes: &[Value]) -> Vec<String> {
    nodes.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["node_id"])).collect()
}

/// Whether the view advertises the node IDs the rules allow: every node type with a ready member
/// advertises exactly one primary, a member of the node type (its primary, whichever it is: a view alone
/// cannot name it, elections §5), no node type advertises two, and exactly one fabric primary is
/// advertised, a node-admin that is also its mesh's primary.
pub fn primaries_as_expected(nodes: &[Value]) -> Result<(), String> {
    for (c, p) in &advertised_primaries(nodes) {
        let ready = nodes.iter().any(|n| s(&n["mesh"]) == c.0 && s(&n["type"]) == c.1 && n["status"] == "ready");
        if (ready && p.len() != 1) || p.len() > 1 {
            return Err(format!("node type {c:?}: advertised {p:?}, expected exactly one primary"));
        }
    }
    let fp = advertised_fabric_primaries(nodes);
    let holds_mesh_role = |node_id: &String| nodes.iter().any(|n| &s(&n["node_id"]) == node_id && n["type"] == "node-admin" && n["is_primary"] == true);
    if fp.len() != 1 || !fp.iter().all(holds_mesh_role) {
        return Err(format!("fabric primary: advertised {fp:?}, expected exactly one that is its mesh's primary"));
    }
    Ok(())
}

/// Whether every role `before` advertised for a birth that `after` still shows live is still held
/// by that birth, for every node type and the fabric-primary role: a primary that lives keeps its role, and a
/// lower NodeId, a newcomer or a restarted node never displaces it (elections §5).
pub fn primaries_kept(before: &[Value], after: &[Value]) -> Result<(), String> {
    for b in before.iter().filter(|n| n["is_primary"] == true || n["is_fabric_primary"] == true) {
        let Some(a) = after.iter().find(|n| n["node_id"] == b["node_id"] && n["incarnation_id"] == b["incarnation_id"]) else { continue };
        if a["status"] == "ready" || a["status"] == "pending-reconnect" {
            for role in ["is_primary", "is_fabric_primary"] {
                if b[role] == true && a[role] != true {
                    return Err(format!("{} lost {role} while it lives ({})", s(&b["node_id"]), s(&a["status"])));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // @feature: mesh-elections
    /// CONTRACT: a rename preserves the primary's exact birth; another node at the same location
    /// does not inherit it, even when its incarnation value is equal. Primary lists contain IDs.
    #[test]
    fn primary_identity_survives_rename_and_distinguishes_a_location_successor() {
        let id = rdm_core::NodeId::mint();
        let incarnation = rdm_core::IncarnationId::mint();
        let original = json!({"node_id": id, "incarnation_id": incarnation,
            "name": {"path": "mesh1.node-admin", "name": "north"}, "mesh": "mesh1", "type": "node-admin",
            "status": "ready", "is_primary": true, "is_fabric_primary": true});
        let before = vec![original.clone()];
        assert_eq!(advertised_fabric_primaries(&before), vec![id.to_string()]);
        assert_eq!(expected_primaries(&before).get(&("mesh1".into(), "node-admin".into())), Some(&id.to_string()));
        let mut renamed = original.clone();
        renamed["name"]["name"] = json!("north-old");
        assert!(primaries_kept(&before, &[renamed.clone()]).is_ok());
        renamed["is_primary"] = json!(false);
        assert!(primaries_kept(&before, &[renamed]).is_err());
        let mut successor = original;
        successor["node_id"] = json!(rdm_core::NodeId::mint());
        successor["is_primary"] = json!(false);
        successor["is_fabric_primary"] = json!(false);
        assert!(primaries_kept(&before, &[successor]).is_ok());
    }
}
