//! The expected election outcome, computed from a public view alone (`node.get`). Every seat, a
//! node-admin type's (the mesh primary), the fabric primary's and every other node type's (rpc,
//! gateway, ...), is held by its holder until that holder yields it or its exact birth is proven gone
//! (docs/architecture/fabric-node-lifecycle-elections.md §5: "For every seat: a living holder
//! keeps its seat; a lower-NodeId candidate that later becomes Ready never displaces it"; "a restarted
//! node is a candidate for a later vacancy and never displaces the holder"), so a view alone cannot
//! name a holder: `seats_as_expected` checks that exactly one holder is advertised per node type with a
//! ready member and that the fabric primary is a mesh primary, and `seats_kept` checks that a holder
//! still present and live in a later view still holds its seat. [`expected_primaries`] is the seat a
//! vacancy goes to: the lowest Ready NodeId of the node type.

use serde_json::Value;
use std::collections::BTreeMap;

/// `(mesh, kind)`.
pub type NodeType = (String, String);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The lowest ready NodeId (its path) of every node type that has a ready member: the node a vacancy of
/// the node type's seat goes to (elections §5: "a holder that yields or whose exact birth is proven gone
/// hands the seat to the lowest Ready NodeId of its own mesh"), never one that displaces a
/// living holder.
pub fn expected_primaries(nodes: &[Value]) -> BTreeMap<NodeType, String> {
    let mut best: BTreeMap<NodeType, (String, String)> = BTreeMap::new();
    for n in nodes.iter().filter(|n| n["status"] == "ready") {
        let (id, name) = (s(&n["node_id"]), s(&n["name"]));
        let e = best.entry((s(&n["mesh"]), s(&n["kind"]))).or_insert((id.clone(), name.clone()));
        if id < e.0 {
            *e = (id, name);
        }
    }
    best.into_iter().map(|(c, (_, name))| (c, name)).collect()
}

/// The primaries a view advertises, per node type.
pub fn advertised_primaries(nodes: &[Value]) -> BTreeMap<NodeType, Vec<String>> {
    let mut out: BTreeMap<NodeType, Vec<String>> = BTreeMap::new();
    for n in nodes {
        let e = out.entry((s(&n["mesh"]), s(&n["kind"]))).or_default();
        if n["is_primary"] == true {
            e.push(s(&n["name"]));
        }
    }
    out
}

/// The fabric primaries a view advertises.
pub fn advertised_fabric_primaries(nodes: &[Value]) -> Vec<String> {
    nodes.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect()
}

/// Whether the view advertises the seats the rules allow: every node type with a ready member
/// advertises exactly one primary, a member of the node type (its holder, whichever it is: a view alone
/// cannot name it, elections §5), no node type advertises two, and exactly one fabric primary is
/// advertised, a node-admin that is also its mesh's primary.
pub fn primaries_as_expected(nodes: &[Value]) -> Result<(), String> {
    for (c, p) in &advertised_primaries(nodes) {
        let ready = nodes.iter().any(|n| s(&n["mesh"]) == c.0 && s(&n["kind"]) == c.1 && n["status"] == "ready");
        if (ready && p.len() != 1) || p.len() > 1 {
            return Err(format!("node type {c:?}: advertised {p:?}, expected exactly one holder"));
        }
    }
    let fp = advertised_fabric_primaries(nodes);
    let holds_mesh_seat = |name: &String| nodes.iter().any(|n| &s(&n["name"]) == name && n["kind"] == "node_admin" && n["is_primary"] == true);
    if fp.len() != 1 || !fp.iter().all(holds_mesh_seat) {
        return Err(format!("fabric primary: advertised {fp:?}, expected exactly one that is its mesh's primary"));
    }
    Ok(())
}

/// Whether every seat `before` advertised for a birth that `after` still shows live is still held
/// by that birth, for every node type and the fabric seat: a holder that lives keeps its seat, and a
/// lower NodeId, a newcomer or a restarted node never displaces it (elections §5).
pub fn primaries_kept(before: &[Value], after: &[Value]) -> Result<(), String> {
    for b in before.iter().filter(|n| n["is_primary"] == true || n["is_fabric_primary"] == true) {
        let Some(a) = after.iter().find(|n| n["name"] == b["name"] && n["incarnation_id"] == b["incarnation_id"]) else { continue };
        if a["status"] == "ready" || a["status"] == "pending-reconnect" {
            for seat in ["is_primary", "is_fabric_primary"] {
                if b[seat] == true && a[seat] != true {
                    return Err(format!("{} lost {seat} while it lives ({})", s(&b["name"]), s(&a["status"])));
                }
            }
        }
    }
    Ok(())
}
