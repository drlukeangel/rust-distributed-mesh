//! The expected election outcome, computed from a public view alone
//! (`GET /api/nodes`). An rpc, gateway or other cohort's primary is its ready member with the
//! lowest `node_id`. A node-admin seat (a mesh primary, the fabric primary) is held by its holder
//! until that holder is proven unable to hold it (ruling R-A2), so a view alone cannot name it:
//! [`seats_as_expected`] checks that exactly one holder is advertised per cohort and that the
//! fabric primary is a mesh primary, and [`seats_kept`] checks that a holder that is still present
//! in a later view still holds its seat.

use serde_json::Value;
use std::collections::BTreeMap;

/// `(mesh, kind)`.
pub type Cohort = (String, String);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The expected primary (path) of every cohort that has a ready member.
pub fn expected_primaries(nodes: &[Value]) -> BTreeMap<Cohort, String> {
    let mut best: BTreeMap<Cohort, (String, String)> = BTreeMap::new();
    for n in nodes.iter().filter(|n| n["status"] == "ready-for-traffic") {
        let (id, name) = (s(&n["node_id"]), s(&n["name"]));
        let e = best.entry((s(&n["mesh"]), s(&n["kind"]))).or_insert((id.clone(), name.clone()));
        if id < e.0 {
            *e = (id, name);
        }
    }
    best.into_iter().map(|(c, (_, name))| (c, name)).collect()
}

/// The primaries a view advertises, per cohort.
pub fn advertised_primaries(nodes: &[Value]) -> BTreeMap<Cohort, Vec<String>> {
    let mut out: BTreeMap<Cohort, Vec<String>> = BTreeMap::new();
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

/// Whether the view advertises the seats the rules allow: an rpc, gateway or other cohort with a
/// ready member advertises exactly the lowest ready NodeId; a node-admin cohort with a ready
/// member advertises exactly one primary, a member of the cohort (the holder, whichever it is);
/// and exactly one fabric primary is advertised, a node-admin that is also its mesh's primary.
pub fn seats_as_expected(nodes: &[Value]) -> Result<(), String> {
    let want = expected_primaries(nodes);
    let have = advertised_primaries(nodes);
    for (c, p) in &have {
        if c.1 == "node_admin" {
            let ready = nodes.iter().any(|n| s(&n["mesh"]) == c.0 && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic");
            if (ready && p.len() != 1) || p.len() > 1 {
                return Err(format!("cohort {c:?}: advertised {p:?}, expected exactly one holder"));
            }
            continue;
        }
        match want.get(c) {
            Some(w) if p.as_slice() == [w.clone()] => {}
            None if p.is_empty() => {}
            w => return Err(format!("cohort {c:?}: advertised {p:?}, expected {w:?}")),
        }
    }
    let fp = advertised_fabric_primaries(nodes);
    let holds_mesh_seat = |name: &String| nodes.iter().any(|n| &s(&n["name"]) == name && n["kind"] == "node_admin" && n["is_primary"] == true);
    if fp.len() != 1 || !fp.iter().all(holds_mesh_seat) {
        return Err(format!("fabric primary: advertised {fp:?}, expected exactly one that is its mesh's primary"));
    }
    Ok(())
}

/// Whether every seat `before` advertised for a birth that `after` still shows is still held by
/// that birth: a holder that lives keeps its seat, and a lower NodeId never displaces it.
pub fn seats_kept(before: &[Value], after: &[Value]) -> Result<(), String> {
    for b in before.iter().filter(|n| n["kind"] == "node_admin" && (n["is_primary"] == true || n["is_fabric_primary"] == true)) {
        let Some(a) = after.iter().find(|n| n["name"] == b["name"] && n["incarnation_id"] == b["incarnation_id"]) else { continue };
        if a["status"] == "ready-for-traffic" || a["status"] == "pending-reconnect" {
            for seat in ["is_primary", "is_fabric_primary"] {
                if b[seat] == true && a[seat] != true {
                    return Err(format!("{} lost {seat} while it lives ({})", s(&b["name"]), s(&a["status"])));
                }
            }
        }
    }
    Ok(())
}
