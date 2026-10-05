//! The expected election outcome, computed from a public view alone
//! (`GET /api/nodes`): every cohort's primary is its ready member with the
//! lowest `node_id`, and the fabric primary is the mesh primary (node-admin
//! cohort primary) with the lowest `node_id`. A test compares a view's
//! advertised seats with this, never with an incumbent it remembers.

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

/// The expected fabric primary (path): the lowest-NodeId mesh primary.
pub fn expected_fabric_primary(nodes: &[Value]) -> Option<String> {
    let primaries = expected_primaries(nodes);
    nodes
        .iter()
        .filter(|n| n["kind"] == "node_admin" && primaries.get(&(s(&n["mesh"]), "node_admin".into())) == Some(&s(&n["name"])))
        .min_by_key(|n| s(&n["node_id"]))
        .map(|n| s(&n["name"]))
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

/// Whether the view advertises exactly the expected seats: one primary per
/// cohort with a ready member, and one fabric primary, each the computed one.
pub fn seats_as_expected(nodes: &[Value]) -> Result<(), String> {
    let want = expected_primaries(nodes);
    let have = advertised_primaries(nodes);
    for (c, p) in &have {
        match want.get(c) {
            Some(w) if p.as_slice() == [w.clone()] => {}
            None if p.is_empty() => {}
            w => return Err(format!("cohort {c:?}: advertised {p:?}, expected {w:?}")),
        }
    }
    let fp = advertised_fabric_primaries(nodes);
    let wfp = expected_fabric_primary(nodes);
    if fp != wfp.clone().into_iter().collect::<Vec<_>>() {
        return Err(format!("fabric primary: advertised {fp:?}, expected {wfp:?}"));
    }
    Ok(())
}
