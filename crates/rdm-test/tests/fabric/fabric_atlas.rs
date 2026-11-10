//! The fabric-primary's atlas (docs/plans/proposal/fabric-atlas.md, Q194, Q224): one view that
//! combines command admission, execution progress and gossip observations, read by Build acceptance
//! and exposed through `fabric.atlas.get`.

use rdm_core::PathName;
use rdm_test::estate::{named, Estate, Owner};
use rdm_client::{AtlasNodeView, AtlasView};
use serde_json::{json, Value};

/// What the cells read of the atlas: `fabric.atlas.get` through the client's own call,
/// `Fabrics::atlas_get()` (Q167's read naming; `fabric.get` is unchanged), one row per node the
/// fabric-primary holds, with its birth, its status and whether it is stopped, as the node view names
/// them, and the operation of the command accepted on it that has not yet completed (none once it
/// completes).
async fn atlas(estate: &Estate, why: &str) -> AtlasView {
    estate.fabrics_obj().await.atlas_get().await.unwrap_or_else(|e| panic!("{why}: {e}"))
}

/// A row's status as the node view's JSON names it.
fn status_of(row: &AtlasNodeView) -> String {
    serde_json::to_value(row.node.status).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default()
}

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "build".into(), subfeature: "fabric-atlas".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// The `rdm.node_admin.node.reject.via-not-live` spans that name `node`.
fn not_live_of(spans: &[Value], node: &PathName) -> Vec<Value> {
    named(spans, "rdm.node_admin.node.reject.via-not-live").into_iter().filter(|sp| rdm_test::estate::at_location(sp, node)).cloned().collect()
}

/// The node id a settled view row carries.
fn id_of(n: &Value) -> rdm_core::NodeId {
    serde_json::from_value(n["node_id"].clone()).expect("a node id")
}

/// Two meshes, the fabric-primary in mesh1 and two node-admins and two nodes in mesh2, the fabric
/// ready and the typed objects entered at the fabric-primary. mesh2's nodes, each a node view: the rpc
/// node that is not its type's primary, the rpc type's primary, the node-admin that is not the
/// mesh-primary, then the mesh-primary (a primary's stop moves its primary first).
async fn two_meshes(estate: &mut Estate) -> Vec<Value> {
    estate.resize_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(std::time::Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    // Each type's primary is elected once its nodes are Ready and reaches the views by gossip, after
    // the fabric is ready: wait, bounded, until the views name mesh2's node and node-admin primaries.
    let pick = |views: &[Value], kind: &str, primary: bool| views.iter().find(|n| rdm_test::estate::mesh_of(n) == "mesh2" && n["type"] == kind && (n["is_primary"] == true) == primary).cloned();
    let estate: &Estate = estate;
    rdm_test::estate::wait_for("mesh2's views name its node and node-admin primaries", std::time::Duration::from_secs(60), || async move {
        let views = estate.node_values().await;
        Some(vec![pick(&views, "node", false)?, pick(&views, "node", true)?, pick(&views, "node-admin", false)?, pick(&views, "node-admin", true)?])
    })
    .await
}

fn path_of(n: &Value) -> PathName {
    rdm_test::estate::location(n)
}

/// CONTRACT (fabric-atlas.md R-FA5, Q189; R-FA2: "execution progress writes it from each attempt's
/// receipts as they stream up `build.section.run`, before the caller is answered `Complete`"): a
/// `node.start` of another mesh's node, sent the moment that node's `node.stop` streams `Complete`
/// with no wait of any kind, is accepted and completes, the same birth ready again; for every node of
/// the other mesh, a primary's stop moving its primary first included. Must NOT happen: the start
/// refused `node-not-live` (`rdm.node_admin.node.reject.via-not-live` naming the node) because the stop
/// the fabric-primary itself ran has not yet reached it by the other mesh's gossip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_of_another_meshs_node_sent_the_moment_its_stop_completes_is_admitted() {
    let test = "a_start_of_another_meshs_node_sent_the_moment_its_stop_completes_is_admitted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let targets = two_meshes(&mut estate).await;
    let mut refused_starts = Vec::new();
    for before in &targets {
        let node = path_of(before);
        let mut stop = estate.nodes_obj().await.stop(&node).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {node:?}: {e}"));
        Estate::complete(&mut stop).await;
        // No wait: the start goes out on the stop's Complete.
        match estate.nodes_obj().await.start(&node).await {
            Ok(mut start) => {
                Estate::complete(&mut start).await;
            }
            Err(e) => {
                refused_starts.push(format!("{node:?}: {e}"));
                // The cell goes on to the next node with this one started, so every node is asked.
                estate.until_stopped_at_the_fabric_primary(&node).await;
                Estate::workflow(&format!("node.start of {node:?}"), estate.nodes_obj().await.start(&node).await).await;
            }
        }
        let after = estate.until_node(&node, "the node ready again after its stop and start", std::time::Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["state"] == "ready").then_some(n)
        }).await;
        assert_eq!((&after["node_id"], &after["incarnation"]), (&before["node_id"], &before["incarnation"]), "{node:?}: the same birth after its stop and start: {after}");
    }
    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<Value> = targets.iter().flat_map(|n| not_live_of(&spans, &path_of(n))).collect();
    assert!(refused_starts.is_empty() && refused.is_empty(), "R-FA5: the fabric-primary refused a start sent on its node.stop's Complete: {refused_starts:#?}; node-not-live spans: {refused:#?}");
}

/// CONTRACT (fabric-atlas.md R-FA2: "execution progress writes it from each attempt's receipts as they
/// stream up `build.section.run`, before the caller is answered `Complete`"; R-FA1: admission reads it
/// and nothing else): a `node.stop` of another mesh's node, sent the moment that node's `node.start`
/// streams `Complete` with no wait of any kind, is accepted and completes, for every node of the other
/// mesh. Must NOT happen: the stop refused `node-not-live` ("a stop needs a live node") because the
/// fabric-primary still holds the birth stopped until the other mesh's gossip says it rejoined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_another_meshs_node_sent_the_moment_its_start_completes_is_admitted() {
    let test = "a_stop_of_another_meshs_node_sent_the_moment_its_start_completes_is_admitted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let targets = two_meshes(&mut estate).await;
    let mut refused_stops = Vec::new();
    for before in &targets {
        let node = path_of(before);
        estate.stop_then_start(&node).await;
        // No wait: the second stop goes out on the start's Complete.
        match estate.nodes_obj().await.stop(&node).await {
            Ok(mut stop) => {
                Estate::complete(&mut stop).await;
            }
            Err(e) => {
                refused_stops.push(format!("{node:?}: {e}"));
                // The cell goes on with this node stopped, so every node is asked.
                let views_live = |n: Option<&rdm_client::NodeView>| n.filter(|n| serde_json::to_value(n).map(|v| v["state"] == "ready").unwrap_or(false)).map(|_| ());
                estate.until_node(&node, "the started node live at the asked admin", std::time::Duration::from_secs(60), views_live).await;
                Estate::workflow(&format!("node.stop of {node:?}"), estate.nodes_obj().await.stop(&node).await).await;
            }
        }
        estate.until_stopped_at_the_fabric_primary(&node).await;
        Estate::workflow(&format!("node.start of {node:?}"), estate.nodes_obj().await.start(&node).await).await;
        estate.until_node(&node, "the node ready again", std::time::Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["state"] == "ready").then_some(())
        }).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<Value> = targets.iter().flat_map(|n| not_live_of(&spans, &path_of(n))).collect();
    assert!(refused_stops.is_empty() && refused.is_empty(), "R-FA2: the fabric-primary refused a stop sent on its node.start's Complete: {refused_stops:#?}; node-not-live spans: {refused:#?}");
}

/// The atlas as JSON, the shape a caller reads.
async fn atlas_value(estate: &Estate, why: &str) -> Value {
    serde_json::to_value(atlas(estate, why).await).expect("the atlas serializes")
}

/// `v` without the keys `keys`.
fn without(v: &Value, keys: &[&str]) -> Value {
    let mut v = v.clone();
    for k in keys {
        v.as_object_mut().expect("an object").remove(*k);
    }
    v
}

/// The node row of `node_id` in the atlas `read`, fabric by fabric and mesh by mesh.
fn atlas_row<'a>(read: &'a Value, node_id: &Value) -> Option<&'a Value> {
    read["fabrics"].as_array()?.iter().flat_map(|f| f["meshes"].as_array().into_iter().flatten()).flat_map(|m| m["nodes"].as_array().into_iter().flatten()).find(|n| n["node"]["node_id"] == *node_id)
}

/// CONTRACT (query.md §1, Q439: "The atlas, `fabric.atlas.get` and `mesh.atlas.get`: the topology plus
/// what is in flight (Q439), laid out as `fabric.get` answers: a list of fabrics, each with its fields as
/// `fabric.get` answers them and the current Build and its phase, and inside it its meshes in name order,
/// each as `mesh.get` answers it, with its nodes in `(path, name)` order. Each node is the topology's node
/// row as `node.topology.get` serves it, read from the node's record, beside what is in flight on it: the
/// operation accepted and not yet complete, whether it is stopped, ..."; node-investigation.md: "A
/// node-admin's atlas is its topology and its in-flight commands"): on a ready fabric of two meshes,
/// `fabric.atlas.get` at the fabric-primary answers one fabric with `fabric.get`'s fields under its names
/// and the current Build complete, then mesh1 and mesh2, each with `mesh.get`'s fields and every node the
/// node view holds of it in `(path, name)` order, the same birth, type and status. While the
/// fabric-primary holds a node.stop of a mesh1 node it commands (its stopped frame held), that node's row
/// carries the stop as accepted and is not stopped; once the stop completes it is stopped with nothing
/// accepted. `mesh.atlas.get [name: mesh2]` answers the same fabric with mesh2 alone, and a mesh the
/// fabric-primary does not hold as the fabric with no mesh. Must NOT happen: a flat node list, a mesh or
/// a node missing or out of order, a field spelled other than its own read spells it, the stop absent
/// from its node's row while it runs, or a `mesh.atlas.get` answering a mesh it did not name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_atlas_answers_the_fabric_then_its_meshes_and_nodes_with_a_stop_in_flight() {
    use rdm_test::faults::{binding_set, candidate_sha, FaultClient};
    let test = "the_atlas_answers_the_fabric_then_its_meshes_and_nodes_with_a_stop_in_flight";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    let shape = [("mesh1", 1, 2), ("mesh2", 2, 2)];
    estate.resize_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(std::time::Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    let fp = estate.entry().await;
    // mesh1's node that is not its type's primary: the fabric-primary, mesh1's mesh-primary, commands its
    // stop, and the stop moves no role.
    let estate_ref: &Estate = &estate;
    let node = rdm_test::estate::wait_for("mesh1's views name its node type's primary", std::time::Duration::from_secs(60), || async move {
        let views = estate_ref.node_values().await;
        views.iter().any(|n| rdm_test::estate::mesh_of(n) == "mesh1" && n["type"] == "node" && n["is_primary"] == true).then_some(())?;
        views.into_iter().find(|n| rdm_test::estate::mesh_of(n) == "mesh1" && n["type"] == "node" && n["is_primary"] != true)
    })
    .await;
    let fabric = serde_json::to_value(estate.fabric_view().await).expect("fabric.get serializes");
    let meshes_get = serde_json::to_value(estate.meshes_obj().await.get(&rdm_client::Filter::new()).await.expect("mesh.get at the fabric-primary")).expect("mesh.get serializes");
    let views = estate.node_values().await;
    let read = atlas_value(&estate, "Q439: fabric.atlas.get at the fabric-primary").await;
    let fabrics = read["fabrics"].as_array().unwrap_or_else(|| panic!("Q439: the atlas is a list of fabrics, as fabric.get answers: {read}"));
    assert_eq!(fabrics.len(), 1, "one fabric: {read}");
    let f = &fabrics[0];
    assert_eq!(without(f, &["build", "meshes"]), without(&fabric, &["meshes"]), "Q439: the atlas's fabric carries fabric.get's fields, under its names: {read}");
    assert_eq!((&f["build"]["build_id"], &f["build"]["phase"]), (&fabric["build_id"], &json!(rdm_core::build::BuildPhase::Complete)), "Q439: what is in flight at the fabric, the current Build and its phase: {read}");
    let meshes = f["meshes"].as_array().unwrap_or_else(|| panic!("the fabric's meshes: {read}"));
    assert_eq!(meshes.iter().map(|m| m["name"].clone()).collect::<Vec<_>>(), vec![json!("mesh1"), json!("mesh2")], "Q439: every mesh, in name order: {read}");
    for m in meshes {
        let name = m["name"].as_str().unwrap_or_default();
        let get = meshes_get.as_array().into_iter().flatten().find(|g| g["name"] == m["name"]).unwrap_or_else(|| panic!("mesh.get answers {name}: {meshes_get}"));
        assert_eq!(without(m, &["nodes"]), without(get, &["nodes"]), "Q439: the atlas's {name} carries mesh.get's fields, under its names: {read}");
        let rows: Vec<(Value, Value, Value, Value, Value, Value)> = m["nodes"].as_array().unwrap_or_else(|| panic!("{name}'s nodes: {read}")).iter().map(|n| (n["node"]["path"].clone(), n["node"]["name"].clone(), n["node"]["node_id"].clone(), n["node"]["incarnation"].clone(), n["node"]["type"].clone(), n["node"]["status"].clone())).collect();
        let mut want: Vec<(Value, Value, Value, Value, Value, Value)> = views.iter().filter(|v| rdm_test::estate::mesh_of(v) == name).map(|v| (v["path"].clone(), v["name"].clone(), v["node_id"].clone(), v["incarnation"].clone(), v["type"].clone(), v["state"].clone())).collect();
        want.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
        assert_eq!(rows, want, "Q439: {name}'s nodes are every node the node view holds of it, in (path, name) order, each the same birth, type and status: {read}");
    }
    // mesh.atlas.get (Q439 scope): the same fabric with only the meshes the filter names; a mesh the
    // fabric-primary does not hold, the fabric with no mesh.
    let ids = |m: &Value| m["nodes"].as_array().into_iter().flatten().map(|n| n["node"]["node_id"].clone()).collect::<Vec<_>>();
    let named = serde_json::to_value(estate.meshes_obj().await.atlas_get(&rdm_client::Filter::new().eq("name", ["mesh2"])).await.expect("mesh.atlas.get [name: mesh2] at the fabric-primary")).expect("the atlas serializes");
    let named_meshes = named["fabrics"][0]["meshes"].as_array().cloned().unwrap_or_default();
    assert_eq!(named["fabrics"].as_array().map(Vec::len), Some(1), "mesh.atlas.get answers one fabric: {named}");
    assert_eq!(without(&named["fabrics"][0], &["meshes"]), without(f, &["meshes"]), "mesh.atlas.get answers the same fabric as fabric.atlas.get: {named}");
    assert_eq!(named_meshes.iter().map(|m| (m["name"].clone(), ids(m))).collect::<Vec<_>>(), vec![(json!("mesh2"), ids(&meshes[1]))], "Q439: mesh.atlas.get [name: mesh2] answers mesh2 alone, with its nodes: {named}");
    let unknown = serde_json::to_value(estate.meshes_obj().await.atlas_get(&rdm_client::Filter::new().eq("name", ["mesh9"])).await.expect("mesh.atlas.get [name: mesh9] at the fabric-primary")).expect("the atlas serializes");
    assert_eq!(unknown["fabrics"][0]["meshes"], json!([]), "a mesh the fabric-primary does not hold is answered as its fabric with no mesh: {unknown}");
    // A stop in flight at the fabric-primary: the node's row carries it, accepted and not complete.
    let (at, node_id) = (path_of(&node), node["node_id"].clone());
    let faults = FaultClient::at(&estate, &fp).await;
    faults.arm("atlas:stop-in-flight", json!({"kind": "event", "event": "stopped", "node": node_id})).await;
    let stop = tokio::spawn({
        let (nodes, at) = (estate.nodes_obj().await, at.clone());
        async move { Estate::workflow(&format!("node.stop of {at:?}"), nodes.stop(&at).await).await }
    });
    faults.wait_held("atlas:stop-in-flight").await;
    let in_flight = atlas_value(&estate, "fabric.atlas.get while the stop is held").await;
    let row = atlas_row(&in_flight, &node_id).unwrap_or_else(|| panic!("the atlas holds no row of {at:?}: {in_flight}"));
    assert_eq!((&row["accepted"]["action"], &row["stopped"]), (&json!("node.stop"), &json!(false)), "node-investigation.md: while its stop runs, {at:?}'s row carries the stop accepted and not complete: {row}");
    faults.release("atlas:stop-in-flight").await;
    stop.await.expect("the stop's task");
    let done = atlas_value(&estate, "fabric.atlas.get once the stop completed").await;
    let row = atlas_row(&done, &node_id).unwrap_or_else(|| panic!("the atlas holds no row of {at:?}: {done}"));
    assert_eq!((&row["accepted"], &row["stopped"]), (&Value::Null, &json!(true)), "once its stop completed, {at:?} is stopped with nothing accepted: {row}");
    estate.stop().await;
}

/// CONTRACT (fabric-atlas.md R-FA2: "execution progress writes it ... before the caller is answered
/// `Complete`"; Q197: no action decision waits for gossip; node-stop.md: the fabric-primary runs the stop
/// of a mesh-primary, a node command, never a Build, Q342): with mesh2's topology withheld from the
/// backbone (so no gossip observation can be the source), the fabric-primary runs `node.stop` of mesh2's
/// primary; the moment the stop streams `Complete`, the atlas holds that node stopped with nothing
/// accepted on it (a node command is never a Build's accepted operation). Must NOT happen: the stop
/// absent from the atlas at `Complete` until mesh2's gossip echoes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_atlas_holds_a_stop_the_fabric_primary_ran_at_its_complete() {
    use rdm_test::faults::{binding_set, candidate_sha, FaultClient};
    let test = "the_atlas_holds_a_stop_the_fabric_primary_ran_at_its_complete";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    let targets = two_meshes(&mut estate).await;
    let (node, node_id) = (path_of(&targets[3]), id_of(&targets[3]));
    let mut mesh2_faults = Vec::new();
    for v in estate.node_values().await.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin") {
        mesh2_faults.push((path_of(v), FaultClient::at(&estate, &path_of(v)).await));
    }
    for (_, d) in &mesh2_faults {
        d.arm("atlas:no-gossip", json!({"kind": "announcement", "announcement": "aggregate-in-flight", "channel": "backbone"})).await;
    }
    let mut stop = estate.nodes_obj().await.stop(&node).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {node:?}: {e}"));
    Estate::complete(&mut stop).await;
    let at_complete = atlas(&estate, "R-FA2: fabric.atlas.get on the stop's Complete").await;
    let row = at_complete.node(&node_id).unwrap_or_else(|| panic!("R-FA2: the atlas holds no row of {node:?}: {at_complete:#?}"));
    assert!(row.stopped && row.accepted.is_none(), "R-FA2: on the stop's Complete the atlas holds {node:?} stopped from its own command's frames, nothing accepted on it: {row:#?}");
    // The stopped node's fault service answers no release: it is stopped.
    for (_, d) in mesh2_faults.iter().filter(|(at, _)| *at != node) {
        d.release("atlas:no-gossip").await;
    }
    estate.stop().await;
}

/// The atlas row of `node` read every gossip interval for `floors` staleness floors, each passed to
/// `holds`, which names what it found wrong.
async fn atlas_holds_for(estate: &Estate, node: &PathName, node_id: &rdm_core::NodeId, floors: u32, why: &str, holds: impl Fn(&AtlasNodeView) -> Result<(), String>) {
    let until = tokio::time::Instant::now() + rdm_core::cadence::staleness_floor() * floors;
    while tokio::time::Instant::now() < until {
        let read = atlas(estate, why).await;
        let row = read.node(node_id).unwrap_or_else(|| panic!("{why}: the atlas holds no row of {node:?}: {read:#?}"));
        if let Err(wrong) = holds(row) {
            panic!("{why}: {wrong}: {row:#?}");
        }
        tokio::time::sleep(rdm_core::cadence::gossip_interval()).await;
    }
}

/// CONTRACT (fabric-atlas.md R-FA3: "Gossip observations merge in only what the fabric-primary did not
/// do itself, each under the receiver rules' ordering (gossip.md §3.1: a frame older than the newest
/// held for its key is ignored; a birth's `digest_seq`; a Build's attempt number), so a delayed event
/// never moves it backward"): for each node-admin of the other mesh, stopped then started by the
/// fabric-primary, the atlas holds the node stopped from the stop's `Complete` and live, the same
/// birth, from the start's `Complete`, and keeps each for two staleness floors read every gossip
/// interval while the other mesh's mesh topologies, which reach the fabric-primary after its own receipts,
/// arrive. Must NOT happen: a digest of the birth from before its stop taking it live again while it is
/// stopped, or the other mesh's projection still carrying the stop (or its `NodeStopped`) taking it
/// stopped again after the start completed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delayed_observation_of_another_meshs_node_never_moves_the_atlas_back() {
    let test = "a_delayed_observation_of_another_meshs_node_never_moves_the_atlas_back";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let targets = two_meshes(&mut estate).await;
    for before in targets.iter().filter(|n| n["type"] == "node-admin") {
        let (node, node_id) = (path_of(before), id_of(before));
        let birth = (before["node_id"].as_str().unwrap_or_default().to_string(), before["incarnation"].as_str().unwrap_or_default().to_string());
        Estate::workflow(&format!("node.stop of {node:?}"), estate.nodes_obj().await.stop(&node).await).await;
        atlas_holds_for(&estate, &node, &node_id, 1, "R-FA3: after the stop's Complete", |row| if row.stopped { Ok(()) } else { Err(format!("{node:?} is not stopped in the atlas")) }).await;
        Estate::workflow(&format!("node.start of {node:?}"), estate.nodes_obj().await.start(&node).await).await;
        atlas_holds_for(&estate, &node, &node_id, 2, "R-FA3: after the start's Complete", |row| {
            if row.stopped || status_of(row) != "ready" {
                return Err(format!("{node:?} is not live in the atlas"));
            }
            if (row.node.node_id.to_string(), row.node.incarnation.as_str().to_string()) != birth {
                return Err(format!("{node:?} is not the birth it was before its stop and start"));
            }
            Ok(())
        }).await;
    }
    estate.stop().await;
}

/// CONTRACT (node-name-update.md step 1: "an admin that is not the fabric-primary carries it to the
/// fabric-primary ... The fabric-primary sends the command to the node's mesh-primary ... and, on its
/// answer that the row moved, writes the new name to the node in its atlas"; Q197, Q203, Q204:
/// node reads answer from the atlas and each op writes it): a live node of mesh2, its connections
/// deleted, is renamed by a caller's `node.name.update` sent directly to mesh2's
/// primary, answered `NameUpdated`; the fabric-primary's node read, at once and with no wait, lists the
/// birth at the new name and none at the old. mesh2's primary carried the call up
/// (`rdm.node_admin.node.update.via-forward`, op node.name.update), the fabric-primary sent it down and
/// renamed the node in its atlas (`rdm.node_admin.node.update.via-fabric-primary`, atlas=renamed), and
/// mesh2's primary wrote the row once (`rdm.node_admin.name.update.via-writer`). Must NOT happen:
/// the fabric-primary answering the old name after the rename was answered, until a fill or gossip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_name_update_served_at_another_meshs_primary_is_read_at_the_fabric_primary_at_once() {
    let test = "a_name_update_served_at_another_meshs_primary_is_read_at_the_fabric_primary_at_once";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let targets = two_meshes(&mut estate).await;
    let (node, primary) = (path_of(&targets[0]), &targets[3]);
    let renamed = node.with_name("renamed").unwrap();
    let nodes = estate.nodes_obj().await;
    // The node's connections deleted: the precondition of a rename.
    for (what, stream) in [("node.connection.delete", nodes.delete_connections(&node, &rdm_core::status::ConnectionScope::All).await)] {
        let mut stream = stream.unwrap_or_else(|e| panic!("{what} of {node:?} was refused: {e}"));
        let mut last = None;
        while let Some(f) = stream.next().await {
            let f = f.unwrap_or_else(|e| panic!("{what} of {node:?}: {e}"));
            let terminal = f.is_terminal();
            last = Some(f);
            if terminal { break; }
        }
        assert_eq!(last, Some(rdm_client::Frame::Complete), "{what} of {node:?} completes");
    }
    let id = |v: &Value, k: &str| v[k].as_str().unwrap_or_default().to_string();
    let node_id: rdm_core::NodeId = serde_json::from_value(targets[0]["node_id"].clone()).expect("a node id");
    let birth = rdm_client::ExactBirth {
        target: rdm_transport::NodeTarget::ExactNode(node_id.clone()),
        node_id,
        incarnation: serde_json::from_value(targets[0]["incarnation"].clone()).expect("an incarnation id"),
    };
    let mesh_primary: rdm_core::NodeId = serde_json::from_value(primary["node_id"].clone()).expect("a node id");
    let reply = nodes.name_update(&birth, &mesh_primary, &renamed.name).await.unwrap_or_else(|e| panic!("node.name.update of {node:?} at mesh2's primary: {e}"));
    assert!(matches!(reply, rdm_core::status::StatusReply::NameUpdated { .. }), "node.name.update of {node:?} at mesh2's primary: {reply:?}");
    // No wait: the fabric-primary's read the moment the caller is answered.
    let read = estate.node_values().await;
    let at: Vec<PathName> = read.iter().filter(|n| n["node_id"] == targets[0]["node_id"]).map(path_of).collect();
    estate.stop().await;
    assert_eq!(at, vec![renamed.clone()], "the fabric-primary's node read lists the birth of {node:?} at {renamed:?} once the rename was answered: {read:#?}");
    let spans = estate.spans();
    let (mp, mp_id) = (path_of(primary), id(primary, "node_id"));
    let forwarded: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-forward").into_iter().filter(|sp| attr(sp, "receiver_node_id") == mp_id && attr(sp, "op") == "node.name.update").collect();
    assert_eq!(forwarded.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(), vec!["name-updated".to_string()], "mesh2's primary carried the caller's rename up to the fabric-primary once: {forwarded:#?}");
    let down: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-fabric-primary").into_iter().filter(|sp| attr(sp, "op") == "node.name.update").collect();
    assert_eq!(down.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(), vec!["name-updated".to_string()], "the fabric-primary sent it down once: {down:#?}");
    let written: Vec<&Value> = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().filter(|sp| attr(sp, "node_id") == id(&targets[0], "node_id")).collect();
    assert_eq!(written.iter().map(|sp| (rdm_test::estate::at_location(sp, &mp), attr(sp, "outcome"))).collect::<Vec<_>>(), vec![(true, "name-updated".to_string())], "mesh2's primary wrote the row once: {written:#?}");
}

/// CONTRACT (fabric-atlas.md R-FA1: the fabric-primary accepts or denies from the atlas, the gossip view
/// context only; R-FA2; node-primary-update.md "No-successor cases"): with the fabric-primary's gossip
/// view holding mesh2's non-primary node-admin stopped, that node-admin is started by the
/// fabric-primary; the moment its start streams `Complete`,
/// with no wait, a Delete of mesh2's primary, whose only other node-admin is
/// the one just started, is accepted and completes. Must NOT happen: the Delete refused `last-admin`
/// (`rdm.node_admin.build.reject.via-last-admin`) because the gossip view still holds the started
/// node-admin stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_of_a_primary_right_after_its_only_other_admins_start_is_accepted() {
    let test = "a_delete_of_a_primary_right_after_its_only_other_admins_start_is_accepted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let targets = two_meshes(&mut estate).await;
    let (other, primary) = (path_of(&targets[2]), path_of(&targets[3]));
    let other_id: rdm_core::NodeId = serde_json::from_value(targets[2]["node_id"].clone()).expect("a node id");
    let fp = estate.fabric_view().await.fabric_primary.expect("fabric.get names the fabric-primary");
    let fp_view = estate.node_views().await.into_iter().find(|n| n.node_id == fp).expect("the fabric-primary is listed");
    Estate::workflow(&format!("node.stop of {other:?}"), estate.nodes_obj().await.stop(&other).await).await;
    // The fabric-primary's gossip view holds the stop: the birth dropped and its stop overlay held.
    rdm_test::typed::peer_holds(&*estate.peers().await, &fp_view, "mesh2", "the fabric-primary's view holds the stopped node-admin stopped", |v| v.birth(&other_id).is_none() && v.overlay(&other_id, "node.stop").is_some()).await;
    Estate::workflow(&format!("node.start of {other:?}"), estate.nodes_obj().await.start(&other).await).await;
    // No wait: the Delete goes out on the start's Complete.
    let primary_id = estate.node_view(&primary).await.node_id;
    let change = rdm_client::Change::Delete { meshes: Vec::new(), node_ids: vec![primary_id] };
    let submitted = estate.carrier().await.create(change).await;
    let refused = match submitted {
        Ok(stream) => {
            estate.follow(stream).await;
            None
        }
        Err(e) => Some(e.to_string()),
    };
    estate.stop().await;
    let spans = estate.spans();
    let last_admin: Vec<Value> = named(&spans, "rdm.node_admin.build.reject.via-last-admin").into_iter().cloned().collect();
    assert!(refused.is_none() && last_admin.is_empty(), "R-FA1: the Delete of {primary:?} right after {other:?}'s start completed was refused: {refused:?}; last-admin spans: {last_admin:#?}");
}
