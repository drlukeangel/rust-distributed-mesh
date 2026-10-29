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
/// them, and the operation of the command accepted on it that has not yet completed (none once its
/// attempt's receipts are in).
async fn atlas(estate: &Estate, why: &str) -> AtlasView {
    estate.fabrics_obj().await.atlas_get().await.unwrap_or_else(|e| panic!("{why}: {e}"))
}

/// A row's status as the node view's JSON names it.
fn status_of(row: &AtlasNodeView) -> String {
    serde_json::to_value(row.status).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default()
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
fn not_live_of(spans: &[Value], node: &str) -> Vec<Value> {
    named(spans, "rdm.node_admin.node.reject.via-not-live").into_iter().filter(|sp| attr(sp, "node") == node).cloned().collect()
}

/// Two meshes, the fabric-primary in mesh1 and two node-admins and two rpc nodes in mesh2, the fabric
/// ready and the typed objects entered at the fabric-primary. mesh2's nodes, each a node view: the rpc
/// node that is not its type's primary, the rpc type's primary, the node-admin that is not the
/// mesh-primary, then the mesh-primary (a primary's stop moves its primary first).
async fn two_meshes(estate: &mut Estate) -> Vec<Value> {
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(std::time::Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    let views = estate.node_values().await;
    let pick = |kind: &str, primary: bool| views.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == kind && (n["is_primary"] == true) == primary).cloned().unwrap_or_else(|| panic!("mesh2 holds a {kind} with is_primary={primary}: {views:#?}"));
    vec![pick("rpc_node", false), pick("rpc_node", true), pick("node_admin", false), pick("node_admin", true)]
}

fn path_of(n: &Value) -> PathName {
    n["name"].as_str().unwrap_or_default().parse().unwrap_or_else(|e| panic!("{n} names no path.name: {e:?}"))
}

/// CONTRACT (fabric-atlas.md R-FA5, Q189; R-FA2: "execution progress writes it from each attempt's
/// receipts as they stream up `build.attempt.run`, before the caller is answered `Complete`"): a
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
        let mut stop = estate.nodes_obj().await.stop(&node).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {node}: {e}"));
        Estate::complete(&mut stop).await;
        // No wait: the start goes out on the stop's Complete.
        match estate.nodes_obj().await.start(&node).await {
            Ok(mut start) => {
                Estate::complete(&mut start).await;
            }
            Err(e) => {
                refused_starts.push(format!("{node}: {e}"));
                // The cell goes on to the next node with this one started, so every node is asked.
                estate.until_stopped_at_the_fabric_primary(&node).await;
                Estate::workflow(&format!("node.start of {node}"), estate.nodes_obj().await.start(&node).await).await;
            }
        }
        let after = estate.until_node(&node.to_string(), "the node ready again after its stop and start", std::time::Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["stopped"] == false).then_some(n)
        }).await;
        assert_eq!((&after["node_id"], &after["incarnation_id"]), (&before["node_id"], &before["incarnation_id"]), "{node}: the same birth after its stop and start: {after}");
    }
    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<Value> = targets.iter().flat_map(|n| not_live_of(&spans, &path_of(n).to_string())).collect();
    assert!(refused_starts.is_empty() && refused.is_empty(), "R-FA5: the fabric-primary refused a start sent on its node.stop's Complete: {refused_starts:#?}; node-not-live spans: {refused:#?}");
}

/// CONTRACT (fabric-atlas.md R-FA2: "execution progress writes it from each attempt's receipts as they
/// stream up `build.attempt.run`, before the caller is answered `Complete`"; R-FA1: admission reads it
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
        let (_, _) = estate.stop_then_start(&node).await;
        // No wait: the second stop goes out on the start's Complete.
        match estate.nodes_obj().await.stop(&node).await {
            Ok(mut stop) => {
                Estate::complete(&mut stop).await;
            }
            Err(e) => {
                refused_stops.push(format!("{node}: {e}"));
                // The cell goes on with this node stopped, so every node is asked.
                let views_live = |n: Option<&rdm_client::NodeView>| n.filter(|n| serde_json::to_value(n).map(|v| v["status"] == "ready" && v["stopped"] == false).unwrap_or(false)).map(|_| ());
                estate.until_node(&node.to_string(), "the started node live at the asked admin", std::time::Duration::from_secs(60), views_live).await;
                Estate::workflow(&format!("node.stop of {node}"), estate.nodes_obj().await.stop(&node).await).await;
            }
        }
        estate.until_stopped_at_the_fabric_primary(&node).await;
        Estate::workflow(&format!("node.start of {node}"), estate.nodes_obj().await.start(&node).await).await;
        estate.until_node(&node.to_string(), "the node ready again", std::time::Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["stopped"] == false).then_some(())
        }).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<Value> = targets.iter().flat_map(|n| not_live_of(&spans, &path_of(n).to_string())).collect();
    assert!(refused_stops.is_empty() && refused.is_empty(), "R-FA2: the fabric-primary refused a stop sent on its node.start's Complete: {refused_stops:#?}; node-not-live spans: {refused:#?}");
}

/// CONTRACT (fabric-atlas.md R-FA4: "`fabric.atlas.get` returns it (Q167's read naming); `fabric.get`
/// is unchanged"): on a ready fabric of two meshes, `fabric.atlas.get` at the fabric-primary answers
/// the atlas, one row for every node the fabric-primary's node view holds, each the same birth (node
/// id and incarnation) and status; `fabric.get` still answers the Fabric as before. Must NOT happen:
/// the read refused or absent, a node of either mesh missing from it, or `fabric.get` changed by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fabrics_atlas_get_answers_every_node_the_fabric_primary_holds() {
    let test = "fabrics_atlas_get_answers_every_node_the_fabric_primary_holds";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let _ = two_meshes(&mut estate).await;
    let fabric = estate.fabric_view().await;
    let views = estate.node_values().await;
    let read = atlas(&estate, "R-FA4: fabric.atlas.get at the fabric-primary").await;
    for v in &views {
        let name = v["name"].as_str().unwrap_or_default();
        let row = read.node(name).unwrap_or_else(|| panic!("R-FA4: the atlas holds no row of {name}: {read:#?}"));
        assert_eq!((row.node_id.to_string().as_str(), row.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or_default(), status_of(row).as_str(), row.stopped), (v["node_id"].as_str().unwrap_or_default(), v["incarnation_id"].as_str().unwrap_or_default(), v["status"].as_str().unwrap_or_default(), v["stopped"] == true), "R-FA4: the atlas row of {name} is its birth as the node view holds it: {row:#?} vs {v}");
    }
    assert_eq!(read.nodes.len(), views.len(), "R-FA4: the atlas holds exactly the nodes the node view holds: {read:#?}");
    let again = estate.fabric_view().await;
    assert_eq!((&again.id, &again.name, &again.fabric_primary, &again.build_id), (&fabric.id, &fabric.name, &fabric.fabric_primary, &fabric.build_id), "fabric.get answers the Fabric as before the atlas read");
    estate.stop().await;
}

/// CONTRACT (fabric-atlas.md R-FA2: "Command admission writes it at acceptance; execution progress
/// writes it from each attempt's receipts as they stream up `build.attempt.run`, before the caller is
/// answered `Complete`"): with the other mesh's topology withheld from the backbone (so no gossip
/// observation can be the source) and the fabric-primary held after it claimed the `node.stop`
/// attempt and before it dispatched it, the atlas already names the accepted stop on the node; once
/// released, the moment the stop streams `Complete`, the atlas holds the node stopped with nothing
/// accepted on it. Must NOT happen: the atlas silent on an accepted command until its attempt runs, or
/// the stop absent from it at `Complete` until the other mesh's gossip echoes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_atlas_names_a_stop_at_its_admission_and_its_receipts_before_complete() {
    use rdm_test::faults::{binding_set, candidate_sha, Door};
    let test = "the_atlas_names_a_stop_at_its_admission_and_its_receipts_before_complete";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let targets = two_meshes(&mut estate).await;
    let node = path_of(&targets[0]);
    let fp = estate.fabric_view().await.fabric_primary.expect("fabric.get names the fabric-primary").to_string();
    let fp_door = Door::at(&estate, &fp).await;
    let mut mesh2_doors = Vec::new();
    for v in estate.node_values().await.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        mesh2_doors.push(Door::at(&estate, v["name"].as_str().unwrap_or_default()).await);
    }
    for d in &mesh2_doors {
        d.arm("atlas:no-gossip", json!({"kind": "announcement", "announcement": "aggregate-in-flight", "channel": "backbone"})).await;
    }
    fp_door.arm("atlas:claimed", json!({"kind": "claimed-attempt"})).await;
    let mut stop = estate.nodes_obj().await.stop(&node).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {node}: {e}"));
    fp_door.wait_held("atlas:claimed").await;
    let at_acceptance = atlas(&estate, "R-FA2: fabric.atlas.get while the accepted stop's attempt is held before its dispatch").await;
    let row = at_acceptance.node(&node.to_string()).unwrap_or_else(|| panic!("R-FA2: the atlas holds no row of {node}: {at_acceptance:#?}"));
    assert_eq!(row.accepted.as_deref(), Some(format!("node.stop:{node}").as_str()), "R-FA2: the atlas names the accepted stop on {node} before its attempt runs: {row:#?}");
    assert!(!row.stopped, "R-FA2: an accepted stop whose attempt has not run does not stop the node in the atlas: {row:#?}");
    fp_door.release("atlas:claimed").await;
    Estate::complete(&mut stop).await;
    let at_complete = atlas(&estate, "R-FA2: fabric.atlas.get on the stop's Complete").await;
    let row = at_complete.node(&node.to_string()).unwrap_or_else(|| panic!("R-FA2: the atlas holds no row of {node}: {at_complete:#?}"));
    assert!(row.stopped && row.accepted.is_none(), "R-FA2: on the stop's Complete the atlas holds {node} stopped from its receipts, nothing accepted on it: {row:#?}");
    for d in &mesh2_doors {
        d.release("atlas:no-gossip").await;
    }
    estate.stop().await;
}

/// The atlas row of `node` read every gossip interval for `floors` staleness floors, each passed to
/// `holds`, which names what it found wrong.
async fn atlas_holds_for(estate: &Estate, node: &PathName, floors: u32, why: &str, holds: impl Fn(&AtlasNodeView) -> Result<(), String>) {
    let until = tokio::time::Instant::now() + rdm_core::cadence::staleness_floor() * floors;
    while tokio::time::Instant::now() < until {
        let read = atlas(estate, why).await;
        let row = read.node(&node.to_string()).unwrap_or_else(|| panic!("{why}: the atlas holds no row of {node}: {read:#?}"));
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
    for before in targets.iter().filter(|n| n["kind"] == "node_admin") {
        let node = path_of(before);
        let birth = (before["node_id"].as_str().unwrap_or_default().to_string(), before["incarnation_id"].as_str().unwrap_or_default().to_string());
        Estate::workflow(&format!("node.stop of {node}"), estate.nodes_obj().await.stop(&node).await).await;
        atlas_holds_for(&estate, &node, 1, "R-FA3: after the stop's Complete", |row| if row.stopped { Ok(()) } else { Err(format!("{node} is not stopped in the atlas")) }).await;
        Estate::workflow(&format!("node.start of {node}"), estate.nodes_obj().await.start(&node).await).await;
        atlas_holds_for(&estate, &node, 2, "R-FA3: after the start's Complete", |row| {
            if row.stopped || status_of(row) != "ready" {
                return Err(format!("{node} is not live in the atlas"));
            }
            if (row.node_id.to_string(), row.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default()) != birth {
                return Err(format!("{node} is not the birth it was before its stop and start"));
            }
            Ok(())
        }).await;
    }
    estate.stop().await;
}

/// CONTRACT (fabric-atlas.md R-FA2: "execution progress writes it from each attempt's receipts as they
/// stream up `build.attempt.run`, before the caller is answered `Complete`"; node-rpc-envelope.md
/// node.get: "node.get, mesh.nodes.get and fabric.nodes.get are answered by the fabric-primary from its
/// atlas"): with the other mesh's topology withheld from the backbone (so no gossip observation
/// can be the source), the predecessor's half of the replace steps of a mesh2 node (connections
/// deleted, mesh left, renamed `<path>.old`, stopped; lifecycles/node-replace.md steps 1-2), executed
/// by mesh2's primary, is read back through `mesh.nodes.get` at the fabric-primary the moment it
/// streams `Complete`: the same birth listed stopped at `<path>.old`, and the atlas holds it so. Must
/// NOT happen: the read answering the birth at `<path>`, not stopped, from a gossip view that has not
/// heard the stop (release red e58dbc369: the fill at .926687, the read at .929307, the view's last
/// word `ConnectionsDeleting` from .798319).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_of_steps_another_meshs_primary_executed_is_read_back_at_the_fabric_primary_on_its_complete() {
    use rdm_test::faults::{binding_set, candidate_sha, Door};
    let test = "a_build_of_steps_another_meshs_primary_executed_is_read_back_at_the_fabric_primary_on_its_complete";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let targets = two_meshes(&mut estate).await;
    let before = &targets[0];
    let node = path_of(before);
    let renamed = node.renamed();
    let mut mesh2_doors = Vec::new();
    for v in estate.node_values().await.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        mesh2_doors.push(Door::at(&estate, v["name"].as_str().unwrap_or_default()).await);
    }
    for d in &mesh2_doors {
        d.arm("atlas:no-gossip", json!({"kind": "announcement", "announcement": "aggregate-in-flight", "channel": "backbone"})).await;
    }
    Estate::workflow(&format!("the predecessor's half of the replace of {node}"), estate.nodes_obj().await.steps(rdm_test::steps::predecessor_steps(&node)).await).await;
    // No wait: the read goes out on the Build's Complete.
    let listed = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [&node.mesh])).await.expect("node.get [mesh] at the fabric-primary");
    let read = atlas(&estate, "R-FA2: fabric.atlas.get on the Build's Complete").await;
    for d in &mesh2_doors {
        d.release("atlas:no-gossip").await;
    }
    let at_old = listed.iter().find(|n| n.name == renamed).cloned();
    assert!(
        at_old.as_ref().is_some_and(|n| n.stopped && n.node_id.to_string() == before["node_id"].as_str().unwrap_or_default()),
        "mesh.nodes.get at the fabric-primary lists {node}'s birth stopped at {renamed} on the Build's Complete: {listed:#?}"
    );
    assert!(!listed.iter().any(|n| n.name == node && n.node_id.to_string() == before["node_id"].as_str().unwrap_or_default()), "the renamed birth is not answered at {node} too: {listed:#?}");
    // `fabric.atlas.get` answers the paths the current Build names (R-FA4): the birth the rename moved
    // off `<path>` is not at it.
    assert!(read.node(&node.to_string()).is_none_or(|row| row.node_id.to_string() != before["node_id"].as_str().unwrap_or_default()), "R-FA2: on the Build's Complete the atlas holds no birth of {node} at {node}: {read:#?}");
    estate.stop().await;
    let spans = estate.spans();
    let executed: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|sp| attr(sp, "outcome") == "complete" && attr(sp, "executor").starts_with("mesh2.admin.")).collect();
    assert!(!executed.is_empty(), "mesh2's primary executed the Build: {:#?}", named(&spans, "rdm.node_admin.build.update.via-dispatch"));
    let taken: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-atlas-receipt").into_iter().filter(|sp| attr(sp, "operation") == format!("node.update:{node}") || attr(sp, "operation") == format!("node.stop:{renamed}")).collect();
    assert_eq!(taken.len(), 2, "the rename's and the stop's receipts are taken into the atlas as they stream up: {taken:#?}");
}

/// CONTRACT (node-name-update.md step 1: "an admin that is not the fabric-primary carries it to the
/// fabric-primary ... The fabric-primary sends the command to the node's mesh-primary ... and, on its
/// answer that the row moved, writes the new name to the node in its atlas"; Q197, Q203, Q204:
/// node reads answer from the atlas and each op writes it): a live rpc node of mesh2, its connections
/// deleted and off its mesh, is renamed `.old` by a caller's `node.name.update` sent directly to mesh2's
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
    let renamed = node.renamed();
    let nodes = estate.nodes_obj().await;
    // The node's connections deleted and its mesh left: the precondition of a live rename.
    Estate::workflow(&format!("node.connections.delete and node.mesh.leave of {node}"), nodes.steps(vec![rdm_core::build::Step::DeleteConnections { node: node.clone() }, rdm_core::build::Step::LeaveMesh { node: node.clone() }]).await).await;
    let id = |v: &Value, k: &str| v[k].as_str().unwrap_or_default().to_string();
    let node_id: rdm_core::NodeId = serde_json::from_value(targets[0]["node_id"].clone()).expect("a node id");
    let birth = rdm_client::ExactBirth {
        target: rdm_transport::NodeTarget::ExactNode(node_id.clone()),
        node_id,
        incarnation: serde_json::from_value(targets[0]["incarnation_id"].clone()).expect("an incarnation id"),
    };
    let mesh_primary: rdm_core::NodeId = serde_json::from_value(primary["node_id"].clone()).expect("a node id");
    let reply = nodes.name_update(&birth, &mesh_primary, &rdm_transport::RouteChoice::Direct, &renamed.name()).await.unwrap_or_else(|e| panic!("node.name.update of {node} at mesh2's primary: {e}"));
    assert!(matches!(reply, rdm_core::status::StatusReply::NameUpdated { .. }), "node.name.update of {node} at mesh2's primary: {reply:?}");
    // No wait: the fabric-primary's read the moment the caller is answered.
    let read = estate.node_values().await;
    let at: Vec<String> = read.iter().filter(|n| n["node_id"] == targets[0]["node_id"]).map(|n| id(n, "name")).collect();
    estate.stop().await;
    assert_eq!(at, vec![renamed.to_string()], "the fabric-primary's node read lists the birth of {node} at {renamed} once the rename was answered: {read:#?}");
    let spans = estate.spans();
    let mp = id(primary, "name");
    let forwarded: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-forward").into_iter().filter(|sp| attr(sp, "node") == mp && attr(sp, "op") == "node.name.update").collect();
    assert_eq!(forwarded.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(), vec!["name-updated".to_string()], "mesh2's primary carried the caller's rename up to the fabric-primary once: {forwarded:#?}");
    let down: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-fabric-primary").into_iter().filter(|sp| attr(sp, "op") == "node.name.update").collect();
    assert_eq!(down.iter().map(|sp| (attr(sp, "outcome"), attr(sp, "atlas"))).collect::<Vec<_>>(), vec![("name-updated".to_string(), "renamed".to_string())], "the fabric-primary sent it down and renamed the node in its atlas: {down:#?}");
    let written: Vec<&Value> = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().filter(|sp| attr(sp, "node_id") == id(&targets[0], "node_id")).collect();
    assert_eq!(written.iter().map(|sp| (attr(sp, "node"), attr(sp, "outcome"))).collect::<Vec<_>>(), vec![(mp.clone(), "name-updated".to_string())], "mesh2's primary wrote the row once: {written:#?}");
}

/// CONTRACT (fabric-atlas.md R-FA1: the fabric-primary accepts or denies from the atlas, the gossip view
/// context only; R-FA2; node-primary-update.md "No-successor cases"): with the fabric-primary's gossip
/// view holding mesh2's non-primary node-admin stopped, that node-admin is started by the
/// fabric-primary; the moment its start streams `Complete`,
/// with no wait, a complete-topology Reconcile removing mesh2's primary, whose only other node-admin is
/// the one just started, is accepted and completes. Must NOT happen: the Reconcile refused `last-admin`
/// (`rdm.node_admin.build.reject.via-last-admin`) because the gossip view still holds the started
/// node-admin stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconcile_removing_a_primary_right_after_its_only_other_admins_start_is_accepted() {
    let test = "a_reconcile_removing_a_primary_right_after_its_only_other_admins_start_is_accepted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let targets = two_meshes(&mut estate).await;
    let (other, primary) = (path_of(&targets[2]), path_of(&targets[3]));
    let other_id: rdm_core::NodeId = serde_json::from_value(targets[2]["node_id"].clone()).expect("a node id");
    let fp = estate.fabric_view().await.fabric_primary.expect("fabric.get names the fabric-primary");
    let fp_view = estate.node_views().await.into_iter().find(|n| n.name == fp).expect("the fabric-primary is listed");
    Estate::workflow(&format!("node.stop of {other}"), estate.nodes_obj().await.stop(&other).await).await;
    // The fabric-primary's gossip view holds the stop: the birth dropped and its stop overlay held.
    estate.peers().await.fact(&fp_view, "mesh2", "the fabric-primary's view holds the stopped node-admin stopped", |v| v.birth(&other_id).is_none() && v.overlay(&other_id, "node.stop:").is_some()).await;
    Estate::workflow(&format!("node.start of {other}"), estate.nodes_obj().await.start(&other).await).await;
    // No wait: the Reconcile goes out on the start's Complete.
    let current = estate.current().await;
    let mut held = current.held(&estate.executables).unwrap_or_else(|e| panic!("{e}"));
    held.get_mut("mesh2").expect("the topology holds mesh2").remove(&primary);
    let change = rdm_client::Change::Reconcile { topology_storage_provider: None, based_on: current.based_on.clone(), nodes: held.into_values().flat_map(|m| m.into_values()).collect() };
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
    assert!(refused.is_none() && last_admin.is_empty(), "R-FA1: the Reconcile removing {primary} right after {other}'s start completed was refused: {refused:?}; last-admin spans: {last_admin:#?}");
}
