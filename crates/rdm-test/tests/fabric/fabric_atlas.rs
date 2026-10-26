//! The fabric-primary's `FabricAtlas` (docs/plans/proposal/fabric-atlas.md, Q194): one view that
//! combines command admission, execution progress and gossip observations, read by Build acceptance
//! and exposed through `fabrics.atlas.get`.

use rdm_core::PathName;
use rdm_test::estate::{named, Estate, Owner};
use rdm_client::{AtlasNodeView, AtlasView};
use serde_json::{json, Value};

/// What the cells read of the atlas: `fabrics.atlas.get` through the client's own call,
/// `Fabrics::atlas_get()` (Q167's read naming; `fabrics.get` is unchanged), one row per node the
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
            (n["status"] == "ready-for-traffic" && n["stopped"] == false).then_some(n)
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
                let views_live = |n: Option<&rdm_client::NodeView>| n.filter(|n| serde_json::to_value(n).map(|v| v["status"] == "ready-for-traffic" && v["stopped"] == false).unwrap_or(false)).map(|_| ());
                estate.until_node(&node.to_string(), "the started node live at the asked admin", std::time::Duration::from_secs(60), views_live).await;
                Estate::workflow(&format!("node.stop of {node}"), estate.nodes_obj().await.stop(&node).await).await;
            }
        }
        estate.until_stopped_at_the_fabric_primary(&node).await;
        Estate::workflow(&format!("node.start of {node}"), estate.nodes_obj().await.start(&node).await).await;
        estate.until_node(&node.to_string(), "the node ready again", std::time::Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready-for-traffic" && n["stopped"] == false).then_some(())
        }).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<Value> = targets.iter().flat_map(|n| not_live_of(&spans, &path_of(n).to_string())).collect();
    assert!(refused_stops.is_empty() && refused.is_empty(), "R-FA2: the fabric-primary refused a stop sent on its node.start's Complete: {refused_stops:#?}; node-not-live spans: {refused:#?}");
}

/// CONTRACT (fabric-atlas.md R-FA4: "`fabrics.atlas.get` returns it (Q167's read naming); `fabrics.get`
/// is unchanged"): on a ready fabric of two meshes, `fabrics.atlas.get` at the fabric-primary answers
/// the atlas, one row for every node the fabric-primary's node view holds, each the same birth (node
/// id and incarnation) and status; `fabrics.get` still answers the Fabric as before. Must NOT happen:
/// the read refused or absent, a node of either mesh missing from it, or `fabrics.get` changed by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fabrics_atlas_get_answers_every_node_the_fabric_primary_holds() {
    let test = "fabrics_atlas_get_answers_every_node_the_fabric_primary_holds";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let _ = two_meshes(&mut estate).await;
    let fabric = estate.fabric_view().await;
    let views = estate.node_values().await;
    let read = atlas(&estate, "R-FA4: fabrics.atlas.get at the fabric-primary").await;
    for v in &views {
        let name = v["name"].as_str().unwrap_or_default();
        let row = read.node(name).unwrap_or_else(|| panic!("R-FA4: the atlas holds no row of {name}: {read:#?}"));
        assert_eq!((row.node_id.to_string().as_str(), row.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or_default(), status_of(row).as_str(), row.stopped), (v["node_id"].as_str().unwrap_or_default(), v["incarnation_id"].as_str().unwrap_or_default(), v["status"].as_str().unwrap_or_default(), v["stopped"] == true), "R-FA4: the atlas row of {name} is its birth as the node view holds it: {row:#?} vs {v}");
    }
    assert_eq!(read.nodes.len(), views.len(), "R-FA4: the atlas holds exactly the nodes the node view holds: {read:#?}");
    let again = estate.fabric_view().await;
    assert_eq!((&again.id, &again.name, &again.fabric_primary, &again.build_id), (&fabric.id, &fabric.name, &fabric.fabric_primary, &fabric.build_id), "fabrics.get answers the Fabric as before the atlas read");
    estate.stop().await;
}

/// CONTRACT (fabric-atlas.md R-FA2: "Command admission writes it at acceptance; execution progress
/// writes it from each attempt's receipts as they stream up `build.attempt.run`, before the caller is
/// answered `Complete`"): with the other mesh's aggregate withheld from the backbone (so no gossip
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
    let at_acceptance = atlas(&estate, "R-FA2: fabrics.atlas.get while the accepted stop's attempt is held before its dispatch").await;
    let row = at_acceptance.node(&node.to_string()).unwrap_or_else(|| panic!("R-FA2: the atlas holds no row of {node}: {at_acceptance:#?}"));
    assert_eq!(row.accepted.as_deref(), Some(format!("node.stop:{node}").as_str()), "R-FA2: the atlas names the accepted stop on {node} before its attempt runs: {row:#?}");
    assert!(!row.stopped, "R-FA2: an accepted stop whose attempt has not run does not stop the node in the atlas: {row:#?}");
    fp_door.release("atlas:claimed").await;
    Estate::complete(&mut stop).await;
    let at_complete = atlas(&estate, "R-FA2: fabrics.atlas.get on the stop's Complete").await;
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
/// interval while the other mesh's aggregates, which reach the fabric-primary after its own receipts,
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
            if row.stopped || status_of(row) != "ready-for-traffic" {
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
