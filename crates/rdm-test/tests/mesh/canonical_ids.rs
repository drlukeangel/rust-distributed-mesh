//! i143.e4.s15 process E2E: canonical Crockford60 product ids (rafka-v2 #2842).
//!
//! From the typed objects and the evidence only, on an MM fabric:
//! - the Fabric, Mesh and Node views carry canonical 12-char Crockford ids
//!   (no 32-char hex product id), every admin names the same Fabric id, and a
//!   node's transport identity is never its product identity;
//! - a node's incarnation and a Build's id are canonical Crockford ids too (R-BI5: one id type,
//!   never hex), and the Digest a peer mesh's admin hears from a restarted birth names its incarnation and node as such;
//! - every birth's ready span names its node, mesh and fabric ids with
//!   `id_format = "crockford60"`, equal to the views';
//! - a restart keeps NodeId, FabricId and the incarnation (a restarted node is the same node.id and
//!   incarnation; a re-born node is the same path.name and a new node.id), and a second restart of
//!   the same node restarts it again;
//! - an intentional replacement mints new identity: a mesh removed and
//!   created again gets a new MeshId, a node removed and created again at the
//!   same path a new NodeId; the Fabric keeps its id throughout.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, Meshes, NodeStatus};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const CROCKFORD: &str = "0123456789abcdefghjkmnpqrstvwxyz";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-identity".into(),
        subfeature: "canonical-ids".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn canonical(what: &str, v: &Value) -> String {
    let id = s(v);
    assert!(id.len() == 12 && id.chars().all(|c| CROCKFORD.contains(c)), "{what} {v} is not a canonical 12-char Crockford id");
    id
}

fn shape(meshes: &[&str], rpc: u32) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: meshes.iter().map(|m| MeshDesired::of(*m, [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, rpc)])).collect() }
}

fn names(meshes: &[&str], rpc: u32) -> BTreeSet<String> {
    meshes.iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=rpc).map(move |i| format!("{m}.rpc.{i}")))).collect()
}

async fn build(estate: &Estate, body: &FabricDesired) {
    let accepted = estate.reconcile(body).await;
    canonical("build id", &Value::String(accepted.build_id.to_string()));
}

fn json_of<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).expect("a view serializes")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle() {
    let mut estate = Estate::bootstrap(owner("every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle"), "fabric1", "mesh1").await;
    build(&estate, &shape(&["mesh1", "mesh2"], 2)).await;
    let nodes = Estate::values(&estate.settles_to_named(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await);

    // Views: canonical ids, one Fabric id, distinct mesh ids.
    let fabric = json_of(&estate.fabric_view().await);
    let fabric_id = canonical("fabric id", &fabric["id"]);
    let mesh_ids: BTreeMap<String, String> =
        fabric["meshes"].as_array().unwrap().iter().map(|m| (s(&m["name"]), canonical("mesh id", &m["id"]))).collect();
    assert_eq!(mesh_ids.len(), 2);
    assert_ne!(mesh_ids["mesh1"], mesh_ids["mesh2"]);
    for (mesh, id) in &mesh_ids {
        let meshes = estate.meshes_obj().await;
        let by_name = meshes.get(&rdm_client::Filter::new().eq("name", [mesh])).await.unwrap_or_else(|e| panic!("mesh.get [name: {mesh}]: {e}"));
        let by_id = meshes.get(&rdm_client::Filter::new().eq("id", [id])).await.unwrap_or_else(|e| panic!("mesh.get [id: {id}]: {e}"));
        assert_eq!(by_name.len(), 1, "one mesh by name: {by_name:?}");
        assert_eq!(by_name, by_id, "a mesh resolves by name or by id");
    }
    let node_ids: BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), canonical("node id", &n["node_id"]))).collect();
    assert_eq!(node_ids.values().collect::<BTreeSet<_>>().len(), node_ids.len(), "node ids are distinct");
    for n in &nodes {
        canonical("incarnation id", &n["incarnation_id"]);
    }
    for n in &nodes {
        let transport = s(&n["endpoint_id"]);
        assert!(!transport.is_empty() && transport != s(&n["node_id"]) && transport != fabric_id, "transport identity is not product identity: {n}");
    }
    for admin in nodes.iter().filter(|n| n["kind"] == "node_admin") {
        let own = json_of(&estate.fabric_view_at(&s(&admin["name"]).parse().unwrap()).await.unwrap_or_else(|| panic!("{} does not answer fabric.get for this Fabric", admin["name"])));
        assert_eq!(s(&own["id"]), fabric_id, "{} names the Fabric by the same id", admin["name"]);
    }

    // The initial fabric refuses a node restart until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // A restart, node.stop then node.start (Q183), keeps NodeId, FabricId and the incarnation: the
    // process is stopped and started.
    let before = nodes.iter().find(|n| n["name"] == "mesh2.rpc.2").unwrap().clone();
    let (a, _) = estate.stop_then_start(&"mesh2.rpc.2".parse().unwrap()).await;
    let after = estate
        .until_node("mesh2.rpc.2", "mesh2.rpc.2 ready after its restart", Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Ready && !n.stopped).map(json_of))
        .await;
    assert_eq!(after["node_id"], before["node_id"], "restart keeps the node id");
    assert_eq!(after["incarnation_id"], before["incarnation_id"], "restart keeps the incarnation");

    // A second stop and start of the same node are later attempts of the same Build: the first
    // start's finished run hands them nothing, so they stop and start the node again.
    let (_, again) = estate.stop_then_start(&"mesh2.rpc.2".parse().unwrap()).await;
    assert_eq!(again.build_id, a.build_id, "a restart is an attempt of the accepted Build: {again:?}");
    let third = estate
        .until_node("mesh2.rpc.2", "mesh2.rpc.2 ready after its second restart", Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Ready && !n.stopped).map(json_of))
        .await;
    assert_eq!(third["node_id"], before["node_id"], "a second restart keeps the node id");
    assert_eq!(third["incarnation_id"], before["incarnation_id"], "and the incarnation");
    // What mesh1.admin.1, a node-admin of the peer mesh, holds of the restarted birth: its own view,
    // from the newest version of mesh2's projection it installed (gossip.md §3.1, `MeshTopology`:
    // "Receivers install the complete snapshot atomically"). The Digest it holds names the node and
    // the incarnation the views show, as Crockford ids.
    let held = estate
        .until_view_at(&"mesh1.admin.1".parse().unwrap(), "mesh1.admin.1 holds the restarted mesh2.rpc.2 ready, same node and incarnation", Duration::from_secs(30), |views| {
            views
                .iter()
                .find(|n| n.name.to_string() == "mesh2.rpc.2" && n.status == NodeStatus::Ready)
                .map(json_of)
                .filter(|n| n["node_id"] == before["node_id"] && n["incarnation_id"] == before["incarnation_id"])
        })
        .await;
    canonical("heard Digest incarnation_id", &held["incarnation_id"]);
    canonical("heard Digest node_id", &held["node_id"]);
    let restart_build = again.build_id.to_string();

    // Replacement of a node: removed, then created again at the same path.
    let replaced = node_ids["mesh1.rpc.2"].clone();
    let mut removal = estate.nodes_obj().await.delete(&"mesh1.rpc.2".parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of mesh1.rpc.2: {e}"));
    Estate::complete(&mut removal).await;
    build(&estate, &shape(&["mesh1", "mesh2"], 2)).await;
    let regrown = Estate::values(&estate.settles_to_named(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await);
    let new_id = canonical("node id", &regrown.iter().find(|n| n["name"] == "mesh1.rpc.2").unwrap()["node_id"]);
    assert_ne!(new_id, replaced, "a recreated node is a new logical node");

    // Replacement of a mesh: removed, then created again. mesh.delete ends at mesh.deleted with
    // mesh2's last admin alive; its end is its own node.delete (mesh-delete.md steps 5-7), and only
    // that delete releases its name (R-MN1).
    build(&estate, &shape(&["mesh1"], 2)).await;
    let last = estate
        .until_view("mesh2's last admin alive after mesh.deleted, its members deleted", Duration::from_secs(60), |views| {
            let mesh2: Vec<String> = views.iter().filter(|n| n.mesh == "mesh2").map(|n| n.name.to_string()).collect();
            let alive = views.iter().any(|n| n.mesh == "mesh2" && n.status == NodeStatus::Ready);
            (mesh2.len() == 1 && alive).then(|| mesh2[0].clone())
        })
        .await;
    let mut last_delete = estate.nodes_obj().await.delete(&last.parse().unwrap()).await.unwrap_or_else(|e| panic!("the node.delete of mesh2's last admin {last} was refused: {e}"));
    Estate::complete(&mut last_delete).await;
    estate.settles_to_named(&names(&["mesh1"], 2), Duration::from_secs(30)).await;
    build(&estate, &shape(&["mesh1", "mesh2"], 2)).await;
    let recreated = Estate::values(&estate.settles_to_named(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await);
    // R-NS1 item 1 + states.md ("A mesh id is never reissued; dead is final ... a mesh created again
    // under that name is a new mesh with a new id"): the removed mesh2 was Dead, and Dead belongs to its
    // id. No admin's view states the new mesh2 Dead.
    for n in recreated.iter().filter(|n| n["kind"] == "node_admin") {
        let carrier = estate.carrier_at(&s(&n["name"]).parse().unwrap()).await;
        let m = json_of(&rdm_test::typed::one_mesh(&Meshes::new(carrier), "mesh2").await.unwrap_or_else(|e| panic!("mesh.get [name: mesh2] at {}: {e}", n["name"])).unwrap_or_else(|| panic!("{} holds mesh2", n["name"])));
        assert_ne!(s(&m["status"]), "dead", "{} states the recreated mesh2 Dead: {m}", s(&n["name"]));
    }
    let fabric_after = json_of(&estate.fabric_view().await);
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id for its lifetime");
    let m2 = fabric_after["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh2").unwrap().clone();
    let m1 = fabric_after["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh1").unwrap().clone();
    assert_ne!(canonical("mesh id", &m2["id"]), mesh_ids["mesh2"], "a mesh created again is a new mesh");
    assert_eq!(s(&m1["id"]), mesh_ids["mesh1"], "an untouched mesh keeps its id");

    estate.enter_fabric_primary().await;
    estate.stop().await;

    // The span evidence is read only now: every process flushes its spans on exit (`Estate::stop`),
    // and a span read mid-run can still sit in its process's export batch.
    // Evidence: each birth's ready span names the ids the views show.
    let spans = estate.spans();
    for n in &nodes {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == n["name"] && sp["attributes"]["incarnation_id"] == n["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", n["name"]));
        let a = &ready["attributes"];
        assert_eq!(a["id_format"], "crockford60");
        canonical("ready span incarnation_id", &a["incarnation_id"]);
        assert_eq!(s(&a["node_id"]), s(&n["node_id"]));
        assert_eq!(s(&a["fabric_id"]), fabric_id);
        assert_eq!(s(&a["mesh_id"]), mesh_ids[&s(&n["mesh"])]);
    }

    // The second restart's return reached the peer mesh's admin. mesh2's primary authored the
    // birth's Ready (`from-node-started`), and its next publication bumped mesh2's
    // topology_version (gossip.md §3.1, `MeshTopology`: "at once on a material change"). mesh1.admin.1
    // installed that version of mesh2's projection or a later one: a receiver holds the newest
    // version it installed, and a snapshot overtaken by a later one is ignored in the gossip receive
    // path (gossip.md receiver rules: `MeshTopology` order by `published_at_rafka_ms`, then
    // `topology_version`). So the peer is owed the restart's return, never each state between.
    // The publisher is matched too: the recreated mesh2's primary starts its versions again.
    let returned_at = named(&spans, "rdm.mesh.topology.update.from-node-started")
        .into_iter()
        .filter(|sp| {
            let a = &sp["attributes"];
            a["member"] == "mesh2.rpc.2" && a["incarnation_id"] == after["incarnation_id"] && a["status"] == "Ready"
        })
        .max_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0))
        .unwrap_or_else(|| panic!("mesh2's primary authored the restarted mesh2.rpc.2's Ready at incarnation {}", after["incarnation_id"]));
    let returned_ns = returned_at["start_unix_nano"].as_u64().unwrap_or(0);
    let bumped = named(&spans, "rdm.mesh.backbone.update.via-topology-version")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == returned_at["attributes"]["node"] && sp["attributes"]["mesh"] == "mesh2" && sp["start_unix_nano"].as_u64().unwrap_or(0) >= returned_ns)
        .min_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0))
        .unwrap_or_else(|| panic!("{} bumped mesh2's topology_version after the second restart's return", returned_at["attributes"]["node"]));
    let (publisher, version) = (s(&bumped["attributes"]["publisher"]), s(&bumped["attributes"]["topology_version"]).parse::<u64>().expect("a topology_version is a number"));
    let installed: Vec<String> = named(&spans, "rdm.mesh.topology.update.via-snapshot-installed")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["mesh"] == "mesh2" && s(&sp["attributes"]["publisher"]) == publisher)
        .map(|sp| s(&sp["attributes"]["topology_version"]))
        .collect();
    assert!(
        installed.iter().any(|v| v.parse::<u64>().is_ok_and(|v| v >= version)),
        "mesh1.admin.1 installed mesh2's projection by {publisher} at topology_version {version} (the second restart's return) or later: it installed {installed:?}"
    );

    // The removed mesh2's last admin published MeshStatus Dead while it lived, and it left with MeshLeft;
    // after that nothing publishes mesh2 Dead: the recreated mesh2's primary authors its new mesh's own
    // status and restates no heard status as its own (gossip.md frame catalog, MeshStatus).
    let left_at = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left-receipt").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == mesh_ids["mesh2"]).map(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0)).min().expect("the removed mesh2 left with a MeshLeft receipt");
    let dead_after: Vec<String> = named(&spans, "rdm.mesh.fabric.update.via-status-send")
        .into_iter()
        .filter(|sp| sp["attributes"]["scope"] == "mesh:mesh2" && sp["attributes"]["status"] == "dead" && sp["start_unix_nano"].as_u64().unwrap_or(0) > left_at)
        .map(|sp| format!("{}@{}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["changed_at_rafka_ms"])))
        .collect();
    assert!(dead_after.is_empty(), "MeshStatus Dead for mesh2 was sent after the removed mesh2 left: {dead_after:?}");

    let re_born = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["incarnation_id"] == after["incarnation_id"])
        .expect("the restarted birth reports ready");
    assert_eq!(s(&re_born["attributes"]["fabric_id"]), fabric_id, "restart keeps the fabric id");
    assert_eq!(s(&re_born["attributes"]["node_id"]), s(&before["node_id"]));
    // A second restart of the same node ran its own StartNode: none reused.
    let ran = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| {
            let at = &sp["attributes"];
            s(&at["build_id"]) == restart_build && at["node"] == "mesh2.rpc.2" && at["step"] == "StartNode" && at["outcome"] == "complete"
        })
        .count();
    assert!(ran >= 2, "each restart ran its own StartNode, none reused: {ran} complete StartNode steps under {restart_build}");

    // R-MN1: mesh2's removal deleted every roster birth (each member's row by its node.delete under
    // the mesh.delete, the last admin's by its own node.delete, the fabric-primary its executor) and
    // wrote the mesh row Dead; mesh2 created again claimed mesh2.admin.1 over the Deleted row, its
    // config seeded from it, under a new MeshId.
    let leave = format!("mesh.delete:{}", mesh_ids["mesh2"]);
    let deleted = named(&spans, "rdm.node_admin.node.delete.via-node-row");
    for n in regrown.iter().filter(|n| n["mesh"] == "mesh2") {
        let row = deleted.iter().find(|sp| {
            let a = &sp["attributes"];
            (s(&a["operation"]) == leave || s(&a["operation"]) == format!("node.delete:{}", s(&n["name"]))) && a["subject"] == n["name"] && s(&a["node_id"]) == s(&n["node_id"]) && a["outcome"] == "row-deleted"
        });
        assert!(row.is_some_and(|sp| sp["attributes"]["outcome"] == "row-deleted"), "{}'s row is written Deleted by mesh2's removal: {row:?}", n["name"]);
    }
    let dead = named(&spans, "rdm.node_admin.mesh.update.via-dead-row");
    assert!(dead.iter().any(|sp| s(&sp["attributes"]["mesh_id"]) == mesh_ids["mesh2"] && sp["attributes"]["outcome"] == "applied"), "the old mesh2's row is written Dead: {dead:?}");
    let old_admin = regrown.iter().find(|n| n["name"] == "mesh2.admin.1").unwrap();
    let new_admin = recreated.iter().find(|n| n["name"] == "mesh2.admin.1").unwrap();
    assert_ne!(new_admin["node_id"], old_admin["node_id"], "the recreated mesh's admin is a new node");
    let claim = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| {
        let a = &sp["attributes"];
        a["subject"] == "mesh2.admin.1" && a["moment"] == "claim" && s(&a["node_id"]) == s(&new_admin["node_id"])
    });
    let claim = claim.unwrap_or_else(|| panic!("the recreated mesh2.admin.1 {} claims its name", new_admin["node_id"]));
    assert_eq!(claim["attributes"]["outcome"], "row-seeded", "{claim}");
    assert_eq!(s(&claim["attributes"]["seeded_from"]), s(&old_admin["node_id"]), "seeded from the old mesh2.admin.1's Deleted row: {claim}");
    assert_eq!(s(&new_admin["mesh"]), "mesh2");
    // node-identity-and-state.md §2.7 + R-MN1 change 4: a mesh's name is claimed at its key, by its
    // writer (Q109), before anything else of the mesh is written (CLAUDE.md), at CreateMesh by the
    // fabric-primary as the acting primary of a mesh with none (R13); the removed mesh2's Mesh row is
    // written Dead, which releases the name; the recreated mesh2 claims the released name under its new
    // MeshId, before the recreated mesh2.admin.1's row is claimed, by the same
    // writer. No mesh's claim conflicts with itself across creation and a re-claim.
    let claims = named(&spans, "rdm.node_admin.mesh.create.via-mesh-row");
    let claim_of = |id: &str| claims.iter().find(|sp| sp["attributes"]["mesh"] == "mesh2" && s(&sp["attributes"]["mesh_id"]) == id && sp["attributes"]["moment"] == "claim" && sp["attributes"]["outcome"] == "row-written").copied();
    let old_claim = claim_of(&mesh_ids["mesh2"]).unwrap_or_else(|| panic!("the removed mesh2's name was claimed: {claims:?}"));
    assert_eq!(old_claim["attributes"]["outcome"], "row-written", "{old_claim}");
    assert!(
        dead.iter().any(|sp| s(&sp["attributes"]["mesh_id"]) == mesh_ids["mesh2"] && sp["attributes"]["mesh_row"] == "dead-written"),
        "the removed mesh2's Mesh row is written Dead, releasing its name: {dead:?}"
    );
    let new_claim = claim_of(&s(&m2["id"])).unwrap_or_else(|| panic!("the recreated mesh2's name is claimed: {claims:?}"));
    assert_eq!(new_claim["attributes"]["outcome"], "row-written", "{new_claim}");
    // Whole records (storage-whole-records.md): the Dead write released the removed mesh2's name claim,
    // and its record stands Dead at its own id; the recreated mesh2 claims the released name, so no
    // live claim is taken over.
    assert_eq!(s(&new_claim["attributes"]["over"]), "", "the recreated mesh2 claims the name the removed mesh2's Dead write released: {new_claim}");
    assert_eq!(new_claim["attributes"]["node"], claim["attributes"]["node"], "the recreated mesh2's name is claimed by the writer of its first admin's row, the fabric-primary: {new_claim} {claim}");
    assert!(new_claim["end_unix_nano"].as_u64().unwrap() <= claim["start_unix_nano"].as_u64().unwrap(), "the recreated mesh2's name claim ends before mesh2.admin.1's row is claimed: {new_claim} {claim}");
    assert!(claims.iter().all(|c| c["attributes"]["outcome"] == "row-written" || c["attributes"]["outcome"] == "row-held-by-mesh"), "no mesh's claim is refused: {claims:?}");
    eprintln!("trace {}: rdm.node_admin.mesh.create.via-mesh-row mesh=mesh2 mesh_id={} outcome=row-written over={}", s(&new_claim["trace_id"]), s(&m2["id"]), mesh_ids["mesh2"]);
    let ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["incarnation_id"] == new_admin["incarnation_id"]).expect("the recreated mesh2.admin.1 reports ready");
    assert_eq!(s(&ready["attributes"]["mesh_id"]), s(&m2["id"]), "under the new MeshId");
    // A mesh created again is a new mesh (R-MN1): the removed mesh's births deleted at mesh-left,
    // so its first admin enters from none of them, and each member is a create over the Deleted
    // rows, never a replace of a removed birth.
    let old_ids: BTreeSet<String> = regrown.iter().filter(|n| n["mesh"] == "mesh2").map(|n| s(&n["node_id"])).collect();
    let swept_old: Vec<&Value> = named(&spans, "rdm.node_admin.node.reject.via-entry-sweep-no-reply").into_iter().filter(|sp| old_ids.contains(&s(&sp["attributes"]["node_id"]))).collect();
    assert!(swept_old.is_empty(), "the recreated mesh's first admin holds no birth of the removed mesh: {swept_old:?}");
    // A create whose view holds a removed birth at its path investigates it (admin.rs create -> seat_revision.rs
    // `via-path-investigation`, the incarnation it investigated): no create of the recreated mesh investigates one.
    let old_incarnations: BTreeSet<String> = regrown.iter().filter(|n| n["mesh"] == "mesh2").map(|n| s(&n["incarnation_id"])).collect();
    let targeted_old: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.delete.via-path-investigation").into_iter().filter(|sp| old_incarnations.contains(&s(&sp["attributes"]["incarnation"]))).collect();
    assert!(targeted_old.is_empty(), "no member of the recreated mesh is created over a removed birth: {targeted_old:?}");
}
