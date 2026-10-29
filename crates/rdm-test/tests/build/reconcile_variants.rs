// Why missed: see crates/rdm-admin/tests/reconcile_variants.rs. In short: every cell's first request
// after day 0 grew the fabric, so the Day-0 admin's entry (naming no executable) met a request naming
// the executable its row records only beside a resize, and no estate ever submitted that delta alone
// (the topology `Current::read` returns carries no executable at any path, so the harness names every
// held path from its row; only the Day-0 admin's accepted entry truly names none);
// and no estate had the fabric's change finish while a held run with nothing to execute was in flight,
// which only a fabric whose every member is already ready (the Day-0 admin alone) does.
//
//! product=mesh, feature=build, subfeature=reconcile-variants, rung=SN/MM, provider=process.
//!
//! The Reconcile variant matrix on a real estate (the functional rows are rdm-admin
//! tests/reconcile_variants.rs and `fabric_status::tests::a_reconcile_puts_the_fabric_back_to_pending_only_for_a_resize_or_an_executable_replacement`).
//! A one-mesh fabric (the Day-0 admin alone, then grown) and a two-mesh fabric each take, one Build at
//! a time, every variant of the complete-topology request: the unchanged request (each held path naming
//! none), the Day-0 admin named on the executable its row records (its entry names none), every path
//! named on the executable its entry names, tags only, config only, storage only, a new path, another
//! executable (a replacement), a removed path. A placement-only request is the unchanged one:
//! `PlacementMeta` declares no constraint. The tags, the config and the storage each reach the node's
//! row, the Build applying every property that differs from it (build.md §1, Q166): the tags its tag rows,
//! the config as `node.config.update` applies a map (the live node carries it), the storage onto the row.
//!
//! Every row (states.md "Resize and executable replacement"; Q154, Q158): the Build is Complete within
//! the bound and the fabric holds ready after it; a resize or a replacement puts the fabric
//! back to pending at its acceptance, and a request that is neither (the same executables, the unchanged
//! request, tags only, config only, storage only: proposal executable-bindings-at-create.md rule 5 re-births a path
//! for a storage or placement change only "with its executable") leaves it ready, read from
//! the `via-state-reset` spans. Must NOT happen: a
//! Build never Complete (Pending at attempt 1 with no step) after a request that changed nothing the
//! fabric runs (Rafka 141).

use rdm_client::{BuildFrame, Change, LaunchKind as NodeKind, NodeView, PathName, ScopeStatus};
use rdm_core::binding::{sha256_file, Binding};
use rdm_core::meta::{NodeSpec, PersistentDisposition, StorageMeta};
use rdm_test::estate::{Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

/// A Build of this matrix is Complete within this bound, its births' launches and check-ins included.
const BOUND: Duration = Duration::from_secs(180);

fn owner(test: &str, rung: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "build".into(), subfeature: "reconcile-variants".into(), rung: rung.into(), provider: "process".into(), test: test.into() }
}

/// What the pages say a variant does to a ready fabric at its acceptance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Back {
    ToPending,
    Stays,
}

/// The request a caller builds from the current read: every path of the current Build's accepted
/// topology with its entry's create spec, each held path naming no executable (the read's topology carries
/// none; the compiler keeps each held entry's) and the tags and config its live row carries (the read's
/// topology carries neither; the Build applies every property that differs from the row, build.md §1),
/// so the request is the accepted topology unchanged.
async fn verbatim(estate: &Estate) -> (rdm_client::BuildId, Vec<NodeSpec>, Vec<NodeView>) {
    let current = estate.current().await;
    let live = |p: &PathName| current.rows.iter().find(|n| &n.name == p && !matches!(n.status, rdm_client::NodeStatus::Deleted | rdm_client::NodeStatus::Dead));
    let nodes = current
        .topology
        .meshes
        .values()
        .flat_map(|m| m.spec_by_path.values())
        .map(|s| match live(&s.path) {
            Some(row) => NodeSpec { executable: None, tags: row.tags.clone(), config: row.config.clone(), ..s.clone() },
            None => NodeSpec { executable: None, ..s.clone() },
        })
        .collect();
    (current.based_on, nodes, current.rows)
}

fn at<'a>(request: &'a mut [NodeSpec], path: &str) -> &'a mut NodeSpec {
    request.iter_mut().find(|s| s.path.to_string() == path).unwrap_or_else(|| panic!("the request lists {path}"))
}

/// The executable the live row at `path` records.
fn row_executable(rows: &[NodeView], path: &str) -> Binding {
    rows.iter()
        .find(|n| n.name.to_string() == path && !matches!(n.status, rdm_client::NodeStatus::Deleted | rdm_client::NodeStatus::Dead))
        .and_then(|n| n.executable.clone())
        .unwrap_or_else(|| panic!("the row at {path} records its executable"))
}

/// `b` copied under the estate's root with one byte appended: the same program under another sha256.
fn another_build(estate: &Estate, b: &Binding, tag: &str) -> Binding {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    let dir = estate.root.join("executables");
    std::fs::create_dir_all(&dir).unwrap();
    let to = dir.join(format!("{}-{tag}", b.launch_id));
    std::fs::copy(&b.executable, &to).unwrap();
    std::fs::OpenOptions::new().append(true).open(&to).unwrap().write_all(&[0]).unwrap();
    std::fs::set_permissions(&to, std::fs::Permissions::from_mode(0o755)).unwrap();
    Binding { sha256: sha256_file(&to).unwrap(), executable: to, ..b.clone() }
}

/// The record of the live node at `path` on the fabric's store root, and its view (`fabric.nodes.get`).
async fn node_row(estate: &Estate, path: &str) -> (Value, NodeView) {
    let node = estate.nodes_obj().await.get(&rdm_client::Filter::new()).await.unwrap_or_else(|e| panic!("node.get: {e}")).into_iter().find(|n| n.name.to_string() == path).unwrap_or_else(|| panic!("fabric.nodes.get answers {path}"));
    let stored: Value = estate.node_record(node.node_id.as_str()).unwrap_or_else(|| panic!("the record of {path} is not in nodes.jsonl"));
    (stored["record"].clone(), node)
}

/// One Build of the matrix: `request` submitted on `based_on`, Complete within [`BOUND`], and the fabric
/// ready after it. A Build with no terminal frame within the bound is the defect, named with
/// what `build.get` and `fabric.get` say of it then.
async fn row(estate: &Estate, what: &str, based_on: rdm_client::BuildId, request: Vec<NodeSpec>, back: Back, rows: &mut Vec<(String, String, Back)>) {
    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await;
    let id = stream.build_id().clone();
    let done = match tokio::time::timeout(BOUND, estate.follow(stream)).await {
        Ok(done) => done,
        Err(_) => {
            let got = estate.carrier().await.build(&id).await;
            let fabric = estate.try_fabric_view().await;
            panic!("{what}: Build {id} is not Complete within {BOUND:?}: build.get [id] {got:?}; fabric.get {fabric:?}");
        }
    };
    assert!(matches!(done.frames.last(), Some(BuildFrame::Complete { .. })), "{what}: Build {id} ends Complete: {:?}", done.frames.last());
    estate.until_fabric_ready(Duration::from_secs(90)).await;
    let fabric = estate.fabric_view().await;
    assert_eq!(fabric.status, ScopeStatus::Ready, "{what}: the fabric is ready after Build {id}");
    rows.push((what.to_string(), id.to_string(), back));
}

/// After the estate stopped: each row's Build put the fabric back to pending (a `via-state-reset` span
/// naming it) exactly when the pages say; every row's outcome is recorded as an artifact.
fn resets_as_the_pages_say(estate: &Estate, rows: &[(String, String, Back)]) {
    let spans = estate.spans();
    let reset = |id: &str| spans.iter().any(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-state-reset" && sp["attributes"]["build_id"] == id);
    let mut seen = Vec::new();
    let mut red = Vec::new();
    for (what, id, back) in rows {
        let got = if reset(id) { Back::ToPending } else { Back::Stays };
        seen.push(json!({ "variant": what, "build_id": id, "pages": format!("{back:?}"), "fabric": format!("{got:?}") }));
        if *back != got {
            red.push(format!("{what} (Build {id}): the pages say {back:?}, the fabric {got:?}"));
        }
    }
    estate.artifact("reconcile-variants.json", &Value::Array(seen));
    assert!(red.is_empty(), "rows whose fabric differs from the pages:\n  {}", red.join("\n  "));
}

/// The variants that act on `mesh`'s rpc paths, in order, from a fabric where `mesh` holds `<mesh>.rpc.1`
/// on the estate's executable: tags only, storage only, a new path, another executable, a removed path.
async fn rpc_variants(estate: &Estate, mesh: &str, rows: &mut Vec<(String, String, Back)>) {
    let rpc1 = format!("{mesh}.rpc.1");
    let rpc2 = format!("{mesh}.rpc.2");
    let (based_on, mut r, _) = verbatim(estate).await;
    at(&mut r, &rpc1).tags = ["zone_a".to_string()].into();
    row(estate, &format!("{mesh}: tags only"), based_on, r, Back::Stays, rows).await;
    let (record, node) = node_row(estate, &rpc1).await;
    assert_eq!(record["tags"], json!(["zone_a"]), "{mesh}: the tags reach {rpc1}'s row: {record}");
    assert_eq!(node.tags, ["zone_a".to_string()].into(), "{mesh}: and fabric.nodes.get answers them");

    let (based_on, mut r, _) = verbatim(estate).await;
    let zone: std::collections::BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
    at(&mut r, &rpc1).config = zone.clone();
    row(estate, &format!("{mesh}: config only"), based_on, r, Back::Stays, rows).await;
    let (record, _) = node_row(estate, &rpc1).await;
    assert_eq!(record["config"], json!({"zone": "a"}), "{mesh}: the config reaches {rpc1}'s row: {record}");
    // The node applied it before the Build's operation completed (row first, then the node); its digest
    // carries it from its next gossip, which mesh.nodes.get answers a live node's config from.
    estate.until_node(&rpc1, &format!("{rpc1} carries the config"), Duration::from_secs(30), |n| n.filter(|n| n.config == zone).map(|_| ())).await;

    let (based_on, mut r, _) = verbatim(estate).await;
    at(&mut r, &rpc1).storage_persistence = StorageMeta::Persistent { disposition: PersistentDisposition::Preserve };
    row(estate, &format!("{mesh}: storage only"), based_on, r, Back::Stays, rows).await;
    let (record, node) = node_row(estate, &rpc1).await;
    assert_eq!(record["storage_persistence"], json!({"persistence": "persistent", "disposition": "preserve"}), "{mesh}: the storage reaches {rpc1}'s row: {record}");
    assert_eq!((record["config"].clone(), node.config), (json!({"zone": "a"}), zone), "{mesh}: a storage-only change leaves the config as it is");

    let (based_on, mut r, _) = verbatim(estate).await;
    r.push(rdm_test::executables::created_spec(rpc2.parse().unwrap(), estate.executables.of(NodeKind::RpcNode)));
    row(estate, &format!("{mesh}: a new path"), based_on, r, Back::ToPending, rows).await;
    estate.until_node(&rpc2, &format!("{rpc2} ready"), BOUND, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready).map(|_| ())).await;

    let (based_on, mut r, rows_now) = verbatim(estate).await;
    let next = another_build(estate, &row_executable(&rows_now, &rpc2), &format!("{mesh}-next"));
    at(&mut r, &rpc2).executable = Some(next.clone());
    row(estate, &format!("{mesh}: another executable"), based_on, r, Back::ToPending, rows).await;
    let replaced = estate.until_node(&rpc2, &format!("{rpc2} on the new executable"), BOUND, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.executable.as_ref() == Some(&next)).cloned()).await;
    assert_eq!(replaced.executable.as_ref(), Some(&next), "{rpc2} runs the executable its request named");

    let (based_on, mut r, _) = verbatim(estate).await;
    r.retain(|s| s.path.to_string() != rpc2);
    row(estate, &format!("{mesh}: a removed path"), based_on, r, Back::ToPending, rows).await;
}

/// The Day-0 admin's rows: the unchanged request (its entry naming none), then the request naming the
/// executable its row records, then every path named on what its entry names.
async fn day0_variants(estate: &Estate, rows: &mut Vec<(String, String, Back)>) {
    let (based_on, r, _) = verbatim(estate).await;
    row(estate, "the unchanged request (each held path naming none)", based_on, r, Back::Stays, rows).await;

    let (based_on, mut r, rows_now) = verbatim(estate).await;
    at(&mut r, "mesh1.admin.1").executable = Some(row_executable(&rows_now, "mesh1.admin.1"));
    row(estate, "the Day-0 admin named on the executable its row records", based_on, r, Back::Stays, rows).await;

    // Every entry names what its row records now (the Day-0 admin's from the row above, every created
    // path's from its create): the harness's own request, each path on the executable its row records.
    let current = estate.current().await;
    let r = current.held(&estate.executables).expect("every path is named").into_values().flat_map(|m| m.into_values()).collect();
    row(estate, "every path named on the executable its entry names", current.based_on, r, Back::Stays, rows).await;
}

/// CONTRACT (states.md "Resize and executable replacement"; Q154, Q158; proposal
/// executable-bindings-at-create.md rule 1): on a one-mesh fabric that is the Day-0 admin alone, every
/// Reconcile variant, one Build at a time, is Complete within the bound with the fabric ready
/// after it, and puts the fabric back to pending exactly when it is a resize or a replacement. Must NOT
/// happen: the request naming the executable the Day-0 admin's row records (its entry names none) left
/// Pending at attempt 1 with no step for ever (Rafka 141).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reconcile_variant_on_a_day0_fabric_completes_with_the_fabric_ready() {
    let test = "every_reconcile_variant_on_a_day0_fabric_completes_with_the_fabric_ready";
    let mut estate = Estate::bootstrap(owner(test, "SN"), "fabric1", "mesh1").await;
    estate.until_fabric_ready(BOUND).await;
    let mut rows = Vec::new();
    day0_variants(&estate, &mut rows).await;
    // mesh1.rpc.1, so the rpc variants have a held path to act on.
    let (based_on, mut r, _) = verbatim(&estate).await;
    r.push(rdm_test::executables::created_spec("mesh1.rpc.1".parse().unwrap(), estate.executables.of(NodeKind::RpcNode)));
    row(&estate, "mesh1: a new path, mesh1.rpc.1", based_on, r, Back::ToPending, &mut rows).await;
    rpc_variants(&estate, "mesh1", &mut rows).await;
    estate.stop().await;
    resets_as_the_pages_say(&estate, &rows);
}

/// CONTRACT (as above, on two meshes): mesh2 is created by a request that keeps the Day-0 admin's entry
/// naming no executable; then every variant, the Day-0 admin's in mesh1 and the rpc paths' in mesh2, is
/// Complete within the bound with the fabric ready after it, and puts the fabric back to
/// pending exactly when it is a resize or a replacement. Must NOT happen: a Build never Complete after a
/// request that changed nothing the fabric runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reconcile_variant_on_a_two_mesh_fabric_completes_with_the_fabric_ready() {
    let test = "every_reconcile_variant_on_a_two_mesh_fabric_completes_with_the_fabric_ready";
    let mut estate = Estate::bootstrap(owner(test, "MM"), "fabric1", "mesh1").await;
    estate.until_fabric_ready(BOUND).await;
    let mut rows = Vec::new();
    let (based_on, mut r, _) = verbatim(&estate).await;
    for (p, kind) in [("mesh2.admin.1", NodeKind::NodeAdmin), ("mesh2.rpc.1", NodeKind::RpcNode)] {
        r.push(rdm_test::executables::created_spec(p.parse().unwrap(), estate.executables.of(kind)));
    }
    row(&estate, "mesh2 created, the Day-0 admin's entry kept naming none", based_on, r, Back::ToPending, &mut rows).await;
    estate.settles_to(&[("mesh1", 1, 0), ("mesh2", 1, 1)], BOUND).await;
    day0_variants(&estate, &mut rows).await;
    rpc_variants(&estate, "mesh2", &mut rows).await;
    estate.stop().await;
    resets_as_the_pages_say(&estate, &rows);
}
