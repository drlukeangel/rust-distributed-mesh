//! Q338 (node-investigation.md: "a call whose attempts end unanswered marks its birth at the primary
//! above it (the caller itself when it is that primary; any other caller reports the birth to it with
//! Status `ReportCallFailed`), and the primary investigates that birth as it investigates silence";
//! Caller: "the fabric-primary for a mesh-primary"; node-recovery.md step 2: "For an unheard
//! mesh-primary the fabric-primary runs it").
//!
//! CONTRACT: in a two-mesh estate at a 60 s staleness floor (mesh1 holds the fabric-primary alone;
//! mesh2 its mesh-primary and one node), mesh2's mesh-primary's exact runtime is hung (it neither
//! gossips nor answers). At once the caller asks mesh2's node for `fabric.get`, which it forwards to
//! its mesh-primary: that call spends its attempts with no answer, and the member reports it to the
//! fabric-primary (`rdm.mesh.node.update.via-call-failed-report`, outcome `answered`, `primary_above`
//! the fabric-primary). The fabric-primary marks the mesh-primary
//! (`rdm.node_admin.node.update.via-call-failed`, sender the node, outcome `marked`), and its
//! primary watch investigates it from that mark (`rdm.node_admin.primary.update.via-investigation`,
//! trigger `call-failed`) once mesh2's aggregate is unheard past the floor (Q335 as amended, D66: with
//! it heard, the successor that publishes it investigates), by the watch's next pass: at most the floor
//! plus one gossip interval after the hold. Must NOT happen: no report, a report to anyone but the
//! fabric-primary, no mark, or an investigation later than that bound.
use rdm_client::{NodeView, PathName};

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::ExactRuntime;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

const TEST: &str = "a_members_spent_call_to_its_mesh_primary_is_reported_to_the_fabric_primary_which_marks_and_investigates_it";
/// The staleness floor given to every node of the estate: past the ~30 s a call to a held birth takes to
/// spend its three attempts, so the mark is recorded before the mesh's aggregate is unheard past it.
const FLOOR: Duration = Duration::from_secs(60);

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-recovery".into(),
        subfeature: "spent-call".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: TEST.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn now_nano() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
fn one(views: &[NodeView], what: &str, f: impl Fn(&NodeView) -> bool) -> NodeView {
    views.iter().find(|n| f(n)).cloned().unwrap_or_else(|| panic!("{what}: {views:#?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_members_spent_call_to_its_mesh_primary_is_reported_to_the_fabric_primary_which_marks_and_investigates_it() {
    let floor_ms = FLOOR.as_millis().to_string();
    let mut estate = Estate::bootstrap_with_env(owner(), "fabric1", "mesh1", &[("RDM_STALENESS_MS", floor_ms.as_str())]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    let views = estate.node_views().await;
    let fp = one(&views, "a fabric-primary", |n| n.is_fabric_primary);
    let mp = one(&views, "mesh2's mesh-primary", |n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary);
    let rpc = one(&views, "mesh2's node", |n| n.mesh == "mesh2" && n.r#type == "node");
    let (fp_id, mp_id, rpc_id) = (fp.node_id.to_string(), mp.node_id.to_string(), rpc.node_id.to_string());

    // The hold: mesh2's mesh-primary neither gossips nor answers from here.
    let mp_path: PathName = mp.name.clone();
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(&mp_path).await)).expect("the mesh-primary's published runtime");
    let held_at = now_nano();
    let hung = rt.hang().expect("the hang is acknowledged by the OS");
    // The member's own call to its mesh-primary: fabric.get, which it forwards there.
    let carrier = estate.carrier().await;
    let opts = rdm_transport::CallOptions { resend: rdm_transport::Resend::with_time_limit(Duration::from_secs(50)), ..rdm_transport::CallOptions::default() };
    let (out, _) = carrier.client().call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(rpc.node_id.clone()), &rdm_core::status::StatusRequest::GetFabric, &opts).await;
    eprintln!("fabric.get at the member ended {}", out.name());

    let report = wait_for("the member reports its spent call to the fabric-primary", FLOOR, || async {
        named(&estate.spans(), "rdm.mesh.node.update.via-call-failed-report")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == rpc_id && s(&sp["attributes"]["receiver_node_id"]) == mp_id && at(sp) >= held_at)
            .cloned()
    })
    .await;
    let mark = wait_for("the fabric-primary marks the mesh-primary the member reported", FLOOR, || async {
        named(&estate.spans(), "rdm.node_admin.node.update.via-call-failed")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == mp_id && s(&sp["attributes"]["sender"]) == rpc_id && at(sp) >= held_at)
            .cloned()
    })
    .await;
    // The fabric-primary investigates a peer mesh's primary once that mesh's aggregate is unheard past the
    // floor (Q335 as amended, D66: with it heard, the successor that publishes it investigates): the mark is
    // recorded first, and the investigation follows on the watch's next pass past the floor.
    let bound = FLOOR + rdm_core::cadence::gossip_interval();
    let investigation = wait_for("the fabric-primary's primary watch investigates the marked mesh-primary", bound, || async {
        named(&estate.spans(), "rdm.node_admin.primary.update.via-investigation")
            .into_iter()
            .filter(|sp| s(&sp["attributes"]["suspect_node_id"]) == mp_id && at(sp) >= held_at)
            .min_by_key(|sp| at(sp))
            .cloned()
    })
    .await;
    hung.release();
    estate.stop().await;
    eprintln!(
        "report trace {} outcome={} primary_above={}; mark trace {} outcome={} at +{} ms after the hold; investigation trace {} trigger={} finding={} at +{} ms after the mark",
        s(&report["trace_id"]), s(&report["attributes"]["outcome"]), s(&report["attributes"]["primary_above"]),
        s(&mark["trace_id"]), s(&mark["attributes"]["outcome"]), (at(&mark) - held_at) / 1_000_000,
        s(&investigation["trace_id"]), s(&investigation["attributes"]["trigger"]), s(&investigation["attributes"]["finding"]), at(&investigation).saturating_sub(at(&mark)) / 1_000_000,
    );
    assert_eq!(s(&report["attributes"]["primary_above"]), fp_id, "reported to the fabric-primary, the primary above a mesh-primary: {report}");
    assert_eq!(s(&report["attributes"]["outcome"]), "answered", "the fabric-primary answered the report: {report}");
    assert_eq!(s(&mark["attributes"]["outcome"]), "marked", "{mark}");
    assert_eq!(s(&investigation["attributes"]["trigger"]), "call-failed", "investigated from the spent call's mark: {investigation}");
    assert!(at(&investigation) >= at(&mark), "the investigation follows the mark");
    assert!(at(&investigation) <= held_at + bound.as_nanos() as u64, "investigated by the watch's next pass once the mesh's aggregate is unheard past the floor (D66): hold {held_at}, investigation {}", at(&investigation));
    for sp in [&report, &mark, &investigation] {
        estate.record_trace_url(sp["trace_id"].as_str().unwrap_or(""));
    }
}
