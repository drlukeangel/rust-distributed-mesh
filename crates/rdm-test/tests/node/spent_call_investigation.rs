//! i151.s2 (node-rpc.md §43; Q311: "a call whose attempts end unanswered starts node investigation on
//! that birth (the mesh-primary itself, ...)"; node-recovery.md step 1: "The mesh-primary also marks a
//! member `PendingReconnect` at once when a command to it was never sent"; step 2: "Half a floor after
//! the mark it runs node investigation").
//!
//! CONTRACT: in a one-mesh estate at a 60 s staleness floor, a node's exact runtime
//! is hung (every task but the leader held, so it neither gossips nor answers). At once the caller asks
//! the mesh-primary (here the fabric-primary too) for `node.config.update` of that node: the
//! mesh-primary's own `ApplyNodeConfig` to the hung birth spends its attempts with no answer, and the
//! mesh-primary marks the exact birth (`rdm.node_admin.node.update.via-call-failed`, sender `own-call`,
//! outcome `marked`). Its tick then investigates the birth from that mark (the provider's inspection,
//! `rdm.node_admin.node.resolve.via-provider-inspection`), half a floor after the mark, before the birth's
//! silence alone would have started it (floor and half a floor after the hold). Must NOT happen: no
//! mark, an investigation before half a floor after the mark, or the first investigation only once
//! silence passed the floor.
use rdm_client::PathName;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::ExactRuntime;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

const TEST: &str = "a_mesh_primarys_spent_call_marks_the_birth_and_its_tick_investigates_it_half_a_floor_after";
/// The staleness floor given to every node of the estate: past the ~30 s a call to a held birth takes to
/// spend its three attempts, so the mark from the spent call comes before the silence one.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primarys_spent_call_marks_the_birth_and_its_tick_investigates_it_half_a_floor_after() {
    let floor_ms = FLOOR.as_millis().to_string();
    let mut estate = Estate::bootstrap_with_env(owner(), "fabric1", "mesh1", &[("RDM_STALENESS_MS", floor_ms.as_str())]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let path = PathName::new("mesh1.node", "1").unwrap();
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let node = mesh.iter().find(|n| n.location() == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary && rdm_core::is_node_admin(&n.r#type)).cloned().expect("node.get names mesh1's primary");
    let node_id = node.node_id.to_string();

    // The hold: the birth neither gossips nor answers from here.
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(&path).await)).expect("the node's published runtime");
    let held_at = now_nano();
    let hung = rt.hang().expect("the hang is acknowledged by the OS");
    // The caller's node.config.update at the mesh-primary: its own ApplyNodeConfig to the hung birth.
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().expect("the node's birth") };
    let carrier = estate.carrier().await;
    let opts = rdm_transport::CallOptions { resend: rdm_transport::Resend::with_time_limit(Duration::from_secs(40)), ..rdm_transport::CallOptions::default() };
    let reply = rdm_client::NodeRpc::new(carrier.client()).config_update(&primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told, &opts).await;
    eprintln!("node.config.update of the hung birth answered: {reply:?}");

    // The mark, then the investigation from it.
    let mark = wait_for("the mesh-primary marks the birth its own spent call did not reach", FLOOR, || async {
        named(&estate.spans(), "rdm.node_admin.node.update.via-call-failed")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == node_id && s(&sp["attributes"]["sender"]) == "own-call" && s(&sp["attributes"]["outcome"]) == "marked" && at(sp) >= held_at)
            .cloned()
    })
    .await;
    let investigation = wait_for("the mesh-primary's tick investigates the marked birth", FLOOR * 2, || async {
        named(&estate.spans(), "rdm.node_admin.node.resolve.via-provider-inspection")
            .into_iter()
            .filter(|sp| s(&sp["attributes"]["node_id"]) == node_id && at(sp) >= held_at)
            .min_by_key(|sp| at(sp))
            .cloned()
    })
    .await;
    hung.release();
    estate.stop().await;
    let half = FLOOR.as_nanos() as u64 / 2;
    let (marked, looked) = (at(&mark), at(&investigation));
    eprintln!("trace {}: via-call-failed sender=own-call outcome=marked at +{} ms after the hold; trace {}: via-provider-inspection at +{} ms after the mark", s(&mark["trace_id"]), (marked - held_at) / 1_000_000, s(&investigation["trace_id"]), looked.saturating_sub(marked) / 1_000_000);
    assert!(looked + 1_000_000_000 >= marked + half, "the investigation runs half a floor after the mark (node-recovery.md step 2), not before: mark {marked}, investigation {looked}");
    assert!(looked < held_at + FLOOR.as_nanos() as u64 + half, "the spent call's mark started it, before silence alone would have: hold {held_at}, investigation {looked}");
    estate.record_trace_url(mark["trace_id"].as_str().unwrap_or(""));
    estate.record_trace_url(investigation["trace_id"].as_str().unwrap_or(""));
}
