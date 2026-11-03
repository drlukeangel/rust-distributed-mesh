//! i151.s5, the proof of one `Resend` for every Node RPC call (node-rpc.md §34, §43; Q311-Q315;
//! fabric-node-hooks.md "on_call_failed", "on_node_investigated"; node-recovery.md steps 1-3).
//!
//! CONTRACT: in a one-mesh estate whose node-admins are the testkit's (`faulted-node-admin`: the testkit
//! application's `on_call_failed` and `on_node_investigated`, each recording a span) at a 60 s staleness
//! floor, an rpc member holding no role is hung (`ExactRuntime::hang`: every task but the leader held, so
//! it neither gossips nor answers, and the provider reads it running). At once the caller asks the
//! mesh-primary for `node.config.update` of that member, so the mesh-primary's own `ApplyNodeConfig` call
//! goes to the hung birth. In order:
//! 1. that call ends with its attempts spent, after three attempts (`rdm.node_rpc.request.update.via-call`,
//!    `attempts` 3, `delivery` `attempts-spent`, outcome `NotSent` or `Indeterminate`);
//! 2. `on_call_failed` is called once for it, under the call's span, with both births (the mesh-primary's
//!    the sender, the hung birth the receiver) and the call's stored end as the reason
//!    (`rdm.mesh.node.update.via-on-call-failed`, the application's `rdm.testkit.app.update.via-on-call-failed`);
//! 3. the mesh-primary marks the birth from its own spent call (`via-call-failed`, sender `own-call`,
//!    outcome `marked`), its tick investigates it from that mark (`via-provider-inspection`) before
//!    silence alone would have, and submits its re-create on the verdict `running` (`via-node-recovery`);
//!    the member is re-created, ready, as a new node.id at the same `(path, name)`;
//! 4. `on_node_investigated` is called once for that birth, with verdict `running` and next `recreate`
//!    (`rdm.node_admin.node.update.via-on-node-investigated`, the application's
//!    `rdm.testkit.app.update.via-on-node-investigated`), under the span that records the verdict and
//!    opens the re-create: "a re-create may be under way when the application's hook runs"
//!    (fabric-node-hooks.md "on_node_investigated"), so the notice follows the investigation and the
//!    submit, and orders nothing against the re-create's end; sent from the write of the verdict's one
//!    `rdm.node.investigated` event of the birth's node.id on the fabric's store root, by the mesh-primary
//!    (node-investigation.md, build.md §5).
//!
//! Must NOT happen: a call to the hung birth that ends before its third attempt, a second
//! `on_call_failed` for the one call, `on_call_failed` before the call ended, an investigation started
//! by silence instead of the spent call, the member held, or a second `on_node_investigated` for the birth.
use rdm_client::{NodeStatus, PathName};

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::process_faults::ExactRuntime;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

const TEST: &str = "a_call_to_a_hung_birth_spends_three_attempts_tells_on_call_failed_and_the_birth_is_investigated_and_re_created";
/// The staleness floor given to every node of the estate: past the ~30 s a call to a hung birth takes
/// to spend its three attempts, so the mark from the spent call comes before the silence one.
const FLOOR: Duration = Duration::from_secs(60);
const SETTLE: Duration = Duration::from_secs(120);

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-rpc".into(),
        subfeature: "resend".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: TEST.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}
fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}
fn now_nano() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
fn ms(from: u64, to: u64) -> i64 {
    (to as i64 - from as i64) / 1_000_000
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_to_a_hung_birth_spends_three_attempts_tells_on_call_failed_and_the_birth_is_investigated_and_re_created() {
    let floor_ms = FLOOR.as_millis().to_string();
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on_with_env(owner(), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"], &[("RDM_STALENESS_MS", floor_ms.as_str())]).await.expect("the testkit binding set is accepted");
    estate.resize_shape("fabric1", &[("mesh1", 1, 2)]).await;
    let views = estate.settles_to(&[("mesh1", 1, 2)], SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary = views.iter().find(|n| n.r#type == "node-admin" && n.is_primary).cloned().expect("the view names mesh1's mesh-primary");
    // An rpc member holding no role: a node-type primary runs node.primary.update before its own stop
    // or delete (lifecycles/node-connections-delete.md step 0), which is not the member workflow here.
    let node = views.iter().filter(|n| n.r#type == "node" && !n.is_primary && !n.is_fabric_primary).min_by_key(|n| n.name.clone()).cloned().expect("an rpc member of mesh1 that holds no role");
    let path: PathName = node.location();
    let node_id = node.node_id.to_string();
    let incarnation = node.incarnation_id.clone().expect("the member's birth");
    let primary_id = primary.node_id.to_string();

    // The hang: the birth neither gossips nor answers from here, and its runtime still runs.
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(&path).await)).expect("the member's published runtime");
    let held_at = now_nano();
    let hung = rt.hang().expect("the hang is acknowledged by the OS");
    assert!(!hung.held.is_empty(), "every task but the leader is held: the provider reads it running");

    // The caller's node.config.update at the mesh-primary: its own ApplyNodeConfig to the hung birth.
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    let carrier = estate.carrier().await;
    let opts = rdm_transport::CallOptions { resend: rdm_transport::Resend::with_time_limit(Duration::from_secs(40)), ..rdm_transport::CallOptions::default() };
    let reply = rdm_client::NodeRpc::new(carrier.client()).config_update(&primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told, &opts).await;
    eprintln!("node.config.update of the hung birth answered: {reply:?}");

    // 2. on_call_failed for the mesh-primary's spent call to the hung birth.
    let notice = wait_for("the mesh-primary's on_call_failed for its spent call to the hung birth", FLOOR, || async {
        named(&estate.spans(), "rdm.mesh.node.update.via-on-call-failed")
            .into_iter()
            .filter(|sp| attr(sp, "node_id") == primary_id && attr(sp, "receiver_node_id") == node_id && attr(sp, "receiver_incarnation_id") == incarnation.as_str() && at(sp) >= held_at)
            .min_by_key(|sp| at(sp))
            .cloned()
    })
    .await;
    // 3. The mark from the spent call, the investigation from the mark, the re-create on its verdict.
    let mark = wait_for("the mesh-primary marks the birth its own spent call did not reach", FLOOR, || async {
        named(&estate.spans(), "rdm.node_admin.node.update.via-call-failed")
            .into_iter()
            .find(|sp| attr(sp, "node_id") == node_id && attr(sp, "sender") == "own-call" && attr(sp, "outcome") == "marked" && at(sp) >= held_at)
            .cloned()
    })
    .await;
    let after = estate.until_node(&path, &format!("{path:?} re-created, ready, as a new node.id"), FLOOR * 3, |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != node.node_id).cloned()).await;
    hung.release();
    estate.stop().await;

    // Read once every runtime has flushed its evidence.
    let spans = estate.spans();
    let span_of = |id: &str| spans.iter().find(|sp| s(&sp["span_id"]) == id).cloned();
    // 1. The call the notice is told under: three attempts, spent.
    let call = span_of(&s(&notice["parent_span_id"])).unwrap_or_else(|| panic!("the on_call_failed notice's parent span is exported: {notice}"));
    assert_eq!(s(&call["name"]), "rdm.node_rpc.request.update.via-call", "on_call_failed is told under the spent call's span: {call}");
    assert_eq!(attr(&call, "attempts"), "3", "the call ends after three attempts (Resend::default, Q311): {call}");
    assert_eq!(attr(&call, "delivery"), "attempts-spent", "the call ends with its attempts spent: {call}");
    let outcome = attr(&call, "outcome");
    assert!(outcome == "NotSent" || outcome == "Indeterminate", "a spent call ends NotSent or Indeterminate by name (node-rpc.md §43): {call}");
    assert!(at(&call) >= held_at, "the call is the one made after the hang: {call}");
    // 2. Once, after the call ended, with both births and the call's stored end.
    let told_for_call: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-on-call-failed").into_iter().filter(|sp| s(&sp["parent_span_id"]) == s(&call["span_id"])).collect();
    assert_eq!(told_for_call.len(), 1, "on_call_failed is called once for the spent call: {told_for_call:#?}");
    // Told from the call's stored end (the client's `tell_call_end`, inside the call's span): it
    // starts after the call and before the call's span closes.
    assert!(at(&notice) >= at(&call) && at(&notice) <= end(&call), "on_call_failed is told at the call's end, under its span: call {}..{}, notice {}", at(&call), end(&call), at(&notice));
    assert_eq!((attr(&notice, "node_id"), attr(&notice, "incarnation_id")), (primary_id.clone(), primary.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default()), "the sender is the mesh-primary's birth: {notice}");
    assert_eq!((attr(&notice, "receiver_path"), attr(&notice, "receiver_name")), (path.path.to_string(), path.name.to_string()), "the receiver is the hung birth at its (path, name): {notice}");
    assert_eq!(attr(&notice, "outcome"), outcome, "the reason is the call's stored end: {notice} vs {call}");
    assert!(attr(&notice, "reason").starts_with(&outcome), "the reason carries the call's end and its reason: {notice}");
    let app_told: Vec<&Value> = named(&spans, "rdm.testkit.app.update.via-on-call-failed").into_iter().filter(|sp| s(&sp["parent_span_id"]) == s(&notice["span_id"])).collect();
    assert_eq!(app_told.len(), 1, "the application's on_call_failed ran once, under the notice: {app_told:#?}");
    assert_eq!((attr(app_told[0], "receiver_node_id"), attr(app_told[0], "receiver_incarnation_id"), attr(app_told[0], "outcome")), (node_id.clone(), incarnation.as_str().to_string(), outcome.clone()), "the application is told the receiver's birth and the reason: {}", app_told[0]);
    // 3. Investigated from the spent call's mark and re-created.
    assert!(at(&mark) >= at(&call) && at(&mark) <= end(&call), "the mark is made from the spent call's stored end: call {}..{}, mark {mark}", at(&call), end(&call));
    let investigation = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection")
        .into_iter()
        .filter(|sp| attr(sp, "node_id") == node_id && at(sp) >= held_at)
        .min_by_key(|sp| at(sp))
        .cloned()
        .unwrap_or_else(|| panic!("the mesh-primary's investigation of {path:?} is exported"));
    assert!(at(&investigation) > at(&mark), "the investigation follows the mark: mark {}, investigation {}", at(&mark), at(&investigation));
    assert!(at(&investigation) < held_at + FLOOR.as_nanos() as u64 + FLOOR.as_nanos() as u64 / 2, "the spent call's mark started it, before silence alone would have: hang {held_at}, investigation {}", at(&investigation));
    let recovery = named(&spans, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .filter(|sp| attr(sp, "node_id") == node_id && at(sp) >= held_at)
        .min_by_key(|sp| at(sp))
        .cloned()
        .unwrap_or_else(|| panic!("the mesh-primary's recovery submit of {path:?} is exported"));
    assert_eq!(attr(&recovery, "submitter"), primary_id, "the member's mesh-primary submits its re-create: {recovery}");
    assert_eq!(attr(&recovery, "verdict"), "running", "on the investigation's verdict: {recovery}");
    assert!(at(&recovery) >= at(&investigation), "the submit follows the investigation: {recovery}");
    // 4. on_node_investigated once for the birth, recreate, under the submit that records the verdict.
    let investigated: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-on-node-investigated").into_iter().filter(|sp| attr(sp, "node_id") == node_id && attr(sp, "incarnation_id") == incarnation.as_str()).collect();
    assert_eq!(investigated.len(), 1, "on_node_investigated is called once for the birth: {investigated:#?}");
    let told = investigated[0];
    assert_eq!((attr(told, "verdict"), attr(told, "next"), attr(told, "outcome")), ("running".to_string(), "recreate".to_string(), "sent".to_string()), "verdict running, next recreate, sent to the application: {told}");
    assert_eq!(s(&told["parent_span_id"]), s(&recovery["span_id"]), "told under the span that records the verdict and opens the re-create: {told}");
    assert!(at(told) >= at(&investigation) && at(told) >= at(&notice), "told after the investigation and after on_call_failed: {told}");
    let app_investigated: Vec<&Value> = named(&spans, "rdm.testkit.app.update.via-on-node-investigated").into_iter().filter(|sp| attr(sp, "node_id") == node_id && attr(sp, "incarnation_id") == incarnation.as_str()).collect();
    assert_eq!(app_investigated.len(), 1, "the application's on_node_investigated ran once: {app_investigated:#?}");
    assert_eq!((attr(app_investigated[0], "verdict"), attr(app_investigated[0], "next")), ("running".to_string(), "recreate".to_string()), "{}", app_investigated[0]);
    // The verdict is one `rdm.node.investigated` event of the hung birth's node.id on the fabric's store
    // root, by its mesh-primary, the event the notice's span names (node-investigation.md, build.md §5).
    let events = rdm_admin::events_storage::FileEventsStorage::open(&rdm_node::store_root::StoreRoot::at(&estate.fabric_store_root()), &rdm_node::store_log::LogWriter::mint()).await.expect("the fabric's events store opens");
    let of_node = rdm_admin::events_storage::EventsStorage::of_node(&events, &node.node_id).await.expect("the hung birth's events read");
    let verdicts: Vec<_> = of_node.iter().filter(|e| e.r#type == rdm_admin::events_storage::NODE_INVESTIGATED && e.data.get("incarnation").and_then(Value::as_str) == Some(incarnation.as_str())).collect();
    assert_eq!(verdicts.len(), 1, "one rdm.node.investigated event for the birth: {of_node:#?}");
    let event = verdicts[0];
    assert_eq!((event.data.get("verdict").and_then(Value::as_str), event.data.get("next").and_then(Value::as_str)), (Some("running"), Some("recreate")), "{event:?}");
    assert!(event.source.contains(&primary_id), "written by the member's mesh-primary, its exact birth: {event:?}");
    assert_eq!(attr(told, "event_id"), event.id.to_string(), "the notice is sent from the event's write: {told}");

    eprintln!(
        "resend: hang at 0 | call op {} attempts {} delivery {} outcome {} ended +{} ms (trace {}) | on_call_failed +{} ms reason {} | mark +{} ms | investigation +{} ms | recovery submit +{} ms (trace {}) | on_node_investigated +{} ms verdict {} next {} | new birth {} at {path:?}",
        attr(&call, "op"),
        attr(&call, "attempts"),
        attr(&call, "delivery"),
        outcome,
        ms(held_at, end(&call)),
        s(&call["trace_id"]),
        ms(held_at, at(&notice)),
        attr(&notice, "reason"),
        ms(held_at, at(&mark)),
        ms(held_at, at(&investigation)),
        ms(held_at, at(&recovery)),
        s(&recovery["trace_id"]),
        ms(held_at, at(told)),
        attr(told, "verdict"),
        attr(told, "next"),
        after.node_id,
    );
    estate.record_trace_url(&s(&call["trace_id"]));
    estate.record_trace_url(&s(&recovery["trace_id"]));
}
