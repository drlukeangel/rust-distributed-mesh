//! i143.e6.s15 acceptance (rafka-v2 #2938, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2938-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest and every process's spans land under it, feature `i143-2938`, test the
//! cell's name).
//!
//! The estate is one mesh, `{mesh1: 2 node-admins, 3 rpc nodes}`, born by a Build. The source is
//! `mesh1.rpc.1`, the carrier `mesh1.rpc.2`, the destination `mesh1.rpc.3`. A Proxy is seeded on
//! the source through the testkit originate door (`record-proxy`): the door runs the writer's own
//! observer path for the Direct Failed observation and writes `Proxy Connected` naming the births
//! the source's topology view holds now (connections.md section 7); nothing in this repository runs
//! the cold discovery that writes one in the product. Every call afterwards is a real call from the
//! source node over its own held projection.

use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const SOURCE: &str = "mesh1.rpc.1";
const CARRIER: &str = "mesh1.rpc.2";
const DEST: &str = "mesh1.rpc.3";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2938".into(),
        subfeature: "connections".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2938/process").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// The born estate: two node-admins and three rpc nodes, every one ready.
async fn born(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // The settled view is the admin's. The source records a Proxy to the destination through the
    // carrier, so its own resolver must hold both births first: a node learns a peer's birth by
    // gossip, and a source that has not heard the destination yet refuses it Unknown by name.
    for peer in [CARRIER, DEST] {
        estate.witness_holds_birth(SOURCE, peer).await;
    }
    estate
}

/// `node.get` of `name`, as the JSON the cell records.
async fn view(estate: &Estate, name: &str) -> Value {
    serde_json::to_value(estate.node_view(name).await).expect("a view serializes")
}

/// One probe invocation: the typed answer, and the instants it ran between.
fn probe(estate: &Estate, args: &[&str]) -> (Value, u64, u64) {
    let t0 = now_ns();
    let out = estate.probe(args);
    (out, t0, now_ns())
}

fn snapshot(estate: &Estate, node: &str) -> Value {
    let (out, _, _) = probe(estate, &["snapshot", "--target", &format!("path:{node}"), "--key", "x"]);
    assert_eq!(out["outcome"], "Reply", "{node} answers the snapshot door: {out}");
    out
}

fn originate(estate: &Estate, from: &str, to: &str, key: &str) -> (Value, u64, u64) {
    probe(estate, &["originate", "--target", &format!("path:{from}"), "--destination", &format!("path:{to}"), "--key", key])
}

/// The source's durable raw log on the fabric's store root: every fact it wrote, in order.
async fn history(estate: &Estate, node: &str) -> Vec<Value> {
    estate.connection_history(node)
}

/// Seed the Proxy `SOURCE -> CARRIER -> DEST` after `failed` Direct Failed observations.
fn seed_proxy(estate: &Estate, failed: u32) -> Value {
    let (out, _, _) = probe(
        estate,
        &["record-proxy", "--target", &format!("path:{SOURCE}"), "--destination", &format!("path:{DEST}"), "--carrier", &format!("path:{CARRIER}"), "--failed-attempts", &failed.to_string(), "--key", "x"],
    );
    assert_eq!(out["proxy_recorded"], true, "the source recorded the Proxy: {out}");
    out
}

/// The spans of the call whose `via-held-projection` span started in `[from, to]` on `own`.
fn route_span<'a>(spans: &'a [Value], own: &str, from: u64, to: u64) -> Option<&'a Value> {
    named(spans, "rdm.node_rpc.route.resolve.via-held-projection").into_iter().find(|sp| sp["attributes"]["own"] == own && start(sp) >= from && start(sp) <= to)
}

/// CONN-G: with a Proxy active, the destination dials the source. The source's accept writes
/// nothing and wakes the source's own reconciler; once the series' reconnect backoff ends the
/// source dials its own data-plane connection, and only that dial's Direct Connected
/// (`via-observed origin=dial`) owes the Proxy's deletion. While the deletion write is refused
/// the source's calls stay on the Proxy; only after `Proxy Disconnected(direct-restored)` lands
/// durably do new calls go Direct.
///
/// CONTRACT: an inbound accept is a recovery trigger for the source's own direct dial, never
/// Direct evidence (R-CN1); cutback follows durable deletion (connections.md section 10). The
/// spans show the accept, the reconciler's wake under it, the source's dial no earlier than the
/// series' `next_due`, that dial's entry, the refused and landed deletions, then the first Direct
/// route. What must NOT happen: a Direct entry observed on the accept, a dial before the backoff
/// ends, or a Direct route chosen before the deletion landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback() {
    let cell = "source_accept_wakes_its_own_dial_whose_direct_disconnects_proxy_before_cutback";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = born(cell).await;
    let (dest, source) = (view(&estate, DEST).await, view(&estate, SOURCE).await);

    // 1. The Proxy (Direct Failed E1/1, then Proxy Connected), and a healthy call over it. The
    // Failed row's backoff is when the source's own dial may run (connections.md section 9.3).
    let seeded = seed_proxy(&estate, 1);
    let seeded_rows = history(&estate, SOURCE).await;
    let failed: rdm_core::connections::NodeConnection = serde_json::from_value(
        seeded_rows.iter().rev().find(|r| r["kind"] == "Direct" && r["state"] == "Failed" && r["destination"]["name"] == DEST).cloned().expect("the seeded Direct Failed row"),
    )
    .expect("a connections row decodes");
    let due_ms = rdm_core::reconnect::next_due(&failed).expect("a Failed row with its recovery is due once its backoff ends");
    let (healthy, ..) = originate(&estate, SOURCE, DEST, "before-inbound");
    assert_eq!((healthy["call_outcome"].as_str(), healthy["route"].as_str(), healthy["carrier"].as_str()), (Some("reply"), Some("via-peer"), Some(CARRIER)), "{healthy}");

    // 2. The source's deletion write is refused: its own dial's Direct Connected append passes (a
    // fact is held only once its write lands), every later one is refused until released.
    let (armed, ..) = probe(&estate, &["fault", "--target", &format!("path:{SOURCE}"), "--refuse-history", "100000", "--pass-history", "1", "--key", "x"]);
    assert_eq!(armed["fault"], "armed", "{armed}");

    // 3. The destination dials the source over its own data plane: the source accepts.
    let (inbound, t_inbound0, t_inbound1) = probe(&estate, &["dial", "--target", &format!("path:{DEST}"), "--destination", &format!("path:{SOURCE}"), "--key", "x"]);
    assert_eq!((inbound["dialed"].as_str(), inbound["destination_node_id"].as_str()), (Some("Reply"), source["node_id"].as_str()), "the destination reached the source: {inbound}");

    // 4. The source's own dial connected and its deletion is owed and refused by name; the Proxy
    // stays effective.
    let owed_refused = wait_for("the source's dial connected, it owes the deletion and its write was refused", Duration::from_secs(15), || async {
        let snap = snapshot(&estate, SOURCE);
        let owed = snap["owed"].as_array().map(Vec::len).unwrap_or(0);
        (owed == 1 && snap["fault_refused"].as_u64().unwrap_or(0) >= 2).then_some(snap)
    })
    .await;
    assert_eq!(owed_refused["own_active_proxies"].as_array().map(Vec::len), Some(1), "the Proxy is still held: {owed_refused}");
    assert!(
        owed_refused["own_latest_directs"].as_array().into_iter().flatten().any(|d| d["destination"]["name"] == DEST && d["state"] == "Connected"),
        "the source's own dial is its latest Direct toward the destination: {owed_refused}"
    );
    let (during, d0, d1) = originate(&estate, SOURCE, DEST, "while-refused");
    assert_eq!((during["call_outcome"].as_str(), during["route"].as_str(), during["carrier"].as_str()), (Some("reply"), Some("via-peer"), Some(CARRIER)), "new calls stay on the Proxy: {during}");
    let durable_during = history(&estate, SOURCE).await;
    assert!(!durable_during.iter().any(|r| r["kind"] == "Proxy" && r["state"] == "Disconnected"), "nothing durable says the Proxy deleted: {durable_during:?}");
    let connected_rows: Vec<&Value> = durable_during[seeded_rows.len()..].iter().filter(|r| r["kind"] == "Direct" && r["destination"]["name"] == DEST).collect();
    assert_eq!(connected_rows.len(), 1, "one Direct entry toward the destination after the accept, the source's own dial's: {connected_rows:?}");
    assert_eq!(connected_rows[0]["state"], "Connected", "{connected_rows:?}");

    // 5. The write can land: released, the owed deletion lands by the bounded reconciler.
    let (released, ..) = probe(&estate, &["fault", "--target", &format!("path:{SOURCE}"), "--release", "--key", "x"]);
    assert_eq!(released["fault"], "released", "{released}");
    let landed = wait_for("the owed deletion landed", Duration::from_secs(15), || async {
        let snap = snapshot(&estate, SOURCE);
        (snap["owed"].as_array().map(Vec::len) == Some(0) && snap["own_active_proxies"].as_array().map(Vec::len) == Some(0)).then_some(snap)
    })
    .await;
    let durable_after = history(&estate, SOURCE).await;
    let deleted: Vec<&Value> = durable_after.iter().filter(|r| r["kind"] == "Proxy" && r["state"] == "Disconnected").collect();
    assert_eq!(deleted.len(), 1, "exactly one durable delete: {durable_after:?}");
    assert_eq!(deleted[0]["reason"], "direct-restored");

    // 6. Only now is Direct effective.
    let (cut, c0, c1) = originate(&estate, SOURCE, DEST, "after-delete");
    assert_eq!((cut["call_outcome"].as_str(), cut["route"].as_str()), (Some("reply"), Some("direct")), "{cut}");
    assert_eq!(cut["reply"]["incarnation_id"], dest["incarnation_id"], "{cut}");

    // 7. Spans: the accept and the wake under it, the source's dial and its entry, the refusals by
    // name, the landing, then the Direct route.
    let spans = wait_for("the delete and route spans exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        let landed = named(&spans, "rdm.node_admin.connection.delete.via-direct-restored").into_iter().any(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["outcome"] == "landed");
        (landed && route_span(&spans, SOURCE, c0, c1).is_some() && route_span(&spans, SOURCE, d0, d1).is_some()).then_some(spans)
    })
    .await;
    let wakes: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-accept-trigger")
        .into_iter()
        .filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST)
        .collect();
    assert_eq!(wakes.len(), 1, "one accept from the destination, one wake: {wakes:?}");
    let wake = wakes[0];
    // The accept meets the attempt the Direct Failed row armed either while it is pending (`coalesced`)
    // or after the source's own dial already connected it (`direct-connected`: connections.md §12, "was
    // already satisfied by Direct Connected"), whichever comes first; neither opens a second attempt.
    assert!(matches!(wake["attributes"]["outcome"].as_str(), Some("coalesced" | "direct-connected")), "the accept coalesced into the pending attempt the Direct Failed row armed, or found it connected: {wake}");
    assert_eq!(s(&wake["attributes"]["destination_incarnation"]), s(&dest["incarnation_id"]), "the attempt is toward the exact birth the Proxy names: {wake}");
    assert!(start(wake) >= t_inbound0 && start(wake) <= t_inbound1 + 5_000_000_000, "the wake belongs to the destination's call");
    let accept = named(&spans, "rdm.node_rpc.connection.create.via-accept").into_iter().find(|sp| sp["span_id"] == wake["parent_span_id"]).expect("the wake runs under the accept span");
    assert!(s(&dest["endpoint_id"]).starts_with(&s(&accept["attributes"]["peer"])), "the accepted peer is the destination: {accept}");
    let observed: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-observed")
        .into_iter()
        .filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST && sp["attributes"]["kind"] == "direct" && start(sp) >= start(wake))
        .collect();
    assert!(!observed.iter().any(|sp| sp["attributes"]["origin"] == "accept"), "the accept is no Direct entry: {observed:?}");
    let dials: Vec<&Value> = named(&spans, "rdm.node_admin.connection.create.via-recovery-dial").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST).collect();
    assert_eq!(dials.len(), 1, "one dial of the source's own: {dials:?}");
    let dial = dials[0];
    assert!(start(dial) >= start(wake), "the dial follows the wake");
    assert_eq!((&dial["trace_id"], &dial["parent_span_id"]), (&wake["trace_id"], &wake["span_id"]), "the dial is the wake's, in the accept's trace: {dial}");
    assert!(start(dial) >= due_ms * 1_000_000, "the dial waited for the series' backoff: started {} ns, due {} ms", start(dial), due_ms);
    let entries: Vec<&&Value> = observed.iter().filter(|sp| sp["attributes"]["state"] == "connected" && sp["attributes"]["origin"] == "dial").collect();
    assert_eq!(entries.len(), 1, "the source's dial is the one Direct Connected entry: {observed:?}");
    let entry = entries[0];
    assert_eq!(entry["attributes"]["outcome"], "landed", "{entry}");
    assert!(start(entry) >= start(dial), "the entry is the dial's");
    assert_eq!((s(&dial["attributes"]["fact"]), s(&dial["attributes"]["entry"])), ("connected".to_string(), s(&entry["attributes"]["entry"])), "the entry is the dial's own observation: {dial} {entry}");
    let mut deletes: Vec<&Value> = named(&spans, "rdm.node_admin.connection.delete.via-direct-restored").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE).collect();
    deletes.sort_by_key(|sp| start(sp));
    let (refused, lands): (Vec<&Value>, Vec<&Value>) = deletes.iter().partition(|sp| sp["attributes"]["outcome"] == "refused");
    assert!(!refused.is_empty(), "the refused write is named on a span: {deletes:?}");
    assert!(refused.iter().all(|sp| s(&sp["attributes"]["detail"]).contains("testkit fault armed")), "{refused:?}");
    assert!(refused.iter().all(|sp| start(sp) >= start(entry)), "every delete attempt follows the dial's entry");
    assert_eq!((&refused[0]["trace_id"], &refused[0]["parent_span_id"]), (&entry["trace_id"], &entry["span_id"]), "the first delete attempt is the entry's own settlement: {:?}", refused[0]);
    assert_eq!(lands.len(), 1, "one landing: {deletes:?}");
    assert!(refused.iter().all(|sp| end(sp) <= start(lands[0])), "every refusal precedes the landing");
    let during_route = route_span(&spans, SOURCE, d0, d1).unwrap();
    let cut_route = route_span(&spans, SOURCE, c0, c1).unwrap();
    assert_eq!(during_route["attributes"]["route"], "via-peer");
    assert_eq!(cut_route["attributes"]["route"], "direct");
    assert!(start(during_route) < start(lands[0]), "the call during the refusal chose its route before the landing");
    assert!(start(cut_route) >= end(lands[0]), "the first Direct route is chosen after the delete landed");
    estate.record_trace_url(&s(&wake["trace_id"]));

    let result = json!({
        "cell": cell,
        "seeded": seeded,
        "seeded_failed_row": failed,
        "due_ms": due_ms,
        "healthy_call_over_proxy": healthy,
        "fault_armed": armed,
        "inbound_call_from_destination": inbound,
        "owed_and_refused_snapshot": owed_refused,
        "call_while_refused": during,
        "durable_rows_while_refused": durable_during.len(),
        "fault_released": released,
        "landed_snapshot": landed,
        "durable_delete": deleted[0],
        "call_after_delete": cut,
        "spans": {
            "accept": { "span_id": accept["span_id"], "trace_id": accept["trace_id"], "peer": accept["attributes"]["peer"] },
            "wake": { "span_id": wake["span_id"], "trace_id": wake["trace_id"], "outcome": wake["attributes"]["outcome"], "start_unix_nano": start(wake) },
            "dial": { "span_id": dial["span_id"], "trace_id": dial["trace_id"], "outcome": dial["attributes"]["outcome"], "start_unix_nano": start(dial) },
            "entry": { "span_id": entry["span_id"], "trace_id": entry["trace_id"], "origin": entry["attributes"]["origin"], "start_unix_nano": start(entry) },
            "refused_deletes": refused.iter().map(|sp| json!({"span_id": sp["span_id"], "start_unix_nano": start(sp), "reason": sp["attributes"]["reason"]})).collect::<Vec<_>>(),
            "landed_delete": { "span_id": lands[0]["span_id"], "start_unix_nano": start(lands[0]), "end_unix_nano": end(lands[0]) },
            "route_while_refused": { "span_id": during_route["span_id"], "trace_id": during_route["trace_id"], "route": during_route["attributes"]["route"] },
            "route_after_delete": { "span_id": cut_route["span_id"], "trace_id": cut_route["trace_id"], "route": cut_route["attributes"]["route"], "start_unix_nano": start(cut_route) },
        },
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    estate.stop().await;
}
