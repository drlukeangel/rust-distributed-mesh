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
//! the source's membership holds now (connections.md section 7); nothing in this repository runs
//! the cold discovery that writes one in the product. Every call afterwards is a real call from the
//! source node over its own held projection.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
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

fn ready(n: &Value) -> bool {
    n["status"] == "ready-for-traffic"
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// The born estate: two node-admins and three rpc nodes, every one ready.
async fn born(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
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
    let dir = estate.connections_dir_of(node);
    std::fs::read_to_string(dir.join("history.jsonl")).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
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

/// CONN-B: the source's runtime is killed and its mesh-primary recovers it (node-recovery.md): a
/// new node at the same path.name over the inherited data dir and its endpoint key. The first call
/// after its hydration uses the Proxy written before, through the same carrier, with no Direct dial
/// toward the destination.
///
/// CONTRACT: a source's latest indexed Proxy survives its process and is the route of the first
/// call from the node that comes back at its path.name (connections.md section 13 B: the Proxy is
/// hydrated from storage and the first call uses it). The destination and carrier births it names
/// are the ones membership holds, so it is valid; the carrier's own edge to the destination is not
/// read from anywhere but the carried call. The source dials the carrier and never the
/// destination, and writes no Direct fact toward the destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_restart_hydrates_proxy_routes_first_call_without_rediscovery() {
    let cell = "source_restart_hydrates_proxy_routes_first_call_without_rediscovery";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = born(cell).await;
    let (dest, carrier, source) = (view(&estate, DEST).await, view(&estate, CARRIER).await, view(&estate, SOURCE).await);

    // 1. The facts a proven carried path leaves: Direct Failed(E1,1) and Proxy Connected via the carrier.
    let seeded = seed_proxy(&estate, 1);
    assert_eq!((s(&seeded["destination_node_id"]), s(&seeded["destination_incarnation"])), (s(&dest["node_id"]), s(&dest["incarnation_id"])));
    assert_eq!((s(&seeded["carrier_node_id"]), s(&seeded["carrier_incarnation"])), (s(&carrier["node_id"]), s(&carrier["incarnation_id"])));
    let before = snapshot(&estate, SOURCE);
    assert_eq!(before["own_active_proxies"].as_array().map(Vec::len), Some(1), "{before}");
    let durable_before: Vec<Value> = history(&estate, SOURCE).await;
    assert!(durable_before.iter().any(|r| r["kind"] == "Direct" && r["state"] == "Failed" && r["recovery"]["recovery_epoch"] == 1), "the Direct Failed row is durable: {durable_before:?}");
    assert!(durable_before.iter().any(|r| r["kind"] == "Proxy" && r["state"] == "Connected" && r["carrier"]["name"] == CARRIER), "the Proxy row is durable: {durable_before:?}");

    // 2. The source's runtime is killed; its mesh-primary recovers it: a new node at the same
    // path.name over the inherited data dir (new node.id and incarnation, the same endpoint key).
    let old_inc = s(&source["incarnation_id"]);
    let killed_at = now_ns();
    estate.kill_node(SOURCE).await;
    let after = estate
        .until_node(SOURCE, "the source is recovered as a new node at its path.name", crate::node_recovery_due(), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (ready(&n) && n["node_id"] != source["node_id"]).then_some(n)
        })
        .await;
    assert_ne!(s(&after["incarnation_id"]), old_inc, "a new incarnation");
    assert_eq!(after["endpoint_id"], source["endpoint_id"], "the same path.name and endpoint key, loaded from the inherited data dir");
    let recovery = wait_for("the mesh-primary's recovery submit for the source is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node"] == SOURCE && sp["attributes"]["node_id"] == source["node_id"]).cloned()
    })
    .await;
    assert_eq!(recovery["attributes"]["incarnation_id"], source["incarnation_id"], "the recovery is keyed on the exact killed birth: {recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "{recovery}");

    // 3. Hydrated before any call: the held projection holds the Proxy and the Failed Direct.
    let hydrated = snapshot(&estate, SOURCE);
    let proxies = hydrated["own_active_proxies"].as_array().cloned().unwrap_or_default();
    assert_eq!(proxies.len(), 1, "the recovered source holds its latest Proxy: {hydrated}");
    assert_eq!((s(&proxies[0]["carrier"]["name"]), s(&proxies[0]["destination"]["name"])), (CARRIER.to_string(), DEST.to_string()));
    let directs = hydrated["own_latest_directs"].as_array().cloned().unwrap_or_default();
    assert!(directs.iter().any(|d| d["destination"]["name"] == DEST && d["state"] == "Failed"), "the backoff series continues from the latest Failed Direct: {hydrated}");
    assert_eq!(hydrated["owed"].as_array().map(Vec::len), Some(0), "{hydrated}");

    // 4. The first call after the recovery.
    let (first, t0, t1) = originate(&estate, SOURCE, DEST, "after-recovery");
    assert_eq!(first["call_outcome"], "reply", "{first}");
    assert_eq!((first["route"].as_str(), first["carrier"].as_str()), (Some("via-peer"), Some(CARRIER)), "{first}");
    assert!(first["retired"].is_null(), "the hydrated Proxy is valid, not retired: {first}");
    assert_eq!(first["reply"]["executing_node"], dest["node_id"], "{first}");
    assert_eq!(first["reply"]["incarnation_id"], dest["incarnation_id"], "the destination's current birth answered: {first}");
    let pooled: Vec<String> = first["pooled"].as_array().into_iter().flatten().map(s).collect();
    assert!(pooled.iter().any(|k| k.contains(&s(&carrier["incarnation_id"]))), "the carrier's connection is pooled: {pooled:?}");
    assert!(!pooled.iter().any(|k| k.contains(&s(&dest["incarnation_id"]))), "no connection to the destination was dialled: {pooled:?}");
    let durable_after = history(&estate, SOURCE).await;
    assert!(
        !durable_after[durable_before.len()..].iter().any(|r| r["kind"] == "Direct" && r["destination"]["name"] == DEST),
        "the source wrote no Direct fact toward the destination after the recovery: {:?}",
        &durable_after[durable_before.len()..]
    );

    // 5. Spans: hydrate, then the route chosen over the held projection, then the carrier's hop and
    // the destination's handler, one trace.
    let spans = wait_for("the call's span chain exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        let r = route_span(&spans, SOURCE, t0, t1)?;
        let trace = r["trace_id"].clone();
        let hop = named(&spans, "rdm.node_rpc.request.serve.via-carried-inner").into_iter().any(|sp| sp["trace_id"] == trace);
        let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().any(|sp| sp["trace_id"] == trace);
        (hop && served).then_some(spans)
    })
    .await;
    let route = route_span(&spans, SOURCE, t0, t1).unwrap();
    assert_eq!((route["attributes"]["route"].as_str(), route["attributes"]["destination"].as_str()), (Some("via-peer"), Some(DEST)), "{route}");
    let trace = route["trace_id"].clone();
    let hydrate = named(&spans, "rdm.node_admin.connection.update.via-hydrate")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == SOURCE && end(sp) < start(route))
        .max_by_key(|sp| start(sp))
        .expect("the recovered source's hydrate span");
    assert!(start(hydrate) > killed_at, "the hydrate is the recovered node's, after the kill: {hydrate}");
    assert!(hydrate["attributes"]["applied"].as_str().and_then(|a| a.parse::<u32>().ok()).unwrap_or(0) >= 2, "the hydrate applied the Direct Failed and the Proxy rows: {hydrate}");
    let hop = named(&spans, "rdm.node_rpc.request.serve.via-carried-inner").into_iter().find(|sp| sp["trace_id"] == trace).unwrap();
    assert_eq!(hop["attributes"]["outcome"], "Reply");
    assert_eq!(s(&hop["attributes"]["target"]), s(&dest["node_id"]), "the carrier made its one inner call to the destination");
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().find(|sp| sp["trace_id"] == trace).unwrap();
    let observed_dest_dials: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-observed")
        .into_iter()
        .filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST && sp["attributes"]["kind"] == "direct" && start(sp) > start(hydrate))
        .collect();
    assert!(observed_dest_dials.is_empty(), "no Direct fact toward the destination was observed after the hydrate: {observed_dest_dials:?}");
    estate.record_trace_url(&s(&trace));

    let result = json!({
        "cell": cell,
        "seeded": seeded,
        "source": { "node_id_before": source["node_id"], "node_id_after": after["node_id"], "endpoint_id": after["endpoint_id"], "incarnation_before": old_inc, "incarnation_after": after["incarnation_id"] },
        "recovery": { "trace_id": recovery["trace_id"], "span_id": recovery["span_id"], "build_id": recovery["attributes"]["build_id"], "attempt": recovery["attributes"]["attempt"] },
        "durable_rows_before_recovery": durable_before.len(),
        "hydrated_snapshot": hydrated,
        "first_call": first,
        "pooled_after_first_call": pooled,
        "durable_rows_after_first_call": durable_after.len(),
        "spans": {
            "hydrate": { "trace_id": hydrate["trace_id"], "span_id": hydrate["span_id"], "applied": hydrate["attributes"]["applied"] },
            "route": { "trace_id": route["trace_id"], "span_id": route["span_id"], "route": route["attributes"]["route"], "outcome": route["attributes"]["outcome"] },
            "carried_hop": { "trace_id": hop["trace_id"], "span_id": hop["span_id"], "parent_span_id": hop["parent_span_id"] },
            "destination_handler": { "trace_id": served["trace_id"], "span_id": served["span_id"], "parent_span_id": served["parent_span_id"], "outcome": served["attributes"]["outcome"] },
        },
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    estate.stop().await;
}

/// CONN-G: with a Proxy active, the destination dials the source. The source's accept writes
/// nothing and wakes the source's own reconciler; once the series' reconnect backoff ends the
/// source dials its own data-plane connection, and only that dial's Direct Connected
/// (`via-observed origin=dial`) owes the Proxy's retirement. While the retirement write is refused
/// the source's calls stay on the Proxy; only after `Proxy Disconnected(direct-restored)` lands
/// durably do new calls go Direct.
///
/// CONTRACT: an inbound accept is a recovery trigger for the source's own direct dial, never
/// Direct evidence (R-CN1); cutback follows durable retirement (connections.md section 10). The
/// spans show the accept, the reconciler's wake under it, the source's dial no earlier than the
/// series' `next_due`, that dial's entry, the refused and landed retirements, then the first Direct
/// route. What must NOT happen: a Direct entry observed on the accept, a dial before the backoff
/// ends, or a Direct route chosen before the retirement landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_accept_wakes_its_own_dial_whose_direct_retires_proxy_before_cutback() {
    let cell = "source_accept_wakes_its_own_dial_whose_direct_retires_proxy_before_cutback";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = born(cell).await;
    let (dest, source) = (view(&estate, DEST).await, view(&estate, SOURCE).await);

    // 1. The Proxy (Direct Failed E1/1, then Proxy Connected), and a healthy call over it. The
    // Failed row's backoff is when the source's own dial may run (connections.md section 9.3).
    let seeded = seed_proxy(&estate, 1);
    let seeded_rows = history(&estate, SOURCE).await;
    let failed: rafka_mesh_entity::connections::NodeConnection = serde_json::from_value(
        seeded_rows.iter().rev().find(|r| r["kind"] == "Direct" && r["state"] == "Failed" && r["destination"]["name"] == DEST).cloned().expect("the seeded Direct Failed row"),
    )
    .expect("a connections row decodes");
    let due_ms = rafka_mesh_entity::reconnect::next_due(&failed).expect("a Failed row with its recovery is due once its backoff ends");
    let (healthy, ..) = originate(&estate, SOURCE, DEST, "before-inbound");
    assert_eq!((healthy["call_outcome"].as_str(), healthy["route"].as_str(), healthy["carrier"].as_str()), (Some("reply"), Some("via-peer"), Some(CARRIER)), "{healthy}");

    // 2. The source's retirement write is refused: its own dial's Direct Connected append passes (a
    // fact is held only once its write lands), every later one is refused until released.
    let (armed, ..) = probe(&estate, &["fault", "--target", &format!("path:{SOURCE}"), "--refuse-history", "100000", "--pass-history", "1", "--key", "x"]);
    assert_eq!(armed["fault"], "armed", "{armed}");

    // 3. The destination dials the source over its own data plane: the source accepts.
    let (inbound, t_inbound0, t_inbound1) = probe(&estate, &["dial", "--target", &format!("path:{DEST}"), "--destination", &format!("path:{SOURCE}"), "--key", "x"]);
    assert_eq!((inbound["dialed"].as_str(), inbound["destination_node_id"].as_str()), (Some("Reply"), source["node_id"].as_str()), "the destination reached the source: {inbound}");

    // 4. The source's own dial connected and its retirement is owed and refused by name; the Proxy
    // stays effective.
    let owed_refused = wait_for("the source's dial connected, it owes the retirement and its write was refused", Duration::from_secs(15), || async {
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
    assert!(!durable_during.iter().any(|r| r["kind"] == "Proxy" && r["state"] == "Disconnected"), "nothing durable says the Proxy retired: {durable_during:?}");
    let connected_rows: Vec<&Value> = durable_during[seeded_rows.len()..].iter().filter(|r| r["kind"] == "Direct" && r["destination"]["name"] == DEST).collect();
    assert_eq!(connected_rows.len(), 1, "one Direct entry toward the destination after the accept, the source's own dial's: {connected_rows:?}");
    assert_eq!(connected_rows[0]["state"], "Connected", "{connected_rows:?}");

    // 5. The write can land: released, the owed retirement lands by the bounded reconciler.
    let (released, ..) = probe(&estate, &["fault", "--target", &format!("path:{SOURCE}"), "--release", "--key", "x"]);
    assert_eq!(released["fault"], "released", "{released}");
    let landed = wait_for("the owed retirement landed", Duration::from_secs(15), || async {
        let snap = snapshot(&estate, SOURCE);
        (snap["owed"].as_array().map(Vec::len) == Some(0) && snap["own_active_proxies"].as_array().map(Vec::len) == Some(0)).then_some(snap)
    })
    .await;
    let durable_after = history(&estate, SOURCE).await;
    let retirement: Vec<&Value> = durable_after.iter().filter(|r| r["kind"] == "Proxy" && r["state"] == "Disconnected").collect();
    assert_eq!(retirement.len(), 1, "exactly one durable retirement: {durable_after:?}");
    assert_eq!(retirement[0]["reason"], "direct-restored");

    // 6. Only now is Direct effective.
    let (cut, c0, c1) = originate(&estate, SOURCE, DEST, "after-retirement");
    assert_eq!((cut["call_outcome"].as_str(), cut["route"].as_str()), (Some("reply"), Some("direct")), "{cut}");
    assert_eq!(cut["reply"]["incarnation_id"], dest["incarnation_id"], "{cut}");

    // 7. Spans: the accept and the wake under it, the source's dial and its entry, the refusals by
    // name, the landing, then the Direct route.
    let spans = wait_for("the retirement and route spans exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        let landed = named(&spans, "rdm.node_admin.connection.update.via-retirement").into_iter().any(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["outcome"] == "landed");
        (landed && route_span(&spans, SOURCE, c0, c1).is_some() && route_span(&spans, SOURCE, d0, d1).is_some()).then_some(spans)
    })
    .await;
    let wakes: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-accept-trigger")
        .into_iter()
        .filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST)
        .collect();
    assert_eq!(wakes.len(), 1, "one accept from the destination, one wake: {wakes:?}");
    let wake = wakes[0];
    assert_eq!(wake["attributes"]["outcome"], "pending", "the accept armed the source's attempt: {wake}");
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
    let mut retirements: Vec<&Value> = named(&spans, "rdm.node_admin.connection.update.via-retirement").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE).collect();
    retirements.sort_by_key(|sp| start(sp));
    let (refused, lands): (Vec<&Value>, Vec<&Value>) = retirements.iter().partition(|sp| sp["attributes"]["outcome"] == "refused");
    assert!(!refused.is_empty(), "the refused write is named on a span: {retirements:?}");
    assert!(refused.iter().all(|sp| s(&sp["attributes"]["reason"]).contains("testkit fault armed")), "{refused:?}");
    assert!(refused.iter().all(|sp| start(sp) >= start(entry)), "every retirement attempt follows the dial's entry");
    assert_eq!((&refused[0]["trace_id"], &refused[0]["parent_span_id"]), (&entry["trace_id"], &entry["span_id"]), "the first retirement attempt is the entry's own settlement: {:?}", refused[0]);
    assert_eq!(lands.len(), 1, "one landing: {retirements:?}");
    assert!(refused.iter().all(|sp| end(sp) <= start(lands[0])), "every refusal precedes the landing");
    let during_route = route_span(&spans, SOURCE, d0, d1).unwrap();
    let cut_route = route_span(&spans, SOURCE, c0, c1).unwrap();
    assert_eq!(during_route["attributes"]["route"], "via-peer");
    assert_eq!(cut_route["attributes"]["route"], "direct");
    assert!(start(during_route) < start(lands[0]), "the call during the refusal chose its route before the landing");
    assert!(start(cut_route) >= end(lands[0]), "the first Direct route is chosen after the retirement landed");
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
        "durable_retirement": retirement[0],
        "call_after_retirement": cut,
        "spans": {
            "accept": { "span_id": accept["span_id"], "trace_id": accept["trace_id"], "peer": accept["attributes"]["peer"] },
            "wake": { "span_id": wake["span_id"], "trace_id": wake["trace_id"], "outcome": wake["attributes"]["outcome"], "start_unix_nano": start(wake) },
            "dial": { "span_id": dial["span_id"], "trace_id": dial["trace_id"], "outcome": dial["attributes"]["outcome"], "start_unix_nano": start(dial) },
            "entry": { "span_id": entry["span_id"], "trace_id": entry["trace_id"], "origin": entry["attributes"]["origin"], "start_unix_nano": start(entry) },
            "refused_retirements": refused.iter().map(|sp| json!({"span_id": sp["span_id"], "start_unix_nano": start(sp), "reason": sp["attributes"]["reason"]})).collect::<Vec<_>>(),
            "landed_retirement": { "span_id": lands[0]["span_id"], "start_unix_nano": start(lands[0]), "end_unix_nano": end(lands[0]) },
            "route_while_refused": { "span_id": during_route["span_id"], "trace_id": during_route["trace_id"], "route": during_route["attributes"]["route"] },
            "route_after_retirement": { "span_id": cut_route["span_id"], "trace_id": cut_route["trace_id"], "route": cut_route["attributes"]["route"], "start_unix_nano": start(cut_route) },
        },
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    estate.stop().await;
}
