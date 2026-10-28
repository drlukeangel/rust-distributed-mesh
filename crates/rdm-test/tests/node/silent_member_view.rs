//! Q254 (Luke, 2026-10-28: "One clock, the pruner's: its member-stale mark is a view change"; "no
//! deadline sleep replaces a tick (a sleep is a timer)"): the admin's view rebuild waits on events
//! only. A member silent past the staleness floor is an event raised where that clock is read, the
//! topology view's pruner (`rdm.mesh.topology.update.via-member-stale`), never a poll of the view.
//!
//! CONTRACT: in a one-mesh estate (two node-admins, two rpc nodes), once the fabric is ready and
//! nothing happens, no admin rebuilds its view on a poll: every `rdm.node_admin.topology.update.via-view-rebuild`
//! names the event that made it due, and none is `poll`. Then a non-primary rpc node is SIGKILLed with
//! nothing else happening: its mesh-primary's pruner marks it stale, that mark makes the mesh-primary's
//! view rebuild due (`woken = silence`) in the same sweep, and the node is recovered: a new birth,
//! ready, at its path.name. What must NOT happen: a view rebuilt on a poll, or a SIGKILLed member
//! left unrecovered because no event marked its silence.

use rdm_client::{LaunchKind as NodeKind, NodeStatus};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-recovery".into(),
        subfeature: "silent-member-view".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

// @feature: node-lifecycle
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_admin_rebuilds_its_view_on_no_poll_and_a_sigkilled_member_is_marked_by_the_pruner_and_recovered() {
    let test = "an_idle_admin_rebuilds_its_view_on_no_poll_and_a_sigkilled_member_is_marked_by_the_pruner_and_recovered";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let nodes = estate
        .until_view("two ready node-admins and two ready rpc nodes, one rpc node not a primary", Duration::from_secs(60), |views| {
            let v = Estate::values(views);
            let ready = v.iter().filter(|n| n["status"] == "ready").count();
            (ready == 4 && v.iter().any(|n| n["kind"] == "rpc_node" && n["is_primary"] != true)).then_some(v)
        })
        .await;
    let mp = s(&nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh1's primary")["name"]);
    let victim_node = nodes.iter().find(|n| n["kind"] == "rpc_node" && n["is_primary"] != true).expect("an rpc node that is no primary").clone();
    let (victim, victim_id) = (s(&victim_node["name"]), s(&victim_node["node_id"]));

    // Nothing happens: the estate formed, and it is left alone for a while.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let idle_from = now_ns();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let idle_to = now_ns();

    estate.kill_node(&victim).await;
    let killed_at = now_ns();
    estate
        .until_view(&format!("a new birth at {victim} restores two ready rpc nodes"), floor + Duration::from_secs(60), |views| {
            let ready: Vec<_> = views.iter().filter(|n| n.kind == NodeKind::RpcNode && n.status == NodeStatus::Ready).collect();
            (ready.len() == 2 && !ready.iter().any(|n| n.node_id.to_string() == victim_id)).then_some(())
        })
        .await;
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let ms = |t: u64| t.saturating_sub(idle_from) / 1_000_000;
    let rebuilds = named(&spans, "rdm.node_admin.topology.update.via-view-rebuild");

    // Idle: per admin, the rebuilds and what made each due.
    let mut idle: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    for sp in rebuilds.iter().filter(|sp| at(sp) >= idle_from && at(sp) < idle_to) {
        *idle.entry(attr(sp, "node")).or_default().entry(attr(sp, "woken")).or_default() += 1;
    }
    let secs = (idle_to - idle_from) as f64 / 1e9;
    for (admin, by) in &idle {
        eprintln!("Q254 idle {secs:.1} s: {admin} rebuilt its view {} times ({:.1}/s): {by:?}", by.values().sum::<usize>(), by.values().sum::<usize>() as f64 / secs);
    }
    let polled: Vec<(&String, usize)> = idle.iter().filter_map(|(a, by)| by.get("poll").map(|n| (a, *n))).collect();
    assert!(polled.is_empty(), "no idle admin rebuilds its view on a poll: {polled:?} (all idle rebuilds: {idle:?})");

    // The SIGKILLed member: its mesh-primary's pruner marks it stale, and that mark makes its view rebuild due.
    let stale = named(&spans, "rdm.mesh.topology.update.via-member-stale")
        .into_iter()
        .filter(|sp| at(sp) >= killed_at && attr(sp, "node") == mp && attr(sp, "member_id") == victim_id)
        .min_by_key(|sp| at(sp))
        .cloned()
        .unwrap_or_else(|| panic!("{mp}'s pruner marked {victim} stale after its SIGKILL"));
    let marked = rebuilds
        .iter()
        .filter(|sp| attr(sp, "node") == mp && attr(sp, "woken") == "silence" && at(sp) >= at(&stale) && at(sp) <= at(&stale) + 1_000_000_000)
        .min_by_key(|sp| at(sp))
        .copied()
        .unwrap_or_else(|| {
            let near: Vec<String> = rebuilds.iter().filter(|sp| attr(sp, "node") == mp && at(sp) + 2_000_000_000 >= at(&stale) && at(sp) <= at(&stale) + 2_000_000_000).map(|sp| format!("+{} ms woken={} changed={}", ms(at(sp)), attr(sp, "woken"), attr(sp, "changed"))).collect();
            panic!("the pruner's stale mark of {victim} at +{} ms made {mp}'s view rebuild due within a second (woken = silence); its rebuilds around it: {near:#?}", ms(at(&stale)))
        });
    let recovery = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .filter(|sp| at(sp) >= killed_at && attr(sp, "node_id") == victim_id && attr(sp, "outcome").starts_with("complete"))
        .min_by_key(|sp| at(sp))
        .cloned()
        .unwrap_or_else(|| panic!("{victim}'s recovery completed"));
    eprintln!(
        "Q254: {victim} SIGKILLed at +{} ms; {mp} marked it stale at +{} ms (silent_ms {}); its view rebuild woken by that mark at +{} ms (changed {}); its recovery by {} at +{} ms, outcome {}",
        ms(killed_at),
        ms(at(&stale)),
        attr(&stale, "silent_ms"),
        ms(at(marked)),
        attr(marked, "changed"),
        attr(&recovery, "submitter"),
        ms(at(&recovery)),
        attr(&recovery, "outcome")
    );
    estate.record_trace_url(&s(&stale["trace_id"]));
    estate.record_trace_url(&s(&marked["trace_id"]));
    estate.record_trace_url(&s(&recovery["trace_id"]));
}
