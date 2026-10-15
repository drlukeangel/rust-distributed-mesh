//! i143.e6.s14 acceptance (rafka-v2 #2902), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2902-unit`, which exports `I143_ACCEPTANCE_DIR`; the cell
//! leaves `result.json` (the schedule it observed) and `spans.json` there.

use rafka_mesh_transport::membership::{RepairSchedule, RepairTarget};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2902/unit").join(cell),
    }
}

fn target(name: &str, silent_ms: u64) -> RepairTarget {
    let key = iroh::SecretKey::generate().public();
    RepairTarget { addr: iroh::EndpointAddr::new(key).with_ip_addr("127.0.0.1:1".parse().unwrap()), node: name.into(), node_id: format!("id-{name}"), silent_for: Duration::from_millis(silent_ms) }
}

/// CONTRACT (#2902, gossip.md §6): a held member whose coverage has been stale for the repair
/// window is scheduled once per window per peer, whether or not the channel has neighbours; a
/// member heard within the window is never scheduled; a member no longer held (retired or
/// departed) leaves the schedule and is never attempted again. What must NOT happen: a hot loop on
/// one peer, a healthy member re-fed, or a retired member re-fed.
#[test]
fn refeed_scheduler_bounds_stale_held_peer_attempts() {
    let cell = "refeed_scheduler_bounds_stale_held_peer_attempts";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let window = Duration::from_millis(3000);
    let mut schedule = RepairSchedule::new(window);
    let (stale, healthy, retired) = (target("mesh2.admin.1", 4000), target("mesh2.rpc.1", 500), target("mesh2.rpc.2", 9000));
    let t0 = Instant::now();

    let first = schedule.due(vec![stale.clone(), healthy.clone(), retired.clone()], t0);
    let first_names: Vec<String> = first.iter().map(|t| t.node.clone()).collect();
    assert_eq!(first_names, vec!["mesh2.admin.1".to_string(), "mesh2.rpc.2".to_string()], "only members stale for the window are due");
    // Within the window: nothing is attempted again (bounded, one per peer per window).
    let mut within = Vec::new();
    for step in 1..=29u64 {
        let due = schedule.due(vec![stale.clone(), healthy.clone(), retired.clone()], t0 + Duration::from_millis(step * 100));
        within.extend(due.into_iter().map(|t| t.node));
    }
    assert!(within.is_empty(), "no peer attempted twice within one window: {within:?}");
    // The retired member leaves the held set: never due again, even past the window.
    let next = schedule.due(vec![stale.clone(), healthy.clone()], t0 + window);
    let next_names: Vec<String> = next.iter().map(|t| t.node.clone()).collect();
    assert_eq!(next_names, vec!["mesh2.admin.1".to_string()], "one attempt per window for the still-held stale member; the retired one is gone");
    let later = schedule.due(vec![stale.clone(), healthy.clone(), retired.clone()], t0 + window + Duration::from_millis(1));
    assert!(later.iter().all(|t| t.node != "mesh2.admin.1"), "the stale member waits a whole window again");

    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&json!([])).unwrap()).unwrap();
    let result = json!({
        "cell": cell,
        "window_ms": window.as_millis() as u64,
        "first_due": first_names,
        "attempts_within_window": within.len(),
        "after_window_due": next_names,
        "healthy_never_due": true,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
