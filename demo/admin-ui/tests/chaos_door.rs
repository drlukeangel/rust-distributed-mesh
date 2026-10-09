//! The Chaos tab's door: a fault is aimed at one exact runtime, answers a typed outcome, and is
//! refused by name when it is aimed at the fabric-primary (nothing is signalled).

#![cfg(target_os = "linux")]
use rafka_admin_ui::chaos::{Chaos, NodeFacts, FABRIC_PRIMARY_REFUSAL};
use serde_json::json;
use rafka_chaos::process_faults::{local_control_domain, proc_state, start_token};
use std::process::{Command, Stdio};

/// A sleeper with a published runtime record in a fresh data dir.
fn node(name: &str, fabric_primary: bool) -> (std::process::Child, NodeFacts, std::path::PathBuf) {
    let child = Command::new("sleep").arg("60").stdout(Stdio::null()).spawn().unwrap();
    let dir = std::env::temp_dir().join(format!("chaos-{}-{}", std::process::id(), name));
    std::fs::create_dir_all(&dir).unwrap();
    let record = json!({"deployment_id": "dep", "provider": "process", "control_domain": local_control_domain(), "locator": {"kind": "process", "pid": child.id(), "start": start_token(child.id()).unwrap()}});
    std::fs::write(dir.join("runtime.json"), record.to_string()).unwrap();
    let facts = NodeFacts { name: name.into(), mesh: "mesh1".into(), is_fabric_primary: fabric_primary, data_dir: Some(dir.display().to_string()), transport_port: Some(40000) };
    (child, facts, dir)
}

#[tokio::test]
async fn a_fault_aimed_at_the_fabric_primary_is_refused_by_name_and_signals_nothing() {
    let (mut fp, fp_facts, d1) = node("mesh1.admin.1", true);
    let (mut rpc, rpc_facts, d2) = node("mesh1.rpc.1", false);
    let nodes = [fp_facts, rpc_facts];
    let chaos = Chaos::default();
    for action in ["kill", "stop"] {
        let r = chaos.fault(&nodes, "mesh1.admin.1", action).await;
        assert_eq!((r.outcome, r.detail["refusal"].as_str()), ("refused", Some(FABRIC_PRIMARY_REFUSAL)), "{r:?}");
    }
    assert!(matches!(proc_state(fp.id()), Some('S' | 'R')), "the fabric-primary was not signalled");
    let cut = chaos.cut(&nodes, "mesh", "mesh1").await;
    assert_eq!(cut.detail["refusal"], FABRIC_PRIMARY_REFUSAL, "{cut:?}");

    let stopped = chaos.fault(&nodes, "mesh1.rpc.1", "stop").await;
    assert_eq!((stopped.outcome, stopped.detail["state_after"].as_str()), ("applied", Some("T")), "{stopped:?}");
    let resumed = chaos.fault(&nodes, "mesh1.rpc.1", "continue").await;
    assert_eq!(resumed.outcome, "applied", "{resumed:?}");
    let killed = chaos.fault(&nodes, "mesh1.rpc.1", "kill").await;
    assert_eq!((killed.outcome, killed.detail["exited"].as_bool()), ("applied", Some(true)), "{killed:?}");
    let _ = rpc.wait();
    let again = chaos.fault(&nodes, "mesh1.rpc.1", "kill").await;
    assert_eq!((again.outcome, again.detail["refusal"].as_str()), ("refused", Some("already_exited")), "a refusal carries the kit's reason: {again:?}");
    assert_eq!(chaos.faults().len(), 7, "every action, applied or refused, is on the record");
    let _ = fp.kill();
    let _ = fp.wait();
    let _ = std::fs::remove_dir_all(d1);
    let _ = std::fs::remove_dir_all(d2);
}
