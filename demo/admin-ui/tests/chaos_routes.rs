//! The Chaos tab's routes: a fault is aimed at one exact runtime, answers a typed outcome, and is
//! refused by name when it is aimed at the fabric-primary (nothing is signalled).

#![cfg(target_os = "linux")]
use rdm_admin_ui::chaos::{Chaos, NodeFacts, Target, FABRIC_PRIMARY_REFUSAL};
use rdm_core::PathName;
use serde_json::json;
use rdm_test::process_faults::{local_control_domain, proc_state, start_token};
use std::process::{Command, Stdio};

/// The status a ready fabric read answers.
fn ready() -> Option<rdm_client::ScopeStatus> {
    Some(rdm_client::ScopeStatus::Ready)
}

/// A sleeper with a published runtime record in a fresh data dir.
fn node(name: &PathName, fabric_primary: bool) -> (std::process::Child, NodeFacts, std::path::PathBuf) {
    let child = Command::new("sleep").arg("60").stdout(Stdio::null()).spawn().unwrap();
    let dir = std::env::temp_dir().join(format!("chaos-{}-{}-{}", std::process::id(), name.path, name.name));
    std::fs::create_dir_all(&dir).unwrap();
    let record = json!({"deployment_id": "dep", "provider": "process", "control_domain": local_control_domain(), "locator": {"kind": "process", "pid": child.id(), "start": start_token(child.id()).unwrap()}});
    std::fs::write(dir.join("runtime.json"), record.to_string()).unwrap();
    let facts = NodeFacts { name: name.clone(), mesh: "mesh1".into(), is_fabric_primary: fabric_primary, data_dir: Some(dir.display().to_string()), transport_port: Some(40000) };
    (child, facts, dir)
}

#[tokio::test]
async fn a_fault_aimed_at_the_fabric_primary_is_refused_by_name_and_signals_nothing() {
    let admin1 = PathName::new("mesh1.node-admin", "1").unwrap();
    let broker1 = PathName::new("mesh1.broker", "1").unwrap();
    let (mut fp, fp_facts, d1) = node(&admin1, true);
    let (mut broker, broker_facts, d2) = node(&broker1, false);
    let nodes = [fp_facts, broker_facts];
    let chaos = Chaos::default();
    for action in ["kill", "stop"] {
        let r = chaos.fault(&nodes, &admin1, action, ready()).await;
        assert_eq!((r.outcome, r.detail["refusal"].as_str()), ("refused", Some(FABRIC_PRIMARY_REFUSAL)), "{r:?}");
    }
    let state = proc_state(fp.id());
    assert!(matches!(state, Some('S' | 'R' | 'D')), "the fabric-primary was not signalled: it reads {state:?}");
    let cut = chaos.cut(&nodes, &Target::Mesh { mesh: "mesh1".into() }).await;
    assert_eq!(cut.detail["refusal"], FABRIC_PRIMARY_REFUSAL, "{cut:?}");
    let cut = chaos.cut(&nodes, &Target::Node(admin1.clone())).await;
    assert_eq!(cut.detail["refusal"], FABRIC_PRIMARY_REFUSAL, "a cut of the fabric-primary by its path and name is refused: {cut:?}");
    assert_eq!(serde_json::to_value(&cut.target).unwrap(), json!({"scope": "node", "path": "mesh1.node-admin", "name": "1"}), "a record names its node by path and name");
    assert_eq!(serde_json::from_value::<Target>(json!({"scope": "node", "path": "mesh1.broker", "name": "1"})).unwrap(), Target::Node(broker1.clone()), "a cut's body names its node by path and name");

    let not_ready = chaos.fault(&nodes, &broker1, "kill", None).await;
    assert_eq!((not_ready.outcome, not_ready.detail["refusal"].as_str()), ("refused", Some("fabric-not-ready")), "a kill before the fabric is read ready is refused by name: {not_ready:?}");
    let state = proc_state(broker.id());
    assert!(matches!(state, Some('S' | 'R' | 'D')), "the refused kill signalled nothing: it reads {state:?}");
    let stopped = chaos.fault(&nodes, &broker1, "stop", ready()).await;
    assert_eq!((stopped.outcome, stopped.detail["state_after"].as_str()), ("applied", Some("T")), "{stopped:?}");
    let resumed = chaos.fault(&nodes, &broker1, "continue", ready()).await;
    assert_eq!(resumed.outcome, "applied", "{resumed:?}");
    let killed = chaos.fault(&nodes, &broker1, "kill", ready()).await;
    assert_eq!((killed.outcome, killed.detail["exited"].as_bool()), ("applied", Some(true)), "{killed:?}");
    let _ = broker.wait();
    let again = chaos.fault(&nodes, &broker1, "kill", ready()).await;
    assert_eq!((again.outcome, again.detail["refusal"].as_str()), ("refused", Some("already_exited")), "a refusal carries the kit's reason: {again:?}");
    assert_eq!(chaos.faults().len(), 9, "every action, applied or refused, is on the record");
    let _ = fp.kill();
    let _ = fp.wait();
    let _ = std::fs::remove_dir_all(d1);
    let _ = std::fs::remove_dir_all(d2);
}
