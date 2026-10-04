//! Scenario runner: brings the scenario's shape up through node-admin Build on
//! the selected provider, then executes every operation and assertion over real
//! Node RPC through `rafka-rpc-probe` (PRD §6: no in-process shortcut).

use crate::estate::{wait_for, Estate, Owner};
use crate::scenario::{Assertion, Operation, Scenario};
use serde_json::{json, Value};
use std::time::Duration;

pub const SETTLE: Duration = Duration::from_secs(120);

/// One failed operation or assertion, named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub step: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct RunReport {
    pub build_id: String,
    pub failures: Vec<Failure>,
}

/// Run `scenario` on `provider` (the scenario's own provider when `None`).
pub async fn run(scenario: &Scenario, provider: Option<&str>, test: &str) -> RunReport {
    let provider = provider.unwrap_or(&scenario.provider).to_string();
    let first_mesh = scenario.meshes.first().map(|m| m.name.clone()).unwrap_or_else(|| "mesh1".into());
    let estate = Estate::bootstrap(
        Owner {
            product: scenario.product.clone(),
            feature: scenario.feature.clone(),
            subfeature: scenario.subfeature.clone(),
            rung: scenario.rung.clone(),
            provider: provider.clone(),
            test: test.into(),
        },
        &scenario.fabric,
        &first_mesh,
    )
    .await;
    estate.artifact("scenario.json", &serde_json::to_value(scenario).expect("scenario serializes"));

    let body = scenario.build_request();
    estate.artifact("build-request.json", &json!({"route": "POST /api/build", "body": body}));
    let (status, accepted) = estate.post("/api/build", &body).await;
    assert_eq!(status, 202, "POST /api/build: {accepted}");
    let build_id = accepted["build_id"].as_str().expect("build_id").to_string();
    let done = estate.await_build(&build_id, SETTLE).await;
    estate.artifact("build-status.json", &done);

    let want_admins: u32 = scenario.meshes.iter().map(|m| m.node_admin).sum();
    let want_rpc: u32 = scenario.meshes.iter().map(|m| m.rpc_node).sum();
    wait_for("every desired member ready-for-traffic", SETTLE, || async {
        let nodes = estate.nodes().await;
        let ready = |kind: &str| nodes.iter().filter(|n| n["kind"] == kind && n["status"] == "ready-for-traffic").count() as u32;
        (ready("node_admin") == want_admins && ready("rpc_node") == want_rpc).then_some(())
    })
    .await;
    estate.artifact("nodes-before.json", &json!(estate.nodes().await));

    let mut failures = Vec::new();
    for (i, op) in scenario.operations.iter().enumerate() {
        let (step, out, check): (String, Value, fn(&Value) -> bool) = match op {
            Operation::Put { target, key, value } => (
                format!("operations[{i}] put {target} {key}"),
                estate.probe(&["put", "--target", &format!("path:{target}"), "--key", &key.to_string(), "--value", value]),
                |v| v["outcome"] == "Reply",
            ),
            Operation::Cas { target, key, expected, value } => (
                format!("operations[{i}] cas {target} {key}"),
                estate.probe(&[
                    "cas", "--target", &format!("path:{target}"), "--key", &key.to_string(), "--expected", expected, "--value", value,
                ]),
                |v| v["outcome"] == "Reply" && v["reply"]["result"]["swapped"] == true,
            ),
            Operation::Delete { target, key } => (
                format!("operations[{i}] delete {target} {key}"),
                estate.probe(&["delete", "--target", &format!("path:{target}"), "--key", &key.to_string()]),
                |v| v["outcome"] == "Reply",
            ),
        };
        if !check(&out) {
            failures.push(Failure { step, reason: out.to_string() });
        }
    }
    for (i, a) in scenario.assert.iter().enumerate() {
        let Assertion::Get { target, key, equals, absent } = a;
        let out = estate.probe(&["get", "--target", &format!("path:{target}"), "--key", &key.to_string()]);
        let ok = out["outcome"] == "Reply"
            && out["reply"]["executing_node"].is_string()
            && match (equals, absent) {
                (Some(v), _) => out["reply"]["result"]["value"] == v.as_str(),
                (None, Some(true)) => out["reply"]["result"]["found"] == false,
                _ => false,
            };
        if !ok {
            failures.push(Failure { step: format!("assert[{i}] get {target} {key}"), reason: out.to_string() });
        }
    }
    estate.artifact("nodes-after.json", &json!(estate.nodes().await));
    estate.artifact(
        "result.json",
        &json!({"build_id": build_id, "failures": failures.iter().map(|f| json!({"step": f.step, "reason": f.reason})).collect::<Vec<_>>()}),
    );
    estate.shutdown().await;
    RunReport { build_id, failures }
}
