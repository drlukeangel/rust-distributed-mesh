//! Scenario runner: brings the scenario's shape up through node-admin Build on
//! the selected provider, then executes every operation and assertion over real
//! Node RPC through `rafka-rpc-probe` (PRD §6: no in-process shortcut).

use crate::estate::{wait_for, Estate, Owner};
use crate::replay::{EvidenceOwner, Fault, ManifestError, ReplayManifest, ScheduledFault, Source, StepOutcome, SCHEMA_VERSION};
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

/// A run that can be replayed: the outcomes of one scenario run with its seed and fault schedule,
/// as the replay manifest that reproduces it.
pub struct ReplayableRun {
    pub manifest: ReplayManifest,
    pub failures: Vec<Failure>,
}

/// What a probe reply says that the scenario asserts on, with every locator of the serving
/// process (node ids, routes, incarnations) left out.
fn semantic(out: &Value) -> (String, Value) {
    let outcome = out["outcome"].as_str().unwrap_or("none").to_string();
    let r = &out["reply"]["result"];
    let mut result = serde_json::Map::new();
    for k in ["swapped", "found", "value"] {
        if !r[k].is_null() {
            result.insert(k.into(), r[k].clone());
        }
    }
    (outcome, Value::Object(result))
}

/// Run `scenario` once under `faults`, recording typed outcomes in order: the replay manifest of
/// the run. The Build, the operations, the restarts and the final proof reads all go through
/// public surfaces (node-admin Build, the probe); a restart is a Build attempt of the rectifier.
pub async fn run_replayable(
    scenario: &Scenario,
    provider: Option<&str>,
    test: &str,
    seed: u64,
    source: Source,
    faults: Vec<ScheduledFault>,
) -> Result<ReplayableRun, ManifestError> {
    let provider = provider.unwrap_or(&scenario.provider).to_string();
    let mut sc = scenario.clone();
    sc.provider = provider.clone();
    let owner = EvidenceOwner { product: sc.product.clone(), feature: sc.feature.clone(), subfeature: sc.subfeature.clone(), rung: sc.rung.clone(), provider: provider.clone(), test: test.into() };
    let mut manifest = ReplayManifest {
        schema_version: SCHEMA_VERSION,
        owner: owner.clone(),
        seed,
        source,
        shapes: sc.meshes.clone(),
        scenario: sc.clone(),
        fault_schedule: faults,
        outcomes: Vec::new(),
        final_state: Default::default(),
    };
    manifest.validate()?;

    let first_mesh = sc.meshes.first().map(|m| m.name.clone()).unwrap_or_else(|| "mesh1".into());
    let mut estate = Estate::bootstrap(Owner { product: owner.product, feature: owner.feature, subfeature: owner.subfeature, rung: owner.rung, provider, test: test.into() }, &sc.fabric, &first_mesh).await;
    estate.set_seed(seed);
    // A run that fails or crashes still leaves its complete manifest: the outcomes are filled in at the end.
    estate.artifact("replay-manifest.json", &serde_json::to_value(&manifest).expect("manifest serializes"));
    estate.artifact("scenario.json", &serde_json::to_value(&sc).expect("scenario serializes"));
    let body = sc.build_request();
    let (status, accepted) = estate.post("/api/build", &body).await;
    assert_eq!(status, 202, "POST /api/build: {accepted}");
    let build_id = accepted["build_id"].as_str().expect("build_id").to_string();
    estate.await_build(&build_id, SETTLE).await;
    let want_admins: u32 = sc.meshes.iter().map(|m| m.node_admin).sum();
    let want_rpc: u32 = sc.meshes.iter().map(|m| m.rpc_node).sum();
    wait_for("every desired member ready-for-traffic", SETTLE, || async {
        let nodes = estate.nodes().await;
        let ready = |kind: &str| nodes.iter().filter(|n| n["kind"] == kind && n["status"] == "ready-for-traffic").count() as u32;
        (ready("node_admin") == want_admins && ready("rpc_node") == want_rpc).then_some(())
    })
    .await;

    let mut failures = Vec::new();
    let mut outcomes = Vec::new();
    let mut fault_no = 0;
    for i in 0..=sc.operations.len() {
        for sf in manifest.fault_schedule.iter().filter(|f| f.before_operation == i) {
            let Fault::RestartNode { node } = &sf.fault;
            let before = estate.node(node).await;
            let (status, r) = estate.post(&format!("/api/nodes/{node}/restart"), &Value::Null).await;
            let step = format!("fault[{fault_no}]");
            fault_no += 1;
            if status != 202 {
                failures.push(Failure { step: step.clone(), reason: format!("restart {node}: {status} {r}") });
                outcomes.push(StepOutcome { step, action: "restart_node".into(), target: node.clone(), key: None, outcome: format!("Refused({status})"), result: Value::Null });
                continue;
            }
            estate.await_attempt(r["build_id"].as_str().expect("build_id"), Estate::attempt_of(&r), SETTLE).await;
            let after = wait_for(&format!("{node} reborn ready under a new incarnation"), SETTLE, || async {
                let n = estate.node_opt(node).await?;
                (n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"]).then_some(n)
            })
            .await;
            outcomes.push(StepOutcome {
                step,
                action: "restart_node".into(),
                target: node.clone(),
                key: None,
                outcome: "Restarted".into(),
                result: json!({ "same_node_id": after["node_id"] == before["node_id"], "new_incarnation": after["incarnation_id"] != before["incarnation_id"] }),
            });
        }
        let Some(op) = sc.operations.get(i) else { break };
        let (action, target, key, args, check): (&str, &String, u64, Vec<String>, fn(&Value) -> bool) = match op {
            Operation::Put { target, key, value } => ("put", target, *key, vec!["put".into(), "--target".into(), format!("path:{target}"), "--key".into(), key.to_string(), "--value".into(), value.clone()], |v| v["outcome"] == "Reply"),
            Operation::Cas { target, key, expected, value } => (
                "cas",
                target,
                *key,
                vec!["cas".into(), "--target".into(), format!("path:{target}"), "--key".into(), key.to_string(), "--expected".into(), expected.clone(), "--value".into(), value.clone()],
                |v| v["outcome"] == "Reply" && v["reply"]["result"]["swapped"] == true,
            ),
            Operation::Delete { target, key } => ("delete", target, *key, vec!["delete".into(), "--target".into(), format!("path:{target}"), "--key".into(), key.to_string()], |v| v["outcome"] == "Reply"),
        };
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = estate.probe(&argv);
        let step = format!("operation[{i}]");
        if !check(&out) {
            failures.push(Failure { step: step.clone(), reason: out.to_string() });
        }
        let (outcome, result) = semantic(&out);
        outcomes.push(StepOutcome { step, action: action.into(), target: target.clone(), key: Some(key), outcome, result });
    }
    for (i, a) in sc.assert.iter().enumerate() {
        let Assertion::Get { target, key, equals, absent } = a;
        let out = estate.probe(&["get", "--target", &format!("path:{target}"), "--key", &key.to_string()]);
        let ok = out["outcome"] == "Reply"
            && match (equals, absent) {
                (Some(v), _) => out["reply"]["result"]["value"] == v.as_str(),
                (None, Some(true)) => out["reply"]["result"]["found"] == false,
                _ => false,
            };
        let step = format!("assert[{i}]");
        if !ok {
            failures.push(Failure { step: step.clone(), reason: out.to_string() });
        }
        let (outcome, result) = semantic(&out);
        outcomes.push(StepOutcome { step, action: "get".into(), target: target.clone(), key: Some(*key), outcome, result });
    }
    // The final proof state: every (target, key) the scenario touches, read once more.
    let mut touched: std::collections::BTreeSet<(String, u64)> = std::collections::BTreeSet::new();
    for o in &sc.operations {
        match o {
            Operation::Put { target, key, .. } | Operation::Cas { target, key, .. } | Operation::Delete { target, key } => touched.insert((target.clone(), *key)),
        };
    }
    for Assertion::Get { target, key, .. } in &sc.assert {
        touched.insert((target.clone(), *key));
    }
    for (target, key) in touched {
        let out = estate.probe(&["get", "--target", &format!("path:{target}"), "--key", &key.to_string()]);
        let (outcome, result) = semantic(&out);
        manifest.final_state.insert(format!("{target}/{key}"), json!({ "outcome": outcome, "found": result["found"], "value": result["value"] }));
    }
    manifest.outcomes = outcomes;
    estate.artifact("replay-manifest.json", &serde_json::to_value(&manifest).expect("manifest serializes"));
    estate.shutdown().await;
    Ok(ReplayableRun { manifest, failures })
}
