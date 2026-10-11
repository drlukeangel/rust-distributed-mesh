//! The restart hold an admin's book carries is the Build facts' truth from the moment a fact enters
//! the admin's log (node-restart.md "Completion: node.restarted after the whole start completes"):
//! the `NodeRestarting` pre-event opens it, and the `restart-node:<path>` `Complete` receipt or the
//! attempt's `Converged` verdict (node-rpc-envelope.md: progress waits on no gossip) closes it, each
//! inside the append that brings it, never at the next hierarchy round. A restart door reading the
//! book after that fact must not refuse a live node as still `restarting`.
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`) whose rounds are stretched far
//! past the cell's bound (`RDM_GOSSIP_INTERVAL_MS`, `RDM_BACKBONE_INTERVAL_MS`), in a process of its
//! own: nothing but the fact's own arrival can move the hold inside the bound.

use rafka_mesh_entity::{IncarnationId, LifecycleOp, NodeId};
use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptReceipt, BuildStepReceipt, StepOutcome};
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

/// Far past any round this cell could see: the rounds are stretched to ten minutes.
const STRETCHED_MS: &str = "600000";
/// What "the fact moved the hold" is given before the cell fails: the rounds cannot run in it.
const NEVER: Duration = Duration::from_secs(20);

fn evidence_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/3016/unit").join(cell),
    }
}

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// Every span this process emitted, from the evidence exporter's JSONL files in `dir`.
fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl")) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
}

/// CONTRACT (node-restart.md "Completion"; membership.rs `restarts_from_facts`: "each is open here
/// until its operation's `Complete` is receipted"): a restart's `NodeRestarting` receipt entering an
/// admin's Build log opens the hold on that birth in the admin's book, and the restart's
/// `restart-node:<path>` `Complete` receipt entering the log closes it, each with no hierarchy round
/// between the receipt and the book's overlay watch ticking. The close is spanned
/// `rdm.mesh.membership.update.via-restart-closed-by-facts` naming the birth, the Build and the attempt.
/// Must NOT happen: the hold outliving its `Complete` until a round re-derives it (the restart door
/// then refuses the live node `node-not-live`, status `restarting`).
#[test]
fn a_restart_hold_opens_and_closes_as_its_receipts_enter_the_log() {
    let cell = "a_restart_hold_opens_and_closes_as_its_receipts_enter_the_log";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let evidence = evidence_dir(cell);
    let _ = std::fs::remove_dir_all(&evidence);
    std::fs::create_dir_all(&evidence).unwrap();
    std::env::set_var("RDM_GOSSIP_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_BACKBONE_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_EVIDENCE_DIR", &evidence);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-3016");
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("restart-hold-{}", NodeId::mint()));
        let wiring = Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        let build_id = running.control.accepted.build_id().await.expect("the Day-0 admin holds its accepted Build");
        let book = running.membership.book.clone();
        let builds = running.runner.builds.clone();

        let path: rafka_node_admin_core::model::PathName = "mesh1.rpc.1".parse().unwrap();
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let op = LifecycleOp {
            build_id: build_id.0.clone(),
            attempt: 2,
            operation: format!("restart-node:{path}"),
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            name: path.clone(),
            event_at_rafka_ms: 1,
        };
        let receipt = |operation: String, step: &str| BuildStepReceipt {
            build_id: build_id.clone(),
            attempt: 2,
            operation,
            step: step.into(),
            outcome: StepOutcome::Complete,
            // The pre-event records the operation it opens; a Complete commits no output.
            output: (step == "NodeRestarting").then(|| serde_json::to_value(&op).unwrap()),
            executor: Some("mesh2.admin.1".into()),
        };

        // The pre-event enters the log: the hold opens on the exact birth, the overlay watch ticks.
        let mut overlays = book.overlay_changes();
        overlays.borrow_and_update();
        builds.append_step_receipt(&receipt(format!("retire-node:{path}"), "NodeRestarting")).await.unwrap();
        tokio::time::timeout(NEVER, overlays.changed()).await.expect("the NodeRestarting receipt entered the log and no hold opened: the hold waits on a round").unwrap();
        assert_eq!(book.restart_of(node_id.as_str(), &incarnation).map(|o| o.operation), Some(op.operation.clone()), "the hold is the restart's, on the exact birth");

        // The restart's Complete enters the log: the hold closes, the overlay watch ticks.
        overlays.borrow_and_update();
        builds.append_step_receipt(&receipt(op.operation.clone(), "Complete")).await.unwrap();
        tokio::time::timeout(NEVER, overlays.changed()).await.expect("the restart's Complete receipt entered the log and the hold stayed open: it waits on a round").unwrap();
        assert_eq!(book.restart_of(node_id.as_str(), &incarnation), None, "the restart is over when its Complete is receipted");

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    drop(rt);
    drop(telemetry);
    let spans = collect_spans(&evidence);
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    let opened = named("rdm.mesh.membership.update.via-node-restarting");
    let closed = named("rdm.mesh.membership.update.via-restart-closed-by-facts");
    assert_eq!(opened.len(), 1, "one hold opened: {opened:?}");
    assert_eq!(closed.len(), 1, "one hold closed, by its Complete: {closed:?}");
    let text = closed[0].to_string();
    assert!(text.contains("mesh1.rpc.1") && text.contains("restart-node:mesh1.rpc.1"), "the close names the birth and the operation: {text}");
}

/// CONTRACT (node-restart.md: "node.restarted after the whole start completes; no event is inferred
/// from gossip"; membership.rs `restarts_from_facts`: "each is open here until its operation's
/// `Complete` is receipted"): a restart hold the `NodeRestarting` frame opened before any of its
/// receipts reached this admin's Build log stays open while an unrelated fact enters the log (here a
/// `retire-node` pre-event of another node, which ticks the overlay watch as its own hold opens), and
/// closes only when its own `restart-node:<path>` `Complete` is receipted, spanned
/// `rdm.mesh.membership.update.via-restart-closed-by-facts` once.
/// Must NOT happen: the unrelated fact's recomputation closing the hold because the facts do not name
/// it yet (the restarting birth then reads routable mid-restart).
#[test]
fn a_frame_opened_restart_hold_stays_open_through_an_unrelated_fact_and_closes_on_its_complete() {
    let cell = "a_frame_opened_restart_hold_stays_open_through_an_unrelated_fact_and_closes_on_its_complete";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let evidence = evidence_dir(cell);
    let _ = std::fs::remove_dir_all(&evidence);
    std::fs::create_dir_all(&evidence).unwrap();
    std::env::set_var("RDM_GOSSIP_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_BACKBONE_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_EVIDENCE_DIR", &evidence);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-3016");
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("restart-hold-frame-{}", NodeId::mint()));
        let wiring = Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        let build_id = running.control.accepted.build_id().await.expect("the Day-0 admin holds its accepted Build");
        let book = running.membership.book.clone();
        let builds = running.runner.builds.clone();

        let op_at = |path: &str, kind: &str| {
            let path: rafka_node_admin_core::model::PathName = path.parse().unwrap();
            LifecycleOp {
                build_id: build_id.0.clone(),
                attempt: 2,
                operation: format!("{kind}:{path}"),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
                name: path,
                event_at_rafka_ms: 1,
            }
        };
        let restart = op_at("mesh1.rpc.1", "restart-node");
        let retire = op_at("mesh1.rpc.2", "retire-node");
        let receipt = |operation: String, step: &str, output: Option<&LifecycleOp>| BuildStepReceipt {
            build_id: build_id.clone(),
            attempt: 2,
            operation,
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: output.map(|o| serde_json::to_value(o).unwrap()),
            executor: Some("mesh2.admin.1".into()),
        };

        // The NodeRestarting frame opens the hold (the call `Frame::NodeRestarting` makes) before any
        // receipt of the restart has reached this admin's log.
        let mut overlays = book.overlay_changes();
        overlays.borrow_and_update();
        assert!(book.restarting(restart.clone()), "the frame opens the hold");
        assert_eq!(book.restart_of(restart.node_id.as_str(), &restart.incarnation).map(|o| o.operation), Some(restart.operation.clone()));

        // An unrelated fact enters the log: another node's retire pre-event. Its own hold opening is
        // the tick that proves the facts were recomputed; the restart hold must still be open.
        overlays.borrow_and_update();
        builds.append_step_receipt(&receipt(retire.operation.clone(), "NodeDeleting", Some(&retire))).await.unwrap();
        tokio::time::timeout(NEVER, overlays.changed()).await.expect("the retire pre-event entered the log and no overlay moved: the facts were not recomputed").unwrap();
        assert!(book.restart_of(retire.node_id.as_str(), &retire.incarnation).is_none(), "the retire is no restart");
        assert_eq!(
            book.restart_of(restart.node_id.as_str(), &restart.incarnation).map(|o| o.operation),
            Some(restart.operation.clone()),
            "an unrelated fact closed the restart hold: the facts have not receipted its Complete"
        );

        // The restart's own Complete enters the log: the hold closes.
        overlays.borrow_and_update();
        builds.append_step_receipt(&receipt(restart.operation.clone(), "Complete", None)).await.unwrap();
        tokio::time::timeout(NEVER, overlays.changed()).await.expect("the restart's Complete receipt entered the log and the hold stayed open").unwrap();
        assert_eq!(book.restart_of(restart.node_id.as_str(), &restart.incarnation), None, "the restart is over when its Complete is receipted");

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    drop(rt);
    drop(telemetry);
    let spans = collect_spans(&evidence);
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    let opened = named("rdm.mesh.membership.update.via-node-restarting");
    let closed = named("rdm.mesh.membership.update.via-restart-closed-by-facts");
    assert_eq!(opened.len(), 1, "one restart hold opened, by the frame: {opened:?}");
    assert_eq!(closed.len(), 1, "one restart hold closed, by its Complete: {closed:?}");
    let text = closed[0].to_string();
    assert!(text.contains("mesh1.rpc.1") && text.contains("restart-node:mesh1.rpc.1"), "the close names the birth and the operation: {text}");
}

/// CONTRACT (node-rpc-envelope.md "Frames, durable receipts and reattachment": "Progress waits on no
/// gossip; Build-topic facts remain durability and recovery evidence"; node-restart.md "Completion":
/// "no event is inferred from gossip"; rafka-node-rpc-contract build.rs: a `Step` frame only after
/// the step's `Complete` receipt is durable on its executor, then the terminal): an attempt's
/// `Converged` verdict, which the fabric-primary records from the executor's stream terminal, ends
/// every restart hold of that attempt in this admin's book, with no `restart-node:<path>`
/// `Complete` receipt in the log (that receipt reaches the fabric-primary only by Build-topic
/// gossip). The hold is closed by the time the verdict's append returns: no reader of the log can
/// see the attempt `Converged` while the book still holds the restart. The close is spanned
/// `rdm.mesh.membership.update.via-restart-closed-by-facts` once, `by = attempt-converged`.
/// Must NOT happen: the hold outliving its attempt's `Converged` until the gossiped `Complete`
/// arrives (the restart door then refuses the live node `node-not-live`, status `restarting`,
/// mesh_identity__canonical_ids :106), or a hold of another attempt closing on this verdict.
#[test]
fn a_restart_hold_closes_when_its_attempt_converges_before_its_complete_receipt_arrives() {
    let cell = "a_restart_hold_closes_when_its_attempt_converges_before_its_complete_receipt_arrives";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let evidence = evidence_dir(cell);
    let _ = std::fs::remove_dir_all(&evidence);
    std::fs::create_dir_all(&evidence).unwrap();
    std::env::set_var("RDM_GOSSIP_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_BACKBONE_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_EVIDENCE_DIR", &evidence);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-3016");
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("restart-hold-converged-{}", NodeId::mint()));
        let wiring = Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        let build_id = running.control.accepted.build_id().await.expect("the Day-0 admin holds its accepted Build");
        let book = running.membership.book.clone();
        let builds = running.runner.builds.clone();

        // An attempt number the Day-0 Build has given no verdict: the first verdict of an attempt is its one.
        const ATTEMPT: u32 = 9;
        let op_at = |path: &str, attempt: u32| {
            let path: rafka_node_admin_core::model::PathName = path.parse().unwrap();
            LifecycleOp {
                build_id: build_id.0.clone(),
                attempt,
                operation: format!("restart-node:{path}"),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
                name: path,
                event_at_rafka_ms: 1,
            }
        };
        // The restart this attempt ran, and one of another attempt the verdict must not end.
        let restart = op_at("mesh1.rpc.1", ATTEMPT);
        let other = op_at("mesh1.rpc.2", ATTEMPT + 1);
        let pre_event = |op: &LifecycleOp| BuildStepReceipt {
            build_id: build_id.clone(),
            attempt: op.attempt,
            operation: format!("retire-node:{}", op.name),
            step: "NodeRestarting".into(),
            outcome: StepOutcome::Complete,
            output: Some(serde_json::to_value(op).unwrap()),
            executor: Some("mesh2.admin.1".into()),
        };

        // Both pre-events enter the log: both holds open on their exact births.
        let mut overlays = book.overlay_changes();
        overlays.borrow_and_update();
        builds.append_step_receipt(&pre_event(&restart)).await.unwrap();
        builds.append_step_receipt(&pre_event(&other)).await.unwrap();
        tokio::time::timeout(NEVER, async {
            while book.restart_of(other.node_id.as_str(), &other.incarnation).is_none() || book.restart_of(restart.node_id.as_str(), &restart.incarnation).is_none() {
                overlays.changed().await.unwrap();
            }
        })
        .await
        .expect("the NodeRestarting receipts entered the log and the holds did not open");

        // The attempt's Converged enters the log (the fabric-primary's own verdict from the stream's
        // terminal), no restart-node Complete with it: the hold is over when the append returns.
        builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: build_id.clone(), attempt: ATTEMPT, outcome: AttemptOutcome::Converged }).await.unwrap();
        assert_eq!(
            book.restart_of(restart.node_id.as_str(), &restart.incarnation),
            None,
            "attempt {ATTEMPT} is Converged in the log and its restart is still held: the hold waits on the gossiped Complete receipt"
        );
        assert_eq!(
            book.restart_of(other.node_id.as_str(), &other.incarnation).map(|o| o.operation),
            Some(other.operation.clone()),
            "attempt {ATTEMPT}'s verdict closed a restart of attempt {}", ATTEMPT + 1
        );

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    drop(rt);
    drop(telemetry);
    let spans = collect_spans(&evidence);
    let closed: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-restart-closed-by-facts").cloned().collect();
    assert_eq!(closed.len(), 1, "one restart hold closed, by its attempt's Converged: {closed:?}");
    let text = closed[0].to_string();
    assert!(text.contains("restart-node:mesh1.rpc.1") && text.contains("attempt-converged"), "the close names the operation and its cause: {text}");
}
