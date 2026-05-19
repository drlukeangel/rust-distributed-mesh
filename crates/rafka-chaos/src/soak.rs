//! Soak runner — picks a random primitive every N seconds, executes, detects, records.
//! Per chaos PRD §3. Sprint 11 phase 1 ships a smoke variant (5min run with 10 events).

use crate::primitives::{BurstKill, DiskFull, KillNode, RestartNode};
use crate::{ChaosContext, ChaosOutcome, ChaosPrimitive, DetectionResult};
use rand::Rng;
use serde::Serialize;
use serde_json::json;
use std::time::{Duration, Instant};

/// Minimum target pool size — if /api/spawned drops below this, soak tops up by
/// spawning fresh subprocesses before the next primitive. Keeps long soaks viable
/// (otherwise kill-heavy primitives drain the pool and remaining events all fail
/// with InvalidTarget).
const MIN_POOL_SIZE: usize = 4;
const NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry"];

#[derive(Serialize)]
pub struct SoakEvent {
    pub timestamp_ms: u64,
    pub primitive: String,
    pub targets: Vec<String>,
    pub detection: DetectionLabel,
    pub waited_ms: u64,
}

#[derive(Serialize)]
pub enum DetectionLabel {
    Passed,
    FailedTimeout,
    FailedAssertion(String),
}

#[derive(Serialize)]
pub struct SoakReport {
    pub seed: u64,
    pub started_ms: u64,
    pub ended_ms: u64,
    pub event_count: usize,
    pub passed: usize,
    pub failed_timeout: usize,
    pub failed_assertion: usize,
    pub events: Vec<SoakEvent>,
}

/// Run a soak for the given duration. Picks primitives at random per `interval`.
pub async fn run_soak(
    ctx: &ChaosContext,
    duration: Duration,
    interval: Duration,
    seed: u64,
) -> SoakReport {
    use std::io::Write;
    let started = Instant::now();
    let started_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut events: Vec<SoakEvent> = Vec::new();
    let mut passed = 0usize;
    let mut failed_timeout = 0usize;
    let mut failed_assertion = 0usize;
    // Every 10 events, print a heartbeat line so log readers see progress in real
    // time. Without this the soak appears hung in stdout-redirected background
    // runs (block-buffered) even when chaos events are flowing fine via Jaeger.
    const HEARTBEAT_EVERY: usize = 10;

    while started.elapsed() < duration {
        // Top up target pool if it's too thin — keeps long soaks viable.
        maintain_pool(ctx).await;

        // pick a primitive
        let pick: u8 = {
            let mut rng = ctx.rng.lock().await;
            rng.gen_range(0..4)
        };
        let primitive: Box<dyn ChaosPrimitive> = match pick {
            0 => Box::new(KillNode { target: None }),
            1 => Box::new(RestartNode { target: None }),
            2 => Box::new(BurstKill { count: 2 }),
            _ => Box::new(DiskFull { target: None, max_bytes: 4 * 1024 * 1024 }),
        };

        let exec_started = Instant::now();
        let outcome_result = primitive.execute(ctx).await;
        let outcome = match outcome_result {
            Ok(o) => o,
            Err(e) => {
                events.push(SoakEvent {
                    timestamp_ms: timestamp_ms(),
                    primitive: primitive.name().into(),
                    targets: vec![],
                    detection: DetectionLabel::FailedAssertion(format!("execute: {e}")),
                    waited_ms: exec_started.elapsed().as_millis() as u64,
                });
                failed_assertion += 1;
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let detect_result = primitive.detect(ctx, &outcome, 30_000).await;
        let (label, waited_ms) = match detect_result {
            Ok(DetectionResult::Passed { waited_ms }) => {
                passed += 1;
                (DetectionLabel::Passed, waited_ms)
            }
            Ok(DetectionResult::FailedTimeout { waited_ms }) => {
                failed_timeout += 1;
                (DetectionLabel::FailedTimeout, waited_ms)
            }
            Ok(DetectionResult::FailedAssertion { msg, waited_ms }) => {
                failed_assertion += 1;
                (DetectionLabel::FailedAssertion(msg), waited_ms)
            }
            Err(e) => {
                failed_assertion += 1;
                (
                    DetectionLabel::FailedAssertion(format!("detect: {e}")),
                    exec_started.elapsed().as_millis() as u64,
                )
            }
        };
        events.push(SoakEvent {
            timestamp_ms: timestamp_ms(),
            primitive: primitive.name().into(),
            targets: outcome.targets,
            detection: label,
            waited_ms,
        });
        // Heartbeat every N events so background-redirected log files show
        // forward motion even with block-buffered stdout.
        if events.len() % HEARTBEAT_EVERY == 0 {
            let elapsed_s = started.elapsed().as_secs();
            println!(
                "soak progress: events={} passed={} failed={}  elapsed={}s",
                events.len(),
                passed,
                failed_timeout + failed_assertion,
                elapsed_s
            );
            let _ = std::io::stdout().flush();
        }
        tokio::time::sleep(interval).await;
    }

    let ended_ms = timestamp_ms();
    let event_count = events.len();
    SoakReport {
        seed,
        started_ms,
        ended_ms,
        event_count,
        passed,
        failed_timeout,
        failed_assertion,
        events,
    }
}

/// Top up the UI-spawned subprocess pool to MIN_POOL_SIZE if it has dropped lower.
/// Spawns one subprocess per missing slot, round-robin through NODE_TYPES.
async fn maintain_pool(ctx: &ChaosContext) {
    let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
    let current: usize = match ctx.http.get(&url).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => body["spawned"].as_array().map(|a| a.len()).unwrap_or(0),
            Err(_) => 0,
        },
        Err(_) => return, // topology-ui unreachable; let next primitive raise the error
    };
    if current >= MIN_POOL_SIZE {
        return;
    }
    let to_spawn = MIN_POOL_SIZE - current;
    let spawn_url = format!("{}/api/nodes/spawn", ctx.topology_ui_url);
    for i in 0..to_spawn {
        let node_type = NODE_TYPES[i % NODE_TYPES.len()];
        let _ = ctx
            .http
            .post(&spawn_url)
            .json(&json!({"node_type": node_type}))
            .send()
            .await;
    }
    // Brief wait so the spawned subprocesses appear in /api/spawned before the next
    // primitive picks targets.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
}

fn timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _outcome_targets(o: &ChaosOutcome) -> Vec<String> {
    o.targets.clone()
}
