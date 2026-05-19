//! Soak runner — picks a random primitive every N seconds, executes, detects, records.
//! Per chaos PRD §3. Sprint 11 phase 1 ships a smoke variant (5min run with 10 events).

use crate::primitives::{KillNode, RestartNode};
use crate::{ChaosContext, ChaosOutcome, ChaosPrimitive, DetectionResult};
use rand::Rng;
use serde::Serialize;
use std::time::{Duration, Instant};

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
    let started = Instant::now();
    let started_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut events: Vec<SoakEvent> = Vec::new();
    let mut passed = 0usize;
    let mut failed_timeout = 0usize;
    let mut failed_assertion = 0usize;

    while started.elapsed() < duration {
        // pick a primitive
        let pick: u8 = {
            let mut rng = ctx.rng.lock().await;
            rng.gen_range(0..2)
        };
        let primitive: Box<dyn ChaosPrimitive> = match pick {
            0 => Box::new(KillNode { target: None }),
            _ => Box::new(RestartNode { target: None }),
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
