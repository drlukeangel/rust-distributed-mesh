//! `on_build_started` and `on_build_ended`: an application's notices of each Build the fabric runs
//! (fabric-node-hooks.md).
//!
//! The fabric-primary calls `on_build_started` from the write of a Build's `rdm.build.started` event,
//! at acceptance or when a caller's resubmission runs a failed or incomplete Build again, and
//! `on_build_ended` from the write of its `rdm.build.ended`. Both are fire and forget: they gate
//! nothing. `BuildStartedCtx` names the fabric, the Build and its observability context;
//! `BuildEndedCtx` adds how it ended (Q346): `Complete`, every section finished; `Incomplete`, naming
//! each section that did not finish (its mesh, operation, step, reason and code); or `Failed`, it could
//! not start, with its reason and code.
//!
//! Here the application keeps the set of Builds it was told started and not yet ended, and writes one
//! log line per notice; an incomplete or failed Build's line is a warning naming why. A product hangs its own work
//! here: holding its maintenance while a Build runs, paging on a failed one.
//!
//! Build: `cargo build --example on_build_started_ended` in `demo/`. Run: as a node-admin executable, in
//! place of `rshape-node-admin`; every node-admin registers the same notices, since whichever admin is
//! fabric-primary calls them.
use rdm_admin::wiring::{BuildEndedCtx, BuildEndedNotice, BuildEndedState, BuildStartedCtx, BuildStartedNotice, FabricHooks, Wiring};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

const SERVICE: &str = "example-on-build-started-ended";

/// The Builds this admin was told started and has not yet been told ended.
type Running = Arc<Mutex<BTreeSet<String>>>;

fn on_build_started(running: Running) -> BuildStartedNotice {
    Arc::new(move |started: BuildStartedCtx| {
        let running = running.clone();
        Box::pin(async move {
            let in_flight = {
                let mut r = running.lock().unwrap();
                r.insert(started.build_id.clone());
                r.len()
            };
            tracing::info!(target: "example", build_id = %started.build_id, in_flight, "on_build_started");
        })
    })
}

fn on_build_ended(running: Running) -> BuildEndedNotice {
    Arc::new(move |ended: BuildEndedCtx| {
        let running = running.clone();
        Box::pin(async move {
            let in_flight = {
                let mut r = running.lock().unwrap();
                r.remove(&ended.build_id);
                r.len()
            };
            match &ended.state {
                BuildEndedState::Complete => {
                    tracing::info!(target: "example", build_id = %ended.build_id, in_flight, "on_build_ended: complete")
                }
                BuildEndedState::Incomplete { sections } => {
                    let unfinished: Vec<String> = sections.iter().map(|s| format!("{} {} {}: {}", s.mesh, s.operation, s.step, s.reason)).collect();
                    tracing::warn!(target: "example", build_id = %ended.build_id, sections = ?unfinished, in_flight, "on_build_ended: incomplete")
                }
                BuildEndedState::Failed { reason, code } => {
                    tracing::warn!(target: "example", build_id = %ended.build_id, failure = %reason, code = ?code, in_flight, "on_build_ended: failed")
                }
            }
        })
    })
}

#[tokio::main]
async fn main() {
    let running: Running = Arc::default();
    rdm_admin::entry::run_with(SERVICE, move |cfg| Wiring {
        fabric_hooks: FabricHooks { on_build_started: Some(on_build_started(running.clone())), on_build_ended: Some(on_build_ended(running)), ..FabricHooks::no_app_work() },
        certs: rdm_admin::entry::material_or_exit(SERVICE, cfg),
        ..Wiring::default()
    })
    .await
}
