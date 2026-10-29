//! `on_fabric_ready`: an application's notice that its fabric is ready
//! (`docs/architecture/fabric-node-hooks.md:129`, `crates/rdm-admin/src/wiring.rs:91`).
//!
//! A node-admin executable registers `on_fabric_ready` in its [`Wiring`]'s [`FabricHooks`]. The
//! fabric-primary calls it once the fabric's `Ready` state is recorded on the Fabric record:
//! fire and forget, gating nothing. The context ([`FabricReadyCtx`]) carries `fabric_id`, the
//! accepted `build_id` and `attempt` whose change made the fabric ready, and that attempt's
//! observability `context`.
//!
//! Build: `cargo build --example on_fabric_ready` in `demo/`.
//! Run as an executable node-admin (in place of `rshape-node-admin`), or against a live demo
//! estate with `RDM_NODE_ADMIN_BIRTH` set to verify the registered notice and the fabric-primary's
//! emitted `on_fabric_ready` log lines.
use rdm_admin::wiring::{FabricHooks, FabricReadyCtx, FabricReadyNotice, Wiring};
use rdm_client::{birth_of_line, Builds, Fabrics, Filter, Peers, ScopeStatus};
use rdm_core::context::CallContext;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const SERVICE: &str = "example-on-fabric-ready";

/// The notice: counts how many times the fabric-primary reported `Ready` and logs the Build and attempt.
fn on_fabric_ready(told: Arc<AtomicU64>) -> FabricReadyNotice {
    Arc::new(move |ready: FabricReadyCtx| {
        let told = told.clone();
        Box::pin(async move {
            let n = told.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!(
                target: "example",
                fabric_id = %ready.fabric_id,
                build_id = %ready.build_id,
                attempt = ready.attempt,
                fabrics_ready = n,
                "on_fabric_ready: the fabric is ready"
            );
            println!(
                "on_fabric_ready notice #{n}: fabric_id={} build_id={} attempt={}",
                ready.fabric_id, ready.build_id, ready.attempt
            );
        })
    })
}

async fn verify_against_live_estate(raw_birth: &str) -> Result<(), String> {
    let _telemetry = rdm_transport::init_evidence_telemetry(SERVICE);
    let line = if let Some(rest) = raw_birth.trim().strip_prefix(rdm_client::BIRTH_LINE) {
        format!("{}{rest}", rdm_client::BIRTH_LINE)
    } else {
        format!("{}{}", rdm_client::BIRTH_LINE, raw_birth.trim())
    };
    let birth = birth_of_line(&line).ok_or("not a birth line")??;
    let peers = Peers::at(&birth).await?;
    let carrier = peers.carrier_at(birth.name.clone());

    let fabric = Fabrics::new(carrier.clone())
        .fact("fabric ready", std::time::Duration::from_secs(30), |f| {
            f.status == ScopeStatus::Ready
        })
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(fabric.status, ScopeStatus::Ready, "the live fabric must be Ready");

    let current = Builds::new(carrier)
        .get(&Filter::new().eq("current", ["true"]))
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .ok_or("build.get [current: true] returned no current Build")?;

    let told = Arc::new(AtomicU64::new(0));
    let hooks = FabricHooks {
        on_fabric_ready: Some(on_fabric_ready(told.clone())),
        ..FabricHooks::no_app_work()
    };
    let notice = hooks.on_fabric_ready.as_ref().expect("on_fabric_ready registered");
    notice(FabricReadyCtx {
        fabric_id: fabric.id.clone(),
        build_id: current.build_id.to_string(),
        attempt: current.attempt,
        context: CallContext::default(),
    })
    .await;
    assert_eq!(told.load(Ordering::SeqCst), 1);

    if let Ok(root) = std::fs::read_to_string("target/demo-estate.current") {
        let log_path = std::path::Path::new(root.trim()).join("admin1.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            for line in log.lines().filter(|l| l.contains("on_fabric_ready: the fabric is ready")) {
                println!("estate admin1.log: {line}");
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Ok(raw_birth) = std::env::var("RDM_NODE_ADMIN_BIRTH") {
        if let Err(e) = verify_against_live_estate(&raw_birth).await {
            eprintln!("on_fabric_ready failed: {e}");
            std::process::exit(1);
        }
        return;
    }
    let told = Arc::new(AtomicU64::new(0));
    rdm_admin::entry::run_with(SERVICE, move |cfg| Wiring {
        fabric_hooks: FabricHooks {
            on_fabric_ready: Some(on_fabric_ready(told.clone())),
            ..FabricHooks::no_app_work()
        },
        certs: rdm_admin::entry::material_or_exit(SERVICE, cfg),
        ..Wiring::default()
    })
    .await
}
