//! Declared secrets (`RDM_DECLARED_SECRETS`): a value only the application's own kinds read, carried by
//! RDM and never written anywhere (Q150).
//!
//! The application declares each secret in its node-admin's [`Wiring`] by name, with the node kinds that
//! read it: here `EXAMPLE_APP_TOKEN`, read by brokers. The bootstrap node-admin reads each declared
//! name once from its own environment, so the value is set where the fabric is started. Every node-admin
//! RDM launches is written the values beside the fabric's root, so whichever admin creates a broker can
//! set it; each admin keeps them beside its data dir and reads them back on a start, so a value never
//! changes while the fabric runs. The value is set only in the environment of the kinds its declaration
//! names and withheld from every other process; it is never written to a row, a Build record, a
//! receipt, the launch record or a span, and a spawn error is scrubbed of it. A name RDM sets or passes
//! through (`RDM_*`, `TRACEPARENT`, ...) or a name declared twice refuses the admin's start by name.
//!
//! This one executable is both halves, by the launch it is given: a launched broker reads its secret
//! and runs as an ordinary broker; anything else runs as the node-admin that declares it. A Build names
//! it for the node-admin and broker paths. Start the estate with the value in the environment:
//! `EXAMPLE_APP_TOKEN=<value> scripts/demo-estate.sh up` with this executable as the Day-0 admin.
//!
//! Build: `cargo build --example declared_secrets` in `demo/`.
use anyhow::{anyhow, Result};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::launch::DeclaredSecret;
use rdm_core::{NodeKind, PathName};

const SERVICE: &str = "example-declared-secrets";
/// The application's one declared secret.
const TOKEN: &str = "EXAMPLE_APP_TOKEN";

/// The application's declarations, as every one of its node-admins registers them.
fn declared() -> Vec<DeclaredSecret> {
    vec![DeclaredSecret { name: TOKEN.to_string(), kinds: vec![NodeKind::Broker] }]
}

#[tokio::main]
async fn main() -> Result<()> {
    let launched_as = std::env::var("RDM_NODE_NAME").ok().and_then(|n| n.parse::<PathName>().ok());
    match launched_as {
        Some(name) if name.kind == NodeKind::Broker => {
            // The broker's own read of its secret. Its value is the application's: log only that it is held.
            let held = std::env::var(TOKEN).map_err(|_| anyhow!("{name} was launched without {TOKEN}"))?;
            eprintln!("{name}: {TOKEN} held ({} bytes)", held.len());
            rdm_node::run(rdm_node::Role { kind: NodeKind::Broker }, &[]).await
        }
        Some(name) if name.kind != NodeKind::NodeAdmin => Err(anyhow!("{name}: this example runs node-admins and brokers only")),
        _ => {
            rdm_admin::entry::run_with(SERVICE, |cfg| Wiring {
                declared: declared(),
                fabric_hooks: FabricHooks::no_app_work(),
                certs: rdm_admin::entry::material_or_exit(SERVICE, cfg),
                ..Wiring::default()
            })
            .await;
            Ok(())
        }
    }
}
