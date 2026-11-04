//! Declared secrets (`RDM_DECLARED_SECRETS`): a value only the application's own types read, carried by
//! RDM and never written anywhere (Q150).
//!
//! The application declares each secret in its node-admin's [`Wiring`] by name, with the node types that
//! read it: here `EXAMPLE_APP_TOKEN`, read by brokers. The bootstrap node-admin reads each declared
//! name once from its own environment, so the value is set where the fabric is started. Every node-admin
//! RDM launches is written the values beside the fabric's root, so whichever admin creates a broker can
//! set it; each admin keeps them beside its data dir and reads them back on a start, so a value never
//! changes while the fabric runs. The value is set only in the environment of the types its declaration
//! names and withheld from every other process; it is never written to a row, a Build record,
//! an event, the launch record or a span, and a spawn error is scrubbed of it. A name RDM sets or passes
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
use rdm_core::launch::{DeclaredSecret, ENV_NODE_TYPE, ENV_NODE_NAME, ENV_NODE_PATH};


const SERVICE: &str = "example-declared-secrets";
/// The application's one declared secret.
const TOKEN: &str = "EXAMPLE_APP_TOKEN";

/// The application's declarations, as every one of its node-admins registers them.
fn declared() -> Vec<DeclaredSecret> {
    vec![DeclaredSecret { name: TOKEN.to_string(), types: vec!["broker".into()] }]
}

#[tokio::main]
async fn main() -> Result<()> {
    // The type the launch names (`RDM_NODE_TYPE`, written by the launching admin), never read from the
    // node's `(path, name)`; the bootstrap admin is given none.
    let launched_as = std::env::var(ENV_NODE_TYPE).ok();
    let (path, name) = (std::env::var(ENV_NODE_PATH).unwrap_or_default(), std::env::var(ENV_NODE_NAME).unwrap_or_default());
    match launched_as.as_deref() {
        Some("broker") => {
            // The broker's own read of its secret. Its value is the application's: log only that it is held.
            let held = std::env::var(TOKEN).map_err(|_| anyhow!("path {path:?}, name {name:?} was launched without {TOKEN}"))?;
            eprintln!("path {path:?}, name {name:?}: {TOKEN} held ({} bytes)", held.len());
            rdm_node::run(rdm_node::Role { r#type: "broker".into() }).await
        }
        Some(r#type) if !rdm_core::is_node_admin(r#type) => Err(anyhow!("path {path:?}, name {name:?}: this example runs node-admins and brokers only")),
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
