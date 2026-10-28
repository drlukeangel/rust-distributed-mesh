//! Named listeners (`RDM_LISTENERS`): an application's own ports on a node kind, declared once and
//! assigned by RDM (Q151).
//!
//! The application declares, in its node-admin's [`Wiring`], each listener of each of its kinds by
//! name: here the gateway's `client` listener. When a node-admin creates a gateway it assigns each
//! declared name the birth's address with port 0 (`RDM_LISTENERS=client=<addr>:0`): RDM never invents
//! a port. The gateway's node runtime binds each, keeps the binds open through its join and reports the
//! ports the operating system picked; the join refuses a report that does not carry exactly the
//! declared names, and the node's row publishes them (`NodeView::listeners`), which is where a client
//! finds the port. The application takes its bound listener from the running node and serves on it.
//!
//! This one executable is both halves, by the launch it is given: a launched gateway serves a one-line
//! greeting on its `client` listener; anything else runs as the node-admin that declares it. A Build
//! names it for the node-admin and gateway paths, so whichever admin creates a gateway holds the
//! declaration. The declaration is fixed for an admin's birth; a name RDM owns, a name repeated in a
//! kind, or a listener declared on the node-admin kind refuses the admin's start by name.
//!
//! Build: `cargo build --example named_listeners` in `demo/`.
use anyhow::{anyhow, Result};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::launch::DeclaredListeners;
use rdm_core::{NodeKind, PathName};
use tokio::io::AsyncWriteExt;

const SERVICE: &str = "example-named-listeners";
/// The gateway's one listener.
const CLIENT: &str = "client";

/// The application's listener declarations, as every one of its node-admins registers them.
fn listeners() -> Vec<DeclaredListeners> {
    vec![DeclaredListeners { kind: NodeKind::Gateway, names: vec![CLIENT.to_string()] }]
}

/// The gateway: RDM's node runtime brings it up and binds its listeners; the application serves on
/// `client`.
async fn gateway() -> Result<()> {
    rdm_node::node::listen_for_stop();
    let _telemetry = rdm_transport::init_evidence_telemetry(SERVICE);
    let launch = rdm_node::launch_for(rdm_node::Role { kind: NodeKind::Gateway })?;
    let running = rdm_node::node::start_pre_node_ready(&launch, rdm_node::node_hooks::NodeHooks::none(), |b, _| b).await?;
    let (_, bound) = running.listeners().iter().find(|(name, _)| name == CLIENT).ok_or_else(|| anyhow!("{} was launched without its {CLIENT} listener", launch.name))?;
    let std_listener = bound.try_clone()?;
    std_listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(std_listener)?;
    tracing::info!(target: "example", node = %launch.name, addr = %listener.local_addr()?, "serving on the {CLIENT} listener");
    let greeting = format!("hello from {}\n", launch.name);
    tokio::spawn(async move {
        while let Ok((mut conn, _)) = listener.accept().await {
            let greeting = greeting.clone();
            tokio::spawn(async move {
                let _ = conn.write_all(greeting.as_bytes()).await;
            });
        }
    });
    rdm_node::node::wait_for_signal(SERVICE).await;
    running.stop().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let launched_as = std::env::var("RDM_NODE_NAME").ok().and_then(|n| n.parse::<PathName>().ok());
    match launched_as {
        Some(name) if name.kind == NodeKind::Gateway => gateway().await,
        Some(name) if name.kind != NodeKind::NodeAdmin => Err(anyhow!("{name}: this example runs node-admins and gateways only")),
        _ => {
            rdm_admin::entry::run_with(SERVICE, |cfg| Wiring {
                listeners: listeners(),
                fabric_hooks: FabricHooks::no_app_work(),
                certs: rdm_admin::entry::material_or_exit(SERVICE, cfg),
                ..Wiring::default()
            })
            .await;
            Ok(())
        }
    }
}
