//! Testkit wrapper around `rdm_node::node`, wiring `FaultedConnectionsStorage`,
//! `crate::originate::Seams`, `ProofStore` carrying, and `testkit-skew` rafka-time knobs.

use std::sync::Arc;

use anyhow::Result;
use rdm_transport::ServerBuilder;
use rdm_core::launch::Launch;

pub use rdm_node::node::{
    core_protocols, listen_for_stop,
    load_or_mint_key, stop_requested, wait_for_signal, NodeSeams, RafkaTimeKnobFn,
    RunningNode,
};

/// [`start_with_seams`] for an app with a hook before Ready: `before_ready` carries its
/// `before_ready` hook (and the gate its own server-side ops sit behind).
pub async fn start_before_ready(
    launch: &Launch,
    before_ready: rdm_node::before_ready::BeforeReadyHook,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_rafka_time_before_ready(
        launch,
        rdm_transport::clock::RafkaTime::unadopted(),
        before_ready,
        register,
    )
    .await
}

/// Bring the node up exactly as `launch` says.
pub async fn start(
    launch: &Launch,
    register: impl FnOnce(ServerBuilder, Arc<rdm_transport::LiveNodeResolver>) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_client(launch, |b, resolver, _client| register(b, resolver)).await
}

/// [`start`], handing `register` the process's one client too (a product family that calls out,
/// or carries for others, holds it before the server seals).
pub async fn start_with_client(
    launch: &Launch,
    register: impl FnOnce(
        ServerBuilder,
        Arc<rdm_transport::LiveNodeResolver>,
        Arc<rdm_transport::NodeRpcClient>,
    ) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_seams(launch, |b, s| register(b, s.resolver, s.client)).await
}

/// [`start_with_client`], handing `register` every process seam the testkit doors act through:
/// the resolver, the client, the connections writer and the storage fault.
pub async fn start_with_seams(
    launch: &Launch,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<RunningNode> {
    let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
    start_with_rafka_time_before_ready(
        launch,
        rafka_time,
        rdm_node::before_ready::BeforeReadyHook::none(),
        register,
    )
    .await
}

/// The storage backend a testkit node runs on: RDM standalone's file backend, or, in a build without
/// it (the memory estate's), one process's memory.
fn node_backend() -> Arc<dyn rdm_node::storage_backend::NodeStorageBackend> {
    #[cfg(feature = "file-backend")]
    return rdm_node::storage_backend::file_node_backend();
    #[cfg(not(feature = "file-backend"))]
    return Arc::new(rdm_node::storage_backend::MemoryNodeBackend);
}

async fn start_with_rafka_time_before_ready(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    before_ready: rdm_node::before_ready::BeforeReadyHook,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<RunningNode> {
    let fault = Arc::new(crate::faults::StorageFault::default());
    let fault_for_storage = fault.clone();
    let knob_fn: RafkaTimeKnobFn = Box::new(|launch, rt, boot| rafka_time_knob(launch, rt, boot));
    rdm_node::node::start_on(
        launch,
        node_backend(),
        rafka_time,
        before_ready,
        move |base| Arc::new(crate::faults::FaultedConnectionsStorage::new(base, fault_for_storage)),
        Some(knob_fn),
        move |b, ns| {
            let seams = crate::originate::Seams {
                resolver: ns.resolver,
                client: ns.client,
                connections: ns.connections,
                fault,
                name: ns.name,
            };
            register(b, seams).carry::<crate::proof_store::ProofStore>()
        },
        &[],
        &[],
    )
    .await
}

#[cfg(feature = "testkit-skew")]
fn rafka_time_knob(
    launch: &Launch,
    rafka_time: &rdm_transport::clock::RafkaTime,
    boot: &tracing::Span,
) -> Option<Box<dyn FnOnce() + Send>> {
    let root = launch.data_dir.parent()?.to_path_buf();
    let (name, rafka_time, boot) = (launch.name.to_string(), rafka_time.clone(), boot.clone());
    Some(Box::new(move || {
        crate::apply_rafka_time_knob(&root, &name, &rafka_time, &boot);
    }))
}

#[cfg(not(feature = "testkit-skew"))]
fn rafka_time_knob(
    _: &Launch,
    _: &rdm_transport::clock::RafkaTime,
    _: &tracing::Span,
) -> Option<Box<dyn FnOnce() + Send>> {
    None
}
