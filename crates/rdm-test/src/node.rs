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

/// [`start_with_seams`] for an app with a hook before Ready: `pre_node_ready` carries its
/// `pre_node_ready` hook (and the gate its own server-side ops sit behind).
pub async fn start_pre_node_ready(
    launch: &Launch,
    node_hooks: rdm_node::node_hooks::NodeHooks,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_rafka_time_pre_node_ready(
        launch,
        rdm_transport::clock::RafkaTime::unadopted(),
        node_hooks,
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
/// or carries for others, holds it before the server is built).
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
    start_with_rafka_time_pre_node_ready(
        launch,
        rafka_time,
        rdm_node::node_hooks::NodeHooks::none(),
        register,
    )
    .await
}

/// The topology storage providers a testkit node registers (storage.md §2): RDM's `file` backend and
/// the testkit's [`crate::test_app::TOPOLOGY_STORAGE_TARGET`] at its place beside `launch`'s store dir
/// (made here when absent), or, in a build without the file backend (the memory estate's), one
/// process's memory as `memory`, the name the memory estate's admin registers its own under.
fn node_backend(launch: &Launch) -> rdm_node::storage_backend::NodeStorageProviders {
    #[cfg(feature = "file-backend")]
    {
        let target = crate::test_app::topology_storage_dir(&launch.store_dir);
        let _ = std::fs::create_dir_all(&target);
        rdm_node::storage_backend::NodeStorageProviders::file().register(crate::test_app::TOPOLOGY_STORAGE_TARGET, Arc::new(rdm_node::storage_backend::FileNodeBackend::at(target)))
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = launch;
        rdm_node::storage_backend::NodeStorageProviders::new().register("memory", Arc::new(rdm_node::storage_backend::MemoryNodeBackend))
    }
}

/// The testkit's app protocol: a registered ALPN (fabric-certs.md §On a connection) an estate's rpc
/// node registers when the root holding its data dir holds [`APP_PROTOCOL_KNOB`], every admitted
/// connection's streams answered by [`echo_streams`].
pub const APP_PROTOCOL: &[u8] = b"rdm-test/echo/1";

/// The file, under the root that holds an estate's data dirs, that has its rpc nodes register
/// [`APP_PROTOCOL`].
pub const APP_PROTOCOL_KNOB: &str = "app-protocol.knob";

/// [`APP_PROTOCOL`] as the `Alpn` a node or a node-admin registers.
pub fn app_protocol() -> rdm_transport::connections::Alpn {
    rdm_transport::connections::Alpn { name: APP_PROTOCOL.to_vec(), forwardable: false, carriers: vec![rdm_core::NodeKind::RpcNode] }
}

/// Whether `launch`'s root holds [`APP_PROTOCOL_KNOB`].
pub fn app_protocol_knob(launch: &Launch) -> bool {
    launch.data_dir.parent().is_some_and(|root| root.join(APP_PROTOCOL_KNOB).exists())
}

/// Answer every stream of every connection `acceptor` admits with `echo <own>: <the bytes it read>`.
pub fn echo_streams(mut acceptor: rdm_transport::connections::Acceptor, own: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(conn) = acceptor.accept().await {
            let own = own.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let Ok(bytes) = recv.read_to_end(1 << 20).await else { break };
                    let answer = format!("echo {own}: {}", String::from_utf8_lossy(&bytes));
                    if send.write_all(answer.as_bytes()).await.is_err() || send.finish().is_err() {
                        break;
                    }
                }
            });
        }
    })
}

/// [`start_pre_node_ready`], also registering one data-plane `alpn`: its dial half handed to the
/// originate door (`OriginateRequest::Stream`), its accept half returned.
pub async fn start_pre_node_ready_with_alpn(
    launch: &Launch,
    node_hooks: rdm_node::node_hooks::NodeHooks,
    alpn: rdm_transport::connections::Alpn,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<(RunningNode, rdm_transport::connections::Acceptor)> {
    let name = alpn.name.clone();
    let dialer = Arc::new(std::sync::OnceLock::new());
    let running = start_with_rafka_time_pre_node_ready_alpns(launch, rdm_transport::clock::RafkaTime::unadopted(), node_hooks, register, &[alpn], dialer.clone()).await?;
    let (d, acceptor) = running.connections(&name).map_err(|e| anyhow::anyhow!("taking the {:?} handle: {e:?}", String::from_utf8_lossy(&name)))?.split();
    let _ = dialer.set(d);
    Ok((running, acceptor))
}

/// [`start_with_seams`], also registering one data-plane `alpn` (connections.md §5): its dial half is
/// handed to the originate door (`OriginateRequest::Stream`), its accept half returned for the role to
/// answer on.
pub async fn start_with_seams_and_alpn(
    launch: &Launch,
    alpn: rdm_transport::connections::Alpn,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<(RunningNode, rdm_transport::connections::Acceptor)> {
    let name = alpn.name.clone();
    let dialer = Arc::new(std::sync::OnceLock::new());
    let running = start_with_rafka_time_pre_node_ready_alpns(launch, rdm_transport::clock::RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), register, &[alpn], dialer.clone()).await?;
    let (d, acceptor) = running.connections(&name).map_err(|e| anyhow::anyhow!("taking the {:?} handle: {e:?}", String::from_utf8_lossy(&name)))?.split();
    let _ = dialer.set(d);
    Ok((running, acceptor))
}

async fn start_with_rafka_time_pre_node_ready(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: rdm_node::node_hooks::NodeHooks,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_rafka_time_pre_node_ready_alpns(launch, rafka_time, node_hooks, register, &[], Arc::new(std::sync::OnceLock::new())).await
}

async fn start_with_rafka_time_pre_node_ready_alpns(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: rdm_node::node_hooks::NodeHooks,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
    alpns: &[rdm_transport::connections::Alpn],
    data_plane: Arc<std::sync::OnceLock<rdm_transport::connections::Dialer>>,
) -> Result<RunningNode> {
    let fault = Arc::new(crate::faults::StorageFault::default());
    let fault_for_storage = fault.clone();
    let knob_fn: RafkaTimeKnobFn = Box::new(|launch, rt, boot| rafka_time_knob(launch, rt, boot));
    rdm_node::node::start_on(
        launch,
        node_backend(launch),
        rafka_time,
        node_hooks,
        move |base| Arc::new(crate::faults::FaultedConnectionsStorage::new(base, fault_for_storage)),
        Some(knob_fn),
        move |b, ns| {
            let seams = crate::originate::Seams {
                resolver: ns.resolver,
                client: ns.client,
                connections: ns.connections,
                fault,
                name: ns.name,
                data_plane,
            };
            register(b, seams).carry::<crate::proof_store::ProofStore>()
        },
        &[],
        alpns,
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
