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

/// The name a testkit node registers one process's memory under: the memory estate's admin registers
/// its own as it, and launches its nodes on it (storage.md §2: a node writes the fabric's provider).
const MEMORY: &str = "memory";

/// The topology storage providers a testkit node registers (storage.md §2): RDM's `file` backend, the
/// testkit's [`crate::test_app::TOPOLOGY_STORAGE_TARGET`] at its place beside `launch`'s store dir (made
/// here when absent) and one process's memory as [`MEMORY`]; in a build without the file backend,
/// [`MEMORY`] alone.
fn node_backend(launch: &Launch) -> rdm_node::storage_backend::NodeStorageProviders {
    #[cfg(feature = "file-backend")]
    {
        let target = crate::test_app::topology_storage_dir(&launch.store_dir);
        let _ = std::fs::create_dir_all(&target);
        rdm_node::storage_backend::NodeStorageProviders::file()
            .register(crate::test_app::TOPOLOGY_STORAGE_TARGET, Arc::new(TargetRefusedByKnob(Arc::new(rdm_node::storage_backend::FileNodeBackend::at(target)))))
            .register(MEMORY, Arc::new(rdm_node::storage_backend::MemoryNodeBackend))
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = launch;
        rdm_node::storage_backend::NodeStorageProviders::new().register(MEMORY, Arc::new(rdm_node::storage_backend::MemoryNodeBackend))
    }
}

/// The file, under the root that holds an estate's data dirs, that has the birth `node_id` refuse to
/// open the testkit's [`crate::test_app::TOPOLOGY_STORAGE_TARGET`]: a storage change's move of that
/// birth onto it is refused by name (storage.md §2 step 4), and the birth writes the provider it wrote.
/// Keyed on the birth: a birth re-created at the same `path.name` opens it.
pub fn target_refused_knob(root: &std::path::Path, node_id: &str) -> std::path::PathBuf {
    root.join(format!("{node_id}.target-refused.knob"))
}

/// The testkit target's backend, refusing its open by name for a birth [`target_refused_knob`] names.
#[cfg(feature = "file-backend")]
struct TargetRefusedByKnob(Arc<dyn rdm_node::storage_backend::NodeStorageBackend>);

#[cfg(feature = "file-backend")]
#[async_trait::async_trait]
impl rdm_node::storage_backend::NodeStorageBackend for TargetRefusedByKnob {
    async fn open(&self, launch: &Launch) -> Result<rdm_node::storage_backend::NodeStorage, String> {
        if launch.data_dir.parent().is_some_and(|root| target_refused_knob(root, launch.node_id.as_str()).exists()) {
            return Err(format!("the testkit's knob has the birth {} refuse to open {}", launch.node_id, crate::test_app::TOPOLOGY_STORAGE_TARGET));
        }
        self.0.open(launch).await
    }
}

/// The testkit's app protocol: a registered ALPN (fabric-certs.md §On a connection) an estate's rpc
/// node registers when the root holding its data dir holds [`APP_PROTOCOL_KNOB`], every admitted
/// connection's streams answered by [`echo_streams`].
pub const APP_PROTOCOL: &[u8] = b"rdm-test/echo/1";

/// The file, under the root that holds an estate's data dirs, that has its rpc nodes register
/// [`APP_PROTOCOL`].
pub const APP_PROTOCOL_KNOB: &str = "app-protocol.knob";

/// The directory, under the root that holds an estate's data dirs, of the node-row knob: a file named
/// by an rpc node's `path.name` whose text is another node's `path.name` has that node read the other's
/// row from the nodes storage ([`watch_node_row_knob`]).
pub fn node_row_knob_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("node-row-knob")
}

/// The rpc node `launch` names reads another node's row when the estate's root holds its node-row
/// knob ([`node_row_knob_dir`]`/<path.name>`): looked at every 200 ms for the process's life, the knob
/// is taken (removed), the row it names is read once through `read` (`RunningNode::node_row`), and the
/// answer is written beside it as `<path.name>.answer`, JSON: `outcome` (`found`, `none` or `refused`)
/// with the row's `node_id` and `config`, or the refusal's `cause`.
pub fn watch_node_row_knob(launch: &Launch, read: rdm_node::node::NodeRowRead) {
    let Some(root) = launch.data_dir.parent() else { return };
    let (dir, name) = (node_row_knob_dir(root), launch.name.to_string());
    tokio::spawn(async move {
        let knob = dir.join(&name);
        loop {
            if let Ok(text) = tokio::fs::read_to_string(&knob).await {
                let _ = tokio::fs::remove_file(&knob).await;
                let answer = match text.trim().parse::<rdm_core::PathName>() {
                    Err(e) => serde_json::json!({"outcome": "refused", "cause": format!("the knob names no path.name ({:?}): {e}", text.trim())}),
                    Ok(target) => match read.read(target).await {
                        Ok(Some(row)) => serde_json::json!({"outcome": "found", "node_id": row.node_id.to_string(), "config": row.config}),
                        Ok(None) => serde_json::json!({"outcome": "none"}),
                        Err(e) => serde_json::json!({"outcome": "refused", "cause": e.to_string()}),
                    },
                };
                let staged = dir.join(format!("{name}.answer.staged"));
                if tokio::fs::write(&staged, answer.to_string()).await.is_ok() {
                    let _ = tokio::fs::rename(&staged, dir.join(format!("{name}.answer"))).await;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    });
}

/// The file, under the root that holds an estate's data dirs, that has its rpc nodes register
/// [`APP_PROTOCOL`] proxied through an rpc node carrier (connections.md §6, §7): a Direct open
/// that ends `NotSent` proves a Proxy through a carrier.
pub const APP_PROTOCOL_PROXY_KNOB: &str = "app-protocol-proxy.knob";

/// [`APP_PROTOCOL`] as the `Alpn` a node or a node-admin registers.
pub fn app_protocol() -> rdm_transport::connections::Alpn {
    rdm_transport::connections::Alpn { name: APP_PROTOCOL.to_vec(), proxy: false, carriers: vec![rdm_core::NodeKind::RpcNode] }
}

/// [`app_protocol`], proxied through an rpc node carrier.
pub fn app_protocol_proxy() -> rdm_transport::connections::Alpn {
    rdm_transport::connections::Alpn { proxy: true, ..app_protocol() }
}

/// Whether `launch`'s root holds [`APP_PROTOCOL_KNOB`] or [`APP_PROTOCOL_PROXY_KNOB`].
pub fn app_protocol_knob(launch: &Launch) -> bool {
    app_protocol_for(launch).is_some()
}

/// The app protocol `launch`'s root asks its rpc nodes to register: proxied under
/// [`APP_PROTOCOL_PROXY_KNOB`], not under [`APP_PROTOCOL_KNOB`], none without either.
pub fn app_protocol_for(launch: &Launch) -> Option<rdm_transport::connections::Alpn> {
    let root = launch.data_dir.parent()?;
    if root.join(APP_PROTOCOL_PROXY_KNOB).exists() {
        Some(app_protocol_proxy())
    } else if root.join(APP_PROTOCOL_KNOB).exists() {
        Some(app_protocol())
    } else {
        None
    }
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
    let registry = Arc::new(std::sync::OnceLock::new());
    let running = start_with_rafka_time_pre_node_ready_alpns(launch, rdm_transport::clock::RafkaTime::unadopted(), node_hooks, register, &[], &[alpn], dialer.clone(), registry.clone()).await?;
    let (d, acceptor) = running.connections(&name).map_err(|e| anyhow::anyhow!("taking the {:?} handle: {e:?}", String::from_utf8_lossy(&name)))?.split();
    let _ = dialer.set(d);
    let _ = registry.set((running.data_connections_for_testkit().clone(), name));
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
    start_with_seams_alpn_types_and_hooks(launch, alpn, &[], rdm_node::node_hooks::NodeHooks::none(), register).await
}

/// [`start_with_seams_and_alpn`], the node also declaring the app's channel `types` it holds
/// (gossip.md §8) and running `node_hooks` (its `pre_node_ready` before Ready, its `on_node_ready`
/// after).
pub async fn start_with_seams_alpn_types_and_hooks(
    launch: &Launch,
    alpn: rdm_transport::connections::Alpn,
    types: &[&str],
    node_hooks: rdm_node::node_hooks::NodeHooks,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<(RunningNode, rdm_transport::connections::Acceptor)> {
    let name = alpn.name.clone();
    let dialer = Arc::new(std::sync::OnceLock::new());
    let registry = Arc::new(std::sync::OnceLock::new());
    let running = start_with_rafka_time_pre_node_ready_alpns(launch, rdm_transport::clock::RafkaTime::unadopted(), node_hooks, register, types, &[alpn], dialer.clone(), registry.clone()).await?;
    let (d, acceptor) = running.connections(&name).map_err(|e| anyhow::anyhow!("taking the {:?} handle: {e:?}", String::from_utf8_lossy(&name)))?.split();
    let _ = dialer.set(d);
    let _ = registry.set((running.data_connections_for_testkit().clone(), name));
    Ok((running, acceptor))
}

async fn start_with_rafka_time_pre_node_ready(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: rdm_node::node_hooks::NodeHooks,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_rafka_time_pre_node_ready_alpns(launch, rafka_time, node_hooks, register, &[], &[], Arc::new(std::sync::OnceLock::new()), Arc::new(std::sync::OnceLock::new())).await
}

async fn start_with_rafka_time_pre_node_ready_alpns(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: rdm_node::node_hooks::NodeHooks,
    register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder,
    types: &[&str],
    alpns: &[rdm_transport::connections::Alpn],
    data_plane: Arc<std::sync::OnceLock<rdm_transport::connections::Dialer>>,
    data_plane_registry: Arc<std::sync::OnceLock<(rdm_transport::connections::ConnectionsRegistry, Vec<u8>)>>,
) -> Result<RunningNode> {
    let fault = Arc::new(crate::faults::StorageFault::default());
    let fault_for_storage = fault.clone();
    let knob_fn: RafkaTimeKnobFn = Box::new(|launch, rt, boot| rafka_time_knob(launch, rt, boot));
    let own_config: Arc<std::sync::OnceLock<rdm_node::node::OwnConfig>> = Arc::new(std::sync::OnceLock::new());
    let door = own_config.clone();
    let running = rdm_node::node::start_on(
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
                data_plane_registry,
                own_config: door,
            };
            register(b, seams).carry::<crate::proof_store::ProofStore>()
        },
        types,
        alpns,
    )
    .await?;
    let _ = own_config.set(running.own_config());
    Ok(running)
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
