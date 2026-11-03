//! The testkit's application: it takes the fabric's notices the way an embedding does.
//!
//! `on_mesh_ready` and `on_fabric_ready` (Q175), `on_build_started` and `on_build_ended` (Q256), `on_node_investigated` (Q312)
//! and every node's `on_call_failed` (Q314, [`CallFailed`]) each only record a span. A scenario that holds a fabric
//! short of ready holds a node's `pre_node_ready` (`crate::write_pre_node_ready_knob`), as an
//! application holds its own node.
//!
//! It declares one secret (Q150), [`DECLARED_SECRET`], set on the broker and the gateway only: a
//! scenario sets its value in the bootstrap admin's environment and reads which processes hold it.
//!
//! It declares three listeners (Q151), [`LISTENERS`], on the gateway: a gateway binds them from its
//! launch, reports them at its join and its row publishes them.
//!
//! It registers a second topology storage provider (storage.md §2), [`TOPOLOGY_STORAGE_TARGET`], in
//! every executable of the testkit: RDM's file backend at a place of its own beside the fabric's store
//! dir ([`topology_storage_dir`]), a separate durable namespace standing in for an application's
//! backend; a third, [`TOPOLOGY_STORAGE_REFUSING`], the same backend at a place of its own whose
//! store lines are refused by name once a scenario writes its knob ([`refuse_list_knob`]: an
//! application's backend that answers its list with a named error), its claims answered; and a fourth,
//! [`TOPOLOGY_STORAGE_MEMORY`], RDM's memory backend, whose boundaries open and whose lines do not.

use rdm_admin::wiring::{BuildEndedCtx, BuildStartedCtx, FabricHooks, FabricReadyCtx, MeshReadyCtx, NodeInvestigatedCtx};
use std::sync::Arc;
use tracing::Instrument;

/// The testkit application's node types, declared on the Fabric at Day 0 (`RDM_NODE_TYPES`), each
/// extending `node`: a broker keeps its storage across a restart and a delete; a gateway's and a
/// compute's are ephemeral. No executable: every create names its own.
pub fn types() -> Vec<rdm_core::NodeType> {
    use rdm_core::meta::{PersistentDisposition, StoragePersistence};
    vec![
        rdm_core::NodeType { name: "broker".into(), storage_persistence: StoragePersistence::Persistent { disposition: PersistentDisposition::Preserve }, executable: None },
        rdm_core::NodeType { name: "gateway".into(), storage_persistence: StoragePersistence::Ephemeral, executable: None },
        rdm_core::NodeType { name: "compute".into(), storage_persistence: StoragePersistence::Ephemeral, executable: None },
    ]
}

/// The storage a node of `type` takes when its create states none: the declared type's ([`types`]),
/// else RDM's ([`StoragePersistence::of_type`](rdm_core::meta::StoragePersistence::of_type)).
pub fn storage_of(r#type: &str) -> rdm_core::meta::StoragePersistence {
    rdm_core::NodeType::of(&types(), r#type).map_or_else(|| rdm_core::meta::StoragePersistence::of_type(r#type), |t| t.storage_persistence.clone())
}

/// [`types`] as `RDM_NODE_TYPES` carries them.
pub fn types_document() -> String {
    serde_json::to_string(&types()).expect("node types encode as JSON")
}

/// The testkit application's declared secret: set in the process environment of [`DECLARED_TYPES`]
/// only.
pub const DECLARED_SECRET: &str = "TESTKIT_DECLARED_SECRET";

/// The types [`DECLARED_SECRET`] is set on.
pub const DECLARED_TYPES: [&str; 2] = ["broker", "gateway"];

/// The type the testkit application declares listeners on (Q151).
pub const LISTENER_TYPE: &str = "gateway";

/// The listeners the testkit application declares on [`LISTENER_TYPE`], as Rafka's gateway names its
/// own.
pub const LISTENERS: [&str; 3] = ["kafka", "rest", "proxy"];

/// The name the testkit registers its second topology storage provider under (storage.md §2: a name
/// is bound to its place at registration).
pub const TOPOLOGY_STORAGE_TARGET: &str = "test-target";

/// Where [`TOPOLOGY_STORAGE_TARGET`] keeps its stores for a fabric whose store dir
/// (`RDM_STORE_DIR`) is `store_dir`: a directory beside it, never inside it, so the file provider's
/// store dir holds only its own roots.
pub fn topology_storage_dir(store_dir: &std::path::Path) -> std::path::PathBuf {
    provider_dir(store_dir, TOPOLOGY_STORAGE_TARGET)
}

/// The name the testkit registers its third topology storage provider under: RDM's file backend at a
/// place of its own ([`provider_dir`]) that, while its knob stands ([`refuse_list_knob`]), answers a
/// read of its store lines with a named refusal
/// ([`StorageError::Refused`](rdm_node::storage_error::StorageError::Refused)), and its claims as the
/// file backend does.
pub const TOPOLOGY_STORAGE_REFUSING: &str = "test-refusing";

/// The name the testkit registers RDM's memory backend under: its boundaries open, and it answers no
/// lines to copy.
pub const TOPOLOGY_STORAGE_MEMORY: &str = "test-memory";

/// The knob of [`TOPOLOGY_STORAGE_REFUSING`] for a fabric whose store dir is `store_dir`: while this
/// file stands, every read of its store lines is refused by name.
pub fn refuse_list_knob(store_dir: &std::path::Path) -> std::path::PathBuf {
    provider_dir(store_dir, TOPOLOGY_STORAGE_REFUSING).join("refuse-list")
}

/// Where the testkit provider `provider` keeps its stores for a fabric whose store dir is `store_dir`:
/// a directory beside it, never inside it.
pub fn provider_dir(store_dir: &std::path::Path, provider: &str) -> std::path::PathBuf {
    let name = store_dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    store_dir.with_file_name(format!("{name}-{provider}"))
}

/// A backend whose copy lines refuse every store's list by name while `knob` stands; everything else
/// is `inner`'s.
#[cfg(feature = "file-backend")]
struct RefusesItsLines(Arc<dyn rdm_admin::storage_backend::StorageBackend>, std::path::PathBuf);

#[cfg(feature = "file-backend")]
#[async_trait::async_trait]
impl rdm_admin::storage_backend::StorageBackend for RefusesItsLines {
    fn qualify(&self, provider: rdm_admin::model::ProviderKind) -> Result<(), String> {
        self.0.qualify(provider)
    }
    async fn own_row(&self, data_dir: &std::path::Path, writer: &rdm_admin::model::IncarnationId) -> Result<Option<rdm_admin::storage::NodeRecord>, rdm_node::storage_error::StorageError> {
        self.0.own_row(data_dir, writer).await
    }
    async fn put_own_row(&self, data_dir: &std::path::Path, writer: &rdm_admin::model::IncarnationId, record: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_node::storage_error::StorageError> {
        self.0.put_own_row(data_dir, writer, record).await
    }
    async fn open(&self, scope: &rdm_admin::storage_backend::StorageScope) -> Result<rdm_admin::storage_backend::StorageBoundaries, String> {
        self.0.open(scope).await
    }
    async fn delete_fabric(&self, data_dir: &std::path::Path, fabric_id: &rdm_admin::model::FabricId) -> Result<(), String> {
        self.0.delete_fabric(data_dir, fabric_id).await
    }
    async fn lines(&self, provider: &str, fabric_id: &rdm_admin::model::FabricId) -> Result<Arc<dyn rdm_node::storage_copy::ProviderLines>, rdm_node::storage_error::StorageError> {
        Ok(Arc::new(ListRefused { inner: self.0.lines(provider, fabric_id).await?, provider: provider.into(), knob: self.1.clone() }))
    }
}

/// A provider's lines whose every store's list is refused by name while `knob` stands.
#[cfg(feature = "file-backend")]
struct ListRefused {
    inner: Arc<dyn rdm_node::storage_copy::ProviderLines>,
    provider: String,
    knob: std::path::PathBuf,
}

#[cfg(feature = "file-backend")]
#[async_trait::async_trait]
impl rdm_node::storage_copy::ProviderLines for ListRefused {
    async fn lines(&self, store: rdm_node::storage_copy::Store) -> Result<Vec<rdm_node::store_log::Line>, rdm_node::storage_error::StorageError> {
        if !self.knob.exists() {
            return self.inner.lines(store).await;
        }
        Err(rdm_node::storage_error::StorageError::Refused { file: format!("the {store} store of `{}`", self.provider), reason: "this provider lists no lines".into() })
    }
    async fn append_line(&self, store: rdm_node::storage_copy::Store, line: &rdm_node::store_log::Line) -> Result<(), rdm_node::storage_error::StorageError> {
        self.inner.append_line(store, line).await
    }
}

/// The topology storage providers the testkit's node-admin registers (storage.md §2): RDM's `file`
/// backend on the fabric's store dir, [`TOPOLOGY_STORAGE_TARGET`] on its own place beside it
/// ([`topology_storage_dir`]), [`TOPOLOGY_STORAGE_REFUSING`] on its own ([`provider_dir`]), each made
/// here when absent, and [`TOPOLOGY_STORAGE_MEMORY`].
#[cfg(feature = "file-backend")]
pub fn topology_storage_providers(cfg: &rdm_admin::admin::AdminConfig) -> rdm_admin::storage_backend::TopologyStorageProviders {
    let target = topology_storage_dir(&cfg.store_dir);
    let refusing = provider_dir(&cfg.store_dir, TOPOLOGY_STORAGE_REFUSING);
    let _ = std::fs::create_dir_all(&target);
    let _ = std::fs::create_dir_all(&refusing);
    rdm_admin::storage_backend::TopologyStorageProviders::file(cfg.store_dir.clone())
        .register(TOPOLOGY_STORAGE_TARGET, rdm_admin::storage_backend::file_backend(target))
        .register(TOPOLOGY_STORAGE_REFUSING, Arc::new(RefusesItsLines(rdm_admin::storage_backend::file_backend(refusing), refuse_list_knob(&cfg.store_dir))))
        .register(TOPOLOGY_STORAGE_MEMORY, Arc::new(rdm_admin::storage_backend::MemoryBackend))
}

/// The testkit application's listener declarations, as its wiring registers them.
pub fn listeners() -> Vec<rdm_core::launch::DeclaredListeners> {
    vec![rdm_core::launch::DeclaredListeners { r#type: LISTENER_TYPE.to_string(), names: LISTENERS.iter().map(|n| n.to_string()).collect() }]
}

/// The testkit application's declarations, as its wiring registers them.
pub fn declared() -> Vec<rdm_core::launch::DeclaredSecret> {
    vec![rdm_core::launch::DeclaredSecret { name: DECLARED_SECRET.into(), types: DECLARED_TYPES.iter().map(|t| t.to_string()).collect() }]
}

/// The hooks of this application: each notice records a span.
pub fn hooks() -> FabricHooks {
    let on_mesh_ready: rdm_admin::wiring::MeshReadyNotice = Arc::new(|ready: MeshReadyCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-mesh-ready", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the mesh is ready") }.instrument(span))
    });
    let on_fabric_ready: rdm_admin::wiring::FabricReadyNotice = Arc::new(|ready: FabricReadyCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-fabric-ready", fabric_id = %ready.fabric_id, build_id = %ready.build_id, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    let on_build_started: rdm_admin::wiring::BuildStartedNotice = Arc::new(|started: BuildStartedCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-build-started", fabric_id = %started.fabric_id, build_id = %started.build_id, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the Build started") }.instrument(span))
    });
    let on_build_ended: rdm_admin::wiring::BuildEndedNotice = Arc::new(|ended: BuildEndedCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-build-ended", fabric_id = %ended.fabric_id, build_id = %ended.build_id, state = ended.state.as_str(), "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the Build ended") }.instrument(span))
    });
    let on_node_investigated: rdm_admin::wiring::NodeInvestigatedNotice = Arc::new(|told: NodeInvestigatedCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-node-investigated", fabric_id = %told.fabric_id, mesh_id = %told.mesh_id, node_id = %told.node_id, incarnation_id = %told.incarnation.as_str(), node_path = %told.path, node_name = %told.name, verdict = told.verdict.as_str(), next = told.next.as_str(), "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told a node investigation's verdict") }.instrument(span))
    });
    FabricHooks { on_mesh_ready: Some(on_mesh_ready), on_fabric_ready: Some(on_fabric_ready), on_build_started: Some(on_build_started), on_build_ended: Some(on_build_ended), on_node_investigated: Some(on_node_investigated) }
}

/// The testkit application's `on_call_failed`, on every node it runs: it records a span.
pub struct CallFailed;

#[async_trait::async_trait]
impl rdm_node::node_hooks::OnCallFailed for CallFailed {
    async fn on_call_failed(&self, ctx: &rdm_node::node_hooks::CallFailedCtx) {
        let receiver = ctx.receiver.as_ref();
        tracing::info_span!(
            "rdm.testkit.app.update.via-on-call-failed",
            node_id = %ctx.sender.node_id,
            receiver_node_id = %receiver.map(|r| r.node_id.to_string()).unwrap_or_default(),
            receiver_incarnation_id = %receiver.map(|r| r.incarnation.as_str().to_string()).unwrap_or_default(),
            outcome = ctx.reason.name(),
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!("the application was told one of its calls' attempts are spent"));
    }
}

/// The testkit application's one gossip channel (gossip.md §8, Q362: RDM provides channels; an
/// application's nodes join the channels its own code names). Every testkit node and node-admin joins
/// it in its `pre_node_ready` when the estate's root holds [`APP_CHANNEL_KNOB`] ([`JoinsAppChannel`]).
pub const APP_CHANNEL: &str = "testkit.app";

/// The file, under the root that holds an estate's data dirs, that has the testkit's nodes and
/// node-admins join [`APP_CHANNEL`].
pub const APP_CHANNEL_KNOB: &str = "app-channel.knob";

/// Whether `root`, the directory holding an estate's data dirs, holds [`APP_CHANNEL_KNOB`].
pub fn app_channel_knob(root: &std::path::Path) -> bool {
    root.join(APP_CHANNEL_KNOB).exists()
}

/// This process's publisher on [`APP_CHANNEL`], once its [`JoinsAppChannel`] joined it.
static APP_CHANNEL_PUBLISHER: std::sync::OnceLock<rdm_transport::gossip::Publisher> = std::sync::OnceLock::new();

/// This process's publisher on [`APP_CHANNEL`], `None` before it joined the channel.
pub fn app_channel_publisher() -> Option<&'static rdm_transport::gossip::Publisher> {
    APP_CHANNEL_PUBLISHER.get()
}

/// The peers the testkit application names, by `endpoint_id`, for its node's join of [`APP_CHANNEL`]
/// in `pre_node_ready`: its mesh-primary from the context (`accepting_primary`), and, for a
/// node-admin, every other node-admin its topology view holds, of any mesh. A node that is not a
/// node-admin names its mesh-primary alone, so its events reach another mesh only through the
/// node-admins the application joined to the channel.
pub fn app_channel_peers(ctx: &rdm_node::node_hooks::NodeReadyCtx) -> Vec<rdm_core::EndpointId> {
    let mut peers: Vec<rdm_core::EndpointId> = ctx.accepting_primary.accepted().map(|a| rdm_core::EndpointId(a.endpoint.to_string())).into_iter().collect();
    if rdm_core::is_node_admin(&ctx.birth.r#type) {
        for d in ctx.topology_view.digest_view.all() {
            if d.node.node_id != ctx.birth.node_id && rdm_core::is_node_admin(&d.node.r#type) && !peers.contains(&d.node.endpoint_id) {
                peers.push(d.node.endpoint_id.clone());
            }
        }
    }
    peers
}

/// The testkit application's `pre_node_ready` over its node's own hook `inner`: it joins
/// [`APP_CHANNEL`] once, naming [`app_channel_peers`], keeps the publisher
/// ([`app_channel_publisher`]) and records each event it receives
/// (`rdm.testkit.channel.update.via-received`, under the event's receive span), then answers what
/// `inner` answers (Ok with none).
pub struct JoinsAppChannel {
    inner: Option<Arc<dyn rdm_node::node_hooks::PreNodeReady>>,
}

impl JoinsAppChannel {
    /// The hook, over the node's own hook `inner`.
    pub fn over(inner: Option<Arc<dyn rdm_node::node_hooks::PreNodeReady>>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait::async_trait]
impl rdm_node::node_hooks::PreNodeReady for JoinsAppChannel {
    fn send(&self) -> rdm_transport::Send {
        self.inner.as_ref().map(|h| h.send()).unwrap_or_default()
    }

    async fn pre_node_ready(&self, ctx: &rdm_node::node_hooks::NodeReadyCtx) -> rdm_node::node_hooks::HookOutcome {
        if APP_CHANNEL_PUBLISHER.get().is_none() {
            let peers = app_channel_peers(ctx);
            let named: Vec<String> = peers.iter().map(|p| p.0.clone()).collect();
            let span = tracing::info_span!("rdm.testkit.channel.update.via-join", node_id = %ctx.birth.node_id, node_path = %ctx.birth.path, node_name = %ctx.birth.name, channel = APP_CHANNEL, peers = %named.join(","), "otel.kind" = "internal");
            let joined = ctx.topology_view.join_channel(APP_CHANNEL, &peers).instrument(span.clone()).await;
            match joined {
                Ok(channel) => {
                    let (publisher, mut subscriber) = channel.split();
                    if APP_CHANNEL_PUBLISHER.set(publisher).is_ok() {
                        let node_id = ctx.birth.node_id.clone();
                        tokio::spawn(async move {
                            while let Some(received) = subscriber.recv().await {
                                received.span.in_scope(|| {
                                    tracing::info_span!("rdm.testkit.channel.update.via-received", node_id = %node_id, channel = APP_CHANNEL, subject = %received.subject, sequence = ?received.sequence, source_node_id = %received.source.node, source_incarnation_id = %received.source.incarnation, "otel.kind" = "internal")
                                        .in_scope(|| tracing::info!("the testkit application received an event on its channel"))
                                });
                            }
                        });
                    }
                    span.in_scope(|| tracing::info!("the testkit application joined its channel"));
                }
                Err(e) => return rdm_node::node_hooks::HookOutcome::Pending { reason: format!("{APP_CHANNEL}: {e}") },
            }
        }
        match &self.inner {
            Some(h) => h.pre_node_ready(ctx).await,
            None => rdm_node::node_hooks::HookOutcome::Ok,
        }
    }
}
