//! Running node-admin handle, RPC server wiring, and boot sequence.

use super::*;

/// A running node-admin (`docs/architecture/node-admin-lifecycle.md` §2): what `run` needs to keep
/// alive and shut down.
pub struct Running {
    /// Rafka-time: the app's way to read it. The one reader every gossip stamp, lifecycle event and
    /// fleet judgement of this admin reads, already adopted when the admin has started. Cloneable;
    /// every clone reads the same instance. `now_ms()` is `max(reference + monotonic elapsed, last
    /// returned)`; an app that stamps its records with rafka-time (the OrderKey's
    /// `writer_timestamp_ms`) reads it here and never from the OS clock.
    pub rafka_time: rdm_transport::clock::RafkaTime,
    /// The base of this admin's control API.
    pub api_base: String,
    /// The control plane the API serves.
    pub control: Arc<ControlPlane>,
    /// The runner that realises Build operations.
    pub runner: Arc<AdminRunner>,
    /// The admin's topology view.
    pub topology_view: TopologyView,
    /// The admin's own digest.
    pub digest: Arc<Mutex<MeshDigest>>,
    /// This process's one Node RPC client and live resolver: every Node RPC
    /// caller in the process takes it by clone.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// This admin's place on the backbone: while it leaves, its mesh topology publication carries its
    /// own `Leaving` out of its mesh ([`Running::leave`]).
    pub(crate) backbone: Backbone,
    pub(crate) router: IrohRouter,
    /// This admin's Node RPC server: the leave waits for the replies it owes before it closes the transport.
    pub(crate) rpc_server: rdm_transport::NodeRpcServer,
    /// The digest cadence, the Build executor, the hierarchy and the anti-entropy:
    /// stopped first on leave.
    pub(crate) publisher: tokio::task::JoinHandle<()>,
    pub(crate) executor: tokio::task::JoinHandle<()>,
    /// The attempt runs this admin executes, each a task of its own: stopped with the reconciling.
    pub(crate) runs: Arc<crate::build_run::AttemptRuns>,
    pub(crate) hierarchy: tokio::task::JoinHandle<()>,
    /// The mesh topology anti-entropy: this admin's mesh topology on the backbone, on its own timer.
    pub(crate) anti_entropy: tokio::task::JoinHandle<()>,
    pub(crate) tasks: Vec<tokio::task::JoinHandle<()>>,
    /// The tasks node.start's steps after the hook started (the own-mesh sweep of a launched admin).
    pub(crate) start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// The gate a stop stops (`node_self`'s `Session::cut`) and a start opens.
    pub(crate) gate: crate::stop_gate::StopGate,
    /// This admin as the subject of the connections delete, stop and start its mesh-admin commands.
    pub(crate) node_self: Arc<crate::node_self::NodeSelf>,
    /// Asked before each leave announcement publish (a testkit executable's wiring; none in the product).
    pub(crate) leave_seam: Option<Arc<dyn crate::wiring::LeaveSeam>>,
    /// This admin's Build log: targeted before the first `Leaving` (ruling R-A2).
    pub(crate) builds: Arc<FabricBuildStateAdapter>,
    /// Cancelled when this admin is deleted (stop-node, leaving): its hook and any pull
    /// it has in flight end.
    pub(crate) pre_node_ready_cancel: rdm_transport::BirthCancel,
    pub(crate) pre_node_ready: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>,
    /// The member cert the launching admin's signer issued for this birth, as the opaque bytes the
    /// join that admitted it carried (empty when that admin is configured with no certs); for a birth
    /// that made no JoinNode and is its own maker (the Day-0 root, an admin re-created at its path over
    /// its inherited data dir, an adoption launch), issued by its own issuer. `None` for a recovery
    /// start on neither.
    pub member_cert: Option<Vec<u8>>,
    /// This admin's Status door, once it holds a view.
    pub(crate) status_door: crate::status_rpc::StatusDoorSlot,
    /// Told once [`Self::status_door`] is set: what [`Self::config_update`] awaits before this admin holds a view.
    pub(crate) status_door_set: Arc<tokio::sync::Notify>,
    /// The app's protocols on this admin's endpoint ([`crate::wiring::Wiring::alpns`]).
    pub(crate) data_connections: rdm_transport::connections::ConnectionsRegistry,
}

impl Running {
    /// Take the one [`rdm_transport::connections::Connections`] handle for the app protocol `alpn`
    /// registered at bind ([`crate::wiring::Wiring::alpns`]), as a node's app takes its own
    /// (`RunningNode::connections`): `UnregisteredAlpn` for any other, `AlreadyTaken` on a second call.
    pub fn connections(&self, alpn: &[u8]) -> std::result::Result<rdm_transport::connections::Connections, rdm_transport::connections::ConnectError> {
        self.data_connections.connections(alpn)
    }

    /// The birth this admin runs under: its digest's node, the birth its Node RPC door opens for.
    pub fn birth(&self) -> MeshNode {
        self.digest.lock().unwrap().node.clone()
    }

    /// Print what the process that started this admin reads on its stdout: the birth it runs under
    /// (`rdm_client::BIRTH_LINE`), by which that process reaches it over Node RPC holding
    /// nothing else, then its control base.
    pub fn announce(&self) {
        println!("{}{}", rdm_client::BIRTH_LINE, serde_json::to_string(&self.birth()).expect("a birth serializes"));
        println!("RDM_NODE_ADMIN_API_BASE={}", self.api_base);
    }

    /// The gate a stop stops and a start opens: every task this admin runs is held at its own wait
    /// while it is stopped.
    pub fn gate(&self) -> &crate::stop_gate::StopGate {
        &self.gate
    }

    /// Whether this admin publishes its mesh's topology on the backbone: the role its hierarchy
    /// last decided (its mesh's primary, its view deciding).
    pub fn publishes_its_mesh(&self) -> bool {
        self.backbone.is_mesh_primary()
    }

    /// This admin as the subject of `node.connection.delete`, `stop-node` and `start-node`: what such a command
    /// naming it reaches once its Status handler has decided it (`status_rpc`'s `serve_command`).
    pub fn node_self(&self) -> Arc<crate::node_self::NodeSelf> {
        self.node_self.clone()
    }

    /// This admin's Status door, once it holds a view: what decides the declarations of its
    /// mesh's births while it is its mesh's primary.
    pub fn status_door(&self) -> Option<Arc<crate::status_rpc::StatusDoor>> {
        self.status_door.get().cloned()
    }

    /// Put this admin's own config, the whole map (`node.config.update` of its own birth,
    /// node-config-update.md): what an application's node-admin calls to put a key on its own row (its
    /// `admin_api_base`). It answers once the command completed: `ConfigUpdated` once its mesh-primary
    /// wrote the row and this admin holds the map, or the refusal by name. The caller never names the
    /// fabric-primary: an admin that is not it carries the command up to the one its records name
    /// ([`crate::status_rpc::StatusDoor::own_config_update`]). Called before this admin holds a
    /// view, or before its view holds its own birth, it waits for that view rather than refusing: the
    /// update is owed until answered (node-config-update.md "A node's own update", D12, D23). The view
    /// waited on is the one the update is served from, so it is never refused `SubjectUnknown` for this
    /// admin's own birth; the wait ends with this birth.
    pub async fn config_update(&self, config: std::collections::BTreeMap<String, String>) -> rdm_core::status::StatusReply {
        let birth = self.birth();
        let status = loop {
            let told = self.status_door_set.notified();
            if let Some(status) = self.status_door() {
                break status;
            }
            told.await;
        };
        let (mut views, ended) = (self.runner.records.subscribe_view(), self.pre_node_ready_cancel.token());
        while status.topology.read().await.own(&birth.node_id).is_none() {
            tokio::select! {
                changed = views.changed() => if changed.is_err() { break },
                () = ended.cancelled() => return rdm_core::reply::status::not_ready(format!("{}: this birth ended before its view held it; its own config is not put", birth.name)),
            }
        }
        status.own_config_update(birth.incarnation, config).await
    }

    /// Where this admin's `pre_node_ready` stands; `None` when the app registered no hook.
    pub fn pre_node_ready(&self) -> Option<crate::node_hooks::PreNodeReadyHandle> {
        self.pre_node_ready.lock().unwrap().clone()
    }

    /// Resolves with the reason once this admin's hook failed; never resolves otherwise. The
    /// process selects on it beside its stop signal and ends by name.
    pub async fn pre_node_ready_failed(&self) -> String {
        let handle = self.pre_node_ready.lock().unwrap().clone();
        match handle {
            Some(h) => h.failed().await,
            None => std::future::pending().await,
        }
    }

    /// Stop executing and reconciling: no Build attempt and no drift
    /// recovery starts after this. A fabric shutdown does this first, so the
    /// runtimes it stops are not recovered as proven drift. The reconcile loop ends, and so does
    /// every Build's drive it started, whose own passes would otherwise plan and begin the Build's
    /// next attempt, and every attempt run this admin executes, a run a drive dispatched before the
    /// stop included, whose executor would otherwise begin its operations after it. It returns once
    /// each of them has ended.
    pub async fn stop_reconciling(&self) {
        self.executor.abort();
        self.runner.drives.stop().await;
        self.runs.stop().await;
    }

    /// Stop on this process's signal (i146 Q55; node-stop.md steps 2-4), this admin its own subject: the
    /// one stop sequence an rpc node's signal runs ([`crate::node_self::NodeSelf::stop_on_signal`]). The
    /// Build executor, every Build's drive and every attempt run this admin executes stop first
    /// ([`Self::stop_reconciling`]; an attempt it was running is continued by the Build's next attempt).
    /// Then the whole `node.connection.delete { All }` under the birth's bound, its connections-deleting
    /// owed to its mesh-primary; its tasks held; its last mesh topology before the cut carrying itself
    /// Leaving when it is its mesh's primary; `node.mesh.leave`, its own mesh and the backbone left and
    /// its Node RPC pool cut but for the connection to its mesh-primary; its `Stopped` owed to its
    /// mesh-primary (node-rpc.md §43); then the wait until each owed declaration is answered or its
    /// receiver proven gone (Q206). Then the process end (`Self::leave_with`): every task aborted,
    /// each call this node answered settled with its caller, the transport closed.
    pub async fn leave(self) {
        self.pre_node_ready_cancel.cancel();
        self.stop_reconciling().await;
        let _ = self.node_self.stop_on_signal().await;
        self.leave_with(false).await;
    }

    /// End a stopped birth (a `stop-node` completed): it says nothing and closes. Its stop already
    /// cut its gossip neighbours and left its topics, and a stopped node gossips nothing
    /// (node-stop.md step 4): every node holds it Leaving and stopped from the `NodeStopped` its
    /// stopped reply produced.
    pub async fn leave_after_stop(self) {
        self.pre_node_ready_cancel.cancel();
        self.stop_reconciling().await;
        self.leave_with(true).await
    }

    pub(crate) async fn leave_with(self, _stopped: bool) {
        let _ = &self.leave_seam;
        self.executor.abort();
        self.hierarchy.abort();
        self.anti_entropy.abort();
        self.publisher.abort();
        let me = self.digest.lock().unwrap().node.name.to_string();
        let started = std::time::Instant::now();
        let of_start = self.start_tasks.lock().unwrap().drain(..).collect::<Vec<_>>();
        let aborted = self.tasks.len() + of_start.len();
        for t in self.tasks.into_iter().chain(of_start) {
            t.abort();
        }
        tracing::info_span!("rdm.mesh.node.update.via-leave-tasks", node = %me, aborted, elapsed_ms = started.elapsed().as_millis() as u64, outcome = "aborted").in_scope(|| tracing::info!("the node's tasks are aborted"));
        let started = std::time::Instant::now();
        let span = tracing::info_span!("rdm.mesh.node.update.via-leave-shutdown", node = %me, elapsed_ms = tracing::field::Empty, settled = tracing::field::Empty, unsettled = tracing::field::Empty, outcome = tracing::field::Empty);
        // Every call this node answered is settled with its caller before the transport closes:
        // a close takes an unacknowledged reply down with the connection. The wait is the birth's bound
        // (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`: how long in-flight handlers may finish before the node
        // leaves; node-rpc.md §35): a handler still running at it, its call never answered, holds the
        // leave no longer, and the span names each such call.
        let bound = self.node_self.connections_delete_deadline;
        if tokio::time::timeout(bound, self.rpc_server.settled()).instrument(span.clone()).await.is_ok() {
            span.record("settled", "settled");
        } else {
            span.record("settled", "deadline");
            span.record("unsettled", self.rpc_server.unsettled().join(",").as_str());
        }
        let r = self.router.shutdown().instrument(span.clone()).await;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", if r.is_ok() { "closed" } else { "refused" });
        span.in_scope(|| tracing::info!("the transport closed"));
    }
}

/// What the provider runs a node in, for explicit executable bindings.
pub(crate) fn provider_image(p: ProviderKind) -> rdm_core::binding::ProviderImage<'static> {
    match p {
        ProviderKind::Process => rdm_core::binding::ProviderImage::Process,
        ProviderKind::Container => rdm_core::binding::ProviderImage::Container(crate::deployment::container::RUNTIME_IMAGE),
    }
}

/// The Node RPC server a node-admin serves, before it is built: core (Ping, Forward), then the families the
/// admin owns as a primary (status, build claim, join).
pub fn rpc_server(
    connections: Arc<crate::connections_writer::ConnectionsWriter>,
    client: Arc<rdm_transport::NodeRpcClient>,
    status_door: crate::status_rpc::StatusDoorSlot,
    claim: crate::build_claim::ClaimSlot,
    join: crate::join::JoinSlot,
) -> rdm_transport::ServerBuilder {
    let core = rdm_transport::ServerBuilder::new().serve_core(client, Some(connections));
    // Status is forwardable: a peer may carry an authority's status call through this admin.
    crate::join::serve(crate::build_claim::serve(crate::status_rpc::serve(core, status_door), claim), join).carry::<rdm_core::status::Status>()
}

/// The unreadable pre-event steps already named: each is named once, not on every fact.
pub(crate) type NamedSteps = HashSet<(String, u32, String, String)>;

/// Apply the lifecycle overlays `facts` name open to `digest_view`: every open operation as a hold
/// that its attempt's terminal verdict or its Build's forgetting closes
/// (`DigestView::deletions_from_facts`), and every pre-event step this build cannot read refused by
/// name, once. Run inside the
/// log's append as each fact enters it ([`crate::build_state::LocalBuildLog::derive_on_every_fact`]).
pub(crate) fn overlays_from_facts(facts: &[crate::build_state::BuildFact], digest_view: &DigestView, named: &mut NamedSteps) {
    let builds = crate::build_state::fold(facts);
    if let Some(b) = facts
        .iter()
        .rev()
        .filter_map(|f| match f {
            crate::build_state::BuildFact::Accepted(a) => builds.get(&a.build_id),
            _ => None,
        })
        .find(|p| p.lane_open() || !matches!(p.state, crate::build_state::BuildState::Failed) || p.completed_at.is_some())
    {
        digest_view.set_accepted_meshes(b.topology.mesh_names());
    }
    let in_flight = crate::build_state::in_flight_ops(facts, &builds);
    for op in in_flight.ops {
        digest_view.deleting(op);
    }
    digest_view.deletions_from_facts(&in_flight.removals_ended, &crate::build_state::forgotten_builds(facts, &builds));
    for u in in_flight.unrecognised {
        if named.insert((u.build_id.to_string(), u.attempt, u.operation.clone(), u.step.clone())) {
            tracing::info_span!("rdm.node_admin.build.reject.via-unrecognised-step", build_id = %u.build_id.log(), attempt = u.attempt, operation = %u.operation, step = %u.step, reason = %u.reason)
                .in_scope(|| tracing::warn!("a lifecycle pre-event step this build cannot read opens no overlay"));
        }
    }
}

/// Bring a node-admin up (`docs/architecture/node-admin-lifecycle.md` §2): identity, policy, the
/// topology view, Build state, the control API, the projection and the executor.
#[cfg(feature = "file-backend")]
pub async fn start(cfg: AdminConfig) -> Result<Running, String> {
    // `start` carries no hook and no wrapper, so it is an embedding with no application work.
    start_with(cfg, crate::wiring::Wiring { fabric_hooks: crate::wiring::FabricHooks::no_app_work(), ..crate::wiring::Wiring::no_certs() }).await
}

/// The app's protocols ([`crate::wiring::Wiring::alpns`]) on this admin's endpoint, registered like
/// every node's: a `ConnectionsRegistry` over the admin's connections log and live resolver, and each
/// protocol's handler routed, so RDM owns its accept loop and `node.connection.create` runs before
/// the app sees a connection.
fn route_app_alpns(
    router: iroh::protocol::RouterBuilder,
    endpoint: &iroh::Endpoint,
    resolver: Arc<rdm_transport::LiveNodeResolver>,
    connections: Arc<crate::connections_writer::ConnectionsWriter>,
    alpns: &[rdm_transport::connections::Alpn],
) -> Result<(iroh::protocol::RouterBuilder, rdm_transport::connections::ConnectionsRegistry), String> {
    let (registry, handlers) = rdm_transport::connections::ConnectionsRegistry::new(endpoint.clone(), resolver, connections, alpns, Vec::new())
        .map_err(|e| format!("an app protocol: {e}"))?;
    let mut router = router;
    for (alpn, handler) in handlers {
        router = router.accept(alpn, handler);
    }
    Ok((router, registry))
}

/// [`start`], with the decorators and hooks of `wiring` applied to the parts the admin is built
/// from (`docs/architecture/node-admin-lifecycle.md` §2), on RDM standalone's storage backend
/// ([`crate::storage_backend::file_backend`]).
#[cfg(feature = "file-backend")]
pub async fn start_with(cfg: AdminConfig, wiring: crate::wiring::Wiring) -> Result<Running, String> {
    let providers = crate::storage_backend::TopologyStorageProviders::file(cfg.store_dir.clone());
    start_on(cfg, wiring, providers).await
}

/// [`start_with`] on the topology storage `providers` its executable registers
/// (`docs/architecture/storage.md` §2): every durable read and write of the admin goes through the
/// boundaries the one its `topology_storage_provider` names opens.
pub async fn start_on(mut cfg: AdminConfig, wiring: crate::wiring::Wiring, providers: crate::storage_backend::TopologyStorageProviders) -> Result<Running, String> {
    // One boot trace per birth: this root is never entered (iroh's tasks must not inherit it), each
    // step is a child that lives exactly as long as the step, and `via-ready` closes it.
    let boot = tracing::info_span!(parent: None, "rdm.mesh.node.create.via-boot", node = tracing::field::Empty, node_id = %cfg.node_id, incarnation_id = tracing::field::Empty, kind = "node-admin");
    if let Err(e) = own_row_provider(&mut cfg, &providers).await {
        boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
        return Err(e);
    }
    let started = match lone_birth_storage_deleted(&cfg, &providers, &boot).await {
        Ok(deleted) => {
            if deleted {
                // Day 0 (Q200): a recovery's flags, seeds and mesh id name a fabric that is no more.
                (cfg.mesh_primary, cfg.fabric_primary, cfg.mesh_id) = (false, false, None);
                cfg.seeds.clear();
            }
            // Boxed: the boot is one large state machine, and its caller holds only the box.
            Box::pin(start_booted(cfg, wiring, providers, boot.clone())).await
        }
        Err(e) => Err(e),
    };
    if let Err(e) = &started {
        boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
    }
    started
}

/// The provider a start with no launcher opens (storage.md §2 "The property": "a restart with no launcher
/// (the adoption launch) | the node itself | its own row in its data dir"): the one its own row in its
/// data dir names, read before anything opens, whatever its environment names. A data dir holding no
/// own row, or a row written before the row named its provider, leaves the environment's value
/// (`RDM_TOPOLOGY_STORAGE_PROVIDER`, `file` when unset): Day 0's. A launched node's is its launch's.
async fn own_row_provider(cfg: &mut AdminConfig, providers: &crate::storage_backend::TopologyStorageProviders) -> Result<(), String> {
    if cfg.launch.is_some() {
        return Ok(());
    }
    let storage = providers.named(&cfg.topology_storage_provider).map_err(|e| format!("refusing to start: {e}"))?;
    let own = storage.own_row(&cfg.data_dir, &IncarnationId::mint()).await.map_err(|e| format!("refusing to start: {}: its own row could not be read: {e}", cfg.data_dir.display()))?;
    if let Some(provider) = own.and_then(|own| own.topology_storage_provider).filter(|p| *p != cfg.topology_storage_provider) {
        tracing::info!(data_dir = %cfg.data_dir.display(), environment = %cfg.topology_storage_provider, own_row = %provider, "a start with no launcher opens the topology storage provider its own row names");
        cfg.topology_storage_provider = provider;
    }
    Ok(())
}

/// Q200 (fabric-recovery.md "Nothing alive": "A store that names no node but the launching admin's own
/// lost birth holds no fabric to recover: the launch deletes that storage and starts as Day 0, a new
/// fabric-primary."): a launch on its own data dir (no launcher, no adoption) whose own row names a
/// Fabric whose store names no node but that row's birth deletes that Fabric's storage before anything of
/// its boot reads it, and its boot then finds no own row: Day 0. A store naming any other node is a
/// recovery, left as it is; a fabric id handed in that differs from the own row's is refused by the boot.
/// Whether the storage was deleted.
async fn lone_birth_storage_deleted(cfg: &AdminConfig, providers: &crate::storage_backend::TopologyStorageProviders, boot: &tracing::Span) -> Result<bool, String> {
    if cfg.launch.is_some() || cfg.adopt.is_some() {
        return Ok(false);
    }
    let storage = providers.named(&cfg.topology_storage_provider).map_err(|e| format!("refusing to start: {e}"))?;
    let reader = IncarnationId::mint();
    let Some(own) = storage.own_row(&cfg.data_dir, &reader).await.map_err(|e| e.to_string())? else { return Ok(false) };
    if cfg.fabric_id_named && cfg.fabric_id != own.fabric_id {
        return Ok(false);
    }
    let scope = crate::storage_backend::StorageScope { data_dir: cfg.data_dir.clone(), fabric_id: own.fabric_id.clone(), own_mesh: own.name.mesh.clone(), writer: reader, node_id: cfg.node_id.clone() };
    let unreadable = |e: &dyn std::fmt::Display| format!("refusing to start: {}: the store of Fabric {} could not be read: {e}", cfg.data_dir.display(), own.fabric_id);
    let handle = crate::storage_backend::TopologyStorageHandle::open(providers, &cfg.topology_storage_provider, scope).await?;
    let others = crate::storage::live_records(&*handle.meshes(), &*handle.nodes(), &own.name.mesh).await.map_err(|e| unreadable(&e))?.into_iter().filter(|r| r.node_id != own.node_id).count();
    if others > 0 {
        return Ok(false);
    }
    let step = tracing::info_span!(parent: boot, "rdm.node_admin.fabric.delete.via-lone-birth", node = %own.name, inherited_node_id = %own.node_id, fabric_id = %own.fabric_id, data_dir = %cfg.data_dir.display(), name_released = tracing::field::Empty, outcome = tracing::field::Empty);
    // The deleted fabric releases its name (R-FN1: "a deleted fabric releases it"), then its storage goes.
    let deleted = async {
        let fabric = handle.fabric();
        if let Some(record) = fabric.fabric().await.map_err(|e| unreadable(&e))? {
            let identity = crate::fabric_storage::FabricIdentity { fabric_id: record.fabric_id.clone(), name: record.name.clone() };
            let released = crate::fabric_storage::release_name(&*fabric, &identity).await.map_err(|e| format!("the name {} of Fabric {} could not be released: {e}", record.name, record.fabric_id))?;
            tracing::Span::current().record("name_released", released);
        }
        drop(fabric);
        drop(handle);
        storage.delete_fabric(&cfg.data_dir, &own.fabric_id).await
    }
    .instrument(step.clone())
    .await;
    step.record("outcome", if deleted.is_ok() { "deleted" } else { "refused" });
    step.in_scope(|| tracing::info!("the store names no node but this launch's own lost birth: no fabric to recover; its storage is deleted and the launch starts as Day 0"));
    deleted.map(|()| true).map_err(|e| format!("refusing to start: {}: the store of Fabric {} names no node but the lost birth {} at {}, and its storage could not be deleted: {e}", cfg.data_dir.display(), own.fabric_id, own.node_id, own.name))
}

pub(crate) async fn start_booted(mut cfg: AdminConfig, mut wiring: crate::wiring::Wiring, providers: crate::storage_backend::TopologyStorageProviders, boot: tracing::Span) -> Result<Running, String> {
    // The topology storage provider this admin writes (storage.md §2): one its executable registers, or
    // the start is refused by name before anything opens.
    let storage = providers.named(&cfg.topology_storage_provider).map_err(|e| format!("refusing to start: {e}"))?;
    // Every task this admin runs is spawned behind this gate: a stop holds it, and a start opens it again.
    let stop_gate = crate::stop_gate::StopGate::new();
    // The app's declared secrets (Q150): a name RDM sets or passes through, or a name declared twice,
    // refuses the start here, by name and naming the fabric's provider (R-D3.3), before anything is
    // bootstrapped. Each value is read once: from the values this admin's data dir keeps, else from the
    // launch's custody document when a pipeline launched this admin, else from its own environment, as
    // the fabric's root is; they are kept beside the data dir below.
    let provider_named = match (crate::deployment::provider::parse(cfg.spawn_type.as_deref()), &cfg.launch) {
        (Ok(Some(ProviderKind::Container)), _) => "container",
        (Ok(Some(ProviderKind::Process)), _) | (Ok(None), None) => "process",
        _ => "the one its entry pull names",
    };
    let declared_secrets = crate::deployment::declared::DeclaredSecrets::read(std::mem::take(&mut wiring.declared), cfg.launch.as_ref(), |k| std::env::var(k).ok(), &cfg.data_dir, provider_named)?;
    // The app's listeners (Q151): a duplicate name in a kind or a name RDM owns refuses the start
    // here, by name, before anything is bootstrapped.
    let listener_names = crate::deployment::endpoint::ListenerNames::read(&wiring.listeners)?;
    let fabric_hooks = wiring.fabric_hooks.clone();
    let build_notices = crate::build_drive::BuildNotices::of(cfg.fabric_id.clone(), &fabric_hooks);
    // The Rafka-time this process composes: held by no one until the join (or the Day-0 root's
    // boot) adopts it. Every gossip stamp, lifecycle event and fleet judgement this admin makes
    // reads this one reader.
    let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
    let clock: rdm_transport::clock::SharedClock = Arc::new(rafka_time.clone());
    // The cert material, chosen explicitly (fabric-certs.md "How RDM is instantiated"): a node-admin
    // started with no choice is refused here, by name, before it touches anything.
    let issuer = Arc::new(std::mem::take(&mut wiring.certs).issuer(&cfg.data_dir, crate::certs::day_zero(&cfg), rafka_time.clone()).map_err(|e| format!("{}: {e}", cfg.launch.as_ref().map(|l| l.name.to_string()).unwrap_or_else(|| "node-admin".into())))?);
    // The fabric's cert policy is the bootstrap admin's own (fabric-certs.md §Tracking: read once by
    // the bootstrap admin); a launched admin takes it from its entry pull below.
    if cfg.launch.is_none() {
        issuer.set_policy(cfg.cert_policy);
    }
    // The declared values this admin holds, kept beside its data dir as the root is (`ca-secret`), so
    // the same admin started again holds the same values: the value never changes while the fabric runs.
    declared_secrets.keep(&cfg.data_dir).map_err(|e| format!("refusing to start: {e}"))?;
    let key_file = cfg.data_dir.join("node-key");
    let key_held = crate::blocking::blocking_io(move || key_file.try_exists()).await.map_err(|e| format!("{}: {e}", cfg.data_dir.join("node-key").display()))?;
    let step = if key_held {
        tracing::info_span!(parent: &boot, "rdm.mesh.node.resolve.via-identity-loaded")
    } else {
        tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-identity-minted")
    };
    let key = load_or_mint_key(&cfg.data_dir).await?;
    drop(step);
    let storage_step = tracing::info_span!(parent: &boot, "rdm.mesh.node.resolve.via-storage-opened", data_dir = %cfg.data_dir.display(), store_dir = %cfg.store_dir.display(), topology_storage_provider = %cfg.topology_storage_provider, fabric_id = tracing::field::Empty, inherited_root = tracing::field::Empty, writes_to = tracing::field::Empty, mesh_id = tracing::field::Empty, error = tracing::field::Empty);
    // The node-admin storage boundaries: its own row, the runtimes it started, its Build journal and
    // attempt contexts in its own data dir; the fabric, mesh, nodes, connections and status stores
    // under the fabric's shared store root (R14). An admin that finds its own row in nodes.storage was
    // started on an inherited root: fabric recovery, a new node with a new node.id and incarnation over
    // the inherited data dir, at the same path.name, in the same Mesh and Fabric, holding the same Builds
    // (fabric-recovery.md; R-SB1). Every store's temp files carry this process's incarnation, decided
    // here, before any store opens: the launch's, else a new one.
    let storage_err = |e: crate::storage_error::StorageError| e.to_string();
    let incarnation = cfg.launch.as_ref().map(|l| l.incarnation.clone()).unwrap_or_else(IncarnationId::mint);
    if let (None, Ok(p)) = (&cfg.launch, FabricPolicy::bootstrap(cfg.spawn_type.as_deref())) {
        storage.qualify(p.provider).map_err(|e| format!("refusing to start: {e}"))?;
    }
    // The own row (data dir), read before the store root opens: the inherited row names its Fabric,
    // whose root this admin opens, and its mesh, whose rows it opens on (R-FI1). An admin started on an
    // inherited root is never handed its fabric id: a different one named beside the row is refused by
    // name.
    let own_row = storage.own_row(&cfg.data_dir, &incarnation).await.map_err(storage_err)?;
    if let (None, Some(own)) = (&cfg.launch, &own_row) {
        if cfg.fabric_id_named && cfg.fabric_id != own.fabric_id {
            return Err(format!(
                "refusing to start: {}: this admin's own row ({}) belongs to Fabric {}, and {} names Fabric {}: an admin started on an inherited root opens the Fabric of its own row",
                cfg.data_dir.display(),
                own.name,
                own.fabric_id,
                rdm_core::launch::ENV_FABRIC_ID,
                cfg.fabric_id
            ));
        }
        cfg.fabric_id = own.fabric_id.clone();
    }
    storage_step.record("fabric_id", tracing::field::display(&cfg.fabric_id));
    storage_step.record("inherited_root", cfg.launch.is_none() && own_row.is_some());
    let own_mesh = match (&cfg.launch, &own_row) {
        (None, Some(own)) => own.name.mesh.clone(),
        _ => cfg.mesh.clone(),
    };
    // The node id this birth runs as, the one its configuration holds (Q208: a launched node's given one,
    // else minted by the bootstrap's first step, `AdminConfig::from_env`), held before any store opens,
    // so every store line this process appends names it (R-SR3).
    let minted_node_id = cfg.node_id.clone();
    let scope = crate::storage_backend::StorageScope { data_dir: cfg.data_dir.clone(), fabric_id: cfg.fabric_id.clone(), own_mesh: own_mesh.clone(), writer: incarnation.clone(), node_id: minted_node_id.clone() };
    // Every store this admin reads and writes is reached through its one handle, never a store held
    // from here, so a provider change moves every owner at once ("Each node moves to the new
    // provider", storage.md §2).
    let topology_storage = crate::storage_backend::TopologyStorageHandle::open(&providers, &cfg.topology_storage_provider, scope).await?;
    let (fabric_backed, mesh_storage, nodes_storage, root_runtimes, journal, finish_writes) = (topology_storage.fabric(), topology_storage.meshes(), topology_storage.nodes(), topology_storage.root_runtimes(), topology_storage.builds(), topology_storage.finish_writes());
    storage_step.record("writes_to", topology_storage.writes_to().as_str());
    // The records this admin writes, as it holds them: every write of a node, mesh or Fabric record
    // starts from its copy here (storage-whole-records.md, "The rule"), its first writes at boot too.
    let caches = crate::record_cache::Caches::default();
    // A name claim this admin writes records the provider it writes on the claimed row (storage.md §2).
    caches.nodes.writes_to(&cfg.topology_storage_provider);
    // Every record this admin writes names its birth as its writer (Q109).
    caches.nodes.writes_as(crate::storage::BirthRef { node_id: minted_node_id.clone(), incarnation_id: incarnation.clone() });
    {
        // A storage change moving this admin: the rows it writes record the provider it writes then.
        let rows = caches.nodes.clone();
        topology_storage.on_provider_written(move |provider| rows.writes_to(provider));
    }
    let storage_changes = Arc::new(crate::storage_change::StorageChanges::new(topology_storage.clone(), providers.clone(), cfg.fabric_id.clone(), minted_node_id.clone()));
    let fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage> = crate::wiring::apply(wiring.fabric_storage.take(), fabric_backed.clone());
    let attempt_contexts = Arc::new(crate::build_claim::AttemptContexts::on(journal.clone()));
    // Each mesh's in-flight row (R-BI7 caveat 3): the one set of rows the claim door, the drive's
    // release and the recovery door all take and release.
    let mesh_claims = mesh_storage.clone();
    // A launched node-admin starts holding the runtime rows its launcher held (`CreateRequest::
    // held_runtimes`): handed in its data dir beside the launch record, put in its own nodes storage
    // before anything reads it. On a provider whose runtime rows are the fabric's shared store root's
    // (`root_runtimes`), the launching admin wrote each there (build.md §5.3: "the launching admin a
    // birth's runtime row"): the launched admin neither reads nor writes them again (Q227).
    if cfg.launch.is_some() && root_runtimes.is_none() {
        for row in crate::deployment::provider::held_runtimes_in(&cfg.data_dir).await.map_err(|e| format!("refusing to start: {e}"))? {
            nodes_storage.put_runtime(&row).await.map_err(storage_err)?;
        }
    }
    // The adoption launch (Q156; fabric-recovery.md "Nothing alive"): what the store holds is read with
    // nothing written, and a store root that does not hold the fabric, its provider, the mesh or the lost
    // fabric-primary at this path.name refuses the start by name. An inherited data dir gives it only its
    // custody and its endpoint key, never its path.
    let adoption = match &cfg.adopt {
        None => None,
        Some(name) => {
            if let Some(own) = own_row.as_ref().filter(|own| own.name != *name) {
                return Err(format!("refusing to start: {}: the adoption launch at {name} is on the data dir of {} ({}): an inherited data dir is the lost birth's own", cfg.data_dir.display(), own.name, own.node_id));
            }
            let a = match super::adoption::read(name, &cfg.fabric_id, &cfg.store_dir, cfg.spawn_type.as_deref(), &*fabric_storage, &*mesh_storage, &*nodes_storage, &*journal).await {
                Ok(a) => a,
                Err(e) => {
                    let e = format!("refusing to start: {e}");
                    storage_step.record("error", e.as_str());
                    return Err(e);
                }
            };
            storage_step.record("mesh_id", tracing::field::display(&a.mesh.mesh_id));
            cfg.fabric = a.fabric.name.clone();
            cfg.mesh_id = Some(a.mesh.mesh_id.clone());
            // The provider is the Fabric record's (Q156): every provider this admin prepares is it.
            cfg.spawn_type = Some(match a.provider {
                ProviderKind::Process => "process".into(),
                ProviderKind::Container => "container".into(),
            });
            Some(a)
        }
    };
    // The adoption launch's own mesh's state, the greatest its Mesh record holds: what it applies to
    // itself at its own instant and republishes once its checklist completes (Q272).
    let adopted_mesh_state = adoption.as_ref().map(|a| a.mesh.states.iter().max().copied().unwrap_or(rdm_core::status::MeshState::Pending));
    let inherited = match (&cfg.launch, own_row) {
        _ if adoption.is_some() => adoption.as_ref().map(|a| a.lost.clone()),
        (None, Some(own)) => {
            let fabric = fabric_storage.fabric().await.map_err(storage_err)?.ok_or_else(|| {
                format!("{}: nodes.storage holds this admin ({}) but fabric.storage holds no Fabric record", cfg.data_dir.display(), own.name)
            })?;
            storage_step.record("mesh_id", tracing::field::display(&own.mesh_id));
            let mesh = match own_mesh_row(&*mesh_storage, &own).await {
                Ok(row) => row,
                Err(e) => {
                    let e = format!("refusing to start: {}: {e}", cfg.data_dir.display());
                    storage_step.record("error", e.as_str());
                    return Err(e);
                }
            };
            cfg.fabric = fabric.name.clone();
            cfg.fabric_id = fabric.fabric_id.clone();
            cfg.mesh = mesh.name.clone();
            cfg.mesh_id = Some(mesh.mesh_id.clone());
            Some(own)
        }
        _ => None,
    };
    // The contradictory-start check reads the store only for a start with no launcher (Day 0, a recovery
    // started by a person): a node-admin a launcher starts is handed its rows before it runs (runtimes),
    // owes no clean data dir and reads nothing from the store at its start (Q227).
    if inherited.is_none() && cfg.launch.is_none() {
        let holds = if fabric_storage.fabric().await.map_err(storage_err)?.is_some() {
            Some(super::session::HOLDS_FABRIC)
        } else if !mesh_storage.meshes().await.map_err(storage_err)?.is_empty() {
            Some("a mesh record")
        } else if !super::rows_writer::recorded_births(&*mesh_storage, &*nodes_storage, &own_mesh).await.map_err(storage_err)?.is_empty() || !nodes_storage.runtimes().await.map_err(storage_err)?.is_empty() {
            Some("a durable topology")
        } else {
            None
        };
        refuse_contradictory_start(&cfg, holds).map_err(|e| format!("refusing to start: {e}"))?;
    }
    drop(storage_step);
    // A container fabric's node-admin is a host process its containers reach only through the
    // fabric network's gateway (a host address, reachable from host processes too): the provider is
    // prepared before the mesh endpoint binds, and Day 0 binds it on the gateway. On an inherited
    // root, the provider also proves the inherited birth exited before `take_inherited_name` writes
    // its row `Deleted` (fabric-recovery.md "Nothing alive"). A launched admin learns the policy from
    // its launcher's entry answer, after it binds.
    let early = match (&cfg.launch, FabricPolicy::bootstrap(cfg.spawn_type.as_deref())) {
        (None, Ok(p)) => {
            let prepared = crate::deployment::prepare(p, &cfg.fabric_id.to_string()).await.map_err(|e| e.to_string())?;
            let provider: Arc<dyn crate::deployment::provider::DeploymentProvider> = crate::wiring::apply(wiring.provider.take(), prepared.provider.clone());
            Some((prepared, provider))
        }
        _ => None,
    };
    // Identity and the two addresses: assigned by the launching pipeline, the bootstrap admin's own,
    // or (an inherited root) a node.id and incarnation this admin mints at the path.name and endpoint
    // key of the inherited row, with a new port (fabric-recovery.md: the re-created node-admin is a new
    // node over its inherited data dir). A new node supersedes no birth (node-stop.md, node-start.md: a stop
    // and its start alone keep the node.id and incarnation). Its bootstrap contacts are the births it last heard,
    // its own Mesh first: hints, any one of which answering is enough.
    // The fabric-recovery attempt an adoption launch claimed, held until it completes or fails.
    let mut adoption_claim: Option<Arc<super::adoption::Claimed>> = None;
    // The adoption launch's own runtime, adopted at its claim (deployment-provider.md §3, the externally
    // started Day-0 adoption): its last step, the publication, runs with the bootstrap's digest below.
    let mut adopted_at_claim: Option<CurrentRuntimeAdoption> = None;
    // Whether this birth's runtime row was kept at its adoption claim: it is written once.
    let mut own_runtime_kept = false;
    // The launch on an inherited data dir, its step and the inherited birth, until its check before its
    // first mutation decides it (below).
    let mut inherited_launch: Option<(tracing::Span, crate::storage::NodeRecord)> = None;
    let (name, node_id, incarnation, supersedes, mesh_addr, seeds) = match (&cfg.launch, &inherited) {
        (Some(l), _) => (l.name.clone(), l.node_id.clone(), incarnation.clone(), l.supersedes.clone(), l.bind_addr, l.seeds.clone()),
        (None, Some(lost)) if adoption.is_some() => {
            // The adoption launch: a new birth with its own node.id, at the lost fabric-primary's path.name,
            // or, under an in-force fabric.delete, at its own new path.name (Q159). Its claim of the
            // fabric-recovery attempt, or of the delete's next attempt, is its first recovery mutation (Q156);
            // a lost claim refuses the start by name, having written nothing else.
            let node_id = minted_node_id.clone();
            let a = adoption.as_ref().expect("an adoption launch read its store");
            let at = match (&a.delete, &cfg.adopt) {
                (Some(_), Some(new)) => new.clone(),
                _ => lost.name.clone(),
            };
            let claimed = super::adoption::claim(&at, &node_id, a, &*journal, &*mesh_claims, &attempt_contexts, crate::admin::local_now_ms())
                .instrument(boot.clone())
                .await
                .map_err(|e| format!("refusing to start: {e}"))?;
            adoption_claim = Some(Arc::new(claimed));
            // An adoption launch is the externally started Day-0 adoption (deployment-provider.md §3), and no
            // launcher records its runtime: right after its claim it adopts its current runtime and keeps its
            // runtime row on the fabric's store root, its nodes store, so a later adoption launch resolves it
            // from the store when no live node holds its digest. The rows the store root keeps are its nodes
            // store's: a lost Day-0 admin's or earlier adoption launch's runtime is resolved here as any
            // birth's is, its digest first, then its row.
            if root_runtimes.is_some() {
                let in_container = early.as_ref().and_then(|(p, _)| (p.provider.kind() == ProviderKind::Container && rdm_core::runtime::this_container_id().is_some()).then(|| p.provider.control_domain()));
                let a = CurrentRuntimeAdoption::begin_in(&at, &cfg.data_dir, &crate::model::DeploymentId::mint(), in_container).instrument(boot.clone()).await.map_err(|e| format!("refusing to start: {e}"))?;
                let row = crate::storage::RuntimeRow { node_id: node_id.clone(), name: at.clone(), incarnation_id: incarnation.clone(), runtime: a.fact().clone(), data_dir: Some(cfg.data_dir.display().to_string()) };
                nodes_storage.put_runtime(&row).await.map_err(storage_err)?;
                own_runtime_kept = true;
                adopted_at_claim = Some(a);
            }
            // Its contacts are the births the store records, its own mesh first, the lost birth not among them.
            let mut contacts = super::rows_writer::recorded_births(&*mesh_storage, &*nodes_storage, &lost.name.mesh).await.map_err(storage_err)?;
            contacts.retain(|c| c.node_id != lost.node_id);
            contacts.sort_by_key(|c| (c.name.mesh != lost.name.mesh, c.name.to_string()));
            let seeds = contacts.iter().filter_map(|c| c.gossip_addr().and_then(|a| a.ip_addrs().next().map(|ip| (a.id.to_string(), *ip)))).chain(cfg.seeds.iter().cloned()).collect();
            (at, node_id, incarnation.clone(), None, SocketAddr::new(cfg.api_bind.ip(), 0), seeds)
        }
        (None, Some(own)) => {
            let node_id = minted_node_id.clone();
            let step = tracing::info_span!(
                parent: &boot,
                "rdm.node_admin.node.create.via-inherited-root",
                node = %own.name,
                node_id = %node_id,
                incarnation_id = %incarnation.as_str(),
                inherited_node_id = %own.node_id,
                inherited_incarnation_id = %own.incarnation_id,
                fabric_id = %cfg.fabric_id,
                contacts = tracing::field::Empty,
                answered = tracing::field::Empty,
                outcome = tracing::field::Empty,
            );
            // Nothing is written here: the launch first asks whether any node-admin of the fabric answers,
            // once its endpoint can call (below, before its first mutation; Q156, Q199).
            // Its contacts are the births the store records, its own mesh first, the inherited birth not
            // among them: its row stands until this launch's node.delete, and it is no other node.
            let mut contacts = super::rows_writer::recorded_births(&*mesh_storage, &*nodes_storage, &own.name.mesh).await.map_err(storage_err)?;
            contacts.retain(|c| c.node_id != own.node_id);
            contacts.sort_by_key(|c| (c.name.mesh != own.name.mesh, c.name.to_string()));
            let seeds = contacts.iter().filter_map(|c| c.gossip_addr().and_then(|a| a.ip_addrs().next().map(|ip| (a.id.to_string(), *ip)))).collect();
            step.record("contacts", contacts.len());
            inherited_launch = Some((step, own.clone()));
            (
                own.name.clone(),
                node_id,
                incarnation.clone(),
                None,
                // The inherited endpoint key and a fresh port (the operating system assigns it), never the
                // one the inherited birth held: peers learn the new address from its digest.
                SocketAddr::new(own.transport_addr.ip(), 0),
                seeds,
            )
        }
        (None, None) => {
            let name = PathName::new(&cfg.mesh, NodeKind::NodeAdmin, 1);
            let node_id = minted_node_id.clone();
            // Day 0 (R-OW1 item 2): the bootstrap admin, its mesh's primary, claims its own path.name,
            // the first write of its birth, before Ready (`claim_day0_name`). A name another
            // node holds refuses the start by name. A recovery start (both primary flags) is no Day 0: its row
            // is written when it holds its mesh's seat, as every member's is (`NodeRows::arrivals`).
            if !cfg.mesh_primary && !cfg.fabric_primary {
                // The row records what this birth runs (build-deployment.md §2.3): its running executable,
                // hashed now; there is no configured sha to hold it to.
                let provider = early.as_ref().map(|(p, _)| p.provider.kind()).unwrap_or(ProviderKind::Process);
                let running = super::runner::running_binding(provider).await.map_err(|e| format!("refusing to start: {}: {e}", cfg.data_dir.display()))?;
                crate::node_rows::claim_day0_name_in_boot(&boot, &*nodes_storage, &caches.nodes, &name, &node_id, Some(&running))
                    .await
                    .map_err(|e| format!("refusing to start: {}: Day 0's {node_id} claims {name}: {e}", cfg.data_dir.display()))?;
            }
            (name, node_id, incarnation.clone(), None, SocketAddr::new(cfg.api_bind.ip(), 0), cfg.seeds.clone())
        }
    };

    // This node's own connections rows, appended to the store root from an open that reads none of them (R14, Q227).
    let connections_storage: Arc<dyn crate::storage::ConnectionsStorage> = topology_storage.connections(name.clone()).await.map_err(storage_err)?;
    boot.record("node", tracing::field::display(&name));
    boot.record("incarnation_id", tracing::field::display(&incarnation.as_str()));
    // A host-process admin binds on the gateway; an admin that itself runs in a container of the
    // fabric binds on its own address there (its control address), the gateway not being local.
    let mesh_addr = match (&early, &cfg.launch, &inherited) {
        (Some((p, _)), None, None) if p.provider.kind() == ProviderKind::Container && rdm_core::runtime::this_container_id().is_none() => SocketAddr::new(p.admin_ip, 0),
        _ => mesh_addr,
    };

    // The mesh endpoint: gossip for the topology view and Build facts.
    // The transport is the one every node kind binds with (`rdm_transport::endpoint::transport_config`).
    // The app's own protocols are registered Connections like every node's: one of RDM's own two
    // ALPNs among them refuses the start by name (fabric-certs.md §On a connection).
    rdm_transport::connections::validate_alpns(&wiring.alpns).map_err(|e| format!("{name}: an app protocol: {e}"))?;
    let mut alpns = vec![iroh_gossip::ALPN.to_vec(), rdm_transport::ALPN.to_vec()];
    alpns.extend(wiring.alpns.iter().map(|alpn| alpn.name.clone()));
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-endpoint-bound", node = %name, requested = %mesh_addr, bound = tracing::field::Empty);
    let endpoint = rdm_transport::endpoint::bind_exact(key.clone(), mesh_addr, alpns.clone(), rdm_transport::endpoint::transport_config())
        .await
        .map_err(|e| format!("mesh address {mesh_addr}: {e}"))?;
    let mesh_addr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap_or(mesh_addr);
    step.record("bound", tracing::field::display(mesh_addr));
    drop(step);
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-gossip-started", node = %name);
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    drop(step);
    // Entry: what this admin holds, for a node it launched (filled once the
    // admin's own digest exists; until then a join is told it is not ready).
    let entry: Arc<std::sync::OnceLock<EntryState>> = Arc::new(std::sync::OnceLock::new());
    // Node RPC on the admin's one endpoint: the status declarations it applies as an authority
    // (`status_rpc`). The authority is filled once this admin holds a view; until then NotReady.
    // This admin's own connections: none at its birth, read from nowhere (Q227), then kept by what its
    // client observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    let connections = Arc::new(crate::connections_writer::ConnectionsWriter::new(
        rdm_core::connections::ConnectionEnd { name: name.clone(), node_id: node_id.clone(), incarnation: Some(incarnation.clone()) },
        connections_storage.clone(),
        Arc::new(std::sync::Mutex::new(rdm_core::connections::ConnectionsHeld::new())),
    ));
    // The process's one live-node resolver: the client's and the server's (an accepted
    // connection's peer is named by it).
    let node_rpc_resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
    connections.held().lock().unwrap().set_topology_view(node_rpc_resolver.clone());
    let status_door: crate::status_rpc::StatusDoorSlot = Arc::new(std::sync::OnceLock::new());
    let status_door_set = Arc::new(tokio::sync::Notify::new());
    let claim_slot: crate::build_claim::ClaimSlot = Arc::new(std::sync::OnceLock::new());
    // The births this admin deployed and awaits a `JoinNode` from, and the door that answers it.
    let joins = Arc::new(crate::join::Joins::over(nodes_storage.clone()));
    // The runner, once built: the seat watch registers a seat holder heard again through it (Q87).
    let runner_slot: Arc<std::sync::OnceLock<Arc<AdminRunner>>> = Arc::default();
    let heard_again_slot: crate::heard_again::HeardAgainSlot = Arc::default();
    let join_slot: crate::join::JoinSlot = Arc::new(std::sync::OnceLock::new());
    let topology_slot: crate::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    let build_facts_slot: crate::build_facts_read::BuildFactsSlot = Arc::new(std::sync::OnceLock::new());
    let build_slot: crate::build_op::BuildSlot = Arc::new(std::sync::OnceLock::new());
    let handover_slot: crate::fabric_handover::HandoverSlot = Arc::new(std::sync::OnceLock::new());
    // This admin's view, once built: the Build log's target reads fabric recovery from it (Q122).
    let view_slot: Arc<std::sync::OnceLock<Arc<tokio::sync::RwLock<Topology>>>> = Arc::new(std::sync::OnceLock::new());
    // The process's one Node RPC client is made before the server: core Forward is one direct inner
    // call through it, and every other caller in the process takes it by clone.
    // One delivery state for the process (node-rpc.md §43): its birth mints every frame's identity,
    // and its client and its server share the windows and the open invocations.
    let delivery = rdm_transport::Delivery::new(rdm_core::framing::Birth { node_id: node_id.clone(), incarnation: incarnation.clone() });
    let node_rpc = crate::node_rpc::ProcessNodeRpc::new(node_rpc_resolver.clone(), endpoint.clone()).with_delivery(delivery.clone());
    // Closed until the Status authority is filled and its view holds this admin (it decides from
    // that view): the router refuses every op, ping included, with a typed NotReady before then.
    let rpc_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rpc_builder = crate::build_op::serve(crate::fabric_handover::serve(crate::build_facts_read::serve(crate::read_op::serve(crate::topology_read::serve(rpc_server(connections.clone(), node_rpc.client.clone(), status_door.clone(), claim_slot.clone(), join_slot.clone()), topology_slot.clone()), status_door.clone()), build_facts_slot.clone()), handover_slot.clone()), build_slot.clone());
    // The app's own ops (each behind the gate of its hook), before the catalog is built.
    let rpc_builder = match wiring.serve_app.take() {
        Some(serve_app) => serve_app(rpc_builder),
        None => rpc_builder,
    };
    let rpc_server = rpc_builder
        .with_ready_gate(rpc_ready.clone())
        .with_delivery(delivery)
        .build(rdm_transport::ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .map_err(|e| format!("the admin's protocol catalog refused to build: {e:?}"))?;
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.add.via-alpn-registered", node = %name, alpns = "node-rpc,gossip");
    let router_builder = IrohRouter::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .accept(rdm_transport::ALPN, rpc_server.clone());
    // The app's protocols on this endpoint: RDM's accept loop and `node.connection.create` for each,
    // their rows in this admin's connections log. The member cert it presents is its join's.
    let (router_builder, data_connections) = route_app_alpns(router_builder, &endpoint, node_rpc_resolver.clone(), connections.clone(), &wiring.alpns)
        .map_err(|e| format!("{name}: {e}"))?;
    // The fabric's root set this admin admits its app protocols' dialers on and writes into every
    // launch it makes (`RDM_ROOTS`, fabric-ca-renew.md "Launch"): the set its own launch carried, else
    // the root it holds; none when it was started with no certs, and then it admits by name.
    let own_roots = cfg.launch.as_ref().map(|l| l.roots.clone()).filter(|r| !r.is_empty()).or_else(|| issuer.root().map(|r| vec![r])).unwrap_or_default();
    data_connections.admit_on(own_roots, rafka_time.clone());
    // An owed Proxy close (connections.md §10) whose write was refused is attempted again while
    // owed, and a Direct series on an app protocol is dialled again through the app's registry; an
    // admin with no app protocol has no data plane and no recovery dial. The task ends with the
    // process.
    let _reconciler = if wiring.alpns.is_empty() {
        connections.spawn_reconciler(crate::connections_writer::RECONCILER_TICK, None)
    } else {
        let dial = rdm_node::connection_reconnect::registry_recovery_dial(&data_connections, None);
        rdm_node::connection_reconnect::spawn_alpn_reconciler(&connections, crate::connections_writer::RECONCILER_TICK, dial)
    };
    let iroh_router = router_builder.spawn();
    drop(step);
    tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-accept-loop-started", node = %name).in_scope(|| tracing::info!("the router accepts node-rpc and gossip connections"));
    let seed_addrs: Vec<EndpointAddr> = seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    if cfg.mesh_primary || cfg.fabric_primary {
        tracing::info_span!(
            "rdm.node_admin.node.update.via-recovery-start",
            node = %name,
            mesh = %cfg.mesh,
            mesh_primary = cfg.mesh_primary,
            fabric_primary = cfg.fabric_primary,
            seeds = seed_addrs.len(),
        )
        .in_scope(|| tracing::info!("started to recover: Ready is held until the topology and the current Build are held"));
    }
    // The adoption launch's checks after its claim and before its first other mutation (Q156): node
    // investigation of the lost birth, then whether any node-admin the store records answers. An answer
    // refuses it NameHeld, the attempt closed through the failure path. Otherwise, as every own-mesh
    // recovery of a node-admin, the lost birth's runtime is terminated on every read but os-stopped, its
    // name released and taken by this birth.
    if let Some(claimed) = &adoption_claim {
        let provider = match &early {
            Some((_, p)) => p.clone(),
            None => return Err(format!("refusing to start: {}: no deployment provider prepared for the adoption of {} at {}", cfg.data_dir.display(), claimed.lost.node_id, claimed.lost.name)),
        };
        let answers = super::adoption::ProbeAnswers {
            me: name.clone(),
            me_id: node_id.clone(),
            client: node_rpc.client.clone(),
            resolver: node_rpc_resolver.clone(),
            provider: provider.clone(),
            nodes: nodes_storage.clone(),
            meshes: mesh_storage.clone(),
            fabric: crate::model::Fabric { id: cfg.fabric_id.clone(), name: cfg.fabric.clone(), status: crate::model::ScopeStatus::Pending, provider: provider.kind() },
            inherited: None,
        };
        let checked = super::adoption::check(claimed, &answers).instrument(boot.clone()).await;
        let released: Result<(), super::adoption::Refused> = match checked {
            Err(refusal) => Err(refusal),
            // Under an in-force fabric.delete (Q159) nothing of the lost birth is released or re-created: the
            // launch claims its own new path.name, the one create a delete admits; the teardown deletes the
            // lost birth with its mesh.
            Ok(_) if claimed.under_delete.is_some() => super::adoption::take_new_name(&name, &node_id, &*nodes_storage, &caches.nodes)
                .instrument(boot.clone())
                .await
                .map_err(|e| super::adoption::Refused { reason: e, answered: name.to_string() }),
            Ok(_) => async {
                let lost = &claimed.lost;
                let own_runtime = nodes_storage.runtime(&lost.node_id, &lost.incarnation_id).await.map_err(storage_err)?;
                if let Some(handle) = own_runtime.as_ref().and_then(|r| crate::deployment::provider::adopt(&*provider, &r.runtime).ok()) {
                    let mode = crate::deployment::provider::TerminationMode::Graceful { grace: crate::deployment::pipeline::Timeouts::default().stop_grace };
                    crate::deployment::provider::terminate_unless_os_stopped(&*provider, &handle, mode)
                        .await
                        .map_err(|e| format!("{node_id} cannot terminate the lost birth {} ({}) at {}: {e}", lost.node_id, lost.incarnation_id.as_str(), lost.name))?;
                }
                super::adoption::release_name(claimed, &node_id, &*nodes_storage, &caches.nodes, &*journal).await
            }
            .instrument(boot.clone())
            .await
            .map_err(|e| super::adoption::Refused { reason: e, answered: claimed.lost.name.to_string() }),
        };
        if let Err(refusal) = released {
            if let Err(e) = super::adoption::fail(claimed, &*journal, &*mesh_claims, &refusal).await {
                return Err(format!("refusing to start: {refusal}; and the claimed attempt could not be closed: {e}"));
            }
            return Err(format!("refusing to start: {refusal}"));
        }
    }
    // The launch on an inherited data dir (fabric-recovery.md; node-admin-lifecycle.md §4.4): before its
    // first mutation it asks whether any node-admin the store records answers, the adoption launch's check
    // (Q156: "a launch that finds a living admin is refused, never an ordinary admin at the primary's name";
    // Q199). An answer refuses it `NameHeld` by name, having written nothing: the fabric has a live admin,
    // whose fabric recovery re-creates the birth at this path.name. Otherwise the inherited birth is
    // re-created through node recovery, never waited for (fabric-recovery.md "Nothing alive"):
    // `TerminateRuntime` first on every read of the old runtime but os-stopped (node-runtime-inspect.md
    // "What follows", Q145), then its node.delete and this node's claim of its path.name, the first writes
    // of this birth (`take_inherited_name`). An old process that comes back is refused at admission and
    // terminated.
    if let Some((step, own)) = inherited_launch.take() {
        let provider = match &early {
            Some((_, p)) => p.clone(),
            None => return Err(format!("refusing to start: {}: no deployment provider prepared for the inherited birth {} at {}", cfg.data_dir.display(), own.node_id, own.name)),
        };
        let answers = super::adoption::ProbeAnswers {
            me: name.clone(),
            me_id: node_id.clone(),
            client: node_rpc.client.clone(),
            resolver: node_rpc_resolver.clone(),
            provider: provider.clone(),
            nodes: nodes_storage.clone(),
            meshes: mesh_storage.clone(),
            fabric: crate::model::Fabric { id: cfg.fabric_id.clone(), name: cfg.fabric.clone(), status: crate::model::ScopeStatus::Pending, provider: provider.kind() },
            inherited: Some(own.node_id.clone()),
        };
        if let Some(refused) = super::adoption::admin_answered(&own, &answers).instrument(step.clone()).await {
            step.record("answered", refused.answered.as_str());
            step.record("outcome", "name-held");
            step.in_scope(|| tracing::info!("the launch on an inherited data dir is refused: a node-admin of the fabric answered before its first mutation"));
            return Err(format!("refusing to start: {}: {refused}", cfg.data_dir.display()));
        }
        let own_runtime = nodes_storage.runtime(&own.node_id, &own.incarnation_id).await.map_err(storage_err)?;
        if let Some(handle) = own_runtime.as_ref().and_then(|r| crate::deployment::provider::adopt(&*provider, &r.runtime).ok()) {
            let mode = crate::deployment::provider::TerminationMode::Graceful { grace: crate::deployment::pipeline::Timeouts::default().stop_grace };
            crate::deployment::provider::terminate_unless_os_stopped(&*provider, &handle, mode)
                .instrument(step.clone())
                .await
                .map_err(|e| format!("refusing to start: {}: {} cannot terminate the inherited birth {} ({}) at {}: {e}", cfg.data_dir.display(), node_id, own.node_id, own.incarnation_id.as_str(), own.name))?;
        }
        // A name another node holds refuses the start by name.
        crate::node_rows::take_inherited_name(&*nodes_storage, &caches.nodes, &own, &node_id)
            .instrument(step.clone())
            .await
            .map_err(|e| format!("refusing to start: {}: {} takes {} over the inherited birth {}: {e}", cfg.data_dir.display(), node_id, own.name, own.node_id))?;
        step.record("outcome", "taken");
        step.in_scope(|| tracing::info!("fabric recovery: a new node over the inherited data dir, at its path.name"));
    }
    // No crate of RDM serves a route or opens an HTTP listener (rdm-crate-and-test-layout.md §2): the
    // admin's application contact (`admin_api_base`, config, never checked) names this admin's node id.
    let api_base = format!("rdm://{node_id}");
    // A launched admin takes the runtime record its launcher's pipeline made available; it is
    // part of the digest it reports at its join.
    let launched_runtime = match &cfg.launch {
        Some(_) => {
            let dir = cfg.data_dir.clone();
            Some(crate::node_self::wait_for_own_record(&dir, Duration::from_secs(10)).await?)
        }
        None => None,
    };
    // The mesh's id names its topology channel: the launch names it (the
    // launching admin always does), else the join answer's projection knows
    // it, else this admin is the mesh's first and mints it.
    // The process's one Node RPC client, built before the join: the join is its first call, and
    // the topology feed starts once the process holds a view.
    let launched_anchor = if inherited.is_some() { None } else { cfg.launch.as_ref().and(seed_addrs.first().cloned()) };
    let launcher_of_launch = cfg.launch.as_ref().and_then(|l| l.launcher.clone());
    // The join's authority: the admin that deployed this one. A birth that makes no `JoinNode` (the
    // Day-0 root, an admin started on an inherited root, a recovery start) has none, and says why.
    let joins_through = match (&launched_anchor, &launcher_of_launch, &launched_runtime) {
        (Some(anchor), Some(launcher), Some(runtime)) => Some((anchor.clone(), launcher.clone(), runtime.clone())),
        _ => None,
    };
    // This admin's first call after it binds: `JoinNode`, carrying its full digest with the
    // addresses it really bound, to the admin that deployed it.
    let join_types = wiring.types.clone();
    rdm_core::app_types::check(&join_types).map_err(|k| format!("app type declaration: {k:?}"))?;
    let join_digest: Option<MeshDigest> = joins_through.as_ref().map(|(_, _, runtime)| MeshDigest { held_hash: rdm_core::app_types::hash(&join_types.iter().cloned().collect()),
        fabric_id: cfg.fabric_id.clone(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.clone(),
            endpoint_id: EndpointId(key.public().to_string()),
            transport_addr: mesh_addr,
            incarnation: incarnation.clone(),
            supersedes: supersedes.clone(),
            runtime: Some(runtime.clone()),
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: Some(api_base.clone()),
        emitted_at_rafka_ms: 0,
        digest_seq: 0,
        mesh_id: cfg.mesh_id.clone(),
        in_flight: None,
        load: None,
        gossip: None,
        data_dir: Some(cfg.data_dir.display().to_string()), is_primary: None, is_fabric_primary: None
    });
    let joining = match &joins_through {
        Some((_, launcher, _)) => crate::workflows::node_start::Joining::AcceptingPrimary { label: launcher.name.to_string() },
        None if inherited.is_some() => crate::workflows::node_start::Joining::None(crate::node_hooks::NoAcceptingPrimary::InheritedRoot),
        None if cfg.mesh_primary || cfg.fabric_primary => crate::workflows::node_start::Joining::None(crate::node_hooks::NoAcceptingPrimary::Recovery),
        None => crate::workflows::node_start::Joining::None(crate::node_hooks::NoAcceptingPrimary::Day0Root),
    };
    let resolve: Box<dyn FnOnce() -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> + Send> = {
        let (resolver, through) = (node_rpc_resolver.clone(), joins_through.clone());
        Box::new(move || {
            let (anchor, launcher, _) = through.expect("a start that joins has its launching admin");
            resolver.apply(
                rdm_transport::ResolvedNode {
                    node_id: launcher.node_id.clone(),
                    name: launcher.name.clone(),
                    endpoint_id: anchor.id,
                    transport_addr: anchor.ip_addrs().next().copied().ok_or_else(|| crate::node_self::StartFailed { step: "node.join", reason: format!("the launching admin {} has no address in this admin's seeds", launcher.name) })?,
                    incarnation: launcher.incarnation.clone(),
                },
                None,
            );
            Ok(crate::workflows::node_start::JoinTo::Launcher { target: rdm_transport::NodeTarget::ExactNode(launcher.node_id.clone()), endpoint: anchor.id, name: launcher.name.clone() })
        })
    };
    // What the start brought up, once it did: handed to the caller.
    let out: Arc<Mutex<Option<Running>>> = Arc::default();
    let starting = crate::workflows::node_start::Starting { root: boot.clone(), name: name.to_string(), path: name.clone(), node_id: node_id.clone(), client: node_rpc.client.clone(), rafka_time: rafka_time.clone() };
    // The admin plugs into `node.start` by continuations: each step runs in the scope the previous
    // one left. Its channel step also fills its stores; its installed-topology step takes its
    // entry and builds what it serves and elects with; after its hook its Ready check reports.
    let enter: crate::workflows::node_start::EnterSeam<String> = {
        let out = out.clone();
        let own_issuer = issuer.clone();
        Box::new(move |step: tracing::Span, joined: Option<crate::wire::JoinAnswer>| -> crate::workflows::node_start::BoxFut<Result<(TopologyView, crate::workflows::node_start::JoinedSeam<String>), String>> {
            Box::pin(async move {
                // The member cert the launching admin's signer issued for this birth, carried by the join that
                // admitted it; none yet for a birth that made no JoinNode (its own maker issues its own below).
                let member_cert: Option<Vec<u8>> = joined.as_ref().map(|a| a.control.member_cert.clone());
                if let Some(cert) = &member_cert {
                    rdm_node::node::install_join_cert(&data_connections, cert, &name.to_string(), &node_id, &incarnation);
                }
                // The fabric's Day-0 root has no authority above it: it adopts its own OS clock as rafka-time,
                // once, here. A recovery start (a flag or an inherited root) takes it from the nodes it reaches below.
                let mut sole_node_root = false;
                if cfg.launch.is_none() && inherited.is_none() && !cfg.mesh_primary && !cfg.fabric_primary {
                    crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_DAY0_ROOT);
                    sole_node_root = true;
                }
                // A node-admin birth that made no JoinNode, and is the Day-0 root, an admin re-created at its path
                // over its inherited data dir, or an adoption launch, is its own maker (fabric-certs.md, When a cert
                // is issued: "a new mesh's first node-admin is deployed | the maker, which holds the root or an
                // issuing cert that may mint for that mesh"): it issues its own member cert below, once it holds
                // rafka-time.
                let own_maker = member_cert.is_none() && (sole_node_root || inherited.is_some());
                let mesh_id = match (&cfg.mesh_id, &joined) {
                    (Some(id), _) => id.clone(),
                    (None, Some(answer)) => return Err(format!("{name}: a launched admin takes its mesh's id from its launch, and this launch names none (joined {})", answer.served_by)),
                    (None, None) => MeshId::mint(),
                };
                // Subscribe first, pull second: what changes during the pull arrives by
                // gossip, and the view keeps the newer copy.
                step.record("mesh_id", tracing::field::display(&mesh_id));
                step.record("seeds", seed_addrs.len());
                let topology_view = TopologyView::join(&gossip, &endpoint, &cfg.fabric_id, &cfg.mesh, &mesh_id, &name.to_string(), clock.clone(), seed_addrs.clone(), &wiring.types.iter().map(String::as_str).collect::<Vec<_>>())
                    .await
                    .map_err(|e| format!("topology view: {e}"))?;
                topology_view.set_author(&rdm_core::PublisherId { node: name.to_string(), node_id: node_id.clone(), incarnation: incarnation.clone() });
                // The name this admin holds is its topology view's (R12): its connections rows name it as their
                // source from here on, whatever name the join hands it or an `ApplyNodeName` gives it.
                {
                    let (held, birth) = (topology_view.clone(), name.clone());
                    connections.follow_held_name(move || held.held_name(&birth))?;
                }
                // The row's name and config a launched admin's join handed: held before it publishes anything
                // (R-JN1); at a first start the name is its create spec's, verified by the join.
                let handed: Option<(Option<PathName>, Option<BTreeMap<String, String>>)> = joined.as_ref().map(|a| (a.control.name.clone(), a.control.config.clone()));
                if let Some((n, c)) = &handed {
                    if let Some(n) = n {
                        topology_view.set_name(n.clone());
                    }
                    if let Some(c) = c {
                        topology_view.set_config(c.clone());
                    }
                }
                tracing::info_span!(
                    parent: &step,
                    "rdm.node.mesh.update.via-joined",
                    node = %topology_view.node_name(),
                    node_id = %node_id,
                    mesh = %cfg.mesh,
                    mesh_id = %mesh_id,
                    which = rdm_core::status::WhichMesh::Own.name(),
                )
                .in_scope(|| tracing::info!("joined the mesh channel"));
                drop(step);
                // An admin that made no `JoinNode` and is no Day-0 root (an inherited root, or a recovery
                // started with a seed) holds no rafka-time yet. It takes it from the first node the store
                // names that answers; when the root holds no other node from which to obtain it (none is
                // named, or none answers), this admin is the only node, the fabric-primary, and its own clock
                // is rafka-time from the start (fabric-recovery.md "Nothing alive"; origin-ordering.md): every
                // node the store names is re-created through node recovery, never waited for, so each is gone
                // from every seat here.
                if !rafka_time.is_adopted() {
                    let contacts: Vec<crate::reenter::MapNode> = super::rows_writer::recorded_births(&*mesh_storage, &*nodes_storage, &cfg.mesh)
                        .await
                        .map_err(storage_err)?
                        .into_iter()
                        .filter(|r| r.node_id != node_id)
                        .collect();
                    let alone = inherited.is_some() || seed_addrs.is_empty();
                    let gone: Vec<IncarnationId> = contacts.iter().map(|c| c.incarnation.clone()).collect();
                    let answered = if contacts.is_empty() && alone {
                        Err("the store names no other node".to_string())
                    } else {
                        crate::rafka_time::adopt_from_reachable(&rafka_time, &name, &node_rpc.client, &node_rpc_resolver, &topology_view, contacts, &boot).await
                    };
                    match answered {
                        Ok(_) => {}
                        Err(_) if alone => {
                            for incarnation in gone {
                                topology_view.digest_view.seats.mark_gone(incarnation);
                            }
                            crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_NOTHING_ALIVE);
                            sole_node_root = true;
                        }
                        Err(e) => return Err(format!("refusing to start: {e}")),
                    }
                }
                // Its own maker holds a member cert of its own, issued from its own issuer once it holds rafka-time
                // and before it can dial an app protocol, as every birth holds one ("held by the birth it was
                // issued to"), so the app protocols it dials admit it. Its dates are on its own record, as a join's
                // admission writes them (§Tracking), so the fabric-primary renews it in process. An own maker
                // whose issuer cannot mint for its mesh refuses to start by name: no admin runs without one.
                let member_cert = match member_cert {
                    None if own_maker => {
                        let kind = if sole_node_root && inherited.is_none() {
                            "the Day-0 root"
                        } else if adoption_claim.is_some() {
                            "the adoption launch"
                        } else {
                            "the admin re-created over its inherited data dir"
                        };
                        let birth = crate::certs::BirthIdentity { node_id: node_id.clone(), incarnation: incarnation.clone(), mesh: name.mesh.clone(), name: name.clone(), endpoint_key: EndpointId(key.public().to_string()) };
                        let issued = own_issuer.issue_member(&birth).map_err(|e| format!("refusing to start: {name}: {kind} is its own maker and its own member cert could not be issued: {e}"))?;
                        if !issued.bytes.is_empty() {
                            let dates = crate::cert_renewal::dates_of(&issued);
                            let signing = own_issuer.held_mint(&birth.mesh).map(|m| crate::cert_renewal::signing_dates(&m));
                            let base = crate::storage::NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Pending);
                            caches
                                .nodes
                                .update(&*nodes_storage, &node_id, Some(base), |r| {
                                    let b = r.birth_mut(&incarnation, &name);
                                    b.cert = Some(dates);
                                    b.signing_cert = signing;
                                })
                                .await
                                .map_err(|e| format!("refusing to start: {name}: {kind}'s own member cert dates could not be written on its record: {e}"))?;
                        }
                        rdm_node::node::install_join_cert(&data_connections, &issued.bytes, &name.to_string(), &node_id, &incarnation);
                        Some(issued.bytes)
                    }
                    other => other,
                };
                let backbone = Backbone::join(&gossip, &endpoint, &topology_view, &cfg.mesh, rdm_core::PublisherId { node: name.to_string(), incarnation: incarnation.clone(), node_id: node_id.clone() }, seed_addrs.clone())
                    .await
                    .map_err(|e| format!("backbone: {e}"))?;
                tracing::info_span!(
                    parent: &boot,
                    "rdm.node.mesh.update.via-joined",
                    node = %topology_view.node_name(),
                    node_id = %node_id,
                    mesh = %cfg.mesh,
                    mesh_id = %mesh_id,
                    which = rdm_core::status::WhichMesh::Backbone.name(),
                )
                .in_scope(|| tracing::info!("joined the backbone channel"));
                if let Some(seam) = wiring.announcement_seam.take() {
                    let me = name.to_string();
                    backbone.withhold_announcements(Arc::new(move |announcement, channel| seam.withholds(&me, announcement, channel)));
                }
                // fabric.storage: `fabric/` on the store root, whose one writer is the fabric-primary (R14 item
                // 2): Day 0 writes the Fabric identity (below). An admin that joined takes the identity its
                // entry pull carries, never reading the store (Q227); a start with no launcher reads it there. An
                // identity of another Fabric is refused.
                let fabric_identity = crate::fabric_storage::FabricIdentity { fabric_id: cfg.fabric_id.clone(), name: cfg.fabric.clone() };
                let held = match &joined {
                    Some(answer) => answer.control.fabric.clone().map(|f| (f.fabric_id, f.name, "its entry pull")),
                    None => fabric_storage.fabric().await.map_err(|e| e.to_string())?.map(|f| (f.fabric_id, f.name, "fabric.storage")),
                };
                if let Some((fabric_id, fabric_name, from)) = held {
                    if fabric_id != cfg.fabric_id {
                        return Err(format!("{from} holds the identity of Fabric {fabric_id} ({fabric_name}), this admin is configured for Fabric {}", cfg.fabric_id));
                    }
                }
                let mut tasks_early: Vec<tokio::task::JoinHandle<()>> = Vec::new();
                // When the fabric seat moves to another holder, this admin, if it is a mesh primary, pulls
                // rafka-time once from the new fabric-primary.
                tasks_early.push(stop_gate.spawn(crate::rafka_time::run_seat_move_pull(rafka_time.clone(), name.clone(), node_id.clone(), node_rpc.client.clone(), topology_view.clone())));
                // A delta that does not follow what this admin holds desynchronizes that source of its mesh
                // channel; it tops up from its Mesh's own primary (gossip.md §3.3) with `GetTopology` of each
                // such mesh, as every node does. Held behind the gate: a stopped admin tops up nothing, and a
                // gap signalled while it was stopped is topped up once it runs again.
                tasks_early.push(stop_gate.spawn(topology_view.top_ups(crate::topology_read::top_up_fetch(node_rpc.client.clone()))));
                // The recorded primaries: an election input this admin enters with (elections §3), its own
                // included. A launched admin reads no store (Q227): its mesh-primary's hydration sync serves
                // them (`Primaries`, node.topology.get), a lost birth's record marked gone. A start with no
                // launcher, the re-born fabric-primary with nobody to pull from, reads them off the node
                // records of the fabric's store root. A record older than one held changes nothing.
                if cfg.launch.is_none() {
                    crate::admin::rows_writer::take_recorded_primaries(&*mesh_storage, &*nodes_storage, &cfg.mesh, topology_view.primaries()).await.map_err(storage_err)?;
                }
                if let Some(own) = &inherited {
                    topology_view.digest_view.seats.mark_gone(own.incarnation_id.clone());
                }
                // The current Build: Day 0 accepts the first Build itself; every other admin reads it from
                // the store root, its entry pull naming it.
                let accepted = Arc::new(AcceptedStore::new(fabric_storage.clone(), name.to_string()));
                let records = Arc::new(Records { caches: caches.clone(), ..Records::default() });
                records.set_me(node_id.clone());
                // `fabric/` has one writer, the fabric-primary (R14 item 2): `FabricRowsWriter`, a sweep on every
                // seat change.
                {
                    let seats = topology_view.primaries().clone();
                    let mut writer = FabricRowsWriter::new((node_id.clone(), incarnation.clone()), fabric_identity.clone(), fabric_storage.clone());
                    let mut changed = seats.subscribe();
                    let stop = stop_gate.clone();
                    tasks_early.push(tokio::spawn(async move {
                        loop {
                            // Held here while the admin is stopped: its own wait, outside every lock.
                            let turn = stop.turn("seat-rows").await;
                            writer.sweep(&seats).await;
                            drop(turn);
                            if changed.changed().await.is_err() {
                                return;
                            }
                        }
                    }));
                }
                // The facts on this admin's mesh's births, on every mesh and on the fabric: folded into
                // what this admin applied when it becomes its mesh's primary.
                records.set_status(nodes_storage.clone(), mesh_storage.clone(), fabric_storage.clone(), own_mesh.clone(), name.clone(), node_id.clone());
                records.set_digest_view(topology_view.digest_view.clone());
                let builds = Arc::new(FabricBuildStateAdapter::new(journal.clone(), &topology_view.location_view(), name.to_string(), fabric_seat_source(&topology_view.digest_view, &records, (node_id.clone(), incarnation.clone()), &name, view_slot.clone())));
                // Only the fabric-primary, fabric recovery's executor in its place, or an admin started with
                // no launcher while no live birth holds the fabric-primary record reads the Build records
                // from here on (Q227); a launched admin's start reads none (Q242).
                journal.read_records_while(build_records_reader(&topology_view.digest_view, &records, (node_id.clone(), incarnation.clone()), &name, cfg.launch.as_ref().and_then(|l| l.launcher.as_ref()).is_none(), view_slot.clone())).await;
                // The Build state every consumer after the fetch shares: the control routes, the executor, the
                // pipelines and the drift check.
                let runs = Arc::new(crate::build_run::AttemptRuns::default());
                // An application's wrap of the Build state sees every write before the run decides
                // whether it writes it or reports it up its stream (Q118).
                let framed: Arc<dyn BuildStateAdapter> = Arc::new(crate::build_run::FramedBuilds::new(builds.clone(), runs.clone()));
                let builds_dyn: Arc<dyn BuildStateAdapter> = crate::wiring::apply(wiring.builds.take(), framed);

                // Fabric policy and the first view. The bootstrap admin takes its policy
                // from MESH_SPAWN_TYPE. A launched admin takes its entry from the admin
                // that launched it (its first seed) over QUIC: that admin's topology
                // projection and the topology view it is projected from, recorded as heard,
                // so this admin's first view is its launcher's; the fabric record carries
                // the policy. Without that answer it cannot know the policy, and it
                // refuses to start by name.
                let entry_floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>> = Arc::default();
                let _ = build_facts_slot.set(Arc::new(crate::build_facts_read::BuildFactsDoor { me: name.clone(), builds: builds_dyn.clone(), floor: entry_floor.clone() }));
                // The topology this admin enters its mesh from without an authority (its durable map), and
                // where it came from; a launched admin's comes from node.start's topology step, after its hook.
                let mut entry_map: Option<(&'static str, Vec<crate::reenter::MapNode>)> = None;
                let channel = topology_view.clone();
                let seam_journal = journal.clone();
                let joined_seam: crate::workflows::node_start::JoinedSeam<String> = Box::new(
                    move |_: Option<crate::workflows::node_start::JoinThrough>| -> crate::workflows::node_start::BoxFut<Result<(crate::workflows::node_start::HookParts, Option<crate::workflows::node_start::HookStarted<String>>), String>> {
                        Box::pin(async move {
                            // A launched admin's provider policy is its launcher's entry answer's.
                            let policy = match joined {
                                Some(answer) => {
                                    // Fetch the current Build from the launching admin, and record the attempt floor its
                                    // answer names; without them this admin is not started. The Build it names is read from
                                    // the store root, or fetched from a node-admin that holds it (`FetchBuildFacts`,
                                    // node.start's Build-floor step after the hook).
                                    take_entry_control(&answer, &accepted, &entry_floor, &records, &cfg.mesh, &mesh_id).await;
                                    tracing::info_span!("rdm.node_admin.fabric.update.via-join", node = %name, joined = %answer.served_by)
                                        .in_scope(|| tracing::info!("entry pulled; eligible to execute Builds"));
                                    // The cert policy rides the entry pull, as the provider does.
                                    if let Some(p) = answer.control.cert_policy {
                                        issuer.set_policy(p);
                                    }
                                    FabricPolicy { provider: answer.control.provider }
                                }
                                None => {
                                    // Day 0: no Fabric authority exists before this admin. It accepts the first Build, its
                                    // own mesh with one node-admin; it is the current Build once written.
                                    if sole_node_root && inherited.is_some() {
                                        // The re-born fabric-primary with nobody to pull from reads the Build records
                                        // itself (Q227), before any record names it fabric-primary.
                                        seam_journal.read_records_now("inherited_root", &name.to_string()).await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?;
                                        let build_id = builds.current_build().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.into_build_id().map(|b| b.log().to_string()).unwrap_or_default();
                                        tracing::info_span!(parent: &boot, "rdm.node_admin.fabric.update.via-inherited-root", node = %name, build_id = %build_id)
                                            .in_scope(|| tracing::info!("the current Build reloaded from the inherited data dir's builds store"));
                                    } else if cfg.fabric_primary || inherited.is_some() {
                                        tracing::info_span!("rdm.node_admin.fabric.update.via-recovery", node = %name, fabric_id = %cfg.fabric_id, mesh_id = %mesh_id)
                                            .in_scope(|| tracing::info!("a fabric recovery accepts no Build of its own: the current Build is taken from the nodes it reaches"));
                                    } else if matches!(builds.current_build().await, Ok(crate::build_state::CurrentBuild::None)) {
                                        let b0 = crate::build_state::BuildAccepted {
                                            build_id: crate::build::BuildId::mint(),
                                            // Day 0's shape is the bootstrap admin alone; its row records what it runs.
                                            topology: crate::accepted::FabricTopology::root_on(&cfg.fabric, &cfg.mesh, mesh_id.clone(), &cfg.topology_storage_provider),
                                            submitted_change: None,
                                            submitted_at_ms: rafka_time.now_ms(),
                                        };
                                        // The fabric-primary claims the Fabric's identity at Day 0 (node-identity-and-state.md
                                        // §2.8; fabric-create.md).
                                        if let Err(held) = claim_fabric_name(&*fabric_storage, &fabric_identity, &node_id).await? {
                                            return Err(format!("refusing to start: this admin is configured for Fabric {}: {held}", cfg.fabric_id));
                                        }
                                        // At Day 0 the fabric-primary is its mesh's primary too and creates its own mesh's
                                        // row as that mesh's primary (node-identity-and-state.md §2.7): the claim of the
                                        // mesh's name is the mesh's first durable write, so nothing of the mesh (its states
                                        // included) is written before it wins.
                                        match claim_mesh_name(&*mesh_storage, &caches, &name.to_string(), &cfg.mesh, &mesh_id).await {
                                            Ok(Ok(_)) => {}
                                            Ok(Err(refused)) => return Err(format!("refusing to start: this admin's mesh {} (mesh {mesh_id}) cannot claim its name: {refused}", cfg.mesh)),
                                            Err(e) => return Err(format!("refusing to start: {e}")),
                                        }
                                        // The fabric's provider, fixed by Day 0's MESH_SPAWN_TYPE, is the Fabric record's: an
                                        // adoption launch takes it from there (Q156).
                                        let provider = FabricPolicy::bootstrap(cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?.provider;
                                        // So is its topology_storage_provider, Day 0's `RDM_TOPOLOGY_STORAGE_PROVIDER` (storage.md §2).
                                        let topology_storage_provider = cfg.topology_storage_provider.clone();
                                        caches
                                            .update_fabric(&*fabric_storage, &cfg.fabric_id, None, |r| {
                                                r.provider = Some(provider);
                                                r.topology_storage_provider = Some(topology_storage_provider);
                                            })
                                            .await
                                            .map_err(|e| format!("refusing to start: {e}"))?;
                                        attempt_contexts.put(&b0.build_id, 1, &crate::build_claim::current_context()).await.map_err(|e| format!("attempt-context: {e}"))?;
                                        crate::build_state::accept(&*builds, &b0, None, &name.to_string(), "day-0").await.map_err(|e| e.to_string())?;
                                    }
                                    FabricPolicy::bootstrap(cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?
                                }
                            };

                            if inherited.is_some() && cfg.launch.is_none() {
                                // An admin started on an inherited root has no maker: its durable map is the topology.
                                let mut rows = super::rows_writer::recorded_births(&*mesh_storage, &*nodes_storage, &cfg.mesh).await.map_err(storage_err)?;
                                rows.retain(|r| r.node_id != node_id);
                                if let Ok(map) = crate::reenter::get_topology(crate::reenter::TopologySource::DurableMap(&rows)).await {
                                    entry_map = Some(("durable-map", map));
                                }
                            }
                            let entering_existing_mesh = entry_map.as_ref().is_some_and(|(_, m)| m.iter().any(|n| n.name.mesh == cfg.mesh && n.node_id != node_id));
                            // Whether this admin enters an existing mesh: from its durable map here, or from the maker's
                            // topology at node.start's topology step. The Ready check waits for that entry's sweep.
                            let entering = Arc::new(std::sync::atomic::AtomicBool::new(entering_existing_mesh));

                            // A restart that enters its mesh from its durable map elects nobody until its entry read has
                            // told it who holds the seats.
                            records.set_entering(entering_existing_mesh && cfg.launch.is_none());
                            let _ = records.contacts.set(nodes_storage.clone());
                            // A fabric primary recovered from its data dir holds its durable topology as its map (rule 5):
                            // every birth the accepted Build names that nodes.storage heard is in its view as not yet
                            // reached, until the topology view or the sweep says more. Its view is never an empty one that plans
                            // a create for every member.
                            if inherited.is_some() && cfg.launch.is_none() {
                                let desired = builds_dyn.current().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.map(|b| b.topology);
                                for m in super::rows_writer::recorded_births(&*mesh_storage, &*nodes_storage, &cfg.mesh).await.map_err(storage_err)? {
                                    if m.node_id != node_id && desired.as_ref().is_some_and(|t| t.contains(&m.name)) {
                                        if let Some(res) = m.resolved() {
                                            node_rpc_resolver.apply(res, None);
                                        }
                                        records.publish(m.as_node(NodeStatus::PendingReconnect, policy.provider));
                                    }
                                }
                            }
                            // A start with no launcher, the re-born fabric-primary (Q227, the one reader of the Build
                            // records), holds the ids its store holds for the meshes the current Build names. A launched
                            // admin reads neither (Q227: "only FP reads the builds"): the meshes' ids come in its
                            // hydration sync, node.topology.get at node.start's topology step (`AdminFirstStart`).
                            if cfg.launch.is_none() {
                                if let Some(named) = builds_dyn.current().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.map(|b| b.topology) {
                                    for m in mesh_storage.meshes().await.map_err(storage_err)?.into_iter().filter(|m| !m.dead && named.meshes.contains_key(&m.name)) {
                                        records.meshes.lock().unwrap().insert(m.name, m.mesh_id);
                                        records.inputs_moved();
                                    }
                                }
                            }
                                records
                                    .meshes
                                    .lock()
                                    .unwrap()
                                    .insert(cfg.mesh.clone(), mesh_id.clone());
                                let digest_view = topology_view.digest_view.clone();
                                let node_rpc_feed = node_rpc.feed(&digest_view, &name.to_string());
                                let control = Arc::new(
                                    ControlPlane::new(
                                        builds_dyn.clone(),
                                        accepted.clone(),
                                        name.clone(),
                                        project(
                                            &cfg.fabric,
                                            &cfg.fabric_id,
                                            policy.provider,
                                            &digest_view,
                                            &records,
                                        ),
                                        rafka_time.clone(),
                                    )
                                    .with_contexts(attempt_contexts.clone())
                                    .with_claims(mesh_claims.clone())
                                    .with_executable_dirs(cfg.executable_dirs.clone())
                                    .with_topology_storage_providers(providers.names()),
                                );
                                let _ = control.storage_changes.set(storage_changes.clone());
                                // The claim door: this admin decides a fabric-lane claim only while it holds the fabric-primary
                                // seat, and its own mesh's drift-lane claim only while it holds its mesh's primary seat (R-BI7).
                                let claim_door = Arc::new(crate::build_claim::ClaimDoor {
                                    me: name.clone(),
                                    node_id: node_id.clone(),
                                    topology: control.topology.clone(),
                                    builds: builds_dyn.clone(),
                                    contexts: attempt_contexts.clone(),
                                    claims: mesh_claims.clone(),
                                    claimed: Default::default(),
                                });
                                let _ = claim_slot.set(claim_door.clone());
                                {
                                    let (digest_view, records) = (digest_view.clone(), records.clone());
                                    let _ =
                                        control.absence.set(Arc::new(move |name: &PathName| {
                                            describe_absence(&digest_view, &records, name)
                                        }));
                                }
                                let _ = view_slot.set(control.topology.clone());
                                let _ = control.fabric_recovery.set(
                                    crate::fabric_recovery::reader(digest_view.clone(), records.clone()),
                                );
                                let _ = control.seats.set(digest_view.seats.clone());
                                {
                                    let (me, digest_view, records, fabric, fabric_id, provider) = (
                                        name.clone(),
                                        digest_view.clone(),
                                        records.clone(),
                                        cfg.fabric.clone(),
                                        cfg.fabric_id.clone(),
                                        policy.provider,
                                    );
                                    let _ = control.view_now.set(Arc::new(move || {
                                        let (me, digest_view, records, fabric, fabric_id) = (
                                            me.clone(),
                                            digest_view.clone(),
                                            records.clone(),
                                            fabric.clone(),
                                            fabric_id.clone(),
                                        );
                                        Box::pin(async move {
                                            let reads =
                                                ViewReads::read(&me, &digest_view, &records, &fabric_id)
                                                    .await;
                                            project_with(
                                                &fabric, &fabric_id, provider, &digest_view, &records,
                                                &reads,
                                            )
                                        })
                                    }));
                                }
                                let _ = control.connections.set(connections.held());
                                {
                                    let digest_view = digest_view.clone();
                                    let _ = control.loads.set(Arc::new(move || {
                                        digest_view.current(digest_view.staleness_floor())
                                            .into_iter()
                                            .filter(|d| d.load.is_some() || d.gossip.is_some())
                                            .map(|d| {
                                                (d.node.node_id.to_string(), (d.load, d.gossip))
                                            })
                                            .collect()
                                    }));
                                }
                                // Where this admin's own `pre_node_ready` stands: written by its start (and a rejoin's),
                                // read by the Ready check and by a probe naming this admin.
                                let pre_node_ready_cell: crate::status_rpc::PreNodeReadyCell =
                                    Arc::default();
                                // This admin's re-publish of its presence, for a probe naming it (the checklist's down op): filled once its
                                // digest exists, below.
                                let republish: crate::status_rpc::Republish =
                                    Arc::new(std::sync::OnceLock::new());
                                // This admin holding the config it is told (`ApplyNodeConfig`): filled with the republish, below.
                                let configure: crate::status_rpc::Configure =
                                    Arc::new(std::sync::OnceLock::new());
                                // This admin taking the name its row gives it (`ApplyNodeName`): filled with the republish, below.
                                let rename: crate::status_rpc::Rename =
                                    Arc::new(std::sync::OnceLock::new());
                                let commands = Arc::new(crate::node_commands::CommandView::new(
                                    cfg.connections_delete_deadline,
                                ));
                                let own_commands: Arc<
                                    std::sync::OnceLock<Arc<crate::node_self::NodeSelf>>,
                                > = Arc::new(std::sync::OnceLock::new());
                                let mesh_deleter: crate::mesh_delete::MeshDeleterSlot =
                                    Arc::new(std::sync::OnceLock::new());
                                let states_slot: Arc<
                                    std::sync::OnceLock<Arc<crate::fabric_status::FabricStates>>,
                                > = Arc::new(std::sync::OnceLock::new());
                                let configs_slot: crate::node_config::NodeConfigsSlot =
                                    Arc::new(std::sync::OnceLock::new());
                                // The record the writer of this admin's mesh rows holds, shared with its config door: a config
                                // put through the door is what the node's next row carries (`crate::node_rows::WrittenRows`).
                                let written = records.caches.nodes.clone();
                                // Every record the writer holds is this admin's atlas's too (Q203).
                                written.feeds(control.atlas.clone());
                                control.atlas.writes(&own_mesh);
                                let configs = crate::node_config::NodeConfigs::new(node_rpc.client.clone(), configure.clone(), written.clone());
                                // A command it sends down that a mesh-primary not yet holding the primary refused waits on the view's next change (D12).
                                configs.watch_views(records.subscribe_view());
                                // An owed push of a node's row up to the fabric-primary reads the view again on each send (Q235 P3).
                                configs.reads(control.topology.clone());
                                let _ = configs_slot.set(Arc::new(configs));
                                {
                                    let records_for_ids = records.clone();
                                    let _ = status_door.set(Arc::new(crate::status_rpc::StatusDoor {
                                    app_channels: Some(topology_view.clone()),
                                    declaration_seam: wiring.declaration_seam.clone(),
                                    command_frame_seam: wiring.command_frame_seam.clone(),
                                    operation_effects: Default::default(),
                                    stopped: Default::default(),
                                    meshes_storage: mesh_storage.clone(),
                                    fabric_storage: fabric_storage.clone(),
                                    nodes_storage: nodes_storage.clone(),
                                    caches: records.caches.clone(),
                                    me: name.clone(),
                                    node_id: node_id.clone(),
                                    fabric_id: cfg.fabric_id.clone(),
                                    topology: control.topology.clone(),
                                    declared: records.declared.clone(),
                                    mesh_ids: Arc::new(move || records_for_ids.meshes.lock().unwrap().clone()),
                                    durable: records.durable.clone(),
                                    commands: commands.clone(),
                                    own: own_commands.clone(),
                                    certs: Some(issuer.clone()),
                                    mesh_deleter: mesh_deleter.clone(),
                                    hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                                    wake: records.wake.clone(),
                                    fabric_states: states_slot.clone(), atlas: Arc::new(std::sync::OnceLock::from(control.atlas.clone())), fabric_recovery: Arc::new(std::sync::OnceLock::from(crate::fabric_recovery::reader(digest_view.clone(), records.clone()))),
                                    configs: configs_slot.clone(),
                                    republish: republish.clone(),
                                    configure: configure.clone(),
                                    pre_node_ready: pre_node_ready_cell.clone(),
                                    rename: rename.clone(),
                                    row_moved: {
                                        // The view is projected at once: it reads the moved row, as every admin's next
                                        // projection does (`ViewReads`), so it holds nothing of the move itself.
                                        let moved: crate::status_rpc::RowMoved = Arc::new(std::sync::OnceLock::new());
                                        let project = view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone(), records.clone());
                                        let _ = moved.set(Arc::new(move |_from: PathName, _node_id: NodeId, _incarnation: IncarnationId, _to: PathName| project()));
                                        moved
                                    },
                                    loads: control.loads.get().cloned(),
                                    digest_view: Some(digest_view.clone()),
                                    builds: control.builds.clone(),
                                }));
                                    status_door_set.notify_waiters();
                            }
                            // The Node RPC door: opened by the first view installed that holds this admin's exact birth
                            // once the authority above is filled, whichever task installs it.
                            {
                                let status_door = status_door.clone();
                                let door = ServingDoor::new(rpc_ready.clone(), (node_id.clone(), incarnation.clone()), Box::new(move || status_door.get().is_some()), name.clone());
                                records.set_door(door, &*control.topology.read().await);
                            }
                            // The backbone holds the meshes each installed view names: the fabric-primary's
                            // publication of a deleted mesh reads them (mesh-delete.md step 7).
                            records.set_backbone(backbone.clone(), &*control.topology.read().await);
                            // What this admin owes its authorities.
                            let declarer = crate::status_declare::Declarer::new();
                            declarer.fabric_recovery.set(crate::fabric_recovery::reader(digest_view.clone(), records.clone()));
                            // As its mesh's primary, this admin records the Mesh state the fabric-primary
                            // answered on its own Mesh record (Q144).
                            let _ = declarer.record_mesh.set(crate::status_declare::RecordMesh({
                                let (meshes, caches, mesh, fabric_id, on_mesh_ready) = (mesh_storage.clone(), records.caches.clone(), cfg.mesh.clone(), cfg.fabric_id.clone(), fabric_hooks.on_mesh_ready.clone());
                                Arc::new(move |mesh_id: MeshId, state: rdm_core::status::MeshState| {
                                    let (meshes, caches, mesh, fabric_id, on_mesh_ready) = (meshes.clone(), caches.clone(), mesh.clone(), fabric_id.clone(), on_mesh_ready.clone());
                                    Box::pin(async move {
                                        let base = crate::storage::MeshRecord::new(mesh_id.clone(), mesh.clone());
                                        // Whether this write is the one that put the state on the record: the mesh's entry to it.
                                        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
                                        let written = caches.update_mesh(&*meshes, &mesh_id, Some(base), |r| { entered.store(r.states.insert(state), std::sync::atomic::Ordering::SeqCst); }).await;
                                        let outcome = if written.is_ok() { "recorded" } else { "record-refused" };
                                        tracing::info_span!("rdm.node_admin.mesh.update.via-applied", mesh = %mesh, mesh_id = %mesh_id, state = ?state, outcome, error = %written.as_ref().err().cloned().unwrap_or_default(), "otel.kind" = "internal")
                                            .in_scope(|| tracing::info!("the fabric-primary answered the mesh's state: its primary records it on the mesh's own record"));
                                        if written.is_ok() && state == rdm_core::status::MeshState::Ready && entered.load(std::sync::atomic::Ordering::SeqCst) {
                                            crate::fabric_status::notify_mesh_ready(on_mesh_ready, crate::wiring::MeshReadyCtx { fabric_id, mesh_id: mesh_id.clone(), mesh: mesh.clone() });
                                        }
                                        written.map(|_| ())
                                    })
                                })
                            }));
                            if let Some(maker) = cfg.launch.as_ref().and_then(|l| l.launcher.as_ref()) {
                                let _ = declarer.maker.set(maker.node_id.clone());
                            }
                            let _ = states_slot.set(crate::fabric_status::FabricStates::new(crate::fabric_status::StatesEnv {
                                me: name.clone(),
                                node_id: node_id.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                topology: control.topology.clone(),
                                client: node_rpc.client.clone(),
                                builds: builds_dyn.clone(),
                                contexts: attempt_contexts.clone(),
                                records: records.clone(),
                                hooks: fabric_hooks,
                            }));
                            // A Build this admin accepts as the fabric-primary reaches its change inside the acceptance:
                            // a resize puts the fabric back to pending, and the view the door's caller reads next holds it.
                            {
                                let (fabric_states, me, digest_view, records, fabric, fabric_id, provider, topology) = (states_slot.get().expect("set above").clone(), name.clone(), digest_view.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, control.topology.clone());
                                let _ = control.resize.set(Arc::new(move || {
                                    let (fabric_states, me, digest_view, records, fabric, fabric_id, topology) = (fabric_states.clone(), me.clone(), digest_view.clone(), records.clone(), fabric.clone(), fabric_id.clone(), topology.clone());
                                    Box::pin(async move {
                                        if fabric_states.accepted().await {
                                            records.install_read(&me, &topology, &digest_view, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &digest_view, &records, reads)).await;
                                        }
                                    })
                                }));
                            }

                            // The deployment hand (used while this admin is fabric primary). A provider's host-wide
                            // resources (a container fabric's network and labels) are keyed by the Fabric's id: a Fabric
                            // name is not unique on a host.
                            let (prepared, provider_dyn) = match early {
                                Some((p, dyn_p)) => (p, dyn_p),
                                None => {
                                    let p = crate::deployment::prepare(policy, &cfg.fabric_id.to_string()).await.map_err(|e| e.to_string())?;
                                    let dyn_p: Arc<dyn crate::deployment::provider::DeploymentProvider> = crate::wiring::apply(wiring.provider.take(), p.provider.clone());
                                    (p, dyn_p)
                                }
                            };
                            let mut admin_env = BTreeMap::new();
                            admin_env.insert("RDM_BIN_DIR".to_string(), cfg.bin_dir.display().to_string());
                            // Every node-admin this admin launches mounts the directories its own launch named
                            // (proposal executable-bindings-at-create.md §2), as it is written the root.
                            if !cfg.executable_dirs.is_empty() {
                                admin_env.insert(rdm_core::launch::ENV_EXECUTABLE_DIRS.to_string(), cfg.executable_dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(","));
                            }
                            let data_root = cfg.data_dir.parent().map(Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone());
                            // Every node this admin launches is written the topology storage provider it writes itself
                            // (storage.md §2 "every launch").
                            let mut env = cfg.passthrough.clone();
                            env.insert(rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER.to_string(), cfg.topology_storage_provider.clone());
                            let template = LaunchTemplate { fabric: cfg.fabric.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                executable: PathBuf::new(),
                                // The address this admin's mesh endpoint is bound on: where every birth it launches reaches
                                // it (loopback for a process fabric, the gateway or its own container address for a
                                // container fabric).
                                seeds: vec![(key.public().to_string(), mesh_addr)],
                                launcher: rdm_core::launch::Launcher { name: name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() },
                                env,
                                data_root,
                                store_dir: cfg.store_dir.clone(),
                                issuing: None,
                                roots: data_connections.roots(),
                                connections_delete_deadline: cfg.connections_delete_deadline,
                                declared: declared_secrets,
                                binding: None,
                            };
                            // RDM's own transition pipeline: no application hook is registered on it; the
                            // application's work enters through the Fabric and node hooks (build.md §9, Q177).
                            let hooks = HookRegistry::new().build().map_err(|e| format!("{e:?}"))?;
                            let handover = Arc::new(crate::fabric_handover::HandoverDoor {
                                fabric: cfg.fabric.clone(),
                                me: name.clone(),
                                node_id: node_id.clone(),
                                incarnation: incarnation.clone(),
                                topology: control.topology.clone(),
                                topology_view: topology_view.clone(),
                                backbone: backbone.clone(),
                                client: node_rpc.client.clone(),
                                digest_view: digest_view.clone(),
                                records: records.clone(),
                                locks: Default::default(),
                                memo: Default::default(),
                                decisions: builds.decisions(),
                                project_view: view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone(), records.clone()),
                                finish_writes: finish_writes.clone(),
                                looker: crate::seat_watch::Looker { me: name.clone(), me_id: node_id.clone(), topology_view: topology_view.clone(), client: node_rpc.client.clone(), provider: provider_dyn.clone(), nodes: Some(nodes_storage.clone()), heard_again: heard_again_slot.clone(), connections: Some(connections_storage.clone()), commands: Some(commands.clone()) },
                            });
                            let _ = handover_slot.set(handover.clone());
                            // The drives of the Builds this admin drives (`build_drive`): its executor ends its own
                            // runtime only once its own node.delete's callers read their terminal.
                            let drives = Arc::new(crate::build_drive::Drives::default());
                            let runner = Arc::new(AdminRunner {
                                storage_changes: storage_changes.clone(),
                                drives: drives.clone(),
                                listeners: listener_names.clone(),
                                mesh_deleter: mesh_deleter.clone(),
                                connections: Some(connections_storage.clone()),
                                nodes_storage: nodes_storage.clone(),
                                mesh_storage: mesh_storage.clone(),
                                fabric_storage: fabric_storage.clone(),
                                provider: provider_dyn.clone(),
                                joins: joins.clone(),
                                observer: Arc::new(TopologyObserver { digest_view: digest_view.clone(), client: Some(node_rpc.client.clone()), commands: commands.clone(), status_door: status_door.clone(), handover: handover_slot.clone(), unreachable: records.unreachable.clone() }),
                                records: records.clone(),
                                builds: builds_dyn.clone(),
                                local: journal.clone(),
                                template,
                                admin_env,
                                lifecycle: LifecycleTransitionPipeline::new(hooks, Arc::new(MemoryReceiptLog::default())),
                                handles: Mutex::new(HashMap::new()),
                                topology: control.topology.clone(),
                                fabric: cfg.fabric.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                fabric_provider: policy.provider,
                                digest_view: digest_view.clone(),
                                me: name.clone(),
                                node_id: node_id.clone(),
                                endpoint: Some(endpoint.clone()),
                                node_rpc: node_rpc.clone(),
                                lifecycle_events: crate::wiring::apply(wiring.lifecycle_events.take(), {
                                    let project = Some(view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone(), records.clone()));
                                    if let Some(status) = status_door.get() {
                                        let _ = status.stopped.set(super::runner::stopped_publisher(topology_view.clone(), backbone.clone(), project.clone()));
                                    }
                                    Arc::new(GossipLifecycle { topology_view: topology_view.clone(), backbone: backbone.clone(), me: name.clone(), mesh_deleter: mesh_deleter.clone(), project, status_door: status_door.clone() })
                                }),
                                leaves: Arc::new(crate::mesh_delete::MeshDeleteView::default()),
                                mesh_delete_runs: crate::mesh_delete::MeshDeleteRuns::default(),
                                handover: handover.clone(),
                                issuer: issuer.clone(),
                            });
                            let _ = runner_slot.set(runner.clone());
                            let _ = heard_again_slot.set(Arc::new(crate::heard_again::HeardAgain { client: node_rpc.client.clone(), status_door: status_door.clone(), builds: builds_dyn.clone(), runner: runner_slot.clone(), digest_view: topology_view.digest_view.clone() }));
                            // What the provider holds recorded at a path: the proof of a node the accepted topology names
                            // that no view and no durable runtime row holds a birth of (mesh-investigation.md step 5).
                            records.set_path_probe(runner.clone());
                            // Gate 5's verdict on a delete of a birth neither stopped nor heard live (Q219).
                            let _ = control.investigation.set(runner.clone());
                            let _ = mesh_deleter.set(Arc::new(MeshDeleteDoor { runner: runner.clone() }));
                            // The Build family on `0x20`: the executor's door for `build.attempt.run` and the fabric-primary's
                            // drive of every Build. The same admin holds both; which one a call reaches depends on the seat.
                            let drive_env = {
                                let exec = Arc::new(BuildExecutor { executor: name.to_string(), builds: builds_dyn.clone(), topology: control.topology.clone(), runner: runner.clone() });
                                let run_door = Arc::new(crate::build_run::RunDoor {
                                    me: name.clone(),
                                    builds: builds_dyn.clone(),
                                    exec,
                                    runs: runs.clone(),
                                    local: journal.clone(),
                                    gate: stop_gate.clone(),
                                });
                                let refresh_view = view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone(), records.clone());
                                let env = Arc::new(crate::build_drive::DriveEnv {
                                    me: name.clone(),
                                    topology: control.topology.clone(),
                                    builds: builds_dyn.clone(),
                                    door: claim_door.clone(),
                                    dispatcher: Arc::new(crate::build_op::RpcDispatcher { me: name.clone(), local: run_door.clone(), client: node_rpc.client.clone(), topology: control.topology.clone() }),
                                    gate: Arc::new(SeatGate { builds: builds.clone(), control: control.clone(), handover: handover.clone(), project: refresh_view.clone(), meshes: mesh_storage.clone() }),
                                    departure: Arc::new(ProviderDeparture {
                                        me: name.clone(),
                                        topology: control.topology.clone(),
                                        refresh: refresh_view,
                                        digest_view: digest_view.clone(), provider: runner.provider.clone(), nodes: nodes_storage.clone(), records: records.clone() }),
                                    verdicts: Arc::new(crate::build_op::LocalVerdicts { local: journal.clone() }),
                                    notices: build_notices.clone(),
                                    turns: stop_gate.clone(),
                                });
                                let _ = control.departure.set(env.departure.clone());
                                let _ = control.node_rows.set(nodes_storage.clone());
                                let _ = control.node_id.set(node_id.clone());
                                records.set_rows(Arc::new(crate::node_rows::NodeRows::new(name.clone(), node_id.clone(), control.topology.clone(), nodes_storage.clone()).with_fabric_recovery(crate::fabric_recovery::reader(digest_view.clone(), records.clone())).with_configs(configs_slot.clone()).with_written(written.clone())));
                                let _ = build_slot.set(Arc::new(crate::build_op::BuildDoor { me: name.clone(), control: control.clone(), drives: drives.clone(), env: env.clone(), run: run_door, seats: topology_view.primaries().clone(), incarnation: incarnation.clone() }));
                                env
                            };

                            // This birth's exact runtime. A launched admin takes the record its
                            // launcher's pipeline made available; the bootstrap admin, which nobody
                            // launched, adopts its own process (Day 0, one receipt per step). Either
                            // way it is published with the birth.
                            let mut adoption = None;
                            let runtime = match launched_runtime {
                                Some(r) => r,
                                None => {
                                    // In a container of this fabric's container runtime, its runtime is that container;
                                    // a host process stays a process (it then cannot be failed over by a container admin,
                                    // which refuses Ready by name).
                                    let in_container = (prepared.provider.kind() == ProviderKind::Container && rdm_core::runtime::this_container_id().is_some()).then(|| prepared.provider.control_domain());
                                    let a = match adopted_at_claim.take() {
                                        Some(a) => a,
                                        None => CurrentRuntimeAdoption::begin_in(&name, &cfg.data_dir, &crate::model::DeploymentId::mint(), in_container).await.map_err(|e| e.to_string())?,
                                    };
                                    let f = a.fact().clone();
                                    adoption = Some(a);
                                    f
                                }
                            };
                            // The identity this process records its intentional exit under (a transport that stopped).
                            rdm_core::runtime::set_own_exit(rdm_core::runtime::OwnExit {
                                data_dir: cfg.data_dir.clone(),
                                deployment_id: runtime.deployment_id.clone(),
                                incarnation: incarnation.to_string(),
                            });
                            // This birth's exact runtime, a blind put of its own key (node-recovery.md "Detection and
                            // proof" step 3: "the exact runtime recorded at launch"; build.md §5.3: "the launching admin a
                            // birth's runtime row"): a launched admin's is its launcher's, kept at its join; an admin no
                            // launcher recorded (Day 0, a start on its own data dir) keeps its own, once (an adoption
                            // launch kept it at its claim). An admin started on this data dir after this birth exits
                            // proves this birth's exit from it, and a later adoption launch reads it from the fabric's
                            // store root once no live node holds its digest (deployment-provider.md §3).
                            if cfg.launch.is_none() && !own_runtime_kept {
                                let own_runtime_row = crate::storage::RuntimeRow { node_id: node_id.clone(), name: name.clone(), incarnation_id: incarnation.clone(), runtime: runtime.clone(), data_dir: Some(cfg.data_dir.display().to_string()) };
                                nodes_storage.put_runtime(&own_runtime_row).await.map_err(storage_err)?;
                            }
                            // Own digest, published on the topology cadence.
                            let digest = Arc::new(Mutex::new(MeshDigest { held_hash: topology_view.held_hash(),
                                fabric_id: cfg.fabric_id.clone(),
                                node: MeshNode {
                                    node_id: node_id.clone(),
                                    name: handed.as_ref().and_then(|(n, _)| n.clone()).unwrap_or_else(|| name.clone()),
                                    endpoint_id: EndpointId(key.public().to_string()),
                                    // The mesh endpoint as bound (a rebind may have moved it).
                                    transport_addr: mesh_addr,
                                    incarnation: incarnation.clone(),
                                    supersedes,
                                    // Day 0: PublishRuntimeFactAndCurrentRuntimeMetadata sets them.
                                    runtime: adoption.is_none().then(|| runtime.clone()),
                                },
                                // Pending until it can take authority at once (the Ready gate below).
                                status: MemberStatus::Pending,
                                node_state: Default::default(),
                                admin_api_base: Some(api_base.clone()),
                                // Stamped by `TopologyView::publish`: this birth's own sequence and the composed clock.
                                emitted_at_rafka_ms: 0,
                                digest_seq: 0,
                                mesh_id: Some(mesh_id.clone()),
                                in_flight: None,
                                load: None,
                                gossip: None,
                                data_dir: adoption.is_none().then(|| cfg.data_dir.display().to_string()), is_primary: None, is_fabric_primary: None
                            }));
                            // The own row in the data dir: this birth and its Mesh, so a restart on this data dir is the
                            // same logical node in the same Mesh.
                            {
                                let d = digest.lock().unwrap().clone();
                                topology_storage
                                    .put_own_row(crate::storage::NodeRecord {
                                        node_id: node_id.clone(),
                                        fabric_id: cfg.fabric_id.clone(),
                                        name: name.clone(),
                                        mesh_id: mesh_id.clone(),
                                        incarnation_id: incarnation.clone(),
                                        endpoint_id: d.node.endpoint_id.clone(),
                                        transport_addr: d.node.transport_addr,
                                        listeners: Vec::new(),
                                        is_primary: None,
                                        is_fabric_primary: None,
                                        // The provider the handle writes now, set by the handle.
                                        topology_storage_provider: None,
                                    })
                                    .await.map_err(storage_err)?;
                            }
                            // The members' addresses and primaries on their Node records, and the Mesh row, on the
                            // fabric's shared root: this admin's mesh's while it is its mesh's mesh-primary, and a mesh
                            // with no primary's while it is the fabric-primary (the writer ladder; `MeshRowsWriter`), as
                            // bootstrap hints for a later restart of any node of the fabric. A birth is written the moment
                            // the digest view first holds it or it changes (the view's own birth watch, as the runtime rows are),
                            // and every member at once when the writing comes to this admin (the view install's watch of
                            // the meshes it writes): a hint that waits on a timer is lost to a restart that comes first. A
                            // deleted node's record is `Deleted` and names no node to reach. Nothing here is topology.
                            let (writes_tx, writes) = tokio::sync::watch::channel(std::collections::BTreeSet::<String>::new());
                            let contacts_task = {
                                let (digest_view, fabric_id) = (topology_view.digest_view.clone(), cfg.fabric_id.clone());
                                let writer = MeshRowsWriter::new(&name.to_string(), &cfg.mesh, mesh_id.clone(), nodes_storage.clone(), mesh_storage.clone(), records.caches.clone());
                                let stop = stop_gate.clone();
                                tokio::spawn(super::rows_writer::keep_mesh_rows(writer, digest_view, fabric_id, writes, stop, commands.clone()))
                            };
                            if let Some(a) = adoption {
                                let digest = digest.clone();
                                a.publish(move |f, dir| {
                                    let mut d = digest.lock().unwrap();
                                    d.node.runtime = Some(f.clone());
                                    d.data_dir = Some(dir);
                                })
                                .await
                                .map_err(|e| e.to_string())?;
                            }
                            let _ = entry.set(EntryState {
                                name: name.clone(),
                                provider: policy.provider,
                                accepted: accepted.clone(),
                                builds: builds_dyn.clone(),
                                topology_view: topology_view.clone(),
                                time: rafka_time.clone(),
                                issuer: issuer.clone(),
                            });
                            // The join door: this admin verifies a node's digest against what it deployed, installs the
                            // address for its key and answers what it holds.
                            {
                                let (st, resolver, m, topo) = (entry.clone(), node_rpc_resolver.clone(), topology_view.clone(), topology_view.clone());
                                let door = Arc::new(crate::join::JoinDoor {
                                    me: name.clone(),
                                    joins: joins.clone(),
                                    answer: Arc::new(move || {
                                        let st = st.clone();
                                        Box::pin(async move {
                                            match st.get() {
                                                Some(e) => e.answer().await,
                                                None => Err("this admin holds no view yet".to_string()),
                                            }
                                        })
                                    }),
                                    types: Arc::new({
                                        let (topology_view, records) = (topology_view.clone(), records.clone());
                                        move |d: MeshDigest, types: std::collections::BTreeSet<String>| {
                                            let (topology_view, records) = (topology_view.clone(), records.clone());
                                            Box::pin(async move {
                                                records.declare_app_types_or_hold(&d, types.clone()).await?;
                                                // The joiner's types are held before the join is answered; the joiner is handed to no
                                                // channel as a peer: it dials this admin, its first neighbour (gossip.md §8, Q84).
                                                topology_view.hold_app_types_from(&d, types.clone(), types).await.map_err(|e| e.to_string())
                                            })
                                        }
                                    }),
                                    install: Arc::new(move |d: &MeshDigest| {
                                        // The same effect hearing the digest on gossip has: where the key is, and the
                                        // live resolver's birth (which cancels every dial aimed at the key's old address).
                                        m.learn(d.clone(), "join");
                                        if let Ok(endpoint_id) = d.node.endpoint_id.0.parse::<iroh::PublicKey>() {
                                            let applied = resolver.apply(
                                                rdm_transport::ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id, transport_addr: d.node.transport_addr, incarnation: d.node.incarnation.clone() },
                                                d.node.supersedes.as_ref(),
                                            );
                                            tracing::info!(node = %d.node.name, applied = ?applied, "the resolver holds the joined birth");
                                        }
                                    }),
                                    known: view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone(), records.clone()),
                                    // The accepted join report's one Pending per join, recorded and published by this
                                    // admin's status authority before the join is answered (node-mesh-join.md step 10);
                                    // a retry the door tells apart applies as already applied (Q99).
                                    pending: Arc::new({
                                        let status_door = status_door.clone();
                                        move |d: MeshDigest, new_join: bool| {
                                            let status_door = status_door.get().cloned();
                                            Box::pin(async move {
                                                if let Some(status_door) = status_door {
                                                    status_door.joined_pending(&d.node.node_id, &d.node.name, &d.node.incarnation, new_join).await;
                                                }
                                            })
                                        }
                                    }),
                                    primary: Arc::new(move || topo.mesh_primary().ok().map(|p| p.node.name.to_string())),
                                    issuer: issuer.clone(),
                                    nodes: Some((nodes_storage.clone(), records.caches.nodes.clone())),
                                    // A joiner's row is read from this admin's atlas, never the store (Q227, Q234).
                                    atlas: Some(control.atlas.clone()),
                                });
                                let _ = join_slot.set(door);
                                // Every node serves the topology it holds (`GetTopology`, op `0x1E`).
                                let own = digest.clone();
                                // For a mesh it holds no snapshot of, this admin answers from the records its atlas holds
                                // (the meshes it is primary for) and the births its view holds now, never from the store
                                // (Q227): a read is never refused because the store is mid-write.
                                let (atlas, held_digest_view, held_fabric, held_me, held_ids) = (control.atlas.clone(), digest_view.clone(), cfg.fabric_id.clone(), node_id.clone(), records.clone());
                                let stored: crate::topology_read::StoredSource = Arc::new(move || {
                                    let (atlas, held_digest_view, held_fabric, held_me, held_ids) = (atlas.clone(), held_digest_view.clone(), held_fabric.clone(), held_me.clone(), held_ids.clone());
                                    Box::pin(async move {
                                        // The id this admin holds for each mesh it knows, a deleted mesh's none.
                                        let mut ids = held_ids.meshes.lock().unwrap().clone();
                                        let deleted = held_ids.deleted_meshes.lock().unwrap().clone();
                                        ids.retain(|_, id| !deleted.contains(id));
                                        let mut nodes: BTreeMap<NodeId, rdm_core::topology::StoredNode> = atlas
                                            .records()
                                            .iter()
                                            .filter(|r| r.status != NodeStatus::Deleted)
                                            .filter_map(crate::reenter::MapNode::of_record)
                                            .map(|r| (r.node_id.clone(), rdm_core::topology::StoredNode { node_id: r.node_id, name: r.name.to_string(), endpoint_id: r.endpoint_id, incarnation: r.incarnation, transport_addr: r.transport_addr }))
                                            .collect();
                                        for d in held_births(&held_digest_view, &held_fabric, &held_me) {
                                            nodes.insert(
                                                d.node.node_id.clone(),
                                                rdm_core::topology::StoredNode { node_id: d.node.node_id, name: d.node.name.to_string(), endpoint_id: d.node.endpoint_id, incarnation: d.node.incarnation, transport_addr: d.node.transport_addr },
                                            );
                                        }
                                        Ok(crate::topology_read::StoredMap { nodes: nodes.into_values().collect(), mesh_ids: ids })
                                    })
                                });
                                let gone: crate::topology_read::GoneSource = {
                                    let (digest_view, records) = (digest_view.clone(), records.clone());
                                    Arc::new(move || incumbency_of(&digest_view, &records).lost)
                                };
                                // The hydration sync's own row (Q234): a node of this admin's mesh reading its mesh-primary
                                // is answered its whole row from this admin's atlas, never the store; any other
                                // caller, or this admin while it is not its mesh's primary, is answered none.
                                let own_row: crate::topology_read::OwnRowSource = {
                                    let (atlas, view, mesh, me) = (control.atlas.clone(), control.topology.clone(), cfg.mesh.clone(), node_id.clone());
                                    Arc::new(move |caller: String| {
                                        let (atlas, view, mesh, me) = (atlas.clone(), view.clone(), mesh.clone(), me.clone());
                                        Box::pin(async move {
                                            let view = view.read().await;
                                            if !view.is_mesh_primary(&mesh, &me) {
                                                return None;
                                            }
                                            let held = view.births().find(|n| n.endpoint_id.as_ref().is_some_and(|e| e.0 == caller)).and_then(|n| atlas.record(&n.node_id));
                                            let row = held.or_else(|| atlas.record_of_endpoint(&caller)).filter(|r| r.name.mesh == mesh && r.node_id != me)?;
                                            Some((crate::views::own_row_view(&view, &atlas, &row), row.revision))
                                        })
                                    })
                                };
                                let own_topology_view = topology_view.clone();
                                let _ = topology_slot.set(Arc::new(crate::topology_read::TopologyDoor::new(topology_view.clone(), Arc::new(move || own_topology_view.reported(&own.lock().unwrap())), rafka_time.clone()).with_stored(stored).with_gone(gone).with_own_row(own_row)));
                            }
                            // A birth the view shows superseded (a later incarnation of its node held, or its node
                            // deleted): the declared-state rows this admin put for it as an authority are tombstoned.
                            let superseded_rows_task = {
                                let (digest_view, status_door) = (topology_view.digest_view.clone(), status_door.clone());
                                let stop = stop_gate.clone();
                                tokio::spawn(async move {
                                    let mut births = digest_view.birth_changes();
                                    loop {
                                        // Held here while the admin is stopped: its own wait, outside every lock.
                                        let turn = stop.turn("superseded-rows").await;
                                        births.borrow_and_update();
                                        if let Some(a) = status_door.get() {
                                            a.drop_superseded(&digest_view).await;
                                        }
                                        drop(turn);
                                        if births.changed().await.is_err() {
                                            return;
                                        }
                                    }
                                })
                            };
                            let app_channels_task = {
                                let (topology_view, client, rows, me) = (topology_view.clone(), node_rpc.client.clone(), nodes_storage.clone(), node_id.clone());
                                let stop = stop_gate.clone();
                                tokio::spawn(async move {
                                    let mut changes = topology_view.digest_view.digest_changes();
                                    let mut learned = std::collections::HashMap::new();
                                    loop {
                                        let turn = stop.turn("app-channels").await;
                                        changes.borrow_and_update();
                                        let work = topology_view.digest_view.all().into_iter().filter(|d| d.node.node_id != me).filter(|d| learned.get(&(d.node.node_id.clone(), d.node.incarnation.clone())) != Some(&d.held_hash));
                                        let results = futures_util::future::join_all(work.map(|d| {
                                            let (topology_view, client, rows) = (topology_view.clone(), client.clone(), rows.clone());
                                            let span = tracing::info_span!("rdm.node_admin.node.update.via-types-read", node_id = %d.node.node_id, incarnation = %d.node.incarnation, subject = %d.node.name);
                                            async move {
                                                let birth = (d.node.node_id.clone(), d.node.incarnation.clone());
                                                if d.node.name.kind == rdm_core::NodeKind::NodeAdmin {
                                                    // node.get of its own row, on op 0x23 (Q285).
                                                    let filter = rdm_core::filter::Filter::new().eq("id", [d.node.node_id.to_string()]);
                                                    let read = rdm_client::NodeRpc::new(&client).read(&rdm_transport::NodeTarget::ExactNode(d.node.node_id.clone()), rdm_core::filter::Entity::Nodes, &filter, &rdm_transport::CallOptions::default()).await;
                                                    {
                                                        if let Ok(rdm_core::read::Rows::Nodes(nodes)) = read {
                                                            if let Some(n) = nodes.iter().find(|n| n.node_id == birth.0 && n.incarnation_id.as_ref() == Some(&birth.1)) {
                                                                let hash = rdm_core::app_types::hash(&n.held);
                                                                if hash == n.held_hash && n.types.is_subset(&n.held) {
                                                                    if topology_view.learn_app_types_from(&d, n.types.clone(), n.held.clone()).await.is_ok() {
                                                                        tracing::info_span!("rdm.node_admin.node.update.via-held-types", node_id = %birth.0, incarnation = %birth.1, types = n.types.len(), held = n.held.len()).in_scope(|| tracing::info!("peer node.get supplied the joined types"));
                                                                        return Some((birth, d.held_hash));
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                } else if let Ok(Some(row)) = crate::node_rows::record_at(&*rows, &d.node.name).await {
                                                    if row.node_id == birth.0 {
                                                        if let Some(t) = row.app_types.filter(|t| t.incarnation == birth.1) {
                                                            // A member of this admin's own mesh: its types are held, it is dialled by no one but §6's repair (Q84).
                                                            if topology_view.hold_app_types_from(&d, t.types.clone(), t.types).await.is_ok() { return Some((birth, d.held_hash)); }
                                                        }
                                                    }
                                                }
                                                None
                                            }.instrument(span)
                                        })).await;
                                        learned.extend(results.into_iter().flatten());
                                        drop(turn);
                                        if changes.changed().await.is_err() { return; }
                                    }
                                })
                            };
                            let mut tasks = vec![contacts_task, node_rpc_feed, superseded_rows_task, app_channels_task];
                            tasks.extend(tasks_early);
                            // Entering an existing mesh (a recovering mesh's first admin after its maker's join; a fabric
                            // primary recovered into its mesh from its durable map): connect to a local node, read its
                            // topology and sweep the own mesh once with a Ping. Ready waits for the sweep; every node that
                            // did not answer is unheard, and node recovery's to prove (mesh-recovery.md step 6).
                            let sweep_slot: Arc<std::sync::OnceLock<crate::reenter::SweepReport>> = Arc::new(std::sync::OnceLock::new());
                            let entry_ctx = crate::reenter::EntryCtx { me: name.clone(), me_id: node_id.clone(), client: node_rpc.client.clone(), resolver: node_rpc_resolver.clone(), topology_view: topology_view.clone(), time: rafka_time.clone() };
                            if let (true, Some((source, map))) = (entering_existing_mesh, entry_map.take()) {
                                tasks.push(spawn_entry_sweep(&stop_gate, entry_ctx.clone(), source, map, sweep_slot.clone(), records.clone(), policy.provider));
                            }
                            // Authority-capable Ready: this admin commits `Ready` (and so
                            // becomes eligible for every seat) only once it can manage every birth it
                            // holds: each live member publishes a runtime fact this admin's provider
                            // adopts in its own control domain, and the Day-0 admin holds a Complete
                            // receipt for every step of its own adoption. Until then it publishes
                            // Pending.
                            // `pre_node_ready` (the app's hook): `node.start` runs it once the join is accepted and the
                            // mesh channel joined, before any topology is read, in a task behind this admin's gate, and then
                            // its topology, Build-floor and rafka-time steps in the same task; the Ready check below names
                            // the hook's blocker and the step the start is at. A birth with no launcher has no authority to
                            // pull from, and says why.
                            let pre_node_ready_cancel = rdm_transport::BirthCancel::new();
                            // The node-admins a Build-facts read asks, in order: the fabric-primary, every other live
                            // node-admin, then the launcher. Read at each request, so the order follows the leadership
                            // projection; every start of this admin asks them.
                            let responders: crate::fetch_build::Responders = {
                                let topology = control.topology.clone();
                                let launcher = cfg.launch.as_ref().and_then(|l| l.launcher.clone());
                                Arc::new(move || {
                                    let (topology, launcher) = (topology.clone(), launcher.clone());
                                    Box::pin(async move {
                                        let t = topology.read().await;
                                        let mut out: Vec<crate::fetch_build::Responder> = t.fabric_primary().map(|n| crate::fetch_build::Responder { name: n.name.clone(), node_id: n.node_id.clone() }).into_iter().collect();
                                        let mut admins: Vec<&crate::model::Node> = t.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).collect();
                                        admins.sort_by(|a, b| a.name.cmp(&b.name));
                                        for n in admins {
                                            if !out.iter().any(|r| r.name == n.name) {
                                                out.push(crate::fetch_build::Responder { name: n.name.clone(), node_id: n.node_id.clone() });
                                            }
                                        }
                                        if let Some(l) = launcher {
                                            if !out.iter().any(|r| r.name == l.name) {
                                                out.push(crate::fetch_build::Responder { name: l.name.clone(), node_id: l.node_id.clone() });
                                            }
                                        }
                                        out
                                    })
                                })
                            };
                            // What node.start has yet to do once the hook runs: the Ready check names it until the start's
                            // last step before node.started has ended.
                            let progress = StartProgress::begun(&name);
                            // The tasks node.start's topology step starts (the own-mesh sweep of a launched admin
                            // entering an existing mesh): ended with this admin's other tasks.
                            let start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::default();
                            let after = AdminFirstStart {
                                me: name.clone(),
                                me_id: node_id.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                mesh: cfg.mesh.clone(),
                                backbone: backbone.clone(),
                                records: records.clone(),
                                provider: policy.provider,
                                gate: stop_gate.clone(),
                                entry: entry_ctx,
                                entering: entering.clone(),
                                sweep: sweep_slot.clone(),
                                start_tasks: start_tasks.clone(),
                                floor: AdminFloor {
                                    me: name.clone(),
                                    builds: builds_dyn.clone(),
                                    floor: entry_floor.clone(),
                                    fetch_build: crate::fetch_build::FetchBuild::new(name.clone(), node_rpc.client.clone(), journal.clone(), responders.clone()),
                                    progress: Some(progress.clone()),
                                    held: cfg.launch.is_some().then(|| journal.clone()),
                                },
                                progress: progress.clone(),
                                read_seam: wiring.read_seam.clone(),
                            };
                            let parts = crate::workflows::node_start::HookParts {
                                birth: crate::node_hooks::Birth { name: name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), fabric_id: cfg.fabric_id.clone(), mesh_id: mesh_id.clone(), kind: NodeKind::NodeAdmin },
                                node_hooks: wiring.node_hooks.clone(),
                                events: crate::node_hooks::Events::new(digest_view.clone(), records.wake.pre_node_ready.clone()),
                                cancel: pre_node_ready_cancel.token(),
                                state: pre_node_ready_cell.clone(),
                                runs: crate::workflows::node_start::HookRuns::Spawned {
                                    spawn: {
                                        let stop = stop_gate.clone();
                                        Box::new(move |hook: crate::workflows::node_start::BoxFut<()>| stop.spawn(hook))
                                    },
                                },
                                after: Box::new(after),
                            };
                            let started: crate::workflows::node_start::HookStarted<String> = Box::new(move |hook: tokio::task::JoinHandle<()>| -> crate::workflows::node_start::BoxFut<Result<(), String>> {
                                Box::pin(async move {
                                    tasks.push(hook);
                                    {
                                        let pre_node_ready_state = pre_node_ready_cell.clone();
                                        let (digest, digest_view, provider, topology_view) = (digest.clone(), digest_view.clone(), provider_dyn.clone(), topology_view.clone());
                                        let day0 = cfg.launch.is_none().then(|| cfg.data_dir.clone());
                                        let rafka_time_at_apply = rafka_time.clone();
                                        // The adoption launch's attempt completes once it is Ready (Q156).
                                        let adopted = adoption_claim.clone().map(|c| (c, journal.clone(), mesh_claims.clone()));
                                        let (hydrated, start_progress, entering_gate) = (builds_dyn.clone(), progress.clone(), entering.clone());
                                        // A launched admin's Ready names the current Build its log holds, reading no store (Q227).
                                        let held_log = cfg.launch.is_some().then(|| journal.clone());
                                        let sweep_slot_gate = sweep_slot.clone();
                                        let (incarnation, node_id, mesh_id, fabric_id) = (incarnation.clone(), node_id.clone(), mesh_id.clone(), cfg.fabric_id.clone());
                                        let runtime = runtime.clone();
                                        let boot = boot.clone();
                                         let (records, status_door, mesh_name, fabric_name, view_provider, backbone_ready) =
                                            (records.clone(), status_door.clone(), cfg.mesh.clone(), cfg.fabric.clone(), policy.provider, backbone.clone());
                                        let stop = stop_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut reported = None;
                                            loop {
                                                // Held here while the admin is stopped: its own wait, outside every lock.
                                                let turn = stop.turn("ready").await;
                                                let me = digest.lock().unwrap().node.name.clone();
                                                let held: Vec<MeshDigest> = digest_view
                                                    .current(digest_view.staleness_floor())
                                                    .into_iter()
                                                    .filter(|d| d.node.name != me && d.status != MemberStatus::Leaving)
                                                    .collect();
                                                let mut blocked = take_over_blockers(&held, &*provider);
                                                if let Some(dir) = &day0 {
                                                    match adoption_missing(dir).await {
                                                        Ok(missing) => blocked.extend(missing.into_iter().map(|s| format!("{me}: no Complete receipt for its own {s}"))),
                                                        Err(e) => blocked.push(format!("{me}: its own adoption receipts cannot be read: {e}")),
                                                    }
                                                }
                                                if entering_gate.load(std::sync::atomic::Ordering::SeqCst) && sweep_slot_gate.get().is_none() {
                                                    blocked.push(format!("{me}: the own-mesh sweep on entering the mesh has not finished"));
                                                }
                                                // The app's hook: Ready waits on it, and its blocker is named here with the others.
                                                blocked.extend(pre_node_ready_state.lock().unwrap().as_ref().and_then(|h| h.state().blocker(&me)));
                                                // node.start's steps after the hook (topology, Build floor, rafka-time): Ready waits on
                                                // them, and the step it is at is named here with the others.
                                                blocked.extend(start_progress.blocker());
                                                // A mesh is Pending until its authority says so (e4.s11): mesh.create's first admin
                                                // holds its own Ready until the fabric primary applied MeshStatus::Pending at it; the
                                                // Day-0 root, with no upstream authority, applies its own. An admin entering an
                                                // existing mesh (its entry read holds another node of it) joins that mesh and owes
                                                // nothing here, its cohort live or not.
                                                let pending_applied = records.declared.lock().unwrap().meshes.contains_key(&mesh_id);
                                                let enters_existing_mesh = entering_gate.load(std::sync::atomic::Ordering::SeqCst);
                                                match pending_gate(&me, &mesh_name, day0.is_some(), pending_applied, enters_existing_mesh) {
                                                    PendingGate::Clear => {}
                                                    PendingGate::Blocked(why) => blocked.push(why),
                                                    // The Day-0 root applies its new mesh's Pending with no instant; the adoption
                                                    // launch its Mesh record's state at its own instant (Q272).
                                                    PendingGate::SelfApply => match (status_door.get(), adopted_mesh_state) {
                                                        (Some(_), Some(_)) if rafka_time_at_apply.try_now_ms().is_none() => {
                                                            blocked.push(format!("{me}: no rafka-time yet to stamp its mesh's state with"))
                                                        }
                                                        (Some(door), state) => {
                                                            let (state, at) = match state {
                                                                Some(state) => (state, rafka_time_at_apply.try_now_ms().unwrap_or(0)),
                                                                None => (rdm_core::status::MeshState::Pending, 0),
                                                            };
                                                            let reply = door.self_apply_mesh(&mesh_id, state, at).await;
                                                            if !matches!(reply, rdm_core::status::StatusReply::Applied | rdm_core::status::StatusReply::AlreadyApplied) {
                                                                blocked.push(format!("{me}: its mesh's {state:?} is not applied (self-apply answered {reply:?})"));
                                                            }
                                                        }
                                                        (None, _) => blocked.push(format!("{me}: no view yet to apply its mesh's state to itself")),
                                                    },
                                                }
                                                if blocked.is_empty() {
                                                    let (set, d_ready) = super::session::set_own_status(&digest, Some(MemberStatus::Pending), MemberStatus::Ready);
                                                    if !set {
                                                        break;
                                                    }
                                                    let _ = topology_view.publish(&d_ready).await;
                                                    let reads = ViewReads::read(&me, &digest_view, &records, &fabric_id).await;
                                                    let decided = incumbency_of(&digest_view, &records);
                                                    let t = project_decided(&fabric_name, &fabric_id, view_provider, &digest_view, &records, &decided, &reads);
                                                    // The election's result this admin holds: its digest carries it and, as its
                                                    // mesh's writer, it records it on the rows.
                                                    for (seat, holder) in seat_announcements(&t, &mesh_name, (&node_id, &incarnation), &decided) {
                                                        backbone_ready.hold_primary(seat, holder);
                                                    }
                                                    // The current Build the not-ready check above found held: the Ready span names it,
                                                    // so "it held the current Build before Ready" is read off the Ready itself.
                                                    let held_build = match match &held_log {
                                                        Some(log) => log.held_current_build().await,
                                                        None => hydrated.current_build().await,
                                                    } {
                                                        Ok(c) => c.into_build_id().map(|b| b.to_string()).unwrap_or_default(),
                                                        Err(e) => format!("unreadable: {e}"),
                                                    };
                                                    records.wake.poke();
                                                    tracing::info_span!(
                                                        parent: &boot,
                                                        "rdm.mesh.node.update.via-ready",
                                                        node = %me,
                                                        incarnation_id = %incarnation.as_str(),
                                                        meshes = topology_view.meshes_held(),
                                                        node_id = %node_id,
                                                        mesh_id = %mesh_id,
                                                        fabric_id = %fabric_id,
                                                        id_format = rdm_core::ID_FORMAT,
                                                        deployment_id = %runtime.deployment_id,
                                                        provider = runtime.provider.as_str(),
                                                        provider_control_domain_fingerprint = %runtime.domain_fingerprint(),
                                                        runtime_locator_kind = runtime.locator.kind(),
                                                        runtime_locator_fingerprint = %runtime.locator_fingerprint(),
                                                        source = "self-published-topology",
                                                        runtime_facts_held = held.len(),
                                                        build_id = %held_build,
                                                    )
                                                    .in_scope(|| tracing::info!("ready: every held birth's runtime is adoptable"));
                                                    // Under a delete the claimed attempt is the delete's own: the teardown ends it (Q159).
                                                    if let Some((claimed, builds, claims)) = adopted.as_ref().filter(|(c, _, _)| c.under_delete.is_none()) {
                                                        if let Err(e) = super::adoption::complete(claimed, &node_id, &**builds, &**claims).await {
                                                            tracing::warn!(node = %me, error = %e, "the adoption launch's attempt could not be completed");
                                                        }
                                                    }
                                                    break;
                                                }
                                                if reported.as_ref() != Some(&blocked) {
                                                    tracing::info_span!("rdm.node_admin.runtime.reject.via-not-ready", node = %me, blocked = blocked.len(), detail = %blocked.join("; "))
                                                        .in_scope(|| tracing::info!("not ready: a held birth's runtime cannot be managed from here"));
                                                    reported = Some(blocked);
                                                }
                                                drop(turn);
                                                tokio::time::sleep(Duration::from_millis(100)).await;
                                            }
                                        }));
                                    }
                                    {
                                        // The config is held on this admin's own digest and by its topology view, so every publish and
                                        // every read of its own digest carries it.
                                        let (topology_view, digest) = (topology_view.clone(), digest.clone());
                                        let _ = republish.set(Arc::new(move || {
                                            let (topology_view, digest) = (topology_view.clone(), digest.clone());
                                            Box::pin(async move {
                                                let d = digest.lock().unwrap().clone();
                                                let _ = topology_view.publish(&d).await;
                                            })
                                        }));
                                    }
                                    {
                                        let topology_view = topology_view.clone();
                                        let _ = configure.set(Arc::new(move |held: crate::status_rpc::ConfigHeld| {
                                            let topology_view = topology_view.clone();
                                            Box::pin(async move {
                                                // Every install over an older revision (Q235 P1); its own update held at once (P2).
                                                match held {
                                                    crate::status_rpc::ConfigHeld::Told { config, revision } => {
                                                        topology_view.install_config(config, revision);
                                                    }
                                                    crate::status_rpc::ConfigHeld::Sent(config) => topology_view.hold_sent_config(config),
                                                    crate::status_rpc::ConfigHeld::Answered(row) => topology_view.own_config_answered(row),
                                                }
                                            })
                                        }));
                                    }
                                    {
                                        // The name is held on this admin's own digest and by its topology view, taken only while it
                                        // has no connection open (R-CR1), read from what is open now, never from the rows that have
                                        // landed (Q169); a stopped admin holds it and publishes nothing.
                                        let (topology_view, digest, connections, registry) = (topology_view.clone(), digest.clone(), connections.clone(), data_connections.clone());
                                        let _ = rename.set(Arc::new(move |asked: PathName| {
                                            let (topology_view, digest, connections, registry) = (topology_view.clone(), digest.clone(), connections.clone(), registry.clone());
                                            Box::pin(async move {
                                                let held = digest.lock().unwrap().node.name.clone();
                                                let routes = connections.live_routes(None, Some(&registry));
                                                let decision = crate::node_name::decide(&held, &asked, &routes);
                                                if decision == crate::node_name::NameApply::Applied {
                                                    topology_view.set_name(asked.clone());
                                                    let d = {
                                                        let mut d = digest.lock().unwrap();
                                                        d.node.name = asked;
                                                        d.clone()
                                                    };
                                                    if !crate::node_self::process_stop().is_stopped() {
                                                        let _ = topology_view.publish(&d).await;
                                                    }
                                                }
                                                (held, decision)
                                            })
                                        }));
                                    }
                                    // This admin as the subject of its node commands from its own mesh-admin.
                                    {
                                        let digest_for_status = digest.clone();
                                        let digest_now = digest.clone();
                                        let set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync> = Arc::new(move |st| super::session::set_own_status(&digest_for_status, None, st).1);
                                        let status: Arc<dyn Fn() -> MemberStatus + Send + Sync> = Arc::new(move || digest_now.lock().unwrap().status);
                                        // Its stop's Stopped names the stop, as every node's does (node-status.md "Status
                                        // report", node-stop.md step 4; i146 Q37): owed in its declarer and sent from this
                                        // call, never from the declarer's own turns, which are held while it is stopped;
                                        // whichever of it and the stop's reply reaches the primary first is applied (Q49).
                                        // An admin that is its own mesh's primary has no primary to report to, and after its
                                        // cut it publishes nothing (node-stop.md step 4): its stop is its commander's to publish.
                                        let declare: crate::node_self::Declare = {
                                            let (declarer, topology, client, status_door, digest, me_id, incarnation, wake) =
                                                (declarer.clone(), control.topology.clone(), node_rpc.client.clone(), status_door.clone(), digest.clone(), node_id.clone(), incarnation.clone(), records.wake.clone());
                                            Arc::new(move |state, operation| {
                                                let me = digest.lock().unwrap().node.name.clone();
                                                let (declarer, topology, client, status_door, me_id, incarnation, wake) = (declarer.clone(), topology.clone(), client.clone(), status_door.clone(), me_id.clone(), incarnation.clone(), wake.clone());
                                                Box::pin(async move {
                                                    let view = topology.read().await.clone();
                                                    if view.node_type_primary(&me.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.node_id == me_id) {
                                                        tracing::info_span!("rdm.node_admin.status.reject.via-own-mesh-primary", node = %me, state = ?state, operation = %operation.as_ref().map(|o| o.operation.as_str()).unwrap_or(""), "otel.kind" = "internal")
                                                            .in_scope(|| tracing::info!("this admin is its own mesh's primary: there is no primary to report its state to, and after its cut it publishes nothing"));
                                                        return;
                                                    }
                                                    declarer.owe_own(&me, &me_id, &incarnation, state, operation);
                                                    declarer.sweep(&me, &me_id, &view, &client, status_door.get().map(|a| a.as_ref()), &BTreeSet::new(), &wake).await;
                                                })
                                            })
                                        };
                                        let own = Arc::new(crate::node_self::NodeSelf::new(node_id.clone(), incarnation.clone(), name.clone(), rpc_server.clone(), node_rpc.client.clone(), node_rpc.resolver.clone(), topology_view.clone(), set_status, status, Some(declare), cfg.connections_delete_deadline).with_cancel(pre_node_ready_cancel.clone()).with_backbone(backbone.clone()));
                                        own.set_connections(connections.clone());
                                        // A storage change's move of this admin (storage.md §2 step 4): the provider beneath its
                                        // one storage handle replaced, every reader and writer moving with it.
                                        own.set_storage_moves({
                                            let (writes, moves) = (topology_storage.clone(), topology_storage.clone());
                                            Arc::new(crate::storage_move::StorageMoves::new(
                                                node_id.clone(),
                                                Arc::new(move || writes.provider()),
                                                Arc::new(move |provider: String| {
                                                    let handle = moves.clone();
                                                    Box::pin(async move { handle.replace(&provider).await })
                                                }),
                                            ))
                                        });
                                        // The app protocols this admin opens connections on: what a renewed member cert
                                        // is installed on, and what node.connection.delete closes, as on every node.
                                        own.set_data_connections(data_connections.clone());
                                        own.set_session(Arc::new(AdminSession {
                                            data_connections: data_connections.clone(),
                                            drives: drives.clone(),
                                            gate: stop_gate.clone(),
                                            node_id: node_id.clone(),
                                            incarnation: incarnation.clone(),
                                            fabric_id: cfg.fabric_id.clone(),
                                            mesh_id: mesh_id.clone(),
                                            topology_view: topology_view.clone(),
                                            backbone: backbone.clone(),
                                            client: node_rpc.client.clone(),
                                            server: rpc_server.clone(),
                                            digest: digest.clone(),
                                            rafka_time: rafka_time.clone(),
                                            accepted: accepted.clone(),
                                            builds: builds_dyn.clone(),
                                            entry_floor: entry_floor.clone(),
                                            node_hooks: wiring.node_hooks.clone(),
                                            addressed: records.wake.pre_node_ready.clone(),
                                            pre_node_ready_cell: pre_node_ready_cell.clone(),
                                            pre_node_ready_task: Arc::default(),
                                            wake: records.clone(),
                                            local: journal.clone(),
                                            responders: responders.clone(),
                                            declarer: declarer.clone(),
                                        }));
                                        let _ = own_commands.set(own);
                                    }
                                    let d = digest.clone();
                                    let publisher = topology_view.publish_every(gossip_interval(), move || {
                                        d.lock().unwrap().clone()
                                    });
                                    // The declarer (i143.e4.s11, R-S2): what this admin owes its authorities over `Status`, sent
                                    // when owed and again only on an eligible event, woken by its inputs; nothing it does gates
                                    // gossip or readiness.
                                    {
                                        let declarer = declarer.clone();
                                        let (digest, topology, records, me_id, incarnation, client, resolver, digest_view, topology_view, status_door) =
                                            (digest.clone(), control.topology.clone(), records.clone(), node_id.clone(), incarnation.clone(), node_rpc.client.clone(), node_rpc.resolver.clone(), digest_view.clone(), topology_view.clone(), status_door.clone());
                                        let stop = stop_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                // Held here while the admin is stopped: its own wait, outside every lock.
                                                let turn = stop.turn("declarer").await;
                                                let addressed = records.wake.take_addressed();
                                                let view = topology.read().await.clone();
                                                // Named by the name it holds now: a start installs the name its join
                                                // answer hands (R12, R-JN1), and its declarations say so.
                                                let (me, own) = {
                                                    let d = digest.lock().unwrap();
                                                    (d.node.name.clone(), d.status)
                                                };
                                                let mesh_ids: BTreeMap<String, String> = records.meshes.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.to_string())).collect();
                                                let own_mesh_id = records.meshes.lock().unwrap().get(&me.mesh).cloned();
                                                let own_mesh_held = own_mesh_id.and_then(|id| records.declared.lock().unwrap().meshes.get(&id).copied());
                                                // The instant this admin's own MeshStatus holds for each state: what its publisher sent.
                                                let own_mesh_changed_at = |state| {
                                                    topology_view
                                                        .adopted_mesh_status()
                                                        .filter(|h| super::reconcile::mesh_state_of(&h.status).ok() == Some(state) && h.changed_at_rafka_ms > 0)
                                                        .map(|h| h.changed_at_rafka_ms)
                                                        .unwrap_or_else(|| super::runner::held_mesh_changed_at(&digest_view, &records, &me.mesh, state))
                                                };
                                                crate::status_declare::owed_from_view(&declarer, &me, &me_id, &incarnation, own, records.declare_gate(), own_mesh_held, own_mesh_changed_at, &view, &mesh_ids);
                                                rdm_node::node_rpc::feed_once(&digest_view, &resolver, &me.to_string());
                                                declarer.sweep(&me, &me_id, &view, &client, status_door.get().map(|a| a.as_ref()), &addressed, &records.wake).await;
                                                drop(turn);
                                                // One coalesced wake from the inputs, never a timer: a poke during the sweep is
                                                // the stored permit, so the next sweep runs at once; a burst runs one.
                                                records.wake.woken().await;
                                            }
                                        }));
                                    }

                                                // The offline tickle (node-recovery.md "Detection and proof"): every admin keeps the silence
                                                // bookkeeping; the one holding its mesh's primary seat tickles each node of its own mesh the
                                                // topology shows PendingReconnect, half a floor after the mark, with the one ladder
                                                // (node-tickle.md, `offline::tickle`): the probe (op 27) direct, and when it does not answer,
                                                // the same probe through up to two peers at once on the ViaPeer route, the leads the store's
                                                // by-destination connections index names first, then live nodes of its mesh (Q107 item 1). The
                                                // node's status lands on its row in nodes.storage and in the view.
                                                let recovery = RecoverySubmitter {
                                                    me: name.clone(),
                                                    topology: control.topology.clone(),
                                                    digest_view: digest_view.clone(),
                                                    provider: runner.provider.clone(),
                                                    nodes_storage: nodes_storage.clone(),
                                                    written: records.caches.nodes.clone(),
                                                    door: build_slot.clone(),
                                                    client: Some(node_rpc.client.clone()),
                                                    builds: control.builds.clone(),
                                                    data_root: cfg
                                                        .data_dir
                                                        .parent()
                                                        .map(Path::to_path_buf)
                                                        .unwrap_or_else(|| cfg.data_dir.clone()),
                                                    gone_dirs: Arc::default(),
                                                    resubmits: Arc::default(),
                                                };
                                                // A refused caller (Q207) is decided by its path.name and this admin's atlas, and acts
                                                // through this recovery submitter's terminate report and the heard-again door.
                                                // Its verification at the root is owed to the fabric-primary the view names, sent again
                                                // on each view installed (rdm#177).
                                                let callers = Arc::new(crate::refused_caller::RefusedCallers::new(node_id.clone(), recovery.clone(), heard_again_slot.clone()));
                                                callers.watch_views(records.subscribe_view());
                                                control.atlas.refused_callers(callers);
                                                // Fabric recovery (Q122): started by the seat watch in the fabric-primary's own mesh, and by mesh
                                                // investigation's decision step from the backbone.
                                                let fabric_recovery = Arc::new(
                                                    crate::fabric_recovery::FabricRecoveryDoor::new(
                                                        name.clone(),
                                                        node_id.clone(),
                                                        control.topology.clone(),
                                                        digest_view.clone(),
                                                        records.clone(),
                                                        crate::seat_watch::Looker {
                                                            me: name.clone(),
                                                            me_id: node_id.clone(),
                                                            topology_view: topology_view.clone(),
                                                            client: node_rpc.client.clone(),
                                                            provider: runner.provider.clone(),
                                                            nodes: Some(nodes_storage.clone()),
                                                            heard_again: heard_again_slot.clone(),
                                                            connections: Some(
                                                                connections_storage.clone(),
                                                            ),
                                                            commands: Some(commands.clone()),
                                                        },
                                                        recovery.clone(),
                                                        handover_slot.clone(),
                                                        cfg.fabric_primary || cfg.adopt.is_some(),
                                                        stop_gate.clone(),
                                                    )
                                                    .adopted_at_new_path(adoption_claim.as_ref().is_some_and(|c| c.under_delete.is_some())),
                                                );
                                                {
                                                    let (
                                                        topology,
                                                        records,
                                                        silence_nodes,
                                                        runner,
                                                        me,
                                                        me_id,
                                                        mesh,
                                                        recovery,
                                                    ) = (
                                                        control.topology.clone(),
                                                        records.clone(),
                                                        nodes_storage.clone(),
                                                        runner.clone(),
                                                        name.clone(),
                                                        node_id.clone(),
                                                        cfg.mesh.clone(),
                                                        recovery.clone(),
                                                    );
                                                    let silence_records = records.caches.nodes.clone();
                                                    let stop = stop_gate.clone();
                                                    let mut cadence =
                                                        crate::offline::Cadence::every(
                                                            gossip_interval(),
                                                        )?;
                                                    // A birth heard again is commanded back through node.mesh.join (Q87).
                                                    let ladder_heard_again =
                                                        heard_again_slot.clone();
                                                    // The ladder's leads: the by-destination index of the store (Q107 item 1).
                                                    let ladder_connections =
                                                        connections_storage.clone();
                                                    // The commands this admin sent: a member that left one unanswered past the
                                                    // floor is silent whatever it gossips (Q182).
                                                    let ladder_commands = commands.clone();
                                                    // The refused callers' atlas (Q207): a returned birth is decided from it.
                                                    let control_atlas = control.atlas.clone();
                                                    tasks.push(tokio::spawn(async move {
                                            // The births whose inspection or submitted recovery is in flight. The tick never waits
                                            // on it.
                                            let in_flight: Arc<Mutex<HashSet<(NodeId, IncarnationId)>>> = Arc::default();
                                            let mut tickle = crate::offline::OfflineTickle::observed_by(me.to_string(), stop.clone());
                                            let floor = rdm_transport::topology::staleness_floor();
                                            let mut written: WrittenSilence = HashMap::new();
                                            let mut heard_changes = recovery.digest_view.heard_changes();
                                            let mut tenure_rows = TenureRows::default();
                                            loop {
                                                // Held here while the admin is stopped: its own wait, outside every lock.
                                                let turn = stop.turn("offline-tickle").await;
                                                heard_changes.borrow_and_update();
                                                // The births held true offline are shown `Dead` by the projection: a change of
                                                // them this tick makes the view's rebuild due where it is made (Q254).
                                                let offline_before = records.offline.lock().unwrap().clone();
                                                let view = topology.read().await.clone();
                                                let holds_seat = view.is_mesh_primary(&mesh, &me_id);
                                                // The births only a durable runtime row names: the mesh-primary's alone, from its mesh's
                                                // rows read once as it takes the role (`TenureRows`); an admin that is no primary reads
                                                // none. A birth the view holds is marked from this admin's own digest view, so a
                                                // successor tickles on the window already running.
                                                let durable = {
                                                    let rows = tenure_rows.for_tick(holds_seat, &*recovery.nodes_storage, &recovery.held_name(), &mesh).await;
                                                    let planned = records.planned();
                                                    durable_births(&view, rows, &|p| planned.get(&p.mesh).is_some_and(|m| m.contains(p)), &|id| recovery.digest_view.is_deleted(id))
                                                };
                                                *records.durable.lock().unwrap() = durable.clone();
                                                let mut watched = watched_births(&view, &me_id, &mesh, &durable);
                                                // A node-admin the election replaced as this mesh's primary is the fabric-primary's to
                                                // investigate and recover (node-recovery.md step 2), never this tick's.
                                                watched.retain(|_, n| !(n.kind == NodeKind::NodeAdmin && n.incarnation_id.as_ref().is_some_and(|i| recovery.digest_view.seats.replaced(rdm_core::Seat::MeshPrimary, &mesh, i))));
                                                // One reading of the clocks for the tick: each birth's mark is read from the view at it.
                                                let (now, now_ms) = (std::time::Instant::now(), local_now_ms());
                                                // node-tickle.md step 1: a birth a command never reached for a reachability reason is
                                                // PendingReconnect from that instant, so its ladder runs now.
                                                let mut marks = crate::offline::held_marks(&records.unreachable, &view, &mesh, &recovery.digest_view, now);
                                                // node-investigation.md (Q182): a member that left a command unanswered past the floor is
                                                // silent whatever it gossips, marked from the instant the command passed the floor. The
                                                // commands' own record decides, each tick: an answer ends the mark.
                                                for (id, mark) in crate::offline::unanswered_marks(&ladder_commands, &view, &mesh, floor, now, now_ms) {
                                                    marks.entry(id).or_insert(mark);
                                                }
                                                crate::offline::watch_marked(&mut watched, &marks, &view, &me_id);
                                                // The topology decides who is tickled (Luke, Q87): a mesh-primary's view comes from the
                                                // fabric-primary and is current, so a member it holds ready there is not
                                                // tickled on this admin's own fresh digest view; only a member the view holds off
                                                // ready (its mark, or a command never sent) is.
                                                let silent: Vec<crate::offline::SilentNode> = crate::offline::silent_set(
                                                    watched.values().filter(|n| n.status != NodeStatus::Ready).filter(|n| {
                                                        marks.contains_key(&n.node_id)
                                                            || !recovery
                                                                .digest_view
                                                                .get_at(n.node_id.as_str(), now)
                                                                .or_else(|| recovery.digest_view.projected_at(n.node_id.as_str(), now))
                                                                .is_some_and(|(d, unheard)| Some(&d.node.incarnation) == n.incarnation_id.as_ref() && unheard < floor)
                                                    }),
                                                    &recovery.digest_view,
                                                    now,
                                                    now_ms,
                                                    floor,
                                                )
                                                .into_iter()
                                                .map(|s| crate::offline::marked_from(s, &marks))
                                                .collect();
                                                let client = Some(runner.node_rpc.client.clone());
                                                // The one ladder's view of the carriers: this tick's, shared by its ladders.
                                                let ladder_view = Arc::new(view.clone());
                                                // What the tick watches decides "heard again" (`heard_against`, R-FM1).
                                                let in_view: HashSet<String> = heard_against(&view, &watched);
                                                let report = tickle
                                                    .tick(
                                                        now_ms,
                                                        holds_seat,
                                                        silent,
                                                        |id: &str| in_view.contains(id),
                                                        floor.as_millis() as u64 / 2,
                                                        |id| {
                                                            // The one ladder over the birth (node-tickle.md): the probe direct, then
                                                            // through up to two ready members of its mesh at once, this admin never
                                                            // one of them. A birth the tick does not hold, or an admin without a Node
                                                            // RPC client, runs no ladder, by name.
                                                            let (client, node, view) = (client.clone(), watched.get(&id).cloned(), ladder_view.clone());
                                                            let (me_id, me, connections) = (me_id.clone(), me.to_string(), Some(ladder_connections.clone()));
                                                            let commands = Some(ladder_commands.clone());
                                                            let inspector = runner.clone();
                                                            async move {
                                                                use crate::offline::Ladder;
                                                                let Some(node) = node else { return Ladder::Tickled(Err(format!("{id} is not in the topology"))) };
                                                                // node.runtime.inspect first (Q273): exited or os-stopped is the verdict, and no
                                                                // tickle is sent.
                                                                let (inspected, handle) = crate::node_investigation::inspect_birth_with_handle(&*inspector.provider, &inspector.digest_view, inspector.records.contacts.get().map(|s| &**s as &dyn crate::storage::NodesStorage), &node).await;
                                                                crate::node_investigation::span_inspection(&me, &node, &inspected, handle.as_ref(), "the recovery tick's investigation asked the provider first");
                                                                if matches!(inspected, crate::deployment::provider::Inspected::Exited(_) | crate::deployment::provider::Inspected::OsStopped) {
                                                                    return Ladder::Gone(crate::node_investigation::Finding::from(inspected).name().to_string());
                                                                }
                                                                let Some(client) = client else { return Ladder::Tickled(Err("this admin has no Node RPC client".to_string())) };
                                                                Ladder::Tickled(crate::offline::tickle(&node, &view, &crate::offline::TickleClient { client, me_id, me, connections, commands }).await)
                                                            }
                                                        },
                                                        |id| {
                                                            // One read of iroh's local view of the node, recorded beside the event.
                                                            let (endpoint, key) = (runner.endpoint.clone(), watched.get(&id).and_then(|n| n.endpoint_id.as_ref()).and_then(|k| k.0.parse::<iroh::PublicKey>().ok()));
                                                            async move {
                                                                match endpoint {
                                                                    Some(ep) => rdm_transport::iroh_obs::observe_remote(&ep, key).await,
                                                                    None => rdm_transport::iroh_obs::IrohObservation::unavailable(),
                                                                }
                                                            }
                                                        },
                                                    )
                                                    .await;
                                                // The birth's own silence row is put blind, keyed by the exact birth this admin
                                                // holds now; no other row is read or touched. What this loop last put is held here,
                                                // so an unchanged status is not put again.
                                                let write = async |id: &str, status: Option<NodeStatus>, written: &mut HashMap<NodeId, (Option<crate::model::IncarnationId>, Option<NodeStatus>)>| {
                                                    let held = topology.read().await.births().find(|n| n.node_id.as_str() == id).cloned();
                                                    let Some(node) = held.or_else(|| watched.get(id).cloned()) else { return };
                                                    let fact = (node.incarnation_id.clone(), status);
                                                    if written.get(&node.node_id) == Some(&fact) {
                                                        return;
                                                    }
                                                    // A refusal is spanned where it happened (`offline::put_silence_status`); the memo
                                                    // keeps the last status that stands, and nothing below waits on this row.
                                                    if crate::offline::put_silence_status(&*silence_nodes, &silence_records, &node, status).await.is_ok() {
                                                        written.insert(node.node_id.clone(), fact);
                                                    }
                                                };
                                                if holds_seat {
                                                    for (id, n) in &watched {
                                                        if n.status == NodeStatus::PendingReconnect {
                                                            write(id, Some(NodeStatus::PendingReconnect), &mut written).await;
                                                        }
                                                    }
                                                }
                                                for id in &report.offline {
                                                    if let Some(n) = watched.get(id) {
                                                        if let Some(inc) = n.incarnation_id.clone() {
                                                            records.offline.lock().unwrap().insert((n.node_id.clone(), inc));
                                                        }
                                                    }
                                                    write(id, Some(NodeStatus::Dead), &mut written).await;
                                                }
                                                // A birth heard again, or answering its tickle, is reached (node-recovery.md step 3).
                                                for id in report.returned.iter().chain(&report.reachable_silent) {
                                                    records.unreachable.lock().unwrap().retain(|nid, _| nid.as_str() != id);
                                                }
                                                for id in &report.returned {
                                                    // A held birth heard again by its digest is commanded back through node.mesh.join
                                                    // too (Q87: the view returns on the bare digest, its cert, time and cache do not):
                                                    // by the mesh-primary, on its own task.
                                                    if holds_seat {
                                                        if let (Some(heard_again), Some(mut n)) = (ladder_heard_again.get().cloned(), watched.get(id).or_else(|| view.births().find(|n| n.node_id.as_str() == id)).cloned()) {
                                                            // The tick held this birth off its mesh (it was tickled and returned): the view's
                                                            // ready, when it shows one, is the birth's own last word before the
                                                            // silence, not where it stands now (Q87).
                                                            if n.status == NodeStatus::Ready {
                                                                n.status = NodeStatus::PendingReconnect;
                                                            }
                                                            if let Some(incarnation) = n.incarnation_id.clone() {
                                                                // Ended at this admin's stop where it stands (the stop gate's `until_stopped`).
                                                                tokio::spawn(stop.until_stopped(async move { heard_again.command(&n, &incarnation).await; }));
                                                            }
                                                        }
                                                    }
                                                    // The mark goes before the rows are cleared: a follower in flight reads the
                                                    // mark after its own write and removes what it wrote (`RecoverySubmitter::offline`).
                                                    records.offline.lock().unwrap().retain(|(nid, _)| nid.as_str() != id);
                                                    // The silence row is written and removed by the mesh-primary
                                                    // (node-identity-and-state.md): an admin that does not hold the seat hears the
                                                    // return and clears only its own records.
                                                    if !holds_seat {
                                                        continue;
                                                    }
                                                    write(id, None, &mut written).await;
                                                }
                                                for id in &report.reachable_silent {
                                                    // A held birth that answered its probe `Current` (`report.answered_current`) is
                                                    // commanded back through node.mesh.join (Q87): its commanded start runs on its own
                                                    // task, after the ladder's fold. Only the mesh-primary commands its members
                                                    // (commands go down); every admin keeps the bookkeeping.
                                                    if report.answered_current.contains(id) && holds_seat {
                                                        if let (Some(heard_again), Some(mut n)) = (ladder_heard_again.get().cloned(), watched.get(id).or_else(|| view.births().find(|n| n.node_id.as_str() == id)).cloned()) {
                                                            // Held off its mesh in the tick's eyes (it was kicked and answered): the view's
                                                            // ready is its last word before the silence (Q87).
                                                            if n.status == NodeStatus::Ready {
                                                                n.status = NodeStatus::PendingReconnect;
                                                            }
                                                            if let Some(incarnation) = n.incarnation_id.clone() {
                                                                // Ended at this admin's stop where it stands (the stop gate's `until_stopped`).
                                                                tokio::spawn(stop.until_stopped(async move { heard_again.command(&n, &incarnation).await; }));
                                                            }
                                                        }
                                                    }
                                                }
                                                clear_unwatched(&watched, &mut records.offline.lock().unwrap(), &mut written);
                                                if *records.offline.lock().unwrap() != offline_before {
                                                    records.inputs_moved();
                                                }
                                                // node-recovery.md step 3: on TRUE OFFLINE the mesh primary asks the provider about the
                                                // exact runtime of each member of its own mesh, its secondary node-admins included
                                                // (mesh-recovery.md:8-9), and the verdict opens the recovery at once, keyed on the
                                                // birth. Re-evaluated every tick while the member stays TRUE OFFLINE, so a recovery
                                                // that failed is submitted again once its birth is eligible (node-recovery.md, Q209):
                                                // a birth whose last submit ended without the recovery is neither inspected nor
                                                // submitted until its backoff, doubling from the gossip interval to the staleness
                                                // floor, has passed, and a birth whose submit is in flight is followed, never
                                                // submitted again beside it (`RecoverySubmitter::take_eligible`).
                                                {
                                                    let offline = records.offline.lock().unwrap().clone();
                                                    let now = std::time::Instant::now();
                                                    for n in recovery_due(holds_seat, &mesh, &watched, &offline) {
                                                        let Some(incarnation) = n.incarnation_id.clone() else { continue };
                                                        let birth = (n.node_id.clone(), incarnation);
                                                        if !recovery.take_eligible(&birth, now, &in_flight) {
                                                            continue;
                                                        }
                                                        let (recovery, n) = (recovery.clone(), n.clone());
                                                        crate::offline::spawn_recovery_follower(&stop, in_flight.clone(), birth, async move {
                                                            recovery.recover(&n).await;
                                                        });
                                                    }
                                                    // node-recovery.md "Failure and receipts": a birth whose workflow in the
                                                    // accepted Build stopped at a Failed attempt is submitted again, read from the
                                                    // durable facts each tick, whether or not the view still holds it (a workflow
                                                    // whose delete completed deleted it) and whoever submitted it before. One submit
                                                    // per birth in flight, whichever way it is due, and on the same backoff.
                                                    let owed_now = recovery.owed_submits(holds_seat, &mesh).await;
                                                    let owed_births: HashSet<(NodeId, IncarnationId)> = owed_now.iter().map(|o| (o.node_id.clone(), o.incarnation.clone())).collect();
                                                    for owed in owed_now {
                                                        let birth = (owed.node_id.clone(), owed.incarnation.clone());
                                                        if !recovery.take_eligible(&birth, now, &in_flight) {
                                                            continue;
                                                        }
                                                        let recovery = recovery.clone();
                                                        crate::offline::spawn_recovery_follower(&stop, in_flight.clone(), birth, async move {
                                                            let _ = recovery.submit_owed(&owed).await;
                                                        });
                                                    }
                                                    // A birth owed nothing now (heard again, gone from the view or replaced at its
                                                    // path.name, or this admin no longer its mesh-primary) keeps no backoff: unheard
                                                    // again, it is eligible at once (Q209).
                                                    recovery.forget_resubmits(|birth| holds_seat && (offline.contains(birth) || owed_births.contains(birth)));
                                                    // A second birth another executor made at a path.name of this mesh across a cut:
                                                    // superseded, its row written deleted and its NodeDeleted published, so its next
                                                    // digest is a deleted birth heard again (node-investigation.md, Q142).
                                                    if holds_seat {
                                                        for op in recovery.superseded_births(&view, &mesh).await {
                                                            runner.lifecycle_events.deleted(&op).await;
                                                        }
                                                    }
                                                    // A deleted birth of this mesh heard again by its own digest: refused by every
                                                    // view, and deleted or re-admitted by its path.name and this admin's atlas (Q207,
                                                    // node-investigation.md).
                                                    if let (true, Some(callers)) = (holds_seat, control_atlas.callers().cloned()) {
                                                        for d in recovery.digest_view.returned_births().into_iter().filter(|d| d.node.name.mesh == mesh) {
                                                            let (recovery, callers, atlas) = (recovery.clone(), callers.clone(), control_atlas.clone());
                                                            // Ended at this admin's stop where it stands (the stop gate's `until_stopped`).
                                                            tokio::spawn(stop.until_stopped(async move {
                                                                if let Some((_, outcome)) = callers.decide(&atlas, "digest", &d.node.node_id, &d.node.incarnation).await {
                                                                    if outcome == "terminate-reported" || outcome.starts_with("mesh-join:") {
                                                                        recovery.digest_view.returned_reported(d.node.node_id.as_str(), d.node.incarnation.as_str());
                                                                    }
                                                                }
                                                            }));
                                                        }
                                                    }
                                                }
                                                drop(turn);
                                                tokio::select! {
                                                    _ = cadence.next() => {}
                                                    _ = heard_changes.changed() => {}
                                                }
                                            }
                                        }));
                                    }

                                    // Cert renewal (node-cert-renew.md, mesh-issuer-renew.md): on every gossip interval, the
                                    // births this admin's seats make it the issuing authority of are renewed from their cert rows;
                                    // holding the fabric seat, each mesh's issuer, sent to that mesh's primary; holding a mesh's
                                    // primary seat, the issuer it installed carried to its mesh's other admins. Whoever holds a
                                    // seat continues the schedule from the rows; a stopped admin renews nothing.
                                    {
                                        let own_cert = own_commands.clone();
                                        let renewals = crate::cert_renewal::Renewals::new(
                                            name.clone(),
                                            node_id.clone(),
                                            issuer.clone(),
                                            nodes_storage.clone(),
                                            records.caches.clone(),
                                            node_rpc.client.clone(),
                                            Arc::new(move |bytes: Vec<u8>| {
                                                if let Some(own) = own_cert.get() {
                                                    let _ = own.renew_cert(&bytes);
                                                }
                                            }),
                                        );
                                        renewals.set_fabric(fabric_storage.clone(), cfg.fabric_id.clone());
                                        {
                                            // The fabric-primary's own node.roots.update, in process: the set installed on its own birth, the
                                            // root's secret it rides with kept as the door keeps it (fabric-ca-renew.md).
                                            let (own_roots, roots_issuer, roots_me, roots_id) = (own_commands.clone(), issuer.clone(), name.to_string(), node_id.clone());
                                            renewals.set_own_roots(Arc::new(move |incarnation: &rdm_core::IncarnationId, roots: &[String], phase: rdm_core::status::RootsPhase, secret: Option<rdm_core::status::RootSecretHex>| {
                                                let Some(registry) = own_roots.get().and_then(|o| o.registry()) else {
                                                    return rdm_core::reply::status::not_ready(format!("{roots_me} holds no connections registry yet; node.roots.update is refused"));
                                                };
                                                let installed = rdm_node::roots_update::install_roots(&registry, &roots_me, &roots_id, incarnation, roots, phase, secret.is_some());
                                                if let (rdm_core::status::StatusReply::RootsUpdated { .. }, Some(secret)) = (&installed, secret) {
                                                    let kept = rdm_core::certs::decode_key(&secret.0).and_then(|k| if phase == rdm_core::status::RootsPhase::Updated { roots_issuer.replace_root_with(k) } else { roots_issuer.hold_next_root(k) });
                                                    if let Err(e) = kept {
                                                        return rdm_core::reply::status::rejected_cert(format!("{roots_me}: the root's secret: {e}"));
                                                    }
                                                }
                                                installed
                                            }));
                                        }
                                        let (topology, gate) = (control.topology.clone(), stop_gate.clone());
                                        let mut cadence = crate::offline::Cadence::every(gossip_interval())?;
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                let turn = gate.turn("cert-renewal").await;
                                                let view = topology.read().await.clone();
                                                crate::cert_renewal::renew_issuers(&renewals, &view).await;
                                                crate::cert_renewal::renew_members(&renewals, &view).await;
                                                drop(turn);
                                                cadence.next().await;
                                            }
                                        }));
                                    }

                                    // The seat watch (ruling R-A2): a silent seat holder is looked at, never replaced for being
                                    // silent. Each admin measures silence in its own view and looks at the exact runtime;
                                    // no gossip frame commands the investigation.
                                    {
                                        let watch = crate::seat_watch::Watch {
                                            gate: stop_gate.clone(),
                                            me: name.clone(),
                                            me_id: node_id.clone(),
                                            incarnation: incarnation.clone(),
                                            topology_view: topology_view.clone(),
                                            topology: control.topology.clone(),
                                            records: records.clone(),
                                            looker: crate::seat_watch::Looker { me: name.clone(), me_id: node_id.clone(), topology_view: topology_view.clone(), client: node_rpc.client.clone(), provider: runner.provider.clone(), nodes: Some(nodes_storage.clone()), heard_again: heard_again_slot.clone(), connections: Some(connections_storage.clone()), commands: Some(commands.clone()) },
                                            recovery: Some(recovery.clone()),
                                            fabric_recovery: fabric_recovery.clone(),
                                        };
                                        // Not behind the gate: it reads the view; it is held at its own wait (`Watch::gate`).
                                        tasks.push(tokio::spawn(crate::seat_watch::run(watch)));
                                    }

                                    // The open lifecycle overlays, derived from the Build facts inside each append to this admin's
                                    // log (its own appends and the records it reads from the store root alike), before the append
                                    // returns: a hold opens with its pre-event's receipt and closes with its operation's `Complete`
                                    // or its attempt's `Converged`, and no reader of the log sees that fact while the view still
                                    // holds the hold (a log on a plane other admins write converges at the derivation instead:
                                    // `LocalBuildLog::derive_on_every_fact`). A successor primary publishes the same overlays from the same facts. Derived
                                    // once over the facts held before the hierarchy's first turn, then on every fact.
                                    {
                                        let (digest_view, named) = (topology_view.digest_view.clone(), Mutex::new(NamedSteps::new()));
                                        // The Builds the view holds overlays of are read with the current Build and the new
                                        // facts' own, so an overlay heard by gossip ends on its own Build's facts.
                                        let holds = digest_view.clone();
                                        let held = move || holds.in_flight().into_iter().filter_map(|o| crate::build::BuildId::parse(&o.build_id).ok()).collect::<Vec<_>>();
                                        journal.derive_on_every_fact(crate::build_state::FactDerivation::new(held, move |facts| overlays_from_facts(facts, &digest_view, &mut named.lock().unwrap()))).await;
                                    }

                                    let hierarchy;
                                    let anti_entropy;
                                    // What the hierarchy hands the mesh topology anti-entropy: a role it took is held for
                                    // writing until its seat is announced (`roles`), the anti-entropy is told once it
                                    // is (`roles_moved`).
                                    let roles = Arc::new(tokio::sync::RwLock::new(()));
                                    let roles_moved = Arc::new(tokio::sync::Notify::new());
                                    // The hierarchy decides this admin's roles: its mesh's primary forwards the other
                                    // meshes onto its mesh channel; the fabric primary publishes the fabric's status and
                                    // changes the fabric states. It joins the backbone through every admin it knows and
                                    // its mesh channel through every member of its mesh it knows. It publishes no
                                    // mesh topology: that is the anti-entropy's timer, below.
                                    {
                                        let (roles, roles_moved) = (roles.clone(), roles_moved.clone());
                                        let (backbone, topology_view, topology, me, mesh) = (backbone.clone(), topology_view.clone(), control.topology.clone(), name.clone(), cfg.mesh.clone());
                                        let (view_fabric, view_fabric_id, view_provider, view_digest_view) = (cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone());
                                        let (seat_node_id, seat_incarnation) = (node_id.clone(), incarnation.clone());
                                        let adapter = runner.builds.clone();
                                        let records = records.clone();
                                        let fabric_states = states_slot.get().cloned().expect("the fabric states are built before the hierarchy runs");
                                        let stop = stop_gate.clone();
                                        let claim_door = claim_slot.clone();
                                        // A mesh-primary taking its role reads the accepted Build from the fabric-primary before it
                                        // counts its fresh checklist (states.md "The check-in checklist", Q276): node start's Build floor
                                        // read again, its own mesh's section of the current Build (`FetchBuildSection`, Q284) from the fabric-primary the view names, owed until answered.
                                        let fabric_primary_only: crate::fetch_build::Responders = {
                                            let topology = control.topology.clone();
                                            Arc::new(move || {
                                                let topology = topology.clone();
                                                Box::pin(async move { topology.read().await.fabric_primary().map(|n| crate::fetch_build::Responder { name: n.name.clone(), node_id: n.node_id.clone() }).into_iter().collect() })
                                            })
                                        };
                                        let (build_read_client, build_read_log) = (node_rpc.client.clone(), journal.clone());
                                        let (atlas, atlas_rows) = (control.atlas.clone(), nodes_storage.clone());
                                        hierarchy = tokio::spawn(async move {
                                            let mut was_fabric_primary = false;
                                            let mut was_mesh_primary = false;
                                            let mut build_read = crate::fetch_build::FetchBuild::new(me.clone(), build_read_client, build_read_log, fabric_primary_only);
                                            let mut build_read_owed = false;
                                            let mut unreadable_named: Option<String> = None;
                                            loop {
                                                // Held here while the admin is stopped: its own wait, outside every lock.
                                                let turn = stop.turn("hierarchy").await;
                                                let me = view_digest_view.held_path_name().unwrap_or_else(|| me.clone());
                                                // A claim this admin's door wins from here on, during the turn included, ends the
                                                // turn's wait: the attempt it opens is the one the fabric's change names.
                                                let door = claim_door.get().cloned();
                                                let mut claimed = door.as_ref().map(|d| Box::pin(d.claimed.notified()));
                                                if let Some(c) = claimed.as_mut() {
                                                    c.as_mut().enable();
                                                }
                                                // The current Build, read once per turn. A builds store that cannot be read is
                                                // named once per distinct error and decides nothing this turn: no planned births,
                                                // accepted meshes or step are taken from a read that failed.
                                                // Every step of the turn that needs it is handed this read (the fabric change's among
                                                // them): no step reads it again, and nothing holds it past the turn.
                                                let read = adapter.current().await;
                                                let current = match &read {
                                                    Ok(c) => {
                                                        unreadable_named = None;
                                                        Some(c)
                                                    }
                                                    Err(e) => {
                                                        let text = e.to_string();
                                                        if unreadable_named.as_deref() != Some(text.as_str()) {
                                                            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "hierarchy", node = %me, error = %text)
                                                                .in_scope(|| tracing::info!("the current Build could not be read: this turn takes no planned births, accepted meshes or step from it"));
                                                            unreadable_named = Some(text);
                                                        }
                                                        None
                                                    }
                                                };
                                                // The current Build's planned births are what a mesh's readiness is counted over.
                                                if let Some(Some(b)) = current {
                                                    records.set_planned(&b.topology);
                                                }
                                                // Every mesh the Build names, its section held here or not (Q284).
                                                let named = current.and_then(|c| c.as_ref()).map(|b| b.topology.mesh_names()).unwrap_or_default();
                                                let adopted_ready = backbone.adopted_mesh_status().is_some_and(|h| h.status == rdm_transport::topology::MESH_READY);
                                                records.set_heard_ready(
                                                    records
                                                        .planned()
                                                        .keys()
                                                        .chain(named.iter())
                                                        .chain(std::iter::once(&mesh))
                                                        .filter(|m| {
                                                            topology_view.mesh_status(m).is_some_and(|h| h.status == rdm_transport::topology::MESH_READY)
                                                                || (**m == mesh && adopted_ready)
                                                        })
                                                        .cloned()
                                                        .collect(),
                                                );
                                                // A MeshStatus Dead is bound to the mesh id its publisher's own live digest names.
                                                records.set_heard_dead(topology_view.mesh_statuses().into_iter().filter(|(_, h)| h.status == "dead").map(|(m, h)| (m, h.publisher, h.changed_at_rafka_ms)).collect(), |mesh, publisher| {
                                                    topology_view.digest_view.all().into_iter().find(|d| d.node.name.mesh == mesh && d.node.name.to_string() == publisher).and_then(|d| d.mesh_id)
                                                });
                                                // Decide on the view as it is now: a cached view can predate the seat record this admin holds
                                                // (a hand-over just taken), and a seat is announced from the decision, never from a stale one.
                                                // The seat records are read once per decision: the projection elects from them and
                                                // the announcement below supersedes only them (`seat_announcements`).
                                                // Read under the view's write lock, as the projection always was: the installed
                                                // view is never older than the records it replaces.
                                                let mut decided = crate::election::Incumbency::default();
                                                let t = records
                                                    .install_read(&me, &topology, &view_digest_view, &view_fabric_id, |reads| {
                                                        decided = incumbency_of(&view_digest_view, &records);
                                                        project_decided(&view_fabric, &view_fabric_id, view_provider, &view_digest_view, &records, &decided, reads)
                                                    })
                                                    .await;
                                                // The accepted Build is the authority on which meshes the fabric has: the view
                                                // takes no old mesh topology of a mesh it does not name, and a mesh it does not name
                                                // that has no member left keeps no recorded id (a name added again is a new mesh).
                                                if let Some(Some(b)) = current {
                                                    topology_view.digest_view.set_accepted_meshes(b.topology.mesh_names());
                                                    let forgot = {
                                                        let mut meshes = records.meshes.lock().unwrap();
                                                        let before = meshes.len();
                                                        meshes.retain(|name, _| b.topology.names_mesh(name) || t.members().any(|n| &n.mesh == name));
                                                        meshes.len() < before
                                                    };
                                                    if forgot {
                                                        records.inputs_moved();
                                                    }
                                                }
                                                let mesh_primary = t.is_mesh_primary(&mesh, &seat_node_id);
                                                // The mesh's rows on the shared root follow its mesh-primary, and a mesh with no
                                                // primary's follow the fabric-primary (`MeshRowsWriter`, `crate::node_rows::writes_for`).
                                                let written: std::collections::BTreeSet<String> = t
                                                    .members()
                                                    .map(|n| n.mesh.clone())
                                                    .filter(|m| *m != mesh && crate::node_rows::writes_for(&t, &seat_node_id, m))
                                                    .chain(mesh_primary.then(|| mesh.clone()))
                                                    .collect();
                                                writes_tx.send_if_modified(|held| std::mem::replace(held, written.clone()) != written);
                                                let fabric_primary = t.is_fabric_primary(&seat_node_id);
                                                // A role taken or given up this turn is held from the anti-entropy until its
                                                // seat is announced below: a role's first mesh topology follows its seat record.
                                                let announcing = if (mesh_primary, fabric_primary) != (was_mesh_primary, was_fabric_primary) { Some(roles.write().await) } else { None };
                                                // The declarations held before this admin wrote rows (Q59 = x) are its first write
                                                // after it takes a seat that writes them.
                                                // Taking the mesh-primary role opens the read of the accepted Build from the fabric-primary; the
                                                // fabric-primary itself reads none (it is the read's source); giving the role up ends it (Q276).
                                                if mesh_primary && !was_mesh_primary && !fabric_primary {
                                                    build_read_owed = true;
                                                } else if !mesh_primary || fabric_primary {
                                                    build_read_owed = false;
                                                }
                                                let took_a_writing_seat = (mesh_primary && !was_mesh_primary) || (fabric_primary && !was_fabric_primary);
                                                was_mesh_primary = mesh_primary;
                                                backbone.set_mesh_primary(mesh_primary);
                                                backbone.set_fabric_primary(fabric_primary);
                                                if took_a_writing_seat {
                                                    records.write_held_declarations().await;
                                                }
                                                // Taking the fabric-primary role while the fabric is degraded adopts degraded: the
                                                // new holder authors no ready on a topology reading.
                                                if fabric_primary && !was_fabric_primary {
                                                    let adopted = topology_view.fabric_status().is_some_and(|s| s.status == "degraded");
                                                    records.set_adopted_degraded(adopted);
                                                }
                                                // The atlas covers the meshes this admin is primary for (Q207): every mesh its view and
                                                // the current Build name as the fabric-primary, its own as a mesh-primary, none else. A
                                                // mesh this admin writes holds its writer's rows, read once from the store as its writing
                                                // was taken (storage-whole-records.md); any other mesh taken (the fabric-primary's start or
                                                // hand-over) is filled once from its node records (Q204); a mesh handed away is narrowed off.
                                                atlas.writing(written.clone());
                                                let scope: std::collections::BTreeSet<String> = if fabric_primary {
                                                    t.members().map(|n| n.mesh.clone()).chain(current.and_then(|c| c.as_ref()).map(|b| b.topology.mesh_names().into_iter().collect::<Vec<_>>()).unwrap_or_default()).collect()
                                                } else if mesh_primary {
                                                    std::iter::once(mesh.clone()).collect()
                                                } else {
                                                    Default::default()
                                                };
                                                match atlas.scope(&*atlas_rows, scope.clone()).await {
                                                    Ok((taken, handed)) if taken.is_empty() && handed.is_empty() => {}
                                                    filled => {
                                                        let (taken, handed) = filled.as_ref().map(|(t, h)| (t.join(","), h.join(","))).unwrap_or_default();
                                                        tracing::info_span!("rdm.node_admin.fabric.update.via-atlas-filled", node = %me, meshes = %scope.iter().map(String::as_str).collect::<Vec<_>>().join(","), taken = %taken, handed = %handed, fabric_primary, mesh_primary, outcome = if filled.is_ok() { "filled" } else { "fill-refused" }, detail = %filled.err().unwrap_or_default(), "otel.kind" = "internal")
                                                            .in_scope(|| tracing::info!("the atlas holds the node records of the meshes this admin is primary for: those it writes from its writer, each other read once as it was taken"));
                                                    }
                                                }
                                                if !scope.is_empty() {
                                                    // A birth first heard in one of its meshes this admin does not write (another
                                                    // mesh-primary's self-healing among them) has its record read once into the atlas
                                                    // (Q197, Q203); a mesh it writes holds its writer's rows.
                                                    let births: Vec<(rdm_core::NodeId, rdm_core::IncarnationId)> = t.births().filter(|n| scope.contains(&n.mesh) && !written.contains(&n.mesh)).filter_map(|n| Some((n.node_id.clone(), n.incarnation_id.clone()?))).collect();
                                                    let read = atlas.read_new_births(&*atlas_rows, births).await;
                                                    if read > 0 {
                                                        tracing::info_span!("rdm.node_admin.fabric.update.via-atlas-births-read", node = %me, records = read as u64, "otel.kind" = "internal")
                                                            .in_scope(|| tracing::info!("the records of births first heard are the atlas's"));
                                                    }
                                                }
                                                if fabric_primary {
                                                    records.set_published_degraded(false);
                                                    crate::investigate::restore_peer_recovery(&me, &records, &t);
                                                } else {
                                                    // The seat is released in one place and one order: the published `degraded` is
                                                    // held before the authority records it replaces are cleared, so no projection
                                                    // between the two answers ready for a fabric the holder published degraded.
                                                    records.set_published_degraded(topology_view.fabric_status().is_some_and(|s| s.status == "degraded"));
                                                    records.clear_peer_recoveries();
                                                    records.set_adopted_degraded(false);
                                                }
                                                was_fabric_primary = fabric_primary;
                                                // The fabric-primary's change through the fabric states, one non-blocking step per turn
                                                // of this loop; every other admin serves the state the holder published.
                                                fabric_states.change_step(crate::fabric_status::ChangeInputs { is_fabric_primary: fabric_primary, heard: topology_view.fabric_status().as_ref().map(|f| f.status.as_str()), view: &t, current: &read }).await;
                                                // The election's result this admin holds when a primary changes: the mesh-primary
                                                // first, then the fabric-primary (R-A2); its digest carries it and the rows record it.
                                                for (seat, holder) in seat_announcements(&t, &mesh, (&seat_node_id, &seat_incarnation), &decided) {
                                                    backbone.hold_primary(seat, holder);
                                                }
                                                if let Some(announced) = announcing {
                                                    drop(announced);
                                                    roles_moved.notify_one();
                                                }
                                                let heard = topology_view.digest_view.current(topology_view.digest_view.staleness_floor());
                                                let status_of = |s: crate::model::ScopeStatus| serde_json::to_value(s).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
                                                // A status is sent when it changed, never per turn (gossip.md §3.2).
                                                let mesh_status = t.meshes.iter().find(|m| m.name == mesh).map(|m| status_of(m.status)).unwrap_or_default();
                                                // The fabric's status as the live records state it: a checklist that just cleared the adopted
                                                // degraded must not be answered by the older projection `t`.
                                                // The fabric-primary publishes the state it holds now, not the projection's last refresh:
                                                // every state the change enters is its own frame.
                                                let held_state = records.fabric_state();
                                                let fabric_status = |records: &Records| {
                                                    let basis = if fabric_primary { held_state.scope() } else { t.fabric.status };
                                                    match basis {
                                                        crate::model::ScopeStatus::Degraded | crate::model::ScopeStatus::Ready => {
                                                            status_of(if records.adopted_degraded() || records.peer_recovering() { crate::model::ScopeStatus::Degraded } else { crate::model::ScopeStatus::Ready })
                                                        }
                                                        other => status_of(other),
                                                    }
                                                };
                                                let degraded_meshes = |records: &Records| -> Vec<(String, Option<u64>)> {
                                                    records.peer_recoveries().into_iter().map(|r| (r.mesh, r.verdict_rafka_ms)).collect()
                                                };
                                                let mut announced = backbone.announce_statuses_with_degraded_meshes(&mesh_status, &fabric_status(&records), false, false, &degraded_meshes(&records)).await;
                                                // While the read of the accepted Build is owed, the checklist is neither counted from the facts
                                                // held before it nor completed: the turn sends the read (or takes its answer) and counts on the
                                                // next turn's read of the Build log, which then holds the fabric-primary's Build (Q276).
                                                let mut build_read_now = false;
                                                if build_read_owed {
                                                    if build_read.absorbed().await {
                                                        build_read_owed = false;
                                                    } else {
                                                        build_read.tick(Some(&crate::fetch_build::NodeAdminNotReady::NoCurrentBuild { me: me.clone(), wanted: None })).await;
                                                    }
                                                    build_read_now = true;
                                                }
                                                // A status adopted from the previous publisher is republished only when its checklist is
                                                // complete: the planned births checked in, every mesh primary reported.
                                                if build_read_now {
                                                    // The read is owed or was answered this turn: nothing is counted, nothing ended.
                                                } else if announced.mesh_awaiting_checklist || announced.fabric_awaiting_checklist {
                                                    // An unreadable builds store steps no checklist: the adopted status waits, named above.
                                                    if let Some(current) = current {
                                                        let planned = current.as_ref().map(|b| crate::checklist::planned(&b.topology, &mesh));
                                                        // The checklist a new primary runs before it republishes the status it adopted (R-S3).
                                                        let done = fabric_states.adopted_step(announced, crate::fabric_status::AdoptedInputs { view: &t, planned, digest_view: &topology_view.digest_view });
                                                        if done.mesh || done.fabric {
                                                            announced = backbone.announce_statuses_with_degraded_meshes(&mesh_status, &fabric_status(&records), done.mesh, done.fabric, &degraded_meshes(&records)).await;
                                                        }
                                                    }
                                                } else {
                                                    fabric_states.adopted_end();
                                                }
                                                // The state just announced is the one the change may move on from.
                                                if records.set_published_fabric_state((fabric_primary && !announced.fabric_awaiting_checklist).then_some(held_state)) && fabric_primary {
                                                    fabric_states.poke_change();
                                                }
                                                records.set_declare_gate(mesh_primary && !announced.mesh_awaiting_checklist);
                                                // R-S2: a mesh primary the fabric-primary's view gained or changed is sent the down op.
                                                fabric_states.address_changed_mesh_primaries(&t, fabric_primary);
                                                let addr = rdm_transport::topology::gossip_addr;
                                                let admins: Vec<_> = heard.iter().filter(|d| d.node.name.kind == NodeKind::NodeAdmin && d.node.name != me).filter_map(addr).collect();
                                                backbone.join_admins(admins).await;
                                                // The turn dials no member of its own mesh: a member joins through this admin, and a
                                                // held member gone stale is dialled by §6's repair alone (gossip.md §6, §8; Q84).
                                                drop(turn);
                                                // The next turn is due after one gossip interval, or at once when what the turn
                                                // decides on moved: a checklist the fabric-primary waits on; an attempt claimed; or the view changes a member's status or a primary,
                                                // its mesh's or a node type's (this admin's own roles among them), or a mesh's status.
                                                // The fabric is ready once every planned mesh is, and a
                                                // status change is sent at once (states.md; gossip.md §3.2: timers only for
                                                // heartbeat and anti-entropy).
                                                let decides = |v: &Topology| (v.members().map(|n| (n.name.clone(), n.status, n.is_primary, n.is_fabric_primary)).collect::<Vec<_>>(), v.meshes.iter().map(|m| (m.name.clone(), m.status)).collect::<Vec<_>>());
                                                let decided_on = decides(&t);
                                                let publication_wake = tokio::time::sleep_until(tokio::time::Instant::now() + if build_read_now && !build_read_owed { std::time::Duration::ZERO } else { gossip_interval() });
                                                tokio::pin!(publication_wake);
                                                loop {
                                                    tokio::select! {
                                                        _ = &mut publication_wake => break,
                                                        _ = build_read.request_ended(), if build_read_owed => break,
                                                        _ = fabric_states.change_due() => break,
                                                        _ = records.wake.report_notify.notified() => break,
                                                        _ = async { match claimed.as_mut() { Some(c) => c.as_mut().await, None => std::future::pending().await } } => break,
                                                        _ = records.view_changed.notified() => {
                                                            let now = topology.read().await.clone();
                                                            if decides(&now) != decided_on || announced.mesh_awaiting_checklist || announced.fabric_awaiting_checklist || records.peer_recovering() {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    // The mesh topology anti-entropy (gossip.md §3.2, "PERPETUAL TIMERS … mesh topology anti-entropy —
                                    // the mesh primary on the backbone; also sent at once on a material change"): its own
                                    // timer, never a step of the hierarchy's turn. It reads the view and the roles the
                                    // hierarchy decided, nothing else, so no store read or write of the turn (the
                                    // current Build, the exit rows, the view's installation, the change) delays
                                    // it: a peer's t of this mesh (mesh-investigation.md) is the publication's own
                                    // interval, never the store's. While this admin is its mesh's primary it publishes
                                    // its mesh's members on the backbone (and puts the same source into its own mesh);
                                    // no admin publishes a live mesh on its behalf (R-RX4). While it is the fabric
                                    // primary it publishes each deleted mesh whose last admin it stopped or deleted
                                    // (mesh-delete.md step 7). Held at its own wait while the admin is stopped.
                                    {
                                        // `mesh.block`'s current mesh topology of this admin's mesh (storage.md §2 step 1).
                                        let (backbone, topology_view) = (backbone.clone(), topology_view.clone());
                                        let _ = storage_changes.aggregate.set(Arc::new(move || {
                                            let (backbone, topology_view) = (backbone.clone(), topology_view.clone());
                                            Box::pin(async move {
                                                backbone.publish(&topology_view, backbone.own_members(&topology_view)).await;
                                            })
                                        }));
                                    }
                                    {
                                        let (backbone, topology_view, stop) = (backbone.clone(), topology_view.clone(), stop_gate.clone());
                                        anti_entropy = tokio::spawn(async move {
                                            loop {
                                                let turn = stop.turn("anti-entropy").await;
                                                {
                                                    let _announced = roles.read().await;
                                                    backbone.publish(&topology_view, backbone.own_members(&topology_view)).await;
                                                }
                                                drop(turn);
                                                tokio::select! {
                                                    _ = backbone.publication_wake(&topology_view, tokio::time::Instant::now() + rdm_transport::topology::backbone_gossip_interval()) => {}
                                                    _ = roles_moved.notified() => {}
                                                }
                                            }
                                        });
                                    }
                                    // The projection, refreshed into the control plane's topology.
                                    {
                                        let (topology, fabric, fabric_id, provider, digest_view, records) = (control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, digest_view.clone(), records.clone());
                                        let elections = ElectionLog::new(name.clone());
                                        let me = name.clone();
                                        // Not spawned behind the gate: the rebuild takes the view's lock, which every handler of a
                                        // stopped admin reads (its start among them). It is held at its own wait instead
                                        // (`view_rebuild_due`), where it holds nothing.
                                        let rebuild_gate = stop_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut held: BTreeSet<PathName> = BTreeSet::new();
                                            let mut wake = ViewWake::new(&digest_view, &records);
                                            let mut turn = rebuild_gate.turn("view-rebuild").await;
                                            let mut woken = "start";
                                            loop {
                                                wake.seen();
                                                let me_now = digest_view.held_path_name().unwrap_or_else(|| me.clone());
                                                let installed = *records.view_tick.borrow();
                                                let t = records.install_read(&me_now, &topology, &digest_view, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &digest_view, &records, reads)).await;
                                                // Each rebuild names what made it due: the count of these per admin is the
                                                // rebuild's work, and every one has an event before it (Q254).
                                                tracing::info_span!("rdm.node_admin.topology.update.via-view-rebuild", node = %me_now, woken, changed = *records.view_tick.borrow() != installed)
                                                    .in_scope(|| tracing::info!("the view is rebuilt from the digest view and the records"));
                                                elections.observe_as(&me_now, &t);
                                                // A name this view held and now lacks: the exclusion is named from the same
                                                // inputs at the moment it happens, not reconstructed later.
                                                let now: BTreeSet<PathName> = t.members().map(|n| n.name.clone()).collect();
                                                for gone in held.difference(&now) {
                                                    tracing::info_span!("rdm.node_admin.topology.update.via-vanished-from-view", node = %me_now, name = %gone, why = %describe_absence(&digest_view, &records, gone))
                                                        .in_scope(|| tracing::info!("a name this admin's view held is excluded by the projection now"));
                                                }
                                                held = now;
                                                drop(turn);
                                                (turn, woken) = view_rebuild_due(&mut wake, &rebuild_gate).await;
                                            }
                                        }));
                                    }
                                    // The peer-mesh investigation (`crate::investigate`): while this admin is the fabric primary,
                                    // a peer mesh it stops hearing on the backbone is probed through its own members and recovered
                                    // only when one of them cannot reach its own node-admin. Its decision is what releases the
                                    // drift check below to replace that mesh's node-admins.
                                    let ladder = Arc::new(Mutex::new(crate::investigate::Ladder::new(crate::investigate::Rungs::configured())));
                                    {
                                        // The R-D1 target as the Build door and the drive read it: t from this admin's last direct
                                        // backbone receipt of the mesh, whether or not the ladder has stepped the mesh yet.
                                        let (ladder, digest_view) = (ladder.clone(), digest_view.clone());
                                        let _ = control.peer_mesh.set(Arc::new(move |mesh: &str| ladder.lock().unwrap().peer_mesh_unheard(mesh, digest_view.mesh_unheard(mesh, std::time::Instant::now()))));
                                    }
                                    {
                                        let held = connections.held();
                                        let watch = crate::investigate::Watch {
                                            gate: stop_gate.clone(),
                                            me: name.clone(),
                                            me_id: node_id.clone(),
                                            interval: rdm_transport::topology::backbone_gossip_interval(),
                                            topology_view: topology_view.clone(),
                                            topology: control.topology.clone(),
                                            records: records.clone(),
                                            settled: { let builds = builds_dyn.clone(); Arc::new(move || { let builds = builds.clone(); Box::pin(async move { builds.current().await.map(|c| c.is_some_and(|b| !matches!(b.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running))).map_err(|e| e.to_string()) }) }) },
                                            client: node_rpc.client.clone(),
                                            connected: Arc::new(move |id: &NodeId| held.lock().unwrap().own_latest_directs().iter().any(|d| &d.destination.node_id == id && d.state == rdm_core::connections::ConnectionState::Connected)),
                                            ladder: ladder.clone(),
                                            carriers: Mutex::new(BTreeMap::new()),
                                            fabric_recovery: fabric_recovery.clone(),
                                        };
                                        // Not behind the gate: it reads the view; it is held at its own wait (`Watch::gate`).
                                        tasks.push(tokio::spawn(crate::investigate::run(watch)));
                                    }
                                    let executor;
                                    // The reconcile loop: the drift check, and the fabric-primary's drive of every active Build
                                    // (`build_drive`). Only the fabric-primary drives; an executor runs what it is called with
                                    // (`build_run`) and picks nothing up from its projection. A launched admin starts it only after
                                    // its entry pull, so its first view is its launcher's. Each pass after the first runs on an
                                    // event, never a tick (Q253 amended, `ReconcileWake`).
                                    {
                                        let (topology, submitted) = (control.topology.clone(), control.build_submitted.clone());
                                        let (digest_view, records, fabric, fabric_id, provider) = (digest_view.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider);
                                        let (me, me_id, deployer, drift_builds, drift_contexts) = (name.clone(), node_id.clone(), runner.provider.clone(), builds_dyn.clone(), attempt_contexts.clone());
                                        let drift_admission = control.admission.clone();
                                        let (drives, drive_env) = (drives.clone(), drive_env.clone());
                                        let drift_time = rafka_time.clone();
                                        let drift_nodes = nodes_storage.clone();
                                        let ladder = ladder.clone();
                                        let stop = stop_gate.clone();
                                        let drift_seam = wiring.drift_seam.take();
                                        let mut wake = ReconcileWake::new(submitted, records.subscribe_view(), records.subscribe_fabric_state(), ladder.lock().unwrap().subscribe_moved(), drives.subscribe_ended(), deployer.exits());
                                        executor = tokio::spawn(async move {
                                            let mut started = HashSet::new();
                                            let mut held = HashSet::new();
                                            // Every mesh's durable runtime rows, read once as this admin takes the
                                            // fabric-primary role and held while it keeps it (`FabricTenureRows`).
                                            let mut fabric_rows = FabricTenureRows::default();
                                            loop {
                                                let woken = wake.next().await;
                                                // Each pass names the event that woke it: the count of these per admin is the
                                                // loop's work, and a pass with no event before it is a tick.
                                                tracing::info_span!("rdm.node_admin.build.update.via-reconcile-woken", node = %me, woken = woken.as_str())
                                                    .in_scope(|| tracing::debug!("the reconcile loop runs a pass"));
                                                // The drift seam's hold, awaited before the pass's turn and outside every lock, inside
                                                // a span that names what held the pass and lasts as long as the hold.
                                                if let Some(hold) = drift_seam.as_ref().and_then(|seam| seam.before_drift_pass(&me.to_string())) {
                                                    let span = tracing::info_span!("rdm.node_admin.build.update.via-drift-held", node = %me, by = %hold.by);
                                                    async {
                                                        hold.released.await;
                                                        tracing::info!("the reconcile pass was held by the drift seam and runs now");
                                                    }
                                                    .instrument(span)
                                                    .await;
                                                }
                                                // Held here while the admin is stopped: its own wait, outside every lock.
                                                let turn = stop.turn("executor").await;
                                                // Decide on, and plan from, the view as it is now: a cached view
                                                // can predate the members that make another admin primary.
                                                let now = records.install_read(&me, &topology, &digest_view, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &digest_view, &records, reads)).await;
                                                // Every Build whose next operation this admin executes. The
                                                // fabric authority first starts a reconciliation Build for
                                                // proven drift.
                                                {
                                                    let durable_rows = fabric_rows.for_pass(now.is_fabric_primary(&me_id), &*drift_nodes, &me).await;
                                let opened =                     reconcile_drift(&drift_time, &me, &me_id, &now, &digest_view, &*deployer, &*drift_builds, &drift_contexts, &drift_admission, durable_rows, &mut started, &mut held, &|mesh| ladder.lock().unwrap().peer_mesh_unheard(mesh, digest_view.mesh_unheard(mesh, std::time::Instant::now())).and_then(|p| p.unheard_ms), &|mesh| (digest_view.backbone_meshes().contains(mesh) || now.members().any(|n| n.mesh == mesh)) && !ladder.lock().unwrap().recovery_decided(mesh), &|mesh| ladder.lock().unwrap().recovery_decided(mesh), &|mesh| ladder.lock().unwrap().unreachable_decided(mesh)).await;
                                                    // The attempt is planned from the view as it is now.
                                                    if opened.is_some() {
                                                        records.install_read(&me, &topology, &digest_view, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &digest_view, &records, reads)).await;
                                                    }
                                                    if now.is_fabric_primary(&me_id) {
                                                        crate::build_op::ensure_active(&drives, &drive_env).await;
                                                    }
                                                }
                                                drop(turn);
                                            }
                                        });
                                    }
                                    // The stopped-node garbage collector (node-identity-and-state.md §6.9): a sweep every ttl, by the
                                    // fabric-primary only.
                                    // A blank ttl runs no sweep. The sweep submits one ordinary node.delete Build and deletes nothing.
                                    if let Some(ttl) = cfg.stopped_node_ttl {
                                        let control = control.clone();
                                        let stop = stop_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                tokio::time::sleep(ttl).await;
                                                // Held here while the admin is stopped: its own wait, outside every lock.
                                                let turn = stop.turn("stopped-sweep").await;
                                                let swept = control.sweep_stopped(Some(ttl)).await;
                                                tracing::debug!(?swept, "stopped-node sweep");
                                                drop(turn);
                                            }
                                        }));
                                    }
                                    *out.lock().unwrap() = Some(Running { rafka_time: rafka_time.clone(), api_base, control, runner, topology_view, digest, node_rpc, backbone: backbone.clone(), router: iroh_router, rpc_server: rpc_server.clone(), publisher, executor, runs: runs.clone(), hierarchy, anti_entropy, tasks, start_tasks, leave_seam: wiring.leave_seam.take(), builds: builds.clone(), pre_node_ready_cancel, pre_node_ready: pre_node_ready_cell.clone(), member_cert, gate: stop_gate.clone(), node_self: own_commands.get().cloned().expect("this admin's command surface is set before it runs"), status_door: status_door.clone(), status_door_set: status_door_set.clone(), data_connections: data_connections.clone() });
                                    Ok::<_, String>(())
                                })
                            });
                            Ok::<_, String>((parts, Some(started)))
                        })
                    },
                );
                Ok::<_, String>((channel, joined_seam))
            })
        })
    };
    let mut start = crate::workflows::node_start::FirstStart { types: join_types, listeners: Vec::new(),
        starting: Some(starting),
        joining: Some(joining),
        resolve: Some(resolve),
        digest: join_digest,
        enter: Some(enter),
        joined: None,
        hook_started: None,
    };
    crate::workflows::node_start::node_start(&mut start, crate::node_hooks::StartKind::FirstStart).await?;
    let running = out.lock().unwrap().take();
    running.ok_or_else(|| "the start ended without the node-admin it brought up".to_string())
}
