//! Running node-admin handle, RPC server wiring, and boot sequence.

use super::*;

/// A running node-admin: what `run` needs to keep alive and shut down.
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
    /// The admin's membership.
    pub membership: Membership,
    /// The admin's own digest.
    pub digest: Arc<Mutex<MeshDigest>>,
    /// This process's one Node RPC client and live resolver: every Node RPC
    /// caller in the process takes it by clone.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// This admin's place on the backbone: while it leaves, its aggregate publication carries its
    /// own `Leaving` out of its mesh ([`Running::leave`]).
    pub(crate) backbone: Backbone,
    pub(crate) router: IrohRouter,
    /// This admin's Node RPC server: the leave waits for the replies it owes before it closes the transport.
    pub(crate) rpc_server: rdm_transport::NodeRpcServer,
    /// The digest cadence, the Build executor, the hierarchy and the anti-entropy:
    /// stopped first on leave.
    pub(crate) publisher: tokio::task::JoinHandle<()>,
    pub(crate) executor: tokio::task::JoinHandle<()>,
    pub(crate) hierarchy: tokio::task::JoinHandle<()>,
    /// The Members anti-entropy: this admin's mesh aggregate on the backbone, on its own timer.
    pub(crate) anti_entropy: tokio::task::JoinHandle<()>,
    pub(crate) tasks: Vec<tokio::task::JoinHandle<()>>,
    /// The tasks node.start's steps after the hook started (the own-mesh sweep of a launched admin).
    pub(crate) start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// The gate a stop parks (`node_self`'s `Session::cut`) and a start opens.
    pub(crate) gate: crate::stop_gate::StopGate,
    /// This admin as the subject of the drain, stop and start its mesh-admin commands.
    pub(crate) node_self: Arc<crate::node_self::NodeSelf>,
    /// Asked before each leave announcement publish (a testkit executable's wiring; none in the product).
    pub(crate) leave_seam: Option<Arc<dyn crate::wiring::LeaveSeam>>,
    /// This admin's Build log: fenced before the first `Leaving` (ruling R-A2).
    pub(crate) builds: Arc<FabricBuildStateAdapter>,
    /// Cancelled when this admin is retired (stop-node, leaving): its hook and any pull
    /// it has in flight end.
    pub(crate) before_ready_cancel: rdm_transport::Retirement,
    pub(crate) before_ready: Arc<Mutex<Option<crate::before_ready::BeforeReadyHandle>>>,
    /// The member cert the launching admin's signer issued for this birth, as the opaque bytes the
    /// join that admitted it carried (empty when that admin is configured with no certs). `None`
    /// for a birth that made no JoinNode: the Day-0 root and a recovery start.
    pub member_cert: Option<Vec<u8>>,
    /// This admin's status authority, once it holds a view.
    pub(crate) authority: crate::status_rpc::AuthoritySlot,
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

    /// The gate a stop parks and a start opens: every task this admin runs is held at its own wait
    /// while it is parked.
    pub fn gate(&self) -> &crate::stop_gate::StopGate {
        &self.gate
    }

    /// Whether this admin publishes its mesh's aggregate on the backbone: the role its hierarchy
    /// last decided (its mesh's primary, its view authorizing).
    pub fn publishes_its_mesh(&self) -> bool {
        self.backbone.is_mesh_primary()
    }

    /// This admin as the subject of `node.connections.delete`, `stop-node` and `start-node`: what such a command
    /// naming it reaches once its Status authority has decided it (`status_rpc`'s `serve_command`).
    pub fn node_self(&self) -> Arc<crate::node_self::NodeSelf> {
        self.node_self.clone()
    }

    /// This admin's status authority, once it holds a view: what decides the declarations of its
    /// mesh's births while it holds the mesh's seat.
    pub fn status_authority(&self) -> Option<Arc<crate::status_rpc::StatusAuthority>> {
        self.authority.get().cloned()
    }

    /// Where this admin's `before_ready` stands; `None` when the app registered no hook.
    pub fn before_ready(&self) -> Option<crate::before_ready::BeforeReadyHandle> {
        self.before_ready.lock().unwrap().clone()
    }

    /// Resolves with the reason once this admin's hook failed; never resolves otherwise. The
    /// process selects on it beside its stop signal and ends by name.
    pub async fn before_ready_failed(&self) -> String {
        let handle = self.before_ready.lock().unwrap().clone();
        match handle {
            Some(h) => h.failed().await,
            None => std::future::pending().await,
        }
    }

    /// Stop executing and reconciling: no Build attempt and no drift
    /// recovery starts after this. A fabric shutdown does this first, so the
    /// runtimes it stops are not recovered as proven drift.
    pub fn stop_reconciling(&self) {
        self.executor.abort();
    }

    /// Stop on this process's signal (i146 Q55; node-stop.md steps 2-4), this admin its own subject: the
    /// one stop sequence an rpc node's signal runs ([`crate::node_self::NodeSelf::stop_on_signal`]). The
    /// Build executor stops first (an attempt it was running is continued by the Build's next attempt).
    /// Then the whole `node.connections.delete { All }` under the birth's bound, its connections-deleting
    /// owed to its mesh-primary; its tasks held; its last aggregate before the cut carrying itself
    /// Leaving when it is its mesh's primary; `node.mesh.leave`, its own mesh and the backbone left and
    /// its Node RPC pool cut; its `Stopped` owed to its mesh-primary (node-rpc.md §43), waited on by
    /// nothing. Then the process end ([`Self::leave_with`]): every task aborted, each call this node
    /// answered settled with its caller, the transport closed; the owed declarations end with the
    /// process (node-rpc.md §34).
    pub async fn leave(self) {
        self.before_ready_cancel.cancel();
        self.executor.abort();
        let stopped = self.node_self.stop_on_signal().await;
        self.leave_with(false).await;
        for owed in stopped.declarations {
            owed.abort();
        }
    }

    /// End a parked birth (a `stop-node` completed): it says nothing and closes. Its stop already
    /// cut its gossip neighbours and left its topics, and a stopped node gossips nothing
    /// (node-stop.md step 4): every node holds it Leaving and parked from the `NodeStopped` its
    /// stopped reply produced.
    pub async fn leave_after_stop(self) {
        self.before_ready_cancel.cancel();
        self.leave_with(true).await
    }

    pub(crate) async fn leave_with(self, _parked: bool) {
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
        let span = tracing::info_span!("rdm.mesh.node.update.via-leave-shutdown", node = %me, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
        // Every call this node answered is settled with its caller before the transport closes:
        // a close takes an unacknowledged reply down with the connection.
        self.rpc_server.settled().instrument(span.clone()).await;
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

/// The Node RPC server a node-admin serves, before it seals: core (Ping, Forward), then the families the
/// admin owns as an authority (status, build claim, join).
pub fn rpc_server(
    connections: Arc<crate::connections_writer::ConnectionsWriter>,
    client: Arc<rdm_transport::NodeRpcClient>,
    authority: crate::status_rpc::AuthoritySlot,
    claim: crate::build_claim::ClaimSlot,
    join: crate::join::JoinSlot,
) -> rdm_transport::ServerBuilder {
    let core = rdm_transport::ServerBuilder::new().serve_core(client, Some(connections));
    // Status is forwardable: a peer may carry an authority's status call through this admin.
    crate::join::serve(crate::build_claim::serve(crate::status_rpc::serve(core, authority), claim), join).carry::<rdm_core::status::Status>()
}

/// The unreadable pre-event steps already named: each is named once, not on every fact.
pub(crate) type NamedSteps = HashSet<(String, u32, String, String)>;

/// Apply the lifecycle overlays `facts` name open to `book`: the restarts as the facts' truth
/// (`DigestBook::restarts_from_facts`: open while named open, closed only by its own `Complete`
/// receipt, its attempt's `Converged` verdict or its Build's forgetting), every other open operation
/// as a hold that its attempt's terminal verdict or its Build's forgetting closes
/// (`DigestBook::deletions_from_facts`), and every pre-event step this build cannot read refused by
/// name, once. Run inside the
/// log's append as each fact enters it ([`crate::build_state::LocalBuildLog::derive_on_every_fact`]).
pub(crate) fn overlays_from_facts(facts: &[crate::build_state::BuildFact], book: &DigestBook, named: &mut NamedSteps) {
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
        book.set_accepted_meshes(b.topology.meshes.keys().cloned().collect());
    }
    let in_flight = crate::build_state::in_flight_ops(facts, &builds);
    book.restarts_from_facts(&in_flight.ops, &in_flight.restarts_completed, &in_flight.attempts_converged, &crate::build_state::forgotten_builds(facts, &builds));
    for op in in_flight.ops.into_iter().filter(|o| !o.is_restart()) {
        book.deleting(op);
    }
    book.deletions_from_facts(&in_flight.removals_ended, &crate::build_state::forgotten_builds(facts, &builds));
    for u in in_flight.unrecognised {
        if named.insert((u.build_id.to_string(), u.attempt, u.operation.clone(), u.step.clone())) {
            tracing::info_span!("rdm.node_admin.build.reject.via-unrecognised-step", build_id = %u.build_id.log(), attempt = u.attempt, operation = %u.operation, step = %u.step, reason = %u.reason)
                .in_scope(|| tracing::warn!("a lifecycle pre-event step this build cannot read opens no overlay"));
        }
    }
}

/// Bring a node-admin up: identity, policy, membership, Build state, the
/// control API, the projection and the executor.
#[cfg(feature = "file-backend")]
pub async fn start(cfg: AdminConfig) -> Result<Running, String> {
    // `start` carries no hook and no wrapper, so it is an embedding with no application work in state-sync.
    start_with(cfg, crate::wiring::Wiring { fabric_hooks: crate::wiring::FabricHooks::no_app_work(), ..crate::wiring::Wiring::no_certs() }).await
}

/// The app's protocols ([`crate::wiring::Wiring::alpns`]) on this admin's endpoint, registered like
/// every node's: a `ConnectionsRegistry` over the admin's connections log and live resolver, and each
/// protocol's handler routed, so RDM owns its accept loop and `node.connections.admit` runs before
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
/// from, on RDM standalone's storage backend ([`crate::storage_backend::file_backend`]).
#[cfg(feature = "file-backend")]
pub async fn start_with(cfg: AdminConfig, wiring: crate::wiring::Wiring) -> Result<Running, String> {
    let storage = crate::storage_backend::file_backend(cfg.store_dir.clone());
    start_on(cfg, wiring, storage).await
}

/// [`start_with`] on `storage`: every durable read and write of the admin goes through the six
/// boundaries it opens.
pub async fn start_on(cfg: AdminConfig, wiring: crate::wiring::Wiring, storage: Arc<dyn crate::storage_backend::StorageBackend>) -> Result<Running, String> {
    // One boot trace per birth: this root is never entered (iroh's tasks must not inherit it), each
    // step is a child that lives exactly as long as the step, and `via-ready` closes it.
    let boot = tracing::info_span!(parent: None, "rdm.mesh.node.create.via-boot", node = tracing::field::Empty, node_id = tracing::field::Empty, incarnation_id = tracing::field::Empty, kind = "node-admin");
    let started = start_booted(cfg, wiring, storage, boot.clone()).await;
    if let Err(e) = &started {
        boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
    }
    started
}

pub(crate) async fn start_booted(mut cfg: AdminConfig, mut wiring: crate::wiring::Wiring, storage: Arc<dyn crate::storage_backend::StorageBackend>, boot: tracing::Span) -> Result<Running, String> {
    // Every task this admin runs is spawned behind this gate: a stop parks it, and a start opens it again.
    let park_gate = crate::stop_gate::StopGate::new();
    // The `sync_state` choice is made on purpose: an embedding with no application work says so
    // (`SyncState::NoAppWork`); one that registered nothing would skip its day 0 and seed unnoticed.
    if wiring.fabric_hooks.sync_state.is_none() {
        return Err("refusing to start: no `sync_state` choice was made; pass SyncState::NoAppWork for an embedding with no application work in state-sync, or the application's sync_state hook".into());
    }
    let fabric_hooks = wiring.fabric_hooks.clone();
    // The embedding's own round action, if it binds one: run after this admin publishes its
    // scratchpad at commit-state (bound once the store root is open, below).
    let embedding_round_actions = wiring.round_actions.take();
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
    let key_file = cfg.data_dir.join("node-key");
    let key_held = crate::blocking::blocking_io(move || key_file.try_exists()).await.map_err(|e| format!("{}: {e}", cfg.data_dir.join("node-key").display()))?;
    let step = if key_held {
        tracing::info_span!(parent: &boot, "rdm.mesh.node.resolve.via-identity-loaded")
    } else {
        tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-identity-minted")
    };
    let key = load_or_mint_key(&cfg.data_dir).await?;
    drop(step);
    let storage_step = tracing::info_span!(parent: &boot, "rdm.mesh.node.resolve.via-storage-opened", data_dir = %cfg.data_dir.display(), store_dir = %cfg.store_dir.display(), fabric_id = tracing::field::Empty, inherited_root = tracing::field::Empty, writes_to = tracing::field::Empty, mesh_id = tracing::field::Empty, error = tracing::field::Empty);
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
    let scope = crate::storage_backend::StorageScope { data_dir: cfg.data_dir.clone(), fabric_id: cfg.fabric_id.clone(), own_mesh: own_mesh.clone(), writer: incarnation.clone() };
    let crate::storage_backend::StorageBoundaries { fabric: fabric_backed, meshes: mesh_storage, nodes: nodes_storage, status: status_storage, connections: open_connections, builds: journal, round_actions, cut_off_writes, writes_to } = storage.open(&scope, embedding_round_actions).await?;
    storage_step.record("writes_to", writes_to.as_str());
    let fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage> = crate::wiring::apply(wiring.fabric_storage.take(), fabric_backed.clone());
    let attempt_contexts = Arc::new(crate::build_claim::AttemptContexts::on(journal.clone()));
    // Each mesh's in-flight row (R-BI7 caveat 3): the one set of rows the claim door, the drive's
    // release and the recovery door all take and release.
    let mesh_claims = mesh_storage.clone();
    // A launched node-admin starts holding the runtime rows its launcher held (`CreateRequest::
    // held_runtimes`): handed in its data dir beside the launch record, put in its own nodes storage
    // before anything reads it.
    if cfg.launch.is_some() {
        for row in crate::deployment::provider::held_runtimes_in(&cfg.data_dir).await.map_err(|e| format!("refusing to start: {e}"))? {
            nodes_storage.put_runtime(&row).await.map_err(storage_err)?;
        }
    }
    let inherited = match (&cfg.launch, own_row) {
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
    if inherited.is_none() {
        let holds = if fabric_storage.fabric().await.map_err(storage_err)?.is_some() {
            Some("a Fabric record")
        } else if !mesh_storage.meshes().await.map_err(storage_err)?.is_empty() {
            Some("a mesh record")
        } else if !nodes_storage.contacts().await.map_err(storage_err)?.is_empty() || !nodes_storage.runtimes().await.map_err(storage_err)?.is_empty() {
            Some("a durable topology")
        } else {
            None
        };
        // A node-admin a launcher starts is handed its rows before it runs (runtimes): a launch
        // owes no clean data dir.
        refuse_contradictory_start(&cfg, holds.filter(|_| cfg.launch.is_none())).map_err(|e| format!("refusing to start: {e}"))?;
    }
    drop(storage_step);
    // A container fabric's node-admin is a host process its containers reach only through the
    // fabric network's gateway (a host address, reachable from host processes too): the provider is
    // prepared before the mesh endpoint binds, and Day 0 binds it on the gateway. On an inherited
    // root, the provider also proves the inherited birth exited before `take_inherited_name` writes
    // its row `Deleted` (fabric-recovery.md "Nothing alive"). A launched admin learns the policy from
    // its launcher's entry answer, after it binds. Explicit bindings are held to the provider's image
    // before the provider does anything.
    if let (Some(b), None, Ok(p)) = (&cfg.bindings, &cfg.launch, FabricPolicy::bootstrap(cfg.spawn_type.as_deref())) {
        b.check_image(provider_image(p.provider)).map_err(|e| format!("refusing to start: explicit executable bindings: {e}"))?;
    }
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
    // key of the inherited row, with a new port (fabric-recovery.md: "The relaunched node-admin is a new
    // node over its inherited data dir"). A new node supersedes no birth (node-restart.md: a restart
    // alone keeps the node.id and incarnation). Its bootstrap contacts are the births it last heard,
    // its own Mesh first: hints, any one of which answering is enough.
    let (name, node_id, incarnation, supersedes, mesh_addr, control_addr, seeds) = match (&cfg.launch, &inherited) {
        (Some(l), _) => {
            let control = cfg.api_bind;
            (l.name.clone(), l.node_id.clone(), incarnation.clone(), l.supersedes.clone(), l.bind_addr, control, l.seeds.clone())
        }
        (None, Some(own)) => {
            let node_id = NodeId::mint();
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
            );
            // The inherited birth's node.delete is node recovery's node.delete (fabric-recovery.md "Nothing
            // alive": "Every node the store names, the old process on the inherited data dir included, is
            // confirmed gone by the provider, never by a wait", "node.delete of the exited instance, then
            // node.create at the same path.name, with a new node.id over the inherited data dir"): it requires
            // the provider's exact exit proof on the old process before writing `Deleted`.
            let provider = match &early {
                Some((_, p)) => p.as_ref(),
                None => return Err(format!("refusing to start: {}: no deployment provider prepared for the inherited birth {} at {}", cfg.data_dir.display(), own.node_id, own.name)),
            };
            let own_runtime = nodes_storage.runtime(&own.node_id, &own.incarnation_id).await.map_err(storage_err)?;
            exit_proof_of(provider, &None, own_runtime.as_ref())
                .instrument(step.clone())
                .await
                .map_err(|e| format!("refusing to start: {}: {} cannot delete the inherited birth {} ({}) at {}: {e}", cfg.data_dir.display(), node_id, own.node_id, own.incarnation_id.as_str(), own.name))?;
            // The inherited birth's node.delete, then this node's claim of its path.name: the first writes
            // of this birth (`take_inherited_name`). A name another node holds refuses the start by name.
            crate::node_rows::take_inherited_name(&*nodes_storage, own, &node_id)
                .instrument(step.clone())
                .await
                .map_err(|e| format!("refusing to start: {}: {} takes {} over the inherited birth {}: {e}", cfg.data_dir.display(), node_id, own.name, own.node_id))?;
            let addr_of = |s: &str| own.listeners.iter().find(|(n, _)| n == s).map(|(_, a)| *a);
            let mut contacts = nodes_storage.contacts().await.map_err(storage_err)?;
            contacts.sort_by_key(|c| (c.name.mesh != own.name.mesh, c.name.to_string()));
            let seeds = contacts.iter().filter_map(|c| c.gossip_addr().and_then(|a| a.ip_addrs().next().map(|ip| (a.id.to_string(), *ip)))).collect();
            step.record("contacts", contacts.len());
            step.in_scope(|| tracing::info!("fabric recovery: a new node over the inherited data dir, at its path.name"));
            (
                own.name.clone(),
                node_id,
                incarnation.clone(),
                None,
                // The inherited endpoint key and a fresh port (the operating system assigns it), never the
                // one the inherited birth held: peers learn the new address from its digest.
                SocketAddr::new(own.transport_addr.ip(), 0),
                addr_of("control").unwrap_or(cfg.api_bind),
                seeds,
            )
        }
        (None, None) => {
            let name = PathName::new(&cfg.mesh, NodeKind::NodeAdmin, 1);
            let node_id = NodeId::mint();
            // Day 0 (R-OW1 item 2): the bootstrap admin, its mesh's primary, claims its own path.name by
            // insert-and-fail, the first write of its birth, before Ready (`claim_day0_name`). A name another
            // node holds refuses the start by name. A recovery start (both primary flags) is no Day 0: its row
            // is written when it holds its mesh's seat, as every member's is (`NodeRows::arrivals`).
            if !cfg.mesh_primary && !cfg.fabric_primary {
                crate::node_rows::claim_day0_name_in_boot(&boot, &*nodes_storage, &name, &node_id)
                    .await
                    .map_err(|e| format!("refusing to start: {}: Day 0's {node_id} claims {name}: {e}", cfg.data_dir.display()))?;
            }
            (name, node_id, incarnation.clone(), None, SocketAddr::new(cfg.api_bind.ip(), 0), cfg.api_bind, cfg.seeds.clone())
        }
    };

    // This node's own connections rows: `connections/<its path.name>/` of the store root (R14).
    let connections_storage: Arc<dyn crate::storage::ConnectionsStorage> = open_connections(name.clone()).await.map_err(storage_err)?;
    boot.record("node", tracing::field::display(&name));
    boot.record("node_id", tracing::field::display(&node_id));
    boot.record("incarnation_id", tracing::field::display(&incarnation.as_str()));
    // A host-process admin binds on the gateway; an admin that itself runs in a container of the
    // fabric binds on its own address there (its control address), the gateway not being local.
    let mesh_addr = match (&early, &cfg.launch, &inherited) {
        (Some((p, _)), None, None) if p.provider.kind() == ProviderKind::Container && rdm_core::runtime::this_container_id().is_none() => SocketAddr::new(p.admin_ip, 0),
        _ => mesh_addr,
    };

    // The mesh endpoint: gossip for membership and Build facts.
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
    // This admin's own connections: hydrated from its storage, then kept by what its client
    // observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    let connections = Arc::new(crate::connections_writer::ConnectionsWriter::new(
        rdm_core::connections::ConnectionEnd { name: name.clone(), node_id: node_id.clone(), incarnation: Some(incarnation.clone()) },
        connections_storage.clone(),
        Arc::new(std::sync::Mutex::new(rdm_core::connections::ConnectionsHeld::new())),
    ));
    match connections.hydrate().await {
        Ok(n) => tracing::info!(node = %name, rows = n, "connections hydrated from storage"),
        Err(e) => return Err(format!("connections storage: {e}")),
    }
    // The process's one live-node resolver: the client's and the server's (an accepted
    // connection's peer is named by it).
    let node_rpc_resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
    connections.held().lock().unwrap().set_membership(node_rpc_resolver.clone());
    let authority: crate::status_rpc::AuthoritySlot = Arc::new(std::sync::OnceLock::new());
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
    // The takeover this admin holds because the store could not commit it: written by its handover
    // door, read by its fabric.get.
    let held_takeover: crate::fabric_handover::HeldTakeovers = Default::default();
    // The process's one Node RPC client is made before the server: core Forward is one direct inner
    // call through it, and every other caller in the process takes it by clone.
    // One delivery state for the process (node-rpc.md §43): its birth mints every frame's identity,
    // and its client and its server share the windows and the open invocations.
    let delivery = rdm_transport::Delivery::new(rdm_core::framing::Birth { node_id: node_id.clone(), incarnation: incarnation.clone() });
    let node_rpc = crate::node_rpc::ProcessNodeRpc::new(node_rpc_resolver.clone(), endpoint.clone()).with_delivery(delivery.clone());
    // Closed until the Status authority is filled and its view holds this admin (it decides from
    // that view): the router refuses every op, ping included, with a typed NotReady before then.
    let rpc_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rpc_builder = crate::build_op::serve(crate::fabric_handover::serve(crate::build_facts_read::serve(crate::topology_read::serve(rpc_server(connections.clone(), node_rpc.client.clone(), authority.clone(), claim_slot.clone(), join_slot.clone()), topology_slot.clone()), build_facts_slot.clone()), handover_slot.clone()), build_slot.clone());
    // The app's own ops (each behind the gate of its hook), before the catalog seals.
    let rpc_builder = match wiring.serve_app.take() {
        Some(serve_app) => serve_app(rpc_builder),
        None => rpc_builder,
    };
    let rpc_server = rpc_builder
        .with_ready_gate(rpc_ready.clone())
        .with_delivery(delivery)
        .seal(rdm_transport::ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .map_err(|e| format!("the admin's protocol catalog refused to seal: {e:?}"))?;
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.add.via-alpn-registered", node = %name, alpns = "node-rpc,gossip");
    let router_builder = IrohRouter::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .accept(rdm_transport::ALPN, rpc_server.clone());
    // The app's protocols on this endpoint: RDM's accept loop and `node.connections.admit` for each,
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
    // No crate of RDM serves a route or opens an HTTP listener (rdm-crate-and-test-layout.md §2): the
    // admin's application contact (`admin_api_base`, config, never checked) names this admin's node id.
    let _ = control_addr;
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
    // The mesh's id names its membership channel: the launch names it (the
    // launching admin always does), else the join answer's projection knows
    // it, else this admin is the mesh's first and mints it.
    // The process's one Node RPC client, built before the join: the join is its first call, and
    // the membership feed starts once the process holds a book.
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
        config: BTreeMap::new(),
        load: None,
        gossip: None,
        data_dir: Some(cfg.data_dir.display().to_string()),
    });
    let joining = match &joins_through {
        Some((_, launcher, _)) => crate::workflows::node_start::Joining::Authority { label: launcher.name.to_string() },
        None if inherited.is_some() => crate::workflows::node_start::Joining::None(crate::before_ready::NoAuthority::InheritedRoot),
        None if cfg.mesh_primary || cfg.fabric_primary => crate::workflows::node_start::Joining::None(crate::before_ready::NoAuthority::Recovery),
        None => crate::workflows::node_start::Joining::None(crate::before_ready::NoAuthority::Day0Root),
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
    // What a repeat of the control entry calls: the join to the launching admin, as first made.
    let entry_retrieval: Option<(rdm_transport::NodeTarget, String, MeshDigest)> = match (&joins_through, &join_digest) {
        (Some((anchor, launcher, _)), Some(digest)) => Some((rdm_transport::NodeTarget::ExactNode(launcher.node_id.clone()), anchor.id.fmt_short().to_string(), digest.clone())),
        _ => None,
    };
    // What the start brought up, once it did: handed to the caller.
    let out: Arc<Mutex<Option<Running>>> = Arc::default();
    let starting = crate::workflows::node_start::Starting { root: boot.clone(), name: name.to_string(), path: name.clone(), node_id: node_id.clone(), client: node_rpc.client.clone(), rafka_time: rafka_time.clone() };
    // The admin plugs into `node.start` by continuations: each step runs in the scope the previous
    // one left. Its channel step also fills its stores; its installed-topology step takes its
    // entry and builds what it serves and elects with; after its hook its Ready check reports.
    let enter: crate::workflows::node_start::EnterSeam<String> = {
        let out = out.clone();
        Box::new(move |step: tracing::Span, joined: Option<crate::wire::JoinAnswer>| -> crate::workflows::node_start::BoxFut<Result<(Membership, crate::workflows::node_start::JoinedSeam<String>), String>> {
            Box::pin(async move {
                // The member cert the launching admin's signer issued for this birth, carried by the join that
                // admitted it; none for a birth that made no JoinNode (the Day-0 root, a recovery start).
                let member_cert: Option<Vec<u8>> = joined.as_ref().map(|a| a.control.member_cert.clone());
                if let Some(cert) = &member_cert {
                    data_connections.set_member_cert(cert.clone());
                }
                // The fabric's Day-0 root has no authority above it: it adopts its own OS clock as rafka-time,
                // once, here. A recovery start (a flag or an inherited root) takes it from the nodes it reaches below.
                let mut sole_node_root = false;
                if cfg.launch.is_none() && inherited.is_none() && !cfg.mesh_primary && !cfg.fabric_primary {
                    crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_DAY0_ROOT);
                    sole_node_root = true;
                }
                let mesh_id = match (&cfg.mesh_id, &joined) {
                    (Some(id), _) => id.clone(),
                    (None, Some(answer)) => return Err(format!("{name}: a launched admin takes its mesh's id from its launch, and this launch names none (joined {})", answer.served_by)),
                    (None, None) => MeshId::mint(),
                };
                // Subscribe first, pull second: what changes during the pull arrives by
                // gossip, and the book keeps the newer copy.
                step.record("mesh_id", tracing::field::display(&mesh_id));
                step.record("seeds", seed_addrs.len());
                let membership = Membership::join(&gossip, &endpoint, &cfg.fabric_id, &cfg.mesh, &mesh_id, &name.to_string(), clock.clone(), seed_addrs.clone(), &wiring.types.iter().map(String::as_str).collect::<Vec<_>>())
                    .await
                    .map_err(|e| format!("membership: {e}"))?;
                membership.set_author(&rdm_core::PublisherId { node: name.to_string(), node_id: node_id.clone(), incarnation: incarnation.clone() });
                // The name this admin holds is its membership's (R12): its connections rows name it as their
                // source from here on, whatever name the join hands it or an `ApplyNodeName` gives it.
                {
                    let (held, birth) = (membership.clone(), name.clone());
                    connections.follow_held_name(move || held.held_name(&birth))?;
                }
                // The row's name and config a launched admin's join handed: held before it publishes anything
                // (R-JN1); at a first start the name is its create spec's, verified by the join.
                let handed: Option<(Option<PathName>, Option<BTreeMap<String, String>>)> = joined.as_ref().map(|a| (a.control.name.clone(), a.control.config.clone()));
                if let Some((n, c)) = &handed {
                    if let Some(n) = n {
                        membership.set_name(n.clone());
                    }
                    if let Some(c) = c {
                        membership.set_config(c.clone());
                    }
                }
                tracing::info_span!(
                    parent: &step,
                    "rdm.node.mesh.update.via-joined",
                    node = %membership.node_name(),
                    node_id = %node_id,
                    mesh = %cfg.mesh,
                    mesh_id = %mesh_id,
                    which = rdm_core::status::WhichMesh::Own.name(),
                )
                .in_scope(|| tracing::info!("joined the mesh channel"));
                drop(step);
                // An admin that made no `JoinNode` and is no Day-0 root (an inherited root, or a recovery
                // started with a seed) holds no rafka-time yet: when every node the store names — the old
                // process on the inherited data dir included (confirmed gone by the provider before
                // `take_inherited_name` above) — is confirmed gone by the provider, never by a wait, this
                // admin is the only node, the fabric-primary, and its own clock is rafka-time from the
                // start (fabric-recovery.md "Nothing alive"; origin-ordering.md). If any node answers, it is
                // not this case and this admin takes rafka-time from the existing fabric-primary.
                if !rafka_time.is_adopted() {
                    let mut contacts = Vec::new();
                    for r in nodes_storage.contacts().await.map_err(storage_err)?.into_iter().filter(|r| r.node_id != node_id) {
                        let gone = match (&early, nodes_storage.runtime(&r.node_id, &r.incarnation_id).await.map_err(storage_err)?) {
                            (Some((_, provider)), row) => exit_proof_of(provider.as_ref(), &None, row.as_ref()).await.is_ok(),
                            (None, _) => false,
                        };
                        if gone {
                            membership.book.seats.mark_gone(r.incarnation_id);
                        } else {
                            contacts.push(crate::reenter::MapNode { node_id: r.node_id, name: r.name, endpoint_id: r.endpoint_id, transport_addr: r.transport_addr, incarnation: r.incarnation_id, transition: None, ready: false, data_dir: None, admin_api_base: None });
                        }
                    }
                    if contacts.is_empty() && (inherited.is_some() || seed_addrs.is_empty()) {
                        crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_NOTHING_ALIVE);
                        sole_node_root = true;
                    } else {
                        crate::rafka_time::adopt_from_reachable(&rafka_time, &name, &node_rpc.client, &node_rpc_resolver, &membership, contacts, &boot)
                            .await
                            .map_err(|e| format!("refusing to start: {e}"))?;
                    }
                }
                let backbone = Backbone::join(&gossip, &endpoint, &membership, &cfg.mesh, rdm_core::PublisherId { node: name.to_string(), incarnation: incarnation.clone(), node_id: node_id.clone() }, seed_addrs.clone())
                    .await
                    .map_err(|e| format!("backbone: {e}"))?;
                tracing::info_span!(
                    parent: &boot,
                    "rdm.node.mesh.update.via-joined",
                    node = %membership.node_name(),
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
                // 2): Day 0 writes the Fabric identity (below), and every other admin reads it there. A store
                // root holding another Fabric's identity is refused. A shutdown already held there (a restart or
                // a join during one) is in force from the start.
                let fabric_identity = crate::fabric_storage::FabricIdentity { fabric_id: cfg.fabric_id.clone(), name: cfg.fabric.clone() };
                if let Some(held) = fabric_storage.fabric().await.map_err(|e| e.to_string())? {
                    if held.fabric_id != cfg.fabric_id {
                        return Err(format!("fabric.storage holds the identity of Fabric {} ({}), this admin is configured for Fabric {}", held.fabric_id, held.name, cfg.fabric_id));
                    }
                }
                let mut tasks_early: Vec<tokio::task::JoinHandle<()>> = Vec::new();
                // When the fabric seat moves to another holder, this admin, if it is a mesh primary, pulls
                // rafka-time once from the new fabric-primary.
                tasks_early.push(park_gate.spawn(crate::rafka_time::run_seat_move_pull(rafka_time.clone(), name.clone(), node_id.clone(), node_rpc.client.clone(), membership.clone())));
                // A delta that does not follow what this admin holds desynchronizes that source of its mesh
                // channel; it tops up from its Mesh's own primary (gossip.md §3.3) with `GetTopology` of each
                // such mesh, as every node does. Held behind the gate: a parked admin tops up nothing, and a
                // gap signalled while it was parked is topped up once it runs again.
                tasks_early.push(park_gate.spawn(membership.top_ups(crate::topology_read::top_up_fetch(node_rpc.client.clone()))));
                // The seat-transfer rows on the store root: this admin enters knowing who holds every seat a
                // committed node.primary.update moved, its own included (proposal Part C1 failure contract:
                // a holder that died after its write cut-off reads here whether its row committed).
                crate::fabric_handover::enter_seats(&*fabric_storage, &membership, &backbone, &name, &node_id).await?;
                if let Some(own) = &inherited {
                    membership.book.seats.mark_gone(own.incarnation_id.clone());
                }
                // The current Build: Day 0 accepts the first Build itself; every other admin fetches it from
                // its entry pull or the fabric control topic.
                let accepted = Arc::new(AcceptedStore::new(fabric_storage.clone(), name.to_string()));
                let records = Arc::new(Records::default());
                records.set_me(node_id.clone());
                // Only the fabric seat holder writes the Fabric rows, and only its held `fabric/` rows are published: the
                // file store reads the committed fabric seat record, as the Build gate does.
                fabric_backed.set_seat_source(fabric_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name));
                {
                    if inherited.is_some() {
                        // Recovery on an inherited root (fabric-recovery.md): load the existing Build journal once
                        // before locking `journal` to `membership.book.seats`, so `builds_dyn.current()` during boot
                        // (before the hierarchy loop seats this birth) holds the accepted Build and preserves stored MeshIds.
                        let _ = journal.current_build().await;
                    }
                    let seat_for_holds = fabric_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name);
                    // One fact gets one source (R-BI6): the seat record held in `membership.book.seats`.
                    let holds_fabric = Arc::new(move || matches!((seat_for_holds.read)(), crate::fabric_builds::SeatRecord::Open { holder: Some(_) }));
                    let mesh_seat_for_holds = mesh_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name, &own_mesh);
                    let holds_mesh = Arc::new(move || matches!((mesh_seat_for_holds.read)(), crate::fabric_builds::SeatRecord::Open { holder: Some(_) }));
                    // Each lane is written under the revision of its seat's record this birth holds
                    // (Q28, Q35): the fabric seat's for `fabric/builds/`, a mesh's primary seat's for
                    // `meshes/<mesh>/builds/`.
                    let (lane_book, lane_records, lane_me) = (membership.book.clone(), records.clone(), (node_id.clone(), incarnation.clone()));
                    let revisions: crate::build_state::LaneRevisions = Arc::new(move |lane: &crate::build_state::LaneId| {
                        let incumbency = incumbency_of(&lane_book, &lane_records);
                        match lane {
                            crate::build_state::LaneId::Fabric => crate::seat_watch::lane_seat("the fabric seat record", incumbency.fabric.as_ref(), &lane_me, &incumbency.lost),
                            crate::build_state::LaneId::Mesh(m) => crate::seat_watch::lane_seat(&format!("mesh {m}'s primary seat record"), incumbency.meshes.get(m), &lane_me, &incumbency.lost),
                        }
                    });
                    journal.hold_seats(crate::build_state::JournalSeats { holds_fabric, holds_mesh, revisions, seals: crate::fabric_storage::seals_owed(fabric_storage.clone(), rdm_core::handover::HandoverBirth { mesh: own_mesh.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() }) });
                }
                // `fabric/` has one writer, the fabric-primary (R14 item 2): `FabricRowsWriter`, a round on every
                // seat change.
                {
                    let seats = membership.seats().clone();
                    let mut writer = FabricRowsWriter::new((node_id.clone(), incarnation.clone()), fabric_identity.clone(), fabric_storage.clone());
                    let mut changed = seats.subscribe();
                    let park = park_gate.clone();
                    tasks_early.push(tokio::spawn(async move {
                        loop {
                            // Held here while the admin is parked: its own wait, outside every lock.
                            let turn = park.turn("seat-rows").await;
                            writer.round(&seats).await;
                            drop(turn);
                            if changed.changed().await.is_err() {
                                return;
                            }
                        }
                    }));
                }
                // The status rows of this admin's mesh (`status.storage`): written by
                // the mesh's seat holder, folded into what this admin applied when it takes the seat.
                if let Some(own) = &inherited {
                    let _ = status_storage
                        .put_node_exit(&crate::status_storage::NodeExitRow {
                            node_id: own.node_id.clone(),
                            name: own.name.clone(),
                            incarnation_id: own.incarnation_id.clone(),
                            exit_code: None,
                            proof: "inherited-root-provider-exit-proof".into(),
                            observed_at_ms: 0,
                        })
                        .await;
                }
                records.set_status(status_storage.clone(), own_mesh.clone(), name.clone(), node_id.clone());
                // The ready-for-traffic event's row among this mesh's status rows is the durable fact that the fabric
                // first reached ready-for-traffic (states.md, "The initial fabric before ready-for-traffic"): an
                // admin booting on a root that holds it knows it whatever status it hears next (a resize puts the
                // fabric back to pending). The Fabric row is published at the first state-commit, before it.
                records.first_ready.read_from(&*status_storage, &cfg.fabric_id, None).await.map_err(storage_err)?;
                records.set_book(membership.book.clone());
                let builds = Arc::new(
                    FabricBuildStateAdapter::join(&gossip, &membership.location_book(), &cfg.fabric_id, seed_addrs.clone(), journal.clone(), accepted.clone(), name.to_string(), fabric_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name), wiring.catch_up_seam.take())
                        .await
                        .map_err(|e| e.to_string())?
                        .with_mesh_seat(mesh_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name, &own_mesh)),
                );
                // The Build state every consumer after the fetch shares: the control routes, the executor, the
                // pipelines and the drift check.
                let runs = Arc::new(crate::build_run::AttemptRuns::default());
                let builds_dyn: Arc<dyn BuildStateAdapter> = Arc::new(crate::build_run::FramedBuilds::new(crate::wiring::apply(wiring.builds.take(), builds.clone()), runs.clone()));

                // Fabric policy and the first view. The bootstrap admin takes its policy
                // from MESH_SPAWN_TYPE. A launched admin takes its entry from the admin
                // that launched it (its first seed) over QUIC: that admin's topology
                // projection and the membership it is projected from, recorded as heard,
                // so this admin's first view is its launcher's; the fabric record carries
                // the policy. Without that answer it cannot know the policy, and it
                // refuses to start by name.
                let entry_floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>> = Arc::default();
                let _ = build_facts_slot.set(Arc::new(crate::build_facts_read::BuildFactsDoor { me: name.clone(), builds: builds_dyn.clone(), floor: entry_floor.clone() }));
                // The topology this admin enters its mesh from without an authority (its durable map), and
                // where it came from; a launched admin's comes from node.start's topology step, after its hook.
                let mut entry_map: Option<(&'static str, Vec<crate::reenter::MapNode>)> = None;
                let channel = membership.clone();
                let joined_seam: crate::workflows::node_start::JoinedSeam<String> = Box::new(
                    move |_: Option<crate::workflows::node_start::JoinThrough>| -> crate::workflows::node_start::BoxFut<Result<(crate::workflows::node_start::HookParts, Option<crate::workflows::node_start::HookStarted<String>>), String>> {
                        Box::pin(async move {
                            let policy = match joined {
                                Some(answer) => {
                                    // Fetch the current Build from the launching admin, and record the attempt floor its
                                    // answer names; without them this admin is not started. The Build it names arrives on
                                    // the Build topic, or is fetched from a node-admin that holds it (`FetchBuildFacts`,
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
                                        let build_id = builds.current_build().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.into_build_id().map(|b| b.log().to_string()).unwrap_or_default();
                                        tracing::info_span!(parent: &boot, "rdm.node_admin.fabric.update.via-inherited-root", node = %name, build_id = %build_id)
                                            .in_scope(|| tracing::info!("the current Build reloaded from the inherited data dir's builds store"));
                                    } else if cfg.fabric_primary || inherited.is_some() {
                                        tracing::info_span!("rdm.node_admin.fabric.update.via-recovery", node = %name, fabric_id = %cfg.fabric_id, mesh_id = %mesh_id)
                                            .in_scope(|| tracing::info!("a fabric recovery accepts no Build of its own: the current Build is taken from the nodes it reaches"));
                                    } else if matches!(builds.current_build().await, Ok(crate::build_state::CurrentBuild::None)) {
                                        let b0 = crate::build_state::BuildAccepted {
                                            build_id: crate::build::BuildId::mint(),
                                            topology: crate::accepted::FabricTopology::root(&cfg.fabric, &cfg.mesh, mesh_id.clone()),
                                            submitted_change: None,
                                            submitted_at_ms: rafka_time.now_ms(),
                                        };
                                        // The fabric-primary claims the Fabric's identity at Day 0, into its scratchpad until
                                        // the fabric's first state-commit (node-identity-and-state.md §2.8; fabric-create.md).
                                        if let Err(held) = claim_fabric_name(&*fabric_storage, &fabric_identity, &node_id).await? {
                                            return Err(format!("refusing to start: this admin is configured for Fabric {}: {held}", cfg.fabric_id));
                                        }
                                        attempt_contexts.put(&b0.build_id, 1, &crate::build_claim::current_context()).await.map_err(|e| format!("attempt-context: {e}"))?;
                                        crate::build_state::accept(&*builds, &b0, None, &name.to_string(), "day-0").await.map_err(|e| e.to_string())?;
                                    }
                                    FabricPolicy::bootstrap(cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?
                                }
                            };

                            if inherited.is_some() && cfg.launch.is_none() {
                                // An admin started on an inherited root has no maker: its durable map is the topology.
                                let mut rows = nodes_storage.contacts().await.map_err(storage_err)?;
                                rows.retain(|r| r.node_id != node_id);
                                if let Ok(map) = crate::reenter::get_topology(crate::reenter::TopologySource::DurableMap(&rows)).await {
                                    entry_map = Some(("durable-map", map));
                                }
                            }
                            let entering_existing_mesh = entry_map.as_ref().is_some_and(|(_, m)| m.iter().any(|n| n.name.mesh == cfg.mesh && n.node_id != node_id));
                            // Whether this admin enters an existing mesh: from its durable map here, or from the maker's
                            // topology at node.start's topology step. The Ready check waits for that entry's sweep.
                            let entering = Arc::new(std::sync::atomic::AtomicBool::new(entering_existing_mesh));

                            if let Some(b) = &cfg.bindings {
                                b.check_image(provider_image(policy.provider)).map_err(|e| format!("refusing to start: explicit executable bindings: {e}"))?;
                            }

                            // A restart that enters its mesh from its durable map elects nobody until its entry read has
                            // told it who holds the seats.
                            records.set_entering(entering_existing_mesh && cfg.launch.is_none());
                            let _ = records.contacts.set(nodes_storage.clone());
                            // A fabric primary recovered from its data dir holds its durable topology as its map (rule 5):
                            // every birth the accepted Build names that nodes.storage heard is in its view as not yet
                            // reached, until membership or the sweep says more. Its view is never an empty one that plans
                            // a create for every member.
                            if inherited.is_some() && cfg.launch.is_none() {
                                let desired = builds_dyn.current().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.map(|b| b.topology);
                                for r in nodes_storage.contacts().await.map_err(storage_err)? {
                                    if r.node_id != node_id && desired.as_ref().is_some_and(|t| t.contains(&r.name)) {
                                        let m = crate::reenter::MapNode { node_id: r.node_id, name: r.name, endpoint_id: r.endpoint_id, transport_addr: r.transport_addr, incarnation: r.incarnation_id, transition: None, ready: false, data_dir: None, admin_api_base: None };
                                        if let Some(res) = m.resolved() {
                                            node_rpc_resolver.apply(res, None);
                                        }
                                        records.publish(m.as_node(NodeStatus::PendingReconnect, policy.provider));
                                    }
                                }
                            }
                            // The meshes the accepted Build names keep the ids this admin stored for them.
                            if let Some(named) = builds_dyn.current().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.map(|b| b.topology) {
                                for m in mesh_storage.meshes().await.map_err(storage_err)?.into_iter().filter(|m| !m.dead && named.meshes.contains_key(&m.name)) {
                                    records.meshes.lock().unwrap().insert(m.name, m.mesh_id);
                                }
                            }
                            records.meshes.lock().unwrap().insert(cfg.mesh.clone(), mesh_id.clone());
                            let book = membership.book.clone();
                            let node_rpc_feed = node_rpc.feed(&book, &name.to_string());
                            let control = Arc::new(
                                ControlPlane::new(builds_dyn.clone(), accepted.clone(), name.clone(), project(&cfg.fabric, &cfg.fabric_id, policy.provider, &book, &records), rafka_time.clone()).with_contexts(attempt_contexts.clone()).with_first_ready(records.first_ready.clone(), status_storage.clone()).with_claims(mesh_claims.clone()),
                            );
                            // The claim door: this admin decides a fabric-lane claim only while it holds the fabric-primary
                            // seat, and its own mesh's drift-lane claim only while it holds its mesh's primary seat (R-BI7).
                            let claim_door = Arc::new(crate::build_claim::ClaimDoor { me: name.clone(), node_id: node_id.clone(), topology: control.topology.clone(), builds: builds_dyn.clone(), contexts: attempt_contexts.clone(), claims: mesh_claims.clone() });
                            let _ = claim_slot.set(claim_door.clone());
                            {
                                let (book, records) = (book.clone(), records.clone());
                                let _ = control.absence.set(Arc::new(move |name: &PathName| describe_absence(&book, &records, name)));
                            }
                            {
                                let (me, book, records, fabric, fabric_id, provider) = (name.clone(), book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider);
                                let _ = control.view_now.set(Arc::new(move || {
                                    let (me, book, records, fabric, fabric_id) = (me.clone(), book.clone(), records.clone(), fabric.clone(), fabric_id.clone());
                                    Box::pin(async move {
                                        let reads = ViewReads::read(&me, &book, &records, &fabric_id).await;
                                        project_with(&fabric, &fabric_id, provider, &book, &records, &reads)
                                    })
                                }));
                            }
                            let _ = control.connections.set(connections.held());
                            {
                                let book = book.clone();
                                let _ = control.loads.set(Arc::new(move || book.current(book.staleness_floor()).into_iter().filter(|d| d.load.is_some() || d.gossip.is_some()).map(|d| (d.node.node_id.to_string(), (d.load, d.gossip))).collect()));
                            }
                            // Where this admin's own `before_ready` stands: written by its start (and a rejoin's),
                            // read by the Ready check and by a probe naming this admin.
                            let before_ready_cell: crate::status_rpc::BeforeReadyCell = Arc::default();
                            // This admin's re-publish of its presence, for a probe naming it (the round's down op): filled once its
                            // digest exists, below.
                            let republish: crate::status_rpc::Republish = Arc::new(std::sync::OnceLock::new());
                            // This admin holding the config it is told (`ApplyNodeConfig`): filled with the republish, below.
                            let configure: crate::status_rpc::Configure = Arc::new(std::sync::OnceLock::new());
                            // This admin taking the name its row gives it (`ApplyNodeName`): filled with the republish, below.
                            let rename: crate::status_rpc::Rename = Arc::new(std::sync::OnceLock::new());
                            let commands = Arc::new(crate::node_commands::CommandBook::new(cfg.connections_delete_deadline));
                            let own_commands: Arc<std::sync::OnceLock<Arc<crate::node_self::NodeSelf>>> = Arc::new(std::sync::OnceLock::new());
                            let mesh_deleter: crate::mesh_delete::MeshDeleterSlot = Arc::new(std::sync::OnceLock::new());
                            let rounds_slot: Arc<std::sync::OnceLock<Arc<crate::fabric_rounds::FabricRounds>>> = Arc::new(std::sync::OnceLock::new());
                            let configs_slot: crate::node_config::NodeConfigsSlot = Arc::new(std::sync::OnceLock::new());
                            // The record the writer of this admin's mesh rows holds, shared with its config door: a config
                            // put through the door is what the node's next row carries (`crate::node_rows::WrittenRows`).
                            let written = Arc::new(crate::node_rows::WrittenRows::default());
                            let _ = configs_slot.set(Arc::new(crate::node_config::NodeConfigs::new(node_rpc.client.clone(), configure.clone(), written.clone())));
                            {
                                let records_for_ids = records.clone();
                                let _ = authority.set(Arc::new(crate::status_rpc::StatusAuthority {
                                    app_channels: Some(membership.clone()),
                                    held_takeover: held_takeover.clone(),
                                    declaration_seam: wiring.declaration_seam.clone(),
                                    command_frame_seam: wiring.command_frame_seam.clone(),
                                    operation_effects: Default::default(),
                                    stopped: Default::default(),
                                    status_storage: status_storage.clone(),
                                    nodes_storage: nodes_storage.clone(),
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
                                    rounds: rounds_slot.clone(),
                                    configs: configs_slot.clone(),
                                    republish: republish.clone(),
                                    configure: configure.clone(),
                                    before_ready: before_ready_cell.clone(),
                                    rename: rename.clone(),
                                    row_moved: {
                                        // The view is projected at once: it reads the moved row, as every admin's next
                                        // projection does (`ViewReads`), so it holds nothing of the move itself.
                                        let moved: crate::status_rpc::RowMoved = Arc::new(std::sync::OnceLock::new());
                                        let project = view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone());
                                        let _ = moved.set(Arc::new(move |_from: PathName, _node_id: NodeId, _incarnation: IncarnationId, _to: PathName| project()));
                                        moved
                                    },
                                    loads: control.loads.get().cloned(),
                                    book: Some(book.clone()),
                                    first_ready: records.first_ready.clone(),
                                    builds: control.builds.clone(),
                                }));
                            }
                            // The Node RPC door: opened by the first view installed that holds this admin's exact birth
                            // once the authority above is filled, whichever task installs it.
                            {
                                let authority = authority.clone();
                                let door = ServingDoor::new(rpc_ready.clone(), (node_id.clone(), incarnation.clone()), Box::new(move || authority.get().is_some()), name.clone());
                                records.set_door(door, &*control.topology.read().await);
                            }
                            // The backbone holds the meshes each installed view names: the fabric-primary's
                            // publication of a deleted mesh reads them (mesh-delete.md step 7).
                            records.set_backbone(backbone.clone(), &*control.topology.read().await);
                            // What this admin owes its authorities, shared with the rounds: a mesh primary's completion of
                            // a fabric round is owed through it.
                            let declarer = crate::status_declare::Declarer::with_first_ready(records.first_ready.clone());
                            let _ = rounds_slot.set(crate::fabric_rounds::FabricRounds::new(crate::fabric_rounds::RoundEnv {
                                me: name.clone(),
                                node_id: node_id.clone(),
                                incarnation: incarnation.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                topology: control.topology.clone(),
                                client: node_rpc.client.clone(),
                                builds: builds_dyn.clone(),
                                contexts: attempt_contexts.clone(),
                                declarer: declarer.clone(),
                                wake: records.wake.clone(),
                                records: records.clone(),
                                hooks: fabric_hooks,
                                actions: round_actions.clone(),
                            }));
                            rounds_slot.get().expect("set above").set_gossip(backbone.clone(), membership.clone());
                            // A Build this admin accepts as the fabric-primary reaches its climb inside the acceptance:
                            // a resize puts the fabric back to pending, and the view the door's caller reads next holds it.
                            {
                                let (rounds, me, book, records, fabric, fabric_id, provider, topology) = (rounds_slot.get().expect("set above").clone(), name.clone(), book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, control.topology.clone());
                                let _ = control.resize.set(Arc::new(move || {
                                    let (rounds, me, book, records, fabric, fabric_id, topology) = (rounds.clone(), me.clone(), book.clone(), records.clone(), fabric.clone(), fabric_id.clone(), topology.clone());
                                    Box::pin(async move {
                                        if rounds.accepted().await {
                                            records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
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
                            // A node-admin this admin launches honors the same explicit bindings: its own launches and
                            // restarts run the bound executables too.
                            admin_env.extend(cfg.bindings_env.clone());
                            let data_root = cfg.data_dir.parent().map(Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone());
                            let template = LaunchTemplate {
                                fabric: cfg.fabric.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                executable: PathBuf::new(),
                                // The address this admin's mesh endpoint is bound on: where every birth it launches reaches
                                // it (loopback for a process fabric, the gateway or its own container address for a
                                // container fabric).
                                seeds: vec![(key.public().to_string(), mesh_addr)],
                                launcher: rdm_core::launch::Launcher { name: name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() },
                                env: cfg.passthrough.clone(),
                                data_root,
                                store_dir: cfg.store_dir.clone(),
                                issuing: None,
                                roots: data_connections.roots(),
                                connections_delete_deadline: cfg.connections_delete_deadline,
                            };
                            let mut registry = HookRegistry::new();
                            for (spec, hook) in wiring.hooks.drain(..) {
                                registry = registry.register(spec, hook);
                            }
                            let hooks = registry.seal().map_err(|e| format!("{e:?}"))?;
                            let handover = Arc::new(crate::fabric_handover::HandoverDoor {
                                fabric: cfg.fabric.clone(),
                                me: name.clone(),
                                node_id: node_id.clone(),
                                incarnation: incarnation.clone(),
                                topology: control.topology.clone(),
                                membership: membership.clone(),
                                backbone: backbone.clone(),
                                storage: fabric_storage.clone(),
                                client: node_rpc.client.clone(),
                                book: book.clone(),
                                records: records.clone(),
                                locks: Default::default(),
                                memo: Default::default(),
                                decisions: builds.decisions(),
                                builds: builds_dyn.clone(),
                                project_view: view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone()),
                                cut_off_writes: cut_off_writes.clone(),
                                looker: crate::seat_watch::Looker { me: name.clone(), me_id: node_id.clone(), membership: membership.clone(), client: node_rpc.client.clone(), provider: provider_dyn.clone(), nodes: Some(nodes_storage.clone()), heard_again: heard_again_slot.clone() },
                                held_takeover: held_takeover.clone(),
                            });
                            let _ = handover_slot.set(handover.clone());
                            let runner = Arc::new(AdminRunner {
                                status_storage: status_storage.clone(),
                                mesh_storage: mesh_storage.clone(),
                                fabric_storage: fabric_storage.clone(),
                                provider: provider_dyn.clone(),
                                joins: joins.clone(),
                                observer: Arc::new(MembershipObserver { book: book.clone(), client: Some(node_rpc.client.clone()), commands: commands.clone(), authority: authority.clone(), handover: handover_slot.clone(), unreachable: records.unreachable.clone() }),
                                records: records.clone(),
                                builds: builds_dyn.clone(),
                                local: journal.clone(),
                                template,
                                admin_env,
                                bin_dir: cfg.bin_dir.clone(),
                                bindings: cfg.bindings.clone(),
                                lifecycle: LifecycleTransitionPipeline::new(hooks, Arc::new(MemoryReceiptLog::default())),
                                handles: Mutex::new(HashMap::new()),
                                topology: control.topology.clone(),
                                fabric: cfg.fabric.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                fabric_provider: policy.provider,
                                book: book.clone(),
                                me: name.clone(),
                                node_id: node_id.clone(),
                                endpoint: Some(endpoint.clone()),
                                node_rpc: node_rpc.clone(),
                                lifecycle_events: crate::wiring::apply(wiring.lifecycle_events.take(), {
                                    let project = Some(view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone()));
                                    if let Some(status) = authority.get() {
                                        let _ = status.stopped.set(super::runner::stopped_publisher(membership.clone(), backbone.clone(), project.clone()));
                                    }
                                    Arc::new(GossipLifecycle { membership: membership.clone(), backbone: backbone.clone(), me: name.clone(), mesh_deleter: mesh_deleter.clone(), project, authority: authority.clone() })
                                }),
                                leaves: Arc::new(crate::mesh_delete::MeshDeleteBook::default()),
                                mesh_delete_runs: crate::mesh_delete::MeshDeleteRuns::default(),
                                handover: handover.clone(),
                                issuer: issuer.clone(),
                            });
                            let _ = runner_slot.set(runner.clone());
                            let _ = heard_again_slot.set(Arc::new(crate::heard_again::HeardAgain { client: node_rpc.client.clone(), authority: authority.clone(), builds: builds_dyn.clone(), runner: runner_slot.clone(), book: membership.book.clone() }));
                            // What the provider holds recorded at a path: the proof of a node the accepted topology names
                            // that no view and no durable runtime row holds a birth of (mesh-investigation.md step 5).
                            records.set_path_probe(runner.clone());
                            let _ = mesh_deleter.set(Arc::new(MeshDeleteDoor { runner: runner.clone() }));
                            // The Build family on `0x20`: the executor's door for `build.attempt.run` and the fabric-primary's
                            // drive of every Build. The same admin holds both; which one a call reaches depends on the seat.
                            let drives = Arc::new(crate::build_drive::Drives::default());
                            let drive_env = {
                                let exec = Arc::new(BuildExecutor { executor: name.to_string(), builds: builds_dyn.clone(), topology: control.topology.clone(), runner: runner.clone() });
                                let run_door = Arc::new(crate::build_run::RunDoor {
                                    me: name.clone(),
                                    builds: builds_dyn.clone(),
                                    exec,
                                    runs: runs.clone(),
                                    local: journal.clone(),
                                    gate: park_gate.clone(),
                                });
                                let refresh_view = view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone());
                                let env = Arc::new(crate::build_drive::DriveEnv {
                                    me: name.clone(),
                                    topology: control.topology.clone(),
                                    builds: builds_dyn.clone(),
                                    door: claim_door.clone(),
                                    dispatcher: Arc::new(crate::build_op::RpcDispatcher { me: name.clone(), local: run_door.clone(), client: node_rpc.client.clone(), topology: control.topology.clone() }),
                                    gate: Arc::new(SeatGate { membership: membership.clone(), builds: builds.clone(), control: control.clone(), handover: handover.clone(), project: refresh_view.clone() }),
                                    departure: Arc::new(ProviderDeparture {
                                        me: name.clone(),
                                        topology: control.topology.clone(),
                                        refresh: refresh_view,
                                        book: book.clone(), provider: runner.provider.clone(), nodes: nodes_storage.clone(), records: records.clone(), fabric: fabric_storage.clone() }),
                                    verdicts: Arc::new(crate::build_op::LocalVerdicts { local: journal.clone() }),
                                    turns: park_gate.clone(),
                                });
                                records.set_rows(Arc::new(crate::node_rows::NodeRows::new(name.clone(), node_id.clone(), control.topology.clone(), nodes_storage.clone()).with_configs(configs_slot.clone()).with_written(written.clone())));
                                let _ = build_slot.set(Arc::new(crate::build_op::BuildDoor { me: name.clone(), control: control.clone(), drives: drives.clone(), env: env.clone(), run: run_door, seats: membership.seats().clone(), incarnation: incarnation.clone(), incumbent: handover.clone() }));
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
                                    let a = CurrentRuntimeAdoption::begin_in(&name, &cfg.data_dir, &crate::model::DeploymentId::mint(), in_container).await.map_err(|e| e.to_string())?;
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
                            // This birth's exact runtime, recorded in its own data dir under its own key, a blind put
                            // (node-recovery.md "Detection and proof" step 3: "the exact runtime recorded at launch"):
                            // an admin started on this data dir after this birth exits proves this birth's exit from it
                            // before deleting its row, and every node-admin this one launches is handed it with the
                            // other runtime rows it holds.
                            nodes_storage
                                .put_runtime(&crate::storage::RuntimeRow { node_id: node_id.clone(), name: name.clone(), incarnation_id: incarnation.clone(), runtime: runtime.clone(), data_dir: Some(cfg.data_dir.display().to_string()) })
                                .await
                                .map_err(storage_err)?;
                            // Own digest, published on the membership cadence.
                            let digest = Arc::new(Mutex::new(MeshDigest { held_hash: membership.held_hash(),
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
                                // Stamped by `Membership::publish`: this birth's own sequence and the composed clock.
                                emitted_at_rafka_ms: 0,
                                digest_seq: 0,
                                mesh_id: Some(mesh_id.clone()),
                                in_flight: None,
                                config: handed.as_ref().and_then(|(_, c)| c.clone()).unwrap_or_default(),
                                load: None,
                                gossip: None,
                                data_dir: adoption.is_none().then(|| cfg.data_dir.display().to_string()),
                            }));
                            // nodes.storage and mesh.storage: this birth and its Mesh, so a restart on this data dir is
                            // the same logical node in the same Mesh.
                            {
                                let d = digest.lock().unwrap().clone();
                                nodes_storage
                                    .put_own(&crate::storage::NodeRecord {
                                        node_id: node_id.clone(),
                                        fabric_id: cfg.fabric_id.clone(),
                                        name: name.clone(),
                                        mesh_id: mesh_id.clone(),
                                        incarnation_id: incarnation.clone(),
                                        endpoint_id: d.node.endpoint_id.clone(),
                                        transport_addr: d.node.transport_addr,
                                        listeners: Vec::new(),
                                    })
                                    .await.map_err(storage_err)?;
                            }
                            // nodes.storage contacts and mesh.storage: this admin's mesh's members and its Mesh row on the
                            // fabric's shared root, written while this admin holds its mesh's primary seat (the writer
                            // ladder; `MeshRowsWriter`), as bootstrap hints for a later restart of any node of the fabric.
                            // A birth is written the moment the book first holds it or it changes (the book's own birth
                            // watch, as the runtime rows are), and every member at once when the seat comes to this admin
                            // (the view install's seat watch): a hint that waits on a timer is lost to a restart that comes
                            // first. A contact whose node departed, or whose birth the book no longer holds at a path
                            // another logical node now holds, is dropped (`dropped_contacts`). Nothing here is topology.
                            let (own_seat_tx, mut own_seat) = tokio::sync::watch::channel(false);
                            let contacts_task = {
                                let (book, fabric_id) = (membership.book.clone(), cfg.fabric_id.clone());
                                let mut writer = MeshRowsWriter::new(&name.to_string(), cfg.fabric_id.clone(), &cfg.mesh, mesh_id.clone(), nodes_storage.clone(), mesh_storage.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let mut births = book.birth_changes();
                                    loop {
                                        // Held here while the admin is parked: its own wait, outside every lock.
                                        let turn = park.turn("mesh-rows").await;
                                        births.borrow_and_update();
                                        let seated = *own_seat.borrow_and_update();
                                        let held: Vec<MeshDigest> = book.all().into_iter().filter(|d| d.fabric_id == fabric_id).collect();
                                        writer.round(&held, seated, |id| book.is_departed(id.as_str())).await;
                                        drop(turn);
                                        tokio::select! {
                                            changed = births.changed() => if changed.is_err() { return },
                                            changed = own_seat.changed() => if changed.is_err() { return },
                                        }
                                    }
                                })
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
                                membership: membership.clone(),
                                time: rafka_time.clone(),
                                issuer: issuer.clone(),
                            });
                            // The join door: this admin verifies a node's digest against what it deployed, installs the
                            // address for its key and answers what it holds.
                            {
                                let (st, resolver, m, topo) = (entry.clone(), node_rpc_resolver.clone(), membership.clone(), membership.clone());
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
                                        let (membership, records) = (membership.clone(), records.clone());
                                        move |d: MeshDigest, types: std::collections::BTreeSet<String>| {
                                            let (membership, records) = (membership.clone(), records.clone());
                                            Box::pin(async move {
                                                records.declare_app_types_or_hold(&d, types.clone()).await?;
                                                // The joiner's types are held before the join is answered; the joiner is handed to no
                                                // channel as a peer: it dials this admin, its first neighbour (gossip.md §8, Q84).
                                                membership.hold_app_types_from(&d, types.clone(), types).await.map_err(|e| e.to_string())
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
                                    known: view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone()),
                                    primary: Arc::new(move || topo.mesh_primary().ok().map(|p| p.node.name.to_string())),
                                    issuer: issuer.clone(),
                                    status_storage: Some(status_storage.clone()),
                                    row: Arc::new({
                                        let rows = nodes_storage.clone();
                                        move |name: PathName| {
                                            let rows = rows.clone();
                                            Box::pin(async move { rows.node_row_at(&name).await.map_err(|e| e.to_string()) })
                                        }
                                    }),
                                });
                                let _ = join_slot.set(door);
                                // Every node serves the topology it holds (`GetTopology`, op `0x1E`).
                                let own = digest.clone();
                                // For a mesh it holds no snapshot of, this admin answers from the births it stores.
                                // The births it stores and the births its book holds now: what this admin knows of a mesh
                                // does not wait on the contacts being written.
                                let (contacts, meshes, held_book, held_fabric, held_me) = (nodes_storage.clone(), mesh_storage.clone(), book.clone(), cfg.fabric_id.clone(), node_id.clone());
                                let stored: crate::topology_read::StoredSource = Arc::new(move || {
                                    let (contacts, meshes, held_book, held_fabric, held_me) = (contacts.clone(), meshes.clone(), held_book.clone(), held_fabric.clone(), held_me.clone());
                                    Box::pin(async move {
                                        let rows = contacts.contacts().await.map_err(|e| e.to_string())?;
                                        let ids = meshes.meshes().await.map_err(|e| e.to_string())?;
                                        let mut nodes: BTreeMap<NodeId, rdm_core::topology::StoredNode> = rows
                                            .into_iter()
                                            .map(|r| (r.node_id.clone(), rdm_core::topology::StoredNode { node_id: r.node_id, name: r.name.to_string(), endpoint_id: r.endpoint_id, incarnation: r.incarnation_id, transport_addr: r.transport_addr }))
                                            .collect();
                                        for d in held_births(&held_book, &held_fabric, &held_me) {
                                            nodes.insert(
                                                d.node.node_id.clone(),
                                                rdm_core::topology::StoredNode { node_id: d.node.node_id, name: d.node.name.to_string(), endpoint_id: d.node.endpoint_id, incarnation: d.node.incarnation, transport_addr: d.node.transport_addr },
                                            );
                                        }
                                        Ok(crate::topology_read::StoredMap {
                                            nodes: nodes.into_values().collect(),
                                            mesh_ids: ids.into_iter().filter(|m| !m.dead).map(|m| (m.name, m.mesh_id)).collect(),
                                        })
                                    })
                                });
                                let gone: crate::topology_read::GoneSource = {
                                    let (book, records) = (book.clone(), records.clone());
                                    Arc::new(move |h: &rdm_core::SeatHolder| incumbency_of(&book, &records).lost.contains(&h.incarnation))
                                };
                                let own_membership = membership.clone();
                                let _ = topology_slot.set(Arc::new(crate::topology_read::TopologyDoor::new(membership.clone(), Arc::new(move || own_membership.with_config(&own.lock().unwrap())), rafka_time.clone()).with_stored(stored).with_gone(gone)));
                            }
                            // Each birth's exact runtime, the moment this admin first holds the birth: its own keyed row,
                            // a blind put (a successor fabric primary proves an exit from it).
                            let runtime_rows_task = {
                                let (book, nodes_storage, me, fabric_id) = (membership.book.clone(), nodes_storage.clone(), node_id.clone(), cfg.fabric_id.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let turn = park.turn("runtime-rows").await;
                                    let mut written: HashSet<(NodeId, IncarnationId)> = nodes_storage.runtimes().await.unwrap_or_default().into_iter().map(|r| (r.node_id, r.incarnation_id)).collect();
                                    drop(turn);
                                    let mut births = book.birth_changes();
                                    loop {
                                        // Held here while the admin is parked: its own wait, outside every lock.
                                        let turn = park.turn("runtime-rows").await;
                                        births.borrow_and_update();
                                        for d in book.all().into_iter().filter(|d| d.fabric_id == fabric_id && d.node.node_id != me) {
                                            let key = (d.node.node_id.clone(), d.node.incarnation.clone());
                                            let Some(runtime) = d.node.runtime.clone() else { continue };
                                            if written.contains(&key) {
                                                continue;
                                            }
                                            let row = crate::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime, data_dir: d.data_dir.clone() };
                                            match nodes_storage.put_runtime(&row).await {
                                                Ok(()) => {
                                                    written.insert(key);
                                                }
                                                Err(e) => tracing::info!(node = %d.node.name, error = %e, "a heard birth's runtime could not be stored"),
                                            }
                                        }
                                        drop(turn);
                                        if births.changed().await.is_err() {
                                            return;
                                        }
                                    }
                                })
                            };
                            // A birth the book shows superseded (a later incarnation of its node held, or its node
                            // departed): the declared-state rows this admin put for it as an authority are tombstoned.
                            let superseded_rows_task = {
                                let (book, authority) = (membership.book.clone(), authority.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let mut births = book.birth_changes();
                                    loop {
                                        // Held here while the admin is parked: its own wait, outside every lock.
                                        let turn = park.turn("superseded-rows").await;
                                        births.borrow_and_update();
                                        if let Some(a) = authority.get() {
                                            a.retire_superseded(&book).await;
                                        }
                                        drop(turn);
                                        if births.changed().await.is_err() {
                                            return;
                                        }
                                    }
                                })
                            };
                            let app_channels_task = {
                                let (membership, client, rows, me) = (membership.clone(), node_rpc.client.clone(), nodes_storage.clone(), node_id.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let mut changes = membership.book.digest_changes();
                                    let mut learned = std::collections::HashMap::new();
                                    loop {
                                        let turn = park.turn("app-channels").await;
                                        changes.borrow_and_update();
                                        let work = membership.book.all().into_iter().filter(|d| d.node.node_id != me).filter(|d| learned.get(&(d.node.node_id.clone(), d.node.incarnation.clone())) != Some(&d.held_hash));
                                        let results = futures_util::future::join_all(work.map(|d| {
                                            let (membership, client, rows) = (membership.clone(), client.clone(), rows.clone());
                                            let span = tracing::info_span!("rdm.node_admin.node.update.via-types-read", node_id = %d.node.node_id, incarnation = %d.node.incarnation, subject = %d.node.name);
                                            async move {
                                                let birth = (d.node.node_id.clone(), d.node.incarnation.clone());
                                                if d.node.name.kind == rdm_core::NodeKind::NodeAdmin {
                                                    let req = rdm_core::status::StatusRequest::GetNodes { select: rdm_core::status::NodeSelect::Id(d.node.node_id.clone()) };
                                                    let (out, _) = client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(d.node.node_id.clone()), &req, &rdm_transport::CallOptions::default()).await;
                                                    if let Some(reply) = out.reply() {
                                                        if let rdm_core::status::StatusReply::Nodes { nodes } = reply.value() {
                                                            if let Some(n) = nodes.iter().find(|n| n.node_id == birth.0 && n.incarnation_id.as_ref() == Some(&birth.1)) {
                                                                let hash = rdm_core::app_types::hash(&n.held);
                                                                if hash == n.held_hash && n.types.is_subset(&n.held) {
                                                                    if membership.learn_app_types_from(&d, n.types.clone(), n.held.clone()).await.is_ok() {
                                                                        tracing::info_span!("rdm.node_admin.node.update.via-held-types", node_id = %birth.0, incarnation = %birth.1, types = n.types.len(), held = n.held.len()).in_scope(|| tracing::info!("peer node.get supplied the joined types"));
                                                                        return Some((birth, d.held_hash));
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                } else if let Ok(Some(row)) = rows.node_row_at(&d.node.name).await {
                                                    if row.node_id == birth.0 {
                                                        if let Some(t) = row.app_types.filter(|t| t.incarnation == birth.1) {
                                                            // A member of this admin's own mesh: its types are held, it is dialled by no one but §6's repair (Q84).
                                                            if membership.hold_app_types_from(&d, t.types.clone(), t.types).await.is_ok() { return Some((birth, d.held_hash)); }
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
                            let mut tasks = vec![contacts_task, node_rpc_feed, runtime_rows_task, superseded_rows_task, app_channels_task];
                            tasks.extend(tasks_early);
                            // Entering an existing mesh (a recovering mesh's first admin after its maker's join; a fabric
                            // primary recovered into its mesh from its durable map): connect to a local node, read its
                            // topology and sweep the own mesh once with a Ping. Ready waits for the sweep; every node that
                            // did not answer is unheard, and node recovery's to prove (mesh-recovery.md step 6).
                            let sweep_slot: Arc<std::sync::OnceLock<crate::reenter::SweepReport>> = Arc::new(std::sync::OnceLock::new());
                            let entry_ctx = crate::reenter::EntryCtx { me: name.clone(), me_id: node_id.clone(), client: node_rpc.client.clone(), resolver: node_rpc_resolver.clone(), membership: membership.clone(), time: rafka_time.clone() };
                            if let (true, Some((source, map))) = (entering_existing_mesh, entry_map.take()) {
                                tasks.push(spawn_entry_sweep(&park_gate, entry_ctx.clone(), source, map, sweep_slot.clone(), records.clone(), policy.provider));
                            }
                            // Authority-capable Ready: this admin commits `ReadyForTraffic` (and so
                            // becomes eligible for every seat) only once it can manage every birth it
                            // holds: each live member publishes a runtime fact this admin's provider
                            // adopts in its own control domain, and the Day-0 admin holds a Complete
                            // receipt for every step of its own adoption. Until then it publishes
                            // Pending.
                            // `before_ready` (the app's hook): `node.start` runs it once the join is accepted and the
                            // mesh channel joined, before any topology is read, in a task behind this admin's gate, and then
                            // its topology, Build-floor and rafka-time steps in the same task; the Ready check below names
                            // the hook's blocker and the step the start is at. A birth with no launcher has no authority to
                            // pull from, and says why.
                            let before_ready_cancel = rdm_transport::Retirement::new();
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
                                gate: park_gate.clone(),
                                entry: entry_ctx,
                                entering: entering.clone(),
                                sweep: sweep_slot.clone(),
                                start_tasks: start_tasks.clone(),
                                floor: AdminFloor {
                                    me: name.clone(),
                                    builds: builds_dyn.clone(),
                                    floor: entry_floor.clone(),
                                    fetch_build: crate::fetch_build::FetchBuild::new(
                                        name.clone(),
                                        node_rpc.client.clone(),
                                        journal.clone(),
                                        responders.clone(),
                                        entry_retrieval.map(|(target, anchor, digest)| entry_repeat(node_rpc.client.clone(), target, anchor, digest, accepted.clone(), entry_floor.clone(), rafka_time.clone(), records.clone(), cfg.mesh.clone(), mesh_id.clone())),
                                    ),
                                    progress: Some(progress.clone()),
                                },
                                progress: progress.clone(),
                                read_seam: wiring.read_seam.clone(),
                            };
                            let parts = crate::workflows::node_start::HookParts {
                                birth: crate::before_ready::Birth { name: name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), fabric_id: cfg.fabric_id.clone(), mesh_id: mesh_id.clone(), kind: NodeKind::NodeAdmin },
                                before_ready: wiring.before_ready.clone(),
                                events: crate::before_ready::Events::new(book.clone(), records.wake.before_ready.clone()),
                                cancel: before_ready_cancel.token(),
                                state: before_ready_cell.clone(),
                                runs: crate::workflows::node_start::HookRuns::Spawned {
                                    spawn: {
                                        let park = park_gate.clone();
                                        Box::new(move |hook: crate::workflows::node_start::BoxFut<()>| park.spawn(hook))
                                    },
                                },
                                after: Box::new(after),
                            };
                            let started: crate::workflows::node_start::HookStarted<String> = Box::new(move |hook: tokio::task::JoinHandle<()>| -> crate::workflows::node_start::BoxFut<Result<(), String>> {
                                Box::pin(async move {
                                    tasks.push(hook);
                                    {
                                        let before_ready_state = before_ready_cell.clone();
                                        let (digest, book, provider, membership) = (digest.clone(), book.clone(), provider_dyn.clone(), membership.clone());
                                        let day0 = cfg.launch.is_none().then(|| cfg.data_dir.clone());
                                        let (hydrated, start_progress, entering_gate) = (builds_dyn.clone(), progress.clone(), entering.clone());
                                        let sweep_slot_gate = sweep_slot.clone();
                                        let (incarnation, node_id, mesh_id, fabric_id) = (incarnation.clone(), node_id.clone(), mesh_id.clone(), cfg.fabric_id.clone());
                                        let runtime = runtime.clone();
                                        let boot = boot.clone();
                                         let (records, authority, mesh_name, fabric_name, view_provider, backbone_ready) =
                                            (records.clone(), authority.clone(), cfg.mesh.clone(), cfg.fabric.clone(), policy.provider, backbone.clone());
                                        let park = park_gate.clone();
                                        let election_storage = fabric_storage.clone();
                                        let election_cut_off = cut_off_writes.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut reported = None;
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("ready").await;
                                                let me = digest.lock().unwrap().node.name.clone();
                                                let held: Vec<MeshDigest> = book
                                                    .current(book.staleness_floor())
                                                    .into_iter()
                                                    .filter(|d| d.node.name != me && d.status != MemberStatus::Leaving)
                                                    .collect();
                                                let mut blocked = authority_blockers(&held, &*provider);
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
                                                blocked.extend(before_ready_state.lock().unwrap().as_ref().and_then(|h| h.state().blocker(&me)));
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
                                                    PendingGate::SelfApply => match authority.get() {
                                                        Some(auth) => {
                                                            let reply = auth.self_apply_mesh_pending(&mesh_id).await;
                                                            if !matches!(reply, rdm_core::status::StatusReply::Applied | rdm_core::status::StatusReply::AlreadyApplied) {
                                                                blocked.push(format!("{me}: its mesh's Pending is not applied (self-apply answered {reply:?})"));
                                                            }
                                                        }
                                                        None => blocked.push(format!("{me}: no view yet to apply its mesh's Pending to itself")),
                                                    },
                                                }
                                                if blocked.is_empty() {
                                                    let d_ready = {
                                                        let mut d = digest.lock().unwrap();
                                                        if d.status != MemberStatus::Pending {
                                                            break;
                                                        }
                                                        d.status = MemberStatus::ReadyForTraffic;
                                                        d.clone()
                                                    };
                                                    let _ = membership.publish(&d_ready).await;
                                                    let reads = ViewReads::read(&me, &book, &records, &fabric_id).await;
                                                    let decided = incumbency_of(&book, &records);
                                                    let t = project_decided(&fabric_name, &fabric_id, view_provider, &book, &records, &decided, &reads);
                                                    let live = membership.authorizes();
                                                    // Each seat taken is written as the fabric's next seat move before it is said (Q48).
                                                    let said = seat_announcements(&t, live, &mesh_name, (&node_id, &incarnation), &decided);
                                                    let me_birth = rdm_core::handover::HandoverBirth { mesh: mesh_name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() };
                                                    let mut seated_again = (false, false);
                                                    for (seat, holder) in crate::fabric_handover::elect(&*election_storage, membership.seats(), &me_birth, &decided, said).await {
                                                        if holder.node_id == node_id {
                                                            match seat {
                                                                rdm_core::Seat::MeshPrimary => seated_again.0 = true,
                                                                rdm_core::Seat::FabricPrimary => seated_again.1 = true,
                                                                rdm_core::Seat::Cohort(_) => {}
                                                            }
                                                        }
                                                        backbone_ready.announce_seat(seat, holder).await;
                                                    }
                                                    // A seat its own earlier transfer moved away left that seat's stores cut off; the election
                                                    // row seating it again makes it that seat's row writer from the row's revision
                                                    // (node-primary-update.md: "A holder the rule seats again ... restores only those seats'
                                                    // writes under the new revision"). A store standing open is left as it is.
                                                    if seated_again.0 || seated_again.1 {
                                                        (election_cut_off)(crate::fabric_handover::WriteCutOff::Restore { reason: format!("{me}: its election row seated it again"), mesh_seat: seated_again.0, fabric_seat: seated_again.1 }).await;
                                                    }
                                                    // The current Build the not-ready check above found held: the Ready span names it,
                                                    // so "it held the current Build before Ready" is read off the Ready itself.
                                                    let held_build = match hydrated.current_build().await {
                                                        Ok(c) => c.into_build_id().map(|b| b.to_string()).unwrap_or_default(),
                                                        Err(e) => format!("unreadable: {e}"),
                                                    };
                                                    records.wake.poke();
                                                    tracing::info_span!(
                                                        parent: &boot,
                                                        "rdm.mesh.node.update.via-ready",
                                                        node = %me,
                                                        incarnation_id = %incarnation.as_str(),
                                                        meshes = membership.meshes_held(),
                                                        node_id = %node_id,
                                                        mesh_id = %mesh_id,
                                                        fabric_id = %fabric_id,
                                                        id_format = rdm_core::ID_FORMAT,
                                                        deployment_id = %runtime.deployment_id,
                                                        provider = runtime.provider.as_str(),
                                                        provider_control_domain_fingerprint = %runtime.domain_fingerprint(),
                                                        runtime_locator_kind = runtime.locator.kind(),
                                                        runtime_locator_fingerprint = %runtime.locator_fingerprint(),
                                                        source = "self-published-membership",
                                                        runtime_facts_held = held.len(),
                                                        build_id = %held_build,
                                                    )
                                                    .in_scope(|| tracing::info!("ready for traffic: every held birth's runtime is adoptable"));
                                                    break;
                                                }
                                                if reported.as_ref() != Some(&blocked) {
                                                    tracing::info_span!("rdm.node_admin.runtime.reject.via-not-authority-capable", node = %me, blocked = blocked.len(), detail = %blocked.join("; "))
                                                        .in_scope(|| tracing::info!("not ready: a held birth's runtime cannot be managed from here"));
                                                    reported = Some(blocked);
                                                }
                                                drop(turn);
                                                tokio::time::sleep(Duration::from_millis(100)).await;
                                            }
                                        }));
                                    }
                                    {
                                        // The config is held on this admin's own digest and by its membership, so every publish and
                                        // every read of its own digest carries it.
                                        let (membership, digest) = (membership.clone(), digest.clone());
                                        let _ = republish.set(Arc::new(move || {
                                            let (membership, digest) = (membership.clone(), digest.clone());
                                            Box::pin(async move {
                                                let d = digest.lock().unwrap().clone();
                                                let _ = membership.publish(&d).await;
                                            })
                                        }));
                                    }
                                    {
                                        let (membership, digest) = (membership.clone(), digest.clone());
                                        let _ = configure.set(Arc::new(move |config: BTreeMap<String, String>| {
                                            let (membership, digest) = (membership.clone(), digest.clone());
                                            Box::pin(async move {
                                                membership.set_config(config.clone());
                                                let d = {
                                                    let mut d = digest.lock().unwrap();
                                                    d.config = config;
                                                    d.clone()
                                                };
                                                let _ = membership.publish(&d).await;
                                            })
                                        }));
                                    }
                                    {
                                        // The name is held on this admin's own digest and by its membership, taken only while it
                                        // holds no connection (R-CR1); a stopped admin holds it and publishes nothing.
                                        let (membership, digest, connections) = (membership.clone(), digest.clone(), connections.clone());
                                        let _ = rename.set(Arc::new(move |asked: PathName| {
                                            let (membership, digest, connections) = (membership.clone(), digest.clone(), connections.clone());
                                            Box::pin(async move {
                                                let held = digest.lock().unwrap().node.name.clone();
                                                let routes = connections.held_routes().await;
                                                let decision = crate::node_name::decide(&held, &asked, &routes);
                                                if decision == crate::node_name::NameApply::Applied {
                                                    membership.set_name(asked.clone());
                                                    let d = {
                                                        let mut d = digest.lock().unwrap();
                                                        d.node.name = asked;
                                                        d.clone()
                                                    };
                                                    if !crate::node_self::parking().is_parked() {
                                                        let _ = membership.publish(&d).await;
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
                                        let set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync> = Arc::new(move |st| {
                                            let mut d = digest_for_status.lock().unwrap();
                                            d.status = st;
                                            d.clone()
                                        });
                                        let status: Arc<dyn Fn() -> MemberStatus + Send + Sync> = Arc::new(move || digest_now.lock().unwrap().status);
                                        // Its stop's Stopped names the stop, as every node's does (node-status.md "Status
                                        // report", node-stop.md step 4; i146 Q37): owed in its declarer and sent from this
                                        // call, never from the declarer's own turns, which are held while it is stopped;
                                        // whichever of it and the stop's reply reaches the primary first is applied (Q49).
                                        // An admin that is its own mesh's primary has no primary to report to, and after its
                                        // cut it publishes nothing (node-stop.md step 4): its stop is its commander's to publish.
                                        let declare: crate::node_self::Declare = {
                                            let (declarer, topology, client, authority, digest, me_id, incarnation, wake) =
                                                (declarer.clone(), control.topology.clone(), node_rpc.client.clone(), authority.clone(), digest.clone(), node_id.clone(), incarnation.clone(), records.wake.clone());
                                            Arc::new(move |state, operation| {
                                                let me = digest.lock().unwrap().node.name.clone();
                                                let (declarer, topology, client, authority, me_id, incarnation, wake) = (declarer.clone(), topology.clone(), client.clone(), authority.clone(), me_id.clone(), incarnation.clone(), wake.clone());
                                                Box::pin(async move {
                                                    let view = topology.read().await.clone();
                                                    if view.cohort_primary(&me.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.node_id == me_id) {
                                                        tracing::info_span!("rdm.node_admin.status.reject.via-own-mesh-primary", node = %me, state = ?state, operation = %operation.as_ref().map(|o| o.operation.as_str()).unwrap_or(""), "otel.kind" = "internal")
                                                            .in_scope(|| tracing::info!("this admin is its own mesh's primary: there is no primary to report its state to, and after its cut it publishes nothing"));
                                                        return;
                                                    }
                                                    declarer.owe_own(&me, &me_id, &incarnation, state, operation);
                                                    declarer.round(&me, &me_id, &view, &client, authority.get().map(|a| a.as_ref()), &BTreeSet::new(), &wake).await;
                                                })
                                            })
                                        };
                                        let own = Arc::new(crate::node_self::NodeSelf::new(node_id.clone(), incarnation.clone(), name.clone(), rpc_server.clone(), node_rpc.client.clone(), node_rpc.resolver.clone(), membership.clone(), set_status, status, Some(declare), cfg.connections_delete_deadline).with_retire(before_ready_cancel.clone()).with_round_actions(round_actions.clone()).with_backbone(backbone.clone()));
                                        own.set_connections(connections.clone());
                                        // The app protocols this admin opens connections on: what a renewed member cert
                                        // is installed on, and what node.connections.delete closes, as on every node.
                                        own.set_data_connections(data_connections.clone());
                                        own.set_session(Arc::new(AdminSession {
                                            gate: park_gate.clone(),
                                            node_id: node_id.clone(),
                                            incarnation: incarnation.clone(),
                                            fabric_id: cfg.fabric_id.clone(),
                                            mesh_id: mesh_id.clone(),
                                            membership: membership.clone(),
                                            backbone: backbone.clone(),
                                            client: node_rpc.client.clone(),
                                            server: rpc_server.clone(),
                                            digest: digest.clone(),
                                            rafka_time: rafka_time.clone(),
                                            accepted: accepted.clone(),
                                            builds: builds_dyn.clone(),
                                            entry_floor: entry_floor.clone(),
                                            before_ready: wiring.before_ready.clone(),
                                            addressed: records.wake.before_ready.clone(),
                                            before_ready_cell: before_ready_cell.clone(),
                                            before_ready_task: Arc::default(),
                                            wake: records.clone(),
                                            local: journal.clone(),
                                            responders: responders.clone(),
                                            declarer: declarer.clone(),
                                        }));
                                        let _ = own_commands.set(own);
                                    }
                                    // A node's own `node.connections.deleted` / `node-left`, heard on this mesh's channel: this admin carries
                                    // it onto the backbone when it holds the command (gossip.md, node-connections-deleted, node-left).
                                    {
                                        let (commands, backbone, rounds) = (commands.clone(), backbone.clone(), rounds_slot.get().cloned().expect("the rounds are built before the carrier"));
                                        membership.set_completion_carrier(Arc::new(move |f: &rdm_transport::membership::Frame| {
                                            use rdm_transport::membership::{CompletionCarry, Frame};
                                            if matches!(f.data(), Frame::StateCommitted { .. } | Frame::TrafficOpened { .. }) {
                                                return rounds.mesh_book.carry_check_in(f, |f| {
                                                    let (backbone, f) = (backbone.clone(), f.clone());
                                                    tokio::spawn(async move { backbone.carry_completion(f).await });
                                                });
                                            }
                                            let (Frame::NodeLeft { op, .. } | Frame::NodeConnectionsDeleted { op, .. }) = f.data() else { return CompletionCarry::NotHeld };
                                            let key = crate::node_commands::CommandKey { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone(), node_id: op.node_id.clone(), incarnation: op.incarnation.clone() };
                                            if commands.is_open(&key) {
                                                let (backbone, f) = (backbone.clone(), f.clone());
                                                tokio::spawn(async move { backbone.carry_completion(f).await });
                                                CompletionCarry::Carried
                                            } else {
                                                CompletionCarry::NotHeld
                                            }
                                        }));
                                    }
                                    let d = digest.clone();
                                    let publisher = membership.publish_every(gossip_interval(), move || {
                                        d.lock().unwrap().clone()
                                    });
                                    // The declarer (i143.e4.s11, R-S2): what this admin owes its authorities over `Status`, sent
                                    // when owed and again only on an eligible event, woken by its inputs; nothing it does gates
                                    // gossip or readiness.
                                    {
                                        let declarer = declarer.clone();
                                        let (digest, topology, records, me_id, incarnation, client, resolver, book, authority, status_storage) =
                                            (digest.clone(), control.topology.clone(), records.clone(), node_id.clone(), incarnation.clone(), node_rpc.client.clone(), node_rpc.resolver.clone(), book.clone(), authority.clone(), status_storage.clone());
                                        let rounds = rounds_slot.clone();
                                        let park = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("declarer").await;
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
                                                // The fabric-primary owes the ready-for-traffic event from the durable fact: its mesh's
                                                // status rows on the store root, read here, latch the handle the derivation reads.
                                                if view.holds_fabric_seat(&me_id) {
                                                    if let Err(e) = declarer.read_first_ready(&*status_storage, &view).await {
                                                        tracing::info_span!("rdm.node_admin.fabric.reject.via-ready-row-unreadable", node = %me, fabric_id = %view.fabric.id, status = view.fabric.status.name(), detail = %e)
                                                            .in_scope(|| tracing::info!("the ready-for-traffic row of the fabric could not be read from this mesh's status rows: this round owes from the latch and the view"));
                                                    }
                                                }
                                                crate::status_declare::owed_from_view(&declarer, &me, &me_id, &incarnation, own, records.declare_gate(), own_mesh_held, &view, &mesh_ids);
                                                // A completion owed for a round the accepted Build moved past is withdrawn before anything is sent.
                                                if let Some(rounds) = rounds.get() {
                                                    rounds.withdraw_superseded().await;
                                                }
                                                rdm_node::node_rpc::feed_once(&book, &resolver, &me.to_string());
                                                declarer.round(&me, &me_id, &view, &client, authority.get().map(|a| a.as_ref()), &addressed, &records.wake).await;
                                                drop(turn);
                                                // One coalesced wake from the inputs, never a timer: a poke during the round is
                                                // the stored permit, so the next round runs at once; a burst runs one.
                                                records.wake.woken().await;
                                            }
                                        }));
                                    }

                                    // The offline tickle (node-recovery.md "Detection and proof"): every admin keeps the silence
                                    // bookkeeping; the one holding its mesh's primary seat tickles each node of its own mesh the
                                    // topology shows PendingReconnect, half a floor after the mark: a ping (op 1), and when it
                                    // answers the status kick (op 27); when it does not, the same kick through a live node of its
                                    // mesh on the ViaPeer route. The node's status lands on its row in nodes.storage and in the view.
                                    let recovery = RecoverySubmitter {
                                        me: name.clone(),
                                        topology: control.topology.clone(),
                                        book: book.clone(),
                                        provider: runner.provider.clone(),
                                        nodes_storage: nodes_storage.clone(),
                                        status_storage: status_storage.clone(),
                                        door: build_slot.clone(),
                                        builds: control.builds.clone(),
                                        data_root: cfg.data_dir.parent().map(Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone()),
                                        gone_dirs: Arc::default(),
                                        os_stopped_confirmed: Arc::default(),
                                        terminate_refusals_named: Arc::default(),
                                        offline: records.offline.clone(),
                                    };
                                    {
                                        let (topology, records, status_storage, runner, me, me_id, mesh, recovery) = (control.topology.clone(), records.clone(), status_storage.clone(), runner.clone(), name.clone(), node_id.clone(), cfg.mesh.clone(), recovery.clone());
                                        let park = park_gate.clone();
                                        let mut cadence = crate::offline::Cadence::every(gossip_interval())?;
                                        // A birth heard again is commanded back through node.mesh.join (Q87).
                                        let ladder_heard_again = heard_again_slot.clone();
                                        tasks.push(tokio::spawn(async move {
                                            // The births whose inspection or submitted recovery is in flight, and the last verdict
                                            // named per birth. The tick never waits on either.
                                            let in_flight: Arc<Mutex<HashSet<(NodeId, IncarnationId)>>> = Arc::default();
                                            let verdicts: Arc<Mutex<HashMap<(NodeId, IncarnationId), Inspected>>> = Arc::default();
                                            let mut tickle = crate::offline::OfflineTickle::observed_by(me.to_string(), park.clone());
                                            // The births whose kick answered `Current`: a round ends in a later tick than the
                                            // one that began it, so the set outlives the tick; an entry is taken when its
                                            // birth is folded as reached (node-recovery.md step 2; Q87).
                                            let answered_current: Arc<Mutex<HashSet<String>>> = Arc::default();
                                            let floor = rdm_transport::membership::staleness_floor();
                                            let mut written: WrittenSilence = HashMap::new();
                                            let mut heard_changes = recovery.book.heard_changes();
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("offline-tickle").await;
                                                heard_changes.borrow_and_update();
                                                let view = topology.read().await.clone();
                                                let holds_seat = view.holds_mesh_seat(&mesh, &me_id);
                                                // The per-member verdicts and os-stopped confirmations are the seat holder's
                                                // (node-recovery.md step 3): an admin that does not hold the seat keeps none, so
                                                // when it takes the seat, or takes it again, its first inspection of each member
                                                // is its own, spanned, and it revalidates a row another admin wrote
                                                // (deployment-provider.md §2.5: "a new authority revalidates the stored fact with
                                                // its own inspection").
                                                if !holds_seat {
                                                    verdicts.lock().unwrap().clear();
                                                    recovery.os_stopped_confirmed.lock().unwrap().clear();
                                                }
                                                // Every admin watches the same set, the seat holder or not, so a successor tickles on
                                                // the window already running; the durable rows are read each tick (one point in time).
                                                let durable = {
                                                    let rows = recovery.durable_rows().await;
                                                    let planned = records.planned();
                                                    durable_births(&view, &rows, &|p| planned.get(&p.mesh).is_some_and(|m| m.contains(p)), &|id| recovery.book.is_departed(id))
                                                };
                                                *records.durable.lock().unwrap() = durable.clone();
                                                let mut watched = watched_births(&view, &me_id, &mesh, &durable);
                                                // One reading of the clocks for the tick: each birth's mark is read from the book at it.
                                                let (now, now_ms) = (std::time::Instant::now(), local_now_ms());
                                                // node-tickle.md step 1: a birth a command never reached for a reachability reason is
                                                // PendingReconnect from that instant, so its ladder runs now.
                                                let marks = crate::offline::held_marks(&records.unreachable, &view, &mesh, &recovery.book, now);
                                                crate::offline::watch_marked(&mut watched, &marks, &view, &me_id);
                                                // The topology decides who is tickled (Luke, Q87): a mesh-primary's view comes from the
                                                // fabric-primary and is current, so a member it holds ready-for-traffic there is not
                                                // tickled on this admin's own fresh book; only a member the view holds off
                                                // ready-for-traffic (its mark, or a command never sent) is.
                                                let silent: Vec<crate::offline::SilentNode> = crate::offline::silent_set(
                                                    watched.values().filter(|n| n.status != NodeStatus::ReadyForTraffic).filter(|n| {
                                                        marks.contains_key(&n.node_id)
                                                            || !recovery
                                                                .book
                                                                .get_at(n.node_id.as_str(), now)
                                                                .or_else(|| recovery.book.projected_at(n.node_id.as_str(), now))
                                                                .is_some_and(|(d, unheard)| Some(&d.node.incarnation) == n.incarnation_id.as_ref() && unheard < floor)
                                                    }),
                                                    &recovery.book,
                                                    now,
                                                    now_ms,
                                                    floor,
                                                )
                                                .into_iter()
                                                .map(|s| crate::offline::marked_from(s, &marks))
                                                .collect();
                                                let client = Some(runner.node_rpc.client.clone());
                                                // The tickle: ask the exact birth to reassert itself.
                                                // Protocol name: ProbeNodeState.
                                                let kick_of = |n: &Node, incarnation: &crate::model::IncarnationId| rdm_core::status::StatusRequest::ProbeNodeState { node_id: n.node_id.clone(), incarnation: incarnation.clone() };
                                                let own_id = me_id.clone();
                                                // What the tick watches decides "heard again" (`heard_against`, R-FM1).
                                                let in_view: HashSet<String> = heard_against(&view, &watched);
                                                for ((nid, _), v) in verdicts.lock().unwrap().iter() {
                                                    if v.is_running() {
                                                        tickle.reprobe(nid.to_string());
                                                    }
                                                }
                                                let report = tickle
                                                    .tick(
                                                        now_ms,
                                                        holds_seat,
                                                        silent,
                                                        |id: &str| in_view.contains(id),
                                                        floor.as_millis() as u64 / 2,
                                                        floor.as_millis() as u64,
                                                        |id| {
                                                            let (client, node) = (client.clone(), watched.get(&id).cloned());
                                                            let answered_current = answered_current.clone();
                                                            async move {
                                                                let Some(node) = node else { return Err(format!("{id} is not in the topology")) };
                                                                let Some(client) = client else { return Err("this admin has no Node RPC client".to_string()) };
                                                                let target = rdm_transport::NodeTarget::ExactNode(node.node_id.clone());
                                                                // One QUIC connect, direct (node-recovery.md step 2): asked once, never
                                                                // resent; the ladder, not this call, goes on to the peers.
                                                                let once = rdm_transport::CallOptions { once: true, ..rdm_transport::CallOptions::default() };
                                                                let ping = rdm_core::ping::PingRequest::Ping { payload: b"tickle".to_vec() };
                                                                let (out, _) = client.call::<rdm_core::ping::Ping>(&target, &ping, &once).await;
                                                                if out.reply().is_none() {
                                                                    return Err(format!("{} did not answer the ping: {}", node.name, out.name()));
                                                                }
                                                                // The ping answered: the status kick. The node answers its status, and a
                                                                // birth this admin holds off its mesh (the ladder tickles no other) that
                                                                // answers Current is commanded back through node.mesh.join (node-tickle.md
                                                                // "What follows", node-recovery.md step 2, Q87).
                                                                let Some(incarnation) = node.incarnation_id.clone() else { return Err(format!("{} ({}) holds no recorded birth: there is no exact birth to tickle", node.name, node.node_id)) };
                                                                let birth = rdm_client::ExactBirth { target: target.clone(), node_id: node.node_id.clone(), incarnation };
                                                                let kick = match rdm_client::NodeRpc::new(&client).get(&birth, &once).await {
                                                                    Ok(reply) => {
                                                                        let name = reply.name();
                                                                        if matches!(reply, rdm_core::status::StatusReply::Current { .. }) {
                                                                            answered_current.lock().unwrap().insert(node.node_id.to_string());
                                                                        }
                                                                        name
                                                                    }
                                                                    Err(end) => end.rpc_name(),
                                                                };
                                                                tracing::info!(node = %node.name, kick, "status kick after an answered ping");
                                                                Ok(())
                                                            }
                                                        },
                                                        |id| {
                                                            let (client, node) = (client.clone(), watched.get(&id).cloned());
                                                            let answered_current = answered_current.clone();
                                                            // Carriers from the topology: every ready member of the target's mesh
                                                            // other than the target and this admin, whatever its kind (every node
                                                            // serves the core Forward op).
                                                            let carriers: Vec<Node> = view
                                                                .members()
                                                                .filter(|c| node.as_ref().is_some_and(|n| c.mesh == n.mesh && c.node_id != n.node_id) && c.node_id != own_id && c.status == NodeStatus::ReadyForTraffic)
                                                                .take(crate::offline::VIA_PEER_TICKLE_FANOUT)
                                                                .cloned()
                                                                .collect();
                                                            async move {
                                                                let (Some(node), Some(client)) = (node, client) else { return crate::offline::ViaPeerVerdict::CandidatesUnreadable(format!("{id} is not in the topology, or this admin has no Node RPC client")) };
                                                                let Some(incarnation) = node.incarnation_id.clone() else {
                                                                    return crate::offline::ViaPeerVerdict::CandidatesUnreadable(format!("{} ({}) holds no recorded birth: there is no exact birth to probe", node.name, node.node_id));
                                                                };
                                                                // Every probe names the same exact birth: one kick, by node id and incarnation.
                                                                let kick = kick_of(&node, &incarnation);
                                                                let (node, client, kick) = (&node, &client, &kick);
                                                                let peers: Vec<_> = carriers
                                                                    .iter()
                                                                    .map(|c| {
                                                                        let route = rdm_transport::RouteChoice::ViaPeer { carrier: c.node_id.clone(), path: c.name.clone() };
                                                                        let answered_current = answered_current.clone();
                                                                        let probe = async move {
                                                                            let (out, _, _) = client.call_routed::<rdm_core::status::Status>(&node.node_id, &route, kick, &rdm_transport::CallOptions::default()).await;
                                                                            let reply = out.reply().map(|r| r.value().clone());
                                                                            if matches!(reply, Some(rdm_core::status::StatusReply::Current { .. })) {
                                                                                answered_current.lock().unwrap().insert(node.node_id.to_string());
                                                                            }
                                                                            reply
                                                                        };
                                                                        (c.name.to_string(), probe)
                                                                    })
                                                                    .collect();
                                                                crate::offline::via_peer_verdict(&node.node_id, &incarnation, peers).await
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
                                                    if crate::offline::put_silence_status(&*status_storage, &node, status).await.is_ok() {
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
                                                // A birth heard again, or answering its tickle, runs: its hold and its os-stopped
                                                // fact end (node-recovery.md steps 2-4; deployment-provider.md §2.5).
                                                for id in report.returned.iter().chain(&report.reachable_silent) {
                                                    records.unreachable.lock().unwrap().retain(|nid, _| nid.as_str() != id);
                                                }
                                                for id in &report.returned {
                                                    // A held birth heard again by its digest is commanded back through node.mesh.join
                                                    // too (Q87: the view returns on the bare digest, its cert, time and cache do not):
                                                    // by the mesh-primary, on its own task.
                                                    answered_current.lock().unwrap().remove(id);
                                                    if holds_seat {
                                                        if let (Some(heard_again), Some(n)) = (ladder_heard_again.get().cloned(), watched.get(id).or_else(|| view.births().find(|n| n.node_id.as_str() == id)).cloned()) {
                                                            if let Some(incarnation) = n.incarnation_id.clone() {
                                                                tokio::spawn(async move { heard_again.command(&n, &incarnation).await; });
                                                            }
                                                        }
                                                    }
                                                    // The mark goes before the rows are cleared: a follower in flight reads the
                                                    // mark after its own write and removes what it wrote (`RecoverySubmitter::offline`).
                                                    records.offline.lock().unwrap().retain(|(nid, _)| nid.as_str() != id);
                                                    // The silence, held-running and os-stopped rows are written and removed by the
                                                    // mesh-primary (node-identity-and-state.md): an admin that does not hold the
                                                    // seat hears the return and clears only its own records.
                                                    if !holds_seat {
                                                        continue;
                                                    }
                                                    write(id, None, &mut written).await;
                                                    if let Some(n) = watched.get(id).or_else(|| view.births().find(|n| n.node_id.as_str() == id)) {
                                                        records.clear_held_running(n).await;
                                                        recovery.clear_os_stopped(n, "returned").await;
                                                    }
                                                }
                                                for id in &report.reachable_silent {
                                                    // A held birth that answered its kick `Current` is commanded back through
                                                    // node.mesh.join (Q87): its commanded start runs on its own task, after the ladder's fold.
                                                    // Only the mesh-primary commands its members (commands go down); every admin
                                                    // keeps the bookkeeping.
                                                    if answered_current.lock().unwrap().remove(id) && holds_seat {
                                                        if let (Some(heard_again), Some(n)) = (ladder_heard_again.get().cloned(), watched.get(id).or_else(|| view.births().find(|n| n.node_id.as_str() == id)).cloned()) {
                                                            if let Some(incarnation) = n.incarnation_id.clone() {
                                                                tokio::spawn(async move { heard_again.command(&n, &incarnation).await; });
                                                            }
                                                        }
                                                    }
                                                    verdicts.lock().unwrap().retain(|(nid, _), _| nid.as_str() != id);
                                                    if let Some(n) = watched.get(id).or_else(|| view.births().find(|n| n.node_id.as_str() == id)) {
                                                        records.clear_held_running(n).await;
                                                        recovery.clear_os_stopped(n, "reached").await;
                                                    }
                                                }
                                                clear_unwatched(&watched, &mut records.offline.lock().unwrap(), &mut verdicts.lock().unwrap(), &mut written);
                                                // An os-stopped confirmation speaks only for a birth this tick still watches.
                                                recovery.os_stopped_confirmed.lock().unwrap().retain(|(nid, inc)| watched.get(nid.as_str()).is_some_and(|n| n.incarnation_id.as_ref() == Some(inc)));
                                                // node-recovery.md: on TRUE OFFLINE the mesh primary asks the provider about the exact
                                                // runtime of each member of its own mesh, its secondary node-admins included
                                                // (mesh-recovery.md:8-9); Exited is the proof, and the
                                                // recovery is submitted keyed on the exited birth. Re-evaluated every tick while the
                                                // member stays TRUE OFFLINE, so a recovery that failed is submitted again.
                                                {
                                                    let offline = records.offline.lock().unwrap().clone();
                                                    for n in recovery_due(holds_seat, &mesh, &watched, &offline) {
                                                        let Some(incarnation) = n.incarnation_id.clone() else { continue };
                                                        let birth = (n.node_id.clone(), incarnation);
                                                        if !in_flight.lock().unwrap().insert(birth.clone()) {
                                                            continue;
                                                        }
                                                        let (recovery, n, verdicts) = (recovery.clone(), n.clone(), verdicts.clone());
                                                        crate::offline::spawn_recovery_follower(&park, in_flight.clone(), birth, async move {
                                                            recovery.recover_if_proven(&n, &verdicts).await;
                                                        });
                                                    }
                                                    // node-recovery.md "Failure and receipts": an exited birth whose workflow in the
                                                    // accepted Build stopped at a Failed attempt is submitted again, read from the
                                                    // durable facts each tick, whether or not the view still holds it (a workflow
                                                    // whose delete completed departed it) and whoever submitted it before. One submit
                                                    // per birth in flight, whichever way it is due.
                                                    for owed in recovery.owed_submits(holds_seat, &mesh).await {
                                                        let birth = (owed.node_id.clone(), owed.incarnation.clone());
                                                        if !in_flight.lock().unwrap().insert(birth.clone()) {
                                                            continue;
                                                        }
                                                        let recovery = recovery.clone();
                                                        crate::offline::spawn_recovery_follower(&park, in_flight.clone(), birth, async move {
                                                            if let Err(e) = recovery.submit_owed(&owed).await {
                                                                tracing::info!(node = %owed.path, error = %e, "the submit of an owed exit ended without the recovery");
                                                            }
                                                        });
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
                                            status_storage.clone(),
                                            node_rpc.client.clone(),
                                            Arc::new(move |bytes: Vec<u8>| {
                                                if let Some(own) = own_cert.get() {
                                                    let _ = own.renew_cert(&bytes);
                                                }
                                            }),
                                        );
                                        let (topology, gate) = (control.topology.clone(), park_gate.clone());
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
                                            gate: park_gate.clone(),
                                            me: name.clone(),
                                            me_id: node_id.clone(),
                                            incarnation: incarnation.clone(),
                                            membership: membership.clone(),
                                            topology: control.topology.clone(),
                                            records: records.clone(),
                                            looker: crate::seat_watch::Looker { me: name.clone(), me_id: node_id.clone(), membership: membership.clone(), client: node_rpc.client.clone(), provider: runner.provider.clone(), nodes: Some(nodes_storage.clone()), heard_again: heard_again_slot.clone() },
                                            builds: builds.clone(),
                                            recovery: Some(recovery.clone()),
                                            handover: handover_slot.clone(),
                                        };
                                        // Not behind the gate: it reads the view; it is held at its own wait (`Watch::gate`).
                                        tasks.push(tokio::spawn(crate::seat_watch::run(watch)));
                                    }

                                    // The open lifecycle overlays, derived from the Build facts inside each append to this admin's
                                    // log (its own appends and the facts it absorbs from the fabric alike), before the append
                                    // returns: a hold opens with its pre-event's receipt and closes with its operation's `Complete`
                                    // or its attempt's `Converged`, and no reader of the log sees that fact while the book still
                                    // holds the hold (a log on a plane other admins write converges at the derivation instead:
                                    // `LocalBuildLog::derive_on_every_fact`). A successor primary publishes the same overlays from the same facts. Derived
                                    // once over the facts held before the hierarchy's first round, then on every fact.
                                    {
                                        let (book, named) = (membership.book.clone(), Mutex::new(NamedSteps::new()));
                                        // The Builds the book holds overlays of are read with the current Build and the new
                                        // facts' own, so an overlay heard by gossip ends on its own Build's facts.
                                        let holds = book.clone();
                                        let held = move || holds.in_flight().into_iter().filter_map(|o| crate::build::BuildId::parse(&o.build_id).ok()).collect::<Vec<_>>();
                                        journal.derive_on_every_fact(crate::build_state::FactDerivation::new(held, move |facts| overlays_from_facts(facts, &book, &mut named.lock().unwrap()))).await;
                                    }

                                    let hierarchy;
                                    let anti_entropy;
                                    // What the hierarchy hands the Members anti-entropy: a role it took is held for
                                    // writing until its seat is announced (`roles`), the anti-entropy is told once it
                                    // is (`roles_moved`).
                                    let roles = Arc::new(tokio::sync::RwLock::new(()));
                                    let roles_moved = Arc::new(tokio::sync::Notify::new());
                                    // The hierarchy decides this admin's roles: its mesh's primary forwards the other
                                    // meshes onto its mesh channel; the fabric primary publishes the fabric's status and
                                    // climbs the fabric states. It joins the backbone through every admin it knows and
                                    // its mesh channel through every member of its mesh it knows. It publishes no
                                    // Members aggregate: that is the anti-entropy's timer, below.
                                    {
                                        let (roles, roles_moved) = (roles.clone(), roles_moved.clone());
                                        let (backbone, membership, topology, me, mesh, builds) = (backbone.clone(), membership.clone(), control.topology.clone(), name.clone(), cfg.mesh.clone(), builds.clone());
                                        let (view_fabric, view_fabric_id, view_provider, view_book) = (cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone());
                                        let (seat_node_id, seat_incarnation) = (node_id.clone(), incarnation.clone());
                                        let seat_storage = fabric_storage.clone();
                                        let seat_cut_off = cut_off_writes.clone();
                                        let adapter = runner.builds.clone();
                                        let records = records.clone();
                                        let fabric_rounds = rounds_slot.get().cloned().expect("the rounds are built before the hierarchy runs");
                                        let park = park_gate.clone();
                                        hierarchy = tokio::spawn(async move {
                                            let mut was_fabric_primary = false;
                                            let mut was_mesh_primary = false;
                                            let mut unreadable_named: Option<String> = None;
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("hierarchy").await;
                                                let me = view_book.held_path_name().unwrap_or_else(|| me.clone());
                                                // The current Build, read once per turn. A builds store that cannot be read is
                                                // named once per distinct error and decides nothing this turn: no planned births,
                                                // accepted meshes or round step are taken from a read that failed.
                                                let current = match adapter.current().await {
                                                    Ok(c) => {
                                                        unreadable_named = None;
                                                        Some(c)
                                                    }
                                                    Err(e) => {
                                                        let text = e.to_string();
                                                        if unreadable_named.as_deref() != Some(text.as_str()) {
                                                            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "hierarchy", node = %me, error = %text)
                                                                .in_scope(|| tracing::info!("the current Build could not be read: this turn takes no planned births, accepted meshes or round step from it"));
                                                            unreadable_named = Some(text);
                                                        }
                                                        None
                                                    }
                                                };
                                                // The current Build's planned births are what a mesh's readiness is counted over.
                                                if let Some(Some(b)) = &current {
                                                    records.set_planned(&b.topology);
                                                }
                                                records.set_heard_ready(records.planned().keys().chain(std::iter::once(&mesh)).filter(|m| membership.mesh_status(m).is_some_and(|h| h.status == rdm_transport::membership::MESH_READY_FOR_TRAFFIC)).cloned().collect());
                                                // A MeshStatus Dead is bound to the mesh id its publisher's own live digest names.
                                                records.set_heard_dead(membership.mesh_statuses().into_iter().filter(|(_, h)| h.status == "dead").map(|(m, h)| (m, h.publisher, h.changed_at_rafka_ms)).collect(), |mesh, publisher| {
                                                    membership.book.all().into_iter().find(|d| d.node.name.mesh == mesh && d.node.name.to_string() == publisher).and_then(|d| d.mesh_id)
                                                });
                                                // Decide on the view as it is now: a cached view can predate the seat record this admin holds
                                                // (a hand-over just taken), and a seat is announced from the decision, never from a stale one.
                                                // The seat records are read once per decision: the projection elects from them and
                                                // the announcement below supersedes only them (`seat_announcements`).
                                                // Read under the view's write lock, as the projection always was: the installed
                                                // view is never older than the records it replaces.
                                                let mut decided = crate::election::Incumbency::default();
                                                let t = records
                                                    .install_read(&me, &topology, &view_book, &view_fabric_id, |reads| {
                                                        decided = incumbency_of(&view_book, &records);
                                                        project_decided(&view_fabric, &view_fabric_id, view_provider, &view_book, &records, &decided, reads)
                                                    })
                                                    .await;
                                                // The accepted Build is the authority on which meshes the fabric has: the book
                                                // takes no old aggregate of a mesh it does not name, and a mesh it does not name
                                                // that has no member left keeps no recorded id (a name added again is a new mesh).
                                                if let Some(Some(b)) = &current {
                                                    membership.book.set_accepted_meshes(b.topology.meshes.keys().cloned().collect());
                                                    records.meshes.lock().unwrap().retain(|name, _| b.topology.meshes.contains_key(name) || t.members().any(|n| &n.mesh == name));
                                                }
                                                // Cut off, or within one silence window of healing, its view
                                                // authorizes nothing: it publishes as no primary.
                                                let live = membership.authorizes();
                                                let mesh_primary = live && t.holds_mesh_seat(&mesh, &seat_node_id);
                                                // The mesh's rows on the shared root follow this seat (`MeshRowsWriter`).
                                                own_seat_tx.send_if_modified(|held| std::mem::replace(held, mesh_primary) != mesh_primary);
                                                let fabric_primary = live && t.holds_fabric_seat(&seat_node_id);
                                                // A role taken or given up this round is held from the anti-entropy until its
                                                // seat is announced below: a role's first aggregate follows its seat record.
                                                let announcing = if (mesh_primary, fabric_primary) != (was_mesh_primary, was_fabric_primary) { Some(roles.write().await) } else { None };
                                                // The declarations held before this admin wrote rows (Q59 = x) are its first write
                                                // after it takes a seat that writes them.
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
                                                    let adopted = membership.fabric_status().is_some_and(|s| s.status == "degraded");
                                                    records.set_adopted_degraded(adopted);
                                                }
                                                if fabric_primary {
                                                    records.set_published_degraded(false);
                                                    crate::investigate::restore_peer_recovery(&me, &records, &t);
                                                } else {
                                                    // The seat is released in one place and one order: the published `degraded` is
                                                    // held before the authority records it replaces are cleared, so no projection
                                                    // between the two answers ready for a fabric the holder published degraded.
                                                    records.set_published_degraded(membership.fabric_status().is_some_and(|s| s.status == "degraded"));
                                                    records.clear_peer_recoveries();
                                                    records.set_adopted_degraded(false);
                                                }
                                                was_fabric_primary = fabric_primary;
                                                // The fabric-primary's climb through the fabric states, one non-blocking turn per round
                                                // of this loop; every other admin serves the state the holder published.
                                                fabric_rounds.climb_step(crate::fabric_rounds::ClimbInputs { is_fabric_primary: fabric_primary, heard: membership.fabric_status().as_ref().map(|f| f.status.as_str()), view: &t }).await;
                                                // A seat taken is announced once, when it changes hands: the mesh primary first, then
                                                // the fabric primary (R-A2).
                                                // Each seat taken is written as the fabric's next seat move before it is said (Q48).
                                                let said = seat_announcements(&t, live, &mesh, (&seat_node_id, &seat_incarnation), &decided);
                                                let me_birth = rdm_core::handover::HandoverBirth { mesh: mesh.clone(), node_id: seat_node_id.clone(), incarnation: seat_incarnation.clone() };
                                                let mut seated_again = (false, false);
                                                for (seat, holder) in crate::fabric_handover::elect(&*seat_storage, membership.seats(), &me_birth, &decided, said).await {
                                                    if holder.node_id == seat_node_id {
                                                        match seat {
                                                            rdm_core::Seat::MeshPrimary => seated_again.0 = true,
                                                            rdm_core::Seat::FabricPrimary => seated_again.1 = true,
                                                            rdm_core::Seat::Cohort(_) => {}
                                                        }
                                                    }
                                                    backbone.announce_seat(seat, holder).await;
                                                }
                                                // A seat its own earlier transfer moved away left that seat's stores cut off
                                                // (`WriteCutOff::Committed`); the election row seating it again, after the
                                                // successor's process was proven gone, makes it that seat's row writer from the
                                                // row's revision (node-primary-update.md: "A holder the rule seats again writes
                                                // its seat record at the next revision first and announces it second ... and
                                                // restores only those seats' writes under the new revision"). A store standing
                                                // open is left as it is.
                                                if seated_again.0 || seated_again.1 {
                                                    (seat_cut_off)(crate::fabric_handover::WriteCutOff::Restore { reason: format!("{me}: its election row seated it again"), mesh_seat: seated_again.0, fabric_seat: seated_again.1 }).await;
                                                }
                                                if let Some(announced) = announcing {
                                                    drop(announced);
                                                    roles_moved.notify_one();
                                                }
                                                let heard = membership.book.current(membership.book.staleness_floor());
                                                let status_of = |s: crate::model::ScopeStatus| serde_json::to_value(s).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
                                                // A status is sent when it changed, never per round (gossip.md §3.2).
                                                let mesh_status = t.meshes.iter().find(|m| m.name == mesh).map(|m| status_of(m.status)).unwrap_or_default();
                                                // The fabric's status as the live records state it: a round that just cleared the adopted
                                                // degraded must not be answered by the older projection `t`.
                                                // The fabric-primary publishes the state it holds now, not the projection's last refresh:
                                                // every state the climb enters is its own frame.
                                                let held_state = records.fabric_state();
                                                let fabric_status = |records: &Records| {
                                                    let basis = if fabric_primary { held_state.scope() } else { t.fabric.status };
                                                    match basis {
                                                        crate::model::ScopeStatus::Degraded | crate::model::ScopeStatus::ReadyForTraffic => {
                                                            status_of(if records.adopted_degraded() || records.peer_recovering() { crate::model::ScopeStatus::Degraded } else { crate::model::ScopeStatus::ReadyForTraffic })
                                                        }
                                                        other => status_of(other),
                                                    }
                                                };
                                                let mut announced = backbone.announce_statuses(&mesh_status, &fabric_status(&records), false, false).await;
                                                // A status adopted from the previous publisher is republished only when its round is
                                                // complete: the planned births checked in, every mesh primary reported.
                                                if announced.mesh_awaiting_round || announced.fabric_awaiting_round {
                                                    // An unreadable builds store steps no round: the adopted status waits, named above.
                                                    if let Some(current) = &current {
                                                        let planned = current.as_ref().map(|b| crate::round::planned(&b.topology, &mesh));
                                                        // The round a new primary runs before it republishes the status it adopted (R-S3).
                                                        let done = fabric_rounds.adopted_step(announced, crate::fabric_rounds::AdoptedInputs { view: &t, planned, book: &membership.book });
                                                        if done.mesh || done.fabric {
                                                            announced = backbone.announce_statuses(&mesh_status, &fabric_status(&records), done.mesh, done.fabric).await;
                                                        }
                                                    }
                                                } else {
                                                    fabric_rounds.adopted_end();
                                                }
                                                // The state just announced is the one the climb may move on from.
                                                if records.set_published_fabric_state((fabric_primary && !announced.fabric_awaiting_round).then_some(held_state)) && fabric_primary {
                                                    fabric_rounds.poke_climb();
                                                }
                                                records.set_declare_gate(mesh_primary && !announced.mesh_awaiting_round);
                                                // R-S2: a mesh primary the fabric-primary's view gained or changed is sent the down op.
                                                fabric_rounds.address_changed_mesh_primaries(&t, fabric_primary);
                                                let addr = rdm_transport::membership::gossip_addr;
                                                let admins: Vec<_> = heard.iter().filter(|d| d.node.name.kind == NodeKind::NodeAdmin && d.node.name != me).filter_map(addr).collect();
                                                builds.join_admins(admins.clone()).await;
                                                backbone.join_admins(admins).await;
                                                // The round dials no member of its own mesh: a member joins through this admin, and a
                                                // held member gone stale is dialled by §6's repair alone (gossip.md §6, §8; Q84).
                                                drop(turn);
                                                // The next round is due after one gossip interval, or at once when a checklist the
                                                // fabric-primary waits on, or the application's answer, moved, or when the view changes
                                                // a role or a status it announces: this admin takes or loses a seat, or its mesh's
                                                // status moves (gossip.md §3.2, a status change is sent at once).
                                                let round_wake = tokio::time::sleep_until(tokio::time::Instant::now() + gossip_interval());
                                                tokio::pin!(round_wake);
                                                loop {
                                                    tokio::select! {
                                                        _ = &mut round_wake => break,
                                                        _ = fabric_rounds.climb_due() => break,
                                                        _ = records.wake.report_notify.notified() => break,
                                                        _ = records.view_changed.notified() => {
                                                            let now = topology.read().await.clone();
                                                            let live = membership.authorizes();
                                                            let (mp, fp) = (live && now.holds_mesh_seat(&mesh, &seat_node_id), live && now.holds_fabric_seat(&seat_node_id));
                                                            let status = now.meshes.iter().find(|m| m.name == mesh).map(|m| status_of(m.status)).unwrap_or_default();
                                                            if (mp, fp) != (mesh_primary, fabric_primary) || status != mesh_status || announced.mesh_awaiting_round || announced.fabric_awaiting_round || records.peer_recovering() {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    // The Members anti-entropy (gossip.md §3.2, "PERPETUAL TIMERS … Members anti-entropy —
                                    // the mesh primary on the backbone; also sent at once on a material change"): its own
                                    // timer, never a step of the hierarchy's round. It reads the book and the roles the
                                    // hierarchy decided, nothing else, so no store read or write of the round (the
                                    // current Build, the exit rows, the view's installation, the climb) delays
                                    // it: a peer's t of this mesh (mesh-investigation.md) is the publication's own
                                    // interval, never the store's. While this admin is its mesh's primary it publishes
                                    // its mesh's members on the backbone (and puts the same source into its own mesh);
                                    // no admin publishes a live mesh on its behalf (R-RX4). While it is the fabric
                                    // primary it publishes each deleted mesh whose last admin it stopped or deleted
                                    // (mesh-delete.md step 7). Held at its own wait while the admin is parked.
                                    {
                                        let (backbone, membership, park) = (backbone.clone(), membership.clone(), park_gate.clone());
                                        anti_entropy = tokio::spawn(async move {
                                            loop {
                                                let turn = park.turn("anti-entropy").await;
                                                {
                                                    let _announced = roles.read().await;
                                                    backbone.publish(&membership, backbone.own_members(&membership)).await;
                                                }
                                                drop(turn);
                                                tokio::select! {
                                                    _ = backbone.round_wake(&membership, tokio::time::Instant::now() + rdm_transport::membership::backbone_gossip_interval()) => {}
                                                    _ = roles_moved.notified() => {}
                                                }
                                            }
                                        });
                                    }
                                    // The projection, refreshed into the control plane's topology.
                                    {
                                        let (topology, fabric, fabric_id, provider, book, records) = (control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone());
                                        let elections = ElectionLog::new(name.clone());
                                        let me = name.clone();
                                        // Not spawned behind the gate: the rebuild takes the view's lock, which every handler of a
                                        // parked admin reads (its start among them). It is held at its own wait instead
                                        // (`view_rebuild_due`), where it holds nothing.
                                        let rebuild_gate = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut held: BTreeSet<PathName> = BTreeSet::new();
                                            let mut facts = book.routing_watch();
                                            let mut turn = rebuild_gate.turn("view-rebuild").await;
                                            loop {
                                                facts.seen();
                                                let me_now = book.held_path_name().unwrap_or_else(|| me.clone());
                                                let t = records.install_read(&me_now, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                elections.observe_as(&me_now, &t);
                                                // A name this view held and now lacks: the exclusion is named from the same
                                                // inputs at the moment it happens, not reconstructed later.
                                                let now: BTreeSet<PathName> = t.members().map(|n| n.name.clone()).collect();
                                                for gone in held.difference(&now) {
                                                    tracing::info_span!("rdm.node_admin.topology.update.via-vanished-from-view", node = %me_now, name = %gone, why = %describe_absence(&book, &records, gone))
                                                        .in_scope(|| tracing::info!("a name this admin's view held is excluded by the projection now"));
                                                }
                                                held = now;
                                                drop(turn);
                                                turn = view_rebuild_due(&mut facts, VIEW_POLL, &rebuild_gate).await;
                                            }
                                        }));
                                    }
                                    // The peer-mesh investigation (`crate::investigate`): while this admin is the fabric primary,
                                    // a peer mesh it stops hearing on the backbone is probed through its own members and recovered
                                    // only when one of them cannot reach its own node-admin. Its decision is what releases the
                                    // drift check below to replace that mesh's node-admins.
                                    let ladder = Arc::new(Mutex::new(crate::investigate::Ladder::new(crate::investigate::Rungs::configured())));
                                    {
                                        // The R-D1 fence as the Build door and the drive read it: t from this admin's last direct
                                        // backbone receipt of the mesh, whether or not the ladder has stepped the mesh yet.
                                        let (ladder, book) = (ladder.clone(), book.clone());
                                        let _ = control.peer_mesh.set(Arc::new(move |mesh: &str| ladder.lock().unwrap().fence(mesh, book.mesh_unheard(mesh, std::time::Instant::now()))));
                                    }
                                    {
                                        let held = connections.held();
                                        let watch = crate::investigate::Watch {
                                            gate: park_gate.clone(),
                                            me: name.clone(),
                                            me_id: node_id.clone(),
                                            round: rdm_transport::membership::backbone_gossip_interval(),
                                            membership: membership.clone(),
                                            topology: control.topology.clone(),
                                            records: records.clone(),
                                            settled: { let builds = builds_dyn.clone(); Arc::new(move || { let builds = builds.clone(); Box::pin(async move { builds.current().await.map(|c| c.is_some_and(|b| !matches!(b.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running))).map_err(|e| e.to_string()) }) }) },
                                            client: node_rpc.client.clone(),
                                            connected: Arc::new(move |id: &NodeId| held.lock().unwrap().own_latest_directs().iter().any(|d| &d.destination.node_id == id && d.state == rdm_core::connections::ConnectionState::Connected)),
                                            ladder: ladder.clone(),
                                            carriers: Mutex::new(BTreeMap::new()),
                                        };
                                        // Not behind the gate: it reads the view; it is held at its own wait (`Watch::gate`).
                                        tasks.push(tokio::spawn(crate::investigate::run(watch)));
                                    }
                                    let executor;
                                    // The reconcile loop: the drift check, and the fabric-primary's drive of every active Build
                                    // (`build_drive`). Only the fabric-primary drives; an executor runs what it is called with
                                    // (`build_run`) and picks nothing up from its projection. A launched admin starts it only after
                                    // its entry pull, so its first view is its launcher's.
                                    {
                                        let (topology, submitted) = (control.topology.clone(), control.build_submitted.clone());
                                        let (book, records, fabric, fabric_id, provider, cut_off_view) = (book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, membership.clone());
                                        let (me, me_id, deployer, drift_builds, drift_contexts) = (name.clone(), node_id.clone(), runner.provider.clone(), builds_dyn.clone(), attempt_contexts.clone());
                                        let drift_admission = control.admission.clone();
                                        let (drives, drive_env) = (drives.clone(), drive_env.clone());
                                        let drift_time = rafka_time.clone();
                                        let drift_nodes = nodes_storage.clone();
                                        let ladder = ladder.clone();
                                        let park = park_gate.clone();
                                        let drift_seam = wiring.drift_seam.take();
                                        executor = tokio::spawn(async move {
                                            let mut started = HashSet::new();
                                            let mut held = HashSet::new();
                                            let exit_verdicts = ExitVerdicts::default();
                                            loop {
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
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("executor").await;
                                                // Decide on, and plan from, the view as it is now: a cached view
                                                // can predate the members that make another admin primary.
                                                let now = records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                // Every Build whose next operation this admin executes; none
                                                // while it is cut off or within one silence window of healing
                                                // (its view then authorizes nothing). The fabric authority first
                                                // starts a reconciliation Build for proven drift.
                                                if cut_off_view.authorizes() {
                                                    let durable_rows = if now.holds_fabric_seat(&me_id) { drift_nodes.runtimes().await.unwrap_or_default() } else { Vec::new() };
                                let taken = drive_env.departure.taken_from().await;
                                let opened =                     reconcile_drift(&drift_time, &me, &me_id, &now, &book, &*deployer, &*drift_builds, &drift_contexts, &drift_admission, &durable_rows, &mut started, &mut held, &exit_verdicts, &|mesh| ladder.lock().unwrap().fence(mesh, book.mesh_unheard(mesh, std::time::Instant::now())).and_then(|p| p.unheard_ms), &|mesh| (book.backbone_meshes().contains(mesh) || now.members().any(|n| n.mesh == mesh)) && !ladder.lock().unwrap().recovery_decided(mesh), &|mesh| ladder.lock().unwrap().recovery_decided(mesh), &|mesh| ladder.lock().unwrap().unreachable_decided(mesh), &records, taken.as_ref()).await;
                                                    // The proofs the pass just took are in the view the attempt is planned from: a birth
                                                    // proven exited is not live there, and holds no seat.
                                                    if opened.is_some() {
                                                        records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                    }
                                                    if now.holds_fabric_seat(&me_id) {
                                                        crate::build_op::ensure_active(&drives, &drive_env).await;
                                                    }
                                                }
                                                drop(turn);
                                                tokio::select! {
                                                    _ = submitted.notified() => {}
                                                    _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                                                }
                                            }
                                        });
                                    }
                                    // The stopped-node garbage collector (node-identity-and-state.md §6.9): a sweep every ttl, by the
                                    // fabric-primary only, while its view authorizes.
                                    // A blank ttl runs no sweep. The sweep submits one ordinary node.delete Build and deletes nothing.
                                    if let Some(ttl) = cfg.stopped_node_ttl {
                                        let (control, cut_off_view) = (control.clone(), membership.clone());
                                        let park = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                tokio::time::sleep(ttl).await;
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("stopped-sweep").await;
                                                if cut_off_view.authorizes() {
                                                    let swept = control.sweep_stopped(Some(ttl)).await;
                                                    tracing::debug!(?swept, "stopped-node sweep");
                                                }
                                                drop(turn);
                                            }
                                        }));
                                    }
                                    *out.lock().unwrap() = Some(Running { rafka_time: rafka_time.clone(), api_base, control, runner, membership, digest, node_rpc, backbone: backbone.clone(), router: iroh_router, rpc_server: rpc_server.clone(), publisher, executor, hierarchy, anti_entropy, tasks, start_tasks, leave_seam: wiring.leave_seam.take(), builds: builds.clone(), before_ready_cancel, before_ready: before_ready_cell.clone(), member_cert, gate: park_gate.clone(), node_self: own_commands.get().cloned().expect("this admin's command surface is set before it runs"), authority: authority.clone(), data_connections: data_connections.clone() });
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
    let mut start = crate::workflows::node_start::FirstStart { types: join_types,
        starting: Some(starting),
        joining: Some(joining),
        resolve: Some(resolve),
        digest: join_digest,
        enter: Some(enter),
        joined: None,
        hook_started: None,
    };
    crate::workflows::node_start::node_start(&mut start, crate::before_ready::StartKind::FirstStart).await?;
    let running = out.lock().unwrap().take();
    running.ok_or_else(|| "the start ended without the node-admin it brought up".to_string())
}
