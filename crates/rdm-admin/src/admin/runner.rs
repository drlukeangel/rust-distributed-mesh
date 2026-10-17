//! AdminRunner, GossipLifecycle, and mesh state handoff.

use super::*;

use crate::seat_revision::FenceOutcome;

/// Realises Build operations through the deployment and lifecycle pipelines.
pub struct AdminRunner {
    /// The provider that realises runtimes.
    pub provider: Arc<dyn DeploymentProvider>,
    /// The births this admin deployed and awaits a `JoinNode` from.
    pub joins: Arc<crate::join::Joins>,
    /// The observer of births' membership.
    pub observer: Arc<MembershipObserver>,
    /// The records this admin holds.
    pub records: Arc<Records>,
    /// The Build state.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// This admin's own Build log, the one `builds` reads: facts read from another admin are
    /// absorbed into it without authoring Build gossip.
    pub local: Arc<dyn crate::build_state::LocalBuildLog>,
    /// The facts every launch carries.
    pub template: LaunchTemplate,
    /// The environment passed to every node-admin launch.
    pub admin_env: BTreeMap<String, String>,
    /// The directory the node executables are looked up in.
    pub bin_dir: PathBuf,
    /// Explicit executable bindings: when set, every launch runs its bound executable and
    /// `bin_dir` is never consulted.
    pub bindings: Option<rdm_core::binding::Validated>,
    /// The lifecycle transitions of the births this admin executes.
    pub lifecycle: LifecycleTransitionPipeline,
    /// The runtimes this admin started, by node.
    pub handles: Mutex<HashMap<PathName, (Node, DeploymentHandle)>>,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// What the views are projected from.
    pub fabric: String,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The provider that runs the fabric's nodes.
    pub fabric_provider: ProviderKind,
    /// The digest book.
    pub book: DigestBook,
    /// This admin's own path.
    pub me: PathName,
    /// This admin's node id (the adopter on every runtime it adopts).
    pub node_id: NodeId,
    /// This admin's Iroh endpoint: it asks a node directly whether it answers.
    pub endpoint: Option<Endpoint>,
    /// This process's one Node RPC client (and its live resolver): every running admin holds it.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// Where the retire pipeline's lifecycle events go: this admin's membership and backbone.
    pub lifecycle_events: Arc<dyn crate::deployment::pipeline::LifecycleEvents>,
    /// The mesh leaves this admin, as the fabric-primary, has admitted and awaits a handoff for.
    pub leaves: Arc<crate::mesh_delete::MeshDeleteBook>,
    /// The mesh leaves this admin, as a leaving mesh's primary, is running or has finished.
    pub mesh_delete_runs: crate::mesh_delete::MeshDeleteRuns,
    /// The fabric-primary handover: this admin's initiator, doors and recorded operations.
    pub handover: Arc<crate::fabric_handover::HandoverDoor>,
    /// The signer this admin issues a new mesh's first admin's issuing material with.
    pub issuer: Arc<crate::certs::CertIssuer>,
    /// This admin's status.storage: as a mesh primary, the exit-proof rows of its members' exited
    /// births, which a recovery publishes node.deleted from.
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// This admin's mesh.storage: as a mesh primary, its mesh's row Dead at mesh.deleted, and as
    /// fabric-primary at mesh-left, a leaving mesh's row Dead when no primary survived (R-MD1).
    pub mesh_storage: Arc<dyn crate::storage::MeshStorage>,
    /// This admin's fabric.storage: as fabric-primary, releases the fabric name at fabric.deleted.
    pub fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage>,
}

/// The lifecycle events of an operation this admin executes, on its own mesh channel only. The
/// backbone carries them as the open overlays and retained departures of this mesh's aggregate,
/// which its mesh primary publishes; a peer mesh's primary forwards them onto its own channel.
pub struct GossipLifecycle {
    /// This admin's mesh channel.
    pub membership: Membership,
    /// This admin's place on the backbone: the fabric round hooks publish there as well.
    pub backbone: Backbone,
    /// This admin's own path: the publisher of the mesh frames it authors.
    pub me: PathName,
    /// This admin's mesh.delete door, once its runner exists: every birth this executor publishes
    /// deleted is handed to it (mesh-left follows the final primary's exit proof, R-MD1 item 2).
    pub mesh_deleter: crate::mesh_delete::MeshDeleterSlot,
    /// This admin's view projected and installed at once ([`view_projector`]): a stop this executor
    /// applied to its own book is in its view before the next step decides from it, as a moved row is.
    pub project: Option<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>,
}

impl GossipLifecycle {
    #[allow(dead_code)]
    pub(crate) fn held_name(&self) -> PathName {
        self.membership.book.held_path_name().unwrap_or_else(|| self.me.clone())
    }
}

#[async_trait::async_trait]
impl crate::deployment::pipeline::LifecycleEvents for GossipLifecycle {
    fn now_rafka_ms(&self) -> u64 {
        self.membership.clock().now_rafka_ms()
    }
    async fn deleting(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-deleting", node = %op.name, node_id = %op.node_id, build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rdm_transport::membership::StatusEvent::Deleting, op).await;
            tracing::info!("the node is being removed: every mesh hears it is not routable");
        }
        .instrument(span)
        .await
    }
    async fn restarting(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-restarting", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rdm_transport::membership::StatusEvent::Restarting, op).await;
            tracing::info!("the node is being restarted: every mesh holds it through its Leaving");
        }
        .instrument(span)
        .await
    }
    async fn stopped(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-stopped", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rdm_transport::membership::StatusEvent::Stopped, op).await;
            if let Some(project) = &self.project {
                project().await;
            }
            tracing::info!("the node is stopped: every mesh drops it from its book; no heartbeat of it ends the stop, only this mesh-primary's node.joined");
        }
        .instrument(span)
        .await
    }
    async fn stop_completed(&self, op: &rdm_core::LifecycleOp) {
        self.backbone.announce_update_of_stopped_holder(op).await;
    }
    async fn joined(&self, op: &rdm_core::LifecycleOp) {
        self.membership.book.joined(op);
    }
    async fn leaving(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-stop-admitted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        span.in_scope(|| {
            self.membership.book.admitted(op, MemberStatus::Leaving);
            tracing::info!("the birth admitted stop-node: this mesh-primary sets it stopping and its projection carries that; no frame is published");
        });
    }
    async fn deleted(&self, op: &rdm_core::LifecycleOp) {
        let f = rdm_transport::membership::Frame::NodeDeleted { op: op.clone(), forwarded_by: None };
        let span = tracing::info_span!("rdm.node_admin.node.delete.via-node-deleted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            if let Err(e) = self.membership.publish_lifecycle(&f).await {
                tracing::info!(error = %e, "NodeDeleted not sent on the mesh channel");
            }
            tracing::info!("the exact birth is proven terminal: it has left");
        }
        .instrument(span)
        .await;
        if let Some(mesh_deleter) = self.mesh_deleter.get().cloned() {
            mesh_deleter.birth_deleted(op).await;
        }
    }
}

impl AdminRunner {
    /// The name this admin currently holds in its membership digest book, or its boot name before its
    /// birth is in the book.
    pub(crate) fn held_name(&self) -> PathName {
        self.book.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// Publish the view as it is now, so a Build that completes after this
    /// operation is never read back against an older view.
    pub(crate) async fn refresh_view(&self) {
        let me = self.held_name();
        self.records.install_read(&me, &self.topology, &self.book, &self.fabric_id, |reads| project_with(&self.fabric, &self.fabric_id, self.fabric_provider, &self.book, &self.records, reads)).await;
    }

    /// A launch of `kind` in `mesh`. Every node carries its mesh's identity (it
    /// names the mesh's channel): the one this admin created, else the one the
    /// fabric knows (a recovered mesh is reconstructed as itself, never anew).



    pub(crate) async fn template_for(&self, kind: NodeKind, mesh: &str) -> Result<LaunchTemplate, String> {
        let known = self.topology.read().await.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone());
        let mut t = self.template.clone();
        t.launcher.name = self.held_name();
        t.executable = match &self.bindings {
            // Explicit mode: the bound executable, re-hashed now; an unbound kind is refused, never
            // launched from the built-in set.
            Some(b) => {
                let r = resolve_bound(b, kind).await.map_err(|e| format!("{kind:?} in {mesh}: {e}", kind = kind.name()))?;
                tracing::info!(launch_id = %r.launch_id, executable = %r.executable.display(), sha256 = %r.sha256, candidate = %r.candidate.sha, "launching from the explicit executable binding");
                r.executable
            }
            // The built-in set: the node-admin, and the one node executable every other node_type
            // runs, taking its node_type from the launch (rafka-node-base `run_launched`). The rpc
            // node is the testkit's proof node: the proof protocols on top of the node runtime.
            None => {
                let name = match kind {
                    NodeKind::NodeAdmin => "rafka-node-admin",
                    NodeKind::RpcNode => "rafka-rpc-node",
                    _ => "rafka-node",
                };
                self.bin_dir.join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
            }
        };
        if kind == NodeKind::NodeAdmin {
            t.env.extend(self.admin_env.clone());
        }
        // Every node of a mesh carries its id: it names the mesh's channel.
        if let Some(id) = self.records.meshes.lock().unwrap().get(mesh).cloned().or(known) {
            t.env.insert(rdm_core::launch::ENV_MESH_ID.into(), id.to_string());
        }
        if mesh != self.held_name().mesh {
            let peers: Vec<(String, SocketAddr)> = self
                .topology
                .read()
                .await
                .members()
                .filter(|n| n.mesh == mesh && matches!(n.status, NodeStatus::ReadyForTraffic | NodeStatus::PendingReconnect))
                .filter_map(|n| Some((n.endpoint_id.as_ref()?.0.clone(), n.transport_addr?)))
                .collect();
            for p in peers {
                if !t.seeds.contains(&p) {
                    t.seeds.push(p);
                }
            }
        }
        Ok(t)
    }

    /// `template` for the launch of a mesh's FIRST node-admin: it carries the mesh's issuing
    /// material, issued by the app's signer on this admin's rafka-time. A refusal fails the create
    /// by name: the admin is never launched without it.
    pub(crate) fn with_mesh_issuer(&self, mut t: LaunchTemplate, mesh: &str) -> Result<LaunchTemplate, String> {
        let issuer = self.issuer.mesh_issuer(mesh).map_err(|e| match e {
            crate::certs::IssueFailure::Refused(r) => format!("{mesh}: the cert signer refused to issue the mesh's issuing material: {r}"),
            crate::certs::IssueFailure::NoRafkaTime(why) => format!("{mesh}: {why}"),
        })?;
        t.mesh_issuer = Some(issuer).filter(|b| !b.is_empty());
        Ok(t)
    }

    pub(crate) fn pipeline<'a>(&'a self, template: &'a LaunchTemplate) -> DeploymentPipeline<'a> {
        self.pipeline_with(template, &*self.builds)
    }

    /// [`Self::pipeline`] writing its receipts to and reading its Build from `builds` (an attempt's
    /// lane, [`crate::build_run::LaneBuilds`]).
    pub(crate) fn pipeline_with<'a>(&'a self, template: &'a LaunchTemplate, builds: &'a dyn BuildStateAdapter) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: &*self.provider,
            joins: &self.joins,
            observer: &*self.observer,
            sink: &*self.records,
            lifecycle: &*self.lifecycle_events,
            builds,
            template,
            timeouts: Timeouts::default(),
        }
    }

    /// The runtime of `node`'s birth: this admin's own handle, or the exact
    /// runtime the birth publishes with its membership (`MeshNode::runtime`),
    /// adopted when its provider and control domain are this admin's. No
    /// Build history is read: whoever launched it, and however long ago, the
    /// birth itself says where its runtime is. A fact from another provider
    /// or control domain is refused by name, never acted on.
    pub(crate) async fn handle_for(&self, node: &Node) -> Result<(Node, DeploymentHandle), String> {
        if let Some((rec, h)) = self.handles.lock().unwrap().get(&node.name).cloned() {
            if node.incarnation_id.is_none() || rec.incarnation_id == node.incarnation_id {
                return Ok((rec, h));
            }
        }
        let me = self.held_name();
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", node.name))?;
        // The birth says where its runtime is: its digest, or (a birth this admin never heard) the
        // durable runtime row its maker handed over.
        let held = self
            .book
            .get(node.node_id.as_str())
            .or_else(|| self.book.projected_at(node.node_id.as_str(), std::time::Instant::now()))
            .map(|(d, _)| d)
            .or_else(|| self.book.parked().into_iter().find(|d| d.node.node_id == node.node_id))
            .filter(|d| d.node.incarnation == incarnation);
        let row = match (&held, self.records.contacts.get()) {
            (None, Some(store)) => birth_runtime_row(&**store, &me, &node.name, &node.node_id, &incarnation).await?,
            _ => None,
        };
        let (published, data_dir) = match (&held, &row) {
            (Some(d), _) => (d.node.runtime.clone(), d.data_dir.clone()),
            (None, Some(r)) => (Some(r.runtime.clone()), r.data_dir.clone()),
            (None, None) => return Err(format!("{} (birth {}) is not held in this admin's membership or its durable runtime rows; its runtime is unknown", node.name, incarnation.as_str())),
        };
        let refuse = |reason: &str, detail: String| {
            let span = match reason {
                "unpublished" => tracing::info_span!("rdm.node_admin.runtime.reject.via-unpublished", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %me, detail = %detail),
                "other-provider" => tracing::info_span!("rdm.node_admin.runtime.reject.via-other-provider", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %me, detail = %detail),
                "foreign-control-domain" => tracing::info_span!("rdm.node_admin.runtime.reject.via-foreign-control-domain", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %me, detail = %detail),
                _ => tracing::info_span!("rdm.node_admin.runtime.reject.via-invalid-fact", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %me, detail = %detail),
            };
            span.in_scope(|| tracing::info!("runtime not adopted"));
            format!("{} (birth {}): {detail}", node.name, incarnation.as_str())
        };
        let fact = published.ok_or_else(|| refuse("unpublished", "the birth publishes no runtime fact; it cannot be adopted".into()))?;
        let handle = crate::deployment::provider::adopt(&*self.provider, &fact).map_err(|r| refuse(r.reason(), r.to_string()))?;
        let mut record = node.clone();
        record.deployment_id = Some(handle.deployment_id.clone());
        if record.data_dir.is_none() {
            record.data_dir = data_dir;
        }
        tracing::info_span!(
            "rdm.node_admin.runtime.update.via-adopt",
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.as_str(),
            deployment_id = %fact.deployment_id,
            provider = fact.provider.as_str(),
            provider_control_domain_fingerprint = %fact.domain_fingerprint(),
            runtime_locator_kind = fact.locator.kind(),
            runtime_locator_fingerprint = %fact.locator_fingerprint(),
            source = if held.is_some() { "self-published-membership" } else { "durable-runtime-row" },
            adopter = %me,
            adopter_node_id = %self.node_id,
            execution_node_id = %self.node_id,
        )
        .in_scope(|| tracing::info!("adopted a runtime this admin did not launch"));
        self.handles.lock().unwrap().insert(node.name.clone(), (record.clone(), handle.clone()));
        Ok((record, handle))
    }

    /// Fence `path` before a new birth (`crate::fence`): this admin answers the fence's questions.
    pub(crate) async fn revise_predecessor(&self, path: &PathName) -> FenceOutcome {
        let prev = self.topology.read().await.node(path).cloned();
        let out = crate::seat_revision::fence(path, prev, self).await;
        if matches!(out, FenceOutcome::Clear { .. }) {
            self.handles.lock().unwrap().remove(path);
        }
        out
    }

    /// Does `node`'s runtime answer when asked directly (not through gossip)?
    /// A Node RPC `Ping`, two seconds; a frozen or gone runtime does not answer.
    pub(crate) async fn answers(&self, node: &Node) -> bool {
        const WITHIN: Duration = Duration::from_secs(2);
        // Every kind, a node-admin included, serves Ping on its one endpoint.
        let node_rpc = &self.node_rpc;
        let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Overall(WITHIN), ..Default::default() };
        let req = rdm_core::ping::PingRequest::Ping { payload: b"fence".to_vec() };
        let (out, _) = node_rpc.client.call::<rdm_core::ping::Ping>(&rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), &req, &opts).await;
        out.reply().is_some()
    }

    /// `Node: Pending -> ReadyForTraffic`, committed once the node reports ready.
    pub(crate) async fn bring_into_traffic(&self, node: &Node) -> Result<(), String> {
        let key = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
        let shape = ShapeFacts { desired_meshes: self.topology.read().await.meshes.len() as u32 };
        let ready = self.observer.ready(node).await;
        let birth = node.incarnation_id.as_ref().ok_or_else(|| format!("{} has no known birth", node.name))?;
        let t = Transition { transition_id: Transition::id_for_birth(key, &node.name.to_string(), &birth.as_str()), target: node.name.to_string(), key, shape: &shape };
        match self.lifecycle.transition(t, || ready, || {}).await {
            TransitionResult::Committed => Ok(()),
            other => Err(format!("{}: {other:?}", node.name)),
        }
    }

    /// The births' runtimes this admin holds, durable rows and the digests it hears now: the map
    /// a node-admin it launches starts from (a successor fabric primary proves exits from it).
    pub(crate) async fn held_runtimes(&self) -> Vec<crate::storage::RuntimeRow> {
        let mut rows: BTreeMap<String, crate::storage::RuntimeRow> = BTreeMap::new();
        if let Some(store) = self.records.contacts.get() {
            for r in store.runtimes().await.unwrap_or_default() {
                rows.insert(r.key(), r);
            }
        }
        for d in self.book.all() {
            if let Some(runtime) = d.node.runtime.clone() {
                let r = crate::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime, data_dir: d.data_dir.clone() };
                rows.insert(r.key(), r);
            }
        }
        rows.into_values().collect()
    }

    pub(crate) async fn create(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, before_ready: Option<crate::deployment::pipeline::BeforeReady>) -> Result<(), String> {
        match self.revise_predecessor(node).await {
            // The member is alive (it answers) or held (its exact runtime still runs):
            // nothing to create. Silence never authorizes a replacement.
            FenceOutcome::Alive | FenceOutcome::Held => return Ok(()),
            // Nothing proves the path free: the create fails, naming what could not be read.
            FenceOutcome::RecordsUnreadable { reason } => return Err(format!("{node}: the runtimes recorded at this path cannot be read, so no birth is created over them: {reason}")),
            // A birth the provider proves terminal holds the path's name until it is deleted (only a
            // delete releases a name, R-MN1): its exit proof is recorded, node.delete deletes it as a
            // crashed target (NodeDeleted, its Deleted row), then the create runs (node-recovery.md
            // "Steps": node.delete of the exited instance, then node.create at the same path.name).
            FenceOutcome::Clear { gone: Some(prev) } => {
                if let Some(incarnation) = prev.incarnation_id.clone() {
                    let exit = self.records.put_exit(&prev, None, "provider").await.map_err(|e| format!("{node}: the provider proves birth {} at {} exited, and its exit-proof row is not written, so it is not deleted and nothing is created over it: {e}", prev.node_id, incarnation.as_str()))?;
                    self.delete_crashed(&*self.builds, build_id, attempt, node, &crate::build::ExitedBirth { node_id: prev.node_id.clone(), incarnation }, exit).await?;
                }
            }
            FenceOutcome::Clear { gone: None } => {}
        }
        let mut template = self.template_for(node.kind, &node.mesh).await?;
        if node.kind == NodeKind::NodeAdmin && before_ready.is_some() {
            template = self.with_mesh_issuer(template, &node.mesh)?;
        }
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: spec_for(node.kind), data_dir: None, held_runtimes };
        let created = self.pipeline(&template).create_with(&req, before_ready, None).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The fabric-primary -> mesh state hand-off at a mesh's first admin, as the create pipeline's
    /// `ApplyMeshPending` step at the exact birth just joined at `admin`: `Pending` for a new mesh's
    /// bootstrap admin (e4.s11), the mesh's current state for the admin created for an existing
    /// mesh. A bounded reachability wait (Ping, at most 20 s, no Status), then ONE
    /// `ApplyMeshState(state)` call. `Applied` or
    /// `AlreadyApplied` succeed; any other outcome fails the step typed (a stale-birth answer, a
    /// backward move, an unreachable target, a lost reply), and the rectifier is the retry, so
    /// `Build(shape)` never starts under an assumed state.
    pub(crate) fn mesh_state_hook(&self, admin: &PathName, state: rdm_core::status::MeshState) -> Result<crate::deployment::pipeline::BeforeReady, String> {
        let node_rpc = &self.node_rpc;
        let (client, resolver, me, book, me_id, topology, mesh, records) = (node_rpc.client.clone(), node_rpc.resolver.clone(), self.me.clone(), self.book.clone(), self.node_id.clone(), self.topology.clone(), admin.mesh.clone(), self.records.clone());
        Ok(Arc::new(move |target: Node| {
            let (client, resolver, me, book, me_id, topology, mesh, records) = (client.clone(), resolver.clone(), me.clone(), book.clone(), me_id.clone(), topology.clone(), mesh.clone(), records.clone());
            Box::pin(async move {
                let me = book.held_path_name().unwrap_or_else(|| me.clone());
                // The mesh's exact identity, as the launch took it: what this admin created, or else
                // what its view holds from the mesh's members (a recovery never mints one).
                let known = topology.read().await.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone());
                let Some(mesh_id) = records.meshes.lock().unwrap().get(&mesh).cloned().or(known).map(|m| m.to_string()) else {
                    return Err(format!("{}: neither this admin's records nor its view hold a mesh id for {mesh}, so no {state:?} can be applied", target.name));
                };
                apply_mesh_state(&client, &resolver, &me, &me_id, &topology, &mesh, &mesh_id, state, &target).await.map(|()| state)
            })
        }))
    }

    /// The exact exited birth `from` at `node` a node recovery acts for (node-recovery.md), and its
    /// exact runtime: the view's record while it holds that birth, else a record of the exact birth
    /// resolved from the membership book or the durable runtime row this admin wrote when it first
    /// held the birth (a departed birth has left the book). `op` names the operation in a refusal.
    pub(crate) async fn exited_birth(&self, op: &str, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(Node, DeploymentHandle), String> {
        let seen = self.topology.read().await.node(node).cloned().filter(|n| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        let exact = seen.unwrap_or_else(|| {
            let mut n = Node::allocated(node.clone());
            n.node_id = from.node_id.clone();
            n.incarnation_id = Some(from.incarnation.clone());
            n
        });
        let (exited, handle) = self.handle_for(&exact).await.map_err(|e| format!("{op}:{node}: the exited birth's runtime is not resolved: {e}"))?;
        if exited.node_id != from.node_id || exited.incarnation_id.as_ref() != Some(&from.incarnation) {
            return Err(format!("{op}:{node}: resolved birth {} at {:?}, the recovery is of {} at {}", exited.node_id, exited.incarnation_id.as_ref().map(|i| i.as_str()), from.node_id, from.incarnation.as_str()));
        }
        Ok((exited, handle))
    }

    /// Node recovery's node.delete (node-recovery.md sequence step 3; node-delete.md "Crashed
    /// target"): the exact exited birth `from` at `node`, deleted through the delete workflow on this
    /// admin's recorded exit proof, nothing sent to the dead process. A birth the provider sees running
    /// is not exited and is not deleted. The fence of the old birth is the create's
    /// ([`Self::create_over`]), done right before the new birth with its key is launched.
    pub(crate) async fn delete_exited(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(), String> {
        // The exit-proof row is read among the birth's own mesh's status rows, where its prover wrote it
        // (`Records::put_exit`): the fabric-primary recovering a peer mesh's node-admin reads mesh2's
        // rows, never its own mesh's.
        let me = self.held_name();
        let mut exited = Node::allocated(node.clone());
        exited.node_id = from.node_id.clone();
        exited.incarnation_id = Some(from.incarnation.clone());
        let exit = self
            .records
            .exit_row_of(&exited)
            .await
            .map_err(|e| format!("node.delete:{node}: the exit-proof row of birth {} at {} could not be read by {me}: {e}", from.node_id, from.incarnation.as_str()))?;
        if let Some(exit) = exit {
            return self.delete_crashed(builds, build_id, attempt, node, from, exit).await;
        }
        let held = self
            .records
            .held_running_row_of(&exited)
            .await
            .map_err(|e| format!("node.delete:{node}: the held-running row of birth {} at {} could not be read by {me}: {e}", from.node_id, from.incarnation.as_str()))?;
        if held.is_some() {
            return self.delete_hung(builds, build_id, attempt, node, from).await;
        }
        Err(format!("node.delete:{node}: {me} reads no exit-proof row for birth {} at {} among {}'s status rows: only a proven exit is deleted as a crashed target", from.node_id, from.incarnation.as_str(), node.mesh))
    }

    /// `node.delete` of a hung target `from` at `node` whose `held-running` hold expired (`R-FM1`):
    /// runs `TerminateRuntime` first, records the provider's exit proof and clears `held-running`,
    /// then completes `NodeDeleting -> NodeDeleted -> RemoveTopologyMembership`.
    pub(crate) async fn delete_hung(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(), String> {
        let (exited, handle) = self.exited_birth("node.delete", node, from).await?;
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: exited, handle, exit: None };
        self.pipeline_with(&template, builds).delete_hung(&req, &*self.records).await.map_err(|e| e.to_string())?;
        let held = self.handles.lock().unwrap().get(node).is_some_and(|(n, _)| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        if held {
            self.handles.lock().unwrap().remove(node);
        }
        self.refresh_view().await;
        Ok(())
    }

    /// node.delete of the crashed target `from` at `node` on the exit-proof row `exit` in hand
    /// (node-delete.md "Crashed target"): the delete workflow, nothing sent to the dead process. A
    /// birth the provider sees running is not exited and is not deleted.
    pub(crate) async fn delete_crashed(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth, exit: crate::status_storage::NodeExitRow) -> Result<(), String> {
        let (exited, handle) = self.exited_birth("node.delete", node, from).await?;
        match self.provider.inspect(&handle).await {
            crate::deployment::provider::DeploymentStatus::Running => {
                return Err(format!("node.delete:{node}: birth {} at {} still runs: it is not exited and is not deleted as a crashed target", from.node_id, from.incarnation.as_str()));
            }
            crate::deployment::provider::DeploymentStatus::OsStopped => {
                return Err(format!("node.delete:{node}: the operating system holds birth {} at {} suspended (os-stopped): it is not exited and is not deleted as a crashed target", from.node_id, from.incarnation.as_str()));
            }
            crate::deployment::provider::DeploymentStatus::Exited { .. } | crate::deployment::provider::DeploymentStatus::Unknown => {}
        }
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: exited, handle, exit: Some(exit) };
        self.pipeline_with(&template, builds).retire(&req).await.map_err(|e| e.to_string())?;
        let held = self.handles.lock().unwrap().get(node).is_some_and(|(n, _)| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        if held {
            self.handles.lock().unwrap().remove(node);
        }
        Ok(())
    }

    /// Node recovery's node.create (node-recovery.md sequence steps 4-5): a new node at `node` over
    /// the exited birth `from`'s data dir, through the create workflow: it loads the endpoint key
    /// that dir holds and binds port 0, with a new node.id and incarnation, no supersedes. Not through
    /// the create door's fence: the birth at the path is the exited one this recovery deleted, and
    /// the fence would replace it.
    ///
    /// The exited birth is fenced here, by the create (node-recovery.md "Delete fences the old
    /// instance before the new join"): right before the new runtime with the same key is launched,
    /// inside its `DeployRuntime` step ([`crate::deployment::pipeline::BeforeLaunch`]). Whichever
    /// executor runs the create fences, so an executor lost after the delete's Complete leaves
    /// nothing unfenced; a rerun that reuses a runtime an earlier attempt launched (it was fenced
    /// before that launch) cuts none of that runtime's paths.
    pub(crate) async fn create_over(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(), String> {
        let (exited, _) = self.exited_birth("node.create", node, from).await?;
        // A node-admin created into a mesh with no other live node-admin is that mesh's first admin, as
        // every node.create of one is: it takes the mesh's issuing material and the fabric primary's
        // Pending hand-off before it is asked to be Ready (e4.s11; mesh-investigation.md "A node-admin
        // joins the live mesh").
        let first_admin = {
            let t = self.topology.read().await;
            t.first_admin_of(node) && crate::executor::held_admins(&node.mesh, Some(node), &t).is_empty()
        };
        let before_ready = if first_admin { Some(self.mesh_state_hook(node, rdm_core::status::MeshState::Pending)?) } else { None };
        // The launch is the kind's template, as every create's: the new node's config is seeded
        // from the exited birth's Deleted row by the create's name claim (`NodeRows::claim`) and told
        // to it when its row is written at its arrival (`NodeRows::write_arrivals`).
        let mut template = self.template_for(node.kind, &node.mesh).await?;
        if first_admin {
            template = self.with_mesh_issuer(template, &node.mesh)?;
        }
        let pipeline = self.pipeline_with(&template, builds);
        let data_dir = pipeline.birth_dir(&exited);
        // The key the fence names is the one the kept data dir holds (a record resolved from the
        // durable runtime rows carries none): the node created over that dir loads exactly it.
        let mut fenced = exited;
        if fenced.endpoint_id.is_none() {
            fenced.endpoint_id = crate::deployment::pipeline::load_transport_key(&data_dir).ok();
        }
        // A node-admin's recovery is a node-admin birth whose maker is this mesh-primary (the
        // template's seed): it is handed the durable runtime rows before it runs, as every admin
        // birth is.
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let lost = match self.records.status.get() {
            Some(fold) => fold.storage().node_data_dir_lost_of(&node.mesh, &from.node_id, &from.incarnation).await.ok().flatten().is_some(),
            None => false,
        };
        let created = if lost {
            let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: spec_for(node.kind), data_dir: None, held_runtimes };
            pipeline.create_recovery_with(&req, before_ready, Some(Box::pin(self.revise_exited(&fenced)))).await.map_err(|e| e.to_string())?
        } else {
            if let Err(crate::deployment::pipeline::InheritedDirError::Broken(ref cause)) = crate::deployment::pipeline::inspect_transport_key(&data_dir) {
                if let Some(fold) = self.records.status.get() {
                    let row = crate::status_storage::NodeDataDirUnreadableRow {
                        node_id: from.node_id.clone(),
                        name: node.clone(),
                        incarnation_id: from.incarnation.clone(),
                        path: data_dir.display().to_string(),
                        cause: cause.clone(),
                    };
                    let _ = fold.storage().put_node_data_dir_unreadable_of(&node.mesh, &row).await;
                }
                tracing::info_span!(
                    "rdm.node_admin.node.reject.via-data-dir-unreadable",
                    node = %node,
                    node_id = %from.node_id,
                    incarnation_id = %from.incarnation.as_str(),
                    path = %data_dir.display(),
                    cause = %cause,
                    "otel.kind" = "internal",
                )
                .in_scope(|| tracing::info!("inherited data dir is broken; holding recovery until repaired"));
            }
            let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: spec_for(node.kind), data_dir: Some(data_dir), held_runtimes };
            pipeline.create_with(&req, before_ready, Some(Box::pin(self.revise_exited(&fenced)))).await.map_err(|e| e.to_string())?
        };
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// node-recovery.md "Delete fences the old instance before the new join": the exited birth's
    /// resolver entry is retired and its direct paths dropped, awaited, right before the new birth
    /// with the same endpoint key is launched (the create over its data dir, [`Self::create_over`]).
    ///
    /// The await is bounded, and it is awaited (not spawned as `after_exit` does) because the new
    /// birth carries the SAME key: a retire landing after the new birth's paths exist would retire
    /// those. Bounded, in the pinned iroh (rev 921afe53): `Socket::replace_direct_addrs`
    /// (iroh/src/socket.rs:1408-1420) returns at once when no actor runs for the key, else sends one
    /// message into the actor's 64-slot inbox (socket/remote_map/remote_state.rs:246) and awaits its
    /// oneshot. The actor's loop (remote_state.rs:300-360) runs only synchronous arm bodies and
    /// drains one inbox message per turn through the synchronous `handle_message` (:387); the
    /// `ReplaceDirectAddrs` arm (:403-405) retires the paths in the synchronous
    /// `handle_msg_replace_direct_addrs` (:529) and answers in the same turn. An actor that ends drops
    /// its inbox (and the queued message's oneshot, :372), so the send or the reply resolves with an
    /// error and this returns. The wait is at most the actor's turns over the messages queued ahead
    /// of this one, none of which awaits the exited peer.
    pub(crate) async fn revise_exited(&self, record: &Node) {
        if let Some(incarnation) = record.incarnation_id.as_ref() {
            self.node_rpc.resolver.retire_birth(&record.node_id, incarnation);
        }
        if let (Some(ep), Some(key)) = (&self.endpoint, record.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok())) {
            ep.replace_direct_addrs(key, []).await;
            tracing::info_span!("rdm.node_admin.node.update.via-exit-paths-retired", node = %record.name, node_id = %record.node_id, endpoint = %key.fmt_short(), kind = "recovery", reason = "proven-exit")
                .in_scope(|| tracing::info!("the exited birth's direct paths are retired before the new birth with its key is launched"));
        }
    }

    /// What follows a birth's proven exit: the resolver names
    /// no current birth for it, and its direct paths are retired. A later birth that keeps the key
    /// binds a port the operating system assigns, so this endpoint holds no path to the key until
    /// that birth reports one (R-I1's replacement, with no address left).
    pub(crate) fn after_exit(&self, record: &Node) {
        if let Some(incarnation) = record.incarnation_id.as_ref() {
            let rpc = &self.node_rpc;
            // The exit is proven: the resolver names no current birth for the node until its
            // successor is applied, so no dial, probe or install aims at the dead birth's socket.
            rpc.resolver.retire_birth(&record.node_id, incarnation);
        }
        // The exit is proven: the exited birth's direct paths are retired. Until now they stayed
        // open for the typed drain.
        if let (Some(ep), Some(key)) = (&self.endpoint, record.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok())) {
            // Not awaited: the endpoint's actor for this key answers when it is free, and the
            // retire does not wait for a dead peer's actor (the same call membership spawns).
            let ep = ep.clone();
            tokio::spawn(async move { ep.replace_direct_addrs(key, []).await });
            tracing::info_span!("rdm.node_admin.node.update.via-exit-paths-retired", node = %record.name, node_id = %record.node_id, endpoint = %key.fmt_short(), kind = "removal", reason = "proven-exit")
                .in_scope(|| tracing::info!("the exited birth's direct paths are retired: nothing aims at its old socket"));
        }
    }

    /// `node.connections.delete:<path>` / `node.stop:<path>` (node-connections-delete.md, node-stop.md):
    /// the exact birth `from` at `node`, targeted by incarnation. A node.connections.delete leaves the
    /// process running; a stop leaves it stopped, its process alive (node-stop.md: no runtime Exited
    /// receipt), and publishes no departure.
    pub(crate) async fn standalone(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId, stop: bool) -> Result<(), String> {
        let op = if stop { "node.stop" } else { "node.connections.delete" };
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) => {
                if n.incarnation_id.as_ref() != Some(from) {
                    return Err(format!("{op}:{node} targets birth {}, the view holds {} at that path", from.as_str(), n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none")));
                }
                self.handle_for(&n).await?
            }
            None => self.handles.lock().unwrap().get(node).cloned().filter(|(rec, _)| rec.incarnation_id.as_ref() == Some(from)).ok_or_else(|| format!("{op}:{node}: birth {} is not in this admin's view and no runtime handle of it is held", from.as_str()))?,
        };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, exit: None };
        if stop {
            // The process is parked, not exited: nothing is retired about its paths or its birth.
            self.pipeline(&template).stop(&req).await.map_err(|e| e.to_string())?;
        } else {
            self.pipeline(&template).delete_connections(&req).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// node.delete of a stopped birth (`node.delete:<path>`, node-delete.md): the stopped birth `from`
    /// at `node`, fenced by incarnation, ended with the provider's exact Exited proof and deleted
    /// (`DeploymentPipeline::delete_stopped`); what follows its proven exit follows.
    pub(crate) async fn delete_stopped(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) if n.incarnation_id.as_ref() == Some(from) => self.handle_for(&n).await?,
            Some(n) => return Err(format!("node.delete:{node} is fenced to birth {}, the view holds {} at that path", from.as_str(), n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none"))),
            None => {
                let launched = self.handles.lock().unwrap().get(node).cloned().filter(|(rec, _)| rec.incarnation_id.as_ref() == Some(from));
                match launched {
                    Some(h) => h,
                    None => {
                        let birth = self.stopped_birth_by_incarnation(node, from).await?;
                        self.handle_for(&birth).await?
                    }
                }
            }
        };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, exit: None };
        self.pipeline(&template).delete_stopped(&req).await.map_err(|e| e.to_string())?;
        self.after_exit(&record);
        self.handles.lock().unwrap().remove(node);
        Ok(())
    }

    /// The exact parked birth `from` that a delete of `node` (`<path>.old`) is fenced to, for an admin
    /// whose view does not name `node` and which did not run the replace (the mesh's primary changed
    /// since): the birth as its membership holds it parked, or else its durable runtime row (every
    /// admin keeps one per birth it heard, `runtime_rows_task`). The record is named `node`; its
    /// runtime is then adopted from the same sources by [`Self::handle_for`].
    pub(crate) async fn stopped_birth_by_incarnation(&self, node: &PathName, from: &IncarnationId) -> Result<Node, String> {
        let mut birth = Node::allocated(node.clone());
        birth.incarnation_id = Some(from.clone());
        birth.parked = true;
        birth.status = NodeStatus::Leaving;
        if let Some(d) = self.book.parked().into_iter().find(|d| &d.node.incarnation == from) {
            birth.node_id = d.node.node_id.clone();
            birth.endpoint_id = Some(d.node.endpoint_id.clone());
            birth.transport_addr = Some(d.node.transport_addr);
            birth.data_dir = d.data_dir.clone();
            return Ok(birth);
        }
        let Some(store) = self.records.contacts.get() else {
            return Err(format!("node.delete:{node}: birth {} is not in this admin's view or membership, and this admin has no nodes store to read its runtime row from", from.as_str()));
        };
        let rows = store.runtimes().await.map_err(|e| format!("node.delete:{node}: birth {} is not in this admin's view or membership, and its runtime rows could not be read: {e}", from.as_str()))?;
        let row = rows.into_iter().find(|r| &r.incarnation_id == from).ok_or_else(|| format!("node.delete:{node}: birth {} is not in this admin's view or membership, and this admin holds no runtime row of it", from.as_str()))?;
        birth.node_id = row.node_id;
        birth.data_dir = row.data_dir;
        Ok(birth)
    }

    /// What this admin holds for the start of the parked `record`: the deployment its `JoinNode` is
    /// verified against, as the birth was admitted.
    pub(crate) fn deployed_for(&self, record: &Node, handle: &DeploymentHandle) -> Result<crate::join::Deployed, String> {
        let incarnation = record.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", record.name))?;
        let admitted = self.book.parked().into_iter().find(|d| d.node.node_id == record.node_id && d.node.incarnation == incarnation);
        Ok(crate::join::Deployed {
            name: record.name.clone(),
            node_id: record.node_id.clone(),
            incarnation,
            supersedes: admitted.as_ref().and_then(|d| d.node.supersedes.clone()),
            endpoint_id: record.endpoint_id.clone().ok_or_else(|| format!("{} has no known endpoint id", record.name))?,
            runtime: handle.fact().ok_or_else(|| format!("{}: the handle names no exact runtime to verify its rejoin against", record.name))?,
            data_dir: record.data_dir.clone().ok_or_else(|| format!("{} has no known data dir", record.name))?,
            // The name this authority last accepted for this exact birth: its held stopped digest's (R-JN1).
            last_accepted_name: admitted.as_ref().map(|d| d.node.name.clone()),
            held: true,
        })
    }

    /// The parked birth `from` at `node`: its record and exact runtime, and what the rejoin is
    /// verified against.
    pub(crate) async fn stopped_birth(&self, op: &str, node: &PathName, from: Option<&IncarnationId>) -> Result<(Node, DeploymentHandle, crate::join::Deployed), String> {
        let seen = self.topology.read().await.node(node).cloned().ok_or_else(|| format!("{op}:{node} is not in this admin's view"))?;
        if let Some(from) = from {
            if seen.incarnation_id.as_ref() != Some(from) {
                return Err(format!("{op}:{node} is fenced to birth {}, the view holds {} at that path", from.as_str(), seen.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none")));
            }
        }
        let (record, handle) = self.handle_for(&seen).await?;
        // The record the pipeline starts is the view's: it carries the endpoint, the address and the data dir the birth was admitted at.
        let record = Node { status: seen.status, parked: seen.parked, ..record };
        let record = Node { endpoint_id: seen.endpoint_id.clone().or(record.endpoint_id), transport_addr: seen.transport_addr.or(record.transport_addr), data_dir: seen.data_dir.clone().or(record.data_dir), ..record };
        let deployed = self.deployed_for(&record, &handle)?;
        Ok((record, handle, deployed))
    }

    /// `node.restart:<path>` (node-restart.md): stop, then start, on the same process: the same node id,
    /// incarnation, endpoint key and port. The process never exits and no runtime is deployed.
    pub(crate) async fn restart_in_place(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> Result<(), String> {
        // A birth this restart's own earlier attempt parked is held as a parked one (its digest the
        // stopped one its rejoin is verified against), and its run resumes at the start.
        let parked = self.topology.read().await.node(node).is_some_and(|n| n.parked);
        let (record, handle, deployed) = if parked { self.stopped_birth("node.restart", node, None).await? } else { self.stopped_birth_or_live("node.restart", node).await? };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = crate::deployment::pipeline::StartRequest { build_id: build_id.clone(), attempt, node: record, handle, deployed };
        self.pipeline(&template).restart(&req).await.map_err(|e| e.to_string())
    }

    /// The live birth at `node` before its stop, for a restart: the same record shape as a parked one.
    pub(crate) async fn stopped_birth_or_live(&self, op: &str, node: &PathName) -> Result<(Node, DeploymentHandle, crate::join::Deployed), String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) => self.handle_for(&n).await?,
            None => self.handles.lock().unwrap().get(node).cloned().ok_or_else(|| format!("{op}:{node} is not in this admin's view and no runtime handle of it is held"))?,
        };
        let seen = self.topology.read().await.node(node).cloned().unwrap_or_else(|| record.clone());
        let record = Node { endpoint_id: seen.endpoint_id.clone().or(record.endpoint_id), transport_addr: seen.transport_addr.or(record.transport_addr), data_dir: seen.data_dir.clone().or(record.data_dir), status: seen.status, ..record };
        let deployed = self.deployed_for_live(&record, &handle)?;
        Ok((record, handle, deployed))
    }

    /// [`Self::deployed_for`] for a birth still in the book: its admitted digest is the book's.
    pub(crate) fn deployed_for_live(&self, record: &Node, handle: &DeploymentHandle) -> Result<crate::join::Deployed, String> {
        let incarnation = record.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", record.name))?;
        let admitted = self.book.get(record.node_id.as_str()).map(|(d, _)| d).filter(|d| d.node.incarnation == incarnation);
        Ok(crate::join::Deployed {
            name: record.name.clone(),
            node_id: record.node_id.clone(),
            incarnation,
            supersedes: admitted.as_ref().and_then(|d| d.node.supersedes.clone()),
            endpoint_id: record.endpoint_id.clone().ok_or_else(|| format!("{} has no known endpoint id", record.name))?,
            runtime: handle.fact().ok_or_else(|| format!("{}: the handle names no exact runtime to verify its rejoin against", record.name))?,
            data_dir: record.data_dir.clone().or_else(|| admitted.as_ref().and_then(|d| d.data_dir.clone())).ok_or_else(|| format!("{} has no known data dir", record.name))?,
            last_accepted_name: admitted.as_ref().map(|d| d.node.name.clone()),
            held: true,
        })
    }

    /// `node.start:<path>` (node-start.md): the parked birth `from` rejoins as itself.
    pub(crate) async fn start_stopped(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), String> {
        let (record, handle, deployed) = self.stopped_birth("node.start", node, Some(from)).await?;
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = crate::deployment::pipeline::StartRequest { build_id: build_id.clone(), attempt, node: record, handle, deployed };
        self.pipeline(&template).start(&req).await.map_err(|e| e.to_string())
    }

    pub(crate) async fn delete_node(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> Result<Option<Node>, String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match &seen {
            Some(n) => self.handle_for(n).await?,
            None => {
                let held = self.handles.lock().unwrap().get(node).cloned();
                match held {
                    Some(h) => h,
                    None => return Err(format!("node.delete:{node}: {} holds no birth at {node} in its view and no runtime handle of one", self.held_name())),
                }
            }
        };
        // The record handed back with the runtime is the one this admin deployed; whether the birth
        // is stopped (parked) is the view's, as `stopped_birth` takes it: a stopped birth's delete
        // goes straight to its exact runtime's exit proof.
        let record = match &seen {
            Some(n) => Node { parked: n.parked, ..record },
            None => record,
        };
        self.delete_birth(build_id, attempt, record.clone(), handle).await?;
        Ok(Some(record))
    }

    /// node.delete of the live or stopped exact birth `record` (node-delete.md): the ordinary
    /// workflow (`DeploymentPipeline::retire`: node.stop for a live process, the provider's exact
    /// exit proof, its row Deleted, NodeDeleted) over its exact runtime `handle`; then its paths are
    /// retired and this admin's handle of it dropped. The one composition a removal, a mesh
    /// delete's member and a deleted mesh's last admin all call. Answers the terminal receipt.
    pub(crate) async fn delete_birth(&self, build_id: &crate::build::BuildId, attempt: u32, record: Node, handle: DeploymentHandle) -> Result<crate::deployment::pipeline::TerminalReceipt, String> {
        let template = self.template_for(record.kind, &record.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, exit: None };
        let terminal = self.pipeline(&template).retire(&req).await.map_err(|e| e.to_string())?;
        // The deleted birth's os-stopped fact ends with it (deployment-provider.md §2.5: the row is
        // removed on `Exited`).
        self.records.clear_os_stopped(&record, "deleted").await;
        self.after_exit(&record);
        self.handles.lock().unwrap().remove(&record.name);
        Ok(terminal)
    }
}

#[async_trait::async_trait]
impl crate::seat_revision::PathProbe for AdminRunner {
    async fn answers(&self, node: &Node) -> bool {
        AdminRunner::answers(self, node).await
    }
    async fn inspect(&self, node: &Node) -> Result<crate::deployment::provider::DeploymentStatus, String> {
        let (_, h) = self.handle_for(node).await?;
        Ok(self.provider.inspect(&h).await)
    }
    async fn recorded(&self, path: &PathName) -> Result<Vec<(String, crate::deployment::provider::DeploymentStatus)>, String> {
        let mut out = Vec::new();
        for dir in crate::seat_revision::recorded_dirs(&self.template.data_root, path).await? {
            // A data dir that holds no runtime record holds no runtime.
            let Some(fact) = crate::seat_revision::runtime_record(&dir).await? else { continue };
            // A record of another provider or control domain names no runtime this provider runs.
            let Ok(h) = crate::deployment::provider::adopt(&*self.provider, &fact) else { continue };
            out.push((dir.display().to_string(), self.provider.inspect(&h).await));
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl OperationRunner for AdminRunner {
    async fn run(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), crate::executor::RunEnd> {
        let r = self.run_op(build_id, lane, attempt, op).await;
        self.refresh_view().await;
        r
    }
}

impl AdminRunner {
    pub(crate) async fn run_op(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), crate::executor::RunEnd> {
        use tracing::Instrument;
        let me = self.held_name();
        // The workflows write the attempt's receipts and read its Build through the lane's Build
        // state (R-BI7): a drift attempt's in its mesh's lane, a fabric attempt's in the fabric lane.
        let builds = crate::build_run::LaneBuilds::of(&self.builds, lane);
        if let Some(mesh) = lane {
            // A mesh's drift lane carries a node recovery's attempts only (its open is
            // `ControlPlane::open_recovery`'s): any other operation is not one this lane runs.
            if !matches!(op, BuildOperation::RetireNode { exited: Some(_), .. } | BuildOperation::CreateNode { over: Some(_), .. }) {
                return Err(crate::executor::RunEnd::Failed(format!("{me}: {} is not an operation of a node recovery, the only work mesh {mesh}'s drift lane runs (attempt {attempt} of {build_id})", op.key())));
            }
        }
        let ended: Result<(), String> = match op {
            BuildOperation::CreateMesh { .. } => create_mesh_step(&self.records, &*self.mesh_storage, &*self.builds, &self.template.executor(), &me.to_string(), build_id, attempt, op).await,
            BuildOperation::CreateNode { node, over: Some(from) } => {
                // node-recovery.md names the create's span `node.create.via-build`.
                let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id.log(), node = %node, attempt, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str());
                self.create_over(&*builds, build_id, attempt, node, from).instrument(span).await
            }
            BuildOperation::CreateNode { node, over: None } => {
                let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id.log(), node = %node, attempt);
                async {
                    // A mesh's first admin (a new mesh, or a mesh whose admins were all lost): once it
                    // has joined its mesh, the fabric primary applies MeshStatus::Pending at that exact
                    // birth, before it is asked to be Ready; only with that certainty does the shape
                    // proceed (e4.s11).
                    let first_admin = self.topology.read().await.first_admin_of(node);
                    let before_ready = if first_admin { Some(self.mesh_state_hook(node, rdm_core::status::MeshState::Pending)?) } else { None };
                    self.create(build_id, attempt, node, before_ready).await
                }
                .instrument(span)
                .await
            }
            BuildOperation::RestartNode { node } => {
                // node-restart.md: the live process parks and rejoins; there is no relaunch. A member
                // proven exited is node recovery's (node-recovery.md).
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt);
                self.restart_in_place(build_id, attempt, node).instrument(span).await
            }
            BuildOperation::StartNode { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt);
                self.start_stopped(build_id, attempt, node, from_incarnation).instrument(span).await
            }
            BuildOperation::DeleteConnections { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt);
                self.standalone(build_id, attempt, node, from_incarnation, false).instrument(span).await
            }
            BuildOperation::StopNode { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt);
                self.standalone(build_id, attempt, node, from_incarnation, true).instrument(span).await
            }
            BuildOperation::RetireNode { node, exited: Some(from) } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-build", build_id = %build_id.log(), node = %node, attempt, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str());
                self.delete_exited(&*builds, build_id, attempt, node, from).instrument(span).await
            }
            BuildOperation::DeleteNode { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-build", build_id = %build_id.log(), node = %node, attempt);
                self.delete_stopped(build_id, attempt, node, from_incarnation).instrument(span).await
            }
            BuildOperation::RetireNode { node, exited: None } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-build", build_id = %build_id.log(), node = %node, attempt);
                self.delete_node(build_id, attempt, node).instrument(span).await.map(|_| ())
            }
            BuildOperation::ShutdownMesh { mesh, mesh_id } => return self.delete_mesh(build_id, attempt, mesh, mesh_id).await,
            BuildOperation::DeleteFabric { fabric_id } => crate::mesh_delete::complete_fabric_delete(&*builds, &*self.fabric_storage, build_id, attempt, fabric_id, &self.me.to_string()).await,
            BuildOperation::RecipeStep { step, birth } => return Box::pin(self.recipe_step(build_id, lane, attempt, step, birth.as_ref())).await,
        };
        ended.map_err(crate::executor::RunEnd::Failed)
    }

    /// One step of a recipe (`BuildChange::Recipe`; lifecycles/node-replace.md): the op it names, run
    /// as that op's own workflow on the exact birth the step acts on, the one it is fenced to, or for a
    /// node an earlier step of the recipe created, the birth this executor holds at the step's path.
    /// node.create is the create workflow; node.stop the standalone stop; node.name.update its
    /// own core at this admin as the node's mesh-primary; node.start the start of the stopped birth;
    /// node.delete node.delete's workflow from the state the birth is in.
    pub(crate) async fn recipe_step(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, step: &rdm_core::build::RecipeStep, birth: Option<&crate::build::PinnedBirth>) -> Result<(), crate::executor::RunEnd> {
        use rdm_core::build::RecipeStep as S;
        let me = self.held_name();
        let node = step.node();
        let key = crate::build::recipe_step_key(step);
        if let S::Create { node } = step {
            return self.run_op(build_id, lane, attempt, &BuildOperation::CreateNode { node: node.clone(), over: None }).await;
        }
        let birth = match birth {
            Some(b) => b.clone(),
            None => {
                let seen = self.topology.read().await.node(node).cloned();
                let held = seen.or_else(|| self.handles.lock().unwrap().get(node).map(|(n, _)| n.clone()));
                match held.and_then(|n| n.incarnation_id.clone().map(|incarnation| crate::build::PinnedBirth { node_id: n.node_id.clone(), incarnation })) {
                    Some(b) => b,
                    None => return Err(crate::executor::RunEnd::Failed(format!("{key}: the step acts on the node an earlier step of the recipe made at {node}, and {me} holds no birth at {node}"))),
                }
            }
        };
        let from_incarnation = birth.incarnation.clone();
        match step {
            S::Create { .. } => unreachable!("a create returned above"),
            S::Stop { node } => self.run_op(build_id, lane, attempt, &BuildOperation::StopNode { node: node.clone(), from_incarnation }).await,
            S::Start { node } => self.run_op(build_id, lane, attempt, &BuildOperation::StartNode { node: node.clone(), from_incarnation }).await,
            S::Delete { node } => {
                // node.delete runs no stop steps for a stopped or proven-exited birth and stops a live
                // one first (node-delete.md): the birth's state decides, never its name. A birth this
                // view no longer holds (a resumed delete whose NodeDeleting it already heard) is
                // deleted fenced to its exact birth, its runtime read from its row.
                let held = self.topology.read().await.node(node).filter(|n| n.incarnation_id.as_ref() == Some(&from_incarnation)).cloned();
                let op = match held {
                    Some(n) if !n.parked && n.status != crate::model::NodeStatus::Dead => BuildOperation::RetireNode { node: node.clone(), exited: None },
                    _ => BuildOperation::DeleteNode { node: node.clone(), from_incarnation },
                };
                self.run_op(build_id, lane, attempt, &op).await
            }
            S::Update { node, name } => {
                use tracing::Instrument;
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt, node_id = %birth.node_id, incarnation_id = %birth.incarnation.as_str(), to = %name);
                async {
                    let template = self.template_for(node.kind, &node.mesh).await?;
                    let put = async {
                        let Some(authority) = self.observer.authority.get() else {
                            return Err(format!("{key}: {me} holds no status authority yet to serve node.name.update; nothing is written"));
                        };
                        let cutover = if node.new && *name == node.base() {
                            let retained = name.renamed();
                            let retained_id = match self.topology.read().await.node(&retained).map(|n| n.node_id.clone()) {
                                Some(id) => Some(id),
                                None => authority.nodes_storage.node_row_at(&retained).await.ok().flatten().map(|r| r.node_id),
                            };
                            retained_id.map(|retained_id| crate::node_name_update::Cutover { retained, retained_id })
                        } else {
                            None
                        };
                        let req = rdm_core::status::StatusRequest::UpdateName { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: name.name() };
                        match authority.update_name(&req, cutover.as_ref()).await {
                            rdm_core::status::StatusReply::NameUpdated { .. } => Ok(()),
                            refused => Err(format!("{key}: node.name.update of {node} ({}) to {name} answered {refused:?}", birth.node_id)),
                        }
                    };
                    self.pipeline(&template).update_name(build_id, attempt, node, put).await.map_err(|e| e.to_string())
                }
                .instrument(span)
                .await
                .map_err(crate::executor::RunEnd::Failed)
            }
        }
    }
}

/// `ApplyMeshState(Pending)` at `target` (a mesh's first admin, just joined), from `me` (born as `me_id`).
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub async fn apply_mesh_state(client: &rdm_transport::NodeRpcClient, resolver: &rdm_transport::LiveNodeResolver, me: &PathName, me_id: &NodeId, topology: &Arc<RwLock<Topology>>, mesh: &str, mesh_id: &str, state: rdm_core::status::MeshState, target: &Node) -> Result<(), String> {
    use rdm_core::ping::{Ping, PingReply, PingRequest};
    use rdm_core::status::{MeshState, Status, StatusReply, StatusRequest};
    // The target is not heard yet (a peer mesh's admin reaches the backbone only as its mesh's
    // primary): this admin launched it and holds its birth exactly, so its resolver takes the
    // birth from what it launched, never from a digest it has not heard.
    let (Some(endpoint_id), Some(transport_addr), Some(incarnation)) = (target.endpoint_id.as_ref(), target.transport_addr, target.incarnation_id.clone()) else {
        return Err(format!("{}: the birth just launched has no endpoint id, transport address or incarnation to reach it by", target.name));
    };
    let endpoint_id = endpoint_id.0.parse::<iroh::PublicKey>().map_err(|e| format!("{}: its endpoint id is not an iroh key: {e}", target.name))?;
    let fed = resolver.apply(rdm_transport::ResolvedNode { node_id: target.node_id.clone(), name: target.name.clone(), endpoint_id, transport_addr, incarnation }, None);
    tracing::info!(target = %target.name, applied = ?fed, "the resolver holds the launched birth for the mesh-state hand-off");
    let (me_id, me_inc) = {
        let v = topology.read().await;
        (me_id.to_string(), v.own(me_id).and_then(|n| n.incarnation_id.clone()).map(|i| i.to_string()).unwrap_or_default())
    };
    let request = StatusRequest::ApplyMeshState {
        mesh_id: crate::model::MeshId::parse(mesh_id).map_err(|e| format!("{mesh}: its mesh id {mesh_id:?} is not a MeshId: {e}"))?,
        mesh_name: mesh.to_string(),
        state,
    };
    let key = format!("mesh:{mesh_id}:{state:?}");
    // A bounded reachability wait on the exact launched birth, a Ping and no Status: the target
    // answers `Pong` once its router serves with its authority filled (until then every op,
    // ping included, is a typed NotReady). Then exactly one Status call.
    let until = std::time::Instant::now() + Duration::from_secs(20);
    let started = std::time::Instant::now();
    let mut pings: u32 = 0;
    let reach = loop {
        pings += 1;
        let (out, _) = client
            .call::<Ping>(&rdm_transport::NodeTarget::ExactNode(target.node_id.clone()), &PingRequest::Ping { payload: Vec::new() }, &rdm_transport::CallOptions::default())
            .await;
        let answered = matches!(&out, rdm_core::outcome::RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }));
        let seen = match &out {
            rdm_core::outcome::RpcOutcome::Reply(r) => format!("{:?}", r.value()),
            other => format!("{}: {other:?}", other.name()),
        };
        if answered {
            break Ok(seen);
        }
        if std::time::Instant::now() >= until {
            break Err(seen);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    tracing::info_span!(
        "rdm.node_admin.mesh.update.via-pending-reachability",
        node = %me,
        target = %target.name,
        target_node_id = %target.node_id,
        mesh = %mesh,
        pings,
        waited_ms = started.elapsed().as_millis() as u64,
        reachable = reach.is_ok(),
        last = %match &reach { Ok(s) | Err(s) => s.as_str() },
    )
    .in_scope(|| tracing::info!("the launched birth was waited for before the mesh-state hand-off"));
    if let Err(last) = reach {
        return Err(format!("{}: unreachable for 20 s before the {state:?} hand-off ({pings} pings, last: {last}); nothing was sent, and Build(shape) does not start under an assumed state", target.name));
    }
    // The one call. Applied and AlreadyApplied succeed; every other outcome fails the step typed,
    // and the rectifier is the retry.
    let (out, _) = client.call::<Status>(&rdm_transport::NodeTarget::ExactNode(target.node_id.clone()), &request, &rdm_transport::CallOptions::default()).await;
    let (outcome, verdict) = match &out {
        rdm_core::outcome::RpcOutcome::Reply(r) => match r.value() {
            StatusReply::Applied => ("applied".to_string(), Ok(())),
            StatusReply::AlreadyApplied => ("already-applied".to_string(), Ok(())),
            StatusReply::RejectedStaleMesh { held } => (format!("rejected-stale-mesh: the target holds mesh id {held}"), Err(format!("{} holds mesh id {held}, not {mesh_id}: no replacement id is minted; the {state:?} hand-off is refused", target.name))),
            StatusReply::RejectedInvalidMeshTransition { current } => (format!("rejected-invalid-mesh-transition: {current:?}"), Err(format!("{} already holds its Mesh at {current:?}: {state:?} is a backward move", target.name))),
            other => (format!("{other:?}"), Err(format!("{}: the {state:?} hand-off was answered {other:?}; {state:?} is not certain and Build(shape) does not start under an assumed state", target.name))),
        },
        other => (format!("{}: {other:?}", other.name()), Err(format!("{}: the {state:?} hand-off call ended {}: {state:?} is not certain and Build(shape) does not start under an assumed state ({other:?})", target.name, other.name()))),
    };
    // A new mesh's bootstrap admin is handed Pending; the admin created for an existing mesh is
    // handed the mesh's current state (states.md: a mesh's state only moves forward).
    macro_rules! handoff_span {
        ($name:literal) => {
            tracing::info_span!(
                $name,
                node = %me,
                node_id = %me_id,
                incarnation = %me_inc,
                target = %target.name,
                target_node_id = %target.node_id,
                target_incarnation = %target.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default(),
                mesh = %mesh,
                mesh_id = %mesh_id,
                key = %key,
                state = ?state,
                attempt = 1u32,
                outcome = %outcome,
            )
        };
    }
    let span = if state == MeshState::Pending { handoff_span!("rdm.node_admin.mesh.update.via-pending-handoff") } else { handoff_span!("rdm.node_admin.mesh.update.via-state-handoff") };
    span.in_scope(|| tracing::info!("the fabric primary applies its mesh's state at the mesh's first admin"));
    verdict
}

/// The executable bound to `kind`, re-hashed now: the launch proves the bytes it launches are the
/// bound ones. The hash reads the whole file, so it runs on the blocking pool, never on a worker of
/// the runtime: a task the launching task woke just before waits in that worker's LIFO slot, which
/// no other worker steals, until the worker is free (Q30).
pub(crate) async fn resolve_bound(bindings: &rdm_core::binding::Validated, kind: NodeKind) -> Result<rdm_core::binding::Resolved, rdm_core::binding::BindingError> {
    let span = tracing::info_span!("rdm.node_admin.deployment.resolve.via-executable-hash", launch_id = kind.name(), elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
    let started = std::time::Instant::now();
    let bound = bindings.clone();
    let resolved = crate::record_store::blocking_io(move || bound.resolve(kind)).await;
    span.record("elapsed_ms", started.elapsed().as_millis() as u64);
    span.record("outcome", if resolved.is_ok() { "hashed" } else { "refused" });
    span.in_scope(|| tracing::info!("the bound executable's bytes are hashed against its binding"));
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::binding::{Binding, BindingSet, Candidate, Expect, ProviderImage};

    /// CONTRACT (Q30): a launch's executable hash never holds a task its worker woke. The hash reads
    /// the whole file; a task the launching task woke just before waits in that worker's LIFO slot,
    /// which no other worker steals, until the launch yields. The woken task runs while the hash
    /// runs, never after it (an admin's endpoint held past the QUIC idle timeout otherwise: every
    /// connection reset, trace 055c493a9ba3750bffc1854699ce7829).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_launch_hashing_its_executable_never_holds_a_task_its_worker_woke() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("resolve-bound-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let exe = dir.join("bound-node");
        std::fs::write(&exe, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::OpenOptions::new().write(true).open(&exe).unwrap().set_len(16 << 20).unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let sha256 = rdm_core::binding::sha256_file(&exe).unwrap();
        let bind = |id: &str| Binding { launch_id: id.into(), sha256: sha256.clone(), executable: exe.clone(), image: None };
        let set = BindingSet {
            candidate: Candidate { sha: "candidate".into(), build: "cell".into() },
            launch_ids: vec!["node_admin".into(), "rpc_node".into()],
            bindings: vec![bind("node_admin"), bind("rpc_node")],
        };
        let bindings = set.validate(&Expect { candidate_sha: "candidate", required: &[], provider_image: ProviderImage::Unchecked }).unwrap();

        // A task parked on its channel, as a connection's driver is between packets.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (wake_tx, mut wake_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let woken = tokio::spawn(async move {
            let _ = ready_tx.send(());
            wake_rx.recv().await.unwrap();
            std::time::Instant::now()
        });
        ready_rx.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // The launch wakes it, then re-hashes its executable.
        let hashed_at = tokio::spawn(async move {
            wake_tx.send(()).unwrap();
            let resolved = resolve_bound(&bindings, NodeKind::RpcNode).await;
            assert!(resolved.is_ok(), "{resolved:?}");
            std::time::Instant::now()
        })
        .await
        .unwrap();
        let ran_at = woken.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(ran_at < hashed_at, "the woken task ran {:?} after the hash returned: it waited for the hash", ran_at.saturating_duration_since(hashed_at));
    }
}
