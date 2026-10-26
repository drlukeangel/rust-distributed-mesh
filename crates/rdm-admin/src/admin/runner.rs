//! AdminRunner, GossipLifecycle, and mesh state handoff.

use super::*;

use crate::seat_revision::FenceOutcome;

/// The exact id of `mesh` a launch or a hand-off names: what this admin created (`records`), else what
/// its view holds from the mesh's members, else the mesh's own row, a point read by its name in
/// `mesh_rows` (a mesh none of whose births the view still holds). `Ok(None)` when none of them holds
/// one; a Dead row or a row that cannot be read is named. A recovery never mints one.
pub(crate) async fn mesh_id_of(records: &Records, topology: &RwLock<Topology>, mesh_rows: &dyn crate::storage::MeshStorage, mesh: &str) -> Result<Option<String>, String> {
    let known = topology.read().await.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone());
    if let Some(id) = records.meshes.lock().unwrap().get(mesh).cloned().or(known) {
        return Ok(Some(id.to_string()));
    }
    match mesh_rows.mesh_named(mesh).await {
        Ok(Some(row)) if !row.dead => Ok(Some(row.mesh_id.to_string())),
        Ok(Some(row)) => Err(format!("mesh {mesh}'s row is Dead (mesh id {})", row.mesh_id)),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("neither this admin's records nor its view hold a mesh id for {mesh}, and its row could not be read from mesh.storage: {e}")),
    }
}

/// Realises Build operations through the deployment and lifecycle pipelines.
pub struct AdminRunner {
    /// The provider that realises runtimes.
    pub provider: Arc<dyn DeploymentProvider>,
    /// The births this admin deployed and awaits a `JoinNode` from.
    pub joins: Arc<crate::join::Joins>,
    /// The observer of births' topology view.
    pub observer: Arc<TopologyObserver>,
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
    /// The digest view.
    pub digest_view: DigestView,
    /// This admin's own path.
    pub me: PathName,
    /// This admin's node id (the adopter on every runtime it adopts).
    pub node_id: NodeId,
    /// This admin's Iroh endpoint: it asks a node directly whether it answers.
    pub endpoint: Option<Endpoint>,
    /// This process's one Node RPC client (and its live resolver): every running admin holds it.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// Its connections store: the by-destination index names node investigation's leads.
    pub connections: Option<Arc<dyn crate::storage::ConnectionsStorage>>,
    /// Where the retire pipeline's lifecycle events go: this admin's topology view and backbone.
    pub lifecycle_events: Arc<dyn crate::deployment::pipeline::LifecycleEvents>,
    /// The mesh leaves this admin, as the fabric-primary, has admitted and awaits a handoff for.
    pub leaves: Arc<crate::mesh_delete::MeshDeleteView>,
    /// The mesh leaves this admin, as a leaving mesh's primary, is running or has finished.
    pub mesh_delete_runs: crate::mesh_delete::MeshDeleteRuns,
    /// This admin's own mesh.delete door: the fabric-primary that is its own mesh's primary (the last
    /// mesh of fabric.delete, lifecycles/fabric-delete.md step 1) commands that mesh's delete here, in
    /// process, as a Node RPC client reaches no endpoint of its own.
    pub mesh_deleter: crate::mesh_delete::MeshDeleterSlot,
    /// The fabric-primary handover: this admin's initiator, doors and recorded operations.
    pub handover: Arc<crate::fabric_handover::HandoverDoor>,
    /// The signer this admin issues a new mesh's first admin's issuing material with.
    pub issuer: Arc<crate::certs::CertIssuer>,
    /// This admin's nodes.storage: the facts on its mesh's births (cert dates among them).
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// This admin's mesh.storage: as a mesh primary, its mesh's row Dead at mesh.deleted, and as
    /// fabric-primary at mesh-left, a leaving mesh's row Dead when no primary survived (R-MD1).
    pub mesh_storage: Arc<dyn crate::storage::MeshStorage>,
    /// This admin's fabric.storage: as fabric-primary, releases the fabric name at fabric.deleted.
    pub fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage>,
    /// The app's listeners by kind (Q151), registered at this admin's start: what a create assigns and
    /// a join is verified against.
    pub listeners: crate::deployment::endpoint::ListenerNames,
    /// The drives of the Builds this admin drives: the node.delete of this admin's own birth ends its
    /// runtime once the callers of that Build read its terminal ([`Self::delete_own`]).
    pub drives: Arc<crate::build_drive::Drives>,
}

/// The lifecycle events of an operation this admin executes, on its own mesh channel only. The
/// backbone carries them as the open overlays and retained departures of this mesh's aggregate,
/// which its mesh primary publishes; a peer mesh's primary forwards them onto its own channel.
pub struct GossipLifecycle {
    /// This admin's mesh channel.
    pub topology_view: TopologyView,
    /// This admin's place on the backbone: the fabric state hooks publish there as well.
    pub backbone: Backbone,
    /// This admin's own path: the publisher of the mesh frames it authors.
    pub me: PathName,
    /// This admin's mesh.delete door, once its runner exists: every birth this executor publishes
    /// deleted is handed to it (mesh-left follows the final primary's exit proof, R-MD1 item 2).
    pub mesh_deleter: crate::mesh_delete::MeshDeleterSlot,
    /// This admin's view projected and installed at once ([`view_projector`]): a stop this executor
    /// applied to its own digest view is in its view before the next step decides from it, as a moved row is.
    pub project: Option<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>,
    /// This admin's status authority: a stop's `stopped` is recorded in the birth's declared-state
    /// row there, which publishes NodeStopped once, from the reply or the birth's declaration,
    /// whichever arrives first (i146 Q29, Q36). An executor with no status authority (a rig) holds no
    /// declared row: it publishes from the reply alone.
    pub authority: crate::status_rpc::AuthoritySlot,
}

/// NodeStopped for `op`'s exact birth, published by its mesh-primary (node-stop.md step 5): the
/// departure in this mesh's in-flight overlay, and this admin's view projected at once.
pub(crate) async fn publish_node_stopped(backbone: &Backbone, project: Option<&Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>, op: &rdm_core::LifecycleOp) {
    let span = tracing::info_span!("rdm.node_admin.node.update.via-node-stopped", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
    async {
        backbone.publish_status_event(rdm_transport::topology::StatusEvent::Stopped, op).await;
        if let Some(project) = project {
            project().await;
        }
        tracing::info!("the node is stopped: every mesh drops it from its view; no heartbeat of it ends the stop, only this mesh-primary's node.joined");
    }
    .instrument(span)
    .await
}

/// The publisher of NodeStopped a status authority calls once it applies a birth's Leaving that names
/// the operation which stopped it (i146 Q29, Q36): NodeStopped, the operation stamped with this
/// admin's Rafka-time.
pub(crate) fn stopped_publisher(topology_view: TopologyView, backbone: Backbone, project: Option<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>) -> crate::status_rpc::StoppedPublisher {
    Arc::new(move |row: crate::status_rpc::Declaration, operation: rdm_core::status::DeclaredOperation| {
        let (topology_view, backbone, project) = (topology_view.clone(), backbone.clone(), project.clone());
        Box::pin(async move {
            // Inside node.restart the stop's stopped is held by NodeRestarting, never published as
            // NodeStopped (gossip.md, "Node-stopped lifecycle overlay"; Q135): NodeRestarting ends at
            // node.started of the same birth, where a NodeStopped would wait for a node.joined.
            if let Some(restart) = topology_view.digest_view.restart_of(row.node_id.as_str(), &row.incarnation_id) {
                tracing::info_span!("rdm.node_admin.node.update.via-node-stopped", node = %row.name, node_id = %row.node_id, incarnation_id = %row.incarnation_id.as_str(), build_id = %operation.build_id, attempt = operation.attempt, operation = %operation.operation, outcome = "held-by-restart", restart = %restart.operation)
                    .in_scope(|| tracing::info!("the stop belongs to an open restart of this birth: NodeRestarting holds it and no NodeStopped is published"));
                return;
            }
            let op = rdm_core::LifecycleOp {
                build_id: operation.build_id,
                attempt: operation.attempt,
                operation: operation.operation,
                node_id: row.node_id,
                incarnation: row.incarnation_id,
                name: row.name,
                event_at_rafka_ms: topology_view.clock().now_rafka_ms(),
            };
            publish_node_stopped(&backbone, project.as_ref(), &op).await;
        })
    })
}

impl GossipLifecycle {
    #[allow(dead_code)]
    pub(crate) fn held_name(&self) -> PathName {
        self.topology_view.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }
}

#[async_trait::async_trait]
impl crate::deployment::pipeline::LifecycleEvents for GossipLifecycle {
    fn now_rafka_ms(&self) -> u64 {
        self.topology_view.clock().now_rafka_ms()
    }
    async fn deleting(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-deleting", node = %op.name, node_id = %op.node_id, build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rdm_transport::topology::StatusEvent::Deleting, op).await;
            tracing::info!("the node is being removed: every mesh hears it is not routable");
        }
        .instrument(span)
        .await
    }
    async fn restarting(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-restarting", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rdm_transport::topology::StatusEvent::Restarting, op).await;
            tracing::info!("the node is being restarted: every mesh holds it through its Leaving");
        }
        .instrument(span)
        .await
    }
    async fn stopped(&self, op: &rdm_core::LifecycleOp) {
        match self.authority.get() {
            Some(_) => {
                // The command stream's owner already recorded NodeStopped before recovery.
            }
            None => publish_node_stopped(&self.backbone, self.project.as_ref(), op).await,
        }
    }
    async fn joined(&self, op: &rdm_core::LifecycleOp) {
        let _ = crate::status_rpc::set_member_status(&self.topology_view.digest_view, crate::status_rpc::MemberStatusReceipt::NodeJoined { op, already: None });
    }
    async fn leaving(&self, op: &rdm_core::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-stop-admitted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        span.in_scope(|| {
            let _ = crate::status_rpc::set_member_status(&self.topology_view.digest_view, crate::status_rpc::MemberStatusReceipt::StopAdmitted(op));
            tracing::info!("the birth admitted stop-node: this mesh-primary sets it stopping and its projection carries that; no frame is published");
        });
    }
    async fn deleted(&self, op: &rdm_core::LifecycleOp) {
        let f = rdm_transport::topology::Frame::NodeDeleted { op: op.clone(), forwarded_by: None };
        let span = tracing::info_span!("rdm.node_admin.node.delete.via-node-deleted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            if let Err(e) = self.topology_view.publish_lifecycle(&f).await {
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
    /// The name this admin currently holds in its topology digest view, or its boot name before its
    /// birth is in the view.
    pub(crate) fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// Publish the view as it is now, so a Build that completes after this
    /// operation is never read back against an older view.
    pub(crate) async fn refresh_view(&self) {
        let me = self.held_name();
        self.records.install_read(&me, &self.topology, &self.digest_view, &self.fabric_id, |reads| project_with(&self.fabric, &self.fabric_id, self.fabric_provider, &self.digest_view, &self.records, reads)).await;
    }

    /// The template of an operation on a node of `kind` in `mesh` that launches nothing (a delete, a
    /// stop, a start, a restart in place, a terminate, a rename): it names no executable. Every node
    /// carries its mesh's identity (it names the mesh's channel): the one this admin created, else the
    /// one the fabric knows (a recovered mesh is reconstructed as itself, never anew). A create's
    /// template is [`Self::create_template`]'s, which names the executable it launches.
    pub(crate) async fn template_for(&self, kind: NodeKind, mesh: &str) -> Result<LaunchTemplate, String> {
        self.template_launching(kind, mesh, PathBuf::new(), None).await
    }

    /// The template a node.create of `node` launches from (node-rpc-envelope.md "Build recipe";
    /// node-replace.md step 3; node-recovery.md "Retained"; build-deployment.md §2): the binding
    /// `requested` names (the Build's: a step's, or the path's entry), else the one the node's seeding
    /// row records (the row at the path, else its `<name>.old` row: the predecessor's, the exited
    /// birth's). A create nothing names an executable for is refused by name and nothing is launched:
    /// no executable is taken from a directory with no sha. A binding the step or a row names is
    /// re-hashed now: a missing file and a hash that differs are refused by name and nothing is
    /// launched. The template carries the binding, so the create's name claim records it on the row.
    pub(crate) async fn create_template(&self, node: &PathName, requested: Option<&rdm_core::binding::Binding>) -> Result<LaunchTemplate, String> {
        let chosen = match requested {
            Some(b) => Some((b.clone(), "build")),
            None => self.recorded_executable(node).await?.map(|b| (b, "row")),
        };
        let Some((binding, from)) = chosen else {
            tracing::info_span!("rdm.node_admin.node.reject.via-executable-missing", node = %node, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("nothing names the executable this create launches: refused, nothing launched"));
            return Err(format!("node.create:{node}: executable-missing: neither the Build (its step or the path's entry) nor the row at the path names the executable it runs; nothing is launched (build-deployment.md §2)"));
        };
        self.template_bound(node, binding, from).await
    }

    /// The template a node.create of `node` launches `binding` from, the binding `from` names (the
    /// Build, a row): re-hashed now, a missing file and a hash that differs refused by name and nothing
    /// launched.
    pub(crate) async fn template_bound(&self, node: &PathName, binding: rdm_core::binding::Binding, from: &str) -> Result<LaunchTemplate, String> {
        verify_binding(&binding, node.kind, self.fabric_provider, from).await.map_err(|e| format!("node.create:{node}: the executable the {from} names is refused; nothing is launched: {e}"))?;
        tracing::info!(launch_id = %binding.launch_id, executable = %binding.executable.display(), sha256 = %binding.sha256, from, "launching from the explicit executable binding");
        let executable = binding.executable.clone();
        self.template_launching(node.kind, &node.mesh, executable, Some(binding)).await
    }

    /// The executable `node`'s entry names in Build `build_id`'s accepted topology as builds.storage
    /// records it ([`BuildStateAdapter::stored_accepted`]), when it names one.
    async fn entry_executable(&self, build_id: &crate::build::BuildId, node: &PathName) -> Result<Option<rdm_core::binding::Binding>, String> {
        let stored = self.builds.stored_accepted(build_id).await.map_err(|e| format!("node.create:{node}: the accepted Build {build_id} could not be read from builds.storage for the path's executable: {e}"))?;
        Ok(stored.and_then(|a| a.topology.mesh(&node.mesh).and_then(|m| m.spec(node)).and_then(|c| c.executable.clone())))
    }

    /// The storage `node`'s entry names in Build `build_id`'s accepted topology as builds.storage records
    /// it ([`BuildStateAdapter::stored_accepted`]), when the topology holds the path.
    async fn entry_storage(&self, build_id: &crate::build::BuildId, node: &PathName) -> Result<Option<rdm_core::meta::StorageMeta>, String> {
        let stored = self.builds.stored_accepted(build_id).await.map_err(|e| format!("node.create:{node}: the accepted Build {build_id} could not be read from builds.storage for the path's storage: {e}"))?;
        Ok(stored.and_then(|a| a.topology.mesh(&node.mesh).and_then(|m| m.spec(node)).map(|c| c.storage)))
    }

    /// The executable binding the row a create of `node` is seeded from records (the claim's seed,
    /// [`crate::node_rows::NodeRows::claim`]): the row at the path, else its `<name>.old` row.
    async fn recorded_executable(&self, node: &PathName) -> Result<Option<rdm_core::binding::Binding>, String> {
        let Some(rows) = self.records.contacts.get() else { return Ok(None) };
        let at = crate::node_rows::record_at(&**rows, node).await.map_err(|e| format!("node.create:{node}: the row at {node} could not be read for its executable: {e}"))?;
        let seed = match at {
            Some(row) => Some(row),
            None => crate::node_rows::record_at(&**rows, &node.renamed()).await.map_err(|e| format!("node.create:{node}: the row at {} could not be read for its executable: {e}", node.renamed()))?,
        };
        Ok(seed.and_then(|row| row.executable))
    }

    /// [`Self::template_for`] launching `executable`, `binding` its binding when one is known.
    async fn template_launching(&self, kind: NodeKind, mesh: &str, executable: PathBuf, binding: Option<rdm_core::binding::Binding>) -> Result<LaunchTemplate, String> {
        let known = mesh_id_of(&self.records, &self.topology, &*self.mesh_storage, mesh).await.map_err(|e| format!("{kind:?} in {mesh}: {e}", kind = kind.name()))?;
        let mut t = self.template.clone();
        t.launcher.name = self.held_name();
        t.executable = executable;
        t.binding = binding;
        if kind == NodeKind::NodeAdmin {
            // `RDM_EXECUTABLE_DIRS` among them: the launched admin mounts the directories this admin's
            // launch named (proposal executable-bindings-at-create.md §2).
            t.env.extend(self.admin_env.clone());
        }
        // Every node of a mesh carries its id: it names the mesh's channel.
        if let Some(id) = known {
            t.env.insert(rdm_core::launch::ENV_MESH_ID.into(), id);
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

    /// `template` for the launch of a node-admin of `mesh`: it carries the mesh's issuing material
    /// (fabric-certs.md "Where a node-admin's signing material comes from"), the issuer this admin
    /// holds or, holding the root, mints once for the mesh (`mesh.issuer.create`) and keeps in process;
    /// the launched admin's signing cert dates are written when its join is admitted. A refusal fails
    /// the create by name: the admin is never launched without it.
    pub(crate) async fn with_issuing_material(&self, mut t: LaunchTemplate, mesh: &str) -> Result<LaunchTemplate, String> {
        let material = self.issuer.issuing_material(mesh).map_err(|e| format!("{mesh}: the mesh's issuing material: {e}"))?;
        t.issuing = material;
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
    /// runtime the birth publishes with its topology view (`MeshNode::runtime`),
    /// adopted when its provider and control domain are this admin's. No
    /// Build history is read: whoever launched it, and however long ago, the
    /// birth itself says where its runtime is. A fact from another provider
    /// or control domain is refused by name, never acted on.
    /// The runtime fact `node`'s exact birth published, the data dir it named, and whether it was read
    /// from its digest (`true`) or from the durable runtime row its maker handed over: the birth says
    /// where its runtime is. A birth in neither is refused by name.
    async fn published_runtime(&self, node: &Node, incarnation: &IncarnationId) -> Result<(Option<rdm_core::runtime::RuntimeFact>, Option<String>, bool), String> {
        let me = self.held_name();
        let held = self
            .digest_view
            .get(node.node_id.as_str())
            .or_else(|| self.digest_view.projected_at(node.node_id.as_str(), std::time::Instant::now()))
            .map(|(d, _)| d)
            .or_else(|| self.digest_view.stopped_digests().into_iter().find(|d| d.node.node_id == node.node_id))
            .filter(|d| d.node.incarnation == *incarnation);
        let row = match (&held, self.records.contacts.get()) {
            (None, Some(store)) => birth_runtime_row(&**store, &me, &node.name, &node.node_id, incarnation).await?,
            _ => None,
        };
        match (&held, &row) {
            (Some(d), _) => Ok((d.node.runtime.clone(), d.data_dir.clone(), true)),
            (None, Some(r)) => Ok((Some(r.runtime.clone()), r.data_dir.clone(), false)),
            (None, None) => Err(format!("{} (birth {}) is not held in this admin's topology view or its durable runtime rows; its runtime is unknown", node.name, incarnation.as_str())),
        }
    }

    /// The launch the create's claim on `node`'s row records for the exact birth `incarnation` (the row's
    /// allocation: the birth's deployment), as the provider finds it from the launch record in the
    /// birth's data dir (deployment-provider.md §2: "an executor lost at any point of the spawn leaves
    /// `find` a record to read"; "Find is for the launcher's successor"; lifecycles/node-create.md: exact
    /// provider proof of a runtime's state, even for a deployment that never joined). A runtime that runs
    /// is held here by its exact handle, as an adopted one is, so the birth's delete acts on that runtime
    /// and proves its exit through it. `Ok(None)` when the row holds no claim of this birth. One
    /// `via-claimed-launch` span.
    pub(crate) async fn claimed_launch(&self, node: &Node, incarnation: &IncarnationId) -> Result<Option<(crate::deployment::provider::Found, Node)>, String> {
        use tracing::Instrument;
        let Some(store) = self.records.contacts.get() else { return Ok(None) };
        let row = store.node(&node.node_id).await.map_err(|e| format!("the row of {} ({}) could not be read for the launch its claim records: {e}", node.name, node.node_id))?;
        let Some((row, allocated)) = row.filter(|r| r.name == node.name).and_then(|r| r.allocated.clone().filter(|a| a.incarnation == *incarnation).map(|a| (r, a))) else { return Ok(None) };
        let data_dir = self.template.data_root.join(format!("{}-{}", node.name, node.node_id));
        let executable = row.executable.as_ref().map(|b| b.executable.clone()).unwrap_or_default();
        let spec = crate::deployment::provider::ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(), node: node.name.clone(), deployment_id: allocated.deployment_id.clone(), executable, args: vec![], env: Default::default(), data_dir: data_dir.clone(), transport: SocketAddr::from(([0, 0, 0, 0], 0)), listeners: vec![] };
        let span = tracing::info_span!(
            "rdm.node_admin.runtime.resolve.via-claimed-launch",
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.as_str(),
            deployment_id = %allocated.deployment_id,
            build_id = %allocated.build_id.log(),
            operation = %allocated.operation,
            reader = %self.held_name(),
            outcome = tracing::field::Empty,
        );
        let found = self.provider.find(&spec).instrument(span.clone()).await.map_err(|e| format!("the launch of {} at deployment {} could not be found: {e}", node.name, allocated.deployment_id))?;
        span.record("outcome", found.name());
        span.in_scope(|| tracing::info!("the birth's runtime is the launch its create's claim records, as the provider finds it"));
        let mut record = node.clone();
        record.deployment_id = Some(allocated.deployment_id.clone());
        record.data_dir = Some(data_dir.to_string_lossy().into_owned());
        if let crate::deployment::provider::Found::Running(handle) = &found {
            self.handles.lock().unwrap().insert(node.name.clone(), (record.clone(), handle.clone()));
        }
        Ok(Some((found, record)))
    }

    pub(crate) async fn handle_for(&self, node: &Node) -> Result<(Node, DeploymentHandle), String> {
        if let Some((rec, h)) = self.handles.lock().unwrap().get(&node.name).cloned() {
            if node.incarnation_id.is_none() || rec.incarnation_id == node.incarnation_id {
                return Ok((rec, h));
            }
        }
        let me = self.held_name();
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", node.name))?;
        let (published, data_dir, held) = match self.published_runtime(node, &incarnation).await {
            Ok(resolved) => resolved,
            // A birth launched by a create that never reached its join publishes no runtime fact and has
            // no runtime row: its runtime is the launch its claim records, as the launcher's successor
            // finds it ([`Self::claimed_launch`]).
            Err(e) => {
                return match self.claimed_launch(node, &incarnation).await {
                    Ok(Some((crate::deployment::provider::Found::Running(handle), record))) => Ok((record, handle)),
                    // An exited launch with its exact handle: the delete inspects that runtime and its
                    // Exited is the exit proof (node-delete.md "Crashed target", "Terminal event").
                    Ok(Some((crate::deployment::provider::Found::Exited(Some(handle)), record))) => Ok((record, handle)),
                    Ok(Some((found, _))) => Err(format!("{e}; the launch its create's claim records is {}, so no runtime of it runs", found.name())),
                    Ok(None) => Err(e),
                    Err(why) => Err(format!("{e}; {why}")),
                };
            }
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
            source = if held { "self-published-topology" } else { "durable-runtime-row" },
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
        let out = crate::seat_revision::investigate_path(path, prev, self).await;
        if matches!(out, FenceOutcome::Clear { .. }) {
            self.handles.lock().unwrap().remove(path);
        }
        out
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
        for d in self.digest_view.all() {
            if let Some(runtime) = d.node.runtime.clone() {
                let r = crate::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime, data_dir: d.data_dir.clone() };
                rows.insert(r.key(), r);
            }
        }
        rows.into_values().collect()
    }

    /// node.create of `node`, its name claimed for `claim_for` (a plain create, or a replace
    /// successor's at the name its predecessor released), launching the executable the step names, else
    /// the path's entry in the accepted Build as builds.storage records it ([`Self::entry_executable`];
    /// proposal executable-bindings-at-create.md rules 1-2: a Build message carries none, rule 7, so this
    /// admin's own copy of the Build may name none), else the row's ([`Self::create_template`]).
    pub(crate) async fn create(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, executable: Option<&rdm_core::binding::Binding>, pre_node_ready: Option<crate::deployment::pipeline::PreNodeReady>, claim_for: crate::node_rows::ClaimFor) -> Result<(), crate::executor::RunEnd> {
        let stored = match executable {
            Some(_) => None,
            None => self.entry_executable(build_id, node).await?,
        };
        let executable = executable.or(stored.as_ref());
        match self.revise_predecessor(node).await {
            // The member answers: it is commanded back, never created over (node-recovery.md step 3).
            FenceOutcome::Alive => return Ok(()),
            // An unreached birth holds the path's name until it is deleted (only a delete releases a
            // name, R-MN1): node.delete deletes it (`TerminateRuntime` first when its runtime runs), then
            // the create runs at the same path.name, over the inherited data dir when the delete read
            // the old runtime exited and over a newly allocated one otherwise (node-recovery.md
            // "Retained", `R-FM2`), launching this create's executable: the one its step names (a Build's
            // create of a path its topology names, the path's entry), never the unreached birth's
            // row's in its place (proposal executable-bindings-at-create.md rule 5: one whose entry
            // differs from its row is replaced; rule 6 is a recovery's create, which names none).
            FenceOutcome::Clear { gone: Some(prev) } => {
                if let Some(incarnation) = prev.incarnation_id.clone() {
                    let from = crate::build::ExitedBirth { node_id: prev.node_id.clone(), incarnation };
                    self.delete_unreached(&*self.builds, build_id, attempt, node, &from).await?;
                    return self.create_over(&*self.builds, build_id, attempt, node, &from, executable).await;
                }
            }
            FenceOutcome::Clear { gone: None } => {}
        }
        let mut template = self.create_template(node, executable).await?;
        if node.kind == NodeKind::NodeAdmin {
            template = self.with_issuing_material(template, &node.mesh).await?;
        }
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: self.listeners.spec_for(node.kind), data_dir: None, held_runtimes };
        let created = match claim_for {
            crate::node_rows::ClaimFor::Recovery => self.pipeline(&template).create_recovery_with(&req, pre_node_ready, None).await,
            crate::node_rows::ClaimFor::Successor => self.pipeline(&template).create_successor_with(&req, pre_node_ready).await,
            crate::node_rows::ClaimFor::Create => self.pipeline(&template).create_with(&req, pre_node_ready, None).await,
            crate::node_rows::ClaimFor::Arrival => unreachable!("a node.create claims its name; an arrival's row is the writer's, never a create's"),
        }
        .map_err(crate::executor::RunEnd::from)?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The state this admin's hand-off gives a mesh's first admin (the hook below): the mesh's current
    /// held state for an existing mesh, as its MeshStatus is held here, and `Pending` for a mesh it
    /// holds no state for (a new mesh, e4.s11). A held word that is no Mesh lifecycle state is no
    /// state held.
    fn held_mesh_state(&self, mesh: &str) -> rdm_core::status::MeshState {
        self.digest_view.statuses.mesh(mesh).and_then(|h| super::reconcile::mesh_state_of(&h.status).ok()).unwrap_or(rdm_core::status::MeshState::Pending)
    }

    /// The fabric-primary -> mesh state hand-off at a mesh's first admin, as the create pipeline's
    /// `ApplyMeshPending` step at the exact birth just joined at `admin`: `Pending` for a new mesh's
    /// bootstrap admin (e4.s11), the mesh's current state for the admin created for an existing
    /// mesh. A bounded reachability wait (Ping, at most 20 s, no Status), then ONE
    /// `ApplyMeshState(state)` call. `Applied` or
    /// `AlreadyApplied` succeed; any other outcome fails the step typed (a stale-birth answer, a
    /// backward move, an unreachable target, a lost reply), and the rectifier is the retry, so
    /// `Build(shape)` never starts under an assumed state.
    pub(crate) fn mesh_state_hook(&self, admin: &PathName, state: rdm_core::status::MeshState) -> Result<crate::deployment::pipeline::PreNodeReady, String> {
        let node_rpc = &self.node_rpc;
        let (client, resolver, me, digest_view, me_id, topology, mesh, records, mesh_rows) = (node_rpc.client.clone(), node_rpc.resolver.clone(), self.me.clone(), self.digest_view.clone(), self.node_id.clone(), self.topology.clone(), admin.mesh.clone(), self.records.clone(), self.mesh_storage.clone());
        Ok(Arc::new(move |target: Node| {
            let (client, resolver, me, digest_view, me_id, topology, mesh, records, mesh_rows) = (client.clone(), resolver.clone(), me.clone(), digest_view.clone(), me_id.clone(), topology.clone(), mesh.clone(), records.clone(), mesh_rows.clone());
            Box::pin(async move {
                let me = digest_view.held_path_name().unwrap_or_else(|| me.clone());
                // The mesh's exact identity, as the launch took it ([`mesh_id_of`]; a recovery never mints one).
                let mesh_id = match mesh_id_of(&records, &topology, &*mesh_rows, &mesh).await {
                    Ok(Some(id)) => id,
                    Ok(None) => return Err(format!("{}: neither this admin's records, its view nor mesh.storage hold a mesh id for {mesh}, so no {state:?} can be applied", target.name)),
                    Err(e) => return Err(format!("{}: {e}, so no {state:?} can be applied", target.name)),
                };
                apply_mesh_state(&client, &resolver, &me, &me_id, &topology, &mesh, &mesh_id, state, &target).await.map(|()| state)
            })
        }))
    }

    /// The exact exited birth `from` at `node` a node recovery acts for (node-recovery.md), and its
    /// exact runtime: the view's record while it holds that birth, else a record of the exact birth
    /// resolved from the topology view or the durable runtime row this admin wrote when it first
    /// held the birth (a departed birth has left the view). `op` names the operation in a refusal.
    pub(crate) async fn exited_birth(&self, op: &str, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(Node, DeploymentHandle), String> {
        let seen = self.topology.read().await.node(node).cloned().filter(|n| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        let exact = seen.unwrap_or_else(|| {
            let mut n = Node::allocated(node.clone());
            n.node_id = from.node_id.clone();
            n.incarnation_id = Some(from.incarnation.clone());
            n
        });
        let (exited, handle) = match self.handle_for(&exact).await {
            Ok(adopted) => adopted,
            Err(e) => self.unadoptable(&exact).await.ok_or_else(|| format!("{op}:{node}: the exited birth's runtime is not resolved: {e}"))?,
        };
        if exited.node_id != from.node_id || exited.incarnation_id.as_ref() != Some(&from.incarnation) {
            return Err(format!("{op}:{node}: resolved birth {} at {:?}, the recovery is of {} at {}", exited.node_id, exited.incarnation_id.as_ref().map(|i| i.as_str()), from.node_id, from.incarnation.as_str()));
        }
        Ok((exited, handle))
    }

    /// The exact unreached birth `exact` whose published runtime this admin's provider may not act on
    /// (another provider, or another control domain): its record, and a handle that locates nothing.
    /// Node recovery acts on an unreached birth's runtime only when it reads `running`
    /// (node-recovery.md step 3, Q111); a runtime this provider cannot read is read by nothing here, so
    /// the delete's `TerminateRuntime` reads it `unavailable`, terminates nothing, and the create
    /// that follows is over a newly allocated data dir. The provider's refusal to act on the locator
    /// stands. `None` when the birth published no runtime fact.
    async fn unadoptable(&self, exact: &Node) -> Option<(Node, DeploymentHandle)> {
        let incarnation = exact.incarnation_id.clone()?;
        let (Some(fact), data_dir, _) = self.published_runtime(exact, &incarnation).await.ok()? else { return None };
        let foreign = matches!(crate::deployment::provider::adopt(&*self.provider, &fact), Err(crate::deployment::provider::AdoptRefusal::ForeignControlDomain { .. } | crate::deployment::provider::AdoptRefusal::OtherProvider { .. }));
        if !foreign {
            return None;
        }
        let deployment_id = rdm_core::DeploymentId::parse(&fact.deployment_id).ok()?;
        let provider = match fact.provider {
            rdm_core::runtime::RuntimeProvider::Process => crate::model::ProviderKind::Process,
            rdm_core::runtime::RuntimeProvider::Container => crate::model::ProviderKind::Container,
        };
        let mut record = exact.clone();
        record.deployment_id = Some(deployment_id.clone());
        if record.data_dir.is_none() {
            record.data_dir = data_dir;
        }
        Some((record, DeploymentHandle { deployment_id, provider, pid: None, start: None, container: None, domain: Some(fact.control_domain.clone()) }))
    }

    /// Node recovery's node.delete (node-recovery.md sequence step 3): the exact unreached birth `from`
    /// at `node`, deleted through the delete workflow, nothing sent to it: `TerminateRuntime` first
    /// when its runtime runs at the step's instant, and nothing waits for a proof of termination. The
    /// fence of the old birth is the create's ([`Self::create_over`]), done right before the new birth
    /// with its key is launched. Answers the delete's `TerminateRuntime` receipt: a mesh.delete takes
    /// it as the birth's roster receipt.
    pub(crate) async fn delete_unreached(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<crate::deployment::pipeline::TerminalReceipt, crate::executor::RunEnd> {
        self.delete_unreached_as(builds, build_id, attempt, node, from, false).await
    }

    /// [`Self::delete_unreached`], `explicit` for an explicit node.delete of a birth no call reaches (a
    /// mesh.delete's lost or silent birth, a member of a mesh deleted in process under fabric.delete;
    /// fabric-delete.md "Receipts"): an `os-stopped` runtime is terminated `Immediate`
    /// (deployment-provider.md §2.5, node-runtime-inspect.md "What follows"), where node recovery's
    /// delete leaves it as it is.
    pub(crate) async fn delete_unreached_as(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth, explicit: bool) -> Result<crate::deployment::pipeline::TerminalReceipt, crate::executor::RunEnd> {
        let (unreached, handle) = match self.exited_birth("node.delete", node, from).await {
            Ok(resolved) => resolved,
            Err(e) => match receipted_exit(builds, build_id, node, from).await {
                Some(resolved) => resolved,
                // An explicit delete of a birth whose runtime is not resolved here has nothing to inspect or
                // terminate: its TerminateRuntime fails by name and no receipt is manufactured
                // (fabric-recovery.md step 4, Q160; fabric-delete.md "Failure").
                None if explicit => {
                    let step = crate::deployment::pipeline::RetireStep::TerminateRuntime.name();
                    let reason = format!("node.delete:{node} {step}: the runtime of {} at {} is not resolved here, so none can be inspected or terminated: the step fails by name and no receipt is manufactured (Q160): {e}", from.node_id, from.incarnation.as_str());
                    tracing::info_span!("rdm.node_admin.node.delete.via-unproven-runtime", build_id = %build_id.log(), attempt, node = %node, incarnation_id = %from.incarnation.as_str(), step, reason = %reason)
                        .in_scope(|| tracing::info!("the birth's runtime is not known here: its delete's step fails by name"));
                    return Err(crate::executor::RunEnd::failed(reason));
                }
                None => return Err(e.into()),
            },
        };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: unreached, handle, unreached: true };
        let pipeline = self.pipeline_with(&template, builds);
        // Fabric recovery's backbone arm terminates nothing in the mesh (fabric-recovery.md step 3, Q148;
        // node-delete.md): its delete reads the exact runtime, writes the row deleted and releases the name.
        let terminal = if backbone_attempt(builds, build_id, attempt, node).await {
            pipeline.delete_from_backbone(&req).await
        } else if explicit {
            pipeline.delete_explicit(&req).await
        } else {
            pipeline.delete_node(&req).await
        };
        let terminal = terminal.map_err(crate::executor::RunEnd::from)?;
        let held = self.handles.lock().unwrap().get(node).is_some_and(|(n, _)| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        if held {
            self.handles.lock().unwrap().remove(node);
        }
        self.refresh_view().await;
        Ok(terminal)
    }

    /// The termination of the deleted birth `(node_id, from_incarnation)` at `node`, heard again (Q128): its
    /// runtime adopted from its durable runtime row (its digest is refused), then the pipeline's
    /// `TerminateRuntime` on it.
    async fn terminate_returned(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, node_id: &crate::model::NodeId, from_incarnation: &IncarnationId) -> Result<(), crate::executor::RunEnd> {
        let mut birth = Node::allocated(node.clone());
        birth.node_id = node_id.clone();
        birth.incarnation_id = Some(from_incarnation.clone());
        let (birth, handle) = self.handle_for(&birth).await.map_err(|e| crate::executor::RunEnd::from(format!("node.terminate:{node}: {e}")))?;
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: birth, handle, unreached: true };
        self.pipeline_with(&template, builds).terminate_returned(&req).await.map_err(crate::executor::RunEnd::from)?;
        Ok(())
    }

    /// Whether node recovery's node.delete of `from` at `node` in `build_id` read the old runtime exited at
    /// its `TerminateRuntime` (node-recovery.md step 4: the create's spec names the old data dir "when the
    /// verdict was `exited`"; `R-FM2`), from that step's receipt for the birth: this attempt's, or the
    /// attempt's it resumes, whose delete completed before it ended.
    async fn delete_read_exited(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<bool, String> {
        let operation = format!("node.delete:{node}");
        let terminate = crate::deployment::pipeline::RetireStep::TerminateRuntime.name();
        let p = builds.read_build(build_id).await.map_err(|e| format!("node.create:{node}: Build {build_id} could not be read for its delete's TerminateRuntime receipt: {e}"))?;
        let receipt = p
            .steps
            .iter()
            .filter(|s| s.attempt <= attempt && s.operation == operation && s.step == terminate && s.outcome == crate::build_state::StepOutcome::Complete)
            .filter_map(|s| s.output.clone().and_then(|o| serde_json::from_value::<crate::deployment::pipeline::TerminalReceipt>(o).ok()).map(|t| (s.attempt, t)))
            .filter(|(_, t)| t.node_id == from.node_id.to_string() && t.incarnation_id == from.incarnation.to_string())
            .max_by_key(|(a, _)| *a)
            .map(|(_, t)| t);
        match receipt {
            Some(t) => Ok(t.verdict.as_deref() == Some("exited")),
            None => Err(format!("node.create:{node}: Build {build_id} holds no completed TerminateRuntime of {operation} for birth {} at {} up to attempt {attempt}", from.node_id, from.incarnation.as_str())),
        }
    }

    /// Node recovery's node.create (node-recovery.md sequence steps 4-5): a new node at `node` over the
    /// unreached birth `from`'s data dir when its delete read the old runtime exited, through the create
    /// workflow: it loads the endpoint key that dir holds and binds port 0, with a new node.id and
    /// incarnation, no supersedes; over a newly allocated data dir with a newly minted key otherwise. Not through
    /// the create door's fence: the birth at the path is the exited one this recovery deleted, and
    /// the fence would replace it.
    ///
    /// The exited birth is fenced here, by the create (node-recovery.md "Delete fences the old
    /// instance before the new join"): right before the new runtime with the same key is launched,
    /// inside its `DeployRuntime` step ([`crate::deployment::pipeline::BeforeLaunch`]). Whichever
    /// executor runs the create fences, so an executor lost after the delete's Complete leaves
    /// nothing unfenced; a rerun that reuses a runtime an earlier attempt launched (it was fenced
    /// before that launch) cuts none of that runtime's paths.
    pub(crate) async fn create_over(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth, executable: Option<&rdm_core::binding::Binding>) -> Result<(), crate::executor::RunEnd> {
        let (exited, _) = self.exited_birth("node.create", node, from).await?;
        // A node-admin created into a mesh with no other live node-admin is that mesh's first admin, as
        // every node.create of one is: it takes the mesh's issuing material and the fabric primary's
        // Pending hand-off before it is asked to be Ready (e4.s11; mesh-investigation.md "A node-admin
        // joins the live mesh").
        let first_admin = {
            let t = self.topology.read().await;
            t.first_admin_of(node) && crate::executor::held_admins(&node.mesh, Some(node), &t).is_empty()
        };
        let pre_node_ready = if first_admin { Some(self.mesh_state_hook(node, self.held_mesh_state(&node.mesh))?) } else { None };
        // A recovery's create names no executable: the launch runs the one the exited birth's row records,
        // on both arms (node-recovery.md "Retained": never the recovering admin's): the new node's config
        // and executable are seeded from that Deleted row by the create's name claim (`NodeRows::claim`),
        // and the config is told to it when its row is written at its arrival (`NodeRows::write_arrivals`).
        // A Build's create over an unreached birth launches the executable its step names.
        let mut template = self.create_template(node, executable).await?;
        if node.kind == NodeKind::NodeAdmin {
            template = self.with_issuing_material(template, &node.mesh).await?;
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
        // The inherited data dir only when the delete read the old runtime exited, and its loss is not
        // recorded (node-recovery.md step 4, `R-FM2`); a newly allocated one otherwise.
        let lost = match self.records.status.get() {
            Some(fold) => fold.caches().nodes.birth_facts(&**fold.nodes(), &from.node_id, &from.incarnation).await.ok().flatten().is_some_and(|f| f.data_dir_lost.is_some()),
            None => false,
        };
        let exited = self.delete_read_exited(builds, build_id, attempt, node, from).await?;
        let created = if lost || !exited {
            let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: self.listeners.spec_for(node.kind), data_dir: None, held_runtimes };
            pipeline.create_recovery_with(&req, pre_node_ready, Some(Box::pin(self.revise_exited(&fenced)))).await.map_err(crate::executor::RunEnd::from)?
        } else {
            if let Err(crate::deployment::pipeline::InheritedDirError::Broken(ref cause)) = crate::deployment::pipeline::inspect_transport_key(&data_dir) {
                if let Some(fold) = self.records.status.get() {
                    let unreadable = crate::storage::DataDirUnreadable { path: data_dir.display().to_string(), cause: cause.clone() };
                    let held = crate::storage::NodeRow::new(node.clone(), from.node_id.clone(), crate::model::NodeStatus::Deleted);
                    let _ = fold.caches().nodes.update(&**fold.nodes(), &from.node_id, Some(held), |r| r.birth_mut(&from.incarnation, node).data_dir_unreadable = Some(unreadable)).await;
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
            let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: self.listeners.spec_for(node.kind), data_dir: Some(data_dir), held_runtimes };
            pipeline.create_with(&req, pre_node_ready, Some(Box::pin(self.revise_exited(&fenced)))).await.map_err(crate::executor::RunEnd::from)?
        };
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// A replacement's node.create of the member at `node` whose birth `from` the replacement's
    /// node.delete deleted (build-deployment.md §3): a new node.id and incarnation at the same
    /// `path.name`, its storage following the path's entry in the accepted topology: a `Persistent`
    /// path's over the predecessor's kept data dir (`inherited`, `RDM_INHERITED_DATA_DIR=1`, the endpoint
    /// key loaded from it), an `Ephemeral` path's over a newly allocated data dir with a newly minted key
    /// (`RDM_INHERITED_DATA_DIR=0`); config and tags seeded from the Deleted row (node-replace.md
    /// "Inherited"), launching `executable`, the path's entry's. The Build fixes the binding, as a recipe's Create
    /// step does, so the launch never takes the one the Deleted row still records; the claim records it
    /// on the node's row, so a later node recovery runs it (node-recovery.md "Retained"). A kept data dir
    /// is the one the delete's `TerminateRuntime` receipt names: the delete stopped the exact runtime
    /// and proved it exited, and kept its storage (node-delete.md step 3). The old birth's resolver entry
    /// and direct paths are dropped right before the new runtime with its key is launched, as node
    /// recovery's create drops them ([`Self::revise_exited`]). A birth
    /// other than `from` already live at the path (a create whose receipt an earlier attempt never
    /// wrote) is the member: nothing is created again.
    pub(crate) async fn create_replacement(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth, executable: &rdm_core::binding::Binding) -> Result<(), crate::executor::RunEnd> {
        if let Some(n) = self.topology.read().await.node(node).filter(|n| n.status.is_live() && n.node_id != from.node_id) {
            tracing::info!(successor = %n.node_id, "the member's successor is already live at its path: nothing is created again");
            return Ok(());
        }
        let operation = format!("node.delete:{node}");
        let terminate = crate::deployment::pipeline::RetireStep::TerminateRuntime.name();
        let p = builds.read_build(build_id).await.map_err(|e| format!("node.create:{node}: Build {build_id} could not be read for its delete's TerminateRuntime receipt: {e}"))?;
        let terminal = p
            .steps
            .iter()
            .filter(|s| s.attempt <= attempt && s.operation == operation && s.step == terminate && s.outcome == crate::build_state::StepOutcome::Complete)
            .filter_map(|s| s.output.clone().and_then(|o| serde_json::from_value::<crate::deployment::pipeline::TerminalReceipt>(o).ok()).map(|t| (s.attempt, t)))
            .filter(|(_, t)| t.node_id == from.node_id.to_string() && t.incarnation_id == from.incarnation.to_string())
            .max_by_key(|(a, _)| *a)
            .map(|(_, t)| t)
            .ok_or_else(|| format!("node.create:{node}: Build {build_id} holds no completed TerminateRuntime of {operation} for birth {} at {} up to attempt {attempt}: the successor is created only after that delete", from.node_id, from.incarnation.as_str()))?;
        // The successor's storage follows the path's entry in the accepted topology (build-deployment.md
        // §3): `Persistent` inherits the data dir the delete kept, `Ephemeral` allocates a fresh one.
        let inherits = match self.entry_storage(build_id, node).await? {
            Some(rdm_core::meta::StorageMeta::Ephemeral) => false,
            Some(rdm_core::meta::StorageMeta::Persistent { .. }) => true,
            None => return Err(format!("node.create:{node}: the accepted Build {build_id} holds no entry for the path: its storage decides the successor's data dir; nothing is launched").into()),
        };
        let first_admin = {
            let t = self.topology.read().await;
            t.first_admin_of(node) && crate::executor::held_admins(&node.mesh, Some(node), &t).is_empty()
        };
        let pre_node_ready = if first_admin { Some(self.mesh_state_hook(node, self.held_mesh_state(&node.mesh))?) } else { None };
        let mut template = self.create_template(node, Some(executable)).await?;
        if node.kind == NodeKind::NodeAdmin {
            template = self.with_issuing_material(template, &node.mesh).await?;
        }
        let pipeline = self.pipeline_with(&template, builds);
        let mut predecessor = Node::allocated(node.clone());
        predecessor.node_id = from.node_id.clone();
        predecessor.incarnation_id = Some(from.incarnation.clone());
        let data_dir = if inherits {
            predecessor.data_dir = terminal.data_dir;
            let data_dir = pipeline.birth_dir(&predecessor);
            crate::deployment::pipeline::inspect_transport_key(&data_dir).map_err(|e| format!("node.create:{node}: {}", e.into_message()))?;
            // The key whose old paths are dropped is the one the kept data dir holds: the node created over it loads exactly it.
            predecessor.endpoint_id = crate::deployment::pipeline::load_transport_key(&data_dir).ok();
            Some(data_dir)
        } else {
            None
        };
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: self.listeners.spec_for(node.kind), data_dir, held_runtimes };
        // The claim seeds config and tags from the Deleted row on both arms (node-replace.md "Inherited").
        let created = pipeline.create_recovery_with(&req, pre_node_ready, Some(Box::pin(self.revise_exited(&predecessor)))).await.map_err(crate::executor::RunEnd::from)?;
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
            self.node_rpc.resolver.mark_exited(&record.node_id, incarnation);
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
            rpc.resolver.mark_exited(&record.node_id, incarnation);
        }
        // The exit is proven: the exited birth's direct paths are retired. Until now they stayed
        // open for the typed drain.
        if let (Some(ep), Some(key)) = (&self.endpoint, record.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok())) {
            // Not awaited: the endpoint's actor for this key answers when it is free, and the
            // retire does not wait for a dead peer's actor (the same call the topology view spawns).
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
    pub(crate) async fn standalone(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId, stop: bool) -> Result<(), crate::executor::RunEnd> {
        let op = if stop { "node.stop" } else { "node.connections.delete" };
        // The operation is fenced to the exact birth `from`, found by its incarnation: a birth that left
        // its mesh and took a name live publishes nothing more, so the view lists it at the name it last
        // published until it is stopped and its row is read (node-replace.md steps 1-2). Its row's name
        // is the operation's path.
        let (exact, at_path) = {
            let t = self.topology.read().await;
            let exact = t.births().find(|n| n.incarnation_id.as_ref() == Some(from)).cloned();
            (exact, t.node(node).cloned())
        };
        let (record, handle) = match (exact, at_path) {
            (Some(n), _) => {
                let (record, handle) = self.handle_for(&n).await?;
                (Node { name: node.clone(), ..record }, handle)
            }
            (None, Some(n)) => return Err(format!("{op}:{node} targets birth {}, the view holds {} at that path", from.as_str(), n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none")).into()),
            (None, None) => self.handles.lock().unwrap().get(node).cloned().filter(|(rec, _)| rec.incarnation_id.as_ref() == Some(from)).ok_or_else(|| format!("{op}:{node}: birth {} is not in this admin's view and no runtime handle of it is held", from.as_str()))?,
        };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, unreached: false };
        if stop {
            // The process is stopped, not exited: nothing is retired about its paths or its birth.
            self.pipeline(&template).stop(&req).await.map_err(crate::executor::RunEnd::from)?;
        } else {
            self.pipeline(&template).delete_connections(&req).await.map_err(crate::executor::RunEnd::from)?;
        }
        Ok(())
    }

    /// node.delete of a stopped birth (`node.delete:<path>`, node-delete.md): the stopped birth `from`
    /// at `node`, fenced by incarnation, ended with the provider's exact Exited proof and deleted
    /// (`DeploymentPipeline::delete_stopped`); what follows its proven exit follows.
    pub(crate) async fn delete_stopped(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), crate::executor::RunEnd> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) if n.incarnation_id.as_ref() == Some(from) => self.handle_for(&n).await?,
            Some(n) => return Err(format!("node.delete:{node} is fenced to birth {}, the view holds {} at that path", from.as_str(), n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none")).into()),
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
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, unreached: false };
        self.pipeline(&template).delete_stopped(&req).await.map_err(crate::executor::RunEnd::from)?;
        self.after_exit(&record);
        self.handles.lock().unwrap().remove(node);
        Ok(())
    }

    /// The exact stopped birth `from` that a delete of `node` (`<path>.old`) is fenced to, for an admin
    /// whose view does not name `node` and which did not run the replace (the mesh's primary changed
    /// since): the birth as its topology view holds it stopped, or else its durable runtime row (every
    /// admin keeps one per birth it heard, `runtime_rows_task`). The record is named `node`; its
    /// runtime is then adopted from the same sources by [`Self::handle_for`].
    pub(crate) async fn stopped_birth_by_incarnation(&self, node: &PathName, from: &IncarnationId) -> Result<Node, String> {
        let mut birth = Node::allocated(node.clone());
        birth.incarnation_id = Some(from.clone());
        birth.stopped = true;
        birth.status = NodeStatus::Leaving;
        if let Some(d) = self.digest_view.stopped_digests().into_iter().find(|d| &d.node.incarnation == from) {
            birth.node_id = d.node.node_id.clone();
            birth.endpoint_id = Some(d.node.endpoint_id.clone());
            birth.transport_addr = Some(d.node.transport_addr);
            birth.data_dir = d.data_dir.clone();
            return Ok(birth);
        }
        let Some(store) = self.records.contacts.get() else {
            return Err(format!("node.delete:{node}: birth {} is not in this admin's view or the topology view, and this admin has no nodes store to read its runtime row from", from.as_str()));
        };
        let rows = store.runtimes().await.map_err(|e| format!("node.delete:{node}: birth {} is not in this admin's view or the topology view, and its runtime rows could not be read: {e}", from.as_str()))?;
        let row = rows.into_iter().find(|r| &r.incarnation_id == from).ok_or_else(|| format!("node.delete:{node}: birth {} is not in this admin's view or the topology view, and this admin holds no runtime row of it", from.as_str()))?;
        birth.node_id = row.node_id;
        birth.data_dir = row.data_dir;
        Ok(birth)
    }

    /// The listeners `record`'s birth was assigned at its create (Q151): each declared name of its
    /// kind on the birth's address, port 0. A held birth's rejoin reports the same binds, which it
    /// kept open since; a birth with listeners to assign and no known address is refused by name.
    pub(crate) fn assigned_listeners(&self, record: &Node) -> Result<Vec<(String, std::net::SocketAddr)>, String> {
        let names = &self.listeners.spec_for(record.name.kind).listeners;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let ip = record.transport_addr.ok_or_else(|| format!("{} has no known address to verify its listeners against", record.name))?.ip();
        Ok(names.iter().map(|n| (n.clone(), std::net::SocketAddr::new(ip, 0))).collect())
    }

    /// What this admin holds for the start of the stopped `record`: the deployment its `JoinNode` is
    /// verified against, as the birth was admitted.
    pub(crate) fn deployed_for(&self, record: &Node, handle: &DeploymentHandle) -> Result<crate::join::Deployed, String> {
        let incarnation = record.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", record.name))?;
        let admitted = self.digest_view.stopped_digests().into_iter().find(|d| d.node.node_id == record.node_id && d.node.incarnation == incarnation);
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
            listeners: self.assigned_listeners(record)?,
        })
    }

    /// The stopped birth `from` at `node`: its record and exact runtime, and what the rejoin is
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
        let record = Node { status: seen.status, stopped: seen.stopped, ..record };
        let record = Node { endpoint_id: seen.endpoint_id.clone().or(record.endpoint_id), transport_addr: seen.transport_addr.or(record.transport_addr), data_dir: seen.data_dir.clone().or(record.data_dir), ..record };
        let deployed = self.deployed_for(&record, &handle)?;
        Ok((record, handle, deployed))
    }

    /// `node.restart:<path>` (node-restart.md): stop, then start, on the same process: the same node id,
    /// incarnation, endpoint key and port. The process never exits and no runtime is deployed.
    pub(crate) async fn restart_in_place(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> Result<(), crate::executor::RunEnd> {
        // A birth this restart's own earlier attempt stopped is held as a stopped one (its digest the
        // stopped one its rejoin is verified against), and its run resumes at the start.
        let stopped = self.topology.read().await.node(node).is_some_and(|n| n.stopped);
        let (record, handle, deployed) = if stopped { self.stopped_birth("node.restart", node, None).await? } else { self.stopped_birth_or_live("node.restart", node).await? };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = crate::deployment::pipeline::StartRequest { build_id: build_id.clone(), attempt, node: record, handle, deployed };
        self.pipeline(&template).restart(&req).await.map_err(crate::executor::RunEnd::from)
    }

    /// The live birth at `node` before its stop, for a restart: the same record shape as a stopped one.
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

    /// [`Self::deployed_for`] for a birth still in the digest view: its admitted digest is the view's.
    pub(crate) fn deployed_for_live(&self, record: &Node, handle: &DeploymentHandle) -> Result<crate::join::Deployed, String> {
        let incarnation = record.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", record.name))?;
        let admitted = self.digest_view.get(record.node_id.as_str()).map(|(d, _)| d).filter(|d| d.node.incarnation == incarnation);
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
            listeners: self.assigned_listeners(record)?,
        })
    }

    /// `node.start:<path>` (node-start.md): the stopped birth `from` rejoins as itself.
    pub(crate) async fn start_stopped(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), crate::executor::RunEnd> {
        let (record, handle, deployed) = self.stopped_birth("node.start", node, Some(from)).await?;
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = crate::deployment::pipeline::StartRequest { build_id: build_id.clone(), attempt, node: record, handle, deployed };
        self.pipeline(&template).start(&req).await.map_err(crate::executor::RunEnd::from)
    }

    pub(crate) async fn delete_node(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> Result<Option<Node>, crate::executor::RunEnd> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match &seen {
            Some(n) => self.handle_for(n).await?,
            None => {
                let held = self.handles.lock().unwrap().get(node).cloned();
                match held {
                    Some(h) => h,
                    // A delete this Build began, whose birth has since left the view (its runtime
                    // terminated): the exact birth its own NodeDeleting receipt names, its runtime read
                    // from the birth's durable row, so the delete ends with its receipt (fabric-delete.md
                    // "Failure": a runtime whose delete has no receipt is never reported deleted).
                    None => match self.begun_delete_birth(build_id, node).await? {
                        Some(birth) => self.handle_for(&birth).await?,
                        None => return Err(format!("node.delete:{node}: {} holds no birth at {node} in its view and no runtime handle of one", self.held_name()).into()),
                    },
                }
            }
        };
        // The record handed back with the runtime is the one this admin deployed; whether the birth
        // is stopped is the view's, as `stopped_birth` takes it: a stopped birth's delete
        // goes straight to its exact runtime's exit proof.
        let record = match &seen {
            Some(n) => Node { stopped: n.stopped, ..record },
            None => record,
        };
        if record.node_id == self.node_id {
            self.delete_own(build_id, attempt, record.clone(), handle).await?;
            return Ok(Some(record));
        }
        self.delete_birth(build_id, attempt, record.clone(), handle).await?;
        Ok(Some(record))
    }

    /// node.delete of this admin's own birth, of which it is the executor (fabric-delete.md step 4 and
    /// "After completion": the fabric-primary's own node.delete, called from outside the fabric; its
    /// node.deleted is its last reply and the caller takes its word). Its stop, its row Deleted and
    /// node.id and name released, node.deleted on the caller's stream
    /// ([`crate::deployment::pipeline::DeploymentPipeline::delete_own`]); then, once every caller of
    /// the Build reads its terminal, this admin ends its own runtime through the provider. No `Exited`
    /// of the runtime is receipted: nobody is left to record it ("Receipts": the one exception).
    async fn delete_own(&self, build_id: &crate::build::BuildId, attempt: u32, record: Node, handle: DeploymentHandle) -> Result<(), crate::executor::RunEnd> {
        use tracing::Instrument;
        let template = self.template_for(record.kind, &record.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle: handle.clone(), unreached: false };
        self.pipeline(&template).delete_own(&req).await.map_err(crate::executor::RunEnd::from)?;
        self.handles.lock().unwrap().remove(&record.name);
        let (drives, provider, id) = (self.drives.clone(), self.provider.clone(), build_id.clone());
        // The callers read the terminal within the node's connections-delete bound, as every in-flight
        // handler finishes within it before the node leaves (RDM_CONNECTIONS_DELETE_DEADLINE_MS).
        let deadline = self.template.connections_delete_deadline;
        let grace = crate::deployment::pipeline::Timeouts::default().stop_grace;
        let span = tracing::info_span!("rdm.node_admin.node.delete.via-own-runtime", node = %record.name, node_id = %record.node_id, build_id = %build_id.log(), attempt, outcome = tracing::field::Empty);
        // Its own task: the run, and the drive pass it runs under, end with the Build's terminal, and
        // only after the callers read it does the runtime end.
        tokio::spawn(
            async move {
                if tokio::time::timeout(deadline, drives.until_read(&id)).await.is_err() {
                    tracing::info!(deadline_ms = deadline.as_millis() as u64, "a caller of the Build had not read its terminal within the connections-delete bound: the runtime ends");
                }
                // Said before the provider is asked: the runtime this admin runs in ends with it.
                tracing::info!("node.deleted was this admin's last reply and its callers read the Build's terminal: its own runtime ends through the provider");
                if let Err(e) = provider.terminate(&handle, crate::deployment::provider::TerminationMode::Graceful { grace }).await {
                    tracing::Span::current().record("outcome", e.to_string().as_str());
                    tracing::info!("the provider refused to end this admin's own runtime");
                }
            }
            .instrument(span),
        );
        Ok(())
    }

    /// The exact birth whose `node.delete:<node>` this Build began: the one its `NodeDeleting` receipt
    /// names. `None` when no attempt of the Build receipted that step.
    async fn begun_delete_birth(&self, build_id: &crate::build::BuildId, node: &PathName) -> Result<Option<Node>, String> {
        let key = format!("node.delete:{node}");
        let build = self.builds.read_build(build_id).await.map_err(|e| format!("{key}: the Build {build_id} could not be read for its receipts: {e}"))?;
        let Some(op) = build.steps.iter().filter(|r| r.operation == key && r.step == "NodeDeleting").find_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<rdm_core::lifecycle::LifecycleOp>(v).ok())) else {
            return Ok(None);
        };
        let mut birth = Node::allocated(node.clone());
        birth.node_id = op.node_id;
        birth.incarnation_id = Some(op.incarnation);
        Ok(Some(birth))
    }

    /// node.delete of the live or stopped exact birth `record` (node-delete.md): the ordinary
    /// workflow (`DeploymentPipeline::retire`: node.stop for a live process, the provider's exact
    /// exit proof, its row Deleted, NodeDeleted) over its exact runtime `handle`; then its paths are
    /// retired and this admin's handle of it dropped. The one composition a removal, a mesh
    /// delete's member and a deleted mesh's last admin all call. Answers the terminal receipt.
    pub(crate) async fn delete_birth(&self, build_id: &crate::build::BuildId, attempt: u32, record: Node, handle: DeploymentHandle) -> Result<crate::deployment::pipeline::TerminalReceipt, crate::executor::RunEnd> {
        let template = self.template_for(record.kind, &record.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, unreached: false };
        let terminal = self.pipeline(&template).delete_node(&req).await.map_err(crate::executor::RunEnd::from)?;
        self.after_exit(&record);
        self.handles.lock().unwrap().remove(&record.name);
        Ok(terminal)
    }
}

#[async_trait::async_trait]
impl crate::seat_revision::PathProbe for AdminRunner {
    /// Node investigation (the one primitive, Q107 item 1) on `node` from this admin's view: the leads,
    /// the direct tickle and the same kick through peers, then node.runtime.inspect over the one runtime
    /// resolution.
    async fn investigate(&self, node: &Node) -> crate::node_investigation::Finding {
        let view = self.topology.read().await.clone();
        let calls = crate::offline::TickleClient { client: self.node_rpc.client.clone(), me_id: self.node_id.clone(), me: self.held_name().to_string(), connections: self.connections.clone(), commands: None };
        let inspect = async { crate::node_investigation::Finding::from(crate::node_investigation::inspect_birth_with_handle(&*self.provider, &self.digest_view, self.records.contacts.get().map(|s| &**s as &dyn crate::storage::NodesStorage), node).await.0) };
        crate::node_investigation::investigate(node, &view, &calls, inspect).await
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
    pub(crate) fn run_op<'a>(&'a self, build_id: &'a crate::build::BuildId, lane: Option<&'a str>, attempt: u32, op: &'a BuildOperation) -> futures_util::future::BoxFuture<'a, Result<(), crate::executor::RunEnd>> {
        // Boxed where it is made: the operation's workflows are one large state machine, and every
        // caller (the executor, a recipe's steps) holds only the box on its worker's stack.
        Box::pin(self.run_op_body(build_id, lane, attempt, op))
    }

    async fn run_op_body(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), crate::executor::RunEnd> {
        use tracing::Instrument;
        let me = self.held_name();
        // The workflows write the attempt's receipts and read its Build through the lane's Build
        // state (R-BI7): a drift attempt's in its mesh's lane, a fabric attempt's in the fabric lane.
        let builds = crate::build_run::LaneBuilds::of(&self.builds, lane);
        if let Some(mesh) = lane {
            // A mesh's drift lane carries a node recovery's attempts only (its open is
            // `ControlPlane::open_recovery`'s): any other operation is not one this lane runs.
            if !matches!(op, BuildOperation::RetireNode { exited: Some(_), .. } | BuildOperation::CreateNode { over: Some(_), .. }) {
                return Err(crate::executor::RunEnd::failed(format!("{me}: {} is not an operation of a node recovery, the only work mesh {mesh}'s drift lane runs (attempt {attempt} of {build_id})", op.key())));
            }
        }
        let ended: Result<(), crate::executor::RunEnd> = match op {
            BuildOperation::CreateMesh { .. } => create_mesh_step(&self.records, &*self.mesh_storage, &*self.builds, &self.template.executor(), &me.to_string(), build_id, attempt, op).await.map_err(Into::into),
            // A replacement's create of a replaced member: over its predecessor's kept data dir, launching
            // the binding the Build fixed (build-deployment.md §3).
            BuildOperation::CreateNode { node, over: Some(from), executable: Some(executable) } => {
                let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id.log(), node = %node, attempt, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str(), sha256 = %executable.sha256);
                self.create_replacement(&*builds, build_id, attempt, node, from, executable).instrument(span).await
            }
            BuildOperation::CreateNode { node, over: Some(from), executable: None } => {
                // node-recovery.md names the create's span `node.create.via-build`.
                let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id.log(), node = %node, attempt, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str());
                self.create_over(&*builds, build_id, attempt, node, from, None).instrument(span).await
            }
            BuildOperation::CreateNode { node, over: None, executable } => self.create_node(build_id, attempt, node, executable.as_ref(), crate::node_rows::ClaimFor::Create).await,
            BuildOperation::RestartNode { node } => {
                // node-restart.md: the live process stops and rejoins; there is no relaunch. A member
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
                self.delete_unreached(&*builds, build_id, attempt, node, from).instrument(span).await.map(|_| ())
            }
            BuildOperation::TerminateNode { node, node_id, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-terminate-returned", build_id = %build_id.log(), node = %node, attempt, node_id = %node_id, incarnation_id = %from_incarnation.as_str());
                self.terminate_returned(&*builds, build_id, attempt, node, node_id, from_incarnation).instrument(span).await
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
            BuildOperation::DeleteFabric { fabric_id } => crate::mesh_delete::complete_fabric_delete(&*builds, &*self.fabric_storage, build_id, attempt, fabric_id, &self.me.to_string()).await.map_err(Into::into),
            BuildOperation::RecipeStep { step, birth } => return self.recipe_step(build_id, lane, attempt, step, birth.as_ref()).await,
            BuildOperation::UpdateConfig { node } | BuildOperation::CreateTags { node, .. } | BuildOperation::DeleteTags { node, .. } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt, operation = %op.key());
                self.apply_in_place(build_id, attempt, node, op).instrument(span).await
            }
        };
        ended
    }

    /// A property the Build applies in place on `node` (build.md §1, "The Build applies the diff"): at
    /// this admin, the node's mesh-primary, through the door a caller's command takes. `UpdateConfig`
    /// puts the entry's config as `node.config.update` does (row first, then the node) and writes the
    /// entry's storage and placement onto the row with it; `CreateTags` / `DeleteTags` put the labels as
    /// `node.tags.create` / `node.tags.delete` do. The node is the birth this admin's view holds at the
    /// path; one the view does not hold fails the operation by name, nothing written.
    async fn apply_in_place(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, op: &BuildOperation) -> Result<(), crate::executor::RunEnd> {
        use rdm_core::status::{StatusReply, StatusRequest, TagScope};
        let me = self.held_name();
        let key = op.key();
        let template = self.template_for(node.kind, &node.mesh).await?;
        let step = match op {
            BuildOperation::UpdateConfig { .. } => "UpdateConfig",
            BuildOperation::CreateTags { .. } => "CreateTags",
            _ => "DeleteTags",
        };
        let put = async {
            let Some(authority) = self.observer.authority.get() else {
                return Err(format!("{key}: {me} holds no status authority yet to write the row of {node}; nothing is written"));
            };
            let held = self.topology.read().await.node(node).and_then(|n| n.incarnation_id.clone().map(|incarnation| (n.node_id.clone(), incarnation)));
            let Some((node_id, incarnation)) = held else {
                return Err(format!("{key}: {me} holds no birth at {node}; nothing is written"));
            };
            let reply = match op {
                BuildOperation::UpdateConfig { .. } => {
                    let stored = self.builds.stored_accepted(build_id).await.map_err(|e| format!("{key}: the accepted Build {build_id} could not be read for {node}'s create spec; nothing is written: {e}"))?;
                    let spec = stored.and_then(|a| a.topology.mesh(&node.mesh).and_then(|m| m.spec(node)).cloned()).ok_or_else(|| format!("{key}: the accepted Build {build_id} names no create spec for {node}; nothing is written"))?;
                    let req = StatusRequest::UpdateNodeConfig { node_id, incarnation, config: spec.config };
                    match authority.update_config_from_spec(&req, spec.storage, spec.placement).await {
                        StatusReply::ConfigUpdated { .. } => return Ok(()),
                        refused => refused,
                    }
                }
                BuildOperation::CreateTags { labels, .. } => match authority.apply(None, &StatusRequest::CreateTags { node_id, incarnation, labels: labels.clone() }).await {
                    StatusReply::TagsCreated { .. } => return Ok(()),
                    refused => refused,
                },
                BuildOperation::DeleteTags { labels, .. } => match authority.apply(None, &StatusRequest::DeleteTags { node_id, incarnation, scope: TagScope::Labels(labels.clone()) }).await {
                    StatusReply::TagsDeleted { .. } => return Ok(()),
                    refused => refused,
                },
                _ => unreachable!("apply_in_place is called for a property applied in place only"),
            };
            Err(format!("{key}: {node} answered {reply:?}"))
        };
        self.pipeline(&template).apply_in_place(build_id, attempt, node, key.clone(), step, put).await.map_err(crate::executor::RunEnd::from)
    }

    /// A Build's node.create of `node` with no exited birth (`CreateNode { over: None }`, and a recipe's
    /// create step, whose claim is a replace successor's), its name claimed for `claim_for`.
    fn create_node<'a>(&'a self, build_id: &'a crate::build::BuildId, attempt: u32, node: &'a PathName, executable: Option<&'a rdm_core::binding::Binding>, claim_for: crate::node_rows::ClaimFor) -> futures_util::future::BoxFuture<'a, Result<(), crate::executor::RunEnd>> {
        // Boxed where it is made, as `run_op`.
        Box::pin(self.create_node_body(build_id, attempt, node, executable, claim_for))
    }

    async fn create_node_body(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, executable: Option<&rdm_core::binding::Binding>, claim_for: crate::node_rows::ClaimFor) -> Result<(), crate::executor::RunEnd> {
        use tracing::Instrument;
        let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id.log(), node = %node, attempt);
        async {
            // A mesh's first admin (a new mesh, or a mesh whose admins were all lost): once it has
            // joined its mesh, the fabric primary applies MeshStatus::Pending at that exact birth,
            // before it is asked to be Ready; only with that certainty does the shape proceed (e4.s11).
            let first_admin = self.topology.read().await.first_admin_of(node);
            let pre_node_ready = if first_admin { Some(self.mesh_state_hook(node, self.held_mesh_state(&node.mesh))?) } else { None };
            self.create(build_id, attempt, node, executable, pre_node_ready, claim_for).await
        }
        .instrument(span)
        .await
    }

    /// One step of a recipe (`BuildChange::Recipe`; lifecycles/node-replace.md): the op it names, run
    /// as that op's own workflow on the exact birth the step acts on, the one it is fenced to, or for a
    /// node an earlier step of the recipe created, the birth this executor holds at the step's path.
    /// node.create is the create workflow, its claim a replace successor's ([`ClaimFor::Successor`]);
    /// node.connections.delete the standalone `{ All }`; node.mesh.leave carried by this admin as the
    /// node's mesh-primary, which holds its `node.mesh.left`; node.stop the standalone stop;
    /// node.name.update its own core at this admin as the node's mesh-primary; node.start the start of
    /// the stopped birth; node.delete node.delete's workflow from the state the birth is in.
    ///
    /// [`ClaimFor::Successor`]: crate::node_rows::ClaimFor::Successor
    pub(crate) fn recipe_step<'a>(&'a self, build_id: &'a crate::build::BuildId, lane: Option<&'a str>, attempt: u32, step: &'a rdm_core::build::RecipeStep, birth: Option<&'a crate::build::PinnedBirth>) -> futures_util::future::BoxFuture<'a, Result<(), crate::executor::RunEnd>> {
        // Boxed where it is made, as `run_op`: a step runs `run_op` again, and neither holds the
        // other's state machine inline.
        Box::pin(self.recipe_step_body(build_id, lane, attempt, step, birth))
    }

    async fn recipe_step_body(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, step: &rdm_core::build::RecipeStep, birth: Option<&crate::build::PinnedBirth>) -> Result<(), crate::executor::RunEnd> {
        use rdm_core::build::RecipeStep as S;
        let me = self.held_name();
        let node = step.node();
        let key = crate::build::recipe_step_key(step);
        if let S::Create { node, executable } = step {
            return self.create_node(build_id, attempt, node, executable.as_ref(), crate::node_rows::ClaimFor::Successor).await;
        }
        let birth = match birth {
            Some(b) => b.clone(),
            None => {
                let seen = self.topology.read().await.node(node).cloned();
                let held = seen.or_else(|| self.handles.lock().unwrap().get(node).map(|(n, _)| n.clone()));
                match held.and_then(|n| n.incarnation_id.clone().map(|incarnation| crate::build::PinnedBirth { node_id: n.node_id.clone(), incarnation })) {
                    Some(b) => b,
                    None => return Err(crate::executor::RunEnd::failed(format!("{key}: the step acts on the node an earlier step of the recipe made at {node}, and {me} holds no birth at {node}"))),
                }
            }
        };
        let from_incarnation = birth.incarnation.clone();
        match step {
            S::Create { .. } => unreachable!("a create returned above"),
            S::Stop { node } => self.run_op(build_id, lane, attempt, &BuildOperation::StopNode { node: node.clone(), from_incarnation }).await,
            S::DeleteConnections { node } => self.run_op(build_id, lane, attempt, &BuildOperation::DeleteConnections { node: node.clone(), from_incarnation }).await,
            S::LeaveMesh { node } => {
                use tracing::Instrument;
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id.log(), node = %node, attempt, node_id = %birth.node_id, incarnation_id = %birth.incarnation.as_str());
                async {
                    let template = self.template_for(node.kind, &node.mesh).await?;
                    let leave = async {
                        let Some(authority) = self.observer.authority.get() else {
                            return Err(format!("{key}: {me} holds no status authority yet to carry node.mesh.leave to {node}; nothing is sent"));
                        };
                        match authority.carry_mesh_leave(&birth.node_id, &birth.incarnation, build_id.as_str(), attempt, &key).await? {
                            rdm_core::status::StatusReply::MeshLeft { .. } => Ok(()),
                            refused => Err(format!("{key}: node.mesh.leave of {node} ({}) answered {refused:?}", birth.node_id)),
                        }
                    };
                    self.pipeline(&template).leave_mesh(build_id, attempt, node, leave).await.map_err(crate::executor::RunEnd::from)
                }
                .instrument(span)
                .await
            }
            S::Start { node } => self.run_op(build_id, lane, attempt, &BuildOperation::StartNode { node: node.clone(), from_incarnation }).await,
            S::Delete { node } => {
                // node.delete runs no stop steps for a stopped or proven-exited birth and stops a live
                // one first (node-delete.md): the birth's state decides, never its name. A birth this
                // view no longer holds (a resumed delete whose NodeDeleting it already heard) is
                // deleted fenced to its exact birth, its runtime read from its row.
                let held = self.topology.read().await.node(node).filter(|n| n.incarnation_id.as_ref() == Some(&from_incarnation)).cloned();
                let op = match held {
                    Some(n) if !n.stopped && n.status != crate::model::NodeStatus::Dead => BuildOperation::RetireNode { node: node.clone(), exited: None },
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
                        let req = rdm_core::status::StatusRequest::UpdateName { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: name.name() };
                        match authority.update_name(&req).await {
                            rdm_core::status::StatusReply::NameUpdated { .. } => Ok(()),
                            refused => Err(format!("{key}: node.name.update of {node} ({}) to {name} answered {refused:?}", birth.node_id)),
                        }
                    };
                    self.pipeline(&template).update_name(build_id, attempt, node, put).await.map_err(crate::executor::RunEnd::from)
                }
                .instrument(span)
                .await
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

/// A binding a request, a Build or a node row names, checked at the launch that runs it: its
/// launch id is the node's kind, and its file is re-hashed now, so the launch proves the bytes it
/// launches are the bound ones. A missing file and a hash that differs are refused by name. The hash
/// reads the whole file, so it runs on the blocking pool, never on a worker of the runtime: a task the
/// launching task woke just before waits in that worker's LIFO slot, which no other worker steals,
/// until the worker is free (Q30).
pub(crate) async fn verify_binding(binding: &rdm_core::binding::Binding, kind: NodeKind, provider: ProviderKind, from: &str) -> Result<(), String> {
    let span = tracing::info_span!("rdm.node_admin.deployment.resolve.via-executable-hash", launch_id = %binding.launch_id, executable = %binding.executable.display(), sha256 = %binding.sha256, from, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
    if binding.launch_id != kind.name() {
        span.record("outcome", "refused");
        return Err(format!("the binding is for launch id `{}`, the node is a `{}`", binding.launch_id, kind.name()));
    }
    if let Err(e) = binding.check_image(super::boot::provider_image(provider)) {
        span.record("outcome", "refused");
        return Err(e.to_string());
    }
    let started = std::time::Instant::now();
    let b = binding.clone();
    let checked = crate::blocking::blocking_io(move || b.verify()).await;
    span.record("elapsed_ms", started.elapsed().as_millis() as u64);
    span.record("outcome", if checked.is_ok() { "hashed" } else { "refused" });
    span.in_scope(|| tracing::info!("the executable's bytes are hashed against the binding the create launches"));
    checked.map(|_| ()).map_err(|e| e.to_string())
}

/// The binding the bootstrap admin's own birth runs, recorded by its Day-0 claim (build-deployment.md
/// §2.3: "the bootstrap admin's own included: it hashes the executable it is running"): its running
/// executable (`current_exe`) hashed now, on the blocking pool; there is no configured sha to hold it to.
pub(crate) async fn running_binding(provider: ProviderKind) -> Result<rdm_core::binding::Binding, String> {
    let exe = std::env::current_exe().map_err(|e| format!("this admin's running executable could not be named: {e}"))?;
    hashed_binding(NodeKind::NodeAdmin, &exe, provider).await
}

/// `executable` as `kind`'s binding: its file hashed now, so the row of the birth that runs it records
/// exactly these bytes, and the image `provider` runs it in (the container provider's runtime image;
/// none under the process provider), so a recovery seeded from that row passes the launch's image
/// check.
async fn hashed_binding(kind: NodeKind, executable: &std::path::Path, provider: ProviderKind) -> Result<rdm_core::binding::Binding, String> {
    let path = executable.to_path_buf();
    let sha256 = crate::blocking::blocking_io(move || rdm_core::binding::sha256_file(&path)).await.map_err(|e| format!("the executable {} could not be hashed: {e}", executable.display()))?;
    let image = match provider {
        ProviderKind::Process => None,
        ProviderKind::Container => Some(crate::deployment::container::RUNTIME_IMAGE.to_string()),
    };
    Ok(rdm_core::binding::Binding { launch_id: kind.name().into(), executable: executable.to_path_buf(), sha256, image })
}

/// The exact birth `from` at `node` whose node.delete in `build_id` already holds its `TerminateRuntime`
/// receipt naming that birth (the provider's exit proof, recorded by an earlier attempt's executor), when
/// this admin cannot resolve its runtime: its record, and a handle of the receipted runtime that locates
/// nothing. The delete resumes from its receipts (mesh-delete.md "Resume after the primary's loss" step 3:
/// "Completed member receipts stand"; `DeploymentPipeline::begin` reuses every step an earlier attempt
/// receipted), so the reused `TerminateRuntime` touches no runtime and the steps after it run. `None` when
/// no such receipt is held.
pub(super) async fn receipted_exit(builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, node: &PathName, from: &crate::build::ExitedBirth) -> Option<(Node, DeploymentHandle)> {
    let operation = format!("node.delete:{node}");
    let terminate = crate::deployment::pipeline::RetireStep::TerminateRuntime.name();
    let p = builds.read_build(build_id).await.ok()?;
    let t = p
        .steps
        .iter()
        .filter(|r| r.operation == operation && r.step == terminate && r.outcome == crate::build_state::StepOutcome::Complete)
        .filter_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<crate::deployment::pipeline::TerminalReceipt>(v).ok()))
        .find(|t| t.node_id == from.node_id.as_str() && t.incarnation_id == from.incarnation.as_str())?;
    let proof = t.runtime.as_ref()?;
    let deployment_id = rdm_core::DeploymentId::parse(&proof.deployment_id).ok()?;
    let provider = match proof.provider.as_str() {
        "container" => ProviderKind::Container,
        _ => ProviderKind::Process,
    };
    let mut record = Node::allocated(node.clone());
    record.node_id = from.node_id.clone();
    record.incarnation_id = Some(from.incarnation.clone());
    record.deployment_id = Some(deployment_id.clone());
    record.data_dir = t.data_dir.clone();
    tracing::info_span!("rdm.node_admin.node.resolve.via-receipted-exit", node = %node, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str(), build_id = %build_id.log(), operation = %operation)
        .in_scope(|| tracing::info!("the birth's runtime is not resolved here and its delete holds the TerminateRuntime receipt: the delete resumes from its receipts"));
    Some((record, DeploymentHandle { deployment_id, provider, pid: None, start: None, container: None, domain: None }))
}

/// Whether attempt `attempt` of `build_id` is fabric recovery's backbone re-create of `node` (its lane's
/// action, or the fabric lane's, is a `Recover` from the backbone, fabric-recovery.md step 3, Q148).
async fn backbone_attempt(builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> bool {
    let Ok(p) = builds.read_build(build_id).await else { return false };
    let backbone = |a: Option<&crate::accepted::AttemptAction>| matches!(a, Some(crate::accepted::AttemptAction::Recover { path, backbone: true, .. }) if path == node);
    backbone(p.lanes.get(&node.mesh).and_then(|l| l.attempt_actions.get(&attempt))) || backbone(p.attempt_actions.get(&attempt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::binding::Binding;

    /// CONTRACT (e12.s91; node-rpc-envelope.md "the executable binding the Build fixes ... verified at
    /// launch"): a binding a recipe step or a row names is checked against the provider that launches it,
    /// as the startup set is. An image under the process provider, and no image or another image under
    /// the container provider, are refused by name before anything is hashed; no image under the process
    /// provider and the runtime image under the container provider pass.
    #[tokio::test]
    async fn a_step_binding_is_refused_by_name_when_its_image_is_not_the_providers() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("step-image-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let exe = dir.join("rdm-rpc-node");
        std::fs::write(&exe, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let sha256 = rdm_core::binding::sha256_file(&exe).unwrap();
        let b = |image: Option<&str>| Binding { launch_id: NodeKind::RpcNode.name().into(), executable: exe.clone(), sha256: sha256.clone(), image: image.map(String::from) };
        let image = crate::deployment::container::RUNTIME_IMAGE;
        assert_eq!(verify_binding(&b(None), NodeKind::RpcNode, ProviderKind::Process, "build").await, Ok(()));
        assert_eq!(verify_binding(&b(Some(image)), NodeKind::RpcNode, ProviderKind::Container, "build").await, Ok(()));
        for (binding, provider) in [(b(Some(image)), ProviderKind::Process), (b(None), ProviderKind::Container), (b(Some("another:image")), ProviderKind::Container)] {
            let want = binding.check_image(super::super::boot::provider_image(provider)).unwrap_err().to_string();
            assert_eq!(verify_binding(&binding, NodeKind::RpcNode, provider, "build").await, Err(want), "{provider:?}");
        }
        // The running binding a row records passes the same check under each provider: a container
        // recovery seeded from that row launches (the release red at dc2b20be7: "the binding names no image").
        for provider in [ProviderKind::Process, ProviderKind::Container] {
            let hashed = hashed_binding(NodeKind::RpcNode, &exe, provider).await.unwrap();
            assert_eq!(verify_binding(&hashed, NodeKind::RpcNode, provider, "row").await, Ok(()), "{provider:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

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
        let binding = Binding { launch_id: NodeKind::RpcNode.name().into(), sha256, executable: exe.clone(), image: None };

        // A task waiting on its channel, as a connection's driver is between packets.
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
            let resolved = verify_binding(&binding, NodeKind::RpcNode, ProviderKind::Process, "build").await;
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
