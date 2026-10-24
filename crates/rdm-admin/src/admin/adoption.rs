//! The adoption launch (Q156; fabric-recovery.md "Nothing alive"): no node-admin is alive, the fabric's
//! other nodes alive or not, and a person or a script launches a node-admin at the lost fabric-primary's
//! path.name with no launcher, given the fabric id, its mesh and the store root. It is told from Day 0 by
//! that intent (`RDM_NODE_NAME`, [`AdminConfig::adopt`]), never by what the store holds.
//!
//! In order, each step before the next: what the store holds is read with nothing written ([`read`]: the
//! Fabric record and its provider, the mesh's row, the lost fabric-primary's birth at the path.name, the
//! current Build), and a store that does not hold them refuses the start by name. The claim of the
//! fabric-recovery attempt of the current Build in the mesh's lane is the launch's first recovery
//! mutation ([`claim`]): the mesh's in-flight row on the store root decides it (build.md §5.2, Q142), the
//! row naming this birth, so two launches at one path.name are two claimants and one loses; the attempt's
//! open and claim are then recorded on the Build journal. Then node investigation of the lost birth, and,
//! last before the first other mutation, whether any node-admin the store records answers ([`check`]): an
//! answer refuses the launch `NameHeld`, the attempt closed through the failure path ([`fail`]). Otherwise
//! the attempt releases the lost birth's name and the launch takes it as itself ([`release_name`]), and
//! once it is ReadyForTraffic the attempt completes ([`complete`]); fabric recovery restores it as
//! fabric-primary (step 4).
//!
//! The launch writes under build.md §5.3's executor exception (Q122): this attempt's Build records, and the
//! mesh and node records its own operations require, never as mesh-primary.

use super::*;
use crate::accepted::AttemptAction;
use crate::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptClaim, BuildAttemptReceipt, BuildProjection, BuildState, BuildStateAdapter, BuildStepReceipt, ClaimOutcome, StepOutcome};
use crate::mesh_claims::{InFlight, Taken};
use crate::node_investigation::Finding;

/// What the store holds that an adoption launch adopts, read with nothing written.
#[derive(Debug, Clone)]
pub struct Adoptable {
    /// The Fabric record.
    pub fabric: crate::fabric_storage::FabricRecord,
    /// The provider the Fabric record names.
    pub provider: ProviderKind,
    /// The mesh's row.
    pub mesh: crate::storage::MeshRecord,
    /// The lost fabric-primary's birth at the launch's path.name, as its node row records it.
    pub lost: crate::storage::NodeRecord,
    /// The current Build.
    pub current: BuildProjection,
}

/// The provider's name as `MESH_SPAWN_TYPE` spells it.
fn provider_name(p: ProviderKind) -> &'static str {
    match p {
        ProviderKind::Process => "process",
        ProviderKind::Container => "container",
    }
}

/// What the store root `store` holds for the adoption launch at `name` of Fabric `fabric_id` with
/// `MESH_SPAWN_TYPE` `spawn_type`: the Fabric record, which must name the provider `spawn_type` names
/// when it names one; the live row of `name`'s mesh; the fabric-primary the node rows record, which must
/// be the birth at `name`; and the current Build. Each missing or differing input refuses by name.
#[allow(clippy::too_many_arguments)]
pub async fn read(
    name: &PathName,
    fabric_id: &FabricId,
    store: &Path,
    spawn_type: Option<&str>,
    fabric: &dyn crate::fabric_storage::FabricStorage,
    meshes: &dyn crate::storage::MeshStorage,
    nodes: &dyn crate::storage::NodesStorage,
    builds: &dyn BuildStateAdapter,
) -> Result<Adoptable, String> {
    let root = store.join(fabric_id.to_string());
    let of = format!("the adoption launch at {name} of Fabric {fabric_id}");
    let record = fabric.fabric().await.map_err(|e| format!("{of}: the Fabric record on the store root {} could not be read: {e}", root.display()))?;
    let Some(record) = record.filter(|r| r.fabric_id == *fabric_id) else {
        return Err(format!("{of}: the store root {} holds no Fabric record of {fabric_id}: an adoption launch adopts a fabric its store root holds and creates none", root.display()));
    };
    let Some(provider) = record.provider else {
        return Err(format!("{of}: the Fabric record on the store root {} names no provider: the adoption launch takes the fabric's provider from it", root.display()));
    };
    match crate::deployment::provider::parse(spawn_type) {
        Ok(Some(given)) if given != provider => {
            return Err(format!(
                "{of}: MESH_SPAWN_TYPE={} and the Fabric record names provider {}: an adoption launch takes the fabric's provider, and a differing MESH_SPAWN_TYPE is refused",
                provider_name(given),
                provider_name(provider)
            ))
        }
        Ok(_) => {}
        Err(e) => return Err(format!("{of}: {e}")),
    }
    let mesh = meshes.mesh_named(&name.mesh).await.map_err(|e| format!("{of}: mesh.storage could not read mesh {}: {e}", name.mesh))?;
    let mesh = match mesh {
        Some(m) if !m.dead => m,
        Some(m) => return Err(format!("{of}: mesh {} ({}) is Dead: a dead mesh is never adopted", name.mesh, m.mesh_id)),
        None => return Err(format!("{of}: the store root {} holds no row of mesh {}", root.display(), name.mesh)),
    };
    // The fabric-primary the node rows record (`is_fabric_primary` on a birth, the newest revision): the
    // lost birth the launch is at the path.name of.
    let rows = crate::storage::records(meshes, nodes, &name.mesh).await.map_err(|e| format!("{of}: nodes.storage could not read the node rows: {e}"))?;
    let recorded = rows.iter().filter(|r| r.status != NodeStatus::Deleted).filter_map(|r| r.newest_birth().and_then(|b| b.is_fabric_primary.map(|rev| (rev, r, b)))).max_by_key(|(rev, _, _)| *rev);
    let Some((_, row, birth)) = recorded else {
        return Err(format!("{of}: the node rows on the store root {} record no fabric-primary: an adoption launch is at the lost fabric-primary's path.name", root.display()));
    };
    if row.name != *name {
        return Err(format!("{of}: the node rows record the fabric-primary at {} ({}): an adoption launch is at the lost fabric-primary's path.name", row.name, row.node_id));
    }
    let lost = crate::storage::NodeRecord {
        node_id: row.node_id.clone(),
        fabric_id: fabric_id.clone(),
        name: row.name.clone(),
        mesh_id: row.mesh_id.clone().unwrap_or_else(|| mesh.mesh_id.clone()),
        incarnation_id: birth.incarnation_id.clone(),
        endpoint_id: birth.endpoint_id.clone().unwrap_or_else(|| EndpointId(String::new())),
        transport_addr: birth.transport_addr.unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0))),
        listeners: birth.listeners.clone(),
        is_primary: birth.is_primary,
        is_fabric_primary: birth.is_fabric_primary,
    };
    let current = builds.current().await.map_err(|e| format!("{of}: the Build journal on the store root could not be read: {e}"))?;
    let Some(current) = current else {
        return Err(format!("{of}: the Build journal on the store root {} holds no current Build", root.display()));
    };
    Ok(Adoptable { fabric: record, provider, mesh, lost, current })
}

/// The fabric-recovery attempt an adoption launch claimed: the mesh's in-flight row it holds, and the
/// lost birth it recovers.
#[derive(Debug, Clone)]
pub struct Claimed {
    /// The mesh's in-flight row, naming this birth.
    pub row: InFlight,
    /// The lost fabric-primary's birth.
    pub lost: crate::storage::NodeRecord,
}

impl Claimed {
    fn mesh(&self) -> &str {
        &self.lost.name.mesh
    }
}

/// The action the fabric-recovery attempt of `lost` carries.
fn action_of(lost: &crate::storage::NodeRecord) -> AttemptAction {
    AttemptAction::Recover { path: lost.name.clone(), node_id: lost.node_id.clone(), from_incarnation: lost.incarnation_id.clone(), backbone: false }
}

/// The attempt of `current` in `mesh`'s lane the recovery `action` resolves to: the lane's attempt
/// opened or claimed with that very action while it is in progress, else the lane's next.
fn attempt_for(current: &BuildProjection, mesh: &str, action: &AttemptAction) -> u32 {
    match current.lanes.get(mesh) {
        Some(l) if l.state == BuildState::Pending && l.opened == l.attempt + 1 && l.action.as_ref() == Some(action) => l.opened,
        Some(l) if l.state == BuildState::Running && l.attempt_actions.get(&l.attempt) == Some(action) => l.attempt,
        Some(l) => l.attempt + 1,
        None => 1,
    }
}

/// Claim the fabric-recovery attempt of `a`'s current Build for the lost birth, in its mesh's lane, as
/// `me` born `me_id`, before any other recovery mutation: the mesh's in-flight row on the store root
/// decides (insert-and-fail, naming this birth); a row naming the same attempt and another claimant is a
/// lost claim, refused by name naming that claimant; a row of another attempt refuses by name naming it.
/// Won, the attempt's open and claim are recorded on the journal. A lost birth a workflow already
/// recovered refuses by name. One `rdm.node_admin.fabric.update.via-adoption-claim` span.
pub async fn claim(me: &PathName, me_id: &NodeId, a: &Adoptable, builds: &dyn BuildStateAdapter, claims: &dyn crate::storage::MeshStorage, contexts: &crate::build_claim::AttemptContexts, now_ms: u64) -> Result<Claimed, String> {
    let lost = &a.lost;
    let mesh = lost.name.mesh.clone();
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.update.via-adoption-claim",
        node = %me,
        node_id = %me_id,
        lost_node_id = %lost.node_id,
        lost_incarnation_id = %lost.incarnation_id.as_str(),
        build_id = %a.current.build_id.log(),
        lane = %mesh,
        attempt = tracing::field::Empty,
        outcome = tracing::field::Empty,
        held_by = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let decided = async {
        if let Some((build_id, _, attempt, operation)) = builds.recovery_of(&lost.name, &lost.node_id, &lost.incarnation_id).await.map_err(|e| ("unreadable", format!("the Build journal could not be read: {e}")))? {
            return Err(("already-recovered", format!("the lost birth {} ({}) at {} was recovered already, by {operation} at attempt {attempt} of {build_id}: there is nothing to adopt", lost.node_id, lost.incarnation_id.as_str(), lost.name)));
        }
        let action = action_of(lost);
        let attempt = attempt_for(&a.current, &mesh, &action);
        tracing::Span::current().record("attempt", attempt);
        let row = InFlight { build_id: a.current.build_id.clone(), lane: Some(mesh.clone()), attempt, claimant: me.to_string(), claimant_node: Some(me_id.clone()) };
        match claims.take_in_flight(&mesh, &row).await {
            Ok(Taken::Won) => {}
            Ok(Taken::Held(held)) if held.same_attempt(&row) => {
                tracing::Span::current().record("held_by", held.claimant_named().as_str());
                return Err((
                    "lost",
                    format!("the fabric-recovery attempt {attempt} of {} in mesh {mesh}'s lane for the lost birth {} was claimed by {}: this launch's claim lost", row.build_id, lost.node_id, held.claimant_named()),
                ));
            }
            Ok(Taken::Held(held)) => {
                tracing::Span::current().record("held_by", held.claimant_named().as_str());
                return Err((
                    "build-in-progress",
                    format!("mesh {mesh} is held by attempt {} of {} ({} lane), claimed by {}: one attempt in flight per mesh", held.attempt, held.build_id, held.lane.as_deref().unwrap_or("fabric"), held.claimant_named()),
                ));
            }
            Err(e) => return Err(("unreadable", format!("mesh {mesh}'s in-flight row on the store root could not be taken: {e}"))),
        }
        // The claim stands on the store: its records follow on the Build journal. A record refused
        // releases the row taken for it.
        let recorded = async {
            contexts.put_lane(&row.build_id, Some(&mesh), attempt, &crate::build_claim::current_context()).await.map_err(|e| format!("attempt-context: {e}"))?;
            let opened = AttemptOpened { build_id: row.build_id.clone(), attempt, reason: AttemptReason::Recovery, action: Some(action), opened_by: me.to_string(), opened_at_ms: now_ms, mesh: Some(mesh.clone()) };
            builds.open_attempt(&opened).await.map_err(|e| format!("attempt {attempt} of {} in mesh {mesh}'s lane could not be opened: {e}", row.build_id))?;
            let claim = BuildAttemptClaim { build_id: row.build_id.clone(), attempt, executor: me.to_string(), claimant: Some(me.to_string()), mesh: Some(mesh.clone()) };
            match builds.claim_attempt(&claim).await.map_err(|e| format!("the claim of attempt {attempt} could not be recorded: {e}"))? {
                ClaimOutcome::Won => Ok(()),
                other => Err(format!("the Build journal answers the claim of attempt {attempt} of {} {other:?}", row.build_id)),
            }
        };
        if let Err(e) = recorded.await {
            let _ = claims.release_in_flight(&mesh, &row).await;
            return Err(("unrecorded", e));
        }
        Ok(Claimed { row, lost: lost.clone() })
    }
    .instrument(span.clone())
    .await;
    match &decided {
        Ok(_) => span.record("outcome", "won"),
        Err((outcome, _)) => span.record("outcome", *outcome),
    };
    span.in_scope(|| tracing::info!("the adoption launch's claim of the fabric-recovery attempt is decided on the store root"));
    decided.map_err(|(_, e)| format!("{me} ({me_id}): {e}"))
}

/// Who answers an adoption launch before its first mutation (fabric-recovery.md "Nothing alive").
#[async_trait::async_trait]
pub trait Answers: Send + Sync {
    /// Node investigation of the exact lost birth `lost`: its verdict.
    async fn investigate(&self, lost: &crate::storage::NodeRecord) -> Finding;
    /// The node-admins of the fabric the store records, the lost birth among them, that answer a probe
    /// now, by path.name.
    async fn admins_answering(&self) -> Vec<String>;
}

/// The checks before the claimed attempt's first mutation: node investigation of the lost birth, then,
/// last, whether any node-admin the store records answers. Reached, or an admin answering, refuses
/// `NameHeld` naming who answered; otherwise the investigation's verdict. One
/// `rdm.node_admin.fabric.update.via-adoption-check` span.
pub async fn check(c: &Claimed, answers: &dyn Answers) -> Result<Finding, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.update.via-adoption-check",
        node = %c.row.claimant,
        lost_node_id = %c.lost.node_id,
        lost_incarnation_id = %c.lost.incarnation_id.as_str(),
        build_id = %c.row.build_id.log(),
        attempt = c.row.attempt,
        verdict = tracing::field::Empty,
        answered = tracing::field::Empty,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let decided = async {
        let finding = answers.investigate(&c.lost).await;
        tracing::Span::current().record("verdict", finding.name());
        if let Finding::Reached { .. } = finding {
            tracing::Span::current().record("answered", c.lost.name.to_string().as_str());
            return Err(format!("NameHeld: the lost birth {} ({}) at {} answered node investigation: it is alive, and {} is held by it", c.lost.node_id, c.lost.incarnation_id.as_str(), c.lost.name, c.lost.name));
        }
        let answering = answers.admins_answering().await;
        if !answering.is_empty() {
            tracing::Span::current().record("answered", answering.join(",").as_str());
            return Err(format!("NameHeld: node-admin {} answered before this launch's first mutation: the fabric has a live admin, and {} is held by {}", answering.join(", "), c.lost.name, c.lost.node_id));
        }
        Ok(finding)
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", if decided.is_ok() { "clear" } else { "name-held" });
    span.in_scope(|| tracing::info!("the adoption launch's checks before its first mutation"));
    decided
}

/// Close the claimed attempt through the failure path: its verdict `Failed` with NameHeld's code,
/// `reason` named, then the mesh's in-flight row released. A recovery attempt refused because the birth
/// or an admin answered owes no exit (`build_state::BirthIndex::exits_owed`).
pub async fn fail(c: &Claimed, builds: &dyn BuildStateAdapter, claims: &dyn crate::storage::MeshStorage, reason: &str) -> Result<(), String> {
    let span = tracing::info_span!("rdm.node_admin.build.update.via-adoption-failed", node = %c.row.claimant, build_id = %c.row.build_id.log(), lane = %c.mesh(), attempt = c.row.attempt, reason, outcome = tracing::field::Empty);
    let verdict = BuildAttemptReceipt { build_id: c.row.build_id.clone(), attempt: c.row.attempt, outcome: AttemptOutcome::Failed { reason: reason.to_string(), code: rdm_core::outcome::Code::AlreadyExists }, mesh: Some(c.mesh().to_string()) };
    let r = async {
        builds.append_attempt_receipt(&verdict).await.map_err(|e| format!("the Failed verdict of attempt {} could not be written: {e}", c.row.attempt))?;
        claims.release_in_flight(c.mesh(), &c.row).await.map_err(|e| format!("mesh {}'s in-flight row could not be released: {e}", c.mesh()))
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", if r.is_ok() { "closed" } else { "unwritten" });
    span.in_scope(|| tracing::info!("the adoption launch's attempt is closed through the failure path"));
    r
}

/// The claimed attempt releases the lost birth's name and the launch, `me_id`, takes it
/// ([`crate::node_rows::take_inherited_name`]: the lost birth's row written `Deleted`, then this node's
/// claim of the path.name), then the deletion's receipt (`node.delete:<path>` `NodeDeleted`, naming the
/// exact birth) on the journal.
pub async fn release_name(c: &Claimed, me_id: &NodeId, nodes: &dyn crate::storage::NodesStorage, written: &crate::node_rows::WrittenRows, builds: &dyn BuildStateAdapter) -> Result<(), String> {
    crate::node_rows::take_inherited_name(nodes, written, &c.lost, me_id).await.map_err(|e| format!("{me_id} takes {} over the lost birth {}: {e}", c.lost.name, c.lost.node_id))?;
    let receipt = BuildStepReceipt {
        build_id: c.row.build_id.clone(),
        attempt: c.row.attempt,
        operation: format!("node.delete:{}", c.lost.name),
        step: "NodeDeleted".into(),
        outcome: StepOutcome::Complete,
        output: Some(serde_json::json!({ "node_id": c.lost.node_id, "incarnation": c.lost.incarnation_id })),
        executor: None,
        mesh: Some(c.mesh().to_string()),
    };
    builds.append_step_receipt(&receipt).await.map_err(|e| format!("the NodeDeleted receipt of the lost birth {} could not be written: {e}", c.lost.node_id))
}

/// The claimed attempt completes once the launch, `me_id`, is ReadyForTraffic at the path.name: the
/// re-create's `node.create:<path>` `Complete` receipt, the attempt's `Converged` verdict, then the
/// mesh's in-flight row released.
pub async fn complete(c: &Claimed, me_id: &NodeId, builds: &dyn BuildStateAdapter, claims: &dyn crate::storage::MeshStorage) -> Result<(), String> {
    let span = tracing::info_span!("rdm.node_admin.build.update.via-adoption-complete", node = %c.row.claimant, node_id = %me_id, build_id = %c.row.build_id.log(), lane = %c.mesh(), attempt = c.row.attempt, outcome = tracing::field::Empty);
    let r = async {
        let receipt = BuildStepReceipt {
            build_id: c.row.build_id.clone(),
            attempt: c.row.attempt,
            operation: format!("node.create:{}", c.lost.name),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: Some(serde_json::json!({ "node_id": me_id })),
            executor: None,
            mesh: Some(c.mesh().to_string()),
        };
        builds.append_step_receipt(&receipt).await.map_err(|e| format!("the re-create's Complete receipt could not be written: {e}"))?;
        let verdict = BuildAttemptReceipt { build_id: c.row.build_id.clone(), attempt: c.row.attempt, outcome: AttemptOutcome::Converged, mesh: Some(c.mesh().to_string()) };
        builds.append_attempt_receipt(&verdict).await.map_err(|e| format!("the Converged verdict could not be written: {e}"))?;
        claims.release_in_flight(c.mesh(), &c.row).await.map_err(|e| format!("mesh {}'s in-flight row could not be released: {e}", c.mesh()))
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", if r.is_ok() { "converged" } else { "unwritten" });
    span.in_scope(|| tracing::info!("the adoption launch is ReadyForTraffic: its fabric-recovery attempt completes"));
    r
}

/// [`Answers`] over this process's Node RPC client: node investigation of the lost birth direct (no view
/// holds carriers yet) and then the provider's inspection of its recorded runtime; the probe of every
/// node-admin birth the store's live rows record, at once.
pub struct ProbeAnswers {
    /// This launch's path.name.
    pub me: PathName,
    /// This launch's node id.
    pub me_id: NodeId,
    /// The process's Node RPC client.
    pub client: Arc<rdm_transport::NodeRpcClient>,
    /// The process's live resolver: each probed birth is applied to it first.
    pub resolver: Arc<rdm_transport::LiveNodeResolver>,
    /// The fabric's provider, prepared.
    pub provider: Arc<dyn crate::deployment::provider::DeploymentProvider>,
    /// The fabric's nodes and mesh stores.
    pub nodes: Arc<dyn crate::storage::NodesStorage>,
    pub meshes: Arc<dyn crate::storage::MeshStorage>,
    /// The fabric, as the empty view names it.
    pub fabric: crate::model::Fabric,
}

impl ProbeAnswers {
    fn resolve(&self, node_id: &NodeId, name: &PathName, endpoint: &EndpointId, addr: SocketAddr, incarnation: &IncarnationId) -> bool {
        let Ok(endpoint_id) = endpoint.0.parse::<iroh::PublicKey>() else { return false };
        self.resolver.apply(rdm_transport::ResolvedNode { node_id: node_id.clone(), name: name.clone(), endpoint_id, transport_addr: addr, incarnation: incarnation.clone() }, None);
        true
    }

    fn calls(&self) -> crate::offline::TickleClient {
        crate::offline::TickleClient { client: self.client.clone(), me_id: self.me_id.clone(), me: self.me.to_string(), connections: None }
    }
}

#[async_trait::async_trait]
impl Answers for ProbeAnswers {
    async fn investigate(&self, lost: &crate::storage::NodeRecord) -> Finding {
        let mut node = Node::allocated(lost.name.clone());
        node.node_id = lost.node_id.clone();
        node.incarnation_id = Some(lost.incarnation_id.clone());
        let view = Topology { fabric: self.fabric.clone(), meshes: Vec::new(), nodes: Vec::new() };
        let inspect = async {
            let row = self.nodes.runtime(&lost.node_id, &lost.incarnation_id).await.ok().flatten();
            Finding::from(super::rows_writer::inspection_of(&*self.provider, &None, row.as_ref()).await)
        };
        if !self.resolve(&lost.node_id, &lost.name, &lost.endpoint_id, lost.transport_addr, &lost.incarnation_id) {
            return inspect.await;
        }
        crate::node_investigation::investigate(&node, &view, &self.calls(), inspect).await
    }

    async fn admins_answering(&self) -> Vec<String> {
        let rows = match crate::storage::live_records(&*self.meshes, &*self.nodes, &self.me.mesh).await {
            Ok(rows) => rows,
            Err(e) => return vec![format!("(the node rows could not be read: {e})")],
        };
        let calls = self.calls();
        let probes = rows.iter().filter(|r| r.name.kind == NodeKind::NodeAdmin && r.node_id != self.me_id).filter_map(|r| {
            let b = r.newest_birth()?;
            let (endpoint, addr) = (b.endpoint_id.clone()?, b.transport_addr?);
            self.resolve(&r.node_id, &r.name, &endpoint, addr, &b.incarnation_id).then(|| {
                let mut node = Node::allocated(r.name.clone());
                node.node_id = r.node_id.clone();
                node.incarnation_id = Some(b.incarnation_id.clone());
                let probe = rdm_core::status::StatusRequest::ProbeNodeState { node_id: r.node_id.clone(), incarnation: b.incarnation_id.clone() };
                let calls = &calls;
                async move {
                    let end = crate::offline::TickleCalls::direct(calls, &node, &probe).await;
                    matches!(end, crate::offline::ProbeEnd::Reply(_)).then(|| node.name.to_string())
                }
            })
        });
        futures_util::future::join_all(probes).await.into_iter().flatten().collect()
    }
}
