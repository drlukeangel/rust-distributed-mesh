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
//! once it is Ready the attempt completes ([`complete`]); fabric recovery restores it as
//! fabric-primary (step 4).
//!
//! The launch writes under build.md §5.3's executor exception (Q122): this attempt's Build records, and the
//! mesh and node records its own operations require, never as mesh-primary.
//!
//! Under an in-force fabric.delete (Q159; "Nothing alive", its delete bullet) the launch is created at a
//! new path.name of the node-admin type in an existing, not-yet-deleted mesh, never at the lost
//! fabric-primary's, which the delete removes: the one create a delete admits. Its claim is of the delete's
//! next attempt; it releases and re-creates nothing of the lost birth, whose row the teardown deletes; it
//! claims its own path.name ([`take_new_name`]) and its attempt is the delete's own, which the teardown
//! ends, never completed at its Ready. With no not-yet-deleted mesh left the launch is refused by name:
//! the remaining work is the dead fabric-primary's own node.delete from outside, and nothing is created.

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
    /// The fabric.delete in force (Q159): the launch is then at a new path.name and claims the delete's
    /// next attempt. `None` when no fabric.delete is in force.
    pub delete: Option<BuildProjection>,
}

/// The meshes of `meshes` no fabric.delete has deleted: not Dead, and no `MeshDeleted` receipt of their
/// mesh.delete in `delete`.
fn not_yet_deleted(meshes: &[crate::storage::MeshRecord], delete: &BuildProjection) -> Vec<String> {
    let deleted = |m: &crate::storage::MeshRecord| {
        let op = crate::mesh_delete::operation(&m.mesh_id);
        delete.steps.iter().any(|r| r.operation == op && r.step == crate::mesh_delete::STEP_MESH_DELETED)
    };
    let mut out: Vec<String> = meshes.iter().filter(|m| !m.dead && !deleted(m)).map(|m| m.name.clone()).collect();
    out.sort();
    out
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
    let delete = crate::build_state::fabric_delete_in_force(builds).await.map_err(|e| format!("{of}: the Build journal on the store root could not be read for a fabric.delete in force: {e}"))?.map(|(d, _)| d);
    // Under a delete, the launch is created in an existing, not-yet-deleted mesh; with none left, nothing
    // is created (Q159).
    if let Some(d) = &delete {
        let all = meshes.meshes().await.map_err(|e| format!("{of}: mesh.storage could not read the meshes: {e}"))?;
        let left = not_yet_deleted(&all, d);
        if left.is_empty() {
            return Err(format!(
                "{of}: fabric.delete {} is in force and no not-yet-deleted mesh remains: the remaining work is the dead fabric-primary's own node.delete from outside, and nothing is created (Q159)",
                d.build_id
            ));
        }
        if !left.contains(&name.mesh) {
            return Err(format!("{of}: fabric.delete {} is in force and mesh {} is deleted: the adoption launch is created in an existing, not-yet-deleted mesh, one of {} (Q159)", d.build_id, name.mesh, left.join(", ")));
        }
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
    match &delete {
        None if row.name != *name => {
            return Err(format!("{of}: the node rows record the fabric-primary at {} ({}): an adoption launch is at the lost fabric-primary's path.name", row.name, row.node_id));
        }
        None => {}
        Some(d) if row.name == *name => {
            return Err(format!(
                "{of}: fabric.delete {} is in force and {name} is the lost fabric-primary's path.name, which the delete removes: under a delete the adoption launch is created at a new path.name of the node-admin type in an existing, not-yet-deleted mesh (Q159)",
                d.build_id
            ));
        }
        Some(d) => {
            if let Some(held) = rows.iter().find(|r| r.name == *name && r.status != NodeStatus::Deleted) {
                return Err(format!("{of}: fabric.delete {} is in force and {name} is held by {}: the adoption launch under a delete is created at a new path.name (Q159)", d.build_id, held.node_id));
            }
        }
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
        topology_storage_provider: None,
    };
    let current = match &delete {
        Some(d) => Some(d.clone()),
        None => builds.current().await.map_err(|e| format!("{of}: the Build journal on the store root could not be read: {e}"))?,
    };
    let Some(current) = current else {
        return Err(format!("{of}: the Build journal on the store root {} holds no current Build", root.display()));
    };
    Ok(Adoptable { fabric: record, provider, mesh, lost, current, delete })
}

/// The fabric-recovery attempt an adoption launch claimed: the mesh's in-flight row it holds, and the
/// lost birth it recovers.
#[derive(Debug, Clone)]
pub struct Claimed {
    /// The mesh's in-flight row, naming this birth.
    pub row: InFlight,
    /// The lost fabric-primary's birth.
    pub lost: crate::storage::NodeRecord,
    /// Under an in-force fabric.delete (Q159): the launch's own new path.name. `None` for the adoption at
    /// the lost birth's path.name.
    pub under_delete: Option<PathName>,
}

impl Claimed {
    /// The mesh whose in-flight row the claim holds.
    fn mesh(&self) -> &str {
        &self.lost.name.mesh
    }

    /// The Build lane the claimed attempt is numbered in: the fabric lane under a delete, else the mesh's.
    fn numbered_in(&self) -> Option<String> {
        self.row.lane.clone()
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
    if let Some(delete) = &a.delete {
        return claim_under_delete(me, me_id, a, delete, builds, claims, contexts).await;
    }
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
            let opened = AttemptOpened { build_id: row.build_id.clone(), attempt, reason: AttemptReason::Adoption, action: Some(action), opened_by: me.to_string(), opened_at_ms: now_ms, mesh: Some(mesh.clone()), context: None };
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
        Ok(Claimed { row, lost: lost.clone(), under_delete: None })
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

/// The claim of the in-force fabric.delete `delete`'s next attempt (Q159: "It claims the delete's next
/// attempt") by the launch `me`, born `me_id`, at its new path.name: the attempt the lost fabric-primary
/// claimed (the delete's current, in the fabric lane) holds the lost executor's mesh's in-flight row,
/// which this claim supersedes and so releases (a removal that takes the row only while it names that
/// attempt); then the lost fabric-primary's mesh's in-flight row is taken for the next attempt, naming
/// this birth, the one row every contender for the attempt takes (Q160), so two launches, or a launch and
/// an executor, are two claimants and one loses by name; won, the claim is recorded on the journal in the
/// fabric lane, `me` its executor. One `rdm.node_admin.fabric.update.via-adoption-claim` span, `lane`
/// `fabric`.
async fn claim_under_delete(me: &PathName, me_id: &NodeId, a: &Adoptable, delete: &BuildProjection, builds: &dyn BuildStateAdapter, claims: &dyn crate::storage::MeshStorage, contexts: &crate::build_claim::AttemptContexts) -> Result<Claimed, String> {
    let lost = &a.lost;
    let mesh = lost.name.mesh.clone();
    let attempt = delete.attempt + 1;
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.update.via-adoption-claim",
        node = %me,
        node_id = %me_id,
        lost_node_id = %lost.node_id,
        lost_incarnation_id = %lost.incarnation_id.as_str(),
        build_id = %delete.build_id.log(),
        lane = "fabric",
        attempt,
        under = "fabric-delete",
        outcome = tracing::field::Empty,
        held_by = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let decided = async {
        // The lost fabric-primary's attempt holds its executor's mesh: this claim supersedes it.
        let lost_executor = delete.executor.as_deref().and_then(|e| e.parse::<PathName>().ok()).filter(|e| *e == lost.name);
        if let Some(executor) = lost_executor.filter(|_| delete.state == BuildState::Running) {
            let held = claims.in_flight(&executor.mesh).await.map_err(|e| ("unreadable", format!("mesh {}'s in-flight row could not be read: {e}", executor.mesh)))?;
            if let Some(held) = held.filter(|h| h.build_id == delete.build_id && h.lane.is_none() && h.attempt == delete.attempt && h.claimant == lost.name.to_string()) {
                claims.release_in_flight(&executor.mesh, &held).await.map_err(|e| ("unreleased", format!("mesh {}'s in-flight row of the lost fabric-primary's attempt {} could not be released: {e}", executor.mesh, delete.attempt)))?;
            }
        }
        let row = InFlight { build_id: delete.build_id.clone(), lane: None, attempt, claimant: me.to_string(), claimant_node: Some(me_id.clone()) };
        match claims.take_in_flight(&mesh, &row).await {
            Ok(Taken::Won) => {}
            Ok(Taken::Held(held)) if held.same_attempt(&row) => {
                tracing::Span::current().record("held_by", held.claimant_named().as_str());
                return Err(("lost", format!("attempt {attempt} of fabric.delete {} was claimed by {}: this launch's claim lost", row.build_id, held.claimant_named())));
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
        let recorded = async {
            contexts.put_lane(&row.build_id, None, attempt, &crate::build_claim::current_context()).await.map_err(|e| format!("attempt-context: {e}"))?;
            let claim = BuildAttemptClaim { build_id: row.build_id.clone(), attempt, executor: me.to_string(), claimant: Some(me.to_string()), mesh: None };
            match builds.claim_attempt(&claim).await.map_err(|e| format!("the claim of attempt {attempt} could not be recorded: {e}"))? {
                ClaimOutcome::Won => Ok(()),
                other => Err(format!("the Build journal answers the claim of attempt {attempt} of {} {other:?}", row.build_id)),
            }
        };
        if let Err(e) = recorded.await {
            let _ = claims.release_in_flight(&mesh, &row).await;
            return Err(("unrecorded", e));
        }
        Ok(Claimed { row, lost: lost.clone(), under_delete: Some(me.clone()) })
    }
    .instrument(span.clone())
    .await;
    match &decided {
        Ok(_) => span.record("outcome", "won"),
        Err((outcome, _)) => span.record("outcome", *outcome),
    };
    span.in_scope(|| tracing::info!("the adoption launch's claim of the in-force fabric.delete's next attempt is decided on the store root"));
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
pub async fn check(c: &Claimed, answers: &dyn Answers) -> Result<Finding, Refused> {
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
            return Err(Refused { reason: format!("NameHeld: the lost birth {} ({}) at {} answered node investigation: it is alive, and {} is held by it", c.lost.node_id, c.lost.incarnation_id.as_str(), c.lost.name, c.lost.name), answered: c.lost.name.to_string() });
        }
        if let Some(refused) = admin_answered(&c.lost, answers).await {
            tracing::Span::current().record("answered", refused.answered.as_str());
            return Err(refused);
        }
        Ok(finding)
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", if decided.is_ok() { "clear" } else { "name-held" });
    span.in_scope(|| tracing::info!("the adoption launch's checks before its first mutation"));
    decided
}

/// The last check before a launch's first mutation, the adoption launch's and the launch on an
/// inherited data dir's alike (Q156: "a launch that finds a living admin is refused, never an ordinary
/// admin at the primary's name"; Q199): whether any node-admin the store records answers a probe now.
/// `Some` refusal, `NameHeld` naming who answered, when one does: the fabric has a live admin, and the
/// lost birth `lost`'s path.name is its fabric recovery's to re-create.
pub async fn admin_answered(lost: &crate::storage::NodeRecord, answers: &dyn Answers) -> Option<Refused> {
    let answering = answers.admins_answering().await;
    let first = answering.first()?.clone();
    Some(Refused { reason: format!("NameHeld: node-admin {} answered before this launch's first mutation: the fabric has a live admin, and {} is held by {}", answering.join(", "), lost.name, lost.node_id), answered: first })
}

/// Why the checks refused the launch, and the admin that answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    /// The refusal, `NameHeld` by name.
    pub reason: String,
    /// The path.name of the admin, or of the lost birth, that answered.
    pub answered: String,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

/// Close the claimed attempt through the failure path: its verdict `Failed` with NameHeld's code,
/// `reason` named, then the mesh's in-flight row released. An adoption attempt abandoned because the
/// birth or an admin answered owes no exit (`build_state::BirthIndex::exits_owed`, node-recovery.md
/// "Failure and receipts"). Under an in-force fabric.delete (Q159) the claimed attempt is the delete's own:
/// it is handed off to the admin that answered (`HandedOff { to }`), whose executor continues the delete,
/// and the delete is never failed by the abandoned launch.
pub async fn fail(c: &Claimed, builds: &dyn BuildStateAdapter, claims: &dyn crate::storage::MeshStorage, refused: &Refused) -> Result<(), String> {
    let reason = refused.reason.as_str();
    let span = tracing::info_span!("rdm.node_admin.build.update.via-adoption-failed", node = %c.row.claimant, build_id = %c.row.build_id.log(), lane = c.numbered_in().as_deref().unwrap_or("fabric"), attempt = c.row.attempt, reason, outcome = tracing::field::Empty);
    let outcome = match &c.under_delete {
        Some(_) => AttemptOutcome::HandedOff { to: refused.answered.clone() },
        None => AttemptOutcome::Failed { reason: reason.to_string(), code: rdm_core::outcome::Code::AlreadyExists },
    };
    let verdict = BuildAttemptReceipt { build_id: c.row.build_id.clone(), attempt: c.row.attempt, outcome, mesh: c.numbered_in() };
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

/// Under an in-force fabric.delete (Q159), the launch `me_id` claims its own new path.name `me`, the one
/// create a delete admits ([`crate::node_rows::claim_adopted_name`]): nothing of the lost birth is written,
/// its row deleted by the teardown.
pub async fn take_new_name(me: &PathName, me_id: &NodeId, nodes: &dyn crate::storage::NodesStorage, written: &crate::node_rows::WrittenRows) -> Result<(), String> {
    crate::node_rows::claim_adopted_name(nodes, written, me, me_id).await.map(|_| ()).map_err(|e| format!("{me_id} claims its path.name {me}: {e}"))
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

/// The claimed attempt completes once the launch, `me_id`, is Ready at the path.name: the
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
    span.in_scope(|| tracing::info!("the adoption launch is Ready: its fabric-recovery attempt completes"));
    r
}

/// [`Answers`] over this process's Node RPC client: node investigation of the lost birth direct (no view
/// holds carriers yet), the provider's inspection of its recorded runtime first (Q273); the probe of every
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
    /// The inherited birth a launch on its data dir is over, never asked: it holds this launch's own
    /// endpoint key, so it is no other node. `None` for the adoption launch, which asks the lost birth too.
    pub inherited: Option<NodeId>,
}

impl ProbeAnswers {
    fn resolve(&self, node_id: &NodeId, name: &PathName, endpoint: &EndpointId, addr: SocketAddr, incarnation: &IncarnationId) -> bool {
        let Ok(endpoint_id) = endpoint.0.parse::<iroh::PublicKey>() else { return false };
        self.resolver.apply(rdm_transport::ResolvedNode { node_id: node_id.clone(), name: name.clone(), endpoint_id, transport_addr: addr, incarnation: incarnation.clone() }, None);
        true
    }

    fn calls(&self) -> crate::offline::TickleClient {
        crate::offline::TickleClient { client: self.client.clone(), me_id: self.me_id.clone(), me: self.me.to_string(), connections: None, commands: None }
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
        let probes = rows.iter().filter(|r| r.name.kind == NodeKind::NodeAdmin && r.node_id != self.me_id && self.inherited.as_ref() != Some(&r.node_id)).filter_map(|r| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::FabricTopology;
    use crate::build::BuildId;
    use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
    use crate::fabric_storage::{FabricIdentity, FabricRecord, FabricStorage, MemoryFabricStorage};
    use crate::node_rows::{WriteRecord as _, WrittenRows};
    use crate::storage::{MemoryMeshStorage, MemoryNodesStorage, MeshRecord, MeshStorage as _, NodeRow};
    use std::path::PathBuf;

    struct StubAnswers {
        finding: Finding,
        answering: Vec<String>,
    }

    #[async_trait::async_trait]
    impl Answers for StubAnswers {
        async fn investigate(&self, _lost: &crate::storage::NodeRecord) -> Finding {
            self.finding.clone()
        }
        async fn admins_answering(&self) -> Vec<String> {
            self.answering.clone()
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-recovery.md:172 "Before any recovery mutation other than the claim itself, it claims the fabric-recovery attempt of the current Build for that mesh; a second launch's claim loses and it exits. Concurrent adoption launches resolve to the same recovery attempt for the exact lost birth, read from the Build journal on the store; while that attempt is active, no other launch opens a competing attempt for that birth. It installs the recorded primaries before electing (Q149) and runs node investigation on the lost birth: reached, or any node-admin answering before its first mutation, it is refused NameHeld and exits, closing the attempt through the failure path. Otherwise the claimed attempt releases the name, re-creates the birth as itself, adopts rafka-time from live members, and restores it as fabric-primary by step 4 once Ready"
    #[test]
    fn adoption_read_claim_check_fail_release_name_and_complete_decide_on_store_records_and_emit_spans() {
        let capture = crate::span_capture::Capture::new(
            "adoption_read_claim_check_fail_release_name_and_complete_decide_on_store_records_and_emit_spans",
        );
        let dispatch = capture.dispatch.clone();
        let d = dispatch.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = tracing::dispatcher::set_default(&d);

        rt.block_on(async move {
            let path: PathName = "mesh1.admin.1".parse().unwrap();
            let fabric_id = FabricId::parse("fab000000001").unwrap();
            let mesh_id = MeshId::parse("meshd0000001").unwrap();
            let store = PathBuf::from("/tmp/rdm-adoption-unit");

            let fabric_store = MemoryFabricStorage::default();
            let mesh_store = MemoryMeshStorage::default();
            let nodes_store = MemoryNodesStorage::default();
            let builds_store = MemoryBuildStateAdapter::default();
            let contexts = crate::build_claim::AttemptContexts::in_memory();

            // 1. Missing Fabric record refuses by name.
            let err = read(
                &path,
                &fabric_id,
                &store,
                Some("process"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .unwrap_err();
            assert!(err.contains("holds no Fabric record"), "{err}");

            // Populate Fabric record with provider = Process; differing MESH_SPAWN_TYPE is refused.
            let mut f_rec = FabricRecord::new(FabricIdentity {
                fabric_id: fabric_id.clone(),
                name: "fabric1".into(),
            });
            f_rec.provider = Some(ProviderKind::Process);
            fabric_store.put_fabric(&f_rec).await.unwrap();

            let err = read(
                &path,
                &fabric_id,
                &store,
                Some("container"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .unwrap_err();
            assert!(err.contains("MESH_SPAWN_TYPE=container and the Fabric record names provider process"), "{err}");

            // Populate live mesh1 record and lost fabric-primary row at mesh1.admin.1, plus current Build.
            crate::storage::claim_mesh(
                &mesh_store,
                None,
                &MeshRecord::new(mesh_id.clone(), "mesh1"),
            )
            .await
            .unwrap()
            .unwrap();

            let lost_id = NodeId::mint();
            let lost_inc = IncarnationId::mint();
            let mut row = NodeRow::new(path.clone(), lost_id.clone(), NodeStatus::Ready);
            row.mesh_id = Some(mesh_id.clone());
            let b = row.birth_mut(&lost_inc, &path);
            b.is_primary = Some(1);
            b.is_fabric_primary = Some(1);
            nodes_store.write_record(&row).await.unwrap();

            let build_id = BuildId::mint();
            crate::build_state::accept(
                &builds_store,
                &BuildAccepted {
                    build_id: build_id.clone(),
                    topology: FabricTopology::root("fabric1", "mesh1", mesh_id.clone()),
                    submitted_change: None,
                    submitted_at_ms: 1000,
                },
                None,
                "mesh1.admin.1",
                "day-0",
            )
            .await
            .unwrap();

            let adoptable = read(
                &path,
                &fabric_id,
                &store,
                Some("process"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .expect("adoptable store succeeds");
            assert_eq!(adoptable.lost.node_id, lost_id);
            assert_eq!(adoptable.lost.incarnation_id, lost_inc);
            assert_eq!(adoptable.current.build_id, build_id);

            // 2. First adoption launch claims attempt 1; concurrent second launch resolves to attempt 1 and loses.
            let me_1 = NodeId::mint();
            let claimed_1 = claim(
                &path,
                &me_1,
                &adoptable,
                &builds_store,
                &mesh_store,
                &contexts,
                2000,
            )
            .await
            .expect("first launch wins recovery claim");
            assert_eq!(claimed_1.row.attempt, 1);

            let adoptable_2 = read(
                &path,
                &fabric_id,
                &store,
                Some("process"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .unwrap();
            let me_2 = NodeId::mint();
            let lost_claim = claim(
                &path,
                &me_2,
                &adoptable_2,
                &builds_store,
                &mesh_store,
                &contexts,
                2001,
            )
            .await
            .unwrap_err();
            assert!(lost_claim.contains("this launch's claim lost"), "{lost_claim}");

            // 3. Check refuses NameHeld when an admin answers, and fail closes attempt 1 and releases in-flight.
            let refused = check(
                &claimed_1,
                &StubAnswers {
                    finding: Finding::Exited { code: Some(0) },
                    answering: vec!["mesh1.admin.2".into()],
                },
            )
            .await
            .expect_err("answering admin refuses NameHeld");
            assert!(refused.reason.contains("NameHeld"), "{}", refused.reason);
            assert_eq!(refused.answered, "mesh1.admin.2");

            fail(&claimed_1, &builds_store, &mesh_store, &refused)
                .await
                .expect("fail closes attempt and releases in-flight");
            assert!(mesh_store.in_flight("mesh1").await.unwrap().is_none());

            // 4. Subsequent adoption launch claims attempt 2, passes check, releases name, and completes.
            let adoptable_3 = read(
                &path,
                &fabric_id,
                &store,
                Some("process"),
                &fabric_store,
                &mesh_store,
                &nodes_store,
                &builds_store,
            )
            .await
            .unwrap();
            let me_3 = NodeId::mint();
            let claimed_3 = claim(
                &path,
                &me_3,
                &adoptable_3,
                &builds_store,
                &mesh_store,
                &contexts,
                3000,
            )
            .await
            .expect("launch after failed attempt wins attempt 2");
            assert_eq!(claimed_3.row.attempt, 2);

            let finding = check(
                &claimed_3,
                &StubAnswers {
                    finding: Finding::Exited { code: Some(0) },
                    answering: Vec::new(),
                },
            )
            .await
            .expect("no admin answering passes check");
            assert_eq!(finding, Finding::Exited { code: Some(0) });

            let written = WrittenRows::default();
            release_name(&claimed_3, &me_3, &nodes_store, &written, &builds_store)
                .await
                .expect("release_name marks lost birth Deleted and claims path for me_3");

            complete(&claimed_3, &me_3, &builds_store, &mesh_store)
                .await
                .expect("complete writes Complete and Converged receipts and releases in-flight");
            assert!(mesh_store.in_flight("mesh1").await.unwrap().is_none());

            // Attempting to claim again for an already-recovered birth is refused by name.
            let me_4 = NodeId::mint();
            let already = claim(
                &path,
                &me_4,
                &adoptable_3,
                &builds_store,
                &mesh_store,
                &contexts,
                4000,
            )
            .await
            .unwrap_err();
            assert!(already.contains("was recovered already"), "{already}");
        });

        let claims = capture.named("rdm.node_admin.fabric.update.via-adoption-claim");
        let outcomes: Vec<_> = claims
            .iter()
            .filter_map(|s| s.attributes.get("outcome").map(String::as_str))
            .collect();
        assert_eq!(outcomes, vec!["won", "lost", "won", "already-recovered"]);

        let checks = capture.named("rdm.node_admin.fabric.update.via-adoption-check");
        let check_outcomes: Vec<_> = checks
            .iter()
            .filter_map(|s| s.attributes.get("outcome").map(String::as_str))
            .collect();
        assert_eq!(check_outcomes, vec!["name-held", "clear"]);

        let fails = capture.named("rdm.node_admin.build.update.via-adoption-failed");
        assert_eq!(fails.len(), 1);
        assert_eq!(
            fails[0].attributes.get("outcome").map(String::as_str),
            Some("closed")
        );

        let completes = capture.named("rdm.node_admin.build.update.via-adoption-complete");
        assert_eq!(completes.len(), 1);
        assert_eq!(
            completes[0].attributes.get("outcome").map(String::as_str),
            Some("converged")
        );
    }
}

