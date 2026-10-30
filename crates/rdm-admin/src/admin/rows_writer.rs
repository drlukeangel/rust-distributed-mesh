//! Durable node, mesh, and fabric row writers and claims.

use super::*;

/// Every birth of `fabric` this admin's view holds besides itself, heard a moment ago or only
/// carried in an installed topology read: the births a maker serves a joining admin for a mesh it
/// holds no snapshot of, beside the ones its records hold.
pub(crate) fn held_births(digest_view: &DigestView, fabric: &FabricId, me: &NodeId) -> Vec<MeshDigest> {
    digest_view.all().into_iter().filter(|d| &d.fabric_id == fabric && &d.node.node_id != me).collect()
}

/// The durable runtime row of the exact birth `(node_id, incarnation)` of `node`, read by its key
/// (`NodesStorage::runtime`, the point read; never a list of every row searched for it), with
/// `reader` naming the admin that reads it. A row never written (`Ok(None)`) and a store that could
/// not be read (`Err`, naming the store's cause) are two facts, each on its own span.
pub(crate) async fn birth_runtime_row(nodes: &dyn crate::storage::NodesStorage, reader: &PathName, node: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, String> {
    match nodes.runtime(node_id, incarnation).await {
        Ok(Some(row)) => Ok(Some(row)),
        Ok(None) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-no-runtime-row", reader = %reader, node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), store = "nodes.storage")
                .in_scope(|| tracing::info!("no durable runtime row of this birth was ever written here"));
            Ok(None)
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-runtime-row-unreadable", reader = %reader, node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), store = "nodes.storage", error = %e)
                .in_scope(|| tracing::info!("the durable runtime row of this birth could not be read"));
            Err(format!("the durable runtime row of {node} ({node_id}) at {} could not be read from {reader}'s nodes.storage: {e}", incarnation.as_str()))
        }
    }
}

/// A mesh's members' addresses and primaries on their Node records, and its Mesh row, on the fabric's
/// shared store root, written by that mesh's primary alone, for its own members alone (R14 item 2; the
/// writer ladder, node-identity-and-state.md §4.3: a mesh row and its members' rows are that mesh's
/// primary's), and by the fabric-primary for the members of a mesh with no primary, as it writes their
/// records (R13, `crate::node_rows::writes_for`). A sweep that writes no mesh writes nothing. The first
/// sweep that writes its own mesh claims the mesh's name ([`crate::storage::claim_mesh`]) and
/// writes the Mesh row (node-identity-and-state.md §2.7): nothing else of the mesh is written until the
/// claim wins, and a name another mesh's live row holds leaves the mesh's rows unwritten for the tenure,
/// named. Then each member birth's endpoint, address and the primaries the view holds for it are
/// written onto the newest facts of its node's record, from the copy this admin holds, whole
/// (storage-whole-records.md, "The rule"), only when they differ. A birth leaves its record's births by
/// the record's own rule ([`crate::storage::BIRTHS_KEPT`]), and a deleted node's record is `Deleted`:
/// neither is read as a node to reach. An address is a restart's hint, never topology.
pub(crate) struct MeshRowsWriter {
    /// The admin's `path.name`, the writer its spans name.
    pub(crate) me: String,
    pub(crate) mesh: String,
    pub(crate) mesh_id: MeshId,
    pub(crate) nodes: Arc<dyn crate::storage::NodesStorage>,
    pub(crate) meshes: Arc<dyn crate::storage::MeshStorage>,
    /// The records this admin writes, as it holds them.
    pub(crate) caches: crate::record_cache::Caches,
    /// The claim of the mesh's name this role tenure decided: `None` outside a tenure or before the
    /// store answered it; `Some(Ok)` once it won; `Some(Err)` when it was refused (another mesh's
    /// live row holds the name, or this mesh is Dead): the mesh's rows stay unwritten for the tenure.
    pub(crate) claim: Option<Result<(), crate::storage::MeshClaimRefused>>,
}

impl MeshRowsWriter {
    pub(crate) fn new(me: &str, mesh: &str, mesh_id: MeshId, nodes: Arc<dyn crate::storage::NodesStorage>, meshes: Arc<dyn crate::storage::MeshStorage>, caches: crate::record_cache::Caches) -> Self {
        Self { me: me.to_string(), mesh: mesh.to_string(), mesh_id, nodes, meshes, caches, claim: None }
    }

    /// One sweep over the births `held` (every birth of the fabric this admin holds, itself
    /// included, each with the primaries its view holds for it), `writes` the meshes whose rows this
    /// admin writes now (its own mesh while it is its mesh-primary; a mesh with no primary while it is
    /// the fabric-primary), `deleted` whether the view holds a node deleted. Returns whether a row is
    /// owed: a write or a read the store did not acknowledge, or a member whose record is not written
    /// yet (its create's claim or its arrival writes it), which the caller retries on its own clock
    /// under the one backoff rule (node-identity-and-state.md §2.7), never waiting for a birth or a
    /// primary change.
    pub(crate) async fn sweep(&mut self, held: &[MeshDigest], writes: &std::collections::BTreeSet<String>, deleted: impl Fn(&NodeId) -> bool) -> bool {
        if !writes.contains(&self.mesh) {
            self.claim = None;
        } else if self.claim.is_none() {
            // A store that did not answer decides nothing: the next sweep asks again.
            self.claim = claim_mesh_name(&*self.meshes, &self.caches, &self.me, &self.mesh, &self.mesh_id).await.ok().map(|c| c.map(|_| ()));
            if self.claim.is_none() {
                return true;
            }
        }
        let own = matches!(self.claim, Some(Ok(())));
        let mut owed = false;
        // Another mesh whose mesh-primary this admin's view holds is that primary's to write (one writer per
        // record, storage-whole-records.md): the writing set comes from a view that can still show it with
        // none, and a whole write from this admin's older copy would replace what its primary wrote.
        let primaried: std::collections::BTreeSet<&str> = held
            .iter()
            .filter(|d| d.node.name.mesh != self.mesh && d.node.name.kind == crate::model::NodeKind::NodeAdmin && d.is_primary.is_some() && !deleted(&d.node.node_id))
            .map(|d| d.node.name.mesh.as_str())
            .collect();
        // A deleted birth is never written: its node's record is written `Deleted` by its delete.
        for d in held.iter().filter(|d| !deleted(&d.node.node_id) && writes.contains(&d.node.name.mesh) && !primaried.contains(d.node.name.mesh.as_str()) && (d.node.name.mesh != self.mesh || own)) {
            let mesh_id = if d.node.name.mesh == self.mesh { Some(self.mesh_id.clone()) } else { d.mesh_id.clone() };
            // The birth's address and the primaries the view holds, put whole on its record from the copy
            // this admin holds; a `Deleted` record is left as it stands.
            let written = self
                .caches
                .nodes
                .update(&*self.nodes, &d.node.node_id, None, |r| {
                    if r.status == crate::model::NodeStatus::Deleted {
                        return false;
                    }
                    let before = r.clone();
                    if mesh_id.is_some() {
                        r.mesh_id = mesh_id.clone();
                    }
                    let b = r.birth_mut(&d.node.incarnation, &d.node.name);
                    b.endpoint_id = Some(d.node.endpoint_id.clone());
                    b.transport_addr = Some(d.node.transport_addr);
                    b.is_primary = d.is_primary;
                    b.is_fabric_primary = d.is_fabric_primary;
                    *r != before
                })
                .await;
            match written {
                Ok(Some(true)) => tracing::info_span!("rdm.node_admin.node.update.via-node-row", node = %self.me, subject = %d.node.name, node_id = %d.node.node_id, is_primary = ?d.is_primary, is_fabric_primary = ?d.is_fabric_primary, outcome = "written")
                    .in_scope(|| tracing::info!("the node's record is put whole, the birth's address and the primaries the view holds recorded on it")),
                Ok(Some(false)) => {}
                Ok(None) => owed = true,
                Err(e) => {
                    owed = true;
                    tracing::info_span!("rdm.node_admin.node.update.via-node-row", node = %self.me, subject = %d.node.name, node_id = %d.node.node_id, is_primary = ?d.is_primary, is_fabric_primary = ?d.is_fabric_primary, outcome = "owed", error = %e)
                        .in_scope(|| tracing::info!("the store did not acknowledge the node's record: it is owed and written again under the one backoff rule"));
                }
            }
        }
        owed
    }
}

/// The mesh rows writer's loop: a sweep over what `digest_view` holds of `fabric` each time a birth or a
/// primary record changes there or `writes` (the meshes whose rows this admin writes) moves, each birth
/// carrying the primaries the view holds for it. A row the store did not acknowledge is owed: the sweep
/// runs again on this writer's own clock under the one backoff rule (node-rpc.md §43), never waiting
/// for a birth, a digest or a primary change (node-identity-and-state.md §2.7). Each sweep takes its
/// turn of `stop`. While this admin, a mesh-primary, has replied `blocked` to a storage change
/// (`commands`), nothing of its mesh is written to the old provider: its rows are held, owed, and the
/// first sweep once its `mesh.unblock` moved it writes them on the provider current then, the new one
/// after the switch, the old one when the change failed before it (storage.md §2 steps 1, 2 and 4; i149
/// R8).
pub(crate) async fn keep_mesh_rows(mut writer: MeshRowsWriter, digest_view: DigestView, fabric: FabricId, mut writes: tokio::sync::watch::Receiver<std::collections::BTreeSet<String>>, stop: crate::stop_gate::StopGate, commands: Arc<crate::node_commands::CommandView>) {
    let mut births = digest_view.birth_changes();
    let mut primaries = digest_view.primaries.subscribe();
    let pacing = rdm_transport::delivery::Pacing::from_cadences();
    let mut owed_sweeps = 0u32;
    loop {
        // Held here while the admin is stopped: its own wait, outside every lock.
        let turn = stop.turn("mesh-rows").await;
        let Some(writing) = commands.row_writes().await else {
            drop(turn);
            tracing::info_span!("rdm.node_admin.node.update.via-rows-held", node = %writer.me, mesh = %writer.mesh)
                .in_scope(|| tracing::info!("this mesh-primary replied blocked to a storage change: its rows are held, owed, and written at its unblock"));
            commands.rows_released().await;
            continue;
        };
        births.borrow_and_update();
        primaries.borrow_and_update();
        let meshes = writes.borrow_and_update().clone();
        let held: Vec<MeshDigest> = digest_view
            .all()
            .into_iter()
            .filter(|d| d.fabric_id == fabric)
            .map(|mut d| {
                digest_view.recorded_primaries(&mut d);
                d
            })
            .collect();
        let owed = writer.sweep(&held, &meshes, |id| digest_view.is_deleted(id.as_str())).await;
        owed_sweeps = if owed { owed_sweeps + 1 } else { 0 };
        drop(writing);
        drop(turn);
        let again = async {
            if owed_sweeps == 0 {
                std::future::pending::<()>().await
            }
            tokio::time::sleep(pacing.after(owed_sweeps)).await
        };
        tokio::select! {
            changed = births.changed() => if changed.is_err() { return },
            changed = primaries.changed() => if changed.is_err() { return },
            changed = writes.changed() => if changed.is_err() { return },
            () = again => {}
        }
    }
}

/// Every birth the live records of every mesh hold an address for (`crate::storage::live_records`),
/// each record's newest birth: the map a restarting admin reaches its fabric through (its gossip
/// seeds, its durable map, the peers it takes rafka-time from, the births it publishes not yet
/// reached) and a maker's stored topology. A record whose newest birth carries no address names no
/// node to reach.
pub(crate) async fn recorded_births(meshes: &dyn crate::storage::MeshStorage, nodes: &dyn crate::storage::NodesStorage, own_mesh: &str) -> Result<Vec<crate::reenter::MapNode>, crate::storage_error::StorageError> {
    Ok(crate::storage::live_records(meshes, nodes, own_mesh).await?.iter().filter_map(crate::reenter::MapNode::of_record).collect())
}

/// Hold the primaries the node records on the store root record (`is_primary`, `is_fabric_primary` on
/// each record's newest birth, a `Deleted` record's included: [`crate::storage::records`]), the own
/// record's included: an election input (elections §3), each
/// held only when it supersedes the record held. The records record the role; the election decides it.
/// The store root is the fabric's own, so every record on it is of this fabric.
pub(crate) async fn take_recorded_primaries(meshes: &dyn crate::storage::MeshStorage, nodes: &dyn crate::storage::NodesStorage, own_mesh: &str, primaries: &rdm_transport::topology::PrimaryView) -> Result<usize, crate::storage_error::StorageError> {
    let mut taken = 0;
    for r in crate::storage::records(meshes, nodes, own_mesh).await? {
        let Some(b) = r.newest_birth() else { continue };
        let record = |revision| rdm_core::Primary { mesh: r.name.mesh.clone(), node_id: r.node_id.clone(), incarnation: b.incarnation_id.clone(), revision };
        if let Some(revision) = b.is_primary {
            taken += usize::from(matches!(primaries.take(rdm_core::PrimaryRole::of_node_type(r.name.kind), &record(revision)), rdm_transport::topology::RoleTaken::Held { .. }));
        }
        if let Some(revision) = b.is_fabric_primary {
            taken += usize::from(matches!(primaries.take(rdm_core::PrimaryRole::FabricPrimary, &record(revision)), rdm_transport::topology::RoleTaken::Held { .. }));
        }
    }
    Ok(taken)
}

/// The Mesh row of the mesh a restarting node-admin's own row names, read by that mesh's id (R-MN1: a
/// mesh created again under the same name is a new mesh with a new id; R-ML2: a dead mesh is never
/// restarted): the point read at the mesh's key, the name, is taken as the admin's mesh only when it
/// is the record of the mesh id its own row holds, read by that id, and live. The name's claim held by
/// another MeshId means the admin's mesh was removed and a mesh created again under the name; refused
/// by name, naming both ids, as are a Dead record and no record.
pub(crate) async fn own_mesh_row(meshes: &dyn crate::storage::MeshStorage, own: &crate::storage::NodeRecord) -> Result<crate::storage::MeshRecord, String> {
    let (mesh, mesh_id) = (&own.name.mesh, &own.mesh_id);
    let unread = |e: crate::storage_error::StorageError| format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage could not read the record of {mesh}: {e}", own.name);
    let held = meshes.mesh(mesh_id).await.map_err(unread)?;
    if held.as_ref().is_some_and(|row| row.dead) {
        return Err(format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), whose record is Dead: a dead mesh is never restarted", own.name));
    }
    if let Some(other) = meshes.mesh_named(mesh).await.map_err(unread)?.filter(|row| row.mesh_id != *mesh_id) {
        return Err(format!(
            "this admin ({}) belongs to mesh {mesh_id} ({mesh}), which was removed: the name {mesh} is held by mesh {}, a mesh created again under it, and a removed mesh is never rejoined",
            own.name, other.mesh_id
        ));
    }
    held.ok_or_else(|| format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage holds no record of {mesh}", own.name))
}

/// mesh.create's claim of the mesh's name at its key (node-identity-and-state.md §2.7)
/// ([`crate::storage::claim_mesh`]), made by `me`: the
/// fabric-primary at CreateMesh, as the writer of a mesh with no primary (R13), the mesh's first
/// durable write; and the mesh's primary when its role tenure starts, which finds the mesh's own row
/// (the same MeshId re-claiming, idempotent). The Mesh row written, or this mesh's own row found
/// standing, or refused by name. One `rdm.node_admin.mesh.create.via-mesh-row` span, `moment`
/// `claim`. `Err`: the store did not answer, and nothing is decided.
pub(crate) async fn claim_mesh_name(meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<Result<crate::storage::MeshClaim, crate::storage::MeshClaimRefused>, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.mesh.create.via-mesh-row",
        node = %me,
        mesh = %mesh,
        mesh_id = %mesh_id,
        moment = "claim",
        outcome = tracing::field::Empty,
        holder = tracing::field::Empty,
        over = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    // The record this writer holds of the mesh (read when it holds none: the claim is its writing's
    // first act), live under its name: the claim writes it whole.
    let _one = caches.meshes.lock(mesh_id).await;
    let claimed: Result<Result<crate::storage::MeshClaim, crate::storage::MeshClaimRefused>, String> = async {
        let held = match caches.meshes.get(mesh_id) {
            Some(r) => Some(r),
            None => meshes.mesh(mesh_id).await.map_err(|e| format!("{me}: mesh.storage did not answer the read of {mesh}'s record (mesh {mesh_id}): {e}"))?,
        };
        let mut record = held.clone().unwrap_or_else(|| crate::storage::MeshRecord::new(mesh_id.clone(), mesh));
        record.name = mesh.to_string();
        record.dead = false;
        let claimed = crate::storage::claim_mesh(&*meshes, held.as_ref(), &record).await.map_err(|e| format!("{me}: mesh.storage did not answer the claim of {mesh} for mesh {mesh_id}: {e}"))?;
        if claimed.is_ok() {
            caches.meshes.hold(mesh_id.clone(), record);
        }
        Ok(claimed)
    }
    .await;
    match &claimed {
        Ok(Ok(crate::storage::MeshClaim::Written { over })) => {
            span.record("outcome", "row-written");
            if let Some(over) = over {
                span.record("over", tracing::field::display(over));
            }
        }
        Ok(Ok(crate::storage::MeshClaim::Held)) => {
            span.record("outcome", "row-held-by-mesh");
        }
        Ok(Err(refused)) => {
            span.record("outcome", refused.outcome()).record("detail", refused.to_string().as_str());
            if let crate::storage::MeshClaimRefused::NameHeld { by, .. } = refused {
                span.record("holder", tracing::field::display(by));
            }
        }
        Err(e) => {
            span.record("outcome", "row-refused").record("detail", e.as_str());
        }
    }
    span.in_scope(|| tracing::info!("mesh.create: the mesh's name is claimed at its key; nothing else of the mesh is written until the claim wins"));
    claimed
}

/// Why `create-mesh:<mesh>` did not claim the mesh's name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateMeshError {
    /// The mesh store answered and refused the claim by name: nothing of the mesh is written.
    Refused(String),
    /// The mesh store did not answer: nothing is decided, and nothing of the mesh is written.
    Unanswered(String),
}

impl std::fmt::Display for CreateMeshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(why) | Self::Unanswered(why) => f.write_str(why),
        }
    }
}

/// The mesh id the operation carries, held for what every launch of the mesh carries.
pub(crate) fn hold_mesh_id(records: &Records, mesh: &str, mesh_id: &MeshId) {
    records.meshes.lock().unwrap().insert(mesh.to_string(), mesh_id.clone());
    records.wake.poke();
    records.inputs_moved();
}

/// The claim `create-mesh:<mesh>` makes at the fabric-primary `me` (lifecycles/node-create.md "Mesh absent
/// ... Until the mesh has a primary, the fabric-primary writes its rows"; R13; CLAUDE.md: the claim is
/// the first durable write of a create): `mesh_id`, the id the operation carries (minted once, when the
/// mesh entered the accepted topology: node-identity-and-state.md §2.7, "Who mints it"), is claimed at the
/// mesh's name before anything else of the mesh is written, and held in `records` (what every launch of
/// the mesh carries) only once the claim won. Every run of the operation claims with that id, so a run
/// whose predecessor claimed and was lost finds the mesh's own row: the same MeshId re-claiming is
/// idempotent. A refused claim fails the operation by name, holding nothing of the mesh: the Build runs
/// no create of its nodes, so no node row of the mesh is claimed.
pub(crate) async fn create_mesh(records: &Records, meshes: &dyn crate::storage::MeshStorage, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<MeshId, CreateMeshError> {
    match claim_mesh_name(meshes, &records.caches, me, mesh, mesh_id).await {
        Ok(Ok(_)) => {
            // The won claim's id is the mesh's: it replaces any id held for the name before (a
            // removed mesh's), so no launch of this mesh carries another.
            hold_mesh_id(records, mesh, mesh_id);
            Ok(mesh_id.clone())
        }
        Ok(Err(refused)) => Err(CreateMeshError::Refused(format!("{me}: create-mesh:{mesh}: the claim of the mesh's name for mesh {mesh_id} was refused, and nothing of the mesh is written: {refused}"))),
        Err(e) => Err(CreateMeshError::Unanswered(format!("{e}; nothing of the mesh is written"))),
    }
}

/// A step's receipt, its durable commit: the Build state takes it, or the step is not committed and the
/// refusal is named with the step, its operation and its attempt.
pub(crate) async fn commit_receipt(builds: &dyn BuildStateAdapter, me: &str, receipt: crate::build_state::BuildStepReceipt) -> Result<(), (&'static str, String)> {
    builds
        .append_step_receipt(&receipt)
        .await
        .map_err(|e| ("receipt-refused", format!("{me}: the {} receipt of {} (Build {}, attempt {}) was not written: {e}", receipt.step, receipt.operation, receipt.build_id, receipt.attempt)))
}

/// `create-mesh:<mesh>` as the Build runs it (node-rpc-envelope.md, Build op 0x20: "completed step
/// receipts are read and replayed, never executed again"; Build carrier: "Completed steps are not
/// rerun. Settle an idempotent uncertain step by its natural key"). The operation's one step is the
/// claim of the mesh's name ([`create_mesh`]), and its `Complete` receipt, written by `executor`, is the
/// step's durable commit.
///
/// - A `Complete` receipt of the operation in the Build's current run (from the attempt a reopen began,
///   [`crate::build_state::BuildProjection::opened`], to this attempt) is the step done: it is replayed, which holds the operation's mesh id for this admin's launches and claims
///   nothing and writes nothing.
/// - No receipt is an uncertain step, run by its natural key: the claim finds the mesh free (written),
///   or its own row standing (`row-held-by-mesh`: an earlier run claimed and was lost before it
///   receipted), and the `Complete` receipt follows. A claim refused by name leaves a `Failed` receipt of
///   `ClaimMeshName` naming the holder, and the step fails.
/// - A mesh store that does not answer decides nothing and receipts nothing; a receipt the Build state
///   does not take leaves the step uncommitted. Each fails the step by its own name.
///
/// One `rdm.node_admin.mesh.create.via-receipt` span per run; its `outcome` is `replayed`, `complete`,
/// `refused`, `store-unanswered`, `receipt-refused` or `build-unreadable`, and the claim's own span is its
/// child.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_mesh_step(records: &Records, meshes: &dyn crate::storage::MeshStorage, builds: &dyn BuildStateAdapter, executor: &str, me: &str, build_id: &crate::build::BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
    use crate::build_state::{BuildStepReceipt, StepOutcome};
    use crate::deployment::pipeline::CreateMeshStep;
    let BuildOperation::CreateMesh { mesh, mesh_id } = op else { return Err(format!("{me}: {} is not a create-mesh operation", op.key())) };
    let operation = op.key();
    let span = tracing::info_span!(
        "rdm.node_admin.mesh.create.via-receipt",
        node = %me,
        mesh = %mesh,
        mesh_id = %mesh_id,
        build_id = %build_id.log(),
        attempt,
        operation = %operation,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let receipt = |step: CreateMeshStep, outcome: StepOutcome| BuildStepReceipt { mesh: None, build_id: build_id.clone(), attempt, operation: operation.clone(), step: step.name().into(), outcome, output: None, executor: Some(executor.to_string()) };
    let run = async {
        let held = builds.read_build(build_id).await.map_err(|e| ("build-unreadable", format!("{me}: {operation}: the Build {build_id} could not be read for its receipts: {e}")))?;
        if held.steps.iter().any(|r| r.operation == operation && r.step == CreateMeshStep::Complete.name() && r.outcome == StepOutcome::Complete && r.attempt >= held.opened && r.attempt <= attempt) {
            hold_mesh_id(records, mesh, mesh_id);
            return Ok("replayed");
        }
        match create_mesh(records, meshes, me, mesh, mesh_id).await {
            Ok(_) => {
                commit_receipt(builds, me, receipt(CreateMeshStep::Complete, StepOutcome::Complete)).await?;
                Ok("complete")
            }
            Err(CreateMeshError::Refused(reason)) => {
                commit_receipt(builds, me, receipt(CreateMeshStep::ClaimMeshName, StepOutcome::failed(reason.clone()))).await?;
                Err(("refused", reason))
            }
            Err(CreateMeshError::Unanswered(reason)) => Err(("store-unanswered", reason)),
        }
    };
    let r = tracing::Instrument::instrument(run, span.clone()).await;
    match &r {
        Ok(outcome) => span.record("outcome", *outcome),
        Err((outcome, detail)) => span.record("outcome", *outcome).record("detail", detail.as_str()),
    };
    span.in_scope(|| tracing::info!("create-mesh: the operation's step is receipted, or replayed from its receipt"));
    r.map(|_| ()).map_err(|(_, detail)| detail)
}

/// fabric.create's claim of the fabric's name (node-identity-and-state.md §2.8)
/// ([`crate::fabric_storage::claim_identity`]): Day 0's first write of
/// the fabric, and the fabric-primary's row writer's before it writes anything else to `fabric/`. One
/// `rdm.node_admin.fabric.create.via-fabric-row` span, `moment` `claim`, names the outcome
/// (`row-written`, `row-held-by-fabric`, `name-held` with the holder, `row-refused`).
pub(crate) async fn claim_fabric_name(storage: &dyn crate::fabric_storage::FabricStorage, identity: &crate::fabric_storage::FabricIdentity, node_id: &NodeId) -> Result<Result<crate::fabric_storage::FabricClaim, crate::fabric_storage::FabricNameHeld>, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.create.via-fabric-row",
        node_id = %node_id,
        fabric = %identity.name,
        fabric_id = %identity.fabric_id,
        moment = "claim",
        outcome = tracing::field::Empty,
        holder = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let claimed = crate::fabric_storage::claim_identity(&*storage, identity).await.map_err(|e| format!("fabric.storage did not answer the claim of the fabric name {} for Fabric {}: {e}", identity.name, identity.fabric_id));
    match &claimed {
        Ok(Ok(crate::fabric_storage::FabricClaim::Written)) => span.record("outcome", "row-written"),
        Ok(Ok(crate::fabric_storage::FabricClaim::Held)) => span.record("outcome", "row-held-by-fabric"),
        Ok(Err(held)) => span.record("outcome", "name-held").record("holder", tracing::field::display(&held.by)).record("detail", held.to_string().as_str()),
        Err(e) => span.record("outcome", "row-refused").record("detail", e.as_str()),
    };
    span.in_scope(|| tracing::info!("fabric.create: the fabric's name is claimed at its key; nothing else of the fabric is written until the claim wins"));
    claimed
}

/// `fabric/` on the fabric's store root, written by the fabric-primary alone (R14 item 2): while this
/// birth holds the fabric-primary role it writes the Fabric identity it is configured for, so a role that
/// came to it from a lost primary still leaves the Fabric row on the store root. Who is primary is no `fabric/` row: it is the
/// election's result, recorded as `is_primary` and `is_fabric_primary` on the node rows (build.md
/// §5.3). The Fabric row carries no pointer to the newest Build (R-BI3). The claim of the
/// fabric's name comes first: nothing else is written to `fabric/` until it wins, and a name another
/// Fabric's identity holds leaves `fabric/` unwritten by this writer, named once. While it does not
/// hold the role it writes nothing to `fabric/`.
pub(crate) struct FabricRowsWriter {
    /// This admin's exact birth: it writes only while the fabric-primary role names it.
    pub(crate) me: (NodeId, IncarnationId),
    pub(crate) identity: crate::fabric_storage::FabricIdentity,
    pub(crate) storage: Arc<dyn crate::fabric_storage::FabricStorage>,
    /// The claim of the fabric's name: `None` before the store answered it; `Some(Ok)` once it won;
    /// `Some(Err)` when another Fabric's identity holds the key (a held identity is never rewritten).
    pub(crate) claim: Option<Result<(), crate::fabric_storage::FabricNameHeld>>,
}

impl FabricRowsWriter {
    pub(crate) fn new(me: (NodeId, IncarnationId), identity: crate::fabric_storage::FabricIdentity, storage: Arc<dyn crate::fabric_storage::FabricStorage>) -> Self {
        Self { me, identity, storage, claim: None }
    }

    /// One sweep on the primary records `roles` holds: while the fabric-primary role names this birth, the claim
    /// of the fabric's name (which writes the identity row) until the store answers it.
    pub(crate) async fn sweep(&mut self, primaries: &rdm_transport::topology::PrimaryView) {
        if !primaries.fabric().is_some_and(|h| h.is_birth(&self.me.0, &self.me.1)) {
            return;
        }
        let (storage, identity) = (&self.storage, &self.identity);
        if self.claim.is_none() {
            // A store that did not answer decides nothing: the next sweep asks again.
            self.claim = claim_fabric_name(&**storage, identity, &self.me.0).await.ok().map(|c| c.map(|_| ()));
        }
    }
}

/// The provider's inspection of the exact birth a digest or a durable row names
/// (node-runtime-inspect.md): the runtime fact is the digest's (when it is of this birth) or the
/// row's, adopted in this provider's control domain and inspected; its answer is node investigation's
/// provider step.
pub async fn inspection_of(
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    held_row: &Option<(MeshDigest, Duration)>,
    row: Option<&crate::storage::RuntimeRow>,
) -> Inspected {
    inspection_with_handle(provider, held_row, row).await.0
}

/// [`inspection_of`], with the exact runtime handle the provider was asked about beside the answer
/// (node-runtime-inspect.md Target; the inspect span's `handle`): `None` when no handle was adopted,
/// so nothing was asked.
pub async fn inspection_with_handle(
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    held_row: &Option<(MeshDigest, Duration)>,
    row: Option<&crate::storage::RuntimeRow>,
) -> (Inspected, Option<DeploymentHandle>) {
    // The runtime fact one way for every inspect (`node_investigation::runtime_of`).
    let Some(crate::node_investigation::BirthRuntime { fact, data_dir, incarnation: birth }) = crate::node_investigation::runtime_of(held_row.as_ref().map(|(dg, _)| dg), row) else {
        return (Inspected::Unavailable(format!("no runtime fact: the view holds {} for it and no durable row names it", if held_row.is_some() { "a digest without a runtime" } else { "no digest of this birth" })), None);
    };
    // The exact runtime handle the inspect asks about: none when the provider refuses to adopt the
    // fact, which the inspect names as its `unavailable`.
    let handle = crate::deployment::provider::adopt(provider, &fact).ok();
    (crate::deployment::provider::inspect_runtime(provider, &fact, data_dir.as_deref(), &birth.as_str()).await, handle)
}

/// The workflow a member's removal was announced by ([`RecoverySubmitter::removal_announced`]):
/// which announcement, and the operation, Build and attempt it names (a mesh.delete announced by its
/// mesh's status names its operation and no Build).
pub(crate) struct Announced {
    pub(crate) announced: &'static str,
    pub(crate) operation: String,
    pub(crate) build_id: String,
    pub(crate) attempt: u32,
}
