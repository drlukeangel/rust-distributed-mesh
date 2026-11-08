//! The Mesh record's and the Fabric record's writers, and the reads of the node records the stores
//! hold (each node writes its own record, Q356).

use super::*;

/// Every birth of `fabric` this admin's view holds besides itself, heard a moment ago or only
/// carried in an installed topology read: the births a maker serves a joining admin for a mesh it
/// holds no aggregate of, beside the ones its records hold.
pub(crate) fn held_births(digest_view: &DigestView, fabric: &FabricId, me: &NodeId) -> Vec<MeshDigest> {
    digest_view.all().into_iter().filter(|d| &d.fabric_id == fabric && &d.node.node_id != me).collect()
}

/// Read the exact birth's node record by node ID and incarnation: the runtime the node wrote on it.
/// A missing record and an unreadable store remain distinct outcomes.
pub(crate) async fn birth_runtime_row(nodes: &dyn crate::storage::NodesStorage, reader: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::NodeRecord>, String> {
    match crate::node_rows::birth_record(nodes, node_id, incarnation).await {
        Ok(Some(row)) => Ok(Some(row)),
        Ok(None) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-no-runtime-row", reader_path = %reader.path, reader_name = %reader.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), store = "nodes.storage")
                .in_scope(|| tracing::info!("no node record of this birth was ever written here"));
            Ok(None)
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-runtime-row-unreadable", reader_path = %reader.path, reader_name = %reader.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), store = "nodes.storage", error = %e)
                .in_scope(|| tracing::info!("the node record of this birth could not be read"));
            Err(format!("the node record of node {node_id}, incarnation {} could not be read from nodes.storage by path {:?}, name {:?}: {e}", incarnation.as_str(), reader.path, reader.name))
        }
    }
}

/// A mesh's Mesh row on the fabric's shared store root, written by that mesh's primary alone, and by
/// the fabric-primary for a mesh with no primary (R13; node-identity-and-state.md §2.7). The first
/// sweep of a tenure that writes its own mesh checks the mesh's name against the mesh records
/// ([`crate::storage::write_mesh`]) and writes the Mesh row; a name another mesh's live row holds leaves
/// it unwritten for the tenure, named. Each node writes its own node record (Q356).
pub(crate) struct MeshRowsWriter {
    /// The admin's globally unique node ID, rendered for writer diagnostics.
    pub(crate) me: String,
    pub(crate) mesh: String,
    pub(crate) mesh_id: MeshId,
    pub(crate) meshes: Arc<dyn crate::storage::MeshStorage>,
    /// The records this admin writes, as it holds them.
    pub(crate) caches: crate::record_cache::Caches,
    /// The write of the mesh's row this role tenure decided: `None` outside a tenure or before the
    /// store answered it; `Some(Ok)` once it is written; `Some(Err)` when it was refused (another mesh's
    /// live row holds the name, or this mesh is deleted).
    pub(crate) written: Option<Result<(), crate::storage::MeshRefused>>,
}

impl MeshRowsWriter {
    pub(crate) fn new(me: &str, mesh: &str, mesh_id: MeshId, meshes: Arc<dyn crate::storage::MeshStorage>, caches: crate::record_cache::Caches) -> Self {
        Self { me: me.to_string(), mesh: mesh.to_string(), mesh_id, meshes, caches, written: None }
    }

    /// One sweep: `writes` the meshes this admin writes now (its own mesh while it is its mesh-primary;
    /// a mesh with no primary while it is the fabric-primary). Returns whether the Mesh row is owed: a
    /// write the store did not acknowledge, which the caller retries on its own clock under the one
    /// backoff rule (node-identity-and-state.md §2.7).
    pub(crate) async fn sweep(&mut self, writes: &std::collections::BTreeSet<String>) -> bool {
        if !writes.contains(&self.mesh) {
            self.written = None;
        } else if self.written.is_none() {
            // A store that did not answer decides nothing: the next sweep asks again.
            self.written = write_mesh_row(&*self.meshes, &self.caches, &self.me, &self.mesh, &self.mesh_id).await.ok().map(|c| c.map(|_| ()));
            if self.written.is_none() {
                return true;
            }
        }
        false
    }
}

/// The Mesh row writer's loop: a sweep each time `writes` (the meshes whose rows this admin writes)
/// moves, and again on this writer's own clock under the one backoff rule (node-rpc.md §43) while a
/// write is owed. Each sweep takes its turn of `stop`. While this admin, a mesh-primary, has replied
/// `blocked` to a storage change (`commands`), nothing of its mesh is written to the old provider
/// (storage.md §2 steps 1, 2 and 4; i149 R8).
pub(crate) async fn keep_mesh_rows(mut writer: MeshRowsWriter, mut writes: tokio::sync::watch::Receiver<std::collections::BTreeSet<String>>, stop: crate::stop_gate::StopGate, commands: Arc<crate::node_commands::CommandView>) {
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
        let meshes = writes.borrow_and_update().clone();
        let owed = writer.sweep(&meshes).await;
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
    let mut out = Vec::new();
    for record in crate::storage::live_records(meshes, nodes, own_mesh).await? {
        out.extend(crate::reenter::MapNode::of_record(&record));
    }
    Ok(out)
}

/// Hold the primaries the node records on the store root record (`is_primary`, `is_fabric_primary` on
/// each record's newest birth, a `Deleted` record's included: [`crate::storage::records`]), the own
/// record's included: an election input (elections §3), each
/// held only when it supersedes the record held. The records record the role; the election decides it.
/// The store root is the fabric's own, so every record on it is of this fabric.
pub(crate) async fn take_recorded_primaries(meshes: &dyn crate::storage::MeshStorage, nodes: &dyn crate::storage::NodesStorage, own_mesh: &str, primaries: &rdm_gossip::primary::PrimaryView) -> Result<usize, crate::storage_error::StorageError> {
    let mut taken = 0;
    for r in crate::storage::records(meshes, nodes, own_mesh).await? {
        let record = |revision| rdm_core::Primary { mesh: r.mesh(), node_id: r.node_id.clone(), incarnation: r.incarnation.clone(), revision };
        if let Some(revision) = r.is_primary {
            taken += usize::from(matches!(primaries.take(rdm_core::PrimaryRole::of_type(&r.r#type), &record(revision)), rdm_gossip::primary::RoleTaken::Held { .. }));
        }
        if let Some(revision) = r.is_fabric_primary {
            taken += usize::from(matches!(primaries.take(rdm_core::PrimaryRole::FabricPrimary, &record(revision)), rdm_gossip::primary::RoleTaken::Held { .. }));
        }
    }
    Ok(taken)
}

/// The Mesh row of the mesh a restarting node-admin's own row names, read by that mesh's id (R-MN1: a
/// mesh created again under the same name is a new mesh with a new id; R-ML2: a deleted mesh is never
/// restarted): the point read at the mesh's key, the name, is taken as the admin's mesh only when it
/// is the record of the mesh id its own row holds, read by that id, and live. The name held by
/// another MeshId means the admin's mesh was removed and a mesh created again under the name; refused
/// by name, naming both ids, as are a deleted record and no record.
pub(crate) async fn own_mesh_row(meshes: &dyn crate::storage::MeshStorage, own: &crate::storage::OwnRow) -> Result<crate::storage::MeshRecord, String> {
    let (mesh, mesh_id) = (&own.mesh, &own.mesh_id);
    let unread = |e: crate::storage_error::StorageError| format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage could not read the record of {mesh}: {e}", own.node_id);
    let held = meshes.mesh(mesh_id).await.map_err(unread)?;
    if held.as_ref().is_some_and(|row| row.is_deleted()) {
        return Err(format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), whose record is deleted: a deleted mesh is never restarted", own.node_id));
    }
    if let Some(other) = meshes.mesh_named(mesh).await.map_err(unread)?.filter(|row| row.mesh_id != *mesh_id) {
        return Err(format!(
            "this admin ({}) belongs to mesh {mesh_id} ({mesh}), which was removed: the name {mesh} is held by mesh {}, a mesh created again under it, and a removed mesh is never rejoined",
            own.node_id, other.mesh_id
        ));
    }
    held.ok_or_else(|| format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage holds no record of {mesh}", own.node_id))
}

/// mesh.create's write of the mesh's row at its name's key (node-identity-and-state.md §2.7)
/// ([`crate::storage::write_mesh`]), made by `me`: the
/// fabric-primary at CreateMesh, as the writer of a mesh with no primary (R13), the mesh's first
/// durable write; and the mesh's primary when its role tenure starts, which finds the mesh's own row
/// (the same MeshId writing its own row again, idempotent). The Mesh row written, or this mesh's own row found
/// standing, or refused by name. One `rdm.node_admin.mesh.create.via-mesh-row` span, `moment`
/// `write`. `Err`: the store did not answer, and nothing is decided.
pub(crate) async fn write_mesh_row(meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<Result<crate::storage::MeshWritten, crate::storage::MeshRefused>, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.mesh.create.via-mesh-row",
        node = %me,
        mesh = %mesh,
        mesh_id = %mesh_id,
        moment = "write",
        outcome = tracing::field::Empty,
        holder = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    // The record this writer holds of the mesh (read when it holds none: the row is the mesh's
    // first write), live under its name: the write puts it whole.
    let _one = caches.meshes.lock(mesh_id).await;
    let written: Result<Result<crate::storage::MeshWritten, crate::storage::MeshRefused>, String> = async {
        let held = match caches.meshes.get(mesh_id) {
            Some(r) => Some(r),
            None => meshes.mesh(mesh_id).await.map_err(|e| format!("{me}: mesh.storage did not answer the read of {mesh}'s record (mesh {mesh_id}): {e}"))?,
        };
        let mut record = held.clone().unwrap_or_else(|| crate::storage::MeshRecord::new(mesh_id.clone(), mesh));
        record.name = mesh.to_string();
        // The write is of a live mesh: a deleted one's is refused (`MeshDeleted`), never written.
        if record.is_deleted() {
            record.state = rdm_core::status::MeshState::Pending;
        }
        let written = crate::storage::write_mesh(&*meshes, held.as_ref(), &record).await.map_err(|e| format!("{me}: mesh.storage did not answer the write of {mesh}'s row for mesh {mesh_id}: {e}"))?;
        if let Ok(crate::storage::MeshWritten::Written) = &written {
            caches.mesh_moved(held.as_ref(), &record).await;
        }
        if written.is_ok() {
            caches.meshes.hold(mesh_id.clone(), record);
        }
        Ok(written)
    }
    .await;
    match &written {
        Ok(Ok(crate::storage::MeshWritten::Written)) => {
            span.record("outcome", "row-written");
        }
        Ok(Ok(crate::storage::MeshWritten::Held)) => {
            span.record("outcome", "row-held-by-mesh");
        }
        Ok(Err(refused)) => {
            span.record("outcome", refused.outcome()).record("detail", refused.to_string().as_str());
            if let crate::storage::MeshRefused::NameHeld { by, .. } = refused {
                span.record("holder", tracing::field::display(by));
            }
        }
        Err(e) => {
            span.record("outcome", "row-refused").record("detail", e.as_str());
        }
    }
    span.in_scope(|| tracing::info!("mesh.create: the mesh's name is checked against the records and its row written; nothing else of the mesh is written until it is"));
    written
}

/// Why `create-mesh:<mesh>` did not write the mesh's row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateMeshError {
    /// The mesh store answered and refused the write by name: nothing of the mesh is written.
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

/// The row `create-mesh:<mesh>` writes at the fabric-primary `me` (lifecycles/node-create.md "Mesh absent
/// ... Until the mesh has a primary, the fabric-primary writes its rows"; R13: the row is
/// the first durable write of a create): `mesh_id`, the id the operation carries (minted once, when the
/// mesh entered the accepted topology: node-identity-and-state.md §2.7, "Who mints it"), is written at the
/// mesh's name before anything else of the mesh is written, and held in `records` (what every launch of
/// the mesh carries) only once the row is written. Every run of the operation writes with that id, so a run
/// whose predecessor wrote and was lost finds the mesh's own row: the same MeshId writing its own row again is
/// idempotent. A refused write fails the operation by name, holding nothing of the mesh: the Build runs
/// no create of its nodes, so no node row of the mesh is written.
pub(crate) async fn create_mesh(records: &Records, meshes: &dyn crate::storage::MeshStorage, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<MeshId, CreateMeshError> {
    match write_mesh_row(meshes, &records.caches, me, mesh, mesh_id).await {
        Ok(Ok(_)) => {
            // The written row's id is the mesh's: it replaces any id held for the name before (a
            // removed mesh's), so no launch of this mesh carries another.
            hold_mesh_id(records, mesh, mesh_id);
            Ok(mesh_id.clone())
        }
        Ok(Err(refused)) => Err(CreateMeshError::Refused(format!("{me}: create-mesh:{mesh}: the write of the mesh's row for mesh {mesh_id} was refused, and nothing of the mesh is written: {refused}"))),
        Err(e) => Err(CreateMeshError::Unanswered(format!("{e}; nothing of the mesh is written"))),
    }
}

/// A step's `rdm.build.step` event, its durable commit: the Build state writes it, or the step is not
/// committed and the refusal is named with the step and its operation.
pub(crate) async fn commit_receipt(builds: &dyn BuildStateAdapter, me: &str, step: crate::build_state::StepWrite) -> Result<(), (&'static str, String)> {
    builds
        .write_step(&step)
        .await
        .map_err(|e| ("receipt-refused", format!("{me}: the {} event of {} (Build {}) was not written: {e}", step.step, step.operation, step.build_id)))
}

/// `create-mesh:<mesh>` as the Build runs it (node-rpc-envelope.md, Build op 0x20: "completed step
/// receipts are read and replayed, never executed again"; Build carrier: "Completed steps are not
/// rerun. Settle an idempotent uncertain step by its natural key"). The operation's one step is the
/// write of the mesh's row ([`create_mesh`]), and its `Complete` step event, written by this admin, is
/// the step's durable commit.
///
/// - A `Complete` step of the operation whose event ended `complete` is the step done: it is replayed,
///   which holds the operation's mesh id for this admin's launches and writes nothing else.
/// - No receipt is an uncertain step, run by its natural key: the write finds the mesh free (written),
///   or its own row standing (`row-held-by-mesh`: an earlier run wrote and was lost before it
///   receipted), and the `Complete` receipt follows. A write refused by name leaves a `Failed` receipt of
///   `ClaimMeshName` naming the holder, and the step fails.
/// - A mesh store that does not answer decides nothing and receipts nothing; a receipt the Build state
///   does not take leaves the step uncommitted. Each fails the step by its own name.
///
/// One `rdm.node_admin.mesh.create.via-receipt` span per run; its `outcome` is `replayed`, `complete`,
/// `refused`, `store-unanswered`, `receipt-refused` or `build-unreadable`, and the write's own span is its
/// child.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_mesh_step(records: &Records, meshes: &dyn crate::storage::MeshStorage, builds: &dyn BuildStateAdapter, me: &str, build_id: &crate::build::BuildId, op: &BuildOperation) -> Result<(), String> {
    use crate::build_state::{StepOutcome, StepWrite};
    use crate::deployment::pipeline::CreateMeshStep;
    let BuildOperation::CreateMesh { mesh, mesh_id } = op else { return Err(format!("{me}: {:?} is not a create-mesh operation", op.key())) };
    let operation = op.key();
    let span = tracing::info_span!(
        "rdm.node_admin.mesh.create.via-receipt",
        node = %me,
        mesh = %mesh,
        mesh_id = %mesh_id,
        build_id = %build_id.log(),
        operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let receipt = |step: CreateMeshStep, outcome: StepOutcome| StepWrite { build_id: build_id.clone(), mesh: None, operation: operation.clone(), step: step.name().into(), outcome };
    let run = async {
        let held = builds.read_build(build_id).await.map_err(|e| ("build-unreadable", format!("{me}: {operation:?}: the Build {build_id} could not be read for its receipts: {e}")))?;
        if held.completed(&operation.to_string(), CreateMeshStep::Complete.name()) {
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

/// fabric.create's write of the fabric's row (node-identity-and-state.md §2.7)
/// ([`crate::fabric_storage::write_identity`]): Day 0's first write of
/// the fabric, and the fabric-primary's row writer's before it writes anything else to `fabric/`. One
/// `rdm.node_admin.fabric.create.via-fabric-row` span, `moment` `write`, names the outcome
/// (`row-written`, `row-held-by-fabric`, `name-held` with the holder, `row-refused`).
pub(crate) async fn write_fabric_row(storage: &dyn crate::fabric_storage::FabricStorage, identity: &crate::fabric_storage::FabricIdentity, node_id: &NodeId) -> Result<Result<crate::fabric_storage::FabricWritten, crate::fabric_storage::FabricIdentityHeld>, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.create.via-fabric-row",
        node_id = %node_id,
        fabric = %identity.name,
        fabric_id = %identity.fabric_id,
        moment = "write",
        outcome = tracing::field::Empty,
        holder = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let written = crate::fabric_storage::write_identity(&*storage, identity).await.map_err(|e| format!("fabric.storage did not answer the write of the fabric's row {} for Fabric {}: {e}", identity.name, identity.fabric_id));
    match &written {
        Ok(Ok(crate::fabric_storage::FabricWritten::Written)) => span.record("outcome", "row-written"),
        Ok(Ok(crate::fabric_storage::FabricWritten::Held)) => span.record("outcome", "row-held-by-fabric"),
        Ok(Err(held)) => span.record("outcome", "name-held").record("holder", tracing::field::display(&held.by)).record("detail", held.to_string().as_str()),
        Err(e) => span.record("outcome", "row-refused").record("detail", e.as_str()),
    };
    span.in_scope(|| tracing::info!("fabric.create: the fabric's row is written at its key; nothing else of the fabric is written until it is"));
    written
}

/// `fabric/` on the fabric's store root, written by the fabric-primary alone (R14 item 2): while this
/// birth holds the fabric-primary role it writes the Fabric identity it is configured for, so a role that
/// came to it from a lost primary still leaves the Fabric row on the store root. Who is primary is no `fabric/` row: it is the
/// election's result, recorded as `is_primary` and `is_fabric_primary` on the node rows (build.md
/// §5.3). The Fabric row carries no pointer to the newest Build (R-BI3). The write of the
/// fabric's row comes first: nothing else is written to `fabric/` until it is written, and a name another
/// Fabric's identity holds leaves `fabric/` unwritten by this writer, named once. While it does not
/// hold the role it writes nothing to `fabric/`.
pub(crate) struct FabricRowsWriter {
    /// This admin's exact birth: it writes only while the fabric-primary role names it.
    pub(crate) me: (NodeId, IncarnationId),
    pub(crate) identity: crate::fabric_storage::FabricIdentity,
    pub(crate) storage: Arc<dyn crate::fabric_storage::FabricStorage>,
    /// The write of the fabric's row: `None` before the store answered it; `Some(Ok)` once it is written;
    /// `Some(Err)` when another Fabric's identity holds the key (a held identity is never rewritten).
    pub(crate) written: Option<Result<(), crate::fabric_storage::FabricIdentityHeld>>,
}

impl FabricRowsWriter {
    pub(crate) fn new(me: (NodeId, IncarnationId), identity: crate::fabric_storage::FabricIdentity, storage: Arc<dyn crate::fabric_storage::FabricStorage>) -> Self {
        Self { me, identity, storage, written: None }
    }

    /// One sweep on the primary records `roles` holds: while the fabric-primary role names this birth, the write
    /// of the fabric's identity row, until the store answers it.
    pub(crate) async fn sweep(&mut self, primaries: &rdm_gossip::primary::PrimaryView) {
        if !primaries.fabric().is_some_and(|h| h.is_birth(&self.me.0, &self.me.1)) {
            return;
        }
        let (storage, identity) = (&self.storage, &self.identity);
        if self.written.is_none() {
            // A store that did not answer decides nothing: the next sweep asks again.
            self.written = write_fabric_row(&**storage, identity, &self.me.0).await.ok().map(|c| c.map(|_| ()));
        }
    }
}

/// The provider's inspection of the exact birth a held runtime or a durable row names
/// (node-runtime-inspect.md): the runtime fact is the one this admin holds of the birth (`held`: its
/// incarnation and what its join report or a topology read carried) or the row's, adopted in this
/// provider's control domain and inspected; its answer is node investigation's provider step.
pub async fn inspection_of(
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    held: Option<(&crate::model::IncarnationId, &rdm_core::NodeRuntime)>,
    row: Option<&crate::storage::NodeRecord>,
) -> Inspected {
    inspection_with_handle(provider, held, row).await.0
}

/// [`inspection_of`], with the exact runtime handle the provider was asked about beside the answer
/// (node-runtime-inspect.md Target; the inspect span's `handle`): `None` when no handle was adopted,
/// so nothing was asked.
pub async fn inspection_with_handle(
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    held: Option<(&crate::model::IncarnationId, &rdm_core::NodeRuntime)>,
    row: Option<&crate::storage::NodeRecord>,
) -> (Inspected, Option<DeploymentHandle>) {
    // The runtime fact one way for every inspect (`node_investigation::runtime_of`).
    let found = match held {
        Some((incarnation, r)) => crate::node_investigation::runtime_of(incarnation, Some(r), row),
        None => row.and_then(|r| crate::node_investigation::runtime_of(&r.incarnation, None, Some(r))),
    };
    let Some(crate::node_investigation::BirthRuntime { fact, data_dir, incarnation: birth }) = found else {
        return (Inspected::Unavailable(format!("no runtime fact: this admin holds {} of it and no node record names it", if held.is_some() { "a runtime without a fact" } else { "no runtime" })), None);
    };
    // The exact runtime handle the inspect asks about: none when the provider refuses to adopt the
    // fact, which the inspect names as its `unavailable`.
    let handle = crate::deployment::provider::adopt(provider, &fact).ok();
    (crate::deployment::provider::inspect_runtime(provider, &fact, data_dir.as_deref(), &birth.as_str()).await, handle)
}

/// The workflow a member's removal was announced by ([`RecoverySubmitter::removal_announced`]):
/// which announcement, and the operation and Build it names (a mesh.delete announced by its
/// mesh's status names its operation and no Build).
pub(crate) struct Announced {
    pub(crate) announced: &'static str,
    pub(crate) operation: rdm_core::OperationId,
    pub(crate) build_id: String,
}
