//! Durable node, mesh, and fabric row writers and claims.

use super::*;

/// Every birth of `fabric` this admin's book holds besides itself, heard a moment ago or only
/// carried in an installed topology read. These are the contacts a restarted admin can try, and the
/// births a maker serves a joining admin for a mesh it holds no snapshot of.
pub(crate) fn held_births(book: &DigestBook, fabric: &FabricId, me: &NodeId) -> Vec<MeshDigest> {
    book.all().into_iter().filter(|d| &d.fabric_id == fabric && &d.node.node_id != me).collect()
}

/// The contacts this admin drops from nodes.storage, of `written`, the contacts it holds, given
/// `held`, the births its book holds and `departed`, whether the book holds a node departed: a
/// contact whose node departed (node.delete's NodeDeleted, or mesh-left, which departs every birth
/// of the removed mesh: mesh-delete.md step 7, "Departed births resolve Gone"), and a contact whose
/// birth the book no longer holds, at a path another logical node now holds. A contact is a
/// bootstrap hint for a birth that may still answer; a departed one never does, and a maker's
/// stored map never hands it to a mesh created again under the same name (R-MN1).
pub(crate) fn dropped_contacts(written: &HashMap<NodeId, crate::storage::NodeRecord>, held: &[MeshDigest], departed: impl Fn(&NodeId) -> bool) -> Vec<NodeId> {
    written
        .values()
        .filter(|w| departed(&w.node_id) || (!held.iter().any(|d| d.node.node_id == w.node_id) && held.iter().any(|d| d.node.name == w.name)))
        .map(|w| w.node_id.clone())
        .collect()
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

/// A mesh's contacts and its Mesh row on the fabric's shared store root, written by that mesh's
/// primary alone, for its own members alone (R14 item 2; the writer ladder,
/// node-identity-and-state.md §4.3: a mesh row and its members' rows are that mesh's primary's). A
/// round that does not hold the seat writes nothing. A round that takes it first claims the mesh's
/// name ([`crate::storage::claim_mesh`]) and writes the Mesh row (node-identity-and-state.md
/// §2.7): nothing else of the mesh is written until the claim wins, and a name another mesh's live
/// row holds leaves the mesh's rows unwritten for the tenure, named. Then it reads what the shared
/// root holds for the mesh, writes each member whose held birth differs and drops a contact whose
/// node departed or whose path.name another node now holds (`dropped_contacts`). A contact is a
/// restart's hint, never topology.
pub(crate) struct MeshRowsWriter {
    /// The admin's `path.name`, the writer its spans name.
    pub(crate) me: String,
    pub(crate) fabric_id: FabricId,
    pub(crate) mesh: String,
    pub(crate) mesh_id: MeshId,
    pub(crate) nodes: Arc<dyn crate::storage::NodesStorage>,
    pub(crate) meshes: Arc<dyn crate::storage::MeshStorage>,
    /// What this seat tenure knows the shared root holds for the mesh's contacts; `None` outside a tenure.
    pub(crate) written: Option<HashMap<NodeId, crate::storage::NodeRecord>>,
    /// The claim of the mesh's name this seat tenure decided: `None` outside a tenure or before the
    /// store answered it; `Some(Ok)` once it won; `Some(Err)` when it was refused (another mesh's
    /// live row holds the name, or this mesh is Dead): the mesh's rows stay unwritten for the tenure.
    pub(crate) claim: Option<Result<(), crate::storage::MeshClaimRefused>>,
}

impl MeshRowsWriter {
    pub(crate) fn new(me: &str, fabric_id: FabricId, mesh: &str, mesh_id: MeshId, nodes: Arc<dyn crate::storage::NodesStorage>, meshes: Arc<dyn crate::storage::MeshStorage>) -> Self {
        Self { me: me.to_string(), fabric_id, mesh: mesh.to_string(), mesh_id, nodes, meshes, written: None, claim: None }
    }

    /// One round over the births `held` (every birth of the fabric this admin holds, itself
    /// included, each with the primaries its book holds for it), `seated` when this admin is its mesh's
    /// mesh-primary now, `departed` whether the book holds a node departed. Returns whether a row is
    /// owed: a write or a read the store did not acknowledge, which the caller retries on its own
    /// clock under the one backoff rule (node-identity-and-state.md §2.7), never waiting for a birth
    /// or a primary change.
    pub(crate) async fn round(&mut self, held: &[MeshDigest], seated: bool, departed: impl Fn(&NodeId) -> bool) -> bool {
        if !seated {
            self.written = None;
            self.claim = None;
            return false;
        }
        if self.claim.is_none() {
            // A store that did not answer decides nothing: the next round asks again.
            self.claim = claim_mesh_name(&*self.meshes, &self.me, &self.mesh, &self.mesh_id).await.ok().map(|c| c.map(|_| ()));
            if self.claim.is_none() {
                return true;
            }
        }
        if !matches!(self.claim, Some(Ok(()))) {
            return false;
        }
        if self.written.is_none() {
            match self.nodes.contacts().await {
                Ok(rows) => self.written = Some(rows.into_iter().filter(|c| c.name.mesh == self.mesh).map(|c| (c.node_id.clone(), c)).collect()),
                Err(e) => {
                    tracing::info!(mesh = %self.mesh, error = %e, "the mesh's contacts could not be read; none is written this round");
                    return true;
                }
            }
        }
        let mut owed = false;
        let written = self.written.as_mut().expect("read above");
        // A departed birth is never written: its contact is dropped below and stays dropped.
        let members: Vec<MeshDigest> = held.iter().filter(|d| d.node.name.mesh == self.mesh && !departed(&d.node.node_id)).cloned().collect();
        for d in &members {
            let r = crate::storage::NodeRecord {
                node_id: d.node.node_id.clone(),
                fabric_id: self.fabric_id.clone(),
                name: d.node.name.clone(),
                mesh_id: self.mesh_id.clone(),
                incarnation_id: d.node.incarnation.clone(),
                endpoint_id: d.node.endpoint_id.clone(),
                transport_addr: d.node.transport_addr,
                listeners: Vec::new(),
                is_primary: d.is_primary,
                is_fabric_primary: d.is_fabric_primary,
            };
            // The contact is what is held, put whole, the primaries recorded on it; the declared state
            // and the silence status are their own rows in status.storage, never carried on it. It is
            // marked written only on the store's acknowledgement.
            if written.get(&r.node_id) == Some(&r) {
                continue;
            }
            match self.nodes.put_contact(&r).await {
                Ok(()) => {
                    written.insert(r.node_id.clone(), r);
                }
                Err(e) => {
                    owed = true;
                    tracing::info_span!("rdm.node_admin.node.update.via-node-row", node = %self.me, subject = %r.name, node_id = %r.node_id, is_primary = ?r.is_primary, is_fabric_primary = ?r.is_fabric_primary, outcome = "owed", error = %e)
                        .in_scope(|| tracing::info!("the store did not acknowledge the node's row: it is owed and written again under the one backoff rule"));
                }
            }
        }
        let dropped: Vec<crate::storage::NodeRecord> = dropped_contacts(written, &members, &departed).into_iter().filter_map(|id| written.get(&id).cloned()).collect();
        for r in dropped {
            if self.nodes.remove_contact(&r.name.mesh, &r.node_id).await.is_ok() {
                written.remove(&r.node_id);
            } else {
                owed = true;
            }
        }
        owed
    }
}

/// The mesh rows writer's loop: a round over what `book` holds of `fabric` each time a birth or a
/// primary record changes there or `own_seat` (whether this admin is its mesh's mesh-primary) moves,
/// each birth carrying the primaries the book holds for it. A row the store did not acknowledge is
/// owed: the round runs again on this writer's own clock under the one backoff rule (node-rpc.md §43),
/// never waiting for a birth, a digest or a primary change (node-identity-and-state.md §2.7). Each
/// round takes its turn of `park`.
pub(crate) async fn keep_mesh_rows(mut writer: MeshRowsWriter, book: DigestBook, fabric: FabricId, mut own_seat: tokio::sync::watch::Receiver<bool>, park: crate::stop_gate::StopGate) {
    let mut births = book.birth_changes();
    let mut primaries = book.seats.subscribe();
    let pacing = rdm_transport::delivery::Pacing::from_cadences();
    let mut owed_rounds = 0u32;
    loop {
        // Held here while the admin is parked: its own wait, outside every lock.
        let turn = park.turn("mesh-rows").await;
        births.borrow_and_update();
        primaries.borrow_and_update();
        let seated = *own_seat.borrow_and_update();
        let held: Vec<MeshDigest> = book
            .all()
            .into_iter()
            .filter(|d| d.fabric_id == fabric)
            .map(|mut d| {
                book.recorded_primaries(&mut d);
                d
            })
            .collect();
        let owed = writer.round(&held, seated, |id| book.is_departed(id.as_str())).await;
        owed_rounds = if owed { owed_rounds + 1 } else { 0 };
        drop(turn);
        let again = async {
            if owed_rounds == 0 {
                std::future::pending::<()>().await
            }
            tokio::time::sleep(pacing.after(owed_rounds)).await
        };
        tokio::select! {
            changed = births.changed() => if changed.is_err() { return },
            changed = primaries.changed() => if changed.is_err() { return },
            changed = own_seat.changed() => if changed.is_err() { return },
            () = again => {}
        }
    }
}

/// Hold the primaries the node rows of `fabric` on the store root record (`is_primary`,
/// `is_fabric_primary`), the own row's included: an election input (elections §3), each held only when
/// it supersedes the record held. The rows record the role; the election decides it.
pub(crate) async fn take_recorded_primaries(nodes: &dyn crate::storage::NodesStorage, fabric: &FabricId, seats: &rdm_transport::membership::SeatBook) -> Result<usize, crate::storage_error::StorageError> {
    let mut rows = nodes.contacts().await?;
    rows.extend(nodes.own().await?);
    let mut taken = 0;
    for r in rows.iter().filter(|r| &r.fabric_id == fabric) {
        let record = |revision| rdm_core::Primary { mesh: r.name.mesh.clone(), node_id: r.node_id.clone(), incarnation: r.incarnation_id.clone(), revision };
        if let Some(revision) = r.is_primary {
            taken += usize::from(matches!(seats.take(rdm_core::Seat::of_cohort(r.name.kind), &record(revision)), rdm_transport::membership::SeatTaken::Held { .. }));
        }
        if let Some(revision) = r.is_fabric_primary {
            taken += usize::from(matches!(seats.take(rdm_core::Seat::FabricPrimary, &record(revision)), rdm_transport::membership::SeatTaken::Held { .. }));
        }
    }
    Ok(taken)
}

/// The Mesh row of the mesh a restarting node-admin's own row names, read by that mesh's id (R-MN1: a
/// mesh created again under the same name is a new mesh with a new id; R-ML2: a dead mesh is never
/// restarted): the point read at the mesh's key, the name, is taken as the admin's mesh only when it
/// is the row of the mesh id its own row holds, and live. A row of another MeshId at the name means
/// the admin's mesh was removed and a mesh created again under the name; refused by name, naming
/// both ids, as are a Dead row and no row.
pub(crate) async fn own_mesh_row(meshes: &dyn crate::storage::MeshStorage, own: &crate::storage::NodeRecord) -> Result<crate::storage::MeshRecord, String> {
    let (mesh, mesh_id) = (&own.name.mesh, &own.mesh_id);
    match meshes.mesh(mesh).await.map_err(|e| format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage could not read the row of {mesh}: {e}", own.name))? {
        None => Err(format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage holds no row of {mesh}", own.name)),
        Some(row) if row.mesh_id != *mesh_id => Err(format!(
            "this admin ({}) belongs to mesh {mesh_id} ({mesh}), which was removed: the name {mesh} is held by mesh {}, a mesh created again under it, and a removed mesh is never rejoined",
            own.name, row.mesh_id
        )),
        Some(row) if row.dead => Err(format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), whose row is Dead: a dead mesh is never restarted", own.name)),
        Some(row) => Ok(row),
    }
}

/// mesh.create's claim of the mesh's name at its key (node-identity-and-state.md §2.7)
/// ([`crate::storage::claim_mesh`]), made by `me`: the
/// fabric-primary at CreateMesh, as the writer of a mesh with no primary (R13), the mesh's first
/// durable write; and the mesh's primary when its seat tenure starts, which finds the mesh's own row
/// (the same MeshId re-claiming, idempotent). The Mesh row written, or this mesh's own row found
/// standing, or refused by name. One `rdm.node_admin.mesh.create.via-mesh-row` span, `moment`
/// `claim`. `Err`: the store did not answer, and nothing is decided.
pub(crate) async fn claim_mesh_name(meshes: &dyn crate::storage::MeshStorage, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<Result<crate::storage::MeshClaim, crate::storage::MeshClaimRefused>, String> {
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
    let record = crate::storage::MeshRecord { mesh_id: mesh_id.clone(), name: mesh.to_string(), dead: false };
    let claimed = crate::storage::claim_mesh(&*meshes, &record).await.map_err(|e| format!("{me}: mesh.storage did not answer the claim of {mesh} for mesh {mesh_id}: {e}"));
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
    match claim_mesh_name(meshes, me, mesh, mesh_id).await {
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
/// birth holds the fabric seat it writes the Fabric identity it is configured for, so a seat that
/// came to it before the fabric's first state-commit, from a holder whose held rows were dropped,
/// still leaves the Fabric row on the store root. Who is primary is no `fabric/` row: it is the
/// election's result, recorded as `is_primary` and `is_fabric_primary` on the node rows (build.md
/// §5.3). The Fabric row carries no pointer to the newest Build (R-BI3). The claim of the
/// fabric's name comes first: nothing else is written to `fabric/` until it wins, and a name another
/// Fabric's identity holds leaves `fabric/` unwritten by this writer, named once. While it does not
/// hold the seat it writes nothing to `fabric/`.
pub(crate) struct FabricRowsWriter {
    /// This admin's exact birth: it writes only while the fabric seat names it.
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

    /// One round on the seat records `seats` holds: while the fabric seat names this birth, the claim
    /// of the fabric's name (which writes the identity row) until the store answers it.
    pub(crate) async fn round(&mut self, seats: &rdm_transport::membership::SeatBook) {
        if !seats.fabric().is_some_and(|h| h.is_birth(&self.me.0, &self.me.1)) {
            return;
        }
        let (storage, identity) = (&self.storage, &self.identity);
        if self.claim.is_none() {
            // A store that did not answer decides nothing: the next round asks again.
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
        return (Inspected::Unavailable(format!("no runtime fact: the book holds {} for it and no durable row names it", if held_row.is_some() { "a digest without a runtime" } else { "no digest of this birth" })), None);
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
