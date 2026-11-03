//! The two sides of the mesh-delete workflow a runner executes (mesh-delete.md).
//!
//! The owner's side ([`AdminRunner::delete_mesh`]) is the fabric-primary: it owns the outer
//! workflow `mesh.delete:<mesh_id>` once the fabric-primary role is outside the deleting mesh (the
//! confirmed node.fabric-primary.update, `primary_update`). It captures the accepted roster and the
//! final primary's provider access (refusing before anything is dismantled when it has none),
//! publishes `mesh-leaving`, sends `DeleteMesh`, waits for the validated `MeshDeleted` handoff
//! (mesh.deleted) and records its completion receipt: the mesh.delete ends there, the final primary
//! alive (steps 5-6). The final primary's end is a separate node.delete with the fabric-primary as
//! executor (step 7); once this admin publishes that birth deleted from its exact exited verdict
//! ([`AdminRunner::mesh_left_after_delete`]) every birth is proven exited and it records `MeshLeft`
//! and publishes mesh-left (R-MD1 items 2-3).
//! The deleting mesh's primary's side ([`AdminRunner::admit_delete_mesh`]) admits `DeleteMesh`, runs the
//! member workflows (every ordinary node in parallel, then every other node-admin in parallel),
//! records one exit manifest as a step receipt of the accepted Build and calls `MeshDeleted` up to
//! the owner, still running.

use super::*;
use crate::build_state::{StepOutcome};
use crate::executor::RunEnd;
use crate::deployment::pipeline::{RuntimeIdentity, TerminalReceipt};
use crate::mesh_delete::{accepted_roster, manifest_reference, members_exited, Admitted, ExitManifest, MeshDeleteKey, MeshDeleter, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_MESH_DELETED, STEP_MESH_LEFT};
use rdm_core::status::{NotPrimary, Status, StatusReply, StatusRequest};
use tracing::Instrument;

/// What serves `DeleteMesh` and `MeshDeleted` for a running admin.
pub struct MeshDeleteDoor {
    pub(super) runner: Arc<AdminRunner>,
}

#[async_trait::async_trait]
impl MeshDeleter for MeshDeleteDoor {
    async fn delete_mesh(&self, peer: Option<iroh::PublicKey>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        self.runner.clone().admit_delete_mesh(peer, sender, req).await
    }
    async fn mesh_deleted(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        self.runner.validate_handoff(sender, req).await
    }
    async fn birth_deleted(&self, op: &rdm_core::LifecycleOp) {
        self.runner.mesh_left_after_delete(op).await
    }
}

impl AdminRunner {
    /// The runtime fact the view holds for `node` (its digest).
    fn held_runtime(&self, node: &Node) -> Option<rdm_core::RuntimeFact> {
        self.digest_view.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref()).and_then(|d| d.node.runtime)
    }

    /// The record this admin's topology view holds of the exact roster birth `b`: its digest, live or
    /// stopped, as the view projects a digest (the leave's records, [`crate::mesh_delete::roster_records`]).
    fn held_record(&self, b: &RosterBirth) -> Option<Node> {
        let d = self.digest_view.get(&b.node_id).map(|(d, _)| d).or_else(|| self.digest_view.stopped_digests().into_iter().find(|d| d.node.node_id.as_str() == b.node_id)).filter(|d| d.node.incarnation.as_str() == b.incarnation_id)?;
        let mut n = Node::allocated(d.node.location(), d.node.mesh.clone(), &d.node.r#type);
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(self.provider.kind());
        n.status = crate::admin::node_status(d.status);
        n.routable = false;
        n.admin_api_base = d.admin_api_base.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        Some(n)
    }

    /// The owner's outer workflow `mesh.delete:<mesh_id>`.
    pub(super) async fn delete_mesh(&self, build_id: &crate::build::BuildId, mesh: &str, mesh_id: &MeshId) -> Result<(), crate::executor::RunEnd> {
        let outer = crate::mesh_delete::operation(mesh_id);
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-shutdown", mesh = %mesh, mesh_id = %mesh_id, build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, outcome = tracing::field::Empty);
        let result = self.delete_mesh_in(build_id, mesh, mesh_id, &outer).instrument(span.clone()).await;
        span.record("outcome", if result.is_ok() { "complete" } else { "incomplete" });
        result
    }

    async fn delete_mesh_in(&self, build_id: &crate::build::BuildId, mesh: &str, mesh_id: &MeshId, outer: &rdm_core::OperationId) -> Result<(), crate::executor::RunEnd> {
        // Where this owner resumes, from its own Build facts, before it calls anything; the run this
        // attempt belongs to keys the fabric-primary role's update (ruling R-PU2).
        let build = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its events: {e}"))?;
        let steps = build.steps.clone();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        // The view is projected now: a primary an earlier attempt deleted holds no role here, so the
        // admin created for its mesh, or the one that survived it, is read in its role.
        self.refresh_view().await;
        let view = self.topology.read().await.clone();
        match crate::mesh_delete::owner_resume(&steps, &outer.to_string()) {
            crate::mesh_delete::OwnerResume::Left => {
                // Resumed after the departure was recorded: the receipt is the record.
                self.forget_mesh_id(mesh);
                return Ok(());
            }
            crate::mesh_delete::OwnerResume::Deleted => {
                // Resumed after mesh.deleted's completion was recorded: the mesh.delete ended, its
                // final primary alive; nothing is called. Its end is its own node.delete.
                return Ok(());
            }
            crate::mesh_delete::OwnerResume::Lead => {}
        }
        let node_rpc = &self.node_rpc;
        // Step 1 (lifecycles/mesh-delete.md): this owner is the fabric-primary. When the departing mesh
        // held the fabric-primary, it moved first by node.fabric-primary.update with the mesh excluded:
        // the leave opens only once this admin's view names a fabric-primary outside the mesh
        // ("`node.fabric-primary.updated` confirms the committed transfer before any member is
        // deleted"). During fabric.delete, on
        // the fabric-primary's own mesh, the fabric-primary role stays put so mesh.delete skips its
        // node.fabric-primary.update step (lifecycles/fabric-delete.md step 1).
        if !build.topology.meshes.is_empty() || mesh != self.template.launcher.mesh.as_str() {
            let fabric_update = crate::primary_update::update_operation(rdm_core::PrimaryRole::FabricPrimary, outer);
            self.primary_update.fabric_primary_updated(mesh, &fabric_update).await.map_err(|e| format!("{outer:?} is refused before {mesh} is dismantled: {e}"))?;
        }
        // The workflow is handed to a live primary. A primary that is not live keeps its role (R-EL1)
        // but cannot take the workflow: it goes to the lost-final-primary path below, which deletes it
        // before anything is amended (R11). During fabric.delete, an unanswered mesh with no prior
        // AcceptedRoster resolves by deleting its unreached births (R-FS1 revised).
        let primary = match view.node_type_primary(mesh, "node-admin").filter(|p| p.status.is_live()).cloned() {
            Some(p) => p,
            None => {
                // Under fabric.delete (its plan names no mesh), a mesh with no live node-admin gains none: it
                // is deleted in process at this executor, which holds the fabric-primary's roles
                // (mesh-delete.md "Resume after the primary's loss" step 2, Q160; fabric-delete.md step 1).
                if build.topology.meshes.is_empty() {
                    return self.delete_mesh_in_process(build_id, mesh, &key, &steps, &view).await;
                }
                self.admin_for_deleting_mesh(build_id, mesh, &key, &steps, &view).await?
            }
        };
        // A resumed leave whose final primary was lost (mesh-delete.md "Resume after the primary's loss"):
        // every other node-admin of the mesh's records this view holds silent is deleted before the
        // workflow is handed to the primary the election names now (D67: the roster is the records).
        if build.completed(&outer.to_string(), STEP_ACCEPTED_ROSTER) {
            let held = RosterBirth::of(&primary)?;
            let roster = self.stored_roster(mesh, mesh_id, &steps).await.map_err(|e| format!("{outer}: {e}"))?;
            for other in roster.iter().filter(|b| **b != held && rdm_core::is_node_admin(&b.r#type) && b.mesh == mesh) {
                let live = view.births().any(|n| n.node_id.as_str() == other.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == other.incarnation_id) && n.status.is_live());
                if !live {
                    self.delete_silent_admin(build_id, &key, mesh, other, &steps).await?;
                }
            }
            self.refresh_view().await;
        }
        // The final primary's provider domain: access before anything is dismantled
        // (its node.delete, executed by this fabric-primary, proves its exit through it).
        let (_, final_handle) = self.handle_for(&primary).await.map_err(|e| format!("{outer:?} is refused before {mesh} is dismantled: no provider proof path for its final primary: {e}"))?;
        let final_fact = final_handle.fact().ok_or_else(|| format!("{outer:?} is refused before {mesh} is dismantled: the final primary {} has no exact runtime handle", primary.node_id))?;
        let (final_primary, final_runtime) = (RosterBirth::of(&primary)?, RuntimeIdentity::of(&final_fact));
        // The roster is the mesh's node records at the step, never the view, which drops a birth once it
        // is gone: a member not yet Deleted is a birth of the roster and gets its node.delete (fabric-delete.md
        // "Failure"); a birth this Build's delete deleted keeps its Deleted record (D67).
        let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its events: {e}"))?.steps;
        let stored = self.stored_roster(mesh, mesh_id, &steps).await.map_err(|e| format!("{outer}: {e}"))?;
        let roster = accepted_roster(&*self.builds, &key, STEP_ACCEPTED_ROSTER, &self.node_id.to_string(), || Ok(stored)).await?;
        let admitted = Admitted { roster, final_primary, final_runtime };
        if !admitted.roster.contains(&admitted.final_primary) {
            return Err(format!("{outer:?} is refused before {mesh} is dismantled: its primary {} {} is not a birth of the roster accepted at admission", admitted.final_primary.node_id, admitted.final_primary.incarnation_id).into());
        }
        let handoff = self.leaves.open(key.clone(), admitted.clone());
        let result = self.lead_delete(build_id, mesh_id, outer, node_rpc, &primary, &key, handoff).await;
        if result.is_err() {
            // This attempt's open leave ends with it; the receipts are the record a resumed attempt reads.
            self.leaves.close(&key);
        }
        result
    }

    /// Under an in-force fabric.delete, the mesh `mesh` with no live node-admin primary is deleted in
    /// process at this executor, the fabric-primary, and no node-admin is created for
    /// it (mesh-delete.md "Resume after the primary's loss" step 2, Q160): its roster is the AcceptedRoster
    /// receipt, captured once, from the mesh's node rows on the store, by the first executor that runs it
    /// and read by every later one; every birth without a terminal receipt is deleted, a live member by its
    /// ordinary node.delete (node.stop, the exact exited verdict), this admin its executor, every other birth as
    /// an unreached birth (its runtime read from its durable row, `TerminateRuntime` first when it runs), and
    /// a delete holding its exited verdict runs on from its receipts. A runtime this admin cannot inspect or
    /// terminate ends the step by name, no receipt manufactured. Then mesh.deleted's and mesh-left's receipts
    /// are recorded and the mesh leaves this view. A live node-admin of the mesh not yet its primary is no
    /// loss: the attempt ends and the next one hands that admin the workflow.
    async fn delete_mesh_in_process(&self, build_id: &crate::build::BuildId, mesh: &str, key: &MeshDeleteKey, steps: &[crate::build_state::BuildStep], view: &Topology) -> Result<(), RunEnd> {
        let outer = &key.operation;
        if let Some(live) = view.nodes_of_type(mesh, "node-admin").find(|n| n.status.is_live()) {
            return Err(RunEnd::Incomplete(format!("{outer:?}: {mesh}'s admin {} is live and holds no mesh-primary role yet, so it is not lost; the next attempt hands the workflow to the admin that holds it", live.node_id)));
        }
        let span = tracing::info_span!("rdm.node_admin.mesh.delete.via-in-process", node_path = %self.held_name().path, node_name = %self.held_name().name, mesh = %mesh, mesh_id = %key.mesh_id, build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, roster = tracing::field::Empty, outcome = tracing::field::Empty);
        let r = self.delete_mesh_in_process_in(build_id, mesh, key, steps, view).instrument(span.clone()).await;
        span.record("outcome", if r.is_ok() { "deleted" } else { "incomplete" });
        span.in_scope(|| tracing::info!("under fabric.delete the mesh with no node-admin is deleted in process at the executor; no node-admin is created"));
        r
    }

    async fn delete_mesh_in_process_in(&self, build_id: &crate::build::BuildId, mesh: &str, key: &MeshDeleteKey, steps: &[crate::build_state::BuildStep], view: &Topology) -> Result<(), RunEnd> {
        let outer = &key.operation;
        let me = self.node_id.to_string();
        let live: Vec<Node> = view.nodes.iter().filter(|n| n.mesh == mesh && !rdm_core::is_node_admin(&n.r#type) && n.status.is_live()).cloned().collect();
        // The roster is captured once and never recaptured (mesh-delete.md "Resume after the primary's
        // loss"): the AcceptedRoster receipt an earlier executor recorded is read; with none recorded, the
        // capture has not happened yet and this one is the first, taken from the mesh's node rows on the
        // store, never from the view, which drops a birth once it is gone (fabric-delete.md "Failure").
        let stored = self.stored_roster(mesh, &key.mesh_id, steps).await.map_err(|e| format!("{outer}: {e}"))?;
        tracing::Span::current().record("roster", stored.len());
        let roster = accepted_roster(&*self.builds, key, STEP_ACCEPTED_ROSTER, &me, || Ok(stored)).await?;
        let is_admin = |b: &RosterBirth| rdm_core::is_node_admin(&b.r#type);
        let final_primary = roster.iter().find(|b| is_admin(b)).or(roster.first()).cloned().ok_or_else(|| format!("{outer}: the store holds no node row of {mesh} to delete"))?;
        let final_runtime = self.roster_birth_runtime(&final_primary).await.ok();
        // Each birth by its tier (ordinary nodes, then node-admins), running only the deletes without a
        // terminal receipt: a delete with every step's receipt stands; one that holds its exited verdict and not
        // every later step runs on from its receipts; a live member by its ordinary node.delete; every other
        // birth as an unreached birth.
        let is_admin = |b: &RosterBirth| rdm_core::is_node_admin(&b.r#type);
        for admins in [false, true] {
            for b in roster.iter().filter(|b| is_admin(b) == admins) {
                let held = self.builds.read_build(build_id).await.map_err(|e| format!("{outer:?}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
                if crate::mesh_delete::finished_delete(&held, b) {
                    continue;
                }
                if crate::mesh_delete::exit_receipts(&held, std::slice::from_ref(b)).contains_key(&(b.node_id.clone(), b.incarnation_id.clone())) {
                    self.delete_lost_birth(build_id, b).await?;
                    continue;
                }
                match live.iter().find(|n| n.node_id.as_str() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id)) {
                    Some(n) => {
                        self.delete_member_in_process(build_id, n).await?;
                    }
                    None => {
                        // An admin node investigation reaches is not lost (Q111): the next attempt hands it the
                        // workflow, and nothing of it is deleted here.
                        let seen = view.nodes_of_type(mesh, "node-admin").find(|n| n.node_id.as_str() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id)).cloned();
                        if let Some(n) = seen {
                            if matches!(crate::path_investigation::PathProbe::investigate(self, &n).await, crate::node_investigation::Finding::Reached { .. }) {
                                return Err(RunEnd::Incomplete(format!("{outer:?}: {mesh}'s node-admin {} {} answers, so it is not lost; the next attempt hands the workflow to the admin the election names", b.node_id, b.incarnation_id)));
                            }
                        }
                        // Every other birth as an unreached birth: its runtime resolved as node investigation
                        // resolves it and inspected at TerminateRuntime's instant; a runtime not resolved here
                        // fails that step by name (Q160).
                        self.delete_lost_birth(build_id, b).await?;
                    }
                }
            }
        }
        let now = self.builds.read_build(build_id).await.map_err(|e| format!("{outer:?}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
        let held = crate::mesh_delete::exit_receipts(&now, &roster);
        if let Some(b) = roster.iter().find(|b| !held.contains_key(&(b.node_id.clone(), b.incarnation_id.clone()))) {
            return Err(RunEnd::Incomplete(format!("{outer:?}: {} {} of {mesh} holds no exit receipt after its delete in process: it is never reported deleted", b.node_id, b.incarnation_id)));
        }
        let node_rows = self.records.node_rows();
        crate::mesh_delete::deleted_in_process(&*self.builds, &self.records, node_rows.as_deref(), &*self.mesh_storage, key, &me, mesh, roster, &final_primary, final_runtime, held.into_values().collect()).await?;
        self.left_in_process(build_id, mesh, key).await;
        Ok(())
    }

    /// The roster of `mesh` (`mesh_id`) captured from its node rows on the store
    /// ([`crate::mesh_delete::stored_roster`]). A birth a create's claim allocated and no fact of which is
    /// written is a birth of the roster while the provider finds its launch running, held here by its exact
    /// handle ([`AdminRunner::claimed_launch`]); a launch the provider proves never started, or started and
    /// exited, left no runtime for the delete to delete, and its birth is no birth of the roster. A launch
    /// that cannot be found refuses the capture by name.
    async fn stored_roster(&self, mesh: &str, mesh_id: &MeshId, steps: &[crate::build_state::BuildStep]) -> Result<Vec<RosterBirth>, String> {
        let store = self.records.contacts.get().ok_or_else(|| format!("{} holds no nodes.storage to read the node rows of {mesh} from", self.node_id))?;
        let rows = store.nodes(mesh).await.map_err(|e| format!("the node rows of {mesh} could not be read: {e}"))?;
        let mut roster = crate::mesh_delete::stored_roster(&rows, mesh, mesh_id, steps);
        for row in rows.iter().filter(|r| r.births.is_empty() && r.allocated.is_some()) {
            let Some(b) = roster.iter().find(|b| b.node_id == row.node_id.to_string()).cloned() else { continue };
            let birth = b.birth().map_err(|e| format!("the roster birth {}: {e}", b.node_id))?;
            match self.claimed_launch(&b.record()?, &birth).await? {
                Some((crate::deployment::provider::Found::Running(_), _)) | None => {}
                Some((found, _)) => {
                    tracing::info_span!("rdm.node_admin.mesh.resolve.via-claimed-birth-gone", mesh = %mesh, mesh_id = %mesh_id, member = %b.node_id, node_id = %b.node_id, incarnation_id = %b.incarnation_id, found = found.name())
                        .in_scope(|| tracing::info!("the launch the create's claim records left no runtime: its birth is no birth of the roster"));
                    roster.retain(|x| *x != b);
                }
            }
        }
        Ok(roster)
    }

    /// The exact runtime of the roster birth `b`, by fingerprint: the runtime this admin resolves for the
    /// birth (its digest, its durable runtime row).
    async fn roster_birth_runtime(&self, b: &RosterBirth) -> Result<RuntimeIdentity, String> {
        let (_, handle) = self.handle_for(&b.record()?).await?;
        handle.fact().map(|f| RuntimeIdentity::of(&f)).ok_or_else(|| format!("{} {} has no exact runtime handle", b.node_id, b.incarnation_id))
    }

    /// The live member `n` of a mesh deleted in process: its ordinary node.delete (node.stop, the exact
    /// exited verdict, its row Deleted), this admin its executor. Answers its terminal receipt.
    async fn delete_member_in_process(&self, build_id: &crate::build::BuildId, n: &Node) -> Result<TerminalReceipt, RunEnd> {
        let (record, handle) = self.handle_for(n).await.map_err(|e| format!("node.delete:{}: no exact runtime of the live member to delete in process: {e}", n.node_id))?;
        let operation = rdm_core::OperationId::node("node.delete", &record.node_id);
        self.delete_birth(build_id, record, handle, &operation).await
    }

    /// After a mesh is deleted in process: its births depart this view, its deletion is held, and its
    /// MeshId is forgotten here.
    async fn left_in_process(&self, build_id: &crate::build::BuildId, mesh: &str, key: &MeshDeleteKey) {
        self.digest_view.depart_mesh_births(mesh, build_id.as_str(), self.lifecycle_events.now_rafka_ms());
        self.records.hold_mesh_deleted(&key.mesh_id);
        self.forget_mesh_id(mesh);
        self.refresh_view().await;
    }

    /// The leaving mesh holds no node-admin primary. Before admission that refuses the leave; after
    /// it, with no admin of the mesh left live, the fabric-primary resumes the leave through the one
    /// node-admin it creates for the mesh (mesh-delete.md, "Resume after the primary's loss", steps
    /// 1-2): the lost final primary is deleted first, `TerminateRuntime` first when it runs; then the
    /// admin is created at a path no birth held,
    /// into the existing mesh under its existing MeshId. A live admin not yet in its role is no loss:
    /// the attempt ends and the next one hands that admin the workflow.
    async fn admin_for_deleting_mesh(&self, build_id: &crate::build::BuildId, mesh: &str, key: &MeshDeleteKey, steps: &[crate::build_state::BuildStep], view: &Topology) -> Result<Node, RunEnd> {
        let outer = &key.operation;
        let no_primary = format!("{outer:?} is refused: {mesh} holds no node-admin primary to hand the workflow to");
        // Before admission the leave is refused by name: nothing was dismantled and nothing is resumed.
        if !steps.iter().any(|s| s.operation == outer.to_string() && s.step == STEP_ACCEPTED_ROSTER && s.outcome == StepOutcome::Complete) {
            return Err(RunEnd::failed(no_primary));
        }
        let roster = self.stored_roster(mesh, &key.mesh_id, steps).await.map_err(|e| format!("{outer}: {e}"))?;
        if let Some(live) = view.nodes_of_type(mesh, "node-admin").find(|n| n.status.is_live()) {
            return Err(RunEnd::Incomplete(format!("{no_primary}: its admin {} is live and holds no primary role yet, so it is not lost; the next attempt hands the workflow to the admin primary", live.node_id)));
        }
        // None survives (mesh-delete.md "Resume after the primary's loss" steps 1-2): every node-admin of
        // the mesh's records, silent in this view, is deleted before anything is created for the mesh, the
        // lost final primary among them; the first names the executable the created admin runs.
        let admins: Vec<&RosterBirth> = roster.iter().filter(|b| rdm_core::is_node_admin(&b.r#type) && b.mesh == mesh).collect();
        let lost = admins.first().map(|b| (*b).clone()).ok_or_else(|| RunEnd::failed(format!("{no_primary}: its records hold no node-admin")))?;
        for other in &admins {
            self.delete_silent_admin(build_id, key, mesh, other, steps).await?;
        }
        // The lost primary's departure vacates its role in this view before the name of the admin
        // created for the mesh is claimed: the claim's writer is the fabric-primary only while the
        // mesh has no primary (R11: the predecessor's delete first).
        self.refresh_view().await;
        let held: Vec<PathName> = view.nodes.iter().map(|n| n.location()).collect();
        let path = crate::mesh_delete::leaving_mesh_admin_path(mesh, &roster, &held)?;
        let (node_path, node_name) = (&path.path, &path.name);
        let span = tracing::info_span!("rdm.node_admin.node.create.via-leaving-mesh", mesh = %mesh, mesh_id = %key.mesh_id, node_path = %path.path, node_name = %path.name, build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, predecessor = %format!("{} {}", lost.node_id, lost.incarnation_id), outcome = tracing::field::Empty);
        let created = self.create_deleting_mesh_admin(build_id, mesh, &path, &key.mesh_id, &lost).instrument(span.clone()).await;
        span.record("outcome", if created.is_ok() { "created" } else { "failed" });
        created.map_err(|e| format!("{outer:?}: the node-admin path={node_path:?} name={node_name:?} created for {mesh}, which has no live admin: {e}"))?;
        self.refresh_view().await;
        let now = self.topology.read().await.clone();
        now.node_type_primary(mesh, "node-admin").cloned().ok_or_else(|| RunEnd::Incomplete(format!("{outer:?}: path={node_path:?} name={node_name:?} was created for {mesh} and holds no mesh-primary role yet; the next attempt amends the roster for it")))
    }

    /// node.create of `path`, the one node-admin of a leaving mesh no admin of which survives, into
    /// the EXISTING mesh (mesh-recovery.md: "it joins under the EXISTING MeshId and never mints
    /// another"): the mesh's issuing material, as any mesh's only admin carries, and the mesh's
    /// CURRENT state as this fabric-primary holds it, handed before its Ready like any mesh's first
    /// admin is handed its mesh's state. Never Pending for a mesh past it: the state only moves
    /// forward (states.md, "pending -> ready -> leaving -> dead"), and a mesh that is
    /// leaving is never told it is pending. A fabric-primary that holds no state for the mesh
    /// creates nothing: no state is assumed. The admin runs the executable the row of the lost
    /// predecessor `predecessor` (the amendment's `predecessor`) records: the create is the mesh's
    /// recovery (build.md, "One node.create is recovery"), and a recovery runs the executable its
    /// exited birth's row records, never the recovering admin's (node-recovery.md "Retained", Q157).
    /// A predecessor whose row records none refuses the create by name; nothing is launched.
    async fn create_deleting_mesh_admin(&self, build_id: &crate::build::BuildId, mesh: &str, path: &PathName, mesh_id: &MeshId, predecessor: &RosterBirth) -> Result<(), String> {
        let (node_path, node_name) = (&path.path, &path.name);
        let state = self.records.declared.lock().unwrap().meshes.get(mesh_id).copied().ok_or_else(|| {
            format!("{}: this fabric-primary holds no declared state for {} ({mesh_id}), so the admin created for it could be handed none; nothing is created under an assumed state", self.node_id, mesh)
        })?;
        let executable = self.predecessor_executable(path, predecessor).await?;
        match self.revise_predecessor(path).await {
            crate::path_investigation::TargetOutcome::Clear { gone: None } => {}
            other => return Err(format!("path={node_path:?} name={node_name:?} is not a path no birth held: the create door targeted {other:?}")),
        }
        let hand_off = self.mesh_state_hook(mesh, state)?;
        let template = self.with_issuing_material(self.template_bound(path, mesh, "node-admin", executable, "row").await?, mesh).await?;
        let held_runtimes = self.held_runtimes().await;
        // The admin is a new node.id at the path, minted when the fabric-primary decides the create.
        let born = crate::model::NodeId::mint();
        let req = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &born), node_id: born.clone(), mesh: mesh.to_string(), r#type: "node-admin".into(), build_id: Some(build_id.clone()), path: path.path.clone(), name: path.name.clone(), spec: self.listeners.spec_for("node-admin"), data_dir: None, held_runtimes, entry: None };
        let created = self.pipeline(&template).create_with(&req, Some(hand_off), None).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(created.node.node_id.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The executable the row of `predecessor`, the lost final primary the admin `path` replaces,
    /// records (read by its node id: the `Deleted` row keeps it). A row that records none, or no row,
    /// refuses the create by name.
    async fn predecessor_executable(&self, path: &PathName, predecessor: &RosterBirth) -> Result<rdm_core::binding::Binding, String> {
        let (node_path, node_name) = (&path.path, &path.name);
        let node_id = NodeId::parse(&predecessor.node_id).map_err(|e| format!("the roster names {} by node id `{}`, which does not parse: {e}", predecessor.node_id, predecessor.node_id))?;
        let store = self.records.contacts.get().ok_or_else(|| format!("node.create:path={node_path:?} name={node_name:?}: {} holds no node rows, so the executable of the predecessor {} {} cannot be read; nothing is launched", self.node_id, predecessor.node_id, predecessor.incarnation_id))?;
        let row = store.node(&node_id).await.map_err(|e| format!("node.create:path={node_path:?} name={node_name:?}: the row of the predecessor {} ({node_id}) could not be read for its executable: {e}", predecessor.node_id))?;
        row.and_then(|r| r.executable).ok_or_else(|| format!("node.create:path={node_path:?} name={node_name:?}: the row of the predecessor {} {} records no executable, so the admin created for {} has none to run; nothing is launched", predecessor.node_id, predecessor.incarnation_id, predecessor.mesh))
    }

    /// The durable runtime row of the roster birth `b` (the lost final primary, or another
    /// node-admin of the leaving mesh), read by its key ([`crate::admin::birth_runtime_row`]); a
    /// roster birth whose ids do not parse is refused by name.
    async fn roster_birth_row(&self, store: &dyn crate::storage::NodesStorage, b: &RosterBirth) -> Result<Option<crate::storage::RuntimeRow>, String> {
        let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("the roster names {} by node id `{}`, which does not parse: {e}", b.node_id, b.node_id))?;
        crate::admin::birth_runtime_row(store, &self.held_name(), &node_id, &b.birth().map_err(|e| format!("the roster names {} at birth `{}`, which does not parse: {e}", b.node_id, b.incarnation_id))?).await
    }

    /// "When none survives" (mesh-delete.md, "Resume after the primary's loss", step 2), for the
    /// node-admin `b` of the leaving mesh `mesh` other than the lost final primary, which this view
    /// holds silent: its node.delete, unless this leave already holds its receipt, this fabric-primary
    /// the executor, `TerminateRuntime` first when it runs (node-recovery.md step 3). A runtime unknown
    /// here ends the attempt incomplete, named, and nothing is created for the mesh.
    async fn delete_silent_admin(&self, build_id: &crate::build::BuildId, key: &MeshDeleteKey, mesh: &str, b: &RosterBirth, steps: &[crate::build_state::BuildStep]) -> Result<(), RunEnd> {
        let outer = &key.operation;
        if crate::mesh_delete::exit_receipts(steps, std::slice::from_ref(b)).contains_key(&(b.node_id.clone(), b.incarnation_id.clone())) {
            return Ok(());
        }
        // An admin node investigation reaches is not lost (Q111): silent in this view only for a moment (the
        // lost primary published the mesh's topology), it is the surviving admin the election names
        // (mesh-delete.md "Resume after the primary's loss" step 2), and the next attempt hands it the
        // workflow. Nothing is deleted or created for the mesh.
        let held = self.topology.read().await.nodes_of_type(mesh, "node-admin").find(|n| n.node_id.as_str() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id)).cloned();
        if let Some(n) = held {
            if matches!(crate::path_investigation::PathProbe::investigate(self, &n).await, crate::node_investigation::Finding::Reached { .. }) {
                return Err(RunEnd::Incomplete(format!("{outer:?}: {mesh}'s node-admin {} {} answers, so it survives; the next attempt hands the workflow to the admin the election names", b.node_id, b.incarnation_id)));
            }
        }
        let refused = |reason: String| {
            tracing::info_span!("rdm.node_admin.mesh.reject.via-unknown-admin-runtime", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, admin = %format!("{} {}", b.node_id, b.incarnation_id), reason = %reason)
                .in_scope(|| tracing::info!("a node-admin of the leaving mesh is silent and its runtime is not known here: no admin is created for the mesh"));
            RunEnd::Incomplete(format!("{outer:?}: {mesh} holds no live admin in this view, and the runtime of its silent node-admin {} {} is not known here ({reason}): no admin is created for the mesh", b.node_id, b.incarnation_id))
        };
        let store = self.records.contacts.get().ok_or_else(|| refused(format!("{} holds no durable runtime rows", self.node_id)))?;
        let row = self.roster_birth_row(&**store, b).await.map_err(|e| refused(e))?;
        crate::mesh_delete::predecessor_verdict(&*self.provider, row.as_ref(), b).await.map_err(refused)?;
        self.delete_lost_birth(build_id, b).await.map(|_| ())
    }

    /// node.delete of the lost roster birth `b` as an unreached birth (mesh-delete.md "Resume after the
    /// primary's loss" step 1; node-recovery.md step 3), this fabric-primary its executor, under the
    /// leave's Build and attempt; the `TerminateRuntime` receipt under `node.delete` with an explicit node ID is the
    /// birth's roster receipt. Answers that receipt.
    async fn delete_lost_birth(&self, build_id: &crate::build::BuildId, b: &RosterBirth) -> Result<TerminalReceipt, RunEnd> {
        let path: PathName = b.node.clone();
        let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("the roster names {} by node id `{}`, which does not parse: {e}", b.node_id, b.node_id))?;
        let incarnation = b.birth().map_err(|e| format!("the roster names {} at birth `{}`, which does not parse: {e}", b.node_id, b.incarnation_id))?;
        // An explicit node.delete (fabric-delete.md "Receipts"; mesh-delete.md "Resume after the primary's
        // loss"): an os-stopped runtime is terminated Immediate, never left as node recovery leaves it.
        let operation = rdm_core::OperationId::node("node.delete", &node_id);
        self.delete_unreached_as(&*self.builds, Some(build_id), &path, &crate::build::ExitedBirth { node_id, incarnation }, true, false, &operation).await
    }

    /// Steps 2-5 of the owner's workflow, for the leave `key` opened at admission: `mesh.deleting`,
    /// `DeleteMesh`, the validated handoff (mesh.deleted) and its completion receipt.
    #[allow(clippy::too_many_arguments)]
    async fn lead_delete(
        &self,
        build_id: &crate::build::BuildId,
        mesh_id: &MeshId,
        outer: &rdm_core::OperationId,
        node_rpc: &crate::node_rpc::ProcessNodeRpc,
        primary: &Node,
        key: &MeshDeleteKey,
        handoff: tokio::sync::watch::Receiver<Option<crate::mesh_delete::MeshDeleteOutcome>>,
    ) -> Result<(), RunEnd> {
        let req = StatusRequest::DeleteMesh { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), operation: outer.clone() };
        // The fabric-primary that is the deleting mesh's own primary (its own mesh, the last of
        // fabric.delete) commands the delete in process: a Node RPC client reaches no endpoint of its own.
        let replied: Result<StatusReply, String> = match self.mesh_deleter.get().filter(|_| primary.node_id == self.node_id) {
            Some(own) => Ok(own.delete_mesh(None, None, &req).await),
            None => match node_rpc.client.call::<Status>(&rdm_transport::NodeTarget::ExactNode(primary.node_id.clone()), &req, &rdm_transport::CallOptions::default()).await.0 {
                rdm_core::outcome::RpcOutcome::Reply(r) => Ok(r.into_value()),
                other => Err(format!("{} ({other:?})", other.name())),
            },
        };
        match replied {
            Ok(StatusReply::Applied | StatusReply::AlreadyApplied) => {}
            Ok(refused) => return Err(RunEnd::failed(format!("{outer:?}: {} refused delete-mesh: {} ({refused:?})", primary.node_id, refused.name()))),
            // The call did not come back: the primary is unreached or the stream broke. A broken stream
            // proves neither a failed step nor a completed one (node-rpc-envelope.md, "Caller execution
            // and certainty"): the leave is incomplete and its receipts stand.
            Err(ended) => return Err(RunEnd::Incomplete(format!("{outer:?}: delete-mesh to {} ended {ended}: the mesh may or may not have admitted it", primary.node_id))),
        }
        // Steps 3-4 run at the mesh-primary; its validated handoff, mesh.deleted, is what this waits
        // for. The validation covered the accepted roster (and every amendment) exactly, every birth
        // but the final primary, which reports still running.
        // The wait ends on the handoff, or on the final primary unheard in this owner's view.
        let Some(admitted) = self.leaves.admitted(key) else {
            return Err(RunEnd::Incomplete(format!("{outer:?}: the open leave was closed before its handoff was awaited")));
        };
        let named = format!("{} {}", admitted.final_primary.node_id, admitted.final_primary.incarnation_id);
        let inspected = std::sync::Mutex::new(None);
        let manifest = crate::mesh_delete::await_mesh_deleted(key, &named, handoff, self.records.subscribe_view(), || self.final_primary_unheard(key, primary, &admitted, &inspected), crate::mesh_delete::MESH_DELETED_WAIT).await?;
        // Step 5: mesh.deleted ends the mesh.delete. The final primary is not deleted inside its own
        // mesh.delete: it keeps publishing its mesh topology until a separate node.delete (step 7).
        self.record_mesh_deleted(key, manifest).await.map_err(RunEnd::failed)
    }

    /// Whether the leave's final primary `primary` (the admitted or amended birth `admitted` names) is
    /// unheard in this owner's view (no longer live, or gone from the view), naming what the provider
    /// reads of its runtime: the wait ends and the next attempt resumes the leave after the primary's
    /// loss (mesh-delete.md "Resume after the primary's loss"), whatever the verdict (node-recovery.md
    /// step 3, Q111). The provider is asked once per status the view holds it at (`inspected`).
    async fn final_primary_unheard(&self, key: &MeshDeleteKey, primary: &Node, admitted: &crate::mesh_delete::Admitted, inspected: &std::sync::Mutex<Option<Option<NodeStatus>>>) -> Option<String> {
        let lost = &admitted.final_primary;
        let incarnation = lost.birth().ok()?;
        let status = self.topology.read().await.nodes.iter().find(|n| n.node_id == primary.node_id && n.incarnation_id.as_ref() == Some(&incarnation)).map(|n| n.status);
        if status.is_some_and(NodeStatus::is_live) {
            return None;
        }
        if inspected.lock().unwrap().replace(status) == Some(status) {
            return None;
        }
        let row = match self.records.contacts.get() {
            Some(store) => self.roster_birth_row(&**store, lost).await,
            None => Err(format!("{} holds no durable runtime rows", self.node_id)),
        };
        let verdict = match row {
            Ok(row) => match crate::mesh_delete::predecessor_verdict(&*self.provider, row.as_ref(), lost).await {
                Ok(found) => crate::admin::verdict_name(&found).to_string(),
                Err(reason) => reason,
            },
            Err(reason) => reason,
        };
        tracing::info_span!("rdm.node_admin.mesh.resolve.via-final-primary-unheard", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, final_primary = %format!("{} {}", lost.node_id, lost.incarnation_id), status = ?status, verdict = %verdict)
            .in_scope(|| tracing::info!("the final primary is unheard: the owner's wait ends and the leave resumes after its loss"));
        Some(format!("the final primary is unheard in this fabric-primary's view ({status:?}); its runtime reads {verdict}"))
    }

    /// mesh.deleted's completion (mesh-delete.md step 5): the validated manifest `manifest` (every
    /// birth of the mesh but its final primary) is recorded as the `MeshDeleted` receipt and the open
    /// leave ends. Nothing of the final primary is stopped, written or published here.
    async fn record_mesh_deleted(&self, key: &MeshDeleteKey, manifest: ExitManifest) -> Result<(), String> {
        crate::mesh_delete::record_completion(&*self.builds, key, &self.node_id.to_string(), &manifest).await?;
        // The mesh-primary committed the row deleted before it called: this fabric-primary's view states
        // the mesh deleted from its acceptance on, so a node.delete of its last admin names it.
        self.records.hold_mesh_deleted(&key.mesh_id);
        self.leaves.close(key);
        self.refresh_view().await;
        Ok(())
    }

    /// mesh-left (R-MD1 items 2-3): this admin published `op`'s birth deleted from the provider's
    /// exact exited verdict. When that birth is the final primary a mesh.delete's completion receipt names
    /// (a point read of the Build facts, [`crate::build_state::BuildStateAdapter::mesh_deleted_of`]),
    /// every birth of the mesh is now proven exited: the final birth's exact Exited receipt is read
    /// from the node.delete's own `TerminateRuntime` step receipt, the complete proof set is checked
    /// ([`crate::mesh_delete::mesh_left_manifest`]) and recorded as `MeshLeft`, and mesh-left is published.
    /// Any other deleted birth names no completion and nothing happens. A refusal is spanned by name
    /// and leaves the node.delete as it is.
    pub(super) async fn mesh_left_after_delete(&self, op: &rdm_core::LifecycleOp) {
        let me = self.held_name();
        let found = match self.mesh_deleted_of(op).await {
            Ok(Some(found)) => found,
            Ok(None) => return,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.mesh.reject.via-mesh-left", node_path = %me.path, node_name = %me.name, subject_path = %op.path, subject_name = %op.name, node_id = %op.node_id, reason = %format!("the mesh.delete of the deleted birth's mesh could not be read: {e}"))
                    .in_scope(|| tracing::info!("mesh-left is not decided: the deleted birth's record or its mesh's mesh.delete could not be read"));
                return;
            }
        };
        let (build_id, operation, mesh_id) = found;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-final-birth-deleted", node_path = %me.path, node_name = %me.name, subject_path = %op.path, subject_name = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %build_id.log(), operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, deleted_by = %op.operation.action, deleted_by_subject = ?op.operation.subject, deleted_by_step = ?op.operation.step, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let result = self.mesh_left_in(op, &build_id, &operation, &mesh_id).instrument(span.clone()).await;
        match &result {
            Ok(outcome) => {
                span.record("outcome", *outcome);
            }
            Err(reason) => {
                span.record("outcome", "refused");
                span.record("reason", reason.as_str());
            }
        }
        span.in_scope(|| tracing::info!("the final primary of a deleted mesh is proven exited: mesh-left is decided"));
    }

    /// The mesh.delete whose `MeshDeleted` step ended `complete` and whose `MeshLeft` did not, of the mesh
    /// of `op`'s birth: that birth is the mesh's final primary, the one birth of the mesh its records do
    /// not hold deleted (D67). The mesh is the one the birth's own node record names by its id (a
    /// fabric-primary that took the role from the deleted mesh never created it, and holds no id of it
    /// otherwise). Only a mesh whose record is deleted has one: its mesh-primary writes the mesh's record
    /// deleted before it reports mesh.deleted (mesh-delete.md step 5). Its Build is the newest one holding
    /// a step event of `mesh.delete:<mesh_id>`, read newest first: the final primary's own node.delete is
    /// a Build of its own, accepted after it, so the current Build is not it. `None` when no such
    /// mesh.delete is open.
    async fn mesh_deleted_of(&self, op: &rdm_core::LifecycleOp) -> Result<Option<(crate::build::BuildId, rdm_core::OperationId, MeshId)>, String> {
        let store = self.records.contacts.get().ok_or_else(|| format!("{} holds no nodes.storage to read the node record of {}", self.node_id, op.node_id))?;
        let row = store.node(&op.node_id).await.map_err(|e| format!("the node record of {} could not be read: {e}", op.node_id))?;
        let Some(mesh_id) = row.and_then(|r| r.mesh_id) else { return Ok(None) };
        let mesh = self.mesh_storage.mesh(&mesh_id).await.map_err(|e| format!("the record of {} ({mesh_id}) could not be read: {e}", op.mesh))?;
        if !mesh.is_some_and(|m| m.state == rdm_core::status::MeshState::Deleted) {
            return Ok(None);
        }
        let operation = crate::mesh_delete::operation(&mesh_id);
        let key = operation.to_string();
        for id in self.builds.accepted_newest_first().await.map_err(|e| e.to_string())? {
            let p = match self.builds.read_build(&id).await {
                Ok(p) => p,
                Err(crate::build_state::BuildStateError::UnknownBuild(_)) => continue,
                Err(e) => return Err(e.to_string()),
            };
            if p.steps.iter().any(|s| s.operation == key) {
                return Ok((crate::mesh_delete::owner_resume(&p.steps, &key) == crate::mesh_delete::OwnerResume::Deleted).then(|| (p.build_id.clone(), operation, mesh_id)));
            }
        }
        Ok(None)
    }

    async fn mesh_left_in(&self, op: &rdm_core::LifecycleOp, build_id: &crate::build::BuildId, operation: &rdm_core::OperationId, mesh_id: &MeshId) -> Result<&'static str, String> {
        let mut steps = self.builds.read_build(build_id).await.map_err(|e| format!("{operation}: the Build {build_id} could not be read for its completion: {e}"))?.steps;
        if crate::mesh_delete::owner_resume(&steps, &operation.to_string()) == crate::mesh_delete::OwnerResume::Left {
            return Ok("already-applied");
        }
        // The final birth's exact Exited: its node.delete's TerminateRuntime step, in its own Build.
        let deleted_in = crate::build::BuildId::parse(&op.build_id).map_err(|e| format!("{}: the node.delete names Build {:?}, which is not a Crockford id: {e}", op.operation, op.build_id))?;
        if deleted_in != *build_id {
            steps.extend(self.builds.read_build(&deleted_in).await.map_err(|e| format!("{}: the Build {deleted_in} of the node.delete could not be read for its exited verdict: {e}", op.operation))?.steps);
        }
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: operation.clone() };
        // The proof set over the mesh's records: every birth, the final primary included, proven exited
        // by its own node.delete's events (R-MD1 item 2).
        let roster = self.stored_roster(&op.mesh, mesh_id, &steps).await?;
        let last = roster.iter().find(|b| b.node_id == op.node_id.as_str() && b.incarnation_id == op.incarnation.as_str()).cloned().ok_or_else(|| format!("{operation}: {} {} is no birth of {}'s records", op.node_id, op.incarnation.as_str(), op.mesh))?;
        let all = crate::mesh_delete::manifest_of(&key, &roster, &last, None, &steps, true);
        if let Some((field, expected, reported)) = crate::mesh_delete::first_mismatch(&all, &roster, &last, true) {
            return Err(format!("{operation}: the proof set is not complete: {field}: expected {expected}, reported {reported}"));
        }
        self.record_mesh_left(&key, &op.mesh, all).await?;
        Ok("applied")
    }

    /// mesh-left: the complete proof set `all` (every birth, the final primary included) is recorded
    /// as the `MeshLeft` receipt after the mesh's deleted contacts are dropped and its row deleted is
    /// ensured; then the mesh-left hook runs and the mesh's id is dropped.
    async fn record_mesh_left(&self, key: &MeshDeleteKey, mesh: &str, all: ExitManifest) -> Result<(), String> {
        let (build_id, outer, mesh_id) = (key.build_id.clone(), &key.operation, &key.mesh_id);
        let reference = manifest_reference(key.build_id.as_str(), outer, true);
        let me = self.node_id.to_string();
        // This fabric-primary is the acting primary of a mesh that has none left: the final birth's
        // node.delete wrote its row Deleted and proved its exit.
        crate::mesh_delete::mesh_left_rows(self.records.node_rows().as_deref(), &*self.mesh_storage, &self.records.caches, &self.records.declared, &me, key, mesh, &all.receipts).await?;
        self.builds
            .write_step(&crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, operation: outer.clone(), step: STEP_MESH_LEFT.into(), outcome: StepOutcome::Complete })
            .await
            .map_err(|e| format!("{outer:?}: the MeshLeft receipt could not be recorded: {e}"))?;
        tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-left-receipt", mesh_id = %mesh_id, build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, receipts = all.receipts.len(), roster = all.roster.len(), manifest = ?reference)
            .in_scope(|| tracing::info!("every exact exit is proven: the MeshLeft receipt is recorded"));
        self.digest_view.depart_mesh_births(mesh, build_id.as_str(), self.lifecycle_events.now_rafka_ms());
        self.forget_mesh_id(mesh);
        self.refresh_view().await;
        Ok(())
    }

    /// A mesh that has left keeps no id here: the MeshLeft receipt is the end of that mesh, and a
    /// mesh of the same name created after it is a new mesh with a new id. The id is dropped at the
    /// receipt, never left for a later sweep to find the mesh without a member.
    fn forget_mesh_id(&self, mesh: &str) {
        let forgot = self.records.meshes.lock().unwrap().remove(mesh);
        if let Some(id) = forgot {
            // The projection reads the mesh ids held: the view's rebuild is due (Q254).
            self.records.inputs_moved();
            self.records.deleted_meshes_forget(&id);
        }
    }

    /// `DeleteMesh` at the leaving mesh's primary: admit it and run the member workflows.
    pub(super) async fn admit_delete_mesh(self: Arc<Self>, peer: Option<iroh::PublicKey>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::DeleteMesh { mesh_id, build_id, operation } = req else { unreachable!("admit_delete_mesh serves delete-mesh") };
        let me_name = self.held_name();
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            return rdm_core::reply::status::not_ready(format!("{} holds no view of itself yet; delete-mesh for {mesh_id} is refused", self.node_id));
        };
        let held = self.records.meshes.lock().unwrap().get(&me.mesh).cloned().or_else(|| view.meshes.iter().find(|m| m.name == me.mesh).and_then(|m| m.id.clone()));
        match held {
            Some(h) if &h != mesh_id => return rdm_core::reply::status::rejected_stale_mesh(h),
            None => return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("an admin of the mesh {mesh_id}") }),
            Some(_) => {}
        }
        if !me.is_primary {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("mesh-primary of {}", me.mesh) });
        }
        // The handoff goes back to whoever owns the workflow: the admin whose call sent this command,
        // reached back over that call's endpoint and path with no record of it read, else the
        // fabric-primary this admin's view names (DeleteMesh is the fabric-primary's command,
        // node-rpc-envelope.md "Mesh delete command pair"). Nothing checks who is calling
        // (node-rpc.md §14).
        let build = match crate::build::BuildId::parse(build_id) {
            Ok(build) => build,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.mesh.reject.via-malformed-build-id", node_path = %me_name.path, node_name = %me_name.name, mesh_id = %mesh_id, build_id = %build_id, reason = %e, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("delete-mesh refused: its Build id is not a Crockford id"));
                return rdm_core::reply::status::malformed(rdm_core::outcome::MalformedKind::Corrupt);
            }
        };
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build, operation: operation.clone() };
        // The leave's owner: the caller reached back over its call, else the fabric-primary the view
        // names ([`crate::mesh_delete::leave_owner`]). The command is admitted on its key alone, whoever sent it.
        let me_str = self.node_id.to_string();
        let call = match peer {
            Some(key) => crate::workflows::node_start::reached_back(&me_str, &self.node_rpc.client, &key).await.ok().map(|target| crate::mesh_delete::MeshDeleteOwner { target, name: sender.map(|n| n.node_id.to_string()).unwrap_or_else(|| format!("the admin at endpoint {}", key.fmt_short())) }),
            None => None,
        };
        let fabric_primary = view.fabric_primary().map(|n| crate::mesh_delete::MeshDeleteOwner { target: rdm_transport::NodeTarget::ExactNode(n.node_id.clone()), name: n.node_id.to_string() });
        let owner = match crate::mesh_delete::leave_owner(call, fabric_primary, &me_str, &key) {
            Ok(owner) => owner,
            Err(refused) => return refused,
        };
        // The mesh row Leaving and the workflow's admission (the command hook's mesh-leaving is the
        // owner's): [`crate::mesh_delete::admit_delete_mesh`].
        let crate::mesh_delete::DeleteMeshAdmission { leaving_row, admission } = crate::mesh_delete::admit_delete_mesh(&self.mesh_delete_runs, &*self.mesh_storage, &self.records.caches, &self.records.declared, &me.mesh, &key, &owner).await;
        if let Some(committed) = &leaving_row {
            tracing::info_span!("rdm.node_admin.mesh.update.via-leaving-row", node_path = %me_name.path, node_name = %me_name.name, mesh = %me.mesh, mesh_id = %mesh_id, build_id = %build_id, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, owner = %owner, outcome = committed.name())
                .in_scope(|| tracing::info!("delete-mesh at the mesh's primary: its own mesh row Leaving is stored and applied before the answer"));
            if matches!(committed, StatusReply::Applied | StatusReply::AlreadyApplied) {
                // The committed Leaving is owed upward now: the declarer reports it to the
                // fabric-primary (DeclareMeshState, "report committed state upward"), whose map
                // holds the state it hands an admin it creates for this mesh.
                self.records.wake.poke();
                // The same commit applies the row to the view: the mesh is projected leaving now,
                // and the view change wakes the hierarchy loop, which announces it on the mesh's
                // MeshStatus send.
                self.refresh_view().await;
            }
        }
        let resend = match admission {
            crate::mesh_delete::MeshDeleteAdmission::Running => return rdm_core::reply::already_applied(),
            crate::mesh_delete::MeshDeleteAdmission::Repeat { manifest } => Some(manifest),
            crate::mesh_delete::MeshDeleteAdmission::Start => None,
            crate::mesh_delete::MeshDeleteAdmission::Refused(reply) => return reply,
        };
        // The command's serve span carries the caller's propagated trace; the workflow it starts
        // continues it, so every member's calls join the outer trace.
        let parent = tracing::Span::current();
        let runner = self.clone();
        match resend {
            // The members all exited and the manifest is recorded: the handoff is what repeats.
            Some(manifest) => {
                tokio::spawn(async move { runner.hand_off(&key, &owner, &manifest).instrument(parent).await });
                rdm_core::reply::already_applied()
            }
            None => {
                tokio::spawn(async move { runner.run_leave(key, parent).await });
                rdm_core::reply::applied()
            }
        }
    }

    /// The one run of the member workflows of the leave `key` admitted, on its natural key
    /// `(mesh_id, Leaving)` ([`crate::mesh_delete::run_admitted`]): a `delete-mesh` of a later attempt
    /// admitted meanwhile joins it, and the handoff goes under the key admitted last.
    async fn run_leave(self: Arc<Self>, key: MeshDeleteKey, parent: tracing::Span) {
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.mesh.update.via-leave-members", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, outcome = tracing::field::Empty, handoff_attempt = tracing::field::Empty);
        async {
            let end = crate::mesh_delete::run_admitted(&self.mesh_delete_runs, &key.mesh_id, |key, owner| {
                let runner = self.clone();
                async move { runner.leave_members(&key, &owner).await }
            })
            .await;
            match end {
                crate::mesh_delete::MeshDeleteRunEnd::Exited { key, owner, manifest } => {
                    tracing::Span::current().record("outcome", "exited");
                    // The handoff goes to the owner that sent delete-mesh last, read now.
                    self.hand_off(&key, &owner, &manifest).await;
                }
                crate::mesh_delete::MeshDeleteRunEnd::Incomplete { key, owner, reason } => {
                    tracing::Span::current().record("outcome", "incomplete");
                    // The run ends; its receipts stand. Every member that exited holds its exact
                    // Exited step receipt, and the roster its LeavingRoster receipt: a repeated
                    // delete-mesh, or the Build's next attempt, runs only the members without one.
                    tracing::info!(error = %reason, "the member workflows are incomplete: the owner is told now");
                    // The owner learns it inside the call: mesh-deleted with no recorded manifest,
                    // which it refuses by name (RejectedIncompleteManifest, the births without an
                    // Exited receipt named) and ends its open leave with. No mesh-deleted gossip runs:
                    // this is no handoff.
                    self.call_mesh_deleted(&key, &owner, "incomplete", 0).await;
                }
                crate::mesh_delete::MeshDeleteRunEnd::Forgotten => tracing::info!("this leave's run is no longer held: the handoff waits for a repeated delete-mesh"),
            }
        }
        .instrument(span)
        .await
    }

    /// Both tiers over the accepted roster, then the one manifest receipt
    /// ([`crate::mesh_delete::members_exited`]). `owner` is the fabric-primary that sent `DeleteMesh`:
    /// the Build is read from its committed records (Q292).
    async fn leave_members(&self, key: &MeshDeleteKey, owner: &crate::mesh_delete::MeshDeleteOwner) -> Result<ExitManifest, String> {
        let me_name = self.node_id.to_string();
        let view = self.topology.read().await.clone();
        let me = view.own(&self.node_id).cloned().ok_or_else(|| format!("{me_name} holds no view of itself"))?;
        let my_fact = self.held_runtime(&me).ok_or_else(|| format!("{} publishes no runtime fact: its own exact runtime cannot be named", me.node_id))?;
        let me_birth = RosterBirth::of(&me)?;
        let my_runtime = RuntimeIdentity::of(&my_fact);
        // The roster the owner captured once, as its LeavingRoster receipt here, or, when this admin holds
        // none, what the owner admitted: the
        // roster amendments the fabric-primary recorded (a primary lost mid-leave) are read from the
        // Build's committed records in storage when this admin has not yet heard them (Q292;
        // [`crate::mesh_delete::leaving_roster`]).
        let _ = owner;
        // The Build is read from its committed records first, as a section's run reads it (Q292): the
        // leaving primary holds no record of the Build its delete-mesh names until it reads it.
        let facts = self.local.committed_facts(Some(&key.build_id)).await.map_err(|e| format!("{me_name}: the committed records of Build {} could not be read for the leave's roster: {e}", key.build_id))?;
        self.local.absorb_facts(&facts).await;
        let steps = self.builds.read_build(&key.build_id).await.map_err(|e| format!("{me_name}: the Build {} could not be read for the leave's roster: {e}", key.build_id))?.steps;
        let stored = self.stored_roster(&me.mesh, &key.mesh_id, &steps).await?;
        let roster = crate::mesh_delete::leaving_roster(&*self.builds, key, &me_name, &me_birth, || Ok(stored)).await?;
        let mesh: Vec<Node> = view.nodes.iter().filter(|n| n.mesh == me.mesh).cloned().collect();
        let records = crate::mesh_delete::roster_records(&mesh, &roster, |b| self.held_record(b));
        crate::mesh_delete::roster_rows_read(self.records.node_rows().as_deref(), &me_name, &key.operation, &roster, &records).await;
        members_exited(&*self.builds, key, &me_name, &roster, &me_birth, my_runtime, &records, |n| async move { self.shutdown_one(key, &n).await }).await
    }

    /// One member's end (mesh-delete.md step 3: "The mesh-primary runs the whole node.delete for
    /// every ordinary node"): the ordinary node.delete workflow ([`AdminRunner::delete_birth`]) of the
    /// exact birth, under the mesh.delete's Build and attempt. The provider's terminal receipt is the
    /// member's roster receipt.
    async fn shutdown_one(&self, key: &MeshDeleteKey, n: &Node) -> Result<TerminalReceipt, String> {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-shutdown-member", node_path = %n.path, node_name = %n.name, node_id = %n.node_id, operation = %key.operation.action, operation_subject = ?key.operation.subject, build_id = %key.build_id.log(), outcome = tracing::field::Empty);
        let result = async {
            // A member whose delete already holds its TerminateRuntime receipt runs on from its receipts
            // when its runtime is no longer resolved here (no digest, no runtime row), as an unreached
            // birth's delete does (`delete_unreached_as`): the exited verdict is a step already taken.
            let (record, handle) = match self.handle_for(n).await {
                Ok(resolved) => resolved,
                Err(e) => {
                    let from = n.incarnation_id.clone().map(|incarnation| crate::build::ExitedBirth { node_id: n.node_id.clone(), incarnation });
                    match from {
                        Some(from) => super::runner::receipted_exit(&*self.builds, self.records.contacts.get().map(|s| &**s as &dyn crate::storage::NodesStorage), Some(&key.build_id), &n, &from, &rdm_core::OperationId::node("node.delete", &from.node_id)).await.ok_or(e)?,
                        None => return Err(e),
                    }
                }
            };
            // Whether the birth is stopped is the record's the leave works from (a stopped member's
            // delete goes straight to its exited verdict), as `delete` takes it from the view.
            let record = Node { stopped: n.stopped, ..record };
            let operation = rdm_core::OperationId::node("node.delete", &record.node_id);
            self.delete_birth(&key.build_id.clone(), record, handle, &operation).await.map_err(|e| format!("{}'s node.delete: {e}", n.node_id))
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", if result.is_ok() { "exited" } else { "incomplete" });
        result
    }

    /// The `MeshDeleted` handoff: the mesh-leave gossip hook, then the call up to the owner.
    async fn hand_off(&self, key: &MeshDeleteKey, owner: &crate::mesh_delete::MeshDeleteOwner, manifest: &ExitManifest) {
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            tracing::info!(operation = %key.operation.action, operation_subject = ?key.operation.subject, "this admin holds no view of itself: the handoff waits for a repeated delete-mesh");
            return;
        };
        if self.held_runtime(&me).is_none() {
            tracing::info!(node = %me.node_id, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        }
        // mesh.deleted (ops-naming mesh.delete: "reported by the mesh-primary, still alive, from its
        // members' exited verdicts; mesh is dead, id kept deleted"): every other member's exit is proven,
        // so this primary commits its mesh row deleted (states.md: "one commit stores the row, applies
        // it, gossips it and answers"); its view then states the mesh Dead and its status workflow
        // publishes MeshStatus Dead, five sends, while it lives (R-NS1 item 1). A refused put
        // answers no handoff: the owner's repeated delete-mesh decides it afresh.
        // Its Mesh row is written deleted on mesh.storage with it: writing the deleted record frees the
        // name (R-MN1 item 2); a refused write answers no handoff either.
        let committed = crate::status_rpc::commit_mesh_state(&*self.mesh_storage, &self.records.caches, &self.records.declared, &me.mesh, &key.mesh_id, rdm_core::status::MeshState::Deleted, 0).await;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-deleted-row", node_path = %self.held_name().path, node_name = %self.held_name().name, mesh = %me.mesh, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, receipts = manifest.receipts.len(), outcome = committed.name(), mesh_row = tracing::field::Empty, error = tracing::field::Empty);
        let mesh_row = match committed {
            StatusReply::Applied | StatusReply::AlreadyApplied => crate::mesh_delete::mesh_row_deleted(&*self.mesh_storage, &self.records.caches, &me.mesh, &key.mesh_id).await,
            refused => Err(format!("the Dead status row was not committed: {}", refused.name())),
        };
        match &mesh_row {
            Ok(row) => span.record("mesh_row", *row),
            Err(e) => span.record("mesh_row", "not-written").record("error", e.as_str()),
        };
        span.in_scope(|| tracing::info!("mesh.deleted at the mesh's primary: its own mesh row deleted is stored and applied from its members' exited verdicts, and its name released"));
        if let Err(e) = mesh_row {
            tracing::info!(operation = %key.operation.action, operation_subject = ?key.operation.subject, error = %e, "the mesh row deleted was not committed: the handoff waits for a repeated delete-mesh");
            return;
        }
        self.refresh_view().await;
        self.call_mesh_deleted(key, owner, "complete", manifest.receipts.len()).await;
    }

    /// The `MeshDeleted` call up to the owner, over the call that sent `delete-mesh` (or to the
    /// fabric-primary its view named): this primary's exact birth and runtime, and the reference of
    /// its exit manifest receipt. `manifest` says whether that receipt is recorded (`complete`) or
    /// the member workflows ended without one (`incomplete`).
    async fn call_mesh_deleted(&self, key: &MeshDeleteKey, owner: &crate::mesh_delete::MeshDeleteOwner, manifest: &'static str, manifest_receipts: usize) {
        let reference = manifest_reference(key.build_id.as_str(), &key.operation, false);
        let view = self.topology.read().await.clone();
        let node_rpc = &self.node_rpc;
        let Some(me) = view.own(&self.node_id).cloned() else {
            tracing::info!(operation = %key.operation.action, operation_subject = ?key.operation.subject, owner = %owner, "this admin holds no view of itself: the handoff waits for a repeated delete-mesh");
            return;
        };
        let Some(final_runtime) = self.held_runtime(&me) else {
            tracing::info!(node = %me.node_id, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        };
        let (Some(final_incarnation), final_node_id) = (me.incarnation_id.clone(), me.node_id.clone()) else { return };
        let req = StatusRequest::MeshDeleted { mesh_id: key.mesh_id.clone(), build_id: key.build_id.to_string(), operation: key.operation.clone(), final_node_id, final_incarnation, final_runtime, receipt_manifest: reference };
        // The owner that is this admin itself (the fabric-primary's own mesh) validates the handoff in
        // process: a Node RPC client reaches no endpoint of its own.
        let own = matches!(&owner.target, rdm_transport::NodeTarget::ExactNode(id) if *id == self.node_id);
        let outcome = if own {
            let r = self.validate_handoff(None, &req).await;
            format!("{} {r:?}", r.name())
        } else {
            match node_rpc.client.call::<Status>(&owner.target, &req, &rdm_transport::CallOptions::default()).await.0 {
                rdm_core::outcome::RpcOutcome::Reply(r) => format!("{} {:?}", r.value().name(), r.value()),
                other => format!("{} {other:?}", other.name()),
            }
        };
        tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-sent", mesh_id = %key.mesh_id, operation = %key.operation.action, operation_subject = ?key.operation.subject, owner = %owner, manifest, manifest_receipts, outcome = %outcome)
            .in_scope(|| tracing::info!("the mesh-deleted handoff was called"));
    }

    /// `MeshDeleted` at the owner: read the manifest from the sender, validate it against the roster
    /// captured at admission and the final primary's provider access, and release the outer
    /// workflow. A reference alone is not proof.
    pub(super) async fn validate_handoff(&self, _sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::MeshDeleted { mesh_id, build_id, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest } = req else { unreachable!("validate_handoff serves mesh-deleted") };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-validated", mesh_id = %mesh_id, build_id = %build_id, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, outcome = tracing::field::Empty, field = tracing::field::Empty, without_exit = tracing::field::Empty);
        let reply = self.validate_handoff_in(mesh_id, build_id, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        if let StatusReply::RejectedUnmatchedCompletion { field, .. } = &reply {
            span.record("field", field.as_str());
        }
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn validate_handoff_in(&self, mesh_id: &MeshId, build_id: &str, operation: &rdm_core::OperationId, final_node_id: &NodeId, final_incarnation: &IncarnationId, final_runtime: &rdm_core::RuntimeFact, receipt_manifest: &rdm_core::status::ManifestReference) -> StatusReply {
        let me_name = self.node_id.to_string();
        let unmatched = |field: &str, expected: String, reported: String| rdm_core::reply::status::rejected_unmatched_completion(field.into(), expected, reported);
        let build = match crate::build::BuildId::parse(build_id) {
            Ok(build) => build,
            Err(e) => return unmatched("build_id", format!("a Crockford Build id ({e})"), build_id.to_string()),
        };
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build.clone(), operation: operation.clone() };
        let admitted = match self.leaves.admitted_for(&key) {
            Ok(admitted) => admitted,
            Err(refused) => {
                // No open leave: after mesh.deleted's completion receipt the handoff is a replay,
                // answered from the receipt (AlreadyApplied, no second receipt).
                match self.builds.read_build(&build).await {
                    Ok(b) => {
                        // The final primary is the mesh's one node-admin its records hold and no event proves exited.
                        let mesh = self.records.meshes.lock().unwrap().iter().find(|(_, id)| *id == mesh_id).map(|(m, _)| m.clone());
                        let roster = match &mesh {
                            Some(m) => self.stored_roster(m, mesh_id, &b.steps).await.unwrap_or_default(),
                            None => Vec::new(),
                        };
                        let exited = crate::mesh_delete::exit_receipts(&b.steps, &roster);
                        let final_primary = roster.into_iter().find(|x| rdm_core::is_node_admin(&x.r#type) && !exited.contains_key(&(x.node_id.clone(), x.incarnation_id.clone())));
                        if let Some(reply) = crate::mesh_delete::replayed_handoff(&b.steps, &key, final_primary.as_ref(), final_node_id, &final_incarnation.as_str()) {
                            return reply;
                        }
                    }
                    // A Build this admin holds no fact of holds no completion of it either.
                    Err(crate::build_state::BuildStateError::UnknownBuild(_)) => {}
                    Err(e) => return crate::mesh_delete::refuse_source_failed("mesh-deleted", &key, &format!("the Build log of {me_name}"), &format!("read {build} for the completion of {operation:?}"), &e.to_string()),
                }
                return refused;
            }
        };
        match receipt_manifest.encoded_len() {
            Ok(size) if size <= rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES => {},
            Ok(size) => return unmatched("receipt_manifest", format!("at most {} encoded bytes", rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES), format!("{size} encoded bytes")),
            Err(error) => return unmatched("receipt_manifest", "an encodable structured reference".into(), error.to_string()),
        }
        if final_node_id.to_string() != admitted.final_primary.node_id || final_incarnation.as_str() != admitted.final_primary.incarnation_id {
            return unmatched("final_primary", format!("{} {}", admitted.final_primary.node_id, admitted.final_primary.incarnation_id), format!("{final_node_id} {}", final_incarnation.as_str()));
        }
        // The final primary's exact runtime, and this admin's provider access to it.
        if RuntimeIdentity::of(final_runtime) != admitted.final_runtime {
            return unmatched("final_runtime", format!("{:?}", admitted.final_runtime), format!("{:?}", RuntimeIdentity::of(final_runtime)));
        }
        if let Err(refused) = crate::mesh_delete::adopt_final_runtime(&*self.provider, final_runtime, &me_name, &key) {
            return refused;
        }
        // The manifest: the receipts the final primary wrote, read now from the Build's committed
        // records in storage (Q292), where its receipts are durable before it calls.
        let read = self.builds.read_build(&build).await.map(|p| p.steps);
        let verdict = match crate::mesh_delete::handoff_verdict_from_read(read, &key, &admitted, receipt_manifest, &me_name, &final_node_id.to_string()) {
            Ok(v) => v,
            Err(refused) => return refused,
        };
        let manifest = match verdict {
            crate::mesh_delete::HandoffVerdict::Accept(m) => m,
            refused => {
                if let crate::mesh_delete::HandoffVerdict::Incomplete { reason, without_exit } = &refused {
                    // The leaving primary recorded no manifest: the open leave ends now with the
                    // births named, and this attempt ends inside the call.
                    tracing::Span::current().record("without_exit", without_exit.join(", ").as_str());
                    self.leaves.incomplete(&key, reason.clone());
                }
                return refused.refusal().expect("a verdict other than Accept refuses");
            }
        };
        if self.leaves.resolve(&key, manifest) {
            rdm_core::reply::applied()
        } else {
            rdm_core::reply::already_applied()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::{FabricTopology, MeshTopology};
    use crate::build::MeshDesired;
    use crate::build_state::{BuildAccepted, BuildStateAdapter as _, MemoryBuildStateAdapter};
    use crate::deployment::pipeline::DeleteStep;
    use crate::deployment::process::ProcessDeploymentProvider;
    use crate::lifecycle::{HookRegistry, LifecycleTransitionPipeline, MemoryReceiptLog};
    use crate::model::{Fabric, ScopeStatus};
    use rdm_transport::topology::{Backbone, TopologyView};
    use std::net::SocketAddr;

    /// One fabric-primary's `AdminRunner`, `mesh1.node-admin.1`, in process: its Build state a run's step
    /// log over the memory store (`steps`, the only adapter that writes step events), its records,
    /// nodes and mesh stores in memory, and its own endpoint, resolver and client.
    struct Rig {
        runner: Arc<AdminRunner>,
        steps: Arc<crate::build_run::FramedBuilds>,
        records: Arc<Records>,
        ep: iroh::Endpoint,
        dir: std::path::PathBuf,
        mesh1_id: MeshId,
        mesh2_id: MeshId,
    }

    async fn rig(name: &str) -> Rig {
        let ep = rdm_transport::endpoint::bind(
            iroh::SecretKey::from_bytes(&[37u8; 32]),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let fabric_id = FabricId::mint();
        let mesh1_id = MeshId::mint();
        let mesh2_id = MeshId::mint();
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let node_id = NodeId::mint();
        let incarnation = IncarnationId::mint();

        let topology_view = TopologyView::join(
            &gossip,
            &ep,
            &fabric_id,
            "mesh1",
            &mesh1_id,
            "mesh1.node-admin.1",
            Arc::new(time.clone()),
            vec![],
            &[],
        )
        .await
        .unwrap();
        let backbone = Backbone::join(
            &gossip,
            &ep,
            &topology_view,
            "mesh1",
            rdm_core::PublisherId {
                incarnation: incarnation.clone(),
                node_id: node_id.clone(),
            },
            vec![],
        )
        .await
        .unwrap();

        let dir = crate::test_support::tempdir(name);
        let providers = crate::storage_backend::TopologyStorageProviders::file(dir.clone());
        let scope = crate::storage_backend::StorageScope {
            data_dir: dir.clone(),
            fabric_id: fabric_id.clone(),
            own_mesh: "mesh1".into(),
            writer: incarnation.clone(),
            node_id: node_id.clone(),
        };
        let topology_storage = crate::storage_backend::TopologyStorageHandle::open(&providers, "file", scope)
            .await
            .unwrap();
        let storage_changes = Arc::new(crate::storage_change::StorageChanges::new(
            topology_storage,
            providers,
            fabric_id.clone(),
            node_id.clone(),
        ));

        let node_rpc = crate::node_rpc::ProcessNodeRpc::new(
            Arc::new(rdm_transport::LiveNodeResolver::default()),
            ep.clone(),
        );
        let provider_dyn: Arc<dyn DeploymentProvider> = Arc::new(ProcessDeploymentProvider::new());
        let records = Arc::new(Records::default());
        let topology = Arc::new(RwLock::new(Topology {
            fabric: Fabric {
                id: fabric_id.clone(),
                name: "fab".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: Vec::new(),
        }));
        let adapter = Arc::new(MemoryBuildStateAdapter::new());
        let steps = Arc::new(crate::build_run::FramedBuilds::stepping(adapter.clone(), &fabric_id, &node_id, "mesh1"));
        let primary_update = Arc::new(crate::primary_update::PrimaryUpdate {
            fabric: "fab".into(),
            me_path: me.path.clone(), me_name: me.name.clone(),
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            topology: topology.clone(),
            digest_view: topology_view.digest_view.clone(),
            topology_view: topology_view.clone(),
            backbone: backbone.clone(),
            client: node_rpc.client.clone(),
            records: records.clone(),
            locks: Default::default(),
            memo: Default::default(),
            decisions: Arc::default(),
            project_view: Arc::new(|| Box::pin(async {})),
            finish_writes: Arc::new(|| Box::pin(async {})),
            looker: crate::primary_watch::Looker {
                me_path: me.path.clone(), me_name: me.name.clone(),
                me_id: node_id.clone(),
                topology_view: topology_view.clone(),
                client: node_rpc.client.clone(),
                provider: provider_dyn.clone(),
                nodes: None,
                heard_again: Default::default(),
                connections: None,
            },
            atlas: Arc::default(),
        });
        let template = LaunchTemplate {
            fabric: "fab".into(),
            fabric_id: fabric_id.clone(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(),
                path: me.path.clone(), name: me.name.clone(),
                node_id: node_id.clone(),
                incarnation: incarnation.clone(),
            },
            env: BTreeMap::new(),
            data_root: dir.clone(),
            store_dir: dir.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let nodes_storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let mesh_storage = Arc::new(crate::storage::MemoryMeshStorage::default());
        let fabric_storage = Arc::new(crate::fabric_storage::MemoryFabricStorage::default());
        records.set_status(
            nodes_storage.clone(),
            mesh_storage.clone(),
            fabric_storage.clone(),
            "mesh1".into(),
            me.clone(),
            node_id.clone(),
        );
        // The nodes store the admin reads the mesh's node records from (boot.rs: one nodes store).
        let _ = records.contacts.set(nodes_storage.clone());
        records.set_rows(Arc::new(
            crate::node_rows::NodeRows::new(
                me.clone(),
                node_id.clone(),
                topology.clone(),
                nodes_storage.clone(),
            )
            .with_written(records.caches.nodes.clone()),
        ));
        let runner = Arc::new(AdminRunner {
            storage_changes,
            target_seam: None,
            drives: Arc::new(crate::build_drive::Drives::default()),
            listeners: Default::default(),
            mesh_deleter: Arc::default(),
            connections: None,
            nodes_storage,
            mesh_storage: mesh_storage.clone(),
            fabric_storage,
            provider: provider_dyn,
            joins: Arc::new(crate::join::Joins::default()),
            observer: Arc::new(TopologyObserver {
                digest_view: topology_view.digest_view.clone(),
                client: Some(node_rpc.client.clone()),
                commands: Arc::default(),
                status_door: Arc::default(),
                primary_update: Arc::default(),
                unreachable: records.unreachable.clone(),
            }),
            records: records.clone(),
            builds: steps.clone(),
            local: adapter.clone(),
            template,
            admin_env: BTreeMap::new(),
            lifecycle: LifecycleTransitionPipeline::new(
                HookRegistry::new().build().unwrap(),
                Arc::new(MemoryReceiptLog::default()),
            ),
            handles: Mutex::new(HashMap::new()),
            topology,
            fabric: "fab".into(),
            fabric_id: fabric_id.clone(),
            fabric_provider: ProviderKind::Process,
            digest_view: topology_view.digest_view.clone(),
            me_path: me.path.clone(), me_name: me.name.clone(),
            node_id: node_id.clone(),
            endpoint: Some(ep.clone()),
            node_rpc,
            lifecycle_events: Arc::new(GossipLifecycle {
                topology_view,
                backbone,
                me_path: me.path.clone(), me_name: me.name.clone(),
                mesh_deleter: Arc::default(),
                project: None,
                status_door: Arc::default(),
            }),
            leaves: Arc::new(crate::mesh_delete::MeshDeleteView::default()),
            mesh_delete_runs: crate::mesh_delete::MeshDeleteRuns::default(),
            primary_update,
            issuer: Arc::new(crate::certs::CertIssuer::no_certs(time)),
            node_work: Default::default(),
            repairs_stopped: Default::default(),
        });

        Rig { runner, steps, records, ep, dir, mesh1_id, mesh2_id }
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-delete.md:166
    /// "Each member's `NodeStopped` and `NodeDeleted` are [node.delete](node-delete.md)'s"
    #[tokio::test]
    async fn delete_mesh_and_mesh_left_after_delete_resume_from_receipts_and_record_mesh_left_on_final_primary_exit() {
        let Rig { runner, steps, records, ep, dir, mesh1_id, mesh2_id } = rig("admin-mesh-delete-unit").await;

        // Set up a Build with `mesh.delete:<mesh2_id>` and its `MeshDeleted` completion receipt.
        let build_id = crate::build::BuildId::mint();
        let mut fab_top = FabricTopology::root("fab", "mesh1", mesh1_id);
        fab_top.meshes.insert(
            "mesh2".into(),
            MeshTopology::of(&MeshDesired::of("mesh2", [("node-admin", 1)])),
        );
        steps
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: fab_top,
                change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                source: None, time: 1000,
            })
            .await
            .unwrap();

        let final_id = NodeId::mint();
        let final_inc = IncarnationId::mint();
        // The final primary's record, its birth on it, written Deleted by its own node.delete.
        let mut final_row = crate::storage::NodeRow::new(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", final_id.clone(), NodeStatus::Deleted);
        final_row.birth_mut(&final_inc, &rdm_core::PathName::new("mesh2.node-admin", "1").unwrap());
        final_row.mesh_id = Some(mesh2_id.clone());
        runner.nodes_storage.put_node(&final_row).await.unwrap();
        // mesh2's mesh-primary wrote the mesh's record deleted before it reported mesh.deleted.
        crate::mesh_delete::mesh_row_deleted(&*runner.mesh_storage, &records.caches, "mesh2", &mesh2_id).await.unwrap();
        let final_birth = RosterBirth { mesh: "mesh2".into(), r#type: "node-admin".into(),
            node: rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(),
            node_id: final_id.to_string(),
            incarnation_id: final_inc.to_string(),
        };
        let final_runtime = RuntimeIdentity {
            deployment_id: "dep-final".into(),
            provider: "process".into(),
            control_domain_fingerprint: "dom-1".into(),
            locator_kind: "process".into(),
            locator_fingerprint: "loc-1".into(),
        };
        let outer = crate::mesh_delete::operation(&mesh2_id);
        let manifest = ExitManifest {
            mesh_id: mesh2_id.to_string(),
            roster: vec![final_birth.clone()],
            final_primary: final_birth.clone(),
            final_runtime: Some(final_runtime.clone()),
            receipts: Vec::new(),
        };
        let key = MeshDeleteKey {
            mesh_id: mesh2_id.clone(),
            build_id: build_id.clone(),
            operation: outer.clone(),
        };
        crate::mesh_delete::record_completion(&*steps, &key, "mesh1.node-admin.1", &manifest)
            .await
            .unwrap();

        // 1. `delete_mesh` resumes after `MeshDeleted` completion (`OwnerResume::Deleted`) and returns `Ok(())`.
        records.meshes.lock().unwrap().insert("mesh2".into(), mesh2_id.clone());
        assert_eq!(runner.delete_mesh(&build_id, "mesh2", &mesh2_id).await, Ok(()));

        // 2. `mesh_left_after_delete` records `MeshLeft` and drops `mesh2` from `records.meshes` once the final
        //    primary's `TerminateRuntime` receipt is present on its `node.delete` Build.
        let del_op = rdm_core::OperationId::node("node.delete", &final_id);
        let term = TerminalReceipt {
            path: "mesh2.node-admin".into(), name: "1".into(),
            node_id: final_id.to_string(),
            incarnation_id: final_inc.to_string(),
            runtime: Some(final_runtime),
            exit_code: Some(0),
            verdict: None,
            data_dir: None,
        };
        for step in [DeleteStep::NodeDeleting, DeleteStep::TerminateRuntime] {
            steps
                .write_step(&crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, operation: del_op.clone(), step: step.name().into(), outcome: StepOutcome::Complete })
                .await
                .unwrap();
        }

        let op = rdm_core::LifecycleOp { r#type: "node-admin".into(), mesh: "mesh2".into(),
            build_id: build_id.to_string(),
            operation: del_op,
            node_id: final_id,
            incarnation: final_inc,
            path: "mesh2.node-admin".into(), name: "1".into(),
            event_at_rafka_ms: 5_000_000,
            traceparent: None,
            tracestate: None,
        };
        assert_eq!(
            runner.mesh_deleted_of(&op).await.unwrap(),
            Some((build_id.clone(), outer.clone(), mesh2_id.clone()))
        );
        runner.mesh_left_after_delete(&op).await;
        assert_eq!(runner.mesh_left_in(&op, &build_id, &outer, &mesh2_id).await, Ok("already-applied"));
        assert!(!records.meshes.lock().unwrap().contains_key("mesh2"));

        // 3. `delete_mesh` also returns `Ok(())` when `OwnerResume::Left` is reached.
        records.meshes.lock().unwrap().insert("mesh2".into(), mesh2_id.clone());
        assert_eq!(runner.delete_mesh(&build_id, "mesh2", &mesh2_id).await, Ok(()));
        assert!(!records.meshes.lock().unwrap().contains_key("mesh2"));

        ep.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-delete.md, "Resume after the primary's loss", step 1:
    /// "a verdict of `reached` refuses the resume by name, since the primary is not lost". The leaving
    /// mesh2's node-admin `mesh2.node-admin.1` is silent in the fabric-primary's view, and its exact birth
    /// answers node investigation's probe (`ProbeNodeState`, `Current`): the resume ends incomplete
    /// naming it as alive, and nothing of it is deleted (no step event of its node.delete). Must NOT
    /// happen: a node-admin that answers deleted as lost, or an admin created for its mesh.
    #[tokio::test]
    async fn a_node_admin_of_the_leaving_mesh_that_answers_its_investigation_refuses_the_resume_and_is_not_deleted() {
        let Rig { runner, steps, ep, dir, mesh2_id, .. } = rig("admin-mesh-delete-reached").await;
        // mesh2's node-admin, alive: its endpoint serves its exact birth's probe.
        let (admin_id, admin_inc) = (NodeId::mint(), IncarnationId::mint());
        let at: PathName = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
        let admin_ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
        let server = {
            let (me, born) = (admin_id.clone(), admin_inc.clone());
            rdm_transport::ServerBuilder::new()
                .serve::<rdm_core::status::Status, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer: rdm_transport::PeerContext, req: StatusRequest| {
                    let (me, born) = (me.clone(), born.clone());
                    async move {
                        Ok::<_, rdm_transport::HandlerFault>(match req {
                            StatusRequest::ProbeNodeState { node_id, incarnation } if node_id == me && incarnation == born => StatusReply::Current { node_id, incarnation, state: rdm_core::status::NodeState::Ready },
                            other => rdm_core::reply::status::not_ready(format!("this cell's admin answers only the probe of its own birth, not {}", other.op())),
                        })
                    }
                })
                .build(rdm_transport::ServedBirth { node_id: admin_id.to_string(), incarnation: admin_inc.to_string() })
                .unwrap()
        };
        let router = iroh::protocol::Router::builder(admin_ep.clone()).accept(rdm_transport::ALPN, server).spawn();
        let addr = admin_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        runner.node_rpc.resolver.apply(rdm_transport::ResolvedNode { mesh: "mesh2".into(), r#type: "node-admin".into(), node_id: admin_id.clone(), path: at.path.clone(), name: at.name.clone(), endpoint_id: admin_ep.id(), transport_addr: addr, incarnation: admin_inc.clone() }, None);
        // The fabric-primary's view holds the admin, silent: it published nothing this view heard.
        let mut admin = Node::allocated(at.clone(), "mesh2", "node-admin");
        admin.node_id = admin_id.clone();
        admin.incarnation_id = Some(admin_inc.clone());
        admin.status = NodeStatus::Ready;
        runner.topology.write().await.nodes.push(admin);

        let build_id = crate::build::BuildId::mint();
        steps.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: FabricTopology::root("fab", "mesh1", MeshId::mint()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1000 }).await.unwrap();
        let key = MeshDeleteKey { mesh_id: mesh2_id.clone(), build_id: build_id.clone(), operation: crate::mesh_delete::operation(&mesh2_id) };
        let b = RosterBirth { mesh: "mesh2".into(), r#type: "node-admin".into(), node: at.clone(), node_id: admin_id.to_string(), incarnation_id: admin_inc.to_string() };
        let before = steps.read_build(&build_id).await.unwrap().steps;

        let refused = runner.delete_silent_admin(&build_id, &key, "mesh2", &b, &before).await;
        match &refused {
            Err(RunEnd::Incomplete(reason)) => assert!(reason.contains(admin_id.as_str()) && reason.contains("answers, so it survives"), "the refusal names the admin that answered: {reason}"),
            other => panic!("a node-admin that answers refuses the resume, incomplete by name: {other:?}"),
        }
        let after = steps.read_build(&build_id).await.unwrap().steps;
        let of_its_delete = rdm_core::OperationId::node("node.delete", &admin_id).to_string();
        assert!(after.iter().all(|s| s.operation != of_its_delete), "nothing of the admin that answered is deleted: {after:?}");

        router.shutdown().await.unwrap();
        admin_ep.close().await;
        ep.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
