//! The two sides of the mesh-delete workflow a runner executes (mesh-delete.md).
//!
//! The owner's side ([`AdminRunner::shutdown_mesh`]) is the fabric-primary: it owns the outer
//! workflow `shutdown-mesh:<mesh_id>` once the fabric seat is outside the deleting mesh (the
//! confirmed fabric-primary handover, `fabric_handover`). It captures the accepted roster and the
//! final primary's provider access (refusing before anything is dismantled when it has none),
//! publishes `mesh-leaving`, sends `LeaveMesh`, waits for the validated `MeshLeave` handoff
//! (mesh.deleted) and records its completion receipt: the mesh.delete ends there, the final primary
//! alive (steps 5-6). The final primary's end is a separate node.delete with the fabric-primary as
//! executor (step 7); once this admin publishes that birth deleted from its exact exit proof
//! ([`AdminRunner::mesh_left_after_delete`]) every birth is proven exited and it records `MeshLeft`
//! and publishes mesh-left (R-MD1 items 2-3).
//! The deleting mesh's primary's side ([`AdminRunner::admit_leave`]) admits `LeaveMesh`, runs the
//! member workflows (every ordinary node in parallel, then every other node-admin in parallel),
//! records one exit manifest as a step receipt of the accepted Build and calls `MeshLeave` up to
//! the owner, still running.

use super::*;
use crate::build_state::{BuildStepReceipt, StepOutcome};
use crate::deployment::pipeline::{MeshLeaveEvent, RuntimeProof, TerminalReceipt};
use crate::mesh_leave::{accepted_roster, manifest_reference, members_exited, Admitted, ExitManifest, LeaveKey, MeshLeaver, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_MESH_DELETED, STEP_MESH_LEFT};
use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
use tracing::Instrument;

/// How long the owner waits for the leaving mesh's `MeshLeave` handoff before its attempt ends
/// incomplete. A deleting primary whose member workflows end incomplete says so inside a
/// `MeshLeave` call, which ends the attempt at once; this bound covers a handoff that never
/// arrives (a lost call, a lost primary). A failed attempt ends the Build; the fabric-primary's
/// drift check (`crate::admin::reconcile_drift`) opens the next attempt while the removed mesh's
/// mesh.delete holds no completion receipt, and that attempt resumes from the receipts.
const HANDOFF_WAIT: Duration = Duration::from_secs(120);

/// What serves `LeaveMesh` and `MeshLeave` for a running admin.
pub struct LeaveDoor {
    pub(super) runner: Arc<AdminRunner>,
}

#[async_trait::async_trait]
impl MeshLeaver for LeaveDoor {
    async fn leave_mesh(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        self.runner.clone().admit_leave(sender, req).await
    }
    async fn mesh_leave(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        self.runner.validate_handoff(sender, req).await
    }
    async fn birth_deleted(&self, op: &rafka_mesh_entity::LifecycleOp) {
        self.runner.mesh_left_after_delete(op).await
    }
}

impl AdminRunner {
    /// The runtime fact the book holds for `node` (its digest).
    fn held_runtime(&self, node: &Node) -> Option<rafka_mesh_entity::RuntimeFact> {
        self.book.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref()).and_then(|d| d.node.runtime)
    }

    /// The record this admin's membership holds of the exact roster birth `b`: its digest, live or
    /// parked, as the view projects a digest (the leave's records, [`crate::mesh_leave::roster_records`]).
    fn held_record(&self, b: &RosterBirth) -> Option<Node> {
        let d = self.book.get(&b.node_id).map(|(d, _)| d).or_else(|| self.book.parked().into_iter().find(|d| d.node.node_id.as_str() == b.node_id)).filter(|d| d.node.incarnation.0 == b.incarnation_id)?;
        let mut n = Node::allocated(d.node.name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(self.provider.kind());
        n.status = crate::admin::node_status(d.status);
        n.routable = false;
        n.admin_api_base = d.admin_api_base.clone();
        n.config = d.config.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        n.deployment_id = d.node.runtime.as_ref().map(|f| DeploymentId(f.deployment_id.clone()));
        Some(n)
    }

    /// The owner's outer workflow `shutdown-mesh:<mesh_id>`.
    pub(super) async fn shutdown_mesh(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, mesh_id: &MeshId) -> Result<(), crate::executor::RunEnd> {
        let outer = crate::mesh_leave::operation(mesh_id);
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-shutdown", mesh = %mesh, mesh_id = %mesh_id, build_id = %build_id, attempt, operation = %outer, outcome = tracing::field::Empty);
        let result = self.shutdown_mesh_in(build_id, attempt, mesh, mesh_id, &outer).instrument(span.clone()).await;
        span.record("outcome", if result.is_ok() { "complete" } else { "incomplete" });
        result
    }

    async fn shutdown_mesh_in(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, mesh_id: &MeshId, outer: &str) -> Result<(), crate::executor::RunEnd> {
        // Where this owner resumes, from its own Build facts, before it calls anything.
        let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string() };
        // The view is projected now, its proven exits read from their exit-proof rows (R-EL1, R11):
        // a primary whose exit an earlier attempt proved and recorded holds no seat here, so the
        // admin created for its mesh, or the one that survived it, is read seated.
        self.refresh_view().await;
        let view = self.topology.read().await.clone();
        match crate::mesh_leave::owner_resume(&steps, outer)? {
            crate::mesh_leave::OwnerResume::Left => {
                // Resumed after the departure was recorded: the receipt is the record.
                self.forget_mesh_id(mesh);
                return Ok(());
            }
            crate::mesh_leave::OwnerResume::Deleted(_) => {
                // Resumed after mesh.deleted's completion was recorded: the mesh.delete ended, its
                // final primary alive; nothing is called. Its end is its own node.delete.
                return Ok(());
            }
            crate::mesh_leave::OwnerResume::Lead => {}
        }
        let node_rpc = &self.node_rpc;
        // Step 1: this owner is the fabric-primary. When the leaving mesh held the fabric seat it
        // was handed over first (fabric-primary-handover.md): the leave opens only on the confirmed
        // transfer, and an unconfirmed one is reconciled here before anything is dismantled.
        self.handover.confirmed(mesh_id).await.map_err(|e| format!("{outer} is refused before {mesh} is dismantled: the fabric-primary handover is not confirmed: {e}"))?;
        // The workflow is handed to a live holder. A holder that is not live keeps its seat (R-EL1)
        // but cannot take the workflow: it goes to the lost-final-primary path below, which proves
        // its exit before anything is amended (R11).
        let primary = match view.cohort_primary(mesh, NodeKind::NodeAdmin).filter(|p| p.status.is_live()).cloned() {
            Some(p) => p,
            None => self.admin_for_leaving_mesh(build_id, attempt, mesh, &key, &steps, &view).await?,
        };
        // A resumed attempt hands the workflow only to the final primary admitted, or to the one the
        // last roster amendment names. A different primary is the leaving mesh's primary lost
        // mid-leave: the lost one's exact exit is proven and recorded first, then the amendment
        // admits the new one (mesh-leave.md, "Resume after the primary's loss").
        let mut recorded = crate::mesh_leave::admitted_of(&steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))?;
        if let Some(adm) = &recorded {
            let held = RosterBirth::of(&primary)?;
            if held != adm.final_primary {
                self.amend_after_loss(&key, adm, &primary, &held).await?;
                let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
                // The predecessor's exit-proof row, put when its exit was proven, is read by this view.
                self.refresh_view().await;
                recorded = crate::mesh_leave::admitted_of(&steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))?;
            }
        }
        // The final primary's authoritative provider domain: access before anything is dismantled
        // (its node.delete, executed by this fabric-primary, proves its exit through it).
        let (_, final_handle) = self.handle_for(&primary).await.map_err(|e| format!("{outer} is refused before {mesh} is dismantled: no provider proof path for its final primary: {e}"))?;
        let final_fact = final_handle.fact().ok_or_else(|| format!("{outer} is refused before {mesh} is dismantled: the final primary {} has no exact runtime handle", primary.name))?;
        let (final_primary, final_runtime) = (RosterBirth::of(&primary)?, RuntimeProof::of(&final_fact));
        let admitted = match recorded {
            Some(adm) => {
                if adm.final_primary != final_primary {
                    return Err(format!(
                        "{outer} is refused: {mesh}'s primary is {} {}, not the final primary {} {} admitted or amended; a resumed leave hands its workflow to no other birth",
                        final_primary.node, final_primary.incarnation_id, adm.final_primary.node, adm.final_primary.incarnation_id
                    )
                    .into());
                }
                if adm.final_runtime != final_runtime {
                    return Err(format!("{outer} is refused before {mesh} is dismantled further: the final primary {}'s runtime is {final_runtime:?}, not the one admitted or amended for it {:?}", primary.name, adm.final_runtime).into());
                }
                adm
            }
            None => {
                let roster = accepted_roster(&*self.builds, &key, STEP_ACCEPTED_ROSTER, &self.me.to_string(), &final_primary, &final_runtime, || view.members().filter(|n| n.mesh == mesh && n.status.is_live()).map(RosterBirth::of).collect()).await?;
                Admitted { roster, final_primary, final_runtime, amendments: Vec::new() }
            }
        };
        if !admitted.roster.contains(&admitted.final_primary) {
            return Err(format!("{outer} is refused before {mesh} is dismantled: its primary {} {} is not a birth of the roster accepted at admission", admitted.final_primary.node, admitted.final_primary.incarnation_id).into());
        }
        let handoff = self.leaves.open(key.clone(), admitted.clone());
        let result = self.lead_leave(build_id, attempt, mesh_id, outer, node_rpc, &primary, &key, handoff).await;
        if result.is_err() {
            // This attempt's open leave ends with it; the receipts are the record a resumed attempt reads.
            self.leaves.close(&key);
        }
        result.map_err(crate::executor::RunEnd::Failed)
    }

    /// The leaving mesh holds no node-admin primary. Before admission that refuses the leave; after
    /// it, with no admin of the mesh left live, the fabric-primary resumes the leave through the one
    /// node-admin it creates for the mesh (mesh-leave.md, "Resume after the primary's loss", steps
    /// 1-2): the lost final primary's exact Exited is proven and recorded first, so nothing is born
    /// for the mesh on an unproven exit (R-D1); then the admin is created at a path no birth held,
    /// into the existing mesh under its existing MeshId. A live admin not yet seated is no loss:
    /// the attempt ends and the next one hands that admin the workflow.
    async fn admin_for_leaving_mesh(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, key: &LeaveKey, steps: &[BuildStepReceipt], view: &Topology) -> Result<Node, String> {
        let outer = key.operation.as_str();
        let no_primary = format!("{outer} is refused: {mesh} holds no node-admin primary to hand the workflow to");
        let Some(adm) = crate::mesh_leave::admitted_of(steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))? else {
            return Err(no_primary);
        };
        if let Some(live) = view.cohort(mesh, NodeKind::NodeAdmin).find(|n| n.status.is_live()) {
            return Err(format!("{no_primary}: its admin {} is live and holds no seat yet, so it is not lost; the next attempt hands the workflow to the admin seated", live.name));
        }
        let lost = adm.final_primary.clone();
        match crate::mesh_leave::exit_receipts(steps, outer, std::slice::from_ref(&lost)).remove(&(lost.node_id.clone(), lost.incarnation_id.clone())) {
            Some(_recorded) => {}
            None => {
                let store = self.records.contacts.get().ok_or_else(|| format!("{outer} is refused: {} holds no durable runtime rows to prove the lost final primary {} {}'s exit from", self.me, lost.node, lost.incarnation_id))?;
                let row = self.lost_primary_row(&**store, &lost).await.map_err(|e| format!("{outer} is refused: the lost final primary's exit cannot be proven: {e}"))?;
                let terminal = crate::mesh_leave::prove_predecessor_exit(&*self.provider, row.as_ref(), &lost, &adm.final_runtime).await.map_err(|reason| {
                    crate::mesh_leave::span_unproven(key, &lost, &reason);
                    format!("{outer} is refused: {mesh} has no live admin and its final primary {} {} is silent but not proven exited, so no admin is created for it: {reason}", lost.node, lost.incarnation_id)
                })?;
                // Recorded durably before its receipt: the exit-proof row is the proven loss every
                // projection reads.
                self.records.put_exit_of_receipt(&terminal).await.map_err(|e| format!("{outer} is refused: the lost final primary's proven exit is not recorded: {e}"))?;
                crate::mesh_leave::record_predecessor_exit(&*self.builds, key, &self.me.to_string(), &terminal).await?;
            }
        }
        // The lost primary's proven exit, read from its exit-proof row, vacates its seat in this view
        // before the name of the admin created for the mesh is claimed: the claim's writer is the
        // fabric-primary only while the mesh has no primary (R-EL1: proven loss; R11: the
        // predecessor's Exited first).
        self.refresh_view().await;
        let held: Vec<PathName> = view.nodes.iter().map(|n| n.name.clone()).collect();
        let path = crate::mesh_leave::leaving_mesh_admin_path(mesh, &adm.roster, &held)?;
        let span = tracing::info_span!("rdm.node_admin.node.create.via-leaving-mesh", mesh = %mesh, mesh_id = %key.mesh_id, node = %path, build_id = %build_id, attempt, operation = %outer, predecessor = %format!("{} {}", lost.node, lost.incarnation_id), outcome = tracing::field::Empty);
        let created = self.create_leaving_mesh_admin(build_id, attempt, &path, &key.mesh_id).instrument(span.clone()).await;
        span.record("outcome", if created.is_ok() { "created" } else { "failed" });
        created.map_err(|e| format!("{outer}: the node-admin {path} created for {mesh}, which has no live admin: {e}"))?;
        self.refresh_view().await;
        let now = self.topology.read().await.clone();
        now.cohort_primary(mesh, NodeKind::NodeAdmin).cloned().ok_or_else(|| format!("{outer}: {path} was created for {mesh} and holds no mesh-primary seat yet; the next attempt amends the roster for it"))
    }

    /// node.create of `path`, the one node-admin of a leaving mesh no admin of which survives, into
    /// the EXISTING mesh (mesh-recovery.md: "it joins under the EXISTING MeshId and never mints
    /// another"): the mesh's issuing material, as any mesh's only admin carries, and the mesh's
    /// CURRENT state as this fabric-primary holds it, handed before its Ready like any mesh's first
    /// admin is handed its mesh's state. Never Pending for a mesh past it: the state only moves
    /// forward (states.md, "pending -> ready-for-traffic -> leaving -> dead"), and a mesh that is
    /// leaving is never told it is pending. A fabric-primary that holds no state for the mesh
    /// creates nothing: no state is assumed.
    async fn create_leaving_mesh_admin(&self, build_id: &crate::build::BuildId, attempt: u32, path: &PathName, mesh_id: &MeshId) -> Result<(), String> {
        let state = self.records.declared.lock().unwrap().meshes.get(mesh_id).copied().ok_or_else(|| {
            format!("{}: this fabric-primary holds no declared state for {} ({mesh_id}), so the admin created for it could be handed none; nothing is created under an assumed state", self.me, path.mesh)
        })?;
        match self.fence_predecessor(path).await {
            crate::fence::FenceOutcome::Clear { gone: None } => {}
            other => return Err(format!("{path} is not a path no birth held: the create door fenced {other:?}")),
        }
        let hand_off = self.mesh_state_hook(path, state)?;
        let template = self.with_mesh_issuer(self.template_for(NodeKind::NodeAdmin, &path.mesh).await?, &path.mesh)?;
        let held_runtimes = self.held_runtimes().await;
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: path.clone(), spec: spec_for(NodeKind::NodeAdmin), data_dir: None, held_runtimes };
        let created = self.pipeline(&template).create_with(&req, Some(hand_off), None).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(path.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The resume after the leaving mesh's final primary `adm.final_primary` was lost and `held`
    /// (`primary`, the surviving admin the election seated or the one node-admin this fabric-primary
    /// created for the mesh) is its primary now (mesh-leave.md, "Resume after the primary's loss",
    /// steps 1-2; [`crate::mesh_leave::resume_after_loss`]): the lost birth's exit is proven from
    /// this admin's durable runtime row of it, never from the view, which drops a departed birth.
    async fn amend_after_loss(&self, key: &LeaveKey, adm: &Admitted, primary: &Node, held: &RosterBirth) -> Result<(), String> {
        let outer = key.operation.as_str();
        if primary.kind != NodeKind::NodeAdmin {
            return Err(format!("{outer} is refused: {} {} is not a node-admin and cannot be the final primary", held.node, held.incarnation_id));
        }
        let (_, handle) = self.handle_for(primary).await.map_err(|e| format!("{outer} is refused: no provider proof path for the new final primary {}: {e}", primary.name))?;
        let fact = handle.fact().ok_or_else(|| format!("{outer} is refused: the new final primary {} has no exact runtime handle", primary.name))?;
        let store = self.records.contacts.get().ok_or_else(|| format!("{outer} is refused: {} holds no durable runtime rows to prove the lost final primary {} {}'s exit from", self.me, adm.final_primary.node, adm.final_primary.incarnation_id))?;
        let row = self.lost_primary_row(&**store, &adm.final_primary).await.map_err(|e| format!("{outer} is refused: the lost final primary's exit cannot be proven: {e}"))?;
        crate::mesh_leave::resume_after_loss(&*self.builds, &*self.provider, &self.records, row.as_ref(), key, &self.me.to_string(), adm, held, &RuntimeProof::of(&fact)).await.map(|_| ())
    }

    /// The durable runtime row of the lost final primary `lost`, read by its key
    /// ([`crate::admin::birth_runtime_row`]); a roster birth whose ids do not parse is refused by name.
    async fn lost_primary_row(&self, store: &dyn crate::storage::NodesStorage, lost: &RosterBirth) -> Result<Option<crate::storage::RuntimeRow>, String> {
        let node: PathName = lost.node.parse().map_err(|e| format!("the roster names the lost final primary by `{}`, which is not a path.name: {e}", lost.node))?;
        let node_id = NodeId::parse(&lost.node_id).map_err(|e| format!("the roster names the lost final primary {} by node id `{}`, which does not parse: {e}", lost.node, lost.node_id))?;
        crate::admin::birth_runtime_row(store, &self.me, &node, &node_id, &IncarnationId(lost.incarnation_id.clone())).await
    }

    /// Steps 2-5 of the owner's workflow, for the leave `key` opened at admission: `mesh.deleting`,
    /// `LeaveMesh`, the validated handoff (mesh.deleted) and its completion receipt.
    #[allow(clippy::too_many_arguments)]
    async fn lead_leave(
        &self,
        build_id: &crate::build::BuildId,
        attempt: u32,
        mesh_id: &MeshId,
        outer: &str,
        node_rpc: &crate::node_rpc::ProcessNodeRpc,
        primary: &Node,
        key: &LeaveKey,
        mut handoff: tokio::sync::watch::Receiver<Option<crate::mesh_leave::LeaveOutcome>>,
    ) -> Result<(), String> {
        let event = MeshLeaveEvent { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string(), receipt_manifest: None, final_primary: None };
        // Step 2: the accepted command's hook, then the command.
        self.lifecycle_events.mesh_leaving(&event).await;
        let req = StatusRequest::LeaveMesh { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string() };
        let (out, _) = node_rpc.client.call::<Status>(&rafka_node_rpc::NodeTarget::ExactNode(primary.node_id.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) if matches!(r.value(), StatusReply::Applied | StatusReply::AlreadyApplied) => {}
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => return Err(format!("{outer}: {} refused leave-mesh: {} ({:?})", primary.name, r.value().name(), r.value())),
            other => return Err(format!("{outer}: leave-mesh to {} ended {} ({other:?}): the mesh may or may not have admitted it", primary.name, other.name())),
        }
        // Steps 3-4 run at the mesh-primary; its validated handoff, mesh.deleted, is what this waits
        // for. The validation covered the accepted roster (and every amendment) exactly, every birth
        // but the final primary, which reports still running.
        let manifest = match tokio::time::timeout(HANDOFF_WAIT, handoff.wait_for(|m| m.is_some())).await {
            Ok(Ok(m)) => match m.clone().expect("waited for Some") {
                Ok(manifest) => manifest,
                Err(reason) => return Err(format!("{outer}: {} reported its member workflows incomplete: {reason}", primary.name)),
            },
            Ok(Err(_)) => return Err(format!("{outer}: the open leave was closed while its handoff was awaited")),
            Err(_) => return Err(format!("{outer}: {} sent no valid mesh-leave within {HANDOFF_WAIT:?}; the workflow is incomplete and its receipts stand", primary.name)),
        };
        // Step 5: mesh.deleted ends the mesh.delete. The final primary is not deleted inside its own
        // mesh.delete: it keeps publishing its aggregate until a separate node.delete (step 7).
        self.record_mesh_deleted(key, manifest).await
    }

    /// mesh.deleted's completion (mesh-delete.md step 5): the validated manifest `manifest` (every
    /// birth of the mesh but its final primary) is recorded as the `MeshDeleted` receipt and the open
    /// leave ends. Nothing of the final primary is stopped, written or published here.
    async fn record_mesh_deleted(&self, key: &LeaveKey, manifest: ExitManifest) -> Result<(), String> {
        crate::mesh_leave::record_completion(&*self.builds, key, &self.me.to_string(), &manifest).await?;
        // The mesh-primary committed the row Dead before it called: this fabric-primary's view states
        // the mesh deleted from its acceptance on, so a node.delete of its last admin names it.
        self.records.hold_mesh_deleted(&key.mesh_id);
        self.leaves.close(key);
        self.refresh_view().await;
        Ok(())
    }

    /// mesh-left (R-MD1 items 2-3): this admin published `op`'s birth deleted from the provider's
    /// exact exit proof. When that birth is the final primary a mesh.delete's completion receipt names
    /// (a point read of the Build facts, [`crate::build_state::BuildStateAdapter::mesh_deleted_of`]),
    /// every birth of the mesh is now proven exited: the final birth's exact Exited receipt is read
    /// from the node.delete's own `TerminateRuntime` step receipt, the complete proof set is checked
    /// ([`crate::mesh_leave::mesh_left_proof`]) and recorded as `MeshLeft`, and mesh-left is published.
    /// Any other deleted birth names no completion and nothing happens. A refusal is spanned by name
    /// and leaves the node.delete as it is.
    pub(super) async fn mesh_left_after_delete(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let found = match self.builds.mesh_deleted_of(&op.node_id, &op.incarnation).await {
            Ok(Some(found)) => found,
            Ok(None) => return,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.mesh.reject.via-mesh-left", node = %self.me, subject = %op.name, node_id = %op.node_id, reason = %format!("the mesh.delete completion index could not be read: {e}"))
                    .in_scope(|| tracing::info!("mesh-left is not decided: the index of mesh.delete completions could not be read"));
                return;
            }
        };
        let (build_id, attempt, operation) = found;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-final-birth-deleted", node = %self.me, subject = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.0, build_id = %build_id, attempt, operation = %operation, deleted_by = %op.operation, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let result = self.mesh_left_in(op, &build_id, attempt, &operation).instrument(span.clone()).await;
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

    async fn mesh_left_in(&self, op: &rafka_mesh_entity::LifecycleOp, build_id: &crate::build::BuildId, attempt: u32, operation: &str) -> Result<&'static str, String> {
        let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{operation}: the Build {build_id} could not be read for its completion: {e}"))?.steps;
        if crate::mesh_leave::owner_resume(&steps, operation)? == crate::mesh_leave::OwnerResume::Left {
            return Ok("already-applied");
        }
        let completion = crate::mesh_leave::completion_of(&steps, operation).ok_or_else(|| format!("{operation}: no {STEP_MESH_DELETED} receipt is held in {build_id}"))??;
        // The final birth's exact Exited: its node.delete's TerminateRuntime receipt, in that Build.
        let deleted_in = crate::build::BuildId(op.build_id.clone());
        let terminate = crate::deployment::pipeline::RetireStep::TerminateRuntime.name();
        let terminal = self
            .builds
            .read_build(&deleted_in)
            .await
            .map_err(|e| format!("{}: the Build {deleted_in} of the node.delete could not be read for its exit proof: {e}", op.operation))?
            .steps
            .into_iter()
            .filter(|r| r.operation == op.operation && r.step == terminate && r.outcome == StepOutcome::Complete)
            .filter_map(|r| r.output.and_then(|v| serde_json::from_value::<crate::deployment::pipeline::TerminalReceipt>(v).ok()))
            .find(|t| t.node_id == op.node_id.as_str() && t.incarnation_id == op.incarnation.0)
            .ok_or_else(|| format!("{}: no exact Exited receipt of {} {} is held in {deleted_in}", op.operation, op.name, op.incarnation.0))?;
        let all = crate::mesh_leave::mesh_left_proof(&completion, &terminal).map_err(|e| format!("{operation}: the proof set is not complete: {e}"))?;
        let mesh_id = MeshId::parse(&completion.mesh_id).map_err(|e| format!("{operation}: the completion names the mesh id {:?}, which does not parse: {e}", completion.mesh_id))?;
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: operation.to_string() };
        let event = MeshLeaveEvent { mesh_id, build_id: build_id.to_string(), attempt, operation: operation.to_string(), receipt_manifest: None, final_primary: None };
        self.record_mesh_left(&key, &op.name.mesh, all, event).await?;
        Ok("applied")
    }

    /// mesh-left: the complete proof set `all` (every birth, the final primary included) is recorded
    /// as the `MeshLeft` receipt after the mesh's departed contacts are dropped and its row Dead is
    /// ensured; then the mesh-left hook runs and the mesh's id is dropped.
    async fn record_mesh_left(&self, key: &LeaveKey, mesh: &str, all: ExitManifest, event: MeshLeaveEvent) -> Result<(), String> {
        let (build_id, attempt, outer, mesh_id) = (crate::build::BuildId(key.build_id.clone()), key.attempt, key.operation.as_str(), &key.mesh_id);
        let reference = manifest_reference(&key.build_id, attempt, outer, true);
        // This fabric-primary is the acting primary of a mesh that has none left: the final birth's
        // node.delete wrote its row Deleted and proved its exit.
        crate::mesh_leave::mesh_left_rows(self.records.node_rows().as_deref(), &*self.status_storage, &*self.mesh_storage, &self.records.declared, &self.me.to_string(), key, mesh, &all.receipts).await?;
        self.builds
            .append_step_receipt(&BuildStepReceipt {
                build_id: build_id.clone(),
                attempt,
                operation: outer.to_string(),
                step: STEP_MESH_LEFT.into(),
                outcome: StepOutcome::Complete,
                output: serde_json::to_value(&all).ok(),
                executor: Some(self.me.to_string()),
            })
            .await
            .map_err(|e| format!("{outer}: the MeshLeft receipt could not be recorded: {e}"))?;
        tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-left-receipt", mesh_id = %mesh_id, build_id = %build_id, attempt, operation = %outer, receipts = all.receipts.len(), roster = all.roster.len(), manifest = %reference)
            .in_scope(|| tracing::info!("every exact exit is proven: the MeshLeft receipt is recorded"));
        self.lifecycle_events.mesh_left(&MeshLeaveEvent { receipt_manifest: Some(reference), ..event }).await;
        self.forget_mesh_id(mesh);
        self.refresh_view().await;
        Ok(())
    }

    /// A mesh that has left keeps no id here: the MeshLeft receipt is the end of that mesh, and a
    /// mesh of the same name created after it is a new mesh with a new id. The id is dropped at the
    /// receipt, never left for a later sweep to find the mesh without a member.
    fn forget_mesh_id(&self, mesh: &str) {
        if let Some(id) = self.records.meshes.lock().unwrap().remove(mesh) {
            self.records.deleted_meshes_forget(&id);
        }
    }

    /// `LeaveMesh` at the leaving mesh's primary: admit it and run the member workflows.
    pub(super) async fn admit_leave(self: Arc<Self>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::LeaveMesh { mesh_id, build_id, attempt, operation } = req else { unreachable!("admit_leave serves leave-mesh") };
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            return StatusReply::NotReady { reason: format!("{} holds no view of itself yet; leave-mesh for {mesh_id} is refused", self.me) };
        };
        let held = self.records.meshes.lock().unwrap().get(&me.mesh).cloned().or_else(|| view.meshes.iter().find(|m| m.name == me.mesh).and_then(|m| m.id.clone()));
        match held {
            Some(h) if &h != mesh_id => return StatusReply::RejectedStaleMesh { held: h },
            None => return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("an admin of the mesh {mesh_id}") } },
            Some(_) => {}
        }
        if !me.is_primary {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("mesh-primary of {}", me.mesh) } };
        }
        // The handoff goes back to whoever owns the workflow: the admin that sent this command, else
        // the fabric-primary this admin's view names (LeaveMesh is the fabric-primary's command,
        // node-rpc-envelope.md "Mesh delete command pair"). Nothing checks who is calling
        // (node-rpc.md §14).
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() };
        let owner = match crate::mesh_leave::leave_owner(sender.map(|n| &n.node_id), view.fabric_primary().map(|n| &n.node_id), &self.me.to_string(), &key) {
            Ok(owner) => owner,
            Err(refused) => return refused,
        };
        // The mesh row Leaving and the round's admission (the command hook's mesh-leaving is the
        // owner's): [`crate::mesh_leave::admit_leave_mesh`].
        let crate::mesh_leave::LeaveMeshAdmission { leaving_row, admission } = crate::mesh_leave::admit_leave_mesh(&self.leave_runs, &*self.status_storage, &self.records.declared, &key, &owner).await;
        if let Some(committed) = &leaving_row {
            tracing::info_span!("rdm.node_admin.mesh.update.via-leaving-row", node = %self.me, mesh = %me.mesh, mesh_id = %mesh_id, build_id = %build_id, attempt = *attempt, operation = %operation, owner = %sender.map(|n| n.name.to_string()).unwrap_or_default(), outcome = committed.name())
                .in_scope(|| tracing::info!("leave-mesh at the mesh's primary: its own mesh row Leaving is stored and applied before the answer"));
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
            crate::mesh_leave::LeaveAdmission::Running => return StatusReply::AlreadyApplied,
            crate::mesh_leave::LeaveAdmission::Resend(manifest) => Some(manifest),
            crate::mesh_leave::LeaveAdmission::Start => None,
            crate::mesh_leave::LeaveAdmission::Refused(reply) => return reply,
        };
        // The command's serve span carries the caller's propagated trace; the workflow it starts
        // continues it, so every member's calls join the outer trace.
        let parent = tracing::Span::current();
        let runner = self.clone();
        match resend {
            // The members all exited and the manifest is recorded: the handoff is what repeats.
            Some(manifest) => {
                tokio::spawn(async move { runner.hand_off(&key, &owner, &manifest).instrument(parent).await });
                StatusReply::AlreadyApplied
            }
            None => {
                tokio::spawn(async move { runner.run_leave(key, parent).await });
                StatusReply::Applied
            }
        }
    }

    /// The one run of the member workflows of the leave `key` admitted, on its natural key
    /// `(mesh_id, Leaving)` ([`crate::mesh_leave::run_admitted`]): a `leave-mesh` of a later attempt
    /// admitted meanwhile joins it, and the handoff goes under the key admitted last.
    async fn run_leave(self: Arc<Self>, key: LeaveKey, parent: tracing::Span) {
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.mesh.update.via-leave-members", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, outcome = tracing::field::Empty, handoff_attempt = tracing::field::Empty);
        async {
            let end = crate::mesh_leave::run_admitted(&self.leave_runs, &key.mesh_id, |key, owner| {
                let runner = self.clone();
                async move { runner.leave_members(&key, &owner).await }
            })
            .await;
            match end {
                crate::mesh_leave::LeaveRunEnd::Exited { key, owner, manifest } => {
                    tracing::Span::current().record("outcome", "exited");
                    tracing::Span::current().record("handoff_attempt", key.attempt);
                    // The handoff goes to the owner that sent leave-mesh last, read now.
                    self.hand_off(&key, &owner, &manifest).await;
                }
                crate::mesh_leave::LeaveRunEnd::Incomplete { key, owner, reason } => {
                    tracing::Span::current().record("outcome", "incomplete");
                    tracing::Span::current().record("handoff_attempt", key.attempt);
                    // The run ends; its receipts stand. Every member that exited holds its exact
                    // Exited step receipt, and the roster its LeavingRoster receipt: a repeated
                    // leave-mesh, or the Build's next attempt, runs only the members without one.
                    tracing::info!(error = %reason, "the member workflows are incomplete: the owner is told now");
                    // The owner learns it inside the call: mesh-leave with no recorded manifest,
                    // which it refuses by name (RejectedIncompleteManifest, the births without an
                    // Exited receipt named) and ends its open leave with. No mesh-leave gossip runs:
                    // this is no handoff.
                    self.call_mesh_leave(&key, &owner, "incomplete", 0).await;
                }
                crate::mesh_leave::LeaveRunEnd::Forgotten => tracing::info!("this leave's run is no longer held: the handoff waits for a repeated leave-mesh"),
            }
        }
        .instrument(span)
        .await
    }

    /// Both tiers over the accepted roster, then the one manifest receipt
    /// ([`crate::mesh_leave::members_exited`]). `owner` is the fabric-primary that sent `LeaveMesh`:
    /// the Build facts it holds are read when this admin's own do not yet admit it.
    async fn leave_members(&self, key: &LeaveKey, owner: &NodeId) -> Result<ExitManifest, String> {
        let view = self.topology.read().await.clone();
        let me = view.own(&self.node_id).cloned().ok_or_else(|| format!("{} holds no view of itself", self.me))?;
        let my_fact = self.held_runtime(&me).ok_or_else(|| format!("{} publishes no runtime fact: its own exact runtime cannot be named", me.name))?;
        let me_birth = RosterBirth::of(&me)?;
        let my_runtime = RuntimeProof::of(&my_fact);
        // The roster captured once, or, when this admin holds none, what the owner admitted: the
        // roster amendments the fabric-primary recorded (a primary lost mid-leave) are read from the
        // owner's Build facts when this admin has not yet heard them ([`crate::mesh_leave::leaving_roster`]).
        let build_id = crate::build::BuildId(key.build_id.clone());
        let fetch_owner = || async {
            crate::build_facts_read::fetch_build_facts(&self.node_rpc.client, &rafka_node_rpc::NodeTarget::ExactNode(owner.clone()), &build_id).await.map(|f| f.facts).map_err(|e| format!("{}: the owner {owner}'s Build facts of {build_id} could not be read for the leave's roster: {e}", self.me))
        };
        let capture = || {
            let mut roster = view.members().filter(|n| n.mesh == me.mesh && n.status.is_live() && n.node_id != me.node_id).map(RosterBirth::of).collect::<Result<Vec<_>, _>>()?;
            roster.push(me_birth.clone());
            Ok(roster)
        };
        let (roster, known) = crate::mesh_leave::leaving_roster(&*self.builds, &*self.local, key, &self.me.to_string(), &me_birth, &my_runtime, capture, fetch_owner).await?;
        let mesh: Vec<Node> = view.nodes.iter().filter(|n| n.mesh == me.mesh).cloned().collect();
        let records = crate::mesh_leave::roster_records(&mesh, &roster, |b| self.held_record(b));
        crate::mesh_leave::roster_rows_read(self.records.node_rows().as_deref(), &self.me.to_string(), &key.operation, &roster, &records).await;
        let records = &records;
        members_exited(&*self.builds, key, &self.me.to_string(), &roster, &me_birth, my_runtime, records, &known, |n| async move { self.shutdown_one(key, &n).await }, |b| async move { crate::mesh_leave::birth_row_deleted(self.records.node_rows().as_deref(), &self.me.to_string(), &key.operation, records, &b).await }).await
    }

    /// One member's end (mesh-delete.md step 3: "The mesh-primary runs the whole node.delete for
    /// every ordinary node"): the ordinary node.delete workflow ([`AdminRunner::delete_birth`]) of the
    /// exact birth, under the mesh.delete's Build and attempt, and its exact Exited recorded as the
    /// birth's exit-proof row. The provider's terminal receipt is the member's roster receipt.
    async fn shutdown_one(&self, key: &LeaveKey, n: &Node) -> Result<TerminalReceipt, String> {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-shutdown-member", node = %n.name, node_id = %n.node_id, operation = %key.operation, build_id = %key.build_id, attempt = key.attempt, outcome = tracing::field::Empty);
        let result = async {
            let (record, handle) = self.handle_for(n).await?;
            // Whether the birth is stopped is the record's the leave works from (a stopped member's
            // delete goes straight to its exit proof), as `retire` takes it from the view.
            let record = Node { parked: n.parked, ..record };
            let terminal = self.delete_birth(&crate::build::BuildId(key.build_id.clone()), key.attempt, record, handle).await.map_err(|e| format!("{}'s node.delete: {e}", n.name))?;
            self.records.put_exit_of_receipt(&terminal).await.map_err(|e| format!("{}'s exit is not recorded: {e}", n.name))?;
            Ok(terminal)
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", if result.is_ok() { "exited" } else { "incomplete" });
        result
    }

    /// The `MeshLeave` handoff: the mesh-leave gossip hook, then the call up to the owner.
    async fn hand_off(&self, key: &LeaveKey, owner: &NodeId, manifest: &ExitManifest) {
        let reference = manifest_reference(&key.build_id, key.attempt, &key.operation, false);
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            tracing::info!(operation = %key.operation, "this admin holds no view of itself: the handoff waits for a repeated leave-mesh");
            return;
        };
        if self.held_runtime(&me).is_none() {
            tracing::info!(node = %me.name, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        }
        let (Some(final_incarnation), final_node_id) = (me.incarnation_id.clone(), me.node_id.clone()) else { return };
        // mesh.deleted (ops-naming mesh.delete: "reported by the mesh-primary, still alive, from its
        // members' exit proofs; mesh is dead, id kept retired"): every other member's exit is proven,
        // so this primary commits its mesh row Dead (states.md: "one commit stores the row, applies
        // it, gossips it and answers"); its view then states the mesh Dead and its status round
        // publishes MeshStatus Dead, five sends, while it lives (R-NS1 item 1). A refused put
        // answers no handoff: the owner's repeated leave-mesh decides it afresh.
        // Its Mesh row is written Dead on mesh.storage with it, which releases the mesh's name (R-MN1
        // item 2); a refused write answers no handoff either.
        let committed = crate::status_rpc::commit_mesh_state(&*self.status_storage, &self.records.declared, &key.mesh_id, rafka_node_rpc_contract::status::MeshState::Dead).await;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-dead-row", node = %self.me, mesh = %me.mesh, mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, receipts = manifest.receipts.len(), outcome = committed.name(), mesh_row = tracing::field::Empty, error = tracing::field::Empty);
        let mesh_row = match committed {
            StatusReply::Applied | StatusReply::AlreadyApplied => crate::mesh_leave::mesh_row_dead(&*self.mesh_storage, &me.mesh, &key.mesh_id).await,
            refused => Err(format!("the Dead status row was not committed: {}", refused.name())),
        };
        match &mesh_row {
            Ok(row) => span.record("mesh_row", *row),
            Err(e) => span.record("mesh_row", "not-written").record("error", e.as_str()),
        };
        span.in_scope(|| tracing::info!("mesh.deleted at the mesh's primary: its own mesh row Dead is stored and applied from its members' exit proofs, and its name released"));
        if let Err(e) = mesh_row {
            tracing::info!(operation = %key.operation, error = %e, "the mesh row Dead was not committed: the handoff waits for a repeated leave-mesh");
            return;
        }
        self.refresh_view().await;
        self.lifecycle_events
            .mesh_leave(&MeshLeaveEvent { mesh_id: key.mesh_id.clone(), build_id: key.build_id.clone(), attempt: key.attempt, operation: key.operation.clone(), receipt_manifest: Some(reference), final_primary: Some((final_node_id, final_incarnation)) })
            .await;
        self.call_mesh_leave(key, owner, "complete", manifest.receipts.len()).await;
    }

    /// The `MeshLeave` call up to the owner: this primary's exact birth and runtime, and the
    /// reference of its exit manifest receipt. `manifest` says whether that receipt is recorded
    /// (`complete`) or the member workflows ended without one (`incomplete`).
    async fn call_mesh_leave(&self, key: &LeaveKey, owner: &NodeId, manifest: &'static str, manifest_receipts: usize) {
        let reference = manifest_reference(&key.build_id, key.attempt, &key.operation, false);
        let view = self.topology.read().await.clone();
        let node_rpc = &self.node_rpc;
        let (Some(me), Some(owner_node)) = (view.own(&self.node_id).cloned(), view.nodes.iter().find(|n| &n.node_id == owner).cloned()) else {
            tracing::info!(operation = %key.operation, owner = %owner, "the owner of this workflow is not held in this view: the handoff waits for a repeated leave-mesh");
            return;
        };
        let Some(final_runtime) = self.held_runtime(&me) else {
            tracing::info!(node = %me.name, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        };
        let (Some(final_incarnation), final_node_id) = (me.incarnation_id.clone(), me.node_id.clone()) else { return };
        let req = StatusRequest::MeshLeave { mesh_id: key.mesh_id.clone(), build_id: key.build_id.clone(), attempt: key.attempt, operation: key.operation.clone(), final_node_id, final_incarnation, final_runtime, receipt_manifest: reference };
        let (out, _) = node_rpc.client.call::<Status>(&rafka_node_rpc::NodeTarget::ExactNode(owner_node.node_id.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        let outcome = match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => format!("{} {:?}", r.value().name(), r.value()),
            other => format!("{} {other:?}", other.name()),
        };
        tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-sent", mesh_id = %key.mesh_id, operation = %key.operation, owner = %owner_node.name, manifest, manifest_receipts, outcome = %outcome)
            .in_scope(|| tracing::info!("the mesh-leave handoff was called"));
    }

    /// `MeshLeave` at the owner: read the manifest from the sender, validate it against the roster
    /// captured at admission and the final primary's provider access, and release the outer
    /// workflow. A reference alone is not proof.
    pub(super) async fn validate_handoff(&self, _sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::MeshLeave { mesh_id, build_id, attempt, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest } = req else { unreachable!("validate_handoff serves mesh-leave") };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-validated", mesh_id = %mesh_id, build_id = %build_id, attempt = *attempt, operation = %operation, outcome = tracing::field::Empty, field = tracing::field::Empty, without_exit = tracing::field::Empty);
        let reply = self.validate_handoff_in(mesh_id, build_id, *attempt, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        if let StatusReply::RejectedUnmatchedCompletion { field, .. } = &reply {
            span.record("field", field.as_str());
        }
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn validate_handoff_in(&self, mesh_id: &MeshId, build_id: &str, attempt: u32, operation: &str, final_node_id: &NodeId, final_incarnation: &IncarnationId, final_runtime: &rafka_mesh_entity::RuntimeFact, receipt_manifest: &str) -> StatusReply {
        let unmatched = |field: &str, expected: String, reported: String| StatusReply::RejectedUnmatchedCompletion { field: field.into(), expected, reported };
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: operation.to_string() };
        let admitted = match self.leaves.admitted_for(&key) {
            Ok(admitted) => admitted,
            Err(refused) => {
                // No open leave: after mesh.deleted's completion receipt the handoff is a replay,
                // answered from the receipt (AlreadyApplied, no second receipt).
                let build = crate::build::BuildId(build_id.to_string());
                match self.builds.read_build(&build).await {
                    Ok(b) => {
                        if let Some(reply) = crate::mesh_leave::replayed_handoff(&b.steps, &key, final_node_id, &final_incarnation.0) {
                            return reply;
                        }
                    }
                    // A Build this admin holds no fact of holds no completion of it either.
                    Err(crate::build_state::BuildStateError::UnknownBuild(_)) => {}
                    Err(e) => return crate::mesh_leave::refuse_source_failed("mesh-leave", &key, &format!("the Build log of {}", self.me), &format!("read {build} for the completion of {operation}"), &e.to_string()),
                }
                return refused;
            }
        };
        if receipt_manifest.len() > rafka_node_rpc_contract::status::MAX_RECEIPT_MANIFEST_BYTES {
            return unmatched("receipt_manifest", format!("at most {} bytes", rafka_node_rpc_contract::status::MAX_RECEIPT_MANIFEST_BYTES), format!("{} bytes", receipt_manifest.len()));
        }
        if final_node_id.to_string() != admitted.final_primary.node_id || final_incarnation.0 != admitted.final_primary.incarnation_id {
            return unmatched("final_primary", format!("{} {}", admitted.final_primary.node_id, admitted.final_primary.incarnation_id), format!("{final_node_id} {}", final_incarnation.0));
        }
        // The final primary's exact runtime, and this admin's provider access to it.
        if RuntimeProof::of(final_runtime) != admitted.final_runtime {
            return unmatched("final_runtime", format!("{:?}", admitted.final_runtime), format!("{:?}", RuntimeProof::of(final_runtime)));
        }
        if let Err(refused) = crate::mesh_leave::adopt_final_runtime(&*self.provider, final_runtime, &self.me.to_string(), &key) {
            return refused;
        }
        // The manifest: the sender's Build facts, read now.
        let node_rpc = &self.node_rpc;
        let read = crate::build_facts_read::fetch_build_facts(&node_rpc.client, &rafka_node_rpc::NodeTarget::ExactNode(final_node_id.clone()), &crate::build::BuildId(build_id.to_string())).await.map(|f| f.facts);
        let verdict = match crate::mesh_leave::handoff_verdict_from_read(read, &key, &admitted, receipt_manifest, &self.me.to_string(), &final_node_id.to_string()) {
            Ok(v) => v,
            Err(refused) => return refused,
        };
        let manifest = match verdict {
            crate::mesh_leave::HandoffVerdict::Accept(m) => m,
            refused => {
                if let crate::mesh_leave::HandoffVerdict::Incomplete { reason, without_exit } = &refused {
                    // The leaving primary recorded no manifest: the open leave ends now with the
                    // births named, and this attempt ends inside the call.
                    tracing::Span::current().record("without_exit", without_exit.join(", ").as_str());
                    self.leaves.incomplete(&key, reason.clone());
                }
                return refused.refusal().expect("a verdict other than Accept refuses");
            }
        };
        if self.leaves.resolve(&key, manifest) {
            StatusReply::Applied
        } else {
            StatusReply::AlreadyApplied
        }
    }
}
