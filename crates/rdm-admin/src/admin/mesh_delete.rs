//! The two sides of the mesh-delete workflow a runner executes (mesh-delete.md).
//!
//! The owner's side ([`AdminRunner::delete_mesh`]) is the fabric-primary: it owns the outer
//! workflow `mesh.delete:<mesh_id>` once the fabric seat is outside the deleting mesh (the
//! confirmed fabric-primary handover, `fabric_handover`). It captures the accepted roster and the
//! final primary's provider access (refusing before anything is dismantled when it has none),
//! publishes `mesh-leaving`, sends `DeleteMesh`, waits for the validated `MeshDeleted` handoff
//! (mesh.deleted) and records its completion receipt: the mesh.delete ends there, the final primary
//! alive (steps 5-6). The final primary's end is a separate node.delete with the fabric-primary as
//! executor (step 7); once this admin publishes that birth deleted from its exact exit proof
//! ([`AdminRunner::mesh_left_after_delete`]) every birth is proven exited and it records `MeshLeft`
//! and publishes mesh-left (R-MD1 items 2-3).
//! The deleting mesh's primary's side ([`AdminRunner::admit_delete_mesh`]) admits `DeleteMesh`, runs the
//! member workflows (every ordinary node in parallel, then every other node-admin in parallel),
//! records one exit manifest as a step receipt of the accepted Build and calls `MeshDeleted` up to
//! the owner, still running.

use super::*;
use crate::build_state::{BuildStepReceipt, StepOutcome};
use crate::executor::RunEnd;
use crate::deployment::pipeline::{RuntimeProof, TerminalReceipt};
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
    /// The runtime fact the book holds for `node` (its digest).
    fn held_runtime(&self, node: &Node) -> Option<rdm_core::RuntimeFact> {
        self.book.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref()).and_then(|d| d.node.runtime)
    }

    /// The record this admin's membership holds of the exact roster birth `b`: its digest, live or
    /// parked, as the view projects a digest (the leave's records, [`crate::mesh_delete::roster_records`]).
    fn held_record(&self, b: &RosterBirth) -> Option<Node> {
        let d = self.book.get(&b.node_id).map(|(d, _)| d).or_else(|| self.book.parked().into_iter().find(|d| d.node.node_id.as_str() == b.node_id)).filter(|d| d.node.incarnation.as_str() == b.incarnation_id)?;
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
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        Some(n)
    }

    /// The owner's outer workflow `mesh.delete:<mesh_id>`.
    pub(super) async fn delete_mesh(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, mesh_id: &MeshId) -> Result<(), crate::executor::RunEnd> {
        let outer = crate::mesh_delete::operation(mesh_id);
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-shutdown", mesh = %mesh, mesh_id = %mesh_id, build_id = %build_id.log(), attempt, operation = %outer, outcome = tracing::field::Empty);
        let result = self.delete_mesh_in(build_id, attempt, mesh, mesh_id, &outer).instrument(span.clone()).await;
        span.record("outcome", if result.is_ok() { "complete" } else { "incomplete" });
        result
    }

    async fn delete_mesh_in(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, mesh_id: &MeshId, outer: &str) -> Result<(), crate::executor::RunEnd> {
        // Where this owner resumes, from its own Build facts, before it calls anything; the run this
        // attempt belongs to keys the fabric seat's update (ruling R-PU2).
        let build = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its receipts: {e}"))?;
        let (steps, run) = (build.steps.clone(), build.run_of(attempt));
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.to_string() };
        // The view is projected now: a primary an earlier attempt deleted holds no seat here, so the
        // admin created for its mesh, or the one that survived it, is read seated.
        self.refresh_view().await;
        let view = self.topology.read().await.clone();
        match crate::mesh_delete::owner_resume(&steps, outer)? {
            crate::mesh_delete::OwnerResume::Left => {
                // Resumed after the departure was recorded: the receipt is the record.
                self.forget_mesh_id(mesh);
                return Ok(());
            }
            crate::mesh_delete::OwnerResume::Deleted(_) => {
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
        // the fabric-primary's own mesh, the fabric-primary seat stays put so mesh.delete skips its
        // node.fabric-primary.update step (lifecycles/fabric-delete.md step 1).
        if !build.topology.meshes.is_empty() || mesh != self.me.mesh.as_str() {
            let fabric_update = crate::fabric_handover::update_operation(rdm_core::Seat::FabricPrimary, build_id.as_str(), run, outer);
            self.handover.fabric_seat_updated(mesh, &fabric_update).await.map_err(|e| format!("{outer} is refused before {mesh} is dismantled: {e}"))?;
        }
        // The workflow is handed to a live holder. A holder that is not live keeps its seat (R-EL1)
        // but cannot take the workflow: it goes to the lost-final-primary path below, which deletes it
        // before anything is amended (R11). During fabric.delete, an unanswered mesh with no prior
        // AcceptedRoster resolves by deleting its unreached births (R-FS1 revised).
        let mut steps = steps;
        let primary = match view.cohort_primary(mesh, NodeKind::NodeAdmin).filter(|p| p.status.is_live()).cloned() {
            Some(p) => p,
            None => {
                if build.topology.meshes.is_empty() && crate::mesh_delete::admitted_of(&steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))?.is_none() {
                    let runtime_store = self.records.contacts.get().ok_or_else(|| format!("{outer}: {} holds no durable runtime rows for unanswered mesh {mesh}", self.me))?;
                    let node_rows = self.records.node_rows();
                    let held_paths: Vec<crate::model::PathName> = view.nodes.iter().map(|n| n.name.clone()).collect();
                    // Each unreached birth's end is its own node.delete, this fabric-primary the executor,
                    // under this Build and attempt.
                    let delete = |b: RosterBirth| async move { self.delete_lost_birth(build_id, attempt, &b).await.map_err(|e| e.to_string()) };
                    match crate::mesh_delete::unanswered_mesh_delete(&*self.builds, &*self.provider, &self.records, node_rows.as_deref(), &*self.status_storage, &*self.mesh_storage, &**runtime_store, &key, &self.me.to_string(), mesh, &view.nodes, &held_paths, delete).await? {
                        crate::mesh_delete::UnansweredMeshOutcome::FullyCrashed(_) => {
                            self.book.depart_mesh_births(mesh, build_id.as_str(), attempt, self.lifecycle_events.now_rafka_ms());
                            self.records.hold_mesh_deleted(&key.mesh_id);
                            self.forget_mesh_id(mesh);
                            self.refresh_view().await;
                            return Ok(());
                        }
                        crate::mesh_delete::UnansweredMeshOutcome::CreateAdmin { .. } => {
                            self.records.declared.lock().unwrap().meshes.entry(key.mesh_id.clone()).or_insert(rdm_core::status::MeshState::ReadyForTraffic);
                            steps = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
                        }
                    }
                }
                self.admin_for_deleting_mesh(build_id, attempt, mesh, &key, &steps, &view).await?
            }
        };
        // A resumed attempt hands the workflow only to the final primary admitted, or to the one the
        // last roster amendment names. A different primary is the leaving mesh's primary lost
        // mid-leave: the lost one is deleted first, then the amendment admits the new one
        // (mesh-delete.md, "Resume after the primary's loss").
        let mut recorded = crate::mesh_delete::admitted_of(&steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))?;
        if let Some(adm) = &recorded {
            let held = RosterBirth::of(&primary)?;
            if held != adm.final_primary {
                self.amend_after_loss(&key, adm, &primary, &held).await?;
                let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
                // The predecessor's departure is read by this view.
                self.refresh_view().await;
                recorded = crate::mesh_delete::admitted_of(&steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))?;
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
                let roster = accepted_roster(&*self.builds, &key, STEP_ACCEPTED_ROSTER, &self.held_name().to_string(), &final_primary, &final_runtime, || view.members().filter(|n| n.mesh == mesh && n.status.is_live()).map(RosterBirth::of).collect()).await?;
                Admitted { roster, final_primary, final_runtime, amendments: Vec::new() }
            }
        };
        if !admitted.roster.contains(&admitted.final_primary) {
            return Err(format!("{outer} is refused before {mesh} is dismantled: its primary {} {} is not a birth of the roster accepted at admission", admitted.final_primary.node, admitted.final_primary.incarnation_id).into());
        }
        let handoff = self.leaves.open(key.clone(), admitted.clone());
        let result = self.lead_delete(build_id, attempt, mesh_id, outer, node_rpc, &primary, &key, handoff).await;
        if result.is_err() {
            // This attempt's open leave ends with it; the receipts are the record a resumed attempt reads.
            self.leaves.close(&key);
        }
        result
    }

    /// The leaving mesh holds no node-admin primary. Before admission that refuses the leave; after
    /// it, with no admin of the mesh left live, the fabric-primary resumes the leave through the one
    /// node-admin it creates for the mesh (mesh-delete.md, "Resume after the primary's loss", steps
    /// 1-2): the lost final primary is deleted first, `TerminateRuntime` first when it runs; then the
    /// admin is created at a path no birth held,
    /// into the existing mesh under its existing MeshId. A live admin not yet seated is no loss:
    /// the attempt ends and the next one hands that admin the workflow.
    async fn admin_for_deleting_mesh(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, key: &MeshDeleteKey, steps: &[BuildStepReceipt], view: &Topology) -> Result<Node, RunEnd> {
        let outer = key.operation.as_str();
        let no_primary = format!("{outer} is refused: {mesh} holds no node-admin primary to hand the workflow to");
        // Before admission the leave is refused by name: nothing was dismantled and nothing is resumed.
        let Some(adm) = crate::mesh_delete::admitted_of(steps, outer).transpose().map_err(|e| format!("{outer}: {e}"))? else {
            return Err(RunEnd::failed(no_primary));
        };
        if let Some(live) = view.cohort(mesh, NodeKind::NodeAdmin).find(|n| n.status.is_live()) {
            return Err(RunEnd::Incomplete(format!("{no_primary}: its admin {} is live and holds no seat yet, so it is not lost; the next attempt hands the workflow to the admin seated", live.name)));
        }
        let lost = adm.final_primary.clone();
        match crate::mesh_delete::exit_receipts(steps, outer, std::slice::from_ref(&lost)).remove(&(lost.node_id.clone(), lost.incarnation_id.clone())) {
            Some(_recorded) => {}
            None => {
                let store = self.records.contacts.get().ok_or_else(|| format!("{outer} is refused: {} holds no durable runtime rows of the lost final primary {} {}", self.held_name(), lost.node, lost.incarnation_id))?;
                let row = self.roster_birth_row(&**store, &lost).await.map_err(|e| format!("{outer} is refused: the lost final primary's runtime cannot be read: {e}"))?;
                // The lost primary's verdict (mesh-delete.md "Resume after the primary's loss" step 1),
                // then its node.delete, this fabric-primary the executor, which records the roster
                // receipt the amendment rests on. A runtime unknown here refuses the resume by name.
                crate::mesh_delete::predecessor_verdict(&*self.provider, row.as_ref(), &lost, &adm.final_runtime).await.map_err(|reason| {
                    crate::mesh_delete::span_unproven(key, &lost, &reason);
                    RunEnd::Incomplete(format!("{outer} is refused: {mesh} has no live admin and the runtime of its final primary {} {} is not known here, so no admin is created for it: {reason}", lost.node, lost.incarnation_id))
                })?;
                self.delete_lost_birth(build_id, attempt, &lost).await?;
            }
        }
        // None survives: every other node-admin of the roster, silent in this view, is deleted before
        // anything is created for the mesh.
        for other in adm.roster.iter().filter(|b| **b != lost && b.node.parse::<PathName>().is_ok_and(|p| p.kind == NodeKind::NodeAdmin && p.mesh == mesh)) {
            self.delete_silent_admin(build_id, attempt, key, mesh, other, steps).await?;
        }
        // The lost primary's departure vacates its seat in this view before the name of the admin
        // created for the mesh is claimed: the claim's writer is the fabric-primary only while the
        // mesh has no primary (R11: the predecessor's delete first).
        self.refresh_view().await;
        let held: Vec<PathName> = view.nodes.iter().map(|n| n.name.clone()).collect();
        let path = crate::mesh_delete::leaving_mesh_admin_path(mesh, &adm.roster, &held)?;
        let span = tracing::info_span!("rdm.node_admin.node.create.via-leaving-mesh", mesh = %mesh, mesh_id = %key.mesh_id, node = %path, build_id = %build_id.log(), attempt, operation = %outer, predecessor = %format!("{} {}", lost.node, lost.incarnation_id), outcome = tracing::field::Empty);
        let created = self.create_deleting_mesh_admin(build_id, attempt, &path, &key.mesh_id).instrument(span.clone()).await;
        span.record("outcome", if created.is_ok() { "created" } else { "failed" });
        created.map_err(|e| format!("{outer}: the node-admin {path} created for {mesh}, which has no live admin: {e}"))?;
        self.refresh_view().await;
        let now = self.topology.read().await.clone();
        now.cohort_primary(mesh, NodeKind::NodeAdmin).cloned().ok_or_else(|| RunEnd::Incomplete(format!("{outer}: {path} was created for {mesh} and holds no mesh-primary seat yet; the next attempt amends the roster for it")))
    }

    /// node.create of `path`, the one node-admin of a leaving mesh no admin of which survives, into
    /// the EXISTING mesh (mesh-recovery.md: "it joins under the EXISTING MeshId and never mints
    /// another"): the mesh's issuing material, as any mesh's only admin carries, and the mesh's
    /// CURRENT state as this fabric-primary holds it, handed before its Ready like any mesh's first
    /// admin is handed its mesh's state. Never Pending for a mesh past it: the state only moves
    /// forward (states.md, "pending -> ready-for-traffic -> leaving -> dead"), and a mesh that is
    /// leaving is never told it is pending. A fabric-primary that holds no state for the mesh
    /// creates nothing: no state is assumed.
    async fn create_deleting_mesh_admin(&self, build_id: &crate::build::BuildId, attempt: u32, path: &PathName, mesh_id: &MeshId) -> Result<(), String> {
        let state = self.records.declared.lock().unwrap().meshes.get(mesh_id).copied().ok_or_else(|| {
            format!("{}: this fabric-primary holds no declared state for {} ({mesh_id}), so the admin created for it could be handed none; nothing is created under an assumed state", self.held_name(), path.mesh)
        })?;
        match self.revise_predecessor(path).await {
            crate::seat_revision::FenceOutcome::Clear { gone: None } => {}
            other => return Err(format!("{path} is not a path no birth held: the create door fenced {other:?}")),
        }
        let hand_off = self.mesh_state_hook(path, state)?;
        let template = self.with_issuing_material(self.create_template(path, None).await?, &path.mesh).await?;
        let held_runtimes = self.held_runtimes().await;
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: path.clone(), spec: spec_for(NodeKind::NodeAdmin), data_dir: None, held_runtimes };
        let created = self.pipeline(&template).create_with(&req, Some(hand_off), None).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(path.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The resume after the leaving mesh's final primary `adm.final_primary` was lost and `held`
    /// (`primary`, the surviving admin the election seated or the one node-admin this fabric-primary
    /// created for the mesh) is its primary now (mesh-delete.md, "Resume after the primary's loss",
    /// steps 1-2; [`crate::mesh_delete::resume_after_loss`]): the lost birth's runtime is read from
    /// this admin's durable runtime row of it, never from the view, which drops a departed birth.
    async fn amend_after_loss(&self, key: &MeshDeleteKey, adm: &Admitted, primary: &Node, held: &RosterBirth) -> Result<(), RunEnd> {
        let outer = key.operation.as_str();
        if primary.kind != NodeKind::NodeAdmin {
            return Err(format!("{outer} is refused: {} {} is not a node-admin and cannot be the final primary", held.node, held.incarnation_id).into());
        }
        let (_, handle) = self.handle_for(primary).await.map_err(|e| format!("{outer} is refused: no provider proof path for the new final primary {}: {e}", primary.name))?;
        let fact = handle.fact().ok_or_else(|| format!("{outer} is refused: the new final primary {} has no exact runtime handle", primary.name))?;
        let me = self.held_name();
        let store = self.records.contacts.get().ok_or_else(|| format!("{outer} is refused: {me} holds no durable runtime rows of the lost final primary {} {}", adm.final_primary.node, adm.final_primary.incarnation_id))?;
        let row = self.roster_birth_row(&**store, &adm.final_primary).await.map_err(|e| format!("{outer} is refused: the lost final primary's runtime cannot be read: {e}"))?;
        let lost = adm.final_primary.clone();
        crate::mesh_delete::resume_after_loss(&*self.builds, &*self.provider, row.as_ref(), key, &me.to_string(), adm, held, &RuntimeProof::of(&fact), || async move { self.delete_lost_birth(&key.build_id, key.attempt, &lost).await.map(|_| ()) }).await.map(|_| ())
    }

    /// The durable runtime row of the roster birth `b` (the lost final primary, or another
    /// node-admin of the leaving mesh), read by its key ([`crate::admin::birth_runtime_row`]); a
    /// roster birth whose ids do not parse is refused by name.
    async fn roster_birth_row(&self, store: &dyn crate::storage::NodesStorage, b: &RosterBirth) -> Result<Option<crate::storage::RuntimeRow>, String> {
        let node: PathName = b.node.parse().map_err(|e| format!("the roster names a birth by `{}`, which is not a path.name: {e}", b.node))?;
        let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("the roster names {} by node id `{}`, which does not parse: {e}", b.node, b.node_id))?;
        crate::admin::birth_runtime_row(store, &self.held_name(), &node, &node_id, &b.birth().map_err(|e| format!("the roster names {} at birth `{}`, which does not parse: {e}", b.node, b.incarnation_id))?).await
    }

    /// "When none survives" (mesh-delete.md, "Resume after the primary's loss", step 2), for the
    /// node-admin `b` of the leaving mesh `mesh` other than the lost final primary, which this view
    /// holds silent: its node.delete, unless this leave already holds its receipt, this fabric-primary
    /// the executor, `TerminateRuntime` first when it runs (node-recovery.md step 3). A runtime unknown
    /// here ends the attempt incomplete, named, and nothing is created for the mesh.
    async fn delete_silent_admin(&self, build_id: &crate::build::BuildId, attempt: u32, key: &MeshDeleteKey, mesh: &str, b: &RosterBirth, steps: &[BuildStepReceipt]) -> Result<(), RunEnd> {
        let outer = key.operation.as_str();
        if crate::mesh_delete::exit_receipts(steps, outer, std::slice::from_ref(b)).contains_key(&(b.node_id.clone(), b.incarnation_id.clone())) {
            return Ok(());
        }
        // An admin that answers is reached, not lost (Q111): silent in this view only for a moment (the
        // lost primary published the mesh's aggregate), it is the surviving admin the election names
        // (mesh-delete.md "Resume after the primary's loss" step 2), and the next attempt hands it the
        // workflow. Nothing is deleted or created for the mesh.
        let held = self.topology.read().await.cohort(mesh, NodeKind::NodeAdmin).find(|n| n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id)).cloned();
        if let Some(n) = held {
            if self.answers(&n).await {
                return Err(RunEnd::Incomplete(format!("{outer}: {mesh}'s node-admin {} {} answers, so it survives; the next attempt hands the workflow to the admin the election names", b.node, b.incarnation_id)));
            }
        }
        let refused = |reason: String| {
            tracing::info_span!("rdm.node_admin.mesh.reject.via-unknown-admin-runtime", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %outer, admin = %format!("{} {}", b.node, b.incarnation_id), reason = %reason)
                .in_scope(|| tracing::info!("a node-admin of the leaving mesh is silent and its runtime is not known here: no admin is created for the mesh"));
            RunEnd::Incomplete(format!("{outer}: {mesh} holds no live admin in this view, and the runtime of its silent node-admin {} {} is not known here ({reason}): no admin is created for the mesh", b.node, b.incarnation_id))
        };
        let store = self.records.contacts.get().ok_or_else(|| refused(format!("{} holds no durable runtime rows", self.held_name())))?;
        let row = self.roster_birth_row(&**store, b).await.map_err(|e| refused(e))?;
        let runtime = row.as_ref().map(|r| RuntimeProof::of(&r.runtime)).ok_or_else(|| refused(format!("no durable runtime row of {} {} was ever written here", b.node, b.incarnation_id)))?;
        crate::mesh_delete::predecessor_verdict(&*self.provider, row.as_ref(), b, &runtime).await.map_err(refused)?;
        self.delete_lost_birth(build_id, attempt, b).await.map(|_| ())
    }

    /// node.delete of the lost roster birth `b` as an unreached birth (mesh-delete.md "Resume after the
    /// primary's loss" step 1; node-recovery.md step 3), this fabric-primary its executor, under the
    /// leave's Build and attempt; the `TerminateRuntime` receipt under `node.delete:<path>` is the
    /// birth's roster receipt. Answers that receipt.
    async fn delete_lost_birth(&self, build_id: &crate::build::BuildId, attempt: u32, b: &RosterBirth) -> Result<TerminalReceipt, RunEnd> {
        let path: PathName = b.node.parse().map_err(|e| format!("the roster names a birth by `{}`, which is not a path.name: {e}", b.node))?;
        let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("the roster names {} by node id `{}`, which does not parse: {e}", b.node, b.node_id))?;
        let incarnation = b.birth().map_err(|e| format!("the roster names {} at birth `{}`, which does not parse: {e}", b.node, b.incarnation_id))?;
        self.delete_unreached(&*self.builds, build_id, attempt, &path, &crate::build::ExitedBirth { node_id, incarnation }).await
    }

    /// Steps 2-5 of the owner's workflow, for the leave `key` opened at admission: `mesh.deleting`,
    /// `DeleteMesh`, the validated handoff (mesh.deleted) and its completion receipt.
    #[allow(clippy::too_many_arguments)]
    async fn lead_delete(
        &self,
        build_id: &crate::build::BuildId,
        attempt: u32,
        mesh_id: &MeshId,
        outer: &str,
        node_rpc: &crate::node_rpc::ProcessNodeRpc,
        primary: &Node,
        key: &MeshDeleteKey,
        handoff: tokio::sync::watch::Receiver<Option<crate::mesh_delete::MeshDeleteOutcome>>,
    ) -> Result<(), RunEnd> {
        let req = StatusRequest::DeleteMesh { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string() };
        let (out, _) = node_rpc.client.call::<Status>(&rdm_transport::NodeTarget::ExactNode(primary.node_id.clone()), &req, &rdm_transport::CallOptions::default()).await;
        match &out {
            rdm_core::outcome::RpcOutcome::Reply(r) if matches!(r.value(), StatusReply::Applied | StatusReply::AlreadyApplied) => {}
            rdm_core::outcome::RpcOutcome::Reply(r) => return Err(RunEnd::failed(format!("{outer}: {} refused delete-mesh: {} ({:?})", primary.name, r.value().name(), r.value()))),
            // The call did not come back: the primary is unreached or the stream broke. A broken stream
            // proves neither a failed step nor a completed one (node-rpc-envelope.md, "Caller execution
            // and certainty"): the leave is incomplete and its receipts stand.
            other => return Err(RunEnd::Incomplete(format!("{outer}: delete-mesh to {} ended {} ({other:?}): the mesh may or may not have admitted it", primary.name, other.name()))),
        }
        // Steps 3-4 run at the mesh-primary; its validated handoff, mesh.deleted, is what this waits
        // for. The validation covered the accepted roster (and every amendment) exactly, every birth
        // but the final primary, which reports still running.
        // The wait ends on the handoff, or on the final primary unheard in this owner's view.
        let Some(admitted) = self.leaves.admitted(key) else {
            return Err(RunEnd::Incomplete(format!("{outer}: the open leave was closed before its handoff was awaited")));
        };
        let named = format!("{} {}", admitted.final_primary.node, admitted.final_primary.incarnation_id);
        let inspected = std::sync::Mutex::new(None);
        let manifest = crate::mesh_delete::await_mesh_deleted(key, &named, handoff, self.records.subscribe_view(), || self.final_primary_unheard(key, primary, &admitted, &inspected), crate::mesh_delete::MESH_DELETED_WAIT).await?;
        // Step 5: mesh.deleted ends the mesh.delete. The final primary is not deleted inside its own
        // mesh.delete: it keeps publishing its aggregate until a separate node.delete (step 7).
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
            None => Err(format!("{} holds no durable runtime rows", self.held_name())),
        };
        let verdict = match row {
            Ok(row) => match crate::mesh_delete::predecessor_verdict(&*self.provider, row.as_ref(), lost, &admitted.final_runtime).await {
                Ok(found) => crate::admin::verdict_name(&found).to_string(),
                Err(reason) => reason,
            },
            Err(reason) => reason,
        };
        tracing::info_span!("rdm.node_admin.mesh.resolve.via-final-primary-unheard", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, final_primary = %format!("{} {}", lost.node, lost.incarnation_id), status = ?status, verdict = %verdict)
            .in_scope(|| tracing::info!("the final primary is unheard: the owner's wait ends and the leave resumes after its loss"));
        Some(format!("the final primary is unheard in this fabric-primary's view ({status:?}); its runtime reads {verdict}"))
    }

    /// mesh.deleted's completion (mesh-delete.md step 5): the validated manifest `manifest` (every
    /// birth of the mesh but its final primary) is recorded as the `MeshDeleted` receipt and the open
    /// leave ends. Nothing of the final primary is stopped, written or published here.
    async fn record_mesh_deleted(&self, key: &MeshDeleteKey, manifest: ExitManifest) -> Result<(), String> {
        crate::mesh_delete::record_completion(&*self.builds, key, &self.held_name().to_string(), &manifest).await?;
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
    /// ([`crate::mesh_delete::mesh_left_proof`]) and recorded as `MeshLeft`, and mesh-left is published.
    /// Any other deleted birth names no completion and nothing happens. A refusal is spanned by name
    /// and leaves the node.delete as it is.
    pub(super) async fn mesh_left_after_delete(&self, op: &rdm_core::LifecycleOp) {
        let me = self.held_name();
        let found = match self.builds.mesh_deleted_of(&op.node_id, &op.incarnation).await {
            Ok(Some(found)) => found,
            Ok(None) => return,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.mesh.reject.via-mesh-left", node = %me, subject = %op.name, node_id = %op.node_id, reason = %format!("the mesh.delete completion index could not be read: {e}"))
                    .in_scope(|| tracing::info!("mesh-left is not decided: the index of mesh.delete completions could not be read"));
                return;
            }
        };
        let (build_id, attempt, operation) = found;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-final-birth-deleted", node = %me, subject = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %build_id.log(), attempt, operation = %operation, deleted_by = %op.operation, outcome = tracing::field::Empty, reason = tracing::field::Empty);
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

    async fn mesh_left_in(&self, op: &rdm_core::LifecycleOp, build_id: &crate::build::BuildId, attempt: u32, operation: &str) -> Result<&'static str, String> {
        let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{operation}: the Build {build_id} could not be read for its completion: {e}"))?.steps;
        if crate::mesh_delete::owner_resume(&steps, operation)? == crate::mesh_delete::OwnerResume::Left {
            return Ok("already-applied");
        }
        let completion = crate::mesh_delete::completion_of(&steps, operation).ok_or_else(|| format!("{operation}: no {STEP_MESH_DELETED} receipt is held in {build_id}"))??;
        // The final birth's exact Exited: its node.delete's TerminateRuntime receipt, in that Build.
        let deleted_in = crate::build::BuildId::parse(&op.build_id).map_err(|e| format!("{}: the node.delete names Build {:?}, which is not a Crockford id: {e}", op.operation, op.build_id))?;
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
            .find(|t| t.node_id == op.node_id.as_str() && t.incarnation_id == op.incarnation.as_str())
            .ok_or_else(|| format!("{}: no exact Exited receipt of {} {} is held in {deleted_in}", op.operation, op.name, op.incarnation.as_str()))?;
        let all = crate::mesh_delete::mesh_left_proof(&completion, &terminal).map_err(|e| format!("{operation}: the proof set is not complete: {e}"))?;
        let mesh_id = MeshId::parse(&completion.mesh_id).map_err(|e| format!("{operation}: the completion names the mesh id {:?}, which does not parse: {e}", completion.mesh_id))?;
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: operation.to_string() };
        self.record_mesh_left(&key, &op.name.mesh, all).await?;
        Ok("applied")
    }

    /// mesh-left: the complete proof set `all` (every birth, the final primary included) is recorded
    /// as the `MeshLeft` receipt after the mesh's departed contacts are dropped and its row Dead is
    /// ensured; then the mesh-left hook runs and the mesh's id is dropped.
    async fn record_mesh_left(&self, key: &MeshDeleteKey, mesh: &str, all: ExitManifest) -> Result<(), String> {
        let (build_id, attempt, outer, mesh_id) = (key.build_id.clone(), key.attempt, key.operation.as_str(), &key.mesh_id);
        let reference = manifest_reference(key.build_id.as_str(), attempt, outer, true);
        let me = self.held_name().to_string();
        // This fabric-primary is the acting primary of a mesh that has none left: the final birth's
        // node.delete wrote its row Deleted and proved its exit.
        crate::mesh_delete::mesh_left_rows(self.records.node_rows().as_deref(), &*self.status_storage, &*self.mesh_storage, &self.records.declared, &me, key, mesh, &all.receipts).await?;
        self.builds
            .append_step_receipt(&BuildStepReceipt {
                mesh: None,
                build_id: build_id.clone(),
                attempt,
                operation: outer.to_string(),
                step: STEP_MESH_LEFT.into(),
                outcome: StepOutcome::Complete,
                output: serde_json::to_value(&all).ok(),
                executor: Some(me),
            })
            .await
            .map_err(|e| format!("{outer}: the MeshLeft receipt could not be recorded: {e}"))?;
        tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-left-receipt", mesh_id = %mesh_id, build_id = %build_id.log(), attempt, operation = %outer, receipts = all.receipts.len(), roster = all.roster.len(), manifest = %reference)
            .in_scope(|| tracing::info!("every exact exit is proven: the MeshLeft receipt is recorded"));
        self.book.depart_mesh_births(mesh, build_id.as_str(), attempt, self.lifecycle_events.now_rafka_ms());
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

    /// `DeleteMesh` at the leaving mesh's primary: admit it and run the member workflows.
    pub(super) async fn admit_delete_mesh(self: Arc<Self>, peer: Option<iroh::PublicKey>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::DeleteMesh { mesh_id, build_id, attempt, operation } = req else { unreachable!("admit_delete_mesh serves delete-mesh") };
        let me_name = self.held_name();
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            return rdm_core::reply::status::not_ready(format!("{} holds no view of itself yet; delete-mesh for {mesh_id} is refused", me_name));
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
                tracing::info_span!("rdm.node_admin.mesh.reject.via-malformed-build-id", node = %me_name, mesh_id = %mesh_id, build_id = %build_id, reason = %e, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("delete-mesh refused: its Build id is not a Crockford id"));
                return rdm_core::reply::status::malformed(rdm_core::outcome::MalformedKind::Corrupt);
            }
        };
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build, attempt: *attempt, operation: operation.clone() };
        let me_str = me_name.to_string();
        let call = match peer {
            Some(key) => crate::workflows::node_start::reached_back(&me_str, &self.node_rpc.client, &key).await.ok().map(|target| crate::mesh_delete::MeshDeleteOwner { target, name: sender.map(|n| n.name.to_string()).unwrap_or_else(|| format!("the admin at endpoint {}", key.fmt_short())) }),
            None => None,
        };
        let fabric_primary = view.fabric_primary().map(|n| crate::mesh_delete::MeshDeleteOwner { target: rdm_transport::NodeTarget::ExactNode(n.node_id.clone()), name: n.name.to_string() });
        let owner = match crate::mesh_delete::leave_owner(call, fabric_primary, &me_str, &key) {
            Ok(owner) => owner,
            Err(refused) => return refused,
        };
        // The mesh row Leaving and the round's admission (the command hook's mesh-leaving is the
        // owner's): [`crate::mesh_delete::admit_delete_mesh`].
        let crate::mesh_delete::DeleteMeshAdmission { leaving_row, admission } = crate::mesh_delete::admit_delete_mesh(&self.mesh_delete_runs, &*self.status_storage, &self.records.declared, &key, &owner).await;
        if let Some(committed) = &leaving_row {
            tracing::info_span!("rdm.node_admin.mesh.update.via-leaving-row", node = %me_name, mesh = %me.mesh, mesh_id = %mesh_id, build_id = %build_id, attempt = *attempt, operation = %operation, owner = %owner, outcome = committed.name())
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
            crate::mesh_delete::MeshDeleteAdmission::Resend { manifest, recorded } => Some((manifest, recorded)),
            crate::mesh_delete::MeshDeleteAdmission::Start => None,
            crate::mesh_delete::MeshDeleteAdmission::Refused(reply) => return reply,
        };
        // The command's serve span carries the caller's propagated trace; the workflow it starts
        // continues it, so every member's calls join the outer trace.
        let parent = tracing::Span::current();
        let runner = self.clone();
        match resend {
            // The members all exited and the manifest is recorded: the handoff is what repeats.
            Some((manifest, recorded)) => {
                tokio::spawn(async move { runner.hand_off(&key, &owner, &manifest, recorded).instrument(parent).await });
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
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.mesh.update.via-leave-members", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, outcome = tracing::field::Empty, handoff_attempt = tracing::field::Empty);
        async {
            let end = crate::mesh_delete::run_admitted(&self.mesh_delete_runs, &key.mesh_id, |key, owner| {
                let runner = self.clone();
                async move { runner.leave_members(&key, &owner).await }
            })
            .await;
            match end {
                crate::mesh_delete::MeshDeleteRunEnd::Exited { key, owner, manifest } => {
                    tracing::Span::current().record("outcome", "exited");
                    tracing::Span::current().record("handoff_attempt", key.attempt);
                    // The handoff goes to the owner that sent delete-mesh last, read now.
                    self.hand_off(&key, &owner, &manifest, key.attempt).await;
                }
                crate::mesh_delete::MeshDeleteRunEnd::Incomplete { key, owner, reason } => {
                    tracing::Span::current().record("outcome", "incomplete");
                    tracing::Span::current().record("handoff_attempt", key.attempt);
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
    /// the Build facts it holds are read when this admin's own do not yet admit it.
    async fn leave_members(&self, key: &MeshDeleteKey, owner: &crate::mesh_delete::MeshDeleteOwner) -> Result<ExitManifest, String> {
        let me_name = self.held_name().to_string();
        let view = self.topology.read().await.clone();
        let me = view.own(&self.node_id).cloned().ok_or_else(|| format!("{me_name} holds no view of itself"))?;
        let my_fact = self.held_runtime(&me).ok_or_else(|| format!("{} publishes no runtime fact: its own exact runtime cannot be named", me.name))?;
        let me_birth = RosterBirth::of(&me)?;
        let my_runtime = RuntimeProof::of(&my_fact);
        // The roster captured once, or, when this admin holds none, what the owner admitted: the
        // roster amendments the fabric-primary recorded (a primary lost mid-leave) are read from the
        // owner's Build facts when this admin has not yet heard them ([`crate::mesh_delete::leaving_roster`]).
        let build_id = key.build_id.clone();
        let me_name_ref = &me_name;
        let fetch_owner = || async {
            crate::build_facts_read::fetch_build_facts(&self.node_rpc.client, &owner.target, &build_id).await.map(|f| f.facts).map_err(|e| format!("{me_name_ref}: the owner {owner}'s Build facts of {build_id} could not be read for the leave's roster: {e}"))
        };
        let capture = || {
            let mut roster = view.members().filter(|n| n.mesh == me.mesh && n.status.is_live() && n.node_id != me.node_id).map(RosterBirth::of).collect::<Result<Vec<_>, _>>()?;
            roster.push(me_birth.clone());
            Ok(roster)
        };
        let (roster, known) = crate::mesh_delete::leaving_roster(&*self.builds, &*self.local, key, &me_name, &me_birth, &my_runtime, capture, fetch_owner).await?;
        let mesh: Vec<Node> = view.nodes.iter().filter(|n| n.mesh == me.mesh).cloned().collect();
        let records = crate::mesh_delete::roster_records(&mesh, &roster, |b| self.held_record(b));
        crate::mesh_delete::roster_rows_read(self.records.node_rows().as_deref(), &me_name, &key.operation, &roster, &records).await;
        members_exited(&*self.builds, key, &me_name, &roster, &me_birth, my_runtime, &records, &known, |n| async move { self.shutdown_one(key, &n).await }).await
    }

    /// One member's end (mesh-delete.md step 3: "The mesh-primary runs the whole node.delete for
    /// every ordinary node"): the ordinary node.delete workflow ([`AdminRunner::delete_birth`]) of the
    /// exact birth, under the mesh.delete's Build and attempt. The provider's terminal receipt is the
    /// member's roster receipt.
    async fn shutdown_one(&self, key: &MeshDeleteKey, n: &Node) -> Result<TerminalReceipt, String> {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-shutdown-member", node = %n.name, node_id = %n.node_id, operation = %key.operation, build_id = %key.build_id.log(), attempt = key.attempt, outcome = tracing::field::Empty);
        let result = async {
            let (record, handle) = self.handle_for(n).await?;
            // Whether the birth is stopped is the record's the leave works from (a stopped member's
            // delete goes straight to its exit proof), as `retire` takes it from the view.
            let record = Node { parked: n.parked, ..record };
            self.delete_birth(&key.build_id.clone(), key.attempt, record, handle).await.map_err(|e| format!("{}'s node.delete: {e}", n.name))
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", if result.is_ok() { "exited" } else { "incomplete" });
        result
    }

    /// The `MeshDeleted` handoff: the mesh-leave gossip hook, then the call up to the owner.
    async fn hand_off(&self, key: &MeshDeleteKey, owner: &crate::mesh_delete::MeshDeleteOwner, manifest: &ExitManifest, _recorded: u32) {
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            tracing::info!(operation = %key.operation, "this admin holds no view of itself: the handoff waits for a repeated delete-mesh");
            return;
        };
        if self.held_runtime(&me).is_none() {
            tracing::info!(node = %me.name, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        }
        // mesh.deleted (ops-naming mesh.delete: "reported by the mesh-primary, still alive, from its
        // members' exit proofs; mesh is dead, id kept retired"): every other member's exit is proven,
        // so this primary commits its mesh row Dead (states.md: "one commit stores the row, applies
        // it, gossips it and answers"); its view then states the mesh Dead and its status round
        // publishes MeshStatus Dead, five sends, while it lives (R-NS1 item 1). A refused put
        // answers no handoff: the owner's repeated delete-mesh decides it afresh.
        // Its Mesh row is written Dead on mesh.storage with it, which releases the mesh's name (R-MN1
        // item 2); a refused write answers no handoff either.
        let committed = crate::status_rpc::commit_mesh_state(&*self.status_storage, &self.records.declared, &key.mesh_id, rdm_core::status::MeshState::Dead).await;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-dead-row", node = %self.held_name(), mesh = %me.mesh, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, receipts = manifest.receipts.len(), outcome = committed.name(), mesh_row = tracing::field::Empty, error = tracing::field::Empty);
        let mesh_row = match committed {
            StatusReply::Applied | StatusReply::AlreadyApplied => crate::mesh_delete::mesh_row_dead(&*self.mesh_storage, &me.mesh, &key.mesh_id).await,
            refused => Err(format!("the Dead status row was not committed: {}", refused.name())),
        };
        match &mesh_row {
            Ok(row) => span.record("mesh_row", *row),
            Err(e) => span.record("mesh_row", "not-written").record("error", e.as_str()),
        };
        span.in_scope(|| tracing::info!("mesh.deleted at the mesh's primary: its own mesh row Dead is stored and applied from its members' exit proofs, and its name released"));
        if let Err(e) = mesh_row {
            tracing::info!(operation = %key.operation, error = %e, "the mesh row Dead was not committed: the handoff waits for a repeated delete-mesh");
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
        let reference = manifest_reference(key.build_id.as_str(), key.attempt, &key.operation, false);
        let view = self.topology.read().await.clone();
        let node_rpc = &self.node_rpc;
        let Some(me) = view.own(&self.node_id).cloned() else {
            tracing::info!(operation = %key.operation, owner = %owner, "this admin holds no view of itself: the handoff waits for a repeated delete-mesh");
            return;
        };
        let Some(final_runtime) = self.held_runtime(&me) else {
            tracing::info!(node = %me.name, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        };
        let (Some(final_incarnation), final_node_id) = (me.incarnation_id.clone(), me.node_id.clone()) else { return };
        let req = StatusRequest::MeshDeleted { mesh_id: key.mesh_id.clone(), build_id: key.build_id.to_string(), attempt: key.attempt, operation: key.operation.clone(), final_node_id, final_incarnation, final_runtime, receipt_manifest: reference };
        let (out, _) = node_rpc.client.call::<Status>(&owner.target, &req, &rdm_transport::CallOptions::default()).await;
        let outcome = match &out {
            rdm_core::outcome::RpcOutcome::Reply(r) => format!("{} {:?}", r.value().name(), r.value()),
            other => format!("{} {other:?}", other.name()),
        };
        tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-sent", mesh_id = %key.mesh_id, operation = %key.operation, owner = %owner, manifest, manifest_receipts, outcome = %outcome)
            .in_scope(|| tracing::info!("the mesh-deleted handoff was called"));
    }

    /// `MeshDeleted` at the owner: read the manifest from the sender, validate it against the roster
    /// captured at admission and the final primary's provider access, and release the outer
    /// workflow. A reference alone is not proof.
    pub(super) async fn validate_handoff(&self, _sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::MeshDeleted { mesh_id, build_id, attempt, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest } = req else { unreachable!("validate_handoff serves mesh-deleted") };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-validated", mesh_id = %mesh_id, build_id = %build_id, attempt = *attempt, operation = %operation, outcome = tracing::field::Empty, field = tracing::field::Empty, without_exit = tracing::field::Empty);
        let reply = self.validate_handoff_in(mesh_id, build_id, *attempt, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        if let StatusReply::RejectedUnmatchedCompletion { field, .. } = &reply {
            span.record("field", field.as_str());
        }
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn validate_handoff_in(&self, mesh_id: &MeshId, build_id: &str, attempt: u32, operation: &str, final_node_id: &NodeId, final_incarnation: &IncarnationId, final_runtime: &rdm_core::RuntimeFact, receipt_manifest: &str) -> StatusReply {
        let me_name = self.held_name().to_string();
        let unmatched = |field: &str, expected: String, reported: String| rdm_core::reply::status::rejected_unmatched_completion(field.into(), expected, reported);
        let build = match crate::build::BuildId::parse(build_id) {
            Ok(build) => build,
            Err(e) => return unmatched("build_id", format!("a Crockford Build id ({e})"), build_id.to_string()),
        };
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build.clone(), attempt, operation: operation.to_string() };
        let admitted = match self.leaves.admitted_for(&key) {
            Ok(admitted) => admitted,
            Err(refused) => {
                // No open leave: after mesh.deleted's completion receipt the handoff is a replay,
                // answered from the receipt (AlreadyApplied, no second receipt).
                match self.builds.read_build(&build).await {
                    Ok(b) => {
                        if let Some(reply) = crate::mesh_delete::replayed_handoff(&b.steps, &key, final_node_id, &final_incarnation.as_str()) {
                            return reply;
                        }
                    }
                    // A Build this admin holds no fact of holds no completion of it either.
                    Err(crate::build_state::BuildStateError::UnknownBuild(_)) => {}
                    Err(e) => return crate::mesh_delete::refuse_source_failed("mesh-deleted", &key, &format!("the Build log of {me_name}"), &format!("read {build} for the completion of {operation}"), &e.to_string()),
                }
                return refused;
            }
        };
        if receipt_manifest.len() > rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES {
            return unmatched("receipt_manifest", format!("at most {} bytes", rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES), format!("{} bytes", receipt_manifest.len()));
        }
        if final_node_id.to_string() != admitted.final_primary.node_id || final_incarnation.as_str() != admitted.final_primary.incarnation_id {
            return unmatched("final_primary", format!("{} {}", admitted.final_primary.node_id, admitted.final_primary.incarnation_id), format!("{final_node_id} {}", final_incarnation.as_str()));
        }
        // The final primary's exact runtime, and this admin's provider access to it.
        if RuntimeProof::of(final_runtime) != admitted.final_runtime {
            return unmatched("final_runtime", format!("{:?}", admitted.final_runtime), format!("{:?}", RuntimeProof::of(final_runtime)));
        }
        if let Err(refused) = crate::mesh_delete::adopt_final_runtime(&*self.provider, final_runtime, &me_name, &key) {
            return refused;
        }
        // The manifest: the sender's Build facts, read now.
        let node_rpc = &self.node_rpc;
        let read = crate::build_facts_read::fetch_build_facts(&node_rpc.client, &rdm_transport::NodeTarget::ExactNode(final_node_id.clone()), &build).await.map(|f| f.facts);
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
