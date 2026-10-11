//! The two sides of the mesh-leave workflow a runner executes (mesh-leave.md).
//!
//! The owner's side ([`AdminRunner::shutdown_mesh`]) is the fabric-primary: it owns the outer
//! workflow `shutdown-mesh:<mesh_id>` once the fabric seat is outside the leaving mesh (the
//! confirmed fabric-primary handover, `fabric_handover`). It captures the accepted roster and the
//! final primary's provider access (refusing before anything is dismantled when it has none),
//! publishes `mesh-leaving`, sends `LeaveMesh`, waits for the validated `MeshLeave` handoff, drains
//! and stops the final primary itself, and only with every exact exit proven records `MeshLeft`.
//! The leaving mesh's primary's side ([`AdminRunner::admit_leave`]) admits `LeaveMesh`, runs the
//! member workflows (every ordinary node in parallel, then every other node-admin in parallel),
//! records one exit manifest as a step receipt of the accepted Build and calls `MeshLeave` up to
//! the owner, still running.

use super::*;
use crate::build_state::{BuildStepReceipt, StepOutcome};
use crate::deployment::pipeline::{MeshLeaveEvent, RuntimeProof, TerminalReceipt};
use crate::mesh_leave::{accepted_roster, first_mismatch, manifest_reference, members_exited, Admitted, ExitManifest, LeaveKey, MeshLeaver, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_MESH_LEFT};
use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
use tracing::Instrument;

/// How long the owner waits for the leaving mesh's `MeshLeave` handoff before its attempt ends
/// incomplete. A leaving primary whose member workflows end incomplete says so inside a
/// `MeshLeave` call, which ends the attempt at once; this bound covers a handoff that never
/// arrives (a lost call, a lost primary). A failed attempt ends the Build; the fabric-primary's
/// drift check (`crate::admin::reconcile_drift`) opens the next attempt while the removed mesh is
/// still live with no `MeshLeft` receipt, and that attempt resumes from the receipts.
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
}

impl AdminRunner {
    /// The runtime fact the book holds for `node` (its digest).
    fn held_runtime(&self, node: &Node) -> Option<rafka_mesh_entity::RuntimeFact> {
        self.book.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref()).and_then(|d| d.node.runtime)
    }

    /// The owner's outer workflow `shutdown-mesh:<mesh_id>`.
    pub(super) async fn shutdown_mesh(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, mesh_id: &MeshId) -> Result<(), String> {
        let outer = crate::mesh_leave::operation(mesh_id);
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-shutdown", mesh = %mesh, mesh_id = %mesh_id, build_id = %build_id, attempt, operation = %outer, outcome = tracing::field::Empty);
        let result = self.shutdown_mesh_in(build_id, attempt, mesh, mesh_id, &outer).instrument(span.clone()).await;
        span.record("outcome", if result.is_ok() { "complete" } else { "incomplete" });
        result
    }

    async fn shutdown_mesh_in(&self, build_id: &crate::build::BuildId, attempt: u32, mesh: &str, mesh_id: &MeshId, outer: &str) -> Result<(), String> {
        // Where this owner resumes, from its own Build facts, before it calls anything.
        let steps = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its receipts: {e}"))?.steps;
        let view = self.topology.read().await.clone();
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string() };
        match crate::mesh_leave::owner_resume(&steps, outer)? {
            crate::mesh_leave::OwnerResume::Left => {
                // Resumed after the departure was recorded: the receipt is the record.
                self.forget_mesh_id(mesh);
                return Ok(());
            }
            crate::mesh_leave::OwnerResume::FinalExited(all) => {
                // Resumed after the final primary's exact exit was recorded: nothing is called (it
                // is gone); the proof set from the receipts is what MeshLeft records.
                let last = &all.final_primary;
                if let Some(node) = view.nodes.iter().find(|n| n.node_id.to_string() == last.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.0 == last.incarnation_id)).cloned() {
                    self.after_mesh_exit(&node, "mesh-leave");
                }
                let event = MeshLeaveEvent { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string(), receipt_manifest: None, final_primary: None };
                return self.record_mesh_left(&key, mesh, all, event).await;
            }
            crate::mesh_leave::OwnerResume::Lead => {}
        }
        let node_rpc = self.node_rpc.as_ref().ok_or_else(|| format!("{}: this admin holds no Node RPC client: {outer} cannot call the mesh", self.me))?;
        // Step 1: this owner is the fabric-primary. When the leaving mesh held the fabric seat it
        // was handed over first (fabric-primary-handover.md): the leave opens only on the confirmed
        // transfer, and an unconfirmed one is reconciled here before anything is dismantled.
        self.handover.confirmed(mesh_id).await.map_err(|e| format!("{outer} is refused before {mesh} is dismantled: the fabric-primary handover is not confirmed: {e}"))?;
        let primary = view.cohort_primary(mesh, NodeKind::NodeAdmin).cloned().ok_or_else(|| format!("{outer} is refused: {mesh} holds no node-admin primary to hand the workflow to"))?;
        // A resumed attempt hands the workflow only to the final primary accepted at admission.
        let recorded = crate::mesh_leave::accepted_row(&steps, outer, STEP_ACCEPTED_ROSTER).transpose().map_err(|e| format!("{outer}: {e}"))?;
        if let Some(row) = &recorded {
            let held = RosterBirth::of(&primary)?;
            if held != row.final_primary {
                return Err(format!(
                    "{outer} is refused: {mesh}'s primary is {} {}, not the final primary {} {} accepted at admission; a resumed leave hands its workflow to no other birth",
                    held.node, held.incarnation_id, row.final_primary.node, row.final_primary.incarnation_id
                ));
            }
        }
        // The final primary's authoritative provider domain: access before anything is dismantled.
        let (final_record, final_handle) = self.handle_for(&primary).await.map_err(|e| format!("{outer} is refused before {mesh} is dismantled: no provider proof path for its final primary: {e}"))?;
        let final_fact = final_handle.fact().ok_or_else(|| format!("{outer} is refused before {mesh} is dismantled: the final primary {} has no exact runtime handle", primary.name))?;
        let (final_primary, final_runtime) = (RosterBirth::of(&primary)?, RuntimeProof::of(&final_fact));
        if let Some(accepted) = recorded.as_ref().and_then(|row| row.final_runtime.as_ref()) {
            if accepted != &final_runtime {
                return Err(format!("{outer} is refused before {mesh} is dismantled further: the final primary {}'s runtime is {final_runtime:?}, not the one accepted at admission {accepted:?}", primary.name));
            }
        }
        let roster = accepted_roster(&*self.builds, &key, STEP_ACCEPTED_ROSTER, &self.me.to_string(), &final_primary, &final_runtime, || view.members().filter(|n| n.mesh == mesh && n.status.is_live()).map(RosterBirth::of).collect()).await?;
        let admitted = Admitted { roster, final_primary, final_runtime };
        if !admitted.roster.contains(&admitted.final_primary) {
            return Err(format!("{outer} is refused before {mesh} is dismantled: its primary {} {} is not a birth of the roster accepted at admission", admitted.final_primary.node, admitted.final_primary.incarnation_id));
        }
        let handoff = self.leaves.open(key.clone(), admitted.clone());
        let result = self.lead_leave(build_id, attempt, mesh, mesh_id, outer, node_rpc, &primary, final_record, final_handle, &key, &admitted, handoff).await;
        if result.is_err() {
            // This attempt's open leave ends with it; the receipts are the record a resumed attempt reads.
            self.leaves.close(&key);
        }
        result
    }

    /// Steps 2-7 of the owner's workflow, for the leave `key` opened with `admitted`.
    #[allow(clippy::too_many_arguments)]
    async fn lead_leave(
        &self,
        build_id: &crate::build::BuildId,
        attempt: u32,
        mesh: &str,
        mesh_id: &MeshId,
        outer: &str,
        node_rpc: &crate::node_rpc::ProcessNodeRpc,
        primary: &Node,
        final_record: Node,
        final_handle: crate::deployment::provider::DeploymentHandle,
        key: &LeaveKey,
        admitted: &Admitted,
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
        // Steps 3-4 run at the mesh-primary; its validated handoff is what this waits for.
        let manifest = match tokio::time::timeout(HANDOFF_WAIT, handoff.wait_for(|m| m.is_some())).await {
            Ok(Ok(m)) => match m.clone().expect("waited for Some") {
                Ok(manifest) => manifest,
                Err(reason) => return Err(format!("{outer}: {} reported its member workflows incomplete: {reason}", primary.name)),
            },
            Ok(Err(_)) => return Err(format!("{outer}: the open leave was closed while its handoff was awaited")),
            Err(_) => return Err(format!("{outer}: {} sent no valid mesh-leave within {HANDOFF_WAIT:?}; the workflow is incomplete and its receipts stand", primary.name)),
        };
        // Step 5: the final primary drains and stops under the owner's authority.
        let template = self.template_for(NodeKind::NodeAdmin, mesh).await?;
        let mut node = final_record.clone();
        let terminal = self.pipeline(&template).shutdown_member(build_id, attempt, outer, &mut node, &final_handle).await.map_err(|e| format!("{outer}: the final primary {}: {e}", primary.name))?;
        self.after_mesh_exit(&node, "mesh-leave");
        // Steps 6-7: the complete proof set, or nothing.
        let mut all = manifest;
        all.final_runtime = None;
        all.receipts.push(terminal);
        if let Some((field, expected, reported)) = first_mismatch(&all, &admitted.roster, &admitted.final_primary, true) {
            return Err(format!("{outer}: the proof set is not complete: {field}: expected {expected}, reported {reported}"));
        }
        self.record_mesh_left(key, mesh, all, event).await
    }

    /// Step 7: the complete proof set `all` is recorded as the `MeshLeft` receipt, then the
    /// mesh-left hook runs and the mesh's id is dropped.
    async fn record_mesh_left(&self, key: &LeaveKey, mesh: &str, all: ExitManifest, event: MeshLeaveEvent) -> Result<(), String> {
        let (build_id, attempt, outer, mesh_id) = (crate::build::BuildId(key.build_id.clone()), key.attempt, key.operation.as_str(), &key.mesh_id);
        let reference = manifest_reference(&key.build_id, attempt, outer, true);
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
        self.leaves.close(key);
        self.lifecycle_events.mesh_left(&MeshLeaveEvent { receipt_manifest: Some(reference), ..event }).await;
        self.forget_mesh_id(mesh);
        self.refresh_view().await;
        Ok(())
    }

    /// A mesh that has left keeps no id here: the MeshLeft receipt is the end of that mesh, and a
    /// mesh of the same name created after it is a new mesh with a new id. The id is dropped at the
    /// receipt, never left for a later sweep to find the mesh without a member.
    fn forget_mesh_id(&self, mesh: &str) {
        self.records.meshes.lock().unwrap().remove(mesh);
    }

    /// `LeaveMesh` at the leaving mesh's primary: admit it and run the member workflows.
    pub(super) async fn admit_leave(self: Arc<Self>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::LeaveMesh { mesh_id, build_id, attempt, operation } = req else { unreachable!("admit_leave serves leave-mesh") };
        let view = self.topology.read().await.clone();
        let Some(me) = view.nodes.iter().find(|n| n.name == self.me).cloned() else {
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
        // The handoff goes back to whoever owns the workflow: the admin that sent this command.
        let Some(owner) = sender.map(|n| n.node_id.clone()) else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "unknown peer".into() } };
        };
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() };
        let resend = match self.leave_runs.admit(&key, &owner) {
            crate::mesh_leave::LeaveAdmission::Running => return StatusReply::AlreadyApplied,
            crate::mesh_leave::LeaveAdmission::Resend(manifest) => Some(manifest),
            crate::mesh_leave::LeaveAdmission::Start => None,
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

    async fn run_leave(self: Arc<Self>, key: LeaveKey, parent: tracing::Span) {
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.mesh.update.via-leave-members", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, outcome = tracing::field::Empty);
        async {
            match self.leave_members(&key).await {
                Ok(manifest) => {
                    tracing::Span::current().record("outcome", "exited");
                    // The handoff goes to the owner that sent leave-mesh last, read now.
                    match self.leave_runs.exited(&key, &manifest) {
                        Some(owner) => self.hand_off(&key, &owner, &manifest).await,
                        None => tracing::info!("this leave's run is no longer held: the handoff waits for a repeated leave-mesh"),
                    }
                }
                Err(e) => {
                    tracing::Span::current().record("outcome", "incomplete");
                    // The run ends; its receipts stand. Every member that exited holds its exact
                    // Exited step receipt, and the roster its LeavingRoster receipt: a repeated
                    // leave-mesh, or the Build's next attempt, runs only the members without one.
                    tracing::info!(error = %e, "the member workflows are incomplete: the owner is told now");
                    // The owner learns it inside the call: mesh-leave with no recorded manifest,
                    // which it refuses not-ready naming the births without an Exited receipt and
                    // ends its open leave with. No mesh-leave gossip runs: this is no handoff.
                    if let Some(owner) = self.leave_runs.incomplete(&key) {
                        self.call_mesh_leave(&key, &owner, "incomplete", 0).await;
                    }
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Both tiers over the accepted roster, then the one manifest receipt
    /// ([`crate::mesh_leave::members_exited`]).
    async fn leave_members(&self, key: &LeaveKey) -> Result<ExitManifest, String> {
        let view = self.topology.read().await.clone();
        let me = view.nodes.iter().find(|n| n.name == self.me).cloned().ok_or_else(|| format!("{} holds no view of itself", self.me))?;
        let my_fact = self.held_runtime(&me).ok_or_else(|| format!("{} publishes no runtime fact: its own exact runtime cannot be named", me.name))?;
        let me_birth = RosterBirth::of(&me)?;
        let my_runtime = RuntimeProof::of(&my_fact);
        let roster = accepted_roster(&*self.builds, key, STEP_LEAVING_ROSTER, &self.me.to_string(), &me_birth, &my_runtime, || {
            let mut roster = view.members().filter(|n| n.mesh == me.mesh && n.status.is_live() && n.name != me.name).map(RosterBirth::of).collect::<Result<Vec<_>, _>>()?;
            roster.push(me_birth.clone());
            Ok(roster)
        })
        .await?;
        let records: Vec<Node> = view.nodes.iter().filter(|n| n.mesh == me.mesh).cloned().collect();
        members_exited(&*self.builds, key, &self.me.to_string(), &roster, &me_birth, my_runtime, &records, |n| async move { self.shutdown_one(key, &n).await }).await
    }

    /// One member's drain and stop and the provider's terminal receipt of its exact runtime.
    async fn shutdown_one(&self, key: &LeaveKey, n: &Node) -> Result<TerminalReceipt, String> {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-shutdown-member", node = %n.name, node_id = %n.node_id, operation = %key.operation, build_id = %key.build_id, attempt = key.attempt, outcome = tracing::field::Empty);
        let result = async {
            let (mut record, handle) = self.handle_for(n).await?;
            let template = self.template_for(n.kind, &n.mesh).await?;
            let terminal = self.pipeline(&template).shutdown_member(&crate::build::BuildId(key.build_id.clone()), key.attempt, &key.operation, &mut record, &handle).await.map_err(|e| e.to_string())?;
            self.after_mesh_exit(&record, "mesh-leave");
            self.handles.lock().unwrap().remove(&n.name);
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
        let Some(me) = view.nodes.iter().find(|n| n.name == self.me).cloned() else {
            tracing::info!(operation = %key.operation, "this admin holds no view of itself: the handoff waits for a repeated leave-mesh");
            return;
        };
        if self.held_runtime(&me).is_none() {
            tracing::info!(node = %me.name, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        }
        let (Some(final_incarnation), final_node_id) = (me.incarnation_id.clone(), me.node_id.clone()) else { return };
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
        let (Some(me), Some(owner_node), Some(node_rpc)) = (view.nodes.iter().find(|n| n.name == self.me).cloned(), view.nodes.iter().find(|n| &n.node_id == owner).cloned(), self.node_rpc.as_ref()) else {
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
        let Some(admitted) = self.leaves.admitted(&key) else {
            let open = self.leaves.open_for(mesh_id);
            return unmatched("operation", if open.is_empty() { "an open shutdown of this mesh".into() } else { open.iter().map(|(b, a, o)| format!("{o} (build {b}, attempt {a})")).collect::<Vec<_>>().join(", ") }, format!("{operation} (build {build_id}, attempt {attempt})"));
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
        if let Err(refusal) = crate::deployment::provider::adopt(&*self.provider, final_runtime) {
            return StatusReply::NotReady { reason: format!("{}: no provider proof path to the final primary's runtime {}: {refusal}", self.me, final_runtime.deployment_id) };
        }
        // The manifest: the sender's Build facts, read now.
        let Some(node_rpc) = self.node_rpc.as_ref() else {
            return StatusReply::NotReady { reason: format!("{} holds no Node RPC client to read the manifest with", self.me) };
        };
        let facts = match crate::build_facts_read::fetch_build_facts(&node_rpc.client, &rafka_node_rpc::NodeTarget::ExactNode(final_node_id.clone()), &crate::build::BuildId(build_id.to_string())).await {
            Ok(f) => f.facts,
            Err(e) => return StatusReply::NotReady { reason: format!("{}: the manifest {receipt_manifest} could not be read from {final_node_id}: {e}", self.me) },
        };
        let manifest = match crate::mesh_leave::handoff_verdict(&facts, &key, &admitted, receipt_manifest) {
            crate::mesh_leave::HandoffVerdict::Accept(m) => m,
            crate::mesh_leave::HandoffVerdict::Incomplete { reason, without_exit } => {
                // The leaving primary recorded no manifest: the open leave ends now with the
                // births named, and this attempt ends inside the call.
                tracing::Span::current().record("without_exit", without_exit.join(", ").as_str());
                self.leaves.incomplete(&key, reason.clone());
                return StatusReply::NotReady { reason };
            }
            refused => return refused.refusal().expect("a verdict other than Accept refuses"),
        };
        if self.leaves.resolve(&key, manifest) {
            StatusReply::Applied
        } else {
            StatusReply::AlreadyApplied
        }
    }

    /// The exact birth's exit is proven: the resolver names no current birth for it and its direct
    /// paths are retired, so nothing aims at its old socket.
    pub(super) fn after_mesh_exit(&self, record: &Node, kind: &'static str) {
        if let (Some(rpc), Some(incarnation)) = (&self.node_rpc, record.incarnation_id.as_ref()) {
            rpc.resolver.retire_birth(&record.node_id, incarnation);
        }
        if let (Some(ep), Some(key)) = (&self.endpoint, record.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok())) {
            let ep = ep.clone();
            tokio::spawn(async move { ep.replace_direct_addrs(key, []).await });
            tracing::info_span!("rdm.node_admin.node.update.via-exit-paths-retired", node = %record.name, node_id = %record.node_id, endpoint = %key.fmt_short(), kind, reason = "proven-exit")
                .in_scope(|| tracing::info!("the exited birth's direct paths are retired: nothing aims at its old socket"));
        }
    }
}
