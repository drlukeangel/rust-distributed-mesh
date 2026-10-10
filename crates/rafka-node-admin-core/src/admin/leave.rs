//! The two sides of the mesh-leave workflow a runner executes (mesh-leave.md).
//!
//! The owner's side ([`AdminRunner::shutdown_mesh`]) owns the outer workflow
//! `shutdown-mesh:<mesh_id>` from outside the leaving mesh: it captures the accepted roster and the
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
use crate::mesh_leave::{first_mismatch, manifest_reference, Admitted, ExitManifest, LeaveKey, MeshLeaver, RosterBirth, STEP_MESH_LEFT, STEP_OTHER_MEMBERS_EXITED};
use futures_util::future::join_all;
use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
use tracing::Instrument;

/// How long the owner waits for the leaving mesh's `MeshLeave` handoff before its attempt ends
/// incomplete (the Build's next attempt resumes from the receipts).
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
        // Resumed after the departure was recorded: the receipt is the record.
        if self.builds.read_build(build_id).await.is_ok_and(|b| b.steps.iter().any(|r| r.operation == outer && r.step == STEP_MESH_LEFT && r.outcome == StepOutcome::Complete)) {
            return Ok(());
        }
        let view = self.topology.read().await.clone();
        let node_rpc = self.node_rpc.as_ref().ok_or_else(|| format!("{}: this admin holds no Node RPC client: {outer} cannot call the mesh", self.me))?;
        // Step 1: this owner is outside the leaving mesh (`executor_for`). The fabric-primary seat
        // may be held inside it: the seat leaves a mesh only when every node-admin of that mesh is
        // gone (election.rs), so the workflow is owned from outside and the seat moves when the
        // mesh's admins have exited.
        if view.nodes.iter().any(|n| n.name == self.me && n.mesh == mesh) {
            return Err(format!("{outer} is refused: {} sits inside {mesh}, so no owner outside it holds this workflow", self.me));
        }
        if let Some(fp) = view.fabric_primary().filter(|fp| fp.mesh == mesh) {
            tracing::info_span!("rdm.node_admin.mesh.update.via-seat-inside-leaving-mesh", mesh = %mesh, seat_holder = %fp.name, owner = %self.me, operation = %outer)
                .in_scope(|| tracing::info!("the fabric-primary seat is held inside the leaving mesh: the workflow is owned from outside it and the seat moves when every admin of the mesh has exited"));
        }
        let members: Vec<Node> = view.members().filter(|n| n.mesh == mesh && n.status.is_live()).cloned().collect();
        let primary = view.cohort_primary(mesh, NodeKind::NodeAdmin).cloned().ok_or_else(|| format!("{outer} is refused: {mesh} holds no node-admin primary to hand the workflow to"))?;
        // The final primary's authoritative provider domain: access before anything is dismantled.
        let (final_record, final_handle) = self.handle_for(&primary).await.map_err(|e| format!("{outer} is refused before {mesh} is dismantled: no provider proof path for its final primary: {e}"))?;
        let final_fact = final_handle.fact().ok_or_else(|| format!("{outer} is refused before {mesh} is dismantled: the final primary {} has no exact runtime handle", primary.name))?;
        let roster = members.iter().map(RosterBirth::of).collect::<Result<Vec<_>, _>>()?;
        let admitted = Admitted { roster: roster.clone(), final_primary: RosterBirth::of(&primary)?, final_runtime: RuntimeProof::of(&final_fact) };
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.to_string(), attempt, operation: outer.to_string() };
        let mut handoff = self.leaves.open(key.clone(), admitted.clone());
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
            Ok(Ok(m)) => m.clone().expect("waited for Some"),
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
        self.leaves.close(&key);
        self.lifecycle_events.mesh_left(&MeshLeaveEvent { receipt_manifest: Some(reference), ..event }).await;
        self.refresh_view().await;
        Ok(())
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
        let resend = {
            let mut runs = self.leave_runs.lock().unwrap();
            match runs.get(&key).cloned() {
                Some(LeaveRun::Running(_)) => return StatusReply::AlreadyApplied,
                Some(LeaveRun::Exited(_, manifest)) => Some(manifest),
                None => {
                    runs.insert(key.clone(), LeaveRun::Running(owner.clone()));
                    None
                }
            }
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
                tokio::spawn(async move { runner.run_leave(key, owner, parent).await });
                StatusReply::Applied
            }
        }
    }

    async fn run_leave(self: Arc<Self>, key: LeaveKey, owner: NodeId, parent: tracing::Span) {
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.mesh.update.via-leave-members", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, outcome = tracing::field::Empty);
        async {
            match self.leave_members(&key).await {
                Ok(manifest) => {
                    tracing::Span::current().record("outcome", "exited");
                    self.leave_runs.lock().unwrap().insert(key.clone(), LeaveRun::Exited(owner.clone(), manifest.clone()));
                    self.hand_off(&key, &owner, &manifest).await;
                }
                Err(e) => {
                    tracing::Span::current().record("outcome", "incomplete");
                    // Nothing is kept: a repeated leave-mesh runs the tiers again from the receipts.
                    self.leave_runs.lock().unwrap().remove(&key);
                    tracing::info!(error = %e, "the member workflows are incomplete: no handoff is sent");
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Both tiers: every ordinary node in parallel, then (only once every one has its exact
    /// terminal receipt) every other node-admin in parallel; then the one manifest receipt.
    async fn leave_members(&self, key: &LeaveKey) -> Result<ExitManifest, String> {
        let view = self.topology.read().await.clone();
        let me = view.nodes.iter().find(|n| n.name == self.me).cloned().ok_or_else(|| format!("{} holds no view of itself", self.me))?;
        let others: Vec<Node> = view.members().filter(|n| n.mesh == me.mesh && n.status.is_live() && n.name != me.name).cloned().collect();
        let (admins, ordinary): (Vec<Node>, Vec<Node>) = others.iter().cloned().partition(|n| n.kind == NodeKind::NodeAdmin);
        let mut receipts: Vec<TerminalReceipt> = Vec::new();
        for (tier, members) in [("ordinary-nodes", ordinary), ("node-admins", admins)] {
            let span = tracing::info_span!("rdm.node_admin.mesh.update.via-leave-tier", tier, members = members.len(), mesh_id = %key.mesh_id, operation = %key.operation, outcome = tracing::field::Empty);
            let outcomes = join_all(members.iter().map(|n| self.shutdown_one(key, n))).instrument(span.clone()).await;
            let mut failed = Vec::new();
            for (n, r) in members.iter().zip(outcomes) {
                match r {
                    Ok(t) => receipts.push(t),
                    Err(e) => failed.push(format!("{}: {e}", n.name)),
                }
            }
            span.record("outcome", if failed.is_empty() { "complete" } else { "incomplete" });
            if !failed.is_empty() {
                return Err(format!("tier {tier} is incomplete, the next tier does not start: {}", failed.join("; ")));
            }
        }
        let my_fact = self.held_runtime(&me).ok_or_else(|| format!("{} publishes no runtime fact: its own exact runtime cannot be named", me.name))?;
        let mut roster = others.iter().map(RosterBirth::of).collect::<Result<Vec<_>, _>>()?;
        roster.push(RosterBirth::of(&me)?);
        let manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster, final_primary: RosterBirth::of(&me)?, final_runtime: Some(RuntimeProof::of(&my_fact)), receipts };
        self.builds
            .append_step_receipt(&BuildStepReceipt {
                build_id: crate::build::BuildId(key.build_id.clone()),
                attempt: key.attempt,
                operation: key.operation.clone(),
                step: STEP_OTHER_MEMBERS_EXITED.into(),
                outcome: StepOutcome::Complete,
                output: serde_json::to_value(&manifest).ok(),
                executor: Some(self.me.to_string()),
            })
            .await
            .map_err(|e| format!("the exit manifest could not be recorded: {e}"))?;
        Ok(manifest)
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
        let (Some(me), Some(owner_node), Some(node_rpc)) = (view.nodes.iter().find(|n| n.name == self.me).cloned(), view.nodes.iter().find(|n| &n.node_id == owner).cloned(), self.node_rpc.as_ref()) else {
            tracing::info!(operation = %key.operation, owner = %owner, "the owner of this workflow is not held in this view: the handoff waits for a repeated leave-mesh");
            return;
        };
        let Some(final_runtime) = self.held_runtime(&me) else {
            tracing::info!(node = %me.name, "this admin publishes no runtime fact: the handoff cannot name its exact runtime");
            return;
        };
        let (Some(final_incarnation), final_node_id) = (me.incarnation_id.clone(), me.node_id.clone()) else { return };
        self.lifecycle_events
            .mesh_leave(&MeshLeaveEvent { mesh_id: key.mesh_id.clone(), build_id: key.build_id.clone(), attempt: key.attempt, operation: key.operation.clone(), receipt_manifest: Some(reference.clone()), final_primary: Some((final_node_id.clone(), final_incarnation.clone())) })
            .await;
        let req = StatusRequest::MeshLeave { mesh_id: key.mesh_id.clone(), build_id: key.build_id.clone(), attempt: key.attempt, operation: key.operation.clone(), final_node_id, final_incarnation, final_runtime, receipt_manifest: reference };
        let (out, _) = node_rpc.client.call::<Status>(&rafka_node_rpc::NodeTarget::ExactNode(owner_node.node_id.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        let outcome = match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => format!("{} {:?}", r.value().name(), r.value()),
            other => format!("{} {other:?}", other.name()),
        };
        tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-sent", mesh_id = %key.mesh_id, operation = %key.operation, owner = %owner_node.name, manifest_receipts = manifest.receipts.len(), outcome = %outcome)
            .in_scope(|| tracing::info!("the mesh-leave handoff was called"));
    }

    /// `MeshLeave` at the owner: read the manifest from the sender, validate it against the roster
    /// captured at admission and the final primary's provider access, and release the outer
    /// workflow. A reference alone is not proof.
    pub(super) async fn validate_handoff(&self, _sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::MeshLeave { mesh_id, build_id, attempt, operation, final_node_id, final_incarnation, final_runtime, receipt_manifest } = req else { unreachable!("validate_handoff serves mesh-leave") };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-handoff-validated", mesh_id = %mesh_id, build_id = %build_id, attempt = *attempt, operation = %operation, outcome = tracing::field::Empty, field = tracing::field::Empty);
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
        let receipt = facts.iter().find_map(|f| match f {
            crate::build_state::BuildFact::Step(r) if r.operation == operation && r.attempt == attempt && r.step == STEP_OTHER_MEMBERS_EXITED && r.outcome == StepOutcome::Complete => Some(r.clone()),
            _ => None,
        });
        let Some(receipt) = receipt else {
            return unmatched("receipt_manifest", format!("a Complete {STEP_OTHER_MEMBERS_EXITED} receipt under {operation}, attempt {attempt}"), format!("{receipt_manifest}: none among the {} facts read", facts.len()));
        };
        let manifest: ExitManifest = match receipt.output.clone().and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return unmatched("receipt_manifest", "a readable exit manifest".into(), format!("{receipt_manifest}: its output does not decode")),
        };
        if let Some((field, expected, reported)) = first_mismatch(&manifest, &admitted.roster, &admitted.final_primary, false) {
            return unmatched(&field, expected, reported);
        }
        if manifest.final_runtime.as_ref() != Some(&admitted.final_runtime) {
            return unmatched("final_runtime", format!("{:?}", admitted.final_runtime), format!("{:?}", manifest.final_runtime));
        }
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
