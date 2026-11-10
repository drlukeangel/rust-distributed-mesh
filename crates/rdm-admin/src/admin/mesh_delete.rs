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
pub struct MeshDeleteService {
    pub(super) runner: Arc<AdminRunner>,
}

#[async_trait::async_trait]
impl MeshDeleter for MeshDeleteService {
    async fn delete_mesh(&self, peer: Option<iroh::PublicKey>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        self.runner.clone().admit_delete_mesh(peer, sender, req).await
    }
    async fn mesh_deleted(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        self.runner.validate_handoff(sender, req).await
    }
    async fn birth_deleted(&self, op: &rdm_core::LifecycleOp) {
        self.runner.mesh_left_after_delete(op).await
    }
    async fn birth_of_deleted_mesh(&self, op: &rdm_core::LifecycleOp) -> bool {
        self.runner.mesh_record_deleted(&op.mesh).await
    }
}

impl AdminRunner {
    /// The runtime fact the view holds for `node`'s exact birth (its join report or a topology read).
    fn held_runtime(&self, node: &Node) -> Option<rdm_core::RuntimeFact> {
        self.digest_view.runtime_of(&node.node_id, node.incarnation_id.as_ref()?)?.runtime
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
        n.status = d.status;
        n.routable = false;
        n.transport_addr = Some(d.node.transport_addr);
        let held = self.digest_view.runtime_of(&d.node.node_id, &d.node.incarnation).unwrap_or_default();
        n.data_dir = held.data_dir;
        n.deployment_id = held.runtime.as_ref().and_then(|f| f.deployment().ok());
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
        // Where this owner resumes, from its own Build facts, before it calls anything; the Build this
        // run belongs to keys the fabric-primary role's update (ruling R-PU2).
        let build = self.builds.read_build(build_id).await.map_err(|e| format!("{outer}: the Build {build_id} could not be read for its events: {e}"))?;
        let steps = build.steps.clone();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        // The view is projected now: a primary an earlier run deleted holds no role here, so the
        // admin created for its mesh, or the one that survived it, is read in its role.
        self.refresh_view().await;
        let view = self.topology.read().await.clone();
        match crate::mesh_delete::owner_resume(&steps, &outer.to_string()) {
            crate::mesh_delete::OwnerResume::Left => {
                // Resumed after the departure was recorded: the receipt is the record.
                self.forget_mesh_id(mesh, mesh_id);
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
                // Under fabric.delete (its plan names no mesh) a lost mesh-primary is re-created at its path and
                // name, as without a delete, and it deletes its mesh (mesh-delete.md "Resume after the primary's
                // loss", Q370): the drive dispatches the mesh's delete once its view holds it again.
                if build.topology.meshes.is_empty() {
                    return Err(RunEnd::Incomplete(format!("{outer:?}: {mesh} holds no live node-admin primary: its node recovery re-creates the mesh-primary at its path and name, and a resubmit of the Build hands it the mesh's delete (Q370)")));
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
        let result = self.lead_delete(build_id, mesh, mesh_id, outer, node_rpc, &primary, &key, handoff, build.topology.meshes.is_empty()).await;
        if result.is_err() {
            // This run's open leave ends with it; the receipts are the record a resubmit of the Build reads.
            self.leaves.close(&key);
        }
        result
    }

    /// The roster of `mesh`: its node records on the store ([`crate::mesh_delete::stored_roster`]), each a
    /// birth that wrote its own record (Q356), and the atlas's in-flight creates of the mesh
    /// ([`Self::in_flight_creates`]): a node the Build record names whose launch has not written its record
    /// yet. Each is deleted as any node of the roster (fabric.delete deletes everything).
    async fn stored_roster(&self, mesh: &str, _mesh_id: &MeshId, steps: &[crate::build_state::BuildStep]) -> Result<Vec<RosterBirth>, String> {
        let store = self.records.contacts.get().ok_or_else(|| format!("{} holds no nodes.storage to read the node records of {mesh} from", self.node_id))?;
        let rows = store.nodes(mesh).await.map_err(|e| format!("the node records of {mesh} could not be read: {e}"))?;
        let mut roster = crate::mesh_delete::stored_roster(&rows, mesh, steps);
        roster.extend(self.in_flight_creates(mesh, &rows).await?);
        roster.sort();
        roster.dedup();
        Ok(roster)
    }

    /// The atlas's in-flight creates of `mesh` (the atlas is the Build record's topology and its in-flight
    /// commands): each node the Build records name created in `mesh` (`topology.created`, the node.id the
    /// Build composed at acceptance, else the newest the Build holds minted for a birth of it lost before
    /// Ready) whose AllocateIdentity is Complete and that has no record in `rows`, of the fabric.delete in
    /// force and the Builds it interrupted (back to the newest that completed). Its birth is the launch
    /// identity its create holds ([`crate::deployment::pipeline::launch_of`]), and its runtime is the
    /// provider's find of that launch in its own dir or the replaced birth's dir it took. A launch whose
    /// exact runtime the provider names, running or exited, is a roster birth, and nothing is held for it:
    /// its node.delete resolves that runtime again. One exited with no exact runtime named, or never
    /// launched, has no runtime to delete and is no roster birth. One
    /// `rdm.node_admin.mesh.resolve.via-in-flight-create` span per create names it (`member`, `looked_in`,
    /// `found`).
    async fn in_flight_creates(&self, mesh: &str, rows: &[crate::storage::NodeRecord]) -> Result<Vec<RosterBirth>, String> {
        use crate::deployment::provider::Found;
        let Some((delete, _)) = crate::build_state::fabric_delete_in_force(&*self.builds).await.map_err(|e| format!("the fabric.delete in force could not be read: {e}"))? else { return Ok(Vec::new()) };
        let allocate = crate::deployment::pipeline::CreateStep::AllocateIdentity.name();
        let mut builds = vec![delete.clone()];
        for id in self.builds.accepted_newest_first().await.map_err(|e| format!("the accepted Builds could not be listed: {e}"))? {
            if id == delete.build_id {
                continue;
            }
            let p = self.builds.read_build(&id).await.map_err(|e| format!("the Build {id} could not be read for its creates: {e}"))?;
            if (p.time, &p.build_id) > (delete.time, &delete.build_id) {
                continue;
            }
            let complete = p.state == crate::build_state::BuildState::Complete;
            builds.push(p);
            if complete {
                break;
            }
        }
        let mut out = Vec::new();
        for p in &builds {
            for (at, created) in &p.topology.created {
                let Some(spec) = p.topology.node_spec(at).filter(|s| s.mesh == mesh) else { continue };
                // The birth the create makes now: its own node.id, else the newest the Build holds minted for
                // a birth of it lost before Ready (deployment-provider.md §3).
                let node_id = p.created_chain(created).last().cloned().unwrap_or_else(|| created.clone());
                let operation = rdm_core::OperationId::node("node.create", &node_id).to_string();
                if rows.iter().any(|r| r.node_id == node_id) || !p.steps.iter().any(|s| s.operation == operation && s.step == allocate && s.outcome == StepOutcome::Complete) {
                    continue;
                }
                let (incarnation, deployment_id) = crate::deployment::pipeline::launch_of(&operation, &node_id);
                // Where its launch ran: its own `<data_root>/<node id>`, and the replaced birth's dir a node's
                // executable replacement took (build-deployment.md §3).
                let mut dirs = vec![crate::deployment::node_data_dir(&self.template.data_root, &node_id)];
                if let Some(pin) = p.topology.replaced(at) {
                    let (dir, _, exists) = self.replaced_dir(pin, Some(&spec.storage_persistence)).await;
                    if exists && !dirs.contains(&dir) {
                        dirs.push(dir);
                    }
                }
                let looked_in = dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(",");
                let span = tracing::info_span!("rdm.node_admin.mesh.resolve.via-in-flight-create", node_path = %self.me().path, node_name = %self.me().name, mesh = %mesh, member = %node_id, member_path = %at.path, member_name = %at.name, build_id = %p.build_id.log(), deployment_id = %deployment_id, looked_in = %looked_in, found = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
                let mut found = Found::NeverLaunched;
                for dir in &dirs {
                    let launch = in_flight_launch(mesh, spec, &deployment_id, dir.clone());
                    found = self.provider.find(&launch, crate::deployment::deadline::Deadline::step()).instrument(span.clone()).await.map_err(|e| {
                        span.record("found", "failed").record("detail", e.to_string().as_str());
                        format!("the in-flight create {operation} of {mesh} (launch {deployment_id}) could not be found in {}: {e}", dir.display())
                    })?;
                    if !matches!(found, Found::NeverLaunched) {
                        break;
                    }
                }
                // A launch whose exact runtime the provider names, running or exited, is a roster birth: its
                // node.delete resolves that runtime again from the launch ([`AdminRunner::exited_birth_of`]),
                // nothing held for it beforehand, and terminates it, an exited one's left runtime removed
                // (Q145). One exited with no exact runtime named, or never launched, has none to delete.
                match &found {
                    Found::Running(_) | Found::Exited(Some(_)) => {
                        out.push(RosterBirth { mesh: mesh.to_string(), r#type: spec.r#type.clone(), node: at.clone(), node_id: node_id.to_string(), incarnation_id: incarnation.as_str().to_string() });
                        span.record("found", found.name());
                    }
                    Found::Exited(None) => {
                        span.record("found", "exited-no-runtime");
                    }
                    Found::NeverLaunched => {
                        span.record("found", "never-launched");
                    }
                }
                span.in_scope(|| tracing::info!("an in-flight create of the atlas, read for the roster"));
            }
        }
        Ok(out)
    }

    /// The leaving mesh holds no node-admin primary. Before admission that refuses the leave; after
    /// it, with no admin of the mesh left live, the fabric-primary resumes the leave through the one
    /// node-admin it creates for the mesh (mesh-delete.md, "Resume after the primary's loss", steps
    /// 1-2): the lost final primary is deleted first, `TerminateRuntime` first when it runs; then the
    /// admin is created at a path no birth held,
    /// into the existing mesh under its existing MeshId. A live admin not yet in its role is no loss:
    /// the section ends incomplete, and a resubmit of the Build hands that admin the workflow.
    async fn admin_for_deleting_mesh(&self, build_id: &crate::build::BuildId, mesh: &str, key: &MeshDeleteKey, steps: &[crate::build_state::BuildStep], view: &Topology) -> Result<Node, RunEnd> {
        let outer = &key.operation;
        let no_primary = format!("{outer:?} is refused: {mesh} holds no node-admin primary to hand the workflow to");
        // Before admission the leave is refused by name: nothing was dismantled and nothing is resumed.
        if !steps.iter().any(|s| s.operation == outer.to_string() && s.step == STEP_ACCEPTED_ROSTER && s.outcome == StepOutcome::Complete) {
            return Err(RunEnd::failed(no_primary));
        }
        let roster = self.stored_roster(mesh, &key.mesh_id, steps).await.map_err(|e| format!("{outer}: {e}"))?;
        if let Some(live) = view.nodes_of_type(mesh, "node-admin").find(|n| n.status.is_live()) {
            return Err(RunEnd::Incomplete(format!("{no_primary}: its admin {} is live and holds no primary role yet, so it is not lost; a resubmit of the Build hands the workflow to the admin primary", live.node_id)));
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
        now.node_type_primary(mesh, "node-admin").cloned().ok_or_else(|| RunEnd::Incomplete(format!("{outer:?}: path={node_path:?} name={node_name:?} was created for {mesh} and holds no mesh-primary role yet; a resubmit of the Build amends the roster for it")))
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
            other => return Err(format!("path={node_path:?} name={node_name:?} is not a path no birth held: the node.create targeted {other:?}")),
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
        row.and_then(|l| l.record.executable).ok_or_else(|| format!("node.create:path={node_path:?} name={node_name:?}: the row of the predecessor {} {} records no executable, so the admin created for {} has none to run; nothing is launched", predecessor.node_id, predecessor.incarnation_id, predecessor.mesh))
    }

    /// The node record of the roster birth `b` (the lost final primary, or another node-admin of the
    /// leaving mesh), read by its node id and incarnation ([`crate::admin::birth_runtime_row`]); a
    /// roster birth whose ids do not parse is refused by name.
    async fn roster_birth_row(&self, store: &dyn crate::storage::NodesStorage, b: &RosterBirth) -> Result<Option<crate::storage::NodeRecord>, String> {
        let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("the roster names {} by node id `{}`, which does not parse: {e}", b.node_id, b.node_id))?;
        crate::admin::birth_runtime_row(store, &self.held_name(), &node_id, &b.birth().map_err(|e| format!("the roster names {} at birth `{}`, which does not parse: {e}", b.node_id, b.incarnation_id))?).await
    }

    /// "When none survives" (mesh-delete.md, "Resume after the primary's loss", step 2), for the
    /// node-admin `b` of the leaving mesh `mesh` other than the lost final primary, which this view
    /// holds silent: its node.delete, unless this leave already holds its receipt, this fabric-primary
    /// the executor, `TerminateRuntime` first when it runs (node-recovery.md step 3). A runtime unknown
    /// here ends the section incomplete, named, and nothing is created for the mesh.
    async fn delete_silent_admin(&self, build_id: &crate::build::BuildId, key: &MeshDeleteKey, mesh: &str, b: &RosterBirth, steps: &[crate::build_state::BuildStep]) -> Result<(), RunEnd> {
        let outer = &key.operation;
        if crate::mesh_delete::exit_receipts(steps, std::slice::from_ref(b)).contains_key(&(b.node_id.clone(), b.incarnation_id.clone())) {
            return Ok(());
        }
        // An admin node investigation reaches is not lost (Q111): silent in this view only for a moment (the
        // lost primary published the mesh's topology), it is the surviving admin the election names
        // (mesh-delete.md "Resume after the primary's loss" step 2), and a resubmit of the Build hands
        // it the workflow. Nothing is deleted or created for the mesh.
        let held = self.topology.read().await.nodes_of_type(mesh, "node-admin").find(|n| n.node_id.as_str() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id)).cloned();
        if let Some(n) = held {
            if matches!(crate::path_investigation::PathProbe::investigate(self, &n).await, crate::node_investigation::Finding::Reached { .. }) {
                return Err(RunEnd::Incomplete(format!("{outer:?}: {mesh}'s node-admin {} {} answers, so it survives; a resubmit of the Build hands the workflow to the admin the election names", b.node_id, b.incarnation_id)));
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
    /// leave's Build; the `TerminateRuntime` receipt under `node.delete` with an explicit node ID is the
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

    /// Under an in-force fabric.delete, the mesh-primary `primary` of `mesh` did not answer `DeleteMesh`
    /// (the call `ended` with no reply). "A mesh whose mesh-primary does not answer its mesh.delete is
    /// reached the way an unheard mesh-primary is reached in a live fabric (node investigation: its exact
    /// birth, direct then via peers of its mesh), and during fabric.delete the outcome is a delete"
    /// (fabric-delete.md step 1): reached, the primary runs the mesh's mesh.delete, the section ends
    /// incomplete and a resubmit of the Build hands it the workflow; any other verdict, the primary is lost
    /// (mesh-delete.md "Resume after the primary's loss"): its node recovery re-creates it at its path and
    /// name, the section ends incomplete, and a resubmit of the Build hands the re-created primary the mesh's
    /// delete (Q370).
    async fn delete_mesh_unanswered(&self, build_id: &crate::build::BuildId, mesh: &str, key: &MeshDeleteKey, primary: &Node, ended: &str) -> Result<(), RunEnd> {
        let outer = &key.operation;
        let me = self.held_name();
        let finding = crate::path_investigation::PathProbe::investigate(self, primary).await;
        let span = tracing::info_span!(
            "rdm.node_admin.mesh.resolve.via-primary-investigation",
            node_path = %me.path, node_name = %me.name,
            mesh = %mesh, mesh_id = %key.mesh_id,
            build_id = %build_id.log(),
            operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step,
            primary_node_id = %primary.node_id,
            primary_incarnation = %primary.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or(""),
            call = %ended,
            verdict = finding.name(),
        );
        if matches!(finding, crate::node_investigation::Finding::Reached { .. }) {
            span.in_scope(|| tracing::info!("the mesh-primary that did not answer delete-mesh is reached by node investigation: it runs the mesh's mesh.delete"));
            return Err(RunEnd::Incomplete(format!("{outer:?}: delete-mesh to {} ended {ended}, and node investigation reaches it: it runs {mesh}'s mesh.delete, and a resubmit of the Build hands it the workflow", primary.node_id)));
        }
        span.in_scope(|| tracing::info!("the mesh-primary that did not answer delete-mesh is lost by node investigation: its node recovery re-creates it at its path and name"));
        Err(RunEnd::Incomplete(format!("{outer:?}: delete-mesh to {} ended {ended}, and node investigation does not reach it ({}): its node recovery re-creates it at its path and name, and a resubmit of the Build hands it {mesh}'s delete (Q370)", primary.node_id, finding.name())))
    }

    /// Steps 2-5 of the owner's workflow, for the leave `key` of `mesh` opened at admission: `mesh.deleting`,
    /// `DeleteMesh`, the validated handoff (mesh.deleted) and its completion receipt. Under an in-force
    /// fabric.delete (`under_fabric_delete`), a `DeleteMesh` the primary does not answer is
    /// [`Self::delete_mesh_unanswered`]'s.
    #[allow(clippy::too_many_arguments)]
    async fn lead_delete(
        &self,
        build_id: &crate::build::BuildId,
        mesh: &str,
        mesh_id: &MeshId,
        outer: &rdm_core::OperationId,
        node_rpc: &crate::node_rpc::ProcessNodeRpc,
        primary: &Node,
        key: &MeshDeleteKey,
        handoff: tokio::sync::watch::Receiver<Option<crate::mesh_delete::MeshDeleteOutcome>>,
        under_fabric_delete: bool,
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
            // The call did not come back: the primary is unreached or the stream broke. Under fabric.delete
            // the primary that does not answer its mesh.delete is reached by node investigation
            // (fabric-delete.md step 1); the open leave ends here, its receipts the record either way.
            Err(ended) if under_fabric_delete => {
                self.leaves.close(key);
                return self.delete_mesh_unanswered(build_id, mesh, key, primary, &ended).await;
            }
            // Otherwise a broken stream proves neither a failed step nor a completed one
            // (node-rpc-envelope.md, "Caller execution and certainty"): the leave is incomplete and its
            // receipts stand.
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
    /// reads of its runtime: the wait ends, and a resubmit of the Build resumes the leave after the
    /// primary's loss (mesh-delete.md "Resume after the primary's loss"), whatever the verdict (node-recovery.md
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
                Ok(found) => found.name().to_string(),
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
    /// not hold deleted (D67). The mesh is the deleted mesh of the birth's mesh name (`op.mesh`, the
    /// first location of its path) whose `mesh.delete:<mesh_id>` step event the newest Build holding one
    /// holds: only a mesh whose record is deleted has one, its mesh-primary writing the mesh's record
    /// deleted before it reports mesh.deleted (mesh-delete.md step 5). The Builds are read newest first:
    /// the final primary's own node.delete is a Build of its own, accepted after it, so the current Build
    /// is not it. `None` when no such mesh.delete is open.
    async fn mesh_deleted_of(&self, op: &rdm_core::LifecycleOp) -> Result<Option<(crate::build::BuildId, rdm_core::OperationId, MeshId)>, String> {
        let deleted = self.deleted_mesh_ids(&op.mesh).await?;
        if deleted.is_empty() {
            return Ok(None);
        }
        for id in self.builds.accepted_newest_first().await.map_err(|e| e.to_string())? {
            let p = match self.builds.read_build(&id).await {
                Ok(p) => p,
                Err(crate::build_state::BuildStateError::UnknownBuild(_)) => continue,
                Err(e) => return Err(e.to_string()),
            };
            for mesh_id in &deleted {
                let operation = crate::mesh_delete::operation(mesh_id);
                let key = operation.to_string();
                if p.steps.iter().any(|s| s.operation == key) {
                    return Ok((crate::mesh_delete::owner_resume(&p.steps, &key) == crate::mesh_delete::OwnerResume::Deleted).then(|| (p.build_id.clone(), operation, mesh_id.clone())));
                }
            }
        }
        Ok(None)
    }

    /// The ids of the deleted meshes named `mesh` (`MeshState::Deleted` records). A mesh is created once:
    /// a later mesh of the same name is another mesh, known by its id (gossip.md "Mesh delete").
    async fn deleted_mesh_ids(&self, mesh: &str) -> Result<Vec<MeshId>, String> {
        Ok(self.mesh_storage.meshes().await.map_err(|e| format!("the mesh records could not be read for {mesh}: {e}"))?.into_iter().filter(|m| m.name == mesh && m.state == rdm_core::status::MeshState::Deleted).map(|m| m.mesh_id).collect())
    }

    /// Whether a mesh named `mesh` has a deleted record: the one fact that decides this executor publishes
    /// no `NodeStopped` and no `NodeDeleted` for a birth of it (gossip.md "Mesh delete": after mesh.deleted
    /// the fabric-primary "publishes nothing for the mesh or that admin"; Q414). A record read that fails
    /// is spanned by name and decides nothing: the mesh is not known deleted.
    pub(super) async fn mesh_record_deleted(&self, mesh: &str) -> bool {
        match self.deleted_mesh_ids(mesh).await {
            Ok(deleted) => !deleted.is_empty(),
            Err(reason) => {
                let me = self.held_name();
                tracing::info_span!("rdm.node_admin.mesh.reject.via-record-read", node_path = %me.path, node_name = %me.name, mesh = %mesh, reason = %reason)
                    .in_scope(|| tracing::info!("the mesh is not known deleted: its record could not be read"));
                false
            }
        }
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
            .write_step(&crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, subject: None, operation: outer.clone(), step: STEP_MESH_LEFT.into(), outcome: StepOutcome::Complete })
            .await
            .map_err(|e| format!("{outer:?}: the MeshLeft receipt could not be recorded: {e}"))?;
        tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-left-receipt", mesh_id = %mesh_id, build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, receipts = all.receipts.len(), roster = all.roster.len(), manifest = ?reference)
            .in_scope(|| tracing::info!("every exact exit is proven: the MeshLeft receipt is recorded"));
        self.digest_view.mesh_left(mesh, build_id.as_str(), self.lifecycle_events.now_rafka_ms());
        self.forget_mesh_id(mesh, mesh_id);
        self.refresh_view().await;
        Ok(())
    }

    /// A mesh that has left keeps no id here: the MeshLeft receipt is the end of that mesh, and a
    /// mesh of the same name created after it is a new mesh with a new id. The left mesh's exact id is
    /// dropped from the topology view at the receipt, never left for a later sweep to find the mesh
    /// without a member; a name the view already holds under the new mesh's id keeps it.
    fn forget_mesh_id(&self, mesh: &str, mesh_id: &MeshId) {
        if self.digest_view.forget_mesh_id(mesh, mesh_id) {
            // The projection reads the mesh ids held: the view's rebuild is due (Q254).
            self.records.inputs_moved();
        }
        self.records.deleted_meshes_forget(mesh_id);
    }

    /// `DeleteMesh` at the leaving mesh's primary: admit it and run the member workflows.
    pub(super) async fn admit_delete_mesh(self: Arc<Self>, peer: Option<iroh::PublicKey>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::DeleteMesh { mesh_id, build_id, operation } = req else { unreachable!("admit_delete_mesh serves delete-mesh") };
        let me_name = self.held_name();
        let view = self.topology.read().await.clone();
        let Some(me) = view.own(&self.node_id).cloned() else {
            return rdm_core::reply::status::not_ready(format!("{} holds no view of itself yet; delete-mesh for {mesh_id} is refused", self.node_id));
        };
        let held = self.digest_view.mesh_id_of(&me.mesh).or_else(|| view.meshes.iter().find(|m| m.name == me.mesh).and_then(|m| m.id.clone()));
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
    /// `(mesh_id, Leaving)` ([`crate::mesh_delete::run_admitted`]): a `delete-mesh` of a later run
    /// admitted meanwhile joins it, and the handoff goes under the key admitted last.
    async fn run_leave(self: Arc<Self>, key: MeshDeleteKey, parent: tracing::Span) {
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.mesh.update.via-leave-members", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, outcome = tracing::field::Empty);
        async {
            let end = crate::mesh_delete::run_admitted(&self.mesh_delete_runs, &key.mesh_id, |key, _| {
                let runner = self.clone();
                async move { runner.leave_members(&key).await }
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
                    // delete-mesh, or a resubmit of the Build, runs only the members without one.
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
    /// ([`crate::mesh_delete::members_exited`]); the Build is read from its committed records (Q292).
    async fn leave_members(&self, key: &MeshDeleteKey) -> Result<ExitManifest, String> {
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
        // The Build is read from its committed records first, as a section's run reads it (Q292): the
        // leaving primary holds no record of the Build its delete-mesh names until it reads it.
        let facts = self.local.committed_facts(Some(&key.build_id)).await.map_err(|e| format!("{me_name}: the committed records of Build {} could not be read for the leave's roster: {e}", key.build_id))?;
        self.local.absorb_facts(&facts).await;
        let steps = self.builds.read_build(&key.build_id).await.map_err(|e| format!("{me_name}: the Build {} could not be read for the leave's roster: {e}", key.build_id))?.steps;
        let stored = self.stored_roster(&me.mesh, &key.mesh_id, &steps).await?;
        let roster = crate::mesh_delete::leaving_roster(&*self.builds, key, &me_name, &me_birth, || Ok(stored)).await?;
        let mesh: Vec<Node> = view.nodes.iter().filter(|n| n.mesh == me.mesh).cloned().collect();
        let records = crate::mesh_delete::roster_records(&mesh, &roster, |b| self.held_record(b));
        members_exited(&*self.builds, key, &me_name, &roster, &me_birth, my_runtime, &records, |n| async move { self.shutdown_one(key, &n).await }).await
    }

    /// One member's end (mesh-delete.md step 3: "The mesh-primary runs the whole node.delete for
    /// every ordinary node"): the ordinary node.delete workflow ([`AdminRunner::delete_birth`]) of the
    /// exact birth, under the mesh.delete's Build. The provider's terminal receipt is the
    /// member's roster receipt.
    async fn shutdown_one(&self, key: &MeshDeleteKey, n: &Node) -> Result<TerminalReceipt, String> {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-shutdown-member", node_path = %n.path, node_name = %n.name, node_id = %n.node_id, operation = %key.operation.action, operation_subject = ?key.operation.subject, build_id = %key.build_id.log(), outcome = tracing::field::Empty);
        let result = async {
            // A member whose delete already holds its TerminateRuntime receipt runs on from its receipts
            // when its runtime is no longer resolved here (no digest, no runtime row), as an unreached
            // birth's delete does (`delete_unreached_as`): the exited verdict is a step already taken.
            // A member that wrote no record (an in-flight create's birth, Q356) is resolved from the launch
            // its create left, as the Build records that create ([`AdminRunner::exited_birth_of`]).
            let (record, handle) = match self.handle_for(n).await {
                Ok(resolved) => resolved,
                Err(e) => {
                    let from = n.incarnation_id.clone().map(|incarnation| crate::build::ExitedBirth { node_id: n.node_id.clone(), incarnation });
                    match from {
                        Some(from) => match self.exited_birth_of("node.delete", &n.location(), &from, None).await {
                            Ok(resolved) => resolved,
                            Err(unresolved) => super::runner::receipted_exit(&*self.builds, self.records.contacts.get().map(|s| &**s as &dyn crate::storage::NodesStorage), Some(&key.build_id), &n, &from, &rdm_core::OperationId::node("node.delete", &from.node_id)).await.ok_or(format!("{e}; {unresolved}"))?,
                        },
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
                        let mesh = self.digest_view.mesh_ids().into_iter().find(|(_, id)| id == mesh_id).map(|(m, _)| m);
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
                    // births named, and this run ends inside the call.
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

/// The launch of an in-flight create of `mesh` that a roster capture asks the provider to find: the
/// accepted entry `spec`'s node, its launch `deployment_id` in `data_dir`, running the executable the
/// create launched, the entry's binding (deployment-provider.md §2, "Find is for the launcher's
/// successor"). The process provider's find reads a launch running only while its pid runs that file;
/// an entry that names no executable (a Build accepted before the field) finds none running.
fn in_flight_launch(mesh: &str, spec: &rdm_core::meta::NodeSpec, deployment_id: &crate::model::DeploymentId, data_dir: std::path::PathBuf) -> crate::deployment::provider::ResolvedNodeLaunch {
    let node = rdm_core::PathName { path: spec.path.clone(), name: spec.name.clone() };
    crate::deployment::provider::ResolvedNodeLaunch::for_find(mesh, &spec.r#type, &node, deployment_id, spec.executable.as_ref().map(|b| b.executable.clone()).unwrap_or_default(), data_dir)
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
    use rdm_gossip::backbone::Backbone;
    use rdm_gossip::topology::TopologyView;
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
        rig_seamed(name, None).await
    }

    /// [`rig`], its runner's create acting on a target through `seam`.
    async fn rig_seamed(name: &str, seam: Option<Arc<dyn crate::wiring::TargetSeam>>) -> Rig {
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

        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh1_id, "mesh1.node-admin.1", Arc::new(time.clone()), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await
        .unwrap();
        channels.set_publisher(&rdm_core::PublisherId {
                incarnation: incarnation.clone(),
                node_id: node_id.clone(),
            });
        channels.join(rdm_gossip::channel::ChannelId::backbone(topology_view.fabric_id()), vec![]).await
        .unwrap();
        let backbone = channels.backbone().expect("the backbone is joined");

        let dir = crate::test_support::tempdir(name);
        let providers = crate::storage_backend::TopologyStorageProviders::file(dir.clone());
        let scope = crate::storage_backend::StorageScope {
            data_dir: dir.clone(),
            fabric_id: fabric_id.clone(),
            own_mesh: "mesh1".into(),
            writer: incarnation.clone(),
            node_id: node_id.clone(),
            time: rdm_transport::clock::RafkaTime::unadopted(),
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
        ));
        let runner = Arc::new(AdminRunner {
            storage_changes,
            target_seam: seam,
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
                status_service: Arc::default(),
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
                channels: channels.clone(),
                backbone,
                me_path: me.path.clone(), me_name: me.name.clone(),
                mesh_deleter: Arc::default(),
                project: None,
                status_service: Arc::default(),
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
        // The final primary's record, written Deleted by its node.delete.
        let final_row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "node-admin", final_id.clone(), final_inc.clone(), NodeStatus::Deleted);
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
        runner.digest_view.hold_mesh_id("mesh2", Some(&mesh2_id));
        assert_eq!(runner.delete_mesh(&build_id, "mesh2", &mesh2_id).await, Ok(()));

        // 2. `mesh_left_after_delete` records `MeshLeft` and drops mesh2's id from the topology view once the final
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
                .write_step(&crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, subject: None, operation: del_op.clone(), step: step.name().into(), outcome: StepOutcome::Complete })
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
        assert_eq!(runner.digest_view.mesh_id_of("mesh2"), None, "the left mesh keeps no id");

        // 3. `delete_mesh` also returns `Ok(())` when `OwnerResume::Left` is reached.
        runner.digest_view.hold_mesh_id("mesh2", Some(&mesh2_id));
        assert_eq!(runner.delete_mesh(&build_id, "mesh2", &mesh2_id).await, Ok(()));
        assert_eq!(runner.digest_view.mesh_id_of("mesh2"), None, "the left mesh keeps no id");

        ep.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-delete.md, "Resume after the primary's loss", step 1:
    /// "a result of `ready` refuses the resume by name, since the primary is not lost". The leaving
    /// mesh2's node-admin `mesh2.node-admin.1` is silent in the fabric-primary's view, and its exact birth
    /// answers node investigation's probe (`ProbeNodeState`, `Current`): the resume ends incomplete
    /// naming it as alive, and nothing of it is deleted (no step event of its node.delete). fabric-delete.md
    /// step 1, "A mesh whose mesh-primary does not answer its mesh.delete is reached the way an unheard
    /// mesh-primary is reached in a live fabric (node investigation ...)": the same admin as the mesh's
    /// primary whose delete-mesh ended with no reply under fabric.delete is reached, so the section ends
    /// incomplete naming it and the mesh is not deleted in process. Must NOT happen: a node-admin that
    /// answers deleted as lost, its mesh deleted in process, or an admin created for its mesh.
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
        // fabric-delete.md step 1: the same admin, mesh2's primary, whose delete-mesh ended with no reply
        // under fabric.delete, is reached by node investigation: it runs its mesh's mesh.delete, the section
        // ends incomplete naming it, and nothing of the mesh is deleted in process.
        let primary = runner.topology.read().await.nodes.iter().find(|n| n.node_id == admin_id).cloned().unwrap();
        match runner.delete_mesh_unanswered(&build_id, "mesh2", &key, &primary, "NotSent (timed out)").await {
            Err(RunEnd::Incomplete(reason)) => assert!(reason.contains(admin_id.as_str()) && reason.contains("node investigation reaches it"), "the unanswered primary that answers its investigation is named: {reason}"),
            other => panic!("an unanswered primary that node investigation reaches ends the section incomplete by name: {other:?}"),
        }
        let after = steps.read_build(&build_id).await.unwrap().steps;
        let of_its_delete = rdm_core::OperationId::node("node.delete", &admin_id).to_string();
        assert!(after.iter().all(|s| s.operation != of_its_delete), "nothing of the admin that answered is deleted: {after:?}");
        assert!(after.iter().all(|s| s.step != STEP_ACCEPTED_ROSTER && s.step != STEP_MESH_DELETED), "mesh2 is not deleted in process: {after:?}");

        router.shutdown().await.unwrap();
        admin_ep.close().await;
        ep.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (fabric-delete.md "Receipts"; deployment-provider.md §2, "Find is for the launcher's
    /// successor"): the launch a roster capture finds for an in-flight create names the executable the
    /// create launched, its accepted entry's binding, so the process provider's find of a launch whose
    /// process still runs is `Running` (a birth of the roster, deleted with it), and `Exited` once it
    /// has exited. Must NOT happen: a running launch found `Exited` (it is no roster birth, nothing
    /// deletes it, and it outlives fabric.deleted), as a find naming no executable reads every launch.
    #[tokio::test]
    async fn the_in_flight_launch_names_its_entrys_executable_so_a_running_launch_is_found_running() {
        use crate::deployment::provider::{DeploymentProvider as _, Found};
        let dir = crate::test_support::tempdir("in-flight-launch-find");
        let spec = rdm_core::meta::NodeSpec {
            path: "mesh1.node".into(), name: "2".into(), mesh: "mesh1".into(), r#type: "node".into(),
            storage_persistence: rdm_core::meta::StoragePersistence::Ephemeral,
            executable: Some(rdm_core::binding::Binding { launch_id: "node".into(), executable: "/bin/sleep".into(), sha256: String::new(), image: None }),
            tags: Default::default(), config: Default::default(),
        };
        let deployment_id = crate::model::DeploymentId::mint();
        let launch = in_flight_launch("mesh1", &spec, &deployment_id, dir.clone());
        assert_eq!(launch.executable, std::path::PathBuf::from("/bin/sleep"), "the launch names the entry's executable");
        let p = ProcessDeploymentProvider::new();
        let spawned = crate::deployment::provider::ResolvedNodeLaunch { args: vec!["30".into()], ..launch.clone() };
        let h = p.spawn(&spawned, crate::deployment::deadline::Deadline::step()).await.unwrap();
        assert_eq!(p.find(&launch, crate::deployment::deadline::Deadline::step()).await.unwrap(), Found::Running(h.clone()), "the launch whose process runs is found running");
        let unnamed = rdm_core::meta::NodeSpec { executable: None, ..spec.clone() };
        assert!(matches!(p.find(&in_flight_launch("mesh1", &unnamed, &deployment_id, dir.clone()), crate::deployment::deadline::Deadline::step()).await.unwrap(), Found::Exited(_)), "an entry naming no executable finds no launch running");
        p.terminate(&h, crate::deployment::provider::TerminationMode::Immediate, crate::deployment::deadline::Deadline::step()).await.unwrap();
        assert!(matches!(p.find(&launch, crate::deployment::deadline::Deadline::step()).await.unwrap(), Found::Exited(Some(_))), "its process is gone: exited");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (rdm#334 A, "the record is in the build and the atlas"; Q356: a node writes its own record;
    /// mesh-delete.md "Resume after the primary's loss" step 1: node.delete of the lost roster birth,
    /// `TerminateRuntime` first when it runs): the fabric-primary's mesh.delete deletes a birth of the
    /// leaving mesh that joined and was lost before it wrote its record, which no runtime fact and no
    /// record name: its runtime is the launch the create of its node made, found at the provider by that
    /// create's deployment id, and the runtime that still runs is terminated. Must NOT happen: the delete
    /// ending "lifecycle record absent", or the runtime left running (rdm#374, finding 9 of its first review),
    /// or the launch looked up with an empty executable and read exited while it runs (finding 7 of its
    /// second review): the Build's create names it.
    #[tokio::test]
    async fn a_mesh_delete_of_a_birth_that_wrote_no_record_terminates_the_runtime_its_launch_left() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        let capture = crate::span_capture::Capture::new("unrecorded-launch-delete");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-launch-delete").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let node_id = NodeId::mint();
        // The Build's create of the node names its executable, `/bin/sleep`; its launch still runs, and the
        // view holds the birth dead: it wrote no record and published no runtime fact.
        let topology = naming_creates(&rig, &[(node.clone(), node_id.clone(), rdm_core::meta::StoragePersistence::Ephemeral, None)]);
        let (incarnation, left) = lost_before_its_record_in(&rig, &build_id, topology, &node, &node_id, crate::deployment::node_data_dir(&rig.dir, &node_id)).await;
        let runner = &rig.runner;
        let roster = RosterBirth { mesh: "mesh1".into(), r#type: "node".into(), node: node.clone(), node_id: node_id.to_string(), incarnation_id: incarnation.as_str().to_string() };

        let terminal = runner.delete_lost_birth(&build_id, &roster).await.unwrap_or_else(|e| panic!("mesh.delete of the birth that wrote no record: {e:?}"));
        assert!(matches!(runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the runtime the launch left is terminated: {terminal:?}");
        assert!(runner.nodes_storage.node(&node_id).await.unwrap().is_none(), "nothing is written for a birth that wrote no record");
        assert!(!runner.handles.lock().unwrap().contains_key(&node_id), "no handle of the birth is kept");
        // The delete names no create of its own: the launch is looked up with the executable its Build's
        // create named, and found running (rdm#374, finding 7 of its second review).
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        let resolved: Vec<_> = capture.named("rdm.node_admin.node.resolve.via-unrecorded-launch").into_iter().filter(|s| attr(s, "node_id") == node_id.to_string()).collect();
        assert!(!resolved.is_empty(), "the birth's runtime is resolved from its launch: {terminal:?}");
        assert_eq!((attr(&resolved[0], "executable").as_str(), attr(&resolved[0], "found").as_str()), ("/bin/sleep", "running"), "{:?}", resolved[0].attributes);
        for s in &resolved {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// The rig's admin as mesh1's primary, the delete's writer, in its own view; the Build `build_id`
    /// accepted; and the birth `node_id` at `node`, launched by `node.create:<node_id>` running `/bin/sleep
    /// 30` on `data_dir` (its record there names the create's deployment id, as its DeployRuntime wrote it),
    /// lost before it wrote its own record (Q356): the view holds it dead, with no runtime fact. Answers the
    /// birth's incarnation and its runtime's handle.
    async fn lost_before_its_record(rig: &Rig, build_id: &crate::build::BuildId, node: &rdm_core::PathName, node_id: &NodeId, data_dir: std::path::PathBuf) -> (IncarnationId, crate::deployment::provider::DeploymentHandle) {
        lost_before_its_record_in(rig, build_id, FabricTopology::root("fab", "mesh1", rig.mesh1_id.clone()), node, node_id, data_dir).await
    }

    /// [`lost_before_its_record`], the Build accepted with `topology`.
    async fn lost_before_its_record_in(rig: &Rig, build_id: &crate::build::BuildId, topology: FabricTopology, node: &rdm_core::PathName, node_id: &NodeId, data_dir: std::path::PathBuf) -> (IncarnationId, crate::deployment::provider::DeploymentHandle) {
        use crate::deployment::provider::{DeploymentProvider as _, ResolvedNodeLaunch};
        let runner = &rig.runner;
        rig.steps
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1000 })
            .await
            .unwrap();
        let (incarnation, deployment_id) = crate::deployment::pipeline::launch_of(&rdm_core::OperationId::node("node.create", node_id).to_string(), node_id);
        let launch = ResolvedNodeLaunch { args: vec!["30".into()], ..ResolvedNodeLaunch::for_find("mesh1", "node", node, &deployment_id, "/bin/sleep".into(), data_dir) };
        let left = runner.provider.spawn(&launch, crate::deployment::deadline::Deadline::step()).await.unwrap();
        let mut me = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        me.node_id = runner.node_id.clone();
        me.incarnation_id = Some(runner.primary_update.incarnation.clone());
        me.status = NodeStatus::Ready;
        me.is_primary = true;
        let mut birth = Node::allocated(node.clone(), "mesh1", "node");
        birth.node_id = node_id.clone();
        birth.incarnation_id = Some(incarnation.clone());
        birth.status = NodeStatus::Dead;
        runner.topology.write().await.nodes.extend([me, birth]);
        (incarnation, left)
    }

    /// The Build `build_id`'s create of `node_id` at `node` in mesh1, its step naming `binding`, run as its
    /// executor runs it: over the Build as the executor holds it now (`create_node_body`'s read).
    async fn create_at(rig: &Rig, build_id: &crate::build::BuildId, node: &rdm_core::PathName, node_id: &NodeId, binding: &rdm_core::binding::Binding, replacing: Option<(&crate::build::PinnedBirth, Option<&rdm_core::meta::StoragePersistence>)>) -> Result<(), crate::executor::RunEnd> {
        let held = rig.runner.builds.read_build(build_id).await.unwrap();
        rig.runner.create(&held, build_id, node, "mesh1", "node", Some(binding), None, None, crate::deployment::pipeline::LaunchOf::Create, node_id, &rdm_core::OperationId::node("node.create", node_id), replacing).await
    }

    /// The executable a create's step names here: `/bin/sleep`, bound by its sha. A birth launched from
    /// it with no operand exits at once, so a create over a lost birth ends at its new birth's bind.
    fn sleep_binding() -> rdm_core::binding::Binding {
        let executable = std::path::PathBuf::from("/bin/sleep");
        let sha256 = rdm_core::binding::sha256_file(&executable).unwrap();
        rdm_core::binding::Binding { launch_id: "node".into(), executable, sha256, image: None }
    }

    /// CONTRACT (rdm#334 A, "the record is in the build and the atlas"; Q356; build.md §4: a resubmit
    /// runs every step of the create whose event did not end complete; node-recovery.md "Delete blocks the
    /// old instance before the new join"): a Build's create of mesh1.node.4 whose own birth joined and was
    /// lost before it wrote its record, run again: its path check finds the birth unreached, and its
    /// node.delete takes the birth's runtime from the launch the create made, found at the provider by the
    /// create's deployment id in the birth's data dir, and terminates the runtime that still runs; nothing
    /// is written for the birth and no handle of it is kept; the path takes a new node.id. Must NOT
    /// happen: the delete ending "lifecycle record absent" (admins_lost_mid_build, release gate at
    /// dcabfde1b), the runtime left running beside the birth the path takes next, or a handle of it kept
    /// after the delete (rdm#374, findings 7 and 9 of its first review).
    #[tokio::test]
    async fn a_resubmitted_create_deletes_its_own_birth_that_wrote_no_record_through_the_runtime_its_launch_left() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        let capture = crate::span_capture::Capture::new("unrecorded-own-birth");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-own-birth").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let node_id = NodeId::mint();
        let launch_dir = crate::deployment::node_data_dir(&rig.dir, &node_id);
        let (_, left) = lost_before_its_record(&rig, &build_id, &node, &node_id, launch_dir.clone()).await;
        let runner = &rig.runner;

        let created = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), None).await;
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        let resolved: Vec<_> = capture.named("rdm.node_admin.node.resolve.via-unrecorded-launch").into_iter().filter(|s| attr(s, "node_id") == node_id.to_string()).collect();
        assert!(!resolved.is_empty(), "the birth's runtime is resolved from its launch: {created:?}");
        assert_eq!((attr(&resolved[0], "operation").as_str(), attr(&resolved[0], "found").as_str(), attr(&resolved[0], "data_dir")), ("node.delete", "running", launch_dir.display().to_string()), "{:?}", resolved[0].attributes);
        assert!(matches!(runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the runtime the launch left is terminated: {created:?}");
        assert!(runner.nodes_storage.node(&node_id).await.unwrap().is_none(), "nothing is written for a birth that wrote no record");
        assert!(!runner.handles.lock().unwrap().contains_key(&node_id), "no handle of the deleted birth is kept");
        let minted = capture.named("rdm.node_admin.node.create.via-birth-lost-before-ready");
        assert!(minted.iter().any(|s| attr(s, "node_id") == node_id.to_string()), "the path takes a new node.id: {created:?} {minted:?}");
        for s in &resolved {
            eprintln!("span rdm.node_admin.node.resolve.via-unrecorded-launch trace_id={} {:?}", s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (build-deployment.md §3, Q378: "The new node takes its predecessor's data dir when its
    /// `storage_persistence` is `persistent`"; node-recovery.md step 4: the create over a birth whose
    /// delete read it exited takes its data dir): a node's executable replacement of mesh1.node.4, its
    /// entry persistent, created its birth over the replaced birth's data dir; that birth joined, was
    /// lost before it wrote its record, and its runtime has exited. The create, run again, deletes it
    /// through the launch found in that inherited dir, reads it exited, and the birth it creates over it
    /// takes the same dir. Must NOT happen: the new birth started on an empty `<data_root>/<lost node
    /// id>`, the replaced birth's data left behind (rdm#374, finding 6 of its first review).
    #[tokio::test]
    async fn a_replacement_whose_birth_was_lost_before_its_record_carries_the_inherited_data_dir_into_the_create_over_it() {
        use crate::deployment::provider::DeploymentProvider as _;
        use crate::storage::NodesStorage;
        let capture = crate::span_capture::Capture::new("unrecorded-replacement");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-replacement").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        // The replaced birth: deleted by the replacement, its record naming its data dir.
        let replaced = crate::build::PinnedBirth { r#type: "node".into(), mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let inherited = rig.dir.join("replaced-birth-data");
        std::fs::create_dir_all(&inherited).unwrap();
        let mut record = rdm_node::nodes_storage::fixture_record(&node, "node", replaced.node_id.clone(), replaced.incarnation.clone(), NodeStatus::Deleted);
        record.storage.data_dir = Some(inherited.display().to_string());
        rig.runner.nodes_storage.put_node(&record).await.unwrap();
        // The replacement's own birth, launched over that dir and lost before its record: its runtime exited.
        let node_id = NodeId::mint();
        let (_, left) = lost_before_its_record(&rig, &build_id, &node, &node_id, inherited.clone()).await;
        let runner = &rig.runner;
        runner.provider.terminate(&left, crate::deployment::provider::TerminationMode::Immediate, crate::deployment::deadline::Deadline::step()).await.unwrap();
        let persistent = rdm_core::meta::StoragePersistence::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve };

        let created = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), Some((&replaced, Some(&persistent)))).await;
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        let prepared: Vec<_> = capture.named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| attr(s, "step") == "PrepareStorage" && attr(s, "node_name") == "4").collect();
        assert!(!prepared.is_empty(), "the birth over the lost one is prepared: {created:?}");
        assert_eq!((attr(&prepared[0], "data_dir"), attr(&prepared[0], "inherited").as_str()), (inherited.display().to_string(), "true"), "the new birth takes the inherited dir: {:?}", prepared[0].attributes);
        let resolved: Vec<_> = capture.named("rdm.node_admin.node.resolve.via-unrecorded-launch").into_iter().filter(|s| attr(s, "node_id") == node_id.to_string()).collect();
        assert!(resolved.iter().any(|s| attr(s, "operation") == "node.delete" && attr(s, "found") == "exited" && attr(s, "data_dir") == inherited.display().to_string()), "the delete finds the launch exited in the inherited dir: {created:?} {resolved:?}");
        for s in resolved.iter().chain(prepared.iter()) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (rdm#334 A; Q356; node-recovery.md "Delete blocks the old instance before the new join"): a
    /// Build's create of mesh1.node.4 minted a new node.id over its own birth lost before Ready
    /// (`via-birth-lost-before-ready`), and that minted birth too joined and was lost before it wrote its
    /// record, launched over the lost birth's data dir. The create, run again, finds the minted birth
    /// unreached at the path, and its node.delete takes the minted birth's runtime from the launch its
    /// create made (`node.create:<minted id>`), found in the dir it inherited, and terminates it. Must NOT
    /// happen: the delete ending "lifecycle record absent" because the birth is not the create's own
    /// node.id, or the minted birth's runtime left running (rdm#374, finding 9 of its first review).
    #[tokio::test]
    async fn a_resubmitted_create_deletes_the_birth_it_minted_over_its_lost_one_through_the_runtime_that_launch_left() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        let capture = crate::span_capture::Capture::new("unrecorded-minted-birth");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-minted-birth").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let node_id = NodeId::mint();
        let minted = NodeId::mint();
        let inherited = crate::deployment::node_data_dir(&rig.dir, &node_id);
        let (_, left) = lost_before_its_record(&rig, &build_id, &node, &minted, inherited.clone()).await;
        let runner = &rig.runner;

        let created = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), None).await;
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        let resolved: Vec<_> = capture.named("rdm.node_admin.node.resolve.via-unrecorded-launch").into_iter().filter(|s| attr(s, "node_id") == minted.to_string()).collect();
        assert!(!resolved.is_empty(), "the minted birth's runtime is resolved from its launch: {created:?}");
        assert_eq!((attr(&resolved[0], "operation").as_str(), attr(&resolved[0], "found").as_str(), attr(&resolved[0], "data_dir")), ("node.delete", "running", inherited.display().to_string()), "{:?}", resolved[0].attributes);
        assert!(matches!(runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the minted birth's runtime is terminated: {created:?}");
        assert!(runner.nodes_storage.node(&minted).await.unwrap().is_none(), "nothing is written for a birth that wrote no record");
        for s in &resolved {
            eprintln!("span rdm.node_admin.node.resolve.via-unrecorded-launch trace_id={} {:?}", s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (Q217: a deleted node.id is never created or admitted again; node-investigation.md: a birth
    /// is re-created at its path and name "with a new node.id"; rdm#334 A; Q356): a Build's create of
    /// mesh1.node.4 whose own birth joined and was lost before it wrote its record is run again (the
    /// resubmit deletes it and mints a new node.id over it), and the minted birth is lost before its record
    /// too. (a) The create, run a third time, deletes the minted birth through its launch and the path takes
    /// another new node.id, minted over the minted birth. (b) Run once more, with no birth holding the path,
    /// the create resumes the newest node.id its Build holds (deployment-provider.md §3: "a resubmitted create
    /// resumes the newest one its Build holds, never minting while that launch is unresolved"). Must NOT
    /// happen: a create of the create's own node.id, deleted by its second run (rdm#374, its second review:
    /// `create()` ran `create_over` of the Build's node.id after the minted birth's loss, and its plain create
    /// of it once no birth held the path), nor of the minted birth deleted by its third.
    #[tokio::test]
    async fn a_resubmitted_create_whose_own_and_minted_births_were_both_lost_never_creates_its_deleted_node_id_again() {
        use rdm_gossip::primary::RoleTaken;
        let capture = crate::span_capture::Capture::new("unrecorded-double-loss");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-double-loss").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let node_id = NodeId::mint();
        let operation = rdm_core::OperationId::node("node.create", &node_id);
        lost_before_its_record(&rig, &build_id, &node, &node_id, crate::deployment::node_data_dir(&rig.dir, &node_id)).await;
        let runner = &rig.runner;
        // This admin is mesh1's mesh-primary, so each create runs to its launch.
        let me = rdm_core::Primary { mesh: "mesh1".into(), node_id: runner.node_id.clone(), incarnation: runner.primary_update.incarnation.clone(), revision: 1 };
        assert!(matches!(runner.digest_view.primaries.take(rdm_core::PrimaryRole::MeshPrimary, &me), RoleTaken::Held { .. }));
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        // Every node.id minted for a birth of this create lost before Ready, in order: the first over the
        // Build's own node.id, each later one over the birth minted before it.
        let minted = || capture.named("rdm.node_admin.node.create.via-birth-lost-before-ready").into_iter().map(|s| (attr(&s, "node_id"), attr(&s, "new_node_id"))).collect::<Vec<_>>();
        let binding = sleep_binding();
        let create = || create_at(&rig, &build_id, &node, &node_id, &binding, None);
        let launch_record = || std::fs::read_to_string(crate::deployment::node_data_dir(&rig.dir, &node_id).join(crate::deployment::process::DEPLOYMENT_FILE)).unwrap_or_default();
        let record_of = |id: &NodeId| std::fs::read_to_string(crate::deployment::node_data_dir(&rig.dir, id).join(crate::deployment::process::DEPLOYMENT_FILE)).unwrap_or_default();

        // The second run: the own birth deleted, a new node.id minted over it; that birth's runtime exits at once.
        let second = create().await;
        let first_minted = minted();
        assert_eq!(first_minted.len(), 1, "the path takes a new node.id over the create's own lost birth: {second:?} {first_minted:?}");
        assert_eq!(first_minted[0].0, node_id.to_string(), "{first_minted:?}");
        let m = NodeId::parse(&first_minted[0].1).unwrap();
        // The minted birth joined and was lost before its record: the view holds this admin, mesh1's
        // primary, and the minted birth dead; the own birth is gone from it (each run publishes the view
        // again from its digests, which this rig feeds none).
        let (m_incarnation, _) = crate::deployment::pipeline::launch_of(&rdm_core::OperationId::node("node.create", &m).to_string(), &m);
        let admin = || {
            let mut me = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
            me.node_id = runner.node_id.clone();
            me.incarnation_id = Some(runner.primary_update.incarnation.clone());
            me.status = NodeStatus::Ready;
            me.is_primary = true;
            me
        };
        let mut birth = Node::allocated(node.clone(), "mesh1", "node");
        birth.node_id = m.clone();
        birth.incarnation_id = Some(m_incarnation);
        birth.status = NodeStatus::Dead;
        runner.topology.write().await.nodes = vec![admin(), birth];
        let own_launch = launch_record();

        // (a) The third run: the minted birth deleted through its launch, another new node.id.
        let third = create().await;
        let after_third = minted();
        assert_eq!(after_third.len(), 2, "the minted birth's loss takes the path to another new node.id: {third:?} {after_third:?}");
        assert_eq!(after_third[1].0, m.to_string(), "minted over the minted birth: {after_third:?}");
        assert!(after_third[1].1 != node_id.to_string() && after_third[1].1 != m.to_string(), "{after_third:?}");
        let m2 = NodeId::parse(&after_third[1].1).unwrap();
        let m_launch = record_of(&m);
        let m_launch_record = || record_of(&m);
        let resolved: Vec<_> = capture.named("rdm.node_admin.node.resolve.via-unrecorded-launch").into_iter().filter(|s| attr(s, "node_id") == m.to_string() && attr(s, "operation") == "node.delete").collect();
        assert!(!resolved.is_empty(), "the minted birth is deleted through its launch: {third:?}");
        assert_eq!(launch_record(), own_launch, "the create's own node.id is never launched again");

        // (b) The fourth run: no birth holds the path (the third run's birth never joined).
        runner.topology.write().await.nodes = vec![admin()];
        let fourth = create().await;
        let resumed: Vec<_> = capture.named("rdm.node_admin.node.create.via-minted-birth").into_iter().map(|s| attr(&s, "resumed_node_id")).collect();
        assert_eq!(resumed.last(), Some(&m2.to_string()), "the create resumes the newest node.id its Build holds: {fourth:?} {resumed:?}");
        assert_eq!(minted(), after_third, "nothing is minted while the newest node.id's launch is unresolved: {fourth:?}");
        assert_eq!(launch_record(), own_launch, "the create's own node.id is never launched again");
        assert_eq!(m_launch_record(), m_launch, "the minted birth its third run deleted is never launched again");
        for s in capture.named("rdm.node_admin.node.create.via-birth-lost-before-ready").iter().chain(resolved.iter()) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// A Build topology of mesh1 that names, for each `(node, node_id, persistence, replaced)`, the create
    /// of `node_id` at `node` (D58): an entry of type `node` running `/bin/sleep` ([`sleep_binding`]), and the
    /// birth `replaced` its node's executable replacement replaces there (build-deployment.md §3).
    fn naming_creates(rig: &Rig, creates: &[(rdm_core::PathName, NodeId, rdm_core::meta::StoragePersistence, Option<crate::build::PinnedBirth>)]) -> FabricTopology {
        let mut t = FabricTopology::root("fab", "mesh1", rig.mesh1_id.clone());
        for (node, node_id, persistence, replaced) in creates {
            let m = t.meshes.get_mut("mesh1").unwrap();
            m.nodes.insert(node.clone());
            m.spec_by_path.insert(node.clone(), rdm_core::meta::NodeSpec { path: node.path.clone(), name: node.name.clone(), mesh: "mesh1".into(), r#type: "node".into(), storage_persistence: persistence.clone(), executable: Some(sleep_binding()), tags: Default::default(), config: Default::default() });
            t.created.push((node.clone(), node_id.clone()));
            if let Some(pin) = replaced {
                t.pinned.push((node.clone(), pin.clone()));
            }
        }
        t
    }

    /// The rig's admin as mesh1's mesh-primary, in its own view and in the primaries it holds, so a create
    /// runs to its launch.
    fn mesh_primary_in_view(runner: &AdminRunner) -> Node {
        use rdm_gossip::primary::RoleTaken;
        let me = rdm_core::Primary { mesh: "mesh1".into(), node_id: runner.node_id.clone(), incarnation: runner.primary_update.incarnation.clone(), revision: 1 };
        assert!(matches!(runner.digest_view.primaries.take(rdm_core::PrimaryRole::MeshPrimary, &me), RoleTaken::Held { .. }));
        let mut admin = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        admin.node_id = runner.node_id.clone();
        admin.incarnation_id = Some(runner.primary_update.incarnation.clone());
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin
    }

    /// The launch `node.create:<node_id>` makes of `node_id` at `node`, running `/bin/sleep 30` on `data_dir`
    /// and left running (its record there names the launch, as DeployRuntime wrote it): its handle.
    async fn left_running(rig: &Rig, node: &rdm_core::PathName, node_id: &NodeId, data_dir: std::path::PathBuf) -> crate::deployment::provider::DeploymentHandle {
        use crate::deployment::provider::{DeploymentProvider as _, ResolvedNodeLaunch};
        let (_, deployment_id) = crate::deployment::pipeline::launch_of(&rdm_core::OperationId::node("node.create", node_id).to_string(), node_id);
        rig.runner.provider.spawn(&ResolvedNodeLaunch { args: vec!["30".into()], ..ResolvedNodeLaunch::for_find("mesh1", "node", node, &deployment_id, "/bin/sleep".into(), data_dir) }, crate::deployment::deadline::Deadline::step()).await.unwrap()
    }

    /// CONTRACT (deployment-provider.md §3: AllocateIdentity "mint the node.id: its step event's subject is
    /// the minted id, and a resubmitted create resumes the newest one its Build holds, never minting while
    /// that launch is unresolved"; DeployRuntime "find and adopt a launch a lost executor left running";
    /// node-recovery.md step 2: no runtime of a superseded birth stays on the host; rdm#334 A): a Build's
    /// create of mesh1.node.4 whose own birth was lost before its record is run again: its birth is deleted
    /// and the path takes a minted node.id M, whose launch the executor is lost beside: it still runs, and
    /// M never joined, so no view holds it. Run again, the create resumes M from its Build, its
    /// DeployRuntime's find stops M's launch (another executor's, never joined) and the path takes the next
    /// node.id, minted over M. Must NOT happen: a fresh node.id minted from the Build's own one, with M's
    /// runtime left running on the host (rdm#374, finding 1 of its second review).
    #[tokio::test]
    async fn a_resubmitted_create_resumes_its_minted_node_id_and_stops_the_launch_a_lost_executor_left_running() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        let capture = crate::span_capture::Capture::new("minted-left-running");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("minted-left-running").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let node_id = NodeId::mint();
        lost_before_its_record(&rig, &build_id, &node, &node_id, crate::deployment::node_data_dir(&rig.dir, &node_id)).await;
        let runner = &rig.runner;
        let admin = mesh_primary_in_view(runner);
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        let minted_over = |id: &NodeId| capture.named("rdm.node_admin.node.create.via-birth-lost-before-ready").into_iter().filter(|s| attr(s, "node_id") == id.to_string()).map(|s| attr(&s, "new_node_id")).collect::<Vec<_>>();

        let first = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), None).await;
        let over_own = minted_over(&node_id);
        assert_eq!(over_own.len(), 1, "the path takes a node.id over the create's own lost birth: {first:?} {over_own:?}");
        let m = NodeId::parse(&over_own[0]).unwrap();
        // The executor is lost beside M's launch: it still runs on M's data dir, no view holds M, and the
        // steps of M's create are that executor's (its birth, never this one's).
        let left = left_running(&rig, &node, &m, crate::deployment::node_data_dir(&rig.dir, &m)).await;
        // What that executor's PrepareStorage left there: the birth's endpoint key.
        crate::admin::load_or_mint_key(&crate::deployment::node_data_dir(&rig.dir, &m)).await.unwrap();
        let inner: Arc<dyn crate::build_state::BuildStateAdapter> = runner.local.clone();
        let lost_executor = crate::build_run::FramedBuilds::stepping(inner, &FabricId::mint(), &NodeId::mint(), "mesh1");
        for step in ["AllocateIdentity", "PrepareStorage", "PrepareNetwork", "DeployRuntime"] {
            let w = crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, subject: None, operation: rdm_core::OperationId::node("node.create", &m), step: step.into(), outcome: StepOutcome::Complete };
            crate::build_state::BuildStateAdapter::write_step(&lost_executor, &w).await.unwrap();
        }
        runner.topology.write().await.nodes = vec![admin.clone()];

        let second = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), None).await;
        assert!(matches!(runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "M's launch, which the lost executor left running, is stopped: {second:?}");
        let (_, m_deployment) = crate::deployment::pipeline::launch_of(&rdm_core::OperationId::node("node.create", &m).to_string(), &m);
        let stopped: Vec<_> = capture.named("rdm.node_admin.deployment.reject.via-lost-executor").into_iter().filter(|s| attr(s, "deployment_id") == m_deployment.to_string()).collect();
        assert!(!stopped.is_empty(), "the resumed create's DeployRuntime stops M's launch: {second:?}");
        assert_eq!(minted_over(&node_id).len(), 1, "nothing is minted over the Build's own node.id again: {second:?}");
        let over_m = minted_over(&m);
        assert_eq!(over_m.len(), 1, "the path takes the next node.id, minted over M: {second:?} {over_m:?}");
        let held = runner.builds.read_build(&build_id).await.unwrap();
        assert_eq!(held.created_chain(&node_id), vec![node_id.clone(), m.clone(), NodeId::parse(&over_m[0]).unwrap()], "the Build holds each minted node.id as its AllocateIdentity's subject");
        for s in capture.named("rdm.node_admin.node.create.via-birth-lost-before-ready").iter().chain(stopped.iter()) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// What a create's target act holds: whether the work turn of the birth it acts on is held then.
    struct TurnSeam {
        runner: std::sync::OnceLock<std::sync::Weak<AdminRunner>>,
        held: std::sync::Mutex<Vec<(NodeId, bool)>>,
    }

    #[async_trait::async_trait]
    impl crate::wiring::TargetSeam for TurnSeam {
        async fn before_target_act(&self, _path: &rdm_core::PathName, node_id: &NodeId) {
            let held = self.runner.get().and_then(std::sync::Weak::upgrade).is_some_and(|r| r.try_node_work(node_id).is_none());
            self.held.lock().unwrap().push((node_id.clone(), held));
        }
    }

    /// CONTRACT (node-recovery.md "Execution": work on one node is serialized at its mesh-primary): a
    /// Build's create of mesh1.node.4 finds a birth of another node.id unreached at the path, lost before
    /// it wrote its record, and deletes it: the create holds that birth's work turn when it acts on it, and
    /// releases it once the create ends. Must NOT happen: the delete of another node's birth under the
    /// create's own turn alone, beside a node command or a recovery of that birth (rdm#374, finding 2 of its
    /// second review).
    #[tokio::test]
    async fn a_create_holds_the_work_turn_of_the_other_birth_it_deletes_at_its_path() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        let seam = Arc::new(TurnSeam { runner: Default::default(), held: Default::default() });
        let rig = rig_seamed("create-other-birth-turn", Some(seam.clone())).await;
        let _ = seam.runner.set(Arc::downgrade(&rig.runner));
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let (node_id, other) = (NodeId::mint(), NodeId::mint());
        let (_, left) = lost_before_its_record(&rig, &build_id, &node, &other, crate::deployment::node_data_dir(&rig.dir, &other)).await;
        let _ = mesh_primary_in_view(&rig.runner);

        let created = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), None).await;
        assert_eq!(seam.held.lock().unwrap().clone(), vec![(other.clone(), true)], "the create holds {other}'s work turn when it acts on it: {created:?}");
        assert!(rig.runner.try_node_work(&other).is_some(), "the turn is released once the create ends: {created:?}");
        assert!(matches!(rig.runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the other birth's runtime is terminated: {created:?}");
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (node-recovery.md steps 2-3: node.delete of the unheard instance, then node.create at its
    /// path and name with a new node.id; rdm#334 A; Q356): node recovery of mesh1.node.4, a birth its Build
    /// created that joined and was lost before it wrote its record: its delete resolves the runtime its
    /// launch left and terminates it, and its create runs over the birth that delete resolved, launching the
    /// executable its Build's create named. Must NOT happen: the create ending "lifecycle record absent"
    /// once the delete has taken the birth out of the view (rdm#374, finding 3 of its second review).
    #[tokio::test]
    async fn node_recovery_of_a_birth_that_wrote_no_record_re_creates_it_over_the_birth_its_delete_resolved() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        let capture = crate::span_capture::Capture::new("unrecorded-recovery");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-recovery").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let node_id = NodeId::mint();
        let topology = naming_creates(&rig, &[(node.clone(), node_id.clone(), rdm_core::meta::StoragePersistence::Ephemeral, None)]);
        let (incarnation, left) = lost_before_its_record_in(&rig, &build_id, topology, &node, &node_id, crate::deployment::node_data_dir(&rig.dir, &node_id)).await;
        let _ = mesh_primary_in_view(&rig.runner);

        let recovered = rig.runner.recover(&node, &crate::build::ExitedBirth { node_id: node_id.clone(), incarnation }, false).await;
        assert!(!format!("{recovered:?}").contains("lifecycle record absent"), "the create runs over the birth its delete resolved: {recovered:?}");
        assert!(matches!(rig.runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the runtime the lost birth's launch left is terminated: {recovered:?}");
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        // The create runs its steps over that birth (this rig feeds no digests, so the view the delete
        // published again no longer holds this admin, and the new birth's name check is refused here).
        let launched: Vec<_> = capture.named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| attr(s, "step") == "PrepareStorage" && attr(s, "node_name") == "4").collect();
        assert!(!launched.is_empty(), "the recovery's create runs its steps over the birth its delete resolved: {recovered:?}");
        let created: Vec<_> = capture.named("rdm.node_admin.node.create.via-node-recovery").into_iter().filter(|s| attr(s, "node_id") == node_id.to_string() && !attr(s, "created_node_id").is_empty()).collect();
        assert!(created.iter().all(|s| attr(s, "created_node_id") != node_id.to_string()) && !created.is_empty(), "a new node.id: {created:?}");
        for s in created.iter().chain(launched.iter()) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (fabric-recovery.md step 4, Q160: a delete of a birth whose runtime is not resolved fails its
    /// TerminateRuntime by name and manufactures no receipt; mesh-delete.md "Resume after the primary's
    /// loss"): the fabric-primary's mesh.delete of a birth of the leaving mesh that wrote no record and
    /// whose incarnation no create launched: no runtime is resolved, and the delete ends naming its
    /// TerminateRuntime and the resolution's own error, on its `via-unresolved-runtime` span. Must NOT
    /// happen: the delete ending "lifecycle record absent", the resolution's error replaced (rdm#374,
    /// finding 4 of its second review).
    #[tokio::test]
    async fn a_mesh_delete_of_a_birth_whose_runtime_is_not_resolved_fails_its_terminate_by_name_with_the_resolutions_error() {
        let capture = crate::span_capture::Capture::new("unresolved-runtime-delete");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unresolved-runtime-delete").await;
        let build_id = crate::build::BuildId::mint();
        rig.steps
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: FabricTopology::root("fab", "mesh1", rig.mesh1_id.clone()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1000 })
            .await
            .unwrap();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let admin = mesh_primary_in_view(&rig.runner);
        let mut birth = Node::allocated(node.clone(), "mesh1", "node");
        birth.node_id = node_id.clone();
        birth.incarnation_id = Some(incarnation.clone());
        birth.status = NodeStatus::Dead;
        rig.runner.topology.write().await.nodes = vec![admin, birth];
        let roster = RosterBirth { mesh: "mesh1".into(), r#type: "node".into(), node: node.clone(), node_id: node_id.to_string(), incarnation_id: incarnation.as_str().to_string() };

        let ended = format!("{:?}", rig.runner.delete_lost_birth(&build_id, &roster).await.expect_err("no runtime is resolved"));
        assert!(ended.contains("Q160") && ended.contains("the exited birth's runtime is not resolved"), "the TerminateRuntime fails by name, with the resolution's error: {ended}");
        assert!(!ended.contains("lifecycle record absent"), "{ended}");
        let unresolved = capture.named("rdm.node_admin.node.delete.via-unresolved-runtime");
        assert!(unresolved.iter().any(|s| s.attributes.get("incarnation_id").is_some_and(|i| i == incarnation.as_str())), "its span names it: {unresolved:?}");
        for s in &unresolved {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (build-deployment.md §3, Q378: "The new node takes the held birth's data dir when ...
    /// persistent and the directory exists", with no exited condition): a node's executable replacement of
    /// mesh1.node.4, its entry persistent, created its birth over the replaced birth's data dir; that birth
    /// joined, was lost before it wrote its record, and its runtime still runs. The create, run again,
    /// terminates it through the launch found running in that inherited dir, and the birth it creates over
    /// it takes the same dir. Must NOT happen: the new birth on an empty `<data_root>/<new id>` because the
    /// lost birth was terminated running, not read exited (rdm#374, finding 5 of its second review).
    #[tokio::test]
    async fn a_replacement_whose_birth_was_lost_running_before_its_record_carries_the_inherited_data_dir_into_the_birth_over_it() {
        use crate::deployment::provider::{DeploymentProvider as _, DeploymentStatus};
        use crate::storage::NodesStorage;
        let capture = crate::span_capture::Capture::new("unrecorded-replacement-running");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let rig = rig("unrecorded-replacement-running").await;
        let build_id = crate::build::BuildId::mint();
        let node = rdm_core::PathName::new("mesh1.node", "4").unwrap();
        let replaced = crate::build::PinnedBirth { r#type: "node".into(), mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let inherited = rig.dir.join("replaced-birth-data");
        std::fs::create_dir_all(&inherited).unwrap();
        let mut record = rdm_node::nodes_storage::fixture_record(&node, "node", replaced.node_id.clone(), replaced.incarnation.clone(), NodeStatus::Deleted);
        record.storage.data_dir = Some(inherited.display().to_string());
        rig.runner.nodes_storage.put_node(&record).await.unwrap();
        let node_id = NodeId::mint();
        let (_, left) = lost_before_its_record(&rig, &build_id, &node, &node_id, inherited.clone()).await;
        let persistent = rdm_core::meta::StoragePersistence::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve };

        let created = create_at(&rig, &build_id, &node, &node_id, &sleep_binding(), Some((&replaced, Some(&persistent)))).await;
        assert!(matches!(rig.runner.provider.inspect(&left, crate::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the lost birth's runtime is terminated: {created:?}");
        let attr = |s: &crate::span_capture::Captured, k: &str| s.attributes.get(k).cloned().unwrap_or_default();
        let resolved: Vec<_> = capture.named("rdm.node_admin.node.resolve.via-unrecorded-launch").into_iter().filter(|s| attr(s, "node_id") == node_id.to_string()).collect();
        assert!(resolved.iter().any(|s| attr(s, "operation") == "node.delete" && attr(s, "found") == "running" && attr(s, "data_dir") == inherited.display().to_string()), "the delete finds the launch running in the inherited dir: {created:?} {resolved:?}");
        let prepared: Vec<_> = capture.named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| attr(s, "step") == "PrepareStorage" && attr(s, "node_name") == "4" && !attr(s, "data_dir").is_empty()).collect();
        assert!(!prepared.is_empty(), "the birth over the lost one is prepared: {created:?}");
        assert_eq!((attr(&prepared[0], "data_dir"), attr(&prepared[0], "inherited").as_str()), (inherited.display().to_string(), "true"), "the new birth takes the inherited dir: {:?}", prepared[0].attributes);
        for s in resolved.iter().chain(prepared.iter()) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }

    /// CONTRACT (fabric-delete.md "Failure": fabric.delete deletes everything; rdm#334 A, the atlas's in-flight
    /// creates; deployment-provider.md §3 AllocateIdentity; Q145: terminate removes what a runtime left):
    /// under a fabric.delete in force, the roster's in-flight creates of a Build that wrote no record are
    /// read from the Build: (a) mesh1.node.1, whose own birth was lost before Ready and whose minted birth
    /// M1's launch runs: M1, the newest node.id the Build holds; (b) mesh1.node.2, whose launch ran and
    /// exited, its exact runtime named: a roster birth, so its delete's terminate removes what it left;
    /// (c) mesh1.node.3, a node's executable replacement that took the replaced birth's data dir, its launch
    /// running there; (d) mesh1.node.5, a plain launch that runs. No handle of any is held before its
    /// delete. Must NOT happen: the Build's own node.id read in place of the minted one, an exited launch
    /// left out, a launch in the inherited dir not looked for, or a handle cached before a delete that may
    /// fail (rdm#374, finding 6 of its second review).
    #[tokio::test]
    async fn the_in_flight_creates_of_a_fabric_delete_read_the_minted_birth_the_exited_launch_and_the_inherited_dir_and_hold_no_handle() {
        use crate::deployment::provider::{DeploymentProvider as _, TerminationMode};
        let rig = rig("in-flight-creates").await;
        let at = |name: &str| rdm_core::PathName::new("mesh1.node", name).unwrap();
        let (n1, m1, n2, n3, n5) = (NodeId::mint(), NodeId::mint(), NodeId::mint(), NodeId::mint(), NodeId::mint());
        let ephemeral = rdm_core::meta::StoragePersistence::Ephemeral;
        let persistent = rdm_core::meta::StoragePersistence::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve };
        let replaced = crate::build::PinnedBirth { r#type: "node".into(), mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let inherited = rig.dir.join("replaced-birth-data");
        std::fs::create_dir_all(&inherited).unwrap();
        {
            use crate::storage::NodesStorage;
            let mut record = rdm_node::nodes_storage::fixture_record(&at("3"), "node", replaced.node_id.clone(), replaced.incarnation.clone(), NodeStatus::Deleted);
            record.storage.data_dir = Some(inherited.display().to_string());
            rig.runner.nodes_storage.put_node(&record).await.unwrap();
        }
        let build_id = crate::build::BuildId::mint();
        let topology = naming_creates(&rig, &[(at("1"), n1.clone(), ephemeral.clone(), None), (at("2"), n2.clone(), ephemeral.clone(), None), (at("3"), n3.clone(), persistent, Some(replaced)), (at("5"), n5.clone(), ephemeral, None)]);
        rig.steps.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1000 }).await.unwrap();
        let allocated = |id: &NodeId, subject: Option<NodeId>| crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, subject, operation: rdm_core::OperationId::node("node.create", id), step: "AllocateIdentity".into(), outcome: StepOutcome::Complete };
        for (id, subject) in [(&n1, None), (&n1, Some(m1.clone())), (&m1, None), (&n2, None), (&n3, None), (&n5, None)] {
            rig.steps.write_step(&allocated(id, subject)).await.unwrap();
        }
        let running_m1 = left_running(&rig, &at("1"), &m1, crate::deployment::node_data_dir(&rig.dir, &m1)).await;
        let exited_n2 = left_running(&rig, &at("2"), &n2, crate::deployment::node_data_dir(&rig.dir, &n2)).await;
        rig.runner.provider.terminate(&exited_n2, TerminationMode::Immediate, crate::deployment::deadline::Deadline::step()).await.unwrap();
        let running_n3 = left_running(&rig, &at("3"), &n3, inherited.clone()).await;
        let running_n5 = left_running(&rig, &at("5"), &n5, crate::deployment::node_data_dir(&rig.dir, &n5)).await;
        let delete = crate::build::BuildId::mint();
        rig.steps.publish_accepted(&BuildAccepted { build_id: delete, topology: FabricTopology::root("fab", "mesh1", rig.mesh1_id.clone()), change: rdm_core::build::BuildChange::DeleteFabric, source: None, time: 2000 }).await.unwrap();

        let roster = rig.runner.in_flight_creates("mesh1", &[]).await.unwrap();
        let named: std::collections::BTreeSet<String> = roster.iter().map(|b| b.node_id.clone()).collect();
        assert_eq!(named, [m1.to_string(), n2.to_string(), n3.to_string(), n5.to_string()].into_iter().collect(), "the minted birth, the exited launch, the launch in the inherited dir and the plain one: {roster:?}");
        let handles = rig.runner.handles.lock().unwrap().clone();
        assert!([&n1, &m1, &n2, &n3, &n5].iter().all(|id| !handles.contains_key(*id)), "no handle is held before a delete: {:?}", handles.keys().collect::<Vec<_>>());
        for h in [&running_m1, &running_n3, &running_n5] {
            let _ = rig.runner.provider.terminate(h, TerminationMode::Immediate, crate::deployment::deadline::Deadline::step()).await;
        }
        rig.ep.close().await;
        let _ = std::fs::remove_dir_all(&rig.dir);
    }
}
