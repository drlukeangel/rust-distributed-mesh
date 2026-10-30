//! Who decides a Build attempt's claim, and the context the attempt carries (R-A1, R-T4, R-X1,
//! R-BI7).
//!
//! An executor never claims. The claim of an attempt is decided by its claimant (R-BI7 caveat 2):
//! the fabric-primary for a topology Build's attempt (the fabric lane), decided by
//! [`ClaimDoor::claim`] in process by its drive (`build_drive`), which then calls the executor
//! with `build.attempt.run` carrying the won claim; the mesh-primary of a mesh for that mesh's drift
//! attempt (its lane), decided in process when it opens the attempt (`http::ControlPlane::
//! open_recovery`). Both go through [`decide_claim`]: the mesh the attempt acts in is taken first by
//! its in-flight row on the store root (`crate::mesh_claims`, one attempt in flight per mesh), which names
//! its claimant: the store decides the claim, and the same attempt another claimant took is held, never
//! won (build.md §5.2, Q142); the claimant's Build log then records what the store decided. The
//! door is also served on `BuildClaim` (op `0x1C`) for a caller that puts a claim over Node RPC; the
//! product has no such caller. An admin whose view names another birth fabric-primary decides no
//! fabric-lane claim; one whose view names another birth its mesh-primary decides no claim of that
//! mesh's drift lane.
//!
//! Context is attempt-scoped. The fabric-primary keeps one [`CallContext`] per `(build_id,
//! attempt)` on its Build log ([`AttemptContexts`]; the file backend keeps it in its own data dir,
//! `attempt-context/`), written when the attempt comes
//! into being (the REST request span, or the `via-proven-drift` span) and returned in `Won`. It is
//! never a Build fact: Build facts are re-broadcast to every new neighbour. A next attempt that
//! has no context of its own inherits the previous attempt's at claim time.

use crate::build::BuildId;
use crate::mesh_claims::InFlight;
use crate::storage::MeshStorage;
use crate::build_state::{BuildAttemptClaim, BuildStateAdapter, ClaimOutcome, InFlightClaim, LocalBuildLog};
use crate::model::{EndpointId, NodeId, PathName};
use crate::storage_error::StorageError;
use crate::topology::Topology;
use rdm_transport::ServerBuilder;
use rdm_core::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
use rdm_core::catalog::OpOwner;
use rdm_core::context::CallContext;
use std::sync::{Arc, OnceLock};
use tracing::Instrument as _;

/// The caller system every RDM-originated attempt context names.
pub const CALLER_SYSTEM: &str = "rdm";

/// The context of the current span: where an attempt created now comes from.
pub fn current_context() -> CallContext {
    CallContext {
        caller_system: Some(CALLER_SYSTEM.into()),
        traceparent: rdm_transport::current_traceparent(),
        tracestate: rdm_transport::current_tracestate(),
        baggage: None,
    }
}

/// Each attempt's context, keyed `(build_id, lane, attempt)`, on the admin's Build log
/// ([`LocalBuildLog::put_attempt_context`]): the context an attempt's claim took. The opening
/// request's context rides the open's own record in the journal ([`AttemptOpened::context`]); no
/// Build message carries either.
///
/// [`AttemptOpened::context`]: crate::build_state::AttemptOpened::context
pub struct AttemptContexts {
    log: Arc<dyn LocalBuildLog>,
}

impl AttemptContexts {
    /// The contexts `log` keeps.
    pub fn on(log: Arc<dyn LocalBuildLog>) -> Self {
        Self { log }
    }

    /// Contexts held in memory only: a fixture whose admin has no data dir.
    pub fn in_memory() -> Self {
        Self::on(Arc::new(crate::build_state::MemoryBuildStateAdapter::new()))
    }

    /// Put the context of the fabric lane's `(build_id, attempt)`: a blind put of its own key.
    pub async fn put(&self, build_id: &BuildId, attempt: u32, context: &CallContext) -> Result<(), StorageError> {
        self.put_lane(build_id, None, attempt, context).await
    }

    /// Put the context of `(build_id, lane, attempt)`: a blind put of its own key.
    pub async fn put_lane(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, context: &CallContext) -> Result<(), StorageError> {
        self.log.put_attempt_context(build_id, lane, attempt, context).await
    }

    /// The context of the fabric lane's `(build_id, attempt)`, when one was put.
    pub async fn get(&self, build_id: &BuildId, attempt: u32) -> Result<Option<CallContext>, StorageError> {
        self.get_lane(build_id, None, attempt).await
    }

    /// The context of `(build_id, lane, attempt)`, when one was put.
    pub async fn get_lane(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<Option<CallContext>, StorageError> {
        self.log.attempt_context(build_id, lane, attempt).await
    }

    /// The context `attempt` is claimed with: its own, else the previous attempt's (a hand-off
    /// continues the trace it came from), stored under `attempt` so a repeat claim answers the
    /// same. An attempt nothing here knows (the fabric-primary changed) starts a new trace.
    pub async fn for_claim(&self, build_id: &BuildId, attempt: u32) -> Result<CallContext, StorageError> {
        self.for_claim_lane(build_id, None, attempt).await
    }

    /// [`Self::for_claim`] of `(build_id, lane, attempt)`: inherited within the lane only.
    pub async fn for_claim_lane(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<CallContext, StorageError> {
        self.for_claim_opened(build_id, lane, attempt, None).await
    }

    /// The context `(build_id, lane, attempt)` is claimed with: the one its claim already took, else
    /// `opened`, the context its open was written with (one record with the open, so there is no
    /// moment after the open without it), else the previous attempt's in the lane; stored under
    /// `attempt` so a repeat claim answers the same.
    pub async fn for_claim_opened(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, opened: Option<CallContext>) -> Result<CallContext, StorageError> {
        if let Some(own) = self.get_lane(build_id, lane, attempt).await? {
            return Ok(own);
        }
        if let Some(opened) = opened {
            self.put_lane(build_id, lane, attempt, &opened).await?;
            return Ok(opened);
        }
        let inherited = match attempt.checked_sub(1).filter(|p| *p > 0) {
            Some(previous) => self.get_lane(build_id, lane, previous).await?,
            None => None,
        };
        let context = inherited.unwrap_or_else(|| CallContext { caller_system: Some(CALLER_SYSTEM.into()), ..CallContext::default() });
        self.put_lane(build_id, lane, attempt, &context).await?;
        Ok(context)
    }
}

/// Decide the claim `claim` (its Build, lane, attempt, executor and claimant), the attempt acting in
/// `mesh`, on this admin `me` (R-BI7 caveats 2, 3), under the one claim-decision span. The mesh's
/// in-flight row is taken first, naming the claim's lane key (Q35), in the same writer's turn as the
/// claim ([`BuildStateAdapter::claim_in_flight`]): another attempt holding it refuses
/// `BuildInProgress` naming it and its claimant, the same attempt another claimant took answers `Lost`
/// naming it (Q142), and nothing is written. Then the claim key `(build_id, lane, attempt)` is recorded
/// on `builds`, the record of what the store decided: `Won` carries the attempt's context. An attempt that is not open
/// releases the row taken for it. A claim `Lost` to an executor of the same mesh leaves the row, which
/// names the attempt, to its verdict; one `Lost` to an executor of another mesh releases the row it took
/// here, because that attempt's verdict releases only its executor's mesh, and the loser runs nothing
/// (build.md §5.2). Who may claim is the caller's rule
/// ([`ClaimDoor`]'s role check, the recovery door's recoverer), decided before this. `executor_birth`
/// is the claim's executor's exact node id with the node rows it is read on, when the caller knows it.
pub async fn decide_claim(me: &PathName, builds: &dyn BuildStateAdapter, claims: &dyn MeshStorage, contexts: &AttemptContexts, claim: &BuildAttemptClaim, mesh: &str, executor_birth: Option<(&dyn crate::storage::NodesStorage, &NodeId)>) -> BuildClaimReply {
    let lane = claim.mesh.as_deref();
    let span = tracing::info_span!(
        "rdm.node_admin.build.update.via-claim-decision",
        node = %me,
        build_id = %claim.build_id.log(),
        attempt = claim.attempt,
        executor = %claim.executor,
        mesh,
        lane = lane.unwrap_or("fabric"),
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
    );
    let reply = async {
        // A deleted birth claims nothing (node-investigation.md: "A node that comes back after it was
        // re-created is a deleted birth ... No admit reason exists for it"; Q181): the executor's exact
        // birth, read on the store's node rows before the in-flight row is taken; nothing is taken or
        // written.
        if let Some((rows, node_id)) = executor_birth {
            let executor = claim.executor.parse::<PathName>();
            let deleted = match &executor {
                Ok(name) => crate::node_rows::deleted_birth(rows, name, node_id).await,
                Err(e) => return rdm_core::reply::build_claim::not_ready(format!("{me}: the executor {:?} of attempt {} of {} is not a path.name: {e}", claim.executor, claim.attempt, claim.build_id)),
            };
            match deleted {
                Ok(None) => {}
                Ok(Some(why)) => {
                    tracing::info_span!("rdm.node_admin.build.reject.via-deleted-birth", node = %me, executor = %claim.executor, node_id = %node_id, build_id = %claim.build_id.log(), attempt = claim.attempt, reason = %why)
                        .in_scope(|| tracing::info!("the claim's executor is a deleted birth on the node rows: it claims nothing"));
                    return rdm_core::reply::build_claim::not_ready(why);
                }
                Err(e) => return rdm_core::reply::build_claim::not_ready(format!("{me}: the node row of the executor {} ({node_id}) could not be read: {e}", claim.executor)),
            }
        }
        // A Build a fabric.delete interrupts opens no further attempt (build.md §1): its claim is refused
        // naming the delete, the Build in progress, and nothing is taken or written.
        if let Ok(p) = builds.read_build(&claim.build_id).await {
            if let Ok(Some(delete)) = crate::build_state::interrupted_by(builds, &p).await {
                return rdm_core::reply::build_claim::build_in_progress(delete.build_id.to_string(), mesh.to_string(), None);
            }
        }
        let row = InFlight { build_id: claim.build_id.clone(), lane: claim.mesh.clone(), attempt: claim.attempt, claimant: claim.claimant.clone().unwrap_or_else(|| me.to_string()), claimant_node: None };
        let decided = match builds.claim_in_flight(claim, claims, mesh, &row).await {
            // The same attempt another claimant took on the store: this claim lost (build.md §5.2, Q142).
            Ok(InFlightClaim::Held(held)) if held.same_attempt(&row) => return rdm_core::reply::build_claim::lost(held.claimant),
            Ok(InFlightClaim::Held(held)) => return rdm_core::reply::build_claim::build_in_progress(held.build_id.to_string(), mesh.to_string(), Some(held.claimant)),
            Ok(InFlightClaim::Decided(outcome)) => Ok(outcome),
            Err(e) => Err(e),
        };
        match decided {
            Ok(ClaimOutcome::Won) => match async {
                // The context the attempt's open was written with, in the open's own record.
                let opened = crate::build_state::opened_context(builds, &claim.build_id, lane, claim.attempt).await.map_err(|e| e.to_string())?;
                contexts.for_claim_opened(&claim.build_id, lane, claim.attempt, opened).await.map_err(|e| e.to_string())
            }
            .await
            {
                Ok(context) => BuildClaimReply::Won { context },
                Err(e) => rdm_core::reply::build_claim::not_ready(format!("{me}: attempt {} of {} is claimed, its context record is unreadable: {e}", claim.attempt, claim.build_id)),
            },
            // The held attempt acts in its executor's mesh, and its verdict releases that mesh's row
            // alone: a row this claim took in another mesh holds that mesh for nothing.
            Ok(ClaimOutcome::Lost { holder }) => {
                if holder.parse::<PathName>().is_ok_and(|h| h.mesh != mesh) {
                    if let Err(e) = claims.release_in_flight(mesh, &row).await {
                        return rdm_core::reply::build_claim::not_ready(format!("{me}: attempt {} of {} is held by {holder} and the in-flight row of mesh {mesh} taken for this claim could not be released: {e}", claim.attempt, claim.build_id));
                    }
                }
                rdm_core::reply::build_claim::lost(holder)
            }
            Ok(ClaimOutcome::NotOpen { next }) => {
                if let Err(e) = claims.release_in_flight(mesh, &row).await {
                    return rdm_core::reply::build_claim::not_ready(format!("{me}: attempt {} of {} is not open (next {next:?}) and the in-flight row of mesh {mesh} taken for it could not be released: {e}", claim.attempt, claim.build_id));
                }
                rdm_core::reply::build_claim::not_open(next)
            }
            Err(e) => rdm_core::reply::build_claim::not_ready(format!("{me}: claiming attempt {} of {}: {e}", claim.attempt, claim.build_id)),
        }
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", reply.name());
    match &reply {
        BuildClaimReply::Lost { holder } => span.record("detail", holder.as_str()),
        BuildClaimReply::NotOpen { next } => span.record("detail", format!("next {next:?}").as_str()),
        BuildClaimReply::BuildInProgress { current_build_id, held_by, .. } => span.record("detail", format!("{current_build_id} held by {held_by:?}").as_str()),
        BuildClaimReply::NotReady { reason } => span.record("detail", reason.as_str()),
        _ => &span,
    };
    span.in_scope(|| tracing::info!("attempt claim decided by its claimant"));
    reply
}

/// The claim door: the one place a Build attempt's claim is decided by its claimant.
pub struct ClaimDoor {
    /// This admin's path.name.
    pub me: PathName,
    /// The node id this admin was born with: whether it holds the fabric-primary role is decided by it.
    pub node_id: NodeId,
    /// The admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// This admin's Build log: the fabric-primary's own, when it holds the role.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The observability contexts of the attempts it decided.
    pub contexts: Arc<AttemptContexts>,
    /// Each mesh's in-flight row: what a claim takes before it is decided.
    pub claims: Arc<dyn MeshStorage>,
    /// Woken at every claim this door wins: the fabric-primary's run of a reported drift attempt
    /// waits on it for its mesh-primary's claim (Q118).
    pub claimed: Arc<tokio::sync::Notify>,
}

impl ClaimDoor {
    /// The `path.name` this admin holds now: its Build adapter's held name when set, else its birth's
    /// name in the view, falling back to `self.me` only before its birth is in the view.
    pub async fn held_name(&self) -> PathName {
        if let Some(held) = self.builds.held_name() {
            return held;
        }
        self.topology.read().await.held_name_of(&self.me, Some(&self.node_id))
    }

    /// The reply of a receiver that may not claim in `lane` (R-BI7 caveat 2), decided from this
    /// admin's view of the roles, never from who called: the fabric lane is the fabric-primary's, a
    /// mesh's drift lane that mesh's primary's. `mesh` is the mesh the attempt acts in, when known.
    async fn not_claimant(&self, lane: Option<&str>, mesh: Option<&str>) -> Option<BuildClaimReply> {
        let t = self.topology.read().await;
        // The fabric-primary decides every claim, a mesh's drift attempt's included (its mesh-primary
        // claims through BuildClaim, Q118); a mesh-primary that is not the fabric-primary decides only
        // its own lane's claims it opens in process, which no report reaches.
        let claims = match lane {
            None => t.is_fabric_primary(&self.node_id),
            Some(lane) => {
                t.is_fabric_primary(&self.node_id) || t.is_mesh_primary(lane, &self.node_id)
            }
        };
        (!claims).then(|| rdm_core::reply::build_claim::not_claimant(mesh.map(str::to_string), t.fabric_primary().map(|n| n.name.to_string()), mesh.and_then(|m| t.node_type_primary(m, crate::model::NodeKind::NodeAdmin)).map(|n| n.name.to_string())))
    }

    /// Decide the claim of the fabric lane's `attempt` of `build_id` for `executor` (a node-admin's
    /// path.name): only the fabric-primary decides it, on the executor's mesh's row. `Won` carries
    /// the attempt's context.
    pub async fn claim(&self, executor: &str, build_id: &BuildId, attempt: u32) -> BuildClaimReply {
        self.claim_in(executor, build_id, None, attempt).await
    }

    /// Decide the claim of `attempt` of `build_id` in `lane` for `executor`: the claimant rule, then
    /// [`decide_claim`] on the mesh the attempt acts in (the lane's own mesh for a drift attempt,
    /// the executor's for a topology attempt).
    pub async fn claim_in(&self, executor: &str, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> BuildClaimReply {
        let me = self.held_name().await;
        let mesh = match lane {
            Some(mesh) => mesh.to_string(),
            None => match executor.parse::<PathName>() {
                Ok(e) => e.mesh,
                Err(e) => return rdm_core::reply::build_claim::not_ready(format!("{me}: the executor {executor:?} of attempt {attempt} of {build_id} is not a path.name: {e}")),
            },
        };
        if let Some(refusal) = self.not_claimant(lane, Some(&mesh)).await {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-claimant", node = %me, build_id = %build_id.log(), attempt, executor, mesh = %mesh, lane = lane.unwrap_or("fabric"), detail = ?refusal)
                .in_scope(|| tracing::info!("this admin may not claim in that lane in its view"));
            return refusal;
        }
        let claim = BuildAttemptClaim {
            claimant: Some(me.to_string()),
            mesh: lane.map(str::to_string),
            build_id: build_id.clone(),
            attempt,
            executor: executor.to_string(),
        };
        let reply = decide_claim(
            &me,
            &*self.builds,
            &*self.claims,
            &self.contexts,
            &claim,
            &mesh,
            None,
        )
        .await;
        if matches!(reply, BuildClaimReply::Won { .. }) {
            self.claimed.notify_waiters();
        }
        reply
    }

    /// A request off the wire: the role first (a receiver that may not claim in the request's lane
    /// only names who may), then the executor the request names, at its exact birth as this admin's view
    /// holds it. Nothing checks who is calling (node-rpc.md §14): `peer` names the call, the claim
    /// is decided for the executor named. A node or birth the view does not hold is refused by name
    /// about that subject, and nothing is claimed.
    pub async fn serve(&self, peer: EndpointId, req: BuildClaimRequest) -> BuildClaimReply {
        let BuildClaimRequest::ClaimAttempt { build_id, attempt, executor_node_id, executor_incarnation, mesh } = req;
        if let Some(refusal) = self.not_claimant(mesh.as_deref(), mesh.as_deref()).await {
            return refusal;
        }
        let me = self.held_name().await;
        let executor = self.topology.read().await.births().find(|n| n.node_id == executor_node_id).cloned();
        let Some(executor) = executor else {
            return rdm_core::reply::build_claim::peer_unresolved(format!("{me}: the request (from endpoint {}) names executor node {executor_node_id}, which is not in this fabric-primary's view", peer.0));
        };
        if executor.incarnation_id.as_ref() != Some(&executor_incarnation) {
            return rdm_core::reply::build_claim::peer_unresolved(format!("{me}: the request names birth {} of {} and this view holds {:?}", executor_incarnation.as_str(), executor.name, executor.incarnation_id.as_ref().map(|i| i.as_str())));
        }
        let build_id = match BuildId::parse(&build_id) {
            Ok(build_id) => build_id,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.build.reject.via-malformed-build-id", node = %me, executor = %executor.name, build_id = %build_id, reason = %e, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the claim is refused: its Build id is not a Crockford id"));
                return rdm_core::reply::build_claim::malformed(rdm_core::outcome::MalformedKind::Corrupt);
            }
        };
        self.claim_in(&executor.name.to_string(), &build_id, mesh.as_deref(), attempt).await
    }
}

/// The door a running admin fills once it holds a view.
pub type ClaimSlot = Arc<OnceLock<Arc<ClaimDoor>>>;

/// Serve `BuildClaim` on this admin. `slot` is filled once the admin holds a view; until then a
/// claim is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: ClaimSlot) -> ServerBuilder {
    b.serve::<BuildClaim, _, _>(OpOwner::Product("rdm".into()), move |peer: rdm_transport::PeerContext, req: BuildClaimRequest| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::build_claim::not_ready("this admin holds no view yet".into()));
            };
            Ok(door.serve(EndpointId(peer.endpoint_id.to_string()), req).await)
        }
    })
}
