//! Who decides a Build attempt's claim, and the context the attempt carries (R-A1, R-T4, R-X1,
//! R-BI7).
//!
//! An executor never claims. The claim of an attempt is decided by its claimant (R-BI7 caveat 2):
//! the fabric-primary for a topology Build's attempt (the fabric lane), decided by
//! [`ClaimDoor::claim`] in process by its drive (`build_drive`), which then calls the executor
//! with `build.attempt.run` carrying the won claim; the mesh-primary of a mesh for that mesh's drift
//! attempt (its lane), decided in process when it opens the attempt (`http::ControlPlane::
//! open_recovery`). Both go through [`decide_claim`]: the mesh the attempt acts in is taken first by
//! its in-flight row on the store root (`crate::mesh_claims`, one attempt in flight per mesh), then
//! the claim key `(build_id, mesh, attempt)` is an insert-and-fail on the claimant's Build log. The
//! door is also served on `BuildClaim` (op `0x1C`) for a caller that puts a claim over Node RPC; the
//! product has no such caller. An admin whose committed fabric seat record names another birth
//! decides no fabric-lane claim; one whose mesh seat record names another birth decides no claim of
//! that mesh's drift lane.
//!
//! Context is attempt-scoped. The fabric-primary keeps one [`CallContext`] per `(build_id,
//! attempt)` in a record of its own data dir (`attempt-context/`), written when the attempt comes
//! into being (the REST request span, or the `via-proven-drift` span) and returned in `Won`. It is
//! never a Build fact: Build facts are re-broadcast to every new neighbour. A next attempt that
//! has no context of its own inherits the previous attempt's at claim time.

use crate::build::BuildId;
use crate::mesh_claims::{InFlight, MeshClaims, Taken};
use crate::build_state::{BuildAttemptClaim, BuildStateAdapter, ClaimOutcome};
use crate::model::{EndpointId, NodeId, PathName};
use crate::record_store::{FileRecords, StorageError};
use crate::topology::Topology;
use rafka_node_rpc::ServerBuilder;
use rafka_node_rpc_contract::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::context::CallContext;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::Instrument as _;

const CONTEXT_DIR: &str = "attempt-context";
const CONTEXT_FORMAT: &str = "attempt-context/1";

/// The caller system every RDM-originated attempt context names.
pub const CALLER_SYSTEM: &str = "rdm";

/// The context of the current span: where an attempt created now comes from.
pub fn current_context() -> CallContext {
    CallContext {
        caller_system: Some(CALLER_SYSTEM.into()),
        traceparent: rafka_mesh_telemetry::current_traceparent(),
        tracestate: rafka_mesh_telemetry::current_tracestate(),
        baggage: None,
    }
}

enum Backing {
    Files(FileRecords),
    Memory(Mutex<BTreeMap<String, CallContext>>),
}

/// The fabric-primary's local record of each attempt's context, keyed `(build_id, attempt)`.
/// Local to the admin that wrote it: nothing here is gossiped, journaled as a fact or read by
/// another admin.
pub struct AttemptContexts {
    backing: Backing,
}

impl AttemptContexts {
    /// The record under `<own_data_dir>/attempt-context/`.
    pub async fn open(own_data_dir: &Path, writer: &crate::model::IncarnationId) -> Result<Self, StorageError> {
        Ok(Self { backing: Backing::Files(FileRecords::open(own_data_dir, CONTEXT_DIR, writer).await?) })
    }

    /// A record held in memory only: a fixture whose admin has no data dir.
    pub fn in_memory() -> Self {
        Self { backing: Backing::Memory(Mutex::new(BTreeMap::new())) }
    }

    /// `<build_id>-<attempt>` for the fabric lane, `<build_id>-<mesh>-<attempt>` for a mesh's drift
    /// lane: an attempt number names one attempt per lane.
    fn key(build_id: &BuildId, lane: Option<&str>, attempt: u32) -> String {
        match lane {
            None => format!("{build_id}-{attempt}"),
            Some(mesh) => format!("{build_id}-{mesh}-{attempt}"),
        }
    }

    /// Put the context of the fabric lane's `(build_id, attempt)`: a blind put of its own key.
    pub async fn put(&self, build_id: &BuildId, attempt: u32, context: &CallContext) -> Result<(), StorageError> {
        self.put_lane(build_id, None, attempt, context).await
    }

    /// Put the context of `(build_id, lane, attempt)`: a blind put of its own key.
    pub async fn put_lane(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, context: &CallContext) -> Result<(), StorageError> {
        let key = Self::key(build_id, lane, attempt);
        match &self.backing {
            Backing::Files(f) => f.write(&key, CONTEXT_FORMAT, context).await,
            Backing::Memory(m) => {
                m.lock().unwrap().insert(key, context.clone());
                Ok(())
            }
        }
    }

    /// The context of the fabric lane's `(build_id, attempt)`, when one was put.
    pub async fn get(&self, build_id: &BuildId, attempt: u32) -> Result<Option<CallContext>, StorageError> {
        self.get_lane(build_id, None, attempt).await
    }

    /// The context of `(build_id, lane, attempt)`, when one was put.
    pub async fn get_lane(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<Option<CallContext>, StorageError> {
        let key = Self::key(build_id, lane, attempt);
        match &self.backing {
            Backing::Files(f) => f.read(&key, CONTEXT_FORMAT).await,
            Backing::Memory(m) => Ok(m.lock().unwrap().get(&key).cloned()),
        }
    }

    /// The context `attempt` is claimed with: its own, else the previous attempt's (a hand-off
    /// continues the trace it came from), stored under `attempt` so a repeat claim answers the
    /// same. An attempt nothing here knows (the fabric-primary changed) starts a new trace.
    pub async fn for_claim(&self, build_id: &BuildId, attempt: u32) -> Result<CallContext, StorageError> {
        self.for_claim_lane(build_id, None, attempt).await
    }

    /// [`Self::for_claim`] of `(build_id, lane, attempt)`: inherited within the lane only.
    pub async fn for_claim_lane(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<CallContext, StorageError> {
        if let Some(own) = self.get_lane(build_id, lane, attempt).await? {
            return Ok(own);
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
/// in-flight row is taken first: another attempt holding it refuses `BuildInProgress` naming it and
/// its claimant, and nothing is written. Then the claim key `(build_id, lane, attempt)` is an
/// insert-and-fail on `builds`: `Won` carries the attempt's context. An attempt that is not open
/// releases the row taken for it; a claim `Lost` to another executor of the same attempt leaves the
/// row, which names the attempt, to its verdict. Who may claim is the caller's rule
/// ([`ClaimDoor`]'s seat check, the recovery door's recoverer), decided before this.
pub async fn decide_claim(me: &PathName, builds: &dyn BuildStateAdapter, claims: &MeshClaims, contexts: &AttemptContexts, claim: &BuildAttemptClaim, mesh: &str) -> BuildClaimReply {
    let lane = claim.mesh.as_deref();
    let span = tracing::info_span!(
        "rdm.node_admin.build.update.via-claim-decision",
        node = %me,
        build_id = %claim.build_id,
        attempt = claim.attempt,
        executor = %claim.executor,
        mesh,
        lane = lane.unwrap_or("fabric"),
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
    );
    let reply = async {
        let row = InFlight { build_id: claim.build_id.clone(), lane: claim.mesh.clone(), attempt: claim.attempt, claimant: claim.claimant.clone().unwrap_or_else(|| me.to_string()) };
        match claims.take(mesh, &row).await {
            Ok(Taken::Won) => {}
            Ok(Taken::Held(held)) => return BuildClaimReply::BuildInProgress { current_build_id: held.build_id.to_string(), mesh: mesh.to_string(), held_by: Some(held.claimant) },
            Err(e) => return BuildClaimReply::NotReady { reason: format!("{me}: the in-flight row of mesh {mesh} could not be taken for attempt {} of {}: {e}", claim.attempt, claim.build_id) },
        }
        match builds.claim_attempt(claim).await {
            Ok(ClaimOutcome::Won) => match contexts.for_claim_lane(&claim.build_id, lane, claim.attempt).await {
                Ok(context) => BuildClaimReply::Won { context },
                Err(e) => BuildClaimReply::NotReady { reason: format!("{me}: attempt {} of {} is claimed, its context record is unreadable: {e}", claim.attempt, claim.build_id) },
            },
            Ok(ClaimOutcome::Lost { holder }) => BuildClaimReply::Lost { holder },
            Ok(ClaimOutcome::NotOpen { next }) => {
                if let Err(e) = claims.release(mesh, &row).await {
                    return BuildClaimReply::NotReady { reason: format!("{me}: attempt {} of {} is not open (next {next:?}) and the in-flight row of mesh {mesh} taken for it could not be released: {e}", claim.attempt, claim.build_id) };
                }
                BuildClaimReply::NotOpen { next }
            }
            Err(e) => BuildClaimReply::NotReady { reason: format!("{me}: claiming attempt {} of {}: {e}", claim.attempt, claim.build_id) },
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
    /// The node id this admin was born with: whether it holds the fabric seat is decided by it.
    pub node_id: NodeId,
    /// The admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// This admin's Build log: the fabric-primary's own, when it holds the seat.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The observability contexts of the attempts it decided.
    pub contexts: Arc<AttemptContexts>,
    /// Each mesh's in-flight row: what a claim takes before it is decided.
    pub claims: Arc<MeshClaims>,
}

impl ClaimDoor {
    /// The reply of a receiver that may not claim in `lane` (R-BI7 caveat 2), decided from this
    /// admin's view of the seats, never from who called: the fabric lane is the fabric-primary's, a
    /// mesh's drift lane that mesh's primary's. `mesh` is the mesh the attempt acts in, when known.
    async fn not_claimant(&self, lane: Option<&str>, mesh: Option<&str>) -> Option<BuildClaimReply> {
        let t = self.topology.read().await;
        let claims = match lane {
            None => t.holds_fabric_seat(&self.node_id),
            Some(lane) => t.holds_mesh_seat(lane, &self.node_id),
        };
        (!claims).then(|| BuildClaimReply::NotClaimant {
            mesh: mesh.map(str::to_string),
            fabric_primary: t.fabric_primary().map(|n| n.name.to_string()),
            mesh_primary: mesh.and_then(|m| t.cohort_primary(m, crate::model::NodeKind::NodeAdmin)).map(|n| n.name.to_string()),
        })
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
        let mesh = match lane {
            Some(mesh) => mesh.to_string(),
            None => match executor.parse::<PathName>() {
                Ok(e) => e.mesh,
                Err(e) => return BuildClaimReply::NotReady { reason: format!("{}: the executor {executor:?} of attempt {attempt} of {build_id} is not a path.name: {e}", self.me) },
            },
        };
        if let Some(refusal) = self.not_claimant(lane, Some(&mesh)).await {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-claimant", node = %self.me, build_id = %build_id, attempt, executor, mesh = %mesh, lane = lane.unwrap_or("fabric"), detail = ?refusal)
                .in_scope(|| tracing::info!("this admin may not claim in that lane in its view"));
            return refusal;
        }
        let claim = BuildAttemptClaim { claimant: Some(self.me.to_string()), mesh: lane.map(str::to_string), build_id: build_id.clone(), attempt, executor: executor.to_string() };
        decide_claim(&self.me, &*self.builds, &self.claims, &self.contexts, &claim, &mesh).await
    }

    /// A request off the wire: the seat first (a receiver that may not claim in the request's lane
    /// only names who may), then the executor the request names, at its exact birth as this admin's view
    /// holds it. Nothing checks who is calling (node-rpc.md §14): `peer` names the call, the claim
    /// is decided for the executor named. A node or birth the view does not hold is refused by name
    /// about that subject, and nothing is claimed.
    pub async fn serve(&self, peer: EndpointId, req: BuildClaimRequest) -> BuildClaimReply {
        let BuildClaimRequest::ClaimAttempt { build_id, attempt, executor_node_id, executor_incarnation, mesh } = req;
        if let Some(refusal) = self.not_claimant(mesh.as_deref(), mesh.as_deref()).await {
            return refusal;
        }
        let executor = self.topology.read().await.births().find(|n| n.node_id == executor_node_id).cloned();
        let Some(executor) = executor else {
            return BuildClaimReply::PeerUnresolved { reason: format!("{}: the request (from endpoint {}) names executor node {executor_node_id}, which is not in this fabric-primary's view", self.me, peer.0) };
        };
        if executor.incarnation_id.as_ref() != Some(&executor_incarnation) {
            return BuildClaimReply::PeerUnresolved {
                reason: format!("{}: the request names birth {} of {} and this view holds {:?}", self.me, executor_incarnation.as_str(), executor.name, executor.incarnation_id.as_ref().map(|i| i.as_str())),
            };
        }
        let build_id = match BuildId::parse(&build_id) {
            Ok(build_id) => build_id,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.build.reject.via-malformed-build-id", node = %self.me, executor = %executor.name, build_id = %build_id, reason = %e, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the claim is refused: its Build id is not a Crockford id"));
                return BuildClaimReply::Malformed { kind: rafka_node_rpc_contract::outcome::MalformedKind::Corrupt };
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
    b.serve::<BuildClaim, _, _>(OpOwner::Product("rdm".into()), move |peer: rafka_node_rpc::PeerContext, req: BuildClaimRequest| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(BuildClaimReply::NotReady { reason: "this admin holds no view yet".into() });
            };
            Ok(door.serve(EndpointId(peer.endpoint_id.to_string()), req).await)
        }
    })
}
