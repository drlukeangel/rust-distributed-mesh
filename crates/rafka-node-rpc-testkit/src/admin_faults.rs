//! Testkit-only node-admin stalls: decorators over the traits a node-admin is built from, armed and
//! released through a door, so a scenario holds a real admin at one named cut of its documented
//! Build, deployment, lifecycle and hydration sequences and releases it (i143.e8.s2).
//!
//! The product node-admin carries no fault code. Its `Wiring` (`rafka_node_admin_core::wiring`)
//! takes the decorators here: a [`FaultedBuilds`] over the Build state, a [`FaultedFabricStorage`]
//! over `fabric.storage`, a [`FaultedProvider`] over the deployment provider, a [`FaultedEvents`]
//! over the lifecycle events and one [`FaultedHook`] per reachable lifecycle phase. Each decorator
//! asks the shared [`AdminFaults`] at its one cut; an armed, matching cut parks the caller there
//! until the door releases it. Nothing else changes: every other call passes straight through.
//!
//! A cut is one-shot: it parks the first matching call, records what parked ([`AdminFaults::state`]),
//! and after its release it is spent. The door ([`router`]) is `POST /faults/arm`,
//! `POST /faults/release` and `GET /faults`; `GET /faults` is the active-injection acknowledgement
//! (`held: true` with the exact call that parked) and the observed consequence counters.

use async_trait::async_trait;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use rafka_mesh_entity::LifecycleOp;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome,
    StepOutcome,
};
use rafka_node_admin_core::deployment::pipeline::LifecycleEvents;
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::fabric_storage::{FabricRecord, FabricShutdown, FabricStorage, FabricStorageError};
use rafka_node_admin_core::lifecycle::{HookContext, HookPhase, LifecycleHook, LifecycleHookSpec, LifecycleScope, LifecycleState, ShapePredicate, TransitionKey};
use rafka_node_admin_core::model::ProviderKind;
use rafka_node_admin_core::wiring::Wiring;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

/// Where one armed cut parks a call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CutSpec {
    /// A deployment-pipeline or Build step: the step's work is done and its receipt is not yet
    /// durable. `operation` is a prefix of the receipt's operation key (`create-node`,
    /// `retire-node`); `node` is the operation's path.
    Receipt { step: String, #[serde(default)] operation: Option<String>, #[serde(default)] node: Option<String> },
    /// A lifecycle event a retirement publishes (`deleting`, `restarting`, `deleted`): the step's
    /// receipt is durable and the event is not yet published.
    Event { event: String, #[serde(default)] node: Option<String> },
    /// A lifecycle hook phase of the `Node: Pending -> ReadyForTraffic` transition.
    Hook { phase: String, #[serde(default)] node: Option<String> },
    /// An accepted Build is durable (`publish_accepted` returned) and `Fabric.build_id` has not moved.
    AcceptedBuild,
    /// A write of the Fabric record to `fabric.storage` has begun and is not durable. With
    /// `moves_pointer`, only a write that names a Build (the `Fabric.build_id` pointer moving).
    PointerWrite { #[serde(default)] moves_pointer: bool },
    /// The deployment provider reports a control domain no held birth's runtime fact is in: the
    /// admin cannot adopt what it hears, so its Ready gate stays blocked until the release.
    ProviderDomain,
}

/// What a decorator offers the cuts at the moment it is called.
#[derive(Debug, Clone)]
pub enum Probe {
    Receipt { step: String, operation: String, build_id: String, attempt: u32 },
    Event { event: &'static str, op: LifecycleOp },
    Hook { phase: &'static str, target: String, transition_id: String },
    Accepted { build_id: String },
    Pointer { build_id: Option<String> },
    ProviderDomain,
}

impl CutSpec {
    fn matches(&self, p: &Probe) -> bool {
        let node_is = |want: &Option<String>, got: &str| want.as_deref().is_none_or(|w| got == w || got.ends_with(&format!(":{w}")));
        match (self, p) {
            (CutSpec::Receipt { step, operation, node }, Probe::Receipt { step: s, operation: o, .. }) => {
                step == s && operation.as_deref().is_none_or(|prefix| o.starts_with(prefix)) && node_is(node, o)
            }
            (CutSpec::Event { event, node }, Probe::Event { event: e, op }) => event == e && node_is(node, &op.name.to_string()),
            (CutSpec::Hook { phase, node }, Probe::Hook { phase: ph, target, .. }) => phase == ph && node_is(node, target),
            (CutSpec::AcceptedBuild, Probe::Accepted { .. }) => true,
            (CutSpec::PointerWrite { moves_pointer }, Probe::Pointer { build_id }) => !*moves_pointer || build_id.is_some(),
            (CutSpec::ProviderDomain, Probe::ProviderDomain) => true,
            _ => false,
        }
    }
}

impl Probe {
    fn detail(&self) -> Value {
        match self {
            Probe::Receipt { step, operation, build_id, attempt } => json!({"step": step, "operation": operation, "build_id": build_id, "attempt": attempt}),
            Probe::Event { event, op } => json!({"event": event, "operation": op.operation, "build_id": op.build_id, "attempt": op.attempt, "node": op.name.to_string(), "incarnation_id": op.incarnation.0}),
            Probe::Hook { phase, target, transition_id } => json!({"phase": phase, "target": target, "transition_id": transition_id}),
            Probe::Accepted { build_id } => json!({"build_id": build_id}),
            Probe::Pointer { build_id } => json!({"pointer_to_build_id": build_id}),
            Probe::ProviderDomain => json!({"control_domain": "reported foreign"}),
        }
    }
}

struct Armed {
    spec: CutSpec,
    /// The first call that parked here.
    hit: Option<Value>,
    held: bool,
    released: bool,
    /// Flips to `true` on release; every parked call waits on it.
    release: tokio::sync::watch::Sender<bool>,
    /// Calls parked here now.
    parked: u64,
    held_since: Option<Instant>,
    held_ms: Option<u64>,
    /// Matching calls that arrived while another was already parked here (they park too).
    seen_while_held: u64,
    /// For a condition (nothing parks): its hold span, open from the first consultation until the release.
    condition_span: Option<tracing::Span>,
}

/// The armed cuts of one node-admin process.
pub struct AdminFaults {
    pub name: String,
    cuts: Mutex<BTreeMap<String, Armed>>,
    /// Every Build step receipt the Build decorator saw, by call (a consequence counter).
    receipts_seen: AtomicU64,
}

impl AdminFaults {
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self { name: name.into(), cuts: Mutex::new(BTreeMap::new()), receipts_seen: AtomicU64::new(0) })
    }

    /// Arm `spec` under `id`. Refused by name when `id` is already armed and not spent.
    pub fn arm(&self, id: &str, spec: CutSpec) -> Result<Value, String> {
        let mut cuts = self.cuts.lock().unwrap();
        if cuts.get(id).is_some_and(|c| !c.released) {
            return Err(format!("{}: cut `{id}` is already armed and not released", self.name));
        }
        let span = tracing::info_span!("rdm.testkit.fault.update.via-arm", node = %self.name, cut = id, spec = %serde_json::to_string(&spec).unwrap_or_default());
        span.in_scope(|| tracing::info!("cut armed"));
        cuts.insert(id.to_string(), Armed { spec: spec.clone(), hit: None, held: false, released: false, release: tokio::sync::watch::channel(false).0, parked: 0, held_since: None, held_ms: None, seen_while_held: 0, condition_span: None });
        Ok(json!({"armed": id, "node": self.name, "spec": spec}))
    }

    /// Release cut `id`: its parked call goes on. Refused by name when nothing is armed under `id`.
    pub fn release(&self, id: &str) -> Result<Value, String> {
        let mut cuts = self.cuts.lock().unwrap();
        let c = cuts.get_mut(id).ok_or_else(|| format!("{}: no cut `{id}` is armed", self.name))?;
        let was_held = c.held;
        let held_so_far = c.held_since.map(|t| t.elapsed().as_millis() as u64);
        c.release.send_replace(true);
        // A cut that parked nothing is spent by its release too: it can no longer park.
        c.released = true;
        // A condition (not a parked call) has no parked caller to clear it.
        if matches!(c.spec, CutSpec::ProviderDomain) {
            c.held = false;
            c.held_ms = c.held_since.take().map(|t| t.elapsed().as_millis() as u64);
            // Dropping the span ends it: the hold span lasts from the first consultation to here.
            c.condition_span = None;
        }
        let held_ms = c.held_ms.or(held_so_far);
        tracing::info_span!("rdm.testkit.fault.update.via-release", node = %self.name, cut = id, was_held)
            .in_scope(|| tracing::info!("cut released"));
        Ok(json!({"released": id, "node": self.name, "was_held": was_held, "held_ms": held_ms}))
    }

    /// Every cut and what it parked, and the consequence counters.
    pub fn state(&self) -> Value {
        let cuts = self.cuts.lock().unwrap();
        let rows: serde_json::Map<String, Value> = cuts
            .iter()
            .map(|(id, c)| {
                (
                    id.clone(),
                    json!({
                        "spec": c.spec, "armed": !c.released, "held": c.held, "released": c.released, "hit": c.hit,
                        "held_for_ms": c.held_since.map(|t| t.elapsed().as_millis() as u64).or(c.held_ms), "seen_while_held": c.seen_while_held,
                    }),
                )
            })
            .collect();
        json!({"node": self.name, "cuts": rows, "receipts_seen": self.receipts_seen.load(Ordering::SeqCst)})
    }

    /// Park the caller if an armed cut matches `probe`; returns when the cut is released. Every
    /// matching call parks until the release (a second writer of the same thing cannot slip past a
    /// cut the first one is parked at).
    pub async fn hold(&self, probe: Probe) {
        let parked = {
            let mut cuts = self.cuts.lock().unwrap();
            cuts.iter_mut().find(|(_, c)| !c.released && c.spec.matches(&probe)).map(|(id, c)| {
                if c.parked > 0 {
                    c.seen_while_held += 1;
                }
                c.hit.get_or_insert_with(|| probe.detail());
                c.parked += 1;
                c.held = true;
                c.held_since.get_or_insert_with(Instant::now);
                (id.clone(), c.release.subscribe())
            })
        };
        let Some((id, mut release)) = parked else { return };
        let span = tracing::info_span!("rdm.testkit.fault.update.via-hold", node = %self.name, cut = %id, detail = %probe.detail());
        span.in_scope(|| tracing::info!("cut holds"));
        let _ = release.wait_for(|released| *released).instrument(span).await;
        let mut cuts = self.cuts.lock().unwrap();
        if let Some(c) = cuts.get_mut(&id) {
            c.parked -= 1;
            if c.parked == 0 {
                c.held = false;
                c.held_ms = c.held_since.take().map(|t| t.elapsed().as_millis() as u64);
            }
        }
    }

    /// For a cut that is a condition rather than a parked call: is it in force now? Records the first
    /// time it was consulted while in force, and counts every consultation.
    pub fn in_force(&self, probe: Probe) -> bool {
        let mut cuts = self.cuts.lock().unwrap();
        let mut any = false;
        for (id, c) in cuts.iter_mut().filter(|(_, c)| !c.released && c.spec.matches(&probe)) {
            any = true;
            c.held = true;
            c.held_since.get_or_insert_with(Instant::now);
            c.hit.get_or_insert_with(|| probe.detail());
            c.seen_while_held += 1;
            c.condition_span.get_or_insert_with(|| tracing::info_span!("rdm.testkit.fault.update.via-hold", node = %self.name, cut = %id, detail = %probe.detail()));
        }
        any
    }
}

// ---- the decorators ------------------------------------------------------------------------------

/// The Build state, with the step-receipt and accepted-Build cuts.
pub struct FaultedBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl BuildStateAdapter for FaultedBuilds {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await?;
        self.faults.hold(Probe::Accepted { build_id: accepted.build_id.to_string() }).await;
        Ok(())
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn publish_fabric(&self, record: &FabricRecord) -> Result<(), BuildStateError> {
        self.inner.publish_fabric(record).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.faults.receipts_seen.fetch_add(1, Ordering::SeqCst);
        if receipt.outcome == StepOutcome::Complete {
            self.faults
                .hold(Probe::Receipt { step: receipt.step.clone(), operation: receipt.operation.clone(), build_id: receipt.build_id.to_string(), attempt: receipt.attempt })
                .await;
        }
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
}

/// `fabric.storage`, with the `Fabric.build_id` pointer-write cut.
pub struct FaultedFabricStorage {
    inner: Arc<dyn FabricStorage>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl FabricStorage for FaultedFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        self.inner.fabric().await
    }
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        self.faults.hold(Probe::Pointer { build_id: record.build_id.as_ref().map(|b| b.to_string()) }).await;
        self.inner.put_fabric(record).await
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.inner.shutdown().await
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.inner.put_shutdown(shutdown).await
    }
}

/// The deployment provider; its control domain reads foreign while the provider-domain cut is armed.
pub struct FaultedProvider {
    inner: Arc<dyn DeploymentProvider>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl DeploymentProvider for FaultedProvider {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }
    fn control_domain(&self) -> String {
        let real = self.inner.control_domain();
        if self.faults.in_force(Probe::ProviderDomain) {
            format!("{real}#testkit-foreign-domain")
        } else {
            real
        }
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        self.inner.spawn(spec).await
    }
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        self.inner.terminate(handle, mode).await
    }
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        self.inner.inspect(handle).await
    }
    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        self.inner.signal_stop(handle).await
    }
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        self.inner.find(spec).await
    }
    async fn holds(&self, handle: &DeploymentHandle, addr: std::net::SocketAddr, transport: rafka_node_admin_core::deployment::endpoint::SlotTransport) -> bool {
        self.inner.holds(handle, addr, transport).await
    }
    fn launched(&self) -> Vec<DeploymentHandle> {
        self.inner.launched()
    }
    async fn failure_detail(&self, handle: &DeploymentHandle, data_dir: &std::path::Path) -> String {
        self.inner.failure_detail(handle, data_dir).await
    }
}

/// The lifecycle events a retirement publishes, each with its cut before the publish.
pub struct FaultedEvents {
    inner: Arc<dyn LifecycleEvents>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl LifecycleEvents for FaultedEvents {
    async fn deleting(&self, op: &LifecycleOp) {
        self.faults.hold(Probe::Event { event: "deleting", op: op.clone() }).await;
        self.inner.deleting(op).await
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.faults.hold(Probe::Event { event: "deleted", op: op.clone() }).await;
        self.inner.deleted(op).await
    }
    async fn restarting(&self, op: &LifecycleOp) {
        self.faults.hold(Probe::Event { event: "restarting", op: op.clone() }).await;
        self.inner.restarting(op).await
    }
}

/// A registered lifecycle hook that does nothing but cross its phase's cut.
pub struct FaultedHook {
    phase: HookPhase,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl LifecycleHook for FaultedHook {
    async fn run(&self, ctx: &HookContext) -> Result<(), String> {
        self.faults.hold(Probe::Hook { phase: self.phase.as_str(), target: ctx.target.clone(), transition_id: ctx.transition_id.clone() }).await;
        Ok(())
    }
}

/// The phases of `Node: Pending -> ReadyForTraffic` the admin runs (`bring_into_traffic`); the
/// drain phases belong to a transition into `Draining`, which no admin path runs.
pub const NODE_READY_PHASES: [HookPhase; 3] = [HookPhase::BeforeEligibility, HookPhase::AfterEligibilityBeforeCommit, HookPhase::AfterTransition];

/// The `Wiring` that puts every decorator and hook of `faults` into a node-admin.
pub fn wiring(faults: Arc<AdminFaults>) -> Wiring {
    let (f_builds, f_storage, f_provider, f_events) = (faults.clone(), faults.clone(), faults.clone(), faults.clone());
    let key = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
    Wiring {
        builds: Some(Box::new(move |inner| Arc::new(FaultedBuilds { inner, faults: f_builds }))),
        fabric_storage: Some(Box::new(move |inner| Arc::new(FaultedFabricStorage { inner, faults: f_storage }))),
        provider: Some(Box::new(move |inner| Arc::new(FaultedProvider { inner, faults: f_provider }))),
        lifecycle_events: Some(Box::new(move |inner| Arc::new(FaultedEvents { inner, faults: f_events }))),
        hooks: NODE_READY_PHASES
            .iter()
            .map(|&phase| {
                (
                    LifecycleHookSpec { hook_id: format!("testkit-{}", phase.as_str()), transition: key, phase, applies_when: ShapePredicate::Always, blocking: true, max_attempts: 1 },
                    Arc::new(FaultedHook { phase, faults: faults.clone() }) as Arc<dyn LifecycleHook>,
                )
            })
            .collect(),
    }
}

// ---- the door ------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct ArmBody {
    id: String,
    #[serde(flatten)]
    spec: CutSpec,
}

#[derive(Deserialize)]
struct ReleaseBody {
    id: String,
}

type Refused = (axum::http::StatusCode, Json<Value>);

fn refuse(e: String) -> Refused {
    (axum::http::StatusCode::CONFLICT, Json(json!({"error": "refused", "detail": e})))
}

/// The door: `POST /faults/arm {id, kind, ...}`, `POST /faults/release {id}`, `GET /faults`.
pub fn router(faults: Arc<AdminFaults>) -> Router {
    Router::new()
        .route("/faults/arm", post(|State(f): State<Arc<AdminFaults>>, Json(b): Json<ArmBody>| async move { f.arm(&b.id, b.spec).map(Json).map_err(refuse) }))
        .route("/faults/release", post(|State(f): State<Arc<AdminFaults>>, Json(b): Json<ReleaseBody>| async move { f.release(&b.id).map(Json).map_err(refuse) }))
        .route("/faults", get(|State(f): State<Arc<AdminFaults>>| async move { Json(f.state()) }))
        .with_state(faults)
}

/// Where a node's door address and boot arms live: `<root>/faults/<path.name>.door` and
/// `<root>/faults/<path.name>.boot.json`, with `root` the directory holding every node's data dir.
pub fn faults_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("faults")
}

/// Arm what `<root>/faults/<name>.boot.json` lists (`[{"id": .., "kind": .., ..}]`) and remove the
/// file: a birth at this path arms its boot cuts once, so a later birth of the same path is clean.
pub fn arm_boot_cuts(faults: &AdminFaults, root: &std::path::Path) -> Result<usize, String> {
    let file = faults_dir(root).join(format!("{}.boot.json", faults.name));
    let bytes = match std::fs::read(&file) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("{}: {e}", file.display())),
    };
    let arms: Vec<ArmBody> = serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", file.display()))?;
    for a in &arms {
        faults.arm(&a.id, a.spec.clone())?;
    }
    std::fs::remove_file(&file).map_err(|e| format!("{}: {e}", file.display()))?;
    Ok(arms.len())
}

/// Bind the door on loopback, record its base in `<root>/faults/<name>.door`, and serve it on the
/// current runtime. Returns the base.
pub fn open_door(faults: Arc<AdminFaults>, root: &std::path::Path) -> Result<String, String> {
    let dir = faults_dir(root);
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| format!("fault door: {e}"))?;
    listener.set_nonblocking(true).map_err(|e| format!("fault door: {e}"))?;
    let base = format!("http://{}", listener.local_addr().map_err(|e| e.to_string())?);
    let name = faults.name.clone();
    let listener = tokio::net::TcpListener::from_std(listener).map_err(|e| format!("fault door: {e}"))?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(faults)).await;
    });
    std::fs::write(dir.join(format!("{name}.door")), &base).map_err(|e| format!("{}: {e}", dir.display()))?;
    Ok(base)
}
