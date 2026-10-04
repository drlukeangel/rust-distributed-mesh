//! `DeploymentPipeline`: create or retire one runtime (PRD §8;
//! mesh-control-plane.md §6).
//!
//! ```text
//! create: AllocateIdentity -> AllocateEndpoints -> PrepareStorage -> PrepareNetwork
//!   -> DeployRuntime -> WaitForBind -> PublishTopology -> WaitForMeshJoin
//!   -> WaitForNodeReady -> Complete
//! retire: MarkDraining -> WaitForDrain -> PublishLeaving -> CloseRpcAdmission
//!   -> TerminateRuntime -> ReleaseEndpoints -> ReleaseStorage (if permanent)
//!   -> RemoveTopologyMembership -> Complete
//! ```
//!
//! Node-admin is the brain: identity, endpoints, storage and readiness are
//! decided here; the provider only realises or retires the runtime. Every
//! step appends a receipt to the Build's state and runs under one child span
//! of the pipeline's parent span. Optional steps (DNS, load balancer,
//! firewall) have no realisation on these providers and are not run.
//!
//! Re-runs reconcile forward. A step whose receipt in an earlier attempt of
//! the same Build is `Complete` is reused from the decision that receipt
//! carries, never decided again, until the first step that has to run; from
//! there every step runs. `DeployRuntime` first looks for the runtime its
//! deployment id already started, so a crash between spawning and the
//! receipt never starts a second one. Every other step is idempotent.

use super::endpoint::{verify_bound_with, EndpointAllocator, SlotSpec};
use super::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use crate::build::BuildId;
use crate::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
use crate::model::{DeploymentId, EndpointSlot, FabricId, IncarnationId, Node, NodeId, NodeStatus, PathName};
use rafka_mesh_entity::launch::Launch;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateStep {
    AllocateIdentity,
    AllocateEndpoints,
    PrepareStorage,
    PrepareNetwork,
    DeployRuntime,
    WaitForBind,
    PublishTopology,
    WaitForMeshJoin,
    WaitForNodeReady,
    Complete,
}

impl CreateStep {
    pub const ORDER: [CreateStep; 10] = [
        Self::AllocateIdentity,
        Self::AllocateEndpoints,
        Self::PrepareStorage,
        Self::PrepareNetwork,
        Self::DeployRuntime,
        Self::WaitForBind,
        Self::PublishTopology,
        Self::WaitForMeshJoin,
        Self::WaitForNodeReady,
        Self::Complete,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::AllocateIdentity => "AllocateIdentity",
            Self::AllocateEndpoints => "AllocateEndpoints",
            Self::PrepareStorage => "PrepareStorage",
            Self::PrepareNetwork => "PrepareNetwork",
            Self::DeployRuntime => "DeployRuntime",
            Self::WaitForBind => "WaitForBind",
            Self::PublishTopology => "PublishTopology",
            Self::WaitForMeshJoin => "WaitForMeshJoin",
            Self::WaitForNodeReady => "WaitForNodeReady",
            Self::Complete => "Complete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireStep {
    MarkDraining,
    WaitForDrain,
    PublishLeaving,
    CloseRpcAdmission,
    TerminateRuntime,
    ReleaseEndpoints,
    /// Only when the retirement is permanent.
    ReleaseStorage,
    RemoveTopologyMembership,
    Complete,
}

impl RetireStep {
    pub const ORDER: [RetireStep; 9] = [
        Self::MarkDraining,
        Self::WaitForDrain,
        Self::PublishLeaving,
        Self::CloseRpcAdmission,
        Self::TerminateRuntime,
        Self::ReleaseEndpoints,
        Self::ReleaseStorage,
        Self::RemoveTopologyMembership,
        Self::Complete,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::MarkDraining => "MarkDraining",
            Self::WaitForDrain => "WaitForDrain",
            Self::PublishLeaving => "PublishLeaving",
            Self::CloseRpcAdmission => "CloseRpcAdmission",
            Self::TerminateRuntime => "TerminateRuntime",
            Self::ReleaseEndpoints => "ReleaseEndpoints",
            Self::ReleaseStorage => "ReleaseStorage",
            Self::RemoveTopologyMembership => "RemoveTopologyMembership",
            Self::Complete => "Complete",
        }
    }
}

/// What the pipeline asks of the live mesh (gossip membership and Node RPC).
#[async_trait::async_trait]
pub trait NodeObserver: Send + Sync {
    /// Has this exact birth (`node_id`, `incarnation`) joined fabric membership?
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool;
    /// Is the node serving on every advertised slot?
    async fn ready(&self, node: &Node) -> Result<(), String>;
    /// After the stop signal: has this birth finished its in-flight work
    /// (it reports `Draining` with nothing in flight, or `Leaving`)?
    async fn drained(&self, node: &Node) -> bool;
    /// Does the node refuse new Node RPC work on every slot (a typed
    /// `Draining`, or nothing admits the call at all)?
    async fn admission_closed(&self, node: &Node) -> Result<(), String>;
}

/// Where node records are published (the fabric control projection).
pub trait TopologySink: Send + Sync {
    fn publish(&self, node: Node);
    fn remove(&self, name: &PathName);
}

/// The fabric-wide facts every launch carries.
#[derive(Debug, Clone)]
pub struct LaunchTemplate {
    pub fabric: String,
    pub executable: PathBuf,
    /// Members to join gossip through: `(public key, address)`.
    pub seeds: Vec<(String, SocketAddr)>,
    /// Passed through to every node (`RAFKA_EVIDENCE_DIR`, `RUST_LOG`, ...).
    pub env: BTreeMap<String, String>,
    pub data_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub build_id: BuildId,
    pub attempt: u32,
    pub node: PathName,
    pub slots: &'static [SlotSpec],
    /// `Some(current record)` for a restart: same node id, data dir and
    /// transport key, a new incarnation, stable slots kept.
    pub restart_of: Option<Node>,
}

#[derive(Debug, Clone)]
pub struct RetireRequest {
    pub build_id: BuildId,
    pub attempt: u32,
    /// The node's current record.
    pub node: Node,
    pub handle: DeploymentHandle,
    /// Permanent: the data dir goes too. Otherwise it is kept (a restart or
    /// a replacement that reuses it).
    pub permanent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineError {
    pub step: &'static str,
    pub reason: String,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} failed: {}", self.step, self.reason)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub bind: Duration,
    pub join: Duration,
    pub ready: Duration,
    /// How long `WaitForDrain` and `CloseRpcAdmission` wait.
    pub drain: Duration,
    /// Stop-ladder grace before a forced kill; longer than the node's own
    /// drain deadline (node-rpc §35).
    pub stop_grace: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            bind: Duration::from_secs(20),
            join: Duration::from_secs(30),
            ready: Duration::from_secs(30),
            drain: Duration::from_secs(10),
            stop_grace: Duration::from_secs(8),
        }
    }
}

pub struct DeploymentPipeline<'a> {
    pub provider: &'a dyn DeploymentProvider,
    pub allocator: &'a Mutex<EndpointAllocator>,
    pub observer: &'a dyn NodeObserver,
    pub sink: &'a dyn TopologySink,
    pub builds: &'a dyn BuildStateAdapter,
    pub template: &'a LaunchTemplate,
    pub timeouts: Timeouts,
}

/// A created node and the runtime realising it.
#[derive(Debug, Clone)]
pub struct Created {
    pub node: Node,
    pub handle: DeploymentHandle,
}

/// What `AllocateIdentity` decides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    node_id: NodeId,
    incarnation: IncarnationId,
    supersedes: Option<IncarnationId>,
    deployment_id: DeploymentId,
}

/// Write (if absent) and read back the node's transport key in its data dir;
/// a restart reuses it, a fresh data dir gets a new one.
fn ensure_transport_key(data_dir: &std::path::Path) -> Result<FabricId, String> {
    std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: {e}", data_dir.display()))?;
    let path = data_dir.join("node-key");
    let key = match std::fs::read_to_string(&path) {
        Ok(h) => {
            let bytes: [u8; 32] = hex::decode(h.trim())
                .map_err(|e| format!("{}: {e}", path.display()))?
                .try_into()
                .map_err(|_| format!("{}: not 32 bytes", path.display()))?;
            iroh::SecretKey::from_bytes(&bytes)
        }
        Err(_) => {
            let k = iroh::SecretKey::generate();
            std::fs::write(&path, hex::encode(k.to_bytes())).map_err(|e| format!("{}: {e}", path.display()))?;
            k
        }
    };
    Ok(FabricId(key.public().to_string()))
}

async fn poll<F, Fut>(within: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let until = Instant::now() + within;
    loop {
        if f().await {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One run of one operation (`create-node:<path>`, `retire-node:<path>`).
struct Run<'r> {
    build_id: &'r BuildId,
    attempt: u32,
    node: &'r PathName,
    operation: String,
    /// Outputs of this operation's `Complete` receipts from earlier attempts.
    done: HashMap<String, Option<serde_json::Value>>,
    /// Still reusing: no step of this run has had to execute yet.
    reusing: bool,
}

impl DeploymentPipeline<'_> {
    async fn begin<'r>(&self, build_id: &'r BuildId, attempt: u32, node: &'r PathName, operation: String) -> Run<'r> {
        let done = match self.builds.read_build(build_id).await {
            Ok(view) => view
                .steps
                .into_iter()
                .filter(|r| r.operation == operation && r.attempt < attempt && r.outcome == StepOutcome::Complete)
                .map(|r| (r.step, r.output))
                .collect(),
            Err(_) => HashMap::new(),
        };
        Run { build_id, attempt, node, operation, done, reusing: true }
    }

    fn pipeline_span(&self, kind: &'static str, build_id: &BuildId, node: &PathName, attempt: u32, restart: bool) -> tracing::Span {
        tracing::info_span!(
            "rafka.node_admin.deployment.update.via-pipeline",
            pipeline = kind,
            build_id = %build_id,
            provider = ?self.provider.kind(),
            node = %node,
            attempt,
            restart,
        )
    }

    /// Run one step under its span and append its receipt (with its output),
    /// or reuse an earlier attempt's decision. A failure stops the run.
    async fn step<T, Fut>(&self, run: &mut Run<'_>, step: &'static str, work: Fut) -> Result<T, PipelineError>
    where
        T: Serialize + DeserializeOwned,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rafka.node_admin.deployment.update.via-step",
            step,
            build_id = %run.build_id,
            provider = ?self.provider.kind(),
            node = %run.node,
            attempt = run.attempt,
            outcome = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        let started = Instant::now();
        if run.reusing {
            let prior = run.done.get(step).map(|o| serde_json::from_value::<T>(o.clone().unwrap_or(serde_json::Value::Null)));
            if let Some(Ok(v)) = prior {
                span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                span.record("outcome", "reused");
                return Ok(v);
            }
            run.reusing = false;
        }
        let r = work.instrument(span.clone()).await;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        let (outcome, output) = match &r {
            Ok(v) => (StepOutcome::Complete, serde_json::to_value(v).ok().filter(|v| !v.is_null())),
            Err(reason) => (StepOutcome::Failed { reason: reason.clone() }, None),
        };
        let _ = self
            .builds
            .append_step_receipt(&BuildStepReceipt {
                build_id: run.build_id.clone(),
                attempt: run.attempt,
                operation: run.operation.clone(),
                step: step.into(),
                outcome,
                output,
            })
            .await;
        r.map_err(|reason| PipelineError { step, reason })
    }

    /// Create (or restart) `req.node` through every step.
    pub async fn create(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("create", &req.build_id, &req.node, req.attempt, req.restart_of.is_some());
        self.create_steps(req).instrument(span).await
    }

    async fn create_steps(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        let op = if req.restart_of.is_some() { "restart-node" } else { "create-node" };
        let mut run = self.begin(&req.build_id, req.attempt, &req.node, format!("{op}:{}", req.node)).await;
        let prior = req.restart_of.clone();
        let id: Identity = self
            .step(&mut run, CreateStep::AllocateIdentity.name(), async {
                Ok(Identity {
                    node_id: prior.as_ref().map(|p| p.node_id.clone()).unwrap_or_else(NodeId::mint),
                    incarnation: IncarnationId::mint(),
                    supersedes: prior.as_ref().and_then(|p| p.incarnation_id.clone()),
                    deployment_id: DeploymentId::mint(),
                })
            })
            .await?;
        let reused_endpoints = run.reusing && run.done.contains_key(CreateStep::AllocateEndpoints.name());
        let endpoints: Vec<EndpointSlot> = self
            .step(&mut run, CreateStep::AllocateEndpoints.name(), async {
                let mut a = self.allocator.lock().unwrap();
                if let Some(p) = &prior {
                    a.adopt(&req.node, p.endpoints.clone());
                }
                a.assign(&req.node, req.slots, prior.is_some()).map_err(|e| e.to_string())
            })
            .await?;
        if reused_endpoints {
            // The decision stands: hold exactly these, take nothing new.
            self.allocator.lock().unwrap().adopt(&req.node, endpoints.clone());
        }
        let data_dir = match prior.as_ref().and_then(|p| p.data_dir.clone()) {
            Some(d) => PathBuf::from(d),
            None => self.template.data_root.join(format!("{}-{}", req.node, id.node_id)),
        };
        let fabric_id: FabricId = self.step(&mut run, CreateStep::PrepareStorage.name(), async { ensure_transport_key(&data_dir) }).await?;
        // The process provider shares the host network namespace and the
        // container provider's network exists per fabric: nothing per node.
        self.step(&mut run, CreateStep::PrepareNetwork.name(), async { Ok(()) }).await?;
        let launch = Launch {
            fabric: self.template.fabric.clone(),
            name: req.node.clone(),
            node_id: id.node_id.clone(),
            incarnation: id.incarnation.clone(),
            supersedes: id.supersedes.clone(),
            endpoints: endpoints.clone(),
            seeds: self.template.seeds.clone(),
            data_dir: data_dir.clone(),
        };
        // The launch environment; its TRACEPARENT is taken inside the
        // DeployRuntime step, so the runtime's boot span is that step's child.
        let spec_for = |traceparent: Option<String>| {
            let mut env = self.template.env.clone();
            env.extend(launch.to_env());
            if let Some(tp) = traceparent {
                env.insert("TRACEPARENT".into(), tp);
            }
            ResolvedNodeLaunch {
                node: req.node.clone(),
                deployment_id: id.deployment_id.clone(),
                executable: self.template.executable.clone(),
                args: vec![],
                env,
                data_dir: data_dir.clone(),
                endpoints: endpoints.clone(),
            }
        };
        // A receipt names a runtime; it is reused only while that runtime runs.
        if run.reusing {
            if let Some(Some(h)) = run.done.get(CreateStep::DeployRuntime.name()) {
                let alive = match serde_json::from_value::<DeploymentHandle>(h.clone()) {
                    Ok(h) => self.provider.inspect(&h).await == DeploymentStatus::Running,
                    Err(_) => false,
                };
                if !alive {
                    run.reusing = false;
                }
            }
        }
        let handle: DeploymentHandle = self
            .step(&mut run, CreateStep::DeployRuntime.name(), async {
                let spec = spec_for(rafka_telemetry::current_traceparent());
                if let Some(h) = self.provider.find(&spec).await {
                    tracing::info!(deployment_id = %spec.deployment_id, "adopting the runtime this deployment already started");
                    return Ok(h);
                }
                self.provider.spawn(&spec).await.map_err(|e| e.to_string())
            })
            .await?;
        self.step(&mut run, CreateStep::WaitForBind.name(), async {
            let until = Instant::now() + self.timeouts.bind;
            loop {
                if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle).await {
                    let detail = self.provider.failure_detail(&handle, &data_dir).await;
                    return Err(format!("runtime exited (code {code:?}) before binding: {detail}"));
                }
                let mut held = Vec::new();
                for e in &endpoints {
                    let transport = req.slots.iter().find(|s| s.slot == e.slot).map(|s| s.transport).unwrap_or(super::endpoint::SlotTransport::Udp);
                    if self.provider.holds(&handle, e.addr, transport).await {
                        held.push(e.addr);
                    }
                }
                match verify_bound_with(&endpoints, &[], |a| held.contains(&a)) {
                    Ok(()) => return Ok(()),
                    Err(e) if Instant::now() >= until => return Err(e.to_string()),
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await?;
        let mut node = Node::allocated(req.node.clone());
        node.node_id = id.node_id.clone();
        node.fabric_id = Some(fabric_id);
        node.incarnation_id = Some(id.incarnation.clone());
        node.deployment_id = Some(id.deployment_id.clone());
        node.provider = Some(self.provider.kind());
        node.data_dir = Some(data_dir.display().to_string());
        node.endpoints = endpoints.clone();
        node.status = NodeStatus::Pending;
        if let Some(p) = &prior {
            node.is_primary = p.is_primary;
        }
        self.step(&mut run, CreateStep::PublishTopology.name(), async {
            self.sink.publish(node.clone());
            Ok(())
        })
        .await?;
        self.step(&mut run, CreateStep::WaitForMeshJoin.name(), async {
            if poll(self.timeouts.join, || self.observer.joined(&id.node_id, &id.incarnation)).await {
                Ok(())
            } else {
                Err(format!("no membership digest from {} incarnation {} within {:?}", req.node, id.incarnation, self.timeouts.join))
            }
        })
        .await?;
        self.step(&mut run, CreateStep::WaitForNodeReady.name(), async {
            let until = Instant::now() + self.timeouts.ready;
            loop {
                match self.observer.ready(&node).await {
                    Ok(()) => return Ok(()),
                    Err(e) if Instant::now() >= until => return Err(e),
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        })
        .await?;
        node.status = NodeStatus::ReadyForTraffic;
        self.step(&mut run, CreateStep::Complete.name(), async {
            self.sink.publish(node.clone());
            Ok(())
        })
        .await?;
        Ok(Created { node, handle })
    }

    /// Retire `req.node` through every step.
    pub async fn retire(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("retire", &req.build_id, &req.node.name, req.attempt, false);
        self.retire_steps(req).instrument(span).await
    }

    async fn retire_steps(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("retire-node:{name}")).await;
        let mut node = req.node.clone();
        let handle = &req.handle;
        node.status = NodeStatus::Draining;
        self.step(&mut run, RetireStep::MarkDraining.name(), async {
            self.sink.publish(node.clone());
            self.provider.signal_stop(handle).await.map_err(|e| e.to_string())
        })
        .await?;
        self.step(&mut run, RetireStep::WaitForDrain.name(), async {
            let drained = poll(self.timeouts.drain, || async {
                self.observer.drained(&node).await || matches!(self.provider.inspect(handle).await, DeploymentStatus::Exited { .. })
            })
            .await;
            if drained {
                Ok(())
            } else {
                Err(format!("{name} still had work in flight after {:?}", self.timeouts.drain))
            }
        })
        .await?;
        node.status = NodeStatus::Leaving;
        self.step(&mut run, RetireStep::PublishLeaving.name(), async {
            self.sink.publish(node.clone());
            Ok(())
        })
        .await?;
        self.step(&mut run, RetireStep::CloseRpcAdmission.name(), async {
            let until = Instant::now() + self.timeouts.drain;
            loop {
                match self.observer.admission_closed(&node).await {
                    Ok(()) => return Ok(()),
                    Err(e) if Instant::now() >= until => return Err(e),
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await?;
        self.step(&mut run, RetireStep::TerminateRuntime.name(), async {
            self.provider
                .terminate(handle, TerminationMode::Graceful { grace: self.timeouts.stop_grace })
                .await
                .map_err(|e| e.to_string())?;
            match self.provider.inspect(handle).await {
                DeploymentStatus::Running => Err(format!("{name} still runs after the stop ladder")),
                _ => Ok(()),
            }
        })
        .await?;
        self.step(&mut run, RetireStep::ReleaseEndpoints.name(), async {
            self.allocator.lock().unwrap().release(name);
            Ok(())
        })
        .await?;
        if req.permanent {
            self.step(&mut run, RetireStep::ReleaseStorage.name(), async {
                match node.data_dir.as_deref() {
                    Some(dir) => match std::fs::remove_dir_all(dir) {
                        Ok(()) => Ok(()),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(e) => Err(format!("{dir}: {e}")),
                    },
                    None => Ok(()),
                }
            })
            .await?;
        }
        self.step(&mut run, RetireStep::RemoveTopologyMembership.name(), async {
            self.sink.remove(name);
            Ok(())
        })
        .await?;
        self.step(&mut run, RetireStep::Complete.name(), async { Ok(()) }).await?;
        Ok(())
    }
}
