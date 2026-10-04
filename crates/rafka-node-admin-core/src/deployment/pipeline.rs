//! `DeploymentPipeline`: create one runtime (PRD §8; mesh-control-plane.md §6).
//!
//! ```text
//! AllocateIdentity -> AllocateEndpoints -> PrepareStorage -> PrepareNetwork
//!   -> DeployRuntime -> WaitForBind -> PublishTopology -> WaitForMeshJoin
//!   -> WaitForNodeReady -> Complete
//! ```
//!
//! Node-admin is the brain: identity, endpoints, storage and readiness are
//! decided here; the provider only realises the runtime. Every step appends a
//! receipt to the Build's state and runs under one child span of the
//! pipeline's parent span. Optional steps (DNS, load balancer, firewall) have
//! no realisation on the process provider and are not run.

use super::endpoint::{verify_bound_with, EndpointAllocator, SlotSpec};
use super::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch};
use crate::build::BuildId;
use crate::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
use crate::model::{DeploymentId, FabricId, IncarnationId, Node, NodeId, NodeStatus, PathName};
use rafka_mesh_entity::launch::Launch;
use std::collections::BTreeMap;
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

/// What the pipeline asks of the live mesh (gossip membership and Node RPC).
#[async_trait::async_trait]
pub trait NodeObserver: Send + Sync {
    /// Has this exact birth (`node_id`, `incarnation`) joined fabric membership?
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool;
    /// Is the node serving on every advertised slot?
    async fn ready(&self, node: &Node) -> Result<(), String>;
}

/// Where a created node is published (the fabric control projection).
pub trait TopologySink: Send + Sync {
    fn publish(&self, node: Node);
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
}

impl Default for Timeouts {
    fn default() -> Self {
        Self { bind: Duration::from_secs(20), join: Duration::from_secs(30), ready: Duration::from_secs(30) }
    }
}

pub struct CreatePipeline<'a> {
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

impl CreatePipeline<'_> {
    async fn receipt(&self, req: &CreateRequest, step: CreateStep, outcome: StepOutcome) {
        let op = if req.restart_of.is_some() { "restart-node" } else { "create-node" };
        let _ = self
            .builds
            .append_step_receipt(&BuildStepReceipt {
                build_id: req.build_id.clone(),
                attempt: req.attempt,
                operation: format!("{op}:{}", req.node),
                step: step.name().into(),
                outcome,
            })
            .await;
    }

    /// Run one step under its span, append its receipt, and stop on failure.
    async fn step<T, Fut>(&self, req: &CreateRequest, step: CreateStep, work: Fut) -> Result<T, PipelineError>
    where
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rafka.node_admin.deployment.update.via-step",
            step = step.name(),
            build_id = %req.build_id,
            provider = ?self.provider.kind(),
            node = %req.node,
            attempt = req.attempt,
            outcome = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        let started = Instant::now();
        let r = work.instrument(span.clone()).await;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        match r {
            Ok(v) => {
                self.receipt(req, step, StepOutcome::Complete).await;
                Ok(v)
            }
            Err(reason) => {
                self.receipt(req, step, StepOutcome::Failed { reason: reason.clone() }).await;
                Err(PipelineError { step: step.name(), reason })
            }
        }
    }

    /// Create (or restart) `req.node` through every step.
    pub async fn run(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rafka.node_admin.deployment.update.via-pipeline",
            pipeline = "create",
            build_id = %req.build_id,
            provider = ?self.provider.kind(),
            node = %req.node,
            restart = req.restart_of.is_some(),
        );
        self.run_steps(req).instrument(span).await
    }

    async fn run_steps(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        let prior = req.restart_of.clone();
        let (node_id, incarnation, supersedes) = self
            .step(req, CreateStep::AllocateIdentity, async {
                Ok(match &prior {
                    Some(p) => (p.node_id.clone(), IncarnationId::mint(), p.incarnation_id.clone()),
                    None => (NodeId::mint(), IncarnationId::mint(), None),
                })
            })
            .await?;
        let endpoints = self
            .step(req, CreateStep::AllocateEndpoints, async {
                let mut a = self.allocator.lock().unwrap();
                if let Some(p) = &prior {
                    a.adopt(&req.node, p.endpoints.clone());
                }
                a.assign(&req.node, req.slots, prior.is_some()).map_err(|e| e.to_string())
            })
            .await?;
        let data_dir = match prior.as_ref().and_then(|p| p.data_dir.clone()) {
            Some(d) => PathBuf::from(d),
            None => self.template.data_root.join(format!("{}-{}", req.node, node_id)),
        };
        let fabric_id = self.step(req, CreateStep::PrepareStorage, async { ensure_transport_key(&data_dir) }).await?;
        // The process provider shares the host network namespace: nothing to prepare.
        self.step(req, CreateStep::PrepareNetwork, async { Ok(()) }).await?;
        let deployment_id = DeploymentId::mint();
        let handle = self
            .step(req, CreateStep::DeployRuntime, async {
                let launch = Launch {
                    fabric: self.template.fabric.clone(),
                    name: req.node.clone(),
                    node_id: node_id.clone(),
                    incarnation: incarnation.clone(),
                    supersedes: supersedes.clone(),
                    endpoints: endpoints.clone(),
                    seeds: self.template.seeds.clone(),
                    data_dir: data_dir.clone(),
                };
                let mut env = self.template.env.clone();
                env.extend(launch.to_env());
                if let Some(tp) = rafka_telemetry::current_traceparent() {
                    env.insert("TRACEPARENT".into(), tp);
                }
                let spec = ResolvedNodeLaunch {
                    node: req.node.clone(),
                    deployment_id: deployment_id.clone(),
                    executable: self.template.executable.clone(),
                    args: vec![],
                    env,
                    data_dir: data_dir.clone(),
                    endpoints: endpoints.clone(),
                };
                self.provider.spawn(&spec).await.map_err(|e| e.to_string())
            })
            .await?;
        self.step(req, CreateStep::WaitForBind, async {
            let until = Instant::now() + self.timeouts.bind;
            loop {
                if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle).await {
                    let detail = self.provider.failure_detail(&handle, &data_dir).await;
                    return Err(format!("runtime exited (code {code:?}) before binding: {detail}"));
                }
                let mut held = Vec::new();
                for e in &endpoints {
                    if self.provider.holds_udp(&handle, e.addr).await {
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
        node.node_id = node_id.clone();
        node.fabric_id = Some(fabric_id);
        node.incarnation_id = Some(incarnation.clone());
        node.deployment_id = Some(deployment_id.clone());
        node.provider = Some(self.provider.kind());
        node.data_dir = Some(data_dir.display().to_string());
        node.endpoints = endpoints.clone();
        node.status = NodeStatus::Pending;
        if let Some(p) = &prior {
            node.is_primary = p.is_primary;
        }
        self.step(req, CreateStep::PublishTopology, async {
            self.sink.publish(node.clone());
            Ok(())
        })
        .await?;
        self.step(req, CreateStep::WaitForMeshJoin, async {
            if poll(self.timeouts.join, || self.observer.joined(&node_id, &incarnation)).await {
                Ok(())
            } else {
                Err(format!("no membership digest from {} incarnation {incarnation} within {:?}", req.node, self.timeouts.join))
            }
        })
        .await?;
        self.step(req, CreateStep::WaitForNodeReady, async {
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
        self.step(req, CreateStep::Complete, async {
            self.sink.publish(node.clone());
            Ok(())
        })
        .await?;
        Ok(Created { node, handle })
    }
}
