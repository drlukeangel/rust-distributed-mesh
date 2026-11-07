//! The deployment provider of a cell whose fabric runs in this process: the fabric-primary creates
//! every node through its own `node.create` (lifecycles/node-create.md), and its deployment pipeline
//! hands each launch to this provider, which starts it in this process with the cell's own start
//! (a node with its hooks and app protocols, a node-admin with its wiring). Nothing of the join is
//! registered by hand: the pipeline holds what it deployed, writes the birth's runtime record, and
//! the birth reads it and joins as a process the process provider spawned would.
//!
//! Every runtime this provider starts is this process (its pid, start token and control domain are
//! the process provider's), so the birth's runtime record names it and verifies. A launch is keyed by
//! its deployment id; every other runtime (the cell's own Day-0 admin) is the wrapped provider's.
//!
//! A cell takes the node or node-admin it created with [`InProcessProvider::node`] or
//! [`InProcessProvider::admin`] and stops it itself. A runtime the cell holds is not the provider's
//! to stop: a terminate or stop signal of it fails by name, never a false `Exited`.
//!
//! Included by each crate's `tests/main.rs` with
//! `#[path = "../../../tools/test-support/in_process_provider.rs"]`.

use rdm_admin::admin::Running;
use rdm_admin::deployment::deadline::Deadline;
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::ProviderKind;
use rdm_core::launch::Launch;
use rdm_core::PathName;
use rdm_node::node::RunningNode;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

/// How long a create, a start or the Day-0 Build may take before the cell fails by name.
pub const BOUND: Duration = Duration::from_secs(60);

/// What the cell's start made of one launch.
pub enum Started {
    /// An ordinary node.
    Node(RunningNode),
    /// A node-admin.
    Admin(Running),
}

/// The cell's start of one launch: the launch the pipeline resolved, and its whole environment (a
/// node-admin's `AdminConfig::from_env` reads it).
pub type Start = Arc<dyn Fn(Launch, BTreeMap<String, String>) -> Pin<Box<dyn Future<Output = Result<Started, String>> + Send>> + Send + Sync>;

/// `start`, each launch held until `release` is notified once for it (`Notify::notify_one`): a cell
/// that acts between the fabric-primary's spawn of a node and the node's start.
pub fn held(start: Start, release: Arc<tokio::sync::Notify>) -> Start {
    Arc::new(move |launch, env| {
        let (start, release) = (start.clone(), release.clone());
        Box::pin(async move {
            release.notified().await;
            start(launch, env).await
        })
    })
}

/// Where one launch's runtime is.
enum Runtime {
    /// The cell's start has not returned.
    Starting,
    /// Started, held here until the cell takes it.
    Held(Started),
    /// The cell took it.
    Taken,
    /// The cell's start failed: its reason.
    Failed(String),
    /// The provider stopped it.
    Exited,
}

struct Launched {
    deployment_id: String,
    launch: Launch,
    env: BTreeMap<String, String>,
    handle: DeploymentHandle,
    runtime: Runtime,
}

/// A deployment provider that starts each launch in this process through the cell's [`Start`].
pub struct InProcessProvider {
    me: Weak<InProcessProvider>,
    inner: OnceLock<Arc<dyn DeploymentProvider>>,
    start: Start,
    launched: Mutex<Vec<Launched>>,
    changes: tokio::sync::watch::Sender<u64>,
    exits: tokio::sync::watch::Sender<u64>,
}

impl InProcessProvider {
    /// A provider that starts each launch with `start`.
    pub fn new(start: Start) -> Arc<Self> {
        Arc::new_cyclic(|me| Self { me: me.clone(), inner: OnceLock::new(), start, launched: Mutex::new(Vec::new()), changes: tokio::sync::watch::channel(0).0, exits: tokio::sync::watch::channel(0).0 })
    }

    /// The admin's `Wiring::provider`: this provider in front of the one the admin built, which keeps
    /// every runtime this provider did not start (the admin's own).
    #[allow(clippy::type_complexity)]
    pub fn wiring(self: &Arc<Self>) -> Option<Box<dyn FnOnce(Arc<dyn DeploymentProvider>) -> Arc<dyn DeploymentProvider> + Send>> {
        let me = self.clone();
        Some(Box::new(move |inner| {
            let _ = me.inner.set(inner);
            me as Arc<dyn DeploymentProvider>
        }))
    }

    fn inner(&self) -> &Arc<dyn DeploymentProvider> {
        self.inner.get().expect("the provider is wired into an admin before it is called")
    }

    fn changed(&self) {
        self.changes.send_modify(|n| *n += 1);
    }

    fn ours(&self, deployment_id: &str) -> bool {
        self.launched.lock().unwrap().iter().any(|l| l.deployment_id == deployment_id)
    }

    /// The launch the pipeline handed this provider last for the node at `at`.
    pub fn launch(&self, at: &PathName) -> Launch {
        self.launched.lock().unwrap().iter().rev().find(|l| l.launch.location() == *at).map(|l| l.launch.clone()).unwrap_or_else(|| panic!("no launch of {at:?} reached the provider"))
    }

    /// The whole environment of the last launch at `at`, as the pipeline resolved it.
    pub fn env(&self, at: &PathName) -> BTreeMap<String, String> {
        self.launched.lock().unwrap().iter().rev().find(|l| l.launch.location() == *at).map(|l| l.env.clone()).unwrap_or_else(|| panic!("no launch of {at:?} reached the provider"))
    }

    /// The first launch of the node at `at`, once the pipeline has handed it to this provider.
    pub async fn arrived(&self, at: &PathName) -> Launch {
        let mut changes = self.changes.subscribe();
        let arrived = async {
            loop {
                if let Some(l) = self.launched.lock().unwrap().iter().find(|l| l.launch.location() == *at) {
                    return l.launch.clone();
                }
                let _ = changes.changed().await;
            }
        };
        tokio::time::timeout(BOUND, arrived).await.unwrap_or_else(|_| panic!("no launch of {at:?} reached the provider within {BOUND:?}"))
    }

    /// The reason the first failed in-process start of any node at `ats` failed, once one has.
    async fn failed(&self, ats: &[PathName]) -> (PathName, String) {
        let mut changes = self.changes.subscribe();
        loop {
            if let Some(f) = self.launched.lock().unwrap().iter().find_map(|l| match &l.runtime {
                Runtime::Failed(why) if ats.contains(&l.launch.location()) => Some((l.launch.location(), why.clone())),
                _ => None,
            }) {
                return f;
            }
            let _ = changes.changed().await;
        }
    }

    /// [`create`] of `nodes` at `root`, ending by name when the in-process start of one of them fails
    /// (the pipeline would wait for a join that never comes).
    pub async fn create(&self, root: &Running, nodes: Vec<rdm_core::build::Nodes>) {
        let ats: Vec<PathName> = nodes.iter().map(|n| PathName { path: n.path.clone(), name: n.name.clone() }).collect();
        tokio::select! {
            _ = create(root, nodes) => {}
            (at, why) = self.failed(&ats) => panic!("the in-process start of {at:?}, created by node.create, failed: {why}"),
        }
    }

    /// The runtime started for the last launch at `at`, taken by the cell once its start returned.
    async fn take(&self, at: &PathName) -> Started {
        let mut changes = self.changes.subscribe();
        let taken = tokio::time::timeout(BOUND, async {
            loop {
                {
                    let mut launched = self.launched.lock().unwrap();
                    match launched.iter_mut().rev().find(|l| l.launch.location() == *at) {
                        Some(l) => match std::mem::replace(&mut l.runtime, Runtime::Taken) {
                            Runtime::Held(s) => return s,
                            Runtime::Failed(why) => panic!("the in-process start of {at:?} failed: {why}"),
                            Runtime::Taken => panic!("{at:?} was taken once already"),
                            Runtime::Exited => panic!("{at:?} was stopped by the provider before the cell took it"),
                            Runtime::Starting => l.runtime = Runtime::Starting,
                        },
                        None => {}
                    }
                }
                let _ = changes.changed().await;
            }
        });
        taken.await.unwrap_or_else(|_| panic!("{at:?} was not started within {BOUND:?}"))
    }

    /// The ordinary node started at `at`, taken by the cell.
    pub async fn node(&self, at: &PathName) -> RunningNode {
        match self.take(at).await {
            Started::Node(n) => n,
            Started::Admin(_) => panic!("{at:?} is a node-admin, not an ordinary node"),
        }
    }

    /// The node-admin started at `at`, taken by the cell.
    pub async fn admin(&self, at: &PathName) -> Running {
        match self.take(at).await {
            Started::Admin(a) => a,
            Started::Node(_) => panic!("{at:?} is an ordinary node, not a node-admin"),
        }
    }

    fn settle(&self, deployment_id: &str, started: Result<Started, String>) {
        if let Some(l) = self.launched.lock().unwrap().iter_mut().find(|l| l.deployment_id == deployment_id) {
            l.runtime = match started {
                Ok(s) => Runtime::Held(s),
                Err(why) => Runtime::Failed(why),
            };
        }
        self.changed();
    }

    /// Stop the runtime of `handle` when the provider holds it; a runtime the cell took or that has
    /// not started is refused by name.
    fn stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        let deployment = handle.deployment_id.to_string();
        let refuse = |reason: String| Err(DeployError::Terminate { deployment: deployment.clone(), reason });
        let held = {
            let mut launched = self.launched.lock().unwrap();
            let Some(l) = launched.iter_mut().find(|l| l.deployment_id == deployment) else { return refuse("not a launch of this provider".into()) };
            match std::mem::replace(&mut l.runtime, Runtime::Exited) {
                Runtime::Held(s) => s,
                Runtime::Exited => return Ok(()),
                Runtime::Failed(why) => {
                    l.runtime = Runtime::Failed(why);
                    return Ok(());
                }
                Runtime::Taken => {
                    l.runtime = Runtime::Taken;
                    return refuse(format!("the cell holds the in-process runtime of {:?}: the provider does not stop it", l.launch.location()));
                }
                Runtime::Starting => {
                    l.runtime = Runtime::Starting;
                    return refuse(format!("the in-process start of {:?} has not returned", l.launch.location()));
                }
            }
        };
        let exits = self.exits.clone();
        tokio::spawn(async move {
            match held {
                Started::Node(n) => n.stop().await,
                Started::Admin(a) => a.leave().await,
            }
            exits.send_modify(|n| *n += 1);
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl DeploymentProvider for InProcessProvider {
    fn kind(&self) -> ProviderKind {
        self.inner().kind()
    }

    fn control_domain(&self) -> String {
        self.inner().control_domain()
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: Deadline) -> Result<DeploymentHandle, DeployError> {
        let refuse = |reason: String| DeployError::Spawn { path: spec.path.clone(), name: spec.name.clone(), reason };
        let launch = Launch::from_env(|k| spec.env.get(k).cloned()).map_err(|e| refuse(format!("the launch environment is not a launch: {e}")))?;
        let pid = std::process::id();
        let start = rdm_core::runtime::process_start_token(pid).ok_or_else(|| refuse(format!("no start token for pid {pid}")))?;
        let handle = DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(pid), start: Some(start), container: None, domain: Some(self.control_domain()) };
        let deployment_id = spec.deployment_id.to_string();
        self.launched.lock().unwrap().push(Launched { deployment_id: deployment_id.clone(), launch: launch.clone(), env: spec.env.clone(), handle: handle.clone(), runtime: Runtime::Starting });
        self.changed();
        let (me, started) = (self.me.clone(), (self.start)(launch, spec.env.clone()));
        tokio::spawn(async move {
            let started = started.await;
            if let Some(me) = me.upgrade() {
                me.settle(&deployment_id, started);
            }
        });
        Ok(handle)
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode, deadline: Deadline) -> Result<(), DeployError> {
        if !self.ours(handle.deployment_id.as_str()) {
            return self.inner().terminate(handle, mode, deadline).await;
        }
        self.stop(handle)
    }

    async fn inspect(&self, handle: &DeploymentHandle, deadline: Deadline) -> DeploymentStatus {
        let status = self.launched.lock().unwrap().iter().find(|l| l.deployment_id == handle.deployment_id.as_str()).map(|l| match l.runtime {
            Runtime::Starting | Runtime::Held(_) | Runtime::Taken => DeploymentStatus::Running,
            Runtime::Failed(_) => DeploymentStatus::Exited { code: Some(1) },
            Runtime::Exited => DeploymentStatus::Exited { code: Some(0) },
        });
        match status {
            Some(s) => s,
            None => self.inner().inspect(handle, deadline).await,
        }
    }

    async fn signal_stop(&self, handle: &DeploymentHandle, deadline: Deadline) -> Result<(), DeployError> {
        if !self.ours(handle.deployment_id.as_str()) {
            return self.inner().signal_stop(handle, deadline).await;
        }
        self.stop(handle)
    }

    async fn find(&self, spec: &ResolvedNodeLaunch, deadline: Deadline) -> Result<Found, DeployError> {
        let found = self.launched.lock().unwrap().iter().find(|l| l.deployment_id == spec.deployment_id.as_str()).map(|l| match l.runtime {
            Runtime::Starting | Runtime::Held(_) | Runtime::Taken => Found::Running(l.handle.clone()),
            Runtime::Failed(_) | Runtime::Exited => Found::Exited(Some(l.handle.clone())),
        });
        match found {
            Some(f) => Ok(f),
            None => self.inner().find(spec, deadline).await,
        }
    }

    async fn bind_ip(&self, node: &rdm_admin::model::PathName, birth: &rdm_admin::model::DeploymentId, deadline: Deadline) -> Result<std::net::IpAddr, DeployError> {
        self.inner().bind_ip(node, birth, deadline).await
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        let mut all: Vec<DeploymentHandle> = self.launched.lock().unwrap().iter().filter(|l| !matches!(l.runtime, Runtime::Exited | Runtime::Failed(_))).map(|l| l.handle.clone()).collect();
        all.extend(self.inner().launched());
        all
    }

    fn exits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.exits.subscribe()
    }

    async fn failure_detail(&self, handle: &DeploymentHandle, data_dir: &std::path::Path, deadline: Deadline) -> String {
        let failed = self.launched.lock().unwrap().iter().find(|l| l.deployment_id == handle.deployment_id.as_str()).map(|l| match &l.runtime {
            Runtime::Failed(why) => format!("the in-process start failed: {why}"),
            _ => "the in-process runtime has not failed".to_string(),
        });
        match failed {
            Some(f) => f,
            None => self.inner().failure_detail(handle, data_dir, deadline).await,
        }
    }
}

/// Until the fabric-primary `root`'s Day-0 Build is complete and `root` holds itself ready as the
/// fabric-primary and its mesh's primary: the fabric-primary creates nothing before (Q170).
pub async fn day0_complete(root: &Running) {
    let me = root.digest.lock().unwrap().node.node_id.clone();
    let waited = async {
        loop {
            let built = matches!(root.control.builds.current().await, Ok(Some(b)) if b.state == rdm_admin::build_state::BuildState::Complete);
            let roles = root.control.topology.read().await.births().any(|n| n.node_id == me && n.status == rdm_admin::model::NodeStatus::Ready && n.is_primary && n.is_fabric_primary);
            if built && roles {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(BOUND, waited).await.unwrap_or_else(|_| panic!("the Day-0 Build was not complete, with its admin in both roles, within {BOUND:?}"));
}

/// The executable a create names for `type`: a file in `dir` the provider never runs (the Build's
/// acceptance hashes it).
pub fn executable(dir: &std::path::Path, r#type: &str) -> rdm_core::binding::Binding {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir).unwrap();
    let p = dir.join(format!("in-process-{type}", type = r#type));
    if !p.exists() {
        std::fs::write(&p, format!("#!/bin/sh\n# started in process: {}\nexit 0\n", r#type)).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    rdm_core::binding::Binding { launch_id: r#type.into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: None }
}

/// The node `at` of `type` in `mesh`, as a create names it, on [`executable`] in `dir`.
pub fn node_spec(at: &PathName, mesh: &str, r#type: &str, dir: &std::path::Path) -> rdm_core::build::Nodes {
    rdm_core::build::Nodes { path: at.path.clone(), name: at.name.clone(), mesh: mesh.into(), r#type: r#type.into(), storage_persistence: None, executable: Some(executable(dir, r#type)), tags: Default::default(), config: Default::default() }
}

/// `node.create` of `nodes` at the fabric-primary `root`, over Node RPC as any caller makes it
/// (`rdm_client::Nodes::create`), once its Day-0 Build is complete ([`day0_complete`]), followed to
/// its `Complete`. A refusal or any other end is a defect of the cell, named.
pub async fn create(root: &Running, nodes: Vec<rdm_core::build::Nodes>) {
    day0_complete(root).await;
    let me = root.digest.lock().unwrap().clone();
    let admin = rdm_transport::ResolvedNode { mesh: me.node.mesh.clone(), r#type: me.node.r#type.clone(), node_id: me.node.node_id.clone(), path: me.node.path.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().expect("the admin's endpoint key"), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
    let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
    let resolver = Arc::new(rdm_transport::StaticResolver::new());
    resolver.insert(admin);
    let client = Arc::new(rdm_transport::NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm"));
    let at: Vec<String> = nodes.iter().map(|n| format!("{}.{}", n.path, n.name)).collect();
    let mut stream = rdm_client::Nodes::new(rdm_client::BuildCarrier::at_node(client, me.node.node_id.clone())).create(nodes).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of {at:?}: {e}"));
    let followed = async {
        loop {
            match stream.next().await {
                Some(Ok(rdm_client::Frame::Complete)) => return,
                Some(Ok(f @ (rdm_client::Frame::Failed { .. } | rdm_client::Frame::FabricPrimaryMoved { .. }))) => panic!("node.create of {at:?} ended {f:?}"),
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("node.create of {at:?}: {e}"),
                None => panic!("node.create of {at:?} ended with no terminal frame"),
            }
        }
    };
    tokio::time::timeout(BOUND, followed).await.unwrap_or_else(|_| panic!("node.create of {at:?} was not complete within {BOUND:?}"));
    endpoint.close().await;
}
