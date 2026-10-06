//! Deployment provider policy (PRD §1.7–8, §8).
//!
//! `MESH_SPAWN_TYPE=process|container` is read once, at the first node-admin
//! bootstrap, and normalised into the fabric's typed policy. Later admins
//! inherit it. An unknown value is a hard bootstrap refusal, never a silent
//! fallback; a joining admin whose local value disagrees with the fabric
//! refuses deployment authority by name; a Build never names a provider.

use crate::model::ProviderKind;
use std::fmt;

/// The bootstrap selector.
pub const SPAWN_TYPE_ENV: &str = "MESH_SPAWN_TYPE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyRefusal {
    /// `MESH_SPAWN_TYPE` holds a value that names no provider.
    UnknownProvider { value: String },
    /// A joining admin's local value conflicts with the established fabric policy.
    ProviderMismatch { fabric: ProviderKind, local: ProviderKind },
}

impl fmt::Display for PolicyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProvider { value } => {
                write!(f, "{SPAWN_TYPE_ENV}={value:?} names no deployment provider (expected process or container)")
            }
            Self::ProviderMismatch { fabric, local } => write!(
                f,
                "this admin's {SPAWN_TYPE_ENV} selects {local:?} but the fabric's established provider is {fabric:?}; refusing deployment authority"
            ),
        }
    }
}

/// Parse one `MESH_SPAWN_TYPE` value. Case and surrounding whitespace are
/// normalised; anything else is refused by name. `None` (unset) is `None`.
pub fn parse(value: Option<&str>) -> Result<Option<ProviderKind>, PolicyRefusal> {
    let Some(raw) = value else { return Ok(None) };
    match raw.trim().to_ascii_lowercase().as_str() {
        "process" => Ok(Some(ProviderKind::Process)),
        "container" => Ok(Some(ProviderKind::Container)),
        _ => Err(PolicyRefusal::UnknownProvider { value: raw.to_string() }),
    }
}

/// The fabric's deployment policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FabricPolicy {
    pub provider: ProviderKind,
}

impl FabricPolicy {
    /// The first admin of a fabric: its `MESH_SPAWN_TYPE` decides; unset is `process`.
    pub fn bootstrap(env_value: Option<&str>) -> Result<Self, PolicyRefusal> {
        Ok(Self { provider: parse(env_value)?.unwrap_or(ProviderKind::Process) })
    }

    /// A later admin joining a fabric whose policy is `established`. Unset
    /// inherits; an equal value is fine; anything else is refused by name.
    pub fn inherit(established: FabricPolicy, local_env_value: Option<&str>) -> Result<Self, PolicyRefusal> {
        match parse(local_env_value)? {
            None => Ok(established),
            Some(p) if p == established.provider => Ok(established),
            Some(local) => Err(PolicyRefusal::ProviderMismatch { fabric: established.provider, local }),
        }
    }
}

/// True when a Build request body tries to choose a provider (a top-level or
/// per-mesh `provider` field): the Build is refused, provider is fabric policy.
pub fn build_names_a_provider(body: &serde_json::Value) -> bool {
    body.get("provider").is_some()
        || body.get("meshes").and_then(|m| m.as_array()).is_some_and(|ms| ms.iter().any(|m| m.get("provider").is_some()))
}


// ---------------------------------------------------------------------------
// The provider contract (mesh-control-plane.md §6). A provider realises or
// retires one runtime from an already-resolved launch; it decides nothing.
// ---------------------------------------------------------------------------

use crate::model::{DeploymentId, PathName};
use rafka_mesh_entity::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Everything already decided by node-admin: identity, endpoints, storage, env.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNodeLaunch {
    pub node: PathName,
    pub deployment_id: DeploymentId,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub data_dir: PathBuf,
    /// The advertised sockets the runtime must bind: its Iroh transport and its
    /// listeners. The provider honours them; it never invents one.
    pub transport: std::net::SocketAddr,
    pub listeners: Vec<(String, std::net::SocketAddr)>,
}

/// A realised runtime, exactly: a pid only together with its start token, a
/// container by its immutable id, each within its provider control domain.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeploymentHandle {
    pub deployment_id: DeploymentId,
    pub provider: ProviderKind,
    /// Process id (process provider); a container's init pid (container provider).
    pub pid: Option<u32>,
    /// The kernel's start time of `pid` (process provider): a recycled pid never matches.
    #[serde(default)]
    pub start: Option<u64>,
    /// The immutable container id (container provider).
    pub container: Option<String>,
    /// The provider control domain the locator means something in.
    #[serde(default)]
    pub domain: Option<String>,
}

impl DeploymentHandle {
    /// The published fact of this runtime; `None` when the handle is not exact.
    pub fn fact(&self) -> Option<RuntimeFact> {
        let locator = match self.provider {
            ProviderKind::Process => RuntimeLocator::Process { pid: self.pid?, start: self.start? },
            ProviderKind::Container => RuntimeLocator::Container { id: self.container.clone()? },
        };
        let fact = RuntimeFact {
            deployment_id: self.deployment_id.0.clone(),
            provider: match self.provider {
                ProviderKind::Process => RuntimeProvider::Process,
                ProviderKind::Container => RuntimeProvider::Container,
            },
            control_domain: self.domain.clone()?,
            locator,
        };
        fact.validate().ok()?;
        Some(fact)
    }
}

/// Why a published runtime fact is not adopted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptRefusal {
    /// The fact is malformed or inexact.
    Invalid { reason: String },
    /// Another provider realised it.
    OtherProvider { fact: &'static str, here: ProviderKind },
    /// Its locator means something only in another control domain.
    ForeignControlDomain { fact_domain: String, here: String },
}

impl AdoptRefusal {
    /// The span reason (`rafka.node_admin.runtime.reject.via-<reason>`).
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "invalid-fact",
            Self::OtherProvider { .. } => "other-provider",
            Self::ForeignControlDomain { .. } => "foreign-control-domain",
        }
    }
}

impl fmt::Display for AdoptRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { reason } => write!(f, "the runtime fact is not exact: {reason}"),
            Self::OtherProvider { fact, here } => write!(f, "a {fact} runtime cannot be controlled by the {here:?} provider"),
            Self::ForeignControlDomain { fact_domain, here } => write!(
                f,
                "the runtime lives in control domain {} and this provider controls {}; refusing to act on its locator",
                rafka_mesh_entity::runtime::fingerprint(fact_domain),
                rafka_mesh_entity::runtime::fingerprint(here)
            ),
        }
    }
}

/// `fact` as a handle `provider` may act on, or the named reason it may not.
pub fn adopt(provider: &dyn DeploymentProvider, fact: &RuntimeFact) -> Result<DeploymentHandle, AdoptRefusal> {
    fact.validate().map_err(|e| AdoptRefusal::Invalid { reason: e.to_string() })?;
    let kind = match fact.provider {
        RuntimeProvider::Process => ProviderKind::Process,
        RuntimeProvider::Container => ProviderKind::Container,
    };
    if kind != provider.kind() {
        return Err(AdoptRefusal::OtherProvider { fact: fact.provider.as_str(), here: provider.kind() });
    }
    let here = provider.control_domain();
    if fact.control_domain != here {
        return Err(AdoptRefusal::ForeignControlDomain { fact_domain: fact.control_domain.clone(), here });
    }
    let (pid, start, container) = match &fact.locator {
        RuntimeLocator::Process { pid, start } => (Some(*pid), Some(*start), None),
        RuntimeLocator::Container { id } => (None, None, Some(id.clone())),
    };
    Ok(DeploymentHandle { deployment_id: DeploymentId(fact.deployment_id.clone()), provider: kind, pid, start, container, domain: Some(here) })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationMode {
    /// SIGTERM (or the provider's stop), then a forced kill after `grace`.
    Graceful { grace: std::time::Duration },
    Immediate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentStatus {
    Running,
    Exited { code: Option<i32> },
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeployError {
    Spawn { node: String, reason: String },
    Terminate { deployment: String, reason: String },
    /// The provider cannot run on this host (named, never a silent pass).
    Unsupported { provider: ProviderKind, reason: String },
}

impl std::fmt::Display for DeployError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn { node, reason } => write!(f, "deploying {node} failed: {reason}"),
            Self::Terminate { deployment, reason } => write!(f, "terminating deployment {deployment} failed: {reason}"),
            Self::Unsupported { provider, reason } => write!(f, "{provider:?} provider is unsupported on this host: {reason}"),
        }
    }
}

#[async_trait::async_trait]
pub trait DeploymentProvider: Send + Sync {
    fn kind(&self) -> ProviderKind;
    /// The provider control domain this provider's locators mean something in.
    fn control_domain(&self) -> String;
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError>;
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError>;
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus;

    /// Stage one of the stop ladder: ask the runtime to drain and stop
    /// (SIGTERM). A runtime already gone is not an error.
    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError>;

    /// The live runtime this exact launch (`spec.deployment_id`) already
    /// started, if any: a re-run after a crash mid-`DeployRuntime` adopts it
    /// instead of starting a second one.
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle>;

    /// Does the runtime hold `addr` over `transport` (`WaitForBind`)? Asked
    /// of the operating system, never of the runtime. Default: something in
    /// this host's network namespace holds it.
    /// Does this runtime hold `addr`? With a pid, the runtime's own descriptors answer: a port
    /// another process holds is not this runtime's bind. Without one, whether anyone holds it.
    async fn holds(&self, handle: &DeploymentHandle, addr: std::net::SocketAddr, transport: super::endpoint::SlotTransport) -> bool {
        match (handle.pid, transport) {
            (Some(pid), super::endpoint::SlotTransport::Udp) => super::container::process_holds(pid, "udp", addr, None).unwrap_or(false),
            (Some(pid), super::endpoint::SlotTransport::Tcp) => super::container::process_holds(pid, "tcp", addr, Some("0A")).unwrap_or(false),
            (None, super::endpoint::SlotTransport::Udp) => super::endpoint::udp_port_is_held(addr),
            (None, super::endpoint::SlotTransport::Tcp) => super::endpoint::tcp_port_is_held(addr),
        }
    }

    /// Every runtime this provider started that it has not seen exit: what a
    /// fabric shutdown stops, including a runtime whose create failed after
    /// `DeployRuntime`.
    fn launched(&self) -> Vec<DeploymentHandle> {
        Vec::new()
    }

    /// The runtime's last words, for a named failure when it stopped early.
    /// Default: the tail of `stderr.log` in its data dir.
    async fn failure_detail(&self, _handle: &DeploymentHandle, data_dir: &std::path::Path) -> String {
        tail(&std::fs::read_to_string(data_dir.join("stderr.log")).unwrap_or_default(), 5)
    }
}

/// The last `n` lines of `text`, joined with ` | `.
pub fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    lines[lines.len().saturating_sub(n)..].join(" | ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A provider whose locators any admin of the same pool can control
    /// (a remotely controllable provider domain): authority is not bound to
    /// the host that launched a runtime.
    struct RemotePool(&'static str);

    #[async_trait::async_trait]
    impl DeploymentProvider for RemotePool {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            format!("remote:{}", self.0)
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "not in this test".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
            None
        }
    }

    fn fact(domain: &str) -> RuntimeFact {
        RuntimeFact {
            deployment_id: "dep-1".into(),
            provider: RuntimeProvider::Process,
            control_domain: domain.into(),
            locator: RuntimeLocator::Process { pid: 4242, start: 17 },
        }
    }

    #[test]
    fn a_published_runtime_is_adopted_only_within_its_provider_and_control_domain() {
        let launcher_pool = RemotePool("pool-a");
        let successor_pool = RemotePool("pool-a");
        // Published by one admin's provider, adopted by another admin of the same domain.
        let h = adopt(&successor_pool, &fact(&launcher_pool.control_domain())).unwrap();
        assert_eq!((h.pid, h.start, h.domain.as_deref()), (Some(4242), Some(17), Some("remote:pool-a")));
        assert_eq!(h.fact(), Some(fact("remote:pool-a")), "the adopted handle names the same runtime");
        // Another domain: refused by name, never acted on.
        let other = adopt(&RemotePool("pool-b"), &fact("remote:pool-a")).unwrap_err();
        assert!(matches!(other, AdoptRefusal::ForeignControlDomain { .. }), "{other}");
        assert_eq!(other.reason(), "foreign-control-domain");
        // Another provider's runtime, or an inexact one.
        let container = RuntimeFact {
            provider: RuntimeProvider::Container,
            locator: RuntimeLocator::Container { id: "a".repeat(64) },
            ..fact("remote:pool-a")
        };
        assert!(matches!(adopt(&successor_pool, &container), Err(AdoptRefusal::OtherProvider { .. })));
        let pid_only = RuntimeFact { locator: RuntimeLocator::Process { pid: 4242, start: 0 }, ..fact("remote:pool-a") };
        assert!(matches!(adopt(&successor_pool, &pid_only), Err(AdoptRefusal::Invalid { .. })));
    }

    #[test]
    fn known_values_normalise_and_unset_bootstraps_process() {
        assert_eq!(FabricPolicy::bootstrap(Some("container")).unwrap().provider, ProviderKind::Container);
        assert_eq!(FabricPolicy::bootstrap(Some(" Process ")).unwrap().provider, ProviderKind::Process);
        assert_eq!(FabricPolicy::bootstrap(None).unwrap().provider, ProviderKind::Process);
    }

    #[test]
    fn an_unknown_value_is_a_hard_bootstrap_refusal_with_a_named_reason() {
        for v in ["vm", "", "docker", "processes", "kubernetes"] {
            let err = FabricPolicy::bootstrap(Some(v)).unwrap_err();
            assert_eq!(err, PolicyRefusal::UnknownProvider { value: v.into() });
            assert!(err.to_string().contains("MESH_SPAWN_TYPE"), "{err}");
        }
    }

    #[test]
    fn a_joining_admin_inherits_or_refuses_by_name() {
        let fabric = FabricPolicy::bootstrap(Some("container")).unwrap();
        assert_eq!(FabricPolicy::inherit(fabric, None), Ok(fabric));
        assert_eq!(FabricPolicy::inherit(fabric, Some("CONTAINER")), Ok(fabric));
        assert_eq!(
            FabricPolicy::inherit(fabric, Some("process")),
            Err(PolicyRefusal::ProviderMismatch { fabric: ProviderKind::Container, local: ProviderKind::Process })
        );
        assert!(matches!(FabricPolicy::inherit(fabric, Some("vm")), Err(PolicyRefusal::UnknownProvider { .. })));
    }

    #[test]
    fn a_build_body_naming_a_provider_is_detected_anywhere() {
        use serde_json::json;
        assert!(build_names_a_provider(&json!({"fabric": "f", "provider": "process", "meshes": []})));
        assert!(build_names_a_provider(&json!({"fabric": "f", "meshes": [{"name": "m", "node_admin": 1, "rpc_node": 1, "provider": "container"}]})));
        assert!(!build_names_a_provider(&json!({"fabric": "f", "meshes": [{"name": "m", "node_admin": 1, "rpc_node": 1}]})));
    }
}
