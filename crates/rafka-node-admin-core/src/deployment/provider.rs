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

use crate::model::{DeploymentId, EndpointSlot, PathName};
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
    /// The advertised endpoints the runtime must bind. The provider honours
    /// them; it never invents one.
    pub endpoints: Vec<EndpointSlot>,
}

/// A realised runtime.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeploymentHandle {
    pub deployment_id: DeploymentId,
    pub provider: ProviderKind,
    /// Process id (process provider).
    pub pid: Option<u32>,
    /// Container name (container provider).
    pub container: Option<String>,
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
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError>;
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError>;
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus;

    /// Does the runtime hold UDP `addr` (`WaitForBind`)? Asked of the
    /// operating system, never of the runtime. Default: something on this
    /// host's network namespace holds it.
    async fn holds_udp(&self, _handle: &DeploymentHandle, addr: std::net::SocketAddr) -> bool {
        super::endpoint::udp_port_is_held(addr)
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
