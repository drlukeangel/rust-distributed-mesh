//! Typed-object and control-plane DTOs (`MeshDesired`, `FabricDesired`, `BuildState`, `StepView`,
//! `BuildView`, `ClientError`, `Accepted`).

use crate::BuildId;
use serde::{Deserialize, Serialize};
use std::fmt;

/// One mesh's desired counts (`POST /api/meshes`, and inside a fabric Build).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDesired {
    /// The mesh's name.
    pub name: String,
    /// The number of nodes of each type, by the type's name; a type not named is at 0.
    #[serde(default)]
    pub counts: std::collections::BTreeMap<String, u32>,
}

impl MeshDesired {
    /// A mesh named `name` with `counts` (type, count); every other type at 0.
    pub fn of<T: Into<String>>(name: impl Into<String>, counts: impl IntoIterator<Item = (T, u32)>) -> Self {
        Self { name: name.into(), counts: counts.into_iter().map(|(t, n)| (t.into(), n)).collect() }
    }

    /// The desired count of `type`.
    pub fn count(&self, r#type: &str) -> u32 {
        self.counts.get(r#type).copied().unwrap_or(0)
    }
}

/// The whole fabric's desired meshes (`POST /api/build`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricDesired {
    /// The fabric's name.
    pub fabric: String,
    /// The fabric's meshes with their desired counts.
    pub meshes: Vec<MeshDesired>,
}

/// Where a Build is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// Accepted, and no `rdm.build.started` written yet.
    ///
    /// Serialized as `pending`.
    Pending,
    /// `rdm.build.started` written, no `rdm.build.ended`.
    ///
    /// Serialized as `running`.
    Running,
    /// Every step is complete.
    ///
    /// Serialized as `complete`.
    Complete,
    /// A step ended `failed`.
    ///
    /// Serialized as `failed`.
    Failed,
}

/// One `rdm.build.step` event of a Build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepView {
    /// The mesh whose section the step belongs to.
    pub mesh: String,
    /// The operation the step belongs to, `noun.verb:<node_id>`.
    pub operation: String,
    /// The step.
    pub step: String,
    /// `"complete"` or `{"failed": {"reason", "code"}}`.
    pub outcome: serde_json::Value,
}

/// The folded Build view (`GET /api/builds?id=`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildView {
    /// The Build's id.
    pub build_id: BuildId,
    /// The complete accepted topology this Build realizes: per mesh, its node `(path, name)`s.
    pub topology: serde_json::Value,
    /// The change that produced it, tagged by `kind` (`add_node`, `remove_node`, ...): history.
    #[serde(default)]
    pub change: Option<serde_json::Value>,
    /// When the Build was submitted, in milliseconds since the Unix epoch.
    pub time: u64,
    /// Where the Build is in its life.
    pub state: BuildState,
    /// The step events of the Build, in event.id order.
    pub steps: Vec<StepView>,
    /// The failed step's reason, when the Build failed.
    pub last_failure: Option<String>,
}

/// Why a call did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Node-admin refused, with its status and named reason.
    Refused {
        /// The HTTP status of the refusal.
        status: u16,
        /// The named reason.
        error: String,
        /// The detail node-admin gave with the reason.
        detail: String,
    },
    /// Node-admin could not be reached or answered something undecodable.
    Transport {
        /// The URL that was called.
        url: String,
        /// Why the call failed.
        reason: String,
        /// Whether the request was written to the connection. `false` only when the
        /// connection was never made, so the request provably reached no handler.
        request_written: bool,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { status, error, detail } => write!(f, "node-admin refused ({status} {error}): {detail}"),
            Self::Transport { url, reason, .. } => write!(f, "node-admin at {url}: {reason}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// The 202 of a request a Build runs: the Build it accepted or joined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accepted {
    /// The Build the request opened or joined.
    pub build_id: BuildId,
}

#[cfg(test)]
mod tests {
    use super::BuildId;

    /// R-BI5: a Build id the client reads is a Crockford id; a stored `bld-<24 hex>` value is refused by name.
    #[test]
    fn a_stored_bld_hex_build_id_is_refused_by_name() {
        let old = "bld-0123456789abcdef01234567";
        let e = serde_json::from_str::<BuildId>(&format!("\"{old}\"")).unwrap_err().to_string();
        assert!(e.contains("build id") && e.contains(old), "{e}");
    }
}
