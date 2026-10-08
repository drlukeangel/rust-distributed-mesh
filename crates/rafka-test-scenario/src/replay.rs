//! Replay manifests (PRD §20): everything needed to run one scenario again exactly, and the
//! typed semantic outcomes it produced, as machine-readable evidence.
//!
//! A manifest carries the run's ownership (product, feature, subfeature, rung, provider, test),
//! its seed, the exact source/build/pin it ran on, the declared shapes and scenario, the fault
//! schedule and the ordered typed outcomes. Process-specific locators (pids, node ids,
//! incarnations, ports) are not part of an outcome: an outcome carries only what the scenario
//! asserts about, so a replay on another estate compares equal when the semantics are equal.
//! A manifest that is lost, of another schema version, names another provider, is missing a
//! field or schedules a fault at a node or step the scenario does not have is refused with a
//! named [`ManifestError`].

use crate::scenario::{MeshShape, Scenario};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

pub const SCHEMA_VERSION: u32 = 1;

/// Who owns the evidence: the mesh product taxonomy (PRD §16), machine-readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceOwner {
    pub product: String,
    pub feature: String,
    pub subfeature: String,
    pub rung: String,
    pub provider: String,
    pub test: String,
}

/// The exact code the run was of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// The source commit.
    pub sha: String,
    /// The build identity of the executables that ran (their own hash set, or `built-in`).
    pub build: String,
    /// The pin the run was imported at, when it ran as a consumer; empty for RDM itself.
    pub pin: String,
}

/// A fault the schedule applies, by public control only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Fault {
    /// Restart `node` through the node-admin Build rectifier (same NodeId, new incarnation).
    RestartNode { node: String },
}

/// A fault applied immediately before operation `before_operation` (an index into the
/// scenario's operations; `operations.len()` means after the last one, before the assertions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledFault {
    pub before_operation: usize,
    pub fault: Fault,
}

/// One step's typed semantic outcome. No locator of the process that served it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepOutcome {
    /// `operation[i]`, `fault[i]` or `assert[i]`.
    pub step: String,
    /// `put`, `cas`, `delete`, `get` or `restart_node`.
    pub action: String,
    pub target: String,
    pub key: Option<u64>,
    /// The call's outcome (`Reply`, ...), or `Restarted` for a fault.
    pub outcome: String,
    /// The typed result the scenario asserts on (`swapped`, `found`, `value`), or the restart's
    /// facts (`same_node_id`, `new_incarnation`).
    pub result: Value,
}

/// The replay manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayManifest {
    pub schema_version: u32,
    pub owner: EvidenceOwner,
    pub seed: u64,
    pub source: Source,
    pub shapes: Vec<MeshShape>,
    pub scenario: Scenario,
    pub fault_schedule: Vec<ScheduledFault>,
    /// The ordered outcomes of the run that wrote the manifest; empty in a manifest not yet run.
    pub outcomes: Vec<StepOutcome>,
    /// The proof state at the end: `target/key` -> `{found, value}`, ordered by key.
    pub final_state: std::collections::BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    /// The manifest is absent or empty.
    Lost,
    /// It does not parse: a missing or unknown field, or a wrong type, named by serde.
    Schema(String),
    UnsupportedSchemaVersion(u32),
    UnknownProvider(String),
    EmptyOwnerField(&'static str),
    EmptySourceField(&'static str),
    /// The embedded scenario is invalid.
    Scenario(String),
    /// `shapes` does not equal the scenario's meshes.
    ShapesDisagree,
    /// A fault is scheduled at an operation index the scenario does not have.
    FaultStepOutOfRange { before_operation: usize, operations: usize },
    /// A fault names a node that is not an rpc node of a declared mesh.
    FaultTargetUnknown(String),
    /// The owner's provider differs from the scenario's.
    ProviderDisagrees { owner: String, scenario: String },
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lost => write!(f, "replay manifest is lost: nothing to replay"),
            Self::Schema(e) => write!(f, "replay manifest does not match the schema: {e}"),
            Self::UnsupportedSchemaVersion(v) => write!(f, "replay manifest schema_version {v} is not supported (expected {SCHEMA_VERSION})"),
            Self::UnknownProvider(p) => write!(f, "replay manifest provider `{p}` is not process or container"),
            Self::EmptyOwnerField(n) => write!(f, "replay manifest owner.{n} is empty"),
            Self::EmptySourceField(n) => write!(f, "replay manifest source.{n} is empty"),
            Self::Scenario(e) => write!(f, "replay manifest scenario is invalid: {e}"),
            Self::ShapesDisagree => write!(f, "replay manifest shapes differ from the scenario's meshes"),
            Self::FaultStepOutOfRange { before_operation, operations } => write!(f, "fault scheduled before operation {before_operation}, the scenario has {operations}"),
            Self::FaultTargetUnknown(n) => write!(f, "fault targets `{n}`, which is no rpc node of the declared meshes"),
            Self::ProviderDisagrees { owner, scenario } => write!(f, "replay manifest owner.provider `{owner}` differs from the scenario's `{scenario}`"),
        }
    }
}

impl std::error::Error for ManifestError {}

impl ReplayManifest {
    pub fn parse(text: &str) -> Result<Self, ManifestError> {
        if text.trim().is_empty() {
            return Err(ManifestError::Lost);
        }
        let m: ReplayManifest = serde_json::from_str(text).map_err(|e| ManifestError::Schema(e.to_string()))?;
        m.validate()?;
        Ok(m)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a manifest serializes")
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ManifestError::UnsupportedSchemaVersion(self.schema_version));
        }
        let o = &self.owner;
        for (name, v) in [("product", &o.product), ("feature", &o.feature), ("subfeature", &o.subfeature), ("rung", &o.rung), ("test", &o.test)] {
            if v.trim().is_empty() {
                return Err(ManifestError::EmptyOwnerField(name));
            }
        }
        if !matches!(o.provider.as_str(), "process" | "container") {
            return Err(ManifestError::UnknownProvider(o.provider.clone()));
        }
        for (name, v) in [("sha", &self.source.sha), ("build", &self.source.build)] {
            if v.trim().is_empty() {
                return Err(ManifestError::EmptySourceField(name));
            }
        }
        self.scenario.validate().map_err(|e| ManifestError::Scenario(e.to_string()))?;
        if o.provider != self.scenario.provider {
            return Err(ManifestError::ProviderDisagrees { owner: o.provider.clone(), scenario: self.scenario.provider.clone() });
        }
        if self.shapes != self.scenario.meshes {
            return Err(ManifestError::ShapesDisagree);
        }
        for sf in &self.fault_schedule {
            if sf.before_operation > self.scenario.operations.len() {
                return Err(ManifestError::FaultStepOutOfRange { before_operation: sf.before_operation, operations: self.scenario.operations.len() });
            }
            let Fault::RestartNode { node } = &sf.fault;
            if !self.scenario.is_rpc_node(node) {
                return Err(ManifestError::FaultTargetUnknown(node.clone()));
            }
        }
        Ok(())
    }

    /// The ordered actions of the run, without outcomes: what a replay must repeat exactly.
    pub fn actions(&self) -> Vec<String> {
        self.outcomes.iter().map(|o| format!("{} {} {} {}", o.step, o.action, o.target, o.key.map(|k| k.to_string()).unwrap_or_default())).collect()
    }
}

/// The fault schedule a seed derives for a scenario: one restart of a seeded rpc node before a
/// seeded operation. The same seed and scenario always derive the same schedule.
pub fn schedule_from_seed(seed: u64, scenario: &Scenario) -> Vec<ScheduledFault> {
    let nodes: Vec<String> = scenario.meshes.iter().flat_map(|m| (1..=m.rpc_node).map(move |n| format!("{}.rpc.{n}", m.name))).collect();
    if nodes.is_empty() {
        return Vec::new();
    }
    let mut state = seed;
    let mut next = || {
        // splitmix64
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let node = nodes[(next() % nodes.len() as u64) as usize].clone();
    let before_operation = (next() % (scenario.operations.len() as u64 + 1)) as usize;
    vec![ScheduledFault { before_operation, fault: Fault::RestartNode { node } }]
}
