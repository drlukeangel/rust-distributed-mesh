//! Declarative scenario files (`scenarios/*.yaml`, PRD §6).

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub version: u32,
    pub product: String,
    pub feature: String,
    pub subfeature: String,
    pub rung: String,
    pub provider: String,
    pub shape: String,
    pub fabric: String,
    pub meshes: Vec<MeshShape>,
    /// Each operation is a one-key map (`- put: {...}`), as PRD §6 writes it.
    #[serde(with = "yaml_serde::with::singleton_map_recursive")]
    pub operations: Vec<Operation>,
    #[serde(with = "yaml_serde::with::singleton_map_recursive")]
    pub assert: Vec<Assertion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshShape {
    pub name: String,
    pub node_admin: u32,
    pub rpc_node: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Put { target: String, key: u64, value: String },
    Cas { target: String, key: u64, expected: String, value: String },
    Delete { target: String, key: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Assertion {
    Get {
        target: String,
        key: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equals: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        absent: Option<bool>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScenarioError {
    Parse(String),
    UnsupportedVersion(u32),
    UnknownShape(String),
    ShapeMismatch { shape: String, reason: String },
    UnknownProvider(String),
    UnknownTarget(String),
    AmbiguousGet { key: u64 },
}

impl fmt::Display for ScenarioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(e) => write!(f, "scenario does not parse: {e}"),
            Self::UnsupportedVersion(v) => write!(f, "scenario version {v} is not supported (expected 1)"),
            Self::UnknownShape(s) => write!(f, "shape `{s}` is not SN, MN or MM"),
            Self::ShapeMismatch { shape, reason } => write!(f, "meshes are below the {shape} floor: {reason}"),
            Self::UnknownProvider(p) => write!(f, "provider `{p}` is not process or container"),
            Self::UnknownTarget(t) => write!(f, "target `{t}` names no rpc node of the declared meshes"),
            Self::AmbiguousGet { key } => write!(f, "get on key {key} must say exactly one of `equals` or `absent: true`"),
        }
    }
}

impl Scenario {
    pub fn parse(text: &str) -> Result<Self, ScenarioError> {
        let s: Scenario = yaml_serde::from_str(text).map_err(|e| ScenarioError::Parse(e.to_string()))?;
        s.validate()?;
        Ok(s)
    }

    /// SN/MN/MM are floors (PRD §1.12-13): SN = 1 admin + 1 rpc node,
    /// MN = 2 admins + 3 rpc nodes, MM = two MN meshes.
    pub fn validate(&self) -> Result<(), ScenarioError> {
        if self.version != 1 {
            return Err(ScenarioError::UnsupportedVersion(self.version));
        }
        if !matches!(self.provider.as_str(), "process" | "container") {
            return Err(ScenarioError::UnknownProvider(self.provider.clone()));
        }
        let (meshes, admins, rpc) = match self.shape.as_str() {
            "SN" => (1, 1, 1),
            "MN" => (1, 2, 3),
            "MM" => (2, 2, 3),
            other => return Err(ScenarioError::UnknownShape(other.into())),
        };
        let mismatch = |reason: String| ScenarioError::ShapeMismatch { shape: self.shape.clone(), reason };
        if self.meshes.len() < meshes {
            return Err(mismatch(format!("{} mesh(es), floor {meshes}", self.meshes.len())));
        }
        for m in &self.meshes {
            if m.node_admin < admins || m.rpc_node < rpc {
                return Err(mismatch(format!("{} has {} admin / {} rpc, floor {admins} / {rpc}", m.name, m.node_admin, m.rpc_node)));
            }
        }
        for t in self.targets() {
            if !self.is_rpc_node(t) {
                return Err(ScenarioError::UnknownTarget(t.into()));
            }
        }
        for a in &self.assert {
            let Assertion::Get { key, equals, absent, .. } = a;
            if equals.is_some() == (*absent == Some(true)) {
                return Err(ScenarioError::AmbiguousGet { key: *key });
            }
        }
        Ok(())
    }

    fn targets(&self) -> impl Iterator<Item = &str> {
        self.operations
            .iter()
            .map(|o| match o {
                Operation::Put { target, .. } | Operation::Cas { target, .. } | Operation::Delete { target, .. } => target.as_str(),
            })
            .chain(self.assert.iter().map(|Assertion::Get { target, .. }| target.as_str()))
    }

    /// `<mesh>.rpc.<ordinal>` with `1 <= ordinal <= rpc_node` of a declared mesh.
    pub fn is_rpc_node(&self, path_name: &str) -> bool {
        let mut parts = path_name.split('.');
        let (Some(mesh), Some("rpc"), Some(n), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            return false;
        };
        let Ok(n) = n.parse::<u32>() else { return false };
        self.meshes.iter().any(|m| m.name == mesh && (1..=m.rpc_node).contains(&n))
    }

    /// The `POST /api/build` body for this scenario's shape.
    pub fn build_request(&self) -> serde_json::Value {
        serde_json::json!({ "fabric": self.fabric, "meshes": self.meshes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: &str = include_str!("../scenarios/i143-node-rpc-seed.yaml");

    #[test]
    fn seed_parses_as_the_prd_section_6_scenario() {
        let s = Scenario::parse(SEED).expect("seed parses");
        assert_eq!((s.product.as_str(), s.feature.as_str(), s.subfeature.as_str()), ("mesh", "node-rpc", "rpc-certainty"));
        assert_eq!((s.rung.as_str(), s.provider.as_str(), s.shape.as_str()), ("multi-node", "process", "MN"));
        assert_eq!(s.meshes, vec![MeshShape { name: "mesh1".into(), node_admin: 2, rpc_node: 3 }]);
        assert_eq!(s.operations.len(), 8);
        assert_eq!(
            s.operations[6],
            Operation::Cas { target: "mesh1.rpc.1".into(), key: 100, expected: "seed-000".into(), value: "seed-000-cas".into() }
        );
        assert_eq!(s.operations[7], Operation::Delete { target: "mesh1.rpc.3".into(), key: 105 });
        assert_eq!(s.assert.len(), 3);
        assert_eq!(s.assert[2], Assertion::Get { target: "mesh1.rpc.3".into(), key: 105, equals: None, absent: Some(true) });
    }

    #[test]
    fn below_the_shape_floor_is_refused() {
        let bad = SEED.replace("rpc_node: 3", "rpc_node: 2");
        assert!(matches!(Scenario::parse(&bad), Err(ScenarioError::ShapeMismatch { .. }) | Err(ScenarioError::UnknownTarget(_))));
        let bad = SEED.replace("node_admin: 2", "node_admin: 1");
        assert!(matches!(Scenario::parse(&bad), Err(ScenarioError::ShapeMismatch { .. })));
    }

    #[test]
    fn unknown_provider_target_and_fields_are_refused() {
        assert_eq!(Scenario::parse(&SEED.replace("provider: process", "provider: vm")), Err(ScenarioError::UnknownProvider("vm".into())));
        assert_eq!(
            Scenario::parse(&SEED.replace("target: mesh1.rpc.3, key: 104", "target: mesh1.rpc.9, key: 104")),
            Err(ScenarioError::UnknownTarget("mesh1.rpc.9".into()))
        );
        assert!(matches!(Scenario::parse(&SEED.replace("shape: MN", "shape: MN\nextra: 1")), Err(ScenarioError::Parse(_))));
    }

    #[test]
    fn legacy_role_names_are_not_rpc_targets() {
        let s = Scenario::parse(SEED).unwrap();
        assert!(!s.is_rpc_node("mesh1.broker.1"));
        assert!(s.is_rpc_node("mesh1.rpc.3"));
    }
}
