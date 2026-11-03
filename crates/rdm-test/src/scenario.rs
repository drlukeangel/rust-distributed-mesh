//! Declarative scenario files (`scenarios/*.yaml`, PRD §6).

use rdm_core::PathName;
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
    /// The number of nodes of each type, by the type's name.
    pub counts: std::collections::BTreeMap<String, u32>,
}

impl MeshShape {
    /// The count of `type`; a type not named is at 0.
    pub fn count(&self, r#type: &str) -> u32 {
        self.counts.get(r#type).copied().unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Put { target: PathName, key: u64, value: String },
    Cas { target: PathName, key: u64, expected: String, value: String },
    Delete { target: PathName, key: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Assertion {
    Get {
        target: PathName,
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
    UnknownTarget(PathName),
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
            Self::UnknownTarget(t) => write!(f, "target {t:?} names no rpc node of the declared meshes"),
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
            if m.count(rdm_core::NODE_ADMIN) < admins || m.count(rdm_core::NODE) < rpc {
                return Err(mismatch(format!("{} has {} node-admin / {} node, floor {admins} / {rpc}", m.name, m.count(rdm_core::NODE_ADMIN), m.count(rdm_core::NODE))));
            }
        }
        for t in self.targets() {
            if !self.is_node(t) {
                return Err(ScenarioError::UnknownTarget(t.clone()));
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

    fn targets(&self) -> impl Iterator<Item = &PathName> {
        self.operations
            .iter()
            .map(|o| match o {
                Operation::Put { target, .. } | Operation::Cas { target, .. } | Operation::Delete { target, .. } => target,
            })
            .chain(self.assert.iter().map(|Assertion::Get { target, .. }| target))
    }

    /// Match an explicitly generated location of a declared `node`; neither field is parsed.
    pub fn is_node(&self, location: &PathName) -> bool {
        self.meshes.iter().any(|mesh| (1..=mesh.count(rdm_core::NODE)).any(|ordinal| {
            PathName::new(format!("{}.node", mesh.name), ordinal.to_string()).as_ref() == Ok(location)
        }))
    }

    /// The fabric this scenario's shape reconciles to: what its `build.create` carries.
    pub fn desired(&self) -> rdm_client::FabricDesired {
        rdm_client::FabricDesired {
            fabric: self.fabric.clone(),
            meshes: self.meshes.iter().map(|m| rdm_client::MeshDesired::of(m.name.clone(), m.counts.clone())).collect(),
        }
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
        assert_eq!(s.meshes, vec![MeshShape { name: "mesh1".into(), counts: [("node-admin".to_string(), 2), ("node".to_string(), 3)].into() }]);
        assert_eq!(s.operations.len(), 8);
        assert_eq!(
            s.operations[6],
            Operation::Cas { target: PathName::new("mesh1.node", "1").unwrap(), key: 100, expected: "seed-000".into(), value: "seed-000-cas".into() }
        );
        assert_eq!(s.operations[7], Operation::Delete { target: PathName::new("mesh1.node", "3").unwrap(), key: 105 });
        assert_eq!(s.assert.len(), 3);
        assert_eq!(s.assert[2], Assertion::Get { target: PathName::new("mesh1.node", "3").unwrap(), key: 105, equals: None, absent: Some(true) });
    }

    #[test]
    fn below_the_shape_floor_is_refused() {
        let bad = SEED.replace("node: 3", "node: 2");
        assert!(matches!(Scenario::parse(&bad), Err(ScenarioError::ShapeMismatch { .. }) | Err(ScenarioError::UnknownTarget(_))));
        let bad = SEED.replace("node-admin: 2", "node-admin: 1");
        assert!(matches!(Scenario::parse(&bad), Err(ScenarioError::ShapeMismatch { .. })));
    }

    #[test]
    fn unknown_provider_target_and_fields_are_refused() {
        assert_eq!(Scenario::parse(&SEED.replace("provider: process", "provider: vm")), Err(ScenarioError::UnknownProvider("vm".into())));
        assert_eq!(
            Scenario::parse(&SEED.replace(r#"target: {path: mesh1.node, name: "3"}, key: 104"#, r#"target: {path: mesh1.node, name: "9"}, key: 104"#)),
            Err(ScenarioError::UnknownTarget(PathName::new("mesh1.node", "9").unwrap()))
        );
        assert!(matches!(Scenario::parse(&SEED.replace("shape: MN", "shape: MN\nextra: 1")), Err(ScenarioError::Parse(_))));
    }

    #[test]
    fn another_types_names_are_not_node_targets() {
        let s = Scenario::parse(SEED).unwrap();
        assert!(!s.is_node(&PathName::new("mesh1.broker", "1").unwrap()));
        assert!(s.is_node(&PathName::new("mesh1.node", "3").unwrap()));
    }
}
