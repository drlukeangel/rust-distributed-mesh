//! `path.name`: the stable topology slot `<mesh>.<admin|rpc>.<ordinal>`.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    NodeAdmin,
    RpcNode,
}

impl NodeKind {
    pub fn segment(self) -> &'static str {
        match self {
            Self::NodeAdmin => "admin",
            Self::RpcNode => "rpc",
        }
    }

    pub fn from_segment(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Self::NodeAdmin),
            "rpc" => Some(Self::RpcNode),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathName {
    pub mesh: String,
    pub kind: NodeKind,
    pub ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathNameError {
    Shape(String),
    MeshName(String),
    Kind(String),
    Ordinal(String),
}

impl fmt::Display for PathNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape(s) => write!(f, "`{s}` is not <mesh>.<admin|rpc>.<ordinal>"),
            Self::MeshName(s) => write!(f, "mesh name `{s}` must match [a-z0-9][a-z0-9-]{{0,63}}"),
            Self::Kind(s) => write!(f, "node kind `{s}` is not admin or rpc"),
            Self::Ordinal(s) => write!(f, "ordinal `{s}` is not a number >= 1"),
        }
    }
}

/// `[a-z0-9][a-z0-9-]{0,63}`.
pub fn is_valid_mesh_name(m: &str) -> bool {
    let b = m.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

impl PathName {
    pub fn new(mesh: impl Into<String>, kind: NodeKind, ordinal: u32) -> Self {
        Self { mesh: mesh.into(), kind, ordinal }
    }
}

impl FromStr for PathName {
    type Err = PathNameError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        let [mesh, kind, ord] = parts[..] else { return Err(PathNameError::Shape(s.into())) };
        if !is_valid_mesh_name(mesh) {
            return Err(PathNameError::MeshName(mesh.into()));
        }
        let kind = NodeKind::from_segment(kind).ok_or_else(|| PathNameError::Kind(kind.into()))?;
        let ordinal = ord
            .parse::<u32>()
            .ok()
            .filter(|n| *n >= 1 && !ord.starts_with('0'))
            .ok_or_else(|| PathNameError::Ordinal(ord.into()))?;
        Ok(Self { mesh: mesh.into(), kind, ordinal })
    }
}

impl fmt::Display for PathName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.mesh, self.kind.segment(), self.ordinal)
    }
}

impl Serialize for PathName {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PathName {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_name_round_trips_and_refuses_by_name() {
        for s in ["mesh1.rpc.2", "mesh1.admin.1", "m-2.rpc.17"] {
            assert_eq!(s.parse::<PathName>().unwrap().to_string(), s);
        }
        assert_eq!("mesh1.broker.1".parse::<PathName>(), Err(PathNameError::Kind("broker".into())));
        assert_eq!("mesh1.rpc.0".parse::<PathName>(), Err(PathNameError::Ordinal("0".into())));
        assert_eq!("mesh1.rpc".parse::<PathName>(), Err(PathNameError::Shape("mesh1.rpc".into())));
    }
}
