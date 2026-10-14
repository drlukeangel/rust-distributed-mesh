//! `path.name`: the stable topology slot `<mesh>.<kind>.<ordinal>`. The kinds are the Mesh's own
//! (`admin`, the proof `rpc`) and the product roles RDM has always named (`broker`, `gateway`,
//! `compute`); RDM's proof shapes use only the first two.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// What a node is in its mesh. The kind is the second segment of its `path.name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// A node-admin.
    NodeAdmin,
    /// A plain product node that serves core protocols only.
    RpcNode,
    /// A broker.
    Broker,
    /// A gateway.
    Gateway,
    /// A compute node.
    Compute,
}

impl NodeKind {
    /// Every kind, `node_admin` first.
    pub const ALL: [NodeKind; 5] = [NodeKind::NodeAdmin, NodeKind::RpcNode, NodeKind::Broker, NodeKind::Gateway, NodeKind::Compute];

    /// The kind's name as a mesh field (`node_admin`, `rpc_node`, `broker`, ...).
    pub fn name(self) -> &'static str {
        match self {
            Self::NodeAdmin => "node_admin",
            Self::RpcNode => "rpc_node",
            Self::Broker => "broker",
            Self::Gateway => "gateway",
            Self::Compute => "compute",
        }
    }
}

impl NodeKind {
    /// The kind's `path.name` segment: `admin`, `rpc`, `broker`, `gateway` or `compute`.
    pub fn segment(self) -> &'static str {
        match self {
            Self::NodeAdmin => "admin",
            Self::RpcNode => "rpc",
            Self::Broker => "broker",
            Self::Gateway => "gateway",
            Self::Compute => "compute",
        }
    }

    /// The kind a `path.name` segment names, `None` for any other segment.
    pub fn from_segment(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Self::NodeAdmin),
            "rpc" => Some(Self::RpcNode),
            "broker" => Some(Self::Broker),
            "gateway" => Some(Self::Gateway),
            "compute" => Some(Self::Compute),
            _ => None,
        }
    }

    /// The node's `node_type` identifier (`node-admin`, `rpc-node`, `broker`, `gateway` or `compute`).
    pub fn node_type(self) -> &'static str {
        ""
    }
}

/// A node's stable logical address `<mesh>.<kind>.<ordinal>`; `<mesh>.<kind>.<ordinal>.old` names
/// the predecessor a replacement renamed to free the path for its successor, and
/// `<mesh>.<kind>.<ordinal>.new` the successor born beside it until it takes the name
/// (node-replace.md). At most one of `old` and `new` is set: every constructor keeps it so.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathName {
    /// The mesh's name.
    pub mesh: String,
    /// The node's kind.
    pub kind: NodeKind,
    /// The node's ordinal within its mesh and kind, from 1.
    pub ordinal: u32,
    /// The path is the `.old` name of a replaced predecessor.
    pub old: bool,
    /// The path is the `.new` name of a successor that has not yet taken the name.
    pub new: bool,
}

/// Why a `path.name` was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathNameError {
    /// The text is not `<mesh>.<kind>.<ordinal>`.
    Shape(String),
    /// The mesh name does not match `[a-z0-9][a-z0-9-]{0,63}`.
    MeshName(String),
    /// The kind segment is not `admin`, `rpc`, `broker`, `gateway` or `compute`.
    Kind(String),
    /// The ordinal is not a number of at least 1.
    Ordinal(String),
    /// A fourth segment other than the one `old` suffix a replaced predecessor carries or the one
    /// `new` suffix a successor carries.
    Suffix(String),
}

impl fmt::Display for PathNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape(s) => write!(f, "`{s}` is not <mesh>.<kind>.<ordinal>"),
            Self::MeshName(s) => write!(f, "mesh name `{s}` must match [a-z0-9][a-z0-9-]{{0,63}}"),
            Self::Kind(s) => write!(f, "node kind `{s}` is not admin, rpc, broker, gateway or compute"),
            Self::Ordinal(s) => write!(f, "ordinal `{s}` is not a number >= 1"),
            Self::Suffix(s) => write!(f, "`{s}` is not a path.name: the only suffixes are one `.old`, on a replaced predecessor, and one `.new`, on its successor"),
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
    /// A `path.name` from its parts.
    pub fn new(mesh: impl Into<String>, kind: NodeKind, ordinal: u32) -> Self {
        Self { mesh: mesh.into(), kind, ordinal, old: false, new: false }
    }

    /// The name the predecessor of a replacement keeps: this path with the `.old` suffix.
    pub fn renamed(&self) -> Self {
        Self { old: true, new: false, ..self.clone() }
    }

    /// The name the successor of a replacement is born at: this path with the `.new` suffix.
    pub fn successor(&self) -> Self {
        Self { old: false, new: true, ..self.clone() }
    }

    /// This path with no suffix: the name a replacement's successor takes.
    pub fn base(&self) -> Self {
        Self { old: false, new: false, ..self.clone() }
    }

    /// The node's path: everything of the `path.name` before its name (`mesh1.rpc` of
    /// `mesh1.rpc.2.old`).
    pub fn path(&self) -> String {
        format!("{}.{}", self.mesh, self.kind.segment())
    }

    /// The node's name: the `path.name` after its path (`2.old` of `mesh1.rpc.2.old`).
    pub fn name(&self) -> String {
        let s = self.to_string();
        s[self.path().len() + 1..].to_string()
    }

    /// The `path.name` of the name `name` at this node's path: the index key `<path>.<name>`, the
    /// path never moving. Refused by name when the join is not a `path.name` this build keys.
    pub fn with_name(&self, name: &str) -> Result<Self, PathNameError> {
        format!("{}.{name}", self.path()).parse()
    }

    /// The node's `node_type` identifier (`node-admin`, `rpc-node`, `broker`, `gateway` or `compute`).
    pub fn node_type(&self) -> &'static str {
        self.kind.node_type()
    }
}

impl FromStr for PathName {
    type Err = PathNameError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        let (mesh, kind, ord, old, new) = match parts[..] {
            [mesh, kind, ord] => (mesh, kind, ord, false, false),
            [mesh, kind, ord, "old"] => (mesh, kind, ord, true, false),
            [mesh, kind, ord, "new"] => (mesh, kind, ord, false, true),
            [_, _, _, _] => return Err(PathNameError::Suffix(s.into())),
            _ => return Err(PathNameError::Shape(s.into())),
        };
        if !is_valid_mesh_name(mesh) {
            return Err(PathNameError::MeshName(mesh.into()));
        }
        let kind = NodeKind::from_segment(kind).ok_or_else(|| PathNameError::Kind(kind.into()))?;
        let ordinal = ord
            .parse::<u32>()
            .ok()
            .filter(|n| *n >= 1 && !ord.starts_with('0'))
            .ok_or_else(|| PathNameError::Ordinal(ord.into()))?;
        Ok(Self { mesh: mesh.into(), kind, ordinal, old, new })
    }
}

impl fmt::Display for PathName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.mesh, self.kind.segment(), self.ordinal)?;
        if self.old {
            f.write_str(".old")?;
        }
        if self.new {
            f.write_str(".new")?;
        }
        Ok(())
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
        for s in ["mesh1.broker.1", "mesh1.gateway.3", "mesh2.compute.2"] {
            assert_eq!(s.parse::<PathName>().unwrap().to_string(), s);
        }
        assert_eq!("mesh1.registry.1".parse::<PathName>(), Err(PathNameError::Kind("registry".into())));
        assert_eq!("mesh1.rpc.0".parse::<PathName>(), Err(PathNameError::Ordinal("0".into())));
        assert_eq!("mesh1.rpc".parse::<PathName>(), Err(PathNameError::Shape("mesh1.rpc".into())));
        // A replaced predecessor carries exactly one `.old`; anything else is refused by name.
        let old: PathName = "mesh1.rpc.2.old".parse().unwrap();
        assert!(old.old);
        assert_eq!(old, "mesh1.rpc.2".parse::<PathName>().unwrap().renamed());
        assert_ne!(old, "mesh1.rpc.2".parse::<PathName>().unwrap());
        assert_eq!(old.to_string(), "mesh1.rpc.2.old");
        assert_eq!(serde_json::to_string(&old).unwrap(), "\"mesh1.rpc.2.old\"");
        assert_eq!("mesh1.rpc.2.older".parse::<PathName>(), Err(PathNameError::Suffix("mesh1.rpc.2.older".into())));
        assert_eq!("mesh1.rpc.2.old.old".parse::<PathName>(), Err(PathNameError::Shape("mesh1.rpc.2.old.old".into())));
        // A successor born beside it carries exactly one `.new` until it takes the name.
        let new: PathName = "mesh1.rpc.2.new".parse().unwrap();
        assert!(new.new && !new.old);
        assert_eq!(new, "mesh1.rpc.2".parse::<PathName>().unwrap().successor());
        assert_eq!(new.to_string(), "mesh1.rpc.2.new");
        assert_eq!(new.base(), "mesh1.rpc.2".parse::<PathName>().unwrap());
        assert_eq!(new.renamed().to_string(), "mesh1.rpc.2.old", "one suffix at most");
        assert_eq!(old.successor().to_string(), "mesh1.rpc.2.new", "one suffix at most");
        assert_eq!("mesh1.rpc.2.new.old".parse::<PathName>(), Err(PathNameError::Shape("mesh1.rpc.2.new.old".into())));
    }

    /// CONTRACT (proposal node-properties-own-commands.md §2.1 Entry: a rename carries the name only;
    /// Luke 2026-10-12: path and name are two fields, `path.name` only their index-key join): the path
    /// is the `path.name` before its name and the name the rest; a name joined at a node's path keeps
    /// that path, and a join this build cannot key is refused by name.
    #[test]
    fn a_name_joined_at_a_path_keeps_the_path() {
        let held: PathName = "mesh1.rpc.2.old".parse().unwrap();
        assert_eq!((held.path(), held.name()), ("mesh1.rpc".to_string(), "2.old".to_string()));
        assert_eq!(held.with_name("9").unwrap().to_string(), "mesh1.rpc.9");
        assert_eq!(held.with_name("2").unwrap(), held.base());
        assert_eq!(held.with_name("rpc.2"), Err(PathNameError::Suffix("mesh1.rpc.rpc.2".into())));
        assert_eq!(held.with_name("2.older"), Err(PathNameError::Suffix("mesh1.rpc.2.older".into())));
    }
}
