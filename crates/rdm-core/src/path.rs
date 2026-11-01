//! Separate node path and name, and explicit runtime kinds.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The runtime kind explicitly selected for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// A node-admin.
    ///
    /// Position 0 on the wire.
    NodeAdmin,
    /// A plain product node that serves core protocols only.
    ///
    /// Position 1 on the wire.
    RpcNode,
    /// A broker.
    ///
    /// Position 2 on the wire.
    Broker,
    /// A gateway.
    ///
    /// Position 3 on the wire.
    Gateway,
    /// A compute node.
    ///
    /// Position 4 on the wire.
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
    /// The conventional path component generated for the explicit kind: `admin`, `rpc`, `broker`, `gateway` or `compute`.
    pub fn segment(self) -> &'static str {
        match self {
            Self::NodeAdmin => "admin",
            Self::RpcNode => "rpc",
            Self::Broker => "broker",
            Self::Gateway => "gateway",
            Self::Compute => "compute",
        }
    }

    /// The kind an explicit kind token names, `None` for any other segment.
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
        match self {
            Self::NodeAdmin => "node-admin",
            Self::RpcNode => "rpc-node",
            Self::Broker => "broker",
            Self::Gateway => "gateway",
            Self::Compute => "compute",
        }
    }
}

/// A structured location containing two separate properties: path says where the node is,
/// and name is the node's name. This is a composite lookup index, never node identity.
/// Code uses globally unique NodeId first. Neither property is parsed for mesh, type or ordinal.
/// There is deliberately no joined-string parser or formatter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "PathFields")]
pub struct PathName {
    /// The node's immutable free-text path.
    pub path: String,
    /// The dot-free name within the path.
    pub name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PathFields { path: String, name: String }

/// Why a path/name pair was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathNameError {
    /// The path contains an empty or invalid segment.
    Path(String),
    /// The name is empty, too long or contains an invalid character.
    Name(String),
}

impl fmt::Display for PathNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(s) => write!(f, "path `{s}` must contain nonempty lowercase, digit, -, _ or ~ segments"),
            Self::Name(s) => write!(f, "name `{s}` must contain 1 to 64 lowercase, digit, -, _ or ~ characters and no dot"),
        }
    }
}

impl std::error::Error for PathNameError {}

fn valid_segment(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64
        && s.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'-' | b'_' | b'~'))
}

impl TryFrom<PathFields> for PathName {
    type Error = PathNameError;
    fn try_from(p: PathFields) -> Result<Self, Self::Error> { Self::new(p.path, p.name) }
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
    /// The pair a caller asked for, decoded unchecked: the deserializer of a request field whose
    /// receiver refuses an invalid pair by name (node-name-update.md Validation: `RejectedName {
    /// reason }`), never a decode failure. Every other decode checks the pair ([`Self::new`]).
    pub fn deserialize_asked<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let PathFields { path, name } = PathFields::deserialize(d)?;
        Ok(Self { path, name })
    }

    /// Validate the two fields at ingress.
    pub fn new(path: impl Into<String>, name: impl Into<String>) -> Result<Self, PathNameError> {
        let (path, name) = (path.into(), name.into());
        if !path.split('.').all(valid_segment) { return Err(PathNameError::Path(path)); }
        if !valid_segment(&name) { return Err(PathNameError::Name(name)); }
        Ok(Self { path, name })
    }

    /// Opaque physical key for a composite location index. This is not a node identifier,
    /// reference or display value; records retain both properties and are keyed by NodeId.
    /// Fixed-width lengths keep the two exact UTF-8 fields distinct before hashing.
    pub fn physical_index_key(&self) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(b"rdm-node-location-index/1\0");
        for field in [&self.path, &self.name] {
            hash.update(&(field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        format!("node-location-{}", hash.finalize().to_hex())
    }

    /// The conventional name minted by the counts convenience API. Ownership is supplied
    /// independently wherever the resulting pair is used.
    pub fn numbered(mesh: impl Into<String>, kind: NodeKind, ordinal: u32) -> Self {
        Self::new(format!("{}.{}", mesh.into(), kind.segment()), ordinal.to_string())
            .expect("count expansion supplies a valid path and name")
    }

    /// The replacement predecessor's reserved name, preserving its immutable path.
    /// Fails when appending `-old` exceeds the name limit.
    pub fn renamed(&self) -> Result<Self, PathNameError> { self.with_name(&format!("{}-old", self.name)) }

    /// The path component.
    pub fn path(&self) -> String { self.path.clone() }

    /// The name component.
    pub fn name(&self) -> String { self.name.clone() }

    /// Rename within the same path.
    pub fn with_name(&self, name: &str) -> Result<Self, PathNameError> { Self::new(&self.path, name) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_name_checks_limit_without_restricting_ordinary_names() {
        let largest = PathName::new("apps", "a".repeat(60)).unwrap();
        let retained = largest.renamed().unwrap();
        assert_eq!(retained.path, largest.path);
        assert_eq!(retained.name.len(), 64);
        assert_eq!(serde_json::from_value::<PathName>(serde_json::to_value(&retained).unwrap()).unwrap(), retained);
        for length in 61..=64 {
            let valid = PathName::new("apps", "a".repeat(length)).unwrap();
            assert!(matches!(valid.renamed(), Err(PathNameError::Name(_))));
            assert_eq!(serde_json::from_value::<PathName>(serde_json::to_value(&valid).unwrap()).unwrap(), valid);
        }
    }

    /// CONTRACT: path and name are stored separately and do not encode mesh or type.
    #[test]
    fn separate_path_and_name_survive_serialization() {
        let pair = PathName::new("production.east.apps", "north-1").unwrap();
        assert!(serde_json::from_value::<PathName>(serde_json::json!("production.east.apps.north-1")).is_err(), "a joined string is not a node location");
        let json = serde_json::to_value(&pair).unwrap();
        assert_eq!(json, serde_json::json!({"path":"production.east.apps", "name":"north-1"}));
        assert_eq!(serde_json::from_value::<PathName>(json).unwrap(), pair);
        assert_eq!(pair.renamed().unwrap(), PathName::new("production.east.apps", "north-1-old").unwrap());
        assert_eq!(pair.with_name("another").unwrap().path, pair.path);
        assert!(pair.with_name("another.name").is_err());
        assert!(serde_json::from_str::<PathName>(r#""production.east.apps.north-1""#).is_err());
        for (path, name) in [("", "a"), ("bad..path", "a"), ("Upper", "a"), ("valid", ""), ("valid", "has.dot")] {
            assert!(PathName::new(path, name).is_err(), "{path}/{name}");
        }
    }

    /// CONTRACT (i150 R1, e1; node-identity-and-state.md "A path is a path": "A path has dotted
    /// free-text segments with no mesh or type encoded in their meaning ... Replacement renames the
    /// predecessor to `<name>-old` at the same path"): the pair is two fields in every shape that
    /// carries it, and each shape gives back the same two fields: the pair's own JSON, an operation's
    /// `Location` subject, the node.get `path`/`name` filter and the launch environment's
    /// `RDM_NODE_PATH`/`RDM_NODE_NAME`. A free-text path is never parsed for a mesh, a kind or an
    /// ordinal; the dot-free name is refused by name when it holds a dot, is empty or is too long; the
    /// replacement move keeps the path and changes only the name. Must NOT happen: a joined string read
    /// as a pair, or a pair whose path and name trade bytes across the dot.
    #[test]
    fn separate_path_and_name_read_back_as_written() {
        let pairs = [
            PathName::new("mesh1.rpc", "north").unwrap(),
            PathName::new("production.east.apps", "north-1").unwrap(),
            PathName::new("apps", "1").unwrap(),
            PathName::new("a.b", "c").unwrap(),
            PathName::new("a", "b.c".replace('.', "~")).unwrap(),
            PathName::numbered("mesh2", NodeKind::Broker, 7),
        ];
        // Two pairs whose joined string would be the same bytes stay two pairs.
        assert_ne!(PathName::new("a.b", "c").unwrap(), PathName::new("a", "b~c").unwrap());
        assert_ne!(PathName::new("a.b", "c").unwrap().physical_index_key(), PathName::new("a", "bc").unwrap().physical_index_key());
        for pair in &pairs {
            let json = serde_json::to_value(pair).unwrap();
            assert_eq!(json, serde_json::json!({"path": pair.path, "name": pair.name}), "the pair is two fields: {json}");
            assert_eq!(serde_json::from_value::<PathName>(json).unwrap(), *pair);
            // An operation's location subject carries the same two fields.
            let op = crate::OperationId::location("node.create", pair);
            let back: crate::OperationId = serde_json::from_value(serde_json::to_value(&op).unwrap()).unwrap();
            assert_eq!(back.subject, Some(crate::OperationSubject::Location(pair.clone())), "the Location subject round-trips: {back:?}");
            // The node.get filter selects by the two fields, and neither alone is the pair.
            let filter = crate::filter::Filter::new().eq("path", [pair.path.clone()]).eq("name", [pair.name.clone()]);
            assert_eq!(filter.refusal(crate::filter::Entity::Nodes), None, "path and name are node.get fields");
            let filter: crate::filter::Filter = serde_json::from_value(serde_json::to_value(&filter).unwrap()).unwrap();
            assert_eq!(filter, crate::filter::Filter::new().eq("path", [pair.path.clone()]).eq("name", [pair.name.clone()]), "the filter's two pairs round-trip");
            // The launch environment carries the two fields under their own names.
            let env = std::collections::BTreeMap::from([
                (crate::launch::ENV_FABRIC.to_string(), "fabric1".to_string()),
                (crate::launch::ENV_FABRIC_ID.to_string(), crate::FabricId::mint().to_string()),
                (crate::launch::ENV_NODE_PATH.to_string(), pair.path.clone()),
                (crate::launch::ENV_NODE_NAME.to_string(), pair.name.clone()),
                (crate::launch::ENV_MESH.to_string(), "mesh1".to_string()),
                (crate::launch::ENV_NODE_KIND.to_string(), "rpc_node".to_string()),
                (crate::launch::ENV_NODE_ID.to_string(), crate::NodeId::mint().to_string()),
                (crate::launch::ENV_INCARNATION.to_string(), crate::IncarnationId::mint().to_string()),
                (crate::launch::ENV_TRANSPORT_ADDR.to_string(), "127.0.0.1:0".to_string()),
                (crate::launch::ENV_DATA_DIR.to_string(), "/tmp/x".to_string()),
                (crate::launch::ENV_STORE_DIR.to_string(), "/tmp/store".to_string()),
            ]);
            let launch = crate::launch::Launch::from_env(|k| env.get(k).cloned()).expect("a launch naming the pair");
            assert_eq!(launch.name, *pair, "the launch reads the pair from its two variables");
            let written = launch.to_env();
            assert_eq!((written[crate::launch::ENV_NODE_PATH].as_str(), written[crate::launch::ENV_NODE_NAME].as_str()), (pair.path.as_str(), pair.name.as_str()), "the launch writes the two variables back unjoined");
            assert_eq!(crate::launch::Launch::from_env(|k| written.get(k).cloned()).unwrap().name, *pair);
            // The replacement move keeps the path and changes only the name.
            let moved = pair.renamed().unwrap();
            assert_eq!((moved.path.as_str(), moved.name.clone()), (pair.path.as_str(), format!("{}-old", pair.name)), "the predecessor moves to <name>-old at the same path");
            assert_eq!(serde_json::from_value::<PathName>(serde_json::to_value(&moved).unwrap()).unwrap(), moved);
        }
        // The path is free text: nothing reads a mesh, a kind or an ordinal out of it.
        let free = PathName::new("not.a.mesh.path", "x").unwrap();
        assert_eq!((free.path(), free.name()), ("not.a.mesh.path".to_string(), "x".to_string()));
        // Grammar: refused by name, the refused field named.
        for (path, name, field) in [("", "a", "path"), ("bad..path", "a", "path"), ("Upper", "a", "path"), (".lead", "a", "path"), ("trail.", "a", "path"), ("valid", "", "name"), ("valid", "has.dot", "name"), ("valid", "Upper", "name")] {
            match (PathName::new(path, name), field) {
                (Err(PathNameError::Path(p)), "path") => assert_eq!(p, path),
                (Err(PathNameError::Name(n)), "name") => assert_eq!(n, name),
                (other, _) => panic!("{path}/{name}: refused naming the {field}, not {other:?}"),
            }
        }
        assert!(matches!(PathName::new("valid", "a".repeat(65)), Err(PathNameError::Name(_))), "a name over 64 characters is refused");
        // A decode is checked: an invalid pair, an extra field or a joined string is no pair.
        for bad in [serde_json::json!({"path": "valid", "name": "has.dot"}), serde_json::json!({"path": "valid", "name": "a", "mesh": "mesh1"}), serde_json::json!("mesh1.rpc.north"), serde_json::json!({"path": "mesh1.rpc"})] {
            assert!(serde_json::from_value::<PathName>(bad.clone()).is_err(), "{bad} is not a pair");
        }
        let env_err = crate::launch::Launch::from_env(|k| match k {
            crate::launch::ENV_NODE_PATH => Some("mesh1.rpc".into()),
            crate::launch::ENV_NODE_NAME => Some("has.dot".into()),
            crate::launch::ENV_FABRIC => Some("fabric1".into()),
            crate::launch::ENV_FABRIC_ID => Some(crate::FabricId::mint().to_string()),
            _ => None,
        })
        .unwrap_err();
        assert!(env_err.starts_with(&format!("{}/{}", crate::launch::ENV_NODE_PATH, crate::launch::ENV_NODE_NAME)), "the launch refuses the pair by its two variables' names: {env_err}");
    }
}
