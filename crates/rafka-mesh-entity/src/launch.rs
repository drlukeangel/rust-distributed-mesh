//! The launch environment node-admin hands a node it deploys, and the node
//! reads back. One contract, one owner.

type Result<T> = std::result::Result<T, String>;
use crate::{FabricId, IncarnationId, MeshId, NodeId, PathName};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

/// The fabric's name.
pub const ENV_FABRIC: &str = "RDM_FABRIC";
/// The fabric's minted id.
pub const ENV_FABRIC_ID: &str = "RDM_FABRIC_ID";
/// The node's `path.name`.
pub const ENV_NODE_NAME: &str = "RDM_NODE_NAME";
/// The node's minted id.
pub const ENV_NODE_ID: &str = "RDM_NODE_ID";
/// The id of the node's incarnation.
pub const ENV_INCARNATION: &str = "RDM_INCARNATION_ID";
/// The incarnation this birth supersedes, when it is a restart.
pub const ENV_SUPERSEDES: &str = "RDM_SUPERSEDES";
/// The address the process's Iroh endpoint binds, with port 0.
pub const ENV_TRANSPORT_ADDR: &str = "RDM_TRANSPORT_ADDR";
/// The non-Iroh listeners the process binds, as `name=addr` entries joined by commas.
pub const ENV_LISTENERS: &str = "RDM_LISTENERS";
/// The members to join gossip through, as `<public key>@<addr>` entries joined by commas.
pub const ENV_SEEDS: &str = "RDM_SEEDS";
/// The node-admin that deployed the birth, as `<path.name>,<node id>,<incarnation id>`.
pub const ENV_LAUNCHER: &str = "RDM_LAUNCHER";
/// The node's data directory.
pub const ENV_DATA_DIR: &str = "RDM_DATA_DIR";
/// The id of the node's mesh.
pub const ENV_MESH_ID: &str = "RDM_MESH_ID";
/// The issuing material a new mesh's first node-admin is launched with, as hex of opaque bytes.
/// RDM never parses it: the embedding app's signer made it and the app's own node-admin reads it.
pub const ENV_MESH_ISSUER: &str = "RDM_MESH_ISSUER";
/// A person-started node-admin must recover its mesh: with `RDM_FABRIC_PRIMARY`, a fabric recovery.
pub const ENV_MESH_PRIMARY: &str = "RDM_MESH_PRIMARY";
/// A person-started node-admin must recover the fabric (total loss); implies `RDM_MESH_PRIMARY`.
pub const ENV_FABRIC_PRIMARY: &str = "RDM_FABRIC_PRIMARY";

/// The node-admin that deployed a birth: the target of its `JoinNode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launcher {
    /// The launching node-admin's `path.name`.
    pub name: PathName,
    /// The launching node-admin's node id.
    pub node_id: NodeId,
    /// The launching node-admin's incarnation.
    pub incarnation: IncarnationId,
}

/// Everything a node needs to come up as the exact node node-admin allocated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launch {
    /// The Fabric's name (its label) and its identity.
    pub fabric: String,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The id of this birth.
    pub incarnation: IncarnationId,
    /// The incarnation this birth supersedes, when it is a restart.
    pub supersedes: Option<IncarnationId>,
    /// Where the process's Iroh endpoint binds (gossip and Node RPC): the host address with port
    /// 0. The operating system assigns the port; the node reports the address it really bound in
    /// its `JoinNode` digest.
    pub bind_addr: SocketAddr,
    /// Where the process binds each non-Iroh listener (a node-admin's `control` HTTP API), by
    /// name: the host address with port 0, reported the same way.
    pub listeners: Vec<(String, SocketAddr)>,
    /// The node-admin that deployed this birth and answers its `JoinNode`.
    pub launcher: Option<Launcher>,
    /// `(public key hex, address)` of members to join gossip through.
    pub seeds: Vec<(String, SocketAddr)>,
    /// The node's data directory.
    pub data_dir: PathBuf,
    /// The id of the node's mesh: it names the mesh's membership channel.
    pub mesh_id: Option<MeshId>,
    /// The issuing material a new mesh's first node-admin is launched with (opaque bytes, never
    /// parsed here). `None` for every other birth and for an app configured with no certs.
    pub mesh_issuer: Option<Vec<u8>>,
}

/// Lowercase hex of `bytes`.
pub fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// The bytes `s` spells in hex; a malformed spelling is refused by name.
pub fn decode_hex(name: &str, s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(format!("{name} has an odd number of hex digits ({})", s.len()));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| s.get(i..i + 2).and_then(|p| u8::from_str_radix(p, 16).ok()).ok_or_else(|| format!("{name} is not hex at digit {i}")))
        .collect()
}

/// `1`/`true` set a primary flag; absent, empty, `0` and `false` leave it off; anything else is
/// refused by name.
pub fn decode_flag(name: &str, v: Option<String>) -> Result<bool> {
    match v.as_deref().map(str::trim) {
        None | Some("") | Some("0") | Some("false") => Ok(false),
        Some("1") | Some("true") => Ok(true),
        Some(other) => Err(format!("{name} `{other}` is not 1, true, 0 or false")),
    }
}


/// `control=127.0.0.1:41002,...`
pub fn encode_listeners(listeners: &[(String, SocketAddr)]) -> String {
    listeners.iter().map(|(n, a)| format!("{n}={a}")).collect::<Vec<_>>().join(",")
}

/// Decode `name=addr` entries joined by commas; an entry that is not `name=addr` is refused by
/// name.
pub fn decode_listeners(s: &str) -> Result<Vec<(String, SocketAddr)>> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|e| {
            let (n, a) = e.split_once('=').ok_or_else(|| format!("{ENV_LISTENERS} entry `{e}` is not name=addr"))?;
            Ok((n.to_string(), a.parse().map_err(|x| format!("address in `{e}`: {x}"))?))
        })
        .collect()
}

/// `<path.name>,<node id>,<incarnation id>`
pub fn decode_launcher(s: &str) -> Result<Launcher> {
    let mut it = s.split(',');
    let (Some(name), Some(node_id), Some(incarnation), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(format!("{ENV_LAUNCHER} `{s}` is not name,node_id,incarnation"));
    };
    Ok(Launcher {
        name: name.parse().map_err(|e| format!("{ENV_LAUNCHER} name: {e}"))?,
        node_id: NodeId::parse(node_id).map_err(|e| format!("{ENV_LAUNCHER} node id: {e}"))?,
        incarnation: IncarnationId(incarnation.to_string()),
    })
}

/// `<public key>@<addr>,...`
pub fn decode_seeds(s: &str) -> Result<Vec<(String, SocketAddr)>> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|e| {
            let (k, a) = e.split_once('@').ok_or_else(|| format!("{ENV_SEEDS} entry `{e}` is not key@addr"))?;
            Ok((k.to_string(), a.parse().map_err(|x| format!("address in `{e}`: {x}"))?))
        })
        .collect()
}

impl Launch {
    /// The launch as the environment a launched process reads, one entry per `ENV_*` variable that
    /// applies.
    pub fn to_env(&self) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert(ENV_FABRIC.into(), self.fabric.clone());
        m.insert(ENV_FABRIC_ID.into(), self.fabric_id.to_string());
        m.insert(ENV_NODE_NAME.into(), self.name.to_string());
        m.insert(ENV_NODE_ID.into(), self.node_id.to_string());
        m.insert(ENV_INCARNATION.into(), self.incarnation.0.clone());
        if let Some(s) = &self.supersedes {
            m.insert(ENV_SUPERSEDES.into(), s.0.clone());
        }
        m.insert(ENV_TRANSPORT_ADDR.into(), self.bind_addr.to_string());
        if let Some(l) = &self.launcher {
            m.insert(ENV_LAUNCHER.into(), format!("{},{},{}", l.name, l.node_id, l.incarnation.0));
        }
        m.insert(ENV_LISTENERS.into(), encode_listeners(&self.listeners));
        m.insert(ENV_SEEDS.into(), self.seeds.iter().map(|(k, a)| format!("{k}@{a}")).collect::<Vec<_>>().join(","));
        m.insert(ENV_DATA_DIR.into(), self.data_dir.display().to_string());
        if let Some(id) = &self.mesh_id {
            m.insert(ENV_MESH_ID.into(), id.to_string());
        }
        if let Some(issuer) = self.mesh_issuer.as_ref().filter(|b| !b.is_empty()) {
            m.insert(ENV_MESH_ISSUER.into(), encode_hex(issuer));
        }
        m
    }

    /// Read a launch from the environment `get` reads; a required variable that is absent, empty or
    /// malformed is refused by name.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let req = |k: &str| get(k).filter(|v| !v.trim().is_empty()).ok_or_else(|| format!("{k} is required and must not be empty"));
        Ok(Self {
            fabric: req(ENV_FABRIC)?,
            fabric_id: FabricId::parse(&req(ENV_FABRIC_ID)?).map_err(|e| format!("{ENV_FABRIC_ID}: {e}"))?,
            name: req(ENV_NODE_NAME)?.parse().map_err(|e| format!("{ENV_NODE_NAME}: {e}"))?,
            node_id: NodeId::parse(&req(ENV_NODE_ID)?).map_err(|e| format!("{ENV_NODE_ID}: {e}"))?,
            incarnation: IncarnationId(req(ENV_INCARNATION)?),
            supersedes: get(ENV_SUPERSEDES).filter(|s| !s.is_empty()).map(IncarnationId),
            bind_addr: req(ENV_TRANSPORT_ADDR)?.parse().map_err(|e| format!("{ENV_TRANSPORT_ADDR}: {e}"))?,
            listeners: decode_listeners(&get(ENV_LISTENERS).unwrap_or_default())?,
            launcher: get(ENV_LAUNCHER).filter(|v| !v.trim().is_empty()).map(|v| decode_launcher(&v)).transpose()?,
            seeds: decode_seeds(&get(ENV_SEEDS).unwrap_or_default())?,
            data_dir: PathBuf::from(req(ENV_DATA_DIR)?),
            mesh_id: get(ENV_MESH_ID).filter(|s| !s.trim().is_empty()).map(|s| MeshId::parse(&s).map_err(|e| format!("{ENV_MESH_ID}: {e}"))).transpose()?,
            mesh_issuer: get(ENV_MESH_ISSUER).filter(|s| !s.trim().is_empty()).map(|s| decode_hex(ENV_MESH_ISSUER, s.trim())).transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_round_trips_through_the_environment() {
        let l = Launch {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            name: "mesh1.rpc.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: Some(IncarnationId::mint()),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
            launcher: Some(Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() }),
            seeds: vec![("abc".into(), "127.0.0.1:41000".parse().unwrap())],
            data_dir: "/tmp/x".into(),
            mesh_id: Some(MeshId::mint()),
            mesh_issuer: Some(vec![0x00, 0xab, 0xff]),
        };
        let env = l.to_env();
        assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap(), l);
        assert!(Launch::from_env(|_| None).unwrap_err().contains("RDM_FABRIC is required"));
        let mut blank = env.clone();
        blank.insert(ENV_FABRIC.into(), " ".into());
        assert!(Launch::from_env(|k| blank.get(k).cloned()).unwrap_err().contains("RDM_FABRIC is required and must not be empty"));
        assert_eq!(env.get(ENV_MESH_ISSUER).map(String::as_str), Some("00abff"));
        let mut odd = env.clone();
        odd.insert(ENV_MESH_ISSUER.into(), "abc".into());
        assert!(Launch::from_env(|k| odd.get(k).cloned()).unwrap_err().starts_with("RDM_MESH_ISSUER has an odd number"));
        odd.insert(ENV_MESH_ISSUER.into(), "zz".into());
        assert!(Launch::from_env(|k| odd.get(k).cloned()).unwrap_err().starts_with("RDM_MESH_ISSUER is not hex"));
        // A launch that carries no issuer sets no variable.
        let none = Launch { mesh_issuer: None, ..l.clone() }.to_env();
        assert!(!none.contains_key(ENV_MESH_ISSUER));
        // A launched node's product ids are canonical or refused by name.
        for (k, v, says) in [(ENV_NODE_ID, "f".repeat(32), "node id"), (ENV_FABRIC_ID, "fabric1".into(), "fabric id"), (ENV_MESH_ID, "4f14".into(), "mesh id")] {
            let mut bad = env.clone();
            bad.insert(k.into(), v);
            let e = Launch::from_env(|x| bad.get(x).cloned()).unwrap_err();
            assert!(e.starts_with(k) && e.contains(says), "{e}");
        }
    }
}
