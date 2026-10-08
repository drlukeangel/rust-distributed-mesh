//! The launch environment node-admin hands a node it deploys, and the node
//! reads back (`docs/i143/design.md` §3). One contract, one owner.

type Result<T> = std::result::Result<T, String>;
use crate::{FabricId, IncarnationId, MeshId, NodeId, PathName};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

pub const ENV_FABRIC: &str = "RDM_FABRIC";
pub const ENV_FABRIC_ID: &str = "RDM_FABRIC_ID";
pub const ENV_NODE_NAME: &str = "RDM_NODE_NAME";
pub const ENV_NODE_ID: &str = "RDM_NODE_ID";
pub const ENV_INCARNATION: &str = "RDM_INCARNATION_ID";
pub const ENV_SUPERSEDES: &str = "RDM_SUPERSEDES";
pub const ENV_TRANSPORT_ADDR: &str = "RDM_TRANSPORT_ADDR";
pub const ENV_LISTENERS: &str = "RDM_LISTENERS";
pub const ENV_SEEDS: &str = "RDM_SEEDS";
pub const ENV_LAUNCHER: &str = "RDM_LAUNCHER";
pub const ENV_DATA_DIR: &str = "RDM_DATA_DIR";
pub const ENV_MESH_ID: &str = "RDM_MESH_ID";
/// This node-admin starts as its mesh's primary node-admin: a recovering mesh's first admin.
pub const ENV_MESH_PRIMARY: &str = "RDM_MESH_PRIMARY";
/// This node-admin starts as the fabric primary: total-loss recovery.
pub const ENV_FABRIC_PRIMARY: &str = "RDM_FABRIC_PRIMARY";

/// The node-admin that deployed a birth: the target of its `JoinNode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launcher {
    pub name: PathName,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
}

/// Everything a node needs to come up as the exact node node-admin allocated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launch {
    /// The Fabric's name (its label) and its identity.
    pub fabric: String,
    pub fabric_id: FabricId,
    pub name: PathName,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
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
    pub data_dir: PathBuf,
    /// The id of the node's mesh: it names the mesh's membership channel.
    pub mesh_id: Option<MeshId>,
    /// This node-admin must recover its mesh (`RDM_MESH_PRIMARY`): it holds Ready until it holds
    /// the current topology, `Fabric.build_id` and its Build. Not a seat: the election decides.
    pub mesh_primary: bool,
    /// This node-admin must recover the fabric (`RDM_FABRIC_PRIMARY`). Implies `mesh_primary`.
    pub fabric_primary: bool,
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
        if self.mesh_primary {
            m.insert(ENV_MESH_PRIMARY.into(), "1".into());
        }
        if self.fabric_primary {
            m.insert(ENV_FABRIC_PRIMARY.into(), "1".into());
        }
        m
    }

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
            mesh_primary: decode_flag(ENV_MESH_PRIMARY, get(ENV_MESH_PRIMARY))?,
            fabric_primary: decode_flag(ENV_FABRIC_PRIMARY, get(ENV_FABRIC_PRIMARY))?,
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
            mesh_primary: false,
            fabric_primary: false,
        };
        let env = l.to_env();
        assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap(), l);
        assert!(Launch::from_env(|_| None).unwrap_err().contains("RDM_FABRIC is required"));
        let mut blank = env.clone();
        blank.insert(ENV_FABRIC.into(), " ".into());
        assert!(Launch::from_env(|k| blank.get(k).cloned()).unwrap_err().contains("RDM_FABRIC is required and must not be empty"));
        // A launched node's product ids are canonical or refused by name.
        for (k, v, says) in [(ENV_NODE_ID, "f".repeat(32), "node id"), (ENV_FABRIC_ID, "fabric1".into(), "fabric id"), (ENV_MESH_ID, "4f14".into(), "mesh id")] {
            let mut bad = env.clone();
            bad.insert(k.into(), v);
            let e = Launch::from_env(|x| bad.get(x).cloned()).unwrap_err();
            assert!(e.starts_with(k) && e.contains(says), "{e}");
        }
    }

    #[test]
    fn primary_flags_round_trip_and_an_unreadable_flag_is_refused_by_name() {
        let mut l = Launch::from_env(|k| {
            Some(match k {
                ENV_FABRIC => "fabric1".into(),
                ENV_FABRIC_ID => FabricId::mint().to_string(),
                ENV_NODE_NAME => "mesh1.admin.1".into(),
                ENV_NODE_ID => NodeId::mint().to_string(),
                ENV_INCARNATION => IncarnationId::mint().0,
                ENV_TRANSPORT_ADDR => "127.0.0.1:0".into(),
                ENV_DATA_DIR => "/tmp/x".into(),
                _ => return None,
            })
        })
        .unwrap();
        assert!(!l.mesh_primary && !l.fabric_primary && !l.to_env().contains_key(ENV_MESH_PRIMARY));
        l.mesh_primary = true;
        l.fabric_primary = true;
        let env = l.to_env();
        assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap(), l);
        let mut bad = env.clone();
        bad.insert(ENV_MESH_PRIMARY.into(), "yes".into());
        assert!(Launch::from_env(|k| bad.get(k).cloned()).unwrap_err().starts_with(ENV_MESH_PRIMARY));
    }
}
