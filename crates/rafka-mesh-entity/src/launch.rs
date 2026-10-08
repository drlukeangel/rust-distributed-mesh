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
        // An absent optional is written EMPTY, never omitted: a child process inherits its
        // launcher's environment, and an omitted variable would be the launcher's own value.
        m.insert(ENV_SUPERSEDES.into(), self.supersedes.as_ref().map(|s| s.0.clone()).unwrap_or_default());
        m.insert(ENV_TRANSPORT_ADDR.into(), self.bind_addr.to_string());
        m.insert(ENV_LAUNCHER.into(), self.launcher.as_ref().map(|l| format!("{},{},{}", l.name, l.node_id, l.incarnation.0)).unwrap_or_default());
        m.insert(ENV_LISTENERS.into(), encode_listeners(&self.listeners));
        m.insert(ENV_SEEDS.into(), self.seeds.iter().map(|(k, a)| format!("{k}@{a}")).collect::<Vec<_>>().join(","));
        m.insert(ENV_DATA_DIR.into(), self.data_dir.display().to_string());
        m.insert(ENV_MESH_ID.into(), self.mesh_id.as_ref().map(|id| id.to_string()).unwrap_or_default());
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

    /// CONTRACT: a node launched by an admin that is itself a restart inherits that admin's
    /// environment, `RDM_SUPERSEDES` among it. A launch that supersedes nothing (a birth or a
    /// re-birth at a path.name) must read back as superseding nothing, whatever it inherited.
    #[test]
    fn a_launch_that_supersedes_nothing_reads_back_as_superseding_nothing_over_an_inherited_environment() {
        let birth = Launch {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            name: "mesh2.admin.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
            launcher: None,
            seeds: vec![],
            data_dir: "/tmp/x".into(),
            mesh_id: None,
        };
        // What a child process sees: its parent's environment with the launch's variables laid over it.
        let mut child: BTreeMap<String, String> = BTreeMap::new();
        child.insert(ENV_SUPERSEDES.into(), IncarnationId::mint().0);
        child.insert(ENV_MESH_ID.into(), MeshId::mint().to_string());
        child.insert(ENV_LAUNCHER.into(), format!("mesh1.admin.1,{},{}", NodeId::mint(), IncarnationId::mint().0));
        child.extend(birth.to_env());
        assert_eq!(Launch::from_env(|k| child.get(k).cloned()).unwrap(), birth);
    }
}
