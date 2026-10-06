//! The launch environment node-admin hands a node it deploys, and the node
//! reads back (`docs/i143/design.md` §3). One contract, one owner.

type Result<T> = std::result::Result<T, String>;
use crate::{EndpointSlot, FabricId, FreshnessToken, IncarnationId, MeshId, NodeId, PathName};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

pub const ENV_FABRIC: &str = "RAFKA_FABRIC";
pub const ENV_FABRIC_ID: &str = "RAFKA_FABRIC_ID";
pub const ENV_NODE_NAME: &str = "RAFKA_NODE_NAME";
pub const ENV_NODE_ID: &str = "RAFKA_NODE_ID";
pub const ENV_INCARNATION: &str = "RAFKA_INCARNATION_ID";
pub const ENV_SUPERSEDES: &str = "RAFKA_SUPERSEDES";
pub const ENV_TRANSPORT_ADDR: &str = "RAFKA_TRANSPORT_ADDR";
pub const ENV_SLOTS: &str = "RAFKA_SLOTS";
pub const ENV_LISTENERS: &str = "RAFKA_LISTENERS";
pub const ENV_SEEDS: &str = "RAFKA_SEEDS";
pub const ENV_DATA_DIR: &str = "RAFKA_DATA_DIR";
pub const ENV_MESH_ID: &str = "RAFKA_MESH_ID";

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
    /// The one address the process's Iroh endpoint binds: gossip and Node RPC.
    pub transport_addr: SocketAddr,
    /// The logical Node RPC slots the process serves, under their tokens.
    pub endpoints: Vec<EndpointSlot>,
    /// Non-Iroh listeners the process binds (a node-admin's `control` HTTP API), by name.
    pub listeners: Vec<(String, SocketAddr)>,
    /// `(public key hex, address)` of members to join gossip through.
    pub seeds: Vec<(String, SocketAddr)>,
    pub data_dir: PathBuf,
    /// The id of the node's mesh: it names the mesh's membership channel.
    pub mesh_id: Option<MeshId>,
}

/// `rpc-0=<token>,rpc-1=<token>`
pub fn encode_slots(slots: &[EndpointSlot]) -> String {
    slots.iter().map(|s| format!("{}={}", s.slot, s.freshness)).collect::<Vec<_>>().join(",")
}

pub fn decode_slots(s: &str) -> Result<Vec<EndpointSlot>> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|e| {
            let (slot, tok) = e.split_once('=').ok_or_else(|| format!("{ENV_SLOTS} entry `{e}` is not slot=token"))?;
            Ok(EndpointSlot { slot: slot.into(), freshness: FreshnessToken(tok.into()) })
        })
        .collect()
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
        m.insert(ENV_TRANSPORT_ADDR.into(), self.transport_addr.to_string());
        m.insert(ENV_SLOTS.into(), encode_slots(&self.endpoints));
        m.insert(ENV_LISTENERS.into(), encode_listeners(&self.listeners));
        m.insert(ENV_SEEDS.into(), self.seeds.iter().map(|(k, a)| format!("{k}@{a}")).collect::<Vec<_>>().join(","));
        m.insert(ENV_DATA_DIR.into(), self.data_dir.display().to_string());
        if let Some(id) = &self.mesh_id {
            m.insert(ENV_MESH_ID.into(), id.to_string());
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
            transport_addr: req(ENV_TRANSPORT_ADDR)?.parse().map_err(|e| format!("{ENV_TRANSPORT_ADDR}: {e}"))?,
            endpoints: decode_slots(&get(ENV_SLOTS).unwrap_or_default())?,
            listeners: decode_listeners(&get(ENV_LISTENERS).unwrap_or_default())?,
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
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            endpoints: vec![EndpointSlot::fresh("rpc-0"), EndpointSlot::fresh("rpc-1")],
            listeners: vec![("control".into(), "127.0.0.1:41002".parse().unwrap())],
            seeds: vec![("abc".into(), "127.0.0.1:41000".parse().unwrap())],
            data_dir: "/tmp/x".into(),
            mesh_id: Some(MeshId::mint()),
        };
        let env = l.to_env();
        assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap(), l);
        assert!(Launch::from_env(|_| None).unwrap_err().contains("RAFKA_FABRIC is required"));
        let mut blank = env.clone();
        blank.insert(ENV_FABRIC.into(), " ".into());
        assert!(Launch::from_env(|k| blank.get(k).cloned()).unwrap_err().contains("RAFKA_FABRIC is required and must not be empty"));
        // A launched node's product ids are canonical or refused by name.
        for (k, v, says) in [(ENV_NODE_ID, "f".repeat(32), "node id"), (ENV_FABRIC_ID, "fabric1".into(), "fabric id"), (ENV_MESH_ID, "4f14".into(), "mesh id")] {
            let mut bad = env.clone();
            bad.insert(k.into(), v);
            let e = Launch::from_env(|x| bad.get(x).cloned()).unwrap_err();
            assert!(e.starts_with(k) && e.contains(says), "{e}");
        }
    }
}
