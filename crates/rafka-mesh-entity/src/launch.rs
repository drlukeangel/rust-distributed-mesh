//! The launch environment node-admin hands a node it deploys, and the node
//! reads back (`docs/i143/design.md` §3). One contract, one owner.

type Result<T> = std::result::Result<T, String>;
use crate::{EndpointSlot, FreshnessToken, IncarnationId, NodeId, PathName};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

pub const ENV_FABRIC: &str = "RAFKA_FABRIC";
pub const ENV_NODE_NAME: &str = "RAFKA_NODE_NAME";
pub const ENV_NODE_ID: &str = "RAFKA_NODE_ID";
pub const ENV_INCARNATION: &str = "RAFKA_INCARNATION_ID";
pub const ENV_SUPERSEDES: &str = "RAFKA_SUPERSEDES";
pub const ENV_ENDPOINTS: &str = "RAFKA_ENDPOINTS";
pub const ENV_SEEDS: &str = "RAFKA_SEEDS";
pub const ENV_DATA_DIR: &str = "RAFKA_DATA_DIR";

/// Everything a node needs to come up as the exact node node-admin allocated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launch {
    pub fabric: String,
    pub name: PathName,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
    pub supersedes: Option<IncarnationId>,
    pub endpoints: Vec<EndpointSlot>,
    /// `(public key hex, address)` of members to join gossip through.
    pub seeds: Vec<(String, SocketAddr)>,
    pub data_dir: PathBuf,
}

/// `rpc-0=127.0.0.1:41001=<token>,rpc-1=...`
pub fn encode_endpoints(slots: &[EndpointSlot]) -> String {
    slots.iter().map(|s| format!("{}={}={}", s.slot, s.addr, s.freshness)).collect::<Vec<_>>().join(",")
}

pub fn decode_endpoints(s: &str) -> Result<Vec<EndpointSlot>> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|e| {
            let mut p = e.splitn(3, '=');
            let (Some(slot), Some(addr), Some(tok)) = (p.next(), p.next(), p.next()) else {
                return Err(format!("{ENV_ENDPOINTS} entry `{e}` is not slot=addr=token"));
            };
            Ok(EndpointSlot { slot: slot.into(), addr: addr.parse().map_err(|x| format!("address in `{e}`: {x}"))?, freshness: FreshnessToken(tok.into()) })
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
        m.insert(ENV_NODE_NAME.into(), self.name.to_string());
        m.insert(ENV_NODE_ID.into(), self.node_id.0.clone());
        m.insert(ENV_INCARNATION.into(), self.incarnation.0.clone());
        if let Some(s) = &self.supersedes {
            m.insert(ENV_SUPERSEDES.into(), s.0.clone());
        }
        m.insert(ENV_ENDPOINTS.into(), encode_endpoints(&self.endpoints));
        m.insert(ENV_SEEDS.into(), self.seeds.iter().map(|(k, a)| format!("{k}@{a}")).collect::<Vec<_>>().join(","));
        m.insert(ENV_DATA_DIR.into(), self.data_dir.display().to_string());
        m
    }

    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let req = |k: &str| get(k).filter(|v| !v.trim().is_empty()).ok_or_else(|| format!("{k} is required and must not be empty"));
        Ok(Self {
            fabric: req(ENV_FABRIC)?,
            name: req(ENV_NODE_NAME)?.parse().map_err(|e| format!("{ENV_NODE_NAME}: {e}"))?,
            node_id: NodeId(req(ENV_NODE_ID)?),
            incarnation: IncarnationId(req(ENV_INCARNATION)?),
            supersedes: get(ENV_SUPERSEDES).filter(|s| !s.is_empty()).map(IncarnationId),
            endpoints: decode_endpoints(&req(ENV_ENDPOINTS)?)?,
            seeds: decode_seeds(&get(ENV_SEEDS).unwrap_or_default())?,
            data_dir: PathBuf::from(req(ENV_DATA_DIR)?),
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
            name: "mesh1.rpc.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: Some(IncarnationId::mint()),
            endpoints: vec![EndpointSlot::assign("rpc-0", "127.0.0.1:41001".parse().unwrap())],
            seeds: vec![("abc".into(), "127.0.0.1:41000".parse().unwrap())],
            data_dir: "/tmp/x".into(),
        };
        let env = l.to_env();
        assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap(), l);
        assert!(Launch::from_env(|_| None).unwrap_err().contains("RAFKA_FABRIC is required"));
        let mut blank = env.clone();
        blank.insert(ENV_FABRIC.into(), " ".into());
        assert!(Launch::from_env(|k| blank.get(k).cloned()).unwrap_err().contains("RAFKA_FABRIC is required and must not be empty"));
    }
}
