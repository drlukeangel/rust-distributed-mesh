//! The launch environment node-admin hands a node it deploys, and the node
//! reads back. One contract, one owner.

type Result<T> = std::result::Result<T, String>;
use crate::{FabricId, IncarnationId, MeshId, NodeId, PathName};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

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
/// The fabric's shared store root every node of the fabric reaches (R14): its stores live under
/// `<store_dir>/<fabric id>/`.
pub const ENV_STORE_DIR: &str = "RDM_STORE_DIR";
/// The id of the node's mesh.
pub const ENV_MESH_ID: &str = "RDM_MESH_ID";
/// A launched node-admin's mesh issuing key (fabric-certs.md "Where a node-admin's signing material
/// comes from"), the ed25519 secret as hex. A secret: it travels only in a launch's environment.
pub const ENV_ISSUING_KEY: &str = "RDM_ISSUING_KEY";
/// A launched node-admin's mesh issuing cert, signed by the fabric's root, as hex.
pub const ENV_ISSUING_CERT: &str = "RDM_ISSUING_CERT";
/// The fabric's root, the ed25519 secret as hex (fabric-certs.md §Custody): given to the bootstrap
/// node-admin, and written into every node-admin's launch beside its mesh's issuing material by a
/// launcher that holds it. A secret: it travels only in a launch's environment.
pub const ENV_CA_SECRET: &str = "RDM_CA_SECRET";
/// A person-started node-admin must recover its mesh: with `RDM_FABRIC_PRIMARY`, a fabric recovery.
pub const ENV_MESH_PRIMARY: &str = "RDM_MESH_PRIMARY";
/// A person-started node-admin must recover the fabric (total loss); implies `RDM_MESH_PRIMARY`.
pub const ENV_FABRIC_PRIMARY: &str = "RDM_FABRIC_PRIMARY";
/// The bound of a `node.connections.delete { All }` at the node, standalone or inside a stop
/// (node-connections-delete.md step 5), in whole milliseconds.
pub const ENV_CONNECTIONS_DELETE_DEADLINE: &str = "RDM_CONNECTIONS_DELETE_DEADLINE_MS";
/// The bound of a `node.connections.delete { All }` when the launch environment names none.
pub const DEFAULT_CONNECTIONS_DELETE_DEADLINE: Duration = Duration::from_millis(5000);

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

/// A secret carried in a launch's environment, as hex: its debug form names its length, never its value.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretHex(pub String);

impl std::fmt::Debug for SecretHex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretHex({} hex digits)", self.0.len())
    }
}

/// The issuing material a launched node-admin signs its mesh's member certs with (`RDM_ISSUING_KEY`,
/// `RDM_ISSUING_CERT`): the issuing key's secret and its root-signed cert, each as hex; and beside
/// them the fabric's root (`RDM_CA_SECRET`) when its launcher holds it (fabric-certs.md §Custody).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuingMaterial {
    /// The issuing key's ed25519 secret.
    pub key: SecretHex,
    /// The issuing cert, signed by the fabric's root.
    pub cert: String,
    /// The fabric's root, written beside them: every node-admin RDM launches holds it (fabric-certs.md
    /// §Custody).
    pub root: SecretHex,
}

impl IssuingMaterial {
    /// The material of `key` with the fabric's `root` beside it, as a launch writes it.
    pub fn of(key: &crate::certs::IssuingKey, root: &crate::certs::SigningKey) -> Self {
        Self { key: SecretHex(crate::certs::encode_key(key.secret())), cert: crate::certs::encode_issuing_cert(key.cert()), root: SecretHex(crate::certs::encode_key(root)) }
    }
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
    /// The fabric's shared store root (R14).
    pub store_dir: PathBuf,
    /// The id of the node's mesh: it names the mesh's membership channel.
    pub mesh_id: Option<MeshId>,
    /// The issuing material of its mesh a node-admin is launched with. `None` for every other birth
    /// and for a launcher configured with no certs.
    pub issuing: Option<IssuingMaterial>,
    /// The bound of a `node.connections.delete { All }` at this birth, standalone or inside a stop
    /// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`): the eligible work in flight is let finish until it
    /// passes, then the closure answers `Deadline`.
    pub connections_delete_deadline: Duration,
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


/// The connections-delete bound `raw` spells in whole milliseconds; absent or blank is
/// [`DEFAULT_CONNECTIONS_DELETE_DEADLINE`]; anything else is refused by name.
pub fn decode_connections_delete_deadline(raw: Option<&str>) -> Result<Duration> {
    match raw.map(str::trim) {
        None | Some("") => Ok(DEFAULT_CONNECTIONS_DELETE_DEADLINE),
        Some(v) => v.parse::<u64>().map(Duration::from_millis).map_err(|e| format!("{ENV_CONNECTIONS_DELETE_DEADLINE} `{v}` is not a whole number of milliseconds: {e}")),
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

/// The issuing material `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT` carry: both, or neither, with the
/// root `RDM_CA_SECRET` carries beside them. One without the other, or any of them not hex, is
/// refused by name. A root with no issuing material is no issuing material: it is the bootstrap
/// admin's, read by its cert choice.
pub fn decode_issuing(key: Option<String>, cert: Option<String>, root: Option<String>) -> Result<Option<IssuingMaterial>> {
    let present = |v: Option<String>| v.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    match (present(key), present(cert)) {
        (None, None) => Ok(None),
        (Some(key), Some(cert)) => {
            decode_hex(ENV_ISSUING_KEY, &key)?;
            decode_hex(ENV_ISSUING_CERT, &cert)?;
            let Some(root) = present(root) else {
                return Err(format!("{ENV_ISSUING_KEY} and {ENV_ISSUING_CERT} are set without {ENV_CA_SECRET}: a node-admin is launched with the fabric's root beside its mesh's issuing material (fabric-certs.md §Custody)"));
            };
            decode_hex(ENV_CA_SECRET, &root)?;
            Ok(Some(IssuingMaterial { key: SecretHex(key), cert, root: SecretHex(root) }))
        }
        (Some(_), None) => Err(format!("{ENV_ISSUING_KEY} is set without {ENV_ISSUING_CERT}: issuing material is the key and its cert together")),
        (None, Some(_)) => Err(format!("{ENV_ISSUING_CERT} is set without {ENV_ISSUING_KEY}: issuing material is the key and its cert together")),
    }
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
        incarnation: IncarnationId::parse(incarnation).map_err(|e| format!("{ENV_LAUNCHER} incarnation: {e}"))?,
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
        m.insert(ENV_INCARNATION.into(), self.incarnation.to_string());
        if let Some(s) = &self.supersedes {
            m.insert(ENV_SUPERSEDES.into(), s.to_string());
        }
        m.insert(ENV_TRANSPORT_ADDR.into(), self.bind_addr.to_string());
        if let Some(l) = &self.launcher {
            m.insert(ENV_LAUNCHER.into(), format!("{},{},{}", l.name, l.node_id, l.incarnation));
        }
        m.insert(ENV_LISTENERS.into(), encode_listeners(&self.listeners));
        m.insert(ENV_SEEDS.into(), self.seeds.iter().map(|(k, a)| format!("{k}@{a}")).collect::<Vec<_>>().join(","));
        m.insert(ENV_DATA_DIR.into(), self.data_dir.display().to_string());
        m.insert(ENV_STORE_DIR.into(), self.store_dir.display().to_string());
        if let Some(id) = &self.mesh_id {
            m.insert(ENV_MESH_ID.into(), id.to_string());
        }
        if let Some(issuing) = &self.issuing {
            m.insert(ENV_ISSUING_KEY.into(), issuing.key.0.clone());
            m.insert(ENV_ISSUING_CERT.into(), issuing.cert.clone());
            m.insert(ENV_CA_SECRET.into(), issuing.root.0.clone());
        }
        m.insert(ENV_CONNECTIONS_DELETE_DEADLINE.into(), self.connections_delete_deadline.as_millis().to_string());
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
            incarnation: IncarnationId::parse(&req(ENV_INCARNATION)?).map_err(|e| format!("{ENV_INCARNATION}: {e}"))?,
            supersedes: get(ENV_SUPERSEDES).filter(|s| !s.is_empty()).map(|s| IncarnationId::parse(&s).map_err(|e| format!("{ENV_SUPERSEDES}: {e}"))).transpose()?,
            bind_addr: req(ENV_TRANSPORT_ADDR)?.parse().map_err(|e| format!("{ENV_TRANSPORT_ADDR}: {e}"))?,
            listeners: decode_listeners(&get(ENV_LISTENERS).unwrap_or_default())?,
            launcher: get(ENV_LAUNCHER).filter(|v| !v.trim().is_empty()).map(|v| decode_launcher(&v)).transpose()?,
            seeds: decode_seeds(&get(ENV_SEEDS).unwrap_or_default())?,
            data_dir: PathBuf::from(req(ENV_DATA_DIR)?),
            store_dir: PathBuf::from(req(ENV_STORE_DIR)?),
            mesh_id: get(ENV_MESH_ID).filter(|s| !s.trim().is_empty()).map(|s| MeshId::parse(&s).map_err(|e| format!("{ENV_MESH_ID}: {e}"))).transpose()?,
            issuing: decode_issuing(get(ENV_ISSUING_KEY), get(ENV_ISSUING_CERT), get(ENV_CA_SECRET))?,
            connections_delete_deadline: decode_connections_delete_deadline(get(ENV_CONNECTIONS_DELETE_DEADLINE).as_deref())?,
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
            store_dir: "/tmp/store".into(),
            mesh_id: Some(MeshId::mint()),
            issuing: Some(IssuingMaterial { key: SecretHex("0a".repeat(32)), cert: "00abff".into(), root: SecretHex("0b".repeat(32)) }),
            connections_delete_deadline: Duration::from_millis(750),
        };
        let env = l.to_env();
        assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap(), l);
        assert!(Launch::from_env(|_| None).unwrap_err().contains("RDM_FABRIC is required"));
        // A launch with no store root is refused by name (R14 item 8).
        let mut no_store = env.clone();
        no_store.remove(ENV_STORE_DIR);
        assert!(Launch::from_env(|k| no_store.get(k).cloned()).unwrap_err().contains("RDM_STORE_DIR is required"));
        let mut blank = env.clone();
        blank.insert(ENV_FABRIC.into(), " ".into());
        assert!(Launch::from_env(|k| blank.get(k).cloned()).unwrap_err().contains("RDM_FABRIC is required and must not be empty"));
        assert_eq!((env.get(ENV_ISSUING_KEY).map(String::len), env.get(ENV_ISSUING_CERT).map(String::as_str)), (Some(64), Some("00abff")));
        assert!(!format!("{l:?}").contains(&"0a".repeat(32)), "the issuing key never prints: {l:?}");
        assert_eq!(env.get(ENV_CA_SECRET).map(String::len), Some(64), "the root rides beside the issuing material");
        assert!(!format!("{l:?}").contains(&"0b".repeat(32)), "the root never prints: {l:?}");
        let mut no_root = env.clone();
        no_root.remove(ENV_CA_SECRET);
        assert!(Launch::from_env(|k| no_root.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY and RDM_ISSUING_CERT are set without RDM_CA_SECRET"), "issuing material without the root is refused by name");
        let mut odd = env.clone();
        odd.insert(ENV_ISSUING_CERT.into(), "abc".into());
        assert!(Launch::from_env(|k| odd.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_CERT has an odd number"));
        odd.insert(ENV_ISSUING_KEY.into(), "zz".into());
        assert!(Launch::from_env(|k| odd.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY is not hex"));
        let mut half = env.clone();
        half.remove(ENV_ISSUING_CERT);
        assert!(Launch::from_env(|k| half.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY is set without RDM_ISSUING_CERT"));
        // A launch that carries no issuing material sets no variable.
        let none = Launch { issuing: None, ..l.clone() }.to_env();
        assert!(!none.contains_key(ENV_ISSUING_KEY) && !none.contains_key(ENV_ISSUING_CERT) && !none.contains_key(ENV_CA_SECRET));
        // The connections-delete bound rides the launch: absent or blank is the default, a value that
        // is no whole number of milliseconds is refused by name.
        assert_eq!(env.get(ENV_CONNECTIONS_DELETE_DEADLINE).map(String::as_str), Some("750"));
        let mut unset = env.clone();
        unset.remove(ENV_CONNECTIONS_DELETE_DEADLINE);
        assert_eq!(Launch::from_env(|k| unset.get(k).cloned()).unwrap().connections_delete_deadline, DEFAULT_CONNECTIONS_DELETE_DEADLINE);
        unset.insert(ENV_CONNECTIONS_DELETE_DEADLINE.into(), " ".into());
        assert_eq!(Launch::from_env(|k| unset.get(k).cloned()).unwrap().connections_delete_deadline, DEFAULT_CONNECTIONS_DELETE_DEADLINE);
        unset.insert(ENV_CONNECTIONS_DELETE_DEADLINE.into(), "5s".into());
        assert!(Launch::from_env(|k| unset.get(k).cloned()).unwrap_err().starts_with("RDM_CONNECTIONS_DELETE_DEADLINE_MS `5s` is not a whole number of milliseconds"));
        // A launched node's product ids are canonical or refused by name.
        for (k, v, says) in [(ENV_NODE_ID, "f".repeat(32), "node id"), (ENV_FABRIC_ID, "fabric1".into(), "fabric id"), (ENV_MESH_ID, "4f14".into(), "mesh id")] {
            let mut bad = env.clone();
            bad.insert(k.into(), v);
            let e = Launch::from_env(|x| bad.get(x).cloned()).unwrap_err();
            assert!(e.starts_with(k) && e.contains(says), "{e}");
        }
    }
}
