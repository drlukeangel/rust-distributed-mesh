//! The launch environment node-admin hands a node it deploys, and the node
//! reads back. One contract, one owner.

type Result<T> = std::result::Result<T, String>;
use crate::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use crate::certs::VerifyingKey;
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
/// The non-Iroh listeners the process binds, as `name=addr` entries joined by commas: each the
/// address node-admin assigned, with port 0; the node reports the port it bound at its join (Q151).
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
/// A containerized node-admin's executable directories (proposal executable-bindings-at-create.md §2):
/// comma-separated host directories mounted read-only at their host paths into a launched admin's
/// container; an executable a Build names outside them is refused by name at acceptance. The
/// bootstrap admin reads it from its environment; the launching admin writes its own value into every
/// node-admin launch.
pub const ENV_EXECUTABLE_DIRS: &str = "RDM_EXECUTABLE_DIRS";
/// The fabric's `topology_storage_provider` (storage.md §2): the registered name of the backend RDM's
/// five record stores are written to. The bootstrap admin reads it from its environment, [`TOPOLOGY_STORAGE_FILE`]
/// when unset; the launching admin writes it into every node's launch.
pub const ENV_TOPOLOGY_STORAGE_PROVIDER: &str = "RDM_TOPOLOGY_STORAGE_PROVIDER";
/// The name every RDM binary registers its own file backend under (storage.md §2).
pub const TOPOLOGY_STORAGE_FILE: &str = "file";
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
/// The fabric's root set a launched node verifies presented member certs against (fabric-certs.md
/// §chain; fabric-ca-renew.md "Launch": "a node born mid-overlap trusts both"): one root, or two
/// during a root rotation, each an ed25519 public key as hex, comma-separated. Written by the
/// launching node-admin from the set it holds at the launch, the way `RDM_MESH_ID` is; absent for a
/// launcher configured with no certs.
pub const ENV_ROOTS: &str = "RDM_ROOTS";
/// The fabric CA's key, the ed25519 secret as hex, written beside the root by a launcher that holds
/// both (fabric-ca-renew.md "Launch": "a launched node-admin is also written ... the current CA
/// material"); the CA's cert travels inside the issuing cert. A secret: only in a launch's environment.
pub const ENV_FABRIC_CA_KEY: &str = "RDM_FABRIC_CA_KEY";
/// The declared secrets a launched node-admin holds in custody (deployment-provider.md §2.3: "a
/// declared secret is written to the admins that may launch its kinds and to those processes, never
/// to a record or a projection"): the custody document, `{"v":1,"declared":[{"name","kinds","value"}]}`
/// ([`encode_custody`]), so a value holding a separator, `=`, a newline or nothing at all round-trips.
/// Written into every node-admin's launch beside the root by a launcher that holds declared values, the
/// way `RDM_CA_SECRET` is. A secret: it travels only in a launch's environment and in the file an admin
/// keeps it in beside its data dir.
pub const ENV_DECLARED_SECRETS: &str = "RDM_DECLARED_SECRETS";
/// RDM's own secrets a launch can carry: the root, the mesh's issuing key, the fabric CA's key and the
/// custody document. A provider keeps each off a command line it runs (the container provider's
/// `docker run`), and RDM's diagnostics are scrubbed of each before they reach a receipt or a span.
pub const RDM_SECRETS: [&str; 4] = [ENV_CA_SECRET, ENV_ISSUING_KEY, ENV_FABRIC_CA_KEY, ENV_DECLARED_SECRETS];
/// The custody document's version ([`ENV_DECLARED_SECRETS`]).
pub const CUSTODY_VERSION: u64 = 1;
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

impl SecretHex {
    /// `value`'s bytes as hex: how a declared secret is held from its read to the spawn.
    pub fn of_value(value: &str) -> Self {
        Self(encode_hex(value.as_bytes()))
    }

    /// The value whose bytes this hex spells; a spelling that is not hex, or bytes that are not
    /// UTF-8, are refused by name.
    pub fn value(&self, name: &str) -> Result<String> {
        String::from_utf8(decode_hex(name, &self.0)?).map_err(|_| format!("{name} is not UTF-8"))
    }
}

/// A per-process variable an application's wiring declares (Q150): its name and the node kinds whose
/// process environment it is set in. Its value is a secret; this names none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredSecret {
    /// The variable's name, as the processes of `kinds` read it.
    pub name: String,
    /// The kinds it is set on.
    pub kinds: Vec<NodeKind>,
}

/// A node kind's listeners as an application's wiring declares them (Q151): each listener by name.
/// Node-admin assigns each the birth's address with port 0 (`RDM_LISTENERS`); the node binds them,
/// keeps every bind open through its join and reports the ports the operating system selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredListeners {
    /// The kind whose processes bind them.
    pub kind: NodeKind,
    /// The listeners' names, unique within the kind.
    pub names: Vec<String>,
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
    /// The fabric CA's key, written beside the root: every node-admin RDM launches holds the CA
    /// (fabric-certs.md §Custody; the CA's cert is the issuing cert's own).
    pub ca_key: SecretHex,
}

impl IssuingMaterial {
    /// The material of `key` with the fabric's `root` and its CA `ca` beside it, as a launch writes it.
    pub fn of(key: &crate::certs::IssuingKey, root: &crate::certs::SigningKey, ca: &crate::certs::CaKey) -> Self {
        Self { key: SecretHex(crate::certs::encode_key(key.secret())), cert: crate::certs::encode_issuing_cert(key.cert()), root: SecretHex(crate::certs::encode_key(root)), ca_key: SecretHex(crate::certs::encode_key(ca.secret())) }
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
    /// The `topology_storage_provider` this birth writes ([`ENV_TOPOLOGY_STORAGE_PROVIDER`]): the name its
    /// executable's wiring registers the backend under, written by the launching admin.
    pub topology_storage_provider: String,
    /// The id of the node's mesh: it names the mesh's topology channel.
    pub mesh_id: Option<MeshId>,
    /// The issuing material of its mesh a node-admin is launched with. `None` for every other birth
    /// and for a launcher configured with no certs.
    pub issuing: Option<IssuingMaterial>,
    /// The fabric's root set this birth verifies presented member certs against (`RDM_ROOTS`), as its
    /// launcher held it at the launch: one root, or two during a root rotation. Empty for a launcher
    /// configured with no certs.
    pub roots: Vec<VerifyingKey>,
    /// The bound of a `node.connections.delete { All }` at this birth, standalone or inside a stop
    /// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`): the eligible work in flight is let finish until it
    /// passes, then the closure answers `Deadline`.
    pub connections_delete_deadline: Duration,
    /// The declared secrets a node-admin is launched with in custody (`RDM_DECLARED_SECRETS`): every
    /// declaration whose value its launcher holds, with that value. Empty for every other birth.
    pub declared: Vec<(DeclaredSecret, SecretHex)>,
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

/// `roots` as `RDM_ROOTS` carries them: each public key as hex, comma-separated.
pub fn encode_roots(roots: &[VerifyingKey]) -> String {
    roots.iter().map(|k| encode_hex(k.as_bytes())).collect::<Vec<_>>().join(",")
}

/// The root set `RDM_ROOTS` carries: absent or empty is none; each entry must be an ed25519 public
/// key as hex, and a set is one root, or two during a root rotation. Anything else is refused by name.
pub fn decode_roots(v: Option<&str>) -> Result<Vec<VerifyingKey>> {
    let entries: Vec<&str> = v.unwrap_or_default().split(',').map(str::trim).filter(|e| !e.is_empty()).collect();
    if entries.len() > 2 {
        return Err(format!("{ENV_ROOTS} carries {} roots: a fabric trusts one root, or two during a root rotation", entries.len()));
    }
    entries
        .into_iter()
        .map(|e| {
            let bytes: [u8; 32] = decode_hex("the entry", e).map_err(|err| format!("{ENV_ROOTS} entry `{e}` is not an ed25519 public key: {err}"))?.try_into().map_err(|b: Vec<u8>| format!("{ENV_ROOTS} entry `{e}` is not an ed25519 public key: it is {} bytes, not 32", b.len()))?;
            VerifyingKey::from_bytes(&bytes).map_err(|err| format!("{ENV_ROOTS} entry `{e}` is not an ed25519 public key: {err}"))
        })
        .collect()
}

/// The issuing material `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT` carry: both, or neither, with the
/// root `RDM_CA_SECRET` and the CA key `RDM_FABRIC_CA_KEY` beside them. One without the other, issuing
/// material without the root or the CA key, or any of them not hex, is refused by name.
/// A root with no issuing material is no issuing material: it is the bootstrap admin's, read by its
/// cert choice.
pub fn decode_issuing(key: Option<String>, cert: Option<String>, root: Option<String>, ca_key: Option<String>) -> Result<Option<IssuingMaterial>> {
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
            let Some(ca_key) = present(ca_key) else {
                return Err(format!("{ENV_ISSUING_KEY} and {ENV_ISSUING_CERT} are set without {ENV_FABRIC_CA_KEY}: a node-admin is launched with the fabric CA beside the root and its mesh's issuing material (fabric-certs.md §Custody)"));
            };
            decode_hex(ENV_FABRIC_CA_KEY, &ca_key)?;
            Ok(Some(IssuingMaterial { key: SecretHex(key), cert, root: SecretHex(root), ca_key: SecretHex(ca_key) }))
        }
        (Some(_), None) => Err(format!("{ENV_ISSUING_KEY} is set without {ENV_ISSUING_CERT}: issuing material is the key and its cert together")),
        (None, Some(_)) => Err(format!("{ENV_ISSUING_CERT} is set without {ENV_ISSUING_KEY}: issuing material is the key and its cert together")),
    }
}

pub use crate::custody::{decode_custody, encode_custody};

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

/// [`ENV_EXECUTABLE_DIRS`] as given: comma-separated directories, each absolute (a container mounts
/// it at its host path), in the order given, each once; an empty entry is skipped. A relative entry is
/// refused by name.
pub fn decode_executable_dirs(s: &str) -> Result<Vec<std::path::PathBuf>> {
    let mut dirs: Vec<std::path::PathBuf> = Vec::new();
    for e in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let d = std::path::PathBuf::from(e);
        if !d.is_absolute() {
            return Err(format!("{ENV_EXECUTABLE_DIRS} entry `{e}` is not an absolute directory: a container mounts each at its host path"));
        }
        if !dirs.contains(&d) {
            dirs.push(d);
        }
    }
    Ok(dirs)
}

/// [`ENV_TOPOLOGY_STORAGE_PROVIDER`] as given: [`TOPOLOGY_STORAGE_FILE`] when unset (a launch written
/// before the variable was, or a bootstrap admin given none); a value that is empty is refused by name.
pub fn decode_topology_storage_provider(v: Option<String>) -> Result<String> {
    match v {
        None => Ok(TOPOLOGY_STORAGE_FILE.into()),
        Some(v) if v.trim().is_empty() => Err(format!("{ENV_TOPOLOGY_STORAGE_PROVIDER} is set and empty: it names a registered topology storage provider, `{TOPOLOGY_STORAGE_FILE}` when unset")),
        Some(v) => Ok(v.trim().to_string()),
    }
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
        m.insert(ENV_TOPOLOGY_STORAGE_PROVIDER.into(), self.topology_storage_provider.clone());
        if let Some(id) = &self.mesh_id {
            m.insert(ENV_MESH_ID.into(), id.to_string());
        }
        if let Some(issuing) = &self.issuing {
            m.insert(ENV_ISSUING_KEY.into(), issuing.key.0.clone());
            m.insert(ENV_ISSUING_CERT.into(), issuing.cert.clone());
            m.insert(ENV_CA_SECRET.into(), issuing.root.0.clone());
            m.insert(ENV_FABRIC_CA_KEY.into(), issuing.ca_key.0.clone());
        }
        if !self.roots.is_empty() {
            m.insert(ENV_ROOTS.into(), encode_roots(&self.roots));
        }
        if !self.declared.is_empty() {
            m.insert(ENV_DECLARED_SECRETS.into(), encode_custody(&self.declared));
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
            topology_storage_provider: decode_topology_storage_provider(get(ENV_TOPOLOGY_STORAGE_PROVIDER))?,
            mesh_id: get(ENV_MESH_ID).filter(|s| !s.trim().is_empty()).map(|s| MeshId::parse(&s).map_err(|e| format!("{ENV_MESH_ID}: {e}"))).transpose()?,
            issuing: decode_issuing(get(ENV_ISSUING_KEY), get(ENV_ISSUING_CERT), get(ENV_CA_SECRET), get(ENV_FABRIC_CA_KEY))?,
            roots: decode_roots(get(ENV_ROOTS).as_deref())?,
            connections_delete_deadline: decode_connections_delete_deadline(get(ENV_CONNECTIONS_DELETE_DEADLINE).as_deref())?,
            declared: decode_custody(get(ENV_DECLARED_SECRETS).as_deref())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (storage.md §2 Day 0: "`RDM_TOPOLOGY_STORAGE_PROVIDER`, `file` when unset"; "every launch:
    /// the launching admin, into the node's environment"): unset reads `file`, a name reads as given, an
    /// empty value is refused by name.
    #[test]
    fn the_topology_storage_provider_is_file_when_unset_and_refused_when_empty() {
        assert_eq!(decode_topology_storage_provider(None).unwrap(), TOPOLOGY_STORAGE_FILE);
        assert_eq!(decode_topology_storage_provider(Some("app".into())).unwrap(), "app");
        assert!(decode_topology_storage_provider(Some(" ".into())).unwrap_err().contains(ENV_TOPOLOGY_STORAGE_PROVIDER));
    }

    /// A node-admin's launch environment with `custody` as its `RDM_DECLARED_SECRETS`.
    fn admin_env_with_custody(custody: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (ENV_FABRIC.to_string(), "fabric1".to_string()),
            (ENV_FABRIC_ID.to_string(), FabricId::mint().to_string()),
            (ENV_NODE_NAME.to_string(), "mesh2.admin.1".to_string()),
            (ENV_NODE_ID.to_string(), NodeId::mint().to_string()),
            (ENV_INCARNATION.to_string(), IncarnationId::mint().to_string()),
            (ENV_TRANSPORT_ADDR.to_string(), "127.0.0.1:0".to_string()),
            (ENV_DATA_DIR.to_string(), "/tmp/x".to_string()),
            (ENV_STORE_DIR.to_string(), "/tmp/store".to_string()),
            (ENV_DECLARED_SECRETS.to_string(), custody.to_string()),
        ])
    }

    /// CONTRACT (Q150 §0.1, the custody document): `RDM_DECLARED_SECRETS` is the JSON document
    /// `{"v":1,"declared":[{"name","kinds","value"}]}`, so a value holding a separator, `=`, a newline or
    /// nothing at all round-trips through a launch unchanged; a document that does not parse, names an
    /// unknown version or repeats a name is refused by name, echoing none of it.
    #[test]
    fn the_custody_document_round_trips_every_value_and_refuses_a_malformed_one_by_name() {
        let values = [("APP_SEPARATED", "a,b=c\nd=,"), ("APP_EMPTY", ""), ("APP_PLAIN", "plain-value")];
        let doc = serde_json::json!({ "v": 1, "declared": values.iter().map(|(n, v)| serde_json::json!({ "name": n, "kinds": ["broker", "gateway"], "value": v })).collect::<Vec<_>>() }).to_string();
        let env = admin_env_with_custody(&doc);
        let launch = Launch::from_env(|k| env.get(k).cloned()).expect("a launch carrying the custody document");
        assert!(!format!("{launch:?}").contains("plain-value") && !format!("{launch:?}").contains("a,b=c"), "a declared value never prints: {launch:?}");
        let written = launch.to_env();
        let back: serde_json::Value = serde_json::from_str(written.get(ENV_DECLARED_SECRETS).expect("the launch writes the custody document back")).expect("the custody document is JSON");
        assert_eq!(back["v"], 1, "{back}");
        for (name, value) in values {
            let entry = back["declared"].as_array().into_iter().flatten().find(|e| e["name"] == name).unwrap_or_else(|| panic!("{name} is carried: {back}"));
            assert_eq!(entry["value"].as_str(), Some(value), "{name} round-trips unchanged");
            assert_eq!(entry["kinds"], serde_json::json!(["broker", "gateway"]), "{name}'s kinds round-trip");
        }
        assert_eq!(Launch::from_env(|k| written.get(k).cloned()).unwrap(), launch, "written and read back, the launch is the same");
        let secret = "s3cret-never-echoed";
        let repeated = serde_json::json!({ "v": 1, "declared": [{ "name": "APP_KEY", "kinds": ["broker"], "value": secret }, { "name": "APP_KEY", "kinds": ["gateway"], "value": secret }] }).to_string();
        let unknown = serde_json::json!({ "v": 2, "declared": [{ "name": "APP_KEY", "kinds": ["broker"], "value": secret }] }).to_string();
        let broken = format!("{{\"v\":1,\"declared\":[{{\"name\":\"APP_KEY\",\"value\":\"{secret}\"");
        let unkinded = serde_json::json!({ "v": 1, "declared": [{ "name": "APP_KEY", "kinds": ["nowhere"], "value": secret }] }).to_string();
        for (bad, says) in [(repeated, "repeats a name"), (unknown, "unknown version"), (broken.clone(), "does not parse"), (format!("APP_KEY={}", encode_hex(secret.as_bytes())), "does not parse"), (unkinded, "does not parse")] {
            let env = admin_env_with_custody(&bad);
            let e = Launch::from_env(|k| env.get(k).cloned()).unwrap_err();
            assert!(e.starts_with(ENV_DECLARED_SECRETS) && e.contains(says), "refused by name ({says}): {e}");
            assert!(!e.contains(secret) && !e.contains(&encode_hex(secret.as_bytes())) && !e.contains("APP_KEY") && !e.contains("nowhere"), "the refusal echoes none of the document: {e}");
        }
    }

    #[test]
    fn launch_round_trips_through_the_environment() {
        let l = Launch { topology_storage_provider: TOPOLOGY_STORAGE_FILE.into(),
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
            issuing: Some(IssuingMaterial { key: SecretHex("0a".repeat(32)), cert: "00abff".into(), root: SecretHex("0b".repeat(32)), ca_key: SecretHex("0c".repeat(32)) }),
            roots: Vec::new(),
            connections_delete_deadline: Duration::from_millis(750),
            declared: vec![(DeclaredSecret { name: "APP_KEY".into(), kinds: vec![NodeKind::Broker] }, SecretHex::of_value("app-secret-value"))],
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
        assert_eq!(env.get(ENV_FABRIC_CA_KEY).map(String::len), Some(64), "the CA key rides beside the root");
        assert!(!format!("{l:?}").contains(&"0c".repeat(32)), "the CA key never prints: {l:?}");
        assert_eq!(serde_json::from_str::<serde_json::Value>(&env[ENV_DECLARED_SECRETS]).unwrap(), serde_json::json!({ "v": 1, "declared": [{ "name": "APP_KEY", "kinds": ["broker"], "value": "app-secret-value" }] }), "the custody document rides the launch");
        assert!(!format!("{l:?}").contains(&encode_hex(b"app-secret-value")) && !format!("{l:?}").contains("app-secret-value"), "a declared value never prints: {l:?}");
        assert_eq!(l.declared[0].1.value("APP_KEY").unwrap(), "app-secret-value");
        let mut no_root = env.clone();
        no_root.remove(ENV_CA_SECRET);
        assert!(Launch::from_env(|k| no_root.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY and RDM_ISSUING_CERT are set without RDM_CA_SECRET"), "issuing material without the root is refused by name");
        let mut no_ca = env.clone();
        no_ca.remove(ENV_FABRIC_CA_KEY);
        assert!(Launch::from_env(|k| no_ca.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY and RDM_ISSUING_CERT are set without RDM_FABRIC_CA_KEY"), "issuing material without the CA key is refused by name");
        let mut odd = env.clone();
        odd.insert(ENV_ISSUING_CERT.into(), "abc".into());
        assert!(Launch::from_env(|k| odd.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_CERT has an odd number"));
        odd.insert(ENV_ISSUING_KEY.into(), "zz".into());
        assert!(Launch::from_env(|k| odd.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY is not hex"));
        let mut half = env.clone();
        half.remove(ENV_ISSUING_CERT);
        assert!(Launch::from_env(|k| half.get(k).cloned()).unwrap_err().starts_with("RDM_ISSUING_KEY is set without RDM_ISSUING_CERT"));
        // A launch that carries no declared value writes no custody variable (a malformed one:
        // `the_custody_document_round_trips_every_value_and_refuses_a_malformed_one_by_name`).
        assert!(!Launch { declared: Vec::new(), ..l.clone() }.to_env().contains_key(ENV_DECLARED_SECRETS));
        // A launch that carries no issuing material sets no variable.
        let none = Launch { issuing: None, ..l.clone() }.to_env();
        assert!(!none.contains_key(ENV_ISSUING_KEY) && !none.contains_key(ENV_ISSUING_CERT) && !none.contains_key(ENV_CA_SECRET) && !none.contains_key(ENV_FABRIC_CA_KEY));
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

    /// CONTRACT (fabric-ca-renew.md "Launch": "A launched node is written the current set at launch
    /// (`RDM_ROOTS`), the way `RDM_MESH_ID` is, so a node born mid-overlap trusts both"): a launch
    /// carries its root set, one root or two, through the environment and reads the same set back; a
    /// launch with no set (a launcher configured with no certs) writes no variable and reads back none.
    /// A value that is no ed25519 public key, and a set of more than two, are refused by name.
    #[test]
    fn a_launch_carries_its_root_set_as_rdm_roots_and_refuses_one_that_is_no_key_by_name() {
        let (a, b) = (crate::certs::mint_key().verifying_key(), crate::certs::mint_key().verifying_key());
        let base = Launch { topology_storage_provider: TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            name: "mesh1.rpc.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            launcher: None,
            seeds: vec![],
            data_dir: "/tmp/x".into(),
            store_dir: "/tmp/store".into(),
            mesh_id: None,
            issuing: None,
            roots: vec![a],
            connections_delete_deadline: DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Vec::new(),
        };
        for roots in [vec![a], vec![a, b]] {
            let l = Launch { roots: roots.clone(), ..base.clone() };
            let env = l.to_env();
            let written = roots.iter().map(|k| encode_hex(k.as_bytes())).collect::<Vec<_>>().join(",");
            assert_eq!(env.get(ENV_ROOTS), Some(&written), "the set as RDM_ROOTS carries it");
            assert_eq!(Launch::from_env(|k| env.get(k).cloned()).unwrap().roots, roots, "the same set read back");
        }
        let none = Launch { roots: vec![], ..base.clone() }.to_env();
        assert!(!none.contains_key(ENV_ROOTS), "a launch with no root set writes no RDM_ROOTS: {none:?}");
        assert_eq!(Launch::from_env(|k| none.get(k).cloned()).unwrap().roots, Vec::<VerifyingKey>::new());
        let env = base.to_env();
        let three = [a, b, crate::certs::mint_key().verifying_key()].iter().map(|k| encode_hex(k.as_bytes())).collect::<Vec<_>>().join(",");
        for (bad, says) in [("zz".to_string(), "is not an ed25519 public key"), ("ab".repeat(31), "is not an ed25519 public key"), (three, "carries 3 roots")] {
            let mut env = env.clone();
            env.insert(ENV_ROOTS.into(), bad.clone());
            let e = Launch::from_env(|k| env.get(k).cloned()).err().unwrap_or_else(|| panic!("RDM_ROOTS `{bad}` must be refused"));
            assert!(e.starts_with(ENV_ROOTS) && e.contains(says), "{e}");
        }
    }

    // @feature: named-listeners
    /// CONTRACT (docs/architecture/node-rpc-envelope.md:1033):
    /// "`listeners` is each listener the application's wiring declares on the birth's kind, by name,
    ///  at the address the birth bound: the IP address node-admin assigned and a nonzero port the
    ///  operating system chose (Q151)."
    #[test]
    fn named_listeners_encode_and_decode_through_rdm_listeners_and_refuse_malformed_entries_by_name() {
        let entries: Vec<(String, SocketAddr)> = vec![
            ("kafka".into(), "127.0.0.1:0".parse().unwrap()),
            ("rest".into(), "127.0.0.1:41010".parse().unwrap()),
            ("proxy".into(), "127.0.0.1:41011".parse().unwrap()),
        ];
        let encoded = encode_listeners(&entries);
        assert_eq!(
            encoded,
            "kafka=127.0.0.1:0,rest=127.0.0.1:41010,proxy=127.0.0.1:41011"
        );
        assert_eq!(decode_listeners(&encoded).unwrap(), entries);
        assert!(decode_listeners("").unwrap().is_empty());

        let base_env = BTreeMap::from([
            (ENV_FABRIC.to_string(), "fabric1".to_string()),
            (ENV_FABRIC_ID.to_string(), FabricId::mint().to_string()),
            (ENV_NODE_NAME.to_string(), "mesh1.gateway.1".to_string()),
            (ENV_NODE_ID.to_string(), NodeId::mint().to_string()),
            (ENV_INCARNATION.to_string(), IncarnationId::mint().to_string()),
            (ENV_TRANSPORT_ADDR.to_string(), "127.0.0.1:0".to_string()),
            (ENV_DATA_DIR.to_string(), "/tmp/x".to_string()),
            (ENV_STORE_DIR.to_string(), "/tmp/store".to_string()),
            (ENV_LISTENERS.to_string(), encoded),
        ]);
        let launch = Launch::from_env(|k| base_env.get(k).cloned()).expect("valid launch");
        assert_eq!(launch.listeners, entries);
        let written = launch.to_env();
        assert_eq!(
            Launch::from_env(|k| written.get(k).cloned())
                .expect("parsed launch")
                .listeners,
            entries
        );

        for (bad, expected) in [
            ("kafka", "RDM_LISTENERS entry `kafka` is not name=addr"),
            ("kafka=not-an-addr", "address in `kafka=not-an-addr`"),
        ] {
            let mut bad_env = base_env.clone();
            bad_env.insert(ENV_LISTENERS.to_string(), bad.to_string());
            let err = Launch::from_env(|k| bad_env.get(k).cloned())
                .expect_err("malformed RDM_LISTENERS must be refused");
            assert!(err.contains(expected), "expected `{expected}` in `{err}`");
        }
    }
}
