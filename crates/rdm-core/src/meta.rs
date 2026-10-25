//! A node's create spec ([`NodeSpec`]): the node's own properties, flat (Q166), as the accepted Build
//! names them for a path.name. Intrinsic, explicit on every materialized path after ingress
//! normalization; never a tag, never inferred later from a flag, a map or a provider's behaviour.

use crate::path::{NodeKind, PathName};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The create spec of one node (build.md §1, Q166): its `path.name` and the node's own properties under
/// the node-properties names (node-rpc-envelope.md "Node properties"), flat. There is no nested
/// configuration type: `config` is the application's map and nothing else. Field order is the wire's:
/// a Build request's entry was `(PathName, { storage, placement, executable, tags })` on the positional
/// codec, and `config` is appended last.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// The node's `path.name`.
    pub path: PathName,
    /// What survives a restart or a retirement.
    pub storage: StorageMeta,
    /// Placement constraints.
    pub placement: PlacementMeta,
    /// The executable the node runs (path, sha256 and, under the container provider, the image):
    /// named per node by the request that creates it, compiled onto the accepted topology's entry,
    /// recorded on the node's row at the claim and re-hashed at the launch (build-deployment.md §2). An
    /// entry that differs from what the row records is that node's replacement. A Build accepted before
    /// the field decodes with none: valid history, its births' rows the record of what they run.
    #[serde(default)]
    pub executable: Option<crate::binding::Binding>,
    /// The tag labels the node carries from its name claim; a node recovered or replaced at the path
    /// takes its predecessor's row's (node-replace.md "Inherited"). A Build accepted before the field
    /// decodes with none.
    #[serde(default)]
    pub tags: BTreeSet<String>,
    /// The application's config, the node's map (node-rpc-envelope.md "Node properties"): written to
    /// the row at the name claim and handed in the first join; capped as `node.config.update` caps it
    /// ([`crate::check_config`]), refused by name at acceptance. RDM never reads it. A Build accepted
    /// before the field decodes with none.
    #[serde(default)]
    pub config: BTreeMap<String, String>,
}

/// What survives a topology-preserving runtime loss or restart, and the declared disposition of
/// persistent storage once its node is gone. A delete releases no storage in any state
/// (node-delete.md "Storage"). The type holds exactly the three meaningful states; no invalid
/// combination is representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMeta {
    /// Fresh storage for a new node.
    Ephemeral,
    /// Storage survives stop, start and restart, and a new node created over an inherited data dir
    /// keeps it; `on_retire` is its declared disposition once the node is gone.
    Persistent {
        /// The declared disposition of the storage once the node is gone.
        on_retire: PersistentRetireDisposition,
    },
}

/// The journaled (JSON) shape of [`StorageMeta`], `persistence`-tagged; a positional codec (the Build
/// wire's postcard, where a request's [`NodeSpec`] travels) carries [`StorageMetaPositional`], since
/// an internally tagged enum cannot be read positionally.
#[derive(Serialize, Deserialize)]
#[serde(tag = "persistence", rename_all = "kebab-case", try_from = "StorageMetaRow")]
enum StorageMetaTagged {
    Ephemeral,
    Persistent { on_retire: PersistentRetireDisposition },
}

impl TryFrom<StorageMetaRow> for StorageMetaTagged {
    type Error = String;

    fn try_from(row: StorageMetaRow) -> Result<Self, String> {
        Ok(match StorageMeta::try_from(row)? {
            StorageMeta::Ephemeral => Self::Ephemeral,
            StorageMeta::Persistent { on_retire } => Self::Persistent { on_retire },
        })
    }
}

/// [`StorageMeta`] on a positional codec: the variant's index, then its fields.
#[derive(Serialize, Deserialize)]
enum StorageMetaPositional {
    Ephemeral,
    Persistent { on_retire: PersistentRetireDisposition },
}

impl Serialize for StorageMeta {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match (*self, s.is_human_readable()) {
            (Self::Ephemeral, true) => StorageMetaTagged::Ephemeral.serialize(s),
            (Self::Persistent { on_retire }, true) => StorageMetaTagged::Persistent { on_retire }.serialize(s),
            (Self::Ephemeral, false) => StorageMetaPositional::Ephemeral.serialize(s),
            (Self::Persistent { on_retire }, false) => StorageMetaPositional::Persistent { on_retire }.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for StorageMeta {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        if d.is_human_readable() {
            Ok(match StorageMetaTagged::deserialize(d)? {
                StorageMetaTagged::Ephemeral => Self::Ephemeral,
                StorageMetaTagged::Persistent { on_retire } => Self::Persistent { on_retire },
            })
        } else {
            Ok(match StorageMetaPositional::deserialize(d)? {
                StorageMetaPositional::Ephemeral => Self::Ephemeral,
                StorageMetaPositional::Persistent { on_retire } => Self::Persistent { on_retire },
            })
        }
    }
}

/// The journaled shape of `StorageMeta`, read field by field so an illegal combination is
/// refused by name: the derived internally-tagged reader would silently drop a field the
/// `persistence` arm does not carry.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageMetaRow {
    persistence: StoragePersistence,
    #[serde(default)]
    on_retire: Option<PersistentRetireDisposition>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum StoragePersistence {
    Ephemeral,
    Persistent,
}

impl TryFrom<StorageMetaRow> for StorageMeta {
    type Error = String;

    fn try_from(row: StorageMetaRow) -> Result<Self, String> {
        match (row.persistence, row.on_retire) {
            (StoragePersistence::Ephemeral, None) => Ok(Self::Ephemeral),
            (StoragePersistence::Ephemeral, Some(_)) => {
                Err("storage meta: `persistence: ephemeral` carries no `on_retire`".into())
            }
            (StoragePersistence::Persistent, Some(on_retire)) => Ok(Self::Persistent { on_retire }),
            (StoragePersistence::Persistent, None) => {
                Err("storage meta: `persistence: persistent` requires `on_retire`".into())
            }
        }
    }
}

/// The declared disposition of persistent storage once its node is gone. node.delete acts on
/// neither: it retains the data dir and endpoint key (node-delete.md "Storage"); application
/// storage removal is separate work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PersistentRetireDisposition {
    /// The storage is kept.
    Preserve,
    /// The storage may be removed by separate application work; node.delete does not release it.
    Release,
}

/// Placement constraints of the logical node. None are declared in this build; the type exists
/// so a constraint, when it comes, is typed Meta and never a tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementMeta {}

impl NodeSpec {
    /// The migration default for `path`'s kind: what ingress normalization gives a materialized path
    /// that declared no spec (brokers and node-admins keep their storage across a restart and a
    /// retirement; gateways, compute and proof rpc nodes are ephemeral), with no executable, tag or
    /// config. Ingress-only: once a Build is accepted its spec is explicit and nothing re-derives it.
    pub fn default_for(path: PathName) -> Self {
        let storage = match path.kind {
            NodeKind::NodeAdmin | NodeKind::Broker => StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve },
            NodeKind::Gateway | NodeKind::Compute | NodeKind::RpcNode => StorageMeta::Ephemeral,
        };
        Self { path, storage, placement: PlacementMeta::default(), executable: None, tags: BTreeSet::new(), config: BTreeMap::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: the storage state space is exactly three states; the per-kind defaults are the
    /// versioned migration defaults; a spec round-trips its journaled form.
    #[test]
    fn the_kind_defaults_are_the_migration_defaults_and_a_spec_round_trips() {
        let preserve = StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve };
        let at = |p: &str| NodeSpec::default_for(p.parse().unwrap()).storage;
        assert_eq!(at("m.broker.1"), preserve);
        assert_eq!(at("m.admin.1"), preserve);
        assert_eq!(at("m.gateway.1"), StorageMeta::Ephemeral);
        assert_eq!(at("m.compute.1"), StorageMeta::Ephemeral);
        assert_eq!(at("m.rpc.1"), StorageMeta::Ephemeral);
        let release = NodeSpec { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }, config: [("zone".to_string(), "a".to_string())].into(), ..NodeSpec::default_for("m.rpc.1".parse().unwrap()) };
        let json = serde_json::to_string(&release).unwrap();
        assert_eq!(serde_json::from_str::<NodeSpec>(&json).unwrap(), release);
        assert!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"ephemeral","on_retire":"preserve"}"#).unwrap_err().to_string().contains("carries no `on_retire`"), "ephemeral carries no retire disposition");
        assert!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"persistent"}"#).unwrap_err().to_string().contains("requires `on_retire`"), "persistent names its retire disposition");
        assert_eq!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"ephemeral"}"#).unwrap(), StorageMeta::Ephemeral);
    }
}
