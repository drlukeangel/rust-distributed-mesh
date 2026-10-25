//! The typed desired-state config of a path: what the accepted Build says a path.name should do.
//! Intrinsic, explicit on every materialized path after ingress normalization; never a tag, never
//! inferred later from a flag, a map or a provider's behaviour.

use crate::path::NodeKind;
use serde::{Deserialize, Serialize};

/// The defaults set at a path that every node created there inherits (here storage and
/// placement): the accepted Build's per-path policy, as distinct from a node's own config bag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathConfig {
    /// What survives a restart or a retirement.
    pub storage: StorageMeta,
    /// Placement constraints.
    pub placement: PlacementMeta,
    /// The executable a node created at the path runs (path, sha256 and, under the container
    /// provider, the image): named per node by the request that creates the path, compiled onto the
    /// accepted topology's entry, recorded on the node's row at the claim and re-hashed at the launch
    /// (proposal executable-bindings-at-create.md rule 1). An entry that differs from what the row
    /// records is that node's replacement. A Build accepted before the field decodes with none: valid
    /// history, its births' rows the record of what they run.
    #[serde(default)]
    pub executable: Option<crate::binding::Binding>,
    /// The tag labels a node created at the path carries from its name claim (the request's node entry,
    /// proposal executable-bindings-at-create.md rule 1: "with `tags`, `storage`, `placement` and
    /// `executable`"); a node recovered or replaced at the path takes its predecessor's row's
    /// (node-replace.md "Inherited"). A Build accepted before the field decodes with none.
    #[serde(default)]
    pub tags: std::collections::BTreeSet<String>,
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
/// wire's postcard, where a request's [`PathConfig`] travels) carries [`StorageMetaPositional`], since
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

impl PathConfig {
    /// The migration default for a kind: what ingress normalization gives a materialized path that
    /// declared no path config (brokers and node-admins keep their storage across a restart and a
    /// retirement; gateways, compute and proof rpc nodes are ephemeral). Ingress-only: once a Build
    /// is accepted its path config is explicit and nothing re-derives it.
    pub fn default_for(kind: NodeKind) -> Self {
        let storage = match kind {
            NodeKind::NodeAdmin | NodeKind::Broker => StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve },
            NodeKind::Gateway | NodeKind::Compute | NodeKind::RpcNode => StorageMeta::Ephemeral,
        };
        Self { storage, placement: PlacementMeta::default(), executable: None, tags: Default::default() }
    }
}

/// What a legacy input (a shape build, a spawn request) may say about one path's storage before
/// normalization.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyPathConfigInput {
    /// The path's explicit typed config, when the input carries one.
    pub explicit: Option<PathConfig>,
    /// A legacy semantic `stateful` choice the caller made (never a hardcoded field the old API
    /// could not express: that is `None`).
    pub stateful: Option<bool>,
}

/// Ingress normalization, the one place a default is applied: explicit path config first, then an
/// explicit legacy `stateful` intent, then the versioned per-kind default. After this the Build
/// is explicit.
pub fn normalize_path_config(kind: NodeKind, legacy: LegacyPathConfigInput) -> PathConfig {
    if let Some(explicit) = legacy.explicit {
        return explicit;
    }
    match legacy.stateful {
        Some(true) => PathConfig { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve }, placement: PlacementMeta::default(), executable: None, tags: Default::default() },
        Some(false) => PathConfig { storage: StorageMeta::Ephemeral, placement: PlacementMeta::default(), executable: None, tags: Default::default() },
        None => PathConfig::default_for(kind),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: the storage state space is exactly three states; the per-kind defaults are the
    /// versioned migration defaults; normalization takes an explicit path config over legacy intent over the
    /// default, and a hardcoded legacy false (represented as `None`) never overrides the default.
    #[test]
    fn normalization_takes_explicit_over_legacy_intent_over_the_kind_default() {
        let preserve = StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve };
        assert_eq!(PathConfig::default_for(NodeKind::Broker).storage, preserve);
        assert_eq!(PathConfig::default_for(NodeKind::NodeAdmin).storage, preserve);
        assert_eq!(PathConfig::default_for(NodeKind::Gateway).storage, StorageMeta::Ephemeral);
        assert_eq!(PathConfig::default_for(NodeKind::Compute).storage, StorageMeta::Ephemeral);
        assert_eq!(PathConfig::default_for(NodeKind::RpcNode).storage, StorageMeta::Ephemeral);
        let release = PathConfig { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }, placement: PlacementMeta::default(), executable: None, tags: Default::default() };
        assert_eq!(normalize_path_config(NodeKind::Gateway, LegacyPathConfigInput { explicit: Some(release.clone()), stateful: Some(false) }), release);
        assert_eq!(normalize_path_config(NodeKind::Gateway, LegacyPathConfigInput { explicit: None, stateful: Some(true) }).storage, preserve);
        assert_eq!(normalize_path_config(NodeKind::Broker, LegacyPathConfigInput { explicit: None, stateful: Some(false) }).storage, StorageMeta::Ephemeral);
        assert_eq!(normalize_path_config(NodeKind::Broker, LegacyPathConfigInput::default()).storage, preserve, "an unexpressed legacy field is not intent");
        let json = serde_json::to_string(&release).unwrap();
        assert_eq!(serde_json::from_str::<PathConfig>(&json).unwrap(), release);
        assert!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"ephemeral","on_retire":"preserve"}"#).unwrap_err().to_string().contains("carries no `on_retire`"), "ephemeral carries no retire disposition");
        assert!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"persistent"}"#).unwrap_err().to_string().contains("requires `on_retire`"), "persistent names its retire disposition");
        assert_eq!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"ephemeral"}"#).unwrap(), StorageMeta::Ephemeral);
    }
}
