//! The typed desired-state properties of a logical node: what the accepted Build says a path.name
//! should do. Intrinsic, explicit on every materialized path after ingress normalization; never
//! a tag, never inferred later from a flag, a map or a provider's behaviour.

use crate::path::NodeKind;
use serde::{Deserialize, Serialize};

/// The intrinsic typed properties of one logical node path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeMeta {
    pub storage: StorageMeta,
    pub placement: PlacementMeta,
}

/// What survives a topology-preserving runtime loss or restart, and what happens to persistent
/// storage when the logical node is intentionally retired. The type holds exactly the three
/// meaningful states; no invalid combination is representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "persistence", rename_all = "kebab-case", try_from = "StorageMetaRow")]
pub enum StorageMeta {
    /// Fresh storage at every birth; nothing survives a restart or a retirement.
    Ephemeral,
    /// Storage survives a restart; `on_retire` says what an intentional retirement does with it.
    Persistent { on_retire: PersistentRetireDisposition },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PersistentRetireDisposition {
    /// Retirement leaves the storage where it is.
    Preserve,
    /// Retirement releases the storage through an explicit provider action on its exact locator.
    Release,
}

/// Placement constraints of the logical node. None are declared in this build; the type exists
/// so a constraint, when it comes, is typed Meta and never a tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementMeta {}

/// The version of the per-kind migration defaults below: a change to what a kind gets by default
/// is a new version, so an accepted Build can say which defaults normalized it.
pub const NODE_META_DEFAULTS_VERSION: u32 = 1;

impl NodeMeta {
    /// The migration default for a kind: what ingress normalization gives a materialized path that
    /// declared no storage meta (brokers and node-admins keep their storage across a restart and a
    /// retirement; gateways, compute and proof rpc nodes are ephemeral). Ingress-only: once a Build
    /// is accepted its meta is explicit and nothing re-derives it.
    pub fn default_for(kind: NodeKind) -> Self {
        let storage = match kind {
            NodeKind::NodeAdmin | NodeKind::Broker => StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve },
            NodeKind::Gateway | NodeKind::Compute | NodeKind::RpcNode => StorageMeta::Ephemeral,
        };
        Self { storage, placement: PlacementMeta::default() }
    }
}

/// What a legacy input (a shape build, a spawn request) may say about one path's storage before
/// normalization.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyNodeMetaInput {
    /// The path's explicit typed meta, when the input carries one.
    pub explicit: Option<NodeMeta>,
    /// A legacy semantic `stateful` choice the caller made (never a hardcoded field the old API
    /// could not express: that is `None`).
    pub stateful: Option<bool>,
}

/// Ingress normalization, the one place a default is applied: explicit meta first, then an
/// explicit legacy `stateful` intent, then the versioned per-kind default. After this the Build
/// is explicit.
pub fn normalize_node_meta(kind: NodeKind, legacy: LegacyNodeMetaInput) -> NodeMeta {
    if let Some(explicit) = legacy.explicit {
        return explicit;
    }
    match legacy.stateful {
        Some(true) => NodeMeta { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve }, placement: PlacementMeta::default() },
        Some(false) => NodeMeta { storage: StorageMeta::Ephemeral, placement: PlacementMeta::default() },
        None => NodeMeta::default_for(kind),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: the storage state space is exactly three states; the per-kind defaults are the
    /// versioned migration defaults; normalization takes explicit meta over legacy intent over the
    /// default, and a hardcoded legacy false (represented as `None`) never overrides the default.
    #[test]
    fn normalization_takes_explicit_over_legacy_intent_over_the_kind_default() {
        let preserve = StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve };
        assert_eq!(NodeMeta::default_for(NodeKind::Broker).storage, preserve);
        assert_eq!(NodeMeta::default_for(NodeKind::NodeAdmin).storage, preserve);
        assert_eq!(NodeMeta::default_for(NodeKind::Gateway).storage, StorageMeta::Ephemeral);
        assert_eq!(NodeMeta::default_for(NodeKind::Compute).storage, StorageMeta::Ephemeral);
        assert_eq!(NodeMeta::default_for(NodeKind::RpcNode).storage, StorageMeta::Ephemeral);
        let release = NodeMeta { storage: StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }, placement: PlacementMeta::default() };
        assert_eq!(normalize_node_meta(NodeKind::Gateway, LegacyNodeMetaInput { explicit: Some(release.clone()), stateful: Some(false) }), release);
        assert_eq!(normalize_node_meta(NodeKind::Gateway, LegacyNodeMetaInput { explicit: None, stateful: Some(true) }).storage, preserve);
        assert_eq!(normalize_node_meta(NodeKind::Broker, LegacyNodeMetaInput { explicit: None, stateful: Some(false) }).storage, StorageMeta::Ephemeral);
        assert_eq!(normalize_node_meta(NodeKind::Broker, LegacyNodeMetaInput::default()).storage, preserve, "an unexpressed legacy field is not intent");
        let json = serde_json::to_string(&release).unwrap();
        assert_eq!(serde_json::from_str::<NodeMeta>(&json).unwrap(), release);
        assert!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"ephemeral","on_retire":"preserve"}"#).unwrap_err().to_string().contains("carries no `on_retire`"), "ephemeral carries no retire disposition");
        assert!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"persistent"}"#).unwrap_err().to_string().contains("requires `on_retire`"), "persistent names its retire disposition");
        assert_eq!(serde_json::from_str::<StorageMeta>(r#"{"persistence":"ephemeral"}"#).unwrap(), StorageMeta::Ephemeral);
    }
}
