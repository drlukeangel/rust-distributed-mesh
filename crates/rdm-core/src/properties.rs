//! Complete Build property effects shared by launch and downward node commands.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Existing Build and entry-operation identities, scoped by the receiving node ID.
/// Attempts do not change this identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyApplication {
    /// The Build applying the entry.
    pub build_id: String,
    /// The entry location, carried as separate path and name fields.
    /// The receiving record supplies the globally unique node ID.
    pub location: crate::PathName,
}

/// One complete entry-owned effect, persisted together with its application identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryProperties {
    /// The Build entry whose repeat must preserve subsequent node and caller writes.
    pub application: PropertyApplication,
    /// The complete configuration map; an empty map removes all previous values.
    pub config: BTreeMap<String, String>,
    /// The complete tag set; an empty set removes all previous labels.
    pub tags: BTreeSet<String>,
}

impl PropertyApplication {
    /// The one semantic entry operation shared by its config and tag diff steps.
    pub fn entry(build_id: &crate::BuildId, node: &crate::PathName) -> Self {
        Self { build_id: build_id.to_string(), location: node.clone() }
    }
}
