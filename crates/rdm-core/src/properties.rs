//! Complete Build property effects shared by launch and downward node commands.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// One complete entry-owned effect: the entry's config and tags, persisted together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryProperties {
    /// The complete configuration map; an empty map removes all previous values.
    pub config: BTreeMap<String, String>,
    /// The complete tag set; an empty set removes all previous labels.
    pub tags: BTreeSet<String>,
}
