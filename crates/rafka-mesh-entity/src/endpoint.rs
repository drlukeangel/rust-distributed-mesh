//! Endpoint slots and their freshness (PRD §1.19, §14; ownership §8.2).
//!
//! A slot is a logical invocation fence, not a transport endpoint: it owns no
//! socket, port, NAT candidate or QUIC connection. A process has one Iroh
//! endpoint at one address; every slot rides it, and a request names the
//! slot it targets under the slot's current freshness token. A slot whose
//! token moves is superseded for callers pinned to the old token; its
//! siblings, and the process's connections, are untouched.

use crate::ids::FreshnessToken;
use serde::{Deserialize, Serialize};

/// Per-slot restart policy: what happens to the slot's freshness token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotPolicy {
    /// A new token on every process birth.
    Fresh,
    /// A restart of the same logical node keeps the token; a replacement
    /// mints a new one.
    Stable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointSlot {
    pub slot: String,
    pub freshness: FreshnessToken,
}

impl EndpointSlot {
    /// `slot` under a newly minted token.
    pub fn fresh(slot: impl Into<String>) -> Self {
        Self { slot: slot.into(), freshness: FreshnessToken::mint() }
    }
}

/// A node's complete endpoint set, keyed by slot name.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EndpointSet(pub Vec<EndpointSlot>);

impl EndpointSet {
    pub fn get(&self, slot: &str) -> Option<&EndpointSlot> {
        self.0.iter().find(|s| s.slot == slot)
    }

    /// Slots whose freshness differs between `self` (old) and `new`: the
    /// exact slots a pool must supersede. A slot only in `old` is superseded
    /// with no successor; an unchanged slot is never listed.
    pub fn superseded_by(&self, new: &EndpointSet) -> Vec<(String, FreshnessToken, Option<FreshnessToken>)> {
        self.0
            .iter()
            .filter_map(|old| match new.get(&old.slot) {
                Some(n) if n.freshness == old.freshness => None,
                Some(n) => Some((old.slot.clone(), old.freshness.clone(), Some(n.freshness.clone()))),
                None => Some((old.slot.clone(), old.freshness.clone(), None)),
            })
            .collect()
    }
}
