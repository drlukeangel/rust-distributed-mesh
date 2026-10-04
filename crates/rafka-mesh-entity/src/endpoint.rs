//! Endpoint slots and their freshness (PRD §1.19, §14; ownership §8.2).
//!
//! Each slot assignment carries an opaque freshness token. A slot whose
//! assignment changes gets a new token; a slot that keeps its assignment keeps
//! its token. Supersession is per exact slot, never whole-peer.

use crate::ids::FreshnessToken;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Per-slot restart policy (`docs/i143/design.md` §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotPolicy {
    /// A new address and token on every process birth.
    Fresh,
    /// A restart keeps the address and token; a replacement does not.
    Stable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointSlot {
    pub slot: String,
    pub addr: SocketAddr,
    pub freshness: FreshnessToken,
}

impl EndpointSlot {
    /// A new assignment of `slot` to `addr`, with a newly minted token.
    pub fn assign(slot: impl Into<String>, addr: SocketAddr) -> Self {
        Self { slot: slot.into(), addr, freshness: FreshnessToken::mint() }
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
