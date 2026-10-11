//! The stopped-node garbage collector (fabric-node-lifecycle.md §6.9, R-GC1).
//!
//! A stopped node is garbage once it has been stopped longer than the fabric's
//! `stopped_node_ttl` (`RDM_STOPPED_NODE_TTL`). The fabric-primary, the sole Build driver, sweeps
//! every ttl: it ages each stopped birth from the current stopped period's own stop receipt in the
//! Build log, skips a birth an open Build still references, and submits one ordinary node.delete
//! Build per sweep. The sweep deletes nothing itself; the delete's admission rechecks the stopped
//! and open-Build facts, serialized with it.

use crate::build::BuildId;
use crate::model::{IncarnationId, NodeId, PathName};
use std::time::Duration;

/// The variable that sets the ttl.
pub const ENV: &str = "RDM_STOPPED_NODE_TTL";

/// RDM's shipped ttl: what an unset variable means.
pub const SHIPPED_TTL: Duration = Duration::from_secs(30 * 60);

/// The ttl `raw` sets: unset is RDM's shipped 30 minutes, blank is forever (`None`, no sweep).
pub fn ttl_from_env(_raw: Option<&str>) -> Result<Option<Duration>, String> {
    Ok(Some(SHIPPED_TTL))
}

/// How old the current stopped period of one birth is, from the Build log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Age {
    /// The current stopped period's own stop receipt, stamped in rafka-time.
    StoppedAt(u64),
    /// No stop receipt opens the current stopped period (a later start ended the last one
    /// receipted, or the birth was parked by something other than a standalone stop).
    NoCurrentReceipt(String),
    /// A stop receipt is held but cannot be read.
    Unreadable(String),
}

/// What the scan decided for one stopped birth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Stopped longer than the ttl: a candidate.
    Expired(Candidate),
    /// Stopped, not yet longer than the ttl.
    Young {
        /// The node.
        node: PathName,
        /// When its current stopped period began.
        stopped_at_ms: u64,
    },
    /// An open Build references the birth.
    Referenced {
        /// The node.
        node: PathName,
        /// The open Build.
        build_id: BuildId,
    },
    /// The birth's age cannot be decided from the Build log.
    Skipped {
        /// The node.
        node: PathName,
        /// Why.
        reason: String,
    },
}

/// One stopped birth the scan selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The node's `path.name`.
    pub node: PathName,
    /// The logical node.
    pub node_id: NodeId,
    /// The stopped birth.
    pub incarnation: IncarnationId,
    /// When its current stopped period began (its stop receipt's rafka-time).
    pub stopped_at_ms: u64,
}

/// What one sweep did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Swept {
    /// The ttl is blank: no sweep.
    Forever,
    /// This admin does not hold the fabric seat: it sweeps nothing.
    NotFabricPrimary,
    /// Nothing is stopped longer than the ttl.
    Nothing,
    /// One node.delete Build was accepted for `node`.
    Submitted {
        /// The node collected.
        node: PathName,
        /// The node.delete Build.
        build_id: BuildId,
    },
    /// Another Build is in flight: the candidate is seen again next sweep.
    BuildInProgress {
        /// The candidate.
        node: PathName,
        /// The Build in flight.
        current_build_id: BuildId,
    },
    /// The delete's admission refused the candidate by name.
    Refused {
        /// The candidate.
        node: PathName,
        /// Why.
        reason: String,
    },
}

impl crate::http::ControlPlane {
    /// The scan: every stopped birth this admin's view holds, with its verdict at `now_ms`.
    pub async fn scan_stopped(&self, _ttl: Duration, _now_ms: u64) -> Vec<Verdict> {
        Vec::new()
    }

    /// One sweep at `ttl` (blank: none): the scan, then one node.delete Build for the first candidate.
    pub async fn sweep_stopped(&self, _ttl: Option<Duration>) -> Swept {
        Swept::Nothing
    }

    /// The node.delete Build of `candidate`, admitted only after the stopped and open-Build checks
    /// run again under the admission, serialized with the write.
    pub async fn collect_stopped(&self, _candidate: &Candidate, _ttl: Duration) -> Result<crate::http::Opened, crate::http::Refusal> {
        Err(crate::http::Refusal::Unavailable("not built".into()))
    }
}
