//! Per-cohort primary election (PRD §11; `docs/i143/design.md` §3).
//!
//! A cohort is the members of one kind in one mesh. Its primary is the
//! member that has been ready for traffic longest: among the members that
//! are `ReadyForTraffic` in the observer's view, the one with the earliest
//! election claim (`ready_since_ms`, which each birth publishes once in its own
//! membership digest). Ties go to the lowest ordinal: a view holds one birth
//! per path, so the order is total within a cohort. Incarnations are never
//! ordered.
//!
//! Every observer reads the same claims, so every converged view names the
//! same primary; no vote and no message beyond membership is involved. The
//! rule gives the matrix its shape:
//! - a new member (grow) or a new birth (restart, a recreated path) claims a
//!   later instant, so it never displaces the incumbent;
//! - when the primary leaves the view (killed, removed, retired), the next
//!   oldest member succeeds it, and there is exactly one;
//! - a partition lets each side elect from what it hears (a transient split);
//!   on heal every view sees the same claims again and agrees.
//!
//! A member whose digest makes no claim ranks after every member that does.
//!
//! The fabric primary is the admin primary of the lowest-named mesh that has
//! one, so when that mesh is lost the next mesh's admin primary holds the
//! fabric.
//!
//! An admin reports each change of a cohort's primary in its own view as
//! `rafka.mesh.election.resolve.via-recompute`, and each change of the
//! fabric primary as `rafka.mesh.election.resolve.via-fabric-recompute`
//! (`ElectionLog`).

use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// One member of a cohort as the election sees it.
#[derive(Debug, Clone)]
pub struct Candidate<'a> {
    pub name: &'a PathName,
    /// `ready_since_ms` from the member's digest.
    pub ready_since: Option<u64>,
}

/// The index of the cohort's primary among `ready` members (every candidate
/// passed is ready for traffic); `None` for an empty cohort.
pub fn elect(ready: &[Candidate<'_>]) -> Option<usize> {
    (0..ready.len()).min_by_key(|&i| (ready[i].ready_since.unwrap_or(u64::MAX), ready[i].name.ordinal))
}

/// What one admin last saw as each cohort's primary; reports every change.
pub struct ElectionLog {
    observer: String,
    last: Mutex<BTreeMap<(String, NodeKind), Option<PathName>>>,
    /// `None` until the first view.
    last_fabric: Mutex<Option<Option<PathName>>>,
}

impl ElectionLog {
    pub fn new(observer: impl Into<String>) -> Self {
        Self { observer: observer.into(), last: Mutex::new(BTreeMap::new()), last_fabric: Mutex::new(None) }
    }

    /// Record `t`'s primaries; a cohort whose primary changed since the last
    /// view (including to or from none) emits one
    /// `rafka.mesh.election.resolve.via-recompute`.
    pub fn observe(&self, t: &Topology) {
        let mut now: BTreeMap<(String, NodeKind), Option<PathName>> = BTreeMap::new();
        for n in &t.nodes {
            let e = now.entry((n.mesh.clone(), n.kind)).or_default();
            if n.is_primary {
                *e = Some(n.name.clone());
            }
        }
        let mut last = self.last.lock().unwrap();
        for ((mesh, kind), primary) in &now {
            let previous = last.get(&(mesh.clone(), *kind)).cloned().flatten();
            if last.contains_key(&(mesh.clone(), *kind)) && previous == *primary {
                continue;
            }
            let members = t.cohort(mesh, *kind).count();
            let ready = t.cohort(mesh, *kind).filter(|n| n.status == crate::model::NodeStatus::ReadyForTraffic).count();
            let span = tracing::info_span!(
                parent: None,
                "rafka.mesh.election.resolve.via-recompute",
                observer = %self.observer,
                mesh = %mesh,
                kind = kind_name(*kind),
                primary = %primary.as_ref().map(ToString::to_string).unwrap_or_default(),
                previous = %previous.as_ref().map(ToString::to_string).unwrap_or_default(),
                members,
                ready,
            );
            span.in_scope(|| tracing::info!("cohort primary resolved"));
        }
        // A cohort that left the view entirely is forgotten.
        *last = now;
        drop(last);

        let fabric = t.fabric_primary().map(|n| n.name.clone());
        let mut last_fabric = self.last_fabric.lock().unwrap();
        if last_fabric.as_ref() != Some(&fabric) {
            let previous = last_fabric.clone().flatten();
            let span = tracing::info_span!(
                parent: None,
                "rafka.mesh.election.resolve.via-fabric-recompute",
                observer = %self.observer,
                fabric = %t.fabric.name,
                primary = %fabric.as_ref().map(ToString::to_string).unwrap_or_default(),
                previous = %previous.as_ref().map(ToString::to_string).unwrap_or_default(),
                meshes = t.meshes.len(),
            );
            span.in_scope(|| tracing::info!("fabric primary resolved"));
            *last_fabric = Some(fabric);
        }
    }
}

fn kind_name(k: NodeKind) -> &'static str {
    match k {
        NodeKind::NodeAdmin => "node_admin",
        NodeKind::RpcNode => "rpc_node",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PathName {
        s.parse().unwrap()
    }

    fn c<'a>(name: &'a PathName, since: Option<u64>) -> Candidate<'a> {
        Candidate { name, ready_since: since }
    }

    #[test]
    fn the_longest_ready_member_is_primary_whatever_its_ordinal() {
        let (r1, r2, r3) = (p("mesh1.rpc.1"), p("mesh1.rpc.2"), p("mesh1.rpc.3"));
        // rpc.1 is a recreated path (a later birth): rpc.2 stays primary.
        assert_eq!(elect(&[c(&r1, Some(300)), c(&r2, Some(100)), c(&r3, Some(200))]), Some(1));
        // The incumbent leaves: the next oldest succeeds, exactly one.
        assert_eq!(elect(&[c(&r1, Some(300)), c(&r3, Some(200))]), Some(1));
        assert_eq!(elect(&[]), None);
    }

    #[test]
    fn ties_go_to_the_lowest_ordinal_and_a_member_without_a_claim_ranks_last() {
        let (r1, r2) = (p("mesh1.rpc.1"), p("mesh1.rpc.2"));
        assert_eq!(elect(&[c(&r2, Some(100)), c(&r1, Some(100))]), Some(1));
        assert_eq!(elect(&[c(&r1, None), c(&r2, Some(u64::MAX - 1))]), Some(1));
        assert_eq!(elect(&[c(&r2, None), c(&r1, None)]), Some(1), "no claims at all: lowest ordinal");
    }
}
