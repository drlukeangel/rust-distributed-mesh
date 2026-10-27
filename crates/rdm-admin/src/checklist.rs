//! The check-in checklist's readers (states.md, "The check-in checklist"): the planned births of the
//! accepted Build, which of them have not checked in, which mesh primaries have not reported, and
//! whether a mesh the fabric-primary decided to recover is back. The checklists that read them are
//! [`crate::fabric_status::FabricStates`]'s.

use crate::accepted::FabricTopology;
use crate::admin::PeerRecovery;
use crate::model::{IncarnationId, NodeKind, PathName};
use crate::status_rpc::MeshReport;
use rdm_core::{MemberStatus, MeshDigest};
use std::collections::BTreeMap;

/// A planned birth that has not checked in this checklist, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Missing {
    /// Who has not checked in.
    pub who: String,
    /// Why.
    pub why: &'static str,
}

impl std::fmt::Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.who, self.why)
    }
}

/// The missing births, named for a span.
pub(crate) fn named(missing: &[Missing]) -> String {
    missing.iter().map(Missing::to_string).collect::<Vec<_>>().join(",")
}

/// The planned births of the accepted Build for `mesh`: every path it names.
pub fn planned(topology: &FabricTopology, mesh: &str) -> Vec<PathName> {
    topology.meshes.get(mesh).map(|m| m.nodes.iter().cloned().collect()).unwrap_or_default()
}

/// The planned births that have not checked in. `me` is the primary running the checklist, ready or not
/// by its own status. `heard` are the digests received from their members since the checklist began;
/// `known` every digest held (to tell a birth never heard from one heard before the checklist).
pub(crate) fn missing_members(planned: &[PathName], me: &PathName, me_ready: bool, heard: &[MeshDigest], known: &[MeshDigest], departed: &dyn Fn(&str) -> bool) -> Vec<Missing> {
    let mut out = Vec::new();
    for path in planned {
        let who = path.to_string();
        if path == me {
            if !me_ready {
                out.push(Missing { who, why: "not-ready" });
            }
            continue;
        }
        let live = |d: &&MeshDigest| &d.node.name == path && !departed(d.node.node_id.as_str());
        let here: Vec<&MeshDigest> = heard.iter().filter(live).collect();
        if here.iter().any(|d| d.status == MemberStatus::ReadyForTraffic) {
            continue;
        }
        let held: Vec<&MeshDigest> = known.iter().filter(|d| &d.node.name == path).collect();
        let why = if !here.is_empty() {
            "not-ready"
        } else if held.is_empty() {
            "no-birth-heard"
        } else if held.iter().all(|d| departed(d.node.node_id.as_str())) {
            "departed"
        } else {
            "not-heard-since-opening"
        };
        out.push(Missing { who, why });
    }
    out
}

/// The meshes whose primary has not reported to the fabric-primary running the checklist. `meshes` is
/// each mesh with its current primary node-admin's birth (`None`: no primary), and whether that
/// primary is `me`; `my_mesh_done` says whether `me`'s own mesh checklist is finished. A report counts
/// only from the exact birth that is the mesh's primary now.
pub(crate) fn fabric_missing(meshes: &[(String, Option<IncarnationId>, bool)], my_mesh_done: bool, reports: &BTreeMap<String, MeshReport>) -> Vec<Missing> {
    let mut out = Vec::new();
    for (mesh, primary, is_me) in meshes {
        let who = mesh.clone();
        let Some(primary) = primary else {
            out.push(Missing { who, why: "no-primary" });
            continue;
        };
        if *is_me {
            if !my_mesh_done {
                out.push(Missing { who, why: "own-mesh-checklist-open" });
            }
            continue;
        }
        match reports.get(mesh) {
            None => out.push(Missing { who, why: "no-report" }),
            Some(r) if &r.incarnation != primary => out.push(Missing { who, why: "report-of-another-birth" }),
            Some(_) => {}
        }
    }
    out
}

/// The report that counts for each of `meshes`, read for the primary `view` names it now
/// ([`crate::status_rpc::Declared::report_of`]).
pub(crate) fn reports_of<'a>(declared: &crate::status_rpc::Declared, view: &crate::topology::Topology, meshes: impl Iterator<Item = &'a str>) -> BTreeMap<String, MeshReport> {
    meshes.filter_map(|m| declared.report_of(m, view.node_type_primary(m, NodeKind::NodeAdmin)).map(|r| (m.to_string(), r))).collect()
}

/// Whether the mesh a fabric-primary decided to recover is back: its current primary is a birth the
/// decision did not find, it reported to this fabric-primary, and it reported after the decision.
/// A ready primary of a newer birth that has not reported is not this.
pub fn recovered(recovery: &PeerRecovery, primary: Option<&IncarnationId>, report: Option<&MeshReport>) -> bool {
    let (Some(primary), Some(report)) = (primary, report) else { return false };
    !recovery.lost.contains(primary) && &report.incarnation == primary && report.at >= recovery.opened_at
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::{FabricId, MeshNode};
    use std::time::Instant;

    fn path(mesh: &str, kind: NodeKind, ord: u32) -> PathName {
        PathName::new(mesh, kind, ord)
    }

    fn digest(p: &PathName, status: MemberStatus) -> MeshDigest {
        MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: MeshNode {
                node_id: crate::model::NodeId::mint(),
                name: p.clone(),
                endpoint_id: crate::model::EndpointId(String::new()),
                transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None, is_primary: None, is_fabric_primary: None
        }
    }

    // @feature: node-lifecycle
    #[test]
    fn checklist_names_every_planned_birth_that_has_not_checked_in_since_the_opening() {
        let (a1, r1, r2, r3) = (path("mesh1", NodeKind::NodeAdmin, 1), path("mesh1", NodeKind::RpcNode, 1), path("mesh1", NodeKind::RpcNode, 2), path("mesh1", NodeKind::RpcNode, 3));
        let planned = vec![a1.clone(), r1.clone(), r2.clone(), r3.clone()];
        let ready = digest(&r1, MemberStatus::ReadyForTraffic);
        let pending = digest(&r2, MemberStatus::Pending);
        let before_opening = digest(&r3, MemberStatus::ReadyForTraffic);
        let none = |_: &str| false;
        // r1 checked in ready; r2 checked in but is not ready; r3 was heard only before the checklist began.
        let m = missing_members(&planned, &a1, true, &[ready.clone(), pending.clone()], &[ready.clone(), pending.clone(), before_opening.clone()], &none);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh1.rpc.2(not-ready)", "mesh1.rpc.3(not-heard-since-opening)"]);
        // A planned birth never heard at all is named as such, and the primary itself counts only when ready.
        let m = missing_members(&planned, &a1, false, &[ready.clone()], &[ready.clone()], &none);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh1.admin.1(not-ready)", "mesh1.rpc.2(no-birth-heard)", "mesh1.rpc.3(no-birth-heard)"]);
        // A departed birth does not check in.
        let gone = |id: &str| id == ready.node.node_id.as_str();
        let m = missing_members(&[r1.clone()], &a1, true, &[ready.clone()], &[ready.clone()], &gone);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh1.rpc.1(departed)"]);
        // Every planned birth checked in: nothing is missing.
        let all: Vec<MeshDigest> = [&r1, &r2, &r3].iter().map(|p| digest(p, MemberStatus::ReadyForTraffic)).collect();
        assert!(missing_members(&planned, &a1, true, &all, &all, &none).is_empty());
    }

    fn report(incarnation: &IncarnationId, at: Instant) -> MeshReport {
        MeshReport { incarnation: incarnation.clone(), at }
    }

    // @feature: node-lifecycle
    #[test]
    fn fabric_checklist_waits_for_a_report_from_each_mesh_primarys_current_birth() {
        let (old, new, other) = (IncarnationId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        let meshes = vec![("mesh1".to_string(), Some(other.clone()), true), ("mesh2".to_string(), Some(new.clone()), false), ("mesh3".to_string(), None, false)];
        let mut reports = BTreeMap::new();
        let m = fabric_missing(&meshes, true, &reports);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh2(no-report)", "mesh3(no-primary)"]);
        // A report of the birth the mesh's primary replaced is not the current primary's report.
        reports.insert("mesh2".to_string(), report(&old, t0));
        let m = fabric_missing(&meshes[..2], true, &reports);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh2(report-of-another-birth)"]);
        reports.insert("mesh2".to_string(), report(&new, t0));
        assert!(fabric_missing(&meshes[..2], true, &reports).is_empty());
        // The holder's own mesh counts only once its own checklist is finished.
        let m = fabric_missing(&meshes[..2], false, &reports);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh1(own-mesh-checklist-open)"]);
    }

    // @feature: node-lifecycle
    #[test]
    fn degraded_clears_only_on_the_current_primarys_report_after_the_decision() {
        let (lost, recovered_id) = (IncarnationId::mint(), IncarnationId::mint());
        let opened_at = Instant::now();
        let recovery = PeerRecovery { mesh: "mesh2".into(), verdict_rafka_ms: Some(1), opened_at, lost: vec![lost.clone()] };
        let later = opened_at + std::time::Duration::from_millis(5);
        // A ready primary of a newer birth that has not reported does not clear it.
        assert!(!recovered(&recovery, Some(&recovered_id), None));
        // The birth the decision found never does, reported or not.
        assert!(!recovered(&recovery, Some(&lost), Some(&report(&lost, later))));
        // A report from a birth that is not the primary now is not the primary's.
        assert!(!recovered(&recovery, Some(&recovered_id), Some(&report(&IncarnationId::mint(), later))));
        // A report received before the decision is not fresh.
        assert!(!recovered(&recovery, Some(&recovered_id), Some(&report(&recovered_id, opened_at - std::time::Duration::from_millis(1)))));
        assert!(!recovered(&recovery, None, Some(&report(&recovered_id, later))));
        assert!(recovered(&recovery, Some(&recovered_id), Some(&report(&recovered_id, later))));
    }
}
