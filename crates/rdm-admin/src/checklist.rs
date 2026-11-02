//! The check-in checklist's readers (states.md, "The check-in checklist"): the planned births of the
//! accepted Build, which of them have not checked in, and which mesh primaries have not reported. The
//! checklists that read them are
//! [`crate::fabric_status::FabricStates`]'s.

use crate::accepted::FabricTopology;
use crate::model::{NodeKind, PathName};
use crate::status_rpc::MeshReport;
use rdm_core::framing::Birth;
use rdm_core::{MemberStatus, MeshDigest};
use std::collections::BTreeMap;

/// A checklist subject keeps node identity distinct from a planned location or a label.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MissingSubject {
    /// An existing node selected by globally unique ID.
    Node(crate::model::NodeId),
    /// An unresolved allocation with separate path and name.
    Location(PathName),
    /// A diagnostic label that is never parsed as identity.
    Label(String),
}

impl From<String> for MissingSubject {
    fn from(label: String) -> Self { Self::Label(label) }
}
impl From<&str> for MissingSubject {
    fn from(label: &str) -> Self { Self::Label(label.into()) }
}
impl std::fmt::Display for MissingSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node(id) => write!(f, "node_id={id}"),
            Self::Location(at) => write!(f, "path={:?}, name={:?}", at.path, at.name),
            Self::Label(label) => f.write_str(label),
        }
    }
}

/// A planned birth that has not checked in this checklist, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Missing {
    /// Who has not checked in.
    pub who: MissingSubject,
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

/// The planned births that have not checked in. `me` is the primary ID; `current` resolves each
/// separate path/name pair to its exact current birth. The primary is ready or not
/// by its own status. `heard` are the digests received from their members since the checklist began;
/// `known` every digest held (to tell a birth never heard from one heard before the checklist). A path
/// `stopped` names is at rest by command and holds no checklist (states.md:16, Q240): it sends no
/// digest and is never missing.
pub(crate) fn missing_members(planned: &[PathName], me: &crate::model::NodeId, current: &dyn Fn(&PathName) -> Option<Birth>, me_ready: bool, heard: &[MeshDigest], known: &[MeshDigest], deleted: &dyn Fn(&str) -> bool, stopped: &dyn Fn(&PathName) -> bool) -> Vec<Missing> {
    let mut out = Vec::new();
    for path in planned {
        if stopped(path) {
            continue;
        }
        let birth = current(path);
        let who = birth.as_ref().map(|b| MissingSubject::Node(b.node_id.clone())).unwrap_or_else(|| MissingSubject::Location(path.clone()));
        if birth.as_ref().is_some_and(|b| &b.node_id == me) {
            if !me_ready {
                out.push(Missing { who, why: "not-ready" });
            }
            continue;
        }
        let matches = |d: &&MeshDigest| birth.as_ref().map_or(&d.node.name == path, |b| d.node.node_id == b.node_id && d.node.incarnation == b.incarnation);
        let live = |d: &&MeshDigest| matches(d) && !deleted(d.node.node_id.as_str());
        let here: Vec<&MeshDigest> = heard.iter().filter(live).collect();
        if birth.is_some() && here.iter().any(|d| d.status == MemberStatus::Ready) {
            continue;
        }
        let held: Vec<&MeshDigest> = known.iter().filter(matches).collect();
        let why = if birth.is_none() {
            "no-current-birth"
        } else if !here.is_empty() {
            "not-ready"
        } else if held.is_empty() {
            "no-birth-heard"
        } else if held.iter().all(|d| deleted(d.node.node_id.as_str())) {
            "deleted"
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
pub(crate) fn fabric_missing(meshes: &[(String, Option<Birth>, bool)], my_mesh_done: bool, reports: &BTreeMap<String, MeshReport>) -> Vec<Missing> {
    let mut out = Vec::new();
    for (mesh, primary, is_me) in meshes {
        let who = MissingSubject::Label(mesh.clone());
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
            Some(r) if &r.birth != primary => out.push(Missing { who, why: "report-of-another-birth" }),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IncarnationId;
    use rdm_core::{FabricId, MeshNode};
    use std::time::Instant;

    fn path(mesh: &str, kind: NodeKind, ord: u32) -> PathName {
        PathName::numbered(mesh, kind, ord)
    }

    fn digest(p: &PathName, status: MemberStatus) -> MeshDigest {
        MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: MeshNode { mesh: "mesh1".into(), kind: NodeKind::RpcNode,
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
        let ready = digest(&r1, MemberStatus::Ready);
        let pending = digest(&r2, MemberStatus::Pending);
        let before_opening = digest(&r3, MemberStatus::Ready);
        let me = birth();
        let none = |_: &str| false;
        let no_stop = |_: &PathName| false;
        let resolve = |at: &PathName| {
            if at == &a1 { return Some(me.clone()); }
            [&ready, &pending, &before_opening].into_iter().find(|d| &d.node.name == at)
                .map(|d| Birth { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() })
        };
        let subject = |d: &MeshDigest| MissingSubject::Node(d.node.node_id.clone());
        let m = missing_members(&planned, &me.node_id, &resolve, true, &[ready.clone(), pending.clone()], &[ready.clone(), pending.clone(), before_opening.clone()], &none, &no_stop);
        assert_eq!(m, vec![Missing { who: subject(&pending), why: "not-ready" }, Missing { who: subject(&before_opening), why: "not-heard-since-opening" }]);
        let unresolved = |at: &PathName| if at == &a1 { Some(me.clone()) } else if at == &r1 { resolve(at) } else { None };
        let m = missing_members(&planned, &me.node_id, &unresolved, false, &[ready.clone()], &[ready.clone()], &none, &no_stop);
        assert_eq!(m, vec![Missing { who: MissingSubject::Node(me.node_id.clone()), why: "not-ready" }, Missing { who: MissingSubject::Location(r2.clone()), why: "no-current-birth" }, Missing { who: MissingSubject::Location(r3.clone()), why: "no-current-birth" }]);
        let gone = |id: &str| id == ready.node.node_id.as_str();
        let m = missing_members(&[r1.clone()], &me.node_id, &resolve, true, &[ready.clone()], &[ready.clone()], &gone, &no_stop);
        assert_eq!(m, vec![Missing { who: subject(&ready), why: "deleted" }]);
        let mut all = vec![ready.clone(), pending.clone(), before_opening.clone()];
        for d in &mut all { d.status = MemberStatus::Ready; }
        assert!(missing_members(&planned, &me.node_id, &resolve, true, &all, &all, &none, &no_stop).is_empty());
        let r3_stopped = |p: &PathName| p == &r3;
        let m = missing_members(&planned, &me.node_id, &unresolved, true, &[ready.clone()], &[ready.clone()], &none, &r3_stopped);
        assert_eq!(m, vec![Missing { who: MissingSubject::Location(r2), why: "no-current-birth" }]);

        // A replacement at the same location never inherits the old node's self exemption,
        // even when its incarnation value happens to match.
        let replacement = Birth { node_id: crate::model::NodeId::mint(), incarnation: me.incarnation.clone() };
        let m = missing_members(&[a1], &me.node_id, &|_| Some(replacement.clone()), true, &[], &[], &none, &no_stop);
        assert_eq!(m, vec![Missing { who: MissingSubject::Node(replacement.node_id), why: "no-birth-heard" }]);
        let unresolved_stale = missing_members(&[r1.clone()], &me.node_id, &|_| None, true, &[ready.clone()], &[ready.clone()], &none, &no_stop);
        assert_eq!(unresolved_stale, vec![Missing { who: MissingSubject::Location(r1.clone()), why: "no-current-birth" }]);
        let different = Birth { node_id: crate::model::NodeId::mint(), incarnation: ready.node.incarnation.clone() };
        assert!(!missing_members(&[r1], &me.node_id, &|_| Some(different.clone()), true, &[ready.clone()], &[ready], &none, &no_stop).is_empty());
    }

    fn birth() -> Birth { Birth { node_id: crate::model::NodeId::mint(), incarnation: IncarnationId::mint() } }

    fn report(birth: &Birth, at: Instant) -> MeshReport {
        MeshReport { birth: birth.clone(), at }
    }

    // @feature: node-lifecycle
    #[test]
    fn fabric_checklist_waits_for_a_report_from_each_mesh_primarys_current_birth() {
        let (old, new, other) = (birth(), birth(), birth());
        let t0 = Instant::now();
        let meshes = vec![("mesh1".to_string(), Some(other.clone()), true), ("mesh2".to_string(), Some(new.clone()), false), ("mesh3".to_string(), None, false)];
        let mut reports = BTreeMap::new();
        let m = fabric_missing(&meshes, true, &reports);
        assert_eq!(m, vec![Missing { who: "mesh2".into(), why: "no-report" }, Missing { who: "mesh3".into(), why: "no-primary" }]);
        // A report of the birth the mesh's primary replaced is not the current primary's report.
        reports.insert("mesh2".to_string(), report(&old, t0));
        let m = fabric_missing(&meshes[..2], true, &reports);
        assert_eq!(m, vec![Missing { who: "mesh2".into(), why: "report-of-another-birth" }]);
        let same_incarnation = Birth { node_id: crate::model::NodeId::mint(), incarnation: new.incarnation.clone() };
        reports.insert("mesh2".to_string(), report(&same_incarnation, t0));
        assert_eq!(fabric_missing(&meshes[..2], true, &reports)[0].why, "report-of-another-birth");
        reports.insert("mesh2".to_string(), report(&new, t0));
        assert!(fabric_missing(&meshes[..2], true, &reports).is_empty());
        // The holder's own mesh counts only once its own checklist is finished.
        let m = fabric_missing(&meshes[..2], false, &reports);
        assert_eq!(m, vec![Missing { who: "mesh1".into(), why: "own-mesh-checklist-open" }]);
    }
}
