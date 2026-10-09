//! Same status, fresh proof (R-S3): the round a new primary runs before it republishes.
//!
//! A newly elected or reborn primary adopts the lifecycle state it hears without inventing a
//! transition (takeover is hydration), but it may not stay silent. It re-sends the round's down op
//! to every member of its checklist, and the checklist is rebuilt from fresh check-ins: a planned
//! birth of the accepted Build counts only when its own digest arrives after the round began, as a
//! ready birth. Only when no planned birth is missing does the primary republish the adopted status
//! (the status keeps its old `changed_at_rafka_ms`: the state did not change) and, for a mesh
//! primary, declare the Mesh ready to the fabric-primary. The fabric-primary clears `degraded`
//! only on a fresh report from the CURRENT mesh primary's birth, never on a topology reading.
//!
//! The down op is [`ProbeNodeState`](rafka_node_rpc_contract::status::StatusRequest::ProbeNodeState):
//! the member re-publishes its presence and answers its state. A failed send is named in its span
//! and answered by nobody: no member leaves the checklist by a timer, and a new primary re-sends.

use crate::accepted::FabricTopology;
use crate::admin::{PeerRecovery, Records};
use crate::model::{IncarnationId, NodeKind, PathName};
use crate::status_rpc::MeshReport;
use crate::topology::Topology;
use rafka_mesh_entity::{MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::{Announced, DigestBook};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::status::{Status, StatusRequest};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

/// A planned birth that has not checked in this round, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Missing {
    pub who: String,
    pub why: &'static str,
}

impl std::fmt::Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.who, self.why)
    }
}

fn named(missing: &[Missing]) -> String {
    missing.iter().map(Missing::to_string).collect::<Vec<_>>().join(",")
}

/// The planned births of the accepted Build for `mesh`: every path it names.
pub fn planned(topology: &FabricTopology, mesh: &str) -> Vec<PathName> {
    topology.meshes.get(mesh).map(|m| m.nodes.iter().cloned().collect()).unwrap_or_default()
}

/// The planned births that have not checked in. `me` is the primary running the round, ready or not
/// by its own status. `heard` are the digests received from their members since the round began;
/// `known` every digest held (to tell a birth never heard from one heard before the round).
pub fn missing_members(planned: &[PathName], me: &PathName, me_ready: bool, heard: &[MeshDigest], known: &[MeshDigest], departed: &dyn Fn(&str) -> bool) -> Vec<Missing> {
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
            "not-heard-since-round"
        };
        out.push(Missing { who, why });
    }
    out
}

/// The meshes whose primary has not reported to the fabric-primary running the round. `meshes` is
/// each mesh with its current primary node-admin's birth (`None`: no primary), and whether that
/// primary is `me`; `my_mesh_done` says whether `me`'s own mesh round is finished. A report counts
/// only from the exact birth that is the mesh's primary now.
pub fn fabric_missing(meshes: &[(String, Option<IncarnationId>, bool)], my_mesh_done: bool, reports: &BTreeMap<String, MeshReport>) -> Vec<Missing> {
    let mut out = Vec::new();
    for (mesh, primary, is_me) in meshes {
        let who = mesh.clone();
        let Some(primary) = primary else {
            out.push(Missing { who, why: "no-primary" });
            continue;
        };
        if *is_me {
            if !my_mesh_done {
                out.push(Missing { who, why: "own-mesh-round-open" });
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

/// Whether the mesh a fabric-primary decided to rebirth is back: its current primary is a birth the
/// decision did not find, it reported to this fabric-primary, and it reported after the decision.
/// A ready primary of a newer birth that has not reported is not this.
pub fn recovered(recovery: &PeerRecovery, primary: Option<&IncarnationId>, report: Option<&MeshReport>) -> bool {
    let (Some(primary), Some(report)) = (primary, report) else { return false };
    !recovery.lost.contains(primary) && &report.incarnation == primary && report.at >= recovery.opened_at
}

/// What the driver needs of its admin each hierarchy round.
pub struct Inputs<'a> {
    pub view: &'a Topology,
    /// The accepted Build's planned births of this admin's mesh; `None`: the Build was unreadable.
    pub planned: Option<Vec<PathName>>,
    pub book: &'a DigestBook,
    pub client: Option<&'a Arc<NodeRpcClient>>,
    pub records: &'a Records,
}

/// Which rounds are complete now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Complete {
    pub mesh: bool,
    pub fabric: bool,
}

struct Round {
    began: Instant,
    held: Option<String>,
}

/// The rounds of one node-admin: its mesh's while it adopted its mesh's status, the fabric's while
/// it adopted the fabric's. A round exists exactly while its status is adopted and unpublished.
pub struct RoundDriver {
    me: PathName,
    mesh: Option<Round>,
    fabric: Option<Round>,
    /// The mesh primaries this admin, as the fabric-primary, has sent the down op to, by mesh: the
    /// view's primary changing from this is what sends it again (R-S2).
    addressed: BTreeMap<String, (crate::model::NodeId, Option<IncarnationId>)>,
}

impl RoundDriver {
    pub fn new(me: PathName) -> Self {
        Self { me, mesh: None, fabric: None, addressed: BTreeMap::new() }
    }

    /// The fabric-primary's view of a mesh's primary changed (a mesh primary appeared, moved or
    /// was reborn): the down op goes to it, so it re-sends what it owes this admin. Not the
    /// takeover alone: a primary this admin has not addressed in this view is addressed now. A
    /// view where this admin is not the fabric-primary addresses nobody and forgets who it did.
    pub fn address_changed_mesh_primaries(&mut self, view: &crate::topology::Topology, is_fabric_primary: bool, client: Option<&Arc<NodeRpcClient>>) {
        if !is_fabric_primary {
            self.addressed.clear();
            return;
        }
        let mut targets = Vec::new();
        let mut present = Vec::new();
        for m in &view.meshes {
            let Some(p) = view.cohort_primary(&m.name, NodeKind::NodeAdmin).filter(|p| p.name != self.me) else { continue };
            present.push(m.name.clone());
            let now = (p.node_id.clone(), p.incarnation_id.clone());
            if self.addressed.get(&m.name) != Some(&now) {
                self.addressed.insert(m.name.clone(), now);
                targets.push((m.name.clone(), Some(p.clone())));
            }
        }
        self.addressed.retain(|mesh, _| present.contains(mesh));
        if !targets.is_empty() {
            send_down(&self.me, "fabric", &view.fabric.name, targets, client);
        }
    }

    /// No status is adopted and waiting: no round is open.
    pub fn end(&mut self) {
        self.mesh = None;
        self.fabric = None;
    }

    /// One step. `awaiting` says which adopted statuses still wait for their round; the answer says
    /// which rounds are complete now (so the publisher may republish).
    pub async fn step(&mut self, awaiting: Announced, i: Inputs<'_>) -> Complete {
        let mesh_name = self.me.mesh.clone();
        let mut out = Complete::default();
        if !awaiting.mesh_awaiting_round {
            self.mesh = None;
        } else {
            let first = self.mesh.is_none();
            let round = self.mesh.get_or_insert_with(|| Round { began: Instant::now(), held: None });
            if first {
                let targets: Vec<(String, Option<crate::model::Node>)> = i.planned.iter().flatten().filter(|p| **p != self.me).map(|p| (p.to_string(), i.view.nodes.iter().find(|n| &n.name == p).cloned())).collect();
                send_down(&self.me, "mesh", &mesh_name, targets, i.client);
            }
            let missing = match &i.planned {
                None => vec![Missing { who: mesh_name.clone(), why: "accepted-build-unreadable" }],
                Some(planned) => {
                    let me_ready = i.view.nodes.iter().any(|n| n.name == self.me && n.status == crate::model::NodeStatus::ReadyForTraffic);
                    let (heard, known) = (i.book.heard_direct_since(round.began), i.book.all());
                    missing_members(planned, &self.me, me_ready, &heard, &known, &|id| i.book.is_departed(id))
                }
            };
            if missing.is_empty() {
                let waited_ms = round.began.elapsed().as_millis() as u64;
                tracing::info_span!("rdm.node_admin.mesh.update.via-round-complete", node = %self.me, mesh = %mesh_name, members = i.planned.as_ref().map(Vec::len).unwrap_or(0), waited_ms)
                    .in_scope(|| tracing::info!("every planned birth checked in: the adopted status is republished"));
                out.mesh = true;
            } else {
                let now = named(&missing);
                if round.held.as_deref() != Some(now.as_str()) {
                    tracing::info_span!("rdm.node_admin.mesh.update.via-round-held", node = %self.me, mesh = %mesh_name, missing = %now, waited_ms = round.began.elapsed().as_millis() as u64)
                        .in_scope(|| tracing::info!("the round is held: a planned birth has not checked in"));
                    round.held = Some(now);
                }
            }
        }
        if !awaiting.fabric_awaiting_round {
            self.fabric = None;
        } else {
            let first = self.fabric.is_none();
            let round = self.fabric.get_or_insert_with(|| Round { began: Instant::now(), held: None });
            let meshes: Vec<(String, Option<IncarnationId>, bool)> = i
                .view
                .meshes
                .iter()
                .map(|m| {
                    let p = i.view.cohort_primary(&m.name, NodeKind::NodeAdmin);
                    (m.name.clone(), p.and_then(|p| p.incarnation_id.clone()), p.is_some_and(|p| p.name == self.me))
                })
                .collect();
            if first {
                let targets: Vec<(String, Option<crate::model::Node>)> = i.view.meshes.iter().filter(|m| m.name != mesh_name).map(|m| (m.name.clone(), i.view.cohort_primary(&m.name, NodeKind::NodeAdmin).cloned())).collect();
                for (mesh, p) in &targets {
                    if let Some(p) = p {
                        self.addressed.insert(mesh.clone(), (p.node_id.clone(), p.incarnation_id.clone()));
                    }
                }
                send_down(&self.me, "fabric", &i.view.fabric.name, targets, i.client);
            }
            let my_mesh_done = !awaiting.mesh_awaiting_round || out.mesh;
            let reports = i.records.declared.lock().unwrap().reports.clone();
            let missing = fabric_missing(&meshes, my_mesh_done, &reports);
            if missing.is_empty() {
                let waited_ms = round.began.elapsed().as_millis() as u64;
                let cleared = i.records.set_adopted_degraded(false);
                tracing::info_span!("rdm.node_admin.fabric.update.via-round-complete", node = %self.me, meshes = meshes.len(), waited_ms, degraded_cleared = cleared)
                    .in_scope(|| tracing::info!("every mesh primary reported for this round"));
                out.fabric = true;
            } else {
                let now = named(&missing);
                if round.held.as_deref() != Some(now.as_str()) {
                    tracing::info_span!("rdm.node_admin.fabric.update.via-round-held", node = %self.me, missing = %now, waited_ms = round.began.elapsed().as_millis() as u64)
                        .in_scope(|| tracing::info!("the fabric round is held: a mesh primary has not reported"));
                    round.held = Some(now);
                }
            }
        }
        out
    }
}

/// The round's down op to each target, once, on its own task: the answer is named in a span and
/// nothing is retried. A target the view holds no birth of is named too.
fn send_down(me: &PathName, scope: &'static str, of: &str, targets: Vec<(String, Option<crate::model::Node>)>, client: Option<&Arc<NodeRpcClient>>) {
    for (who, node) in targets {
        let (me, of, client) = (me.clone(), of.to_string(), client.cloned());
        tokio::spawn(async move {
            let outcome = match (&node, &client) {
                (None, _) => "no-birth-in-view".to_string(),
                (Some(_), None) => "no-node-rpc-client".to_string(),
                (Some(n), Some(client)) => match n.incarnation_id.clone() {
                    None => "birth-has-no-incarnation".to_string(),
                    Some(incarnation) => {
                        let req = StatusRequest::ProbeNodeState { node_id: n.node_id.clone(), incarnation };
                        let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &CallOptions::default()).await;
                        out.reply().map(|r| r.value().name().to_string()).unwrap_or_else(|| out.name().to_string())
                    }
                },
            };
            tracing::info_span!("rdm.node_admin.mesh.update.via-round-down", node = %me, mesh = %of, scope, target = %who, outcome = %outcome)
                .in_scope(|| tracing::info!("the round's down op re-sent"));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{FabricId, MeshNode};

    fn path(mesh: &str, kind: NodeKind, ord: u32) -> PathName {
        PathName::new(mesh, kind, ord)
    }

    fn digest(p: &PathName, status: MemberStatus) -> MeshDigest {
        MeshDigest {
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
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
        }
    }

    // @feature: node-lifecycle
    #[test]
    fn checklist_names_every_planned_birth_that_has_not_checked_in_this_round() {
        let (a1, r1, r2, r3) = (path("mesh1", NodeKind::NodeAdmin, 1), path("mesh1", NodeKind::RpcNode, 1), path("mesh1", NodeKind::RpcNode, 2), path("mesh1", NodeKind::RpcNode, 3));
        let planned = vec![a1.clone(), r1.clone(), r2.clone(), r3.clone()];
        let ready = digest(&r1, MemberStatus::ReadyForTraffic);
        let pending = digest(&r2, MemberStatus::Pending);
        let before_round = digest(&r3, MemberStatus::ReadyForTraffic);
        let none = |_: &str| false;
        // r1 checked in ready; r2 checked in but is not ready; r3 was heard only before the round began.
        let m = missing_members(&planned, &a1, true, &[ready.clone(), pending.clone()], &[ready.clone(), pending.clone(), before_round.clone()], &none);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh1.rpc.2(not-ready)", "mesh1.rpc.3(not-heard-since-round)"]);
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
    fn fabric_round_waits_for_a_report_from_each_mesh_primarys_current_birth() {
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
        // The holder's own mesh counts only once its own round is finished.
        let m = fabric_missing(&meshes[..2], false, &reports);
        assert_eq!(m.iter().map(Missing::to_string).collect::<Vec<_>>(), vec!["mesh1(own-mesh-round-open)"]);
    }

    // @feature: node-lifecycle
    #[test]
    fn degraded_clears_only_on_the_current_primarys_report_after_the_decision() {
        let (lost, reborn) = (IncarnationId::mint(), IncarnationId::mint());
        let opened_at = Instant::now();
        let recovery = PeerRecovery { mesh: "mesh2".into(), verdict_rafka_ms: 1, opened_at, lost: vec![lost.clone()] };
        let later = opened_at + std::time::Duration::from_millis(5);
        // A ready primary of a newer birth that has not reported does not clear it.
        assert!(!recovered(&recovery, Some(&reborn), None));
        // The birth the decision found never does, reported or not.
        assert!(!recovered(&recovery, Some(&lost), Some(&report(&lost, later))));
        // A report from a birth that is not the primary now is not the primary's.
        assert!(!recovered(&recovery, Some(&reborn), Some(&report(&IncarnationId::mint(), later))));
        // A report received before the decision is not fresh.
        assert!(!recovered(&recovery, Some(&reborn), Some(&report(&reborn, opened_at - std::time::Duration::from_millis(1)))));
        assert!(!recovered(&recovery, None, Some(&report(&reborn, later))));
        assert!(recovered(&recovery, Some(&reborn), Some(&report(&reborn, later))));
    }

    use crate::model::{Fabric, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};

    fn member(name: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::ReadyForTraffic;
        n.incarnation_id = Some(IncarnationId::mint());
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Degraded, provider: ProviderKind::Process },
            meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
            nodes,
        }
    }

    const NONE: Announced = Announced { mesh_awaiting_round: false, fabric_awaiting_round: false };

    // @feature: node-lifecycle
    #[tokio::test]
    async fn successor_fabric_holder_keeps_degraded_until_every_mesh_primary_reports_for_its_round() {
        // The seat moved to mesh2's primary while mesh1 was being reborn; the new holder adopted degraded.
        let (me, reborn) = (member("mesh2.admin.1", true, true), member("mesh1.admin.2", true, false));
        let v = view(vec![me.clone(), reborn.clone()]);
        let records = Records::default();
        records.set_adopted_degraded(true);
        let book = DigestBook::default();
        let mut driver = RoundDriver::new(me.name.clone());
        let awaiting = Announced { mesh_awaiting_round: false, fabric_awaiting_round: true };
        let inputs = || Inputs { view: &v, planned: None, book: &book, client: None, records: &records };
        // A ready primary of a newer birth is in the view; nobody has reported to this holder.
        let done = driver.step(awaiting, inputs()).await;
        assert!(!done.fabric && records.adopted_degraded(), "the topology reading alone does not clear degraded");
        // A report from a birth that is not the primary now does not either.
        records.declared.lock().unwrap().reports.insert("mesh1".into(), MeshReport { incarnation: IncarnationId::mint(), at: Instant::now() });
        let done = driver.step(awaiting, inputs()).await;
        assert!(!done.fabric && records.adopted_degraded(), "a report of another birth is not the current primary's");
        // The current primary re-declares to the new holder: the round is complete and degraded clears.
        records.declared.lock().unwrap().reports.insert("mesh1".into(), MeshReport { incarnation: reborn.incarnation_id.clone().unwrap(), at: Instant::now() });
        let done = driver.step(awaiting, inputs()).await;
        assert!(done.fabric && !records.adopted_degraded(), "every mesh primary reported for this round");
    }

    // @feature: node-lifecycle
    #[tokio::test]
    async fn elected_mesh_primary_completes_its_round_only_when_every_planned_birth_checks_in_after_it_began() {
        let (me, rpc) = (member("mesh1.admin.1", true, false), member("mesh1.rpc.1", false, false));
        let v = view(vec![me.clone(), rpc.clone()]);
        let records = Records::default();
        let book = DigestBook::default();
        let mut driver = RoundDriver::new(me.name.clone());
        let awaiting = Announced { mesh_awaiting_round: true, fabric_awaiting_round: false };
        let planned = vec![me.name.clone(), rpc.name.clone()];
        let inputs = || Inputs { view: &v, planned: Some(planned.clone()), book: &book, client: None, records: &records };
        // The member was heard before the round began (it is held, but it has not checked in).
        let before = digest(&rpc.name, MemberStatus::ReadyForTraffic);
        assert!(book.record(before.clone()));
        let done = driver.step(awaiting, inputs()).await;
        assert!(!done.mesh, "a digest heard before the round is not a check-in");
        // Its next heartbeat arrives after the round began: it has checked in.
        let mut again = before;
        again.digest_seq += 1;
        assert!(book.record(again));
        let done = driver.step(awaiting, inputs()).await;
        assert!(done.mesh, "every planned birth checked in after the round began");
        // An unreadable accepted Build holds the round by name.
        let mut unreadable = RoundDriver::new(me.name.clone());
        let done = unreadable.step(awaiting, Inputs { view: &v, planned: None, book: &book, client: None, records: &records }).await;
        assert!(!done.mesh);
    }
}
