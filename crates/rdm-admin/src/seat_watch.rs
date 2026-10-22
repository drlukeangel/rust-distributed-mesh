//! A silent seat holder is looked at, never replaced for being silent (ruling R-A2).
//!
//! The fabric primary stays in its mesh until that mesh is proven unable to hold it, and silence
//! proves nothing. Each admin uses its own view's silence evidence to investigate: an admin of
//! the holder's mesh (a standby included), and any admin looking at the fabric holder's mesh
//! from outside it, looks at the exact silent birth: the one tickle ladder (node-tickle.md: one
//! probe direct, then the same probe through up to two live members of its mesh at once), and on
//! `unreached` the provider's inspection of the birth's exact runtime (node-recovery.md step 2,
//! ruling R-SW1). An answer from the exact birth (`Current`, `CurrentParked`, `Blocked`), or a
//! runtime that still runs, ends it: the seat stays. Only a runtime found exited is a verified loss,
//! its exit-proof row put (`Records::put_exit`); `unreached`, `NotSent`, a deadline, silence and the
//! observer-inferred `Dead` are not.
//!
//! [`plan`] decides what a pass owes, from the view and births already inspected; [`run`] drives it.

use crate::admin::Records;
use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::SeatHolder;
use rdm_transport::membership::Membership;
use rdm_transport::NodeRpcClient;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::Instrument as _;

type Birth = (NodeId, IncarnationId);

/// What one pass over the view owes.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Exact silent births to investigate from this admin's own view.
    pub investigate: Vec<Node>,
}

/// Births already investigated during their current silence mark. Hearing the birth again
/// removes the mark; a later silence can then be investigated from the view again.
#[derive(Debug, Default)]
pub struct Said {
    investigated: HashSet<Birth>,
    /// Each node-admin birth of the fabric-primary's mesh this admin found unreached and
    /// uninspectable (`Unproven`), from the first such look: the hold of elections §5's takeover. Any
    /// other finding ends it.
    unreachable: std::collections::HashMap<Birth, std::time::Instant>,
}

fn birth(n: &Node) -> Option<Birth> {
    n.incarnation_id.clone().map(|i| (n.node_id.clone(), i))
}

/// The hand-over of committed Build facts a pass owes when the fabric seat record this birth held names another
/// birth now, the move planned or not (the handover door's commit, or the record heard on the
/// backbone first): why, and the admin the seat moved to, by the node id the record names (a
/// member of the view whether or not its seat flags have moved yet; the handover door names it the
/// same way). `held` carries whether this birth held the record on an earlier pass.
pub fn seat_yield(record: Option<&SeatHolder>, me: &Birth, held: &mut bool, view: &Topology) -> Option<(String, Option<String>)> {
    match record {
        Some(h) if h.is_birth(&me.0, &me.1) => {
            *held = true;
            None
        }
        Some(h) if *held => {
            *held = false;
            Some((format!("the seat is now {} {}", h.mesh, h.node_id), view.own(&h.node_id).map(|n| n.name.to_string())))
        }
        _ => None,
    }
}

/// What the committed fabric seat record says of `me`'s Build authority (the approved proposal
/// node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 5: "The writer writes the one seat
/// row at the next epoch(s) naming the successor. This is the commit. From this write the holder is no
/// longer primary"; nothing else is written). A record naming this birth, naming none, or naming
/// a birth proven gone (`lost`: a vacancy the election fills before this birth's own record lands)
/// ends nothing; a record naming another live birth ended this birth's authority at its epoch, and
/// names that birth's path.name from `name_of` (the digest book, which holds a birth before the
/// projected view does). An open record naming this birth names its path.name as the holder; one
/// naming none or a birth proven gone names no holder. Read from the record alone: no history, no
/// write.
pub fn fabric_seat_record(record: Option<&SeatHolder>, me: &Birth, lost: &HashSet<IncarnationId>, name_of: impl Fn(&NodeId) -> Option<String>) -> crate::fabric_builds::SeatRecord {
    seat_record("the fabric seat record", record, me, lost, name_of)
}

/// [`fabric_seat_record`] of `mesh`'s primary seat record: what it says of `me`'s hold on its
/// own mesh's drift attempts (R-BI7), judged by the same rule.
pub fn mesh_seat_record(mesh: &str, record: Option<&SeatHolder>, me: &Birth, lost: &HashSet<IncarnationId>, name_of: impl Fn(&NodeId) -> Option<String>) -> crate::fabric_builds::SeatRecord {
    seat_record(&format!("mesh {mesh}'s primary seat record"), record, me, lost, name_of)
}


fn seat_record(which: &str, record: Option<&SeatHolder>, me: &Birth, lost: &HashSet<IncarnationId>, name_of: impl Fn(&NodeId) -> Option<String>) -> crate::fabric_builds::SeatRecord {
    match record {
        Some(h) if !h.is_birth(&me.0, &me.1) && !lost.contains(&h.incarnation) => {
            let to = name_of(&h.node_id);
            let by = format!("{which} names {} {} (birth {}) at revision {}{}", h.mesh, h.node_id, h.incarnation.as_str(), h.revision, if to.is_none() { "; no digest of that birth is held, so its path.name is not known here" } else { "" });
            crate::fabric_builds::SeatRecord::Moved { by, to }
        }
        Some(h) if h.is_birth(&me.0, &me.1) => crate::fabric_builds::SeatRecord::Open { holder: name_of(&h.node_id) },
        _ => crate::fabric_builds::SeatRecord::Open { holder: None },
    }
}

/// The pass uses only this admin's exact birth, its view and the silent births already inspected.
pub fn plan(view: &Topology, me: &PathName, me_birth: &Birth, said: &mut Said) -> Plan {
    let silent: HashSet<Birth> = view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::PendingReconnect).filter_map(birth).collect();
    said.investigated.retain(|b| silent.contains(b));
    let mut plan = Plan::default();
    let mesh = &me.mesh;

    // What this admin sees itself: a silent seat holder of its own mesh, any silent node-admin
    // of the mesh that holds the fabric seat (a standby, or a candidate looking from outside),
    // and, when this admin holds the fabric seat, any silent primary node-admin of a peer mesh
    // (R-FM1: the fabric-primary runs the hung-runtime hold and recovery for a hung mesh-primary).
    let fabric_mesh = view.members().find(|n| n.is_fabric_primary).map(|n| n.mesh.clone());
    let holds_fabric = view.holds_fabric_seat(&me_birth.0);
    for n in view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::PendingReconnect && n.node_id != me_birth.0) {
        let own_holder = &n.mesh == mesh && (n.is_primary || n.is_fabric_primary);
        let incumbent = fabric_mesh.as_ref() == Some(&n.mesh);
        let peer_primary = holds_fabric && n.is_primary;
        if !(own_holder || incumbent || peer_primary) {
            continue;
        }
        let Some(b) = birth(n) else { continue };
        if said.investigated.insert(b) {
            plan.investigate.push(n.clone());
        }
    }
    plan
}

/// What the look found: node investigation's one verdict (rulings INDEX Q107 item 1).
pub use crate::node_investigation::Finding;

/// What the look needs of this admin.
pub struct Looker {
    /// This admin's path.name.
    pub me: PathName,
    /// The node id this admin was born with: it is never its own carrier.
    pub me_id: NodeId,
    /// Its mesh membership.
    pub membership: Membership,
    /// Its Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// Its deployment provider.
    pub provider: Arc<dyn crate::deployment::provider::DeploymentProvider>,
    /// Its nodes.storage: the runtime row of each birth recorded at launch or handed at its create.
    pub nodes: Option<Arc<dyn crate::storage::NodesStorage>>,
    /// A seat holder heard again is commanded back through this (Q87), once the admin holds it.
    pub heard_again: crate::heard_again::HeardAgainSlot,
    /// Its connections store: the by-destination index names the leads of a look (Q107 item 1).
    pub connections: Option<Arc<dyn crate::storage::ConnectionsStorage>>,
}

impl Looker {
    /// Look at `n`'s exact birth: node investigation (rulings INDEX Q107 item 1), the leads, the
    /// tickle ladder and, on `unreached`, the provider's inspection (node-tickle.md "What follows";
    /// node-recovery.md step 2, run by the seat watch: fabric-node-lifecycle-elections.md §4.1).
    pub async fn look(&self, view: &Topology, n: &Node) -> Finding {
        let calls = crate::offline::TickleClient { client: self.client.clone(), me_id: self.me_id.clone(), me: self.me.to_string(), connections: self.connections.clone() };
        let finding = crate::node_investigation::investigate(n, view, &calls, self.inspect(n)).await;
        if let Finding::Reached { answer: rdm_core::status::StatusReply::Current { .. }, .. } = &finding {
            // A seat holder this admin holds off its mesh that answers Current is commanded back
            // through node.mesh.join (node-tickle.md "What follows", Q87) by the fabric-primary, the
            // authority above a mesh-primary: every admin looks, one commands (commands go down).
            let commands = view.own(&self.me_id).is_some_and(|me| me.is_fabric_primary);
            if let (true, Some(heard_again), Some(incarnation)) = (commands, self.heard_again.get(), n.incarnation_id.as_ref()) {
                // Held off its mesh in this watch's eyes (it was looked at because it was silent): the
                // view's ready-for-traffic is its last word before the silence, not where it stands now.
                let held = Node { status: NodeStatus::PendingReconnect, ..n.clone() };
                heard_again.command(&held, incarnation).await;
            }
        }
        finding
    }

    /// The provider's inspection of `n`'s exact runtime.
    async fn inspect(&self, n: &Node) -> Finding {
        inspect_birth(&*self.provider, &self.membership.book, self.nodes.as_deref(), n).await
    }
}

/// The provider's inspection of the exact runtime of `n`'s birth: node.runtime.inspect over the one
/// runtime resolution (node_investigation.rs).
pub use crate::node_investigation::inspect_birth;

pub use crate::offline::birth_answered;

/// What the watch drives.
pub struct Watch {
    /// The admin's stop gate: this watch is held at its own wait while the admin is stopped, never
    /// where it reads the view.
    pub gate: crate::stop_gate::StopGate,
    /// This admin's path.name.
    pub me: PathName,
    /// This admin's NodeId.
    pub me_id: NodeId,
    /// This admin's exact birth.
    pub incarnation: IncarnationId,
    /// Its mesh membership.
    pub membership: Membership,
    /// Its view of the topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// Its records of births and exits.
    pub records: Arc<Records>,
    /// What it looks at a birth with.
    pub looker: Looker,
    /// This admin's Build log: its committed facts are handed on when the fabric seat's record names another birth.
    pub builds: Arc<crate::fabric_builds::FabricBuildStateAdapter>,
    /// The recovery submitter, used when the fabric-primary's hold on a hung peer-mesh primary expires (R-FM1).
    pub recovery: Option<crate::admin::RecoverySubmitter>,
    /// The node.primary.update door, set once it is built: a mesh-primary proposes the takeover of a
    /// fabric seat whose holder's mesh is unreached and uninspectable past the hold through it.
    pub handover: crate::fabric_handover::HandoverSlot,
}

/// Run the watch on seat changes and the gossip cadence, from the admin's own silence evidence.
/// Nothing here publishes an advisory or acts on receipt of one.
pub async fn run(w: Watch) {
    let mut said = Said::default();
    let me_birth = (w.me_id.clone(), w.incarnation.clone());
    let mut seats_changed = w.membership.seats().subscribe();
    let mut held_the_fabric_seat = false;
    // A turn runs from the gate to the next idle wait: the pass below, then the yield check at the
    // top of the loop, which can hand facts on. It is held in an Option so it ends (`take`) right
    // before the idle wait and begins again right after it.
    let mut turn = Some(w.gate.turn("seat-watch").await);
    loop {
        // A fabric seat this birth held, now recorded to another birth: the record already fences
        // its Build log; the committed facts are handed to the neighbours before the new holder acts.
        let record = w.membership.seats().fabric();
        let owed = seat_yield(record.as_ref(), &me_birth, &mut held_the_fabric_seat, &*w.topology.read().await);
        if let Some((by, to)) = owed {
            w.builds.hand_over_facts(&by, to).await;
        }
        drop(turn.take());
        tokio::select! {
            _ = seats_changed.changed() => {}
            _ = tokio::time::sleep(rdm_transport::membership::gossip_interval()) => {}
        }
        // Held here while the admin is parked: its own wait, outside every lock.
        turn = Some(w.gate.turn("seat-watch").await);
        let view = w.topology.read().await.clone();
        // The mesh the fabric seat's record names: the takeover's hold is kept on its node-admins.
        let fabric_mesh = w.membership.seats().fabric().map(|h| h.mesh.clone());
        // Any node-admin in the view whose digest has returned (`ReadyForTraffic`) ends its hold (R-FM1).
        let commands = view.own(&w.me_id).is_some_and(|me| me.is_fabric_primary);
        for n in view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::ReadyForTraffic) {
            w.records.clear_held_running(n).await;
            if let Some(b) = birth(n) {
                // A seat holder this watch said unreachable, heard again by its digest, is commanded
                // back through node.mesh.join by the fabric-primary (Q87), on its own task.
                if said.unreachable.remove(&b).is_some() && commands {
                    if let (Some(heard_again), Some(incarnation)) = (w.looker.heard_again.get().cloned(), n.incarnation_id.clone()) {
                        // The holder this watch said unreachable: held off ready-for-traffic in its eyes
                        // until this digest, as `watch_marked` holds a marked birth.
                        let n = Node { status: NodeStatus::PendingReconnect, ..n.clone() };
                        tokio::spawn(async move { heard_again.command(&n, &incarnation).await; });
                    }
                }
            }
        }
        let me = w.membership.book.held_path_name().unwrap_or_else(|| view.held_name_of(&w.me, Some(&w.me_id)));
        let plan = plan(&view, &me, &me_birth, &mut said);
        for node in plan.investigate {
            let span = tracing::info_span!(
                "rdm.node_admin.seat.update.via-investigation",
                node = %me,
                suspect = %node.name,
                suspect_node_id = %node.node_id,
                suspect_incarnation = node.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or(""),
                trigger = "silence",
                holds_fabric_seat = node.is_fabric_primary,
                finding = tracing::field::Empty,
                detail = tracing::field::Empty,
                iroh_known_addrs = tracing::field::Empty,
                iroh_active_addrs = tracing::field::Empty,
                exit_proof_row = tracing::field::Empty,
            );
            // One read of iroh's local view of the suspect, recorded beside the investigation;
            // the finding is the provider's and the Node RPC's, never this.
            let key = node.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
            let seen = rdm_transport::iroh_obs::observe_remote(&w.membership.endpoint(), key).await;
            span.record("iroh_known_addrs", seen.known_addrs.as_str());
            span.record("iroh_active_addrs", seen.active_addrs.as_str());
            let finding = w.looker.look(&view, &node).instrument(span.clone()).await;
            span.record("finding", finding.name());
            span.record("detail", finding.detail().as_str());
            // The takeover's hold (elections §5): a node-admin of the fabric-primary's mesh unreached
            // and uninspectable is held from its first such look; any other finding ends the hold.
            if let Some(b) = birth(&node) {
                if matches!(finding, Finding::Unproven { .. }) && fabric_mesh.as_deref() == Some(node.mesh.as_str()) {
                    said.unreachable.entry(b).or_insert_with(std::time::Instant::now);
                } else {
                    said.unreachable.remove(&b);
                }
            }
            match &finding {
                Finding::Reached { .. } => {
                    w.records.clear_held_running(&node).await;
                }
                Finding::Running => {
                    // R-FM1: a hung fabric-primary is marked held-running by peer mesh-primaries and
                    // same-mesh standbys and is never terminated; a hung mesh-primary is held-running
                    // by the fabric-primary, which submits its recovery Build once the hold expires.
                    if node.is_fabric_primary || view.holds_fabric_seat(&me_birth.0) {
                        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                        // The fabric-primary's hold of a hung mesh-primary submits its terminate, gated by the
                        // birth's failed terminates (node-recovery.md step 4's end); a hung fabric-primary's
                        // admins only record.
                        let submitter = w.recovery.as_ref().filter(|_| !node.is_fabric_primary && view.holds_fabric_seat(&me_birth.0));
                        let gate = match submitter {
                            Some(recovery) => {
                                let since = w.records.held_running_row_of(&node).await.ok().flatten().map(|r| r.since);
                                let gate = recovery.hold_gate_of(&node, since).await;
                                if let crate::admin::HoldGate::Void(f) = &gate {
                                    w.records.clear_held_running(&node).await;
                                    recovery.span_hold_voided(&node, f);
                                }
                                Some((recovery, gate))
                            }
                            None => None,
                        };
                        if let Some(held) = w.records.record_held_running(&node, now_ms, &me.to_string()).await {
                            if let Some((recovery, gate)) = gate {
                                if held.verdicts > 1 && now_ms.saturating_sub(held.since) >= crate::admin::hung_hold_ms() {
                                    recovery.submit_hold_terminate(&node, gate).await;
                                }
                            }
                        }
                    }
                }
                Finding::Exited { .. } => {
                    w.records.clear_held_running(&node).await;
                }
                // A seat holder the operating system holds suspended is neither hung nor gone: a hold
                // this admin opened while it read Running is cancelled (deployment-provider.md §2.5).
                Finding::OsStopped => {
                    w.records.clear_held_running(&node).await;
                }
                Finding::Unproven { .. } => {}
            }
            let proven = if let Finding::Exited { code, proof } = &finding {
                // The verified loss is recorded durably: the holder's exit-proof row is what every
                // projection reads as its proven loss, its proof the inspect's.
                let recorded = w.records.put_exit(&node, *code, proof).await;
                span.record("exit_proof_row", recorded.as_ref().map(|_| "put".to_string()).unwrap_or_else(|e| e.clone()).as_str());
                recorded.is_ok()
            } else {
                false
            };
            if !proven {
                if let Some(b) = birth(&node) {
                    said.investigated.remove(&b);
                }
            }
            span.in_scope(|| tracing::info!("a silent seat holder was looked at"));
        }
        // A held takeover ends once the seat's record moved or its lost birth is proven gone.
        if let Some(door) = w.handover.get() {
            door.release_held_takeover();
        }
        // The fabric-primary unheard past the staleness floor is superseded (Q89, elections §4.3): its own
        // mesh's standby proposes at the first tick after the mark; outside its mesh, an admin proposes once
        // the whole mesh is held past the hold (the lost-mesh arm). An admin whose view is cut off or
        // healing acts on nothing (gossip.md §6).
        // ... and only once this admin has held the record's holder as the holder for a floor: a record
        // learned a moment ago (after a heal) names a holder whose silence in this book predates it.
        let held_a_floor = w.membership.seats().fabric_held_for() >= w.membership.book.staleness_floor();
        if let Some(lost) = takeover_due(&view, &me_birth, w.membership.seats().fabric()).filter(|_| w.membership.authorizes() && held_a_floor) {
            if let Some(door) = w.handover.get().cloned() {
                drop(turn.take());
                let _ = door.propose_take_over(&lost).await;
                turn = Some(w.gate.turn("seat-watch").await);
            }
        }
    }
}

/// The fabric seat's record `record` when this admin, a ReadyForTraffic node-admin of the record's own
/// mesh and not its holder, holds the record's birth off ready-for-traffic (`PendingReconnect` or
/// `Dead`) in its view: the fabric-primary unheard past the staleness floor is superseded by its own
/// mesh's standby at the first tick after the mark, whatever the provider says of its runtime (Q89;
/// fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 2, "Its own
/// mesh"; elections §4.3). `None` otherwise: outside the record's mesh the proposal is mesh
/// investigation's, at its decision step (`investigate::run`), and the holder never proposes its own.
pub fn takeover_due(view: &Topology, me: &Birth, record: Option<rdm_core::SeatHolder>) -> Option<rdm_core::SeatHolder> {
    let record = record?;
    if record.is_birth(&me.0, &me.1) {
        return None;
    }
    let mine = view.members().find(|n| n.node_id == me.0)?;
    if mine.kind != NodeKind::NodeAdmin || mine.status != NodeStatus::ReadyForTraffic {
        return None;
    }
    // Its own mesh (Q89): the fabric-primary held off ready-for-traffic in this admin's view, at the mark,
    // whatever the provider says of its runtime; the standby that the leadership rule names writes the row.
    if mine.mesh == record.mesh {
        let held = view.members().any(|n| n.node_id == record.node_id && n.incarnation_id.as_ref() == Some(&record.incarnation) && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead));
        return held.then_some(record);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin(path: &str, id: &str, inc: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = NodeId::parse(id).unwrap();
        n.incarnation_id = Some(IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap());
        n.status = status;
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        use crate::model::{Fabric, FabricId, ProviderKind, ScopeStatus};
        Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process }, meshes: Vec::new(), nodes }
    }

    fn me_of(n: &Node) -> Birth {
        (n.node_id.clone(), n.incarnation_id.clone().unwrap())
    }
    fn world() -> (Topology, Node, Node, Node) {
        let mut holder = admin("mesh1.admin.1", "300000000000", "h1", NodeStatus::PendingReconnect);
        holder.is_primary = true;
        holder.is_fabric_primary = true;
        let standby = admin("mesh1.admin.2", "400000000000", "s1", NodeStatus::ReadyForTraffic);
        let mut outsider = admin("mesh2.admin.1", "100000000000", "o1", NodeStatus::ReadyForTraffic);
        outsider.is_primary = true;
        (view(vec![holder.clone(), standby.clone(), outsider.clone()]), holder, standby, outsider)
    }

    /// CONTRACT: a birth that held the fabric seat record and finds it naming another birth owes one
    /// hand-over of its committed Build facts, naming that birth's path.name (resolved by the node id
    /// the record names), whether the record moved by the handover door's commit or was heard on the
    /// backbone first. Once per move; a birth that never held the record owes nothing. The moved
    /// record itself is the fence (`fabric_seat_record`); this pass writes none.
    #[test]
    fn a_held_fabric_seat_record_that_moves_owes_one_hand_over_naming_the_new_holder() {
        let (t, holder, standby, outsider) = world();
        let record = |n: &Node, revision: u64| SeatHolder { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), revision };
        let (me, mut held) = (me_of(&holder), false);
        assert_eq!(seat_yield(Some(&record(&holder, 1)), &me, &mut held, &t), None, "the record names this birth: nothing yields");
        assert_eq!(seat_yield(Some(&record(&outsider, 2)), &me, &mut held, &t), Some(("the seat is now mesh2 100000000000".to_string(), Some("mesh2.admin.1".to_string()))), "the fence names the holder the record names");
        assert_eq!(seat_yield(Some(&record(&outsider, 2)), &me, &mut held, &t), None, "once per move");
        let mut never = false;
        assert_eq!(seat_yield(Some(&record(&outsider, 2)), &me_of(&standby), &mut never, &t), None, "a birth that never held the record owes nothing");
    }

    /// CONTRACT (Q89; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 2;
    /// elections §4.3): the standby of the fabric-primary's own mesh proposes the takeover at the first
    /// tick after the mark, when the record's birth is held off ready-for-traffic in its view, and needs
    /// no provider verdict and no hold. Not a mesh-primary outside that mesh (its proposal is mesh
    /// investigation's decision step), not the holder itself, not a standby that is not ReadyForTraffic,
    /// not while the holder is heard, not without a record.
    #[test]
    fn the_standby_of_the_fabric_primarys_own_mesh_proposes_the_takeover_at_the_mark_and_an_outsider_never_does() {
        let (mut t, holder, standby, outsider) = world();
        let record = SeatHolder { mesh: holder.mesh.clone(), node_id: holder.node_id.clone(), incarnation: holder.incarnation_id.clone().unwrap(), revision: 7 };
        assert_eq!(takeover_due(&t, &me_of(&standby), Some(record.clone())), Some(record.clone()), "the holder PendingReconnect in the standby's view: the standby proposes at the mark");
        assert_eq!(takeover_due(&t, &me_of(&outsider), Some(record.clone())), None, "an admin outside the holder's mesh proposes from the investigation's decision step, never from the seat watch");
        assert_eq!(takeover_due(&t, &me_of(&holder), Some(record.clone())), None, "the holder never proposes its own takeover");
        assert_eq!(takeover_due(&t, &me_of(&standby), None), None, "no record, no takeover");
        t.nodes[0].status = NodeStatus::Dead;
        assert_eq!(takeover_due(&t, &me_of(&standby), Some(record.clone())), Some(record.clone()), "a holder the view holds Dead is held off ready-for-traffic too");
        t.nodes[0].status = NodeStatus::ReadyForTraffic;
        assert_eq!(takeover_due(&t, &me_of(&standby), Some(record.clone())), None, "a holder heard keeps its seats");
        t.nodes[0].status = NodeStatus::PendingReconnect;
        t.nodes[1].status = NodeStatus::Pending;
        assert_eq!(takeover_due(&t, &me_of(&standby), Some(record.clone())), None, "a standby that is not ReadyForTraffic is not the leadership rule's successor");
    }

    /// CONTRACT (Q23): a silent fabric holder is investigated from each admin's own view,
    /// including an admin in another mesh and a standby in the holder's mesh.
    #[test]
    fn an_admin_investigates_a_silent_fabric_holder_from_its_own_view() {
        let (t, holder, standby, outsider) = world();
        for admin in [&standby, &outsider] {
            let p = plan(&t, &admin.name, &me_of(admin), &mut Said::default());
            assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&holder)]);
        }
    }

    /// CONTRACT (Q23): neither a ready holder nor the admin's own exact birth is an investigation
    /// target. No gossip frame is needed to make either decision.
    #[test]
    fn an_admin_investigates_neither_a_ready_holder_nor_its_own_birth() {
        let (mut t, holder, standby, _) = world();
        assert!(plan(&t, &holder.name, &me_of(&holder), &mut Said::default()).investigate.is_empty());
        t.nodes[0].status = NodeStatus::ReadyForTraffic;
        assert!(plan(&t, &standby.name, &me_of(&standby), &mut Said::default()).investigate.is_empty());
    }

    /// CONTRACT (R-A2 items 4-5): an admin looks at a silent birth of the incumbent mesh once per
    /// mark from its own view, and a silent admin of a mesh that holds no seat is
    /// nobody's business.
    #[test]
    fn a_silent_incumbent_birth_is_looked_at_once_per_mark_and_other_silence_is_not() {
        let (mut t, holder, standby, _) = world();
        let mut said = Said::default();
        let own = plan(&t, &standby.name, &me_of(&standby), &mut said);
        assert_eq!(own.investigate.iter().map(|n| n.name.to_string()).collect::<Vec<_>>(), vec!["mesh1.admin.1".to_string()]);
        assert!(plan(&t, &standby.name, &me_of(&standby), &mut said).investigate.is_empty(), "once per mark");
        // A silent admin of a third mesh that holds nothing.
        t.nodes.push(admin("mesh3.admin.1", "500000000000", "x1", NodeStatus::PendingReconnect));
        assert!(plan(&t, &standby.name, &me_of(&standby), &mut said).investigate.is_empty());
        let _ = holder;
    }

    /// CONTRACT: the silent former birth at this admin's path is not its own exact birth.
    /// Investigation uses identity rather than a path match even before the view lists the successor.
    #[test]
    fn an_admin_does_not_treat_a_former_birth_at_its_path_as_itself() {
        let (t, holder, _, _) = world();
        let successor = admin("mesh1.admin.1", "600000000000", "r2", NodeStatus::ReadyForTraffic);
        let p = plan(&t, &successor.name, &me_of(&successor), &mut Said::default());
        assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&holder)]);
    }
}
