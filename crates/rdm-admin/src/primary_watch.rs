//! A silent primary node-admin is looked at by node investigation (node-recovery.md step 2): an admin
//! of the primary's mesh (a standby included), any admin looking at the fabric-primary's mesh from
//! outside it, and the fabric-primary for an unheard mesh-primary, look at the exact silent birth: the
//! one tickle ladder (node-tickle.md: one probe direct, then the same probe through up to two live
//! members of its mesh at once), and on `unreached` the provider's inspection of the birth's exact
//! runtime. An answer from the exact birth is `reached`: the fabric-primary commands it back (Q87).
//! Any other verdict of an unheard mesh-primary is the fabric-primary's recovery of it, at once
//! (node-recovery.md step 3, Q111); the fabric-primary itself is superseded by its successor
//! (`takeover_due`).
//!
//! [`plan`] decides what a pass owes, from the view and births already inspected; [`run`] drives it.

use crate::admin::Records;
use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::Primary;
use rdm_transport::topology::TopologyView;
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
    /// Exact births the view holds ready that left a command unanswered past the floor:
    /// silent whatever they gossip (node-investigation.md, Q182), investigated on the same terms.
    pub unanswered: Vec<Node>,
}

/// Births already investigated during their current silence mark. Hearing the birth again
/// removes the mark; a later silence can then be investigated from the view again.
#[derive(Debug, Default)]
pub struct Said {
    investigated: HashSet<Birth>,
    /// Each node-admin birth of the fabric-primary's mesh this admin found unreached and
    /// uninspectable (`Unproven`), from the first such look: commanded back when heard again. Any
    /// other finding ends it.
    unreachable: std::collections::HashMap<Birth, std::time::Instant>,
}

fn birth(n: &Node) -> Option<Birth> {
    n.incarnation_id.clone().map(|i| (n.node_id.clone(), i))
}

/// What the fabric-primary record this admin holds says of `me`'s Build log: the election's result it
/// installed (lifecycles/node-primary-update.md step 4: the successor "becomes primary" when it installs
/// it; nothing is written). A record naming this birth, naming none, or naming a birth proven gone
/// (`lost`, which the election passes over before this birth's own record lands) ends nothing; a
/// record naming another live birth ends this birth's Build log at its revision, and
/// names that birth's path.name from `name_of` (the digest view, which holds a birth before the
/// projected view does). An open record naming this birth names its path.name as the holder; one
/// naming none or a birth proven gone names no holder. Read from the record alone: no history, no
/// write.
pub fn fabric_seat_record(record: Option<&Primary>, me: &Birth, lost: &HashSet<IncarnationId>, name_of: impl Fn(&NodeId) -> Option<String>) -> crate::fabric_builds::SeatRecord {
    seat_record("the fabric-primary record this admin holds", record, me, lost, name_of)
}

fn seat_record(which: &str, record: Option<&Primary>, me: &Birth, lost: &HashSet<IncarnationId>, name_of: impl Fn(&NodeId) -> Option<String>) -> crate::fabric_builds::SeatRecord {
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

/// The pass uses only this admin's exact birth, its view, the silent births already inspected and
/// `was_mesh_primary`: whether this admin's primary view holds a silent node-admin as its mesh's primary
/// replaced by another birth (the election named a new mesh-primary past the floor).
pub fn plan(view: &Topology, me: &PathName, me_birth: &Birth, said: &mut Said, was_mesh_primary: &dyn Fn(&Node) -> bool, left_unanswered: &dyn Fn(&Node) -> bool) -> Plan {
    // Silent by its gossip, or ready and silent by the command it left unanswered past the
    // floor (node-investigation.md, Q182): whatever it gossips, a node that answers no command is silent.
    let silent_of = |n: &Node| n.kind == NodeKind::NodeAdmin && (n.status == NodeStatus::PendingReconnect || (n.status == NodeStatus::Ready && left_unanswered(n)));
    let silent: HashSet<Birth> = view.members().filter(|n| silent_of(n)).filter_map(birth).collect();
    said.investigated.retain(|b| silent.contains(b));
    let mut plan = Plan::default();
    let mesh = &me.mesh;

    // What this admin sees itself: a silent seat holder of its own mesh, any silent node-admin
    // of the mesh that holds the fabric seat (a standby, or a candidate looking from outside),
    // and, when this admin holds the fabric seat, any silent primary node-admin of a peer mesh, the one
    // the election has replaced as mesh-primary included (node-recovery.md step 2: "For an unheard
    // mesh-primary the fabric-primary runs it, without the half-floor wait, after the election has named
    // the new mesh-primary"). The fabric-primary looks at no other silent node-admin of its own mesh:
    // step 2's "Only the mesh-primary acts" makes a secondary node-admin its mesh-primary's tick's.
    let fabric_mesh = view.members().find(|n| n.is_fabric_primary).map(|n| n.mesh.clone());
    let holds_fabric = view.is_fabric_primary(&me_birth.0);
    for n in view.members().filter(|n| silent_of(n) && n.node_id != me_birth.0) {
        let own_holder = &n.mesh == mesh && (n.is_primary || n.is_fabric_primary || (holds_fabric && was_mesh_primary(n)));
        let incumbent = !holds_fabric && fabric_mesh.as_ref() == Some(&n.mesh);
        let peer_primary = holds_fabric && &n.mesh != mesh && (n.is_primary || was_mesh_primary(n));
        if !(own_holder || incumbent || peer_primary) {
            continue;
        }
        let Some(b) = birth(n) else { continue };
        if said.investigated.insert(b) {
            if n.status == NodeStatus::PendingReconnect {
                plan.investigate.push(n.clone());
            } else {
                plan.unanswered.push(n.clone());
            }
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
    /// Its mesh topology.
    pub topology_view: TopologyView,
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
    /// The commands this admin sent: a birth that left one unanswered past the floor is investigated as
    /// a silent one is, and is not reached by an answer (Q182); `None`: this admin sends none.
    pub commands: Option<Arc<crate::node_commands::CommandView>>,
}

impl Looker {
    /// Look at `n`'s exact birth: node investigation (rulings INDEX Q107 item 1), the leads, the
    /// tickle ladder and, on `unreached`, the provider's inspection (node-tickle.md "What follows";
    /// node-recovery.md step 2, run by the seat watch: fabric-node-lifecycle-elections.md §4.1).
    pub async fn look(&self, view: &Topology, n: &Node) -> Finding {
        let calls = crate::offline::TickleClient { client: self.client.clone(), me_id: self.me_id.clone(), me: self.me.to_string(), connections: self.connections.clone(), commands: self.commands.clone() };
        let finding = crate::node_investigation::investigate(n, view, &calls, self.inspect(n)).await;
        if let Finding::Reached { answer: rdm_core::status::StatusReply::Current { .. }, .. } = &finding {
            // A seat holder this admin holds off its mesh that answers Current is commanded back
            // through node.mesh.join (node-tickle.md "What follows", Q87) by the fabric-primary, the
            // authority above a mesh-primary: every admin looks, one commands (commands go down).
            let commands = view.own(&self.me_id).is_some_and(|me| me.is_fabric_primary);
            if let (true, Some(heard_again), Some(incarnation)) = (commands, self.heard_again.get(), n.incarnation_id.as_ref()) {
                // Held off its mesh in this watch's eyes (it was looked at because it was silent): the
                // view's ready is its last word before the silence, not where it stands now.
                let held = Node { status: NodeStatus::PendingReconnect, ..n.clone() };
                heard_again.command(&held, incarnation).await;
            }
        }
        finding
    }

    /// Whether `n`'s exact birth left a command of this admin unanswered past the staleness floor (Q182).
    pub fn command_unanswered(&self, n: &Node) -> bool {
        let (Some(commands), Some(incarnation)) = (&self.commands, &n.incarnation_id) else { return false };
        commands.unanswered_since(&n.node_id, incarnation, self.topology_view.digest_view.staleness_floor(), std::time::Instant::now()).is_some()
    }

    /// The provider's inspection of `n`'s exact runtime (node.runtime.inspect), its span the evidence
    /// (node-runtime-inspect.md "Observability contract"), named by the path.name this admin holds.
    async fn inspect(&self, n: &Node) -> Finding {
        let (inspected, handle) = crate::node_investigation::inspect_birth_with_handle(&*self.provider, &self.topology_view.digest_view, self.nodes.as_deref(), n).await;
        let me = self.topology_view.digest_view.held_path_name().unwrap_or_else(|| self.me.clone());
        crate::node_investigation::span_inspection(&me.to_string(), n, &inspected, handle.as_ref(), "the seat watch's investigation asked the provider about the unreached birth's exact runtime");
        inspected.into()
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
    /// Its mesh topology.
    pub topology_view: TopologyView,
    /// Its view of the topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// Its records of births and exits.
    pub records: Arc<Records>,
    /// What it looks at a birth with.
    pub looker: Looker,
    /// The recovery submitter: the fabric-primary's recovery of an unheard peer-mesh primary (node-recovery.md step 3).
    pub recovery: Option<crate::admin::RecoverySubmitter>,
    /// Fabric recovery (Q122): this watch starts it at the first tick after the fabric-primary's mark when
    /// this admin is the lowest Ready node-admin of its mesh.
    pub fabric_recovery: Arc<crate::fabric_recovery::FabricRecoveryDoor>,
}

/// Run the watch on seat changes and the gossip cadence, from the admin's own silence evidence.
/// Nothing here publishes an advisory or acts on receipt of one.
pub async fn run(w: Watch) {
    let mut said = Said::default();
    let me_birth = (w.me_id.clone(), w.incarnation.clone());
    let mut seats_changed = w.topology_view.primaries().subscribe();
    // A turn runs from the gate to the next idle wait: the pass below. It is held in an Option so it
    // ends (`take`) right before the idle wait and begins again right after it.
    let mut turn = Some(w.gate.turn("seat-watch").await);
    loop {
        drop(turn.take());
        tokio::select! {
            _ = seats_changed.changed() => {}
            _ = tokio::time::sleep(rdm_transport::topology::gossip_interval()) => {}
        }
        // Held here while the admin is stopped: its own wait, outside every lock.
        turn = Some(w.gate.turn("seat-watch").await);
        // The instant this pass reads its view: a digest of a birth heard after it ends the silence the
        // pass's verdicts are read in.
        let read_at = std::time::Instant::now();
        let view = w.topology.read().await.clone();
        // The mesh the fabric seat's record names: the takeover's hold is kept on its node-admins.
        let fabric_mesh = w.topology_view.primaries().fabric().map(|h| h.mesh.clone());
        // A node-admin in the view whose digest has returned (`Ready`).
        let commands = view.own(&w.me_id).is_some_and(|me| me.is_fabric_primary);
        for n in view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::Ready) {
            if let Some(b) = birth(n) {
                // A seat holder this watch said unreachable, heard again by its digest, is commanded
                // back through node.mesh.join by the fabric-primary (Q87), on its own task.
                if said.unreachable.remove(&b).is_some() && commands {
                    if let (Some(heard_again), Some(incarnation)) = (w.looker.heard_again.get().cloned(), n.incarnation_id.clone()) {
                        // The holder this watch said unreachable: held off ready in its eyes
                        // until this digest, as `watch_marked` holds a marked birth.
                        let n = Node { status: NodeStatus::PendingReconnect, ..n.clone() };
                        // Ended at this admin's stop where it stands (the stop gate's `until_stopped`).
                        tokio::spawn(w.gate.until_stopped(async move { heard_again.command(&n, &incarnation).await; }));
                    }
                }
            }
        }
        let me = w.topology_view.digest_view.held_path_name().unwrap_or_else(|| view.held_name_of(&w.me, Some(&w.me_id)));
        // The fabric-primary unheard past the floor (fabric-recovery.md step 2, its own mesh; Q122): its
        // mesh's lowest Ready node-admin starts fabric recovery at the first tick after the mark,
        // once the record has named the unheard birth for a floor in its own view (a record learned after a
        // heal names a birth whose silence here began before it). It takes no primary role and needs no probe.
        let held_a_floor = w.topology_view.primaries().fabric_held_for() >= w.topology_view.digest_view.staleness_floor();
        w.fabric_recovery.start_if_mine(held_a_floor, false).await;
        let seats = w.topology_view.primaries();
        let was_mesh_primary = |n: &Node| n.incarnation_id.as_ref().is_some_and(|i| seats.replaced(rdm_core::Seat::MeshPrimary, &n.mesh, i));
        let plan = plan(&view, &me, &me_birth, &mut said, &was_mesh_primary, &|n| w.looker.command_unanswered(n));
        let triggers = plan.investigate.into_iter().map(|n| (n, "silence")).chain(plan.unanswered.into_iter().map(|n| (n, "command-unanswered")));
        for (node, trigger) in triggers {
            let span = tracing::info_span!(
                "rdm.node_admin.primary.update.via-investigation",
                node = %me,
                suspect = %node.name,
                suspect_node_id = %node.node_id,
                suspect_incarnation = node.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or(""),
                trigger,
                holds_fabric_seat = node.is_fabric_primary,
                finding = tracing::field::Empty,
                detail = tracing::field::Empty,
                iroh_known_addrs = tracing::field::Empty,
                iroh_active_addrs = tracing::field::Empty,
            );
            // One read of iroh's local view of the suspect, recorded beside the investigation;
            // the finding is the provider's and the Node RPC's, never this.
            let key = node.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
            let seen = rdm_transport::iroh_obs::observe_remote(&w.topology_view.endpoint(), key).await;
            span.record("iroh_known_addrs", seen.known_addrs.as_str());
            span.record("iroh_active_addrs", seen.active_addrs.as_str());
            let finding = w.looker.look(&view, &node).instrument(span.clone()).await;
            span.record("finding", finding.name());
            span.record("detail", finding.detail().as_str());
            // A node-admin of the fabric-primary's mesh unreached and uninspectable is commanded back
            // when heard again; any other finding ends that.
            if let Some(b) = birth(&node) {
                if matches!(finding, Finding::Unproven { .. }) && fabric_mesh.as_deref() == Some(node.mesh.as_str()) {
                    said.unreachable.entry(b).or_insert_with(std::time::Instant::now);
                } else {
                    said.unreachable.remove(&b);
                }
            }
            // Any verdict but `reached` of an unheard mesh-primary is this fabric-primary's recovery of it,
            // at once (node-recovery.md steps 2-3, Q111): nothing holds and nothing waits for a proof.
            // Submitted once per silence and owed until it is opened ([`submit_recovery`]). A recovery that
            // ends Failed is the drift pass's to open again.
            let submitted = match (&finding, w.recovery.clone(), finding.inspected()) {
                (Finding::Reached { .. }, _, _) => false,
                (_, Some(recovery), Some(inspected)) if !node.is_fabric_primary && view.is_fabric_primary(&me_birth.0) && (node.is_primary || was_mesh_primary(&node)) => {
                    let (node, topology, me_id, digest_view) = (node.clone(), w.topology.clone(), me_birth.0.clone(), w.topology_view.digest_view.clone());
                    // Owed until opened, and ended at this admin's stop where it stands (the stop gate's
                    // `until_stopped`): a stopping node takes no new lifecycle step.
                    tokio::spawn(w.gate.until_stopped(async move {
                        submit_recovery(&node, || recovery.submit(&node, &inspected), &topology, &digest_view, &me_id, read_at).await;
                    }));
                    true
                }
                _ => false,
            };
            if !submitted && !matches!(finding, Finding::Reached { .. }) {
                if let Some(b) = birth(&node) {
                    said.investigated.remove(&b);
                }
            }
            span.in_scope(|| tracing::info!("a silent seat holder was looked at"));
        }
    }
}

/// The fabric-primary's recovery of an unheard mesh-primary (node-recovery.md step 3), submitted at
/// once and owed until it is opened: a submit refused (an attempt still holds the mesh) is made again
/// after a delay that doubles, once per silence. The silence the verdict was read in lasts while this
/// admin's view holds the exact birth silent (`PendingReconnect` or `Dead`: the mark never replaces a
/// lifecycle state the mesh-primary recorded, `stopped` among them), this admin is the fabric-primary,
/// and the view has not heard the
/// birth since `since`, the instant the pass that read the verdict read its view (node-recovery.md
/// step 1: the mark "ends when a digest of the exact birth arrives after it, when the birth answers its
/// tickle, or when the view no longer holds that birth"). A birth heard again is reached (step 3): it
/// is commanded back, and its old verdict is never submitted again.
pub async fn submit_recovery<S, F>(node: &Node, mut submit: S, topology: &tokio::sync::RwLock<Topology>, digest_view: &rdm_transport::topology::DigestView, me_id: &NodeId, since: std::time::Instant)
where
    S: FnMut() -> F,
    F: std::future::Future<Output = Result<(), String>>,
{
    let mut backoff = rdm_transport::topology::gossip_interval();
    let ceiling = rdm_transport::topology::staleness_floor() * 4;
    loop {
        match submit().await {
            Ok(()) => break,
            Err(e) => tracing::info!(node = %node.name, error = %e, "the recovery submit of an unheard mesh-primary ended without the recovery"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(ceiling);
        let t = topology.read().await;
        let silent = t.births().any(|n| n.node_id == node.node_id && n.incarnation_id == node.incarnation_id && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead));
        if !silent || !t.is_fabric_primary(me_id) || heard_since(digest_view, node, since) {
            break;
        }
    }
}

/// Whether the view has heard `node`'s exact birth after `since`: a digest of it arrived after that
/// instant, as `heard_again` reads a birth heard since its command.
fn heard_since(digest_view: &rdm_transport::topology::DigestView, node: &Node, since: std::time::Instant) -> bool {
    let now = std::time::Instant::now();
    digest_view.get_at(node.node_id.as_str(), now).is_some_and(|(d, unheard)| Some(&d.node.incarnation) == node.incarnation_id.as_ref() && now.checked_sub(unheard).is_some_and(|heard_at| heard_at > since))
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
        Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: Vec::new(), nodes }
    }

    fn me_of(n: &Node) -> Birth {
        (n.node_id.clone(), n.incarnation_id.clone().unwrap())
    }
    fn world() -> (Topology, Node, Node, Node) {
        let mut holder = admin("mesh1.admin.1", "300000000000", "h1", NodeStatus::PendingReconnect);
        holder.is_primary = true;
        holder.is_fabric_primary = true;
        let standby = admin("mesh1.admin.2", "400000000000", "s1", NodeStatus::Ready);
        let mut outsider = admin("mesh2.admin.1", "100000000000", "o1", NodeStatus::Ready);
        outsider.is_primary = true;
        (view(vec![holder.clone(), standby.clone(), outsider.clone()]), holder, standby, outsider)
    }

    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:14
    /// "- [node investigation](node-investigation.md) of a silent primary ([elections](../fabric-node-lifecycle-elections.md));"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:36
    /// "1. The caller holds the node's id and incarnation and a reason to ask: ... a silent primary"
    /// CONTRACT (Q23): a silent fabric holder is investigated from each admin's own view,
    /// including an admin in another mesh and a standby in the holder's mesh.
    #[test]
    fn an_admin_investigates_a_silent_fabric_primary_from_its_own_view() {
        let (t, holder, standby, outsider) = world();
        for admin in [&standby, &outsider] {
            let p = plan(&t, &admin.name, &me_of(admin), &mut Said::default(), &|_| false, &|_| false);
            assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&holder)]);
        }
    }

    /// CONTRACT (Q23): neither a ready holder nor the admin's own exact birth is an investigation
    /// target. No gossip frame is needed to make either decision.
    #[test]
    fn an_admin_investigates_neither_a_ready_fabric_primary_nor_its_own_birth() {
        let (mut t, holder, standby, _) = world();
        assert!(plan(&t, &holder.name, &me_of(&holder), &mut Said::default(), &|_| false, &|_| false).investigate.is_empty());
        t.nodes[0].status = NodeStatus::Ready;
        assert!(plan(&t, &standby.name, &me_of(&standby), &mut Said::default(), &|_| false, &|_| false).investigate.is_empty());
    }

    /// CONTRACT (node-investigation.md, Q182): "A node that does not answer a command is silent, whatever
    /// it gossips": a node-admin the view holds ready that left a command unanswered past the
    /// floor is investigated like a silent one, once per time it is so, on the same terms of who looks;
    /// one that answers its commands, and one already listed silent, are not listed twice. Must NOT
    /// happen: a gossiping node-admin with no command unanswered investigated, or a silent one
    /// investigated under both triggers.
    #[test]
    fn a_ready_admin_that_left_a_command_unanswered_is_investigated_once_as_a_silent_one_is() {
        let (mut t, holder, standby, _) = world();
        t.nodes[0].status = NodeStatus::Ready;
        let fabric_primary_unanswered = |n: &Node| n.node_id == holder.node_id;
        let mut said = Said::default();
        let none = plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &|_| false);
        assert!(none.investigate.is_empty() && none.unanswered.is_empty(), "every command answered: nothing to investigate");
        let p = plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &fabric_primary_unanswered);
        assert_eq!(p.unanswered.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&holder)], "gossiping, and silent for the command it left unanswered");
        assert!(p.investigate.is_empty(), "it is not silent by its gossip");
        let again = plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &fabric_primary_unanswered);
        assert!(again.unanswered.is_empty() && again.investigate.is_empty(), "once per time it is silent");
        let _ = plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &|_| false);
        let back = plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &fabric_primary_unanswered);
        assert_eq!(back.unanswered.len(), 1, "answering ended it; a later unanswered command is silence again");
        t.nodes[0].status = NodeStatus::PendingReconnect;
        let both = plan(&t, &standby.name, &me_of(&standby), &mut Said::default(), &|_| false, &fabric_primary_unanswered);
        assert_eq!((both.investigate.len(), both.unanswered.len()), (1, 0), "a node silent by its gossip is listed once, as silent");
    }

    /// CONTRACT (R-A2 items 4-5): an admin looks at a silent birth of the incumbent mesh once per
    /// mark from its own view, and a silent admin of a mesh that holds no seat is
    /// nobody's business.
    #[test]
    fn a_silent_incumbent_birth_is_looked_at_once_per_mark_and_other_silence_is_not() {
        let (mut t, holder, standby, _) = world();
        let mut said = Said::default();
        let own = plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &|_| false);
        assert_eq!(own.investigate.iter().map(|n| n.name.to_string()).collect::<Vec<_>>(), vec!["mesh1.admin.1".to_string()]);
        assert!(plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &|_| false).investigate.is_empty(), "once per mark");
        // A silent admin of a third mesh that holds nothing.
        t.nodes.push(admin("mesh3.admin.1", "500000000000", "x1", NodeStatus::PendingReconnect));
        assert!(plan(&t, &standby.name, &me_of(&standby), &mut said, &|_| false, &|_| false).investigate.is_empty());
        let _ = holder;
    }

    /// CONTRACT: the silent former birth at this admin's path is not its own exact birth.
    /// Investigation uses identity rather than a path match even before the view lists the successor.
    #[test]
    fn an_admin_does_not_treat_a_former_birth_at_its_path_as_itself() {
        let (t, holder, _, _) = world();
        let successor = admin("mesh1.admin.1", "600000000000", "r2", NodeStatus::Ready);
        let p = plan(&t, &successor.name, &me_of(&successor), &mut Said::default(), &|_| false, &|_| false);
        assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&holder)]);
    }

    /// CONTRACT (node-recovery.md step 2: "For an unheard mesh-primary the fabric-primary runs it,
    /// without the half-floor wait, after the election has named the new mesh-primary"): the
    /// fabric-primary investigates a silent node-admin of a peer mesh that its primary view holds as that
    /// mesh's primary replaced by the election, though the view no longer names it primary; a silent
    /// node-admin of a peer mesh that was never its primary is that mesh's primary's to investigate.
    #[test]
    fn the_fabric_primary_investigates_a_peer_meshs_former_primary_after_the_election_replaced_it() {
        let mut fp = admin("mesh1.admin.1", "300000000000", "h1", NodeStatus::Ready);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let former = admin("mesh2.admin.1", "100000000000", "o1", NodeStatus::PendingReconnect);
        let mut elected = admin("mesh2.admin.2", "200000000000", "o2", NodeStatus::Ready);
        elected.is_primary = true;
        let member = admin("mesh2.admin.3", "500000000000", "o3", NodeStatus::PendingReconnect);
        let t = view(vec![fp.clone(), former.clone(), elected, member]);
        let replaced = |n: &Node| n.node_id == former.node_id;
        let p = plan(&t, &fp.name, &me_of(&fp), &mut Said::default(), &replaced, &|_| false);
        assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&former)], "the replaced mesh-primary only");
        assert!(plan(&t, &fp.name, &me_of(&fp), &mut Said::default(), &|_| false, &|_| false).investigate.is_empty(), "no silent primary of a peer mesh");
    }

    fn digest_of(n: &Node) -> rdm_core::MeshDigest {
        use crate::model::FabricId;
        rdm_core::MeshDigest {
            held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: crate::model::EndpointId(String::new()), transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)), incarnation: n.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }
    }

    /// The fabric-primary mesh1.admin.1 and mesh2's primary mesh2.admin.1, unheard past the floor.
    fn unheard_peer_primary() -> (Node, Node, Topology) {
        let mut fp = admin("mesh1.admin.1", "300000000000", "h1", NodeStatus::Ready);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let mut peer = admin("mesh2.admin.1", "100000000000", "o1", NodeStatus::PendingReconnect);
        peer.is_primary = true;
        let t = view(vec![fp.clone(), peer.clone()]);
        (fp, peer, t)
    }

    /// CONTRACT (node-recovery.md step 1: the mark "ends when a digest of the exact birth arrives after
    /// it, when the birth answers its tickle, or when the view no longer holds that birth"; step 3:
    /// "once per silence"): the fabric-primary's recovery of an unheard mesh-primary whose submit was
    /// refused build-in-progress is never submitted again once the exact birth is heard again after the
    /// pass that read its verdict: the birth answered and rejoined, and its old verdict would re-create
    /// a running node-admin.
    #[tokio::test(start_paused = true)]
    async fn a_refused_recovery_is_not_submitted_again_once_the_birth_is_heard_again() {
        let (fp, peer, t) = unheard_peer_primary();
        let (topology, digest_view) = (tokio::sync::RwLock::new(t), rdm_transport::topology::DigestView::default());
        let since = std::time::Instant::now();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let (calls_ref, digest_view_ref, heard) = (&calls, &digest_view, digest_of(&peer));
        let submit = || {
            let heard = heard.clone();
            async move {
                // Refused: an attempt still holds the mesh. The birth answers its tickle and rejoins
                // after the refusal: its digest arrives.
                if calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    assert!(digest_view_ref.record(heard), "the view takes the birth's digest");
                }
                Err("build-in-progress: BuildInProgress { mesh: Some(\"mesh2\") }".to_string())
            }
        };
        let ended = tokio::time::timeout(rdm_transport::topology::staleness_floor() * 20, submit_recovery(&peer, submit, &topology, &digest_view, &fp.node_id, since)).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1, "a birth heard again after its refused submit is never submitted again (no recovery, no TerminateRuntime of the rejoined birth)");
        assert!(ended.is_ok(), "the owed submit ends with the silence");
    }

    /// CONTRACT (node-recovery.md step 1: the mark "never replaces a lifecycle state the node reported
    /// and the mesh-primary recorded (`connections-deleting`, `stopping`, `stopped`)"): a refused recovery
    /// whose birth the view now holds in such a state (`Leaving`, as the view shows a stopped birth) is never
    /// submitted again.
    #[tokio::test(start_paused = true)]
    async fn a_refused_recovery_is_not_submitted_again_once_the_view_holds_the_birth_off_its_mark() {
        let (fp, peer, t) = unheard_peer_primary();
        let (topology, digest_view) = (tokio::sync::RwLock::new(t), rdm_transport::topology::DigestView::default());
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let (calls_ref, topology_ref, peer_id) = (&calls, &topology, peer.node_id.clone());
        let submit = || {
            let peer_id = peer_id.clone();
            async move {
                if calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    topology_ref.write().await.nodes.iter_mut().filter(|n| n.node_id == peer_id).for_each(|n| n.status = NodeStatus::Leaving);
                }
                Err("build-in-progress".to_string())
            }
        };
        let ended = tokio::time::timeout(rdm_transport::topology::staleness_floor() * 20, submit_recovery(&peer, submit, &topology, &digest_view, &fp.node_id, std::time::Instant::now())).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1, "a birth the view holds stopping is never submitted again");
        assert!(ended.is_ok(), "the owed submit ends with the mark");
    }

    /// CONTRACT (node-recovery.md step 3: "Any other verdict ...: the recovery below, at once"): a
    /// birth still unheard keeps its refused recovery owed until a submit opens it.
    #[tokio::test(start_paused = true)]
    async fn a_refused_recovery_of_a_birth_still_unheard_is_submitted_until_opened() {
        let (fp, peer, t) = unheard_peer_primary();
        let (topology, digest_view) = (tokio::sync::RwLock::new(t), rdm_transport::topology::DigestView::default());
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let calls_ref = &calls;
        let submit = || async move {
            match calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 | 1 => Err("build-in-progress".to_string()),
                _ => Ok(()),
            }
        };
        tokio::time::timeout(rdm_transport::topology::staleness_floor() * 20, submit_recovery(&peer, submit, &topology, &digest_view, &fp.node_id, std::time::Instant::now())).await.expect("the submit that opens the recovery ends the loop");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3, "submitted until a submit opens it");
    }

    /// CONTRACT (node-recovery.md step 2: "Only the mesh-primary acts. ... For an unheard mesh-primary
    /// the fabric-primary runs it"): the fabric-primary opens no investigation of a silent secondary
    /// node-admin of its own mesh (its mesh-primary's tick looks at it); an admin that is not the
    /// fabric-primary still looks at it, and a fabric-primary that is not its mesh's primary looks at
    /// that mesh-primary when it is unheard.
    #[test]
    fn the_fabric_primary_leaves_a_silent_secondary_admin_of_its_own_mesh_to_its_mesh_primary() {
        let mut fp = admin("mesh1.admin.1", "300000000000", "h1", NodeStatus::Ready);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let secondary = admin("mesh1.admin.2", "400000000000", "s1", NodeStatus::PendingReconnect);
        let other = admin("mesh1.admin.3", "500000000000", "s3", NodeStatus::Ready);
        let t = view(vec![fp.clone(), secondary.clone(), other.clone()]);
        assert!(plan(&t, &fp.name, &me_of(&fp), &mut Said::default(), &|_| false, &|_| false).investigate.is_empty(), "the fabric-primary opens no investigation of a secondary node-admin of its own mesh");
        assert_eq!(plan(&t, &other.name, &me_of(&other), &mut Said::default(), &|_| false, &|_| false).investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&secondary)], "an admin that is not the fabric-primary still looks");
        let mut fp_only = admin("mesh1.admin.1", "300000000000", "h1", NodeStatus::Ready);
        fp_only.is_fabric_primary = true;
        let mut primary = admin("mesh1.admin.2", "400000000000", "s1", NodeStatus::PendingReconnect);
        primary.is_primary = true;
        let t = view(vec![fp_only.clone(), primary.clone(), other.clone()]);
        assert_eq!(plan(&t, &fp_only.name, &me_of(&fp_only), &mut Said::default(), &|_| false, &|_| false).investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&primary)], "an unheard mesh-primary of its own mesh is the fabric-primary's");
        // The election replaced it: the view names another mesh-primary, the primary view the replaced birth.
        let former = Node { is_primary: false, ..primary.clone() };
        let elected = Node { is_primary: true, ..other };
        let never = admin("mesh1.admin.4", "600000000000", "s4", NodeStatus::PendingReconnect);
        let t = view(vec![fp_only.clone(), former.clone(), elected, never]);
        let replaced = |n: &Node| n.node_id == former.node_id;
        assert_eq!(plan(&t, &fp_only.name, &me_of(&fp_only), &mut Said::default(), &replaced, &|_| false).investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&former)], "the replaced mesh-primary of its own mesh only, never a secondary node-admin");
    }
}
