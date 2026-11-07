//! A silent primary node-admin is looked at by node investigation (node-recovery.md step 2): an admin
//! of the primary's mesh (a standby included), any admin looking at the fabric-primary's mesh from
//! outside it, and the fabric-primary for an unheard mesh-primary or one it marked for a spent call
//! (node-investigation.md; Q338), look at the exact silent birth: the
//! one tickle ladder (node-tickle.md: one probe direct, then the same probe through up to two live
//! members of its mesh at once), and on `unreached` the provider's inspection of the birth's exact
//! runtime. An answer from the exact birth is `reached`: the fabric-primary commands it back (Q87).
//! A lost mesh-primary is investigated and recovered by the successor its election names, as any member
//! of its mesh; the fabric-primary looks at it, and recovers it at once on any other verdict
//! (node-recovery.md step 3, Q111), only when no successor takes the role (Q335). The fabric-primary
//! itself is superseded by its successor (`takeover_due`).
//!
//! [`plan`] decides what a pass owes, from the view and births already inspected; [`run`] drives it.

use crate::admin::Records;
use crate::model::{IncarnationId, Node, NodeId, NodeStatus};
use crate::topology::Topology;
use rdm_core::Primary;
use rdm_gossip::topology::TopologyView;
use rdm_transport::NodeRpcClient;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::Instrument as _;

type Birth = (NodeId, IncarnationId);

/// What one pass over the view owes.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Exact silent births to investigate from this admin's own view: unheard (`PendingReconnect`), or
    /// marked by this admin for a call whose attempts are spent, whatever they gossip
    /// (node-investigation.md; Q338).
    pub investigate: Vec<Node>,
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
/// names that birth's globally unique node ID directly from the record. An open record naming this
/// birth names its node ID as the primary; one
/// naming none or a birth proven gone names no primary. Read from the record alone: no history, no
/// write.
pub fn fabric_primary_record(record: Option<&Primary>, me: &Birth, lost: &HashSet<IncarnationId>) -> crate::fabric_builds::PrimaryRecord {
    primary_record("the fabric-primary record this admin holds", record, me, lost)
}

fn primary_record(which: &str, record: Option<&Primary>, me: &Birth, lost: &HashSet<IncarnationId>) -> crate::fabric_builds::PrimaryRecord {
    match record {
        Some(h) if !h.is_birth(&me.0, &me.1) && !lost.contains(&h.incarnation) => {
            let to = Some(rdm_core::framing::Birth { node_id: h.node_id.clone(), incarnation: h.incarnation.clone() });
            let by = format!("{which} names mesh {} node_id {} incarnation {} at revision {}", h.mesh, h.node_id, h.incarnation.as_str(), h.revision);
            crate::fabric_builds::PrimaryRecord::Moved { by, to }
        }
        Some(h) if h.is_birth(&me.0, &me.1) => crate::fabric_builds::PrimaryRecord::Open { primary: Some(h.node_id.clone()) },
        _ => crate::fabric_builds::PrimaryRecord::Open { primary: None },
    }
}

/// The pass uses only this admin's mesh and exact birth, its view, the silent births already inspected,
/// `was_mesh_primary`: whether this admin's primary view holds a silent node-admin as its mesh's primary
/// replaced by another birth (the election named a new mesh-primary past the floor), and `marked`:
/// whether this admin holds a standing mark of the exact birth for a call whose attempts are spent, and
/// `aggregate_unheard`: whether a peer mesh's aggregate has stayed unheard on the backbone for twice the
/// staleness floor (no successor took its primary role, Q349).
pub fn plan(view: &Topology, mesh: &str, me_birth: &Birth, said: &mut Said, was_mesh_primary: &dyn Fn(&Node) -> bool, marked: &dyn Fn(&Node) -> bool, aggregate_unheard: &dyn Fn(&str) -> bool) -> Plan {
    // Silent by its gossip, or marked at once for a call whose attempts are spent (node-investigation.md:
    // "A node that does not answer a call is silent, whatever it gossips"; Q332, Q338).
    let silent_of = |n: &Node| rdm_core::is_node_admin(&n.r#type) && (n.status == NodeStatus::PendingReconnect || marked(n));
    let silent: HashSet<Birth> = view.births().filter(|n| silent_of(n)).filter_map(birth).collect();
    said.investigated.retain(|b| silent.contains(b));
    let mut plan = Plan::default();

    // What this admin sees itself: a silent primary of its own mesh, any silent node-admin
    // of the mesh that holds the fabric-primary role (a standby, or a candidate looking from outside),
    // and, when this admin holds the fabric-primary role, a silent primary node-admin of a peer mesh, the one
    // the election has replaced as mesh-primary included, only when that mesh's aggregate stays unheard
    // on the backbone for twice the staleness floor: no successor took the role (node-recovery.md step 2,
    // Q335, Q349: the election names the successor at the floor, and the window after it is the mesh's own
    // node-admin's to act in). In its own mesh a replaced mesh-primary always has a successor, Ready (the
    // fabric-primary itself at least), whose tick investigates it as any member.
    // The fabric-primary looks at no other silent node-admin of its own mesh: step 2's "Only the
    // mesh-primary acts" makes a secondary node-admin its mesh-primary's tick's.
    let fabric_mesh = view.births().find(|n| n.is_fabric_primary).map(|n| n.mesh.clone());
    let holds_fabric = view.is_fabric_primary(&me_birth.0);
    for n in view.births().filter(|n| silent_of(n) && n.node_id != me_birth.0) {
        let own_primary = n.mesh == mesh && (n.is_primary || n.is_fabric_primary);
        let incumbent = !holds_fabric && fabric_mesh.as_ref() == Some(&n.mesh);
        let peer_primary = holds_fabric && n.mesh != mesh && (n.is_primary || was_mesh_primary(n)) && aggregate_unheard(&n.mesh);
        if !(own_primary || incumbent || peer_primary) {
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
    /// This admin's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
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
    /// A primary heard again is commanded back through this (Q87), once the admin holds it.
    pub heard_again: crate::heard_again::HeardAgainSlot,
    /// Its connections store: the by-destination index names the leads of a look (Q107 item 1).
    pub connections: Option<Arc<dyn crate::storage::ConnectionsStorage>>,
}

impl Looker {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

impl Looker {
    /// Look at `n`'s exact birth: node investigation (rulings INDEX Q107 item 1), the leads, the
    /// tickle ladder and, on `unreached`, the provider's inspection (node-tickle.md "What follows";
    /// node-recovery.md step 2, run by the primary watch: fabric-node-lifecycle-elections.md §4.1).
    pub async fn look(&self, view: &Topology, n: &Node) -> Finding {
        let calls = crate::offline::TickleClient { client: self.client.clone(), me_id: self.me_id.clone(), me: self.me_id.to_string(), connections: self.connections.clone() };
        let finding = crate::node_investigation::investigate(n, view, &calls, self.inspect(n)).await;
        if let Finding::Reached { answer: rdm_core::status::StatusReply::Current { .. }, .. } = &finding {
            // A primary this admin holds off its mesh that answers Current is commanded back
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

    /// A node-admin `mesh` has left besides the birth `lost`: node investigation of each other node-admin
    /// birth of the mesh, the first `reached` or inspected `running` (Q153: a runtime that runs but is not
    /// reached is unreachable from here, not lost). `None`: the mesh has no node-admin left
    /// (fabric-recovery.md "When the mesh has no node-admin left"; Q335 item 4). Named on its span. The
    /// one check both the fabric-primary's recovery of a mesh-primary with no successor and fabric
    /// recovery's backbone arm make.
    pub async fn node_admin_left(&self, view: &Topology, mesh: &str, lost: &NodeId) -> Option<Node> {
        let others: Vec<Node> = view.nodes_of_type(mesh, "node-admin").filter(|n| n.node_id != *lost && n.incarnation_id.is_some()).cloned().collect();
        for n in others {
            let finding = self.look(view, &n).await;
            if matches!(finding, Finding::Reached { .. } | Finding::Running) {
                tracing::info_span!("rdm.node_admin.node.resolve.via-node-admin-left", observer = %self.me_id, mesh, lost_node_id = %lost, subject_path = %n.path, subject_name = %n.name, subject_node_id = %n.node_id, finding = finding.name())
                    .in_scope(|| tracing::info!("a node-admin of the mesh is left: the mesh's own election names it, and nothing is executed from here"));
                return Some(n);
            }
        }
        None
    }

    /// The provider's inspection of `n`'s exact runtime (node.runtime.inspect), its span the evidence
    /// (node-runtime-inspect.md "Observability contract"), named by the path and name this admin holds.
    async fn inspect(&self, n: &Node) -> Finding {
        let (inspected, handle) = crate::node_investigation::inspect_birth_with_handle(&*self.provider, &self.topology_view.digest_view, self.nodes.as_deref(), n).await;
        crate::node_investigation::span_inspection(self.me_id.as_str(), n, &inspected, handle.as_ref(), "the primary watch's investigation asked the provider about the unreached birth's exact runtime");
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
    /// This admin's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// This admin's NodeId.
    pub me_id: NodeId,
    /// This admin's mesh.
    pub mesh: String,
    /// This admin's exact birth.
    pub incarnation: IncarnationId,
    /// Its mesh topology.
    pub topology_view: TopologyView,
    /// Its channels: iroh's local view of a suspect is read from their endpoint.
    pub channels: rdm_gossip::channel::Channels,
    /// Its view of the topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// Its records of births and exits.
    pub records: Arc<Records>,
    /// What it looks at a birth with.
    pub looker: Looker,
    /// The recovery submitter: the fabric-primary's recovery of a lost peer-mesh primary no successor takes the role of (node-recovery.md step 3, Q335).
    pub recovery: Option<crate::admin::RecoverySubmitter>,
    /// Fabric recovery (Q122): this watch starts it at the first tick after the fabric-primary's mark when
    /// this admin is the lowest Ready node-admin of its mesh.
    pub fabric_recovery: Arc<crate::fabric_recovery::FabricRecoveryService>,
}

impl Watch {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

/// Run the watch on role changes and the gossip cadence, from the admin's own silence evidence.
/// Nothing here publishes an advisory or acts on receipt of one.
pub async fn run(w: Watch) {
    let mut said = Said::default();
    let me_birth = (w.me_id.clone(), w.incarnation.clone());
    let mut primaries_changed = w.topology_view.primaries().subscribe();
    // A turn runs from the gate to the next idle wait: the pass below. It is held in an Option so it
    // ends (`take`) right before the idle wait and begins again right after it.
    let mut turn = Some(w.gate.turn("primary-watch").await);
    loop {
        drop(turn.take());
        tokio::select! {
            _ = primaries_changed.changed() => {}
            _ = tokio::time::sleep(rdm_core::cadence::gossip_interval()) => {}
        }
        // Held here while the admin is stopped: its own wait, outside every lock.
        turn = Some(w.gate.turn("primary-watch").await);
        // The instant this pass reads its view: a digest of a birth heard after it ends the silence the
        // pass's verdicts are read in.
        let read_at = std::time::Instant::now();
        let view = w.topology.read().await.clone();
        // The mesh the fabric-primary role's record names: the takeover's hold is kept on its node-admins.
        let fabric_mesh = w.topology_view.primaries().fabric().map(|h| h.mesh.clone());
        // A node-admin in the view whose digest has returned (`Ready`).
        let commands = view.own(&w.me_id).is_some_and(|me| me.is_fabric_primary);
        for n in view.births().filter(|n| rdm_core::is_node_admin(&n.r#type) && n.status == NodeStatus::Ready) {
            if let Some(b) = birth(n) {
                // A primary this watch said unreachable, heard again by its digest, is commanded
                // back through node.mesh.join by the fabric-primary (Q87), on its own task.
                if said.unreachable.remove(&b).is_some() && commands {
                    if let (Some(heard_again), Some(incarnation)) = (w.looker.heard_again.get().cloned(), n.incarnation_id.clone()) {
                        // The primary this watch said unreachable: held off ready in its eyes
                        // until this digest, as `watch_marked` holds a marked birth.
                        let n = Node { status: NodeStatus::PendingReconnect, ..n.clone() };
                        // Ended at this admin's stop where it stands (the stop gate's `until_stopped`).
                        tokio::spawn(w.gate.until_stopped(async move { heard_again.command(&n, &incarnation).await; }));
                    }
                }
            }
        }
        let me = w.topology_view.digest_view.held_path_name().unwrap_or_else(|| view.held_name_of(&w.me(), Some(&w.me_id)));
        // The fabric-primary unheard past the floor (fabric-recovery.md step 2; Q122, Q335): its mesh's
        // lowest Ready node-admin starts fabric recovery at the first tick after the mark, once the record has
        // named the unheard birth for a floor in its own view (a record learned after a heal names a birth
        // whose silence here began before it); with none, the lowest Ready mesh-primary on the backbone, once
        // the fabric-primary has been unheard for twice the floor (Q349).
        // It takes no primary role; this watch's node investigation of the lost birth, below, decides (Q337).
        let held_a_floor = w.topology_view.primaries().fabric_held_for() >= w.topology_view.digest_view.staleness_floor();
        w.fabric_recovery.start_if_mine(held_a_floor).await;
        // An unheard mesh-primary: replaced by the election and unheard since (node-recovery.md step 2), or,
        // with no successor named (no node-admin of its mesh Ready), the birth its mesh's primary record still
        // names (Q335 item 4: the election names nobody). One heard after it was replaced is one more
        // node-admin of its mesh, its mesh-primary's to investigate.
        let now = std::time::Instant::now();
        let was_mesh_primary = |n: &Node| {
            n.incarnation_id.as_ref().is_some_and(|i| {
                w.topology_view.digest_view.unheard_since_replaced(&n.mesh, n.node_id.as_str(), i, now) || w.topology_view.primaries().mesh(&n.mesh).is_some_and(|r| r.is_birth(&n.node_id, i))
            })
        };
        // The marks that stand (node-recovery.md step 1): a mesh-primary the fabric-primary marked for a
        // call whose attempts are spent, its own or one reported to it (Q338), is silent from the mark.
        let marks = crate::offline::held_marks(&w.records.unreachable, &view, &w.topology_view.digest_view, now);
        let marked = |n: &Node| marks.get(&n.node_id).is_some_and(|m| n.incarnation_id.as_ref() == Some(&m.incarnation));
        // A peer mesh's aggregate unheard on the backbone for twice the floor: no successor took its role
        // (Q349: the election names one at the floor; the floor after it is the mesh's own to act in).
        let floor = w.topology_view.digest_view.staleness_floor();
        let aggregate_unheard = |m: &str| w.topology_view.digest_view.mesh_unheard(m, now).is_none_or(|unheard| unheard >= floor * 2);
        let mut plan = plan(&view, &w.mesh, &me_birth, &mut said, &was_mesh_primary, &marked, &aggregate_unheard);
        // The lost birth fabric recovery runs for here, when the workflow asks for a verdict on it: it is
        // unheard past the floor in the fabric record's eyes (fabric-recovery.md step 2), whatever status
        // and role its view row holds, and this watch's node investigation of it is the workflow's
        // verdict, its one look (Q337).
        if let Some(asked) = w.fabric_recovery.asked() {
            if let Some(n) = view.births().find(|n| n.node_id == asked.node_id && n.incarnation_id.as_ref() == Some(&asked.incarnation)) {
                if !plan.investigate.iter().any(|p| p.node_id == n.node_id) {
                    plan.investigate.push(n.clone());
                }
            }
        }
        for node in plan.investigate {
            let trigger = if marked(&node) { "call-failed" } else { "silence" };
            let span = tracing::info_span!(
                "rdm.node_admin.primary.update.via-investigation",
                node_path = %me.path, node_name = %me.name,
                suspect_id = %node.node_id,
                suspect_node_id = %node.node_id,
                suspect_incarnation = node.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or(""),
                trigger,
                holds_fabric_role = node.is_fabric_primary,
                finding = tracing::field::Empty,
                detail = tracing::field::Empty,
                iroh_known_addrs = tracing::field::Empty,
                iroh_active_addrs = tracing::field::Empty,
            );
            // One read of iroh's local view of the suspect, recorded beside the investigation;
            // the finding is the provider's and the Node RPC's, never this.
            let key = node.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
            let seen = rdm_transport::iroh_obs::observe_remote(&w.channels.endpoint(), key).await;
            span.record("iroh_known_addrs", seen.known_addrs.as_str());
            span.record("iroh_active_addrs", seen.active_addrs.as_str());
            let finding = w.looker.look(&view, &node).instrument(span.clone()).await;
            // The verdict on the fabric-primary's lost birth is fabric recovery's input (Q337): the
            // workflow running here for that birth acts on it, with no look of its own.
            w.fabric_recovery.take_verdict(&node, &finding);
            span.record("finding", finding.name());
            span.record("detail", finding.detail().as_str());
            // A birth that answered its tickle ends its mark (node-recovery.md step 1).
            if let (Finding::Reached { .. }, Some(incarnation)) = (&finding, node.incarnation_id.as_ref()) {
                w.records.unreachable.lock().unwrap().retain(|id, m| !(id == &node.node_id && &m.incarnation == incarnation));
            }
            // A node-admin of the fabric-primary's mesh unreached and uninspectable is commanded back
            // when heard again; any other finding ends that.
            if let Some(b) = birth(&node) {
                if matches!(finding, Finding::Unproven { .. }) && fabric_mesh.as_deref() == Some(node.mesh.as_str()) {
                    said.unreachable.entry(b).or_insert_with(std::time::Instant::now);
                } else {
                    said.unreachable.remove(&b);
                }
            }
            // Any verdict but `reached` of a lost mesh-primary no successor took the role of is recovered at
            // once (node-recovery.md steps 2-3, Q111): nothing holds and nothing waits for a proof. A lost
            // mesh-primary with a successor is the successor's, as any member of its mesh (Q335); this watch
            // looks at one only when its mesh's aggregate stays unheard past the floor ([`plan`]). Of those,
            // the fabric-primary recovers it itself only when the mesh has no node-admin left (Q335 item 4;
            // fabric-recovery.md "When the mesh has no node-admin left"; Q153); one with a node-admin left
            // (unheard here, which a cut makes) elects it, and its mesh-primary recovers the lost birth:
            // nothing is written here, and the birth is not looked at again in this silence. The recovery is
            // submitted once per silence and owed until it runs ([`submit_recovery`]).
            let unheard_primary = !matches!(finding, Finding::Reached { .. }) && finding.inspected().is_some() && !node.is_fabric_primary && view.is_fabric_primary(&me_birth.0) && (node.is_primary || was_mesh_primary(&node));
            let left = if unheard_primary { w.looker.node_admin_left(&view, &node.mesh, &node.node_id).await } else { None };
            let recovers = unheard_primary && w.recovery.is_some() && left.is_none();
            let submitted = match (&finding, w.recovery.clone(), finding.inspected()) {
                (Finding::Reached { .. }, _, _) => false,
                _ if unheard_primary && left.is_some() => true,
                (_, Some(recovery), Some(inspected)) if recovers => {
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
            span.in_scope(|| tracing::info!("a silent primary was looked at"));
        }
    }
}

/// The fabric-primary's recovery of a lost mesh-primary no successor takes the role of, its mesh with no
/// node-admin left (node-recovery.md step 3, Q335), submitted at once and owed until it runs: a submit
/// that ends without the recovery (a refusal by name, or the recovery Build's own end) is made again
/// after the schedule's next delay (node-rpc.md §43), once per silence. The silence the verdict was read in lasts while this
/// admin's view holds the exact birth silent (`PendingReconnect` or `Dead`: the mark never replaces a
/// lifecycle state the mesh-primary recorded, `stopped` among them), this admin is the fabric-primary,
/// and the view has not heard the
/// birth since `since`, the instant the pass that read the verdict read its view (node-recovery.md
/// step 1: the mark "ends when a digest of the exact birth arrives after it, when the birth answers its
/// tickle, or when the view no longer holds that birth"). A birth heard again is reached (step 3): it
/// is commanded back, and its old verdict is never submitted again.
pub async fn submit_recovery<S, F>(node: &Node, mut submit: S, topology: &tokio::sync::RwLock<Topology>, digest_view: &rdm_gossip::digest_view::DigestView, me_id: &NodeId, since: std::time::Instant)
where
    S: FnMut() -> F,
    F: std::future::Future<Output = Result<(), String>>,
{
    // The submit is the recovery's Build, run in process: it ends by a named refusal or the Build's own
    // end, a domain answer, never by no reply. Its turns wait on the one schedule (node-rpc.md §43).
    let pacing = rdm_transport::Pacing::from_cadences();
    let mut turn = 0u32;
    loop {
        match submit().await {
            Ok(()) => break,
            Err(e) => tracing::info!(node_path = %node.path, node_name = %node.name, error = %e, "the recovery submit of an unheard mesh-primary ended without the recovery"),
        }
        turn = turn.saturating_add(1);
        tokio::time::sleep(pacing.after(turn)).await;
        let t = topology.read().await;
        let silent = t.births().any(|n| n.node_id == node.node_id && n.incarnation_id == node.incarnation_id && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead));
        if !silent || !t.is_fabric_primary(me_id) || heard_since(digest_view, node, since) {
            break;
        }
    }
}

/// Whether the view has heard `node`'s exact birth after `since`: a digest of it arrived after that
/// instant, as `heard_again` reads a birth heard since its command.
fn heard_since(digest_view: &rdm_gossip::digest_view::DigestView, node: &Node, since: std::time::Instant) -> bool {
    let now = std::time::Instant::now();
    digest_view.get_at(node.node_id.as_str(), now).is_some_and(|(d, unheard)| Some(&d.node.incarnation) == node.incarnation_id.as_ref() && now.checked_sub(unheard).is_some_and(|heard_at| heard_at > since))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin(path: rdm_core::PathName, mesh: &str, r#type: &str, id: &str, inc: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(path, mesh, r#type);
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
        let mut primary = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "300000000000", "h1", NodeStatus::PendingReconnect);
        primary.is_primary = true;
        primary.is_fabric_primary = true;
        let standby = admin(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", "400000000000", "s1", NodeStatus::Ready);
        let mut outsider = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "100000000000", "o1", NodeStatus::Ready);
        outsider.is_primary = true;
        (view(vec![primary.clone(), standby.clone(), outsider.clone()]), primary, standby, outsider)
    }

    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:14
    /// "- [node investigation](node-investigation.md) of a silent primary ([elections](../fabric-node-lifecycle-elections.md));"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:36
    /// "1. The caller holds the node's id and incarnation and a reason to ask: ... a silent primary"
    /// CONTRACT (Q23): a silent fabric primary is investigated from each admin's own view,
    /// including an admin in another mesh and a standby in the primary's mesh.
    #[test]
    fn an_admin_investigates_a_silent_fabric_primary_from_its_own_view() {
        let (t, primary, standby, outsider) = world();
        for admin in [&standby, &outsider] {
            let p = plan(&t, &admin.mesh, &me_of(admin), &mut Said::default(), &|_| false, &|_| false, &|_| true);
            assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&primary)]);
        }
    }

    /// CONTRACT (Q23): neither a ready primary nor the admin's own exact birth is an investigation
    /// target. No gossip frame is needed to make either decision.
    #[test]
    fn an_admin_investigates_neither_a_ready_fabric_primary_nor_its_own_birth() {
        let (mut t, primary, standby, _) = world();
        assert!(plan(&t, &primary.mesh, &me_of(&primary), &mut Said::default(), &|_| false, &|_| false, &|_| true).investigate.is_empty());
        t.nodes[0].status = NodeStatus::Ready;
        assert!(plan(&t, &standby.mesh, &me_of(&standby), &mut Said::default(), &|_| false, &|_| false, &|_| true).investigate.is_empty());
    }

    /// CONTRACT (node-investigation.md: "A node that does not answer a call is silent, whatever it
    /// gossips: a call whose attempts end unanswered marks its birth at the primary above it ..., and
    /// the primary investigates that birth as it investigates silence"; Caller: "the fabric-primary for
    /// a mesh-primary"; Q338): the fabric-primary investigates a peer mesh-primary its view holds ready
    /// (it gossips) once it holds a mark of that exact birth, once per mark, and a later mark again;
    /// without a mark nothing lists it. An admin that is not the primary above it lists it for no mark.
    /// Must NOT happen: a gossiping mesh-primary listed with no mark (Q182's clock is gone), or listed
    /// twice for one mark.
    #[test]
    fn the_fabric_primary_investigates_a_gossiping_mesh_primary_from_its_mark_and_from_nothing_else() {
        let (mut t, primary, standby, outsider) = world();
        t.nodes[0].status = NodeStatus::Ready;
        let mesh2_primary_marked = |n: &Node| n.node_id == outsider.node_id;
        let mut said = Said::default();
        let none = plan(&t, &primary.mesh, &me_of(&primary), &mut said, &|_| false, &|_| false, &|_| true);
        assert!(none.investigate.is_empty(), "a gossiping mesh-primary with no mark: nothing to investigate");
        let p = plan(&t, &primary.mesh, &me_of(&primary), &mut said, &|_| false, &mesh2_primary_marked, &|_| true);
        assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&outsider)], "gossiping, and silent from the mark");
        let again = plan(&t, &primary.mesh, &me_of(&primary), &mut said, &|_| false, &mesh2_primary_marked, &|_| true);
        assert!(again.investigate.is_empty(), "once per mark");
        let _ = plan(&t, &primary.mesh, &me_of(&primary), &mut said, &|_| false, &|_| false, &|_| true);
        let back = plan(&t, &primary.mesh, &me_of(&primary), &mut said, &|_| false, &mesh2_primary_marked, &|_| true);
        assert_eq!(back.investigate.len(), 1, "the mark ended; a later mark is silence again");
        let standby_look = plan(&t, &standby.mesh, &me_of(&standby), &mut Said::default(), &|_| false, &mesh2_primary_marked, &|_| true);
        assert!(standby_look.investigate.is_empty(), "a peer mesh-primary is the fabric-primary's to investigate");
    }

    /// CONTRACT (R-A2 items 4-5): an admin looks at a silent birth of the incumbent mesh once per
    /// mark from its own view, and a silent admin of a mesh that holds no role is
    /// nobody's business.
    #[test]
    fn a_silent_incumbent_birth_is_looked_at_once_per_mark_and_other_silence_is_not() {
        let (mut t, primary, standby, _) = world();
        let mut said = Said::default();
        let own = plan(&t, &standby.mesh, &me_of(&standby), &mut said, &|_| false, &|_| false, &|_| true);
        assert_eq!(own.investigate.iter().map(|n| n.location()).collect::<Vec<_>>(), vec![rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()]);
        assert!(plan(&t, &standby.mesh, &me_of(&standby), &mut said, &|_| false, &|_| false, &|_| true).investigate.is_empty(), "once per mark");
        // A silent admin of a third mesh that holds nothing.
        t.nodes.push(admin(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", "500000000000", "x1", NodeStatus::PendingReconnect));
        assert!(plan(&t, &standby.mesh, &me_of(&standby), &mut said, &|_| false, &|_| false, &|_| true).investigate.is_empty());
        let _ = primary;
    }

    /// CONTRACT: the silent former birth at this admin's path is not its own exact birth.
    /// Investigation uses identity rather than a path match even before the view lists the successor.
    #[test]
    fn an_admin_does_not_treat_a_former_birth_at_its_path_as_itself() {
        let (t, primary, _, _) = world();
        let successor = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "600000000000", "r2", NodeStatus::Ready);
        let p = plan(&t, &successor.mesh, &me_of(&successor), &mut Said::default(), &|_| false, &|_| false, &|_| true);
        assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&primary)]);
    }

    /// CONTRACT (node-recovery.md step 2: "the fabric-primary runs it, without the half-floor wait, only
    /// when the mesh's aggregate stays unheard past the staleness floor: no successor took the role"):
    /// the fabric-primary investigates a silent node-admin of a peer mesh that its primary view holds as
    /// that mesh's primary replaced by the election, though the view no longer names it primary, only
    /// while that mesh's aggregate is unheard; with the aggregate heard, the successor that publishes it
    /// investigates it. A silent node-admin of a peer mesh that was never its primary is that mesh's
    /// primary's to investigate.
    #[test]
    fn the_fabric_primary_investigates_a_peer_meshs_former_primary_only_while_its_meshs_aggregate_is_unheard() {
        let mut fp = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "300000000000", "h1", NodeStatus::Ready);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let former = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "100000000000", "o1", NodeStatus::PendingReconnect);
        let mut elected = admin(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", "200000000000", "o2", NodeStatus::Ready);
        elected.is_primary = true;
        let member = admin(rdm_core::PathName::new("mesh2.node-admin", "3").unwrap(), "mesh2", "node-admin", "500000000000", "o3", NodeStatus::PendingReconnect);
        let t = view(vec![fp.clone(), former.clone(), elected, member]);
        let replaced = |n: &Node| n.node_id == former.node_id;
        let p = plan(&t, &fp.mesh, &me_of(&fp), &mut Said::default(), &replaced, &|_| false, &|_| true);
        assert_eq!(p.investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&former)], "the replaced mesh-primary only");
        assert!(plan(&t, &fp.mesh, &me_of(&fp), &mut Said::default(), &replaced, &|_| false, &|_| false).investigate.is_empty(), "mesh2's aggregate heard: its successor took the role, and investigates it");
        assert!(plan(&t, &fp.mesh, &me_of(&fp), &mut Said::default(), &|_| false, &|_| false, &|_| true).investigate.is_empty(), "no silent primary of a peer mesh");
    }

    fn digest_of(n: &Node) -> rdm_core::MeshDigest {
        use crate::model::FabricId;
        rdm_core::MeshDigest {
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(), node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: crate::model::EndpointId(String::new()), transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)), incarnation: n.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::model::NodeStatus::Ready,
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

    /// The fabric-primary mesh1.node-admin.1 and mesh2's primary mesh2.node-admin.1, unheard past the floor.
    fn unheard_peer_primary() -> (Node, Node, Topology) {
        let mut fp = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "300000000000", "h1", NodeStatus::Ready);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let mut peer = admin(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "100000000000", "o1", NodeStatus::PendingReconnect);
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
        let (topology, digest_view) = (tokio::sync::RwLock::new(t), rdm_gossip::digest_view::DigestView::default());
        let since = std::time::Instant::now();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let (calls_ref, digest_view_ref, heard) = (&calls, &digest_view, digest_of(&peer));
        let submit = || {
            let heard = heard.clone();
            async move {
                // Refused: a Build in progress holds the mesh. The birth answers its tickle and rejoins
                // after the refusal: its digest arrives.
                if calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    assert!(digest_view_ref.record(heard), "the view takes the birth's digest");
                }
                Err("build-in-progress: BuildInProgress { mesh: Some(\"mesh2\") }".to_string())
            }
        };
        let ended = tokio::time::timeout(rdm_core::cadence::staleness_floor() * 20, submit_recovery(&peer, submit, &topology, &digest_view, &fp.node_id, since)).await;
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
        let (topology, digest_view) = (tokio::sync::RwLock::new(t), rdm_gossip::digest_view::DigestView::default());
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
        let ended = tokio::time::timeout(rdm_core::cadence::staleness_floor() * 20, submit_recovery(&peer, submit, &topology, &digest_view, &fp.node_id, std::time::Instant::now())).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1, "a birth the view holds stopping is never submitted again");
        assert!(ended.is_ok(), "the owed submit ends with the mark");
    }

    /// CONTRACT (node-recovery.md step 3: "Any other verdict ...: the recovery below, at once"): a
    /// birth still unheard keeps its refused recovery owed until a submit opens it.
    #[tokio::test(start_paused = true)]
    async fn a_refused_recovery_of_a_birth_still_unheard_is_submitted_until_opened() {
        let (fp, peer, t) = unheard_peer_primary();
        let (topology, digest_view) = (tokio::sync::RwLock::new(t), rdm_gossip::digest_view::DigestView::default());
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let calls_ref = &calls;
        let submit = || async move {
            match calls_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 | 1 => Err("build-in-progress".to_string()),
                _ => Ok(()),
            }
        };
        tokio::time::timeout(rdm_core::cadence::staleness_floor() * 20, submit_recovery(&peer, submit, &topology, &digest_view, &fp.node_id, std::time::Instant::now())).await.expect("the submit that opens the recovery ends the loop");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3, "submitted until a submit opens it");
    }

    /// CONTRACT (node-recovery.md step 2: "Only the mesh-primary acts. ... For a lost mesh-primary the
    /// successor its election names runs it as for any member; the fabric-primary runs it ... only when
    /// ... no successor took the role"): the fabric-primary opens no investigation of a silent secondary
    /// node-admin of its own mesh (its mesh-primary's tick looks at it); an admin that is not the
    /// fabric-primary still looks at it; a fabric-primary that is not its mesh's primary looks at that
    /// mesh-primary when it is unheard, and never at a replaced one, its successor's (the successor is
    /// Ready in the fabric-primary's own mesh, the fabric-primary itself at least).
    #[test]
    fn the_fabric_primary_leaves_a_silent_secondary_admin_of_its_own_mesh_to_its_mesh_primary() {
        let mut fp = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "300000000000", "h1", NodeStatus::Ready);
        fp.is_primary = true;
        fp.is_fabric_primary = true;
        let secondary = admin(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", "400000000000", "s1", NodeStatus::PendingReconnect);
        let other = admin(rdm_core::PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", "500000000000", "s3", NodeStatus::Ready);
        let t = view(vec![fp.clone(), secondary.clone(), other.clone()]);
        assert!(plan(&t, &fp.mesh, &me_of(&fp), &mut Said::default(), &|_| false, &|_| false, &|_| true).investigate.is_empty(), "the fabric-primary opens no investigation of a secondary node-admin of its own mesh");
        assert_eq!(plan(&t, &other.mesh, &me_of(&other), &mut Said::default(), &|_| false, &|_| false, &|_| true).investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&secondary)], "an admin that is not the fabric-primary still looks");
        let mut fp_only = admin(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "300000000000", "h1", NodeStatus::Ready);
        fp_only.is_fabric_primary = true;
        let mut primary = admin(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", "400000000000", "s1", NodeStatus::PendingReconnect);
        primary.is_primary = true;
        let t = view(vec![fp_only.clone(), primary.clone(), other.clone()]);
        assert_eq!(plan(&t, &fp_only.mesh, &me_of(&fp_only), &mut Said::default(), &|_| false, &|_| false, &|_| true).investigate.iter().map(me_of).collect::<Vec<_>>(), vec![me_of(&primary)], "an unheard mesh-primary of its own mesh is the fabric-primary's");
        // The election replaced it: the view names another mesh-primary, the primary view the replaced birth.
        let former = Node { is_primary: false, ..primary.clone() };
        let elected = Node { is_primary: true, ..other };
        let never = admin(rdm_core::PathName::new("mesh1.node-admin", "4").unwrap(), "mesh1", "node-admin", "600000000000", "s4", NodeStatus::PendingReconnect);
        let t = view(vec![fp_only.clone(), former.clone(), elected, never]);
        let replaced = |n: &Node| n.node_id == former.node_id;
        assert!(plan(&t, &fp_only.mesh, &me_of(&fp_only), &mut Said::default(), &replaced, &|_| false, &|_| true).investigate.is_empty(), "a replaced mesh-primary of its own mesh is its successor's, as any member (Q335): the fabric-primary's watch looks at neither it nor a secondary node-admin");
    }
}
