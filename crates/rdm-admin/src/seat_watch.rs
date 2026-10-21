//! A silent seat holder is looked at, never replaced for being silent (ruling R-A2).
//!
//! The fabric primary stays in its mesh until that mesh is proven unable to hold it, and silence
//! proves nothing. Each admin uses its own view's silence evidence to investigate: an admin of
//! the holder's mesh (a standby included), and any admin looking at the fabric holder's mesh
//! from outside it, looks at the exact silent birth: a direct ping, the status kick when it answers, the same kick through up to two
//! live members of its mesh at once, and last the provider's inspection of the birth's exact
//! runtime (node-recovery.md step 2, ruling R-SW1). An answer from the exact birth (`Current`,
//! `CurrentParked`, `Blocked`), or a runtime that still runs, ends it: the seat stays. Only a
//! runtime found exited is a verified loss, its exit-proof row put (`Records::put_exit`); a failed ping, `NotSent`,
//! a deadline, silence and the observer-inferred `Dead` are not.
//!
//! [`plan`] decides what a pass owes, from the view and births already inspected; [`run`] drives it.

use crate::admin::Records;
use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::SeatHolder;
use rdm_transport::membership::Membership;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
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

/// The seat `me` writes a Build lane under (Q28: "`N` is the committed fabric seat revision the
/// writer holds"), from the lane's seat `record` (`which`, in words): the record's revision (0 when
/// none is held); held when the record names `me`, none, or a birth proven gone (`lost`), and not
/// held, naming the record, when it names another live birth.
pub fn lane_seat(which: &str, record: Option<&SeatHolder>, me: &Birth, lost: &HashSet<IncarnationId>) -> crate::build_state::LaneSeat {
    let holds = match record {
        Some(h) if !h.is_birth(&me.0, &me.1) && !lost.contains(&h.incarnation) => Err(format!("{which} names {} {} (birth {}) at revision {}", h.mesh, h.node_id, h.incarnation.as_str(), h.revision)),
        _ => Ok(()),
    };
    crate::build_state::LaneSeat { revision: record.map(|h| h.revision).unwrap_or(0), holds }
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

/// What the look found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// The birth answered a direct ping.
    Answered,
    /// The birth answered the same kick through a live member of its mesh.
    AnsweredViaPeer {
        /// The mesh member that carried the kick.
        carrier: String,
    },
    /// The provider inspected its exact runtime and it still runs.
    Running,
    /// The provider inspected its exact runtime and the operating system holds it suspended:
    /// neither hung nor gone, so no hold opens and nothing is terminated (deployment-provider.md §2.5).
    OsStopped,
    /// The provider inspected its exact runtime and it exited: a verified loss.
    Exited {
        /// The exit code, when one is provable.
        code: Option<i32>,
    },
    /// Nothing proves the birth gone or alive: named by what was missing.
    Unproven {
        /// What was missing.
        reason: String,
    },
}

impl Finding {
    /// The finding's name as it appears in spans.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::AnsweredViaPeer { .. } => "answered-via-peer",
            Self::Running => "running",
            Self::OsStopped => "os-stopped",
            Self::Exited { .. } => "exit-proven",
            Self::Unproven { .. } => "unproven",
        }
    }

    /// What the finding names beyond its name: the carrier, the exit code or what was missing.
    pub fn detail(&self) -> String {
        match self {
            Self::AnsweredViaPeer { carrier } => carrier.clone(),
            Self::Exited { code } => code.map(|c| c.to_string()).unwrap_or_else(|| "no code provable".into()),
            Self::Unproven { reason } => reason.clone(),
            _ => String::new(),
        }
    }
}

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
}

impl Looker {
    /// Look at `n`'s exact birth: the ladder ends at the first rung that settles it
    /// (node-recovery.md step 2, run by the seat watch: fabric-node-lifecycle-elections.md §4.1).
    pub async fn look(&self, view: &Topology, n: &Node) -> Finding {
        let target = NodeTarget::ExactNode(n.node_id.clone());
        let Some(incarnation) = n.incarnation_id.clone() else {
            return Finding::Unproven { reason: format!("{} ({}) holds no recorded birth: there is no exact birth to look at", n.name, n.node_id) };
        };
        // Every probe names the same exact birth: one kick, by node id and incarnation.
        let kick = rdm_core::status::StatusRequest::ProbeNodeState { node_id: n.node_id.clone(), incarnation: incarnation.clone() };
        // 1. A direct ping, one QUIC connect (node-recovery.md step 2): asked once, never resent; when
        // it answers, the status kick makes the birth say it is there.
        let once = CallOptions { once: true, ..CallOptions::default() };
        let direct = async {
            let ping = rdm_core::ping::PingRequest::Ping { payload: b"seat-watch".to_vec() };
            let (out, _) = self.client.call::<rdm_core::ping::Ping>(&target, &ping, &once).await;
            if out.reply().is_none() {
                return false;
            }
            let birth = rdm_client::ExactBirth { target: target.clone(), node_id: n.node_id.clone(), incarnation: incarnation.clone() };
            // A seat holder this admin holds off its mesh that answers Current is commanded back
            // through node.mesh.join (node-tickle.md "What follows", Q87) by the fabric-primary, the
            // authority above a mesh-primary: every admin looks, one commands (commands go down).
            let commands = view.own(&self.me_id).is_some_and(|me| me.is_fabric_primary);
            if let Ok(reply) = rdm_client::NodeRpc::new(&self.client).get(&birth, &once).await {
                if let (true, true, Some(heard_again)) = (commands, matches!(reply, rdm_core::status::StatusReply::Current { .. }), self.heard_again.get()) {
                    heard_again.command(n, &incarnation).await;
                }
            }
            true
        };
        // 2. The same kick through up to two live members of the birth's mesh, at once.
        let kick = &kick;
        let peers: Vec<_> = view
            .members()
            .filter(|c| c.mesh == n.mesh && c.node_id != n.node_id && c.node_id != self.me_id && c.status == NodeStatus::ReadyForTraffic)
            .take(crate::offline::VIA_PEER_TICKLE_FANOUT)
            .map(|c| {
                let route = rdm_transport::RouteChoice::ViaPeer { carrier: c.node_id.clone(), path: c.name.clone() };
                let probe = async move {
                    let (out, _, _) = self.client.call_routed::<rdm_core::status::Status>(&n.node_id, &route, kick, &CallOptions::default()).await;
                    out.reply().map(|r| r.value().clone())
                };
                (c.name.to_string(), probe)
            })
            .collect();
        // 3. The provider's inspection of the exact runtime: the only rung that proves a loss.
        let provider = self.inspect(n);
        ladder(&n.node_id, &incarnation, direct, peers, provider).await
    }

    /// The provider's inspection of `n`'s exact runtime.
    async fn inspect(&self, n: &Node) -> Finding {
        inspect_birth(&*self.provider, &self.membership.book, self.nodes.as_deref(), n).await
    }
}

/// The provider's inspection of the exact runtime of `n`'s birth (node-recovery.md step 3: "the
/// exact runtime recorded at launch"): the runtime fact its digest carries in `book`, or else the
/// runtime row `nodes` holds for the exact birth (recorded at its launch, or handed into this
/// admin's nodes.storage at its own create). A birth lost before this admin heard it has no digest
/// here; its row is still the runtime recorded at launch. Neither held: unproven, naming both.
pub async fn inspect_birth(provider: &dyn crate::deployment::provider::DeploymentProvider, book: &rdm_transport::membership::DigestBook, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> Finding {
    let Some(inc) = n.incarnation_id.clone() else { return Finding::Unproven { reason: "the view holds no incarnation for it".into() } };
    let held = book.get(n.node_id.as_str()).filter(|(d, _)| d.node.incarnation == inc);
    let (fact, data_dir) = match held {
        Some((digest, _)) => match digest.node.runtime.clone() {
            Some(fact) => (fact, digest.data_dir.clone()),
            None => return Finding::Unproven { reason: "its digest carries no runtime fact".into() },
        },
        None => {
            let row = match nodes {
                Some(nodes) => match nodes.runtime(&n.node_id, &inc).await {
                    Ok(row) => row,
                    Err(e) => return Finding::Unproven { reason: format!("this admin holds no digest of this exact birth, and its nodes.storage runtime row could not be read: {e}") },
                },
                None => None,
            };
            match row {
                Some(r) => (r.runtime, r.data_dir),
                None => return Finding::Unproven { reason: "this admin holds no digest of this exact birth and no runtime row of it, so no runtime fact".into() },
            }
        }
    };
    let handle = match crate::deployment::provider::adopt(provider, &fact) {
        Ok(h) => h,
        Err(e) => return Finding::Unproven { reason: format!("the provider refused to adopt its runtime: {e}") },
    };
    let inspected = provider.inspect(&handle).await;
    match crate::deployment::provider::exit_proof(inspected, &fact, data_dir.as_deref().map(std::path::Path::new), &inc.as_str()).await {
        Ok(crate::deployment::provider::DeploymentStatus::Exited { code }) => Finding::Exited { code },
        Ok(crate::deployment::provider::DeploymentStatus::Running) => Finding::Running,
        Ok(crate::deployment::provider::DeploymentStatus::OsStopped) => Finding::OsStopped,
        Ok(crate::deployment::provider::DeploymentStatus::Unknown) => Finding::Unproven { reason: "its runtime is not proven exited: Unknown".into() },
        Err(e) => Finding::Unproven { reason: format!("its exit record cannot be read: {e}") },
    }
}

/// The birth's own answer on the kick (node-recovery.md step 2): `Current`, `CurrentParked` while it
/// is parked, or `Blocked` while its hook is Blocked, naming the exact birth probed. A carrier's
/// carrier-edge-lost (no reply reaches the ladder), a `NotReady` and any refusal are not answers.
pub fn birth_answered(reply: Option<&rdm_core::status::StatusReply>, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
    use rdm_core::status::StatusReply;
    match reply {
        Some(StatusReply::Current { node_id: n, incarnation: i, .. } | StatusReply::CurrentParked { node_id: n, incarnation: i } | StatusReply::Blocked { node_id: n, incarnation: i, .. }) => n == node_id && i == incarnation,
        _ => false,
    }
}

/// The ladder over one exact birth (ruling R-SW1): the direct probe; when it fails, every via-peer
/// kick at once, where the first answer from the exact birth ends the investigation and drops the
/// probes still running, and a failed probe cancels none of the others; the provider only once
/// every peer probe ended unanswered. A failure, a deadline included, proves nothing.
async fn ladder<D, P, V>(node_id: &NodeId, incarnation: &IncarnationId, direct: D, peers: Vec<(String, P)>, provider: V) -> Finding
where
    D: std::future::Future<Output = bool>,
    P: std::future::Future<Output = Option<rdm_core::status::StatusReply>>,
    V: std::future::Future<Output = Finding>,
{
    if direct.await {
        return Finding::Answered;
    }
    match crate::offline::via_peer_verdict(node_id, incarnation, peers).await {
        crate::offline::ViaPeerVerdict::Answered { via } => Finding::AnsweredViaPeer { carrier: via },
        _ => provider.await,
    }
}

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
                Finding::Answered | Finding::AnsweredViaPeer { .. } => {
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
            let proven = if let Finding::Exited { code } = &finding {
                // The verified loss is recorded durably: the holder's exit-proof row is what every
                // projection reads as its proven loss.
                let recorded = w.records.put_exit(&node, *code, "provider").await;
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
        if let Some(lost) = takeover_due(&view, &me_birth, w.membership.seats().fabric(), &said.unreachable, std::time::Duration::from_millis(crate::admin::hung_hold_ms())).filter(|_| w.membership.authorizes()) {
            if let Some(door) = w.handover.get().cloned() {
                drop(turn.take());
                let _ = door.propose_take_over(&lost).await;
                turn = Some(w.gate.turn("seat-watch").await);
            }
        }
    }
}

/// The fabric seat's record `record` when this admin, its own mesh's primary and not the fabric
/// seat's holder, holds every node-admin of the record's mesh in its view unreached and
/// uninspectable past `hold` (`unreachable`, from each birth's first such look), the record's own
/// birth among them (elections §5: "For a fabric-primary that is unreached by the tickle ladder and
/// whose exact runtime no surviving admin can inspect ... the hold runs, and when it expires the seat
/// moves"). `None` otherwise: a mesh with any node-admin answering, running, stopped or proven gone
/// keeps the seat by the ordinary rule.
pub fn takeover_due(view: &Topology, me: &Birth, record: Option<rdm_core::SeatHolder>, unreachable: &std::collections::HashMap<Birth, std::time::Instant>, hold: std::time::Duration) -> Option<rdm_core::SeatHolder> {
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
    if !view.holds_mesh_seat(&mine.mesh, &me.0) {
        return None;
    }
    let expired = |b: &Birth| unreachable.get(b).is_some_and(|since| since.elapsed() >= hold);
    if !expired(&(record.node_id.clone(), record.incarnation.clone())) {
        return None;
    }
    let admins: Vec<&Node> = view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.mesh == record.mesh).collect();
    admins.iter().all(|n| birth(n).is_some_and(|b| expired(&b))).then_some(record)
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

    /// CONTRACT (elections §5: "For a fabric-primary that is unreached by the tickle ladder and whose
    /// exact runtime no surviving admin can inspect ... the hold runs, and when it expires the seat
    /// moves by §4.3's backbone rule"; "Every mesh-primary that notices tells the winner"): mesh2's
    /// primary proposes the takeover of mesh1's fabric seat only once every node-admin of mesh1 it
    /// holds, the record's birth among them, was found unreached and uninspectable at least the hold
    /// ago. Not while one of them is held for less than the hold or not held at all (it answered, runs,
    /// is stopped or proven gone: the ordinary rule), not at mesh1's own standby (the seat stays in its
    /// mesh), and not at the record's holder.
    #[test]
    fn a_mesh_primary_proposes_the_takeover_only_once_the_fabric_primarys_whole_mesh_is_held_past_the_hold() {
        let (mut t, holder, mut standby, outsider) = world();
        standby.status = NodeStatus::PendingReconnect;
        t.nodes[1].status = NodeStatus::PendingReconnect;
        let record = SeatHolder { mesh: holder.mesh.clone(), node_id: holder.node_id.clone(), incarnation: holder.incarnation_id.clone().unwrap(), revision: 7 };
        let hold = std::time::Duration::from_millis(50);
        let past = std::time::Instant::now() - std::time::Duration::from_millis(100);
        let held = |entries: &[(&Node, std::time::Instant)]| entries.iter().map(|(n, at)| (me_of(n), *at)).collect::<std::collections::HashMap<_, _>>();
        let now = std::time::Instant::now();
        assert_eq!(takeover_due(&t, &me_of(&outsider), Some(record.clone()), &held(&[(&holder, past), (&standby, past)]), hold), Some(record.clone()), "every node-admin of mesh1 held past the hold");
        assert_eq!(takeover_due(&t, &me_of(&outsider), Some(record.clone()), &held(&[(&holder, past), (&standby, now)]), hold), None, "the standby is held for less than the hold");
        assert_eq!(takeover_due(&t, &me_of(&outsider), Some(record.clone()), &held(&[(&holder, past)]), hold), None, "the standby is not held: it answered, runs, is stopped or proven gone");
        assert_eq!(takeover_due(&t, &me_of(&standby), Some(record.clone()), &held(&[(&holder, past), (&standby, past)]), hold), None, "mesh1's own admin never proposes moving the seat out of its mesh");
        assert_eq!(takeover_due(&t, &me_of(&holder), Some(record.clone()), &held(&[(&holder, past), (&standby, past)]), hold), None, "the holder never proposes its own takeover");
        assert_eq!(takeover_due(&t, &me_of(&outsider), None, &held(&[(&holder, past), (&standby, past)]), hold), None, "no record, no takeover");
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

    mod ladder {
        //! The ladder's rungs and completion rules (ruling R-SW1; node-recovery.md step 2), under
        //! paused time: a probe's wait is its budget, and the clock moves only when every probe waits.
        use super::super::*;
        use rdm_core::status::{NodeState, RetryOn, StatusReply};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The default call budget each probe spends against a process that never answers.
        const BUDGET: Duration = Duration::from_secs(10);

        fn birth() -> (NodeId, IncarnationId) {
            (NodeId::mint(), IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-the-holder")).unwrap())
        }

        fn current(b: &(NodeId, IncarnationId)) -> StatusReply {
            StatusReply::Current { node_id: b.0.clone(), incarnation: b.1.clone(), state: NodeState::ReadyForTraffic }
        }

        fn parked(b: &(NodeId, IncarnationId)) -> StatusReply {
            StatusReply::CurrentParked { node_id: b.0.clone(), incarnation: b.1.clone() }
        }

        /// A probe that spends `after` and ends with `reply` (`None`: no answer from the birth,
        /// a deadline or a carrier's refusal).
        async fn probe(after: Duration, reply: Option<StatusReply>) -> Option<StatusReply> {
            sleep(after).await;
            reply
        }

        async fn provider_says(f: Finding, asked: &AtomicBool) -> Finding {
            asked.store(true, Ordering::SeqCst);
            f
        }

        /// CONTRACT (R-SW1 acceptance 1): after the direct probe failed, both peer probes run at
        /// once, so a ladder whose two peers each spend a whole budget costs one budget on that
        /// rung, not two. Both failing ends at the provider, which is asked once.
        #[tokio::test(start_paused = true)]
        async fn both_peer_probes_spend_their_budgets_at_once() {
            let b = birth();
            let asked = AtomicBool::new(false);
            let start = Instant::now();
            let peers = vec![("mesh1.rpc.1".to_string(), probe(BUDGET, None)), ("mesh1.rpc.2".to_string(), probe(BUDGET, None))];
            let found = ladder(&b.0, &b.1, async { false }, peers, provider_says(Finding::Running, &asked)).await;
            assert_eq!(start.elapsed(), BUDGET, "the two peer probes overlap: one budget, not two");
            assert_eq!(found, Finding::Running);
            assert!(asked.load(Ordering::SeqCst), "the provider is asked once both peers ended unanswered");
        }

        /// CONTRACT (R-SW1 acceptance 2, completion rule): one failed peer probe does not cancel the
        /// other. The first peer fails at 1 s; the second answers for the exact birth at 3 s and
        /// ends the ladder there, naming that carrier; the provider is never asked.
        #[tokio::test(start_paused = true)]
        async fn the_second_peer_answers_after_the_first_fails() {
            let b = birth();
            let asked = AtomicBool::new(false);
            let start = Instant::now();
            let peers = vec![("mesh1.rpc.1".to_string(), probe(Duration::from_secs(1), None)), ("mesh1.rpc.2".to_string(), probe(Duration::from_secs(3), Some(current(&b))))];
            let found = ladder(&b.0, &b.1, async { false }, peers, provider_says(Finding::Running, &asked)).await;
            assert_eq!(found, Finding::AnsweredViaPeer { carrier: "mesh1.rpc.2".into() });
            assert_eq!(start.elapsed(), Duration::from_secs(3), "the second peer's answer, not the first's failure, ends the rung");
            assert!(!asked.load(Ordering::SeqCst), "an answer settles it: no provider inspection");
        }

        /// CONTRACT (R-SW1 completion rule): the first answer from the exact birth ends the
        /// investigation and drops the probe still running; that probe never completes.
        #[tokio::test(start_paused = true)]
        async fn an_answer_drops_the_probe_still_running() {
            let b = birth();
            let finished = Arc::new(AtomicBool::new(false));
            let slow = {
                let finished = finished.clone();
                async move {
                    sleep(BUDGET).await;
                    finished.store(true, Ordering::SeqCst);
                    None
                }
            };
            let fast = async { probe(Duration::from_secs(1), Some(current(&b))).await };
            let start = Instant::now();
            let peers: Vec<(String, std::pin::Pin<Box<dyn std::future::Future<Output = Option<StatusReply>> + Send + '_>>)> = vec![("mesh1.rpc.1".into(), Box::pin(slow)), ("mesh1.rpc.2".into(), Box::pin(fast))];
            let found = ladder(&b.0, &b.1, async { false }, peers, async { Finding::Running }).await;
            assert_eq!(found, Finding::AnsweredViaPeer { carrier: "mesh1.rpc.2".into() });
            assert_eq!(start.elapsed(), Duration::from_secs(1));
            sleep(BUDGET * 2).await;
            assert!(!finished.load(Ordering::SeqCst), "the probe still running was dropped with the investigation");
        }

        /// CONTRACT (R-SW1 acceptance 3): a stopped holder is parked, its process alive; its
        /// `CurrentParked` through a peer is the birth's answer and ends the ladder. A carrier's
        /// refusal before it (carrier-edge-lost reaches the ladder as no reply) is not an answer.
        #[tokio::test(start_paused = true)]
        async fn a_stopped_holder_answering_current_parked_through_a_peer_ends_the_ladder() {
            let b = birth();
            let asked = AtomicBool::new(false);
            let peers = vec![("mesh1.rpc.1".to_string(), probe(Duration::from_millis(5), None)), ("mesh1.rpc.2".to_string(), probe(Duration::from_millis(20), Some(parked(&b))))];
            let found = ladder(&b.0, &b.1, async { false }, peers, provider_says(Finding::Running, &asked)).await;
            assert_eq!(found, Finding::AnsweredViaPeer { carrier: "mesh1.rpc.2".into() });
            assert!(!asked.load(Ordering::SeqCst));
        }

        /// CONTRACT (R-SW1 item 3): an answer is `Current`, `CurrentParked` or `Blocked` from the
        /// exact birth probed. A NotReady (a carrier's or anyone's), another incarnation's reply,
        /// a refusal and no reply at all are not answers.
        #[test]
        fn only_the_exact_births_state_is_an_answer() {
            let b = birth();
            let blocked = StatusReply::Blocked { node_id: b.0.clone(), incarnation: b.1.clone(), reason: "rows not yet published".into(), retry_on: RetryOn::TargetReachable { node_id: NodeId::mint() } };
            for r in [current(&b), parked(&b), blocked] {
                assert!(birth_answered(Some(&r), &b.0, &b.1), "{} is the birth's answer", r.name());
            }
            let other = (b.0.clone(), IncarnationId::parse(&rdm_core::fixture_crockford60("an-older-birth")).unwrap());
            for r in [StatusReply::NotReady { reason: "carrier not ready".into() }, current(&other), parked(&other), StatusReply::RejectedStaleIncarnation { held: other.1.clone() }] {
                assert!(!birth_answered(Some(&r), &b.0, &b.1), "{} is no answer from the exact birth", r.name());
            }
            assert!(!birth_answered(None, &b.0, &b.1), "no reply (a deadline, carrier-edge-lost) is no answer");
        }

        /// CONTRACT (R-SW1 item 2; elections §4.1): no probe outcome proves a loss. With the direct
        /// probe and both peers failed, deadlines included, the finding is whatever the provider
        /// proves: `Running` or `Unproven` leave the seat, only its `Exited` is a verified loss.
        #[tokio::test(start_paused = true)]
        async fn no_probe_outcome_proves_a_loss_without_the_provider() {
            let b = birth();
            for (said, expect_exit) in [(Finding::Running, false), (Finding::Unproven { reason: "its runtime is not proven exited".into() }, false), (Finding::Exited { code: Some(137) }, true)] {
                let asked = AtomicBool::new(false);
                let peers = vec![("mesh1.rpc.1".to_string(), probe(BUDGET, None)), ("mesh1.rpc.2".to_string(), probe(BUDGET, Some(StatusReply::NotReady { reason: "carrier not ready".into() })))];
                let found = ladder(&b.0, &b.1, async { false }, peers, provider_says(said.clone(), &asked)).await;
                assert!(asked.load(Ordering::SeqCst), "the provider check is mandatory once no probe was answered");
                assert_eq!(found, said);
                assert_eq!(matches!(found, Finding::Exited { .. }), expect_exit, "only the provider's exit proof is a loss");
            }
        }
    }
}
