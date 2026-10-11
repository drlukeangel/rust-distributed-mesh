//! A silent seat holder is looked at, never replaced for being silent (ruling R-A2).
//!
//! The fabric primary stays in its mesh until that mesh is proven unable to hold it, and silence
//! proves nothing. So silence produces two things and neither moves a seat:
//!
//! - a **Concern**: when a mesh primary marks the fabric primary's exact birth `PendingReconnect`
//!   (the staleness floor already marks it; there is no new timeout) it says so on the backbone,
//!   once per mark. The named birth, if it hears it, re-feeds its neighbours and says its seats
//!   again. A Concern is a warning, never permission to take the seat.
//! - an **investigation**: an admin of the holder's mesh (a standby included, it need not hold any
//!   seat), and any admin looking at the fabric holder's mesh from outside it, looks at the exact
//!   silent birth: a direct ping, the status kick when it answers, the same kick through up to two
//!   live members of its mesh at once, and last the provider's inspection of the birth's exact
//!   runtime (node-recovery.md step 2, ruling R-SW1). An answer from the exact birth (`Current`,
//!   `CurrentParked`, `Blocked`), or a runtime that still runs, ends it: the seat stays. Only a
//!   runtime found exited is a verified loss (`Records::mark_exited`); a failed ping, `NotSent`,
//!   a deadline, silence and the observer-inferred `Dead` are not.
//!
//! [`plan`] decides what a pass owes, from the view and what was already said; [`run`] drives it.

use crate::admin::Records;
use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::{Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, ConcernHeard, Membership};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::Instrument as _;

type Birth = (NodeId, IncarnationId);

/// What one pass over the view owes.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Concerns to publish: the fabric primary's exact birth, newly marked silent.
    pub concerns: Vec<Birth>,
    /// Births to look at, each with why.
    pub investigate: Vec<(Node, Trigger)>,
    /// A Concern named this admin's own birth: re-feed the neighbours and say the seats again.
    pub answer: bool,
}

/// Why a birth is looked at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// A Concern named it.
    Concern,
    /// This admin's own view marked it silent.
    Silence,
}

impl Trigger {
    /// The trigger's name as it appears in spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::Concern => "concern",
            Self::Silence => "silence",
        }
    }
}

/// What this admin already said and looked at, by exact birth: a mark is spoken of once, and
/// forgotten when the birth is heard again.
#[derive(Debug, Default)]
pub struct Said {
    concerned: HashSet<Birth>,
    investigated: HashSet<Birth>,
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

/// What the committed fabric seat record says of `me`'s Build authority (node-rpc-envelope.md
/// TakeSeats: "The row is the fence: P's execution authority ends at the row's epoch ... and no
/// separate fence write precedes or follows it"). A record naming this birth, naming none, or naming
/// a birth proven gone (`lost`: a vacancy the election fills before this birth's own record lands)
/// ends nothing; a record naming another live birth ended this birth's authority at its epoch, and
/// names that birth's path.name from `name_of` (the digest book, which holds a birth before the
/// projected view does). Read from the record alone: no history, no write.
pub fn fabric_seat_record(record: Option<&SeatHolder>, me: &Birth, lost: &HashSet<IncarnationId>, name_of: impl Fn(&NodeId) -> Option<String>) -> crate::fabric_builds::SeatRecord {
    match record {
        Some(h) if !h.is_birth(&me.0, &me.1) && !lost.contains(&h.incarnation) => {
            let to = name_of(&h.node_id);
            let by = format!("the fabric seat record names {} {} (birth {}) at epoch {}{}", h.mesh, h.node_id, h.incarnation.0, h.epoch, if to.is_none() { "; no digest of that birth is held, so its path.name is not known here" } else { "" });
            crate::fabric_builds::SeatRecord::Moved { by, to }
        }
        _ => crate::fabric_builds::SeatRecord::Open,
    }
}

/// The pass: `me` is this admin's exact birth, `heard` the Concerns drained since the last pass.
pub fn plan(view: &Topology, me: &PathName, me_birth: &Birth, heard: &[ConcernHeard], said: &mut Said) -> Plan {
    let silent: HashSet<Birth> = view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::PendingReconnect).filter_map(birth).collect();
    said.concerned.retain(|b| silent.contains(b));
    said.investigated.retain(|b| silent.contains(b));
    let mut plan = Plan::default();
    let mesh = &me.mesh;

    // A mesh primary says the fabric primary's exact birth is silent, once per mark.
    if view.holds_mesh_seat(mesh, &me_birth.0) {
        for n in view.members().filter(|n| n.is_fabric_primary && n.status == NodeStatus::PendingReconnect) {
            if let Some(b) = birth(n) {
                if said.concerned.insert(b.clone()) {
                    plan.concerns.push(b);
                }
            }
        }
    }

    // What others said: the named birth looks to its neighbours when it is this admin; an admin of
    // its mesh looks at it.
    for c in heard.iter().filter(|c| c.seat == Seat::FabricPrimary) {
        let Some(n) = view.births().find(|n| n.node_id == c.node_id && n.incarnation_id.as_ref() == Some(&c.incarnation)) else { continue };
        if &c.node_id == &me_birth.0 && c.incarnation == me_birth.1 {
            plan.answer = true;
        } else if &n.mesh == mesh && n.kind == NodeKind::NodeAdmin {
            plan.investigate.push((n.clone(), Trigger::Concern));
        }
    }

    // What this admin sees itself: a silent seat holder of its own mesh, and any silent node-admin
    // of the mesh that holds the fabric seat (a standby, or a candidate looking from outside).
    let fabric_mesh = view.members().find(|n| n.is_fabric_primary).map(|n| n.mesh.clone());
    for n in view.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::PendingReconnect && n.node_id != me_birth.0) {
        let own_holder = &n.mesh == mesh && (n.is_primary || n.is_fabric_primary);
        let incumbent = fabric_mesh.as_ref() == Some(&n.mesh);
        if !(own_holder || incumbent) {
            continue;
        }
        let Some(b) = birth(n) else { continue };
        // A birth a Concern already sends us to is looked at once for both.
        let marked = said.investigated.insert(b.clone());
        if marked && !plan.investigate.iter().any(|(p, _)| birth(p).as_ref() == Some(&b)) {
            plan.investigate.push((n.clone(), Trigger::Silence));
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
            Self::Exited { .. } => "exit-proven",
            Self::Unproven { .. } => "unproven",
        }
    }

    fn detail(&self) -> String {
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
}

impl Looker {
    /// Look at `n`'s exact birth: the ladder ends at the first rung that settles it
    /// (node-recovery.md step 2, run by the seat watch: fabric-node-lifecycle-elections.md §4.1).
    pub async fn look(&self, view: &Topology, n: &Node) -> Finding {
        let target = NodeTarget::ExactNode(n.node_id.clone());
        let incarnation = n.incarnation_id.clone().unwrap_or_else(|| IncarnationId(String::new()));
        // Every probe names the same exact birth: one kick, by node id and incarnation.
        let kick = rafka_node_rpc_contract::status::StatusRequest::ProbeNodeState { node_id: n.node_id.clone(), incarnation: incarnation.clone() };
        // 1. A direct ping; when it answers, the status kick makes the birth say it is there.
        let direct = async {
            let ping = rafka_node_rpc_contract::ping::PingRequest::Ping { payload: b"seat-watch".to_vec() };
            let (out, _) = self.client.call::<rafka_node_rpc_contract::ping::Ping>(&target, &ping, &CallOptions::default()).await;
            if out.reply().is_none() {
                return false;
            }
            let birth = rafka_node_admin_client::ExactBirth { target: target.clone(), node_id: n.node_id.clone(), incarnation: incarnation.clone() };
            let _ = rafka_node_admin_client::NodeRpc::new(&self.client).get(&birth, &CallOptions::default()).await;
            true
        };
        // 2. The same kick through up to two live members of the birth's mesh, at once.
        let kick = &kick;
        let peers: Vec<_> = view
            .members()
            .filter(|c| c.mesh == n.mesh && c.node_id != n.node_id && c.node_id != self.me_id && c.status == NodeStatus::ReadyForTraffic)
            .take(crate::offline::VIA_PEER_TICKLE_FANOUT)
            .map(|c| {
                let route = rafka_node_rpc::RouteChoice::ViaPeer { carrier: c.node_id.clone(), path: c.name.clone() };
                let probe = async move {
                    let (out, _, _) = self.client.call_routed::<rafka_node_rpc_contract::status::Status>(&n.node_id, &route, kick, &CallOptions::default()).await;
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
pub async fn inspect_birth(provider: &dyn crate::deployment::provider::DeploymentProvider, book: &rafka_mesh_transport::membership::DigestBook, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> Finding {
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
    match crate::deployment::provider::exit_proof(inspected, &fact, data_dir.as_deref().map(std::path::Path::new), &inc.0) {
        crate::deployment::provider::DeploymentStatus::Exited { code } => Finding::Exited { code },
        crate::deployment::provider::DeploymentStatus::Running => Finding::Running,
        other => Finding::Unproven { reason: format!("its runtime is not proven exited: {other:?}") },
    }
}

/// The birth's own answer on the kick (node-recovery.md step 2): `Current`, `CurrentParked` while it
/// is parked, or `Blocked` while its hook is Blocked, naming the exact birth probed. A carrier's
/// carrier-edge-lost (no reply reaches the ladder), a `NotReady` and any refusal are not answers.
pub fn birth_answered(reply: Option<&rafka_node_rpc_contract::status::StatusReply>, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
    use rafka_node_rpc_contract::status::StatusReply;
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
    P: std::future::Future<Output = Option<rafka_node_rpc_contract::status::StatusReply>>,
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
    /// This admin's path.name.
    pub me: PathName,
    /// This admin's NodeId.
    pub me_id: NodeId,
    /// This admin's exact birth.
    pub incarnation: IncarnationId,
    /// Its mesh membership.
    pub membership: Membership,
    /// Its place on the backbone.
    pub backbone: Backbone,
    /// Its view of the topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// Its records of births and exits.
    pub records: Arc<Records>,
    /// What it looks at a birth with.
    pub looker: Looker,
    /// Re-publishes this admin's presence and re-joins its mesh's members (`status_rpc::Republish`).
    pub republish: crate::status_rpc::Republish,
    /// This admin's Build log: its committed facts are handed on when the fabric seat's record names another birth.
    pub builds: Arc<crate::fabric_builds::FabricBuildStateAdapter>,
}

/// Run the watch: a pass at each Concern heard and at the gossip interval (the cadence the view
/// itself is refreshed at; nothing here announces on a timer).
pub async fn run(w: Watch) {
    let mut said = Said::default();
    let me_birth = (w.me_id.clone(), w.incarnation.clone());
    let mut seats_changed = w.membership.seats().subscribe();
    let mut held_the_fabric_seat = false;
    loop {
        // A fabric seat this birth held, now recorded to another birth: the record already fences
        // its Build log; the committed facts are handed to the neighbours before the new holder acts.
        let record = w.membership.seats().fabric();
        let owed = seat_yield(record.as_ref(), &me_birth, &mut held_the_fabric_seat, &*w.topology.read().await);
        if let Some((by, to)) = owed {
            w.builds.hand_over_facts(&by, to).await;
        }
        tokio::select! {
            _ = seats_changed.changed() => {}
            _ = w.membership.concerns().heard() => {}
            _ = tokio::time::sleep(rafka_mesh_transport::membership::gossip_interval()) => {}
        }
        let heard = w.membership.concerns().drain();
        let view = w.topology.read().await.clone();
        let plan = plan(&view, &w.me, &me_birth, &heard, &mut said);
        for (node_id, incarnation) in plan.concerns {
            let holder_key = view.births().find(|n| n.node_id == node_id).and_then(|n| n.endpoint_id.as_ref()).and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
            w.backbone.concern(Seat::FabricPrimary, node_id, incarnation, holder_key).await;
        }
        if plan.answer {
            // The Concern names this very birth: it is there, so it says so. Its neighbours are
            // re-fed through the mechanisms that already exist, and each seat it holds is said again.
            let span = tracing::info_span!("rdm.node_admin.seat.update.via-concern-answer", node = %w.me);
            async {
                if let Some(f) = w.republish.get() {
                    f().await;
                }
                let admins: Vec<_> = w.membership.book.current(w.membership.book.staleness_floor()).iter().filter(|d| d.node.name.kind == NodeKind::NodeAdmin && d.node.name != w.me).filter_map(rafka_mesh_transport::membership::gossip_addr).collect();
                let refed = admins.len();
                w.backbone.join_admins(admins).await;
                let seats = w.membership.seats();
                let mut said_again = 0;
                for (seat, held) in seats.meshes().into_values().map(|h| (Seat::MeshPrimary, h)).chain(seats.fabric().map(|h| (Seat::FabricPrimary, h))) {
                    if held.is_birth(&me_birth.0, &me_birth.1) {
                        w.backbone.announce_seat(seat, held).await;
                        said_again += 1;
                    }
                }
                tracing::info!(admins = refed, seats = said_again, "named by a Concern: neighbours re-fed, seats said again");
            }
            .instrument(span)
            .await;
        }
        for (node, trigger) in plan.investigate {
            let span = tracing::info_span!(
                "rdm.node_admin.seat.update.via-investigation",
                node = %w.me,
                suspect = %node.name,
                suspect_node_id = %node.node_id,
                suspect_incarnation = node.incarnation_id.as_ref().map(|i| i.0.as_str()).unwrap_or(""),
                trigger = trigger.name(),
                holds_fabric_seat = node.is_fabric_primary,
                finding = tracing::field::Empty,
                detail = tracing::field::Empty,
                iroh_known_addrs = tracing::field::Empty,
                iroh_active_addrs = tracing::field::Empty,
            );
            // One read of iroh's local view of the suspect, recorded beside the investigation;
            // the finding is the provider's and the Node RPC's, never this.
            let key = node.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
            let seen = rafka_mesh_transport::iroh_obs::observe_remote(&w.membership.endpoint(), key).await;
            span.record("iroh_known_addrs", seen.known_addrs.as_str());
            span.record("iroh_active_addrs", seen.active_addrs.as_str());
            let finding = w.looker.look(&view, &node).instrument(span.clone()).await;
            span.record("finding", finding.name());
            span.record("detail", finding.detail().as_str());
            if let (Finding::Exited { .. }, Some(inc)) = (&finding, node.incarnation_id.as_ref()) {
                w.records.mark_exited(&node.node_id, inc);
            }
            span.in_scope(|| tracing::info!("a silent seat holder was looked at"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin(path: &str, id: &str, inc: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.node_id = NodeId::parse(id).unwrap();
        n.incarnation_id = Some(IncarnationId(inc.into()));
        n.status = status;
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        use crate::model::{Fabric, FabricId, ProviderKind, ScopeStatus};
        Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process }, meshes: Vec::new(), nodes }
    }

    fn concern(n: &Node) -> ConcernHeard {
        ConcernHeard { seat: Seat::FabricPrimary, node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), observer: "mesh2.admin.1".into() }
    }

    fn me_of(n: &Node) -> Birth {
        (n.node_id.clone(), n.incarnation_id.clone().unwrap())
    }

    /// The shape: mesh1 holds the fabric seat (admin.1 silent), admin.2 is its standby, mesh2's
    /// primary looks from outside.
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
        let record = |n: &Node, epoch: u64| SeatHolder { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), epoch };
        let (me, mut held) = (me_of(&holder), false);
        assert_eq!(seat_yield(Some(&record(&holder, 1)), &me, &mut held, &t), None, "the record names this birth: nothing yields");
        assert_eq!(seat_yield(Some(&record(&outsider, 2)), &me, &mut held, &t), Some(("the seat is now mesh2 100000000000".to_string(), Some("mesh2.admin.1".to_string()))), "the fence names the holder the record names");
        assert_eq!(seat_yield(Some(&record(&outsider, 2)), &me, &mut held, &t), None, "once per move");
        let mut never = false;
        assert_eq!(seat_yield(Some(&record(&outsider, 2)), &me_of(&standby), &mut never, &t), None, "a birth that never held the record owes nothing");
    }

    /// CONTRACT (R-A2 item 3): a mesh primary says the fabric primary's exact birth is silent once
    /// per mark, and again only after the birth was heard and went silent again. A node that holds
    /// no mesh primary seat says nothing.
    #[test]
    fn a_mesh_primary_publishes_one_concern_per_staleness_mark() {
        let (t, holder, standby, outsider) = world();
        let mut said = Said::default();
        let first = plan(&t, &outsider.name, &me_of(&outsider), &[], &mut said);
        assert_eq!(first.concerns, vec![me_of(&holder)], "the exact birth, once");
        assert!(plan(&t, &outsider.name, &me_of(&outsider), &[], &mut said).concerns.is_empty(), "the same mark is not spoken of again");
        assert!(plan(&t, &standby.name, &me_of(&standby), &[], &mut Said::default()).concerns.is_empty(), "a standby is no mesh primary");
        // Heard again, then silent again: a new mark.
        let mut heard_again = t.clone();
        heard_again.nodes[0].status = NodeStatus::ReadyForTraffic;
        assert!(plan(&heard_again, &outsider.name, &me_of(&outsider), &[], &mut said).concerns.is_empty());
        assert_eq!(plan(&t, &outsider.name, &me_of(&outsider), &[], &mut said).concerns, vec![me_of(&holder)]);
    }

    /// CONTRACT (R-A2 item 4): the named birth answers a Concern; an admin of its mesh looks at it,
    /// standby included; an admin of another mesh does not act on the Concern.
    #[test]
    fn a_concern_makes_the_named_birth_answer_and_its_mesh_look_at_it() {
        let (t, holder, standby, outsider) = world();
        let c = [concern(&holder)];
        let named = plan(&t, &holder.name, &me_of(&holder), &c, &mut Said::default());
        assert!(named.answer && named.investigate.is_empty(), "the incumbent re-feeds and says its seats again, it investigates nobody");
        let sibling = plan(&t, &standby.name, &me_of(&standby), &c, &mut Said::default());
        assert!(!sibling.answer);
        assert_eq!(sibling.investigate.iter().map(|(n, t)| (n.name.to_string(), *t)).collect::<Vec<_>>(), vec![("mesh1.admin.1".to_string(), Trigger::Concern)]);
        let other = plan(&t, &outsider.name, &me_of(&outsider), &c, &mut Said::default());
        assert!(!other.answer && other.investigate.iter().all(|(_, t)| *t == Trigger::Silence), "another mesh looks only for what its own view shows");
    }

    /// CONTRACT (R-A2 items 4-5): an admin looks at a silent birth of the incumbent mesh once per
    /// mark whether or not a Concern came, and a silent admin of a mesh that holds no seat is
    /// nobody's business.
    #[test]
    fn a_silent_incumbent_birth_is_looked_at_once_per_mark_and_other_silence_is_not() {
        let (mut t, holder, standby, _) = world();
        let mut said = Said::default();
        let own = plan(&t, &standby.name, &me_of(&standby), &[], &mut said);
        assert_eq!(own.investigate.iter().map(|(n, _)| n.name.to_string()).collect::<Vec<_>>(), vec!["mesh1.admin.1".to_string()]);
        assert!(plan(&t, &standby.name, &me_of(&standby), &[], &mut said).investigate.is_empty(), "once per mark");
        // A silent admin of a third mesh that holds nothing.
        t.nodes.push(admin("mesh3.admin.1", "500000000000", "x1", NodeStatus::PendingReconnect));
        assert!(plan(&t, &standby.name, &me_of(&standby), &[], &mut said).investigate.is_empty());
        let _ = holder;
    }

    /// CONTRACT (R-RC1, node-replace.md "Successor | New node.id"): the view holds one row per
    /// path.name (`admin::project_at` keys it by `PathName`), so after a replace of mesh2's primary
    /// the row at mesh2.admin.1 can be the retired birth's, live and still seated, while the reborn
    /// admin's own row is absent. The reborn admin decides its seat by the node id it was born with:
    /// it holds no mesh primary seat and says no Concern. What must NOT happen: the retired birth's
    /// seat read as this admin's, a Concern published as a mesh primary it is not.
    #[test]
    fn a_reborn_admin_whose_view_seats_its_retired_birth_says_no_concern() {
        let (mut t, holder, _, outsider) = world();
        let mut retired = outsider.clone();
        retired.status = NodeStatus::Draining;
        t.nodes.retain(|n| n.node_id != outsider.node_id);
        t.nodes.push(retired.clone());
        let reborn = admin("mesh2.admin.1", "600000000000", "r2", NodeStatus::ReadyForTraffic);
        assert_eq!(t.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.node_id.clone()), Some(retired.node_id.clone()), "the retired birth holds mesh2's seat in this view");
        let p = plan(&t, &reborn.name, &me_of(&reborn), &[], &mut Said::default());
        assert!(p.concerns.is_empty(), "the reborn admin holds no seat: it says no Concern about {}, got {:?}", holder.name, p.concerns);
    }

    mod ladder {
        //! The ladder's rungs and completion rules (ruling R-SW1; node-recovery.md step 2), under
        //! paused time: a probe's wait is its budget, and the clock moves only when every probe waits.
        use super::super::*;
        use rafka_node_rpc_contract::status::{NodeState, RetryOn, StatusReply};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The default call budget each probe spends against a process that never answers.
        const BUDGET: Duration = Duration::from_secs(10);

        fn birth() -> (NodeId, IncarnationId) {
            (NodeId::mint(), IncarnationId("inc-of-the-holder".into()))
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
            let other = (b.0.clone(), IncarnationId("an-older-birth".into()));
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
