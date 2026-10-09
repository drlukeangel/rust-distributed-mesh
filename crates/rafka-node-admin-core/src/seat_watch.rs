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
//!   silent birth: a direct ping, the status kick when it answers, the same kick through a live
//!   member of its mesh, and last the provider's inspection of the birth's exact runtime. An
//!   answer, or a runtime that still runs, ends it: the seat stays. Only a runtime found exited is
//!   a verified loss (`Records::mark_exited`); a failed ping, `NotSent`, silence and the
//!   observer-inferred `Dead` are not.
//!
//! [`plan`] decides what a pass owes, from the view and what was already said; [`run`] drives it.

use crate::admin::Records;
use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::Seat;
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

/// The pass: `me` is this admin's exact birth, `heard` the Concerns drained since the last pass.
pub fn plan(view: &Topology, me: &PathName, me_birth: &Birth, heard: &[ConcernHeard], said: &mut Said) -> Plan {
    let silent: HashSet<Birth> = view.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::PendingReconnect).filter_map(birth).collect();
    said.concerned.retain(|b| silent.contains(b));
    said.investigated.retain(|b| silent.contains(b));
    let mut plan = Plan::default();
    let mesh = &me.mesh;

    // A mesh primary says the fabric primary's exact birth is silent, once per mark.
    if view.cohort_primary(mesh, NodeKind::NodeAdmin).is_some_and(|n| &n.name == me) {
        for n in view.nodes.iter().filter(|n| n.is_fabric_primary && n.status == NodeStatus::PendingReconnect) {
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
        let Some(n) = view.nodes.iter().find(|n| n.node_id == c.node_id && n.incarnation_id.as_ref() == Some(&c.incarnation)) else { continue };
        if &c.node_id == &me_birth.0 && c.incarnation == me_birth.1 {
            plan.answer = true;
        } else if &n.mesh == mesh && n.kind == NodeKind::NodeAdmin {
            plan.investigate.push((n.clone(), Trigger::Concern));
        }
    }

    // What this admin sees itself: a silent seat holder of its own mesh, and any silent node-admin
    // of the mesh that holds the fabric seat (a standby, or a candidate looking from outside).
    let fabric_mesh = view.nodes.iter().find(|n| n.is_fabric_primary).map(|n| n.mesh.clone());
    for n in view.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::PendingReconnect && &n.name != me) {
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
    /// Its mesh membership.
    pub membership: Membership,
    /// Its Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// Its deployment provider.
    pub provider: Arc<dyn crate::deployment::provider::DeploymentProvider>,
}

impl Looker {
    /// Look at `n`'s exact birth: the ladder ends at the first rung that settles it.
    pub async fn look(&self, view: &Topology, n: &Node) -> Finding {
        let target = NodeTarget::ExactNode(n.node_id.clone());
        let incarnation = n.incarnation_id.clone().unwrap_or_else(|| IncarnationId(String::new()));
        let kick = rafka_node_rpc_contract::status::StatusRequest::ProbeNodeState { node_id: n.node_id.clone(), incarnation };
        // 1. A direct ping; when it answers, the status kick makes the birth say it is there.
        let ping = rafka_node_rpc_contract::ping::PingRequest::Ping { payload: b"seat-watch".to_vec() };
        let (out, _) = self.client.call::<rafka_node_rpc_contract::ping::Ping>(&target, &ping, &CallOptions::default()).await;
        if out.reply().is_some() {
            let _ = self.client.call::<rafka_node_rpc_contract::status::Status>(&target, &kick, &CallOptions::default()).await;
            return Finding::Answered;
        }
        // 2. The same kick through a live member of the birth's mesh.
        for c in view.nodes.iter().filter(|c| c.mesh == n.mesh && c.node_id != n.node_id && c.name != self.me && c.status == NodeStatus::ReadyForTraffic).take(crate::offline::VIA_PEER_TICKLE_FANOUT) {
            let route = rafka_node_rpc::RouteChoice::ViaPeer { carrier: c.node_id.clone(), path: c.name.clone() };
            let (out, _, _) = self.client.call_routed::<rafka_node_rpc_contract::status::Status>(&n.node_id, &route, &kick, &CallOptions::default()).await;
            if matches!(out.reply().map(|r| r.value()), Some(rafka_node_rpc_contract::status::StatusReply::Current { .. })) {
                return Finding::AnsweredViaPeer { carrier: c.name.to_string() };
            }
        }
        // 3. The provider's inspection of the exact runtime: the only rung that proves a loss.
        let Some(inc) = n.incarnation_id.clone() else { return Finding::Unproven { reason: "the view holds no incarnation for it".into() } };
        let held = self.membership.book.get(n.node_id.as_str()).filter(|(d, _)| d.node.incarnation == inc);
        let Some((digest, _)) = held else { return Finding::Unproven { reason: "this admin holds no digest of this exact birth, so no runtime fact".into() } };
        let Some(fact) = digest.node.runtime.clone() else { return Finding::Unproven { reason: "its digest carries no runtime fact".into() } };
        let handle = match crate::deployment::provider::adopt(&*self.provider, &fact) {
            Ok(h) => h,
            Err(e) => return Finding::Unproven { reason: format!("the provider refused to adopt its runtime: {e}") },
        };
        let inspected = self.provider.inspect(&handle).await;
        match crate::deployment::provider::exit_proof(inspected, &fact, digest.data_dir.as_deref().map(std::path::Path::new), &inc.0) {
            crate::deployment::provider::DeploymentStatus::Exited { code } => Finding::Exited { code },
            crate::deployment::provider::DeploymentStatus::Running => Finding::Running,
            other => Finding::Unproven { reason: format!("its runtime is not proven exited: {other:?}") },
        }
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
    /// This admin's Build log: fenced when the fabric seat's record names another birth.
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
        // A fabric seat this birth held, now recorded to another birth: it yielded (planned or
        // not), and its Build log is fenced before the new holder acts.
        match w.membership.seats().fabric() {
            Some(h) if h.is_birth(&me_birth.0, &me_birth.1) => held_the_fabric_seat = true,
            Some(h) if held_the_fabric_seat => {
                let by = format!("the seat is now {} {}", h.mesh, h.node_id);
                if let Err(e) = w.builds.yield_seat(&by).await {
                    tracing::info_span!("rdm.node_admin.build.reject.via-seat-fence-unsent", node = %w.me, error = %e).in_scope(|| tracing::info!("the fence is set; the committed Build facts could not all be handed on"));
                }
                held_the_fabric_seat = false;
            }
            _ => {}
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
            let holder_key = view.nodes.iter().find(|n| n.node_id == node_id).and_then(|n| n.endpoint_id.as_ref()).and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
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
}
