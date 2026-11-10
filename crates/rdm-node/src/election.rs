//! The node-type election, held by the nodes of one type in one mesh among themselves
//! (fabric-node-lifecycle-elections.md §4.1; Q406, Q407). It is every type's but `node-admin`'s:
//! node-admins elect only the mesh-primary and the fabric-primary (rdm-admin's `election`), and the
//! mesh-primary and the other node-admins take no part here.
//!
//! A type's first node in a mesh is its first primary: the mesh-primary's create step commands it by
//! `node.primary.update` once it is Ready (Q407). After that the role moves by `node.primary.update`,
//! or, with the primary unheard past the staleness floor or known gone, by this election: each node of
//! the type computes, from its own view, the lowest Ready node of its type in its mesh
//! ([`rdm_core::election::elect`]), and the winner installs itself at the next revision over the record
//! it holds. Taking the role is a change of the node's own: its own record
//! ([`crate::node_records::write_own_roles`]) and its next heartbeat Digest carry it (its `primary`,
//! Q392), and the others take it from that Digest by the supersede rule (`Primary::supersedes`). A node
//! that holds no record of its type's primary elects nothing: the create's command fills that vacancy.

use rdm_core::election::{elect, Candidate};
use rdm_core::model::NodeStatus;
use rdm_core::{IncarnationId, NodeId, Primary, PrimaryRole};
use rdm_gossip::topology::TopologyView;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// The election key every span names.
pub const ELECTION_KEY: &str = "node_id_crockford";

/// Why the record a node holds of its type's primary names no primary it can follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vacant {
    /// The primary's exact birth is known gone: its deletion was heard, a later birth announced that it
    /// replaced it, or a peer's topology said it is gone.
    Gone,
    /// The view holds a later birth of the primary's node id.
    Replaced,
    /// The pruner's sweep found the primary unheard past the staleness floor, and it has not been heard
    /// since: its birth held and silent past the floor, or never held here for a floor since this view
    /// first held the record naming it (a record read off a store or an entry, of a birth lost before
    /// this view heard it).
    Unheard,
}

impl Vacant {
    /// The vacancy as a span names it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Gone => "gone",
            Self::Replaced => "replaced",
            Self::Unheard => "unheard-past-the-floor",
        }
    }
}

/// What one computation over a node's view found: the record it holds, why that record names no
/// primary it can follow, every node of its type it holds as an input, and the winner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vacancy {
    /// The record of the type's primary this node holds.
    pub held: Primary,
    /// Why it is vacant.
    pub why: Vacant,
    /// Every node of the type in the mesh this node holds current, itself included, with the status it
    /// holds for each, by node id: the election's inputs.
    pub inputs: Vec<(NodeId, NodeStatus)>,
    /// The lowest Ready node id among `inputs`; `None` when none is Ready.
    pub winner: Option<NodeId>,
}

/// One node of a type other than `node-admin`, as its type's election sees it.
#[derive(Clone)]
pub struct Elector {
    /// The node's topology view: the records it holds and the digests of its mesh.
    pub topology_view: TopologyView,
    /// The node's id.
    pub node_id: NodeId,
    /// The node's incarnation.
    pub incarnation: IncarnationId,
    /// The node's mesh.
    pub mesh: String,
    /// The node's type.
    pub r#type: String,
    /// The node's own status, as it publishes it.
    pub status: Arc<Mutex<NodeStatus>>,
}

impl Elector {
    /// The vacancy this node's view holds of its type's primary, with the winner the election names for
    /// it; `None` while no record of the type's primary is held (the create's command fills a type's first
    /// primary, Q407), while the record names this birth, or while it names a primary this view follows:
    /// its exact birth, not known gone, not replaced, and not unheard past the staleness floor. A record
    /// naming a birth this view never held is unheard from the instant the view first held it
    /// (fabric-node-lifecycle-elections.md §1: "the primary stays primary while it is heard -> unheard
    /// past the staleness floor"): the pruner's sweep marks it silent a floor later
    /// (`DigestView::mark_silence_at`), so the nodes of the type elect by the same rule.
    pub fn vacancy(&self) -> Option<Vacancy> {
        // The node-admins' own election is the mesh-primary's and the fabric-primary's (rdm-admin).
        if rdm_core::is_node_admin(&self.r#type) {
            return None;
        }
        let primaries = self.topology_view.primaries();
        let digest_view = &self.topology_view.digest_view;
        let held = primaries.node_type_primary(&self.mesh, &self.r#type)?;
        if held.is_birth(&self.node_id, &self.incarnation) {
            return None;
        }
        let gone: HashSet<IncarnationId> = digest_view.deleted().into_iter().map(|op| op.incarnation).chain(digest_view.superseded()).chain(primaries.gone()).collect();
        let why = if gone.contains(&held.incarnation) || digest_view.is_deleted(held.node_id.as_str()) {
            Vacant::Gone
        } else if digest_view.get(held.node_id.as_str()).is_some_and(|(d, _)| d.node.incarnation != held.incarnation) {
            Vacant::Replaced
        } else if digest_view.is_silent(&held.node_id, &held.incarnation) && !self.heard_since_the_sweep(&held) {
            Vacant::Unheard
        } else {
            return None;
        };
        let mut inputs: Vec<(NodeId, NodeStatus)> = digest_view
            .current(digest_view.staleness_floor())
            .into_iter()
            .filter(|d| d.node.mesh == self.mesh && d.node.r#type == self.r#type && d.node.node_id != self.node_id)
            .filter(|d| !gone.contains(&d.node.incarnation) && !digest_view.is_deleted(d.node.node_id.as_str()))
            .map(|d| (d.node.node_id, d.status))
            .collect();
        inputs.push((self.node_id.clone(), *self.status.lock().unwrap()));
        let candidates: Vec<Candidate<'_>> = inputs.iter().map(|(node_id, status)| Candidate { node_id, ready: *status == NodeStatus::Ready }).collect();
        let winner = elect(&candidates).map(|i| inputs[i].0.clone());
        Some(Vacancy { held, why, inputs, winner })
    }

    /// Whether `held`'s exact birth was heard within the staleness floor at this read: heard again since
    /// the pruner's last sweep found it silent. The sweep marks when a birth falls silent (Q254); a birth
    /// heard again is heard from that digest on, so the primary it names stays primary (elections §5:
    /// "the primary stays primary while it is heard").
    fn heard_since_the_sweep(&self, held: &Primary) -> bool {
        let digest_view = &self.topology_view.digest_view;
        digest_view.get(held.node_id.as_str()).is_some_and(|(d, unheard)| d.node.incarnation == held.incarnation && unheard <= digest_view.staleness_floor())
    }

    /// The record this birth installs when it wins `vacancy`: itself, of its mesh, at the next revision
    /// over the record held.
    pub fn record(&self, vacancy: &Vacancy) -> Primary {
        Primary { mesh: self.mesh.clone(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), revision: vacancy.held.revision + 1 }
    }

    /// One computation: when the view holds a vacancy, the winner, if it is this birth, installs itself
    /// ([`Self::record`]). Each vacancy and winner is named once on a span (`last` holds the one named
    /// last). Whether this birth installed itself.
    pub fn elect_once(&self, last: &mut Option<(Primary, Option<NodeId>)>) -> bool {
        let Some(vacancy) = self.vacancy() else { return false };
        let mine = vacancy.winner.as_ref() == Some(&self.node_id);
        let taken = mine && {
            let record = self.record(&vacancy);
            self.topology_view.learn_primaries(&[(PrimaryRole::NodePrimary(self.r#type.clone()), record.clone(), false)], "election");
            self.topology_view.primaries().node_type_primary(&self.mesh, &self.r#type).as_ref() == Some(&record)
        };
        let key = (vacancy.held.clone(), vacancy.winner.clone());
        if last.as_ref() != Some(&key) || taken {
            self.name_on_span(&vacancy, taken);
            *last = Some(key);
        }
        taken
    }

    fn name_on_span(&self, vacancy: &Vacancy, taken: bool) {
        let digest_view = &self.topology_view.digest_view;
        // The winner's path and name as this view holds its digest (this birth's own included: its
        // channel records every digest it publishes).
        let winner = vacancy.winner.as_ref().and_then(|id| digest_view.get(id.as_str()).map(|(d, _)| d.node.location()));
        let eligible = vacancy.inputs.iter().filter(|(_, s)| *s == NodeStatus::Ready).count();
        let inputs: Vec<String> = vacancy.inputs.iter().map(|(id, s)| format!("node_id={id}, status={s:?}")).collect();
        let outcome = match (&vacancy.winner, taken) {
            (None, _) => "no-ready-candidate",
            (Some(_), true) => "installed",
            (Some(id), false) if *id == self.node_id => "not-newer",
            (Some(_), false) => "elected-another",
        };
        let span = tracing::info_span!(
            parent: None,
            "rdm.mesh.election.resolve.via-recompute",
            election_level = "node_type_primary",
            observer_node_id = %self.node_id,
            observer_type = %self.r#type,
            mesh = %self.mesh,
            "type" = self.r#type.as_str(),
            vacancy = vacancy.why.name(),
            candidate_count = vacancy.inputs.len(),
            eligible_count = eligible,
            inputs = %inputs.join(","),
            winner_node_id = %vacancy.winner.as_ref().map(ToString::to_string).unwrap_or_default(),
            winner_path = %winner.as_ref().map(|w| w.path.clone()).unwrap_or_default(),
            winner_name = %winner.as_ref().map(|w| w.name.clone()).unwrap_or_default(),
            election_key = ELECTION_KEY,
            previous_node_id = %vacancy.held.node_id,
            previous_revision = vacancy.held.revision,
            outcome,
        );
        span.in_scope(|| tracing::info!("the nodes of a type elect its primary among themselves: the lowest Ready node of the type in its mesh"));
    }
}

/// Run `elector`'s election at each change of what it decides from: a record held, a birth held,
/// changed or heard again, a birth found silent past the floor by the pruner's sweep, a departure taken
/// (`DigestView::routing_watch`, which holds the primaries watch). Nothing is polled. The task ends
/// when it is aborted.
pub fn spawn(elector: Elector) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut moved = elector.topology_view.digest_view.routing_watch();
        let mut last: Option<(Primary, Option<NodeId>)> = None;
        loop {
            moved.seen();
            elector.elect_once(&mut last);
            moved.moved().await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::{EndpointId, FabricId, MeshDigest, MeshId, MeshNode};
    use std::time::{Duration, Instant};

    struct Mesh {
        fabric_id: FabricId,
        view: TopologyView,
    }

    impl Mesh {
        /// The topology view of one node of mesh1.
        fn new() -> Self {
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            rafka_time.adopt(1_000_000);
            let fabric_id = FabricId::mint();
            let view = TopologyView::new(&fabric_id, "mesh1", &MeshId::mint(), "mesh1.broker.2", Arc::new(rafka_time), Default::default());
            Self { fabric_id, view }
        }

        fn digest(&self, r#type: &str, name: &str, node_id: &str, status: NodeStatus) -> MeshDigest {
            MeshDigest {
                fabric_id: self.fabric_id.clone(),
                node: MeshNode {
                    mesh: "mesh1".into(),
                    r#type: r#type.into(),
                    node_id: NodeId::parse(node_id).unwrap(),
                    path: format!("mesh1.{type}"),
                    name: name.into(),
                    endpoint_id: EndpointId(format!("key-{node_id}")),
                    transport_addr: "127.0.0.1:41000".parse().unwrap(),
                    incarnation: IncarnationId::mint(),
                    supersedes: None,
                },
                status,
                primary: Vec::new(),
                digest_seq: 1,
                emitted_at_rafka_ms: 1,
            }
        }

        /// `d` heard now.
        fn heard(&self, d: &MeshDigest) {
            assert!(self.view.digest_view.record_at(d.clone(), Instant::now()), "the view holds {}", d.node.node_id);
        }

        fn elector(&self, me: &MeshDigest, status: NodeStatus) -> Elector {
            Elector { topology_view: self.view.clone(), node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone(), mesh: "mesh1".into(), r#type: me.node.r#type.clone(), status: Arc::new(Mutex::new(status)) }
        }

        fn record(&self, d: &MeshDigest, revision: u64) -> Primary {
            Primary { mesh: "mesh1".into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
        }

        fn broker_primary(&self) -> Option<Primary> {
            self.view.primaries().node_type_primary("mesh1", "broker")
        }
    }

    /// CONTRACT (Q406; elections §4.1: "A primary unheard past the staleness floor is superseded by the
    /// lowest Ready node of its type in its mesh, which installs itself and gossips the role in its
    /// Digest"): the brokers of a mesh elect among themselves. While the recorded broker primary is heard
    /// no broker elects; once the pruner's sweep finds it unheard past the floor, each broker computes the
    /// lowest Ready broker from its own view, a gateway of a lower NodeId and a Pending broker counting for
    /// nothing, and only the winner installs itself, at the next revision. Must NOT happen: a broker
    /// installing itself while its primary is heard, a node of another type or a Pending one winning, or
    /// a broker that did not win installing itself.
    // @feature: mesh-elections
    #[tokio::test]
    async fn the_brokers_elect_the_lowest_ready_broker_once_their_primary_is_unheard_past_the_floor() {
        let mesh = Mesh::new();
        let floor = mesh.view.digest_view.staleness_floor();
        let primary = mesh.digest("broker", "1", "100000000001", NodeStatus::Ready);
        let me = mesh.digest("broker", "2", "300000000000", NodeStatus::Ready);
        let higher = mesh.digest("broker", "3", "500000000000", NodeStatus::Ready);
        let pending = mesh.digest("broker", "4", "200000000000", NodeStatus::Pending);
        let gateway = mesh.digest("gateway", "1", "000000000001", NodeStatus::Ready);
        // The primary last heard a floor and more ago; the others heard now.
        assert!(mesh.view.digest_view.record_at(primary.clone(), Instant::now().checked_sub(floor * 2).unwrap()));
        for d in [&me, &higher, &pending, &gateway] {
            mesh.heard(d);
        }
        mesh.view.primaries().take(PrimaryRole::NodePrimary("broker".into()), &mesh.record(&primary, 1));
        let (mine, theirs) = (mesh.elector(&me, NodeStatus::Ready), mesh.elector(&higher, NodeStatus::Ready));
        assert_eq!(mine.vacancy(), None, "no sweep has found the primary silent yet: no vacancy");
        mesh.view.digest_view.mark_silence();
        assert!(mesh.view.digest_view.is_silent(&primary.node.node_id, &primary.node.incarnation), "the sweep holds the primary silent past the floor");
        let vacancy = theirs.vacancy().expect("a vacancy");
        assert_eq!(vacancy.why, Vacant::Unheard);
        assert_eq!(vacancy.winner, Some(me.node.node_id.clone()), "the lowest Ready broker: {vacancy:?}");
        assert!(!vacancy.inputs.iter().any(|(id, _)| *id == gateway.node.node_id || *id == primary.node.node_id), "inputs are the type's current nodes: {vacancy:?}");
        assert!(!theirs.elect_once(&mut None), "a broker that is not the winner installs nothing");
        assert_eq!(mesh.broker_primary(), Some(mesh.record(&primary, 1)));
        assert!(mine.elect_once(&mut None), "the winner installs itself");
        assert_eq!(mesh.broker_primary(), Some(mesh.record(&me, 2)), "at the next revision");
        assert_eq!(mine.vacancy(), None, "its own record: no vacancy");
        assert_eq!(theirs.vacancy(), None, "the winner's record names a heard broker: no vacancy");
    }

    /// CONTRACT (elections §5: "the primary stays primary while it is heard"; §4.1, Q406): a broker
    /// primary the pruner's sweep found silent past the floor, then heard again before the next sweep, is
    /// heard: no broker elects. Must NOT happen: a broker installing itself over a primary it hears because
    /// the last sweep still holds it silent (node_type_election, lane 5, 2026-11-07: after the partition
    /// healed, phxc3y677jkx installed itself over wpxgf7bejqyr, `vacancy = unheard-past-the-floor`, its
    /// own `inputs` listing wpxgf7bejqyr Ready).
    // @feature: mesh-elections
    #[tokio::test]
    async fn a_primary_heard_again_after_the_sweep_found_it_silent_is_no_vacancy() {
        let mesh = Mesh::new();
        let floor = mesh.view.digest_view.staleness_floor();
        let primary = mesh.digest("broker", "1", "100000000001", NodeStatus::Ready);
        let me = mesh.digest("broker", "2", "300000000000", NodeStatus::Ready);
        assert!(mesh.view.digest_view.record_at(primary.clone(), Instant::now().checked_sub(floor * 2).unwrap()));
        mesh.heard(&me);
        mesh.view.primaries().take(PrimaryRole::NodePrimary("broker".into()), &mesh.record(&primary, 1));
        mesh.view.digest_view.mark_silence();
        let mine = mesh.elector(&me, NodeStatus::Ready);
        assert_eq!(mine.vacancy().map(|v| v.why), Some(Vacant::Unheard), "the sweep found the primary silent and it is unheard");
        // The primary's next Digest, heard before the next sweep.
        mesh.heard(&MeshDigest { digest_seq: 2, ..primary.clone() });
        assert!(mesh.view.digest_view.is_silent(&primary.node.node_id, &primary.node.incarnation), "the last sweep still holds it silent");
        assert_eq!(mine.vacancy(), None, "a heard primary: no vacancy");
        assert!(!mine.elect_once(&mut None), "nothing installed");
        assert_eq!(mesh.broker_primary(), Some(mesh.record(&primary, 1)));
    }

    /// CONTRACT (Q407: the create's command fills a type's first primary; elections §4.1): a node that
    /// holds no record of its type's primary elects nothing, whatever its view holds; a record naming a
    /// heard primary is no vacancy; a record whose birth is replaced by a later birth of its node id, or
    /// known gone (its deletion heard), is; a Pending node wins nothing, and with no Ready node there is no
    /// winner. Must NOT happen: a type's node electing a first primary the create had not commanded.
    // @feature: mesh-elections
    #[tokio::test]
    async fn a_node_elects_only_when_the_record_it_holds_names_a_primary_gone_replaced_or_unheard() {
        let mesh = Mesh::new();
        let primary = mesh.digest("broker", "1", "100000000001", NodeStatus::Ready);
        let me = mesh.digest("broker", "2", "300000000000", NodeStatus::Ready);
        for d in [&primary, &me] {
            mesh.heard(d);
        }
        let mine = mesh.elector(&me, NodeStatus::Ready);
        assert_eq!(mine.vacancy(), None, "no record held: the create's command fills the type's first primary");
        mesh.view.primaries().take(PrimaryRole::NodePrimary("broker".into()), &mesh.record(&primary, 1));
        assert_eq!(mine.vacancy(), None, "a heard primary");
        // A later birth of the primary's node id.
        let later_birth = MeshDigest { node: MeshNode { incarnation: IncarnationId::mint(), ..primary.node.clone() }, ..primary.clone() };
        mesh.heard(&later_birth);
        assert_eq!(mine.vacancy().map(|v| v.why), Some(Vacant::Replaced));
        // Its deletion heard.
        mesh.view.digest_view.depart(rdm_core::LifecycleOp { r#type: "broker".into(), mesh: "mesh1".into(), build_id: "dbs8z9896j0f".into(), operation: rdm_core::OperationId::node("node.delete", &primary.node.node_id), node_id: primary.node.node_id.clone(), incarnation: primary.node.incarnation.clone(), path: primary.node.path.clone(), name: primary.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None });
        let vacancy = mine.vacancy().expect("a vacancy");
        assert_eq!(vacancy.why, Vacant::Gone);
        assert_eq!(vacancy.winner, Some(me.node.node_id.clone()), "the one Ready broker left: {vacancy:?}");
        let pending = mesh.elector(&me, NodeStatus::Pending);
        assert_eq!(pending.vacancy().and_then(|v| v.winner), None, "a Pending node wins nothing, and no other broker is Ready");
        assert!(!pending.elect_once(&mut None));
        assert_eq!(mesh.broker_primary(), Some(mesh.record(&primary, 1)), "nothing installed");
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §1: "the primary stays primary while it is heard ->
    /// unheard past the staleness floor: eligible candidates of that type in that mesh -> lowest
    /// complete NodeId"; §4.1, Q406): a broker's view holds a record of its type's primary naming a birth
    /// it never held a digest of, not known gone (a record of a birth lost before this view heard it).
    /// Within a floor of first holding the record no broker elects; the pruner's sweep a floor later
    /// finds it unheard, and the lowest Ready broker installs itself at the next revision over it. Must
    /// NOT happen: the type left with a primary nobody hears and no election, or an election before the
    /// record has been held a floor.
    // @feature: mesh-elections
    #[tokio::test]
    async fn a_record_naming_a_birth_this_view_never_heard_is_vacant_a_floor_after_it_was_held() {
        let mesh = Mesh::new();
        let floor = mesh.view.digest_view.staleness_floor();
        let never_heard = mesh.digest("broker", "1", "100000000001", NodeStatus::Ready);
        let me = mesh.digest("broker", "2", "300000000000", NodeStatus::Ready);
        let higher = mesh.digest("broker", "3", "500000000000", NodeStatus::Ready);
        for d in [&me, &higher] {
            mesh.heard(d);
        }
        mesh.view.primaries().take(PrimaryRole::NodePrimary("broker".into()), &mesh.record(&never_heard, 4));
        let held_at = Instant::now();
        let (mine, theirs) = (mesh.elector(&me, NodeStatus::Ready), mesh.elector(&higher, NodeStatus::Ready));
        assert!(!mesh.view.digest_view.mark_silence_at(held_at), "within a floor of the record: nothing silent");
        assert_eq!(mine.vacancy(), None, "a record held under a floor: no vacancy");
        assert!(mesh.view.digest_view.mark_silence_at(held_at + floor * 2), "the sweep a floor later moves the silence watch");
        assert!(mesh.view.digest_view.is_silent(&never_heard.node.node_id, &never_heard.node.incarnation), "the record's birth, never heard, is silent past the floor");
        let vacancy = theirs.vacancy().expect("a vacancy");
        assert_eq!(vacancy.why, Vacant::Unheard);
        assert_eq!(vacancy.winner, Some(me.node.node_id.clone()), "the lowest Ready broker: {vacancy:?}");
        assert!(!theirs.elect_once(&mut None), "a broker that is not the winner installs nothing");
        assert!(mine.elect_once(&mut None), "the winner installs itself");
        assert_eq!(mesh.broker_primary(), Some(mesh.record(&me, 5)), "at the next revision over the record held");
        assert_eq!(theirs.vacancy(), None, "the winner's record names a heard broker: no vacancy");
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §4.2: the node-admin type's primary is the
    /// mesh-primary, the node-admins' own election): a node-admin's elector elects nothing here.
    #[tokio::test]
    async fn the_node_admin_type_is_never_elected_here() {
        let mesh = Mesh::new();
        let admin = mesh.digest("node-admin", "1", "100000000001", NodeStatus::Ready);
        let me = mesh.digest("node-admin", "2", "300000000000", NodeStatus::Ready);
        mesh.heard(&me);
        mesh.view.primaries().take(PrimaryRole::MeshPrimary, &mesh.record(&admin, 1));
        mesh.view.primaries().mark_gone(admin.node.incarnation.clone());
        assert_eq!(mesh.elector(&me, NodeStatus::Ready).vacancy(), None, "its record's birth gone, a node-admin elects nothing here");
    }
}
