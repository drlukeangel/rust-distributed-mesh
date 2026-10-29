//! The join (`JoinNode`, op `0x1D`; i143 R-J1).
//!
//! A node binds its transport on port 0, so the operating system assigns the port and no admin
//! ever picks one. Its first call is `JoinNode` to the node-admin that deployed it, carrying the
//! node's full topology digest with the address read from the bound endpoint. The admin holds
//! what it deployed ([`Deployed`], registered by the deployment pipeline before the runtime
//! starts), verifies the digest against it, and on a match installs the address for that key
//! (the topology view's `register_location`, the live resolver, which cancels every dial aimed at the
//! key's old address) and completes `WaitForBind` and `WaitForMeshJoin` from the node's own report
//! (node-create.md step 5: from the validated report, never from gossip). Once the create that
//! registered the deployment ends, the report stays this admin's record of the admitted birth, so
//! the same deployment's next registration (the Build's next attempt) is handed it. A disagreement is
//! refused by name with both values. A join for a birth whose create ended without hearing it is
//! refused as `DeploymentAbandoned`, naming the Build, the attempt and the exact birth: this admin
//! is the birth's accepting primary and never answers `NotPrimary` about itself.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
use crate::storage::{NodesStorage, RuntimeRow};
use rdm_core::{MeshDigest, RuntimeFact};
use crate::wire::JoinAnswer;
use rdm_transport::ServerBuilder;
use rdm_core::catalog::OpOwner;
use rdm_core::join::{Join, JoinReply, JoinRequest};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use tracing::Instrument as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

/// What the deploying admin holds of one birth before it reports
/// (`docs/architecture/lifecycles/node-create.md` step 2, `docs/architecture/lifecycles/node-mesh-join.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deployed {
    /// The deployed node's `path.name`.
    pub name: PathName,
    /// The deployed node's id.
    pub node_id: NodeId,
    /// The incarnation of the deployed birth.
    pub incarnation: IncarnationId,
    /// The incarnation the birth supersedes, when it restarts a node.
    pub supersedes: Option<IncarnationId>,
    /// The birth's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The exact runtime its provider made available to the birth.
    pub runtime: RuntimeFact,
    /// The birth's data directory.
    pub data_dir: String,
    /// The name this accepting primary last accepted for this exact birth (its held stopped digest), when it
    /// holds the birth: the join of a stopped node renamed while stopped presents it. `None` at a
    /// first start, whose name is the create spec's.
    pub last_accepted_name: Option<PathName>,
    /// The accepting primary holds this birth: the deployment is a later start of a node it admitted
    /// before (a stopped or restarted birth), whose join is reconciled to its row and handed its
    /// row's config; `false` at a first start, admitted under its create spec's name whatever row
    /// stands at that path.name (R-JN1 item 6), and handed its row's config when that row is its own
    /// create's claim. Set by the one that registers the deployment, never
    /// inferred from the last accepted name, which an admin that joined after the stop lacks
    /// (`TopologyView::stopped_unheard`).
    pub held: bool,
    /// The listeners the birth was assigned (Q151): each declared name of its kind on the birth's
    /// address, port 0. Its report carries exactly these names, each on its assigned address with the
    /// port the operating system selected, or it is refused whole.
    pub listeners: Vec<(String, SocketAddr)>,
}

/// A birth's join report as the deploying admin verified it: its digest and the listeners it bound
/// (Q151). What `WaitForBind` and `WaitForMeshJoin` complete from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinReport {
    /// The birth's digest, canonicalized to the name it is admitted under.
    pub digest: MeshDigest,
    /// The listeners the birth bound, by name, each with its selected port.
    pub listeners: Vec<(String, SocketAddr)>,
}

/// The first field of a digest that disagrees with what was deployed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// The name of the field.
    pub field: &'static str,
    /// The value deployed.
    pub deployed: String,
    /// The value the birth reported.
    pub reported: String,
}

impl Deployed {
    /// `Ok` when `d`, reporting `listeners`, is the birth this admin deployed.
    pub fn verify(&self, d: &MeshDigest, listeners: &[(String, SocketAddr)]) -> Result<(), Mismatch> {
        fn same<T: PartialEq + std::fmt::Debug>(field: &'static str, deployed: &T, reported: &T) -> Result<(), Mismatch> {
            if deployed == reported {
                Ok(())
            } else {
                Err(Mismatch { field, deployed: format!("{deployed:?}"), reported: format!("{reported:?}") })
            }
        }
        same("node_id", &self.node_id, &d.node.node_id)?;
        // The name is reconciled apart from identity (node-create.md step 4): a held birth is
        // admitted under its row's name or the name this authority last accepted for it; a first
        // start under its create spec's name only.
        if d.node.name != self.name && self.last_accepted_name.as_ref() != Some(&d.node.name) {
            let deployed = match &self.last_accepted_name {
                Some(last) => format!("row's {:?}, last accepted {:?}", self.name.to_string(), last.to_string()),
                None => format!("{:?}", self.name.to_string()),
            };
            return Err(Mismatch { field: "name", deployed, reported: format!("{:?}", d.node.name.to_string()) });
        }
        same("incarnation", &self.incarnation, &d.node.incarnation)?;
        same("supersedes", &self.supersedes, &d.node.supersedes)?;
        same("endpoint_id", &self.endpoint_id, &d.node.endpoint_id)?;
        same("runtime", &Some(self.runtime.clone()), &d.node.runtime)?;
        same("data_dir", &Some(self.data_dir.clone()), &d.data_dir)?;
        self.verify_listeners(listeners)
    }

    /// The reported listeners against the assigned (Q151; node-create.md step 4): exactly the assigned
    /// names, each once, each on its assigned address with a port that is not 0, as `Bound::of`
    /// requires of the transport. A listener missing, duplicated, undeclared, on another address, on
    /// an unspecified address or on port 0 refuses the report whole: nothing is dropped.
    fn verify_listeners(&self, reported: &[(String, SocketAddr)]) -> Result<(), Mismatch> {
        let show = |l: &[(String, SocketAddr)]| rdm_core::launch::encode_listeners(l);
        let refuse = |why: String| Err(Mismatch { field: "listeners", deployed: show(&self.listeners), reported: format!("{} ({why})", show(reported)) });
        let mut seen = std::collections::BTreeSet::new();
        for (name, addr) in reported {
            if !seen.insert(name.as_str()) {
                return refuse(format!("{name} is reported twice"));
            }
            let Some((_, assigned)) = self.listeners.iter().find(|(n, _)| n == name) else {
                return refuse(format!("{name} was not assigned"));
            };
            if addr.ip().is_unspecified() || addr.port() == 0 {
                return refuse(format!("{name} at {addr} is not an address a peer can dial"));
            }
            if addr.ip() != assigned.ip() || (assigned.port() != 0 && addr.port() != assigned.port()) {
                return refuse(format!("{name} at {addr} is not on its assigned address {assigned}"));
            }
        }
        if let Some((name, _)) = self.listeners.iter().find(|(n, _)| !seen.contains(n.as_str())) {
            return refuse(format!("{name} is missing"));
        }
        Ok(())
    }
}

struct Slot {
    deployed: Deployed,
    reported: watch::Sender<Option<JoinReport>>,
}

/// A deployment whose create ended before its birth reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Abandoned {
    /// The Build whose create deployed the birth.
    pub build_id: String,
    /// The attempt of that Build.
    pub attempt: u32,
    /// What was deployed.
    pub deployed: Deployed,
}

/// How many abandoned deployments an admin remembers by name; the oldest is forgotten first.
const ABANDONED_KEPT: usize = 256;

/// A create's deployment whose birth reported and whose create ended: the admitted birth's validated
/// report, kept for the same deployment's next registration (its Build's next attempt completes
/// `WaitForMeshJoin` from it, never from gossip). Never a held deployment (a restart's, a heard-again's):
/// each of those waits for its own commanded join's report.
#[derive(Debug, Clone)]
struct Admitted {
    deployed: Deployed,
    report: JoinReport,
}

/// How many admitted deployments an admin keeps once their creates ended; the oldest is forgotten first.
const ADMITTED_KEPT: usize = 256;

/// The births this admin deployed and is waiting to hear from, the deployments it ended
/// without hearing from them, and the admitted births whose creates ended.
#[derive(Default)]
pub struct Joins {
    slots: Mutex<HashMap<NodeId, Slot>>,
    abandoned: Mutex<VecDeque<Abandoned>>,
    admitted: Mutex<VecDeque<Admitted>>,
    /// This admin's nodes.storage, where an admitted birth's report is kept durable (its runtime
    /// row) at admission; `None` for a door that keeps none (a cell's).
    runtimes: Option<Arc<dyn NodesStorage>>,
}

/// What a digest is to the births this admin deployed.
#[derive(Debug, PartialEq, Eq)]
pub enum Standing {
    /// The digest is the deployed birth; `name` is its row's, the name it is admitted under.
    Deployed {
        /// The row's name: the accepted digest is canonicalized to it before anything is installed.
        name: PathName,
        /// The authority holds the birth (a later join, reconciled to its row), rather than
        /// admitting its first start under the create spec's name.
        held: bool,
    },
    /// The node is deployed here and the digest disagrees.
    Mismatch(Mismatch),
    /// The digest is a birth this admin deployed and whose create ended without hearing it.
    Abandoned(Abandoned),
    /// This admin deployed no birth of that node.
    Unknown,
}

impl Joins {
    /// `Joins` over this admin's nodes.storage: the admitted birth's report is kept durable there at
    /// admission (the birth's runtime row, node-create.md step 1), so a re-run after this process
    /// restarted completes `WaitForMeshJoin` from it (node-create.md step 5), never from gossip.
    pub fn over(runtimes: Arc<dyn NodesStorage>) -> Self {
        Self { runtimes: Some(runtimes), ..Self::default() }
    }

    /// Keep the admitted birth's validated report durable before it is released: a blind put of the
    /// birth's runtime row (the exact node_id and incarnation, the runtime and data dir the report
    /// carried). Nothing is kept by a door over no storage.
    pub async fn keep_admitted(&self, d: &MeshDigest) -> Result<(), String> {
        let Some(runtimes) = &self.runtimes else { return Ok(()) };
        let runtime = d.node.runtime.clone().ok_or_else(|| format!("{}: the admitted report names no runtime", d.node.name))?;
        let row = RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime, data_dir: d.data_dir.clone() };
        runtimes.put_runtime(&row).await.map_err(|e| format!("{}: the admitted report's runtime row could not be kept: {e}", d.node.name))
    }

    /// The admitted birth's durable report: the runtime row of the exact birth, `None` where this admin
    /// keeps none or no report of that birth was kept.
    pub async fn durable_report(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, String> {
        let Some(runtimes) = &self.runtimes else { return Ok(None) };
        runtimes.runtime(node_id, incarnation).await.map_err(|e| format!("the runtime row of {node_id} incarnation {}: {e}", incarnation.as_str()))
    }

    /// A create of `node_id` is under way at this admin: it holds the deployment the create registered
    /// (a first start, `held: false`), from before the runtime starts until the create ends
    /// (`Joins::end`). A held deployment (a restart's, a heard-again's) is not a create.
    pub fn creating(&self, node_id: &NodeId) -> bool {
        self.slots.lock().unwrap().get(node_id).is_some_and(|s| !s.deployed.held)
    }

    /// Register `deployed` before its runtime starts. The report arrives on the returned
    /// receiver. A registration of the same create's deployment keeps what it already heard: the
    /// report the deployment still held took, or the one an ended create of the same deployment was
    /// handed. A held deployment's registration (a start's, a restart's, a heard-again's) is its
    /// command's own and waits for that command's join: it is handed nothing, not even what an earlier
    /// registration of the same held deployment heard, so the report of the birth's next commanded
    /// join is a join of its own, never a retry of the last one (Q99: one Pending per join).
    pub fn expect(&self, deployed: Deployed) -> watch::Receiver<Option<JoinReport>> {
        let mut slots = self.slots.lock().unwrap();
        if let Some(s) = slots.get(&deployed.node_id) {
            if s.deployed == deployed && !deployed.held {
                return s.reported.subscribe();
            }
        }
        let heard = if deployed.held { None } else { self.admitted.lock().unwrap().iter().find(|a| a.deployed == deployed).map(|a| a.report.clone()) };
        let (tx, rx) = watch::channel(heard);
        slots.insert(deployed.node_id.clone(), Slot { deployed, reported: tx });
        rx
    }

    /// The create of `node_id`'s deployment (`build_id`, `attempt`) ended: stop holding it. A create's
    /// deployment that heard its birth is kept as the admitted birth's report, handed to the same
    /// deployment's next registration (a held deployment's report ends with it); one that never heard
    /// its birth is remembered as abandoned, so a late `JoinNode` is refused by name.
    pub fn end(&self, node_id: &NodeId, build_id: &str, attempt: u32) {
        let Some(slot) = self.slots.lock().unwrap().remove(node_id) else { return };
        let report = slot.reported.borrow().clone();
        if let Some(report) = report {
            if slot.deployed.held {
                return;
            }
            let mut kept = self.admitted.lock().unwrap();
            kept.retain(|a| a.deployed.node_id != *node_id);
            if kept.len() == ADMITTED_KEPT {
                kept.pop_front();
            }
            kept.push_back(Admitted { deployed: slot.deployed, report });
            return;
        }
        let mut kept = self.abandoned.lock().unwrap();
        kept.retain(|a| a.deployed.node_id != *node_id);
        if kept.len() == ABANDONED_KEPT {
            kept.pop_front();
        }
        kept.push_back(Abandoned { build_id: build_id.into(), attempt, deployed: slot.deployed });
    }

    /// How the deploying admin stands toward the birth `d` reports, with the `listeners` it bound.
    pub fn standing(&self, d: &MeshDigest, listeners: &[(String, SocketAddr)]) -> Standing {
        if let Some(s) = self.slots.lock().unwrap().get(&d.node.node_id) {
            return match s.deployed.verify(d, listeners) {
                Ok(()) => Standing::Deployed { name: s.deployed.name.clone(), held: s.deployed.held },
                Err(m) => Standing::Mismatch(m),
            };
        }
        match self.abandoned.lock().unwrap().iter().find(|a| a.deployed.node_id == d.node.node_id) {
            None => Standing::Unknown,
            Some(a) => match a.deployed.verify(d, listeners) {
                Ok(()) => Standing::Abandoned(a.clone()),
                Err(m) => Standing::Mismatch(m),
            },
        }
    }

    /// Whether the registration `d` reports to already heard a report of this exact birth (node id
    /// and incarnation): then `d` is a later report of that join, a retry, never a join of its own
    /// (node-mesh-join.md step 10). Read before [`Self::report`] fills the registration.
    pub fn heard(&self, d: &MeshDigest) -> bool {
        self.slots.lock().unwrap().get(&d.node.node_id).is_some_and(|s| s.reported.borrow().as_ref().is_some_and(|r| r.digest.node.node_id == d.node.node_id && r.digest.node.incarnation == d.node.incarnation))
    }

    /// The node's own report, verified: what `WaitForBind` waits for.
    pub fn report(&self, r: &JoinReport) {
        if let Some(s) = self.slots.lock().unwrap().get(&r.digest.node.node_id) {
            let _ = s.reported.send(Some(r.clone()));
        }
    }
}

/// Fills the admin's side of a join once the admin holds a view.
pub struct JoinDoor {
    /// This admin's `path.name`.
    pub me: PathName,
    /// The births this admin deployed.
    pub joins: Arc<Joins>,
    /// What the admin answers a joiner with.
    pub answer: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<JoinAnswer, String>> + Send>> + Send + Sync>,
    /// Install the reported address for the key: the topology view's `register_location`, the live
    /// resolver, and with it the cancel of every dial aimed at the key's old address.
    pub install: Arc<dyn Fn(&MeshDigest) + Send + Sync>,
    /// Join and persist the declared app types before the birth is admitted.
    pub types: Arc<dyn Fn(MeshDigest, std::collections::BTreeSet<String>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>,
    /// Project the installed birth into the view this admin's status authority decides from, so the
    /// admitted birth is in that view when the join is answered: its first declaration finds its
    /// subject held.
    pub known: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    /// Record and publish the admitted birth once as Pending per join, at its accepted join report and
    /// before the join is answered (node-mesh-join.md step 10): this admin's status authority begins
    /// the birth's next cycle there, at a first start, a start after a stop and a commanded join alike,
    /// so nothing the birth says after its answer precedes it. The second argument is `true` for a new
    /// join and `false` for a retry, a report its registration already heard ([`Joins::heard`]), which
    /// applies as already applied (Q99: per join, not per birth).
    pub pending: Arc<dyn Fn(MeshDigest, bool) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    /// The mesh primary this admin sees, by path.name.
    pub primary: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    /// The signer this admin issues each accepted birth's member cert with, on its rafka-time.
    pub issuer: Arc<crate::certs::CertIssuer>,
    /// Where each admitted birth's cert dates are written on its node record before it is admitted
    /// (fabric-certs.md §Tracking), and a node-admin birth's signing cert dates when this admin minted
    /// the key it holds, through the records this admin holds; `None` for a door that keeps no records
    /// (a testkit admin side).
    pub nodes: Option<(Arc<dyn crate::storage::NodesStorage>, Arc<crate::node_rows::WrittenRows>)>,
    /// The atlas this admin holds its rows in (Q234; every row its writer claims or writes is held
    /// there): the row at a joiner's `path.name` is read from it, never from the store (Q227). The join
    /// answer hands the row's config beside its name. `None` for a door that keeps no records (a
    /// testkit admin side): no row is held at any name.
    pub atlas: Option<Arc<crate::atlas::Atlas>>,
}

/// The door a running admin fills once it holds a view.
pub type JoinSlot = Arc<OnceLock<Arc<JoinDoor>>>;

impl JoinDoor {
    /// Answer a `JoinNode` from `peer`: verify the digest against what was deployed, install its
    /// address and answer with the admin's entry answer.
    pub async fn serve(&self, peer: EndpointId, req: JoinRequest) -> JoinReply {
        let JoinRequest::JoinNode { digest, types, listeners } = req;
        if let Err(kind) = rdm_core::app_types::check(&types) { return rdm_core::reply::join::malformed(kind); }
        let d = MeshDigest::from(digest);
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-join",
            node = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            reported_addr = %d.node.transport_addr,
            listeners = tracing::field::Empty,
            served_by = %self.me,
            caller = tracing::field::Empty,
            admitted_as = tracing::field::Empty,
            config_keys = tracing::field::Empty,
            outcome = tracing::field::Empty,
            build_id = tracing::field::Empty,
            attempt = tracing::field::Empty,
            cert_len = tracing::field::Empty,
        );
        self.decide(peer, d, types.into_iter().collect(), listeners, span.clone()).instrument(span).await
    }

    /// Decide a join on its digest against what was deployed. Nothing checks who is calling
    /// (node-rpc.md §14): the caller's endpoint names the call in its span, and the digest's
    /// endpoint is what the deployment verifies.
    async fn decide(&self, peer: EndpointId, mut d: MeshDigest, types: std::collections::BTreeSet<String>, listeners: Vec<(String, SocketAddr)>, span: tracing::Span) -> JoinReply {
        span.record("caller", peer.0.as_str());
        span.record("listeners", rdm_core::launch::encode_listeners(&listeners).as_str());
        match self.joins.standing(&d, &listeners) {
            Standing::Mismatch(m) => {
                span.record("outcome", "mismatch");
                tracing::info_span!("rdm.node_admin.node.reject.via-join-mismatch", node = %d.node.name, field = m.field, deployed = %m.deployed, reported = %m.reported)
                    .in_scope(|| tracing::warn!("the reported digest disagrees with the deployment"));
                rdm_core::reply::join::join_mismatch(m.field.into(), m.deployed, m.reported)
            }
            Standing::Deployed { name, held } => {
                // Canonicalized to the row's name before anything is issued or installed: the
                // reported name, when it is the last accepted one, is never admitted (R-JN1).
                d.node.name = name;
                span.record("admitted_as", tracing::field::display(&d.node.name));
                // The row's config, handed beside its name and installed on the digest: the row as this
                // admin's atlas holds it (Q234), never read from the store (Q227). A first start
                // is admitted under its create spec's name (R-JN1 item 6), and its row there is its own
                // create's claim, written before it was launched (node-create.md step 1) and seeded
                // from the previous row at that path.name (node-recovery.md step 4): it is handed that
                // config at its join, before it publishes anything or declares readiness (node-start.md
                // step 2). Another node's row at a first start's name (a create that wrote no claim, a
                // predecessor's row) is handed nothing and refuses nothing; a held birth's join is
                // reconciled to its row, and another node's row there is refused by name.
                let config = match self.atlas.as_ref().and_then(|a| a.record_for_join(&d.node.name, &d.node.node_id)) {
                    None => None,
                    // The caller's own record deleted: a caller whose record is deleted is deleted, its
                    // runtime terminated, never re-admitted (node-investigation.md; Q217). Refused by name
                    // and decided as every refused caller is (Q207), never issued a cert or admitted.
                    Some(row) if row.node_id == d.node.node_id && row.status == crate::model::NodeStatus::Deleted => {
                        span.record("outcome", "mismatch");
                        let m = Mismatch { field: "status", deployed: format!("the row of node {} at {} is deleted", row.node_id, row.name), reported: format!("node {} incarnation {} joining as {}", d.node.node_id, d.node.incarnation.as_str(), d.node.name) };
                        tracing::info_span!("rdm.node_admin.node.reject.via-join-mismatch", node = %d.node.name, field = m.field, deployed = %m.deployed, reported = %m.reported)
                            .in_scope(|| tracing::warn!("a birth whose record is deleted asked to join: deleted, never re-admitted"));
                        if let Some(atlas) = &self.atlas {
                            atlas.refused("join", &d.node.node_id, &d.node.incarnation);
                        }
                        return rdm_core::reply::join::join_mismatch(m.field.into(), m.deployed, m.reported);
                    }
                    Some(row) if row.node_id == d.node.node_id => {
                        if let Some(held) = row.app_types.as_ref().filter(|held| held.incarnation == d.node.incarnation) {
                            if held.types != types {
                                return rdm_core::reply::join::join_mismatch("types".into(), format!("{:?}", held.types), format!("{types:?}"));
                            }
                        }
                        Some(row.config)
                    },
                    Some(_) if !held => None,
                    Some(row) => {
                        span.record("outcome", "mismatch");
                        let m = Mismatch { field: "name", deployed: format!("the row at {} holds node {}", row.name, row.node_id), reported: format!("node {} as {}", d.node.node_id, d.node.name) };
                        tracing::info_span!("rdm.node_admin.node.reject.via-join-mismatch", node = %d.node.name, field = m.field, deployed = %m.deployed, reported = %m.reported)
                            .in_scope(|| tracing::warn!("the row at the admitted name belongs to another node"));
                        // A caller birth this admin does not hold (Q207; node-investigation.md: "A node
                        // that comes back after it was re-created is such a caller"): refused by name,
                        // then decided by its path.name and this admin's atlas, on its own task.
                        if let Some(atlas) = &self.atlas {
                            atlas.refused("join", &d.node.node_id, &d.node.incarnation);
                        }
                        return rdm_core::reply::join::join_mismatch(m.field.into(), m.deployed, m.reported);
                    }
                };
                span.record("config_keys", config.as_ref().map_or(0, |c| c.len()) as u64);
                // The member cert is issued for this exact birth BEFORE it is admitted: a refused
                // issuance leaves nothing installed for its key and completes no report.
                let birth = crate::certs::BirthIdentity { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), mesh: d.node.name.mesh.clone(), endpoint_key: d.node.endpoint_id.clone() };
                let issue = match self.issuer.issue_member(&birth) {
                    Ok(c) => c,
                    Err(crate::certs::IssueFailure::NoRafkaTime(why)) => {
                        span.record("outcome", "not-ready");
                        return rdm_core::reply::join::not_ready(format!("{}: {why}", self.me));
                    }
                    Err(crate::certs::IssueFailure::Refused { error, detail }) => {
                        span.record("outcome", "cert-refused");
                        return rdm_core::reply::join::cert_refused(error.as_str().to_string(), detail);
                    }
                };
                // The join admits the birth holding the cert: its dates, the receipt (Q141), are written
                // on its node record before the birth is admitted; a node-admin's signing cert dates
                // beside them when this admin minted the key it was launched with, and the listeners the
                // validated report carries (Q151), in the same write. A write refused leaves nothing
                // installed, and the join is retried.
                if let (Some((storage, written)), true) = (&self.nodes, !issue.bytes.is_empty() || !listeners.is_empty()) {
                    let dates = (!issue.bytes.is_empty()).then(|| crate::cert_renewal::dates_of(&issue));
                    let signing = (!issue.bytes.is_empty() && birth.name.kind == crate::model::NodeKind::NodeAdmin).then(|| self.issuer.held_mint(&birth.mesh)).flatten().map(|m| crate::cert_renewal::signing_dates(&m));
                    let base = crate::storage::NodeRow::new(birth.name.clone(), birth.node_id.clone(), crate::model::NodeStatus::Pending);
                    let put = written.update(&**storage, &birth.node_id, Some(base), |r| {
                        let b = r.birth_mut(&birth.incarnation, &birth.name);
                        if let Some(dates) = dates {
                            b.cert = Some(dates);
                        }
                        if let Some(s) = signing {
                            b.signing_cert = Some(s);
                        }
                        b.listeners = listeners.clone();
                    });
                    if let Err(why) = put.await {
                        span.record("outcome", "not-ready");
                        return rdm_core::reply::join::not_ready(format!("{}: {} ({}): {why}", self.me, birth.name, birth.node_id));
                    }
                }
                let cert = issue.bytes;
                span.record("cert_len", cert.len() as u64);
                if let Err(why) = (self.types)(d.clone(), types).await {
                    return rdm_core::reply::join::not_ready(format!("{}: the app declaration could not be joined and recorded: {why}", self.me));
                }
                // The validated report is kept durable before it is released (node-create.md step 5):
                // a re-run of the create after this process restarted completes WaitForMeshJoin from
                // it. A put refused installs nothing, and the join is retried.
                if let Err(why) = self.joins.keep_admitted(&d).await {
                    span.record("outcome", "not-ready");
                    return rdm_core::reply::join::not_ready(format!("{}: {why}", self.me));
                }
                (self.install)(&d);
                (self.known)().await;
                // A report its registration already heard is a retry of that join; any other accepted
                // report is a new join, whose Pending is its own (Q99).
                let new_join = !self.joins.heard(&d);
                (self.pending)(d.clone(), new_join).await;
                self.joins.report(&JoinReport { digest: d.clone(), listeners });
                span.record("outcome", "installed");
                tracing::info!(addr = %d.node.transport_addr, "the deployed birth reported where it bound: the address is installed for its key");
                self.joined(&d.node.name, cert, config).await
            }
            Standing::Abandoned(a) => {
                span.record("outcome", "deployment-abandoned");
                span.record("build_id", a.build_id.as_str());
                span.record("attempt", a.attempt);
                tracing::warn!(build_id = %a.build_id, attempt = a.attempt, node_id = %a.deployed.node_id, incarnation = %a.deployed.incarnation.as_str(), "this admin ended the deployment of this birth before it reported");
                rdm_core::reply::join::deployment_abandoned(a.build_id, a.attempt, a.deployed.node_id, a.deployed.incarnation)
            }
            Standing::Unknown => {
                span.record("outcome", "not-authority");
                let primary = (self.primary)();
                tracing::info_span!("rdm.node_admin.node.reject.via-join-unknown", node = %d.node.name, node_id = %d.node.node_id, primary = ?primary)
                    .in_scope(|| tracing::warn!("this admin deployed no such birth and holds no member of it"));
                rdm_core::reply::join::not_primary(primary)
            }
        }
    }

    async fn joined(&self, node: &PathName, member_cert: Vec<u8>, config: Option<std::collections::BTreeMap<String, String>>) -> JoinReply {
        let answer = match (self.answer)().await {
            Ok(mut a) => {
                a.control.member_cert = member_cert;
                a.control.name = Some(node.clone());
                a.control.config = config;
                a
            }
            Err(why) => return rdm_core::reply::join::not_ready(format!("{}: not ready to answer a join: {why}", self.me)),
        };
        tracing::info_span!("rdm.mesh.entry.serve.via-pull", node = %node, served_by = %answer.served_by, statuses = answer.statuses.len())
            .in_scope(|| tracing::info!("entry answered"));
        match crate::wire::answer_to_wire(&answer) {
            Ok(bytes) => JoinReply::Joined { answer: bytes },
            Err(e) => rdm_core::reply::join::not_ready(format!("{}: its answer does not encode: {e}", self.me)),
        }
    }
}

/// Serve `JoinNode` on this admin. Until `slot` is filled a join is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: JoinSlot) -> ServerBuilder {
    b.serve::<Join, _, _>(OpOwner::Product("rdm".into()), move |peer: rdm_transport::PeerContext, req: JoinRequest| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::join::not_ready("this admin holds no view yet".into()));
            };
            Ok(door.serve(EndpointId(peer.endpoint_id.to_string()), req).await)
        }
    })
}

pub use rdm_node::join::{call_join_to, JoinFailure};

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::{MemberStatus, MeshNode};

    fn fact() -> RuntimeFact {
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap()
    }

    fn deployed() -> Deployed {
        Deployed {
            name: "mesh1.rpc.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            endpoint_id: EndpointId("k".into()),
            runtime: fact(),
            data_dir: "/d".into(),
            last_accepted_name: None,
            held: false,
            listeners: Vec::new(),
        }
    }

    fn digest_of(d: &Deployed) -> MeshDigest {
        MeshDigest { held_hash: Default::default(),
            fabric_id: rdm_core::FabricId::mint(),
            node: MeshNode {
                node_id: d.node_id.clone(),
                name: d.name.clone(),
                endpoint_id: d.endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: d.incarnation.clone(),
                supersedes: d.supersedes.clone(),
                runtime: Some(d.runtime.clone()),
            },
            status: MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(d.data_dir.clone()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None, is_primary: None, is_fabric_primary: None
        }
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_that_matches_the_deployment_is_deployed_and_is_reported() {
        let joins = Joins::default();
        let dep = deployed();
        let rx = joins.expect(dep.clone());
        let d = digest_of(&dep);
        assert_eq!(joins.standing(&d, &[]), Standing::Deployed { name: dep.name.clone(), held: false });
        joins.report(&JoinReport { digest: d.clone(), listeners: Vec::new() });
        assert_eq!(rx.borrow().as_ref().map(|r| r.digest.node.transport_addr), Some(d.node.transport_addr), "the address the node reported is the one the admin publishes");
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_with_another_incarnation_is_refused_by_the_field_with_both_values() {
        let joins = Joins::default();
        let dep = deployed();
        joins.expect(dep.clone());
        let mut d = digest_of(&dep);
        d.node.incarnation = IncarnationId::mint();
        match joins.standing(&d, &[]) {
            Standing::Mismatch(m) => {
                assert_eq!(m.field, "incarnation");
                assert!(m.deployed.contains(&dep.incarnation.as_str()) && m.reported.contains(&d.node.incarnation.as_str()));
            }
            other => panic!("{other:?}"),
        }
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_with_another_node_id_is_not_a_deployed_birth() {
        let joins = Joins::default();
        let dep = deployed();
        joins.expect(dep.clone());
        let mut d = digest_of(&dep);
        d.node.node_id = NodeId::mint();
        assert_eq!(joins.standing(&d, &[]), Standing::Unknown);
    }

    /// The deployment of a birth this authority holds, stopped as `mesh1.rpc.1` and renamed in its
    /// row to `mesh1.rpc.1.old` while stopped (R-JN1).
    fn renamed_while_stopped() -> Deployed {
        Deployed { name: "mesh1.rpc.1.old".parse().unwrap(), last_accepted_name: Some("mesh1.rpc.1".parse().unwrap()), held: true, ..deployed() }
    }

    // @feature: node-lifecycle
    /// CONTRACT: a held birth's join is admitted under the row's name or the name its authority
    /// last accepted for it; any other name is refused by name naming the reported, the row's and
    /// the last accepted value. A first start (no held name) keeps the create spec's one name.
    #[test]
    fn a_held_births_join_accepts_the_rows_or_the_last_accepted_name_and_refuses_any_other_naming_all_three() {
        let dep = renamed_while_stopped();
        let mut held = digest_of(&dep);
        held.node.name = "mesh1.rpc.1".parse().unwrap();
        assert_eq!(dep.verify(&held, &[]), Ok(()), "the name last accepted for this exact birth is admitted");
        assert_eq!(dep.verify(&digest_of(&dep), &[]), Ok(()), "the row's name is admitted");
        let mut other = digest_of(&dep);
        other.node.name = "mesh1.rpc.7".parse().unwrap();
        let m = dep.verify(&other, &[]).unwrap_err();
        assert_eq!(m.field, "name");
        assert!(m.reported.contains("mesh1.rpc.7") && m.deployed.contains("mesh1.rpc.1.old") && m.deployed.contains("last accepted") && m.deployed.contains("\"mesh1.rpc.1\""), "the refusal names all three: {m:?}");
        let first = deployed();
        let mut off_spec = digest_of(&first);
        off_spec.node.name = "mesh1.rpc.1.old".parse().unwrap();
        assert_eq!(first.verify(&off_spec, &[]).unwrap_err().field, "name", "a first start off its create spec is still refused");
    }

    // @feature: node-lifecycle
    /// CONTRACT: a replacement deployed under the stopped node's old name is another birth: the
    /// stopped node's join is still resolved by its exact birth and admitted, and the replacement's
    /// deployment is untouched.
    #[test]
    fn a_replacement_under_the_old_name_does_not_redirect_the_stopped_nodes_admission() {
        let joins = Joins::default();
        let stopped = renamed_while_stopped();
        joins.expect(stopped.clone());
        let replacement = Deployed { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), endpoint_id: EndpointId("k2".into()), ..deployed() };
        let replacement_rx = joins.expect(replacement.clone());
        let mut d = digest_of(&stopped);
        d.node.name = "mesh1.rpc.1".parse().unwrap();
        assert!(matches!(joins.standing(&d, &[]), Standing::Deployed { .. }), "the stopped birth is admitted by its node id, whatever name it presents");
        joins.report(&JoinReport { digest: d.clone(), listeners: Vec::new() });
        assert!(replacement_rx.borrow().is_none(), "the replacement's deployment heard nothing of the stopped node's join");
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_with_another_runtime_or_data_dir_or_key_is_refused_by_its_field() {
        let dep = deployed();
        let mut other_fact = digest_of(&dep);
        other_fact.node.runtime = Some(RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-2")).unwrap());
        assert_eq!(dep.verify(&other_fact, &[]).unwrap_err().field, "runtime");
        let mut dir = digest_of(&dep);
        dir.data_dir = Some("/elsewhere".into());
        assert_eq!(dep.verify(&dir, &[]).unwrap_err().field, "data_dir");
        let mut key = digest_of(&dep);
        key.node.endpoint_id = EndpointId("other".into());
        assert_eq!(dep.verify(&key, &[]).unwrap_err().field, "endpoint_id");
    }

    /// CONTRACT (node-create.md step 5: the accepting authority "completes WaitForBind /
    /// WaitForMeshJoin from the validated report, never from gossip"; [`Joins::expect`]: "A
    /// registration of the same deployment keeps what it already heard"): once the create that
    /// registered a deployment ends, the report its birth made stays the authority's record of the
    /// admitted birth, and the same deployment's next registration (the next attempt of its Build)
    /// is handed that report; the ended create holds the deployment no more, and another deployment
    /// of the node (its restart, held) is handed nothing. Must NOT happen: the report dropped with
    /// the create, so the Build's next attempt waits for a JoinNode the admitted birth never sends
    /// again.
    /// CONTRACT (Q87: a birth with a lifecycle command of its authority in flight is that command's,
    /// never commanded back; node-create.md step 3: the create composes the whole node.start): a node
    /// whose create this admin runs is creating from the deployment's registration until the create
    /// ends, its report heard or not; a held deployment (a restart, a heard-again) is not a create.
    // @feature: node-lifecycle
    #[test]
    fn a_node_is_creating_while_this_admin_holds_the_deployment_its_create_registered() {
        let joins = Joins::default();
        let dep = deployed();
        assert!(!joins.creating(&dep.node_id), "no deployment registered: no create under way");
        let _rx = joins.expect(dep.clone());
        assert!(joins.creating(&dep.node_id), "the create registered its deployment: creating");
        joins.report(&JoinReport { digest: digest_of(&dep), listeners: Vec::new() });
        assert!(joins.creating(&dep.node_id), "its report heard, the create still runs: creating");
        joins.end(&dep.node_id, "3qcfk1kq47m9", 1);
        assert!(!joins.creating(&dep.node_id), "the create ended: not creating");
        joins.expect(Deployed { held: true, last_accepted_name: Some(dep.name.clone()), ..dep.clone() });
        assert!(!joins.creating(&dep.node_id), "a held deployment (a restart, a heard-again) is not a create");
    }

    // @feature: node-lifecycle
    #[test]
    fn an_ended_create_keeps_the_admitted_report_for_the_same_deployments_next_registration() {
        let joins = Joins::default();
        let dep = deployed();
        let rx = joins.expect(dep.clone());
        let d = digest_of(&dep);
        joins.report(&JoinReport { digest: d.clone(), listeners: Vec::new() });
        drop(rx);
        joins.end(&dep.node_id, "3qcfk1kq47m9", 1);
        assert_eq!(joins.standing(&d, &[]), Standing::Unknown, "an ended create holds the deployment no more");
        let again = joins.expect(dep.clone());
        assert_eq!(again.borrow().as_ref().map(|r| r.digest.node.transport_addr), Some(d.node.transport_addr), "the same deployment's next registration is handed the admitted report");
        let restart = Deployed { held: true, last_accepted_name: Some(dep.name.clone()), ..dep.clone() };
        assert!(joins.expect(restart.clone()).borrow().is_none(), "another deployment of the node is handed no report");
        // A held deployment's report ends with its registration: a later registration of the same
        // held deployment (a heard-again of the live birth) waits for its own commanded join.
        joins.report(&JoinReport { digest: d.clone(), listeners: Vec::new() });
        joins.end(&dep.node_id, "3qcfk1kq47m9", 2);
        assert!(joins.expect(restart).borrow().is_none(), "a held deployment's report is not kept for its next registration");
    }
}
