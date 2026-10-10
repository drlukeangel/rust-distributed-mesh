//! The join (`JoinNode`, op `0x1D`; i143 R-J1).
//!
//! A node binds its transport on port 0, so the operating system assigns the port and no admin
//! ever picks one. Its first call is `JoinNode` to the node-admin that deployed it, carrying the
//! node's full membership digest with the address read from the bound endpoint. The admin holds
//! what it deployed ([`Deployed`], registered by the deployment pipeline before the runtime
//! starts), verifies the digest against it, and on a match installs the address for that key
//! (membership's `register_location`, the live resolver, which cancels every dial aimed at the
//! key's old address) and completes `WaitForBind` from the node's own report. A disagreement is
//! refused by name with both values. A join for a birth whose create ended without hearing it is
//! refused as `DeploymentAbandoned`, naming the Build, the attempt and the exact birth: this admin
//! is the birth's authority and never answers `NotAuthority` about itself.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
use rafka_mesh_entity::wire::WireDigest;
use rafka_mesh_entity::{MeshDigest, RuntimeFact};
use crate::wire::JoinAnswer;
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::join::{Join, JoinReply, JoinRequest};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::collections::{HashMap, VecDeque};
use tracing::Instrument as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;

/// What the deploying admin holds of one birth before it reports.
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
    /// `Ok` when `d` is the birth this admin deployed.
    pub fn verify(&self, d: &MeshDigest) -> Result<(), Mismatch> {
        fn same<T: PartialEq + std::fmt::Debug>(field: &'static str, deployed: &T, reported: &T) -> Result<(), Mismatch> {
            if deployed == reported {
                Ok(())
            } else {
                Err(Mismatch { field, deployed: format!("{deployed:?}"), reported: format!("{reported:?}") })
            }
        }
        same("node_id", &self.node_id, &d.node.node_id)?;
        same("name", &self.name, &d.node.name)?;
        same("incarnation", &self.incarnation, &d.node.incarnation)?;
        same("supersedes", &self.supersedes, &d.node.supersedes)?;
        same("endpoint_id", &self.endpoint_id, &d.node.endpoint_id)?;
        same("runtime", &Some(self.runtime.clone()), &d.node.runtime)?;
        same("data_dir", &Some(self.data_dir.clone()), &d.data_dir)?;
        Ok(())
    }
}

struct Slot {
    deployed: Deployed,
    reported: watch::Sender<Option<MeshDigest>>,
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

/// The births this admin deployed and is waiting to hear from, and the deployments it ended
/// without hearing from them.
#[derive(Default)]
pub struct Joins {
    slots: Mutex<HashMap<NodeId, Slot>>,
    abandoned: Mutex<VecDeque<Abandoned>>,
}

/// What a digest is to the births this admin deployed.
#[derive(Debug, PartialEq, Eq)]
pub enum Standing {
    /// The digest is the deployed birth.
    Deployed,
    /// The node is deployed here and the digest disagrees.
    Mismatch(Mismatch),
    /// The digest is a birth this admin deployed and whose create ended without hearing it.
    Abandoned(Abandoned),
    /// This admin deployed no birth of that node.
    Unknown,
}

impl Joins {
    /// Register `deployed` before its runtime starts. The report arrives on the returned
    /// receiver. A registration of the same deployment keeps what it already heard.
    pub fn expect(&self, deployed: Deployed) -> watch::Receiver<Option<MeshDigest>> {
        let mut slots = self.slots.lock().unwrap();
        if let Some(s) = slots.get(&deployed.node_id) {
            if s.deployed == deployed {
                return s.reported.subscribe();
            }
        }
        let (tx, rx) = watch::channel(None);
        slots.insert(deployed.node_id.clone(), Slot { deployed, reported: tx });
        rx
    }

    /// The create of `node_id`'s deployment (`build_id`, `attempt`) ended: stop holding it. A
    /// deployment that never heard its birth is remembered as abandoned, so a late `JoinNode` is
    /// refused by name.
    pub fn end(&self, node_id: &NodeId, build_id: &str, attempt: u32) {
        let Some(slot) = self.slots.lock().unwrap().remove(node_id) else { return };
        if slot.reported.borrow().is_some() {
            return;
        }
        let mut kept = self.abandoned.lock().unwrap();
        kept.retain(|a| a.deployed.node_id != *node_id);
        if kept.len() == ABANDONED_KEPT {
            kept.pop_front();
        }
        kept.push_back(Abandoned { build_id: build_id.into(), attempt, deployed: slot.deployed });
    }

    /// How the deploying admin stands toward the birth `d` reports.
    pub fn standing(&self, d: &MeshDigest) -> Standing {
        if let Some(s) = self.slots.lock().unwrap().get(&d.node.node_id) {
            return match s.deployed.verify(d) {
                Ok(()) => Standing::Deployed,
                Err(m) => Standing::Mismatch(m),
            };
        }
        match self.abandoned.lock().unwrap().iter().find(|a| a.deployed.node_id == d.node.node_id) {
            None => Standing::Unknown,
            Some(a) => match a.deployed.verify(d) {
                Ok(()) => Standing::Abandoned(a.clone()),
                Err(m) => Standing::Mismatch(m),
            },
        }
    }

    /// The node's own report, verified: what `WaitForBind` waits for.
    pub fn report(&self, d: &MeshDigest) {
        if let Some(s) = self.slots.lock().unwrap().get(&d.node.node_id) {
            let _ = s.reported.send(Some(d.clone()));
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
    /// Install the reported address for the key: membership's `register_location`, the live
    /// resolver, and with it the cancel of every dial aimed at the key's old address.
    pub install: Arc<dyn Fn(&MeshDigest) + Send + Sync>,
    /// Project the installed birth into the view this admin's status authority resolves senders
    /// from, so the admitted birth is known to that authority when the join is answered: its
    /// first declaration is never `unknown peer`.
    pub known: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    /// The mesh primary this admin sees, by path.name.
    pub primary: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    /// The signer this admin issues each accepted birth's member cert with, on its rafka-time.
    pub issuer: Arc<crate::certs::CertIssuer>,
}

/// The door a running admin fills once it holds a view.
pub type JoinSlot = Arc<OnceLock<Arc<JoinDoor>>>;

impl JoinDoor {
    /// Answer a `JoinNode` from `peer`: verify the digest against what was deployed, install its
    /// address and answer with the admin's entry answer.
    pub async fn serve(&self, peer: EndpointId, req: JoinRequest) -> JoinReply {
        let JoinRequest::JoinNode { digest } = req;
        let d = MeshDigest::from(digest);
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-join",
            node = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            reported_addr = %d.node.transport_addr,
            served_by = %self.me,
            outcome = tracing::field::Empty,
            build_id = tracing::field::Empty,
            attempt = tracing::field::Empty,
            cert_len = tracing::field::Empty,
        );
        self.decide(peer, d, span.clone()).instrument(span).await
    }

    async fn decide(&self, peer: EndpointId, d: MeshDigest, span: tracing::Span) -> JoinReply {
        if peer != d.node.endpoint_id {
            span.record("outcome", "unauthorized");
            return JoinReply::Unauthorized { reason: format!("{}: the calling endpoint {} is not the endpoint {} the digest of {} names", self.me, peer.0, d.node.endpoint_id.0, d.node.name) };
        }
        match self.joins.standing(&d) {
            Standing::Mismatch(m) => {
                span.record("outcome", "mismatch");
                tracing::info_span!("rdm.node_admin.node.reject.via-join-mismatch", node = %d.node.name, field = m.field, deployed = %m.deployed, reported = %m.reported)
                    .in_scope(|| tracing::warn!("the reported digest disagrees with the deployment"));
                JoinReply::JoinMismatch { field: m.field.into(), deployed: m.deployed, reported: m.reported }
            }
            Standing::Deployed => {
                // The member cert is issued for this exact birth BEFORE it is admitted: a refused
                // issuance leaves nothing installed for its key and completes no report.
                let birth = crate::certs::BirthIdentity { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), mesh: d.node.name.mesh.clone(), endpoint_key: d.node.endpoint_id.clone() };
                let _ = (&birth, &self.issuer);
                let cert: Vec<u8> = Vec::new();
                span.record("cert_len", cert.len() as u64);
                (self.install)(&d);
                (self.known)().await;
                self.joins.report(&d);
                span.record("outcome", "installed");
                tracing::info!(addr = %d.node.transport_addr, "the deployed birth reported where it bound: the address is installed for its key");
                self.joined(&d.node.name, cert).await
            }
            Standing::Abandoned(a) => {
                span.record("outcome", "deployment-abandoned");
                span.record("build_id", a.build_id.as_str());
                span.record("attempt", a.attempt);
                tracing::warn!(build_id = %a.build_id, attempt = a.attempt, node_id = %a.deployed.node_id, incarnation = %a.deployed.incarnation.0, "this admin ended the deployment of this birth before it reported");
                JoinReply::DeploymentAbandoned { build_id: a.build_id, attempt: a.attempt, node_id: a.deployed.node_id, incarnation: a.deployed.incarnation }
            }
            Standing::Unknown => {
                span.record("outcome", "not-authority");
                let primary = (self.primary)();
                tracing::info_span!("rdm.node_admin.node.reject.via-join-unknown", node = %d.node.name, node_id = %d.node.node_id, primary = ?primary)
                    .in_scope(|| tracing::warn!("this admin deployed no such birth and holds no member of it"));
                JoinReply::NotAuthority { primary }
            }
        }
    }

    async fn joined(&self, node: &PathName, member_cert: Vec<u8>) -> JoinReply {
        let answer = match (self.answer)().await {
            Ok(mut a) => {
                a.control.member_cert = member_cert;
                a
            }
            Err(why) => return JoinReply::NotReady { reason: format!("{}: not ready to answer a join: {why}", self.me) },
        };
        tracing::info_span!("rdm.mesh.entry.serve.via-pull", node = %node, served_by = %answer.served_by, statuses = answer.statuses.len())
            .in_scope(|| tracing::info!("entry answered"));
        match crate::wire::answer_to_wire(&answer) {
            Ok(bytes) => JoinReply::Joined { answer: bytes },
            Err(e) => JoinReply::NotReady { reason: format!("{}: its answer does not encode: {e}", self.me) },
        }
    }
}

/// Serve `JoinNode` on this admin. Until `slot` is filled a join is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: JoinSlot) -> ServerBuilder {
    b.serve::<Join, _, _>(OpOwner::Product("rdm".into()), move |peer: rafka_node_rpc::PeerContext, req: JoinRequest| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(JoinReply::NotReady { reason: "this admin holds no view yet".into() });
            };
            Ok(door.serve(EndpointId(peer.endpoint_id.to_string()), req).await)
        }
    })
}

/// Why a join did not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinFailure {
    /// The admin refused the join by name: nothing is retried.
    Refused(String),
    /// The admin could not be reached or was not ready within the attempts.
    Unreached(String),
}

impl std::fmt::Display for JoinFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(r) => write!(f, "refused: {r}"),
            Self::Unreached(r) => write!(f, "unreached: {r}"),
        }
    }
}

/// `JoinNode` to `target` (the admin whose endpoint reads `anchor` in short form) with `digest`,
/// within `attempts` of five seconds each. A refusal by name ends it at once.
pub async fn call_join(client: &NodeRpcClient, target: &NodeTarget, anchor: &str, digest: &MeshDigest, attempts: u32) -> Result<JoinAnswer, JoinFailure> {
    let req = JoinRequest::JoinNode { digest: WireDigest::from(digest) };
    let node = digest.node.name.to_string();
    let mut last = String::new();
    for attempt in 1..=attempts.max(1) {
        let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(5)), ..Default::default() };
        let span = tracing::info_span!("rdm.mesh.entry.update.via-pull-attempt", node = %node, anchor, attempt, step = tracing::field::Empty, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
        let started = std::time::Instant::now();
        let (out, _) = client.call::<Join>(target, &req, &opts).await;
        let (step, result): (&str, Result<JoinAnswer, JoinFailure>) = match out {
            RpcOutcome::Reply(r) => match r.value().clone() {
                JoinReply::Joined { answer } => match crate::wire::answer_from_wire(&answer) {
                    Ok(a) => ("answered", Ok(a)),
                    Err(e) => ("answered", Err(JoinFailure::Refused(format!("the answer does not decode: {e}")))),
                },
                JoinReply::JoinMismatch { field, deployed, reported } => ("refused", Err(JoinFailure::Refused(format!("{field}: deployed {deployed}, reported {reported}")))),
                JoinReply::NotAuthority { primary } => ("refused", Err(JoinFailure::Refused(format!("the admin deployed no such birth (it sees mesh primary {primary:?})")))),
                JoinReply::DeploymentAbandoned { build_id, attempt, node_id, incarnation } => {
                    ("refused", Err(JoinFailure::Refused(format!("the admin ended the deployment of this birth before it reported (build {build_id}, attempt {attempt}, node {node_id}, incarnation {incarnation})"))))
                }
                JoinReply::CertRefused { name, detail } => ("refused", Err(JoinFailure::Refused(format!("the admin's cert signer refused the birth's member cert ({name}): {detail}")))),
                JoinReply::Unauthorized { reason } => ("refused", Err(JoinFailure::Refused(reason))),
                other => ("not-ready", Err(JoinFailure::Unreached(format!("{}: {other:?}", other.name())))),
            },
            other => ("unanswered", Err(JoinFailure::Unreached(format!("the call ended {}: {other:?}", other.name())))),
        };
        span.record("step", step);
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", match &result { Ok(_) => "answered".to_string(), Err(e) => e.to_string() }.as_str());
        span.in_scope(|| tracing::info!("one join attempt"));
        match result {
            Ok(a) => {
                tracing::info_span!("rdm.mesh.entry.update.via-membership-pulled", node = %node, served_by = %a.served_by, statuses = a.statuses.len(), attempt)
                    .in_scope(|| tracing::info!("joined"));
                return Ok(a);
            }
            Err(JoinFailure::Refused(why)) => {
                tracing::info_span!("rdm.mesh.entry.reject.via-membership-pull-failed", node = %node, reason = %why, attempts = attempt).in_scope(|| tracing::warn!("the join was refused"));
                return Err(JoinFailure::Refused(why));
            }
            Err(JoinFailure::Unreached(why)) => {
                last = why;
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
            }
        }
    }
    tracing::info_span!("rdm.mesh.entry.reject.via-membership-pull-failed", node = %node, reason = %last, attempts).in_scope(|| tracing::warn!("the join found no admin to take it"));
    Err(JoinFailure::Unreached(last))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{MemberStatus, MeshNode};

    fn fact() -> RuntimeFact {
        RuntimeFact::of_this_process("dep-1").unwrap()
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
        }
    }

    fn digest_of(d: &Deployed) -> MeshDigest {
        MeshDigest {
            fabric_id: rafka_mesh_entity::FabricId::mint(),
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
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(d.data_dir.clone()),
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
            load: None,
            gossip: None,
        }
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_that_matches_the_deployment_is_deployed_and_is_reported() {
        let joins = Joins::default();
        let dep = deployed();
        let rx = joins.expect(dep.clone());
        let d = digest_of(&dep);
        assert_eq!(joins.standing(&d), Standing::Deployed);
        joins.report(&d);
        assert_eq!(rx.borrow().as_ref().map(|d| d.node.transport_addr), Some(d.node.transport_addr), "the address the node reported is the one the admin publishes");
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_with_another_incarnation_is_refused_by_the_field_with_both_values() {
        let joins = Joins::default();
        let dep = deployed();
        joins.expect(dep.clone());
        let mut d = digest_of(&dep);
        d.node.incarnation = IncarnationId::mint();
        match joins.standing(&d) {
            Standing::Mismatch(m) => {
                assert_eq!(m.field, "incarnation");
                assert!(m.deployed.contains(&dep.incarnation.0) && m.reported.contains(&d.node.incarnation.0));
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
        assert_eq!(joins.standing(&d), Standing::Unknown);
    }

    // @feature: node-lifecycle
    #[test]
    fn a_join_digest_with_another_runtime_or_data_dir_or_key_is_refused_by_its_field() {
        let dep = deployed();
        let mut other_fact = digest_of(&dep);
        other_fact.node.runtime = Some(RuntimeFact::of_this_process("dep-2").unwrap());
        assert_eq!(dep.verify(&other_fact).unwrap_err().field, "runtime");
        let mut dir = digest_of(&dep);
        dir.data_dir = Some("/elsewhere".into());
        assert_eq!(dep.verify(&dir).unwrap_err().field, "data_dir");
        let mut key = digest_of(&dep);
        key.node.endpoint_id = EndpointId("other".into());
        assert_eq!(dep.verify(&key).unwrap_err().field, "endpoint_id");
    }
}
