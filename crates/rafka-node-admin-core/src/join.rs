//! The join (`JoinNode`, op `0x1D`; i143 R-J1).
//!
//! A node binds its transport on port 0, so the operating system assigns the port and no admin
//! ever picks one. Its first call is `JoinNode` to the node-admin that deployed it, carrying the
//! node's full membership digest with the address read from the bound endpoint. The admin holds
//! what it deployed ([`Deployed`], registered by the deployment pipeline before the runtime
//! starts), verifies the digest against it, and on a match installs the address for that key
//! (membership's `register_location`, the live resolver, which cancels every dial aimed at the
//! key's old address) and completes `WaitForBind` from the node's own report. A disagreement is
//! refused by name with both values.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
use rafka_mesh_entity::{MeshDigest, RuntimeFact};
use rafka_mesh_transport::entry::EntryAnswer;
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::join::{Join, JoinReply, JoinRequest};
use rafka_node_rpc_contract::outcome::{MalformedKind, RpcOutcome};
use std::collections::HashMap;
use tracing::Instrument as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;

/// What the deploying admin holds of one birth before it reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deployed {
    pub name: PathName,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
    pub supersedes: Option<IncarnationId>,
    pub endpoint_id: EndpointId,
    /// The exact runtime its provider made available to the birth.
    pub runtime: RuntimeFact,
    pub data_dir: String,
}

/// The first field of a digest that disagrees with what was deployed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub field: &'static str,
    pub deployed: String,
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

/// The births this admin deployed and is waiting to hear from.
#[derive(Default)]
pub struct Joins {
    slots: Mutex<HashMap<NodeId, Slot>>,
}

/// What a digest is to the births this admin deployed.
#[derive(Debug, PartialEq, Eq)]
pub enum Standing {
    /// The digest is the deployed birth.
    Deployed,
    /// The node is deployed here and the digest disagrees.
    Mismatch(Mismatch),
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

    /// Stop holding `node_id`'s deployment (it joined, or its create ended).
    pub fn forget(&self, node_id: &NodeId) {
        self.slots.lock().unwrap().remove(node_id);
    }

    pub fn standing(&self, d: &MeshDigest) -> Standing {
        match self.slots.lock().unwrap().get(&d.node.node_id) {
            None => Standing::Unknown,
            Some(s) => match s.deployed.verify(d) {
                Ok(()) => Standing::Deployed,
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
    pub me: PathName,
    pub joins: Arc<Joins>,
    /// What the admin answers a joiner with.
    pub answer: Arc<dyn Fn() -> Pin<Box<dyn Future<Output = EntryAnswer> + Send>> + Send + Sync>,
    /// Install the reported address for the key: membership's `register_location`, the live
    /// resolver, and with it the cancel of every dial aimed at the key's old address.
    pub install: Arc<dyn Fn(&MeshDigest) + Send + Sync>,
    /// Is this exact birth already a member this admin holds?
    pub is_member: Arc<dyn Fn(&MeshDigest) -> bool + Send + Sync>,
    /// The mesh primary this admin sees, by path.name.
    pub primary: Arc<dyn Fn() -> Option<String> + Send + Sync>,
}

pub type JoinSlot = Arc<OnceLock<Arc<JoinDoor>>>;

impl JoinDoor {
    pub async fn serve(&self, peer: EndpointId, req: JoinRequest) -> JoinReply {
        let JoinRequest::JoinNode { digest } = req;
        let Some(d) = MeshDigest::decode(&digest) else {
            return JoinReply::Malformed { kind: MalformedKind::Corrupt };
        };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-join",
            node = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            reported_addr = %d.node.transport_addr,
            served_by = %self.me,
            outcome = tracing::field::Empty,
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
                (self.install)(&d);
                self.joins.report(&d);
                span.record("outcome", "installed");
                tracing::info!(addr = %d.node.transport_addr, "the deployed birth reported where it bound: the address is installed for its key");
                self.joined(&d.node.name).await
            }
            Standing::Unknown if (self.is_member)(&d) => {
                span.record("outcome", "member");
                self.joined(&d.node.name).await
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

    async fn joined(&self, node: &PathName) -> JoinReply {
        let answer = (self.answer)().await;
        if answer.served_by.is_empty() {
            return JoinReply::NotReady { reason: format!("{}: this admin is not ready to answer yet", self.me) };
        }
        tracing::info_span!("rdm.mesh.entry.serve.via-pull", node = %node, served_by = %answer.served_by, members = answer.members.len(), sources = answer.sources.len())
            .in_scope(|| tracing::info!("entry answered"));
        match serde_json::to_vec(&answer) {
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
pub async fn call_join(client: &NodeRpcClient, target: &NodeTarget, anchor: &str, digest: &MeshDigest, attempts: u32) -> Result<EntryAnswer, JoinFailure> {
    let req = JoinRequest::JoinNode { digest: digest.encode() };
    let node = digest.node.name.to_string();
    let mut last = String::new();
    for attempt in 1..=attempts.max(1) {
        let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(5)), ..Default::default() };
        let span = tracing::info_span!("rdm.mesh.entry.update.via-pull-attempt", node = %node, anchor, attempt, step = tracing::field::Empty, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
        let started = std::time::Instant::now();
        let (out, _) = client.call::<Join>(target, &req, &opts).await;
        let (step, result): (&str, Result<EntryAnswer, JoinFailure>) = match out {
            RpcOutcome::Reply(r) => match r.value().clone() {
                JoinReply::Joined { answer } => match serde_json::from_slice::<EntryAnswer>(&answer) {
                    Ok(a) => ("answered", Ok(a)),
                    Err(e) => ("answered", Err(JoinFailure::Refused(format!("the answer does not decode: {e}")))),
                },
                JoinReply::JoinMismatch { field, deployed, reported } => ("refused", Err(JoinFailure::Refused(format!("{field}: deployed {deployed}, reported {reported}")))),
                JoinReply::NotAuthority { primary } => ("refused", Err(JoinFailure::Refused(format!("the admin deployed no such birth (it sees mesh primary {primary:?})")))),
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
                tracing::info_span!("rdm.mesh.entry.update.via-membership-pulled", node = %node, served_by = %a.served_by, members = a.members.len(), attempt)
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
