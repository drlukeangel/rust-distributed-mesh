//! Client side of the `node.join` protocol (`rdm-node`).

use std::time::Duration;

use rdm_transport::{NodeRpcClient, NodeTarget};
use rdm_core::join::{Join, JoinReply, JoinRequest};
use rdm_core::wire::WireDigest;
use rdm_core::outcome::RpcOutcome;
use rdm_core::MeshDigest;

use crate::wire::JoinAnswer;

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
    let next = || {
        let to = Ok((target.clone(), anchor.to_string()));
        async move { to }
    };
    call_join_to(client, next, digest, attempts).await.map(|(answer, _)| answer)
}

/// `JoinNode` with `digest`, within `attempts` of five seconds each, each attempt made to whom `next`
/// names then (a target, and the short endpoint id the attempt's span names as its anchor): a start
/// after a stop joins the call that commanded it last. A refusal by name ends it at once; an attempt
/// `next` can name no target for is unanswered, its reason named. The answer, and the target that
/// gave it.
pub async fn call_join_to<F, Fut>(client: &NodeRpcClient, mut next: F, digest: &MeshDigest, attempts: u32) -> Result<(JoinAnswer, NodeTarget), JoinFailure>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(NodeTarget, String), String>>,
{
    let req = JoinRequest::JoinNode { digest: WireDigest::from(digest) };
    let node = digest.node.name.to_string();
    let mut last = String::new();
    for attempt in 1..=attempts.max(1) {
        let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(5)), ..Default::default() };
        let to = next().await;
        let anchor = match &to {
            Ok((_, anchor)) => anchor.clone(),
            Err(_) => String::new(),
        };
        let span = tracing::info_span!("rdm.mesh.entry.update.via-pull-attempt", node = %node, anchor = %anchor, attempt, step = tracing::field::Empty, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
        let started = std::time::Instant::now();
        let (step, result): (&str, Result<JoinAnswer, JoinFailure>) = match &to {
            Err(why) => ("unaddressed", Err(JoinFailure::Unreached(why.clone()))),
            Ok((target, _)) => match client.call::<Join>(target, &req, &opts).await.0 {
                RpcOutcome::Reply(r) => match r.value().clone() {
                    JoinReply::Joined { answer } => match crate::wire::answer_from_wire(&answer) {
                        Ok(a) if a.control.name.is_none() => ("answered", Err(JoinFailure::Refused(format!("{}: its join answer hands no name for this birth to install", a.served_by)))),
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
            },
        };
        span.record("step", step);
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", match &result { Ok(_) => "answered".to_string(), Err(e) => e.to_string() }.as_str());
        span.in_scope(|| tracing::info!("one join attempt"));
        match result {
            Ok(a) => {
                tracing::info_span!("rdm.mesh.entry.update.via-membership-pulled", node = %node, served_by = %a.served_by, statuses = a.statuses.len(), attempt)
                    .in_scope(|| tracing::info!("joined"));
                let (target, _) = to.expect("an answered attempt had a target");
                return Ok((a, target));
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

