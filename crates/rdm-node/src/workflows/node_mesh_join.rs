//! `node.mesh.join` (node-mesh-join.md), the one primitive that puts a node on its mesh: `JoinNode`
//! to its accepting authority, the handed name and rafka-time taken from the answer, then the kind's
//! channels entered holding them (its mesh, and the backbone for a node-admin), nothing published
//! before. It is run by [node.start](super::node_start) step 2, by the mesh-primary's standalone
//! `JoinMesh`, and by a node-admin entering a mesh; each of them calls this, never a copy of it.

use super::node_start::{reached_back, JoinThrough, JoinTo, JOIN_ATTEMPTS};
use crate::model::{NodeId, PathName};
use crate::node_self::StartFailed;
use crate::wire::JoinAnswer;
use rdm_core::{MemberStatus, MeshDigest};
use rdm_transport::topology::Membership;
use rdm_transport::{NodeRpcClient, NodeTarget};
use tracing::Instrument as _;

/// What a node kind plugs into `node.mesh.join`: the digest its report presents and how it enters
/// its channels.
#[async_trait::async_trait]
pub trait MeshJoin: Send {
    /// This kind's refusal; the primitive's typed failure converts into it.
    type Error: From<StartFailed> + Send;

    /// The digest the report presents; the primitive presents it Pending.
    fn join_digest(&self) -> MeshDigest;

    /// The immutable app declaration reported on every join of this birth.
    fn join_types(&self) -> Vec<String> { Vec::new() }

    /// The listeners this birth bound from its launch (`RDM_LISTENERS`), each with the port the
    /// operating system selected, reported on every join of this birth (Q151); none for a kind that
    /// binds none.
    fn join_listeners(&self) -> Vec<(String, std::net::SocketAddr)> { Vec::new() }

    /// Enter the kind's channels holding the name the join handed (`None` with no authority), before
    /// anything is published, emitting `rdm.node.mesh.update.via-joined` per channel. `step` is the
    /// channel step's span: the kind drops it when its channel part ends.
    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<Membership, Self::Error>;
}

/// One run of the primitive.
pub struct JoinCall<'a> {
    /// The name the birth holds when it reports.
    pub name: String,
    /// The birth's node id.
    pub node_id: &'a NodeId,
    /// The birth's mesh, as the spans name it.
    pub mesh: &'a str,
    /// The birth's MeshId, as the spans name it.
    pub mesh_id: &'a str,
    /// The process's one Node RPC client.
    pub client: &'a NodeRpcClient,
    /// The process's rafka-time, adopted from the answer.
    pub rafka_time: &'a rdm_transport::clock::RafkaTime,
    /// Whom the report goes to.
    pub to: &'a JoinTo,
    /// The authority as the report's span names it.
    pub authority: &'a str,
    /// The operation the join runs for.
    pub operation: &'a str,
    /// The span the report's span is a child of (`None`: the current one).
    pub report_parent: Option<tracing::Span>,
    /// The caller's span for the report (a start's join step): who answered and the name installed are
    /// recorded on it, and it ends before the channel is entered. `Span::none()` for none.
    pub reported: tracing::Span,
    /// The span the channel step (`rdm.mesh.node.update.via-membership-joined`) is a child of: a
    /// start's root (`None`: the current one). The step opens once the report has ended.
    pub channel_parent: Option<tracing::Span>,
}

/// A completed join: the answer, who gave it, the name the birth holds from it, and its membership.
pub struct MeshJoined {
    /// The authority's answer.
    pub answer: JoinAnswer,
    /// Who gave it, and where the steps after the join reach it.
    pub through: JoinThrough,
    /// The name the birth holds from the answer on.
    pub name: String,
    /// The birth's membership, on its channel.
    pub membership: Membership,
}

/// `node.mesh.join` (node-mesh-join.md): `joining` from the report until the channels hold.
pub async fn node_mesh_join<J: MeshJoin + ?Sized>(node: &mut J, call: JoinCall<'_>) -> Result<MeshJoined, J::Error> {
    let JoinCall { mut name, node_id, mesh, mesh_id, client, rafka_time, to, authority, operation, report_parent, reported, channel_parent } = call;
    let mut digest = node.join_digest();
    digest.status = MemberStatus::Pending;
    let joining = match &report_parent {
        Some(parent) => tracing::info_span!(parent: parent, "rdm.node.mesh.update.via-joining", node = %name, node_id = %node_id, mesh = %mesh, mesh_id = %mesh_id, authority = %authority, operation = %operation),
        None => tracing::info_span!("rdm.node.mesh.update.via-joining", node = %name, node_id = %node_id, mesh = %mesh, mesh_id = %mesh_id, authority = %authority, operation = %operation),
    };
    let (answer, through) = join(&name, client, to, &digest, &node.join_types(), &node.join_listeners()).instrument(joining).await?;
    drop(report_parent);
    reported.record("served_by", through.name.to_string().as_str());
    reported.record("endpoint", through.anchor.as_str());
    // The name the answer hands (a join that hands none is refused, `crate::join::call_join_to_types`): the
    // birth holds it from the answer on, and every step from here is named by it.
    if let Some(handed) = &answer.control.name {
        name = handed.to_string();
    }
    if reported.is_none() {
        crate::rafka_time::adopt_join_answer(&tracing::Span::current(), rafka_time, &name, &answer);
    } else {
        crate::rafka_time::adopt_join_answer(&reported, rafka_time, &name, &answer);
    }
    reported.record("installed", name.as_str());
    drop(reported);
    // The channel step opens only once the report's step has ended (node.start's steps never overlap).
    let channel = match &channel_parent {
        Some(parent) => tracing::info_span!(parent: parent, "rdm.mesh.node.update.via-membership-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty),
        None => tracing::info_span!("rdm.mesh.node.update.via-membership-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty),
    };
    let membership = node.enter_channel(channel, Some(&answer)).await?;
    membership.learn_statuses(&answer.statuses);
    Ok(MeshJoined { answer, through, name, membership })
}

/// `JoinNode` with `digest`, each of its attempts made to whom `to` names then: the launcher, or the
/// admin whose call commanded the join last, reached back over that call. The answer and who gave
/// it: `served_by` names the admin (the launcher's record names it at a first start).
async fn join(node: &str, client: &NodeRpcClient, to: &JoinTo, digest: &MeshDigest, types: &[String], listeners: &[(String, std::net::SocketAddr)]) -> Result<(JoinAnswer, JoinThrough), StartFailed> {
    let next = || {
        let to = match to {
            JoinTo::Launcher { target, endpoint, .. } => Ok((target.clone(), endpoint.fmt_short().to_string())),
            JoinTo::Commander(commander) => Err(*commander.borrow()),
        };
        async move {
            match to {
                Ok(t) => Ok(t),
                Err(key) => reached_back(node, client, &key).await.map(|t| (t, key.fmt_short().to_string())),
            }
        }
    };
    let (answer, target) = match crate::join::call_join_to_types(client, next, digest, JOIN_ATTEMPTS, types, listeners).await {
        Ok(joined) => joined,
        Err(crate::join::JoinFailure::Refused(why)) => return Err(StartFailed { step: "node.join", reason: format!("{node} was refused its join by {}: {why}", whom(to)) }),
        Err(crate::join::JoinFailure::Unreached(why)) => return Err(StartFailed { step: "node.join", reason: format!("{node} could not join through {}: unreached: {why}", whom(to)) }),
    };
    let (endpoint, name) = match (to, &target) {
        (JoinTo::Launcher { endpoint, name, .. }, _) => (*endpoint, name.clone()),
        (JoinTo::Commander(_), NodeTarget::Endpoint { endpoint_id, .. }) => match answer.served_by.parse::<PathName>() {
            Ok(n) => (*endpoint_id, n),
            Err(e) => return Err(StartFailed { step: "node.join", reason: format!("{node}: the join answer from endpoint {endpoint_id} names who answered as {:?}, which is no path.name: {e}", answer.served_by) }),
        },
        (JoinTo::Commander(_), other) => unreachable!("a commander is reached back over its call's endpoint, never {other:?}"),
    };
    Ok((answer, JoinThrough { target, endpoint, name, anchor: endpoint.fmt_short().to_string() }))
}

/// Whom a join goes to, in words.
fn whom(to: &JoinTo) -> String {
    match to {
        JoinTo::Launcher { name, .. } => name.to_string(),
        JoinTo::Commander(commander) => format!("the admin whose call commanded it (endpoint {})", *commander.borrow()),
    }
}
