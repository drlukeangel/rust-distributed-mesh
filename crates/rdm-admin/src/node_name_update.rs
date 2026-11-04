//! `node.name.update` at the node's mesh-primary (proposal node-properties-own-commands.md §2; R13,
//! R-CR1, R-MN1, R-OW1). The request carries the exact birth
//! and separate path/name fields: the path must match the held node and cannot change. From what the mesh-primary holds, in
//! order (§2.1 Validation): the exact birth matches its record (`RejectedStaleIncarnation`); it is the
//! node's writer, the mesh-primary or, before the mesh has one, the fabric-primary
//! (`ReceiverNotPrimary`); the node holds no connection, the one condition (Q347): stopped, or this
//! mesh-primary holds the birth's `node.connections.deleted` of an `All` that left nothing held
//! (`RejectedAdmittingConnections { status, held_connections }`); the name is within the name grammar
//! and the path is unchanged (`RejectedName`). Then, one change at a time with every other change
//! to a node record: the mesh-primary checks the new pair against the node records, a runtime read
//! (`crate::node_rows::check_name`: `NameHeld { by }`, nothing written), and the node is told
//! `ApplyNodeName` (this admin itself in-process, through its own name decision): the node writes its
//! own record at its node.id with the new name (Q356), the name move, then publishes it. A live node
//! then rejoins its mesh under the new name: this
//! mesh-primary commands its `node.mesh.join` ([`crate::heard_again::HeardAgain::rejoin`]), and the
//! command completes on its `MeshJoined`; a stopped node completes on its `Applied` or
//! `AlreadyApplied`, and its start rejoins it under the name. A repeat naming the name the record already
//! holds resends `ApplyNodeName` (Idempotency).

use crate::model::{Node, PathName};
use crate::storage::NodesStorage;
use crate::topology::Topology;
use rdm_core::ConnectionId;
use rdm_client::{ExactBirth, NodeRpc};
use rdm_transport::{CallOptions, NodeTarget, RouteChoice};
use rdm_core::status::{NotPrimary, StatusReply, StatusRequest};
use tokio::sync::RwLock;

/// What a mesh-primary's `node.name.update` acts through: the `NodeConfigs` its node commands take
/// turns at (`crate::node_config::NodeConfigs`: the client it tells births through), its view's take
/// of a moved name, its own name decision for a rename of itself, and the command of a renamed node's
/// rejoin.
pub struct NodeNameUpdateService<'a> {
    /// The `NodeConfigs`: `None` until this admin's node-RPC client exists.
    pub configs: Option<&'a crate::node_config::NodeConfigs>,
    /// This admin's view taking a moved name.
    pub moved: &'a crate::status_rpc::RowMoved,
    /// This admin deciding a name its mesh-primary gives it.
    pub rename: &'a crate::status_rpc::Rename,
    /// This admin's command of a renamed live birth's `node.mesh.join`.
    pub rejoin: &'a crate::status_rpc::Rejoin,
}

/// Where a caller's `node.name.update` is decided ([`routed`]).
pub enum Routed {
    /// Answered on its route: the fabric-primary's answer carried back up, or the node's mesh-primary's
    /// answer to the fabric-primary that sent it down.
    Answered(StatusReply),
    /// Served here, by the node's mesh-primary ([`update`]).
    Here,
}

/// The route of a `node.name.update` at the admin `me` (born `me_id`) sent by `sent_by`, over the view
/// `view` holds (node-name-update.md step 1; commands go down): a caller's at an admin that is not the fabric-primary is carried up
/// to the fabric-primary its view names, owed until it answers, and that answer is handed back
/// ([`crate::node_config::NodeConfigs::forward_up`]); at the fabric-primary, a node of a mesh another
/// admin is the mesh-primary of is sent the command down, owed until that mesh-primary answers
/// ([`crate::node_config::NodeConfigs::send_down`]): the record the node writes is what the
/// fabric-primary's node reads read next (query.md §1). Every other command, and a node of a mesh this
/// admin acts as the mesh-primary of, is served here ([`Routed::Here`]). A node.update step of a Build is served at its
/// executor and never routed: its receipt is the atlas's.
#[allow(clippy::too_many_arguments)]
pub async fn routed(me: &PathName, me_id: &rdm_core::NodeId, view: &RwLock<Topology>, configs: Option<&crate::node_config::NodeConfigs>, sent_by: crate::node_config::SentBy, req: &StatusRequest) -> Routed {
    let StatusRequest::UpdateName { node_id, name, .. } = req else { unreachable!("routed is called for UpdateName only") };
    let Some(configs) = configs else { return Routed::Here };
    let mut first = view.read().await.clone();
    if sent_by == crate::node_config::SentBy::Caller && !first.is_fabric_primary(me_id) {
        match configs.forward_up(me, me_id, &first, view, req).await {
            crate::node_config::Up::Answered(reply) => return Routed::Answered(reply),
            crate::node_config::Up::InProcess(now) => first = now,
            crate::node_config::Up::Unanswered(why) => return Routed::Answered(crate::node_config::Up::unanswered(why)),
        }
    }
    let Some(subject) = first.own(node_id).cloned() else { return Routed::Here };
    if !first.is_fabric_primary(me_id) || crate::node_rows::acts_as_mesh_primary(&first, me_id, &subject.mesh) {
        return Routed::Here;
    }
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-fabric-primary",
        node_path = %me.path, node_name = %me.name, subject_path = %subject.path, subject_name = %subject.name, node_id = %node_id, op = req.op(), requested_path = %name.path, requested_name = %name.name,
        mesh_primary = tracing::field::Empty, atlas = tracing::field::Empty, route = tracing::field::Empty, sends = tracing::field::Empty,
        outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let down = tracing::Instrument::instrument(configs.send_down(me, me_id, &first, view, &subject, req), span.clone()).await;
    let reply = match down {
        crate::node_config::Down::Answered(reply) => reply,
        // The call down ended with its attempts spent: the caller is told by name.
        crate::node_config::Down::Unanswered(why) => {
            span.record("outcome", "unanswered").record("detail", why.as_str());
            span.in_scope(|| tracing::info!("node.name.update at the fabric-primary: the call down ended unanswered"));
            return Routed::Answered(crate::node_config::Up::unanswered(why));
        }
        crate::node_config::Down::InProcess(_) | crate::node_config::Down::Gone(_) => {
            // This admin holds the node's mesh-primary now, or no longer holds the birth: served here.
            span.record("outcome", "in-process");
            span.in_scope(|| tracing::info!("node.name.update at the fabric-primary is served here"));
            return Routed::Here;
        }
    };
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("node.name.update at the fabric-primary decided"));
    Routed::Answered(reply)
}

/// Serve an `UpdateName` at the admin `me` (born `me_id`): one span holds the whole command, its
/// outcome the reply's name.
#[allow(clippy::too_many_arguments)]
pub async fn update(me: &PathName, me_id: &rdm_core::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, service: NodeNameUpdateService<'_>, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>, req: &StatusRequest) -> StatusReply {
    let StatusRequest::UpdateName { node_id, incarnation, name } = req else { unreachable!("update is called for UpdateName only") };
    let span = tracing::info_span!(
        "rdm.node_admin.name.update.via-writer",
        node_path = %me.path, node_name = %me.name, node_id = %node_id, incarnation_id = %incarnation.as_str(), requested_path = %name.path, requested_name = %name.name, from_path = tracing::field::Empty, from_name = tracing::field::Empty, to_path = tracing::field::Empty, to_name = tracing::field::Empty,
        traffic = tracing::field::Empty, connections = tracing::field::Empty, apply = tracing::field::Empty, rejoin = tracing::field::Empty,
        outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let reply = tracing::Instrument::instrument(
        async {
            // A snapshot of the view: no read lock is held across the store reads and the node's call.
            let now = view.read().await.clone();
            rename(me, me_id, &now, storage, service, deleted, node_id, incarnation, name, &span).await
        },
        span.clone(),
    )
    .await;
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("node.name.update decided"));
    reply
}

/// Whether the held node may take a name (R-CR1a, R-CR2; node-name-update.md Precondition), from what
/// this mesh-primary holds.
struct Admission {
    /// The span's `(traffic, connections)`.
    labels: (String, &'static str),
    /// It holds no connection; or it may: the status the refusal names, the connections it still
    /// held, and why in words.
    quiet: Result<(), (crate::model::NodeStatus, Vec<ConnectionId>, String)>,
}

/// [`Admission`] of `held`: no connection is the one condition (Q347). A stopped node holds none; a
/// live one holds none once this mesh-primary holds its `node.connection.deleted` of an `All` with
/// nothing left held.
fn admission_of(held: &Node, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>) -> Admission {
    if held.stopped {
        return Admission { labels: ("Stopped".into(), "stopped"), quiet: Ok(()) };
    }
    match deleted {
        None => Admission {
            labels: (format!("{:?}", held.status), "none-deleted"),
            quiet: Err((held.status, vec![], format!("is {:?} and its mesh-primary holds no node.connection.deleted for this birth", held.status))),
        },
        Some(r) if !r.all => Admission {
            labels: (format!("{:?}", r.status_at_cut), "partial"),
            quiet: Err((r.status_at_cut, vec![], format!("is {:?}, and its last node.connection.delete named exact connections, not All: it holds no node.connection.deleted of every connection", r.status_at_cut))),
        },
        Some(r) if !r.held.is_empty() => Admission {
            labels: (format!("{:?}", r.status_at_cut), "held"),
            quiet: Err((r.status_at_cut, r.held.clone(), format!("is {:?} and still held {} connection(s) after its node.connection.delete: {}", r.status_at_cut, r.held.len(), r.held.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")))),
        },
        Some(r) => Admission { labels: (format!("{:?}", r.status_at_cut), "deleted"), quiet: Ok(()) },
    }
}

#[allow(clippy::too_many_arguments)]
async fn rename(
    me: &PathName,
    me_id: &rdm_core::NodeId,
    now: &Topology,
    storage: &dyn NodesStorage,
    service: NodeNameUpdateService<'_>,
    deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>,
    node_id: &rdm_core::NodeId,
    incarnation: &rdm_core::IncarnationId,
    name: &PathName,
    span: &tracing::Span,
) -> StatusReply {
    // A subject this receiver does not hold is the receiver's own fact (`SubjectUnknown`), never a
    // peer that did not resolve.
    let Some(held) = now.nodes.iter().find(|n| n.node_id == *node_id).cloned() else {
        span.record("detail", format!("{me_id}: node {node_id} is not in the mesh-primary's view; it cannot be renamed").as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
    };
    span.record("from_path", held.path.as_str()).record("from_name", held.name.as_str());
    // 1. The exact birth.
    match held.incarnation_id.as_ref() {
        Some(h) if h != incarnation => return rdm_core::reply::stale(h.clone()),
        None => {
            span.record("detail", format!("{me_id}: {} has no known birth; it cannot be renamed", held.node_id).as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        }
        Some(_) => {}
    }
    // 2. The receiver: the mesh-primary of the node's mesh, or the fabric-primary for a mesh with none (R-OW1).
    if !crate::node_rows::acts_as_mesh_primary(now, me_id, &held.mesh) {
        let held_by = now.node_type_primary(&held.mesh, "node-admin").map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.node_id));
        let needed = format!("{}'s primary node-admin, the mesh-primary of {} ({held_by}; {me_id} does not hold it)", held.mesh, held.node_id);
        span.record("detail", needed.as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
    }
    // 3. The node holds no connection.
    let admission = admission_of(&held, deleted);
    span.record("traffic", admission.labels.0.as_str()).record("connections", admission.labels.1);
    if let Err((status, held_connections, why)) = admission.quiet {
        span.record("detail", format!("{me_id}: {} {why}; a name moves only on a node that holds no connection, stopped or its node.connection.delete {{ All }} completed with nothing held: nothing is claimed or written", held.node_id).as_str());
        return rdm_core::reply::status::bounded_rejected_admitting_connections(crate::views::node_status(status), &held_connections);
    }
    // Validate both fields before claiming the destination pair.
    let to = match PathName::new(name.path.clone(), name.name.clone()) {
        Ok(to) => to,
        Err(e) => return rdm_core::reply::status::bounded_rejected_name(&format!("{me_id}: {e}; nothing is claimed or written")),
    };
    span.record("to_path", to.path.as_str()).record("to_name", to.name.as_str());
    let (to_path, to_name) = (&to.path, &to.name);
    let Some(configs) = service.configs else {
        return rdm_core::reply::status::not_ready(format!("{me_id} has no node-RPC client yet, and so no `NodeConfigs` its record changes take turns at: the rename of {} is refused and nothing is written", held.node_id));
    };
    // One command to a node at a time. The name the node's record stands at now (a runtime point read,
    // Q237 A), else the name the view holds.
    let one = configs.one_at_a_time().await;
    let known = match storage.node(node_id).await {
        Ok(line) => line.map(|l| l.record),
        Err(why) => return rdm_core::reply::status::bounded_node_row_refused(&node_id.to_string(), &format!("{me_id}: {why}: the rename of {} is refused and nothing is written", held.node_id)),
    };
    let from = known.as_ref().map_or_else(|| held.location(), |r| r.location());
    if from.path != to.path {
        return rdm_core::reply::status::bounded_rejected_name("a node's path is immutable; only its name may change, and nothing is claimed or written");
    }
    let moved = if from != to {
        let Some(moved) = service.moved.get() else {
            return rdm_core::reply::status::not_ready(format!("{me_id}: its view does not take a moved name yet; the rename of {} to path={to_path:?} name={to_name:?} is refused and nothing is written", held.node_id));
        };
        // Another node's live record at the destination refuses it by name; nothing is written.
        match crate::node_rows::check_name(storage, &to, node_id).await {
            Ok(Ok(())) => {}
            Ok(Err(h)) => return rdm_core::reply::status::name_held(h.location(), h.by),
            Err(why) => return rdm_core::reply::status::bounded_node_row_refused(&node_id.to_string(), &format!("{me_id}: the records at path={to_path:?} name={to_name:?} could not be read: {why}; nothing is written")),
        }
        Some(moved)
    } else {
        None
    };
    let done = StatusReply::NameUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
    // The live birth takes the name. This admin itself takes it through its own name
    // decision in-process, as its `NodeConfigs` applies its own config: it never calls itself.
    let applied = if held.node_id == *me_id {
        span.record("apply", "in-process");
        match service.rename.get() {
            Some(rename) => {
                let (_, decision) = rename(to.clone()).await;
                Ok(crate::node_name::reply(&decision))
            }
            None => Ok(rdm_core::reply::status::not_ready(format!("{me_id}: its digest does not exist yet; the name path={to_path:?} name={to_name:?} is not taken"))),
        }
    } else {
        span.record("apply", "direct");
        let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
        NodeRpc::new(configs.client()).name_apply(&birth, &RouteChoice::Direct, &to, &CallOptions::default()).await
    };
    let (answered, detail) = match applied {
        Ok(r @ (StatusReply::Applied | StatusReply::AlreadyApplied)) => {
            span.record("answered", r.name());
            // The node's record holds the name (it wrote it before it answered): the view lists the birth
            // there from now on.
            if let Some(moved) = moved {
                moved(held.location(), node_id.clone(), incarnation.clone(), to.clone()).await;
            }
            // A stopped node rejoins at its start, this admin is on its own mesh: done. A live node
            // rejoins its mesh under the name now (Q347), commanded down by this mesh-primary once the
            // `NodeConfigs` turn is released: the join's arrival takes the same turn.
            if held.stopped || held.node_id == *me_id {
                span.record("rejoin", if held.stopped { "at-start" } else { "in-process" });
                return done;
            }
            drop(one);
            let Some(rejoin) = service.rejoin.get() else {
                span.record("rejoin", "no-runner");
                return rdm_core::reply::status::bounded_name_not_applied(&node_id.to_string(), "not-ready", &format!("the birth took path={to_path:?} name={to_name:?} on its record; this admin has no runner yet to command its node.mesh.join; a resubmit resends the apply and the join"));
            };
            let mut renamed = held.clone();
            (renamed.path, renamed.name) = (to.path.clone(), to.name.clone());
            return match rejoin(renamed, incarnation.clone()).await {
                Ok(StatusReply::MeshJoined { .. }) => {
                    span.record("rejoin", "mesh-joined");
                    done
                }
                Ok(other) => {
                    span.record("rejoin", other.name()).record("detail", format!("the birth took path={to_path:?} name={to_name:?} on its record and answered its node.mesh.join {other:?}; a resubmit resends the apply and the join").as_str());
                    rdm_core::reply::status::bounded_name_not_applied(&node_id.to_string(), other.name(), &format!("{me_id}: the birth's node.mesh.join under path={to_path:?} name={to_name:?} answered {other:?}"))
                }
                Err(why) => {
                    span.record("rejoin", "unanswered").record("detail", format!("the birth took path={to_path:?} name={to_name:?} on its record and its node.mesh.join ended {why}; a resubmit resends the apply and the join").as_str());
                    rdm_core::reply::status::bounded_name_not_applied(&node_id.to_string(), "unanswered", &format!("{me_id}: the birth's node.mesh.join under path={to_path:?} name={to_name:?} ended {why}"))
                }
            };
        }
        Ok(refused) => (refused.name(), format!("{me_id} asked the birth to take path={to_path:?} name={to_name:?}; it answered {refused:?}; a resubmit resends the apply")),
        Err(end) => (end.outcome(), format!("{me_id} asked the birth to take path={to_path:?} name={to_name:?}; apply-node-name ended {end:?}; a resubmit resends the apply")),
    };
    span.record("answered", answered).record("detail", detail.as_str());
    rdm_core::reply::status::bounded_name_not_applied(&node_id.to_string(), answered, &detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, NodeId, NodeStatus, ProviderKind, ScopeStatus};
    use crate::status_rpc::ConnectionsDeletedReceipt;
    use crate::storage::MemoryNodesStorage;

    // CONTRACT (node-name-update.md Precondition: "The node holds no connection: it is stopped, or
    // this mesh-primary holds the birth's `node.connection.deleted` of an `All` that left zero
    // connections held"; Validation: "The exact birth matches the held record, else
    // `RejectedStaleIncarnation { held }`; the receiver is the node's writer, else `ReceiverNotPrimary
    // { needed }` (R-RX5); the precondition holds, else `RejectedAdmittingConnections { status,
    // held_connections }`; the path/name pair is valid and the name has no dot, else `RejectedName {
    // reason }`"): `admission_of` admits a stopped node and a live birth whose `All` left nothing held,
    // and refuses a missing, partial or still-held connections receipt; `update` enforces exact birth,
    // writer role, the precondition and the name grammar before touching storage.
    // @feature: node-name-update
    #[tokio::test]
    async fn admission_and_validation_enforce_exact_birth_writer_no_connection_and_name_grammar() {
        let mut subject = Node::allocated(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        subject.status = NodeStatus::ConnectionsDeleting;
        let inc = IncarnationId::mint();
        subject.incarnation_id = Some(inc.clone());

        let mut stopped = subject.clone();
        stopped.stopped = true;
        stopped.status = NodeStatus::Leaving;
        let stopped_adm = admission_of(&stopped, None);
        assert_eq!(stopped_adm.labels, ("Stopped".into(), "stopped"));
        assert!(stopped_adm.quiet.is_ok(), "a stopped node holds no connection");

        let none_adm = admission_of(&subject, None);
        assert_eq!(none_adm.labels, ("ConnectionsDeleting".into(), "none-deleted"));
        assert!(none_adm.quiet.is_err());

        let partial = ConnectionsDeletedReceipt {
            all: false,
            held: vec![],
            status_at_cut: NodeStatus::ConnectionsDeleting,
        };
        assert_eq!(admission_of(&subject, Some(&partial)).labels.1, "partial");

        let conn = ConnectionId::mint();
        let held_rec = ConnectionsDeletedReceipt {
            all: true,
            held: vec![conn.clone()],
            status_at_cut: NodeStatus::ConnectionsDeleting,
        };
        let held_adm = admission_of(&subject, Some(&held_rec));
        assert_eq!(held_adm.labels.1, "held");
        assert_eq!(held_adm.quiet.unwrap_err().1, vec![conn]);

        let clean_rec = ConnectionsDeletedReceipt {
            all: true,
            held: vec![],
            status_at_cut: NodeStatus::ConnectionsDeleting,
        };
        let ok_adm = admission_of(&subject, Some(&clean_rec));
        assert_eq!(ok_adm.labels, ("ConnectionsDeleting".into(), "deleted"));
        assert!(ok_adm.quiet.is_ok(), "its mesh is never asked: a node on its mesh with no connection takes a name");

        let mut admin1 = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        admin1.status = NodeStatus::Ready;
        admin1.is_primary = true;
        admin1.is_fabric_primary = true;
        admin1.incarnation_id = Some(IncarnationId::mint());
        let admin2 = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin");

        let view = RwLock::new(Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(MeshId::mint()),
                name: "mesh1".into(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![admin1.clone(), admin2.clone(), subject.clone()],
        });
        let storage = MemoryNodesStorage::default();
        let moved = crate::status_rpc::RowMoved::default();
        let rename_slot = crate::status_rpc::Rename::default();
        let rejoin_slot = crate::status_rpc::Rejoin::default();
        let service = || NodeNameUpdateService { configs: None, moved: &moved, rename: &rename_slot, rejoin: &rejoin_slot };

        // 1. Stale incarnation -> RejectedStaleIncarnation
        let stale_req = StatusRequest::UpdateName {
            node_id: subject.node_id.clone(),
            incarnation: IncarnationId::mint(),
            name: subject.location().with_name("renamed").unwrap(),
        };
        assert_eq!(
            update(&admin1.location(), &admin1.node_id, &view, &storage, service(), Some(&clean_rec), &stale_req).await,
            StatusReply::RejectedStaleIncarnation { held: inc.clone() }
        );

        // 2. Non-primary -> ReceiverNotPrimary
        let valid_req = StatusRequest::UpdateName {
            node_id: subject.node_id.clone(),
            incarnation: inc.clone(),
            name: subject.location().with_name("renamed").unwrap(),
        };
        assert!(matches!(
            update(&admin2.location(), &admin2.node_id, &view, &storage, service(), Some(&clean_rec), &valid_req).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } }
        ));

        // 3. Invalid name grammar -> RejectedName
        let bad_name_req = StatusRequest::UpdateName {
            node_id: subject.node_id.clone(),
            incarnation: inc,
            name: PathName { path: subject.path.clone(), name: "Bad_Name".into() },
        };
        assert!(matches!(
            update(&admin1.location(), &admin1.node_id, &view, &storage, service(), Some(&clean_rec), &bad_name_req).await,
            StatusReply::RejectedName { .. }
        ));
    }
}
