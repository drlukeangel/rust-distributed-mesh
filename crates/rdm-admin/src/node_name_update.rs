//! `node.name.update` at the node's mesh-primary, the one writer of its mesh's rows (proposal
//! node-properties-own-commands.md §2; R13, R-CR1, R-MN1, R-OW1). The request carries the exact birth
//! and the name only: the path never travels, so it cannot move. From what the mesh-primary holds, in
//! order (§2.1 Validation): the exact birth matches its record (`RejectedStaleIncarnation`); it is the
//! node's writer, the mesh-primary or, before the mesh has one, the fabric-primary
//! (`ReceiverNotPrimary`); the node is live and admits no connection: this mesh-primary holds the
//! birth's `node.connections.deleted` of an `All` that left nothing held, then its `node.mesh.left`
//! (`RejectedAdmittingConnections { status, held_connections }`; a stopped node is never renamed and is
//! refused the same way); the name is within the name grammar and its join with the held record's path
//! is a `path.name` this build keys (`RejectedName`). Then, one change at a time with every other change
//! to a node record: the row writer reads the new name's row and claims it (`NameHeld { by }`,
//! nothing written) and the row moved through the one writer of a row's move
//! (`crate::node_rows::move_row`): put at the new key carrying `renamed_from`, then the old key
//! released. A release that fails leaves both keys held and ends the command incomplete naming it
//! (`NameNotApplied`, `release-pending`); the next writer that meets the row finishes it
//! (`crate::node_rows::finish_release`). The node is told `ApplyNodeName` (this admin itself
//! in-process, through its own name decision) and the command completes only on its `Applied` or
//! `AlreadyApplied` (§2.3). A repeat naming the name the row already holds finishes any pending release
//! and then resends `ApplyNodeName` (§2.1 Idempotency).

use crate::model::{Node, PathName};
use crate::node_rows::{Moved, RenameRefused};
use crate::storage::NodesStorage;
use crate::topology::Topology;
use rdm_core::ConnectionId;
use rdm_client::{ExactBirth, NodeRpc};
use rdm_transport::{CallOptions, NodeTarget, RouteChoice};
use rdm_core::status::{NotPrimary, StatusReply, StatusRequest};
use tokio::sync::RwLock;

/// What a mesh-primary's `node.name.update` acts through: the door its record changes take turns at
/// (`crate::node_config::NodeConfigs`: the writer's held record and the client it tells births
/// through), its view's take of a moved row, and its own name decision for a rename of itself.
pub struct NameDoors<'a> {
    /// The config door: `None` until this admin's node-RPC client exists.
    pub configs: Option<&'a crate::node_config::NodeConfigs>,
    /// This admin's view taking a moved row.
    pub moved: &'a crate::status_rpc::RowMoved,
    /// This admin deciding a name its own row gives it.
    pub rename: &'a crate::status_rpc::Rename,
}

/// Where a caller's `node.name.update` is decided ([`routed`]).
pub enum Routed {
    /// Answered on its route: the fabric-primary's answer carried back up, or the node's mesh-primary's
    /// answer to the fabric-primary that sent it down.
    Answered(StatusReply),
    /// Served here, by the node's row writer ([`update`]).
    Here,
}

/// The route of a `node.name.update` at the admin `me` (born `me_id`) sent by `sent_by`, over the view
/// `view` holds (node-name-update.md step 1; Q197, Q203, Q204: each op writes the fabric-primary's
/// atlas; commands go down): a caller's at an admin that is not the fabric-primary is carried up
/// to the fabric-primary its view names, owed until it answers, and that answer is handed back
/// ([`crate::node_config::NodeConfigs::forward_up`]); at the fabric-primary, a node whose row another
/// admin writes (its mesh-primary) is sent the command down, owed until that mesh-primary answers
/// ([`crate::node_config::NodeConfigs::send_down`]), and an answer naming its row moved (`NameUpdated`,
/// or `NameNotApplied`: the row moved, its apply unresolved) moves the node's record in `atlas` to the
/// new name, so the fabric-primary's node reads answer it at once. Every other command, and a node
/// whose row this admin writes, is served here ([`Routed::Here`]): the row writer's own write feeds
/// its atlas (`crate::node_rows::WrittenRows`). A node.update step of a Build is served at its
/// executor and never routed: its receipt is the atlas's.
#[allow(clippy::too_many_arguments)]
pub async fn routed(me: &PathName, me_id: &rdm_core::NodeId, view: &RwLock<Topology>, configs: Option<&crate::node_config::NodeConfigs>, atlas: Option<&crate::atlas::Atlas>, sent_by: crate::node_config::SentBy, req: &StatusRequest) -> Routed {
    let StatusRequest::UpdateName { node_id, name, .. } = req else { unreachable!("routed is called for UpdateName only") };
    let Some(door) = configs else { return Routed::Here };
    let mut first = view.read().await.clone();
    if sent_by == crate::node_config::SentBy::Caller && !first.is_fabric_primary(me_id) {
        match door.forward_up(me, me_id, &first, view, req).await {
            crate::node_config::Up::Answered(reply) => return Routed::Answered(reply),
            crate::node_config::Up::InProcess(now) => first = now,
        }
    }
    let Some(subject) = first.own(node_id).cloned() else { return Routed::Here };
    if !first.is_fabric_primary(me_id) || crate::node_rows::writes_for(&first, me_id, &subject.mesh) {
        return Routed::Here;
    }
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-fabric-primary",
        node = %me, subject = %subject.name, node_id = %node_id, op = req.op(), name = %name,
        mesh_primary = tracing::field::Empty, atlas = tracing::field::Empty, route = tracing::field::Empty, sends = tracing::field::Empty,
        outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let down = tracing::Instrument::instrument(door.send_down(me, me_id, &first, view, &subject, req), span.clone()).await;
    let crate::node_config::Down::Answered(reply) = down else {
        // This admin holds the node's mesh-primary now, or no longer holds the birth: served here.
        span.record("outcome", "in-process");
        span.in_scope(|| tracing::info!("node.name.update at the fabric-primary is served here"));
        return Routed::Here;
    };
    if matches!(reply, StatusReply::NameUpdated { .. } | StatusReply::NameNotApplied { .. }) {
        // The op's result is the atlas's (Q204): the record at the new name, carrying the name it came from.
        let written = atlas.is_some_and(|a| {
            a.update_record(node_id, |r| {
                if let Ok(to) = r.name.with_name(name) {
                    if r.name != to {
                        r.renamed_from = Some(r.name.clone());
                        r.name = to;
                    }
                }
            })
        });
        span.record("atlas", if written { "renamed" } else { "no-record" });
    }
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("node.name.update at the fabric-primary decided"));
    Routed::Answered(reply)
}

/// Serve an `UpdateName` at the admin `me` (born `me_id`): one span holds the whole command, its
/// outcome the reply's name.
#[allow(clippy::too_many_arguments)]
pub async fn update(me: &PathName, me_id: &rdm_core::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, doors: NameDoors<'_>, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>, left: bool, req: &StatusRequest) -> StatusReply {
    let StatusRequest::UpdateName { node_id, incarnation, name } = req else { unreachable!("update is called for UpdateName only") };
    let span = tracing::info_span!(
        "rdm.node_admin.name.update.via-writer",
        node = %me, node_id = %node_id, incarnation_id = %incarnation.as_str(), name = %name, from = tracing::field::Empty, to = tracing::field::Empty,
        keys = tracing::field::Empty, tags = tracing::field::Empty, traffic = tracing::field::Empty, connections = tracing::field::Empty, mesh = tracing::field::Empty, apply = tracing::field::Empty,
        outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let reply = tracing::Instrument::instrument(
        async {
            // A snapshot of the view: no read lock is held across the row write's I/O.
            let now = view.read().await.clone();
            rename(me, me_id, &now, storage, doors, deleted, left, node_id, incarnation, name, &span).await
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
    /// The span's `(traffic, connections, mesh)`.
    labels: (String, &'static str, &'static str),
    /// It is live, admits no connection and has left its mesh; or it may not take a name: the status
    /// the refusal names, the connections it still held, and why in words.
    quiet: Result<(), (crate::model::NodeStatus, Vec<ConnectionId>, String)>,
}

/// [`Admission`] of `held`: live, its `node.connections.deleted` of an `All` held with nothing left
/// held, then its `node.mesh.left` (`left`). A stopped node is never renamed: it is renamed live, after
/// its connections and its mesh, and stopped after (node-replace.md).
fn admission_of(held: &Node, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>, left: bool) -> Admission {
    if held.stopped {
        return Admission {
            labels: ("Stopped".into(), "stopped", "stopped"),
            quiet: Err((held.status, vec![], "is stopped, and a stopped node is never renamed: a node is renamed live, after its node.connection.delete { All } and its node.mesh.leave, and stopped after".into())),
        };
    }
    let on_mesh = if left { "left" } else { "on-mesh" };
    match deleted {
        None => Admission {
            labels: (format!("{:?}", held.status), "none-deleted", on_mesh),
            quiet: Err((held.status, vec![], format!("is {:?} and its mesh-primary holds no node.connections.deleted for this birth", held.status))),
        },
        Some(r) if !r.all => Admission {
            labels: (format!("{:?}", r.status_at_cut), "partial", on_mesh),
            quiet: Err((r.status_at_cut, vec![], format!("is {:?}, and its last node.connection.delete named exact connections, not All: it holds no node.connections.deleted of every connection", r.status_at_cut))),
        },
        Some(r) if !r.held.is_empty() => Admission {
            labels: (format!("{:?}", r.status_at_cut), "held", on_mesh),
            quiet: Err((r.status_at_cut, r.held.clone(), format!("is {:?} and still held {} connection(s) after its node.connection.delete: {}", r.status_at_cut, r.held.len(), r.held.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")))),
        },
        Some(r) if !left => Admission {
            labels: (format!("{:?}", r.status_at_cut), "deleted", on_mesh),
            quiet: Err((r.status_at_cut, vec![], format!("is {:?}: its connections are deleted, but its mesh-primary holds no node.mesh.left for this birth: it is still on its mesh", r.status_at_cut))),
        },
        Some(r) => Admission { labels: (format!("{:?}", r.status_at_cut), "deleted", on_mesh), quiet: Ok(()) },
    }
}

#[allow(clippy::too_many_arguments)]
async fn rename(
    me: &PathName,
    me_id: &rdm_core::NodeId,
    now: &Topology,
    storage: &dyn NodesStorage,
    doors: NameDoors<'_>,
    deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>,
    left: bool,
    node_id: &rdm_core::NodeId,
    incarnation: &rdm_core::IncarnationId,
    name: &str,
    span: &tracing::Span,
) -> StatusReply {
    // A subject this receiver does not hold is the receiver's own fact (`SubjectUnknown`), never a
    // peer that did not resolve.
    let Some(held) = now.nodes.iter().find(|n| n.node_id == *node_id).cloned() else {
        span.record("detail", format!("{me}: node {node_id} is not in the mesh-primary's view; it cannot be renamed").as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
    };
    span.record("from", held.name.to_string().as_str());
    // 1. The exact birth.
    match held.incarnation_id.as_ref() {
        Some(h) if h != incarnation => return rdm_core::reply::stale(h.clone()),
        None => {
            span.record("detail", format!("{me}: {} has no known birth; it cannot be renamed", held.name).as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        }
        Some(_) => {}
    }
    // 2. The writer: the mesh-primary of the node's mesh, or the fabric-primary for a mesh with none (R-OW1).
    if !crate::node_rows::writes_for(now, me_id, &held.mesh) {
        let held_by = now.node_type_primary(&held.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
        let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me} does not hold it)", held.mesh, held.name);
        span.record("detail", needed.as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
    }
    // 3. The node is live, admits no connection and has left its mesh.
    let admission = admission_of(&held, deleted, left);
    span.record("traffic", admission.labels.0.as_str()).record("connections", admission.labels.1).record("mesh", admission.labels.2);
    if let Err((status, held_connections, why)) = admission.quiet {
        span.record("detail", format!("{me}: {} {why}; a name moves only on a live node off its mesh, its node.connection.delete {{ All }} completed with nothing held and then its node.mesh.leave: nothing is claimed or written", held.name).as_str());
        return rdm_core::reply::status::bounded_rejected_admitting_connections(crate::views::node_status(status), &held_connections);
    }
    // 4. The name: within the grammar, and joined at the held record's path into a key this build keys.
    if let Some(reason) = rdm_core::status::name_refusal(name) {
        let why = format!("{me}: {reason}; nothing is claimed or written");
        span.record("detail", why.as_str());
        return rdm_core::reply::status::bounded_rejected_name(&why);
    }
    let to = match held.name.with_name(name) {
        Ok(to) => to,
        Err(e) => {
            let why = format!("{me}: the name {name:?} at the path {} makes no path.name this build keys: {e}; nothing is claimed or written", held.name.path());
            span.record("detail", why.as_str());
            return rdm_core::reply::status::bounded_rejected_name(&why);
        }
    };
    span.record("to", to.to_string().as_str());
    let Some(door) = doors.configs else {
        return rdm_core::reply::status::not_ready(format!("{me} has no node-RPC client yet, and so no door its record changes take turns at: the rename of {} is refused and nothing is written", held.name));
    };
    // One change to a node record at a time, through the writer's held record: the row carries every
    // field the writer knows of it, the config and the tags included.
    let _one = door.one_at_a_time().await;
    let known = match door.written().known_row(storage, node_id).await {
        Ok(row) => row,
        Err(why) => return rdm_core::reply::status::bounded_node_row_refused(&held.name.to_string(), &format!("{me}: {why}: the rename of {} is refused and nothing is written", held.name)),
    };
    // The key the node's row stands at now: the row's own, else the name the view holds.
    let from = known.as_ref().map_or_else(|| held.name.clone(), |r| r.name.clone());
    if from == to {
        // A repeat naming the name the row already holds: any release still pending is finished first.
        if let Some(row) = &known {
            if let Err(why) = crate::node_rows::finish_release(storage, door.written(), me, row, "retry").await {
                return release_pending(me, node_id, &to, row.renamed_from.as_ref().unwrap_or(&from), &why, span);
            }
        }
    } else {
        let Some(moved) = doors.moved.get() else {
            return rdm_core::reply::status::not_ready(format!("{me}: its view does not take a moved row yet; the rename of {} to {to} is refused and nothing is written", held.name));
        };
        let (keys, tags) = known.map(|r| (r.config.len(), r.tags)).unwrap_or_default();
        span.record("keys", keys as u64).record("tags", crate::node_rows::labels(&tags).as_str());
        let pending = match crate::node_rows::move_row(storage, door.written(), me, &held, &from, &to).await {
            Ok((m, _)) => {
                match m {
                    Moved::ReleasePending { why } => Some(why),
                    Moved::Renamed | Moved::AlreadyMoved => None,
                }
            }
            Err(RenameRefused::NameHeld(h)) => return rdm_core::reply::status::name_held(h.name.to_string(), h.by),
            Err(RenameRefused::Unwritten(why)) => return rdm_core::reply::status::bounded_node_row_refused(&to.to_string(), &why),
        };
        // The view reads the moved row: the birth is listed at the row's name from now on.
        moved(held.name.clone(), node_id.clone(), incarnation.clone(), to.clone()).await;
        if let Some(why) = pending {
            return release_pending(me, node_id, &to, &from, &why, span);
        }
    }
    let done = StatusReply::NameUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
    // The live birth takes the row's name. This admin itself takes it through its own name
    // decision in-process, as its config door applies its own config: it never calls itself.
    let applied = if held.node_id == *me_id {
        span.record("apply", "in-process");
        match doors.rename.get() {
            Some(rename) => {
                let (_, decision) = rename(to.clone()).await;
                Ok(crate::node_name::reply(&decision))
            }
            None => Ok(rdm_core::reply::status::not_ready(format!("{me}: its digest does not exist yet; the name {to} is not taken"))),
        }
    } else {
        span.record("apply", "direct");
        let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
        NodeRpc::new(door.client()).name_apply(&birth, &RouteChoice::Direct, &to, &CallOptions::default()).await
    };
    let (answered, detail) = match applied {
        Ok(r @ (StatusReply::Applied | StatusReply::AlreadyApplied)) => {
            span.record("answered", r.name());
            return done;
        }
        Ok(refused) => (refused.name(), format!("{me} moved the row to {to}; the birth answered {refused:?}; a resubmit resends the apply")),
        Err(end) => (end.outcome(), format!("{me} moved the row to {to}; apply-node-name ended {end:?}; a resubmit resends the apply")),
    };
    span.record("answered", answered).record("detail", detail.as_str());
    rdm_core::reply::status::bounded_name_not_applied(&to.to_string(), answered, &detail)
}

/// The command ends incomplete after its put at `to`: the release of `from` is pending, both keys held
/// (proposal §2.4); the next writer that meets the row finishes it.
fn release_pending(me: &PathName, node_id: &rdm_core::NodeId, to: &PathName, from: &PathName, why: &str, span: &tracing::Span) -> StatusReply {
    let detail = format!("{me}: {node_id} holds {to} (its row carries renamed_from {from}) and the release of {from} is pending: {why}; a resubmit releases it and continues to the apply");
    span.record("answered", "release-pending").record("detail", detail.as_str());
    rdm_core::reply::status::bounded_name_not_applied(&to.to_string(), "release-pending", &detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, NodeId, NodeStatus, ProviderKind, ScopeStatus};
    use crate::status_rpc::ConnectionsDeletedReceipt;
    use crate::storage::MemoryNodesStorage;

    // CONTRACT (`docs/architecture/lifecycles/node-name-update.md:16` — "Precondition: The node is live
    // and admits no connection: this mesh-primary holds the birth's `node.connections.deleted` of an
    // `All`, then its `node.mesh.left` (`node.mesh.leave`), and it holds zero connections. A stopped node
    // is never renamed; a ready node, or one still holding a connection, is refused." and
    // `docs/architecture/lifecycles/node-name-update.md:17` — "Validation: The exact birth matches the
    // held record, else `RejectedStaleIncarnation { held }`; the receiver is the node's writer, else
    // `ReceiverNotPrimary { needed }` (R-RX5); the precondition holds, else
    // `RejectedAdmittingConnections { status, held_connections }`; the name is within the name grammar,
    // else `RejectedName { reason }`."): `admission_of` refuses stopped nodes, missing/partial/held
    // connection receipts, and nodes still on their mesh, and admits a live left birth with zero held
    // connections; `update` enforces exact birth, writer role, admission precondition, and name grammar
    // before touching storage.
    // @feature: node-name-update
    #[tokio::test]
    async fn admission_and_validation_enforce_exact_birth_writer_quiet_left_state_and_name_grammar() {
        let mut subject = Node::allocated("mesh1.rpc.1".parse().unwrap());
        subject.status = NodeStatus::ConnectionsDeleting;
        let inc = IncarnationId::mint();
        subject.incarnation_id = Some(inc.clone());

        let mut stopped = subject.clone();
        stopped.stopped = true;
        stopped.status = NodeStatus::Leaving;
        let stopped_adm = admission_of(&stopped, None, true);
        assert_eq!(stopped_adm.labels, ("Stopped".into(), "stopped", "stopped"));
        assert!(stopped_adm.quiet.is_err());

        let none_adm = admission_of(&subject, None, false);
        assert_eq!(none_adm.labels, ("ConnectionsDeleting".into(), "none-deleted", "on-mesh"));
        assert!(none_adm.quiet.is_err());

        let partial = ConnectionsDeletedReceipt {
            all: false,
            held: vec![],
            status_at_cut: NodeStatus::ConnectionsDeleting,
        };
        assert_eq!(admission_of(&subject, Some(&partial), true).labels.1, "partial");

        let conn = ConnectionId::mint();
        let held_rec = ConnectionsDeletedReceipt {
            all: true,
            held: vec![conn.clone()],
            status_at_cut: NodeStatus::ConnectionsDeleting,
        };
        let held_adm = admission_of(&subject, Some(&held_rec), true);
        assert_eq!(held_adm.labels.1, "held");
        assert_eq!(held_adm.quiet.unwrap_err().1, vec![conn]);

        let clean_rec = ConnectionsDeletedReceipt {
            all: true,
            held: vec![],
            status_at_cut: NodeStatus::ConnectionsDeleting,
        };
        assert!(admission_of(&subject, Some(&clean_rec), false).quiet.is_err());
        let ok_adm = admission_of(&subject, Some(&clean_rec), true);
        assert_eq!(ok_adm.labels, ("ConnectionsDeleting".into(), "deleted", "left"));
        assert!(ok_adm.quiet.is_ok());

        let mut admin1 = Node::allocated("mesh1.admin.1".parse().unwrap());
        admin1.status = NodeStatus::Ready;
        admin1.is_primary = true;
        admin1.is_fabric_primary = true;
        admin1.incarnation_id = Some(IncarnationId::mint());
        let admin2 = Node::allocated("mesh1.admin.2".parse().unwrap());

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
        let doors = || NameDoors { configs: None, moved: &moved, rename: &rename_slot };

        // 1. Stale incarnation -> RejectedStaleIncarnation
        let stale_req = StatusRequest::UpdateName {
            node_id: subject.node_id.clone(),
            incarnation: IncarnationId::mint(),
            name: "1.old".into(),
        };
        assert_eq!(
            update(&admin1.name, &admin1.node_id, &view, &storage, doors(), Some(&clean_rec), true, &stale_req).await,
            StatusReply::RejectedStaleIncarnation { held: inc.clone() }
        );

        // 2. Non-primary -> ReceiverNotPrimary
        let valid_req = StatusRequest::UpdateName {
            node_id: subject.node_id.clone(),
            incarnation: inc.clone(),
            name: "1.old".into(),
        };
        assert!(matches!(
            update(&admin2.name, &admin2.node_id, &view, &storage, doors(), Some(&clean_rec), true, &valid_req).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } }
        ));

        // 3. Invalid name grammar -> RejectedName
        let bad_name_req = StatusRequest::UpdateName {
            node_id: subject.node_id.clone(),
            incarnation: inc,
            name: "Bad_Name".into(),
        };
        assert!(matches!(
            update(&admin1.name, &admin1.node_id, &view, &storage, doors(), Some(&clean_rec), true, &bad_name_req).await,
            StatusReply::RejectedName { .. }
        ));
    }
}
