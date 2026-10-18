//! A caller's node property commands at the node's mesh-primary, the one writer of its mesh's rows
//! (proposal node-properties-own-commands.md §1, §3; R-OW1, R14, R-TG1): each command is refused
//! `fabric-not-ready` until the initial fabric has first reached ready-for-traffic, and each runs once
//! through the writer's held record, one change at a time with every other change to a node record
//! (`crate::node_config::NodeConfigs::one_at_a_time`). Here: that refusal, shared by every property
//! command, and `node.tags.create` / `node.tags.delete` (node-rpc-envelope.md node.tags): each label is
//! its own key of the node store's tags index, kept with the row by the store adapter (a label the
//! row stops carrying is tombstoned, one it gains is put); a label the node already carries is ignored
//! and writes nothing. Neither has a precondition beyond the fabric being ready, neither touches the
//! name, and the node is told nothing: its tags live in its row and the index, never in its digest.

use crate::model::PathName;
use crate::storage::{NodeRow, NodesStorage};
use crate::topology::Topology;
use rdm_core::NodeId;
use rdm_core::status::{NotAuthority, StatusReply, StatusRequest, TagScope};
use std::collections::BTreeSet;
use tokio::sync::RwLock;

/// Whether the initial fabric first reached ready-for-traffic, as a mesh-primary decides it for a
/// caller's property command: what it knows, and its mesh's status rows on the store root, which hold
/// the ready-for-traffic row from the moment it was put.
#[derive(Clone, Copy)]
pub struct FirstReadyOnRoot<'a> {
    /// What this admin knows.
    pub first_ready: &'a crate::fabric_state::FirstReady,
    /// This admin's mesh's status rows on the store root.
    pub status: &'a dyn crate::status_storage::StatusStorage,
}

/// The refusal of a caller's property command `op` (`node.tags.create`, `node.config.update`, ...) of
/// `node_id` while the initial fabric has not first reached ready-for-traffic (states.md, "The initial
/// fabric before ready-for-traffic"; R-PC1): before state-commit the node's row would live only in this
/// mesh-primary's scratchpad. Decided on the durable fact, the ready-for-traffic row among this mesh's
/// status rows on the store root, else among the fabric-primary's mesh's (the one it put as it entered
/// ready-for-traffic), which the fabric state this admin hears by gossip can lag; a store that cannot be
/// read is refused naming it. Nothing is read of the node, claimed or written. The refusal's detail is
/// recorded on `span`, and one `rdm.node_admin.node.reject.via-fabric-not-ready` span names the
/// workflow and the fabric's status.
pub async fn refused_before_first_ready(me: &PathName, view: &RwLock<Topology>, first_ready: FirstReadyOnRoot<'_>, op: &str, node_id: &NodeId, span: &tracing::Span) -> Option<StatusReply> {
    let (fabric_id, now, subject, fabric_primary_mesh) = {
        let now = view.read().await;
        (now.fabric.id.clone(), now.fabric.status, now.nodes.iter().find(|n| n.node_id == *node_id).map(|n| n.name.to_string()).unwrap_or_else(|| node_id.to_string()), now.fabric_primary().map(|n| n.mesh.clone()))
    };
    let workflow = format!("{op} {subject}");
    let status = now.name();
    match first_ready.first_ready.reached_on_root(first_ready.status, &fabric_id, now, fabric_primary_mesh.as_deref()).await {
        Ok(true) => return None,
        Ok(false) => {}
        Err(e) => {
            let (source, read) = (format!("status storage of {me}"), format!("read whether fabric {fabric_id} first reached ready-for-traffic for {workflow}"));
            span.record("detail", format!("{me}: {workflow} is refused: {source} could not be read to {read}: {e}; nothing is claimed or written").as_str());
            return Some(rdm_core::reply::status::bounded_source_failed(&source, &read, &e.to_string()));
        }
    }
    let refused = rdm_core::reply::status::bounded_rejected_fabric_not_ready(&workflow, status);
    span.record("detail", format!("{me}: {workflow} is refused: the initial fabric has not first reached ready-for-traffic (it is {status}, and its mesh's status rows on the store root hold no ready-for-traffic row of fabric {fabric_id}), and before state-commit the row would live only in this mesh-primary's scratchpad; nothing is claimed or written").as_str());
    tracing::info_span!("rdm.node_admin.node.reject.via-fabric-not-ready", node = %me, workflow = %workflow, status, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("{op} is refused until the initial fabric has first reached ready-for-traffic"));
    Some(refused)
}

/// Serve a caller's `CreateTags` or `DeleteTags` at the admin `me` (born `me_id`): one span holds the
/// whole command, its outcome the reply's name.
pub async fn tags(me: &PathName, me_id: &NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, configs: Option<&crate::node_config::NodeConfigs>, first_ready: FirstReadyOnRoot<'_>, req: &StatusRequest) -> StatusReply {
    let (node_id, incarnation, change) = match req {
        StatusRequest::CreateTags { node_id, incarnation, labels } => (node_id, incarnation, Change::Create(labels)),
        StatusRequest::DeleteTags { node_id, incarnation, scope } => (node_id, incarnation, Change::Delete(scope)),
        _ => unreachable!("tags is called for CreateTags and DeleteTags only"),
    };
    let span = match &change {
        Change::Create(labels) => tracing::info_span!(
            "rdm.node_admin.node.tags.create.via-writer",
            node = %me, subject = tracing::field::Empty, node_id = %node_id, incarnation_id = %incarnation.as_str(), labels = %crate::node_rows::labels(labels),
            arrived = tracing::field::Empty, already_carried = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
        ),
        Change::Delete(scope) => tracing::info_span!(
            "rdm.node_admin.node.tags.delete.via-writer",
            node = %me, subject = tracing::field::Empty, node_id = %node_id, incarnation_id = %incarnation.as_str(), scope = scope.name(), labels = %match scope { TagScope::All => String::new(), TagScope::Labels(l) => crate::node_rows::labels(l) },
            left = tracing::field::Empty, not_carried = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
        ),
    };
    let reply = tracing::Instrument::instrument(
        async {
            match refused_before_first_ready(me, view, first_ready, req.op(), node_id, &span).await {
                Some(refused) => refused,
                None => {
                    // A snapshot of the view: no read lock is held across the row write's I/O.
                    let now = view.read().await.clone();
                    write(me, me_id, &now, storage, configs, node_id, incarnation, &change, &span).await
                }
            }
        },
        span.clone(),
    )
    .await;
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("{} decided", req.op()));
    reply
}

/// What a tags command changes.
enum Change<'a> {
    /// `node.tags.create`: these labels added.
    Create(&'a BTreeSet<String>),
    /// `node.tags.delete`: every label, or these.
    Delete(&'a TagScope),
}

#[allow(clippy::too_many_arguments)]
async fn write(me: &PathName, me_id: &NodeId, view: &Topology, storage: &dyn NodesStorage, configs: Option<&crate::node_config::NodeConfigs>, node_id: &NodeId, incarnation: &rdm_core::IncarnationId, change: &Change<'_>, span: &tracing::Span) -> StatusReply {
    let refusal = match change {
        Change::Create(labels) => rdm_core::status::create_tags_refusal(labels),
        Change::Delete(scope) => scope.refusal(),
    };
    if let Some(reason) = refusal {
        let why = format!("{me}: the labels of node {node_id} are refused and nothing is written: {reason}");
        span.record("detail", why.as_str());
        return rdm_core::reply::status::bounded_rejected_tags(&why);
    }
    // The subject is the exact birth the call names, wherever its row stands (`Topology::own`); one
    // this receiver does not hold is the receiver's own fact, never a peer that did not resolve.
    let Some(subject) = view.own(node_id) else {
        span.record("detail", format!("{me}: node {node_id} holds no birth in this admin's view; its tags cannot be written").as_str());
        return rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown);
    };
    span.record("subject", subject.name.to_string().as_str());
    // The writer of the row is the mesh-primary of the node's mesh, and no other admin (R-OW1).
    if !view.holds_mesh_seat(&subject.mesh, me_id) {
        let held_by = view.cohort_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
        let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me} does not hold it)", subject.mesh, subject.name);
        span.record("detail", needed.as_str());
        return rdm_core::reply::status::rejected_not_authority(NotAuthority::ReceiverNotPrimary { needed });
    }
    match subject.incarnation_id.as_ref() {
        Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
        None => {
            span.record("detail", format!("{me}: {} has no known birth; its tags cannot be written", subject.name).as_str());
            return rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown);
        }
        Some(_) => {}
    }
    let Some(door) = configs else {
        return rdm_core::reply::status::not_ready(format!("{me} has no node-RPC client yet, and so no door its record changes take turns at: the tags of {} are not written", subject.name));
    };
    // One change to a node record at a time, through the writer's held record: the row carries every
    // field the writer knows of it, the config included.
    let _one = door.one_at_a_time().await;
    let known = match door.written().known_row(storage, &subject.mesh, node_id).await {
        Ok(row) => row,
        Err(why) => return rdm_core::reply::status::bounded_node_row_refused(&subject.name.to_string(), &format!("{me}: {why}: the tags of {} are not written", subject.name)),
    };
    // A writer that meets the row finishes the release a rename left pending (proposal
    // node-properties-own-commands.md §2.1), whether or not this command writes; one that fails again
    // stays pending, named on its span.
    if let Some(r) = &known {
        let _ = crate::node_rows::finish_release(storage, me, r, "tags-put").await;
    }
    let was: BTreeSet<String> = known.as_ref().map(|r| r.tags.clone()).unwrap_or_default();
    // The key a rename moved the row from, and the successor that inherited it (its labels stay out of
    // the tags index), stay the row's while it stands at the same key.
    let renamed_from = known.as_ref().and_then(|r| r.renamed_from.clone().filter(|_| r.name == subject.name));
    let inherited_by = known.as_ref().and_then(|r| r.inherited_by.clone().filter(|_| r.name == subject.name));
    let config = known.map(|r| r.config).unwrap_or_else(|| subject.config.clone());
    let (tags, done) = match change {
        Change::Create(labels) => {
            let already: BTreeSet<String> = labels.intersection(&was).cloned().collect();
            let arrived: BTreeSet<String> = labels.difference(&was).cloned().collect();
            span.record("arrived", crate::node_rows::labels(&arrived).as_str()).record("already_carried", crate::node_rows::labels(&already).as_str());
            let tags: BTreeSet<String> = was.union(labels).cloned().collect();
            if let Some(reason) = rdm_core::status::tags_refusal(&tags) {
                let why = format!("{me}: {} carries {} and the create adds {}: {reason}; nothing is written", subject.name, crate::node_rows::labels(&was), crate::node_rows::labels(&arrived));
                span.record("detail", why.as_str());
                return rdm_core::reply::status::bounded_rejected_tags(&why);
            }
            (if arrived.is_empty() { None } else { Some(tags) }, StatusReply::TagsCreated { node_id: node_id.clone(), incarnation: incarnation.clone(), already_carried: already })
        }
        Change::Delete(scope) => {
            let (left, not_carried): (BTreeSet<String>, BTreeSet<String>) = match scope {
                TagScope::All => (was.clone(), BTreeSet::new()),
                TagScope::Labels(labels) => (labels.intersection(&was).cloned().collect(), labels.difference(&was).cloned().collect()),
            };
            span.record("left", crate::node_rows::labels(&left).as_str()).record("not_carried", crate::node_rows::labels(&not_carried).as_str());
            (if left.is_empty() { None } else { Some(was.difference(&left).cloned().collect()) }, StatusReply::TagsDeleted { node_id: node_id.clone(), incarnation: incarnation.clone(), not_carried })
        }
    };
    // Nothing to change writes nothing: a label carried is a key already put, one not carried a key
    // with nothing to tombstone.
    let Some(tags) = tags else { return done };
    let row = NodeRow { name: subject.name.clone(), node_id: node_id.clone(), status: subject.status, config, tags, allocated: None, renamed_from, inherited_by, app_types: door.written().row_of(node_id).and_then(|r| r.app_types) };
    match storage.put_node_row(&row).await {
        Ok(Ok(())) => {
            door.written().hold(&row);
            done
        }
        Ok(Err(held)) => {
            let why = format!("{me}: nodes.storage refused the row of {} ({node_id}): {held}", subject.name);
            span.record("detail", why.as_str());
            rdm_core::reply::status::bounded_node_row_refused(&subject.name.to_string(), &why)
        }
        Err(e) => {
            let why = format!("{me}: nodes.storage refused the row of {} ({node_id}): {e}", subject.name);
            span.record("detail", why.as_str());
            rdm_core::reply::status::bounded_node_row_refused(&subject.name.to_string(), &why)
        }
    }
}
