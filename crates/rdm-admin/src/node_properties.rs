//! A caller's node property commands at the node's mesh-primary, the one writer of its mesh's rows
//! (proposal node-properties-own-commands.md §1, §3; R-OW1, R14, R-TG1): each command is admitted at
//! any time (Q166, Q171) and runs once through the writer's held record, one change at a time with
//! every other change to a node record (`crate::node_config::NodeConfigs::one_at_a_time`). Here:
//! `node.tag.create` / `node.tag.delete` (node-rpc-envelope.md node.tags): each label is
//! its own key of the node store's tags index, kept with the row by the store adapter (a label the
//! row stops carrying is tombstoned, one it gains is put); a label the node already carries is ignored
//! and writes nothing. Neither has a precondition, neither touches the
//! name, and the node is told nothing: its tags live in its row and the index, never in its digest.

use crate::model::PathName;
use crate::storage::{NodeRow, NodesStorage};
use crate::topology::Topology;
use rdm_core::NodeId;
use rdm_core::status::{NotPrimary, StatusReply, StatusRequest, TagScope};
use std::collections::BTreeSet;
use tokio::sync::RwLock;

/// Serve a caller's `CreateTags` or `DeleteTags` at the admin `me` (born `me_id`): one span holds the
/// whole command, its outcome the reply's name. At the fabric-primary of a node of another mesh, the
/// command goes down to the node's mesh-primary, owed until it answers, and the answer's labels are
/// written to the node's record in `atlas` (Q203, Q204: each op writes the atlas; commands go down,
/// fabric-primary to mesh-primary; [`crate::node_config::NodeConfigs::send_down`]). A caller's
/// (`sent_by`) at an admin that is not the fabric-primary is carried up to it and its answer handed back
/// ([`crate::node_config::NodeConfigs::forward_up`]), as `node.config.update` is.
#[allow(clippy::too_many_arguments)]
pub async fn tags(me: &PathName, me_id: &NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, configs: Option<&crate::node_config::NodeConfigs>, atlas: Option<&crate::atlas::Atlas>, sent_by: crate::node_config::SentBy, req: &StatusRequest) -> StatusReply {
    let (node_id, incarnation, change) = match req {
        StatusRequest::CreateTags { node_id, incarnation, labels } => (node_id, incarnation, Change::Create(labels)),
        StatusRequest::DeleteTags { node_id, incarnation, scope } => (node_id, incarnation, Change::Delete(scope)),
        _ => unreachable!("tags is called for CreateTags and DeleteTags only"),
    };
    let mut first = view.read().await.clone();
    if let (Some(door), crate::node_config::SentBy::Caller) = (configs, sent_by) {
        if !first.is_fabric_primary(me_id) {
            match door.forward_up(me, me_id, &first, view, req).await {
                crate::node_config::Up::Answered(reply) => return reply,
                crate::node_config::Up::InProcess(now) => first = now,
            }
        }
    }
    if let (Some(door), Some(subject)) = (configs, first.own(node_id).cloned()) {
        if first.is_fabric_primary(me_id) && !first.is_mesh_primary(&subject.mesh, me_id) {
            let span = tracing::info_span!(
                "rdm.node_admin.node.update.via-fabric-primary",
                node = %me, subject = %subject.name, node_id = %node_id, op = req.op(),
                mesh_primary = tracing::field::Empty, route = tracing::field::Empty, sends = tracing::field::Empty,
                outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
            );
            let down = tracing::Instrument::instrument(door.send_down(me, me_id, &first, view, &subject, req), span.clone()).await;
            if let crate::node_config::Down::Answered(reply) = down {
                if let Some(a) = atlas {
                    match (&reply, &change) {
                        (StatusReply::TagsCreated { .. }, Change::Create(labels)) => {
                            a.update_record(node_id, |r| r.tags.extend(labels.iter().cloned()));
                        }
                        (StatusReply::TagsDeleted { .. }, Change::Delete(TagScope::All)) => {
                            a.update_record(node_id, |r| r.tags.clear());
                        }
                        (StatusReply::TagsDeleted { .. }, Change::Delete(TagScope::Labels(labels))) => {
                            a.update_record(node_id, |r| r.tags.retain(|l| !labels.contains(l)));
                        }
                        _ => {}
                    }
                }
                span.record("outcome", reply.name());
                span.in_scope(|| tracing::info!("{} at the fabric-primary decided", req.op()));
                return reply;
            }
            // This admin holds the node's mesh-primary now, or no longer holds the birth: served here.
        }
    }
    let span = match &change {
        Change::Create(labels) => tracing::info_span!(
            "rdm.node_admin.tag.create.via-writer",
            node = %me, subject = tracing::field::Empty, node_id = %node_id, incarnation_id = %incarnation.as_str(), labels = %crate::node_rows::labels(labels),
            arrived = tracing::field::Empty, already_carried = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
        ),
        Change::Delete(scope) => tracing::info_span!(
            "rdm.node_admin.tag.delete.via-writer",
            node = %me, subject = tracing::field::Empty, node_id = %node_id, incarnation_id = %incarnation.as_str(), scope = scope.name(), labels = %match scope { TagScope::All => String::new(), TagScope::Labels(l) => crate::node_rows::labels(l) },
            left = tracing::field::Empty, not_carried = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
        ),
    };
    let reply = tracing::Instrument::instrument(
        async {
            // A snapshot of the view: no read lock is held across the row write's I/O.
            let now = view.read().await.clone();
            write(me, me_id, &now, storage, configs, node_id, incarnation, &change, &span).await
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
    /// `node.tag.create`: these labels added.
    Create(&'a BTreeSet<String>),
    /// `node.tag.delete`: every label, or these.
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
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
    };
    span.record("subject", subject.name.to_string().as_str());
    // The writer of the row is the mesh-primary of the node's mesh, and no other admin (R-OW1).
    if !view.is_mesh_primary(&subject.mesh, me_id) {
        let held_by = view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
        let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me} does not hold it)", subject.mesh, subject.name);
        span.record("detail", needed.as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
    }
    match subject.incarnation_id.as_ref() {
        Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
        None => {
            span.record("detail", format!("{me}: {} has no known birth; its tags cannot be written", subject.name).as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        }
        Some(_) => {}
    }
    let Some(door) = configs else {
        return rdm_core::reply::status::not_ready(format!("{me} has no node-RPC client yet, and so no door its record changes take turns at: the tags of {} are not written", subject.name));
    };
    // One change to a node record at a time, through the writer's held record: the row carries every
    // field the writer knows of it, the config included.
    let _one = door.one_at_a_time().await;
    let known = match door.written().known_row(storage, node_id).await {
        Ok(row) => row,
        Err(why) => return rdm_core::reply::status::bounded_node_row_refused(&subject.name.to_string(), &format!("{me}: {why}: the tags of {} are not written", subject.name)),
    };
    // A writer that meets the row finishes the release a rename left pending (proposal
    // node-properties-own-commands.md §2.1), whether or not this command writes; one that fails again
    // stays pending, named on its span.
    if let Some(r) = &known {
        let _ = crate::node_rows::finish_release(storage, door.written(), me, r, "tags-put").await;
    }
    let was: BTreeSet<String> = known.as_ref().map(|r| r.tags.clone()).unwrap_or_default();
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
    // The record this writer holds, its tags changed: written whole, its tags index moves with it.
    let base = NodeRow::new(subject.name.clone(), node_id.clone(), subject.status);
    // A tag put moves no status (node-status.md: "never a meta or config put"): the record's stands.
    match door.written().update(storage, node_id, Some(base), |r| {
        r.tags = tags;
    }).await {
        Ok(_) => done,
        Err(why) => {
            let why = format!("{me}: {} ({node_id}): {why}", subject.name);
            span.record("detail", why.as_str());
            rdm_core::reply::status::bounded_node_row_refused(&subject.name.to_string(), &why)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};
    use crate::node_config::{NodeConfigs, SentBy};
    use crate::storage::MemoryNodesStorage;
    use rdm_transport::{NodeRpcClient, StaticResolver};
    use std::sync::Arc;

    // CONTRACT (`docs/architecture/node-rpc-envelope.md:323` — "`node.tag.create { label | [labels] }`
    // adds one label or a list; `node.tag.delete { All | [labels] }` removes every label or the named
    // ones ... Each label is its own keyed row (label, node id) in the node store's tags index. Create is
    // a blind put of that key, so creating a label the node already carries is ignored and writes no
    // duplicate; delete tombstones the named keys. A label is 1-64 characters of a-z 0-9 - _ . ~, with no
    // leading '.', and a node carries at most 32; a label outside that grammar, or one past the 32, is
    // refused by name and nothing is written"): `tags` writes label index entries, reports
    // `already_carried` and `not_carried`, skips writing when no label changes, and refuses invalid
    // grammar, >32 labels, stale incarnations, and non-mesh-primary receivers.
    // @feature: node-tags
    #[tokio::test]
    async fn tags_create_and_delete_maintain_index_report_carried_deltas_and_enforce_label_grammar_and_cap() {
        let mut admin1 = Node::allocated("mesh1.admin.1".parse().unwrap());
        admin1.status = NodeStatus::Ready;
        admin1.is_primary = true;
        admin1.is_fabric_primary = true;
        admin1.incarnation_id = Some(IncarnationId::mint());

        let mut admin2 = Node::allocated("mesh1.admin.2".parse().unwrap());
        admin2.status = NodeStatus::Ready;
        admin2.incarnation_id = Some(IncarnationId::mint());

        let mut rpc1 = Node::allocated("mesh1.rpc.1".parse().unwrap());
        rpc1.status = NodeStatus::Ready;
        let rpc1_inc = IncarnationId::mint();
        rpc1.incarnation_id = Some(rpc1_inc.clone());

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
            nodes: vec![admin1.clone(), admin2.clone(), rpc1.clone()],
        });

        let storage = MemoryNodesStorage::default();
        let written = Arc::new(crate::node_rows::WrittenRows::default());
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::from_bytes(&[9u8; 32]), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let door = NodeConfigs::new(client, Default::default(), written.clone());

        let set = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();

        // 1. Invalid grammar (leading '.') refused before writer check.
        let bad_req = StatusRequest::CreateTags {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc.clone(),
            labels: set(&[".hidden"]),
        };
        assert!(matches!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &bad_req).await,
            StatusReply::RejectedTags { .. }
        ));

        // 2. Non-mesh-primary refused ReceiverNotPrimary.
        let create_req = StatusRequest::CreateTags {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc.clone(),
            labels: set(&["gold", "ssd"]),
        };
        assert!(matches!(
            tags(&admin2.name, &admin2.node_id, &view, &storage, Some(&door), None, SentBy::Other, &create_req).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } }
        ));

        // 3. Create {"gold", "ssd"} writes row at revision 1 and indexes both labels.
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &create_req).await,
            StatusReply::TagsCreated {
                node_id: rpc1.node_id.clone(),
                incarnation: rpc1_inc.clone(),
                already_carried: BTreeSet::new(),
            }
        );
        assert_eq!(written.row_of(&rpc1.node_id).unwrap().revision, 1);
        assert_eq!(storage.tagged("gold").await.unwrap(), BTreeSet::from([rpc1.node_id.clone()]));
        assert_eq!(storage.tagged("ssd").await.unwrap(), BTreeSet::from([rpc1.node_id.clone()]));

        // 4. Repeat create {"gold"} reports already_carried={"gold"} and writes nothing (revision stays 1).
        let dup_req = StatusRequest::CreateTags {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc.clone(),
            labels: set(&["gold"]),
        };
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &dup_req).await,
            StatusReply::TagsCreated {
                node_id: rpc1.node_id.clone(),
                incarnation: rpc1_inc.clone(),
                already_carried: set(&["gold"]),
            }
        );
        assert_eq!(written.row_of(&rpc1.node_id).unwrap().revision, 1);

        // 5. Delete {"gold", "missing"} tombstones "gold", reports not_carried={"missing"}, steps revision to 2.
        let del_req = StatusRequest::DeleteTags {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc.clone(),
            scope: TagScope::Labels(set(&["gold", "missing"])),
        };
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &del_req).await,
            StatusReply::TagsDeleted {
                node_id: rpc1.node_id.clone(),
                incarnation: rpc1_inc.clone(),
                not_carried: set(&["missing"]),
            }
        );
        assert!(storage.tagged("gold").await.unwrap().is_empty());
        assert_eq!(written.row_of(&rpc1.node_id).unwrap().tags, set(&["ssd"]));
        assert_eq!(written.row_of(&rpc1.node_id).unwrap().revision, 2);

        // 6. Delete All clears remaining tags and tombstones "ssd".
        let del_all = StatusRequest::DeleteTags {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc,
            scope: TagScope::All,
        };
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &del_all).await,
            StatusReply::TagsDeleted {
                node_id: rpc1.node_id.clone(),
                incarnation: rpc1.incarnation_id.unwrap(),
                not_carried: BTreeSet::new(),
            }
        );
        assert!(storage.tagged("ssd").await.unwrap().is_empty());
        assert!(written.row_of(&rpc1.node_id).unwrap().tags.is_empty());
        ep.close().await;
    }
}
