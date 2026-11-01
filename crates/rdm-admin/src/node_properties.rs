//! Tag commands admitted by the node's mesh-primary and persisted by the node's own
//! serialized property writer. Node IDs select subjects; incarnation identifies the exact
//! birth. Path and name remain separate descriptive fields. Missing property storage
//! refuses the command rather than writing properties into a combined lifecycle row.

use crate::model::PathName;
use crate::storage::NodesStorage;
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
                node_path = %me.path, node_name = %me.name, subject_path = %subject.name.path, subject_name = %subject.name.name, node_id = %node_id, op = req.op(),
                mesh_primary = tracing::field::Empty, route = tracing::field::Empty, sends = tracing::field::Empty,
                outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
            );
            let down = tracing::Instrument::instrument(door.send_down(me, me_id, &first, view, &subject, req), span.clone()).await;
            if let crate::node_config::Down::Answered(reply) = down {
                if let Some(a) = atlas {
                    if matches!(reply, StatusReply::TagsCreated { .. } | StatusReply::TagsDeleted { .. }) {
                        if let Err(error) = a.refresh_record(storage, node_id).await {
                            span.record("detail", error.as_str());
                        }
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
            node_path = %me.path, node_name = %me.name, subject_path = tracing::field::Empty, subject_name = tracing::field::Empty, node_id = %node_id, incarnation_id = %incarnation.as_str(), labels = %crate::node_rows::labels(labels),
            arrived = tracing::field::Empty, already_carried = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
        ),
        Change::Delete(scope) => tracing::info_span!(
            "rdm.node_admin.tag.delete.via-writer",
            node_path = %me.path, node_name = %me.name, subject_path = tracing::field::Empty, subject_name = tracing::field::Empty, node_id = %node_id, incarnation_id = %incarnation.as_str(), scope = scope.name(), labels = %match scope { TagScope::All => String::new(), TagScope::Labels(l) => crate::node_rows::labels(l) },
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
async fn write(_me: &PathName, me_id: &NodeId, view: &Topology, _storage: &dyn NodesStorage, configs: Option<&crate::node_config::NodeConfigs>, node_id: &NodeId, incarnation: &rdm_core::IncarnationId, change: &Change<'_>, span: &tracing::Span) -> StatusReply {
    let refusal = match change {
        Change::Create(labels) => rdm_core::status::create_tags_refusal(labels),
        Change::Delete(scope) => scope.refusal(),
    };
    if let Some(reason) = refusal {
        let why = format!("{me_id}: the labels of node {node_id} are refused and nothing is written: {reason}");
        span.record("detail", why.as_str());
        return rdm_core::reply::status::bounded_rejected_tags(&why);
    }
    // The subject is the exact birth the call names, wherever its row stands (`Topology::own`); one
    // this receiver does not hold is the receiver's own fact, never a peer that did not resolve.
    let Some(subject) = view.own(node_id) else {
        span.record("detail", format!("{me_id}: node {node_id} holds no birth in this admin's view; its tags cannot be written").as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
    };
    span.record("subject_path", subject.name.path.as_str()).record("subject_name", subject.name.name.as_str());
    // The writer of the row is the mesh-primary of the node's mesh, and no other admin (R-OW1).
    if !view.is_mesh_primary(&subject.mesh, me_id) {
        let held_by = view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.node_id));
        let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me_id} does not hold it)", subject.mesh, subject.node_id);
        span.record("detail", needed.as_str());
        return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
    }
    match subject.incarnation_id.as_ref() {
        Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
        None => {
            span.record("detail", format!("{me_id}: {} has no known birth; its tags cannot be written", subject.node_id).as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        }
        Some(_) => {}
    }
    let Some(door) = configs else {
        return rdm_core::reply::status::not_ready(format!("{me_id} has no node-RPC client yet, and so no door its record changes take turns at: the tags of {} are not written", subject.node_id));
    };
    if door.records().is_some() {
        let change = match change {
            Change::Create(labels) => rdm_node::node_records::TagChange::Create((*labels).clone()),
            Change::Delete(scope) => rdm_node::node_records::TagChange::Delete((*scope).clone()),
        };
        let reply = if node_id == me_id {
            match door.own_tags(subject, change).await {
                Some(result) => result.map_err(|error| error.to_string()),
                None => return rdm_core::reply::status::not_ready(format!("{me_id} has no own property writer yet")),
            }
        } else {
            let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
            let rpc = rdm_client::NodeRpc::new(door.client());
            let options = rdm_transport::CallOptions::default();
            match change {
                rdm_node::node_records::TagChange::Create(labels) => rpc.tags_create(node_id, &rdm_transport::RouteChoice::Direct, &birth, &labels, &options).await,
                rdm_node::node_records::TagChange::Delete(scope) => rpc.tags_delete(node_id, &rdm_transport::RouteChoice::Direct, &birth, &scope, &options).await,
            }.map_err(|error| error.to_string())
        };
        return match reply {
            Ok(reply) => reply,
            Err(error) => rdm_core::reply::status::bounded_node_row_refused(&subject.node_id.to_string(), &error),
        };
    }
    rdm_core::reply::status::not_ready(format!("{me_id} has no bound node property records; the tags of {} are not written", subject.node_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};
    use crate::node_config::{NodeConfigs, SentBy};
    use crate::storage::MemoryNodesStorage;
    use rdm_transport::{NodeRpcClient, StaticResolver};
    use std::sync::Arc;

    // Node-owned tags preserve unrelated properties, report deltas, and refuse invalid
    // labels, stale births and unavailable property storage without writing combined rows.
    // @feature: node-tags
    #[tokio::test]
    async fn tags_use_node_owned_properties_and_refuse_unbound_records() {
        let mut admin1 = Node::allocated(rdm_core::PathName::new("mesh1.admin", "1").unwrap(), "mesh1", rdm_core::NodeKind::NodeAdmin);
        admin1.status = NodeStatus::Ready;
        admin1.is_primary = true;
        admin1.is_fabric_primary = true;
        admin1.incarnation_id = Some(IncarnationId::mint());

        let mut admin2 = Node::allocated(rdm_core::PathName::new("mesh1.admin", "2").unwrap(), "mesh1", rdm_core::NodeKind::NodeAdmin);
        admin2.status = NodeStatus::Ready;
        admin2.incarnation_id = Some(IncarnationId::mint());

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
            nodes: vec![admin1.clone(), admin2.clone()],
        });

        let storage = MemoryNodesStorage::default();
        let written = Arc::new(crate::node_rows::WrittenRows::default());
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::from_bytes(&[9u8; 32]), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let door = NodeConfigs::new(client, written.clone());

        let set = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();

        // 1. Invalid grammar (leading '.') refused before writer check.
        let bad_req = StatusRequest::CreateTags {
            node_id: admin1.node_id.clone(),
            incarnation: admin1.incarnation_id.clone().unwrap(),
            labels: set(&[".hidden"]),
        };
        assert!(matches!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &bad_req).await,
            StatusReply::RejectedTags { .. }
        ));

        // 2. Non-mesh-primary refused ReceiverNotPrimary.
        let create_req = StatusRequest::CreateTags {
            node_id: admin1.node_id.clone(),
            incarnation: admin1.incarnation_id.clone().unwrap(),
            labels: set(&["gold", "ssd"]),
        };
        assert!(matches!(
            tags(&admin2.name, &admin2.node_id, &view, &storage, Some(&door), None, SentBy::Other, &create_req).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } }
        ));

        assert!(matches!(tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &create_req).await, StatusReply::NotReady { .. }));
        assert!(storage.node(&admin1.node_id).await.unwrap().is_none());
        use rdm_node::node_records::{MemoryNodeRecords, NodeRecordsStorage};
        let records = Arc::new(MemoryNodeRecords::default());
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let snapshot = view.read().await.clone();
        let channels = rdm_transport::topology::TopologyView::join(&gossip, &ep, &snapshot.fabric.id, "mesh1", snapshot.meshes[0].id.as_ref().unwrap(), admin1.node_id.as_str(), rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        door.uses_records(records.clone(), channels.clone());
        door.bound_own(&admin1, rdm_core::launch::InitialProperties { config: [("preserved".into(), "value".into())].into(), tags: Default::default(), applied: Default::default() }).await.unwrap();

        // 3. Create {"gold", "ssd"} persists both labels in node-owned properties.
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &create_req).await,
            StatusReply::TagsCreated {
                node_id: admin1.node_id.clone(),
                incarnation: admin1.incarnation_id.clone().unwrap(),
                already_carried: BTreeSet::new(),
            }
        );

        // 4. Repeat create {"gold"} reports already_carried={"gold"} without changing the tag set.
        let dup_req = StatusRequest::CreateTags {
            node_id: admin1.node_id.clone(),
            incarnation: admin1.incarnation_id.clone().unwrap(),
            labels: set(&["gold"]),
        };
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &dup_req).await,
            StatusReply::TagsCreated {
                node_id: admin1.node_id.clone(),
                incarnation: admin1.incarnation_id.clone().unwrap(),
                already_carried: set(&["gold"]),
            }
        );

        // 5. Delete {"gold", "missing"} removes "gold" and reports not_carried={"missing"}.
        let del_req = StatusRequest::DeleteTags {
            node_id: admin1.node_id.clone(),
            incarnation: admin1.incarnation_id.clone().unwrap(),
            scope: TagScope::Labels(set(&["gold", "missing"])),
        };
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &del_req).await,
            StatusReply::TagsDeleted {
                node_id: admin1.node_id.clone(),
                incarnation: admin1.incarnation_id.clone().unwrap(),
                not_carried: set(&["missing"]),
            }
        );
        assert_eq!(records.properties(&admin1.node_id).await.unwrap().unwrap().tags, set(&["ssd"]));

        let too_many = StatusRequest::CreateTags {
            node_id: admin1.node_id.clone(), incarnation: admin1.incarnation_id.clone().unwrap(),
            labels: (0..32).map(|i| format!("tag-{i}")).collect(),
        };
        assert!(matches!(tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &too_many).await, StatusReply::RejectedTags { .. }));
        assert_eq!(records.properties(&admin1.node_id).await.unwrap().unwrap().tags, set(&["ssd"]));
        let stale = StatusRequest::CreateTags { node_id: admin1.node_id.clone(), incarnation: IncarnationId::mint(), labels: set(&["stale"]) };
        assert!(matches!(tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &stale).await, StatusReply::RejectedStaleIncarnation { .. }));

        // 6. Delete All clears the remaining tags.
        let del_all = StatusRequest::DeleteTags {
            node_id: admin1.node_id.clone(),
            incarnation: admin1.incarnation_id.clone().unwrap(),
            scope: TagScope::All,
        };
        assert_eq!(
            tags(&admin1.name, &admin1.node_id, &view, &storage, Some(&door), None, SentBy::Other, &del_all).await,
            StatusReply::TagsDeleted {
                node_id: admin1.node_id.clone(),
                incarnation: admin1.incarnation_id.clone().unwrap(),
                not_carried: BTreeSet::new(),
            }
        );
        assert!(records.properties(&admin1.node_id).await.unwrap().unwrap().tags.is_empty());
        let retained = records.properties(&admin1.node_id).await.unwrap().unwrap();
        assert_eq!(retained.config, [("preserved".into(), "value".into())].into());
        assert!(storage.node(&admin1.node_id).await.unwrap().is_none());
        assert!(written.row_of(&admin1.node_id).is_none());
        ep.close().await;
    }
}
