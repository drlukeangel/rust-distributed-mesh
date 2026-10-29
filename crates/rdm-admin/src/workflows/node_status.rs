//! Node status workflow (`docs/architecture/lifecycles/node-status.md`).
//!
//! Handles node status reporting, status declarations, and status RPC queries.

pub use crate::status_rpc::{
    declared_of_member, member_of_declared, ConnectionsDeletedReceipt, Declared, MeshReport,
    Reporter, SeatFold, StatusAuthority,
};
pub use crate::admin::ServingDoor;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabric_storage::{FabricIdentity, FabricRecord};
    use crate::model::{EndpointId, FabricId, IncarnationId, MeshId, Node, NodeId, NodeKind, NodeStatus, PathName};
    use crate::status_rpc::{Declaration, OperationEffect};
    use crate::storage::{BirthFacts, MeshRecord, NodeOperation, NodeRow};
    use rdm_core::MemberStatus;
    use rdm_core::status::{DeclaredOperation, MeshState, NodeState};
    use std::time::{Duration, Instant};

    #[test]
    fn declared_fold_records_preserves_greatest_state_operation_effects_watch_moves_and_row_cycle_transitions() {
        // docs/architecture/lifecycles/node-status.md:26 — "A node reports to its mesh's
        // primary, which sets its status. A report is an owed message ([Node RPC §43]) and
        // names the operation that produced the state when one did ([DeclaredOperation]): a
        // stop's `Stopped`, from which the mesh-primary publishes NodeStopped ([node.stop]
        // step 5), and a start's `Pending` and `Ready`."
        let path = PathName::new("mesh1", NodeKind::RpcNode, 1);
        let node_id = NodeId::mint();
        let older_inc = IncarnationId::mint();
        let current_inc = IncarnationId::mint();

        let mut row = NodeRow::new(path.clone(), node_id.clone(), NodeStatus::Starting);
        let mut older_birth = BirthFacts::new(older_inc.clone(), path.clone());
        older_birth.declared.insert(NodeState::Pending);
        let mut current_birth = BirthFacts::new(current_inc.clone(), path.clone());
        current_birth.declared.insert(NodeState::Pending);
        current_birth.declared.insert(NodeState::Ready);
        let start_op = DeclaredOperation {
            build_id: "build-status-1".to_string(),
            attempt: 1,
            operation: format!("node.start:{path}"),
        };
        let stop_op = DeclaredOperation {
            build_id: "build-status-2".to_string(),
            attempt: 1,
            operation: format!("node.stop:{path}"),
        };
        current_birth.operations.push(NodeOperation {
            operation: start_op.clone(),
            state: NodeState::Ready,
        });
        current_birth.operations.push(NodeOperation {
            operation: stop_op.clone(),
            state: NodeState::Stopped,
        });
        row.births.push(older_birth);
        row.births.push(current_birth);

        // Declaration::of_records and OperationEffect::of_records extract facts in record order.
        let declarations = Declaration::of_records([&row]);
        assert_eq!(
            declarations,
            vec![
                Declaration {
                    node_id: node_id.clone(),
                    incarnation_id: older_inc.clone(),
                    name: path.clone(),
                    state: NodeState::Pending,
                },
                Declaration {
                    node_id: node_id.clone(),
                    incarnation_id: current_inc.clone(),
                    name: path.clone(),
                    state: NodeState::Pending,
                },
                Declaration {
                    node_id: node_id.clone(),
                    incarnation_id: current_inc.clone(),
                    name: path.clone(),
                    state: NodeState::Ready,
                },
            ]
        );
        let effects = OperationEffect::of_records([&row]);
        assert_eq!(effects.len(), 2);
        assert_ne!(effects[0].key(), effects[1].key());
        // OperationEffect::key depends on (node_id, incarnation, build_id, attempt, operation), never state.
        let mut pending_same_op = effects[0].clone();
        pending_same_op.state = NodeState::Pending;
        assert_eq!(effects[0].key(), pending_same_op.key());

        // Row status transitions follow ready -> stop -> start for the newest birth only.
        assert!(!row.set_status(&older_inc, NodeStatus::Ready));
        assert!(row.set_status(&current_inc, NodeStatus::Pending));
        assert!(row.set_status(&current_inc, NodeStatus::Ready));
        assert!(row.set_status(&current_inc, NodeStatus::Stopping));
        assert!(row.set_status(&current_inc, NodeStatus::ConnectionsDeleting));
        assert!(row.set_status(&current_inc, NodeStatus::ConnectionsDeleted));
        assert!(row.set_status(&current_inc, NodeStatus::Leaving));
        assert!(row.set_status(&current_inc, NodeStatus::Left));
        assert!(row.set_status(&current_inc, NodeStatus::Stopped));
        assert!(!row.set_status(&current_inc, NodeStatus::Stopping));
        assert!(row.set_status(&current_inc, NodeStatus::Starting));
        assert!(row.set_status(&current_inc, NodeStatus::Pending));
        assert!(row.set_status(&current_inc, NodeStatus::Ready));

        // Declared fold_records folds only held births, preserves greater state, deduplicates fabric events, and moves watch.
        let mut declared = Declared::default();
        let rx = declared.subscribe();
        assert_eq!(*rx.borrow(), 0);
        declared.hold_node((node_id.clone(), current_inc.clone()), NodeState::Stopped);
        assert_eq!(*rx.borrow(), 1);
        // Holding the same state again does not move the watch.
        declared.hold_node((node_id.clone(), current_inc.clone()), NodeState::Stopped);
        assert_eq!(*rx.borrow(), 1);

        let mesh_id = MeshId::mint();
        let mut mesh_rec = MeshRecord::new(mesh_id.clone(), "mesh1");
        mesh_rec.states.insert(MeshState::Ready);
        let fabric_id = FabricId::mint();
        let mut fabric_rec = FabricRecord::new(FabricIdentity {
            fabric_id: fabric_id.clone(),
            name: "fabric1".to_string(),
        });
        fabric_rec.events = vec!["ready".to_string(), "ready".to_string()];

        let folded_count = declared.fold_records(
            &[mesh_rec],
            Some(&fabric_rec),
            &[row],
            |id, inc| id == &node_id && inc == &current_inc,
        );
        assert_eq!(folded_count, 1);
        assert_eq!(*rx.borrow(), 2);
        // Stopped > Ready, so the already-held Stopped state stands over the record's Ready.
        assert_eq!(declared.node(&node_id, &current_inc), Some(NodeState::Stopped));
        assert_eq!(declared.node(&node_id, &older_inc), None);
        assert_eq!(declared.meshes.get(&mesh_id), Some(&MeshState::Ready));
        assert_eq!(declared.fabric.get(&fabric_id), Some(&vec!["ready".to_string()]));
        assert_eq!(declared.operations.len(), 2);

        // Forgetting the node state moves the watch while preserving durable operation effects.
        declared.forget_node(&(node_id.clone(), current_inc.clone()));
        assert_eq!(*rx.borrow(), 3);
        assert_eq!(declared.node(&node_id, &current_inc), None);
        assert_eq!(declared.operations.len(), 2);

        // Reporter matching and member status conversion round-trip.
        assert_eq!(member_of_declared(NodeState::Pending), MemberStatus::Pending);
        assert_eq!(member_of_declared(NodeState::Ready), MemberStatus::Ready);
        assert_eq!(member_of_declared(NodeState::ConnectionsDeleting), MemberStatus::ConnectionsDeleting);
        assert_eq!(member_of_declared(NodeState::Stopped), MemberStatus::Leaving);
        assert_eq!(declared_of_member(MemberStatus::Pending), NodeState::Pending);
        assert_eq!(declared_of_member(MemberStatus::Ready), NodeState::Ready);
        assert_eq!(declared_of_member(MemberStatus::ConnectionsDeleting), NodeState::ConnectionsDeleting);
        assert_eq!(declared_of_member(MemberStatus::Leaving), NodeState::Stopped);

        let t0 = Instant::now();
        let t1 = t0 + Duration::from_millis(10);
        declared.record_report("mesh1", Reporter::Endpoint("ep-primary".to_string()), t0);
        declared.record_report("mesh1", Reporter::Birth(older_inc.clone()), t1);
        let mut primary_node = Node::allocated(PathName::new("mesh1", NodeKind::NodeAdmin, 1));
        primary_node.node_id = node_id;
        primary_node.incarnation_id = Some(current_inc.clone());
        primary_node.endpoint_id = Some(EndpointId("ep-primary".to_string()));
        assert_eq!(
            declared.report_of("mesh1", Some(&primary_node)),
            Some(MeshReport {
                incarnation: current_inc,
                at: t0,
            })
        );
        assert_eq!(
            declared.report_of("mesh1", None),
            Some(MeshReport {
                incarnation: older_inc,
                at: t1,
            })
        );
    }
}

