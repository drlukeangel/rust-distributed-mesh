//! Node recovery workflow (`docs/architecture/lifecycles/node-recovery.md`).
//!
//! Reconciles an unheard or failed node: detecting silence, running the one tickle ladder
//! ([`crate::workflows::node_tickle`]), proving runtime exit, and initiating recovery attempts.

pub use crate::build_run::{run_opened_recovery, run_recovery, Recovered, RecoveryNotRun};
pub use crate::offline::{tickle_ladder, tickle_ladder_at_default_budget, VIA_PEER_TICKLE_FANOUT};
pub use crate::admin::{durable_births, recovery_due, watched_births, Inspected};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Fabric, FabricId, IncarnationId, Node, NodeId, NodeStatus, PathName, ProviderKind,
        ScopeStatus,
    };
    use crate::storage::RuntimeRow;
    use crate::topology::Topology;
    use std::collections::HashSet;
    use std::time::Duration;

    // @feature: node-recovery
    /// CONTRACT: docs/architecture/lifecycles/node-recovery.md:19
    /// "Detection: The mesh-primary's tick (every gossip interval) reads the members its view marks
    ///  `PendingReconnect`: unheard past the staleness floor, or marked at once when a command to them
    ///  was never sent for a reachability reason"
    /// CONTRACT: docs/architecture/lifecycles/node-recovery.md:46
    /// "Only the mesh-primary acts. Half a floor after the mark it runs node investigation on the
    ///  exact birth: the by-destination connections index, node.tickle direct, the same tickle via up
    ///  to two eligible live peers of the birth's mesh sent at once, then node.runtime.inspect."
    #[test]
    fn watched_and_durable_births_filter_by_mesh_and_recovery_due_requires_mesh_primary_and_exact_incarnation()
    {
        assert_eq!(VIA_PEER_TICKLE_FANOUT, 2);
        assert_eq!(
            tickle_ladder(Duration::from_secs(5)),
            Duration::from_secs(10),
            "tickle ladder spends one call deadline direct plus one call deadline for concurrent peer fan-out"
        );

        let me_id = NodeId::mint();
        let mut me_node = Node::allocated("mesh1.admin.1".parse().unwrap());
        me_node.node_id = me_id.clone();
        me_node.incarnation_id = Some(IncarnationId::mint());
        me_node.status = NodeStatus::PendingReconnect;

        let mut silent_member = Node::allocated("mesh1.rpc.1".parse().unwrap());
        let silent_inc = IncarnationId::mint();
        silent_member.incarnation_id = Some(silent_inc.clone());
        silent_member.status = NodeStatus::PendingReconnect;

        let mut ready_member = Node::allocated("mesh1.rpc.2".parse().unwrap());
        ready_member.incarnation_id = Some(IncarnationId::mint());
        ready_member.status = NodeStatus::Ready;

        let mut peer_mesh_silent = Node::allocated("mesh2.rpc.1".parse().unwrap());
        peer_mesh_silent.incarnation_id = Some(IncarnationId::mint());
        peer_mesh_silent.status = NodeStatus::PendingReconnect;

        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fab1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![
                me_node,
                silent_member.clone(),
                ready_member.clone(),
                peer_mesh_silent,
            ],
        };

        let rt_fact = |pid: u32, start: u64| rdm_core::RuntimeFact {
            deployment_id: format!("dep-{pid}"),
            provider: rdm_core::RuntimeProvider::Process,
            control_domain: "process:boot1:ns1".into(),
            locator: rdm_core::RuntimeLocator::Process { pid, start },
        };
        let durable_unheard_id = NodeId::mint();
        let durable_unheard_inc = IncarnationId::mint();
        let durable_deleted_id = NodeId::mint();
        let durable_rows = vec![
            // Path not in view, planned, not deleted -> included as PendingReconnect
            RuntimeRow {
                node_id: durable_unheard_id.clone(),
                name: "mesh1.rpc.3".parse().unwrap(),
                incarnation_id: durable_unheard_inc.clone(),
                runtime: rt_fact(4100, 10),
                data_dir: None,
            },
            // Path already in view (`mesh1.rpc.2`) -> skipped
            RuntimeRow {
                node_id: ready_member.node_id.clone(),
                name: ready_member.name.clone(),
                incarnation_id: ready_member.incarnation_id.clone().unwrap(),
                runtime: rt_fact(4101, 11),
                data_dir: None,
            },
            // Deleted birth -> skipped
            RuntimeRow {
                node_id: durable_deleted_id.clone(),
                name: "mesh1.rpc.4".parse().unwrap(),
                incarnation_id: IncarnationId::mint(),
                runtime: rt_fact(4102, 12),
                data_dir: None,
            },
            // Unplanned path -> skipped
            RuntimeRow {
                node_id: NodeId::mint(),
                name: "mesh1.rpc.99".parse().unwrap(),
                incarnation_id: IncarnationId::mint(),
                runtime: rt_fact(4103, 13),
                data_dir: None,
            },
        ];

        let planned = |p: &PathName| p.to_string() != "mesh1.rpc.99";
        let deleted = |id: &str| id == durable_deleted_id.as_str();
        let durable = durable_births(&view, &durable_rows, &planned, &deleted);
        assert_eq!(durable.len(), 1);
        assert_eq!(durable[0].node_id, durable_unheard_id);
        assert_eq!(durable[0].incarnation_id, Some(durable_unheard_inc.clone()));
        assert_eq!(durable[0].status, NodeStatus::PendingReconnect);

        let watched = watched_births(&view, &me_id, "mesh1", &durable);
        assert_eq!(
            watched.keys().cloned().collect::<Vec<_>>(),
            {
                let mut keys = vec![
                    silent_member.node_id.to_string(),
                    durable_unheard_id.to_string(),
                ];
                keys.sort();
                keys
            },
            "watched births include silent view members of own mesh and durable births, excluding self, ready members, and other meshes"
        );

        let mut offline = HashSet::new();
        offline.insert((silent_member.node_id.clone(), silent_inc.clone()));
        // Wrong incarnation for durable_unheard_id -> must not be due
        offline.insert((durable_unheard_id.clone(), IncarnationId::mint()));

        assert!(
            recovery_due(false, "mesh1", &watched, &offline).is_empty(),
            "secondary node-admin never inspects or opens recovery"
        );

        let due = recovery_due(true, "mesh1", &watched, &offline);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].node_id, silent_member.node_id);
        assert_eq!(due[0].incarnation_id, Some(silent_inc));
    }
}
