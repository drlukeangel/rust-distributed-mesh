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

    /// CONTRACT: `docs/architecture/lifecycles/node-status.md:26` — "A node reports to its mesh's
    /// primary, which sets its status. A report is an owed message ([Node RPC §43]) and names the
    /// operation that produced the state when one did ([DeclaredOperation]): a stop's `Stopped`,
    /// from which the mesh-primary publishes NodeStopped ([node.stop] step 5), and a start's
    /// `Pending` and `Ready`."
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

    /// CONTRACT:
    /// - `docs/architecture/lifecycles/node-status.md:5` — "The internal status report and apply; not a caller op. Status moves only through the lifecycle actions and workflows, never a meta or config put ([Meta and config](../node-rpc-envelope.md#node-properties)). The [Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm) owns the codec, the direction and transition checks and the refusal fields."
    /// - `docs/architecture/lifecycles/node-status.md:15` — "A report is sent when owed and again only on an eligible event:"
    /// - `docs/architecture/lifecycles/node-status.md:17` — "- its primary's address changes to a deliverable exact instance;"
    /// - `docs/architecture/lifecycles/node-status.md:18` — "- a failed delivery gains a viable route;"
    /// - `docs/architecture/lifecycles/node-status.md:19` — "- the primary addresses this node with the state's down call."
    /// - `docs/architecture/lifecycles/node-status.md:21` — "Typed replies, including named refusals, are terminal. One coalesced wake drives the work, never a timer. An unreachable target schedules no report."
    /// - `docs/architecture/lifecycles/node-status.md:22` — "`node.get` maps the exact-instance read to ProbeNodeState; its Current reply reasserts held status and does not manufacture a transition or a new change timestamp."
    /// - `docs/architecture/lifecycles/node-status.md:26` — "A node reports to its mesh's primary, which sets its status. A report is an owed message ([Node RPC §43](../node-rpc.md#43-owed-messages-delivery-is-the-runtimes)) and names the operation that produced the state when one did ([DeclaredOperation](../node-rpc-envelope.md#status-op-0x1b-rdm)): a stop's `Stopped`, from which the mesh-primary publishes NodeStopped ([node.stop](node-stop.md) step 5), and a start's `Pending` and `Ready`."
    /// - `docs/architecture/lifecycles/node-status.md:38` — "| Caller / receiver | The node's mesh-primary, downward to the exact accepted node |"
    /// - `docs/architecture/lifecycles/node-status.md:39` — "| Kept | node.id, incarnation, endpoint key, port, immutable path and runtime |"
    /// - `docs/architecture/lifecycles/node-status.md:40` — "| Mapping | ApplyNodeState on the [Status schema](../node-rpc-envelope.md#status-op-0x1b-rdm); connections-deleting is entered only by [node.connections.delete](node-connections-delete.md) with `{ scope: All }` |"
    /// - `docs/architecture/lifecycles/node-status.md:41` — "| Result | a typed reply, or a named typed refusal |"
    /// - `docs/architecture/lifecycles/node-status.md:42` — "| Completion | Stream result for the accepted exact subject; no runtime exit inferred |"
    /// - `docs/architecture/lifecycles/node-status.md:44` — "Workflow actions run the status apply; how a node leaves the stopped status is [Stopped nodes](states.md#stopped-nodes)."
    /// - `docs/architecture/lifecycles/node-status.md:50` — "A refusal is typed and named; the workflow that ran the apply records it as its own step's `Failed { step, reason }`."
    /// - `docs/architecture/lifecycles/node-status.md:55` — "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans)."
    #[tokio::test]
    async fn node_status_report_eligible_events_probe_reassert_and_status_apply_conformance() {
        use crate::deployment::pipeline::{command_admission, CommandAdmission};
        use crate::model::{Fabric, ProviderKind, ScopeStatus};
        use crate::status_declare::{DeclareWake, Declarer};
        use crate::storage::{MemoryMeshStorage, MemoryNodesStorage, NodesStorage};
        use crate::topology::Topology;
        use rdm_core::outcome::{PreCommit, RequestFinished};
        use rdm_core::protocol::NodeProtocol;
        use rdm_core::status::{transition, NotPrimary, Status, StatusReply, StatusRequest, Transition};
        use rdm_node::owed::{eligible, Destination, Sent};
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::{Arc, Mutex};

        // node-status.md:5-9 — Status (op 0x1B) owns the codec, direction and transition checks, and refusal fields.
        assert_eq!(Status::OP, 0x1B);
        assert_eq!(transition(None, NodeState::Pending), Transition::Apply);
        assert_eq!(transition(Some(NodeState::Ready), NodeState::Ready), Transition::AlreadyApplied);
        assert_eq!(
            transition(Some(NodeState::Ready), NodeState::Pending),
            Transition::Backward { current: NodeState::Ready }
        );

        let admin_path = PathName::new("mesh1", NodeKind::NodeAdmin, 1);
        let admin_id = NodeId::mint();
        let admin_inc = IncarnationId::mint();
        let mut admin_node = Node::allocated(admin_path.clone());
        admin_node.kind = NodeKind::NodeAdmin;
        admin_node.mesh = "mesh1".to_string();
        admin_node.node_id = admin_id.clone();
        admin_node.incarnation_id = Some(admin_inc.clone());
        admin_node.endpoint_id = Some(EndpointId("ep-admin-1".to_string()));
        admin_node.transport_addr = Some("127.0.0.1:4101".parse().unwrap());
        admin_node.provider = Some(ProviderKind::Process);
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;

        let rpc_path = PathName::new("mesh1", NodeKind::RpcNode, 1);
        let rpc_id = NodeId::mint();
        let rpc_inc = IncarnationId::mint();
        let mut rpc_node = Node::allocated(rpc_path.clone());
        rpc_node.kind = NodeKind::RpcNode;
        rpc_node.mesh = "mesh1".to_string();
        rpc_node.node_id = rpc_id.clone();
        rpc_node.incarnation_id = Some(rpc_inc.clone());
        rpc_node.endpoint_id = Some(EndpointId("ep-rpc-1".to_string()));
        rpc_node.transport_addr = Some("127.0.0.1:4102".parse().unwrap());
        rpc_node.provider = Some(ProviderKind::Process);
        rpc_node.status = NodeStatus::Pending;

        // node-status.md:15-21 — Eligible events, coalesced wake, and unreachable target scheduling no report.
        let dest1 = Destination::of(&admin_node);
        assert!(eligible(None, &dest1, true, false), "sent when first owed");
        let sent_typed = Sent { to: dest1.clone(), typed: true };
        assert!(!eligible(Some(&sent_typed), &dest1, true, false), "typed reply is terminal while destination is unchanged");
        assert!(!eligible(None, &dest1, false, false), "unreachable target schedules no report");
        assert!(eligible(Some(&sent_typed), &dest1, true, true), "eligible when primary addresses this node with down call");
        let mut moved_admin = admin_node.clone();
        moved_admin.incarnation_id = Some(IncarnationId::mint());
        let dest2 = Destination::of(&moved_admin);
        assert!(eligible(Some(&sent_typed), &dest2, true, false), "eligible when primary's address changes to a deliverable exact instance");

        // Coalesced wake (DeclareWake): multiple pokes coalesce into one wake, never a timer.
        let wake = DeclareWake::new();
        wake.poke();
        wake.poke();
        wake. addressed_by(Some(&admin_id), None);
        tokio::time::timeout(Duration::from_secs(1), wake.woken()).await.expect("coalesced wake fires once");
        assert!(tokio::time::timeout(Duration::from_millis(20), wake.woken()).await.is_err(), "burst of pokes coalesced into one wake");
        assert_eq!(wake.take_addressed(), BTreeSet::from([admin_id.clone()]));

        // Declarer: owed report to mesh-primary, unreachable schedules nothing, down call re-schedules.
        let declarer = Declarer::new();
        let start_op = DeclaredOperation {
            build_id: "build-1".to_string(),
            attempt: 1,
            operation: format!("node.start:{rpc_path}"),
        };
        declarer.owe_own(&rpc_path, &rpc_id, &rpc_inc, NodeState::Ready, Some(start_op.clone()));
        let mut view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fabric1".to_string(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![admin_node.clone(), rpc_node.clone()],
        };
        assert_eq!(declarer.due(&rpc_id, &view, &BTreeSet::new()).len(), 1);
        view.nodes[0].status = NodeStatus::PendingReconnect;
        assert!(declarer.due(&rpc_id, &view, &BTreeSet::new()).is_empty(), "unreachable target schedules no report");
        view.nodes[0].status = NodeStatus::Ready;

        // StatusAuthority setup to test status report, ProbeNodeState, and ApplyNodeState.
        let fabric_id = view.fabric.id.clone();
        let mesh1_id = MeshId::mint();
        let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), mesh1_id)].into_iter().collect();
        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let topology = Arc::new(tokio::sync::RwLock::new(view));
        let stopped_published: Arc<Mutex<Vec<(Declaration, DeclaredOperation)>>> = Arc::default();
        let republished = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let authority = Arc::new(StatusAuthority {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin_path.clone(),
            node_id: admin_id.clone(),
            fabric_id,
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            nodes_storage: nodes_storage.clone(),
            caches: Default::default(),
            mesh_ids: Arc::new(move || mesh_ids.clone()),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(std::sync::OnceLock::new()),
            mesh_deleter: Arc::new(std::sync::OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: Default::default(),
            fabric_recovery: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            configure: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: Some(Arc::new(BTreeMap::new)),
            digest_view: None,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        });
        {
            let stopped_published = stopped_published.clone();
            let _ = authority.stopped.set(Arc::new(move |decl, op| {
                let stopped_published = stopped_published.clone();
                Box::pin(async move {
                    stopped_published.lock().unwrap().push((decl, op));
                })
            }));
            let republished = republished.clone();
            let _ = authority.republish.set(Arc::new(move || {
                let republished = republished.clone();
                Box::pin(async move {
                    republished.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            }));
            let _ = authority.configure.set(Arc::new(|_held| Box::pin(async {})));
        }

        // node-status.md:5-7 — Status never moves through a meta or config put.
        let config_req = StatusRequest::ApplyNodeConfig {
            node_id: admin_id.clone(),
            incarnation: admin_inc.clone(),
            config: BTreeMap::from([("zone".to_string(), "eu-1".to_string())]),
            revision: 1,
        };
        let config_reply = authority.apply(Some(admin_node.clone()), &config_req).await;
        assert_eq!(config_reply, StatusReply::Applied);
        assert_eq!(authority.declared.lock().unwrap().node(&admin_id, &admin_inc), None, "config put never moves node status");

        // node-status.md:26-30 — Node reports Pending and start's Ready, then stop's Stopped naming DeclaredOperation;
        // mesh-primary sets status on the node's row and publishes NodeStopped on Stopped.
        let pending_req = StatusRequest::DeclareNodeState {
            node_id: rpc_id.clone(),
            incarnation: rpc_inc.clone(),
            state: NodeState::Pending,
            operation: None,
        };
        assert_eq!(authority.apply(Some(rpc_node.clone()), &pending_req).await, StatusReply::Applied);
        assert_eq!(authority.declared.lock().unwrap().node(&rpc_id, &rpc_inc), Some(NodeState::Pending));

        let ready_req = StatusRequest::DeclareNodeState {
            node_id: rpc_id.clone(),
            incarnation: rpc_inc.clone(),
            state: NodeState::Ready,
            operation: Some(start_op.clone()),
        };
        assert_eq!(authority.apply(Some(rpc_node.clone()), &ready_req).await, StatusReply::Applied);
        assert_eq!(authority.declared.lock().unwrap().node(&rpc_id, &rpc_inc), Some(NodeState::Ready));

        let stop_op = DeclaredOperation {
            build_id: "build-2".to_string(),
            attempt: 1,
            operation: format!("node.stop:{rpc_path}"),
        };
        let stop_req = StatusRequest::DeclareNodeState {
            node_id: rpc_id.clone(),
            incarnation: rpc_inc.clone(),
            state: NodeState::Stopped,
            operation: Some(stop_op.clone()),
        };
        assert_eq!(authority.apply(Some(rpc_node.clone()), &stop_req).await, StatusReply::Applied);
        assert_eq!(authority.declared.lock().unwrap().node(&rpc_id, &rpc_inc), Some(NodeState::Stopped));
        let published = stopped_published.lock().unwrap().clone();
        assert_eq!(published.len(), 1, "mesh-primary publishes NodeStopped from stop's Stopped report");
        assert_eq!(published[0].0.node_id, rpc_id);
        assert_eq!(published[0].1, stop_op);
        let stored_row = nodes_storage.node(&rpc_id).await.unwrap().expect("node row written");
        assert_eq!(stored_row.births[0].operations.len(), 2, "start and stop operation effects recorded on row");

        // node-status.md:22-24 — ProbeNodeState answers Current, reasserts held status, and manufactures no transition.
        crate::node_self::process_stop().start();
        let probe_req = StatusRequest::ProbeNodeState {
            node_id: admin_id.clone(),
            incarnation: admin_inc.clone(),
        };
        let probe_reply = authority.apply(Some(admin_node.clone()), &probe_req).await;
        assert_eq!(
            probe_reply,
            StatusReply::Current {
                node_id: admin_id.clone(),
                incarnation: admin_inc.clone(),
                state: NodeState::Ready,
            }
        );
        assert_eq!(republished.load(std::sync::atomic::Ordering::SeqCst), 1, "ProbeNodeState reasserts held status");
        assert_eq!(authority.declared.lock().unwrap().node(&admin_id, &admin_inc), None, "ProbeNodeState manufactures no transition");
        let stale_probe = StatusRequest::ProbeNodeState {
            node_id: admin_id.clone(),
            incarnation: IncarnationId::mint(),
        };
        assert_eq!(
            authority.apply(Some(admin_node.clone()), &stale_probe).await,
            StatusReply::RejectedStaleIncarnation { held: admin_inc.clone() }
        );
        let unknown_decl = StatusRequest::DeclareNodeState {
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            state: NodeState::Ready,
            operation: None,
        };
        assert_eq!(
            authority.apply(Some(admin_node.clone()), &unknown_decl).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }
        );

        // node-status.md:38-42, 50-51 — ApplyNodeState(ConnectionsDeleting) is refused with a named typed refusal
        // because connections-deleting is entered only by node.connections.delete { scope: All }; node.id,
        // incarnation, endpoint key, port, and immutable path are kept unchanged; and the workflow step
        // classifies the refusal as Refused (recorded as Failed { step, reason }).
        let apply_cd = StatusRequest::ApplyNodeState {
            node_id: admin_id.clone(),
            incarnation: admin_inc.clone(),
            state: NodeState::ConnectionsDeleting,
        };
        let apply_reply = authority.apply(Some(admin_node.clone()), &apply_cd).await;
        assert_eq!(
            apply_reply,
            StatusReply::RejectedInvalidNodeTransition { current: NodeState::Ready }
        );
        let after_admin = topology.read().await.own(&admin_id).unwrap().clone();
        assert_eq!(after_admin.node_id, admin_node.node_id);
        assert_eq!(after_admin.incarnation_id, admin_node.incarnation_id);
        assert_eq!(after_admin.endpoint_id, admin_node.endpoint_id);
        assert_eq!(after_admin.transport_addr, admin_node.transport_addr);
        assert_eq!(after_admin.name, admin_node.name);
        let encoded_reply = Status::encode_reply(&apply_reply).expect("encode Status reply");
        let committed = PreCommit::begin(Status::OP).commit(RequestFinished::after_clean_finish(1, 1, true).unwrap());
        let outcome = committed.reply::<Status>(&encoded_reply);
        let admission = command_admission(&outcome);
        assert!(
            matches!(admission, CommandAdmission::Refused { ref reply, .. } if reply.contains("rejected-invalid-node-transition")),
            "typed refusal is classified as CommandAdmission::Refused for the workflow step's Failed receipt: {admission:?}"
        );
    }

    /// CONTRACT:
    /// - `docs/architecture/lifecycles/node-status.md:40` — "| Mapping | ApplyNodeState on the [Status schema](../node-rpc-envelope.md#status-op-0x1b-rdm); connections-deleting is entered only by [node.connections.delete](node-connections-delete.md) with `{ scope: All }` |"
    /// - `docs/architecture/lifecycles/node-status.md:47` — "On the applied status the node adjusts its lifecycle admission. No provider stop, endpoint bind, new JoinNode, identity release or replacement is implied."
    #[tokio::test]
    #[ignore = "page/code difference: node-status.md §Status apply"]
    async fn node_status_apply_node_state_applies_non_connections_deleting_transition_and_adjusts_admission() {
        use crate::model::{Fabric, ProviderKind, ScopeStatus};
        use crate::storage::{MemoryMeshStorage, MemoryNodesStorage};
        use crate::topology::Topology;
        use rdm_core::status::{StatusReply, StatusRequest};
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};

        let admin_path = PathName::new("mesh1", NodeKind::NodeAdmin, 1);
        let admin_id = NodeId::mint();
        let admin_inc = IncarnationId::mint();
        let mut admin_node = Node::allocated(admin_path.clone());
        admin_node.kind = NodeKind::NodeAdmin;
        admin_node.mesh = "mesh1".to_string();
        admin_node.node_id = admin_id.clone();
        admin_node.incarnation_id = Some(admin_inc.clone());
        admin_node.endpoint_id = Some(EndpointId("ep-admin-1".to_string()));
        admin_node.transport_addr = Some("127.0.0.1:4101".parse().unwrap());
        admin_node.provider = Some(ProviderKind::Process);
        admin_node.status = NodeStatus::Pending;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;

        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fabric1".to_string(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![admin_node.clone()],
        };
        let fabric_id = view.fabric.id.clone();
        let authority = Arc::new(StatusAuthority {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin_path,
            node_id: admin_id.clone(),
            fabric_id,
            topology: Arc::new(tokio::sync::RwLock::new(view)),
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            nodes_storage: Arc::new(MemoryNodesStorage::default()),
            caches: Default::default(),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(std::sync::OnceLock::new()),
            mesh_deleter: Arc::new(std::sync::OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: Default::default(),
            fabric_recovery: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            configure: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: Some(Arc::new(BTreeMap::new)),
            digest_view: None,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        });

        let apply_ready = StatusRequest::ApplyNodeState {
            node_id: admin_id,
            incarnation: admin_inc,
            state: NodeState::Ready,
        };
        let reply = authority.apply(Some(admin_node), &apply_ready).await;
        assert_eq!(
            reply,
            StatusReply::Applied,
            "node-status.md:40, 47 states Status apply maps to ApplyNodeState on 0x1B (other than connections-deleting) and on the applied status the node adjusts its lifecycle admission"
        );
    }
}

