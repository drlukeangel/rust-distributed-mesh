//! Node tickle primitive (`docs/architecture/lifecycles/node-tickle.md`).
//!
//! The one reach ladder over one exact birth: one probe direct, and if that fails the same probe
//! through up to two live peers of the node's mesh, sent at once; `reached` with what the node said,
//! or `unreached`, never dead. Run by node recovery, mesh investigation, the primary watch and the
//! successor ladder of node.primary.update, each over its own [`TickleCalls`].

/// Reach ladder entry point, probe outcomes, and carrier selection (`docs/architecture/lifecycles/node-tickle.md`).
pub use crate::offline::{birth_answered, eligible_carriers, tickle, tickle_ladder, tickle_ladder_at_default_budget, PeerProbe, ProbeEnd, Tickle, TickleCalls, TickleClient, Unreached, VIA_PEER_TICKLE_FANOUT};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Fabric, FabricId, IncarnationId, Node, NodeId, NodeStatus, ProviderKind, ScopeStatus,
    };
    use crate::topology::Topology;
    use rdm_core::status::{NodeState, RetryOn, StatusReply, StatusRequest};
    use std::time::Duration;

    fn make_node(name: &str, status: NodeStatus) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = status;
        n
    }

    // @feature: node-tickle
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:5
    /// "A primitive. `node.tickle` asks one node, by node.id and incarnation, whether anyone can reach it:
    ///  one probe direct, and if that fails the same probe through up to two live peers of the node's mesh,
    ///  sent at once (R-SW1). It answers `reached`, with what the node said, or `unreached`. It never
    ///  answers dead: a failure, a deadline included, proves nothing, and only node.runtime.inspect proves
    ///  a runtime gone."
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:26
    /// "Target: One node, by `(node.id, incarnation)`; every probe names the same pair, and only that
    ///  pair's answer counts"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:27
    /// "Probe: `ProbeNodeState { node_id, incarnation }` on 0x1B, a bounded call with a deadline;
    ///  the node answers `Current { state }`, `CurrentStopped` or `Blocked { reason, retry_on }`"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:28
    /// "Ladder: Direct first. On failure, the same probe carried by Forward through up to two
    ///  eligible live peers of the node's mesh, both at once; one failed peer probe does not cancel
    ///  the other"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:29
    /// "Ends: Any answer from the node, under the probed incarnation, ends the ladder and cancels the
    ///  probes still running"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:71
    /// "`{ \"Current\": { \"node_id\": \"k8pag1qx95gw\", \"incarnation\": \"0000000000000008\", \"state\": \"Ready\" } }`"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:74
    /// "A stopped node answers `CurrentStopped { node_id, incarnation }`; a node held at its hook answers
    ///  `Blocked { node_id, incarnation, reason, retry_on }`. All three are `reached`."
    #[tokio::test]
    async fn birth_answers_include_current_stopped_and_blocked_and_one_failed_peer_does_not_cancel_the_other()
    {
        let caller = make_node("mesh1.admin.1", NodeStatus::Ready);
        let target = make_node("mesh1.rpc.1", NodeStatus::PendingReconnect);
        let peer1 = make_node("mesh1.rpc.2", NodeStatus::Ready);
        let peer2 = make_node("mesh1.rpc.3", NodeStatus::Ready);
        let peer3 = make_node("mesh1.rpc.4", NodeStatus::Ready);
        let silent_peer = make_node("mesh1.rpc.5", NodeStatus::PendingReconnect);
        let other_mesh = make_node("mesh2.rpc.1", NodeStatus::Ready);

        let target_id = target.node_id.clone();
        let target_inc = target.incarnation_id.clone().unwrap();
        let wrong_inc = IncarnationId::mint();

        // 1. `birth_answered` accepts Current, CurrentStopped, and Blocked only for the exact (node_id, incarnation).
        for valid in [
            StatusReply::Current {
                node_id: target_id.clone(),
                incarnation: target_inc.clone(),
                state: NodeState::Ready,
            },
            StatusReply::CurrentStopped {
                node_id: target_id.clone(),
                incarnation: target_inc.clone(),
            },
            StatusReply::Blocked {
                node_id: target_id.clone(),
                incarnation: target_inc.clone(),
                reason: "waiting for schema".into(),
                retry_on: RetryOn::PrimaryReady,
            },
        ] {
            assert!(
                birth_answered(Some(&valid), &target_id, &target_inc),
                "{valid:?} is a valid birth answer"
            );
            assert!(
                !birth_answered(Some(&valid), &target_id, &wrong_inc),
                "mismatched incarnation must not count as answered"
            );
        }
        assert!(!birth_answered(
            Some(&StatusReply::NotReady {
                reason: "starting".into()
            }),
            &target_id,
            &target_inc
        ));
        assert!(!birth_answered(None, &target_id, &target_inc));

        // 2. `eligible_carriers` picks Ready members of target's mesh other than target and caller, in path order.
        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fab1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![
                caller.clone(),
                target.clone(),
                peer1.clone(),
                peer2.clone(),
                peer3,
                silent_peer,
                other_mesh,
            ],
        };
        let eligible: Vec<String> = eligible_carriers(&view, &target, Some(&caller.node_id))
            .map(|n| n.name.to_string())
            .collect();
        assert_eq!(eligible, vec!["mesh1.rpc.2", "mesh1.rpc.3", "mesh1.rpc.4"]);

        // 3. Ladder truncates to VIA_PEER_TICKLE_FANOUT (2) and a failure on peer1 does not cancel peer2.
        struct PeerFanoutCalls {
            caller_id: NodeId,
            target_id: NodeId,
            target_inc: IncarnationId,
        }

        #[async_trait::async_trait]
        impl TickleCalls for PeerFanoutCalls {
            fn caller(&self) -> String {
                "mesh1.admin.1".into()
            }
            fn deadline(&self) -> Duration {
                Duration::from_secs(2)
            }
            async fn direct(&self, _node: &Node, _probe: &StatusRequest) -> ProbeEnd {
                ProbeEnd::Other {
                    end: "NotSent",
                    detail: "not sent: Connection".into(),
                }
            }
            async fn via_peer(
                &self,
                carrier: &Node,
                _node: &Node,
                _probe: &StatusRequest,
            ) -> ProbeEnd {
                if carrier.name.to_string() == "mesh1.rpc.2" {
                    ProbeEnd::ConnectionLost("edge dropped".into())
                } else {
                    ProbeEnd::Reply(StatusReply::Blocked {
                        node_id: self.target_id.clone(),
                        incarnation: self.target_inc.clone(),
                        reason: "waiting for peer".into(),
                        retry_on: RetryOn::PrimaryReady,
                    })
                }
            }
            fn carriers(&self, view: &Topology, node: &Node) -> Vec<Node> {
                eligible_carriers(view, node, Some(&self.caller_id))
                    .cloned()
                    .collect()
            }
            async fn leads(&self, _view: &Topology, _node: &Node) -> Result<Vec<Node>, String> {
                Ok(Vec::new())
            }
        }

        let res = tickle(
            &target,
            &view,
            &PeerFanoutCalls {
                caller_id: caller.node_id.clone(),
                target_id: target_id.clone(),
                target_inc: target_inc.clone(),
            },
        )
        .await
        .expect("tickle");
        match res {
            Tickle::Reached { answer, carrier } => {
                assert_eq!(carrier, Some("mesh1.rpc.3".into()));
                assert_eq!(
                    answer,
                    StatusReply::Blocked {
                        node_id: target_id,
                        incarnation: target_inc,
                        reason: "waiting for peer".into(),
                        retry_on: RetryOn::PrimaryReady,
                    }
                );
            }
            other => panic!("expected Reached via mesh1.rpc.3, got {other:?}"),
        }
    }
}
