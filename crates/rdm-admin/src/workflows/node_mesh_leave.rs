//! Node mesh leave primitive (`docs/architecture/lifecycles/node-mesh-leave.md`).
//!
//! Takes a node off its mesh (and backbone for node-admins) after its connections
//! are deleted.

pub use crate::node_self::CutReport;
pub use crate::stop_gate::StopGate;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::pipeline::LeaveStep;
    use crate::span_capture::Capture;

    // @feature: node-mesh-leave
    /// Page: docs/architecture/lifecycles/node-mesh-leave.md:5
    /// "`node.mesh.leave` takes a node off its mesh and, for a node-admin, off the backbone
    /// too. It is the step node.stop and node.delete end their live part with, after the node's
    /// connections are deleted and never before (R-CR5): the data plane first, then the control plane.
    /// Progress is `leaving`, completion `node.mesh.left`."
    #[tokio::test]
    async fn leave_step_names_cut_report_and_stop_gate_quiescence_match_contract() {
        let capture = Capture::new("node_mesh_leave_unit_contract");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        assert_eq!(LeaveStep::LeaveMesh.name(), "LeaveMesh");
        assert_eq!(LeaveStep::Complete.name(), "Complete");

        let report = CutReport {
            neighbours: 2,
            pooled: 3,
            accepted: 1,
            turns_in_progress: 0,
        };
        assert_eq!(report.neighbours, 2);
        assert_eq!(report.pooled, 3);
        assert_eq!(report.accepted, 1);
        assert_eq!(CutReport::default(), CutReport {
            neighbours: 0,
            pooled: 0,
            accepted: 0,
            turns_in_progress: 0,
        });

        let gate = StopGate::new();
        assert!(!gate.is_stopped());
        let turn = gate.turn("gossip_tick").await;
        assert_eq!(gate.state().borrow().in_progress(), 1);
        drop(turn);
        let at_stop = gate.quiesce().await;
        assert!(gate.is_stopped());
        assert_eq!(at_stop.in_progress(), 0);
        gate.resume();
        assert!(!gate.is_stopped());

        tracing::info_span!(
            "rdm.node.mesh.update.via-leaving",
            node = "mesh1.rpc.1",
            mesh = "mesh1",
            operation = "node.mesh.leave:mesh1.rpc.1"
        )
        .in_scope(|| {});

        let spans = capture.named("rdm.node.mesh.update.via-leaving");
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].attributes.get("operation").map(String::as_str),
            Some("node.mesh.leave:mesh1.rpc.1")
        );
    }
}

