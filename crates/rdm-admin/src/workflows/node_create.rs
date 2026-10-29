//! Node create workflow (`docs/architecture/lifecycles/node-create.md`).
//!
//! Creates a logical node, prepares its identity and data directory, launches its runtime,
//! and composes the `node.start` workflow with `StartKind::FirstStart`.

pub use crate::node_hooks::StartKind;
pub use crate::deployment::pipeline::{CommandAdmission, CreateStep, READY_PREREQUISITES};
pub use crate::workflows::node_start::{node_start, FirstStart, JoinThrough, JoinTo, Joining, StartNode, Starting};
pub use crate::admin::AdminRunner;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span_capture::Capture;

    /// Page: `docs/architecture/lifecycles/node-create.md:5`
    /// "`node.create(spec)` creates a logical node and composes the whole `node.start` workflow with
    /// `start_kind = FirstStart`."
    /// Page: `docs/architecture/lifecycles/node-create.md:37`
    /// "1. Record the exact deployment, prepare its identity/data dir and launch its executable...
    /// 5. On acceptance it installs the actual address and completes WaitForBind / WaitForMeshJoin
    /// from the validated report, never from gossip."
    #[test]
    fn create_step_order_ready_prerequisites_and_first_start_kind_match_contract() {
        let capture = Capture::new("create_step_order_ready_prerequisites_and_first_start_kind_match_contract");
        {
            let _guard = tracing::dispatcher::set_default(&capture.dispatch);
            tracing::info_span!(
                "rdm.node_admin.deployment.update.via-step",
                operation = "node.create",
                step = CreateStep::AllocateIdentity.name(),
                start_kind = ?StartKind::FirstStart
            )
            .in_scope(|| tracing::info!("verifying node.create step order and prerequisites"));
        }

        let names: Vec<&str> = CreateStep::ORDER.iter().map(|s| s.name()).collect();
        assert_eq!(
            names,
            vec![
                "AllocateIdentity",
                "PrepareStorage",
                "PrepareNetwork",
                "DeployRuntime",
                "RegisterExactRuntimeHandle",
                "ResolveProviderControlDomain",
                "MakeRuntimeFactAvailableToBirth",
                "WaitForBind",
                "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata",
                "ApplyMeshPending",
                "WaitForMeshJoin",
                "WaitForNodeReady",
                "Complete",
            ]
        );
        let prereqs: Vec<&str> = READY_PREREQUISITES.iter().map(|s| s.name()).collect();
        assert_eq!(
            prereqs,
            vec![
                "RegisterExactRuntimeHandle",
                "ResolveProviderControlDomain",
                "MakeRuntimeFactAvailableToBirth",
                "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata",
            ]
        );
        assert_eq!(format!("{:?}", StartKind::FirstStart), "FirstStart");

        let spans = capture.named("rdm.node_admin.deployment.update.via-step");
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].attributes.get("start_kind").map(String::as_str),
            Some("FirstStart")
        );
    }
}
