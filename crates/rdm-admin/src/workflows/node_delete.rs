//! Node delete workflow (`docs/architecture/lifecycles/node-delete.md`).
//!
//! Composes `node.stop`, verifies exact provider runtime exited verdict, releases node ID and
//! name claims, and retains storage.

pub use crate::deployment::pipeline::{DeleteStep, StorageDisposition};
pub use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, TerminationMode};
pub use crate::admin::AdminRunner;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span_capture::Capture;
    use std::time::Duration;

    // @feature: node-delete
    /// Page: docs/architecture/lifecycles/node-delete.md:5
    /// "`node.delete` composes the whole node.stop, then terminates the exact runtime
    /// through its provider, reads it `Exited`, and releases identity. It releases node.id and name, **not storage**."
    #[test]
    fn delete_step_order_storage_disposition_and_termination_modes_match_contract() {
        let capture = Capture::new("node_delete_step_order_and_storage_contract");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let step_names: Vec<&str> = DeleteStep::ORDER.iter().map(|s| s.name()).collect();
        assert_eq!(
            step_names,
            vec![
                "NodeDeleting",
                "StopNode",
                "AwaitNodeLeft",
                "TerminateRuntime",
                "NodeDeleted",
                "RemoveTopologyMembership",
                "Complete",
            ],
            "DeleteStep::ORDER must match the canonical node.delete step sequence"
        );
        assert_eq!(DeleteStep::DeleteConnections.name(), "DeleteConnections");

        let preserved = serde_json::to_value(StorageDisposition::Preserved {}).unwrap();
        assert_eq!(preserved, serde_json::json!({ "arm": "preserved" }));
        let preserved_no_meta = serde_json::to_value(StorageDisposition::PreservedNoMeta {}).unwrap();
        assert_eq!(preserved_no_meta, serde_json::json!({ "arm": "preserved-no-meta" }));
        let released = serde_json::to_value(StorageDisposition::Released {
            locator: "dir:/var/lib/rdm/mesh1.rpc.1".into(),
        })
        .unwrap();
        assert_eq!(
            released,
            serde_json::json!({ "arm": "released", "locator": "dir:/var/lib/rdm/mesh1.rpc.1" })
        );

        let graceful = TerminationMode::Graceful {
            grace: Duration::from_secs(8),
        };
        assert_ne!(graceful, TerminationMode::Immediate);

        tracing::info_span!(
            "rdm.node_admin.deployment.update.via-step",
            step = DeleteStep::TerminateRuntime.name(),
            outcome = "complete"
        )
        .in_scope(|| {});

        let spans = capture.named("rdm.node_admin.deployment.update.via-step");
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].attributes.get("step").map(String::as_str),
            Some("TerminateRuntime")
        );
    }
}

