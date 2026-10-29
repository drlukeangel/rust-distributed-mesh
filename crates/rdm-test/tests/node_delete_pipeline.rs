//! i143.e2.s5 functional: the delete pipeline runs its steps in order —
//! NodeDeleting, StopNode, AwaitNodeLeft,
//! TerminateRuntime, NodeDeleted, RemoveTopologyMembership (exactly the deleted birth), Complete —
//! each with a receipt and a step span, and a delete then create on the same node
//! succeeds. A node.delete releases identity, never storage (node-delete.md
//! "Storage": "Retain data dir and endpoint key; a later spec can inherit
//! them"): the birth's data dir and its endpoint key stay whatever the Build's
//! storage meta says.
//!
//! The span-asserting cell runs in its own process (`own_process`), and its capture enables every
//! callsite before installing its dispatcher (`rdm_transport::span_capture` calls `enable_callsites`).

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published};
use rdm_core::{FabricId, MemberStatus};
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, DeleteRequest, DeleteStep, Timeouts};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, DeploymentStatus};
use rdm_admin::model::NodeStatus;
use std::net::{IpAddr, SocketAddr, UdpSocket};

#[test]
fn node_delete_runs_every_step_in_order_and_the_ports_it_held_are_released() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "node_delete_runs_every_step_in_order_and_the_ports_it_held_are_released") {
        return;
    }
    let cap = crate::common::capture("node_delete_runs_every_step_in_order_and_the_ports_it_held_are_released");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {

        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
        let builds = MemoryBuildStateAdapter::new();
        let sink = Published::default();
        let provider = ProcessDeploymentProvider::new();
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &admin.joins,
            observer: &admin.observer,
            sink: &sink,
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };
        let node: rdm_admin::model::PathName = "mesh1.rpc.1".parse().unwrap();
        let create = |build_id| CreateRequest { build_id, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };

        let first = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("create: {e}"));
        let port: SocketAddr = first.node.transport_addr.expect("a created node carries its transport address");

        let delete_build = publish_build(&builds, add_node()).await; // the Build names the path: an rpc node's meta is Ephemeral
        pipeline
            .delete_node(&DeleteRequest { build_id: delete_build.clone(), attempt: 1, node: first.node.clone(), handle: first.handle.clone(), unreached: false })
            .await
            .unwrap_or_else(|e| panic!("delete: {e}"));

        // Every delete step, in order, Complete once.
        let view = builds.read_build(&delete_build).await.unwrap();
        let got: Vec<(&str, &StepOutcome, &str)> = view.steps.iter().map(|r| (r.step.as_str(), &r.outcome, r.operation.as_str())).collect();
        let want: Vec<(&str, &StepOutcome, &str)> =
            DeleteStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "node.delete:mesh1.rpc.1")).collect();
        assert_eq!(got, want);
        // StopNode is the directed command: its receipt carries what the call established;
        // AwaitNodeLeft carries the completion, the stop's own reply.
        use rdm_admin::deployment::pipeline::{CommandAdmission, Completion};
        let output = |step: DeleteStep| view.steps.iter().find(|r| r.step == step.name()).and_then(|r| r.output.clone()).unwrap_or_else(|| panic!("{} receipted with an output", step.name()));
        // The stop is answered `Stopped` on its own call: the receipt names what its
        // node.connection.delete { All } established.
        let stopped: CommandAdmission = serde_json::from_value(output(DeleteStep::StopNode)).expect("a CommandAdmission");
        assert!(matches!(stopped, CommandAdmission::Stopped { receipt: rdm_core::status::ConnectionsReceipt::Established { .. } }), "StopNode: {stopped:?}");
        for step in [DeleteStep::AwaitNodeLeft] {
            let completion: Completion = serde_json::from_value(output(step)).expect("a Completion");
            assert_eq!(completion, Completion::Received {}, "{}", step.name());
        }

        // What each step did, from outside.
        let statuses: Vec<NodeStatus> = sink.nodes.lock().unwrap().iter().map(|n| n.status).collect();
        assert_eq!(statuses, vec![NodeStatus::Pending, NodeStatus::Ready], "the pipeline does not publish Leaving from a stop answer; the command owner records the node report");
        let (digest, _) = admin.observer.topology_view.digest_view.get(first.node.node_id.as_str()).expect("the node's digest");
        assert_eq!(digest.status, MemberStatus::Leaving, "the admin side holds the status it set from the node's last report, its stop's Leaving (node-status.md \"Status report\"); the node's heartbeats carry none (gossip.md §3.1 `Digest`)");
        assert!(matches!(provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }));
        assert!(UdpSocket::bind(port).is_ok(), "the runtime released its port");
        // node-delete.md step 3: "Retain the data dir, node-identity.json and endpoint key." An
        // Ephemeral path's delete keeps them too: no step of the delete releases storage.
        let first_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's data dir"));
        assert!(first_dir.is_dir(), "the delete keeps the birth's data dir: {first_dir:?}");
        assert!(first_dir.join("node-key").is_file(), "the delete keeps the endpoint key in the data dir: {first_dir:?}");
        assert!(view.steps.iter().all(|r| r.step != "ReleaseStorage"), "no step of a delete releases storage: {:?}", view.steps.iter().map(|r| r.step.as_str()).collect::<Vec<_>>());
        // RemoveTopologyMembership removes exactly the deleted birth (node-delete.md step 5: the old
        // `(node.id, incarnation)` is what is targeted), never whatever the path holds.
        assert!(sink.removed.lock().unwrap().is_empty(), "no path-keyed removal");
        assert_eq!(*sink.removed_births.lock().unwrap(), vec![(node.clone(), first.node.node_id.clone(), first.node.incarnation_id.clone().unwrap())]);

        // Each delete step ran as a child of the delete pipeline span.
        let all = spans.0.lock().unwrap().clone();
        for step in DeleteStep::ORDER {
            let (_, parent, f) = all
                .values()
                .find(|(n, _, f)| {
                    n == "rdm.node_admin.deployment.update.via-step"
                        && f.get("step").map(String::as_str) == Some(step.name())
                        && f.get("build_id") == Some(&delete_build.log().to_string())
                })
                .unwrap_or_else(|| panic!("no span for delete step {}; via-step spans held: {:?}", step.name(), all.values().filter(|(n, _, _)| n == "rdm.node_admin.deployment.update.via-step").map(|(_, _, f)| (f.get("step").cloned(), f.get("build_id").cloned(), f.get("outcome").cloned())).collect::<Vec<_>>()));
            assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-pipeline"));
            assert_eq!(f.get("outcome").map(String::as_str), Some("complete"), "{}", step.name());
        }
        assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline"
            && f.get("pipeline").map(String::as_str) == Some("delete")));
        // The command owner records the stop's ordered stream under its operation. The rig
        // calls that owner directly, so its evidence is the received frames, not the removed
        // unary helper's command-sent span.
        let operation = "node.stop:mesh1.rpc.1";
        let mut reported: Vec<_> = all.values().filter(|(n, _, f)| n == "rdm.node_admin.node.update.via-command-frame" && f.get("operation").map(String::as_str) == Some(operation))
            .map(|(_, _, f)| (f.get("seq").and_then(|s| s.parse::<u32>().ok()).expect("a transport position"), f.get("frame").cloned().expect("a named frame"))).collect();
        reported.sort_by_key(|(seq, _)| *seq);
        assert_eq!(reported, vec![(0, "command-accepted".into()), (1, "connections-deleting".into()), (2, "stop-connections-deleted".into()), (3, "leaving".into()), (4, "stop-mesh-left".into()), (5, "node-stopped".into())], "the directed stop reports its admission, each primitive's acknowledgement and completion, and its completion once (Q205)");
        assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.node.update.via-completion-awaited" && f.get("operation").map(String::as_str) == Some(operation) && f.get("received").map(String::as_str) == Some("true") && f.get("completion").map(String::as_str) == Some("node-left")), "the pipeline receives the stop completion");

        // Create again on the same node.
        let again = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("re-create: {e}"));
        assert_ne!(again.node.node_id, first.node.node_id, "a new node, not the deleted one");

        // The Build names this path's storage Persistent { disposition: Release }: the delete still
        // terminates the runtime and keeps the data dir and its key (node-delete.md: "It releases
        // node.id and name, not storage"; deployment-provider.md: "Delete releases no storage").
        let mut release = common::add_node();
        let released = rdm_core::meta::NodeSpec {
            storage_persistence: rdm_core::meta::StorageMeta::Persistent { disposition: rdm_core::meta::PersistentDisposition::Release },
            ..rdm_core::meta::NodeSpec::default_for(again.node.name.clone())
        };
        release.meshes.get_mut("mesh1").unwrap().set_spec(released).unwrap();
        let again_build = publish_build(&builds, release).await;
        pipeline
            .delete_node(&DeleteRequest { build_id: again_build.clone(), attempt: 1, node: again.node.clone(), handle: again.handle.clone(), unreached: false })
            .await
            .unwrap();
        assert!(matches!(provider.inspect(&again.handle).await, DeploymentStatus::Exited { .. }), "the delete ended the runtime");
        let again_dir = std::path::PathBuf::from(again.node.data_dir.clone().expect("the birth's data dir"));
        assert!(again_dir.is_dir() && again_dir.join("node-key").is_file(), "Persistent/Release: the delete keeps the data dir and its endpoint key: {again_dir:?}");
        let kept = builds.read_build(&again_build).await.unwrap().steps;
        assert!(kept.iter().all(|r| r.step != "ReleaseStorage"), "no step of a delete releases storage: {:?}", kept.iter().map(|r| r.step.as_str()).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
}
