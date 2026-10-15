//! i143.e2.s5 functional: the retire pipeline runs its steps in order —
//! NodeDeleting, StopNode, AwaitNodeLeft,
//! TerminateRuntime, NodeDeleted, RemoveTopologyMembership (exactly the deleted birth), Complete —
//! each with a receipt and a step span, and a retire then create on the same node
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
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, RetireRequest, RetireStep, Timeouts};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, DeploymentStatus};
use rdm_admin::model::NodeStatus;
use std::net::{IpAddr, SocketAddr, UdpSocket};

#[test]
fn retire_runs_every_step_in_order_and_the_ports_it_held_are_released() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "retire_runs_every_step_in_order_and_the_ports_it_held_are_released") {
        return;
    }
    let cap = crate::common::capture("retire_runs_every_step_in_order_and_the_ports_it_held_are_released");
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
        let create = |build_id| CreateRequest { build_id, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };

        let first = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("create: {e}"));
        let port: SocketAddr = first.node.transport_addr.expect("a created node carries its transport address");

        let retire_build = publish_build(&builds, add_node()).await; // the Build names the path: an rpc node's meta is Ephemeral
        pipeline
            .retire(&RetireRequest { build_id: retire_build.clone(), attempt: 1, node: first.node.clone(), handle: first.handle.clone(), exit: None })
            .await
            .unwrap_or_else(|e| panic!("retire: {e}"));

        // Every retire step, in order, Complete once.
        let view = builds.read_build(&retire_build).await.unwrap();
        let got: Vec<(&str, &StepOutcome, &str)> = view.steps.iter().map(|r| (r.step.as_str(), &r.outcome, r.operation.as_str())).collect();
        let want: Vec<(&str, &StepOutcome, &str)> =
            RetireStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "node.delete:mesh1.rpc.1")).collect();
        assert_eq!(got, want);
        // StopNode is the directed command: its receipt carries what the call established;
        // AwaitNodeLeft carries the completion, the stop's own reply.
        use rdm_admin::deployment::pipeline::{CommandAdmission, Completion};
        let output = |step: RetireStep| view.steps.iter().find(|r| r.step == step.name()).and_then(|r| r.output.clone()).unwrap_or_else(|| panic!("{} receipted with an output", step.name()));
        // The stop is answered `Stopped` on its own call: the receipt names what its
        // node.connections.delete { All } established.
        let stopped: CommandAdmission = serde_json::from_value(output(RetireStep::StopNode)).expect("a CommandAdmission");
        assert!(matches!(stopped, CommandAdmission::Stopped { receipt: rdm_core::status::ConnectionsReceipt::Established { .. } }), "StopNode: {stopped:?}");
        for step in [RetireStep::AwaitNodeLeft] {
            let completion: Completion = serde_json::from_value(output(step)).expect("a Completion");
            assert_eq!(completion, Completion::Received {}, "{}", step.name());
        }

        // What each step did, from outside.
        let statuses: Vec<NodeStatus> = sink.nodes.lock().unwrap().iter().map(|n| n.status).collect();
        assert_eq!(statuses, vec![NodeStatus::Pending, NodeStatus::ReadyForTraffic, NodeStatus::Leaving]);
        let (digest, _) = admin.observer.membership.book.get(first.node.node_id.as_str()).expect("the node's digest");
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
        // `(node.id, incarnation)` is what is fenced), never whatever the path holds.
        assert!(sink.removed.lock().unwrap().is_empty(), "no path-keyed removal");
        assert_eq!(*sink.removed_births.lock().unwrap(), vec![(node.clone(), first.node.node_id.clone(), first.node.incarnation_id.clone().unwrap())]);

        // Each retire step ran as a child of the retire pipeline span.
        let all = spans.0.lock().unwrap().clone();
        for step in RetireStep::ORDER {
            let (_, parent, f) = all
                .values()
                .find(|(n, _, f)| {
                    n == "rdm.node_admin.deployment.update.via-step"
                        && f.get("step").map(String::as_str) == Some(step.name())
                        && f.get("build_id") == Some(&retire_build.log().to_string())
                })
                .unwrap_or_else(|| panic!("no span for retire step {}; via-step spans held: {:?}", step.name(), all.values().filter(|(n, _, _)| n == "rdm.node_admin.deployment.update.via-step").map(|(_, _, f)| (f.get("step").cloned(), f.get("build_id").cloned(), f.get("outcome").cloned())).collect::<Vec<_>>()));
            assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-pipeline"));
            assert_eq!(f.get("outcome").map(String::as_str), Some("complete"), "{}", step.name());
        }
        assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline"
            && f.get("pipeline").map(String::as_str) == Some("retire")));
        // Each command was sent under its own operation, and the birth's completion call for that
        // operation reached the commanding side (the server handler's own spans run on its tasks).
        for (command, completion, operation, admission) in [("stop-node", "node-left", "node.stop:mesh1.rpc.1", "stopped")] {
            assert!(
                all.values().any(|(n, _, f)| n == "rdm.node_admin.node.update.via-command-sent" && f.get("operation").map(String::as_str) == Some(operation) && f.get("admission").map(String::as_str) == Some(admission) && f.get("command").map(String::as_str) == Some(command)),
                "no {admission} {command} span"
            );
            assert!(
                all.values().any(|(n, _, f)| n == "rdm.node_admin.node.update.via-completion-awaited" && f.get("operation").map(String::as_str) == Some(operation) && f.get("received").map(String::as_str) == Some("true") && f.get("completion").map(String::as_str) == Some(completion)),
                "no received {completion} span"
            );
        }

        // Create again on the same node.
        let again = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("re-create: {e}"));
        assert_ne!(again.node.node_id, first.node.node_id, "a new node, not the retired one");

        // The Build names this path's storage Persistent { on_retire: Release }: the delete still
        // terminates the runtime and keeps the data dir and its key (node-delete.md: "It releases
        // node.id and name, not storage"; deployment-provider.md: "Delete releases no storage").
        let mut release = common::add_node();
        let released = rdm_core::meta::PathConfig {
            storage: rdm_core::meta::StorageMeta::Persistent { on_retire: rdm_core::meta::PersistentRetireDisposition::Release },
            placement: Default::default(),
        };
        release.meshes.get_mut("mesh1").unwrap().set_path_config(&again.node.name, released).unwrap();
        let again_build = publish_build(&builds, release).await;
        pipeline
            .retire(&RetireRequest { build_id: again_build.clone(), attempt: 1, node: again.node.clone(), handle: again.handle.clone(), exit: None })
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
