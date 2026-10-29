//! Node runtime inspect primitive (`docs/architecture/lifecycles/node-runtime-inspect.md`).
//!
//! Queries the deployment provider for the exact runtime handle to verify whether the
//! process/container is running or proven exited.

/// Exact runtime inspection and deployment provider types (`docs/architecture/lifecycles/node-runtime-inspect.md`).
pub use crate::deployment::provider::{inspect_runtime, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, Inspected};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::provider::{
        adopt, terminate_unless_os_stopped, DeployError, ResolvedNodeLaunch, TerminationMode,
    };
    use crate::model::ProviderKind;
    use rdm_core::{RuntimeFact, RuntimeLocator, RuntimeProvider};
    use std::sync::Mutex;

    struct StubProvider {
        domain: String,
        status: DeploymentStatus,
        terminated: Mutex<Vec<DeploymentHandle>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for StubProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            self.domain.clone()
        }
        async fn spawn(&self, _spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            unreachable!()
        }
        async fn terminate(
            &self,
            handle: &DeploymentHandle,
            _mode: TerminationMode,
        ) -> Result<(), DeployError> {
            self.terminated.lock().unwrap().push(handle.clone());
            Ok(())
        }
        async fn inspect(&self, _handle: &DeploymentHandle) -> DeploymentStatus {
            self.status.clone()
        }
        async fn signal_stop(&self, _handle: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    // @feature: node-runtime-inspect
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:5
    /// "One primitive. `node.runtime.inspect` asks the deployment provider what it knows of a node
    ///  birth's runtime handle and returns a typed inspection answer. It never mutates the runtime
    ///  or the fabric."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:26
    /// "Target | The exact birth's runtime handle (`RuntimeFact`: `deployment_id`, `provider`, `locator`)"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:27
    /// "Control domain | Runs in the control domain that owns the handle ... If the caller is not in
    ///  that control domain, the answer is `Unavailable`"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:28
    /// "Answers | `Running`, `OsStopped`, `Exited { exit_code: i32 }`, `Unavailable { reason }`"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:29
    /// "| Not a result | A RuntimeFact's presence says neither running nor gone; a provider that cannot answer for the domain answers `unavailable { reason }`, which is investigation's `unproven` |"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:31
    /// "Side effects | None. Never signals, stops, removes, or mutates the runtime."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:39
    /// "2. The caller checks whether it is in the control domain for `RuntimeFact`"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:43
    /// "3. The caller queries the provider by the exact handle (`deployment_id` + `locator`)"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:52
    /// "4. The provider returns the typed answer: `Running` ... `OsStopped` ... `Exited` ... `Unavailable`"
    #[tokio::test]
    async fn inspect_runtime_requires_exact_locator_in_control_domain_and_maps_provider_answers() {
        let dep_id = rdm_core::fixture_crockford60("dep-inspect-1");
        let inc = rdm_core::IncarnationId::mint();
        let valid_fact = RuntimeFact {
            deployment_id: dep_id.clone(),
            provider: RuntimeProvider::Process,
            control_domain: "process:boot-a:ns-1".into(),
            locator: RuntimeLocator::Process {
                pid: 48213,
                start: 1718289900,
            },
        };

        for (provider_status, expected) in [
            (DeploymentStatus::Running, Inspected::Running),
            (DeploymentStatus::OsStopped, Inspected::OsStopped),
            (
                DeploymentStatus::Exited { code: Some(0) },
                Inspected::Exited(Some(0)),
            ),
            (
                DeploymentStatus::Unknown,
                Inspected::Unavailable(
                    "the provider cannot tell its runtime's state: Unknown".into(),
                ),
            ),
        ] {
            let p = StubProvider {
                domain: "process:boot-a:ns-1".into(),
                status: provider_status.clone(),
                terminated: Mutex::default(),
            };
            let got = inspect_runtime(&p, &valid_fact, None, inc.as_str()).await;
            assert_eq!(got, expected);

            // Check `terminate_unless_os_stopped`: terminates on Running and Exited, skips on OsStopped.
            let handle = adopt(&p, &valid_fact).expect("adopt valid fact");
            let found = terminate_unless_os_stopped(&p, &handle, TerminationMode::Immediate)
                .await
                .expect("terminate_unless_os_stopped");
            assert_eq!(found, provider_status);
            if provider_status == DeploymentStatus::OsStopped {
                assert!(
                    p.terminated.lock().unwrap().is_empty(),
                    "os-stopped runtime must not be terminated by recovery"
                );
            } else {
                assert_eq!(p.terminated.lock().unwrap().len(), 1);
            }
        }

        let p = StubProvider {
            domain: "process:boot-a:ns-1".into(),
            status: DeploymentStatus::Running,
            terminated: Mutex::default(),
        };

        // Foreign control domain is refused by name.
        let foreign = RuntimeFact {
            control_domain: "process:boot-b:ns-2".into(),
            ..valid_fact.clone()
        };
        match inspect_runtime(&p, &foreign, None, inc.as_str()).await {
            Inspected::Unavailable(why) => {
                assert!(
                    why.contains("refused to adopt") && why.contains("control domain"),
                    "unexpected refusal reason: {why}"
                );
            }
            other => panic!("expected Unavailable for foreign control domain, got {other:?}"),
        }

        // Inexact locator (start == 0) is refused by name.
        let inexact = RuntimeFact {
            locator: RuntimeLocator::Process {
                pid: 48213,
                start: 0,
            },
            ..valid_fact
        };
        match inspect_runtime(&p, &inexact, None, inc.as_str()).await {
            Inspected::Unavailable(why) => {
                assert!(
                    why.contains("pid and its start token"),
                    "unexpected refusal reason: {why}"
                );
            }
            other => panic!("expected Unavailable for inexact locator, got {other:?}"),
        }
    }

    /// A provider whose inspect never answers and records when its in-flight inspect is dropped.
    struct Stalled {
        inner: StubProvider,
        dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    struct DropGuard(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Stalled {
        fn kind(&self) -> ProviderKind {
            self.inner.kind()
        }
        fn control_domain(&self) -> String {
            self.inner.control_domain()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            self.inner.spawn(spec).await
        }
        async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
            self.inner.terminate(handle, mode).await
        }
        async fn inspect(&self, _handle: &DeploymentHandle) -> DeploymentStatus {
            let _guard = DropGuard(self.dropped.clone());
            std::future::pending().await
        }
        async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
            self.inner.signal_stop(handle).await
        }
        async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
            self.inner.find(spec).await
        }
    }

    // @feature: node-runtime-inspect
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:13
    /// "`node.investigation`, step 1: the node's primary inspects the birth's runtime first, under a
    ///  2 s bound. A proven `Exited { exit_code }` is the verdict and skips the tickles; any other
    ///  answer (`Running`, `OsStopped`, `Unavailable`, or the 2 s bound expiring) proves nothing of
    ///  reachability and the tickles follow."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:30
    /// "Bound | 2 s from the provider call; an inspect that has not answered at 2 s answers
    ///  `Unavailable { reason: \"deadline: ...\" }` and the provider ends the subprocess it started;
    ///  node investigation's tickles follow"
    /// CONTRACT (Q273): node.runtime.inspect answers within its bound over every provider: a provider
    /// that does not answer within [`INSPECT_BOUND`] is `unavailable` with the reason deadline, never
    /// a proof of exit. Must NOT happen: an investigation waiting on its provider without end.
    #[tokio::test(start_paused = true)]
    async fn an_inspect_its_provider_does_not_answer_is_unavailable_at_its_bound() {
        use crate::deployment::provider::INSPECT_BOUND;
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let p = Stalled {
            inner: StubProvider { domain: "process:boot-a:ns-1".into(), status: DeploymentStatus::Running, terminated: Mutex::default() },
            dropped: dropped.clone(),
        };
        let fact = RuntimeFact {
            deployment_id: rdm_core::fixture_crockford60("dep-inspect-2"),
            provider: RuntimeProvider::Process,
            control_domain: "process:boot-a:ns-1".into(),
            locator: RuntimeLocator::Process { pid: 48214, start: 1718289901 },
        };
        let started = tokio::time::Instant::now();
        let got = inspect_runtime(&p, &fact, None, rdm_core::IncarnationId::mint().as_str()).await;
        assert!(matches!(&got, Inspected::Unavailable(why) if why.starts_with("deadline")), "{got:?}");
        assert_eq!(started.elapsed(), INSPECT_BOUND, "it answers at its bound");
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst), "the timed-out inspect future (and any kill_on_drop subprocess it started) is dropped at the bound");

        let tickles_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct Calls(std::sync::Arc<std::sync::atomic::AtomicBool>);
        #[async_trait::async_trait]
        impl crate::offline::TickleCalls for Calls {
            fn caller(&self) -> String {
                "mesh1.admin.1".into()
            }
            fn deadline(&self) -> std::time::Duration {
                std::time::Duration::from_secs(1)
            }
            async fn direct(&self, _node: &crate::model::Node, _probe: &rdm_core::status::StatusRequest) -> crate::offline::ProbeEnd {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                crate::offline::ProbeEnd::Other { end: "NotSent", detail: "not sent: Deadline".into() }
            }
            async fn via_peer(&self, _carrier: &crate::model::Node, _node: &crate::model::Node, _probe: &rdm_core::status::StatusRequest) -> crate::offline::ProbeEnd {
                crate::offline::ProbeEnd::Other { end: "NotSent", detail: "not sent: Deadline".into() }
            }
            fn carriers(&self, _view: &crate::topology::Topology, _node: &crate::model::Node) -> Vec<crate::model::Node> {
                Vec::new()
            }
            async fn leads(&self, _view: &crate::topology::Topology, _node: &crate::model::Node) -> Result<Vec<crate::model::Node>, String> {
                Ok(Vec::new())
            }
        }
        let mut node = crate::model::Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.node_id = crate::model::NodeId::mint();
        node.incarnation_id = Some(crate::model::IncarnationId::mint());
        let view = crate::topology::Topology {
            fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![node.clone()],
        };
        let finding = crate::node_investigation::investigate(
            &node,
            &view,
            &Calls(tickles_ran.clone()),
            async { crate::node_investigation::Finding::from(got.clone()) },
        )
        .await;
        assert!(tickles_ran.load(std::sync::atomic::Ordering::SeqCst), "node investigation's tickles follow when the 2 s bound expires");
        assert!(matches!(finding, crate::node_investigation::Finding::Unproven { reason } if reason.starts_with("deadline")));
    }
}
