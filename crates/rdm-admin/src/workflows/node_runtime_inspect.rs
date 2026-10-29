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
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:26
    /// "Target: The exact runtime handle recorded at launch: `provider_control_domain + pid + start
    ///  identity` for a process, `provider_control_domain + container id` for a container. A PID or a
    ///  container name alone is insufficient and is refused by name
    ///  Domain: The handle is acted on only inside its control domain; a cross-domain handle is routed
    ///  to a domain-local execution path or refused by name
    ///  Result: `running` ... `os-stopped` ... `exited { code }` ... `unavailable { reason }`"
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

    /// A provider whose inspect never answers.
    struct Stalled(StubProvider);

    #[async_trait::async_trait]
    impl DeploymentProvider for Stalled {
        fn kind(&self) -> ProviderKind {
            self.0.kind()
        }
        fn control_domain(&self) -> String {
            self.0.control_domain()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            self.0.spawn(spec).await
        }
        async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
            self.0.terminate(handle, mode).await
        }
        async fn inspect(&self, _handle: &DeploymentHandle) -> DeploymentStatus {
            std::future::pending().await
        }
        async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
            self.0.signal_stop(handle).await
        }
        async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
            self.0.find(spec).await
        }
    }

    // @feature: node-runtime-inspect
    /// CONTRACT (Q273): node.runtime.inspect answers within its bound over every provider: a provider
    /// that does not answer within [`INSPECT_BOUND`] is `unavailable` with the reason deadline, never
    /// a proof of exit. Must NOT happen: an investigation waiting on its provider without end.
    #[tokio::test(start_paused = true)]
    async fn an_inspect_its_provider_does_not_answer_is_unavailable_at_its_bound() {
        use crate::deployment::provider::INSPECT_BOUND;
        let p = Stalled(StubProvider { domain: "process:boot-a:ns-1".into(), status: DeploymentStatus::Running, terminated: Mutex::default() });
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
    }
}
