//! Per-process CPU and RAM measurement, with dev-only env-var overrides
//! gated by [`Deployment`]. Production path uses `sysinfo`. Dev path lets
//! tests inject deterministic values.

use crate::deployment::Deployment;
use std::sync::Mutex;
use sysinfo::{Pid, ProcessRefreshKind, RefreshKind, System};

/// Resolved load values to ship inside `GossipDigest`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct NodeLoad {
    pub cpu_used: f32,    // cores (e.g. 2.4 = 2.4 cores' worth of work)
    pub cpu_budget: f32,  // cores ceiling
    pub ram_used: f32,    // GB (resident memory)
    pub ram_budget: f32,  // GB ceiling
}

/// Sampler — holds a long-lived `sysinfo::System` so cpu deltas work
/// (sysinfo's `process.cpu_usage()` only returns meaningful values
/// after at least two refresh ticks separated by ≥200ms).
pub struct LoadSampler {
    sys: Mutex<System>,
    pid: Pid,
    deployment: Deployment,
    /// Effective host cpu count (cached at construction; never changes
    /// for the life of the process). Used as the cpu_budget fallback.
    host_cpu_count: f32,
    /// Total system memory in bytes (cached at construction).
    host_total_ram_bytes: u64,
}

impl LoadSampler {
    pub fn new(deployment: Deployment) -> Self {
        let refresh = RefreshKind::nothing()
            .with_memory(sysinfo::MemoryRefreshKind::everything())
            .with_processes(ProcessRefreshKind::everything().with_cpu());
        let mut sys = System::new_with_specifics(refresh);
        sys.refresh_memory();
        sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
        let pid = Pid::from_u32(std::process::id());
        let host_cpu_count = sys.cpus().len() as f32;
        let host_total_ram_bytes = sys.total_memory();
        Self {
            sys: Mutex::new(sys),
            pid,
            deployment,
            host_cpu_count,
            host_total_ram_bytes,
        }
    }

    /// Sample the current load. Honors `RAFKA_DEV_*` overrides when
    /// `deployment.allows_dev_overrides()`; otherwise always measures.
    /// Override-but-ignored cases (prod mode with a `RAFKA_DEV_*` set)
    /// are logged once at construction by [`warn_on_ignored_overrides`].
    pub fn sample(&self) -> NodeLoad {
        let mut sys = self.sys.lock().unwrap();
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[self.pid]), true);
        sys.refresh_memory();

        let (real_cpu_used, real_ram_used_bytes) = if let Some(p) = sys.process(self.pid) {
            // sysinfo cpu_usage() returns "% of one core" — values >100 mean
            // multi-core utilization. Divide by 100 to get cores.
            (p.cpu_usage() / 100.0, p.memory())
        } else {
            (0.0, 0)
        };
        let real_ram_used_gb = bytes_to_gb(real_ram_used_bytes);
        let real_cpu_budget = self.host_cpu_count;
        let real_ram_budget_gb = bytes_to_gb(self.host_total_ram_bytes);

        let allow_dev = self.deployment.allows_dev_overrides();

        NodeLoad {
            cpu_used:   resolve_override("RAFKA_DEV_CPU_USED",   allow_dev).unwrap_or(real_cpu_used),
            cpu_budget: resolve_override("RAFKA_DEV_CPU_BUDGET", allow_dev).unwrap_or(real_cpu_budget),
            ram_used:   resolve_override("RAFKA_DEV_RAM_USED",   allow_dev).unwrap_or(real_ram_used_gb),
            ram_budget: resolve_override("RAFKA_DEV_RAM_BUDGET", allow_dev).unwrap_or(real_ram_budget_gb),
        }
    }
}

fn bytes_to_gb(bytes: u64) -> f32 {
    (bytes as f64 / 1_073_741_824.0) as f32
}

fn resolve_override(var: &str, allow: bool) -> Option<f32> {
    if !allow {
        return None;
    }
    std::env::var(var).ok()?.parse::<f32>().ok()
}

/// Emit a single startup span describing what's happening with overrides.
/// Two situations produce output:
///   1. deployment != prod AND at least one override is set
///      → INFO span listing which fields are simulated
///   2. deployment == prod AND at least one override is set
///      → WARN span listing the overrides that were ignored
/// Otherwise silent.
pub fn announce_overrides(deployment: Deployment) {
    let vars = [
        "RAFKA_DEV_CPU_USED",
        "RAFKA_DEV_CPU_BUDGET",
        "RAFKA_DEV_RAM_USED",
        "RAFKA_DEV_RAM_BUDGET",
    ];
    let set: Vec<&str> = vars
        .iter()
        .copied()
        .filter(|v| std::env::var(v).is_ok())
        .collect();
    if set.is_empty() {
        return;
    }
    if deployment.allows_dev_overrides() {
        tracing::info_span!(
            "rafka.mesh.node.dev_overrides_active",
            fields = ?set,
            deployment = ?deployment,
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!(?set, ?deployment, "dev load overrides active"));
    } else {
        tracing::info_span!(
            "rafka.mesh.node.dev_overrides_ignored",
            fields = ?set,
            deployment = ?deployment,
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::warn!(?set, ?deployment, "dev load overrides set but ignored (deployment=prod)"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialize env-var mutation across tests in this module.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_envs<F: FnOnce()>(pairs: &[(&str, Option<&str>)], f: F) {
        let _g = ENV_LOCK.lock().unwrap();
        let prev: Vec<(String, Option<String>)> = pairs
            .iter()
            .map(|(k, _)| ((*k).to_string(), std::env::var(*k).ok()))
            .collect();
        for (k, v) in pairs {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        f();
        for (k, prev_val) in prev {
            match prev_val {
                Some(v) => std::env::set_var(&k, v),
                None => std::env::remove_var(&k),
            }
        }
    }

    #[test]
    fn prod_ignores_overrides() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("prod")),
                ("RAFKA_DEV_CPU_BUDGET", Some("999")),
                ("RAFKA_DEV_RAM_BUDGET", Some("999")),
            ],
            || {
                let s = LoadSampler::new(Deployment::Prod);
                let l = s.sample();
                assert!(
                    l.cpu_budget < 999.0,
                    "prod must ignore RAFKA_DEV_CPU_BUDGET; got {}",
                    l.cpu_budget
                );
                assert!(
                    l.ram_budget < 999.0,
                    "prod must ignore RAFKA_DEV_RAM_BUDGET; got {}",
                    l.ram_budget
                );
            },
        );
    }

    #[test]
    fn dev_honors_budget_overrides() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_BUDGET", Some("4.0")),
                ("RAFKA_DEV_RAM_BUDGET", Some("2.0")),
            ],
            || {
                let s = LoadSampler::new(Deployment::Dev);
                let l = s.sample();
                assert_eq!(l.cpu_budget, 4.0);
                assert_eq!(l.ram_budget, 2.0);
            },
        );
    }

    #[test]
    fn dev_honors_used_overrides() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_USED", Some("3.8")),
                ("RAFKA_DEV_RAM_USED", Some("1.5")),
            ],
            || {
                let s = LoadSampler::new(Deployment::Dev);
                let l = s.sample();
                assert_eq!(l.cpu_used, 3.8);
                assert_eq!(l.ram_used, 1.5);
            },
        );
    }

    #[test]
    fn dev_without_overrides_measures_real_values() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_USED", None),
                ("RAFKA_DEV_CPU_BUDGET", None),
                ("RAFKA_DEV_RAM_USED", None),
                ("RAFKA_DEV_RAM_BUDGET", None),
            ],
            || {
                let s = LoadSampler::new(Deployment::Dev);
                let l = s.sample();
                // host cpu count is at least 1 on any test runner
                assert!(l.cpu_budget >= 1.0, "cpu_budget {} should be >= 1.0", l.cpu_budget);
                // host has at least 64 MB of RAM on any test runner
                assert!(l.ram_budget > 0.0, "ram_budget {} should be > 0", l.ram_budget);
            },
        );
    }

    #[test]
    fn unparseable_override_falls_back_to_real() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_BUDGET", Some("not a number")),
            ],
            || {
                let s = LoadSampler::new(Deployment::Dev);
                let l = s.sample();
                // garbage override must NOT clobber the real value
                assert!(l.cpu_budget >= 1.0);
            },
        );
    }
}
