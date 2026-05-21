//! Per-process CPU and RAM measurement with optional programmatic overrides.
//! The sampler is "dumb": it consumes values passed at construction (or falls
//! through to sysinfo if `None`). Env-var reading is the caller's concern.

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
    host_cpu_count: f32,
    host_total_ram_bytes: u64,
    // Programmatic overrides passed at construction. If Some(v), sample()
    // returns v for that field instead of the sysinfo measurement.
    cpu_used_override: Option<f32>,
    cpu_budget_override: Option<f32>,
    ram_used_override: Option<f32>,
    ram_budget_override: Option<f32>,
}

impl LoadSampler {
    pub fn new(
        cpu_budget: Option<f32>,
        ram_budget: Option<f32>,
        cpu_used: Option<f32>,
        ram_used: Option<f32>,
    ) -> Self {
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
            host_cpu_count,
            host_total_ram_bytes,
            cpu_used_override: cpu_used,
            cpu_budget_override: cpu_budget,
            ram_used_override: ram_used,
            ram_budget_override: ram_budget,
        }
    }

    /// Sample the current load. If a field override was provided at
    /// construction, that value is returned directly; otherwise sysinfo
    /// is queried for a live measurement.
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

        NodeLoad {
            cpu_used:   self.cpu_used_override.unwrap_or(real_cpu_used),
            cpu_budget: self.cpu_budget_override.unwrap_or(real_cpu_budget),
            ram_used:   self.ram_used_override.unwrap_or(real_ram_used_gb),
            ram_budget: self.ram_budget_override.unwrap_or(real_ram_budget_gb),
        }
    }
}

fn bytes_to_gb(bytes: u64) -> f32 {
    (bytes as f64 / 1_073_741_824.0) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_overrides_uses_sysinfo() {
        let s = LoadSampler::new(None, None, None, None);
        let l = s.sample();
        // host has at least 1 cpu and >0 ram on any reasonable test runner
        assert!(l.cpu_budget >= 1.0, "cpu_budget {} should be >= 1.0", l.cpu_budget);
        assert!(l.ram_budget > 0.0, "ram_budget {} should be > 0", l.ram_budget);
    }

    #[test]
    fn cpu_budget_override_wins_over_sysinfo() {
        let s = LoadSampler::new(Some(4.0), None, None, None);
        let l = s.sample();
        assert_eq!(l.cpu_budget, 4.0);
        // sysinfo still drives the other three
        assert!(l.ram_budget > 0.0);
    }

    #[test]
    fn ram_budget_override_wins_over_sysinfo() {
        let s = LoadSampler::new(None, Some(2.0), None, None);
        let l = s.sample();
        assert_eq!(l.ram_budget, 2.0);
        assert!(l.cpu_budget >= 1.0);
    }

    #[test]
    fn used_overrides_win_over_sysinfo() {
        let s = LoadSampler::new(None, None, Some(3.8), Some(1.5));
        let l = s.sample();
        assert_eq!(l.cpu_used, 3.8);
        assert_eq!(l.ram_used, 1.5);
    }

    #[test]
    fn all_four_overrides() {
        let s = LoadSampler::new(Some(4.0), Some(2.0), Some(3.0), Some(1.5));
        let l = s.sample();
        assert_eq!(l.cpu_budget, 4.0);
        assert_eq!(l.ram_budget, 2.0);
        assert_eq!(l.cpu_used, 3.0);
        assert_eq!(l.ram_used, 1.5);
    }
}
