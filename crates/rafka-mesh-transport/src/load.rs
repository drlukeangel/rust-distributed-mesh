//! The publishing process's own CPU and RAM, sampled for every digest it publishes.
//!
//! One long-lived `sysinfo::System` per process, so the CPU figure is the use since the previous
//! sample (sysinfo's per-process CPU needs two refreshes apart). The ceilings are the host's cores
//! and memory unless the process was given a budget (`RDM_CPU_BUDGET_MILLICORES`,
//! `RDM_RAM_BUDGET_BYTES`).

use rafka_mesh_entity::NodeLoad;
use std::sync::Mutex;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// This process's load sampler.
pub struct LoadSampler {
    sys: Mutex<System>,
    pid: Pid,
    cpu_budget_millicores: u32,
    ram_budget_bytes: u64,
}

impl LoadSampler {
    /// A sampler for this process; the budgets come from the environment, else the host.
    pub fn for_this_process() -> Self {
        // rafka-v2's sampler (rafka-node-base load.rs): an EMPTY System, host memory read once,
        // and only this pid's cpu+memory refreshed, so no process table is enumerated or held.
        let pid = Pid::from_u32(std::process::id());
        let mut sys = System::new();
        sys.refresh_memory();
        let host_ram = sys.total_memory();
        sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, ProcessRefreshKind::nothing().with_cpu().with_memory());
        let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
        let host_cores = std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1);
        Self {
            cpu_budget_millicores: env("RDM_CPU_BUDGET_MILLICORES").map(|v| v as u32).unwrap_or(host_cores * 1000),
            ram_budget_bytes: env("RDM_RAM_BUDGET_BYTES").unwrap_or(host_ram),
            sys: Mutex::new(sys),
            pid,
        }
    }

    /// The load now: CPU since the previous sample, resident memory, and the ceilings.
    pub fn sample(&self) -> NodeLoad {
        let mut sys = self.sys.lock().unwrap();
        sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[self.pid]), false, ProcessRefreshKind::nothing().with_cpu().with_memory());
        // sysinfo reports a process's CPU as percent of one core (above 100 on several cores).
        let (cpu_pct, rss) = sys.process(self.pid).map(|p| (p.cpu_usage(), p.memory())).unwrap_or((0.0, 0));
        NodeLoad {
            cpu_used_millicores: (cpu_pct * 10.0).round().max(0.0) as u32,
            cpu_budget_millicores: self.cpu_budget_millicores,
            ram_used_bytes: rss,
            ram_budget_bytes: self.ram_budget_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: a sample of this very process names its resident memory and the host ceilings.
    #[test]
    fn a_sample_of_this_process_carries_its_memory_and_ceilings() {
        let s = LoadSampler::for_this_process();
        let l = s.sample();
        assert!(l.ram_used_bytes > 0, "{l:?}");
        assert!(l.ram_budget_bytes >= l.ram_used_bytes, "{l:?}");
        assert!(l.cpu_budget_millicores >= 1000, "{l:?}");
    }
}
