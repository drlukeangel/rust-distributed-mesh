//! Per-process CPU and RAM measurement with optional programmatic overrides.
//! The sampler is "dumb": it consumes values passed at construction (or falls
//! through to sysinfo if `None`). Env-var reading is the caller's concern.
//!
//! Dev-mode helpers (`load_env_dev_from`, `read_dev_cpu_budget`,
//! `read_dev_ram_budget`, `announce_dev_state`) are opt-in — each binary
//! calls them explicitly from main(); NodeRuntime never calls them.

use std::sync::Mutex;
use sysinfo::{Pid, ProcessRefreshKind, RefreshKind, System};

use crate::deployment::Deployment;

// ---------------------------------------------------------------------------
// Dev-mode env helpers (opt-in; called by each binary's main())
// ---------------------------------------------------------------------------

/// Populate the process env from `<manifest_dir>/.env.dev` IF the file
/// exists. Each k=value line is set as a process env var ONLY if the var
/// is not already set (parent-process injection wins). Lines starting
/// with `#` are comments; blank lines ignored. Values may be optionally
/// wrapped in single or double quotes (stripped on read).
///
/// Missing file is NOT an error — production deployments ship only the
/// binary, not the crate source, so this is a silent no-op in prod.
///
/// As a side effect, if the file was loaded AND `RAFKA_DEPLOYMENT` is
/// unset, this sets `RAFKA_DEPLOYMENT=dev`. The presence of `.env.dev`
/// IS the dev-mode signal — no separate flag to remember.
///
/// Callers typically pass `env!("CARGO_MANIFEST_DIR")` from their binary's
/// main(). That macro is captured at compile time and points at the
/// caller's crate root, NOT rafka-node-base.
pub fn load_env_dev_from(manifest_dir: &str) {
    let path = std::path::PathBuf::from(manifest_dir).join(".env.dev");
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return, // missing is fine — non-dev deployments
    };
    let mut loaded_any = false;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, v) = match line.split_once('=') {
            Some(parts) => parts,
            None => continue,
        };
        let k = k.trim();
        let v = v
            .trim()
            .trim_matches('"')
            .trim_matches('\'');
        if !k.is_empty() && std::env::var(k).is_err() {
            std::env::set_var(k, v);
            loaded_any = true;
        }
    }
    if loaded_any && std::env::var("RAFKA_DEPLOYMENT").is_err() {
        std::env::set_var("RAFKA_DEPLOYMENT", "dev");
    }
}

/// Read `RAFKA_DEV_CPU_BUDGET` from process env, returning Some(value)
/// only when:
///   - Deployment mode allows dev overrides (i.e. not Prod), AND
///   - The env var is set, AND
///   - The value parses as an f32
/// Otherwise returns None.
///
/// Designed to be called from a node binary's main() as a fallback when
/// no CLI flag was provided. The Deployment gate ensures a leaked
/// RAFKA_DEV_* env var in a prod deployment has no effect.
pub fn read_dev_cpu_budget() -> Option<f32> {
    read_dev_env_f32("RAFKA_DEV_CPU_BUDGET")
}

/// Same shape as `read_dev_cpu_budget`, for `RAFKA_DEV_RAM_BUDGET`.
pub fn read_dev_ram_budget() -> Option<f32> {
    read_dev_env_f32("RAFKA_DEV_RAM_BUDGET")
}

fn read_dev_env_f32(var: &str) -> Option<f32> {
    if !Deployment::from_env().allows_dev_overrides() {
        return None;
    }
    std::env::var(var).ok()?.parse::<f32>().ok()
}

/// Emit a single startup span describing the dev override state. Call
/// once from main() AFTER load_env_dev_from and AFTER any CLI parsing.
/// Pass the resolved values you're about to hand to NodeRuntime.
///
/// Behavior:
///   - If deployment != Prod AND at least one Some(v) was passed: INFO
///     span listing which fields have overrides + their source-of-truth
///     ("cli" or "env"). Caller computes the source attribution; this
///     helper just describes the resolved view.
///   - If deployment == Prod AND any RAFKA_DEV_* env var is set in the
///     environment: WARN span listing which ones are present-but-ignored.
///   - Otherwise silent.
pub fn announce_dev_state(
    cpu_budget: Option<f32>,
    ram_budget: Option<f32>,
) {
    let deployment = Deployment::from_env();
    let env_vars_present: Vec<&str> = [
        "RAFKA_DEV_CPU_BUDGET",
        "RAFKA_DEV_RAM_BUDGET",
    ]
    .iter()
    .copied()
    .filter(|v| std::env::var(v).is_ok())
    .collect();
    if deployment.allows_dev_overrides() {
        let any_set = cpu_budget.is_some() || ram_budget.is_some();
        if !any_set {
            return;
        }
        tracing::info_span!(
            "rafka.mesh.node.dev_state",
            cpu_budget = ?cpu_budget,
            ram_budget = ?ram_budget,
            deployment = ?deployment,
            "otel.kind" = "internal",
        )
        .in_scope(|| {
            tracing::info!(
                ?cpu_budget,
                ?ram_budget,
                ?deployment,
                "node hydrating with explicit budget"
            );
        });
    } else if !env_vars_present.is_empty() {
        tracing::info_span!(
            "rafka.mesh.node.dev_overrides_ignored",
            ignored = ?env_vars_present,
            deployment = ?deployment,
            "otel.kind" = "internal",
        )
        .in_scope(|| {
            tracing::warn!(
                ignored = ?env_vars_present,
                ?deployment,
                "RAFKA_DEV_* env vars present but ignored (deployment != dev)"
            );
        });
    }
}

// ---------------------------------------------------------------------------
// CLI budget-flag parser (used by each binary's main())
// ---------------------------------------------------------------------------

/// Parsed values for the two budget CLI flags each node binary accepts.
/// Used by each binary's main() to opt into CLI overrides.
#[derive(Debug, Default, Clone, Copy)]
pub struct BudgetCliArgs {
    pub cpu_budget: Option<f32>,
    pub ram_budget: Option<f32>,
}

/// Hand-rolled parser for `--cpu-budget <f32>` and `--ram-budget <f32>`
/// flags out of `std::env::args()`. Other args are ignored (this is not
/// a full CLI; nodes have no other flags today). Unparseable values are
/// silently treated as absent — the binary's main() decides what to do
/// next (typically fall through to env).
pub fn parse_budget_cli_args() -> BudgetCliArgs {
    let mut out = BudgetCliArgs::default();
    let mut args = std::env::args().peekable();
    // Skip argv[0] (the binary name)
    let _ = args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cpu-budget" => {
                out.cpu_budget = args.next().and_then(|s| s.parse::<f32>().ok());
            }
            "--ram-budget" => {
                out.ram_budget = args.next().and_then(|s| s.parse::<f32>().ok());
            }
            // Allow `--cpu-budget=4.0` long form too.
            s if s.starts_with("--cpu-budget=") => {
                out.cpu_budget = s["--cpu-budget=".len()..].parse::<f32>().ok();
            }
            s if s.starts_with("--ram-budget=") => {
                out.ram_budget = s["--ram-budget=".len()..].parse::<f32>().ok();
            }
            _ => {}
        }
    }
    out
}

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
    fn read_dev_cpu_budget_returns_none_in_prod() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("prod")),
                ("RAFKA_DEV_CPU_BUDGET", Some("99")),
            ],
            || {
                assert_eq!(read_dev_cpu_budget(), None);
            },
        );
    }

    #[test]
    fn read_dev_cpu_budget_returns_value_in_dev() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_BUDGET", Some("4.0")),
            ],
            || {
                assert_eq!(read_dev_cpu_budget(), Some(4.0));
            },
        );
    }

    #[test]
    fn read_dev_cpu_budget_returns_none_when_unset() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_BUDGET", None),
            ],
            || {
                assert_eq!(read_dev_cpu_budget(), None);
            },
        );
    }

    #[test]
    fn read_dev_cpu_budget_returns_none_on_unparseable() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_BUDGET", Some("banana")),
            ],
            || {
                assert_eq!(read_dev_cpu_budget(), None);
            },
        );
    }

    #[test]
    fn read_dev_ram_budget_symmetric() {
        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_RAM_BUDGET", Some("2.0")),
            ],
            || {
                assert_eq!(read_dev_ram_budget(), Some(2.0));
            },
        );
    }

    #[test]
    fn load_env_dev_from_nonexistent_dir_is_silent_noop() {
        with_envs(
            &[("RAFKA_DEPLOYMENT", None), ("RAFKA_DEV_CPU_BUDGET", None)],
            || {
                load_env_dev_from("/this/path/does/not/exist");
                assert!(std::env::var("RAFKA_DEPLOYMENT").is_err());
                assert!(std::env::var("RAFKA_DEV_CPU_BUDGET").is_err());
            },
        );
    }

    #[test]
    fn load_env_dev_from_real_file_sets_env_and_dev_mode() {
        // Write a temp .env.dev file in a temp dir, point loader at it.
        let tmp = std::env::temp_dir().join(format!(
            "rafka-test-env-dev-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let file = tmp.join(".env.dev");
        std::fs::write(
            &file,
            "RAFKA_DEV_CPU_BUDGET=4.0\nRAFKA_DEV_RAM_BUDGET=2.0\n# comment\n",
        )
        .unwrap();

        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", None),
                ("RAFKA_DEV_CPU_BUDGET", None),
                ("RAFKA_DEV_RAM_BUDGET", None),
            ],
            || {
                load_env_dev_from(tmp.to_str().unwrap());
                assert_eq!(std::env::var("RAFKA_DEV_CPU_BUDGET").ok().as_deref(), Some("4.0"));
                assert_eq!(std::env::var("RAFKA_DEV_RAM_BUDGET").ok().as_deref(), Some("2.0"));
                // file presence implies dev mode
                assert_eq!(std::env::var("RAFKA_DEPLOYMENT").ok().as_deref(), Some("dev"));
            },
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn load_env_dev_from_does_not_override_preset_env() {
        // If an env var is already set (e.g. by parent process), the loader
        // must NOT override it.
        let tmp = std::env::temp_dir().join(format!(
            "rafka-test-noOverride-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let file = tmp.join(".env.dev");
        std::fs::write(&file, "RAFKA_DEV_CPU_BUDGET=4.0\n").unwrap();

        with_envs(
            &[
                ("RAFKA_DEPLOYMENT", Some("dev")),
                ("RAFKA_DEV_CPU_BUDGET", Some("2.0")), // pre-set by "parent"
            ],
            || {
                load_env_dev_from(tmp.to_str().unwrap());
                // pre-set value wins
                assert_eq!(std::env::var("RAFKA_DEV_CPU_BUDGET").ok().as_deref(), Some("2.0"));
            },
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

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

    #[test]
    fn parse_budget_cli_no_args() {
        let parsed = BudgetCliArgs::default();
        assert!(parsed.cpu_budget.is_none() && parsed.ram_budget.is_none());
    }
}
