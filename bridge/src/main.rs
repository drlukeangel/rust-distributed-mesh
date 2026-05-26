use anyhow::Result;
use rafka_node_base::{
    announce_dev_state, load_env_dev_from, parse_budget_cli_args, read_dev_cpu_budget,
    read_dev_ram_budget, NodeRuntime, Role,
};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Populate process env from this crate's .env.dev (no-op in prod
    //    deployments where the file isn't shipped).
    load_env_dev_from(env!("CARGO_MANIFEST_DIR"));

    // 2. Parse --cpu-budget / --ram-budget CLI flags (passed by admin-ui's
    //    spawn_one, or by an operator running the binary directly).
    let cli = parse_budget_cli_args();

    // 3. Resolve budgets: CLI flag wins; else env (read_dev_*_budget,
    //    gated by RAFKA_DEPLOYMENT); else None → NodeRuntime falls through
    //    to sysinfo measurement.
    let cpu_budget: Option<f32> = if cli.cpu_budget.is_some() {
        cli.cpu_budget
    } else {
        read_dev_cpu_budget()
    };
    let ram_budget: Option<f32> = if cli.ram_budget.is_some() {
        cli.ram_budget
    } else {
        read_dev_ram_budget()
    };

    // 4. Announce the dev state for observability.
    announce_dev_state(cpu_budget, ram_budget);

    // 5. Build and run the node.
    let mut rt = NodeRuntime::new("bridge").with_role(Role::Bridge);
    if let Some(c) = cpu_budget {
        rt = rt.with_cpu_budget(c);
    }
    if let Some(r) = ram_budget {
        rt = rt.with_ram_budget(r);
    }
    rt.run().await
}
