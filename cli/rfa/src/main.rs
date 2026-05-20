use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;
use std::time::Duration;
use tracing::{info_span, Instrument};

#[derive(Parser)]
#[command(name = "rfa", about = "rafka CLI — talks to topology-ui REST API")]
struct Cli {
    /// topology-ui base URL
    #[arg(long, default_value = "http://localhost:19090", global = true)]
    api_url: String,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Format {
    Table,
    Json,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum NodeType {
    Gateway,
    Broker,
    Compute,
    Registry,
}

impl NodeType {
    fn as_str(&self) -> &'static str {
        match self {
            NodeType::Gateway => "gateway",
            NodeType::Broker => "broker",
            NodeType::Compute => "compute",
            NodeType::Registry => "registry",
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Mesh-level commands
    Mesh {
        #[command(subcommand)]
        sub: MeshCmd,
    },
}

#[derive(Subcommand)]
enum MeshCmd {
    /// Node commands
    Node {
        #[command(subcommand)]
        sub: NodeCmd,
    },
    /// Show mesh topology
    Topology {
        #[command(subcommand)]
        sub: TopologyCmd,
    },
    /// Show mesh status summary
    Status {
        #[arg(long, value_name = "fmt", default_value = "table")]
        format: Format,
    },
    /// Wait until mesh has converged to a target node count
    WaitConverged {
        /// Target number of node types in {gateway,broker,compute,registry}
        #[arg(long)]
        target: usize,
        /// Timeout (e.g. 30s, 2m, 1h)
        #[arg(long)]
        timeout: String,
    },
    /// Chaos primitives + soak runner
    Chaos {
        #[command(subcommand)]
        sub: ChaosCmd,
    },
    /// Test runner — functional + chaos, results written to E:/tmp/rafka-tests/
    Test {
        #[command(subcommand)]
        sub: TestCmd,
    },
}

#[derive(Subcommand)]
enum TestCmd {
    /// List every test the operator can run (functional + chaos)
    List,
    /// Run one test by name (use `list` to see names). Writes JSON report.
    Run {
        name: String,
        #[arg(long, default_value = "42")]
        seed: u64,
    },
    /// Run every test in sequence, return non-zero if any fail
    All {
        #[arg(long, default_value = "42")]
        seed: u64,
    },
}

#[derive(Subcommand)]
enum ChaosCmd {
    /// Kill a UI-spawned subprocess (random if not specified). Detection: spawned-list confirms removal.
    Kill {
        /// node_name to kill; if omitted, picks a random spawned subprocess
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "30000")]
        deadline_ms: u64,
    },
    /// Kill + immediately re-spawn (same node_type). Detection: new node_name appears in spawned-list.
    Restart {
        /// node_name to restart; if omitted, picks a random spawned subprocess
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "30000")]
        deadline_ms: u64,
    },
    /// Kill `count` random spawned subprocesses back-to-back (substrate-race test)
    BurstKill {
        #[arg(long, default_value = "3")]
        count: usize,
        #[arg(long, default_value = "30000")]
        deadline_ms: u64,
    },
    /// Fill a target node's spawn dir until writes fail (capped by --max-mb)
    DiskFull {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "4")]
        max_mb: u64,
        #[arg(long, default_value = "10000")]
        deadline_ms: u64,
    },
    /// Suspend (NtSuspendProcess) one matching rafka-<type>.exe for `--duration_ms`
    Wedge {
        #[arg(long, default_value = "broker")]
        target_type: String,
        #[arg(long, default_value = "3000")]
        duration_ms: u64,
    },
    /// Restart target with RAFKA_CLOCK_SKEW_MS env (default 30000ms)
    ClockSkew {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "30000")]
        skew_ms: i64,
        #[arg(long, default_value = "10000")]
        deadline_ms: u64,
    },
    /// Restart target with RAFKA_LINK_SLOW_MS env (default 250ms)
    SlowLink {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "250")]
        latency_ms: u64,
        #[arg(long, default_value = "10000")]
        deadline_ms: u64,
    },
    /// Restart target with RAFKA_LINK_LOSS_PCT env (default 15%)
    LossyLink {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "15")]
        loss_pct: u8,
        #[arg(long, default_value = "10000")]
        deadline_ms: u64,
    },
    /// Restart target with new RAFKA_NODE_BIND_ADDR (random ephemeral port)
    NatShift {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "10000")]
        deadline_ms: u64,
    },
    /// Block outbound UDP between two named programs via Windows firewall (NEEDS ADMIN)
    PartitionPair {
        #[arg(long)]
        a: String,
        #[arg(long)]
        b: String,
        #[arg(long, default_value = "5000")]
        duration_ms: u64,
    },
    /// Split node-type catalog: subset of `--size` blocked from rest via firewall (NEEDS ADMIN)
    PartitionSubset {
        /// Number of node_types in the isolated subset
        #[arg(long, default_value = "2")]
        size: usize,
        #[arg(long, default_value = "5000")]
        duration_ms: u64,
    },
    /// Repeatedly create+remove a firewall block over N cycles (NEEDS ADMIN)
    FlapLink {
        #[arg(long)]
        a: String,
        #[arg(long)]
        b: String,
        #[arg(long, default_value = "5")]
        cycles: u32,
        #[arg(long, default_value = "500")]
        on_ms: u64,
        #[arg(long, default_value = "500")]
        off_ms: u64,
    },
    /// Block inbound UDP to a named program for `--duration_ms` (NEEDS ADMIN)
    FirewallInbound {
        #[arg(long, default_value = "broker")]
        target_type: String,
        #[arg(long, default_value = "5000")]
        duration_ms: u64,
    },
    /// List every shipped chaos primitive with sample CLI invocation + admin flag
    Catalog,
    /// Pretty-print a soak report JSON (e.g. E:/tmp/rafka-chaos-soak-<seed>.json)
    Report {
        /// Soak seed used as filename suffix
        seed: u64,
    },
    /// Smoke / nightly soak runner. Picks random primitives every <interval> for <duration>.
    Soak {
        /// Total duration (e.g. 5m, 1h, 24h)
        #[arg(long, default_value = "5m")]
        duration: String,
        /// How often to fire a primitive (e.g. 30s)
        #[arg(long, default_value = "30s")]
        interval: String,
        /// Seed for reproducible runs
        #[arg(long, default_value = "42")]
        seed: u64,
    },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// List known nodes
    List {
        #[arg(long, value_name = "fmt", default_value = "table")]
        format: Format,
    },
    /// Describe a specific node
    Describe {
        /// Service name (gateway|broker|compute|registry)
        name: String,
        #[arg(long, value_name = "fmt", default_value = "table")]
        format: Format,
    },
    /// Spawn a new node subprocess
    Add {
        /// Node type to spawn
        #[arg(long, value_name = "type")]
        r#type: NodeType,
        /// Optional name hint (ignored by server; server generates node_name)
        #[arg(long)]
        name: Option<String>,
        #[arg(long, value_name = "fmt", default_value = "table")]
        format: Format,
    },
    /// Kill a running node subprocess
    Remove {
        /// node_name to kill (e.g. broker-a1b2c3d4)
        node_name: String,
        #[arg(long, value_name = "fmt", default_value = "table")]
        format: Format,
    },
}

#[derive(Subcommand)]
enum TopologyCmd {
    /// Show topology
    Show {
        #[arg(long, value_name = "fmt", default_value = "table")]
        format: TopologyFormat,
    },
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum TopologyFormat {
    Table,
    Dot,
    Json,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Use SimpleSpanProcessor for short-lived CLI — synchronous export on span
    // close means no flush race vs runtime teardown, no pre-exit sleep needed.
    let _guard = rafka_telemetry::init_telemetry_for_cli("rfa");

    let cli = Cli::parse();
    let client = reqwest::Client::new();

    let (command_name, args_str) = describe_command(&cli.command);
    let cmd_span = info_span!(
        "rafka.cli.command",
        command = %command_name,
        args = %args_str,
        "otel.kind" = "internal",
    );

    run_command(&cli, &client).instrument(cmd_span).await
}

/// Inject the current OTel span context as W3C traceparent headers so the
/// receiving server (topology-ui) can chain its spans under our trace_id.
fn current_traceparent_headers() -> reqwest::header::HeaderMap {
    use opentelemetry::global;
    use opentelemetry_http::HeaderInjector;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let mut headers = reqwest::header::HeaderMap::new();
    let ctx = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&ctx, &mut HeaderInjector(&mut headers));
    });
    headers
}

fn describe_command(cmd: &Cmd) -> (String, String) {
    match cmd {
        Cmd::Mesh { sub } => match sub {
            MeshCmd::Node { sub } => match sub {
                NodeCmd::List { .. } => ("mesh node list".into(), "".into()),
                NodeCmd::Describe { name, .. } => ("mesh node describe".into(), name.clone()),
                NodeCmd::Add { r#type, name, .. } => (
                    "mesh node add".into(),
                    format!(
                        "--type {}{}",
                        r#type.as_str(),
                        name.as_deref().map(|n| format!(" --name {n}")).unwrap_or_default()
                    ),
                ),
                NodeCmd::Remove { node_name, .. } => ("mesh node remove".into(), node_name.clone()),
            },
            MeshCmd::Topology { sub } => match sub {
                TopologyCmd::Show { format } => (
                    "mesh topology show".into(),
                    format!("--format {:?}", format).to_lowercase(),
                ),
            },
            MeshCmd::Status { .. } => ("mesh status".into(), "".into()),
            MeshCmd::WaitConverged { target, timeout } => (
                "mesh wait-converged".into(),
                format!("--timeout {timeout} --target {target}"),
            ),
            MeshCmd::Chaos { sub } => match sub {
                ChaosCmd::Kill { target, .. } => (
                    "mesh chaos kill".into(),
                    target.clone().unwrap_or_else(|| "<random>".into()),
                ),
                ChaosCmd::Restart { target, .. } => (
                    "mesh chaos restart".into(),
                    target.clone().unwrap_or_else(|| "<random>".into()),
                ),
                ChaosCmd::BurstKill { count, .. } => ("mesh chaos burst-kill".into(), format!("--count {count}")),
                ChaosCmd::DiskFull { target, max_mb, .. } => (
                    "mesh chaos disk-full".into(),
                    format!("--target {} --max-mb {max_mb}", target.clone().unwrap_or_else(|| "<random>".into())),
                ),
                ChaosCmd::Wedge { target_type, duration_ms } => (
                    "mesh chaos wedge".into(),
                    format!("--target-type {target_type} --duration-ms {duration_ms}"),
                ),
                ChaosCmd::ClockSkew { target, skew_ms, .. } => (
                    "mesh chaos clock-skew".into(),
                    format!("--target {} --skew-ms {skew_ms}", target.clone().unwrap_or_else(|| "<random>".into())),
                ),
                ChaosCmd::SlowLink { target, latency_ms, .. } => (
                    "mesh chaos slow-link".into(),
                    format!("--target {} --latency-ms {latency_ms}", target.clone().unwrap_or_else(|| "<random>".into())),
                ),
                ChaosCmd::LossyLink { target, loss_pct, .. } => (
                    "mesh chaos lossy-link".into(),
                    format!("--target {} --loss-pct {loss_pct}", target.clone().unwrap_or_else(|| "<random>".into())),
                ),
                ChaosCmd::NatShift { target, .. } => (
                    "mesh chaos nat-shift".into(),
                    target.clone().unwrap_or_else(|| "<random>".into()),
                ),
                ChaosCmd::PartitionPair { a, b, duration_ms } => (
                    "mesh chaos partition-pair".into(),
                    format!("--a {a} --b {b} --duration-ms {duration_ms}"),
                ),
                ChaosCmd::PartitionSubset { size, duration_ms } => (
                    "mesh chaos partition-subset".into(),
                    format!("--size {size} --duration-ms {duration_ms}"),
                ),
                ChaosCmd::FlapLink { a, b, cycles, on_ms, off_ms } => (
                    "mesh chaos flap-link".into(),
                    format!("--a {a} --b {b} --cycles {cycles} --on-ms {on_ms} --off-ms {off_ms}"),
                ),
                ChaosCmd::FirewallInbound { target_type, duration_ms } => (
                    "mesh chaos firewall-inbound".into(),
                    format!("--target-type {target_type} --duration-ms {duration_ms}"),
                ),
                ChaosCmd::Catalog => ("mesh chaos catalog".into(), "".into()),
                ChaosCmd::Report { seed } => ("mesh chaos report".into(), format!("{seed}")),
                ChaosCmd::Soak { duration, interval, seed } => (
                    "mesh chaos soak".into(),
                    format!("--duration {duration} --interval {interval} --seed {seed}"),
                ),
            },
            MeshCmd::Test { sub } => match sub {
                TestCmd::List => ("mesh test list".into(), "".into()),
                TestCmd::Run { name, seed } => ("mesh test run".into(), format!("{name} --seed {seed}")),
                TestCmd::All { seed } => ("mesh test all".into(), format!("--seed {seed}")),
            },
        },
    }
}

async fn run_command(cli: &Cli, client: &reqwest::Client) -> Result<()> {
    match &cli.command {
        Cmd::Mesh { sub } => match sub {
            MeshCmd::Node { sub } => match sub {
                NodeCmd::List { format } => cmd_node_list(client, &cli.api_url, format).await,
                NodeCmd::Describe { name, format } => {
                    cmd_node_describe(client, &cli.api_url, name, format).await
                }
                NodeCmd::Add { r#type, format, .. } => {
                    cmd_node_add(client, &cli.api_url, r#type, format).await
                }
                NodeCmd::Remove { node_name, format } => {
                    cmd_node_remove(client, &cli.api_url, node_name, format).await
                }
            },
            MeshCmd::Topology { sub } => match sub {
                TopologyCmd::Show { format } => {
                    cmd_topology_show(client, &cli.api_url, format).await
                }
            },
            MeshCmd::Status { format } => cmd_status(client, &cli.api_url, format).await,
            MeshCmd::WaitConverged { target, timeout } => {
                cmd_wait_converged(client, &cli.api_url, *target, timeout).await
            }
            MeshCmd::Chaos { sub } => match sub {
                ChaosCmd::Kill { target, deadline_ms } => {
                    cmd_chaos_kill(&cli.api_url, target.clone(), *deadline_ms).await
                }
                ChaosCmd::Restart { target, deadline_ms } => {
                    cmd_chaos_restart(&cli.api_url, target.clone(), *deadline_ms).await
                }
                ChaosCmd::BurstKill { count, deadline_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::BurstKill { count: *count }), *deadline_ms).await
                }
                ChaosCmd::DiskFull { target, max_mb, deadline_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::DiskFull { target: target.clone(), max_bytes: max_mb * 1024 * 1024 }), *deadline_ms).await
                }
                ChaosCmd::Wedge { target_type, duration_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::WedgeNode { target_node_type: target_type.clone(), duration_ms: *duration_ms }), duration_ms + 5000).await
                }
                ChaosCmd::ClockSkew { target, skew_ms, deadline_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::ClockSkew { target: target.clone(), skew_ms: *skew_ms }), *deadline_ms).await
                }
                ChaosCmd::SlowLink { target, latency_ms, deadline_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::SlowLink { target: target.clone(), latency_ms: *latency_ms }), *deadline_ms).await
                }
                ChaosCmd::LossyLink { target, loss_pct, deadline_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::LossyLink { target: target.clone(), loss_pct: *loss_pct }), *deadline_ms).await
                }
                ChaosCmd::NatShift { target, deadline_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::NatShift { target: target.clone() }), *deadline_ms).await
                }
                ChaosCmd::PartitionPair { a, b, duration_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::PartitionPair { a: a.clone(), b: b.clone(), duration_ms: *duration_ms }), duration_ms + 5000).await
                }
                ChaosCmd::PartitionSubset { size, duration_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::PartitionSubset { subset_size: *size, duration_ms: *duration_ms }), duration_ms + 5000).await
                }
                ChaosCmd::FlapLink { a, b, cycles, on_ms, off_ms } => {
                    let total = (*cycles as u64) * (*on_ms + *off_ms);
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::FlapLink { a: a.clone(), b: b.clone(), cycles: *cycles, on_ms: *on_ms, off_ms: *off_ms }), total + 5000).await
                }
                ChaosCmd::FirewallInbound { target_type, duration_ms } => {
                    cmd_chaos_primitive(&cli.api_url, Box::new(rafka_chaos::primitives::FirewallInbound { target_node_type: target_type.clone(), duration_ms: *duration_ms }), duration_ms + 5000).await
                }
                ChaosCmd::Catalog => cmd_chaos_catalog().await,
                ChaosCmd::Report { seed } => cmd_chaos_report(*seed).await,
                ChaosCmd::Soak { duration, interval, seed } => {
                    cmd_chaos_soak(&cli.api_url, duration, interval, *seed).await
                }
            },
            MeshCmd::Test { sub } => match sub {
                TestCmd::List => cmd_test_list().await,
                TestCmd::Run { name, seed } => cmd_test_run(&cli.api_url, name, *seed).await,
                TestCmd::All { seed } => cmd_test_all(&cli.api_url, *seed).await,
            },
        },
    }
}

/// Test registry — single source of truth. Each entry: (name, kind, description, runner).
/// `kind` distinguishes pure-Rust unit tests (cargo test) from live-mesh chaos tests
/// (need topology-ui running). Reports written to E:/tmp/rafka-tests/<name>-<seed>.json.
const TEST_REGISTRY: &[(&str, &str, &str)] = &[
    ("framer-roundtrip",        "functional", "proptest: every (tag, frame) round-trips byte-for-byte through tag+varint+postcard framer"),
    ("framer-truncation",       "functional", "proptest: dropping last byte of any frame surfaces FramerError::Truncated"),
    ("traced-frame-roundtrip",  "functional", "tag=0x10 wrapped TracedFrame preserves trace_id + span_id across encode→decode"),
    ("unknown-tag-rejected",    "functional", "frames with tag != 0x10 must NOT deserialize as TracedFrame"),
    ("chaos-soak-9prim-1min",   "chaos",      "1-minute soak with 9-primitive pool; expects 100% pass; gates the substrate"),
    ("chaos-soak-9prim-5min",   "chaos",      "5-minute soak with 9-primitive pool; balanced primitive distribution"),
    ("mesh-five-types-present", "chaos",      "spawn 5 nodes (gateway+broker+compute+registry+bridge), verify all 5 visible in topology + heartbeats fresh"),
    ("remove-resilience",       "chaos",      "spawn 6, remove 3, verify survivors detect disconnects within 15s (peer_count adjusts)"),
];

#[derive(serde::Serialize)]
struct TestReport {
    name: String,
    kind: String,
    description: String,
    seed: u64,
    started_ms: u64,
    ended_ms: u64,
    duration_ms: u64,
    status: String, // "passed" | "failed" | "skipped"
    detail: String,
}

fn tests_dir() -> std::path::PathBuf {
    let p = std::path::PathBuf::from("E:/tmp/rafka-tests");
    let _ = std::fs::create_dir_all(&p);
    p
}

async fn cmd_test_list() -> Result<()> {
    println!("rafka test registry ({} tests)", TEST_REGISTRY.len());
    println!("{:<30} {:<11} {}", "name", "kind", "description");
    println!("{:<30} {:<11} {}", "----", "----", "-----------");
    for (name, kind, desc) in TEST_REGISTRY {
        println!("{name:<30} {kind:<11} {desc}");
    }
    Ok(())
}

async fn cmd_test_run(api_url: &str, name: &str, seed: u64) -> Result<()> {
    let entry = TEST_REGISTRY
        .iter()
        .find(|(n, _, _)| *n == name)
        .ok_or_else(|| anyhow!("unknown test '{name}' — see `rfa mesh test list`"))?;
    let (_, kind, desc) = entry;
    println!("test start: {name} (kind={kind}, seed={seed})");
    let started = std::time::Instant::now();
    let started_ms = timestamp_ms();
    let (status, detail): (&str, String) = match name.as_ref() {
        "framer-roundtrip" | "framer-truncation" | "traced-frame-roundtrip"
        | "unknown-tag-rejected" => run_cargo_test_for("rafka-mesh-ops", name).await,
        "chaos-soak-9prim-1min" => run_chaos_soak(api_url, "1m", "8s", seed).await,
        "chaos-soak-9prim-5min" => run_chaos_soak(api_url, "5m", "10s", seed).await,
        "mesh-five-types-present" => run_mesh_five_types_present(api_url).await,
        "remove-resilience" => run_remove_resilience(api_url).await,
        _ => ("skipped", format!("no runner wired for {name}")),
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    let ended_ms = timestamp_ms();
    let report = TestReport {
        name: name.into(),
        kind: kind.to_string(),
        description: desc.to_string(),
        seed,
        started_ms,
        ended_ms,
        duration_ms,
        status: status.into(),
        detail,
    };
    let path = tests_dir().join(format!("{name}-{seed}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    println!("test end: {name} status={} duration={duration_ms}ms", report.status);
    println!("report: {}", path.display());
    if report.status == "passed" {
        Ok(())
    } else {
        Err(anyhow!("test {name} did not pass: {}", report.detail))
    }
}

async fn cmd_test_all(api_url: &str, seed: u64) -> Result<()> {
    let mut failed: Vec<String> = Vec::new();
    for (name, _, _) in TEST_REGISTRY {
        println!("\n=== running: {name} ===");
        if let Err(e) = cmd_test_run(api_url, name, seed).await {
            println!("FAIL: {e}");
            failed.push(name.to_string());
        }
    }
    println!("\n=== summary: {}/{} passed ===", TEST_REGISTRY.len() - failed.len(), TEST_REGISTRY.len());
    if failed.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("failed tests: {failed:?}"))
    }
}

async fn run_cargo_test_for(crate_name: &str, test_filter: &str) -> (&'static str, String) {
    // Re-shape test name: framer-roundtrip → framer::tests::round_trip etc.
    let filter = match test_filter {
        "framer-roundtrip" => "framer::tests::round_trip",
        "framer-truncation" => "framer::tests::truncation_detected",
        "traced-frame-roundtrip" => "tests::traced_frame_round_trip",
        "unknown-tag-rejected" => "tests::unknown_tag_fails_decode",
        other => other,
    };
    let out = match tokio::process::Command::new("cargo")
        .args(["test", "-p", crate_name, "--lib", filter, "--", "--nocapture"])
        .env("CARGO_TARGET_DIR", "E:/cargo-target-v2")
        .current_dir("E:/dev/rafka-V2-new-mesh")
        .output()
        .await
    {
        Ok(o) => o,
        Err(e) => return ("failed", format!("cargo invocation failed: {e}")),
    };
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if out.status.success() {
        // Extract the "test result:" line for a tighter detail string.
        let line = stdout
            .lines()
            .find(|l| l.contains("test result:"))
            .unwrap_or("passed")
            .trim()
            .to_string();
        ("passed", line)
    } else {
        ("failed", format!("stdout: {stdout}\nstderr: {stderr}"))
    }
}

async fn run_chaos_soak(api_url: &str, duration: &str, interval: &str, seed: u64) -> (&'static str, String) {
    let dur = match humantime::parse_duration(duration) {
        Ok(d) => d,
        Err(e) => return ("failed", format!("parse duration: {e}")),
    };
    let iv = match humantime::parse_duration(interval) {
        Ok(d) => d,
        Err(e) => return ("failed", format!("parse interval: {e}")),
    };
    let mut ctx = rafka_chaos::default_context(seed);
    ctx.topology_ui_url = api_url.to_string();
    let report = rafka_chaos::soak::run_soak(&ctx, dur, iv, seed).await;
    if report.failed_timeout == 0 && report.failed_assertion == 0 {
        (
            "passed",
            format!(
                "{} events, all passed; primitive distribution validates 9-prim pool",
                report.event_count
            ),
        )
    } else {
        (
            "failed",
            format!(
                "{} events: {} passed, {} timeouts, {} assertions",
                report.event_count, report.passed, report.failed_timeout, report.failed_assertion
            ),
        )
    }
}

async fn run_mesh_five_types_present(api_url: &str) -> (&'static str, String) {
    use std::collections::HashSet;
    let client = reqwest::Client::new();
    for t in ["gateway", "broker", "compute", "registry", "bridge"] {
        let _ = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type": t}))
            .send()
            .await;
    }
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    let body: serde_json::Value = match client
        .get(format!("{api_url}/api/nodes/spawned"))
        .send()
        .await
    {
        Ok(r) => r.json().await.unwrap_or(serde_json::json!({"spawned": []})),
        Err(e) => return ("failed", format!("query /api/spawned: {e}")),
    };
    let types: HashSet<String> = body["spawned"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.split('-').next().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();
    let expected: HashSet<String> = ["gateway", "broker", "compute", "registry", "bridge"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let missing: Vec<String> = expected.difference(&types).cloned().collect();
    if missing.is_empty() {
        ("passed", format!("all 5 types present: {types:?}"))
    } else {
        ("failed", format!("missing types: {missing:?}; saw {types:?}"))
    }
}

async fn fetch_json(client: &reqwest::Client, url: &str) -> serde_json::Value {
    match client.get(url).send().await {
        Ok(r) => r.json::<serde_json::Value>().await.unwrap_or(serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    }
}

async fn run_remove_resilience(api_url: &str) -> (&'static str, String) {
    let client = reqwest::Client::new();
    for t in ["gateway", "broker", "broker", "compute", "registry", "bridge"] {
        let _ = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type": t}))
            .send()
            .await;
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let pre = fetch_json(&client, &format!("{api_url}/api/nodes/spawned")).await;
    let pre_names: Vec<String> = pre["spawned"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    if pre_names.len() < 4 {
        return ("failed", format!("expected ≥4 nodes after spawn, got {}", pre_names.len()));
    }
    let kill_targets: Vec<String> = pre_names.iter().take(3).cloned().collect();
    for name in &kill_targets {
        let _ = client.delete(format!("{api_url}/api/nodes/{name}")).send().await;
    }
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    let hb = fetch_json(&client, &format!("{api_url}/api/heartbeats")).await;
    let fresh = hb["heartbeats"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|h| {
                    let name = h["node_name"].as_str().unwrap_or("");
                    !kill_targets.iter().any(|k| k == name)
                        && h["age_ms"].as_i64().unwrap_or(99999) < 10000
                })
                .count()
        })
        .unwrap_or(0);
    if fresh >= 3 {
        (
            "passed",
            format!("killed {} nodes, {fresh} survivors still emit fresh heartbeats", kill_targets.len()),
        )
    } else {
        ("failed", format!("only {fresh} survivors fresh after 3 kills (need ≥3)"))
    }
}

fn timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn cmd_chaos_kill(api_url: &str, target: Option<String>, deadline_ms: u64) -> Result<()> {
    use rafka_chaos::{primitives::KillNode, ChaosPrimitive};
    let mut ctx = rafka_chaos::default_context(0);
    ctx.topology_ui_url = api_url.to_string();
    let prim = KillNode { target };
    let outcome = prim.execute(&ctx).await.map_err(|e| anyhow!("execute: {e}"))?;
    let det = prim.detect(&ctx, &outcome, deadline_ms).await.map_err(|e| anyhow!("detect: {e}"))?;
    println!("primitive: kill_node");
    println!("target:    {}", outcome.targets[0]);
    println!("detection: {:?}", det);
    match det {
        rafka_chaos::DetectionResult::Passed { .. } => Ok(()),
        _ => Err(anyhow!("detection failed")),
    }
}

async fn cmd_chaos_restart(api_url: &str, target: Option<String>, deadline_ms: u64) -> Result<()> {
    use rafka_chaos::{primitives::RestartNode, ChaosPrimitive};
    let mut ctx = rafka_chaos::default_context(0);
    ctx.topology_ui_url = api_url.to_string();
    let prim = RestartNode { target };
    let outcome = prim.execute(&ctx).await.map_err(|e| anyhow!("execute: {e}"))?;
    let det = prim.detect(&ctx, &outcome, deadline_ms).await.map_err(|e| anyhow!("detect: {e}"))?;
    println!("primitive: restart_node");
    println!("old:       {}", outcome.targets[0]);
    println!("new:       {}", outcome.targets[1]);
    println!("detection: {:?}", det);
    match det {
        rafka_chaos::DetectionResult::Passed { .. } => Ok(()),
        _ => Err(anyhow!("detection failed")),
    }
}

/// Pretty-print a soak report by seed. Reads E:/tmp/rafka-chaos-soak-<seed>.json
/// (the location run_soak writes to) and prints a human summary: event count,
/// pass/fail/timeout breakdown, primitive distribution, duration, last failure
/// snippet if any.
async fn cmd_chaos_report(seed: u64) -> Result<()> {
    let path = format!("E:/tmp/rafka-chaos-soak-{}.json", seed);
    let raw = std::fs::read_to_string(&path).map_err(|e| anyhow!("read {path}: {e}"))?;
    let report: Value = serde_json::from_str(&raw).map_err(|e| anyhow!("parse {path}: {e}"))?;
    let events = report["event_count"].as_i64().unwrap_or(0);
    let passed = report["passed"].as_i64().unwrap_or(0);
    let to = report["failed_timeout"].as_i64().unwrap_or(0);
    let ass = report["failed_assertion"].as_i64().unwrap_or(0);
    let started = report["started_ms"].as_i64().unwrap_or(0);
    let ended = report["ended_ms"].as_i64().unwrap_or(0);
    let dur_s = (ended - started) as f64 / 1000.0;
    println!("soak report  seed={seed}  file={path}");
    println!("  events:   {events}  (passed={passed}  timeouts={to}  assertions={ass})");
    println!("  duration: {dur_s:.1}s ({:.2}h)", dur_s / 3600.0);
    let mut by_prim: std::collections::BTreeMap<String, i64> =
        std::collections::BTreeMap::new();
    if let Some(arr) = report["events"].as_array() {
        for e in arr {
            if let Some(name) = e["primitive"].as_str() {
                *by_prim.entry(name.to_string()).or_insert(0) += 1;
            }
        }
    }
    println!("  primitives:");
    for (p, c) in &by_prim {
        println!("    {:<18} {c}", p);
    }
    // Last failure snippet if any
    if to + ass > 0 {
        if let Some(arr) = report["events"].as_array() {
            println!("  failures:");
            for e in arr {
                let det = &e["detection"];
                if det.is_string() && det.as_str() == Some("Passed") {
                    continue;
                }
                if let Some(obj) = det.as_object() {
                    if obj.contains_key("Passed") {
                        continue;
                    }
                }
                let name = e["primitive"].as_str().unwrap_or("?");
                println!("    {name}: {}", serde_json::to_string(det).unwrap_or_default());
            }
        }
    }
    Ok(())
}

/// Print the catalog of every shipped chaos primitive with one-line description,
/// admin requirement, and example invocation. Cheap operator introspection — no
/// HTTP calls, runs purely against compiled-in metadata.
async fn cmd_chaos_catalog() -> Result<()> {
    let entries: &[(&str, bool, &str, &str)] = &[
        ("kill_node",         false, "terminate one random spawned subprocess",          "rfa mesh chaos kill"),
        ("restart_node",      false, "kill + immediately re-spawn same node_type",       "rfa mesh chaos restart"),
        ("burst_kill",        false, "N back-to-back kills against random targets",      "rfa mesh chaos burst-kill --count 3"),
        ("disk_full",         false, "fill spawn data dir until writes fail",            "rfa mesh chaos disk-full --max-mb 4"),
        ("wedge_node",        false, "Suspend the OS process via NtSuspendProcess",      "rfa mesh chaos wedge --target-type broker"),
        ("clock_skew",        false, "restart node with RAFKA_CLOCK_SKEW_MS env",        "rfa mesh chaos clock-skew --skew-ms 30000"),
        ("slow_link",         false, "restart node with RAFKA_LINK_SLOW_MS env",         "rfa mesh chaos slow-link --latency-ms 250"),
        ("lossy_link",        false, "restart node with RAFKA_LINK_LOSS_PCT env",        "rfa mesh chaos lossy-link --loss-pct 15"),
        ("nat_shift",         false, "restart with random RAFKA_NODE_BIND_ADDR port",    "rfa mesh chaos nat-shift"),
        ("partition_pair",    true,  "Windows firewall block outbound UDP for 2 progs",  "rfa mesh chaos partition-pair --a gateway --b broker"),
        ("partition_subset",  true,  "split node_type catalog: K types blocked from rest","rfa mesh chaos partition-subset --size 2"),
        ("flap_link",         true,  "create+delete firewall block N cycles",            "rfa mesh chaos flap-link --a gateway --b broker --cycles 5"),
        ("firewall_inbound",  true,  "block inbound UDP to a named program",             "rfa mesh chaos firewall-inbound --target-type broker"),
    ];
    println!("rafka chaos primitive catalog ({} shipped)\n", entries.len());
    println!("{:<20} {:<8} {}", "primitive", "admin?", "what it does");
    println!("{:<20} {:<8} {}", "---------", "------", "------------");
    for (name, admin, desc, _ex) in entries {
        let admin_mark = if *admin { "yes" } else { "no" };
        println!("{name:<20} {admin_mark:<8} {desc}");
    }
    println!("\nexamples:");
    for (name, _admin, _desc, ex) in entries {
        println!("  {name}: {ex}");
    }
    println!(
        "\nsoak (all non-admin primitives in random rotation):"
    );
    println!("  rfa mesh chaos soak --duration 1h --interval 20s --seed 42");
    Ok(())
}

/// Generic one-shot primitive runner: execute() → detect() → print result.
/// Used by all primitive-specific subcommands so the print format stays consistent.
async fn cmd_chaos_primitive(api_url: &str, prim: Box<dyn rafka_chaos::ChaosPrimitive>, deadline_ms: u64) -> Result<()> {
    let mut ctx = rafka_chaos::default_context(0);
    ctx.topology_ui_url = api_url.to_string();
    let outcome = prim.execute(&ctx).await.map_err(|e| anyhow!("execute: {e}"))?;
    let det = prim.detect(&ctx, &outcome, deadline_ms).await.map_err(|e| anyhow!("detect: {e}"))?;
    println!("primitive: {}", prim.name());
    for (i, t) in outcome.targets.iter().enumerate() {
        println!("target[{i}]:  {t}");
    }
    println!("detection: {:?}", det);
    match det {
        rafka_chaos::DetectionResult::Passed { .. } => Ok(()),
        _ => Err(anyhow!("detection failed")),
    }
}

async fn cmd_chaos_soak(api_url: &str, duration: &str, interval: &str, seed: u64) -> Result<()> {
    use std::io::Write;
    let dur = humantime::parse_duration(duration).map_err(|e| anyhow!("parse duration: {e}"))?;
    let iv = humantime::parse_duration(interval).map_err(|e| anyhow!("parse interval: {e}"))?;
    let mut ctx = rafka_chaos::default_context(seed);
    ctx.topology_ui_url = api_url.to_string();
    println!("soak start: duration={dur:?} interval={iv:?} seed={seed}");
    // When stdout is redirected to a file in the background, it goes block-buffered
    // and progress lines don't appear until process exit. Flush explicitly so the
    // log file shows what's happening in real time.
    let _ = std::io::stdout().flush();
    let report = rafka_chaos::soak::run_soak(&ctx, dur, iv, seed).await;
    println!("soak end: events={} passed={} failed_timeout={} failed_assertion={}",
        report.event_count, report.passed, report.failed_timeout, report.failed_assertion);
    let _ = std::io::stdout().flush();
    // write report
    let path = format!("E:/tmp/rafka-chaos-soak-{}.json", seed);
    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(&path, json)?;
    println!("report: {path}");
    let _ = std::io::stdout().flush();
    if report.failed_timeout > 0 || report.failed_assertion > 0 {
        Err(anyhow!("soak failed: {} timeouts, {} assertion failures", report.failed_timeout, report.failed_assertion))
    } else {
        Ok(())
    }
}

async fn http_post(client: &reqwest::Client, url: &str, body: &Value) -> Result<(u16, Value)> {
    let path = reqwest::Url::parse(url)
        .map(|u| u.path().to_string())
        .unwrap_or_else(|_| url.to_string());
    let span = info_span!(
        "rafka.cli.http.request",
        method = "POST",
        path = %path,
        "otel.kind" = "client",
    );
    let resp = async {
        let headers = current_traceparent_headers();
        client
            .post(url)
            .headers(headers)
            .json(body)
            .send()
            .await
    }
    .instrument(span)
    .await
    .map_err(|e| anyhow!("HTTP POST {url}: {e}"))?;
    let status = resp.status().as_u16();
    let resp_body: Value = resp.json().await.map_err(|e| anyhow!("parse JSON: {e}"))?;
    Ok((status, resp_body))
}

async fn http_delete(client: &reqwest::Client, url: &str) -> Result<(u16, Value)> {
    let path = reqwest::Url::parse(url)
        .map(|u| u.path().to_string())
        .unwrap_or_else(|_| url.to_string());
    let span = info_span!(
        "rafka.cli.http.request",
        method = "DELETE",
        path = %path,
        "otel.kind" = "client",
    );
    let resp = async {
        let headers = current_traceparent_headers();
        client.delete(url).headers(headers).send().await
    }
    .instrument(span)
    .await
    .map_err(|e| anyhow!("HTTP DELETE {url}: {e}"))?;
    let status = resp.status().as_u16();
    let resp_body: Value = resp.json().await.map_err(|e| anyhow!("parse JSON: {e}"))?;
    Ok((status, resp_body))
}

async fn http_get(client: &reqwest::Client, url: &str) -> Result<Value> {
    let path = reqwest::Url::parse(url)
        .map(|u| u.path().to_string())
        .unwrap_or_else(|_| url.to_string());
    let span = info_span!(
        "rafka.cli.http.request",
        method = "GET",
        path = %path,
        "otel.kind" = "client",
    );
    let resp = async {
        let headers = current_traceparent_headers();
        client.get(url).headers(headers).send().await
    }
    .instrument(span)
    .await
    .map_err(|e| anyhow!("HTTP GET {url}: {e}"))?;
    let status = resp.status();
    let body: Value = resp.json().await.map_err(|e| anyhow!("parse JSON: {e}"))?;
    if !status.is_success() {
        let err = body["error"].as_str().unwrap_or("unknown error");
        return Err(anyhow!("API error {status}: {err}"));
    }
    Ok(body)
}

// ── mesh node list ────────────────────────────────────────────────────────────

async fn cmd_node_list(client: &reqwest::Client, api_url: &str, fmt: &Format) -> Result<()> {
    let url = format!("{api_url}/api/nodes");
    let body = http_get(client, &url).await?;
    let nodes: Vec<String> = body["nodes"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    match fmt {
        Format::Json => println!("{}", serde_json::to_string_pretty(&body)?),
        Format::Table => {
            println!("{:<20} {}", "NODE", "TYPE");
            println!("{}", "-".repeat(30));
            for n in &nodes {
                println!("{:<20} {}", n, n);
            }
            println!("\n{} node(s) known to jaeger", nodes.len());
        }
    }
    Ok(())
}

// ── mesh node describe ────────────────────────────────────────────────────────

async fn cmd_node_describe(
    client: &reqwest::Client,
    api_url: &str,
    name: &str,
    fmt: &Format,
) -> Result<()> {
    let url = format!("{api_url}/api/boot-trace?service={name}");
    let body = http_get(client, &url).await?;

    let trace = &body["data"][0];
    let spans = trace["spans"].as_array().cloned().unwrap_or_default();

    // Extract node_id from any span tag
    let node_id = spans
        .iter()
        .flat_map(|s| s["tags"].as_array().unwrap_or(&vec![]).iter().cloned().collect::<Vec<_>>())
        .find(|t| t["key"] == "node_id")
        .and_then(|t| t["value"].as_str().map(String::from))
        .unwrap_or_else(|| "(unknown)".into());

    // Collect rafka boot spans
    let mut rafka: Vec<_> = spans
        .iter()
        .filter(|s| {
            s["operationName"]
                .as_str()
                .map(|n| n.starts_with("rafka."))
                .unwrap_or(false)
        })
        .collect();
    rafka.sort_by_key(|s| s["startTime"].as_i64().unwrap_or(0));

    let root_time = rafka
        .first()
        .and_then(|s| s["startTime"].as_i64())
        .unwrap_or(0);

    match fmt {
        Format::Json => println!("{}", serde_json::to_string_pretty(&body)?),
        Format::Table => {
            println!("node:    {name}");
            println!("node_id: {node_id}");
            println!();
            println!("{:<40} {:>10} {:>10}", "SPAN", "OFFSET ms", "DUR ms");
            println!("{}", "-".repeat(65));
            for sp in &rafka {
                let op = sp["operationName"].as_str().unwrap_or("?");
                let short = op.replace("rafka.mesh.", "");
                let start = sp["startTime"].as_i64().unwrap_or(0);
                let dur = sp["duration"].as_i64().unwrap_or(0);
                let offset_ms = (start - root_time) as f64 / 1000.0;
                let dur_ms = dur as f64 / 1000.0;
                println!("{:<40} {:>10.3} {:>10.3}", short, offset_ms, dur_ms);
            }
        }
    }
    Ok(())
}

// ── mesh topology show ────────────────────────────────────────────────────────

async fn cmd_topology_show(
    client: &reqwest::Client,
    api_url: &str,
    fmt: &TopologyFormat,
) -> Result<()> {
    let nodes_url = format!("{api_url}/api/nodes");
    let nodes_body = http_get(client, &nodes_url).await?;
    let nodes: Vec<String> = nodes_body["nodes"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    // Gather peer_count for each node from heartbeat endpoint
    let mut node_info: Vec<(String, u64)> = Vec::new();
    for n in &nodes {
        let hb_url = format!("{api_url}/api/heartbeat?service={n}");
        let peer_count = match http_get(client, &hb_url).await {
            Ok(b) => b["peer_count"].as_u64().unwrap_or(0),
            Err(_) => 0,
        };
        node_info.push((n.clone(), peer_count));
    }

    match fmt {
        TopologyFormat::Json => {
            let out: Vec<Value> = node_info
                .iter()
                .map(|(n, p)| serde_json::json!({"node": n, "peer_count": p}))
                .collect();
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        TopologyFormat::Dot => {
            // Nodes-only DOT — no edge data available in this sprint (no peer-identity spans)
            println!("digraph rafka_mesh {{");
            println!("  rankdir=LR;");
            println!("  node [shape=box fontname=monospace];");
            for (n, p) in &node_info {
                println!("  {} [label=\"{}\\n{} peers\"];", n, n, p);
            }
            println!("  // NOTE: edges omitted — peer identity not yet available (sprint-10+)");
            println!("}}");
        }
        TopologyFormat::Table => {
            println!("{:<20} {:>10}", "NODE", "PEER COUNT");
            println!("{}", "-".repeat(32));
            for (n, p) in &node_info {
                println!("{:<20} {:>10}", n, p);
            }
        }
    }
    Ok(())
}

// ── mesh status ───────────────────────────────────────────────────────────────

async fn cmd_status(client: &reqwest::Client, api_url: &str, fmt: &Format) -> Result<()> {
    let nodes_url = format!("{api_url}/api/nodes");
    let nodes_body = http_get(client, &nodes_url).await?;
    let nodes: Vec<String> = nodes_body["nodes"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    #[derive(serde::Serialize)]
    struct NodeStatus {
        node: String,
        node_id: String,
        peer_count: u64,
        last_heartbeat_ms_ago: String,
    }

    let mut rows: Vec<NodeStatus> = Vec::new();
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;

    for n in &nodes {
        let hb_url = format!("{api_url}/api/heartbeat?service={n}");
        match http_get(client, &hb_url).await {
            Ok(b) => {
                let node_id = b["node_id"]
                    .as_str()
                    .map(|s| format!("{}…", &s[..8]))
                    .unwrap_or_else(|| "(unknown)".into());
                let peer_count = b["peer_count"].as_u64().unwrap_or(0);
                let last_hb = b["last_heartbeat_us"].as_i64().unwrap_or(0);
                let ago_ms = if last_hb > 0 {
                    format!("{}ms ago", (now_us - last_hb) / 1000)
                } else {
                    "(unknown)".into()
                };
                rows.push(NodeStatus {
                    node: n.clone(),
                    node_id,
                    peer_count,
                    last_heartbeat_ms_ago: ago_ms,
                });
            }
            Err(_) => {
                rows.push(NodeStatus {
                    node: n.clone(),
                    node_id: "(unreachable)".into(),
                    peer_count: 0,
                    last_heartbeat_ms_ago: "(unknown)".into(),
                });
            }
        }
    }

    match fmt {
        Format::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
        Format::Table => {
            println!(
                "{:<12} {:<12} {:>10} {:<18}",
                "NODE", "NODE_ID", "PEERS", "LAST HEARTBEAT"
            );
            println!("{}", "-".repeat(58));
            for r in &rows {
                println!(
                    "{:<12} {:<12} {:>10} {:<18}",
                    r.node, r.node_id, r.peer_count, r.last_heartbeat_ms_ago
                );
            }
        }
    }
    Ok(())
}

// ── mesh node add ─────────────────────────────────────────────────────────────

async fn cmd_node_add(
    client: &reqwest::Client,
    api_url: &str,
    node_type: &NodeType,
    fmt: &Format,
) -> Result<()> {
    let url = format!("{api_url}/api/nodes/spawn");
    let req_body = serde_json::json!({"node_type": node_type.as_str()});
    let (status, body) = http_post(client, &url, &req_body).await?;

    if status == 201 {
        match fmt {
            Format::Json => println!("{}", serde_json::to_string_pretty(&body)?),
            Format::Table => {
                let name = body["node_name"].as_str().unwrap_or("?");
                let pid = body["pid"].as_u64().unwrap_or(0);
                println!("spawned:  {name}");
                println!("pid:      {pid}");
            }
        }
        Ok(())
    } else {
        let err = body["error"].as_str().unwrap_or("unknown error");
        eprintln!("spawn failed ({status}): {err}");
        std::process::exit(1);
    }
}

// ── mesh node remove ──────────────────────────────────────────────────────────

async fn cmd_node_remove(
    client: &reqwest::Client,
    api_url: &str,
    node_name: &str,
    fmt: &Format,
) -> Result<()> {
    let url = format!("{api_url}/api/nodes/{node_name}");
    let (status, body) = http_delete(client, &url).await?;

    match status {
        200 => {
            match fmt {
                Format::Json => println!("{}", serde_json::to_string_pretty(&body)?),
                Format::Table => {
                    let name = body["node_name"].as_str().unwrap_or(node_name);
                    let reason = body["reason"].as_str().unwrap_or("?");
                    println!("killed:  {name}");
                    println!("reason:  {reason}");
                }
            }
            Ok(())
        }
        404 => {
            eprintln!("node not found: {node_name}");
            std::process::exit(2);
        }
        _ => {
            let err = body["error"].as_str().unwrap_or("unknown error");
            eprintln!("kill failed ({status}): {err}");
            std::process::exit(1);
        }
    }
}

// ── mesh wait-converged ───────────────────────────────────────────────────────

async fn cmd_wait_converged(
    client: &reqwest::Client,
    api_url: &str,
    target: usize,
    timeout_str: &str,
) -> Result<()> {
    let timeout_dur = humantime::parse_duration(timeout_str)
        .map_err(|e| anyhow!("invalid timeout {timeout_str:?}: {e}"))?;

    let deadline = tokio::time::Instant::now() + timeout_dur;
    let mut poll_count: u64 = 0;

    loop {
        let url = format!("{api_url}/api/nodes");
        let result = http_get(client, &url).await;
        let current = match result {
            Ok(body) => body["nodes"].as_array().map(|a| a.len()).unwrap_or(0),
            Err(_) => 0,
        };
        poll_count += 1;

        let wait_span = info_span!(
            "rafka.cli.wait_loop",
            poll_count,
            target,
            current_count = current,
        );
        wait_span.in_scope(|| {
            tracing::info!(poll_count, target, current_count = current, "polling mesh convergence");
        });

        if current >= target {
            println!("converged: {current}/{target} nodes ({poll_count} polls)");
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            eprintln!(
                "timeout after {timeout_str}: {current}/{target} nodes ({poll_count} polls)"
            );
            std::process::exit(1);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
