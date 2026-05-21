use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;
use std::time::Duration;
use tracing::{info_span, Instrument};

#[derive(Parser)]
#[command(name = "rfa", about = "rafka CLI — talks to admin-ui REST API")]
struct Cli {
    /// admin-ui base URL
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
/// receiving server (admin-ui) can chain its spans under our trace_id.
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
/// (need admin-ui running). Reports written to E:/tmp/rafka-tests/<name>-<seed>.json.
const TEST_REGISTRY: &[(&str, &str, &str)] = &[
    ("framer-roundtrip",        "functional", "proptest: every (tag, frame) round-trips byte-for-byte through tag+varint+postcard framer"),
    ("framer-truncation",       "functional", "proptest: dropping last byte of any frame surfaces FramerError::Truncated"),
    ("traced-frame-roundtrip",  "functional", "tag=0x10 wrapped TracedFrame preserves trace_id + span_id across encode→decode"),
    ("unknown-tag-rejected",    "functional", "frames with tag != 0x10 must NOT deserialize as TracedFrame"),
    ("bi-stream-echo",          "functional", "two in-process iroh endpoints exchange a tag=0x11 framed payload over a bidirectional QUIC stream; payload survives byte-for-byte"),
    ("backpressure-stream-flood", "chaos",    "32 concurrent bi-streams flood 1 KiB payloads for 10s; passes if 0 errors AND >= 200 round-trips (proves bi-stream plane back-pressures smoothly without OOM or stall)"),
    ("chaos-soak-9prim-1min",   "chaos",      "1-minute soak with 9-primitive pool; expects 100% pass; gates the substrate"),
    ("chaos-soak-9prim-5min",   "chaos",      "5-minute soak with 9-primitive pool; balanced primitive distribution"),
    ("mesh-five-types-present", "chaos",      "spawn 5 nodes (gateway+broker+compute+registry+bridge), verify all 5 visible in topology + heartbeats fresh"),
    ("remove-resilience",       "chaos",      "spawn 6, remove 3, verify survivors detect disconnects within 15s (peer_count adjusts)"),
    ("gossip-swarm-forms",      "chaos",      "spawn 4 nodes, wait, verify rafka.mesh.gossip.received spans exist (peers exchanging digests via iroh-gossip swarm)"),
    ("gossip-mesh-to-mesh",     "chaos",      "spawn nodes in mesh-A + mesh-B; verify each mesh's gossip stays isolated (separate topic_id per mesh_id) AND cross.peer_connected spans fire"),
    // === Single-primitive chaos tests ===
    ("kill-broker",             "chaos",      "KillNode targeting a random broker; verify subprocess removed within 30s"),
    ("kill-gateway",            "chaos",      "KillNode targeting a random gateway; verify removed within 30s"),
    ("kill-compute",            "chaos",      "KillNode targeting a random compute; verify removed within 30s"),
    ("kill-registry",           "chaos",      "KillNode targeting a random registry; verify removed within 30s"),
    ("restart-broker",          "chaos",      "RestartNode broker (kill + respawn same type); verify new node_name appears within 30s"),
    ("restart-gateway",         "chaos",      "RestartNode gateway; verify new node_name appears within 30s"),
    ("burst-kill-3",            "chaos",      "BurstKill 3 random nodes back-to-back; verify all removed within 30s (substrate race)"),
    ("burst-kill-5",            "chaos",      "BurstKill 5 random nodes back-to-back; verify all removed within 30s"),
    ("wedge-broker-2s",         "chaos",      "WedgeNode broker for 2s (NtSuspendProcess); peer_count drops then recovers"),
    ("wedge-gateway-5s",        "chaos",      "WedgeNode gateway for 5s; longer wedge tests stale-peer expiry"),
    ("clock-skew-5s",           "chaos",      "ClockSkew target node by +5s; restart with RAFKA_CLOCK_SKEW_MS=5000"),
    ("clock-skew-60s",          "chaos",      "ClockSkew target node by +60s; bigger skew stresses heartbeat staleness logic"),
    ("slow-link-100ms",         "chaos",      "SlowLink: restart node with RAFKA_LINK_SLOW_MS=100; verify ping spans show latency"),
    ("slow-link-500ms",         "chaos",      "SlowLink: restart node with RAFKA_LINK_SLOW_MS=500; aggressive latency injection"),
    ("lossy-link-10pct",        "chaos",      "LossyLink: restart node with RAFKA_LINK_LOSS_PCT=10; expect rafka.mesh.frame.dropped_by_fault_inject spans"),
    ("lossy-link-25pct",        "chaos",      "LossyLink: restart node with RAFKA_LINK_LOSS_PCT=25; verify mesh resilience to packet loss"),
    ("nat-shift",               "chaos",      "NatShift: restart target with new random RAFKA_NODE_BIND_ADDR (simulates NAT rebind)"),
    // === Soak variants of increasing duration ===
    ("chaos-soak-9prim-2min",   "chaos",      "2-minute soak with 9-primitive pool; medium-duration substrate check"),
    ("chaos-soak-9prim-10min",  "chaos",      "10-minute soak with 9-primitive pool; long-duration steady-state"),
    ("chaos-soak-9prim-30min",  "chaos",      "30-minute soak with 9-primitive pool; deep substrate validation"),
    // === Mesh shape tests ===
    ("mesh-grow-shrink",        "chaos",      "spawn 10 extra brokers above bootstrap pool, kill them all in sequence, verify pool returns to baseline"),
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
        "bi-stream-echo" => run_cargo_test_for("rafka-node-base", "bi_stream_echo_e2e").await,
        "backpressure-stream-flood" => run_cargo_test_for("rafka-node-base", "backpressure_bi_stream_flood").await,
        "chaos-soak-9prim-1min" => run_chaos_soak(api_url, "1m", "8s", seed).await,
        "chaos-soak-9prim-5min" => run_chaos_soak(api_url, "5m", "10s", seed).await,
        "mesh-five-types-present" => run_mesh_five_types_present(api_url).await,
        "remove-resilience" => run_remove_resilience(api_url).await,
        "gossip-swarm-forms" => run_gossip_swarm_forms(api_url).await,
        "gossip-mesh-to-mesh" => run_gossip_mesh_to_mesh(api_url).await,
        // New single-primitive chaos tests
        "kill-broker"     => run_one_primitive(api_url, "kill", Some("broker".into())).await,
        "kill-gateway"    => run_one_primitive(api_url, "kill", Some("gateway".into())).await,
        "kill-compute"    => run_one_primitive(api_url, "kill", Some("compute".into())).await,
        "kill-registry"   => run_one_primitive(api_url, "kill", Some("registry".into())).await,
        "restart-broker"  => run_one_primitive(api_url, "restart", Some("broker".into())).await,
        "restart-gateway" => run_one_primitive(api_url, "restart", Some("gateway".into())).await,
        "burst-kill-3"    => run_one_primitive(api_url, "burst_kill_3", None).await,
        "burst-kill-5"    => run_one_primitive(api_url, "burst_kill_5", None).await,
        "wedge-broker-2s" => run_one_primitive(api_url, "wedge_broker_2000", None).await,
        "wedge-gateway-5s" => run_one_primitive(api_url, "wedge_gateway_5000", None).await,
        "clock-skew-5s"   => run_one_primitive(api_url, "clock_skew_5000", None).await,
        "clock-skew-60s"  => run_one_primitive(api_url, "clock_skew_60000", None).await,
        "slow-link-100ms" => run_one_primitive(api_url, "slow_link_100", None).await,
        "slow-link-500ms" => run_one_primitive(api_url, "slow_link_500", None).await,
        "lossy-link-10pct" => run_one_primitive(api_url, "lossy_link_10", None).await,
        "lossy-link-25pct" => run_one_primitive(api_url, "lossy_link_25", None).await,
        "nat-shift"       => run_one_primitive(api_url, "nat_shift", None).await,
        "chaos-soak-9prim-2min"  => run_chaos_soak(api_url, "2m", "8s", seed).await,
        "chaos-soak-9prim-10min" => run_chaos_soak(api_url, "10m", "10s", seed).await,
        "chaos-soak-9prim-30min" => run_chaos_soak(api_url, "30m", "15s", seed).await,
        "mesh-grow-shrink"  => run_mesh_grow_shrink(api_url).await,
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
    let http = reqwest::Client::new();
    // Pool floor — re-bootstrap if the cluster drops below this many nodes
    // between tests. Many chaos primitives need >=4 live nodes to target.
    const POOL_FLOOR: i64 = 6;
    for (name, kind, _) in TEST_REGISTRY {
        // Functional tests run cargo tests in isolation — they don't depend
        // on the live mesh pool. Skip the refill check.
        if *kind == "chaos" {
            if let Ok(r) = http.get(format!("{api_url}/api/cluster/summary")).send().await {
                if let Ok(body) = r.json::<serde_json::Value>().await {
                    let n = body["spawned"].as_i64().unwrap_or(0);
                    if n < POOL_FLOOR {
                        println!("=== pool ({n}) below floor ({POOL_FLOOR}) — re-bootstrapping ===");
                        let _ = http
                            .post(format!("{api_url}/api/bootstrap"))
                            .send()
                            .await;
                        tokio::time::sleep(std::time::Duration::from_secs(8)).await;
                    }
                }
            }
        }
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
        "bi-stream-echo" => "tests::bi_stream_echo_e2e",
        "backpressure-stream-flood" => "tests::backpressure_bi_stream_flood",
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

/// Generic single-primitive runner. Dispatches by `kind` to the matching
/// rafka_chaos primitive, executes it, waits for detection within 30s.
/// `target_type_filter` (when Some) narrows the random target selection to
/// node names starting with that prefix.
async fn run_one_primitive(
    api_url: &str,
    kind: &str,
    target_type_filter: Option<String>,
) -> (&'static str, String) {
    use rafka_chaos::{
        primitives::{
            BurstKill, ClockSkew, KillNode, LossyLink, NatShift, RestartNode, SlowLink, WedgeNode,
        },
        ChaosPrimitive, DetectionResult,
    };
    let mut ctx = rafka_chaos::default_context(0);
    ctx.topology_ui_url = api_url.to_string();
    let deadline_ms = 30_000u64;

    // For "*_<type>" tests, pick a real spawned node of that type up front so
    // KillNode/RestartNode operate on a concrete name instead of guessing.
    let target = if let Some(t) = &target_type_filter {
        match pick_node_of_type(api_url, t).await {
            Some(n) => Some(n),
            None => return ("failed", format!("no spawned node of type {t} to target")),
        }
    } else {
        None
    };

    let outcome_result: Result<rafka_chaos::ChaosOutcome, rafka_chaos::ChaosError>;
    let detect_kind = kind.to_string();
    // Dispatch by primitive label
    let det = match kind {
        "kill" => {
            let p = KillNode { target: target.clone() };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        "restart" => {
            let p = RestartNode { target: target.clone() };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        "burst_kill_3" | "burst_kill_5" => {
            let count: usize = if kind.ends_with('5') { 5 } else { 3 };
            let p = BurstKill { count };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        s if s.starts_with("wedge_") => {
            let target_type = if s.contains("broker") { "broker" } else if s.contains("gateway") { "gateway" } else { "compute" };
            let duration_ms: u64 = s.rsplit('_').next().and_then(|n| n.parse().ok()).unwrap_or(2000);
            let p = WedgeNode { target_node_type: target_type.to_string(), duration_ms };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        s if s.starts_with("clock_skew_") => {
            let skew_ms: i64 = s.rsplit('_').next().and_then(|n| n.parse().ok()).unwrap_or(5000);
            let p = ClockSkew { target: None, skew_ms };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        s if s.starts_with("slow_link_") => {
            let latency_ms: u64 = s.rsplit('_').next().and_then(|n| n.parse().ok()).unwrap_or(100);
            let p = SlowLink { target: None, latency_ms };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        s if s.starts_with("lossy_link_") => {
            let pct: u8 = s.rsplit('_').next().and_then(|n| n.parse().ok()).unwrap_or(10);
            let p = LossyLink { target: None, loss_pct: pct };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        "nat_shift" => {
            let p = NatShift { target: None };
            outcome_result = p.execute(&ctx).await;
            match outcome_result {
                Ok(o) => p.detect(&ctx, &o, deadline_ms).await,
                Err(e) => return ("failed", format!("execute: {e}")),
            }
        }
        _ => return ("failed", format!("unknown primitive label {kind}")),
    };
    match det {
        Ok(DetectionResult::Passed { waited_ms }) => (
            "passed",
            format!("{detect_kind} primitive detected in {waited_ms}ms"),
        ),
        Ok(DetectionResult::FailedTimeout { waited_ms }) => (
            "failed",
            format!("{detect_kind}: timed out after {waited_ms}ms"),
        ),
        Ok(DetectionResult::FailedAssertion { msg, waited_ms }) => (
            "failed",
            format!("{detect_kind}: assertion failed after {waited_ms}ms — {msg}"),
        ),
        Err(e) => ("failed", format!("detect err: {e}")),
    }
}

/// Spawn 10 extra brokers above the bootstrap pool, kill them in sequence,
/// verify pool returns to original size. Tests pool-cap discipline + reaper.
async fn run_mesh_grow_shrink(api_url: &str) -> (&'static str, String) {
    let client = reqwest::Client::new();
    // Capture baseline
    let pre = match client
        .get(format!("{api_url}/api/cluster/summary"))
        .send()
        .await
    {
        Ok(r) => r.json::<serde_json::Value>().await.unwrap_or(serde_json::json!({})),
        Err(e) => return ("failed", format!("baseline query: {e}")),
    };
    let baseline = pre["spawned"].as_i64().unwrap_or(0);

    // Spawn 10 extras
    let mut spawned: Vec<String> = Vec::new();
    for _ in 0..10 {
        let resp = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type":"broker","mesh_id":"mesh-a"}))
            .send()
            .await;
        if let Ok(r) = resp {
            if let Ok(body) = r.json::<serde_json::Value>().await {
                if let Some(n) = body["node_name"].as_str() {
                    spawned.push(n.to_string());
                }
            }
        }
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    // Kill them all in sequence
    for n in &spawned {
        let _ = client.delete(format!("{api_url}/api/nodes/{n}")).send().await;
    }
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    // Verify pool returned to baseline
    let post = match client
        .get(format!("{api_url}/api/cluster/summary"))
        .send()
        .await
    {
        Ok(r) => r.json::<serde_json::Value>().await.unwrap_or(serde_json::json!({})),
        Err(e) => return ("failed", format!("post query: {e}")),
    };
    let post_count = post["spawned"].as_i64().unwrap_or(-1);
    if (post_count - baseline).abs() <= 1 {
        (
            "passed",
            format!(
                "baseline={baseline}, spawned 10 ({} survived), killed all, post={post_count}",
                spawned.len()
            ),
        )
    } else {
        (
            "failed",
            format!("pool drift: baseline={baseline}, post={post_count}"),
        )
    }
}

/// Helper: pick a random node_name from /api/heartbeats matching a type prefix.
async fn pick_node_of_type(api_url: &str, type_prefix: &str) -> Option<String> {
    let client = reqwest::Client::new();
    let r = client
        .get(format!("{api_url}/api/heartbeats"))
        .send()
        .await
        .ok()?;
    let body: serde_json::Value = r.json().await.ok()?;
    let mut names: Vec<String> = body["heartbeats"]
        .as_array()?
        .iter()
        .filter_map(|h| {
            let n = h["node_name"].as_str()?;
            if n.starts_with(type_prefix) {
                Some(n.to_string())
            } else {
                None
            }
        })
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort();
    names.first().cloned()
}

async fn run_mesh_five_types_present(api_url: &str) -> (&'static str, String) {
    use std::collections::HashSet;
    let client = reqwest::Client::new();
    for t in ["gateway", "broker", "compute", "registry", "bridge"] {
        let _ = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type": t, "mesh_id": "mesh-a"}))
            .send()
            .await;
    }
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    // Use /api/heartbeats — /api/nodes/spawned was removed in the
    // mesh-native pivot. Heartbeats is the authoritative live list.
    let body: serde_json::Value = match client
        .get(format!("{api_url}/api/heartbeats"))
        .send()
        .await
    {
        Ok(r) => r.json().await.unwrap_or(serde_json::json!({"heartbeats": []})),
        Err(e) => return ("failed", format!("query /api/heartbeats: {e}")),
    };
    let types: HashSet<String> = body["heartbeats"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|h| h["node_type"].as_str().map(|s| s.to_string()))
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

async fn run_gossip_swarm_forms(api_url: &str) -> (&'static str, String) {
    let client = reqwest::Client::new();
    for t in ["gateway", "broker", "compute", "registry"] {
        let _ = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type": t, "mesh_id": "mesh-a"}))
            .send()
            .await
            .ok();
    }
    tokio::time::sleep(std::time::Duration::from_secs(18)).await;
    // Sum received spans across all 4 services — proves the swarm formed.
    let mut total_rx: i64 = 0;
    for svc in ["gateway", "broker", "compute", "registry"] {
        let url = format!(
            "http://localhost:16686/api/traces?service={svc}&operation=rafka.mesh.gossip.received&limit=50&lookback=2m"
        );
        let body = fetch_json(&client, &url).await;
        total_rx += body["data"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t["spans"].as_array())
                    .flat_map(|ss| ss.iter())
                    .filter(|sp| sp["operationName"] == "rafka.mesh.gossip.received")
                    .count() as i64
            })
            .unwrap_or(0);
    }
    if total_rx >= 4 {
        ("passed", format!("gossip swarm exchanged {total_rx} received digests across 4 nodes"))
    } else {
        ("failed", format!("only {total_rx} gossip.received spans seen (need ≥4 across 4 nodes for evidence of swarm formation)"))
    }
}

async fn run_gossip_mesh_to_mesh(api_url: &str) -> (&'static str, String) {
    let client = reqwest::Client::new();
    // Two nodes, distinct mesh_ids
    for (t, mid) in [("gateway", "mesh-test-A"), ("broker", "mesh-test-B")] {
        let _ = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type": t, "extra_env": {"RAFKA_MESH_ID": mid}}))
            .send()
            .await
            .ok();
    }
    tokio::time::sleep(std::time::Duration::from_secs(12)).await;
    // cross.peer_connected fires when Hello frames carry mismatched mesh_ids
    let body = fetch_json(
        &client,
        "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.cross.peer_connected&limit=10&lookback=2m",
    )
    .await;
    let cross_count = body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["spans"].as_array())
                .flat_map(|ss| ss.iter())
                .filter(|sp| sp["operationName"] == "rafka.mesh.cross.peer_connected")
                .count() as i64
        })
        .unwrap_or(0);
    if cross_count >= 1 {
        ("passed", format!("{cross_count} cross.peer_connected spans — mesh-A and mesh-B detected each other across the boundary"))
    } else {
        ("failed", "no cross.peer_connected spans — mesh-to-mesh isolation/discovery broken".into())
    }
}

async fn run_remove_resilience(api_url: &str) -> (&'static str, String) {
    // QA F#5: Test MUST be isolated to its own spawned set. Previously this
    // queried /api/nodes/spawned which returns the ENTIRE ambient pool — so
    // against a warm server with 30+ pre-existing nodes, the pass criterion
    // was met vacuously by ambient survivors. Now we capture exactly the 6
    // names we spawn and only count survivors WITHIN that set.
    let client = reqwest::Client::new();
    let mut my_spawn_names: Vec<String> = Vec::new();
    for t in ["gateway", "broker", "broker", "compute", "registry", "bridge"] {
        let resp = client
            .post(format!("{api_url}/api/nodes/spawn"))
            .json(&serde_json::json!({"node_type": t, "mesh_id": "mesh-a"}))
            .send()
            .await;
        if let Ok(r) = resp {
            if let Ok(body) = r.json::<serde_json::Value>().await {
                if let Some(n) = body["node_name"].as_str() {
                    my_spawn_names.push(n.to_string());
                }
            }
        }
    }
    if my_spawn_names.len() < 6 {
        return (
            "failed",
            format!("spawned only {}/6 nodes successfully", my_spawn_names.len()),
        );
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // Kill the first 3 of our spawned set
    let kill_targets: Vec<String> = my_spawn_names.iter().take(3).cloned().collect();
    let survivors: Vec<String> = my_spawn_names.iter().skip(3).cloned().collect();
    for name in &kill_targets {
        let _ = client.delete(format!("{api_url}/api/nodes/{name}")).send().await;
    }
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    // Count fresh heartbeats ONLY from our 3 survivors (not the ambient pool)
    let hb = fetch_json(&client, &format!("{api_url}/api/heartbeats")).await;
    let fresh = hb["heartbeats"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|h| {
                    let name = h["node_name"].as_str().unwrap_or("");
                    survivors.iter().any(|s| s == name)
                        && h["age_ms"].as_i64().unwrap_or(99999) < 10000
                })
                .count()
        })
        .unwrap_or(0);
    if fresh == survivors.len() {
        (
            "passed",
            format!(
                "killed {} of our 6 spawned nodes; all {fresh} of our survivors emit fresh heartbeats",
                kill_targets.len()
            ),
        )
    } else {
        (
            "failed",
            format!(
                "only {fresh}/{} of OUR survivors fresh after 3 kills (need all 3)",
                survivors.len()
            ),
        )
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
    let req_body = serde_json::json!({"node_type": node_type.as_str(), "mesh_id": "mesh-a"});
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
