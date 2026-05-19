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
    let _guard = rafka_telemetry::init_telemetry("rfa");

    let cli = Cli::parse();
    let client = reqwest::Client::new();

    let (command_name, args_str) = describe_command(&cli.command);
    let cmd_span = info_span!(
        "rafka.cli.command",
        command = %command_name,
        args = %args_str,
        "otel.kind" = "internal",
    );

    let result = run_command(&cli, &client).instrument(cmd_span).await;

    // Give BatchSpanProcessor time to export before process exits
    tokio::time::sleep(Duration::from_millis(400)).await;

    result
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
        },
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
    let resp = client
        .post(url)
        .json(body)
        .send()
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
    let resp = client
        .delete(url)
        .send()
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
    let resp = client
        .get(url)
        .send()
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
