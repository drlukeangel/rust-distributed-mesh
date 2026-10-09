//! i143.e1.s6 ratchet: the Admin UI is a client of node-admin core, never a
//! second lifecycle authority (PRD §4).
//!
//! Over admin-ui's sources and manifest it refuses, by name:
//! - any reach into node-admin core or its old process table;
//! - any process-control primitive (kill, child handles);
//! - any process spawn;
//! - a fault aimed anywhere but through the chaos kit: `rafka_chaos` is named only in `chaos.rs`,
//!   whose faults are typed outcomes on one node's exact runtime and are never lifecycle (a
//!   topology change is a Build);
//! - a runtime dependency on node-admin core, or none on the client crate
//!   (a test may serve the real control API as a dev-dependency).

use std::path::Path;

const FORBIDDEN: &[(&str, &str)] = &[
    ("ProcessTable", "the process table is node-admin lifecycle"),
    ("process_table", "the process table is node-admin lifecycle"),
    ("rafka_node_admin_core", "admin-ui talks to node-admin over rafka-node-admin-client only"),
    ("libc::kill", "admin-ui never signals a runtime"),
    ("start_kill", "admin-ui never signals a runtime"),
    (".kill(", "admin-ui never signals a runtime"),
    ("process::Child", "admin-ui holds no runtime handle"),
];

/// Every violation in the admin-ui crate under `root`, as `file:line: why`.
pub fn lifecycle_violations(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let manifest = root.join("demo/admin-ui/Cargo.toml");
    match std::fs::read_to_string(&manifest) {
        Ok(m) => {
            // Runtime dependencies only: a test may serve the real control API.
            let deps = dependencies_section(&m);
            if deps.iter().any(|l| l.trim_start().starts_with("rafka-node-admin-core")) {
                out.push(format!("{}: depends on rafka-node-admin-core; use rafka-node-admin-client", manifest.display()));
            }
            if !deps.iter().any(|l| l.trim_start().starts_with("rafka-node-admin-client")) {
                out.push(format!("{}: does not depend on rafka-node-admin-client", manifest.display()));
            }
        }
        Err(e) => out.push(format!("{}: {e}", manifest.display())),
    }
    let src = root.join("demo/admin-ui/src");
    let mut files: Vec<_> = walk(&src);
    files.sort();
    for f in files {
        let Ok(text) = std::fs::read_to_string(&f) else { continue };
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            for (needle, why) in FORBIDDEN {
                if code.contains(needle) {
                    out.push(format!("{}:{}: `{needle}`: {why}", f.display(), n + 1));
                }
            }
            if code.contains("rafka_chaos") && f.file_name().and_then(|n| n.to_str()) != Some("chaos.rs") {
                out.push(format!("{}:{}: names the chaos kit outside chaos.rs; faults go through that one door", f.display(), n + 1));
            }
            if code.contains("Command::new") {
                out.push(format!("{}:{}: starts a process; topology changes are Build requests to node-admin", f.display(), n + 1));
            }
        }
    }
    out
}

/// The lines of the manifest's `[dependencies]` table.
fn dependencies_section(manifest: &str) -> Vec<&str> {
    let mut inside = false;
    let mut out = Vec::new();
    for l in manifest.lines() {
        let t = l.trim();
        if t.starts_with('[') {
            inside = t == "[dependencies]";
            continue;
        }
        if inside {
            out.push(l);
        }
    }
    out
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out
}
