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
    ("rafka_node_admin_core", "admin-ui talks to node-admin over rdm-client only"),
    ("rdm_admin", "admin-ui talks to node-admin over rdm-client only"),
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
            if deps.iter().any(|l| {
                let t = l.trim_start();
                t.starts_with("rafka-node-admin-core") || t.starts_with("rdm-admin")
            }) {
                out.push(format!("{}: depends on rafka-node-admin-core/rdm-admin; use rdm-client", manifest.display()));
            }
            if !deps.iter().any(|l| {
                let t = l.trim_start();
                t.starts_with("rafka-node-admin-client") || t.starts_with("rdm-client")
            }) {
                out.push(format!("{}: does not depend on rafka-node-admin-client/rdm-client", manifest.display()));
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
                if names(code, needle) {
                    out.push(format!("{}:{}: `{needle}`: {why}", f.display(), n + 1));
                }
            }
            if (code.contains("rafka_chaos") || code.contains("rdm_test")) && f.file_name().and_then(|n| n.to_str()) != Some("chaos.rs") {
                out.push(format!("{}:{}: names the chaos kit outside chaos.rs; faults go through that one door", f.display(), n + 1));
            }
            if code.contains("Command::new") {
                out.push(format!("{}:{}: starts a process; topology changes are Build requests to node-admin", f.display(), n + 1));
            }
        }
    }
    out
}

/// Whether `code` holds `needle`: an identifier needle only as a whole identifier, so `rdm_admin`
/// is not found inside `rdm_admin_ui`, the admin UI's own crate; any other needle as written.
fn names(code: &str, needle: &str) -> bool {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    if !needle.chars().all(ident) {
        return code.contains(needle);
    }
    code.match_indices(needle).any(|(at, _)| {
        !code[..at].chars().next_back().is_some_and(ident) && !code[at + needle.len()..].chars().next().is_some_and(ident)
    })
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
