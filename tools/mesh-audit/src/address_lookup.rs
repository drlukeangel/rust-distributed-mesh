//! Address-lookup boundary gate (i143.e4.s13, rafka-v2 #2816).
//!
//! RDM's address authority is explicit: node-admin assigns every endpoint,
//! explicit seeds bootstrap gossip, and local mDNS (legacy, opt-in) is address
//! discovery only. Iroh's n0 services (DNS / Pkarr publication and resolution
//! through `iroh.link`) are a second, implicit authority and are never
//! configured. `presets::N0DisableRelay` is not "minimal with relay off": it
//! applies `N0`, n0 DNS/Pkarr included, and then disables relay.
//!
//! Rules, over every `.rs` file of the workspace (build output excluded):
//!
//! 1. no n0 address lookup: no `presets::N0` / `presets::N0DisableRelay`, no
//!    `DnsAddressLookup`, no `PkarrPublisher` / `PkarrResolver`;
//! 2. the canonical i143 crates never use the legacy transport plane: no
//!    `IrohMeshTransport`, no `MdnsAddressLookup`, no `rafka_node_base`
//!    (its `PeerRegistry`, seed-dial, mdns-dial and accept heartbeat are
//!    extraction input, not i143 membership or reachability);
//! 3. the only ALPNs of non-test code are `rafka-node-rpc/1` and `iroh-gossip`: any other ALPN
//!    constant or `Router::accept` registration is refused. `ENTRY_ALPN` is allowed by name,
//!    for the reason "R-J1 retires it", while no op `0x1D` exists in the Node RPC contract; once
//!    one does, `ENTRY_ALPN` itself is the violation.

use std::fmt;
use std::path::{Path, PathBuf};

/// Tokens that configure an n0 address lookup service.
pub const N0_TOKENS: &[&str] = &["presets::N0", "DnsAddressLookup", "PkarrPublisher", "PkarrResolver"];

/// The canonical i143 sources: Node RPC, node-admin, the rpc node, the client,
/// and the membership/entry modules of the transport crate.
pub const CANONICAL: &[&str] = &[
    "crates/rafka-node-rpc/src",
    "crates/rafka-node-rpc-testkit/src",
    "crates/rafka-node-admin-core/src",
    "crates/rafka-node-admin-client/src",
    "crates/rafka-mesh-transport/src/membership.rs",
    "crates/rafka-mesh-transport/src/entry.rs",
];

/// Legacy-plane tokens the canonical sources never use.
pub const LEGACY_TOKENS: &[&str] = &["IrohMeshTransport", "MdnsAddressLookup", "rafka_node_base"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Rule 1: an n0 address lookup at `file:line`.
    N0Lookup { file: String, line: usize, token: String },
    /// Rule 2: a canonical source uses the legacy plane at `file:line`.
    LegacyPlane { file: String, line: usize, token: String },
    /// Rule 3: an ALPN constant or acceptor beyond Node RPC and gossip at `file:line`.
    Alpn { file: String, line: usize, token: String, why: String },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::N0Lookup { file, line, token } => {
                write!(f, "rule 1: {file}:{line} configures an n0 address lookup (`{token}`); RDM's address authority is node-admin's assignment and explicit seeds")
            }
            Self::LegacyPlane { file, line, token } => {
                write!(f, "rule 2: {file}:{line} uses the legacy transport plane (`{token}`) from a canonical i143 source")
            }
            Self::Alpn { file, line, token, why } => {
                write!(f, "rule 3: {file}:{line} `{token}`: {why}; RDM's only ALPNs are rafka-node-rpc/1 and iroh-gossip")
            }
        }
    }
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name();
        if p.is_dir() {
            if name != "target" && name != ".git" {
                rust_files(&p, out);
            }
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// The code of `line` without its `//` comment (a comment may name a token).
fn code(line: &str) -> &str {
    match line.find("//") {
        Some(i) => &line[..i],
        None => line,
    }
}

fn scan(root: &Path, file: &Path, tokens: &[&str], mut found: impl FnMut(String, usize, String)) {
    let Ok(text) = std::fs::read_to_string(file) else { return };
    let rel = file.strip_prefix(root).unwrap_or(file).display().to_string();
    for (i, line) in text.lines().enumerate() {
        let c = code(line);
        for t in tokens {
            if c.contains(t) {
                found(rel.clone(), i + 1, (*t).to_string());
            }
        }
    }
}

/// The only ALPN values non-test code defines.
const NODE_RPC_ALPN_VALUE: &str = "b\"rafka-node-rpc/1\"";
const ENTRY_ALPN_NAME: &str = "ENTRY_ALPN";
/// First arguments `.accept(..)` may register.
const ACCEPTED: &[&str] = &["rafka_node_rpc::ALPN", "iroh_gossip::ALPN", "crate::ALPN"];

/// Non-test text of a file: everything before its first `#[cfg(test)]`.
fn non_test(text: &str) -> &str {
    text.find("#[cfg(test)]").map(|i| &text[..i]).unwrap_or(text)
}

/// Rule 3 over one file. `entry_allowed`: `ENTRY_ALPN` is still live (R-J1 has not landed).
fn alpn_rule(rel: &str, text: &str, entry_allowed: bool, out: &mut Vec<Violation>) {
    let entry_why = "R-J1 retires it: op 0x1D exists, so ENTRY_ALPN is deleted";
    for (i, line) in non_test(text).lines().enumerate() {
        let c = code(line);
        let mut flag = |token: &str, why: &str| out.push(Violation::Alpn { file: rel.to_string(), line: i + 1, token: token.to_string(), why: why.to_string() });
        if let Some(rest) = c.split("const ").nth(1) {
            let name = rest.split(':').next().unwrap_or("").trim();
            if name.contains("ALPN") && rest.contains("&[u8]") {
                if name == ENTRY_ALPN_NAME {
                    if !entry_allowed {
                        flag(name, entry_why);
                    }
                } else if !rest.contains(NODE_RPC_ALPN_VALUE) {
                    flag(name, "an ALPN constant other than Node RPC's");
                }
            }
        }
        let mut rest = c;
        while let Some(at) = rest.find(".accept(") {
            rest = &rest[at + ".accept(".len()..];
            let arg = rest.split(',').next().unwrap_or("").trim();
            if rest.starts_with(')') {
                continue;
            }
            if arg == ENTRY_ALPN_NAME {
                if !entry_allowed {
                    flag(arg, entry_why);
                }
            } else if !ACCEPTED.contains(&arg) {
                flag(arg, "a Router::accept registration for an ALPN other than Node RPC or gossip");
            }
        }
    }
}

/// Every violation in the workspace at `root`. This gate's own source names
/// the tokens it looks for and is not scanned.
pub fn check(root: &Path) -> Vec<Violation> {
    let mut out = Vec::new();
    let mut files = Vec::new();
    rust_files(root, &mut files);
    files.sort();
    let is_test = |rel: &Path| rel.components().any(|c| c.as_os_str() == "tests") || rel.starts_with("tools/mesh-audit");
    let entry_defined = files.iter().any(|f| !is_test(f.strip_prefix(root).unwrap_or(f)) && std::fs::read_to_string(f).is_ok_and(|t| non_test(&t).contains("const ENTRY_ALPN")));
    let join_landed = files
        .iter()
        .filter(|f| f.strip_prefix(root).is_ok_and(|r| r.starts_with("crates/rafka-node-rpc-contract/src")))
        .any(|f| std::fs::read_to_string(f).is_ok_and(|t| non_test(&t).contains("const OP: u8 = 0x1D")));
    let entry_allowed = entry_defined && !join_landed;
    let me = Path::new(file!()).file_name().unwrap_or_default().to_owned();
    for f in &files {
        let rel = f.strip_prefix(root).unwrap_or(f);
        if rel.starts_with("tools/mesh-audit") && f.file_name() == Some(me.as_os_str()) {
            continue;
        }
        if !is_test(rel) {
            if let Ok(text) = std::fs::read_to_string(f) {
                alpn_rule(&rel.display().to_string(), &text, entry_allowed, &mut out);
            }
        }
        scan(root, f, N0_TOKENS, |file, line, token| out.push(Violation::N0Lookup { file, line, token }));
        if CANONICAL.iter().any(|c| rel.starts_with(c)) {
            scan(root, f, LEGACY_TOKENS, |file, line, token| out.push(Violation::LegacyPlane { file, line, token }));
        }
    }
    out
}
