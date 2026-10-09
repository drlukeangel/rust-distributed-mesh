//! i143.e11 static ratchets over the R-shape qualification definition (epic #2943, story #2944).
//!
//! - `rshape_consumer_imports_exact_candidate_without_private_paths`: the independent consumer
//!   workspace (`demo`) imports RDM only by git dependency pinned to one
//!   exact rev, has no path / [patch] / [replace] shortcut, no Rafka business crate, and is
//!   excluded from the RDM root workspace. Every rule has a planted failure that names it.
//! - `rshape_definition_covers_every_scenario_without_business_dependencies`: the matrix
//!   (`tools/mesh-audit/i143-rshape-matrix.json`) covers C1-C20 and S1-S5 on both providers in the
//!   canonical 2/2/3/3 x 2 shape, every cell has its registry row and command, and no second
//!   planner/authority or business dependency is present.
//!
//! Neither test needs a runtime span: they read manifests, lockfiles and registries. Each leaves
//! its result under `target/i143-rshape/<job>/`.

use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const RDM_URL: &str = "https://github.com/drlukeangel/rust-distributed-mesh";
/// The forks the substrate is built from, each a direct git dependency of the imported graph.
const FORK_URLS: [&str; 4] = [
    "https://github.com/drlukeangel/iroh-gossip",
    "https://github.com/drlukeangel/iroh",
    "https://github.com/drlukeangel/netwatch",
    "https://github.com/drlukeangel/noq",
];
const CONSUMER: &str = "demo";
const BINS: [&str; 4] = ["rshape-node-admin", "rshape-compute", "rshape-gateway", "rshape-broker"];
/// The RDM packages a consumer may import: the public node composition surface and what it
/// resolves to. Anything else named `rafka-*` is a business crate.
const APPROVED_RDM: [&str; 9] = [
    "rafka-node-base",
    "rafka-node-admin-core",
    "rafka-mesh-transport",
    "rafka-mesh-telemetry",
    "rafka-node-rpc-testkit",
    "rafka-node-rpc",
    "rafka-node-rpc-contract",
    "rafka-node-admin-client",
    "rafka-mesh-entity",
];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

fn is_rev(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn attr<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let at = line.find(&format!("{key} = \""))? + key.len() + 4;
    let rest = &line[at..];
    Some(&rest[..rest.find('"')?])
}

// ---------------------------------------------------------------- consumer manifest rules

/// Every rule the consumer's `Cargo.toml` breaks, each named `rshape-consumer-<rule>`.
fn check_manifest(toml: &str) -> (Vec<String>, BTreeSet<String>) {
    let mut v = Vec::new();
    let mut revs = BTreeSet::new();
    let (mut section, mut own_root, mut bins) = (String::new(), false, BTreeSet::new());
    for raw in toml.lines() {
        let line = raw.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            section = line.trim_matches(|c| c == '[' || c == ']').to_string();
            if section == "workspace" {
                own_root = true;
            }
            if section.starts_with("patch") || section.starts_with("replace") {
                v.push(format!("rshape-consumer-patch-or-replace: `[{section}]` redirects an import"));
            }
            continue;
        }
        if section == "bin" || section == "[bin" || section.is_empty() {
            // `[[bin]]` trims to `bin`.
        }
        if section == "bin" {
            if let Some(n) = attr(line, "name") {
                bins.insert(n.to_string());
            }
            continue;
        }
        if !section.ends_with("dependencies") {
            continue;
        }
        let Some((name, spec)) = line.split_once('=') else { continue };
        let (name, spec) = (name.trim(), spec.trim());
        if spec.contains("path =") || spec.contains("path=") {
            v.push(format!("rshape-consumer-path-dependency: `{name}` is a path dependency"));
        }
        let is_rdm_name = name.starts_with("rafka-");
        if is_rdm_name && !APPROVED_RDM.contains(&name) {
            v.push(format!("rshape-consumer-business-dependency: `{name}` is not an approved RDM import"));
        }
        let git = attr(spec, "git");
        match (is_rdm_name, git) {
            (true, Some(u)) if u == RDM_URL => {}
            (true, other) => v.push(format!("rshape-consumer-wrong-source: `{name}` must be git {RDM_URL}, is {other:?}")),
            (false, Some(u)) => v.push(format!("rshape-consumer-wrong-source: `{name}` takes git source {u}; only RDM crates are imported by git")),
            (false, None) => {}
        }
        if is_rdm_name {
            if spec.contains("branch") || spec.contains("tag =") {
                v.push(format!("rshape-consumer-floating-dependency: `{name}` names a branch or tag"));
            }
            match attr(spec, "rev") {
                Some(r) if is_rev(r) => {
                    revs.insert(r.to_string());
                }
                other => v.push(format!("rshape-consumer-floating-dependency: `{name}` rev {other:?} is not an exact 40-hex commit")),
            }
        }
    }
    if revs.len() > 1 {
        v.push(format!("rshape-consumer-mixed-revs: RDM dependencies pin {} different revs: {revs:?}", revs.len()));
    }
    if !own_root {
        v.push("rshape-consumer-not-own-workspace: Cargo.toml has no [workspace] table, so a parent workspace would claim it".into());
    }
    for b in BINS {
        if !bins.contains(b) {
            v.push(format!("rshape-consumer-missing-entry-point: no [[bin]] named {b}"));
        }
    }
    (v, revs)
}

/// Every rule the consumer's `Cargo.lock` breaks against the manifest's one rev.
fn check_lock(lock: &str, rev: &str) -> Vec<String> {
    let mut v = Vec::new();
    let want = format!("git+{RDM_URL}?rev={rev}#{rev}");
    let (mut name, mut seen_rdm) = (String::new(), 0);
    for line in lock.lines().map(str::trim) {
        if line == "[[package]]" {
            name.clear();
        } else if let Some(n) = attr(line, "name").filter(|_| line.starts_with("name = ")) {
            name = n.to_string();
        } else if let Some(src) = attr(line, "source").filter(|_| line.starts_with("source = ")) {
            if name.starts_with("rafka-") {
                seen_rdm += 1;
                if src != want {
                    v.push(format!("rshape-consumer-lock-rev-mismatch: `{name}` locks {src}, the manifest pins {want}"));
                }
                if !APPROVED_RDM.contains(&name.as_str()) {
                    v.push(format!("rshape-consumer-business-dependency: `{name}` is locked but not an approved RDM import"));
                }
            } else if src.starts_with("git+") && !FORK_URLS.iter().any(|u| src.starts_with(&format!("git+{u}?rev="))) {
                v.push(format!("rshape-consumer-wrong-source: `{name}` locks {src}"));
            }
        }
    }
    if seen_rdm == 0 {
        v.push("rshape-consumer-lock-rev-mismatch: no RDM package is locked from the RDM git source".into());
    }
    // A package with no `source` line is a path package; only the consumer itself may be one.
    let mut cur: Option<(String, bool)> = None;
    let mut sourceless = Vec::new();
    for line in lock.lines().map(str::trim).chain(std::iter::once("[[package]]")) {
        if line == "[[package]]" {
            if let Some((n, has_src)) = cur.take() {
                if !has_src && n != "rshape-consumer" {
                    sourceless.push(n);
                }
            }
            cur = Some((String::new(), false));
        } else if let Some((n, has_src)) = cur.as_mut() {
            if line.starts_with("name = ") {
                *n = attr(line, "name").unwrap_or("").to_string();
            }
            if line.starts_with("source = ") {
                *has_src = true;
            }
        }
    }
    for n in sourceless {
        v.push(format!("rshape-consumer-path-dependency: `{n}` is locked with no registry or git source"));
    }
    v
}

/// The consumer composes; it never implements a planner, an election or an authority itself.
fn check_sources(files: &[(String, String)]) -> Vec<String> {
    let mut v = Vec::new();
    for (path, text) in files {
        for (i, line) in text.lines().enumerate() {
            let l = line.trim_start();
            let l = l.strip_prefix("pub ").unwrap_or(l);
            for kw in ["fn ", "struct ", "enum ", "trait ", "mod "] {
                if let Some(rest) = l.strip_prefix(kw) {
                    let ident: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect::<String>().to_lowercase();
                    if ["planner", "elect", "authority", "death", "reconcil"].iter().any(|w| ident.contains(w)) {
                        v.push(format!("rshape-consumer-second-mechanism: {path}:{} defines `{ident}`; the consumer imports RDM's planner/authority/death detector, it does not carry one", i + 1));
                    }
                }
            }
            // Every topology operation is a Build the node-admin rectifier executes
            // (build.*.via-rest -> via-reconcile -> node.*.via-build): the consumer never starts,
            // signals or removes a process itself.
            for banned in ["Command::new", "process::Command", "tokio::process", "libc::kill", "nix::sys::signal", "std::process::Child"] {
                if line.contains(banned) && !line.trim_start().starts_with("//") {
                    v.push(format!("rshape-consumer-topology-shortcut: {path}:{} uses `{banned}`; spawn, restart, replace and delete go through a Build", i + 1));
                }
            }
            if l.starts_with("use ") || l.contains("::") {
                for word in l.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
                    if let Some(krate) = word.strip_prefix("rafka_") {
                        let pkg = format!("rafka-{}", krate.replace('_', "-"));
                        if !APPROVED_RDM.contains(&pkg.as_str()) {
                            v.push(format!("rshape-consumer-business-dependency: {path}:{} names crate `{pkg}`", i + 1));
                        }
                    }
                }
            }
        }
    }
    v
}

/// `cargo metadata --locked` of a built consumer against its build manifest: every RDM package at
/// the candidate rev and only approved ones; no path package but the consumer.
fn check_build_receipt(metadata: &Value, manifest: &Value) -> Vec<String> {
    let mut v = Vec::new();
    let cand = manifest["candidate_sha"].as_str().unwrap_or("");
    if !is_rev(cand) {
        v.push(format!("rshape-consumer-receipt-candidate: the build manifest's candidate_sha `{cand}` is not a 40-hex commit"));
    }
    let mut rdm = 0;
    for p in metadata["packages"].as_array().into_iter().flatten() {
        let name = p["name"].as_str().unwrap_or("?");
        match p["source"].as_str() {
            None if name != "rshape-consumer" => v.push(format!("rshape-consumer-path-dependency: package `{name}` resolves to a path ({})", p["manifest_path"])),
            None => {}
            Some(src) if src.starts_with(&format!("git+{RDM_URL}")) => {
                rdm += 1;
                if !src.ends_with(&format!("#{cand}")) || !src.contains(&format!("rev={cand}")) {
                    v.push(format!("rshape-consumer-receipt-rev-mismatch: `{name}` resolves to {src}, the candidate is {cand}"));
                }
                if !APPROVED_RDM.contains(&name) {
                    v.push(format!("rshape-consumer-business-dependency: `{name}` is resolved but not an approved RDM import"));
                }
            }
            Some(_) => {}
        }
    }
    if rdm == 0 {
        v.push("rshape-consumer-receipt-rev-mismatch: no RDM package resolves from the candidate".into());
    }
    for b in BINS {
        if manifest["binaries"][b].as_str().map_or(true, |h| h.len() != 64) {
            v.push(format!("rshape-consumer-receipt-binary: the build manifest hashes no `{b}`"));
        }
    }
    v
}

fn check_root_excludes_consumer(root_toml: &str) -> Vec<String> {
    let mut v = Vec::new();
    let members = root_toml.split("members = [").nth(1).and_then(|r| r.split(']').next()).unwrap_or("");
    if members.contains(CONSUMER) {
        v.push(format!("rshape-consumer-in-root-workspace: the root workspace lists {CONSUMER} as a member"));
    }
    let excluded = root_toml.lines().any(|l| l.trim_start().starts_with("exclude") && l.contains(CONSUMER));
    if !excluded {
        v.push(format!("rshape-consumer-in-root-workspace: the root Cargo.toml does not `exclude` {CONSUMER}"));
    }
    v
}

fn consumer_sources() -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut stack = vec![root().join(CONSUMER).join("src")];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push((p.strip_prefix(root()).unwrap().display().to_string(), std::fs::read_to_string(&p).unwrap()));
            }
        }
    }
    out.sort();
    out
}

fn assert_names(rule: &str, violations: &[String], planted: &str) {
    assert!(violations.iter().any(|m| m.starts_with(&format!("{rule}:"))), "planted {planted} must fail rule {rule}; got {violations:#?}");
}

fn write_result(job: &str, file: &str, v: &Value) {
    let dir = root().join("target/i143-rshape").join(job);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(file), serde_json::to_vec_pretty(v).unwrap()).unwrap();
    // Run by the acceptance runner, the cell's own directory is `I143_ACCEPTANCE_DIR` and the
    // runner requires its `result.json` there: the same record, beside the job-level file above.
    if let Ok(cell) = std::env::var("I143_ACCEPTANCE_DIR") {
        std::fs::create_dir_all(&cell).unwrap();
        std::fs::write(std::path::Path::new(&cell).join("result.json"), serde_json::to_vec_pretty(v).unwrap()).unwrap();
        std::fs::write(std::path::Path::new(&cell).join(file), serde_json::to_vec_pretty(v).unwrap()).unwrap();
    }
}

#[test]
fn rshape_consumer_imports_exact_candidate_without_private_paths() {
    let toml = read(&format!("{CONSUMER}/Cargo.toml"));
    let lock = read(&format!("{CONSUMER}/Cargo.lock"));
    let root_toml = read("Cargo.toml");

    // The committed fixture is clean.
    let (mut clean, revs) = check_manifest(&toml);
    let rev = revs.iter().next().cloned().unwrap_or_default();
    assert_eq!(revs.len(), 1, "the fixture pins exactly one RDM rev: {revs:?}");
    clean.extend(check_lock(&lock, &rev));
    clean.extend(check_sources(&consumer_sources()));
    clean.extend(check_root_excludes_consumer(&root_toml));
    assert!(clean.is_empty(), "the consumer fixture breaks its import rules:\n{}", clean.join("\n"));

    // Every rule fails, by name, on a planted violation.
    let dep = format!("rafka-node-base = {{ git = \"{RDM_URL}\", rev = \"{rev}\" }}");
    assert!(toml.contains(&dep), "the plant anchors on the node-base dependency line");
    let mut planted: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut plant = |rule: &'static str, what: &'static str, v: Vec<String>| {
        assert_names(rule, &v, what);
        planted.entry(rule).or_insert_with(Vec::new).push(what);
    };
    plant("rshape-consumer-path-dependency", "a path dependency into the checkout", check_manifest(&toml.replace(&dep, "rafka-node-base = { path = \"../../crates/rafka-node-base\" }")).0);
    plant("rshape-consumer-patch-or-replace", "a [patch] shortcut", check_manifest(&format!("{toml}\n[patch.\"{RDM_URL}\"]\nrafka-node-base = {{ path = \"/x\" }}\n")).0);
    plant("rshape-consumer-floating-dependency", "a branch dependency", check_manifest(&toml.replace(&format!("rev = \"{rev}\""), "branch = \"main\"")).0);
    plant("rshape-consumer-business-dependency", "a Rafka business crate", check_manifest(&toml.replace("[dependencies]\n", &format!("[dependencies]\nrafka-gateway = {{ git = \"{RDM_URL}\", rev = \"{rev}\" }}\n"))).0);
    plant("rshape-consumer-wrong-source", "a foreign git source", check_manifest(&toml.replace(RDM_URL, "https://github.com/drlukeangel/rafka-v2")).0);
    plant("rshape-consumer-mixed-revs", "two different revs", check_manifest(&toml.replacen(&rev, &"1".repeat(40), 1)).0);
    plant("rshape-consumer-not-own-workspace", "no [workspace] table", check_manifest(&toml.replace("[workspace]", "")).0);
    plant("rshape-consumer-missing-entry-point", "a missing entry point", check_manifest(&toml.replace("name = \"rshape-broker\"", "name = \"other\"")).0);
    plant("rshape-consumer-lock-rev-mismatch", "a lockfile at another rev", check_lock(&lock.replace(&rev, &"2".repeat(40)), &rev));
    plant("rshape-consumer-in-root-workspace", "the consumer as a root member", check_root_excludes_consumer(&root_toml.replace("exclude = [", "xexclude = [").replace("\"crates/rafka-mesh-telemetry\",", &format!("\"crates/rafka-mesh-telemetry\", \"{CONSUMER}\","))));
    plant(
        "rshape-consumer-second-mechanism",
        "a consumer-owned planner",
        check_sources(&[("src/lib.rs".into(), "pub struct ReplacementPlanner;\n".into())]),
    );
    plant("rshape-consumer-topology-shortcut", "a consumer that spawns a process itself", check_sources(&[("src/lib.rs".into(), "let c = std::process::Command::new(\"x\");\n".into())]));
    plant("rshape-consumer-business-dependency", "a business crate named in source", check_sources(&[("src/lib.rs".into(), "use rafka_gateway::route;\n".into())]));

    // The build receipt, when a build has left one: the resolved metadata is the candidate and
    // nothing else. The planted receipts fail by name either way.
    let good_meta = json!({"packages": [
        {"name": "rshape-consumer", "source": null, "manifest_path": "/w/Cargo.toml"},
        {"name": "rafka-node-base", "source": format!("git+{RDM_URL}?rev={rev}#{rev}")}]});
    let bins: serde_json::Map<String, Value> = BINS.iter().map(|b| (b.to_string(), json!("a".repeat(64)))).collect();
    let good_manifest = json!({"candidate_sha": rev, "binaries": bins});
    assert!(check_build_receipt(&good_meta, &good_manifest).is_empty(), "a clean receipt passes");
    let other = "3".repeat(40);
    let moved = json!({"packages": [{"name": "rafka-node-base", "source": format!("git+{RDM_URL}?rev={other}#{other}")}]});
    plant("rshape-consumer-receipt-rev-mismatch", "a package at another rev", check_build_receipt(&moved, &good_manifest));
    let pathpkg = json!({"packages": [{"name": "rafka-node-base", "source": null, "manifest_path": "/checkout/crates/rafka-node-base/Cargo.toml"}]});
    plant("rshape-consumer-path-dependency", "a path package in metadata", check_build_receipt(&pathpkg, &good_manifest));
    let built = root().join("target/i143-rshape/consumer-build");
    let receipt_checked = if built.join("metadata.json").exists() && built.join("manifest.json").exists() {
        let m: Value = serde_json::from_slice(&std::fs::read(built.join("metadata.json")).unwrap()).unwrap();
        let b: Value = serde_json::from_slice(&std::fs::read(built.join("manifest.json")).unwrap()).unwrap();
        let v = check_build_receipt(&m, &b);
        assert!(v.is_empty(), "the consumer build receipt breaks its import rules:\n{}", v.join("\n"));
        b["candidate_sha"].as_str().map(str::to_string)
    } else {
        None
    };

    write_result(
        "consumer-import-static",
        "metadata.json",
        &json!({"fixture_rev": rev, "rdm_url": RDM_URL, "approved_rdm_packages": APPROVED_RDM, "clean": true, "planted_failures_named": planted, "build_receipt_checked_for_candidate": receipt_checked}),
    );
}

// ---------------------------------------------------------------- the definition matrix

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}

/// Every rule the matrix + registry break, each named `rshape-definition-<rule>`.
fn check_matrix(m: &Value, reg: &Value) -> Vec<String> {
    let mut v = Vec::new();
    let cells = m["cells"].as_array().cloned().unwrap_or_default();
    let canonical: Vec<&Value> = cells.iter().filter(|c| s(c, "tier") == "canonical").collect();
    let has = |test: &str, provider: &str| canonical.iter().any(|c| s(c, "test") == test && s(c, "provider") == provider);
    let seed = m["seed"].as_u64().unwrap_or(0);

    // Shapes: 2/2/3/3 x 2 = 20 canonical; 2/1/1/1 x 2 = 10 fast, never export-qualifying.
    let total = |sh: &Value| sh["meshes"].as_u64().unwrap_or(0) * ["node_admin", "compute", "gateway", "broker"].iter().map(|k| sh["per_mesh"][k].as_u64().unwrap_or(0)).sum::<u64>();
    let can = &m["shapes"]["canonical"];
    let canonical_counts = ["node_admin", "compute", "gateway", "broker"].map(|k| can["per_mesh"][k].as_u64().unwrap_or(0));
    if canonical_counts != [2, 2, 3, 3] || can["meshes"] != 2 || total(can) != 20 || can["total"] != 20 {
        v.push(format!("rshape-definition-shape: canonical must be node-admin/compute/gateway/broker = 2/2/3/3 x 2 meshes = 20, is {canonical_counts:?} x {} = {}", can["meshes"], total(can)));
    }
    let fast = &m["shapes"]["fast"];
    if total(fast) != 10 || fast["total"] != 10 || fast["qualifies_export"] != false {
        v.push("rshape-definition-shape: the reduced fast shape is 2/1/1/1 x 2 = 10 and never qualifies export".into());
    }
    if m["providers"] != json!(["process", "container"]) {
        v.push(format!("rshape-definition-missing-provider: providers must be process and container, are {}", m["providers"]));
    }

    // Scenarios: C1-C20, each owned by one of the epic's children, each proven on both providers.
    let mut covered = BTreeSet::new();
    for n in 1..=20 {
        let id = format!("C{n}");
        let sc = &m["scenarios"][&id];
        let tests: Vec<&str> = sc["tests"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
        if tests.is_empty() || !(2944..=2949).contains(&sc["owner_issue"].as_u64().unwrap_or(0)) {
            v.push(format!("rshape-definition-uncovered-scenario: {id} names no test or no owning child issue"));
        }
        for t in tests {
            covered.insert(t.to_string());
            for p in ["process", "container"] {
                if !has(t, p) {
                    v.push(format!("rshape-definition-missing-provider: {id} test {t} has no canonical {p} cell"));
                }
            }
        }
    }
    for n in 1..=5 {
        let id = format!("S{n}");
        let so = &m["soaks"][&id];
        let t = s(so, "test");
        if t.is_empty() || so["seconds"] != 1800 {
            v.push(format!("rshape-definition-uncovered-scenario: {id} must name a test and run 1800 seconds per provider"));
        }
        covered.insert(t.to_string());
        covered.insert(s(so, "replay_test").to_string());
        for p in ["process", "container"] {
            if !has(t, p) {
                v.push(format!("rshape-definition-missing-provider: {id} test {t} has no canonical {p} cell"));
            }
            if !has(s(so, "replay_test"), p) {
                v.push(format!("rshape-definition-missing-provider: {id} replay {} has no canonical {p} cell", s(so, "replay_test")));
            }
        }
        let soak_cell = canonical.iter().find(|c| s(c, "test") == t);
        if soak_cell.is_some_and(|c| !s(c, "command").contains("RDM_RSHAPE_SOAK_SECONDS=1800")) {
            v.push(format!("rshape-definition-soak-duration: {id}'s command does not run RDM_RSHAPE_SOAK_SECONDS=1800"));
        }
    }

    // Cells: each canonical/fast runtime cell names its provider, seed, tier, consumer binaries and
    // estate directory; a cell no scenario, soak or composition story owns is an orphan.
    for c in cells.iter().filter(|c| matches!(s(c, "tier"), "canonical" | "fast")) {
        let (test, provider, tier, job, cmd) = (s(c, "test"), s(c, "provider"), s(c, "tier"), s(c, "job"), s(c, "command"));
        let want_env = [
            "RDM_RSHAPE_CONSUMER_BIN_DIR=target/i143-rshape/consumer-bin".to_string(),
            format!("MESH_SPAWN_TYPE={provider} "),
            format!("RDM_RSHAPE_TIER={tier} "),
            // A replay takes its seed and schedule from the recorded run under its replay root.
            if cmd.contains("RDM_RSHAPE_REPLAY_ROOT=") { "RDM_RSHAPE_REPLAY_ROOT=target/i143-rshape ".to_string() } else { format!("RDM_RSHAPE_SEED={seed} ") },
            format!("RDM_ARTIFACTS_DIR=target/i143-rshape/{job}/{test}/estate "),
        ];
        for w in want_env {
            if !cmd.contains(&w) {
                v.push(format!("rshape-definition-command: {job}/{test} ({provider}) command lacks `{}`", w.trim()));
            }
        }
        if !cmd.ends_with(&format!(" --test rshape_burn_in {test} -- --exact")) {
            v.push(format!("rshape-definition-command: {job}/{test} does not name exactly its own test"));
        }
        if !job.starts_with("i143-rshape-") || !job.ends_with(&format!("-{provider}")) {
            v.push(format!("rshape-definition-command: job {job} does not name provider {provider}"));
        }
        let owned = covered.contains(test) || s(c, "tier") == "fast" || [2944].contains(&c["issue"].as_u64().unwrap_or(0));
        if !owned {
            v.push(format!("rshape-definition-orphan-cell: {test} proves no scenario, soak or composition story"));
        }
    }
    for t in canonical.iter().map(|c| s(c, "test")).collect::<BTreeSet<_>>() {
        for p in ["process", "container"] {
            if !has(t, p) {
                v.push(format!("rshape-definition-missing-provider: canonical test {t} has no {p} cell"));
            }
        }
    }

    // Adversarial cells: extra probes of the scenarios beyond the qualifying matrix. Process provider only, one
    // job each (release gate), on the reduced fixture, each naming the scenarios it probes and the canon its
    // assertions come from; a cell declared red names the fork that owns it.
    let scenario_ids: BTreeSet<String> = (1..=20).map(|n| format!("C{n}")).collect();
    let mut adversarial_jobs: BTreeMap<&str, usize> = BTreeMap::new();
    for c in cells.iter().filter(|c| s(c, "tier") == "adversarial") {
        let (test, provider, job, cmd) = (s(c, "test"), s(c, "provider"), s(c, "job"), s(c, "command"));
        *adversarial_jobs.entry(job).or_default() += 1;
        if provider != "process" || !job.starts_with("i143-rshape-adv-") || !job.ends_with("-process") {
            v.push(format!("rshape-definition-adversarial-cell: {job}/{test} must be a process cell in a job named i143-rshape-adv-<name>-process (provider {provider})"));
        }
        let want_env = [
            "RDM_RSHAPE_CONSUMER_BIN_DIR=target/i143-rshape/consumer-bin".to_string(),
            "MESH_SPAWN_TYPE=process ".to_string(),
            "RDM_RSHAPE_TIER=fast ".to_string(),
            format!("RDM_RSHAPE_SEED={seed} "),
            format!("RDM_ARTIFACTS_DIR=target/i143-rshape/{job}/{test}/estate "),
        ];
        for w in want_env {
            if !cmd.contains(&w) {
                v.push(format!("rshape-definition-adversarial-cell: {job}/{test} command lacks `{}`", w.trim()));
            }
        }
        if !cmd.ends_with(&format!(" --test rshape_burn_in {test} -- --exact")) {
            v.push(format!("rshape-definition-adversarial-cell: {job}/{test} does not name exactly its own test"));
        }
        let covers: Vec<&str> = c["covers"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
        if covers.is_empty() || covers.iter().any(|x| !scenario_ids.contains(*x)) {
            v.push(format!("rshape-definition-adversarial-cell: {job}/{test} covers no scenario, or one that is not C1-C20: {covers:?}"));
        }
        if s(c, "canon").is_empty() {
            v.push(format!("rshape-definition-adversarial-cell: {job}/{test} cites no canon its assertions come from"));
        }
        if c.get("red").is_some_and(|r| r.as_str().is_none_or(str::is_empty)) {
            v.push(format!("rshape-definition-adversarial-cell: {job}/{test} is declared red and names no fork that owns it"));
        }
    }
    for (job, n) in adversarial_jobs {
        if n != 1 {
            v.push(format!("rshape-definition-adversarial-cell: job {job} holds {n} cells; an adversarial job holds one"));
        }
    }

    // Registry: every job of the matrix that the registry owns matches it cell for cell, and no
    // registered rshape job is absent from the matrix.
    let matrix_jobs: BTreeSet<&str> = cells.iter().map(|c| s(c, "job")).collect();
    let rjobs = reg["rshape_jobs"].as_object().cloned().unwrap_or_default();
    for (job, j) in rjobs.iter().filter(|(k, _)| !k.starts_with('_')) {
        if !matrix_jobs.contains(job.as_str()) {
            v.push(format!("rshape-definition-orphan-job: registered job {job} is in no matrix cell"));
        }
        for rc in j["cells"].as_array().into_iter().flatten() {
            let mc = cells.iter().find(|c| s(c, "job") == job && s(c, "test") == s(rc, "name"));
            match mc {
                None => v.push(format!("rshape-definition-orphan-job: {job}/{} is registered but not in the matrix", s(rc, "name"))),
                Some(mc) if s(mc, "command") != s(rc, "command") || s(mc, "dir") != s(rc, "dir") => {
                    v.push(format!("rshape-definition-command: {job}/{} registry row differs from the matrix cell", s(rc, "name")))
                }
                Some(_) => {}
            }
        }
    }
    for (job, issue) in [
        ("i143-rshape-consumer-build", 2944),
        ("i143-rshape-consumer-import-static", 2944),
        ("i143-rshape-definition-static", 2943),
        ("i143-rshape-composition-process", 2944),
        ("i143-rshape-composition-container", 2944),
        ("i143-rshape-fast-process", 2944),
        ("i143-rshape-fast-container", 2944),
    ] {
        let registered = rjobs.get(job).is_some_and(|j| j["issue"] == issue);
        if !registered {
            v.push(format!("rshape-definition-orphan-job: {job} (issue #{issue}) is in the matrix but not registered in rshape_jobs"));
        }
    }
    // Every built cell has its registry row and its test function; a cell the matrix declares pending (a story
    // not yet built) has neither, so a half-built cell fails on whichever side is missing.
    for c in &cells {
        let (job, test, source) = (s(c, "job"), s(c, "test"), s(c, "source"));
        let row = rjobs.get(job).is_some_and(|j| j["cells"].as_array().into_iter().flatten().any(|rc| s(rc, "name") == test));
        let has_fn = std::fs::read_to_string(root().join(source)).is_ok_and(|text| text.contains(&format!("fn {test}(")));
        if c.get("pending").is_some() {
            if row || has_fn {
                v.push(format!("rshape-definition-pending-cell-built: {job}/{test} is declared pending but has a registry row ({row}) or a test function ({has_fn})"));
            }
            continue;
        }
        if !row {
            v.push(format!("rshape-definition-unregistered-cell: {job}/{test} is in the matrix but has no registry row"));
        }
        if matches!(s(c, "tier"), "canonical" | "fast" | "adversarial") && !has_fn {
            v.push(format!("rshape-definition-cell-without-test: {job}/{test} has a registry row or matrix cell but no `fn {test}(` in {source}"));
        }
    }
    v
}

fn read_json(rel: &str) -> Value {
    serde_json::from_str(&read(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[test]
fn rshape_definition_covers_every_scenario_without_business_dependencies() {
    let matrix = read_json("tools/mesh-audit/i143-rshape-matrix.json");
    let reg = read_json("tools/mesh-audit/i143-acceptance-jobs.json");

    let clean = check_matrix(&matrix, &reg);
    assert!(clean.is_empty(), "the R-shape definition breaks its rules:\n{}", clean.join("\n"));

    // No business dependency and no second mechanism in the consumer the matrix names.
    let toml = read(&format!("{CONSUMER}/Cargo.toml"));
    let (mut dep_violations, revs) = check_manifest(&toml);
    dep_violations.extend(check_lock(&read(&format!("{CONSUMER}/Cargo.lock")), revs.iter().next().map(String::as_str).unwrap_or("")));
    dep_violations.extend(check_sources(&consumer_sources()));
    assert!(dep_violations.is_empty(), "the consumer carries business dependencies:\n{}", dep_violations.join("\n"));
    let bins: Vec<&str> = matrix["candidate_import"]["binaries"].as_array().unwrap().iter().filter_map(Value::as_str).collect();
    assert_eq!(bins, BINS, "the matrix names the consumer's four entry points");

    // Each rule fails, by name, on a planted fault.
    let mut planted: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut plant = |rule: &'static str, what: &'static str, m: &Value, r: &Value| {
        assert_names(rule, &check_matrix(m, r), what);
        planted.entry(rule).or_insert_with(Vec::new).push(what);
    };
    let mut r = reg.clone();
    r["rshape_jobs"]["i143-rshape-partition-chaos-process"]["cells"].as_array_mut().unwrap().pop();
    plant("rshape-definition-unregistered-cell", "a matrix cell with no registry row", &matrix, &r);
    let mut m = matrix.clone();
    m["cells"].as_array_mut().unwrap().iter_mut().find(|c| s(c, "test") == "mock_partition_heal_recovers_coverage_and_routes").unwrap()["test"] = Value::from("mock_no_such_cell");
    plant("rshape-definition-cell-without-test", "a matrix cell naming a test no source holds", &m, &reg);
    let mut m = matrix.clone();
    m["cells"].as_array_mut().unwrap().iter_mut().find(|c| s(c, "test") == "mock_partition_heal_recovers_coverage_and_routes").unwrap()["pending"] = Value::from(2948);
    plant("rshape-definition-pending-cell-built", "a pending cell that has a test function and a row", &m, &reg);
    let mut m = matrix.clone();
    m["scenarios"].as_object_mut().unwrap().remove("C7");
    plant("rshape-definition-uncovered-scenario", "a scenario with no cell (C7)", &m, &reg);
    let mut m = matrix.clone();
    let cells = m["cells"].as_array_mut().unwrap();
    let at = cells.iter().position(|c| s(c, "provider") == "container" && s(c, "test") == "mock_estate_forms_twenty_distinct_nodes").unwrap();
    cells.remove(at);
    plant("rshape-definition-missing-provider", "a canonical cell without its container twin", &m, &reg);
    let mut r = reg.clone();
    r["rshape_jobs"].as_object_mut().unwrap().insert("i143-rshape-ghost-process".into(), json!({"issue": 2944, "layer": "process", "cells": []}));
    plant("rshape-definition-orphan-job", "a registered job with no matrix cell", &matrix, &r);
    let mut r = reg.clone();
    r["rshape_jobs"].as_object_mut().unwrap().remove("i143-rshape-composition-container");
    plant("rshape-definition-orphan-job", "a matrix job with no registry row", &matrix, &r);
    let mut m = matrix.clone();
    m["shapes"]["canonical"]["per_mesh"]["gateway"] = json!(1);
    plant("rshape-definition-shape", "a canonical estate smaller than twenty", &m, &reg);
    let mut m = matrix.clone();
    m["shapes"]["fast"]["qualifies_export"] = json!(true);
    plant("rshape-definition-shape", "a reduced shape that qualifies export", &m, &reg);
    let mut m = matrix.clone();
    m["soaks"]["S3"]["seconds"] = json!(60);
    plant("rshape-definition-uncovered-scenario", "a shortened soak", &m, &reg);
    let mut m = matrix.clone();
    let c = m["cells"].as_array_mut().unwrap().iter_mut().find(|c| s(c, "tier") == "canonical").unwrap();
    let cmd = s(c, "command").replace("RDM_RSHAPE_CONSUMER_BIN_DIR=target/i143-rshape/consumer-bin ", "");
    c["command"] = json!(cmd);
    plant("rshape-definition-command", "a canonical command that does not bind the consumer binaries", &m, &reg);
    let mut m = matrix.clone();
    m["cells"].as_array_mut().unwrap().push(json!({"test": "mock_unowned_cell", "issue": 2945, "provider": "process", "tier": "canonical", "job": "i143-rshape-x-process",
        "command": "RDM_RSHAPE_CONSUMER_BIN_DIR=target/i143-rshape/consumer-bin MESH_SPAWN_TYPE=process RDM_ARTIFACTS_DIR=target/i143-rshape/i143-rshape-x-process/mock_unowned_cell/estate RDM_RSHAPE_TIER=canonical RDM_RSHAPE_SEED=1431101 cargo test -p rafka-test-scenario --test rshape_burn_in mock_unowned_cell -- --exact"}));
    plant("rshape-definition-orphan-cell", "a cell that proves no scenario", &m, &reg);

    let first_adv = |m: &mut Value| -> usize { m["cells"].as_array().unwrap().iter().position(|c| s(c, "tier") == "adversarial").expect("the matrix holds an adversarial cell") };
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    m["cells"][i]["provider"] = json!("container");
    plant("rshape-definition-adversarial-cell", "an adversarial cell on the container provider", &m, &reg);
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    m["cells"][i]["covers"] = json!(["C99"]);
    plant("rshape-definition-adversarial-cell", "an adversarial cell covering a scenario that does not exist", &m, &reg);
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    m["cells"][i]["canon"] = json!("");
    plant("rshape-definition-adversarial-cell", "an adversarial cell citing no canon", &m, &reg);
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    m["cells"][i]["red"] = json!("");
    plant("rshape-definition-adversarial-cell", "a red adversarial cell naming no fork", &m, &reg);
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    let cmd = s(&m["cells"][i], "command").replace("RDM_RSHAPE_TIER=fast ", "");
    m["cells"][i]["command"] = json!(cmd);
    plant("rshape-definition-adversarial-cell", "an adversarial command that does not pin the reduced fixture", &m, &reg);
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    let mut twin = m["cells"][i].clone();
    twin["test"] = json!("mock_twin_cell");
    m["cells"].as_array_mut().unwrap().push(twin);
    plant("rshape-definition-adversarial-cell", "two adversarial cells in one job", &m, &reg);
    let mut m = matrix.clone();
    let i = first_adv(&mut m);
    m["cells"][i]["test"] = json!("mock_no_such_adversarial_cell");
    plant("rshape-definition-cell-without-test", "an adversarial cell naming a test no source holds", &m, &reg);

    write_result(
        "definition",
        "invariant-results.json",
        &json!({
            "scenarios": (1..=20).map(|n| format!("C{n}")).chain((1..=5).map(|n| format!("S{n}"))).collect::<Vec<_>>(),
            "cells": matrix["cells"].as_array().map(Vec::len),
            "providers": matrix["providers"],
            "consumer_clean": true,
            "planted_failures_named": planted,
        }),
    );
}
