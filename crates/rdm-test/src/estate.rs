//! Blackbox estate harness for the i143 e2e canaries.
//!
//! Everything here goes through public surfaces only (`docs/plans/i143-design.md`):
//! the `rdm-node-admin` process and its control API, the `rdm-rpc-probe`
//! binary and the JSONL evidence files. No internal map is read.

use rdm_client::binding::{BindingError, BindingSet, Expect, ProviderImage, Validated};
use rdm_client::LaunchKind as NodeKind;
use rdm_client::{birth_of_line, MeshNode, BIRTH_LINE};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Ownership metadata of one run (mesh product taxonomy, PRD §16).
#[derive(Debug, Clone)]
pub struct Owner {
    pub product: String,
    pub feature: String,
    pub subfeature: String,
    pub rung: String,
    pub provider: String,
    pub test: String,
}

/// `RDM_ARTIFACTS_DIR`, else this crate's `tests/artifacts`. A relative `RDM_ARTIFACTS_DIR`
/// is taken from the workspace root, where every registered acceptance command is run from: a
/// test binary itself runs in its crate's directory.
pub fn artifacts_root() -> PathBuf {
    match std::env::var("RDM_ARTIFACTS_DIR") {
        Ok(d) if Path::new(&d).is_absolute() => PathBuf::from(d),
        // Relative to the workspace root, resolved to its real path: the estate hands this path to
        // every admin (RDM_EVIDENCE_DIR) and bind-mounts it into every container, where a path
        // that walks `crates/rdm-test-scenario/../..` names nothing.
        Ok(d) => Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("the workspace root exists").join(d),
        Err(_) => Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/artifacts"),
    }
}

/// The fabrics' name index beside the per-fabric roots under a store_dir (R-FN1,
/// `rdm_admin::store_root::FABRIC_NAMES`): not a fabric root.
const FABRIC_NAMES: &str = "fabrics";

/// Directory holding the built binaries: `RDM_BIN_DIR`, else the cargo
/// target dir's profile directory this test binary was built into.
pub fn bin_dir() -> PathBuf {
    if let Ok(d) = std::env::var("RDM_BIN_DIR") {
        return PathBuf::from(d);
    }
    // A test binary lives in target/<profile>/deps; a bin in target/<profile>.
    let exe = std::env::current_exe().expect("own executable path");
    let dir = exe.parent().expect("exe dir").to_path_buf();
    if dir.file_name().is_some_and(|n| n == "deps") {
        dir.parent().expect("target/<profile>").to_path_buf()
    } else {
        dir
    }
}

/// What a started node-admin prints before it serves: the birth its Node RPC door serves under
/// (`rdm_client::BIRTH_LINE`), then its control base. The receiver gets the birth once the
/// base line is read, or the refusal of an admin that printed its base and no birth (or a birth that
/// does not decode). The reader goes on reading the pipe after it.
fn read_announcement(stdout: impl std::io::Read + Send + 'static) -> std::sync::mpsc::Receiver<Result<MeshNode, String>> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut birth: Option<Result<MeshNode, String>> = None;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(b) = birth_of_line(&line) {
                birth = Some(b);
            } else if let Some(base) = line.strip_prefix("RDM_NODE_ADMIN_API_BASE=") {
                let base = base.trim().to_string();
                let _ = tx.send(match birth.take() {
                    Some(Ok(b)) => Ok(b),
                    Some(Err(e)) => Err(e),
                    None => Err(format!("printed its control base {base} and no birth ({BIRTH_LINE}) before it")),
                });
            }
        }
    });
    rx
}

/// Start a `rdm-node-admin` with `env` and wait for what it prints before it serves: the birth it
/// runs under.
fn spawn_admin(exe: &Path, env: &[(&str, String)], what: &str) -> (Child, MeshNode) {
    let mut cmd = Command::new(exe);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap_or_else(|e| panic!("spawn {what}: {e}"));
    let rx = read_announcement(child.stdout.take().unwrap());
    let refusal = match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Ok(birth)) => return (child, birth),
        Ok(Err(e)) => format!("{what} {e}"),
        Err(_) => format!("{what} never advertised RDM_NODE_ADMIN_API_BASE within 30 s"),
    };
    // No Estate exists yet to stop it: the process is stopped here, before the refusal.
    let status = child.try_wait().ok().flatten().map(|s| s.to_string()).unwrap_or_else(|| "still running; killed".into());
    let _ = child.kill();
    let _ = child.wait();
    panic!("{refusal} (pid {}, {status})", child.id());
}

fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker").args(args).output().map_err(|e| format!("docker {}: {e}", args.join(" ")))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("docker {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

fn current_user() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |k: &str| status.lines().find_map(|l| l.strip_prefix(k)).and_then(|v| v.split_whitespace().next().map(String::from));
    format!("{}:{}", field("Uid:").expect("Uid"), field("Gid:").expect("Gid"))
}

fn socket_group() -> String {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/var/run/docker.sock").map(|m| m.gid().to_string()).expect("the Docker daemon's socket")
}

/// A new Fabric id in its canonical form: 60 random bits as 12 lowercase Crockford characters.
/// The harness mints it only for a container fabric, whose network must exist before its first
/// admin; the admin takes it as given (`RDM_FABRIC_ID`) and validates it.
fn mint_fabric_id() -> String {
    const CROCKFORD: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom").and_then(|mut f| std::io::Read::read_exact(&mut f, &mut bytes)).expect("/dev/urandom");
    let v = u64::from_le_bytes(bytes) >> 4;
    (0..12).map(|i| CROCKFORD[((v >> (5 * (11 - i))) & 0x1f) as usize] as char).collect()
}

/// The empty runtime image every node container runs from (the provider's `RUNTIME_IMAGE`).
pub const RUNTIME_IMAGE: &str = crate::container_faults::RUNTIME_IMAGE;

/// Start the Day-0 node-admin of a new container fabric in a container on that fabric's own
/// network: the Fabric id is minted here so the network exists before the admin does, the admin
/// is given an address on it, and it drives the host's Docker daemon through its socket. The
/// returned child is the attached `docker run`; the container is labelled like every container of
/// the fabric, so the estate stops and removes it with them.
fn spawn_admin_in_container(env: Vec<(&str, String)>, root: &Path, evidence: &Path, node: &str, exe: &Path, read_dirs: &[PathBuf]) -> (Child, MeshNode, String) {
    spawn_admin_in_container_within(env, root, evidence, node, exe, read_dirs, ADVERTISE_WITHIN, "docker").unwrap_or_else(|e| panic!("{}", e.message))
}

/// A Day-0 container given up on: the fabric it was minted for (already removed) and why.
#[derive(Debug)]
pub struct GiveUp {
    pub fabric_id: String,
    pub message: String,
}

/// How long the Day-0 container has, from the first Docker call, to advertise its API base.
const ADVERTISE_WITHIN: Duration = Duration::from_secs(30);

/// Remove everything labelled `rafka.fabric=<fabric_id>` and that fabric's network: each
/// container's output is kept in `artifacts` first when given. Returns what it could not remove.
///
/// A node-admin of the fabric makes containers through the Docker socket, so the admins are
/// removed first: nothing then starts a new container. A create the daemon already accepted from
/// one can still commit after the admin is gone; the listing is repeated until it is empty, so
/// that container is removed too and the network is removed with nothing attached.
pub fn remove_fabric(fabric_id: &str, artifacts: Option<&Path>) -> Vec<String> {
    let mut left = Vec::new();
    let list = || -> Vec<(String, String)> {
        let out = Command::new("docker").args(["ps", "-a", "--no-trunc", "--filter", &format!("label=rafka.fabric={fabric_id}"), "--format", "{{.Label \"rafka.node\"}} {{.ID}}"]).output();
        let mut v: Vec<(String, String)> = out.map(|o| String::from_utf8_lossy(&o.stdout).lines().filter_map(|l| l.split_once(' ').map(|(n, i)| (n.to_string(), i.to_string()))).collect()).unwrap_or_default();
        v.sort_by_key(|(n, _)| !n.contains(".admin."));
        v
    };
    let mut kept: std::collections::HashSet<String> = std::collections::HashSet::new();
    let remove = |node: &str, id: &str, kept: &mut std::collections::HashSet<String>, left: &mut Vec<String>| {
        if let (Some(dir), true) = (artifacts, kept.insert(id.to_string())) {
            if let Ok(logs) = Command::new("docker").args(["logs", id]).output() {
                let _ = std::fs::write(dir.join(format!("{node}.{}.container.log", &id[..12.min(id.len())])), [&logs.stdout[..], &logs.stderr[..]].concat());
            }
        }
        if let Err(e) = docker(&["rm", "-f", id]) {
            if !e.contains("No such container") {
                left.push(e);
            }
        }
    };
    for (node, id) in list().iter().filter(|(n, _)| n.contains(".admin.")) {
        remove(node, id, &mut kept, &mut left);
    }
    for _ in 0..40 {
        let rest = list();
        if rest.is_empty() {
            break;
        }
        for (node, id) in &rest {
            remove(node, id, &mut kept, &mut left);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    if let Err(e) = docker(&["network", "rm", &format!("rafka-{fabric_id}")]) {
        if !e.contains("No such network") && !e.contains("not found") {
            left.push(e);
        }
    }
    left
}

/// The Day-0 container of a new fabric, created and started so that nothing appears after a
/// give-up: `docker create` is awaited to completion (never abandoned in flight), then the
/// container is started attached. `program` is the Docker client for those two calls. When `within` passes before the API base is advertised, the
/// start is killed and everything labelled with the fabric, plus its network, is removed.
pub fn spawn_admin_in_container_within(mut env: Vec<(&str, String)>, root: &Path, evidence: &Path, node: &str, exe: &Path, read_dirs: &[PathBuf], within: Duration, program: &str) -> Result<(Child, MeshNode, String), GiveUp> {
    let begun = Instant::now();
    let fabric_id = mint_fabric_id();
    let network = format!("rafka-{fabric_id}");
    if docker(&["image", "inspect", RUNTIME_IMAGE]).is_err() {
        let mut c = Command::new("docker").args(["import", "-", RUNTIME_IMAGE]).stdin(Stdio::piped()).stdout(Stdio::null()).spawn().expect("docker import");
        c.stdin.take().unwrap().write_all(&[0u8; 1024]).expect("an empty tar archive");
        assert!(c.wait().unwrap().success(), "docker import {RUNTIME_IMAGE}");
    }
    docker(&["network", "create", "--label", &format!("rafka.fabric={fabric_id}"), &network]).unwrap_or_else(|e| panic!("the fabric network: {e}"));
    let subnet = docker(&["network", "inspect", "--format", "{{range .IPAM.Config}}{{.Subnet}}{{end}}", &network]).unwrap();
    let (base, prefix) = subnet.split_once('/').map(|(b, p)| (b.parse::<std::net::Ipv4Addr>().unwrap(), p.parse::<u32>().unwrap())).unwrap_or_else(|| panic!("network {network} subnet {subnet}"));
    // The network's last node address: the provider allocates upward from the gateway.
    let ip = std::net::Ipv4Addr::from(u32::from(base) + (1u32 << (32 - prefix)) - 2);
    let _ = std::fs::write(root.join("fabric_id"), &fabric_id);
    env.push(("RDM_FABRIC_ID", fabric_id.clone()));
    env.push(("RDM_NODE_ADMIN_API_BIND", format!("{ip}:20000")));
    // A native child inherits the harness's cadence. Give the container the same declared
    // values, retaining an explicit value supplied for this birth over its ambient default.
    for k in ["OTEL_EXPORTER_OTLP_ENDPOINT", "RUST_LOG", "RDM_STALENESS_MS", "RDM_GOSSIP_INTERVAL_MS", "RDM_BACKBONE_INTERVAL_MS", "RAFKA_STALENESS_MS", "RAFKA_GOSSIP_INTERVAL_MS", "RAFKA_BACKBONE_INTERVAL_MS"] {
        if !env.iter().any(|(name, _)| *name == k) {
            if let Ok(v) = std::env::var(k) {
                env.push((k, v));
            }
        }
    }
    let bin = bin_dir().display().to_string();
    let root_for_mounts = root.to_path_buf();
    let (root, evidence) = (root.display().to_string(), evidence.display().to_string());
    let mut args: Vec<String> = vec![
        "create".into(), "--name".into(), format!("rafka-{node}-{fabric_id}"),
        "--label".into(), format!("rafka.fabric={fabric_id}"), "--label".into(), format!("rafka.node={node}"),
        "--network".into(), network, "--ip".into(), ip.to_string(),
        "-v".into(), format!("{bin}:{bin}:ro"), "-v".into(), format!("{root}:{root}"), "-v".into(), format!("{evidence}:{evidence}"),
        "-v".into(), "/var/run/docker.sock:/var/run/docker.sock".into(),
        // As this user, in the socket's group: what it writes on the host stays the user's.
        "--user".into(), current_user(), "--group-add".into(), socket_group(),
        // It reads its births' socket tables by their host pids.
        "--pid".into(), "host".into(),
        "-e".into(), format!("HOME={root}"),
    ];
    for m in ["/lib", "/lib64", "/usr"].iter().filter(|m| Path::new(m).exists()) {
        args.extend(["-v".into(), format!("{m}:{m}:ro")]);
    }
    // Explicit bindings: the directories the admin reads the binding file and executables from.
    for d in read_dirs.iter().filter(|d| **d != bin_dir() && !d.starts_with(&root_for_mounts)) {
        args.extend(["-v".into(), format!("{}:{}:ro", d.display(), d.display())]);
    }
    for (k, v) in &env {
        args.extend(["-e".into(), format!("{k}={v}")]);
    }
    args.push(RUNTIME_IMAGE.into());
    args.push(exe.display().to_string());
    let name = format!("rafka-{node}-{fabric_id}");
    let give_up = |why: String, child: Option<Child>| -> GiveUp {
        if let Some(mut c) = child {
            let _ = c.kill();
            let _ = c.wait();
        }
        let state = docker(&["inspect", "--format", "status={{.State.Status}} created={{.Created}} started={{.State.StartedAt}} exit={{.State.ExitCode}}", &name]).unwrap_or_else(|e| e);
        let logs = Command::new("docker").args(["logs", "--tail", "20", &name]).output().map(|o| format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr))).unwrap_or_default();
        let left = remove_fabric(&fabric_id, None);
        let removed = if left.is_empty() { "everything labelled with the fabric, and its network, removed".to_string() } else { format!("NOT removed: {left:?}") };
        GiveUp { fabric_id: fabric_id.clone(), message: format!("the containerised Day-0 node-admin {name} {why} ({state}; {removed}); last log lines:\n{logs}") }
    };
    // `docker create` runs to completion on its own thread: it is waited for, never abandoned in
    // flight, so the container it commits exists before any removal runs.
    let (ctx, crx) = std::sync::mpsc::channel();
    let program_owned = program.to_string();
    let creator = std::thread::spawn(move || {
        let t = Instant::now();
        let out = Command::new(&program_owned).args(&args).output();
        let _ = ctx.send((out, t.elapsed()));
    });
    let created = crx.recv_timeout(within.saturating_sub(begun.elapsed()));
    let (out, took) = match created {
        Ok(v) => v,
        Err(_) => {
            let _ = creator.join();
            return Err(give_up(format!("was not created within {within:?}"), None));
        }
    };
    let _ = creator.join();
    eprintln!("[container] docker create of {name} took {took:?}");
    match out {
        Ok(o) if o.status.success() => {}
        Ok(o) => return Err(give_up(format!("docker create failed: {}", String::from_utf8_lossy(&o.stderr).trim()), None)),
        Err(e) => return Err(give_up(format!("docker create: {e}"), None)),
    }
    let started = Instant::now();
    let mut child = Command::new(program).args(["start", "-a", &name]).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().expect("docker start the Day-0 node-admin");
    let rx = read_announcement(child.stdout.take().unwrap());
    let birth = match rx.recv_timeout(within.saturating_sub(begun.elapsed())) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(give_up(e, Some(child))),
        Err(_) => return Err(give_up(format!("never advertised RDM_NODE_ADMIN_API_BASE within {within:?}"), Some(child))),
    };
    eprintln!("[container] {name} advertised its birth {} {:?} after docker start", birth.name, started.elapsed());
    Ok((child, birth, fabric_id))
}

pub fn binary(name: &str) -> PathBuf {
    let p = bin_dir().join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        p.exists(),
        "RED: binary `{name}` is not built at {} — the generic Mesh product does not exist yet \
         (node-admin: i143.e1/e2; rpc node + probe: i143.e7.s3)",
        p.display()
    );
    if let Err(why) = binary_is_fresh(name, &p) {
        panic!("{why}");
    }
    p
}

/// The package that builds `name`, for the rebuild command a refusal names.
pub(crate) fn package_of(name: &str) -> &'static str {
    match name {
        "rdm-node-admin" => "rdm-admin",
        "rdm-rpc-probe" | "rdm-rpc-node" | "faulted-node-admin" => "rdm-test",
        "rdm-node" => "rdm-test",
        _ => "<the package of this binary>",
    }
}

/// Whether the binary at `exe` was built from the source this checkout holds now. Cargo writes beside every
/// binary a dep-info file (`<name>.d`) listing each source file the binary was compiled from; the binary is
/// stale when any of them is newer than it. A binary with no dep-info file beside it (copied in from another
/// build, as a consumer's executables are, and judged by their recorded hashes instead) has no fingerprint here
/// and is not judged. The refusal names the binary, the file that changed and the rebuild command.
pub fn binary_is_fresh(name: &str, exe: &Path) -> Result<(), String> {
    let dep_info = exe.with_extension("d");
    let Ok(text) = std::fs::read_to_string(&dep_info) else { return Ok(()) };
    let built = std::fs::metadata(exe).and_then(|m| m.modified()).map_err(|e| format!("{name} at {}: {e}", exe.display()))?;
    // `target: dep dep dep` with a space in a path escaped as `\ `.
    let deps = text.lines().next().and_then(|l| l.split_once(": ")).map(|(_, d)| d.to_string()).unwrap_or_default();
    let mut newest: Option<(std::time::SystemTime, String)> = None;
    let mut cur = String::new();
    let mut chars = deps.chars().peekable();
    let mut paths = Vec::new();
    while let Some(c) = chars.next() {
        match c {
            '\\' if chars.peek() == Some(&' ') => {
                cur.push(' ');
                chars.next();
            }
            ' ' => {
                if !cur.is_empty() {
                    paths.push(std::mem::take(&mut cur));
                }
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        paths.push(cur);
    }
    for path in paths {
        if let Ok(m) = std::fs::metadata(&path).and_then(|m| m.modified()) {
            if newest.as_ref().is_none_or(|(t, _)| m > *t) {
                newest = Some((m, path));
            }
        }
    }
    match newest {
        Some((changed, path)) if changed > built => {
            let age = changed.duration_since(built).map(|d| d.as_secs()).unwrap_or(0);
            Err(format!(
                "REFUSED: {name} at {} was built before {path} changed ({age} s after the binary); this checkout's source is not the binary's. Rebuild with `cargo build -p {} --bin {name}`",
                exe.display(),
                package_of(name)
            ))
        }
        _ => Ok(()),
    }
}

/// Poll `check` until it yields `Some`, failing at `deadline` with `what`.
/// Recovery is inferred only from observed state, never from elapsed time.
pub async fn wait_for<T, F, Fut>(what: &str, within: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + within;
    loop {
        if let Some(v) = check().await {
            return v;
        }
        if Instant::now() >= deadline {
            if let Some(v) = check().await {
                return v;
            }
            panic!("timed out after {within:?} waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `check` until it yields `Ok`, failing at `deadline` with `what` and the last `Err` it
/// yielded: a wait that times out names what it last saw, never only what it wanted.
pub async fn wait_for_observed<T, F, Fut>(what: &str, within: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let deadline = Instant::now() + within;
    loop {
        let last = match check().await {
            Ok(v) => return v,
            Err(seen) => seen,
        };
        if Instant::now() >= deadline {
            if let Ok(v) = check().await {
                return v;
            }
            panic!("timed out after {within:?} waiting for: {what}; last seen: {last}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The environment variable naming an external consumer's built executable directory. When it is
/// set, every node-admin and node of a run launches from the consumer's executables through
/// [`Estate::bootstrap_on`]; the testkit binaries are refused.
pub const ENV_CONSUMER_BIN_DIR: &str = "RDM_RSHAPE_CONSUMER_BIN_DIR";

fn refuse_built_ins_in_consumer_mode(what: &str) {
    if let Ok(d) = std::env::var(ENV_CONSUMER_BIN_DIR) {
        panic!("REFUSED {what}: {ENV_CONSUMER_BIN_DIR}={d} selects an external consumer's executables, which launch only through Estate::bootstrap_on with an explicit binding set; the testkit binaries are never a fallback");
    }
}

/// The binding set of a consumer build: its build manifest (`executable_map` kind -> file name,
/// `binaries` file name -> sha256, `candidate_sha`) resolved against `bin_dir`. `container` binds
/// every executable to the provider's image.
pub fn binding_set_from_build_manifest(manifest: &Path, bin_dir: &Path, container: bool) -> Result<BindingSet, String> {
    let v: Value = serde_json::from_slice(&std::fs::read(manifest).map_err(|e| format!("consumer build manifest {}: {e}", manifest.display()))?).map_err(|e| format!("consumer build manifest {}: {e}", manifest.display()))?;
    let sha = v["candidate_sha"].as_str().ok_or_else(|| format!("{}: no candidate_sha", manifest.display()))?.to_string();
    let map = v["executable_map"].as_object().ok_or_else(|| format!("{}: no executable_map", manifest.display()))?;
    let mut bindings = Vec::new();
    for (kind, file) in map {
        let file = file.as_str().ok_or_else(|| format!("{}: executable_map.{kind} is not a file name", manifest.display()))?;
        let sha256 = v["binaries"][file].as_str().ok_or_else(|| format!("{}: binaries names no sha256 for {file}", manifest.display()))?.to_string();
        bindings.push(rdm_client::binding::Binding { launch_id: kind.clone(), executable: bin_dir.join(file), sha256, image: container.then(|| RUNTIME_IMAGE.to_string()) });
    }
    Ok(BindingSet { candidate: rdm_client::binding::Candidate { sha, build: v["consumer_source_sha256"].as_str().unwrap_or_default().to_string() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings })
}

/// The explicit executable set an estate launches from (the external-consumer seam, the faulted
/// node-admin): the validated set, the file it is written to (the run's manifest names it), and the
/// candidate it was validated against. Every Build the estate submits names each created path's
/// executable from the set ([`crate::executables`]); no node-admin is handed the set.
#[derive(Debug, Clone)]
pub struct ExternalLaunch {
    pub validated: Validated,
    pub file: PathBuf,
    pub candidate: String,
    /// The fabric's provider is the container provider.
    pub container: bool,
}

impl ExternalLaunch {
    /// The environment every node-admin of the estate gets: `RDM_BIN_DIR`, where `rdm-node-admin`
    /// itself is found, the set's node-admin's directory (never a launch source).
    fn env(&self) -> Vec<(&'static str, String)> {
        let admin = self.admin_exe();
        vec![("RDM_BIN_DIR", admin.parent().expect("the node-admin executable is a file in a directory").display().to_string())]
    }

    fn admin_exe(&self) -> PathBuf {
        self.validated.resolve(NodeKind::NodeAdmin).expect("node_admin is bound (validated)").executable
    }

    /// The directories the set's executables live in: a container fabric's `RDM_EXECUTABLE_DIRS`.
    fn read_dirs(&self) -> Vec<PathBuf> {
        self.validated.executable_dirs().into_iter().collect()
    }
}

/// `RDM_EXECUTABLE_DIRS` of a container estate's node-admins (proposal executable-bindings-at-create.md
/// §2): the directories every executable it names lives in, mounted read-only at their host paths; the
/// fabric-primary refuses an executable outside them. `dirs` is the external set's, else the testkit's
/// one binary directory.
fn executable_dirs_env(dirs: &[PathBuf]) -> (&'static str, String) {
    let dirs: Vec<PathBuf> = if dirs.is_empty() { vec![bin_dir()] } else { dirs.to_vec() };
    ("RDM_EXECUTABLE_DIRS", dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(","))
}

/// What an estate owns on the host beyond its processes: its root directory under the temp dir
/// and the reaper that outlives a SIGKILLed test binary. Dropping it (a finished estate, a panic
/// anywhere in birth) stops the reaper and removes the root; the reaper does the same when the
/// test binary dies without running any destructor.
/// This estate's own root: test name, process and a per-process sequence, so a test that ends one
/// estate and births another (a second estate) never shares a root. The scope of the estate being
/// replaced deletes its own root when it drops, and only its own.
/// The estate's one store root (R14 item 6, the process provider): `<estate root>/store`, handed to
/// every node-admin the estate starts and, through each launch, to every node of the fabric.
pub fn estate_store_dir(root: &Path) -> PathBuf {
    root.join("store")
}

/// A fresh root for one run of `test`: an estate, or a cell that starts a node-admin of its own.
pub fn estate_root(test: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("i143-{test}-{}-{n}", std::process::id()))
}

pub struct EstateScope {
    root: PathBuf,
    /// The estate's evidence directory under its root, and the directory it is kept in when the
    /// estate ends ([`EstateScope::keeping`]).
    keep: Option<(PathBuf, PathBuf)>,
    reaper: Option<Child>,
}

/// Where an estate's processes write their span evidence (`RDM_EVIDENCE_DIR`): `<root>/spans`, under
/// the temp dir with the rest of the estate, which the gate and `cells.sh` put on tmpfs. A process's
/// exit flush is bounded (telemetry.md "Exit"; Q60): written to a disk the gate's load holds, the spans
/// a signal stop closes last were lost at that bound (rdm#118). The estate keeps the directory in its
/// artifacts (`<artifacts>/spans`) when it ends, and the reaper does when the test binary is killed.
pub fn estate_evidence_dir(root: &Path) -> PathBuf {
    root.join("spans")
}

/// Copy every file under `from` into `to`, keeping its relative path: an estate's evidence kept in its
/// artifacts before its root is removed. A file that cannot be copied is named on stderr.
fn keep_evidence(from: &Path, to: &Path) {
    let _ = std::fs::create_dir_all(to);
    for e in std::fs::read_dir(from).into_iter().flatten().flatten() {
        let (src, dst) = (e.path(), to.join(e.file_name()));
        if src.is_dir() {
            keep_evidence(&src, &dst);
        } else if let Err(err) = std::fs::copy(&src, &dst) {
            eprintln!("estate: the evidence file {} was not kept in {}: {err}", src.display(), to.display());
        }
    }
}

/// The reaper: waits for the test binary to die, then kills every process whose environment names
/// this estate's root: `RDM_ESTATE_ROOT` (the bootstrap admin and the admins the estate restarted)
/// or a data dir under it, `RDM_DATA_DIR` (every node an admin launched: the process provider strips
/// every `RDM_*` of the launcher from the child, so its launch contract is the only mark it has),
/// removes the container fabric recorded in `<root>/fabric_id`, keeps the estate's evidence directory
/// (`$3`, when given) in its artifacts (`$4`), and removes the root. It runs in its own session, so a
/// gate that kills the test binary's process group leaves it running.
const REAPER: &str = r#"
owner=$1; root=$2; evidence=$3; kept=$4
while kill -0 "$owner" 2>/dev/null; do sleep 0.2; done
for pass in 1 2 3 4 5 6 7 8 9 10; do
  found=0
  for e in $(grep -laP "RDM_(ESTATE_ROOT=$root|DATA_DIR=$root/[^\x00]*)\x00" /proc/[0-9]*/environ 2>/dev/null); do
    p=${e#/proc/}; p=${p%/environ}
    [ "$p" = "$$" ] && continue
    kill -9 "$p" 2>/dev/null && found=1
  done
  [ $found = 0 ] && break
  sleep 0.2
done
fabric=$(cat "$root/fabric_id" 2>/dev/null)
if [ -n "$fabric" ]; then
  for pass in 1 2 3 4 5; do
    ids=$(docker ps -aq --filter "label=rafka.fabric=$fabric")
    [ -z "$ids" ] && break
    docker rm -f $ids >/dev/null 2>&1
    sleep 0.2
  done
  docker network rm "rafka-$fabric" >/dev/null 2>&1
fi
if [ -n "$evidence" ] && [ -d "$evidence" ]; then
  mkdir -p "$kept" && cp -a "$evidence/." "$kept/"
fi
rm -rf "$root"
"#;

impl EstateScope {
    /// The scope of `root`: its reaper starts now, and dropping the scope removes the root.
    pub fn begin(root: &Path) -> Self {
        Self::start(root, None)
    }

    /// [`Self::begin`] for an estate whose processes write their evidence to `evidence` under `root`:
    /// dropping the scope, or the reaper once the test binary is killed, copies it into `kept` before the
    /// root is removed.
    pub fn keeping(root: &Path, evidence: &Path, kept: &Path) -> Self {
        Self::start(root, Some((evidence.to_path_buf(), kept.to_path_buf())))
    }

    fn start(root: &Path, keep: Option<(PathBuf, PathBuf)>) -> Self {
        use std::os::unix::process::CommandExt;
        let (evidence, kept) = keep.as_ref().map(|(e, k)| (e.display().to_string(), k.display().to_string())).unwrap_or_default();
        let mut cmd = Command::new("sh");
        cmd.args(["-c", REAPER, "estate-reaper", &std::process::id().to_string(), &root.display().to_string(), &evidence, &kept]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        cmd.env_remove("RDM_ESTATE_ROOT");
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
        EstateScope { root: root.to_path_buf(), keep, reaper: cmd.spawn().ok() }
    }
}

impl Drop for EstateScope {
    fn drop(&mut self) {
        if let Some(mut r) = self.reaper.take() {
            let _ = r.kill();
            let _ = r.wait();
        }
        if let Some((evidence, kept)) = &self.keep {
            keep_evidence(evidence, kept);
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The witness that the fabric an estate runs has first reached ready (states.md "The
/// initial fabric before ready"), which every kill or terminate of one of its nodes takes. A
/// death during fabric start is not tested, and no chaos exercises it (Luke 2026-10-19, Q74): only an
/// estate that has waited for its fabric to reach ready hands one out ([`Estate::fabric_ready`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FabricReady(pub(crate) ());

impl FabricReady {
    /// The witness from one fabric read, `status` as `fabric.get` answers it. An interactive fault panel
    /// (the demo admin-ui's Chaos tab) reads the fabric before each kill and refuses the kill without
    /// one; a cell waits with [`Estate::until_fabric_ready`] instead, so its refusal is the same
    /// on every run.
    pub fn read(status: rdm_client::ScopeStatus) -> Option<FabricReady> {
        (status == rdm_client::ScopeStatus::Ready).then_some(FabricReady(()))
    }
}

pub struct Estate {
    pub owner: Owner,
    pub root: PathBuf,
    pub artifacts: PathBuf,
    /// Where the estate's processes write their spans while it runs ([`estate_evidence_dir`], under the
    /// root): read it live; once the estate has ended its files are in [`Self::kept_evidence`].
    pub evidence: PathBuf,
    /// The node-admin the estate enters at: the typed objects start at it, and a probe is given its birth
    /// ([`Self::entry_birth`]). [`Estate::enter`] changes it.
    pub admin: rdm_client::PathName,
    /// This estate's Fabric id. Control addresses are loopback ports that other estates on the
    /// host reuse once a runtime dies: an admin answers for this estate only when its view names
    /// this id (`Self::fabric_at`).
    pub fabric_id: String,
    bootstrap: Option<Child>,
    /// The explicit executable bindings this estate runs from, if it was born with any.
    pub external: Option<ExternalLaunch>,
    /// Where each created path's executable comes from: every Build the estate submits names it
    /// (`crate::executables`).
    pub executables: crate::executables::Executables,
    /// The seed of the run, recorded in the estate manifest once the run has one.
    seed: Option<u64>,
    /// Admins this estate restarted on their own data dirs.
    restarted: Vec<Child>,
    /// The handover of the typed objects (`crate::typed`): made once per entry admin.
    pub(crate) wire: tokio::sync::Mutex<Option<crate::typed::Wire>>,
    /// The births the admins this estate started printed, not yet learned by the typed objects
    /// (`crate::typed`, the handover learns each once).
    pub(crate) printed: std::sync::Mutex<Vec<MeshNode>>,
    /// The birth each live node-admin `path.name` holds now, as an admin this estate started printed it
    /// or a view the estate read lists it: the admins the estate reads views of, and the entry a probe is
    /// given ([`Self::entry_birth`]).
    pub(crate) births: std::sync::Mutex<std::collections::BTreeMap<rdm_client::PathName, MeshNode>>,
    /// Set once the estate has waited for its fabric to reach ready ([`Self::until_fabric_ready`]).
    pub(crate) ready: std::sync::OnceLock<FabricReady>,
    /// The incremental reader of the estate's `.spans.jsonl` files ([`Self::spans`], [`Self::spans_named`]).
    spans: std::sync::Mutex<SpanReader>,
    /// Dropped last: stops the reaper and removes the root.
    _scope: EstateScope,
}

impl Estate {
    /// Start the first node-admin of `fabric` (bootstrap selects the provider)
    /// and wait for the birth it prints.
    pub async fn bootstrap(owner: Owner, fabric: &str, mesh: &str) -> Self {
        refuse_built_ins_in_consumer_mode("Estate::bootstrap");
        Self::born(owner, fabric, mesh, None, &[]).await
    }

    /// [`Estate::bootstrap`], the first node-admin also given `extra_env` (a setting the fabric's
    /// admins read, such as `RDM_STOPPED_NODE_TTL`, handed on to every admin it launches). An entry
    /// replaces the estate's own value of its name, and an empty value leaves the name unset
    /// (`RDM_EVIDENCE_DIR` empty: no node writes span evidence, the load harness's evidence-off pass).
    pub async fn bootstrap_with_env(owner: Owner, fabric: &str, mesh: &str, extra_env: &[(&str, &str)]) -> Self {
        refuse_built_ins_in_consumer_mode("Estate::bootstrap_with_env");
        Self::born(owner, fabric, mesh, None, extra_env).await
    }

    /// Start the first node-admin of `fabric` on the set's `node_admin`, and name every created
    /// path's executable from the set (proposal executable-bindings-at-create.md §2: the bootstrap
    /// admin is itself alone and records what it runs; every other node is created by a request naming
    /// its executable): nothing the estate launches is taken from a directory with no sha. The set is
    /// validated against `candidate_sha`, the launch ids the run `required` and the provider's image
    /// BEFORE any file is made or any process started; a refusal is returned by name with nothing
    /// launched.
    pub async fn bootstrap_on(owner: Owner, fabric: &str, mesh: &str, set: &BindingSet, candidate_sha: &str, required: &[&str]) -> Result<Self, BindingError> {
        Self::bootstrap_on_with_env(owner, fabric, mesh, set, candidate_sha, required, &[]).await
    }

    /// [`Estate::bootstrap_on`], the first node-admin also given `extra_env` (a setting the fabric's
    /// admins read, such as `RDM_GOSSIP_INTERVAL_MS`).
    pub async fn bootstrap_on_with_env(owner: Owner, fabric: &str, mesh: &str, set: &BindingSet, candidate_sha: &str, required: &[&str], extra_env: &[(&str, &str)]) -> Result<Self, BindingError> {
        let validated = Self::validated(&owner, set, candidate_sha, required)?;
        Ok(Self::born(owner, fabric, mesh, Some((validated, candidate_sha.to_string())), extra_env).await)
    }

    fn validated(owner: &Owner, set: &BindingSet, candidate_sha: &str, required: &[&str]) -> Result<Validated, BindingError> {
        let image = if owner.provider == "container" { ProviderImage::Container(RUNTIME_IMAGE) } else { ProviderImage::Process };
        set.validate(&Expect { candidate_sha, required, provider_image: image })
    }

    async fn born(owner: Owner, fabric: &str, mesh: &str, external: Option<(Validated, String)>, extra_env: &[(&str, &str)]) -> Self {
        let artifacts = artifacts_root().join(&owner.feature).join(&owner.test);
        let _ = std::fs::remove_dir_all(&artifacts);
        let kept = artifacts.join("spans");
        std::fs::create_dir_all(&kept).unwrap();
        let root = estate_root(&owner.test);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(estate_store_dir(&root)).unwrap();
        let evidence = estate_evidence_dir(&root);
        std::fs::create_dir_all(&evidence).unwrap();
        let scope = EstateScope::keeping(&root, &evidence, &kept);
        let container = owner.provider == "container";
        let external = external.map(|(validated, candidate)| {
            let file = root.join("executable-bindings.json");
            std::fs::write(&file, serde_json::to_vec_pretty(validated.set()).unwrap()).unwrap();
            ExternalLaunch { validated, file, candidate, container }
        });
        let executables = match &external {
            Some(x) => crate::executables::Executables::Set(x.validated.set().clone()),
            None => crate::executables::Executables::testkit(&owner.provider),
        };

        let mut env = vec![
            ("MESH_SPAWN_TYPE", owner.provider.clone()),
            ("RDM_FABRIC", fabric.into()),
            ("RDM_MESH", mesh.into()),
            ("RDM_DATA_DIR", root.join(format!("{mesh}.admin.1")).display().to_string()),
            ("RDM_STORE_DIR", estate_store_dir(&root).display().to_string()),
            ("RDM_BIN_DIR", bin_dir().display().to_string()),
            ("RDM_EVIDENCE_DIR", evidence.display().to_string()),
            ("RDM_ESTATE_ROOT", root.display().to_string()),
            // The fabric's root the bootstrap admin is started with (fabric-certs.md): the testkit's.
            ("RDM_CA_SECRET", crate::test_certs::test_root_hex()),
        ];
        for (k, v) in extra_env {
            env.retain(|(name, _)| name != k);
            if !v.is_empty() {
                env.push((*k, v.to_string()));
            }
        }
        let admin_exe = match &external {
            Some(x) => {
                env.retain(|(k, _)| *k != "RDM_BIN_DIR");
                env.extend(x.env());
                x.admin_exe()
            }
            None => binary("rdm-node-admin"),
        };
        // A container fabric's Day-0 admin runs in a container of that fabric, so every admin of
        // the fabric is a container a successor can control in the same Docker domain.
        let (child, birth, container_fabric) = if owner.provider == "container" {
            let mut read_dirs = external.as_ref().map(ExternalLaunch::read_dirs).unwrap_or_default();
            // A cell that names the fabric's executable directories itself has each mounted into the
            // Day-0 container, as a launched admin's launch mounts them.
            match env.iter().find(|(k, _)| *k == "RDM_EXECUTABLE_DIRS").map(|(_, v)| v.clone()) {
                Some(named) => read_dirs.extend(named.split(',').filter(|d| !d.is_empty()).map(PathBuf::from)),
                None => env.push(executable_dirs_env(&read_dirs)),
            }
            spawn_admin_in_container(env, &root, &evidence, &format!("{mesh}.admin.1"), &admin_exe, &read_dirs)
        } else {
            let (c, b) = spawn_admin(&admin_exe, &env, "bootstrap node-admin");
            (c, b, String::new())
        };
        // A container fabric's id is the one minted for its network: the estate holds it from
        // birth, so a panic before the fabric answers still removes the fabric's containers.
        let mut estate = Self { owner, root, artifacts, evidence, admin: birth.name.clone(), fabric_id: container_fabric, bootstrap: Some(child), external, executables, seed: None, restarted: Vec::new(), wire: tokio::sync::Mutex::new(None), printed: std::sync::Mutex::new(Vec::new()), births: Default::default(), ready: std::sync::OnceLock::new(), spans: Default::default(), _scope: scope };
        estate.hold_printed(birth);
        estate.write_manifest();
        // Topology is accepted only by the fabric-primary: the bootstrap admin is one once it is
        // Ready and elected, not when it first answers. `fabric.get` at it names the role, the Fabric
        // and the current Build, which is the Day-0 Build it accepted at its start (its own mesh with
        // one node-admin, R-BI3).
        let (fabric_id, day_zero) = wait_for("the bootstrap admin holds the fabric", Duration::from_secs(30), || async {
            let f = estate.try_fabric_view().await.ok()?;
            f.fabric_primary.as_ref()?;
            Some(f.id.to_string()).filter(|i| !i.is_empty()).map(|id| (id, f.build_id))
        })
        .await;
        estate.fabric_id = fabric_id;
        // The estate started the Day-0 Build by starting its admin, so it follows that Build to its
        // Complete before a cell submits anything: a cell's first build.create is never refused for a
        // Build in progress it did not start. The Day-0 Build is Complete only from its own
        // ready (states.md "Resize and executable replacement": "as fabric.create's is").
        let day_zero = day_zero.unwrap_or_else(|| panic!("the bootstrap admin {} holds fabric {} and no current Build: its Day-0 Build is accepted before it serves", estate.admin, estate.fabric_id));
        let id = rdm_client::BuildId::parse(&day_zero).unwrap_or_else(|e| panic!("fabric.get names the current Build {day_zero}, not a Build id: {e}"));
        let mut stream = estate.carrier().await.resubmit(&id, 1).await.unwrap_or_else(|e| panic!("the bootstrap admin does not stream its Day-0 Build {id}: {e}"));
        let mut frames = Vec::new();
        loop {
            let frame = match stream.next().await {
                Some(frame) => frame.unwrap_or_else(|e| panic!("the stream of the Day-0 Build {id} broke before its work was done: {e}")),
                None => panic!("the stream of the Day-0 Build {id} ended without its Complete: {frames:?}"),
            };
            frames.push(frame.clone());
            match frame {
                rdm_client::BuildFrame::Complete { .. } => break,
                rdm_client::BuildFrame::Failed { attempt, operation, step, reason, .. } => panic!("the Day-0 Build {id} attempt {attempt} failed at {operation} {step}: {reason}"),
                rdm_client::BuildFrame::FabricPrimaryMoved { fabric_primary } => panic!("the fabric-primary role moved ({fabric_primary:?}) while the Day-0 Build {id} ran"),
                _ => {}
            }
        }
        estate
    }

    fn write_manifest(&self) {
        let o = &self.owner;
        self.artifact(
            "manifest.json",
            &json!({
                "product": o.product, "feature": o.feature, "subfeature": o.subfeature,
                "rung": o.rung, "provider": o.provider, "test": o.test,
                "seed": self.seed, "entry_admin": self.admin.to_string(),
                "executable_bindings": self.external.as_ref().map(|x| json!({ "mode": "explicit", "binding_file": x.file, "receipt": x.validated.receipt() })).unwrap_or_else(|| json!({ "mode": "testkit" })),
            }),
        );
    }

    /// Record the run's seed in the estate manifest.
    pub fn set_seed(&mut self, seed: u64) {
        self.seed = Some(seed);
        self.write_manifest();
    }

    pub fn artifact(&self, name: &str, v: &Value) {
        std::fs::write(self.artifacts.join(name), serde_json::to_vec_pretty(v).unwrap()).unwrap();
    }

    pub fn append_ledger(&self, entry: &Value) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.artifacts.join("rpc-ledger.jsonl"))
            .unwrap();
        writeln!(f, "{entry}").unwrap();
    }

    /// The estate's store dir (`RDM_STORE_DIR`), `<estate root>/store`: what every admin it starts is
    /// given.
    pub fn store_dir(&self) -> PathBuf {
        estate_store_dir(&self.root)
    }

    /// The fabric's store root, `<estate store_dir>/<fabric id>/` (R14): the one fabric directory the
    /// estate's store_dir holds beside the fabrics' name index (`fabrics/`, R-FN1).
    pub fn fabric_store_root(&self) -> PathBuf {
        let store = estate_store_dir(&self.root);
        let fabrics: Vec<PathBuf> = std::fs::read_dir(&store)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir() && p.file_name().is_none_or(|n| n != FABRIC_NAMES))
            .collect();
        match fabrics.as_slice() {
            [one] => one.clone(),
            other => panic!("the estate's store_dir {} holds {} fabric roots, not one: {other:?}", store.display(), other.len()),
        }
    }

    /// The latest record of `node_id` in the root's `nodes.jsonl` (R-SR1), as
    /// `{"format", "key", "record"}`; `None` while there is none, or while the file cannot be read whole.
    pub fn node_record(&self, node_id: &str) -> Option<serde_json::Value> {
        rdm_admin::store_log::read_record(&self.fabric_store_root().join(rdm_admin::store_root::NODES_FILE), "node-record", Some(node_id))
    }

    /// The node's raw connection log on the fabric's store root: every fact it wrote to the root's
    /// `connections.jsonl`, oldest first, each as JSON (none while the file cannot be read whole).
    pub fn connection_history(&self, node: &str) -> Vec<serde_json::Value> {
        rdm_node::storage::connection_history(&self.fabric_store_root(), node).unwrap_or_default().iter().filter_map(|f| serde_json::to_value(f).ok()).collect()
    }

    /// The pid of `node`'s runtime: its process (process provider: its `deployment.json`), or its
    /// container's init as the host sees it (container provider).
    pub async fn pid_of(&self, node: &str) -> u64 {
        if self.owner.provider == "container" {
            let id = self.container_of(node).await.unwrap_or_else(|| panic!("{node}: no running container of fabric {}", self.fabric_id));
            return docker(&["inspect", "--format", "{{.State.Pid}}", &id]).ok().and_then(|p| p.parse().ok()).filter(|p| *p > 0).unwrap_or_else(|| panic!("{node}: container {id} has no running init"));
        }
        let dir = self.data_dir(node).await;
        let d: Value = serde_json::from_slice(&std::fs::read(format!("{dir}/deployment.json")).unwrap()).unwrap();
        d["pid"].as_u64().unwrap_or_else(|| panic!("{node}: no pid in {d}"))
    }

    /// The witness a kill or terminate of one of this fabric's nodes takes ([`FabricReady`]): refused by
    /// name, a panic naming the cell, until the estate has waited for the fabric to reach ready.
    pub fn fabric_ready(&self) -> FabricReady {
        *self.ready.get().unwrap_or_else(|| {
            panic!(
                "{}: a kill or terminate of a node before the estate has waited for its fabric to reach ready is refused: a death during fabric start is not tested (Q74, Luke 2026-10-19); the cell waits for the fabric ready (until_fabric_ready) before it kills",
                self.owner.test
            )
        })
    }

    /// SIGKILL `node`'s runtime (a fault: it flushes nothing), once the fabric has first been
    /// ready ([`Self::fabric_ready`]).
    pub async fn kill_node(&self, node: &str) {
        self.fabric_ready();
        let pid = self.pid_of(node).await;
        let ok = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success();
        assert!(ok, "kill -9 {pid} ({node})");
    }

    /// Run one probe invocation and record it in the RPC ledger.
    pub fn probe(&self, args: &[&str]) -> Value {
        self.probe_handle().run(&self.entry_birth(), args).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Estate::probe`], with the probe's stderr: a cell that reads the probe's own span file reads a
    /// loss its exit named there.
    pub fn probe_with_stderr(&self, args: &[&str]) -> (Value, String) {
        self.probe_handle().run_with_stderr(&self.entry_birth(), args).unwrap_or_else(|e| panic!("{e}"))
    }

    /// What a probe invocation needs of this estate, owned: a traffic task issues probes while the
    /// estate itself is borrowed by the scenario's control loop.
    pub fn probe_handle(&self) -> ProbeHandle {
        // A container fabric's nodes are reachable from the host through its network's gateway.
        let bind = (self.owner.provider == "container")
            .then(|| docker(&["network", "inspect", "--format", "{{range .IPAM.Config}}{{.Gateway}}{{end}}", &format!("rafka-{}", self.fabric_id)]).ok())
            .flatten();
        ProbeHandle { evidence: self.evidence.clone(), artifacts: self.artifacts.clone(), bind }
    }

    /// Every span every process of this estate wrote, ordered by start time.
    ///
    /// Reads only complete lines appended since the last call; a file that shrank or was replaced is
    /// read from `0`. A `build_id` span attribute formatted as `bid_xxxx-xxxx-xxxx` (`R-BI5a`) is
    /// stripped to its 12-character bare id for scenario comparisons against REST/RPC `build_id`
    /// strings; the `.spans.jsonl` files on disk keep the raw `bid_xxxx-xxxx-xxxx` rendering.
    pub fn spans(&self) -> Vec<Value> {
        self.spans.lock().unwrap().all(&self.evidence)
    }

    /// [`Self::spans`], keeping only spans whose `name` is one of `names` and deciding that from the
    /// line's bytes before parsing it into a [`Value`]. Each name set keeps its own file offsets and
    /// kept spans.
    pub fn spans_named(&self, names: &[&str]) -> Vec<Value> {
        self.spans.lock().unwrap().named(&self.evidence, names)
    }

    /// Drop the span evidence written so far, for a run that outlasts the room its evidence takes
    /// (load-testing.md M10, a 4-hour soak): every `.spans.jsonl` file's bytes since the last drop, by
    /// file name, then each file truncated and the reader started over. A span a node is writing as its
    /// file is cut may lose its first bytes; the reader skips a line that is not JSON.
    pub fn drop_evidence(&self) -> Vec<(String, u64)> {
        let mut reader = self.spans.lock().unwrap();
        let mut sizes = Vec::new();
        for e in std::fs::read_dir(&self.evidence).into_iter().flatten().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".spans.jsonl") {
                continue;
            }
            sizes.push((name, e.metadata().map(|m| m.len()).unwrap_or(0)));
            let _ = std::fs::OpenOptions::new().write(true).open(e.path()).and_then(|f| f.set_len(0));
        }
        *reader = SpanReader::default();
        sizes
    }

    /// Where the estate's evidence is kept when it ends: `<artifacts>/spans`.
    pub fn kept_evidence(&self) -> PathBuf {
        self.artifacts.join("spans")
    }

    /// The trace URL for `trace_id` when a Jaeger query base is configured.
    pub fn record_trace_url(&self, trace_id: &str) {
        let base = std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://localhost:16686".into());
        std::fs::write(self.artifacts.join("trace-url.txt"), format!("{base}/trace/{trace_id}\n")).unwrap();
    }

    /// Stop the whole estate through its own provider and wait until every runtime of it has exited:
    /// the test's teardown, not a fabric operation (no op on the wire begins it). Every admin the estate
    /// started, every runtime an admin launched (the pid its `deployment.json` names) and every
    /// container of its Fabric is sent SIGTERM at once, and each leaves as a local stop does
    /// (`rdm.mesh.node.delete.via-signal`), its evidence flushed as it exits, so read `spans()` after
    /// this. A runtime an admin launched while the others were leaving is told the same. One still
    /// running when the bound passes is named and killed, so a stop never hangs on it: the pids so
    /// killed are returned, empty when every runtime ended by its own stop.
    pub async fn stop(&mut self) -> Vec<u32> {
        let mut told = self.terminate(&[]);
        let until = Instant::now() + Duration::from_secs(30);
        let mut killed = Vec::new();
        loop {
            for c in self.restarted.iter_mut().chain(self.bootstrap.as_mut()) {
                let _ = c.try_wait();
            }
            told.extend(self.terminate(&told));
            let running: Vec<u32> = told.iter().copied().filter(|p| runs(*p)).collect();
            let containers = self.live_containers();
            if running.is_empty() && containers.is_empty() {
                break;
            }
            if Instant::now() >= until {
                eprintln!("estate stop: still running when the stop bound passed, killed: pids {running:?}, containers {containers:?}");
                for p in &running {
                    let _ = Command::new("kill").args(["-9", &p.to_string()]).status();
                }
                for (_, id) in &containers {
                    let _ = docker(&["kill", id]);
                }
                killed = running;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for mut c in std::mem::take(&mut self.restarted).into_iter().chain(self.bootstrap.take()) {
            let _ = c.kill();
            let _ = c.wait();
        }
        killed
    }

    /// Send SIGTERM to every process of this estate not in `told` (the admins it started and every
    /// runtime an admin launched) and to every container of its Fabric: each leaves as a local stop
    /// does. The pids it signalled.
    fn terminate(&self, told: &[u32]) -> Vec<u32> {
        let pids: Vec<u32> = self.restarted.iter().map(Child::id).chain(self.bootstrap.as_ref().map(Child::id)).chain(self.live_runtimes().into_iter().map(|(_, p)| p)).filter(|p| !told.contains(p) && runs(*p)).collect();
        for p in &pids {
            let _ = Command::new("kill").args(["-TERM", &p.to_string()]).status();
        }
        if told.is_empty() {
            for (_, id) in self.live_containers() {
                let _ = docker(&["kill", "--signal", "TERM", &id]);
            }
        }
        pids
    }

    /// The bootstrap admin's process id while the estate still holds it. The bootstrap runtime is
    /// adopted on day 0, so it has no provider `deployment.json`.
    pub fn bootstrap_pid(&self) -> Option<u32> {
        self.bootstrap.as_ref().map(|c| c.id())
    }

    /// The process ids of the node-admins this estate started after day 0 ([`Self::restart_admin_with`]),
    /// in the order it started them.
    pub fn restarted_pids(&self) -> Vec<u32> {
        self.restarted.iter().map(Child::id).collect()
    }

    /// The bootstrap admin's data dir.
    pub fn bootstrap_data_dir(&self, mesh: &str) -> PathBuf {
        self.root.join(format!("{mesh}.admin.1"))
    }

    /// Start a node-admin on `data_dir` it ran on before: it finds its own row in nodes.storage and
    /// restarts as the same logical node. Only the caller's environment is given (provider,
    /// binaries, evidence); identity, Mesh and Fabric come from its storage. Returns its
    /// `path.name`.
    pub fn restart_admin(&mut self, data_dir: &Path) -> rdm_client::PathName {
        self.restart_admin_with(data_dir, &[])
    }

    /// [`Self::restart_admin`] with more caller environment (the recovery flags
    /// `RDM_MESH_PRIMARY` and `RDM_FABRIC_PRIMARY`).
    pub fn restart_admin_with(&mut self, data_dir: &Path, operator_env: &[(&str, &str)]) -> rdm_client::PathName {
        let (exe, env) = self.admin_launch(data_dir, operator_env);
        let (child, birth) = spawn_admin(&exe, &env, &format!("restarted node-admin on {}", data_dir.display()));
        self.restarted.push(child);
        let name = birth.name.clone();
        self.hold_printed(birth);
        name
    }

    /// A node-admin started on `data_dir` as [`Self::restart_admin_with`] starts one, for a launch the
    /// cell expects to be refused: its exit status and everything it wrote to stderr. A launch still
    /// running after `within` is killed and named by the panic.
    pub fn admin_launch_refused(&self, data_dir: &Path, operator_env: &[(&str, &str)], within: Duration) -> (std::process::ExitStatus, String) {
        let (exe, env) = self.admin_launch(data_dir, operator_env);
        let mut cmd = Command::new(&exe);
        for (k, v) in &env {
            cmd.env(k, v);
        }
        let mut child = cmd.stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap_or_else(|e| panic!("spawn the node-admin on {}: {e}", data_dir.display()));
        let mut stderr = child.stderr.take().unwrap();
        let read = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = stderr.read_to_string(&mut s);
            s
        });
        let until = Instant::now() + within;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= until {
                let _ = child.kill();
                let _ = child.wait();
                panic!("the node-admin launched on {} was not refused within {within:?}: it still ran (pid {})", data_dir.display(), child.id());
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        (status, read.join().unwrap_or_default())
    }

    /// The executable and environment of a node-admin a person starts on `data_dir` of this estate:
    /// the environment a person gives one (provider, store, binaries, evidence, the fabric root) and `operator_env`.
    fn admin_launch<'a>(&self, data_dir: &Path, operator_env: &[(&'a str, &str)]) -> (PathBuf, Vec<(&'a str, String)>) {
        let mut env = vec![
            ("MESH_SPAWN_TYPE", self.owner.provider.clone()),
            ("RDM_DATA_DIR", data_dir.display().to_string()),
            ("RDM_STORE_DIR", estate_store_dir(&self.root).display().to_string()),
            ("RDM_BIN_DIR", bin_dir().display().to_string()),
            ("RDM_EVIDENCE_DIR", self.evidence.display().to_string()),
            ("RDM_ESTATE_ROOT", self.root.display().to_string()),
            // A person starting a node-admin on an estate holds the fabric's root and hands it in.
            ("RDM_CA_SECRET", crate::test_certs::test_root_hex()),
        ];
        env.extend(operator_env.iter().map(|(k, v)| (*k, v.to_string())));
        let exe = match &self.external {
            Some(x) => {
                env.retain(|(k, _)| *k != "RDM_BIN_DIR");
                env.extend(x.env());
                x.admin_exe()
            }
            None => {
                refuse_built_ins_in_consumer_mode("Estate::restart_admin");
                binary("rdm-node-admin")
            }
        };
        if self.owner.provider == "container" && !env.iter().any(|(k, _)| *k == "RDM_EXECUTABLE_DIRS") {
            env.push(executable_dirs_env(&self.external.as_ref().map(ExternalLaunch::read_dirs).unwrap_or_default()));
        }
        (exe, env)
    }

    /// Hold the birth an admin this estate started printed: the typed objects learn it at their next
    /// handover, and a probe can enter at it now.
    fn hold_printed(&self, birth: MeshNode) {
        self.births.lock().unwrap().insert(birth.name.clone(), birth.clone());
        self.printed.lock().unwrap().push(birth);
    }

    /// The birth of the node-admin the estate enters at (`Estate::admin`): what a probe is given
    /// (`--admin-birth`). An entry whose birth the estate never held is named with every birth it holds.
    pub fn entry_birth(&self) -> MeshNode {
        let births = self.births.lock().unwrap();
        births.get(&self.admin).cloned().unwrap_or_else(|| panic!("the entry {} has no birth this estate holds; births held: {:?}", self.admin, births.keys().collect::<Vec<_>>()))
    }

    /// SIGKILL `pid` (a fault: it flushes nothing) and wait until it is gone, once the fabric has first
    /// been ready ([`Self::fabric_ready`]).
    pub fn kill_pid(&self, pid: u32) {
        self.fabric_ready();
        let ok = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success();
        assert!(ok, "kill -9 {pid}");
        // Gone only once every task of the thread group is: the leader reads as a zombie while its
        // sibling threads still run, and a child it forked is reparented only when the last of them
        // exits. A caller that freezes that child before then has it killed by the kernel (SIGHUP to
        // a stopped, newly orphaned process group).
        let alive = || {
            std::fs::read_dir(format!("/proc/{pid}/task"))
                .map(|tasks| tasks.flatten().any(|t| std::fs::read_to_string(t.path().join("stat")).is_ok_and(|s| !s.rsplit(')').next().unwrap_or("").trim_start().starts_with('Z'))))
                .unwrap_or(false)
        };
        let until = Instant::now() + Duration::from_secs(10);
        while Instant::now() < until && alive() {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// SIGKILL the bootstrap admin (a fault: it flushes nothing), once the fabric has first been
    /// ready ([`Self::fabric_ready`]).
    pub fn kill_bootstrap(&mut self) {
        self.fabric_ready();
        if let Some(mut c) = self.bootstrap.take() {
            // A containerised Day-0 admin: the kill reaches its container, not only the attached CLI.
            if self.owner.provider == "container" {
                if let Some(id) = self.container_launched_as(&format!("{}.admin.1", self.root_mesh())) {
                    let _ = docker(&["kill", &id]);
                }
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// The Mesh the estate bootstrapped (its Day-0 admin's data dir names it).
    fn root_mesh(&self) -> String {
        std::fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_suffix(".admin.1")).map(String::from))
            .next()
            .unwrap_or_else(|| "mesh1".into())
    }

    /// Every runtime a provider started for this estate that still runs:
    /// `(data dir, pid)` from each node's `deployment.json` (process provider).
    /// The running containers of this estate's Fabric (container provider): `(node, container id)`.
    /// The provider labels each container with the Fabric's id and the node's path.name.
    pub fn live_containers(&self) -> Vec<(String, String)> {
        if self.owner.provider != "container" || self.fabric_id.is_empty() {
            return Vec::new();
        }
        let out = Command::new("docker")
            .args(["ps", "--no-trunc", "--filter", &format!("label=rafka.fabric={}", self.fabric_id), "--filter", "status=running", "--format", "{{.Label \"rafka.node\"}} {{.ID}}"])
            .output();
        let Ok(out) = out else { return Vec::new() };
        String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| l.split_once(' ').map(|(n, i)| (n.to_string(), i.to_string()))).collect()
    }

    /// The running container of `node`'s current birth (container provider): the container id its
    /// birth's RuntimeFact names, which the provider writes to the birth's data dir (`runtime.json`).
    /// Never the `rafka.node` label: that is the name the container was launched under, and a node
    /// renamed since (a replace's predecessor, renamed `<path.name>.old`) keeps it.
    pub async fn container_of(&self, node: &str) -> Option<String> {
        if self.owner.provider != "container" {
            return None;
        }
        let dir = self.advertised_data_dir(node).await?;
        let fact: Value = serde_json::from_slice(&std::fs::read(Path::new(&dir).join(rdm_core::runtime::RUNTIME_FILE)).ok()?).ok()?;
        let id = fact["locator"]["id"].as_str().filter(|_| fact["locator"]["kind"] == "container")?.to_string();
        (docker(&["inspect", "--format", "{{.State.Running}}", &id]).ok()?.trim() == "true").then_some(id)
    }

    /// The running container launched under `name` (its `rafka.node` label): the bootstrap admin's,
    /// which the estate holds as the birth it started.
    fn container_launched_as(&self, name: &str) -> Option<String> {
        self.live_containers().into_iter().find(|(n, _)| n == name).map(|(_, id)| id)
    }

    /// Remove every container of this estate's Fabric and its network (container provider).
    fn remove_containers(&self) {
        if self.owner.provider != "container" || self.fabric_id.is_empty() {
            return;
        }
        let left = remove_fabric(&self.fabric_id, Some(&self.artifacts));
        if !left.is_empty() {
            eprintln!("estate drop: fabric {} not fully removed: {left:?}", self.fabric_id);
        }
    }

    pub fn live_runtimes(&self) -> Vec<(PathBuf, u32)> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(&self.root).into_iter().flatten().flatten() {
            let Ok(raw) = std::fs::read_to_string(e.path().join("deployment.json")) else { continue };
            let Some(pid) = serde_json::from_str::<Value>(&raw).ok().and_then(|v| v["pid"].as_u64()) else { continue };
            if runs(pid as u32) {
                out.push((e.path(), pid as u32));
            }
        }
        out
    }

    /// [`Self::stop`], consuming the estate.
    pub async fn shutdown(mut self) {
        self.stop().await;
    }
}

impl Drop for Estate {
    fn drop(&mut self) {
        // Whatever still runs after the test (a failed run, or a fabric whose
        // control moved) is stopped here, never left behind.
        // Until nothing under the root is alive: a surviving admin re-creates what one pass killed
        // (a failed run leaves it reconciling), so one pass is not a stop.
        let sweep = |e: &Estate| {
            for _ in 0..10 {
                let live = e.live_runtimes();
                if live.is_empty() {
                    return;
                }
                for (_, pid) in live {
                    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            let left = e.live_runtimes();
            if !left.is_empty() {
                eprintln!("estate drop: {} runtime(s) still alive under {} after 10 sweeps: {left:?}", left.len(), e.root.display());
            }
        };
        // A failed run: every process of the estate is told to stop (SIGTERM, so its evidence flushes)
        // and given the time to leave before it is forced.
        let told = self.terminate(&[]);
        let until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < until && told.iter().any(|p| runs(*p)) {
            for c in self.restarted.iter_mut().chain(self.bootstrap.as_mut()) {
                let _ = c.try_wait();
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Some(mut c) = self.bootstrap.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        sweep(self);
        // Admins this estate restarted on their own data dirs are its children too, and run under
        // no deployment of any admin: a failed run leaves them unless they are killed here, and
        // whatever they launched with them.
        for mut c in self.restarted.drain(..) {
            let _ = c.kill();
            let _ = c.wait();
        }
        kill_estate_processes(&self.root);
        // Containers outlive every admin: the estate removes its Fabric's containers and network.
        self.remove_containers();
        keep_node_logs(&self.root, &self.artifacts);
    }
}

/// Whether process `pid` runs: it exists and is no zombie.
fn runs(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists() && !std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default().contains(") Z ")
}

/// Does this process environment (`/proc/<pid>/environ`, NUL-separated) name `root` as its estate:
/// `RDM_ESTATE_ROOT` is the root, or `RDM_DATA_DIR` is a directory under it?
pub fn environ_names_estate(env: &[u8], root: &Path) -> bool {
    let (estate, data) = (format!("RDM_ESTATE_ROOT={}", root.display()), format!("RDM_DATA_DIR={}/", root.display()));
    env.split(|b| *b == 0).any(|var| var == estate.as_bytes() || var.starts_with(data.as_bytes()))
}

/// Kill, until none is left, every process whose environment names `root` (see
/// [`environ_names_estate`]): the reaper's own rule, applied by the estate when it ends, so a failed
/// run leaves nothing behind for the reaper to find later.
fn kill_estate_processes(root: &Path) {
    for _ in 0..10 {
        let mut found = false;
        for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
            let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
            if pid == std::process::id() {
                continue;
            }
            let Ok(env) = std::fs::read(format!("/proc/{pid}/environ")) else { continue };
            if environ_names_estate(&env, root) {
                found |= Command::new("kill").args(["-9", &pid.to_string()]).status().is_ok_and(|s| s.success());
            }
        }
        if !found {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// One estate's probe door: runs `rdm-rpc-probe` entered at a node-admin's birth (it reads that
/// admin's view over Node RPC) and appends the invocation to the estate's RPC ledger
/// (`rpc-ledger.jsonl`).
#[derive(Clone)]
pub struct ProbeHandle {
    evidence: PathBuf,
    artifacts: PathBuf,
    bind: Option<String>,
}

impl ProbeHandle {
    fn command(&self, admin: &MeshNode, args: &[&str]) -> Result<Command, String> {
        let birth = serde_json::to_string(admin).map_err(|e| format!("the birth of {} does not serialize: {e}", admin.name))?;
        let mut cmd = Command::new(binary("rdm-rpc-probe"));
        cmd.arg("--admin-birth").arg(birth).args(args).env("RDM_EVIDENCE_DIR", &self.evidence);
        if let Some(gw) = &self.bind { cmd.env("RDM_PROBE_BIND", gw); }
        Ok(cmd)
    }

    fn record(&self, args: impl serde::Serialize, value: &Value, status: &std::process::ExitStatus, stderr: &str) -> Result<(), String> {
        let entry = if status.success() && !stderr.contains("panicked") && !stderr.contains("flush ended at its bound") {
            json!({ "args": args, "result": value })
        } else {
            json!({ "args": args, "result": value, "exit": status.code(), "signal": std::os::unix::process::ExitStatusExt::signal(status), "stderr_tail": stderr.chars().rev().take(1200).collect::<Vec<_>>().into_iter().rev().collect::<String>() })
        };
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(self.artifacts.join("rpc-ledger.jsonl")).map_err(|e| e.to_string())?;
        let mut record = serde_json::to_vec(&entry).map_err(|e| e.to_string())?;
        record.push(b'\n');
        file.write_all(&record).map_err(|e| e.to_string())
    }

    /// One probe invocation, including its endpoint close, exit and evidence flush.
    pub fn run(&self, admin: &MeshNode, args: &[&str]) -> Result<Value, String> {
        self.run_with_stderr(admin, args).map(|(value, _)| value)
    }

    /// [`ProbeHandle::run`], with the probe's stderr.
    pub fn run_with_stderr(&self, admin: &MeshNode, args: &[&str]) -> Result<(Value, String), String> {
        let out = self.command(admin, args)?.output().map_err(|e| format!("run rdm-rpc-probe: {e}"))?;
        let line = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let value = serde_json::from_str(line.trim()).map_err(|e| format!("probe {args:?} printed no JSON ({e}): {line} / {stderr}"))?;
        self.record(args, &value, &out.status, &stderr)?;
        Ok((value, stderr.into_owned()))
    }

    /// Observe the real probe's answer when printed, retaining ownership of its
    /// process. The caller must finish it to check its exit and keep the evidence;
    /// dropping the observation kills and reaps an unfinished probe.
    pub fn observe(&self, admin: &MeshNode, args: &[&str]) -> Result<ProbeObservation, String> {
        let mut child = self.command(admin, args)?.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e| e.to_string())?;
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut error = child.stderr.take().unwrap();
        let stderr = std::thread::spawn(move || { let mut text = String::new(); let _ = error.read_to_string(&mut text); text });
        let mut line = String::new();
        let parsed = output.read_line(&mut line).map_err(|e| e.to_string()).and_then(|_| serde_json::from_str(&line).map_err(|e| e.to_string()));
        let answer = match parsed {
            Ok(answer) => answer,
            Err(error) => {
                let _ = child.kill();
                let status = child.wait();
                return Err(format!("probe {args:?} printed no JSON ({error}): {line}; status={status:?}; stderr={}", stderr.join().unwrap_or_default()));
            }
        };
        let stdout = std::thread::spawn(move || { let mut text = String::new(); let _ = output.read_to_string(&mut text); text });
        Ok(ProbeObservation { handle: self.clone(), args: args.iter().map(|s| s.to_string()).collect(), answer, child: Some(child), stdout: Some(stdout), stderr: Some(stderr) })
    }
}

/// A probe's observed JSON answer and the process still owed its exit check.
/// Transport shutdown and evidence flushing do not change the call's outcome.
pub struct ProbeObservation {
    handle: ProbeHandle,
    args: Vec<String>,
    answer: Value,
    child: Option<Child>,
    stdout: Option<std::thread::JoinHandle<String>>,
    stderr: Option<std::thread::JoinHandle<String>>,
}

impl ProbeObservation {
    /// The answer already printed by the real probe process.
    pub fn answer(&self) -> &Value { &self.answer }

    /// Reap the probe, record its status and evidence errors, and require a clean
    /// exit. A successful-looking JSON answer never substitutes for that check.
    pub fn finish(mut self) -> Result<Value, String> {
        let mut child = self.child.take().unwrap();
        let status = child.wait().map_err(|e| format!("wait for probe {:?}: {e}", self.args))?;
        let stdout = self.stdout.take().unwrap().join().map_err(|_| "probe stdout reader panicked".to_string())?;
        let stderr = self.stderr.take().unwrap().join().map_err(|_| "probe stderr reader panicked".to_string())?;
        self.handle.record(&self.args, &self.answer, &status, &stderr)?;
        if !status.success() || stderr.contains("panicked") || !stdout.trim().is_empty() {
            return Err(format!("probe {:?}: status={status:?}; answer={}; extra stdout={stdout}; stderr={stderr}", self.args, self.answer));
        }
        Ok(self.answer.clone())
    }
}

impl Drop for ProbeObservation {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() { let _ = child.kill(); let _ = child.wait(); }
        if let Some(reader) = self.stdout.take() { let _ = reader.join(); }
        if let Some(reader) = self.stderr.take() { let _ = reader.join(); }
    }
}

/// Incremental reader of an estate's `.spans.jsonl` files: keeps per-file byte offsets and parsed
/// spans across calls, reading only complete lines appended since the last call and re-reading a
/// file from `0` when it shrank or was replaced. [`Self::named`] keeps its own offsets and spans per
/// name set, checking the `"name":"` field in the line's bytes before parsing the line into a
/// [`Value`].
#[derive(Default)]
pub(crate) struct SpanReader {
    all: SpanStream,
    by_names: std::collections::BTreeMap<Vec<String>, SpanStream>,
}

impl SpanReader {
    pub(crate) fn all(&mut self, dir: &Path) -> Vec<Value> {
        self.all.read(dir, None)
    }

    pub(crate) fn named(&mut self, dir: &Path, names: &[&str]) -> Vec<Value> {
        let mut set: Vec<String> = names.iter().map(|s| (*s).to_string()).collect();
        set.sort();
        set.dedup();
        self.by_names.entry(set).or_default().read(dir, Some(names))
    }
}

#[derive(Default)]
struct SpanStream {
    files: std::collections::BTreeMap<PathBuf, FileSpans>,
}

struct FileSpans {
    file_id: (u64, u64),
    offset: u64,
    prefix: Vec<u8>,
    spans: Vec<Value>,
}

impl SpanStream {
    fn read(&mut self, dir: &Path, names: Option<&[&str]>) -> Vec<Value> {
        use std::io::{Seek, SeekFrom};
        use std::os::unix::fs::{FileExt, MetadataExt};

        let mut seen = std::collections::BTreeSet::new();
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = e.path();
            if path.to_string_lossy().ends_with(".spans.jsonl") {
                seen.insert(path);
            }
        }
        self.files.retain(|path, _| seen.contains(path));

        for path in seen {
            let Ok(mut file) = std::fs::File::open(&path) else { continue };
            let Ok(meta) = file.metadata() else { continue };
            let file_id = (meta.dev(), meta.ino());
            let len = meta.len();
            let reset = match self.files.get(&path) {
                None => true,
                Some(cur) => {
                    if cur.file_id != file_id || len < cur.offset {
                        true
                    } else if !cur.prefix.is_empty() {
                        let mut head = vec![0u8; cur.prefix.len()];
                        file.read_exact_at(&mut head, 0).is_err() || head != cur.prefix
                    } else {
                        false
                    }
                }
            };
            if reset {
                self.files.insert(
                    path.clone(),
                    FileSpans { file_id, offset: 0, prefix: Vec::new(), spans: Vec::new() },
                );
            }
            let entry = self.files.get_mut(&path).unwrap();
            if file.seek(SeekFrom::Start(entry.offset)).is_err() {
                continue;
            }
            let mut buf = Vec::new();
            if file.read_to_end(&mut buf).is_err() || buf.is_empty() {
                continue;
            }
            let Some(end) = buf.iter().rposition(|&b| b == b'\n') else {
                continue;
            };
            let complete = &buf[..=end];
            if entry.prefix.len() < 256 {
                let take = (256 - entry.prefix.len()).min(complete.len());
                entry.prefix.extend_from_slice(&complete[..take]);
            }
            entry.offset += (end + 1) as u64;
            for raw in complete.split(|&b| b == b'\n') {
                let line = raw.strip_suffix(b"\r").unwrap_or(raw);
                if line.is_empty() {
                    continue;
                }
                if let Some(wanted) = names {
                    if !line_carries_name(line, wanted) {
                        continue;
                    }
                }
                let mut v = serde_json::from_slice::<Value>(line).unwrap_or_else(|e| {
                    panic!("{}: span line is not JSON ({e}): {}", path.display(), String::from_utf8_lossy(line))
                });
                if let Some(wanted) = names {
                    if !v["name"].as_str().is_some_and(|n| wanted.contains(&n)) {
                        continue;
                    }
                }
                if let Some(s) = v["attributes"]["build_id"].as_str() {
                    if s.len() == 18 && s.starts_with("bid_") && s.as_bytes()[8] == b'-' && s.as_bytes()[13] == b'-' {
                        let bare = format!("{}{}{}", &s[4..8], &s[9..13], &s[14..18]);
                        v["attributes"]["build_id"] = Value::String(bare);
                    }
                }
                entry.spans.push(v);
            }
        }

        let mut out: Vec<Value> = self.files.values().flat_map(|f| f.spans.iter().cloned()).collect();
        out.sort_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0));
        out
    }
}

/// Whether `line` contains a `"name":"<value>"` field whose value is one of `names`, checked on the
/// raw bytes before parsing the line as JSON.
fn line_carries_name(mut line: &[u8], names: &[&str]) -> bool {
    const FIELD: &[u8] = b"\"name\":";
    while let Some(pos) = line.windows(FIELD.len()).position(|w| w == FIELD) {
        let mut rest = &line[pos + FIELD.len()..];
        while let Some((&b, tail)) = rest.split_first() {
            if b.is_ascii_whitespace() {
                rest = tail;
            } else {
                break;
            }
        }
        let Some(rest) = rest.strip_prefix(b"\"") else {
            line = rest;
            continue;
        };
        let mut end = 0;
        while end < rest.len() {
            if rest[end] == b'\\' {
                end += 2;
                continue;
            }
            if rest[end] == b'"' {
                break;
            }
            end += 1;
        }
        if end < rest.len() {
            let found = &rest[..end];
            if names.iter().any(|n| n.as_bytes() == found) {
                return true;
            }
            line = &rest[end + 1..];
        } else {
            break;
        }
    }
    false
}

/// Spans by name.
pub fn named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

/// iroh's local view of a remote recorded on `sp` (A1, `rdm_transport::iroh_obs`) has its
/// shape: `iroh_known_addrs` and `iroh_active_addrs` are both present; either both are
/// `unavailable` (no record) or the known list is a non-empty comma list of `<kind>:<addr>` and
/// every active address is one of them (none active is a reading). Shape only: the fields are
/// iroh's observations, never proof of liveness, so nothing here reads what they say about it.
pub fn iroh_observation_shape(sp: &Value) -> Result<(), String> {
    let field = |k: &str| sp["attributes"][k].as_str().map(str::to_string).ok_or_else(|| format!("{} carries no {k}: {sp}", sp["name"]));
    let (known, active) = (field("iroh_known_addrs")?, field("iroh_active_addrs")?);
    match (known.as_str(), active.as_str()) {
        ("unavailable", "unavailable") => Ok(()),
        ("unavailable", _) | (_, "unavailable") => Err(format!("{}: one field unavailable, the other not: known={known:?} active={active:?}", sp["name"])),
        ("", _) => Err(format!("{}: an empty known list stands for no record; it must say unavailable", sp["name"])),
        _ => {
            let list = |v: &str| v.split(',').filter(|a| !a.is_empty()).map(str::to_string).collect::<Vec<_>>();
            let known_list = list(&known);
            if let Some(bad) = known_list.iter().find(|a| !a.split_once(':').is_some_and(|(kind, addr)| !kind.is_empty() && !addr.is_empty())) {
                return Err(format!("{}: {bad:?} is not <kind>:<addr> in {known:?}", sp["name"]));
            }
            match list(&active).into_iter().find(|a| !known_list.contains(a)) {
                Some(a) => Err(format!("{}: active {a:?} is not among the known {known:?}", sp["name"])),
                None => Ok(()),
            }
        }
    }
}

/// True when `child` descends from `ancestor` by `parent_span_id` links.
/// The node-admin that decided the claim of `attempt` of `build_id` as won (the fabric-primary of that
/// moment; `None` when no claim of it was won), read from its `via-claim-decision` span. An attempt's reconcile continues the trace of
/// the request that created the attempt only while the fabric-primary that holds that request's
/// context decides the claim; after the role moved, the new fabric-primary starts the attempt's trace.
pub fn claim_decider(spans: &[Value], build_id: &str, attempt: &str) -> Option<String> {
    named(spans, "rdm.node_admin.build.update.via-claim-decision")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == build_id && s["attributes"]["attempt"] == attempt && s["attributes"]["outcome"] == "won")
        .map(|s| s["attributes"]["node"].as_str().unwrap_or_default().to_string())
}

pub fn descends_from(spans: &[Value], child: &Value, ancestor: &Value) -> bool {
    let mut cur = child.clone();
    for _ in 0..64 {
        let parent = cur["parent_span_id"].as_str().unwrap_or("");
        if parent.is_empty() {
            return false;
        }
        if parent == ancestor["span_id"].as_str().unwrap_or("-") {
            return true;
        }
        match spans.iter().find(|s| s["span_id"] == parent) {
            Some(p) => cur = p.clone(),
            None => return false,
        }
    }
    false
}

#[cfg(test)]
mod spawn_tests {
    /// CONTRACT: a test that births a second estate in the same process gets its own root, and
    /// dropping the first estate's scope leaves the second estate's root in place.
    #[test]
    fn a_re_born_estate_never_shares_or_loses_its_root_to_the_estate_it_replaces() {
        let (a, b) = (super::estate_root("reroll"), super::estate_root("reroll"));
        assert_ne!(a, b, "two estates of one test in one process share no root");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let first = super::EstateScope::begin(&a);
        let second = super::EstateScope::begin(&b);
        drop(first);
        assert!(!a.exists(), "the replaced estate's own root is removed");
        assert!(b.exists(), "the live estate's root survives the replaced estate's teardown");
        drop(second);
        assert!(!b.exists());
    }

    use super::*;

    /// CONTRACT: the reaper that outlives a SIGKILLed test binary kills every node of the estate, the
    /// ones an admin launched included. The process provider strips every `RDM_*` of the launcher from
    /// the child, so such a node carries no `RDM_ESTATE_ROOT`; its launch contract names its data dir
    /// under the estate's root (`RDM_DATA_DIR`). What must NOT happen: a node left running, adopted by
    /// init, after the test binary that owned it was killed.
    #[test]
    fn the_reaper_of_a_killed_test_binary_kills_a_node_whose_only_mark_is_its_data_dir() {
        let root = estate_root("reaper-data-dir");
        std::fs::create_dir_all(&root).unwrap();
        let mut owner = Command::new("sleep").arg("120").spawn().unwrap();
        let mut node = Command::new("sleep").arg("120").env_clear().env("RDM_DATA_DIR", root.join("mesh1.rpc.1-test")).spawn().unwrap();
        let reaper = Command::new("sh")
            .args(["-c", REAPER, "estate-reaper", &owner.id().to_string(), &root.display().to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env_remove("RDM_ESTATE_ROOT")
            .spawn()
            .unwrap();
        owner.kill().unwrap();
        owner.wait().unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        while Instant::now() < until && node.try_wait().unwrap().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
        let left = node.try_wait().unwrap().is_none();
        let _ = node.kill();
        let _ = node.wait();
        let mut reaper = reaper;
        let _ = reaper.kill();
        let _ = reaper.wait();
        let _ = std::fs::remove_dir_all(&root);
        assert!(!left, "the reaper left the node running 30 s after its owner was killed");
    }

    /// An estate root holding evidence in its spans directory (`estate_evidence_dir`), one file in a
    /// directory of its own, and the directory it is kept in (outside the root, emptied): what each cell
    /// below keeps.
    fn evidence_of_an_estate(test: &str) -> (PathBuf, PathBuf, PathBuf, [(&'static str, &'static str); 2]) {
        let root = estate_root(test);
        let evidence = estate_evidence_dir(&root);
        let kept = root.with_extension("kept");
        let _ = std::fs::remove_dir_all(&kept);
        let files = [("rdm-node-admin.1-2.spans.jsonl", "{\"name\":\"rdm.mesh.node.delete.via-signal\"}\n"), ("probe/rdm-rpc-probe.3-4.spans.jsonl", "{\"name\":\"a probe's span\"}\n")];
        for (name, body) in files {
            let f = evidence.join(name);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(f, body).unwrap();
        }
        (root, evidence, kept, files)
    }

    /// CONTRACT (rdm#118): the reaper of a SIGKILLed test binary keeps the estate's evidence, which its
    /// processes write under the estate's root on tmpfs, in the estate's artifacts before it removes the
    /// root, so a gate's red keeps its spans. What must NOT happen: the root removed with the spans in it.
    #[test]
    fn the_reaper_of_a_killed_test_binary_keeps_the_estates_evidence_before_it_removes_the_root() {
        let (root, evidence, kept, files) = evidence_of_an_estate("reaper-kept-evidence");
        let mut owner = Command::new("sleep").arg("120").spawn().unwrap();
        let mut reaper = Command::new("sh")
            .args(["-c", REAPER, "estate-reaper", &owner.id().to_string(), &root.display().to_string(), &evidence.display().to_string(), &kept.display().to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env_remove("RDM_ESTATE_ROOT")
            .spawn()
            .unwrap();
        owner.kill().unwrap();
        owner.wait().unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        while Instant::now() < until && reaper.try_wait().unwrap().is_none() {
            std::thread::sleep(Duration::from_millis(100));
        }
        let ended = reaper.try_wait().unwrap().is_some();
        let _ = reaper.kill();
        let _ = reaper.wait();
        let read: Vec<Option<String>> = files.iter().map(|(name, _)| std::fs::read_to_string(kept.join(name)).ok()).collect();
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&kept);
        assert!(ended, "the reaper ended within 30 s of its owner's kill");
        assert!(!root.exists(), "the reaper removed the estate's root");
        assert_eq!(read, files.iter().map(|(_, body)| Some(body.to_string())).collect::<Vec<_>>(), "every evidence file is kept, whole, under its own relative path");
    }

    /// CONTRACT (rdm#118): an estate's scope that drops (a finished estate, a panicking cell) keeps the
    /// estate's evidence in its artifacts before it removes the root. What must NOT happen: the root
    /// removed with the spans in it.
    #[test]
    fn a_dropped_estate_scope_keeps_the_estates_evidence_before_it_removes_the_root() {
        let (root, evidence, kept, files) = evidence_of_an_estate("scope-kept-evidence");
        drop(EstateScope::keeping(&root, &evidence, &kept));
        let read: Vec<Option<String>> = files.iter().map(|(name, _)| std::fs::read_to_string(kept.join(name)).ok()).collect();
        let _ = std::fs::remove_dir_all(&kept);
        assert!(!root.exists(), "the scope removed the estate's root");
        assert_eq!(read, files.iter().map(|(_, body)| Some(body.to_string())).collect::<Vec<_>>(), "every evidence file is kept, whole, under its own relative path");
    }

    /// CONTRACT: a node-admin that never names its application contact is refused by name, and the
    /// refusal leaves nothing running: no Estate exists yet to stop it. What must NOT happen: a
    /// panic that leaves the process alive.
    #[test]
    fn an_admin_that_never_advertises_is_stopped_before_the_refusal() {
        let dir = std::env::temp_dir().join(format!("rdm-spawn-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let script = dir.join("silent-admin.sh");
        let pid_file = dir.join("pid");
        // The script is written by a child process, never through a write fd of this one: a fork by
        // another test's thread (the reaper cell spawns) inherits every fd this process holds open
        // until its exec, and an executable with a write fd open anywhere refuses exec (ETXTBSY).
        let body = format!("#!/bin/sh\necho $$ > {}\nexec sleep 60 >/dev/null\n", pid_file.display());
        let wrote = Command::new("sh").args(["-c", "printf '%s' \"$1\" > \"$2\" && chmod 755 \"$2\"", "write-script", &body, &script.display().to_string()]).status().unwrap();
        assert!(wrote.success(), "the silent admin's script is written");
        let err = std::panic::catch_unwind(|| spawn_admin(&script, &[], "the silent admin")).expect_err("refused");
        let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("the silent admin never advertised RDM_NODE_ADMIN_API_BASE"), "{msg}");
        let pid: u32 = std::fs::read_to_string(&pid_file).unwrap().trim().parse().unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists() || std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| s.contains(") Z ")), "pid {pid} still runs after the refusal");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The transport diagnostic file every node writes in its data dir (`rdm_transport::TRANSPORT_DIAG_FILE`,
/// pinned equal by `the_kept_transport_diag_file_is_the_one_nodes_write`).
const TRANSPORT_DIAG_FILE: &str = "transport-diag.log";

/// The node and admin logs under an estate root, kept in the estate's artifacts before the root is
/// removed: `<node dir>/stdout.log`, `stderr.log` and the transport diagnostic file (R-QH1), which
/// the bootstrap admin writes too though its stderr is the test's.
fn keep_node_logs(root: &Path, artifacts: &Path) {
    let dest = artifacts.join("node-logs");
    for e in std::fs::read_dir(root).into_iter().flatten().flatten() {
        for name in ["stdout.log", "stderr.log", TRANSPORT_DIAG_FILE] {
            let f = e.path().join(name);
            if f.is_file() {
                let _ = std::fs::create_dir_all(&dest);
                let _ = std::fs::copy(&f, dest.join(format!("{}.{name}", e.file_name().to_string_lossy())));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // CONTRACT: the file the estate keeps with every node's logs is the transport diagnostic file
    // the nodes write (R-QH1); a rename of one without the other loses a red gate's handshake evidence.
    #[test]
    fn the_kept_transport_diag_file_is_the_one_nodes_write() {
        assert_eq!(TRANSPORT_DIAG_FILE, rdm_transport::TRANSPORT_DIAG_FILE);
    }

    // CONTRACT: a binary is refused by name when a source file its dep-info lists is newer than it, and
    // accepted when none is; a binary with no dep-info beside it is not judged.
    #[test]
    fn a_binary_older_than_its_source_is_refused_by_name_and_a_fresh_one_is_not() {
        let dir = std::env::temp_dir().join(format!("rdm-stale-bin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("lib one.rs");
        std::fs::write(&src, "fn main() {}").unwrap();
        let exe = dir.join("rdm-rpc-probe");
        let set = |path: &Path, secs_from_unix_epoch: u64| {
            let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            f.set_modified(std::time::UNIX_EPOCH + Duration::from_secs(secs_from_unix_epoch)).unwrap();
        };
        std::fs::write(&exe, "bin").unwrap();
        assert_eq!(binary_is_fresh("rdm-rpc-probe", &exe), Ok(()), "no dep-info beside the binary: not judged");
        std::fs::write(exe.with_extension("d"), format!("{}: {}\n", exe.display(), src.display().to_string().replace(' ', "\\ "))).unwrap();
        set(&src, 1_000);
        set(&exe, 2_000);
        assert_eq!(binary_is_fresh("rdm-rpc-probe", &exe), Ok(()), "the source is older than the binary");
        set(&src, 3_000);
        let why = binary_is_fresh("rdm-rpc-probe", &exe).unwrap_err();
        assert!(why.contains("rdm-rpc-probe") && why.contains("lib one.rs") && why.contains("cargo build -p rdm-test --bin rdm-rpc-probe"), "{why}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod iroh_observation_shape_tests {
    use super::iroh_observation_shape;
    use serde_json::json;

    fn sp(known: Option<&str>, active: Option<&str>) -> serde_json::Value {
        let mut a = serde_json::Map::new();
        if let Some(k) = known {
            a.insert("iroh_known_addrs".into(), json!(k));
        }
        if let Some(v) = active {
            a.insert("iroh_active_addrs".into(), json!(v));
        }
        json!({"name": "rdm.test.span", "attributes": a})
    }

    /// CONTRACT (A1): a recorded observation is accepted in its shape: no record (both
    /// `unavailable`), or known addresses with the active ones among them, none active included.
    #[test]
    fn a_recorded_observation_in_its_shape_is_accepted() {
        assert_eq!(iroh_observation_shape(&sp(Some("unavailable"), Some("unavailable"))), Ok(()));
        assert_eq!(iroh_observation_shape(&sp(Some("ip:127.0.0.1:40051"), Some("ip:127.0.0.1:40051"))), Ok(()));
        assert_eq!(iroh_observation_shape(&sp(Some("ip:127.0.0.1:1,ip:127.0.0.1:2"), Some(""))), Ok(()));
    }

    /// CONTRACT (A1): a missing field, an empty known list standing for no record, one field
    /// unavailable alone, a malformed address and an active address iroh does not know are refused.
    #[test]
    fn a_missing_or_misshapen_observation_is_refused() {
        assert!(iroh_observation_shape(&sp(None, Some("unavailable"))).is_err());
        assert!(iroh_observation_shape(&sp(Some("unavailable"), None)).is_err());
        assert!(iroh_observation_shape(&sp(Some(""), Some(""))).is_err());
        assert!(iroh_observation_shape(&sp(Some("unavailable"), Some(""))).is_err());
        assert!(iroh_observation_shape(&sp(Some("127.0.0.1"), Some(""))).is_err());
        assert!(iroh_observation_shape(&sp(Some("ip:127.0.0.1:1"), Some("ip:127.0.0.1:2"))).is_err());
    }
}

impl crate::container_faults::ContainerEstate for Estate {
    fn fabric_id(&self) -> &str {
        &self.fabric_id
    }
    fn container_of(&self, node: &str) -> impl std::future::Future<Output = Option<String>> + Send {
        Estate::container_of(self, node)
    }
    fn live_containers(&self) -> Vec<(String, String)> {
        Estate::live_containers(self)
    }
}

#[cfg(test)]
mod span_reader_tests {
    use super::SpanReader;
    use std::io::Write;

    /// CONTRACT: the incremental span reader reads only complete lines appended since its last call,
    /// holds a half line (no `\n`) until it is completed, reads a replaced or shrunk file from `0`,
    /// and in `named` keeps only the listed names while deciding from the line's bytes before
    /// parsing JSON (a non-JSON line carrying another name is neither a parse error nor a kept span).
    #[test]
    fn incremental_reader_reads_appended_lines_re_reads_a_replaced_file_and_filters_by_name_before_parsing() {
        let dir = std::env::temp_dir().join(format!("rdm-span-reader-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("rdm-node-admin.1-1.spans.jsonl");

        std::fs::write(
            &file,
            "{\"name\":\"rdm.one\",\"start_unix_nano\":1,\"attributes\":{\"build_id\":\"bid_aaaa-bbbb-cccc\"}}\n",
        )
        .unwrap();

        let mut reader = SpanReader::default();
        let first = reader.all(&dir);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["name"], "rdm.one");
        assert_eq!(first[0]["attributes"]["build_id"], "aaaabbbbcccc");

        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&file).unwrap();
            f.write_all(b"{\"name\":\"rdm.two\",\"start_unix_nano\":2}\n{\"name\":\"rdm.three\",\"start_unix_nano\":3").unwrap();
        }
        let second = reader.all(&dir);
        let names: Vec<&str> = second.iter().filter_map(|s| s["name"].as_str()).collect();
        assert_eq!(names, vec!["rdm.one", "rdm.two"], "the whole line appears once and the half line not until completed");

        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&file).unwrap();
            f.write_all(b"}\n").unwrap();
        }
        let third = reader.all(&dir);
        let names: Vec<&str> = third.iter().filter_map(|s| s["name"].as_str()).collect();
        assert_eq!(names, vec!["rdm.one", "rdm.two", "rdm.three"], "the completed line now appears");

        let replacement = dir.join("rdm-node-admin.1-1.spans.jsonl.new");
        std::fs::write(&replacement, "{\"name\":\"rdm.from_zero\",\"start_unix_nano\":4}\n").unwrap();
        std::fs::rename(&replacement, &file).unwrap();
        let replaced = reader.all(&dir);
        let names: Vec<&str> = replaced.iter().filter_map(|s| s["name"].as_str()).collect();
        assert_eq!(names, vec!["rdm.from_zero"], "a replaced file is read from 0");

        let second_file = dir.join("rdm-rpc-node.2-1.spans.jsonl");
        std::fs::write(
            &second_file,
            concat!(
                "{\"name\":\"rdm.unlisted\", not valid json at all\n",
                "{\"name\":\"rdm.wanted.a\",\"start_unix_nano\":10}\n",
                "{\"events\":[{\"name\":\"rdm.unlisted\"}],\"name\":\"rdm.wanted.b\",\"start_unix_nano\":20}\n",
                "{\"name\":\"rdm.other\",\"start_unix_nano\":30}\n",
            ),
        )
        .unwrap();

        let filtered = reader.named(&dir, &["rdm.wanted.a", "rdm.wanted.b"]);
        let filtered_names: Vec<&str> = filtered.iter().filter_map(|s| s["name"].as_str()).collect();
        assert_eq!(
            filtered_names,
            vec!["rdm.wanted.a", "rdm.wanted.b"],
            "spans_named keeps only the listed names and never parses the non-JSON line carrying another name"
        );

        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reader.named(&dir, &["rdm.unlisted"]);
        }));
        assert!(err.is_err(), "a line that is parsed when its name is requested fails on invalid JSON");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
