//! Blackbox estate harness for the i143 e2e canaries.
//!
//! Everything here goes through public surfaces only (`docs/i143/design.md`):
//! the `rafka-node-admin` process and its control API, the `rafka-rpc-probe`
//! binary and the JSONL evidence files. No internal map is read.

use rafka_node_admin_client::binding::{BindingError, BindingSet, Expect, ProviderImage, Validated, ENV_EXECUTABLE_BINDINGS, ENV_EXECUTABLE_CANDIDATE};
use rafka_node_admin_client::LaunchKind as NodeKind;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
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
        // that walks `crates/rafka-test-scenario/../..` names nothing.
        Ok(d) => Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("the workspace root exists").join(d),
        Err(_) => Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/artifacts"),
    }
}

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

/// Start a `rafka-node-admin` with `env` and wait for the control API base it advertises.
fn spawn_admin(exe: &Path, env: &[(&str, String)], what: &str) -> (Child, String) {
    let mut cmd = Command::new(exe);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap_or_else(|e| panic!("spawn {what}: {e}"));
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(base) = line.strip_prefix("RDM_NODE_ADMIN_API_BASE=") {
                let _ = tx.send(base.trim().to_string());
            }
        }
    });
    let Ok(base) = rx.recv_timeout(Duration::from_secs(30)) else {
        // No Estate exists yet to stop it: the process is stopped here, before the refusal.
        let status = child.try_wait().ok().flatten().map(|s| s.to_string()).unwrap_or_else(|| "still running; killed".into());
        let _ = child.kill();
        let _ = child.wait();
        panic!("{what} never advertised RDM_NODE_ADMIN_API_BASE within 30 s (pid {}, {status})", child.id());
    };
    (child, base)
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
pub const RUNTIME_IMAGE: &str = "rafka-node-runtime:empty";

/// Start the Day-0 node-admin of a new container fabric in a container on that fabric's own
/// network: the Fabric id is minted here so the network exists before the admin does, the admin
/// is given an address on it, and it drives the host's Docker daemon through its socket. The
/// returned child is the attached `docker run`; the container is labelled like every container of
/// the fabric, so the estate stops and removes it with them.
fn spawn_admin_in_container(env: Vec<(&str, String)>, root: &Path, evidence: &Path, node: &str, exe: &Path, read_dirs: &[PathBuf]) -> (Child, String, String) {
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
pub fn spawn_admin_in_container_within(mut env: Vec<(&str, String)>, root: &Path, evidence: &Path, node: &str, exe: &Path, read_dirs: &[PathBuf], within: Duration, program: &str) -> Result<(Child, String, String), GiveUp> {
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
    for k in ["OTEL_EXPORTER_OTLP_ENDPOINT", "RUST_LOG"] {
        if let Ok(v) = std::env::var(k) {
            env.push((k, v));
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
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(base) = line.strip_prefix("RDM_NODE_ADMIN_API_BASE=") {
                let _ = tx.send(base.trim().to_string());
            }
        }
    });
    let Ok(base) = rx.recv_timeout(within.saturating_sub(begun.elapsed())) else {
        return Err(give_up(format!("never advertised RDM_NODE_ADMIN_API_BASE within {within:?}"), Some(child)));
    };
    eprintln!("[container] {name} advertised {base} {:?} after docker start", started.elapsed());
    Ok((child, base, fabric_id))
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
fn package_of(name: &str) -> &'static str {
    match name {
        "rafka-node-admin" => "rafka-node-admin-core",
        "rafka-rpc-probe" | "rafka-rpc-node" => "rafka-node-rpc-testkit",
        "rafka-broker" => "rafka-broker",
        "rafka-gateway" => "rafka-gateway",
        "rafka-compute" => "rafka-compute",
        _ => "<the package of this binary>",
    }
}

/// Whether the binary at `exe` was built from the source this tree holds now. Cargo writes beside every
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
                "REFUSED: {name} at {} was built before {path} changed ({age} s after the binary); this tree's source is not the binary's. Rebuild with `cargo build -p {} --bin {name}`",
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
        assert!(Instant::now() < deadline, "timed out after {within:?} waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The environment variable naming an external consumer's built executable directory. When it is
/// set, every node-admin and node of a run launches from the consumer's executables through
/// [`Estate::bootstrap_external`]; the built-in set is refused.
pub const ENV_CONSUMER_BIN_DIR: &str = "RDM_RSHAPE_CONSUMER_BIN_DIR";

fn refuse_built_ins_in_consumer_mode(what: &str) {
    if let Ok(d) = std::env::var(ENV_CONSUMER_BIN_DIR) {
        panic!("REFUSED {what}: {ENV_CONSUMER_BIN_DIR}={d} selects an external consumer's executables, which launch only through Estate::bootstrap_external with an explicit binding set; the built-in binaries are never a fallback");
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
        bindings.push(rafka_node_admin_client::binding::Binding { launch_id: kind.clone(), executable: bin_dir.join(file), sha256, image: container.then(|| RUNTIME_IMAGE.to_string()) });
    }
    Ok(BindingSet { candidate: rafka_node_admin_client::binding::Candidate { sha, build: v["consumer_source_sha256"].as_str().unwrap_or_default().to_string() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings })
}

/// The explicit executable bindings an estate launches from (the external-consumer seam): the
/// validated set, the file node-admin reads it from, and the candidate it was validated against.
#[derive(Debug, Clone)]
pub struct ExternalLaunch {
    pub validated: Validated,
    pub file: PathBuf,
    pub candidate: String,
}

impl ExternalLaunch {
    /// The environment every node-admin of the estate gets.
    fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            (ENV_EXECUTABLE_BINDINGS, self.file.display().to_string()),
            (ENV_EXECUTABLE_CANDIDATE, self.candidate.clone()),
            // Nothing is launched from a built-in directory in this mode: an empty one makes any
            // fallback fail rather than quietly run a built-in.
            ("RDM_BIN_DIR", self.file.parent().expect("a file in the estate root").join("no-built-ins").display().to_string()),
        ]
    }

    fn admin_exe(&self) -> PathBuf {
        self.validated.resolve(NodeKind::NodeAdmin).expect("node_admin is bound (validated)").executable
    }

    fn read_dirs(&self) -> Vec<PathBuf> {
        self.validated.executable_dirs().into_iter().chain(self.file.parent().map(Path::to_path_buf)).collect()
    }
}

/// What an estate owns on the host beyond its processes: its root directory under the temp dir
/// and the reaper that outlives a SIGKILLed test binary. Dropping it (a finished estate, a panic
/// anywhere in birth) stops the reaper and removes the root; the reaper does the same when the
/// test binary dies without running any destructor.
/// This estate's own root: test name, process and a per-process sequence, so a test that ends one
/// estate and births another (a re-roll) never shares a root. The scope of the estate being
/// replaced deletes its own root when it drops, and only its own.
fn estate_root(test: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("i143-{test}-{}-{n}", std::process::id()))
}

pub struct EstateScope {
    root: PathBuf,
    reaper: Option<Child>,
}

/// The reaper: waits for the test binary to die, then kills every process whose environment names
/// this estate's root (`RDM_ESTATE_ROOT`: every admin, and every node an admin launched, inherits
/// it), removes the container fabric recorded in `<root>/fabric_id`, and removes the root. It runs
/// in its own session, so a gate that kills the test binary's process group leaves it running.
const REAPER: &str = r#"
owner=$1; root=$2
while kill -0 "$owner" 2>/dev/null; do sleep 0.2; done
for pass in 1 2 3 4 5 6 7 8 9 10; do
  found=0
  for e in $(grep -laP "RDM_ESTATE_ROOT=$root\x00" /proc/[0-9]*/environ 2>/dev/null); do
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
rm -rf "$root"
"#;

impl EstateScope {
    fn begin(root: &Path) -> Self {
        use std::os::unix::process::CommandExt;
        let mut cmd = Command::new("sh");
        cmd.args(["-c", REAPER, "estate-reaper", &std::process::id().to_string(), &root.display().to_string()]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        cmd.env_remove("RDM_ESTATE_ROOT");
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
        EstateScope { root: root.to_path_buf(), reaper: cmd.spawn().ok() }
    }
}

impl Drop for EstateScope {
    fn drop(&mut self) {
        if let Some(mut r) = self.reaper.take() {
            let _ = r.kill();
            let _ = r.wait();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub struct Estate {
    pub owner: Owner,
    pub root: PathBuf,
    pub artifacts: PathBuf,
    pub evidence: PathBuf,
    pub admin: String,
    /// This estate's Fabric id. Control addresses are loopback ports that other estates on the
    /// host reuse once a runtime dies: an admin answers for this estate only when its view names
    /// this id ([`Self::fabric_at`]).
    pub fabric_id: String,
    bootstrap: Option<Child>,
    /// The explicit executable bindings this estate runs from, if it was born with any.
    pub external: Option<ExternalLaunch>,
    /// The seed of the run, recorded in the estate manifest once the run has one.
    seed: Option<u64>,
    /// Admins this estate restarted on their own data dirs.
    restarted: Vec<Child>,
    http: reqwest::Client,
    /// Dropped last: stops the reaper and removes the root.
    _scope: EstateScope,
}

impl Estate {
    /// Start the first node-admin of `fabric` (bootstrap selects the provider)
    /// and wait for its advertised control API base.
    pub async fn bootstrap(owner: Owner, fabric: &str, mesh: &str) -> Self {
        refuse_built_ins_in_consumer_mode("Estate::bootstrap");
        Self::born(owner, fabric, mesh, None).await
    }

    /// Start the first node-admin of `fabric` from an explicit executable binding set: every
    /// launch of the estate (this admin, every node, every restart) runs the executable its
    /// launch id is bound to and nothing falls back to a built-in. The set is validated against
    /// `candidate_sha`, the launch ids the run `required` and the provider's image BEFORE any
    /// file is made or any process started; a refusal is returned by name with nothing launched.
    pub async fn bootstrap_external(owner: Owner, fabric: &str, mesh: &str, set: &BindingSet, candidate_sha: &str, required: &[&str]) -> Result<Self, BindingError> {
        let image = if owner.provider == "container" { ProviderImage::Container(RUNTIME_IMAGE) } else { ProviderImage::Process };
        let validated = set.validate(&Expect { candidate_sha, required, provider_image: image })?;
        // The file is written under the estate root once `born` makes it.
        Ok(Self::born(owner, fabric, mesh, Some((validated, candidate_sha.to_string()))).await)
    }

    async fn born(owner: Owner, fabric: &str, mesh: &str, external: Option<(Validated, String)>) -> Self {
        let artifacts = artifacts_root().join(&owner.feature).join(&owner.test);
        let _ = std::fs::remove_dir_all(&artifacts);
        let evidence = artifacts.join("spans");
        std::fs::create_dir_all(&evidence).unwrap();
        let root = estate_root(&owner.test);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let scope = EstateScope::begin(&root);
        let external = external.map(|(validated, candidate)| {
            let file = root.join("executable-bindings.json");
            std::fs::write(&file, serde_json::to_vec_pretty(validated.set()).unwrap()).unwrap();
            std::fs::create_dir_all(root.join("no-built-ins")).unwrap();
            ExternalLaunch { validated, file, candidate }
        });

        let mut env = vec![
            ("MESH_SPAWN_TYPE", owner.provider.clone()),
            ("RDM_FABRIC", fabric.into()),
            ("RDM_MESH", mesh.into()),
            ("RDM_DATA_DIR", root.join(format!("{mesh}.admin.1")).display().to_string()),
            ("RDM_BIN_DIR", bin_dir().display().to_string()),
            ("RDM_EVIDENCE_DIR", evidence.display().to_string()),
            ("RDM_ESTATE_ROOT", root.display().to_string()),
        ];
        let admin_exe = match &external {
            Some(x) => {
                env.retain(|(k, _)| *k != "RDM_BIN_DIR");
                env.extend(x.env());
                x.admin_exe()
            }
            None => binary("rafka-node-admin"),
        };
        // A container fabric's Day-0 admin runs in a container of that fabric, so every admin of
        // the fabric is a container a successor can control in the same Docker domain.
        let (child, admin, container_fabric) = if owner.provider == "container" {
            let read_dirs = external.as_ref().map(ExternalLaunch::read_dirs).unwrap_or_default();
            spawn_admin_in_container(env, &root, &evidence, &format!("{mesh}.admin.1"), &admin_exe, &read_dirs)
        } else {
            let (c, a) = spawn_admin(&admin_exe, &env, "bootstrap node-admin");
            (c, a, String::new())
        };
        // A container fabric's id is the one minted for its network: the estate holds it from
        // birth, so a panic before the fabric answers still removes the fabric's containers.
        let mut estate = Self { owner, root, artifacts, evidence, admin, fabric_id: container_fabric, bootstrap: Some(child), external, seed: None, restarted: Vec::new(), http: reqwest::Client::new(), _scope: scope };
        estate.write_manifest();
        // Topology is accepted only by the fabric-primary: the bootstrap admin is one once it is
        // Ready and elected, not when its API first answers.
        estate.fabric_id = wait_for("the bootstrap admin holds the fabric", Duration::from_secs(30), || async {
            let f = estate.get("/api/fabric").await.1;
            f["fabric_primary"].as_str()?;
            f["id"].as_str().filter(|i| !i.is_empty()).map(String::from)
        })
        .await;
        estate
    }

    fn write_manifest(&self) {
        let o = &self.owner;
        self.artifact(
            "manifest.json",
            &json!({
                "product": o.product, "feature": o.feature, "subfeature": o.subfeature,
                "rung": o.rung, "provider": o.provider, "test": o.test,
                "seed": self.seed, "control_api": self.admin,
                "executable_bindings": self.external.as_ref().map(|x| json!({ "mode": "explicit", "binding_file": x.file, "receipt": x.validated.receipt() })).unwrap_or_else(|| json!({ "mode": "built-in" })),
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

    /// `GET base/api/fabric`, only when the admin at `base` answers for this estate's Fabric.
    pub async fn fabric_at(&self, base: &str) -> Option<Value> {
        own_fabric_at(base, &self.fabric_id).await
    }

    pub async fn get(&self, path: &str) -> (u16, Value) {
        let r = self.http.get(format!("{}{path}", self.admin)).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// `GET path` on another admin's control API (`base`).
    pub async fn http_get(&self, base: &str, path: &str) -> (u16, Value) {
        let r = self.http.get(format!("{base}{path}")).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// `POST base+path` with `body`: status and the JSON body (or `Null`).
    pub async fn http_post(&self, base: &str, path: &str, body: &Value) -> (u16, Value) {
        let r = self.http.post(format!("{base}{path}")).json(body).send().await.unwrap_or_else(|e| panic!("POST {base}{path}: {e}"));
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// `POST` to this estate's entry admin, as is.
    pub async fn post_raw(&self, path: &str, body: &Value) -> (u16, Value) {
        self.http_post(&self.admin, path, body).await
    }

    /// `DELETE base+path`, as is.
    pub async fn delete_at(&self, base: &str, path: &str) -> (u16, Value) {
        let r = self.http.delete(format!("{base}{path}")).send().await.unwrap_or_else(|e| panic!("DELETE {base}{path}: {e}"));
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// `DELETE` at this estate's entry admin, as is.
    pub async fn delete_raw(&self, path: &str) -> (u16, Value) {
        let r = self.http.delete(format!("{}{path}", self.admin)).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// A topology request: what an operator does. Topology is accepted only by the current
    /// fabric-primary, one Build at a time, so the request goes to the fabric control endpoint
    /// the entry admin advertises, follows the seat if it answers `rejected-not-authority`, and
    /// waits out a Build still reconciling (`build-in-progress`), for up to two minutes. Every
    /// other answer is returned as is.
    async fn topology_request(&self, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut last = (0u16, Value::Null);
        loop {
            let advertised = self.get("/api/fabric").await.1["admin_api_base"].as_str().filter(|b| !b.is_empty()).map(String::from);
            let base = match advertised {
                Some(b) if self.fabric_at(&b).await.is_some() => b,
                _ => self.admin.clone(),
            };
            // Removing a node: the authority removes a birth it sees, so give its view the moment
            // gossip needs to hear a node another Mesh just created.
            if method == "DELETE" {
                if let Some(name) = path.strip_prefix("/api/nodes/") {
                    let seen_by = Instant::now() + Duration::from_secs(10);
                    while Instant::now() < seen_by {
                        let v = self.http_get(&base, "/api/nodes").await.1;
                        if v["nodes"].as_array().is_some_and(|ns| ns.iter().any(|n| n["name"] == name && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect")))) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            let r = match (method, body) {
                ("DELETE", _) => self.http.delete(format!("{base}{path}")).send().await,
                (_, Some(b)) => self.http.post(format!("{base}{path}")).json(b).send().await,
                _ => self.http.post(format!("{base}{path}")).send().await,
            };
            if let Ok(r) = r {
                let status = r.status().as_u16();
                let v = r.json().await.unwrap_or(Value::Null);
                let waits = status == 409 && matches!(v["error"].as_str(), Some("rejected-not-authority") | Some("build-in-progress"));
                if !waits {
                    return (status, v);
                }
                last = (status, v);
            }
            assert!(Instant::now() < deadline, "{method} {path}: no fabric-primary accepted it within 120s; last answer: {} {}", last.0, last.1);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn post(&self, path: &str, body: &Value) -> (u16, Value) {
        if path.starts_with("/api/build") || path.starts_with("/api/nodes") || path.starts_with("/api/meshes") {
            return self.topology_request("POST", path, Some(body)).await;
        }
        self.post_raw(path, body).await
    }

    pub async fn delete(&self, path: &str) -> (u16, Value) {
        if path.starts_with("/api/nodes") || path.starts_with("/api/meshes") {
            return self.topology_request("DELETE", path, None).await;
        }
        self.delete_raw(path).await
    }

    /// Wait until Build `id` is `complete`; a `failed` Build fails the test.
    pub async fn await_build(&self, id: &str, within: Duration) -> Value {
        wait_for(&format!("build {id} complete"), within, || async {
            let (_, b) = self.get(&format!("/api/builds?id={id}")).await;
            match b["state"].as_str() {
                Some("complete") => Some(b),
                Some("failed") => panic!("build {id} failed: {b:#}"),
                _ => None,
            }
        })
        .await
    }

    /// The attempt a 202 named: the one its request opened (attempt 1 for an accepted Build).
    pub fn attempt_of(accepted: &Value) -> u64 {
        accepted["attempt"].as_u64().unwrap_or_else(|| panic!("the 202 names the attempt it opened: {accepted}"))
    }

    /// Wait until attempt `attempt` of Build `id` is complete, where `attempt` is the one the
    /// request that opened it answered 202 with. The Build's reported attempt must have reached
    /// `attempt` and that attempt must have converged; a Build still reporting an earlier
    /// attempt's `complete` is not yet this attempt's. A failed attempt fails the test with the
    /// Build JSON.
    pub async fn await_attempt(&self, id: &str, attempt: u64, within: Duration) -> Value {
        wait_for(&format!("build {id} attempt {attempt} complete"), within, || async {
            let (_, b) = self.get(&format!("/api/builds?id={id}")).await;
            attempt_verdict(id, attempt, &b)
        })
        .await
    }

    pub async fn nodes(&self) -> Vec<Value> {
        let (status, v) = self.get("/api/nodes").await;
        assert_eq!(status, 200, "GET /api/nodes: {v}");
        v["nodes"].as_array().cloned().unwrap_or_default()
    }

    /// `GET /api/nodes` from the admin serving `base` (another admin's view).
    pub async fn nodes_at(&self, base: &str) -> Vec<Value> {
        let r = self.http.get(format!("{base}/api/nodes")).send().await.expect("control API reachable");
        assert_eq!(r.status().as_u16(), 200, "GET {base}/api/nodes");
        let v: Value = r.json().await.unwrap_or(Value::Null);
        v["nodes"].as_array().cloned().unwrap_or_default()
    }

    /// The view once every cohort holds exactly its desired count of nodes
    /// (`(mesh, node_admin, rpc_node)`), every one ready for traffic. A
    /// Build's shape is counts, not paths: shrink retires non-primaries, so
    /// which ordinals remain depends on where the seats are.
    pub async fn settled_shape(&self, meshes: &[(&str, u32, u32)], within: Duration) -> Vec<Value> {
        let want: std::collections::BTreeMap<(String, String), usize> = meshes
            .iter()
            .flat_map(|(m, a, r)| [((m.to_string(), "node_admin".to_string()), *a as usize), ((m.to_string(), "rpc_node".to_string()), *r as usize)])
            .collect();
        let until = Instant::now() + within;
        loop {
            let nodes = self.nodes().await;
            let mut have: std::collections::BTreeMap<(String, String), usize> = std::collections::BTreeMap::new();
            for n in &nodes {
                *have.entry((n["mesh"].as_str().unwrap_or("").into(), n["kind"].as_str().unwrap_or("").into())).or_default() += 1;
            }
            if have == want && nodes.iter().all(|n| n["status"] == "ready-for-traffic") {
                return nodes;
            }
            if Instant::now() >= until {
                panic!("the view never settled on {want:?} within {within:?}; last view: {have:?}: {nodes:#?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `node`'s data dir. Only the admin that launched a node advertises it,
    /// and that is whichever admin executed its creation, so every live
    /// admin's view is asked.
    pub async fn data_dir_of(&self, node: &str) -> String {
        let bases: Vec<String> = std::iter::once(self.admin.clone())
            .chain(self.nodes().await.iter().filter(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic").filter_map(|n| n["admin_api_base"].as_str().map(String::from)))
            .collect();
        for base in bases {
            let Ok(r) = self.http.get(format!("{base}/api/nodes")).timeout(Duration::from_secs(2)).send().await else { continue };
            let v: Value = r.json().await.unwrap_or(Value::Null);
            let dir = v["nodes"].as_array().into_iter().flatten().find(|n| n["name"] == node).and_then(|n| n["data_dir"].as_str().map(String::from));
            if let Some(dir) = dir {
                return dir;
            }
        }
        panic!("no live admin advertises {node}'s data dir")
    }

    /// The pid of `node`'s runtime: its process (process provider: its `deployment.json`), or its
    /// container's init as the host sees it (container provider).
    pub async fn pid_of(&self, node: &str) -> u64 {
        if self.owner.provider == "container" {
            let id = self.container_of(node).unwrap_or_else(|| panic!("{node}: no running container of fabric {}", self.fabric_id));
            return docker(&["inspect", "--format", "{{.State.Pid}}", &id]).ok().and_then(|p| p.parse().ok()).filter(|p| *p > 0).unwrap_or_else(|| panic!("{node}: container {id} has no running init"));
        }
        let dir = self.data_dir_of(node).await;
        let d: Value = serde_json::from_slice(&std::fs::read(format!("{dir}/deployment.json")).unwrap()).unwrap();
        d["pid"].as_u64().unwrap_or_else(|| panic!("{node}: no pid in {d}"))
    }

    /// SIGKILL `node`'s runtime (a fault: it flushes nothing).
    pub async fn kill_node(&self, node: &str) {
        let pid = self.pid_of(node).await;
        let ok = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success();
        assert!(ok, "kill -9 {pid} ({node})");
    }

    /// The view once it holds exactly `names`, every one ready for traffic.
    /// A Build can complete on another admin (a mesh's own primary) before
    /// this admin hears the last member it created: the view converges
    /// within gossip delay, and a view that does not within `within` fails.
    pub async fn settled(&self, names: &std::collections::BTreeSet<String>, within: Duration) -> Vec<Value> {
        let deadline = Instant::now() + within;
        loop {
            let nodes = self.nodes().await;
            let have: std::collections::BTreeSet<String> = nodes.iter().filter_map(|n| n["name"].as_str().map(str::to_string)).collect();
            if &have == names && nodes.iter().all(|n| n["status"] == "ready-for-traffic") {
                return nodes;
            }
            if Instant::now() >= deadline {
                let seen: Vec<String> = nodes.iter().map(|n| format!("{}={}", n["name"], n["status"])).collect();
                panic!("the view never settled on {names:?} within {within:?}; last view: {seen:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn node(&self, name: &str) -> Value {
        self.node_opt(name).await.unwrap_or_else(|| panic!("no node {name}"))
    }

    /// The node at `name` if the view lists it now. A departed birth leaves the view before its
    /// replacement's record appears, so a wait on a path uses this, never [`Self::node`].
    pub async fn node_opt(&self, name: &str) -> Option<Value> {
        self.nodes().await.into_iter().find(|n| n["name"] == name)
    }

    /// Run one probe invocation and record it in the RPC ledger.
    pub fn probe(&self, args: &[&str]) -> Value {
        self.probe_handle().run(&self.admin, args).unwrap_or_else(|e| panic!("{e}"))
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

    /// Every span every process of this estate wrote.
    pub fn spans(&self) -> Vec<Value> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(&self.evidence).into_iter().flatten().flatten() {
            if e.path().to_string_lossy().ends_with(".spans.jsonl") {
                let text = std::fs::read_to_string(e.path()).unwrap_or_default();
                out.extend(text.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
            }
        }
        out
    }

    /// The trace URL for `trace_id` when a Jaeger query base is configured.
    pub fn record_trace_url(&self, trace_id: &str) {
        let base = std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://localhost:16686".into());
        std::fs::write(self.artifacts.join("trace-url.txt"), format!("{base}/trace/{trace_id}\n")).unwrap();
    }

    /// Stop the whole estate through the runtime-administration route of the
    /// admin that holds the fabric now (as `self.admin` sees it: drift
    /// recovery can move the fabric back to a recovered mesh) and wait until
    /// every runtime of the estate has exited. Every process flushes its
    /// evidence on exit, so read `spans()` after this.
    pub async fn stop(&mut self) {
        let entry = self.admin.clone();
        let (_, fabric) = self.get("/api/fabric").await;
        if let Some(holder) = fabric["admin_api_base"].as_str().filter(|b| !b.is_empty()) {
            if self.fabric_at(holder).await.is_some() {
                self.admin = holder.to_string();
            }
        }
        // Only the fabric-primary accepts the shutdown, and the seat can still be moving when the
        // test asks: follow the advertised fabric control endpoint until one accepts, and name the
        // last refusal rather than waiting on a shutdown nobody began.
        let until = Instant::now() + Duration::from_secs(30);
        let mut last: (u16, Value);
        loop {
            // The shutdown goes only to an admin of this estate's Fabric: a dead admin's port can
            // already belong to another estate's admin.
            let r = match self.fabric_at(&self.admin).await {
                Some(_) => self.http.post(format!("{}/api/shutdown", self.admin)).json(&json!({})).send().await.map_err(|e| e.to_string()),
                None => Err(format!("{} does not answer for Fabric {}", self.admin, self.fabric_id)),
            };
            let refused_or_gone = match r {
                Ok(r) => {
                    let status = r.status().as_u16();
                    let body = r.json().await.unwrap_or(Value::Null);
                    if status == 202 {
                        break;
                    }
                    last = (status, body);
                    true
                }
                Err(e) => {
                    last = (0, Value::String(e));
                    true
                }
            };
            // The advertised holder refused or is gone: ask the entry admin, then every admin it
            // lists, who holds the fabric now.
            if refused_or_gone {
                let mut candidates = vec![entry.clone()];
                let entry_is_ours = self.fabric_at(&entry).await.is_some();
                if let Some(r) = if entry_is_ours { self.http.get(format!("{entry}/api/nodes")).timeout(Duration::from_secs(2)).send().await.ok() } else { None } {
                    let v: Value = r.json().await.unwrap_or(Value::Null);
                    candidates.extend(v["nodes"].as_array().into_iter().flatten().filter(|n| n["kind"] == "node_admin" && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).filter_map(|n| n["admin_api_base"].as_str().map(String::from)));
                }
                for c in candidates {
                    let Some(f) = self.fabric_at(&c).await else { continue };
                    if let Some(base) = f["admin_api_base"].as_str().filter(|b| !b.is_empty() && *b != self.admin) {
                        self.admin = base.to_string();
                        break;
                    }
                }
            }
            assert!(Instant::now() < until, "no admin accepted /api/shutdown within 30s; last answer from {}: {} {}", self.admin, last.0, last.1);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // The bootstrap admin is stopped by the shutdown like any other admin; one still running
        // when the bound passes is named and killed, so a stop never hangs on it. The bound covers
        // the drain bound a stopper waits on (`shutdown::drain_bound`) plus the grace of one stop.
        let until = Instant::now() + Duration::from_secs(30);
        let mut bootstrap = self.bootstrap.take();
        if let Some(c) = bootstrap.as_mut() {
            while Instant::now() < until && c.try_wait().ok().flatten().is_none() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if c.try_wait().ok().flatten().is_none() {
                // Its control API may already be closed (it closes before the Leaving linger ends):
                // the view is evidence when it answers, never a condition of the stop.
                let fabric = match self.http.get(format!("{}/api/fabric", self.admin)).timeout(Duration::from_secs(2)).send().await {
                    Ok(r) => r.json::<Value>().await.unwrap_or(Value::Null),
                    Err(e) => Value::String(format!("control API closed: {e}")),
                };
                eprintln!("estate stop: bootstrap admin pid {} still ran when the shutdown bound passed; killed. Its fabric view: {fabric}", c.id());
                let _ = c.kill();
                let _ = c.wait();
            }
        }
        // A restarted admin is stopped by the shutdown like any other; one still running when the
        // bound passes is named and killed, so a stop never hangs on it.
        for mut c in std::mem::take(&mut self.restarted) {
            while Instant::now() < until && c.try_wait().ok().flatten().is_none() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if c.try_wait().ok().flatten().is_none() {
                eprintln!("estate stop: restarted admin pid {} still ran when the shutdown bound passed; killed", c.id());
                let _ = c.kill();
                let _ = c.wait();
            }
        }
        while Instant::now() < until && (!self.live_runtimes().is_empty() || !self.live_containers().is_empty()) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The bootstrap admin's process id while the estate still holds it. The bootstrap runtime is
    /// adopted on day 0, so it has no provider `deployment.json`.
    pub fn bootstrap_pid(&self) -> Option<u32> {
        self.bootstrap.as_ref().map(|c| c.id())
    }

    /// The bootstrap admin's data dir.
    pub fn bootstrap_data_dir(&self, mesh: &str) -> PathBuf {
        self.root.join(format!("{mesh}.admin.1"))
    }

    /// Start a node-admin on `data_dir` it ran on before: it finds its own row in nodes.storage and
    /// restarts as the same logical node. Only the operator's environment is given (provider,
    /// binaries, evidence); identity, Mesh and Fabric come from its storage. Returns its control
    /// API base.
    pub fn restart_admin(&mut self, data_dir: &Path) -> String {
        self.restart_admin_with(data_dir, &[])
    }

    /// [`Self::restart_admin`] with more operator environment (the recovery flags
    /// `RDM_MESH_PRIMARY` and `RDM_FABRIC_PRIMARY`).
    pub fn restart_admin_with(&mut self, data_dir: &Path, operator_env: &[(&str, &str)]) -> String {
        let mut env = vec![
            ("MESH_SPAWN_TYPE", self.owner.provider.clone()),
            ("RDM_DATA_DIR", data_dir.display().to_string()),
            ("RDM_BIN_DIR", bin_dir().display().to_string()),
            ("RDM_EVIDENCE_DIR", self.evidence.display().to_string()),
            ("RDM_ESTATE_ROOT", self.root.display().to_string()),
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
                binary("rafka-node-admin")
            }
        };
        let (child, base) = spawn_admin(&exe, &env, &format!("restarted node-admin on {}", data_dir.display()));
        self.restarted.push(child);
        base
    }

    /// Stop every runtime of this estate with a local signal (SIGTERM), as an operator does when
    /// no admin can stop the fabric, and wait for them.
    pub async fn stop_locally(&mut self) {
        let mut pids: Vec<u32> = self.restarted.iter().map(|c| c.id()).chain(self.bootstrap.as_ref().map(|c| c.id())).collect();
        pids.extend(self.live_runtimes().into_iter().map(|(_, p)| p));
        for p in &pids {
            let _ = std::process::Command::new("kill").args(["-TERM", &p.to_string()]).status();
        }
        let until = Instant::now() + Duration::from_secs(30);
        while Instant::now() < until && pids.iter().any(|p| Path::new(&format!("/proc/{p}")).exists() && !std::fs::read_to_string(format!("/proc/{p}/stat")).unwrap_or_default().contains(") Z ")) {
            for c in self.restarted.iter_mut().chain(self.bootstrap.as_mut()) {
                let _ = c.try_wait();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for mut c in std::mem::take(&mut self.restarted).into_iter().chain(self.bootstrap.take()) {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// SIGKILL `pid` (a fault: it flushes nothing) and wait until it is gone.
    pub fn kill_pid(&self, pid: u32) {
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

    /// SIGKILL the bootstrap admin (a fault: it flushes nothing).
    pub fn kill_bootstrap(&mut self) {
        if let Some(mut c) = self.bootstrap.take() {
            // A containerised Day-0 admin: the kill reaches its container, not only the attached CLI.
            if self.owner.provider == "container" {
                if let Some(id) = self.container_of(&format!("{}.admin.1", self.root_mesh())) {
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

    /// The running container of `node` (container provider).
    pub fn container_of(&self, node: &str) -> Option<String> {
        self.live_containers().into_iter().find(|(n, _)| n == node).map(|(_, id)| id)
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
            if Path::new(&format!("/proc/{pid}")).exists() && !std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default().contains(") Z ") {
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
        if self.bootstrap.is_none() {
            sweep(self);
        }
        if let Some(mut c) = self.bootstrap.take() {
            // Best-effort fabric shutdown on a failed run, then the bootstrap process.
            if let Some(hostport) = self.admin.strip_prefix("http://") {
                if let Ok(mut s) = std::net::TcpStream::connect(hostport.trim_end_matches('/')) {
                    let _ = s.set_write_timeout(Some(Duration::from_secs(2)));
                    let _ = write!(
                        s,
                        "POST /api/shutdown HTTP/1.1\r\nHost: {hostport}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                    );
                }
            }
            // Give it the time to stop what it started before forcing it.
            let until = Instant::now() + Duration::from_secs(20);
            while Instant::now() < until && matches!(c.try_wait(), Ok(None)) {
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = c.kill();
            let _ = c.wait();
            sweep(self);
        }
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

/// Kill, until none is left, every process whose environment names `root` (`RDM_ESTATE_ROOT`: every
/// admin and every node an admin launched inherits it): the reaper's own rule, applied by the
/// estate when it ends, so a failed run leaves nothing behind for the reaper to find later.
fn kill_estate_processes(root: &Path) {
    let needle = format!("RDM_ESTATE_ROOT={}\0", root.display());
    for _ in 0..10 {
        let mut found = false;
        for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
            let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
            if pid == std::process::id() {
                continue;
            }
            let Ok(env) = std::fs::read(format!("/proc/{pid}/environ")) else { continue };
            if env.windows(needle.len()).any(|w| w == needle.as_bytes()) {
                found |= Command::new("kill").args(["-9", &pid.to_string()]).status().is_ok_and(|s| s.success());
            }
        }
        if !found {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// One estate's probe door: runs `rafka-rpc-probe` against an admin's node view and appends the
/// invocation to the estate's RPC ledger (`rpc-ledger.jsonl`).
#[derive(Clone)]
pub struct ProbeHandle {
    evidence: PathBuf,
    artifacts: PathBuf,
    bind: Option<String>,
}

impl ProbeHandle {
    /// One probe invocation through the admin at `admin`. `Err` names an invocation that printed
    /// no JSON line at all; a probe that printed a line (a typed outcome, or `Refused`) is `Ok`.
    pub fn run(&self, admin: &str, args: &[&str]) -> Result<Value, String> {
        let mut cmd = Command::new(binary("rafka-rpc-probe"));
        cmd.arg("--admin").arg(admin).args(args).env("RDM_EVIDENCE_DIR", &self.evidence);
        if let Some(gw) = &self.bind {
            cmd.env("RDM_PROBE_BIND", gw);
        }
        let out = cmd.output().map_err(|e| format!("run rafka-rpc-probe: {e}"))?;
        let line = String::from_utf8_lossy(&out.stdout);
        let v: Value = serde_json::from_str(line.trim()).map_err(|e| format!("probe {args:?} printed no JSON ({e}): {line} / {}", String::from_utf8_lossy(&out.stderr)))?;
        // A probe that did not exit cleanly says how, so its missing evidence has a reason on record.
        let stderr = String::from_utf8_lossy(&out.stderr);
        let entry = if out.status.success() && !stderr.contains("panicked") {
            json!({ "args": args, "result": v })
        } else {
            json!({ "args": args, "result": v, "exit": out.status.code(), "signal": std::os::unix::process::ExitStatusExt::signal(&out.status), "stderr_tail": stderr.chars().rev().take(1200).collect::<Vec<_>>().into_iter().rev().collect::<String>() })
        };
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(self.artifacts.join("rpc-ledger.jsonl")).map_err(|e| e.to_string())?;
        writeln!(f, "{entry}").map_err(|e| e.to_string())?;
        Ok(v)
    }
}

/// Spans by name.
pub fn named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

/// True when `child` descends from `ancestor` by `parent_span_id` links.
/// The node-admin that decided the claim of `attempt` of `build_id` as won (the fabric-primary of that
/// moment; `None` when no claim of it was won), read from its `via-claim-decision` span. An attempt's reconcile continues the trace of
/// the request that created the attempt only while the fabric-primary that holds that request's
/// context decides the claim; after the seat moved, the new fabric-primary starts the attempt's trace.
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

/// `GET base/api/fabric`, only when the admin at `base` answers within 2s and its view names the
/// Fabric `fabric_id`. A loopback control port freed by a dead runtime is reused by other estates
/// on the host, so an answer from an address is never taken as this Fabric's on its own.
pub async fn own_fabric_at(base: &str, fabric_id: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}/api/fabric")).timeout(Duration::from_secs(2)).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    let f: Value = r.json().await.ok()?;
    (f["id"].as_str() == Some(fabric_id)).then_some(f)
}

/// What one reading of Build `id` says about attempt `attempt`: its converged Build, or nothing
/// yet. A failed attempt panics with the Build JSON.
pub fn attempt_verdict(id: &str, attempt: u64, b: &Value) -> Option<Value> {
    // The Build reports the highest attempt claimed; a reading below the awaited attempt is an
    // earlier attempt's state (its `complete` included), never this attempt's.
    if b["attempt"].as_u64()? < attempt {
        return None;
    }
    match b["state"].as_str() {
        Some("complete") => Some(b.clone()),
        Some("failed") => panic!("build {id} failed: {b:#}"),
        _ => None,
    }
}

#[cfg(test)]
mod attempt_verdict_tests {
    use super::*;

    fn at(state: &str, attempt: u64) -> Value {
        json!({"build_id": "b", "state": state, "attempt": attempt})
    }

    /// CONTRACT: a reading that still carries the previous attempt's `complete` is not the
    /// awaited attempt's verdict, and neither is the opened-but-unclaimed or running reading.
    #[test]
    fn the_previous_attempts_complete_is_not_the_awaited_attempts_verdict() {
        assert!(attempt_verdict("b", 2, &at("complete", 1)).is_none(), "the previous attempt's complete");
        assert!(attempt_verdict("b", 2, &at("pending", 1)).is_none(), "opened, not yet claimed");
        assert!(attempt_verdict("b", 2, &at("running", 2)).is_none(), "claimed, running");
    }

    /// CONTRACT: the awaited attempt converging, or a later one, is the verdict.
    #[test]
    fn the_awaited_attempt_or_a_later_one_complete_is_the_verdict() {
        assert!(attempt_verdict("b", 2, &at("complete", 2)).is_some());
        assert!(attempt_verdict("b", 2, &at("complete", 3)).is_some());
    }

    /// CONTRACT: a failed awaited attempt fails the test carrying the Build; an earlier attempt's
    /// failure is not this attempt's.
    #[test]
    fn a_failed_awaited_attempt_panics_with_the_build_and_an_earlier_failure_does_not() {
        assert!(attempt_verdict("b", 2, &at("failed", 1)).is_none());
        let e = std::panic::catch_unwind(|| attempt_verdict("b", 2, &at("failed", 2))).expect_err("a failed attempt fails the test");
        let msg = e.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("build b failed") && msg.contains("\"attempt\": 2"), "{msg}");
    }
}

#[cfg(test)]
mod spawn_tests {
    /// CONTRACT: a test that births a second estate in the same process gets its own root, and
    /// dropping the first estate's scope leaves the second estate's root in place.
    #[test]
    fn a_reborn_estate_never_shares_or_loses_its_root_to_the_estate_it_replaces() {
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

    /// CONTRACT: a node-admin that never advertises its control API is refused by name, and the
    /// refusal leaves nothing running: no Estate exists yet to stop it. What must NOT happen: a
    /// panic that leaves the process alive.
    #[test]
    fn an_admin_that_never_advertises_is_stopped_before_the_refusal() {
        let dir = std::env::temp_dir().join(format!("rdm-spawn-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let script = dir.join("silent-admin.sh");
        let pid_file = dir.join("pid");
        std::fs::write(&script, format!("#!/bin/sh\necho $$ > {}\nexec sleep 60 >/dev/null\n", pid_file.display())).unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        let err = std::panic::catch_unwind(|| spawn_admin(&script, &[], "the silent admin")).expect_err("refused");
        let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("the silent admin never advertised RDM_NODE_ADMIN_API_BASE"), "{msg}");
        let pid: u32 = std::fs::read_to_string(&pid_file).unwrap().trim().parse().unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists() || std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| s.contains(") Z ")), "pid {pid} still runs after the refusal");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The node and admin logs under an estate root, kept in the estate's artifacts before the root is
/// removed: `<node dir>/stdout.log` and `stderr.log`.
fn keep_node_logs(root: &Path, artifacts: &Path) {
    let dest = artifacts.join("node-logs");
    for e in std::fs::read_dir(root).into_iter().flatten().flatten() {
        for name in ["stdout.log", "stderr.log"] {
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

    // CONTRACT: a binary is refused by name when a source file its dep-info lists is newer than it, and
    // accepted when none is; a binary with no dep-info beside it is not judged.
    #[test]
    fn a_binary_older_than_its_source_is_refused_by_name_and_a_fresh_one_is_not() {
        let dir = std::env::temp_dir().join(format!("rdm-stale-bin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("lib one.rs");
        std::fs::write(&src, "fn main() {}").unwrap();
        let exe = dir.join("rafka-rpc-probe");
        let set = |path: &Path, secs_from_epoch: u64| {
            let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            f.set_modified(std::time::UNIX_EPOCH + Duration::from_secs(secs_from_epoch)).unwrap();
        };
        std::fs::write(&exe, "bin").unwrap();
        assert_eq!(binary_is_fresh("rafka-rpc-probe", &exe), Ok(()), "no dep-info beside the binary: not judged");
        std::fs::write(exe.with_extension("d"), format!("{}: {}\n", exe.display(), src.display().to_string().replace(' ', "\\ "))).unwrap();
        set(&src, 1_000);
        set(&exe, 2_000);
        assert_eq!(binary_is_fresh("rafka-rpc-probe", &exe), Ok(()), "the source is older than the binary");
        set(&src, 3_000);
        let why = binary_is_fresh("rafka-rpc-probe", &exe).unwrap_err();
        assert!(why.contains("rafka-rpc-probe") && why.contains("lib one.rs") && why.contains("cargo build -p rafka-node-rpc-testkit --bin rafka-rpc-probe"), "{why}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
