//! `ProcessDeploymentProvider`: one OS process per node on the shared host
//! network namespace (PRD §10: the process provider is the port-collision gate).
//!
//! The process is started detached in its own process group so it outlives the
//! admin that launched it. Its runtime is the pid together with the kernel's
//! start time of that pid, within this host's boot and pid namespace (the
//! control domain): a successor admin adopts it from the birth's published
//! runtime fact, and a recycled pid never matches the old birth.

use super::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
#[cfg(test)]
use rdm_core::RuntimeLocator;
use crate::model::ProviderKind;
use rdm_core::runtime::{process_control_domain, process_start_token};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

/// Written into the node's data dir at spawn: `{"deployment_id", "pid"}`.
pub(crate) const DEPLOYMENT_FILE: &str = "deployment.json";

/// One child this provider started: its deployment, and its exit once seen.
#[derive(Debug, Clone)]
struct Launched {
    deployment_id: crate::model::DeploymentId,
    start: Option<u64>,
    /// `None` while it runs; `Some(code)` once it exited (reaped).
    exit: Option<Option<i32>>,
}

/// Where a test loses the launching executor in a spawn. Empty outside tests.
#[derive(Clone, Default)]
struct Failpoints {
    /// The executor is lost the instant the child exists: the pid is stored here and the spawn
    /// fails with nothing further done (no child registered, no watcher).
    #[cfg(test)]
    lost_after_spawn: Option<std::sync::Arc<Mutex<Option<u32>>>>,
    /// The executor is lost while the child waits at the gate, before the launch record is
    /// written: the pid is stored here and the spawn fails there.
    #[cfg(test)]
    lost_before_record: Option<std::sync::Arc<Mutex<Option<u32>>>>,
}

impl Failpoints {
    /// The child `pid` waits at the gate and its launch record is not written: fail the spawn here
    /// when a test lost the executor at this point.
    fn before_record(&self, pid: u32) -> Result<(), String> {
        #[cfg(test)]
        if let Some(slot) = &self.lost_before_record {
            *slot.lock().unwrap() = Some(pid);
            return Err(format!("failpoint: the executor is lost before it records pid {pid}"));
        }
        let _ = pid;
        Ok(())
    }

    /// The spawn of child `pid` has returned: fail it here when a test lost the executor at this point.
    fn after_spawn(&self, pid: u32) -> Result<(), String> {
        #[cfg(test)]
        if let Some(slot) = &self.lost_after_spawn {
            *slot.lock().unwrap() = Some(pid);
            return Err(format!("failpoint: the executor is lost after spawning pid {pid}"));
        }
        let _ = pid;
        Ok(())
    }
}

/// Realises runtimes as host processes (`docs/architecture/deployment-provider.md` §2).
pub struct ProcessDeploymentProvider {
    /// Children this provider started. A watcher thread per child waits on
    /// it, so an exit is reaped at once (no zombie) and its code recorded.
    children: std::sync::Arc<Mutex<HashMap<u32, Launched>>>,
    /// Moves each time a child's watcher reaps its exit ([`DeploymentProvider::exits`]).
    exits: std::sync::Arc<tokio::sync::watch::Sender<u64>>,
    /// This host's boot and pid namespace.
    domain: String,
    failpoints: Failpoints,
}

impl Default for ProcessDeploymentProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessDeploymentProvider {
    /// A provider with no children.
    pub fn new() -> Self {
        Self { children: Default::default(), exits: std::sync::Arc::new(tokio::sync::watch::Sender::new(0)), domain: process_control_domain().unwrap_or_else(|e| format!("process:unknown:{e}")), failpoints: Failpoints::default() }
    }

    /// A provider whose executor is lost the instant a child exists ([`Failpoints::lost_after_spawn`]).
    #[cfg(test)]
    fn lost_after_spawn(slot: std::sync::Arc<Mutex<Option<u32>>>) -> Self {
        Self { failpoints: Failpoints { lost_after_spawn: Some(slot), ..Default::default() }, ..Self::new() }
    }

    /// A provider whose executor is lost while a child waits at the gate ([`Failpoints::lost_before_record`]).
    #[cfg(test)]
    fn lost_before_record(slot: std::sync::Arc<Mutex<Option<u32>>>) -> Self {
        Self { failpoints: Failpoints { lost_before_record: Some(slot), ..Default::default() }, ..Self::new() }
    }

    fn handle(&self, deployment_id: crate::model::DeploymentId, pid: u32, start: Option<u64>) -> DeploymentHandle {
        DeploymentHandle { deployment_id, provider: ProviderKind::Process, pid: Some(pid), start, container: None, domain: Some(self.domain.clone()) }
    }
}

/// `pid` is the process `start` names: alive, and started then. A handle
/// without a start token is trusted on the pid alone only for a child this
/// provider holds. A process whose identity cannot be read is not proven to be it.
#[cfg(unix)]
fn is(pid: u32, start: Option<u64>) -> bool {
    matches!(exact_group(PROC, pid, start), Group::Running | Group::OsStopped)
}

/// The stop ladder's wait: until `exited` answers yes, for at most `grace`; then `kill`, and up to
/// two seconds more for the exit it forces. Only `exited` ends a wait: a runtime that cannot be read
/// is waited on, never read as gone (deployment-provider.md §2.6).
#[cfg(unix)]
async fn wait_for_exit<E, F>(grace: Duration, mut exited: E, kill: impl FnOnce())
where
    E: FnMut() -> F,
    F: std::future::Future<Output = bool>,
{
    let until = tokio::time::Instant::now() + grace;
    loop {
        if exited().await {
            return;
        }
        if tokio::time::Instant::now() >= until {
            kill();
            for _ in 0..100 {
                if exited().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The process `start` names has exited: its group read over `proc` is `Gone`. A group that cannot
/// be read whole is not proven exited (deployment-provider.md §2.6: unreadable or partial is
/// `Unknown`): a stop waits on, as inspect answers it. [`is`] is the other question, whether a
/// process is proven to be the one to signal, where an unreadable identity is not.
#[cfg(unix)]
fn gone_at(proc: &str, pid: u32, start: Option<u64>) -> bool {
    exact_group(proc, pid, start) == Group::Gone
}

/// The host's process filesystem, which every reader below reads.
const PROC: &str = "/proc";

/// What the operating system shows of a thread group (deployment-provider.md §2.6): the state
/// letter of every task in `/proc/<pid>/task/*/stat`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Group {
    /// A live task is in a state other than `T` or `t`.
    Running,
    /// Every live task is in `T` (stopped by a signal) or `t` (stopped by a tracer).
    OsStopped,
    /// No task is live (each a zombie or dead), or `/proc/<pid>` is gone.
    Gone,
    /// The task files are present and cannot be read whole: never exited verdict. `path` is the file or
    /// directory whose read failed and `cause` the error or the line that names no state.
    Unreadable { path: String, cause: String },
}

impl Group {
    fn unreadable(path: impl std::fmt::Display, cause: impl std::fmt::Display) -> Self {
        Group::Unreadable { path: path.to_string(), cause: cause.to_string() }
    }
}

/// The thread group of the process `start` names: a pid that now runs another process (its start
/// token is another) is `Gone`, as is a pid that no longer exists; a start token that cannot be read
/// is `Unreadable`. A handle without a start token is read on the pid alone.
fn exact_group(proc: &str, pid: u32, start: Option<u64>) -> Group {
    if let Some(s) = start {
        let path = format!("{proc}/{pid}/stat");
        if let Some(g) = start_token_group(&path, std::fs::read_to_string(&path), s) {
            return g;
        }
    }
    group(proc, pid)
}

/// The leader's `stat` read at `path` against the handle's start token `start`: `None` when it names
/// that process, so its thread group is read; `Gone` when it names another process or the pid no
/// longer exists (gone before the open, or released between the open and the read: [`task_exited`]);
/// `Unreadable` when no start token can be read from it.
fn start_token_group(path: &str, read: std::io::Result<String>, start: u64) -> Option<Group> {
    match read {
        Ok(stat) => match rdm_core::runtime::start_token_of_stat(&stat) {
            Some(t) if t != start => Some(Group::Gone),
            Some(_) => None,
            None => Some(Group::unreadable(path, format!("no start token in {stat:?}"))),
        },
        Err(e) if task_exited(&e) => Some(Group::Gone),
        Err(e) => Some(Group::unreadable(path, e)),
    }
}

/// The state of `pid`'s whole thread group. A task listed and gone before its `stat` was read has
/// exited; any other task whose `stat` cannot be read, or whose line names no state, makes the
/// group `Unreadable`.
fn group(proc: &str, pid: u32) -> Group {
    let dir = format!("{proc}/{pid}/task");
    match std::fs::read_dir(&dir) {
        Ok(tasks) => {
            let mut letters = Vec::new();
            for task in tasks {
                let task = match task {
                    Ok(task) => task,
                    Err(e) => return Group::unreadable(&dir, e),
                };
                let path = task.path().join("stat");
                match std::fs::read_to_string(&path) {
                    Ok(stat) => match state_letter(&stat) {
                        Some(letter) => letters.push(letter),
                        None => return Group::unreadable(path.display(), format!("no state letter in {stat:?}")),
                    },
                    Err(e) if task_exited(&e) => {}
                    Err(e) => return Group::unreadable(path.display(), e),
                }
            }
            fold_group(letters)
        }
        // No task directory: the pid is gone when `/proc/<pid>` is; present without its tasks, or
        // /proc not readable here, it cannot be told.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => match std::fs::metadata(format!("{proc}/{pid}")) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Group::Gone,
            Ok(_) => Group::unreadable(&dir, format!("absent while {proc}/{pid} is present")),
            Err(m) => Group::unreadable(&dir, format!("absent, and {proc}/{pid} cannot be read: {m}")),
        },
        Err(e) => Group::unreadable(&dir, e),
    }
}

/// A task's `stat` read failed because the task has exited: it was gone before the file was opened
/// (`NotFound`), or released between the open and the read (`ESRCH`, what the open `stat` of a
/// released task answers).
fn task_exited(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound || e.raw_os_error() == Some(libc::ESRCH)
}

/// The state letter of a `stat` line (`/proc/<pid>/stat` or `/proc/<pid>/task/<tid>/stat`): the
/// first field after the command's closing parenthesis. `None` when the line is not one.
fn state_letter(stat: &str) -> Option<char> {
    stat.rsplit_once(')').and_then(|(_, rest)| rest.trim_start().chars().next())
}

/// Fold the state letters of a thread group's tasks: a zombie (`Z`) or dead (`X`, `x`) task is not
/// live; every live task in `T` or `t` is `OsStopped`; any other live task is `Running`; no live
/// task is `Gone`.
fn fold_group(letters: impl IntoIterator<Item = char>) -> Group {
    let (mut live, mut stopped) = (0usize, 0usize);
    for letter in letters {
        match letter {
            'Z' | 'X' | 'x' => {}
            'T' | 't' => {
                live += 1;
                stopped += 1;
            }
            _ => live += 1,
        }
    }
    match (live, stopped) {
        (0, _) => Group::Gone,
        (live, stopped) if live == stopped => Group::OsStopped,
        _ => Group::Running,
    }
}

#[cfg(unix)]
fn signal(pid: u32, sig: i32) -> bool {
    // SAFETY: kill(2) with a pid we were handed; no memory is touched.
    unsafe { libc::kill(pid as i32, sig) == 0 }
}

/// A live task of `pid`'s thread group exists, running or OS-suspended. A process is gone only once
/// every task of its thread group has exited. The leader's own state is not proof: during
/// `exit_group` the leader can already read as a zombie while its sibling threads are still
/// exiting, and the group's files (its sockets) are released only by the last of them. Judging the
/// leader alone let a restart spawn the next birth while the old one still held the transport port
/// (`WaitForBind: Address already in use`).
#[cfg(unix)]
fn alive(pid: u32) -> bool {
    matches!(group(PROC, pid), Group::Running | Group::OsStopped)
}

/// Gated spawns of this process, one at a time: a gate's write end is open in this process from its
/// pipe until the child closes its copy, and a second gated child forked in that span would hold
/// it, so the loss of this process would not release the first (each waiting on the other).
static GATED_SPAWN: Mutex<()> = Mutex::new(());

/// In the forked child, before its executable runs: announce the child's pid on `pid_w`, then wait
/// on `gate_r` for the one byte that releases it. The gate's end of EOF (the executor that held the
/// write end is gone, or gave it up) cancels the launch: the executable never runs. Only
/// async-signal-safe calls (`close`, `getpid`, `write`, `read`), and no allocation.
#[cfg(unix)]
fn hold_at_gate(gate_r: i32, gate_w: i32, pid_w: i32) -> std::io::Result<()> {
    // SAFETY: close(2), getpid(2), write(2) and read(2) on descriptors this child inherited; the
    // buffers are on its stack. The child's copy of the gate's write end is closed first, so the
    // executor's loss is what ends the wait.
    unsafe {
        libc::close(gate_w);
        let pid = libc::getpid().to_ne_bytes();
        loop {
            match libc::write(pid_w, pid.as_ptr().cast(), pid.len()) {
                4 => break,
                n if n < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) => continue,
                _ => return Err(std::io::Error::last_os_error()),
            }
        }
        let mut byte = 0u8;
        loop {
            match libc::read(gate_r, std::ptr::addr_of_mut!(byte).cast(), 1) {
                1 => return Ok(()),
                0 => return Err(std::io::Error::from_raw_os_error(libc::ECANCELED)),
                _ if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) => continue,
                _ => return Err(std::io::Error::last_os_error()),
            }
        }
    }
}

/// `cmd`'s child, whose executable does not run until `record(pid)` has returned `Ok`: the launch
/// record is durable before the process it names runs, so a process that runs always has its record
/// (lifecycles/node-create.md step 1: record the exact deployment, then launch its executable).
/// The pid exists only once the child is forked, so the child is forked first and waits at a gate
/// (a pipe) before it executes: it announces its pid on a second pipe, the executor records it, then
/// releases the gate. `record` failing closes the gate and the child ends without running its
/// executable; an executor lost anywhere in between closes the gate the same way. Nothing here
/// times out: each read ends on the byte or on the end of the pipe.
#[cfg(unix)]
fn spawn_gated(cmd: &mut std::process::Command, record: impl FnOnce(u32) -> Result<(), String>) -> Result<std::process::Child, String> {
    use std::io::Read as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::process::CommandExt as _;
    let pipe = || -> Result<(OwnedFd, OwnedFd), String> {
        let mut fds = [0i32; 2];
        // SAFETY: pipe2(2) fills the two-element array it is given; both ends are close-on-exec.
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(format!("pipe: {}", std::io::Error::last_os_error()));
        }
        // SAFETY: two fresh descriptors pipe2 just opened, owned by nothing else.
        Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
    };
    let serial = GATED_SPAWN.lock().unwrap_or_else(|e| e.into_inner());
    let (gate_r, gate_w) = pipe()?;
    let (pid_r, pid_w) = pipe()?;
    let (gr, gw, pw) = (gate_r.as_raw_fd(), gate_w.as_raw_fd(), pid_w.as_raw_fd());
    // SAFETY: `hold_at_gate` is the whole closure and is async-signal-safe.
    unsafe { cmd.pre_exec(move || hold_at_gate(gr, gw, pw)) };
    std::thread::scope(|scope| {
        // `spawn` returns only once the child has executed (or failed to), which is after the release
        // below: it runs beside the executor's own steps. It gives up the executor's copies of the
        // read end of the gate and the write end of the pid pipe when it returns, so a child that
        // never announced a pid ends the executor's wait on that pipe.
        let spawner = scope.spawn(move || {
            let spawned = cmd.spawn();
            drop((gate_r, pid_w));
            spawned
        });
        let mut announced = [0u8; 4];
        let heard = std::fs::File::from(pid_r).read_exact(&mut announced).is_ok().then(|| u32::from_ne_bytes(announced));
        // The child closed its copy of the gate's write end before it announced: no other child can
        // hold it now, and the lock has done its work.
        drop(serial);
        let recorded = match heard {
            Some(pid) => Some(record(pid).map(|()| pid)),
            None => None,
        };
        match &recorded {
            // The release byte: the child's end of the gate may already be gone (it died), which the
            // spawner's own result reports.
            Some(Ok(_)) => {
                let _ = std::io::Write::write_all(&mut std::fs::File::from(gate_w), &[1u8]);
            }
            Some(Err(_)) | None => drop(gate_w),
        }
        let spawned = spawner.join().map_err(|_| "the spawning thread panicked".to_string())?;
        match (recorded, spawned) {
            (Some(Err(recording)), _) => Err(recording),
            (Some(Ok(pid)), Ok(child)) if child.id() == pid => Ok(child),
            (Some(Ok(pid)), Ok(child)) => Err(format!("the child announced pid {pid} and pid {} was spawned", child.id())),
            (Some(Ok(_)), Err(e)) | (None, Err(e)) => Err(e.to_string()),
            (None, Ok(child)) => Err(format!("pid {} was spawned and never announced its pid", child.id())),
        }
    })
}

/// A host without the gate's primitives launches no process: refused by name.
#[cfg(not(unix))]
fn spawn_gated(_: &mut std::process::Command, _: impl FnOnce(u32) -> Result<(), String>) -> Result<std::process::Child, String> {
    Err("the process provider holds a child at a pipe gate until its launch record is durable, which this host does not provide".into())
}

/// [`ProcessDeploymentProvider::spawn`] on the blocking pool: the launch's pid and start token.
fn spawn_on_disk(spec: &ResolvedNodeLaunch, children: &std::sync::Arc<Mutex<HashMap<u32, Launched>>>, exits: &std::sync::Arc<tokio::sync::watch::Sender<u64>>, failpoints: &Failpoints) -> Result<(u32, Option<u64>), DeployError> {
    let err = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason };
    std::fs::create_dir_all(&spec.data_dir).map_err(|e| err(format!("data dir {}: {e}", spec.data_dir.display())))?;
    let log = |name: &str| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(spec.data_dir.join(name))
            .map(Stdio::from)
            .map_err(|e| err(format!("{}: {e}", spec.data_dir.join(name).display())))
    };
    // A record left by an earlier birth in this data dir is not this one's:
    // the birth waits for the one the pipeline makes available.
    let stale = spec.data_dir.join(rdm_core::runtime::RUNTIME_FILE);
    match std::fs::remove_file(&stale) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(err(format!("{}: {e}", stale.display()))),
    }
    rdm_core::runtime::ExitRecord::clear(&spec.data_dir).map_err(err)?;
    let mut cmd = std::process::Command::new(&spec.executable);
    // The child inherits the launcher's host environment (PATH, HOME, ...) except every `RDM_*`
    // variable, the trace context (`TRACEPARENT`, `TRACESTATE`) and every declared name
    // (`spec.withhold`, Q150), which are removed; then the spec's env is applied, and the declared
    // values it sets for this kind (`spec.secrets`). Unlike a container, which inherits nothing, a
    // process keeps the host's other names.
    for (k, _) in std::env::vars_os() {
        if k.to_str().is_some_and(|k| k.starts_with("RDM_") || k == "TRACEPARENT" || k == "TRACESTATE" || spec.withhold.iter().any(|w| w == k)) {
            cmd.env_remove(&k);
        }
    }
    cmd.args(&spec.args).envs(&spec.env).stdin(Stdio::null()).stdout(log("stdout.log")?).stderr(log("stderr.log")?);
    for (k, v) in &spec.secrets {
        cmd.env(k, v.value(k).map_err(err)?);
    }
    // A session of its own (deployment-provider.md §2.5: the provider signals no runtime the operating
    // system holds suspended outside a terminate). A member in a process group of its launcher's session
    // is orphaned when that launcher dies, and the kernel sends SIGHUP then SIGCONT to an orphaned group
    // that holds a stopped member: a suspended member would be woken and ended by its launcher's death.
    // In its own session its group is never orphaned by that death, so the provider is the only one that
    // signals it.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid(2) is async-signal-safe and touches no memory of the parent.
        unsafe {
            cmd.pre_exec(|| if libc::setsid() == -1 { Err(std::io::Error::last_os_error()) } else { Ok(()) });
        }
    }
    // The record names the launch and its pid, so a re-run can find it: durable before the executable
    // runs.
    let mut start = None;
    let child = spawn_gated(&mut cmd, |pid| {
        failpoints.before_record(pid)?;
        start = process_start_token(pid);
        super::provider::write_launch_record(&spec.data_dir, DEPLOYMENT_FILE, &serde_json::json!({ "deployment_id": spec.deployment_id.as_str(), "pid": pid, "start": start }))
    })
    .map_err(|reason| err(format!("{}: {reason}", spec.executable.display())))?;
    let pid = child.id();
    failpoints.after_spawn(pid).map_err(err)?;
    children.lock().unwrap().insert(pid, Launched { deployment_id: spec.deployment_id.clone(), start, exit: None });
    let (children, exits) = (children.clone(), exits.clone());
    std::thread::Builder::new()
        .name(format!("reap-{pid}"))
        .spawn(move || {
            let mut child = child;
            let code = child.wait().ok().and_then(|s| s.code());
            if let Some(l) = children.lock().unwrap().get_mut(&pid) {
                l.exit = Some(code);
            }
            // The exit is an event at its source: the reconcile loop wakes on it (Q254).
            exits.send_modify(|v| *v += 1);
        })
        .map_err(|e| err(format!("watching pid {pid}: {e}")))?;
    Ok((pid, start))
}

/// What the deployment record in a launch's data dir says of it ([`ProcessDeploymentProvider::find`]).
#[derive(Debug, Clone, PartialEq, Eq)]
enum OnDisk {
    /// The record names this launch and its process runs: its pid and start token.
    Running(u32, Option<u64>),
    /// The record names this launch and its process is gone (or the pid names another process now):
    /// the pid and start token the record names.
    Exited(u32, Option<u64>),
    /// No record names this launch: none was written, or it names another launch.
    NeverLaunched,
}

/// [`ProcessDeploymentProvider::find`] on the blocking pool: the spawn writes the deployment record
/// (`deployment.json`) naming the launch and its pid, so a record naming this launch says it was
/// launched, and its pid says whether that process still runs. `Err` names a deployment record that
/// cannot be read or parsed; only a record never written, or one naming another launch, reads as
/// never launched.
fn find_on_disk(spec: &ResolvedNodeLaunch) -> Result<OnDisk, DeployError> {
    let path = spec.data_dir.join(DEPLOYMENT_FILE);
    let named = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason: format!("{}: {reason}", path.display()) };
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(OnDisk::NeverLaunched),
        Err(e) => return Err(named(e.to_string())),
    };
    let v: serde_json::Value = serde_json::from_str(&raw).map_err(|e| named(e.to_string()))?;
    if v.get("deployment_id").and_then(|d| d.as_str()) != Some(spec.deployment_id.as_str()) {
        return Ok(OnDisk::NeverLaunched);
    }
    let pid = v.get("pid").and_then(|p| p.as_u64()).and_then(|p| u32::try_from(p).ok()).ok_or_else(|| named("no pid".into()))?;
    let start = v.get("start").and_then(|s| s.as_u64());
    #[cfg(unix)]
    if !ProcessDeploymentProvider::runs(pid, &spec.executable) || start.is_some_and(|s| process_start_token(pid) != Some(s)) {
        return Ok(OnDisk::Exited(pid, start));
    }
    Ok(OnDisk::Running(pid, start.or_else(|| process_start_token(pid))))
}

#[async_trait::async_trait]
impl DeploymentProvider for ProcessDeploymentProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn exits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.exits.subscribe()
    }

    fn control_domain(&self) -> String {
        self.domain.clone()
    }

    /// The data dir, logs, records and the fork run on the blocking pool (`blocking::blocking_io`).
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        let (owned, children, exits, failpoints) = (spec.clone(), self.children.clone(), self.exits.clone(), self.failpoints.clone());
        let (pid, start) = crate::blocking::blocking_io(move || spawn_on_disk(&owned, &children, &exits, &failpoints)).await?;
        // The pipeline registers this exact handle and makes its runtime fact
        // available to the birth (`MakeRuntimeFactAvailableToBirth`).
        Ok(self.handle(spec.deployment_id.clone(), pid, start))
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        let Some(pid) = handle.pid else {
            return Err(DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason: "no pid".into() });
        };
        #[cfg(unix)]
        if !self.is_ours(pid, handle.start).await {
            // Exited, or the pid now names another process: nothing of this birth to stop.
            return Ok(());
        }
        #[cfg(unix)]
        {
            let grace = match mode {
                TerminationMode::Graceful { grace } => {
                    signal(pid, libc::SIGTERM);
                    grace
                }
                TerminationMode::Immediate => Duration::ZERO,
            };
            let start = handle.start;
            wait_for_exit(grace, || self.exited(pid, start), || { signal(pid, libc::SIGKILL); }).await;
        }
        Ok(())
    }

    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        let Some(pid) = handle.pid else {
            return Err(DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason: "no pid".into() });
        };
        match self.inspect(handle).await {
            DeploymentStatus::Running => {
                #[cfg(unix)]
                signal(pid, libc::SIGTERM);
                Ok(())
            }
            // The provider signals no runtime the operating system holds suspended outside a
            // terminate, as the container provider cannot (deployment-provider.md §2.5, §5): by name.
            DeploymentStatus::OsStopped => Err(DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason: format!("pid {pid} is held suspended by the operating system (os-stopped): the provider signals no suspended runtime outside a terminate; nothing was sent") }),
            DeploymentStatus::Exited { .. } | DeploymentStatus::Unknown => Ok(()),
        }
    }

    /// The deployment record and /proc are read on the blocking pool (`blocking::blocking_io`).
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        let owned = spec.clone();
        Ok(match crate::blocking::blocking_io(move || find_on_disk(&owned)).await? {
            OnDisk::Running(pid, start) => Found::Running(self.handle(spec.deployment_id.clone(), pid, start)),
            OnDisk::Exited(pid, start) => Found::Exited(Some(self.handle(spec.deployment_id.clone(), pid, start))),
            OnDisk::NeverLaunched => Found::NeverLaunched,
        })
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.children
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, l)| l.exit.is_none())
            .map(|(pid, l)| self.handle(l.deployment_id.clone(), *pid, l.start))
            .collect()
    }

    /// The state letters of the exact runtime's whole thread group (deployment-provider.md §2.6),
    /// for a child of this provider and an adopted runtime alike; a child's exit code is its wait's.
    /// /proc is read on the blocking pool.
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        let Some(pid) = handle.pid else { return DeploymentStatus::Unknown };
        let exit_of = || self.children.lock().unwrap().get(&pid).filter(|l| handle.start.is_none() || l.start == handle.start).map(|l| l.exit);
        if let Some(Some(code)) = exit_of() {
            return DeploymentStatus::Exited { code };
        }
        #[cfg(unix)]
        {
            let start = handle.start;
            match crate::blocking::blocking_io(move || exact_group(PROC, pid, start)).await {
                Group::Running => DeploymentStatus::Running,
                Group::OsStopped => DeploymentStatus::OsStopped,
                Group::Unreadable { path, cause } => {
                    tracing::info_span!("rdm.node_admin.deployment.returns-unknown.via-proc-unreadable", deployment_id = %handle.deployment_id, pid, path = %path, cause = %cause)
                        .in_scope(|| tracing::info!("the runtime's thread group cannot be read whole: its state is unknown, never exited verdict"));
                    DeploymentStatus::Unknown
                }
                // No live task, or a recycled pid that runs another process: this birth has exited.
                // A child's code is its wait's, recorded once the group is gone.
                Group::Gone => DeploymentStatus::Exited { code: exit_of().flatten().flatten() },
            }
        }
        #[cfg(not(unix))]
        DeploymentStatus::Unknown
    }
}

impl ProcessDeploymentProvider {
    /// `pid` is alive and runs `executable` (a recycled pid runs something else).
    #[cfg(unix)]
    fn runs(pid: u32, executable: &std::path::Path) -> bool {
        alive(pid)
            && std::fs::read_link(format!("/proc/{pid}/exe")).is_ok_and(|exe| {
                exe == executable || std::fs::canonicalize(executable).is_ok_and(|c| c == exe)
            })
    }

    /// Still running: a child of ours not yet seen to exit, or the live
    /// process `start` names (an adopted runtime).
    /// /proc is read on the blocking pool (`blocking::blocking_io`).
    async fn running(&self, pid: u32, start: Option<u64>) -> bool {
        let child = self.children.lock().unwrap().get(&pid).filter(|l| start.is_none() || l.start == start).map(|l| l.exit.is_none());
        match child {
            Some(running) => running,
            #[cfg(unix)]
            None => crate::blocking::blocking_io(move || is(pid, start)).await,
            #[cfg(not(unix))]
            None => false,
        }
    }

    /// The handle's process: our child, or the exact adopted process.
    #[cfg(unix)]
    async fn is_ours(&self, pid: u32, start: Option<u64>) -> bool {
        self.running(pid, start).await
    }

    /// The handle's process has exited: our child's wait returned, or the exact adopted process's
    /// group is gone. A group that cannot be read whole has not (inspect answers it `Unknown`).
    #[cfg(unix)]
    async fn exited(&self, pid: u32, start: Option<u64>) -> bool {
        let child = self.children.lock().unwrap().get(&pid).filter(|l| start.is_none() || l.start == start).map(|l| l.exit.is_some());
        match child {
            Some(exited) => exited,
            None => crate::blocking::blocking_io(move || gone_at(PROC, pid, start)).await,
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::deployment::provider::adopt;
    use crate::model::DeploymentId;

    /// A `stat` line of task `pid` in state `letter`, started at clock tick `start` (field 22).
    fn stat_line(pid: u32, command: &str, letter: char, start: u64) -> String {
        // Fields 4-21 are placeholders; field 22 is the start time.
        let middle = (4..=21).map(|_| "0").collect::<Vec<_>>().join(" ");
        format!("{pid} ({command}) {letter} {middle} {start} 0 0\n")
    }

    /// A fixture process filesystem under `root`: `pid` whose leader's stat names `start`, with one
    /// task per `letters` entry (`None` makes a task whose stat cannot be read: a directory there).
    fn fixture_proc(root: &std::path::Path, pid: u32, start: u64, letters: &[Option<char>]) {
        let dir = root.join(pid.to_string());
        std::fs::create_dir_all(dir.join("task")).unwrap();
        std::fs::write(dir.join("stat"), stat_line(pid, "node", letters.first().copied().flatten().unwrap_or('S'), start)).unwrap();
        for (i, letter) in letters.iter().enumerate() {
            let tid = pid + i as u32;
            let t = dir.join("task").join(tid.to_string());
            std::fs::create_dir_all(&t).unwrap();
            match letter {
                Some(l) => std::fs::write(t.join("stat"), stat_line(tid, "node worker (x)", *l, start)).unwrap(),
                None => std::fs::create_dir_all(t.join("stat")).unwrap(),
            }
        }
    }

    /// CONTRACT (deployment-provider.md §2.6): the state letter is the first field after the
    /// command's closing parenthesis, whatever the command holds (spaces, parentheses); a line with
    /// no command is not one.
    #[test]
    fn the_state_letter_is_read_after_the_commands_last_parenthesis() {
        assert_eq!(state_letter(&stat_line(7, "tail", 'R', 1)), Some('R'));
        assert_eq!(state_letter(&stat_line(7, "a) b (c", 'T', 1)), Some('T'));
        assert_eq!(state_letter(&stat_line(7, "rdm node", 't', 1)), Some('t'));
        assert_eq!(state_letter("7 (tail"), None, "a partial line names no state");
        assert_eq!(state_letter(""), None);
    }

    /// CONTRACT (deployment-provider.md §2.6): every live task of the thread group in `T` or `t` is
    /// `OsStopped`; any live task in another state (running, sleeping, in disk wait) is `Running`, so a
    /// group with one stopped task and one that runs is `Running`; zombie and dead tasks are not live,
    /// and a group with none live is `Gone`.
    #[test]
    fn a_thread_group_is_os_stopped_only_when_every_live_task_is_stopped() {
        let fold = |l: &[char]| fold_group(l.iter().copied());
        for running in ['R', 'S', 'D', 'I'] {
            assert_eq!(fold(&[running]), Group::Running, "{running}");
        }
        assert_eq!(fold(&['T']), Group::OsStopped);
        assert_eq!(fold(&['t']), Group::OsStopped);
        assert_eq!(fold(&['T', 't', 'T']), Group::OsStopped, "every task stopped, by signal or by tracer");
        assert_eq!(fold(&['Z', 'T']), Group::OsStopped, "a zombie leader with a stopped sibling: the live task is stopped");
        assert_eq!(fold(&['T', 'S']), Group::Running, "one live task that is not stopped: the group runs");
        assert_eq!(fold(&['t', 'R']), Group::Running);
        assert_eq!(fold(&['Z', 'X', 'x']), Group::Gone, "no live task");
        assert_eq!(fold(&[]), Group::Gone);
    }

    /// CONTRACT (deployment-provider.md §2.6): a task whose `stat` read fails because the task has
    /// exited, gone before the open (`NotFound`) or released between the open and the read (`ESRCH`),
    /// is left out of its group's fold; a read denied any other way (`EACCES`) is `Unreadable`,
    /// never exited verdict.
    #[test]
    fn a_task_released_before_or_during_its_stat_read_has_exited_and_a_denied_read_has_not() {
        assert!(task_exited(&std::io::Error::from(std::io::ErrorKind::NotFound)), "gone before the open");
        assert!(task_exited(&std::io::Error::from_raw_os_error(libc::ESRCH)), "released between the open and the read");
        assert!(!task_exited(&std::io::Error::from_raw_os_error(libc::EACCES)), "a denied read says nothing of the task: Unreadable");
    }

    /// CONTRACT (deployment-provider.md §2.6: "`/proc/<pid>` gone with the start identity matched, is
    /// `Exited`"; task files "present but unreadable or partial" are `Unknown`): the leader's `stat`
    /// gone before its open (`NotFound`) or released between the open and the read (`ESRCH`, the node
    /// exiting after its stop) is `Gone`; a read denied any other way (`EACCES`) is `Unreadable`.
    /// Must NOT happen: `ESRCH` read as `Unreadable`, so inspect answers `Unknown` after the stop
    /// ladder and the terminate is refused (soak seed 1432787 round 113, `node.delete:mesh1.admin.2`).
    #[test]
    fn a_leader_released_during_its_start_token_read_is_gone_and_a_denied_read_is_unreadable() {
        let path = "/proc/7/stat";
        let read = |e: std::io::Error| start_token_group(path, Err(e), 1);
        assert_eq!(read(std::io::Error::from(std::io::ErrorKind::NotFound)), Some(Group::Gone), "gone before the open");
        assert_eq!(read(std::io::Error::from_raw_os_error(libc::ESRCH)), Some(Group::Gone), "released between the open and the read");
        assert!(matches!(read(std::io::Error::from_raw_os_error(libc::EACCES)), Some(Group::Unreadable { .. })), "a denied read says nothing of the process");
        assert_eq!(start_token_group(path, Ok(stat_line(7, "node", 'S', 1)), 1), None, "the handle's own process: its thread group is read");
        assert_eq!(start_token_group(path, Ok(stat_line(7, "node", 'S', 2)), 1), Some(Group::Gone), "another process at the pid");
    }

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:45
    /// "For `process`: inspect `/proc/<pid>/stat` and `/proc/<pid>/environ` (verifying
    ///  `RDM_DEPLOYMENT_ID=<deployment_id>` so a recycled PID is never mistaken for the birth) and,
    ///  if the child belongs to this admin, `waitpid(pid, WNOHANG)`."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:52
    /// "4. The provider returns the typed answer: `Running` ... `OsStopped` ... `Exited` ... `Unavailable`"
    /// CONTRACT (deployment-provider.md §2.6: task files present but unreadable or partial is
    /// `Unknown`): the stop ladder waits on a runtime whose thread group cannot be read whole, and
    /// ends when the group is gone, inside its grace and with no kill. Must NOT happen: the wait read
    /// an unreadable group as exited and ended at once (the red at 56bf15655: TerminateRuntime failed
    /// "still runs after the stop ladder" when a sibling task was still live).
    #[tokio::test]
    async fn the_stop_ladder_waits_on_an_unreadable_group_until_it_is_gone() {
        let root = crate::test_support::tempdir("proc-fixture-ladder");
        let proc = root.to_str().unwrap().to_string();
        fixture_proc(&root, 700, 555, &[Some('Z'), None]);
        let gone = root.join("700");
        let exit = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            std::fs::remove_dir_all(gone).unwrap();
        });
        let killed = std::sync::atomic::AtomicBool::new(false);
        let began = std::time::Instant::now();
        let p = proc.clone();
        wait_for_exit(Duration::from_secs(5), || { let p = p.clone(); async move { gone_at(&p, 700, Some(555)) } }, || killed.store(true, std::sync::atomic::Ordering::SeqCst)).await;
        let waited = began.elapsed();
        exit.await.unwrap();
        assert!(waited >= Duration::from_millis(250), "the wait ended after {waited:?}, while the group was still unreadable");
        assert!(waited < Duration::from_secs(5), "the wait ended at the group's exit, inside its grace: {waited:?}");
        assert!(!killed.load(std::sync::atomic::Ordering::SeqCst), "a runtime that exits inside its grace is never killed");
    }

    /// CONTRACT (deployment-provider.md §2.6, PRD i144 R3): read over a process filesystem, a pid
    /// whose `/proc/<pid>` is gone, or whose start token names another process, is `Gone` (the exact
    /// runtime exited); task files present but unreadable or partial are `Unreadable`, naming the file
    /// and the error or the line, which inspect answers `Unknown`, never exited verdict, even beside a
    /// zombie task; a pid listed without its task directory cannot be told.
    #[test]
    fn a_partial_or_unreadable_task_read_is_unreadable_and_a_gone_or_recycled_pid_is_gone() {
        let root = crate::test_support::tempdir("proc-fixture");
        let proc = root.to_str().unwrap();
        fixture_proc(&root, 100, 555, &[Some('S'), Some('T')]);
        assert_eq!(exact_group(proc, 100, Some(555)), Group::Running);
        fixture_proc(&root, 200, 555, &[Some('T'), Some('t')]);
        assert_eq!(exact_group(proc, 200, Some(555)), Group::OsStopped);
        assert_eq!(exact_group(proc, 200, None), Group::OsStopped, "a handle with no start token reads the pid alone");
        assert_eq!(exact_group(proc, 200, Some(556)), Group::Gone, "a start token that names another process: this runtime exited");
        assert_eq!(exact_group(proc, 999, Some(555)), Group::Gone, "the pid is gone");
        assert_eq!(group(proc, 999), Group::Gone);
        let names = |g: Group, path: &str, cause: &str| match g {
            Group::Unreadable { path: p, cause: c } => assert!(p == format!("{proc}/{path}") && c.contains(cause), "{p}: {c}; expected {path}: {cause}"),
            other => panic!("{other:?}; expected Unreadable at {path}"),
        };
        fixture_proc(&root, 300, 555, &[Some('S'), None]);
        names(exact_group(proc, 300, Some(555)), "300/task/301/stat", "Is a directory");
        // A stop's wait reads an unreadable group as not exited; only a gone one has.
        assert!(!gone_at(proc, 300, Some(555)), "an unreadable group is not proven exited: the stop ladder waits on");
        assert!(gone_at(proc, 999, Some(555)) && gone_at(proc, 200, Some(556)), "a gone or recycled pid has exited");
        assert!(!gone_at(proc, 100, Some(555)), "a running group has not exited");
        fixture_proc(&root, 400, 555, &[Some('S')]);
        std::fs::write(root.join("400/task/400/stat"), "400 (node").unwrap();
        names(exact_group(proc, 400, Some(555)), "400/task/400/stat", "no state letter in \"400 (node\"");
        fixture_proc(&root, 600, 555, &[Some('Z'), Some('S')]);
        std::fs::write(root.join("600/task/601/stat"), "601 (node").unwrap();
        names(exact_group(proc, 600, Some(555)), "600/task/601/stat", "no state letter");
        std::fs::write(root.join("400/stat"), "400 (node").unwrap();
        names(exact_group(proc, 400, Some(555)), "400/stat", "no start token in \"400 (node\"");
        std::fs::create_dir_all(root.join("500")).unwrap();
        names(group(proc, 500), "500/task", "absent while");
    }

    fn launch(dir: &std::path::Path) -> ResolvedNodeLaunch {
        ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(),
            node: "mesh1.rpc.1".parse().unwrap(),
            deployment_id: DeploymentId::mint(),
            executable: "/bin/sleep".into(),
            args: vec!["30".into()],
            env: Default::default(),
            data_dir: dir.to_path_buf(),
            transport: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
        }
    }

    #[tokio::test]
    async fn a_successor_controls_the_exact_process_and_never_a_recycled_pid() {
        let dir = std::env::temp_dir().join(format!("rafka-process-adopt-{}", std::process::id()));
        let launcher = ProcessDeploymentProvider::new();
        let h = launcher.spawn(&launch(&dir)).await.unwrap();
        // The handle names exactly the runtime it started; making its fact
        // available to the birth is the pipeline's step, not the provider's.
        let fact = h.fact().expect("an exact handle");
        assert!(rdm_core::RuntimeFact::read_record(&dir).is_none());
        let successor = ProcessDeploymentProvider::new();
        // The same pid under another start token is another process: never ours to signal.
        let mut forged = fact.clone();
        let RuntimeLocator::Process { pid, start } = fact.locator.clone() else { panic!() };
        forged.locator = RuntimeLocator::Process { pid, start: start + 1 };
        let forged = adopt(&successor, &forged).unwrap();
        assert!(matches!(successor.inspect(&forged).await, DeploymentStatus::Exited { .. }), "a recycled pid is not the old birth");
        successor.terminate(&forged, TerminationMode::Immediate).await.unwrap();
        assert!(alive(pid), "nothing was signalled through a recycled pid");
        // The exact fact: the successor inspects and stops it.
        let adopted = adopt(&successor, &fact).unwrap();
        assert_eq!(successor.inspect(&adopted).await, DeploymentStatus::Running);
        successor.terminate(&adopted, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        for _ in 0..100 {
            if launcher.inspect(&h).await != DeploymentStatus::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(matches!(launcher.inspect(&h).await, DeploymentStatus::Exited { .. }));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: find reads the launch's deployment record on the blocking pool.
    #[test]
    fn find_reads_the_deployment_record_on_the_blocking_pool() {
        let dir = crate::test_support::tempdir("process-find-fifo");
        let spec = launch(&dir);
        let fifo = dir.join(DEPLOYMENT_FILE);
        let (heard, found) = crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, br#"{"deployment_id":"another","pid":1}"#.to_vec(), Box::pin(async move { ProcessDeploymentProvider::new().find(&spec).await }));
        assert!(heard, "the runtime stopped while the deployment record was read");
        assert_eq!(found.unwrap(), Found::NeverLaunched, "another launch's record");
    }

    /// CONTRACT (lifecycles/node-create.md: exact provider proof of a runtime's state, even for a
    /// deployment that never joined): the process provider's `find` tells the three facts of one exact
    /// launch apart: a launch it spawned whose process runs is `Running` with its exact handle; the
    /// same launch once its process exited is `Exited`; a launch never spawned in the data dir (no
    /// record, or the record of another launch) is `NeverLaunched`. Must NOT happen: an exited launch
    /// read as never launched, so a re-run starts it under the same incarnation.
    #[tokio::test]
    async fn find_tells_a_running_launch_an_exited_one_and_one_never_launched_apart() {
        let dir = crate::test_support::tempdir("process-find-three");
        let p = ProcessDeploymentProvider::new();
        let never = launch(&dir);
        assert_eq!(p.find(&never).await.unwrap(), Found::NeverLaunched, "no record in the data dir");
        let spec = launch(&dir);
        let h = p.spawn(&spec).await.unwrap();
        assert_eq!(p.find(&spec).await.unwrap(), Found::Running(h.clone()), "its process runs");
        assert_eq!(p.find(&never).await.unwrap(), Found::NeverLaunched, "the record names another launch");
        p.terminate(&h, TerminationMode::Immediate).await.unwrap();
        assert!(matches!(p.find(&spec).await.unwrap(), Found::Exited(Some(_))), "its process is gone");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (lifecycles/node-create.md step 1: "Record the exact deployment, prepare its
    /// identity/data dir and launch its executable"): a live process always has its record. The
    /// executor that launches a process is lost the instant the child exists, before anything else of
    /// the spawn is done; a successor of the same control domain asks `find` for that exact launch and
    /// is told the process runs, with its exact pid. Must NOT happen: `NeverLaunched` for a running
    /// child, which makes the claimed-allocation decision continue the same birth over a live process
    /// it never recorded.
    #[tokio::test]
    async fn a_process_whose_launching_executor_is_lost_the_instant_it_exists_is_found_running() {
        let dir = crate::test_support::tempdir("process-lost-after-spawn");
        let spec = launch(&dir);
        let slot = std::sync::Arc::new(Mutex::new(None));
        ProcessDeploymentProvider::lost_after_spawn(slot.clone()).spawn(&spec).await.expect_err("the executor is lost inside the spawn");
        let pid = slot.lock().unwrap().expect("the child existed when the executor was lost");
        let found = ProcessDeploymentProvider::new().find(&spec).await;
        // The orphan list stays empty: the child is ours, killed by its exact pid and reaped.
        assert!(signal(pid, libc::SIGKILL), "the child {pid} was alive to kill");
        // SAFETY: waitpid(2) on the child this test spawned; the status is not read.
        assert_eq!(unsafe { libc::waitpid(pid as i32, std::ptr::null_mut(), 0) }, pid as i32);
        let _ = std::fs::remove_dir_all(&dir);
        match found.expect("find answers") {
            Found::Running(h) => assert_eq!(h.pid, Some(pid), "the runtime found is the child that was spawned"),
            other => panic!("the child {pid} runs, and find answered {other:?}"),
        }
    }

    /// CONTRACT (lifecycles/node-create.md step 1: record the exact deployment, then launch its
    /// executable): the executable of a launch does not run before the launch's record is durable.
    /// The executor is lost while the forked child waits at the gate, before the record is written:
    /// the child ends without running its executable, no record names the launch, and `find` says it
    /// was never launched. Must NOT happen: an executable that ran with no record of it.
    #[tokio::test]
    async fn a_process_whose_launching_executor_is_lost_before_its_record_never_runs_its_executable() {
        let dir = crate::test_support::tempdir("process-lost-before-record");
        let marker = dir.join("ran");
        let spec = ResolvedNodeLaunch { executable: "/bin/sh".into(), args: vec!["-c".into(), "touch \"$0\"; sleep 30".into(), marker.display().to_string()], ..launch(&dir) };
        let slot = std::sync::Arc::new(Mutex::new(None));
        let refused = ProcessDeploymentProvider::lost_before_record(slot.clone()).spawn(&spec).await.expect_err("the executor is lost at the gate");
        let pid = slot.lock().unwrap().expect("the child waited at the gate when the executor was lost");
        assert!(refused.to_string().contains("failpoint"), "{refused}");
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists(), "the child at the gate ended and was reaped");
        assert!(!marker.exists(), "its executable never ran");
        assert_eq!(ProcessDeploymentProvider::new().find(&spec).await.unwrap(), Found::NeverLaunched, "no record names the launch");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: a launch record that cannot be written ends the launch by name before its
    /// executable runs; the child waiting at the gate is cancelled, never released.
    #[tokio::test]
    async fn a_launch_record_that_cannot_be_written_stops_the_launch_before_its_executable_runs() {
        let dir = crate::test_support::tempdir("process-record-unwritable");
        let marker = dir.join("ran");
        let spec = ResolvedNodeLaunch { executable: "/bin/sh".into(), args: vec!["-c".into(), "touch \"$0\"; sleep 30".into(), marker.display().to_string()], ..launch(&dir) };
        // The record's temporary file is a directory: it cannot be created.
        std::fs::create_dir_all(dir.join(format!("{DEPLOYMENT_FILE}.tmp"))).unwrap();
        let refused = ProcessDeploymentProvider::new().spawn(&spec).await.expect_err("no record, no launch");
        assert!(refused.to_string().contains(DEPLOYMENT_FILE), "the refusal names the record: {refused}");
        assert!(!marker.exists(), "its executable never ran");
        assert_eq!(ProcessDeploymentProvider::new().find(&spec).await.unwrap(), Found::NeverLaunched);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: a record with no live process reads as exited. A launch whose executable cannot run
    /// fails by name, after its record is durable: the record names a pid that is gone, so `find`
    /// answers `Exited`, never `Running` and never `NeverLaunched`.
    #[tokio::test]
    async fn a_launch_whose_executable_cannot_run_leaves_a_record_that_reads_exited() {
        let dir = crate::test_support::tempdir("process-exec-fails");
        let spec = ResolvedNodeLaunch { executable: dir.join("no-such-executable"), ..launch(&dir) };
        let refused = ProcessDeploymentProvider::new().spawn(&spec).await.expect_err("the executable does not exist");
        assert!(refused.to_string().contains("no-such-executable"), "{refused}");
        assert!(matches!(ProcessDeploymentProvider::new().find(&spec).await.unwrap(), Found::Exited(Some(_))), "its record names a process that is gone");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: a deployment record that cannot be read or parsed is an error naming it, never
    /// "this launch started nothing" (which would start a second runtime).
    #[tokio::test]
    async fn an_unreadable_deployment_record_is_an_error_naming_it() {
        let dir = crate::test_support::tempdir("process-find-unreadable");
        std::fs::create_dir_all(dir.join(DEPLOYMENT_FILE)).unwrap();
        let err = ProcessDeploymentProvider::new().find(&launch(&dir)).await.expect_err("an unreadable record is not an absent one");
        assert!(err.to_string().contains(DEPLOYMENT_FILE), "{err}");
        std::fs::remove_dir(dir.join(DEPLOYMENT_FILE)).unwrap();
        std::fs::write(dir.join(DEPLOYMENT_FILE), b"not json").unwrap();
        let err = ProcessDeploymentProvider::new().find(&launch(&dir)).await.expect_err("an unparsable record is not an absent one");
        assert!(err.to_string().contains(DEPLOYMENT_FILE), "{err}");
    }

    /// CONTRACT: inspecting an adopted process reads /proc on the blocking pool. An `Unknown` names
    /// the read that failed (`rdm.node_admin.deployment.returns-unknown.via-proc-unreadable`).
    #[test]
    fn inspecting_an_adopted_process_waits_on_the_blocking_pool() {
        let me = std::process::id();
        let capture = crate::span_capture::Capture::new("process-inspect-blocking-pool");
        let _cell = tracing::dispatcher::set_default(&capture.dispatch);
        let (waited, status) = crate::test_support::op_waits_on_the_blocking_pool(Box::pin(async {}), move |()| {
            Box::pin(async move {
                let p = ProcessDeploymentProvider::new();
                let h = p.handle(DeploymentId::mint(), me, process_start_token(me));
                p.inspect(&h).await
            })
        });
        assert!(waited, "the inspection finished on the runtime's thread while the blocking pool was held");
        let unread: Vec<_> = capture.named("rdm.node_admin.deployment.returns-unknown.via-proc-unreadable").into_iter().map(|s| s.attributes).collect();
        assert_eq!(status, DeploymentStatus::Running, "the read that failed: {unread:?}");
    }

    /// CONTRACT: a spawn (its data dir, logs, records and the fork) runs on the blocking pool.
    #[test]
    fn a_spawn_waits_on_the_blocking_pool() {
        let dir = crate::test_support::tempdir("process-spawn-pool");
        let spec = launch(&dir);
        let p = std::sync::Arc::new(ProcessDeploymentProvider::new());
        let spawner = p.clone();
        let (waited, h) = crate::test_support::op_waits_on_the_blocking_pool(Box::pin(async {}), move |()| Box::pin(async move { spawner.spawn(&spec).await }));
        let h = h.unwrap();
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(p.terminate(&h, TerminationMode::Immediate)).unwrap();
        assert!(waited, "the spawn finished on the runtime's thread while the blocking pool was held");
    }

    /// CONTRACT (Q254: a provider exit is an event the reconcile loop wakes on): the exit of a runtime this
    /// provider started, killed from outside, moves its exits watch once its watcher reaps it, and nothing
    /// moves it while the runtime runs.
    /// Must NOT happen: an exit the reconcile loop never hears of, read only on a tick.
    #[tokio::test]
    async fn the_exit_of_a_runtime_this_provider_started_moves_its_exits_watch() {
        let dir = crate::test_support::tempdir("process-exits-watch");
        let p = ProcessDeploymentProvider::new();
        let mut exits = p.exits();
        let h = p.spawn(&launch(&dir)).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(300), exits.changed()).await.is_err(), "nothing moves while the runtime runs");
        assert!(signal(h.pid.expect("a pid"), libc::SIGKILL), "the runtime was alive to kill");
        tokio::time::timeout(Duration::from_secs(5), exits.changed()).await.expect("the exit never moved the watch").expect("the provider lives");
        assert!(matches!(p.inspect(&h).await, DeploymentStatus::Exited { .. }), "the exit is the provider's");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
