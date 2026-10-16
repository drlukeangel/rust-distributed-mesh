//! i144 e1 (deployment-provider.md §2.5-§2.7): the deployment provider reports the exact runtime's
//! state with one meaning for a process and a container: `Running`, `OsStopped`, `Exited { code }`,
//! `Unknown`. Each behavior is one body, run by `mod process` and `mod container`; the only difference
//! is how the host suspends, continues and kills the runtime (SIGSTOP, SIGCONT, SIGKILL; `docker
//! pause`, `docker unpause`, `docker kill`). Every body checks the host's own view (the oracle: the
//! task states in `/proc`, the daemon's state) before it asks the provider.
//!
//! The container module needs a Docker daemon: it runs with `RDM_CONTAINER_PROOF=1` (the
//! container-proof step) and skips by name otherwise; `RDM_REQUIRE_CONTAINER=1` fails a host that
//! cannot run containers instead of skipping.

use rdm_admin::deployment::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, TerminationMode};
use std::time::{Duration, Instant};

/// The host's own hand on one runtime: what it suspends, continues and kills with, and what it shows.
#[async_trait::async_trait]
trait Hand: Send + Sync {
    fn provider(&self) -> &dyn DeploymentProvider;
    fn handle(&self) -> &DeploymentHandle;
    /// The host's own view of the runtime, read without the provider: `running`, `os-stopped`,
    /// `idle` (alive, nothing executing: sleeping tasks or a created container) or `exited`.
    async fn host_view(&self) -> &'static str;
    async fn suspend(&self);
    async fn resume(&self);
    async fn kill(&self);
    /// The exit code the host gives a runtime killed by SIGKILL: none for a process (the wait status
    /// is a signal), Docker's 137 for a container.
    fn killed_code(&self) -> Option<i32>;
}

/// Wait until the host shows the runtime as `want` (the fault took effect), at most 5 s.
async fn until_host(hand: &dyn Hand, want: &[&str], what: &str) {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let seen = hand.host_view().await;
        if want.contains(&seen) {
            return;
        }
        assert!(Instant::now() < until, "the host never showed {what}: it shows {seen}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// CONTRACT (deployment-provider.md §2.5, §2.6, §2.7): a runtime that exists and is not suspended
/// reads `Running` while nothing in it executes (a sleeping task, a runtime still idle); held by the
/// operating system it reads `OsStopped`, never `Running` and never exit proof; continued, it reads
/// `Running` again.
async fn a_suspended_runtime_reads_os_stopped_and_running_again_when_continued(hand: &dyn Hand) {
    until_host(hand, &["idle", "running"], "the runtime alive").await;
    assert_eq!(hand.provider().inspect(hand.handle()).await, DeploymentStatus::Running, "an alive runtime that is not suspended");
    hand.suspend().await;
    until_host(hand, &["os-stopped"], "the runtime suspended").await;
    assert_eq!(hand.provider().inspect(hand.handle()).await, DeploymentStatus::OsStopped, "the operating system holds it suspended");
    hand.resume().await;
    until_host(hand, &["idle", "running"], "the runtime continued").await;
    assert_eq!(hand.provider().inspect(hand.handle()).await, DeploymentStatus::Running, "continued, it runs again");
}

/// CONTRACT (deployment-provider.md §2.5, §2.8): a suspended runtime that the host then kills reads
/// `Exited` with the code the host gives it: `OsStopped` is never in the way of exit proof, and the
/// provider sent nothing to continue it.
async fn a_stopped_runtime_that_then_exits_reads_exited_with_its_code(hand: &dyn Hand) {
    hand.suspend().await;
    until_host(hand, &["os-stopped"], "the runtime suspended").await;
    assert_eq!(hand.provider().inspect(hand.handle()).await, DeploymentStatus::OsStopped);
    hand.kill().await;
    until_host(hand, &["exited"], "the runtime gone").await;
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let status = hand.provider().inspect(hand.handle()).await;
        if status == (DeploymentStatus::Exited { code: hand.killed_code() }) {
            return;
        }
        // The host has reaped it; the provider's own record of the exit (a child's wait) follows it.
        assert!(Instant::now() < until && matches!(status, DeploymentStatus::Exited { .. }), "a killed suspended runtime reads Exited {{ code: {:?} }}: {status:?}", hand.killed_code());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// CONTRACT (deployment-provider.md §2.5: an explicit `node.delete` of an OS-stopped birth "runs
/// `TerminateRuntime` on the exact handle, takes the exit proof and continues"; §3: "skips the stop by
/// name and begins at `TerminateRuntime`"): a runtime the operating system holds suspended is ended by
/// the provider's own terminate of its exact handle, and then reads `Exited`: the suspension is never
/// in the way of the exit proof. Must NOT happen: a terminate that returns while the runtime still
/// exists, or an `OsStopped` or `Running` read after it.
async fn a_suspended_runtime_is_ended_by_its_terminate_and_reads_exited(hand: &dyn Hand) {
    hand.suspend().await;
    until_host(hand, &["os-stopped"], "the runtime suspended").await;
    assert_eq!(hand.provider().inspect(hand.handle()).await, DeploymentStatus::OsStopped);
    hand.provider().terminate(hand.handle(), TerminationMode::Graceful { grace: Duration::from_secs(1) }).await.expect("the terminate of the exact handle");
    until_host(hand, &["exited"], "the terminated runtime gone").await;
    let status = hand.provider().inspect(hand.handle()).await;
    assert!(matches!(status, DeploymentStatus::Exited { .. }), "a terminated suspended runtime reads Exited: {status:?}");
}

/// CONTRACT (deployment-provider.md §2.5 R19: `Running` is nonterminal and not OS-suspended, nothing
/// more): a runtime in which nothing executes (every task asleep; a container created and not
/// started) reads `Running`, never `OsStopped` and never `Exited`.
async fn an_idle_runtime_reads_running(hand: &dyn Hand) {
    until_host(hand, &["idle"], "the runtime idle").await;
    assert_eq!(hand.provider().inspect(hand.handle()).await, DeploymentStatus::Running);
}

mod process {
    use super::*;
    use rdm_admin::deployment::process::ProcessDeploymentProvider;
    use rdm_admin::deployment::provider::ResolvedNodeLaunch;
    use rdm_admin::model::DeploymentId;

    /// The state letters of every task of `pid`, read straight from `/proc`: the oracle.
    fn letters(pid: u32) -> Option<Vec<char>> {
        let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).ok()?;
        Some(tasks.flatten().filter_map(|t| std::fs::read_to_string(t.path().join("stat")).ok()).filter_map(|s| s.rsplit_once(')').and_then(|(_, r)| r.trim_start().chars().next())).collect())
    }

    struct Child {
        provider: ProcessDeploymentProvider,
        handle: DeploymentHandle,
        dir: std::path::PathBuf,
    }

    impl Child {
        async fn spawn(cell: &str, executable: &str, args: &[&str]) -> Self {
            let dir = std::env::temp_dir().join(format!("provider-state-{cell}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let provider = ProcessDeploymentProvider::new();
            let spec = ResolvedNodeLaunch {
                node: "mesh1.rpc.1".parse().unwrap(),
                deployment_id: DeploymentId::mint(),
                executable: executable.into(),
                args: args.iter().map(|a| a.to_string()).collect(),
                env: Default::default(),
                data_dir: dir.clone(),
                transport: "127.0.0.1:0".parse().unwrap(),
                listeners: vec![],
            };
            let handle = provider.spawn(&spec).await.expect("the launch");
            Self { provider, handle, dir }
        }
        fn pid(&self) -> u32 {
            self.handle.pid.expect("a process handle names its pid")
        }
        fn signal(&self, sig: i32) {
            // SAFETY: kill(2) on this cell's own child.
            assert_eq!(unsafe { libc::kill(self.pid() as i32, sig) }, 0, "kill {sig} {}", self.pid());
        }
    }

    impl Drop for Child {
        fn drop(&mut self) {
            // SAFETY: kill(2) on this cell's own child; it may already be gone.
            unsafe {
                libc::kill(self.pid() as i32, libc::SIGKILL);
            }
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[async_trait::async_trait]
    impl Hand for Child {
        fn provider(&self) -> &dyn DeploymentProvider {
            &self.provider
        }
        fn handle(&self) -> &DeploymentHandle {
            &self.handle
        }
        async fn host_view(&self) -> &'static str {
            let live: Vec<char> = letters(self.pid()).unwrap_or_default().into_iter().filter(|l| !matches!(l, 'Z' | 'X' | 'x')).collect();
            match live.as_slice() {
                [] => "exited",
                l if l.iter().all(|c| matches!(c, 'T' | 't')) => "os-stopped",
                l if l.iter().all(|c| matches!(c, 'S' | 'I')) => "idle",
                _ => "running",
            }
        }
        async fn suspend(&self) {
            self.signal(libc::SIGSTOP);
        }
        async fn resume(&self) {
            self.signal(libc::SIGCONT);
        }
        async fn kill(&self) {
            self.signal(libc::SIGKILL);
        }
        fn killed_code(&self) -> Option<i32> {
            None
        }
    }

    async fn sleeper(cell: &str) -> Child {
        Child::spawn(cell, "/usr/bin/tail", &["-f", "/dev/null"]).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_suspended_runtime_reads_os_stopped_and_running_again_when_continued() {
        super::a_suspended_runtime_reads_os_stopped_and_running_again_when_continued(&sleeper("suspend").await).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_runtime_that_then_exits_reads_exited_with_its_code() {
        super::a_stopped_runtime_that_then_exits_reads_exited_with_its_code(&sleeper("exit").await).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_suspended_runtime_is_ended_by_its_terminate_and_reads_exited() {
        super::a_suspended_runtime_is_ended_by_its_terminate_and_reads_exited(&sleeper("terminate").await).await;
    }

    /// The process side of R19: a runtime whose every task sleeps.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sleeping_task_reads_running() {
        super::an_idle_runtime_reads_running(&sleeper("idle").await).await;
    }

    /// CONTRACT (deployment-provider.md §2.6: "any live task in another state is `Running`"): a thread
    /// group with one task stopped (a tracer's stop, `t`) and another live task asleep reads `Running`,
    /// never `OsStopped`: the group is suspended only when every live task is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_thread_group_with_one_running_task_reads_running_while_another_is_stopped() {
        let child = Child::spawn("thread", "/usr/bin/python3", &["-c", "import threading, time\nt = threading.Thread(target=lambda: time.sleep(300))\nt.start()\ntime.sleep(300)"]).await;
        let pid = child.pid();
        // The worker thread: the task that is not the leader.
        let until = Instant::now() + Duration::from_secs(10);
        let worker = loop {
            let tids: Vec<u32> = std::fs::read_dir(format!("/proc/{pid}/task")).unwrap().flatten().filter_map(|t| t.file_name().to_str()?.parse().ok()).filter(|t| *t != pid).collect();
            if let Some(t) = tids.first() {
                break *t;
            }
            assert!(Instant::now() < until, "the child's worker thread never started");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        // A tracer stops the worker alone (PTRACE_SEIZE + PTRACE_INTERRUPT puts only that task in
        // `t`); every ptrace request comes from the one thread that attached.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let (stopped_tx, stopped_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        let tracer = std::thread::spawn(move || {
            // SAFETY: ptrace(2) and waitpid(2) on this cell's own child's thread.
            let none = std::ptr::null_mut::<libc::c_void>();
            unsafe {
                if libc::ptrace(libc::PTRACE_SEIZE, worker as libc::pid_t, none, none) != 0 {
                    let _ = stopped_tx.send(Err(format!("PTRACE_SEIZE {worker}: {}", std::io::Error::last_os_error())));
                    return;
                }
                libc::ptrace(libc::PTRACE_INTERRUPT, worker as libc::pid_t, none, none);
                let mut status = 0;
                libc::waitpid(worker as libc::pid_t, &mut status, libc::__WALL);
                let _ = stopped_tx.send(Ok(()));
                let _ = rx.recv();
                libc::ptrace(libc::PTRACE_DETACH, worker as libc::pid_t, none, none);
            }
        });
        stopped_rx.recv().unwrap().unwrap_or_else(|e| panic!("the tracer could not stop the worker: {e}"));
        let seen = letters(pid).unwrap();
        assert!(seen.contains(&'t') && seen.iter().any(|l| !matches!(l, 't' | 'T' | 'Z' | 'X' | 'x')), "the host shows one task stopped and one live task that is not: {seen:?}");
        let status = child.provider().inspect(child.handle()).await;
        tx.send(()).unwrap();
        tracer.join().unwrap();
        assert_eq!(status, DeploymentStatus::Running, "one live task that is not stopped: the group runs ({seen:?})");
    }

    /// CONTRACT (deployment-provider.md §2.6: "`/proc/<pid>` gone with the start identity matched, is
    /// `Exited`"): a successor that adopted the runtime from its fact (it never launched it, so holds no
    /// wait of it) reads `Exited` once the runtime is gone, with no code (no exit record proves one).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pid_gone_with_its_start_identity_matched_is_exited() {
        let child = sleeper("gone").await;
        let successor = ProcessDeploymentProvider::new();
        let fact = child.handle().fact().expect("an exact handle has a fact");
        let adopted = rdm_admin::deployment::provider::adopt(&successor, &fact).expect("the same control domain adopts it");
        assert_eq!(successor.inspect(&adopted).await, DeploymentStatus::Running);
        child.kill().await;
        let until = Instant::now() + Duration::from_secs(5);
        while std::path::Path::new(&format!("/proc/{}", child.pid())).exists() {
            assert!(Instant::now() < until, "the killed child was never reaped");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(successor.inspect(&adopted).await, DeploymentStatus::Exited { code: None });
    }
}

mod container {
    use super::*;
    use crate::container_proof::{fabric_name, launch, provider, Fabric};
    use rdm_admin::deployment::container::ContainerDeploymentProvider;

    fn docker(args: &[&str]) -> String {
        let o = std::process::Command::new("docker").args(args).output().unwrap();
        assert!(o.status.success(), "docker {args:?}: {}", String::from_utf8_lossy(&o.stderr));
        String::from_utf8_lossy(&o.stdout).trim().to_string()
    }

    struct Container {
        provider: ContainerDeploymentProvider,
        handle: DeploymentHandle,
        _fabric: Fabric,
    }

    impl Container {
        fn id(&self) -> String {
            self.handle.container.clone().expect("a container handle names its container")
        }
    }

    #[async_trait::async_trait]
    impl Hand for Container {
        fn provider(&self) -> &dyn DeploymentProvider {
            &self.provider
        }
        fn handle(&self) -> &DeploymentHandle {
            &self.handle
        }
        async fn host_view(&self) -> &'static str {
            let o = std::process::Command::new("docker").args(["inspect", "--format", "{{.State.Status}}", &self.id()]).output().unwrap();
            match String::from_utf8_lossy(&o.stdout).trim() {
                "paused" => "os-stopped",
                "created" => "idle",
                "running" => "running",
                "exited" | "dead" => "exited",
                _ if !o.status.success() => "exited",
                _ => "unknown",
            }
        }
        async fn suspend(&self) {
            docker(&["pause", &self.id()]);
        }
        async fn resume(&self) {
            docker(&["unpause", &self.id()]);
        }
        async fn kill(&self) {
            docker(&["kill", "--signal", "KILL", &self.id()]);
        }
        fn killed_code(&self) -> Option<i32> {
            Some(137)
        }
    }

    /// One running container of the host's `tail -f /dev/null` on its own fabric network, or `None`
    /// (named) when the cell is not asked for or the host cannot run containers.
    async fn sleeper(cell: &str) -> Option<Container> {
        let fabric = fabric_name(cell);
        let provider = provider(&fabric).await?;
        let cleanup = Fabric(fabric.clone());
        let handle = provider.spawn(&launch(&provider, &fabric, 0)).await.expect("the launch");
        Some(Container { provider, handle, _fabric: cleanup })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_suspended_runtime_reads_os_stopped_and_running_again_when_continued() {
        let Some(c) = sleeper("state-suspend").await else { return };
        super::a_suspended_runtime_reads_os_stopped_and_running_again_when_continued(&c).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_runtime_that_then_exits_reads_exited_with_its_code() {
        let Some(c) = sleeper("state-exit").await else { return };
        super::a_stopped_runtime_that_then_exits_reads_exited_with_its_code(&c).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_suspended_runtime_is_ended_by_its_terminate_and_reads_exited() {
        let Some(c) = sleeper("state-terminate").await else { return };
        super::a_suspended_runtime_is_ended_by_its_terminate_and_reads_exited(&c).await;
    }

    /// The container side of R19: a container created and not started.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_created_container_reads_running() {
        let fabric = fabric_name("state-created");
        let Some(provider) = provider(&fabric).await else { return };
        let cleanup = Fabric(fabric.clone());
        let id = docker(&["create", "--label", &format!("rafka.fabric={fabric}"), rdm_admin::deployment::container::RUNTIME_IMAGE, "/usr/bin/tail", "-f", "/dev/null"]);
        let handle = DeploymentHandle { deployment_id: rdm_admin::model::DeploymentId::mint(), provider: rdm_admin::model::ProviderKind::Container, pid: None, start: None, container: Some(id), domain: Some(provider.control_domain()) };
        let c = Container { provider, handle, _fabric: cleanup };
        super::an_idle_runtime_reads_running(&c).await;
    }
}
