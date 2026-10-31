//! A provider's client command (the container provider's `docker`), run under the one absolute
//! deadline its step hands it (Q304, rdm#241 §3, §6).
//!
//! The client is a child this module owns from spawn to reap: its stdout and stderr are read at
//! once, and its exit is awaited until the deadline. Past the deadline the client is killed, its
//! reap awaited at most [`CLEANUP_BOUND`] and owned after that, and the failure is
//! [`Failure::Deadline`]: the client wait ended, which proves nothing of the daemon's side. A caller
//! that drops the call ends the client the same way (`kill_on_drop`), and its span says it was
//! cancelled. Nothing here retries: the owning workflow's next attempt is the retry.
//!
//! One span per invocation, `rdm.node_admin.deployment.update.via-provider-command`: operation,
//! deployment, exact container when known, control domain, deadline, elapsed, outcome, exit code,
//! the client's kill and cleanup, and the tail of stderr with the call's secrets scrubbed and its
//! truncation marked. Never the arguments or the environment.

use super::deadline::{Deadline, CLEANUP_BOUND};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;

/// How much of a client's stderr is kept for its failure and its span: the newest bytes.
pub const STDERR_TAIL: usize = 8 * 1024;

/// One invocation of a provider's client.
pub struct Invocation<'a> {
    /// What it does, as spans and failures name it (`container.run`, `network.inspect`, …).
    pub operation: &'static str,
    /// Its arguments; never recorded.
    pub args: &'a [&'a str],
    /// The client's own environment (a secret a `-e K` names rides here); never recorded, and every
    /// value is scrubbed from what is.
    pub env: &'a BTreeMap<String, String>,
    /// The deployment it acts on, when it names one.
    pub deployment: Option<&'a str>,
    /// The exact container it acts on, when known.
    pub container: Option<&'a str>,
    /// Whether it changes a runtime: a mutation past its deadline has an unknown outcome.
    pub mutation: bool,
    /// Keep stderr whole: it is the answer (`docker logs` writes a container's stderr there).
    pub stderr_is_data: bool,
}

/// What a client that exited zero wrote.
#[derive(Debug)]
pub struct Answer {
    /// Its stdout, whole.
    pub stdout: Vec<u8>,
    /// Its stderr: whole for an invocation whose stderr is data, else the newest [`STDERR_TAIL`] bytes.
    pub stderr: Vec<u8>,
}

/// Why an invocation has no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// The client exited non-zero: the daemon answered.
    Answered {
        /// The client's exit code.
        exit_code: Option<i32>,
        /// What it wrote to stderr: scrubbed, the tail.
        stderr: String,
    },
    /// The deadline passed first: the client was killed. A mutation's outcome is unknown.
    Deadline {
        /// How long the client ran.
        elapsed: Duration,
        /// The bound its deadline was taken from.
        bound: Duration,
        /// Whether it changes a runtime.
        mutation: bool,
    },
    /// The client could not be started or read (named).
    Client(String),
}

impl Failure {
    /// The failure as the provider's error strings say it: `<program> <verb>: <what>`. A deadline's
    /// text never reads as an answer (it names no daemon reply, absent object included).
    pub fn message(&self, program: &str, verb: &str) -> String {
        match self {
            Failure::Answered { stderr, .. } => format!("{program} {verb}: {stderr}"),
            Failure::Deadline { elapsed, bound, mutation } => format!(
                "{program} {verb}: answered nothing within its deadline ({} ms bound, ended after {} ms); the client was killed{}",
                bound.as_millis(),
                elapsed.as_millis(),
                if *mutation { " and the external outcome is unknown" } else { "" }
            ),
            Failure::Client(reason) => format!("{program} {verb}: {reason}"),
        }
    }
}

/// The span's record of one invocation, written however it ends, a cancellation included.
struct Record {
    span: tracing::Span,
    started: Instant,
    ended: bool,
}

impl Record {
    fn end(&mut self, outcome: &str) {
        self.span.record("elapsed_ms", self.started.elapsed().as_millis() as u64).record("outcome", outcome);
        self.ended = true;
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        if !self.ended {
            // Dropped by its caller before it ended: the client is killed with it (`kill_on_drop`).
            self.span.record("client_kill", "on-drop").record("cleanup", "runtime-reaped");
            self.end("cancelled");
        }
    }
}

/// `program` run as `call` says, until `deadline`.
pub async fn run(program: &str, domain: Option<&str>, call: &Invocation<'_>, deadline: Deadline) -> Result<Answer, Failure> {
    let span = tracing::info_span!(
        "rdm.node_admin.deployment.update.via-provider-command",
        operation = call.operation,
        deployment_id = call.deployment.unwrap_or(""),
        container = call.container.unwrap_or(""),
        domain = domain.unwrap_or(""),
        deadline_ms = deadline.bound().as_millis() as u64,
        remaining_ms = deadline.remaining().as_millis() as u64,
        mutation = call.mutation,
        elapsed_ms = tracing::field::Empty,
        outcome = tracing::field::Empty,
        exit_code = tracing::field::Empty,
        client_kill = tracing::field::Empty,
        cleanup = tracing::field::Empty,
        external_outcome = tracing::field::Empty,
        stderr = tracing::field::Empty,
        stderr_truncated = tracing::field::Empty,
    );
    let mut record = Record { span: span.clone(), started: Instant::now(), ended: false };
    let secrets: Vec<(String, String)> = call.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let scrub = |bytes: &[u8]| super::declared::scrub(String::from_utf8_lossy(bytes).trim(), &secrets);
    let mut child = match tokio::process::Command::new(program)
        .args(call.args)
        .envs(call.env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            record.end("client-error");
            return Err(Failure::Client(e.to_string()));
        }
    };
    let (mut out, mut err) = (child.stdout.take().expect("piped"), child.stderr.take().expect("piped"));
    let keep_all = call.stderr_is_data;
    let waited = deadline
        .run(async {
            let read_out = async {
                let mut v = Vec::new();
                out.read_to_end(&mut v).await.map(|_| v)
            };
            // Consumed as it comes, so a flood never blocks the client; only the newest bytes kept.
            let read_err = async {
                let (mut kept, mut truncated, mut buf) = (Vec::new(), false, [0u8; 4096]);
                loop {
                    let n = err.read(&mut buf).await?;
                    if n == 0 {
                        return Ok::<_, std::io::Error>((kept, truncated));
                    }
                    kept.extend_from_slice(&buf[..n]);
                    if !keep_all && kept.len() > STDERR_TAIL {
                        kept = kept.split_off(kept.len() - STDERR_TAIL);
                        truncated = true;
                    }
                }
            };
            tokio::join!(read_out, read_err, child.wait())
        })
        .await;
    match waited {
        Ok((Ok(stdout), Ok((stderr, truncated)), Ok(status))) => {
            span.record("exit_code", status.code().unwrap_or(-1)).record("stderr_truncated", truncated);
            if status.success() {
                record.end("answered");
                Ok(Answer { stdout, stderr })
            } else {
                let text = scrub(&stderr);
                span.record("stderr", text.as_str());
                record.end("failed");
                Err(Failure::Answered { exit_code: status.code(), stderr: text })
            }
        }
        Ok((out, err, status)) => {
            let reason = [out.err(), err.err(), status.err()].into_iter().flatten().map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
            record.end("client-error");
            Err(Failure::Client(reason))
        }
        Err(elapsed) => {
            // The daemon's side is unknown: only the client is ended, killed and reaped here, the
            // reap owned past the cleanup bound.
            let _ = child.start_kill();
            span.record("client_kill", "sent");
            match tokio::time::timeout(CLEANUP_BOUND, child.wait()).await {
                Ok(_) => {
                    span.record("cleanup", "reaped");
                }
                Err(_) => {
                    span.record("cleanup", "reaping");
                    tokio::spawn(async move {
                        let _ = child.wait().await;
                    });
                }
            }
            if call.mutation {
                span.record("external_outcome", "unknown");
            }
            record.end("deadline");
            Err(Failure::Deadline { elapsed, bound: deadline.bound(), mutation: call.mutation })
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn sh<'a>(script: &'a [&'a str], env: &'a BTreeMap<String, String>, mutation: bool) -> Invocation<'a> {
        Invocation { operation: "test.sh", args: script, env, deployment: Some("d1"), container: None, mutation, stderr_is_data: false }
    }

    #[tokio::test]
    async fn a_hung_client_ends_at_its_deadline_killed_and_reaped_with_an_unknown_outcome() {
        let env = BTreeMap::new();
        let pidfile = std::env::temp_dir().join(format!("rdm-client-hung-{}.pid", std::process::id()));
        let script = format!("echo $$ > {}; exec sleep 600", pidfile.display());
        let args = ["-c", script.as_str()];
        let started = Instant::now();
        let got = run("sh", None, &sh(&args, &env, true), Deadline::after(Duration::from_millis(300))).await;
        let elapsed = started.elapsed();
        match got {
            Err(Failure::Deadline { mutation: true, bound, .. }) => assert_eq!(bound, Duration::from_millis(300)),
            other => panic!("a hung mutation is a deadline failure with its outcome unknown, got {other:?}"),
        }
        assert!(elapsed < Duration::from_millis(300) + CLEANUP_BOUND + Duration::from_millis(500), "named within the bound and its cleanup: {elapsed:?}");
        let pid: i32 = std::fs::read_to_string(&pidfile).unwrap().trim().parse().unwrap();
        let _ = std::fs::remove_file(&pidfile);
        // SAFETY: kill with signal 0 only asks whether the pid exists.
        assert_ne!(unsafe { libc::kill(pid, 0) }, 0, "the client {pid} is killed and reaped, not left running");
    }

    #[tokio::test]
    async fn a_callers_shorter_budget_ends_the_client_and_its_outcome_stands() {
        let env = BTreeMap::new();
        let args = ["-c", "exec sleep 600"];
        let call = sh(&args, &env, false);
        let ended = tokio::time::timeout(Duration::from_millis(200), run("sh", None, &call, Deadline::step())).await;
        assert!(ended.is_err(), "the caller's 200 ms budget wins over the step's 30 s");
    }

    #[tokio::test]
    async fn a_flooded_stderr_is_bounded_and_the_calls_secrets_never_reach_the_failure() {
        let mut env = BTreeMap::new();
        env.insert("RDM_CA_SECRET".to_string(), "s3cr3t-value".to_string());
        let args = ["-c", "head -c 2000000 /dev/zero | tr '\\0' x >&2; echo \" $RDM_CA_SECRET\" >&2; exit 3"];
        match run("sh", None, &sh(&args, &env, false), Deadline::step()).await {
            Err(Failure::Answered { exit_code, stderr }) => {
                assert_eq!(exit_code, Some(3));
                assert!(stderr.len() <= STDERR_TAIL + 64, "the stderr tail stays bounded: {} bytes", stderr.len());
                assert!(!stderr.contains("s3cr3t-value"), "a secret of the call's environment is scrubbed");
                assert!(stderr.contains("[RDM_CA_SECRET redacted]"), "{}", &stderr[stderr.len().saturating_sub(80)..]);
            }
            other => panic!("a non-zero exit is the daemon's answer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_deadline_never_reads_as_an_absent_object() {
        let f = Failure::Deadline { elapsed: Duration::from_secs(30), bound: Duration::from_secs(30), mutation: false };
        let text = f.message("docker", "inspect").to_ascii_lowercase();
        assert!(!text.contains("no such"), "{text}");
    }
}
