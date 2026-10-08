//! The deterministic network/time simulation seams (i143.e8.s3, PRD §15 layer 3).
//!
//! - [`Scheduler`]: every choice of a run is drawn from one seed ([`crate::model::Rng`]) and every
//!   step is recorded with a logical tick. The tick is the run's clock: it advances once per
//!   recorded step and never reads wall time, so two runs of one seed record equal events.
//! - [`plan`]: the order a set of cuts runs in, drawn by the seed.
//! - [`Tap`]: a testkit UDP forwarder between a caller and a node. Datagrams in each direction
//!   pass, are held (queued, delivered when the direction passes again) or are dropped. A caller
//!   pointed at the tap's address reaches the node; the node and the caller run unmodified Node RPC.
//!
//! The product takes no test code: the cuts that need a seam inside Node RPC use the options the
//! client already takes (`cut_before_finish`, `after_connect`) and the resolver's `changes()`.

use crate::model::Rng;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// A cut the network or the pool imposes on an invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// The request stream is reset before its complete send and FIN.
    UnfinishedSend,
    /// The dial never completes: the network drops every datagram toward the node.
    PartitionedDial,
    /// The reply datagrams are dropped after the request committed.
    ReplyLost,
    /// The reply datagrams are held after the request committed, delivered on release.
    ReplyHeld,
    /// The resolver supersedes a birth while a dial or a connection of the old birth is in use.
    SupersededBirth,
}

impl Cut {
    pub fn name(self) -> &'static str {
        match self {
            Cut::UnfinishedSend => "unfinished_send",
            Cut::PartitionedDial => "partitioned_dial",
            Cut::ReplyLost => "reply_lost",
            Cut::ReplyHeld => "reply_held",
            Cut::SupersededBirth => "superseded_birth",
        }
    }
}

/// One recorded step: the logical tick it happened at, what it was and its detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub tick: u64,
    pub step: String,
    pub detail: String,
}

/// A seeded run: the draws and the ordered record of what the run did.
#[derive(Debug, Clone)]
pub struct Scheduler {
    seed: u64,
    rng: Rng,
    tick: u64,
    events: Vec<Event>,
}

impl Scheduler {
    pub fn new(seed: u64) -> Self {
        Self { seed, rng: Rng(seed), tick: 0, events: Vec::new() }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The logical clock: recorded steps so far.
    pub fn tick(&self) -> u64 {
        self.tick
    }

    /// Record a step; returns the tick it took.
    pub fn record(&mut self, step: &str, detail: &str) -> u64 {
        self.tick += 1;
        self.events.push(Event { tick: self.tick, step: step.into(), detail: detail.into() });
        self.tick
    }

    /// A choice in `0..below`, drawn from the seed and recorded as a step.
    pub fn draw(&mut self, what: &str, below: u64) -> u64 {
        let v = self.rng.below(below);
        self.record(&format!("draw:{what}"), &v.to_string());
        v
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn events_json(&self) -> Vec<Value> {
        self.events.iter().map(|e| json!({"tick": e.tick, "step": e.step, "detail": e.detail})).collect()
    }
}

/// `cuts` in the order `seed` draws (Fisher-Yates over SplitMix64): the same seed, the same order.
pub fn plan(seed: u64, cuts: &[Cut]) -> Vec<Cut> {
    let mut rng = Rng(seed ^ 0x5CED_5CED_5CED_5CED);
    let mut out = cuts.to_vec();
    for i in (1..out.len()).rev() {
        out.swap(i, rng.below(i as u64 + 1) as usize);
    }
    out
}

/// What the tap does with the datagrams of one direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Pass,
    Hold,
    Drop,
}

/// A UDP forwarder in front of one node. The caller dials [`Tap::addr`]; the node sees the tap.
/// Dropped with the test: its tasks end with it.
pub struct Tap {
    addr: SocketAddr,
    stats: std::sync::Arc<TapStats>,
    latency: std::sync::Arc<std::sync::atomic::AtomicU64>,
    to_server: watch::Sender<Mode>,
    to_client: watch::Sender<Mode>,
    tasks: Vec<JoinHandle<()>>,
}

/// What the tap did, per direction: datagrams forwarded, queued and dropped.
#[derive(Debug, Default)]
pub struct TapStats {
    pub to_server: DirStats,
    pub to_client: DirStats,
}

#[derive(Debug, Default)]
pub struct DirStats {
    pub forwarded: std::sync::atomic::AtomicU64,
    pub held: std::sync::atomic::AtomicU64,
    pub dropped: std::sync::atomic::AtomicU64,
}

impl DirStats {
    /// `(forwarded, held, dropped)`.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.forwarded.load(Relaxed), self.held.load(Relaxed), self.dropped.load(Relaxed))
    }
}

impl Tap {
    /// Forward to `upstream`; both directions pass until set otherwise.
    pub async fn start(upstream: SocketAddr) -> std::io::Result<Self> {
        let front = std::sync::Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let back = std::sync::Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        back.connect(upstream).await?;
        let addr = front.local_addr()?;
        let client: std::sync::Arc<std::sync::Mutex<Option<SocketAddr>>> = Default::default();
        let (to_server, server_mode) = watch::channel(Mode::Pass);
        let (to_client, client_mode) = watch::channel(Mode::Pass);
        let stats = std::sync::Arc::new(TapStats::default());
        let latency: std::sync::Arc<std::sync::atomic::AtomicU64> = Default::default();
        let (up_tx, up_rx) = tokio::sync::mpsc::unbounded_channel();
        let (down_tx, down_rx) = tokio::sync::mpsc::unbounded_channel();
        let up_out = {
            let back = back.clone();
            tokio::spawn(deliver(up_rx, move |d| {
                let back = back.clone();
                async move { let _ = back.send(&d).await; }
            }))
        };
        let down_out = {
            let (front, client) = (front.clone(), client.clone());
            tokio::spawn(deliver(down_rx, move |d| {
                let (front, client) = (front.clone(), client.clone());
                async move {
                    let to = *client.lock().unwrap();
                    if let Some(to) = to {
                        let _ = front.send_to(&d, to).await;
                    }
                }
            }))
        };
        let up = {
            let (stats, latency) = (stats.clone(), latency.clone());
            let (front, client) = (front.clone(), client.clone());
            tokio::spawn(async move {
                pump(server_mode, stats, |s| &s.to_server, || async { recv(&front).await.map(|(d, from)| { *client.lock().unwrap() = Some(from); d }) }, |d| {
                    let _ = up_tx.send((due(&latency), d));
                    async {}
                })
                .await
            })
        };
        let down = {
            let (stats, latency) = (stats.clone(), latency.clone());
            let back = back.clone();
            tokio::spawn(async move {
                pump(client_mode, stats, |s| &s.to_client, || async { recv(&back).await.map(|(d, _)| d) }, |d| {
                    let _ = down_tx.send((due(&latency), d));
                    async {}
                })
                .await
            })
        };
        Ok(Self { addr, stats, latency, to_server, to_client, tasks: vec![up, down, up_out, down_out] })
    }

    /// Datagram counts per direction.
    pub fn stats(&self) -> &TapStats {
        &self.stats
    }

    /// Every forwarded datagram, in both directions, is delivered `d` after it is read (order kept).
    pub fn set_latency(&self, d: std::time::Duration) {
        self.latency.store(d.as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// The address a caller dials.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Datagrams toward the node. A switch to [`Mode::Pass`] delivers what was held, in order.
    pub fn set_to_server(&self, m: Mode) {
        self.to_server.send_replace(m);
    }

    /// Datagrams toward the caller. A switch to [`Mode::Pass`] delivers what was held, in order.
    pub fn set_to_client(&self, m: Mode) {
        self.to_client.send_replace(m);
    }
}

impl Drop for Tap {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn due(latency: &std::sync::atomic::AtomicU64) -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_micros(latency.load(std::sync::atomic::Ordering::Relaxed))
}

/// Deliver each datagram at its due time, in the order read.
async fn deliver<S, SF>(mut rx: tokio::sync::mpsc::UnboundedReceiver<(tokio::time::Instant, Vec<u8>)>, send: S)
where
    S: Fn(Vec<u8>) -> SF,
    SF: std::future::Future<Output = ()>,
{
    while let Some((at, d)) = rx.recv().await {
        tokio::time::sleep_until(at).await;
        send(d).await;
    }
}

async fn recv(s: &UdpSocket) -> Option<(Vec<u8>, SocketAddr)> {
    let mut buf = vec![0u8; 65536];
    let (n, from) = s.recv_from(&mut buf).await.ok()?;
    buf.truncate(n);
    Some((buf, from))
}

/// One direction: read a datagram, then pass it, queue it or drop it by the current mode; a switch
/// back to pass delivers the queue first.
async fn pump<R, RF, S, SF>(mut mode: watch::Receiver<Mode>, stats: std::sync::Arc<TapStats>, dir: fn(&TapStats) -> &DirStats, read: R, send: S)
where
    R: Fn() -> RF,
    RF: std::future::Future<Output = Option<Vec<u8>>>,
    S: Fn(Vec<u8>) -> SF,
    SF: std::future::Future<Output = ()>,
{
    use std::sync::atomic::Ordering::Relaxed;
    let mut held: std::collections::VecDeque<Vec<u8>> = Default::default();
    loop {
        tokio::select! {
            changed = mode.changed() => {
                if changed.is_err() { return; }
                if *mode.borrow() == Mode::Pass {
                    while let Some(d) = held.pop_front() { dir(&stats).forwarded.fetch_add(1, Relaxed); send(d).await; }
                }
            }
            got = read() => {
                let Some(d) = got else { return };
                let m = *mode.borrow();
                match m {
                    Mode::Pass => { dir(&stats).forwarded.fetch_add(1, Relaxed); send(d).await }
                    Mode::Hold => { dir(&stats).held.fetch_add(1, Relaxed); held.push_back(d) }
                    Mode::Drop => { dir(&stats).dropped.fetch_add(1, Relaxed); }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_seed_draws_one_plan_and_one_event_record() {
        let all = [Cut::UnfinishedSend, Cut::PartitionedDial, Cut::ReplyLost, Cut::ReplyHeld, Cut::SupersededBirth];
        assert_eq!(plan(7, &all), plan(7, &all));
        assert!((0..32).any(|s| plan(s, &all) != plan(7, &all)), "the seed decides the order");
        let run = |seed| {
            let mut s = Scheduler::new(seed);
            let d = s.draw("len", 1000);
            s.record("cut", "x");
            (d, s.events().to_vec())
        };
        assert_eq!(run(3), run(3));
        assert_ne!(run(3).0, run(4).0);
        assert_eq!(run(3).1.iter().map(|e| e.tick).collect::<Vec<_>>(), [1, 2], "the clock is the step count");
    }
}
