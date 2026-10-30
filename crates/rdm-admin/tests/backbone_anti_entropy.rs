//! A mesh primary's mesh topology anti-entropy is a perpetual timer (gossip.md §3.2: "PERPETUAL TIMERS …
//! mesh topology anti-entropy — the mesh primary on the backbone; a few seconds; also sent at once on a
//! material change"). The fabric-primary reads a peer mesh's liveness from that publication alone:
//! t is the time since its last direct backbone receipt from the mesh (mesh-investigation.md), and
//! from the track rung (`investigate::Rungs::track`, twice the backbone interval) the mesh is held
//! unheard and nothing of it is restarted, replaced or deleted (R-D1). A publisher that goes quiet
//! for longer than that while it is live and serving targets its own mesh.
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`), its mesh's primary, at the
//! gate's cadence (`tools/test-runner` CADENCE, set by the binary's ctor in `tests/main.rs`), in a
//! process of its own. Its Build state is wrapped
//! (`Wiring::builds`) so the cell can make every read of the current Build (R-BI3, the read the
//! admin's publication makes each turn) take three backbone intervals, which is what a store read does
//! while the disk is held by another writer. The observer is a peer mesh's place on the backbone.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{IncarnationId, MeshId, NodeId};
use rdm_transport::topology::{gossip_addr, learn_addresses, Backbone, TopologyView};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_core::PathName;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, CurrentBuild, OwedExit};
use rdm_admin::investigate::Rungs;
use rdm_admin::wiring::{FabricHooks, Wiring};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// What a step that is immediate on a correct admin is given before the cell fails by name.
const NEVER: Duration = Duration::from_secs(20);
/// How long one read of the current Build takes while the store is held: three backbone intervals.
const HELD_READ: Duration = Duration::from_millis(1500);
/// How long the store stays held: two held reads end to end.
const HELD_FOR: Duration = Duration::from_millis(3000);
/// How often the observer reads its t of the publisher's mesh.
const SAMPLE: Duration = Duration::from_millis(20);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// The Build state whose reads of the current Build (R-BI3: the Build entities read last in, first
/// out) take [`HELD_READ`] while the cell holds it, each held read a `cell.store.update.via-held-read`
/// span; every other call passes straight through.
struct HeldBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    held: Arc<AtomicBool>,
    reads: Arc<AtomicU64>,
    began: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for HeldBuilds {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        if self.held.load(Ordering::SeqCst) {
            let span = tracing::info_span!("cell.store.update.via-held-read", held_ms = HELD_READ.as_millis() as u64);
            self.began.notify_waiters();
            tracing::Instrument::instrument(tokio::time::sleep(HELD_READ), span).await;
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        let facts = self.facts().await?;
        Ok(match build { Some(b) => facts.into_iter().filter(|f| f.build_id() == b).collect(), None => facts })
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn note_blocked(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str) {
        self.inner.note_blocked(build_id, lane, attempt, operation, step, reason).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

/// The cell's own span capture: the product span pipeline in front of an in-memory exporter, behind
/// a dispatcher installed on every thread of the cell's runtime.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("mesh1.admin.1", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self) -> Vec<Value> {
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let ns = |t: std::time::SystemTime| t.duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({ "name": s.name, "trace_id": s.span_context.trace_id().to_string(), "start_ns": ns(s.start_time), "end_ns": ns(s.end_time), "attributes": attributes })
            })
            .collect()
    }
}

/// CONTRACT (gossip.md §3.2, the `MeshTopology` row: "on its anti-entropy timer and at once on a material
/// change"; mesh-investigation.md: t from the last direct backbone receipt; R-D1): a mesh primary
/// whose reads of the shared store take three backbone intervals each goes on publishing its mesh
/// on the backbone every interval. A peer mesh's t of it stays under the track rung the whole time
/// the store is held, so the fabric-primary never holds a live, serving mesh unheard. Must NOT
/// happen: the publication waiting on a store read of the admin's publication (the current Build, the exit
/// rows, the view's installation), so a held disk reads as a silent mesh.
#[test]
fn a_mesh_primary_whose_store_reads_are_held_publishes_its_mesh_on_the_backbone_every_interval() {
    let cell = "a_mesh_primary_whose_store_reads_are_held_publishes_its_mesh_on_the_backbone_every_interval";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let track = Rungs::configured().track;
    let cap = capture();
    let (worst, held_reads, during) = cap.run(async {
        let dir = std::env::temp_dir().join(format!("backbone-anti-entropy-{}", NodeId::mint()));
        let (held, reads, began) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicU64::new(0)), Arc::new(tokio::sync::Notify::new()));
        let wrap = {
            let (held, reads, began) = (held.clone(), reads.clone(), began.clone());
            move |inner: Arc<dyn BuildStateAdapter>| -> Arc<dyn BuildStateAdapter> { Arc::new(HeldBuilds { inner, held, reads, began }) }
        };
        let wiring = Wiring { fabric_hooks: FabricHooks::no_app_work(), builds: Some(Box::new(wrap)), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        tokio::time::timeout(NEVER, async {
            while !running.publishes_its_mesh() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the Day-0 admin never became its mesh's publisher");
        let root = running.digest.lock().unwrap().clone();

        // A peer mesh's place on the backbone, seeded with the publisher's address.
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let seed = gossip_addr(&root).expect("the publisher's digest names its gossip address");
        learn_addresses(&ep, &[seed.clone()]).unwrap();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(root.emitted_at_rafka_ms);
        let observer = TopologyView::join(&gossip, &ep, &root.fabric_id, "mesh2", &MeshId::mint(), "mesh2.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        let _observer_bb = Backbone::join(&gossip, &ep, &observer, "mesh2", rdm_core::PublisherId { node: "mesh2.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![seed]).await.unwrap();
        let unheard = || observer.digest_view.mesh_unheard(&root.node.name.mesh, std::time::Instant::now());
        tokio::time::timeout(NEVER, async {
            while unheard().is_none() {
                tokio::time::sleep(SAMPLE).await;
            }
        })
        .await
        .expect("the peer mesh never received the publisher's mesh on the backbone");

        // The store is held: every read of the current Build now takes three backbone intervals. The
        // window opens once a held read has begun.
        let first = began.notified();
        held.store(true, Ordering::SeqCst);
        tokio::time::timeout(NEVER, first).await.expect("no read of the current Build began while the store was held");
        let opened = std::time::Instant::now();
        let mut worst = Duration::ZERO;
        let mut during = 0u32;
        while opened.elapsed() < HELD_FOR {
            if let Some(t) = unheard() {
                worst = worst.max(t);
            }
            during += 1;
            tokio::time::sleep(SAMPLE).await;
        }
        held.store(false, Ordering::SeqCst);
        let held_reads = reads.load(Ordering::SeqCst);

        drop(_observer_bb);
        ep.close().await;
        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
        (worst, held_reads, during)
    });

    let spans = cap.spans();
    let artifact = std::env::temp_dir().join("i143-testkit").join(cell);
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("spans.jsonl"), spans.iter().map(|s| s.to_string() + "\n").collect::<String>()).unwrap();
    let receipts: Vec<(u64, String)> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.mesh.topology.update.via-snapshot-installed" && s["attributes"]["node"] == "mesh2.admin.1" && s["attributes"]["channel"] == "backbone" && s["attributes"]["mesh"] == "mesh1")
        .map(|s| (s["start_ns"].as_u64().unwrap_or(0), s["attributes"]["snapshot_id"].as_str().unwrap_or_default().to_string()))
        .collect();
    let held_spans = spans.iter().filter(|s| s["name"] == "cell.store.update.via-held-read").count();
    println!("{cell}: worst t {worst:?} over {during} samples while the store was held ({held_reads} held reads ended, {held_spans} held-read spans); track rung {track:?}; backbone receipts at the peer {receipts:?}; spans in {}", artifact.display());
    assert!(held_reads >= 1, "the store was never held for a whole read inside the window: the cell proved nothing");
    assert!(
        worst < track,
        "the peer mesh's t of the publisher reached {worst:?} while the publisher's store reads were held, at or past the track rung {track:?}: the fabric-primary would hold a live, serving mesh unheard (R-D1). The mesh topology anti-entropy waited on the admin's store reads. Backbone receipts at the peer (start ns, snapshot id): {receipts:?}"
    );
}
