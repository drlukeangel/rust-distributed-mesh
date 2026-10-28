//! `EvidenceCapture`: a cell's span evidence is scoped to the cell's own threads, in a file of its
//! own, and exported to the collector the cell names; one test binary is one process, so nothing
//! of it is process-wide.

use rdm_transport::EvidenceCapture;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};

/// The span names and `service` attributes a capture read back.
fn read(spans: &[Value]) -> BTreeSet<(String, String)> {
    spans.iter().map(|s| (s["name"].as_str().unwrap_or("").to_string(), s["attributes"]["service"].as_str().unwrap_or("").to_string())).collect()
}

/// CONTRACT (rdm-crate-and-test-layout.md §3, "One binary is one process": a test captures tracing
/// with a thread-scoped `set_default`, never a process-wide default or a telemetry `init`; the
/// provider's owner flushes then shuts it down in its own Drop): two cells capture at once in one
/// process. Each reads back exactly the spans its own threads opened (one on the cell's thread, one
/// on a worker of its runtime), from a file of its own. A thread of neither cell has no default
/// dispatcher, and its span reaches neither capture. The cell given a collector exports its spans
/// there over OTLP/HTTP when its capture drops; the other cell's spans never reach it. Must NOT
/// happen: the first capture installed process-wide, so it takes the other threads' spans and the
/// second capture's never reach their own file.
#[test]
fn two_cells_capturing_at_once_each_read_only_their_own_spans_and_the_one_given_a_collector_exports_them() {
    if crate::own_process::delegated(module_path!(), "two_cells_capturing_at_once_each_read_only_their_own_spans_and_the_one_given_a_collector_exports_them") {
        return;
    }
    let collector = crate::otlp_http_export::Collector::start();
    let dir = std::env::temp_dir().join(format!("rdm-evidence-capture-{}", std::process::id()));
    let both = Arc::new(Barrier::new(3));
    let cell = |service: &'static str, endpoint: Option<String>| {
        let (dir, both) = (dir.clone(), both.clone());
        std::thread::spawn(move || {
            let capture = EvidenceCapture::start_with_collector(&dir, service, endpoint.as_deref()).expect("the capture's evidence file can be created");
            both.wait();
            capture.run(2, async move {
                tracing::info_span!("rdm.test.capture.serve.from-cell-thread", service).in_scope(|| {});
                tokio::spawn(async move { tracing::info_span!("rdm.test.capture.serve.from-worker", service).in_scope(|| {}) }).await.unwrap();
            });
            both.wait();
            let spans = capture.spans().unwrap_or_else(|e| panic!("{service}: {e}"));
            (read(&spans), capture.path().to_path_buf())
        })
    };
    let a = cell("rdm-capture-a", Some(collector.endpoint.clone()));
    let b = cell("rdm-capture-b", None);
    both.wait();
    let bare = std::thread::spawn(|| {
        let none = tracing::dispatcher::get_default(|d| d.is::<tracing::subscriber::NoSubscriber>());
        tracing::info_span!("rdm.test.capture.serve.from-no-cell", service = "none").in_scope(|| {});
        none
    })
    .join()
    .unwrap();
    both.wait();
    let ((a_read, a_path), (b_read, b_path)) = (a.join().unwrap(), b.join().unwrap());
    let own = |service: &str| ["rdm.test.capture.serve.from-cell-thread", "rdm.test.capture.serve.from-worker"].iter().map(|n| (n.to_string(), service.to_string())).collect::<BTreeSet<_>>();
    assert!(bare, "a thread of neither cell has no default dispatcher: no capture is process-wide");
    assert_eq!(a_read, own("rdm-capture-a"), "cell a reads exactly its own threads' spans from {}", a_path.display());
    assert_eq!(b_read, own("rdm-capture-b"), "cell b reads exactly its own threads' spans from {}", b_path.display());
    assert_ne!(a_path, b_path, "each capture writes a file of its own");
    let traces: Vec<_> = collector.seen().into_iter().filter(|s| s.request_line.starts_with("POST /v1/traces ")).collect();
    let carried = |needle: &str| traces.iter().any(|s| s.body.windows(needle.len()).any(|w| w == needle.as_bytes()));
    assert!(carried("rdm.test.capture.serve.from-worker") && carried("rdm-capture-a"), "cell a's spans reached its collector over OTLP/HTTP ({} trace requests)", traces.len());
    assert!(!carried("rdm-capture-b") && !carried("rdm.test.capture.serve.from-no-cell"), "only cell a's spans reached cell a's collector");
    let _ = std::fs::remove_file(a_path);
    let _ = std::fs::remove_file(b_path);
}

/// Drop `dispatch` where tracing-core drops another cell's dispatcher: inside its dispatcher lock,
/// taken for writing by a new dispatcher's registration while it asks every live dispatcher's
/// interest in every registered callsite (tracing-core 0.1.36 `callsite.rs` 485-487 and 552). The
/// new dispatcher's one layer drops `dispatch` the first time it is asked. `Err` names a
/// registration that did not return within its bound: the drop waited on the lock its own thread
/// holds, and so does every thread that next registers a dispatcher or a callsite.
fn dropped_inside_the_dispatcher_lock(dispatch: tracing::Dispatch) -> Result<(), String> {
    use tracing_subscriber::layer::SubscriberExt;
    struct DropWhenAsked(std::sync::Mutex<Option<tracing::Dispatch>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for DropWhenAsked {
        fn register_callsite(&self, _: &'static tracing::Metadata<'static>) -> tracing::subscriber::Interest {
            drop(self.0.lock().unwrap_or_else(|e| e.into_inner()).take());
            tracing::subscriber::Interest::sometimes()
        }
    }
    let (registered, returned) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("dispatcher-lock-drop".into())
        .spawn(move || {
            let registering = tracing::Dispatch::new(tracing_subscriber::registry().with(DropWhenAsked(std::sync::Mutex::new(Some(dispatch)))));
            let _ = registered.send(());
            drop(registering);
        })
        .map_err(|e| format!("the registering thread could not start: {e}"))?;
    returned
        .recv_timeout(std::time::Duration::from_secs(20))
        .map_err(|_| "a dispatcher's registration did not return within 20 s: the capture's dispatcher, dropped inside tracing-core's dispatcher lock, waited on that lock".to_string())
}

/// CONTRACT (rdm-crate-and-test-layout.md §3, "One binary is one process"): a cell's capture owns its
/// provider and ends it in its own Drop, on the cell's thread; its dispatcher holds nothing whose
/// drop runs telemetry code. tracing-core drops a cell's dispatcher wherever its last reference
/// goes, which can be inside its own dispatcher lock while another cell registers a dispatcher
/// (rdm#106: `Rebuilder::for_each` upgrades the dispatcher and drops it at `callsite.rs:572`). The
/// capture's dispatcher, dropped there, lets that registration return. Must NOT happen: the
/// dispatcher's drop ends the provider (`TracerProviderInner::drop`, opentelemetry_sdk 0.31
/// `provider.rs:142`), whose event registers its callsite and waits on the dispatcher lock its
/// thread already holds, and every cell of the binary waits on it (`rdm-transport:main`: every
/// running test past 60 s, the binary killed at 300 s).
#[test]
fn a_captures_dispatcher_dropped_inside_tracing_cores_dispatcher_lock_lets_the_registration_return() {
    if crate::own_process::delegated(module_path!(), "a_captures_dispatcher_dropped_inside_tracing_cores_dispatcher_lock_lets_the_registration_return") {
        return;
    }
    let capture = EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-capture-lock-drop").expect("the cell's evidence file can be created");
    tracing::info_span!("rdm.test.capture.delete.inside-dispatcher-lock").in_scope(|| {});
    let dispatch = tracing::dispatcher::get_default(|d| d.clone());
    drop(capture);
    dropped_inside_the_dispatcher_lock(dispatch).unwrap_or_else(|e| panic!("{e}"));
}
