//! `rdm_node::operation`'s cells: acceptance and response recovery over a store the application
//! provides (consumer-guide-ops.md §5; node-rpc.md §33, §43), each decision read from its span.

use rdm_core::NodeId;
use rdm_node::operation::{Decision, OperationStore, Operations, Recorded, State};
use rdm_transport::Acceptance;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// What survives a restart: the store's records, shared by the store of every "process".
#[derive(Default)]
struct Durable {
    records: BTreeMap<String, Recorded<String, String>>,
    puts: Vec<String>,
}

/// An application's store over [`Durable`]. `watch` is the request's acceptance, read at each put
/// so a cell sees whether the request was accepted before its record was durable; `fail` makes the
/// next put fail.
#[derive(Clone, Default)]
struct Store {
    durable: Arc<Mutex<Durable>>,
    watch: Arc<Mutex<Option<Acceptance>>>,
    accepted_before_put: Arc<Mutex<Vec<String>>>,
    fail: Arc<Mutex<bool>>,
}

impl Store {
    fn after_restart(&self) -> Self {
        Store { durable: self.durable.clone(), ..Default::default() }
    }

    fn put(&self, what: String) -> Result<(), String> {
        if std::mem::take(&mut *self.fail.lock().unwrap()) {
            return Err(format!("the disk refused {what}"));
        }
        if self.watch.lock().unwrap().as_ref().is_some_and(Acceptance::is_accepted) {
            self.accepted_before_put.lock().unwrap().push(what.clone());
        }
        self.durable.lock().unwrap().puts.push(what);
        Ok(())
    }

    fn puts(&self) -> Vec<String> {
        self.durable.lock().unwrap().puts.clone()
    }
}

#[async_trait::async_trait]
impl OperationStore for Store {
    type Input = String;
    type Output = String;
    type Error = String;

    async fn get(&self, operation: &str) -> Result<Option<Recorded<String, String>>, String> {
        // A store that yields: a second decision would interleave here were decisions not one at a time.
        tokio::task::yield_now().await;
        Ok(self.durable.lock().unwrap().records.get(operation).cloned())
    }

    async fn put_accepted(&self, operation: &str, input: &String) -> Result<(), String> {
        self.put(format!("accepted {operation}"))?;
        self.durable.lock().unwrap().records.insert(operation.into(), Recorded { input: input.clone(), output: None });
        Ok(())
    }

    async fn put_completed(&self, operation: &str, output: &String) -> Result<(), String> {
        self.put(format!("completed {operation}"))?;
        if let Some(r) = self.durable.lock().unwrap().records.get_mut(operation) {
            r.output = Some(output.clone());
        }
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<(String, String)>, String> {
        Ok(self.durable.lock().unwrap().records.iter().filter(|(_, r)| r.output.is_none()).map(|(id, r)| (id.clone(), r.input.clone())).collect())
    }
}

fn capture(tag: &str) -> rdm_transport::EvidenceCapture {
    let dir = std::env::temp_dir().join(format!("rdm-operations-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    rdm_transport::EvidenceCapture::start(&dir, "rdm-operations").expect("the cell's evidence file can be created")
}

/// Each span named `name`, as `(operation, field)`: the operation it names and its `field`.
fn spans_of(spans: &[serde_json::Value], name: &str, field: &str) -> Vec<(String, String)> {
    let text = |v: &serde_json::Value| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
    spans.iter().filter(|s| s["name"] == name).map(|s| (text(&s["attributes"]["operation"]), text(&s["attributes"][field]))).collect()
}

/// An acceptance watched by `store`, as a new request's.
fn request(store: &Store) -> Acceptance {
    let a = Acceptance::detached();
    *store.watch.lock().unwrap() = Some(a.clone());
    a
}

/// CONTRACT: a new operation is recorded, then accepted; a repeat of it while its work runs is
/// in-flight and a repeat once completed answers its recorded output, both accepted, neither recorded
/// again; its id with another input is a conflict, not accepted; a completion is recorded once, its
/// first output standing; a read by id answers unknown, in-flight and completed. Each decision is its
/// span, naming the operation.
/// Must NOT happen: the request accepted before its record is durable, or the work recorded twice.
#[test]
fn an_operation_is_recorded_then_accepted_and_its_result_is_recovered_by_its_id() {
    let capture = capture("accept");
    capture.run(2, async {
        let store = Store::default();
        let ops = Operations::new(store.clone());
        assert_eq!(ops.read("op-1").await.unwrap(), State::Unknown);

        let first = request(&store);
        assert_eq!(ops.accept(&first, "op-1", &"in".to_string()).await.unwrap(), Decision::Recorded);
        assert!(first.is_accepted(), "a recorded operation is accepted");
        assert_eq!(store.accepted_before_put.lock().unwrap().clone(), Vec::<String>::new(), "accepted only once its record was durable");

        let repeat = request(&store);
        assert_eq!(ops.accept(&repeat, "op-1", &"in".to_string()).await.unwrap(), Decision::InFlight);
        assert!(repeat.is_accepted(), "its acceptance record is durable already");
        assert_eq!(ops.read("op-1").await.unwrap(), State::InFlight);

        let other = request(&store);
        assert_eq!(ops.accept(&other, "op-1", &"other".to_string()).await.unwrap(), Decision::Conflict);
        assert!(!other.is_accepted(), "a conflict is the protocol's refusal, accepted at its reply");

        assert!(ops.complete("op-1", &"OUT".to_string()).await.unwrap(), "the completion is recorded");
        assert!(!ops.complete("op-1", &"AGAIN".to_string()).await.unwrap(), "a second completion is not");
        assert!(!ops.complete("never-accepted", &"x".to_string()).await.unwrap(), "nor one of an operation never accepted");
        assert_eq!(ops.read("op-1").await.unwrap(), State::Completed("OUT".into()));
        let after = request(&store);
        assert_eq!(ops.accept(&after, "op-1", &"in".to_string()).await.unwrap(), Decision::Completed("OUT".into()));
        assert!(after.is_accepted());
        assert_eq!(store.puts(), vec!["accepted op-1".to_string(), "completed op-1".to_string()], "one acceptance and one completion");
    });
    let spans = capture.spans().expect("the cell's spans");
    let op = |d: &[&str]| d.iter().map(|d| ("op-1".to_string(), d.to_string())).collect::<Vec<_>>();
    assert_eq!(spans_of(&spans, "rdm.node_rpc.operation.create.via-accept", "decision"), op(&["recorded", "in-flight", "conflict", "completed"]));
    assert_eq!(
        spans_of(&spans, "rdm.node_rpc.operation.update.via-complete", "recorded"),
        vec![("op-1".into(), "true".into()), ("op-1".into(), "false".into()), ("never-accepted".into(), "false".into())]
    );
    assert_eq!(spans_of(&spans, "rdm.node_rpc.operation.serve.via-read", "state"), op(&["unknown", "in-flight", "completed"]));
}

/// CONTRACT: an operation accepted and not completed when its process ends is named by the next
/// process's `pending` over the same store, and once completed it is pending no more; the resume is
/// its span, counting what it found.
/// Must NOT happen: the accepted operation lost across the restart.
#[test]
fn an_operation_accepted_before_a_restart_is_pending_after_it() {
    let capture = capture("restart");
    capture.run(2, async {
        let before = Store::default();
        {
            let ops = Operations::new(before.clone());
            assert_eq!(ops.accept(&request(&before), "op-1", &"in".to_string()).await.unwrap(), Decision::Recorded);
            assert_eq!(ops.accept(&request(&before), "op-2", &"in".to_string()).await.unwrap(), Decision::Recorded);
            assert!(ops.complete("op-2", &"OUT".to_string()).await.unwrap());
        }
        let ops = Operations::new(before.after_restart());
        assert_eq!(ops.pending().await.unwrap(), vec![("op-1".to_string(), "in".to_string())], "the one accepted and not completed");
        assert!(ops.complete("op-1", &"OUT".to_string()).await.unwrap());
        assert_eq!(ops.pending().await.unwrap(), Vec::<(String, String)>::new());
    });
    let spans = capture.spans().expect("the cell's spans");
    let counts: Vec<String> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.operation.update.via-resume").map(|s| s["attributes"]["pending"].as_str().unwrap_or_default().to_string()).collect();
    assert_eq!(counts, vec!["1", "0"]);
}

/// CONTRACT: two invocations of one new operation decided at once record it once: one is recorded,
/// the other in-flight.
/// Must NOT happen: two acceptance records of one operation.
#[test]
fn two_invocations_of_one_new_operation_record_it_once() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let store = Store::default();
        let ops = Arc::new(Operations::new(store.clone()));
        let calls: Vec<_> = (0..8)
            .map(|_| {
                let ops = ops.clone();
                tokio::spawn(async move { ops.accept(&Acceptance::detached(), "op-1", &"in".to_string()).await.unwrap() })
            })
            .collect();
        let mut decisions = Vec::new();
        for c in calls {
            decisions.push(c.await.unwrap());
        }
        assert_eq!(decisions.iter().filter(|d| **d == Decision::Recorded).count(), 1, "{decisions:?}");
        assert_eq!(decisions.iter().filter(|d| **d == Decision::InFlight).count(), 7, "{decisions:?}");
        assert_eq!(store.puts(), vec!["accepted op-1".to_string()]);
    });
}

/// CONTRACT: a store that cannot record the operation leaves the request unaccepted, and its span
/// names the store's error; the next request for it is decided anew.
/// Must NOT happen: a request acknowledged with nothing durable naming its work.
#[test]
fn a_record_the_store_refuses_leaves_the_request_unaccepted() {
    let capture = capture("refused");
    capture.run(2, async {
        let store = Store::default();
        let ops = Operations::new(store.clone());
        *store.fail.lock().unwrap() = true;
        let refused = request(&store);
        let err = ops.accept(&refused, "op-1", &"in".to_string()).await.expect_err("the store's failure is the call's");
        assert!(err.contains("the disk refused"), "{err}");
        assert!(!refused.is_accepted(), "nothing durable: not accepted");
        let again = request(&store);
        assert_eq!(ops.accept(&again, "op-1", &"in".to_string()).await.unwrap(), Decision::Recorded);
        assert!(again.is_accepted());
    });
    let spans = capture.spans().expect("the cell's spans");
    let accepts: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.operation.create.via-accept").collect();
    assert_eq!(accepts.len(), 2);
    assert!(accepts[0]["attributes"]["error"].as_str().is_some_and(|e| e.contains("the disk refused")), "{:?}", accepts[0]);
    assert_eq!(accepts[1]["attributes"]["decision"], "recorded");
}
