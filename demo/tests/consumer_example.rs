//! The consumer example's cells (`rshape_consumer::consumer_example`; consumer-guide-ops.md §7 item 6):
//! each op through the conformance harness by naming its type, and the mutating op's accepted work
//! still done after its service is opened again over the same record.

use rdm_core::catalog::OpOwner;
use rdm_transport::stream::ReplyStream;
use rdm_transport::PreNodeReadyGate;
use rshape_consumer::consumer_example::{
    read_records, register, ExampleRecords, ExampleService, ExampleWork, RecordsRequest, Submitted, WorkReply, WorkRequest, OWNER,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// A directory of its own for one cell's record.
fn record_dir(cell: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("consumer-example-{cell}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn evidence_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/evidence/consumer-example");
    std::fs::create_dir_all(&dir).expect("the evidence directory can be created");
    dir
}

/// The node's gate as it stands once `pre_node_ready` passed: the harness runs the op's handlers, and
/// its own not-ready check closes the runtime's door, not this gate.
fn passed_gate() -> PreNodeReadyGate {
    let gate = PreNodeReadyGate::new();
    gate.open();
    gate
}

/// CONTRACT: `ExampleWork`, registered as the example's node registers it, passes every unary check of
/// the conformance harness: answered, the reserved refusals and the runtime's not-ready, busy and
/// connections-deleting, its own named refusals (`unknown`, `conflict`), the request over its
/// ceiling refused `Malformed(TooLarge)`, a duplicate delivery suppressed, and a reply lost after
/// acceptance delivered once. One trace per check, under `rdm.test.conformance.serve.via-check`.
#[test]
fn the_example_work_op_passes_the_conformance_harness() {
    let capture = rdm_transport::EvidenceCapture::start(&evidence_dir(), "consumer-example-work").expect("the cell's evidence file can be created");
    let report = capture.run(4, async {
        let service = ExampleService::open(&record_dir("work"), Duration::from_millis(50)).expect("the record opens");
        assert_eq!(service.submit("seeded", "a").unwrap(), Submitted::Recorded);
        let gate = passed_gate();
        let fixture = rdm_test::conformance::Unary::<ExampleWork> {
            serve: Arc::new(move |b| register(b, service.clone(), gate.clone())),
            answered: WorkRequest::Submit { operation: "conformance".into(), input: "x".into() },
            refusals: vec![
                (WorkRequest::Get { operation: "never-submitted".into() }, "unknown"),
                (WorkRequest::Submit { operation: "seeded".into(), input: "b".into() }, "conflict"),
            ],
        };
        rdm_test::conformance::unary(&fixture).await
    });
    println!("{}", report.summary());
    assert!(report.failed().is_empty(), "{}", report.summary());
}

/// CONTRACT: `ExampleRecords`, registered as the example's node registers it and read by the
/// example's own consumer (`read_records`), passes every streaming check of the conformance harness:
/// frames in order, the 64-frame bound holding the producer, a caller dropped mid-stream releasing
/// the invocation, its named refusal (`out-of-range`), the request over its ceiling refused, a
/// duplicate suppressed, frames lost after acceptance resent once, and the consumer acknowledging
/// only what it recorded.
#[test]
fn the_example_records_op_passes_the_conformance_harness() {
    let capture = rdm_transport::EvidenceCapture::start(&evidence_dir(), "consumer-example-records").expect("the cell's evidence file can be created");
    let report = capture.run(4, async {
        // A record of 100 accepted operations: the long read hands over more than 64 frames.
        let service = ExampleService::open(&record_dir("records"), Duration::from_secs(3600)).expect("the record opens");
        for i in 0..100 {
            assert_eq!(service.submit(&format!("op-{i}"), "x").unwrap(), Submitted::Recorded);
        }
        let gate = passed_gate();
        let fixture = rdm_test::conformance::Streaming::<ExampleRecords> {
            serve: Arc::new(move |b| register(b, service.clone(), gate.clone())),
            answered: RecordsRequest::Read { from: 0, max: 3 },
            long: RecordsRequest::Read { from: 0, max: 1000 },
            refusals: vec![(RecordsRequest::Read { from: 1_000_000, max: 1 }, "out-of-range")],
            consume: Arc::new(|stream: ReplyStream<ExampleRecords>, record: rdm_test::conformance::StreamRecord| {
                Box::pin(async move {
                    let _ = read_records(stream, |position, kind, _| {
                        record.put(position, kind);
                        Ok(())
                    })
                    .await;
                })
            }),
        };
        rdm_test::conformance::streaming(&fixture).await
    });
    println!("{}", report.summary());
    assert!(report.failed().is_empty(), "{}", report.summary());
}

/// CONTRACT: an operation accepted and not completed when its service ends is completed by the next
/// service opened over the same record, once it resumes, and completed once: the record holds one
/// acceptance and one completion, and a second submit of the same id answers the recorded result.
/// Must NOT happen: the pending operation lost, or its work recorded twice.
#[test]
fn an_operation_accepted_before_a_restart_is_done_after_it() {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let dir = record_dir("restart");
        // The first process: accepts, and ends before its work completes.
        {
            let before = ExampleService::open(&dir, Duration::from_secs(3600)).unwrap();
            assert_eq!(before.submit("op-1", "hello").unwrap(), Submitted::Recorded);
            assert!(before.start("op-1".into()), "its work starts");
            assert_eq!(before.get("op-1"), WorkReply::Pending { operation: "op-1".into() });
        }
        // The process after the restart: its record names the pending operation, and its start resumes it.
        let after = ExampleService::open(&dir, Duration::from_millis(10)).unwrap();
        assert_eq!(after.get("op-1"), WorkReply::Pending { operation: "op-1".into() }, "the accepted work is in the record");
        assert_eq!(after.resume(), 1, "the one pending operation is resumed");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while after.get("op-1") != (WorkReply::Done { operation: "op-1".into(), output: "HELLO".into() }) {
            assert!(tokio::time::Instant::now() < deadline, "the resumed operation completes: {:?}", after.get("op-1"));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(after.resume(), 0, "nothing is pending once it completed");
        assert_eq!(after.submit("op-1", "hello").unwrap(), Submitted::Completed("HELLO".into()), "a second submit answers the recorded result");
        let (end, entries) = after.entries(0, 10).unwrap();
        assert_eq!(end, 2, "one acceptance and one completion: {entries:?}");
    });
}

/// CONTRACT: a record whose last line a crash cut short opens without that line (it was never synced,
/// so never acknowledged), and a line inside the record that does not decode refuses the open naming
/// its line.
#[test]
fn a_record_cut_short_by_a_crash_opens_without_its_last_line() {
    let dir = record_dir("torn");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("operations.jsonl");
    std::fs::write(&file, "{\"Accepted\":{\"operation\":\"a\",\"input\":\"x\"}}\n{\"Accepted\":{\"opera").unwrap();
    let service = ExampleService::open(&dir, Duration::from_secs(1)).unwrap();
    assert_eq!(service.entries(0, 10).unwrap().0, 1);
    drop(service);
    assert!(std::fs::read_to_string(&file).unwrap().ends_with("}}\n"), "the cut line is removed from the file");
    std::fs::write(&file, "not json\n{\"Accepted\":{\"operation\":\"a\",\"input\":\"x\"}}\n").unwrap();
    let refused = ExampleService::open(&dir, Duration::from_secs(1)).err().expect("a line that does not decode refuses the open");
    assert!(refused.to_string().contains("line 1"), "{refused}");
}

/// CONTRACT: the ops' ledger rows are the product's, live, on the ops the protocols declare.
#[test]
fn the_ledger_rows_reserve_both_ops_for_the_product() {
    use rdm_core::protocol::NodeProtocol;
    let rows = rshape_consumer::consumer_example::ledger_rows();
    assert_eq!(rows.iter().map(|r| r.op).collect::<Vec<_>>(), vec![ExampleWork::OP, ExampleRecords::OP]);
    assert!(rows.iter().all(|r| r.owner == OpOwner::Product(OWNER.into())));
}
