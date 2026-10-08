//! i143.e9.s1 acceptance (rafka-v2 #2786, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2786-unit`, which exports `I143_ACCEPTANCE_DIR`; each
//! cell leaves its `result.json` (direct observations) there. A pure model/algebra cell: no
//! estate, no spans.
//!
//! Every outcome is built through the contract's own proving-condition API (`PreCommit`,
//! `RequestFinished`, `Committed`), so no variant is invented here.

use rafka_node_rpc_contract::framing::Fence;
use rafka_node_rpc_contract::outcome::{Committed, IndeterminateReason, NotSentReason, PreCommit, RequestFinished, ResolveFailure, RpcOutcome};
use rafka_node_rpc_contract::ping::{Ping, PingReply};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_test_scenario::ledger::{Bucket, Ledger, MutationIntent, OperationId, Violation, CLASSIFIES_EVERY_OPERATION_ONCE, DETECTS_LOST_APPLIED_MUTATION};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2786/unit").join(cell),
    }
}

fn write_result(cell: &str, v: &Value) {
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(v).unwrap()).unwrap();
}

/// SplitMix64: the seed is the whole run.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

const SEED: u64 = 0x2786_0001;
const OPS: usize = 400;

fn committed() -> Committed {
    PreCommit::begin(Ping::OP).commit(RequestFinished::after_clean_finish(8, 8, true).unwrap())
}

fn fence() -> Fence {
    Fence { target_node_id: "node-target".into(), op: Ping::OP }
}

/// One outcome of each shape, chosen by `pick`; `success` is whether a Reply is a success.
fn outcome(pick: u64) -> RpcOutcome<PingReply> {
    let pre = || PreCommit::begin(Ping::OP);
    match pick % 11 {
        0 => committed().reply::<Ping>(&Ping::encode_reply(&PingReply::Pong { payload: vec![1] }).unwrap()),
        1 => committed().reply::<Ping>(&Ping::encode_reply(&PingReply::Busy { reason: "busy".into() }).unwrap()),
        2 => pre().not_sent(NotSentReason::Deadline),
        3 => pre().not_sent(NotSentReason::Resolve(ResolveFailure::Gone)),
        4 => pre().cut_before_finish().1,
        5 => pre().unserved_before_finish(),
        6 => pre().stale_before_finish(&fence()),
        7 => committed().reset(425, &fence()),
        8 => committed().indeterminate(IndeterminateReason::ReplyDeadline),
        9 => committed().indeterminate(IndeterminateReason::ReplyLost("stream lost".into())),
        _ => committed().reset(7, &fence()),
    }
}

struct Run {
    ledger: Ledger,
    outcomes: Vec<RpcOutcome<PingReply>>,
}

/// A seeded run: `OPS` operations, about half of them mutations of one of eight keys.
fn seeded_run(seed: u64) -> Run {
    let mut rng = Rng(seed);
    let mut ledger = Ledger::new();
    let mut outcomes = Vec::new();
    for i in 0..OPS {
        let mutation = (rng.next() % 2 == 0).then(|| MutationIntent { key: format!("k{}", rng.next() % 8), value: format!("v{i}").into_bytes() });
        let id = ledger.issue(Ping::OP, format!("node-{}", rng.next() % 4), mutation);
        let o = outcome(rng.next());
        ledger.classify(id, &o).expect("a first classification is accepted");
        outcomes.push(o);
    }
    Run { ledger, outcomes }
}

/// The persistent final state the seeded run leaves, under `coin` for the Indeterminate ones: an
/// operation applies exactly when it replied success, or was Indeterminate and the coin says so.
/// Later operations overwrite earlier ones on the same key.
fn final_state(run: &Run, rng: &mut Rng) -> (BTreeMap<String, Vec<u8>>, Vec<OperationId>) {
    let mut state = BTreeMap::new();
    let mut applied_indeterminate = Vec::new();
    for (o, out) in run.ledger.operations().iter().zip(&run.outcomes) {
        let Some(m) = &o.mutation else { continue };
        let applies = match out {
            RpcOutcome::Reply(r) => r.class() == rafka_node_rpc_contract::outcome::ReplyKind::Success,
            RpcOutcome::Indeterminate(_) => {
                let a = rng.next() % 2 == 0;
                if a {
                    applied_indeterminate.push(o.id);
                }
                a
            }
            _ => false,
        };
        if applies {
            state.insert(m.key.clone(), m.value.clone());
        }
    }
    (state, applied_indeterminate)
}

/// CONTRACT (#2786): on a seeded run of 400 operations, every issued operation ends in exactly
/// one of Reply, NotSent, Unserved, RejectedStale or Indeterminate, chosen by its typed outcome,
/// and `issued` equals the sum of the five buckets. A timeout after the commit cut is counted
/// Indeterminate, never NotSent; a typed refusal reply is a Reply. What must NOT happen: an
/// operation counted twice or never, a classification of an operation never issued, or a count
/// that balances over a violation; each is refused by name with the operation id.
#[test]
fn operation_ledger_classifies_every_operation_once() {
    let cell = "operation_ledger_classifies_every_operation_once";
    let run = seeded_run(SEED);
    let rec = run.ledger.reconcile().unwrap_or_else(|r| panic!("{cell}: the clean seeded run was refused:\n{r}"));
    assert_eq!(rec.issued, OPS);
    let sum: usize = rec.buckets.values().sum();
    assert_eq!(sum, OPS, "issued = Reply + NotSent + Unserved + RejectedStale + Indeterminate: {:?}", rec.buckets);
    for b in Bucket::ALL {
        assert!(rec.buckets[&b] > 0, "the seeded run exercises {b:?}: {:?}", rec.buckets);
    }
    // Each operation's bucket is the one its outcome's own name says.
    for (o, out) in run.ledger.operations().iter().zip(&run.outcomes) {
        let c = o.classification.as_ref().unwrap();
        assert_eq!(format!("{:?}", c.bucket), out.name(), "{}", o.id);
    }
    // A timeout after the commit cut is Indeterminate with its typed reason, not NotSent.
    let mut one = Ledger::new();
    let id = one.issue(Ping::OP, "node-0", None);
    one.classify(id, &outcome(8)).unwrap();
    let c = one.operations()[0].classification.clone().unwrap();
    assert_eq!((c.bucket, c.detail.as_str()), (Bucket::Indeterminate, "ReplyDeadline"));
    assert_eq!(one.buckets()[&Bucket::NotSent], 0);

    // Planted: a double classification of op#7 (the first stands, the second is refused).
    let mut planted = seeded_run(SEED);
    let victim = OperationId(7);
    let refused = planted.ledger.classify(victim, &outcome(2)).expect_err("a second classification is refused");
    assert!(matches!(&refused, Violation::AlreadyClassified { id, .. } if *id == victim), "{refused}");
    let text = refused.to_string();
    assert!(text.starts_with(CLASSIFIES_EVERY_OPERATION_ONCE) && text.contains("op#7") && text.contains("classified twice"), "{text}");
    let after = planted.ledger.reconcile().expect_err("the count cannot balance over a double classification");
    assert!(after.0.iter().any(|v| v.operation() == Some(victim) && v.invariant() == CLASSIFIES_EVERY_OPERATION_ONCE), "{after}");

    // Planted: an operation issued and never classified; one classified and never issued.
    let mut missing = Ledger::new();
    let a = missing.issue(Ping::OP, "node-1", None);
    let b = missing.issue(Ping::OP, "node-2", None);
    missing.classify(a, &outcome(0)).unwrap();
    let ghost = OperationId(99);
    let unknown = missing.classify(ghost, &outcome(0)).expect_err("never issued");
    assert!(matches!(&unknown, Violation::UnknownOperation { id } if *id == ghost), "{unknown}");
    let refusal = missing.reconcile().expect_err("op#1 is unclassified");
    assert!(refusal.0.iter().any(|v| matches!(v, Violation::Unclassified { id, .. } if *id == b)), "{refusal}");
    assert!(refusal.to_string().contains("op#1"), "{refusal}");

    write_result(
        cell,
        &json!({
            "cell": cell, "seed": SEED, "issued": rec.issued, "buckets": rec.buckets,
            "post_commit_timeout": {"bucket": c.bucket, "detail": c.detail},
            "planted_double_classification": {"operation": "op#7", "refused": text, "reconcile": after.to_string()},
            "planted_unclassified": {"operation": "op#1", "refused": refusal.to_string()},
            "planted_unknown_operation": unknown.to_string(),
        }),
    );
}

/// CONTRACT (#2786): every successful mutation (a Reply success) is found in the persistent final
/// state; an Indeterminate mutation is accepted whether it applied or not and is reported, never
/// replayed to balance a count (the ledger has no replay); an operation proven not dispatched
/// (NotSent, Unserved, RejectedStale) or refused by a non-success reply leaves no effect. What must
/// NOT happen: a lost applied mutation, an applied mutation of an operation proven not
/// dispatched, or a value in the final state no operation explains; each fails
/// `operation_reconciler_detects_lost_applied_mutation` naming the operation id, outcome,
/// expected and actual.
#[test]
fn operation_reconciler_detects_lost_applied_mutation() {
    let cell = "operation_reconciler_detects_lost_applied_mutation";
    let run = seeded_run(SEED);
    let mut rng = Rng(SEED ^ 0xFFFF);
    let (state, applied_ind) = final_state(&run, &mut rng);
    let rec = run.ledger.reconcile_state(&state).unwrap_or_else(|r| panic!("{cell}: the clean ledger and state were refused:\n{r}"));
    assert!(rec.applied_mutations > 0, "the run applies mutations");
    assert!(rec.indeterminate_mutations.iter().any(|(_, a)| *a) && rec.indeterminate_mutations.iter().any(|(_, a)| !*a), "both indeterminate fates occur: {:?}", rec.indeterminate_mutations);
    let reported: Vec<OperationId> = rec.indeterminate_mutations.iter().filter(|(_, a)| *a).map(|(i, _)| *i).collect();
    // A reported "applied" indeterminate may have been overwritten later, so it is a subset check.
    assert!(reported.iter().all(|i| applied_ind.contains(i)), "reported {reported:?} within applied {applied_ind:?}");

    // Planted LOST: an acknowledged mutation whose key is missing from the final state.
    let last_success = run
        .ledger
        .operations()
        .iter()
        .rev()
        .find(|o| o.mutation.is_some() && o.classification.as_ref().is_some_and(|c| c.bucket == Bucket::Reply && c.reply_success))
        .unwrap();
    let key = last_success.mutation.as_ref().unwrap().key.clone();
    let mut lost = state.clone();
    lost.remove(&key);
    let refusal = run.ledger.reconcile_state(&lost).expect_err("a lost acknowledged mutation is refused");
    let v = refusal.0.iter().find(|v| v.operation() == Some(last_success.id)).expect("the lost operation is named");
    assert!(matches!(v, Violation::LostAppliedMutation { actual: None, .. }), "{v}");
    let lost_text = v.to_string();
    assert!(lost_text.starts_with(DETECTS_LOST_APPLIED_MUTATION) && lost_text.contains(&last_success.id.to_string()) && lost_text.contains(&key), "{lost_text}");

    // Planted DOUBLED/phantom: an operation proven not dispatched whose value is in the state.
    let not_dispatched = run
        .ledger
        .operations()
        .iter()
        .find(|o| o.mutation.is_some() && o.classification.as_ref().is_some_and(|c| matches!(c.bucket, Bucket::NotSent | Bucket::Unserved | Bucket::RejectedStale)))
        .unwrap();
    let m = not_dispatched.mutation.as_ref().unwrap();
    let mut phantom = state.clone();
    phantom.insert(m.key.clone(), m.value.clone());
    let refusal = run.ledger.reconcile_state(&phantom).expect_err("an effect of a proven-not-dispatched operation is refused");
    let v = refusal.0.iter().find(|v| v.operation() == Some(not_dispatched.id)).expect("the phantom operation is named");
    assert!(matches!(v, Violation::AppliedDespiteNoDispatch { .. }), "{v}");
    let phantom_text = v.to_string();
    assert!(phantom_text.starts_with(DETECTS_LOST_APPLIED_MUTATION) && phantom_text.contains(&not_dispatched.id.to_string()), "{phantom_text}");

    // Planted unexplained value.
    let mut stray = state.clone();
    stray.insert("k0".into(), b"nobody-wrote-this".to_vec());
    let refusal = run.ledger.reconcile_state(&stray).expect_err("a value no operation explains is refused");
    assert!(refusal.0.iter().any(|v| matches!(v, Violation::AppliedWithoutReceipt { key, .. } if key == "k0")), "{refusal}");

    // An Indeterminate mutation passes whether it applied or not (no replay needed to balance).
    let mut l = Ledger::new();
    let id = l.issue(Ping::OP, "node-0", Some(MutationIntent { key: "x".into(), value: b"1".to_vec() }));
    l.classify(id, &outcome(8)).unwrap();
    assert!(l.reconcile_state(&BTreeMap::new()).is_ok());
    assert!(l.reconcile_state(&BTreeMap::from([("x".to_string(), b"1".to_vec())])).is_ok());

    write_result(
        cell,
        &json!({
            "cell": cell, "seed": SEED, "issued": rec.issued, "applied_mutations": rec.applied_mutations,
            "indeterminate_mutations": rec.indeterminate_mutations.len(),
            "planted_lost": {"operation": last_success.id, "key": key, "refused": lost_text},
            "planted_phantom": {"operation": not_dispatched.id, "refused": phantom_text},
            "planted_stray": refusal.to_string(),
        }),
    );
}
