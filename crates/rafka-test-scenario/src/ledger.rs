//! The RPC operation ledger and its reconciliation algebra (i143.e9.s1, PRD §15 "RPC ledger" and
//! "final reconciliation report").
//!
//! Every issued Node RPC operation is recorded once, then classified exactly once from its typed
//! [`RpcOutcome`]:
//!
//! ```text
//! issued = Reply + NotSent + Unserved + RejectedStale + Indeterminate
//! ```
//!
//! The bucket comes from the outcome itself, never from a caller's label: a call that crossed the
//! commit cut has no `NotSent` constructor, so a timeout after commit is `Indeterminate` and is
//! counted there. The ledger has no replay or re-issue method: an `Indeterminate` mutation may
//! have applied, and the reconciliation reads the final state to learn which, it never repeats
//! the call to balance a count.
//!
//! A mutation operation declares the `(key, value)` it intends to put. Reconciling against the
//! final state of a persistent store then checks that every successful mutation landed, that no
//! operation proven not dispatched landed, and that nothing landed that no operation explains.
//! Every refusal is a [`Violation`] naming the invariant, the operation id and the expected and
//! actual values.

use rafka_node_rpc_contract::outcome::{ReplyKind, RpcOutcome};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

/// The invariant every classification violation fails under.
pub const CLASSIFIES_EVERY_OPERATION_ONCE: &str = "operation_ledger_classifies_every_operation_once";
/// The invariant every mutation-reconciliation violation fails under.
pub const DETECTS_LOST_APPLIED_MUTATION: &str = "operation_reconciler_detects_lost_applied_mutation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct OperationId(pub u64);

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "op#{}", self.0)
    }
}

/// The five terminal buckets of `RpcOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Bucket {
    Reply,
    NotSent,
    Unserved,
    RejectedStale,
    Indeterminate,
}

impl Bucket {
    pub const ALL: [Bucket; 5] = [Bucket::Reply, Bucket::NotSent, Bucket::Unserved, Bucket::RejectedStale, Bucket::Indeterminate];

    pub fn of<R>(outcome: &RpcOutcome<R>) -> Self {
        match outcome {
            RpcOutcome::Reply(_) => Bucket::Reply,
            RpcOutcome::NotSent(_) => Bucket::NotSent,
            RpcOutcome::Unserved(_) => Bucket::Unserved,
            RpcOutcome::RejectedStale(_) => Bucket::RejectedStale,
            RpcOutcome::Indeterminate(_) => Bucket::Indeterminate,
        }
    }
}

/// The terminal classification of one operation: the bucket plus the typed detail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Classification {
    pub bucket: Bucket,
    /// The typed reason (`NotSentReason`, `IndeterminateReason`, the unserved op, the stale
    /// target) or, for a `Reply`, the `ReplyKind` class, rendered with `{:?}`.
    pub detail: String,
    /// Set for a `Reply` only: the reply was a success, as classified by its protocol.
    pub reply_success: bool,
}

impl Classification {
    pub fn of<R>(outcome: &RpcOutcome<R>) -> Self {
        let (detail, reply_success) = match outcome {
            RpcOutcome::Reply(r) => (format!("{:?}", r.class()), r.class() == ReplyKind::Success),
            RpcOutcome::NotSent(n) => (format!("{:?}", n.reason()), false),
            RpcOutcome::Unserved(u) => (format!("op {} unserved", u.op()), false),
            RpcOutcome::RejectedStale(s) => (format!("stale target {}", s.target_node_id()), false),
            RpcOutcome::Indeterminate(i) => (format!("{:?}", i.reason()), false),
        };
        Self { bucket: Bucket::of(outcome), detail, reply_success }
    }
}

impl Classification {
    /// The classification of an outcome as `rafka-rpc-probe` prints it (`{"outcome": ..}`): the
    /// probe is the typed `RpcOutcome` rendered to one JSON line, so the bucket is the outcome's
    /// own name and the detail is the same typed reason text [`Self::of`] renders. A `Reply` is a
    /// success when the proof store applied or answered the request; a compare-and-swap mismatch
    /// is a protocol success that applied nothing, so it is a `Reply` with `reply_success` false
    /// (the state algebra reads `reply_success` as "the mutation took effect"). A line that names
    /// no outcome is refused by name, never guessed into a bucket.
    pub fn of_probe(out: &Value) -> Result<Self, String> {
        let text = |v: &Value| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
        match out["outcome"].as_str() {
            Some("Reply") => {
                let result = &out["reply"]["result"];
                if let Some(refused) = result.get("refused") {
                    let r = text(refused);
                    let class = match r.as_str() {
                        "too-large" | "store-failed" => "ProtocolRefusal".to_string(),
                        other => other.chars().take_while(|c| c.is_alphanumeric()).collect(),
                    };
                    return Ok(Self { bucket: Bucket::Reply, detail: class, reply_success: false });
                }
                if result.get("swapped") == Some(&Value::Bool(false)) {
                    return Ok(Self { bucket: Bucket::Reply, detail: "Success:mismatch".into(), reply_success: false });
                }
                if ["stored", "deleted", "found", "swapped"].iter().any(|k| result.get(*k).is_some()) {
                    return Ok(Self { bucket: Bucket::Reply, detail: "Success".into(), reply_success: true });
                }
                Err(format!("a Reply whose result is none of stored, deleted, found, swapped or refused: {out}"))
            }
            Some("NotSent") => Ok(Self { bucket: Bucket::NotSent, detail: text(&out["reason"]), reply_success: false }),
            Some("Unserved") => Ok(Self { bucket: Bucket::Unserved, detail: text(&out["reason"]), reply_success: false }),
            Some("RejectedStale") => Ok(Self { bucket: Bucket::RejectedStale, detail: format!("stale target {}", text(&out["target_node_id"])), reply_success: false }),
            Some("Indeterminate") => Ok(Self { bucket: Bucket::Indeterminate, detail: text(&out["reason"]), reply_success: false }),
            other => Err(format!("the probe printed no typed RpcOutcome ({other:?}): {out}")),
        }
    }
}

/// What the operation intends to put in the persistent final state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MutationIntent {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Operation {
    pub id: OperationId,
    pub op: u8,
    pub target_node_id: String,
    pub mutation: Option<MutationIntent>,
    pub classification: Option<Classification>,
}

/// A refusal, by name, with the operation and the expected vs actual.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Violation {
    UnknownOperation { id: OperationId },
    AlreadyClassified { id: OperationId, first: Classification, second: Classification },
    Unclassified { id: OperationId, op: u8, target_node_id: String },
    BucketSumMismatch { issued: usize, classified: usize, buckets: BTreeMap<Bucket, usize> },
    LostAppliedMutation { id: OperationId, op: u8, key: String, expected: Vec<u8>, actual: Option<Vec<u8>>, outcome: Classification },
    AppliedDespiteNoDispatch { id: OperationId, op: u8, key: String, intended: Vec<u8>, actual: Vec<u8>, outcome: Classification },
    AppliedWithoutReceipt { key: String, actual: Vec<u8>, explained_by: Vec<OperationId> },
}

impl Violation {
    /// The named invariant this violation fails.
    pub fn invariant(&self) -> &'static str {
        match self {
            Violation::UnknownOperation { .. } | Violation::AlreadyClassified { .. } | Violation::Unclassified { .. } | Violation::BucketSumMismatch { .. } => {
                CLASSIFIES_EVERY_OPERATION_ONCE
            }
            _ => DETECTS_LOST_APPLIED_MUTATION,
        }
    }

    /// The operation the violation is about, when it is about one.
    pub fn operation(&self) -> Option<OperationId> {
        match self {
            Violation::UnknownOperation { id }
            | Violation::AlreadyClassified { id, .. }
            | Violation::Unclassified { id, .. }
            | Violation::LostAppliedMutation { id, .. }
            | Violation::AppliedDespiteNoDispatch { id, .. } => Some(*id),
            Violation::BucketSumMismatch { .. } | Violation::AppliedWithoutReceipt { .. } => None,
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.invariant())?;
        match self {
            Violation::UnknownOperation { id } => write!(f, "{id} was classified but never issued"),
            Violation::AlreadyClassified { id, first, second } => {
                write!(f, "{id} classified twice: first {:?} ({}), then {:?} ({})", first.bucket, first.detail, second.bucket, second.detail)
            }
            Violation::Unclassified { id, op, target_node_id } => {
                write!(f, "{id} (op 0x{op:02x} to {target_node_id}) was issued and never classified into any of Reply/NotSent/Unserved/RejectedStale/Indeterminate")
            }
            Violation::BucketSumMismatch { issued, classified, buckets } => {
                write!(f, "issued {issued} but the buckets sum to {classified} ({buckets:?})")
            }
            Violation::LostAppliedMutation { id, op, key, expected, actual, outcome } => write!(
                f,
                "{id} (op 0x{op:02x}) ended {:?} ({}) as an applied mutation of key {key:?}; expected final value {expected:?}, found {actual:?}",
                outcome.bucket, outcome.detail
            ),
            Violation::AppliedDespiteNoDispatch { id, op, key, intended, actual, outcome } => write!(
                f,
                "{id} (op 0x{op:02x}) ended {:?} ({}), which proves it was never dispatched, yet the final state holds key {key:?} = {actual:?} (its intended value {intended:?})",
                outcome.bucket, outcome.detail
            ),
            Violation::AppliedWithoutReceipt { key, actual, explained_by } => {
                write!(f, "final state holds key {key:?} = {actual:?} that no operation's possible effect explains (operations on that key: {explained_by:?})")
            }
        }
    }
}

/// Every violation found, never just the first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Refusal(pub Vec<Violation>);

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{v}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Refusal {}

/// The totals of a ledger that reconciled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Reconciliation {
    pub issued: usize,
    pub buckets: BTreeMap<Bucket, usize>,
    /// Mutations that ended Reply(success): all found in the final state.
    pub applied_mutations: usize,
    /// Mutations that ended Indeterminate, with whether the final state shows them applied.
    pub indeterminate_mutations: Vec<(OperationId, bool)>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Ledger {
    ops: Vec<Operation>,
    /// Violations met while recording (a double classification, an unknown id): kept so a
    /// reconciliation can never report a clean ledger over them.
    recorded: Vec<Violation>,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an issued operation. `mutation` is `Some` for an operation that puts a value.
    pub fn issue(&mut self, op: u8, target_node_id: impl Into<String>, mutation: Option<MutationIntent>) -> OperationId {
        let id = OperationId(self.ops.len() as u64);
        self.ops.push(Operation { id, op, target_node_id: target_node_id.into(), mutation, classification: None });
        id
    }

    /// Classify an issued operation from its outcome, once. A second classification, or one for
    /// an operation never issued, is refused by name and also kept for [`Self::reconcile`].
    pub fn classify<R>(&mut self, id: OperationId, outcome: &RpcOutcome<R>) -> Result<(), Violation> {
        self.classify_as(id, Classification::of(outcome))
    }

    /// Classify an issued operation from the outcome line `rafka-rpc-probe` printed
    /// ([`Classification::of_probe`]), once. A line that is not a typed outcome leaves the
    /// operation unclassified, which [`Self::reconcile`] names.
    pub fn classify_probe(&mut self, id: OperationId, out: &Value) -> Result<(), String> {
        let class = Classification::of_probe(out)?;
        self.classify_as(id, class).map_err(|v| v.to_string())
    }

    fn classify_as(&mut self, id: OperationId, class: Classification) -> Result<(), Violation> {
        let violation = match self.ops.get_mut(id.0 as usize) {
            None => Violation::UnknownOperation { id },
            Some(o) => match &o.classification {
                None => {
                    o.classification = Some(class);
                    return Ok(());
                }
                Some(first) => Violation::AlreadyClassified { id, first: first.clone(), second: class },
            },
        };
        self.recorded.push(violation.clone());
        Err(violation)
    }

    pub fn operations(&self) -> &[Operation] {
        &self.ops
    }

    /// The operations `keep` selects, with their original ids, as a ledger of their own: the
    /// state algebra of one store (one node birth) reads only the operations that reached it.
    /// Violations met while recording stay with the whole ledger's [`Self::reconcile`].
    pub fn select(&self, keep: impl Fn(&Operation) -> bool) -> Ledger {
        Ledger { ops: self.ops.iter().filter(|o| keep(o)).cloned().collect(), recorded: Vec::new() }
    }

    pub fn issued(&self) -> usize {
        self.ops.len()
    }

    pub fn buckets(&self) -> BTreeMap<Bucket, usize> {
        let mut m: BTreeMap<Bucket, usize> = Bucket::ALL.iter().map(|b| (*b, 0)).collect();
        for o in &self.ops {
            if let Some(c) = &o.classification {
                *m.entry(c.bucket).or_default() += 1;
            }
        }
        m
    }

    /// The count algebra: every issued operation classified exactly once and
    /// `issued = Reply + NotSent + Unserved + RejectedStale + Indeterminate`.
    pub fn reconcile(&self) -> Result<Reconciliation, Refusal> {
        let mut found = self.recorded.clone();
        for o in &self.ops {
            if o.classification.is_none() {
                found.push(Violation::Unclassified { id: o.id, op: o.op, target_node_id: o.target_node_id.clone() });
            }
        }
        let buckets = self.buckets();
        let classified: usize = buckets.values().sum();
        if classified != self.ops.len() {
            found.push(Violation::BucketSumMismatch { issued: self.ops.len(), classified, buckets: buckets.clone() });
        }
        if !found.is_empty() {
            return Err(Refusal(found));
        }
        Ok(Reconciliation { issued: self.ops.len(), buckets, applied_mutations: 0, indeterminate_mutations: Vec::new() })
    }

    /// The count algebra plus the state algebra against `final_state` (key to value, read from
    /// the persistent store after the run):
    ///
    /// - a mutation that ended `Reply` success holds its intended value as the final value of its
    ///   key, unless a later success on that key legitimately replaced it;
    /// - a mutation that ended NotSent, Unserved, RejectedStale or a non-success Reply never
    ///   dispatched or was refused: its value is not the final value of its key unless another
    ///   operation that may have applied the same value explains it;
    /// - an `Indeterminate` mutation may or may not have applied: both are accepted, and which one
    ///   happened is reported, never repaired;
    /// - a key in the final state that no Reply-success or Indeterminate mutation could have
    ///   written is `AppliedWithoutReceipt`.
    pub fn reconcile_state(&self, final_state: &BTreeMap<String, Vec<u8>>) -> Result<Reconciliation, Refusal> {
        let mut found = match self.reconcile() {
            Ok(_) => Vec::new(),
            Err(Refusal(v)) => v,
        };
        // Per key: the operations whose effect could be the final value.
        let mut may_explain: BTreeMap<&str, Vec<&Operation>> = BTreeMap::new();
        for o in &self.ops {
            if let (Some(m), Some(c)) = (&o.mutation, &o.classification) {
                if (c.bucket == Bucket::Reply && c.reply_success) || c.bucket == Bucket::Indeterminate {
                    may_explain.entry(m.key.as_str()).or_default().push(o);
                }
            }
        }
        let mut applied = 0;
        let mut indeterminate = Vec::new();
        for o in &self.ops {
            let (Some(m), Some(c)) = (&o.mutation, &o.classification) else { continue };
            let actual = final_state.get(&m.key);
            match c.bucket {
                Bucket::Reply if c.reply_success => {
                    // Overwritten by a later operation on the same key that may have applied: fine.
                    let superseded = may_explain.get(m.key.as_str()).is_some_and(|ops| ops.iter().any(|p| p.id > o.id));
                    if actual == Some(&m.value) || (superseded && actual.is_some()) {
                        applied += 1;
                    } else {
                        found.push(Violation::LostAppliedMutation {
                            id: o.id,
                            op: o.op,
                            key: m.key.clone(),
                            expected: m.value.clone(),
                            actual: actual.cloned(),
                            outcome: c.clone(),
                        });
                    }
                }
                Bucket::Indeterminate => indeterminate.push((o.id, actual == Some(&m.value))),
                _ => {
                    let explained = may_explain.get(m.key.as_str()).is_some_and(|ops| ops.iter().any(|p| p.mutation.as_ref().map(|x| &x.value) == Some(&m.value)));
                    if let Some(a) = actual {
                        if a == &m.value && !explained {
                            found.push(Violation::AppliedDespiteNoDispatch {
                                id: o.id,
                                op: o.op,
                                key: m.key.clone(),
                                intended: m.value.clone(),
                                actual: a.clone(),
                                outcome: c.clone(),
                            });
                        }
                    }
                }
            }
        }
        for (key, value) in final_state {
            let explainers = may_explain.get(key.as_str());
            if !explainers.is_some_and(|ops| ops.iter().any(|o| o.mutation.as_ref().map(|m| &m.value) == Some(value))) {
                let on_key = self.ops.iter().filter(|o| o.mutation.as_ref().is_some_and(|m| &m.key == key)).map(|o| o.id).collect();
                // A value only a no-dispatch operation intended was already named above.
                let named = found.iter().any(|v| matches!(v, Violation::AppliedDespiteNoDispatch { key: k, .. } if k == key));
                if !named {
                    found.push(Violation::AppliedWithoutReceipt { key: key.clone(), actual: value.clone(), explained_by: on_key });
                }
            }
        }
        if !found.is_empty() {
            return Err(Refusal(found));
        }
        Ok(Reconciliation { issued: self.ops.len(), buckets: self.buckets(), applied_mutations: applied, indeterminate_mutations: indeterminate })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn probe_lines_classify_into_the_five_buckets() {
        let c = |v: Value| Classification::of_probe(&v).unwrap();
        assert_eq!(c(json!({"outcome": "Reply", "reply": {"result": {"stored": true}}})).reply_success, true);
        let mismatch = c(json!({"outcome": "Reply", "reply": {"result": {"swapped": false, "current": null}}}));
        assert_eq!((mismatch.bucket, mismatch.reply_success), (Bucket::Reply, false));
        let draining = c(json!({"outcome": "Reply", "reply": {"result": {"refused": "Draining { reason: \"x\" }"}}}));
        assert_eq!((draining.bucket, draining.detail.as_str(), draining.reply_success), (Bucket::Reply, "Draining", false));
        assert_eq!(c(json!({"outcome": "NotSent", "reason": "Deadline"})).bucket, Bucket::NotSent);
        assert_eq!(c(json!({"outcome": "Unserved", "reason": "x"})).bucket, Bucket::Unserved);
        assert_eq!(c(json!({"outcome": "RejectedStale", "target_node_id": "n"})).bucket, Bucket::RejectedStale);
        assert_eq!(c(json!({"outcome": "Indeterminate", "reason": "ReplyDeadline"})).detail, "ReplyDeadline");
        assert!(Classification::of_probe(&json!({"outcome": "Refused", "reason": "x"})).is_err());
    }

    #[test]
    fn a_probe_line_that_is_not_typed_leaves_the_operation_unclassified() {
        let mut l = Ledger::new();
        let a = l.issue(0x70, "n1", Some(MutationIntent { key: "1".into(), value: b"v".to_vec() }));
        let b = l.issue(0x70, "n2", None);
        l.classify_probe(a, &json!({"outcome": "Reply", "reply": {"result": {"stored": true}}})).unwrap();
        assert!(l.classify_probe(b, &json!({"outcome": "Refused"})).is_err());
        let refusal = l.reconcile().unwrap_err();
        assert!(refusal.0.iter().any(|v| matches!(v, Violation::Unclassified { id, .. } if *id == b)), "{refusal}");
        // One store's state algebra: its own operations against its own final state.
        let n1 = l.select(|o| o.target_node_id == "n1");
        assert!(n1.reconcile_state(&BTreeMap::from([("1".to_string(), b"v".to_vec())])).is_ok());
        assert!(n1.reconcile_state(&BTreeMap::new()).is_err(), "a stored put that is not in the final state is lost");
    }
}
