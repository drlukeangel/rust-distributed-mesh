//! i143.e8.s1 acceptance (rafka-v2 #2779), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2779-unit`, which exports `I143_ACCEPTANCE_DIR` (each
//! cell's `result.json` goes there). Pure model cells: their evidence is the model's own result
//! (seed, sequences, failures), not spans.

use rafka_test_scenario::model::{generate, observe, replay, shrink, Action, Capabilities, ClassBounds, Failure, Model, NodeClass};
use rafka_test_scenario::scenario::Operation;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const SEED: u64 = 1_432_779;
const LEN: usize = 240;

fn acceptance_dir(cell: &str) -> PathBuf {
    let dir = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2779/unit").join(cell),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Two meshes of 2 node-admins and 2 rpc nodes; a mesh holds 1..=3 admins and 1..=4 rpc nodes.
fn initial(network_faults: bool) -> Model {
    let bounds = BTreeMap::from([(NodeClass::NodeAdmin, ClassBounds { min: 1, max: 3 }), (NodeClass::RpcNode, ClassBounds { min: 1, max: 4 })]);
    Model::new(&[("mesh1", 2, 2), ("mesh2", 2, 2)], bounds, Capabilities { network_faults })
}

fn kind(a: &Action) -> &'static str {
    match a {
        Action::Grow { .. } => "grow",
        Action::Shrink { .. } => "shrink",
        Action::Restart { .. } => "restart",
        Action::Replace { .. } => "replace",
        Action::HandOff { .. } => "hand-off",
        Action::Unheard { .. } => "unheard",
        Action::Heal { .. } => "heal",
        Action::Proof(Operation::Put { .. }) => "proof-put",
        Action::Proof(Operation::Cas { .. }) => "proof-cas",
        Action::Proof(Operation::Delete { .. }) => "proof-delete",
    }
}

/// The model's own invariants after every row: each class within its bounds, every mesh keeps a
/// node-admin, at least one mesh heard.
fn bounded(cell: &str) -> impl FnMut(usize, &Action, &Model) -> Result<(), Failure> + '_ {
    move |step, a, m| {
        for (mesh, ms) in &m.meshes {
            for (class, b) in &m.bounds {
                let n = m.count(mesh, *class);
                if n < b.min || n > b.max {
                    return Err(Failure { rule: cell.into(), step, action: a.clone(), detail: format!("{mesh} holds {n} {class:?}, bounds {}..={}", b.min, b.max) });
                }
            }
            if !ms.nodes.values().any(|n| n.class == NodeClass::NodeAdmin) {
                return Err(Failure { rule: cell.into(), step, action: a.clone(), detail: format!("{mesh} has no node-admin") });
            }
        }
        if m.meshes.values().all(|ms| ms.unheard) {
            return Err(Failure { rule: cell.into(), step, action: a.clone(), detail: "every mesh is unheard".into() });
        }
        Ok(())
    }
}

/// The first row where two sequences differ, named.
fn same_sequence(cell: &str, a: &[Action], b: &[Action]) -> Result<(), Failure> {
    if let Some(i) = (0..a.len().max(b.len())).find(|&i| a.get(i) != b.get(i)) {
        let action = a.get(i).or(b.get(i)).cloned().expect("a row of one of them");
        return Err(Failure { rule: cell.into(), step: i, action, detail: format!("replay diverges: {:?} vs {:?}", a.get(i).map(|x| x.to_string()), b.get(i).map(|x| x.to_string())) });
    }
    Ok(())
}

/// CONTRACT (#2779): the same explicit seed and the same initial model generate the identical
/// ordered sequence of actions, and every action is legal in the state it is applied to (its
/// preconditions are re-checked after every row, and the model's bounds hold after every row).
/// The generator works on node classes and their capabilities only: a provider without network
/// faults never gets a network action. What must NOT happen: a replay that differs from the
/// original at any row, or an action whose precondition does not hold where it lands. Both are
/// planted and must be refused by this cell's name, with the row and the action.
#[test]
fn action_generator_replays_seed_preserves_legal_sequence() {
    let cell = "action_generator_replays_seed_preserves_legal_sequence";
    let dir = acceptance_dir(cell);
    let model = initial(true);
    let first = generate(SEED, &model, LEN);
    let again = generate(SEED, &model, LEN);
    assert_eq!(first.len(), LEN, "the generator never ran out of legal actions");
    same_sequence(cell, &first, &again).unwrap_or_else(|f| panic!("{f}"));
    let end = observe(&model, &first, bounded(cell)).unwrap_or_else(|f| panic!("{f}"));
    let kinds: BTreeMap<&str, usize> = first.iter().fold(BTreeMap::new(), |mut m, a| {
        *m.entry(kind(a)).or_default() += 1;
        m
    });
    let every = ["grow", "shrink", "restart", "replace", "hand-off", "unheard", "heal", "proof-put", "proof-cas", "proof-delete"];
    let missing: Vec<&&str> = every.iter().filter(|k| !kinds.contains_key(**k)).collect();
    assert!(missing.is_empty(), "seed {SEED}: every action kind is generated; missing {missing:?} of {kinds:?}");

    // A provider with no network faults: the same legality, and no network action at all.
    let process = initial(false);
    let p = generate(SEED, &process, LEN);
    observe(&process, &p, bounded(cell)).unwrap_or_else(|f| panic!("{f}"));
    let network: Vec<&Action> = p.iter().filter(|a| matches!(a, Action::Unheard { .. } | Action::Heal { .. })).collect();
    assert!(network.is_empty(), "no network action without the capability: {network:?}");

    // Planted 1: a replay from another seed must be refused at its first divergent row.
    let other = generate(SEED + 1, &model, LEN);
    let divergence = same_sequence(cell, &first, &other).expect_err("another seed's sequence is not a replay");
    assert_eq!(divergence.rule, cell);

    // Planted 2: right after the first Shrink, restart the node it removed. That row's
    // precondition fails on the state the earlier rows produced.
    let at = first.iter().position(|a| matches!(a, Action::Shrink { .. })).expect("the sequence shrinks a node");
    let Action::Shrink { node } = &first[at] else { unreachable!() };
    let mut planted = first.clone();
    planted.insert(at + 1, Action::Restart { node: node.clone() });
    let refused = replay(&model, &planted).expect_err("a restart of a removed node is illegal");
    let refusal = Failure { rule: cell.into(), step: refused.step, action: refused.action.clone(), detail: refused.to_string() };
    assert_eq!(refusal.step, at + 1, "refused at the planted row: {refusal}");
    assert_eq!(refusal.action, Action::Restart { node: node.clone() });
    assert_eq!(refused.precondition, "node-exists", "{refusal}");

    // Clean candidate: the unplanted sequence replays to the same end state.
    assert_eq!(replay(&model, &first).expect("the clean sequence is legal"), end);

    let result = json!({
        "cell": cell,
        "seed": SEED,
        "len": first.len(),
        "initial": model,
        "kinds": kinds,
        "sequence": first,
        "replay_identical": true,
        "process_provider": {"len": p.len(), "network_actions": network.len()},
        "end_state": end,
        "planted": [
            {"violation": "replay from seed+1", "refused": divergence.to_string()},
            {"violation": "restart of a node shrunk at the row before", "refused": refusal.to_string(), "precondition": refused.precondition},
        ],
        "clean_pass": true,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// The planted rule of the shrinker cell: a proof key written to an rpc node of mesh2 that this
/// sequence grew, and that node then restarted.
fn grown_written_restarted(rule: &str) -> impl Fn(&Model, &[Action]) -> Result<(), Failure> + '_ {
    move |initial, actions| {
        let mut prev: BTreeSet<String> = initial.meshes["mesh2"].nodes.keys().cloned().collect();
        let mut grown: BTreeSet<String> = BTreeSet::new();
        let mut written: BTreeSet<String> = BTreeSet::new();
        observe(initial, actions, |step, a, m| {
            let now: BTreeSet<String> = m.meshes["mesh2"].nodes.keys().cloned().collect();
            if let Action::Grow { mesh, class: NodeClass::RpcNode } = a {
                if mesh == "mesh2" {
                    grown.extend(now.difference(&prev).cloned());
                }
            }
            for gone in prev.difference(&now) {
                grown.remove(gone);
                written.remove(gone);
            }
            if let Action::Replace { node } = a {
                grown.remove(node);
                written.remove(node);
            }
            prev = now;
            match a {
                Action::Proof(Operation::Put { target, .. }) if grown.contains(target) => {
                    written.insert(target.clone());
                }
                Action::Restart { node } if written.contains(node) => {
                    return Err(Failure { rule: rule.into(), step, action: a.clone(), detail: format!("{node} was grown by this sequence, written, then restarted") });
                }
                _ => {}
            }
            Ok(())
        })
        .map(|_| ())
    }
}

/// CONTRACT (#2779): a sequence that breaks a named invariant shrinks to a minimal sequence that
/// breaks the SAME rule, every action of it still legal from the initial model, and 1-minimal under
/// the stated strategy (removing any one remaining action makes it illegal or stops it reproducing);
/// replaying the minimal sequence fails identically; a property that holds is never shrunk. What
/// must NOT happen: a minimized sequence that fails by a different rule, contains an illegal
/// action, or still reproduces with one of its actions removed.
#[test]
fn action_shrinker_preserves_failure_returns_minimal_legal_sequence() {
    let cell = "action_shrinker_preserves_failure_returns_minimal_legal_sequence";
    let dir = acceptance_dir(cell);
    let model = initial(true);
    let property = grown_written_restarted(cell);
    // The first seed from SEED whose sequence breaks the planted rule (deterministic).
    let (seed, original) = (SEED..SEED + 2_000)
        .map(|s| (s, generate(s, &model, LEN)))
        .find(|(_, seq)| property(&model, seq).is_err())
        .expect("a seed in 2000 breaks the planted rule");
    let original_failure = property(&model, &original).expect_err("the original fails");
    assert_eq!(original_failure.rule, cell);

    let shrunk = shrink(&model, &original, &property).expect("a failing sequence shrinks");
    assert_eq!(shrunk.failure.rule, cell, "the same rule fails: {}", shrunk.failure);
    assert!(shrunk.minimized.len() < original.len(), "{} -> {}", original.len(), shrunk.minimized.len());
    replay(&model, &shrunk.minimized).unwrap_or_else(|e| panic!("every minimized action is legal: {e}"));
    let replayed = property(&model, &shrunk.minimized).expect_err("the minimized sequence reproduces");
    assert_eq!(replayed, shrunk.failure, "replaying the minimized sequence fails identically");

    // Minimal under the strategy: removing any one action, or any two adjacent ones, makes the
    // sequence illegal or stops the rule failing.
    let mut removals = Vec::new();
    for width in [1usize, 2] {
        for i in 0..=shrunk.minimized.len().saturating_sub(width) {
            let mut cand = shrunk.minimized.clone();
            let removed: Vec<Action> = cand.drain(i..i + width).collect();
            let why = match replay(&model, &cand) {
                Err(e) => format!("illegal: {e}"),
                Ok(_) => match property(&model, &cand) {
                    Err(f) if f.rule == cell => panic!("removing rows {i}..{} {removed:?} still reproduces: {f}", i + width),
                    Err(f) => format!("fails by another rule: {f}"),
                    Ok(()) => "no longer reproduces".into(),
                },
            };
            removals.push(json!({"rows": [i, i + width], "removed": removed, "without_them": why}));
        }
    }
    // The story the rule names survives: a grow of a mesh2 rpc node, a write to it, its restart last.
    let kinds: Vec<&str> = shrunk.minimized.iter().map(kind).collect();
    assert!(kinds.contains(&"grow") && kinds.contains(&"proof-put") && kinds.last() == Some(&"restart"), "{:?}", shrunk.minimized);
    assert_eq!(shrunk.failure.step, shrunk.minimized.len() - 1, "the rule breaks at the last row: {}", shrunk.failure);

    // A property that holds is never shrunk.
    assert!(shrink(&model, &original, |_: &Model, _: &[Action]| Ok(())).is_none(), "a clean property passes and shrinks nothing");

    let result = json!({
        "cell": cell,
        "seed": seed,
        "strategy": "windows of halving size (n/2 .. 1) slid one row at a time; keep a removal that is legal from the initial model and fails by the same rule; whole passes until one removes nothing",
        "original": {"len": original.len(), "failure": original_failure},
        "minimized": shrunk.minimized,
        "failure": shrunk.failure,
        "candidates_tried": shrunk.candidates_tried,
        "minimality": removals,
        "clean_property_shrinks": false,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
