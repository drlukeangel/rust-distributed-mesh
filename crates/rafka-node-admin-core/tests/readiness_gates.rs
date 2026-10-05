//! i143.e3.s2 functional: readiness gates run through the transition pipeline.
//!
//! One rule gates SN/MN and MM (desired topology); one genuinely
//! shape-conditional hook runs for MM only; the Fabric commits ReadyForTraffic
//! only after every desired mesh is ready and the blocking pre-ready hooks ran.

use async_trait::async_trait;
use rafka_node_admin_core::build::{FabricDesired, MeshDesired};
use rafka_node_admin_core::lifecycle::*;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::readiness::fabric_ready_eligibility;
use rafka_node_admin_core::topology::Topology;
use std::sync::{Arc, Mutex};

const READY: TransitionKey = TransitionKey { scope: LifecycleScope::Fabric, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };

struct Log(&'static str, Arc<Mutex<Vec<&'static str>>>);

#[async_trait]
impl LifecycleHook for Log {
    async fn run(&self, _: &HookContext) -> Result<(), String> {
        self.1.lock().unwrap().push(self.0);
        Ok(())
    }
}

fn desired(meshes: &[&str]) -> FabricDesired {
    FabricDesired {
        fabric: "fabric1".into(),
        meshes: meshes.iter().map(|m| MeshDesired { name: (*m).into(), node_admin: 2, rpc_node: 3 }).collect(),
    }
}

fn ready_topology(meshes: &[&str]) -> Topology {
    let mut nodes = Vec::new();
    for m in meshes {
        for (k, n) in [("admin", 2), ("rpc", 3)] {
            for i in 1..=n {
                let mut node = Node::allocated(format!("{m}.{k}.{i}").parse().unwrap());
                node.status = NodeStatus::ReadyForTraffic;
                nodes.push(node);
            }
        }
    }
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: ProviderKind::Process },
        meshes: meshes.iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
        nodes,
    }
}

fn pipeline(log: &Arc<Mutex<Vec<&'static str>>>) -> LifecycleTransitionPipeline {
    let spec = |id: &str, pred: ShapePredicate| LifecycleHookSpec {
        hook_id: id.into(),
        transition: READY,
        phase: HookPhase::AfterEligibilityBeforeCommit,
        applies_when: pred,
        blocking: true,
        max_attempts: 1,
    };
    let hooks = HookRegistry::new()
        .register(spec("every-shape", ShapePredicate::Always), Arc::new(Log("every-shape", log.clone())))
        .register(spec("cross-mesh-only", ShapePredicate::MultiMesh), Arc::new(Log("cross-mesh-only", log.clone())))
        .seal()
        .unwrap();
    LifecycleTransitionPipeline::new(hooks, Arc::new(MemoryReceiptLog::default()))
}

async fn make_ready(meshes_desired: &[&str], meshes_observed: &[&str]) -> (TransitionResult, Vec<&'static str>, bool) {
    let log = Arc::new(Mutex::new(vec![]));
    let p = pipeline(&log);
    let d = desired(meshes_desired);
    let t = ready_topology(meshes_observed);
    let shape = ShapeFacts::from_desired(&d);
    let mut committed = false;
    let r = p
        .transition(
            Transition { transition_id: Transition::id_for(READY, "fabric1"), target: "fabric1".into(), key: READY, shape: &shape },
            || fabric_ready_eligibility(&d, &t),
            || committed = true,
        )
        .await;
    let ran = log.lock().unwrap().clone();
    (r, ran, committed)
}

#[tokio::test]
async fn the_shape_conditional_hook_runs_for_mm_and_not_for_sn_or_mn() {
    let (r, ran, committed) = make_ready(&["mesh1", "mesh2"], &["mesh1", "mesh2"]).await;
    assert_eq!((r, committed), (TransitionResult::Committed, true));
    assert_eq!(ran, vec!["every-shape", "cross-mesh-only"]);
    let (r, ran, committed) = make_ready(&["mesh1"], &["mesh1"]).await;
    assert_eq!((r, committed), (TransitionResult::Committed, true));
    assert_eq!(ran, vec!["every-shape"], "SN/MN desire one mesh: the multi-mesh hook does not apply");
}

#[tokio::test]
async fn fabric_readiness_waits_on_every_desired_mesh() {
    let (r, ran, committed) = make_ready(&["mesh1", "mesh2"], &["mesh1"]).await;
    assert_eq!(r, TransitionResult::NotEligible { reason: "mesh mesh2 does not exist".into() });
    assert!(!committed);
    assert!(ran.is_empty(), "pre-ready hooks wait for eligibility");
}
