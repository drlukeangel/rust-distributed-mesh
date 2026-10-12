//! The mesh delete command pair's refusals when what the receiver must read to decide a call fails
//! (node-rpc-envelope.md, "Mesh delete command pair"; CLAUDE.md error rule: "Name what failed and
//! where ... 'Never written' and 'could not be read' are different errors"). The leaving
//! mesh-primary resolves the owner a `LeaveMesh` hands back to (`mesh_leave::leave_owner`, which its
//! door `admin::leave::admit_leave` answers from); the fabric-primary adopts the final primary's
//! runtime and reads its manifest before it accepts a `MeshLeave` (`mesh_leave::adopt_final_runtime`
//! and `mesh_leave::handoff_verdict_from_read`, which its door `validate_handoff` answers from). A
//! read that fails is `RejectedSourceFailed` naming what failed, the operation and the cause, under
//! the reject span `rdm.node_admin.mesh.reject.via-source-failed`; it is never this admin's own
//! `NotReady`, which is the readiness door's answer (node-rpc-envelope.md, "Node RPC readiness
//! gate").
// @feature: node-lifecycle

use crate::mesh_leave_resume::{capture, trace_id};
use rafka_mesh_entity::{MeshId, NodeId, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_node_admin_core::build_facts_read::FetchFailure;
use rafka_node_admin_core::deployment::pipeline::RuntimeProof;
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::mesh_leave::{adopt_final_runtime, handoff_verdict_from_read, leave_owner, manifest_reference, operation, Admitted, LeaveKey, RosterBirth};
use rafka_node_admin_core::model::ProviderKind;
use rafka_node_rpc_contract::status::StatusReply;
use serde_json::Value;

const SOURCE_FAILED: &str = "rdm.node_admin.mesh.reject.via-source-failed";
const DOMAIN: &str = "mesh-delete-source";

fn key(mesh_id: &MeshId) -> LeaveKey {
    LeaveKey { mesh_id: mesh_id.clone(), build_id: "bld_source".into(), attempt: 1, operation: operation(mesh_id) }
}

/// The cell's spans, written to `target/cells/mesh_delete_source/<cell>/spans.json`; its trace id and
/// that path are printed.
fn keep(cell: &str, trace_id: &str, spans: &[Value]) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_delete_source").join(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(spans).unwrap()).unwrap();
    println!("cell={cell} trace_id={trace_id} spans={}", dir.join("spans.json").display());
}

/// The `(call, source, op, cause)` of each `via-source-failed` span in the cell's own trace.
fn refusals(spans: &[Value], trace_id: &str) -> Vec<(String, String, String, String)> {
    spans
        .iter()
        .filter(|s| s["name"] == SOURCE_FAILED && s["trace_id"] == trace_id)
        .map(|s| {
            let a = |k: &str| s["attributes"][k].as_str().unwrap_or_default().to_string();
            (a("call"), a("source"), a("op"), a("cause"))
        })
        .collect()
}

/// The `(source, op, cause)` of a refusal that must be `RejectedSourceFailed`; anything else panics.
fn source_failed<T: std::fmt::Debug>(r: &Result<T, StatusReply>) -> (String, String, String) {
    match r {
        Err(StatusReply::RejectedSourceFailed { source, op, cause }) => (source.clone(), op.clone(), cause.clone()),
        other => panic!("refused by name, RejectedSourceFailed naming what failed, the operation and the cause: {other:?}"),
    }
}

/// CONTRACT (mesh-leave.md step 4: the fabric-primary validates the manifest it reads from the final
/// primary; node-rpc-envelope.md, "Mesh delete command pair"): a `MeshLeave` whose manifest cannot be
/// read from the final primary (its Build facts unreached, or its door not ready) is refused
/// `RejectedSourceFailed` naming the final primary's Build log, the fetch of the Build with the
/// manifest reference, and the read's own failure; the leave stays open (no verdict, so nothing
/// ends it). What must NOT happen: this admin answering its own `NotReady` for a read of another node,
/// or the read answered as an incomplete manifest (that is the final primary holding no fact of the
/// Build, never written there).
#[test]
fn a_mesh_leave_whose_manifest_cannot_be_read_is_refused_by_name_and_the_leave_stays_open() {
    let mesh_id = MeshId::mint();
    let k = key(&mesh_id);
    let fp = RosterBirth { node: "mesh2.admin.1".into(), node_id: NodeId::mint().to_string(), incarnation_id: "i1".into() };
    let runtime = RuntimeProof { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() };
    let admitted = Admitted { roster: vec![fp.clone()], final_primary: fp, final_runtime: runtime, amendments: Vec::new() };
    let reference = manifest_reference(&k.build_id, 1, &k.operation, false);
    let cap = capture();
    let (unreached, not_ready, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-manifest-unread", mesh_id = %mesh_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let unreached = handoff_verdict_from_read(Err(FetchFailure::Unreached("no path to mesh2.admin.1".into())), &k, &admitted, &reference, "mesh1.admin.1", "mesh2.admin.1");
        let not_ready = handoff_verdict_from_read(Err(FetchFailure::NotReady("hydrating".into())), &k, &admitted, &reference, "mesh1.admin.1", "mesh2.admin.1");
        (unreached, not_ready, trace_id)
    });
    let spans = cap.spans();
    keep("manifest_unread", &trace_id, &spans);
    let (source, op, cause) = source_failed(&unreached);
    assert!(source.contains("Build log of mesh2.admin.1") && op.contains("fetch-build-facts bld_source") && op.contains(&reference) && cause.contains("unreached") && cause.contains("no path"), "{source} | {op} | {cause}");
    let (_, _, cause) = source_failed(&not_ready);
    assert!(cause.contains("not ready") && cause.contains("hydrating"), "the final primary's door not ready is a fact of the read: {cause}");
    let named = refusals(&spans, &trace_id);
    for r in [&unreached, &not_ready] {
        let (source, op, cause) = source_failed(r);
        assert!(named.contains(&("mesh-leave".to_string(), source, op, cause)), "each read failure is its reject span naming the call and the three fields: {named:?}");
    }
}

/// CONTRACT (node-rpc-envelope.md, "Mesh delete command pair": LeaveMesh is the fabric-primary's
/// command and its handoff goes back to the workflow's owner): the leaving mesh-primary hands the
/// leave back to the admin that sent it, else to the fabric-primary its view names. With neither (an
/// endpoint it holds no birth for, and a view naming no fabric-primary) the `LeaveMesh` is refused
/// `RejectedSourceFailed` naming its view, the resolution of the owner and why, under its reject span.
/// What must NOT happen: `NotReady`, or a leave admitted with nobody to hand it back to.
#[test]
fn a_leave_mesh_with_no_owner_to_hand_back_to_is_refused_by_name() {
    let mesh_id = MeshId::mint();
    let k = key(&mesh_id);
    let (sender, fp) = (NodeId::mint(), NodeId::mint());
    let cap = capture();
    let (none, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-no-owner", mesh_id = %mesh_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        assert_eq!(leave_owner(Some(&sender), Some(&fp), "mesh2.admin.1", &k), Ok(sender.clone()), "the sender owns the workflow");
        assert_eq!(leave_owner(None, Some(&fp), "mesh2.admin.1", &k), Ok(fp.clone()), "else the fabric-primary the view names");
        (leave_owner(None, None, "mesh2.admin.1", &k), trace_id)
    });
    let spans = cap.spans();
    keep("no_owner", &trace_id, &spans);
    let (source, op, cause) = source_failed(&none);
    assert!(source == "the view of mesh2.admin.1" && op.contains("owner") && op.contains(&mesh_id.to_string()) && cause.contains("no birth") && cause.contains("no fabric-primary"), "{source} | {op} | {cause}");
    assert_eq!(refusals(&spans, &trace_id), vec![("leave-mesh".to_string(), source, op, cause)], "one reject span, naming the call and the three fields");
}

/// A process provider whose control domain is [`DOMAIN`]: it adopts only what its domain controls.
struct Host;

#[async_trait::async_trait]
impl DeploymentProvider for Host {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, _: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        unreachable!("adoption launches nothing")
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        unreachable!("adoption terminates nothing")
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        unreachable!("adoption inspects nothing")
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        unreachable!("adoption stops nothing")
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
        Ok(rafka_node_admin_core::deployment::provider::Found::NeverLaunched)
    }
}

/// CONTRACT (mesh-leave.md step 5: the owner stops the final primary through its provider proof
/// path): a `MeshLeave` naming a final runtime this admin's provider cannot adopt (another control
/// domain) is refused `RejectedSourceFailed` naming the provider, the adoption of the runtime and the
/// provider's refusal, under its reject span; a runtime in the provider's domain is adopted. What must
/// NOT happen: `NotReady`.
#[test]
fn a_mesh_leave_naming_a_runtime_the_provider_cannot_adopt_is_refused_by_name() {
    let mesh_id = MeshId::mint();
    let k = key(&mesh_id);
    let fact = |domain: &str| RuntimeFact { deployment_id: "dep-final".into(), provider: RuntimeProvider::Process, control_domain: domain.into(), locator: RuntimeLocator::Process { pid: 4242, start: 7 } };
    let cap = capture();
    let (foreign, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-provider-refused", mesh_id = %mesh_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        assert_eq!(adopt_final_runtime(&Host, &fact(DOMAIN), "mesh1.admin.1", &k), Ok(()), "a runtime in the provider's domain is adopted");
        (adopt_final_runtime(&Host, &fact("another-host"), "mesh1.admin.1", &k), trace_id)
    });
    let spans = cap.spans();
    keep("provider_refused", &trace_id, &spans);
    let (source, op, cause) = source_failed(&foreign);
    assert!(source.contains("provider of mesh1.admin.1") && op.contains("adopt") && op.contains("dep-final") && cause.contains("control domain"), "{source} | {op} | {cause}");
    assert_eq!(refusals(&spans, &trace_id), vec![("mesh-leave".to_string(), source, op, cause)], "one reject span, naming the call and the three fields");
}
