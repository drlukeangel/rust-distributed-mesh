//! The mesh delete command pair's refusals when what the receiver must read to decide a call fails
//! (node-rpc-envelope.md, "Mesh delete command pair"; CLAUDE.md error rule: "Name what failed and
//! where ... 'Never written' and 'could not be read' are different errors"). The leaving
//! mesh-primary resolves the owner a `DeleteMesh` hands back to (`mesh_delete::leave_owner`, which its
//! door `admin::mesh_delete::admit_delete_mesh` answers from); the fabric-primary adopts the final primary's
//! runtime and reads its manifest before it accepts a `MeshDeleted` (`mesh_delete::adopt_final_runtime`
//! and `mesh_delete::handoff_verdict_from_read`, which its door `validate_handoff` answers from). A
//! read that fails is `RejectedSourceFailed` naming what failed, the operation and the cause, under
//! the reject span `rdm.node_admin.mesh.reject.via-source-failed`; it is never this admin's own
//! `NotReady`, which is the readiness door's answer (node-rpc-envelope.md, "Node RPC readiness
//! gate").
// @feature: node-lifecycle

use crate::mesh_delete_resume::{capture, trace_id};
use rdm_core::{BuildId, MeshId, NodeId, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rdm_admin::build_state::BuildStateError;
use rdm_admin::deployment::pipeline::RuntimeProof;
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::mesh_delete::{adopt_final_runtime, handoff_verdict_from_read, leave_owner, manifest_reference, operation, Admitted, MeshDeleteKey, MeshDeleteOwner, RosterBirth};
use rdm_transport::NodeTarget;
use rdm_admin::model::ProviderKind;
use rdm_core::status::StatusReply;
use serde_json::Value;

const SOURCE_FAILED: &str = "rdm.node_admin.mesh.reject.via-source-failed";
const DOMAIN: &str = "mesh-delete-source";

fn key(mesh_id: &MeshId) -> MeshDeleteKey {
    MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::parse("pdz453kf4br6").unwrap(), attempt: 1, operation: operation(mesh_id) }
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

/// CONTRACT (mesh-delete.md step 4: the fabric-primary validates the manifest it reads from the final
/// primary; node-rpc-envelope.md, "Mesh delete command pair"): a `MeshDeleted` whose manifest cannot be
/// read from the final primary (its Build facts unreached, or its door not ready) is refused
/// `RejectedSourceFailed` naming the final primary's Build log, the fetch of the Build with the
/// manifest reference, and the read's own failure; the leave stays open (no verdict, so nothing
/// ends it). What must NOT happen: this admin answering its own `NotReady` for a read of another node,
/// or the read answered as an incomplete manifest (that is the final primary holding no fact of the
/// Build, never written there).
#[test]
fn a_mesh_deleted_report_whose_manifest_cannot_be_read_is_refused_by_name_and_the_leave_stays_open() {
    let mesh_id = MeshId::mint();
    let k = key(&mesh_id);
    let fp = RosterBirth { node: "mesh2.admin.1".into(), node_id: NodeId::mint().to_string(), incarnation_id: "i1".into() };
    let runtime = RuntimeProof { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() };
    let admitted = Admitted { roster: vec![fp.clone()], final_primary: fp, final_runtime: runtime, amendments: Vec::new() };
    let reference = manifest_reference(k.build_id.as_str(), 1, &k.operation, false);
    let cap = capture();
    let (unreached, not_ready, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-manifest-unread", mesh_id = %mesh_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let unreached = handoff_verdict_from_read(Err(BuildStateError::Io("builds.jsonl: no such device".into())), &k, &admitted, &reference, "mesh1.admin.1", "mesh2.admin.1");
        let not_ready = handoff_verdict_from_read(Err(BuildStateError::Io("builds.jsonl: interrupted".into())), &k, &admitted, &reference, "mesh1.admin.1", "mesh2.admin.1");
        (unreached, not_ready, trace_id)
    });
    let spans = cap.spans();
    keep("manifest_unread", &trace_id, &spans);
    let (source, op, cause) = source_failed(&unreached);
    assert!(source.contains("Build storage") && source.contains("mesh2.admin.1") && op.contains("read-committed pdz453kf4br6") && op.contains(&reference) && cause.contains("no such device"), "{source} | {op} | {cause}");
    let (_, _, cause) = source_failed(&not_ready);
    assert!(cause.contains("interrupted"), "the storage read's failure is a fact of the read: {cause}");
    let named = refusals(&spans, &trace_id);
    for r in [&unreached, &not_ready] {
        let (source, op, cause) = source_failed(r);
        assert!(named.contains(&("mesh-deleted".to_string(), source, op, cause)), "each read failure is its reject span naming the call and the three fields: {named:?}");
    }
}

/// CONTRACT (node-rpc-envelope.md, "Mesh delete command pair": DeleteMesh is the fabric-primary's
/// command and its handoff goes back to the workflow's owner; node-start.md step 2: a node "joins
/// back over the endpoint and path of the call that commanded it; it holds no record of the
/// commander", the way the leaving mesh-primary hands back too): the leaving mesh-primary hands the
/// leave back over the call that sent it, the caller's endpoint at the call's path, whatever its
/// view holds of the caller; with no call to reach back over, to the fabric-primary its view names.
/// With neither, the `DeleteMesh` is refused `RejectedSourceFailed` naming its view, the resolution
/// of the owner and why, under its reject span. What must NOT happen: the handoff aimed at a birth
/// the view names when the call's endpoint is there, `NotReady`, or a leave admitted with nobody to
/// hand it back to.
#[test]
fn a_delete_mesh_hands_back_over_its_call_else_to_the_fabric_primary_else_is_refused_by_name() {
    let mesh_id = MeshId::mint();
    let k = key(&mesh_id);
    let caller = MeshDeleteOwner { target: NodeTarget::Endpoint { endpoint_id: iroh::SecretKey::generate().public(), addr: "127.0.0.1:4100".parse().unwrap() }, name: "the admin at its call's endpoint".into() };
    let fp_id = NodeId::mint();
    let fp = MeshDeleteOwner { target: NodeTarget::ExactNode(fp_id), name: "mesh1.admin.1".into() };
    let cap = capture();
    let (none, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-no-owner", mesh_id = %mesh_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        assert_eq!(leave_owner(Some(caller.clone()), Some(fp.clone()), "mesh2.admin.1", &k), Ok(caller.clone()), "the handoff goes back over the call that sent delete-mesh");
        assert_eq!(leave_owner(None, Some(fp.clone()), "mesh2.admin.1", &k), Ok(fp.clone()), "else to the fabric-primary the view names");
        (leave_owner(None, None, "mesh2.admin.1", &k), trace_id)
    });
    let spans = cap.spans();
    keep("no_owner", &trace_id, &spans);
    let (source, op, cause) = source_failed(&none);
    assert!(source == "the view of mesh2.admin.1" && op.contains("owner") && op.contains(&mesh_id.to_string()) && cause.contains("no call") && cause.contains("no fabric-primary"), "{source} | {op} | {cause}");
    assert_eq!(refusals(&spans, &trace_id), vec![("delete-mesh".to_string(), source, op, cause)], "one reject span, naming the call and the three fields");
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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// CONTRACT (mesh-delete.md step 5: the owner stops the final primary through its provider proof
/// path): a `MeshDeleted` naming a final runtime this admin's provider cannot adopt (another control
/// domain) is refused `RejectedSourceFailed` naming the provider, the adoption of the runtime and the
/// provider's refusal, under its reject span; a runtime in the provider's domain is adopted. What must
/// NOT happen: `NotReady`.
#[test]
fn a_mesh_deleted_report_naming_a_runtime_the_provider_cannot_adopt_is_refused_by_name() {
    let mesh_id = MeshId::mint();
    let k = key(&mesh_id);
    let fact = |domain: &str| RuntimeFact { deployment_id: rdm_core::fixture_crockford60("dep-final"), provider: RuntimeProvider::Process, control_domain: domain.into(), locator: RuntimeLocator::Process { pid: 4242, start: 7 } };
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
    assert!(source.contains("provider of mesh1.admin.1") && op.contains("adopt") && op.contains(&rdm_core::fixture_crockford60("dep-final")) && cause.contains("control domain"), "{source} | {op} | {cause}");
    assert_eq!(refusals(&spans, &trace_id), vec![("mesh-deleted".to_string(), source, op, cause)], "one reject span, naming the call and the three fields");
}
