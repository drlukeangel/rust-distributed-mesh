//! R-BI5 (Luke 2026-10-12, "build-id is a crockford id, we only have one id type, never a hex"):
//! a Build id and a deployment id are minted as every other id is, 12 lowercase Crockford
//! characters, and a stored `bld-<hex>` or 32-hex value is an old row that reading refuses by name.

use rdm_admin::build::BuildId;
use rdm_admin::model::DeploymentId;

const CROCKFORD: &str = "0123456789abcdefghjkmnpqrstvwxyz";

/// CONTRACT: a minted Build id is 12 characters of the lowercase Crockford alphabet, bare (no `bld-`).
#[test]
fn a_minted_build_id_is_twelve_lowercase_crockford_characters() {
    for _ in 0..200 {
        let id = BuildId::mint().to_string();
        assert_eq!(id.len(), 12, "{id}");
        assert!(id.chars().all(|c| CROCKFORD.contains(c)), "{id}");
    }
}

/// CONTRACT: a minted deployment id is 12 characters of the lowercase Crockford alphabet.
#[test]
fn a_minted_deployment_id_is_twelve_lowercase_crockford_characters() {
    for _ in 0..200 {
        let id = DeploymentId::mint().to_string();
        assert_eq!(id.len(), 12, "{id}");
        assert!(id.chars().all(|c| CROCKFORD.contains(c)), "{id}");
    }
}

/// CONTRACT: a stored `bld-<24 hex>` Build id is refused by name: the error names the kind and the value.
#[test]
fn a_stored_bld_hex_build_id_is_refused_by_name() {
    let old = "bld-0123456789abcdef01234567";
    let e = serde_json::from_str::<BuildId>(&format!("\"{old}\"")).unwrap_err().to_string();
    assert!(e.contains("build id") && e.contains(old), "{e}");
}

/// CONTRACT: a stored 32-hex deployment id is refused by name: the error names the kind and the value.
#[test]
fn a_stored_hex_deployment_id_is_refused_by_name() {
    let old = "0123456789abcdef0123456789abcdef";
    let e = serde_json::from_str::<DeploymentId>(&format!("\"{old}\"")).unwrap_err().to_string();
    assert!(e.contains("deployment id") && e.contains(old), "{e}");
}

/// CONTRACT (R-BI5a, Luke 2026-10-14: "bid_xxxx-xxxx-xxxx but only in logs otherwise we just use
/// the raw-crockford as its a bigint storage value at 12"): `BuildId`'s `Display` and serde stay the
/// bare 12 lowercase Crockford characters that feed row keys, journal lines and the `0x20` Build
/// carrier; `BuildId::parse` and serde refuse the dashed `bid_xxxx-xxxx-xxxx` log form (and any
/// 12-character `bid_...` / `b1d-...` string) by name; and a span's `build_id` attribute renders as
/// `bid_xxxx-xxxx-xxxx`.
#[test]
fn a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans() {
    use rdm_core::ids::IdError;
    use rdm_core::PrimaryRole;
    use rdm_admin::accepted::{AcceptedStore, FabricTopology};
    use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
    use rdm_admin::fabric_handover::update_operation;
    use rdm_admin::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};
    use rdm_admin::topology::Topology;
    use rdm_core::build::{Build, BuildReply, Disposition};
    use rdm_core::NodeProtocol;

    let dir = std::env::temp_dir().join(format!("rdm-rbi5a-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-node-admin-core-rbi5a").expect("the cell's evidence file can be created");
    let (id, facts_json) = capture.run(2, async move {
        let builds = MemoryBuildStateAdapter::new();
        let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
        admin.status = NodeStatus::Ready;
        admin.incarnation_id = Some(IncarnationId::mint());
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let observed = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![admin],
        };
        AcceptedStore::seeded(&builds, observed.fabric.id.clone(), FabricTopology::of_observed(&observed), "mesh1.admin.1").await.unwrap();
        let id = builds.current_build().await.unwrap().into_build_id().unwrap();
        let facts_json = serde_json::to_string(&builds.facts().await.unwrap()).unwrap();
        (id, facts_json)
    });

    let bare = id.to_string();
    assert_eq!(bare.len(), 12, "{bare}");
    assert!(bare.chars().all(|c| CROCKFORD.contains(c)), "{bare}");
    assert_eq!(serde_json::to_string(&id).unwrap(), format!("\"{bare}\""));

    let dashed = format!("bid_{}-{}-{}", &bare[0..4], &bare[4..8], &bare[8..12]);
    assert_eq!(id.log().to_string(), dashed);
    assert_eq!(
        BuildId::parse(&dashed),
        Err(IdError::NonCanonical { kind: "build id", value: dashed.clone(), at: 1, found: 'i' })
    );
    let wrong_width = format!("b1d{bare}");
    assert_eq!(
        BuildId::parse(&wrong_width),
        Err(IdError::WrongWidth { kind: "build id", value: wrong_width, len: 15 })
    );
    let serde_err = serde_json::from_str::<BuildId>(&format!("\"{dashed}\"")).unwrap_err().to_string();
    assert!(serde_err.contains("build id") && serde_err.contains(&dashed), "{serde_err}");
    for bad12 in ["bid_01234567", "b1d-01234567"] {
        assert!(
            matches!(BuildId::parse(bad12), Err(IdError::NonCanonical { kind: "build id", .. })),
            "{bad12} must be refused as NonCanonical"
        );
    }

    // Row key, stored BuildFact row and 0x20 Build carrier all carry the bare 12 characters, never `bid_...`.
    let row_key = update_operation(PrimaryRole::MeshPrimary, &bare, 1, "node.stop:mesh1.admin.1");
    assert_eq!(row_key, format!("node.primary.update:{bare}:1:node.stop:mesh1.admin.1"));
    assert!(!row_key.contains("bid_"), "{row_key}");
    assert!(facts_json.contains(&format!("\"build_id\":\"{bare}\"")) && !facts_json.contains("bid_"), "{facts_json}");
    let started = BuildReply::Started { build_id: id.to_string(), attempt: 1, disposition: Disposition::Created };
    let wire = Build::encode_reply(&started).unwrap();
    let decoded = Build::decode_reply(&wire).unwrap();
    assert_eq!(decoded, BuildReply::Started { build_id: bare.clone(), attempt: 1, disposition: Disposition::Created });

    let spans = capture.spans().expect("the cell's spans are readable");
    let accepted = spans
        .iter()
        .find(|s| s["name"] == "rdm.node_admin.build.create.via-accepted")
        .unwrap_or_else(|| panic!("no rdm.node_admin.build.create.via-accepted span in {spans:?}"));
    assert_eq!(
        accepted["attributes"]["build_id"].as_str(),
        Some(dashed.as_str()),
        "the span's build_id attribute carries the dashed bid_xxxx-xxxx-xxxx log rendering: {accepted}"
    );
    eprintln!("span rdm.node_admin.build.create.via-accepted {accepted}");
    let _ = std::fs::remove_dir_all(&dir);
}

