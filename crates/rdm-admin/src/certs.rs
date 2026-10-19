//! Fabric certs (fabric-certs.md): the material a node-admin holds, and what it issues with it.
//!
//! A node-admin is started with one of two choices: the fabric's root ([`CertChoice::Ca`], the
//! bootstrap admin, and every admin RDM launches, with its mesh's issuing material beside it), or no
//! certs by name ([`CertChoice::NoCerts`]). With neither it is refused at start. RDM signs with what
//! it was given, in the format of [`rdm_core::certs`]:
//!
//! - A birth's `JoinNode` is accepted by the admin that deployed it. That admin issues the member
//!   cert for the exact birth BEFORE it admits it ([`CertIssuer::member`], `node.cert.create`), and
//!   the bytes travel in the join answer (`JoinControl::member_cert`). A refusal refuses the join by
//!   name: the birth is not admitted (nothing is installed for its key, `WaitForBind` is not
//!   completed).
//! - A node-admin is launched with its mesh's issuing material ([`CertIssuer::issuing_material`],
//!   `RDM_ISSUING_KEY`, `RDM_ISSUING_CERT`) and the root beside it (`RDM_CA_SECRET`, fabric-certs.md
//!   §Custody). An admin mints a mesh's issuer once, the first time it needs it
//!   (`mesh.issuer.create`), and hands that one to every admin of the mesh; an admin launched with
//!   its mesh's issuer holds it as given and mints another mesh's from its own copy of the root.
//!
//! Issue time and expiry are the issuing admin's rafka-time ([`RafkaTime`]); an admin that does not
//! yet hold rafka-time issues nothing and says so. A member cert lives
//! [`MEMBER_CERT_TTL_SECS`](rdm_core::certs::MEMBER_CERT_TTL_SECS), cut to its issuer's expiry when
//! the issuer expires first.

pub use rdm_core::certs::{BirthIdentity, CertError, IssueFailure};
use rdm_core::certs::{IssuingKey, SigningKey, VerifyingKey};
use rdm_core::launch::{IssuingMaterial, ENV_ISSUING_CERT, ENV_ISSUING_KEY};
use rdm_transport::clock::RafkaTime;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

/// The fabric's root, given in the environment (fabric-certs.md "Where a node-admin's signing
/// material comes from"), as hex.
pub const ENV_CA_SECRET: &str = rdm_core::launch::ENV_CA_SECRET;

/// The file in a node-admin's data dir that keeps the fabric's root it holds.
pub const CA_SECRET_FILE: &str = "ca-secret";

/// What a node-admin is started with for certs. There is no default: [`CertChoice::Unchosen`] is
/// refused at start by name.
#[derive(Default)]
pub enum CertChoice {
    /// Nothing was chosen.
    #[default]
    Unchosen,
    /// No certs, by explicit choice: every issuance is empty, each by name.
    NoCerts,
    /// The fabric's root: the bootstrap admin, and every admin RDM launches.
    Ca {
        /// The root's ed25519 secret.
        secret: SigningKey,
        /// Its mesh's issuing key, as its launch gave it: the mesh's one issuer, never minted again.
        issuing: Option<IssuingKey>,
    },
}

impl CertChoice {
    /// The material an RDM node-admin executable is started with, read from `get` (its environment)
    /// and `data_dir`: `RDM_CA_SECRET`, else the `ca-secret` its data dir keeps, the fabric's root, with
    /// `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT` beside it when it was launched with them (its mesh's
    /// one issuer, which must be `own_mesh`'s and signed by that root); else, for the bootstrap admin of
    /// day 0 (`day_zero`: no launch and no seeds), a root minted now. A root the environment gives or
    /// this call mints is kept as `ca-secret` in the data dir when none is kept there yet, so the same
    /// admin started again holds the same root. Material that does not decode, issuing material
    /// without the root, and an admin that is not day 0's and holds no root, are refused by name.
    pub fn from_material(data_dir: &Path, own_mesh: &str, day_zero: bool, get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let kept = data_dir.join(CA_SECRET_FILE);
        let present = |k: &str| get(k).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
        let keep = |root: &SigningKey| -> Result<(), String> {
            if kept.exists() {
                return Ok(());
            }
            std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: the data dir the fabric's root is kept in: {e}", data_dir.display()))?;
            std::fs::write(&kept, rdm_core::certs::encode_key(root)).map_err(|e| format!("{}: the fabric's root could not be kept: {e}", kept.display()))
        };
        let issuing = rdm_core::launch::decode_issuing(get(ENV_ISSUING_KEY), get(ENV_ISSUING_CERT), get(ENV_CA_SECRET))?;
        let root = match present(ENV_CA_SECRET) {
            Some(hex) => {
                let secret = rdm_core::certs::decode_key(&hex).map_err(|e| format!("{ENV_CA_SECRET}: {e}"))?;
                keep(&secret)?;
                Some(secret)
            }
            None => match std::fs::read_to_string(&kept) {
                Ok(hex) => Some(rdm_core::certs::decode_key(&hex).map_err(|e| format!("{}: {e}", kept.display()))?),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(format!("{}: the fabric's root this data dir keeps cannot be read: {e}", kept.display())),
            },
        };
        if let Some(secret) = root {
            let issuing = issuing.map(|m| issuing_key(&m, own_mesh, &secret.verifying_key())).transpose()?;
            return Ok(Self::Ca { secret, issuing });
        }
        if day_zero {
            let secret = rdm_core::certs::mint_key();
            keep(&secret)?;
            tracing::info_span!("rdm.node_admin.cert.create.via-day-zero-root", mesh = own_mesh, kept = %kept.display())
                .in_scope(|| tracing::info!("the bootstrap admin holds no root and has no seeds: it minted the fabric's root (fabric.ca.create)"));
            return Ok(Self::Ca { secret, issuing: None });
        }
        Err(format!(
            "this node-admin holds no cert material: {ENV_CA_SECRET} or a `{CA_SECRET_FILE}` in {} (fabric-certs.md: an admin with seeds or an anchor and no root refuses to start)",
            data_dir.display(),
        ))
    }

    /// [`Self::from_material`] for the node-admin `cfg` configures, from this process's environment:
    /// day 0 is a bootstrap admin with no launch, no seeds and no recovery flag, so a person-started
    /// recovery never mints a root of its own.
    pub fn from_config(cfg: &crate::admin::AdminConfig) -> Result<Self, String> {
        let day_zero = cfg.launch.is_none() && cfg.seeds.is_empty() && !cfg.mesh_primary && !cfg.fabric_primary;
        Self::from_material(&cfg.data_dir, &cfg.mesh, day_zero, |k| std::env::var(k).ok())
    }

    /// The issuer this choice makes, issuing on `time`. The unchosen choice is refused by name.
    pub fn issuer(self, time: RafkaTime) -> Result<CertIssuer, String> {
        match self {
            Self::Unchosen => Err("this node-admin was started with no cert choice: set Wiring::certs to CertChoice::Ca or CertChoice::NoCerts".into()),
            Self::NoCerts => Ok(CertIssuer::no_certs(time)),
            Self::Ca { secret, issuing } => Ok(CertIssuer::with_root(secret, issuing, time)),
        }
    }
}

/// The issuing key `issuing` carries, which must be `own_mesh`'s, must be the key its cert names, and
/// must be signed by `root`.
fn issuing_key(issuing: &IssuingMaterial, own_mesh: &str, root: &VerifyingKey) -> Result<IssuingKey, String> {
    let key = rdm_core::certs::decode_key(&issuing.key.0).map_err(|e| format!("{ENV_ISSUING_KEY}: {e}"))?;
    let cert = rdm_core::certs::decode_issuing_cert(&issuing.cert).map_err(|e| format!("{ENV_ISSUING_CERT}: {e}"))?;
    if cert.cert.mesh != own_mesh {
        return Err(format!("{ENV_ISSUING_CERT}: the issuing material is mesh {}'s, and this node-admin is of mesh {own_mesh}", cert.cert.mesh));
    }
    if cert.cert.issuer_pub != key.verifying_key() {
        return Err(format!("{ENV_ISSUING_KEY}: the issuing key is not the key its cert names"));
    }
    rdm_core::certs::verify_issuer(&cert, root).map_err(|e| format!("{ENV_ISSUING_CERT}: {e}: the issuing cert is not signed by the fabric's root this node-admin holds ({ENV_CA_SECRET})"))?;
    Ok(IssuingKey::from_parts(key, cert))
}

/// What an admin signs with.
enum Material {
    /// No certs, by name.
    NoCerts,
    /// The fabric's root: every mesh's issuer can be minted.
    Root(SigningKey),
}

/// A node-admin's cert material together with the rafka-time it issues on: what the join door and
/// the deployment executor call.
pub struct CertIssuer {
    material: Material,
    time: RafkaTime,
    /// The issuing key of each mesh this admin signs for: minted once from the root, or the one it
    /// was given.
    issuers: Mutex<BTreeMap<String, IssuingKey>>,
}

impl CertIssuer {
    /// The explicit choice of no certs, on `time`.
    pub fn no_certs(time: RafkaTime) -> Self {
        Self { material: Material::NoCerts, time, issuers: Mutex::default() }
    }

    /// An issuer holding the fabric's root `secret`, and its own mesh's issuing key `issuing` when its
    /// launch gave it one, on `time`.
    pub fn with_root(secret: SigningKey, issuing: Option<IssuingKey>, time: RafkaTime) -> Self {
        let issuers = issuing.into_iter().map(|k| (k.mesh().to_string(), k)).collect();
        Self { material: Material::Root(secret), time, issuers: Mutex::new(issuers) }
    }

    /// The fabric root's public key, when this admin holds the root.
    pub fn root(&self) -> Option<VerifyingKey> {
        match &self.material {
            Material::Root(secret) => Some(secret.verifying_key()),
            Material::NoCerts => None,
        }
    }

    fn now(&self, what: &str) -> Result<u64, IssueFailure> {
        self.time.try_now_ms().ok_or_else(|| IssueFailure::NoRafkaTime(format!("this admin holds no rafka-time to issue {what} on")))
    }

    /// The issuing key of `mesh`: the one held, or one minted now from the root (`mesh.issuer.create`)
    /// and held from then on.
    fn issuer_for(&self, root: &SigningKey, mesh: &str, now_ms: u64) -> IssuingKey {
        let mut issuers = self.issuers.lock().unwrap();
        if let Some(key) = issuers.get(mesh) {
            return key.clone();
        }
        let key = IssuingKey::mint(root, mesh, rdm_core::certs::ISSUING_CERT_TTL_SECS, now_ms);
        tracing::info_span!("rdm.node_admin.cert.create.via-mesh-birth", mesh, issued_at_rafka_ms = now_ms, issuer_expires_at_rafka_ms = key.expiry_ms(), outcome = "issued")
            .in_scope(|| tracing::info!("the mesh's issuing key is minted under the fabric's root (mesh.issuer.create)"));
        issuers.insert(mesh.to_string(), key.clone());
        key
    }

    /// The member cert of the exact birth `birth`, as `JoinControl::member_cert` carries it: signed by
    /// `birth`'s mesh's issuing key, expiring [`rdm_core::certs::MEMBER_CERT_TTL_SECS`] from now on
    /// this admin's rafka-time, cut to its issuer's expiry. Empty when configured with no certs.
    pub fn member(&self, birth: &BirthIdentity) -> Result<Vec<u8>, IssueFailure> {
        let Material::Root(root) = &self.material else {
            let now = self.time.try_now_ms().unwrap_or(0);
            tracing::info_span!("rdm.node_admin.cert.create.via-no-certs-configured", node = %birth.name, node_id = %birth.node_id, incarnation_id = %birth.incarnation.as_str(), issued_at_rafka_ms = now)
                .in_scope(|| tracing::info!("this node-admin is configured with no certs: the member cert is empty"));
            return Ok(Vec::new());
        };
        let span = tracing::info_span!(
            "rdm.node_admin.cert.create.via-join",
            node = %birth.name,
            node_id = %birth.node_id,
            incarnation_id = %birth.incarnation.as_str(),
            issued_at_rafka_ms = tracing::field::Empty,
            expires_at_rafka_ms = tracing::field::Empty,
            issuer_expires_at_rafka_ms = tracing::field::Empty,
            cert_len = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let _g = span.enter();
        let issued = (|| {
            let now_ms = self.now("a member cert")?;
            span.record("issued_at_rafka_ms", now_ms);
            let issuer = self.issuer_for(root, &birth.mesh, now_ms);
            span.record("issuer_expires_at_rafka_ms", issuer.expiry_ms());
            let expiry_ms = now_ms.saturating_add(rdm_core::certs::MEMBER_CERT_TTL_SECS.saturating_mul(1000)).min(issuer.expiry_ms());
            let signed = rdm_core::certs::issue(&issuer, birth, expiry_ms).map_err(|error| IssueFailure::Refused { error, detail: format!("the member cert of {} ({} {})", birth.name, birth.node_id, birth.incarnation.as_str()) })?;
            span.record("expires_at_rafka_ms", signed.cert.expiry_ms);
            Ok(rdm_core::certs::cert_to_bytes(&signed))
        })();
        match &issued {
            Ok(bytes) => {
                span.record("cert_len", bytes.len() as u64);
                span.record("outcome", "issued");
            }
            Err(IssueFailure::NoRafkaTime(_)) => {
                span.record("outcome", "no-rafka-time");
            }
            Err(IssueFailure::Refused { error, detail }) => {
                span.record("outcome", "refused");
                tracing::info_span!("rdm.node_admin.cert.reject.via-refused", node = %birth.name, node_id = %birth.node_id, incarnation_id = %birth.incarnation.as_str(), refusal = %error, detail = %detail)
                    .in_scope(|| tracing::warn!("the member cert is refused: the join is refused"));
            }
        }
        issued
    }

    /// The issuing material of `mesh` a node-admin of it is launched with (`RDM_ISSUING_KEY`,
    /// `RDM_ISSUING_CERT`), with the root beside it (`RDM_CA_SECRET`, fabric-certs.md §Custody); `None`
    /// when configured with no certs.
    pub fn issuing_material(&self, mesh: &str) -> Result<Option<IssuingMaterial>, IssueFailure> {
        let Material::Root(root) = &self.material else {
            tracing::info_span!("rdm.node_admin.cert.create.via-no-certs-configured", mesh)
                .in_scope(|| tracing::info!("this node-admin is configured with no certs: the admin is launched with no issuing material"));
            return Ok(None);
        };
        let now_ms = self.now("a mesh's issuing material")?;
        Ok(Some(IssuingMaterial::of(&self.issuer_for(root, mesh, now_ms), root)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EndpointId, IncarnationId, NodeId};
    use rdm_core::launch::SecretHex;

    fn birth(name: &str) -> BirthIdentity {
        let name: crate::model::PathName = name.parse().unwrap();
        BirthIdentity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), mesh: name.mesh.clone(), name, endpoint_key: EndpointId("k".into()) }
    }

    fn time_at(ms: u64) -> RafkaTime {
        let time = RafkaTime::unadopted();
        time.adopt(ms);
        time
    }

    // @feature: node-lifecycle
    #[test]
    fn an_issuer_whose_admin_holds_no_rafka_time_issues_nothing_and_says_so() {
        let issuer = CertIssuer::with_root(rdm_core::certs::mint_key(), None, RafkaTime::unadopted());
        match issuer.member(&birth("mesh1.rpc.1")) {
            Err(IssueFailure::NoRafkaTime(why)) => assert!(why.contains("no rafka-time"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(issuer.issuing_material("mesh2"), Err(IssueFailure::NoRafkaTime(_))));
    }

    // @feature: node-lifecycle
    #[test]
    fn the_explicit_no_certs_choice_issues_empty_bytes_and_an_unchosen_wiring_resolves_to_a_named_refusal() {
        let issuer = CertChoice::NoCerts.issuer(time_at(5)).unwrap();
        assert_eq!(issuer.member(&birth("mesh1.rpc.1")), Ok(Vec::new()));
        assert_eq!(issuer.issuing_material("mesh2"), Ok(None));
        let refused = CertChoice::default().issuer(time_at(5)).err().expect("no choice is refused");
        assert!(refused.contains("no cert choice") && refused.contains("NoCerts"), "{refused}");
    }

    /// CONTRACT (node-cert-renew.md "Request": "its expiry cut to the issuer's when the issuer expires
    /// first (`MemberOutlivesIssuer`)"): a member cert issued near its issuer's expiry is cut to it.
    #[test]
    fn a_member_cert_due_past_its_issuers_expiry_is_cut_to_it() {
        let root = rdm_core::certs::mint_key();
        let issuing_life_ms = rdm_core::certs::ISSUING_CERT_TTL_SECS * 1000;
        let fabric_primary = CertIssuer::with_root(root.clone(), None, time_at(1_000));
        let material = fabric_primary.issuing_material("mesh1").unwrap().unwrap();
        // The launched admin's clock is one hour short of its issuer's expiry: a day's cert is cut.
        let issuing = issuing_key(&material, "mesh1", &root.verifying_key()).unwrap();
        let late = CertChoice::Ca { secret: root, issuing: Some(issuing) }.issuer(time_at(1_000 + issuing_life_ms - 3_600_000)).unwrap();
        let signed = rdm_core::certs::cert_from_bytes(&late.member(&birth("mesh1.rpc.1")).unwrap()).unwrap();
        assert_eq!(signed.cert.expiry_ms, signed.issuer.cert.expiry_ms, "cut to the issuer's expiry");
    }

    /// CONTRACT (fabric-certs.md "Where a node-admin's signing material comes from"): the bootstrap
    /// admin takes the root from `RDM_CA_SECRET`, else the `ca-secret` its data dir keeps, else mints
    /// one on day 0 and keeps it; a launched admin takes `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT` beside
    /// the root, which must be its own mesh's; issuing material without the root, an admin that is not
    /// day 0's and holds no root, and material that does not decode, are refused by name.
    #[test]
    fn a_node_admin_reads_its_material_and_one_that_holds_none_is_refused_by_name() {
        let dir = crate::test_support::tempdir("cert-material");
        let env = |pairs: Vec<(&'static str, String)>| move |k: &str| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone());
        // Day 0: minted and kept; started again, the same root is read back.
        let minted = CertChoice::from_material(&dir, "mesh1", true, env(vec![])).unwrap();
        let CertChoice::Ca { secret, issuing: None } = minted else { panic!("day 0 mints a root, and no issuer") };
        let again = CertChoice::from_material(&dir, "mesh1", false, env(vec![])).unwrap();
        assert!(matches!(&again, CertChoice::Ca { secret: s, .. } if s.to_bytes() == secret.to_bytes()), "the kept root is read back");
        // The environment's root wins over nothing kept, and is kept.
        let other = tempdir_in(&dir, "env-root");
        let given = rdm_core::certs::mint_key();
        let read = CertChoice::from_material(&other, "mesh1", false, env(vec![(ENV_CA_SECRET, rdm_core::certs::encode_key(&given))])).unwrap();
        assert!(matches!(&read, CertChoice::Ca { secret: s, .. } if s.to_bytes() == given.to_bytes()));
        assert_eq!(std::fs::read_to_string(other.join(CA_SECRET_FILE)).unwrap(), rdm_core::certs::encode_key(&given));
        // A launched admin: the root with its mesh's material; refused for another mesh, and refused by
        // name without the root.
        let material = CertIssuer::with_root(given, None, time_at(1)).issuing_material("mesh2").unwrap().unwrap();
        let launch = |root: bool| {
            let mut pairs = vec![(ENV_ISSUING_KEY, material.key.0.clone()), (ENV_ISSUING_CERT, material.cert.clone())];
            if root {
                pairs.push((ENV_CA_SECRET, material.root.0.clone()));
            }
            env(pairs)
        };
        assert!(matches!(CertChoice::from_material(&tempdir_in(&dir, "launched"), "mesh2", false, launch(true)), Ok(CertChoice::Ca { issuing: Some(_), .. })));
        let wrong = CertChoice::from_material(&tempdir_in(&dir, "wrong-mesh"), "mesh3", false, launch(true)).err().unwrap();
        assert!(wrong.contains("mesh mesh2's") && wrong.contains("mesh mesh3"), "{wrong}");
        let rootless = CertChoice::from_material(&tempdir_in(&dir, "rootless"), "mesh2", false, launch(false)).err().unwrap();
        assert!(rootless.contains("set without RDM_CA_SECRET") && rootless.contains("§Custody"), "{rootless}");
        // None, not day 0: refused by name; undecodable: refused naming the variable.
        let none = CertChoice::from_material(&tempdir_in(&dir, "none"), "mesh2", false, env(vec![])).err().unwrap();
        assert!(none.contains("holds no cert material") && none.contains(ENV_CA_SECRET), "{none}");
        let bad = CertChoice::from_material(&tempdir_in(&dir, "bad"), "mesh1", false, env(vec![(ENV_CA_SECRET, "zz".into())])).err().unwrap();
        assert!(bad.starts_with(ENV_CA_SECRET), "{bad}");
    }

    /// CONTRACT (fabric-certs.md §Custody: "Every node-admin holds the root secret ... written into its
    /// launch environment beside its mesh's issuing material"; "a successor seated by election ...
    /// mints and rotates from its own copy"): a root holder writes the root beside a mesh's issuing
    /// material into every admin's launch. An admin launched with both holds the root and its mesh's
    /// one issuer: it hands that same issuer on, never minting a second, and mints another mesh's
    /// issuer from its own copy of the root (a successor fabric-primary's mesh.create). Issuing material
    /// its root did not sign is refused by name. Must NOT happen: a launched admin minting a second
    /// issuer for its own mesh.
    #[test]
    fn a_launched_admin_holds_the_root_beside_its_meshs_issuer_and_mints_no_second_one() {
        let dir = crate::test_support::tempdir("cert-root-launch");
        let root = rdm_core::certs::mint_key();
        let material = CertIssuer::with_root(root.clone(), None, time_at(1_000)).issuing_material("mesh1").unwrap().unwrap();
        assert_eq!(material.root, SecretHex(rdm_core::certs::encode_key(&root)), "the root rides beside the issuing material");
        let launch_env = |m: &IssuingMaterial| {
            let m = m.clone();
            move |k: &str| match k {
                ENV_ISSUING_KEY => Some(m.key.0.clone()),
                ENV_ISSUING_CERT => Some(m.cert.clone()),
                ENV_CA_SECRET => Some(m.root.0.clone()),
                _ => None,
            }
        };
        let choice = CertChoice::from_material(&dir, "mesh1", false, launch_env(&material)).unwrap();
        assert!(
            matches!(&choice, CertChoice::Ca { secret, issuing: Some(k) } if secret.to_bytes() == root.to_bytes() && rdm_core::certs::encode_issuing_cert(k.cert()) == material.cert),
            "the root, with its mesh's issuer as given"
        );
        let launched = choice.issuer(time_at(2_000)).unwrap();
        assert_eq!(launched.root(), Some(root.verifying_key()));
        assert_eq!(launched.issuing_material("mesh1").unwrap(), Some(material.clone()), "its mesh's one issuer handed on with the root, never a second");
        let mesh2 = launched.issuing_material("mesh2").unwrap().expect("another mesh's issuer, minted from its own copy of the root");
        assert_eq!(rdm_core::certs::verify_issuer(&rdm_core::certs::decode_issuing_cert(&mesh2.cert).unwrap(), &root.verifying_key()), Ok(()));
        // Issuing material another root signed, beside this root: refused by name.
        let stranger = CertIssuer::with_root(rdm_core::certs::mint_key(), None, time_at(1_000)).issuing_material("mesh1").unwrap().unwrap();
        let mixed = IssuingMaterial { root: material.root.clone(), ..stranger };
        let refused = CertChoice::from_material(&tempdir_in(&dir, "mixed"), "mesh1", false, launch_env(&mixed)).err().expect("refused");
        assert!(refused.contains("issuer-bad-signature") && refused.contains("not signed by the fabric's root"), "{refused}");
    }

    fn tempdir_in(dir: &Path, name: &str) -> std::path::PathBuf {
        let d = dir.join(name);
        std::fs::create_dir_all(&d).unwrap();
        d
    }
}
