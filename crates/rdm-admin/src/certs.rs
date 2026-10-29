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

/// Birth identity and certificate issuance error types (`docs/architecture/fabric-certs.md`).
pub use rdm_core::certs::{BirthIdentity, CertError, IssueFailure};
use rdm_core::certs::{CaKey, IssuingKey, SigningKey, VerifyingKey};
use rdm_core::launch::{IssuingMaterial, ENV_FABRIC_CA_KEY, ENV_ISSUING_CERT, ENV_ISSUING_KEY};
use rdm_transport::clock::RafkaTime;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

/// The fabric's root, given in the environment (fabric-certs.md "Where a node-admin's signing
/// material comes from"), as hex.
pub const ENV_CA_SECRET: &str = rdm_core::launch::ENV_CA_SECRET;

/// The file in a node-admin's data dir that keeps the fabric's root it holds.
pub const CA_SECRET_FILE: &str = "ca-secret";
/// The next root's secret a node-admin holds through a root rotation, beside `ca-secret`, from its
/// `distributed` phase to its `retired` one, when it becomes `ca-secret` (fabric-ca-renew.md).
pub const NEXT_SECRET_FILE: &str = "ca-secret-next";

/// The file in a node-admin's data dir that keeps the fabric CA it holds: the CA's key and its
/// root-signed cert, as hex, one per line.
pub const FABRIC_CA_FILE: &str = "fabric-ca";

/// What a node-admin is started with for certs (`docs/architecture/fabric-certs.md`
/// "How RDM is instantiated"). There is no default: [`CertChoice::Unchosen`] is refused at start by
/// name.
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
    /// one issuer, which must be `own_mesh`'s and chain to that root); else, for the bootstrap admin of
    /// day 0 (`day_zero`: no launch and no seeds), a root minted now. A root the environment gives or
    /// this call mints is kept as `ca-secret` in the data dir when none is kept there yet, so the same
    /// admin started again holds the same root. The fabric CA a launch carries beside them
    /// (`RDM_FABRIC_CA_KEY`) is kept as `fabric-ca`, which [`Self::issuer`] reads. Material that does
    /// not decode, issuing material without the root or the CA key, and an admin that is not day 0's
    /// and holds no root, are refused by name.
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
        let issuing = rdm_core::launch::decode_issuing(get(ENV_ISSUING_KEY), get(ENV_ISSUING_CERT), get(ENV_CA_SECRET), get(ENV_FABRIC_CA_KEY))?;
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
        let secret = match root {
            Some(secret) => secret,
            None if day_zero => {
                let secret = rdm_core::certs::mint_key();
                keep(&secret)?;
                tracing::info_span!("rdm.node_admin.cert.create.via-day-zero-root", mesh = own_mesh, kept = %kept.display())
                    .in_scope(|| tracing::info!("the bootstrap admin holds no root and has no seeds: it minted the fabric's root (fabric.ca.create)"));
                secret
            }
            None => {
                return Err(format!(
                    "this node-admin holds no cert material: {ENV_CA_SECRET} or a `{CA_SECRET_FILE}` in {} (fabric-certs.md: an admin with seeds or an anchor and no root refuses to start)",
                    data_dir.display(),
                ))
            }
        };
        let key = match (&issuing, issuing.as_ref().map(|m| issuing_key(m, own_mesh, &secret.verifying_key())).transpose()?) {
            (Some(m), Some(k)) => {
                keep_ca(data_dir, &ca_key(m, &k)?)?;
                Some(k)
            }
            _ => None,
        };
        Ok(Self::Ca { secret, issuing: key })
    }

    /// [`Self::from_material`] for the node-admin `cfg` configures, from this process's environment:
    /// day 0 is a bootstrap admin with no launch, no seeds and no recovery flag, so a person-started
    /// recovery never mints a root of its own.
    pub fn from_config(cfg: &crate::admin::AdminConfig) -> Result<Self, String> {
        Self::from_material(&cfg.data_dir, &cfg.mesh, day_zero(cfg), |k| std::env::var(k).ok())
    }

    /// The issuer this choice makes, issuing on `time`. A root holder issues under the fabric CA its
    /// data dir keeps (`fabric-ca`: its launch's, or its own day 0's); on day 0 (`day_zero`) with none
    /// kept, it mints the CA now under the root and keeps it (`fabric.ca.create`, step 1 of
    /// fabric.create). A root holder with no CA off day 0, a kept CA that is not the one its mesh's
    /// issuer chains to, and the unchosen choice, are refused by name.
    pub fn issuer(self, data_dir: &Path, day_zero: bool, time: RafkaTime) -> Result<CertIssuer, String> {
        match self {
            Self::Unchosen => Err("this node-admin was started with no cert choice: set Wiring::certs to CertChoice::Ca or CertChoice::NoCerts".into()),
            Self::NoCerts => Ok(CertIssuer::no_certs(time)),
            Self::Ca { secret, issuing } => {
                let kept = data_dir.join(FABRIC_CA_FILE);
                let ca = match read_ca(&kept)? {
                    Some(ca) => ca,
                    None if day_zero => {
                        // Day 0's rafka-time is this admin's own clock, which it adopts at boot (rafka_time.rs).
                        let now_ms = rdm_transport::clock::Clock::now_rafka_ms(&rdm_transport::clock::OsClock);
                        let ca = CaKey::mint(&secret, rdm_core::certs::CA_CERT_TTL_SECS, now_ms);
                        keep_ca(data_dir, &ca)?;
                        tracing::info_span!("rdm.node_admin.cert.create.via-ca-create", ca_expires_at_ms = ca.expiry_ms(), kept = %kept.display())
                            .in_scope(|| tracing::info!("the bootstrap admin minted the fabric CA under the root (fabric.ca.create, step 1 of fabric.create)"));
                        ca
                    }
                    None => return Err(format!("this node-admin holds the root and no fabric CA: no `{FABRIC_CA_FILE}` in {} and no {ENV_FABRIC_CA_KEY} in its launch (fabric-certs.md §Custody: every admin holds the current CA material)", data_dir.display())),
                };
                if let Some(k) = &issuing {
                    if &k.cert().ca != ca.cert() {
                        return Err(format!("{}: the kept fabric CA is not the CA its mesh's issuer chains to", kept.display()));
                    }
                }
                let issuer = CertIssuer::with_root(secret, ca, issuing, time);
                let _ = issuer.kept.set(data_dir.to_path_buf());
                // A rotation's next root kept before a restart is held again (fabric-ca-renew.md).
                match std::fs::read_to_string(data_dir.join(NEXT_SECRET_FILE)) {
                    Ok(hex) => *issuer.next_root.lock().unwrap() = Some(rdm_core::certs::decode_key(hex.trim()).map_err(|e| format!("{}: {e}", data_dir.join(NEXT_SECRET_FILE).display()))?),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(format!("{}: the next root this data dir keeps cannot be read: {e}", data_dir.join(NEXT_SECRET_FILE).display())),
                }
                Ok(issuer)
            }
        }
    }
}

/// The issuing key `issuing` carries, which must be `own_mesh`'s, must be the key its cert names, and
/// must have a chain that verifies to `root` (its CA signed by the root, the issuing cert by its CA).
fn issuing_key(issuing: &IssuingMaterial, own_mesh: &str, root: &VerifyingKey) -> Result<IssuingKey, String> {
    let key = rdm_core::certs::decode_key(&issuing.key.0).map_err(|e| format!("{ENV_ISSUING_KEY}: {e}"))?;
    let cert = rdm_core::certs::decode_issuing_cert(&issuing.cert).map_err(|e| format!("{ENV_ISSUING_CERT}: {e}"))?;
    if cert.cert.mesh != own_mesh {
        return Err(format!("{ENV_ISSUING_CERT}: the issuing material is mesh {}'s, and this node-admin is of mesh {own_mesh}", cert.cert.mesh));
    }
    if cert.cert.issuer_pub != key.verifying_key() {
        return Err(format!("{ENV_ISSUING_KEY}: the issuing key is not the key its cert names"));
    }
    rdm_core::certs::verify_issuer(&cert, std::slice::from_ref(root)).map_err(|e| format!("{ENV_ISSUING_CERT}: {e}: the issuing cert's chain does not verify to the fabric's root this node-admin holds ({ENV_CA_SECRET})"))?;
    Ok(IssuingKey::from_parts(key, cert))
}

/// The fabric CA `issuing` carries beside its root (`RDM_FABRIC_CA_KEY`), whose cert is `key`'s own
/// CA cert: the key must be the one that cert names.
fn ca_key(issuing: &IssuingMaterial, key: &IssuingKey) -> Result<CaKey, String> {
    let secret = rdm_core::certs::decode_key(&issuing.ca_key.0).map_err(|e| format!("{ENV_FABRIC_CA_KEY}: {e}"))?;
    if secret.verifying_key() != key.cert().ca.cert.ca_pub {
        return Err(format!("{ENV_FABRIC_CA_KEY}: the CA key is not the key the issuing cert's CA cert names"));
    }
    Ok(CaKey::from_parts(secret, key.cert().ca.clone()))
}

/// Whether the node-admin `cfg` configures is day 0's bootstrap admin: no launch, no adoption, no seeds
/// and no recovery flag, so a person-started recovery or adoption launch never mints a root or a CA of
/// its own: it is given them, from its environment or an inherited data dir, or refused by name (Q156).
pub fn day_zero(cfg: &crate::admin::AdminConfig) -> bool {
    cfg.launch.is_none() && cfg.adopt.is_none() && cfg.seeds.is_empty() && !cfg.mesh_primary && !cfg.fabric_primary
}

/// Keep `ca` as the fabric CA `data_dir` holds (`fabric-ca`: its key and its cert, hex, one per line),
/// in place of any kept before: the CA a launch gives an admin is the fabric's current one.
pub fn keep_ca(data_dir: &Path, ca: &CaKey) -> Result<(), String> {
    let path = data_dir.join(FABRIC_CA_FILE);
    std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: the data dir the fabric CA is kept in: {e}", data_dir.display()))?;
    let text = format!("{}\n{}\n", rdm_core::certs::encode_key(ca.secret()), rdm_core::certs::encode_ca_cert(ca.cert()));
    std::fs::write(&path, text).map_err(|e| format!("{}: the fabric CA could not be kept: {e}", path.display()))
}

/// The fabric CA the data dir keeps at `path` (`fabric-ca`), when one is kept: its key must be the one
/// its cert names.
fn read_ca(path: &Path) -> Result<Option<CaKey>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: the fabric CA this data dir keeps cannot be read: {e}", path.display())),
    };
    let mut lines = text.lines();
    let (Some(key), Some(cert)) = (lines.next(), lines.next()) else {
        return Err(format!("{}: the kept fabric CA is not its key and its cert, one per line", path.display()));
    };
    let secret = rdm_core::certs::decode_key(key).map_err(|e| format!("{}: the kept CA key: {e}", path.display()))?;
    let cert = rdm_core::certs::decode_ca_cert(cert).map_err(|e| format!("{}: the kept CA cert: {e}", path.display()))?;
    if secret.verifying_key() != cert.cert.ca_pub {
        return Err(format!("{}: the kept CA key is not the key its cert names", path.display()));
    }
    Ok(Some(CaKey::from_parts(secret, cert)))
}

/// What an admin signs with.
#[derive(Clone)]
enum Material {
    /// No certs, by name.
    NoCerts,
    /// The fabric's root and the fabric CA under it: every mesh's issuer can be minted.
    Root(SigningKey, CaKey),
}

/// What [`CertIssuer::install_ca`] did with a fabric CA `node.issuer.update` carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaInstall {
    /// Newer than the one held: held and kept from now on.
    Installed {
        /// The expiry of the CA it replaced.
        previous_expires_at: u64,
    },
    /// The CA held already, or an admin configured with no certs: nothing changes.
    Held,
    /// Older than the one held: refused `ca-older`, nothing installed.
    Older {
        /// The expiry of the CA held.
        held_expires_at: u64,
    },
}

/// One member cert issue (fabric-certs.md §Tracking): the bytes the birth is handed, and what its cert
/// row records of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberIssue {
    /// The signed cert's bytes, as `JoinControl::member_cert` and `RenewCert` carry them.
    pub bytes: Vec<u8>,
    /// When it was issued, on the issuing admin's rafka-time.
    pub issued_at: u64,
    /// When it expires: the policy's member TTL from `issued_at`, cut to its issuer's expiry.
    pub expires_at: u64,
    /// When its issuing cert expires.
    pub issuer_expires_at: u64,
    /// When its issuing authority renews it: the policy's point plus the birth's fixed jitter
    /// ([`rdm_core::certs::renew_at`]).
    pub renew_at: u64,
    /// The mesh issuer this issue minted first (`mesh.issuer.create`), whose row the caller puts.
    pub minted: Option<IssuerMint>,
}

/// A mesh issuer minted under the root (`mesh.issuer.create`, `mesh.issuer.renew`): what the mesh's
/// issuer row records of it (mesh-issuer-renew.md step 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuerMint {
    /// The mesh it signs for.
    pub mesh: String,
    /// When it was minted, on the minting admin's rafka-time.
    pub issued_at: u64,
    /// When its issuing cert expires.
    pub expires_at: u64,
    /// When the fabric-primary renews it: the policy's point of its life plus its fixed jitter.
    pub renew_at: u64,
}

/// A node-admin's cert material together with the rafka-time it issues on: what the join door and
/// the deployment executor call.
pub struct CertIssuer {
    /// The root and the fabric CA under it: renewed in place by `fabric.ca.renew`.
    material: std::sync::RwLock<Material>,
    time: RafkaTime,
    /// The issuing key of each mesh this admin signs for: minted once under the fabric CA, or the one
    /// it was given.
    issuers: Mutex<BTreeMap<String, IssuingKey>>,
    /// The mint of each issuing key this admin minted, by mesh: the dates the fabric-primary writes on
    /// a node-admin's row when that admin holds it.
    mints: Mutex<BTreeMap<String, IssuerMint>>,
    /// The fabric's cert policy this admin issues under: the defaults until it is set.
    policy: Mutex<rdm_core::certs::CertPolicy>,
    /// The data dir its fabric CA is kept beside (`fabric-ca`), once known: a renewed or installed CA
    /// is kept there, so a restart reads back the current one (fabric-certs.md §Custody).
    kept: std::sync::OnceLock<std::path::PathBuf>,
    /// The next root a root rotation carried to this admin (`ca-secret-next`), until it retires the
    /// old one (fabric-ca-renew.md "fabric.root.update").
    next_root: Mutex<Option<SigningKey>>,
}

impl CertIssuer {
    /// The explicit choice of no certs, on `time`.
    pub fn no_certs(time: RafkaTime) -> Self {
        Self { material: std::sync::RwLock::new(Material::NoCerts), time, issuers: Mutex::default(), mints: Mutex::default(), policy: Mutex::default(), kept: std::sync::OnceLock::new(), next_root: Mutex::default() }
    }

    /// An issuer holding the fabric's root `secret`, the fabric CA `ca`, and its own mesh's issuing key
    /// `issuing` when its launch gave it one, on `time`.
    pub fn with_root(secret: SigningKey, ca: CaKey, issuing: Option<IssuingKey>, time: RafkaTime) -> Self {
        let issuers = issuing.into_iter().map(|k| (k.mesh().to_string(), k)).collect();
        Self { material: std::sync::RwLock::new(Material::Root(secret, ca)), time, issuers: Mutex::new(issuers), mints: Mutex::default(), policy: Mutex::default(), kept: std::sync::OnceLock::new(), next_root: Mutex::default() }
    }

    /// Hold `policy`, the fabric's cert policy, from now on (fabric-certs.md §Tracking: read once by
    /// the bootstrap admin, carried to every later admin in its entry pull). Until it is set, the
    /// policy is the defaults.
    pub fn set_policy(&self, policy: rdm_core::certs::CertPolicy) {
        *self.policy.lock().unwrap() = policy;
    }

    /// The cert policy this admin issues under.
    pub fn policy(&self) -> rdm_core::certs::CertPolicy {
        *self.policy.lock().unwrap()
    }

    /// Whether this admin was started with no certs: it issues nothing and renews nothing.
    pub fn is_no_certs(&self) -> bool {
        matches!(self.material(), Material::NoCerts)
    }

    /// This admin's rafka-time now, when it holds one: what its issues and renewals are judged on.
    pub fn now_ms(&self) -> Option<u64> {
        self.time.try_now_ms()
    }

    /// The member cert of `birth` issued at its join, with what the birth's cert row records of it:
    /// [`Self::member`], spanned `rdm.node_admin.cert.create.via-join`.
    pub fn issue_member(&self, birth: &BirthIdentity) -> Result<MemberIssue, IssueFailure> {
        if matches!(self.material(), Material::NoCerts) {
            let now = self.time.try_now_ms().unwrap_or(0);
            tracing::info_span!("rdm.node_admin.cert.create.via-no-certs-configured", node = %birth.name, node_id = %birth.node_id, incarnation_id = %birth.incarnation.as_str(), issued_at_rafka_ms = now)
                .in_scope(|| tracing::info!("this node-admin is configured with no certs: the member cert is empty"));
            return Ok(MemberIssue { bytes: Vec::new(), issued_at: now, expires_at: 0, issuer_expires_at: 0, renew_at: 0, minted: None });
        }
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
        let issued = self.now("a member cert").and_then(|now_ms| {
            span.record("issued_at_rafka_ms", now_ms);
            self.sign(birth, now_ms)
        });
        match &issued {
            Ok(i) => {
                span.record("issuer_expires_at_rafka_ms", i.issuer_expires_at);
                span.record("expires_at_rafka_ms", i.expires_at);
                span.record("cert_len", i.bytes.len() as u64);
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

    /// A fresh member cert of `birth`, issued now for its renewal (node-cert-renew.md step 2).
    pub fn renewal(&self, birth: &BirthIdentity) -> Result<MemberIssue, IssueFailure> {
        let now_ms = self.now("a member cert")?;
        self.sign(birth, now_ms)
    }

    /// Sign `birth`'s member cert as issued at `issued_at`: expiring the policy's member TTL from it,
    /// cut to its issuer's expiry when the issuer expires first (fabric-certs.md §Tracking: "A member
    /// cert never outlives its issuer").
    fn sign(&self, birth: &BirthIdentity, issued_at: u64) -> Result<MemberIssue, IssueFailure> {
        let policy = self.policy();
        let (issuer, minted) = self.issuer_for(&birth.mesh, issued_at)?;
        let wanted = issued_at.saturating_add(policy.member_ttl_secs.saturating_mul(1000));
        let expiry_ms = wanted.min(issuer.expiry_ms());
        let signed = rdm_core::certs::issue(&issuer, birth, issued_at, expiry_ms).map_err(|error| IssueFailure::Refused { error, detail: format!("the member cert of {} ({} {})", birth.name, birth.node_id, birth.incarnation.as_str()) })?;
        let dates = member_cert_dates(&signed, policy.renew_at_permille);
        Ok(MemberIssue { bytes: rdm_core::certs::cert_to_bytes(&signed), issued_at: dates.issued_at, expires_at: dates.expires_at, issuer_expires_at: dates.issuer_expires_at, renew_at: dates.renew_at, minted })
    }

    /// The issuing key this admin holds for `mesh`, if any.
    pub fn held_issuer(&self, mesh: &str) -> Option<IssuingKey> {
        self.issuers.lock().unwrap().get(mesh).cloned()
    }

    /// The mint of the issuing key this admin holds for `mesh`, when this admin minted it.
    pub fn held_mint(&self, mesh: &str) -> Option<IssuerMint> {
        let held = self.held_issuer(mesh)?;
        self.mints.lock().unwrap().get(mesh).filter(|m| m.expires_at == held.expiry_ms()).cloned()
    }

    /// `mesh.issuer.renew` step 1: a new issuing key of `mesh` minted under the fabric CA now, held from
    /// now on in place of the one held.
    pub fn renew_issuer(&self, mesh: &str) -> Result<(IssuingKey, IssuerMint), IssueFailure> {
        let now_ms = self.now("a mesh's issuing material")?;
        let material = self.material();
        let Material::Root(_, ca) = &material else {
            unreachable!("a no-certs admin renews no issuer of mesh {mesh}: its caller reads is_no_certs first")
        };
        let (key, mint) = self.mint_issuer(ca, mesh, now_ms)?;
        self.issuers.lock().unwrap().insert(mesh.to_string(), key.clone());
        self.mints.lock().unwrap().insert(mesh.to_string(), mint.clone());
        Ok((key, mint))
    }

    /// `node.issuer.update` step 3's install: hold `key` as its mesh's issuing key from now on. The
    /// caller has checked it is its own mesh's and that the root verifies it. Returns the expiry of the
    /// one it replaces.
    pub fn install_issuer(&self, key: IssuingKey) -> Option<u64> {
        self.issuers.lock().unwrap().insert(key.mesh().to_string(), key).map(|old| old.expiry_ms())
    }

    /// The one place an issuing key is minted (`mesh.issuer.create`, `mesh.issuer.renew`): under the
    /// fabric CA `ca`, at `now_ms`, living the policy's issuing TTL cut to the CA's expiry.
    fn mint_issuer(&self, ca: &CaKey, mesh: &str, now_ms: u64) -> Result<(IssuingKey, IssuerMint), IssueFailure> {
        let policy = self.policy();
        let expiry_ms = now_ms.saturating_add(policy.issuing_ttl_secs.saturating_mul(1000)).min(ca.expiry_ms());
        let key = IssuingKey::mint(ca, mesh, now_ms, expiry_ms).map_err(|error| IssueFailure::Refused { error, detail: format!("mesh {mesh}'s issuing cert under the fabric CA expiring at {}", ca.expiry_ms()) })?;
        let dates = signing_cert_dates(key.cert(), policy.renew_at_permille);
        let mint = IssuerMint { mesh: mesh.to_string(), issued_at: dates.issued_at, expires_at: dates.expires_at, renew_at: dates.renew_at };
        Ok((key, mint))
    }

    fn material(&self) -> Material {
        self.material.read().unwrap().clone()
    }

    /// The fabric CA's expiry and renew point (fabric-ca-renew.md "When": once the CA has lived
    /// `RDM_CERT_RENEW_AT` of its TTL), when this admin holds the root.
    pub fn ca_dates(&self) -> Option<(u64, u64, u64)> {
        let Material::Root(_, ca) = self.material() else { return None };
        // The CA cert names its expiry alone; it was minted the policy's CA TTL before it.
        let expires = ca.expiry_ms();
        let issued = expires.saturating_sub(self.policy().ca_ttl_secs.saturating_mul(1000));
        let renew_at = issued.saturating_add(expires.saturating_sub(issued).saturating_mul(self.policy().renew_at_permille as u64) / 1000);
        Some((issued, expires, renew_at))
    }

    /// `fabric.ca.renew` step 1 (fabric-ca-renew.md): a new fabric CA keypair, its cert signed by the
    /// root this admin holds, living the policy's CA TTL from now, held from now on in place of the one
    /// held. Returns the new CA and the expiry of the one it replaces; `None` for an admin configured
    /// with no certs. Every mesh's issuer minted under the old CA is due from then on
    /// ([`Self::issuer_under_current_ca`]).
    pub fn renew_ca(&self) -> Result<Option<(CaKey, u64)>, IssueFailure> {
        let now_ms = self.now("the fabric CA")?;
        let mut material = self.material.write().unwrap();
        let Material::Root(root, old) = &*material else {
            return Ok(None);
        };
        let ca = CaKey::mint(root, self.policy().ca_ttl_secs, now_ms);
        let previous = old.expiry_ms();
        *material = Material::Root(root.clone(), ca.clone());
        self.keep(&ca);
        Ok(Some((ca, previous)))
    }

    /// The key, as hex, of the fabric CA `issuing` is minted under, when it is the CA this admin holds:
    /// what `node.issuer.update` carries beside the issuing material (fabric-ca-renew.md step 2, Q275).
    pub fn ca_key_of(&self, issuing: &IssuingKey) -> Option<String> {
        let Material::Root(_, ca) = self.material() else { return None };
        (ca.cert() == &issuing.cert().ca).then(|| rdm_core::certs::encode_key(ca.secret()))
    }

    /// `node.issuer.update`'s install of the fabric CA `ca` (its key and its cert, which chains to the root
    /// the caller verified): held from now on, and kept beside the data dir, when it is newer than the one
    /// held. The CA held already is [`CaInstall::Held`]; an older one is [`CaInstall::Older`] and nothing
    /// is installed (node-issuer-update.md, `ca-older`; Q275).
    pub fn install_ca(&self, ca: CaKey) -> CaInstall {
        let mut material = self.material.write().unwrap();
        let Material::Root(root, held) = &*material else { return CaInstall::Held };
        if held.cert() == ca.cert() {
            return CaInstall::Held;
        }
        if ca.expiry_ms() <= held.expiry_ms() {
            return CaInstall::Older { held_expires_at: held.expiry_ms() };
        }
        let previous = held.expiry_ms();
        *material = Material::Root(root.clone(), ca.clone());
        drop(material);
        self.keep(&ca);
        CaInstall::Installed { previous_expires_at: previous }
    }

    /// The next root's secret this admin holds through a rotation, as hex: what `node.roots.update`
    /// carries to every other node-admin (fabric-ca-renew.md step 2).
    pub fn next_root_secret(&self) -> Option<String> {
        self.next_root.lock().unwrap().as_ref().map(rdm_core::certs::encode_key)
    }

    /// The next root this admin holds through a rotation, as its public key.
    pub fn next_root(&self) -> Option<VerifyingKey> {
        self.next_root.lock().unwrap().as_ref().map(|k| k.verifying_key())
    }

    /// Hold `secret` as the next root, through a rotation's `distributed` phase to its `retired` one, and
    /// keep it beside the data dir (`ca-secret-next`): an admin started again holds it, the way it
    /// holds `ca-secret` (fabric-ca-renew.md step 2).
    pub fn hold_next_root(&self, secret: SigningKey) -> Result<(), String> {
        if let Some(dir) = self.kept.get() {
            let path = dir.join(NEXT_SECRET_FILE);
            std::fs::write(&path, rdm_core::certs::encode_key(&secret)).map_err(|e| format!("{}: the next root could not be kept: {e}", path.display()))?;
        }
        *self.next_root.lock().unwrap() = Some(secret);
        Ok(())
    }

    /// `fabric.root.update` step 3: a new fabric CA signed by the next root this admin holds, held from
    /// now on and kept; every mesh's issuer minted under the old CA is due from then on. The root it
    /// signs with stays the next root until the rotation retires the old one. Returns the new CA and
    /// the expiry of the one it replaces; `None` without a next root or certs.
    pub fn renew_ca_under_next_root(&self) -> Result<Option<(CaKey, u64)>, IssueFailure> {
        let now_ms = self.now("the fabric CA")?;
        let Some(next) = self.next_root.lock().unwrap().clone() else { return Ok(None) };
        let mut material = self.material.write().unwrap();
        let Material::Root(root, old) = &*material else { return Ok(None) };
        let ca = CaKey::mint(&next, self.policy().ca_ttl_secs, now_ms);
        let previous = old.expiry_ms();
        *material = Material::Root(root.clone(), ca.clone());
        drop(material);
        self.keep(&ca);
        Ok(Some((ca, previous)))
    }

    /// `fabric.root.update` step 5 at this admin: `secret` is the fabric's root from now on, kept as
    /// `ca-secret`; the old root's secret and the next root are destroyed (fabric-ca-renew.md).
    pub fn replace_root_with(&self, secret: SigningKey) -> Result<(), String> {
        if let Some(dir) = self.kept.get() {
            let path = dir.join(CA_SECRET_FILE);
            std::fs::write(&path, rdm_core::certs::encode_key(&secret)).map_err(|e| format!("{}: the new root could not be kept: {e}", path.display()))?;
            let _ = std::fs::remove_file(dir.join(NEXT_SECRET_FILE));
        }
        let mut material = self.material.write().unwrap();
        if let Material::Root(_, ca) = &*material {
            *material = Material::Root(secret, ca.clone());
        }
        *self.next_root.lock().unwrap() = None;
        Ok(())
    }

    /// Keep `ca` beside the data dir, when it is known; a write that fails is named and the CA stays held.
    fn keep(&self, ca: &CaKey) {
        if let Some(dir) = self.kept.get() {
            if let Err(e) = keep_ca(dir, ca) {
                tracing::warn!(error = %e, "the fabric CA is held but not kept: a restart reads back the one kept before");
            }
        }
    }

    /// Whether the issuing key this admin holds for `mesh` was minted under the fabric CA it holds now:
    /// one minted under an older CA is due for `mesh.issuer.renew` (fabric-ca-renew.md step 2).
    pub fn issuer_under_current_ca(&self, mesh: &str) -> bool {
        let Material::Root(_, ca) = self.material() else { return true };
        self.held_issuer(mesh).is_none_or(|k| k.cert().ca.cert == ca.cert().cert)
    }

    /// The fabric root's public key, when this admin holds the root.
    pub fn root(&self) -> Option<VerifyingKey> {
        match &self.material() {
            Material::Root(secret, _) => Some(secret.verifying_key()),
            Material::NoCerts => None,
        }
    }

    fn now(&self, what: &str) -> Result<u64, IssueFailure> {
        self.time.try_now_ms().ok_or_else(|| IssueFailure::NoRafkaTime(format!("this admin holds no rafka-time to issue {what} on")))
    }

    /// The issuing key of `mesh`: the one held, or one minted now under the fabric CA
    /// (`mesh.issuer.create`), cut to the CA's expiry, and held from then on.
    fn issuer_for(&self, mesh: &str, now_ms: u64) -> Result<(IssuingKey, Option<IssuerMint>), IssueFailure> {
        let mut issuers = self.issuers.lock().unwrap();
        if let Some(key) = issuers.get(mesh) {
            return Ok((key.clone(), None));
        }
        match &self.material() {
            Material::Root(_, ca) => {
                let (key, mint) = self.mint_issuer(ca, mesh, now_ms)?;
                tracing::info_span!("rdm.node_admin.cert.create.via-mesh-birth", mesh, issued_at_rafka_ms = now_ms, issuer_expires_at_rafka_ms = key.expiry_ms(), outcome = "issued")
                    .in_scope(|| tracing::info!("the mesh's issuing key is minted under the fabric CA (mesh.issuer.create)"));
                issuers.insert(mesh.to_string(), key.clone());
                self.mints.lock().unwrap().insert(mesh.to_string(), mint.clone());
                Ok((key, Some(mint)))
            }
            Material::NoCerts => unreachable!("a no-certs issuer issues nothing"),
        }
    }

    /// The member cert of the exact birth `birth`, as `JoinControl::member_cert` carries it: signed by
    /// `birth`'s mesh's issuing key, expiring the policy's member TTL from now on this admin's
    /// rafka-time, cut to its issuer's expiry. Empty when configured with no certs.
    pub fn member(&self, birth: &BirthIdentity) -> Result<Vec<u8>, IssueFailure> {
        self.issue_member(birth).map(|i| i.bytes)
    }

    /// The issuing material of `mesh` a node-admin of it is launched with (`RDM_ISSUING_KEY`,
    /// `RDM_ISSUING_CERT`), with the root and the fabric CA's key beside it (`RDM_CA_SECRET`,
    /// `RDM_FABRIC_CA_KEY`; fabric-certs.md §Custody); `None` when configured with no certs.
    pub fn issuing_material(&self, mesh: &str) -> Result<Option<IssuingMaterial>, IssueFailure> {
        self.issuing_material_minting(mesh).map(|(m, _)| m)
    }

    /// [`Self::issuing_material`], with the issuer it minted first, if any (`mesh.issuer.create`).
    pub fn issuing_material_minting(&self, mesh: &str) -> Result<(Option<IssuingMaterial>, Option<IssuerMint>), IssueFailure> {
        if matches!(self.material(), Material::NoCerts) {
            tracing::info_span!("rdm.node_admin.cert.create.via-no-certs-configured", mesh)
                .in_scope(|| tracing::info!("this node-admin is configured with no certs: the admin is launched with no issuing material"));
            return Ok((None, None));
        }
        let now_ms = self.now("a mesh's issuing material")?;
        let (key, minted) = self.issuer_for(mesh, now_ms)?;
        let material = self.material();
        let Material::Root(root, ca) = &material else { unreachable!("a no-certs issuer hands no issuing material") };
        Ok((Some(IssuingMaterial::of(&key, root, ca)), minted))
    }
}

/// The dates of the member cert `signed` its cert row records (fabric-certs.md §Tracking; Q141, Q241):
/// issued and expiring as the cert says, its issuer's expiry, and the renew point derived from those
/// and the fabric's `renew_at_permille` alone, so its issuer, its installer and any successor hold one
/// value. Never a clock's reading.
pub fn member_cert_dates(signed: &rdm_core::certs::SignedCert, renew_at_permille: u32) -> crate::storage::CertDates {
    let (issued_at, expires_at) = (signed.cert.issued_ms, signed.cert.expiry_ms);
    crate::storage::CertDates { issued_at, expires_at, issuer_expires_at: signed.issuer.cert.expiry_ms, renew_at: rdm_core::certs::renew_at(&signed.cert.node_id, &signed.cert.incarnation, issued_at, expires_at, renew_at_permille) }
}

/// The dates of the issuing cert `signed` an admin's signing cert row records (node-issuer-update.md
/// step 4; Q241): issued and expiring as the cert says, and the renew point derived from those and the
/// fabric's `renew_at_permille` alone. Never a clock's reading.
pub fn signing_cert_dates(signed: &rdm_core::certs::SignedIssuingCert, renew_at_permille: u32) -> crate::storage::SigningCertDates {
    let (issued_at, expires_at) = (signed.cert.issued_ms, signed.cert.expiry_ms);
    crate::storage::SigningCertDates { issued_at, expires_at, renew_at: issuer_renew_at(&signed.cert.mesh, issued_at, expires_at, renew_at_permille) }
}

/// When the fabric-primary renews `mesh`'s issuer minted at `issued_at`, expiring at `expires_at`:
/// the policy's point of its life plus a jitter fixed for that issue, up to 5 % (mesh-issuer-renew.md;
/// [`rdm_core::certs::renew_at`]'s rule, over the mesh's name).
pub fn issuer_renew_at(mesh: &str, issued_at: u64, expires_at: u64, renew_at_permille: u32) -> u64 {
    rdm_core::certs::renew_point(&[mesh.as_bytes()], issued_at, expires_at, renew_at_permille)
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

    /// A fabric CA under `root`.
    fn ca_of(root: &SigningKey) -> CaKey {
        CaKey::mint(root, rdm_core::certs::CA_CERT_TTL_SECS, 1)
    }

    fn time_at(ms: u64) -> RafkaTime {
        let time = RafkaTime::unadopted();
        time.adopt(ms);
        time
    }

    // @feature: node-lifecycle
    #[test]
    fn an_issuer_whose_admin_holds_no_rafka_time_issues_nothing_and_says_so() {
        let root = rdm_core::certs::mint_key();
        let issuer = CertIssuer::with_root(root.clone(), ca_of(&root), None, RafkaTime::unadopted());
        match issuer.member(&birth("mesh1.rpc.1")) {
            Err(IssueFailure::NoRafkaTime(why)) => assert!(why.contains("no rafka-time"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(issuer.issuing_material("mesh2"), Err(IssueFailure::NoRafkaTime(_))));
    }

    // @feature: node-lifecycle
    #[test]
    fn the_explicit_no_certs_choice_issues_empty_bytes_and_an_unchosen_wiring_resolves_to_a_named_refusal() {
        let nowhere = std::path::Path::new("/nonexistent");
        let issuer = CertChoice::NoCerts.issuer(nowhere, false, time_at(5)).unwrap();
        assert_eq!(issuer.member(&birth("mesh1.rpc.1")), Ok(Vec::new()));
        assert_eq!(issuer.issuing_material("mesh2"), Ok(None));
        let refused = CertChoice::default().issuer(nowhere, false, time_at(5)).err().expect("no choice is refused");
        assert!(refused.contains("no cert choice") && refused.contains("NoCerts"), "{refused}");
    }

    /// CONTRACT (node-cert-renew.md "Request": "its expiry cut to the issuer's when the issuer expires
    /// first (`MemberOutlivesIssuer`)"): a member cert issued near its issuer's expiry is cut to it.
    #[test]
    fn a_member_cert_due_past_its_issuers_expiry_is_cut_to_it() {
        let root = rdm_core::certs::mint_key();
        let issuing_life_ms = rdm_core::certs::ISSUING_CERT_TTL_SECS * 1000;
        let fabric_primary = CertIssuer::with_root(root.clone(), ca_of(&root), None, time_at(1_000));
        let material = fabric_primary.issuing_material("mesh1").unwrap().unwrap();
        // The launched admin's clock is one hour short of its issuer's expiry: a day's cert is cut.
        let issuing = issuing_key(&material, "mesh1", &root.verifying_key()).unwrap();
        let dir = crate::test_support::tempdir("cert-cut-to-issuer");
        keep_ca(&dir, &ca_key(&material, &issuing).unwrap()).unwrap();
        let late = CertChoice::Ca { secret: root, issuing: Some(issuing) }.issuer(&dir, false, time_at(1_000 + issuing_life_ms - 3_600_000)).unwrap();
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
        let root_only = || env(vec![(ENV_CA_SECRET, rdm_core::certs::encode_key(&given))]);
        let root_only_dir = tempdir_in(&dir, "root-only");
        let no_ca = CertChoice::from_material(&root_only_dir, "mesh1", false, root_only()).unwrap().issuer(&root_only_dir, false, time_at(1)).err().expect("a root and no CA, off day 0");
        assert!(no_ca.contains("no fabric CA") && no_ca.contains(ENV_FABRIC_CA_KEY), "{no_ca}");
        let read = CertChoice::from_material(&other, "mesh1", false, root_only()).unwrap();
        assert!(matches!(&read, CertChoice::Ca { secret: s, .. } if s.to_bytes() == given.to_bytes()));
        assert_eq!(std::fs::read_to_string(other.join(CA_SECRET_FILE)).unwrap(), rdm_core::certs::encode_key(&given));
        // A launched admin: the root with its mesh's material; refused for another mesh, and refused by
        // name without the root.
        let material = CertIssuer::with_root(given.clone(), ca_of(&given), None, time_at(1)).issuing_material("mesh2").unwrap().unwrap();
        let launch = |root: bool| {
            let mut pairs = vec![(ENV_ISSUING_KEY, material.key.0.clone()), (ENV_ISSUING_CERT, material.cert.clone())];
            if root {
                pairs.push((ENV_CA_SECRET, material.root.0.clone()));
                pairs.push((ENV_FABRIC_CA_KEY, material.ca_key.0.clone()));
            }
            env(pairs)
        };
        let launched = tempdir_in(&dir, "launched");
        assert!(matches!(CertChoice::from_material(&launched, "mesh2", false, launch(true)), Ok(CertChoice::Ca { issuing: Some(_), .. })));
        assert_eq!(read_ca(&launched.join(FABRIC_CA_FILE)).unwrap().map(|ca| rdm_core::certs::encode_key(ca.secret())), Some(material.ca_key.0.clone()), "the launch's CA is kept beside the root");
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

    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:23
    /// "| Runner | The fabric-primary, which holds the root secret (as every node-admin does, [custody](../fabric-certs.md#custody)) |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:53
    /// "| Launch | A launched node is written the current set at launch (`RDM_ROOTS`), the way `RDM_MESH_ID` is, so a node born mid-overlap trusts both; a launched node-admin is also written the root secret (`RDM_CA_SECRET`) and the current CA material (`RDM_FABRIC_CA_KEY`; the CA's cert travels inside the issuing cert) |"
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
        let material = CertIssuer::with_root(root.clone(), ca_of(&root), None, time_at(1_000)).issuing_material("mesh1").unwrap().unwrap();
        assert_eq!(material.root, SecretHex(rdm_core::certs::encode_key(&root)), "the root rides beside the issuing material");
        assert!(!material.ca_key.0.is_empty(), "so does the fabric CA's key");
        let launch_env = |m: &IssuingMaterial| {
            let m = m.clone();
            move |k: &str| match k {
                ENV_ISSUING_KEY => Some(m.key.0.clone()),
                ENV_ISSUING_CERT => Some(m.cert.clone()),
                ENV_CA_SECRET => Some(m.root.0.clone()),
                ENV_FABRIC_CA_KEY => Some(m.ca_key.0.clone()),
                _ => None,
            }
        };
        let choice = CertChoice::from_material(&dir, "mesh1", false, launch_env(&material)).unwrap();
        assert!(
            matches!(&choice, CertChoice::Ca { secret, issuing: Some(k) } if secret.to_bytes() == root.to_bytes() && rdm_core::certs::encode_issuing_cert(k.cert()) == material.cert && read_ca(&dir.join(FABRIC_CA_FILE)).unwrap().is_some_and(|ca| ca.cert() == &k.cert().ca)),
            "the root and the fabric CA, with its mesh's issuer as given"
        );
        let launched = choice.issuer(&dir, false, time_at(2_000)).unwrap();
        assert_eq!(launched.root(), Some(root.verifying_key()));
        assert_eq!(launched.issuing_material("mesh1").unwrap(), Some(material.clone()), "its mesh's one issuer handed on with the root, never a second");
        let mesh2 = launched.issuing_material("mesh2").unwrap().expect("another mesh's issuer, minted from its own copy of the root");
        assert_eq!(rdm_core::certs::verify_issuer(&rdm_core::certs::decode_issuing_cert(&mesh2.cert).unwrap(), &[root.verifying_key()]), Ok(()));
        assert_eq!(rdm_core::certs::decode_issuing_cert(&mesh2.cert).unwrap().ca, rdm_core::certs::decode_issuing_cert(&material.cert).unwrap().ca, "under the CA it was launched with: one CA, no second");
        assert_eq!(mesh2.ca_key, material.ca_key, "the CA key handed on is that CA's");
        // Issuing material another root signed, beside this root: refused by name.
        let other = rdm_core::certs::mint_key();
        let stranger = CertIssuer::with_root(other.clone(), ca_of(&other), None, time_at(1_000)).issuing_material("mesh1").unwrap().unwrap();
        let mixed = IssuingMaterial { root: material.root.clone(), ..stranger };
        let refused = CertChoice::from_material(&tempdir_in(&dir, "mixed"), "mesh1", false, launch_env(&mixed)).err().expect("refused");
        assert!(refused.contains("ca-bad-signature") && refused.contains("does not verify to the fabric's root"), "{refused}");
    }

    // @feature: node-lifecycle
    /// CONTRACT (fabric-certs.md §Tracking: "A member cert never outlives its issuer ... whatever the
    /// policy says"; node-cert-renew.md Request: "its expiry cut to the issuer's when the issuer expires
    /// first"; mesh-issuer-renew.md step 3): an admin under a 12 s member TTL issues a member cert that
    /// lives 12 s while its issuer outlives that, and its row's renew point is the policy's fraction
    /// of that life plus the birth's jitter. A member due near the issuer's expiry (the issuer has 6 s
    /// left) is cut to it: the cert, the row's `expires_at` and its `issuer_expires_at` are the
    /// issuer's expiry, and the renew point is taken from the cut life, so the birth is renewed before
    /// its issuer expires.
    #[test]
    fn a_member_due_near_the_issuers_expiry_is_cut_to_it() {
        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, 1_000);
        let issuer = rdm_core::certs::IssuingKey::mint(&ca, "mesh1", 1_000, 16_000).expect("within the CA's life");
        let policy = rdm_core::certs::CertPolicy { member_ttl_secs: 12, ..Default::default() };
        let in_window = |issue: &MemberIssue| {
            let life = issue.expires_at - issue.issued_at;
            (issue.issued_at + life * 670 / 1000..=issue.issued_at + life * 720 / 1000).contains(&issue.renew_at)
        };

        let early = CertIssuer::with_root(root.clone(), ca.clone(), Some(issuer.clone()), time_at(1_000));
        early.set_policy(policy);
        assert_eq!(early.policy(), policy, "the admin holds the policy it was given");
        let first = early.issue_member(&birth("mesh1.rpc.1")).unwrap();
        assert_eq!((first.expires_at - first.issued_at, first.issuer_expires_at), (12_000, 16_000), "the policy's 12 s from its issue, inside the issuer's life: {first:?}");
        assert!(in_window(&first), "renewed at two thirds of its life plus at most 5 %: {first:?}");

        let late = CertIssuer::with_root(root, ca, Some(issuer), time_at(10_000));
        late.set_policy(policy);
        let cut = late.issue_member(&birth("mesh1.rpc.2")).unwrap();
        assert!(cut.issued_at >= 10_000 && (cut.expires_at, cut.issuer_expires_at) == (16_000, 16_000), "cut to the issuer's expiry: {cut:?}");
        assert_eq!(rdm_core::certs::cert_from_bytes(&cut.bytes).unwrap().cert.expiry_ms, 16_000, "the bytes carry the cut expiry");
        assert!(in_window(&cut) && cut.renew_at < 16_000, "the renew point is taken from the cut life: {cut:?}");
    }

    /// CONTRACT (fabric-certs.md "The chain": the fabric CA is "minted by the fabric-primary on day 0
    /// (`fabric.ca.create`)"; §Custody: "Only the fabric-primary mints"; fabric-create.md step 1): the
    /// bootstrap admin mints the fabric CA once, on day 0, and the same admin started again over its data
    /// dir holds that CA: an issuer it mints after the restart is under the day-0 CA. Must NOT happen: a
    /// second `rdm.node_admin.cert.create.via-ca-create`, or an issuer under a CA minted after day 0.
    #[test]
    fn a_restarted_bootstrap_admin_mints_issuers_under_its_day_zero_ca() {
        let capture = crate::span_capture::Capture::new("i145-e5-ca-restart");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let dir = crate::test_support::tempdir("cert-ca-restart");
        let nothing = |_: &str| None;
        let day0 = CertChoice::from_material(&dir, "mesh1", true, nothing).unwrap().issuer(&dir, true, time_at(1_000)).unwrap();
        let before = day0.issuing_material("mesh2").unwrap().expect("mesh2's issuer, minted on day 0");
        // The same admin started again over its data dir: not day 0, nothing in its environment.
        let restarted = CertChoice::from_material(&dir, "mesh1", false, nothing).unwrap().issuer(&dir, false, time_at(2_000)).unwrap();
        let after = restarted.issuing_material("mesh3").unwrap().expect("mesh3's issuer, minted after the restart");
        let ca_of = |m: &IssuingMaterial| rdm_core::certs::decode_issuing_cert(&m.cert).unwrap().ca;
        assert_eq!(ca_of(&after), ca_of(&before), "the issuer minted after the restart is under the day-0 CA");
        assert_eq!(capture.named("rdm.node_admin.cert.create.via-ca-create").len(), 1, "the fabric CA is minted once");
    }

    fn tempdir_in(dir: &Path, name: &str) -> std::path::PathBuf {
        let d = dir.join(name);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    // @feature: node-lifecycle
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:5
    /// "Two levels sit above a mesh's issuers ([the chain](../fabric-certs.md#the-chain)): the **fabric root**, the trust anchor the embedding app passed in, which signs nothing but RDM's CA; and the **fabric CA**, RDM's own intermediate, signed by the root, which signs every mesh's issuing cert."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:10
    /// "- `fabric.ca.renew` rotates the CA internally, on policy, with nothing pushed to any node but the ordinary issuer and member renewals: the new CA chains to the same anchor, so every verifier's trust set is unchanged."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:23
    /// "| Runner | The fabric-primary, which holds the root secret (as every node-admin does, [custody](../fabric-certs.md#custody)) |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:24
    /// "| When | Its own tick, once the CA has lived `RDM_CERT_RENEW_AT` of its TTL (`RDM_CA_CERT_TTL_SECS`, default 365 days); also run by `fabric.root.update` step 3, under the new anchor |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:25
    /// "| Steps | 1 mint a new CA keypair and sign its cert with the root; write the CA row; 2 [mesh.issuer.renew](mesh-issuer-renew.md) for every mesh under the new CA, each `node.issuer.update` carrying the new CA material; members renew under the new issuers at their own points |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:26
    /// "| Trust set | Unchanged: the new CA chains to the same anchor every node already trusts |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:27
    /// "| Overlap | The old CA, and every issuer and member it signed, stay valid to their own expiry; an issuer never outlives its CA, a member never its issuer |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:29
    /// "| Never | A cert cut short, a status set, a frame published, a connection closed, a root changed |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:92
    /// "An internal `fabric.ca.renew` is step 3 alone, with no caller and no `node.roots.update`."
    #[test]
    fn a_renewed_fabric_ca_chains_to_the_same_root_and_leaves_older_issuers_due() {
        let root = rdm_core::certs::mint_key();
        let issuer = CertIssuer::with_root(root.clone(), ca_of(&root), None, time_at(1_000));
        let (before, _) = issuer.renew_issuer("mesh1").expect("an issuer under the first CA");
        assert!(issuer.issuer_under_current_ca("mesh1"));
        let first_ca_expiry = issuer.ca_dates().unwrap().1;
        let (_ca, previous) = issuer.renew_ca().unwrap().expect("a root holder renews its CA");
        assert_eq!(previous, first_ca_expiry);
        assert_eq!(issuer.root(), Some(root.verifying_key()), "the root is unchanged");
        assert!(!issuer.issuer_under_current_ca("mesh1"), "the issuer minted under the old CA is due");
        let (after, _) = issuer.renew_issuer("mesh1").unwrap();
        assert!(issuer.issuer_under_current_ca("mesh1"));
        assert_ne!(before.cert().ca.cert, after.cert().ca.cert);
        assert!(rdm_core::certs::verify_issuer(after.cert(), &[root.verifying_key()]).is_ok(), "the renewed issuer chains to the same root");
        assert!(CertIssuer::no_certs(time_at(1_000)).renew_ca().unwrap().is_none(), "an admin with no certs renews nothing");
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:5
    /// "Two levels sit above a mesh's issuers ([the chain](../fabric-certs.md#the-chain)): the **fabric root**, the trust anchor the embedding app passed in, which signs nothing but RDM's CA; and the **fabric CA**, RDM's own intermediate, signed by the root, which signs every mesh's issuing cert."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:13
    /// "- `fabric.root.update` rotates the anchor itself, only on the app's or a person's call with a new root, through an overlap in which every node trusts both anchors (`node.roots.update`), and retires the old one once nothing chains to it."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:39
    /// "| Phases | `minted` → `distributed` (both anchors trusted everywhere) → `ca-rotated` → `retired` (one anchor again); the roots row is durable and a successor continues from it |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:40
    /// "| Steps | 1 write the new root secret beside the old one on the fabric-primary; row `minted`; 2 `node.roots.update { roots: [old, new] }` to every node, admins first (the secret to admins, the public set to every other node); row `distributed`; 3 `fabric.ca.renew` under the new anchor, which re-mints every issuer; row `ca-rotated`; 4 wait until every cert row chains to the new anchor (at most one member TTL after step 3); 5 `node.roots.update { roots: [new] }` to every node; row `retired`; the old secret is destroyed everywhere |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:57
    /// "1. `fabric.root.update { secret }` arrives at the fabric-primary. It writes the new secret beside the old one, writes the roots row `minted`, and answers `fabric.root.updated`."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:61
    /// "3. `fabric.ca.renew` under the new anchor: a new CA signed by the new root, every mesh's issuer re-minted under it ([mesh.issuer.renew](mesh-issuer-renew.md)). The row goes `ca-rotated`. Members renew under the new issuers at their own points ([node.cert.renew](node-cert-renew.md))."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:67
    /// "5. `node.roots.update { [new] }` to every node. The row goes `retired` on the last receipt, and the old secret is destroyed on every admin."
    #[test]
    fn a_root_rotation_holds_the_next_root_mints_the_ca_under_it_and_retires_the_old() {
        let (old, new) = (rdm_core::certs::mint_key(), rdm_core::certs::mint_key());
        let dir = std::env::temp_dir().join(format!("rdm-root-rotation-{}", rdm_core::certs::encode_key(&rdm_core::certs::mint_key())));
        std::fs::create_dir_all(&dir).unwrap();
        let issuer = CertIssuer::with_root(old.clone(), ca_of(&old), None, time_at(5_000));
        issuer.kept.set(dir.clone()).unwrap();
        assert_eq!(issuer.next_root(), None);
        issuer.hold_next_root(new.clone()).unwrap();
        assert_eq!(issuer.next_root(), Some(new.verifying_key()));
        assert_eq!(std::fs::read_to_string(dir.join(NEXT_SECRET_FILE)).unwrap(), rdm_core::certs::encode_key(&new), "the next root is kept beside the data dir");
        assert_eq!(issuer.next_root_secret(), Some(rdm_core::certs::encode_key(&new)));
        let (ca, _) = issuer.renew_ca_under_next_root().unwrap().expect("a CA under the next root");
        let (minted, _) = issuer.renew_issuer("mesh1").unwrap();
        assert_eq!(minted.cert().ca.cert, ca.cert().cert);
        assert!(rdm_core::certs::verify_issuer(minted.cert(), &[new.verifying_key()]).is_ok(), "the issuer chains to the new root");
        assert!(rdm_core::certs::verify_issuer(minted.cert(), &[old.verifying_key()]).is_err(), "and not to the old");
        assert_eq!(issuer.root(), Some(old.verifying_key()), "the old root stays the root until the rotation retires it");
        issuer.replace_root_with(new.clone()).unwrap();
        assert_eq!(issuer.root(), Some(new.verifying_key()));
        assert_eq!(issuer.next_root(), None);
        assert_eq!(std::fs::read_to_string(dir.join(CA_SECRET_FILE)).unwrap(), rdm_core::certs::encode_key(&new), "the new root is kept as ca-secret");
        assert!(!dir.join(NEXT_SECRET_FILE).exists(), "the next root's file is destroyed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:25
    /// "| Steps | 1 mint a new CA keypair and sign its cert with the root; write the CA row; 2 [mesh.issuer.renew](mesh-issuer-renew.md) for every mesh under the new CA, each `node.issuer.update` carrying the new CA material; members renew under the new issuers at their own points |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:96
    /// "A node not reached in a phase is retried every tick; the phase does not advance without it, and nothing expires meanwhile that the overlap does not cover. A successor fabric-primary continues from the roots row and the CA row, and from its own copy of the root secret, which every node-admin holds ([custody](../fabric-certs.md#custody)). A successor that does not hold the CA the CA row names (a renewal whose step 2 had not reached it) never mints under a CA it does not hold: it runs `fabric.ca.renew` again from the root secret, and step 2 reaches every admin under the new CA."
    #[test]
    fn an_admin_installs_and_keeps_a_newer_fabric_ca_and_refuses_an_older_one() {
        let root = rdm_core::certs::mint_key();
        let first = ca_of(&root);
        let fabric_primary = CertIssuer::with_root(root.clone(), first.clone(), None, time_at(5_000));
        let dir = std::env::temp_dir().join(format!("rdm-ca-install-{}", rdm_core::certs::encode_key(&rdm_core::certs::mint_key())));
        let other = CertIssuer::with_root(root.clone(), first.clone(), None, time_at(5_000));
        other.kept.set(dir.clone()).unwrap();
        let (renewed, _) = fabric_primary.renew_ca().unwrap().expect("the fabric-primary renews its CA");
        let (issuing, _) = fabric_primary.renew_issuer("mesh1").unwrap();
        assert_eq!(fabric_primary.ca_key_of(&issuing), Some(rdm_core::certs::encode_key(renewed.secret())), "the issuer rides with the CA it is minted under");
        assert_eq!(other.install_ca(renewed.clone()), CaInstall::Installed { previous_expires_at: first.expiry_ms() });
        assert_eq!(read_ca(&dir.join(FABRIC_CA_FILE)).unwrap().map(|ca| ca.cert().clone()), Some(renewed.cert().clone()), "the installed CA is kept beside the data dir");
        assert_eq!(other.ca_key_of(&issuing), Some(rdm_core::certs::encode_key(renewed.secret())), "the admin now holds the CA the issuer is minted under");
        assert_eq!(other.install_ca(renewed.clone()), CaInstall::Held, "the CA held already changes nothing");
        assert_eq!(other.install_ca(first.clone()), CaInstall::Older { held_expires_at: renewed.expiry_ms() }, "an older CA is refused");
        assert_eq!(other.ca_dates().unwrap().1, renewed.expiry_ms(), "nothing installed on the refusal");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
