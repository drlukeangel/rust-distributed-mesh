//! Cert tracking and renewal (fabric-certs.md §Tracking; node-cert-renew.md, mesh-issuer-renew.md,
//! node-issuer-update.md).
//!
//! A birth's member cert dates are on its node's record ([`CertDates`]), written by the node's
//! mesh-primary when the birth holds the cert: at its join's admission, and on a renewal's
//! `CertRenewed` naming the expiry signed. The written expiry is the receipt (Q141); an unanswered
//! renewal writes nothing, so the record's `renew_at`, already passed, renews it on the next tick. A
//! mesh-primary's own cert, issued by the fabric-primary, it writes on its own record when it installs
//! it; the fabric-primary schedules that renewal from the dates it issued and the expiry the reply
//! confirmed, kept in its own cache. A node-admin birth's signing cert dates are on its record too
//! ([`SigningCertDates`]), written by its mesh-primary; the fabric-primary schedules the renewals of
//! the mesh issuers from the dates it minted and each admin's `IssuerUpdated` confirmed, kept in its
//! cache, and a successor fabric-primary reads the records when it acquires ownership.
//!
//! - The issuer's tick ([`renew_members`]): a node is renewed by its mesh's primary, a mesh-primary
//!   by the fabric-primary, the fabric-primary by itself in process. A birth whose cert has passed its
//!   renew point is issued a fresh cert and sent `RenewCert`, each tick until its cert expires.
//! - The fabric-primary's tick ([`renew_issuers`]): a mesh whose newest signing cert has passed its
//!   renew point, and no newer issuing key held here, is given a new one, held in process, and sent by
//!   `node.issuer.update` to that mesh's primary directly; once it confirms, to each other admin of the
//!   mesh as a command forwarded through the mesh-primary (Forward 0x1A, the mesh-primary the exact
//!   carrier). Its own mesh's admins it calls directly, itself in process.
//! - `fabric.get`'s next expiry and a mesh's stalled renewals are folded from the node records the
//!   answering admin's atlas holds ([`fabric_watch`]), never read from the store.

use crate::certs::{BirthIdentity, CertIssuer, IssueFailure, IssuerMint, MemberIssue};
use crate::model::{IncarnationId, Node, NodeId, NodeKind, PathName};
use crate::storage::{CertDates, NodesStorage, SigningCertDates};
use crate::topology::Topology;
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{IssuingKeyHex, Status, StatusReply, StatusRequest};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Whether the birth whose cert dates are `dates` stalls at `now_ms`
/// (`docs/architecture/fabric-certs.md` §Tracking: "past `renew_at` by more than a floor with no
/// `node.cert.renewed`").
pub fn stalled(dates: &CertDates, now_ms: u64, floor_ms: u64) -> bool {
    now_ms > dates.renew_at.saturating_add(floor_ms)
}

/// Each birth's member cert dates on `records`, by exact birth.
pub fn fold_of(records: &[crate::storage::NodeRow]) -> HashMap<(NodeId, IncarnationId), CertDates> {
    records.iter().flat_map(|r| r.births.iter().filter_map(|b| Some(((r.node_id.clone(), b.incarnation_id.clone()), b.cert.clone()?)))).collect()
}

/// The dates a member cert issue writes once its birth holds the cert
/// (`docs/architecture/fabric-certs.md` §Tracking, `docs/architecture/lifecycles/node-cert-renew.md`).
pub fn dates_of(issue: &MemberIssue) -> CertDates {
    CertDates { issued_at: issue.issued_at, expires_at: issue.expires_at, issuer_expires_at: issue.issuer_expires_at, renew_at: issue.renew_at }
}

/// The member cert dates a mesh-primary writes on its own record when it installs the cert `req`
/// (`RenewCert`) carried to it, under `receiver`'s policy
/// (`docs/architecture/lifecycles/node-cert-renew.md` Row: "A mesh-primary's own cert, issued by the
/// fabric-primary, it writes on its own record when it installs it"; Q141, Q241): the cert's own
/// dates, never the receiver's clock; nothing for another request or a cert that does not decode.
pub fn installed_dates(req: &StatusRequest, receiver: &CertIssuer) -> Option<CertDates> {
    let StatusRequest::RenewCert { cert, .. } = req else { return None };
    let signed = rdm_core::certs::cert_from_bytes(cert).ok()?;
    Some(crate::certs::member_cert_dates(&signed, receiver.policy().renew_at_permille))
}

/// The signing cert dates an admin's record holds once it installs the issuing cert `signed` that a
/// `node.issuer.update` carried to it, under `receiver`'s policy
/// (`docs/architecture/lifecycles/node-issuer-update.md` step 4; Q141, Q241): the cert's own dates,
/// never the receiver's clock.
pub fn installed_signing_dates(signed: &rdm_core::certs::SignedIssuingCert, receiver: &CertIssuer) -> SigningCertDates {
    crate::certs::signing_cert_dates(signed, receiver.policy().renew_at_permille)
}

/// The signing cert dates of the issuing key `mint` minted
/// (`docs/architecture/lifecycles/mesh-issuer-renew.md`).
pub fn signing_dates(mint: &IssuerMint) -> SigningCertDates {
    SigningCertDates { issued_at: mint.issued_at, expires_at: mint.expires_at, renew_at: mint.renew_at }
}

/// The births the view holds at their exact incarnation, by (node id, incarnation).
fn births_of<'a>(view: &'a Topology, mesh: &str) -> HashMap<(NodeId, IncarnationId), &'a Node> {
    view.births().filter(|n| n.mesh == mesh).filter_map(|n| Some(((n.node_id.clone(), n.incarnation_id.clone()?), n))).collect()
}

/// What `fabric.get` shows of the cert dates (fabric-certs.md §Tracking), folded from the node records
/// `records` (this admin's atlas) holds, never a store read: the next expiry across the births the view
/// holds, and each mesh's births whose renewal stalls at `now_ms`.
pub fn fabric_watch(view: &Topology, records: &crate::atlas::Atlas, now_ms: Option<u64>) -> (Option<u64>, BTreeMap<String, Vec<PathName>>) {
    let floor = rdm_core::cadence::staleness_floor().as_millis() as u64;
    let mut next: Option<u64> = None;
    let mut stalls: BTreeMap<String, Vec<PathName>> = BTreeMap::new();
    let meshes: BTreeSet<String> = view.births().map(|n| n.mesh.clone()).collect();
    for mesh in meshes {
        let folded = fold_of(&records.records_of(&mesh));
        let births = births_of(view, &mesh);
        for (birth, cert) in folded {
            let Some(node) = births.get(&birth) else { continue };
            next = Some(next.map_or(cert.expires_at, |n| n.min(cert.expires_at)));
            if now_ms.is_some_and(|now| stalled(&cert, now, floor)) {
                stalls.entry(mesh.clone()).or_default().push(node.name.clone());
            }
        }
    }
    (next, stalls)
}

/// The births whose certs this admin, `me`, renews (node-cert-renew.md Caller): every other birth of
/// a mesh whose primary seat it holds, and, holding the fabric seat, every other mesh's primary and
/// itself.
fn renewed_by(view: &Topology, me: &NodeId) -> Vec<Node> {
    let mut out = Vec::new();
    let fabric_primary = view.fabric_primary().is_some_and(|f| f.node_id == *me);
    for m in &view.meshes {
        let primary = view.node_type_primary(&m.name, NodeKind::NodeAdmin).map(|p| p.node_id.clone());
        let holds_mesh = primary.as_ref() == Some(me);
        for n in view.births().filter(|n| n.mesh == m.name) {
            let is_primary = primary.as_ref() == Some(&n.node_id);
            let mine = (holds_mesh && n.node_id != *me) || (fabric_primary && (is_primary || n.node_id == *me));
            if mine && n.incarnation_id.is_some() {
                out.push(n.clone());
            }
        }
    }
    out
}

/// How a renewal ended, as its span names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The birth answered `node.cert.renewed`: it holds and presents the new cert.
    Installed,
    /// The call did not reach it, or its answer did not come back (`NotSent`, `Indeterminate`).
    NotSent,
    /// The birth, or the issue, refused it by name.
    Refused,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::NotSent => "not-sent",
            Self::Refused => "refused",
        }
    }
}

/// Installs a renewed member cert on this admin itself (the fabric-primary renews its own cert in
/// process): the bytes it presents from now on.
pub type InstallOwn = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

/// The fabric-primary's own `node.roots.update` (its exact birth, the set, the phase, whether its secret
/// rides along), answered as a node answers it.
pub type OwnRoots = Arc<dyn Fn(&IncarnationId, &[String], rdm_core::status::RootsPhase, Option<rdm_core::status::RootSecretHex>) -> StatusReply + Send + Sync>;

/// What the renewal ticks act through.
pub struct Renewals {
    /// This admin.
    pub me: PathName,
    /// Its node id.
    pub node_id: NodeId,
    /// What it signs with.
    pub issuer: Arc<CertIssuer>,
    /// The nodes store: the node records carrying the cert dates.
    pub nodes_storage: Arc<dyn NodesStorage>,
    /// The records this admin writes, as it holds them.
    pub caches: crate::record_cache::Caches,
    /// Its Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// Its own install, for its own renewal.
    pub install_own: InstallOwn,
    /// The births a renewal call is in flight to: a tick never sends a second.
    in_flight: Mutex<HashSet<(NodeId, IncarnationId)>>,
    /// The admins an issuer update call is in flight to.
    updating: Mutex<HashSet<(NodeId, IncarnationId)>>,
    /// The fabric-primary's own copy of the member cert dates of the births whose records it does not
    /// write (another mesh's primary): the dates it issued and the expiry the reply confirmed, filled
    /// from their records when it holds none.
    issued: Mutex<HashMap<(NodeId, IncarnationId), CertDates>>,
    /// The fabric-primary's copy of every node-admin's signing cert dates, by mesh: the dates it
    /// minted and each `IssuerUpdated` confirmed, a mesh's filled from its records once.
    signing: Mutex<HashMap<String, HashMap<(NodeId, IncarnationId), SigningCertDates>>>,
    /// The meshes whose issuer is re-minted under the fabric CA this admin renewed (fabric-ca-renew.md
    /// step 2), each until its next issuing key is minted.
    ca_due: Mutex<BTreeSet<String>>,
    /// The fabric store and the fabric's id the CA row is written to (fabric-ca-renew.md "write the CA
    /// row"); set once at start, absent in a cell that writes none.
    fabric: std::sync::OnceLock<(Arc<dyn crate::fabric_storage::FabricStorage>, crate::model::FabricId)>,
    /// The fabric-primary's own install of a root set (`node.roots.update` on itself, in process).
    own_roots: std::sync::OnceLock<OwnRoots>,
    /// The births that answered a root set of the rotation, by birth and the phase the set belongs to.
    roots_acked: Mutex<BTreeSet<String>>,
}

impl Renewals {
    /// The renewal ticks of the admin `me`.
    pub fn new(me: PathName, node_id: NodeId, issuer: Arc<CertIssuer>, nodes_storage: Arc<dyn NodesStorage>, caches: crate::record_cache::Caches, client: Arc<NodeRpcClient>, install_own: InstallOwn) -> Arc<Self> {
        Arc::new(Self { me, node_id, issuer, nodes_storage, caches, client, install_own, in_flight: Mutex::default(), updating: Mutex::default(), issued: Mutex::default(), signing: Mutex::default(), ca_due: Mutex::default(), fabric: std::sync::OnceLock::new(), own_roots: std::sync::OnceLock::new(), roots_acked: Mutex::default() })
    }

    /// Where the CA row is written: the fabric store and the fabric's id, set once.
    pub fn set_fabric(&self, storage: Arc<dyn crate::fabric_storage::FabricStorage>, fabric_id: crate::model::FabricId) {
        let _ = self.fabric.set((storage, fabric_id));
    }

    /// The fabric-primary's own install of a root set, once its node is running.
    pub fn set_own_roots(&self, install: OwnRoots) {
        let _ = self.own_roots.set(install);
    }

    /// Whether this admin writes the record of `n` in `view`: the primary of `n`'s mesh, or the
    /// fabric-primary while that mesh has none (the writer map).
    fn writes(&self, view: &Topology, n: &Node) -> bool {
        crate::node_rows::writes_for(view, &self.node_id, &n.mesh)
    }

    /// The member cert dates of the birth `n` at `incarnation` this admin schedules its renewal from:
    /// its record's, when it writes it; else its own copy, filled from the record when it holds none.
    async fn dates(&self, view: &Topology, n: &Node, incarnation: &IncarnationId) -> Result<Option<CertDates>, String> {
        let birth = (n.node_id.clone(), incarnation.clone());
        if self.writes(view, n) {
            return Ok(self.caches.nodes.known_row(&*self.nodes_storage, &n.node_id).await?.and_then(|r| r.birth(incarnation).and_then(|b| b.cert.clone())));
        }
        if let Some(d) = self.issued.lock().unwrap().get(&birth).cloned() {
            return Ok(Some(d));
        }
        let found = self.nodes_storage.node(&n.node_id).await.map_err(|e| format!("the record of {}: {e}", n.node_id))?.and_then(|r| r.birth(incarnation).and_then(|b| b.cert.clone()));
        if let Some(d) = &found {
            self.issued.lock().unwrap().insert(birth, d.clone());
        }
        Ok(found)
    }
}

fn identity(n: &Node, incarnation: &IncarnationId) -> BirthIdentity {
    BirthIdentity { node_id: n.node_id.clone(), incarnation: incarnation.clone(), mesh: n.mesh.clone(), name: n.name.clone(), endpoint_key: n.endpoint_id.clone().unwrap_or_else(|| rdm_core::EndpointId(String::new())) }
}

/// One tick of `node.cert.renew` (node-cert-renew.md steps 1-5) on `view`: each birth this admin
/// renews whose cert has passed its renew point is issued a fresh cert and sent it, each tick until
/// its cert expires; its dates are written only on the answer. Each send runs on its own task; the
/// tick never waits for one.
pub async fn renew_members(r: &Arc<Renewals>, view: &Topology) {
    if r.issuer.is_no_certs() {
        return;
    }
    let Some(now) = r.issuer.now_ms() else { return };
    for n in renewed_by(view, &r.node_id) {
        let Some(incarnation) = n.incarnation_id.clone() else { continue };
        let held = match r.dates(view, &n, &incarnation).await {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(why) => {
                tracing::info_span!("rdm.node_admin.cert.reject.via-renewal", node = %n.name, reason = %why)
                    .in_scope(|| tracing::warn!("the cert dates cannot be read: no renewal this tick"));
                continue;
            }
        };
        // Past its renew point and before its expiry: renewed again each tick until it answers.
        if now < held.renew_at || now >= held.expires_at {
            continue;
        }
        if !r.in_flight.lock().unwrap().insert((n.node_id.clone(), incarnation.clone())) {
            continue;
        }
        let birth = identity(&n, &incarnation);
        let issue = r.issuer.renewal(&birth);
        let writes = r.writes(view, &n);
        let r2 = r.clone();
        let n2 = n.clone();
        tokio::spawn(async move {
            renew_one(&r2, &n2, &birth, &held, issue, writes).await;
            r2.in_flight.lock().unwrap().remove(&(n2.node_id.clone(), birth.incarnation.clone()));
        });
    }
}

async fn renew_one(r: &Renewals, n: &Node, birth: &BirthIdentity, held: &CertDates, issue: Result<MemberIssue, IssueFailure>, writes: bool) {
    let span = tracing::info_span!(
        "rdm.node_admin.cert.update.via-renewal",
        node = %n.name,
        node_id = %n.node_id,
        incarnation_id = %birth.incarnation.as_str(),
        issued_at = tracing::field::Empty,
        expires_at = tracing::field::Empty,
        previous_expires_at = held.expires_at,
        reason = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let outcome = tracing::Instrument::instrument(
        async {
            let issue = match issue {
                Ok(i) => i,
                Err(e) => {
                    tracing::Span::current().record("reason", e.to_string().as_str());
                    return Outcome::Refused;
                }
            };
            tracing::Span::current().record("issued_at", issue.issued_at);
            tracing::Span::current().record("expires_at", issue.expires_at);
            let dates = dates_of(&issue);
            // The fabric-primary's own cert is renewed in process, with no call.
            if n.node_id == r.node_id {
                (r.install_own)(issue.bytes.clone());
                return written(r, n, birth, &dates, writes).await;
            }
            let req = StatusRequest::RenewCert { node_id: n.node_id.clone(), incarnation: birth.incarnation.clone(), cert: issue.bytes.clone() };
            let (out, _) = r.client.call::<Status>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &CallOptions::default()).await;
            let shown = format!("{out:?}");
            match out {
                RpcOutcome::Reply(reply) => match reply.into_value() {
                    // The answer names the expiry of the cert installed: the one issued here, or nothing is written.
                    StatusReply::CertRenewed { expires_at, .. } if expires_at == issue.expires_at => written(r, n, birth, &dates, writes).await,
                    StatusReply::CertRenewed { expires_at, .. } => {
                        tracing::Span::current().record("reason", format!("the birth answered it holds a cert expiring at {expires_at}, not the one issued here ({}): nothing is written", issue.expires_at).as_str());
                        Outcome::Refused
                    }
                    other => {
                        tracing::Span::current().record("reason", format!("{}: {other:?}", other.name()).as_str());
                        Outcome::Refused
                    }
                },
                RpcOutcome::NotSent(_) | RpcOutcome::Indeterminate(_) => {
                    tracing::Span::current().record("reason", shown.as_str());
                    Outcome::NotSent
                }
                _ => {
                    tracing::Span::current().record("reason", shown.as_str());
                    Outcome::Refused
                }
            }
        },
        span.clone(),
    )
    .await;
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(outcome = outcome.as_str(), "member cert renewal"));
}

/// Keep the dates of a renewal its birth holds: on the birth's record, written whole, when this admin
/// writes it (the written expiry is the receipt, Q141); else in this fabric-primary's own copy, the
/// mesh-primary writing its own record as it installs the cert. A write refused leaves the record as
/// it was, so the next tick renews again.
async fn written(r: &Renewals, n: &Node, birth: &BirthIdentity, dates: &CertDates, writes: bool) -> Outcome {
    if !writes {
        r.issued.lock().unwrap().insert((n.node_id.clone(), birth.incarnation.clone()), dates.clone());
        // The renewal is this admin's op: its result is its atlas's at once (Q197, Q203); the
        // record is the writer of `n`'s mesh's.
        if let Some(atlas) = r.caches.nodes.atlas() {
            atlas.update_record(&n.node_id, |rec| rec.birth_mut(&birth.incarnation, &birth.name).cert = Some(dates.clone()));
        }
        return Outcome::Installed;
    }
    let base = crate::storage::NodeRow::new(n.name.clone(), n.node_id.clone(), n.status);
    match r.caches.nodes.update(&*r.nodes_storage, &n.node_id, Some(base), |rec| rec.birth_mut(&birth.incarnation, &birth.name).cert = Some(dates.clone())).await {
        Ok(_) => Outcome::Installed,
        Err(e) => {
            tracing::Span::current().record("reason", format!("the cert dates could not be written: {e}").as_str());
            Outcome::NotSent
        }
    }
}

/// The signing cert dates of `mesh`'s node-admin births this fabric-primary schedules from: its own
/// copy, filled from the mesh's records the first time it reads them, with the dates on the records
/// it holds as the writer of `mesh` (`own_mesh`: an admin launched with the held key has them written
/// at its join's admission, by this admin).
async fn signing_of(r: &Renewals, mesh: &str, own_mesh: bool) -> Result<HashMap<(NodeId, IncarnationId), SigningCertDates>, String> {
    if let Some(mut held) = r.signing.lock().unwrap().get(mesh).cloned() {
        if own_mesh {
            for rec in r.caches.nodes.rows_of(mesh) {
                for b in &rec.births {
                    if let Some(dates) = &b.signing_cert {
                        held.insert((rec.node_id.clone(), b.incarnation_id.clone()), dates.clone());
                    }
                }
            }
        }
        return Ok(held);
    }
    let records = r.nodes_storage.nodes(mesh).await.map_err(|e| format!("the node records of mesh {mesh}: {e}"))?;
    let found: HashMap<(NodeId, IncarnationId), SigningCertDates> = records.iter().flat_map(|rec| rec.births.iter().filter_map(|b| Some(((rec.node_id.clone(), b.incarnation_id.clone()), b.signing_cert.clone()?)))).collect();
    Ok(r.signing.lock().unwrap().entry(mesh.to_string()).or_insert(found).clone())
}

/// One tick of `mesh.issuer.renew` (mesh-issuer-renew.md) on `view`, run while this admin is the
/// fabric-primary, from the signing cert dates it holds in its cache (the dates it minted and each
/// admin's `IssuerUpdated` confirmed): a mesh whose newest signing cert has passed its renew point,
/// and no newer issuing key held here, is given a new one, held in process. Until the key it replaces
/// expires, the newest key held is sent by `node.issuer.update` to each admin that has not confirmed
/// it: the mesh's primary directly first, then each other admin as a command forwarded through the
/// mesh-primary once the primary confirmed. The fabric-primary's own mesh: itself in process, its
/// other admins directly.
pub async fn renew_issuers(r: &Arc<Renewals>, view: &Topology) {
    if r.issuer.is_no_certs() || r.issuer.root().is_none() || !view.fabric_primary().is_some_and(|f| f.node_id == r.node_id) {
        return;
    }
    let Some(now) = r.issuer.now_ms() else { return };
    renew_ca(r, view, now).await;
    rotate_roots(r, view).await;
    for m in &view.meshes {
        let admins: Vec<(Node, IncarnationId)> = view.births().filter(|n| n.mesh == m.name && n.kind == NodeKind::NodeAdmin).filter_map(|n| Some((n.clone(), n.incarnation_id.clone()?))).collect();
        // Every admin's dates of the mesh, a re-created admin's predecessor's included: the schedule is theirs.
        // The signing cert dates are written by the admin's mesh-primary (node-issuer-update.md step 4):
        // this fabric-primary writes them only for the mesh whose primary it is, never as the stand-in
        // writer of a mesh it sees with none, whose own primary may already be writing its records.
        let own_mesh = view.is_mesh_primary(&m.name, &r.node_id);
        let Ok(rows) = signing_of(r, &m.name, own_mesh).await else { continue };
        // The newest key is the one held here while no admin's dates name a newer, else the newest
        // dates'; its renew point passed, the next is minted, and otherwise the held key is carried to
        // every admin lacking it.
        let newest = rows.values().max_by_key(|row| row.expires_at);
        let newest_row = newest.map(|row| row.expires_at).unwrap_or(0);
        let held = r.issuer.held_mint(&m.name).filter(|mint| mint.expires_at >= newest_row);
        let due = r.ca_due.lock().unwrap().contains(&m.name)
            || match &held {
                Some(mint) => now >= mint.renew_at,
                None => newest.is_some_and(|row| now >= row.renew_at),
            };
        let for_ca = r.ca_due.lock().unwrap().contains(&m.name);
        let current = match (due, held) {
            (true, held) => match mint_next(r, &m.name, held.map(|h| h.expires_at).unwrap_or(newest_row)) {
                Some(mint) => {
                    // The mesh's issuer under the renewed CA: its receipt on the CA row.
                    if for_ca {
                        write_ca_row(r, |row| {
                            row.issuer_receipts.insert(m.name.clone(), mint.expires_at);
                        })
                        .await;
                    }
                    mint
                }
                None => continue,
            },
            (false, Some(mint)) => mint,
            (false, None) => continue,
        };
        let row_of_admin = |a: &Node, i: &IncarnationId| rows.get(&(a.node_id.clone(), i.clone()));
        let holds = |a: &Node, i: &IncarnationId| row_of_admin(a, i).is_some_and(|row| row.expires_at == current.expires_at);
        // A mesh's admins are carried the newest key until the one it replaces expires: an admin with
        // no dates is carried it, one whose older key has expired is not.
        let lacking: Vec<&(Node, IncarnationId)> = admins.iter().filter(|(a, i)| !holds(a, i)).collect();
        if lacking.is_empty() || lacking.iter().all(|(a, i)| row_of_admin(a, i).is_some_and(|row| now >= row.expires_at)) {
            continue;
        }
        let Some(primary) = view.node_type_primary(&m.name, NodeKind::NodeAdmin).cloned() else { continue };
        let others: Vec<Node> = admins.iter().filter(|(a, _)| a.node_id != primary.node_id).map(|(a, _)| a.clone()).collect();
        let primary_holds = primary.incarnation_id.as_ref().is_some_and(|i| holds(&primary, i));
        let sends: Vec<(Node, Option<NodeId>)> = if primary.node_id == r.node_id {
            std::iter::once(primary).chain(others).map(|a| (a, None)).collect()
        } else if primary_holds {
            others.into_iter().map(|a| (a, Some(primary.node_id.clone()))).collect()
        } else {
            vec![(primary, None)]
        };
        let sends: Vec<(Node, Option<NodeId>)> = sends.into_iter().filter(|(a, _)| a.incarnation_id.as_ref().is_some_and(|i| !holds(a, i))).collect();
        update_admins(r, &m.name, sends, &current, own_mesh).await;
    }
}

/// `fabric.ca.renew` (fabric-ca-renew.md) on the fabric-primary's tick: once the fabric CA has lived
/// `RDM_CERT_RENEW_AT` of its TTL, a new CA is minted under the same root and held from then on, and
/// every mesh of `view` is due for `mesh.issuer.renew` under it. The trust set is unchanged: the new CA
/// chains to the root every node already trusts. One span, `rdm.node_admin.ca.update.via-renewal`.
async fn renew_ca(r: &Renewals, view: &Topology, now: u64) {
    let Some((_, expires, renew_at)) = r.issuer.ca_dates() else { return };
    // A successor that does not hold the CA the CA row names (a renewal whose step 2 had not reached
    // it) never mints under a CA it does not hold: it renews again from the root secret, at once
    // (fabric-ca-renew.md, Q275). The row is the one this admin's caches hold; nothing is read here.
    let behind_row = r.fabric.get().and_then(|(_, id)| r.caches.fabric.get(id)).and_then(|record| record.ca.map(|row| row.expires_at)).is_some_and(|row| row > expires);
    if now < renew_at && !behind_row {
        return;
    }
    let meshes: Vec<String> = view.meshes.iter().map(|m| m.name.clone()).collect();
    let span = tracing::info_span!("rdm.node_admin.ca.update.via-renewal", ca_expires_at = tracing::field::Empty, previous_ca_expires_at = expires, meshes = meshes.len(), reason = tracing::field::Empty, outcome = tracing::field::Empty);
    match r.issuer.renew_ca() {
        Ok(Some((ca, previous))) => {
            span.record("reason", if behind_row { "behind-ca-row" } else { "renew-at" });
            span.record("ca_expires_at", ca.expiry_ms());
            span.record("previous_ca_expires_at", previous);
            span.record("outcome", "minted");
            r.ca_due.lock().unwrap().extend(meshes);
            span.in_scope(|| tracing::info!("the fabric CA is renewed under the same root: every mesh's issuer is re-minted under it"));
            tracing::Instrument::instrument(write_ca_row(r, |row| *row = crate::fabric_storage::CaRow { expires_at: ca.expiry_ms(), previous_expires_at: previous, issuer_receipts: Default::default() }), span).await;
        }
        Ok(None) => {}
        Err(e) => {
            span.record("outcome", "refused");
            span.in_scope(|| tracing::warn!(error = %e, "the fabric CA could not be renewed: tried again next tick"));
        }
    }
}

/// `fabric.root.update`'s phases (fabric-ca-renew.md "Numbered end-to-end sequence (root update)") on the
/// fabric-primary's tick, from the roots row and the next root it holds: `minted`, both roots to every
/// live birth, node-admins first, the secret to the admins (row `distributed` on the last receipt);
/// `distributed`, a new fabric CA signed by the new root, every mesh's issuer due under it (row
/// `ca-rotated`); `ca-rotated`, once every live birth's member cert chains to the new root, the new
/// root alone to every live birth (row `retired` on the last receipt, the old secret destroyed here as
/// on every admin). A birth not answered is sent again on the next tick; a birth born mid-rotation is
/// sent the set its phase holds. One span per phase move, `rdm.node_admin.root.update.via-caller`.
async fn rotate_roots(r: &Arc<Renewals>, view: &Topology) {
    use rdm_core::status::RootsPhase;
    let Some((_, fabric_id)) = r.fabric.get() else { return };
    let Some(row) = r.caches.fabric.get(fabric_id).and_then(|record| record.roots) else { return };
    if row.phase == RootsPhase::Retired {
        return;
    }
    let (Some(old), Some(new)) = (row.roots.first().cloned(), row.roots.get(1).cloned()) else { return };
    let both = vec![old.clone(), new.clone()];
    let move_to = |phase: RootsPhase| async move {
        let span = tracing::info_span!("rdm.node_admin.root.update.via-caller", node = %r.me, phase = ?phase, root_expires_at = tracing::field::Empty, previous_root_expires_at = row.expires_at, nodes = view.births().count() as u64, outcome = tracing::field::Empty);
        write_roots_row(r, |held| held.phase = phase).await;
        span.record("outcome", "moved");
        span.in_scope(|| tracing::info!("the root rotation moved to its next phase"));
        span
    };
    match row.phase {
        RootsPhase::Minted => {
            if distribute_roots(r, view, &both, RootsPhase::Distributed).await {
                move_to(RootsPhase::Distributed).await;
            }
        }
        RootsPhase::Distributed => {
            // A birth born since the set went out holds it before the CA moves under the new root.
            if !distribute_roots(r, view, &both, RootsPhase::Distributed).await {
                return;
            }
            match r.issuer.renew_ca_under_next_root() {
                Ok(Some((ca, previous))) => {
                    r.ca_due.lock().unwrap().extend(view.meshes.iter().map(|m| m.name.clone()));
                    tracing::Instrument::instrument(write_ca_row(r, |held| *held = crate::fabric_storage::CaRow { expires_at: ca.expiry_ms(), previous_expires_at: previous, issuer_receipts: Default::default() }), tracing::Span::current()).await;
                    move_to(RootsPhase::CaRotated).await.record("root_expires_at", ca.expiry_ms());
                }
                Ok(None) => tracing::warn!("the root rotation is at distributed and this fabric-primary holds no next root: nothing is minted"),
                Err(e) => tracing::warn!(error = %e, "the fabric CA could not be minted under the new root: tried again next tick"),
            }
        }
        RootsPhase::CaRotated => {
            if !distribute_roots(r, view, &both, RootsPhase::Distributed).await || !every_cert_under_new_ca(r, view).await {
                return;
            }
            if distribute_roots(r, view, &[new.clone()], RootsPhase::Retired).await {
                move_to(RootsPhase::Retired).await;
            }
        }
        RootsPhase::Retired => {}
    }
}

/// `node.roots.update` of `roots` (the set phase `phase` holds) to every live birth of `view` not yet
/// answered: node-admins first, the next root's secret to each admin, the fabric-primary itself in
/// process, each mesh's own primary directly and its other births through that primary. Whether every
/// live birth has answered.
async fn distribute_roots(r: &Arc<Renewals>, view: &Topology, roots: &[String], phase: rdm_core::status::RootsPhase) -> bool {
    let secret = r.issuer.next_root_secret().map(rdm_core::status::RootSecretHex);
    let mut births: Vec<(Node, IncarnationId)> = view.births().filter(|n| n.status.is_live()).filter_map(|n| Some((n.clone(), n.incarnation_id.clone()?))).collect();
    births.sort_by_key(|(n, _)| n.kind != NodeKind::NodeAdmin);
    let owed: Vec<(Node, IncarnationId)> = births.into_iter().filter(|(n, i)| !r.roots_acked.lock().unwrap().contains(&roots_key(&n.node_id, i, phase))).collect();
    let sends = owed.iter().map(|(n, incarnation)| {
        let secret = (n.kind == NodeKind::NodeAdmin).then(|| secret.clone()).flatten();
        let req = StatusRequest::UpdateRoots { node_id: n.node_id.clone(), incarnation: incarnation.clone(), roots: roots.to_vec(), phase, secret: secret.clone() };
        let carrier = view.node_type_primary(&n.mesh, NodeKind::NodeAdmin).map(|p| p.node_id.clone()).filter(|p| *p != n.node_id && !view.is_mesh_primary(&n.mesh, &r.node_id));
        async move {
            let reply = if n.node_id == r.node_id {
                r.own_roots.get().map(|own| own(incarnation, roots, phase, secret))
            } else {
                let out = match &carrier {
                    Some(c) => r.client.call_via::<Status>(&NodeTarget::ExactNode(c.clone()), &n.node_id, &req, &CallOptions::default()).await.0,
                    None => r.client.call::<Status>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &CallOptions::default()).await.0,
                };
                match out {
                    RpcOutcome::Reply(reply) => Some(reply.into_value()),
                    _ => None,
                }
            };
            if matches!(reply, Some(StatusReply::RootsUpdated { .. })) {
                r.roots_acked.lock().unwrap().insert(roots_key(&n.node_id, incarnation, phase));
            }
        }
    });
    futures_util::future::join_all(sends).await;
    view.births().filter(|n| n.status.is_live()).filter_map(|n| Some((n.node_id.clone(), n.incarnation_id.clone()?))).all(|(id, i)| r.roots_acked.lock().unwrap().contains(&roots_key(&id, &i, phase)))
}

/// The key of a birth's answer to a root set of phase `phase`.
fn roots_key(node_id: &NodeId, incarnation: &IncarnationId, phase: rdm_core::status::RootsPhase) -> String {
    format!("{node_id}/{}/{phase:?}", incarnation.as_str())
}

/// Whether every live birth of `view` holds a member cert issued under the new CA (fabric-ca-renew.md
/// step 4: the rotation retires the old root only once nothing chains to it). A row keeps a cert's
/// dates, not the cert: a member cert chains to the new root when its issuer is its mesh's first issuer
/// under the new CA or a later one, so its `issuer_expires_at` is at or past that mesh's receipt on the
/// CA row; every issuer under the old CA was minted before the new CA, and expires before the receipt.
async fn every_cert_under_new_ca(r: &Renewals, view: &Topology) -> bool {
    let Some((_, fabric_id)) = r.fabric.get() else { return false };
    let Some(ca) = r.caches.fabric.get(fabric_id).and_then(|record| record.ca) else { return false };
    for n in view.births().filter(|n| n.status.is_live()) {
        let Some(incarnation) = n.incarnation_id.as_ref() else { return false };
        let Some(receipt) = ca.issuer_receipts.get(&n.mesh).copied() else { return false };
        let dates = match r.caches.nodes.known_row(&*r.nodes_storage, &n.node_id).await {
            Ok(row) => row.and_then(|row| row.birth(incarnation).and_then(|b| b.cert.clone())),
            Err(_) => return false,
        };
        if !dates.is_some_and(|d| d.issuer_expires_at >= receipt) {
            return false;
        }
    }
    true
}

/// Change the roots row of the Fabric record the fabric-primary writes, as `change` says. A store that
/// refuses is named and the next tick writes again.
async fn write_roots_row(r: &Renewals, change: impl FnOnce(&mut crate::fabric_storage::RootsRow)) {
    let Some((storage, fabric_id)) = r.fabric.get() else { return };
    let written = r.caches.update_fabric(&**storage, fabric_id, None, |record| {
        if let Some(row) = record.roots.as_mut() {
            change(row);
        }
    }).await;
    if let Err(e) = written {
        tracing::warn!(error = %e, "the roots row is not written: written again next tick");
    }
}

/// Change the CA row of the Fabric record the fabric-primary writes, as `change` says (fabric-ca-renew.md
/// "Receipt": the CA row carries each mesh's issuer receipt). A store that refuses is named on the span
/// and the next tick writes again.
async fn write_ca_row(r: &Renewals, change: impl FnOnce(&mut crate::fabric_storage::CaRow)) {
    let Some((storage, fabric_id)) = r.fabric.get() else { return };
    let written = r.caches.update_fabric(&**storage, fabric_id, None, |record| change(record.ca.get_or_insert_with(Default::default))).await;
    if let Err(e) = written {
        tracing::warn!(error = %e, "the CA row is not written: written again next tick");
    }
}

/// Mint the next issuing key of `mesh` after the one expiring at `previous_expiry`, held here.
fn mint_next(r: &Renewals, mesh: &str, previous_expiry: u64) -> Option<IssuerMint> {
    let span = tracing::info_span!("rdm.node_admin.issuer.create.via-renewal", mesh, issuer_expires_at = tracing::field::Empty, previous_issuer_expires_at = previous_expiry, admins = tracing::field::Empty, reason = tracing::field::Empty, outcome = tracing::field::Empty);
    match r.issuer.renew_issuer(mesh) {
        Ok((_, mint)) => {
            r.ca_due.lock().unwrap().remove(mesh);
            span.record("issuer_expires_at", mint.expires_at);
            span.record("outcome", "minted");
            span.in_scope(|| tracing::info!("the mesh's issuing key is renewed under the root"));
            Some(mint)
        }
        Err(e) => {
            span.record("reason", e.to_string().as_str());
            span.record("outcome", "refused");
            span.in_scope(|| tracing::warn!("the mesh's issuer cannot be renewed"));
            None
        }
    }
}

/// `node.issuer.update` of the key `current` minted to each of `admins`, each on its own task: through
/// its carrier when one is named, else directly; this admin itself in process.
async fn update_admins(r: &Arc<Renewals>, mesh: &str, admins: Vec<(Node, Option<NodeId>)>, current: &IssuerMint, own_mesh: bool) {
    let Some(key) = r.issuer.held_issuer(mesh).filter(|k| k.expiry_ms() == current.expires_at) else { return };
    for (admin, carrier) in admins {
        let Some(incarnation) = admin.incarnation_id.clone() else { continue };
        if !r.updating.lock().unwrap().insert((admin.node_id.clone(), incarnation.clone())) {
            continue;
        }
        let (r2, current, key, mesh) = (r.clone(), current.clone(), key.clone(), mesh.to_string());
        tokio::spawn(async move {
            update_one(&r2, &mesh, &admin, carrier.as_ref(), &incarnation, &current, &key, own_mesh).await;
            r2.updating.lock().unwrap().remove(&(admin.node_id.clone(), incarnation));
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_one(r: &Renewals, mesh: &str, admin: &Node, carrier: Option<&NodeId>, incarnation: &IncarnationId, current: &IssuerMint, key: &rdm_core::certs::IssuingKey, own_mesh: bool) {
    let span = tracing::info_span!(
        "rdm.node_admin.issuer.update.via-renewal",
        node = %admin.name,
        node_id = %admin.node_id,
        incarnation_id = %incarnation.as_str(),
        sender = %r.me,
        carrier = %carrier.map(|c| c.to_string()).unwrap_or_default(),
        mesh,
        issuer_expires_at = current.expires_at,
        reason = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let outcome = if admin.node_id == r.node_id {
        // This admin holds it already: installed in process, with no call.
        tracing::info_span!("rdm.node_admin.issuer.update.via-installed", node = %r.me, node_id = %r.node_id, mesh, sender = %r.me, issuer_expires_at = current.expires_at)
            .in_scope(|| tracing::info!("the mesh's new issuer is installed in process"));
        Outcome::Installed
    } else {
        // The CA the issuer is minted under rides with it (Q275): this admin minted it under the CA it holds.
        let ca_key = rdm_core::status::CaKeyHex(r.issuer.ca_key_of(key).unwrap_or_default());
        let req = StatusRequest::UpdateIssuer {
            node_id: admin.node_id.clone(),
            incarnation: incarnation.clone(),
            key: IssuingKeyHex(rdm_core::certs::encode_key(key.secret())),
            cert: rdm_core::certs::encode_issuing_cert(key.cert()),
            ca_key,
        };
        let call = async {
            match carrier {
                Some(carrier) => r.client.call_via::<Status>(&NodeTarget::ExactNode(carrier.clone()), &admin.node_id, &req, &CallOptions::default()).await,
                None => r.client.call::<Status>(&NodeTarget::ExactNode(admin.node_id.clone()), &req, &CallOptions::default()).await,
            }
        };
        let (out, _) = tracing::Instrument::instrument(call, span.clone()).await;
        let shown = format!("{out:?}");
        match out {
            RpcOutcome::Reply(reply) => match reply.into_value() {
                // The answer names the expiry of the key installed: the one minted, or nothing is written.
                StatusReply::IssuerUpdated { issuer_expires_at, .. } if issuer_expires_at == current.expires_at => Outcome::Installed,
                StatusReply::IssuerUpdated { issuer_expires_at, .. } => {
                    span.record("reason", format!("the admin answered it holds an issuer expiring at {issuer_expires_at}, not the one minted ({}): nothing is written", current.expires_at).as_str());
                    Outcome::Refused
                }
                other => {
                    span.record("reason", format!("{}: {other:?}", other.name()).as_str());
                    Outcome::Refused
                }
            },
            RpcOutcome::NotSent(_) | RpcOutcome::Indeterminate(_) => {
                span.record("reason", shown.as_str());
                Outcome::NotSent
            }
            _ => {
                span.record("reason", shown.as_str());
                Outcome::Refused
            }
        }
    };
    // The confirmed dates are this fabric-primary's schedule. The written dates are the receipt
    // (Q141): an admin of its own mesh it writes on the admin's record, its own included; another
    // mesh's admin's record is written by that mesh's primary. A write refused leaves the record as it
    // was, so the next tick sends again.
    let outcome = match outcome {
        Outcome::Installed if own_mesh => {
            let dates = signing_dates(current);
            let base = crate::storage::NodeRow::new(admin.name.clone(), admin.node_id.clone(), admin.status);
            match r.caches.nodes.update(&*r.nodes_storage, &admin.node_id, Some(base), |rec| rec.birth_mut(incarnation, &admin.name).signing_cert = Some(dates)).await {
                Ok(_) => Outcome::Installed,
                Err(e) => {
                    span.record("reason", format!("the signing cert dates could not be written: {e}").as_str());
                    Outcome::NotSent
                }
            }
        }
        other => other,
    };
    if outcome == Outcome::Installed {
        r.signing.lock().unwrap().entry(mesh.to_string()).or_default().insert((admin.node_id.clone(), incarnation.clone()), signing_dates(current));
    }
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(outcome = outcome.as_str(), "mesh issuer update"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time_at(ms: u64) -> rdm_transport::clock::RafkaTime {
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(ms);
        time
    }

    // @feature: node-lifecycle
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:20
    /// "| Row | On `CertRenewed` the node's mesh-primary writes the birth's cert dates (`issued_at`, `expires_at`, `renew_at`) on the node's record and writes the record whole: the written expiry is the receipt. A mesh-primary's own cert, issued by the fabric-primary, it writes on its own record when it installs it. A renewal not answered writes nothing, and the row's `renew_at`, already passed, renews it on the next tick |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:28
    /// "2. It signs a fresh member cert for that exact birth with the mesh's current issuing key, on rafka-time."
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:33
    /// "5. The node's mesh-primary writes the expiry the node answered on the node's record. The old cert expires on its own."
    /// CONTRACT (node-cert-renew.md Row: "A mesh-primary's own cert, issued by the fabric-primary, it
    /// writes on its own record when it installs it"; Q141: on `CertRenewed` the row holds "that cert's
    /// `issued_at`, `expires_at` and `renew_at`"): the fabric-primary renews mesh2's primary, whose
    /// rafka-time is behind its own (gate 3 at e47aa813f: mesh2.admin.1's heartbeat `clock_skew_ms=-1`;
    /// here a second, so the clocks' own ticks between the two reads cannot hide it). The dates mesh2's
    /// primary writes on installing the cert are the issue's, `dates_of(&issue)`, whole: never its own
    /// clock beside the issue's expiry.
    #[test]
    fn a_mesh_primary_installing_its_renewed_cert_writes_the_issues_own_dates() {
        let root = rdm_core::certs::mint_key();
        let ca = |root: &rdm_core::certs::SigningKey| rdm_core::certs::CaKey::mint(root, rdm_core::certs::CA_CERT_TTL_SECS, 1_793_169_000_000);
        let fabric_primary = CertIssuer::with_root(root.clone(), ca(&root), None, time_at(1_793_169_598_407));
        let mesh_primary = CertIssuer::with_root(root.clone(), ca(&root), None, time_at(1_793_169_597_407));
        let name: PathName = "mesh2.admin.1".parse().unwrap();
        let birth = BirthIdentity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), mesh: name.mesh.clone(), name, endpoint_key: rdm_core::EndpointId("k".into()) };
        let issue = fabric_primary.renewal(&birth).expect("the fabric-primary issues mesh2's primary a renewed cert");
        let req = StatusRequest::RenewCert { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), cert: issue.bytes.clone() };
        assert_eq!(installed_dates(&req, &mesh_primary), Some(dates_of(&issue)), "the row mesh2's primary writes is the issue's dates, whole");
    }

    /// The fabric-primary on rafka-time `at`, holding a fresh root and CA.
    fn fabric_primary_at(at: u64) -> (CertIssuer, rdm_core::certs::SigningKey) {
        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, at - 1_000);
        (CertIssuer::with_root(root.clone(), ca, None, time_at(at)), root)
    }

    /// An admin's issuer on rafka-time `at` holding `root`: here only its clock and policy are read.
    fn admin_at(root: &rdm_core::certs::SigningKey, at: u64) -> CertIssuer {
        CertIssuer::with_root(root.clone(), rdm_core::certs::CaKey::mint(root, rdm_core::certs::CA_CERT_TTL_SECS, at - 1_000), None, time_at(at))
    }

    fn birth_of(name: &str) -> BirthIdentity {
        let name: PathName = name.parse().unwrap();
        BirthIdentity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), mesh: name.mesh.clone(), name, endpoint_key: rdm_core::EndpointId("k".into()) }
    }

    // @feature: node-lifecycle
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:19
    /// "| Receipt | On each admin's `IssuerUpdated` its mesh-primary writes its signing cert issue date and expiry on the admin's record (its own at its install); the workflow is complete when every admin in the view holds the new dates |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:33
    /// "4. Each admin's record holds the dates it answered, written by its mesh-primary. The old issuer expires on its own."
    /// CONTRACT: docs/architecture/lifecycles/node-issuer-update.md:34
    /// "4. The admin's mesh-primary writes the signing cert's issue date and expiry on the admin's record: its own at its install, another admin's when its `IssuerUpdated` returns through the mesh-primary as the carrier."
    /// CONTRACT (node-issuer-update.md step 4: a mesh-primary writes its own signing cert dates on its
    /// own record at its install; Q141, Q241: every cert row takes its dates from the cert it installs,
    /// never from a clock): the fabric-primary renews mesh2's issuer, and mesh2's primary, its clock a
    /// second behind, installs it. The signing cert dates it writes are the mint's, `signing_dates`,
    /// whole, renew point included.
    #[test]
    fn a_mesh_primary_installing_its_renewed_issuer_writes_the_mints_own_dates() {
        let (fabric_primary, root) = fabric_primary_at(1_793_169_598_407);
        let mesh_primary = admin_at(&root, 1_793_169_597_407);
        let (key, mint) = fabric_primary.renew_issuer("mesh2").expect("the fabric-primary renews mesh2's issuer");
        assert_eq!(installed_signing_dates(key.cert(), &mesh_primary), signing_dates(&mint), "the signing cert dates mesh2's primary writes are the mint's, whole");
    }

    // @feature: node-lifecycle
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:62
    /// "Failures and stream certainty follow the [caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty). A renewal that does not reach the node is retried every tick and marks the node nothing: reachability is [node recovery](node-recovery.md)'s business, not this primitive's. A successor primary folds the cert rows and continues the schedule from them."
    /// CONTRACT (fabric-certs.md §Tracking: "A successor mesh-primary reads its nodes' records and
    /// continues the schedule from them"; Q241: renew_at is derived from the cert's dates and the
    /// fabric's policy alone, so the issuer, the installer and any successor compute one value): the
    /// fabric-primary issues mesh2's primary a renewed cert; mesh2's primary installs it a second behind,
    /// and a successor fabric-primary a minute ahead reconstructs its dates from the same cert. All
    /// three hold one renew point, and one set of dates.
    #[test]
    fn a_renewed_certs_renew_point_is_one_value_at_its_issuer_its_installer_and_a_successor() {
        let (fabric_primary, root) = fabric_primary_at(1_793_169_598_407);
        let installer = admin_at(&root, 1_793_169_597_407);
        let successor = admin_at(&root, 1_793_169_658_407);
        let birth = birth_of("mesh2.admin.1");
        let issue = fabric_primary.renewal(&birth).expect("the fabric-primary issues the renewal");
        let req = StatusRequest::RenewCert { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), cert: issue.bytes.clone() };
        let (installed, reconstructed) = (installed_dates(&req, &installer).expect("the cert decodes"), installed_dates(&req, &successor).expect("the cert decodes"));
        assert_eq!((installed.renew_at, reconstructed.renew_at), (issue.renew_at, issue.renew_at), "one renew point: issuer {}, installer {}, successor {}", issue.renew_at, installed.renew_at, reconstructed.renew_at);
        assert_eq!((&installed, &reconstructed), (&dates_of(&issue), &dates_of(&issue)), "one set of dates");
    }

    // @feature: node-lifecycle
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:29
    /// "3. It sends `RenewCert` to the birth's held address. `NotSent` or `Indeterminate` is spanned and retried on the next tick, until the old cert expires; nothing else happens."
    /// CONTRACT (node-cert-renew.md step 3: an unanswered renewal is retried on the next tick; Q241:
    /// the dates are the cert's own): one issue reaches mesh2's primary twice, the second delivery
    /// later on its clock. Both installs write the issue's dates; the second moves nothing.
    #[test]
    fn the_same_issue_delivered_twice_keeps_its_dates() {
        let (fabric_primary, root) = fabric_primary_at(1_793_169_598_407);
        let mesh_primary = admin_at(&root, 1_793_169_598_407);
        let birth = birth_of("mesh2.admin.1");
        let issue = fabric_primary.renewal(&birth).expect("the fabric-primary issues the renewal");
        let req = StatusRequest::RenewCert { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), cert: issue.bytes.clone() };
        let first = installed_dates(&req, &mesh_primary);
        std::thread::sleep(std::time::Duration::from_millis(20));
        let again = installed_dates(&req, &mesh_primary);
        assert_eq!((first.clone(), again), (Some(dates_of(&issue)), Some(dates_of(&issue))), "both deliveries write the issue's dates: first {first:?}");
    }

    fn row(_node: &NodeId, _inc: &IncarnationId, issued_at: u64) -> CertDates {
        CertDates { issued_at, expires_at: issued_at + 12_000, issuer_expires_at: 1_000_000, renew_at: issued_at + 8_100 }
    }

    // @feature: node-lifecycle
    /// CONTRACT (fabric-certs.md §Tracking; node-cert-renew.md Row, Q141): a birth's cert row is its
    /// renewal state; it stalls once past its renew point by more than a floor with no renewal written.
    #[test]
    fn a_birth_stalls_past_its_renew_point_by_more_than_a_floor() {
        let (n, i) = (NodeId::mint(), IncarnationId::mint());
        let r = row(&n, &i, 9_000);
        assert!(!stalled(&r, 9_000 + 8_100 + 3_000, 3_000) && stalled(&r, 9_000 + 8_100 + 3_001, 3_000));
    }

    /// A nodes store refusing every write of a cert row while `refusing` is set: the issuer answered,
    /// then lost before its row was written.
    struct CertWriteLost {
        inner: crate::storage::MemoryNodesStorage,
        refusing: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl NodesStorage for CertWriteLost {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node(node_id).await
        }
        async fn node_at(&self, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node_at(name).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.nodes(mesh).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            if self.refusing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::storage_error::StorageError::Io { file: format!("nodes/{}/records/{}", row.name.mesh, row.node_id), reason: "the write was lost".into() });
            }
            self.inner.put_node(row).await
        }
        async fn index_path(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.index_path(name, node_id).await
        }
        async fn index_tags(&self, mesh: &str, node_id: &crate::model::NodeId, leaving: &std::collections::BTreeSet<String>, arriving: &std::collections::BTreeSet<String>) -> Result<(), crate::storage_error::StorageError> {
            self.inner.index_tags(mesh, node_id, leaving, arriving).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.tagged(label).await
        }
        async fn name_claim(&self, name: &crate::model::PathName) -> Result<Option<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.name_claim(name).await
        }
        async fn claim_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<Result<(), crate::storage::NameHeld>, crate::storage_error::StorageError> {
            self.inner.claim_node_name(name, node_id).await
        }
        async fn release_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.release_node_name(name, node_id).await
        }
    }

    // @feature: node-lifecycle
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:15
    /// "| Caller | The birth's issuer: its mesh-primary for a member, the fabric-primary for a mesh-primary; the fabric-primary renews its own cert in-process with its mesh's issuing key, with no call (`via-renewal`, outcome `installed`) |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:20
    /// "| Row | On `CertRenewed` the node's mesh-primary writes the birth's cert dates (`issued_at`, `expires_at`, `renew_at`) on the node's record and writes the record whole: the written expiry is the receipt. A mesh-primary's own cert, issued by the fabric-primary, it writes on its own record when it installs it. A renewal not answered writes nothing, and the row's `renew_at`, already passed, renews it on the next tick |"
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:27
    /// "1. The issuer's tick reads its cert rows; a row whose `renew_at` has passed names a birth to renew."
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:28
    /// "2. It signs a fresh member cert for that exact birth with the mesh's current issuing key, on rafka-time."
    /// CONTRACT: docs/architecture/lifecycles/node-cert-renew.md:33
    /// "5. The node's mesh-primary writes the expiry the node answered on the node's record. The old cert expires on its own."
    /// CONTRACT (node-cert-renew.md Row, Q141: "the written expiry is the receipt. A renewal not answered
    /// writes nothing, and the row's `renew_at`, already passed, renews it on the next tick";
    /// storage-by-object.md: "An issuer that answered the node but was lost before it wrote is the same
    /// case"): the fabric-primary renews its own cert in process; its row's write is lost, so the row
    /// still names the old cert and its passed renew point, and the next tick renews again, installs the
    /// newer cert and writes it. What must NOT happen: a row naming a cert that was never written, or no
    /// renewal after a lost write.
    #[tokio::test]
    async fn a_renewal_answered_but_lost_before_its_write_is_renewed_again_on_the_next_tick() {
        use crate::model::{Fabric, Mesh, ProviderKind, ScopeStatus};
        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, 1);
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(1_000_000);
        let issuer = Arc::new(CertIssuer::with_root(root, ca, None, time.clone()));
        let mut me = Node::allocated("mesh1.admin.1".parse().unwrap());
        me.incarnation_id = Some(IncarnationId::mint());
        me.is_primary = true;
        me.is_fabric_primary = true;
        let view = Topology {
            fabric: Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(crate::model::MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![me.clone()],
        };
        let incarnation = me.incarnation_id.clone().unwrap();
        // The join's cert: past its renew point, not yet expired.
        let joined = CertDates { issued_at: 1, expires_at: 9_000_000, issuer_expires_at: 9_000_000, renew_at: 500_000 };
        let store = Arc::new(CertWriteLost { inner: Default::default(), refusing: std::sync::atomic::AtomicBool::new(true) });
        let mut held = crate::storage::NodeRow::new(me.name.clone(), me.node_id.clone(), me.status);
        held.birth_mut(&incarnation, &me.name).cert = Some(joined.clone());
        store.inner.put_node(&held).await.unwrap();
        let cert_of = |store: Arc<CertWriteLost>, node_id: NodeId, inc: IncarnationId| async move { store.inner.node(&node_id).await.unwrap().and_then(|r| r.birth(&inc).and_then(|b| b.cert.clone())) };
        let installs = Arc::new(Mutex::new(0usize));
        let counted = installs.clone();
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let r = Renewals::new(me.name.clone(), me.node_id.clone(), issuer, store.clone(), Default::default(), client, Arc::new(move |_| *counted.lock().unwrap() += 1));
        let tick = |r: Arc<Renewals>| {
            let view = view.clone();
            async move {
                renew_members(&r, &view).await;
                for _ in 0..200 {
                    if r.in_flight.lock().unwrap().is_empty() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        };

        tick(r.clone()).await;
        assert_eq!(*installs.lock().unwrap(), 1, "the issuer renewed and installed its own cert");
        assert_eq!(cert_of(store.clone(), me.node_id.clone(), incarnation.clone()).await, Some(joined.clone()), "the write was lost: the record still names the join's cert and its passed renew point");

        store.refusing.store(false, std::sync::atomic::Ordering::SeqCst);
        tick(r.clone()).await;
        assert_eq!(*installs.lock().unwrap(), 2, "the next tick renews again");
        let renewed = cert_of(store.clone(), me.node_id.clone(), incarnation.clone()).await.expect("the birth's cert dates");
        assert!(renewed.issued_at > joined.issued_at && renewed.renew_at > 1_000_000, "the record names the renewed cert, written once installed: {renewed:?}");
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:10
    /// "- `fabric.ca.renew` rotates the CA internally, on policy, with nothing pushed to any node but the ordinary issuer and member renewals: the new CA chains to the same anchor, so every verifier's trust set is unchanged."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:24
    /// "| When | Its own tick, once the CA has lived `RDM_CERT_RENEW_AT` of its TTL (`RDM_CA_CERT_TTL_SECS`, default 365 days); also run by `fabric.root.update` step 3, under the new anchor |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:25
    /// "| Steps | 1 mint a new CA keypair and sign its cert with the root; write the CA row; 2 [mesh.issuer.renew](mesh-issuer-renew.md) for every mesh under the new CA, each `node.issuer.update` carrying the new CA material; members renew under the new issuers at their own points |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:28
    /// "| Receipt | The CA row carries each mesh's issuer receipt; complete when every mesh in the view has one |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:30
    /// "| Spans | `rdm.node_admin.ca.update.via-renewal` (ca_expires_at, previous_ca_expires_at, meshes, outcome) on the fabric-primary, and each `mesh.issuer.renew`'s spans |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:40
    /// "| Steps | 1 write the new root secret beside the old one on the fabric-primary; row `minted`; 2 `node.roots.update { roots: [old, new] }` to every node, admins first (the secret to admins, the public set to every other node); row `distributed`; 3 `fabric.ca.renew` under the new anchor, which re-mints every issuer; row `ca-rotated`; 4 wait until every cert row chains to the new anchor (at most one member TTL after step 3); 5 `node.roots.update { roots: [new] }` to every node; row `retired`; the old secret is destroyed everywhere |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:64
    /// "4. The tick waits until every cert row chains to the new anchor: at most one member TTL after step 3. A member whose renewal has not reached it is the ordinary not-sent case, retried; the rotation waits rather than retiring an anchor something still chains to."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:92
    /// "An internal `fabric.ca.renew` is step 3 alone, with no caller and no `node.roots.update`."
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:96
    /// "A node not reached in a phase is retried every tick; the phase does not advance without it, and nothing expires meanwhile that the overlap does not cover. A successor fabric-primary continues from the roots row and the CA row, and from its own copy of the root secret, which every node-admin holds ([custody](../fabric-certs.md#custody)). A successor that does not hold the CA the CA row names (a renewal whose step 2 had not reached it) never mints under a CA it does not hold: it runs `fabric.ca.renew` again from the root secret, and step 2 reaches every admin under the new CA."
    #[tokio::test]
    async fn renew_ca_mints_at_renew_point_or_when_behind_ca_row_and_records_ca_row_and_span() {
        use crate::model::{Fabric, FabricId, Mesh, MeshId, ProviderKind, ScopeStatus};
        use crate::span_capture::Capture;

        let capture = Capture::new("renew-ca-unit");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, 30, 1_000);
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(1_000);
        let issuer = Arc::new(CertIssuer::with_root(root, ca.clone(), None, time.clone()));
        issuer.set_policy(rdm_core::certs::CertPolicy { ca_ttl_secs: 30, ..Default::default() });
        let (_, first_expiry, renew_at) = issuer.ca_dates().unwrap();

        let fabric_id = FabricId::mint();
        let mut me = Node::allocated("mesh1.admin.1".parse().unwrap());
        let incarnation = IncarnationId::mint();
        me.incarnation_id = Some(incarnation.clone());
        me.is_primary = true;
        me.is_fabric_primary = true;
        me.status = crate::model::NodeStatus::Ready;
        let view = Topology {
            fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![me.clone()],
        };
        let nodes_store = Arc::new(crate::storage::MemoryNodesStorage::default());
        let fabric_store = Arc::new(crate::fabric_storage::MemoryFabricStorage::default());
        let identity = crate::fabric_storage::FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() };
        crate::fabric_storage::claim_identity(&*fabric_store, &identity).await.unwrap().unwrap();
        let caches = crate::record_cache::Caches::default();
        caches.update_fabric(&*fabric_store, &fabric_id, Some(crate::fabric_storage::FabricRecord::new(identity)), |_| {}).await.unwrap();
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let r = Renewals::new(me.name.clone(), me.node_id.clone(), issuer.clone(), nodes_store.clone(), caches, client, Arc::new(|_| {}));
        r.set_fabric(fabric_store.clone(), fabric_id.clone());

        // Before `renew_at` and not behind the CA row: nothing is minted.
        renew_ca(&r, &view, renew_at - 1).await;
        assert!(r.ca_due.lock().unwrap().is_empty());
        assert!(capture.named("rdm.node_admin.ca.update.via-renewal").is_empty());

        // At `renew_at`: `renew_ca` mints a new CA, writes the CA row, marks every mesh in `ca_due`, and emits the span.
        time.adopt(renew_at);
        renew_ca(&r, &view, renew_at).await;
        assert!(r.ca_due.lock().unwrap().contains("mesh1"));
        let ca_row = r.caches.fabric.get(&fabric_id).and_then(|rec| rec.ca).expect("CA row written");
        assert_eq!(ca_row.previous_expires_at, first_expiry);
        assert!(ca_row.expires_at > first_expiry);
        let spans = capture.named("rdm.node_admin.ca.update.via-renewal");
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].attributes.get("outcome").map(String::as_str), Some("minted"));
        assert_eq!(spans[0].attributes.get("reason").map(String::as_str), Some("renew-at"));
        assert_eq!(spans[0].attributes.get("meshes").map(String::as_str), Some("1"));

        // `every_cert_under_new_ca` is false before the mesh's issuer receipt and the birth's cert row chain to the new CA.
        assert!(!every_cert_under_new_ca(&r, &view).await);
        write_ca_row(&r, |row| {
            row.issuer_receipts.insert("mesh1".into(), 50_000);
        })
        .await;
        let mut node_row = crate::storage::NodeRow::new(me.name.clone(), me.node_id.clone(), me.status);
        node_row.birth_mut(&incarnation, &me.name).cert = Some(CertDates { issued_at: 1_000, expires_at: 13_000, issuer_expires_at: 40_000, renew_at: 9_000 });
        r.caches.nodes.update(&*nodes_store, &me.node_id, Some(node_row), |_| {}).await.unwrap();
        assert!(!every_cert_under_new_ca(&r, &view).await, "birth's cert still chains to an issuer older than the receipt");
        r.caches
            .nodes
            .update(&*nodes_store, &me.node_id, None, |rec| {
                rec.birth_mut(&incarnation, &me.name).cert.as_mut().unwrap().issuer_expires_at = 50_000;
            })
            .await
            .unwrap();
        assert!(every_cert_under_new_ca(&r, &view).await, "every live birth's cert chains to the new CA");

        // A successor whose held CA expires before the CA row's `expires_at` (`behind-ca-row`) renews immediately even before `renew_at`.
        write_ca_row(&r, |row| row.expires_at = 999_000).await;
        time.adopt(renew_at + 1_000);
        renew_ca(&r, &view, 0).await;
        let spans = capture.named("rdm.node_admin.ca.update.via-renewal");
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[1].attributes.get("reason").map(String::as_str), Some("behind-ca-row"));
        assert_eq!(spans[1].attributes.get("outcome").map(String::as_str), Some("minted"));
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:5
    /// "`mesh.issuer.renew` re-mints a mesh's issuing cert before it expires and carries it to that mesh's node-admins. It is run by the fabric-primary on its own tick, once the issuing cert has lived `RDM_CERT_RENEW_AT` (default two thirds) of its TTL (`RDM_ISSUING_CERT_TTL_SECS`, default 30 days). It is not a Build and not a caller op. Member certs issued after it carry the new issuer; the old issuer, and every member cert it signed, stay valid to their own expiry, so nothing is cut and nothing reconnects."
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:15
    /// "| Runner | The fabric-primary, which holds the fabric root |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:16
    /// "| Target | One mesh; every node-admin of it, exact births, in the view |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:18
    /// "| Order | The mesh-primary first (the fabric-primary's own mesh: installed in-process, with no call, spanned `via-installed`), then the others; a member renewal due in the window is signed with the new key once its own admin holds it |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:19
    /// "| Receipt | On each admin's `IssuerUpdated` its mesh-primary writes its signing cert issue date and expiry on the admin's record (its own at its install); the workflow is complete when every admin in the view holds the new dates |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:20
    /// "| Never | A member cert re-issued by this workflow (that is [node.cert.renew](node-cert-renew.md)'s on its own schedule), the root rotated (a fabric re-birth), a status set, a frame published |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:21
    /// "| Spans | `rdm.node_admin.issuer.create.via-renewal` (mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome) on the fabric-primary, and each `node.issuer.update`'s spans |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:25
    /// "1. The fabric-primary's tick reads the signing cert dates it holds in its cache, the dates it issued and each admin's `IssuerUpdated` confirmed; a mesh with a row whose `renew_at` has passed, and no newer issuing cert held, is renewed. It mints the issuing cert for that mesh with the root, on rafka-time, and holds it in process."
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:33
    /// "4. Each admin's record holds the dates it answered, written by its mesh-primary. The old issuer expires on its own."
    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:54
    /// "An admin that refuses the material by name (another mesh, a CA no root in its set verifies, an issuing cert the CA does not verify) ends the workflow `Failed { step: node.issuer.update:<path.name>, reason }`; the next tick runs it again from the rows. A successor fabric-primary reads the node-admin rows and continues the schedule from them."
    #[tokio::test]
    async fn renew_issuers_mints_at_renew_point_and_installs_in_process_for_own_mesh_primary_and_writes_signing_cert_row() {
        use crate::model::{Fabric, FabricId, Mesh, MeshId, ProviderKind, ScopeStatus};
        use crate::span_capture::Capture;

        let capture = Capture::new("renew-issuers-unit");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, 1_000);
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(10_000);
        let issuer = Arc::new(CertIssuer::with_root(root, ca, None, time.clone()));
        let mut me = Node::allocated("mesh1.admin.1".parse().unwrap());
        let incarnation = IncarnationId::mint();
        me.incarnation_id = Some(incarnation.clone());
        me.is_primary = true;
        me.is_fabric_primary = true;
        me.status = crate::model::NodeStatus::Ready;
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![me.clone()],
        };
        let nodes_store = Arc::new(crate::storage::MemoryNodesStorage::default());
        let mut held = crate::storage::NodeRow::new(me.name.clone(), me.node_id.clone(), me.status);
        let old_signing = SigningCertDates { issued_at: 1_000, expires_at: 30_000, renew_at: 8_000 };
        held.birth_mut(&incarnation, &me.name).signing_cert = Some(old_signing.clone());
        nodes_store.put_node(&held).await.unwrap();
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let r = Renewals::new(me.name.clone(), me.node_id.clone(), issuer, nodes_store.clone(), Default::default(), client, Arc::new(|_| {}));

        // A successor fabric-primary reads the node-admin rows (`signing_of`) and sees `renew_at = 8_000 <= now = 10_000`, so `renew_issuers` mints and installs in-process.
        renew_issuers(&r, &view).await;
        for _ in 0..200 {
            if r.updating.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let create_spans = capture.named("rdm.node_admin.issuer.create.via-renewal");
        assert_eq!(create_spans.len(), 1);
        assert_eq!(create_spans[0].attributes.get("mesh").map(String::as_str), Some("mesh1"));
        assert_eq!(create_spans[0].attributes.get("previous_issuer_expires_at").map(String::as_str), Some("30000"));
        assert_eq!(create_spans[0].attributes.get("outcome").map(String::as_str), Some("minted"));
        let installed_spans = capture.named("rdm.node_admin.issuer.update.via-installed");
        assert_eq!(installed_spans.len(), 1);
        assert_eq!(installed_spans[0].attributes.get("node").map(String::as_str), Some("mesh1.admin.1"));
        let written = nodes_store.node(&me.node_id).await.unwrap().and_then(|row| row.birth(&incarnation).and_then(|b| b.signing_cert.clone())).expect("signing_cert written on record");
        assert!(written.expires_at > old_signing.expires_at);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-issuer-renew.md:21
    /// "| Spans | `rdm.node_admin.issuer.create.via-renewal` (mesh, issuer_expires_at, previous_issuer_expires_at, admins, outcome) on the fabric-primary, and each `node.issuer.update`'s spans |"
    /// CONTRACT: docs/architecture/lifecycles/node-issuer-update.md:22
    /// "| Spans | `rdm.node_admin.issuer.update.via-renewal` (node, node_id, incarnation_id, mesh, issuer_expires_at, previous_issuer_expires_at, outcome) on the fabric-primary; `rdm.node_admin.issuer.update.via-installed` (node, node_id, mesh, sender, issuer_expires_at, previous_issuer_expires_at) on the admin, `sender` the node the call reached it from: the fabric-primary, or the mesh-primary carrying it |"
    #[tokio::test]
    #[ignore = "page/code difference: node-issuer-update.md §Operation contract"]
    async fn issuer_renewal_spans_record_admins_count_and_previous_issuer_expires_at() {
        use crate::model::{Fabric, FabricId, Mesh, MeshId, ProviderKind, ScopeStatus};
        use crate::span_capture::Capture;

        let capture = Capture::new("renew-issuers-span-fields");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, 1_000);
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(10_000);
        let issuer = Arc::new(CertIssuer::with_root(root, ca, None, time));
        let mut me = Node::allocated("mesh1.admin.1".parse().unwrap());
        let incarnation = IncarnationId::mint();
        me.incarnation_id = Some(incarnation.clone());
        me.is_primary = true;
        me.is_fabric_primary = true;
        me.status = crate::model::NodeStatus::Ready;
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![me.clone()],
        };
        let nodes_store = Arc::new(crate::storage::MemoryNodesStorage::default());
        let mut held = crate::storage::NodeRow::new(me.name.clone(), me.node_id.clone(), me.status);
        held.birth_mut(&incarnation, &me.name).signing_cert = Some(SigningCertDates { issued_at: 1_000, expires_at: 30_000, renew_at: 8_000 });
        nodes_store.put_node(&held).await.unwrap();
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let r = Renewals::new(me.name.clone(), me.node_id.clone(), issuer, nodes_store, Default::default(), client, Arc::new(|_| {}));

        renew_issuers(&r, &view).await;
        for _ in 0..200 {
            if r.updating.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let create = &capture.named("rdm.node_admin.issuer.create.via-renewal")[0];
        assert_eq!(create.attributes.get("admins").map(String::as_str), Some("1"));
        let update = &capture.named("rdm.node_admin.issuer.update.via-renewal")[0];
        assert_eq!(update.attributes.get("previous_issuer_expires_at").map(String::as_str), Some("30000"));
        let installed = &capture.named("rdm.node_admin.issuer.update.via-installed")[0];
        assert_eq!(installed.attributes.get("previous_issuer_expires_at").map(String::as_str), Some("30000"));
    }
}
