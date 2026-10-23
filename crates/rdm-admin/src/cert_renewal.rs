//! Cert tracking and renewal (fabric-certs.md §Tracking; node-cert-renew.md, mesh-issuer-renew.md,
//! node-issuer-update.md).
//!
//! A birth's member cert dates are one row on the birth ([`NodeCertRow`]), written by its issuer when
//! the birth holds the cert: at its join's admission, and on a renewal's `CertRenewed` naming the
//! expiry the issuer signed. The written expiry is the receipt (Q141); an unanswered renewal writes
//! nothing, so the row's `renew_at`, already passed, renews it on the next tick. A node-admin birth's
//! signing cert dates are one row on it too ([`SigningCertRow`]), written by the fabric-primary on its
//! `IssuerUpdated`. Nothing about renewal lives only in memory but the issuing key minted for a
//! renewal in flight, so whichever admin is primary continues the schedule from the rows.
//!
//! - The issuer's tick ([`renew_members`]): a member is renewed by its mesh's primary, a mesh-primary
//!   by the fabric-primary, the fabric-primary by itself in process. A birth whose row has passed its
//!   renew point is issued a fresh cert and sent `RenewCert`, each tick until its cert expires, and
//!   marks the birth nothing.
//! - The fabric-primary's tick ([`renew_issuers`]): a mesh with a node-admin row whose signing cert
//!   has passed its renew point, and no newer issuing key held here, is given a new one, held in
//!   process, and sent by `node.issuer.update` to that mesh's primary directly; once its row holds the
//!   new dates, to each other admin of the mesh as a command forwarded through the mesh-primary
//!   (Forward 0x1A, the mesh-primary the exact carrier). Its own mesh's admins it calls directly,
//!   itself in process.
//! - `fabric.get`'s next expiry and a mesh's stalled renewals are read from the same rows
//!   ([`fabric_watch`]).

use crate::certs::{BirthIdentity, CertIssuer, IssueFailure, IssuerMint, MemberIssue};
use crate::model::{IncarnationId, Node, NodeId, NodeKind, PathName};
use crate::storage::{NodeCertRow, NodesStorage, SigningCertRow};
use crate::topology::Topology;
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{IssuingKeyHex, Status, StatusReply, StatusRequest};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Whether the birth whose cert row is `row` stalls at `now_ms` (fabric-certs.md §Tracking: "a row past
/// `renew_at` by more than a floor with no `node.cert.renewed`").
pub fn stalled(row: &NodeCertRow, now_ms: u64, floor_ms: u64) -> bool {
    now_ms > row.renew_at.saturating_add(floor_ms)
}

/// Each birth's cert row of `mesh` on `storage`, by exact birth.
pub async fn fold_of(storage: &dyn NodesStorage, mesh: &str) -> Result<HashMap<(NodeId, IncarnationId), NodeCertRow>, String> {
    let rows = storage.node_certs(mesh).await.map_err(|e| format!("the cert rows of mesh {mesh}: {e}"))?;
    Ok(rows.into_iter().map(|r| ((r.node_id.clone(), r.incarnation_id.clone()), r)).collect())
}

/// The row a member cert issue writes once its birth holds the cert.
pub fn row_of(birth: &BirthIdentity, issue: &MemberIssue) -> NodeCertRow {
    NodeCertRow { node_id: birth.node_id.clone(), name: birth.name.clone(), incarnation_id: birth.incarnation.clone(), issued_at: issue.issued_at, expires_at: issue.expires_at, issuer_expires_at: issue.issuer_expires_at, renew_at: issue.renew_at }
}

/// The signing cert row of the node-admin birth `admin` holding the issuing key `mint` minted.
pub fn signing_row(admin: &Node, incarnation: &IncarnationId, mint: &IssuerMint) -> SigningCertRow {
    SigningCertRow { node_id: admin.node_id.clone(), name: admin.name.clone(), incarnation_id: incarnation.clone(), issued_at: mint.issued_at, expires_at: mint.expires_at, renew_at: mint.renew_at }
}

/// The births the view holds at their exact incarnation, by (node id, incarnation).
fn births_of<'a>(view: &'a Topology, mesh: &str) -> HashMap<(NodeId, IncarnationId), &'a Node> {
    view.births().filter(|n| n.mesh == mesh).filter_map(|n| Some(((n.node_id.clone(), n.incarnation_id.clone()?), n))).collect()
}

/// What `fabric.get` shows of the cert rows (fabric-certs.md §Tracking): the next expiry across the
/// births the view holds, and each mesh's births whose renewal stalls at `now_ms`.
pub async fn fabric_watch(view: &Topology, storage: &dyn NodesStorage, now_ms: Option<u64>) -> (Option<u64>, BTreeMap<String, Vec<PathName>>) {
    let floor = rdm_core::cadence::staleness_floor().as_millis() as u64;
    let mut next: Option<u64> = None;
    let mut stalls: BTreeMap<String, Vec<PathName>> = BTreeMap::new();
    let meshes: BTreeSet<String> = view.births().map(|n| n.mesh.clone()).collect();
    for mesh in meshes {
        let Ok(folded) = fold_of(storage, &mesh).await else { continue };
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
        let primary = view.cohort_primary(&m.name, NodeKind::NodeAdmin).map(|p| p.node_id.clone());
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

/// What the renewal ticks act through.
pub struct Renewals {
    /// This admin.
    pub me: PathName,
    /// Its node id.
    pub node_id: NodeId,
    /// What it signs with.
    pub issuer: Arc<CertIssuer>,
    /// The nodes store: the cert dates on each birth.
    pub nodes_storage: Arc<dyn NodesStorage>,
    /// Its Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// Its own install, for its own renewal.
    pub install_own: InstallOwn,
    /// The births a renewal call is in flight to: a tick never sends a second.
    in_flight: Mutex<HashSet<(NodeId, IncarnationId)>>,
    /// The admins an issuer update call is in flight to.
    updating: Mutex<HashSet<(NodeId, IncarnationId)>>,
}

impl Renewals {
    /// The renewal ticks of the admin `me`.
    pub fn new(me: PathName, node_id: NodeId, issuer: Arc<CertIssuer>, nodes_storage: Arc<dyn NodesStorage>, client: Arc<NodeRpcClient>, install_own: InstallOwn) -> Arc<Self> {
        Arc::new(Self { me, node_id, issuer, nodes_storage, client, install_own, in_flight: Mutex::default(), updating: Mutex::default() })
    }
}

fn identity(n: &Node, incarnation: &IncarnationId) -> BirthIdentity {
    BirthIdentity { node_id: n.node_id.clone(), incarnation: incarnation.clone(), mesh: n.mesh.clone(), name: n.name.clone(), endpoint_key: n.endpoint_id.clone().unwrap_or_else(|| rdm_core::EndpointId(String::new())) }
}

/// One tick of `node.cert.renew` (node-cert-renew.md steps 1-5) on `view`: each birth this admin
/// renews whose cert row has passed its renew point is issued a fresh cert and sent it, each tick
/// until its cert expires; its row is written only on the answer. Each send runs on its own task; the
/// tick never waits for one.
pub async fn renew_members(r: &Arc<Renewals>, view: &Topology) {
    if r.issuer.is_no_certs() {
        return;
    }
    let Some(now) = r.issuer.now_ms() else { return };
    let mut folds: BTreeMap<String, HashMap<(NodeId, IncarnationId), NodeCertRow>> = BTreeMap::new();
    for n in renewed_by(view, &r.node_id) {
        let Some(incarnation) = n.incarnation_id.clone() else { continue };
        if !folds.contains_key(&n.mesh) {
            match fold_of(&*r.nodes_storage, &n.mesh).await {
                Ok(f) => {
                    folds.insert(n.mesh.clone(), f);
                }
                Err(why) => {
                    tracing::info_span!("rdm.node_admin.cert.reject.via-renewal", node = %n.name, reason = %why)
                        .in_scope(|| tracing::warn!("the cert rows cannot be read: no renewal this tick"));
                    continue;
                }
            }
        }
        let Some(held) = folds.get(&n.mesh).and_then(|f| f.get(&(n.node_id.clone(), incarnation.clone()))).cloned() else { continue };
        // Past its renew point and before its expiry: renewed again each tick until it answers.
        if now < held.renew_at || now >= held.expires_at {
            continue;
        }
        if !r.in_flight.lock().unwrap().insert((n.node_id.clone(), incarnation.clone())) {
            continue;
        }
        let birth = identity(&n, &incarnation);
        let issue = r.issuer.renewal(&birth);
        let r2 = r.clone();
        let n2 = n.clone();
        tokio::spawn(async move {
            renew_one(&r2, &n2, &birth, &held, issue).await;
            r2.in_flight.lock().unwrap().remove(&(n2.node_id.clone(), birth.incarnation.clone()));
        });
    }
}

async fn renew_one(r: &Renewals, n: &Node, birth: &BirthIdentity, held: &NodeCertRow, issue: Result<MemberIssue, IssueFailure>) {
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
            let row = row_of(birth, &issue);
            // The fabric-primary's own cert is renewed in process, with no call.
            if n.node_id == r.node_id {
                (r.install_own)(issue.bytes.clone());
                return written(r, &n.mesh, &row).await;
            }
            let req = StatusRequest::RenewCert { node_id: n.node_id.clone(), incarnation: birth.incarnation.clone(), cert: issue.bytes.clone() };
            let (out, _) = r.client.call::<Status>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &CallOptions::default()).await;
            let shown = format!("{out:?}");
            match out {
                RpcOutcome::Reply(reply) => match reply.into_value() {
                    // The answer names the expiry of the cert installed: the one issued here, or nothing is written.
                    StatusReply::CertRenewed { expires_at, .. } if expires_at == issue.expires_at => written(r, &n.mesh, &row).await,
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

/// Write the cert row of a renewal its birth holds: the written expiry is the receipt (Q141). A write
/// refused leaves the row as it was, so the next tick renews again.
async fn written(r: &Renewals, mesh: &str, row: &NodeCertRow) -> Outcome {
    match r.nodes_storage.put_node_cert(mesh, row).await {
        Ok(()) => Outcome::Installed,
        Err(e) => {
            tracing::Span::current().record("reason", format!("the cert row could not be written: {e}").as_str());
            Outcome::NotSent
        }
    }
}

/// One tick of `mesh.issuer.renew` (mesh-issuer-renew.md) on `view`, run while this admin is the
/// fabric-primary. Each mesh's node-admin births carry their signing cert dates on their rows: a mesh
/// with a row past its renew point, and no newer issuing key held here, is given a new one, held in
/// process. Until the key it replaces expires, the newest key held is sent by `node.issuer.update` to
/// each admin whose row does not hold it: the mesh's primary directly first, then each other admin as
/// a command forwarded through the mesh-primary once the primary's row holds it. The fabric-primary's
/// own mesh: itself in process, its other admins directly.
pub async fn renew_issuers(r: &Arc<Renewals>, view: &Topology) {
    if r.issuer.is_no_certs() || r.issuer.root().is_none() || !view.fabric_primary().is_some_and(|f| f.node_id == r.node_id) {
        return;
    }
    let Some(now) = r.issuer.now_ms() else { return };
    for m in &view.meshes {
        let admins: Vec<(Node, IncarnationId)> = view.births().filter(|n| n.mesh == m.name && n.kind == NodeKind::NodeAdmin).filter_map(|n| Some((n.clone(), n.incarnation_id.clone()?))).collect();
        // Every row of the mesh, a re-created admin's predecessor's included: the schedule is the rows'.
        let Ok(rows) = r.nodes_storage.signing_certs(&m.name).await else { continue };
        // The newest key is the one held here while no row names a newer, else the newest row's; its
        // renew point passed, the next is minted, and otherwise the held key is carried to every admin
        // lacking it.
        let newest = rows.iter().max_by_key(|row| row.expires_at);
        let newest_row = newest.map(|row| row.expires_at).unwrap_or(0);
        let held = r.issuer.held_mint(&m.name).filter(|mint| mint.expires_at >= newest_row);
        let due = match &held {
            Some(mint) => now >= mint.renew_at,
            None => newest.is_some_and(|row| now >= row.renew_at),
        };
        let current = match (due, held) {
            (true, held) => match mint_next(r, &m.name, held.map(|h| h.expires_at).unwrap_or(newest_row)) {
                Some(mint) => mint,
                None => continue,
            },
            (false, Some(mint)) => mint,
            (false, None) => continue,
        };
        let row_of_admin = |a: &Node, i: &IncarnationId| rows.iter().find(|row| row.node_id == a.node_id && row.incarnation_id == *i);
        let holds = |a: &Node, i: &IncarnationId| row_of_admin(a, i).is_some_and(|row| row.expires_at == current.expires_at);
        // A mesh's admins are carried the newest key until the one it replaces expires: an admin with
        // no row is carried it, one whose older key has expired is not.
        let lacking: Vec<&(Node, IncarnationId)> = admins.iter().filter(|(a, i)| !holds(a, i)).collect();
        if lacking.is_empty() || lacking.iter().all(|(a, i)| row_of_admin(a, i).is_some_and(|row| now >= row.expires_at)) {
            continue;
        }
        let Some(primary) = view.cohort_primary(&m.name, NodeKind::NodeAdmin).cloned() else { continue };
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
        update_admins(r, &m.name, sends, &current).await;
    }
}

/// Mint the next issuing key of `mesh` after the one expiring at `previous_expiry`, held here.
fn mint_next(r: &Renewals, mesh: &str, previous_expiry: u64) -> Option<IssuerMint> {
    let span = tracing::info_span!("rdm.fabric.issuer.update.via-renewal", mesh, issuer_expires_at = tracing::field::Empty, previous_issuer_expires_at = previous_expiry, admins = tracing::field::Empty, reason = tracing::field::Empty, outcome = tracing::field::Empty);
    match r.issuer.renew_issuer(mesh) {
        Ok((_, mint)) => {
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
async fn update_admins(r: &Arc<Renewals>, mesh: &str, admins: Vec<(Node, Option<NodeId>)>, current: &IssuerMint) {
    let Some(key) = r.issuer.held_issuer(mesh).filter(|k| k.expiry_ms() == current.expires_at) else { return };
    for (admin, carrier) in admins {
        let Some(incarnation) = admin.incarnation_id.clone() else { continue };
        if !r.updating.lock().unwrap().insert((admin.node_id.clone(), incarnation.clone())) {
            continue;
        }
        let (r2, current, key, mesh) = (r.clone(), current.clone(), key.clone(), mesh.to_string());
        tokio::spawn(async move {
            update_one(&r2, &mesh, &admin, carrier.as_ref(), &incarnation, &current, &key).await;
            r2.updating.lock().unwrap().remove(&(admin.node_id.clone(), incarnation));
        });
    }
}

async fn update_one(r: &Renewals, mesh: &str, admin: &Node, carrier: Option<&NodeId>, incarnation: &IncarnationId, current: &IssuerMint, key: &rdm_core::certs::IssuingKey) {
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
        let req = StatusRequest::UpdateIssuer {
            node_id: admin.node_id.clone(),
            incarnation: incarnation.clone(),
            key: IssuingKeyHex(rdm_core::certs::encode_key(key.secret())),
            cert: rdm_core::certs::encode_issuing_cert(key.cert()),
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
    // The written dates are the receipt (Q141): a write refused leaves the row as it was, so the next
    // tick sends again.
    let outcome = match outcome {
        Outcome::Installed => match r.nodes_storage.put_signing_cert(mesh, &signing_row(admin, incarnation, current)).await {
            Ok(()) => Outcome::Installed,
            Err(e) => {
                span.record("reason", format!("the signing cert row could not be written: {e}").as_str());
                Outcome::NotSent
            }
        },
        other => other,
    };
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(outcome = outcome.as_str(), "mesh issuer update"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(node: &NodeId, inc: &IncarnationId, issued_at: u64) -> NodeCertRow {
        NodeCertRow { node_id: node.clone(), name: "mesh1.rpc.1".parse().unwrap(), incarnation_id: inc.clone(), issued_at, expires_at: issued_at + 12_000, issuer_expires_at: 1_000_000, renew_at: issued_at + 8_100 }
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
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::storage_error::StorageError> {
            self.inner.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::storage_error::StorageError> {
            self.inner.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &crate::model::NodeId) -> Result<Option<crate::model::PathName>, crate::storage_error::StorageError> {
            self.inner.node_row_name(mesh, node_id).await
        }
        async fn remove_node_row(&self, name: &crate::model::PathName) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_row(name).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.tagged(label).await
        }
        async fn put_node_declared(&self, mesh: &str, row: &crate::storage::NodeDeclaredRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_declared(mesh, row).await
        }
        async fn remove_node_declared(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_declared(mesh, node_id, incarnation_id).await
        }
        async fn node_declared(&self, mesh: &str) -> Result<Vec<crate::storage::NodeDeclaredRow>, crate::storage_error::StorageError> {
            self.inner.node_declared(mesh).await
        }
        async fn put_node_operation(&self, mesh: &str, row: &crate::storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_operation(mesh, row).await
        }
        async fn node_operations(&self, mesh: &str) -> Result<Vec<crate::storage::NodeOperationRow>, crate::storage_error::StorageError> {
            self.inner.node_operations(mesh).await
        }
        async fn remove_node_operation(&self, mesh: &str, row: &crate::storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_operation(mesh, row).await
        }
        async fn put_node_silence(&self, mesh: &str, row: &crate::storage::NodeSilenceRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_silence(mesh, row).await
        }
        async fn node_silences(&self, mesh: &str) -> Result<Vec<crate::storage::NodeSilenceRow>, crate::storage_error::StorageError> {
            self.inner.node_silences(mesh).await
        }
        async fn node_data_dir_lost(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_lost(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_lost_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_lost_at(mesh, name).await
        }
        async fn put_node_data_dir_lost(&self, mesh: &str, row: &crate::storage::NodeDataDirLostRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_data_dir_lost(mesh, row).await
        }
        async fn node_data_dir_unreadable(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_unreadable(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_unreadable_at(mesh, name).await
        }
        async fn put_node_data_dir_unreadable(&self, mesh: &str, row: &crate::storage::NodeDataDirUnreadableRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_data_dir_unreadable(mesh, row).await
        }
        async fn remove_node_data_dir_unreadable(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId, name: &crate::model::PathName) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_data_dir_unreadable(mesh, node_id, incarnation_id, name).await
        }
        async fn node_certs(&self, mesh: &str) -> Result<Vec<crate::storage::NodeCertRow>, crate::storage_error::StorageError> {
            self.inner.node_certs(mesh).await
        }
        async fn signing_certs(&self, mesh: &str) -> Result<Vec<crate::storage::SigningCertRow>, crate::storage_error::StorageError> {
            self.inner.signing_certs(mesh).await
        }
        async fn put_signing_cert(&self, mesh: &str, row: &crate::storage::SigningCertRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_signing_cert(mesh, row).await
        }
        async fn put_node_cert(&self, mesh: &str, row: &NodeCertRow) -> Result<(), crate::storage_error::StorageError> {
            if self.refusing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::storage_error::StorageError::Io { file: format!("nodes/{mesh}/cert/{}", row.key()), reason: "the writer was lost before the write".into() });
            }
            self.inner.put_node_cert(mesh, row).await
        }
    }

    // @feature: node-lifecycle
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
            fabric: Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(crate::model::MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
            nodes: vec![me.clone()],
        };
        let incarnation = me.incarnation_id.clone().unwrap();
        // The join's cert: past its renew point, not yet expired.
        let joined = NodeCertRow { node_id: me.node_id.clone(), name: me.name.clone(), incarnation_id: incarnation.clone(), issued_at: 1, expires_at: 9_000_000, issuer_expires_at: 9_000_000, renew_at: 500_000 };
        let store = Arc::new(CertWriteLost { inner: Default::default(), refusing: std::sync::atomic::AtomicBool::new(true) });
        store.inner.put_node_cert("mesh1", &joined).await.unwrap();
        let installs = Arc::new(Mutex::new(0usize));
        let counted = installs.clone();
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let r = Renewals::new(me.name.clone(), me.node_id.clone(), issuer, store.clone(), client, Arc::new(move |_| *counted.lock().unwrap() += 1));
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
        assert_eq!(store.inner.node_certs("mesh1").await.unwrap(), vec![joined.clone()], "the write was lost: the row still names the join's cert and its passed renew point");

        store.refusing.store(false, std::sync::atomic::Ordering::SeqCst);
        tick(r.clone()).await;
        assert_eq!(*installs.lock().unwrap(), 2, "the next tick renews again");
        let rows = store.inner.node_certs("mesh1").await.unwrap();
        assert_eq!(rows.len(), 1, "one row per birth: {rows:?}");
        assert!(rows[0].issued_at > joined.issued_at && rows[0].renew_at > 1_000_000, "the row names the renewed cert, written once installed: {rows:?}");
    }
}
