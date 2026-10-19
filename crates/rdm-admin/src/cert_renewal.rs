//! Cert tracking and renewal (fabric-certs.md §Tracking; node-cert-renew.md, mesh-issuer-renew.md,
//! node-issuer-update.md).
//!
//! Every issue is a blind put keyed (birth, `issued_at`) ([`NodeCertRow`]), and every receipt a put
//! under the same key ([`NodeCertReceiptRow`]): a join's issue is received when the join admits the
//! birth with it, a renewal's when the birth answers `node.cert.renewed`. The fold of a birth's rows
//! ([`fold`]) is its renewal state: the cert it holds (its newest received issue) and the renewal
//! not yet received, if any. Nothing about renewal lives only in memory, so whichever admin holds
//! the seat continues the schedule from the rows.
//!
//! - The issuing authority's tick ([`renew_members`]): a member is renewed by its mesh's primary, a
//!   mesh-primary by the fabric-primary, the fabric-primary by itself in process. A birth whose held
//!   cert has passed its renew point is issued a fresh cert (the row put before the send) and sent
//!   `RenewCert`; a renewal not received is sent again each tick, the same issue signed again, until
//!   the held cert expires, and marks the birth nothing.
//! - The fabric-primary's tick ([`renew_issuers`]): a mesh whose newest issuer row has passed its renew
//!   point is given a new issuer (the row put first), installed in process on the fabric-primary and
//!   carried by `node.issuer.update` to its mesh-primary, then to each other admin of the mesh.
//! - `fabric.get`'s next expiry and a mesh's stalled renewals are read from the same fold
//!   ([`fabric_watch`]).

use crate::certs::{BirthIdentity, CertIssuer, IssueFailure, MemberIssue};
use crate::model::{IncarnationId, Node, NodeId, NodeKind, PathName};
use crate::status_storage::{MeshIssuerReceiptRow, MeshIssuerRow, NodeCertReceiptRow, NodeCertRow, StatusStorage};
use crate::topology::Topology;
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{IssuingKeyHex, Status, StatusReply, StatusRequest};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A birth's renewal state, folded from its cert rows and their receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BirthCert {
    /// The issue the birth holds: its newest received one.
    pub held: NodeCertRow,
    /// The newest issue, when it is newer than the one held and not yet received.
    pub pending: Option<NodeCertRow>,
}

impl BirthCert {
    /// Whether the birth's renewal stalls at `now_ms` (fabric-certs.md §Tracking: "a row past
    /// `renew_at` by more than a floor with no `node.cert.renewed`").
    pub fn stalled(&self, now_ms: u64, floor_ms: u64) -> bool {
        now_ms > self.held.renew_at.saturating_add(floor_ms)
    }
}

/// Every birth's renewal state among `rows` and `receipts`, by exact birth. A birth none of whose
/// issues was received holds its first.
pub fn fold(rows: Vec<NodeCertRow>, receipts: Vec<NodeCertReceiptRow>) -> HashMap<(NodeId, IncarnationId), BirthCert> {
    let received: HashSet<String> = receipts.iter().map(NodeCertReceiptRow::key).collect();
    let mut by_birth: HashMap<(NodeId, IncarnationId), Vec<NodeCertRow>> = HashMap::new();
    for r in rows {
        by_birth.entry((r.node_id.clone(), r.incarnation_id.clone())).or_default().push(r);
    }
    by_birth
        .into_iter()
        .filter_map(|(birth, mut issues)| {
            issues.sort_by_key(|r| r.issued_at);
            let held = issues.iter().rev().find(|r| received.contains(&r.key())).or_else(|| issues.first())?.clone();
            let pending = issues.last().filter(|r| r.issued_at > held.issued_at).cloned();
            Some((birth, BirthCert { held, pending }))
        })
        .collect()
}

/// The fold of `mesh`'s cert rows on `storage`.
pub async fn fold_of(storage: &dyn StatusStorage, mesh: &str) -> Result<HashMap<(NodeId, IncarnationId), BirthCert>, String> {
    let rows = storage.node_certs_of(mesh).await.map_err(|e| format!("the cert rows of mesh {mesh}: {e}"))?;
    let receipts = storage.node_cert_receipts_of(mesh).await.map_err(|e| format!("the cert receipts of mesh {mesh}: {e}"))?;
    Ok(fold(rows, receipts))
}

/// The row a member cert issue puts.
pub fn row_of(birth: &BirthIdentity, issue: &MemberIssue) -> NodeCertRow {
    NodeCertRow { node_id: birth.node_id.clone(), name: birth.name.clone(), incarnation_id: birth.incarnation.clone(), issued_at: issue.issued_at, expires_at: issue.expires_at, issuer_expires_at: issue.issuer_expires_at, renew_at: issue.renew_at }
}

/// The receipt of the issue `row` names.
pub fn receipt_of(row: &NodeCertRow) -> NodeCertReceiptRow {
    NodeCertReceiptRow { node_id: row.node_id.clone(), incarnation_id: row.incarnation_id.clone(), issued_at: row.issued_at, expires_at: row.expires_at }
}

/// The issuer row a mint puts.
pub fn issuer_row(mint: &crate::certs::IssuerMint) -> MeshIssuerRow {
    MeshIssuerRow { mesh: mint.mesh.clone(), issued_at: mint.issued_at, expires_at: mint.expires_at, renew_at: mint.renew_at }
}

/// The births the view holds at their exact incarnation, by (node id, incarnation).
fn births_of<'a>(view: &'a Topology, mesh: &str) -> HashMap<(NodeId, IncarnationId), &'a Node> {
    view.births().filter(|n| n.mesh == mesh).filter_map(|n| Some(((n.node_id.clone(), n.incarnation_id.clone()?), n))).collect()
}

/// What `fabric.get` shows of the cert rows (fabric-certs.md §Tracking): the next expiry across the
/// births the view holds, and each mesh's births whose renewal stalls at `now_ms`.
pub async fn fabric_watch(view: &Topology, storage: &dyn StatusStorage, now_ms: Option<u64>) -> (Option<u64>, BTreeMap<String, Vec<PathName>>) {
    let floor = rdm_core::cadence::staleness_floor().as_millis() as u64;
    let mut next: Option<u64> = None;
    let mut stalled: BTreeMap<String, Vec<PathName>> = BTreeMap::new();
    let meshes: BTreeSet<String> = view.births().map(|n| n.mesh.clone()).collect();
    for mesh in meshes {
        let Ok(folded) = fold_of(storage, &mesh).await else { continue };
        let births = births_of(view, &mesh);
        for (birth, cert) in folded {
            let Some(node) = births.get(&birth) else { continue };
            next = Some(next.map_or(cert.held.expires_at, |n| n.min(cert.held.expires_at)));
            if now_ms.is_some_and(|now| cert.stalled(now, floor)) {
                stalled.entry(mesh.clone()).or_default().push(node.name.clone());
            }
        }
    }
    (next, stalled)
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
    /// The status rows.
    pub status_storage: Arc<dyn StatusStorage>,
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
    pub fn new(me: PathName, node_id: NodeId, issuer: Arc<CertIssuer>, status_storage: Arc<dyn StatusStorage>, client: Arc<NodeRpcClient>, install_own: InstallOwn) -> Arc<Self> {
        Arc::new(Self { me, node_id, issuer, status_storage, client, install_own, in_flight: Mutex::default(), updating: Mutex::default() })
    }
}

fn identity(n: &Node, incarnation: &IncarnationId) -> BirthIdentity {
    BirthIdentity { node_id: n.node_id.clone(), incarnation: incarnation.clone(), mesh: n.mesh.clone(), name: n.name.clone(), endpoint_key: n.endpoint_id.clone().unwrap_or_else(|| rdm_core::EndpointId(String::new())) }
}

/// One tick of `node.cert.renew` (node-cert-renew.md steps 1-5) on `view`: each birth this admin
/// renews whose held cert has passed its renew point is issued a fresh cert, its row put, and sent it;
/// a renewal not yet received is sent again, the same issue, until the held cert expires. Each send
/// runs on its own task; the tick never waits for one.
pub async fn renew_members(r: &Arc<Renewals>, view: &Topology) {
    if r.issuer.is_no_certs() {
        return;
    }
    let Some(now) = r.issuer.now_ms() else { return };
    let mut folds: BTreeMap<String, HashMap<(NodeId, IncarnationId), BirthCert>> = BTreeMap::new();
    for n in renewed_by(view, &r.node_id) {
        let Some(incarnation) = n.incarnation_id.clone() else { continue };
        if !folds.contains_key(&n.mesh) {
            match fold_of(&*r.status_storage, &n.mesh).await {
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
        let Some(cert) = folds.get(&n.mesh).and_then(|f| f.get(&(n.node_id.clone(), incarnation.clone()))).cloned() else { continue };
        let birth = identity(&n, &incarnation);
        let issue = match &cert.pending {
            // A renewal not received is sent again until the held cert expires; past that, nothing.
            Some(pending) if now < cert.held.expires_at => r.issuer.renewal_again(&birth, pending.issued_at, pending.expires_at),
            Some(_) => continue,
            None if now >= cert.held.renew_at => r.issuer.renewal(&birth),
            None => continue,
        };
        if !r.in_flight.lock().unwrap().insert((n.node_id.clone(), incarnation.clone())) {
            continue;
        }
        let r2 = r.clone();
        let n2 = n.clone();
        tokio::spawn(async move {
            renew_one(&r2, &n2, &birth, &cert, issue).await;
            r2.in_flight.lock().unwrap().remove(&(n2.node_id.clone(), birth.incarnation.clone()));
        });
    }
}

async fn renew_one(r: &Renewals, n: &Node, birth: &BirthIdentity, cert: &BirthCert, issue: Result<MemberIssue, IssueFailure>) {
    let span = tracing::info_span!(
        "rdm.node_admin.cert.update.via-renewal",
        node = %n.name,
        node_id = %n.node_id,
        incarnation_id = %birth.incarnation.as_str(),
        issued_at = tracing::field::Empty,
        expires_at = tracing::field::Empty,
        previous_expires_at = cert.held.expires_at,
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
            // The row is put before the send; a renewal sent again puts it only when its issuer moved.
            if cert.pending.as_ref() != Some(&row) {
                if let Err(e) = r.status_storage.put_node_cert_of(&n.mesh, &row).await {
                    tracing::Span::current().record("reason", format!("the cert row could not be put: {e}").as_str());
                    return Outcome::NotSent;
                }
            }
            // The fabric-primary's own cert is renewed in process, with no call.
            if n.node_id == r.node_id {
                (r.install_own)(issue.bytes.clone());
                return match r.status_storage.put_node_cert_receipt_of(&n.mesh, &receipt_of(&row)).await {
                    Ok(()) => Outcome::Installed,
                    Err(e) => {
                        tracing::Span::current().record("reason", format!("the receipt could not be put: {e}").as_str());
                        Outcome::NotSent
                    }
                };
            }
            let req = StatusRequest::RenewCert { node_id: n.node_id.clone(), incarnation: birth.incarnation.clone(), cert: issue.bytes.clone() };
            let (out, _) = r.client.call::<Status>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &CallOptions::default()).await;
            let shown = format!("{out:?}");
            match out {
                RpcOutcome::Reply(reply) => match reply.into_value() {
                    StatusReply::CertRenewed { .. } => match r.status_storage.put_node_cert_receipt_of(&n.mesh, &receipt_of(&row)).await {
                        Ok(()) => Outcome::Installed,
                        Err(e) => {
                            tracing::Span::current().record("reason", format!("the receipt could not be put: {e}").as_str());
                            Outcome::NotSent
                        }
                    },
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

/// One tick of `mesh.issuer.renew` (mesh-issuer-renew.md) on `view`, run while this admin holds the
/// fabric seat: each mesh whose newest issuer row has passed its renew point is given a new issuer,
/// its row put first and installed here; then every admin of the mesh in the view without a receipt
/// for the newest issuer is sent `node.issuer.update`, the mesh-primary first, until the issuer it
/// replaces expires.
pub async fn renew_issuers(r: &Arc<Renewals>, view: &Topology) {
    if r.issuer.is_no_certs() || r.issuer.root().is_none() {
        return;
    }
    let Some(now) = r.issuer.now_ms() else { return };
    for m in &view.meshes {
        let Ok(rows) = r.status_storage.mesh_issuers_of(&m.name).await else { continue };
        let Some(newest) = rows.iter().max_by_key(|row| row.issued_at).cloned() else { continue };
        let (current, previous) = if now >= newest.renew_at {
            match mint_next(r, &m.name, &newest).await {
                Some(next) => (next, Some(newest)),
                None => continue,
            }
        } else {
            let previous = rows.iter().filter(|row| row.issued_at < newest.issued_at).max_by_key(|row| row.issued_at).cloned();
            (newest, previous)
        };
        // Admins are carried the newest issuer until the one it replaces expires.
        if previous.as_ref().is_none_or(|p| now >= p.expires_at) {
            continue;
        }
        update_admins(r, view, &m.name, &current, previous.as_ref()).await;
    }
}

/// Mint the next issuer of `mesh` after `newest`, put its row and hold it here.
async fn mint_next(r: &Renewals, mesh: &str, newest: &MeshIssuerRow) -> Option<MeshIssuerRow> {
    let span = tracing::info_span!("rdm.fabric.issuer.update.via-renewal", mesh, issuer_expires_at = tracing::field::Empty, previous_issuer_expires_at = newest.expires_at, admins = tracing::field::Empty, reason = tracing::field::Empty, outcome = tracing::field::Empty);
    let minted = match r.issuer.renew_issuer(mesh) {
        Ok((_, mint)) => mint,
        Err(e) => {
            span.record("reason", e.to_string().as_str());
            span.record("outcome", "refused");
            span.in_scope(|| tracing::warn!("the mesh's issuer cannot be renewed"));
            return None;
        }
    };
    let row = crate::cert_renewal::issuer_row(&minted);
    span.record("issuer_expires_at", row.expires_at);
    if let Err(e) = r.status_storage.put_mesh_issuer_of(mesh, &row).await {
        span.record("reason", format!("the issuer row could not be put: {e}").as_str());
        span.record("outcome", "not-written");
        span.in_scope(|| tracing::warn!("the mesh's new issuer row was not written"));
        return None;
    }
    span.record("outcome", "minted");
    span.in_scope(|| tracing::info!("the mesh's issuer is renewed under the root"));
    Some(row)
}

/// `node.issuer.update` to every admin of `mesh` in `view` without a receipt for `current`: the
/// mesh-primary first, then the others; this admin itself in process.
async fn update_admins(r: &Arc<Renewals>, view: &Topology, mesh: &str, current: &MeshIssuerRow, previous: Option<&MeshIssuerRow>) {
    let Some(key) = r.issuer.held_issuer(mesh).filter(|k| k.expiry_ms() == current.expires_at) else { return };
    let received: HashSet<(NodeId, IncarnationId)> = r.status_storage.mesh_issuer_receipts_of(mesh).await.unwrap_or_default().into_iter().filter(|x| x.issued_at == current.issued_at).map(|x| (x.node_id, x.incarnation_id)).collect();
    let primary = view.cohort_primary(mesh, NodeKind::NodeAdmin).map(|p| p.node_id.clone());
    let mut admins: Vec<Node> = view.births().filter(|n| n.mesh == mesh && n.kind == NodeKind::NodeAdmin && n.incarnation_id.is_some()).cloned().collect();
    admins.sort_by_key(|n| (Some(&n.node_id) != primary.as_ref(), n.name.to_string()));
    for admin in admins {
        let incarnation = admin.incarnation_id.clone().expect("filtered on it");
        if received.contains(&(admin.node_id.clone(), incarnation.clone())) {
            continue;
        }
        if !r.updating.lock().unwrap().insert((admin.node_id.clone(), incarnation.clone())) {
            continue;
        }
        let (r2, current, previous_expiry, key) = (r.clone(), current.clone(), previous.map(|p| p.expires_at), key.clone());
        let mesh = mesh.to_string();
        let primary_first = Some(&admin.node_id) == primary.as_ref();
        let update = async move {
            update_one(&r2, &mesh, &admin, &incarnation, &current, previous_expiry, &key).await;
            r2.updating.lock().unwrap().remove(&(admin.node_id.clone(), incarnation));
        };
        // The mesh-primary is updated before the others are sent theirs.
        if primary_first {
            update.await;
        } else {
            tokio::spawn(update);
        }
    }
}

async fn update_one(r: &Renewals, mesh: &str, admin: &Node, incarnation: &IncarnationId, current: &MeshIssuerRow, previous_expiry: Option<u64>, key: &rdm_core::certs::IssuingKey) {
    let span = tracing::info_span!(
        "rdm.node_admin.issuer.update.via-renewal",
        node = %admin.name,
        node_id = %admin.node_id,
        incarnation_id = %incarnation.as_str(),
        mesh,
        issuer_expires_at = current.expires_at,
        previous_issuer_expires_at = previous_expiry.unwrap_or(0),
        reason = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let receipt = MeshIssuerReceiptRow { mesh: mesh.to_string(), issued_at: current.issued_at, node_id: admin.node_id.clone(), incarnation_id: incarnation.clone(), issuer_expires_at: current.expires_at };
    let outcome = if admin.node_id == r.node_id {
        // This admin holds it already: installed in process, with no call.
        tracing::info_span!("rdm.node_admin.issuer.update.via-installed", node = %r.me, node_id = %r.node_id, mesh, issuer_expires_at = current.expires_at, previous_issuer_expires_at = previous_expiry.unwrap_or(0))
            .in_scope(|| tracing::info!("the mesh's new issuer is installed in process"));
        Outcome::Installed
    } else {
        let req = StatusRequest::UpdateIssuer {
            node_id: admin.node_id.clone(),
            incarnation: incarnation.clone(),
            key: IssuingKeyHex(rdm_core::certs::encode_key(key.secret())),
            cert: rdm_core::certs::encode_issuing_cert(key.cert()),
        };
        let (out, _) = tracing::Instrument::instrument(r.client.call::<Status>(&NodeTarget::ExactNode(admin.node_id.clone()), &req, &CallOptions::default()), span.clone()).await;
        let shown = format!("{out:?}");
        match out {
            RpcOutcome::Reply(reply) => match reply.into_value() {
                StatusReply::IssuerUpdated { .. } => Outcome::Installed,
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
    let outcome = match outcome {
        Outcome::Installed => match r.status_storage.put_mesh_issuer_receipt_of(mesh, &receipt).await {
            Ok(()) => Outcome::Installed,
            Err(e) => {
                span.record("reason", format!("the receipt could not be put: {e}").as_str());
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
    /// CONTRACT (fabric-certs.md §Tracking; node-cert-renew.md Row): a birth holds its newest received
    /// issue; a newer issue not yet received is its pending renewal; a birth none of whose issues was
    /// received holds its first. The fold reads the rows alone.
    #[test]
    fn a_birth_holds_its_newest_received_issue_and_a_newer_one_is_pending() {
        let (n, i) = (NodeId::mint(), IncarnationId::mint());
        let rows = vec![row(&n, &i, 1_000), row(&n, &i, 9_000), row(&n, &i, 17_000)];
        let folded = fold(rows.clone(), vec![receipt_of(&rows[0]), receipt_of(&rows[1])]);
        let b = &folded[&(n.clone(), i.clone())];
        assert_eq!((b.held.issued_at, b.pending.as_ref().map(|p| p.issued_at)), (9_000, Some(17_000)));
        assert!(!b.stalled(9_000 + 8_100 + 3_000, 3_000) && b.stalled(9_000 + 8_100 + 3_001, 3_000));
        let none_received = fold(rows.clone(), vec![]);
        assert_eq!((none_received[&(n.clone(), i.clone())].held.issued_at, none_received[&(n, i)].pending.as_ref().map(|p| p.issued_at)), (1_000, Some(17_000)));
    }
}
