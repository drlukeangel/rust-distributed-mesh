//! A birth heard again (Q87, Luke 2026-10-20): every node coming to ready from any other
//! state connects to its node-admin for its cert, its time and its cache, and the `pre_node_ready`
//! hook is where it does that; node-admins included, the Day-0 fabric-primary excepted. When the
//! tickle reaches a birth its authority holds off its mesh (node-recovery.md step 2, node-tickle.md
//! "What follows"), the authority begins the birth's next cycle and commands `node.mesh.join` to it,
//! back over its own call; the birth runs node.start steps 2 to 5 again on it and is published
//! ready only once its hook has passed (node-mesh-join.md step 10). One function, run by
//! the mesh-primary's offline ladder for its members and by the fabric-primary's primary watch for a
//! held primary; nothing re-publishes a presence by hand. `AdminRunner` registers the birth's
//! join (as it does a stopped birth's) so the `JoinService` admits the report.

use crate::model::{IncarnationId, Node};
use crate::status_rpc::StatusServiceSlot;
use rdm_core::status::{Status, StatusRequest};
use rdm_gossip::digest_view::DigestView;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

/// The births this process commanded back and has not heard ready since: one command
/// per silence. An admin looks at a silent birth every pass; the start it commanded takes the
/// birth off its mesh for the length of that start, and a second command inside it would start
/// it over.
fn commanded() -> &'static Mutex<HashMap<rdm_core::framing::Birth, Instant>> {
    static COMMANDED: OnceLock<Mutex<HashMap<rdm_core::framing::Birth, Instant>>> = OnceLock::new();
    COMMANDED.get_or_init(Default::default)
}

/// What an admin needs to command a birth back: this admin's client, `StatusService`, Build state,
/// runner (once built) and digest view: one function, run by the offline ladder and the primary watch.
pub struct HeardAgain {
    /// This admin's Node RPC client: the command goes out on it.
    pub client: Arc<NodeRpcClient>,
    /// This admin's `StatusService`: the birth's next cycle begins there.
    pub status_service: StatusServiceSlot,
    /// This admin's Build state: the current Build the command names.
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// This admin's runner, once built: it registers the birth's join before the command.
    pub runner: Arc<OnceLock<Arc<crate::admin::runner::AdminRunner>>>,
    /// This admin's digest view: whether the birth has been heard ready since.
    pub digest_view: DigestView,
    /// Where this admin, its mesh's primary, writes the result `rejoined` of a self-heal that brought a
    /// birth of its mesh back, and the application's `on_node_investigated`, sent from that write (Q312,
    /// Q422).
    pub investigated: crate::node_investigation::NodeInvestigatedWriter,
}

/// The handle, once this admin's runner is built.
pub type HeardAgainSlot = Arc<OnceLock<Arc<HeardAgain>>>;

/// How a self-heal's command to its birth ended (node-recovery.md step 3, Q393).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelfHealEnd {
    /// The birth answered with the command's own answer (`MeshJoined` to the command back, `Started` to
    /// the start): it is back, and the investigation ends `rejoined`.
    Answered(String),
    /// The command went out and was not answered by the end of its attempts, or was refused (or not
    /// sent, with nothing in flight holding the birth) to the end of the self-heal's `Send`: how it
    /// ended. The birth is written `dead`, and its recovery follows.
    Unanswered(String),
    /// node.start refused as already live: the birth runs.
    Live(String),
    /// No command went out, by name: a command or create of the primary's in flight on the birth, or
    /// one already sent this silence, whose own end is the birth's; or this admin is not the birth's
    /// commander now, and the primary its view names runs its own investigation. Nothing is concluded
    /// here.
    NotSent(String),
    /// One attempt the birth refused (any answer but the command's own), or one that sent nothing
    /// with nothing in flight holding the birth (no runner, no `StatusService`, a join not registered):
    /// not a return (note 0005 R1, R4), and sent again under the self-heal's `Send`. Never how a
    /// self-heal ends.
    Refused(String),
}

impl SelfHealEnd {
    /// `answered`, `unanswered`, `live`, `not-sent` or `refused`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Answered(_) => "answered",
            Self::Unanswered(_) => "unanswered",
            Self::Live(_) => "live",
            Self::NotSent(_) => "not-sent",
            Self::Refused(_) => "refused",
        }
    }
}

/// An attempt that sent nothing and names nothing in flight that holds the birth: this admin's runner
/// or `StatusService` not built yet, or the birth's join not registered (note 0005 R4).
fn holds_nothing(outcome: &str) -> bool {
    ["no-runner", "no-authority", "no-status-service", "join-not-registered"].iter().any(|p| outcome.starts_with(p))
}

/// How one attempt of a self-heal's command back ended, from `command_mesh_join`'s outcome and whether
/// its call was answered (note 0005 R1, R4): `MeshJoined` is the birth back; any other answer is a
/// refusal, sent again; a call whose attempts were spent unanswered is unanswered; nothing sent with
/// nothing holding the birth is sent again, and nothing sent for a command, create or reservation of
/// the birth in flight, or by an admin that is not its commander, is not sent.
fn join_attempt(outcome: String, answered: Option<bool>) -> SelfHealEnd {
    let round_1 = crate::node_investigation::ROUND_1;
    match answered {
        Some(true) if round_1 || outcome.starts_with("mesh-joined") => SelfHealEnd::Answered(outcome),
        Some(true) => SelfHealEnd::Refused(outcome),
        Some(false) => SelfHealEnd::Unanswered(outcome),
        None if !round_1 && holds_nothing(&outcome) => SelfHealEnd::Refused(outcome),
        None => SelfHealEnd::NotSent(outcome),
    }
}

/// How one attempt of a self-heal's start ended, from the birth's admission of the command (note 0005
/// R1): `Started` is the birth back; a start refused as already live is [`SelfHealEnd::Live`]; any
/// other refusal, and a start that failed at a step (it is stopped still), is sent again; a call that
/// never reached the birth, or whose reply never came, is unanswered.
fn start_attempt(admission: crate::deployment::pipeline::CommandAdmission) -> SelfHealEnd {
    use crate::deployment::pipeline::CommandAdmission as A;
    match admission {
        A::NotSent { reason, .. } => SelfHealEnd::Unanswered(format!("NotSent: {reason}")),
        A::Indeterminate { reason, .. } => SelfHealEnd::Unanswered(format!("Indeterminate: {reason}")),
        A::Refused { reply, .. } if reply.starts_with("rejected-invalid-node-transition") => SelfHealEnd::Live(reply),
        started @ (A::Started {} | A::AlreadyAdmitted {}) => SelfHealEnd::Answered(format!("{started:?}")),
        other if crate::node_investigation::ROUND_1 => SelfHealEnd::Answered(format!("{other:?}")),
        other => SelfHealEnd::Refused(format!("{other:?}")),
    }
}

/// A self-heal's command sent under its one `Send` (Q344; node-investigation.md step 5; note 0005 R1):
/// `attempt` is one command, bounded by its own call's `Send`, and an attempt [`SelfHealEnd::Refused`]
/// is made again after its delay on `pacing`, the one schedule (node-rpc.md §43), up to `send`'s
/// attempts. Still refused when they are spent, the command is [`SelfHealEnd::Unanswered`]: the birth
/// is written `dead`. Any other end of an attempt is the command's end.
pub(crate) async fn sent_until_answered<F, Fut>(send: rdm_transport::Send, pacing: rdm_transport::Pacing, mut attempt: F) -> SelfHealEnd
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = SelfHealEnd>,
{
    let attempts = if crate::node_investigation::ROUND_1 { 1 } else { send.attempts.max(1) };
    let mut last = String::new();
    for n in 1..=attempts {
        if n > 1 {
            tokio::time::sleep(pacing.after(u32::from(n - 1))).await;
        }
        match attempt().await {
            SelfHealEnd::Refused(why) => last = why,
            end => return end,
        }
    }
    SelfHealEnd::Unanswered(format!("{last} (refused to the end of the self-heal's Send: {attempts} attempts)"))
}

impl HeardAgain {
    /// Command `node`'s exact birth back through `node.mesh.join` (`command_mesh_join`) for its view
    /// (Q87). The command writes no result: a held birth that answered its tickle, or was heard again by
    /// its digest while its ladder ran, is `ready`, written by the tick (`NodeInvestigatedWriter::end`;
    /// node-investigation.md, Q420).
    pub async fn command(&self, node: &Node, incarnation: &IncarnationId) -> String {
        self.command_on(node, incarnation).await.0
    }

    /// `command_mesh_join` of `node`'s exact birth: the outcome's name, and whether the call that went out
    /// was answered (`None`: no call went out).
    async fn command_on(&self, node: &Node, incarnation: &IncarnationId) -> (String, Option<bool>) {
        let (outcome, answered) = match self.runner.get() {
            None => ("no-runner".to_string(), None),
            Some(runner) => command_mesh_join(&self.client, &self.status_service, &self.builds, runner, &self.digest_view, node, incarnation).await,
        };
        // A command not sent is spanned too, with why: the flame of a birth left off its mesh reads it.
        if !outcome.starts_with("mesh-joined") && !outcome.starts_with("commanded") {
            tracing::info_span!("rdm.node_admin.node.update.via-mesh-join-commanded", node_path = %node.path, node_name = %node.name, node_id = %node.node_id, incarnation_id = %incarnation.as_str(), sent = false, outcome = %outcome, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("a heard-again birth was looked at: node.mesh.join not sent"));
        }
        (outcome, answered)
    }

    /// A self-heal's command (node-recovery.md step 3, Q393; node-investigation.md step 5): `node`'s
    /// exact birth commanded to start (`start`) or back through node.mesh.join, for `reason`, its status
    /// in the topology `status`, `inspected` node.runtime.inspect's answer that led to the command.
    /// Answered, the investigation ends `rejoined` (Q422, Q423), written with `inspected`. A start
    /// refused as already live is the birth running: commanded back when [`crate::node_investigation::follows`]
    /// says so, its result naming `inspected` still (note 0005 R6); else (an ordered investigation whose
    /// topology holds the birth in a status it runs in) it ends `ready`, the birth's answer that it runs
    /// (note 0005 B). [`SelfHealEnd::Unanswered`] the birth's mesh-primary writes `dead`, and its
    /// re-create ends it `re-created`; [`SelfHealEnd::NotSent`] leaves its end to the command, create or
    /// primary that holds the birth (note 0005 R4).
    pub async fn self_heal(&self, node: &Node, incarnation: &IncarnationId, start: bool, inspected: &crate::deployment::provider::Inspected, reason: crate::node_investigation::Reason, status: crate::model::NodeStatus) -> SelfHealEnd {
        use crate::node_investigation::{follows, Finding, Follows};
        let end = if start { self.self_heal_start(node, incarnation, inspected, reason).await } else { self.self_heal_join(node, incarnation, inspected, reason).await };
        let SelfHealEnd::Live(_) = &end else { return end };
        match follows(&Finding::Running, reason, status) {
            Follows::MeshJoin if crate::node_investigation::ROUND_1 => self.self_heal_join(node, incarnation, &crate::deployment::provider::Inspected::Running, reason).await,
            Follows::MeshJoin => self.self_heal_join(node, incarnation, inspected, reason).await,
            _ => {
                if !crate::node_investigation::ROUND_1 {
                    self.write_end(node, incarnation, crate::wiring::NodeInvestigatedResult::Ready, None, reason).await;
                }
                end
            }
        }
    }

    /// A self-heal whose inspect answered `running` (node-recovery.md step 3, Q393): `node`'s exact birth
    /// commanded back through node.mesh.join under the self-heal's `Send` ([`sent_until_answered`]), each
    /// attempt under its call's own. Answered with `MeshJoined`, the investigation ends `rejoined` (Q422,
    /// Q423), written as its `rdm.node.investigated` event with `inspected`, the inspect's answer that
    /// led to it, for `reason`; refused or unanswered to the end, [`SelfHealEnd::Unanswered`], which the
    /// birth's mesh-primary writes `dead`.
    async fn self_heal_join(&self, node: &Node, incarnation: &IncarnationId, inspected: &crate::deployment::provider::Inspected, reason: crate::node_investigation::Reason) -> SelfHealEnd {
        let end = sent_until_answered(rdm_transport::Send::default(), rdm_transport::Pacing::from_cadences(), || async {
            let (outcome, answered) = self.command_on(node, incarnation).await;
            join_attempt(outcome, answered)
        })
        .await;
        if let SelfHealEnd::Answered(_) = &end {
            self.write_end(node, incarnation, crate::wiring::NodeInvestigatedResult::Rejoined, Some(inspected), reason).await;
        }
        end
    }

    /// A self-heal whose birth is stopped by no command (node-recovery.md step 3, Q393): its inspect
    /// answered `os-stopped`, or it answered its tickle `CurrentStopped` (note 0006). `node`'s exact birth
    /// commanded to start under the self-heal's `Send` ([`sent_until_answered`]), each attempt one
    /// [`Self::start_once`]. `Started`, the investigation ends `rejoined`, written as its event with
    /// `inspected`, the inspect's answer that led to it; a start refused as already live is
    /// [`SelfHealEnd::Live`]; refused or unanswered to the end, [`SelfHealEnd::Unanswered`].
    async fn self_heal_start(&self, node: &Node, incarnation: &IncarnationId, inspected: &crate::deployment::provider::Inspected, reason: crate::node_investigation::Reason) -> SelfHealEnd {
        let end = sent_until_answered(rdm_transport::Send::default(), rdm_transport::Pacing::from_cadences(), || self.start_once(node, incarnation, reason)).await;
        if let SelfHealEnd::Answered(_) = &end {
            self.write_end(node, incarnation, crate::wiring::NodeInvestigatedResult::Rejoined, Some(inspected), reason).await;
        }
        end
    }

    /// One attempt of a self-heal's start: `node`'s exact birth commanded to start (node.start,
    /// NodeCommand 0x22 `StartNode`), the mesh-primary's own command to its node, under its call's
    /// `Send` (`StatusService::carry_lifecycle`), its end read by [`start_attempt`]. A suspended runtime
    /// answers nothing. One `rdm.node_admin.node.update.via-start-commanded` span.
    async fn start_once(&self, node: &Node, incarnation: &IncarnationId, reason: crate::node_investigation::Reason) -> SelfHealEnd {
        let operation = rdm_core::OperationId::new("node.start", rdm_core::OperationSubject::Birth { node_id: node.node_id.clone(), incarnation: incarnation.clone() })
            .in_occurrence(rdm_core::OperationOccurrenceId::mint());
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-start-commanded",
            node_path = %node.path, node_name = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.as_str(),
            operation = %operation.action, operation_subject = ?operation.subject, operation_occurrence = ?operation.occurrence,
            reason = reason.as_str(),
            trigger = reason.trigger(),
            outcome = tracing::field::Empty,
            "otel.kind" = "internal"
        );
        let unsent = |why: String| if crate::node_investigation::ROUND_1 { SelfHealEnd::NotSent(why) } else { SelfHealEnd::Refused(why) };
        let ended = async {
            let Some(runner) = self.runner.get() else { return unsent("no-runner".into()) };
            let Some(status_service) = self.status_service.get() else { return unsent("no-status-service".into()) };
            {
                let view = status_service.topology.read().await;
                if let Err(why) = commands_join(&view, &status_service.node_id, node) {
                    return SelfHealEnd::NotSent(format!("not-commander: {why}"));
                }
            }
            // The start runs node.mesh.join on the birth (node-start.md step 2): its join report is
            // verified against the deployment this admin holds of it, as a commanded join's is.
            if let Err(why) = runner.stopped_birth_or_live(node).await.map(|(_, _, deployed)| runner.joins.expect(deployed)) {
                return unsent(format!("join-not-registered: {why}"));
            }
            let ctx = crate::deployment::pipeline::CommandContext { build_id: rdm_core::LifecycleOp::NO_BUILD.to_string(), operation };
            start_attempt(status_service.carry_lifecycle(node, true, &ctx).await)
        };
        let ended = tracing::Instrument::instrument(ended, span.clone()).await;
        span.record("outcome", ended.name());
        span.in_scope(|| tracing::info!(outcome = ended.name(), "a birth stopped by no command is commanded to start, sent until answered"));
        ended
    }

    /// The investigation of `node`'s birth at `incarnation` ends `result`, with `inspected`, the
    /// inspect's answer that led to it, written by the mesh-primary of its mesh once
    /// ([`crate::node_investigation::NodeInvestigatedWriter::end`]; fabric-node-hooks.md
    /// "on_node_investigated").
    async fn write_end(&self, node: &Node, incarnation: &IncarnationId, result: crate::wiring::NodeInvestigatedResult, inspected: Option<&crate::deployment::provider::Inspected>, reason: crate::node_investigation::Reason) {
        let Some(status_service) = self.status_service.get() else { return };
        let ids = {
            let view = status_service.topology.read().await;
            view.is_mesh_primary(&node.mesh, &status_service.node_id).then(|| crate::node_investigation::fabric_and_mesh_of(&view, node))
        };
        if let Some(ids) = ids {
            self.investigated.end(ids, node, incarnation, Some(result), inspected, reason).await;
        }
    }

    /// `node.name.update`'s rejoin (node-name-update.md step 6; Q347): the live birth `node` at the
    /// name its row now holds, its connections deleted and its `UpdateName` answered, is commanded
    /// `node.mesh.join` by this mesh-primary over its own call, its join registered first under the
    /// row's name so the `JoinService` admits its report (node-mesh-join.md step 4). Answers the birth's
    /// reply (`MeshJoined` once it is back on its mesh), or `Err` naming how the call ended. A command
    /// of the primary's own, never a Build or part of one (Q342). One
    /// `rdm.node_admin.node.update.via-mesh-join-commanded` span.
    pub async fn rejoin(&self, node: &Node, incarnation: &IncarnationId) -> Result<rdm_core::status::StatusReply, String> {
        let operation = rdm_core::OperationId::new("node.mesh.join", rdm_core::OperationSubject::Birth { node_id: node.node_id.clone(), incarnation: incarnation.clone() })
            .in_occurrence(rdm_core::OperationOccurrenceId::mint());
        let build_id = rdm_core::LifecycleOp::NO_BUILD;
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-mesh-join-commanded",
            node_path = %node.path, node_name = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.as_str(),
            build_id = %build_id,
            operation = %operation.action, operation_subject = ?operation.subject, operation_occurrence = ?operation.occurrence,
            cause = "node.name.update",
            outcome = tracing::field::Empty,
            "otel.kind" = "internal"
        );
        let ended = async {
            let runner = self.runner.get().ok_or_else(|| "no-runner: this admin's runner is not built yet".to_string())?;
            let status_service = self.status_service.get().ok_or_else(|| "no-authority: this admin's StatusService is not open yet".to_string())?;
            let (_, _, deployed) = runner.stopped_birth_or_live(node).await.map_err(|why| format!("join-not-registered: {why}"))?;
            let _ = runner.joins.expect(deployed);
            let command = crate::node_commands::CommandKey { operation: operation.clone(), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
            // Open from the send until the call ends, a cancelled caller's included.
            let call = status_service.commands.track(command);
            let req = StatusRequest::JoinMesh { node_id: node.node_id.clone(), incarnation: incarnation.clone(), operation: operation.clone(), primary: self.digest_view.primaries.mesh(&node.mesh) };
            let (out, _) = self.client.call::<Status>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &CallOptions::default()).await;
            drop(call);
            match out.reply() {
                Some(r) => Ok(r.value().clone()),
                None => Err(out.name().to_string()),
            }
        };
        let ended = tracing::Instrument::instrument(ended, span.clone()).await;
        span.record("outcome", match &ended { Ok(r) => r.name().to_string(), Err(why) => why.clone() }.as_str());
        span.in_scope(|| tracing::info!("a renamed birth is commanded back on its mesh through node.mesh.join"));
        ended
    }
}

/// Begin `node`'s next cycle (its declared state is measured from Pending again) and command
/// `node.mesh.join` to its exact birth, naming the primary's current Build; the reply's name, and
/// whether the call that went out was answered (`None`: no call went out). One span,
/// `rdm.node_admin.node.update.via-mesh-join-commanded`, with the outcome.
async fn command_mesh_join(
    client: &Arc<NodeRpcClient>,
    status_service: &StatusServiceSlot,
    builds: &Arc<dyn crate::build_state::BuildStateAdapter>,
    runner: &crate::admin::runner::AdminRunner,
    digest_view: &DigestView,
    node: &Node,
    incarnation: &IncarnationId,
) -> (String, Option<bool>) {
    // The Day-0 fabric-primary, and any fabric primary: no node above it to connect to (Luke,
    // Q87); it is never commanded back.
    if node.is_fabric_primary {
        return ("fabric-primary: no authority above it".to_string(), None);
    }
    // Only a birth the view holds off ready re-enters (Luke: "coming from a state that is
    // not ready"). What an admin just started has heard is not the birth's state (it holds every
    // member from the projection, unheard until their digests reach it); the topology is.
    if node.status == crate::model::NodeStatus::Ready {
        return ("ready-in-view: on its mesh".to_string(), None);
    }
    // A birth with a lifecycle command of its authority in flight (a stop, a start, a restart) is that
    // command's: the start it runs is the command's own (node-start.md), never this one.
    if let Some(op) = digest_view.in_flight().into_iter().find(|op| op.node_id == node.node_id && op.incarnation == *incarnation) {
        return (format!("in-flight: {:?}", op.operation), None);
    }
    // A birth whose node.create this admin runs is that create's (node-create.md step 3: the create
    // composes the whole node.start; `node.created` follows it): never commanded back while it runs.
    // The tickle lists it from the runtime row its admission kept before its digest was installed,
    // and hears it again at the install: a look inside the create's first start, not a return.
    if runner.joins.creating(&node.node_id) {
        return (format!("create-in-flight: node.create node_id={}", node.node_id), None);
    }
    // Once per silence: a birth commanded back is commanded again only after a staleness floor, and
    // only once this process has heard it ready since (the admin looks at it every pass
    // while its view still holds it silent).
    let key = rdm_core::framing::Birth { node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    if let Some(at) = commanded().lock().unwrap().get(&key).copied() {
        if at.elapsed() < digest_view.staleness_floor() {
            return ("commanded-already".to_string(), None);
        }
        let heard_ready_since = digest_view
            .get(node.node_id.as_str())
            .is_some_and(|(d, unheard)| d.node.incarnation == *incarnation && d.status == rdm_core::model::NodeStatus::Ready && Instant::now().checked_sub(unheard).is_some_and(|heard_at| heard_at > at));
        // Heard ready since, or unheard again (a commanded start that stopped at a step left the
        // birth off its mesh): a new silence, commanded again; else the one command stands.
        let unheard_again = !digest_view.get(node.node_id.as_str()).is_some_and(|(d, unheard)| d.node.incarnation == *incarnation && unheard < digest_view.staleness_floor());
        if !heard_ready_since && !unheard_again {
            return ("commanded-already".to_string(), None);
        }
    }
    // Reserved now, before the command goes out: concurrent looks at one birth command it once.
    commanded().lock().unwrap().insert(key.clone(), Instant::now());
    // Each command back is its own join (node-mesh-join.md step 10, Q97, Q99: one Pending per accepted
    // join report, per join and not per birth), so its operation carries an opaque occurrence ID minted
    // once at reservation: every resend of this one request carries it unchanged. The birth's
    // ready declaration carries the operation, and an operation row survives every cycle
    // begin with no state in its key (`NodeOperationRow::key`): a second command back in one Build
    // under the first one's operation would have its ready answered AlreadyApplied from the
    // first join's row, and the birth would stay Pending.
    let operation = rdm_core::OperationId::new("node.mesh.join", rdm_core::OperationSubject::Birth { node_id: node.node_id.clone(), incarnation: incarnation.clone() })
        .in_occurrence(rdm_core::OperationOccurrenceId::mint());
    // A command back is the primary's own work, never a Build or part of one (Q342).
    let build_id = rdm_core::LifecycleOp::NO_BUILD.to_string();
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-mesh-join-commanded",
        node_path = %node.path, node_name = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        build_id = %build_id,
        operation = %operation.action, operation_subject = ?operation.subject, operation_occurrence = ?operation.occurrence,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal"
    );
    let outcome = async {
        let Some(status_service) = status_service.get() else {
            return ("no-authority".to_string(), None);
        };
        // A command of this authority to the birth is open (its node.start after a stop, its stop):
        // the birth's lifecycle is that command's until its completion (node-start.md, node-stop.md).
        if status_service.commands.open_for(&node.node_id, incarnation) {
            commanded().lock().unwrap().remove(&key);
            return ("command-open: a start or stop of this authority is under way".to_string(), None);
        }
        // Commands go down (node-mesh-join.md, Commander): read in the view as it stands at the send, not
        // the one the look began in, where this admin may since have stopped being the mesh's primary.
        let view = status_service.topology.read().await;
        if let Err(why) = commands_join(&view, &status_service.node_id, node) {
            return (format!("not-commander: {why}"), None);
        }
        drop(view);
        // The join the command makes the birth report is verified against the deployment this admin
        // holds of it (node-mesh-join.md step 4): the live birth is registered as a start after a
        // stop registers a stopped one, from its runtime handle and its admitted digest.
        // Its report, when the `JoinService` accepts it, is where the birth's next cycle begins and it is
        // published Pending, before the join is answered (node-mesh-join.md step 10), as every
        // accepted join report is: nothing is written before the command reached it (node-start.md
        // step 2), and the birth's hook and its ready follow it.
        if let Err(why) = runner.stopped_birth_or_live(node).await.map(|(_, _, deployed)| runner.joins.expect(deployed)) {
            return (format!("join-not-registered: {why}"), None);
        }
        // The command back is this mesh-primary's command to the birth, open from the send to the answer
        // as every command it sends is, a cancelled caller's included: the birth's lifecycle is
        // the command's until then.
        let command = crate::node_commands::CommandKey { operation: operation.clone(), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
        let call = status_service.commands.track(command);
        // The command carries this admin's held mesh-primary record of the birth's mesh, so a
        // commanded node-admin runs its election over the same records (D57; Q335).
        let req = StatusRequest::JoinMesh {
            node_id: node.node_id.clone(),
            incarnation: incarnation.clone(),
            operation,
            primary: digest_view.primaries.mesh(&node.mesh),
        };
        let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &CallOptions::default()).await;
        drop(call);
        match out.reply() {
            Some(r) => (r.value().name().to_string(), Some(true)),
            None => (out.name().to_string(), Some(false)),
        }
    };
    let (outcome, answered) = tracing::Instrument::instrument(outcome, span.clone()).await;
    // A command the birth refused by name (it is not ready for one yet, or it answered anything but
    // `MeshJoined`) is looked at again on the next pass, and sent again under a self-heal's `Send`, not
    // held for a floor: the refusal is the birth's knowledge at that instant, not a new silence, and not a
    // return (note 0005 R1).
    let refused = !crate::node_investigation::ROUND_1 && answered == Some(true) && !outcome.starts_with("mesh-joined");
    if refused || outcome.starts_with("no-authority") || outcome.starts_with("join-not-registered") || outcome.starts_with("rejected-not-primary") || outcome.starts_with("not-ready") || outcome.starts_with("not-commander") {
        commanded().lock().unwrap().remove(&key);
    }
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(node_path = %node.path, node_name = %node.name, outcome = %outcome, "a birth heard again is commanded back through node.mesh.join"));
    (outcome, answered)
}

/// Whether the admin born as `me` commands `node.mesh.join` to `subject` in its view now
/// (node-mesh-join.md, Commander: "Standalone: the node's mesh-primary, after a tickle that reached
/// the exact birth; the fabric-primary for a mesh-primary. Commands go down"). `Err` names why not.
pub(crate) fn commands_join(view: &crate::topology::Topology, me: &crate::model::NodeId, subject: &Node) -> Result<(), String> {
    if view.is_mesh_primary(&subject.mesh, &subject.node_id) {
        return if view.is_fabric_primary(me) { Ok(()) } else { Err(format!("{} is the mesh-primary of {} and this admin is not the fabric-primary", subject.node_id, subject.mesh)) };
    }
    if view.is_mesh_primary(&subject.mesh, me) { Ok(()) } else { Err(format!("this admin is not the mesh-primary of {}, which commands {}", subject.mesh, subject.node_id)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeStatus};
    use crate::topology::Topology;

    fn at(name: rdm_core::PathName, mesh: &str, fixture_kind: String, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name, mesh, r#type);
        n.incarnation_id = Some(IncarnationId::mint());
        n.mesh = mesh.into();
        n.r#type = r#type.to_string();
        n.status = NodeStatus::Ready;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:12
    /// "5. The command back or the start, each under its `Send`: answered, the investigation ends
    ///  `rejoined`; unanswered at the end of its attempts, the birth is written `dead`. A birth written
    ///  `dead` ends it `re-created`."
    /// (review finding 1, R1: a refusal of the command back or the start is not a return, and the Send
    /// goes on to its end; note 0005 B): only `MeshJoined` answers a command back and only `Started`
    /// answers a start; every other answer is a refusal, sent again, as is an attempt that sent nothing
    /// with nothing in flight holding the birth; a start refused as already live re-routes; an attempt
    /// not sent for a command, create or reservation in flight, or by an admin that is no commander,
    /// ends the command there. Under the self-heal's one Send a refused command is made again after its
    /// delay up to the Send's attempts and is unanswered once they are spent. Must NOT happen: a refusal
    /// read as answered, or a command made once and given up on a refusal.
    // @feature: node-recovery
    #[tokio::test(start_paused = true)]
    async fn a_refused_command_back_or_start_is_sent_again_and_never_ends_rejoined() {
        use crate::deployment::pipeline::CommandAdmission as A;
        use rdm_core::outcome::Code;
        use std::sync::atomic::{AtomicUsize, Ordering};
        assert_eq!(join_attempt("mesh-joined".into(), Some(true)), SelfHealEnd::Answered("mesh-joined".into()));
        for refusal in ["not-ready", "rejected-not-primary", "start-failed", "rejected-stale-incarnation"] {
            assert_eq!(join_attempt(refusal.into(), Some(true)), SelfHealEnd::Refused(refusal.into()), "a command back refused {refusal} is no return");
        }
        assert_eq!(join_attempt("not-sent".into(), Some(false)), SelfHealEnd::Unanswered("not-sent".into()));
        for unsent in ["no-runner", "no-authority", "join-not-registered: no deployment held"] {
            assert_eq!(join_attempt(unsent.into(), None), SelfHealEnd::Refused(unsent.into()), "{unsent}: nothing holds the birth, so it is sent again");
        }
        for held in ["commanded-already", "in-flight: node.stop", "create-in-flight: node.create", "command-open: a start", "not-commander: another mesh-primary"] {
            assert_eq!(join_attempt(held.into(), None), SelfHealEnd::NotSent(held.into()), "{held}: the command ends there");
        }
        assert!(matches!(start_attempt(A::Started {}), SelfHealEnd::Answered(_)));
        assert!(matches!(start_attempt(A::Refused { reply: "rejected-invalid-node-transition: Ready".into(), code: Code::FailedPrecondition }), SelfHealEnd::Live(_)));
        for refused in [A::Refused { reply: "not-ready: a stop is still under way".into(), code: Code::Unavailable }, A::StartFailed { step: "pre_node_ready".into(), reason: "refused".into(), code: Code::FailedPrecondition }] {
            assert!(matches!(start_attempt(refused.clone()), SelfHealEnd::Refused(_)), "a start {refused:?} is no return");
        }
        assert!(matches!(start_attempt(A::NotSent { reason: "dial refused".into(), code: Code::Unavailable }), SelfHealEnd::Unanswered(_)));
        let pacing = rdm_transport::Pacing { jitter: false, ..rdm_transport::Pacing::from_cadences() };
        let send = rdm_transport::Send::default();
        let made = AtomicUsize::new(0);
        let end = sent_until_answered(send, pacing, || async { made.fetch_add(1, Ordering::SeqCst); SelfHealEnd::Refused("not-ready".into()) }).await;
        assert!(matches!(&end, SelfHealEnd::Unanswered(why) if why.contains("not-ready")), "refused to the end of the Send: unanswered: {end:?}");
        assert_eq!(made.load(Ordering::SeqCst), usize::from(send.attempts), "the Send's every attempt is made");
        made.store(0, Ordering::SeqCst);
        let end = sent_until_answered(send, pacing, || async { if made.fetch_add(1, Ordering::SeqCst) == 0 { SelfHealEnd::Refused("not-ready".into()) } else { SelfHealEnd::Answered("mesh-joined".into()) } }).await;
        assert_eq!((end, made.load(Ordering::SeqCst)), (SelfHealEnd::Answered("mesh-joined".into()), 2), "answered at the second attempt");
        made.store(0, Ordering::SeqCst);
        let end = sent_until_answered(send, pacing, || async { made.fetch_add(1, Ordering::SeqCst); SelfHealEnd::NotSent("commanded-already".into()) }).await;
        assert_eq!((end, made.load(Ordering::SeqCst)), (SelfHealEnd::NotSent("commanded-already".into()), 1), "not sent for a command in flight: no second attempt");
    }

    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:25
    /// "Caller: The primary above the node: its mesh-primary for a member, the fabric-primary for a
    ///  mesh-primary or another mesh's node-admin"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:31
    /// "What follows: The caller's: on `unreached` the investigation's verdict is the inspect's answer,
    ///  read before the tickle; on `reached` for a node that is off its channel it commands
    ///  [node.mesh.join](node-mesh-join.md)"
    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:46
    /// "5. The caller acts on the result per its own page. Nothing on this page writes a row, changes a
    ///  primary or publishes a frame."
    /// CONTRACT (node-mesh-join.md, Commander: "the node's mesh-primary ...; the fabric-primary for a
    /// mesh-primary. Commands go down"; #3211 red 2, gate 19d4afbee release: mesh2.node-admin.1 stopped
    /// publishing mesh2 at 484.371, held mesh2.node-admin.2's role at 484.404, and commanded
    /// `node.mesh.join:mesh2.node-admin.2` at 484.416): an admin commands a node of a mesh only as that
    /// mesh's primary, and a mesh-primary only as the fabric-primary. Must NOT happen: a former
    /// mesh-primary commanding the mesh's primary, or the fabric-primary commanding a peer mesh's node.
    #[test]
    fn only_the_mesh_primary_commands_its_node_and_only_the_fabric_primary_commands_a_mesh_primary() {
        let fp = at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin".to_string(), "node-admin", true, true);
        let rpc = at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node".to_string(), "node", false, false);
        let former = at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin".to_string(), "node-admin", false, false);
        let m2p = at(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin".to_string(), "node-admin", true, false);
        let broker = at(rdm_core::PathName::new("mesh2.broker", "1").unwrap(), "mesh2", "broker".to_string(), "node", false, false);
        let view = Topology {
            fabric: crate::model::Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), rpc.clone(), former.clone(), m2p.clone(), broker.clone()],
        };
        assert!(commands_join(&view, &former.node_id, &m2p).is_err(), "a former mesh-primary does not command the mesh's primary");
        assert!(commands_join(&view, &former.node_id, &broker).is_err(), "an admin that is not its mesh's primary commands none of its nodes");
        assert!(commands_join(&view, &fp.node_id, &broker).is_err(), "the fabric-primary commands a peer mesh's node only through its mesh-primary");
        assert_eq!(commands_join(&view, &fp.node_id, &m2p), Ok(()), "the fabric-primary commands a mesh-primary");
        assert_eq!(commands_join(&view, &m2p.node_id, &broker), Ok(()), "the mesh-primary commands its own mesh's node");
        assert_eq!(commands_join(&view, &m2p.node_id, &former), Ok(()), "the mesh-primary commands its own mesh's other node-admin");
        assert_eq!(commands_join(&view, &fp.node_id, &rpc), Ok(()), "the fabric-primary, mesh1's primary, commands mesh1's node");
    }
}
