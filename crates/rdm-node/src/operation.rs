//! Acceptance and response recovery for a mutating op, over persistence the application provides
//! (consumer-guide-ops.md §3, §5; node-rpc.md §33, §43).
//!
//! A product's mutating request carries its own operation id. The product provides the persistence
//! of its operations as an [`OperationStore`]: what it records is durable when the call returns, and
//! it decides nothing. [`Operations`] decides over it, the same way in every handler and every start:
//!
//! - **Acceptance** ([`Operations::decide`]): the operation's id is looked up first. A new id is
//!   recorded, then the request is accepted (`Acceptance::accepted`), so the runtime acks it only
//!   once the accepted work is recoverable. An id the store holds with its work not completed is
//!   [`Decision::InFlight`], and one completed is [`Decision::Completed`] with its recorded output:
//!   both are accepted at once, the acceptance record or the result being durable already. An id
//!   held with another input is [`Decision::Conflict`], which the protocol answers by its own named
//!   refusal. One decision at a time, so two invocations of one new id record it once.
//! - **Completion** ([`Operations::complete`]): the work's output is recorded once; an operation
//!   already completed keeps its first output.
//! - **Response recovery** ([`Operations::read`], and `accept`'s `Completed`): a caller whose reply
//!   was lost (`Indeterminate`) asks by the operation id and gets the recorded result, never a
//!   second execution.
//! - **The restart** ([`Operations::pending`]): every operation the store holds accepted and not
//!   completed, read in the node's `pre_node_ready` so its work is started again before the node is
//!   Ready; each finishes in [`Operations::complete`].
//!
//! What stays the application's: the work itself, run by its own mechanism; the atomicity of an
//! effect and its completion record (its transaction or idempotent operation, §5 step 3); and the
//! store. There is no transport delivery loop here and no replay: delivery is the runtime's (§43),
//! and a new invocation of the same operation is recognised by its id.
//!
//! Each decision is a span carrying the `operation` field: `rdm.node_rpc.operation.create.via-accept`
//! (`decision`), `rdm.node_rpc.operation.update.via-complete` (`recorded`),
//! `rdm.node_rpc.operation.serve.via-read` (`state`) and `rdm.node_rpc.operation.update.via-resume`
//! (`pending`), each a child of the span it is called in: the request's serve span, or the
//! `pre_node_ready` attempt.

use rdm_transport::Acceptance;
use tracing::Instrument;

/// What the application's store holds for one operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded<I, O> {
    /// The input it was accepted on.
    pub input: I,
    /// Its output, once its completion is recorded.
    pub output: Option<O>,
}

/// The application's persistence of its operations. Persistence only: every `put` is durable when
/// it returns `Ok`, and the decisions are [`Operations`]'. A store reached by more than one
/// [`Operations`] is outside this contract.
#[async_trait::async_trait]
pub trait OperationStore: Send + Sync + 'static {
    /// An operation's input, compared to tell a repeat of the operation from a reuse of its id.
    type Input: Clone + PartialEq + Send + Sync + 'static;
    /// An operation's output, its recorded result.
    type Output: Clone + Send + Sync + 'static;
    /// The store's failure, named.
    type Error: std::fmt::Display + Send + 'static;

    /// What the store holds for `operation`.
    async fn get(&self, operation: &str) -> Result<Option<Recorded<Self::Input, Self::Output>>, Self::Error>;
    /// Durably record `operation` accepted on `input`.
    async fn put_accepted(&self, operation: &str, input: &Self::Input) -> Result<(), Self::Error>;
    /// Durably record `operation` completed with `output`.
    async fn put_completed(&self, operation: &str, output: &Self::Output) -> Result<(), Self::Error>;
    /// Every operation held accepted and not completed, with its input.
    async fn pending(&self) -> Result<Vec<(String, Self::Input)>, Self::Error>;
}

/// What [`Operations::decide`] decided for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision<O> {
    /// New: recorded and accepted now. The handler starts its work.
    Recorded,
    /// Recorded before and not completed: its work is under way, or is finished by the next start.
    InFlight,
    /// Completed before: its recorded output, the reply a lost one is recovered as.
    Completed(O),
    /// Recorded before with another input: the protocol's named refusal.
    Conflict,
}

impl<O> Decision<O> {
    /// The decision's name, on its span.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::InFlight => "in-flight",
            Self::Completed(_) => "completed",
            Self::Conflict => "conflict",
        }
    }
}

/// Where one operation stands, read by its id ([`Operations::read`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State<O> {
    /// The store holds no operation of that id.
    ///
    /// `name()` is `unknown`.
    Unknown,
    /// Recorded, its work not completed.
    ///
    /// `name()` is `in-flight`.
    InFlight,
    /// Completed, with its recorded output.
    ///
    /// `name()` is `completed`.
    Completed(O),
}

impl<O> State<O> {
    /// The state's name, on its span.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::InFlight => "in-flight",
            Self::Completed(_) => "completed",
        }
    }
}

/// The decisions over an application's [`OperationStore`]; see the module docs. Hold one per store,
/// in the `Arc`'d application service every handler clones.
pub struct Operations<S: OperationStore> {
    store: S,
    /// One decision at a time: a lookup and the record it decides are never split by another's.
    decide: tokio::sync::Mutex<()>,
}

impl<S: OperationStore> Operations<S> {
    /// The decisions over `store`.
    pub fn new(store: S) -> Self {
        Self { store, decide: tokio::sync::Mutex::new(()) }
    }

    /// The application's store, for what it serves of its own (a read of its record).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Decide the request for `operation` on `input` (`node-rpc.md` §33, §43) and, unless it is a [`Decision::Conflict`],
    /// accept it once the store holds it (see the module docs). An `Err` is the store's: nothing was
    /// accepted, and the handler answers it as its fault.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rdm_node::operation::{Decision, OperationStore, Operations};
    /// use rdm_transport::Acceptance;
    ///
    /// async fn handle<S>(ops: &Operations<S>, acceptance: &Acceptance, id: &str, input: &S::Input) -> Result<Decision<S::Output>, S::Error>
    /// where
    ///     S: OperationStore,
    /// {
    ///     ops.decide(acceptance, id, input).await
    /// }
    /// ```
    pub async fn decide(&self, acceptance: &Acceptance, operation: &str, input: &S::Input) -> Result<Decision<S::Output>, S::Error> {
        let span = tracing::info_span!("rdm.node_rpc.operation.create.via-accept", operation, decision = tracing::field::Empty, error = tracing::field::Empty);
        async {
            let _one = self.decide.lock().await;
            let decision = match self.store.get(operation).await? {
                Some(held) if held.input != *input => Decision::Conflict,
                Some(Recorded { output: Some(output), .. }) => Decision::Completed(output),
                Some(_) => Decision::InFlight,
                None => {
                    self.store.put_accepted(operation, input).await?;
                    Decision::Recorded
                }
            };
            if !matches!(decision, Decision::Conflict) {
                acceptance.accepted();
            }
            Ok(decision)
        }
        .instrument(span.clone())
        .await
        .inspect(|d: &Decision<S::Output>| {
            span.record("decision", d.name());
        })
        .inspect_err(|e| {
            span.record("error", tracing::field::display(e));
        })
    }

    /// Record `operation` completed with `output`. `Ok(true)` when this recorded it; `Ok(false)`
    /// when the store held it completed already (its first output stands) or does not hold it.
    pub async fn complete(&self, operation: &str, output: &S::Output) -> Result<bool, S::Error> {
        let span = tracing::info_span!("rdm.node_rpc.operation.update.via-complete", operation, recorded = tracing::field::Empty, error = tracing::field::Empty);
        async {
            let _one = self.decide.lock().await;
            match self.store.get(operation).await? {
                Some(Recorded { output: None, .. }) => {
                    self.store.put_completed(operation, output).await?;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        .instrument(span.clone())
        .await
        .inspect(|recorded| {
            span.record("recorded", *recorded);
        })
        .inspect_err(|e| {
            span.record("error", tracing::field::display(e));
        })
    }

    /// Where `operation` stands: what a read-only request by its id answers, after a caller's
    /// `Indeterminate`.
    pub async fn read(&self, operation: &str) -> Result<State<S::Output>, S::Error> {
        let span = tracing::info_span!("rdm.node_rpc.operation.serve.via-read", operation, state = tracing::field::Empty, error = tracing::field::Empty);
        async {
            Ok(match self.store.get(operation).await? {
                None => State::Unknown,
                Some(Recorded { output: Some(output), .. }) => State::Completed(output),
                Some(_) => State::InFlight,
            })
        }
        .instrument(span.clone())
        .await
        .inspect(|s: &State<S::Output>| {
            span.record("state", s.name());
        })
        .inspect_err(|e| {
            span.record("error", tracing::field::display(e));
        })
    }

    /// Every operation the store holds accepted and not completed, with its input: what a start
    /// resumes before the node is Ready (the node's `pre_node_ready`), each finishing in
    /// [`Operations::complete`]. Each is named on the span's events.
    pub async fn pending(&self) -> Result<Vec<(String, S::Input)>, S::Error> {
        let span = tracing::info_span!("rdm.node_rpc.operation.update.via-resume", pending = tracing::field::Empty, error = tracing::field::Empty);
        async {
            let pending = self.store.pending().await?;
            for (operation, _) in &pending {
                tracing::info!(%operation, "an operation accepted and not completed is resumed");
            }
            Ok(pending)
        }
        .instrument(span.clone())
        .await
        .inspect(|p: &Vec<(String, S::Input)>| {
            span.record("pending", p.len());
        })
        .inspect_err(|e| {
            span.record("error", tracing::field::display(e));
        })
    }
}
