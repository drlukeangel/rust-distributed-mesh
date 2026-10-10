//! The server half of born-full (`hydrate_before_ready`): the gate an app's own ops sit behind
//! until the app's hook on this node returned `Ok`, and the cancellation token a birth's retirement
//! ends its hook with.
//!
//! RDM dispatches app ops and owns neither their meaning nor their replies. What it owns is the
//! answer to a caller that reaches a node whose hook has not passed: the op's own protocol's typed
//! `NotReady`, never a handler run on a half-built node.

use crate::server::{HandlerFault, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Whether this node's `hydrate_before_ready` returned `Ok`. Closed at birth; RDM opens it when the
/// hook passes, and nothing closes it again. Cloned into every handler that must not answer before
/// then.
#[derive(Clone, Debug, Default)]
pub struct HydrationGate {
    passed: Arc<AtomicBool>,
}

impl HydrationGate {
    /// A closed gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// The hook returned `Ok`: app ops are served from here on.
    pub fn open(&self) {
        self.passed.store(true, Ordering::SeqCst);
    }

    /// Whether the hook has passed.
    pub fn is_open(&self) -> bool {
        self.passed.load(Ordering::SeqCst)
    }

    /// The reason a closed gate gives, `None` once it is open.
    pub fn refusal(&self) -> Option<String> {
        (!self.is_open()).then(|| "this node's hydrate_before_ready has not returned Ok: it holds no hydrated state to answer from".to_string())
    }
}

/// A birth's cancellation: cancelled once when the birth is retired (drain-node, stop-node, or its
/// process stopping), and never reset. Cloned into the hook's context and everything that ends with
/// the birth.
#[derive(Clone, Debug)]
pub struct CancelToken {
    tx: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Default for CancelToken {
    fn default() -> Self {
        Self { tx: Arc::new(tokio::sync::watch::Sender::new(false)) }
    }
}

impl CancelToken {
    /// A token not yet cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel: every waiter ends.
    pub fn cancel(&self) {
        self.tx.send_replace(true);
    }

    /// Whether the birth was retired.
    pub fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }

    /// Resolves once the birth is retired (at once when it already was).
    pub async fn cancelled(&self) {
        let _ = self.tx.subscribe().wait_for(|v| *v).await;
    }
}

impl ServerBuilder {
    /// [`ServerBuilder::serve`] for an app op that answers from state its hook hydrates: until
    /// `gate` is open the handler does not run and the caller gets the protocol's typed `NotReady`
    /// (`rdm.node_rpc.request.reject.via-hydration-not-passed`). A lifecycle op is RDM's, not the
    /// app's, and is never served through this door.
    pub fn serve_gated<P, F, Fut>(self, owner: OpOwner, gate: HydrationGate, f: F) -> Self
    where
        P: NodeProtocol,
        F: Fn(PeerContext, P::Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<P::Reply, HandlerFault>> + Send + 'static,
    {
        self.serve::<P, _, _>(owner, move |peer: PeerContext, req: P::Request| {
            let answered = gate.is_open().then(|| f(peer, req));
            let gate = gate.clone();
            async move {
                match answered {
                    Some(fut) => fut.await,
                    None => {
                        let reason = gate.refusal().unwrap_or_default();
                        tracing::info_span!("rdm.node_rpc.request.reject.via-hydration-not-passed", protocol = P::NAME, op = P::OP, reason = %reason)
                            .in_scope(|| tracing::info!("an app op reached a node whose hydrate_before_ready has not passed: typed NotReady"));
                        Ok(P::not_ready(reason))
                    }
                }
            }
        })
    }
}
