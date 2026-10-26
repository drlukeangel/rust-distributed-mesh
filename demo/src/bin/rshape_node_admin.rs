//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`rdm_admin::entry::run_on`). All configuration is the environment.
//!
//! It is the primer for the two seams a product's node-admin plugs in besides its hooks and channels:
//! - its own storage backend, handed over through `run_on`: [`ConsumerBackend`] is asked for every
//!   boundary the admin opens (its own row, then the six boundaries), here by delegating to RDM's file
//!   backend; a product's own backend (Rafka's topic-backed one) takes the file backend's place;
//! - a lifecycle hook, registered on `Wiring.hooks`: [`ReadyHook`] runs on every node's
//!   `Pending -> ReadyForTraffic` the admin brings into traffic, after eligibility and before the commit,
//!   and gates it (a product's entry step for a node, such as a returning broker's, lives here).
use async_trait::async_trait;
use rdm_admin::lifecycle::{HookContext, HookPhase, LifecycleHook, LifecycleHookSpec, LifecycleScope, LifecycleState, ShapePredicate, TransitionKey};
use rdm_admin::model::{IncarnationId, ProviderKind};
use rdm_admin::storage::NodeRecord;
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope};
use rdm_admin::storage_error::StorageError;
use rdm_admin::wiring::{FabricHooks, Wiring};
use std::path::Path;
use std::sync::Arc;

const SERVICE: &str = "rshape-node-admin";

/// The consumer's storage backend: every boundary RDM's node-admin opens is asked of it.
struct ConsumerBackend(Arc<dyn StorageBackend>);

#[async_trait]
impl StorageBackend for ConsumerBackend {
    fn qualify(&self, provider: ProviderKind) -> Result<(), String> {
        self.0.qualify(provider)
    }

    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        self.0.own_row(data_dir, writer).await
    }

    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
        self.0.put_own_row(data_dir, writer, record).await
    }

    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
        // Where the boundaries write, as the boot's storage span records it: this backend, over the file
        // backend's own answer.
        self.0.open(scope).await.map(|b| StorageBoundaries { writes_to: format!("rshape-backend:{}", b.writes_to), ..b })
    }
}

/// The consumer's lifecycle hook on a node's way into traffic.
struct ReadyHook;

#[async_trait]
impl LifecycleHook for ReadyHook {
    async fn run(&self, ctx: &HookContext) -> Result<(), String> {
        // RDM spans each hook run (`rdm.node_admin.lifecycle_hook.update.via-transition`, its `hook_id`).
        tracing::info!(target: "rshape", node = %ctx.target, "the consumer's hook ran before the node's ReadyForTraffic commit");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let ready = LifecycleHookSpec {
        hook_id: "rshape-ready".into(),
        transition: TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic },
        phase: HookPhase::AfterEligibilityBeforeCommit,
        applies_when: ShapePredicate::Always,
        blocking: true,
        max_attempts: 1,
    };
    rdm_admin::entry::run_on(
        SERVICE,
        |cfg| Arc::new(ConsumerBackend(rdm_admin::storage_backend::file_backend(cfg.store_dir.clone()))),
        |cfg| Wiring { fabric_hooks: FabricHooks::no_app_work(), certs: rdm_admin::entry::material_or_exit(SERVICE, cfg), hooks: vec![(ready, Arc::new(ReadyHook))], ..Wiring::default() },
    )
    .await
}
