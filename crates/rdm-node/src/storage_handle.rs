//! A node's one replaceable access point to its topology storage (storage.md §2): every reader and
//! writer on the node reaches the provider it writes through one [`StorageHandle`], never through a
//! store opened at its boot, so a provider change moves all of them at once ("Each node moves to the
//! new provider").
//!
//! A read takes the provider the handle holds now. A write is admitted to that provider and counted
//! there until it is answered; the admission and a replacement take the same lock, so every write is
//! counted on exactly one provider. [`StorageHandle::replace`] puts the new provider in place and then
//! waits until every write admitted to the old one has been answered ("Finish outstanding writes":
//! "Each node brings every write and claim operation it issued before the block to a durable outcome
//! on the old provider"). Holding new writes while it waits is the block's, not this handle's.

use std::sync::{Arc, RwLock};

/// One provider opened on the node: its name, its stores and the writes admitted to it that have not
/// been answered yet.
pub struct Opened<S> {
    provider: String,
    stores: S,
    admitted: tokio::sync::watch::Sender<usize>,
}

impl<S> Opened<S> {
    fn new(provider: &str, stores: S) -> Self {
        Self { provider: provider.to_string(), stores, admitted: tokio::sync::watch::channel(0).0 }
    }

    /// The provider's registered name.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// The provider's opened stores.
    pub fn stores(&self) -> &S {
        &self.stores
    }
}

/// A write admitted to an opened provider: its stores, counted on that provider until this is dropped,
/// once the write is answered.
pub struct Admitted<S>(Arc<Opened<S>>);

impl<S> std::ops::Deref for Admitted<S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.0.stores
    }
}

impl<S> Drop for Admitted<S> {
    fn drop(&mut self) {
        self.0.admitted.send_modify(|n| *n -= 1);
    }
}

/// The provider a node writes now, replaceable while every caller keeps what it took from the handle.
pub struct StorageHandle<S> {
    current: RwLock<Arc<Opened<S>>>,
}

impl<S> StorageHandle<S> {
    /// The handle over `stores`, opened from the provider registered as `provider`.
    pub fn new(provider: &str, stores: S) -> Self {
        Self { current: RwLock::new(Arc::new(Opened::new(provider, stores))) }
    }

    /// The provider the node reads and writes now.
    pub fn read(&self) -> Arc<Opened<S>> {
        self.current.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The provider the node writes now, this write counted on it until the returned admission drops.
    pub fn write(&self) -> Admitted<S> {
        let current = self.current.read().unwrap_or_else(|e| e.into_inner());
        current.admitted.send_modify(|n| *n += 1);
        Admitted(current.clone())
    }

    /// Put `stores`, opened from the provider registered as `provider`, in place of the provider the
    /// node writes now, and return the old one once every write admitted to it has been answered.
    pub async fn replace(&self, provider: &str, stores: S) -> Arc<Opened<S>> {
        let old = std::mem::replace(&mut *self.current.write().unwrap_or_else(|e| e.into_inner()), Arc::new(Opened::new(provider, stores)));
        // No write is admitted to `old` after the swap: its count only falls from here.
        let _ = old.admitted.subscribe().wait_for(|n| *n == 0).await;
        old
    }
}
