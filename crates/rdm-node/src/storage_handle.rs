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

/// The provider a node writes now, replaceable while every caller keeps what it took from the handle,
/// and the block of a storage change (storage.md §2 step 1): while it stands no write is admitted, and
/// every write the node would make waits as owed, never refused, until the unblock admits it to the
/// provider the node writes then.
pub struct StorageHandle<S> {
    current: RwLock<Arc<Opened<S>>>,
    /// Whether the node is blocked.
    blocked: tokio::sync::watch::Sender<bool>,
    /// The writes waiting on the block: owed.
    owed: std::sync::atomic::AtomicUsize,
}

impl<S> StorageHandle<S> {
    /// The handle over `stores`, opened from the provider registered as `provider`.
    pub fn new(provider: &str, stores: S) -> Self {
        Self { current: RwLock::new(Arc::new(Opened::new(provider, stores))), blocked: tokio::sync::watch::channel(false).0, owed: Default::default() }
    }

    /// The provider the node reads and writes now.
    pub fn read(&self) -> Arc<Opened<S>> {
        self.current.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The provider the node writes now, this write counted on it until the returned admission drops.
    /// While the node is blocked the write waits, owed, and is admitted at the unblock to the provider the
    /// node writes then.
    pub async fn write(&self) -> Admitted<S> {
        let mut blocked = self.blocked.subscribe();
        let mut owed = false;
        loop {
            if *blocked.borrow_and_update() {
                if !owed {
                    owed = true;
                    self.owed.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                }
                let _ = blocked.wait_for(|b| !*b).await;
                continue;
            }
            // The admission and the block take the same lock: a write is admitted before the block or
            // waits for its unblock, never both.
            let current = self.current.read().unwrap_or_else(|e| e.into_inner());
            if *self.blocked.borrow() {
                continue;
            }
            current.admitted.send_modify(|n| *n += 1);
            if owed {
                self.owed.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
            return Admitted(current.clone());
        }
    }

    /// Block: from here no write is admitted, each waiting as owed; answers how many writes admitted
    /// before the block were not answered yet (issued). [`Self::finish_issued`] waits for them.
    pub fn block(&self) -> usize {
        let current = self.current.write().unwrap_or_else(|e| e.into_inner());
        self.blocked.send_replace(true);
        let issued = *current.admitted.borrow();
        issued
    }

    /// Resolves once every write admitted to the provider the node writes now has been answered there.
    pub async fn finish_issued(&self) {
        let current = self.read();
        let _ = current.admitted.subscribe().wait_for(|n| *n == 0).await;
    }

    /// Lift the block: every owed write is admitted, in the order each waits, to the provider the node
    /// writes now.
    pub fn unblock(&self) {
        let _current = self.current.write().unwrap_or_else(|e| e.into_inner());
        self.blocked.send_replace(false);
    }

    /// Whether the node is blocked.
    pub fn is_blocked(&self) -> bool {
        *self.blocked.borrow()
    }

    /// How many writes wait on the block now.
    pub fn owed(&self) -> usize {
        self.owed.load(std::sync::atomic::Ordering::Acquire)
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
