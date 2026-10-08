//! Running a future whose own code holds a span guard across an `.await`.
//!
//! `tracing-subscriber` keeps each thread's entered spans on that thread's stack, with no
//! reference of its own behind an entry. A future that holds `span.enter()` across an `.await`
//! enters on the worker that polled it first and exits on whichever worker resumes it: the exit
//! finds nothing on the second worker's stack, and the span stays entered on the first with its
//! last reference gone. The next span that worker opens takes it as its contextual parent, and
//! the registry panics "tried to clone a span ... that already closed" when that span is closing.
//!
//! iroh's `Endpoint::builder().bind()` is such a future. [`on_one_thread`] polls it on one blocking
//! thread, so the enter and the exit are the same thread's and the stack is left clean. The
//! caller's current span is kept as the future's parent.

use std::future::Future;
use tracing::Instrument;

/// Drive `fut` to completion on a single blocking thread of the current runtime. Tasks `fut`
/// spawns run on the runtime's workers as usual.
pub async fn on_one_thread<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    let fut = fut.in_current_span();
    match handle.clone().spawn_blocking(move || handle.block_on(fut)).await {
        Ok(out) => out,
        Err(e) => match e.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(e) => panic!("the thread driving a future was cancelled: {e}"),
        },
    }
}
