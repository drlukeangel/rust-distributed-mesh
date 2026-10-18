//! Unit test helpers of admin-core: a temp dir removed when the test binary exits, and a FIFO a
//! read of which waits for its writer.

use std::path::PathBuf;

pub(crate) fn tempdir(tag: &str) -> PathBuf {
    static MADE: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());
    extern "C" fn remove_made() {
        for d in MADE.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            let _ = std::fs::remove_dir_all(d);
        }
    }
    let d = std::env::temp_dir().join(format!("{tag}-{}-{}", std::process::id(), rand::random::<u64>()));
    std::fs::create_dir_all(&d).unwrap();
    let mut made = MADE.lock().unwrap();
    if made.is_empty() {
        // Every directory a test of this binary made is removed when the binary exits.
        unsafe { libc::atexit(remove_made) };
    }
    made.push(d.clone());
    d
}

/// A future a test cell runs, boxed so a cell can hand one over.
pub(crate) type CellFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// Make `fifo` (its directory too) a FIFO a read of which waits until a writer opens it, and run
/// `op` on a single-thread runtime. The writer is a thread that, once a reader has the FIFO open,
/// waits for word from the runtime that it still runs (up to 5 s), then writes `bytes`. Answers
/// whether that word came while the read waited, and what `op` answered.
pub(crate) fn runtime_runs_while_a_fifo_is_read<T: Send + 'static>(fifo: PathBuf, bytes: Vec<u8>, op: CellFuture<T>) -> (bool, T) {
    std::fs::create_dir_all(fifo.parent().unwrap()).unwrap();
    let c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0, "mkfifo {}", fifo.display());
    let (word_tx, word_rx) = std::sync::mpsc::channel::<()>();
    let writer = std::thread::spawn(move || {
        use std::io::Write as _;
        // Blocks until the read has the FIFO open: the read is waiting from here on.
        let mut f = std::fs::OpenOptions::new().write(true).open(&fifo).unwrap();
        let heard = word_rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok();
        f.write_all(&bytes).unwrap();
        heard
    });
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let out = rt.block_on(async move {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        // The read and its start signal are one poll: this task resumes only once that poll returns.
        let read = tokio::spawn(async move {
            let _ = started_tx.send(());
            op.await
        });
        started_rx.await.unwrap();
        let _ = word_tx.send(());
        read.await.unwrap()
    });
    (writer.join().unwrap(), out)
}

/// Run `setup`, then `op` on its answer, on a single-thread runtime whose blocking pool has one
/// thread, held busy while `op` first runs. A disk call `op` makes on the blocking pool queues
/// behind the held thread, so `op` is still waiting after the runtime has turned over; one it makes
/// on the runtime's thread has finished by then. Answers whether `op` waited, and what it answered
/// once the pool's thread was let go. (A FIFO cannot hold a `stat` or a `mkdir`: this is the cell
/// for a disk call that never opens a file.)
pub(crate) fn op_waits_on_the_blocking_pool<S: Send + 'static, T: Send + 'static>(setup: CellFuture<S>, op: impl FnOnce(S) -> CellFuture<T> + Send + 'static) -> (bool, T) {
    let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
    rt.block_on(async move {
        let state = setup.await;
        let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        });
        held_rx.await.unwrap();
        let task = tokio::spawn(op(state));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let waited = !task.is_finished();
        release_tx.send(()).unwrap();
        holder.await.unwrap();
        (waited, task.await.unwrap())
    })
}

