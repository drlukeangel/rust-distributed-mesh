//! The admitted attempt (Q168; fabric-node-hooks.md, `sync_state`): the newest state-sync attempt
//! this node admitted, so the work an older attempt's hook started on it is refused before it
//! changes the state the current attempt runs on.
//!
//! An attempt is named by the triple the state-sync handoff carries: the accepted Build's id, its
//! acceptance stamp `submitted_at_ms` and the Build attempt. The order is `(submitted_at_ms,
//! attempt)`; a Build id is never ordered. The acceptance stamp is strictly increasing across
//! accepted Builds by construction (`max(now, newest accepted + 1)`, the newest read from the builds
//! store, R-BI3), so a newer Build's attempt 1 is newer than an older Build's attempt 8, and a clock
//! rollback never lowers the order: nothing here reads a clock.
//!
//! RDM owns the comparison and the durable value; the application owns calling
//! [`AdmittedAttempt::admit_attempt`] under the same per-resource serialization every competing
//! attempt takes, immediately before each mutation and each publication.

use crate::storage_error::StorageError;
use std::path::{Path, PathBuf};

/// The file beside the node's data dir that holds the admitted triple, one whole record.
pub const ADMITTED_ATTEMPT_FILE: &str = "admitted-attempt";

/// A state-sync attempt: the accepted Build, its acceptance stamp and the Build attempt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttemptTriple {
    /// The accepted Build.
    pub build_id: String,
    /// The Build's acceptance stamp (R-BI3).
    pub submitted_at_ms: u64,
    /// The Build attempt.
    pub attempt: u32,
}

/// Why [`AdmittedAttempt::admit_attempt`] did not admit an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptRefused {
    /// `superseded`: the node admitted a newer attempt, named.
    Superseded {
        /// The attempt the node admitted.
        admitted: AttemptTriple,
    },
    /// `ambiguous-order`: the node admitted another Build carrying the same acceptance stamp, named.
    AmbiguousOrder {
        /// The attempt the node admitted.
        admitted: AttemptTriple,
    },
    /// The newer attempt's record could not be written: it is not admitted, because its effect is
    /// never allowed ahead of its record.
    Storage(StorageError),
}

impl AttemptRefused {
    /// The refusal's name: `superseded`, `ambiguous-order`, or `storage` for a record not written.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Superseded { .. } => "superseded",
            Self::AmbiguousOrder { .. } => "ambiguous-order",
            Self::Storage(_) => "storage",
        }
    }
}

impl std::fmt::Display for AttemptRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Superseded { admitted } => write!(f, "superseded: this node admitted build {} (accepted at {} ms) attempt {}", admitted.build_id, admitted.submitted_at_ms, admitted.attempt),
            Self::AmbiguousOrder { admitted } => write!(f, "ambiguous-order: this node admitted build {} accepted at the same {} ms", admitted.build_id, admitted.submitted_at_ms),
            Self::Storage(e) => write!(f, "storage: the newer attempt could not be recorded, so it is not admitted: {e}"),
        }
    }
}

impl std::error::Error for AttemptRefused {}

/// What the order decides for `asked` against the `held` admitted triple.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    /// Newer: written, then admitted.
    Newer,
    /// The admitted triple again: admitted, nothing written.
    Repeat,
    /// Refused.
    Refused(AttemptRefused),
}

/// The order: `(submitted_at_ms, attempt)`, the Build id only compared for equality at an equal
/// stamp, never ordered.
fn order(held: Option<&AttemptTriple>, asked: &AttemptTriple) -> Verdict {
    use std::cmp::Ordering::*;
    let Some(held) = held else { return Verdict::Newer };
    let refused = |r: fn(AttemptTriple) -> AttemptRefused| Verdict::Refused(r(held.clone()));
    match asked.submitted_at_ms.cmp(&held.submitted_at_ms) {
        Less => refused(|admitted| AttemptRefused::Superseded { admitted }),
        Greater => Verdict::Newer,
        Equal if asked.build_id != held.build_id => refused(|admitted| AttemptRefused::AmbiguousOrder { admitted }),
        Equal => match asked.attempt.cmp(&held.attempt) {
            Less => refused(|admitted| AttemptRefused::Superseded { admitted }),
            Equal => Verdict::Repeat,
            Greater => Verdict::Newer,
        },
    }
}

/// The on-disk record: one whole JSON object, versioned.
#[derive(serde::Serialize, serde::Deserialize)]
struct Record {
    v: u32,
    #[serde(flatten)]
    admitted: AttemptTriple,
}

/// This node's admitted attempt: the comparison and the durable value (Q168). One per node, held
/// by every door that admits work for an attempt; the node's served ops reach it on
/// [`crate::NodeSeams`].
#[derive(Debug)]
pub struct AdmittedAttempt {
    dir: PathBuf,
    held: tokio::sync::Mutex<Option<AttemptTriple>>,
}

impl AdmittedAttempt {
    /// The admitted attempt kept beside `data_dir`, read back as the node starts: none when the
    /// node never admitted one. A file present that is not a whole record of this version is
    /// refused by name, never read as no attempt.
    pub fn open(data_dir: &Path) -> Result<Self, StorageError> {
        let file = data_dir.join(ADMITTED_ATTEMPT_FILE);
        let held = match std::fs::read(&file) {
            Ok(bytes) => {
                let unrecognised = |reason: String| StorageError::Unrecognised { file: file.display().to_string(), reason };
                let r: Record = serde_json::from_slice(&bytes).map_err(|e| unrecognised(e.to_string()))?;
                if r.v != 1 {
                    return Err(unrecognised(format!("version {} (this build reads version 1)", r.v)));
                }
                Some(r.admitted)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(StorageError::Io { file: file.display().to_string(), reason: e.to_string() }),
        };
        Ok(Self { dir: data_dir.to_path_buf(), held: tokio::sync::Mutex::new(held) })
    }

    /// The attempt this node admitted, if any.
    pub async fn admitted(&self) -> Option<AttemptTriple> {
        self.held.lock().await.clone()
    }

    /// Admit the state-sync attempt `(build_id, submitted_at_ms, attempt)`, or refuse it by name.
    /// Ordered by `(submitted_at_ms, attempt)`; a Build id is never ordered. An older attempt is
    /// refused `superseded`; another Build carrying the admitted acceptance stamp is refused
    /// `ambiguous-order`; the admitted triple again is admitted (a repeat, subject to the
    /// operation's own idempotency); a newer one is written durably beside the node's data dir
    /// before this returns, then admitted. Calls are serialized here, so two newer attempts never
    /// both write; the application still holds its own per-resource serialization across this call
    /// and the mutation it guards.
    pub async fn admit_attempt(&self, build_id: &str, submitted_at_ms: u64, attempt: u32) -> Result<(), AttemptRefused> {
        let asked = AttemptTriple { build_id: build_id.to_string(), submitted_at_ms, attempt };
        let mut held = self.held.lock().await;
        let span = tracing::info_span!("rdm.node.attempt.update.via-admit", build_id = %asked.build_id, submitted_at_ms, attempt, admitted_build_id = tracing::field::Empty, admitted_submitted_at_ms = tracing::field::Empty, admitted_attempt = tracing::field::Empty, outcome = tracing::field::Empty, "otel.kind" = "internal");
        if let Some(h) = held.as_ref() {
            span.record("admitted_build_id", h.build_id.as_str());
            span.record("admitted_submitted_at_ms", h.submitted_at_ms);
            span.record("admitted_attempt", h.attempt);
        }
        let verdict = order(held.as_ref(), &asked);
        match verdict {
            Verdict::Repeat => {
                span.record("outcome", "repeat");
                span.in_scope(|| tracing::info!("the admitted attempt again: admitted"));
                Ok(())
            }
            Verdict::Refused(refused) => {
                span.record("outcome", refused.name());
                span.in_scope(|| tracing::info!(reason = %refused, "the attempt is refused"));
                Err(refused)
            }
            Verdict::Newer => {
                let bytes = serde_json::to_vec(&Record { v: 1, admitted: asked.clone() }).expect("an attempt triple serializes");
                let dir = self.dir.clone();
                let written = crate::blocking::blocking_io(move || {
                    let path = dir.join(ADMITTED_ATTEMPT_FILE);
                    let tmp = dir.join(format!("{ADMITTED_ATTEMPT_FILE}.tmp"));
                    crate::durable_file::write_file(&dir, &tmp, &path, &bytes)
                })
                .await;
                if let Err(e) = written {
                    span.record("outcome", "storage");
                    span.in_scope(|| tracing::info!(error = %e, "the newer attempt could not be recorded: not admitted"));
                    return Err(AttemptRefused::Storage(e));
                }
                *held = Some(asked);
                span.record("outcome", "admitted");
                span.in_scope(|| tracing::info!("the newer attempt is recorded beside the data dir, then admitted"));
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("admitted-attempt-{tag}-{}-{}", std::process::id(), rand::random::<u64>()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn triple(build_id: &str, submitted_at_ms: u64, attempt: u32) -> AttemptTriple {
        AttemptTriple { build_id: build_id.into(), submitted_at_ms, attempt }
    }

    async fn admit(a: &AdmittedAttempt, t: &AttemptTriple) -> Result<(), AttemptRefused> {
        a.admit_attempt(&t.build_id, t.submitted_at_ms, t.attempt).await
    }

    /// The application's resource as an embedding serves it (the packet's arm A, "What the
    /// application does"): every op that mutates it takes the resource's one serialization, calls
    /// `admit_attempt` under it and mutates under the same hold. `events` records the order of each
    /// admission and mutation.
    struct Resource {
        rows: tokio::sync::Mutex<Vec<String>>,
        attempts: Arc<AdmittedAttempt>,
        events: std::sync::Mutex<Vec<String>>,
    }

    impl Resource {
        fn new(attempts: Arc<AdmittedAttempt>) -> Self {
            Self { rows: tokio::sync::Mutex::new(Vec::new()), attempts, events: std::sync::Mutex::new(Vec::new()) }
        }

        fn event(&self, e: String) {
            self.events.lock().unwrap().push(e);
        }

        /// The seed: check and write under the one serialization.
        async fn seed(&self, t: &AttemptTriple, row: &str) -> Result<(), AttemptRefused> {
            let mut rows = self.rows.lock().await;
            admit(&self.attempts, t).await?;
            self.event(format!("seed@{} admitted", t.attempt));
            rows.push(row.to_string());
            self.event(format!("seed@{} written", t.attempt));
            Ok(())
        }

        /// The restore: checked as it is taken, its image prepared outside the serialization, then
        /// the serialization taken again and `admit_attempt` called again immediately before the
        /// image is published. `between` runs after the final check and before the publish, still
        /// under the hold.
        async fn restore_from<P, B>(&self, t: &AttemptTriple, image: Vec<String>, prepared: P, between: B) -> Result<(), AttemptRefused>
        where
            P: std::future::Future<Output = ()>,
            B: std::future::Future<Output = ()>,
        {
            {
                let _held = self.rows.lock().await;
                admit(&self.attempts, t).await?;
                self.event(format!("restore@{} taken", t.attempt));
            }
            prepared.await;
            let mut rows = self.rows.lock().await;
            admit(&self.attempts, t).await?;
            self.event(format!("restore@{} admitted", t.attempt));
            between.await;
            *rows = image;
            self.event(format!("restore@{} written", t.attempt));
            Ok(())
        }
    }

    /// CONTRACT (Q168 arm A, cell 1): attempt 1's restore, delayed until after attempt 2's seed wrote,
    /// is refused `superseded` and the seed's rows stand.
    #[tokio::test]
    async fn a_delayed_old_restore_after_a_newer_seed_is_refused_and_the_seed_stands() {
        let attempts = Arc::new(AdmittedAttempt::open(&dir("delayed-restore")).unwrap());
        let app = Resource::new(attempts.clone());
        let (one, two) = (triple("bld-a", 1_000, 1), triple("bld-a", 1_000, 2));
        app.seed(&two, "seed").await.expect("attempt 2's seed is admitted");
        match app.restore_from(&one, vec!["restored".into()], async {}, async {}).await {
            Err(AttemptRefused::Superseded { admitted }) => assert_eq!(admitted, two, "the refusal names the admitted attempt"),
            other => panic!("attempt 1's restore after attempt 2's seed is refused superseded, not {other:?}"),
        }
        assert_eq!(*app.rows.lock().await, vec!["seed".to_string()], "the seed's rows stand");
        assert_eq!(AttemptRefused::Superseded { admitted: two.clone() }.name(), "superseded");
        assert_eq!(attempts.admitted().await, Some(two));
    }

    /// CONTRACT (Q168 arm A, cell 2): no write interleaves between the final check and the mutation.
    /// A restore prepared outside the serialization, overtaken by a newer seed while it prepared, is
    /// refused at its final check and publishes nothing; a seed that arrives while a restore holds the
    /// serialization between its final check and its publish is admitted only after the publish; and
    /// concurrent admissions leave the newest recorded, never an older one written over it.
    #[tokio::test]
    async fn no_write_interleaves_between_the_final_check_and_the_mutation() {
        // A restore overtaken while it prepared.
        let attempts = Arc::new(AdmittedAttempt::open(&dir("final-check")).unwrap());
        let app = Arc::new(Resource::new(attempts.clone()));
        let (one, two) = (triple("bld-a", 1_000, 1), triple("bld-a", 1_000, 2));
        let (prepared_tx, prepared_rx) = tokio::sync::oneshot::channel::<()>();
        let restore = {
            let (app, one) = (app.clone(), one.clone());
            tokio::spawn(async move { app.restore_from(&one, vec!["restored".into()], async { prepared_rx.await.unwrap() }, async {}).await })
        };
        while !app.events.lock().unwrap().iter().any(|e| e == "restore@1 taken") {
            tokio::task::yield_now().await;
        }
        app.seed(&two, "seed").await.expect("attempt 2's seed lands while attempt 1's restore prepares");
        prepared_tx.send(()).unwrap();
        assert!(matches!(restore.await.unwrap(), Err(AttemptRefused::Superseded { .. })), "the restore's final check refuses it");
        assert_eq!(*app.rows.lock().await, vec!["seed".to_string()], "the overtaken restore published nothing");

        // A seed arriving while a restore holds the serialization past its final check.
        let attempts = Arc::new(AdmittedAttempt::open(&dir("no-interleave")).unwrap());
        let app = Arc::new(Resource::new(attempts.clone()));
        let (checked_tx, checked_rx) = tokio::sync::oneshot::channel::<()>();
        let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
        let restore = {
            let (app, one) = (app.clone(), one.clone());
            tokio::spawn(async move {
                app.restore_from(&one, vec!["restored".into()], async {}, async move {
                    checked_tx.send(()).unwrap();
                    go_rx.await.unwrap();
                })
                .await
            })
        };
        checked_rx.await.unwrap();
        let seed = {
            let (app, two) = (app.clone(), two.clone());
            tokio::spawn(async move { app.seed(&two, "seed").await })
        };
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(attempts.admitted().await, Some(one.clone()), "no newer attempt is admitted between the restore's final check and its publish");
        go_tx.send(()).unwrap();
        restore.await.unwrap().expect("attempt 1's restore, checked before any newer attempt, publishes");
        seed.await.unwrap().expect("attempt 2's seed is admitted after it");
        assert_eq!(
            *app.events.lock().unwrap(),
            ["restore@1 taken", "restore@1 admitted", "restore@1 written", "seed@2 admitted", "seed@2 written"].map(String::from).to_vec(),
            "the seed's admission and write follow the restore's publish, never between its check and its write"
        );
        assert_eq!(*app.rows.lock().await, vec!["restored".to_string(), "seed".to_string()]);

        // Concurrent admissions: the newest is recorded, an older never written over it.
        let d = dir("concurrent");
        let attempts = Arc::new(AdmittedAttempt::open(&d).unwrap());
        let calls: Vec<_> = (1..=16u32)
            .rev()
            .map(|n| {
                let a = attempts.clone();
                tokio::spawn(async move { a.admit_attempt("bld-a", 1_000, n).await })
            })
            .collect();
        for c in calls {
            let _ = c.await.unwrap();
        }
        assert_eq!(attempts.admitted().await, Some(triple("bld-a", 1_000, 16)));
        assert_eq!(AdmittedAttempt::open(&d).unwrap().admitted().await, Some(triple("bld-a", 1_000, 16)), "the record is the newest admitted");
    }

    /// CONTRACT (Q168 arm A, cell 3): a restart keeps the admitted triple: a node started again over
    /// its data dir refuses the older attempt it refused before, and admits its admitted one again.
    #[tokio::test]
    async fn a_restart_keeps_the_admitted_triple() {
        let d = dir("restart");
        let two = triple("bld-a", 1_000, 2);
        {
            let attempts = AdmittedAttempt::open(&d).unwrap();
            admit(&attempts, &two).await.unwrap();
        }
        let again = AdmittedAttempt::open(&d).unwrap();
        assert_eq!(again.admitted().await, Some(two.clone()), "the admitted triple is read back at start");
        assert!(matches!(admit(&again, &triple("bld-a", 1_000, 1)).await, Err(AttemptRefused::Superseded { .. })), "the older attempt is refused after the restart");
        admit(&again, &two).await.expect("the admitted triple again is a repeat, admitted");
        std::fs::write(d.join(ADMITTED_ATTEMPT_FILE), b"not a record").unwrap();
        assert!(matches!(AdmittedAttempt::open(&d), Err(StorageError::Unrecognised { .. })), "a record that does not parse is refused by name, never read as none");
    }

    /// CONTRACT (Q168 arm A, cell 4): ordered by `(submitted_at_ms, attempt)`, never by Build id: a
    /// newer Build's attempt 1 supersedes an older Build's attempt 8, whatever their ids' order.
    #[tokio::test]
    async fn a_newer_builds_attempt_1_supersedes_an_older_builds_attempt_8() {
        let attempts = AdmittedAttempt::open(&dir("newer-build")).unwrap();
        let (older, newer) = (triple("zzz-older", 1_000, 8), triple("aaa-newer", 2_000, 1));
        admit(&attempts, &older).await.unwrap();
        admit(&attempts, &newer).await.expect("the newer Build's attempt 1 is admitted over the older Build's attempt 8");
        match admit(&attempts, &older).await {
            Err(AttemptRefused::Superseded { admitted }) => assert_eq!(admitted, newer),
            other => panic!("the older Build's attempt 8 is refused superseded, not {other:?}"),
        }
        assert!(matches!(admit(&attempts, &triple("aaa-newer", 2_000, 0)).await, Err(AttemptRefused::Superseded { .. })), "a lower attempt of the admitted Build is superseded");
    }

    /// CONTRACT (Q168 arm A, cell 5): another Build carrying the admitted acceptance stamp is refused
    /// `ambiguous-order`, whatever its attempt, and the admitted triple stands.
    #[tokio::test]
    async fn equal_stamps_with_different_build_ids_are_refused_ambiguous_order() {
        let attempts = AdmittedAttempt::open(&dir("ambiguous")).unwrap();
        let held = triple("bld-a", 1_000, 2);
        admit(&attempts, &held).await.unwrap();
        for attempt in [1, 2, 3] {
            match admit(&attempts, &triple("bld-b", 1_000, attempt)).await {
                Err(r @ AttemptRefused::AmbiguousOrder { .. }) => assert_eq!(r.name(), "ambiguous-order"),
                other => panic!("bld-b attempt {attempt} at the same stamp is refused ambiguous-order, not {other:?}"),
            }
        }
        assert_eq!(attempts.admitted().await, Some(held));
    }
}
