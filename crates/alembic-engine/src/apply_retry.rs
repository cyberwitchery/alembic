use crate::journal::Journal;
use crate::{AdapterApplyError, AppliedOp, Op};
use alembic_core::Uid;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub struct RetryApplyResult {
    pub applied: Vec<AppliedOp>,
    pub pending: Vec<Op>,
    /// ops earlier runs of this plan applied, recovered from the journal in plan order.
    pub resumed: Vec<AppliedOp>,
}

#[async_trait]
pub trait RetryApplyDriver {
    async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp>;
    fn is_retryable(&self, err: &anyhow::Error) -> bool;
    /// handed the ops an earlier run applied, before this run's first op, so the
    /// driver can resolve references into objects it is not going to create again.
    fn resume(&mut self, _resumed: &[AppliedOp]) {}
}

pub async fn apply_non_delete_with_retries<'a>(
    ops: &[Op],
    mut journal: Option<&'a mut Journal>,
    driver: &mut impl RetryApplyDriver,
) -> Result<(RetryApplyResult, JournalGuard<'a>)> {
    let mut applied = Vec::new();
    let mut resumed = Vec::new();
    let mut pending: Vec<Op> = ops
        .iter()
        .filter(|op| !matches!(op, Op::Delete { .. }))
        .cloned()
        .collect();

    if let Some(journal) = journal.as_mut() {
        let done_ops = journal.done_ops();
        let done_ops_len = done_ops.len();

        let mut done = done_ops
            .into_iter()
            .collect::<std::collections::HashSet<_>>();

        if done.len() != done_ops_len {
            // the use of a hash set here is an optimization, but it rules out ops with
            // exactly the same uid, typename and hash.
            return Err(anyhow!("journal contained duplicated ops (same uid, typename and hash) which is not supported"));
        }

        pending.retain(|op| !done.remove(&(op.uid(), op.type_name().clone(), op.hashed())));

        if !done.is_empty() {
            return Err(anyhow!(
                "journal contains done ops that are not present in the provided ops"
            ));
        }

        resumed = journal.done_applied_ops();
        driver.resume(&resumed);
    }

    while !pending.is_empty() {
        let current = std::mem::take(&mut pending);
        let applied_before = applied.len();

        for op in current {
            match driver.apply_non_delete(&op).await {
                Ok(applied_op) => {
                    // the journal is append-only, so marking is the persist: the record
                    // is on disk before the next op is applied against it
                    if let Some(journal) = journal.as_mut() {
                        journal.mark_op_as_done(&op, applied_op.backend_id.as_ref())?;
                    }
                    applied.push(applied_op);
                }
                Err(err) if driver.is_retryable(&err) => pending.push(op),
                Err(err) => {
                    if let Some(journal) = journal.as_mut() {
                        report_resumable(journal);
                    }
                    return Err(err);
                }
            }
        }

        if applied.len() == applied_before {
            break;
        }
    }

    // the backing file outlives this loop either way: the deletes still have to run,
    // and they are what a re-run must not lose the creates and updates to
    if let Some(journal) = journal.as_deref() {
        if !journal.is_completed() {
            // ops remain pending (stuck with no progress): a re-run resumes from what
            // is already on disk
            report_resumable(journal);
        }
    }

    Ok((
        RetryApplyResult {
            applied,
            pending,
            resumed,
        },
        JournalGuard::borrowed(journal),
    ))
}

/// tell the user what the interrupted apply left behind. resuming is automatic and
/// silent, so this is the only place the journal is ever named; warn-level so the
/// cli's default filter shows it.
///
/// the count is cumulative across runs, and nothing applied means nothing to resume
/// from: a backend unreachable on the first op leaves the error as the whole story.
fn report_resumable(journal: &Journal) {
    let done = journal.done_ops_count();
    let Some(path) = journal.backing_file_path().filter(|_| done > 0) else {
        return;
    };
    tracing::warn!(
        "apply stopped after {} of {} create/update operations; the journal at {} records what was applied, and re-running the same plan resumes from there",
        done,
        journal.op_count(),
        path.display()
    );
}

/// the journal, handed back to the caller so it outlives the whole apply. deletes are
/// not journaled but still have to run, and until they do the file is what a re-run
/// recovers the creates and updates from; `finish` drops it once the apply is through.
///
/// `must_use` only catches discarding the whole returned tuple; a caller that binds the
/// guard and never `finish`es it compiles, and the `Drop` notice is what reports that.
#[derive(Debug)]
#[must_use = "the deletes still have to run: `finish` the journal once they are through, or the file stays behind"]
pub struct JournalGuard<'a>(Option<JournalRef<'a>>);

/// `apply_non_delete_journaled` builds the journal itself and hands it back owned; a
/// caller driving the retry loop with its own journal gets a guard over the borrow, so
/// there is one rule for both.
#[derive(Debug)]
enum JournalRef<'a> {
    Owned(Journal),
    Borrowed(&'a mut Journal),
}

impl JournalRef<'_> {
    fn get(&self) -> &Journal {
        match self {
            Self::Owned(journal) => journal,
            Self::Borrowed(journal) => journal,
        }
    }

    fn get_mut(&mut self) -> &mut Journal {
        match self {
            Self::Owned(journal) => journal,
            Self::Borrowed(journal) => journal,
        }
    }
}

impl<'a> JournalGuard<'a> {
    fn borrowed(journal: Option<&'a mut Journal>) -> Self {
        Self(journal.map(JournalRef::Borrowed))
    }

    fn owned(journal: Option<Journal>) -> Self {
        Self(journal.map(JournalRef::Owned))
    }

    /// the apply is through, deletes included: there is nothing left to resume.
    pub fn finish(mut self) -> Result<()> {
        match self.0.take() {
            Some(mut journal) => journal.get_mut().delete_backing_file(),
            None => Ok(()),
        }
    }

    /// this guard is not the one that outlives the apply: give it up without reporting,
    /// leaving that to the caller's own guard. also ends the borrow it held.
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for JournalGuard<'_> {
    fn drop(&mut self) {
        // the retry loop reports its own exits, so the only case left here is a delete
        // phase that never finished, and `finish` takes the journal so it says nothing
        if let Some(journal) = self
            .0
            .as_ref()
            .map(JournalRef::get)
            .filter(|journal| journal.is_completed())
        {
            report_unfinished_deletes(journal);
        }
    }
}

/// every create and update applied, then the delete phase failed or died. warn-level
/// like `report_resumable`, and for the same reason: the file left behind is the
/// difference between a re-run that skips them and one that re-applies them all.
fn report_unfinished_deletes(journal: &Journal) {
    let Some(path) = journal
        .backing_file_path()
        .filter(|_| journal.op_count() > 0)
    else {
        return;
    };
    tracing::warn!(
        "apply stopped during the delete phase; the journal at {} records all {} create/update operations as applied, and re-running the same plan skips them and re-issues the deletes",
        path.display(),
        journal.op_count()
    );
}

/// journal-wiring shared by the internal apply-adapters: build the journal from `state`,
/// run the retry loop, and return the result, the resumed count (`None` when none) ready
/// for `ApplyReport::previously_applied_count`, and the journal to `finish` after the
/// caller's delete phase.
pub async fn apply_non_delete_journaled(
    state: &crate::StateStore,
    adapter_name: &str,
    creates_updates: &[Op],
    driver: &mut impl RetryApplyDriver,
) -> Result<(RetryApplyResult, Option<usize>, JournalGuard<'static>)> {
    let mut journal = match state.journal_dir() {
        Some(dir) => Some(Journal::load_or_create(dir, adapter_name, creates_updates)?),
        None => None,
    };
    let (result, borrowed) =
        apply_non_delete_with_retries(creates_updates, journal.as_mut(), driver).await?;
    // the borrow guard covers the local only; the owned one below is what the caller keeps
    borrowed.disarm();
    let previously_applied = result.resumed.len();
    Ok((
        result,
        (previously_applied > 0).then_some(previously_applied),
        JournalGuard::owned(journal),
    ))
}

/// true when `err` is a retryable missing-ref apply error.
pub fn is_missing_ref_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<AdapterApplyError>()
        .is_some_and(|e| matches!(e, AdapterApplyError::MissingRef { .. }))
}

/// comma-joined referenced uids in `ops` that are absent from `resolved`.
pub fn describe_missing_refs<V>(ops: &[Op], resolved: &BTreeMap<Uid, V>) -> String {
    let mut missing = BTreeSet::new();
    for op in ops {
        if let Op::Create { desired, .. } | Op::Update { desired, .. } = op {
            for value in desired.attrs.values() {
                collect_missing_refs(value, resolved, &mut missing);
            }
            for value in desired.key.values() {
                collect_missing_refs(value, resolved, &mut missing);
            }
        }
    }
    missing
        .into_iter()
        .map(|uid| uid.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn collect_missing_refs<V>(
    value: &Value,
    resolved: &BTreeMap<Uid, V>,
    missing: &mut BTreeSet<Uid>,
) {
    match value {
        Value::String(raw) => {
            if let Ok(uid) = Uid::parse_str(raw) {
                if !resolved.contains_key(&uid) {
                    missing.insert(uid);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_missing_refs(item, resolved, missing);
            }
        }
        Value::Object(map) => {
            for value in map.values() {
                collect_missing_refs(value, resolved, missing);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BackendId;
    use alembic_core::{JsonMap, Key, Object, TypeName, Uid};
    use anyhow::anyhow;
    use futures::executor::block_on;
    use rand::rng;
    use rand::seq::SliceRandom;
    use serde_json::json;
    use std::sync::Mutex;
    use tempfile::tempdir;

    // the resume notice is a single tracing callsite, and callsite interest is global: a
    // journaled run on another thread with no subscriber caches it as `never` and the
    // capturing test then sees nothing. serialize every journaled run.
    static JOURNAL_LOCK: Mutex<()> = Mutex::new(());

    fn journal_guard() -> std::sync::MutexGuard<'static, ()> {
        JOURNAL_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn is_missing_ref_error_matches_only_missing_ref() {
        let err = anyhow::Error::from(AdapterApplyError::MissingRef {
            uid: Uid::from_u128(1),
        });
        assert!(is_missing_ref_error(&err));
        assert!(!is_missing_ref_error(&anyhow!("some other error")));
    }

    #[test]
    fn describe_missing_refs_reports_unresolved_nested_refs() {
        let present = Uid::from_u128(1);
        let missing = Uid::from_u128(2);

        let attrs = JsonMap::from(BTreeMap::from([
            ("resolved_ref".to_string(), json!(present.to_string())),
            (
                "nested".to_string(),
                json!({ "list": [missing.to_string()] }),
            ),
        ]));
        let op = Op::Create {
            uid: present,
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid: present,
                type_name: TypeName::new("test.item"),
                key: Key::default(),
                attrs,
                source: None,
            },
        };

        let mut resolved = BTreeMap::new();
        resolved.insert(present, BackendId::Int(1));

        let described = describe_missing_refs(&[op], &resolved);
        assert!(described.contains(&missing.to_string()));
        assert!(!described.contains(&present.to_string()));
    }

    #[test]
    fn describe_missing_refs_reports_unresolved_key_refs() {
        let present = Uid::from_u128(1);
        let missing = Uid::from_u128(3);
        let mut key = Key::default();
        key.insert("device".to_string(), json!(missing.to_string()));
        let op = Op::Create {
            uid: present,
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid: present,
                type_name: TypeName::new("test.item"),
                key,
                attrs: JsonMap::default(),
                source: None,
            },
        };
        let resolved: BTreeMap<Uid, BackendId> = BTreeMap::new();
        let described = describe_missing_refs(&[op], &resolved);
        assert!(described.contains(&missing.to_string()));
    }

    fn create_op(uid: Uid) -> Op {
        Op::Create {
            uid,
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid,
                type_name: TypeName::new("test.item"),
                key: Key::default(),
                attrs: JsonMap::default(),
                source: None,
            },
        }
    }

    #[derive(Clone, Copy)]
    enum Mode {
        RetryThenOk,
        AlwaysRetry,
        AlwaysRetryUid(Uid),
        Fatal,
    }

    struct TestDriver {
        attempts: usize,
        mode: Mode,
    }

    #[async_trait]
    impl RetryApplyDriver for TestDriver {
        async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp> {
            self.attempts += 1;
            match self.mode {
                Mode::RetryThenOk if self.attempts == 1 => {
                    Err(anyhow!("missing referenced uid {}", op.uid()))
                }
                Mode::AlwaysRetry => Err(anyhow!("missing referenced uid {}", op.uid())),
                Mode::AlwaysRetryUid(uid) if op.uid() == uid => {
                    Err(anyhow!("missing referenced uid {uid}"))
                }
                Mode::Fatal => Err(anyhow!("boom")),
                Mode::AlwaysRetryUid(_) | Mode::RetryThenOk => Ok(AppliedOp {
                    uid: op.uid(),
                    type_name: op.type_name().clone(),
                    backend_id: Some(BackendId::Int(1)),
                }),
            }
        }

        fn is_retryable(&self, err: &anyhow::Error) -> bool {
            err.to_string().contains("missing referenced uid")
        }
    }

    #[tokio::test]
    async fn retries_then_applies() {
        let uid1 = Uid::from_u128(1);
        let uid2 = Uid::from_u128(2);
        let ops = vec![create_op(uid1), create_op(uid2)];
        let mut driver = TestDriver {
            attempts: 0,
            mode: Mode::RetryThenOk,
        };

        let (result, _) = apply_non_delete_with_retries(&ops, None, &mut driver)
            .await
            .unwrap();

        assert_eq!(driver.attempts, 3);
        assert_eq!(result.applied.len(), 2);
        assert!(result.pending.is_empty());
    }

    #[tokio::test]
    async fn returns_pending_when_stuck() {
        let uid = Uid::from_u128(1);
        let ops = vec![create_op(uid)];
        let mut driver = TestDriver {
            attempts: 0,
            mode: Mode::AlwaysRetry,
        };

        let (result, _) = apply_non_delete_with_retries(&ops, None, &mut driver)
            .await
            .unwrap();

        assert!(result.applied.is_empty());
        assert_eq!(result.pending.len(), 1);
    }

    #[tokio::test]
    async fn returns_non_retryable_error() {
        let uid = Uid::from_u128(1);
        let ops = vec![create_op(uid)];
        let mut driver = TestDriver {
            attempts: 0,
            mode: Mode::Fatal,
        };

        let err = apply_non_delete_with_retries(&ops, None, &mut driver)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn ignores_delete_ops() {
        let uid = Uid::from_u128(1);
        let ops = vec![Op::Delete {
            uid,
            type_name: TypeName::new("test.item"),
            key: Key::default(),
            backend_id: None,
        }];
        let mut driver = TestDriver {
            attempts: 0,
            mode: Mode::Fatal,
        };

        let (result, _) = apply_non_delete_with_retries(&ops, None, &mut driver)
            .await
            .unwrap();

        assert_eq!(driver.attempts, 0);
        assert!(result.pending.is_empty());
        assert!(result.applied.is_empty());
    }

    struct ErraticDriver {
        countdown_to_crash: u32,
        applied_ops: Vec<AppliedOp>,
    }

    #[async_trait]
    impl RetryApplyDriver for ErraticDriver {
        async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp> {
            self.countdown_to_crash -= 1;

            if self.countdown_to_crash == 0 {
                return Err(anyhow!("planned error"));
            }

            let applied_op = AppliedOp {
                uid: op.uid(),
                type_name: op.type_name().clone(),
                backend_id: None,
            };
            self.applied_ops.push(applied_op.clone());

            Ok(applied_op)
        }

        fn is_retryable(&self, _err: &anyhow::Error) -> bool {
            false
        }
    }
    #[test]
    fn erratic_driver_first_fails_then_succeeds() {
        let _guard = journal_guard();
        let uid1 = Uid::from_u128(1);
        let uid2 = Uid::from_u128(2);
        let ops = vec![create_op(uid1), create_op(uid2)];
        let mut driver = ErraticDriver {
            countdown_to_crash: 2,
            applied_ops: vec![],
        };
        let dir = tempdir().unwrap();
        let mut journal = Journal::load_or_create(dir.path(), "erratic_driver", &ops).unwrap();

        block_on(apply_non_delete_with_retries(
            &ops,
            Some(&mut journal),
            &mut driver,
        ))
        .expect_err("should fail (on second op applied this run)");
        assert_eq!(driver.applied_ops.len(), 1);
        assert!(!journal.is_completed());

        // turn off crashing
        driver.countdown_to_crash = 99999;
        _ = block_on(apply_non_delete_with_retries(
            &ops,
            Some(&mut journal),
            &mut driver,
        ))
        .unwrap();
        assert_eq!(
            driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>(),
            vec![uid1, uid2]
        );
        assert!(journal.is_completed());
    }

    #[test]
    fn resumes_from_disk_after_error() {
        let _guard = journal_guard();
        let uid1 = Uid::from_u128(1);
        let uid2 = Uid::from_u128(2);
        let uid3 = Uid::from_u128(3);
        let ops = vec![create_op(uid1), create_op(uid2), create_op(uid3)];
        let dir = tempdir().unwrap();

        // first run crashes after applying the first op; the journal is dropped to
        // simulate the process exiting, so resume must rely on what was flushed to disk.
        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 2,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "resume_test", &ops).unwrap();
            block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .expect_err("should fail on the second op");
            assert_eq!(driver.applied_ops.len(), 1);
        }

        // second run reloads the journal from disk and applies only the remaining ops.
        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "resume_test", &ops).unwrap();
            let (result, _) = block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .unwrap();
            assert_eq!(
                driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>(),
                vec![uid2, uid3]
            );
            assert_eq!(result.applied.len(), 2);
            assert!(result.pending.is_empty());
            assert!(journal.is_completed());
        }
    }

    #[test]
    fn erratic_driver_with_shuffled_ops() {
        let _guard = journal_guard();
        let mut ops = Vec::new();
        for i in 1..10 {
            ops.push(create_op(Uid::from_u128(i)));
        }

        let mut rng = rng();
        ops.shuffle(&mut rng);

        let mut driver = ErraticDriver {
            countdown_to_crash: 5,
            applied_ops: vec![],
        };
        let dir = tempdir().unwrap();
        let mut journal = Journal::load_or_create(dir.path(), "erratic_driver", &ops).unwrap();

        block_on(apply_non_delete_with_retries(
            &ops,
            Some(&mut journal),
            &mut driver,
        ))
        .expect_err("should fail (on fifth op applied this run)");
        assert_eq!(driver.applied_ops.len(), 4);
        assert!(!journal.is_completed());

        ops.shuffle(&mut rng);

        // turn off crashing
        driver.countdown_to_crash = 99999;
        _ = block_on(apply_non_delete_with_retries(
            &ops,
            Some(&mut journal),
            &mut driver,
        ))
        .unwrap();

        let mut applied_uids = driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>();
        applied_uids.sort();
        let mut op_uids = ops.iter().map(|op| op.uid()).collect::<Vec<_>>();
        op_uids.sort();
        assert_eq!(applied_uids, op_uids,);
        assert!(journal.is_completed());
    }

    /// exercises the same `apply_non_delete_journaled` wiring the adapters use:
    /// filter to non-delete ops, run the journaled retry loop, and surface the
    /// resumed count on the report.
    async fn run_journaled_apply(
        state: &crate::StateStore,
        ops: &[Op],
        driver: &mut impl RetryApplyDriver,
    ) -> Result<crate::ApplyReport> {
        run_journaled_apply_with_deletes(state, ops, driver, Ok(())).await
    }

    /// the adapters' shape down to the order: retry loop, early return on leftover
    /// pending ops, the unjournaled delete phase (`deletes` stands in for it), then
    /// `finish` on the success path only.
    async fn run_journaled_apply_with_deletes(
        state: &crate::StateStore,
        ops: &[Op],
        driver: &mut impl RetryApplyDriver,
        deletes: Result<()>,
    ) -> Result<crate::ApplyReport> {
        let creates_updates: Vec<Op> = ops
            .iter()
            .filter(|op| !matches!(op, Op::Delete { .. }))
            .cloned()
            .collect();
        let (result, previously_applied_count, journal) =
            apply_non_delete_journaled(state, "test", &creates_updates, driver).await?;
        if !result.pending.is_empty() {
            let resolved: BTreeMap<Uid, BackendId> = BTreeMap::new();
            return Err(anyhow!(
                "unresolved references: {}",
                describe_missing_refs(&result.pending, &resolved)
            ));
        }
        deletes?;
        journal.finish()?;
        Ok(crate::ApplyReport {
            applied: result.applied,
            resumed: result.resumed,
            previously_applied_count,
            ..Default::default()
        })
    }

    /// creates that carry a `site` ref, which must already resolve or the op comes
    /// back as a retryable missing-ref: the plan shape resume exists for.
    struct RefDriver {
        resolved: BTreeMap<Uid, BackendId>,
        next_id: u64,
        fatal_on: Option<Uid>,
        // dies without unwinding to the apply's exits, the way a sigkill does
        kill_on: Option<Uid>,
        seen_refs: Vec<(Uid, BackendId)>,
    }

    impl RefDriver {
        fn new(fatal_on: Option<Uid>) -> Self {
            Self {
                resolved: BTreeMap::new(),
                next_id: 100,
                fatal_on,
                kill_on: None,
                seen_refs: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl RetryApplyDriver for RefDriver {
        async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp> {
            assert_ne!(self.kill_on, Some(op.uid()), "killed mid-apply");
            if self.fatal_on == Some(op.uid()) {
                return Err(anyhow!("planned error"));
            }
            if let Op::Create { desired, .. } = op {
                if let Some(referenced) = desired
                    .attrs
                    .get("site")
                    .and_then(|value| value.as_str())
                    .and_then(|value| value.parse::<Uid>().ok())
                {
                    let id = self
                        .resolved
                        .get(&referenced)
                        .ok_or(AdapterApplyError::MissingRef { uid: referenced })?;
                    self.seen_refs.push((op.uid(), id.clone()));
                }
            }
            let backend_id = match op {
                // the journaling adapters hand back the id of the object they updated
                Op::Update {
                    backend_id: Some(id),
                    ..
                } => id.clone(),
                _ => {
                    let id = BackendId::Int(self.next_id);
                    self.next_id += 1;
                    id
                }
            };
            self.resolved.insert(op.uid(), backend_id.clone());
            Ok(AppliedOp {
                uid: op.uid(),
                type_name: op.type_name().clone(),
                backend_id: Some(backend_id),
            })
        }

        fn is_retryable(&self, err: &anyhow::Error) -> bool {
            is_missing_ref_error(err)
        }

        fn resume(&mut self, resumed: &[AppliedOp]) {
            for op in resumed {
                if let Some(backend_id) = &op.backend_id {
                    self.resolved.insert(op.uid, backend_id.clone());
                }
            }
        }
    }

    fn update_op(uid: Uid, backend_id: u64) -> Op {
        Op::Update {
            uid,
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid,
                type_name: TypeName::new("test.item"),
                key: Key::default(),
                attrs: JsonMap::default(),
                source: None,
            },
            changes: vec![],
            backend_id: Some(BackendId::Int(backend_id)),
        }
    }

    fn create_op_referencing(uid: Uid, site: Uid) -> Op {
        let attrs = JsonMap::from(BTreeMap::from([(
            "site".to_string(),
            json!(site.to_string()),
        )]));
        Op::Create {
            uid,
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid,
                type_name: TypeName::new("test.item"),
                key: Key::default(),
                attrs,
                source: None,
            },
        }
    }

    #[test]
    fn a_resumed_run_resolves_refs_into_what_the_interrupted_run_created() {
        let _guard = journal_guard();
        let site = Uid::from_u128(1);
        let device = Uid::from_u128(2);
        let ops = vec![create_op(site), create_op_referencing(device, site)];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());

        // run 1 creates the site, then dies on the device that references it.
        let mut first = RefDriver::new(Some(device));
        block_on(run_journaled_apply(&state, &ops, &mut first))
            .expect_err("should fail on the device");
        assert_eq!(first.resolved.get(&site), Some(&BackendId::Int(100)));

        // run 2 starts cold: nothing in memory, and run 1 never reached a state save,
        // so the device's ref resolves only if the journal handed the site's id back.
        let mut second = RefDriver::new(None);
        let report = block_on(run_journaled_apply(&state, &ops, &mut second)).unwrap();
        assert_eq!(second.seen_refs, vec![(device, BackendId::Int(100))]);
        assert_eq!(
            report.applied.iter().map(|a| a.uid).collect::<Vec<_>>(),
            vec![device],
            "`applied` stays this run's ops"
        );
        assert_eq!(
            report
                .resumed
                .iter()
                .map(|a| (a.uid, a.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![(site, Some(BackendId::Int(100)))],
            "the interrupted run's op comes back with the id it created"
        );
        assert_eq!(report.previously_applied_count, Some(1));
    }

    #[test]
    fn a_run_killed_mid_apply_resumes_from_what_it_had_applied() {
        let _guard = journal_guard();
        // #46's headline, and the case an error-only resume never covered: the run
        // dies instead of returning, so neither exit of the retry loop runs. the ops
        // it applied are on disk anyway, because marking one is what writes it.
        let site = Uid::from_u128(1);
        let device = Uid::from_u128(2);
        let ops = vec![create_op(site), create_op_referencing(device, site)];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());
        let journal_path = crate::Journal::stable_file_name(dir.path(), "test", &ops);

        let mut first = RefDriver::new(None);
        first.kill_on = Some(device);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(run_journaled_apply(&state, &ops, &mut first))
        }));
        assert!(outcome.is_err(), "the run must have died on the device");
        assert!(journal_path.exists());

        let mut second = RefDriver::new(None);
        let report = block_on(run_journaled_apply(&state, &ops, &mut second)).unwrap();
        assert_eq!(
            second.seen_refs,
            vec![(device, BackendId::Int(100))],
            "the device resolves its ref into the site the killed run created"
        );
        assert_eq!(
            report.applied.iter().map(|a| a.uid).collect::<Vec<_>>(),
            vec![device],
            "the site is not created a second time"
        );
        assert_eq!(report.previously_applied_count, Some(1));
        assert!(!journal_path.exists());
    }

    #[test]
    fn a_resumed_run_resolves_refs_into_what_the_interrupted_run_updated() {
        let _guard = journal_guard();
        // same shape as the create case: nothing about the journal is per op kind, and
        // an update's id is the one thing a ref into an existing object can resolve to.
        let site = Uid::from_u128(1);
        let device = Uid::from_u128(2);
        let ops = vec![update_op(site, 55), create_op_referencing(device, site)];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());

        let mut first = RefDriver::new(Some(device));
        block_on(run_journaled_apply(&state, &ops, &mut first))
            .expect_err("should fail on the device");

        let mut second = RefDriver::new(None);
        let report = block_on(run_journaled_apply(&state, &ops, &mut second)).unwrap();
        assert_eq!(
            second.seen_refs,
            vec![(device, BackendId::Int(55))],
            "the device resolves its ref into the object run 1 updated"
        );
        assert_eq!(
            report
                .resumed
                .iter()
                .map(|a| (a.uid, a.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![(site, Some(BackendId::Int(55)))],
            "an update's id is journaled and recovered like a create's"
        );
    }

    #[test]
    fn journaled_apply_via_state_store_resumes_and_reports() {
        let _guard = journal_guard();
        let uid1 = Uid::from_u128(1);
        let uid2 = Uid::from_u128(2);
        let uid3 = Uid::from_u128(3);
        let ops = vec![create_op(uid1), create_op(uid2), create_op(uid3)];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());
        let journal_path = crate::Journal::stable_file_name(dir.path(), "test", &ops);

        // first run crashes after applying the first op; the journal persists progress.
        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 2,
                applied_ops: vec![],
            };
            block_on(run_journaled_apply(&state, &ops, &mut driver))
                .expect_err("should crash on the second op");
            assert_eq!(driver.applied_ops.len(), 1);
            assert!(journal_path.exists());
        }

        // second run resumes: only the remaining ops apply, the report notes the
        // resumed count, and the completed journal is cleaned up.
        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            let report = block_on(run_journaled_apply(&state, &ops, &mut driver)).unwrap();
            assert_eq!(
                driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>(),
                vec![uid2, uid3]
            );
            assert_eq!(report.applied.len(), 2);
            assert_eq!(report.previously_applied_count, Some(1));
            assert!(!journal_path.exists());
        }
    }

    #[test]
    fn fatal_error_reports_the_journal_and_that_a_re_run_resumes() {
        let _guard = journal_guard();
        let ops = vec![
            create_op(Uid::from_u128(1)),
            create_op(Uid::from_u128(2)),
            create_op(Uid::from_u128(3)),
        ];
        let dir = tempdir().unwrap();
        let journal_path = Journal::stable_file_name(dir.path(), "notice", &ops);

        let logged = crate::test_log::capture(|| {
            let mut driver = ErraticDriver {
                countdown_to_crash: 2,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "notice", &ops).unwrap();
            block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .expect_err("should fail on the second op");
        })
        .1;

        assert!(
            logged.contains("apply stopped after 1 of 3 create/update operations"),
            "got: {logged}"
        );
        assert!(
            logged.contains(&journal_path.display().to_string()),
            "the message names the journal file, got: {logged}"
        );
        assert!(logged.contains("resumes from there"), "got: {logged}");
    }

    #[test]
    fn stuck_with_no_progress_reports_the_journal_too() {
        let _guard = journal_guard();
        // the retry loop exits without an error here, but the adapters turn leftover
        // pending ops into one, so the user is in the same failed-apply spot.
        let stuck = Uid::from_u128(2);
        let ops = vec![create_op(Uid::from_u128(1)), create_op(stuck)];
        let dir = tempdir().unwrap();

        let (result, logged) = crate::test_log::capture(|| {
            let mut driver = TestDriver {
                attempts: 0,
                mode: Mode::AlwaysRetryUid(stuck),
            };
            let mut journal = Journal::load_or_create(dir.path(), "stuck", &ops).unwrap();
            let (result, _) = block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .unwrap();
            result
        });

        assert_eq!(result.pending.len(), 1);
        assert!(
            logged.contains("apply stopped after 1 of 2 create/update operations"),
            "got: {logged}"
        );
    }

    #[test]
    fn a_failure_before_any_op_applied_says_nothing_about_the_journal() {
        let _guard = journal_guard();
        // an unreachable backend fails on the first op: there is no progress to describe.
        let ops = vec![create_op(Uid::from_u128(1)), create_op(Uid::from_u128(2))];
        let dir = tempdir().unwrap();

        let logged = crate::test_log::capture(|| {
            let mut driver = TestDriver {
                attempts: 0,
                mode: Mode::Fatal,
            };
            let mut journal = Journal::load_or_create(dir.path(), "cold", &ops).unwrap();
            block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .expect_err("should fail on the first op");
        })
        .1;

        assert!(!logged.contains("apply stopped"), "got: {logged}");
    }

    #[test]
    fn a_successful_apply_says_nothing_about_the_journal() {
        let _guard = journal_guard();
        let ops = vec![create_op(Uid::from_u128(1))];
        let dir = tempdir().unwrap();

        let logged = crate::test_log::capture(|| {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "clean", &ops).unwrap();
            block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .unwrap()
            .1
            .finish()
            .unwrap();
        })
        .1;

        assert!(!logged.contains("apply stopped"), "got: {logged}");
    }

    #[test]
    fn a_successful_journaled_apply_says_nothing_about_the_journal() {
        let _guard = journal_guard();
        // the same rule through the other entry point: the borrow guard covers the engine's
        // local journal only, so on the success path the owned one is all that may speak.
        let ops = vec![create_op(Uid::from_u128(1))];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());

        let logged = crate::test_log::capture(|| {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            block_on(run_journaled_apply(&state, &ops, &mut driver)).unwrap();
        })
        .1;

        assert!(!logged.contains("apply stopped"), "got: {logged}");
    }

    #[test]
    fn the_journal_outlives_a_completed_create_update_phase() {
        let _guard = journal_guard();
        // the deletes still have to run, and they are not journaled: unlinking the file
        // as the last create landed is what left a delete-phase crash with no record.
        let ops = vec![create_op(Uid::from_u128(1))];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());
        let journal_path = Journal::stable_file_name(dir.path(), "test", &ops);

        let mut driver = ErraticDriver {
            countdown_to_crash: 99999,
            applied_ops: vec![],
        };
        let (result, _, journal) = block_on(apply_non_delete_journaled(
            &state,
            "test",
            &ops,
            &mut driver,
        ))
        .unwrap();

        assert_eq!(result.applied.len(), 1);
        assert!(journal_path.exists(), "the retry loop must not unlink it");
        journal.finish().unwrap();
        assert!(!journal_path.exists(), "finish is what unlinks it");
    }

    #[test]
    fn a_delete_phase_that_never_finished_leaves_the_journal_and_says_so() {
        let _guard = journal_guard();
        let ops = vec![create_op(Uid::from_u128(1)), create_op(Uid::from_u128(2))];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());
        let journal_path = Journal::stable_file_name(dir.path(), "test", &ops);

        let logged = crate::test_log::capture(|| {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            block_on(run_journaled_apply_with_deletes(
                &state,
                &ops,
                &mut driver,
                Err(anyhow!("the backend refused the delete")),
            ))
            .expect_err("the delete phase fails");
        })
        .1;

        assert!(journal_path.exists(), "a re-run has to find it");
        assert!(
            logged.contains("apply stopped during the delete phase"),
            "got: {logged}"
        );
        assert!(
            logged.contains(&journal_path.display().to_string()),
            "the message names the journal file, got: {logged}"
        );
        assert!(logged.contains("re-issues the deletes"), "got: {logged}");
    }

    #[test]
    fn an_unfinished_create_update_phase_is_not_reported_twice() {
        let _guard = journal_guard();
        // the retry loop reports this case itself, and the journal reaches the guard
        // dropped rather than finished, so the notice must not come out twice.
        let stuck = Uid::from_u128(2);
        let ops = vec![create_op(Uid::from_u128(1)), create_op(stuck)];
        let dir = tempdir().unwrap();
        let state = crate::StateStore::new(None, crate::StateData::default())
            .with_journal_dir(dir.path().to_path_buf());

        let logged = crate::test_log::capture(|| {
            let mut driver = TestDriver {
                attempts: 0,
                mode: Mode::AlwaysRetryUid(stuck),
            };
            block_on(run_journaled_apply(&state, &ops, &mut driver))
                .expect_err("leftover pending ops end the apply");
        })
        .1;

        assert_eq!(logged.matches("apply stopped").count(), 1, "got: {logged}");
        assert!(!logged.contains("delete phase"), "got: {logged}");
    }

    #[test]
    fn an_external_caller_driving_the_retry_loop_leaves_nothing_to_resume() {
        let _guard = journal_guard();
        // the sdk surface an out-of-tree adapter builds against: its own journal, no
        // `apply_non_delete_journaled`. the file is this caller's to clean up too.
        let ops = vec![create_op(Uid::from_u128(1)), create_op(Uid::from_u128(2))];
        let dir = tempdir().unwrap();
        let journal_path = Journal::stable_file_name(dir.path(), "external", &ops);

        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "external", &ops).unwrap();
            block_on(apply_non_delete_with_retries(
                &ops,
                Some(&mut journal),
                &mut driver,
            ))
            .unwrap()
            .1
            .finish()
            .unwrap();
        }
        assert!(
            !journal_path.exists(),
            "an apply through with its deletes leaves nothing to resume"
        );

        // the same plan again: it has to be applied, not resumed off a run that finished.
        let mut driver = ErraticDriver {
            countdown_to_crash: 99999,
            applied_ops: vec![],
        };
        let mut journal = Journal::load_or_create(dir.path(), "external", &ops).unwrap();
        let (result, _) = block_on(apply_non_delete_with_retries(
            &ops,
            Some(&mut journal),
            &mut driver,
        ))
        .unwrap();

        assert_eq!(
            driver.applied_ops.len(),
            2,
            "the backend is written to again"
        );
        assert!(result.resumed.is_empty(), "nothing was resumed");
    }

    #[test]
    fn a_retry_loop_journal_dropped_without_finish_stays_for_the_re_run() {
        let _guard = journal_guard();
        // the same caller, its deletes not through: one rule, so the notice and the file
        // left behind are the ones `apply_non_delete_journaled` gives its own adapters.
        let ops = vec![create_op(Uid::from_u128(1))];
        let dir = tempdir().unwrap();
        let journal_path = Journal::stable_file_name(dir.path(), "external", &ops);

        let logged = crate::test_log::capture(|| {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "external", &ops).unwrap();
            drop(
                block_on(apply_non_delete_with_retries(
                    &ops,
                    Some(&mut journal),
                    &mut driver,
                ))
                .unwrap(),
            );
        })
        .1;

        assert!(journal_path.exists(), "a re-run has to find it");
        assert!(
            logged.contains("apply stopped during the delete phase"),
            "got: {logged}"
        );
    }

    #[tokio::test]
    async fn journaled_apply_without_journal_dir_reports_no_resume() {
        let ops = vec![create_op(Uid::from_u128(1))];
        let state = crate::StateStore::new(None, crate::StateData::default());
        let mut driver = ErraticDriver {
            countdown_to_crash: 99999,
            applied_ops: vec![],
        };
        let report = run_journaled_apply(&state, &ops, &mut driver)
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.previously_applied_count, None);
    }
}
