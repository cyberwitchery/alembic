use crate::errors::AdapterApplyError;
use crate::journal::Journal;
use crate::types::{AppliedOp, Op};
use alembic_core::Uid;
use anyhow::anyhow;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Debug)]
pub struct RetryApplyResult {
    pub applied: Vec<AppliedOp>,
    pub pending: Vec<Op>,
    /// ops earlier runs of this plan applied, recovered from the journal in plan order.
    pub resumed: Vec<AppliedOp>,
}

#[async_trait]
pub trait RetryApplyDriver {
    async fn apply_non_delete(&mut self, op: &Op) -> anyhow::Result<AppliedOp>;
    fn is_retryable(&self, err: &anyhow::Error) -> bool;
    /// handed the ops an earlier run applied, before this run's first op, so the
    /// driver can resolve references into objects it is not going to create again.
    fn resume(&mut self, _resumed: &[AppliedOp]) {}
}

pub async fn apply_non_delete_with_retries<'a>(
    ops: &[Op],
    mut journal: Option<&'a mut Journal>,
    driver: &mut impl RetryApplyDriver,
) -> anyhow::Result<(RetryApplyResult, JournalGuard<'a>)> {
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

/// run the retry loop over a journal loaded from `dir` under `scope` (none when `dir` is
/// `None`), returning the result, the resumed count (`None` when none) ready for
/// `ApplyReport::previously_applied_count`, and the journal to `finish` after the deletes.
pub async fn apply_non_delete_with_journal(
    dir: Option<&Path>,
    scope: &str,
    creates_updates: &[Op],
    driver: &mut impl RetryApplyDriver,
) -> anyhow::Result<(RetryApplyResult, Option<usize>, JournalGuard<'static>)> {
    let mut journal = match dir {
        Some(dir) => Some(Journal::load_or_create(dir, scope, creates_updates)?),
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
    pub fn finish(mut self) -> anyhow::Result<()> {
        match self.0.take() {
            Some(mut journal) => Ok(journal.get_mut().delete_backing_file()?),
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
