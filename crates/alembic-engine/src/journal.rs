//! keep track of successfully applied ops to enable resume after an error.
//!
//! when resuming, the journal must match the previous run's non-delete op sequence (including op hashes).
//!
//! the file is append-only: a version line, one line per planned op, then one line
//! per op as it completes. each line is a yaml document in flow style, so a record
//! is a single `write` an interrupted process cannot leave half-applied to what came
//! before it. a run killed without unwinding therefore still leaves every op it
//! applied on disk.

use crate::{AppliedOp, BackendId, Op};
use alembic_core::{TypeName, Uid};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

/// bumped when a record shape changes. an unknown version is refused by name
/// rather than read as this one.
const FORMAT_VERSION: u32 = 1;

/// every line carries it, so a stray line is never mistaken for a record.
const DOCUMENT_PREFIX: &str = "--- ";

#[derive(Debug)]
pub struct Journal {
    file: Option<(File, PathBuf)>,
    ops: Vec<OpWithMeta>,
    // not-yet-done op positions, keyed by (uid, typename, hash), in plan order.
    // derived from `ops` and rebuilt whenever `ops` is (re)loaded; lets
    // `mark_op_as_done` be O(1) instead of a linear scan from the start.
    pending_index: HashMap<(Uid, TypeName, u64), VecDeque<usize>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Version {
    version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Record {
    Op(OpKey),
    Done(Done),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpKey {
    op_uid: Uid,
    op_typename: TypeName,
    op_hash: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Done {
    // position in the op lines above, which is what makes a record one short line
    op: usize,
    backend_id: Option<BackendId>,
}

/// the whole-document shape written before the journal was append-only.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyJournal {
    ops: Vec<OpWithMeta>,
}

fn record_line<T: Serialize>(value: &T) -> Result<String> {
    Ok(format!(
        "{DOCUMENT_PREFIX}{}\n",
        serde_json::to_string(value)?
    ))
}

fn build_pending_index(ops: &[OpWithMeta]) -> HashMap<(Uid, TypeName, u64), VecDeque<usize>> {
    let mut index: HashMap<(Uid, TypeName, u64), VecDeque<usize>> = HashMap::new();
    for (i, owm) in ops.iter().enumerate() {
        if !owm.done {
            index
                .entry((owm.op_uid, owm.op_typename.clone(), owm.op_hash))
                .or_default()
                .push_back(i);
        }
    }
    index
}

impl Journal {
    pub fn stable_file_name(directory: &Path, adapter_name: &str, ops: &[Op]) -> PathBuf {
        let hash = crate::types::stable_json_hash(&ops);
        let file_name: PathBuf = format!("{}_journal_{}.yaml", adapter_name, hash).into();
        directory.join(file_name)
    }

    /// tries to load a Journal from `file_path`, otherwise creates a new one.
    /// in either case, the new Journal instance will be backed by the file at `file_path`.
    /// delete ops will not be saved in the journal.
    pub fn load_or_create(directory: &Path, adapter_name: &str, ops: &[Op]) -> Result<Self> {
        // apply writes the journal before any state save, so `directory` (e.g. `.alembic/`)
        // may not exist yet on a fresh checkout.
        fs::create_dir_all(directory)?;
        let file_name = Self::stable_file_name(directory, adapter_name, ops);
        if fs::metadata(&file_name).is_ok() {
            Self::new_from_existing_file(directory, adapter_name, ops)
        } else {
            Self::new_with_file(directory, adapter_name, ops)
        }
    }

    /// loads a journal from the file with `file_path` and sets its backing file to that file
    fn new_from_existing_file(
        directory: &Path,
        adapter_name: &str,
        expected_ops: &[Op],
    ) -> Result<Self> {
        let file_name = Self::stable_file_name(directory, adapter_name, expected_ops);
        let contents = fs::read_to_string(&file_name)?;

        let (ops, rewrite) = match contents.starts_with(DOCUMENT_PREFIX) {
            true => parse_records(&contents, &file_name)?,
            false => (parse_legacy(&contents, &file_name)?, true),
        };

        let mut journal = Journal {
            file: None,
            pending_index: build_pending_index(&ops),
            ops,
        };

        let journal_keys = journal
            .ops
            .iter()
            .map(|op_with_meta| {
                (
                    op_with_meta.op_uid,
                    &op_with_meta.op_typename,
                    op_with_meta.op_hash,
                )
            })
            .collect::<Vec<(Uid, &TypeName, u64)>>();

        let expected_keys = expected_ops
            .iter()
            .filter(|op| !matches!(op, Op::Delete { .. }))
            .map(|op| (op.uid(), op.type_name(), op.hashed()))
            .collect::<Vec<(Uid, &TypeName, u64)>>();
        if journal_keys != expected_keys {
            return Err(anyhow!(
                "the ops in the loaded journal file `{}` don't match the expected ops",
                file_name.display()
            ));
        }

        // neither a legacy whole-document journal nor one whose torn tail the parse
        // dropped can be appended to as it stands. rewriting once, here, is what makes
        // every write from now on an append.
        let file = match rewrite {
            true => write_whole_file(&file_name, &journal.ops)?,
            false => open_for_append(&file_name)?,
        };
        journal.file = Some((file, file_name));
        Ok(journal)
    }

    /// creates a journal with a new backing file
    fn new_with_file(directory: &Path, adapter_name: &str, ops: &[Op]) -> Result<Self> {
        let file_name = Self::stable_file_name(directory, adapter_name, ops);
        let mut journal = Self::new_ephemeral(ops);

        // write the plan out before applying any op, both to check the file works and
        // because a `done` record is only meaningful against the op lines it indexes
        let file = write_whole_file(&file_name, &journal.ops)?;
        journal.file = Some((file, file_name));

        Ok(journal)
    }

    /// creates a journal without a backing file set
    pub fn new_ephemeral(ops: &[Op]) -> Self {
        let ops: Vec<OpWithMeta> = ops
            .iter()
            .filter(|op| !matches!(op, Op::Delete { .. }))
            .map(OpWithMeta::new)
            .collect();
        let pending_index = build_pending_index(&ops);
        Self {
            file: None,
            ops,
            pending_index,
        }
    }

    pub fn done_ops(&self) -> Vec<(Uid, TypeName, u64)> {
        self.ops
            .iter()
            .filter(|owm| owm.done)
            .map(|owm| (owm.op_uid, owm.op_typename.clone(), owm.op_hash))
            .collect()
    }

    /// ops recorded as done, in plan order, with the backend id each one returned.
    /// a journal written before ids were recorded yields `None` for every id.
    pub fn done_applied_ops(&self) -> Vec<AppliedOp> {
        self.ops
            .iter()
            .filter(|owm| owm.done)
            .map(|owm| AppliedOp {
                uid: owm.op_uid,
                type_name: owm.op_typename.clone(),
                backend_id: owm.backend_id.clone(),
            })
            .collect()
    }

    pub fn done_ops_count(&self) -> usize {
        self.ops.iter().filter(|op| op.done).count()
    }

    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    /// path of the file this journal is backed by, if any.
    pub fn backing_file_path(&self) -> Option<&Path> {
        self.file.as_ref().map(|(_, path)| path.as_path())
    }

    pub fn is_completed(&self) -> bool {
        self.ops.iter().all(|op| op.done)
    }

    /// marks the first not-done op matching `op`'s (uid, typename, hash) done and
    /// records the id the backend returned for it. the record reaches disk before
    /// this returns, so the next op is never applied against an unrecorded one.
    /// O(1) via `pending_index`; errors if there's no such op.
    pub fn mark_op_as_done(&mut self, op: &Op, backend_id: Option<&BackendId>) -> Result<()> {
        let key = (op.uid(), op.type_name().clone(), op.hashed());
        let op_index = self
            .pending_index
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| anyhow!("no matching op found in journal, can't mark any as done"))?;
        // the position came from the pending index, so it is in range and not yet done
        self.ops[op_index].done = true;
        self.ops[op_index].backend_id = backend_id.cloned();

        let Some((file, path)) = self.file.as_mut() else {
            return Ok(());
        };
        let record = Record::Done(Done {
            op: op_index,
            backend_id: backend_id.cloned(),
        });
        let line = record_line(&record)?;
        // one fsync per applied op. measured on an apfs ssd at ~5ms against ~12ms for
        // the whole-file rewrite this replaces, and it is what makes the record
        // survive a power cut rather than only a killed process.
        file.write_all(line.as_bytes())
            .and_then(|()| file.sync_data())
            .with_context(|| format!("failed to append to the journal at {}", path.display()))
    }

    pub fn delete_backing_file(&mut self) -> Result<()> {
        if let Some((file, file_path)) = self.file.take() {
            drop(file);
            fs::remove_file(file_path)?;
        }
        Ok(())
    }
}

/// writes the whole file and hands back an append handle. used to lay down a fresh
/// journal and to migrate a legacy one; the rename keeps a crash mid-write from
/// replacing a readable journal with half of one.
fn write_whole_file(path: &Path, ops: &[OpWithMeta]) -> Result<File> {
    let mut body = record_line(&Version {
        version: FORMAT_VERSION,
    })?;
    for op in ops {
        body.push_str(&record_line(&Record::Op(OpKey {
            op_uid: op.op_uid,
            op_typename: op.op_typename.clone(),
            op_hash: op.op_hash,
        }))?);
    }
    for (index, op) in ops.iter().enumerate().filter(|(_, op)| op.done) {
        body.push_str(&record_line(&Record::Done(Done {
            op: index,
            backend_id: op.backend_id.clone(),
        }))?);
    }

    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("file path has no parent directory"))?;
    let mut temp_file = NamedTempFile::new_in(dir)?;
    temp_file.write_all(body.as_bytes())?;
    temp_file.as_file().sync_all()?; // fsync data + metadata before it can become visible
    temp_file.persist(path)?;
    File::open(dir)?.sync_all()?;

    open_for_append(path)
}

fn open_for_append(path: &Path) -> Result<File> {
    Ok(OpenOptions::new().append(true).open(path)?)
}

/// rebuilds the ops from the record stream, and reports whether a torn final line was
/// dropped: it is the crash this format exists for, everything before it is intact by
/// construction, and the caller has to rewrite the file rather than append after bytes
/// that would leave a record no later load can read.
fn parse_records(contents: &str, path: &Path) -> Result<(Vec<OpWithMeta>, bool)> {
    let mut lines: Vec<&str> = contents.split_inclusive('\n').collect();
    let mut truncated = false;
    if lines.last().is_some_and(|line| !line.ends_with('\n')) {
        lines.pop();
        truncated = true;
    }

    let version_line = lines
        .first()
        .and_then(|line| line.trim_end().strip_prefix(DOCUMENT_PREFIX))
        .ok_or_else(|| anyhow!("the journal file `{}` has no version line", path.display()))?;
    let version: Version = serde_json::from_str(version_line)
        .with_context(|| format!("failed to read the journal file `{}`", path.display()))?;
    if version.version != FORMAT_VERSION {
        return Err(anyhow!(
            "the journal file `{}` is format version {}, but this alembic writes version {}; remove it to start a fresh apply",
            path.display(),
            version.version,
            FORMAT_VERSION
        ));
    }

    let mut ops: Vec<OpWithMeta> = Vec::new();
    let last = lines.len() - 1;
    for (index, line) in lines.iter().enumerate().skip(1) {
        let record: Result<Record> = line
            .trim_end()
            .strip_prefix(DOCUMENT_PREFIX)
            .ok_or_else(|| anyhow!("line {} is not a journal record", index + 1))
            .and_then(|body| Ok(serde_json::from_str(body)?));
        let record = match record {
            Ok(record) => record,
            // a power cut can leave the tail of an appended line unwritten even where
            // the newline landed, so only a broken line further up is corruption
            Err(err) if index == last => {
                tracing::warn!(
                    error = %err,
                    "dropping an incomplete final record from the journal at {}",
                    path.display()
                );
                truncated = true;
                break;
            }
            Err(err) => {
                return Err(err.context(format!(
                    "failed to read the journal file `{}`",
                    path.display()
                )))
            }
        };

        match record {
            Record::Op(key) => ops.push(OpWithMeta {
                op_uid: key.op_uid,
                op_typename: key.op_typename,
                op_hash: key.op_hash,
                done: false,
                backend_id: None,
            }),
            Record::Done(done) => {
                let declared = ops.len();
                let op = ops.get_mut(done.op).ok_or_else(|| {
                    anyhow!(
                        "the journal file `{}` records op {} as done, but only declares {declared}",
                        path.display(),
                        done.op,
                    )
                })?;
                op.done = true;
                op.backend_id = done.backend_id;
            }
        }
    }

    Ok((ops, truncated))
}

fn parse_legacy(contents: &str, path: &Path) -> Result<Vec<OpWithMeta>> {
    let journal: LegacyJournal = serde_yaml::from_str(contents)
        .with_context(|| format!("failed to read the journal file `{}`", path.display()))?;
    Ok(journal.ops)
}

// the op itself is not stored, only its identity (uid, typename, hash). on disk it
// is split in two: an `op` line at plan time, a `done` line when it completes.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct OpWithMeta {
    op_uid: Uid,
    op_typename: TypeName,
    op_hash: u64,
    done: bool,
    // id the backend returned for this op, so a resumed run can resolve references
    // into what an earlier run created or updated. the journaling adapters return one
    // for both; `null` in a journal written before ids were recorded.
    backend_id: Option<BackendId>,
}

impl OpWithMeta {
    fn new(op: &Op) -> Self {
        OpWithMeta {
            op_uid: op.uid(),
            op_typename: op.type_name().clone(),
            op_hash: op.hashed(),
            done: false,
            backend_id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{Object, TypeName};
    use tempfile::tempdir;

    fn test_ops() -> Vec<Op> {
        vec![
            Op::Create {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("dcim.device"),
                desired: Object {
                    uid: Uid::from_u128(1),
                    type_name: TypeName::new("dcim.device"),
                    key: Default::default(),
                    attrs: Default::default(),
                    source: None,
                },
            },
            Op::Create {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("dcim.device"),
                desired: Object {
                    uid: Uid::from_u128(2),
                    type_name: TypeName::new("dcim.device"),
                    key: Default::default(),
                    attrs: Default::default(),
                    source: None,
                },
            },
            Op::Update {
                uid: Uid::from_u128(3),
                type_name: TypeName::new("dcim.site"),
                desired: Object {
                    uid: Uid::from_u128(3),
                    type_name: TypeName::new("dcim.site"),
                    key: Default::default(),
                    attrs: Default::default(),
                    source: None,
                },
                changes: vec![],
                backend_id: None,
            },
        ]
    }

    #[test]
    fn journal_identity_is_stable_across_toolchains() {
        // pinned constants: the journal file name and per-op hashes are persisted
        // to disk and compared on resume, so they must not depend on the rust or
        // dependency versions. if this test breaks, cross-version resume broke.
        let ops = test_ops();
        assert_eq!(ops[0].hashed(), 2629757075790778505);
        assert_eq!(
            Journal::stable_file_name(Path::new("/tmp"), "test", &ops),
            PathBuf::from("/tmp/test_journal_5133211470659736648.yaml")
        );
    }

    #[test]
    fn save_and_load_journal() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
            drop(journal);
        }
        {
            let journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
            assert_eq!(
                journal
                    .ops
                    .iter()
                    .map(|owm| (owm.op_uid, &owm.op_typename))
                    .collect::<Vec<(Uid, &TypeName)>>(),
                ops.iter()
                    .map(|op| (op.uid(), op.type_name()))
                    .collect::<Vec<(Uid, &TypeName)>>()
            );
        }
    }

    #[test]
    fn load_and_append_to_existing_journal() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        Journal::new_with_file(dir.path(), "test", &ops).unwrap();
        {
            let mut journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
            journal.mark_op_as_done(&ops[1], None).unwrap();
            assert_eq!(journal.ops.len(), 3);
        }
        let journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
        assert_eq!(journal.done_ops_count(), 1);
    }

    #[test]
    fn resume_rejects_mismatched_journal_contents() {
        // the resume-mismatch guard is unreachable through `load_or_create` (the
        // file name is a stable hash over all ops, so a changed plan gets a fresh
        // file). craft the collision directly: write a journal whose CONTENTS are
        // `other` at the file name `expected` resolves to, then load it as
        // `expected` and confirm the guard rejects it.
        let dir = tempdir().unwrap();
        let expected = test_ops();
        let other = vec![Op::Create {
            uid: Uid::from_u128(42),
            type_name: TypeName::new("dcim.device"),
            desired: Object {
                uid: Uid::from_u128(42),
                type_name: TypeName::new("dcim.device"),
                key: Default::default(),
                attrs: Default::default(),
                source: None,
            },
        }];
        let target = Journal::stable_file_name(dir.path(), "test", &expected);
        let journal = Journal::new_ephemeral(&other);
        write_whole_file(&target, &journal.ops).unwrap();

        let err = Journal::new_from_existing_file(dir.path(), "test", &expected)
            .expect_err("mismatched journal must be rejected");
        assert!(
            err.to_string().contains("don't match the expected ops"),
            "got: {err}"
        );
    }

    #[test]
    fn mark_ops_as_done() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal.mark_op_as_done(&ops[0], None).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[1], None).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[2], None).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn mark_ops_as_done_backwards() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal.mark_op_as_done(&ops[2], None).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[1], None).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[0], None).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn records_the_backend_id_each_op_returned() {
        // the whole point of the journal for a resumed run: the ids survive the
        // crash, in plan order, so the next run can resolve refs into them.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
            journal
                .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                .unwrap();
            journal
                .mark_op_as_done(&ops[2], Some(&BackendId::String("abc".into())))
                .unwrap();
        }

        let journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
        let done = journal.done_applied_ops();
        assert_eq!(
            done.iter()
                .map(|a| (a.uid, a.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![
                (ops[0].uid(), Some(BackendId::Int(7))),
                (ops[2].uid(), Some(BackendId::String("abc".into()))),
            ]
        );
    }

    #[test]
    fn a_journal_without_recorded_ids_loads_with_none() {
        // marking done without an id is the emitter case; it must load, and report
        // no id rather than fail.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "test", &ops);
        {
            let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
            journal.mark_op_as_done(&ops[0], None).unwrap();
        }
        assert!(fs::read_to_string(&path)
            .unwrap()
            .contains(r#""backend_id":null"#));

        let journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
        let done = journal.done_applied_ops();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].backend_id, None);
    }

    #[test]
    fn mark_invalid_op_as_done() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal
            .mark_op_as_done(
                &Op::Create {
                    uid: Uid::from_u128(999),
                    type_name: TypeName::new("dcim.site"),
                    desired: Object {
                        uid: Uid::from_u128(999),
                        type_name: TypeName::new("dcim.site"),
                        key: Default::default(),
                        attrs: Default::default(),
                        source: None,
                    },
                },
                None,
            )
            .expect_err("should fail");
        assert!(!journal.is_completed());
    }

    #[test]
    fn mark_same_op_as_done_twice() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal.mark_op_as_done(&ops[1], None).unwrap();
        journal
            .mark_op_as_done(&ops[1], None)
            .expect_err("should fail");
    }

    #[test]
    fn mark_op_after_loading_partial_journal() {
        // mark one op, then reload: the rebuilt pending index must respect the
        // on-disk `done` records, so the already-done op can't be re-marked and the
        // remaining ops still mark and complete.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
            journal.mark_op_as_done(&ops[0], None).unwrap();
        }

        let mut journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
        // (a) a still-pending op marks after reload
        journal.mark_op_as_done(&ops[1], None).unwrap();
        assert!(!journal.is_completed());
        // (b) the op done before the reload is respected: re-marking errors
        let err = journal
            .mark_op_as_done(&ops[0], None)
            .expect_err("already-done op must not be markable again");
        assert!(
            err.to_string().contains("no matching op found"),
            "got: {err}"
        );
        // (c) completed only once the last remaining op is marked
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[2], None).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn mark_scales_to_a_large_plan() {
        // marking every op in a large plan in order completes without a quadratic
        // blowup (no wall-clock assert, just correctness at scale). ephemeral on
        // purpose: a backing file would make this 5000 fsyncs, measured at 25s here,
        // to guard the pending index rather than the io the smaller tests cover.
        const N: u128 = 5000;
        let ops: Vec<Op> = (0..N)
            .map(|i| Op::Create {
                uid: Uid::from_u128(i),
                type_name: TypeName::new("dcim.device"),
                desired: Object {
                    uid: Uid::from_u128(i),
                    type_name: TypeName::new("dcim.device"),
                    key: Default::default(),
                    attrs: Default::default(),
                    source: None,
                },
            })
            .collect();

        let mut journal = Journal::new_ephemeral(&ops);
        for op in &ops {
            journal.mark_op_as_done(op, None).unwrap();
        }
        assert!(journal.is_completed());
    }

    /// models a process that dies without unwinding: nothing after this point runs,
    /// so only what `mark_op_as_done` already wrote is on disk.
    fn killed(journal: Journal) {
        std::mem::forget(journal);
    }

    #[test]
    fn a_process_killed_mid_apply_keeps_the_ops_it_applied() {
        // the headline of #46. the journal used to be written at the apply's exit
        // points, so a run that never reached one left a journal recording nothing.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::load_or_create(dir.path(), "killed", &ops).unwrap();
            journal
                .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                .unwrap();
            journal
                .mark_op_as_done(&ops[1], Some(&BackendId::Int(8)))
                .unwrap();
            killed(journal);
        }

        let journal = Journal::load_or_create(dir.path(), "killed", &ops).unwrap();
        assert_eq!(
            journal
                .done_applied_ops()
                .iter()
                .map(|a| (a.uid, a.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![
                (ops[0].uid(), Some(BackendId::Int(7))),
                (ops[1].uid(), Some(BackendId::Int(8))),
            ]
        );
        assert!(!journal.is_completed());
    }

    #[test]
    fn a_torn_final_record_loads_as_everything_before_it() {
        // a power cut can leave the last append half-written. everything ahead of it
        // is intact by construction, and refusing to load would cost the whole resume.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "torn", &ops);
        {
            let mut journal = Journal::load_or_create(dir.path(), "torn", &ops).unwrap();
            journal
                .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                .unwrap();
            killed(journal);
        }
        let mut contents = fs::read_to_string(&path).unwrap();
        contents.push_str(r#"--- {"done":{"op":1,"backe"#);
        fs::write(&path, &contents).unwrap();

        let journal = Journal::load_or_create(dir.path(), "torn", &ops).unwrap();
        assert_eq!(
            journal
                .done_applied_ops()
                .iter()
                .map(|a| (a.uid, a.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![(ops[0].uid(), Some(BackendId::Int(7)))]
        );
    }

    #[test]
    fn a_resume_from_a_torn_journal_repairs_it_before_appending() {
        // tolerating the torn tail on read is only half of it. appending after the torn
        // bytes leaves a record the next load either drops, re-applying an op that
        // already ran, or cannot read at all, losing every id the killed run recorded.
        for tail in [
            r#"--- {"done":{"op":1,"backe"#,     // the newline never landed
            "--- {\"done\":{\"op\":1,\"backe\n", // it did, the rest of the body did not
        ] {
            let dir = tempdir().unwrap();
            let ops = test_ops();
            let path = Journal::stable_file_name(dir.path(), "torn", &ops);
            {
                let mut journal = Journal::load_or_create(dir.path(), "torn", &ops).unwrap();
                journal
                    .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                    .unwrap();
                killed(journal);
            }
            let mut contents = fs::read_to_string(&path).unwrap();
            contents.push_str(tail);
            fs::write(&path, &contents).unwrap();

            // the resumed run applies the op the torn record was for, and dies again
            {
                let mut journal = Journal::load_or_create(dir.path(), "torn", &ops).unwrap();
                journal
                    .mark_op_as_done(&ops[1], Some(&BackendId::Int(8)))
                    .unwrap();
                killed(journal);
            }
            // and once more, since a merged line only reads as corruption once it is no
            // longer the last one
            {
                let mut journal = Journal::load_or_create(dir.path(), "torn", &ops).unwrap();
                journal
                    .mark_op_as_done(&ops[2], Some(&BackendId::Int(9)))
                    .unwrap();
                killed(journal);
            }

            let journal = Journal::load_or_create(dir.path(), "torn", &ops).unwrap();
            assert_eq!(
                journal
                    .done_applied_ops()
                    .iter()
                    .map(|a| (a.uid, a.backend_id.clone()))
                    .collect::<Vec<_>>(),
                vec![
                    (ops[0].uid(), Some(BackendId::Int(7))),
                    (ops[1].uid(), Some(BackendId::Int(8))),
                    (ops[2].uid(), Some(BackendId::Int(9))),
                ],
                "torn with {tail:?}"
            );
            assert!(journal.is_completed());
        }
    }

    #[test]
    fn a_record_broken_before_the_last_one_is_corruption() {
        // only the tail can be torn, so a broken line anywhere else is a damaged file
        // and reading past it would report ops as pending that already applied.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "corrupt", &ops);
        {
            let mut journal = Journal::load_or_create(dir.path(), "corrupt", &ops).unwrap();
            journal.mark_op_as_done(&ops[0], None).unwrap();
            journal.mark_op_as_done(&ops[1], None).unwrap();
            killed(journal);
        }
        let contents = fs::read_to_string(&path).unwrap();
        let broken = contents.replacen(r#"--- {"op":"#, "--- {\"op\"", 1);
        fs::write(&path, broken).unwrap();

        let err = Journal::load_or_create(dir.path(), "corrupt", &ops)
            .expect_err("a broken record above the tail must not load");
        assert!(err.to_string().contains("failed to read"), "got: {err}");
    }

    #[test]
    fn a_journal_from_before_the_append_only_format_still_loads() {
        // the previous format was one mutable yaml document. it must resume rather
        // than be read as an empty append-only file, and it is rewritten on load so
        // every write after that is an append.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "test", &ops);
        let legacy = format!(
            "ops:\n\
             - op_uid: {}\n  op_typename: dcim.device\n  op_hash: {}\n  done: true\n  backend_id: 7\n\
             - op_uid: {}\n  op_typename: dcim.device\n  op_hash: {}\n  done: false\n  backend_id: null\n\
             - op_uid: {}\n  op_typename: dcim.site\n  op_hash: {}\n  done: false\n  backend_id: null\n",
            ops[0].uid(), ops[0].hashed(),
            ops[1].uid(), ops[1].hashed(),
            ops[2].uid(), ops[2].hashed(),
        );
        fs::write(&path, legacy).unwrap();

        let mut journal = Journal::load_or_create(dir.path(), "test", &ops).unwrap();
        let done = journal.done_applied_ops();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].backend_id, Some(BackendId::Int(7)));

        journal.mark_op_as_done(&ops[1], None).unwrap();
        drop(journal);
        let migrated = fs::read_to_string(&path).unwrap();
        assert!(migrated.starts_with(r#"--- {"version":1}"#), "{migrated}");
        assert_eq!(
            Journal::load_or_create(dir.path(), "test", &ops)
                .unwrap()
                .done_ops_count(),
            2
        );
    }

    #[test]
    fn a_journal_from_a_newer_alembic_is_refused_by_name() {
        // the alternative is reading records this version does not understand as if
        // it did, and skipping ops that never applied.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "test", &ops);
        fs::write(&path, "--- {\"version\":99}\n").unwrap();

        let err = Journal::load_or_create(dir.path(), "test", &ops)
            .expect_err("an unknown format version must not be read as this one");
        assert!(err.to_string().contains("format version 99"), "got: {err}");
    }

    #[test]
    fn delete_backing_file() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
        let file_path = Journal::stable_file_name(dir.path(), "test", &ops);
        assert!(file_path.exists());
        journal.delete_backing_file().unwrap();
        assert!(!file_path.exists());
    }

    /// identity is the uid alone, so a retype plans a create and (elsewhere) a
    /// delete under one uid; the journal keys ops by (uid, type, hash), so the
    /// new materialization's create is its own entry and marking it done never
    /// touches another op sharing the uid.
    #[test]
    fn ops_sharing_a_uid_across_types_journal_independently() {
        let dir = tempdir().unwrap();
        let make = |type_name: &str| Op::Create {
            uid: Uid::from_u128(9),
            type_name: TypeName::new(type_name),
            desired: Object {
                uid: Uid::from_u128(9),
                type_name: TypeName::new(type_name),
                key: Default::default(),
                attrs: Default::default(),
                source: None,
            },
        };
        let ops = vec![make("location.site"), make("net.zone")];
        let mut journal = Journal::load_or_create(dir.path(), "retype", &ops).unwrap();
        journal.mark_op_as_done(&ops[0], None).unwrap();
        assert_eq!(journal.done_ops_count(), 1);

        let reloaded = Journal::load_or_create(dir.path(), "retype", &ops).unwrap();
        let done = reloaded.done_ops();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].1, TypeName::new("location.site"));
        assert!(!reloaded.is_completed());
    }
}
