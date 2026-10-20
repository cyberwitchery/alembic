//! keep track of successfully applied ops to enable resume after an error.
//!
//! when resuming, the journal must match the previous run's non-delete op sequence (including op hashes).
//!
//! the file is append-only: a version line, one line per planned op, then one line
//! per op as it completes. each line is a yaml document in flow style, so a record
//! is a single `write` an interrupted process cannot leave half-applied to what came
//! before it. a run killed without unwinding therefore still leaves every op it
//! applied on disk.

use crate::types::{AppliedOp, BackendId, Op};
use alembic_core::{TypeName, Uid};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use thiserror::Error;

/// bumped when a record shape changes. an unknown version is refused by name
/// rather than read as this one.
const FORMAT_VERSION: u32 = 1;

/// every line carries it, so a stray line is never mistaken for a record.
const DOCUMENT_PREFIX: &str = "--- ";

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JournalError {
    #[error("failed to access the journal at `{}`", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to append to the journal at {}", path.display())]
    Append {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to read the journal file `{}`", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to encode a journal record")]
    Encode(#[source] serde_json::Error),
    #[error("the journal file `{}` has no version line", path.display())]
    MissingVersion { path: PathBuf },
    #[error(
        "the journal file `{}` is format version {found}, but this alembic writes version {FORMAT_VERSION}; remove it to start a fresh apply",
        path.display()
    )]
    UnsupportedVersion { path: PathBuf, found: u32 },
    #[error("failed to read the journal file `{}`: line {line} is not a journal record", path.display())]
    NotARecord { path: PathBuf, line: usize },
    #[error("failed to read the journal file `{}`: line {line}", path.display())]
    BadRecord {
        path: PathBuf,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("the journal file `{}` records op {op} as done, but only declares {declared}", path.display())]
    DoneOutOfRange {
        path: PathBuf,
        op: usize,
        declared: usize,
    },
    #[error("the ops in the loaded journal at `{location}` don't match the expected ops")]
    OpsMismatch { location: String },
    #[error("no matching op found in journal, can't mark any as done")]
    NotPlanned,
    #[error("the journal has not been loaded or written yet")]
    NotOpen,
    #[error("journal path `{}` has no parent directory", path.display())]
    NoParent { path: PathBuf },
    #[error("the journal at {location} is held by another apply of this plan; re-run once it is through")]
    Locked { location: String },
    #[error("failed to access the journal at {location}")]
    Backend {
        location: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

type Result<T, E = JournalError> = std::result::Result<T, E>;

fn io_at(path: &Path) -> impl FnOnce(io::Error) -> JournalError + '_ {
    move |source| JournalError::Io {
        path: path.to_owned(),
        source,
    }
}

#[derive(Debug)]
pub struct Journal {
    backend: Option<Box<dyn JournalBackend>>,
    ops: Vec<OpWithMeta>,
    // not-yet-done op positions, keyed by (uid, typename, hash), in plan order.
    // derived from `ops` and rebuilt whenever `ops` is (re)loaded; lets
    // `mark_op_as_done` be O(1) instead of a linear scan from the start.
    pending_index: HashMap<(Uid, TypeName, u64), VecDeque<usize>>,
}

#[async_trait]
pub trait JournalBackend: Send + Sync + Debug {
    /// records of an earlier run under `key`, if there is one.
    async fn load(&mut self, key: &str) -> Result<Option<Vec<OpWithMeta>>>;
    /// lay down a fresh (or rewritten) journal under `key`.
    async fn write_all(&mut self, key: &str, ops: &[OpWithMeta]) -> Result<()>;
    /// durably record op `index` as done before returning.
    async fn append_done(&mut self, index: usize, backend_id: Option<&BackendId>) -> Result<()>;
    async fn delete(self: Box<Self>) -> Result<()>;
    /// shown in the resume / unfinished-delete warnings.
    fn description(&self) -> String;
}

/// opens a fresh, not yet loaded backend for each journal.
pub trait JournalBackendFactory: Send + Sync + Debug {
    fn open(&self) -> Box<dyn JournalBackend>;
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

#[derive(Debug, Clone)]
pub enum JournalStorageLocation {
    None,
    File(PathBuf),
    Backend(Arc<dyn JournalBackendFactory>),
}

fn record_line<T: Serialize>(value: &T) -> Result<String> {
    Ok(format!(
        "{DOCUMENT_PREFIX}{}\n",
        serde_json::to_string(value).map_err(JournalError::Encode)?
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

fn op_keys(ops: &[OpWithMeta]) -> Vec<(Uid, &TypeName, u64)> {
    ops.iter()
        .map(|owm| (owm.op_uid, &owm.op_typename, owm.op_hash))
        .collect()
}

impl Journal {
    pub fn stable_file_name(directory: &Path, adapter_name: &str, ops: &[Op]) -> PathBuf {
        JournalFileBackend::file_path(directory, &Self::stable_key(adapter_name, ops))
    }

    /// names one plan's journal: the same scope and ops always yield the same key.
    fn stable_key(adapter_name: &str, ops: &[Op]) -> String {
        let hash = crate::types::stable_json_hash(&ops);
        format!("{}_journal_{}", adapter_name, hash)
    }

    /// like `load_or_create_with_file`, with the journal kept by `backend`.
    pub async fn load_or_create_with_backend(
        backend: Box<dyn JournalBackend>,
        adapter_name: &str,
        ops: &[Op],
    ) -> Result<Self> {
        Self::load_or_create(backend, &Self::stable_key(adapter_name, ops), ops).await
    }

    /// tries to load a Journal from its file in `directory`, otherwise creates a new one.
    /// in either case, the new Journal instance will be backed by that file.
    /// delete ops will not be saved in the journal.
    pub async fn load_or_create_with_file(
        directory: &Path,
        adapter_name: &str,
        ops: &[Op],
    ) -> Result<Self> {
        let backend = JournalFileBackend::new(directory.to_owned());
        Self::load_or_create(Box::new(backend), &Self::stable_key(adapter_name, ops), ops).await
    }

    /// resumes the journal `backend` holds under `key` if there is one, otherwise
    /// writes a fresh one for `ops`. delete ops will not be saved in the journal.
    pub async fn load_or_create(
        mut backend: Box<dyn JournalBackend>,
        key: &str,
        ops: &[Op],
    ) -> Result<Self> {
        let mut journal = Self::new_ephemeral(ops);
        match backend.load(key).await? {
            Some(loaded) => {
                if op_keys(&loaded) != op_keys(&journal.ops) {
                    return Err(JournalError::OpsMismatch {
                        location: backend.description(),
                    });
                }
                journal.pending_index = build_pending_index(&loaded);
                journal.ops = loaded;
            }
            // write the plan out before applying any op, both to check the storage works
            // and because a `done` record is only meaningful against the ops it indexes
            None => backend.write_all(key, &journal.ops).await?,
        }
        journal.backend = Some(backend);
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
            backend: None,
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

    /// returns a string describing where the journal storage is located (if any)
    pub fn journal_storage_description(&self) -> Option<String> {
        self.backend.as_ref().map(|storage| storage.description())
    }

    pub fn is_completed(&self) -> bool {
        self.ops.iter().all(|op| op.done)
    }

    /// marks the first not-done op matching `op`'s (uid, typename, hash) done and
    /// records the id the backend returned for it. the record reaches disk before
    /// this returns, so the next op is never applied against an unrecorded one.
    /// O(1) via `pending_index`; errors if there's no such op.
    pub async fn mark_op_as_done(&mut self, op: &Op, backend_id: Option<&BackendId>) -> Result<()> {
        let key = (op.uid(), op.type_name().clone(), op.hashed());
        let op_index = self
            .pending_index
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .ok_or(JournalError::NotPlanned)?;
        // the position came from the pending index, so it is in range and not yet done
        self.ops[op_index].done = true;
        self.ops[op_index].backend_id = backend_id.cloned();

        match &mut self.backend {
            Some(storage) => storage.append_done(op_index, backend_id).await,
            None => Ok(()),
        }
    }

    pub async fn delete_backing_storage(&mut self) -> Result<()> {
        if let Some(storage) = self.backend.take() {
            storage.delete().await
        } else {
            Ok(())
        }
    }
}

/// keeps each journal as an append-only file in `directory`, named by its key.
#[derive(Debug)]
pub struct JournalFileBackend {
    directory: PathBuf,
    // the file `load` or `write_all` opened for appending, and its path
    open: Option<(File, PathBuf)>,
}

impl JournalFileBackend {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            open: None,
        }
    }

    fn file_path(directory: &Path, key: &str) -> PathBuf {
        directory.join(format!("{key}.yaml"))
    }
}

#[async_trait]
impl JournalBackend for JournalFileBackend {
    async fn load(&mut self, key: &str) -> Result<Option<Vec<OpWithMeta>>> {
        let path = Self::file_path(&self.directory, key);
        // only a missing file is a fresh start: the caller writes a new journal over
        // whatever `None` stands for
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_at(&path)(err)),
        };

        let (ops, truncated) = parse_records(&contents, &path)?;

        // a journal whose torn tail the parse dropped cannot be appended to as it
        // stands. rewriting once, here, is what makes every write from now on an append.
        let file = match truncated {
            true => write_whole_file(&path, &ops)?,
            false => open_for_append(&path)?,
        };
        self.open = Some((file, path));
        Ok(Some(ops))
    }

    async fn write_all(&mut self, key: &str, ops: &[OpWithMeta]) -> Result<()> {
        // apply writes the journal before any state save, so `directory` (e.g. `.alembic/`)
        // may not exist yet on a fresh checkout.
        fs::create_dir_all(&self.directory).map_err(io_at(&self.directory))?;
        let path = Self::file_path(&self.directory, key);
        let file = write_whole_file(&path, ops)?;
        self.open = Some((file, path));
        Ok(())
    }

    async fn append_done(&mut self, op_index: usize, backend_id: Option<&BackendId>) -> Result<()> {
        let (file, path) = self.open.as_mut().ok_or(JournalError::NotOpen)?;
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
            .map_err(|source| JournalError::Append {
                path: path.clone(),
                source,
            })
    }

    async fn delete(self: Box<Self>) -> Result<()> {
        let Some((file, path)) = self.open else {
            return Ok(());
        };
        // closed before the remove, which windows needs
        drop(file);
        fs::remove_file(&path).map_err(io_at(&path))
    }

    fn description(&self) -> String {
        match &self.open {
            Some((_, path)) => path.display().to_string(),
            None => self.directory.display().to_string(),
        }
    }
}

/// writes the whole file and hands back an append handle. used to lay down a fresh
/// journal and to repair a torn one; the rename keeps a crash mid-write from
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

    let dir = path.parent().ok_or_else(|| JournalError::NoParent {
        path: path.to_owned(),
    })?;
    let mut temp_file = NamedTempFile::new_in(dir).map_err(io_at(dir))?;
    temp_file.write_all(body.as_bytes()).map_err(io_at(path))?;
    // fsync data + metadata before it can become visible
    temp_file.as_file().sync_all().map_err(io_at(path))?;
    temp_file.persist(path).map_err(|e| io_at(path)(e.error))?;
    File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(io_at(dir))?;

    open_for_append(path)
}

fn open_for_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(io_at(path))
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
        .ok_or_else(|| JournalError::MissingVersion {
            path: path.to_owned(),
        })?;
    let version: Version =
        serde_json::from_str(version_line).map_err(|source| JournalError::Json {
            path: path.to_owned(),
            source,
        })?;
    if version.version != FORMAT_VERSION {
        return Err(JournalError::UnsupportedVersion {
            path: path.to_owned(),
            found: version.version,
        });
    }

    let mut ops: Vec<OpWithMeta> = Vec::new();
    let last = lines.len() - 1;
    for (index, line) in lines.iter().enumerate().skip(1) {
        let record: Result<Record> = line
            .trim_end()
            .strip_prefix(DOCUMENT_PREFIX)
            .ok_or_else(|| JournalError::NotARecord {
                path: path.to_owned(),
                line: index + 1,
            })
            .and_then(|body| {
                serde_json::from_str(body).map_err(|source| JournalError::BadRecord {
                    path: path.to_owned(),
                    line: index + 1,
                    source,
                })
            });
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
            Err(err) => return Err(err),
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
                let op = ops
                    .get_mut(done.op)
                    .ok_or_else(|| JournalError::DoneOutOfRange {
                        path: path.to_owned(),
                        op: done.op,
                        declared,
                    })?;
                op.done = true;
                op.backend_id = done.backend_id;
            }
        }
    }

    Ok((ops, truncated))
}

// the op itself is not stored, only its identity (uid, typename, hash). on disk it
// is split in two: an `op` line at plan time, a `done` line when it completes.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OpWithMeta {
    pub op_uid: Uid,
    pub op_typename: TypeName,
    pub op_hash: u64,
    pub done: bool,
    // id the backend returned for this op, so a resumed run can resolve references
    // into what an earlier run created or updated. the journaling adapters return one
    // for both; `null` in a journal written before ids were recorded.
    pub backend_id: Option<BackendId>,
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

    #[tokio::test]
    async fn save_and_load_journal() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
                .await
                .unwrap();
            drop(journal);
        }
        {
            let journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
                .await
                .unwrap();
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

    #[tokio::test]
    async fn resume_rejects_mismatched_journal_contents() {
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

        let err = Journal::load_or_create_with_file(dir.path(), "test", &expected)
            .await
            .expect_err("mismatched journal must be rejected");
        assert!(
            err.to_string().contains("don't match the expected ops"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn mark_ops_as_done() {
        let ops = test_ops();
        for order in [[0, 1, 2], [2, 1, 0]] {
            let mut journal =
                Journal::load_or_create_with_file(tempdir().unwrap().path(), "test", &ops)
                    .await
                    .unwrap();
            for (marked, index) in order.into_iter().enumerate() {
                assert!(!journal.is_completed(), "{order:?} after {marked}");
                journal.mark_op_as_done(&ops[index], None).await.unwrap();
            }
            assert!(journal.is_completed(), "{order:?}");
        }
    }

    #[tokio::test]
    async fn records_the_backend_id_each_op_returned() {
        // the whole point of the journal for a resumed run: the ids survive the
        // crash, in plan order, so the next run can resolve refs into them.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
                .await
                .unwrap();
            journal
                .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                .await
                .unwrap();
            journal
                .mark_op_as_done(&ops[2], Some(&BackendId::String("abc".into())))
                .await
                .unwrap();
        }

        let journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn a_journal_without_recorded_ids_loads_with_none() {
        // marking done without an id is the emitter case; it must load, and report
        // no id rather than fail.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "test", &ops);
        {
            let mut journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
                .await
                .unwrap();
            journal.mark_op_as_done(&ops[0], None).await.unwrap();
        }
        assert!(fs::read_to_string(&path)
            .unwrap()
            .contains(r#""backend_id":null"#));

        let journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
            .await
            .unwrap();
        let done = journal.done_applied_ops();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].backend_id, None);
    }

    #[tokio::test]
    async fn mark_invalid_op_as_done() {
        let ops = test_ops();
        let mut journal =
            Journal::load_or_create_with_file(tempdir().unwrap().path(), "test", &ops)
                .await
                .unwrap();
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
            .await
            .expect_err("should fail");
        assert!(!journal.is_completed());
    }

    #[tokio::test]
    async fn mark_same_op_as_done_twice() {
        let ops = test_ops();
        let mut journal =
            Journal::load_or_create_with_file(tempdir().unwrap().path(), "test", &ops)
                .await
                .unwrap();
        journal.mark_op_as_done(&ops[1], None).await.unwrap();
        journal
            .mark_op_as_done(&ops[1], None)
            .await
            .expect_err("should fail");
    }

    #[tokio::test]
    async fn mark_op_after_loading_partial_journal() {
        // mark one op, then reload: the rebuilt pending index must respect the
        // on-disk `done` records, so the already-done op can't be re-marked and the
        // remaining ops still mark and complete.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
                .await
                .unwrap();
            journal.mark_op_as_done(&ops[0], None).await.unwrap();
        }

        let mut journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
            .await
            .unwrap();
        // (a) a still-pending op marks after reload
        journal.mark_op_as_done(&ops[1], None).await.unwrap();
        assert!(!journal.is_completed());
        // (b) the op done before the reload is respected: re-marking errors
        let err = journal
            .mark_op_as_done(&ops[0], None)
            .await
            .expect_err("already-done op must not be markable again");
        assert!(
            err.to_string().contains("no matching op found"),
            "got: {err}"
        );
        // (c) completed only once the last remaining op is marked
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[2], None).await.unwrap();
        assert!(journal.is_completed());
    }

    #[tokio::test]
    async fn mark_scales_to_a_large_plan() {
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
            journal.mark_op_as_done(op, None).await.unwrap();
        }
        assert!(journal.is_completed());
    }

    /// models a process that dies without unwinding: nothing after this point runs,
    /// so only what `mark_op_as_done` already wrote is on disk.
    fn killed(journal: Journal) {
        std::mem::forget(journal);
    }

    #[tokio::test]
    async fn a_process_killed_mid_apply_keeps_the_ops_it_applied() {
        // the headline of #46. the journal used to be written at the apply's exit
        // points, so a run that never reached one left a journal recording nothing.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::load_or_create_with_file(dir.path(), "killed", &ops)
                .await
                .unwrap();
            journal
                .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                .await
                .unwrap();
            journal
                .mark_op_as_done(&ops[1], Some(&BackendId::Int(8)))
                .await
                .unwrap();
            killed(journal);
        }

        let journal = Journal::load_or_create_with_file(dir.path(), "killed", &ops)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn a_torn_final_record_loads_as_everything_before_it() {
        // a power cut can leave the last append half-written. everything ahead of it
        // is intact by construction, and refusing to load would cost the whole resume.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "torn", &ops);
        {
            let mut journal = Journal::load_or_create_with_file(dir.path(), "torn", &ops)
                .await
                .unwrap();
            journal
                .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                .await
                .unwrap();
            killed(journal);
        }
        let mut contents = fs::read_to_string(&path).unwrap();
        contents.push_str(r#"--- {"done":{"op":1,"backe"#);
        fs::write(&path, &contents).unwrap();

        let journal = Journal::load_or_create_with_file(dir.path(), "torn", &ops)
            .await
            .unwrap();
        assert_eq!(
            journal
                .done_applied_ops()
                .iter()
                .map(|a| (a.uid, a.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![(ops[0].uid(), Some(BackendId::Int(7)))]
        );
    }

    #[tokio::test]
    async fn a_resume_from_a_torn_journal_repairs_it_before_appending() {
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
                let mut journal = Journal::load_or_create_with_file(dir.path(), "torn", &ops)
                    .await
                    .unwrap();
                journal
                    .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
                    .await
                    .unwrap();
                killed(journal);
            }
            let mut contents = fs::read_to_string(&path).unwrap();
            contents.push_str(tail);
            fs::write(&path, &contents).unwrap();

            // the resumed run applies the op the torn record was for, and dies again
            {
                let mut journal = Journal::load_or_create_with_file(dir.path(), "torn", &ops)
                    .await
                    .unwrap();
                journal
                    .mark_op_as_done(&ops[1], Some(&BackendId::Int(8)))
                    .await
                    .unwrap();
                killed(journal);
            }
            // and once more, since a merged line only reads as corruption once it is no
            // longer the last one
            {
                let mut journal = Journal::load_or_create_with_file(dir.path(), "torn", &ops)
                    .await
                    .unwrap();
                journal
                    .mark_op_as_done(&ops[2], Some(&BackendId::Int(9)))
                    .await
                    .unwrap();
                killed(journal);
            }

            let journal = Journal::load_or_create_with_file(dir.path(), "torn", &ops)
                .await
                .unwrap();
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

    #[tokio::test]
    async fn a_record_broken_before_the_last_one_is_corruption() {
        // only the tail can be torn, so a broken line anywhere else is a damaged file
        // and reading past it would report ops as pending that already applied.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "corrupt", &ops);
        {
            let mut journal = Journal::load_or_create_with_file(dir.path(), "corrupt", &ops)
                .await
                .unwrap();
            journal.mark_op_as_done(&ops[0], None).await.unwrap();
            journal.mark_op_as_done(&ops[1], None).await.unwrap();
            killed(journal);
        }
        let contents = fs::read_to_string(&path).unwrap();
        let broken = contents.replacen(r#"--- {"op":"#, "--- {\"op\"", 1);
        fs::write(&path, broken).unwrap();

        let err = Journal::load_or_create_with_file(dir.path(), "corrupt", &ops)
            .await
            .expect_err("a broken record above the tail must not load");
        assert!(err.to_string().contains("failed to read"), "got: {err}");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn a_journal_that_cannot_be_read_is_an_error_not_a_fresh_start() {
        // `None` makes the caller write a fresh journal over the file, so only a
        // journal that is not there at all may load as `None`
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().unwrap();
        let journals = dir.path().join("journals");
        fs::create_dir(&journals).unwrap();
        fs::write(journals.join("k.yaml"), "").unwrap();
        fs::set_permissions(&journals, fs::Permissions::from_mode(0o000)).unwrap();

        let loaded = JournalFileBackend::new(journals.clone()).load("k").await;
        fs::set_permissions(&journals, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            matches!(loaded, Err(JournalError::Io { .. })),
            "got: {loaded:?}"
        );
    }

    #[tokio::test]
    async fn a_journal_without_a_version_line_is_refused_and_left_alone() {
        // a journal without the version line is refused by name rather than resumed
        // as an empty journal, and the file is not touched
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "test", &ops);
        let legacy = "ops: []\n";
        fs::write(&path, legacy).unwrap();

        let err = Journal::load_or_create_with_file(dir.path(), "test", &ops)
            .await
            .expect_err("a journal without a version line must be refused");
        assert!(
            matches!(err, JournalError::MissingVersion { .. }),
            "got: {err}"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), legacy);
    }

    #[tokio::test]
    async fn a_journal_from_a_newer_alembic_is_refused_by_name() {
        // the alternative is reading records this version does not understand as if
        // it did, and skipping ops that never applied.
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let path = Journal::stable_file_name(dir.path(), "test", &ops);
        fs::write(&path, "--- {\"version\":99}\n").unwrap();

        let err = Journal::load_or_create_with_file(dir.path(), "test", &ops)
            .await
            .expect_err("an unknown format version must not be read as this one");
        assert!(err.to_string().contains("format version 99"), "got: {err}");
    }

    #[tokio::test]
    async fn delete_backing_file() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let mut journal = Journal::load_or_create_with_file(dir.path(), "test", &ops)
            .await
            .unwrap();
        let file_path = Journal::stable_file_name(dir.path(), "test", &ops);
        assert!(file_path.exists());
        journal.delete_backing_storage().await.unwrap();
        assert!(!file_path.exists());
    }

    /// identity is the uid alone, so a retype plans a create and (elsewhere) a
    /// delete under one uid; the journal keys ops by (uid, type, hash), so the
    /// new materialization's create is its own entry and marking it done never
    /// touches another op sharing the uid.
    #[tokio::test]
    async fn ops_sharing_a_uid_across_types_journal_independently() {
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
        let mut journal = Journal::load_or_create_with_file(dir.path(), "retype", &ops)
            .await
            .unwrap();
        journal.mark_op_as_done(&ops[0], None).await.unwrap();
        assert_eq!(journal.done_ops_count(), 1);

        let reloaded = Journal::load_or_create_with_file(dir.path(), "retype", &ops)
            .await
            .unwrap();
        let done = reloaded.done_ops();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].1, TypeName::new("location.site"));
        assert!(!reloaded.is_completed());
    }

    /// journals in a shared map, standing in for a database.
    #[derive(Debug, Default)]
    struct MemoryFactory(Arc<std::sync::Mutex<HashMap<String, Vec<OpWithMeta>>>>);

    #[derive(Debug)]
    struct MemoryBackend {
        store: Arc<std::sync::Mutex<HashMap<String, Vec<OpWithMeta>>>>,
        key: Option<String>,
    }

    impl JournalBackendFactory for MemoryFactory {
        fn open(&self) -> Box<dyn JournalBackend> {
            Box::new(MemoryBackend {
                store: Arc::clone(&self.0),
                key: None,
            })
        }
    }

    #[async_trait]
    impl JournalBackend for MemoryBackend {
        async fn load(&mut self, key: &str) -> Result<Option<Vec<OpWithMeta>>> {
            self.key = Some(key.to_owned());
            Ok(self.store.lock().unwrap().get(key).cloned())
        }

        async fn write_all(&mut self, key: &str, ops: &[OpWithMeta]) -> Result<()> {
            self.key = Some(key.to_owned());
            self.store
                .lock()
                .unwrap()
                .insert(key.to_owned(), ops.to_vec());
            Ok(())
        }

        async fn append_done(
            &mut self,
            index: usize,
            backend_id: Option<&BackendId>,
        ) -> Result<()> {
            let key = self.key.as_ref().ok_or(JournalError::NotOpen)?;
            let mut store = self.store.lock().unwrap();
            let op = &mut store.get_mut(key).ok_or(JournalError::NotOpen)?[index];
            op.done = true;
            op.backend_id = backend_id.cloned();
            Ok(())
        }

        async fn delete(self: Box<Self>) -> Result<()> {
            if let Some(key) = &self.key {
                self.store.lock().unwrap().remove(key);
            }
            Ok(())
        }

        fn description(&self) -> String {
            "memory".to_owned()
        }
    }

    #[tokio::test]
    async fn a_backend_journal_resumes_from_what_its_factory_kept() {
        let factory = MemoryFactory::default();
        let ops = test_ops();
        let mut journal = Journal::load_or_create_with_backend(factory.open(), "test", &ops)
            .await
            .unwrap();
        journal
            .mark_op_as_done(&ops[1], Some(&BackendId::Int(9)))
            .await
            .unwrap();

        let mut reloaded = Journal::load_or_create_with_backend(factory.open(), "test", &ops)
            .await
            .unwrap();
        assert_eq!(reloaded.done_ops_count(), 1);
        assert_eq!(
            reloaded.done_applied_ops()[0].backend_id,
            Some(BackendId::Int(9))
        );

        reloaded.delete_backing_storage().await.unwrap();
        assert!(factory.0.lock().unwrap().is_empty());
    }
}
