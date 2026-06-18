//! Keep track of successfully applied ops to enable resume after an error.
//!
//! When resuming, the journal must match the previous run's non-delete op sequence by (uid, type_name).

use crate::{BackendId, Op};
use alembic_core::{TypeName, Uid};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::{File, OpenOptions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

#[derive(Debug, Serialize, Deserialize)]
pub struct Journal {
    #[serde(skip)]
    file: Option<(File, PathBuf)>,
    ops: Vec<OpWithMeta>,
}

impl Journal {
    pub fn stable_file_name(directory: &Path, adapter_name: &str, ops: &[Op]) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        ops.hash(&mut hasher);
        let hash = hasher.finish();
        let file_name: PathBuf = format!("{}_journal_{}.yaml", adapter_name, hash).into();
        directory.join(file_name)
    }

    /// tries to load a Journal from `file_path`, otherwise creates a new one.
    /// in either case, the new Journal instance will be backed by the file at `file_path`.
    /// delete ops will not be saved in the journal.
    pub fn load_or_create(directory: &Path, adapter_name: &str, ops: &[Op]) -> Result<Self> {
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

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .append(false)
            .open(&file_name)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let mut journal: Journal = serde_yaml::from_str(&contents)?;

        let journal_keys = journal
            .ops
            .iter()
            .map(|op_with_meta| (op_with_meta.op_uid, &op_with_meta.op_typename))
            .collect::<Vec<(Uid, &TypeName)>>();

        let expected_keys = expected_ops
            .iter()
            .filter(|op| !matches!(op, Op::Delete { .. }))
            .map(|op| (op.uid(), op.type_name()))
            .collect::<Vec<(Uid, &TypeName)>>();
        if journal_keys != expected_keys {
            return Err(anyhow!(
                "the ops in the loaded journal file `{}` doesn't match the expected ops",
                file_name.display()
            ));
        }

        journal.file = Some((file, file_name));
        Ok(journal)
    }

    /// creates a journal with a new backing file
    fn new_with_file(directory: &Path, adapter_name: &str, ops: &[Op]) -> Result<Self> {
        let file_name = Self::stable_file_name(directory, adapter_name, ops);
        let mut journal = Self::new_ephemeral(ops);

        // create and write to the file to check that it works before applying any ops
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .append(false)
            .open(&file_name)?;
        file.set_len(0)?;
        file.rewind()?;

        journal.file = Some((file, file_name));
        journal.save()?;

        Ok(journal)
    }

    /// creates a journal without a backing file set
    pub fn new_ephemeral(ops: &[Op]) -> Self {
        Self {
            file: None,
            ops: ops
                .iter()
                .filter(|op| !matches!(op, Op::Delete { .. }))
                .map(OpWithMeta::new)
                .collect(),
        }
    }

    pub fn done_ops(&self) -> Vec<(Uid, TypeName, u64)> {
        self.ops
            .iter()
            .filter(|owm| owm.done)
            .map(|owm| (owm.op_uid, owm.op_typename.clone(), owm.op_hash))
            .collect()
    }

    pub fn done_ops_count(&self) -> usize {
        self.ops.iter().filter(|op| op.done).count()
    }

    pub fn is_completed(&self) -> bool {
        self.ops.iter().all(|op| op.done)
    }

    /// will mark the first op that is not done (will fail if there's no such op).
    /// uses a linear search from the start.
    pub fn mark_op_as_done(&mut self, op: &Op) -> Result<()> {
        let op_hash = op.hashed();
        let op_uid = op.uid();
        let op_typename = op.type_name();

        let Some(op_index) = self.ops.iter().position(|op| {
            !op.done
                && op.op_hash == op_hash
                && op.op_uid == op_uid
                && &op.op_typename == op_typename
        }) else {
            return Err(anyhow!(
                "no matching op found in journal, can't mark any as done"
            ));
        };

        // index comes from call to `position` above, so it must be in range
        self.ops[op_index].done = true;

        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        let str = serde_yaml::to_string(self)?;

        let (_, path) = self
            .file
            .as_ref()
            .ok_or_else(|| anyhow!("can't save journal because it's missing a backing file"))?;

        let path = path.clone();
        let dir = path
            .parent()
            .ok_or_else(|| anyhow!("file path has no parent directory"))?;

        let mut temp_file = NamedTempFile::new_in(dir)?;
        temp_file.write_all(str.as_bytes())?;
        temp_file.as_file().sync_all()?; // fsync data + metadata before it can become visible
        temp_file.persist(&path)?;
        File::open(dir)?.sync_all()?;
        let new_file = OpenOptions::new().read(true).write(true).open(&path)?;
        self.file = Some((new_file, path));

        Ok(())
    }

    pub fn delete_backing_file(&mut self) -> Result<()> {
        if let Some((file, file_path)) = self.file.take() {
            drop(file);
            fs::remove_file(file_path)?;
        }
        Ok(())
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        if let Some((file, _)) = self.file.take() {
            let _ = file.sync_all();
        }
    }
}

// we're only storing the uid and typename for the Op to keep this struct small and readable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OpWithMeta {
    op_uid: Uid,
    op_typename: TypeName,
    op_hash: u64,
    done: bool,
    backend_id: Option<BackendId>, // FIXME: not sure if/when this is needed
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
    fn save_and_load_journal() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
            journal.save().unwrap();
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
    fn load_and_save_existing_journal() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        {
            let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
            journal.save().unwrap();
        }
        {
            let mut journal = Journal::new_from_existing_file(dir.path(), "test", &ops).unwrap();
            journal.save().unwrap();
            assert_eq!(journal.ops.len(), 3);
        }
    }

    #[test]
    fn mark_ops_as_done() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal.mark_op_as_done(&ops[0]).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[1]).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[2]).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn mark_ops_as_done_backwards() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal.mark_op_as_done(&ops[2]).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[1]).unwrap();
        assert!(!journal.is_completed());
        journal.mark_op_as_done(&ops[0]).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn mark_invalid_op_as_done() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal
            .mark_op_as_done(&Op::Create {
                uid: Uid::from_u128(999),
                type_name: TypeName::new("dcim.site"),
                desired: Object {
                    uid: Uid::from_u128(999),
                    type_name: TypeName::new("dcim.site"),
                    key: Default::default(),
                    attrs: Default::default(),
                    source: None,
                },
            })
            .expect_err("should fail");
        assert!(!journal.is_completed());
    }

    #[test]
    fn mark_same_op_as_done_twice() {
        let ops = test_ops();
        let mut journal = Journal::new_with_file(tempdir().unwrap().path(), "test", &ops).unwrap();
        journal.mark_op_as_done(&ops[1]).unwrap();
        journal.mark_op_as_done(&ops[1]).expect_err("should fail");
    }

    #[test]
    fn delete_backing_file() {
        let dir = tempdir().unwrap();
        let ops = test_ops();
        let mut journal = Journal::new_with_file(dir.path(), "test", &ops).unwrap();
        journal.save().unwrap();
        let file_path = Journal::stable_file_name(dir.path(), "test", &ops);
        assert!(file_path.exists());
        journal.delete_backing_file().unwrap();
        assert!(!file_path.exists());
    }
}
