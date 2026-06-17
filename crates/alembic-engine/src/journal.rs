//! Keep track of successfully applied ops to enable resume after an error.
//!
//! When resuming, the journal must match the previous run's non-delete op sequence by (uid, type_name).

use crate::{BackendId, Op};
use alembic_core::{TypeName, Uid};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct Journal {
    #[serde(skip)]
    file: Option<(File, PathBuf)>,
    next_op_index: usize,
    ops: Vec<OpWithMeta>,
    completed: bool,
}

impl Journal {
    /// tries to load a Journal from `file_path`, otherwise creates a new one.
    /// in either case, the new Journal instance will be backed by the file at `file_path`.
    /// delete ops will not be saved in the journal.
    pub fn load_or_create(file_path: PathBuf, ops: &[Op]) -> Result<Self> {
        if fs::metadata(&file_path).is_ok() {
            Self::new_from_existing_file(file_path, ops)
        } else {
            Self::new_with_file(file_path, ops)
        }
    }

    /// loads a journal from the file with `file_path` and sets its backing file to that file
    fn new_from_existing_file(file_path: PathBuf, expected_ops: &[Op]) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .append(false)
            .open(&file_path)?;
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
            .map(|op| (op.uid(), op.type_name()))
            .collect::<Vec<(Uid, &TypeName)>>();

        if journal_keys != expected_keys {
            return Err(anyhow!(
                "the ops in the loaded journal file `{}` doesn't match the expected ops",
                file_path.display()
            ));
        }

        journal.file = Some((file, file_path));
        Ok(journal)
    }

    /// creates a journal with a new backing file
    fn new_with_file(file_path: PathBuf, ops: &[Op]) -> Result<Self> {
        let mut journal = Self::new_ephemeral(ops);

        // create and write to the file to check that it works before applying any ops
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .append(false)
            .open(&file_path)?;
        file.set_len(0)?;
        file.rewind()?;

        journal.file = Some((file, file_path));
        journal.save()?;

        Ok(journal)
    }

    /// creates a journal without a backing file (mainly usable for testing)
    pub fn new_ephemeral(ops: &[Op]) -> Self {
        Self {
            file: None,
            next_op_index: 0,
            ops: ops
                .iter()
                .filter(|op| !matches!(op, Op::Delete { .. }))
                .enumerate()
                .map(|(index, op)| OpWithMeta::new(index, op.clone()))
                .collect(),
            completed: ops.is_empty(),
        }
    }

    pub fn done_ops(&self) -> usize {
        self.next_op_index
    }

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    pub fn mark_next_op_as_done(&mut self, expected_uid: Uid) -> Result<()> {
        if self.completed {
            return Err(anyhow!(
                "can't mark next op as done when journal was already completed"
            ));
        };

        let Some(at_op) = self.ops.get_mut(self.next_op_index) else {
            return Err(anyhow!("corrupt journal (index out of bounds)"));
        };

        if at_op.op_uid != expected_uid {
            return Err(anyhow!(
                "op uid in journal ({}) doesn't match expected uid {}, trying to mark another op as done",
                at_op.op_uid,
                expected_uid,
            ));
        }

        at_op.done = true;
        self.next_op_index += 1;

        if self.next_op_index >= self.ops.len() {
            self.completed = true;
        }

        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        let str = serde_yaml::to_string(self)?;
        if let Some((file, _)) = self.file.as_mut() {
            file.rewind()?;
            file.set_len(0)?;
            file.write_all(str.as_bytes())?;
            file.sync_all()?;
            Ok(())
        } else {
            Err(anyhow!(
                "can't save journal because it's missing a backing file"
            ))
        }
    }

    pub fn delete_backing_file(&mut self) -> Result<()> {
        if let Some((_, file_path)) = self.file.take() {
            fs::remove_file(file_path)?;
        }
        Ok(())
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        if let Some((file, _)) = self.file.take() {
            file.sync_all().unwrap();
        }
    }
}

// we're only storing the uid and typename for the Op to keep this struct small and readable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OpWithMeta {
    op_uid: Uid,
    op_typename: TypeName,
    /// this is the index of the op in the journal (so sans deletes)
    index: usize,
    done: bool,
    backend_id: Option<BackendId>, // FIXME: not sure if/when this is needed
}

impl OpWithMeta {
    fn new(index: usize, op: Op) -> Self {
        OpWithMeta {
            op_uid: op.uid(),
            op_typename: op.type_name().clone(),
            index,
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
        ]
    }

    #[test]
    fn save_and_load_journal() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("temp_journal.yaml");
        let ops = test_ops();
        {
            let mut journal = Journal::new_with_file(file_path.clone(), &ops).unwrap();
            journal.save().unwrap();
        }
        {
            let journal = Journal::new_from_existing_file(file_path, &ops).unwrap();
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
        let file_path = dir.path().join("temp_journal.yaml");
        let ops = test_ops();
        {
            let mut journal = Journal::new_with_file(file_path.clone(), &ops).unwrap();
            journal.save().unwrap();
        }
        {
            let mut journal = Journal::new_from_existing_file(file_path, &ops).unwrap();
            journal.save().unwrap();
            assert_eq!(journal.ops.len(), 2);
        }
    }

    #[test]
    fn mark_ops_as_done() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("temp_journal.yaml");
        let ops = test_ops();
        let mut journal = Journal::new_with_file(file_path.clone(), &ops).unwrap();
        journal.mark_next_op_as_done(Uid::from_u128(1)).unwrap();
        assert!(!journal.is_completed());
        journal.mark_next_op_as_done(Uid::from_u128(2)).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn mark_invalid_op_as_done() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("temp_journal.yaml");
        let ops = test_ops();
        let mut journal = Journal::new_with_file(file_path.clone(), &ops).unwrap();
        journal
            .mark_next_op_as_done(Uid::from_u128(2))
            .expect_err("should fail");
        assert!(!journal.is_completed());
        journal.mark_next_op_as_done(Uid::from_u128(1)).unwrap();
        journal.mark_next_op_as_done(Uid::from_u128(2)).unwrap();
        assert!(journal.is_completed());
    }

    #[test]
    fn delete_backing_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("temp_journal.yaml");
        let ops = test_ops();
        let mut journal = Journal::new_with_file(file_path.clone(), &ops).unwrap();
        journal.save().unwrap();
        assert!(file_path.exists());
        journal.delete_backing_file().unwrap();
        assert!(!file_path.exists());
    }
}
