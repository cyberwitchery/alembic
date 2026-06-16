//! keep track of successfully applied ops, to enable resume after error.
//!
//! a journal has to match the exact ops in the previous run, when attempting to resume.

use crate::{BackendId, Op};
use alembic_core::Uid;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
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
    pub fn load_or_create(file_path: PathBuf, ops: &[Op]) -> Result<Self> {
        if fs::metadata(&file_path).is_ok() {
            Self::new_from_file(file_path, ops)
        } else {
            Self::new_with_file(file_path, ops)
        }
    }

    /// loads a journal from the file with `file_path` and sets it backing file to that file
    fn new_from_file(file_path: PathBuf, expected_ops: &[Op]) -> Result<Self> {
        let mut file = File::open(&file_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut journal: Journal = serde_yaml::from_str(&contents)?;

        let loaded_raw_ops = journal
            .ops
            .iter()
            .map(|op_with_meta| op_with_meta.op.clone())
            .collect::<Vec<Op>>();

        if loaded_raw_ops != expected_ops {
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
        let mut file = std::fs::File::create(&file_path)?;
        file.write(serde_yaml::to_string(&journal)?.as_bytes())?;
        journal.file = Some((file, file_path));

        Ok(journal)
    }

    /// creates a journal without a backing file (mainly usable for testing)
    pub fn new_ephemeral(ops: &[Op]) -> Self {
        Self {
            file: None,
            next_op_index: 0,
            ops: ops.iter().map(|op| OpWithMeta::new(op.clone())).collect(),
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

        if at_op.op.uid() != expected_uid {
            return Err(anyhow!(
                "op uid in journal ({}) doesn't match expected uid {}, trying to mark another op as done",
                at_op.op.uid(),
                expected_uid,
            ));
        }

        at_op.done = true;
        self.next_op_index += 1;

        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        if let Some((file, _)) = self.file.as_mut() {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OpWithMeta {
    op: Op,
    done: bool,
    backend_id: Option<BackendId>, // not sure if this should be optional
}

impl OpWithMeta {
    fn new(op: Op) -> Self {
        OpWithMeta {
            op,
            done: false,
            backend_id: None,
        }
    }
}
