//! keep track of successfully applied ops, to enable resume after error.
//!
//! a journal has to match the exact ops in the previous run, when attempting to resume.

use crate::{BackendId, Op};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct Journal {
    #[serde(skip)]
    file: Option<File>,
    next_op: usize,
    ops: Vec<OpWithMeta>,
    completed: bool,
}

impl Journal {
    /// creates a journal with a backing file
    pub fn new_with_file(filename: PathBuf, ops: &[Op]) -> Result<Self> {
        let mut journal = Self::new(ops);

        // create and write to the file to check that it works before applying any ops
        let mut file = std::fs::File::create(&filename)?;
        file.write(serde_yaml::to_string(&journal)?.as_bytes())?;
        journal.file = Some(file);

        Ok(journal)
    }

    /// loads a journal from the file with `filename` and sets it backing file to that file
    pub fn new_from_file(filename: PathBuf) -> Result<Self> {
        let mut file = std::fs::File::open(&filename)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut journal: Journal = serde_yaml::from_str(&contents)?;
        journal.file = Some(file);
        Ok(journal)
    }

    /// creates a journal without a backing file (mainly usable for testing)
    fn new(ops: &[Op]) -> Self {
        Self {
            file: None,
            next_op: 0,
            ops: ops.iter().map(|op| OpWithMeta::new(op.clone())).collect(),
            completed: ops.is_empty(),
        }
    }

    pub fn done_ops(&self) -> usize {
        self.next_op
    }

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// in addition to marking the op as done, also writes the journal to disk if it has a backing file
    pub fn mark_next_op_as_done(&mut self, op: &Op) -> Result<()> {
        if self.completed {
            return Err(anyhow!(
                "can't mark next op as done when journal was already completed"
            ));
        };

        let Some(at_op) = self.ops.get_mut(self.next_op) else {
            return Err(anyhow!("corrupt journal (next_op was out of bounds)"));
        };

        if at_op.op != *op {
            return Err(anyhow!(
                "ops don't match, trying to mark another op as done"
            ));
        }

        at_op.done = true;
        self.attempt_save()?;

        Ok(())
    }

    fn attempt_save(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.sync_all()?;
        }
        Ok(())
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
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
