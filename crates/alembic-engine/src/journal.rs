//! keep track of successfully applied ops, to enable resume after error.

use crate::{BackendId, Op};

#[derive(Debug)]
pub struct Journal {
    ops: Vec<OpWithMeta>,
}

impl Journal {
    pub fn new(ops: &[Op]) -> Self {
        Self {
            ops: ops.iter().map(|op| OpWithMeta::new(op.clone())).collect(),
        }
    }
}

#[derive(Debug)]
struct OpWithMeta {
    op: Op,
    completed: bool,
    backend_id: Option<BackendId>, // not sure if this should be optional
}

impl OpWithMeta {
    fn new(op: Op) -> Self {
        OpWithMeta {
            op,
            completed: false,
            backend_id: None,
        }
    }
}
