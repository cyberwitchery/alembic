//! keep track of successfully applied ops, to enable resume after error.

pub struct Journal;

impl Journal {
    pub fn new() -> Self {
        Self {}
    }
}
