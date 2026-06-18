use crate::journal::Journal;
use crate::{AppliedOp, Op};
use anyhow::{anyhow, Result};
use async_trait::async_trait;

#[derive(Debug)]
pub struct RetryApplyResult {
    pub applied: Vec<AppliedOp>,
    pub pending: Vec<Op>,
}

#[async_trait]
pub trait RetryApplyDriver {
    async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp>;
    fn is_retryable(&self, err: &anyhow::Error) -> bool;
}

pub async fn apply_non_delete_with_retries(
    ops: &[Op],
    mut journal: Option<&mut Journal>,
    driver: &mut impl RetryApplyDriver,
) -> Result<RetryApplyResult> {
    let mut applied = Vec::new();
    let mut pending: Vec<Op> = ops
        .iter()
        .filter(|op| !matches!(op, Op::Delete { .. }))
        .cloned()
        .collect();

    if let Some(journal) = journal.as_mut() {
        let done = journal.done_ops();
        if done > pending.len() {
            return Err(anyhow!(
                "corrupt journal (done ops {} exceeds pending ops {})",
                done,
                pending.len()
            ));
        }
        pending.drain(0..done);
    }

    while !pending.is_empty() {
        let current = std::mem::take(&mut pending);
        let applied_before = applied.len();

        for op in current {
            match driver.apply_non_delete(&op).await {
                Ok(applied_op) => {
                    if let Some(journal) = journal.as_mut() {
                        journal.mark_next_op_as_done(applied_op.uid)?;
                        journal.save()?;
                    }
                    applied.push(applied_op);
                }
                Err(err) if driver.is_retryable(&err) => pending.push(op),
                Err(err) => return Err(err),
            }
        }

        // only break if no progress was made (no items applied in this iteration)
        if applied.len() == applied_before {
            break;
        }
    }

    if let Some(journal) = journal.as_mut() {
        if journal.is_completed() {
            journal.delete_backing_file()?;
        }
    }

    Ok(RetryApplyResult { applied, pending })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BackendId;
    use alembic_core::{JsonMap, Key, Object, TypeName, Uid};
    use anyhow::anyhow;
    use tempfile::tempdir;

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
                Mode::Fatal => Err(anyhow!("boom")),
                Mode::RetryThenOk => Ok(AppliedOp {
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

        let result = apply_non_delete_with_retries(&ops, None, &mut driver)
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

        let result = apply_non_delete_with_retries(&ops, None, &mut driver)
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

        let result = apply_non_delete_with_retries(&ops, None, &mut driver)
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
    #[tokio::test]
    async fn erratic_driver_first_fails_then_succeeds() {
        let uid1 = Uid::from_u128(1);
        let uid2 = Uid::from_u128(2);
        let ops = vec![create_op(uid1), create_op(uid2)];
        let mut driver = ErraticDriver {
            countdown_to_crash: 2,
            applied_ops: vec![],
        };
        let dir = tempdir().unwrap();
        let mut journal = Journal::load_or_create(dir.path(), "erratic_driver", &ops).unwrap();

        apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
            .await
            .expect_err("should fail (on second op applied this run)");
        assert_eq!(driver.applied_ops.len(), 1);
        assert!(!journal.is_completed());

        driver.countdown_to_crash = 100;
        _ = apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
            .await
            .unwrap();
        assert_eq!(
            driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>(),
            vec![uid1, uid2]
        );
        assert!(journal.is_completed());
    }
}
