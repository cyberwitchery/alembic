use crate::{AppliedOp, Op};
use anyhow::Result;
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
    driver: &mut impl RetryApplyDriver,
) -> Result<RetryApplyResult> {
    let mut applied = Vec::new();
    let mut pending: Vec<Op> = ops
        .iter()
        .filter(|op| !matches!(op, Op::Delete { .. }))
        .cloned()
        .collect();

    while !pending.is_empty() {
        let current = std::mem::take(&mut pending);
        let current_len = current.len();

        for op in current {
            match driver.apply_non_delete(&op).await {
                Ok(applied_op) => applied.push(applied_op),
                Err(err) if driver.is_retryable(&err) => pending.push(op),
                Err(err) => return Err(err),
            }
        }

        if pending.len() == current_len {
            break;
        }
    }

    Ok(RetryApplyResult { applied, pending })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BackendId, ProjectedObject, ProjectionData};
    use alembic_core::{JsonMap, Key, Object, TypeName, Uid};
    use anyhow::anyhow;
    use std::collections::BTreeSet;

    fn create_op(uid: Uid) -> Op {
        Op::Create {
            uid,
            type_name: TypeName::new("test.item"),
            desired: ProjectedObject {
                base: Object {
                    uid,
                    type_name: TypeName::new("test.item"),
                    key: Key::default(),
                    attrs: JsonMap::default(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
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

        let result = apply_non_delete_with_retries(&ops, &mut driver)
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

        let result = apply_non_delete_with_retries(&ops, &mut driver)
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

        let err = apply_non_delete_with_retries(&ops, &mut driver)
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

        let result = apply_non_delete_with_retries(&ops, &mut driver)
            .await
            .unwrap();

        assert_eq!(driver.attempts, 0);
        assert!(result.pending.is_empty());
        assert!(result.applied.is_empty());
    }
}
