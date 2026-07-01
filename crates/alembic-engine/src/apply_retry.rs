use crate::journal::Journal;
use crate::{AdapterApplyError, AppliedOp, Op};
use alembic_core::Uid;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

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
        let done_ops = journal.done_ops();
        let done_ops_len = done_ops.len();

        let mut done = done_ops
            .into_iter()
            .collect::<std::collections::HashSet<_>>();

        if done.len() != done_ops_len {
            // the use of a hash set here is an optimization, but it rules out ops with
            // exactly the same uid, typename and hash.
            // if there's a need to support such a thing in the future, it can be done by
            // switching the container for `done` into a type that supports duplicates.
            return Err(anyhow!("journal contained duplicated ops (same uid, typename and hash) which is not supported"));
        }

        pending.retain(|op| !done.remove(&(op.uid(), op.type_name().clone(), op.hashed())));

        if !done.is_empty() {
            return Err(anyhow!(
                "journal contains done ops that are not present in the provided ops"
            ));
        }
    }

    while !pending.is_empty() {
        let current = std::mem::take(&mut pending);
        let applied_before = applied.len();

        for op in current {
            match driver.apply_non_delete(&op).await {
                Ok(applied_op) => {
                    // marked in memory only; the journal is flushed to disk at the exit
                    // points below, not once per op (per-op saving was a ~100x regression).
                    if let Some(journal) = journal.as_mut() {
                        journal.mark_op_as_done(&op)?;
                    }
                    applied.push(applied_op);
                }
                Err(err) if driver.is_retryable(&err) => pending.push(op),
                Err(err) => {
                    // a fatal error is a clean unwind: persist progress before surfacing it
                    // so the next run can resume from here. don't mask the original error if
                    // the save itself fails.
                    if let Some(journal) = journal.as_mut() {
                        if let Err(save_err) = journal.save() {
                            tracing::warn!(
                                error = %save_err,
                                "failed to persist journal after apply error"
                            );
                        }
                    }
                    return Err(err);
                }
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
        } else {
            // ops remain pending (stuck with no progress): persist so a re-run can resume
            journal.save()?;
        }
    }

    Ok(RetryApplyResult { applied, pending })
}

/// true when `err` is a retryable missing-ref apply error.
pub fn is_missing_ref_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<AdapterApplyError>()
        .is_some_and(|e| matches!(e, AdapterApplyError::MissingRef { .. }))
}

/// comma-joined referenced uids in `ops` that are absent from `resolved`.
pub fn describe_missing_refs<V>(ops: &[Op], resolved: &BTreeMap<Uid, V>) -> String {
    let mut missing = BTreeSet::new();
    for op in ops {
        if let Op::Create { desired, .. } | Op::Update { desired, .. } = op {
            for value in desired.attrs.values() {
                collect_missing_refs(value, resolved, &mut missing);
            }
        }
    }
    missing
        .into_iter()
        .map(|uid| uid.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn collect_missing_refs<V>(
    value: &Value,
    resolved: &BTreeMap<Uid, V>,
    missing: &mut BTreeSet<Uid>,
) {
    match value {
        Value::String(raw) => {
            if let Ok(uid) = Uid::parse_str(raw) {
                if !resolved.contains_key(&uid) {
                    missing.insert(uid);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_missing_refs(item, resolved, missing);
            }
        }
        Value::Object(map) => {
            for value in map.values() {
                collect_missing_refs(value, resolved, missing);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BackendId;
    use alembic_core::{JsonMap, Key, Object, TypeName, Uid};
    use anyhow::anyhow;
    use rand::rng;
    use rand::seq::SliceRandom;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn is_missing_ref_error_matches_only_missing_ref() {
        let err = anyhow::Error::from(AdapterApplyError::MissingRef {
            uid: Uid::from_u128(1),
        });
        assert!(is_missing_ref_error(&err));
        assert!(!is_missing_ref_error(&anyhow!("some other error")));
    }

    #[test]
    fn describe_missing_refs_reports_unresolved_nested_refs() {
        let present = Uid::from_u128(1);
        let missing = Uid::from_u128(2);

        let attrs = JsonMap::from(BTreeMap::from([
            ("resolved_ref".to_string(), json!(present.to_string())),
            (
                "nested".to_string(),
                json!({ "list": [missing.to_string()] }),
            ),
        ]));
        let op = Op::Create {
            uid: present,
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid: present,
                type_name: TypeName::new("test.item"),
                key: Key::default(),
                attrs,
                source: None,
            },
        };

        let mut resolved = BTreeMap::new();
        resolved.insert(present, BackendId::Int(1));

        let described = describe_missing_refs(&[op], &resolved);
        assert!(described.contains(&missing.to_string()));
        assert!(!described.contains(&present.to_string()));
    }

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

        // turn off crashing
        driver.countdown_to_crash = 99999;
        _ = apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
            .await
            .unwrap();
        assert_eq!(
            driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>(),
            vec![uid1, uid2]
        );
        assert!(journal.is_completed());
    }

    #[tokio::test]
    async fn resumes_from_disk_after_error() {
        let uid1 = Uid::from_u128(1);
        let uid2 = Uid::from_u128(2);
        let uid3 = Uid::from_u128(3);
        let ops = vec![create_op(uid1), create_op(uid2), create_op(uid3)];
        let dir = tempdir().unwrap();

        // first run crashes after applying the first op; the journal is dropped to
        // simulate the process exiting, so resume must rely on what was flushed to disk.
        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 2,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "resume_test", &ops).unwrap();
            apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
                .await
                .expect_err("should fail on the second op");
            assert_eq!(driver.applied_ops.len(), 1);
        }

        // second run reloads the journal from disk and applies only the remaining ops.
        {
            let mut driver = ErraticDriver {
                countdown_to_crash: 99999,
                applied_ops: vec![],
            };
            let mut journal = Journal::load_or_create(dir.path(), "resume_test", &ops).unwrap();
            let result = apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
                .await
                .unwrap();
            assert_eq!(
                driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>(),
                vec![uid2, uid3]
            );
            assert_eq!(result.applied.len(), 2);
            assert!(result.pending.is_empty());
            assert!(journal.is_completed());
        }
    }

    #[tokio::test]
    async fn erratic_driver_with_shuffled_ops() {
        let mut ops = Vec::new();
        for i in 1..10 {
            ops.push(create_op(Uid::from_u128(i)));
        }

        let mut rng = rng();
        ops.shuffle(&mut rng);

        let mut driver = ErraticDriver {
            countdown_to_crash: 5,
            applied_ops: vec![],
        };
        let dir = tempdir().unwrap();
        let mut journal = Journal::load_or_create(dir.path(), "erratic_driver", &ops).unwrap();

        apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
            .await
            .expect_err("should fail (on fifth op applied this run)");
        assert_eq!(driver.applied_ops.len(), 4);
        assert!(!journal.is_completed());

        ops.shuffle(&mut rng);

        // turn off crashing
        driver.countdown_to_crash = 99999;
        _ = apply_non_delete_with_retries(&ops, Some(&mut journal), &mut driver)
            .await
            .unwrap();

        let mut applied_uids = driver.applied_ops.iter().map(|a| a.uid).collect::<Vec<_>>();
        applied_uids.sort();
        let mut op_uids = ops.iter().map(|op| op.uid()).collect::<Vec<_>>();
        op_uids.sort();
        assert_eq!(applied_uids, op_uids,);
        assert!(journal.is_completed());
    }
}
