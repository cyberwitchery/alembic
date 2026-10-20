//! apply journals kept in postgres, next to the state they belong to: one row per
//! planned op, marked done in place as each one lands.

use crate::state::{connect_postgres, create_table_if_missing, PostgresTlsMode};
use alembic_adapter_sdk::journal::{
    JournalBackend, JournalBackendFactory, JournalError, OpWithMeta,
};
use alembic_adapter_sdk::BackendId;
use alembic_core::{TypeName, Uid};
use async_trait::async_trait;
use std::fmt;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Statement};

type Result<T, E = JournalError> = std::result::Result<T, E>;

const MARK_DONE: &str = "UPDATE alembic_journal \
     SET done = TRUE, backend_id = CAST($3 AS TEXT)::jsonb, updated_at = NOW() \
     WHERE journal_key = $1 AND idx = $2";

/// opens a `PostgresJournalBackend` per journal; its keys are namespaced by `state_key`.
pub(crate) struct PostgresJournalFactory {
    url: String,
    state_key: String,
    tls_mode: PostgresTlsMode,
}

impl PostgresJournalFactory {
    pub(crate) fn new(
        url: impl Into<String>,
        state_key: impl Into<String>,
        tls_mode: PostgresTlsMode,
    ) -> Self {
        Self {
            url: url.into(),
            state_key: state_key.into(),
            tls_mode,
        }
    }
}

// the url may carry credentials, so it is left out
impl fmt::Debug for PostgresJournalFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresJournalFactory")
            .field("state_key", &self.state_key)
            .field("tls_mode", &self.tls_mode)
            .finish_non_exhaustive()
    }
}

impl JournalBackendFactory for PostgresJournalFactory {
    fn open(&self) -> Box<dyn JournalBackend> {
        Box::new(PostgresJournalBackend {
            url: self.url.clone(),
            state_key: self.state_key.clone(),
            tls_mode: self.tls_mode,
            open: None,
        })
    }
}

pub(crate) struct PostgresJournalBackend {
    url: String,
    state_key: String,
    tls_mode: PostgresTlsMode,
    // the connection `load` or `write_all` opened
    open: Option<Opened>,
}

struct Opened {
    client: Client,
    journal_key: String,
    // prepared once, so marking an op done is a single round trip
    mark_done: Statement,
}

fn describe(journal_key: Option<&str>) -> String {
    match journal_key {
        Some(journal_key) => format!("postgres table alembic_journal, key `{journal_key}`"),
        None => "postgres table alembic_journal".to_owned(),
    }
}

impl fmt::Debug for PostgresJournalBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresJournalBackend")
            .field("description", &self.description())
            .finish_non_exhaustive()
    }
}

impl PostgresJournalBackend {
    fn backend_error(
        &self,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> JournalError {
        JournalError::Backend {
            location: self.description(),
            source: source.into(),
        }
    }

    /// connect (once), lock `key` for as long as the connection lives, and remember it
    /// as the journal this backend holds.
    async fn open(&mut self, key: &str) -> Result<()> {
        let journal_key = format!("{}/{key}", self.state_key);
        let opened = match self.open.take() {
            Some(opened) => Opened {
                journal_key,
                ..opened
            },
            None => self.connect(journal_key).await?,
        };
        self.open = Some(opened);
        Ok(())
    }

    async fn connect(&self, journal_key: String) -> Result<Opened> {
        let mut client = connect_postgres(&self.url, self.tls_mode)
            .await
            .map_err(|err| self.backend_error(err))?;
        create_table_if_missing(
            &client,
            "CREATE TABLE IF NOT EXISTS alembic_journal (\
            journal_key TEXT NOT NULL, \
            idx INTEGER NOT NULL, \
            op_uid TEXT NOT NULL, \
            op_typename TEXT NOT NULL, \
            op_hash BIGINT NOT NULL, \
            done BOOLEAN NOT NULL DEFAULT FALSE, \
            backend_id JSONB, \
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
            PRIMARY KEY (journal_key, idx)\
            )",
        )
        .await
        .map_err(|err| self.backend_error(err))?;
        if !try_lock(&mut client, &journal_key)
            .await
            .map_err(|err| self.backend_error(err))?
        {
            return Err(JournalError::Locked {
                location: describe(Some(&journal_key)),
            });
        }
        let mark_done = client
            .prepare(MARK_DONE)
            .await
            .map_err(|err| self.backend_error(err))?;
        Ok(Opened {
            client,
            journal_key,
            mark_done,
        })
    }

    fn opened(&self) -> Result<&Opened> {
        self.open.as_ref().ok_or(JournalError::NotOpen)
    }
}

/// a session advisory lock on `journal_key`: two workers never apply the same plan at
/// once, and a worker that dies releases it with its connection. the wait covers a
/// previous run whose connection is still closing; false when it runs out.
async fn try_lock(client: &mut Client, journal_key: &str) -> Result<bool, tokio_postgres::Error> {
    let transaction = client.transaction().await?;
    transaction
        .batch_execute(&format!("SET LOCAL lock_timeout = '{LOCK_WAIT}'"))
        .await?;
    let locked = transaction
        .execute(
            "SELECT pg_advisory_lock(hashtextextended($1, 0))",
            &[&journal_key],
        )
        .await;
    match locked {
        // a session lock outlives the transaction that took it
        Ok(_) => transaction.commit().await.map(|()| true),
        Err(err) if err.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => Ok(false),
        Err(err) => Err(err),
    }
}

/// how long to wait for another connection's lock on the same journal key.
const LOCK_WAIT: &str = "5s";

async fn unlock(client: &Client, journal_key: &str) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
            &[&journal_key],
        )
        .await
        .map(drop)
}

// postgres has no unsigned 64-bit integer; the bits round-trip through BIGINT
fn hash_to_db(hash: u64) -> i64 {
    hash as i64
}

fn hash_from_db(hash: i64) -> u64 {
    hash as u64
}

#[async_trait]
impl JournalBackend for PostgresJournalBackend {
    async fn load(&mut self, key: &str) -> Result<Option<Vec<OpWithMeta>>> {
        self.open(key).await?;
        let opened = self.opened()?;
        let rows = opened
            .client
            .query(
                "SELECT op_uid, op_typename, op_hash, done, backend_id::text \
                 FROM alembic_journal WHERE journal_key = $1 ORDER BY idx",
                &[&opened.journal_key],
            )
            .await
            .map_err(|err| self.backend_error(err))?;
        if rows.is_empty() {
            return Ok(None);
        }
        rows.iter()
            .map(|row| {
                let op_uid: String = row.try_get(0)?;
                let op_typename: String = row.try_get(1)?;
                let op_hash: i64 = row.try_get(2)?;
                let done: bool = row.try_get(3)?;
                let backend_id: Option<String> = row.try_get(4)?;
                let backend_id = backend_id
                    .map(|raw| serde_json::from_str::<BackendId>(&raw))
                    .transpose()?;
                Ok(OpWithMeta {
                    op_uid: Uid::parse_str(&op_uid)?,
                    op_typename: TypeName::new(op_typename),
                    op_hash: hash_from_db(op_hash),
                    done,
                    backend_id,
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()
            .map(Some)
            .map_err(|err| self.backend_error(err))
    }

    async fn write_all(&mut self, key: &str, ops: &[OpWithMeta]) -> Result<()> {
        self.open(key).await?;
        let mut idxs = Vec::with_capacity(ops.len());
        let mut op_uids = Vec::with_capacity(ops.len());
        let mut op_typenames = Vec::with_capacity(ops.len());
        let mut op_hashes = Vec::with_capacity(ops.len());
        let mut dones = Vec::with_capacity(ops.len());
        let mut backend_ids = Vec::with_capacity(ops.len());
        for (index, op) in ops.iter().enumerate() {
            idxs.push(i32::try_from(index).map_err(|err| self.backend_error(err))?);
            op_uids.push(op.op_uid.to_string());
            op_typenames.push(op.op_typename.as_str());
            op_hashes.push(hash_to_db(op.op_hash));
            dones.push(op.done);
            backend_ids.push(
                op.backend_id
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(JournalError::Encode)?,
            );
        }

        let description = self.description();
        let to_error = |err: tokio_postgres::Error| JournalError::Backend {
            location: description.clone(),
            source: err.into(),
        };
        let Some(Opened {
            client,
            journal_key,
            ..
        }) = self.open.as_mut()
        else {
            return Err(JournalError::NotOpen);
        };
        let transaction = client.transaction().await.map_err(to_error)?;
        transaction
            .execute(
                "DELETE FROM alembic_journal WHERE journal_key = $1",
                &[&*journal_key],
            )
            .await
            .map_err(to_error)?;
        // one statement for the whole plan, rather than a round trip per op
        transaction
            .execute(
                "INSERT INTO alembic_journal \
                 (journal_key, idx, op_uid, op_typename, op_hash, done, backend_id) \
                 SELECT $1, idx, op_uid, op_typename, op_hash, done, backend_id::jsonb \
                 FROM unnest($2::int4[], $3::text[], $4::text[], $5::int8[], $6::bool[], $7::text[]) \
                 AS t(idx, op_uid, op_typename, op_hash, done, backend_id)",
                &[
                    &*journal_key,
                    &idxs,
                    &op_uids,
                    &op_typenames,
                    &op_hashes,
                    &dones,
                    &backend_ids,
                ],
            )
            .await
            .map_err(to_error)?;
        transaction.commit().await.map_err(to_error)
    }

    async fn append_done(&mut self, index: usize, backend_id: Option<&BackendId>) -> Result<()> {
        let opened = self.opened()?;
        let idx = i32::try_from(index).map_err(|err| self.backend_error(err))?;
        let backend_id = backend_id
            .map(serde_json::to_string)
            .transpose()
            .map_err(JournalError::Encode)?;
        let updated = opened
            .client
            .execute(&opened.mark_done, &[&opened.journal_key, &idx, &backend_id])
            .await
            .map_err(|err| self.backend_error(err))?;
        if updated == 0 {
            return Err(self.backend_error(format!("op {index} is not in the journal")));
        }
        Ok(())
    }

    async fn delete(self: Box<Self>) -> Result<()> {
        let Some(opened) = &self.open else {
            return Ok(());
        };
        opened
            .client
            .execute(
                "DELETE FROM alembic_journal WHERE journal_key = $1",
                &[&opened.journal_key],
            )
            .await
            .map_err(|err| self.backend_error(err))?;
        // released here rather than when the connection closes, so a run straight
        // after this one does not wait on it
        unlock(&opened.client, &opened.journal_key)
            .await
            .map_err(|err| self.backend_error(err))
    }

    fn description(&self) -> String {
        describe(self.open.as_ref().map(|opened| opened.journal_key.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_adapter_sdk::journal::Journal;
    use alembic_adapter_sdk::Op;
    use alembic_core::Object;
    use uuid::Uuid;

    fn create_op(n: u128) -> Op {
        Op::Create {
            uid: Uid::from_u128(n),
            type_name: TypeName::new("test.item"),
            desired: Object {
                uid: Uid::from_u128(n),
                type_name: TypeName::new("test.item"),
                key: Default::default(),
                attrs: Default::default(),
                source: None,
            },
        }
    }

    /// a factory under a fresh state key, when a test database is configured.
    fn test_factory() -> Option<(String, PostgresJournalFactory)> {
        let url = std::env::var("ALEMBIC_TEST_POSTGRES_URL").ok()?;
        let state_key = format!("alembic-test-{}", Uuid::new_v4());
        Some((
            url.clone(),
            PostgresJournalFactory::new(url, state_key, PostgresTlsMode::Disable),
        ))
    }

    #[test]
    fn op_hashes_above_i64_max_round_trip() {
        for hash in [0, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(hash_from_db(hash_to_db(hash)), hash);
        }
    }

    #[tokio::test]
    async fn postgres_journal_resumes_done_ops_when_configured() {
        let Some((_, factory)) = test_factory() else {
            return;
        };
        let ops = vec![create_op(1), create_op(2)];
        let mut journal = Journal::load_or_create(factory.open(), "k", &ops)
            .await
            .unwrap();
        journal
            .mark_op_as_done(&ops[0], Some(&BackendId::Int(7)))
            .await
            .unwrap();
        // the interrupted run's connection closes, releasing its lock
        drop(journal);

        let reloaded = Journal::load_or_create(factory.open(), "k", &ops)
            .await
            .unwrap();
        assert_eq!(reloaded.op_count(), 2);
        assert_eq!(
            reloaded
                .done_applied_ops()
                .iter()
                .map(|op| (op.uid, op.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![(Uid::from_u128(1), Some(BackendId::Int(7)))]
        );
    }

    #[tokio::test]
    async fn postgres_journal_refuses_other_ops_under_its_key_when_configured() {
        let Some((_, factory)) = test_factory() else {
            return;
        };
        Journal::load_or_create(factory.open(), "k", &[create_op(1)])
            .await
            .unwrap();

        let err = Journal::load_or_create(factory.open(), "k", &[create_op(2)])
            .await
            .unwrap_err();
        let JournalError::OpsMismatch { location } = err else {
            panic!("expected OpsMismatch, got {err:?}");
        };
        assert!(location.contains("alembic_journal"), "got: {location}");
    }

    #[tokio::test]
    async fn postgres_journal_refuses_a_second_apply_of_the_same_plan_when_configured() {
        let Some((_, factory)) = test_factory() else {
            return;
        };
        let ops = vec![create_op(1)];
        let _held = Journal::load_or_create(factory.open(), "k", &ops)
            .await
            .unwrap();

        let err = Journal::load_or_create(factory.open(), "k", &ops)
            .await
            .unwrap_err();
        let JournalError::Locked { location } = err else {
            panic!("expected Locked, got {err:?}");
        };
        assert!(location.contains("/k`"), "got: {location}");
    }

    #[tokio::test]
    async fn postgres_journal_delete_removes_its_rows_when_configured() {
        let Some((_, factory)) = test_factory() else {
            return;
        };
        let ops = vec![create_op(1)];
        let mut journal = Journal::load_or_create(factory.open(), "k", &ops)
            .await
            .unwrap();
        journal.delete_backing_storage().await.unwrap();

        assert_eq!(factory.open().load("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn postgres_journals_under_different_state_keys_are_apart_when_configured() {
        let (Some((_, first)), Some((_, second))) = (test_factory(), test_factory()) else {
            return;
        };
        let ops = vec![create_op(1)];
        let mut journal = Journal::load_or_create(first.open(), "k", &ops)
            .await
            .unwrap();
        journal.mark_op_as_done(&ops[0], None).await.unwrap();

        let other = Journal::load_or_create(second.open(), "k", &ops)
            .await
            .unwrap();
        assert_eq!(other.done_ops_count(), 0);
    }

    #[tokio::test]
    async fn postgres_journal_description_leaves_out_the_url_when_configured() {
        let Some((url, factory)) = test_factory() else {
            return;
        };
        let journal = Journal::load_or_create(factory.open(), "k", &[create_op(1)])
            .await
            .unwrap();
        let description = journal.journal_storage_description().unwrap();
        assert!(!description.contains(&url), "got: {description}");
        assert!(description.contains("/k`"), "got: {description}");
        assert!(!format!("{factory:?}").contains(&url));
    }
}
