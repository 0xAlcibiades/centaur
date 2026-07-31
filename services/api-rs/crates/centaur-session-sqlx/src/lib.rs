//! SQLx-backed session repository.

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use centaur_session_core::{
    ExecutionStatus, HarnessType, MessageRole, SandboxCapabilities, SandboxRepoCacheAccess,
    Session, SessionEvent, SessionExecution, SessionMessage, SessionMessageInput, SessionStatus,
    ThreadKey, empty_object,
};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{
    FromRow, PgPool, Transaction,
    postgres::{PgListener, PgPoolOptions},
    types::Json,
};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use uuid::Uuid;

// The API binary embeds these migrations at compile time.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const SESSION_EVENTS_CHANNEL: &str = "centaur_session_events";
const DEFAULT_MAX_CONNECTIONS: u32 = 500;
const METADATA_TRACE_LOCK_TIMEOUT: &str = "5s";
const METADATA_TRACE_STATEMENT_TIMEOUT: &str = "30s";

async fn set_metadata_trace_transaction_timeouts(
    tx: &mut Transaction<'_, sqlx::Postgres>,
) -> Result<(), SessionStoreError> {
    sqlx::query(
        "select set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
    )
    .bind(METADATA_TRACE_LOCK_TIMEOUT)
    .bind(METADATA_TRACE_STATEMENT_TIMEOUT)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct CreateExecutionResult {
    pub execution: SessionExecution,
    pub created: bool,
}

#[derive(Clone, Debug)]
pub struct ClaimExecutionResult {
    pub execution: SessionExecution,
    /// True only when this call transitioned the execution from `queued` to
    /// `running`. False means another request already claimed it (or it is
    /// terminal), so the caller must not drive the execution.
    pub claimed: bool,
}

/// The only terminal lifecycle transitions a stdout owner may publish.
///
/// The status, resulting session state, and event type are derived from the
/// variant so callers cannot commit a mismatched terminal record.
#[derive(Clone, Debug)]
pub enum OwnedTerminalEvent {
    Completed { payload: Value },
    Failed { error: String, payload: Value },
    Cancelled { reason: String, payload: Value },
}

impl OwnedTerminalEvent {
    fn into_parts(
        self,
    ) -> (
        ExecutionStatus,
        SessionStatus,
        Option<String>,
        &'static str,
        Value,
    ) {
        match self {
            Self::Completed { payload } => (
                ExecutionStatus::Completed,
                SessionStatus::Idle,
                None,
                "session.execution_completed",
                payload,
            ),
            Self::Failed { error, payload } => (
                ExecutionStatus::Failed,
                SessionStatus::Failed,
                Some(error),
                "session.execution_failed",
                payload,
            ),
            Self::Cancelled { reason, payload } => (
                ExecutionStatus::Cancelled,
                SessionStatus::Idle,
                Some(reason),
                "session.execution_cancelled",
                payload,
            ),
        }
    }
}

/// An active execution whose stdout-owner lease was released by
/// [`PgSessionStore::release_stdout_owned_executions`].
#[derive(Clone, Debug)]
pub struct ReleasedExecution {
    pub execution_id: String,
    pub thread_key: ThreadKey,
}

/// An active execution together with its stdout-owner lease state, as
/// returned by [`PgSessionStore::list_active_executions_with_ownership`].
/// The lease snapshot is advisory — only the conditional
/// `claim_expired_stdout_owner` update decides ownership — but it lets an
/// adoption scan skip executions with a live owner without touching the
/// session row or the sandbox backend.
#[derive(Clone, Debug)]
pub struct ActiveExecutionOwnership {
    pub execution: SessionExecution,
    pub stdout_owner_id: Option<String>,
    /// True when a stdout-owner lease exists and has not expired yet.
    pub stdout_owner_lease_active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdleSandboxCandidate {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub execution_id: String,
    pub idle_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCapacityCandidate {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub latest_execution_id: Option<String>,
    pub last_active_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowOwnedSandbox {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTraceConfigIdentity {
    pub generation: i64,
    pub fingerprint: String,
    /// A disabled deployment is still a durable generation. It fences and
    /// retires traced sandboxes instead of leaving the old sidecar active.
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTraceReconcilerLease {
    pub owner_id: String,
    pub fence: i64,
}

/// A shared consent and assignment fence held until the caller finishes the
/// bounded sandbox stdin write. A revoke takes the same consent row FOR UPDATE,
/// so it cannot commit between authorization and delivery.
pub struct MetadataTraceInputGuard {
    transaction: Transaction<'static, sqlx::Postgres>,
    deadline: OffsetDateTime,
    _assignment_epoch: String,
}

impl MetadataTraceInputGuard {
    /// Database-derived consent deadline captured while the lock is held.
    pub fn remaining(&self) -> Option<Duration> {
        let remaining = self.deadline - OffsetDateTime::now_utc();
        (remaining > TimeDuration::ZERO)
            .then(|| Duration::from_secs(u64::try_from(remaining.whole_seconds()).unwrap_or(0)))
    }

    pub async fn commit(self) -> Result<(), SessionStoreError> {
        self.transaction.commit().await?;
        Ok(())
    }
}

/// The durable, actor-scoped trace-consent record. A missing row is represented
/// as disabled revision zero and is never materialized by a read.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct MetadataTraceConsent {
    pub source: String,
    pub workspace_id: String,
    pub user_id: String,
    pub enabled: bool,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    pub revision: i64,
    pub drain_pending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTraceDrainTarget {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub assignment_epoch: String,
    pub revision: i64,
    pub resource_uid: String,
}

/// The exact consent primary key persisted with a traced assignment. This is
/// deliberately separate from the one-way subject hash used by the sandbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTraceAssignmentActor {
    pub source: String,
    pub workspace_id: String,
    pub user_id: String,
}

#[derive(sqlx::FromRow)]
struct MetadataTraceConsentRow {
    source: String,
    workspace_id: String,
    user_id: String,
    enabled: bool,
    expires_at: Option<OffsetDateTime>,
    revision: i64,
    drain_pending: bool,
}

#[derive(sqlx::FromRow)]
struct MetadataTraceConsentRequestRow {
    request_hash: String,
    result_enabled: Option<bool>,
    result_expires_at: Option<OffsetDateTime>,
    result_revision: Option<i64>,
    result_drain_pending: Option<bool>,
    drain_assignment_revision: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct MetadataTraceDrainTargetRow {
    thread_key: String,
    sandbox_id: String,
    assignment_epoch: String,
    revision: i64,
    resource_uid: String,
}

#[derive(sqlx::FromRow)]
struct MetadataTraceAssignmentActorRow {
    source: String,
    workspace_id: String,
    user_id: String,
    assignment_epoch: String,
}

#[derive(sqlx::FromRow)]
struct SandboxAssignmentReconciliationLockRow {
    sandbox_id: Option<String>,
    metadata_trace_enabled: bool,
    metadata_trace_resource_uid: Option<String>,
}

impl TryFrom<MetadataTraceDrainTargetRow> for MetadataTraceDrainTarget {
    type Error = SessionStoreError;

    fn try_from(row: MetadataTraceDrainTargetRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
            assignment_epoch: row.assignment_epoch,
            revision: row.revision,
            resource_uid: row.resource_uid,
        })
    }
}

impl From<MetadataTraceConsentRow> for MetadataTraceConsent {
    fn from(row: MetadataTraceConsentRow) -> Self {
        Self {
            source: row.source,
            workspace_id: row.workspace_id,
            user_id: row.user_id,
            enabled: row.enabled,
            expires_at: row.expires_at,
            revision: row.revision,
            drain_pending: row.drain_pending,
        }
    }
}

fn metadata_trace_request_result(
    source: &str,
    workspace_id: &str,
    user_id: &str,
    request: MetadataTraceConsentRequestRow,
) -> Result<MetadataTraceConsent, SessionStoreError> {
    let (Some(enabled), Some(revision), Some(drain_pending)) = (
        request.result_enabled,
        request.result_revision,
        request.result_drain_pending,
    ) else {
        return Err(SessionStoreError::MetadataTraceIdempotencyIncomplete);
    };
    if (enabled && request.result_expires_at.is_none())
        || (!enabled && request.result_expires_at.is_some())
    {
        return Err(SessionStoreError::MetadataTraceIdempotencyIncomplete);
    }
    Ok(MetadataTraceConsent {
        source: source.to_owned(),
        workspace_id: workspace_id.to_owned(),
        user_id: user_id.to_owned(),
        enabled,
        expires_at: request.result_expires_at,
        revision,
        drain_pending,
    })
}

async fn persist_metadata_trace_request_result(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    source: &str,
    workspace_id: &str,
    user_id: &str,
    idempotency_key: &str,
    consent: &MetadataTraceConsent,
    drain_assignment_revision: Option<i64>,
) -> Result<(), SessionStoreError> {
    sqlx::query(
        r#"
        update metadata_trace_consent_requests
        set result_enabled = $5,
            result_expires_at = $6,
            result_revision = $7,
            result_drain_pending = $8,
            drain_assignment_revision = $9
        where source = $1 and workspace_id = $2 and user_id = $3 and idempotency_key = $4
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .bind(idempotency_key)
    .bind(consent.enabled)
    .bind(consent.expires_at)
    .bind(consent.revision)
    .bind(consent.drain_pending)
    .bind(drain_assignment_revision)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn grant_metadata_trace_consent_in_transaction(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    source: &str,
    workspace_id: &str,
    user_id: &str,
    expires_at: OffsetDateTime,
) -> Result<MetadataTraceConsent, SessionStoreError> {
    let row = sqlx::query_as::<_, MetadataTraceConsentRow>(
        r#"
        insert into metadata_trace_consents (source, workspace_id, user_id, enabled, expires_at, revision, drain_pending)
        values ($1, $2, $3, true, $4, 1, false)
        on conflict (source, workspace_id, user_id) do update
        set enabled = true,
            expires_at = excluded.expires_at,
            revision = case when metadata_trace_consents.enabled and metadata_trace_consents.expires_at = excluded.expires_at then metadata_trace_consents.revision else metadata_trace_consents.revision + 1 end,
            updated_at = now()
        where not metadata_trace_consents.drain_pending
        returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .bind(expires_at)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(Into::into)
        .ok_or(SessionStoreError::MetadataTraceDrainPending)
}

async fn metadata_trace_drain_targets_in_transaction(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    source: &str,
    workspace_id: &str,
    user_id: &str,
    revision: i64,
) -> Result<Vec<MetadataTraceDrainTarget>, SessionStoreError> {
    let targets = sqlx::query_as::<_, MetadataTraceDrainTargetRow>(
        r#"
        select thread_key, sandbox_id,
               sandbox_metadata_trace_assignment_epoch as assignment_epoch,
               sandbox_metadata_trace_consent_revision as revision,
               sandbox_metadata_trace_resource_uid as resource_uid
        from sessions
        where sandbox_metadata_trace_enabled is true
          and sandbox_metadata_trace_source = $1
          and sandbox_metadata_trace_workspace_id = $2
          and sandbox_metadata_trace_user_id = $3
          and sandbox_metadata_trace_consent_revision = $4
          and sandbox_id is not null
          and sandbox_metadata_trace_assignment_epoch is not null
          and sandbox_metadata_trace_resource_uid is not null
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .bind(revision)
    .fetch_all(&mut **tx)
    .await?;
    targets
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()
}

async fn legacy_metadata_trace_assignment_exists(
    tx: &mut Transaction<'_, sqlx::Postgres>,
) -> Result<bool, SessionStoreError> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        select exists (
            select 1 from sessions
            where sandbox_metadata_trace_enabled is true
              and sandbox_id is not null
              and (
                    sandbox_metadata_trace_assignment_epoch is null
                    or sandbox_metadata_trace_source is null
                    or sandbox_metadata_trace_workspace_id is null
                    or sandbox_metadata_trace_user_id is null
                    or sandbox_metadata_trace_resource_uid is null
              )
        )
        "#,
    )
    .fetch_one(&mut **tx)
    .await?)
}

async fn revoke_metadata_trace_consent_in_transaction(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    source: &str,
    workspace_id: &str,
    user_id: &str,
    _subject_hash: &str,
) -> Result<
    (
        MetadataTraceConsent,
        Vec<MetadataTraceDrainTarget>,
        Option<i64>,
    ),
    SessionStoreError,
> {
    let consent = sqlx::query_as::<_, MetadataTraceConsentRow>(
        r#"
        insert into metadata_trace_consents (source, workspace_id, user_id, enabled, expires_at, revision, drain_pending)
        values ($1, $2, $3, false, null, 1, false)
        on conflict (source, workspace_id, user_id) do update
        set enabled = false, expires_at = null,
            revision = case when metadata_trace_consents.enabled then metadata_trace_consents.revision + 1 else metadata_trace_consents.revision end,
            drain_pending = metadata_trace_consents.drain_pending,
            updated_at = now()
        returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .fetch_one(&mut **tx)
    .await?;
    let drain_assignment_revision = consent.revision.checked_sub(1);
    let targets = match drain_assignment_revision {
        Some(revision) => {
            metadata_trace_drain_targets_in_transaction(tx, source, workspace_id, user_id, revision)
                .await?
        }
        None => Vec::new(),
    };
    let drain_pending = consent.drain_pending
        || !targets.is_empty()
        || legacy_metadata_trace_assignment_exists(tx).await?;
    let consent = sqlx::query_as::<_, MetadataTraceConsentRow>(
        r#"
        update metadata_trace_consents
        set drain_pending = $4, updated_at = now()
        where source = $1 and workspace_id = $2 and user_id = $3
        returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .bind(drain_pending)
    .fetch_one(&mut **tx)
    .await?;
    Ok((consent.into(), targets, drain_assignment_revision))
}

/// Holds the session row lock while a reconciler retires a specific sandbox.
/// Any concurrent replacement waits for this transaction, so it is either
/// observed before the stop or committed after the old sandbox is cleared.
pub struct SandboxAssignmentReconciliationLock<'a> {
    transaction: Transaction<'a, sqlx::Postgres>,
    thread_key: String,
    sandbox_id: String,
    metadata_trace_enabled: bool,
    metadata_trace_resource_uid: Option<String>,
}

impl SandboxAssignmentReconciliationLock<'_> {
    /// The exact backend resource to retire for a traced assignment. A null
    /// UID is legacy state and must not be deleted by name.
    pub fn metadata_trace_resource_uid(&self) -> Option<&str> {
        self.metadata_trace_resource_uid.as_deref()
    }

    pub fn metadata_trace_enabled(&self) -> bool {
        self.metadata_trace_enabled
    }

    pub async fn clear_and_commit(mut self) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set
                sandbox_id = null,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_api_server_enabled = null,
                sandbox_metadata_trace_enabled = null,
                sandbox_metadata_trace_expires_at = null,
                sandbox_metadata_trace_subject_hash = null,
                sandbox_metadata_trace_source = null,
                sandbox_metadata_trace_workspace_id = null,
                sandbox_metadata_trace_user_id = null,
                sandbox_metadata_trace_consent_revision = null,
                sandbox_metadata_trace_config_fingerprint = null,
                sandbox_metadata_trace_config_generation = null,
                sandbox_metadata_trace_resource_uid = null,
                sandbox_last_active_at = null,
                updated_at = now()
            where thread_key = $1 and sandbox_id = $2
            "#,
        )
        .bind(&self.thread_key)
        .bind(&self.sandbox_id)
        .execute(&mut *self.transaction)
        .await?;
        self.transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn rollback(self) -> Result<(), SessionStoreError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct PgSessionStore {
    pool: PgPool,
}

#[derive(Clone, Copy)]
enum SessionPrincipalBinding<'a> {
    Unconstrained,
    ClaimOrExact(&'a str),
    Exact(&'a str),
}

impl PgSessionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self, SessionStoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
            .connect(database_url)
            .await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn lock_metadata_trace_input(
        &self,
        expected: &SandboxCapabilities,
        thread_key: &ThreadKey,
        execution_id: &str,
        sandbox_id: &str,
    ) -> Result<Option<MetadataTraceInputGuard>, SessionStoreError> {
        let mut transaction = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut transaction).await?;
        let actor = sqlx::query_as::<_, MetadataTraceAssignmentActorRow>(
            r#"
            select s.sandbox_metadata_trace_source as source,
                   s.sandbox_metadata_trace_workspace_id as workspace_id,
                   s.sandbox_metadata_trace_user_id as user_id,
                   s.sandbox_metadata_trace_assignment_epoch as assignment_epoch
            from sessions s join session_executions e on e.thread_key = s.thread_key
            where e.execution_id = $1
              and s.thread_key = $2
              and e.status in ('queued', 'running')
              and s.sandbox_id = $3
              and s.sandbox_metadata_trace_enabled is true
              and s.sandbox_metadata_trace_assignment_epoch is not null
              and s.sandbox_metadata_trace_source is not null
              and s.sandbox_metadata_trace_workspace_id is not null
              and s.sandbox_metadata_trace_user_id is not null
              and s.sandbox_metadata_trace_subject_hash is not distinct from $4
              and s.sandbox_metadata_trace_consent_revision is not distinct from $5
              and s.sandbox_metadata_trace_expires_at is not distinct from $6
              and s.sandbox_metadata_trace_config_fingerprint is not distinct from $7
              and s.sandbox_metadata_trace_config_generation is not distinct from $8
            for share of s, e
            "#,
        )
        .bind(execution_id)
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .bind(&expected.metadata_trace_subject_hash)
        .bind(expected.metadata_trace_consent_revision)
        .bind(expected.metadata_trace_expires_at)
        .bind(&expected.metadata_trace_config_fingerprint)
        .bind(expected.metadata_trace_config_generation)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(actor) = actor else {
            transaction.rollback().await?;
            return Ok(None);
        };
        let consent = sqlx::query_as::<_, MetadataTraceConsentRow>(
            r#"
            select source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
            from metadata_trace_consents
            where source = $1 and workspace_id = $2 and user_id = $3
            for share
        "#,
        )
        .bind(&actor.source)
        .bind(&actor.workspace_id)
        .bind(&actor.user_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let now = sqlx::query_scalar::<_, OffsetDateTime>("select clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let valid_consent = consent.as_ref().is_some_and(|consent| {
            consent.enabled
                && !consent.drain_pending
                && consent.revision == expected.metadata_trace_consent_revision.unwrap_or_default()
                && consent.expires_at.is_some_and(|expiry| expiry > now)
                && consent.expires_at == expected.metadata_trace_expires_at
        });
        if !valid_consent {
            transaction.rollback().await?;
            return Ok(None);
        }
        let config_active = sqlx::query_scalar::<_, bool>(
            "select generation = $1 and config_fingerprint = $2 from metadata_trace_config_state where singleton = true for share",
        )
        .bind(expected.metadata_trace_config_generation)
        .bind(&expected.metadata_trace_config_fingerprint)
        .fetch_optional(&mut *transaction)
        .await?;
        if config_active != Some(true) {
            transaction.rollback().await?;
            return Ok(None);
        }
        Ok(Some(MetadataTraceInputGuard {
            transaction,
            deadline: consent
                .expect("validated above")
                .expires_at
                .expect("validated above"),
            _assignment_epoch: actor.assignment_epoch,
        }))
    }

    pub async fn metadata_trace_consent(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<MetadataTraceConsent, SessionStoreError> {
        let row = sqlx::query_as::<_, MetadataTraceConsentRow>(
            "select source, workspace_id, user_id, enabled, expires_at, revision, drain_pending from metadata_trace_consents where source = $1 and workspace_id = $2 and user_id = $3",
        )
        .bind(source).bind(workspace_id).bind(user_id)
        .fetch_optional(&self.pool).await?;
        Ok(row.map(Into::into).unwrap_or_else(|| MetadataTraceConsent {
            source: source.to_owned(),
            workspace_id: workspace_id.to_owned(),
            user_id: user_id.to_owned(),
            enabled: false,
            expires_at: None,
            revision: 0,
            drain_pending: false,
        }))
    }

    /// Read the durable actor FK only for the exact currently assigned traced
    /// sandbox. A null legacy FK is not guessed from execution metadata.
    pub async fn metadata_trace_assignment_actor(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<Option<MetadataTraceAssignmentActor>, SessionStoreError> {
        let row = sqlx::query_as::<_, MetadataTraceAssignmentActorRow>(
            r#"
            select sandbox_metadata_trace_source as source,
                   sandbox_metadata_trace_workspace_id as workspace_id,
                   sandbox_metadata_trace_user_id as user_id,
                   sandbox_metadata_trace_assignment_epoch as assignment_epoch
            from sessions
            where thread_key = $1
              and sandbox_id = $2
              and sandbox_metadata_trace_enabled is true
              and sandbox_metadata_trace_assignment_epoch is not null
              and sandbox_metadata_trace_source is not null
              and sandbox_metadata_trace_workspace_id is not null
              and sandbox_metadata_trace_user_id is not null
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| MetadataTraceAssignmentActor {
            source: row.source,
            workspace_id: row.workspace_id,
            user_id: row.user_id,
        }))
    }

    pub async fn pending_metadata_trace_drains(
        &self,
    ) -> Result<Vec<MetadataTraceConsent>, SessionStoreError> {
        let rows = sqlx::query_as::<_, MetadataTraceConsentRow>(
            "select source, workspace_id, user_id, enabled, expires_at, revision, drain_pending from metadata_trace_consents where drain_pending is true order by source, workspace_id, user_id",
        ).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Atomically reserve, apply, and persist a grant result.  A request row
    /// with an incomplete result is never re-applied: it can only have been
    /// written by an older binary, because this method commits the reservation
    /// and result together with the consent mutation.
    pub async fn grant_metadata_trace_consent_idempotent(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        expires_at: OffsetDateTime,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<MetadataTraceConsent, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut tx).await?;
        let inserted = sqlx::query(
            r#"
            insert into metadata_trace_consent_requests
                (source, workspace_id, user_id, idempotency_key, request_hash)
            values ($1, $2, $3, $4, $5)
            on conflict do nothing
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(idempotency_key)
        .bind(request_hash)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        let request = sqlx::query_as::<_, MetadataTraceConsentRequestRow>(
            r#"
            select request_hash, result_enabled, result_expires_at, result_revision, result_drain_pending, drain_assignment_revision
            from metadata_trace_consent_requests
            where source = $1 and workspace_id = $2 and user_id = $3 and idempotency_key = $4
            for update
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(idempotency_key)
        .fetch_one(&mut *tx)
        .await?;
        if request.request_hash != request_hash {
            return Err(SessionStoreError::MetadataTraceIdempotencyConflict);
        }
        if !inserted {
            let result = metadata_trace_request_result(source, workspace_id, user_id, request)?;
            tx.commit().await?;
            return Ok(result);
        }
        let consent = grant_metadata_trace_consent_in_transaction(
            &mut tx,
            source,
            workspace_id,
            user_id,
            expires_at,
        )
        .await?;
        persist_metadata_trace_request_result(
            &mut tx,
            source,
            workspace_id,
            user_id,
            idempotency_key,
            &consent,
            None,
        )
        .await?;
        tx.commit().await?;
        Ok(consent)
    }

    /// Upsert a grant in the only mutable owner. PostgreSQL's conflict lock
    /// makes simultaneous first grants idempotent rather than creating two
    /// revisions.
    pub async fn grant_metadata_trace_consent(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        expires_at: OffsetDateTime,
    ) -> Result<MetadataTraceConsent, SessionStoreError> {
        let row = sqlx::query_as::<_, MetadataTraceConsentRow>(r#"
            insert into metadata_trace_consents (source, workspace_id, user_id, enabled, expires_at, revision, drain_pending)
            values ($1, $2, $3, true, $4, 1, false)
            on conflict (source, workspace_id, user_id) do update
            set enabled = true,
                expires_at = excluded.expires_at,
                revision = case when metadata_trace_consents.enabled and metadata_trace_consents.expires_at = excluded.expires_at then metadata_trace_consents.revision else metadata_trace_consents.revision + 1 end,
                updated_at = now()
            where not metadata_trace_consents.drain_pending
            returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#).bind(source).bind(workspace_id).bind(user_id).bind(expires_at).fetch_optional(&self.pool).await?;
        row.map(Into::into)
            .ok_or(SessionStoreError::MetadataTraceDrainPending)
    }

    /// Fence all matching assignments in the same transaction as the revoke.
    /// The caller must stop the returned sandboxes before acknowledging the
    /// disabled response; `drain_pending` survives a crash in between.
    pub async fn revoke_metadata_trace_consent(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        _subject_hash: &str,
    ) -> Result<(MetadataTraceConsent, Vec<MetadataTraceDrainTarget>), SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut tx).await?;
        let consent = sqlx::query_as::<_, MetadataTraceConsentRow>(r#"
            insert into metadata_trace_consents (source, workspace_id, user_id, enabled, expires_at, revision, drain_pending)
            values ($1, $2, $3, false, null, 1, false)
            on conflict (source, workspace_id, user_id) do update
            set enabled = false, expires_at = null,
                revision = case when metadata_trace_consents.enabled then metadata_trace_consents.revision + 1 else metadata_trace_consents.revision end,
                drain_pending = metadata_trace_consents.drain_pending,
                updated_at = now()
            returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#).bind(source).bind(workspace_id).bind(user_id).fetch_one(&mut *tx).await?;
        let targets = sqlx::query_as::<_, MetadataTraceDrainTargetRow>(r#"
            select thread_key, sandbox_id, sandbox_metadata_trace_assignment_epoch as assignment_epoch, sandbox_metadata_trace_consent_revision as revision, sandbox_metadata_trace_resource_uid as resource_uid
            from sessions
            where sandbox_metadata_trace_enabled is true
              and sandbox_metadata_trace_source = $1
              and sandbox_metadata_trace_workspace_id = $2
              and sandbox_metadata_trace_user_id = $3
              and sandbox_metadata_trace_consent_revision = $4
              and sandbox_id is not null
              and sandbox_metadata_trace_assignment_epoch is not null
              and sandbox_metadata_trace_resource_uid is not null
        "#)
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(consent.revision.saturating_sub(1))
        .fetch_all(&mut *tx)
        .await?;
        let targets = targets
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        let drain_pending = consent.drain_pending
            || !targets.is_empty()
            || legacy_metadata_trace_assignment_exists(&mut tx).await?;
        let consent = sqlx::query_as::<_, MetadataTraceConsentRow>(
            "update metadata_trace_consents set drain_pending = $4, updated_at = now() where source = $1 and workspace_id = $2 and user_id = $3 returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending",
        ).bind(source).bind(workspace_id).bind(user_id).bind(drain_pending).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok((consent.into(), targets))
    }

    /// Atomically reserve, revoke, and persist the response.  The physical
    /// sandbox drain is deliberately outside this transaction; the persisted
    /// response remains `drain_pending` until a later reconciler proves the
    /// exact assignments have gone away.
    pub async fn revoke_metadata_trace_consent_idempotent(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        subject_hash: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<(MetadataTraceConsent, Vec<MetadataTraceDrainTarget>), SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut tx).await?;
        let inserted = sqlx::query(
            r#"
            insert into metadata_trace_consent_requests
                (source, workspace_id, user_id, idempotency_key, request_hash)
            values ($1, $2, $3, $4, $5)
            on conflict do nothing
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(idempotency_key)
        .bind(request_hash)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        let request = sqlx::query_as::<_, MetadataTraceConsentRequestRow>(
            r#"
            select request_hash, result_enabled, result_expires_at, result_revision, result_drain_pending, drain_assignment_revision
            from metadata_trace_consent_requests
            where source = $1 and workspace_id = $2 and user_id = $3 and idempotency_key = $4
            for update
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(idempotency_key)
        .fetch_one(&mut *tx)
        .await?;
        if request.request_hash != request_hash {
            return Err(SessionStoreError::MetadataTraceIdempotencyConflict);
        }
        if !inserted {
            let drain_assignment_revision = request.drain_assignment_revision;
            let consent = metadata_trace_request_result(source, workspace_id, user_id, request)?;
            let targets = match drain_assignment_revision {
                Some(revision) => {
                    metadata_trace_drain_targets_in_transaction(
                        &mut tx,
                        source,
                        workspace_id,
                        user_id,
                        revision,
                    )
                    .await?
                }
                None => Vec::new(),
            };
            tx.commit().await?;
            return Ok((consent, targets));
        }
        let (consent, targets, drain_assignment_revision) =
            revoke_metadata_trace_consent_in_transaction(
                &mut tx,
                source,
                workspace_id,
                user_id,
                subject_hash,
            )
            .await?;
        persist_metadata_trace_request_result(
            &mut tx,
            source,
            workspace_id,
            user_id,
            idempotency_key,
            &consent,
            drain_assignment_revision,
        )
        .await?;
        tx.commit().await?;
        Ok((consent, targets))
    }

    pub async fn run_migrations(&self) -> Result<(), SessionStoreError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Make a deployment configuration the sole active trace configuration.
    /// Generation is a deployment-owned fence: lower generations and a
    /// different fingerprint at the same generation are rejected.
    pub async fn activate_metadata_trace_config(
        &self,
        identity: &MetadataTraceConfigIdentity,
    ) -> Result<(), SessionStoreError> {
        if identity.generation <= 0 {
            return Err(SessionStoreError::InvalidMetadataTraceGeneration(
                identity.generation,
            ));
        }
        let mut tx = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut tx).await?;
        sqlx::query(
            r#"
            insert into metadata_trace_config_state (singleton, generation, config_fingerprint)
            values (true, $1, $2)
            on conflict (singleton) do nothing
            "#,
        )
        .bind(identity.generation)
        .bind(&identity.fingerprint)
        .execute(&mut *tx)
        .await?;
        let current = sqlx::query_as::<_, MetadataTraceConfigStateRow>(
            r#"
            select generation, config_fingerprint
            from metadata_trace_config_state
            where singleton = true
            for update
            "#,
        )
        .fetch_one(&mut *tx)
        .await?;
        match current.generation.cmp(&identity.generation) {
            std::cmp::Ordering::Less => {
                sqlx::query(
                    r#"
                    update metadata_trace_config_state
                    set generation = $1,
                        config_fingerprint = $2,
                        reconciler_owner_id = null,
                        reconciler_fence = reconciler_fence + 1,
                        reconciler_lease_expires_at = null,
                        activated_at = now(),
                        updated_at = now()
                    where singleton = true
                    "#,
                )
                .bind(identity.generation)
                .bind(&identity.fingerprint)
                .execute(&mut *tx)
                .await?;
            }
            std::cmp::Ordering::Equal if current.config_fingerprint == identity.fingerprint => {}
            std::cmp::Ordering::Equal => {
                return Err(SessionStoreError::MetadataTraceConfigConflict {
                    generation: identity.generation,
                    existing_fingerprint: current.config_fingerprint,
                    requested_fingerprint: identity.fingerprint.clone(),
                });
            }
            std::cmp::Ordering::Greater => {
                return Err(SessionStoreError::StaleMetadataTraceGeneration {
                    active: current.generation,
                    requested: identity.generation,
                });
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn metadata_trace_config_is_active(
        &self,
        identity: &MetadataTraceConfigIdentity,
    ) -> Result<bool, SessionStoreError> {
        let active = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (
                select 1 from metadata_trace_config_state
                where singleton = true
                  and generation = $1
                  and config_fingerprint = $2
            )
            "#,
        )
        .bind(identity.generation)
        .bind(&identity.fingerprint)
        .fetch_one(&self.pool)
        .await?;
        Ok(active)
    }

    /// Acquire or renew the singleton reconciliation lease. Every acquisition
    /// advances a fence so an expired owner cannot clear a newer assignment.
    pub async fn acquire_metadata_trace_reconciler_lease(
        &self,
        identity: &MetadataTraceConfigIdentity,
        owner_id: &str,
        lease: TimeDuration,
    ) -> Result<Option<MetadataTraceReconcilerLease>, SessionStoreError> {
        let row = sqlx::query_as::<_, MetadataTraceLeaseRow>(
            r#"
            update metadata_trace_config_state
            set reconciler_owner_id = $3,
                reconciler_fence = reconciler_fence + 1,
                reconciler_lease_expires_at = now() + ($4::float8 * interval '1 second'),
                updated_at = now()
            where singleton = true
              and generation = $1
              and config_fingerprint = $2
              and (
                    reconciler_lease_expires_at is null
                    or reconciler_lease_expires_at <= now()
                    or reconciler_owner_id = $3
              )
            returning reconciler_owner_id, reconciler_fence
            "#,
        )
        .bind(identity.generation)
        .bind(&identity.fingerprint)
        .bind(owner_id)
        .bind(lease.as_seconds_f64())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| MetadataTraceReconcilerLease {
            owner_id: row.reconciler_owner_id,
            fence: row.reconciler_fence,
        }))
    }

    pub async fn metadata_trace_reconciler_lease_is_active(
        &self,
        identity: &MetadataTraceConfigIdentity,
        lease: &MetadataTraceReconcilerLease,
    ) -> Result<bool, SessionStoreError> {
        let active = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (
                select 1 from metadata_trace_config_state
                where singleton = true
                  and generation = $1
                  and config_fingerprint = $2
                  and reconciler_owner_id = $3
                  and reconciler_fence = $4
                  and reconciler_lease_expires_at > now()
            )
            "#,
        )
        .bind(identity.generation)
        .bind(&identity.fingerprint)
        .bind(&lease.owner_id)
        .bind(lease.fence)
        .fetch_one(&self.pool)
        .await?;
        Ok(active)
    }

    pub async fn listen_session_events(&self) -> Result<SessionEventListener, SessionStoreError> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(SESSION_EVENTS_CHANNEL).await?;
        Ok(SessionEventListener { listener })
    }

    pub async fn create_or_get_session(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
    ) -> Result<Session, SessionStoreError> {
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            proxy_labels,
            SessionPrincipalBinding::Unconstrained,
        )
        .await
    }

    pub async fn create_or_get_session_with_exact_principal(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
        iron_control_principal: &str,
    ) -> Result<Session, SessionStoreError> {
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            proxy_labels,
            SessionPrincipalBinding::Exact(iron_control_principal),
        )
        .await
    }

    pub async fn create_or_get_session_with_principal(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
        iron_control_principal: &str,
    ) -> Result<Session, SessionStoreError> {
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            proxy_labels,
            SessionPrincipalBinding::ClaimOrExact(iron_control_principal),
        )
        .await
    }

    async fn create_or_get_session_inner(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
        principal_binding: SessionPrincipalBinding<'_>,
    ) -> Result<Session, SessionStoreError> {
        let (requested_principal, claim_unbound_principal) = match principal_binding {
            SessionPrincipalBinding::Unconstrained => (None, false),
            SessionPrincipalBinding::ClaimOrExact(principal) => (Some(principal), true),
            SessionPrincipalBinding::Exact(principal) => (Some(principal), false),
        };
        let enforce_exact_principal = requested_principal.is_some();

        sqlx::query(
            r#"
            insert into sessions (
                thread_key,
                harness_type,
                persona_id,
                status,
                metadata,
                proxy_labels,
                iron_control_principal
            )
            values ($1, $2, $3, $4, $5, $6, $7)
            on conflict (thread_key) do nothing
            "#,
        )
        .bind(thread_key.as_str())
        .bind(harness_type.as_ref())
        .bind(persona_id)
        .bind(SessionStatus::Idle.as_ref())
        .bind(metadata)
        .bind(Json(proxy_labels.clone()))
        .bind(requested_principal)
        .execute(&self.pool)
        .await?;

        if claim_unbound_principal {
            sqlx::query(
                r#"
                update sessions
                set iron_control_principal = $2, updated_at = now()
                where thread_key = $1 and iron_control_principal is null
                "#,
            )
            .bind(thread_key.as_str())
            .bind(requested_principal)
            .execute(&self.pool)
            .await?;
        }

        if !proxy_labels.is_empty() {
            sqlx::query(
                r#"
                update sessions
                set proxy_labels = $2, updated_at = now()
                where thread_key = $1
                    and proxy_labels = '{}'::jsonb
                    and (
                        not $3
                        or iron_control_principal is not distinct from $4
                    )
                "#,
            )
            .bind(thread_key.as_str())
            .bind(Json(proxy_labels))
            .bind(enforce_exact_principal)
            .bind(requested_principal)
            .execute(&self.pool)
            .await?;
        }

        let session = self.get_session(thread_key).await?;
        if enforce_exact_principal
            && session.iron_control_principal.as_deref() != requested_principal
        {
            return Err(SessionStoreError::PrincipalConflict {
                thread_key: thread_key.as_str().to_owned(),
                existing: session.iron_control_principal,
                requested: requested_principal.map(str::to_owned),
            });
        }
        if session.harness_type != *harness_type {
            return Err(SessionStoreError::HarnessConflict {
                thread_key: thread_key.as_str().to_owned(),
                existing: session.harness_type.to_string(),
                requested: harness_type.as_ref().to_owned(),
            });
        }
        if session.persona_id.as_deref() != persona_id {
            return Err(SessionStoreError::PersonaConflict {
                thread_key: thread_key.as_str().to_owned(),
                existing: session.persona_id,
                requested: persona_id.map(str::to_owned),
            });
        }
        Ok(session)
    }

    pub async fn get_session(&self, thread_key: &ThreadKey) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            select thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            from sessions
            where thread_key = $1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SessionStoreError::NotFound {
            thread_key: thread_key.as_str().to_owned(),
        })?;

        row.try_into()
    }

    pub async fn get_session_title(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<String>, SessionStoreError> {
        let title = sqlx::query_scalar::<_, Option<String>>(
            r#"
            select title
            from sessions
            where thread_key = $1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(title)
    }

    pub async fn append_messages(
        &self,
        thread_key: &ThreadKey,
        messages: &[SessionMessageInput],
    ) -> Result<Vec<String>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let mut message_ids = Vec::with_capacity(messages.len());

        for message in messages {
            let message_id = prefixed_id("msg");
            let parts = Value::Array(message.parts.clone());
            let persisted_message_id = sqlx::query_scalar::<_, String>(
                r#"
                insert into session_messages
                    (message_id, thread_key, client_message_id, role, parts, metadata)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (thread_key, client_message_id)
                    where client_message_id is not null
                do update set client_message_id = excluded.client_message_id
                returning message_id
                "#,
            )
            .bind(&message_id)
            .bind(thread_key.as_str())
            .bind(message.client_message_id.as_deref())
            .bind(message.role.as_ref())
            .bind(parts)
            .bind(message.metadata.clone())
            .fetch_one(&mut *tx)
            .await?;
            message_ids.push(persisted_message_id);
        }

        tx.commit().await?;
        Ok(message_ids)
    }

    pub async fn title_generation_candidate(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<Vec<Value>>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, Value>(
            r#"
            select m.parts
            from sessions s
            join session_messages m on m.thread_key = s.thread_key
            where s.thread_key = $1 and s.title is null
                and m.role = $2
            order by m.created_at, m.message_id
            "#,
        )
        .bind(thread_key.as_str())
        .bind(MessageRole::User.as_ref())
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let parts = rows
            .into_iter()
            .flat_map(|parts| match parts {
                Value::Array(parts) => parts,
                other => vec![other],
            })
            .collect();
        Ok(Some(parts))
    }

    pub async fn set_session_title_if_empty(
        &self,
        thread_key: &ThreadKey,
        title: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set title = $2, updated_at = now()
            where thread_key = $1 and title is null
            "#,
        )
        .bind(thread_key.as_str())
        .bind(title)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_messages(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Vec<SessionMessage>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionMessageRow>(
            r#"
            select message_id, client_message_id, thread_key, role, parts, metadata, created_at
            from session_messages
            where thread_key = $1
            order by created_at, message_id
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn create_execution(
        &self,
        thread_key: &ThreadKey,
        idempotency_key: Option<&str>,
        metadata: Value,
    ) -> Result<CreateExecutionResult, SessionStoreError> {
        let execution_id = prefixed_id("exe");
        let row = sqlx::query_as::<_, CreateExecutionRow>(
            r#"
            insert into session_executions
                (execution_id, thread_key, idempotency_key, status, metadata)
            values ($1, $2, $3, $4, $5)
            on conflict (thread_key, idempotency_key)
                where idempotency_key is not null
            do update set idempotency_key = excluded.idempotency_key
            returning
                execution_id = $1 as created,
                execution_id,
                idempotency_key,
                thread_key,
                status,
                metadata,
                error,
                created_at,
                updated_at,
                started_at,
                completed_at
            "#,
        )
        .bind(&execution_id)
        .bind(thread_key.as_str())
        .bind(idempotency_key)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(metadata)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    /// Records the resolved trace boundary after consent lookup but before the
    /// sandbox receives input. `jsonb ||` preserves ingress metadata while the
    /// control plane owns the reserved trace fields.
    pub async fn merge_execution_metadata(
        &self,
        execution_id: &str,
        metadata: Value,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            "update session_executions set metadata = metadata || $2, updated_at = now() where execution_id = $1",
        )
        .bind(execution_id)
        .bind(metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn active_execution_for_thread(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            from session_executions
            where thread_key = $1 and status in ($2, $3)
            order by created_at desc, execution_id desc
            limit 1
            "#,
        )
        .bind(thread_key.as_str())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    /// Atomically fences an input write to one active execution and its full
    /// trace-consent boundary. The runtime performs this conditional touch
    /// immediately after console has claimed the actor consent and before it
    /// writes to the sandbox pipe.
    pub async fn claim_active_trace_input(
        &self,
        execution_id: &str,
        capabilities: &SandboxCapabilities,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_executions
            set updated_at = now()
            where execution_id = $1
              and status in ($2, $3)
              and coalesce((metadata ->> 'metadata_trace_enabled')::boolean, false) = $4
              and (metadata ->> 'metadata_trace_subject_hash') is not distinct from $5
              and (metadata ->> 'metadata_trace_consent_revision')::bigint is not distinct from $6
              and (metadata ->> 'metadata_trace_expires_at')::timestamptz is not distinct from $7
              and (metadata ->> 'metadata_trace_config_fingerprint') is not distinct from $8
              and (metadata ->> 'metadata_trace_config_generation')::bigint is not distinct from $9
              and (
                    not $4
                    or (
                        (metadata ->> 'metadata_trace_expires_at')::timestamptz > now()
                        and exists (
                            select 1 from metadata_trace_config_state
                            where singleton = true
                              and generation = $9
                              and config_fingerprint = $8
                        )
                    )
              )
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(capabilities.metadata_trace_enabled)
        .bind(&capabilities.metadata_trace_subject_hash)
        .bind(capabilities.metadata_trace_consent_revision)
        .bind(capabilities.metadata_trace_expires_at)
        .bind(&capabilities.metadata_trace_config_fingerprint)
        .bind(capabilities.metadata_trace_config_generation)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Lists every execution still marked queued or running. Used at startup
    /// to adopt executions orphaned by a previous control plane process.
    pub async fn list_active_executions(&self) -> Result<Vec<SessionExecution>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            from session_executions
            where status in ($1, $2)
            order by created_at, execution_id
            "#,
        )
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_active_executions_with_ownership(
        &self,
    ) -> Result<Vec<ActiveExecutionOwnership>, SessionStoreError> {
        let rows = sqlx::query_as::<_, ActiveExecutionOwnershipRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at,
                   stdout_owner_id,
                   coalesce(stdout_owner_lease_expires_at > clock_timestamp(), false) as stdout_owner_lease_active
            from session_executions
            where status in ($1, $2)
            order by created_at, execution_id
            "#,
        )
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveExecutionOwnership {
                    execution: row.execution.try_into()?,
                    stdout_owner_id: row.stdout_owner_id,
                    stdout_owner_lease_active: row.stdout_owner_lease_active,
                })
            })
            .collect()
    }

    pub async fn latest_execution_for_thread(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            from session_executions
            where thread_key = $1
            order by created_at desc, execution_id desc
            limit 1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub async fn mark_execution_running(
        &self,
        execution_id: &str,
    ) -> Result<ClaimExecutionResult, SessionStoreError> {
        let maybe_row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, started_at = coalesce(started_at, now()), updated_at = now()
            where execution_id = $1 and status = $3
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Running.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = maybe_row else {
            // The execution was not queued: a concurrent request already
            // claimed it or it reached a terminal state. Report the current
            // row without taking ownership.
            let row = sqlx::query_as::<_, SessionExecutionRow>(
                r#"
                select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
                from session_executions
                where execution_id = $1
                "#,
            )
            .bind(execution_id)
            .fetch_one(&self.pool)
            .await?;
            return Ok(ClaimExecutionResult {
                execution: row.try_into()?,
                claimed: false,
            });
        };

        self.set_session_status(&row.thread_key, SessionStatus::Executing)
            .await?;
        Ok(ClaimExecutionResult {
            execution: row.try_into()?,
            claimed: true,
        })
    }

    /// Claims an unowned lease or extends this owner's still-live lease.
    /// Ordinary claims never revive an expired owner lease.
    pub async fn claim_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, SessionStoreError> {
        let lease_seconds = lease.as_secs_f64();
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_id = $2,
                stdout_owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and (
                stdout_owner_id is null
                or (
                    stdout_owner_id = $2
                    and stdout_owner_lease_expires_at > clock_timestamp()
                )
                or (
                    stdout_owner_id <> $2
                    and (
                        stdout_owner_lease_expires_at is null
                        or stdout_owner_lease_expires_at <= clock_timestamp()
                    )
                )
              )
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_seconds)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Claims an unowned or expired lease for recovery adoption. A runtime's
    /// local output gate makes reclaiming its own expired lease safe here;
    /// ordinary output writes must still prove a live lease independently.
    pub async fn claim_expired_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, SessionStoreError> {
        let lease_seconds = lease.as_secs_f64();
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_id = $2,
                stdout_owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and (
                stdout_owner_id is null
                or stdout_owner_lease_expires_at is null
                or stdout_owner_lease_expires_at <= clock_timestamp()
              )
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_seconds)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn renew_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, SessionStoreError> {
        let lease_seconds = lease.as_secs_f64();
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                updated_at = now()
            where execution_id = $1
              and stdout_owner_id = $2
              and stdout_owner_lease_expires_at > clock_timestamp()
              and status in ($4, $5)
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_seconds)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn release_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where execution_id = $1 and stdout_owner_id = $2
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn count_executions_with_stdout_owner(
        &self,
        owner_id: &str,
    ) -> Result<u64, SessionStoreError> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            select count(*)
            from session_executions
            where stdout_owner_id = $1 and status in ($2, $3)
            "#,
        )
        .bind(owner_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_one(&self.pool)
        .await?;

        Ok(u64::try_from(count).unwrap_or_default())
    }

    /// Releases every active stdout-owner lease held by `owner_id` in one
    /// statement, returning the affected executions. Used by a clean
    /// control-plane shutdown so a peer's adoption scan can claim the
    /// executions immediately instead of waiting out the lease TTL.
    pub async fn release_stdout_owned_executions(
        &self,
        owner_id: &str,
    ) -> Result<Vec<ReleasedExecution>, SessionStoreError> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            update session_executions
            set stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where stdout_owner_id = $1 and status in ($2, $3)
            returning execution_id, thread_key
            "#,
        )
        .bind(owner_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|(execution_id, thread_key)| {
                Ok(ReleasedExecution {
                    execution_id,
                    thread_key: parse_persisted(thread_key)?,
                })
            })
            .collect()
    }

    pub async fn complete_execution(
        &self,
        execution_id: &str,
    ) -> Result<SessionExecution, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Completed.as_ref())
        .fetch_one(&self.pool)
        .await?;

        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into()
    }

    pub async fn complete_execution_if_active(
        &self,
        execution_id: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1 and status in ($3, $4)
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Completed.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn fail_execution(
        &self,
        execution_id: &str,
        error: &str,
    ) -> Result<SessionExecution, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Failed.as_ref())
        .bind(error)
        .fetch_one(&self.pool)
        .await?;

        self.set_session_status(&row.thread_key, SessionStatus::Failed)
            .await?;
        row.try_into()
    }

    pub async fn fail_execution_if_active(
        &self,
        execution_id: &str,
        error: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1 and status in ($4, $5)
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Failed.as_ref())
        .bind(error)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Failed)
            .await?;
        row.try_into().map(Some)
    }

    /// Records an owner-fenced terminal execution transition and its durable
    /// lifecycle event together. A rejected owner fence produces no state or
    /// event change; an event write failure rolls the transition back.
    pub async fn terminalize_execution_and_append_event_if_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        terminal: OwnedTerminalEvent,
    ) -> Result<Option<(SessionExecution, SessionEvent)>, SessionStoreError> {
        let (terminal_status, session_status, error, event_type, payload) = terminal.into_parts();

        let mut tx = self.pool.begin().await?;
        let execution_row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2,
                error = $3,
                completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and stdout_owner_id = $6
              and stdout_owner_lease_expires_at > clock_timestamp()
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(terminal_status.as_ref())
        .bind(&error)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(owner_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(execution_row) = execution_row else {
            tx.commit().await?;
            return Ok(None);
        };

        // The partial one-active-execution index makes a successor insert
        // wait for this transaction once this row leaves its active state.
        // Updating the session before commit therefore cannot overwrite a
        // successor's `executing` status; the extra predicate preserves that
        // invariant even if historical data somehow violates the index.
        let execution_thread_key = execution_row.thread_key.clone();

        sqlx::query(
            r#"
            update sessions as s
            set status = $2, updated_at = now()
            where s.thread_key = $1
              and not exists (
                  select 1
                  from session_executions active
                  where active.thread_key = s.thread_key
                    and active.status in ($3, $4)
              )
            "#,
        )
        .bind(&execution_thread_key)
        .bind(session_status.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&mut *tx)
        .await?;

        let event_row = match sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(&execution_thread_key)
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .fetch_one(&mut *tx)
        .await
        {
            Ok(row) => row,
            Err(error) => {
                tx.rollback().await?;
                return Err(error.into());
            }
        };

        // Convert while the transaction is still open so malformed persisted
        // data cannot leave a terminal state committed without a usable event.
        let execution = execution_row.try_into()?;
        let event = event_row.try_into()?;
        tx.commit().await?;
        Ok(Some((execution, event)))
    }

    pub async fn append_event(
        &self,
        thread_key: &ThreadKey,
        execution_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) -> Result<SessionEvent, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn append_event_if_stdout_owner(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
        event_type: &str,
        payload: Value,
    ) -> Result<Option<SessionEvent>, SessionStoreError> {
        let lease_seconds = lease.as_secs_f64();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                updated_at = now()
            where execution_id = $1
              and stdout_owner_id = $2
              and stdout_owner_lease_expires_at > clock_timestamp()
              and status in ($4, $5)
              and thread_key = $6
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_seconds)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(thread_key.as_str())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(None);
        }

        let row = sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        row.try_into().map(Some)
    }

    pub async fn list_events_after(
        &self,
        thread_key: &ThreadKey,
        after_event_id: i64,
        execution_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<SessionEvent>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionEventRow>(
            r#"
            select event_id, thread_key, execution_id, event_type, payload, created_at
            from session_events
            where thread_key = $1
              and event_id > $2
              and ($3::text is null or execution_id = $3)
            order by event_id
            limit $4
            "#,
        )
        .bind(thread_key.as_str())
        .bind(after_event_id)
        .bind(execution_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn execution_event_exists(
        &self,
        execution_id: &str,
        event_type: &str,
    ) -> Result<bool, SessionStoreError> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (
                select 1
                from session_events
                where execution_id = $1
                  and event_type = $2
                limit 1
            )
            "#,
        )
        .bind(execution_id)
        .bind(event_type)
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }

    pub async fn list_referenced_sandbox_ids(&self) -> Result<Vec<String>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            select sandbox_id
            from sessions
            where sandbox_id is not null

            union

            select sandbox_id
            from session_warm_sandboxes
            where status in ('ready', 'claimed', 'evicting')
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn list_principal_sandbox_sessions(&self) -> Result<Vec<Session>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionRow>(
            r#"
            select thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            from sessions
            where sandbox_id is not null
              and (
                    iron_control_principal is not null
                    or sandbox_metadata_trace_enabled is true
              )
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_idle_sandbox_candidates(
        &self,
        idle_backstop: Duration,
    ) -> Result<Vec<IdleSandboxCandidate>, SessionStoreError> {
        let rows = sqlx::query_as::<_, IdleSandboxCandidateRow>(
            r#"
            with latest as (
                select distinct on (thread_key)
                    execution_id,
                    thread_key,
                    status,
                    completed_at,
                    metadata
                from session_executions
                order by thread_key, created_at desc, execution_id desc
            )
            select
                s.thread_key,
                s.sandbox_id as sandbox_id,
                latest.execution_id,
                latest.completed_at,
                latest.metadata
            from sessions s
            join latest on latest.thread_key = s.thread_key
            where s.sandbox_id is not null
              and latest.status in ('completed', 'failed', 'cancelled')
              and latest.completed_at is not null
              and not exists (
                  select 1
                  from session_executions active
                  where active.thread_key = s.thread_key
                    and active.status in ('queued', 'running')
              )
            order by latest.completed_at, s.thread_key
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let now = OffsetDateTime::now_utc();
        rows.into_iter()
            .filter_map(|row| idle_candidate_from_row(row, idle_backstop, now).transpose())
            .collect()
    }

    pub async fn list_sandbox_capacity_candidates(
        &self,
        excluded_thread_key: Option<&ThreadKey>,
        hot_idle_grace: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<SandboxCapacityCandidate>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SandboxCapacityCandidateRow>(
            r#"
            with latest as (
                select distinct on (thread_key)
                    execution_id,
                    thread_key,
                    completed_at
                from session_executions
                order by thread_key, created_at desc, execution_id desc
            )
            select
                s.thread_key,
                s.sandbox_id as sandbox_id,
                latest.execution_id as latest_execution_id,
                coalesce(
                    s.sandbox_last_active_at,
                    latest.completed_at,
                    s.updated_at,
                    s.created_at
                ) as last_active_at
            from sessions s
            left join latest on latest.thread_key = s.thread_key
            where s.sandbox_id is not null
              and ($1::text is null or s.thread_key != $1)
              and not exists (
                  select 1
                  from lateral (
                      select e.event_type
                      from session_events e
                      where e.thread_key = s.thread_key
                        and e.payload->>'sandbox_id' = s.sandbox_id
                        and e.event_type in (
                            'session.sandbox_paused',
                            'session.sandbox_ready',
                            'session.sandbox_resumed'
                        )
                      order by e.created_at desc, e.event_id desc
                      limit 1
                  ) latest_sandbox_event
                  where latest_sandbox_event.event_type = 'session.sandbox_paused'
              )
              and coalesce(
                    s.sandbox_last_active_at,
                    latest.completed_at,
                    s.updated_at,
                    s.created_at
                  ) <= now() - ($2::float8 * interval '1 second')
              and not exists (
                  select 1
                  from session_executions active
                  where active.thread_key = s.thread_key
                    and active.status in ('queued', 'running')
              )
            order by last_active_at, s.thread_key
            limit $3
            "#,
        )
        .bind(excluded_thread_key.map(ThreadKey::as_str))
        .bind(hot_idle_grace.as_secs_f64())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_workflow_owned_sandboxes(
        &self,
        workflow_run_id: &str,
    ) -> Result<Vec<WorkflowOwnedSandbox>, SessionStoreError> {
        let rows = sqlx::query_as::<_, WorkflowOwnedSandboxRow>(
            r#"
            select thread_key, sandbox_id as sandbox_id
            from sessions
            where sandbox_id is not null
              and metadata->>'workflow_owned_thread' = 'true'
              and metadata->>'workflow_run_id' = $1
            order by thread_key
            "#,
        )
        .bind(workflow_run_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn update_sandbox_id(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: Option<&str>,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set
                sandbox_id = $2,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_api_server_enabled = null,
                sandbox_metadata_trace_enabled = null,
                sandbox_metadata_trace_expires_at = null,
                sandbox_metadata_trace_subject_hash = null,
                sandbox_metadata_trace_source = null,
                sandbox_metadata_trace_workspace_id = null,
                sandbox_metadata_trace_user_id = null,
                sandbox_metadata_trace_consent_revision = null,
                sandbox_metadata_trace_config_fingerprint = null,
                sandbox_metadata_trace_config_generation = null,
                sandbox_metadata_trace_resource_uid = null,
                sandbox_last_active_at = case
                    when $2::text is null then null
                    else now()
                end,
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn update_sandbox_assignment(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        capabilities: &SandboxCapabilities,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set
                sandbox_id = $2,
                sandbox_repo_cache_enabled = $3,
                sandbox_repo_cache_access = $4,
                sandbox_observability_enabled = $5,
                sandbox_api_server_enabled = $6,
                sandbox_metadata_trace_enabled = $7,
                sandbox_metadata_trace_expires_at = $8,
                sandbox_metadata_trace_subject_hash = $9,
                sandbox_metadata_trace_source = null,
                sandbox_metadata_trace_workspace_id = null,
                sandbox_metadata_trace_user_id = null,
                sandbox_metadata_trace_consent_revision = $10,
                sandbox_metadata_trace_config_fingerprint = $11,
                sandbox_metadata_trace_config_generation = $12,
                sandbox_metadata_trace_assignment_epoch = case when $7 then md5(random()::text || clock_timestamp()::text) else null end,
                sandbox_last_active_at = now(),
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .bind(capabilities.repo_cache_enabled())
        .bind(capabilities.repo_cache.as_str())
        .bind(capabilities.observability_enabled)
        .bind(capabilities.api_server_enabled)
        .bind(capabilities.metadata_trace_enabled)
        .bind(capabilities.metadata_trace_expires_at)
        .bind(&capabilities.metadata_trace_subject_hash)
        .bind(capabilities.metadata_trace_consent_revision)
        .bind(&capabilities.metadata_trace_config_fingerprint)
        .bind(capabilities.metadata_trace_config_generation)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn update_sandbox_assignment_if_metadata_trace_config_active(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        capabilities: &SandboxCapabilities,
        identity: &MetadataTraceConfigIdentity,
        expected_sandbox_id: Option<&str>,
        workspace_id: &str,
        user_id: &str,
        resource_uid: &str,
    ) -> Result<bool, SessionStoreError> {
        let mut transaction = self.pool.begin().await?;
        let consent = sqlx::query_scalar::<_, bool>(
            r#"
            select enabled and not drain_pending and expires_at > clock_timestamp()
                   and revision = $3
            from metadata_trace_consents
            where source = 'slack' and workspace_id = $1 and user_id = $2
            for share
        "#,
        )
        .bind(workspace_id)
        .bind(user_id)
        .bind(capabilities.metadata_trace_consent_revision)
        .fetch_optional(&mut *transaction)
        .await?;
        if consent != Some(true) {
            transaction.rollback().await?;
            return Ok(false);
        }
        let config_active = sqlx::query_scalar::<_, bool>(
            "select generation = $1 and config_fingerprint = $2 from metadata_trace_config_state where singleton = true for share",
        )
        .bind(identity.generation)
        .bind(&identity.fingerprint)
        .fetch_optional(&mut *transaction)
        .await?;
        if config_active != Some(true) {
            transaction.rollback().await?;
            return Ok(false);
        }
        let result = sqlx::query(
            r#"
            update sessions
            set sandbox_id = $2,
                sandbox_repo_cache_enabled = $3,
                sandbox_repo_cache_access = $4,
                sandbox_observability_enabled = $5,
                sandbox_api_server_enabled = $6,
                sandbox_metadata_trace_enabled = $7,
                sandbox_metadata_trace_expires_at = $8,
                sandbox_metadata_trace_subject_hash = $9,
                sandbox_metadata_trace_source = 'slack',
                sandbox_metadata_trace_workspace_id = $10,
                sandbox_metadata_trace_user_id = $11,
                sandbox_metadata_trace_consent_revision = $12,
                sandbox_metadata_trace_config_fingerprint = $13,
                sandbox_metadata_trace_config_generation = $14,
                sandbox_metadata_trace_resource_uid = $15,
                sandbox_metadata_trace_assignment_epoch = md5(random()::text || clock_timestamp()::text),
                sandbox_last_active_at = now(),
                updated_at = now()
            where thread_key = $1
              and exists (
                    select 1 from metadata_trace_config_state
                    where singleton = true
                      and generation = $16
                      and config_fingerprint = $17
              )
              and sandbox_id is not distinct from $18
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .bind(capabilities.repo_cache_enabled())
        .bind(capabilities.repo_cache.as_str())
        .bind(capabilities.observability_enabled)
        .bind(capabilities.api_server_enabled)
        .bind(capabilities.metadata_trace_enabled)
        .bind(capabilities.metadata_trace_expires_at)
        .bind(&capabilities.metadata_trace_subject_hash)
        .bind(workspace_id)
        .bind(user_id)
        .bind(capabilities.metadata_trace_consent_revision)
        .bind(&capabilities.metadata_trace_config_fingerprint)
        .bind(capabilities.metadata_trace_config_generation)
        .bind(resource_uid)
        .bind(identity.generation)
        .bind(&identity.fingerprint)
        .bind(expected_sandbox_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn clear_sandbox_id_if_matches(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set
                sandbox_id = null,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_api_server_enabled = null,
                sandbox_metadata_trace_enabled = null,
                sandbox_metadata_trace_expires_at = null,
                sandbox_metadata_trace_subject_hash = null,
                sandbox_metadata_trace_source = null,
                sandbox_metadata_trace_workspace_id = null,
                sandbox_metadata_trace_user_id = null,
                sandbox_metadata_trace_consent_revision = null,
                sandbox_metadata_trace_config_fingerprint = null,
                sandbox_metadata_trace_config_generation = null,
                sandbox_metadata_trace_resource_uid = null,
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_last_active_at = null,
                updated_at = now()
            where thread_key = $1 and sandbox_id = $2
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Clear only the exact traced assignment fenced by a revoke. A replacement
    /// sandbox (including a later re-grant) has a different epoch and survives.
    pub async fn clear_metadata_trace_assignment_if_matches(
        &self,
        target: &MetadataTraceDrainTarget,
        source: &str,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set sandbox_id = null,
                sandbox_metadata_trace_enabled = null,
                sandbox_metadata_trace_expires_at = null,
                sandbox_metadata_trace_subject_hash = null,
                sandbox_metadata_trace_source = null,
                sandbox_metadata_trace_workspace_id = null,
                sandbox_metadata_trace_user_id = null,
                sandbox_metadata_trace_consent_revision = null,
                sandbox_metadata_trace_config_fingerprint = null,
                sandbox_metadata_trace_config_generation = null,
                sandbox_metadata_trace_resource_uid = null,
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_last_active_at = null,
                updated_at = now()
            where thread_key = $1 and sandbox_id = $2
              and sandbox_metadata_trace_assignment_epoch = $3
              and sandbox_metadata_trace_source = $4
              and sandbox_metadata_trace_workspace_id = $5
              and sandbox_metadata_trace_user_id = $6
              and sandbox_metadata_trace_consent_revision = $7
              and sandbox_metadata_trace_resource_uid = $8
        "#,
        )
        .bind(target.thread_key.as_str())
        .bind(&target.sandbox_id)
        .bind(&target.assignment_epoch)
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(target.revision)
        .bind(&target.resource_uid)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn complete_metadata_trace_drain_if_empty(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<MetadataTraceConsent, SessionStoreError> {
        let row = sqlx::query_as::<_, MetadataTraceConsentRow>(
            r#"
            update metadata_trace_consents consent
            set drain_pending = false, updated_at = now()
            where source = $1 and workspace_id = $2 and user_id = $3
              and not exists (
                  select 1 from sessions
                  where sandbox_metadata_trace_enabled is true
                    and sandbox_id is not null
                    and (
                        (
                            sandbox_metadata_trace_source = $4
                            and sandbox_metadata_trace_workspace_id = $5
                            and sandbox_metadata_trace_user_id = $6
                        )
                        or sandbox_metadata_trace_assignment_epoch is null
                        or sandbox_metadata_trace_source is null
                        or sandbox_metadata_trace_workspace_id is null
                        or sandbox_metadata_trace_user_id is null
                        or sandbox_metadata_trace_resource_uid is null
                    )
              )
            returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into).unwrap_or_else(|| MetadataTraceConsent {
            source: source.to_owned(),
            workspace_id: workspace_id.to_owned(),
            user_id: user_id.to_owned(),
            enabled: false,
            expires_at: None,
            revision: 0,
            drain_pending: true,
        }))
    }

    /// Acquire the row lock only when the durable assignment still refers to
    /// the sandbox observed by a reconciliation sweep.
    pub async fn lock_sandbox_assignment_for_reconciliation(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<Option<SandboxAssignmentReconciliationLock<'_>>, SessionStoreError> {
        let mut transaction = self.pool.begin().await?;
        let current_assignment = sqlx::query_as::<_, SandboxAssignmentReconciliationLockRow>(
            r#"
            select sandbox_id,
                   sandbox_metadata_trace_enabled is true as metadata_trace_enabled,
                   case when sandbox_metadata_trace_enabled is true
                        then sandbox_metadata_trace_resource_uid
                   end as metadata_trace_resource_uid
            from sessions
            where thread_key = $1
            for update
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&mut *transaction)
        .await?;

        let Some(current_assignment) = current_assignment else {
            transaction.rollback().await?;
            return Ok(None);
        };
        if current_assignment.sandbox_id.as_deref() != Some(sandbox_id) {
            transaction.rollback().await?;
            return Ok(None);
        }

        Ok(Some(SandboxAssignmentReconciliationLock {
            transaction,
            thread_key: thread_key.as_str().to_owned(),
            sandbox_id: sandbox_id.to_owned(),
            metadata_trace_enabled: current_assignment.metadata_trace_enabled,
            metadata_trace_resource_uid: current_assignment.metadata_trace_resource_uid,
        }))
    }

    /// Move an existing session onto a different harness. Clears the sandbox
    /// and harness thread state (they belong to the old harness) and resets
    /// the session to idle; messages and events are preserved.
    pub async fn switch_session_harness(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set harness_type = $2,
                harness_thread_id = null,
                sandbox_id = null,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_api_server_enabled = null,
                sandbox_metadata_trace_enabled = null,
                sandbox_metadata_trace_expires_at = null,
                sandbox_metadata_trace_subject_hash = null,
                sandbox_metadata_trace_source = null,
                sandbox_metadata_trace_workspace_id = null,
                sandbox_metadata_trace_user_id = null,
                sandbox_metadata_trace_consent_revision = null,
                sandbox_metadata_trace_config_fingerprint = null,
                sandbox_metadata_trace_config_generation = null,
                sandbox_metadata_trace_resource_uid = null,
                sandbox_last_active_at = null,
                status = $3,
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(harness_type.as_ref())
        .bind(SessionStatus::Idle.as_ref())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SessionStoreError::NotFound {
            thread_key: thread_key.as_str().to_owned(),
        })?;

        row.try_into()
    }

    pub async fn set_iron_control_principal(
        &self,
        thread_key: &ThreadKey,
        iron_control_principal: Option<&str>,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set iron_control_principal = $2, updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(iron_control_principal)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn insert_ready_warm_sandbox(
        &self,
        sandbox_id: &str,
        workload_key: &str,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            insert into session_warm_sandboxes (sandbox_id, workload_key, status)
            values ($1, $2, 'ready')
            "#,
        )
        .bind(sandbox_id)
        .bind(workload_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_ready_warm_sandboxes(
        &self,
        workload_key: &str,
    ) -> Result<i64, SessionStoreError> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            select count(*)::bigint
            from session_warm_sandboxes
            where workload_key = $1 and status = 'ready'
            "#,
        )
        .bind(workload_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn list_ready_warm_sandbox_ids(&self) -> Result<Vec<String>, SessionStoreError> {
        let sandbox_ids = sqlx::query_scalar::<_, String>(
            r#"
            select sandbox_id
            from session_warm_sandboxes
            where status = 'ready'
            order by created_at, sandbox_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(sandbox_ids)
    }

    pub async fn claim_ready_warm_sandbox(
        &self,
        workload_key: &str,
        thread_key: &str,
    ) -> Result<Option<String>, SessionStoreError> {
        let sandbox_id = sqlx::query_scalar::<_, String>(
            r#"
            with candidate as (
                select sandbox_id
                from session_warm_sandboxes
                where workload_key = $1 and status = 'ready'
                order by created_at, sandbox_id
                for update skip locked
                limit 1
            )
            update session_warm_sandboxes warm
            set
                status = 'claimed',
                claimed_thread_key = $2,
                claimed_at = now(),
                updated_at = now()
            from candidate
            where warm.sandbox_id = candidate.sandbox_id
            returning warm.sandbox_id
            "#,
        )
        .bind(workload_key)
        .bind(thread_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(sandbox_id)
    }

    pub async fn reserve_ready_warm_sandboxes_for_eviction(
        &self,
        limit: i64,
    ) -> Result<Vec<String>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            with candidates as (
                select sandbox_id
                from session_warm_sandboxes
                where status = 'ready'
                order by created_at, sandbox_id
                for update skip locked
                limit $1
            )
            update session_warm_sandboxes warm
            set
                status = 'evicting',
                updated_at = now()
            from candidates
            where warm.sandbox_id = candidates.sandbox_id
            returning warm.sandbox_id
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn list_stale_evicting_warm_sandbox_ids(
        &self,
        min_age: Duration,
    ) -> Result<Vec<String>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            select sandbox_id
            from session_warm_sandboxes
            where status = 'evicting'
              and updated_at <= now() - ($1::float8 * interval '1 second')
            order by updated_at, sandbox_id
            "#,
        )
        .bind(min_age.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn mark_warm_sandbox_failed(
        &self,
        sandbox_id: &str,
        error: &str,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            update session_warm_sandboxes
            set status = 'failed', last_error = $2, updated_at = now()
            where sandbox_id = $1
            "#,
        )
        .bind(sandbox_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_harness_thread_id(
        &self,
        thread_key: &ThreadKey,
        harness_thread_id: Option<&str>,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set harness_thread_id = $2, updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(harness_thread_id)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    /// Persists stdout-derived harness state only while the caller still owns
    /// the execution's live stdout lease. This prevents an expired pump from
    /// overwriting the root recovered by an adopting control plane.
    pub async fn update_harness_thread_id_if_stdout_owner(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        owner_id: &str,
        harness_thread_id: Option<&str>,
    ) -> Result<Option<Session>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            with owned_execution as materialized (
                select thread_key, stdout_owner_lease_expires_at
                from session_executions
                where execution_id = $2
                  and stdout_owner_id = $3
                  and status in ($5, $6)
                for update
            )
            update sessions as session
            set harness_thread_id = $4,
                updated_at = now()
            from owned_execution as execution
            where session.thread_key = $1
              and execution.thread_key = session.thread_key
              and execution.stdout_owner_lease_expires_at > clock_timestamp()
            returning session.thread_key, session.title, session.sandbox_id, session.sandbox_repo_cache_enabled, session.sandbox_repo_cache_access, session.sandbox_observability_enabled, session.sandbox_api_server_enabled, session.harness_type, session.harness_thread_id, session.persona_id, session.status, session.iron_control_principal, session.proxy_labels, session.sandbox_last_active_at, session.created_at, session.updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(execution_id)
        .bind(owner_id)
        .bind(harness_thread_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub async fn touch_session_sandbox_activity(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set sandbox_last_active_at = now()
            where thread_key = $1 and sandbox_id is not null
            "#,
        )
        .bind(thread_key.as_str())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn touch_sandbox_activity(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set sandbox_last_active_at = now()
            where thread_key = $1 and sandbox_id = $2
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn set_session_status(
        &self,
        thread_key: &str,
        status: SessionStatus,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            update sessions
            set status = $2, updated_at = now()
            where thread_key = $1
            "#,
        )
        .bind(thread_key)
        .bind(status.as_ref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

pub struct SessionEventListener {
    listener: PgListener,
}

impl SessionEventListener {
    pub async fn recv(&mut self) -> Result<SessionEventNotification, SessionStoreError> {
        loop {
            let notification = self.listener.recv().await?;
            if notification.channel() != SESSION_EVENTS_CHANNEL {
                continue;
            }

            let payload = notification.payload();
            return serde_json::from_str(payload).map_err(|error| {
                SessionStoreError::InvalidNotification {
                    channel: notification.channel().to_owned(),
                    payload: payload.to_owned(),
                    error,
                }
            });
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SessionEventNotification {
    pub thread_key: String,
    pub event_id: i64,
}

#[derive(Debug, Error)]
pub enum SessionStoreError {
    #[error("session not found for thread_key {thread_key}")]
    NotFound { thread_key: String },
    #[error(
        "session {thread_key} already exists with harness_type {existing}, requested {requested}"
    )]
    HarnessConflict {
        thread_key: String,
        existing: String,
        requested: String,
    },
    #[error(
        "session {thread_key} already exists with persona_id {existing:?}, requested {requested:?}"
    )]
    PersonaConflict {
        thread_key: String,
        existing: Option<String>,
        requested: Option<String>,
    },
    #[error(
        "session {thread_key} already exists with iron_control_principal {existing:?}, requested {requested:?}"
    )]
    PrincipalConflict {
        thread_key: String,
        existing: Option<String>,
        requested: Option<String>,
    },
    #[error("invalid persisted value: {0}")]
    InvalidPersistedValue(String),
    #[error("idempotency key was already used for a different metadata trace consent request")]
    MetadataTraceIdempotencyConflict,
    #[error("metadata trace consent request is incomplete and cannot be safely replayed")]
    MetadataTraceIdempotencyIncomplete,
    #[error("metadata trace consent is waiting for an earlier sandbox drain")]
    MetadataTraceDrainPending,
    #[error("metadata trace generation must be positive, got {0}")]
    InvalidMetadataTraceGeneration(i64),
    #[error("metadata trace generation {requested} is stale; active generation is {active}")]
    StaleMetadataTraceGeneration { active: i64, requested: i64 },
    #[error("metadata trace generation {generation} has conflicting fingerprints")]
    MetadataTraceConfigConflict {
        generation: i64,
        existing_fingerprint: String,
        requested_fingerprint: String,
    },
    #[error("invalid notification payload on {channel}: {payload}: {error}")]
    InvalidNotification {
        channel: String,
        payload: String,
        error: serde_json::Error,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

#[derive(Debug, FromRow)]
struct MetadataTraceConfigStateRow {
    generation: i64,
    config_fingerprint: String,
}

#[derive(Debug, FromRow)]
struct MetadataTraceLeaseRow {
    reconciler_owner_id: String,
    reconciler_fence: i64,
}

#[derive(Debug, FromRow)]
struct SessionRow {
    thread_key: String,
    title: Option<String>,
    sandbox_id: Option<String>,
    sandbox_repo_cache_enabled: Option<bool>,
    sandbox_repo_cache_access: Option<String>,
    sandbox_observability_enabled: Option<bool>,
    sandbox_api_server_enabled: Option<bool>,
    sandbox_metadata_trace_enabled: Option<bool>,
    sandbox_metadata_trace_expires_at: Option<OffsetDateTime>,
    sandbox_metadata_trace_subject_hash: Option<String>,
    sandbox_metadata_trace_consent_revision: Option<i64>,
    sandbox_metadata_trace_config_fingerprint: Option<String>,
    sandbox_metadata_trace_config_generation: Option<i64>,
    harness_type: String,
    harness_thread_id: Option<String>,
    persona_id: Option<String>,
    status: String,
    iron_control_principal: Option<String>,
    proxy_labels: Json<BTreeMap<String, String>>,
    sandbox_last_active_at: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl TryFrom<SessionRow> for Session {
    type Error = SessionStoreError;

    fn try_from(row: SessionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            title: row.title,
            sandbox_id: row.sandbox_id,
            sandbox_capabilities: match (
                row.sandbox_repo_cache_enabled,
                row.sandbox_repo_cache_access,
                row.sandbox_observability_enabled,
                row.sandbox_api_server_enabled,
                row.sandbox_metadata_trace_enabled,
                row.sandbox_metadata_trace_expires_at,
                row.sandbox_metadata_trace_subject_hash,
                row.sandbox_metadata_trace_consent_revision,
                row.sandbox_metadata_trace_config_fingerprint,
                row.sandbox_metadata_trace_config_generation,
            ) {
                (
                    Some(repo_cache_enabled),
                    repo_cache_access,
                    Some(observability_enabled),
                    Some(api_server_enabled),
                    metadata_trace_enabled,
                    metadata_trace_expires_at,
                    metadata_trace_subject_hash,
                    metadata_trace_consent_revision,
                    metadata_trace_config_fingerprint,
                    metadata_trace_config_generation,
                ) => Some(SandboxCapabilities {
                    repo_cache: repo_cache_access
                        .as_deref()
                        .and_then(SandboxRepoCacheAccess::parse)
                        .unwrap_or_else(|| {
                            SandboxRepoCacheAccess::from_legacy_enabled(repo_cache_enabled)
                        }),
                    observability_enabled,
                    api_server_enabled,
                    metadata_trace_enabled: metadata_trace_enabled.unwrap_or(false),
                    metadata_trace_expires_at: metadata_trace_enabled
                        .unwrap_or(false)
                        .then_some(metadata_trace_expires_at)
                        .flatten(),
                    metadata_trace_subject_hash: metadata_trace_enabled
                        .unwrap_or(false)
                        .then_some(metadata_trace_subject_hash)
                        .flatten(),
                    metadata_trace_consent_revision: metadata_trace_enabled
                        .unwrap_or(false)
                        .then_some(metadata_trace_consent_revision)
                        .flatten(),
                    metadata_trace_config_fingerprint: metadata_trace_enabled
                        .unwrap_or(false)
                        .then_some(metadata_trace_config_fingerprint)
                        .flatten(),
                    metadata_trace_config_generation: metadata_trace_enabled
                        .unwrap_or(false)
                        .then_some(metadata_trace_config_generation)
                        .flatten(),
                }),
                _ => None,
            },
            harness_type: parse_persisted(row.harness_type)?,
            harness_thread_id: row.harness_thread_id,
            persona_id: row.persona_id,
            status: parse_persisted(row.status)?,
            iron_control_principal: row.iron_control_principal,
            proxy_labels: row.proxy_labels.0,
            sandbox_last_active_at: row.sandbox_last_active_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct SessionMessageRow {
    message_id: String,
    client_message_id: Option<String>,
    thread_key: String,
    role: String,
    parts: Value,
    metadata: Value,
    created_at: OffsetDateTime,
}

impl TryFrom<SessionMessageRow> for SessionMessage {
    type Error = SessionStoreError;

    fn try_from(row: SessionMessageRow) -> Result<Self, Self::Error> {
        let parts = match row.parts {
            Value::Array(parts) => parts,
            other => vec![other],
        };
        Ok(Self {
            message_id: row.message_id,
            client_message_id: row.client_message_id,
            thread_key: parse_persisted(row.thread_key)?,
            role: parse_persisted(row.role)?,
            parts,
            metadata: row.metadata,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct SessionExecutionRow {
    execution_id: String,
    idempotency_key: Option<String>,
    thread_key: String,
    status: String,
    metadata: Value,
    error: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct ActiveExecutionOwnershipRow {
    #[sqlx(flatten)]
    execution: SessionExecutionRow,
    stdout_owner_id: Option<String>,
    stdout_owner_lease_active: bool,
}

#[derive(Debug, FromRow)]
struct IdleSandboxCandidateRow {
    thread_key: String,
    sandbox_id: String,
    execution_id: String,
    completed_at: OffsetDateTime,
    metadata: Value,
}

fn idle_candidate_from_row(
    row: IdleSandboxCandidateRow,
    idle_backstop: Duration,
    now: OffsetDateTime,
) -> Result<Option<IdleSandboxCandidate>, SessionStoreError> {
    let idle_timeout = effective_idle_timeout(&row.metadata, idle_backstop);
    if !idle_deadline_elapsed(row.completed_at, idle_timeout, now) {
        return Ok(None);
    }
    Ok(Some(IdleSandboxCandidate {
        thread_key: parse_persisted(row.thread_key)?,
        sandbox_id: row.sandbox_id,
        execution_id: row.execution_id,
        idle_timeout,
    }))
}

fn effective_idle_timeout(metadata: &Value, idle_backstop: Duration) -> Duration {
    metadata
        .get("idle_timeout_ms")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| std::cmp::max(idle_backstop, Duration::from_millis(1)))
}

fn idle_deadline_elapsed(
    completed_at: OffsetDateTime,
    idle_timeout: Duration,
    now: OffsetDateTime,
) -> bool {
    let elapsed = now - completed_at;
    if elapsed.is_negative() {
        return false;
    }
    elapsed.whole_nanoseconds() >= idle_timeout.as_nanos() as i128
}

#[derive(Debug, FromRow)]
struct SandboxCapacityCandidateRow {
    thread_key: String,
    sandbox_id: String,
    latest_execution_id: Option<String>,
    last_active_at: OffsetDateTime,
}

impl TryFrom<SandboxCapacityCandidateRow> for SandboxCapacityCandidate {
    type Error = SessionStoreError;

    fn try_from(row: SandboxCapacityCandidateRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
            latest_execution_id: row.latest_execution_id,
            last_active_at: row.last_active_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct WorkflowOwnedSandboxRow {
    thread_key: String,
    sandbox_id: String,
}

impl TryFrom<WorkflowOwnedSandboxRow> for WorkflowOwnedSandbox {
    type Error = SessionStoreError;

    fn try_from(row: WorkflowOwnedSandboxRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
        })
    }
}

impl TryFrom<SessionExecutionRow> for SessionExecution {
    type Error = SessionStoreError;

    fn try_from(row: SessionExecutionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            execution_id: row.execution_id,
            idempotency_key: row.idempotency_key,
            thread_key: parse_persisted(row.thread_key)?,
            status: parse_persisted(row.status)?,
            metadata: row.metadata,
            error: row.error,
            created_at: row.created_at,
            updated_at: row.updated_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct CreateExecutionRow {
    created: bool,
    execution_id: String,
    idempotency_key: Option<String>,
    thread_key: String,
    status: String,
    metadata: Value,
    error: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
}

impl TryFrom<CreateExecutionRow> for CreateExecutionResult {
    type Error = SessionStoreError;

    fn try_from(row: CreateExecutionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            created: row.created,
            execution: SessionExecutionRow {
                execution_id: row.execution_id,
                idempotency_key: row.idempotency_key,
                thread_key: row.thread_key,
                status: row.status,
                metadata: row.metadata,
                error: row.error,
                created_at: row.created_at,
                updated_at: row.updated_at,
                started_at: row.started_at,
                completed_at: row.completed_at,
            }
            .try_into()?,
        })
    }
}

#[derive(Debug, FromRow)]
struct SessionEventRow {
    event_id: i64,
    thread_key: String,
    execution_id: Option<String>,
    event_type: String,
    payload: Value,
    created_at: OffsetDateTime,
}

impl TryFrom<SessionEventRow> for SessionEvent {
    type Error = SessionStoreError;

    fn try_from(row: SessionEventRow) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: row.event_id,
            thread_key: parse_persisted(row.thread_key)?,
            execution_id: row.execution_id,
            event_type: row.event_type,
            payload: row.payload,
            created_at: row.created_at,
        })
    }
}

fn parse_persisted<T>(value: String) -> Result<T, SessionStoreError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|err: T::Err| SessionStoreError::InvalidPersistedValue(err.to_string()))
}

fn prefixed_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub fn default_metadata(metadata: Option<Value>) -> Value {
    metadata.unwrap_or_else(empty_object)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use centaur_session_core::{
        ExecutionStatus, HarnessType, SandboxCapabilities, Session, SessionStatus, ThreadKey,
    };
    use serde_json::json;
    use time::{Duration as TimeDuration, OffsetDateTime};
    use uuid::Uuid;

    static TRACE_CONFIG_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    use super::{
        IdleSandboxCandidateRow, MetadataTraceAssignmentActor, MetadataTraceConfigIdentity,
        OwnedTerminalEvent, PgSessionStore, SessionEventNotification, SessionRow,
        SessionStoreError,
    };

    async fn test_store() -> Option<PgSessionStore> {
        let Ok(url) = std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL") else {
            eprintln!("skipping: SESSION_RUNTIME_TEST_DATABASE_URL not set");
            return None;
        };
        let store = PgSessionStore::connect(&url)
            .await
            .expect("connect test db");
        store.run_migrations().await.expect("run migrations");
        Some(store)
    }

    async fn running_execution_with_stdout_owner(
        store: &PgSessionStore,
        label: &str,
        owner_id: &str,
    ) -> (ThreadKey, String) {
        let thread_key =
            ThreadKey::parse(format!("test:stdout-owner-{label}-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&execution_id)
            .await
            .expect("mark running");
        assert!(
            store
                .claim_stdout_owner(&execution_id, owner_id, Duration::from_secs(60))
                .await
                .expect("claim stdout owner")
        );
        (thread_key, execution_id)
    }

    async fn expire_stdout_owner(store: &PgSessionStore, execution_id: &str) {
        sqlx::query(
            r#"
            update session_executions
            set stdout_owner_lease_expires_at = clock_timestamp() - interval '1 second',
                updated_at = now()
            where execution_id = $1
            "#,
        )
        .bind(execution_id)
        .execute(store.pool())
        .await
        .expect("expire stdout-owner lease");
    }

    async fn assert_execution_still_active(store: &PgSessionStore, thread_key: &ThreadKey) {
        let execution = store
            .active_execution_for_thread(thread_key)
            .await
            .expect("read active execution")
            .expect("expired owner must not terminalize execution");
        assert_eq!(
            execution.status,
            centaur_session_core::ExecutionStatus::Running
        );
    }

    async fn cleanup_active_execution(store: &PgSessionStore, execution_id: &str) {
        assert!(
            store
                .fail_execution_if_active(execution_id, "test cleanup")
                .await
                .expect("terminalize test execution")
                .is_some(),
            "test fixture must still be active during cleanup"
        );
    }

    async fn terminalize_completed(
        store: &PgSessionStore,
        execution_id: &str,
        owner_id: &str,
    ) -> Result<Option<centaur_session_core::SessionExecution>, SessionStoreError> {
        store
            .terminalize_execution_and_append_event_if_stdout_owner(
                execution_id,
                owner_id,
                OwnedTerminalEvent::Completed {
                    payload: json!({"source": "test"}),
                },
            )
            .await
            .map(|result| result.map(|(execution, _)| execution))
    }

    async fn terminalize_failed(
        store: &PgSessionStore,
        execution_id: &str,
        owner_id: &str,
        error: &str,
    ) -> Result<Option<centaur_session_core::SessionExecution>, SessionStoreError> {
        store
            .terminalize_execution_and_append_event_if_stdout_owner(
                execution_id,
                owner_id,
                OwnedTerminalEvent::Failed {
                    error: error.to_owned(),
                    payload: json!({"error": error}),
                },
            )
            .await
            .map(|result| result.map(|(execution, _)| execution))
    }

    async fn terminalize_cancelled(
        store: &PgSessionStore,
        execution_id: &str,
        owner_id: &str,
        reason: &str,
    ) -> Result<Option<centaur_session_core::SessionExecution>, SessionStoreError> {
        store
            .terminalize_execution_and_append_event_if_stdout_owner(
                execution_id,
                owner_id,
                OwnedTerminalEvent::Cancelled {
                    reason: reason.to_owned(),
                    payload: json!({"reason": reason}),
                },
            )
            .await
            .map(|result| result.map(|(execution, _)| execution))
    }

    async fn next_trace_generation(store: &PgSessionStore) -> i64 {
        let active_generation = sqlx::query_scalar::<_, i64>(
            "select generation from metadata_trace_config_state where singleton = true",
        )
        .fetch_optional(store.pool())
        .await
        .expect("read current trace generation")
        .unwrap_or(0);
        (OffsetDateTime::now_utc().unix_timestamp_nanos() as i64)
            .max(active_generation.saturating_add(2))
    }

    #[test]
    fn parses_session_event_notification_payload() {
        let notification: SessionEventNotification =
            serde_json::from_str(r#"{"thread_key":"cli:test","event_id":42}"#).unwrap();

        assert_eq!(
            notification,
            SessionEventNotification {
                thread_key: "cli:test".to_owned(),
                event_id: 42,
            }
        );
    }

    #[test]
    fn null_trace_columns_decode_as_pre_trace_capabilities_without_backfill() {
        let now = OffsetDateTime::now_utc();
        let session: Session = SessionRow {
            thread_key: "test:legacy-trace-columns".to_owned(),
            title: None,
            sandbox_id: Some("sbx-legacy".to_owned()),
            sandbox_repo_cache_enabled: Some(true),
            sandbox_repo_cache_access: Some("all".to_owned()),
            sandbox_observability_enabled: Some(true),
            sandbox_api_server_enabled: Some(true),
            sandbox_metadata_trace_enabled: None,
            sandbox_metadata_trace_expires_at: None,
            sandbox_metadata_trace_subject_hash: None,
            sandbox_metadata_trace_consent_revision: None,
            sandbox_metadata_trace_config_fingerprint: None,
            sandbox_metadata_trace_config_generation: None,
            harness_type: "codex".to_owned(),
            harness_thread_id: None,
            persona_id: None,
            status: "idle".to_owned(),
            iron_control_principal: None,
            proxy_labels: sqlx::types::Json(BTreeMap::new()),
            sandbox_last_active_at: None,
            created_at: now,
            updated_at: now,
        }
        .try_into()
        .expect("decode legacy row");
        assert!(!session.sandbox_capabilities.unwrap().metadata_trace_enabled);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sandbox_assignment_round_trips_metadata_trace_expiry() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:trace-expiry-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let capabilities = SandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("subject-hash".to_owned()),
            metadata_trace_consent_revision: Some(7),
            metadata_trace_config_fingerprint: Some("trace-config".to_owned()),
            metadata_trace_config_generation: Some(1),
            ..SandboxCapabilities::default_enabled()
        };
        store
            .update_sandbox_assignment(&thread_key, "sbx-trace-expiry", &capabilities)
            .await
            .expect("persist sandbox assignment");

        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read sandbox assignment")
                .sandbox_capabilities,
            Some(capabilities)
        );
        store
            .clear_sandbox_id_if_matches(&thread_key, "sbx-trace-expiry")
            .await
            .expect("clean up legacy trace fixture");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idempotent_trace_grant_is_one_transaction_and_replays_exact_result() {
        let Some(store) = test_store().await else {
            return;
        };
        let workspace_id = format!("T{}", Uuid::new_v4().simple().to_string().to_uppercase());
        let user_id = format!("U{}", Uuid::new_v4().simple().to_string().to_uppercase());
        let expires_at = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let key = "trace-grant-concurrent";
        let request_hash = "put:fixed-hash";
        let first_store = store.clone();
        let second_store = store.clone();
        let (first, second) = tokio::join!(
            first_store.grant_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                expires_at,
                key,
                request_hash,
            ),
            second_store.grant_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                expires_at,
                key,
                request_hash,
            ),
        );
        let first = first.expect("first concurrent grant");
        let second = second.expect("replayed concurrent grant");
        assert_eq!(first, second);
        assert_eq!(first.revision, 1);

        let conflict = store
            .grant_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                expires_at + TimeDuration::minutes(1),
                key,
                "put:different-hash",
            )
            .await;
        assert!(matches!(
            conflict,
            Err(SessionStoreError::MetadataTraceIdempotencyConflict)
        ));
        assert_eq!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .expect("read consent"),
            first
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn old_idempotent_revoke_replay_never_targets_a_regrant() {
        let Some(store) = test_store().await else {
            return;
        };
        let _trace_config_lock = TRACE_CONFIG_TEST_LOCK.lock().await;
        let workspace_id = format!("T-revoke-{}", Uuid::new_v4());
        let user_id = format!("U-revoke-{}", Uuid::new_v4());
        let thread_key =
            ThreadKey::parse(format!("test:revoke-replay-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .unwrap();
        let identity = MetadataTraceConfigIdentity {
            generation: next_trace_generation(&store).await,
            fingerprint: format!("trace-revoke-{}", Uuid::new_v4()),
            enabled: true,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .unwrap();
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let first_grant = store
            .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
            .await
            .unwrap();
        let first_capabilities = SandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("subject-old".to_owned()),
            metadata_trace_consent_revision: Some(first_grant.revision),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SandboxCapabilities::default_enabled()
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-old",
                    &first_capabilities,
                    &identity,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-old",
                )
                .await
                .unwrap()
        );
        let (revoked, targets) = store
            .revoke_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                "subject-old",
                "revoke-once",
                "delete:once",
            )
            .await
            .unwrap();
        assert_eq!(targets.len(), 1);
        let old = &targets[0];
        assert!(
            store
                .clear_metadata_trace_assignment_if_matches(old, "slack", &workspace_id, &user_id)
                .await
                .unwrap()
        );
        store
            .complete_metadata_trace_drain_if_empty("slack", &workspace_id, &user_id)
            .await
            .unwrap();
        let regrant = store
            .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
            .await
            .unwrap();
        let regranted_capabilities = SandboxCapabilities {
            metadata_trace_consent_revision: Some(regrant.revision),
            metadata_trace_subject_hash: Some("subject-new".to_owned()),
            ..first_capabilities
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-new",
                    &regranted_capabilities,
                    &identity,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-new",
                )
                .await
                .unwrap()
        );
        let (replayed, replay_targets) = store
            .revoke_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                "subject-old",
                "revoke-once",
                "delete:once",
            )
            .await
            .unwrap();
        assert_eq!(replayed, revoked);
        assert!(replay_targets.is_empty());
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .as_deref(),
            Some("sbx-new")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trace_input_claim_fences_actor_revoke_revision_and_expiry() {
        let Some(store) = test_store().await else {
            return;
        };
        let _trace_config_lock = TRACE_CONFIG_TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:trace-steering-{}", Uuid::new_v4())).unwrap();
        let workspace_id = format!("T-test-{}", Uuid::new_v4());
        let user_id = format!("U-test-{}", Uuid::new_v4());
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let generation = next_trace_generation(&store).await;
        let identity = MetadataTraceConfigIdentity {
            generation,
            fingerprint: format!("trace-input-{}", Uuid::new_v4()),
            enabled: true,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .expect("activate trace config");
        let capabilities = SandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(OffsetDateTime::now_utc() + TimeDuration::hours(1)),
            metadata_trace_subject_hash: Some("u1".to_owned()),
            metadata_trace_consent_revision: Some(1),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SandboxCapabilities::default_enabled()
        };
        store
            .grant_metadata_trace_consent(
                "slack",
                &workspace_id,
                &user_id,
                capabilities.metadata_trace_expires_at.unwrap(),
            )
            .await
            .expect("grant assignment test consent");
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-trace-claim",
                    &capabilities,
                    &identity,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-trace-claim",
                )
                .await
                .expect("persist matching trace assignment")
        );
        let metadata = json!({
            "metadata_trace_enabled": capabilities.metadata_trace_enabled,
            "metadata_trace_subject_hash": capabilities.metadata_trace_subject_hash,
            "metadata_trace_consent_revision": capabilities.metadata_trace_consent_revision,
            "metadata_trace_expires_at": capabilities.metadata_trace_expires_at.map(|value| value.to_string()),
            "metadata_trace_config_fingerprint": capabilities.metadata_trace_config_fingerprint,
            "metadata_trace_config_generation": capabilities.metadata_trace_config_generation,
        });
        let execution = store
            .create_execution(&thread_key, None, metadata.clone())
            .await
            .expect("create execution");
        store
            .mark_execution_running(&execution.execution.execution_id)
            .await
            .expect("mark running");

        assert!(
            store
                .claim_active_trace_input(&execution.execution.execution_id, &capabilities)
                .await
                .expect("matching trace boundary claim")
        );
        let mut other_actor = capabilities.clone();
        other_actor.metadata_trace_subject_hash = Some("u2".to_owned());
        assert!(
            !store
                .claim_active_trace_input(&execution.execution.execution_id, &other_actor)
                .await
                .expect("different actor is fenced")
        );

        let mut revoked = metadata.clone();
        revoked["metadata_trace_enabled"] = json!(false);
        sqlx::query("update session_executions set metadata = $2 where execution_id = $1")
            .bind(&execution.execution.execution_id)
            .bind(revoked)
            .execute(store.pool())
            .await
            .expect("record revoke before input claim");
        assert!(
            !store
                .claim_active_trace_input(&execution.execution.execution_id, &capabilities)
                .await
                .expect("revoked actor is fenced")
        );

        let mut revised = metadata.clone();
        revised["metadata_trace_consent_revision"] = json!(8);
        sqlx::query("update session_executions set metadata = $2 where execution_id = $1")
            .bind(&execution.execution.execution_id)
            .bind(revised)
            .execute(store.pool())
            .await
            .expect("record revision before input claim");
        assert!(
            !store
                .claim_active_trace_input(&execution.execution.execution_id, &capabilities)
                .await
                .expect("revised actor is fenced")
        );

        let mut expired = capabilities.clone();
        expired.metadata_trace_expires_at =
            Some(OffsetDateTime::now_utc() - TimeDuration::seconds(1));
        let mut expired_metadata = metadata;
        expired_metadata["metadata_trace_expires_at"] = json!(
            expired
                .metadata_trace_expires_at
                .map(|value| value.to_string())
        );
        sqlx::query("update session_executions set metadata = $2 where execution_id = $1")
            .bind(&execution.execution.execution_id)
            .bind(expired_metadata)
            .execute(store.pool())
            .await
            .expect("record expiry before input claim");
        assert!(
            !store
                .claim_active_trace_input(&execution.execution.execution_id, &expired)
                .await
                .expect("expired actor is fenced")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_trace_input_guard_blocks_revoke_and_config_rollover() {
        let Some(store) = test_store().await else {
            return;
        };
        let _trace_config_lock = TRACE_CONFIG_TEST_LOCK.lock().await;
        let thread_key = ThreadKey::parse(format!("test:trace-guard-{}", Uuid::new_v4())).unwrap();
        let workspace_id = format!("T1-{}", Uuid::new_v4());
        let user_id = format!("U1-{}", Uuid::new_v4());
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .unwrap();
        let identity = MetadataTraceConfigIdentity {
            generation: next_trace_generation(&store).await,
            fingerprint: format!("trace-guard-{}", Uuid::new_v4()),
            enabled: true,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .unwrap();
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let consent = store
            .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
            .await
            .unwrap();
        let capabilities = SandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("guard-u1".to_owned()),
            metadata_trace_consent_revision: Some(consent.revision),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SandboxCapabilities::default_enabled()
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-guard",
                    &capabilities,
                    &identity,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-guard",
                )
                .await
                .unwrap()
        );
        let execution = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap();
        store
            .mark_execution_running(&execution.execution.execution_id)
            .await
            .unwrap();
        assert_eq!(
            store
                .metadata_trace_assignment_actor(&thread_key, "sbx-guard")
                .await
                .unwrap()
                .expect("persisted assignment actor"),
            MetadataTraceAssignmentActor {
                source: "slack".to_owned(),
                workspace_id: workspace_id.clone(),
                user_id: user_id.clone(),
            }
        );
        let guard = store
            .lock_metadata_trace_input(
                &capabilities,
                &thread_key,
                &execution.execution.execution_id,
                "sbx-guard",
            )
            .await
            .unwrap()
            .expect("guard");

        let revoke_store = store.clone();
        let revoke_workspace_id = workspace_id.clone();
        let revoke_user_id = user_id.clone();
        let mut revoke = tokio::spawn(async move {
            revoke_store
                .revoke_metadata_trace_consent(
                    "slack",
                    &revoke_workspace_id,
                    &revoke_user_id,
                    "guard-u1",
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut revoke)
                .await
                .is_err(),
            "revoke must wait for shared guard"
        );
        let rollover_store = store.clone();
        let next_identity = MetadataTraceConfigIdentity {
            generation: identity.generation + 1,
            fingerprint: format!("trace-rollover-{}", Uuid::new_v4()),
            enabled: true,
        };
        let mut rollover = tokio::spawn(async move {
            rollover_store
                .activate_metadata_trace_config(&next_identity)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut rollover)
                .await
                .is_err(),
            "config rollover must wait for shared guard"
        );
        guard.commit().await.unwrap();
        revoke.await.unwrap().unwrap();
        rollover.await.unwrap().unwrap();

        // A fresh guard cannot cross the committed revoke fence.
        assert!(
            store
                .lock_metadata_trace_input(
                    &capabilities,
                    &thread_key,
                    &execution.execution.execution_id,
                    "sbx-guard"
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_trace_grant_rejects_pending_drain_and_duplicate_revoke_preserves_it() {
        let Some(store) = test_store().await else {
            return;
        };
        let workspace_id = format!("T-pending-{}", Uuid::new_v4());
        let user_id = format!("U-pending-{}", Uuid::new_v4());
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        store
            .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
            .await
            .unwrap();
        sqlx::query("update metadata_trace_consents set drain_pending = true where source = 'slack' and workspace_id = $1 and user_id = $2")
            .bind(&workspace_id)
            .bind(&user_id)
            .execute(store.pool())
            .await
            .unwrap();
        let (first, _) = store
            .revoke_metadata_trace_consent("slack", &workspace_id, &user_id, "no-assignment")
            .await
            .unwrap();
        assert!(first.drain_pending);
        let (duplicate, _) = store
            .revoke_metadata_trace_consent("slack", &workspace_id, &user_id, "no-assignment")
            .await
            .unwrap();
        assert!(duplicate.drain_pending);
        assert!(matches!(
            store
                .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
                .await,
            Err(SessionStoreError::MetadataTraceDrainPending)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_trace_generation_fences_activation_leases_and_stale_assignment_clears() {
        let Some(store) = test_store().await else {
            return;
        };
        let _trace_config_lock = TRACE_CONFIG_TEST_LOCK.lock().await;
        let workspace_id = format!("T-test-{}", Uuid::new_v4());
        let user_id = format!("U-test-{}", Uuid::new_v4());
        // A process-wide singleton is intentionally shared by every test DB.
        // This generation is wall-clock ordered so a later invocation can
        // always advance the singleton without deleting its durable history.
        let generation = next_trace_generation(&store).await;
        let a = MetadataTraceConfigIdentity {
            generation,
            fingerprint: format!("trace-a-{}", Uuid::new_v4()),
            enabled: true,
        };
        let b = MetadataTraceConfigIdentity {
            generation: generation + 1,
            fingerprint: format!("trace-b-{}", Uuid::new_v4()),
            enabled: true,
        };
        let (activate_a, activate_b) = tokio::join!(
            store.activate_metadata_trace_config(&a),
            store.activate_metadata_trace_config(&b),
        );
        assert!(
            activate_b.is_ok(),
            "newer B activation must win: {activate_b:?}"
        );
        assert!(
            activate_a.is_ok()
                || matches!(
                    activate_a,
                    Err(SessionStoreError::StaleMetadataTraceGeneration { .. })
                ),
            "A may commit before B, but must otherwise be rejected as stale: {activate_a:?}"
        );
        store
            .activate_metadata_trace_config(&b)
            .await
            .expect("B remains the active identity");
        assert!(matches!(
            store.activate_metadata_trace_config(&a).await,
            Err(SessionStoreError::StaleMetadataTraceGeneration { .. })
        ));
        assert!(matches!(
            store
                .activate_metadata_trace_config(&MetadataTraceConfigIdentity {
                    generation: b.generation,
                    fingerprint: "split-brain".to_owned(),
                    enabled: true,
                })
                .await,
            Err(SessionStoreError::MetadataTraceConfigConflict { .. })
        ));

        let lease_a = store
            .acquire_metadata_trace_reconciler_lease(&b, "reconciler-a", TimeDuration::ZERO)
            .await
            .expect("acquire A lease")
            .expect("A owns expired lease briefly");
        let lease_b = store
            .acquire_metadata_trace_reconciler_lease(&b, "reconciler-b", TimeDuration::seconds(1))
            .await
            .expect("take over lease")
            .expect("B owns lease");
        assert!(lease_b.fence > lease_a.fence);
        assert!(
            !store
                .metadata_trace_reconciler_lease_is_active(&b, &lease_a)
                .await
                .expect("A fence is stale")
        );

        let thread_key = ThreadKey::parse(format!("test:trace-fence-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let consent = store
            .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
            .await
            .expect("grant canonical actor consent");
        let capabilities = SandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("subject-hash".to_owned()),
            metadata_trace_consent_revision: Some(consent.revision),
            metadata_trace_config_fingerprint: Some(b.fingerprint.clone()),
            metadata_trace_config_generation: Some(b.generation),
            ..SandboxCapabilities::default_enabled()
        };
        assert!(
            !store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-stale-a",
                    &capabilities,
                    &a,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-stale-a",
                )
                .await
                .expect("stale A assignment is rejected")
        );
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-active-b",
                    &capabilities,
                    &b,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-active-b",
                )
                .await
                .expect("B assignment")
        );
        assert!(
            !store
                .clear_sandbox_id_if_matches(&thread_key, "sbx-stale-a")
                .await
                .expect("stale request cannot clear B")
        );
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read B assignment")
                .sandbox_id
                .as_deref(),
            Some("sbx-active-b")
        );
        let (_, targets) = store
            .revoke_metadata_trace_consent("slack", &workspace_id, &user_id, "subject-hash")
            .await
            .expect("fence active assignment for revoke");
        let target = targets.into_iter().next().expect("exact drain target");
        assert_eq!(target.resource_uid, "uid-active-b");
        sqlx::query(
            "update sessions set sandbox_metadata_trace_resource_uid = 'uid-replacement' where thread_key = $1",
        )
        .bind(thread_key.as_str())
        .execute(store.pool())
        .await
        .expect("simulate same-name replacement UID");
        let replacement_uid = sqlx::query_scalar::<_, String>(
            "select sandbox_metadata_trace_resource_uid from sessions where thread_key = $1",
        )
        .bind(thread_key.as_str())
        .fetch_one(store.pool())
        .await
        .expect("read replacement UID");
        assert_eq!(replacement_uid, "uid-replacement");
        assert!(
            !store
                .clear_metadata_trace_assignment_if_matches(
                    &target,
                    "slack",
                    &workspace_id,
                    &user_id,
                )
                .await
                .expect("old UID must not clear a replacement")
        );
        assert!(
            !store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-late-writer",
                    &capabilities,
                    &b,
                    None,
                    &workspace_id,
                    &user_id,
                    "uid-late-writer",
                )
                .await
                .expect("a stale create CAS is rejected")
        );
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read fenced assignment")
                .sandbox_id
                .as_deref(),
            Some("sbx-active-b")
        );
    }

    fn idle_row(
        metadata: serde_json::Value,
        completed_at: OffsetDateTime,
    ) -> IdleSandboxCandidateRow {
        IdleSandboxCandidateRow {
            thread_key: "test:idle-row".to_owned(),
            sandbox_id: "sbx-idle-row".to_owned(),
            execution_id: "exe-idle-row".to_owned(),
            completed_at,
            metadata,
        }
    }

    #[test]
    fn idle_candidate_uses_persisted_timeout_deadline() {
        let now = OffsetDateTime::now_utc();
        let candidate = super::idle_candidate_from_row(
            idle_row(
                json!({"idle_timeout_ms": 1000}),
                now - TimeDuration::seconds(2),
            ),
            Duration::from_secs(3600),
            now,
        )
        .unwrap()
        .expect("candidate should use persisted timeout");

        assert_eq!(candidate.idle_timeout, Duration::from_secs(1));
    }

    #[test]
    fn idle_candidate_waits_for_persisted_timeout_even_when_backstop_elapsed() {
        let now = OffsetDateTime::now_utc();
        let candidate = super::idle_candidate_from_row(
            idle_row(
                json!({"idle_timeout_ms": 10_000}),
                now - TimeDuration::seconds(2),
            ),
            Duration::from_secs(1),
            now,
        )
        .unwrap();

        assert!(candidate.is_none());
    }

    #[test]
    fn idle_candidate_falls_back_to_backstop_for_missing_or_invalid_timeout() {
        let now = OffsetDateTime::now_utc();
        let candidate = super::idle_candidate_from_row(
            idle_row(
                json!({"idle_timeout_ms": "not-a-number"}),
                now - TimeDuration::seconds(2),
            ),
            Duration::from_secs(1),
            now,
        )
        .unwrap()
        .expect("candidate should use backstop");

        assert_eq!(candidate.idle_timeout, Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sessions_round_trip_proxy_labels() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:proxy-labels-{}", Uuid::new_v4())).unwrap();
        let labels = BTreeMap::from([
            ("centaur.slack_user_id".to_owned(), "U123".to_owned()),
            ("centaur.slack_team_id".to_owned(), "T123".to_owned()),
            ("centaur.slack_channel_id".to_owned(), "C123".to_owned()),
        ]);

        let created = store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                labels.clone(),
            )
            .await
            .expect("create session");

        assert_eq!(created.proxy_labels, labels);
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("get session")
                .proxy_labels,
            labels
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exact_principal_reuse_rejects_mismatches_without_mutating_proxy_labels() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:exact-principal-{}", Uuid::new_v4()))
            .expect("valid thread key");
        let expected_principal = "principal-expected";
        let mismatched_labels = BTreeMap::from([(
            "centaur.slack_channel_id".to_owned(),
            "C-principal-mismatch".to_owned(),
        )]);

        let created = store
            .create_or_get_session_with_exact_principal(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                BTreeMap::new(),
                expected_principal,
            )
            .await
            .expect("create session with principal");
        assert_eq!(
            created.iron_control_principal.as_deref(),
            Some(expected_principal)
        );

        let error = store
            .create_or_get_session_with_exact_principal(
                &thread_key,
                &HarnessType::ClaudeCode,
                None,
                json!({}),
                mismatched_labels.clone(),
                "principal-mismatch",
            )
            .await
            .expect_err("mismatched principal must fail");
        assert!(matches!(
            error,
            SessionStoreError::PrincipalConflict {
                existing: Some(existing),
                requested: Some(requested),
                ..
            } if existing == expected_principal && requested == "principal-mismatch"
        ));
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("get session")
                .proxy_labels,
            BTreeMap::new(),
            "a principal-mismatched reuse must not update proxy labels"
        );

        let matching_labels = BTreeMap::from([(
            "centaur.slack_channel_id".to_owned(),
            "C-principal-match".to_owned(),
        )]);
        let matched = store
            .create_or_get_session_with_exact_principal(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                matching_labels.clone(),
                expected_principal,
            )
            .await
            .expect("matching principal may reuse the session");
        assert_eq!(matched.proxy_labels, matching_labels);

        let unbound_thread_key =
            ThreadKey::parse(format!("test:exact-unbound-principal-{}", Uuid::new_v4()))
                .expect("valid thread key");
        let unbound_labels = BTreeMap::from([(
            "centaur.slack_channel_id".to_owned(),
            "C-exact-unbound".to_owned(),
        )]);
        store
            .create_or_get_session(
                &unbound_thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                BTreeMap::new(),
            )
            .await
            .expect("create unbound session");
        let error = store
            .create_or_get_session_with_exact_principal(
                &unbound_thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                unbound_labels.clone(),
                "principal-on-unbound-session",
            )
            .await
            .expect_err("unbound session and requested principal must conflict");
        assert!(matches!(
            error,
            SessionStoreError::PrincipalConflict {
                existing: None,
                requested: Some(requested),
                ..
            } if requested == "principal-on-unbound-session"
        ));
        assert_eq!(
            store
                .get_session(&unbound_thread_key)
                .await
                .expect("get unbound session")
                .proxy_labels,
            BTreeMap::new(),
            "an unbound/principal mismatch must not update proxy labels"
        );

        let claimed = store
            .create_or_get_session_with_principal(
                &unbound_thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                unbound_labels.clone(),
                "principal-on-unbound-session",
            )
            .await
            .expect("ordinary session registration may atomically claim a legacy unbound row");
        assert_eq!(
            claimed.iron_control_principal.as_deref(),
            Some("principal-on-unbound-session")
        );
        assert_eq!(claimed.proxy_labels, unbound_labels);

        let error = store
            .create_or_get_session_with_principal(
                &unbound_thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                BTreeMap::new(),
                "principal-after-claim",
            )
            .await
            .expect_err("an ordinary session may not overwrite a claimed principal");
        assert!(matches!(error, SessionStoreError::PrincipalConflict { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_candidates_use_persisted_execution_idle_timeout() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:idle-cleanup-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-idle-{}", Uuid::new_v4());
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        store
            .update_sandbox_id(&thread_key, Some(&sandbox_id))
            .await
            .expect("set sandbox id");
        let execution_id = store
            .create_execution(&thread_key, None, json!({"idle_timeout_ms": 1000}))
            .await
            .expect("create execution")
            .execution
            .execution_id;
        store
            .complete_execution(&execution_id)
            .await
            .expect("complete execution");
        sqlx::query(
            r#"
            update session_executions
            set completed_at = now() - interval '2 seconds', updated_at = now()
            where execution_id = $1
            "#,
        )
        .bind(&execution_id)
        .execute(store.pool())
        .await
        .expect("age execution");

        let candidates = store
            .list_idle_sandbox_candidates(Duration::from_secs(3600))
            .await
            .expect("list idle sandbox candidates");
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.thread_key == thread_key)
            .expect("candidate should use execution idle timeout, not backstop");

        assert_eq!(candidate.sandbox_id, sandbox_id);
        assert_eq!(candidate.execution_id, execution_id);
        assert_eq!(candidate.idle_timeout, Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_owner_fences_output_and_terminal_updates() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:stdout-owner-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&execution_id)
            .await
            .expect("mark running");

        assert!(
            store
                .claim_stdout_owner(&execution_id, "owner-a", Duration::from_secs(60))
                .await
                .expect("owner-a claims stdout")
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "owner-a",
                    Duration::from_secs(60),
                    "session.output.line",
                    json!("line-from-owner-a"),
                )
                .await
                .expect("owner-a appends")
                .is_some()
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "owner-b",
                    Duration::from_secs(60),
                    "session.output.line",
                    json!("line-from-stale-owner-b"),
                )
                .await
                .expect("owner-b append is fenced")
                .is_none()
        );
        assert!(
            terminalize_completed(&store, &execution_id, "owner-b")
                .await
                .expect("owner-b terminal update is fenced")
                .is_none()
        );

        expire_stdout_owner(&store, &execution_id).await;
        assert!(
            store
                .claim_expired_stdout_owner(&execution_id, "owner-b", Duration::from_secs(5))
                .await
                .expect("owner-b claims after lease expiry")
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "owner-a",
                    Duration::from_secs(5),
                    "session.output.line",
                    json!("line-from-expired-owner-a"),
                )
                .await
                .expect("expired owner-a append is fenced")
                .is_none()
        );
        let completed = terminalize_completed(&store, &execution_id, "owner-b")
            .await
            .expect("owner-b completes")
            .expect("completion should be recorded");
        assert_eq!(
            completed.status,
            centaur_session_core::ExecutionStatus::Completed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_stdout_owner_can_renew_append_and_terminalize() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner_id = format!("owner-{}", Uuid::new_v4().simple());

        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "active-complete", &owner_id).await;
        assert!(
            store
                .claim_stdout_owner(&execution_id, &owner_id, Duration::from_secs(60))
                .await
                .expect("active owner extends its lease through a repeat claim")
        );
        assert!(
            store
                .renew_stdout_owner(&execution_id, &owner_id, Duration::from_secs(60))
                .await
                .expect("active owner renews")
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    &owner_id,
                    Duration::from_secs(60),
                    "session.output.line",
                    json!("line-from-active-owner"),
                )
                .await
                .expect("active owner appends")
                .is_some()
        );
        let completed = terminalize_completed(&store, &execution_id, &owner_id)
            .await
            .expect("complete as active owner")
            .expect("active owner completes");
        assert_eq!(
            completed.status,
            centaur_session_core::ExecutionStatus::Completed
        );

        let (_, failed_execution_id) =
            running_execution_with_stdout_owner(&store, "active-fail", &owner_id).await;
        let failed = terminalize_failed(
            &store,
            &failed_execution_id,
            &owner_id,
            "expected test failure",
        )
        .await
        .expect("fail as active owner")
        .expect("active owner fails execution");
        assert_eq!(failed.status, centaur_session_core::ExecutionStatus::Failed);

        let (_, cancelled_execution_id) =
            running_execution_with_stdout_owner(&store, "active-cancel", &owner_id).await;
        let cancelled = terminalize_cancelled(
            &store,
            &cancelled_execution_id,
            &owner_id,
            "expected test cancellation",
        )
        .await
        .expect("cancel as active owner")
        .expect("active owner cancels execution");
        assert_eq!(
            cancelled.status,
            centaur_session_core::ExecutionStatus::Cancelled
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminalize_execution_and_event_commits_exact_terminal_lifecycle() {
        let Some(store) = test_store().await else {
            return;
        };

        let cases = [
            (
                "complete",
                OwnedTerminalEvent::Completed {
                    payload: json!({"completion_reason": "process_exited"}),
                },
                ExecutionStatus::Completed,
                None,
                "session.execution_completed",
                json!({"completion_reason": "process_exited"}),
                SessionStatus::Idle,
            ),
            (
                "fail",
                OwnedTerminalEvent::Failed {
                    error: "expected test failure".to_owned(),
                    payload: json!({"error": "expected test failure"}),
                },
                ExecutionStatus::Failed,
                Some("expected test failure"),
                "session.execution_failed",
                json!({"error": "expected test failure"}),
                SessionStatus::Failed,
            ),
            (
                "cancel",
                OwnedTerminalEvent::Cancelled {
                    reason: "expected test cancellation".to_owned(),
                    payload: json!({"reason": "expected test cancellation"}),
                },
                ExecutionStatus::Cancelled,
                Some("expected test cancellation"),
                "session.execution_cancelled",
                json!({"reason": "expected test cancellation"}),
                SessionStatus::Idle,
            ),
        ];

        for (
            label,
            terminal,
            expected_terminal_status,
            expected_error,
            expected_event_type,
            expected_payload,
            expected_session_status,
        ) in cases
        {
            let owner_id = format!("owner-{label}-{}", Uuid::new_v4().simple());
            let (thread_key, execution_id) =
                running_execution_with_stdout_owner(&store, label, &owner_id).await;
            let (execution, event) = store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution_id,
                    &owner_id,
                    terminal,
                )
                .await
                .expect("terminal transition and event succeed")
                .expect("active owner terminalizes execution");

            assert_eq!(execution.status, expected_terminal_status);
            assert_eq!(execution.error.as_deref(), expected_error);
            assert_eq!(event.thread_key, thread_key);
            assert_eq!(event.execution_id.as_deref(), Some(execution_id.as_str()));
            assert_eq!(event.event_type, expected_event_type);
            assert_eq!(event.payload, expected_payload);
            assert_eq!(
                store
                    .get_session(&thread_key)
                    .await
                    .expect("read terminal session")
                    .status,
                expected_session_status
            );
            assert_eq!(
                store
                    .list_events_after(&thread_key, 0, Some(&execution_id), 100)
                    .await
                    .expect("read terminal event"),
                vec![event],
                "the returned lifecycle event must be durably committed with the transition"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminalize_execution_and_event_owner_fence_is_a_noop() {
        let Some(store) = test_store().await else {
            return;
        };
        let old_owner_id = format!("owner-old-{}", Uuid::new_v4().simple());
        let new_owner_id = format!("owner-new-{}", Uuid::new_v4().simple());
        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "terminal-fence", &old_owner_id).await;

        assert!(
            store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution_id,
                    "wrong-owner",
                    OwnedTerminalEvent::Completed {
                        payload: json!({"source": "wrong_owner"}),
                    },
                )
                .await
                .expect("wrong owner is a fenced no-op")
                .is_none()
        );
        assert_execution_still_active(&store, &thread_key).await;
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 100)
                .await
                .expect("read events after wrong-owner fence")
                .is_empty()
        );

        expire_stdout_owner(&store, &execution_id).await;
        assert!(
            store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution_id,
                    &old_owner_id,
                    OwnedTerminalEvent::Failed {
                        error: "stale owner must not fail execution".to_owned(),
                        payload: json!({"source": "expired_owner"}),
                    },
                )
                .await
                .expect("expired owner is a fenced no-op")
                .is_none()
        );
        assert_execution_still_active(&store, &thread_key).await;
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read session after stale owner fence")
                .status,
            SessionStatus::Executing
        );
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 100)
                .await
                .expect("read events after stale owner fence")
                .is_empty()
        );

        assert!(
            store
                .claim_expired_stdout_owner(&execution_id, &new_owner_id, Duration::from_secs(60))
                .await
                .expect("new owner adopts expired lease")
        );
        assert!(
            store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution_id,
                    &new_owner_id,
                    OwnedTerminalEvent::Completed {
                        payload: json!({"source": "adopter"}),
                    },
                )
                .await
                .expect("adopter transition succeeds")
                .is_some()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminalize_execution_and_event_rolls_back_when_event_insert_fails() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner_id = format!("owner-{}", Uuid::new_v4().simple());
        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "terminal-event-rollback", &owner_id).await;
        let suffix = Uuid::new_v4().simple().to_string();
        let function_name = format!("test_terminal_event_failure_fn_{suffix}");
        let trigger_name = format!("test_terminal_event_failure_trigger_{suffix}");
        let forced_event_type = "session.execution_failed";

        sqlx::query(&format!(
            r#"
            create function {function_name}() returns trigger language plpgsql as $$
            begin
                if new.event_type = '{forced_event_type}' and new.execution_id = '{execution_id}' then
                    raise exception 'forced terminal event insert failure';
                end if;
                return new;
            end;
            $$
            "#,
        ))
        .execute(store.pool())
        .await
        .expect("install forced event failure function");
        sqlx::query(&format!(
            "create trigger {trigger_name} before insert on session_events for each row execute function {function_name}()"
        ))
        .execute(store.pool())
        .await
        .expect("install forced event failure trigger");

        let terminal_result = store
            .terminalize_execution_and_append_event_if_stdout_owner(
                &execution_id,
                &owner_id,
                OwnedTerminalEvent::Failed {
                    error: "must roll back".to_owned(),
                    payload: json!({"error": "must roll back"}),
                },
            )
            .await;

        sqlx::query(&format!("drop trigger {trigger_name} on session_events"))
            .execute(store.pool())
            .await
            .expect("remove forced event failure trigger");
        sqlx::query(&format!("drop function {function_name}()"))
            .execute(store.pool())
            .await
            .expect("remove forced event failure function");

        let error = terminal_result.expect_err("forced event insertion must fail the transaction");
        assert!(
            error
                .to_string()
                .contains("forced terminal event insert failure"),
            "the insert failure must reach the caller: {error}"
        );
        assert_execution_still_active(&store, &thread_key).await;
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read session after rolled-back terminal transition")
                .status,
            SessionStatus::Executing
        );
        assert!(
            store
                .renew_stdout_owner(&execution_id, &owner_id, Duration::from_secs(60))
                .await
                .expect("rolled-back terminal transition preserves live owner")
        );
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 100)
                .await
                .expect("read events after rolled-back terminal transition")
                .is_empty(),
            "a failed event write must not leave a terminal lifecycle event"
        );

        assert!(
            store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution_id,
                    &owner_id,
                    OwnedTerminalEvent::Completed {
                        payload: json!({"source": "rollback-test-cleanup"}),
                    },
                )
                .await
                .expect("owner remains able to terminalize after rollback")
                .is_some()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_transition_cannot_clobber_a_successor_session_status() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner_id = format!("owner-old-{}", Uuid::new_v4().simple());
        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "terminal-successor", &owner_id).await;

        let mut session_lock = store.pool().begin().await.expect("begin session row lock");
        sqlx::query("select 1 from sessions where thread_key = $1 for update")
            .bind(thread_key.as_str())
            .fetch_one(&mut *session_lock)
            .await
            .expect("lock session row");

        let terminal_store = store.clone();
        let terminal_execution_id = execution_id.clone();
        let terminal_owner_id = owner_id.clone();
        let mut terminal = tokio::spawn(async move {
            terminal_store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &terminal_execution_id,
                    &terminal_owner_id,
                    OwnedTerminalEvent::Completed {
                        payload: json!({"source": "contention-test"}),
                    },
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut terminal)
                .await
                .is_err(),
            "the terminal transaction must wait on the session status row"
        );

        let successor_store = store.clone();
        let successor_thread_key = thread_key.clone();
        let mut successor = tokio::spawn(async move {
            successor_store
                .create_execution(&successor_thread_key, None, json!({"successor": true}))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut successor)
                .await
                .is_err(),
            "the one-active-execution index must keep a successor out until the terminal transaction commits"
        );

        session_lock
            .commit()
            .await
            .expect("release session row lock");
        assert!(
            terminal
                .await
                .expect("terminal task completes")
                .expect("terminal transaction succeeds")
                .is_some(),
            "the original owner must terminalize the old execution"
        );
        let successor_execution = successor
            .await
            .expect("successor task completes")
            .expect("successor insert succeeds after terminal commit")
            .execution;
        store
            .mark_execution_running(&successor_execution.execution_id)
            .await
            .expect("mark successor running");

        let active = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("read successor execution")
            .expect("successor remains active");
        assert_eq!(active.execution_id, successor_execution.execution_id);
        assert_eq!(active.status, ExecutionStatus::Running);
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read session after successor begins")
                .status,
            SessionStatus::Executing,
            "the old terminal transition must not overwrite the successor's executing session state"
        );
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 100)
                .await
                .expect("read old terminal event")
                .iter()
                .any(|event| event.event_type == "session.execution_completed"),
            "the old terminal event must commit before the successor can start"
        );
        cleanup_active_execution(&store, &successor_execution.execution_id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_stdout_owner_cannot_mutate_before_adoption() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner_id = format!("owner-{}", Uuid::new_v4().simple());

        let (_, claim_execution_id) =
            running_execution_with_stdout_owner(&store, "expired-claim", &owner_id).await;
        expire_stdout_owner(&store, &claim_execution_id).await;
        assert!(
            !store
                .claim_stdout_owner(&claim_execution_id, &owner_id, Duration::from_secs(60))
                .await
                .expect("expired owner cannot reclaim its own lease")
        );
        assert!(
            store
                .claim_expired_stdout_owner(
                    &claim_execution_id,
                    &owner_id,
                    Duration::from_secs(60),
                )
                .await
                .expect("same runtime adopts its expired lease during recovery")
        );
        let adopted = terminalize_completed(&store, &claim_execution_id, &owner_id)
            .await
            .expect("same-runtime adopter terminalizes")
            .expect("same-runtime adopter owns a live lease");
        assert_eq!(
            adopted.status,
            centaur_session_core::ExecutionStatus::Completed
        );

        let (renew_thread_key, renew_execution_id) =
            running_execution_with_stdout_owner(&store, "expired-renew", &owner_id).await;
        expire_stdout_owner(&store, &renew_execution_id).await;
        assert!(
            !store
                .renew_stdout_owner(&renew_execution_id, &owner_id, Duration::from_secs(60))
                .await
                .expect("expired owner renewal is rejected")
        );
        assert_execution_still_active(&store, &renew_thread_key).await;
        cleanup_active_execution(&store, &renew_execution_id).await;

        let (append_thread_key, append_execution_id) =
            running_execution_with_stdout_owner(&store, "expired-append", &owner_id).await;
        expire_stdout_owner(&store, &append_execution_id).await;
        assert!(
            store
                .append_event_if_stdout_owner(
                    &append_thread_key,
                    &append_execution_id,
                    &owner_id,
                    Duration::from_secs(60),
                    "session.output.line",
                    json!("line-from-expired-owner"),
                )
                .await
                .expect("expired owner append is rejected")
                .is_none()
        );
        assert!(
            store
                .list_events_after(&append_thread_key, 0, Some(&append_execution_id), 100)
                .await
                .expect("read output events")
                .is_empty(),
            "a rejected expired-owner write must not insert an output event"
        );
        assert_execution_still_active(&store, &append_thread_key).await;
        cleanup_active_execution(&store, &append_execution_id).await;

        let (complete_thread_key, complete_execution_id) =
            running_execution_with_stdout_owner(&store, "expired-complete", &owner_id).await;
        expire_stdout_owner(&store, &complete_execution_id).await;
        assert!(
            terminalize_completed(&store, &complete_execution_id, &owner_id)
                .await
                .expect("expired owner completion is rejected")
                .is_none()
        );
        assert_execution_still_active(&store, &complete_thread_key).await;
        cleanup_active_execution(&store, &complete_execution_id).await;

        let (fail_thread_key, fail_execution_id) =
            running_execution_with_stdout_owner(&store, "expired-fail", &owner_id).await;
        expire_stdout_owner(&store, &fail_execution_id).await;
        assert!(
            terminalize_failed(
                &store,
                &fail_execution_id,
                &owner_id,
                "stale owner must not fail execution",
            )
            .await
            .expect("expired owner failure is rejected")
            .is_none()
        );
        assert_execution_still_active(&store, &fail_thread_key).await;
        cleanup_active_execution(&store, &fail_execution_id).await;

        let (cancel_thread_key, cancel_execution_id) =
            running_execution_with_stdout_owner(&store, "expired-cancel", &owner_id).await;
        expire_stdout_owner(&store, &cancel_execution_id).await;
        assert!(
            terminalize_cancelled(
                &store,
                &cancel_execution_id,
                &owner_id,
                "stale owner must not cancel execution",
            )
            .await
            .expect("expired owner cancellation is rejected")
            .is_none()
        );
        assert_execution_still_active(&store, &cancel_thread_key).await;
        cleanup_active_execution(&store, &cancel_execution_id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_owner_terminal_write_cannot_win_an_adoption_race() {
        let Some(store) = test_store().await else {
            return;
        };
        let old_owner_id = format!("owner-old-{}", Uuid::new_v4().simple());
        let new_owner_id = format!("owner-new-{}", Uuid::new_v4().simple());
        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "adoption-race", &old_owner_id).await;
        expire_stdout_owner(&store, &execution_id).await;

        let (claimed, stale_completion) = tokio::join!(
            store.claim_expired_stdout_owner(&execution_id, &new_owner_id, Duration::from_secs(60)),
            terminalize_completed(&store, &execution_id, &old_owner_id),
        );
        assert!(claimed.expect("new owner claims expired lease"));
        assert!(
            stale_completion
                .expect("stale owner completion is rejected")
                .is_none(),
            "an expired owner cannot terminalize while an adopter claims the row"
        );
        assert_execution_still_active(&store, &thread_key).await;

        let completed = terminalize_completed(&store, &execution_id, &new_owner_id)
            .await
            .expect("new owner terminalizes")
            .expect("new owner owns a live lease");
        assert_eq!(
            completed.status,
            centaur_session_core::ExecutionStatus::Completed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_owner_fences_harness_thread_id_updates() {
        let Some(store) = test_store().await else {
            return;
        };
        let old_owner_id = format!("owner-old-{}", Uuid::new_v4().simple());
        let new_owner_id = format!("owner-new-{}", Uuid::new_v4().simple());
        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "harness-thread", &old_owner_id).await;

        let root_a = store
            .update_harness_thread_id_if_stdout_owner(
                &thread_key,
                &execution_id,
                &old_owner_id,
                Some("root-a"),
            )
            .await
            .expect("active owner persists root")
            .expect("active owner owns the root write");
        assert_eq!(root_a.harness_thread_id.as_deref(), Some("root-a"));

        expire_stdout_owner(&store, &execution_id).await;
        assert!(
            store
                .update_harness_thread_id_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    &old_owner_id,
                    Some("stale-root"),
                )
                .await
                .expect("expired root write is rejected")
                .is_none()
        );
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read session after stale root write")
                .harness_thread_id
                .as_deref(),
            Some("root-a")
        );

        assert!(
            store
                .claim_expired_stdout_owner(&execution_id, &new_owner_id, Duration::from_secs(60))
                .await
                .expect("new owner adopts expired lease")
        );
        assert!(
            store
                .update_harness_thread_id_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    &old_owner_id,
                    Some("stale-root-after-adoption"),
                )
                .await
                .expect("old owner remains fenced after adoption")
                .is_none()
        );
        let root_b = store
            .update_harness_thread_id_if_stdout_owner(
                &thread_key,
                &execution_id,
                &new_owner_id,
                Some("root-b"),
            )
            .await
            .expect("adopter persists root")
            .expect("adopter owns the root write");
        assert_eq!(root_b.harness_thread_id.as_deref(), Some("root-b"));
        let completed = terminalize_completed(&store, &execution_id, &new_owner_id)
            .await
            .expect("adopter terminalizes test execution")
            .expect("adopter keeps its lease through cleanup");
        assert_eq!(
            completed.status,
            centaur_session_core::ExecutionStatus::Completed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_owner_root_write_rechecks_lease_after_waiting_on_execution_row() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner_id = format!("owner-{}", Uuid::new_v4().simple());
        let (thread_key, execution_id) =
            running_execution_with_stdout_owner(&store, "root-write-lock", &owner_id).await;
        sqlx::query(
            r#"
            update session_executions
            set stdout_owner_lease_expires_at = clock_timestamp() + interval '1 second',
                updated_at = now()
            where execution_id = $1
            "#,
        )
        .bind(&execution_id)
        .execute(store.pool())
        .await
        .expect("shorten owner lease");

        let mut lock = store
            .pool()
            .begin()
            .await
            .expect("begin row-lock transaction");
        sqlx::query("select 1 from session_executions where execution_id = $1 for update")
            .bind(&execution_id)
            .fetch_one(&mut *lock)
            .await
            .expect("lock execution row");

        let update_store = store.clone();
        let update_thread_key = thread_key.clone();
        let update_execution_id = execution_id.clone();
        let update_owner_id = owner_id.clone();
        let mut root_update = tokio::spawn(async move {
            update_store
                .update_harness_thread_id_if_stdout_owner(
                    &update_thread_key,
                    &update_execution_id,
                    &update_owner_id,
                    Some("stale-root"),
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut root_update)
                .await
                .is_err(),
            "the guarded root write must wait on the execution fence"
        );
        tokio::time::sleep(Duration::from_millis(1100)).await;
        lock.commit().await.expect("release execution row lock");

        assert!(
            root_update
                .await
                .expect("root write task completes")
                .expect("root write query succeeds")
                .is_none(),
            "the lease must be rechecked after the row-lock wait"
        );
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read session after expired root write")
                .harness_thread_id,
            None
        );
        cleanup_active_execution(&store, &execution_id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn releases_all_stdout_leases_held_by_one_owner() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner = format!("owner-{}", Uuid::new_v4().simple());
        let peer = format!("peer-{}", Uuid::new_v4().simple());
        let mut owned = Vec::new();
        for label in ["a", "b"] {
            let thread_key =
                ThreadKey::parse(format!("test:handoff-{label}-{}", Uuid::new_v4())).unwrap();
            store
                .create_or_get_session(
                    &thread_key,
                    &HarnessType::Codex,
                    None,
                    json!({}),
                    Default::default(),
                )
                .await
                .expect("create session");
            let execution_id = store
                .create_execution(&thread_key, None, json!({}))
                .await
                .expect("create execution")
                .execution
                .execution_id;
            store
                .mark_execution_running(&execution_id)
                .await
                .expect("mark running");
            assert!(
                store
                    .claim_stdout_owner(&execution_id, &owner, Duration::from_secs(60))
                    .await
                    .expect("claim stdout owner")
            );
            owned.push((execution_id, thread_key));
        }
        // A bystander owner's lease must survive the release untouched.
        let bystander_thread =
            ThreadKey::parse(format!("test:handoff-bystander-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &bystander_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create bystander session");
        let bystander_execution = store
            .create_execution(&bystander_thread, None, json!({}))
            .await
            .expect("create bystander execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&bystander_execution)
            .await
            .expect("mark bystander running");
        let bystander = format!("bystander-{}", Uuid::new_v4().simple());
        assert!(
            store
                .claim_stdout_owner(&bystander_execution, &bystander, Duration::from_secs(60))
                .await
                .expect("claim bystander lease")
        );
        assert_eq!(
            store
                .count_executions_with_stdout_owner(&owner)
                .await
                .expect("count owned"),
            2
        );

        let released = store
            .release_stdout_owned_executions(&owner)
            .await
            .expect("release owned leases");
        assert_eq!(released.len(), 2);
        for (execution_id, thread_key) in &owned {
            assert!(
                released.iter().any(|execution| {
                    execution.execution_id == *execution_id && execution.thread_key == *thread_key
                }),
                "released set must include {execution_id}"
            );
        }
        assert_eq!(
            store
                .count_executions_with_stdout_owner(&owner)
                .await
                .expect("count after release"),
            0
        );

        // Released leases are immediately claimable by a peer, without
        // waiting for expiry.
        assert!(
            store
                .claim_stdout_owner(&owned[0].0, &peer, Duration::from_secs(60))
                .await
                .expect("peer claims released lease")
        );

        assert_eq!(
            store
                .count_executions_with_stdout_owner(&bystander)
                .await
                .expect("count bystander"),
            1,
            "release must be scoped to the requested owner"
        );
        store
            .fail_execution_if_active(&bystander_execution, "test cleanup")
            .await
            .expect("terminalize bystander");

        // Terminal executions are never part of a release, even if a lease
        // column is still populated.
        for (execution_id, _) in &owned {
            store
                .fail_execution_if_active(execution_id, "test cleanup")
                .await
                .expect("terminalize execution");
        }
        assert!(
            store
                .release_stdout_owned_executions(&peer)
                .await
                .expect("release for peer")
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_eviction_reservation_blocks_later_claims() {
        let Some(store) = test_store().await else {
            return;
        };
        let sandbox_id = format!("sbx-warm-evict-{}", Uuid::new_v4());
        let workload_key = format!("workload-warm-evict-{}", Uuid::new_v4());
        store
            .insert_ready_warm_sandbox(&sandbox_id, &workload_key)
            .await
            .expect("insert warm sandbox");
        sqlx::query(
            r#"
            update session_warm_sandboxes
            set created_at = now() - interval '100 years'
            where sandbox_id = $1
            "#,
        )
        .bind(&sandbox_id)
        .execute(store.pool())
        .await
        .expect("age warm sandbox");

        let reserved = store
            .reserve_ready_warm_sandboxes_for_eviction(1)
            .await
            .expect("reserve warm sandbox");

        assert_eq!(reserved, vec![sandbox_id.clone()]);
        assert_eq!(
            store
                .claim_ready_warm_sandbox(&workload_key, "test-thread")
                .await
                .expect("claim after reservation"),
            None
        );
        assert!(
            store
                .list_referenced_sandbox_ids()
                .await
                .expect("list referenced sandboxes")
                .contains(&sandbox_id)
        );

        store
            .mark_warm_sandbox_failed(&sandbox_id, "test cleanup")
            .await
            .expect("mark reserved warm sandbox failed");
        assert!(
            !store
                .list_referenced_sandbox_ids()
                .await
                .expect("list referenced sandboxes")
                .contains(&sandbox_id)
        );
    }
}
